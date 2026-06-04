"""Load Phase 2 curated silver/gold outputs into the Phase 1 SQL Server warehouse."""

from __future__ import annotations

import calendar
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable

from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common.logging_utils import log_job_end, log_job_start, log_metric
from spark_jobs.common.paths import get_project_paths, get_sql_server_config
from spark_jobs.common.spark_session import create_spark_session


JOB_NAME = "load_gold_to_sql_server"
PHASE2_BATCH_ID = "phase2_gold_daily_customer_consumption"


def build_connection_string(config: dict[str, Any]) -> str:
    driver = config.get("driver", "ODBC Driver 18 for SQL Server")
    server = config.get("server", "localhost")
    database = config.get("database", "EnergyWarehouse")
    encrypt = "yes" if config.get("encrypt", True) else "no"
    trust_certificate = "yes" if config.get("trust_server_certificate", True) else "no"

    parts = [
        f"DRIVER={{{driver}}}",
        f"SERVER={server}",
        f"DATABASE={database}",
        f"Encrypt={encrypt}",
        f"TrustServerCertificate={trust_certificate}",
    ]

    if config.get("trusted_connection", True):
        parts.append("Trusted_Connection=yes")
    else:
        parts.extend([f"UID={config.get('username', '')}", f"PWD={config.get('password', '')}"])

    return ";".join(parts)


def date_id(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def choose_tariff_id(customer: dict[str, Any]) -> str:
    if customer["customer_segment"] == "Small Business":
        return "T-BIZ-2026"
    if int(customer["household_size"]) <= 2:
        return "T-ECO-2026"
    return "T-STD-2026"


def ensure_dim_date(cursor: Any, dates: Iterable[date]) -> None:
    for value in sorted(set(dates)):
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dw.dim_date WHERE date_id = ?)
            BEGIN
                INSERT INTO dw.dim_date
                (
                    date_id,
                    full_date,
                    year_number,
                    quarter_number,
                    month_number,
                    month_name,
                    day_of_month,
                    day_of_week_name,
                    is_weekend
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            END;
            """,
            date_id(value),
            date_id(value),
            value,
            value.year,
            ((value.month - 1) // 3) + 1,
            value.month,
            calendar.month_name[value.month],
            value.day,
            calendar.day_name[value.weekday()],
            1 if value.weekday() >= 5 else 0,
        )


def upsert_regions(cursor: Any, regions: list[dict[str, Any]]) -> None:
    for region in regions:
        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dw.dim_region WHERE region_name = ?)
            BEGIN
                INSERT INTO dw.dim_region (region_name, country, postcode_area)
                VALUES (?, ?, ?);
            END
            ELSE
            BEGIN
                UPDATE dw.dim_region
                SET country = ?, postcode_area = ?
                WHERE region_name = ?;
            END;
            """,
            region["region_name"],
            region["region_name"],
            region["country"],
            region["postcode_area"],
            region["country"],
            region["postcode_area"],
            region["region_name"],
        )


def fetch_region_ids(cursor: Any) -> dict[str, int]:
    cursor.execute("SELECT region_name, region_id FROM dw.dim_region;")
    return {row.region_name: row.region_id for row in cursor.fetchall()}


def upsert_customers(cursor: Any, customers: list[dict[str, Any]], region_ids: dict[str, int]) -> None:
    for customer in customers:
        region_id = region_ids[customer["region_name"]]
        cursor.execute(
            """
            IF EXISTS (SELECT 1 FROM dw.dim_customer WHERE customer_id = ?)
            BEGIN
                UPDATE dw.dim_customer
                SET customer_segment = ?,
                    household_size = ?,
                    dwelling_type = ?,
                    region_id = ?,
                    signup_date = ?,
                    is_active = ?
                WHERE customer_id = ?;
            END
            ELSE
            BEGIN
                INSERT INTO dw.dim_customer
                (
                    customer_id,
                    customer_segment,
                    household_size,
                    dwelling_type,
                    region_id,
                    signup_date,
                    is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?);
            END;
            """,
            customer["customer_id"],
            customer["customer_segment"],
            customer["household_size"],
            customer["dwelling_type"],
            region_id,
            customer["signup_date"],
            customer["is_active"],
            customer["customer_id"],
            customer["customer_id"],
            customer["customer_segment"],
            customer["household_size"],
            customer["dwelling_type"],
            region_id,
            customer["signup_date"],
            customer["is_active"],
        )


def upsert_meters(cursor: Any, meters: list[dict[str, Any]], region_ids: dict[str, int]) -> None:
    for meter in meters:
        region_id = region_ids[meter["region_name"]]
        cursor.execute(
            """
            IF EXISTS (SELECT 1 FROM dw.dim_meter WHERE meter_id = ?)
            BEGIN
                UPDATE dw.dim_meter
                SET customer_id = ?,
                    region_id = ?,
                    meter_type = ?,
                    installation_date = ?,
                    is_active = ?
                WHERE meter_id = ?;
            END
            ELSE
            BEGIN
                INSERT INTO dw.dim_meter
                (
                    meter_id,
                    customer_id,
                    region_id,
                    meter_type,
                    installation_date,
                    is_active
                )
                VALUES (?, ?, ?, ?, ?, ?);
            END;
            """,
            meter["meter_id"],
            meter["customer_id"],
            region_id,
            meter["meter_type"],
            meter["installation_date"],
            meter["is_active"],
            meter["meter_id"],
            meter["meter_id"],
            meter["customer_id"],
            region_id,
            meter["meter_type"],
            meter["installation_date"],
            meter["is_active"],
        )


def upsert_tariffs(cursor: Any, tariffs: list[dict[str, Any]]) -> None:
    for tariff in tariffs:
        cursor.execute(
            """
            IF EXISTS (SELECT 1 FROM dw.dim_tariff WHERE tariff_id = ?)
            BEGIN
                UPDATE dw.dim_tariff
                SET tariff_name = ?,
                    valid_from = ?,
                    valid_to = ?,
                    price_per_kwh = ?,
                    standing_charge_daily = ?,
                    is_active = ?
                WHERE tariff_id = ?;
            END
            ELSE
            BEGIN
                INSERT INTO dw.dim_tariff
                (
                    tariff_id,
                    tariff_name,
                    valid_from,
                    valid_to,
                    price_per_kwh,
                    standing_charge_daily,
                    is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?);
            END;
            """,
            tariff["tariff_id"],
            tariff["tariff_name"],
            tariff["valid_from"],
            tariff["valid_to"],
            tariff["price_per_kwh"],
            tariff["standing_charge_daily"],
            tariff["is_active"],
            tariff["tariff_id"],
            tariff["tariff_id"],
            tariff["tariff_name"],
            tariff["valid_from"],
            tariff["valid_to"],
            tariff["price_per_kwh"],
            tariff["standing_charge_daily"],
            tariff["is_active"],
        )


def load_fact_energy_consumption(
    cursor: Any,
    daily_consumption: list[dict[str, Any]],
    customers_by_id: dict[str, dict[str, Any]],
    tariffs_by_id: dict[str, dict[str, Any]],
    region_ids: dict[str, int],
) -> int:
    # TODO: Replace delete-and-insert with a MERGE keyed by meter_id + date_id in a later hardening pass.
    cursor.execute("DELETE FROM dw.fact_energy_consumption WHERE batch_id = ?;", PHASE2_BATCH_ID)

    rows: list[tuple[Any, ...]] = []
    for record in daily_consumption:
        reading_date = record["reading_date"]
        customer = customers_by_id[record["customer_id"]]
        tariff_id = choose_tariff_id(customer)
        tariff = tariffs_by_id[tariff_id]
        total_kwh = float(record["total_kwh"])
        estimated_cost = round(
            (total_kwh * float(tariff["price_per_kwh"])) + float(tariff["standing_charge_daily"]),
            4,
        )

        rows.append(
            (
                record["meter_id"],
                record["customer_id"],
                region_ids[record["region_name"]],
                tariff_id,
                date_id(reading_date),
                datetime.combine(reading_date, time(hour=23, minute=59, second=59)),
                total_kwh,
                estimated_cost,
                "phase2_gold",
                PHASE2_BATCH_ID,
            )
        )

    cursor.executemany(
        """
        INSERT INTO dw.fact_energy_consumption
        (
            meter_id,
            customer_id,
            region_id,
            tariff_id,
            date_id,
            reading_timestamp,
            kwh,
            estimated_cost,
            source_system,
            batch_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        rows,
    )

    return len(rows)


def collect_dicts(df) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.collect()]


def main() -> None:
    log_job_start(JOB_NAME)

    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError("pyodbc is required for SQL Server loading. Create the Conda environment first.") from exc

    spark = create_spark_session(JOB_NAME)
    paths = get_project_paths()
    sql_config = get_sql_server_config()

    customers = collect_dicts(spark.read.parquet(str(paths.silver_clean_customer)))
    meters = collect_dicts(spark.read.parquet(str(paths.silver_clean_meter)))
    tariffs = collect_dicts(spark.read.parquet(str(paths.silver_clean_tariff)))
    daily_consumption_df = spark.read.parquet(str(paths.gold_daily_customer_consumption))
    daily_consumption = collect_dicts(daily_consumption_df)

    regions = collect_dicts(
        spark.read.parquet(str(paths.silver_clean_customer))
        .select("region_name", "country", "postcode_area")
        .dropDuplicates(["region_name"])
    )

    log_metric(JOB_NAME, "customers_to_load", len(customers))
    log_metric(JOB_NAME, "meters_to_load", len(meters))
    log_metric(JOB_NAME, "tariffs_to_load", len(tariffs))
    log_metric(JOB_NAME, "daily_consumption_rows_to_load", len(daily_consumption))

    customers_by_id = {customer["customer_id"]: customer for customer in customers}
    tariffs_by_id = {tariff["tariff_id"]: tariff for tariff in tariffs}
    fact_dates = [record["reading_date"] for record in daily_consumption]

    connection_string = build_connection_string(sql_config)
    connection = pyodbc.connect(connection_string)

    try:
        cursor = connection.cursor()
        cursor.fast_executemany = True

        upsert_regions(cursor, regions)
        region_ids = fetch_region_ids(cursor)
        ensure_dim_date(cursor, fact_dates)
        upsert_tariffs(cursor, tariffs)
        upsert_customers(cursor, customers, region_ids)
        upsert_meters(cursor, meters, region_ids)
        inserted_facts = load_fact_energy_consumption(
            cursor,
            daily_consumption,
            customers_by_id,
            tariffs_by_id,
            region_ids,
        )

        connection.commit()
        log_metric(JOB_NAME, "fact_energy_consumption_rows_loaded", inserted_facts)
    finally:
        connection.close()
        spark.stop()

    log_job_end(JOB_NAME)


if __name__ == "__main__":
    main()
