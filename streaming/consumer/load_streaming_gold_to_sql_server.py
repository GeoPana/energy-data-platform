"""Optional load of curated streaming outputs into the SQL Server warehouse."""

from __future__ import annotations

import argparse
import calendar
import sys
from datetime import date
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common.paths import get_sql_server_config
from spark_jobs.common.spark_session import create_spark_session
from streaming.common.streaming_logging import log_event
from streaming.common.streaming_paths import get_streaming_paths


def build_connection_string(config: dict[str, Any]) -> str:
    parts = [
        f"DRIVER={{{config.get('driver', 'ODBC Driver 18 for SQL Server')}}}",
        f"SERVER={config.get('server', 'localhost')}",
        f"DATABASE={config.get('database', 'EnergyWarehouse')}",
        f"Encrypt={'yes' if config.get('encrypt', True) else 'no'}",
        f"TrustServerCertificate={'yes' if config.get('trust_server_certificate', True) else 'no'}",
    ]
    if config.get("trusted_connection", True):
        parts.append("Trusted_Connection=yes")
    else:
        parts.extend([f"UID={config.get('username', '')}", f"PWD={config.get('password', '')}"])
    return ";".join(parts)


def date_id(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def ensure_dim_date(cursor: Any, value: date) -> None:
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


def fetch_region_ids(cursor: Any) -> dict[str, int]:
    cursor.execute("SELECT region_name, region_id FROM dw.dim_region;")
    return {row.region_name: row.region_id for row in cursor.fetchall()}


def load_meter_readings(cursor: Any, readings: list[dict[str, Any]], region_ids: dict[str, int]) -> int:
    inserted = 0
    for row in readings:
        event_date = row["event_timestamp"].date()
        ensure_dim_date(cursor, event_date)
        region_id = region_ids.get(row["region_name"])
        if not region_id:
            continue

        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dw.fact_meter_reading WHERE reading_id = ?)
            BEGIN
                INSERT INTO dw.fact_meter_reading
                (
                    reading_id,
                    meter_id,
                    customer_id,
                    region_id,
                    date_id,
                    event_timestamp,
                    kwh,
                    voltage,
                    source_system,
                    event_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            END;
            """,
            row["event_id"],
            row["event_id"],
            row["meter_id"],
            row["customer_id"],
            region_id,
            date_id(event_date),
            row["event_timestamp"],
            float(row["kwh"]),
            float(row["voltage"] or 230.0),
            row["source_system"],
            row["event_id"],
        )
        inserted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
    return inserted


def load_anomalies(cursor: Any, anomalies: list[dict[str, Any]], region_ids: dict[str, int]) -> int:
    inserted = 0
    for row in anomalies:
        event_date = row["event_timestamp"].date()
        ensure_dim_date(cursor, event_date)
        region_id = region_ids.get(row["region_name"])
        if not region_id:
            continue

        cursor.execute(
            """
            IF NOT EXISTS (SELECT 1 FROM dw.fact_anomaly_event WHERE anomaly_id = ?)
            BEGIN
                INSERT INTO dw.fact_anomaly_event
                (
                    anomaly_id,
                    reading_id,
                    meter_id,
                    region_id,
                    date_id,
                    anomaly_type,
                    anomaly_score,
                    kwh,
                    expected_kwh,
                    detected_at,
                    source_system
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            END;
            """,
            row["anomaly_id"],
            row["anomaly_id"],
            row["event_id"],
            row["meter_id"],
            region_id,
            date_id(event_date),
            row["anomaly_type"],
            row["anomaly_score"],
            float(row["kwh"]),
            row.get("expected_kwh"),
            row["detected_at"],
            row["source_system"],
        )
        inserted += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
    return inserted


def collect_rows(spark, path: Path, limit: int) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Expected streaming gold/silver path does not exist: {path}")
    return [row.asDict(recursive=True) for row in spark.read.parquet(str(path)).limit(limit).collect()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Load curated streaming outputs to SQL Server.")
    parser.add_argument("--limit", type=int, default=10000)
    args = parser.parse_args()

    try:
        import pyodbc
    except ImportError as exc:
        raise RuntimeError("pyodbc is required for SQL Server loading. Update environment.yml first.") from exc

    paths = get_streaming_paths()
    spark = create_spark_session("load_streaming_gold_to_sql_server")
    readings = collect_rows(spark, paths.silver_clean_streaming_events, args.limit)
    anomalies = collect_rows(spark, paths.gold_streaming_anomaly_events, args.limit)

    connection = pyodbc.connect(build_connection_string(get_sql_server_config()), timeout=10)
    try:
        cursor = connection.cursor()
        region_ids = fetch_region_ids(cursor)
        reading_count = load_meter_readings(cursor, readings, region_ids)
        anomaly_count = load_anomalies(cursor, anomalies, region_ids)
        connection.commit()
        log_event("streaming_sql_load_complete", meter_readings=reading_count, anomalies=anomaly_count)
    finally:
        connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
