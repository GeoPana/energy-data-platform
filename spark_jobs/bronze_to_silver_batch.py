"""Transform Phase 2 bronze CSV data into cleaned silver Parquet datasets."""

from __future__ import annotations

import sys
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common.data_quality import VALID_LONDON_REGIONS, spark_rejection_reason
from spark_jobs.common.logging_utils import log_job_end, log_job_start, log_metric
from spark_jobs.common.paths import get_project_paths
from spark_jobs.common.schemas import (
    CUSTOMER_METADATA_SCHEMA,
    METER_METADATA_SCHEMA,
    RAW_HISTORICAL_CONSUMPTION_SCHEMA,
    RAW_WEATHER_SCHEMA,
    TARIFF_METADATA_SCHEMA,
)
from spark_jobs.common.spark_session import create_spark_session


JOB_NAME = "bronze_to_silver_batch"


def _csv_files(root: Path) -> list[str]:
    files = sorted(str(path) for path in root.rglob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found under {root}")
    return files


def _read_csv_files(spark: SparkSession, files: list[str], schema) -> DataFrame:
    return spark.read.schema(schema).option("header", True).csv(files)


def _read_csv_file(spark: SparkSession, file_path: Path, schema) -> DataFrame:
    if not file_path.exists():
        raise FileNotFoundError(f"Missing required bronze file: {file_path}")
    return spark.read.schema(schema).option("header", True).csv(str(file_path))


def _clean_text(column_name: str):
    value = F.trim(F.col(column_name))
    return F.when(F.length(value) == 0, F.lit(None)).otherwise(value)


def _write_parquet(df: DataFrame, path: Path, partition_columns: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(path))
    _restore_gitkeep(path)


def _restore_gitkeep(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / ".gitkeep").write_text(f"Tracked placeholder for {path.name} generated data.\n", encoding="utf-8")


def transform_consumption(raw_consumption: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = (
        raw_consumption.withColumn("meter_id_clean", _clean_text("meter_id"))
        .withColumn("customer_id_clean", _clean_text("customer_id"))
        .withColumn("region_name_clean", _clean_text("region_name"))
        .withColumn("reading_timestamp_parsed", F.to_timestamp("reading_timestamp"))
        .withColumn("kwh_value", F.col("kwh").cast("double"))
        .withColumn("ingestion_timestamp_parsed", F.to_timestamp("ingestion_timestamp"))
        .withColumn("raw_payload", F.to_json(F.struct(*[F.col(column) for column in raw_consumption.columns])))
    )

    duplicate_window = Window.partitionBy("meter_id_clean", "reading_timestamp_parsed").orderBy(
        F.col("source_file").asc_nulls_last(),
        F.col("batch_id").asc_nulls_last(),
    )
    typed = typed.withColumn("duplicate_row_number", F.row_number().over(duplicate_window))

    duplicate_condition = (
        F.col("meter_id_clean").isNotNull()
        & F.col("reading_timestamp_parsed").isNotNull()
        & (F.col("duplicate_row_number") > 1)
    )

    typed = typed.withColumn(
        "rejection_reason",
        spark_rejection_reason(
            [
                (F.col("meter_id_clean").isNull(), "Missing meter_id"),
                (F.col("customer_id_clean").isNull(), "Missing customer_id"),
                (F.col("reading_timestamp_parsed").isNull(), "Invalid reading_timestamp"),
                (F.col("kwh_value").isNull(), "Missing or invalid kwh"),
                (F.col("kwh_value") < 0, "Negative kwh"),
                (
                    F.col("region_name_clean").isNull()
                    | ~F.col("region_name_clean").isin(sorted(VALID_LONDON_REGIONS)),
                    "Invalid region_name",
                ),
                (duplicate_condition, "Duplicate meter_id and reading_timestamp"),
            ]
        ),
    )

    valid = (
        typed.where(F.col("rejection_reason") == "")
        .select(
            F.col("batch_id"),
            F.col("meter_id_clean").alias("meter_id"),
            F.col("customer_id_clean").alias("customer_id"),
            F.col("region_name_clean").alias("region_name"),
            F.col("reading_timestamp_parsed").alias("reading_timestamp"),
            F.to_date("reading_timestamp_parsed").alias("reading_date"),
            F.hour("reading_timestamp_parsed").alias("reading_hour"),
            F.col("kwh_value").alias("kwh"),
            F.col("source_file"),
            F.col("ingestion_timestamp_parsed").alias("ingestion_timestamp"),
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("meter_id_clean"),
                    F.col("customer_id_clean"),
                    F.date_format("reading_timestamp_parsed", "yyyy-MM-dd HH:mm:ss"),
                    F.col("kwh_value").cast("string"),
                ),
                256,
            ).alias("record_hash"),
            F.current_timestamp().alias("processed_timestamp"),
        )
        .dropDuplicates(["record_hash"])
    )

    rejected = typed.where(F.col("rejection_reason") != "").select(
        F.lit("bronze_historical_consumption").alias("source_system"),
        F.concat_ws(":", F.coalesce(F.col("source_file"), F.lit("unknown_file")), F.coalesce(F.col("meter_id"), F.lit("missing_meter"))).alias(
            "source_reference"
        ),
        F.col("raw_payload"),
        F.col("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
        F.col("batch_id"),
    )

    return valid, rejected


def transform_weather(raw_weather: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = (
        raw_weather.withColumn("region_name_clean", _clean_text("region_name"))
        .withColumn("weather_timestamp_parsed", F.to_timestamp("weather_timestamp"))
        .withColumn("temperature_value", F.col("temperature_c").cast("double"))
        .withColumn("humidity_value", F.col("humidity_percent").cast("double"))
        .withColumn("wind_speed_value", F.col("wind_speed_kph").cast("double"))
        .withColumn("precipitation_value", F.col("precipitation_mm").cast("double"))
        .withColumn("ingestion_timestamp_parsed", F.to_timestamp("ingestion_timestamp"))
        .withColumn("raw_payload", F.to_json(F.struct(*[F.col(column) for column in raw_weather.columns])))
    )

    typed = typed.withColumn(
        "rejection_reason",
        spark_rejection_reason(
            [
                (F.col("weather_timestamp_parsed").isNull(), "Invalid weather_timestamp"),
                (F.col("temperature_value").isNull(), "Missing or invalid temperature_c"),
                (
                    F.col("region_name_clean").isNull()
                    | ~F.col("region_name_clean").isin(sorted(VALID_LONDON_REGIONS)),
                    "Invalid region_name",
                ),
            ]
        ),
    )

    valid = typed.where(F.col("rejection_reason") == "").select(
        F.col("region_name_clean").alias("region_name"),
        F.col("weather_timestamp_parsed").alias("weather_timestamp"),
        F.to_date("weather_timestamp_parsed").alias("weather_date"),
        F.hour("weather_timestamp_parsed").alias("weather_hour"),
        F.col("temperature_value").alias("temperature_c"),
        F.col("humidity_value").alias("humidity_percent"),
        F.col("wind_speed_value").alias("wind_speed_kph"),
        F.col("precipitation_value").alias("precipitation_mm"),
        F.col("ingestion_timestamp_parsed").alias("ingestion_timestamp"),
        F.current_timestamp().alias("processed_timestamp"),
    )

    rejected = typed.where(F.col("rejection_reason") != "").select(
        F.lit("bronze_weather").alias("source_system"),
        F.concat_ws(":", F.lit("weather"), F.coalesce(F.col("region_name"), F.lit("missing_region"))).alias("source_reference"),
        F.col("raw_payload"),
        F.col("rejection_reason"),
        F.current_timestamp().alias("rejected_at"),
        F.lit(None).cast("string").alias("batch_id"),
    )

    return valid, rejected


def transform_metadata(
    customers: DataFrame,
    meters: DataFrame,
    tariffs: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    clean_customers = (
        customers.select(
            _clean_text("customer_id").alias("customer_id"),
            _clean_text("customer_segment").alias("customer_segment"),
            F.col("household_size").cast("int").alias("household_size"),
            _clean_text("dwelling_type").alias("dwelling_type"),
            _clean_text("region_name").alias("region_name"),
            _clean_text("country").alias("country"),
            _clean_text("postcode_area").alias("postcode_area"),
            F.to_date("signup_date").alias("signup_date"),
            F.col("is_active").cast("boolean").alias("is_active"),
            F.current_timestamp().alias("processed_timestamp"),
        )
        .where(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )

    clean_meters = (
        meters.select(
            _clean_text("meter_id").alias("meter_id"),
            _clean_text("customer_id").alias("customer_id"),
            _clean_text("region_name").alias("region_name"),
            _clean_text("meter_type").alias("meter_type"),
            F.to_date("installation_date").alias("installation_date"),
            F.col("is_active").cast("boolean").alias("is_active"),
            F.current_timestamp().alias("processed_timestamp"),
        )
        .where(F.col("meter_id").isNotNull())
        .dropDuplicates(["meter_id"])
    )

    clean_tariffs = (
        tariffs.select(
            _clean_text("tariff_id").alias("tariff_id"),
            _clean_text("tariff_name").alias("tariff_name"),
            F.to_date("valid_from").alias("valid_from"),
            F.to_date("valid_to").alias("valid_to"),
            F.col("price_per_kwh").cast("double").alias("price_per_kwh"),
            F.col("standing_charge_daily").cast("double").alias("standing_charge_daily"),
            F.col("is_active").cast("boolean").alias("is_active"),
            F.current_timestamp().alias("processed_timestamp"),
        )
        .where(F.col("tariff_id").isNotNull())
        .dropDuplicates(["tariff_id"])
    )

    return clean_customers, clean_meters, clean_tariffs


def union_rejections(rejections: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), rejections)


def main() -> None:
    log_job_start(JOB_NAME)
    spark = create_spark_session(JOB_NAME)
    paths = get_project_paths()
    paths.create_lakehouse_directories()

    raw_consumption = _read_csv_files(
        spark,
        _csv_files(paths.bronze_historical_consumption),
        RAW_HISTORICAL_CONSUMPTION_SCHEMA,
    )
    raw_weather = _read_csv_files(spark, _csv_files(paths.bronze_weather), RAW_WEATHER_SCHEMA)
    raw_customers = _read_csv_file(spark, paths.bronze_customers_file, CUSTOMER_METADATA_SCHEMA)
    raw_meters = _read_csv_file(spark, paths.bronze_meters_file, METER_METADATA_SCHEMA)
    raw_tariffs = _read_csv_file(spark, paths.bronze_tariffs_file, TARIFF_METADATA_SCHEMA)

    log_metric(JOB_NAME, "raw_consumption_rows", raw_consumption.count())
    log_metric(JOB_NAME, "raw_weather_rows", raw_weather.count())
    log_metric(JOB_NAME, "raw_customer_rows", raw_customers.count())
    log_metric(JOB_NAME, "raw_meter_rows", raw_meters.count())
    log_metric(JOB_NAME, "raw_tariff_rows", raw_tariffs.count())

    clean_consumption, rejected_consumption = transform_consumption(raw_consumption)
    clean_weather, rejected_weather = transform_weather(raw_weather)
    clean_customers, clean_meters, clean_tariffs = transform_metadata(raw_customers, raw_meters, raw_tariffs)

    log_metric(JOB_NAME, "clean_consumption_rows", clean_consumption.count())
    log_metric(JOB_NAME, "rejected_consumption_rows", rejected_consumption.count())
    log_metric(JOB_NAME, "clean_weather_rows", clean_weather.count())
    log_metric(JOB_NAME, "rejected_weather_rows", rejected_weather.count())

    _write_parquet(clean_consumption, paths.silver_clean_consumption, ["reading_date"])
    _write_parquet(clean_weather, paths.silver_clean_weather, ["weather_date"])
    _write_parquet(clean_customers, paths.silver_clean_customer)
    _write_parquet(clean_meters, paths.silver_clean_meter)
    _write_parquet(clean_tariffs, paths.silver_clean_tariff)
    _write_parquet(union_rejections([rejected_consumption, rejected_weather]), paths.silver_rejected_records)

    log_job_end(JOB_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
