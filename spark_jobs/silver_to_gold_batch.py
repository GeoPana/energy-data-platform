"""Create business-ready gold Parquet outputs from Phase 2 silver datasets."""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common.logging_utils import log_job_end, log_job_start, log_metric
from spark_jobs.common.paths import get_project_paths
from spark_jobs.common.spark_session import create_spark_session


JOB_NAME = "silver_to_gold_batch"


def _restore_gitkeep(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / ".gitkeep").write_text(f"Tracked placeholder for {path.name} generated data.\n", encoding="utf-8")


def _write_parquet(df: DataFrame, path: Path, partition_columns: list[str] | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(str(path))
    _restore_gitkeep(path)


def build_daily_region_consumption(clean_consumption: DataFrame) -> DataFrame:
    return clean_consumption.groupBy("reading_date", "region_name").agg(
        F.sum("kwh").alias("total_kwh"),
        F.avg("kwh").alias("average_kwh"),
        F.count("*").alias("reading_count"),
        F.countDistinct("meter_id").alias("unique_meter_count"),
    ).select(
        "reading_date",
        "region_name",
        F.round("total_kwh", 4).alias("total_kwh"),
        F.round("average_kwh", 4).alias("average_kwh"),
        "reading_count",
        "unique_meter_count",
        F.current_timestamp().alias("processed_timestamp"),
    )


def build_daily_customer_consumption(clean_consumption: DataFrame) -> DataFrame:
    return clean_consumption.groupBy("reading_date", "customer_id", "meter_id", "region_name").agg(
        F.sum("kwh").alias("total_kwh"),
        F.count("*").alias("reading_count"),
    ).select(
        "reading_date",
        "customer_id",
        "meter_id",
        "region_name",
        F.round("total_kwh", 4).alias("total_kwh"),
        "reading_count",
        F.current_timestamp().alias("processed_timestamp"),
    )


def build_monthly_region_consumption(daily_region_consumption: DataFrame) -> DataFrame:
    return (
        daily_region_consumption.withColumn("year_number", F.year("reading_date"))
        .withColumn("month_number", F.month("reading_date"))
        .groupBy("year_number", "month_number", "region_name")
        .agg(
            F.sum("total_kwh").alias("total_kwh"),
            F.avg("total_kwh").alias("average_daily_kwh"),
            F.max("unique_meter_count").alias("unique_meter_count"),
        )
        .select(
            "year_number",
            "month_number",
            "region_name",
            F.round("total_kwh", 4).alias("total_kwh"),
            F.round("average_daily_kwh", 4).alias("average_daily_kwh"),
            "unique_meter_count",
            F.current_timestamp().alias("processed_timestamp"),
        )
    )


def build_consumption_weather_features(clean_consumption: DataFrame, clean_weather: DataFrame) -> DataFrame:
    return clean_consumption.join(
        clean_weather,
        (clean_consumption.region_name == clean_weather.region_name)
        & (clean_consumption.reading_date == clean_weather.weather_date)
        & (clean_consumption.reading_hour == clean_weather.weather_hour),
        "left",
    ).select(
        clean_consumption.reading_timestamp,
        clean_consumption.reading_date,
        clean_consumption.reading_hour,
        clean_consumption.region_name,
        clean_consumption.meter_id,
        clean_consumption.customer_id,
        F.round(clean_consumption.kwh, 4).alias("kwh"),
        clean_weather.temperature_c,
        clean_weather.humidity_percent,
        clean_weather.wind_speed_kph,
        clean_weather.precipitation_mm,
        F.current_timestamp().alias("processed_timestamp"),
    )


def build_customer_usage_summary(clean_consumption: DataFrame) -> DataFrame:
    daily_customer = clean_consumption.groupBy("customer_id", "region_name", "reading_date").agg(
        F.sum("kwh").alias("daily_kwh")
    )

    summary = clean_consumption.groupBy("customer_id", "region_name").agg(
        F.sum("kwh").alias("total_kwh"),
        F.countDistinct("reading_date").alias("active_days"),
        F.min("reading_timestamp").alias("first_reading_timestamp"),
        F.max("reading_timestamp").alias("last_reading_timestamp"),
    )

    average_daily = daily_customer.groupBy("customer_id", "region_name").agg(
        F.avg("daily_kwh").alias("average_daily_kwh")
    )

    return summary.join(average_daily, ["customer_id", "region_name"], "inner").select(
        "customer_id",
        "region_name",
        F.round("total_kwh", 4).alias("total_kwh"),
        F.round("average_daily_kwh", 4).alias("average_daily_kwh"),
        "active_days",
        "first_reading_timestamp",
        "last_reading_timestamp",
        F.current_timestamp().alias("processed_timestamp"),
    )


def main() -> None:
    log_job_start(JOB_NAME)
    spark = create_spark_session(JOB_NAME)
    paths = get_project_paths()
    paths.create_lakehouse_directories()

    clean_consumption = spark.read.parquet(str(paths.silver_clean_consumption))
    clean_weather = spark.read.parquet(str(paths.silver_clean_weather))

    log_metric(JOB_NAME, "clean_consumption_rows", clean_consumption.count())
    log_metric(JOB_NAME, "clean_weather_rows", clean_weather.count())

    daily_region = build_daily_region_consumption(clean_consumption)
    daily_customer = build_daily_customer_consumption(clean_consumption)
    monthly_region = build_monthly_region_consumption(daily_region)
    weather_features = build_consumption_weather_features(clean_consumption, clean_weather)
    customer_summary = build_customer_usage_summary(clean_consumption)

    _write_parquet(daily_region, paths.gold_daily_region_consumption, ["reading_date"])
    _write_parquet(daily_customer, paths.gold_daily_customer_consumption, ["reading_date"])
    _write_parquet(monthly_region, paths.gold_monthly_region_consumption, ["year_number", "month_number"])
    _write_parquet(weather_features, paths.gold_consumption_weather_features, ["reading_date"])
    _write_parquet(customer_summary, paths.gold_customer_usage_summary)

    log_metric(JOB_NAME, "gold_daily_region_rows", daily_region.count())
    log_metric(JOB_NAME, "gold_daily_customer_rows", daily_customer.count())
    log_metric(JOB_NAME, "gold_monthly_region_rows", monthly_region.count())
    log_metric(JOB_NAME, "gold_weather_feature_rows", weather_features.count())
    log_metric(JOB_NAME, "gold_customer_summary_rows", customer_summary.count())

    log_job_end(JOB_NAME)
    spark.stop()


if __name__ == "__main__":
    main()
