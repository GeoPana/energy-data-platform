"""Structured Streaming job for Phase 4 bronze to silver/gold processing."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streaming.common.event_validation import VALID_EVENT_TYPES, VALID_REGIONS
from streaming.common.streaming_logging import log_batch_counts, log_consumer_end, log_consumer_start
from streaming.common.streaming_paths import get_streaming_paths, resolve_project_path


RAW_BRONZE_SCHEMA = StructType(
    [
        StructField("raw_event_payload", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("source_system", StringType(), True),
        StructField("partition_id", StringType(), True),
        StructField("offset", StringType(), True),
        StructField("producer_metadata", StringType(), True),
    ]
)

SMART_METER_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("kwh", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("event_type", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("producer_timestamp", StringType(), True),
    ]
)


def create_streaming_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def parse_timestamp_expr(column_name: str):
    return F.to_timestamp(
        F.regexp_replace(F.regexp_replace(F.col(column_name), "T", " "), "Z", "")
    )


def rejection_reason_expr(duplicate_condition):
    reasons = [
        (F.col("parsed").isNull(), "Malformed JSON"),
        (F.col("event_id").isNull() | (F.length(F.trim("event_id")) == 0), "Missing event_id"),
        (F.col("meter_id").isNull() | (F.length(F.trim("meter_id")) == 0), "Missing meter_id"),
        (F.col("customer_id").isNull() | (F.length(F.trim("customer_id")) == 0), "Missing customer_id"),
        (
            F.col("region_name").isNull() | ~F.col("region_name").isin(sorted(VALID_REGIONS)),
            "Invalid region_name",
        ),
        (F.col("event_timestamp_parsed").isNull(), "Invalid event_timestamp"),
        (F.col("producer_timestamp_parsed").isNull(), "Invalid producer_timestamp"),
        (F.col("kwh").isNull(), "Missing or invalid kwh"),
        (F.col("kwh") < 0, "Negative kwh"),
        (F.col("voltage").isNotNull() & (F.col("voltage") <= 0), "Invalid voltage"),
        (
            F.col("event_type").isNull() | ~F.col("event_type").isin(sorted(VALID_EVENT_TYPES)),
            "Invalid event_type",
        ),
        (duplicate_condition, "Duplicate event_id in micro-batch"),
    ]
    return F.concat_ws(";", *[F.when(condition, F.lit(reason)) for condition, reason in reasons])


def build_raw_stream(spark: SparkSession, source_format: str, source_path: Path):
    if source_format == "bronze_parquet":
        return spark.readStream.schema(RAW_BRONZE_SCHEMA).parquet(str(source_path))
    if source_format == "json_landing":
        return (
            spark.readStream.format("text")
            .load(str(source_path))
            .select(
                F.col("value").alias("raw_event_payload"),
                F.current_timestamp().alias("ingestion_timestamp"),
                F.lit("local_file_stream").alias("source_system"),
                F.lit(None).cast("string").alias("partition_id"),
                F.lit(None).cast("string").alias("offset"),
                F.input_file_name().alias("producer_metadata"),
            )
        )
    raise ValueError(f"Unsupported source format: {source_format}")


def prepare_micro_batch(batch_df):
    parsed = (
        batch_df.withColumn("parsed", F.from_json("raw_event_payload", SMART_METER_EVENT_SCHEMA))
        .select(
            "raw_event_payload",
            "ingestion_timestamp",
            F.col("source_system").alias("bronze_source_system"),
            "partition_id",
            "offset",
            "producer_metadata",
            "parsed.*",
        )
        .withColumn("event_timestamp_parsed", parse_timestamp_expr("event_timestamp"))
        .withColumn("producer_timestamp_parsed", parse_timestamp_expr("producer_timestamp"))
    )

    duplicate_window = Window.partitionBy("event_id").orderBy(F.col("ingestion_timestamp").asc_nulls_last())
    parsed = parsed.withColumn("duplicate_row_number", F.row_number().over(duplicate_window))
    duplicate_condition = F.col("event_id").isNotNull() & (F.col("duplicate_row_number") > 1)
    return parsed.withColumn("rejection_reason", rejection_reason_expr(duplicate_condition))


def write_metrics(spark: SparkSession, metrics_path: Path, batch_id: int, total: int, valid: int, rejected: int, anomalies: int) -> None:
    metrics = [
        {
            "batch_id": batch_id,
            "total_events": total,
            "valid_events": valid,
            "rejected_events": rejected,
            "anomaly_events": anomalies,
            "latest_processed_timestamp": datetime.now(timezone.utc).replace(microsecond=0),
        }
    ]
    spark.createDataFrame(metrics).write.mode("append").parquet(str(metrics_path))


def process_micro_batch(batch_df, batch_id: int, paths) -> None:
    if batch_df.rdd.isEmpty():
        log_batch_counts(batch_id, 0, 0, 0, 0)
        return

    spark = batch_df.sparkSession
    prepared = prepare_micro_batch(batch_df).cache()
    total_count = prepared.count()

    valid = (
        prepared.where(F.col("rejection_reason") == "")
        .select(
            "event_id",
            "meter_id",
            "customer_id",
            "region_name",
            F.col("event_timestamp_parsed").alias("event_timestamp"),
            "kwh",
            "voltage",
            "event_type",
            F.coalesce(F.col("source_system"), F.col("bronze_source_system")).alias("source_system"),
            F.col("producer_timestamp_parsed").alias("producer_timestamp"),
            "ingestion_timestamp",
            F.sha2(F.concat_ws("||", "event_id", "meter_id", F.col("event_timestamp_parsed").cast("string")), 256).alias(
                "record_hash"
            ),
            F.current_timestamp().alias("processed_timestamp"),
        )
    )

    rejected = prepared.where(F.col("rejection_reason") != "").select(
        "raw_event_payload",
        "rejection_reason",
        F.current_timestamp().alias("rejected_at"),
        F.coalesce(F.col("source_system"), F.col("bronze_source_system")).alias("source_system"),
    )

    anomalies = valid.where((F.col("event_type") == "anomaly") | (F.col("kwh") > 10)).select(
        F.concat(F.lit("anom_"), F.col("event_id")).alias("anomaly_id"),
        "event_id",
        "meter_id",
        "customer_id",
        "region_name",
        "event_timestamp",
        F.when(F.col("event_type") == "anomaly", F.lit("producer_flagged_anomaly"))
        .when(F.col("kwh") > 10, F.lit("high_kwh_threshold"))
        .otherwise(F.lit("unknown"))
        .alias("anomaly_type"),
        F.when(F.col("kwh") > 10, F.lit(0.95)).otherwise(F.lit(0.75)).alias("anomaly_score"),
        "kwh",
        F.lit(None).cast("double").alias("expected_kwh"),
        F.current_timestamp().alias("detected_at"),
        "source_system",
        F.current_timestamp().alias("processed_timestamp"),
    )

    latest_window = Window.partitionBy("meter_id").orderBy(F.col("event_timestamp").desc(), F.col("event_id").desc())
    latest = (
        valid.where(F.col("event_type") != "heartbeat")
        .withColumn("row_number_latest", F.row_number().over(latest_window))
        .where(F.col("row_number_latest") == 1)
        .drop("row_number_latest")
    )

    hourly = (
        valid.where(F.col("event_type") != "heartbeat")
        .groupBy(F.window("event_timestamp", "1 hour"), "region_name")
        .agg(
            F.sum("kwh").alias("total_kwh"),
            F.count("*").alias("event_count"),
            F.countDistinct("meter_id").alias("unique_meter_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "region_name",
            F.round("total_kwh", 4).alias("total_kwh"),
            "event_count",
            "unique_meter_count",
            F.current_timestamp().alias("processed_timestamp"),
        )
    )

    valid_count = valid.count()
    rejected_count = rejected.count()
    anomaly_count = anomalies.count()

    valid.write.mode("append").parquet(str(paths.silver_clean_streaming_events))
    rejected.write.mode("append").parquet(str(paths.silver_rejected_streaming_events))
    latest.write.mode("append").parquet(str(paths.gold_latest_meter_readings))
    anomalies.write.mode("append").parquet(str(paths.gold_streaming_anomaly_events))
    hourly.write.mode("append").parquet(str(paths.gold_hourly_streaming_consumption))
    write_metrics(spark, paths.gold_streaming_pipeline_metrics, batch_id, total_count, valid_count, rejected_count, anomaly_count)

    log_batch_counts(batch_id, total_count, valid_count, rejected_count, anomaly_count)
    prepared.unpersist()


def run_stream(
    *,
    source_format: str,
    source_path: Path,
    checkpoint_path: Path,
    available_now: bool,
    duration_seconds: int,
) -> None:
    paths = get_streaming_paths()
    paths.create_directories()

    spark = create_streaming_spark("stream_bronze_to_silver_gold")
    raw_stream = build_raw_stream(spark, source_format, source_path)
    log_consumer_start("stream_bronze_to_silver_gold", str(source_path), str(checkpoint_path))

    writer = raw_stream.writeStream.foreachBatch(lambda df, batch_id: process_micro_batch(df, batch_id, paths)).option(
        "checkpointLocation", str(checkpoint_path)
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="10 seconds")
    query = writer.start()

    if available_now:
        query.awaitTermination()
    elif duration_seconds > 0:
        query.awaitTermination(duration_seconds)
        query.stop()
    else:
        query.awaitTermination()

    log_consumer_end("stream_bronze_to_silver_gold")
    spark.stop()


def build_parser() -> argparse.ArgumentParser:
    paths = get_streaming_paths()
    parser = argparse.ArgumentParser(description="Process streaming bronze events into silver and gold.")
    parser.add_argument("--source-format", choices=["bronze_parquet", "json_landing"], default="bronze_parquet")
    parser.add_argument("--source-path", default=str(paths.bronze_streaming_events))
    parser.add_argument("--checkpoint-path", default=str(paths.checkpoint_bronze_to_silver_gold))
    parser.add_argument("--available-now", action="store_true", default=True)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--duration-seconds", type=int, default=0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_stream(
        source_format=args.source_format,
        source_path=resolve_project_path(args.source_path),
        checkpoint_path=resolve_project_path(args.checkpoint_path),
        available_now=not args.continuous and args.available_now,
        duration_seconds=args.duration_seconds,
    )


if __name__ == "__main__":
    main()
