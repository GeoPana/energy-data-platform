"""Capture streaming events from Event Hubs Emulator or local files into bronze."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streaming.common.streaming_logging import log_consumer_end, log_consumer_start
from streaming.common.streaming_paths import get_streaming_paths, resolve_project_path


def create_streaming_spark(app_name: str, include_kafka_package: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
    )
    if include_kafka_package:
        kafka_package = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        builder = builder.config("spark.jars.packages", kafka_package)
    return builder.getOrCreate()


def build_file_stream(spark: SparkSession, input_path: Path):
    return (
        spark.readStream.format("text")
        .load(str(input_path))
        .select(
            F.col("value").alias("raw_event_payload"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.lit("local_file_stream").alias("source_system"),
            F.lit(None).cast("string").alias("partition_id"),
            F.lit(None).cast("string").alias("offset"),
            F.input_file_name().alias("producer_metadata"),
        )
    )


def build_eventhub_kafka_stream(spark: SparkSession):
    topic_name = os.getenv("EVENTHUB_NAME", "smart-meter-events")
    bootstrap_servers = os.getenv("EVENTHUB_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    starting_offsets = os.getenv("EVENTHUB_KAFKA_STARTING_OFFSETS", "latest")
    connection_string = os.getenv(
        "EVENTHUB_CONNECTION_STR",
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
        "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;",
    )
    jaas_config = (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{connection_string}";'
    )

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", starting_offsets)
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .load()
        .select(
            F.col("value").cast("string").alias("raw_event_payload"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.lit("eventhubs_emulator_kafka").alias("source_system"),
            F.col("partition").cast("string").alias("partition_id"),
            F.col("offset").cast("string").alias("offset"),
            F.to_json(
                F.struct(
                    F.col("topic"),
                    F.col("timestamp").cast("string").alias("kafka_timestamp"),
                    F.col("timestampType").alias("timestamp_type"),
                )
            ).alias("producer_metadata"),
        )
    )


def run_stream(
    *,
    mode: str,
    input_path: Path,
    output_path: Path,
    checkpoint_path: Path,
    available_now: bool,
    duration_seconds: int,
) -> None:
    include_kafka_package = mode == "eventhub"
    spark = create_streaming_spark("stream_eventhub_to_bronze", include_kafka_package=include_kafka_package)

    if mode == "file":
        source_df = build_file_stream(spark, input_path)
        source_label = str(input_path)
    elif mode == "eventhub":
        source_df = build_eventhub_kafka_stream(spark)
        source_label = os.getenv("EVENTHUB_NAME", "smart-meter-events")
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    log_consumer_start("stream_eventhub_to_bronze", source_label, str(checkpoint_path))

    writer = (
        source_df.writeStream.format("parquet")
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
    )
    writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime="10 seconds")
    query = writer.start(str(output_path))

    if available_now:
        query.awaitTermination()
    elif duration_seconds > 0:
        query.awaitTermination(duration_seconds)
        query.stop()
    else:
        query.awaitTermination()

    log_consumer_end("stream_eventhub_to_bronze")
    spark.stop()


def build_parser() -> argparse.ArgumentParser:
    paths = get_streaming_paths()
    parser = argparse.ArgumentParser(description="Capture raw streaming events into bronze.")
    parser.add_argument("--mode", choices=["file", "eventhub"], default="file")
    parser.add_argument("--input-path", default=str(paths.bronze_streaming_events_landing))
    parser.add_argument("--output-path", default=str(paths.bronze_streaming_events))
    parser.add_argument("--checkpoint-path", default=str(paths.checkpoint_eventhub_to_bronze))
    parser.add_argument("--available-now", action="store_true", default=True)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--duration-seconds", type=int, default=0)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_stream(
        mode=args.mode,
        input_path=resolve_project_path(args.input_path),
        output_path=resolve_project_path(args.output_path),
        checkpoint_path=resolve_project_path(args.checkpoint_path),
        available_now=not args.continuous and args.available_now,
        duration_seconds=args.duration_seconds,
    )


if __name__ == "__main__":
    main()
