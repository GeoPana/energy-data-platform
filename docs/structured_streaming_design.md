# Structured Streaming Design

Phase 4 uses PySpark Structured Streaming to process smart-meter events.

## readStream And writeStream

Structured Streaming uses `readStream` to define an input stream and `writeStream` to define the output.

This project supports:

- text file streaming from local JSON landing files
- Parquet streaming from bronze raw events
- Kafka-compatible Event Hubs ingestion where local setup allows it

## Micro-Batches

The project uses Spark micro-batches. Each micro-batch is processed with `foreachBatch`.

This keeps the first streaming implementation understandable:

- parse raw JSON
- validate records
- split valid and invalid events
- deduplicate by `event_id` within the micro-batch
- write silver events
- write rejected events
- compute latest readings
- compute anomaly events
- compute hourly consumption
- write metrics

## Checkpointing

Checkpoints are stored under:

```text
checkpoints/eventhub_to_bronze/
checkpoints/bronze_to_silver_gold/
```

Checkpoints allow Spark to track what files or offsets have already been processed.

For local development, deleting a checkpoint folder forces Spark to reprocess the source data.

## Schema Validation

The event schema is explicit:

```text
streaming/schemas/smart_meter_event_schema.json
```

Spark applies an explicit `StructType` while parsing JSON. Invalid or malformed records are preserved as rejected events.

## Why Silver And Gold Are Written From Streaming

The streaming job writes silver and gold outputs because different consumers need different shapes:

- Silver clean events are useful for audit and replay.
- Rejected events are useful for data-quality triage.
- Latest readings support operational lookup patterns.
- Anomaly events support alerting and investigation.
- Hourly consumption supports near-real-time analytics.
