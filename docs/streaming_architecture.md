# Streaming Architecture

Phase 4 supports a primary Event Hubs Emulator path and a reliable local file-mode fallback.

## Primary Mode

```mermaid
flowchart LR
    producer[Python smart-meter producer]
    eventhubs[Azure Event Hubs Emulator]
    capture[Structured Streaming capture]
    bronze[Bronze streaming events]
    processor[Structured Streaming validation]
    silver[Silver clean/rejected events]
    gold[Gold latest readings/anomalies/hourly consumption]
    sql[SQL Server warehouse]

    producer --> eventhubs
    eventhubs --> capture
    capture --> bronze
    bronze --> processor
    processor --> silver
    processor --> gold
    gold --> sql
```

## Fallback File Mode

```mermaid
flowchart LR
    producer[Python smart-meter producer]
    landing[Local JSON landing files]
    processor[PySpark Structured Streaming]
    silver[Silver streaming events]
    gold[Gold streaming outputs]

    producer --> landing
    landing --> processor
    processor --> silver
    processor --> gold
```

File mode is intentionally first-class because local Event Hubs and Spark connector setup can vary by machine.

## Lakehouse Outputs

Bronze:

- `data_lake/bronze/streaming_events/`
- `data_lake/bronze/streaming_events_landing/`

Silver:

- `data_lake/silver/clean_streaming_events/`
- `data_lake/silver/rejected_streaming_events/`

Gold:

- `data_lake/gold/latest_meter_readings/`
- `data_lake/gold/streaming_anomaly_events/`
- `data_lake/gold/hourly_streaming_consumption/`
- `data_lake/gold/streaming_pipeline_metrics/`

Checkpoints:

- `checkpoints/eventhub_to_bronze/`
- `checkpoints/bronze_to_silver_gold/`
