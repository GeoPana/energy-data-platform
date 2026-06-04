# Streaming Consumers

Phase 4 consumers read smart-meter events and write local lakehouse outputs.

## File Landing To Bronze

Capture newline-delimited JSON files from the file-mode producer into bronze Parquet:

```powershell
python streaming/consumer/stream_eventhub_to_bronze.py --mode file --available-now
```

Output:

```text
data_lake/bronze/streaming_events/
```

Checkpoint:

```text
checkpoints/eventhub_to_bronze/
```

## Event Hubs Emulator To Bronze

Event Hubs mode uses the Kafka-compatible endpoint exposed by the emulator.

```powershell
python streaming/consumer/stream_eventhub_to_bronze.py --mode eventhub --continuous
```

This may require Spark to download the Kafka connector package. If that is fragile in your local environment, use file mode.

## Bronze To Silver/Gold

Process bronze events into silver and gold:

```powershell
python streaming/consumer/stream_bronze_to_silver_gold.py --available-now
```

For direct local file landing mode:

```powershell
python streaming/consumer/stream_bronze_to_silver_gold.py --source-format json_landing --source-path data_lake/bronze/streaming_events_landing --available-now
```

Outputs:

- `data_lake/silver/clean_streaming_events/`
- `data_lake/silver/rejected_streaming_events/`
- `data_lake/gold/latest_meter_readings/`
- `data_lake/gold/streaming_anomaly_events/`
- `data_lake/gold/hourly_streaming_consumption/`
- `data_lake/gold/streaming_pipeline_metrics/`

## Optional SQL Server Load

```powershell
python streaming/consumer/load_streaming_gold_to_sql_server.py
```

This expects Phase 1 warehouse tables and Phase 2 dimensions to exist.
