# Streaming Spark Jobs

Phase 4 Structured Streaming jobs live here.

## Main Job

```powershell
python spark_jobs/streaming/stream_bronze_to_silver_gold.py
```

Default behavior:

- Reads raw bronze streaming events from `data_lake/bronze/streaming_events/`.
- Uses checkpointing under `checkpoints/bronze_to_silver_gold/`.
- Writes clean events, rejected events, latest readings, anomalies, hourly aggregates, and metrics.

For direct local file landing mode:

```powershell
python spark_jobs/streaming/stream_bronze_to_silver_gold.py --source-format json_landing --source-path data_lake/bronze/streaming_events_landing
```

The wrapper at `streaming/consumer/stream_bronze_to_silver_gold.py` calls this same job.
