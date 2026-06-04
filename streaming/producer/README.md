# Streaming Producers

Phase 4 producers create smart-meter events for local streaming development.

## File Mode

File mode is the reliable local fallback. It writes newline-delimited JSON files to:

```text
data_lake/bronze/streaming_events_landing/
```

Run:

```powershell
python streaming/producer/smart_meter_producer.py --mode file --events-per-second 5 --duration-seconds 60
```

## Event Hubs Mode

Event Hubs mode sends events to the Azure Event Hubs Emulator or a compatible Event Hubs namespace.

Required environment variables:

```powershell
$env:EVENTHUB_CONNECTION_STR = "<connection string>"
$env:EVENTHUB_NAME = "smart-meter-events"
```

Run:

```powershell
python streaming/producer/smart_meter_producer.py --mode eventhub --events-per-second 5 --duration-seconds 60
```

## Replay Batch Data

Replay Phase 2 bronze historical consumption rows as streaming events:

```powershell
python streaming/producer/replay_batch_as_stream.py --mode file --events-per-second 10 --limit 100
```

If Phase 2 bronze data is missing, run:

```powershell
python ingestion/generate_metadata.py
python ingestion/generate_batch_data.py
```
