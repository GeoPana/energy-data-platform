# Phase 4 Streaming

Phase 4 adds a local real-time streaming path.

Primary architecture:

```text
Python smart-meter producer
    -> Azure Event Hubs Emulator
    -> PySpark Structured Streaming
    -> bronze streaming events
    -> silver clean/rejected streaming events
    -> gold latest readings, anomalies, hourly consumption
```

Fallback architecture:

```text
Python smart-meter producer
    -> local JSON landing files
    -> PySpark Structured Streaming
    -> silver/gold streaming outputs
```

## Quick Local File Mode

Generate events:

```powershell
python streaming/producer/smart_meter_producer.py --mode file --events-per-second 5 --duration-seconds 30
```

Process landing files directly:

```powershell
python streaming/consumer/stream_bronze_to_silver_gold.py --source-format json_landing --source-path data_lake/bronze/streaming_events_landing --available-now
```

## Bronze Capture Path

If you want a bronze raw capture step:

```powershell
python streaming/consumer/stream_eventhub_to_bronze.py --mode file --available-now
python streaming/consumer/stream_bronze_to_silver_gold.py --available-now
```

## Event Hubs Emulator Path

Start the emulator:

```powershell
$env:ACCEPT_EULA = "Y"
$env:CONFIG_PATH = "./Config.json"
docker compose -f streaming/emulator/docker-compose.eventhubs.yml up
```

Run producer:

```powershell
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$env:EVENTHUB_NAME = "smart-meter-events"
python streaming/producer/smart_meter_producer.py --mode eventhub --events-per-second 5 --duration-seconds 60
```

Run Event Hubs to bronze:

```powershell
$env:EVENTHUB_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
python streaming/consumer/stream_eventhub_to_bronze.py --mode eventhub --continuous
```

## Optional SQL Server Load

```powershell
python streaming/consumer/load_streaming_gold_to_sql_server.py
```

This expects the SQL Server warehouse from Phase 1 and dimensions loaded by Phase 2.
