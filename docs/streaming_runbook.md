# Streaming Runbook

This runbook explains how to operate the Phase 4 streaming pipeline locally.

## Update The Environment

```powershell
conda env update -f environment.yml --prune
conda activate energy-data-platform
```

## Start Event Hubs Emulator

```powershell
$env:ACCEPT_EULA = "Y"
$env:CONFIG_PATH = "./Config.json"
docker compose -f streaming/emulator/docker-compose.eventhubs.yml up
```

## Run Producer In File Mode

```powershell
python streaming/producer/smart_meter_producer.py --mode file --events-per-second 5 --duration-seconds 60
```

## Run Producer In Event Hubs Mode

```powershell
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$env:EVENTHUB_NAME = "smart-meter-events"
python streaming/producer/smart_meter_producer.py --mode eventhub --events-per-second 5 --duration-seconds 60
```

## Run Streaming Consumer In File Mode

Direct landing to silver/gold:

```powershell
python streaming/consumer/stream_bronze_to_silver_gold.py --source-format json_landing --source-path data_lake/bronze/streaming_events_landing --available-now
```

With bronze capture:

```powershell
python streaming/consumer/stream_eventhub_to_bronze.py --mode file --available-now
python streaming/consumer/stream_bronze_to_silver_gold.py --available-now
```

## Run Event Hubs To Bronze

```powershell
$env:EVENTHUB_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:EVENTHUB_NAME = "smart-meter-events"
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
python streaming/consumer/stream_eventhub_to_bronze.py --mode eventhub --continuous
```

## Optional SQL Server Load

```powershell
python streaming/consumer/load_streaming_gold_to_sql_server.py
```

This expects:

- Phase 1 SQL Server warehouse tables.
- Phase 2 dimensions loaded into SQL Server.
- Microsoft ODBC Driver 18 installed locally.

## Inspect Outputs

```powershell
Get-ChildItem -Recurse data_lake\silver\clean_streaming_events
Get-ChildItem -Recurse data_lake\silver\rejected_streaming_events
Get-ChildItem -Recurse data_lake\gold\streaming_anomaly_events
Get-ChildItem -Recurse data_lake\gold\hourly_streaming_consumption
```

## Reset Checkpoints

Stop any running streaming jobs first. Then delete checkpoint contents:

```powershell
Remove-Item -Recurse -Force checkpoints\eventhub_to_bronze\*
Remove-Item -Recurse -Force checkpoints\bronze_to_silver_gold\*
```

Keep the folders themselves if you want the `.gitkeep` placeholders to remain.

## Common Failures

### Event Hubs Emulator Will Not Start

Check:

- Docker Desktop is running.
- `ACCEPT_EULA=Y`.
- `CONFIG_PATH` points to `streaming/emulator/Config.json`.
- Ports `5672`, `9092`, and `5300` are available.

### Producer Event Hubs Mode Fails

Check:

- `azure-eventhub` is installed from `environment.yml`.
- `EVENTHUB_CONNECTION_STR` is set.
- `EVENTHUB_NAME=smart-meter-events`.
- Emulator is running.

### Spark Kafka/Event Hubs Consumer Fails

Use file mode first. The Kafka-compatible path may require Spark connector package resolution.

### No New Files Are Processed

Check:

- Source path is correct.
- Checkpoint has not already processed those files.
- Delete the checkpoint only when you intentionally want to reprocess.
