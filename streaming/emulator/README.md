# Azure Event Hubs Emulator

Phase 4 uses the Azure Event Hubs Emulator to model an Azure Event Hubs ingestion layer locally.

The emulator is useful because it lets the project demonstrate Event Hubs-style development without a cloud subscription or paid Azure resources.

## Start The Emulator

From the repository root:

```powershell
$env:ACCEPT_EULA = "Y"
$env:CONFIG_PATH = "./Config.json"
docker compose -f streaming/emulator/docker-compose.eventhubs.yml up
```

The emulator uses:

- Event Hub namespace: `emulatorNs1`
- Event Hub name: `smart-meter-events`
- Consumer group: `spark-consumer`
- Partitions: `2`

## Ports

- `5672`: AMQP
- `9092`: Kafka-compatible endpoint
- `5300`: emulator service endpoint

`CONFIG_PATH` is resolved relative to the compose file directory when using the command above. Use an absolute path if your Docker Compose version behaves differently.

## Producer Configuration

For Python Event Hubs producer mode, set:

```powershell
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$env:EVENTHUB_NAME = "smart-meter-events"
```

For a producer running in another container, the host may need to be `eventhubs-emulator` or `host.docker.internal` instead of `localhost`.

## Structured Streaming Configuration

The Spark consumer uses the Kafka-compatible endpoint for Event Hubs mode:

```powershell
$env:EVENTHUB_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:EVENTHUB_NAME = "smart-meter-events"
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
python streaming/consumer/stream_eventhub_to_bronze.py --mode eventhub --continuous
```

This path may require Spark to resolve the Kafka connector package. If local package resolution is unreliable, use file mode.

## Fallback File Mode

File mode is the most reproducible local path:

```powershell
python streaming/producer/smart_meter_producer.py --mode file --events-per-second 5 --duration-seconds 60
python streaming/consumer/stream_bronze_to_silver_gold.py --source-format json_landing --source-path data_lake/bronze/streaming_events_landing --available-now
```

## Known Limitations

- The emulator is for local development, not production parity.
- Kafka-compatible Spark consumption may need additional connector packages.
- Windows path mounting may require absolute paths or escaped backslashes for `CONFIG_PATH`.
- Use the file-mode fallback when the emulator or Spark connector setup blocks local progress.
