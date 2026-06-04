# Event Hubs To Kafka Mapping

Azure Event Hubs is not Kafka, but it can play a Kafka-like event ingestion role in many architectures.

| Kafka Concept | Azure Event Hubs Concept |
| --- | --- |
| Kafka topic | Event Hub |
| Kafka producer | Event Hubs producer |
| Kafka consumer group | Event Hubs consumer group |
| Kafka partition | Event Hubs partition |
| Kafka offset | Event Hubs offset / event position |
| Kafka broker endpoint | Event Hubs endpoint |

## Why This Project Uses Event Hubs

This is an Azure-style portfolio project. Event Hubs maps naturally to the production ingestion service you would use for high-throughput telemetry such as smart-meter readings.

The local emulator lets the project demonstrate:

- event-driven ingestion
- partitions
- consumer groups
- producer/consumer separation
- local development without Azure cost

## Kafka-Compatible Spark Consumption

The Event Hubs Emulator exposes a Kafka-compatible endpoint on port `9092`.

The Phase 4 consumer can use Spark's Kafka source in Event Hubs mode:

```powershell
python streaming/consumer/stream_eventhub_to_bronze.py --mode eventhub --continuous
```

This may require Spark to resolve a Kafka connector package. If that is not reliable locally, use the file-mode fallback.

For the Event Hubs Emulator Kafka endpoint, the Spark consumer uses:

- `kafka.security.protocol=SASL_PLAINTEXT`
- `kafka.sasl.mechanism=PLAIN`
- username `$ConnectionString`
- password set to the emulator connection string
