# Streaming Data Quality

Streaming data quality focuses on catching bad events without stopping the stream.

## Validation Rules

Smart-meter events are checked for:

- missing required fields
- invalid region
- invalid timestamp
- missing or negative `kwh`
- invalid voltage
- invalid event type
- duplicate `event_id` within a micro-batch
- malformed JSON

## Rejected Events

Rejected events are written to:

```text
data_lake/silver/rejected_streaming_events/
```

Rejected event columns:

- `raw_event_payload`
- `rejection_reason`
- `rejected_at`
- `source_system`

Preserving rejected events makes the stream auditable. It also allows later reprocessing after producers are fixed.

## Duplicate Event IDs

The first implementation detects duplicates within each micro-batch.

This is enough to demonstrate the pattern locally. A production version would use watermarking and stateful deduplication across a configured event-time window.

## Anomaly Rules

Anomalies are flagged when:

- `kwh > 10`
- `event_type = anomaly`

The pure Python helper also supports a simple recent-average rule: `kwh > 3x recent average`.

The Spark job keeps the first implementation simple and transparent.

## Observability

The streaming job logs:

- total events processed
- valid events
- rejected events
- anomaly events
- checkpoint locations

It also writes metrics to:

```text
data_lake/gold/streaming_pipeline_metrics/
```
