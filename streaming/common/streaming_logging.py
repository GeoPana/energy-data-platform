"""Simple structured logging helpers for Phase 4 streaming scripts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log_event(event_name: str, **fields: Any) -> None:
    payload = {"event": event_name, "timestamp": utc_now_text(), **fields}
    print(json.dumps(payload, sort_keys=True, default=str))


def log_producer_start(mode: str, target: str, events_per_second: int, duration_seconds: int) -> None:
    log_event(
        "producer_start",
        mode=mode,
        target=target,
        events_per_second=events_per_second,
        duration_seconds=duration_seconds,
    )


def log_producer_end(total_events: int, bad_events: int, anomaly_events: int) -> None:
    log_event("producer_end", total_events=total_events, bad_events=bad_events, anomaly_events=anomaly_events)


def log_consumer_start(consumer_name: str, source: str, checkpoint: str) -> None:
    log_event("consumer_start", consumer_name=consumer_name, source=source, checkpoint=checkpoint)


def log_consumer_end(consumer_name: str) -> None:
    log_event("consumer_end", consumer_name=consumer_name)


def log_batch_counts(batch_id: int, total: int, valid: int, rejected: int, anomalies: int) -> None:
    log_event(
        "streaming_batch_counts",
        batch_id=batch_id,
        total_events=total,
        valid_events=valid,
        rejected_events=rejected,
        anomaly_events=anomalies,
    )
