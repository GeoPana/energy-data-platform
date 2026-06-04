"""Synthetic smart-meter event generation for Phase 4 streaming."""

from __future__ import annotations

import copy
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from streaming.common.event_validation import VALID_REGIONS


SOURCE_SYSTEM = "smart_meter_stream"
METER_COUNT = 100


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def meter_customer_pair(index: int | None = None) -> tuple[str, str]:
    selected = index or random.randint(1, METER_COUNT)
    return f"MTR-LON-{selected:03d}", f"CUST-LON-{selected:03d}"


def region_for_meter_id(meter_id: str) -> str:
    try:
        meter_number = int(meter_id.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        meter_number = random.randint(1, METER_COUNT)
    regions = sorted(VALID_REGIONS)
    return regions[(meter_number - 1) % len(regions)]


def base_event(
    *,
    event_type: str = "reading",
    kwh: float | None = None,
    meter_index: int | None = None,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    meter_id, customer_id = meter_customer_pair(meter_index)
    event_timestamp = timestamp or utc_now()
    normal_kwh = round(random.uniform(0.05, 3.5), 3)
    voltage = round(random.uniform(220.0, 245.0), 1)
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "meter_id": meter_id,
        "customer_id": customer_id,
        "region_name": region_for_meter_id(meter_id),
        "event_timestamp": iso_z(event_timestamp),
        "kwh": round(kwh if kwh is not None else normal_kwh, 3),
        "voltage": voltage,
        "event_type": event_type,
        "source_system": SOURCE_SYSTEM,
        "producer_timestamp": iso_z(utc_now()),
    }


def generate_valid_event() -> dict[str, Any]:
    return base_event(event_type="reading")


def generate_anomaly_event() -> dict[str, Any]:
    return base_event(event_type="anomaly", kwh=round(random.uniform(10.0, 40.0), 3))


def generate_missing_meter_event() -> dict[str, Any]:
    event = generate_valid_event()
    event["meter_id"] = ""
    return event


def generate_negative_kwh_event() -> dict[str, Any]:
    event = generate_valid_event()
    event["kwh"] = round(random.uniform(-5.0, -0.01), 3)
    return event


def generate_invalid_timestamp_event() -> dict[str, Any]:
    event = generate_valid_event()
    event["event_timestamp"] = "not-a-timestamp"
    return event


def generate_duplicate_event(previous_event: dict[str, Any]) -> dict[str, Any]:
    duplicate = copy.deepcopy(previous_event)
    duplicate["producer_timestamp"] = iso_z(utc_now() + timedelta(seconds=1))
    return duplicate


def generate_heartbeat_event() -> dict[str, Any]:
    event = base_event(event_type="heartbeat", kwh=0.0)
    event["voltage"] = None
    return event


def generate_event_batch(batch_size: int, include_bad_events: bool = True) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for index in range(batch_size):
        if include_bad_events and index > 0 and index % 25 == 0:
            events.append(generate_duplicate_event(events[-1]))
        elif include_bad_events and index % 17 == 0:
            events.append(generate_missing_meter_event())
        elif include_bad_events and index % 19 == 0:
            events.append(generate_negative_kwh_event())
        elif include_bad_events and index % 23 == 0:
            events.append(generate_invalid_timestamp_event())
        elif index % 13 == 0:
            events.append(generate_anomaly_event())
        elif index % 29 == 0:
            events.append(generate_heartbeat_event())
        else:
            events.append(generate_valid_event())
    return events
