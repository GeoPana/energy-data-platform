"""Pure Python validation and anomaly helpers for smart-meter events."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping


VALID_REGIONS = {
    "North London",
    "South London",
    "East London",
    "West London",
    "Central London",
}

VALID_EVENT_TYPES = {"reading", "anomaly", "heartbeat"}

REQUIRED_FIELDS = [
    "event_id",
    "meter_id",
    "customer_id",
    "region_name",
    "event_timestamp",
    "kwh",
    "event_type",
    "source_system",
    "producer_timestamp",
]


def is_present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def parse_timestamp(value: Any) -> datetime | None:
    if not is_present(value):
        return None
    cleaned = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned)
    except ValueError:
        return None


def validate_required_fields(event: Mapping[str, Any]) -> list[str]:
    return [field for field in REQUIRED_FIELDS if not is_present(event.get(field))]


def validate_region(event: Mapping[str, Any]) -> bool:
    return str(event.get("region_name", "")).strip() in VALID_REGIONS


def validate_timestamp(event: Mapping[str, Any]) -> bool:
    return parse_timestamp(event.get("event_timestamp")) is not None


def validate_kwh(event: Mapping[str, Any]) -> bool:
    if not is_present(event.get("kwh")):
        return False
    try:
        return float(event["kwh"]) >= 0
    except (TypeError, ValueError):
        return False


def validate_voltage(event: Mapping[str, Any]) -> bool:
    if not is_present(event.get("voltage")):
        return True
    try:
        return float(event["voltage"]) > 0
    except (TypeError, ValueError):
        return False


def validate_event_type(event: Mapping[str, Any]) -> bool:
    return str(event.get("event_type", "")).strip() in VALID_EVENT_TYPES


def validation_reasons(event: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    missing_fields = validate_required_fields(event)
    if missing_fields:
        reasons.append(f"Missing required fields: {', '.join(missing_fields)}")
    if is_present(event.get("region_name")) and not validate_region(event):
        reasons.append("Invalid region_name")
    if is_present(event.get("event_timestamp")) and not validate_timestamp(event):
        reasons.append("Invalid event_timestamp")
    if is_present(event.get("kwh")) and not validate_kwh(event):
        reasons.append("Negative or invalid kwh")
    if not validate_voltage(event):
        reasons.append("Invalid voltage")
    if is_present(event.get("event_type")) and not validate_event_type(event):
        reasons.append("Invalid event_type")
    return reasons


def is_valid_event(event: Mapping[str, Any]) -> bool:
    return not validation_reasons(event)


def detect_simple_anomaly(event: Mapping[str, Any], recent_average_kwh: float | None = None) -> bool:
    if not validate_kwh(event):
        return False
    kwh = float(event["kwh"])
    if str(event.get("event_type", "")).strip() == "anomaly":
        return True
    if kwh > 10:
        return True
    if recent_average_kwh and recent_average_kwh > 0 and kwh > recent_average_kwh * 3:
        return True
    return False


def classify_event(event: Mapping[str, Any], recent_average_kwh: float | None = None) -> str:
    if not is_valid_event(event):
        return "invalid"
    if str(event.get("event_type")) == "heartbeat":
        return "heartbeat"
    if detect_simple_anomaly(event, recent_average_kwh):
        return "anomaly"
    return "reading"


def build_rejected_event(event: Mapping[str, Any], reason: str | list[str]) -> dict[str, Any]:
    reasons = reason if isinstance(reason, list) else [reason]
    return {
        "raw_event_payload": json.dumps(dict(event), sort_keys=True, default=str),
        "rejection_reason": "; ".join(reasons),
        "rejected_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_system": str(event.get("source_system") or "smart_meter_stream"),
    }
