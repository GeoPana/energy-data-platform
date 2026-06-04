"""Reusable validation helpers for Phase 2 data quality."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


VALID_LONDON_REGIONS = {
    "North London",
    "South London",
    "East London",
    "West London",
    "Central London",
}


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


def is_timestamp_parseable(value: Any) -> bool:
    return parse_timestamp(value) is not None


def is_non_negative_number(value: Any) -> bool:
    if not is_present(value):
        return False
    try:
        return float(value) >= 0
    except (TypeError, ValueError):
        return False


def is_valid_region(region_name: Any) -> bool:
    return str(region_name).strip() in VALID_LONDON_REGIONS if is_present(region_name) else False


def consumption_rejection_reasons(row: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []

    if not is_present(row.get("meter_id")):
        reasons.append("Missing meter_id")
    if not is_present(row.get("customer_id")):
        reasons.append("Missing customer_id")
    if not is_timestamp_parseable(row.get("reading_timestamp")):
        reasons.append("Invalid reading_timestamp")
    if not is_present(row.get("kwh")):
        reasons.append("Missing kwh")
    elif not is_non_negative_number(row.get("kwh")):
        reasons.append("Negative or invalid kwh")
    if not is_valid_region(row.get("region_name")):
        reasons.append("Invalid region_name")

    return reasons


def duplicate_key(row: Mapping[str, Any], key_columns: Sequence[str]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in key_columns)


def find_duplicate_keys(
    records: Iterable[Mapping[str, Any]],
    key_columns: Sequence[str],
) -> set[tuple[Any, ...]]:
    counts = Counter(duplicate_key(record, key_columns) for record in records)
    return {key for key, count in counts.items() if count > 1}


def format_rejected_record(
    *,
    source_system: str,
    source_reference: str,
    row: Mapping[str, Any],
    rejection_reasons: Sequence[str],
    batch_id: str | None = None,
) -> dict[str, Any]:
    return {
        "source_system": source_system,
        "source_reference": source_reference,
        "raw_payload": json.dumps(dict(row), sort_keys=True, default=str),
        "rejection_reason": "; ".join(rejection_reasons),
        "rejected_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "batch_id": batch_id,
    }


def spark_rejection_reason(reason_conditions: Sequence[tuple[Any, str]]) -> Any:
    """Build a Spark rejection_reason expression from condition/reason pairs."""
    from pyspark.sql import functions as F

    return F.concat_ws(
        "; ",
        *[F.when(condition, F.lit(reason)) for condition, reason in reason_conditions],
    )
