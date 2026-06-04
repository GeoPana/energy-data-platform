from streaming.common.event_generation import (
    generate_duplicate_event,
    generate_event_batch,
    generate_valid_event,
)
from streaming.common.event_validation import REQUIRED_FIELDS, is_valid_event


def test_valid_generated_event_has_required_fields() -> None:
    event = generate_valid_event()

    for field in REQUIRED_FIELDS:
        assert field in event
        assert event[field] not in (None, "")

    assert is_valid_event(event)
    assert 0.05 <= event["kwh"] <= 3.5
    assert 220.0 <= event["voltage"] <= 245.0


def test_duplicate_event_helper_creates_same_event_id() -> None:
    original = generate_valid_event()
    duplicate = generate_duplicate_event(original)

    assert duplicate["event_id"] == original["event_id"]
    assert duplicate["meter_id"] == original["meter_id"]
    assert duplicate["producer_timestamp"] != original["producer_timestamp"]


def test_event_batch_contains_requested_number_of_events() -> None:
    events = generate_event_batch(20, include_bad_events=True)

    assert len(events) == 20
    assert all("event_id" in event for event in events)
