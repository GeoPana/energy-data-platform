from streaming.common.event_generation import (
    generate_invalid_timestamp_event,
    generate_missing_meter_event,
    generate_negative_kwh_event,
    generate_valid_event,
)
from streaming.common.event_validation import (
    is_valid_event,
    validate_kwh,
    validate_region,
    validate_timestamp,
    validation_reasons,
)


def test_missing_meter_id_fails_validation() -> None:
    event = generate_missing_meter_event()

    assert not is_valid_event(event)
    assert any("Missing required fields" in reason for reason in validation_reasons(event))


def test_negative_kwh_fails_validation() -> None:
    event = generate_negative_kwh_event()

    assert not validate_kwh(event)
    assert "Negative or invalid kwh" in validation_reasons(event)


def test_invalid_timestamp_fails_validation() -> None:
    event = generate_invalid_timestamp_event()

    assert not validate_timestamp(event)
    assert "Invalid event_timestamp" in validation_reasons(event)


def test_invalid_region_fails_validation() -> None:
    event = generate_valid_event()
    event["region_name"] = "Unknown London"

    assert not validate_region(event)
    assert "Invalid region_name" in validation_reasons(event)
