from streaming.common.event_generation import generate_anomaly_event, generate_valid_event
from streaming.common.event_validation import classify_event, detect_simple_anomaly


def test_high_kwh_is_classified_as_anomaly() -> None:
    event = generate_valid_event()
    event["kwh"] = 15.0

    assert detect_simple_anomaly(event)
    assert classify_event(event) == "anomaly"


def test_event_type_anomaly_is_classified_as_anomaly() -> None:
    event = generate_anomaly_event()

    assert event["event_type"] == "anomaly"
    assert detect_simple_anomaly(event)
    assert classify_event(event) == "anomaly"


def test_normal_reading_is_not_anomaly() -> None:
    event = generate_valid_event()
    event["kwh"] = 0.75

    assert not detect_simple_anomaly(event)
    assert classify_event(event) == "reading"
