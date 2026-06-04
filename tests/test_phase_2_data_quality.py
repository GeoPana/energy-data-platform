from spark_jobs.common.data_quality import (
    consumption_rejection_reasons,
    find_duplicate_keys,
    is_non_negative_number,
    is_timestamp_parseable,
    is_valid_region,
)


def test_negative_kwh_is_rejected() -> None:
    row = {
        "meter_id": "MTR-LON-001",
        "customer_id": "CUST-LON-001",
        "region_name": "North London",
        "reading_timestamp": "2026-01-01T00:00:00",
        "kwh": "-0.25",
    }

    assert "Negative or invalid kwh" in consumption_rejection_reasons(row)
    assert not is_non_negative_number(row["kwh"])


def test_null_meter_id_is_rejected() -> None:
    row = {
        "meter_id": "",
        "customer_id": "CUST-LON-001",
        "region_name": "North London",
        "reading_timestamp": "2026-01-01T00:00:00",
        "kwh": "0.25",
    }

    assert "Missing meter_id" in consumption_rejection_reasons(row)


def test_valid_and_invalid_region_checks() -> None:
    assert is_valid_region("North London")
    assert not is_valid_region("Unknown London")


def test_timestamp_parsing_behavior() -> None:
    assert is_timestamp_parseable("2026-01-01T00:00:00")
    assert is_timestamp_parseable("2026-01-01T00:00:00+00:00")
    assert not is_timestamp_parseable("not-a-timestamp")


def test_duplicate_key_detection_logic() -> None:
    records = [
        {"meter_id": "MTR-LON-001", "reading_timestamp": "2026-01-01T00:00:00"},
        {"meter_id": "MTR-LON-001", "reading_timestamp": "2026-01-01T00:00:00"},
        {"meter_id": "MTR-LON-002", "reading_timestamp": "2026-01-01T00:00:00"},
    ]

    duplicates = find_duplicate_keys(records, ["meter_id", "reading_timestamp"])

    assert duplicates == {("MTR-LON-001", "2026-01-01T00:00:00")}
