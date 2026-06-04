from datetime import datetime

from ingestion.generate_batch_data import calculate_kwh
from ingestion.generate_metadata import build_customers, build_meters, build_regions, build_tariffs


def test_generated_metadata_has_expected_shape() -> None:
    regions = build_regions()
    customers = build_customers(customer_count=100, seed=42)
    meters = build_meters(customers, seed=43)
    tariffs = build_tariffs()

    assert len(regions) == 5
    assert len(customers) == 100
    assert len(meters) == 100
    assert len(tariffs) == 3
    assert {region["region_name"] for region in regions} == {
        "North London",
        "South London",
        "East London",
        "West London",
        "Central London",
    }
    assert all(customer["customer_id"].startswith("CUST-LON-") for customer in customers)
    assert all(meter["meter_id"].startswith("MTR-LON-") for meter in meters)


def test_residential_evening_usage_is_higher_than_overnight() -> None:
    overnight = calculate_kwh(
        "Residential",
        household_size=3,
        reading_timestamp=datetime(2026, 1, 5, 2, 0, 0),
        jitter=0,
    )
    evening = calculate_kwh(
        "Residential",
        household_size=3,
        reading_timestamp=datetime(2026, 1, 5, 19, 0, 0),
        jitter=0,
    )

    assert evening > overnight


def test_small_business_daytime_usage_is_higher_than_night() -> None:
    night = calculate_kwh(
        "Small Business",
        household_size=1,
        reading_timestamp=datetime(2026, 1, 5, 2, 0, 0),
        jitter=0,
    )
    daytime = calculate_kwh(
        "Small Business",
        household_size=1,
        reading_timestamp=datetime(2026, 1, 5, 11, 0, 0),
        jitter=0,
    )

    assert daytime > night
