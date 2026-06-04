"""Generate bronze metadata CSV files for Phase 2 local development."""

from __future__ import annotations

import csv
import random
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common.paths import get_project_paths


LONDON_REGIONS = [
    {"region_name": "North London", "country": "United Kingdom", "postcode_area": "N"},
    {"region_name": "South London", "country": "United Kingdom", "postcode_area": "SE/SW"},
    {"region_name": "East London", "country": "United Kingdom", "postcode_area": "E"},
    {"region_name": "West London", "country": "United Kingdom", "postcode_area": "W"},
    {"region_name": "Central London", "country": "United Kingdom", "postcode_area": "EC/WC"},
]

CUSTOMER_SEGMENTS = ["Residential", "Residential", "Residential", "Small Business", "Public Sector"]
RESIDENTIAL_DWELLINGS = ["Flat", "Terraced", "Semi-detached", "Detached"]
BUSINESS_DWELLINGS = ["Office", "Retail Unit", "Community Centre"]


def clear_generated_files(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child in path.iterdir():
        if child.name == ".gitkeep":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def write_csv(path: Path, rows: Iterable[dict[str, object]], fieldnames: list[str]) -> int:
    materialized_rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materialized_rows)
    return len(materialized_rows)


def build_regions() -> list[dict[str, str]]:
    return [dict(region) for region in LONDON_REGIONS]


def build_tariffs() -> list[dict[str, object]]:
    return [
        {
            "tariff_id": "T-STD-2026",
            "tariff_name": "Standard Variable 2026",
            "valid_from": "2026-01-01",
            "valid_to": "",
            "price_per_kwh": 0.2860,
            "standing_charge_daily": 0.5300,
            "is_active": "true",
        },
        {
            "tariff_id": "T-ECO-2026",
            "tariff_name": "Eco Saver 2026",
            "valid_from": "2026-01-01",
            "valid_to": "",
            "price_per_kwh": 0.2510,
            "standing_charge_daily": 0.4900,
            "is_active": "true",
        },
        {
            "tariff_id": "T-BIZ-2026",
            "tariff_name": "Small Business Flex 2026",
            "valid_from": "2026-01-01",
            "valid_to": "",
            "price_per_kwh": 0.3150,
            "standing_charge_daily": 0.7200,
            "is_active": "true",
        },
    ]


def build_customers(customer_count: int = 100, seed: int = 42) -> list[dict[str, object]]:
    rng = random.Random(seed)
    customers: list[dict[str, object]] = []

    for index in range(1, customer_count + 1):
        region = LONDON_REGIONS[(index - 1) % len(LONDON_REGIONS)]
        segment = rng.choice(CUSTOMER_SEGMENTS)
        is_business_like = segment in {"Small Business", "Public Sector"}
        signup_date = date(2021, 1, 1) + timedelta(days=rng.randint(0, 1700))

        customers.append(
            {
                "customer_id": f"CUST-LON-{index:03d}",
                "customer_segment": segment,
                "household_size": 1 if is_business_like else rng.randint(1, 5),
                "dwelling_type": rng.choice(BUSINESS_DWELLINGS if is_business_like else RESIDENTIAL_DWELLINGS),
                "region_name": region["region_name"],
                "country": region["country"],
                "postcode_area": region["postcode_area"],
                "signup_date": signup_date.isoformat(),
                "is_active": "true" if rng.random() > 0.03 else "false",
            }
        )

    return customers


def build_meters(customers: list[dict[str, object]], seed: int = 43) -> list[dict[str, object]]:
    rng = random.Random(seed)
    meters: list[dict[str, object]] = []

    for index, customer in enumerate(customers, start=1):
        segment = str(customer["customer_segment"])
        signup_date = date.fromisoformat(str(customer["signup_date"]))

        if segment == "Small Business":
            meter_type = "Half-Hourly Business Meter"
        elif segment == "Public Sector":
            meter_type = "Commercial Smart Meter"
        else:
            meter_type = rng.choice(["SMETS1 Smart Meter", "SMETS2 Smart Meter"])

        meters.append(
            {
                "meter_id": f"MTR-LON-{index:03d}",
                "customer_id": customer["customer_id"],
                "region_name": customer["region_name"],
                "meter_type": meter_type,
                "installation_date": (signup_date + timedelta(days=rng.randint(7, 90))).isoformat(),
                "is_active": customer["is_active"],
            }
        )

    return meters


def main() -> None:
    paths = get_project_paths()
    paths.create_lakehouse_directories()

    clear_generated_files(paths.bronze_customer_metadata)
    clear_generated_files(paths.bronze_meter_metadata)
    clear_generated_files(paths.bronze_tariff)

    regions = build_regions()
    customers = build_customers()
    meters = build_meters(customers)
    tariffs = build_tariffs()

    region_count = write_csv(
        paths.bronze_regions_file,
        regions,
        ["region_name", "country", "postcode_area"],
    )
    customer_count = write_csv(
        paths.bronze_customers_file,
        customers,
        [
            "customer_id",
            "customer_segment",
            "household_size",
            "dwelling_type",
            "region_name",
            "country",
            "postcode_area",
            "signup_date",
            "is_active",
        ],
    )
    meter_count = write_csv(
        paths.bronze_meters_file,
        meters,
        ["meter_id", "customer_id", "region_name", "meter_type", "installation_date", "is_active"],
    )
    tariff_count = write_csv(
        paths.bronze_tariffs_file,
        tariffs,
        [
            "tariff_id",
            "tariff_name",
            "valid_from",
            "valid_to",
            "price_per_kwh",
            "standing_charge_daily",
            "is_active",
        ],
    )

    print(
        "Generated bronze metadata: "
        f"{region_count} regions, {customer_count} customers, {meter_count} meters, {tariff_count} tariffs"
    )


if __name__ == "__main__":
    main()
