"""Generate bronze historical half-hourly energy consumption CSV files."""

from __future__ import annotations

import csv
import random
import shutil
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from spark_jobs.common.paths import get_project_paths


FIELDNAMES = [
    "source_file",
    "batch_id",
    "meter_id",
    "customer_id",
    "region_name",
    "reading_timestamp",
    "kwh",
    "ingestion_timestamp",
]


def clear_generated_files(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    for child in path.iterdir():
        if child.name == ".gitkeep":
            continue
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run python ingestion/generate_metadata.py first.")
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def calculate_kwh(
    customer_segment: str,
    household_size: int,
    reading_timestamp: datetime,
    jitter: float = 0.0,
) -> float:
    hour_value = reading_timestamp.hour + (reading_timestamp.minute / 60)
    is_weekend = reading_timestamp.weekday() >= 5

    if customer_segment == "Small Business":
        base = 0.35
        business_load = 1.15 if 8 <= hour_value < 18 else 0.20
        value = base + business_load + jitter
    elif customer_segment == "Public Sector":
        office_load = 1.65 if 7 <= hour_value < 20 else 0.35
        weekend_adjustment = -0.30 if is_weekend else 0.0
        value = 0.55 + office_load + weekend_adjustment + jitter
    else:
        morning_peak = 0.42 if 6 <= hour_value < 9 else 0.0
        evening_peak = 0.78 if 17 <= hour_value < 23 else 0.0
        weekend_adjustment = 0.22 if is_weekend else 0.0
        value = 0.16 + (household_size * 0.085) + morning_peak + evening_peak + weekend_adjustment + jitter

    return round(max(value, 0.02), 4)


def build_consumption_row(
    *,
    meter: dict[str, str],
    customer: dict[str, str],
    reading_timestamp: datetime,
    source_file: str,
    batch_id: str,
    rng: random.Random,
) -> dict[str, str]:
    jitter = rng.uniform(-0.045, 0.065)
    kwh = calculate_kwh(
        customer["customer_segment"],
        int(customer["household_size"]),
        reading_timestamp,
        jitter,
    )
    return {
        "source_file": source_file,
        "batch_id": batch_id,
        "meter_id": meter["meter_id"],
        "customer_id": meter["customer_id"],
        "region_name": meter["region_name"],
        "reading_timestamp": reading_timestamp.isoformat(timespec="seconds"),
        "kwh": f"{kwh:.4f}",
        "ingestion_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }


def invalid_rows_for_day(
    *,
    day: date,
    source_file: str,
    batch_id: str,
    valid_row_for_duplicate: dict[str, str],
) -> list[dict[str, str]]:
    ingestion_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    base_timestamp = datetime.combine(day, time(hour=12)).isoformat(timespec="seconds")
    duplicate_row = dict(valid_row_for_duplicate)

    return [
        duplicate_row,
        {
            "source_file": source_file,
            "batch_id": batch_id,
            "meter_id": "",
            "customer_id": "CUST-LON-001",
            "region_name": "North London",
            "reading_timestamp": base_timestamp,
            "kwh": "0.5120",
            "ingestion_timestamp": ingestion_timestamp,
        },
        {
            "source_file": source_file,
            "batch_id": batch_id,
            "meter_id": "MTR-LON-002",
            "customer_id": "CUST-LON-002",
            "region_name": "South London",
            "reading_timestamp": base_timestamp,
            "kwh": "-1.2500",
            "ingestion_timestamp": ingestion_timestamp,
        },
        {
            "source_file": source_file,
            "batch_id": batch_id,
            "meter_id": "MTR-LON-003",
            "customer_id": "CUST-LON-003",
            "region_name": "East London",
            "reading_timestamp": "not-a-timestamp",
            "kwh": "0.7410",
            "ingestion_timestamp": ingestion_timestamp,
        },
        {
            "source_file": source_file,
            "batch_id": batch_id,
            "meter_id": "MTR-LON-004",
            "customer_id": "CUST-LON-004",
            "region_name": "Unknown London",
            "reading_timestamp": base_timestamp,
            "kwh": "0.8400",
            "ingestion_timestamp": ingestion_timestamp,
        },
    ]


def write_partition(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    paths = get_project_paths()
    paths.create_lakehouse_directories()

    customers = read_csv(paths.bronze_customers_file)
    meters = read_csv(paths.bronze_meters_file)
    customer_by_id = {customer["customer_id"]: customer for customer in customers}

    clear_generated_files(paths.bronze_historical_consumption)

    start_date = date(2026, 1, 1)
    days = 30
    rng = random.Random(44)
    total_rows = 0

    for day_offset in range(days):
        current_day = start_date + timedelta(days=day_offset)
        source_file = f"historical_consumption_{current_day:%Y%m%d}.csv"
        batch_id = f"batch_{current_day:%Y%m%d}"
        rows: list[dict[str, str]] = []

        for slot in range(48):
            reading_timestamp = datetime.combine(current_day, time.min) + timedelta(minutes=30 * slot)
            for meter in meters:
                customer = customer_by_id[meter["customer_id"]]
                rows.append(
                    build_consumption_row(
                        meter=meter,
                        customer=customer,
                        reading_timestamp=reading_timestamp,
                        source_file=source_file,
                        batch_id=batch_id,
                        rng=rng,
                    )
                )

        rows.extend(
            invalid_rows_for_day(
                day=current_day,
                source_file=source_file,
                batch_id=batch_id,
                valid_row_for_duplicate=rows[0],
            )
        )

        partition_path = (
            paths.bronze_historical_consumption
            / f"year={current_day:%Y}"
            / f"month={current_day:%m}"
            / f"day={current_day:%d}"
            / source_file
        )
        write_partition(partition_path, rows)
        total_rows += len(rows)

    print(f"Generated {total_rows} bronze historical consumption rows across {days} daily partitions")


if __name__ == "__main__":
    main()
