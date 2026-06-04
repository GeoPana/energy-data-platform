"""Generate bronze hourly weather CSV files for Phase 2 local development."""

from __future__ import annotations

import csv
import math
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
    "weather_timestamp",
    "region_name",
    "temperature_c",
    "humidity_percent",
    "wind_speed_kph",
    "precipitation_mm",
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


def read_regions(path: Path) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}. Run python ingestion/generate_metadata.py first.")
    with path.open(newline="", encoding="utf-8") as handle:
        return [row["region_name"] for row in csv.DictReader(handle)]


def build_weather_row(
    *,
    weather_timestamp: datetime,
    region_name: str,
    rng: random.Random,
) -> dict[str, str]:
    region_adjustment = {
        "North London": -0.3,
        "South London": 0.1,
        "East London": -0.1,
        "West London": 0.2,
        "Central London": 0.5,
    }.get(region_name, 0.0)

    hour = weather_timestamp.hour
    daily_curve = math.sin((hour - 6) / 24 * 2 * math.pi)
    temperature = 7.0 + (3.2 * daily_curve) + region_adjustment + rng.uniform(-0.5, 0.5)
    humidity = 73.0 - (8.0 * daily_curve) + rng.uniform(-4.0, 4.0)
    wind_speed = 12.0 + rng.uniform(-3.5, 5.0)
    precipitation = max(0.0, rng.gauss(0.35, 0.45)) if rng.random() < 0.22 else 0.0

    return {
        "weather_timestamp": weather_timestamp.isoformat(timespec="seconds"),
        "region_name": region_name,
        "temperature_c": f"{temperature:.2f}",
        "humidity_percent": f"{max(35.0, min(humidity, 98.0)):.2f}",
        "wind_speed_kph": f"{max(0.0, wind_speed):.2f}",
        "precipitation_mm": f"{precipitation:.2f}",
        "ingestion_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }


def write_partition(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    paths = get_project_paths()
    paths.create_lakehouse_directories()

    regions = read_regions(paths.bronze_regions_file)
    clear_generated_files(paths.bronze_weather)

    start_date = date(2026, 1, 1)
    days = 30
    rng = random.Random(45)
    total_rows = 0

    for day_offset in range(days):
        current_day = start_date + timedelta(days=day_offset)
        source_file = f"weather_{current_day:%Y%m%d}.csv"
        rows: list[dict[str, str]] = []

        for hour in range(24):
            weather_timestamp = datetime.combine(current_day, time(hour=hour))
            for region_name in regions:
                rows.append(build_weather_row(weather_timestamp=weather_timestamp, region_name=region_name, rng=rng))

        partition_path = (
            paths.bronze_weather
            / f"year={current_day:%Y}"
            / f"month={current_day:%m}"
            / f"day={current_day:%d}"
            / source_file
        )
        write_partition(partition_path, rows)
        total_rows += len(rows)

    print(f"Generated {total_rows} bronze hourly weather rows across {days} daily partitions")


if __name__ == "__main__":
    main()
