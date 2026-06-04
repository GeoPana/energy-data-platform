"""Config-driven local paths for the Phase 2 lakehouse pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "local_config.example.yaml"


def _parse_scalar(value: str) -> Any:
    cleaned = value.strip().strip('"').strip("'")
    if cleaned.lower() == "true":
        return True
    if cleaned.lower() == "false":
        return False
    return cleaned


def _simple_yaml_load(text: str) -> dict[str, Any]:
    """Small YAML fallback for this project's simple config shape."""
    parsed: dict[str, Any] = {}
    current_section: str | None = None

    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        if not raw_line.startswith(" ") and raw_line.rstrip().endswith(":"):
            current_section = raw_line.strip()[:-1]
            parsed[current_section] = {}
            continue

        key, _, value = raw_line.partition(":")
        if not _:
            continue

        key = key.strip()
        value = _parse_scalar(value)

        if raw_line.startswith(" ") and current_section:
            parsed[current_section][key] = value
        else:
            current_section = None
            parsed[key] = value

    return parsed


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load local YAML config, falling back to defaults if no file is present."""
    selected_path = (
        Path(config_path)
        if config_path
        else Path(os.getenv("ENERGY_PLATFORM_CONFIG", DEFAULT_CONFIG_PATH))
    )

    if not selected_path.is_absolute():
        selected_path = PROJECT_ROOT / selected_path

    if not selected_path.exists():
        selected_path = DEFAULT_CONFIG_PATH

    text = selected_path.read_text(encoding="utf-8")

    try:
        import yaml

        loaded = yaml.safe_load(text) or {}
    except ImportError:
        loaded = _simple_yaml_load(text)

    return loaded


def _resolve_project_path(path_value: str | Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


@dataclass(frozen=True)
class ProjectPaths:
    data_lake_root: Path
    bronze_path: Path
    silver_path: Path
    gold_path: Path

    @property
    def bronze_historical_consumption(self) -> Path:
        return self.bronze_path / "historical_consumption"

    @property
    def bronze_weather(self) -> Path:
        return self.bronze_path / "weather"

    @property
    def bronze_customer_metadata(self) -> Path:
        return self.bronze_path / "customer_metadata"

    @property
    def bronze_meter_metadata(self) -> Path:
        return self.bronze_path / "meter_metadata"

    @property
    def bronze_tariff(self) -> Path:
        return self.bronze_path / "tariff"

    @property
    def bronze_customers_file(self) -> Path:
        return self.bronze_customer_metadata / "customers.csv"

    @property
    def bronze_regions_file(self) -> Path:
        return self.bronze_customer_metadata / "regions.csv"

    @property
    def bronze_meters_file(self) -> Path:
        return self.bronze_meter_metadata / "meters.csv"

    @property
    def bronze_tariffs_file(self) -> Path:
        return self.bronze_tariff / "tariffs.csv"

    @property
    def silver_clean_consumption(self) -> Path:
        return self.silver_path / "clean_consumption"

    @property
    def silver_clean_weather(self) -> Path:
        return self.silver_path / "clean_weather"

    @property
    def silver_clean_customer(self) -> Path:
        return self.silver_path / "clean_customer"

    @property
    def silver_clean_meter(self) -> Path:
        return self.silver_path / "clean_meter"

    @property
    def silver_clean_tariff(self) -> Path:
        return self.silver_path / "clean_tariff"

    @property
    def silver_rejected_records(self) -> Path:
        return self.silver_path / "rejected_records"

    @property
    def gold_daily_region_consumption(self) -> Path:
        return self.gold_path / "daily_region_consumption"

    @property
    def gold_daily_customer_consumption(self) -> Path:
        return self.gold_path / "daily_customer_consumption"

    @property
    def gold_monthly_region_consumption(self) -> Path:
        return self.gold_path / "monthly_region_consumption"

    @property
    def gold_consumption_weather_features(self) -> Path:
        return self.gold_path / "consumption_weather_features"

    @property
    def gold_customer_usage_summary(self) -> Path:
        return self.gold_path / "customer_usage_summary"

    def create_lakehouse_directories(self) -> None:
        for path in (
            self.bronze_historical_consumption,
            self.bronze_weather,
            self.bronze_customer_metadata,
            self.bronze_meter_metadata,
            self.bronze_tariff,
            self.silver_clean_consumption,
            self.silver_clean_weather,
            self.silver_clean_customer,
            self.silver_clean_meter,
            self.silver_clean_tariff,
            self.silver_rejected_records,
            self.gold_daily_region_consumption,
            self.gold_daily_customer_consumption,
            self.gold_monthly_region_consumption,
            self.gold_consumption_weather_features,
            self.gold_customer_usage_summary,
        ):
            path.mkdir(parents=True, exist_ok=True)


def get_project_paths(config_path: str | Path | None = None) -> ProjectPaths:
    config = load_config(config_path)
    return ProjectPaths(
        data_lake_root=_resolve_project_path(config.get("data_lake_root", "./data_lake")),
        bronze_path=_resolve_project_path(config.get("bronze_path", "./data_lake/bronze")),
        silver_path=_resolve_project_path(config.get("silver_path", "./data_lake/silver")),
        gold_path=_resolve_project_path(config.get("gold_path", "./data_lake/gold")),
    )


def get_sql_server_config(config_path: str | Path | None = None) -> dict[str, Any]:
    config = load_config(config_path)
    return dict(config.get("sql_server", {}))
