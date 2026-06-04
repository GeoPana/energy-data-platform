"""Centralized local paths for Phase 4 streaming."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def resolve_project_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


@dataclass(frozen=True)
class StreamingPaths:
    bronze_streaming_events: Path
    bronze_streaming_events_landing: Path
    silver_clean_streaming_events: Path
    silver_rejected_streaming_events: Path
    gold_latest_meter_readings: Path
    gold_streaming_anomaly_events: Path
    gold_hourly_streaming_consumption: Path
    gold_streaming_pipeline_metrics: Path
    checkpoint_eventhub_to_bronze: Path
    checkpoint_bronze_to_silver_gold: Path

    def create_directories(self) -> None:
        for path in self.__dict__.values():
            Path(path).mkdir(parents=True, exist_ok=True)


def get_streaming_paths() -> StreamingPaths:
    data_lake_root = resolve_project_path(os.getenv("STREAMING_DATA_LAKE_ROOT", "./data_lake"))
    checkpoint_root = resolve_project_path(os.getenv("STREAMING_CHECKPOINT_ROOT", "./checkpoints"))
    return StreamingPaths(
        bronze_streaming_events=data_lake_root / "bronze" / "streaming_events",
        bronze_streaming_events_landing=data_lake_root / "bronze" / "streaming_events_landing",
        silver_clean_streaming_events=data_lake_root / "silver" / "clean_streaming_events",
        silver_rejected_streaming_events=data_lake_root / "silver" / "rejected_streaming_events",
        gold_latest_meter_readings=data_lake_root / "gold" / "latest_meter_readings",
        gold_streaming_anomaly_events=data_lake_root / "gold" / "streaming_anomaly_events",
        gold_hourly_streaming_consumption=data_lake_root / "gold" / "hourly_streaming_consumption",
        gold_streaming_pipeline_metrics=data_lake_root / "gold" / "streaming_pipeline_metrics",
        checkpoint_eventhub_to_bronze=checkpoint_root / "eventhub_to_bronze",
        checkpoint_bronze_to_silver_gold=checkpoint_root / "bronze_to_silver_gold",
    )
