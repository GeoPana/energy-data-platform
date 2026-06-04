"""Replay Phase 2 batch consumption rows as smart-meter streaming events."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streaming.common.event_generation import iso_z
from streaming.common.streaming_logging import log_producer_end, log_producer_start
from streaming.common.streaming_paths import get_streaming_paths, resolve_project_path
from streaming.producer.smart_meter_producer import send_events_to_eventhub, write_events_to_file


def read_bronze_consumption_rows(input_path: Path, limit: int | None = None) -> list[dict[str, str]]:
    if not input_path.exists():
        raise FileNotFoundError(
            f"Input path does not exist: {input_path}. Run Phase 2 data generation first."
        )

    csv_files = sorted(input_path.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found under {input_path}. Run python ingestion/generate_batch_data.py first."
        )

    rows: list[dict[str, str]] = []
    for csv_file in csv_files:
        with csv_file.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows.append(row)
                if limit and len(rows) >= limit:
                    return rows
    return rows


def row_to_event(row: dict[str, str], sequence: int) -> dict:
    timestamp_text = row.get("reading_timestamp") or datetime.now(timezone.utc).isoformat()
    try:
        event_timestamp = datetime.fromisoformat(timestamp_text)
        event_timestamp_text = iso_z(event_timestamp)
    except ValueError:
        event_timestamp_text = timestamp_text

    try:
        kwh = float(row.get("kwh", "0"))
    except ValueError:
        kwh = row.get("kwh")

    return {
        "event_id": f"replay_evt_{sequence:08d}",
        "meter_id": row.get("meter_id"),
        "customer_id": row.get("customer_id"),
        "region_name": row.get("region_name"),
        "event_timestamp": event_timestamp_text,
        "kwh": kwh,
        "voltage": 230.0,
        "event_type": "anomaly" if isinstance(kwh, float) and kwh > 10 else "reading",
        "source_system": "batch_replay_stream",
        "producer_timestamp": iso_z(datetime.now(timezone.utc)),
    }


def run_replay(
    *,
    input_path: Path,
    mode: str,
    output_path: Path,
    events_per_second: int,
    limit: int | None,
) -> None:
    rows = read_bronze_consumption_rows(input_path, limit)
    target = str(output_path) if mode == "file" else os.getenv("EVENTHUB_NAME", "smart-meter-events")
    log_producer_start(mode, target, events_per_second, duration_seconds=max(1, len(rows) // events_per_second))

    total_events = 0
    anomaly_events = 0
    batch_number = 0
    for index in range(0, len(rows), events_per_second):
        events = [row_to_event(row, index + offset + 1) for offset, row in enumerate(rows[index:index + events_per_second])]
        if mode == "file":
            write_events_to_file(events, output_path, batch_number)
        elif mode == "eventhub":
            send_events_to_eventhub(events)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

        total_events += len(events)
        anomaly_events += sum(1 for event in events if event.get("event_type") == "anomaly")
        batch_number += 1
        time.sleep(1)

    log_producer_end(total_events, bad_events=0, anomaly_events=anomaly_events)


def build_parser() -> argparse.ArgumentParser:
    paths = get_streaming_paths()
    parser = argparse.ArgumentParser(description="Replay Phase 2 batch data as streaming events.")
    parser.add_argument("--input-path", default="./data_lake/bronze/historical_consumption")
    parser.add_argument("--mode", choices=["file", "eventhub"], default="file")
    parser.add_argument("--events-per-second", type=int, default=10)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--output-path", default=str(paths.bronze_streaming_events_landing))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_replay(
        input_path=resolve_project_path(args.input_path),
        mode=args.mode,
        output_path=resolve_project_path(args.output_path),
        events_per_second=max(args.events_per_second, 1),
        limit=args.limit if args.limit > 0 else None,
    )


if __name__ == "__main__":
    main()
