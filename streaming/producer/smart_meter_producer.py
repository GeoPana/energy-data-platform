"""Produce smart-meter events to Event Hubs Emulator or local JSON files."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from streaming.common.event_generation import generate_event_batch
from streaming.common.event_validation import classify_event, is_valid_event
from streaming.common.streaming_logging import log_producer_end, log_producer_start
from streaming.common.streaming_paths import get_streaming_paths, resolve_project_path


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return value.strip().lower() in {"1", "true", "yes", "y"}


EventPayload = dict | str


def write_events_to_file(events: list[EventPayload], output_path: Path, batch_number: int) -> Path:
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / f"smart_meter_events_{int(time.time())}_{batch_number:05d}.jsonl"
    with file_path.open("w", encoding="utf-8") as handle:
        for event in events:
            payload = event if isinstance(event, str) else json.dumps(event, sort_keys=True)
            handle.write(payload + "\n")
    return file_path


def send_events_to_eventhub(events: list[EventPayload]) -> None:
    try:
        from azure.eventhub import EventData, EventHubProducerClient
    except ImportError as exc:
        raise RuntimeError(
            "azure-eventhub is required for --mode eventhub. "
            "Update the Conda environment with environment.yml first."
        ) from exc

    connection_str = os.getenv("EVENTHUB_CONNECTION_STR")
    eventhub_name = os.getenv("EVENTHUB_NAME", "smart-meter-events")
    if not connection_str:
        raise RuntimeError("EVENTHUB_CONNECTION_STR is required for --mode eventhub.")

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_str,
        eventhub_name=eventhub_name,
    )

    try:
        event_data_batch = producer.create_batch()
        for event in events:
            payload = event if isinstance(event, str) else json.dumps(event, sort_keys=True)
            try:
                event_data_batch.add(EventData(payload))
            except ValueError:
                producer.send_batch(event_data_batch)
                event_data_batch = producer.create_batch()
                event_data_batch.add(EventData(payload))
        if len(event_data_batch) > 0:
            producer.send_batch(event_data_batch)
    finally:
        producer.close()


def run_producer(
    *,
    mode: str,
    events_per_second: int,
    duration_seconds: int,
    output_path: Path,
    include_bad_events: bool,
) -> None:
    target = str(output_path) if mode == "file" else os.getenv("EVENTHUB_NAME", "smart-meter-events")
    log_producer_start(mode, target, events_per_second, duration_seconds)

    total_events = 0
    bad_events = 0
    anomaly_events = 0

    for batch_number in range(duration_seconds):
        events: list[EventPayload] = generate_event_batch(events_per_second, include_bad_events=include_bad_events)
        if include_bad_events and batch_number % 10 == 0 and events:
            events[0] = '{"event_id":"malformed_event","meter_id":'
        if mode == "file":
            write_events_to_file(events, output_path, batch_number)
        elif mode == "eventhub":
            send_events_to_eventhub(events)
        else:
            raise ValueError(f"Unsupported mode: {mode}")

        total_events += len(events)
        bad_events += sum(1 for event in events if isinstance(event, str) or not is_valid_event(event))
        anomaly_events += sum(1 for event in events if isinstance(event, dict) and classify_event(event) == "anomaly")
        time.sleep(1)

    log_producer_end(total_events, bad_events, anomaly_events)


def build_parser() -> argparse.ArgumentParser:
    default_output_path = get_streaming_paths().bronze_streaming_events_landing
    parser = argparse.ArgumentParser(description="Produce smart-meter streaming events.")
    parser.add_argument("--mode", choices=["file", "eventhub"], default="file")
    parser.add_argument("--events-per-second", type=int, default=5)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--output-path", default=str(default_output_path))
    parser.add_argument("--include-bad-events", default="true")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_producer(
        mode=args.mode,
        events_per_second=max(args.events_per_second, 1),
        duration_seconds=max(args.duration_seconds, 1),
        output_path=resolve_project_path(args.output_path),
        include_bad_events=parse_bool(args.include_bad_events),
    )


if __name__ == "__main__":
    main()
