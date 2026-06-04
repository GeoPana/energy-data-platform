"""Small structured logging helpers for local jobs."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def log_job_start(job_name: str) -> None:
    print(f'{{"event":"job_start","job":"{job_name}","timestamp":"{utc_now_text()}"}}')


def log_job_end(job_name: str) -> None:
    print(f'{{"event":"job_end","job":"{job_name}","timestamp":"{utc_now_text()}"}}')


def log_metric(job_name: str, metric_name: str, value: int | float | str) -> None:
    print(
        f'{{"event":"metric","job":"{job_name}",'
        f'"metric":"{metric_name}","value":"{value}","timestamp":"{utc_now_text()}"}}'
    )
