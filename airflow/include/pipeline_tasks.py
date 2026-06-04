"""Reusable Airflow task helpers for orchestrating Phase 2 scripts."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


LOGGER = logging.getLogger(__name__)


REQUIRED_PATHS = [
    "config",
    "data_lake",
    "docs",
    "ingestion",
    "spark_jobs",
    "sql",
]

REQUIRED_FILES = [
    "config/local_config.example.yaml",
    "config/airflow_config.example.yaml",
    "ingestion/generate_metadata.py",
    "ingestion/generate_batch_data.py",
    "ingestion/generate_weather_data.py",
    "spark_jobs/bronze_to_silver_batch.py",
    "spark_jobs/silver_to_gold_batch.py",
    "spark_jobs/load_gold_to_sql_server.py",
    "sql/01_create_database.sql",
    "sql/04_create_facts.sql",
]


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def get_project_root() -> Path:
    """Resolve the project root without hard-coding a machine-specific path."""
    configured = os.getenv("PROJECT_ROOT")
    if configured:
        return Path(configured).resolve()
    return Path(__file__).resolve().parents[2]


def get_python_executable() -> str:
    return os.getenv("ENERGY_PIPELINE_PYTHON", sys.executable)


def check_required_paths() -> None:
    project_root = get_project_root()
    missing = [path for path in REQUIRED_PATHS if not (project_root / path).is_dir()]
    if missing:
        raise FileNotFoundError(f"Missing required project paths: {', '.join(missing)}")
    LOGGER.info("Required project paths exist: %s", ", ".join(REQUIRED_PATHS))


def check_required_files() -> None:
    project_root = get_project_root()
    missing = [path for path in REQUIRED_FILES if not (project_root / path).is_file()]
    if missing:
        raise FileNotFoundError(f"Missing required project files: {', '.join(missing)}")
    LOGGER.info("Required project files exist: %s", ", ".join(REQUIRED_FILES))


def check_project_structure() -> None:
    check_required_paths()
    check_required_files()


def run_python_script(script_path: str | Path, args: Sequence[str] | None = None) -> None:
    """Run a project Python script with captured logs and clear errors."""
    project_root = get_project_root()
    resolved_script = project_root / script_path
    if not resolved_script.exists():
        raise FileNotFoundError(f"Script not found: {resolved_script}")

    command = [get_python_executable(), str(resolved_script)]
    if args:
        command.extend(args)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root) + os.pathsep + env.get("PYTHONPATH", "")

    LOGGER.info("Starting script: %s at %s", resolved_script, utc_now_text())
    LOGGER.info("Command: %s", " ".join(command))

    completed = subprocess.run(
        command,
        cwd=str(project_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.stdout:
        LOGGER.info("stdout from %s:\n%s", resolved_script.name, completed.stdout.strip())
    if completed.stderr:
        LOGGER.warning("stderr from %s:\n%s", resolved_script.name, completed.stderr.strip())

    if completed.returncode != 0:
        raise RuntimeError(f"Script failed with exit code {completed.returncode}: {resolved_script}")

    LOGGER.info("Finished script: %s at %s", resolved_script, utc_now_text())


def run_generate_metadata() -> None:
    run_python_script("ingestion/generate_metadata.py")


def run_generate_batch_data() -> None:
    run_python_script("ingestion/generate_batch_data.py")


def run_generate_weather_data() -> None:
    run_python_script("ingestion/generate_weather_data.py")


def run_bronze_to_silver() -> None:
    run_python_script("spark_jobs/bronze_to_silver_batch.py")


def run_silver_to_gold() -> None:
    run_python_script("spark_jobs/silver_to_gold_batch.py")


def run_load_gold_to_sql_server() -> None:
    run_python_script("spark_jobs/load_gold_to_sql_server.py")
