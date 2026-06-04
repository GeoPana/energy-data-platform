"""Best-effort SQL Server monitoring helpers for Airflow orchestration."""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def pipeline_run_id(dag_id: str, run_id: str) -> str:
    digest = hashlib.sha1(f"{dag_id}:{run_id}".encode("utf-8")).hexdigest()[:20]
    return f"AF-{digest}"


def _simple_yaml_load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    parsed: dict[str, Any] = {}
    current_section: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
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
        value = value.strip().strip('"').strip("'")
        if raw_line.startswith(" ") and current_section:
            parsed[current_section][key] = value
        else:
            current_section = None
            parsed[key] = value
    return parsed


def _config_sql_server() -> dict[str, Any]:
    config_path = os.getenv("ENERGY_AIRFLOW_CONFIG")
    if not config_path:
        project_root = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
        config_path = str(project_root / "config" / "airflow_config.example.yaml")

    try:
        import yaml

        loaded = yaml.safe_load(Path(config_path).read_text(encoding="utf-8")) or {}
    except Exception:
        loaded = _simple_yaml_load(Path(config_path))

    return dict(loaded.get("sql_server", {}))


def get_sql_server_config() -> dict[str, Any]:
    config = _config_sql_server()
    return {
        "driver": os.getenv("SQLSERVER_DRIVER", config.get("driver", "ODBC Driver 18 for SQL Server")),
        "server": os.getenv("SQLSERVER_HOST", config.get("server", "localhost")),
        "database": os.getenv("SQLSERVER_DATABASE", config.get("database", "EnergyWarehouse")),
        "trusted_connection": _parse_bool(
            os.getenv("SQLSERVER_TRUSTED_CONNECTION"),
            _parse_bool(config.get("trusted_connection"), True),
        ),
        "username": os.getenv("SQLSERVER_USERNAME", config.get("username", "")),
        "password": os.getenv("SQLSERVER_PASSWORD", config.get("password", "")),
        "encrypt": _parse_bool(os.getenv("SQLSERVER_ENCRYPT"), _parse_bool(config.get("encrypt"), True)),
        "trust_server_certificate": _parse_bool(
            os.getenv("SQLSERVER_TRUST_SERVER_CERTIFICATE"),
            _parse_bool(config.get("trust_server_certificate"), True),
        ),
    }


def build_connection_string(config: dict[str, Any] | None = None) -> str:
    selected = config or get_sql_server_config()
    parts = [
        f"DRIVER={{{selected['driver']}}}",
        f"SERVER={selected['server']}",
        f"DATABASE={selected['database']}",
        f"Encrypt={'yes' if selected['encrypt'] else 'no'}",
        f"TrustServerCertificate={'yes' if selected['trust_server_certificate'] else 'no'}",
    ]
    if selected["trusted_connection"]:
        parts.append("Trusted_Connection=yes")
    else:
        parts.extend([f"UID={selected['username']}", f"PWD={selected['password']}"])
    return ";".join(parts)


def connect_to_sql_server():
    import pyodbc

    return pyodbc.connect(build_connection_string(), timeout=10)


def _execute_best_effort(action_name: str, sql: str, params: tuple[Any, ...]) -> None:
    try:
        connection = connect_to_sql_server()
        try:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()
        finally:
            connection.close()
    except Exception as exc:
        LOGGER.warning("Monitoring write skipped for %s: %s", action_name, exc)


def create_pipeline_run_record(dag_id: str, run_id: str, pipeline_name: str) -> str:
    selected_run_id = pipeline_run_id(dag_id, run_id)
    _execute_best_effort(
        "create_pipeline_run_record",
        """
        IF NOT EXISTS (SELECT 1 FROM monitoring.pipeline_run WHERE pipeline_run_id = ?)
        BEGIN
            INSERT INTO monitoring.pipeline_run
            (
                pipeline_run_id,
                pipeline_name,
                pipeline_type,
                status,
                started_at,
                records_read,
                records_written,
                records_rejected,
                error_message
            )
            VALUES (?, ?, 'batch_orchestration', 'running', ?, 0, 0, 0, NULL);
        END;
        """,
        (
            selected_run_id,
            selected_run_id,
            pipeline_name,
            datetime.now(timezone.utc).replace(microsecond=0),
        ),
    )
    return selected_run_id


def update_pipeline_run_success(dag_id: str, run_id: str) -> None:
    selected_run_id = pipeline_run_id(dag_id, run_id)
    _execute_best_effort(
        "update_pipeline_run_success",
        """
        UPDATE monitoring.pipeline_run
        SET status = 'succeeded',
            finished_at = ?,
            error_message = NULL
        WHERE pipeline_run_id = ?;
        """,
        (datetime.now(timezone.utc).replace(microsecond=0), selected_run_id),
    )


def update_pipeline_run_failure(dag_id: str, run_id: str, error_message: str) -> None:
    selected_run_id = pipeline_run_id(dag_id, run_id)
    _execute_best_effort(
        "update_pipeline_run_failure",
        """
        UPDATE monitoring.pipeline_run
        SET status = 'failed',
            finished_at = ?,
            error_message = ?
        WHERE pipeline_run_id = ?;
        """,
        (datetime.now(timezone.utc).replace(microsecond=0), error_message[:4000], selected_run_id),
    )


def insert_data_quality_result(
    *,
    check_id: str,
    pipeline_run_id_value: str,
    check_name: str,
    table_name: str,
    status: str,
    failed_count: int,
) -> None:
    _execute_best_effort(
        "insert_data_quality_result",
        """
        IF NOT EXISTS (SELECT 1 FROM monitoring.data_quality_check WHERE check_id = ?)
        BEGIN
            INSERT INTO monitoring.data_quality_check
            (
                check_id,
                pipeline_run_id,
                check_name,
                table_name,
                status,
                failed_count
            )
            VALUES (?, ?, ?, ?, ?, ?);
        END;
        """,
        (
            check_id,
            check_id,
            pipeline_run_id_value,
            check_name,
            table_name,
            status,
            failed_count,
        ),
    )
