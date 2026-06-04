"""SQL Server data-quality checks executed by the Phase 3 Airflow DAG."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Any

from airflow_logging import connect_to_sql_server, insert_data_quality_result, pipeline_run_id


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class QualityCheck:
    check_name: str
    table_name: str
    sql: str
    expected_zero_failures: bool = True


QUALITY_CHECKS = [
    QualityCheck(
        "fact_energy_consumption_has_rows",
        "dw.fact_energy_consumption",
        "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM dw.fact_energy_consumption;",
    ),
    QualityCheck(
        "fact_energy_consumption_no_negative_kwh",
        "dw.fact_energy_consumption",
        "SELECT COUNT(*) FROM dw.fact_energy_consumption WHERE kwh < 0;",
    ),
    QualityCheck(
        "fact_energy_consumption_no_null_meter_id",
        "dw.fact_energy_consumption",
        "SELECT COUNT(*) FROM dw.fact_energy_consumption WHERE meter_id IS NULL;",
    ),
    QualityCheck(
        "fact_energy_consumption_no_missing_region_reference",
        "dw.fact_energy_consumption",
        """
        SELECT COUNT(*)
        FROM dw.fact_energy_consumption AS f
        LEFT JOIN dw.dim_region AS r
            ON r.region_id = f.region_id
        WHERE r.region_id IS NULL;
        """,
    ),
    QualityCheck(
        "fact_meter_reading_no_duplicate_reading_id",
        "dw.fact_meter_reading",
        """
        SELECT COUNT(*)
        FROM
        (
            SELECT reading_id
            FROM dw.fact_meter_reading
            GROUP BY reading_id
            HAVING COUNT(*) > 1
        ) AS duplicate_readings;
        """,
    ),
    QualityCheck(
        "fact_energy_consumption_latest_timestamp_present",
        "dw.fact_energy_consumption",
        "SELECT CASE WHEN MAX(reading_timestamp) IS NOT NULL THEN 0 ELSE 1 END FROM dw.fact_energy_consumption;",
    ),
]


def _check_id(selected_pipeline_run_id: str, check_name: str) -> str:
    digest = hashlib.sha1(f"{selected_pipeline_run_id}:{check_name}".encode("utf-8")).hexdigest()[:18]
    return f"AF-DQ-{digest}"


def _monitoring_check(selected_pipeline_run_id: str) -> QualityCheck:
    return QualityCheck(
        "monitoring_pipeline_run_record_exists",
        "monitoring.pipeline_run",
        f"SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM monitoring.pipeline_run WHERE pipeline_run_id = '{selected_pipeline_run_id}';",
    )


def _execute_scalar(cursor: Any, sql: str) -> int:
    cursor.execute(sql)
    value = cursor.fetchone()[0]
    return int(value or 0)


def run_sql_quality_checks(dag_id: str, run_id: str) -> list[dict[str, Any]]:
    """Run SQL checks and raise if any check fails."""
    selected_pipeline_run_id = pipeline_run_id(dag_id, run_id)
    checks = [*QUALITY_CHECKS, _monitoring_check(selected_pipeline_run_id)]
    results: list[dict[str, Any]] = []

    connection = connect_to_sql_server()
    try:
        cursor = connection.cursor()
        for check in checks:
            failed_count = _execute_scalar(cursor, check.sql)
            status = "passed" if failed_count == 0 else "failed"
            result = {
                "check_name": check.check_name,
                "table_name": check.table_name,
                "status": status,
                "failed_count": failed_count,
            }
            results.append(result)
            LOGGER.info("Quality check result: %s", result)

            insert_data_quality_result(
                check_id=_check_id(selected_pipeline_run_id, check.check_name),
                pipeline_run_id_value=selected_pipeline_run_id,
                check_name=check.check_name,
                table_name=check.table_name,
                status=status,
                failed_count=failed_count,
            )
    finally:
        connection.close()

    failures = [result for result in results if result["status"] == "failed"]
    if failures:
        raise ValueError(f"Data-quality checks failed: {failures}")

    return results
