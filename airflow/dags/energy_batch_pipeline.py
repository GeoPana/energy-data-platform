"""Airflow DAG for Phase 3 batch lakehouse orchestration."""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


AIRFLOW_ROOT = Path(__file__).resolve().parents[1]
INCLUDE_DIR = AIRFLOW_ROOT / "include"
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

from airflow_logging import (  # noqa: E402
    create_pipeline_run_record,
    update_pipeline_run_failure,
    update_pipeline_run_success,
)
from pipeline_tasks import (  # noqa: E402
    check_project_structure,
    run_bronze_to_silver,
    run_generate_batch_data,
    run_generate_metadata,
    run_generate_weather_data,
    run_load_gold_to_sql_server,
    run_silver_to_gold,
)
from sql_quality_checks import run_sql_quality_checks  # noqa: E402


LOGGER = logging.getLogger(__name__)

DAG_ID = "energy_batch_lakehouse_pipeline"
PIPELINE_NAME = "energy_batch_lakehouse_pipeline"


def start_pipeline_task(**context) -> str:
    dag_run = context["dag_run"]
    selected_pipeline_run_id = create_pipeline_run_record(
        dag_id=context["dag"].dag_id,
        run_id=dag_run.run_id,
        pipeline_name=PIPELINE_NAME,
    )
    LOGGER.info("Started pipeline run %s for Airflow run_id %s", selected_pipeline_run_id, dag_run.run_id)
    return selected_pipeline_run_id


def run_data_quality_checks_task(**context):
    return run_sql_quality_checks(
        dag_id=context["dag"].dag_id,
        run_id=context["dag_run"].run_id,
    )


def write_pipeline_run_summary_task(**context) -> None:
    update_pipeline_run_success(
        dag_id=context["dag"].dag_id,
        run_id=context["dag_run"].run_id,
    )
    LOGGER.info("Pipeline completed successfully for run_id %s", context["dag_run"].run_id)


def mark_pipeline_failure(context) -> None:
    exception = context.get("exception")
    update_pipeline_run_failure(
        dag_id=context["dag"].dag_id,
        run_id=context["dag_run"].run_id,
        error_message=str(exception or "Airflow task failed"),
    )


default_args = {
    "owner": "data-engineering-portfolio",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": mark_pipeline_failure,
}


with DAG(
    dag_id=DAG_ID,
    description="Orchestrates the local batch lakehouse pipeline from bronze generation to SQL Server quality checks.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["portfolio", "batch", "lakehouse", "sql-server"],
) as dag:
    start_pipeline = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline_task,
    )

    check_project_structure_task = PythonOperator(
        task_id="check_project_structure",
        python_callable=check_project_structure,
    )

    generate_metadata = PythonOperator(
        task_id="generate_metadata",
        python_callable=run_generate_metadata,
    )

    generate_batch_energy_data = PythonOperator(
        task_id="generate_batch_energy_data",
        python_callable=run_generate_batch_data,
    )

    generate_weather_data = PythonOperator(
        task_id="generate_weather_data",
        python_callable=run_generate_weather_data,
    )

    run_bronze_to_silver = PythonOperator(
        task_id="run_bronze_to_silver",
        python_callable=run_bronze_to_silver,
    )

    run_silver_to_gold = PythonOperator(
        task_id="run_silver_to_gold",
        python_callable=run_silver_to_gold,
    )

    load_gold_to_sql_server = PythonOperator(
        task_id="load_gold_to_sql_server",
        python_callable=run_load_gold_to_sql_server,
    )

    run_data_quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks_task,
    )

    write_pipeline_run_summary = PythonOperator(
        task_id="write_pipeline_run_summary",
        python_callable=write_pipeline_run_summary_task,
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline
        >> check_project_structure_task
        >> generate_metadata
        >> generate_batch_energy_data
        >> generate_weather_data
        >> run_bronze_to_silver
        >> run_silver_to_gold
        >> load_gold_to_sql_server
        >> run_data_quality_checks
        >> write_pipeline_run_summary
        >> end_pipeline
    )
