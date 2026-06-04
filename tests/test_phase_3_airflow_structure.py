from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_text(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def test_phase_3_dag_file_exists_and_has_expected_dag_id() -> None:
    dag_file = PROJECT_ROOT / "airflow" / "dags" / "energy_batch_pipeline.py"

    assert dag_file.exists()
    assert 'DAG_ID = "energy_batch_lakehouse_pipeline"' in dag_file.read_text(encoding="utf-8")


def test_phase_3_expected_task_names_are_defined() -> None:
    dag_text = read_text("airflow/dags/energy_batch_pipeline.py")
    expected_tasks = [
        "start_pipeline",
        "check_project_structure",
        "generate_metadata",
        "generate_batch_energy_data",
        "generate_weather_data",
        "run_bronze_to_silver",
        "run_silver_to_gold",
        "load_gold_to_sql_server",
        "run_data_quality_checks",
        "write_pipeline_run_summary",
        "end_pipeline",
    ]

    for task_name in expected_tasks:
        assert f'task_id="{task_name}"' in dag_text


def test_phase_3_helper_functions_exist() -> None:
    pipeline_tasks = read_text("airflow/include/pipeline_tasks.py")
    airflow_logging = read_text("airflow/include/airflow_logging.py")
    sql_quality_checks = read_text("airflow/include/sql_quality_checks.py")

    for function_name in [
        "get_project_root",
        "run_python_script",
        "check_required_paths",
        "check_required_files",
        "run_generate_metadata",
        "run_generate_batch_data",
        "run_generate_weather_data",
        "run_bronze_to_silver",
        "run_silver_to_gold",
        "run_load_gold_to_sql_server",
    ]:
        assert f"def {function_name}" in pipeline_tasks

    for function_name in [
        "create_pipeline_run_record",
        "update_pipeline_run_success",
        "update_pipeline_run_failure",
        "insert_data_quality_result",
    ]:
        assert f"def {function_name}" in airflow_logging

    assert "def run_sql_quality_checks" in sql_quality_checks


def test_phase_3_required_docs_and_docker_files_exist() -> None:
    required_files = [
        "airflow/README.md",
        "docker-compose.yml",
        "docker/airflow/Dockerfile",
        "docker/airflow/README.md",
        "config/airflow_config.example.yaml",
        ".env.example",
        "docs/phase_3_overview.md",
        "docs/orchestration_design.md",
        "docs/airflow_to_adf_mapping.md",
        "docs/operational_runbook.md",
    ]

    for relative_path in required_files:
        assert (PROJECT_ROOT / relative_path).exists()
