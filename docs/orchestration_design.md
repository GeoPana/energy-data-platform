# Orchestration Design

Phase 3 separates orchestration logic from transformation logic.

The Airflow DAG controls task order, retries, logging, and monitoring. The actual data generation and transformation still lives in the Phase 2 scripts.

## DAG Structure

The DAG is defined in:

```text
airflow/dags/energy_batch_pipeline.py
```

Task helpers live in:

```text
airflow/include/
```

The DAG is intentionally linear for the first orchestration version:

```text
start_pipeline
    -> check_project_structure
    -> generate_metadata
    -> generate_batch_energy_data
    -> generate_weather_data
    -> run_bronze_to_silver
    -> run_silver_to_gold
    -> load_gold_to_sql_server
    -> run_data_quality_checks
    -> write_pipeline_run_summary
    -> end_pipeline
```

Metadata and weather generation could be parallelized later, but a simple linear DAG is easier to review and debug.

## Why Orchestration Is Separate

Airflow should not contain transformation logic. It should answer operational questions:

- What runs first?
- What depends on what?
- What retries if a task fails?
- Where are logs?
- Which run failed?
- Which quality checks failed?

PySpark scripts should answer data questions:

- How is bronze cleaned?
- How are silver tables validated?
- How are gold tables aggregated?
- How is curated data loaded to SQL Server?

This separation mirrors professional data engineering practice.

## Retry Behavior

The DAG sets:

- `retries=1`
- `retry_delay=5 minutes`
- `catchup=False`

This is enough to demonstrate retry policy without making local debugging noisy.

## Logging Strategy

`airflow/include/pipeline_tasks.py` runs Phase 2 scripts through `subprocess.run` and captures:

- script path
- command
- start time
- stdout
- stderr
- non-zero exit code

Airflow task logs become the primary operational log surface.

## Failure Behavior

If a task fails:

- Airflow marks the task failed.
- Airflow applies the retry policy.
- The failure callback tries to update `monitoring.pipeline_run`.
- Monitoring write failures are logged as warnings so a temporary monitoring issue does not hide the root task failure.

The `run_data_quality_checks` task is stricter: SQL Server must be available because the purpose of that task is to validate warehouse output.

## Manual vs Scheduled Runs

The DAG is manual by default with `schedule=None`.

This is best for a portfolio MVP because it avoids background runs while you are developing. A daily schedule can be added later after the local pipeline is stable.
