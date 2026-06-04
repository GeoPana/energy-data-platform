# Phase 3 Airflow Orchestration

Phase 3 adds Apache Airflow orchestration for the existing Phase 2 batch lakehouse pipeline.

The DAG does not duplicate transformation logic. It calls the existing project scripts:

```text
generate_metadata
    -> generate_batch_energy_data
    -> generate_weather_data
    -> bronze_to_silver_batch
    -> silver_to_gold_batch
    -> load_gold_to_sql_server
    -> run_data_quality_checks
    -> write_pipeline_run_summary
```

## DAG

- DAG ID: `energy_batch_lakehouse_pipeline`
- Schedule: manual by default
- Retries: 1
- Tags: `portfolio`, `batch`, `lakehouse`, `sql-server`

## Docker Setup

From the repository root:

```powershell
copy .env.example .env
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Open:

```text
http://localhost:8080
```

Default local login:

- username: `airflow`
- password: `airflow`

## SQL Server Connection

The Airflow container connects to SQL Server using environment variables from `.env`.

For SQL Server Developer running on the Windows host, use:

```text
SQLSERVER_HOST=host.docker.internal
```

Windows trusted authentication usually does not work from a Linux container. SQL authentication is often simpler for the Docker Airflow path. Do not commit real passwords.

## Trigger The DAG

In the Airflow UI:

1. Open `energy_batch_lakehouse_pipeline`.
2. Unpause the DAG.
3. Click Trigger DAG.
4. Watch the Graph or Grid view.

CLI option:

```powershell
docker compose exec airflow-webserver airflow dags trigger energy_batch_lakehouse_pipeline
```

## Debug Underlying Scripts

You can still run Phase 2 manually from the repository root:

```powershell
python ingestion/generate_metadata.py
python ingestion/generate_batch_data.py
python ingestion/generate_weather_data.py
python spark_jobs/bronze_to_silver_batch.py
python spark_jobs/silver_to_gold_batch.py
python spark_jobs/load_gold_to_sql_server.py
```

This is useful when a DAG task fails and you want to isolate whether the issue is orchestration, Spark, file paths, or SQL Server connectivity.

## Troubleshooting

- If the DAG is not visible, check that `airflow/dags` is mounted and the scheduler is running.
- If Spark fails, check Java and PySpark inside the Airflow image.
- If SQL Server loading fails from Docker, check `SQLSERVER_HOST`, firewall rules, SQL authentication, and ODBC Driver 18.
- If monitoring writes fail, the DAG logs a warning during start/failure updates; the data-quality task still requires SQL Server.
