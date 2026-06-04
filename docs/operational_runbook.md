# Operational Runbook

This runbook explains how to operate the Phase 3 Airflow pipeline locally.

## Start Airflow

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

## Trigger The DAG

In the UI:

1. Open `energy_batch_lakehouse_pipeline`.
2. Unpause the DAG.
3. Click Trigger DAG.

CLI:

```powershell
docker compose exec airflow-webserver airflow dags trigger energy_batch_lakehouse_pipeline
```

## Inspect Logs

In the UI:

1. Open the DAG.
2. Go to Grid view.
3. Click a task instance.
4. Open Logs.

CLI:

```powershell
docker compose logs --tail 200 airflow-scheduler
docker compose logs --tail 200 airflow-webserver
```

## Rerun A Failed Task

In the UI:

1. Open the failed DAG run.
2. Select the failed task.
3. Clear the task.
4. Let Airflow rerun it.

CLI pattern:

```powershell
docker compose exec airflow-webserver airflow tasks clear energy_batch_lakehouse_pipeline --task-regex TASK_ID --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

## Check SQL Server Output

After a successful run, inspect:

```sql
SELECT COUNT(*) FROM dw.fact_energy_consumption;
SELECT TOP (20) * FROM monitoring.pipeline_run ORDER BY started_at DESC;
SELECT TOP (20) * FROM monitoring.data_quality_check ORDER BY checked_at DESC;
```

You can also run:

```powershell
sqlcmd -S localhost -E -d EnergyWarehouse -Q "SELECT COUNT(*) AS fact_rows FROM dw.fact_energy_consumption;"
```

## Common Failures

### DAG Does Not Appear

Check:

- `airflow/dags/energy_batch_pipeline.py` exists.
- `airflow-scheduler` is running.
- DAG import errors in the Airflow UI.

### SQL Server Cannot Be Reached From Docker

Check:

- `.env` has `SQLSERVER_HOST=host.docker.internal`.
- SQL Server allows remote TCP connections.
- Firewall permits the SQL Server port.
- SQL authentication is configured if Windows trusted authentication is not available from Docker.

### PySpark Fails

Check:

- Airflow image was rebuilt after Dockerfile changes.
- Java exists in the container.
- The task log includes PySpark startup details.

Rebuild:

```powershell
docker compose build --no-cache airflow-webserver airflow-scheduler
```

### Data-Quality Task Fails

Check:

- Phase 1 SQL scripts have been run.
- Phase 2 load task completed successfully.
- `monitoring.pipeline_run` and `monitoring.data_quality_check` exist.
- The failed check name and failed count in task logs.

## Recovery Steps

1. Fix the root cause.
2. Rerun the failed task if upstream outputs are still valid.
3. Rerun the full DAG if source data or gold outputs need to be regenerated.
4. Use `sql/08_drop_all.sql` only when you intentionally want to reset Phase 1 warehouse objects.
