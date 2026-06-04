# Airflow Docker Runtime

This folder contains the Docker image used by Phase 3 Airflow.

The image extends the official Airflow image and adds:

- Java 17 for PySpark.
- PySpark.
- PyYAML.
- pyodbc.
- Microsoft ODBC Driver 18 for SQL Server.

Build and initialize through Docker Compose from the repository root:

```powershell
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

The Airflow web UI is available at:

```text
http://localhost:8080
```

Default local login comes from `.env.example`:

- username: `airflow`
- password: `airflow`

For Windows host SQL Server, `SQLSERVER_HOST=host.docker.internal` is usually required because `localhost` inside a container means the container itself.
