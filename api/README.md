# FastAPI Serving Layer

The Phase 5 API exposes curated SQL Server serving views as local HTTP endpoints.

The API maps conceptually to Azure App Service or Azure Functions, but it runs locally with FastAPI.

## Requirements

- SQL Server Developer running locally.
- `EnergyWarehouse` database created.
- Phase 1 warehouse tables created.
- Serving views created with `sql/09_create_serving_schema_and_views.sql`.
- Warehouse populated by seed data or previous pipeline phases.
- Microsoft ODBC Driver 18 for SQL Server installed.

## Environment Variables

```powershell
$env:SQLSERVER_DRIVER = "ODBC Driver 18 for SQL Server"
$env:SQLSERVER_HOST = "localhost"
$env:SQLSERVER_DATABASE = "EnergyWarehouse"
$env:SQLSERVER_TRUSTED_CONNECTION = "true"
$env:SQLSERVER_USERNAME = ""
$env:SQLSERVER_PASSWORD = ""
$env:API_ENV = "local"
$env:API_HOST = "127.0.0.1"
$env:API_PORT = "8000"
```

Do not commit real usernames or passwords.

## Run Locally

```powershell
conda env update -f environment.yml --prune
conda activate energy-data-platform

uvicorn api.main:app --reload --host 127.0.0.1 --port 8000
```

Open:

```text
http://127.0.0.1:8000/docs
```

## Example Endpoints

- `GET /health`
- `GET /version`
- `GET /regions`
- `GET /regions/East%20London/daily-consumption`
- `GET /regions/East%20London/monthly-consumption`
- `GET /meters/MTR-LON-001/latest-reading`
- `GET /meters/latest-readings`
- `GET /anomalies`
- `GET /anomalies/region/East%20London`
- `GET /consumption/daily`
- `GET /consumption/monthly`
- `GET /consumption/customer/CUST-LON-001`
- `GET /dashboard/kpis`
- `GET /monitoring/pipeline-runs`
- `GET /monitoring/data-quality`
- `GET /monitoring/freshness`

## Troubleshooting

- If `/health` works but data endpoints fail, check SQL Server connectivity.
- If SQL Server rejects the connection, confirm ODBC Driver 18 is installed.
- If views are missing, run `sql/09_create_serving_schema_and_views.sql`.
- If endpoints return empty lists, run Phase 1 seed data or previous pipeline loads.
- If using SQL authentication, set `SQLSERVER_TRUSTED_CONNECTION=false`, `SQLSERVER_USERNAME`, and `SQLSERVER_PASSWORD`.
