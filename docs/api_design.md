# API Design

Phase 5 adds a FastAPI app that reads from SQL Server serving views.

## Endpoint Groups

- Health: `/health`, `/version`
- Regions: `/regions`, regional daily and monthly consumption
- Meters: latest meter readings
- Anomalies: anomaly detail endpoints
- Consumption: daily, monthly, customer, dashboard KPIs
- Monitoring: pipeline runs, data quality, freshness

## Database Access

The API uses `pyodbc` and parameterized SQL. It does not use a full ORM because the API is a thin read-only serving layer over views.

All query definitions live in:

```text
api/services/warehouse_queries.py
```

## Parameterized Queries

User inputs such as region, dates, meter ID, status, and limit are passed as parameters. This avoids string-built SQL and reduces SQL injection risk.

## Error Handling

Warehouse connection and query errors are wrapped in a clear API response with HTTP `503`.

The health endpoint does not require a database connection, which makes it useful for checking whether the API process is alive separately from SQL Server availability.

## OpenAPI Docs

FastAPI automatically exposes docs at:

```text
http://127.0.0.1:8000/docs
```

## Local To Azure Mapping

| Local Component | Azure-Style Equivalent |
| --- | --- |
| FastAPI with uvicorn | Azure App Service |
| FastAPI endpoint | HTTP-triggered Azure Function or App Service route |
| Environment variables | App Service configuration |
| SQL Server Developer | Azure SQL Database |
| Serving views | SQL data product contract |

Authentication is intentionally deferred to a later production-hardening phase.
