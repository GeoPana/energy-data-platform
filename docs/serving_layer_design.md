# Serving Layer Design

The serving layer sits between warehouse tables and consumers.

```text
dw and monitoring tables
    -> serving views
    -> FastAPI, Power BI, SQL analysts
```

## Why Use Serving Views

Serving views provide:

- stable consumer contracts
- cleaner column names
- pre-joined business context
- reduced exposure to internal warehouse design
- dashboard-ready shapes
- API-friendly result sets

Consumers should not query `staging` tables or raw facts directly.

## Why API And BI Should Not Depend On Raw Facts

Raw facts often require:

- dimension joins
- date transformations
- filtering
- deduplication assumptions
- operational context

If every consumer repeats that logic, the platform becomes inconsistent. Serving views centralize the contract.

## Azure Mapping

| Local Asset | Azure-Style Equivalent |
| --- | --- |
| SQL Server `serving` schema | Azure SQL / Synapse SQL serving schema |
| FastAPI local app | Azure App Service or Azure Functions |
| Power BI Desktop | Power BI Desktop / Power BI semantic model |
| Serving views | Curated SQL endpoints or data products |

## Future Hardening

Later phases could add:

- SQL permissions by schema
- API authentication
- semantic model definitions
- deployment automation
- automated view tests
