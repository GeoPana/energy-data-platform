# Phase 1 Overview

Phase 1 builds the SQL Server warehouse foundation for the Azure-Style Energy Lakehouse & Data Warehouse Platform.

The goal is to establish a realistic serving-layer contract before adding processing engines, orchestration, streaming, APIs, or dashboards. Later phases can load curated gold data into this warehouse without redesigning the core analytics model.

## What Phase 1 Delivers

Phase 1 creates:

- A local SQL Server database named `EnergyWarehouse`.
- Three schemas:
  - `staging` for raw-like landing and rejected records.
  - `dw` for dimensional warehouse tables.
  - `monitoring` for pipeline and quality metadata.
- Dimensions for dates, regions, customers, meters, and tariffs.
- Facts for historical energy consumption, smart-meter readings, and anomaly events.
- Sample data that is internally consistent and large enough for meaningful queries.
- Data-quality investigation queries.
- Analytics queries using joins, CTEs, aggregation, and window functions.
- Documentation that explains the model and implementation choices.

## What Phase 1 Does Not Deliver

This phase deliberately does not implement:

- PySpark jobs.
- Airflow DAGs.
- Streaming producers or consumers.
- FastAPI endpoints.
- Power BI dashboards.
- Azure Event Hubs Emulator.
- CI/CD workflows.
- Cloud infrastructure.

Those components depend on having a stable warehouse target. This phase defines that target first.

## Architectural Shape

The repository models a common data platform flow:

```text
Raw source extracts and events
        ↓
staging schema
        ↓
validation and transformation
        ↓
dw dimensional model
        ↓
analytics, monitoring, reporting
```

Only the SQL Server objects are implemented now. The transformation step is represented by sample seed data and queries rather than external processing code.

## Phase 1 Success Criteria

Phase 1 is complete when:

- The database and schemas can be created locally.
- The warehouse tables enforce basic relational integrity.
- Sample data can be loaded repeatedly without accidental duplication.
- Data-quality queries can be used to inspect common failure modes.
- Analytics queries return meaningful result sets.
- Documentation explains how the local implementation maps to cloud-style data engineering concepts.

## Suggested Manual Validation

After running scripts `01` through `05`, open SQL Server Management Studio, Azure Data Studio, or VS Code MSSQL and confirm:

- `EnergyWarehouse` exists.
- Schemas `staging`, `dw`, and `monitoring` exist.
- Tables are present under each schema.
- `dw.fact_energy_consumption` contains 140 rows.
- `dw.fact_meter_reading` contains 60 rows.
- `sql/07_analytics_queries.sql` returns non-empty analytical results.

TODO for later phases: replace SQL-only seed data with curated output loaded from a lakehouse pipeline.
