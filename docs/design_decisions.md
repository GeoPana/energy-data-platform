# Design Decisions

## SQL Server As The Warehouse

SQL Server Developer Edition is used because it is free for local development and supports the T-SQL features expected in many professional data engineering environments:

- Relational constraints.
- Schemas.
- Identity columns.
- Window functions.
- CTEs.
- Indexes.
- SQL Server Management Studio, Azure Data Studio, VS Code MSSQL, and `sqlcmd`.

In an Azure implementation, this layer maps most closely to an Azure SQL Database, Azure SQL Managed Instance, or a serving model in Synapse.

## Star Schema For Analytics

The `dw` schema uses a star-schema style because it gives portfolio reviewers a familiar analytics model:

- Dimensions describe business entities such as dates, regions, customers, meters, and tariffs.
- Facts store measurable events such as consumption, readings, and anomalies.
- Queries remain readable and interview-friendly.
- Business metrics can be grouped by region, date, customer segment, meter, and tariff.

This is intentionally simpler than a heavily normalized operational model.

## Separate `staging` And `dw` Schemas

The `staging` schema stores raw-like inputs before validation. It allows loose typing for values that may arrive as strings or malformed payloads.

The `dw` schema stores validated analytical data with stronger constraints and relationships.

Keeping these layers separate makes the later ingestion path clear:

```text
raw file or event → staging table → validation and transformation → dw table
```

## Monitoring Tables In Phase 1

Monitoring is included from the start because warehouse foundations should capture operational context, not only business facts.

`monitoring.pipeline_run` and `monitoring.data_quality_check` provide a simple local equivalent of the operational metadata that would usually be produced by Airflow, Azure Data Factory, Databricks workflows, or another orchestrator.

## Rejected Records Are Stored

Rejected records are stored so data issues can be audited and fixed. Without a rejected-record table, bad inputs either disappear or block the entire load.

The rejected-record pattern supports:

- Debugging source data problems.
- Explaining count differences between source and warehouse.
- Building quality dashboards later.
- Reprocessing corrected records in a future phase.

## Data-Quality Checks Are Part Of The Foundation

Data-quality checks are included now because they shape the warehouse contract. The checks focus on problems that matter in energy data:

- Missing meter or customer identifiers.
- Negative consumption values.
- Duplicate events.
- Missing dimension references.
- Inactive customer assignments.
- Future timestamps.
- Failed pipeline and quality status.

Future phases can turn these SQL checks into automated pipeline tasks, but the logic is useful immediately for local validation.

## Rerunnable SQL Scripts

The setup scripts use `IF NOT EXISTS`, `OBJECT_ID`, and idempotent seed patterns where practical. This keeps local development comfortable without introducing migration tooling before it is needed.

The reset script is available for a clean rebuild when table definitions need to change during Phase 1.

## Avoided Patterns

The project intentionally avoids overcomplicated enterprise patterns in Phase 1:

- No slowly changing dimension implementation yet.
- No stored procedure loading framework yet.
- No external orchestration code yet.
- No cloud resource provisioning yet.
- No secrets or environment-specific connection files.

The result is a clear warehouse foundation that can grow naturally in later phases.
