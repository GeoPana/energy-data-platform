# Azure-Style Energy Lakehouse & Data Warehouse Platform

This repository is a portfolio Data Engineering project that models an Azure-style energy analytics platform using free local tools.

Phase 1 creates the SQL Server warehouse foundation only. It defines the local serving layer that later lakehouse, orchestration, streaming, API, and reporting phases can load and consume.

## Phase 1 Scope

Implemented in this phase:

- SQL Server database bootstrap for `EnergyWarehouse`.
- Separate schemas for raw-like landing, dimensional warehouse, and monitoring data:
  - `staging`
  - `dw`
  - `monitoring`
- Star-schema warehouse tables for dates, regions, customers, meters, tariffs, consumption facts, meter readings, and anomaly events.
- Staging tables for batch files, event payloads, and rejected records.
- Monitoring tables for pipeline runs and data-quality checks.
- Coherent London-style sample data.
- Interview-relevant analytics queries.
- Data-quality investigation queries.
- Reset script for dropping Phase 1 objects.
- Documentation explaining the model and design decisions.

Intentionally not implemented yet:

- PySpark processing.
- Airflow orchestration.
- Streaming ingestion.
- FastAPI services.
- Power BI reports.
- Azure Event Hubs Emulator.
- CI/CD pipelines.
- Cloud resources or paid Azure services.

## Local Tool To Azure Concept Mapping

| Local Phase 1 Asset | Azure-Style Concept |
| --- | --- |
| SQL Server Developer Edition | Azure SQL Database / Synapse serving layer |
| `staging` schema | Raw or landing zone tables |
| `dw` schema | Curated warehouse / dimensional serving model |
| `monitoring` schema | Operational metadata and data-quality observability |
| SQL seed data | Small local stand-in for curated lakehouse output |
| Analytics SQL | Warehouse consumption layer for BI and analysis |
| Data-quality SQL | Validation checks normally run during pipelines |

## Repository Structure

```text
.
├── README.md
├── docs
│   ├── data_model.md
│   ├── design_decisions.md
│   └── phase_1_overview.md
└── sql
    ├── 01_create_database.sql
    ├── 02_create_schemas.sql
    ├── 03_create_dimensions.sql
    ├── 04_create_facts.sql
    ├── 05_seed_sample_data.sql
    ├── 06_data_quality_checks.sql
    ├── 07_analytics_queries.sql
    ├── 08_drop_all.sql
    └── README.md
```

## Prerequisites

- SQL Server Developer Edition, SQL Server Express, or a local SQL Server container.
- SQL Server Management Studio, Azure Data Studio, VS Code with the MSSQL extension, or `sqlcmd`.
- A login with permission to create a local database.

No secrets, passwords, or cloud credentials are required by this phase.

## How To Run

Run the scripts in order:

1. `sql/01_create_database.sql`
2. `sql/02_create_schemas.sql`
3. `sql/03_create_dimensions.sql`
4. `sql/04_create_facts.sql`
5. `sql/05_seed_sample_data.sql`
6. Optional validation: `sql/06_data_quality_checks.sql`
7. Optional analytics: `sql/07_analytics_queries.sql`

Example with Windows authentication:

```powershell
sqlcmd -S localhost -E -i sql\01_create_database.sql
sqlcmd -S localhost -E -i sql\02_create_schemas.sql
sqlcmd -S localhost -E -i sql\03_create_dimensions.sql
sqlcmd -S localhost -E -i sql\04_create_facts.sql
sqlcmd -S localhost -E -i sql\05_seed_sample_data.sql
```

If you use SQL authentication, replace `-E` with your local authentication options.

The scripts contain `USE EnergyWarehouse;` statements where needed, so they can also be opened and run directly in SQL Server Management Studio, Azure Data Studio, or the VS Code MSSQL extension.

## Expected Outputs

After running the setup and seed scripts, the database should contain:

- 5 London-style regions.
- 10 customers.
- 10 meters.
- 3 tariffs.
- 14 date dimension rows.
- 140 batch consumption fact rows.
- 60 smart-meter reading fact rows.
- 3 anomaly events.
- Sample pipeline run metadata.
- Sample data-quality check metadata.
- Sample staging and rejected-record rows.

The analytics script returns regional consumption, customer segment usage, tariff cost, anomaly, freshness, and pipeline monitoring examples.

The data-quality script returns investigation result sets for null keys, negative usage, duplicates, missing dimension references, inactive customer assignments, future timestamps, rejected records, and failed pipeline or quality checks.

## Resetting Phase 1 Objects

To drop all Phase 1 tables and schemas, run:

```powershell
sqlcmd -S localhost -E -i sql\08_drop_all.sql
```

The reset script intentionally keeps the `EnergyWarehouse` database itself, but drops Phase 1 objects inside it.

## Next Phases

Planned later phases:

- Lakehouse-style bronze, silver, and gold processing with PySpark.
- Batch orchestration with Airflow.
- Streaming-style smart-meter ingestion.
- API access patterns with FastAPI.
- Dashboarding with Power BI.
- Local Azure Event Hubs Emulator integration.
- Automated checks and CI/CD.

Phase 1 keeps the project grounded: a usable warehouse contract that later ingestion and transformation work can target.
