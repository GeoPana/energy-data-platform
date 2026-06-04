# Azure-Style Energy Lakehouse & Data Warehouse Platform

This repository is a portfolio Data Engineering project that models an Azure-style energy analytics platform using free local tools.

The project is built in phases. Phase 1 created the SQL Server warehouse foundation. Phase 2 adds a local batch lakehouse pipeline that generates source-style data, transforms it through bronze, silver, and gold layers with PySpark, and loads curated outputs into SQL Server.

## Current Scope

Implemented:

- Phase 1 SQL Server warehouse foundation.
- Phase 2 local batch lakehouse pipeline.
- Local data generation for metadata, energy consumption, and weather.
- Bronze raw-style CSV landing folders.
- Silver cleaned Parquet outputs and rejected records.
- Gold curated Parquet outputs.
- SQL Server loader for curated warehouse tables.
- Documentation and unit tests for core helper logic.

Intentionally not implemented yet:

- Airflow orchestration.
- Streaming ingestion.
- Azure Event Hubs Emulator.
- FastAPI services.
- Power BI reports.
- CI/CD pipelines.
- Cloud resources.

## Local Tool To Azure Concept Mapping

| Local Asset | Azure-Style Concept |
| --- | --- |
| SQL Server Developer Edition | Azure SQL Database / Synapse serving layer |
| `data_lake/bronze` | ADLS Gen2 bronze/raw zone |
| `data_lake/silver` | Databricks/Synapse silver cleaned tables |
| `data_lake/gold` | Curated gold lakehouse tables |
| PySpark local jobs | Databricks or Synapse Spark jobs |
| Parquet files | Local stand-in for Delta Lake tables |
| `monitoring` schema | Operational metadata and quality observability |

## Repository Structure

```text
.
|-- README.md
|-- .gitignore
|-- environment.yml
|-- config
|   `-- local_config.example.yaml
|-- data_lake
|   |-- bronze
|   |-- silver
|   `-- gold
|-- docs
|-- ingestion
|-- spark_jobs
|-- sql
`-- tests
```

## Phase 1: SQL Server Warehouse Foundation

Phase 1 creates a local SQL Server database named `EnergyWarehouse` with:

- `staging` schema for raw-like landing tables.
- `dw` schema for dimensions and facts.
- `monitoring` schema for pipeline and data-quality metadata.
- Dimension tables for dates, regions, customers, meters, and tariffs.
- Fact tables for energy consumption, meter readings, and anomaly events.
- Seed data, analytics SQL, data-quality SQL, and reset scripts.

Run Phase 1 scripts in order:

```powershell
sqlcmd -S localhost -E -i sql\01_create_database.sql
sqlcmd -S localhost -E -i sql\02_create_schemas.sql
sqlcmd -S localhost -E -i sql\03_create_dimensions.sql
sqlcmd -S localhost -E -i sql\04_create_facts.sql
sqlcmd -S localhost -E -i sql\05_seed_sample_data.sql
```

Optional:

```powershell
sqlcmd -S localhost -E -i sql\06_data_quality_checks.sql
sqlcmd -S localhost -E -i sql\07_analytics_queries.sql
```

## Phase 2: Batch Lakehouse Pipeline

Phase 2 simulates this batch flow:

```text
Python data generation
    -> local bronze data lake
    -> PySpark bronze-to-silver validation
    -> PySpark silver-to-gold transformation
    -> curated gold outputs
    -> SQL Server warehouse load
```

Phase 2 uses Parquet for silver and gold. Delta Lake can be enabled in a later Databricks-style phase, but Parquet keeps the local project reliable.

## Setup

Create and activate the local Conda environment:

```powershell
conda env create -f environment.yml
conda activate energy-data-platform
```

The environment includes `openjdk` for local PySpark. If Spark still cannot find Java in your shell, restart the terminal after activation.

The SQL Server load step also requires Microsoft ODBC Driver 18 for SQL Server installed on the machine. `pyodbc` is included in the Conda environment, but the native SQL Server ODBC driver is installed separately.

If the environment already exists and you want to update it:

```powershell
conda env update -f environment.yml --prune
conda activate energy-data-platform
```

Copy the config example if you want local overrides:

```powershell
copy config\local_config.example.yaml config\local_config.yaml
```

Then set:

```powershell
$env:ENERGY_PLATFORM_CONFIG = "config\local_config.yaml"
```

Do not commit real usernames, passwords, or local secrets.

## Run Phase 2

Generate bronze source data:

```powershell
python ingestion/generate_metadata.py
python ingestion/generate_batch_data.py
python ingestion/generate_weather_data.py
```

Transform bronze to silver:

```powershell
python spark_jobs/bronze_to_silver_batch.py
```

Transform silver to gold:

```powershell
python spark_jobs/silver_to_gold_batch.py
```

Load curated data to SQL Server:

```powershell
python spark_jobs/load_gold_to_sql_server.py
```

The SQL Server load expects the Phase 1 database and tables to already exist.

## Expected Phase 2 Outputs

Bronze:

- `data_lake/bronze/customer_metadata/`
- `data_lake/bronze/meter_metadata/`
- `data_lake/bronze/tariff/`
- `data_lake/bronze/historical_consumption/`
- `data_lake/bronze/weather/`

Silver:

- `data_lake/silver/clean_consumption/`
- `data_lake/silver/clean_weather/`
- `data_lake/silver/clean_customer/`
- `data_lake/silver/clean_meter/`
- `data_lake/silver/clean_tariff/`
- `data_lake/silver/rejected_records/`

Gold:

- `data_lake/gold/daily_region_consumption/`
- `data_lake/gold/daily_customer_consumption/`
- `data_lake/gold/monthly_region_consumption/`
- `data_lake/gold/consumption_weather_features/`
- `data_lake/gold/customer_usage_summary/`

## Tests

Run the basic unit tests:

```powershell
python -m pytest
```

The tests focus on pure Python helper logic and do not require SQL Server.

## Resetting Phase 1 Objects

To drop Phase 1 SQL Server objects:

```powershell
sqlcmd -S localhost -E -i sql\08_drop_all.sql
```

The reset script keeps the `EnergyWarehouse` database itself.

## Next Phases

Planned later phases:

- Airflow orchestration for the batch pipeline.
- Streaming-style smart-meter ingestion.
- Local Azure Event Hubs Emulator integration.
- FastAPI access patterns.
- Power BI dashboarding.
- Automated checks and CI/CD.
