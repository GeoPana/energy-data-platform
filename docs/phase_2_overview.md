# Phase 2 Overview

Phase 2 adds a local batch lakehouse pipeline to the Azure-Style Energy Lakehouse & Data Warehouse Platform.

The goal is to simulate an Azure-style medallion architecture without using cloud resources:

```text
Python data generation
    -> bronze local data lake
    -> PySpark bronze-to-silver validation
    -> PySpark silver-to-gold transformations
    -> curated gold outputs
    -> SQL Server warehouse load
```

## What Phase 2 Implements

- Python scripts that generate local source-style batch data.
- Bronze CSV files for:
  - historical half-hourly energy consumption
  - hourly weather data
  - customer metadata
  - meter metadata
  - tariff metadata
- PySpark validation and typing from bronze to silver.
- Silver rejected-record capture with rejection reasons.
- Gold business outputs:
  - daily region consumption
  - daily customer consumption
  - monthly region consumption
  - consumption and weather features
  - customer usage summary
- A SQL Server loader for curated warehouse tables.
- Unit tests for pure helper logic.
- Documentation for the lakehouse layers and quality strategy.

## What Phase 2 Does Not Implement

This phase intentionally does not add:

- Airflow orchestration.
- Streaming or Event Hubs Emulator.
- FastAPI services.
- Power BI dashboards.
- CI/CD.
- Cloud infrastructure.

Those are later phases. Phase 2 focuses on batch ETL/ELT and local medallion architecture.

## Expected Local Data Volumes

The default generators create approximately:

- 5 regions.
- 100 customers.
- 100 meters.
- 3 tariffs.
- 30 days of half-hourly meter readings.
- 30 days of hourly weather data.
- A small number of intentionally invalid rows for quality testing.

The exact row count can vary slightly if the generator settings are changed, but the default consumption output is around 144,000 valid source rows plus invalid test records.

## Why Parquet

Phase 2 uses Parquet for silver and gold outputs because it is reliable with standard local PySpark.

Delta Lake is the natural next step for an Azure Databricks implementation, but Phase 2 prioritizes a working local project over extra dependency friction.
