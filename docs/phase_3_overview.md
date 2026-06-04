# Phase 3 Overview

Phase 3 adds Apache Airflow orchestration to the Azure-Style Energy Lakehouse & Data Warehouse Platform.

Phase 1 created the SQL Server warehouse foundation. Phase 2 created the local batch lakehouse pipeline. Phase 3 connects those pieces into a single orchestrated workflow.

## What Phase 3 Adds

- A local Airflow DAG named `energy_batch_lakehouse_pipeline`.
- Docker Compose support for Airflow webserver, scheduler, init, and Postgres metadata database.
- Task helpers that run the existing Phase 2 scripts.
- SQL Server monitoring helpers that write to Phase 1 monitoring tables when available.
- SQL Server data-quality checks after the warehouse load.
- Documentation that maps Airflow concepts to Azure Data Factory concepts.
- A lightweight structure test that does not require a running Airflow instance.

## Orchestrated Flow

```text
start_pipeline
    -> check_project_structure
    -> generate_metadata
    -> generate_batch_energy_data
    -> generate_weather_data
    -> run_bronze_to_silver
    -> run_silver_to_gold
    -> load_gold_to_sql_server
    -> run_data_quality_checks
    -> write_pipeline_run_summary
    -> end_pipeline
```

## What Phase 3 Does Not Add

This phase intentionally does not implement:

- streaming ingestion
- Event Hubs Emulator
- FastAPI
- Power BI
- Azure cloud deployment
- CI/CD

Those are later phases. Phase 3 is focused on orchestration, dependency management, retries, monitoring, and operational visibility.
