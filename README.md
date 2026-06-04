# Azure-Style Energy Lakehouse & Data Warehouse Platform

This repository is a portfolio Data Engineering project that models an Azure-style energy analytics platform using free local tools.

The project is built in phases. Phase 1 created the SQL Server warehouse foundation. Phase 2 adds a local batch lakehouse pipeline that generates source-style data, transforms it through bronze, silver, and gold layers with PySpark, and loads curated outputs into SQL Server.
Phase 3 orchestrates that batch pipeline with Apache Airflow.
Phase 4 adds an Event Hubs-style real-time streaming path with PySpark Structured Streaming.

## Current Scope

Implemented:

- Phase 1 SQL Server warehouse foundation.
- Phase 2 local batch lakehouse pipeline.
- Phase 3 Apache Airflow batch orchestration.
- Phase 4 smart-meter streaming pipeline.
- Local data generation for metadata, energy consumption, and weather.
- Local smart-meter event generation.
- Bronze raw-style CSV landing folders.
- Silver cleaned Parquet outputs and rejected records.
- Gold curated Parquet outputs.
- Streaming bronze, silver, gold, checkpoint, anomaly, and metrics outputs.
- SQL Server loader for curated warehouse tables.
- Airflow DAG, retries, task dependencies, SQL quality checks, and monitoring hooks.
- Azure Event Hubs Emulator configuration with file-mode fallback.
- Documentation and unit tests for core helper logic.

Intentionally not implemented yet:

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
| Airflow DAG | Azure Data Factory pipeline |
| Airflow task | Azure Data Factory activity |
| Event Hubs Emulator | Azure Event Hubs |
| PySpark Structured Streaming | Azure Databricks Structured Streaming |
| Parquet files | Local stand-in for Delta Lake tables |
| `monitoring` schema | Operational metadata and quality observability |

## Repository Structure

```text
.
|-- README.md
|-- .gitignore
|-- environment.yml
|-- docker-compose.yml
|-- airflow
|-- checkpoints
|-- config
|   |-- airflow_config.example.yaml
|   `-- local_config.example.yaml
|-- data_lake
|   |-- bronze
|   |-- silver
|   `-- gold
|-- docker
|-- docs
|-- ingestion
|-- spark_jobs
|-- streaming
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

## Phase 3: Airflow Orchestration

Phase 3 stops running Phase 2 scripts manually and orchestrates them through Airflow.

DAG:

```text
energy_batch_lakehouse_pipeline
```

Task flow:

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

Start Airflow with Docker Compose:

```powershell
copy .env.example .env
docker compose up airflow-init
docker compose up airflow-webserver airflow-scheduler
```

Open:

```text
http://localhost:8080
```

Default local credentials:

- username: `airflow`
- password: `airflow`

Trigger from the UI or CLI:

```powershell
docker compose exec airflow-webserver airflow dags trigger energy_batch_lakehouse_pipeline
```

For Docker on Windows, SQL Server Developer running on the host usually requires:

```text
SQLSERVER_HOST=host.docker.internal
```

The DAG writes best-effort pipeline status to `monitoring.pipeline_run` and inserts SQL data-quality results into `monitoring.data_quality_check` when SQL Server is available.

## Phase 4: Real-Time Streaming Pipeline

Phase 4 adds smart-meter streaming:

```text
Python smart-meter producer
    -> Azure Event Hubs Emulator or local JSON landing files
    -> PySpark Structured Streaming
    -> streaming bronze/silver/gold lakehouse outputs
    -> optional SQL Server load
```

Update the Conda environment:

```powershell
conda env update -f environment.yml --prune
conda activate energy-data-platform
```

Run the reliable local file-mode path:

```powershell
python streaming/producer/smart_meter_producer.py --mode file --events-per-second 5 --duration-seconds 60
python streaming/consumer/stream_bronze_to_silver_gold.py --source-format json_landing --source-path data_lake/bronze/streaming_events_landing --available-now
```

Run with a bronze capture step:

```powershell
python streaming/consumer/stream_eventhub_to_bronze.py --mode file --available-now
python streaming/consumer/stream_bronze_to_silver_gold.py --available-now
```

Start the Event Hubs Emulator:

```powershell
$env:ACCEPT_EULA = "Y"
$env:CONFIG_PATH = "./Config.json"
docker compose -f streaming/emulator/docker-compose.eventhubs.yml up
```

Run Event Hubs producer mode:

```powershell
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
$env:EVENTHUB_NAME = "smart-meter-events"
python streaming/producer/smart_meter_producer.py --mode eventhub --events-per-second 5 --duration-seconds 60
```

Run Event Hubs to bronze mode:

```powershell
$env:EVENTHUB_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:EVENTHUB_CONNECTION_STR = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;"
python streaming/consumer/stream_eventhub_to_bronze.py --mode eventhub --continuous
```

Optional SQL Server load:

```powershell
python streaming/consumer/load_streaming_gold_to_sql_server.py
```

Checkpoints are stored under:

```text
checkpoints/eventhub_to_bronze/
checkpoints/bronze_to_silver_gold/
```

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

Streaming outputs:

- `data_lake/bronze/streaming_events/`
- `data_lake/bronze/streaming_events_landing/`
- `data_lake/silver/clean_streaming_events/`
- `data_lake/silver/rejected_streaming_events/`
- `data_lake/gold/latest_meter_readings/`
- `data_lake/gold/streaming_anomaly_events/`
- `data_lake/gold/hourly_streaming_consumption/`
- `data_lake/gold/streaming_pipeline_metrics/`

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

- FastAPI access patterns.
- Power BI dashboarding.
- Automated checks and CI/CD.
