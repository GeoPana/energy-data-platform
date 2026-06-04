# Batch Pipeline Design

Phase 2 follows a medallion-style batch pipeline:

```text
Source simulation
    Python generation scripts

Bronze
    Raw CSV files in local folders

Silver
    Cleaned, typed, validated Parquet datasets
    Rejected records preserved with reasons

Gold
    Business-ready Parquet outputs

Warehouse
    Curated SQL Server dimensions and facts
```

## 1. Python Data Generation

The `ingestion` scripts simulate source-system extracts:

- `generate_metadata.py` creates regions, customers, meters, and tariffs.
- `generate_batch_data.py` creates half-hourly historical consumption.
- `generate_weather_data.py` creates hourly weather observations.

The generated bronze files are intentionally simple CSVs because many real batch platforms still begin with files from source systems.

## 2. Bronze Layer

Bronze stores raw-like data with minimal assumptions:

- Consumption timestamps and kWh values arrive as strings.
- Weather values arrive as strings.
- Invalid records can exist.
- Partition-like folders organize daily consumption and weather files.

The bronze layer is not loaded directly to SQL Server.

## 3. Bronze To Silver

`spark_jobs/bronze_to_silver_batch.py` reads bronze CSVs with explicit schemas, casts values into useful types, and applies validation rules.

Consumption validation includes:

- `meter_id` present.
- `customer_id` present.
- timestamp parseable.
- `kwh` present.
- `kwh >= 0`.
- valid London region.
- duplicate `meter_id + reading_timestamp` flagged.

Invalid rows are written to `data_lake/silver/rejected_records/`.

## 4. Silver To Gold

`spark_jobs/silver_to_gold_batch.py` creates business-ready outputs:

- daily consumption by region
- daily consumption by customer
- monthly consumption by region
- consumption joined to hourly weather
- customer usage summaries

These outputs are the local equivalent of curated gold lakehouse tables.

## 5. Gold To SQL Server

`spark_jobs/load_gold_to_sql_server.py` loads curated silver/gold outputs into the Phase 1 SQL Server warehouse.

It loads dimensions from clean metadata and daily consumption facts from gold. This keeps SQL Server as a curated serving layer rather than a raw landing zone.

The current fact load is intentionally simple for local development: it deletes the Phase 2 batch ID and inserts the latest generated rows. A future hardening pass can replace this with `MERGE` logic.
