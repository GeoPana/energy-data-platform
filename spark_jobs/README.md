# Spark Jobs

Phase 2 Spark jobs implement a local batch medallion pipeline using PySpark and Parquet.

Run from the repository root:

```powershell
python spark_jobs/bronze_to_silver_batch.py
python spark_jobs/silver_to_gold_batch.py
python spark_jobs/load_gold_to_sql_server.py
```

## Jobs

| Job | Purpose |
| --- | --- |
| `bronze_to_silver_batch.py` | Reads bronze CSVs, applies explicit schemas, validates records, writes clean silver Parquet, and writes rejected records. |
| `silver_to_gold_batch.py` | Reads silver Parquet and creates business-ready gold outputs for regional, customer, monthly, weather, and summary analysis. |
| `load_gold_to_sql_server.py` | Loads curated silver/gold outputs into the Phase 1 SQL Server warehouse. |

## Common Helpers

| File | Purpose |
| --- | --- |
| `common/spark_session.py` | Creates a local Spark session. |
| `common/paths.py` | Loads config and centralizes bronze, silver, gold, and rejected-record paths. |
| `common/schemas.py` | Defines explicit schemas for bronze inputs. |
| `common/data_quality.py` | Provides reusable validation helpers and Spark rejection-reason expressions. |
| `common/logging_utils.py` | Prints simple structured job and metric logs. |

## File Format

Silver and gold outputs use Parquet. This keeps the local setup dependable with standard PySpark.

In an Azure Databricks implementation, these Parquet tables could be upgraded to Delta Lake tables to support ACID transactions, schema evolution, and `MERGE`-based upserts.

## SQL Server Loading

`load_gold_to_sql_server.py` expects the Phase 1 database and tables to already exist. It reads:

- `silver/clean_customer`
- `silver/clean_meter`
- `silver/clean_tariff`
- `gold/daily_customer_consumption`

Then it loads or updates:

- `dw.dim_region`
- `dw.dim_customer`
- `dw.dim_meter`
- `dw.dim_tariff`
- `dw.dim_date`
- `dw.fact_energy_consumption`

The fact load uses a local-development delete-and-insert pattern for the Phase 2 batch ID. A later production-style improvement can replace this with SQL Server `MERGE` logic.
