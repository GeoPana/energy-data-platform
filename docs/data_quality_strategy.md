# Data Quality Strategy

Phase 2 treats data quality as part of the pipeline, not an afterthought.

## Validation Rules

Historical consumption records are checked for:

- missing `meter_id`
- missing `customer_id`
- invalid `reading_timestamp`
- missing or invalid `kwh`
- negative `kwh`
- invalid London region
- duplicate `meter_id + reading_timestamp`

Weather records are checked for:

- invalid `weather_timestamp`
- missing or invalid `temperature_c`
- invalid London region

Metadata is cleaned with explicit schemas and duplicate business keys are removed.

## Rejected Records

Invalid records are written to:

```text
data_lake/silver/rejected_records/
```

Each rejected record includes:

- `source_system`
- `source_reference`
- `raw_payload`
- `rejection_reason`
- `rejected_at`
- `batch_id`

This makes failed records auditable and explainable.

## Why Preserve Invalid Records

Bad records should not silently disappear. Preserving rejected records helps with:

- source-system debugging
- data-quality reporting
- reconciliation between raw and curated counts
- future reprocessing after corrections
- interview discussion around operational maturity

## Professional Practice Mapping

In a production Azure platform, these checks might run in:

- Databricks notebooks or jobs
- Spark structured validation steps
- Great Expectations or another quality framework
- Airflow or Azure Data Factory pipeline tasks

This local implementation keeps the rules simple and visible in PySpark so the project remains easy to understand.

## Future Improvements

Later phases can add:

- automated pipeline orchestration
- data-quality run tables populated from Spark
- alerting on failed checks
- quarantine reprocessing
- SQL Server `MERGE`-based upserts
- Delta Lake constraints and expectations
