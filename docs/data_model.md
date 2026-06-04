# Data Model

The Phase 1 model separates raw-like landing data, dimensional warehouse data, and monitoring metadata.

## Schemas

| Schema | Purpose |
| --- | --- |
| `staging` | Landing area for raw batch files, event payloads, and rejected records. |
| `dw` | Dimensional warehouse tables for analytics. |
| `monitoring` | Pipeline run and data-quality execution metadata. |

## Dimensions

### `dw.dim_date`

Calendar dimension keyed by `date_id` in `YYYYMMDD` format. It supports daily, monthly, quarterly, yearly, weekday, and weekend analysis.

Key columns:

- `date_id`
- `full_date`
- `year_number`
- `quarter_number`
- `month_number`
- `month_name`
- `day_of_month`
- `day_of_week_name`
- `is_weekend`

### `dw.dim_region`

London-style geography dimension for regional analysis.

Key columns:

- `region_id`
- `region_name`
- `country`
- `postcode_area`
- `created_at`

### `dw.dim_customer`

Customer attributes used for segmentation and customer-level consumption analysis.

Key columns:

- `customer_id`
- `customer_segment`
- `household_size`
- `dwelling_type`
- `region_id`
- `signup_date`
- `is_active`
- `created_at`

### `dw.dim_meter`

Meter dimension that connects physical meters to customers and regions.

Key columns:

- `meter_id`
- `customer_id`
- `region_id`
- `meter_type`
- `installation_date`
- `is_active`
- `created_at`

### `dw.dim_tariff`

Tariff and pricing dimension used to calculate or analyze estimated energy cost.

Key columns:

- `tariff_id`
- `tariff_name`
- `valid_from`
- `valid_to`
- `price_per_kwh`
- `standing_charge_daily`
- `is_active`
- `created_at`

## Facts

### `dw.fact_energy_consumption`

Batch or historical consumption fact. This represents curated daily consumption records that later PySpark or orchestration phases would load from a gold layer.

Important relationships:

- `meter_id` to `dw.dim_meter`
- `customer_id` to `dw.dim_customer`
- `region_id` to `dw.dim_region`
- `tariff_id` to `dw.dim_tariff`
- `date_id` to `dw.dim_date`

Measures:

- `kwh`
- `estimated_cost`

Useful analysis:

- Daily consumption by region.
- Monthly consumption by region.
- Top meters by usage.
- Cost by tariff.
- Weekday versus weekend consumption.

### `dw.fact_meter_reading`

Streaming-style smart-meter fact. It stores individual readings with event identifiers and load timestamps.

Important relationships:

- `meter_id` to `dw.dim_meter`
- `customer_id` to `dw.dim_customer`
- `region_id` to `dw.dim_region`
- `date_id` to `dw.dim_date`

Measures:

- `kwh`
- `voltage`

Useful analysis:

- Latest reading per meter.
- Data freshness.
- Event-level monitoring.

### `dw.fact_anomaly_event`

Anomaly fact used to record detected consumption or meter-reading anomalies.

Important relationships:

- Optional `reading_id` to `dw.fact_meter_reading`
- `meter_id` to `dw.dim_meter`
- `region_id` to `dw.dim_region`
- `date_id` to `dw.dim_date`

Measures and attributes:

- `anomaly_type`
- `anomaly_score`
- `kwh`
- `expected_kwh`
- `detected_at`
- `source_system`

Useful analysis:

- Anomalies by region.
- Spike detection review.
- Operational data-quality investigation.

## Staging Tables

### `staging.raw_energy_consumption`

Raw batch landing table. Values such as timestamps and kWh are stored as strings because raw files can contain invalid or inconsistent values before validation.

### `staging.raw_meter_reading_events`

Raw JSON/event landing table. The `raw_payload` column stores the original payload before parsing, validation, and loading.

### `staging.rejected_records`

Stores rejected raw records with source information and rejection reasons. This keeps invalid records visible instead of silently discarding them.

## Monitoring Tables

### `monitoring.pipeline_run`

Stores one row per pipeline execution, including status, timing, counts, and error details.

### `monitoring.data_quality_check`

Stores data-quality check outcomes linked back to a pipeline run.

This allows quality failures to be queried alongside warehouse health and operational status.
