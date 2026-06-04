/*
    Phase 1 - Data-Quality Investigation Queries

    These queries are written as analyst-friendly checks. They return rows
    that should be investigated, or grouped summaries where useful.
*/

USE EnergyWarehouse;
GO

-- Checks for missing meter identifiers in fact tables. Meter IDs are essential for usage attribution and downstream billing-style analysis.
SELECT 'dw.fact_energy_consumption' AS table_name, CAST(consumption_id AS VARCHAR(100)) AS record_id, meter_id
FROM dw.fact_energy_consumption
WHERE meter_id IS NULL
UNION ALL
SELECT 'dw.fact_meter_reading' AS table_name, reading_id AS record_id, meter_id
FROM dw.fact_meter_reading
WHERE meter_id IS NULL
UNION ALL
SELECT 'dw.fact_anomaly_event' AS table_name, anomaly_id AS record_id, meter_id
FROM dw.fact_anomaly_event
WHERE meter_id IS NULL;

-- Checks for missing customer identifiers in facts that should be customer-attributable.
SELECT 'dw.fact_energy_consumption' AS table_name, CAST(consumption_id AS VARCHAR(100)) AS record_id, customer_id
FROM dw.fact_energy_consumption
WHERE customer_id IS NULL
UNION ALL
SELECT 'dw.fact_meter_reading' AS table_name, reading_id AS record_id, customer_id
FROM dw.fact_meter_reading
WHERE customer_id IS NULL;

-- Checks for negative consumption. Valid energy consumption facts should never contain negative kWh.
SELECT 'dw.fact_energy_consumption' AS table_name, CAST(consumption_id AS VARCHAR(100)) AS record_id, kwh
FROM dw.fact_energy_consumption
WHERE kwh < 0
UNION ALL
SELECT 'dw.fact_meter_reading' AS table_name, reading_id AS record_id, kwh
FROM dw.fact_meter_reading
WHERE kwh < 0
UNION ALL
SELECT 'dw.fact_anomaly_event' AS table_name, anomaly_id AS record_id, kwh
FROM dw.fact_anomaly_event
WHERE kwh < 0;

-- Checks for duplicate reading IDs. This should return no rows because reading_id is the event fact primary key.
SELECT
    reading_id,
    COUNT(*) AS duplicate_count
FROM dw.fact_meter_reading
GROUP BY reading_id
HAVING COUNT(*) > 1;

-- Checks for duplicate source event IDs. Duplicate events can indicate replay or ingestion idempotency problems.
SELECT
    event_id,
    COUNT(*) AS duplicate_count
FROM dw.fact_meter_reading
GROUP BY event_id
HAVING COUNT(*) > 1;

-- Checks for fact rows that do not resolve to required dimension rows. These should be prevented by foreign keys, but the query is useful after migrations or manual loads.
SELECT
    'fact_energy_consumption_missing_meter' AS issue_name,
    CAST(f.consumption_id AS VARCHAR(100)) AS record_id,
    f.meter_id AS missing_key
FROM dw.fact_energy_consumption AS f
LEFT JOIN dw.dim_meter AS m
    ON m.meter_id = f.meter_id
WHERE m.meter_id IS NULL
UNION ALL
SELECT
    'fact_energy_consumption_missing_customer',
    CAST(f.consumption_id AS VARCHAR(100)),
    f.customer_id
FROM dw.fact_energy_consumption AS f
LEFT JOIN dw.dim_customer AS c
    ON c.customer_id = f.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT
    'fact_energy_consumption_missing_region',
    CAST(f.consumption_id AS VARCHAR(100)),
    CAST(f.region_id AS VARCHAR(100))
FROM dw.fact_energy_consumption AS f
LEFT JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
WHERE r.region_id IS NULL
UNION ALL
SELECT
    'fact_energy_consumption_missing_tariff',
    CAST(f.consumption_id AS VARCHAR(100)),
    f.tariff_id
FROM dw.fact_energy_consumption AS f
LEFT JOIN dw.dim_tariff AS t
    ON t.tariff_id = f.tariff_id
WHERE f.tariff_id IS NOT NULL
  AND t.tariff_id IS NULL
UNION ALL
SELECT
    'fact_energy_consumption_missing_date',
    CAST(f.consumption_id AS VARCHAR(100)),
    CAST(f.date_id AS VARCHAR(100))
FROM dw.fact_energy_consumption AS f
LEFT JOIN dw.dim_date AS d
    ON d.date_id = f.date_id
WHERE d.date_id IS NULL
UNION ALL
SELECT
    'fact_meter_reading_missing_meter',
    f.reading_id,
    f.meter_id
FROM dw.fact_meter_reading AS f
LEFT JOIN dw.dim_meter AS m
    ON m.meter_id = f.meter_id
WHERE m.meter_id IS NULL
UNION ALL
SELECT
    'fact_meter_reading_missing_customer',
    f.reading_id,
    f.customer_id
FROM dw.fact_meter_reading AS f
LEFT JOIN dw.dim_customer AS c
    ON c.customer_id = f.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT
    'fact_meter_reading_missing_region',
    f.reading_id,
    CAST(f.region_id AS VARCHAR(100))
FROM dw.fact_meter_reading AS f
LEFT JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
WHERE r.region_id IS NULL
UNION ALL
SELECT
    'fact_meter_reading_missing_date',
    f.reading_id,
    CAST(f.date_id AS VARCHAR(100))
FROM dw.fact_meter_reading AS f
LEFT JOIN dw.dim_date AS d
    ON d.date_id = f.date_id
WHERE d.date_id IS NULL;

-- Checks active meters linked to inactive customers. This can cause operational and billing ambiguity.
SELECT
    m.meter_id,
    m.customer_id,
    c.is_active AS customer_is_active,
    m.is_active AS meter_is_active
FROM dw.dim_meter AS m
INNER JOIN dw.dim_customer AS c
    ON c.customer_id = m.customer_id
WHERE m.is_active = 1
  AND c.is_active = 0;

-- Checks readings with future timestamps. Future-dated readings usually indicate source clock or timezone issues.
SELECT 'dw.fact_energy_consumption' AS table_name, CAST(consumption_id AS VARCHAR(100)) AS record_id, reading_timestamp AS timestamp_value
FROM dw.fact_energy_consumption
WHERE reading_timestamp > SYSUTCDATETIME()
UNION ALL
SELECT 'dw.fact_meter_reading' AS table_name, reading_id AS record_id, event_timestamp AS timestamp_value
FROM dw.fact_meter_reading
WHERE event_timestamp > SYSUTCDATETIME();

-- Summarizes rejected records by reason so recurring source-data problems are visible.
SELECT
    rejection_reason,
    COUNT(*) AS rejected_count,
    MIN(rejected_at) AS first_rejected_at,
    MAX(rejected_at) AS latest_rejected_at
FROM staging.rejected_records
GROUP BY rejection_reason
ORDER BY rejected_count DESC, rejection_reason;

-- Lists failed pipeline runs. These rows should be reviewed before trusting the associated outputs.
SELECT
    pipeline_run_id,
    pipeline_name,
    pipeline_type,
    status,
    started_at,
    finished_at,
    records_read,
    records_written,
    records_rejected,
    error_message
FROM monitoring.pipeline_run
WHERE status = 'failed'
ORDER BY started_at DESC;

-- Lists failed data-quality checks. These rows identify which rules failed and how many records were affected.
SELECT
    check_id,
    pipeline_run_id,
    check_name,
    table_name,
    status,
    failed_count,
    checked_at
FROM monitoring.data_quality_check
WHERE status = 'failed'
ORDER BY checked_at DESC;
GO
