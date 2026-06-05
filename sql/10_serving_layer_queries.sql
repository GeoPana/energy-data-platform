/*
    Phase 5 - Serving Layer Query Examples

    These examples intentionally query serving views, not raw staging or dw
    tables. They represent the SQL contracts used by APIs, dashboards, and
    analysts.
*/

USE EnergyWarehouse;
GO

-- API query: daily consumption for one region and optional date range.
DECLARE @region_name VARCHAR(100) = 'East London';
DECLARE @start_date DATE = '2026-01-01';
DECLARE @end_date DATE = '2026-01-31';

SELECT
    reading_date,
    region_name,
    total_kwh,
    average_kwh,
    reading_count,
    unique_meter_count
FROM serving.vw_daily_region_consumption
WHERE region_name = @region_name
  AND reading_date BETWEEN @start_date AND @end_date
ORDER BY reading_date;

-- API query: latest reading for one meter.
DECLARE @meter_id VARCHAR(50) = 'MTR-LON-001';

SELECT
    meter_id,
    customer_id,
    region_name,
    event_timestamp,
    kwh,
    voltage
FROM serving.vw_meter_latest_reading
WHERE meter_id = @meter_id;

-- API query: anomalies by region.
SELECT TOP (100)
    anomaly_id,
    meter_id,
    region_name,
    anomaly_type,
    anomaly_score,
    kwh,
    expected_kwh,
    detected_at
FROM serving.vw_anomaly_events
WHERE region_name = @region_name
ORDER BY detected_at DESC;

-- API query: dashboard KPI summary.
SELECT
    total_kwh,
    total_customers,
    total_meters,
    total_anomalies,
    latest_reading_timestamp,
    failed_pipeline_runs,
    failed_data_quality_checks
FROM serving.vw_dashboard_kpis;

-- Power BI query: daily regional trend.
SELECT
    reading_date,
    region_name,
    total_kwh,
    average_kwh,
    reading_count,
    unique_meter_count
FROM serving.vw_daily_region_consumption
ORDER BY reading_date, region_name;

-- Power BI query: monthly regional trend.
SELECT
    year_number,
    month_number,
    region_name,
    total_kwh,
    average_daily_kwh,
    unique_meter_count
FROM serving.vw_monthly_region_consumption
ORDER BY year_number, month_number, region_name;

-- Power BI query: anomaly detail table.
SELECT
    anomaly_id,
    meter_id,
    region_name,
    anomaly_type,
    anomaly_score,
    kwh,
    expected_kwh,
    detected_at,
    source_system
FROM serving.vw_anomaly_events
ORDER BY detected_at DESC;

-- Monitoring query: failed pipeline runs.
SELECT
    pipeline_run_id,
    pipeline_name,
    pipeline_type,
    status,
    started_at,
    finished_at,
    duration_seconds,
    records_read,
    records_written,
    records_rejected,
    error_message
FROM serving.vw_pipeline_run_summary
WHERE status = 'failed'
ORDER BY started_at DESC;

-- Monitoring query: failed data-quality checks.
SELECT
    check_id,
    pipeline_run_id,
    check_name,
    table_name,
    status,
    failed_count,
    checked_at
FROM serving.vw_data_quality_summary
WHERE status = 'failed'
ORDER BY checked_at DESC;

-- Data freshness query: latest warehouse reading timestamp exposed to consumers.
SELECT
    latest_reading_timestamp,
    DATEDIFF(HOUR, latest_reading_timestamp, SYSUTCDATETIME()) AS hours_since_latest_reading
FROM serving.vw_dashboard_kpis;
GO
