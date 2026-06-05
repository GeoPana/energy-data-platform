/*
    Phase 5 - Serving Schema And Consumer-Facing Views

    The serving schema exposes stable, dashboard/API-friendly views over the
    dimensional warehouse and monitoring tables. Consumers should use these
    views instead of querying staging or warehouse internals directly.
*/

USE EnergyWarehouse;
GO

IF SCHEMA_ID(N'serving') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA serving AUTHORIZATION dbo;');
END;
GO

/*
    serving.vw_daily_region_consumption
    Purpose: daily consumption analytics by London region.
    Consumers: FastAPI region endpoints, Power BI trend charts, SQL analysts.
    Business question: how much energy did each region consume per day?
*/
CREATE OR ALTER VIEW serving.vw_daily_region_consumption
AS
SELECT
    d.full_date AS reading_date,
    r.region_name,
    CAST(SUM(f.kwh) AS DECIMAL(18,4)) AS total_kwh,
    CAST(AVG(f.kwh) AS DECIMAL(18,4)) AS average_kwh,
    COUNT_BIG(*) AS reading_count,
    COUNT_BIG(DISTINCT f.meter_id) AS unique_meter_count
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_date AS d
    ON d.date_id = f.date_id
INNER JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
GROUP BY
    d.full_date,
    r.region_name;
GO

/*
    serving.vw_monthly_region_consumption
    Purpose: monthly region-level consumption summary.
    Consumers: Power BI monthly trend visuals, FastAPI monthly endpoints.
    Business question: how does consumption trend by region across months?
*/
CREATE OR ALTER VIEW serving.vw_monthly_region_consumption
AS
WITH DailyRegion AS
(
    SELECT
        d.year_number,
        d.month_number,
        d.full_date,
        r.region_name,
        SUM(f.kwh) AS daily_kwh,
        COUNT(DISTINCT f.meter_id) AS daily_unique_meter_count
    FROM dw.fact_energy_consumption AS f
    INNER JOIN dw.dim_date AS d
        ON d.date_id = f.date_id
    INNER JOIN dw.dim_region AS r
        ON r.region_id = f.region_id
    GROUP BY
        d.year_number,
        d.month_number,
        d.full_date,
        r.region_name
)
SELECT
    year_number,
    month_number,
    region_name,
    CAST(SUM(daily_kwh) AS DECIMAL(18,4)) AS total_kwh,
    CAST(AVG(daily_kwh) AS DECIMAL(18,4)) AS average_daily_kwh,
    MAX(daily_unique_meter_count) AS unique_meter_count
FROM DailyRegion
GROUP BY
    year_number,
    month_number,
    region_name;
GO

/*
    serving.vw_customer_usage_summary
    Purpose: customer-level consumption profile.
    Consumers: FastAPI customer endpoint, Power BI customer drill-through pages.
    Business question: which customers consume the most energy and over what period?
*/
CREATE OR ALTER VIEW serving.vw_customer_usage_summary
AS
WITH DailyCustomer AS
(
    SELECT
        f.customer_id,
        d.full_date AS reading_date,
        SUM(f.kwh) AS daily_kwh
    FROM dw.fact_energy_consumption AS f
    INNER JOIN dw.dim_date AS d
        ON d.date_id = f.date_id
    GROUP BY
        f.customer_id,
        d.full_date
),
CustomerAverages AS
(
    SELECT
        customer_id,
        AVG(daily_kwh) AS average_daily_kwh
    FROM DailyCustomer
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.customer_segment,
    r.region_name,
    CAST(SUM(f.kwh) AS DECIMAL(18,4)) AS total_kwh,
    CAST(MAX(ca.average_daily_kwh) AS DECIMAL(18,4)) AS average_daily_kwh,
    MIN(f.reading_timestamp) AS first_reading_timestamp,
    MAX(f.reading_timestamp) AS last_reading_timestamp
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_customer AS c
    ON c.customer_id = f.customer_id
INNER JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
INNER JOIN CustomerAverages AS ca
    ON ca.customer_id = f.customer_id
GROUP BY
    c.customer_id,
    c.customer_segment,
    r.region_name;
GO

/*
    serving.vw_meter_latest_reading
    Purpose: latest smart-meter reading per meter.
    Consumers: FastAPI meter endpoints and Power BI smart-meter monitoring page.
    Business question: what is the latest known reading for every meter?
*/
CREATE OR ALTER VIEW serving.vw_meter_latest_reading
AS
WITH RankedReadings AS
(
    SELECT
        f.meter_id,
        f.customer_id,
        r.region_name,
        f.event_timestamp,
        f.kwh,
        f.voltage,
        ROW_NUMBER() OVER (
            PARTITION BY f.meter_id
            ORDER BY f.event_timestamp DESC, f.reading_id DESC
        ) AS row_number_latest
    FROM dw.fact_meter_reading AS f
    INNER JOIN dw.dim_region AS r
        ON r.region_id = f.region_id
)
SELECT
    meter_id,
    customer_id,
    region_name,
    event_timestamp,
    kwh,
    voltage
FROM RankedReadings
WHERE row_number_latest = 1;
GO

/*
    serving.vw_anomaly_events
    Purpose: curated anomaly event detail for monitoring and investigation.
    Consumers: FastAPI anomaly endpoints, Power BI anomaly table.
    Business question: which meters produced anomalous readings and where?
*/
CREATE OR ALTER VIEW serving.vw_anomaly_events
AS
SELECT
    a.anomaly_id,
    a.meter_id,
    r.region_name,
    a.anomaly_type,
    a.anomaly_score,
    a.kwh,
    a.expected_kwh,
    a.detected_at,
    a.source_system
FROM dw.fact_anomaly_event AS a
INNER JOIN dw.dim_region AS r
    ON r.region_id = a.region_id;
GO

/*
    serving.vw_data_quality_summary
    Purpose: data-quality status for warehouse operations.
    Consumers: FastAPI monitoring endpoints and Power BI operational page.
    Business question: which checks failed, where, and when?
*/
CREATE OR ALTER VIEW serving.vw_data_quality_summary
AS
SELECT
    check_id,
    pipeline_run_id,
    check_name,
    table_name,
    status,
    failed_count,
    checked_at
FROM monitoring.data_quality_check;
GO

/*
    serving.vw_pipeline_run_summary
    Purpose: pipeline execution summary for operational monitoring.
    Consumers: FastAPI monitoring endpoints and Power BI pipeline status table.
    Business question: which pipelines ran, how long did they take, and did they fail?
*/
CREATE OR ALTER VIEW serving.vw_pipeline_run_summary
AS
SELECT
    pipeline_run_id,
    pipeline_name,
    pipeline_type,
    status,
    started_at,
    finished_at,
    DATEDIFF(SECOND, started_at, ISNULL(finished_at, SYSUTCDATETIME())) AS duration_seconds,
    records_read,
    records_written,
    records_rejected,
    error_message
FROM monitoring.pipeline_run;
GO

/*
    serving.vw_dashboard_kpis
    Purpose: single-row executive KPI surface.
    Consumers: FastAPI dashboard endpoint and Power BI executive overview page.
    Business question: what is the current high-level health and scale of the platform?
*/
CREATE OR ALTER VIEW serving.vw_dashboard_kpis
AS
SELECT
    CAST((SELECT COALESCE(SUM(kwh), 0) FROM dw.fact_energy_consumption) AS DECIMAL(18,4)) AS total_kwh,
    (SELECT COUNT_BIG(*) FROM dw.dim_customer) AS total_customers,
    (SELECT COUNT_BIG(*) FROM dw.dim_meter) AS total_meters,
    (SELECT COUNT_BIG(*) FROM dw.fact_anomaly_event) AS total_anomalies,
    (
        SELECT MAX(latest_timestamp)
        FROM
        (
            SELECT MAX(reading_timestamp) AS latest_timestamp
            FROM dw.fact_energy_consumption
            UNION ALL
            SELECT MAX(event_timestamp)
            FROM dw.fact_meter_reading
        ) AS latest_readings
    ) AS latest_reading_timestamp,
    (
        SELECT COUNT_BIG(*)
        FROM monitoring.pipeline_run
        WHERE status = 'failed'
    ) AS failed_pipeline_runs,
    (
        SELECT COUNT_BIG(*)
        FROM monitoring.data_quality_check
        WHERE status = 'failed'
    ) AS failed_data_quality_checks;
GO

SELECT
    s.name AS schema_name,
    v.name AS view_name,
    v.create_date,
    v.modify_date
FROM sys.views AS v
INNER JOIN sys.schemas AS s
    ON s.schema_id = v.schema_id
WHERE s.name = N'serving'
ORDER BY v.name;
GO
