/*
    Phase 1 - Analytics Query Examples

    These examples are designed to be useful in interviews and project walkthroughs.
*/

USE EnergyWarehouse;
GO

-- Daily consumption by region.
SELECT
    d.full_date,
    r.region_name,
    SUM(f.kwh) AS total_kwh,
    SUM(f.estimated_cost) AS total_estimated_cost,
    COUNT(*) AS reading_count
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_date AS d
    ON d.date_id = f.date_id
INNER JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
GROUP BY
    d.full_date,
    r.region_name
ORDER BY
    d.full_date,
    r.region_name;

-- Monthly consumption by region.
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    r.region_name,
    SUM(f.kwh) AS total_kwh,
    SUM(f.estimated_cost) AS total_estimated_cost
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_date AS d
    ON d.date_id = f.date_id
INNER JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
GROUP BY
    d.year_number,
    d.month_number,
    d.month_name,
    r.region_name
ORDER BY
    d.year_number,
    d.month_number,
    r.region_name;

-- Top 5 meters by total consumption.
SELECT TOP (5)
    f.meter_id,
    c.customer_segment,
    r.region_name,
    SUM(f.kwh) AS total_kwh,
    SUM(f.estimated_cost) AS total_estimated_cost
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_customer AS c
    ON c.customer_id = f.customer_id
INNER JOIN dw.dim_region AS r
    ON r.region_id = f.region_id
GROUP BY
    f.meter_id,
    c.customer_segment,
    r.region_name
ORDER BY
    total_kwh DESC;

-- Latest reading per meter using ROW_NUMBER().
WITH RankedReadings AS
(
    SELECT
        f.reading_id,
        f.meter_id,
        f.event_timestamp,
        f.kwh,
        f.voltage,
        ROW_NUMBER() OVER (
            PARTITION BY f.meter_id
            ORDER BY f.event_timestamp DESC, f.reading_id DESC
        ) AS row_number_latest
    FROM dw.fact_meter_reading AS f
)
SELECT
    reading_id,
    meter_id,
    event_timestamp,
    kwh,
    voltage
FROM RankedReadings
WHERE row_number_latest = 1
ORDER BY meter_id;

-- Running daily consumption by region using SUM() OVER.
WITH DailyRegionConsumption AS
(
    SELECT
        d.full_date,
        r.region_name,
        SUM(f.kwh) AS daily_kwh
    FROM dw.fact_energy_consumption AS f
    INNER JOIN dw.dim_date AS d
        ON d.date_id = f.date_id
    INNER JOIN dw.dim_region AS r
        ON r.region_id = f.region_id
    GROUP BY
        d.full_date,
        r.region_name
)
SELECT
    full_date,
    region_name,
    daily_kwh,
    SUM(daily_kwh) OVER (
        PARTITION BY region_name
        ORDER BY full_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_region_kwh
FROM DailyRegionConsumption
ORDER BY
    region_name,
    full_date;

-- Average kWh by customer segment.
SELECT
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    COUNT(*) AS fact_row_count,
    AVG(f.kwh) AS average_daily_kwh,
    SUM(f.kwh) AS total_kwh
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_customer AS c
    ON c.customer_id = f.customer_id
GROUP BY c.customer_segment
ORDER BY total_kwh DESC;

-- Estimated cost by tariff.
SELECT
    t.tariff_id,
    t.tariff_name,
    t.price_per_kwh,
    t.standing_charge_daily,
    COUNT(*) AS consumption_rows,
    SUM(f.kwh) AS total_kwh,
    SUM(f.estimated_cost) AS total_estimated_cost
FROM dw.fact_energy_consumption AS f
LEFT JOIN dw.dim_tariff AS t
    ON t.tariff_id = f.tariff_id
GROUP BY
    t.tariff_id,
    t.tariff_name,
    t.price_per_kwh,
    t.standing_charge_daily
ORDER BY total_estimated_cost DESC;

-- Anomaly events by region.
SELECT
    r.region_name,
    a.anomaly_type,
    COUNT(*) AS anomaly_count,
    AVG(a.anomaly_score) AS average_anomaly_score,
    SUM(a.kwh - ISNULL(a.expected_kwh, 0)) AS total_kwh_above_expected
FROM dw.fact_anomaly_event AS a
INNER JOIN dw.dim_region AS r
    ON r.region_id = a.region_id
GROUP BY
    r.region_name,
    a.anomaly_type
ORDER BY
    anomaly_count DESC,
    average_anomaly_score DESC;

-- Consumption comparison between weekdays and weekends.
SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*) AS consumption_rows,
    AVG(f.kwh) AS average_kwh,
    SUM(f.kwh) AS total_kwh,
    SUM(f.estimated_cost) AS total_estimated_cost
FROM dw.fact_energy_consumption AS f
INNER JOIN dw.dim_date AS d
    ON d.date_id = f.date_id
GROUP BY CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END
ORDER BY day_type;

-- Data freshness query showing the latest reading timestamp in batch and event facts.
WITH Freshness AS
(
    SELECT
        'batch_consumption' AS source_name,
        MAX(reading_timestamp) AS latest_reading_timestamp
    FROM dw.fact_energy_consumption
    UNION ALL
    SELECT
        'smart_meter_events',
        MAX(event_timestamp)
    FROM dw.fact_meter_reading
)
SELECT
    source_name,
    latest_reading_timestamp,
    DATEDIFF(HOUR, latest_reading_timestamp, SYSUTCDATETIME()) AS hours_since_latest_reading
FROM Freshness
ORDER BY latest_reading_timestamp DESC;

-- Pipeline monitoring summary.
SELECT
    pipeline_name,
    pipeline_type,
    status,
    COUNT(*) AS run_count,
    SUM(records_read) AS total_records_read,
    SUM(records_written) AS total_records_written,
    SUM(records_rejected) AS total_records_rejected,
    AVG(DATEDIFF(SECOND, started_at, ISNULL(finished_at, SYSUTCDATETIME()))) AS average_duration_seconds
FROM monitoring.pipeline_run
GROUP BY
    pipeline_name,
    pipeline_type,
    status
ORDER BY
    pipeline_type,
    pipeline_name,
    status;

-- Data-quality failure summary.
SELECT
    q.table_name,
    q.check_name,
    COUNT(*) AS check_run_count,
    SUM(CASE WHEN q.status = 'failed' THEN 1 ELSE 0 END) AS failed_run_count,
    SUM(q.failed_count) AS total_failed_records,
    MAX(q.checked_at) AS latest_checked_at
FROM monitoring.data_quality_check AS q
GROUP BY
    q.table_name,
    q.check_name
ORDER BY
    total_failed_records DESC,
    q.table_name,
    q.check_name;
GO
