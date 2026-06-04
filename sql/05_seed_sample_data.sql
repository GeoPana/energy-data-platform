/*
    Phase 1 - Sample Data

    Seeds a small, coherent London-style dataset for local development,
    data-quality checks, and analytics query examples.
*/

USE EnergyWarehouse;
GO

SET NOCOUNT ON;
SET LANGUAGE us_english;
GO

DECLARE @sample_batch_id VARCHAR(100) = 'sample_phase1_batch_001';

INSERT INTO dw.dim_region (region_name, country, postcode_area)
SELECT seed.region_name, seed.country, seed.postcode_area
FROM
(
    VALUES
        ('North London', 'United Kingdom', 'N'),
        ('South London', 'United Kingdom', 'SE/SW'),
        ('East London', 'United Kingdom', 'E'),
        ('West London', 'United Kingdom', 'W'),
        ('Central London', 'United Kingdom', 'EC/WC')
) AS seed (region_name, country, postcode_area)
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.dim_region AS r
    WHERE r.region_name = seed.region_name
);

INSERT INTO dw.dim_tariff
(
    tariff_id,
    tariff_name,
    valid_from,
    valid_to,
    price_per_kwh,
    standing_charge_daily,
    is_active
)
SELECT
    seed.tariff_id,
    seed.tariff_name,
    seed.valid_from,
    seed.valid_to,
    seed.price_per_kwh,
    seed.standing_charge_daily,
    seed.is_active
FROM
(
    VALUES
        ('T-STD-2026', 'Standard Variable 2026', CAST('2026-01-01' AS DATE), NULL, CAST(0.2860 AS DECIMAL(10,4)), CAST(0.5300 AS DECIMAL(10,4)), CAST(1 AS BIT)),
        ('T-ECO-2026', 'Eco Saver 2026', CAST('2026-01-01' AS DATE), NULL, CAST(0.2510 AS DECIMAL(10,4)), CAST(0.4900 AS DECIMAL(10,4)), CAST(1 AS BIT)),
        ('T-BIZ-2026', 'Small Business Flex 2026', CAST('2026-01-01' AS DATE), NULL, CAST(0.3150 AS DECIMAL(10,4)), CAST(0.7200 AS DECIMAL(10,4)), CAST(1 AS BIT))
) AS seed
(
    tariff_id,
    tariff_name,
    valid_from,
    valid_to,
    price_per_kwh,
    standing_charge_daily,
    is_active
)
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.dim_tariff AS t
    WHERE t.tariff_id = seed.tariff_id
);

;WITH DateSeed AS
(
    SELECT CAST('2026-01-01' AS DATE) AS full_date
    UNION ALL
    SELECT DATEADD(DAY, 1, full_date)
    FROM DateSeed
    WHERE full_date < CAST('2026-01-14' AS DATE)
)
INSERT INTO dw.dim_date
(
    date_id,
    full_date,
    year_number,
    quarter_number,
    month_number,
    month_name,
    day_of_month,
    day_of_week_name,
    is_weekend
)
SELECT
    CONVERT(INT, CONVERT(CHAR(8), ds.full_date, 112)) AS date_id,
    ds.full_date,
    DATEPART(YEAR, ds.full_date) AS year_number,
    DATEPART(QUARTER, ds.full_date) AS quarter_number,
    DATEPART(MONTH, ds.full_date) AS month_number,
    DATENAME(MONTH, ds.full_date) AS month_name,
    DATEPART(DAY, ds.full_date) AS day_of_month,
    DATENAME(WEEKDAY, ds.full_date) AS day_of_week_name,
    CASE WHEN DATENAME(WEEKDAY, ds.full_date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend
FROM DateSeed AS ds
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.dim_date AS d
    WHERE d.date_id = CONVERT(INT, CONVERT(CHAR(8), ds.full_date, 112))
)
OPTION (MAXRECURSION 100);

INSERT INTO dw.dim_customer
(
    customer_id,
    customer_segment,
    household_size,
    dwelling_type,
    region_id,
    signup_date,
    is_active
)
SELECT
    seed.customer_id,
    seed.customer_segment,
    seed.household_size,
    seed.dwelling_type,
    r.region_id,
    seed.signup_date,
    seed.is_active
FROM
(
    VALUES
        ('CUST-LON-001', 'Residential', 2, 'Flat', 'North London', CAST('2024-03-12' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-002', 'Residential', 4, 'Terraced', 'South London', CAST('2023-07-04' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-003', 'Small Business', 1, 'Retail Unit', 'East London', CAST('2022-11-18' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-004', 'Residential', 1, 'Flat', 'West London', CAST('2025-01-20' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-005', 'Public Sector', 3, 'Community Centre', 'Central London', CAST('2021-06-09' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-006', 'Residential', 5, 'Semi-detached', 'North London', CAST('2020-09-23' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-007', 'Residential', 3, 'Terraced', 'South London', CAST('2024-05-28' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-008', 'Small Business', 1, 'Office', 'East London', CAST('2023-02-14' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-009', 'Residential', 2, 'Flat', 'West London', CAST('2025-08-01' AS DATE), CAST(1 AS BIT)),
        ('CUST-LON-010', 'Residential', 4, 'Detached', 'Central London', CAST('2022-04-30' AS DATE), CAST(1 AS BIT))
) AS seed
(
    customer_id,
    customer_segment,
    household_size,
    dwelling_type,
    region_name,
    signup_date,
    is_active
)
INNER JOIN dw.dim_region AS r
    ON r.region_name = seed.region_name
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.dim_customer AS c
    WHERE c.customer_id = seed.customer_id
);

INSERT INTO dw.dim_meter
(
    meter_id,
    customer_id,
    region_id,
    meter_type,
    installation_date,
    is_active
)
SELECT
    seed.meter_id,
    seed.customer_id,
    c.region_id,
    seed.meter_type,
    seed.installation_date,
    seed.is_active
FROM
(
    VALUES
        ('MTR-LON-001', 'CUST-LON-001', 'SMETS2 Smart Meter', CAST('2024-04-02' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-002', 'CUST-LON-002', 'SMETS2 Smart Meter', CAST('2023-08-10' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-003', 'CUST-LON-003', 'Half-Hourly Business Meter', CAST('2022-12-01' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-004', 'CUST-LON-004', 'SMETS1 Smart Meter', CAST('2025-02-15' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-005', 'CUST-LON-005', 'Commercial Smart Meter', CAST('2021-07-19' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-006', 'CUST-LON-006', 'SMETS2 Smart Meter', CAST('2020-10-05' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-007', 'CUST-LON-007', 'SMETS2 Smart Meter', CAST('2024-06-11' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-008', 'CUST-LON-008', 'Half-Hourly Business Meter', CAST('2023-03-05' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-009', 'CUST-LON-009', 'SMETS2 Smart Meter', CAST('2025-08-22' AS DATE), CAST(1 AS BIT)),
        ('MTR-LON-010', 'CUST-LON-010', 'SMETS2 Smart Meter', CAST('2022-05-16' AS DATE), CAST(1 AS BIT))
) AS seed (meter_id, customer_id, meter_type, installation_date, is_active)
INNER JOIN dw.dim_customer AS c
    ON c.customer_id = seed.customer_id
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.dim_meter AS m
    WHERE m.meter_id = seed.meter_id
);

;WITH MeterDays AS
(
    SELECT
        m.meter_id,
        c.customer_id,
        c.customer_segment,
        c.household_size,
        m.region_id,
        d.date_id,
        d.full_date,
        d.day_of_month,
        d.is_weekend,
        ROW_NUMBER() OVER (ORDER BY m.meter_id) AS meter_sequence
    FROM dw.dim_meter AS m
    INNER JOIN dw.dim_customer AS c
        ON c.customer_id = m.customer_id
    INNER JOIN dw.dim_date AS d
        ON d.full_date BETWEEN CAST('2026-01-01' AS DATE) AND CAST('2026-01-14' AS DATE)
),
ConsumptionRows AS
(
    SELECT
        md.meter_id,
        md.customer_id,
        md.region_id,
        CASE
            WHEN md.customer_segment = 'Small Business' THEN 'T-BIZ-2026'
            WHEN md.household_size <= 2 THEN 'T-ECO-2026'
            ELSE 'T-STD-2026'
        END AS tariff_id,
        md.date_id,
        DATEADD(HOUR, 23, CONVERT(DATETIME2(0), md.full_date)) AS reading_timestamp,
        CAST(ROUND(
            CASE
                WHEN md.customer_segment = 'Small Business'
                    THEN 22.0000 + (md.meter_sequence * 0.4500) + (md.day_of_month * 0.3100)
                WHEN md.customer_segment = 'Public Sector'
                    THEN 34.0000 + (md.day_of_month * 0.4200) + CASE WHEN md.is_weekend = 1 THEN -4.0000 ELSE 0.0000 END
                ELSE
                    5.5000 + (md.household_size * 1.3500) + ((md.meter_sequence + md.day_of_month) % 5) * 0.4200
                    + CASE WHEN md.is_weekend = 1 THEN 1.8000 ELSE 0.0000 END
            END,
            4
        ) AS DECIMAL(12,4)) AS kwh
    FROM MeterDays AS md
)
INSERT INTO dw.fact_energy_consumption
(
    meter_id,
    customer_id,
    region_id,
    tariff_id,
    date_id,
    reading_timestamp,
    kwh,
    estimated_cost,
    source_system,
    batch_id
)
SELECT
    cr.meter_id,
    cr.customer_id,
    cr.region_id,
    cr.tariff_id,
    cr.date_id,
    cr.reading_timestamp,
    cr.kwh,
    CAST(ROUND((cr.kwh * t.price_per_kwh) + t.standing_charge_daily, 4) AS DECIMAL(12,4)) AS estimated_cost,
    'sample_batch',
    @sample_batch_id
FROM ConsumptionRows AS cr
INNER JOIN dw.dim_tariff AS t
    ON t.tariff_id = cr.tariff_id
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.fact_energy_consumption AS f
    WHERE f.batch_id = @sample_batch_id
      AND f.meter_id = cr.meter_id
      AND f.date_id = cr.date_id
);

;WITH EventSlots AS
(
    SELECT slot_number
    FROM
    (
        VALUES (1), (2), (3), (4), (5), (6)
    ) AS slots (slot_number)
),
MeterSequence AS
(
    SELECT
        m.meter_id,
        m.customer_id,
        m.region_id,
        c.customer_segment,
        c.household_size,
        ROW_NUMBER() OVER (ORDER BY m.meter_id) AS meter_sequence
    FROM dw.dim_meter AS m
    INNER JOIN dw.dim_customer AS c
        ON c.customer_id = m.customer_id
),
ReadingRows AS
(
    SELECT
        CONCAT(
            'READ-20260114-',
            RIGHT('00' + CAST(ms.meter_sequence AS VARCHAR(2)), 2),
            '-',
            RIGHT('00' + CAST(es.slot_number AS VARCHAR(2)), 2)
        ) AS reading_id,
        ms.meter_id,
        ms.customer_id,
        ms.region_id,
        20260114 AS date_id,
        DATEADD(HOUR, (es.slot_number - 1) * 4, CAST('2026-01-14T00:00:00' AS DATETIME2(0))) AS event_timestamp,
        CAST(ROUND(
            CASE
                WHEN ms.customer_segment = 'Small Business'
                    THEN 3.2500 + (ms.meter_sequence * 0.1100) + (es.slot_number * 0.2400)
                WHEN ms.customer_segment = 'Public Sector'
                    THEN 5.4000 + (es.slot_number * 0.2800)
                ELSE
                    0.7200 + (ms.household_size * 0.1800) + (es.slot_number * 0.1200)
            END
            + CASE
                WHEN ms.meter_id = 'MTR-LON-003' AND es.slot_number = 6 THEN 5.5000
                WHEN ms.meter_id = 'MTR-LON-006' AND es.slot_number = 5 THEN 2.7000
                ELSE 0.0000
              END,
            4
        ) AS DECIMAL(12,4)) AS kwh,
        CAST(ROUND(229.500 + (ms.meter_sequence * 0.350) + (es.slot_number * 0.180), 3) AS DECIMAL(10,3)) AS voltage,
        CONCAT(
            'EVT-20260114-',
            RIGHT('00' + CAST(ms.meter_sequence AS VARCHAR(2)), 2),
            '-',
            RIGHT('00' + CAST(es.slot_number AS VARCHAR(2)), 2)
        ) AS event_id
    FROM MeterSequence AS ms
    CROSS JOIN EventSlots AS es
)
INSERT INTO dw.fact_meter_reading
(
    reading_id,
    meter_id,
    customer_id,
    region_id,
    date_id,
    event_timestamp,
    kwh,
    voltage,
    source_system,
    event_id
)
SELECT
    rr.reading_id,
    rr.meter_id,
    rr.customer_id,
    rr.region_id,
    rr.date_id,
    rr.event_timestamp,
    rr.kwh,
    rr.voltage,
    'sample_streaming',
    rr.event_id
FROM ReadingRows AS rr
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.fact_meter_reading AS f
    WHERE f.reading_id = rr.reading_id
);

INSERT INTO dw.fact_anomaly_event
(
    anomaly_id,
    reading_id,
    meter_id,
    region_id,
    date_id,
    anomaly_type,
    anomaly_score,
    kwh,
    expected_kwh,
    detected_at,
    source_system
)
SELECT
    seed.anomaly_id,
    r.reading_id,
    r.meter_id,
    r.region_id,
    r.date_id,
    seed.anomaly_type,
    seed.anomaly_score,
    r.kwh,
    seed.expected_kwh,
    DATEADD(MINUTE, 5, r.event_timestamp),
    'sample_quality_rules'
FROM
(
    VALUES
        ('ANOM-20260114-001', 'READ-20260114-03-06', 'High Consumption Spike', CAST(0.9200 AS DECIMAL(12,4)), CAST(4.6000 AS DECIMAL(12,4))),
        ('ANOM-20260114-002', 'READ-20260114-06-05', 'Residential Usage Spike', CAST(0.8400 AS DECIMAL(12,4)), CAST(1.9000 AS DECIMAL(12,4))),
        ('ANOM-20260114-003', 'READ-20260114-05-04', 'Unusual Public Sector Load', CAST(0.7800 AS DECIMAL(12,4)), CAST(4.9000 AS DECIMAL(12,4)))
) AS seed (anomaly_id, reading_id, anomaly_type, anomaly_score, expected_kwh)
INNER JOIN dw.fact_meter_reading AS r
    ON r.reading_id = seed.reading_id
WHERE NOT EXISTS
(
    SELECT 1
    FROM dw.fact_anomaly_event AS a
    WHERE a.anomaly_id = seed.anomaly_id
);

IF NOT EXISTS
(
    SELECT 1
    FROM staging.raw_energy_consumption
    WHERE batch_id = @sample_batch_id
)
BEGIN
    INSERT INTO staging.raw_energy_consumption
    (
        source_file,
        batch_id,
        meter_id,
        customer_id,
        region_name,
        reading_timestamp,
        kwh
    )
    VALUES
        ('energy_consumption_20260114.csv', @sample_batch_id, 'MTR-LON-001', 'CUST-LON-001', 'North London', '2026-01-14T23:00:00', '10.2400'),
        ('energy_consumption_20260114.csv', @sample_batch_id, 'MTR-LON-002', 'CUST-LON-002', 'South London', '2026-01-14T23:00:00', '13.5800'),
        ('energy_consumption_20260114.csv', @sample_batch_id, NULL, 'CUST-LON-004', 'West London', '2026-01-14T23:00:00', '7.4400'),
        ('energy_consumption_20260114.csv', @sample_batch_id, 'MTR-LON-008', 'CUST-LON-008', 'East London', 'not-a-date', '29.7100'),
        ('energy_consumption_20260114.csv', @sample_batch_id, 'MTR-LON-009', 'CUST-LON-009', 'West London', '2026-01-14T23:00:00', '-3.1200');
END;

INSERT INTO staging.raw_meter_reading_events
(
    event_id,
    raw_payload,
    processing_status
)
SELECT
    seed.event_id,
    seed.raw_payload,
    seed.processing_status
FROM
(
    VALUES
        ('EVT-RAW-001', N'{"eventId":"EVT-RAW-001","meterId":"MTR-LON-001","timestamp":"2026-01-14T00:00:00","kwh":1.31,"voltage":230.2}', 'loaded'),
        ('EVT-RAW-002', N'{"eventId":"EVT-RAW-002","meterId":"MTR-LON-003","timestamp":"2026-01-14T20:00:00","kwh":9.91,"voltage":231.6}', 'validated'),
        ('EVT-RAW-003', N'{"eventId":"EVT-RAW-003","meterId":null,"timestamp":"2026-01-14T04:00:00","kwh":0.82,"voltage":229.9}', 'rejected'),
        ('EVT-RAW-004', N'{"eventId":"EVT-RAW-004","meterId":"MTR-LON-006","timestamp":"2026-01-14T16:00:00","kwh":4.88,"voltage":232.5}', 'loaded')
) AS seed (event_id, raw_payload, processing_status)
WHERE NOT EXISTS
(
    SELECT 1
    FROM staging.raw_meter_reading_events AS r
    WHERE r.event_id = seed.event_id
);

INSERT INTO staging.rejected_records
(
    source_system,
    source_reference,
    raw_payload,
    rejection_reason,
    batch_id
)
SELECT
    seed.source_system,
    seed.source_reference,
    seed.raw_payload,
    seed.rejection_reason,
    seed.batch_id
FROM
(
    VALUES
        ('sample_batch', 'energy_consumption_20260114.csv:row:3', N'{"meter_id":null,"customer_id":"CUST-LON-004","kwh":"7.4400"}', 'Missing meter_id', @sample_batch_id),
        ('sample_batch', 'energy_consumption_20260114.csv:row:4', N'{"meter_id":"MTR-LON-008","reading_timestamp":"not-a-date","kwh":"29.7100"}', 'Invalid reading timestamp', @sample_batch_id),
        ('sample_batch', 'energy_consumption_20260114.csv:row:5', N'{"meter_id":"MTR-LON-009","kwh":"-3.1200"}', 'Negative kwh', @sample_batch_id),
        ('sample_streaming', 'EVT-RAW-003', N'{"eventId":"EVT-RAW-003","meterId":null,"kwh":0.82}', 'Missing meter_id', NULL)
) AS seed (source_system, source_reference, raw_payload, rejection_reason, batch_id)
WHERE NOT EXISTS
(
    SELECT 1
    FROM staging.rejected_records AS r
    WHERE r.source_system = seed.source_system
      AND r.source_reference = seed.source_reference
);

INSERT INTO monitoring.pipeline_run
(
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
)
SELECT
    seed.pipeline_run_id,
    seed.pipeline_name,
    seed.pipeline_type,
    seed.status,
    seed.started_at,
    seed.finished_at,
    seed.records_read,
    seed.records_written,
    seed.records_rejected,
    seed.error_message
FROM
(
    VALUES
        ('PIPE-20260114-BATCH-001', 'energy_consumption_batch_load', 'batch', 'succeeded', CAST('2026-01-14T23:05:00' AS DATETIME2(0)), CAST('2026-01-14T23:09:00' AS DATETIME2(0)), 143, 140, 3, CAST(NULL AS VARCHAR(MAX))),
        ('PIPE-20260114-STREAM-001', 'smart_meter_event_ingestion', 'streaming', 'succeeded', CAST('2026-01-14T00:00:00' AS DATETIME2(0)), CAST('2026-01-14T23:59:00' AS DATETIME2(0)), 62, 60, 2, CAST(NULL AS VARCHAR(MAX))),
        ('PIPE-20260114-DQ-001', 'warehouse_data_quality_checks', 'quality', 'failed', CAST('2026-01-14T23:10:00' AS DATETIME2(0)), CAST('2026-01-14T23:11:00' AS DATETIME2(0)), 12, 11, 1, CAST('Rejected records require review before publishing daily report.' AS VARCHAR(MAX)))
) AS seed
(
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
)
WHERE NOT EXISTS
(
    SELECT 1
    FROM monitoring.pipeline_run AS p
    WHERE p.pipeline_run_id = seed.pipeline_run_id
);

INSERT INTO monitoring.data_quality_check
(
    check_id,
    pipeline_run_id,
    check_name,
    table_name,
    status,
    failed_count
)
SELECT
    seed.check_id,
    seed.pipeline_run_id,
    seed.check_name,
    seed.table_name,
    seed.status,
    seed.failed_count
FROM
(
    VALUES
        ('DQ-20260114-001', 'PIPE-20260114-DQ-001', 'fact_energy_consumption_negative_kwh', 'dw.fact_energy_consumption', 'passed', 0),
        ('DQ-20260114-002', 'PIPE-20260114-DQ-001', 'fact_meter_reading_duplicate_event_id', 'dw.fact_meter_reading', 'passed', 0),
        ('DQ-20260114-003', 'PIPE-20260114-DQ-001', 'staging_rejected_records_present', 'staging.rejected_records', 'failed', 4),
        ('DQ-20260114-004', 'PIPE-20260114-DQ-001', 'fact_missing_dimension_reference', 'dw.fact_energy_consumption', 'passed', 0),
        ('DQ-20260114-005', 'PIPE-20260114-DQ-001', 'future_reading_timestamp', 'dw.fact_meter_reading', 'passed', 0)
) AS seed
(
    check_id,
    pipeline_run_id,
    check_name,
    table_name,
    status,
    failed_count
)
WHERE NOT EXISTS
(
    SELECT 1
    FROM monitoring.data_quality_check AS q
    WHERE q.check_id = seed.check_id
);

SELECT 'dw.dim_region' AS table_name, COUNT(*) AS row_count FROM dw.dim_region
UNION ALL
SELECT 'dw.dim_customer', COUNT(*) FROM dw.dim_customer
UNION ALL
SELECT 'dw.dim_meter', COUNT(*) FROM dw.dim_meter
UNION ALL
SELECT 'dw.dim_tariff', COUNT(*) FROM dw.dim_tariff
UNION ALL
SELECT 'dw.dim_date', COUNT(*) FROM dw.dim_date
UNION ALL
SELECT 'dw.fact_energy_consumption', COUNT(*) FROM dw.fact_energy_consumption
UNION ALL
SELECT 'dw.fact_meter_reading', COUNT(*) FROM dw.fact_meter_reading
UNION ALL
SELECT 'dw.fact_anomaly_event', COUNT(*) FROM dw.fact_anomaly_event
UNION ALL
SELECT 'monitoring.pipeline_run', COUNT(*) FROM monitoring.pipeline_run
UNION ALL
SELECT 'monitoring.data_quality_check', COUNT(*) FROM monitoring.data_quality_check
UNION ALL
SELECT 'staging.rejected_records', COUNT(*) FROM staging.rejected_records;
GO
