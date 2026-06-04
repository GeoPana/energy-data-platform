/*
    Phase 1 - Fact Tables
    Creates historical consumption, smart-meter reading, and anomaly facts.
*/

USE EnergyWarehouse;
GO

IF OBJECT_ID(N'dw.fact_energy_consumption', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fact_energy_consumption
    (
        consumption_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_fact_energy_consumption PRIMARY KEY,
        meter_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        region_id INT NOT NULL,
        tariff_id VARCHAR(50) NULL,
        date_id INT NOT NULL,
        reading_timestamp DATETIME2(0) NOT NULL,
        kwh DECIMAL(12,4) NOT NULL,
        estimated_cost DECIMAL(12,4) NOT NULL,
        source_system VARCHAR(50) NOT NULL,
        batch_id VARCHAR(100) NOT NULL,
        load_timestamp DATETIME2(0) NOT NULL
            CONSTRAINT DF_fact_energy_consumption_load_timestamp DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_fact_energy_consumption_dim_meter
            FOREIGN KEY (meter_id)
            REFERENCES dw.dim_meter (meter_id),
        CONSTRAINT FK_fact_energy_consumption_dim_customer
            FOREIGN KEY (customer_id)
            REFERENCES dw.dim_customer (customer_id),
        CONSTRAINT FK_fact_energy_consumption_dim_region
            FOREIGN KEY (region_id)
            REFERENCES dw.dim_region (region_id),
        CONSTRAINT FK_fact_energy_consumption_dim_tariff
            FOREIGN KEY (tariff_id)
            REFERENCES dw.dim_tariff (tariff_id),
        CONSTRAINT FK_fact_energy_consumption_dim_date
            FOREIGN KEY (date_id)
            REFERENCES dw.dim_date (date_id),
        CONSTRAINT CK_fact_energy_consumption_kwh_non_negative
            CHECK (kwh >= 0),
        CONSTRAINT CK_fact_energy_consumption_estimated_cost_non_negative
            CHECK (estimated_cost >= 0)
    );
END;
GO

IF OBJECT_ID(N'dw.fact_meter_reading', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fact_meter_reading
    (
        reading_id VARCHAR(100) NOT NULL
            CONSTRAINT PK_fact_meter_reading PRIMARY KEY,
        meter_id VARCHAR(50) NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        region_id INT NOT NULL,
        date_id INT NOT NULL,
        event_timestamp DATETIME2(0) NOT NULL,
        kwh DECIMAL(12,4) NOT NULL,
        voltage DECIMAL(10,3) NOT NULL,
        source_system VARCHAR(50) NOT NULL,
        event_id VARCHAR(100) NOT NULL,
        load_timestamp DATETIME2(0) NOT NULL
            CONSTRAINT DF_fact_meter_reading_load_timestamp DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_fact_meter_reading_event_id
            UNIQUE (event_id),
        CONSTRAINT FK_fact_meter_reading_dim_meter
            FOREIGN KEY (meter_id)
            REFERENCES dw.dim_meter (meter_id),
        CONSTRAINT FK_fact_meter_reading_dim_customer
            FOREIGN KEY (customer_id)
            REFERENCES dw.dim_customer (customer_id),
        CONSTRAINT FK_fact_meter_reading_dim_region
            FOREIGN KEY (region_id)
            REFERENCES dw.dim_region (region_id),
        CONSTRAINT FK_fact_meter_reading_dim_date
            FOREIGN KEY (date_id)
            REFERENCES dw.dim_date (date_id),
        CONSTRAINT CK_fact_meter_reading_kwh_non_negative
            CHECK (kwh >= 0),
        CONSTRAINT CK_fact_meter_reading_voltage_positive
            CHECK (voltage > 0)
    );
END;
GO

IF OBJECT_ID(N'dw.fact_anomaly_event', N'U') IS NULL
BEGIN
    CREATE TABLE dw.fact_anomaly_event
    (
        anomaly_id VARCHAR(100) NOT NULL
            CONSTRAINT PK_fact_anomaly_event PRIMARY KEY,
        reading_id VARCHAR(100) NULL,
        meter_id VARCHAR(50) NOT NULL,
        region_id INT NOT NULL,
        date_id INT NOT NULL,
        anomaly_type VARCHAR(100) NOT NULL,
        anomaly_score DECIMAL(12,4) NULL,
        kwh DECIMAL(12,4) NOT NULL,
        expected_kwh DECIMAL(12,4) NULL,
        detected_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_fact_anomaly_event_detected_at DEFAULT SYSUTCDATETIME(),
        source_system VARCHAR(50) NOT NULL,
        CONSTRAINT FK_fact_anomaly_event_fact_meter_reading
            FOREIGN KEY (reading_id)
            REFERENCES dw.fact_meter_reading (reading_id),
        CONSTRAINT FK_fact_anomaly_event_dim_meter
            FOREIGN KEY (meter_id)
            REFERENCES dw.dim_meter (meter_id),
        CONSTRAINT FK_fact_anomaly_event_dim_region
            FOREIGN KEY (region_id)
            REFERENCES dw.dim_region (region_id),
        CONSTRAINT FK_fact_anomaly_event_dim_date
            FOREIGN KEY (date_id)
            REFERENCES dw.dim_date (date_id),
        CONSTRAINT CK_fact_anomaly_event_kwh_non_negative
            CHECK (kwh >= 0),
        CONSTRAINT CK_fact_anomaly_event_expected_kwh_non_negative
            CHECK (expected_kwh IS NULL OR expected_kwh >= 0),
        CONSTRAINT CK_fact_anomaly_event_anomaly_score_non_negative
            CHECK (anomaly_score IS NULL OR anomaly_score >= 0)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_energy_consumption')
      AND name = N'IX_fact_energy_consumption_date_id'
)
BEGIN
    CREATE INDEX IX_fact_energy_consumption_date_id
        ON dw.fact_energy_consumption (date_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_energy_consumption')
      AND name = N'IX_fact_energy_consumption_meter_id'
)
BEGIN
    CREATE INDEX IX_fact_energy_consumption_meter_id
        ON dw.fact_energy_consumption (meter_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_energy_consumption')
      AND name = N'IX_fact_energy_consumption_region_id'
)
BEGIN
    CREATE INDEX IX_fact_energy_consumption_region_id
        ON dw.fact_energy_consumption (region_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_energy_consumption')
      AND name = N'IX_fact_energy_consumption_customer_id'
)
BEGIN
    CREATE INDEX IX_fact_energy_consumption_customer_id
        ON dw.fact_energy_consumption (customer_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_energy_consumption')
      AND name = N'IX_fact_energy_consumption_reading_timestamp'
)
BEGIN
    CREATE INDEX IX_fact_energy_consumption_reading_timestamp
        ON dw.fact_energy_consumption (reading_timestamp);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_meter_reading')
      AND name = N'IX_fact_meter_reading_date_id'
)
BEGIN
    CREATE INDEX IX_fact_meter_reading_date_id
        ON dw.fact_meter_reading (date_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_meter_reading')
      AND name = N'IX_fact_meter_reading_meter_id'
)
BEGIN
    CREATE INDEX IX_fact_meter_reading_meter_id
        ON dw.fact_meter_reading (meter_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_meter_reading')
      AND name = N'IX_fact_meter_reading_region_id'
)
BEGIN
    CREATE INDEX IX_fact_meter_reading_region_id
        ON dw.fact_meter_reading (region_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_meter_reading')
      AND name = N'IX_fact_meter_reading_customer_id'
)
BEGIN
    CREATE INDEX IX_fact_meter_reading_customer_id
        ON dw.fact_meter_reading (customer_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_meter_reading')
      AND name = N'IX_fact_meter_reading_event_timestamp'
)
BEGIN
    CREATE INDEX IX_fact_meter_reading_event_timestamp
        ON dw.fact_meter_reading (event_timestamp);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_anomaly_event')
      AND name = N'IX_fact_anomaly_event_date_id'
)
BEGIN
    CREATE INDEX IX_fact_anomaly_event_date_id
        ON dw.fact_anomaly_event (date_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_anomaly_event')
      AND name = N'IX_fact_anomaly_event_meter_id'
)
BEGIN
    CREATE INDEX IX_fact_anomaly_event_meter_id
        ON dw.fact_anomaly_event (meter_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.fact_anomaly_event')
      AND name = N'IX_fact_anomaly_event_region_id'
)
BEGIN
    CREATE INDEX IX_fact_anomaly_event_region_id
        ON dw.fact_anomaly_event (region_id);
END;
GO
