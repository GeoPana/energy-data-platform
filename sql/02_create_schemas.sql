/*
    Phase 1 - Schemas, Staging Tables, And Monitoring Tables

    Schemas:
      staging   - raw-like landing and rejected records
      dw        - dimensional warehouse tables
      monitoring - pipeline and data-quality metadata
*/

USE EnergyWarehouse;
GO

IF SCHEMA_ID(N'staging') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA staging AUTHORIZATION dbo;');
END;
GO

IF SCHEMA_ID(N'dw') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA dw AUTHORIZATION dbo;');
END;
GO

IF SCHEMA_ID(N'monitoring') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA monitoring AUTHORIZATION dbo;');
END;
GO

IF OBJECT_ID(N'staging.raw_energy_consumption', N'U') IS NULL
BEGIN
    CREATE TABLE staging.raw_energy_consumption
    (
        raw_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_raw_energy_consumption PRIMARY KEY,
        source_file VARCHAR(255) NULL,
        batch_id VARCHAR(100) NULL,
        meter_id VARCHAR(50) NULL,
        customer_id VARCHAR(50) NULL,
        region_name VARCHAR(100) NULL,
        reading_timestamp VARCHAR(50) NULL,
        kwh VARCHAR(50) NULL,
        ingestion_timestamp DATETIME2(0) NOT NULL
            CONSTRAINT DF_raw_energy_consumption_ingestion_timestamp DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'staging.raw_meter_reading_events', N'U') IS NULL
BEGIN
    CREATE TABLE staging.raw_meter_reading_events
    (
        raw_event_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_raw_meter_reading_events PRIMARY KEY,
        event_id VARCHAR(100) NULL,
        raw_payload NVARCHAR(MAX) NULL,
        ingestion_timestamp DATETIME2(0) NOT NULL
            CONSTRAINT DF_raw_meter_reading_events_ingestion_timestamp DEFAULT SYSUTCDATETIME(),
        processing_status VARCHAR(30) NOT NULL
            CONSTRAINT DF_raw_meter_reading_events_processing_status DEFAULT 'received',
        CONSTRAINT CK_raw_meter_reading_events_processing_status
            CHECK (processing_status IN ('received', 'validated', 'rejected', 'loaded'))
    );
END;
GO

IF OBJECT_ID(N'staging.rejected_records', N'U') IS NULL
BEGIN
    CREATE TABLE staging.rejected_records
    (
        rejected_id BIGINT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_rejected_records PRIMARY KEY,
        source_system VARCHAR(50) NOT NULL,
        source_reference VARCHAR(255) NOT NULL,
        raw_payload NVARCHAR(MAX) NULL,
        rejection_reason VARCHAR(255) NOT NULL,
        rejected_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_rejected_records_rejected_at DEFAULT SYSUTCDATETIME(),
        batch_id VARCHAR(100) NULL
    );
END;
GO

IF OBJECT_ID(N'monitoring.pipeline_run', N'U') IS NULL
BEGIN
    CREATE TABLE monitoring.pipeline_run
    (
        pipeline_run_id VARCHAR(100) NOT NULL
            CONSTRAINT PK_pipeline_run PRIMARY KEY,
        pipeline_name VARCHAR(100) NOT NULL,
        pipeline_type VARCHAR(50) NOT NULL,
        status VARCHAR(30) NOT NULL,
        started_at DATETIME2(0) NOT NULL,
        finished_at DATETIME2(0) NULL,
        records_read INT NOT NULL
            CONSTRAINT DF_pipeline_run_records_read DEFAULT 0,
        records_written INT NOT NULL
            CONSTRAINT DF_pipeline_run_records_written DEFAULT 0,
        records_rejected INT NOT NULL
            CONSTRAINT DF_pipeline_run_records_rejected DEFAULT 0,
        error_message VARCHAR(MAX) NULL,
        CONSTRAINT CK_pipeline_run_status
            CHECK (status IN ('started', 'running', 'succeeded', 'failed', 'cancelled')),
        CONSTRAINT CK_pipeline_run_counts_non_negative
            CHECK (records_read >= 0 AND records_written >= 0 AND records_rejected >= 0),
        CONSTRAINT CK_pipeline_run_finished_after_started
            CHECK (finished_at IS NULL OR finished_at >= started_at)
    );
END;
GO

IF OBJECT_ID(N'monitoring.data_quality_check', N'U') IS NULL
BEGIN
    CREATE TABLE monitoring.data_quality_check
    (
        check_id VARCHAR(100) NOT NULL
            CONSTRAINT PK_data_quality_check PRIMARY KEY,
        pipeline_run_id VARCHAR(100) NOT NULL,
        check_name VARCHAR(100) NOT NULL,
        table_name VARCHAR(100) NOT NULL,
        status VARCHAR(30) NOT NULL,
        failed_count INT NOT NULL
            CONSTRAINT DF_data_quality_check_failed_count DEFAULT 0,
        checked_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_data_quality_check_checked_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_data_quality_check_pipeline_run
            FOREIGN KEY (pipeline_run_id)
            REFERENCES monitoring.pipeline_run (pipeline_run_id),
        CONSTRAINT CK_data_quality_check_status
            CHECK (status IN ('passed', 'failed', 'warning')),
        CONSTRAINT CK_data_quality_check_failed_count_non_negative
            CHECK (failed_count >= 0)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'staging.raw_energy_consumption')
      AND name = N'IX_raw_energy_consumption_batch_id'
)
BEGIN
    CREATE INDEX IX_raw_energy_consumption_batch_id
        ON staging.raw_energy_consumption (batch_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'staging.raw_meter_reading_events')
      AND name = N'IX_raw_meter_reading_events_processing_status'
)
BEGIN
    CREATE INDEX IX_raw_meter_reading_events_processing_status
        ON staging.raw_meter_reading_events (processing_status);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'monitoring.pipeline_run')
      AND name = N'IX_pipeline_run_status_started_at'
)
BEGIN
    CREATE INDEX IX_pipeline_run_status_started_at
        ON monitoring.pipeline_run (status, started_at);
END;
GO
