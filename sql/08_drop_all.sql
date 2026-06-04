/*
    Phase 1 - Reset Script

    WARNING:
    This script deletes all Phase 1 database objects inside EnergyWarehouse.
    It drops tables in dependency order, then drops the Phase 1 schemas if empty.

    It does not drop the EnergyWarehouse database itself.
*/

USE EnergyWarehouse;
GO

DROP TABLE IF EXISTS dw.fact_anomaly_event;
DROP TABLE IF EXISTS dw.fact_meter_reading;
DROP TABLE IF EXISTS dw.fact_energy_consumption;
GO

DROP TABLE IF EXISTS monitoring.data_quality_check;
DROP TABLE IF EXISTS monitoring.pipeline_run;
GO

DROP TABLE IF EXISTS staging.rejected_records;
DROP TABLE IF EXISTS staging.raw_meter_reading_events;
DROP TABLE IF EXISTS staging.raw_energy_consumption;
GO

DROP TABLE IF EXISTS dw.dim_meter;
DROP TABLE IF EXISTS dw.dim_customer;
DROP TABLE IF EXISTS dw.dim_tariff;
DROP TABLE IF EXISTS dw.dim_region;
DROP TABLE IF EXISTS dw.dim_date;
GO

IF SCHEMA_ID(N'staging') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'staging'))
BEGIN
    EXEC(N'DROP SCHEMA staging;');
END;
GO

IF SCHEMA_ID(N'monitoring') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'monitoring'))
BEGIN
    EXEC(N'DROP SCHEMA monitoring;');
END;
GO

IF SCHEMA_ID(N'dw') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.objects WHERE schema_id = SCHEMA_ID(N'dw'))
BEGIN
    EXEC(N'DROP SCHEMA dw;');
END;
GO

SELECT 'Phase 1 objects dropped. EnergyWarehouse database retained.' AS reset_status;
GO
