/*
    Phase 1 - Database Bootstrap
    Creates the local SQL Server database used as the warehouse foundation.
*/

IF DB_ID(N'EnergyWarehouse') IS NULL
BEGIN
    CREATE DATABASE EnergyWarehouse;
END;
GO

ALTER DATABASE EnergyWarehouse SET RECOVERY SIMPLE;
GO

USE EnergyWarehouse;
GO

SELECT
    name AS database_name,
    create_date,
    compatibility_level
FROM sys.databases
WHERE name = N'EnergyWarehouse';
GO
