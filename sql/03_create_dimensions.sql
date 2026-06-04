/*
    Phase 1 - Dimension Tables
    Creates the dimensional entities used by the warehouse facts.
*/

USE EnergyWarehouse;
GO

IF OBJECT_ID(N'dw.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_date
    (
        date_id INT NOT NULL
            CONSTRAINT PK_dim_date PRIMARY KEY,
        full_date DATE NOT NULL
            CONSTRAINT UQ_dim_date_full_date UNIQUE,
        year_number INT NOT NULL,
        quarter_number INT NOT NULL,
        month_number INT NOT NULL,
        month_name VARCHAR(20) NOT NULL,
        day_of_month INT NOT NULL,
        day_of_week_name VARCHAR(20) NOT NULL,
        is_weekend BIT NOT NULL,
        CONSTRAINT CK_dim_date_date_id_range
            CHECK (date_id BETWEEN 19000101 AND 20991231),
        CONSTRAINT CK_dim_date_quarter_number
            CHECK (quarter_number BETWEEN 1 AND 4),
        CONSTRAINT CK_dim_date_month_number
            CHECK (month_number BETWEEN 1 AND 12),
        CONSTRAINT CK_dim_date_day_of_month
            CHECK (day_of_month BETWEEN 1 AND 31)
    );
END;
GO

IF OBJECT_ID(N'dw.dim_region', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_region
    (
        region_id INT IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_dim_region PRIMARY KEY,
        region_name VARCHAR(100) NOT NULL,
        country VARCHAR(100) NOT NULL,
        postcode_area VARCHAR(20) NOT NULL,
        created_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_dim_region_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_dim_region_region_name
            UNIQUE (region_name),
        CONSTRAINT CK_dim_region_region_name_not_blank
            CHECK (LEN(LTRIM(RTRIM(region_name))) > 0),
        CONSTRAINT CK_dim_region_country_not_blank
            CHECK (LEN(LTRIM(RTRIM(country))) > 0)
    );
END;
GO

IF OBJECT_ID(N'dw.dim_customer', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_customer
    (
        customer_id VARCHAR(50) NOT NULL
            CONSTRAINT PK_dim_customer PRIMARY KEY,
        customer_segment VARCHAR(50) NOT NULL,
        household_size INT NOT NULL,
        dwelling_type VARCHAR(50) NOT NULL,
        region_id INT NOT NULL,
        signup_date DATE NOT NULL,
        is_active BIT NOT NULL
            CONSTRAINT DF_dim_customer_is_active DEFAULT 1,
        created_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_dim_customer_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_dim_customer_dim_region
            FOREIGN KEY (region_id)
            REFERENCES dw.dim_region (region_id),
        CONSTRAINT CK_dim_customer_household_size_positive
            CHECK (household_size > 0),
        CONSTRAINT CK_dim_customer_segment_not_blank
            CHECK (LEN(LTRIM(RTRIM(customer_segment))) > 0),
        CONSTRAINT CK_dim_customer_dwelling_type_not_blank
            CHECK (LEN(LTRIM(RTRIM(dwelling_type))) > 0)
    );
END;
GO

IF OBJECT_ID(N'dw.dim_meter', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_meter
    (
        meter_id VARCHAR(50) NOT NULL
            CONSTRAINT PK_dim_meter PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        region_id INT NOT NULL,
        meter_type VARCHAR(50) NOT NULL,
        installation_date DATE NOT NULL,
        is_active BIT NOT NULL
            CONSTRAINT DF_dim_meter_is_active DEFAULT 1,
        created_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_dim_meter_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_dim_meter_dim_customer
            FOREIGN KEY (customer_id)
            REFERENCES dw.dim_customer (customer_id),
        CONSTRAINT FK_dim_meter_dim_region
            FOREIGN KEY (region_id)
            REFERENCES dw.dim_region (region_id),
        CONSTRAINT CK_dim_meter_meter_type_not_blank
            CHECK (LEN(LTRIM(RTRIM(meter_type))) > 0)
    );
END;
GO

IF OBJECT_ID(N'dw.dim_tariff', N'U') IS NULL
BEGIN
    CREATE TABLE dw.dim_tariff
    (
        tariff_id VARCHAR(50) NOT NULL
            CONSTRAINT PK_dim_tariff PRIMARY KEY,
        tariff_name VARCHAR(100) NOT NULL,
        valid_from DATE NOT NULL,
        valid_to DATE NULL,
        price_per_kwh DECIMAL(10,4) NOT NULL,
        standing_charge_daily DECIMAL(10,4) NOT NULL,
        is_active BIT NOT NULL
            CONSTRAINT DF_dim_tariff_is_active DEFAULT 1,
        created_at DATETIME2(0) NOT NULL
            CONSTRAINT DF_dim_tariff_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT CK_dim_tariff_price_per_kwh_non_negative
            CHECK (price_per_kwh >= 0),
        CONSTRAINT CK_dim_tariff_standing_charge_daily_non_negative
            CHECK (standing_charge_daily >= 0),
        CONSTRAINT CK_dim_tariff_valid_date_range
            CHECK (valid_to IS NULL OR valid_to >= valid_from),
        CONSTRAINT CK_dim_tariff_name_not_blank
            CHECK (LEN(LTRIM(RTRIM(tariff_name))) > 0)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.dim_customer')
      AND name = N'IX_dim_customer_region_id'
)
BEGIN
    CREATE INDEX IX_dim_customer_region_id
        ON dw.dim_customer (region_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.dim_meter')
      AND name = N'IX_dim_meter_customer_id'
)
BEGIN
    CREATE INDEX IX_dim_meter_customer_id
        ON dw.dim_meter (customer_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'dw.dim_meter')
      AND name = N'IX_dim_meter_region_id'
)
BEGIN
    CREATE INDEX IX_dim_meter_region_id
        ON dw.dim_meter (region_id);
END;
GO
