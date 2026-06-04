"""Explicit PySpark schemas for Phase 2 bronze inputs."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


RAW_HISTORICAL_CONSUMPTION_SCHEMA = StructType(
    [
        StructField("source_file", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("meter_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("reading_timestamp", StringType(), True),
        StructField("kwh", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)

RAW_WEATHER_SCHEMA = StructType(
    [
        StructField("weather_timestamp", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("temperature_c", StringType(), True),
        StructField("humidity_percent", StringType(), True),
        StructField("wind_speed_kph", StringType(), True),
        StructField("precipitation_mm", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
    ]
)

CUSTOMER_METADATA_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_segment", StringType(), False),
        StructField("household_size", IntegerType(), True),
        StructField("dwelling_type", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode_area", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)

METER_METADATA_SCHEMA = StructType(
    [
        StructField("meter_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("region_name", StringType(), True),
        StructField("meter_type", StringType(), True),
        StructField("installation_date", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)

TARIFF_METADATA_SCHEMA = StructType(
    [
        StructField("tariff_id", StringType(), False),
        StructField("tariff_name", StringType(), False),
        StructField("valid_from", StringType(), True),
        StructField("valid_to", StringType(), True),
        StructField("price_per_kwh", DoubleType(), True),
        StructField("standing_charge_daily", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
    ]
)
