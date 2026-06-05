"""Pydantic response models for the serving API."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    environment: str
    database: str


class VersionResponse(BaseModel):
    name: str
    version: str
    phase: str


class DailyRegionConsumption(BaseModel):
    reading_date: date
    region_name: str
    total_kwh: Decimal
    average_kwh: Decimal
    reading_count: int
    unique_meter_count: int


class MonthlyRegionConsumption(BaseModel):
    year_number: int
    month_number: int
    region_name: str
    total_kwh: Decimal
    average_daily_kwh: Decimal
    unique_meter_count: int


class CustomerUsageSummary(BaseModel):
    customer_id: str
    customer_segment: str
    region_name: str
    total_kwh: Decimal
    average_daily_kwh: Decimal
    first_reading_timestamp: datetime | None = None
    last_reading_timestamp: datetime | None = None


class MeterLatestReading(BaseModel):
    meter_id: str
    customer_id: str
    region_name: str
    event_timestamp: datetime
    kwh: Decimal
    voltage: Decimal | None = None


class AnomalyEvent(BaseModel):
    anomaly_id: str
    meter_id: str
    region_name: str
    anomaly_type: str
    anomaly_score: Decimal | None = None
    kwh: Decimal
    expected_kwh: Decimal | None = None
    detected_at: datetime
    source_system: str | None = None


class DashboardKpis(BaseModel):
    total_kwh: Decimal
    total_customers: int
    total_meters: int
    total_anomalies: int
    latest_reading_timestamp: datetime | None = None
    failed_pipeline_runs: int
    failed_data_quality_checks: int


class PipelineRunSummary(BaseModel):
    pipeline_run_id: str
    pipeline_name: str
    pipeline_type: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: int
    records_read: int
    records_written: int
    records_rejected: int
    error_message: str | None = None


class DataQualitySummary(BaseModel):
    check_id: str
    pipeline_run_id: str
    check_name: str
    table_name: str
    status: str
    failed_count: int
    checked_at: datetime


class FreshnessResponse(BaseModel):
    latest_reading_timestamp: datetime | None = None
    hours_since_latest_reading: int | None = None
