"""Parameterized SQL definitions for serving API endpoints."""

from __future__ import annotations

from typing import Any


DEFAULT_LIMIT = 100
MAX_LIMIT = 1000


def normalize_limit(limit: int | None, default: int = DEFAULT_LIMIT) -> int:
    selected = default if limit is None else limit
    return max(1, min(selected, MAX_LIMIT))


def regions_query() -> tuple[str, tuple[Any, ...]]:
    return (
        """
        SELECT DISTINCT region_name
        FROM
        (
            SELECT region_name FROM serving.vw_daily_region_consumption
            UNION
            SELECT region_name FROM serving.vw_monthly_region_consumption
            UNION
            SELECT region_name FROM serving.vw_meter_latest_reading
            UNION
            SELECT region_name FROM serving.vw_anomaly_events
        ) AS regions
        ORDER BY region_name;
        """,
        (),
    )


def daily_region_consumption_query(
    *,
    region_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
) -> tuple[str, tuple[Any, ...]]:
    sql = """
        SELECT TOP (?)
            reading_date,
            region_name,
            total_kwh,
            average_kwh,
            reading_count,
            unique_meter_count
        FROM serving.vw_daily_region_consumption
        WHERE (? IS NULL OR region_name = ?)
          AND (? IS NULL OR reading_date >= TRY_CONVERT(date, ?))
          AND (? IS NULL OR reading_date <= TRY_CONVERT(date, ?))
        ORDER BY reading_date, region_name;
    """
    return (
        sql,
        (
            normalize_limit(limit),
            region_name,
            region_name,
            start_date,
            start_date,
            end_date,
            end_date,
        ),
    )


def monthly_region_consumption_query(
    *,
    region_name: str | None = None,
    limit: int | None = None,
) -> tuple[str, tuple[Any, ...]]:
    sql = """
        SELECT TOP (?)
            year_number,
            month_number,
            region_name,
            total_kwh,
            average_daily_kwh,
            unique_meter_count
        FROM serving.vw_monthly_region_consumption
        WHERE (? IS NULL OR region_name = ?)
        ORDER BY year_number, month_number, region_name;
    """
    return (sql, (normalize_limit(limit), region_name, region_name))


def customer_usage_query(customer_id: str) -> tuple[str, tuple[Any, ...]]:
    return (
        """
        SELECT
            customer_id,
            customer_segment,
            region_name,
            total_kwh,
            average_daily_kwh,
            first_reading_timestamp,
            last_reading_timestamp
        FROM serving.vw_customer_usage_summary
        WHERE customer_id = ?;
        """,
        (customer_id,),
    )


def latest_meter_reading_query(meter_id: str | None = None, limit: int | None = None) -> tuple[str, tuple[Any, ...]]:
    sql = """
        SELECT TOP (?)
            meter_id,
            customer_id,
            region_name,
            event_timestamp,
            kwh,
            voltage
        FROM serving.vw_meter_latest_reading
        WHERE (? IS NULL OR meter_id = ?)
        ORDER BY event_timestamp DESC, meter_id;
    """
    return (sql, (normalize_limit(limit), meter_id, meter_id))


def anomaly_events_query(
    *,
    region_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
) -> tuple[str, tuple[Any, ...]]:
    sql = """
        SELECT TOP (?)
            anomaly_id,
            meter_id,
            region_name,
            anomaly_type,
            anomaly_score,
            kwh,
            expected_kwh,
            detected_at,
            source_system
        FROM serving.vw_anomaly_events
        WHERE (? IS NULL OR region_name = ?)
          AND (? IS NULL OR detected_at >= TRY_CONVERT(datetime2, ?))
          AND (? IS NULL OR detected_at < DATEADD(DAY, 1, TRY_CONVERT(datetime2, ?)))
        ORDER BY detected_at DESC;
    """
    return (
        sql,
        (
            normalize_limit(limit),
            region_name,
            region_name,
            start_date,
            start_date,
            end_date,
            end_date,
        ),
    )


def dashboard_kpis_query() -> tuple[str, tuple[Any, ...]]:
    return (
        """
        SELECT
            total_kwh,
            total_customers,
            total_meters,
            total_anomalies,
            latest_reading_timestamp,
            failed_pipeline_runs,
            failed_data_quality_checks
        FROM serving.vw_dashboard_kpis;
        """,
        (),
    )


def pipeline_runs_query(status: str | None = None, limit: int | None = None) -> tuple[str, tuple[Any, ...]]:
    sql = """
        SELECT TOP (?)
            pipeline_run_id,
            pipeline_name,
            pipeline_type,
            status,
            started_at,
            finished_at,
            duration_seconds,
            records_read,
            records_written,
            records_rejected,
            error_message
        FROM serving.vw_pipeline_run_summary
        WHERE (? IS NULL OR status = ?)
        ORDER BY started_at DESC;
    """
    return (sql, (normalize_limit(limit), status, status))


def data_quality_query(status: str | None = None, limit: int | None = None) -> tuple[str, tuple[Any, ...]]:
    sql = """
        SELECT TOP (?)
            check_id,
            pipeline_run_id,
            check_name,
            table_name,
            status,
            failed_count,
            checked_at
        FROM serving.vw_data_quality_summary
        WHERE (? IS NULL OR status = ?)
        ORDER BY checked_at DESC;
    """
    return (sql, (normalize_limit(limit), status, status))


def freshness_query() -> tuple[str, tuple[Any, ...]]:
    return (
        """
        SELECT
            latest_reading_timestamp,
            DATEDIFF(HOUR, latest_reading_timestamp, SYSUTCDATETIME()) AS hours_since_latest_reading
        FROM serving.vw_dashboard_kpis;
        """,
        (),
    )
