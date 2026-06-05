"""Consumption and dashboard serving endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.db import execute_query
from api.models import (
    CustomerUsageSummary,
    DailyRegionConsumption,
    DashboardKpis,
    MonthlyRegionConsumption,
)
from api.services import warehouse_queries as queries


router = APIRouter(tags=["consumption"])


@router.get("/consumption/daily", response_model=list[DailyRegionConsumption])
def get_daily_consumption(
    region_name: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.daily_region_consumption_query(
        region_name=region_name,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    return execute_query(sql, params)


@router.get("/consumption/monthly", response_model=list[MonthlyRegionConsumption])
def get_monthly_consumption(
    region_name: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.monthly_region_consumption_query(region_name=region_name, limit=limit)
    return execute_query(sql, params)


@router.get("/consumption/customer/{customer_id}", response_model=CustomerUsageSummary)
def get_customer_consumption(customer_id: str) -> dict:
    sql, params = queries.customer_usage_query(customer_id)
    rows = execute_query(sql, params)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No consumption summary found for customer_id={customer_id}")
    return rows[0]


@router.get("/dashboard/kpis", response_model=DashboardKpis)
def get_dashboard_kpis() -> dict:
    sql, params = queries.dashboard_kpis_query()
    rows = execute_query(sql, params)
    if not rows:
        raise HTTPException(status_code=404, detail="Dashboard KPI view returned no rows")
    return rows[0]
