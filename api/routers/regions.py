"""Region-focused serving endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import execute_query
from api.models import DailyRegionConsumption, MonthlyRegionConsumption
from api.services import warehouse_queries as queries


router = APIRouter(prefix="/regions", tags=["regions"])


@router.get("")
def get_regions() -> list[dict[str, str]]:
    sql, params = queries.regions_query()
    return execute_query(sql, params)


@router.get("/{region_name}/daily-consumption", response_model=list[DailyRegionConsumption])
def get_region_daily_consumption(
    region_name: str,
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


@router.get("/{region_name}/monthly-consumption", response_model=list[MonthlyRegionConsumption])
def get_region_monthly_consumption(
    region_name: str,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.monthly_region_consumption_query(region_name=region_name, limit=limit)
    return execute_query(sql, params)
