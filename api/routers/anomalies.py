"""Anomaly serving endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import execute_query
from api.models import AnomalyEvent
from api.services import warehouse_queries as queries


router = APIRouter(prefix="/anomalies", tags=["anomalies"])


@router.get("", response_model=list[AnomalyEvent])
def get_anomalies(
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.anomaly_events_query(start_date=start_date, end_date=end_date, limit=limit)
    return execute_query(sql, params)


@router.get("/region/{region_name}", response_model=list[AnomalyEvent])
def get_anomalies_by_region(
    region_name: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.anomaly_events_query(
        region_name=region_name,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    return execute_query(sql, params)
