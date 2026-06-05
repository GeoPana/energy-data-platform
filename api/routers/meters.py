"""Smart-meter serving endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.db import execute_query
from api.models import MeterLatestReading
from api.services import warehouse_queries as queries


router = APIRouter(prefix="/meters", tags=["meters"])


@router.get("/{meter_id}/latest-reading", response_model=MeterLatestReading)
def get_meter_latest_reading(meter_id: str) -> dict:
    sql, params = queries.latest_meter_reading_query(meter_id=meter_id, limit=1)
    rows = execute_query(sql, params)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No latest reading found for meter_id={meter_id}")
    return rows[0]


@router.get("/latest-readings", response_model=list[MeterLatestReading])
def get_latest_meter_readings(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict]:
    sql, params = queries.latest_meter_reading_query(limit=limit)
    return execute_query(sql, params)
