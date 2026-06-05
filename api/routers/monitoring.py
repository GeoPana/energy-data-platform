"""Pipeline and data-quality monitoring endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import execute_query
from api.models import DataQualitySummary, FreshnessResponse, PipelineRunSummary
from api.services import warehouse_queries as queries


router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/pipeline-runs", response_model=list[PipelineRunSummary])
def get_pipeline_runs(
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.pipeline_runs_query(status=status, limit=limit)
    return execute_query(sql, params)


@router.get("/data-quality", response_model=list[DataQualitySummary])
def get_data_quality_results(
    status: str | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[dict]:
    sql, params = queries.data_quality_query(status=status, limit=limit)
    return execute_query(sql, params)


@router.get("/freshness", response_model=FreshnessResponse)
def get_data_freshness() -> dict:
    sql, params = queries.freshness_query()
    rows = execute_query(sql, params)
    return rows[0] if rows else {"latest_reading_timestamp": None, "hours_since_latest_reading": None}
