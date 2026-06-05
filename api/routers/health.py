"""Health and version endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from api.config import get_settings
from api.models import HealthResponse, VersionResponse


router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    settings = get_settings()
    return HealthResponse(
        status="ok",
        environment=settings.environment,
        database=settings.sql_server.database,
    )


@router.get("/version", response_model=VersionResponse)
def version() -> VersionResponse:
    return VersionResponse(
        name="Azure-Style Energy Lakehouse API",
        version="0.5.0",
        phase="Phase 5 - Serving Layer",
    )
