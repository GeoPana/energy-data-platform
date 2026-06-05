"""FastAPI app exposing curated SQL Server serving views."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api.db import WarehouseQueryError
from api.routers import anomalies, consumption, health, meters, monitoring, regions


app = FastAPI(
    title="Azure-Style Energy Lakehouse API",
    description="Local API serving curated SQL Server warehouse views from the Azure-Style Energy Lakehouse project.",
    version="0.5.0",
)


@app.exception_handler(WarehouseQueryError)
def warehouse_query_error_handler(_: Request, exc: WarehouseQueryError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "detail": str(exc),
            "hint": "Confirm SQL Server is running, EnergyWarehouse exists, and serving views have been created.",
        },
    )


app.include_router(health.router)
app.include_router(regions.router)
app.include_router(meters.router)
app.include_router(anomalies.router)
app.include_router(consumption.router)
app.include_router(monitoring.router)
