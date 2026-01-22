from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

import db
from config import LOG_LEVEL
from logging_config import configure_logging


class MeasurementIn(BaseModel):
    """Incoming measurement payload."""

    ts: datetime = Field(..., description="Timestamp in ISO-8601 format")
    source: str = Field(..., description="Source label")
    metric: str = Field(..., description="Metric name")
    value: Optional[float] = Field(None, description="Metric value")
    perimetre: str = Field(..., description="Perimeter identifier")
    nature: str = Field(..., description="Nature identifier")


class InsertRequest(BaseModel):
    """Batch insert request payload."""

    rows: List[MeasurementIn] = Field(..., description="Measurement rows to insert")


class MeasurementOut(BaseModel):
    """Measurement response payload."""

    ts: str
    source: str
    metric: str
    value: Optional[float]
    ukey: str
    version: int
    inserted_at: str


class InsertResponse(BaseModel):
    """Insert response payload."""

    inserted: int


class CountResponse(BaseModel):
    """Count response payload."""

    count: int


class HealthResponse(BaseModel):
    """Health response payload."""

    status: str
    db_ok: bool


configure_logging(LOG_LEVEL)
app = FastAPI(title="PowerSense Data API", version="1.0.0")


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    """Service health check with ClickHouse connectivity."""

    return HealthResponse(status="ok", db_ok=db.ping())


@app.post("/measurements/ingest", response_model=InsertResponse)
def ingest_measurements(payload: InsertRequest) -> InsertResponse:
    """Insert a batch of measurements into ClickHouse."""

    rows = [
        (
            item.ts,
            item.source,
            item.metric,
            item.value,
            item.perimetre,
            item.nature,
        )
        for item in payload.rows
    ]
    try:
        inserted = db.insert_measurements(rows)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"insert failed: {exc}") from exc
    return InsertResponse(inserted=inserted)


@app.get("/measurements", response_model=List[MeasurementOut])
def list_measurements(
    start_ts: Optional[datetime] = Query(None, description="Start timestamp (inclusive)"),
    end_ts: Optional[datetime] = Query(None, description="End timestamp (inclusive)"),
    source: Optional[str] = Query(None, description="Source filter"),
    metric: Optional[str] = Query(None, description="Metric filter"),
    ukey: Optional[str] = Query(None, description="Ukey filter"),
    limit: int = Query(100, ge=1, le=5000, description="Row limit"),
    order: str = Query("desc", description="Sort order"),
) -> List[MeasurementOut]:
    """Fetch measurements with optional filters."""

    if order.lower() not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
    rows = db.fetch_measurements(
        start_ts=start_ts,
        end_ts=end_ts,
        source=source,
        metric=metric,
        ukey=ukey,
        limit=limit,
        order=order,
    )
    return [MeasurementOut(**row) for row in rows]


@app.get("/measurements/latest", response_model=List[MeasurementOut])
def list_latest_measurements(
    ukey: Optional[str] = Query(None, description="Ukey filter"),
    limit: int = Query(100, ge=1, le=5000, description="Row limit"),
    order: str = Query("desc", description="Sort order"),
) -> List[MeasurementOut]:
    """Fetch latest measurements per ukey."""

    if order.lower() not in {"asc", "desc"}:
        raise HTTPException(status_code=400, detail="order must be 'asc' or 'desc'")
    rows = db.fetch_latest_measurements(
        ukey=ukey,
        limit=limit,
        order=order,
    )
    return [MeasurementOut(**row) for row in rows]


@app.get("/measurements/count", response_model=CountResponse)
def count_measurements() -> CountResponse:
    """Return total measurement row count."""

    return CountResponse(count=db.count_measurements())
