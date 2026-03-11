"""Fundamentals & Macro Indicators API endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse

router = APIRouter()


@router.get("/{instrument_id}", response_model=APIResponse)
async def get_fundamentals(instrument_id: str):
    """Get latest fundamental metrics for an instrument."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT pe_ratio, roe, de_ratio, peg_ratio, fetched_at
                FROM fundamental_metrics
                WHERE instrument_id = :iid
                ORDER BY fetched_at DESC
                LIMIT 1
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()

    if not row:
        return APIResponse(data=None, timestamp=datetime.now(timezone.utc))

    return APIResponse(
        data={
            "pe_ratio": float(row.pe_ratio) if row.pe_ratio is not None else None,
            "roe": float(row.roe) if row.roe is not None else None,
            "de_ratio": float(row.de_ratio) if row.de_ratio is not None else None,
            "peg_ratio": float(row.peg_ratio) if row.peg_ratio is not None else None,
            "fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
        },
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/macro/indicators", response_model=APIResponse)
async def get_macro_indicators():
    """Get latest macro economic indicators (DXY, 10Y Treasury, GDP Growth, Brent Crude)."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (indicator_name)
                    indicator_name, value, label, unit, fetched_at
                FROM macro_indicators
                ORDER BY indicator_name, fetched_at DESC
            """)
        )
        rows = result.fetchall()

    indicators = [
        {
            "name": r.indicator_name,
            "value": float(r.value),
            "label": r.label,
            "unit": r.unit,
            "fetched_at": r.fetched_at.isoformat() if r.fetched_at else None,
        }
        for r in rows
    ]

    return APIResponse(data=indicators, timestamp=datetime.now(timezone.utc))
