"""Technical Analysis API endpoints."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, TechnicalIndicatorSchema

router = APIRouter()


@router.get("/{instrument_id}", response_model=APIResponse)
async def get_technical_indicators(instrument_id: str):
    """Get latest technical indicators for an instrument."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (indicator_name)
                    indicator_name, value, signal, date, calculated_at
                FROM technical_indicators
                WHERE instrument_id = :iid
                ORDER BY indicator_name, date DESC
            """),
            {"iid": instrument_id},
        )
        rows = result.fetchall()

    indicators = [
        TechnicalIndicatorSchema(
            indicator_name=r.indicator_name,
            value=json.loads(r.value) if isinstance(r.value, str) else r.value,
            signal=r.signal,
            date=str(r.date),
            calculated_at=r.calculated_at,
        )
        for r in rows
    ]
    return APIResponse(data=[i.model_dump() for i in indicators], timestamp=datetime.now(timezone.utc))
