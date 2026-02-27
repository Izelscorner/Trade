"""Instruments API endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, InstrumentSchema

router = APIRouter()


@router.get("", response_model=APIResponse)
async def list_instruments(category: str | None = None):
    """Get all instruments, optionally filtered by category."""
    async with async_session() as session:
        if category:
            result = await session.execute(
                text("SELECT id, symbol, name, category FROM instruments WHERE category = :cat ORDER BY symbol"),
                {"cat": category},
            )
        else:
            result = await session.execute(
                text("SELECT id, symbol, name, category FROM instruments ORDER BY symbol")
            )
        rows = result.fetchall()

    instruments = [
        InstrumentSchema(
            id=str(r.id),
            symbol=r.symbol,
            name=r.name,
            category=r.category,
        )
        for r in rows
    ]
    return APIResponse(data=[i.model_dump() for i in instruments], timestamp=datetime.now(timezone.utc))


@router.get("/{instrument_id}", response_model=APIResponse)
async def get_instrument(instrument_id: str):
    """Get a single instrument by ID."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category FROM instruments WHERE id = :iid"),
            {"iid": instrument_id},
        )
        row = result.fetchone()

    if not row:
        return APIResponse(error="Instrument not found", timestamp=datetime.now(timezone.utc))

    inst = InstrumentSchema(
        id=str(row.id),
        symbol=row.symbol,
        name=row.name,
        category=row.category,
    )
    return APIResponse(data=inst.model_dump(), timestamp=datetime.now(timezone.utc))
