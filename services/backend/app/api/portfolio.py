"""Portfolio API endpoints — DB-backed user watchlist."""

from datetime import datetime, timezone

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse

router = APIRouter()


class AddToPortfolioRequest(BaseModel):
    instrument_id: str


@router.get("", response_model=APIResponse)
async def get_portfolio():
    """Get all portfolio instrument IDs."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT instrument_id::text FROM portfolio ORDER BY added_at DESC")
        )
        ids = [row.instrument_id for row in result.fetchall()]
    return APIResponse(data=ids, timestamp=datetime.now(timezone.utc))


@router.post("", response_model=APIResponse)
async def add_to_portfolio(body: AddToPortfolioRequest):
    """Add an instrument to the portfolio."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO portfolio (instrument_id)
                VALUES (:iid)
                ON CONFLICT (instrument_id) DO NOTHING
            """),
            {"iid": body.instrument_id},
        )
        await session.commit()
    return APIResponse(data={"ok": True}, timestamp=datetime.now(timezone.utc))


@router.delete("/{instrument_id}", response_model=APIResponse)
async def remove_from_portfolio(instrument_id: str):
    """Remove an instrument from the portfolio."""
    async with async_session() as session:
        await session.execute(
            text("DELETE FROM portfolio WHERE instrument_id = :iid"),
            {"iid": instrument_id},
        )
        await session.commit()
    return APIResponse(data={"ok": True}, timestamp=datetime.now(timezone.utc))
