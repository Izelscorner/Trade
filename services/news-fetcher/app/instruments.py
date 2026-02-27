"""Load tracked instruments from the database."""

import logging
from sqlalchemy import text
from .db import async_session

logger = logging.getLogger(__name__)


async def get_instruments() -> list[dict]:
    """Fetch all instruments from DB."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category, yfinance_symbol FROM instruments ORDER BY symbol")
        )
        rows = result.fetchall()
        return [
            {
                "id": str(row.id),
                "symbol": row.symbol,
                "name": row.name,
                "category": row.category,
                "yfinance_symbol": row.yfinance_symbol,
            }
            for row in rows
        ]
