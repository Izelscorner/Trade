"""Fetch and store historical OHLCV data. Only accumulates new data."""

import logging
from datetime import date, timedelta

from sqlalchemy import text

from .db import async_session
from .yahoo import fetch_chart, parse_historical

logger = logging.getLogger(__name__)


async def get_last_stored_date(instrument_id: str) -> date | None:
    """Get the most recent date we have historical data for."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT MAX(date) FROM historical_prices WHERE instrument_id = :iid"),
            {"iid": instrument_id},
        )
        row = result.fetchone()
        return row[0] if row and row[0] else None


def compute_period(last_date: date | None) -> str:
    """Compute yfinance-compatible period string based on last stored date."""
    if last_date is None:
        return "5y"  # Fetch 5 years on first run

    days_diff = (date.today() - last_date).days
    if days_diff <= 5:
        return "5d"
    elif days_diff <= 30:
        return "1mo"
    elif days_diff <= 90:
        return "3mo"
    elif days_diff <= 180:
        return "6mo"
    elif days_diff <= 365:
        return "1y"
    elif days_diff <= 730:
        return "2y"
    else:
        return "5y"


async def fetch_and_store_historical(instrument: dict) -> int:
    """Fetch historical data from Yahoo Finance. Only stores new data."""
    yf_symbol = instrument["yfinance_symbol"]
    instrument_id = instrument["id"]

    last_date = await get_last_stored_date(instrument_id)

    if last_date:
        if last_date >= date.today() - timedelta(days=1):
            logger.info("[%s] Historical data already up to date", instrument["symbol"])
            return 0

    period = compute_period(last_date)
    logger.info("[%s] Fetching historical data (period=%s, last=%s)",
                instrument["symbol"], period, last_date)

    chart_data = fetch_chart(yf_symbol, period=period, interval="1d")
    if chart_data is None:
        logger.warning("[%s] No chart data returned", instrument["symbol"])
        return 0

    rows = parse_historical(chart_data)
    if not rows:
        logger.warning("[%s] No historical rows parsed", instrument["symbol"])
        return 0

    # Filter to only new dates
    if last_date:
        rows = [r for r in rows if r["date"] > last_date]

    if not rows:
        logger.info("[%s] No new historical rows to insert", instrument["symbol"])
        return 0

    inserted = 0
    async with async_session() as session:
        for row in rows:
            try:
                result = await session.execute(
                    text("""
                        INSERT INTO historical_prices (instrument_id, date, open, high, low, close, volume)
                        VALUES (:iid, :date, :open, :high, :low, :close, :volume)
                        ON CONFLICT (instrument_id, date) DO NOTHING
                        RETURNING id
                    """),
                    {
                        "iid": instrument_id,
                        "date": row["date"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                    },
                )
                if result.fetchone():
                    inserted += 1
            except Exception:
                logger.exception("[%s] Failed to insert for %s", instrument["symbol"], row["date"])
                await session.rollback()
                continue
        await session.commit()

    logger.info("[%s] Inserted %d historical price rows", instrument["symbol"], inserted)
    return inserted
