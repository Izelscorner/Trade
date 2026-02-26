"""Fetch and store historical OHLCV data. Only accumulates new data."""

import logging
from datetime import date, timedelta

import yfinance as yf
from sqlalchemy import text

from .db import async_session

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


async def fetch_and_store_historical(instrument: dict) -> int:
    """Fetch historical data from yfinance. Only fetches data newer than what's stored."""
    yf_symbol = instrument["yfinance_symbol"]
    instrument_id = instrument["id"]

    last_date = await get_last_stored_date(instrument_id)

    if last_date:
        start_date = last_date + timedelta(days=1)
        if start_date >= date.today():
            logger.info("[%s] Historical data already up to date", instrument["symbol"])
            return 0
        start_str = start_date.isoformat()
    else:
        start_str = "2020-01-01"

    logger.info("[%s] Fetching historical data from %s", instrument["symbol"], start_str)

    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(start=start_str, end=date.today().isoformat(), auto_adjust=True)
    except Exception:
        logger.exception("[%s] Failed to fetch historical data", instrument["symbol"])
        return 0

    if df.empty:
        logger.info("[%s] No new historical data available", instrument["symbol"])
        return 0

    df = df.reset_index()
    inserted = 0

    async with async_session() as session:
        for _, row in df.iterrows():
            row_date = row["Date"]
            if hasattr(row_date, "date"):
                row_date = row_date.date()

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
                        "date": row_date,
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row.get("Volume", 0)),
                    },
                )
                if result.fetchone():
                    inserted += 1
            except Exception:
                logger.exception("[%s] Failed to insert row for %s", instrument["symbol"], row_date)
                await session.rollback()
                continue
        await session.commit()

    logger.info("[%s] Inserted %d historical price rows", instrument["symbol"], inserted)
    return inserted
