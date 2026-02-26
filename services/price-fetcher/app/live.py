"""Fetch live/real-time price data from yfinance."""

import logging
from datetime import datetime, timezone

import yfinance as yf
from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)


def determine_market_status(info: dict) -> str:
    """Determine if the market is active, pre/post-market, or closed."""
    state = info.get("marketState", "").lower()
    if "pre" in state:
        return "pre_market"
    if "post" in state:
        return "after_hours"
    if state in ("regular", "open"):
        return "active"
    return "closed"


async def fetch_and_store_live(instrument: dict) -> bool:
    """Fetch current price for an instrument and store it."""
    yf_symbol = instrument["yfinance_symbol"]
    instrument_id = instrument["id"]

    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
    except Exception:
        logger.exception("[%s] Failed to fetch live data", instrument["symbol"])
        return False

    price = info.get("regularMarketPrice") or info.get("currentPrice")
    if price is None:
        logger.warning("[%s] No price data available", instrument["symbol"])
        return False

    prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")
    change_amount = None
    change_percent = None
    if prev_close and prev_close > 0:
        change_amount = price - prev_close
        change_percent = (change_amount / prev_close) * 100

    market_status = determine_market_status(info)

    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO live_prices (instrument_id, price, change_amount, change_percent, market_status, fetched_at)
                VALUES (:iid, :price, :change_amount, :change_percent, :market_status, :fetched_at)
            """),
            {
                "iid": instrument_id,
                "price": price,
                "change_amount": change_amount,
                "change_percent": change_percent,
                "market_status": market_status,
                "fetched_at": datetime.now(timezone.utc),
            },
        )
        await session.commit()

    logger.info(
        "[%s] Price: %.2f (%s) status=%s",
        instrument["symbol"], price,
        f"{change_percent:+.2f}%" if change_percent else "N/A",
        market_status,
    )
    return True
