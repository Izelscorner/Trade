"""Fetch live/real-time price data from Yahoo Finance API."""

import logging
from datetime import datetime, timezone

from sqlalchemy import text

from .db import async_session
from .yahoo import fetch_chart, parse_live_price

logger = logging.getLogger(__name__)


async def fetch_and_store_live(instrument: dict) -> bool:
    """Fetch current price for an instrument and store it.

    Includes a dedup guard: skips insert if the last stored price is
    identical and was fetched less than 30 seconds ago (avoids duplicates
    from overlapping fetch cycles).
    """
    yf_symbol = instrument["yfinance_symbol"]
    instrument_id = instrument["id"]

    chart_data = fetch_chart(yf_symbol, period="1d", interval="1m")
    if chart_data is None:
        # Fallback to daily
        chart_data = fetch_chart(yf_symbol, period="5d", interval="1d")

    if chart_data is None:
        logger.warning("[%s] Could not fetch price data", instrument["symbol"])
        return False

    live = parse_live_price(chart_data)
    if live is None or live["price"] is None:
        logger.warning("[%s] No live price available", instrument["symbol"])
        return False

    now = datetime.now(timezone.utc)

    async with async_session() as session:
        # Dedup guard: skip if identical price was stored in last 30 seconds
        last = await session.execute(
            text("""
                SELECT price, fetched_at FROM live_prices
                WHERE instrument_id = :iid
                ORDER BY fetched_at DESC LIMIT 1
            """),
            {"iid": instrument_id},
        )
        last_row = last.fetchone()

        if last_row:
            last_price = float(last_row.price)
            seconds_ago = (now - last_row.fetched_at).total_seconds()
            if last_price == live["price"] and seconds_ago < 30:
                return True  # Same price, too recent — skip insert

        # Update live prices
        await session.execute(
            text("""
                INSERT INTO live_prices (instrument_id, price, change_amount, change_percent, market_status, fetched_at)
                VALUES (:iid, :price, :change_amount, :change_percent, :market_status, :fetched_at)
            """),
            {
                "iid": instrument_id,
                "price": live["price"],
                "change_amount": live["change_amount"],
                "change_percent": live["change_percent"],
                "market_status": live["market_status"],
                "fetched_at": now,
            },
        )

        # Update historical price for 'today'
        today = now.date()
        await session.execute(
            text("""
                INSERT INTO historical_prices (instrument_id, date, open, high, low, close, volume)
                VALUES (:iid, :date, :price, :price, :price, :price, 0)
                ON CONFLICT (instrument_id, date) DO UPDATE 
                SET close = EXCLUDED.close,
                    high = GREATEST(historical_prices.high, EXCLUDED.high),
                    low = LEAST(historical_prices.low, EXCLUDED.low)
            """),
            {
                "iid": instrument_id,
                "date": today,
                "price": live["price"],
            },
        )
        
        await session.commit()

    logger.info(
        "[%s] Price: %.2f (%s) status=%s",
        instrument["symbol"], live["price"],
        f"{live['change_percent']:+.2f}%" if live["change_percent"] else "N/A",
        live["market_status"],
    )
    return True
