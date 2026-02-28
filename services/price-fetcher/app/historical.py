"""Fetch and store historical OHLCV data. Only accumulates new data."""

import logging
from datetime import date, timedelta

from sqlalchemy import text

from .db import async_session
from .yahoo import fetch_chart, parse_historical

logger = logging.getLogger(__name__)

MIN_HISTORICAL_ROWS = 26  # Minimum rows needed for technical analysis

# Track which instruments have been fully synced this session
_synced_this_session: set[str] = set()


def reset_sync_state():
    """Reset sync tracking (called on service restart)."""
    _synced_this_session.clear()


async def get_historical_stats(instrument_id: str) -> tuple[date | None, int, int]:
    """Get the most recent date, total count, and count of last 5 trading days."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT MAX(date), COUNT(*),
                    (SELECT COUNT(*) FROM historical_prices
                     WHERE instrument_id = :iid
                     AND date >= CURRENT_DATE - INTERVAL '7 days'
                     AND volume > 0) as recent_with_volume
                FROM historical_prices WHERE instrument_id = :iid
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()
        last_date = row[0] if row and row[0] else None
        count = row[1] if row else 0
        recent_real = row[2] if row else 0
        return last_date, count, recent_real


def compute_period(last_date: date | None, row_count: int = 0) -> str:
    """Compute yfinance-compatible period string based on last stored date."""
    if last_date is None or row_count < MIN_HISTORICAL_ROWS:
        return "5y"

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


async def fetch_and_store_historical(instrument: dict, force: bool = False) -> int:
    """Fetch historical data from Yahoo Finance. Only stores new data.

    On first call per session (or force=True), always fetches to catch up
    any gaps from downtime. Subsequent calls within the same session use
    the optimized skip logic.
    """
    yf_symbol = instrument["yfinance_symbol"]
    instrument_id = instrument["id"]
    symbol = instrument["symbol"]

    last_date, row_count, recent_real = await get_historical_stats(instrument_id)

    first_sync = instrument_id not in _synced_this_session

    # Determine if we need to fetch
    needs_fetch = False
    reason = ""

    if row_count < MIN_HISTORICAL_ROWS:
        needs_fetch = True
        reason = f"insufficient data ({row_count} rows)"
    elif first_sync or force:
        # On first run this session, always fetch to catch up gaps
        # Live prices insert today with volume=0, so check recent_real
        # (rows with volume > 0 from the last 7 days) to detect gaps
        needs_fetch = True
        reason = "startup catch-up"
    elif last_date and last_date < date.today() - timedelta(days=1):
        needs_fetch = True
        reason = f"stale data (last={last_date})"
    else:
        logger.info("[%s] Historical data up to date (%d rows, last=%s)", symbol, row_count, last_date)
        return 0

    period = compute_period(last_date, row_count)
    # On startup catch-up with existing data, fetch at least 1mo to cover any gaps
    if first_sync and row_count >= MIN_HISTORICAL_ROWS and period == "5d":
        period = "1mo"

    logger.info("[%s] Fetching historical data (period=%s, last=%s, rows=%d, reason=%s)",
                symbol, period, last_date, row_count, reason)

    chart_data = fetch_chart(yf_symbol, period=period, interval="1d")
    if chart_data is None:
        logger.warning("[%s] No chart data returned", symbol)
        return 0

    rows = parse_historical(chart_data)
    if not rows:
        logger.warning("[%s] No historical rows parsed", symbol)
        return 0

    # Use ON CONFLICT DO UPDATE to fill in proper OHLCV for days that only
    # have live-price stubs (volume=0)
    inserted = 0
    updated = 0
    async with async_session() as session:
        for row in rows:
            try:
                result = await session.execute(
                    text("""
                        INSERT INTO historical_prices (instrument_id, date, open, high, low, close, volume)
                        VALUES (:iid, :date, :open, :high, :low, :close, :volume)
                        ON CONFLICT (instrument_id, date) DO UPDATE
                        SET open = CASE WHEN historical_prices.volume = 0 THEN EXCLUDED.open ELSE historical_prices.open END,
                            high = CASE WHEN historical_prices.volume = 0 THEN EXCLUDED.high ELSE GREATEST(historical_prices.high, EXCLUDED.high) END,
                            low = CASE WHEN historical_prices.volume = 0 THEN EXCLUDED.low ELSE LEAST(historical_prices.low, EXCLUDED.low) END,
                            close = EXCLUDED.close,
                            volume = CASE WHEN EXCLUDED.volume > 0 THEN EXCLUDED.volume ELSE historical_prices.volume END
                        RETURNING id, (xmax = 0) as was_inserted
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
                r = result.fetchone()
                if r:
                    if r.was_inserted:
                        inserted += 1
                    else:
                        updated += 1
            except Exception:
                logger.exception("[%s] Failed to insert for %s", symbol, row["date"])
                await session.rollback()
                continue
        await session.commit()

    _synced_this_session.add(instrument_id)

    if inserted > 0 or updated > 0:
        logger.info("[%s] Historical: %d inserted, %d updated", symbol, inserted, updated)
    else:
        logger.info("[%s] Historical data confirmed up to date", symbol)
    return inserted
