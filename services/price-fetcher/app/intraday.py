"""Fetch and store intraday (5-minute) candle data for 1D charts."""

import logging
from datetime import datetime, timezone

from sqlalchemy import text

from .db import async_session
from .yahoo import fetch_chart

logger = logging.getLogger(__name__)


def parse_intraday(chart_data: dict) -> list[dict]:
    """Parse chart data into intraday candle rows."""
    timestamps = chart_data.get("timestamp", [])
    quotes = chart_data.get("indicators", {}).get("quote", [{}])[0]

    opens = quotes.get("open", [])
    highs = quotes.get("high", [])
    lows = quotes.get("low", [])
    closes = quotes.get("close", [])
    volumes = quotes.get("volume", [])

    rows = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        rows.append({
            "timestamp": dt,
            "open": float(opens[i]) if opens[i] is not None else float(close),
            "high": float(highs[i]) if highs[i] is not None else float(close),
            "low": float(lows[i]) if lows[i] is not None else float(close),
            "close": float(close),
            "volume": int(volumes[i]) if volumes[i] is not None else 0,
        })

    return rows


async def fetch_and_store_intraday(instrument: dict) -> int:
    """Fetch 5-minute candles for the current/last trading day and store them."""
    yf_symbol = instrument["yfinance_symbol"]
    instrument_id = instrument["id"]
    symbol = instrument["symbol"]

    chart_data = fetch_chart(yf_symbol, period="1d", interval="5m")
    if chart_data is None:
        logger.warning("[%s] No intraday chart data returned", symbol)
        return 0

    rows = parse_intraday(chart_data)
    if not rows:
        logger.warning("[%s] No intraday rows parsed", symbol)
        return 0

    inserted = 0
    async with async_session() as session:
        # Clean old intraday data (keep only today's/last session's data)
        # The Yahoo API returns only the current/last trading day anyway,
        # so we delete anything older than 2 days to keep the table lean
        await session.execute(
            text("""
                DELETE FROM intraday_prices
                WHERE instrument_id = :iid
                AND timestamp < NOW() - INTERVAL '2 days'
            """),
            {"iid": instrument_id},
        )

        for row in rows:
            try:
                await session.execute(
                    text("""
                        INSERT INTO intraday_prices (instrument_id, timestamp, open, high, low, close, volume)
                        VALUES (:iid, :ts, :open, :high, :low, :close, :volume)
                        ON CONFLICT (instrument_id, timestamp) DO UPDATE
                        SET open = EXCLUDED.open, high = EXCLUDED.high,
                            low = EXCLUDED.low, close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """),
                    {
                        "iid": instrument_id,
                        "ts": row["timestamp"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                    },
                )
                inserted += 1
            except Exception:
                logger.exception("[%s] Failed to insert intraday candle at %s", symbol, row["timestamp"])
                await session.rollback()
                continue
        await session.commit()

    if inserted > 0:
        logger.info("[%s] Intraday: %d candles stored", symbol, inserted)
    return inserted
