"""Technical Analysis Service - computes indicators from historical price data."""

import asyncio
import json
import logging
from datetime import date, datetime, timezone

import pandas as pd
from sqlalchemy import text

from .db import async_session
from .indicators import run_all_indicators

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("technical-analysis")

ANALYSIS_INTERVAL = 900  # 15 minutes


async def get_instruments() -> list[dict]:
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category FROM instruments ORDER BY symbol")
        )
        return [{"id": str(r.id), "symbol": r.symbol, "name": r.name, "category": r.category} for r in result.fetchall()]


async def get_price_data(instrument_id: str) -> pd.DataFrame:
    """Load historical price data into a pandas DataFrame."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT date, open, high, low, close, volume
                FROM historical_prices
                WHERE instrument_id = :iid
                ORDER BY date ASC
            """),
            {"iid": instrument_id},
        )
        rows = result.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [(r.date, float(r.open), float(r.high), float(r.low), float(r.close), int(r.volume)) for r in rows],
        columns=["date", "open", "high", "low", "close", "volume"],
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


async def store_indicators(instrument_id: str, indicators: list[dict], analysis_date: date) -> None:
    """Store computed indicators in the database."""
    async with async_session() as session:
        for ind in indicators:
            try:
                await session.execute(
                    text("""
                        INSERT INTO technical_indicators (instrument_id, date, indicator_name, value, signal, calculated_at)
                        VALUES (:iid, :date, :name, :value, :signal, :calc_at)
                        ON CONFLICT (instrument_id, date, indicator_name)
                        DO UPDATE SET value = :value, signal = :signal, calculated_at = :calc_at
                    """),
                    {
                        "iid": instrument_id,
                        "date": analysis_date,
                        "name": ind["indicator_name"],
                        "value": json.dumps(ind["value"]),
                        "signal": ind["signal"],
                        "calc_at": datetime.now(timezone.utc),
                    },
                )
            except Exception:
                logger.exception("Failed to store indicator %s", ind["indicator_name"])
                await session.rollback()
                continue
        await session.commit()


async def analyze_instrument(instrument: dict) -> None:
    """Run full technical analysis on a single instrument."""
    df = await get_price_data(instrument["id"])
    if df.empty:
        logger.warning("[%s] No price data available, skipping", instrument["symbol"])
        return

    indicators = run_all_indicators(df)
    if not indicators:
        logger.warning("[%s] Not enough data for analysis (need 50+ days)", instrument["symbol"])
        return

    analysis_date = df["date"].iloc[-1].date() if hasattr(df["date"].iloc[-1], "date") else df["date"].iloc[-1]
    await store_indicators(instrument["id"], indicators, analysis_date)

    signals = {ind["indicator_name"]: ind["signal"] for ind in indicators}
    logger.info("[%s] Analysis complete: %s", instrument["symbol"], signals)


async def analysis_loop() -> None:
    """Main loop: run TA on all instruments periodically."""
    # Wait for price data to be available
    await asyncio.sleep(30)

    while True:
        try:
            instruments = await get_instruments()
            for inst in instruments:
                await analyze_instrument(inst)
        except Exception:
            logger.exception("Error in analysis loop")
        await asyncio.sleep(ANALYSIS_INTERVAL)


async def main() -> None:
    logger.info("Technical Analysis Service starting...")
    await analysis_loop()


if __name__ == "__main__":
    asyncio.run(main())
