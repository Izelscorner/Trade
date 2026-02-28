"""Price Fetcher Service - fetches historical and live price data.

On startup, immediately fetches historical data for ALL instruments (backfilling
any gaps from days the app was offline). A fast-check loop then re-scans every
60s for instruments that still have no/stale historical data (e.g. newly added
assets), while the full historical loop continues hourly.
"""

import asyncio
import logging
from datetime import date, timedelta

from sqlalchemy import text

from .db import async_session
from .instruments import get_instruments
from .historical import fetch_and_store_historical, MIN_HISTORICAL_ROWS, reset_sync_state
from .intraday import fetch_and_store_intraday
from .live import fetch_and_store_live

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("price-fetcher")

LIVE_INTERVAL = 60           # 1 minute
INTRADAY_INTERVAL = 300      # 5 minutes (fetch intraday candles)
HISTORICAL_INTERVAL = 3600   # 1 hour (full sweep)
NEW_ASSET_CHECK_INTERVAL = 60  # Check for new/stale assets every 60s
DELAY_BETWEEN_SYMBOLS = 2    # 2 seconds between each symbol


async def get_instruments_needing_history() -> list[dict]:
    """Find instruments with insufficient historical data (< 26 rows or not synced yet)."""
    from .historical import _synced_this_session
    instruments = await get_instruments()
    needs_history = []

    async with async_session() as session:
        for inst in instruments:
            # If never synced this session, it needs a check
            if inst["id"] not in _synced_this_session:
                needs_history.append(inst)
                continue
            result = await session.execute(
                text("SELECT COUNT(*) FROM historical_prices WHERE instrument_id = :iid"),
                {"iid": inst["id"]},
            )
            row = result.fetchone()
            count = row[0] if row else 0
            if count < MIN_HISTORICAL_ROWS:
                needs_history.append(inst)

    return needs_history


async def historical_loop() -> None:
    """Full historical sweep: runs immediately on startup, then hourly."""
    # Reset sync state on startup so all instruments get checked
    reset_sync_state()

    while True:
        try:
            instruments = await get_instruments()
            logger.info("Running full historical sweep for %d instruments", len(instruments))
            for inst in instruments:
                await fetch_and_store_historical(inst)
                await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)
            logger.info("Full historical sweep complete")
        except Exception:
            logger.exception("Error in historical fetch loop")
        await asyncio.sleep(HISTORICAL_INTERVAL)


async def new_asset_history_loop() -> None:
    """Fast loop: checks every 60s for instruments missing historical data."""
    # Let the initial full sweep start first
    await asyncio.sleep(60)

    while True:
        try:
            needing = await get_instruments_needing_history()
            if needing:
                logger.info("Found %d instruments needing historical data", len(needing))
                for inst in needing:
                    await fetch_and_store_historical(inst)
                    await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)
        except Exception:
            logger.exception("Error in new asset history check")
        await asyncio.sleep(NEW_ASSET_CHECK_INTERVAL)


async def live_loop() -> None:
    """Fetch live prices every 60 seconds for near-real-time updates."""
    # Wait for initial historical fetch to get a head start
    await asyncio.sleep(45)

    while True:
        try:
            instruments = await get_instruments()
            for inst in instruments:
                await fetch_and_store_live(inst)
                await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)
        except Exception:
            logger.exception("Error in live fetch loop")
        await asyncio.sleep(LIVE_INTERVAL)


async def intraday_loop() -> None:
    """Fetch 5-minute intraday candles every 5 minutes for 1D charts."""
    # Wait for initial startup tasks
    await asyncio.sleep(30)

    while True:
        try:
            instruments = await get_instruments()
            for inst in instruments:
                await fetch_and_store_intraday(inst)
                await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)
        except Exception:
            logger.exception("Error in intraday fetch loop")
        await asyncio.sleep(INTRADAY_INTERVAL)


async def cleanup_loop() -> None:
    """Keep only the latest 1000 live price entries per instrument."""
    while True:
        await asyncio.sleep(3600)
        try:
            instruments = await get_instruments()

            async with async_session() as session:
                for inst in instruments:
                    await session.execute(
                        text("""
                            DELETE FROM live_prices
                            WHERE instrument_id = :iid
                            AND id NOT IN (
                                SELECT id FROM live_prices
                                WHERE instrument_id = :iid
                                ORDER BY fetched_at DESC
                                LIMIT 1000
                            )
                        """),
                        {"iid": inst["id"]},
                    )
                await session.commit()
            logger.info("Live price cleanup complete")
        except Exception:
            logger.exception("Error in cleanup loop")


async def main() -> None:
    logger.info("Price Fetcher Service starting...")
    await asyncio.gather(
        historical_loop(),
        new_asset_history_loop(),
        live_loop(),
        intraday_loop(),
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
