"""Price Fetcher Service - fetches historical and live price data."""

import asyncio
import logging

from .instruments import get_instruments
from .historical import fetch_and_store_historical
from .live import fetch_and_store_live

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("price-fetcher")

LIVE_INTERVAL = 60           # 1 minute - faster updates for live trading signals
HISTORICAL_INTERVAL = 3600   # 1 hour
DELAY_BETWEEN_SYMBOLS = 2    # 2 seconds between each symbol


async def historical_loop() -> None:
    """Fetch historical data on startup, then check hourly for new data."""
    while True:
        try:
            instruments = await get_instruments()
            for inst in instruments:
                await fetch_and_store_historical(inst)
                await asyncio.sleep(DELAY_BETWEEN_SYMBOLS)
        except Exception:
            logger.exception("Error in historical fetch loop")
        await asyncio.sleep(HISTORICAL_INTERVAL)


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


async def cleanup_loop() -> None:
    """Keep only the latest 1000 live price entries per instrument."""
    while True:
        await asyncio.sleep(3600)
        try:
            instruments = await get_instruments()
            from sqlalchemy import text
            from .db import async_session

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
        live_loop(),
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
