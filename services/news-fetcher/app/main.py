"""News Fetcher Service - continuously fetches RSS feeds and stores articles.

Fetches macro news (markets, politics, conflict) and asset-specific news
from Yahoo Finance and Google News. Articles are stored with
ollama_processed=false and picked up by the llm-processor service.
"""

import asyncio
import logging
import urllib.parse

from sqlalchemy import text

from .feeds import FEEDS, MACRO_CATEGORIES
from .fetcher import fetch_feed
from .store import upsert_articles, cleanup_old_macro_news, cleanup_old_asset_news
from .instruments import get_instruments
from .db import async_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("news-fetcher")

MACRO_INTERVAL = 300         # 5 minutes
ASSET_INTERVAL = 900         # 15 minutes
CLEANUP_INTERVAL = 900       # 15 minutes
NEW_ASSET_CHECK_INTERVAL = 120  # 2 minutes


async def fetch_category(category: str, feeds: list[dict], instrument_id: str | None = None, asset_name: str | None = None) -> int:
    """Fetch all feeds for a category concurrently."""
    tasks = [fetch_feed(f["url"], f["source"], category, instrument_id, asset_name) for f in feeds]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_articles = []
    for result in results:
        if isinstance(result, Exception):
            logger.error("Feed fetch error: %s", result)
            continue
        all_articles.extend(result)

    return await upsert_articles(all_articles)


async def fetch_instrument_news(inst: dict) -> int:
    """Fetch news for a single instrument from Yahoo Finance and Google News."""
    symbol = inst["symbol"]
    name = inst["name"]
    yf_symbol = inst["yfinance_symbol"]

    logger.info("Fetching specific news for %s (%s)", symbol, name)
    yf_url = f"https://finance.yahoo.com/rss/headline?s={yf_symbol}"
    encoded_name = urllib.parse.quote(name)
    gf_url = f"https://news.google.com/rss/search?q={encoded_name}&hl=en-US&gl=US&ceid=US:en"

    feeds = [
        {"url": yf_url, "source": "Yahoo Finance (Asset)"},
        {"url": gf_url, "source": "Google Finance (Asset)"}
    ]

    count = await fetch_category("asset_specific", feeds, instrument_id=inst["id"], asset_name=name)
    if count > 0:
        logger.info("[asset_specific] Stored %d new articles for %s", count, symbol)
    return count


async def macro_loop() -> None:
    """Fetch all macro news categories on a regular interval."""
    while True:
        try:
            for category in sorted(MACRO_CATEGORIES):
                feeds = FEEDS[category]
                count = await fetch_category(category, feeds)
                if count > 0:
                    logger.info("[%s] Stored %d new articles", category, count)
        except Exception:
            logger.exception("Error in macro fetch loop")
        await asyncio.sleep(MACRO_INTERVAL)


async def get_prioritized_instrument() -> dict | None:
    """Check if there's a prioritized instrument in the processing queue."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT i.id, i.symbol, i.name, i.yfinance_symbol
                FROM processing_priority pp
                JOIN instruments i ON i.id = pp.instrument_id
                ORDER BY pp.requested_at DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row:
            return {"id": str(row.id), "symbol": row.symbol, "name": row.name, "yfinance_symbol": row.yfinance_symbol}
    return None


async def instruments_loop() -> None:
    """Fetch instrument-specific news from Yahoo Finance and Google News.

    Checks for prioritized instruments first and fetches those before
    the regular cycle through all instruments. All instruments are fetched
    concurrently (up to 3 at a time) to reduce cycle time.
    """
    sem = asyncio.Semaphore(3)

    async def fetch_with_semaphore(inst: dict) -> int:
        async with sem:
            return await fetch_instrument_news(inst)

    while True:
        try:
            # Check for prioritized instrument and fetch it first
            priority = await get_prioritized_instrument()
            if priority:
                logger.info("Priority fetch for %s", priority["symbol"])
                await fetch_instrument_news(priority)

            instruments = await get_instruments()

            # Fetch all instruments concurrently with a semaphore limit
            tasks = [fetch_with_semaphore(inst) for inst in instruments]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for inst, result in zip(instruments, results):
                if isinstance(result, Exception):
                    logger.error("Error fetching news for %s: %s", inst["symbol"], result)

        except Exception:
            logger.exception("Error in instruments fetch loop")
        await asyncio.sleep(ASSET_INTERVAL)


async def new_asset_news_loop() -> None:
    """Fast loop: checks every 2 min for instruments with zero news articles."""
    await asyncio.sleep(60)  # Let initial loops start first

    while True:
        try:
            instruments = await get_instruments()
            async with async_session() as session:
                for inst in instruments:
                    result = await session.execute(
                        text("""
                            SELECT COUNT(*) as cnt FROM news_instrument_map
                            WHERE instrument_id = :iid
                        """),
                        {"iid": inst["id"]},
                    )
                    row = result.fetchone()
                    if row and row.cnt == 0:
                        logger.info("New asset %s has no news — fetching now", inst["symbol"])
                        await fetch_instrument_news(inst)
                        await asyncio.sleep(2)
        except Exception:
            logger.exception("Error in new asset news check")
        await asyncio.sleep(NEW_ASSET_CHECK_INTERVAL)


async def cleanup_loop() -> None:
    """Periodically clean up stale articles."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        try:
            await cleanup_old_macro_news()
            await cleanup_old_asset_news()
        except Exception:
            logger.exception("Error in cleanup loop")


async def main() -> None:
    logger.info("News Fetcher Service starting...")
    await asyncio.gather(
        macro_loop(),
        instruments_loop(),
        new_asset_news_loop(),
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
