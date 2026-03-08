"""News Fetcher Service - continuously fetches RSS feeds and stores articles.

Fetches macro news (markets, politics, conflict) and asset-specific news
from Yahoo Finance and Google News. Articles are stored with
ollama_processed=false and picked up by the llm-processor service.

Two fetch loops:
  - Main loop (10s): all macro feeds + asset-specific feeds
  - Slow loop (30s): high-volume feeds (StockTitan, MarketWatch)
"""

import asyncio
import logging
import urllib.parse

from sqlalchemy import text

from .feeds import MAIN_FEEDS, SLOW_FEEDS, MACRO_CATEGORIES
from .fetcher import fetch_feed
from .store import upsert_articles, cleanup_old_macro_news, cleanup_old_asset_news
from .instruments import get_instruments
from .db import async_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("news-fetcher")

MAIN_INTERVAL = 10           # 10 seconds
SLOW_INTERVAL = 30           # 30 seconds
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

    yf_url = f"https://finance.yahoo.com/rss/headline?s={yf_symbol}"
    encoded_query = urllib.parse.quote(f"{symbol} stock")
    gn_url = f"https://news.google.com/rss/search?q={encoded_query}"

    feeds = [
        {"url": yf_url, "source": f"Yahoo Finance ({symbol})"},
        {"url": gn_url, "source": f"Google News ({symbol})"},
    ]

    count = await fetch_category("asset_specific", feeds, instrument_id=inst["id"], asset_name=name)
    if count > 0:
        logger.info("[asset_specific] Stored %d new articles for %s", count, symbol)
    return count


async def main_loop() -> None:
    """Main fetch loop — macro feeds + asset-specific feeds every 10 seconds."""
    sem = asyncio.Semaphore(5)

    async def fetch_with_semaphore(inst: dict) -> int:
        async with sem:
            return await fetch_instrument_news(inst)

    while True:
        try:
            # Fetch all macro categories
            for category in sorted(MACRO_CATEGORIES):
                feeds = MAIN_FEEDS.get(category, [])
                if feeds:
                    count = await fetch_category(category, feeds)
                    if count > 0:
                        logger.info("[%s] Stored %d new articles", category, count)

            # Check for prioritized instrument and fetch it first
            priority = await get_prioritized_instrument()
            if priority:
                logger.info("Priority fetch for %s", priority["symbol"])
                await fetch_instrument_news(priority)

            # Fetch all instrument-specific feeds concurrently
            instruments = await get_instruments()
            tasks = [fetch_with_semaphore(inst) for inst in instruments]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for inst, result in zip(instruments, results):
                if isinstance(result, Exception):
                    logger.error("Error fetching news for %s: %s", inst["symbol"], result)

        except Exception:
            logger.exception("Error in main fetch loop")
        await asyncio.sleep(MAIN_INTERVAL)


async def slow_loop() -> None:
    """Slow fetch loop — high-volume feeds (StockTitan, MarketWatch) every 30 seconds."""
    await asyncio.sleep(5)  # Stagger start
    while True:
        try:
            for category, feeds in SLOW_FEEDS.items():
                count = await fetch_category(category, feeds)
                if count > 0:
                    logger.info("[slow/%s] Stored %d new articles", category, count)
        except Exception:
            logger.exception("Error in slow fetch loop")
        await asyncio.sleep(SLOW_INTERVAL)


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
        main_loop(),
        slow_loop(),
        new_asset_news_loop(),
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
