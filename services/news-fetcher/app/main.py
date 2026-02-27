"""News Fetcher Service - continuously fetches RSS feeds and stores articles."""

import asyncio
import logging

from .feeds import FEEDS, POLITICS_CATEGORIES, FINANCE_CATEGORIES
from .fetcher import fetch_feed
from .store import upsert_articles, cleanup_old_politics_news, cleanup_old_finance_news
from .instruments import get_instruments

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("news-fetcher")

POLITICS_INTERVAL = 300      # 5 minutes
FINANCE_INTERVAL = 600       # 10 minutes
ASSET_INTERVAL = 900         # 15 minutes
CLEANUP_INTERVAL = 900       # 15 minutes


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


async def politics_loop() -> None:
    """Fetch political news on a short interval (macro sentiment)."""
    while True:
        try:
            for category in sorted(POLITICS_CATEGORIES):
                feeds = FEEDS[category]
                count = await fetch_category(category, feeds)
                if count > 0:
                    logger.info("[%s] Stored %d new articles", category, count)
        except Exception:
            logger.exception("Error in politics fetch loop")
        await asyncio.sleep(POLITICS_INTERVAL)


async def finance_loop() -> None:
    """Fetch financial news on a longer interval (general market)."""
    while True:
        try:
            for category in sorted(FINANCE_CATEGORIES):
                feeds = FEEDS[category]
                count = await fetch_category(category, feeds)
                if count > 0:
                    logger.info("[%s] Stored %d new articles", category, count)
        except Exception:
            logger.exception("Error in finance fetch loop")
        await asyncio.sleep(FINANCE_INTERVAL)


async def instruments_loop() -> None:
    """Fetch instrument-specific news directly from Yahoo Finance."""
    while True:
        try:
            instruments = await get_instruments()
            import urllib.parse
            for inst in instruments:
                symbol = inst["symbol"]
                name = inst["name"]
                yf_symbol = inst["yfinance_symbol"]
                
                logger.info("Fetching specific news for %s (%s)", symbol, name)
                yf_url = f"https://finance.yahoo.com/rss/headline?s={yf_symbol}"
                
                # Use name for Google News for better broad matches (important for ETFs)
                encoded_name = urllib.parse.quote(name)
                gf_url = f"https://news.google.com/rss/search?q={encoded_name}&hl=en-US&gl=US&ceid=US:en"
                
                feeds = [
                    {"url": yf_url, "source": "Yahoo Finance (Asset)"},
                    {"url": gf_url, "source": "Google Finance (Asset)"}
                ]
                
                count = await fetch_category("asset_specific", feeds, instrument_id=inst["id"], asset_name=name)
                if count > 0:
                    logger.info("[asset_specific] Stored %d new articles for %s", count, symbol)
                
                # Sleep a tiny bit to avoid spamming Yahoo Finance too rapidly
                await asyncio.sleep(2)
                
        except Exception:
            logger.exception("Error in instruments fetch loop")
        await asyncio.sleep(ASSET_INTERVAL)


async def cleanup_loop() -> None:
    """Periodically clean up stale articles."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        try:
            await cleanup_old_politics_news()
            await cleanup_old_finance_news()
        except Exception:
            logger.exception("Error in cleanup loop")


async def main() -> None:
    logger.info("News Fetcher Service starting...")
    await asyncio.gather(
        politics_loop(),
        finance_loop(),
        instruments_loop(),
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
