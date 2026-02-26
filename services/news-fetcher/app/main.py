"""News Fetcher Service - continuously fetches RSS feeds and stores articles."""

import asyncio
import logging

from .feeds import FEEDS, POLITICS_CATEGORIES, FINANCE_CATEGORIES
from .fetcher import fetch_feed
from .store import upsert_articles, cleanup_old_politics_news, cleanup_old_finance_news

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("news-fetcher")

POLITICS_INTERVAL = 300      # 5 minutes
FINANCE_INTERVAL = 600       # 10 minutes
CLEANUP_INTERVAL = 900       # 15 minutes


async def fetch_category(category: str, feeds: list[dict]) -> int:
    """Fetch all feeds for a category concurrently."""
    tasks = [fetch_feed(f["url"], f["source"], category) for f in feeds]
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
                logger.info("[%s] Stored %d new articles", category, count)
        except Exception:
            logger.exception("Error in politics fetch loop")
        await asyncio.sleep(POLITICS_INTERVAL)


async def finance_loop() -> None:
    """Fetch financial news on a longer interval (instrument-specific)."""
    while True:
        try:
            for category in sorted(FINANCE_CATEGORIES):
                feeds = FEEDS[category]
                count = await fetch_category(category, feeds)
                logger.info("[%s] Stored %d new articles", category, count)
        except Exception:
            logger.exception("Error in finance fetch loop")
        await asyncio.sleep(FINANCE_INTERVAL)


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
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
