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

from .feeds import MAIN_FEEDS, SLOW_FEEDS, SECTOR_FEEDS, MACRO_CATEGORIES
from .fetcher import fetch_feed
from .store import upsert_articles, cleanup_old_macro_news, cleanup_old_asset_news, cleanup_old_sector_news
from .instruments import get_instruments
from .db import async_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("news-fetcher")

MAIN_INTERVAL = 120          # 2 minutes — RSS feeds rarely update faster
SLOW_INTERVAL = 180          # 3 minutes
CLEANUP_INTERVAL = 900       # 15 minutes
NEW_ASSET_CHECK_INTERVAL = 120  # 2 minutes
SECTOR_INTERVAL = 300        # 5 minutes — sector news is less time-critical

# Dynamic semaphore scales with instrument count
BASE_SEMAPHORE = 5           # base concurrent fetches for <=20 instruments
MAX_SEMAPHORE = 15           # cap for large instrument counts

# Priority tiers control fetch depth — high-priority instruments get
# more search sources while low-priority get fewer.
PRIORITY_HIGH_SOURCES = 3    # Yahoo + Google + extra Google query
PRIORITY_NORMAL_SOURCES = 2  # Yahoo + Google (default)


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


async def fetch_instrument_news(inst: dict, priority: str = "normal") -> int:
    """Fetch news for a single instrument from Yahoo Finance and Google News.

    Priority levels control fetch depth:
      - 'high': Yahoo + Google + extra Google query (deeper search)
      - 'normal': Yahoo + Google (default)
      - 'low': Google only (minimal — for bulk low-priority instruments)
    """
    symbol = inst["symbol"]
    name = inst["name"]
    yf_symbol = inst["yfinance_symbol"]

    yf_url = f"https://finance.yahoo.com/rss/headline?s={yf_symbol}"
    encoded_query = urllib.parse.quote(f"{symbol} stock")
    gn_url = f"https://news.google.com/rss/search?q={encoded_query}"

    if priority == "high":
        # Extra query with company name for deeper coverage
        encoded_name_query = urllib.parse.quote(f"{name} stock market")
        gn_url_extra = f"https://news.google.com/rss/search?q={encoded_name_query}"
        feeds = [
            {"url": yf_url, "source": f"Yahoo Finance ({symbol})"},
            {"url": gn_url, "source": f"Google News ({symbol})"},
            {"url": gn_url_extra, "source": f"Google News ({name})"},
        ]
    elif priority == "low":
        feeds = [
            {"url": gn_url, "source": f"Google News ({symbol})"},
        ]
    else:
        feeds = [
            {"url": yf_url, "source": f"Yahoo Finance ({symbol})"},
            {"url": gn_url, "source": f"Google News ({symbol})"},
        ]

    count = await fetch_category("asset_specific", feeds, instrument_id=inst["id"], asset_name=name)
    if count > 0:
        logger.info("[asset_specific] Stored %d new articles for %s (priority=%s)", count, symbol, priority)
    return count


async def get_instrument_article_counts() -> dict[str, int]:
    """Get article counts per instrument for priority classification."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT i.id::text, COUNT(nim.article_id) as cnt
                FROM instruments i
                LEFT JOIN news_instrument_map nim ON nim.instrument_id = i.id
                LEFT JOIN news_articles a ON a.id = nim.article_id AND a.published_at >= NOW() - INTERVAL '2 days'
                WHERE i.is_active = true
                GROUP BY i.id
            """)
        )
        return {str(r.id): r.cnt for r in result.fetchall()}


def classify_instrument_priority(inst: dict, article_counts: dict[str, int], prioritized_ids: set[str]) -> str:
    """Classify instrument fetch priority based on article volume and user interest.

    - 'high': user-prioritized or zero articles (needs urgent coverage)
    - 'normal': moderate article count (standard fetch)
    - 'low': already has many recent articles (diminishing returns)
    """
    iid = inst["id"]

    if iid in prioritized_ids:
        return "high"

    count = article_counts.get(iid, 0)
    if count == 0:
        return "high"
    elif count >= 30:
        return "low"
    else:
        return "normal"


async def main_loop() -> None:
    """Main fetch loop — macro feeds + asset-specific feeds.

    Scaling features:
      - Dynamic semaphore: scales with instrument count (5 base, up to 15)
      - Priority-based fetch depth: high/normal/low instruments get different coverage
      - High-priority instruments (user-clicked, zero articles) get deeper search
      - Low-priority instruments (30+ recent articles) get minimal search
    """
    _cached_instruments: list[dict] = []
    _instrument_refresh_counter = 0

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
            prioritized_ids: set[str] = set()
            if priority:
                prioritized_ids.add(priority["id"])
                logger.info("Priority fetch for %s", priority["symbol"])
                await fetch_instrument_news(priority, priority="high")
                # For ETFs, also priority-fetch news for all tracked constituent instruments
                if priority.get("category") == "ETF":
                    constituents = await get_etf_constituent_instruments(priority["id"])
                    if constituents:
                        logger.info("Priority fetch for %d constituents of ETF %s", len(constituents), priority["symbol"])
                        for const_inst in constituents:
                            await fetch_instrument_news(const_inst, priority="high")

            # Refresh instrument list every 10 cycles (~20 min) instead of every cycle
            _instrument_refresh_counter += 1
            if not _cached_instruments or _instrument_refresh_counter % 10 == 0:
                _cached_instruments = await get_instruments()
                logger.info("Refreshed instrument list: %d instruments", len(_cached_instruments))

            # Dynamic semaphore: scale with instrument count
            sem_size = min(MAX_SEMAPHORE, max(BASE_SEMAPHORE, len(_cached_instruments) // 10 + BASE_SEMAPHORE))
            sem = asyncio.Semaphore(sem_size)

            # Classify instruments by priority for fetch depth
            article_counts = await get_instrument_article_counts()

            async def fetch_with_semaphore(inst: dict, pri: str) -> int:
                async with sem:
                    return await fetch_instrument_news(inst, priority=pri)

            # Build tasks with per-instrument priority
            tasks = []
            for inst in _cached_instruments:
                pri = classify_instrument_priority(inst, article_counts, prioritized_ids)
                tasks.append(fetch_with_semaphore(inst, pri))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for inst, result in zip(_cached_instruments, results):
                if isinstance(result, Exception):
                    logger.error("Error fetching news for %s: %s", inst["symbol"], result)

            high_count = sum(1 for i in _cached_instruments
                           if classify_instrument_priority(i, article_counts, prioritized_ids) == "high")
            low_count = sum(1 for i in _cached_instruments
                          if classify_instrument_priority(i, article_counts, prioritized_ids) == "low")
            logger.info("Fetch cycle complete: %d instruments (sem=%d, high=%d, low=%d)",
                        len(_cached_instruments), sem_size, high_count, low_count)

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
                SELECT i.id, i.symbol, i.name, i.yfinance_symbol, i.category
                FROM processing_priority pp
                JOIN instruments i ON i.id = pp.instrument_id
                ORDER BY pp.requested_at DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()
        if row:
            return {"id": str(row.id), "symbol": row.symbol, "name": row.name, "yfinance_symbol": row.yfinance_symbol, "category": row.category}
    return None


async def get_etf_constituent_instruments(etf_instrument_id: str) -> list[dict]:
    """Get tracked instruments that are constituents of the given ETF."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT i.id, i.symbol, i.name, i.yfinance_symbol
                FROM etf_constituents ec
                JOIN instruments i ON UPPER(i.symbol) = UPPER(ec.constituent_symbol)
                WHERE ec.etf_instrument_id = :etf_id::uuid
            """),
            {"etf_id": etf_instrument_id},
        )
        return [
            {"id": str(r.id), "symbol": r.symbol, "name": r.name, "yfinance_symbol": r.yfinance_symbol}
            for r in result.fetchall()
        ]


ETF_CONSTITUENT_INTERVAL = 600  # 10 minutes — constituents don't change often


async def get_all_etf_constituents() -> list[dict]:
    """Get all ETF constituents with their parent ETF instrument info.

    Returns constituents that are NOT already tracked as full instruments,
    since tracked instruments already have their own news fetch cycle.
    """
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT ec.constituent_symbol, ec.constituent_name, ec.weight_percent,
                       ec.etf_instrument_id, i_etf.symbol AS etf_symbol
                FROM etf_constituents ec
                JOIN instruments i_etf ON i_etf.id = ec.etf_instrument_id
                LEFT JOIN instruments i_tracked ON UPPER(i_tracked.symbol) = UPPER(ec.constituent_symbol)
                WHERE i_tracked.id IS NULL
                ORDER BY ec.weight_percent DESC
            """)
        )
        return [
            {
                "symbol": r.constituent_symbol,
                "name": r.constituent_name,
                "weight_percent": float(r.weight_percent),
                "etf_instrument_id": str(r.etf_instrument_id),
                "etf_symbol": r.etf_symbol,
            }
            for r in result.fetchall()
        ]


async def fetch_constituent_news(constituent: dict) -> int:
    """Fetch news for an ETF constituent and map articles to the parent ETF."""
    symbol = constituent["symbol"]
    name = constituent["name"]
    etf_id = constituent["etf_instrument_id"]

    encoded_query = urllib.parse.quote(f"{symbol} stock")
    yf_url = f"https://finance.yahoo.com/rss/headline?s={symbol}"
    gn_url = f"https://news.google.com/rss/search?q={encoded_query}"

    feeds = [
        {"url": yf_url, "source": f"Yahoo Finance ({symbol})"},
        {"url": gn_url, "source": f"Google News ({symbol})"},
    ]

    count = await fetch_category("asset_specific", feeds, instrument_id=etf_id, asset_name=name)
    if count > 0:
        logger.info("[etf_constituent] Stored %d new articles for %s (-> ETF %s)", count, symbol, constituent["etf_symbol"])
    return count


async def etf_constituents_loop() -> None:
    """Continuously fetch news for untracked ETF constituents every 5 minutes.

    Constituents that are already tracked instruments (e.g., NVDA, AAPL) are
    skipped since their news is fetched in the main loop and propagated via
    the LLM processor. This handles the remaining holdings (MSFT, AVGO, CRM,
    etc.) and maps articles directly to the parent ETF.
    """
    await asyncio.sleep(30)  # Let main loop start first
    sem = asyncio.Semaphore(3)

    async def fetch_with_semaphore(c: dict) -> int:
        async with sem:
            return await fetch_constituent_news(c)

    while True:
        try:
            constituents = await get_all_etf_constituents()
            if constituents:
                logger.info("Fetching news for %d untracked ETF constituents", len(constituents))
                tasks = [fetch_with_semaphore(c) for c in constituents]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                total = sum(r for r in results if isinstance(r, int))
                if total > 0:
                    logger.info("[etf_constituents] Total %d new articles from %d constituents", total, len(constituents))
        except Exception:
            logger.exception("Error in ETF constituents news loop")
        await asyncio.sleep(ETF_CONSTITUENT_INTERVAL)


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


async def sector_loop() -> None:
    """Fetch sector-specific news feeds every 5 minutes."""
    await asyncio.sleep(20)  # Stagger start
    while True:
        try:
            for category, feeds in SECTOR_FEEDS.items():
                count = await fetch_category(category, feeds)
                if count > 0:
                    logger.info("[%s] Stored %d new sector articles", category, count)
        except Exception:
            logger.exception("Error in sector fetch loop")
        await asyncio.sleep(SECTOR_INTERVAL)


async def cleanup_loop() -> None:
    """Periodically clean up stale articles."""
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        try:
            await cleanup_old_macro_news()
            await cleanup_old_asset_news()
            await cleanup_old_sector_news()
        except Exception:
            logger.exception("Error in cleanup loop")


async def main() -> None:
    logger.info("News Fetcher Service starting...")
    await asyncio.gather(
        main_loop(),
        slow_loop(),
        new_asset_news_loop(),
        etf_constituents_loop(),
        sector_loop(),
        cleanup_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
