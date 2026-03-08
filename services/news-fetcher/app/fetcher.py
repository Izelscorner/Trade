"""Fetches and parses RSS feeds — title + description only, no URL scraping."""

import logging
import re
from datetime import datetime, timezone

import aiohttp
import feedparser
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# Shared HTTP session — reused across all feed fetches to avoid
# creating/destroying TCP connections every cycle.
_shared_session: aiohttp.ClientSession | None = None
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; TradeSignal/1.0)",
    "Accept": "application/rss+xml, application/xml, text/xml",
}


def _get_session() -> aiohttp.ClientSession:
    global _shared_session
    if _shared_session is None or _shared_session.closed:
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
        _shared_session = aiohttp.ClientSession(
            timeout=timeout, headers=_HEADERS, connector=connector
        )
    return _shared_session


async def fetch_feed(
    url: str,
    source: str,
    category: str,
    instrument_id: str | None = None,
    asset_name: str | None = None,
) -> list[dict]:
    """Fetch a single RSS feed and return parsed articles (title + description only)."""
    try:
        session = _get_session()
        async with session.get(url, ssl=False) as resp:
            if resp.status != 200:
                logger.warning("Feed %s returned status %d", source, resp.status)
                return []
            body = await resp.text()
    except Exception:
        logger.exception("Failed to fetch feed %s", source)
        return []

    feed = feedparser.parse(body)
    articles = []

    for entry in feed.entries:
        title = entry.get("title", "").strip()
        link = entry.get("link", "")
        summary = entry.get("summary", entry.get("description", ""))

        if summary:
            # Strip HTML tags from summary
            summary = re.sub(r"<[^>]+>", " ", summary)
            summary = re.sub(r"&\w+;", " ", summary)
            summary = re.sub(r"\s+", " ", summary).strip()
            summary = summary[:2000]

        # Skip articles with no title or very short titles
        if not title or len(title) < 10:
            continue

        # Skip if no description
        if not summary or len(summary.strip()) < 5:
            continue

        published_at = None
        for date_field in ("published_parsed", "updated_parsed"):
            parsed = entry.get(date_field)
            if parsed:
                try:
                    published_at = datetime(*parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    pass
                break

        if not published_at:
            raw = entry.get("published", entry.get("updated", ""))
            if raw:
                try:
                    published_at = dateparser.parse(raw)
                    if published_at and published_at.tzinfo is None:
                        published_at = published_at.replace(tzinfo=timezone.utc)
                except Exception:
                    pass

        if not published_at:
            published_at = datetime.now(timezone.utc)

        articles.append({
            "title": title,
            "link": link,
            "summary": summary,
            "content": None,  # No scraping — LLM uses title + summary
            "source": source,
            "category": category,
            "published_at": published_at,
            "instrument_id": instrument_id,
            "asset_name": asset_name,
        })

    logger.info("Fetched %d articles from %s", len(articles), source)
    return articles
