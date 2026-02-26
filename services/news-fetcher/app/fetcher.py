"""Fetches and parses RSS feeds."""

import logging
from datetime import datetime, timezone

import aiohttp
import feedparser
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)


async def fetch_feed(url: str, source: str, category: str) -> list[dict]:
    """Fetch a single RSS feed and return parsed articles."""
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {"User-Agent": "TradeSignal-NewsFetcher/1.0"}
            async with session.get(url, headers=headers, ssl=False) as resp:
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
        if not title:
            continue

        link = entry.get("link", "")
        summary = entry.get("summary", entry.get("description", ""))
        if summary:
            summary = summary[:2000]

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
            "source": source,
            "category": category,
            "published_at": published_at,
        })

    logger.info("Fetched %d articles from %s", len(articles), source)
    return articles
