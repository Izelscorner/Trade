"""Fetches and parses RSS feeds with smart content scraping."""

import logging
import re
from datetime import datetime, timezone

import aiohttp
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

logger = logging.getLogger(__name__)

# Tags that typically contain article body text
_ARTICLE_TAGS = [
    "article", "main", "[role='main']",
    ".article-body", ".story-body", ".post-content",
    ".entry-content", ".article-content", ".article__body",
    ".wysiwyg", ".field-body", ".article-text",
]

# Paywall / cookie wall indicators
_PAYWALL_INDICATORS = [
    "subscribe to continue", "subscription required", "premium content",
    "sign in to read", "create an account", "register to read",
    "you've reached your limit", "this content is for subscribers",
    "paywall", "metered content",
]

_COOKIE_WALL_SELECTORS = [
    ".cookie-banner", ".cookie-consent", ".cookie-notice",
    "#cookie-consent", "#gdpr-consent", ".consent-wall",
    "[data-testid='cookie-policy']", ".privacy-gate",
]


async def fetch_article_content(url: str, session: aiohttp.ClientSession) -> str | None:
    """Scrape article body text from a URL. Returns plain text or None on failure."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        async with session.get(url, headers=headers, ssl=False, timeout=aiohttp.ClientTimeout(total=15), allow_redirects=True) as resp:
            if resp.status != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                return None
            html = await resp.text(errors="replace")
    except Exception:
        return None

    try:
        soup = BeautifulSoup(html, "lxml")

        # Remove noisy elements
        for tag in soup.find_all(["script", "style", "nav", "footer", "aside",
                                   "header", "form", "noscript", "iframe",
                                   "svg", "button", "input"]):
            tag.decompose()

        # Remove cookie/consent walls
        for selector in _COOKIE_WALL_SELECTORS:
            for el in soup.select(selector):
                el.decompose()

        # Remove ads and related content sections
        for cls in ["ad", "ads", "advertisement", "sidebar", "related-articles",
                     "recommended", "newsletter", "social-share", "share-buttons"]:
            for el in soup.find_all(class_=re.compile(cls, re.I)):
                el.decompose()

        # Check for paywall indicators
        page_text = soup.get_text(separator=" ", strip=True).lower()
        paywall_count = sum(1 for indicator in _PAYWALL_INDICATORS if indicator in page_text)
        if paywall_count >= 2:
            return None

        # Try to find the article body using common selectors
        body = None
        for selector in _ARTICLE_TAGS:
            if selector.startswith(".") or selector.startswith("["):
                body = soup.select_one(selector)
            else:
                body = soup.find(selector)
            if body:
                break

        # Fallback to body tag
        if not body:
            body = soup.find("body")

        if not body:
            return None

        # Extract text from paragraphs for cleaner output
        paragraphs = body.find_all("p")
        if paragraphs:
            text = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)
        else:
            text = body.get_text(separator=" ", strip=True)

        # Clean up whitespace and residual HTML entities
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"&\w+;", " ", text)

        # Return first 5000 chars (enough for sentiment, not too much to store)
        return text[:5000] if len(text) > 50 else None
    except Exception:
        return None


def is_spam(title: str, summary: str, link: str, category: str = "") -> bool:
    """Filter out obvious ad/spam listings and enforce relevance."""
    content = f"{title} {summary}".lower()

    # Aggressive negative keywords for lifestyle, entertainment, viral junk
    spam_keywords = [
        "sponsored", "advertisement", "promotional", "promo code", "buy now",
        "discount", "ad by", "unsubscribe", "deals of the day",
        "viral", "celebrity", "pajamas", "flight", "tiktok", "instagram", "post sparks debate",
        "kardashian", "taylor swift", "movie", "netflix show", "hollywood", "outfit",
        "red carpet", "recipe", "diet", "weight loss", "fitness routine", "horoscope",
        "zodiac", "skincare", "best places to live", "vacation", "tourist", "game review",
        "listicle", "click here", "free trial", "limited time offer",
    ]
    if any(k in content for k in spam_keywords):
        return True

    # Avoid completely empty info
    if not title or len(title) < 10:
        return True

    # Enforce some finance/political/market/geopolitical terminology
    relevance_keywords = {
        # Finance / Market
        "stock", "share", "market", "price", "invest", "trade", "fund", "etf", "bank",
        "economy", "economic", "rate", "inflation", "tax", "earnings", "revenue", "profit",
        "loss", "dividend", "yield", "ceo", "business", "company", "firm", "acquisition", "merger",
        "debt", "bond", "futures", "commodity", "oil", "gas", "gold", "crypto", "bitcoin", "percent",
        "growth", "sale", "retail", "consumer", "job", "employment", "wage", "gdp", "cpi", "fed",
        "central bank", "interest", "wealth", "asset", "capital", "equity", "investment", "portfolio",
        "wall street", "index", "dow", "nasdaq", "s&p", "ftse", "nikkei", "bull", "bear", "rally", "plunge", "soar",
        # Politics / Geopolitics / Conflict
        "policy", "government", "election", "vote", "voter", "campaign", "senate", "congress",
        "parliament", "minister", "president", "democrat", "republican",
        "court", "judge", "law", "bill", "act", "strike", "union",
        "tariff", "sanction", "trade war", "eu", "nato", "un",
        "war", "military", "defense", "defence", "border", "immigration",
        "conflict", "geopolitical", "diplomacy", "diplomat", "treaty", "alliance",
        "nuclear", "missile", "troops", "invasion", "ceasefire", "humanitarian",
        "crisis", "refugee", "terrorism", "insurgent", "security",
    }

    content_words = set(content.replace("-", " ").replace(".", " ").replace(",", " ").split())
    has_relevant = False
    for word in content_words:
        if word in relevance_keywords:
            has_relevant = True
            break

    if not has_relevant:
        for phrase in relevance_keywords:
            if " " in phrase and phrase in content:
                has_relevant = True
                break

    if not has_relevant:
        return True

    return False


async def fetch_feed(url: str, source: str, category: str, instrument_id: str | None = None, asset_name: str | None = None) -> list[dict]:
    """Fetch a single RSS feed and return parsed articles."""
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {
                "User-Agent": "Mozilla/5.0 (compatible; TradeSignal/1.0)",
                "Accept": "application/rss+xml, application/xml, text/xml",
            }
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
        link = entry.get("link", "")
        summary = entry.get("summary", entry.get("description", ""))

        if summary:
            # Strip HTML tags from summary
            summary = re.sub(r"<[^>]+>", " ", summary)
            summary = re.sub(r"\s+", " ", summary).strip()
            summary = summary[:2000]

        if is_spam(title, summary, link, category):
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
            "source": source,
            "category": category,
            "published_at": published_at,
            "instrument_id": instrument_id,
            "asset_name": asset_name,
        })

    # Scrape full article content in parallel for Ollama analysis
    if articles:
        import asyncio as _asyncio
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                tasks = []
                task_indices = []
                for i, a in enumerate(articles):
                    if a.get("link"):
                        tasks.append(fetch_article_content(a["link"], session))
                        task_indices.append(i)
                    else:
                        a["content"] = None

                contents = await _asyncio.gather(*tasks, return_exceptions=True)
                for idx, content in zip(task_indices, contents):
                    if isinstance(content, str) and content:
                        articles[idx]["content"] = content
                    else:
                        articles[idx]["content"] = None
        except Exception:
            logger.warning("Failed to scrape article content, continuing without it")
        for a in articles:
            a.setdefault("content", None)

    logger.info("Fetched %d articles from %s", len(articles), source)
    return articles
