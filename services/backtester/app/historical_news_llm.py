"""Historical news sentiment via Google News RSS + NIM/Qwen LLM.

Fetches date-range-filtered Google News articles for each instrument,
runs them through the same NIM sentiment pipeline as production, and
stores weekly average scores in backtest_av_cache.

This replaces Alpha Vantage for backtesting — covers all instruments
(including GOLD, OIL, IITU) and runs indefinitely without API limits.
"""

import asyncio
import json
import logging
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import httpx
from sqlalchemy import text

from .config import NIM_API_KEY, NIM_BASE_URL, NIM_MODEL
from .db import async_session

logger = logging.getLogger(__name__)

# Sentinel for "no data available" — different from 0.0 which means neutral
_NO_DATA = None

# Label → numeric score (same scale as AV; multiplied ×3 at use time → [-3, 3])
LABEL_SCORES = {
    "very_positive": 1.0,
    "positive": 0.5,
    "neutral": 0.0,
    "negative": -0.5,
    "very_negative": -1.0,
}

# Per-instrument search queries (special handling for commodities + ETFs)
SEARCH_QUERIES = {
    "GOLD": "gold price futures OR bullion OR safe haven",
    "OIL":  '"crude oil" OR WTI OR OPEC OR "oil price"',
    "IITU": "iShares US Technology ETF OR IITU",
    "SMH":  "VanEck Semiconductor ETF OR SMH semiconductor",
    "VOO":  "Vanguard S&P 500 ETF OR VOO",
}

# Category-specific role prompts (mirrors production prompts.py)
_CATEGORY_ROLES = {
    "stock":     "You are a Wall Street equity analyst. You think like both a day-trader (short-term price action) and a fundamental investor (long-term value).",
    "etf":       "You are a Wall Street ETF analyst. You understand constituent-level impacts propagate to the ETF with weight-proportional magnitude.",
    "commodity": "You are a commodity futures trader. You ONLY care about the PRICE of this commodity going UP or DOWN. Supply disruptions = PRICE UP = positive. Demand destruction = PRICE DOWN = negative.",
}

_CATEGORY_DESCS = {
    "stock":     "{name} stock price on US exchanges",
    "etf":       "{name} ETF price on US exchanges",
    "commodity": "{name} futures price",
}

SENTIMENT_SYSTEM = (
    "You predict PRICE DIRECTION of a specific asset. You ONLY care about the PRICE going UP or DOWN. "
    "IMPORTANT: For commodities (OIL, GOLD), wars and supply disruptions make the PRICE GO UP — that is POSITIVE. "
    "Always respond with valid JSON only. No explanations."
)


def _week_start(d: date) -> date:
    """Return Monday of the week containing d."""
    return d - timedelta(days=d.weekday())


def _google_news_url(query: str, after: date, before: date) -> str:
    q = f"{query} after:{after.isoformat()} before:{before.isoformat()}"
    params = urllib.parse.urlencode({"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"})
    return f"https://news.google.com/rss/search?{params}"


async def _fetch_rss_articles(
    client: httpx.AsyncClient,
    query: str,
    week: date,
) -> list[dict]:
    """Fetch Google News RSS articles for a query in the given week window."""
    after = week
    before = week + timedelta(days=7)
    url = _google_news_url(query, after, before)

    try:
        resp = await client.get(url, timeout=20.0)
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("RSS fetch failed for %r: %s", query, exc)
        return []

    articles = []
    try:
        root = ET.fromstring(resp.text)
        channel = root.find("channel")
        if channel is None:
            return []
        for item in channel.findall("item"):
            title = (item.findtext("title") or "").strip()
            desc  = (item.findtext("description") or "").strip()
            # Strip HTML tags from description
            desc = re.sub(r"<[^>]+>", " ", desc).strip()
            if title:
                articles.append({"title": title, "content": desc[:300]})
    except ET.ParseError as exc:
        logger.debug("XML parse error: %s", exc)

    return articles[:20]  # cap per week to avoid huge batches


def _build_sentiment_prompt(
    articles: list[dict],
    instrument: dict,
) -> str:
    """Build batch sentiment prompt (short-term only for backtest)."""
    category = instrument.get("category", "stock")
    name = instrument["name"].replace("Futures", "").strip()
    role = _CATEGORY_ROLES.get(category, _CATEGORY_ROLES["stock"])
    asset_desc = _CATEGORY_DESCS.get(category, _CATEGORY_DESCS["stock"]).format(name=name)

    articles_text = ""
    for i, art in enumerate(articles):
        text = art["title"]
        if art.get("content"):
            text += ". " + art["content"][:200]
        articles_text += f'\n{i + 1}. "{text[:350]}"\n'

    return f"""{role}

Predict whether each article pushes the PRICE of {asset_desc} UP or DOWN.

MANDATORY RULES — follow these exactly:
- "overweight", "buy", "upgrade", "price target raised" → positive or very_positive
- "underweight", "sell", "downgrade", "price target cut" → negative or very_negative
- "Fund buys/sells X shares" (institutional rebalancing) → neutral, confidence 0.1-0.2
- OIL futures: war, supply disruption, sanctions, OPEC cuts → positive (price goes UP). Demand destruction, recession, supply increase → negative
- GOLD futures: war, crisis, fear, rate cuts → positive (safe haven). Rate hikes, risk-on → negative
- Defense stocks (RTX): wars, military spending → positive
- Article not about this asset → neutral, confidence 0.1-0.3
- Article directly about this asset → be decisive, avoid neutral

Labels: very_positive, positive, neutral, negative, very_negative

Articles:{articles_text}
Respond ONLY with a JSON array of objects with index "i" (1-based) and sentiment "s".
Example: [{{"i": 1, "s": "positive"}}, {{"i": 2, "s": "neutral"}}]
Include ALL {len(articles)} articles."""


async def _call_nim_sentiment(
    client: httpx.AsyncClient,
    articles: list[dict],
    instrument: dict,
) -> float | None:
    """Call NIM API and return average sentiment score for the articles batch.

    Returns a value in [-1, 1], or None if the call fails or returns no usable data.
    """
    if not articles:
        return None

    prompt = _build_sentiment_prompt(articles, instrument)
    payload = {
        "model": NIM_MODEL,
        "messages": [
            {"role": "system", "content": SENTIMENT_SYSTEM},
            {"role": "user",   "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 512,
    }

    try:
        resp = await client.post(
            f"{NIM_BASE_URL}/chat/completions",
            json=payload,
            headers={"Authorization": f"Bearer {NIM_API_KEY}"},
            timeout=30.0,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        logger.warning("NIM API error: %s", exc)
        return None

    # Parse JSON array from response
    try:
        # Find JSON array in response (model may add prose)
        match = re.search(r"\[.*\]", content, re.DOTALL)
        if not match:
            return None
        results = json.loads(match.group())
    except (json.JSONDecodeError, ValueError):
        logger.debug("JSON parse failed for NIM response: %r", content[:200])
        return None

    scores = []
    for item in results:
        label = item.get("s", "neutral")
        score = LABEL_SCORES.get(label, 0.0)
        scores.append(score)

    if not scores:
        return None

    return sum(scores) / len(scores)


async def _load_cached_weeks(symbols: list[str]) -> dict[str, set[date]]:
    """Pre-load existing cache entries to avoid redundant NIM calls."""
    if not symbols:
        return {}
    async with async_session() as session:
        result = await session.execute(
            text("SELECT symbol, week_start FROM backtest_av_cache WHERE symbol = ANY(:syms)"),
            {"syms": symbols},
        )
        cached: dict[str, set[date]] = {s: set() for s in symbols}
        for row in result.fetchall():
            cached[row.symbol].add(row.week_start)
    return cached


async def _store_week_cache(symbol: str, week: date, avg_score: float | None, article_count: int) -> None:
    """Upsert a week's sentiment into backtest_av_cache."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO backtest_av_cache (symbol, week_start, avg_score, article_count)
                VALUES (:sym, :week, :score, :cnt)
                ON CONFLICT (symbol, week_start) DO UPDATE
                  SET avg_score = EXCLUDED.avg_score,
                      article_count = EXCLUDED.article_count
            """),
            {"sym": symbol, "week": week, "score": avg_score, "cnt": article_count},
        )
        await session.commit()


async def fetch_llm_sentiment_history(
    instruments: list[dict],
    start_date: date,
    end_date: date,
    concurrency: int = 3,
) -> dict[str, dict[date, float]]:
    """Fetch Google News + NIM sentiment for all instruments over the date range.

    Stores results in backtest_av_cache (same schema as AV version).
    Returns {symbol: {week_start: avg_score}} for all weeks processed.
    """
    symbols = [i["symbol"] for i in instruments]
    cached_weeks = await _load_cached_weeks(symbols)

    # Build list of (instrument, week) work items — skip already cached
    all_weeks: list[date] = []
    current = _week_start(start_date)
    while current <= end_date:
        all_weeks.append(current)
        current += timedelta(weeks=1)

    work_items: list[tuple[dict, date]] = []
    for inst in instruments:
        sym = inst["symbol"]
        for week in reversed(all_weeks):  # most recent first
            if week not in cached_weeks.get(sym, set()):
                work_items.append((inst, week))

    total = len(work_items)
    logger.info("LLM sentiment: %d instrument-weeks to process (%d already cached)", total, sum(len(v) for v in cached_weeks.values()))

    if not work_items:
        logger.info("All weeks already cached.")
    else:
        sem = asyncio.Semaphore(concurrency)

        async def process_one(inst: dict, week: date) -> None:
            sym = inst["symbol"]
            query = SEARCH_QUERIES.get(sym, f"{sym} stock")
            async with sem:
                async with httpx.AsyncClient(
                    headers={"User-Agent": "Mozilla/5.0 (compatible; TradeSignal/1.0)"},
                    follow_redirects=True,
                ) as rss_client:
                    articles = await _fetch_rss_articles(rss_client, query, week)

                if not articles:
                    await _store_week_cache(sym, week, None, 0)
                    return

                async with httpx.AsyncClient() as nim_client:
                    avg_score = await _call_nim_sentiment(nim_client, articles, inst)

                await _store_week_cache(sym, week, avg_score, len(articles))
                logger.debug("[%s] week=%s articles=%d score=%s", sym, week, len(articles), avg_score)

        tasks = [process_one(inst, week) for inst, week in work_items]
        done = 0
        for coro in asyncio.as_completed(tasks):
            await coro
            done += 1
            if done % 50 == 0:
                logger.info("LLM sentiment progress: %d / %d", done, total)

    # Load full cache and return
    return await _load_sentiment_history(symbols)


async def _load_sentiment_history(symbols: list[str]) -> dict[str, dict[date, float]]:
    """Load all cached sentiment scores from DB."""
    if not symbols:
        return {}
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT symbol, week_start, avg_score
                FROM backtest_av_cache
                WHERE symbol = ANY(:syms) AND avg_score IS NOT NULL
                ORDER BY week_start ASC
            """),
            {"syms": symbols},
        )
        history: dict[str, dict[date, float]] = {s: {} for s in symbols}
        for row in result.fetchall():
            history[row.symbol][row.week_start] = float(row.avg_score)
    return history


def get_sentiment_score_for_date(
    symbol: str,
    target_date: date,
    sentiment_history: dict[str, dict[date, float]],
    lookback_weeks: int = 4,
) -> tuple[float, float]:
    """Compute exponentially-decayed sentiment score for a given date.

    Returns (score_in_[-3,3], confidence_in_[0,1]).
    Identical to the AV version — score ×3 conversion happens here.
    """
    import math

    symbol_data = sentiment_history.get(symbol, {})
    if not symbol_data:
        return 0.0, 0.0

    target_week = _week_start(target_date)
    half_life = 1.0  # weeks

    weighted_sum = 0.0
    weight_total = 0.0
    n_weeks = 0

    for w in range(lookback_weeks):
        wk = target_week - timedelta(weeks=w)
        score = symbol_data.get(wk)
        if score is None:
            continue
        decay = math.exp(-math.log(2) * w / half_life)
        weighted_sum += score * decay
        weight_total += decay
        n_weeks += 1

    if weight_total == 0:
        return 0.0, 0.0

    raw = weighted_sum / weight_total          # in [-1, 1]
    scaled = raw * 3.0                          # convert to [-3, 3]
    confidence = min(1.0, n_weeks / lookback_weeks)

    return round(scaled, 4), round(confidence, 4)
