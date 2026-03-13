"""Alpha Vantage NEWS_SENTIMENT historical sentiment fetcher.

Fetches per-ticker sentiment scores for weekly windows and caches results
in the backtest_av_cache DB table to avoid redundant API calls.

FREE TIER (25 req/day):
  - Run daily: docker compose run --rm backtester python -m app.main fetch-sentiment
  - Each run fetches up to 24 weeks (default --max-requests 24)
  - Progress is automatically resumed via DB cache on each run
  - 12 supported tickers × ~156 weeks (3y) = ~1,872 total calls → ~75 daily runs

PREMIUM TIER (75 req/min):
  - Set AV_REQUESTS_PER_MINUTE=75 in .env
  - Set --max-requests 9999 to fetch everything in one run

AV API endpoint:
  GET https://www.alphavantage.co/query
    ?function=NEWS_SENTIMENT
    &tickers=NVDA
    &time_from=20230101T0000
    &time_to=20230107T2359
    &limit=1000
    &sort=EARLIEST
    &apikey={key}

Returns ticker_sentiment_score ∈ [-1, 1] per article per ticker.
We map to [-3, 3] via score * 3.0 to match the grading system's scale.
"""

import asyncio
import logging
import math
from datetime import date, timedelta

import httpx
from sqlalchemy import text

from .config import AV_API_KEY, AV_REQUESTS_PER_MINUTE
from .db import async_session

logger = logging.getLogger(__name__)

AV_BASE = "https://www.alphavantage.co/query"

# Map our symbols to AV ticker symbols.
# Commodities (GOLD, OIL) and UK-listed ETFs (IITU) are not supported by AV.
# They will receive 0.0 sentiment (technical signal dominates for commodities anyway).
AV_SYMBOL_MAP: dict[str, str | None] = {
    "RTX":   "RTX",
    "NVDA":  "NVDA",
    "GOOGL": "GOOGL",
    "AAPL":  "AAPL",
    "TSLA":  "TSLA",
    "PLTR":  "PLTR",
    "LLY":   "LLY",
    "NVO":   "NVO",
    "WMT":   "WMT",
    "XOM":   "XOM",
    "SMH":   "SMH",
    "VOO":   "VOO",
    "GOLD":  None,   # GC=F futures — not supported by AV
    "OIL":   None,   # CL=F futures — not supported by AV
    "IITU":  None,   # UK-listed ETF (IITU.L) — not supported by AV
}


async def _fetch_av_week(
    av_ticker: str,
    week_start: date,
    client: httpx.AsyncClient,
) -> tuple[float | None, int]:
    """Fetch AV NEWS_SENTIMENT for one ticker for one calendar week.

    Returns (avg_ticker_sentiment_score ∈ [-1, 1] or None on error, article_count).
    """
    week_end = week_start + timedelta(days=6)
    time_from = week_start.strftime("%Y%m%dT0000")
    time_to   = week_end.strftime("%Y%m%dT2359")

    params = {
        "function": "NEWS_SENTIMENT",
        "tickers":  av_ticker,
        "time_from": time_from,
        "time_to":   time_to,
        "limit":     "1000",
        "sort":      "EARLIEST",
        "apikey":    AV_API_KEY,
    }

    try:
        resp = await client.get(AV_BASE, params=params, timeout=30.0)

        if resp.status_code == 429:
            logger.warning("[AV:%s] Rate limited (429) — week %s", av_ticker, week_start)
            return None, 0

        if resp.status_code != 200:
            logger.warning("[AV:%s] HTTP %d — week %s", av_ticker, resp.status_code, week_start)
            return None, 0

        data = resp.json()

        # AV sends limit/rate error messages in the body
        if "Information" in data:
            logger.warning("[AV:%s] API limit hit: %s", av_ticker, data["Information"][:120])
            return None, 0
        if "Note" in data:
            logger.warning("[AV:%s] API note: %s", av_ticker, data["Note"][:120])
            return None, 0

        feed = data.get("feed", [])
        if not feed:
            # No articles this week — valid empty response, cache as 0.0
            return 0.0, 0

        scores = []
        for article in feed:
            for ts in article.get("ticker_sentiment", []):
                if ts.get("ticker", "").upper() == av_ticker.upper():
                    try:
                        score = float(ts["ticker_sentiment_score"])
                        relevance = float(ts.get("relevance_score", "0.5"))
                        if relevance >= 0.50:   # filter noise: "sell X, buy NVDA instead" articles have ~0.6 relevance but aren't really about the ticker
                            scores.append(score)
                    except (ValueError, KeyError):
                        continue

        if not scores:
            return 0.0, len(feed)   # articles existed but ticker not sufficiently relevant

        avg = sum(scores) / len(scores)
        return avg, len(scores)

    except httpx.TimeoutException:
        logger.warning("[AV:%s] Timeout — week %s", av_ticker, week_start)
        return None, 0
    except Exception:
        logger.exception("[AV:%s] Unexpected error — week %s", av_ticker, week_start)
        return None, 0


async def _cache_lookup(symbol: str, week_start: date) -> tuple[float | None, int | None]:
    """Return (avg_score, article_count) from cache, or (None, None) if not cached."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT avg_score, article_count FROM backtest_av_cache WHERE symbol = :s AND week_start = :w"),
            {"s": symbol, "w": week_start},
        )
        row = result.fetchone()
    if row:
        return (float(row.avg_score) if row.avg_score is not None else 0.0), (row.article_count or 0)
    return None, None


async def _cache_store(symbol: str, week_start: date, avg_score: float, article_count: int) -> None:
    """Upsert result into backtest_av_cache."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO backtest_av_cache (symbol, week_start, avg_score, article_count)
                VALUES (:s, :w, :score, :count)
                ON CONFLICT (symbol, week_start) DO UPDATE
                  SET avg_score = EXCLUDED.avg_score,
                      article_count = EXCLUDED.article_count,
                      fetched_at = NOW()
            """),
            {"s": symbol, "w": week_start, "score": avg_score, "count": article_count},
        )
        await session.commit()


def _week_start(d: date) -> date:
    """Return the Monday of the week containing d."""
    return d - timedelta(days=d.weekday())


def _build_week_list(start_date: date, end_date: date) -> list[date]:
    """Return weekly Monday dates from start_date to end_date in DESCENDING order.

    Most-recent weeks first: if we hit the daily API limit, the most
    valuable (recent) data is fetched before older history.
    """
    weeks: list[date] = []
    current = _week_start(start_date)
    while current <= end_date:
        weeks.append(current)
        current += timedelta(weeks=1)
    return list(reversed(weeks))   # most recent first


async def fetch_all_sentiment_history(
    symbols: list[str],
    start_date: date,
    end_date: date,
    max_requests: int = 24,
) -> dict[str, dict[date, float]]:
    """Fetch historical weekly sentiment for all symbols.

    FREE TIER SAFE: respects max_requests (default 24 to stay under 25/day limit).
    Progress is automatically resumed via DB cache — already-cached weeks are skipped
    and don't count toward max_requests.

    Returns dict: {symbol: {week_start_date: score_on_[-3,3]_scale}}.
    Symbols with no AV mapping (GOLD, OIL, IITU) return empty dicts.
    """
    if not AV_API_KEY:
        logger.warning("[AV] AV_API_KEY not set — returning empty sentiment for all symbols")
        return {sym: {} for sym in symbols}

    weeks = _build_week_list(start_date, end_date)
    # Seconds between requests: 60 / RPM, minimum 12s on free tier
    delay = max(60.0 / max(AV_REQUESTS_PER_MINUTE, 1), 1.0)

    results: dict[str, dict[date, float]] = {sym: {} for sym in symbols}
    requests_made = 0

    # Pre-load entire cache for these symbols to avoid per-week DB roundtrips
    cached_data: dict[tuple[str, date], float] = {}
    fetchable_syms = [s for s in symbols if AV_SYMBOL_MAP.get(s) is not None]
    if fetchable_syms:
        async with async_session() as session:
            result = await session.execute(
                text("""
                    SELECT symbol, week_start, avg_score
                    FROM backtest_av_cache
                    WHERE symbol = ANY(:syms)
                """),
                {"syms": fetchable_syms},
            )
            for row in result.fetchall():
                cached_data[(row.symbol, row.week_start)] = float(row.avg_score) if row.avg_score is not None else 0.0

    logger.info("[AV] Pre-loaded %d cached entries from DB", len(cached_data))

    # Populate results from cache immediately (no API call needed)
    for sym in symbols:
        av_ticker = AV_SYMBOL_MAP.get(sym)
        if av_ticker is None:
            continue
        for week in weeks:
            cache_key = (sym, week)
            if cache_key in cached_data:
                results[sym][week] = round(cached_data[cache_key] * 3.0, 4)

    # Count how many weeks still need fetching per symbol
    total_missing = sum(
        sum(1 for w in weeks if (sym, w) not in cached_data)
        for sym in symbols
        if AV_SYMBOL_MAP.get(sym) is not None
    )
    logger.info("[AV] %d cached, %d still need fetching (budget: %d requests this run)",
                len(cached_data), total_missing, max_requests)

    if total_missing == 0:
        logger.info("[AV] All weeks already cached — nothing to fetch!")
        return results

    # Fetch missing weeks — interleave across symbols so no single ticker
    # exhausts the whole daily budget (e.g. NVDA first 24 weeks, then nothing else).
    # Strategy: round-robin across symbols, most recent week first for each.
    symbol_queues: dict[str, list[date]] = {}
    for sym in symbols:
        av_ticker = AV_SYMBOL_MAP.get(sym)
        if av_ticker is None:
            continue
        missing = [w for w in weeks if (sym, w) not in cached_data]
        if missing:
            symbol_queues[sym] = missing

    if not symbol_queues:
        return results

    async with httpx.AsyncClient() as client:
        # Round-robin: take one week from each symbol in turn
        while symbol_queues and requests_made < max_requests:
            for sym in list(symbol_queues.keys()):
                if requests_made >= max_requests:
                    break

                queue = symbol_queues[sym]
                if not queue:
                    del symbol_queues[sym]
                    continue

                week = queue.pop(0)
                av_ticker = AV_SYMBOL_MAP[sym]

                avg_score, article_count = await _fetch_av_week(av_ticker, week, client)
                requests_made += 1

                store_score = avg_score if avg_score is not None else 0.0
                await _cache_store(sym, week, store_score, article_count)

                if avg_score is not None:
                    results[sym][week] = round(avg_score * 3.0, 4)

                remaining_budget = max_requests - requests_made
                remaining_total = sum(len(q) for q in symbol_queues.values())
                logger.info(
                    "[AV] %s week %s → score=%.3f articles=%d | %d/%d requests used, %d in budget, ~%d weeks left total",
                    sym, week, store_score, article_count,
                    requests_made, max_requests, remaining_budget, remaining_total,
                )

                if not queue:
                    del symbol_queues[sym]

                # Rate limit delay between requests
                if requests_made < max_requests and symbol_queues:
                    await asyncio.sleep(delay)

    # Summary
    remaining_after = sum(len(q) for q in symbol_queues.values()) if symbol_queues else 0
    if remaining_after > 0:
        days_left = math.ceil(remaining_after / max_requests)
        logger.info(
            "[AV] Run complete: %d requests used. ~%d weeks still uncached. "
            "Run again tomorrow (~%d more daily runs needed).",
            requests_made, remaining_after, days_left,
        )
    else:
        logger.info("[AV] All sentiment history fetched and cached! (%d requests used)", requests_made)

    return results


def get_sentiment_score_for_date(
    symbol: str,
    target_date: date,
    sentiment_history: dict[str, dict[date, float]],
    lookback_weeks: int = 4,
) -> tuple[float, float]:
    """Get sentiment score for a specific backtest date using cached weekly history.

    Applies exponential time-decay (half-life = 1 week) over lookback_weeks.
    Mirrors the production sentiment scorer's decay logic.

    Returns (score ∈ [-3, 3], confidence ∈ [0, 1]).
    """
    sym_history = sentiment_history.get(symbol, {})
    if not sym_history:
        return 0.0, 0.0

    relevant = []
    for week_start, score in sym_history.items():
        week_end = week_start + timedelta(days=6)
        if week_end <= target_date:
            age_weeks = (target_date - week_end).days / 7.0
            if age_weeks <= lookback_weeks:
                relevant.append((age_weeks, score))

    if not relevant:
        return 0.0, 0.0

    # Exponential decay: half-life = 1 week
    decay_lambda = math.log(2) / 1.0
    weighted_sum = 0.0
    weight_total = 0.0
    for age_weeks, score in relevant:
        w = math.exp(-decay_lambda * age_weeks)
        weighted_sum += score * w
        weight_total += w

    if weight_total == 0:
        return 0.0, 0.0

    mean_score = weighted_sum / weight_total
    confidence = min(1.0, len(relevant) / 4.0)
    effective = max(-3.0, min(3.0, mean_score * confidence))
    return round(effective, 4), round(confidence, 4)
