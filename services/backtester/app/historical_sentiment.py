"""Daily historical news sentiment for backtesting — production-faithful.

Fetches Google News RSS with date operators for three signal types:
  - asset:  per-instrument news (15 instruments)
  - macro:  global macro/market news (→ macro_sentiment table in production)
  - sector: GICS sector news (→ sector_sentiment table in production)

Runs dual-horizon NIM LLM sentiment (same Qwen 122B model as production),
stores daily aggregates in backtest_sentiment_cache.

Scoring functions mirror production scorer.py exactly:
  - exponential time-decay with term-specific half-lives
  - consensus dampening (contrarian + priced-in detection)
  - logarithmic confidence ramp
  - macro/sector: raw_mean × 3.0 to match [-3, 3] scale (same as production scorer.py line 565)
"""

import asyncio
import json
import logging
import math
import random
import re
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import httpx
from sqlalchemy import text

from .config import NIM_API_KEY, NIM_BASE_URL, NIM_MODEL
from .db import async_session

logger = logging.getLogger(__name__)

# ─── Score mapping: labels → [-1, 1] (×3 at use time = [-3, 3]) ──────────────
# Production macro/sector pipeline uses SENTIMENT_MULTIPLIERS = {1.0, 0.5, 0, -0.5, -1.0}
# Asset pipeline uses SENTIMENT_SCORES = {3.0, 1.5, 0, -1.5, -3.0} = same ×3
LABEL_SCORES: dict[str, float] = {
    "very_positive": 1.0,
    "positive":      0.5,
    "neutral":       0.0,
    "negative":     -0.5,
    "very_negative": -1.0,
}
VALID_LABELS = set(LABEL_SCORES.keys())

# ─── Exact copies of production scorer.py constants ──────────────────────────
SENTIMENT_PARAMS: dict[str, dict] = {
    "short": {"half_life_hours": 12.0,  "window_days": 2,  "full_confidence_at": 20},
    "long":  {"half_life_hours": 168.0, "window_days": 30, "full_confidence_at": 40},
}
MACRO_PARAMS: dict[str, dict] = {
    "short": {"half_life_hours": 24.0,   "window_hours": 72},
    "long":  {"half_life_hours": 648.0,  "window_hours": 4320},
}
SECTOR_PARAMS: dict[str, dict] = {
    "short": {"half_life_hours": 36.0,  "window_hours": 120},
    "long":  {"half_life_hours": 240.0, "window_hours": 720},
}

# ─── Google News query definitions ───────────────────────────────────────────
# Per-instrument — mirrors production news-fetcher per-asset queries
ASSET_QUERIES: dict[str, str] = {
    "RTX":   "RTX Corporation Raytheon defense aerospace stock",
    "NVDA":  "NVIDIA stock GPU AI chips semiconductor earnings",
    "GOOGL": "Alphabet Google stock search advertising earnings",
    "AAPL":  "Apple stock iPhone earnings technology",
    "TSLA":  "Tesla stock EV electric vehicle earnings revenue",
    "PLTR":  "Palantir stock AI defense government software earnings",
    "LLY":   "Eli Lilly stock pharma drug GLP-1 earnings",
    "NVO":   "Novo Nordisk stock diabetes obesity Ozempic drug",
    "WMT":   "Walmart stock retail earnings consumer spending",
    "XOM":   "Exxon Mobil stock oil energy earnings upstream",
    "IITU":  "iShares US technology ETF tech sector performance",
    "SMH":   "VanEck semiconductor ETF chips sector performance",
    "VOO":   "S&P 500 ETF Vanguard stock market index performance",
    "GOLD":  "gold price futures bullion safe haven precious metals",
    "OIL":   "crude oil price WTI brent OPEC futures barrel energy",
}

# Macro — mirrors production MAIN_FEEDS macro categories
# Two queries per day to cover market + geopolitical macro
MACRO_QUERIES: list[str] = [
    "Federal Reserve interest rates monetary policy inflation central bank",
    "global economy recession GDP growth trade war tariffs geopolitics",
]

# Sector — mirrors production SECTOR_FEEDS Google News queries
SECTOR_QUERIES: dict[str, str] = {
    "technology":             "technology sector semiconductor industry stocks",
    "financials":             "financial sector banking industry wall street",
    "healthcare":             "healthcare sector biotech pharma drug industry",
    "consumer_discretionary": "consumer discretionary retail automotive sector",
    "consumer_staples":       "consumer staples food beverage household industry",
    "communication":          "communication services media telecom streaming sector",
    "energy":                 "energy sector oil gas renewable industry stocks",
    "industrials":            "industrials sector manufacturing defense aerospace",
    "materials":              "materials sector mining metals chemicals industry",
    "utilities":              "utilities sector electricity grid power water",
    "real_estate":            "real estate sector REIT commercial property housing",
}

# ─── LLM prompt builders (mirrors production llm-processor/prompts.py) ───────
_DUAL_HORIZON_RULES = """Rules (apply strictly):
1. Analyst upgrade/overweight/buy ratings: always positive (both short AND long term)
2. Analyst downgrade/underweight/sell ratings: always negative
3. Small institutional fund buy/sell: neutral (noise, not market-moving)
4. Commodity supply disruptions or OPEC cuts: positive for commodity PRICE (supply down = price up)
5. Safe-haven assets (gold, bonds): positive during market crises and geopolitical risk
6. Defense stocks (RTX): positive during wars and conflicts
7. Recession/GDP slowdown: negative for stocks, but check if already priced in
8. Earnings beats: very_positive short-term; consider long-term sustainability"""


def _asset_role(symbol: str, category: str) -> str:
    roles = {
        "stock":     f"You are a Wall Street equity analyst covering {symbol}. "
                     f"Evaluate news for its PRICE impact on {symbol} shares.",
        "etf":       f"You are a Wall Street ETF analyst covering {symbol}. "
                     f"Constituent-level impacts propagate with weight-proportional magnitude.",
        "commodity": "You are a commodity futures trader. "
                     "Supply disruptions = PRICE UP = positive. "
                     "Demand destruction = PRICE DOWN = negative. "
                     "Bad global economy ≠ bad commodity price if supply is disrupted.",
    }
    return roles.get(category, roles["stock"])


_MACRO_ROLE = (
    "You are a macro economist at a major investment bank. "
    "Evaluate news for its impact on the S&P 500 stock market index overall."
)


def _sector_role(sector: str) -> str:
    return (
        f"You are a {sector} sector analyst at a Western investment bank. "
        f"Evaluate news for its impact on the entire {sector} sector — "
        f"regulatory changes, industry trends, and supply chain shifts affecting sector ETFs."
    )


def _build_prompt(role: str, articles: list[dict], subject_desc: str) -> str:
    lines = [
        role,
        "",
        f"For each article about {subject_desc}, provide sentiment for TWO time horizons:",
        '  "ss": short-term (1-7 days): very_positive | positive | neutral | negative | very_negative',
        '  "ls": long-term (1-6 months): very_positive | positive | neutral | negative | very_negative',
        "",
        _DUAL_HORIZON_RULES,
        "",
        "Articles:",
    ]
    for art in articles:
        lines.append(f'[{art["i"]}] {art["title"]}')
        if art.get("summary"):
            lines.append(f'    {art["summary"][:180]}')
    lines += [
        "",
        "Respond with ONLY a compact JSON array — no markdown, no explanation:",
        '[{"i":1,"ss":"positive","ls":"neutral"},{"i":2,"ss":"negative","ls":"negative"}]',
    ]
    return "\n".join(lines)


# ─── NIM API ──────────────────────────────────────────────────────────────────
_NIM_BATCH = 8  # articles per NIM call (same as production SENTIMENT_BATCH)
_NIM_DELAY = 1.5  # seconds between NIM calls


async def _call_nim(client: httpx.AsyncClient, prompt: str, retries: int = 3) -> list[dict]:
    """POST to NIM /chat/completions, return parsed [{i, ss, ls}] list."""
    for attempt in range(retries):
        try:
            resp = await client.post(
                f"{NIM_BASE_URL}/chat/completions",
                headers={
                    "Authorization": f"Bearer {NIM_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": NIM_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,
                    "max_tokens": 512,
                },
                timeout=45.0,
            )
            if resp.status_code == 429:
                wait = 60.0 + random.uniform(0, 20)
                logger.warning("NIM rate limited (429), waiting %.0fs...", wait)
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            match = re.search(r"\[.*\]", content, re.DOTALL)
            if not match:
                logger.debug("No JSON array in NIM response: %s", content[:120])
                return []
            return json.loads(match.group())
        except json.JSONDecodeError as e:
            logger.debug("NIM JSON parse error attempt %d: %s", attempt + 1, e)
        except httpx.HTTPStatusError as e:
            logger.warning("NIM HTTP error attempt %d: %s", attempt + 1, e)
        except httpx.RequestError as e:
            logger.warning("NIM request error attempt %d: %s", attempt + 1, e)
        if attempt < retries - 1:
            await asyncio.sleep(2.0 ** attempt)
    return []


async def _score_articles(
    client: httpx.AsyncClient,
    articles: list[dict],
    role: str,
    subject_desc: str,
) -> list[tuple[float, float, str, str]]:
    """Run articles through NIM dual-horizon LLM.

    Returns list of (short_score, long_score, ss_label, ls_label).
    """
    if not articles:
        return []

    # Deduplicate by title prefix
    seen: set[str] = set()
    unique: list[dict] = []
    for art in articles:
        key = art["title"].lower()[:80]
        if key not in seen:
            seen.add(key)
            unique.append(art)

    results: list[tuple[float, float, str, str]] = []
    for chunk_start in range(0, len(unique), _NIM_BATCH):
        chunk = unique[chunk_start : chunk_start + _NIM_BATCH]
        numbered = [
            {"i": i + 1, "title": a["title"], "summary": a.get("summary", "")}
            for i, a in enumerate(chunk)
        ]
        prompt = _build_prompt(role, numbered, subject_desc)
        responses = await _call_nim(client, prompt)
        scored = {r["i"]: r for r in responses if isinstance(r, dict) and "ss" in r}

        for i, _ in enumerate(chunk):
            sr = scored.get(i + 1, {})
            ss = sr.get("ss", "neutral")
            ls = sr.get("ls", "neutral")
            if ss not in VALID_LABELS:
                ss = "neutral"
            if ls not in VALID_LABELS:
                ls = "neutral"
            results.append((LABEL_SCORES[ss], LABEL_SCORES[ls], ss, ls))

        await asyncio.sleep(_NIM_DELAY)

    return results


# ─── RSS fetch ────────────────────────────────────────────────────────────────
def _google_rss_url(query: str, day: date) -> str:
    """Google News RSS with narrow ±1-day window around target date."""
    date_from = (day - timedelta(days=1)).isoformat()
    date_to = (day + timedelta(days=1)).isoformat()
    q = query.replace(" ", "+").replace('"', "%22")
    return (
        f"https://news.google.com/rss/search"
        f"?q={q}+after:{date_from}+before:{date_to}"
        f"&hl=en-US&gl=US&ceid=US:en"
    )


async def _fetch_rss(client: httpx.AsyncClient, url: str, max_articles: int = 20) -> list[dict]:
    """Fetch Google News RSS, return list of {title, summary} dicts."""
    try:
        resp = await client.get(url, timeout=15.0, follow_redirects=True)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        articles = []
        for item in root.findall(".//item"):
            title_el = item.find("title")
            desc_el = item.find("description")
            title = (title_el.text or "").strip()
            summary = re.sub(r"<[^>]+>", "", (desc_el.text or "")).strip()[:300]
            if len(title) >= 15:
                articles.append({"title": title, "summary": summary})
            if len(articles) >= max_articles:
                break
        return articles
    except Exception as e:
        logger.debug("RSS fetch failed for %s: %s", url[:80], e)
        return []


# ─── DB helpers ──────────────────────────────────────────────────────────────
async def _load_cached_keys(type_: str) -> set[tuple[str, date]]:
    """Return set of (key, date) already stored for this type."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT key, date FROM backtest_sentiment_cache WHERE type = :t"),
            {"t": type_},
        )
        return {(r.key, r.date) for r in result.fetchall()}


async def _upsert_daily(
    type_: str,
    key: str,
    d: date,
    scored: list[tuple[float, float, str, str]],
) -> None:
    """Aggregate scored articles and upsert one row per (type, key, date)."""
    if not scored:
        # Store empty day so we don't re-fetch it
        short_s = long_s = 0.0
        n = nn = pos = neg = 0
    else:
        n = len(scored)
        short_s = round(sum(s for s, l, ss, ls in scored) / n, 4)
        long_s = round(sum(l for s, l, ss, ls in scored) / n, 4)
        nn = sum(1 for s, l, ss, ls in scored if ss != "neutral")
        pos = sum(1 for s, l, ss, ls in scored if "positive" in ss)
        neg = sum(1 for s, l, ss, ls in scored if "negative" in ss)

    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO backtest_sentiment_cache
                    (type, key, date, short_score, long_score,
                     article_count, non_neutral_count, positive_count, negative_count)
                VALUES (:type, :key, :date, :ss, :ls, :n, :nn, :pos, :neg)
                ON CONFLICT (type, key, date) DO UPDATE
                    SET short_score      = EXCLUDED.short_score,
                        long_score       = EXCLUDED.long_score,
                        article_count    = EXCLUDED.article_count,
                        non_neutral_count = EXCLUDED.non_neutral_count,
                        positive_count   = EXCLUDED.positive_count,
                        negative_count   = EXCLUDED.negative_count
            """),
            {
                "type": type_, "key": key, "date": d,
                "ss": short_s, "ls": long_s,
                "n": n, "nn": nn, "pos": pos, "neg": neg,
            },
        )
        await session.commit()


# ─── Main fetch orchestrator ──────────────────────────────────────────────────
def _all_weekdays(start: date, end: date) -> list[date]:
    """All weekdays (Mon–Fri) between start and end inclusive."""
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


async def fetch_all_historical_sentiment(
    instruments: list[dict],
    start_date: date,
    end_date: date,
    concurrency: int = 3,
) -> None:
    """Fetch and store daily sentiment for all assets, macro, and 11 sectors.

    Idempotent — skips (type, key, date) already in backtest_sentiment_cache.
    Can be run multiple times safely to resume after interruption.

    Data volume: ~800 trading days × (15 asset + 1 macro + 11 sector) = ~21,600 items.
    Estimated time: 4–6 hours at concurrency=3 (first run), resumable.
    """
    trading_days = _all_weekdays(start_date, end_date)
    logger.info(
        "Sentiment fetch: %d trading days, %s → %s",
        len(trading_days), start_date, end_date,
    )

    logger.info("Loading existing cache keys...")
    existing_asset  = await _load_cached_keys("asset")
    existing_macro  = await _load_cached_keys("macro")
    existing_sector = await _load_cached_keys("sector")
    logger.info(
        "Already cached: %d asset, %d macro, %d sector",
        len(existing_asset), len(existing_macro), len(existing_sector),
    )

    # Build work list: (type, key, queries, role, subject_desc, day)
    work: list[tuple] = []

    # --- Asset ---
    for inst in instruments:
        sym = inst["symbol"]
        cat = inst["category"]
        query = ASSET_QUERIES.get(sym, f"{sym} stock")
        role = _asset_role(sym, cat)
        subj = f"{sym} {cat} stock price"
        for d in trading_days:
            if (sym, d) not in existing_asset:
                work.append(("asset", sym, [query], role, subj, d))

    # --- Macro (2 queries combined per day) ---
    for d in trading_days:
        if ("global", d) not in existing_macro:
            work.append(("macro", "global", MACRO_QUERIES, _MACRO_ROLE, "the S&P 500 market", d))

    # --- Sector ---
    for sector_name, query in SECTOR_QUERIES.items():
        role = _sector_role(sector_name)
        subj = f"the {sector_name} sector"
        for d in trading_days:
            if (sector_name, d) not in existing_sector:
                work.append(("sector", sector_name, [query], role, subj, d))

    total = len(work)
    logger.info("Work items remaining: %d", total)
    if total == 0:
        logger.info("All sentiment data already cached.")
        return

    semaphore = asyncio.Semaphore(concurrency)
    done = 0
    errors = 0

    async def process_one(item: tuple) -> None:
        nonlocal done, errors
        type_, key, queries, role, subj, d = item
        async with semaphore:
            try:
                async with httpx.AsyncClient(
                    headers={"User-Agent": "Mozilla/5.0 (compatible; backtester/1.0)"},
                    timeout=20.0,
                ) as client:
                    # Fetch all queries, merge articles
                    all_articles: list[dict] = []
                    for q in queries:
                        url = _google_rss_url(q, d)
                        articles = await _fetch_rss(client, url)
                        all_articles.extend(articles)
                        await asyncio.sleep(0.3)

                    scored = await _score_articles(client, all_articles, role, subj)
                    await _upsert_daily(type_, key, d, scored)
                    done += 1
                    if done % 200 == 0 or done == total:
                        logger.info(
                            "Progress: %d / %d (%.1f%%) — errors: %d",
                            done, total, 100 * done / total, errors,
                        )
            except Exception as e:
                errors += 1
                logger.warning("process_one failed [%s %s %s]: %s", type_, key, d, e)

    await asyncio.gather(*[process_one(item) for item in work])
    logger.info("Sentiment fetch complete. Processed: %d, errors: %d", done, errors)


async def load_sentiment_cache(
    types: list[str] | None = None,
) -> dict[tuple[str, str, date], dict]:
    """Load backtest_sentiment_cache into memory for fast lookups.

    Returns dict keyed by (type, key, date) → {short_score, long_score,
    article_count, non_neutral_count, positive_count, negative_count}.
    """
    where = ""
    params: dict = {}
    if types:
        where = "WHERE type = ANY(:types)"
        params["types"] = types

    async with async_session() as session:
        result = await session.execute(
            text(f"""
                SELECT type, key, date,
                       short_score, long_score,
                       article_count, non_neutral_count,
                       positive_count, negative_count
                FROM backtest_sentiment_cache
                {where}
            """),
            params,
        )
        rows = result.fetchall()

    cache: dict[tuple[str, str, date], dict] = {}
    for r in rows:
        cache[(r.type, r.key, r.date)] = {
            "short_score":      float(r.short_score),
            "long_score":       float(r.long_score),
            "article_count":    r.article_count,
            "non_neutral_count": r.non_neutral_count,
            "positive_count":   r.positive_count,
            "negative_count":   r.negative_count,
        }
    logger.info("Loaded %d sentiment cache entries (%s)", len(cache), types or "all")
    return cache


# ─── Scoring helpers (mirror production scorer.py) ────────────────────────────
def _log_confidence(n: float, full_at: int) -> float:
    if full_at <= 0:
        return 1.0
    return min(1.0, math.log(1 + n) / math.log(1 + full_at))


def _clip(x: float) -> float:
    return max(-3.0, min(3.0, x))


def _consensus_mult(pos_w: float, neg_w: float, avg_age_hours: float) -> float:
    """Mirrors production _consensus_adjustment(): contrarian + priced-in dampening."""
    total = pos_w + neg_w
    if total < 3.0:
        return 1.0
    agreement = max(pos_w, neg_w) / total
    mult = 1.0
    if agreement > 0.80:
        mult *= 0.85   # herding signal
    if agreement > 0.75 and avg_age_hours > 48:
        mult *= 0.90   # priced-in signal
    return max(0.70, mult)


# ─── Public scoring functions (one per signal type) ───────────────────────────
def get_asset_sentiment_for_date(
    symbol: str,
    d: date,
    cache: dict,
    term: str = "short",
) -> tuple[float, float]:
    """Asset sentiment score for a backtest date.

    Returns (confidence_scaled_score ∈ [-3,3], confidence ∈ [0,1]).
    Mirrors production get_sentiment_score() exactly.
    """
    params = SENTIMENT_PARAMS[term]
    half_life = params["half_life_hours"]
    window_days = params["window_days"]
    full_at = params["full_confidence_at"]
    decay_lambda = math.log(2) / half_life

    weighted_sum = 0.0
    weight_total = 0.0
    non_neutral_w = 0.0
    pos_w = 0.0
    neg_w = 0.0
    age_sum = 0.0
    nn_count = 0

    score_key = "short_score" if term == "short" else "long_score"

    for day_offset in range(window_days + 1):
        day = d - timedelta(days=day_offset)
        row = cache.get(("asset", symbol, day))
        if not row or row["article_count"] == 0:
            continue
        age_hours = float(day_offset * 24)
        decay = math.exp(-decay_lambda * age_hours)

        count = row["article_count"]
        nn = row["non_neutral_count"]
        weighted_sum += row[score_key] * count * decay
        weight_total += count * decay
        non_neutral_w += nn * decay
        pos_w += row["positive_count"] * decay
        neg_w += row["negative_count"] * decay
        age_sum += age_hours * nn
        nn_count += nn

    if weight_total == 0.0:
        return 0.0, 0.0

    # Scale from [-1, 1] to [-3, 3] (same as SENTIMENT_SCORES in production)
    mean = (weighted_sum / weight_total) * 3.0
    confidence = _log_confidence(min(round(non_neutral_w), full_at), full_at)
    avg_age = age_sum / nn_count if nn_count > 0 else 0.0
    cmult = _consensus_mult(pos_w, neg_w, avg_age)

    final = _clip(mean * confidence * cmult)
    return round(final, 4), round(confidence, 4)


def get_macro_sentiment_for_date(
    d: date,
    cache: dict,
    term: str = "short",
) -> tuple[float, float]:
    """Global macro sentiment score for a backtest date.

    Returns (confidence_scaled_score ∈ [-3,3], confidence ∈ [0,1]).
    Mirrors production get_macro_score(): decay-weighted mean × 3 × confidence.
    """
    params = MACRO_PARAMS[term]
    half_life = params["half_life_hours"]
    window_days = params["window_hours"] // 24
    decay_lambda = math.log(2) / half_life

    weighted_sum = 0.0
    weight_total = 0.0
    total_articles = 0

    score_key = "short_score" if term == "short" else "long_score"

    for day_offset in range(window_days + 1):
        day = d - timedelta(days=day_offset)
        row = cache.get(("macro", "global", day))
        if not row or row["article_count"] == 0:
            continue
        age_hours = float(day_offset * 24)
        decay = math.exp(-decay_lambda * age_hours)

        count = row["article_count"]
        weighted_sum += row[score_key] * count * decay
        weight_total += count * decay
        total_articles += count

    if weight_total == 0.0:
        return 0.0, 0.0

    # ×3 to match production scorer.py line 565: score = _clip(float(row.score) * 3.0)
    macro_mean = (weighted_sum / weight_total) * 3.0
    confidence = _log_confidence(min(total_articles, 30), full_at=10)
    effective = _clip(macro_mean * confidence)
    return round(effective, 4), round(confidence, 4)


def get_sector_sentiment_for_date(
    sector: str | None,
    d: date,
    cache: dict,
    term: str = "short",
) -> tuple[float, float]:
    """GICS sector sentiment score for a backtest date.

    Returns (confidence_scaled_score ∈ [-3,3], confidence ∈ [0,1]).
    Returns (0.0, 0.0) for instruments with no sector (broad-market ETFs like VOO).
    Mirrors production get_sector_score(): decay-weighted mean × 3 × confidence.
    """
    if not sector:
        return 0.0, 0.0

    params = SECTOR_PARAMS[term]
    half_life = params["half_life_hours"]
    window_days = params["window_hours"] // 24
    decay_lambda = math.log(2) / half_life

    weighted_sum = 0.0
    weight_total = 0.0
    total_articles = 0

    score_key = "short_score" if term == "short" else "long_score"

    for day_offset in range(window_days + 1):
        day = d - timedelta(days=day_offset)
        row = cache.get(("sector", sector, day))
        if not row or row["article_count"] == 0:
            continue
        age_hours = float(day_offset * 24)
        decay = math.exp(-decay_lambda * age_hours)

        count = row["article_count"]
        weighted_sum += row[score_key] * count * decay
        weight_total += count * decay
        total_articles += count

    if weight_total == 0.0:
        return 0.0, 0.0

    # ×3 to match production scorer.py line 643: score = _clip(float(row.score) * 3.0)
    sector_mean = (weighted_sum / weight_total) * 3.0
    confidence = _log_confidence(min(total_articles, 20), full_at=8)
    effective = _clip(sector_mean * confidence)
    return round(effective, 4), round(confidence, 4)
