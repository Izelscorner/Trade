"""Unified fetch + score pipeline — aligned with backtester architecture.

Replaces the two-service pattern (news-fetcher → llm-processor) with a single
pipeline that fetches Google News RSS and scores sentiment via NIM inline.

Architecture (identical to backtester/historical_sentiment.py):
  - Sequential RSS fetching (1 req/sec token bucket, no Google 503s)
  - Adaptive date-range splitting (threshold=90, recursive)
  - Query rotation for macro/sector (multiple phrasings per time window)
  - NIM rate-limited to 40 req/min (token bucket), max 3 concurrent calls
  - Direct writes to production tables (news_articles, sentiment_scores,
    news_instrument_map, macro_sentiment, sector_sentiment)

Startup: fills gaps for each category's decay window (aligned with scorer.py):
  - Asset sentiment: 30 days (scorer long-term window = 30d, half-life 7d)
  - Sector sentiment: 30 days (scorer long-term window = 720h = 30d, half-life 10d)
  - Macro sentiment: 30 days (practical cap; 27d half-life)
After initial fill, ongoing loop every 10 seconds fetches latest stories.
"""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import httpx
from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

# ─── Configuration ─────────────────────────────────────────────────────────
NIM_API_KEY = os.environ.get("NIM_API_KEY", "")
NIM_BASE_URL = os.environ.get("NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")
NIM_MODEL = os.environ.get("NIM_MODEL", "qwen/qwen3-next-80b-a3b-instruct")

# Category-specific decay windows for gap filling on startup.
# All categories capped at 30 days — beyond that, exponential decay renders articles negligible.
LOOKBACK_DAYS = {
    "asset": 30,    # Scorer: SENTIMENT_PARAMS["long"]["window_days"] = 30
    "sector": 30,   # Scorer: SECTOR_PARAMS["long"]["window_hours"] = 720h = 30 days
    "macro": 30,    # Practical cap: 27d half-life means 30d-old articles have ~46% weight, older negligible
}
# Ongoing cycle interval — catch new stories as they appear
ONGOING_INTERVAL = 10  # seconds

# NIM rate limiting — NVIDIA free tier is ~40 req/min
NIM_BATCH = 100
NIM_RPM = int(os.environ.get("NIM_RATE_LIMIT_RPM", "40"))
NIM_CONCURRENCY = int(os.environ.get("NIM_CONCURRENCY", "3"))

# ─── Sentiment label → probability distribution (production-faithful) ──────
SENTIMENT_PROBABILITIES = {
    "very_positive": {"positive": 0.90, "negative": 0.02, "neutral": 0.08},
    "positive":      {"positive": 0.70, "negative": 0.05, "neutral": 0.25},
    "neutral":       {"positive": 0.15, "negative": 0.15, "neutral": 0.70},
    "negative":      {"positive": 0.05, "negative": 0.70, "neutral": 0.25},
    "very_negative": {"positive": 0.02, "negative": 0.90, "neutral": 0.08},
}

# Backtester → production label format: very_positive → very positive
def _prod_label(label: str) -> str:
    return label.replace("_", " ")

VALID_LABELS = {"very_positive", "positive", "neutral", "negative", "very_negative"}

SENTIMENT_MULTIPLIERS = {
    "very positive": 1.0, "positive": 0.5, "neutral": 0.0,
    "negative": -0.5, "very negative": -1.0,
}

# ─── Google News query definitions (aligned with backtester) ───────────────
ASSET_QUERIES: dict[str, list[str]] = {
    "RTX":   ["intitle:RTX stock", "RTX Raytheon defense aerospace stock"],
    "NVDA":  ["intitle:NVDA OR intitle:NVIDIA stock", "NVIDIA GPU AI chip earnings"],
    "GOOGL": ["intitle:GOOGL OR intitle:Alphabet OR intitle:Google stock", "Google Alphabet search advertising"],
    "AAPL":  ["intitle:AAPL OR intitle:Apple stock", "Apple iPhone revenue earnings"],
    "TSLA":  ["intitle:TSLA OR intitle:Tesla stock", "Tesla electric vehicle deliveries"],
    "PLTR":  ["intitle:PLTR OR intitle:Palantir stock", "Palantir government contract AI"],
    "LLY":   ["intitle:LLY OR intitle:Eli Lilly stock", "Eli Lilly drug approval obesity"],
    "NVO":   ["intitle:NVO OR intitle:Novo Nordisk stock", "Novo Nordisk Ozempic Wegovy"],
    "WMT":   ["intitle:WMT OR intitle:Walmart stock", "Walmart earnings revenue retail"],
    "XOM":   ["intitle:XOM OR intitle:Exxon stock", "Exxon Mobil oil earnings production"],
    "IITU":  ["iShares technology ETF", "technology ETF stock market"],
    "SMH":   ["SMH semiconductor ETF", "semiconductor ETF stock"],
    "VOO":   ["VOO S&P 500 ETF", "S&P 500 ETF stock market"],
    "GOLD":  ["gold price futures market", "gold bullion safe haven investment"],
    "OIL":   ["crude oil price futures market", "oil futures OPEC production barrel"],
}

MACRO_QUERIES: list[str] = [
    "stock market today Wall Street S&P 500",
    "stock market crash rally correction bear bull",
    "Federal Reserve interest rates inflation economy",
    "US economy jobs unemployment consumer spending",
    "global economy recession GDP trade war tariffs",
    "CPI inflation report consumer prices economic data",
    "Treasury bond yields debt ceiling fiscal policy",
    "US politics economy policy regulation legislation",
    "sanctions trade policy export controls geopolitics",
    "war conflict military geopolitical risk market",
    "Russia Ukraine war sanctions energy Europe",
    "China Taiwan tensions trade decoupling supply chain",
    "Middle East conflict oil supply disruption",
    "FOMC meeting minutes Federal Reserve decision",
    "US jobs report nonfarm payrolls unemployment",
]

SECTOR_QUERIES: dict[str, list[str]] = {
    "technology": [
        "technology stocks semiconductor AI chip industry",
        "tech sector earnings cloud computing artificial intelligence",
    ],
    "financials": [
        "banking stocks financial sector interest rates Wall Street",
        "bank earnings financial regulation credit lending",
    ],
    "healthcare": [
        "healthcare stocks biotech pharma drug FDA",
        "FDA approval drug trial clinical biotech pharmaceutical",
    ],
    "consumer_discretionary": [
        "consumer stocks retail spending automotive Tesla Amazon",
        "retail sales consumer confidence discretionary spending",
    ],
    "consumer_staples": [
        "consumer staples stocks food beverage Walmart Costco",
        "grocery food prices consumer goods inflation staples",
    ],
    "communication": [
        "media stocks streaming telecom Meta Google Disney",
        "social media advertising digital media regulation",
    ],
    "energy": [
        "energy stocks oil gas prices OPEC Exxon Chevron",
        "oil prices crude OPEC production supply demand",
    ],
    "industrials": [
        "industrial stocks manufacturing defense Boeing Caterpillar",
        "defense spending military contracts aerospace industrial",
    ],
    "materials": [
        "materials stocks mining metals copper lithium gold",
        "commodity prices mining sector raw materials",
    ],
    "utilities": [
        "utilities stocks power electricity grid renewable energy",
        "electric utility regulation rate increase power grid",
    ],
    "real_estate": [
        "real estate stocks REIT housing market property mortgage",
        "housing market prices mortgage rates real estate",
    ],
}

# ─── LLM prompt builders (from backtester, production-faithful) ────────────
_SENTIMENT_SYSTEM = (
    "You predict PRICE DIRECTION of a specific asset. You ONLY care about the PRICE going UP or DOWN.\n"
    "IMPORTANT: For commodities (OIL, GOLD), wars and supply disruptions make the PRICE GO UP — that is POSITIVE.\n"
    "Always respond with valid JSON only. No explanations."
)
_MACRO_SYSTEM = (
    "You predict S&P 500 INDEX direction from news events. Be decisive — wars, inflation, rate changes are NOT neutral.\n"
    "Always respond with valid JSON only. No explanations."
)
_SECTOR_SYSTEM = (
    "You predict impact on a GICS sector's stock prices. Be decisive — regulatory changes and industry trends are NOT neutral.\n"
    "Always respond with valid JSON only. No explanations."
)

_CATEGORY_ROLES = {
    "stock": "You are a Wall Street equity analyst covering {name}. You think like both a day-trader (short-term price action) and a fundamental investor (long-term value).",
    "etf": "You are a Wall Street ETF analyst covering {name}. You understand constituent-level impacts propagate to the ETF with weight-proportional magnitude.",
    "commodity": "You are a commodity futures trader at a Western bank trading {name}. You ONLY care about the PRICE of this commodity going UP or DOWN. Supply disruptions = PRICE UP = positive. Demand destruction = PRICE DOWN = negative.",
}
_CATEGORY_DESCS = {
    "stock": "{name} stock price on US exchanges",
    "etf": "{name} ETF price on US exchanges",
    "commodity": "{name} futures price",
}


def _build_asset_prompt(role: str, articles: list[dict], asset_desc: str) -> str:
    articles_text = ""
    for art in articles:
        title = art["title"]
        summary = art.get("summary", "")
        text_ = f"{title}. {summary[:200]}" if summary else title
        articles_text += f'\n{art["i"]}. id="{art["i"]}": "{text_[:350]}"\n'
    return f"""{role}

Predict whether each article pushes the PRICE of {asset_desc} UP or DOWN.

MANDATORY RULES — follow these exactly:
- "overweight", "buy", "upgrade", "price target raised" → positive or very_positive
- "underweight", "sell", "downgrade", "price target cut" → negative or very_negative
- "Fund buys/sells X shares" (institutional rebalancing) → neutral, confidence 0.1-0.2
- OIL futures: war, supply disruption, sanctions, OPEC cuts → positive (price goes UP). Demand destruction, recession, supply increase → negative
- GOLD futures: war, crisis, fear, rate cuts → positive (safe haven). Rate hikes, risk-on → negative
- Defense stocks (RTX): wars, military spending → positive
- Oil price surge → negative for most stocks, positive for energy stocks (XOM)
- Article not about this asset → neutral, confidence 0.1-0.3
- Article directly about this asset → be decisive, avoid neutral

TWO HORIZONS:
- short_sentiment: 1-7 day price reaction
- long_sentiment: 1-6 month fundamental impact
They CAN differ.

Labels: very_positive, positive, neutral, negative, very_negative
Confidence: 0.0-1.0

Articles:{articles_text}
Respond ONLY with JSON: {{"results": [{{"id": "...", "short_sentiment": "...", "short_confidence": 0.5, "long_sentiment": "...", "long_confidence": 0.5}}, ...]}}
Include ALL {len(articles)} articles."""


def _build_macro_prompt(articles: list[dict]) -> str:
    articles_text = ""
    for art in articles:
        title = art["title"]
        summary = art.get("summary", "")
        text_ = f"{title}. {summary[:200]}" if summary else title
        articles_text += f'\n{art["i"]}. id="{art["i"]}": "{text_[:350]}"\n'
    return f"""Assess each article's impact on the S&P 500 INDEX PRICE.

MANDATORY RULES:
- Wars, oil supply disruption, inflation spike → negative (both horizons usually)
- Rate hikes → negative short-term, may be positive long-term if inflation controlled
- Rate cuts, stimulus, strong GDP → positive
- Geopolitical crises → negative short-term, assess long-term separately
- Company-specific news (earnings, products, analyst ratings) → neutral, confidence 0.1-0.2
- Foreign domestic politics with no US impact → neutral, confidence 0.1
- DO NOT default to neutral. If the event affects markets at all, pick a direction.

TWO HORIZONS:
- short_sentiment: 1-7 day S&P 500 reaction
- long_sentiment: 1-6 month structural impact
They CAN differ.

Labels: very_positive, positive, neutral, negative, very_negative
Confidence: 0.0-1.0

Articles:{articles_text}
Respond ONLY with JSON: {{"results": [{{"id": "...", "short_sentiment": "...", "short_confidence": 0.8, "long_sentiment": "...", "long_confidence": 0.6}}, ...]}}
Include ALL {len(articles)} articles."""


def _build_sector_prompt(articles: list[dict], sector: str) -> str:
    sector_display = sector.replace("_", " ").title()
    articles_text = ""
    for art in articles:
        title = art["title"]
        summary = art.get("summary", "")
        text_ = f"{title}. {summary[:200]}" if summary else title
        articles_text += f'\n{art["i"]}. id="{art["i"]}": "{text_[:350]}"\n'
    return f"""Assess each article's impact on {sector_display} SECTOR stock prices.

RULES:
- Regulatory changes, industry trends, supply chain shifts → pick a direction, high confidence
- Company-specific news not reflecting sector trends → neutral, confidence 0.1-0.2
- DO NOT default to neutral. Industry news almost always has directional impact.

TWO HORIZONS:
- short_sentiment: 1-7 day sector reaction
- long_sentiment: 1-6 month structural impact

Labels: very_positive, positive, neutral, negative, very_negative
Confidence: 0.0-1.0

Articles:{articles_text}
Respond ONLY with JSON: {{"results": [{{"id": "...", "short_sentiment": "...", "short_confidence": 0.5, "long_sentiment": "...", "long_confidence": 0.5}}, ...]}}
Include ALL {len(articles)} articles."""


# ─── Token Bucket Rate Limiter ─────────────────────────────────────────────
@dataclass
class TokenBucket:
    rate: float
    capacity: float
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    def __post_init__(self):
        self._tokens = self.capacity
        self._last_refill = time.monotonic()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
                self._last_refill = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            await asyncio.sleep(0.1)


# ─── RSS fetch (from backtester, Chrome TLS impersonation) ─────────────────
_IMPERSONATE_PROFILES = ["chrome120", "chrome119", "chrome116", "chrome110"]
_RSS_SPLIT_THRESHOLD = 90
_RSS_MIN_RANGE_DAYS = 7

_rss_consecutive_503s = 0
_rss_503_lock = asyncio.Lock()


def _google_rss_url(query: str, start: date, end: date) -> str:
    date_from = (start - timedelta(days=1)).isoformat()
    date_to = (end + timedelta(days=1)).isoformat()
    q = query.replace(" ", "+").replace('"', "%22")
    return (
        f"https://news.google.com/rss/search"
        f"?q={q}+after:{date_from}+before:{date_to}"
        f"&hl=en-US&gl=US&ceid=US:en"
    )


async def _fetch_rss(session, url: str, fallback_date: date, max_articles: int = 100, retries: int = 4) -> list[dict]:
    global _rss_consecutive_503s
    for attempt in range(retries):
        try:
            profile = random.choice(_IMPERSONATE_PROFILES)
            resp = await session.get(url, timeout=15.0, allow_redirects=True, impersonate=profile)

            if resp.status_code == 503:
                async with _rss_503_lock:
                    _rss_consecutive_503s += 1
                    consec = _rss_consecutive_503s
                if consec > 100:
                    wait = 120.0 + random.uniform(0, 30)
                elif consec > 50:
                    wait = 60.0 + random.uniform(0, 10)
                elif consec > 20:
                    wait = 30.0 + random.uniform(0, 5)
                else:
                    wait = min(30.0, 3.0 * (2 ** attempt)) + random.uniform(0, 2)
                if consec % 20 == 0:
                    logger.warning("Google News 503 — %d consecutive. Backing off %.0fs.", consec, wait)
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = 30.0 + random.uniform(0, 10)
                logger.warning("Google News 429. Waiting %.0fs.", wait)
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            async with _rss_503_lock:
                _rss_consecutive_503s = 0

            root = ET.fromstring(resp.content)
            articles = []
            for item in root.findall(".//item"):
                title_el = item.find("title")
                desc_el = item.find("description")
                pub_el = item.find("pubDate")
                title = (title_el.text or "").strip()
                summary = re.sub(r"<[^>]+>", "", (desc_el.text or "")).strip()[:300]

                published_at = None
                if pub_el is not None and pub_el.text:
                    try:
                        published_at = parsedate_to_datetime(pub_el.text)
                    except Exception:
                        pass
                if published_at is None:
                    published_at = datetime(
                        fallback_date.year, fallback_date.month, fallback_date.day,
                        12, 0, 0, tzinfo=timezone.utc,
                    )
                if len(title) >= 15:
                    articles.append({"title": title, "summary": summary, "published_at": published_at})
                if len(articles) >= max_articles:
                    break
            return articles
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2.0 ** attempt)
            else:
                logger.warning("RSS fetch failed: %s — %s", url[:80], e)
    return []


async def _adaptive_fetch(session, query, start, end, rss_bucket, depth=0, max_depth=5) -> list[dict]:
    if start > end:
        return []
    mid_date = start + (end - start) // 2
    range_days = (end - start).days
    await rss_bucket.acquire()
    url = _google_rss_url(query, start, end)
    articles = await _fetch_rss(session, url, mid_date)

    if len(articles) >= _RSS_SPLIT_THRESHOLD and range_days > _RSS_MIN_RANGE_DAYS and depth < max_depth:
        mid = start + timedelta(days=range_days // 2)
        left = await _adaptive_fetch(session, query, start, mid, rss_bucket, depth + 1, max_depth)
        right = await _adaptive_fetch(session, query, mid + timedelta(days=1), end, rss_bucket, depth + 1, max_depth)
        return left + right
    return articles


async def _rotated_fetch(session, queries, start, end, rss_bucket, max_depth=5) -> list[dict]:
    if not queries:
        return []
    total_days = (end - start).days
    if total_days <= 0:
        return []

    all_articles, seen_hashes = [], set()
    chunk_days = max(14, min(60, total_days // max(len(queries), 1)))
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)

    num_passes = min(2, len(queries))
    for pass_idx in range(num_passes):
        for chunk_idx, (cs, ce) in enumerate(chunks):
            q = queries[(chunk_idx + pass_idx) % len(queries)]
            articles = await _adaptive_fetch(session, q, cs, ce, rss_bucket, max_depth=max_depth)
            for art in articles:
                h = _md5_hash(art["title"])
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    all_articles.append(art)
    return all_articles


# ─── Dedup helpers ─────────────────────────────────────────────────────────
def _md5_hash(text: str) -> str:
    return hashlib.md5(text.strip().lower().encode()).hexdigest()


def _fuzzy_dedup(articles: list[dict], threshold: float = 0.85) -> list[dict]:
    seen_hashes, unique, seen_titles = set(), [], []
    try:
        from rapidfuzz import fuzz
        has_rf = True
    except ImportError:
        has_rf = False

    for art in articles:
        h = _md5_hash(art["title"])
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        tn = art["title"].lower().strip()
        is_dup = False
        if has_rf:
            for seen in seen_titles:
                if fuzz.ratio(tn, seen) >= threshold * 100:
                    is_dup = True
                    break
                if len(tn) > 30 and fuzz.partial_ratio(tn, seen) >= 90:
                    is_dup = True
                    break
        if not is_dup:
            seen_titles.append(tn)
            art["url_hash"] = h
            unique.append(art)
    return unique


# ─── NIM API client (rate-limited: token bucket + concurrency semaphore) ────
_nim_semaphore: asyncio.Semaphore | None = None
_nim_bucket: "TokenBucket | None" = None


def _get_nim_semaphore() -> asyncio.Semaphore:
    global _nim_semaphore
    if _nim_semaphore is None:
        _nim_semaphore = asyncio.Semaphore(NIM_CONCURRENCY)
    return _nim_semaphore


def _get_nim_bucket() -> "TokenBucket":
    global _nim_bucket
    if _nim_bucket is None:
        # Rate: NIM_RPM per minute → tokens per second
        _nim_bucket = TokenBucket(rate=NIM_RPM / 60.0, capacity=min(5.0, NIM_RPM / 10.0))
    return _nim_bucket


async def _call_nim(client: httpx.AsyncClient, prompt: str, system_msg: str = "", retries: int = 3) -> list[dict]:
    messages = []
    if system_msg:
        messages.append({"role": "system", "content": system_msg})
    messages.append({"role": "user", "content": prompt})
    sem = _get_nim_semaphore()
    bucket = _get_nim_bucket()

    for attempt in range(retries):
        await bucket.acquire()  # Rate limit: wait for token
        async with sem:         # Concurrency limit: max N in-flight
            try:
                resp = await client.post(
                    f"{NIM_BASE_URL}/chat/completions",
                    headers={"Authorization": f"Bearer {NIM_API_KEY}", "Content-Type": "application/json"},
                    json={"model": NIM_MODEL, "messages": messages, "temperature": 0.0, "max_tokens": 6000},
                    timeout=120.0,
                )
                if resp.status_code == 429:
                    wait = min(30.0, 3.0 * (2 ** attempt)) + random.uniform(0, 2)
                    logger.warning("NIM 429 (attempt %d), backoff %.1fs", attempt + 1, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"].strip()
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict) and "results" in parsed:
                        return parsed["results"]
                    if isinstance(parsed, list):
                        return parsed
                except json.JSONDecodeError:
                    pass
                match = re.search(r"\[.*\]", content, re.DOTALL)
                if match:
                    return json.loads(match.group())
                return []
            except (json.JSONDecodeError, httpx.HTTPStatusError, httpx.RequestError) as e:
                logger.warning("NIM error attempt %d: %s", attempt + 1, e)
        if attempt < retries - 1:
            await asyncio.sleep(1.0)
    return []


def _parse_nim_results(chunk: list[dict], responses: list[dict]) -> list[dict]:
    scored = {}
    for r in responses:
        if isinstance(r, dict):
            rid = r.get("id", r.get("i"))
            if rid is not None:
                scored[str(rid)] = r

    results = []
    for i, art in enumerate(chunk):
        sr = scored.get(str(i + 1), {})
        ss = sr.get("short_sentiment", sr.get("ss", "neutral"))
        if ss not in VALID_LABELS:
            ss = "neutral"
        sc = max(0.0, min(1.0, float(sr.get("short_confidence", sr.get("sc", 0.5)))))
        ls = sr.get("long_sentiment", sr.get("ls", "neutral"))
        if ls not in VALID_LABELS:
            ls = "neutral"
        lc = max(0.0, min(1.0, float(sr.get("long_confidence", sr.get("lc", 0.5)))))
        results.append({
            **art,
            "short_label": ss, "long_label": ls,
            "short_confidence": round(sc, 3), "long_confidence": round(lc, 3),
        })
    return results


# ─── Production DB helpers ─────────────────────────────────────────────────
async def _load_instruments() -> dict[str, dict]:
    """Load instruments from DB. Returns {symbol: {id, name, category, sector}}."""
    async with async_session() as s:
        r = await s.execute(text(
            "SELECT id, symbol, name, category, sector FROM instruments WHERE is_active = true"
        ))
        return {
            row.symbol: {"id": str(row.id), "name": row.name, "category": row.category, "sector": row.sector}
            for row in r.fetchall()
        }


async def _load_existing_hashes(lookback_days: int = 180) -> set[str]:
    """Load recent article title hashes for dedup."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    async with async_session() as s:
        r = await s.execute(text(
            "SELECT title FROM news_articles WHERE fetched_at >= :cutoff"
        ), {"cutoff": cutoff})
        return {_md5_hash(row.title) for row in r.fetchall()}


def _label_to_probs(label: str, confidence: float) -> tuple[float, float, float]:
    """Convert sentiment label + confidence to probability distribution (production-faithful)."""
    key = label.replace(" ", "_")
    probs = SENTIMENT_PROBABILITIES.get(key, SENTIMENT_PROBABILITIES["neutral"])
    pos = round(probs["positive"] * confidence + 0.15 * (1 - confidence), 6)
    neg = round(probs["negative"] * confidence + 0.15 * (1 - confidence), 6)
    neu = round(1.0 - pos - neg, 6)
    if neu < 0:
        neu = 0.0
        total = pos + neg
        if total > 0:
            pos, neg = pos / total, neg / total
    return pos, neg, neu


async def _store_article_with_sentiment(
    type_: str,
    key: str,
    art: dict,
    instrument_map: dict[str, dict],
) -> str | None:
    """Store one scored article into production tables.

    Inserts into news_articles, sentiment_scores, and news_instrument_map.
    Returns article_id or None on conflict.
    """
    title = art["title"][:500]
    summary = (art.get("summary") or "")[:2000]
    published_at = art.get("published_at")
    short_label = art.get("short_label", "neutral")
    long_label = art.get("long_label", "neutral")
    short_conf = art.get("short_confidence", 0.5)
    long_conf = art.get("long_confidence", 0.5)

    # Determine category and flags
    if type_ == "macro":
        category = "macro_markets"
        is_macro = True
        is_asset_specific = False
    elif type_ == "sector":
        category = f"sector_{key}"
        is_macro = False
        is_asset_specific = False
    else:
        category = "asset_specific"
        is_macro = False
        is_asset_specific = True

    async with async_session() as session:
        # Insert article
        r = await session.execute(
            text("""
                INSERT INTO news_articles
                    (title, link, summary, source, category, is_macro, is_asset_specific,
                     ollama_processed, published_at, macro_sentiment_label, macro_long_term_label)
                VALUES (:title, :link, :summary, 'google_news_unified', :category,
                        :is_macro, :is_asset_specific, true, :pub, :macro_short, :macro_long)
                ON CONFLICT (title, source) DO NOTHING
                RETURNING id
            """),
            {
                "title": title,
                "link": f"https://news.google.com/search?q={key}",
                "summary": summary,
                "category": category,
                "is_macro": is_macro,
                "is_asset_specific": is_asset_specific,
                "pub": published_at,
                "macro_short": _prod_label(short_label) if is_macro or type_ == "sector" else None,
                "macro_long": _prod_label(long_label) if is_macro or type_ == "sector" else None,
            },
        )
        row = r.fetchone()
        if not row:
            await session.rollback()
            return None
        article_id = str(row.id)

        # Insert sentiment score
        pos, neg, neu = _label_to_probs(short_label, short_conf)
        await session.execute(
            text("""
                INSERT INTO sentiment_scores
                    (article_id, positive, negative, neutral, label, long_term_label, long_term_confidence)
                VALUES (:aid, :pos, :neg, :neu, :label, :lt_label, :lt_conf)
                ON CONFLICT (article_id) DO UPDATE
                SET positive = :pos, negative = :neg, neutral = :neu, label = :label,
                    long_term_label = :lt_label, long_term_confidence = :lt_conf
            """),
            {
                "aid": article_id,
                "pos": pos, "neg": neg, "neu": neu,
                "label": _prod_label(short_label),
                "lt_label": _prod_label(long_label),
                "lt_conf": long_conf,
            },
        )

        # Insert instrument mapping (asset articles only)
        if type_ == "asset" and key in instrument_map:
            inst_id = instrument_map[key]["id"]
            await session.execute(
                text("""
                    INSERT INTO news_instrument_map (article_id, instrument_id, relevance_score)
                    VALUES (:aid, :iid, 1.0)
                    ON CONFLICT (article_id, instrument_id) DO NOTHING
                """),
                {"aid": article_id, "iid": inst_id},
            )

        await session.commit()
        return article_id


async def _batch_store_articles(
    type_: str,
    key: str,
    articles: list[dict],
    instrument_map: dict[str, dict],
) -> int:
    """Store a batch of scored articles. Returns count of new articles stored."""
    stored = 0
    for art in articles:
        aid = await _store_article_with_sentiment(type_, key, art, instrument_map)
        if aid:
            stored += 1
    return stored


# ─── Macro/Sector aggregation (mirrors production llm-processor) ───────────
async def update_macro_sentiment() -> None:
    """Aggregate macro sentiment from recent articles (production-faithful)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=72)
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT COALESCE(macro_sentiment_label, 'neutral') AS macro_sentiment_label,
                       COALESCE(macro_long_term_label, 'neutral') AS macro_long_term_label,
                       published_at
                FROM news_articles
                WHERE is_macro = true AND ollama_processed = true AND published_at >= :cutoff
                ORDER BY published_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()
        if not rows:
            return

        now_ts = datetime.now(timezone.utc)

        for term, half_life_h, label_attr in [("short", 24.0, "macro_sentiment_label"), ("long", 96.0, "macro_long_term_label")]:
            decay_lambda = math.log(2) / half_life_h
            wsum, wtotal = 0.0, 0.0
            for r in rows:
                lbl = getattr(r, label_attr)
                if lbl == "neutral":
                    continue
                score = SENTIMENT_MULTIPLIERS.get(lbl, 0.0)
                pub = r.published_at or now_ts
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=timezone.utc)
                age_h = max(0, (now_ts - pub).total_seconds() / 3600)
                w = math.exp(-decay_lambda * age_h)
                wsum += score * w
                wtotal += w

            avg = wsum / wtotal if wtotal > 0 else 0.0
            avg = max(-1.0, min(1.0, avg))
            if avg >= 0.3:
                label = "positive"
            elif avg >= 0.1:
                label = "slightly positive"
            elif avg > -0.1:
                label = "neutral"
            elif avg > -0.3:
                label = "slightly negative"
            else:
                label = "negative"

            await session.execute(
                text("""
                    INSERT INTO macro_sentiment (region, term, score, label, article_count)
                    VALUES ('global', :term, :score, :label, :cnt)
                """),
                {"term": term, "score": round(avg, 6), "label": label, "cnt": len(rows)},
            )

            # Keep last 100 per term
            await session.execute(
                text("""
                    DELETE FROM macro_sentiment WHERE id IN (
                        SELECT id FROM macro_sentiment WHERE region = 'global' AND term = :term
                        ORDER BY calculated_at DESC OFFSET 100
                    )
                """),
                {"term": term},
            )
        await session.commit()


async def update_sector_sentiment() -> None:
    """Aggregate sector sentiment from recent articles (production-faithful)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=14)
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT category,
                       COALESCE(macro_sentiment_label, 'neutral') AS short_label,
                       COALESCE(macro_long_term_label, 'neutral') AS long_label
                FROM news_articles
                WHERE category LIKE 'sector_%%'
                AND ollama_processed = true AND published_at >= :cutoff
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()
        if not rows:
            return

        sector_articles: dict[str, list] = {}
        for r in rows:
            sector = r.category.replace("sector_", "")
            sector_articles.setdefault(sector, []).append(r)

        for sector, arts in sector_articles.items():
            for term, label_attr in [("short", "short_label"), ("long", "long_label")]:
                non_neutral = [a for a in arts if getattr(a, label_attr) != "neutral"]
                if not non_neutral:
                    score, label = 0.0, "neutral"
                else:
                    scores = [SENTIMENT_MULTIPLIERS.get(getattr(a, label_attr), 0.0) for a in non_neutral]
                    score = sum(scores) / len(scores)
                    score = max(-1.0, min(1.0, score))
                    if score >= 0.3:
                        label = "positive"
                    elif score >= 0.1:
                        label = "slightly positive"
                    elif score > -0.1:
                        label = "neutral"
                    elif score > -0.3:
                        label = "slightly negative"
                    else:
                        label = "negative"

                await session.execute(
                    text("""
                        INSERT INTO sector_sentiment (sector, term, score, label, article_count)
                        VALUES (:sector, :term, :score, :label, :cnt)
                    """),
                    {"sector": sector, "term": term, "score": round(score, 6), "label": label, "cnt": len(arts)},
                )
                await session.execute(
                    text("""
                        DELETE FROM sector_sentiment WHERE id IN (
                            SELECT id FROM sector_sentiment WHERE sector = :sector AND term = :term
                            ORDER BY calculated_at DESC OFFSET 100
                        )
                    """),
                    {"sector": sector, "term": term},
                )
        await session.commit()


# ─── Main pipeline ─────────────────────────────────────────────────────────
@dataclass
class _WorkItem:
    type_: str          # "asset", "macro", "sector"
    key: str            # symbol, "global", or sector name
    queries: list[str]
    name: str
    category: str       # "stock", "etf", "commodity"
    sector: str
    start_date: date | None = None  # per-item date range
    end_date: date | None = None


def _build_work_items(
    instrument_map: dict[str, dict],
    lookback_overrides: dict[str, int] | None = None,
) -> list[_WorkItem]:
    """Build work items with category-specific date ranges."""
    now = datetime.now(timezone.utc).date()
    lb = lookback_overrides or LOOKBACK_DAYS
    work: list[_WorkItem] = []

    asset_start = now - timedelta(days=lb.get("asset", 30))
    for sym, inst in instrument_map.items():
        queries = ASSET_QUERIES.get(sym)
        if not queries:
            continue
        work.append(_WorkItem(
            "asset", sym, queries, inst["name"], inst["category"],
            inst.get("sector") or "", asset_start, now,
        ))

    macro_start = now - timedelta(days=lb.get("macro", 180))
    work.append(_WorkItem("macro", "global", MACRO_QUERIES, "", "", "", macro_start, now))

    sector_start = now - timedelta(days=lb.get("sector", 180))
    for sector_name, queries in SECTOR_QUERIES.items():
        work.append(_WorkItem("sector", sector_name, queries, "", "", sector_name, sector_start, now))

    return work


def _google_rss_url_latest(query: str) -> str:
    """Build a Google News RSS URL for the latest stories (no date restriction)."""
    q = query.replace(" ", "+").replace('"', "%22")
    return f"https://news.google.com/rss/search?q={q}+when:1d&hl=en-US&gl=US&ceid=US:en"


async def _run_nim_scoring(
    nim_client: httpx.AsyncClient,
    item: _WorkItem,
    filtered: list[dict],
    instrument_map: dict[str, dict],
) -> int:
    """Score articles via NIM and store. Returns count stored."""
    # Build prompt function
    if item.type_ == "asset":
        role = _CATEGORY_ROLES.get(item.category, _CATEGORY_ROLES["stock"]).format(name=item.name)
        desc = _CATEGORY_DESCS.get(item.category, _CATEGORY_DESCS["stock"]).format(name=item.name)
        sys_msg = _SENTIMENT_SYSTEM
        def build_prompt(numbered, r=role, d=desc):
            return _build_asset_prompt(r, numbered, d)
    elif item.type_ == "macro":
        sys_msg = _MACRO_SYSTEM
        def build_prompt(numbered):
            return _build_macro_prompt(numbered)
    else:
        sys_msg = _SECTOR_SYSTEM
        sec = item.sector
        def build_prompt(numbered, s=sec):
            return _build_sector_prompt(numbered, s)

    total_stored = 0
    for chunk_start in range(0, len(filtered), NIM_BATCH):
        chunk = filtered[chunk_start:chunk_start + NIM_BATCH]
        numbered = [{"i": i + 1, "title": a["title"], "summary": a.get("summary", ""), **a} for i, a in enumerate(chunk)]
        prompt = build_prompt(numbered)
        responses = await _call_nim(nim_client, prompt, system_msg=sys_msg)
        results = _parse_nim_results(chunk, responses)
        if results:
            n = await _batch_store_articles(item.type_, item.key, results, instrument_map)
            total_stored += n
            if n:
                logger.info("[%s %s] %d articles scored & stored", item.type_.upper(), item.key, n)
    return total_stored


async def run_gap_fill(instrument_map: dict[str, dict]) -> int:
    """Fill gaps for each category's specific decay window on startup.

    Uses full adaptive date-range fetching with rotation for historical coverage.
    Asset: 30 days, Sector: 180 days, Macro: 180 days.
    """
    from curl_cffi.requests import AsyncSession

    work = _build_work_items(instrument_map)
    max_lb = max(LOOKBACK_DAYS.values())
    existing_hashes = await _load_existing_hashes(max_lb + 7)

    logger.info("Gap fill: %d work items (asset=%dd, sector=%dd, macro=%dd)",
                len(work), LOOKBACK_DAYS["asset"], LOOKBACK_DAYS["sector"], LOOKBACK_DAYS["macro"])

    rss_bucket = TokenBucket(rate=1.0, capacity=3.0)
    total_stored = 0

    async with AsyncSession() as rss_session, httpx.AsyncClient(
        timeout=120.0,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=40),
    ) as nim_client:

        for idx, item in enumerate(work):
            start_d = item.start_date
            end_d = item.end_date

            # RSS fetch (sequential, paced via token bucket)
            if len(item.queries) > 1:
                raw_articles = await _rotated_fetch(
                    rss_session, item.queries, start_d, end_d,
                    rss_bucket, max_depth=5,
                )
            else:
                raw_articles = await _adaptive_fetch(
                    rss_session, item.queries[0], start_d, end_d,
                    rss_bucket, max_depth=5,
                )

            # Dedup against existing DB + within batch
            filtered = []
            for art in raw_articles:
                h = _md5_hash(art["title"])
                if h not in existing_hashes:
                    existing_hashes.add(h)
                    filtered.append(art)
            filtered = _fuzzy_dedup(filtered)

            if not filtered:
                continue

            logger.info(
                "[%d/%d] %s %s (%s→%s) — %d raw → %d new",
                idx + 1, len(work), item.type_.upper(), item.key,
                start_d, end_d, len(raw_articles), len(filtered),
            )

            # Score via NIM (rate-limited)
            n = await _run_nim_scoring(nim_client, item, filtered, instrument_map)
            total_stored += n

    # Aggregate macro/sector sentiment
    await update_macro_sentiment()
    await update_sector_sentiment()

    logger.info("Gap fill complete: %d new articles stored", total_stored)
    return total_stored


async def run_ongoing_cycle(instrument_map: dict[str, dict]) -> int:
    """Fetch the latest stories (last 24h) for all categories. Fast 10-second cycle.

    Uses `when:1d` Google News parameter — no date-range splitting needed.
    One RSS call per query, sequential at 1 req/sec.
    """
    from curl_cffi.requests import AsyncSession

    existing_hashes = await _load_existing_hashes(7)  # Recent 7 days for dedup
    rss_bucket = TokenBucket(rate=1.0, capacity=3.0)
    total_stored = 0

    # Build flat list of (type, key, query, item_meta) — one RSS call each
    fetch_list: list[tuple[str, str, str, _WorkItem]] = []

    for sym, inst in instrument_map.items():
        queries = ASSET_QUERIES.get(sym)
        if not queries:
            continue
        item = _WorkItem("asset", sym, queries, inst["name"], inst["category"], inst.get("sector") or "")
        # Use first query only for ongoing (speed)
        fetch_list.append(("asset", sym, queries[0], item))

    macro_item = _WorkItem("macro", "global", MACRO_QUERIES, "", "", "")
    # Rotate through macro queries across cycles
    macro_q_idx = int(time.time()) % len(MACRO_QUERIES)
    fetch_list.append(("macro", "global", MACRO_QUERIES[macro_q_idx], macro_item))

    for sector_name, queries in SECTOR_QUERIES.items():
        item = _WorkItem("sector", sector_name, queries, "", "", sector_name)
        # Rotate through sector queries
        sq_idx = int(time.time()) % len(queries)
        fetch_list.append(("sector", sector_name, queries[sq_idx], item))

    async with AsyncSession() as rss_session, httpx.AsyncClient(
        timeout=120.0,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=40),
    ) as nim_client:

        # Group articles by work item for batched NIM scoring
        item_articles: dict[str, tuple[_WorkItem, list[dict]]] = {}

        for type_, key, query, item in fetch_list:
            await rss_bucket.acquire()
            url = _google_rss_url_latest(query)
            raw = await _fetch_rss(rss_session, url, datetime.now(timezone.utc).date(), max_articles=50)

            # Dedup
            filtered = []
            for art in raw:
                h = _md5_hash(art["title"])
                if h not in existing_hashes:
                    existing_hashes.add(h)
                    filtered.append(art)
            filtered = _fuzzy_dedup(filtered)

            if filtered:
                item_key = f"{type_}:{key}"
                if item_key in item_articles:
                    item_articles[item_key][1].extend(filtered)
                else:
                    item_articles[item_key] = (item, filtered)

        # Score all collected articles via NIM
        for item_key, (item, articles) in item_articles.items():
            if not articles:
                continue
            logger.info("[ongoing] %s %s — %d new articles to score", item.type_.upper(), item.key, len(articles))
            n = await _run_nim_scoring(nim_client, item, articles, instrument_map)
            total_stored += n

    # Aggregate if we stored anything
    if total_stored > 0:
        await update_macro_sentiment()
        await update_sector_sentiment()
        logger.info("Ongoing cycle: %d new articles stored", total_stored)

    return total_stored


async def run_unified_pipeline() -> None:
    """Main entry point: gap fill on startup, then 10-second ongoing cycles."""
    logger.info("=== Unified Pipeline starting ===")
    instrument_map = await _load_instruments()
    logger.info("Loaded %d instruments", len(instrument_map))

    # Phase 1: Fill gaps for each category's decay window
    logger.info("=== Phase 1: Gap fill (asset=%dd, sector=%dd, macro=%dd) ===",
                LOOKBACK_DAYS["asset"], LOOKBACK_DAYS["sector"], LOOKBACK_DAYS["macro"])
    try:
        await run_gap_fill(instrument_map)
    except Exception:
        logger.exception("Gap fill failed — continuing to ongoing cycles")

    # Phase 2: Ongoing cycles every 10 seconds
    logger.info("=== Phase 2: Ongoing cycle (every %ds) ===", ONGOING_INTERVAL)
    cycle_count = 0
    instrument_refresh_interval = 60  # Refresh instrument list every ~60 cycles (10min)

    while True:
        await asyncio.sleep(ONGOING_INTERVAL)
        try:
            cycle_count += 1
            if cycle_count % instrument_refresh_interval == 0:
                instrument_map = await _load_instruments()
                logger.info("Refreshed instrument list: %d instruments", len(instrument_map))

            await run_ongoing_cycle(instrument_map)
        except Exception:
            logger.exception("Ongoing cycle error")
            await asyncio.sleep(30)
