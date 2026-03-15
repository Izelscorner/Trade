"""Historical news sentiment for backtesting — production-faithful.

Fetches Google News RSS with date operators for three signal types:
  - asset:  per-instrument news (53 instruments with extended set)
  - macro:  global macro/market news (→ macro_sentiment table in production)
  - sector: GICS sector news (→ sector_sentiment table in production)

Runs dual-horizon NIM LLM sentiment (same Qwen 122B model as production),
stores individual articles in backtest_articles table (like production).

Architecture: streaming pipeline for maximum throughput.
  RSS workers → NIM workers → DB writer (all concurrent)

Scoring functions mirror production scorer.py exactly:
  - exponential time-decay with term-specific half-lives
  - consensus dampening (contrarian + priced-in detection) using article counts
  - logarithmic confidence ramp
  - macro/sector: raw_mean × 3.0 to match [-3, 3] scale
"""

import asyncio
import hashlib
import json
import logging
import math
import random
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

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
# All 50 instruments — aligned with production news-fetcher (main.py:61-99)
# Production uses: "{TICKER} stock" + "{name} stock market" per instrument
ASSET_QUERIES: dict[str, list[str]] = {
    # Queries use "intitle:" to force the ticker/name in the headline,
    # ensuring articles are actually about this asset (not just mentioning "stock").
    # ── Original 10 Stocks ──
    "RTX":   ["intitle:RTX stock"],
    "NVDA":  ["intitle:NVDA OR intitle:NVIDIA stock"],
    "GOOGL": ["intitle:GOOGL OR intitle:Alphabet OR intitle:Google stock"],
    "AAPL":  ["intitle:AAPL OR intitle:Apple stock"],
    "TSLA":  ["intitle:TSLA OR intitle:Tesla stock"],
    "PLTR":  ["intitle:PLTR OR intitle:Palantir stock"],
    "LLY":   ["intitle:LLY OR intitle:Eli Lilly stock"],
    "NVO":   ["intitle:NVO OR intitle:Novo Nordisk stock"],
    "WMT":   ["intitle:WMT OR intitle:Walmart stock"],
    "XOM":   ["intitle:XOM OR intitle:Exxon stock"],
    # ── Technology ──
    "MSFT":  ["intitle:MSFT OR intitle:Microsoft stock"],
    "AMD":   ["intitle:AMD stock"],
    "CRM":   ["intitle:CRM OR intitle:Salesforce stock"],
    # ── Financials ──
    "JPM":   ["intitle:JPM OR intitle:JPMorgan stock"],
    "GS":    ["intitle:Goldman Sachs stock"],
    "BAC":   ["intitle:BAC OR intitle:Bank of America stock"],
    "V":     ["intitle:Visa stock"],
    "MA":    ["intitle:Mastercard stock"],
    # ── Healthcare ──
    "JNJ":   ["intitle:JNJ OR intitle:Johnson Johnson stock"],
    "UNH":   ["intitle:UNH OR intitle:UnitedHealth stock"],
    "PFE":   ["intitle:PFE OR intitle:Pfizer stock"],
    # ── Consumer Discretionary ──
    "AMZN":  ["intitle:AMZN OR intitle:Amazon stock"],
    "HD":    ["intitle:Home Depot stock"],
    "NKE":   ["intitle:NKE OR intitle:Nike stock"],
    # ── Consumer Staples ──
    "PG":    ["intitle:Procter Gamble stock"],
    "KO":    ["intitle:Coca-Cola OR intitle:KO stock"],
    "COST":  ["intitle:COST OR intitle:Costco stock"],
    # ── Communication ──
    "META":  ["intitle:META OR intitle:Meta Platforms stock"],
    "DIS":   ["intitle:DIS OR intitle:Disney stock"],
    # ── Energy ──
    "CVX":   ["intitle:CVX OR intitle:Chevron stock"],
    "COP":   ["intitle:COP OR intitle:ConocoPhillips stock"],
    "SLB":   ["intitle:SLB OR intitle:Schlumberger stock"],
    # ── Industrials ──
    "CAT":   ["intitle:CAT OR intitle:Caterpillar stock"],
    "BA":    ["intitle:Boeing stock"],
    "GE":    ["intitle:GE Aerospace OR intitle:GE stock"],
    # ── Materials ──
    "LIN":   ["intitle:LIN OR intitle:Linde stock"],
    "FCX":   ["intitle:FCX OR intitle:Freeport stock"],
    # ── Utilities ──
    "NEE":   ["intitle:NEE OR intitle:NextEra stock"],
    "DUK":   ["intitle:DUK OR intitle:Duke Energy stock"],
    # ── Real Estate ──
    "AMT":   ["intitle:AMT OR intitle:American Tower stock"],
    "PLD":   ["intitle:PLD OR intitle:Prologis stock"],
    # ── ETFs ──
    "IITU":  ["iShares technology ETF", "technology ETF stock market"],
    "SMH":   ["SMH semiconductor ETF", "semiconductor ETF stock"],
    "VOO":   ["VOO S&P 500 ETF", "S&P 500 ETF stock market"],
    "QQQ":   ["QQQ Nasdaq ETF", "Nasdaq 100 ETF stock"],
    "IWM":   ["IWM Russell 2000 ETF", "small cap ETF stock market"],
    "XLF":   ["XLF financial ETF", "financial sector ETF stock"],
    "XLE":   ["XLE energy ETF", "energy sector ETF stock"],
    # ── Commodities ──
    "GOLD":   ["gold price futures market"],
    "OIL":    ["crude oil price futures market"],
    "SILVER": ["silver price futures market"],
    "NATGAS": ["natural gas price futures market"],
}

# Macro — broad queries covering markets, politics, conflicts, economics
# Multiple query variants — rotated across time windows for broader coverage
MACRO_QUERIES: list[str] = [
    # Markets
    "stock market today Wall Street S&P 500",
    "stock market crash rally correction bear bull",
    "Dow Jones Nasdaq index futures trading",
    # Economics / Fed
    "Federal Reserve interest rates inflation economy",
    "US economy jobs unemployment consumer spending",
    "global economy recession GDP trade war tariffs",
    "CPI inflation report consumer prices economic data",
    "Treasury bond yields debt ceiling fiscal policy",
    # Politics
    "US politics economy policy regulation legislation",
    "election economy market impact political uncertainty",
    "sanctions trade policy export controls geopolitics",
    "government shutdown stimulus spending fiscal budget",
    # Conflicts / Geopolitics
    "war conflict military geopolitical risk market",
    "Russia Ukraine war sanctions energy Europe",
    "China Taiwan tensions trade decoupling supply chain",
    "Middle East conflict oil supply disruption",
    "NATO defense spending military buildup security",
]

# Sector — multiple query variants per sector for broader coverage
SECTOR_QUERIES: dict[str, list[str]] = {
    "technology": [
        "technology stocks semiconductor AI chip industry",
        "tech sector earnings FAANG cloud computing",
        "artificial intelligence machine learning tech companies",
        "cybersecurity software SaaS tech regulation antitrust",
    ],
    "financials": [
        "banking stocks financial sector interest rates Wall Street",
        "bank earnings JPMorgan Goldman Sachs Morgan Stanley",
        "financial regulation banking crisis credit lending",
        "insurance fintech payments financial services",
    ],
    "healthcare": [
        "healthcare stocks biotech pharma drug FDA",
        "FDA approval drug trial clinical biotech pharmaceutical",
        "healthcare reform drug pricing insurance Medicare",
        "medical devices hospital health sector earnings",
    ],
    "consumer_discretionary": [
        "consumer stocks retail spending automotive Tesla Amazon",
        "retail sales consumer confidence discretionary spending",
        "e-commerce online shopping holiday season consumer",
        "luxury goods travel leisure entertainment spending",
    ],
    "consumer_staples": [
        "consumer staples stocks food beverage Walmart Costco",
        "grocery food prices consumer goods inflation staples",
        "household products Procter Gamble Coca Cola Pepsi",
        "consumer staples sector defensive stocks dividend",
    ],
    "communication": [
        "media stocks streaming telecom Meta Google Disney",
        "social media advertising digital media regulation",
        "streaming wars content subscription telecom 5G",
        "communication sector earnings media industry",
    ],
    "energy": [
        "energy stocks oil gas prices OPEC Exxon Chevron",
        "oil prices crude OPEC production supply demand",
        "renewable energy solar wind transition clean",
        "natural gas pipeline energy infrastructure drilling",
    ],
    "industrials": [
        "industrial stocks manufacturing defense Boeing Caterpillar",
        "defense spending military contracts aerospace industrial",
        "manufacturing supply chain factory orders PMI",
        "transportation logistics infrastructure spending",
    ],
    "materials": [
        "materials stocks mining metals copper lithium gold",
        "commodity prices mining sector raw materials",
        "steel aluminum rare earth metals supply chain",
        "chemical industry materials sector construction",
    ],
    "utilities": [
        "utilities stocks power electricity grid renewable energy",
        "electric utility regulation rate increase power grid",
        "nuclear energy power plant infrastructure",
        "clean energy utility sector dividend regulated",
    ],
    "real_estate": [
        "real estate stocks REIT housing market property mortgage",
        "housing market prices mortgage rates real estate",
        "commercial real estate office vacancy REIT",
        "property market construction home sales housing",
    ],
}

# ─── LLM prompt builders — exact copies of production prompts.py ─────────────
# System messages (production prompts.py lines 20-25, 230-231)
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

# Category-based role templates (production prompts.py lines 29-33)
_CATEGORY_ROLES = {
    "stock": "You are a Wall Street equity analyst covering {name}. You think like both a day-trader (short-term price action) and a fundamental investor (long-term value).",
    "etf": "You are a Wall Street ETF analyst covering {name}. You understand constituent-level impacts propagate to the ETF with weight-proportional magnitude.",
    "commodity": "You are a commodity futures trader at a Western bank trading {name}. You ONLY care about the PRICE of this commodity going UP or DOWN. Supply disruptions = PRICE UP = positive. Demand destruction = PRICE DOWN = negative. You distinguish between spot-price catalysts (short-term) and supply/demand structural shifts (long-term).",
}
_CATEGORY_DESCS = {
    "stock": "{name} stock price on US exchanges",
    "etf": "{name} ETF price on US exchanges",
    "commodity": "{name} futures price",
}


def _clean_name(name: str, category: str) -> str:
    if category == "commodity":
        return name.replace("Futures", "").strip()
    return name


def _asset_role(name: str, category: str) -> str:
    """Build role string — production prompts.py get_role()."""
    clean = _clean_name(name, category)
    template = _CATEGORY_ROLES.get(category, _CATEGORY_ROLES["stock"])
    return template.format(name=clean)


def _asset_desc(name: str, category: str) -> str:
    """Build asset description — production prompts.py get_asset_description()."""
    clean = _clean_name(name, category)
    template = _CATEGORY_DESCS.get(category, _CATEGORY_DESCS["stock"])
    return template.format(name=clean)


def _build_asset_prompt(role: str, articles: list[dict], asset_desc: str) -> str:
    """Production batch_sentiment_prompt() — exact prompt structure."""
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
    """Production batch_macro_sentiment_prompt() — exact prompt structure."""
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
    """Production batch_sector_sentiment_prompt() — exact prompt structure."""
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


# ─── Token Bucket Rate Limiter ───────────────────────────────────────────────
@dataclass
class TokenBucket:
    """Token bucket rate limiter — allows bursts while capping sustained rate."""
    rate: float
    capacity: float
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)
    _lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _consecutive_429s: int = field(init=False, default=0)

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

    async def on_429(self) -> float:
        async with self._lock:
            self._consecutive_429s += 1
            backoff = min(40.0, 5.0 * (2 ** (self._consecutive_429s - 1)))
            backoff += random.uniform(0, 3)
            self._tokens = max(0.0, self._tokens - 2.0)
            return backoff

    async def on_success(self) -> None:
        async with self._lock:
            self._consecutive_429s = 0


# ─── NIM API (rate-limited, system+user messages like production) ────────────
_NIM_BATCH = 100  # articles per NIM call (tested: 100 articles = ~56s, barely slower than 50)
_NIM_CONCURRENCY = 40  # max concurrent in-flight NIM calls


# Global semaphore: limits concurrent NIM calls (no rate delay, just concurrency cap)
_nim_semaphore: asyncio.Semaphore | None = None


def _get_nim_semaphore() -> asyncio.Semaphore:
    global _nim_semaphore
    if _nim_semaphore is None:
        _nim_semaphore = asyncio.Semaphore(_NIM_CONCURRENCY)
    return _nim_semaphore


async def _call_nim(
    client: httpx.AsyncClient,
    prompt: str,
    system_msg: str = "",
    retries: int = 3,
) -> list[dict]:
    """POST to NIM /chat/completions with system+user messages (like production).

    No rate delay — fires immediately. Concurrency limited by global semaphore.
    """
    messages = []
    if system_msg:
        messages.append({"role": "system", "content": system_msg})
    messages.append({"role": "user", "content": prompt})

    sem = _get_nim_semaphore()

    for attempt in range(retries):
        async with sem:
            try:
                resp = await client.post(
                    f"{NIM_BASE_URL}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {NIM_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": NIM_MODEL,
                        "messages": messages,
                        "temperature": 0.0,
                        "max_tokens": 6000,
                    },
                    timeout=120.0,
                )
                if resp.status_code == 429:
                    wait = min(30.0, 3.0 * (2 ** attempt)) + random.uniform(0, 2)
                    logger.warning("NIM 429 (attempt %d), backoff %.1fs", attempt + 1, wait)
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"].strip()
                # Try to extract results array from JSON response
                # Production format: {"results": [...]}
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict) and "results" in parsed:
                        return parsed["results"]
                    if isinstance(parsed, list):
                        return parsed
                except json.JSONDecodeError:
                    pass
                # Fallback: regex extract array
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
            await asyncio.sleep(1.0)
    return []


# ─── Deduplication (MD5 + fuzzy matching like production) ─────────────────────
def _md5_hash(text: str) -> str:
    """MD5 hash of normalized text — mirrors production news-fetcher store.py."""
    return hashlib.md5(text.strip().lower().encode()).hexdigest()


def _fuzzy_dedup(articles: list[dict], threshold: float = 0.85) -> list[dict]:
    """Deduplicate articles using MD5 URL hash + fuzzy title matching.

    Like production's rapidfuzz-based dedup in store.py.
    Falls back to prefix matching if rapidfuzz not available.
    """
    seen_hashes: set[str] = set()
    unique: list[dict] = []

    # Try rapidfuzz first (production uses it)
    try:
        from rapidfuzz import fuzz
        has_rapidfuzz = True
    except ImportError:
        has_rapidfuzz = False

    seen_titles: list[str] = []

    for art in articles:
        # MD5 dedup on title (we don't have URLs from RSS)
        title_hash = _md5_hash(art["title"])
        if title_hash in seen_hashes:
            continue
        seen_hashes.add(title_hash)

        # Fuzzy title matching
        title_norm = art["title"].lower().strip()
        is_dup = False
        if has_rapidfuzz:
            for seen in seen_titles:
                if fuzz.ratio(title_norm, seen) >= threshold * 100:
                    is_dup = True
                    break
                if len(title_norm) > 30 and fuzz.partial_ratio(title_norm, seen) >= 90:
                    is_dup = True
                    break
        else:
            # Fallback: prefix matching
            prefix = title_norm[:80]
            for seen in seen_titles:
                if seen.startswith(prefix) or prefix.startswith(seen[:80]):
                    is_dup = True
                    break

        if not is_dup:
            seen_titles.append(title_norm)
            art["url_hash"] = title_hash
            unique.append(art)

    return unique


def _parse_nim_results(
    chunk: list[dict],
    responses: list[dict],
) -> list[dict]:
    """Parse NIM responses into article-level result dicts.

    Production format: {"id": "1", "short_sentiment": "positive",
                        "short_confidence": 0.8, "long_sentiment": "neutral",
                        "long_confidence": 0.5}
    """
    # Build lookup by id (string or int)
    scored = {}
    for r in responses:
        if not isinstance(r, dict):
            continue
        rid = r.get("id", r.get("i"))
        if rid is not None:
            scored[str(rid)] = r

    results: list[dict] = []
    for i, art in enumerate(chunk):
        sr = scored.get(str(i + 1), {})

        # Parse short-term sentiment (production format)
        ss = sr.get("short_sentiment", sr.get("ss", "neutral"))
        if ss not in VALID_LABELS:
            ss = "neutral"
        sc = float(sr.get("short_confidence", sr.get("sc", 0.5)))
        sc = max(0.0, min(1.0, sc))

        # Parse long-term sentiment (production format)
        ls = sr.get("long_sentiment", sr.get("ls", "neutral"))
        if ls not in VALID_LABELS:
            ls = "neutral"
        lc = float(sr.get("long_confidence", sr.get("lc", 0.5)))
        lc = max(0.0, min(1.0, lc))

        results.append({
            "title": art["title"],
            "summary": art.get("summary", ""),
            "url_hash": art.get("url_hash", _md5_hash(art["title"])),
            "published_at": art.get("published_at"),
            "short_label": ss,
            "long_label": ls,
            "short_confidence": round(sc, 3),
            "long_confidence": round(lc, 3),
        })
    return results


# ─── RSS fetch ────────────────────────────────────────────────────────────────
def _google_rss_url(query: str, start: date, end: date) -> str:
    """Google News RSS with date range (exclusive bounds, so we pad ±1 day)."""
    date_from = (start - timedelta(days=1)).isoformat()
    date_to = (end + timedelta(days=1)).isoformat()
    q = query.replace(" ", "+").replace('"', "%22")
    return (
        f"https://news.google.com/rss/search"
        f"?q={q}+after:{date_from}+before:{date_to}"
        f"&hl=en-US&gl=US&ceid=US:en"
    )


def _combine_queries(queries: list[str]) -> str:
    """Combine multiple search queries into a single OR query for Google News.

    No quotes around terms — broader matching yields more articles.
    """
    if len(queries) == 1:
        return queries[0]
    return " OR ".join(queries)


def _nearest_weekday(d: date) -> date:
    """Map weekend dates to nearest weekday (Sat→Fri, Sun→Mon)."""
    if d.weekday() == 5:  # Saturday
        return d - timedelta(days=1)
    if d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


_RSS_SPLIT_THRESHOLD = 90   # if we get >=90 articles, range is probably truncated
_RSS_MIN_RANGE_DAYS = 7     # don't split below 1 week


async def _adaptive_fetch(
    session,  # curl_cffi AsyncSession
    query: str,
    start: date,
    end: date,
    rss_bucket: "TokenBucket",
    depth: int = 0,
    max_depth: int = 99,
    article_queue: asyncio.Queue | None = None,
) -> list[dict]:
    """Fetch articles for a date range, recursively splitting if too many results.

    Tries the full range first. If Google returns near-max articles (>=90),
    the range is likely truncated — split in half and fetch both halves.
    max_depth limits recursion (e.g. 2 for sectors = max 4 RSS calls).

    If article_queue is provided, leaf-node articles are pushed to the queue
    immediately (enabling concurrent NIM scoring while RSS continues).
    Returns the full collected list as well.
    """
    if start > end:
        return []

    mid_date = start + (end - start) // 2
    range_days = (end - start).days
    await rss_bucket.acquire()
    url = _google_rss_url(query, start, end)
    logger.debug("  RSS fetch: %s→%s (%dd) depth=%d q=%s",
                 start, end, range_days, depth, query[:60])
    articles = await _fetch_rss(session, url, mid_date, max_articles=100)
    logger.debug("  RSS result: %d articles for %s→%s", len(articles), start, end)

    # If we got near-max results AND range is wide enough to split, recurse
    if len(articles) >= _RSS_SPLIT_THRESHOLD and range_days > _RSS_MIN_RANGE_DAYS and depth < max_depth:
        mid = start + timedelta(days=range_days // 2)
        logger.info("  RSS splitting: %d articles in %dd range → two halves (%s | %s)",
                     len(articles), range_days, mid, mid + timedelta(days=1))
        left = await _adaptive_fetch(session, query, start, mid, rss_bucket, depth + 1, max_depth, article_queue)
        right = await _adaptive_fetch(session, query, mid + timedelta(days=1), end,
                                       rss_bucket, depth + 1, max_depth, article_queue)
        return left + right

    # Leaf node — push to queue for concurrent NIM processing
    if article_queue is not None and articles:
        await article_queue.put(articles)

    return articles


async def _rotated_fetch(
    session,
    queries: list[str],
    start: date,
    end: date,
    rss_bucket: "TokenBucket",
    max_depth: int = 5,
) -> list[dict]:
    """Rotate through query variants across time windows for maximum coverage.

    Splits the full date range into N chunks (one per query variant),
    then fetches each chunk with its assigned query. After the first pass,
    does a second pass with different query assignments for the same ranges
    to capture articles the first query missed.

    This is critical for macro/sector because Google News returns different
    articles for "stock market crash rally" vs "Federal Reserve interest rates".
    """
    if not queries:
        return []

    total_days = (end - start).days
    if total_days <= 0:
        return []

    all_articles: list[dict] = []
    seen_hashes: set[str] = set()

    # Split range into chunks of ~90 days (quarterly), rotate queries
    chunk_days = max(30, min(90, total_days // max(len(queries), 1)))
    chunks: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)

    # Pass 1: Each chunk gets a primary query (round-robin)
    # Pass 2: Each chunk gets the next query in rotation
    num_passes = min(2, len(queries))

    for pass_idx in range(num_passes):
        for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
            query_idx = (chunk_idx + pass_idx) % len(queries)
            query = queries[query_idx]

            articles = await _adaptive_fetch(
                session, query, chunk_start, chunk_end,
                rss_bucket, max_depth=max_depth,
            )

            # Dedup across passes
            for art in articles:
                h = _md5_hash(art["title"])
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    all_articles.append(art)

        if pass_idx == 0:
            logger.info(
                "  Rotation pass 1: %d articles from %d chunks × %d queries",
                len(all_articles), len(chunks), 1,
            )

    logger.info(
        "  Rotation complete: %d total unique articles from %d passes × %d chunks",
        len(all_articles), num_passes, len(chunks),
    )
    return all_articles


# Chrome TLS impersonation profiles for curl_cffi
_IMPERSONATE_PROFILES = ["chrome120", "chrome119", "chrome116", "chrome110"]

# Consecutive 503 tracker for adaptive backoff
_rss_consecutive_503s = 0
_rss_503_lock = asyncio.Lock()


async def _fetch_rss(
    session,  # curl_cffi.requests.AsyncSession
    url: str,
    fallback_date: date,
    max_articles: int = 60,
    retries: int = 4,
) -> list[dict]:
    """Fetch Google News RSS using curl_cffi with Chrome TLS impersonation.

    Extracts pubDate from each RSS item for precise article-level timestamps.
    fallback_date is used when pubDate is missing (e.g., mid-week Wednesday).
    """
    global _rss_consecutive_503s

    for attempt in range(retries):
        try:
            profile = random.choice(_IMPERSONATE_PROFILES)
            resp = await session.get(
                url,
                timeout=15.0,
                allow_redirects=True,
                impersonate=profile,
            )

            if resp.status_code == 503:
                async with _rss_503_lock:
                    _rss_consecutive_503s += 1
                    consec = _rss_consecutive_503s
                if consec > 100:
                    wait = 120.0 + random.uniform(0, 30)
                    if consec % 100 == 0:
                        logger.warning(
                            "Google News IP blocked — %d consecutive 503s. "
                            "Waiting %.0fs between attempts.",
                            consec, wait,
                        )
                elif consec > 50:
                    wait = 60.0 + random.uniform(0, 10)
                elif consec > 20:
                    wait = 30.0 + random.uniform(0, 5)
                else:
                    wait = min(30.0, 3.0 * (2 ** attempt)) + random.uniform(0, 2)
                if consec <= 20 and attempt == 0 and consec % 10 == 0:
                    logger.warning(
                        "Google News 503 — %d consecutive. Backing off %.0fs.",
                        consec, wait,
                    )
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = 30.0 + random.uniform(0, 10)
                logger.warning("Google News 429 rate limit. Waiting %.0fs.", wait)
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()

            # Success — reset consecutive counter
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

                # Parse pubDate for precise article timestamp
                published_at = None
                if pub_el is not None and pub_el.text:
                    try:
                        published_at = parsedate_to_datetime(pub_el.text)
                    except Exception:
                        pass

                # Fallback: use fallback_date noon UTC
                if published_at is None:
                    published_at = datetime(
                        fallback_date.year, fallback_date.month, fallback_date.day,
                        12, 0, 0, tzinfo=timezone.utc,
                    )

                if len(title) >= 15:
                    articles.append({
                        "title": title,
                        "summary": summary,
                        "published_at": published_at,
                    })
                if len(articles) >= max_articles:
                    break
            return articles
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2.0 ** attempt)
            else:
                logger.warning("RSS fetch failed after %d attempts: %s — %s", retries, url[:80], e)
    return []


# ─── DB helpers ──────────────────────────────────────────────────────────────
async def _load_cached_days(type_: str) -> set[tuple[str, date]]:
    """Return set of (key, date) already stored in backtest_articles for this type."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT DISTINCT key, date FROM backtest_articles WHERE type = :t"),
            {"t": type_},
        )
        return {(r.key, r.date) for r in result.fetchall()}


async def _store_articles(
    type_: str,
    key: str,
    d: date,
    articles: list[dict],
) -> None:
    """Store individual articles in backtest_articles table.

    Each article has: title, summary, url_hash, published_at,
    short_label, long_label, short_confidence, long_confidence.
    """
    async with async_session() as session:
        if not articles:
            # Store a placeholder so we don't re-fetch this day
            await session.execute(
                text("""
                    INSERT INTO backtest_articles
                        (type, key, date, title, summary, url_hash,
                         published_at, short_label, long_label,
                         short_confidence, long_confidence)
                    VALUES (:type, :key, :date, :title, :summary, :url_hash,
                            :pub, :sl, :ll, :sc, :lc)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "type": type_, "key": key, "date": d,
                    "title": "[no articles]", "summary": "",
                    "url_hash": _md5_hash(f"{type_}:{key}:{d}:empty"),
                    "pub": datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc),
                    "sl": "neutral", "ll": "neutral",
                    "sc": 0.0, "lc": 0.0,
                },
            )
        else:
            for art in articles:
                pub = art.get("published_at")
                if pub and not pub.tzinfo:
                    pub = pub.replace(tzinfo=timezone.utc)
                await session.execute(
                    text("""
                        INSERT INTO backtest_articles
                            (type, key, date, title, summary, url_hash,
                             published_at, short_label, long_label,
                             short_confidence, long_confidence)
                        VALUES (:type, :key, :date, :title, :summary, :url_hash,
                                :pub, :sl, :ll, :sc, :lc)
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "type": type_, "key": key, "date": d,
                        "title": art["title"][:500],
                        "summary": (art.get("summary") or "")[:500],
                        "url_hash": art.get("url_hash", _md5_hash(art["title"])),
                        "pub": pub,
                        "sl": art.get("short_label", "neutral"),
                        "ll": art.get("long_label", "neutral"),
                        "sc": art.get("short_confidence", 0.5),
                        "lc": art.get("long_confidence", 0.5),
                    },
                )
        await session.commit()


async def _batch_store_articles(
    items: list[tuple[str, str, date, list[dict]]],
) -> None:
    """Batch store multiple (type, key, date, articles) in one transaction."""
    if not items:
        return
    async with async_session() as session:
        for type_, key, d, articles in items:
            if not articles:
                await session.execute(
                    text("""
                        INSERT INTO backtest_articles
                            (type, key, date, title, summary, url_hash,
                             published_at, short_label, long_label,
                             short_confidence, long_confidence)
                        VALUES (:type, :key, :date, :title, :summary, :url_hash,
                                :pub, :sl, :ll, :sc, :lc)
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "type": type_, "key": key, "date": d,
                        "title": "[no articles]", "summary": "",
                        "url_hash": _md5_hash(f"{type_}:{key}:{d}:empty"),
                        "pub": datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc),
                        "sl": "neutral", "ll": "neutral",
                        "sc": 0.0, "lc": 0.0,
                    },
                )
            else:
                for art in articles:
                    pub = art.get("published_at")
                    if pub and not pub.tzinfo:
                        pub = pub.replace(tzinfo=timezone.utc)
                    await session.execute(
                        text("""
                            INSERT INTO backtest_articles
                                (type, key, date, title, summary, url_hash,
                                 published_at, short_label, long_label,
                                 short_confidence, long_confidence)
                            VALUES (:type, :key, :date, :title, :summary, :url_hash,
                                    :pub, :sl, :ll, :sc, :lc)
                            ON CONFLICT DO NOTHING
                        """),
                        {
                            "type": type_, "key": key, "date": d,
                            "title": art["title"][:500],
                            "summary": (art.get("summary") or "")[:500],
                            "url_hash": art.get("url_hash", _md5_hash(art["title"])),
                            "pub": pub,
                            "sl": art.get("short_label", "neutral"),
                            "ll": art.get("long_label", "neutral"),
                            "sc": art.get("short_confidence", 0.5),
                            "lc": art.get("long_confidence", 0.5),
                        },
                    )
        await session.commit()


# ─── Main fetch orchestrator (pipeline architecture) ─────────────────────────
def _all_weekdays(start: date, end: date) -> list[date]:
    """All weekdays (Mon–Fri) between start and end inclusive."""
    days, cur = [], start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


@dataclass
class _WorkItem:
    """One (type, key) group: adaptively fetch full date range → score via NIM → store per-day."""
    type_: str          # 'asset', 'macro', 'sector'
    key: str            # symbol, 'global', or sector_name
    queries: list[str]  # RSS search queries
    name: str           # instrument name for role building
    category: str       # 'stock', 'etf', 'commodity' (for asset); '' for macro/sector
    sector: str         # sector name (for sector type)
    uncached_days: list[date] = field(default_factory=list)  # all days needing data
    articles: list[dict] = field(default_factory=list)


async def _score_one_chunk(
    chunk: list[dict],
    build_prompt_fn,
    system_msg: str,
    nim_client,
    item: "_WorkItem",
    group_idx: int,
    group_total: int,
) -> list[dict]:
    """Score a single chunk of articles via NIM. Returns list of scored article dicts."""
    numbered = [
        {"i": i + 1, "title": a["title"],
         "summary": a.get("summary", ""),
         "published_at": a.get("published_at"),
         "url_hash": a.get("url_hash")}
        for i, a in enumerate(chunk)
    ]
    prompt = build_prompt_fn(numbered)
    try:
        responses = await _call_nim(nim_client, prompt, system_msg=system_msg)
        return _parse_nim_results(chunk, responses)
    except Exception as e:
        logger.warning("NIM error [%s %s]: %s", item.type_, item.key, e)
        return []


async def fetch_all_historical_sentiment(
    instruments: list[dict],
    start_date: date,
    end_date: date,
    concurrency: int = 3,
) -> None:
    """Fetch and store article-level sentiment for all assets, macro, and 11 sectors.

    Sequential batch model — paced by NIM, not RSS:
      1. Fetch RSS for one (type, key) group using adaptive date ranges
      2. Score all articles via NIM (the real bottleneck at 40 req/min)
      3. Store results in DB
      4. Move to next group
    No concurrent RSS hammering → no Google 503s.
    OR-combined queries (2→1) and larger NIM batch (12) for efficiency.
    Idempotent — skips (type, key) groups where all days are cached.
    """
    trading_days = _all_weekdays(start_date, end_date)
    logger.info(
        "Sentiment fetch: %d trading days, %s → %s",
        len(trading_days), start_date, end_date,
    )

    # Load groups that already have real articles (skip on restart)
    logger.info("Loading existing article data...")
    async with async_session() as s:
        r = await s.execute(
            text("""
                SELECT type, key, COUNT(*) as cnt
                FROM backtest_articles
                WHERE title != '[no articles]'
                GROUP BY type, key
            """)
        )
        completed_groups: dict[tuple[str, str], int] = {
            (row.type, row.key): row.cnt for row in r.fetchall()
        }
    logger.info(
        "Already completed: %d groups with real articles",
        len(completed_groups),
    )

    # Build work list — skip groups that already have real articles
    work: list[_WorkItem] = []
    total_uncached_days = 0
    skipped = 0

    # --- Asset ---
    for inst in instruments:
        sym = inst["symbol"]
        cat = inst["category"]
        name = inst["name"]
        if ("asset", sym) in completed_groups:
            skipped += 1
            continue
        queries = ASSET_QUERIES.get(sym, [f"intitle:{sym} OR intitle:{name} stock"])
        work.append(_WorkItem("asset", sym, queries, name, cat, "", trading_days[:]))
        total_uncached_days += len(trading_days)

    # --- Macro ---
    if ("macro", "global") not in completed_groups:
        work.append(_WorkItem("macro", "global", MACRO_QUERIES, "", "", "", trading_days[:]))
        total_uncached_days += len(trading_days)
    else:
        skipped += 1

    # --- Sector ---
    for sector_name, queries in SECTOR_QUERIES.items():
        if ("sector", sector_name) not in completed_groups:
            work.append(_WorkItem("sector", sector_name, queries, "", "", sector_name, trading_days[:]))
            total_uncached_days += len(trading_days)
        else:
            skipped += 1

    if skipped:
        logger.info("Skipped %d already-completed groups.", skipped)

    total = len(work)
    logger.info(
        "Work items: %d (type,key) groups covering %d uncached days.",
        total, total_uncached_days,
    )
    if total == 0:
        logger.info("All sentiment data already cached.")
        return

    # ── RSS sequential, NIM concurrent across all groups ────────────────
    #
    # RSS: one group at a time, 2s delay between calls, no 503 risk
    # NIM: all fired tasks run concurrently (semaphore caps at 40 in-flight)
    # No waiting between groups — RSS moves to next group immediately,
    # NIM tasks from previous groups keep running in background.
    #
    from curl_cffi.requests import AsyncSession
    import time as _time

    rss_bucket = TokenBucket(rate=1.0, capacity=3.0)    # 1 RSS call per 1s
    nim_calls = 0
    total_articles_scored = 0
    all_nim_tasks: list[asyncio.Task] = []  # NIM tasks across ALL groups
    group_results: dict[int, list[dict]] = {}  # idx → scored articles
    pipeline_start = _time.monotonic()

    async with AsyncSession() as rss_session, httpx.AsyncClient(
        headers={
            "Authorization": f"Bearer {NIM_API_KEY}",
            "Content-Type": "application/json",
        },
        timeout=120.0,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=40),
    ) as nim_client:

        # ── Phase 1: Sequential RSS, fire NIM tasks immediately ──────
        for idx, item in enumerate(work):
            range_start = min(item.uncached_days)
            range_end = max(item.uncached_days)
            uncached_set = set(item.uncached_days)
            _max_depth = {"sector": 5, "macro": 6}.get(item.type_, 5)

            # For macro/sector: rotate through query variants across time windows
            # For assets: combine all queries with OR (usually 1-2 queries)
            use_rotation = item.type_ in ("macro", "sector") and len(item.queries) > 1

            if use_rotation:
                logger.info(
                    "[%d/%d] RSS ── %s %s ── %s→%s (%d days, %d query variants, rotating)",
                    idx + 1, total, item.type_.upper(), item.key,
                    range_start, range_end, len(item.uncached_days), len(item.queries),
                )
            else:
                combined_q = _combine_queries(item.queries)
                logger.info(
                    "[%d/%d] RSS ── %s %s ── %s→%s (%d days, query: %s)",
                    idx + 1, total, item.type_.upper(), item.key,
                    range_start, range_end, len(item.uncached_days),
                    combined_q[:60],
                )

            # Build prompt helpers for this group
            if item.type_ == "asset":
                _role = _asset_role(item.name, item.category)
                _desc = _asset_desc(item.name, item.category)
                _sys = _SENTIMENT_SYSTEM
                def _build_prompt(numbered, r=_role, d=_desc):
                    return _build_asset_prompt(r, numbered, d)
            elif item.type_ == "macro":
                _sys = _MACRO_SYSTEM
                def _build_prompt(numbered):
                    return _build_macro_prompt(numbered)
            else:
                _sys = _SECTOR_SYSTEM
                _sector = item.sector
                def _build_prompt(numbered, s=_sector):
                    return _build_sector_prompt(numbered, s)

            # RSS fetch (sequential, paced)
            rss_start = _time.monotonic()
            try:
                if use_rotation:
                    # Query rotation: split date range into chunks, use different
                    # query for each chunk. This maximizes coverage since Google
                    # returns different articles for different phrasings.
                    raw_articles = await _rotated_fetch(
                        rss_session, item.queries, range_start, range_end,
                        rss_bucket, max_depth=_max_depth,
                    )
                else:
                    raw_articles = await _adaptive_fetch(
                        rss_session, combined_q, range_start, range_end,
                        rss_bucket, max_depth=_max_depth,
                    )
            except Exception as e:
                logger.warning("[%d/%d] RSS error [%s %s]: %s",
                               idx + 1, total, item.type_, item.key, e)
                raw_articles = []
            rss_elapsed = _time.monotonic() - rss_start

            # Dedup and filter to uncached days
            seen_hashes: set[str] = set()
            filtered: list[dict] = []
            for art in raw_articles:
                h = _md5_hash(art["title"])
                if h in seen_hashes:
                    continue
                seen_hashes.add(h)
                pub = art.get("published_at")
                if pub:
                    art_date = _nearest_weekday(pub.date() if hasattr(pub, 'date') else pub)
                    if art_date in uncached_set:
                        filtered.append(art)
                else:
                    filtered.append(art)

            logger.info(
                "[%d/%d] RSS done: %d raw → %d unique in %.1fs. Firing %d NIM tasks.",
                idx + 1, total, len(raw_articles), len(filtered), rss_elapsed,
                (len(filtered) + _NIM_BATCH - 1) // _NIM_BATCH if filtered else 0,
            )

            # Fire NIM tasks immediately — each task scores AND stores to DB
            if filtered:
                for chunk_start in range(0, len(filtered), _NIM_BATCH):
                    chunk = filtered[chunk_start:chunk_start + _NIM_BATCH]
                    _item = item
                    _idx = idx
                    _prompt_fn = _build_prompt
                    _system = _sys

                    async def _nim_task(
                        c=chunk, pfn=_prompt_fn, sys_=_system,
                        it=_item, i=_idx,
                    ):
                        nonlocal total_articles_scored
                        results = await _score_one_chunk(
                            c, pfn, sys_, nim_client, it, i + 1, total,
                        )
                        if not results:
                            return 0

                        # Store to DB immediately
                        uncached_set_local = set(it.uncached_days)
                        day_buckets: dict[date, list[dict]] = {}
                        for art in results:
                            pub = art.get("published_at")
                            if pub:
                                art_date = _nearest_weekday(
                                    pub.date() if hasattr(pub, 'date') else pub
                                )
                                if art_date not in uncached_set_local:
                                    art_date = min(it.uncached_days,
                                                   key=lambda dd: abs((dd - art_date).days))
                            else:
                                art_date = it.uncached_days[0]
                            day_buckets.setdefault(art_date, []).append(art)

                        store_items = [(it.type_, it.key, d, arts)
                                       for d, arts in day_buckets.items()]
                        await _batch_store_articles(store_items)
                        total_articles_scored += len(results)

                        logger.info(
                            "[%s %s] NIM+DB: %d articles scored & stored",
                            it.type_.upper(), it.key, len(results),
                        )
                        return len(results)

                    all_nim_tasks.append(asyncio.create_task(_nim_task()))
                    nim_calls += 1

            # Log in-flight NIM tasks
            pending = sum(1 for t in all_nim_tasks if not t.done())
            if (idx + 1) % 10 == 0 or idx + 1 == total:
                logger.info(
                    "═══ RSS %d/%d done — %d NIM tasks fired (%d pending, %d scored) ═══",
                    idx + 1, total, len(all_nim_tasks), pending, total_articles_scored,
                )

        # ── Phase 2: Wait for remaining NIM tasks ────────────────────
        pending = sum(1 for t in all_nim_tasks if not t.done())
        if pending:
            logger.info("RSS complete. Waiting for %d pending NIM tasks...", pending)
        await asyncio.gather(*all_nim_tasks)

        # ── Phase 3: Store placeholders for days with no articles ────
        logger.info("Storing placeholders for empty days...")
        for idx, item in enumerate(work):
            # Find which days already have articles
            async with async_session() as s:
                r = await s.execute(
                    text("SELECT DISTINCT date FROM backtest_articles WHERE type=:t AND key=:k"),
                    {"t": item.type_, "k": item.key},
                )
                existing_days = {row.date for row in r.fetchall()}

            empty_days = [d for d in item.uncached_days if d not in existing_days]
            if empty_days:
                store_items = [(item.type_, item.key, d, []) for d in empty_days]
                await _batch_store_articles(store_items)

    # ── Phase 4: Derive macro/sector from asset articles ─────────
    logger.info("Phase 4: Deriving macro/sector sentiment from asset articles...")
    macro_derived = await derive_macro_from_assets(start_date, end_date)
    sector_derived = await derive_sector_from_assets(start_date, end_date)
    logger.info(
        "Derivation complete: %d macro + %d sector synthetic articles",
        macro_derived, sector_derived,
    )

    elapsed = _time.monotonic() - pipeline_start
    logger.info(
        "Sentiment fetch complete. "
        "%d groups, %d NIM calls, %d articles scored, %d days, "
        "%d derived macro, %d derived sector — %.0fs (%.1f min)",
        total, nim_calls, total_articles_scored, total_uncached_days,
        macro_derived, sector_derived,
        elapsed, elapsed / 60,
    )


# ─── Derive macro/sector sentiment from asset articles ────────────────────────
# When Google News returns few results for broad queries, we can infer macro and
# sector sentiment from the 57K+ scored asset articles. This produces synthetic
# articles that fill gaps in the macro/sector signal.

# Instrument → GICS sector mapping (matches production instruments table)
_INSTRUMENT_SECTOR: dict[str, str] = {
    "AAPL": "technology", "AMD": "technology", "CRM": "technology",
    "MSFT": "technology", "NVDA": "technology", "PLTR": "technology",
    "IITU": "technology", "QQQ": "technology", "SMH": "technology",
    "JPM": "financials", "GS": "financials", "BAC": "financials",
    "V": "financials", "MA": "financials", "XLF": "financials",
    "JNJ": "healthcare", "UNH": "healthcare", "PFE": "healthcare",
    "LLY": "healthcare", "NVO": "healthcare",
    "AMZN": "consumer_discretionary", "HD": "consumer_discretionary",
    "NKE": "consumer_discretionary", "TSLA": "consumer_discretionary",
    "PG": "consumer_staples", "KO": "consumer_staples",
    "COST": "consumer_staples", "WMT": "consumer_staples",
    "META": "communication", "DIS": "communication", "GOOGL": "communication",
    "XOM": "energy", "CVX": "energy", "COP": "energy",
    "SLB": "energy", "OIL": "energy", "NATGAS": "energy", "XLE": "energy",
    "RTX": "industrials", "CAT": "industrials", "BA": "industrials", "GE": "industrials",
    "LIN": "materials", "FCX": "materials", "GOLD": "materials", "SILVER": "materials",
    "NEE": "utilities", "DUK": "utilities",
    "AMT": "real_estate", "PLD": "real_estate",
}
# Broad-market ETFs have no sector
_NO_SECTOR = {"VOO", "IWM"}


async def derive_macro_from_assets(
    start_date: date,
    end_date: date,
) -> int:
    """Derive macro sentiment from asset articles.

    For each trading day, aggregates all asset articles' sentiment into a
    synthetic macro article. The logic: if most stocks are negative, macro
    sentiment is negative. Uses median sentiment to avoid outlier bias.

    Only creates entries for days where we have ≥5 asset articles and
    no existing real macro articles.

    Returns count of synthetic articles created.
    """
    trading_days = _all_weekdays(start_date, end_date)

    # Load all real asset articles
    async with async_session() as s:
        r = await s.execute(text("""
            SELECT date, short_label, long_label, short_confidence, long_confidence
            FROM backtest_articles
            WHERE type = 'asset' AND title != '[no articles]'
            ORDER BY date
        """))
        asset_rows = r.fetchall()

        # Load existing real macro days
        r2 = await s.execute(text("""
            SELECT DISTINCT date FROM backtest_articles
            WHERE type = 'macro' AND title != '[no articles]'
        """))
        existing_macro_days = {row.date for row in r2.fetchall()}

    # Group asset articles by date
    by_date: dict[date, list] = {}
    for row in asset_rows:
        by_date.setdefault(row.date, []).append(row)

    created = 0
    store_items: list[tuple[str, str, date, list[dict]]] = []

    for d in trading_days:
        # Don't overwrite real macro articles from Google News
        if d in existing_macro_days:
            continue

        day_arts = by_date.get(d, [])
        if len(day_arts) < 5:
            continue

        # Aggregate: compute mean sentiment from all asset articles
        short_scores = [LABEL_SCORES.get(a.short_label, 0.0) for a in day_arts]
        long_scores = [LABEL_SCORES.get(a.long_label, 0.0) for a in day_arts]

        # Use mean for aggregation
        short_mean = sum(short_scores) / len(short_scores)
        long_mean = sum(long_scores) / len(long_scores)

        # Map back to label
        short_label = _score_to_label(short_mean)
        long_label = _score_to_label(long_mean)

        # Confidence based on article count (more articles = higher confidence)
        conf = min(0.8, 0.3 + 0.02 * len(day_arts))

        synthetic = {
            "title": f"[derived] Macro from {len(day_arts)} asset articles",
            "summary": "",
            "url_hash": _md5_hash(f"derived:macro:{d}"),
            "published_at": datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=timezone.utc),
            "short_label": short_label,
            "long_label": long_label,
            "short_confidence": round(conf, 3),
            "long_confidence": round(conf, 3),
        }
        store_items.append(("macro", "global", d, [synthetic]))
        created += 1

        # Batch store every 500
        if len(store_items) >= 500:
            await _batch_store_articles(store_items)
            store_items = []

    if store_items:
        await _batch_store_articles(store_items)

    logger.info("Derived %d macro sentiment entries from asset articles", created)
    return created


async def derive_sector_from_assets(
    start_date: date,
    end_date: date,
) -> int:
    """Derive sector sentiment from asset articles in that sector.

    For each sector and each trading day, aggregates sentiment from all
    instruments belonging to that sector. Only fills days where we don't
    already have real sector articles from Google News.

    Returns count of synthetic articles created.
    """
    trading_days = _all_weekdays(start_date, end_date)

    # Load all real asset articles with their key (symbol)
    async with async_session() as s:
        r = await s.execute(text("""
            SELECT key, date, short_label, long_label,
                   short_confidence, long_confidence
            FROM backtest_articles
            WHERE type = 'asset' AND title != '[no articles]'
            ORDER BY date
        """))
        asset_rows = r.fetchall()

        # Load existing real sector days
        r2 = await s.execute(text("""
            SELECT DISTINCT key, date FROM backtest_articles
            WHERE type = 'sector' AND title != '[no articles]'
        """))
        existing_sector_days = {(row.key, row.date) for row in r2.fetchall()}

    # Group by (sector, date)
    by_sector_date: dict[tuple[str, date], list] = {}
    for row in asset_rows:
        sector = _INSTRUMENT_SECTOR.get(row.key)
        if not sector:
            continue
        by_sector_date.setdefault((sector, row.date), []).append(row)

    created = 0
    store_items: list[tuple[str, str, date, list[dict]]] = []

    for sector_name in SECTOR_QUERIES:
        for d in trading_days:
            # Don't overwrite real sector articles
            if (sector_name, d) in existing_sector_days:
                continue

            day_arts = by_sector_date.get((sector_name, d), [])
            if len(day_arts) < 2:
                continue

            short_scores = [LABEL_SCORES.get(a.short_label, 0.0) for a in day_arts]
            long_scores = [LABEL_SCORES.get(a.long_label, 0.0) for a in day_arts]

            short_mean = sum(short_scores) / len(short_scores)
            long_mean = sum(long_scores) / len(long_scores)

            short_label = _score_to_label(short_mean)
            long_label = _score_to_label(long_mean)

            conf = min(0.7, 0.3 + 0.05 * len(day_arts))

            synthetic = {
                "title": f"[derived] {sector_name} from {len(day_arts)} asset articles",
                "summary": "",
                "url_hash": _md5_hash(f"derived:{sector_name}:{d}"),
                "published_at": datetime(d.year, d.month, d.day, 16, 0, 0, tzinfo=timezone.utc),
                "short_label": short_label,
                "long_label": long_label,
                "short_confidence": round(conf, 3),
                "long_confidence": round(conf, 3),
            }
            store_items.append(("sector", sector_name, d, [synthetic]))
            created += 1

            if len(store_items) >= 500:
                await _batch_store_articles(store_items)
                store_items = []

    if store_items:
        await _batch_store_articles(store_items)

    logger.info("Derived %d sector sentiment entries from asset articles", created)
    return created


def _score_to_label(score: float) -> str:
    """Map a mean score in [-1, 1] back to a sentiment label."""
    if score >= 0.6:
        return "very_positive"
    elif score >= 0.2:
        return "positive"
    elif score > -0.2:
        return "neutral"
    elif score > -0.6:
        return "negative"
    else:
        return "very_negative"


# ─── Load articles into memory for scoring ───────────────────────────────────
async def load_sentiment_cache(
    types: list[str] | None = None,
) -> dict[tuple[str, str], list[dict]]:
    """Load backtest_articles into memory for fast lookups.

    Returns dict keyed by (type, key) → list of article dicts sorted by published_at.
    Each article has: date, published_at, short_label, long_label,
    short_confidence, long_confidence, title.
    """
    params: dict = {}
    if types:
        params["types"] = types

    async with async_session() as session:
        result = await session.execute(
            text(f"""
                SELECT type, key, date, title, published_at,
                       short_label, long_label,
                       short_confidence, long_confidence
                FROM backtest_articles
                WHERE title != '[no articles]'
                {"AND type = ANY(:types)" if types else ""}
                ORDER BY published_at ASC
            """),
            params,
        )
        rows = result.fetchall()

    cache: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        k = (r.type, r.key)
        if k not in cache:
            cache[k] = []
        cache[k].append({
            "date": r.date,
            "published_at": r.published_at,
            "short_label": r.short_label,
            "long_label": r.long_label,
            "short_confidence": float(r.short_confidence),
            "long_confidence": float(r.long_confidence),
            "title": r.title,
        })

    total_articles = sum(len(v) for v in cache.values())
    logger.info("Loaded %d articles across %d (type, key) groups (%s)",
                total_articles, len(cache), types or "all")
    return cache


# ─── Scoring helpers (mirror production scorer.py exactly) ────────────────────
def _log_confidence(n: float, full_at: int) -> float:
    if full_at <= 0:
        return 1.0
    return min(1.0, math.log(1 + n) / math.log(1 + full_at))


def _clip(x: float) -> float:
    return max(-3.0, min(3.0, x))


def _consensus_adjustment(labels: list[str], avg_age_hours: float) -> float:
    """Mirrors production _consensus_adjustment() exactly.

    Uses article counts (not weighted counts) — matches production scorer.py lines 221-261.
    Returns a multiplier in [0.7, 1.0].
    """
    if not labels:
        return 1.0

    non_neutral = [l for l in labels if l != "neutral"]
    if len(non_neutral) < 3:
        return 1.0

    positive_count = sum(1 for l in non_neutral if "positive" in l)
    negative_count = sum(1 for l in non_neutral if "negative" in l)
    dominant = max(positive_count, negative_count)
    agreement_ratio = dominant / len(non_neutral)

    multiplier = 1.0

    # Contrarian dampening: >80% agreement = herd signal
    if agreement_ratio > 0.80:
        multiplier *= 0.85

    # Priced-in detection: high consensus + old average age
    if agreement_ratio > 0.75 and avg_age_hours > 48:
        multiplier *= 0.90

    return max(0.7, multiplier)


# ─── Public scoring functions (article-level, production-faithful) ────────────
def get_asset_sentiment_for_date(
    symbol: str,
    d: date,
    cache: dict,
    term: str = "short",
) -> tuple[float, float]:
    """Asset sentiment score for a backtest date.

    Returns (confidence_scaled_score ∈ [-3,3], confidence ∈ [0,1]).
    Mirrors production get_sentiment_score() exactly:
    - Time-decay with article-level timestamps
    - Consensus dampening using article counts
    - Logarithmic confidence ramp
    """
    params = SENTIMENT_PARAMS[term]
    half_life = params["half_life_hours"]
    window_days = params["window_days"]
    full_at = params["full_confidence_at"]
    decay_lambda = math.log(2) / half_life

    articles = cache.get(("asset", symbol), [])
    if not articles:
        return 0.0, 0.0

    target_dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
    window_start = target_dt - timedelta(days=window_days)

    weighted_sum = 0.0
    weight_total = 0.0
    non_neutral_weighted = 0.0
    all_labels: list[str] = []
    age_sum = 0.0
    nn_count = 0

    label_key = "short_label" if term == "short" else "long_label"

    for art in articles:
        pub = art["published_at"]
        if pub is None:
            continue
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        if pub > target_dt or pub < window_start:
            continue

        label = art[label_key]
        all_labels.append(label)

        # Production skips neutral articles in weighted sum (scorer.py line 470)
        if label == "neutral":
            continue

        score = LABEL_SCORES.get(label, 0.0)

        age_hours = (target_dt - pub).total_seconds() / 3600.0
        decay = math.exp(-decay_lambda * age_hours)

        weighted_sum += score * decay
        weight_total += decay
        non_neutral_weighted += decay
        age_sum += age_hours
        nn_count += 1

    if weight_total == 0.0:
        return 0.0, 0.0

    # Scale from [-1, 1] to [-3, 3] (LABEL_SCORES are in [-1,1], production uses [-3,3] directly)
    mean = (weighted_sum / weight_total) * 3.0

    # Effective count: production uses non_neutral_weighted_count directly (no *2)
    effective_count = non_neutral_weighted
    confidence = _log_confidence(min(round(effective_count), full_at), full_at=full_at)

    # Consensus dampening using article counts (production-exact)
    avg_age = age_sum / nn_count if nn_count > 0 else 0.0
    consensus_mult = _consensus_adjustment(all_labels, avg_age)

    final = _clip(mean * confidence * consensus_mult)
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
    window_hours = params["window_hours"]
    decay_lambda = math.log(2) / half_life

    articles = cache.get(("macro", "global"), [])
    if not articles:
        return 0.0, 0.0

    target_dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
    window_start = target_dt - timedelta(hours=window_hours)

    weighted_sum = 0.0
    weight_total = 0.0
    total_count = 0

    label_key = "short_label" if term == "short" else "long_label"

    for art in articles:
        pub = art["published_at"]
        if pub is None:
            continue
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        if pub > target_dt or pub < window_start:
            continue

        label = art[label_key]
        score = LABEL_SCORES.get(label, 0.0)

        age_hours = (target_dt - pub).total_seconds() / 3600.0
        decay = math.exp(-decay_lambda * age_hours)

        # Production macro uses all articles (including neutral) in weighted sum
        weighted_sum += score * decay
        weight_total += decay
        total_count += 1

    if weight_total == 0.0:
        return 0.0, 0.0

    # ×3 to match production scorer.py: score = _clip(float(row.score) * 3.0)
    macro_mean = (weighted_sum / weight_total) * 3.0
    confidence = _log_confidence(min(total_count, 30), full_at=10)
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
    window_hours = params["window_hours"]
    decay_lambda = math.log(2) / half_life

    articles = cache.get(("sector", sector), [])
    if not articles:
        return 0.0, 0.0

    target_dt = datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
    window_start = target_dt - timedelta(hours=window_hours)

    weighted_sum = 0.0
    weight_total = 0.0
    total_count = 0

    label_key = "short_label" if term == "short" else "long_label"

    for art in articles:
        pub = art["published_at"]
        if pub is None:
            continue
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        if pub > target_dt or pub < window_start:
            continue

        label = art[label_key]
        score = LABEL_SCORES.get(label, 0.0)

        age_hours = (target_dt - pub).total_seconds() / 3600.0
        decay = math.exp(-decay_lambda * age_hours)

        # Production sector uses all articles (including neutral) in weighted sum
        weighted_sum += score * decay
        weight_total += decay
        total_count += 1

    if weight_total == 0.0:
        return 0.0, 0.0

    # ×3 to match production scorer.py
    sector_mean = (weighted_sum / weight_total) * 3.0
    confidence = _log_confidence(min(total_count, 20), full_at=8)
    effective = _clip(sector_mean * confidence)
    return round(effective, 4), round(confidence, 4)
