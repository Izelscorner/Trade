"""Batch processing strategy:
  - Classification: N articles → 1 API call → JSON array of results
  - Sentiment: group articles by first instrument → 1 API call per instrument bucket → JSON array
    * Now returns DUAL-HORIZON: short-term (1-7d) AND long-term (1-6mo) sentiment per article
  - Macro sentiment: N macro articles → 1 API call → JSON array of results
    * Also dual-horizon
  - ETF constituent awareness: news about ETF holdings auto-tags the parent ETF with weight-based relevance
This dramatically reduces the number of API calls to NVIDIA NIM.
"""

import asyncio
import re
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session
from .nim_client import generate_json, generate_json_array, get_api_metrics
from .prompts import (
    CLASSIFY_SYSTEM,
    SENTIMENT_SYSTEM,
    MACRO_SYSTEM,
    SECTOR_SENTIMENT_SYSTEM,
    classify_prompt,
    batch_classify_prompt,
    batch_sentiment_prompt,
    batch_macro_sentiment_prompt,
    batch_sector_sentiment_prompt,
    sector_classify_prompt,
    sentiment_prompt,
    macro_sentiment_prompt,
    etf_constituent_prompt,
    build_instrument_context,
    get_role,
    get_asset_description,
)

logger = logging.getLogger(__name__)

PROCESS_INTERVAL = 5   # seconds between processing waves
BATCH_SIZE = 90        # articles fetched per wave from DB
BATCH_SIZE_MAX = 150   # max batch size when queue is deep

# Articles per API call — larger batches = fewer API calls = faster throughput.
CLASSIFY_BATCH = 20    # articles per classify API call
SENTIMENT_BATCH = 20   # articles per sentiment API call per instrument
MACRO_BATCH = 20       # macro articles per macro-sentiment API call
SECTOR_BATCH = 20      # sector articles per sector-sentiment API call

# Token budgets per article (generous to avoid truncation → fallback burst)
CLASSIFY_TOKENS_PER_ARTICLE = 100   # {"id","type","instruments","is_macro"} × N
SENTIMENT_TOKENS_PER_ARTICLE = 70   # dual-horizon sentiment
MACRO_TOKENS_PER_ARTICLE = 80       # dual-horizon macro
TOKENS_OVERHEAD = 150               # wrapper object + whitespace slack

# Weighted concurrency distribution for parallel processing.
# 20 total concurrent API calls: 14 asset + 4 macro + 2 sector.
# All types run simultaneously in every wave.
TOTAL_CONCURRENCY = 20
ASSET_CONCURRENCY = 14     # concurrent asset sentiment API calls
MACRO_CONCURRENCY = 4      # concurrent macro sentiment API calls
SECTOR_CONCURRENCY = 2     # concurrent sector sentiment API calls
CLASSIFY_CONCURRENCY = 10  # concurrent classify API calls (phase 1)

# Confidence-based skip thresholds — instruments with sufficient sentiment
# confidence can skip LLM sentiment processing to save API calls.
CONFIDENCE_SKIP_THRESHOLD = 0.85  # skip if confidence >= this (short-term)
CONFIDENCE_SKIP_ARTICLE_MIN = 15  # minimum non-neutral articles before skipping

# Map sentiment labels to probability distributions
SENTIMENT_PROBABILITIES = {
    "very_positive": {"positive": 0.90, "negative": 0.02, "neutral": 0.08},
    "positive":      {"positive": 0.70, "negative": 0.05, "neutral": 0.25},
    "neutral":       {"positive": 0.15, "negative": 0.15, "neutral": 0.70},
    "negative":      {"positive": 0.05, "negative": 0.70, "neutral": 0.25},
    "very_negative": {"positive": 0.02, "negative": 0.90, "neutral": 0.08},
}

# Map sentiment display labels to score multipliers (for macro aggregation)
SENTIMENT_MULTIPLIERS = {
    "very positive": 1.0,
    "positive": 0.5,
    "neutral": 0.0,
    "negative": -0.5,
    "very negative": -1.0,
}

# Map sentiment labels to display labels
SENTIMENT_LABEL_MAP = {
    "very_positive": "very positive",
    "positive": "positive",
    "neutral": "neutral",
    "negative": "negative",
    "very_negative": "very negative",
}

# --- Deterministic pre-processing: low-quality filter ---


def is_low_quality_article(title: str, summary: str, content: str) -> bool:
    """Pre-filter articles that are too low-quality for meaningful sentiment analysis.

    Saves API calls and prevents noise from polluting the signal.
    """
    # Title is required and must have substance
    if not title or len(title.strip()) < 15:
        return True

    # Combined text must have enough substance for analysis
    text = f"{title} {summary} {content}".strip()
    if len(text) < 40:
        return True

    # Title is just the summary repeated (aggregator filler)
    if summary and title.strip().lower() == summary.strip().lower():
        # Only title available, no additional content — borderline but allow if title is substantial
        if not content and len(title) < 60:
            return True

    # SEO/auto-generated patterns
    _LOW_QUALITY_PATTERNS = re.compile(
        r"""(?ix)
        (?:
            ^(?:stock|share)s?\s+to\s+(?:watch|keep\s+an\s+eye\s+on|follow)\s+(?:today|this\s+week|right\s+now)
            | ^(?:top|best|worst)\s+\d+\s+(?:stock|share|etf|fund)s?\s+(?:to|for)\b
            | \bhigh\s+accuracy\s+investment\s+signals\b
            | \bnaître\s+et\s+grandir\b
            | ^how\s+.*\s+stock\s+responds?\s+to\s+policy\s+changes\b
        )
        """
    )
    if _LOW_QUALITY_PATTERNS.search(title):
        return True

    return False


# --- Deterministic post-processing rules ---

# Categories from the news-fetcher that are inherently macro
MACRO_SOURCE_CATEGORIES = {"macro_markets", "macro_politics", "macro_conflict"}

# Patterns in titles that indicate asset-specific (NOT macro) content
_ASSET_SPECIFIC_PATTERNS = re.compile(
    r"""(?ix)
    (?:
        \$[A-Z]{1,5}\b
        | shares?\s+(?:of|sold|acquired|bought|purchased|cut)
        | (?:buys?|sells?|acquires?|purchases?)\s+\d[\d,.]*\s+shares?
        | (?:stake|position|holding)\s+in\b
        | \b(?:bull|bear)\s+case\s+theory\b
        | \b(?:stock|share)\s+(?:price|target|slides?|surges?|jumps?|drops?|rallies?)
        | \b(?:earnings|revenue|Q[1-4]|fiscal)\b
        | \banalyst[s]?\s+(?:rating|target|upgrade|downgrade)
        | \b(?:dividend|buyback|IPO|insider)\b
        | \bmarket\s*(?:cap|beat)\b
        | \bLLC['s]*\s+(?:\d|position|stake|holding|purchase|acquire)
    )
    """
)

# Patterns that indicate genuine macro/geopolitical news
_MACRO_PATTERNS = re.compile(
    r"""(?ix)
    (?:
        \b(?:strikes?\s+(?:on|against|across)|invasion|missile|bomb(?:ing)?|attack(?:ed|s)?\s+(?:on|against|in)|sanction[s]?|blockade)\b
        | \b(?:NATO|G7|G20|OPEC|IMF|World\s+Bank)\b
        | \b(?:Fed(?:eral\s+Reserve)?|ECB|BOJ|central\s+bank|rate\s+(?:hike|cut|decision))\b
        | \b(?:GDP|inflation|recession|unemployment|CPI|PPI)\b
        | \b(?:trade\s+war|tariff|embargo|geopolitic)\b
        | \b(?:Iran|Russia|China|Ukraine|Israel|Gaza|Taiwan)\b.*\b(?:military|nuclear|attack|strike|war|conflict|tension)\b
        | \b(?:military|nuclear|attack|strike|war|conflict|tension)\b.*\b(?:Iran|Russia|China|Ukraine|Israel|Gaza|Taiwan)\b
    )
    """
)

# Commodity-specific price keywords
_COMMODITY_PRICE_KEYWORDS = {
    "gold": ["gold price", "gold futures", "gold market", "gold rally", "gold surge", "gold drop", "bullion",
             "safe haven", "safe-haven", "flight to safety"],
    "oil": ["oil price", "crude oil", "oil futures", "oil market", "brent", "wti",
            "oil surge", "oil output", "oil supply", "oil depots", "oil disruption",
            "barrel", "opec", "strait of hormuz", "fuel depots", "oil and gas",
            "energy crisis", "petrol"],
    "silver": ["silver price", "silver futures", "silver market"],
    "natural gas": ["natural gas price", "gas futures", "henry hub"],
    "copper": ["copper price", "copper futures"],
}

# ETF constituent cache: etf_symbol -> {constituent_symbol: weight_percent}
_ETF_CONSTITUENTS: dict[str, dict[str, float]] = {}


def get_adaptive_batch_size(queue_depth: int) -> int:
    """Return batch size based on queue depth and API headroom.

    When the queue is deep (many unprocessed articles), pull larger batches
    to maximize throughput. When queue is shallow, use smaller batches for
    lower latency.
    """
    metrics = get_api_metrics()

    # If we're getting 429s, shrink batch size to reduce pressure
    if metrics["consecutive_429s"] >= 2:
        return max(10, BATCH_SIZE // 2)

    # Scale batch size based on queue depth
    if queue_depth > 100:
        return BATCH_SIZE_MAX
    elif queue_depth > 50:
        return min(BATCH_SIZE_MAX, BATCH_SIZE + 15)
    else:
        return BATCH_SIZE


def get_adaptive_sub_batch(base_size: int, queue_depth: int) -> int:
    """Increase sub-batch size when queue is deep and API has headroom."""
    metrics = get_api_metrics()

    # Conservative if we're hitting rate limits
    if metrics["consecutive_429s"] >= 1:
        return base_size

    # Increase sub-batch by 50% when queue is deep (fewer API calls needed)
    if queue_depth > 50 and metrics["headroom_pct"] > 30:
        return min(base_size + base_size // 2, base_size * 2)

    return base_size


async def get_instrument_sentiment_confidence(instrument_id: str) -> dict:
    """Check current sentiment confidence for an instrument.

    Returns confidence metrics used to decide whether to skip LLM processing.
    """
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT COUNT(*) as total,
                       COUNT(*) FILTER (WHERE s.label != 'neutral') as non_neutral
                FROM sentiment_scores s
                JOIN news_instrument_map m ON m.article_id = s.article_id
                JOIN news_articles a ON a.id = m.article_id
                WHERE m.instrument_id = :iid
                AND a.ollama_processed = true
                AND a.published_at >= NOW() - INTERVAL '2 days'
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()
        total = row.total if row else 0
        non_neutral = row.non_neutral if row else 0

        # Log confidence ramp: log(1+n) / log(1+N) where N=20
        import math
        confidence = min(1.0, math.log(1 + non_neutral) / math.log(1 + 20)) if non_neutral > 0 else 0.0

        return {
            "total": total,
            "non_neutral": non_neutral,
            "confidence": round(confidence, 4),
        }


async def get_unprocessed_queue_depth() -> int:
    """Get count of unprocessed articles for adaptive batch sizing."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT COUNT(*) FROM news_articles WHERE ollama_processed = false")
        )
        return result.scalar() or 0


async def get_instruments() -> list[dict]:
    """Load all tracked instruments from the database."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category, sector FROM instruments WHERE is_active = true ORDER BY symbol")
        )
        return [
            {
                "id": str(row.id),
                "symbol": row.symbol,
                "name": row.name,
                "category": row.category,
                "sector": row.sector,
            }
            for row in result.fetchall()
        ]


def build_name_lookup(instruments: list[dict]) -> dict[str, list[str]]:
    """Build a symbol -> [search names] map dynamically from instrument data.

    Uses MULTI-WORD PHRASES (not individual words) to avoid false positives.
    Single common words like "and", "500", "semiconductor" caused mass-mistagging.
    """
    # Words too generic to use as standalone search terms
    _STOP_WORDS = {
        "and", "the", "of", "inc", "corp", "ltd", "llc", "plc", "etf",
        "futures", "co", "company", "group", "holdings", "technologies",
        "a", "an", "in", "on", "at", "to", "for", "by", "with", "from",
        "us", "s&p", "500", "100", "50", "vanguard", "ishares", "vaneck",
        "trust", "fund", "index", "technology", "semiconductor", "energy",
        "financial", "global", "international", "capital", "resources",
        "a/s",
    }
    lookup = {}
    for inst in instruments:
        symbol = inst["symbol"]
        name = inst["name"].lower()
        category = inst["category"]

        if category == "commodity":
            lookup[symbol] = []
        else:
            # Clean corporate suffixes and punctuation
            clean = re.sub(r'\b(inc\.?|corp\.?|corporation|ltd\.?|llc|plc|etf|futures|co\.?|a/s)\b', '', name, flags=re.I)
            clean = re.sub(r'[.,]+', ' ', clean)
            clean = re.sub(r'\s+', ' ', clean).strip()

            search_names = []

            # For multi-word names, extract significant words first
            words = [w for w in clean.split() if w and w.lower() not in _STOP_WORDS and len(w) > 1]

            if len(words) >= 2:
                # Full multi-word phrase (e.g. "eli lilly", "exxon mobil", "novo nordisk")
                full_phrase = " ".join(words)
                if len(full_phrase) > 4:
                    search_names.append(full_phrase)
                # Also add consecutive word pairs for partial matching
                for i in range(len(words) - 1):
                    pair = f"{words[i]} {words[i+1]}"
                    if len(pair) > 5:
                        search_names.append(pair)
            elif len(words) == 1 and len(words[0]) >= 4:
                # Single distinctive word (e.g. "nvidia", "alphabet", "apple", "walmart", "tesla", "palantir")
                search_names.append(words[0])
            # If no significant words extracted (e.g. "RTX Corporation" -> "rtx" is too short),
            # that's OK — the ticker symbol check in _check_direct_mention handles it.

            seen = set()
            unique = []
            for n in search_names:
                nl = n.lower()
                if nl not in seen:
                    seen.add(nl)
                    unique.append(nl)
            lookup[symbol] = unique

    return lookup


def _check_direct_mention(title: str, content: str, name_lookup: dict[str, list[str]], instruments: list[dict]) -> set[str]:
    """Check which tracked instruments are directly mentioned by name in text.

    Uses word-boundary matching to avoid false positives (e.g. 'and' in 'demand').
    """
    combined = f"{title} {content[:500]}".lower()
    mentioned = set()

    for symbol, names in name_lookup.items():
        # Check ticker symbol with word boundary (avoids matching "gold" in "Goldman")
        if re.search(r'\b' + re.escape(symbol.lower()) + r'\b', combined):
            mentioned.add(symbol)
            continue
        # Check name phrases — these are already multi-word so substring is safer
        if any(n in combined for n in names):
            mentioned.add(symbol)

    for inst in instruments:
        if inst["category"] != "commodity":
            continue
        symbol = inst["symbol"]
        name_lower = inst["name"].lower()
        for commodity_key, keywords in _COMMODITY_PRICE_KEYWORDS.items():
            if commodity_key in name_lower:
                if any(kw in combined for kw in keywords):
                    mentioned.add(symbol)
                break

    return mentioned


def postprocess_classification(
    title: str,
    content: str,
    source_category: str,
    llm_instruments: list[str],
    llm_is_macro: bool,
    valid_symbols: set[str],
    name_lookup: dict[str, list[str]],
    instruments: list[dict],
) -> tuple[list[str], bool]:
    """Apply deterministic rules to correct LLM classification errors."""
    combined = f"{title} {content[:300]}"

    _MACRO_CATEGORIES = {"macro_markets", "macro_politics", "macro_conflict"}

    # For macro feed articles: trust LLM if it says NOT macro AND the content
    # is clearly about a specific company (stock patterns). Otherwise default to macro.
    if source_category in _MACRO_CATEGORIES:
        if not llm_is_macro and _ASSET_SPECIFIC_PATTERNS.search(title) and not _MACRO_PATTERNS.search(combined):
            # LLM correctly identified this as company-specific despite being in a macro feed
            llm_is_macro = False
        elif _MACRO_PATTERNS.search(combined):
            # Has actual macro content — force macro
            llm_is_macro = True
        else:
            # From macro feed but no strong signals either way — trust LLM
            pass

    if source_category == "asset_specific":
        if llm_is_macro and not _MACRO_PATTERNS.search(combined):
            llm_is_macro = False

    if source_category == "asset_specific" and llm_is_macro and _ASSET_SPECIFIC_PATTERNS.search(title):
        if not _MACRO_PATTERNS.search(combined):
            llm_is_macro = False

    # Force macro=true if strong macro patterns present regardless of source
    if not llm_is_macro and _MACRO_PATTERNS.search(combined):
        llm_is_macro = True

    # Instead of filtering DOWN the LLM's instruments, we trust the LLM
    # if it identified them. We simply ADD any instruments that were missed
    # but found via direct keyword mentions.
    direct = _check_direct_mention(title, content, name_lookup, instruments)
    for sym in direct:
        if sym not in llm_instruments:
            llm_instruments.append(sym)


    # --- ETF constituent propagation ---
    # If a tagged instrument is a constituent of a tracked ETF, also tag the ETF
    etf_tags_to_add: dict[str, float] = {}  # etf_symbol -> max relevance
    for symbol in list(llm_instruments):
        for etf_symbol, constituents in _ETF_CONSTITUENTS.items():
            if symbol in constituents:
                weight = constituents[symbol]
                if etf_symbol not in etf_tags_to_add or weight > etf_tags_to_add[etf_symbol]:
                    etf_tags_to_add[etf_symbol] = weight

    for etf_symbol in etf_tags_to_add:
        if etf_symbol not in llm_instruments:
            llm_instruments.append(etf_symbol)

    return llm_instruments, llm_is_macro


async def get_unprocessed_articles(limit: int = BATCH_SIZE) -> list[dict]:
    """Get articles that haven't been processed yet.

    Fetches a proportional mix matching concurrency weights:
      asset : macro : sector = 10 : 3 : 2  (of the total limit)
    User-prioritized articles always come first regardless of type.
    If any category has fewer articles than its quota, the remainder
    is redistributed to the other categories.
    """
    # Proportional quotas (14:4:2 = 70%:20%:10%)
    asset_quota = int(limit * 14 / 20)
    macro_quota = int(limit * 4 / 20)
    sector_quota = limit - asset_quota - macro_quota  # remainder gets sector

    def _parse_rows(result) -> list[dict]:
        return [
            {
                "id": str(r.id),
                "title": r.title,
                "summary": r.summary or "",
                "content": r.content or "",
                "category": r.category,
            }
            for r in result.fetchall()
        ]

    _priority_join = """
        LEFT JOIN LATERAL (
            SELECT pp.requested_at
            FROM news_instrument_map nim
            JOIN processing_priority pp ON pp.instrument_id = nim.instrument_id
            WHERE nim.article_id = a.id
            ORDER BY pp.requested_at DESC
            LIMIT 1
        ) pri ON true
    """
    _order_by = """
        ORDER BY
            CASE WHEN pri.requested_at IS NOT NULL THEN 0 ELSE 1 END,
            COALESCE(pri.requested_at, '1970-01-01'::timestamptz) DESC,
            a.published_at DESC
    """

    async with async_session() as session:
        # Fetch each category separately with its quota
        asset_res = await session.execute(
            text(f"""
                SELECT a.id, a.title, a.summary, a.content, a.category
                FROM news_articles a
                {_priority_join}
                WHERE a.ollama_processed = false
                  AND a.category NOT LIKE 'macro_%%'
                  AND a.category NOT LIKE 'sector_%%'
                {_order_by}
                LIMIT :lim
            """),
            {"lim": asset_quota},
        )
        asset_articles = _parse_rows(asset_res)

        macro_res = await session.execute(
            text(f"""
                SELECT a.id, a.title, a.summary, a.content, a.category
                FROM news_articles a
                {_priority_join}
                WHERE a.ollama_processed = false
                  AND a.category LIKE 'macro_%%'
                {_order_by}
                LIMIT :lim
            """),
            {"lim": macro_quota},
        )
        macro_articles = _parse_rows(macro_res)

        sector_res = await session.execute(
            text(f"""
                SELECT a.id, a.title, a.summary, a.content, a.category
                FROM news_articles a
                {_priority_join}
                WHERE a.ollama_processed = false
                  AND a.category LIKE 'sector_%%'
                {_order_by}
                LIMIT :lim
            """),
            {"lim": sector_quota},
        )
        sector_articles = _parse_rows(sector_res)

        # Redistribute unused quota to other categories
        total = len(asset_articles) + len(macro_articles) + len(sector_articles)
        if total < limit:
            remaining = limit - total
            seen_ids = {a["id"] for a in asset_articles + macro_articles + sector_articles}
            extra_res = await session.execute(
                text(f"""
                    SELECT a.id, a.title, a.summary, a.content, a.category
                    FROM news_articles a
                    {_priority_join}
                    WHERE a.ollama_processed = false
                    {_order_by}
                    LIMIT :lim
                """),
                {"lim": remaining + len(seen_ids)},
            )
            for row in _parse_rows(extra_res):
                if row["id"] not in seen_ids and len(asset_articles) + len(macro_articles) + len(sector_articles) < limit:
                    seen_ids.add(row["id"])
                    if row["category"].startswith("macro_"):
                        macro_articles.append(row)
                    elif row["category"].startswith("sector_"):
                        sector_articles.append(row)
                    else:
                        asset_articles.append(row)

        # Interleave: asset, asset, asset, macro, asset, sector, ... (10:3:2 pattern)
        result_list: list[dict] = []
        ai, mi, si = 0, 0, 0
        while ai < len(asset_articles) or mi < len(macro_articles) or si < len(sector_articles):
            # 14 asset
            for _ in range(14):
                if ai < len(asset_articles):
                    result_list.append(asset_articles[ai])
                    ai += 1
            # 4 macro
            for _ in range(4):
                if mi < len(macro_articles):
                    result_list.append(macro_articles[mi])
                    mi += 1
            # 2 sector
            for _ in range(2):
                if si < len(sector_articles):
                    result_list.append(sector_articles[si])
                    si += 1

        return result_list


# ---------------------------------------------------------------------------
# ETF Constituent Management
# ---------------------------------------------------------------------------

async def populate_etf_constituents(etf_instruments: list[dict]) -> None:
    """Populate ETF constituent data using LLM if not already in DB.

    Falls back to known hardcoded data for common ETFs if LLM fails.
    """
    global _ETF_CONSTITUENTS

    # Hardcoded fallback for known ETFs
    _KNOWN_ETF_CONSTITUENTS = {
        "IITU": [
            {"symbol": "NVDA", "name": "NVIDIA Corporation", "weight_percent": 23.06},
            {"symbol": "AAPL", "name": "Apple Inc.", "weight_percent": 18.19},
            {"symbol": "MSFT", "name": "Microsoft Corporation", "weight_percent": 16.13},
            {"symbol": "AVGO", "name": "Broadcom Inc.", "weight_percent": 8.03},
            {"symbol": "CRM", "name": "Salesforce Inc.", "weight_percent": 3.51},
            {"symbol": "ORCL", "name": "Oracle Corporation", "weight_percent": 3.21},
            {"symbol": "AMD", "name": "Advanced Micro Devices", "weight_percent": 2.89},
            {"symbol": "NOW", "name": "ServiceNow Inc.", "weight_percent": 2.71},
            {"symbol": "MU", "name": "Micron Technology", "weight_percent": 2.41},
            {"symbol": "ADBE", "name": "Adobe Inc.", "weight_percent": 2.29},
        ],
    }

    async with async_session() as session:
        for etf in etf_instruments:
            etf_id = etf["id"]
            etf_symbol = etf["symbol"]
            
            if etf_symbol in _ETF_CONSTITUENTS:
                continue
                
            etf_name = etf["name"]

            # Check if already populated
            result = await session.execute(
                text("SELECT constituent_symbol, weight_percent FROM etf_constituents WHERE etf_instrument_id = :eid"),
                {"eid": etf_id},
            )
            rows = result.fetchall()
            if rows:
                constituent_map = {r.constituent_symbol: float(r.weight_percent) for r in rows}
                _ETF_CONSTITUENTS[etf_symbol] = constituent_map
                logger.info("ETF %s: loaded %d constituents from DB", etf_symbol, len(rows))
                
                # Backfill mappings for existing articles of constituents
                await backfill_etf_mappings(etf_id, constituent_map)
                continue

            # Use LLM to identify constituents
            logger.info("ETF %s: fetching constituents via LLM...", etf_symbol)
            prompt = etf_constituent_prompt(etf_name, etf_symbol)
            result_data = await generate_json(prompt, system="You are a financial data expert. Always respond with valid JSON.", max_tokens=500)

            # Fallback to hardcoded data if LLM fails or returns empty/invalid
            has_valid = (
                result_data
                and "constituents" in result_data
                and isinstance(result_data["constituents"], list)
                and len(result_data["constituents"]) >= 3
            )
            if not has_valid:
                logger.warning("ETF %s: LLM response invalid or insufficient, trying hardcoded fallback", etf_symbol)
                if etf_symbol in _KNOWN_ETF_CONSTITUENTS:
                    result_data = {"constituents": _KNOWN_ETF_CONSTITUENTS[etf_symbol]}
                    logger.info("ETF %s: using hardcoded constituents", etf_symbol)

            if result_data and "constituents" in result_data:
                constituents = result_data["constituents"]
                etf_cache = {}
                for c in constituents:
                    if not isinstance(c, dict):
                        continue
                    c_symbol = c.get("symbol", "").strip().upper()
                    c_name = c.get("name", "").strip()
                    c_weight = float(c.get("weight_percent", 0))
                    if c_symbol and c_weight > 0:
                        try:
                            await session.execute(
                                text("""
                                    INSERT INTO etf_constituents (etf_instrument_id, constituent_symbol, constituent_name, weight_percent)
                                    VALUES (:eid, :sym, :name, :weight)
                                    ON CONFLICT (etf_instrument_id, constituent_symbol) DO UPDATE
                                    SET weight_percent = :weight, constituent_name = :name, updated_at = NOW()
                                """),
                                {"eid": etf_id, "sym": c_symbol, "name": c_name, "weight": c_weight},
                            )
                            etf_cache[c_symbol] = c_weight
                        except Exception as e:
                            logger.warning("Failed to store ETF constituent %s: %s", c_symbol, e)
                await session.commit()
                _ETF_CONSTITUENTS[etf_symbol] = etf_cache
                logger.info("ETF %s: stored %d constituents", etf_symbol, len(etf_cache))
                
                # Backfill mappings for existing articles of constituents
                await backfill_etf_mappings(etf_id, etf_cache)
            else:
                logger.warning("ETF %s: LLM failed to provide constituents", etf_symbol)


async def backfill_etf_mappings(etf_id: str, constituents: dict[str, float]) -> None:
    """Retroactively map already-processed articles of constituents to the parent ETF.
    
    This ensures newly added ETFs immediately have historical news and sentiment
    without needing to re-process (re-score) any articles.
    """
    if not constituents:
        return
        
    logger.info("Backfilling ETF mappings for instrument %s...", etf_id)
    async with async_session() as session:
        try:
            # Efficiently insert mappings for all articles associated with any tracked constituent
            result = await session.execute(
                text("""
                    INSERT INTO news_instrument_map (article_id, instrument_id, relevance_score)
                    SELECT m.article_id, :eid, (ec.weight_percent / 100.0)
                    FROM news_instrument_map m
                    JOIN instruments i_const ON i_const.id = m.instrument_id
                    JOIN etf_constituents ec ON (UPPER(ec.constituent_symbol) = UPPER(i_const.symbol))
                    WHERE ec.etf_instrument_id = :eid
                    ON CONFLICT (article_id, instrument_id) DO NOTHING
                """),
                {"eid": etf_id}
            )
            await session.commit()
            logger.info("Backfill complete for ETF %s", etf_id)
        except Exception as e:
            logger.error("Failed to backfill ETF mappings: %s", e)


async def get_etf_constituents() -> dict[str, dict[str, float]]:
    """Get cached ETF constituents (populated at startup)."""
    return _ETF_CONSTITUENTS


# ---------------------------------------------------------------------------
# Batch processing — the main public entry point
# ---------------------------------------------------------------------------

async def process_batch(
    articles: list[dict],
    instrument_ids: dict[str, str],
    valid_symbols: set[str],
    instruments: list[dict],
    instruments_by_symbol: dict[str, dict],
    symbol_mapping: str,
    valid_symbols_str: str,
    name_lookup: dict[str, list[str]],
) -> None:
    """Process a batch of articles with minimal API calls.

    Steps:
      1. Batch-classify all articles (1 call per CLASSIFY_BATCH articles)
      2. Apply deterministic post-processing per article (incl. ETF constituent propagation)
      3. Filter spam / irrelevant articles
      4. Group remaining articles by first instrument, batch-sentiment per group (DUAL-HORIZON)
      5. Batch macro-sentiment for all macro articles (DUAL-HORIZON)
      6. Mark all articles processed
    """
    if not articles:
        return

    # --- Step 0: Pre-filter low-quality articles ---
    low_quality_ids = []
    quality_articles = []
    for art in articles:
        if is_low_quality_article(art["title"], art.get("summary", ""), art.get("content", "")):
            logger.info("Filtered low-quality: '%s'", art["title"][:60])
            low_quality_ids.append(art["id"])
        else:
            quality_articles.append(art)

    for art_id in low_quality_ids:
        await delete_article(art_id)

    articles = quality_articles
    if not articles:
        return

    # --- Step 1: Batch classify (parallel chunks with concurrency limit) ---
    classify_results: dict[str, dict] = {}  # article_id -> classification
    classify_sem = asyncio.Semaphore(CLASSIFY_CONCURRENCY)

    async def _classify_chunk(chunk: list[dict]) -> list[tuple[str, dict]]:
        """Classify a chunk of articles, return list of (article_id, classification)."""
        async with classify_sem:
            results = []
            prompt = batch_classify_prompt(chunk, symbol_mapping, valid_symbols_str)
            max_tok = CLASSIFY_TOKENS_PER_ARTICLE * len(chunk) + TOKENS_OVERHEAD
            raw_results = await generate_json_array(
                prompt, system=CLASSIFY_SYSTEM, max_tokens=max_tok
            )
            if raw_results is None:
                logger.warning("Batch classify failed for chunk of %d, falling back to individual calls", len(chunk))
                for art in chunk:
                    result = await _classify_single(art, symbol_mapping, valid_symbols_str)
                    if result:
                        results.append((art["id"], result))
            else:
                for item in raw_results:
                    if isinstance(item, dict) and "id" in item:
                        results.append((item["id"], item))
                found_ids = {r[0] for r in results}
                for art in chunk:
                    if art["id"] not in found_ids:
                        logger.warning("Article %s missing from batch classify response, retrying individually", art["id"])
                        result = await _classify_single(art, symbol_mapping, valid_symbols_str)
                        if result:
                            results.append((art["id"], result))
            return results

    classify_chunks = [
        articles[i:i + CLASSIFY_BATCH]
        for i in range(0, len(articles), CLASSIFY_BATCH)
    ]
    logger.info("Classify: dispatching %d parallel chunks (%d articles)", len(classify_chunks), len(articles))
    chunk_results = await asyncio.gather(*[_classify_chunk(c) for c in classify_chunks])
    for result_list in chunk_results:
        for art_id, clf in result_list:
            classify_results[art_id] = clf

    # --- Step 2: Post-process classifications ---
    article_map = {a["id"]: a for a in articles}

    keep: list[dict] = []          # articles to run sentiment on
    macro_articles: list[dict] = [] # macro articles needing macro-sentiment
    sector_articles: dict[str, list[dict]] = {}  # sector -> articles needing sector sentiment
    to_delete: list[str] = []      # spam article IDs

    for art in articles:
        art_id = art["id"]
        clf = classify_results.get(art_id)
        if not clf:
            logger.warning("No classification for article %s, skipping", art_id)
            continue

        article_type = clf.get("type", "spam")
        raw_instruments = clf.get("instruments", [])
        is_macro = clf.get("is_macro", False)

        # Build complete set of recognized symbols: explicitly tracked + all ETF constituents
        recognized_symbols = set(valid_symbols)
        for etf_sym, constituents in _ETF_CONSTITUENTS.items():
            recognized_symbols.update(constituents.keys())

        # Extract symbols
        tagged_instruments = []
        for inst in raw_instruments:
            if isinstance(inst, dict):
                for key in inst.keys():
                    sym = key.strip().upper()
                    if sym in recognized_symbols:
                        tagged_instruments.append(sym)
            elif isinstance(inst, str):
                symbol = inst.split(" ")[0].split("-")[0].strip().upper()
                if symbol in recognized_symbols:
                    tagged_instruments.append(symbol)
        tagged_instruments = list(dict.fromkeys(tagged_instruments))

        if article_type == "spam":
            logger.info("Filtered spam: '%s'", art["title"][:60])
            to_delete.append(art_id)
            continue

        title = art["title"]
        content = art["content"] or art["summary"]
        source_category = art["category"]

        tagged_instruments, is_macro = postprocess_classification(
            title, content, source_category, tagged_instruments, is_macro,
            valid_symbols, name_lookup, instruments,
        )

        # Sector articles: route to sector sentiment pipeline
        is_sector = source_category.startswith("sector_")
        if is_sector:
            sector_name = source_category.replace("sector_", "")
            art["_tagged"] = tagged_instruments
            art["_is_macro"] = False
            art["_is_sector"] = True
            art["_sector"] = sector_name
            sector_articles.setdefault(sector_name, []).append(art)
            keep.append(art)
            # Also tag instruments if any were found
            if tagged_instruments:
                etf_relevance_sector: dict[str, float] = {}
                await update_article_tags(art_id, False, bool(tagged_instruments), tagged_instruments, instrument_ids, etf_relevance_sector)
            continue

        if not is_macro and not tagged_instruments:
            logger.info("Filtered irrelevant: '%s'", title[:60])
            to_delete.append(art_id)
            continue

        # Store article state for later steps
        art["_tagged"] = tagged_instruments
        art["_is_macro"] = is_macro
        keep.append(art)

        if is_macro:
            macro_articles.append(art)

        # Compute ETF relevance scores for instrument map
        etf_relevance: dict[str, float] = {}
        for symbol in list(tagged_instruments):
            # Propagate constituent tags to parent ETFs deterministically
            for etf_symbol, constituents in _ETF_CONSTITUENTS.items():
                if symbol in constituents:
                    if etf_symbol not in tagged_instruments:
                        tagged_instruments.append(etf_symbol)
                    
                    # Store relevance based on weight
                    weight_relevance = constituents[symbol] / 100.0
                    etf_relevance[etf_symbol] = max(
                        etf_relevance.get(etf_symbol, 0),
                        weight_relevance
                    )

        # Update DB flags + instrument map (with ETF-aware relevance scores)
        await update_article_tags(art_id, is_macro, bool(tagged_instruments), tagged_instruments, instrument_ids, etf_relevance)

    # --- Step 3: Delete spam/irrelevant ---
    for art_id in to_delete:
        await delete_article(art_id)

    if not keep:
        return

    # --- Step 4: Batch DUAL-HORIZON sentiment by instrument group ---
    # Build per-instrument buckets (keyed by first tagged instrument symbol)
    instrument_buckets: dict[str, list[dict]] = {}
    no_instrument_articles: list[dict] = []

    for art in keep:
        tagged = art.get("_tagged", [])
        if tagged:
            bucket_key = tagged[0]
            instrument_buckets.setdefault(bucket_key, []).append(art)
        else:
            no_instrument_articles.append(art)

    sentiment_stored: dict[str, bool] = {art["id"]: False for art in keep}

    # Adaptive sub-batch size based on queue depth
    queue_depth = len(articles)
    effective_sentiment_batch = get_adaptive_sub_batch(SENTIMENT_BATCH, queue_depth)

    # --- Confidence-based skip logic ---
    # For instruments with sufficiently high sentiment confidence, skip LLM
    # sentiment and assign neutral — the signal is already strong enough.
    skipped_instruments: set[str] = set()
    for symbol in list(instrument_buckets.keys()):
        inst = instruments_by_symbol.get(symbol)
        if not inst:
            continue
        iid = instrument_ids.get(symbol)
        if not iid:
            continue
        conf = await get_instrument_sentiment_confidence(iid)
        if (conf["confidence"] >= CONFIDENCE_SKIP_THRESHOLD
                and conf["non_neutral"] >= CONFIDENCE_SKIP_ARTICLE_MIN):
            # High confidence — skip LLM, assign neutral with low confidence
            skipped_instruments.add(symbol)
            for art in instrument_buckets[symbol]:
                await store_sentiment(art["id"], "neutral", 0.3, "neutral", 0.3)
                sentiment_stored[art["id"]] = True
            logger.info("Skipping sentiment for %s (confidence=%.2f, %d non-neutral articles)",
                        symbol, conf["confidence"], conf["non_neutral"])

    # Remove skipped instruments from buckets
    for sym in skipped_instruments:
        del instrument_buckets[sym]

    # --- Concurrent processing with weighted distribution ---
    # Asset/macro/sector sentiment all run in parallel with dedicated concurrency limits.
    asset_sem = asyncio.Semaphore(ASSET_CONCURRENCY)
    macro_sem = asyncio.Semaphore(MACRO_CONCURRENCY)
    sector_sem = asyncio.Semaphore(SECTOR_CONCURRENCY)

    async def _process_sentiment_chunk(chunk: list[dict], symbol: str, role: str, asset_desc: str) -> None:
        """Process a single sentiment chunk for an instrument (rate-limited by asset_sem)."""
        async with asset_sem:
            prompt = batch_sentiment_prompt(chunk, role, asset_desc)
            max_tok = SENTIMENT_TOKENS_PER_ARTICLE * len(chunk) + TOKENS_OVERHEAD
            raw_results = await generate_json_array(
                prompt, system=SENTIMENT_SYSTEM, max_tokens=max_tok
            )
        if raw_results is None:
            logger.warning("Batch sentiment failed for %s chunk of %d", symbol, len(chunk))
            for art in chunk:
                result = await _sentiment_single(art, role, asset_desc)
                if result:
                    short_label = result.get("short_sentiment", result.get("sentiment", "neutral"))
                    short_conf = float(result.get("short_confidence", result.get("confidence", 0.5)))
                    long_label = result.get("long_sentiment", "neutral")
                    long_conf = float(result.get("long_confidence", 0.5))
                    await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                    sentiment_stored[art["id"]] = True
        else:
            result_map = {item["id"]: item for item in raw_results if isinstance(item, dict) and "id" in item}
            for art in chunk:
                item = result_map.get(art["id"])
                if item:
                    short_label = item.get("short_sentiment", item.get("sentiment", "neutral"))
                    short_conf = float(item.get("short_confidence", item.get("confidence", 0.5)))
                    long_label = item.get("long_sentiment", "neutral")
                    long_conf = float(item.get("long_confidence", 0.5))
                    await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                    logger.info("Sentiment (batch) %s on '%s': short=%s(%.2f) long=%s(%.2f)",
                                symbol, art["title"][:40], short_label, short_conf, long_label, long_conf)
                    sentiment_stored[art["id"]] = True
                else:
                    result = await _sentiment_single(art, role, asset_desc)
                    if result:
                        short_label = result.get("short_sentiment", result.get("sentiment", "neutral"))
                        short_conf = float(result.get("short_confidence", result.get("confidence", 0.5)))
                        long_label = result.get("long_sentiment", "neutral")
                        long_conf = float(result.get("long_confidence", 0.5))
                        await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                        sentiment_stored[art["id"]] = True

    async def process_instrument_bucket(symbol: str, bucket: list[dict]) -> None:
        inst = instruments_by_symbol.get(symbol, {})
        needs_llm = bucket

        role = get_role(inst) if inst else f"You predict {symbol} price direction."
        asset_desc = get_asset_description(inst) if inst else f"{symbol} price"

        # Dispatch all sentiment chunks for this instrument in parallel
        # Each chunk acquires its own slot from asset_sem (10 concurrent max)
        chunks = [
            needs_llm[i:i + effective_sentiment_batch]
            for i in range(0, len(needs_llm), effective_sentiment_batch)
        ]
        if len(chunks) > 1:
            logger.info("Sentiment %s: dispatching %d parallel chunks (%d articles)", symbol, len(chunks), len(needs_llm))
        await asyncio.gather(*[
            _process_sentiment_chunk(c, symbol, role, asset_desc)
            for c in chunks
        ])

    # --- Parallel sentiment processing (asset + macro + sector simultaneously) ---
    # Weighted concurrency: 10 asset / 3 macro / 2 sector = 15 total concurrent API calls.

    async def _run_all_asset_sentiment():
        if not instrument_buckets:
            return
        total_articles = sum(len(b) for b in instrument_buckets.values())
        logger.info("Asset sentiment: dispatching %d instruments (%d articles, max %d concurrent)",
                     len(instrument_buckets), total_articles, ASSET_CONCURRENCY)
        await asyncio.gather(*[
            process_instrument_bucket(symbol, bucket)
            for symbol, bucket in instrument_buckets.items()
        ])

    async def _run_all_macro_sentiment():
        if not macro_articles:
            return
        macro_chunks = [
            macro_articles[i:i + MACRO_BATCH]
            for i in range(0, len(macro_articles), MACRO_BATCH)
        ]
        logger.info("Macro sentiment: dispatching %d parallel chunks (%d articles, max %d concurrent)",
                     len(macro_chunks), len(macro_articles), MACRO_CONCURRENCY)

        async def _macro_chunk(chunk):
            async with macro_sem:
                await _run_batch_macro_sentiment(chunk, sentiment_stored)

        await asyncio.gather(*[_macro_chunk(c) for c in macro_chunks])

    async def _run_all_sector_sentiment():
        sector_tasks = []
        for sector, s_articles in sector_articles.items():
            for i in range(0, len(s_articles), SECTOR_BATCH):
                chunk = s_articles[i:i + SECTOR_BATCH]
                sector_tasks.append((chunk, sector))
        if not sector_tasks:
            return
        logger.info("Sector sentiment: dispatching %d parallel chunks (max %d concurrent)",
                     len(sector_tasks), SECTOR_CONCURRENCY)

        async def _sector_chunk(chunk, sector):
            async with sector_sem:
                await _run_batch_sector_sentiment(chunk, sector, sentiment_stored)

        await asyncio.gather(*[_sector_chunk(c, s) for c, s in sector_tasks])

    # Run all three sentiment types simultaneously
    await asyncio.gather(
        _run_all_asset_sentiment(),
        _run_all_macro_sentiment(),
        _run_all_sector_sentiment(),
    )

    # --- Step 6: Mark processed ---
    for art in keep:
        art_id = art["id"]
        is_macro = art.get("_is_macro", False)
        is_sector = art.get("_is_sector", False)
        stored = sentiment_stored.get(art_id, False)

        if not stored and not is_macro and not is_sector:
            logger.warning("No sentiment for '%s' — skipping mark processed", art["title"][:60])
            continue

        await mark_processed(art_id)
        logger.info("Processed: '%s' (instruments=%s, macro=%s)",
                    art["title"][:60], art.get("_tagged", []), is_macro)


async def _classify_single(article: dict, symbol_mapping: str, valid_symbols_str: str) -> dict | None:
    """Fallback: classify a single article with the old single-call approach."""
    title = article["title"]
    content = article["content"] or article["summary"]
    prompt = classify_prompt(title, content, symbol_mapping, valid_symbols_str)
    result = await generate_json(prompt, system=CLASSIFY_SYSTEM, max_tokens=300)
    if result:
        result["id"] = article["id"]
    return result


async def _sentiment_single(article: dict, role: str, asset_desc: str) -> dict | None:
    """Fallback: dual-horizon sentiment for a single article."""
    title = article["title"]
    content = article["content"] or article["summary"]
    prompt = sentiment_prompt(title, content, role, asset_desc)
    result = await generate_json(prompt, system=SENTIMENT_SYSTEM, max_tokens=100)
    if not result:
        result = await generate_json(prompt, system=SENTIMENT_SYSTEM, max_tokens=100)
    return result


async def _run_batch_macro_sentiment(articles: list[dict], sentiment_stored: dict[str, bool]) -> None:
    """Run dual-horizon macro sentiment in batch; fall back to individual LLM."""
    needs_llm = articles

    prompt = batch_macro_sentiment_prompt(needs_llm)
    max_tok = MACRO_TOKENS_PER_ARTICLE * len(needs_llm) + TOKENS_OVERHEAD
    raw_results = await generate_json_array(
        prompt, system=MACRO_SYSTEM, max_tokens=max_tok
    )

    _MACRO_LABEL_MAP = {
        "good": "positive", "bad": "negative", "mixed": "neutral",
        "very_good": "very_positive", "very_bad": "very_negative",
        "very good": "very_positive", "very bad": "very_negative",
        "GOOD": "positive", "BAD": "negative", "MIXED": "neutral",
        "VERY_GOOD": "very_positive", "VERY_BAD": "very_negative",
    }
    valid_labels = {"very_positive", "positive", "neutral", "negative", "very_negative"}

    def _normalize_macro_label(raw: str) -> str:
        raw = raw.strip().lower().strip("<>")
        label = _MACRO_LABEL_MAP.get(raw, raw)
        if label not in valid_labels:
            label = "neutral"
        return label

    if raw_results is not None:
        result_map = {item["id"]: item for item in raw_results if isinstance(item, dict) and "id" in item}
        for art in needs_llm:
            item = result_map.get(art["id"])
            if item:
                # Extract dual-horizon macro sentiment
                raw_short = item.get("short_sentiment", item.get("sentiment", "neutral"))
                short_conf = float(item.get("short_confidence", item.get("confidence", 0.5)))
                raw_long = item.get("long_sentiment", "neutral")
                long_conf = float(item.get("long_confidence", 0.5))

                short_label = _normalize_macro_label(str(raw_short))
                long_label = _normalize_macro_label(str(raw_long))

                logger.info("Macro (batch) '%s': short=%s(%.2f) long=%s(%.2f)",
                            art["title"][:40], short_label, short_conf, long_label, long_conf)

                store_in_scores = not sentiment_stored.get(art["id"], False)
                if store_in_scores:
                    await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                    sentiment_stored[art["id"]] = True
                await store_macro_label(art["id"], short_label, long_label)
            else:
                # fallback individual
                await run_macro_sentiment(
                    art["id"], art["title"], art["content"] or art["summary"],
                    store_in_scores=not sentiment_stored.get(art["id"], False)
                )
                sentiment_stored[art["id"]] = True
    else:
        # Batch failed — fallback individual
        for art in needs_llm:
            await run_macro_sentiment(
                art["id"], art["title"], art["content"] or art["summary"],
                store_in_scores=not sentiment_stored.get(art["id"], False)
            )
            sentiment_stored[art["id"]] = True


async def _run_batch_sector_sentiment(articles: list[dict], sector: str, sentiment_stored: dict[str, bool]) -> None:
    """Run dual-horizon sector sentiment in batch; store labels for sector aggregation."""
    _LABEL_MAP = {
        "good": "positive", "bad": "negative", "mixed": "neutral",
        "very_good": "very_positive", "very_bad": "very_negative",
        "very good": "very_positive", "very bad": "very_negative",
    }
    valid_labels = {"very_positive", "positive", "neutral", "negative", "very_negative"}

    def _normalize(raw: str) -> str:
        raw = raw.strip().lower().strip("<>")
        label = _LABEL_MAP.get(raw, raw)
        return label if label in valid_labels else "neutral"

    prompt = batch_sector_sentiment_prompt(articles, sector)
    max_tok = MACRO_TOKENS_PER_ARTICLE * len(articles) + TOKENS_OVERHEAD
    raw_results = await generate_json_array(
        prompt, system=SECTOR_SENTIMENT_SYSTEM, max_tokens=max_tok
    )

    if raw_results is not None:
        result_map = {item["id"]: item for item in raw_results if isinstance(item, dict) and "id" in item}
        for art in articles:
            item = result_map.get(art["id"])
            if item:
                short_label = _normalize(str(item.get("short_sentiment", "neutral")))
                long_label = _normalize(str(item.get("long_sentiment", "neutral")))
                # Store as macro_sentiment_label for sector aggregation
                await store_macro_label(art["id"], short_label, long_label)
                if not sentiment_stored.get(art["id"], False):
                    short_conf = float(item.get("short_confidence", 0.5))
                    long_conf = float(item.get("long_confidence", 0.5))
                    await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                    sentiment_stored[art["id"]] = True
                logger.info("Sector sentiment (%s) '%s': short=%s long=%s",
                            sector, art["title"][:40], short_label, long_label)
            else:
                # Default to neutral for missing results
                await store_macro_label(art["id"], "neutral", "neutral")
                if not sentiment_stored.get(art["id"], False):
                    await store_sentiment(art["id"], "neutral", 0.3, "neutral", 0.3)
                    sentiment_stored[art["id"]] = True
    else:
        # Batch failed — store neutral defaults
        for art in articles:
            await store_macro_label(art["id"], "neutral", "neutral")
            if not sentiment_stored.get(art["id"], False):
                await store_sentiment(art["id"], "neutral", 0.3, "neutral", 0.3)
                sentiment_stored[art["id"]] = True


# ---------------------------------------------------------------------------
# Legacy per-article entrypoint (kept for backward compat / fallback)
# ---------------------------------------------------------------------------

async def process_article(
    article: dict,
    instrument_ids: dict[str, str],
    valid_symbols: set[str],
    instruments: list[dict],
    instruments_by_symbol: dict[str, dict],
    symbol_mapping: str,
    valid_symbols_str: str,
    name_lookup: dict[str, list[str]],
) -> None:
    """Process a single article (fallback — use process_batch instead)."""
    await process_batch(
        [article],
        instrument_ids, valid_symbols, instruments, instruments_by_symbol,
        symbol_mapping, valid_symbols_str, name_lookup,
    )


async def run_macro_sentiment(article_id: str, title: str, content: str, store_in_scores: bool = True) -> None:
    """Run dual-horizon macro-level sentiment analysis (individual fallback)."""
    prompt = macro_sentiment_prompt(title, content)
    sentiment_result = await generate_json(prompt, system=MACRO_SYSTEM, max_tokens=100)

    _MACRO_LABEL_MAP = {
        "good": "positive", "bad": "negative", "mixed": "neutral",
        "very_good": "very_positive", "very_bad": "very_negative",
        "very good": "very_positive", "very bad": "very_negative",
    }
    valid_labels = {"very_positive", "positive", "neutral", "negative", "very_negative"}

    if sentiment_result:
        raw_short = sentiment_result.get("short_sentiment", sentiment_result.get("sentiment", "neutral"))
        short_conf = float(sentiment_result.get("short_confidence", sentiment_result.get("confidence", 0.5)))
        raw_long = sentiment_result.get("long_sentiment", "neutral")
        long_conf = float(sentiment_result.get("long_confidence", 0.5))

        short_label = _MACRO_LABEL_MAP.get(str(raw_short).strip().lower().strip("<>"), str(raw_short).strip().lower())
        long_label = _MACRO_LABEL_MAP.get(str(raw_long).strip().lower().strip("<>"), str(raw_long).strip().lower())

        if short_label not in valid_labels:
            short_label = "neutral"
        if long_label not in valid_labels:
            long_label = "neutral"

        logger.info("Macro sentiment (LLM) for '%s': short=%s(%.2f) long=%s(%.2f)",
                    title[:40], short_label, short_conf, long_label, long_conf)
    else:
        short_label = "neutral"
        long_label = "neutral"
        short_conf = 0.3
        long_conf = 0.3

    if store_in_scores:
        await store_sentiment(article_id, short_label, short_conf, long_label, long_conf)
    await store_macro_label(article_id, short_label, long_label)


async def update_article_tags(
    article_id: str,
    is_macro: bool,
    is_asset_specific: bool,
    instruments: list[str],
    instrument_ids: dict[str, str],
    etf_relevance: dict[str, float] | None = None,
) -> None:
    """Update article flags and create instrument mappings with ETF-aware relevance."""
    if etf_relevance is None:
        etf_relevance = {}

    async with async_session() as session:
        await session.execute(
            text("""
                UPDATE news_articles
                SET is_macro = :is_macro, is_asset_specific = :is_asset
                WHERE id = :id
            """),
            {"id": article_id, "is_macro": is_macro, "is_asset": is_asset_specific},
        )

        await session.execute(
            text("DELETE FROM news_instrument_map WHERE article_id = :aid"),
            {"aid": article_id},
        )

        for symbol in instruments:
            iid = instrument_ids.get(symbol)
            if iid:
                # Use ETF-weighted relevance if this is a propagated ETF tag
                relevance = etf_relevance.get(symbol, 1.0)
                await session.execute(
                    text("""
                        INSERT INTO news_instrument_map (article_id, instrument_id, relevance_score)
                        VALUES (:aid, :iid, :rel)
                    """),
                    {"aid": article_id, "iid": iid, "rel": relevance},
                )

        await session.commit()


async def store_sentiment(
    article_id: str,
    sentiment_label: str,
    confidence: float,
    long_term_label: str = "neutral",
    long_term_confidence: float = 0.5,
) -> None:
    """Store dual-horizon sentiment score in the database."""
    # Normalize short-term label
    label = sentiment_label.lower().replace("_", " ").strip().strip("<>")
    if label not in SENTIMENT_LABEL_MAP.values():
        label = SENTIMENT_LABEL_MAP.get(sentiment_label.lower().replace(" ", "_"), "neutral")

    # Normalize long-term label
    lt_label = long_term_label.lower().replace("_", " ").strip().strip("<>")
    if lt_label not in SENTIMENT_LABEL_MAP.values():
        lt_label = SENTIMENT_LABEL_MAP.get(long_term_label.lower().replace(" ", "_"), "neutral")

    # Compute probability distribution from short-term label (primary display)
    key = label.replace(" ", "_")
    probs = SENTIMENT_PROBABILITIES.get(key, SENTIMENT_PROBABILITIES["neutral"])

    pos = round(probs["positive"] * confidence + 0.15 * (1 - confidence), 6)
    neg = round(probs["negative"] * confidence + 0.15 * (1 - confidence), 6)
    neu = round(1.0 - pos - neg, 6)
    if neu < 0:
        neu = 0.0
        total = pos + neg
        pos, neg = pos / total, neg / total

    async with async_session() as session:
        try:
            await session.execute(
                text("""
                    INSERT INTO sentiment_scores (article_id, positive, negative, neutral, label,
                                                  long_term_label, long_term_confidence)
                    VALUES (:aid, :pos, :neg, :neu, :label, :lt_label, :lt_conf)
                    ON CONFLICT (article_id) DO UPDATE
                    SET positive = :pos, negative = :neg, neutral = :neu, label = :label,
                        long_term_label = :lt_label, long_term_confidence = :lt_conf
                """),
                {"aid": article_id, "pos": pos, "neg": neg, "neu": neu,
                 "label": label, "lt_label": lt_label, "lt_conf": long_term_confidence},
            )
            await session.commit()
        except Exception as e:
            logger.warning("Could not store sentiment for article %s: %s", article_id, e)


async def store_macro_label(article_id: str, short_label: str, long_label: str = "neutral") -> None:
    """Store dual-horizon macro sentiment labels directly on the article."""
    s_label = short_label.lower().replace("_", " ").strip().strip("<>")
    if s_label not in SENTIMENT_LABEL_MAP.values():
        s_label = SENTIMENT_LABEL_MAP.get(short_label.lower().replace(" ", "_"), "neutral")

    l_label = long_label.lower().replace("_", " ").strip().strip("<>")
    if l_label not in SENTIMENT_LABEL_MAP.values():
        l_label = SENTIMENT_LABEL_MAP.get(long_label.lower().replace(" ", "_"), "neutral")

    async with async_session() as session:
        try:
            await session.execute(
                text("""
                    UPDATE news_articles
                    SET macro_sentiment_label = :s_label, macro_long_term_label = :l_label
                    WHERE id = :id
                """),
                {"id": article_id, "s_label": s_label, "l_label": l_label},
            )
            await session.commit()
        except Exception as e:
            logger.warning("Could not store macro label for article %s: %s", article_id, e)


async def mark_processed(article_id: str) -> None:
    """Mark article as processed and clear content to save storage."""
    async with async_session() as session:
        await session.execute(
            text("""
                UPDATE news_articles
                SET ollama_processed = true, content = NULL
                WHERE id = :id
            """),
            {"id": article_id},
        )
        await session.commit()


async def delete_article(article_id: str) -> None:
    """Delete a spam/irrelevant article."""
    async with async_session() as session:
        await session.execute(
            text("DELETE FROM news_articles WHERE id = :id"),
            {"id": article_id},
        )
        await session.commit()


async def update_macro_sentiment() -> None:
    """Calculate aggregate global macro sentiment from dual-horizon labels on articles.

    Produces BOTH short-term and long-term macro sentiment records.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=72)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT COALESCE(macro_sentiment_label, 'neutral') AS macro_sentiment_label,
                       COALESCE(macro_long_term_label, 'neutral') AS macro_long_term_label
                FROM news_articles
                WHERE is_macro = true
                AND ollama_processed = true
                AND published_at >= :cutoff
                ORDER BY published_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()

        if not rows:
            return

        total_cnt = len(rows)

        # --- Short-term macro ---
        short_scores = [SENTIMENT_MULTIPLIERS.get(r.macro_sentiment_label, 0.0) for r in rows
                        if r.macro_sentiment_label != "neutral"]
        if short_scores:
            short_net = sum(short_scores) / len(short_scores)
            short_net = max(-3.0, min(3.0, short_net))
            short_label = "positive" if short_net > 0.75 else ("negative" if short_net < -0.75 else "neutral")

            await session.execute(
                text("""
                    INSERT INTO macro_sentiment (region, term, score, label, article_count, calculated_at)
                    VALUES ('global', 'short', :score, :label, :count, NOW())
                """),
                {"score": short_net, "label": short_label, "count": total_cnt},
            )

        # --- Long-term macro ---
        long_scores = [SENTIMENT_MULTIPLIERS.get(r.macro_long_term_label, 0.0) for r in rows
                       if r.macro_long_term_label != "neutral"]
        if long_scores:
            long_net = sum(long_scores) / len(long_scores)
            long_net = max(-3.0, min(3.0, long_net))
            long_label = "positive" if long_net > 0.75 else ("negative" if long_net < -0.75 else "neutral")

            await session.execute(
                text("""
                    INSERT INTO macro_sentiment (region, term, score, label, article_count, calculated_at)
                    VALUES ('global', 'long', :score, :label, :count, NOW())
                """),
                {"score": long_net, "label": long_label, "count": total_cnt},
            )

        # Clean up old records (keep last 100 per term)
        await session.execute(
            text("""
                DELETE FROM macro_sentiment
                WHERE id NOT IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY term ORDER BY calculated_at DESC) as rn
                        FROM macro_sentiment
                    ) ranked WHERE rn <= 100
                )
            """)
        )

        await session.commit()
    logger.info("Global macro sentiment updated (%d articles, short+long)", total_cnt)


async def cleanup_priority() -> None:
    """Remove priority entries for instruments whose articles are all processed."""
    async with async_session() as session:
        await session.execute(
            text("""
                DELETE FROM processing_priority pp
                WHERE NOT EXISTS (
                    SELECT 1 FROM news_instrument_map nim
                    JOIN news_articles a ON a.id = nim.article_id
                    WHERE nim.instrument_id = pp.instrument_id
                    AND a.ollama_processed = false
                )
            """)
        )
        await session.commit()


# ---------------------------------------------------------------------------
# GICS Sector Assignment
# ---------------------------------------------------------------------------

# Known sector assignments — deterministic fallback when LLM is unavailable
_KNOWN_SECTORS: dict[str, str | None] = {
    "RTX": "industrials",
    "NVDA": "technology",
    "GOOGL": "communication",
    "AAPL": "technology",
    "TSLA": "consumer_discretionary",
    "PLTR": "technology",
    "LLY": "healthcare",
    "NVO": "healthcare",
    "WMT": "consumer_staples",
    "XOM": "energy",
    "IITU": "technology",
    "SMH": "technology",
    "VOO": None,
    "GOLD": "materials",
    "OIL": "energy",
}

VALID_SECTORS = {
    "technology", "financials", "healthcare", "consumer_discretionary",
    "consumer_staples", "communication", "energy", "industrials",
    "materials", "utilities", "real_estate",
}


async def assign_sectors(instruments: list[dict]) -> None:
    """Assign GICS sectors to instruments that don't have one yet.

    Uses LLM for classification with deterministic fallback for known instruments.
    This runs once per instrument — sectors are stable and rarely change.
    """
    # Find instruments without sectors
    needs_sector = [i for i in instruments if not i.get("sector")]
    if not needs_sector:
        return

    logger.info("Assigning GICS sectors for %d instruments...", len(needs_sector))

    # Try deterministic assignment first
    remaining = []
    async with async_session() as session:
        for inst in needs_sector:
            known = _KNOWN_SECTORS.get(inst["symbol"])
            if known is not None or inst["symbol"] in _KNOWN_SECTORS:
                if known and known in VALID_SECTORS:
                    await session.execute(
                        text("UPDATE instruments SET sector = :sector WHERE id = :id"),
                        {"sector": known, "id": inst["id"]},
                    )
                    inst["sector"] = known
                    logger.info("Sector (known): %s → %s", inst["symbol"], known)
                else:
                    logger.info("Sector (known): %s → null (broad-market)", inst["symbol"])
            else:
                remaining.append(inst)
        await session.commit()

    if not remaining:
        return

    # Use LLM for unknown instruments
    prompt = sector_classify_prompt(remaining)
    result = await generate_json(
        prompt,
        system="You are a financial classification expert. Always respond with valid JSON.",
        max_tokens=500,
    )

    if result and "results" in result:
        async with async_session() as session:
            for item in result["results"]:
                if not isinstance(item, dict):
                    continue
                symbol = item.get("symbol", "").upper()
                sector = item.get("sector")
                if sector and sector in VALID_SECTORS:
                    await session.execute(
                        text("UPDATE instruments SET sector = :sector WHERE symbol = :symbol"),
                        {"sector": sector, "symbol": symbol},
                    )
                    # Update in-memory
                    for inst in remaining:
                        if inst["symbol"] == symbol:
                            inst["sector"] = sector
                    logger.info("Sector (LLM): %s → %s", symbol, sector)
            await session.commit()
    else:
        logger.warning("LLM sector classification failed, instruments without sectors: %s",
                       [i["symbol"] for i in remaining])


# ---------------------------------------------------------------------------
# Sector Sentiment Aggregation
# ---------------------------------------------------------------------------

async def update_sector_sentiment() -> None:
    """Calculate aggregate sector sentiment from sector news articles.

    Produces BOTH short-term and long-term sector sentiment records for each sector.
    Uses the same dual-horizon labels as macro sentiment.
    """
    # Increase window to 14 days for sector aggregation.
    # 3 days was too restrictive, especially when news volume for certain sectors is low.
    cutoff = datetime.now(timezone.utc) - timedelta(days=14)

    async with async_session() as session:
        # Get all sector categories with recent processed articles
        result = await session.execute(
            text("""
                SELECT category,
                       COALESCE(macro_sentiment_label, 'neutral') AS short_label,
                       COALESCE(macro_long_term_label, 'neutral') AS long_label
                FROM news_articles
                WHERE category LIKE 'sector_%%'
                AND ollama_processed = true
                AND published_at >= :cutoff
                ORDER BY category, published_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()

        if not rows:
            return

        # Group by sector
        sector_articles: dict[str, list] = {}
        for r in rows:
            # Extract sector from category: 'sector_technology' -> 'technology'
            sector = r.category.replace("sector_", "")
            sector_articles.setdefault(sector, []).append(r)

        for sector, articles in sector_articles.items():
            total_cnt = len(articles)

            # Short-term: always insert a record so "all neutral" is distinguishable from "no data"
            short_scores = [SENTIMENT_MULTIPLIERS.get(a.short_label, 0.0) for a in articles
                            if a.short_label != "neutral"]
            if short_scores:
                short_net = sum(short_scores) / len(short_scores)
            else:
                short_net = 0.0
            short_net = max(-1.0, min(1.0, short_net))
            short_label = "positive" if short_net > 0.08 else ("negative" if short_net < -0.08 else "neutral")

            await session.execute(
                text("""
                    INSERT INTO sector_sentiment (sector, term, score, label, article_count, calculated_at)
                    VALUES (:sector, 'short', :score, :label, :count, NOW())
                """),
                {"sector": sector, "score": short_net, "label": short_label, "count": total_cnt},
            )

            # Long-term: always insert
            long_scores = [SENTIMENT_MULTIPLIERS.get(a.long_label, 0.0) for a in articles
                           if a.long_label != "neutral"]
            if long_scores:
                long_net = sum(long_scores) / len(long_scores)
            else:
                long_net = 0.0
            long_net = max(-1.0, min(1.0, long_net))
            long_label = "positive" if long_net > 0.08 else ("negative" if long_net < -0.08 else "neutral")

            await session.execute(
                text("""
                    INSERT INTO sector_sentiment (sector, term, score, label, article_count, calculated_at)
                    VALUES (:sector, 'long', :score, :label, :count, NOW())
                """),
                {"sector": sector, "score": long_net, "label": long_label, "count": total_cnt},
            )

        # Clean up old records (keep last 100 per sector per term)
        await session.execute(
            text("""
                DELETE FROM sector_sentiment
                WHERE id NOT IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY sector, term ORDER BY calculated_at DESC) as rn
                        FROM sector_sentiment
                    ) ranked WHERE rn <= 100
                )
            """)
        )

        await session.commit()
    logger.info("Sector sentiment updated for %d sectors", len(sector_articles))
