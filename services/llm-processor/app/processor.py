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
from .nim_client import generate_json, generate_json_array
from .prompts import (
    CLASSIFY_SYSTEM,
    SENTIMENT_SYSTEM,
    MACRO_SYSTEM,
    classify_prompt,
    batch_classify_prompt,
    batch_sentiment_prompt,
    batch_macro_sentiment_prompt,
    sentiment_prompt,
    macro_sentiment_prompt,
    etf_constituent_prompt,
    build_instrument_context,
    get_role,
    get_asset_description,
)

logger = logging.getLogger(__name__)

PROCESS_INTERVAL = 3  # seconds between processing cycles
BATCH_SIZE = 30

# Sub-batch sizes for LLM calls (keep prompts from getting too long)
CLASSIFY_BATCH = 8    # articles per classify API call
SENTIMENT_BATCH = 6   # articles per sentiment API call (per instrument)
MACRO_BATCH = 3       # macro articles per macro-sentiment API call (small = avoids truncation)

# Token budgets per article (generous to avoid truncation → fallback burst)
CLASSIFY_TOKENS_PER_ARTICLE = 130   # {"id","type","instruments","is_macro"} × N
SENTIMENT_TOKENS_PER_ARTICLE = 80   # increased: dual-horizon {"id","short_sentiment","short_confidence","long_sentiment","long_confidence"} × N
MACRO_TOKENS_PER_ARTICLE = 140      # increased: dual-horizon macro
TOKENS_OVERHEAD = 150               # wrapper object + whitespace slack

# Small sleep between consecutive sub-batch chunks to smooth rate-limit pressure
INTER_CHUNK_DELAY = 1.0  # seconds

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
    "gold": ["gold price", "gold futures", "gold market", "gold rally", "gold surge", "gold drop", "bullion"],
    "oil": ["oil price", "crude oil", "oil futures", "oil market", "brent", "wti"],
    "silver": ["silver price", "silver futures", "silver market"],
    "natural gas": ["natural gas price", "gas futures", "henry hub"],
    "copper": ["copper price", "copper futures"],
}

# ETF constituent cache: etf_symbol -> {constituent_symbol: weight_percent}
_ETF_CONSTITUENTS: dict[str, dict[str, float]] = {}


async def get_instruments() -> list[dict]:
    """Load all tracked instruments from the database."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category FROM instruments ORDER BY symbol")
        )
        return [
            {
                "id": str(row.id),
                "symbol": row.symbol,
                "name": row.name,
                "category": row.category,
            }
            for row in result.fetchall()
        ]


def build_name_lookup(instruments: list[dict]) -> dict[str, list[str]]:
    """Build a symbol -> [search names] map dynamically from instrument data."""
    lookup = {}
    for inst in instruments:
        symbol = inst["symbol"]
        name = inst["name"].lower()
        category = inst["category"]

        if category == "commodity":
            lookup[symbol] = []
        else:
            clean = re.sub(r'\b(inc\.?|corp\.?|corporation|ltd\.?|llc|plc|etf|futures|co\.?)\b', '', name, flags=re.I)
            clean = clean.strip().strip(',').strip()
            names = [n.strip() for n in clean.split() if len(n.strip()) > 2]
            search_names = []
            if clean and len(clean) > 2:
                search_names.append(clean)
            search_names.extend(names)
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
    """Check which tracked instruments are directly mentioned by name in text."""
    combined = f"{title} {content[:500]}".lower()
    mentioned = set()

    for symbol, names in name_lookup.items():
        if symbol.lower() in combined:
            mentioned.add(symbol)
            continue
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
    if source_category in _MACRO_CATEGORIES:
        llm_is_macro = True

    if source_category == "asset_specific":
        if llm_is_macro and not _MACRO_PATTERNS.search(combined):
            llm_is_macro = False

    if source_category == "asset_specific" and llm_is_macro and _ASSET_SPECIFIC_PATTERNS.search(title):
        if not _MACRO_PATTERNS.search(combined):
            llm_is_macro = False

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

    Priority ordering:
      0 - User-prioritized instruments (clicked in frontend)
      1 - Macro articles (until 10+ macro articles are processed, then same as asset)
      2 - Asset-specific articles
    """
    async with async_session() as session:
        macro_count_res = await session.execute(
            text("SELECT count(*) FROM news_articles WHERE is_macro = true AND ollama_processed = true")
        )
        macro_processed = macro_count_res.scalar() or 0
        macro_priority = 1 if macro_processed >= 10 else 0

        result = await session.execute(
            text("""
                SELECT a.id, a.title, a.summary, a.content, a.category
                FROM news_articles a
                LEFT JOIN LATERAL (
                    SELECT pp.requested_at
                    FROM news_instrument_map nim
                    JOIN processing_priority pp ON pp.instrument_id = nim.instrument_id
                    WHERE nim.article_id = a.id
                    ORDER BY pp.requested_at DESC
                    LIMIT 1
                ) pri ON true
                WHERE a.ollama_processed = false
                ORDER BY
                    CASE WHEN pri.requested_at IS NOT NULL THEN 0
                         WHEN a.category LIKE 'macro_%%' THEN :macro_pri
                         ELSE 2
                    END,
                    COALESCE(pri.requested_at, '1970-01-01'::timestamptz) DESC,
                    a.published_at DESC
                LIMIT :limit
            """),
            {"limit": limit, "macro_pri": macro_priority},
        )
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

    # --- Step 1: Batch classify ---
    classify_results: dict[str, dict] = {}  # article_id -> classification
    for chunk_start in range(0, len(articles), CLASSIFY_BATCH):
        if chunk_start > 0:
            await asyncio.sleep(INTER_CHUNK_DELAY)
        chunk = articles[chunk_start:chunk_start + CLASSIFY_BATCH]
        prompt = batch_classify_prompt(chunk, symbol_mapping, valid_symbols_str)
        max_tok = CLASSIFY_TOKENS_PER_ARTICLE * len(chunk) + TOKENS_OVERHEAD
        raw_results = await generate_json_array(
            prompt, system=CLASSIFY_SYSTEM, max_tokens=max_tok
        )
        if raw_results is None:
            # Fallback: try one-at-a-time for this chunk
            logger.warning("Batch classify failed for chunk of %d, falling back to individual calls", len(chunk))
            for art in chunk:
                result = await _classify_single(art, symbol_mapping, valid_symbols_str)
                if result:
                    classify_results[art["id"]] = result
        else:
            for item in raw_results:
                if isinstance(item, dict) and "id" in item:
                    classify_results[item["id"]] = item
            # Articles missing from response: retry individually
            found_ids = {item["id"] for item in raw_results if isinstance(item, dict) and "id" in item}
            for art in chunk:
                if art["id"] not in found_ids:
                    logger.warning("Article %s missing from batch classify response, retrying individually", art["id"])
                    result = await _classify_single(art, symbol_mapping, valid_symbols_str)
                    if result:
                        classify_results[art["id"]] = result

    # --- Step 2: Post-process classifications ---
    article_map = {a["id"]: a for a in articles}

    keep: list[dict] = []          # articles to run sentiment on
    macro_articles: list[dict] = [] # macro articles needing macro-sentiment
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

    for symbol, bucket in instrument_buckets.items():
        inst = instruments_by_symbol.get(symbol, {})

        needs_llm = bucket

        role = get_role(inst) if inst else f"You predict {symbol} price direction."
        asset_desc = get_asset_description(inst) if inst else f"{symbol} price"

        for chunk_start in range(0, len(needs_llm), SENTIMENT_BATCH):
            if chunk_start > 0:
                await asyncio.sleep(INTER_CHUNK_DELAY)
            chunk = needs_llm[chunk_start:chunk_start + SENTIMENT_BATCH]
            prompt = batch_sentiment_prompt(chunk, role, asset_desc)
            max_tok = SENTIMENT_TOKENS_PER_ARTICLE * len(chunk) + TOKENS_OVERHEAD
            raw_results = await generate_json_array(
                prompt, system=SENTIMENT_SYSTEM, max_tokens=max_tok
            )
            if raw_results is None:
                # Fallback: individual calls
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
                        # Extract dual-horizon sentiment
                        short_label = item.get("short_sentiment", item.get("sentiment", "neutral"))
                        short_conf = float(item.get("short_confidence", item.get("confidence", 0.5)))
                        long_label = item.get("long_sentiment", "neutral")
                        long_conf = float(item.get("long_confidence", 0.5))
                        await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                        logger.info("Sentiment (batch) %s on '%s': short=%s(%.2f) long=%s(%.2f)",
                                    symbol, art["title"][:40], short_label, short_conf, long_label, long_conf)
                        sentiment_stored[art["id"]] = True
                    else:
                        # Missing from batch response — fallback
                        result = await _sentiment_single(art, role, asset_desc)
                        if result:
                            short_label = result.get("short_sentiment", result.get("sentiment", "neutral"))
                            short_conf = float(result.get("short_confidence", result.get("confidence", 0.5)))
                            long_label = result.get("long_sentiment", "neutral")
                            long_conf = float(result.get("long_confidence", 0.5))
                            await store_sentiment(art["id"], short_label, short_conf, long_label, long_conf)
                            sentiment_stored[art["id"]] = True

    # --- Step 5: Batch DUAL-HORIZON macro sentiment ---
    if macro_articles:
        for chunk_start in range(0, len(macro_articles), MACRO_BATCH):
            chunk = macro_articles[chunk_start:chunk_start + MACRO_BATCH]
            await _run_batch_macro_sentiment(chunk, sentiment_stored)

    # --- Step 6: Mark processed ---
    for art in keep:
        art_id = art["id"]
        is_macro = art.get("_is_macro", False)
        stored = sentiment_stored.get(art_id, False)

        if not stored and not is_macro:
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
