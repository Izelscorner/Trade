"""Main processing pipeline: classifies and scores unprocessed news articles via Cerebras API.

Batch processing strategy:
  - Classification: N articles → 1 API call → JSON array of results
  - Sentiment: group articles by first instrument → 1 API call per instrument bucket → JSON array
  - Macro sentiment: N macro articles → 1 API call → JSON array of results
This dramatically reduces the number of API calls to Cerebras.
"""

import asyncio
import re
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session
from .cerebras_client import generate_json, generate_json_array
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
    build_instrument_context,
    get_role,
    get_asset_description,
)

logger = logging.getLogger(__name__)

PROCESS_INTERVAL = 15  # seconds between processing cycles
BATCH_SIZE = 20

# Sub-batch sizes for LLM calls (keep prompts from getting too long)
CLASSIFY_BATCH = 8    # articles per classify API call
SENTIMENT_BATCH = 6   # articles per sentiment API call (per instrument)
MACRO_BATCH = 3       # macro articles per macro-sentiment API call (small = avoids truncation)

# Token budgets per article (generous to avoid truncation → fallback burst)
CLASSIFY_TOKENS_PER_ARTICLE = 130   # {"id","type","instruments","is_macro"} × N
SENTIMENT_TOKENS_PER_ARTICLE = 55   # {"id","sentiment","confidence"} × N
MACRO_TOKENS_PER_ARTICLE = 120      # generous: model uses verbose formatting (MIXED/BAD/GOOD + newlines)
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

    foreign_tickers = re.findall(r'\$([A-Z]{1,5})\b', title)
    if foreign_tickers:
        foreign_tickers_set = set(foreign_tickers)
        tracked_in_title = foreign_tickers_set & valid_symbols
        if not tracked_in_title and foreign_tickers_set:
            direct = _check_direct_mention(title, content, name_lookup, instruments)
            llm_instruments = [s for s in llm_instruments if s in direct]

    if llm_instruments:
        direct = _check_direct_mention(title, content, name_lookup, instruments)
        validated = [s for s in llm_instruments if s in direct]
        if validated != llm_instruments:
            logger.debug("Post-proc: filtered instruments %s -> %s for: '%s'", llm_instruments, validated, title[:60])
            llm_instruments = validated

    if not llm_instruments:
        direct = _check_direct_mention(title, "", name_lookup, instruments)
        if direct:
            llm_instruments = list(direct)

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
      2. Apply deterministic post-processing per article
      3. Filter spam / irrelevant articles
      4. Group remaining articles by first instrument, batch-sentiment per group
      5. Batch macro-sentiment for all macro articles
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

        # Extract symbols
        tagged_instruments = []
        for inst in raw_instruments:
            if isinstance(inst, dict):
                for key in inst.keys():
                    sym = key.strip().upper()
                    if sym in valid_symbols:
                        tagged_instruments.append(sym)
            elif isinstance(inst, str):
                symbol = inst.split(" ")[0].split("-")[0].strip().upper()
                if symbol in valid_symbols:
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

        # Update DB flags + instrument map
        await update_article_tags(art_id, is_macro, bool(tagged_instruments), tagged_instruments, instrument_ids)

    # --- Step 3: Delete spam/irrelevant ---
    for art_id in to_delete:
        await delete_article(art_id)

    if not keep:
        return

    # --- Step 4: Batch sentiment by instrument group ---
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

        # First pass: deterministic sentiment
        needs_llm = []
        for art in bucket:
            det = _deterministic_instrument_sentiment(
                art["title"], art["content"] or art["summary"],
                inst.get("category", "stock")
            )
            if det:
                s_label, s_conf = det
                await store_sentiment(art["id"], s_label, s_conf)
                logger.info("Sentiment (det) %s on '%s': %s", symbol, art["title"][:40], s_label)
                sentiment_stored[art["id"]] = True
            else:
                needs_llm.append(art)

        if not needs_llm:
            continue

        # Second pass: batch LLM sentiment
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
                        await store_sentiment(art["id"], result.get("sentiment", "neutral"),
                                              float(result.get("confidence", 0.5)))
                        sentiment_stored[art["id"]] = True
            else:
                result_map = {item["id"]: item for item in raw_results if isinstance(item, dict) and "id" in item}
                for art in chunk:
                    item = result_map.get(art["id"])
                    if item:
                        s_label = item.get("sentiment", "neutral")
                        s_conf = float(item.get("confidence", 0.5))
                        await store_sentiment(art["id"], s_label, s_conf)
                        logger.info("Sentiment (batch) %s on '%s': %s (%.2f)", symbol, art["title"][:40], s_label, s_conf)
                        sentiment_stored[art["id"]] = True
                    else:
                        # Missing from batch response — fallback
                        result = await _sentiment_single(art, role, asset_desc)
                        if result:
                            await store_sentiment(art["id"], result.get("sentiment", "neutral"),
                                                  float(result.get("confidence", 0.5)))
                            sentiment_stored[art["id"]] = True

    # --- Step 5: Batch macro sentiment ---
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
    """Fallback: sentiment for a single article."""
    title = article["title"]
    content = article["content"] or article["summary"]
    prompt = sentiment_prompt(title, content, role, asset_desc)
    result = await generate_json(prompt, system=SENTIMENT_SYSTEM, max_tokens=60)
    if not result:
        result = await generate_json(prompt, system=SENTIMENT_SYSTEM, max_tokens=60)
    return result


async def _run_batch_macro_sentiment(articles: list[dict], sentiment_stored: dict[str, bool]) -> None:
    """Run macro sentiment in batch; fall back to deterministic + then individual LLM."""
    # First pass: deterministic
    needs_llm = []
    for art in articles:
        det = _deterministic_macro_sentiment(art["title"], art["content"] or art["summary"])
        if det:
            s_label, s_conf = det
            logger.info("Macro (det) '%s': %s", art["title"][:40], s_label)
            store_in_scores = not sentiment_stored.get(art["id"], False)
            if store_in_scores:
                await store_sentiment(art["id"], s_label, s_conf)
                sentiment_stored[art["id"]] = True
            await store_macro_label(art["id"], s_label)
        else:
            needs_llm.append(art)

    if not needs_llm:
        return

    # Second pass: batch LLM
    prompt = batch_macro_sentiment_prompt(needs_llm)
    max_tok = MACRO_TOKENS_PER_ARTICLE * len(needs_llm) + TOKENS_OVERHEAD
    raw_results = await generate_json_array(
        prompt, system=MACRO_SYSTEM, max_tokens=max_tok
    )

    _MACRO_LABEL_MAP = {"good": "positive", "bad": "negative", "mixed": "neutral", "GOOD": "positive", "BAD": "negative", "MIXED": "neutral"}
    valid_labels = {"very_positive", "positive", "neutral", "negative", "very_negative"}

    if raw_results is not None:
        result_map = {item["id"]: item for item in raw_results if isinstance(item, dict) and "id" in item}
        for art in needs_llm:
            item = result_map.get(art["id"])
            if item:
                raw_label = item.get("sentiment", "neutral").strip().lower().strip("<>")
                confidence = float(item.get("confidence", 0.5))
                s_label = _MACRO_LABEL_MAP.get(raw_label, raw_label)
                if s_label not in valid_labels:
                    s_label = "neutral"
                logger.info("Macro (batch) '%s': %s (%.2f)", art["title"][:40], s_label, confidence)
                store_in_scores = not sentiment_stored.get(art["id"], False)
                if store_in_scores:
                    await store_sentiment(art["id"], s_label, confidence)
                    sentiment_stored[art["id"]] = True
                await store_macro_label(art["id"], s_label)
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


def _deterministic_instrument_sentiment(title: str, content: str, category: str) -> tuple[str, float] | None:
    """Rule-based instrument sentiment for clear-cut analyst/price signals."""
    combined = f"{title}. {content[:300]}".lower()

    if category == "commodity":
        _COMMODITY_TANGENTIAL = [
            r'\bgold\s+stocks?\b',
            r'\b(?:oil|energy)\s+stocks?\b',
            r'\b(?:gold|silver|oil)\s+(?:miner|mining|producer|company|companies|etf)\b',
        ]
        for pattern in _COMMODITY_TANGENTIAL:
            if re.search(pattern, combined):
                return ("neutral", 0.75)

    _VERY_POSITIVE = [
        r'\b(?:beat|beats|exceeded|surpass)\b.{0,40}\b(?:earn|revenue|estimate|expect)',
        r'\b(?:earn|revenue|profit)\b.{0,40}\b(?:beat|beats|exceeded|surpass)',
        r'\b(?:rais|increas|hik|boost|lift|up)\b.{0,30}\b(?:price\s+target|pt\b|target\s+price)',
        r'\bprice\s+target\b.{0,30}\b(?:rais|increas|hik|boost|lift|up)',
        r'\b(?:upgrad)\b.{0,30}\b(?:buy|outperform|overweight|strong\s+buy)',
        r'\b(?:buy|outperform|overweight|strong\s+buy)\b.{0,30}\b(?:upgrad)',
        r'\b(?:win|land|award|secur|clinch|bag)\w*\b.{0,25}\b(?:contract|deal|order)\b',
        r'\b(?:contract|deal|order)\b.{0,25}\b(?:win|land|award|secur|clinch)\w*\b',
        r'\b(?:new\s+contract|major\s+contract|billion.{0,10}contract|contract\s+award)',
        r'\b(?:F-35|F35|fighter\s+jet|defense\s+contract|military\s+contract)\b',
        r'\b(?:expand|quadrupl|doubl|tripl|scal)\w*\b.{0,40}\b(?:defense|defence|military|weapons?|arms?)\b',
        r'\bweapons?\s+(?:makers?|manufacturer)\b.{0,60}\b(?:expand|agree|produc|increas|ramp)\b',
        r'\b(?:defense|defence|military)\s+(?:production|manufactur|spending|budget)\b.{0,40}\b(?:expand|increas|surge|boost|ramp|quadrupl)\b',
        r'\brecord\b.{0,30}\b(?:revenue|profit|earn|backlog|sales)',
        r'\b(?:revenue|profit|earn|backlog|sales)\b.{0,30}\brecord\b',
        r'\bstrong\s+(?:earn|revenue|result|guidance|demand)\b',
        r'\bmark\s+your\s+calendar',
        r'\$\d+(?:\.\d+)?\s*(?:billion|B)\b.{0,30}\b(?:order|deal|contract|program)',
    ]

    _POSITIVE = [
        r'\b(?:likes?|bullish|favor|recommends?)\b.{0,40}\b(?:stock|share|position)',
        r'\b(?:stock|share)\b.{0,40}\b(?:likes?|bullish|favor|recommends?)',
        r'\b(?:rais)\b.{0,20}\b(?:price\s+target|pt\b)',
        r'\b(?:buy\s+rating|maintains?\s+buy|reiterates?\s+buy|maintains?\s+overweight)',
        r'\biniti\w+\b.{0,20}\b(?:buy|outperform|overweight)',
        r'\b(?:positive|optimistic)\b.{0,30}\b(?:outlook|guidance|view)',
        r'\bpositive\s+(?:earn|revenue|result|catalyst)',
        r'\b(?:dividend\s+(?:increas|grow|hike)|special\s+dividend)\b',
        r'\bshare\s+(?:buyback|repurchase)\b',
        r'\b(?:trump|president|pentagon|government)\b.{0,60}\b(?:defense|weapons?|military)\b.{0,40}\b(?:expand|produc|spend|fund|increas)\b',
        r'\b(?:best|top|number.one|#1|must.own|must.buy)\b.{0,30}\b(?:stock|pick|investment|buy)\b',
        r'\b(?:still\s+cheap|undervalued|bargain|trading\s+at\s+a\s+discount|too\s+cheap)\b',
        r'\b(?:stock|share|price)\b.{0,30}\b(?:surge|soar|rocket|skyrocket|moon)\b',
        r'\b(?:surge|soar|rocket|skyrocket)\b.{0,30}\b(?:stock|share|price)\b',
        r'\breiterates?\b.{0,30}\b(?:stock|rating)\b',
        r'\breiterates?\b.{0,20}\b(?:outperform|buy|overweight)\b',
        r'\b(?:stock|shares?)\b.{0,30}\b(?:making\s+gains|up\s+\d|rall(?:y|ies|ied)|ris(?:es?|ing)|climb)',
        r'\b(?:defense|defence)\s+(?:stock|sector)\w*\b.{0,30}\b(?:gain|ris|up|rall|climb|surge)',
        r'\bcurrently\s+up\b',
        r'\b(?:war|strike|conflict|military)\b.{0,60}\b(?:defense|defence|rtx|raytheon|lockheed|northrop)\b',
        r'\b(?:defense|defence|rtx|raytheon|lockheed|northrop)\b.{0,60}\b(?:war|strike|conflict|military)\b',
        r'\b(?:pentagon|defense\s+(?:chief|leader|ceo|secretary))\b.{0,40}\b(?:meet|discuss|plan|agree)',
    ]

    _VERY_NEGATIVE = [
        r'\b(?:miss|misses|missed|fell\s+short)\b.{0,40}\b(?:earn|revenue|estimate|expect)',
        r'\b(?:earn|revenue|profit)\b.{0,40}\b(?:miss|misses|missed|fell\s+short)',
        r'\b(?:cut|lower|reduc|slash)\b.{0,30}\b(?:price\s+target|pt\b|target\s+price)',
        r'\bprice\s+target\b.{0,30}\b(?:cut|lower|reduc|slash)',
        r'\b(?:downgrad)\b.{0,30}\b(?:sell|underperform|underweight)',
        r'\b(?:sell|underperform|underweight)\b.{0,30}\b(?:downgrad)',
        r'\b(?:layoff|restructur|writedown|write-off|impairment)\b',
        r'\bguidance\b.{0,30}\b(?:cut|lower|below|miss|weak)',
    ]

    _NEGATIVE = [
        r'\b(?:bearish|negative\s+outlook|headwind)\b',
        r'\b(?:downgrad)\b.{0,30}\b(?:hold|neutral)',
        r'\b(?:sell\s+rating|maintains?\s+sell)',
        r'\brisky\b.{0,20}\b(?:stock|invest|bet)',
        r'\b(?:avoid|stay\s+away|cautious)\b.{0,30}\b(?:stock|share)',
        r'\b(?:forget|avoid|abandon|dump|ditch)\b.{0,40}\b(?:gold|oil|silver|commodit)',
        r'\b(?:gold|silver|oil)\b.{0,50}\b(?:not\s+worth|overvalued|bubble)',
        r'\b(?:crypto|bitcoin|btc|digital\s+asset)\b.{0,70}\b(?:better|smarter|outperform|superior|replace)\b.{0,40}\b(?:gold|silver|hard\s+asset)',
        r'\b(?:smarter|better)\b.{0,30}\b(?:bet|alternative|choice)\b.{0,30}\b(?:than\s+gold|than\s+oil|than\s+silver)',
    ]

    for pattern in _VERY_POSITIVE:
        if re.search(pattern, combined):
            return ("very positive", 0.90)
    for pattern in _POSITIVE:
        if re.search(pattern, combined):
            return ("positive", 0.80)
    for pattern in _VERY_NEGATIVE:
        if re.search(pattern, combined):
            return ("very negative", 0.90)
    for pattern in _NEGATIVE:
        if re.search(pattern, combined):
            return ("negative", 0.80)

    return None


def _deterministic_macro_sentiment(title: str, content: str) -> tuple[str, float] | None:
    """Rule-based macro sentiment for clear-cut cases."""
    combined = f"{title}. {content[:500]}".lower()

    _NEUTRAL_PATTERNS = [
        r'\b(?:forced\s+steriliz|sterilisation|reparation[s]?\s+for|human\s+rights\s+court)',
        r'\b(?:shootout|drug\s+bust|gang\s+warfare|human\s+trafficking|domestic\s+violence)\b',
        r'\b(?:mayor|city\s+council|local\s+election|municipal)\b',
        r'\b(?:drone|uav|unmanned)\b.{0,50}\b(?:fire|ignit|damage)\b.{0,50}\b(?:facilit|plant|pipeline|field)\b',
        r'\b(?:ignit|set\s+fire|on\s+fire|fire\s+at)\b.{0,40}\b(?:oil|gas|fuel)\s+(?:facilit|plant|pipeline|field)\b',
        r'\b(?:Cuba|Peru|Bolivia|Paraguay|Honduras|Nicaragua|Myanmar|Cambodia)\b.{0,80}\b(?:court|ruling|shootout|cartel|protest)\b',
    ]

    for pattern in _NEUTRAL_PATTERNS:
        if re.search(pattern, combined):
            return ("neutral", 0.85)

    _NEGATIVE_PATTERNS = [
        r'\b(?:war|attack|strikes?|bomb(?:ing|ed)?|invasi(?:on|ng)|casualties|killed|missile)\b',
        r'\b(?:iran|gulf|middle\s*east|israel|gaza|ukraine|russia)\b.*\b(?:war|attack|strike|conflict|bomb|military)\b',
        r'\b(?:war|attack|strike|conflict|bomb|military)\b.*\b(?:iran|gulf|middle\s*east|israel|gaza|ukraine|russia)\b',
        r'\b(?:escalat|tension|threat)\b.*\b(?:military|nuclear|war)\b',
        r'\brecession\b',
        r'\bunemployment\s+(?:ris|jump|surg|spike)',
        r'\bweak\s+(?:gdp|economy|growth|jobs)',
        r'\b(?:rate\s+hike|hawkish|inflation\s+(?:ris|surg|spike|high))',
        r'\b(?:gas|oil|energy)\s+price[s]?\s+(?:ris|surg|jump|spike|higher|soar)',
        r'\b(?:tariff|trade\s+war|embargo)\b',
    ]

    for pattern in _NEGATIVE_PATTERNS:
        if re.search(pattern, combined):
            return ("negative", 0.85)

    _POSITIVE_PATTERNS = [
        r'\b(?:rate\s+cut|dovish|stimulus|easing)\b',
        r'\b(?:strong\s+(?:gdp|jobs|growth|economy))\b',
        r'\b(?:peace\s+deal|ceasefire|de-escalat)',
        r'\b(?:trade\s+deal|tariff\s+remov)',
        r'\b(?:rally|boom|surge)\b.*\b(?:market|s&p|nasdaq|dow)\b',
        r'\b(?:market|s&p|nasdaq|dow)\b.*\b(?:rally|boom|surge)\b',
    ]

    for pattern in _POSITIVE_PATTERNS:
        if re.search(pattern, combined):
            return ("positive", 0.85)

    return None


async def run_macro_sentiment(article_id: str, title: str, content: str, store_in_scores: bool = True) -> None:
    """Run macro-level sentiment analysis with Western market bias."""
    deterministic = _deterministic_macro_sentiment(title, content)
    if deterministic:
        sentiment_label, confidence = deterministic
        logger.info("Macro sentiment (deterministic) for '%s': %s (conf=%.2f)", title[:40], sentiment_label, confidence)
    else:
        prompt = macro_sentiment_prompt(title, content)
        sentiment_result = await generate_json(prompt, system=MACRO_SYSTEM, max_tokens=60)

        if sentiment_result:
            raw_label = sentiment_result.get("sentiment", "neutral").strip().lower().strip("<>")
            confidence = float(sentiment_result.get("confidence", 0.5))
            _MACRO_LABEL_MAP = {
                "good": "positive", "bad": "negative", "mixed": "neutral",
            }
            sentiment_label = _MACRO_LABEL_MAP.get(raw_label, raw_label)
            valid_labels = {"very_positive", "positive", "neutral", "negative", "very_negative"}
            if sentiment_label not in valid_labels:
                sentiment_label = "neutral"
            logger.info("Macro sentiment (LLM) for '%s': %s (conf=%.2f)", title[:40], sentiment_label, confidence)
        else:
            sentiment_label = "neutral"
            confidence = 0.3

    if store_in_scores:
        await store_sentiment(article_id, sentiment_label, confidence)
    await store_macro_label(article_id, sentiment_label)


async def update_article_tags(
    article_id: str,
    is_macro: bool,
    is_asset_specific: bool,
    instruments: list[str],
    instrument_ids: dict[str, str],
) -> None:
    """Update article flags and create instrument mappings."""
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
                await session.execute(
                    text("""
                        INSERT INTO news_instrument_map (article_id, instrument_id, relevance_score)
                        VALUES (:aid, :iid, 1.0)
                    """),
                    {"aid": article_id, "iid": iid},
                )

        await session.commit()


async def store_sentiment(article_id: str, sentiment_label: str, confidence: float) -> None:
    """Store sentiment score in the database."""
    label = sentiment_label.lower().replace("_", " ").strip().strip("<>")
    if label not in SENTIMENT_LABEL_MAP.values():
        label = SENTIMENT_LABEL_MAP.get(sentiment_label.lower().replace(" ", "_"), "neutral")

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
                    INSERT INTO sentiment_scores (article_id, positive, negative, neutral, label)
                    VALUES (:aid, :pos, :neg, :neu, :label)
                    ON CONFLICT (article_id) DO UPDATE
                    SET positive = :pos, negative = :neg, neutral = :neu, label = :label
                """),
                {"aid": article_id, "pos": pos, "neg": neg, "neu": neu, "label": label},
            )
            await session.commit()
        except Exception as e:
            logger.warning("Could not store sentiment for article %s: %s", article_id, e)


async def store_macro_label(article_id: str, sentiment_label: str) -> None:
    """Store macro sentiment label directly on the article for macro aggregation."""
    label = sentiment_label.lower().replace("_", " ").strip().strip("<>")
    if label not in SENTIMENT_LABEL_MAP.values():
        label = SENTIMENT_LABEL_MAP.get(sentiment_label.lower().replace(" ", "_"), "neutral")
    async with async_session() as session:
        try:
            await session.execute(
                text("UPDATE news_articles SET macro_sentiment_label = :label WHERE id = :id"),
                {"id": article_id, "label": label},
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
    """Calculate aggregate global macro sentiment from macro_sentiment_label on articles."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=72)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT COALESCE(macro_sentiment_label, 'neutral') AS macro_sentiment_label
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

        scores = [SENTIMENT_MULTIPLIERS.get(r.macro_sentiment_label, 0.0) for r in rows
                  if r.macro_sentiment_label != "neutral"]
        if not scores:
            return
        net_score = sum(scores) / len(scores)
        net_score = max(-1.0, min(1.0, net_score))

        if net_score > 0.25:
            label = "positive"
        elif net_score < -0.25:
            label = "negative"
        else:
            label = "neutral"

        await session.execute(
            text("""
                INSERT INTO macro_sentiment (region, score, label, article_count, calculated_at)
                VALUES ('global', :score, :label, :count, NOW())
            """),
            {"score": net_score, "label": label, "count": total_cnt},
        )

        await session.execute(
            text("""
                DELETE FROM macro_sentiment
                WHERE id NOT IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (ORDER BY calculated_at DESC) as rn
                        FROM macro_sentiment
                    ) ranked WHERE rn <= 100
                )
            """)
        )

        await session.commit()
    logger.info("Global macro sentiment updated (%d articles)", total_cnt)


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
