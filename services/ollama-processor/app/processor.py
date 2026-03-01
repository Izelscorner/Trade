"""Main processing pipeline: classifies and scores unprocessed news articles via Ollama."""

import re
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session
from .ollama_client import generate_json
from .prompts import (
    classify_prompt,
    sentiment_prompt,
    macro_sentiment_prompt,
    build_instrument_context,
    get_role,
    get_asset_description,
)

logger = logging.getLogger(__name__)

PROCESS_INTERVAL = 15  # seconds between processing cycles
BATCH_SIZE = 20

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
# These override Llama 3.2 1B's unreliable macro classification.

# Categories from the news-fetcher that are inherently macro
MACRO_SOURCE_CATEGORIES = {"macro_markets", "macro_politics", "macro_conflict"}

# Patterns in titles that indicate asset-specific (NOT macro) content
_ASSET_SPECIFIC_PATTERNS = re.compile(
    r"""(?ix)                # case-insensitive, verbose
    (?:
        \$[A-Z]{1,5}\b               # ticker symbol like $AAPL, $GOOGL
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

# Commodity-specific price keywords that distinguish price news from company news
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
    """Build a symbol -> [search names] map dynamically from instrument data.

    For stocks/ETFs: uses the company name words as search terms.
    For commodities: uses commodity-specific price keywords to avoid
    matching company names (e.g. "gold mining company" != gold futures).
    """
    lookup = {}
    for inst in instruments:
        symbol = inst["symbol"]
        name = inst["name"].lower()
        category = inst["category"]

        if category == "commodity":
            # For commodities, only match price-specific phrases
            # Extract base commodity name from the instrument name
            lookup[symbol] = []  # no generic name match
        else:
            # For stocks/ETFs, extract meaningful name parts
            # Remove common suffixes like "Inc.", "Corporation", "ETF"
            clean = re.sub(r'\b(inc\.?|corp\.?|corporation|ltd\.?|llc|plc|etf|futures|co\.?)\b', '', name, flags=re.I)
            clean = clean.strip().strip(',').strip()
            names = [n.strip() for n in clean.split() if len(n.strip()) > 2]
            # Also add the full clean name and symbol
            search_names = []
            if clean and len(clean) > 2:
                search_names.append(clean)
            search_names.extend(names)
            # Deduplicate
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
        # Check if symbol itself is mentioned
        if symbol.lower() in combined:
            mentioned.add(symbol)
            continue
        # Check name variants
        if any(n in combined for n in names):
            mentioned.add(symbol)

    # Check commodities separately using price-specific keywords
    for inst in instruments:
        if inst["category"] != "commodity":
            continue
        symbol = inst["symbol"]
        name_lower = inst["name"].lower()
        # Try to find matching commodity keywords
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
    """Apply deterministic rules to correct LLM classification errors.

    Returns corrected (instruments, is_macro).
    """
    combined = f"{title} {content[:300]}"

    # Rule 0: Articles from macro feed categories are ALWAYS macro
    _MACRO_CATEGORIES = {"macro_markets", "macro_politics", "macro_conflict"}
    if source_category in _MACRO_CATEGORIES:
        llm_is_macro = True

    # Rule 1: Asset-specific feed articles are NOT macro unless they match macro patterns
    if source_category == "asset_specific":
        if llm_is_macro and not _MACRO_PATTERNS.search(combined):
            logger.debug("Post-proc: overriding is_macro=False for asset feed: '%s'", title[:60])
            llm_is_macro = False

    # Rule 2: Titles matching asset-specific patterns are NOT macro (only for asset feeds)
    if source_category == "asset_specific" and llm_is_macro and _ASSET_SPECIFIC_PATTERNS.search(title):
        if not _MACRO_PATTERNS.search(combined):
            logger.debug("Post-proc: overriding is_macro=False due to asset pattern: '%s'", title[:60])
            llm_is_macro = False

    # Rule 3: Any article matching strong macro patterns should be macro (even from asset feeds)
    if not llm_is_macro and _MACRO_PATTERNS.search(combined):
        logger.debug("Post-proc: overriding is_macro=True due to macro pattern: '%s'", title[:60])
        llm_is_macro = True

    # Rule 4: Don't tag instruments for articles clearly about other companies
    # If the title has a ticker like $NEM, $DELL that's not in our tracked set,
    # and no tracked instrument is actually mentioned, clear the instruments list
    foreign_tickers = re.findall(r'\$([A-Z]{1,5})\b', title)
    if foreign_tickers:
        foreign_tickers_set = set(foreign_tickers)
        tracked_in_title = foreign_tickers_set & valid_symbols
        if not tracked_in_title and foreign_tickers_set:
            # Title mentions tickers we don't track, LLM probably mis-tagged
            direct = _check_direct_mention(title, content, name_lookup, instruments)
            llm_instruments = [s for s in llm_instruments if s in direct]

    # Rule 5: Validate ALL instrument tags — only keep instruments actually
    # mentioned by name/symbol in the article (LLM often hallucinates tags)
    if llm_instruments:
        direct = _check_direct_mention(title, content, name_lookup, instruments)
        validated = [s for s in llm_instruments if s in direct]
        if validated != llm_instruments:
            logger.debug("Post-proc: filtered instruments %s -> %s for: '%s'", llm_instruments, validated, title[:60])
            llm_instruments = validated

    # Rule 6: If LLM returned empty instruments but a tracked company is directly
    # named in the title, add it (LLM 1B often misses obvious mentions)
    if not llm_instruments:
        direct = _check_direct_mention(title, "", name_lookup, instruments)
        if direct:
            llm_instruments = list(direct)
            logger.debug("Post-proc: added instruments %s from title: '%s'", direct, title[:60])

    return llm_instruments, llm_is_macro


async def get_unprocessed_articles(limit: int = BATCH_SIZE) -> list[dict]:
    """Get articles that haven't been processed by Ollama yet.

    Priority ordering:
      0 — User-prioritized instruments (clicked in frontend)
      1 — Macro articles (until 10+ macro articles are processed, then same as asset)
      2 — Asset-specific articles
    """
    async with async_session() as session:
        # Check how many macro articles are already processed (for interleaving)
        macro_count_res = await session.execute(
            text("SELECT count(*) FROM news_articles WHERE is_macro = true AND ollama_processed = true")
        )
        macro_processed = macro_count_res.scalar() or 0

        # Once we have 10+ processed macro articles, stop prioritizing macro
        # over asset-specific — interleave by fetched_at instead
        macro_priority = 1 if macro_processed >= 10 else 0

        result = await session.execute(
            text("""
                SELECT a.id, a.title, a.summary, a.content, a.category
                FROM news_articles a
                LEFT JOIN news_instrument_map nim ON nim.article_id = a.id
                LEFT JOIN processing_priority pp ON pp.instrument_id = nim.instrument_id
                WHERE a.ollama_processed = false
                ORDER BY
                    CASE WHEN pp.instrument_id IS NOT NULL THEN 0
                         WHEN a.category LIKE 'macro_%%' THEN :macro_pri
                         ELSE 2
                    END,
                    a.fetched_at ASC
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
    """Process a single article through the Ollama pipeline.

    1. Classify + tag instruments (single LLM call)
    2. Apply deterministic post-processing rules
    3. If relevant: run contextual sentiment analysis
    4. Mark as processed
    """
    article_id = article["id"]
    title = article["title"]
    content = article["content"] or article["summary"]
    source_category = article["category"]

    # Step 1: Classify + tag via LLM
    prompt = classify_prompt(title, content, symbol_mapping, valid_symbols_str)
    classification = await generate_json(prompt, max_tokens=200)

    if not classification:
        logger.warning("Failed to classify article %s: '%s', skipping for retry", article_id, title[:60])
        return

    article_type = classification.get("type", "spam")
    raw_instruments = classification.get("instruments", [])
    is_macro = classification.get("is_macro", False)

    # Extract symbols from LLM output (handles strings, dicts, and full descriptions)
    tagged_instruments = []
    for inst in raw_instruments:
        if isinstance(inst, dict):
            # LLM returned {"AAPL": "Apple Inc.", ...} — extract keys
            for key in inst.keys():
                sym = key.strip().upper()
                if sym in valid_symbols:
                    tagged_instruments.append(sym)
        elif isinstance(inst, str):
            # Take first word/token which should be the symbol
            symbol = inst.split(" ")[0].split("-")[0].strip().upper()
            if symbol in valid_symbols:
                tagged_instruments.append(symbol)
    tagged_instruments = list(dict.fromkeys(tagged_instruments))  # deduplicate preserving order

    # Step 1b: Handle spam
    if article_type == "spam":
        logger.info("Filtered spam article: '%s'", title[:60])
        await delete_article(article_id)
        return

    # Step 2: Apply deterministic post-processing
    tagged_instruments, is_macro = postprocess_classification(
        title, content, source_category, tagged_instruments, is_macro,
        valid_symbols, name_lookup, instruments,
    )

    # Step 2b: If neither macro nor instrument-related, filter out
    if not is_macro and not tagged_instruments:
        logger.info("Filtered irrelevant article: '%s'", title[:60])
        await delete_article(article_id)
        return

    # Step 3: Update article flags and instrument mappings
    await update_article_tags(article_id, is_macro, bool(tagged_instruments), tagged_instruments, instrument_ids)

    # Step 4: Contextual sentiment analysis
    # For dual articles (macro + instruments), run BOTH sentiments:
    #   - Instrument sentiment → stored in sentiment_scores (for per-instrument grades)
    #   - Macro sentiment → stored as macro_sentiment_label on the article (for macro aggregate)
    sentiment_stored = False
    if tagged_instruments:
        for symbol in tagged_instruments:
            inst = instruments_by_symbol.get(symbol, {})
            role = get_role(inst) if inst else f"You predict {symbol} price direction."
            asset_desc = get_asset_description(inst) if inst else f"{symbol} price"
            prompt = sentiment_prompt(title, content, role, asset_desc)
            sentiment_result = await generate_json(prompt, max_tokens=60)

            if sentiment_result:
                sentiment_label = sentiment_result.get("sentiment", "neutral")
                confidence = float(sentiment_result.get("confidence", 0.5))
                await store_sentiment(article_id, sentiment_label, confidence)
                logger.info("Sentiment for %s on '%s': %s (conf=%.2f)",
                           symbol, title[:40], sentiment_label, confidence)
                sentiment_stored = True
                break  # One sentiment score per article

    if is_macro:
        # Always run macro sentiment for macro articles — stored on the article
        # itself (separate from sentiment_scores) so it feeds the macro aggregate
        await run_macro_sentiment(article_id, title, content, store_in_scores=not sentiment_stored)

    if not sentiment_stored and not is_macro:
        # Fallback: store neutral sentiment so article shows in API
        await store_sentiment(article_id, "neutral", 0.3)
        logger.warning("Fallback neutral sentiment for: '%s'", title[:60])

    # Step 5: Mark as processed and clear content
    await mark_processed(article_id)
    logger.info("Processed article: '%s' (instruments=%s, macro=%s)", title[:60], tagged_instruments, is_macro)


def _deterministic_macro_sentiment(title: str, content: str) -> tuple[str, float] | None:
    """Rule-based macro sentiment for clear-cut cases.

    Returns (label, confidence) or None if LLM should decide.
    The 1B model cannot reliably follow multi-rule prompts, so we handle
    the most important patterns deterministically.
    """
    combined = f"{title}. {content[:500]}".lower()

    # --- NEGATIVE patterns (BAD for S&P 500) ---
    _NEGATIVE_PATTERNS = [
        # War and conflict
        r'\b(?:war|attack|strikes?|bomb(?:ing|ed)?|invasi(?:on|ng)|casualties|killed|missile)\b',
        r'\b(?:iran|gulf|middle\s*east|israel|gaza|ukraine|russia)\b.*\b(?:war|attack|strike|conflict|bomb|military)\b',
        r'\b(?:war|attack|strike|conflict|bomb|military)\b.*\b(?:iran|gulf|middle\s*east|israel|gaza|ukraine|russia)\b',
        r'\b(?:escalat|tension|threat)\b.*\b(?:military|nuclear|war)\b',
        # Economic weakness
        r'\brecession\b',
        r'\bunemployment\s+(?:ris|jump|surg|spike)',
        r'\bweak\s+(?:gdp|economy|growth|jobs)',
        # Inflation / rate hikes
        r'\b(?:rate\s+hike|hawkish|inflation\s+(?:ris|surg|spike|high))',
        r'\b(?:gas|oil|energy)\s+price[s]?\s+(?:ris|surg|jump|spike|higher|soar)',
        # Trade war
        r'\b(?:tariff|trade\s+war|embargo)\b',
    ]

    for pattern in _NEGATIVE_PATTERNS:
        if re.search(pattern, combined):
            logger.debug("Deterministic macro: NEGATIVE (matched '%s') for: '%s'", pattern[:40], title[:60])
            return ("negative", 0.85)

    # --- POSITIVE patterns (GOOD for S&P 500) ---
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
            logger.debug("Deterministic macro: POSITIVE (matched '%s') for: '%s'", pattern[:40], title[:60])
            return ("positive", 0.85)

    # No clear pattern — let LLM decide
    return None


async def run_macro_sentiment(article_id: str, title: str, content: str, store_in_scores: bool = True) -> None:
    """Run macro-level sentiment analysis with Western market bias.

    Uses deterministic rules for clear-cut cases (war, rate changes, etc.)
    and falls back to LLM for ambiguous articles. The 1B model is unreliable
    at multi-rule classification, so deterministic rules handle the important cases.

    Always stores the macro label on the article itself (for macro aggregate).
    Only stores in sentiment_scores if store_in_scores=True (i.e. article has
    no instrument-specific sentiment already stored).
    """
    # Try deterministic rules first
    deterministic = _deterministic_macro_sentiment(title, content)
    if deterministic:
        sentiment_label, confidence = deterministic
        logger.info("Macro sentiment (deterministic) for '%s': %s (conf=%.2f)", title[:40], sentiment_label, confidence)
    else:
        # Fall back to LLM for ambiguous articles
        prompt = macro_sentiment_prompt(title, content)
        sentiment_result = await generate_json(prompt, max_tokens=60)

        if sentiment_result:
            raw_label = sentiment_result.get("sentiment", "neutral").strip().lower().strip("<>")
            confidence = float(sentiment_result.get("confidence", 0.5))
            logger.info("Macro raw LLM response: %s (raw_label='%s')", sentiment_result, raw_label)
            # Map simplified GOOD/BAD/MIXED to standard labels
            _MACRO_LABEL_MAP = {
                "good": "positive", "bad": "negative", "mixed": "neutral",
            }
            sentiment_label = _MACRO_LABEL_MAP.get(raw_label, raw_label)
            # Normalize any non-standard labels
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

        # Delete old instrument mappings before inserting fresh ones
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
    """Calculate aggregate global macro sentiment from macro_sentiment_label on articles.

    Uses the dedicated macro_sentiment_label column (set via macro_sentiment_prompt)
    rather than sentiment_scores, so that dual articles (macro + instrument) contribute
    their Western-market-biased macro sentiment, not their instrument-specific sentiment.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=72)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT macro_sentiment_label
                FROM news_articles
                WHERE is_macro = true
                AND ollama_processed = true
                AND macro_sentiment_label IS NOT NULL
                AND published_at >= :cutoff
                ORDER BY published_at DESC
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()

        if not rows:
            return

        total_cnt = len(rows)

        # Use median for robustness against outliers
        scores = sorted(SENTIMENT_MULTIPLIERS.get(r.macro_sentiment_label, 0.0) for r in rows)
        if total_cnt % 2 == 1:
            net_score = scores[total_cnt // 2]
        else:
            net_score = (scores[total_cnt // 2 - 1] + scores[total_cnt // 2]) / 2
        net_score = max(-1.0, min(1.0, net_score))

        # Determine overall label
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
