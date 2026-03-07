"""Main processing pipeline: classifies and scores unprocessed news articles via Cerebras API."""

import asyncio
import re
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session
from .cerebras_client import generate_json
from .prompts import (
    CLASSIFY_SYSTEM,
    SENTIMENT_SYSTEM,
    MACRO_SYSTEM,
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
    """Process a single article through the Cerebras pipeline.

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
    classification = await generate_json(prompt, system=CLASSIFY_SYSTEM, max_tokens=300)

    if not classification:
        logger.warning("Failed to classify article %s: '%s', skipping for retry", article_id, title[:60])
        return

    article_type = classification.get("type", "spam")
    raw_instruments = classification.get("instruments", [])
    is_macro = classification.get("is_macro", False)

    # Extract symbols from LLM output
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

    # Handle spam
    if article_type == "spam":
        logger.info("Filtered spam article: '%s'", title[:60])
        await delete_article(article_id)
        return

    # Step 2: Apply deterministic post-processing
    tagged_instruments, is_macro = postprocess_classification(
        title, content, source_category, tagged_instruments, is_macro,
        valid_symbols, name_lookup, instruments,
    )

    # If neither macro nor instrument-related, filter out
    if not is_macro and not tagged_instruments:
        logger.info("Filtered irrelevant article: '%s'", title[:60])
        await delete_article(article_id)
        return

    # Step 3: Update article flags and instrument mappings
    await update_article_tags(article_id, is_macro, bool(tagged_instruments), tagged_instruments, instrument_ids)

    # Step 4: Contextual sentiment analysis
    sentiment_stored = False
    if tagged_instruments:
        first_symbol = tagged_instruments[0]
        first_inst = instruments_by_symbol.get(first_symbol, {})

        # Try deterministic rules first
        deterministic_sent = _deterministic_instrument_sentiment(
            title, content, first_inst.get("category", "stock")
        )
        if deterministic_sent:
            sentiment_label, confidence = deterministic_sent
            await store_sentiment(article_id, sentiment_label, confidence)
            logger.info("Sentiment (deterministic) for %s on '%s': %s (conf=%.2f)",
                       first_symbol, title[:40], sentiment_label, confidence)
            sentiment_stored = True
        else:
            # Use Cerebras for sentiment
            for symbol in tagged_instruments:
                inst = instruments_by_symbol.get(symbol, {})
                role = get_role(inst) if inst else f"You predict {symbol} price direction."
                asset_desc = get_asset_description(inst) if inst else f"{symbol} price"
                prompt = sentiment_prompt(title, content, role, asset_desc)

                # Try up to 2 times (rate limits can cause first attempt to fail)
                sentiment_result = await generate_json(prompt, system=SENTIMENT_SYSTEM, max_tokens=60)
                if not sentiment_result:
                    await asyncio.sleep(2)
                    sentiment_result = await generate_json(prompt, system=SENTIMENT_SYSTEM, max_tokens=60)

                if sentiment_result:
                    sentiment_label = sentiment_result.get("sentiment", "neutral")
                    confidence = float(sentiment_result.get("confidence", 0.5))
                    await store_sentiment(article_id, sentiment_label, confidence)
                    logger.info("Sentiment for %s on '%s': %s (conf=%.2f)",
                               symbol, title[:40], sentiment_label, confidence)
                    sentiment_stored = True
                    break

    if is_macro:
        await run_macro_sentiment(article_id, title, content, store_in_scores=not sentiment_stored)

    if not sentiment_stored and not is_macro:
        logger.warning("No sentiment stored for '%s' — leaving unprocessed for retry", title[:60])
        return

    # Step 5: Mark as processed and clear content
    await mark_processed(article_id)
    logger.info("Processed article: '%s' (instruments=%s, macro=%s)", title[:60], tagged_instruments, is_macro)


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
        # Large orders: "$X billion order", "$X.XB deal"
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
        # Bullish analyst conviction: "best stock to own/buy", "top pick", "must own"
        r'\b(?:best|top|number.one|#1|must.own|must.buy)\b.{0,30}\b(?:stock|pick|investment|buy)\b',
        # Undervaluation signals: "still cheap", "undervalued", "bargain", "discount"
        r'\b(?:still\s+cheap|undervalued|bargain|trading\s+at\s+a\s+discount|too\s+cheap)\b',
        # Surge/rally/soar language about a specific stock
        r'\b(?:stock|share|price)\b.{0,30}\b(?:surge|soar|rocket|skyrocket|moon)\b',
        r'\b(?:surge|soar|rocket|skyrocket)\b.{0,30}\b(?:stock|share|price)\b',
        # Analyst reiterates/maintains with positive context (order, growth, beat)
        r'\breiterates?\b.{0,30}\b(?:stock|rating)\b',
        r'\breiterates?\b.{0,20}\b(?:outperform|buy|overweight)\b',
        # Stocks "making gains", "currently up", "rallies", "rises"
        r'\b(?:stock|shares?)\b.{0,30}\b(?:making\s+gains|up\s+\d|rall(?:y|ies|ied)|ris(?:es?|ing)|climb)',
        r'\b(?:defense|defence)\s+(?:stock|sector)\w*\b.{0,30}\b(?:gain|ris|up|rall|climb|surge)',
        r'\bcurrently\s+up\b',
        # War/conflict context → positive for defense stocks specifically
        r'\b(?:war|strike|conflict|military)\b.{0,60}\b(?:defense|defence|rtx|raytheon|lockheed|northrop)\b',
        r'\b(?:defense|defence|rtx|raytheon|lockheed|northrop)\b.{0,60}\b(?:war|strike|conflict|military)\b',
        # Pentagon meetings, defense CEO meetings
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
