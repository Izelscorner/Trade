"""Prompt templates for Llama 3.2 1B — Western quant analysis perspective.

Designed by a finance specialist for Western-biased sentiment analysis.
All prompts are heavily guardrailed with explicit if-then rules because
the 1B model cannot reason — it can only pattern-match.

Key principles:
- We trade US-listed assets: US stocks, US ETFs, gold futures, oil futures
- Sentiment = expected PRICE DIRECTION for each specific asset
- Macro sentiment = impact on S&P 500 / Nasdaq composite
- Western geopolitical bias: what benefits Western power projection is GOOD
- War in Middle East/Gulf ≠ bad for Western markets (it's mixed or good)
- War that directly hits Western homeland/economy = bad
"""


MACRO_ROLE = "You predict the direction of Western financial markets (S&P 500, Nasdaq, Dow Jones)."

# Category-based role templates for sentiment analysis
CATEGORY_ROLES = {
    "stock": "You are a Wall Street equity analyst covering {name}.",
    "etf": "You are a Wall Street ETF analyst covering {name}.",
    "commodity": "You are a commodity futures trader at a Western bank trading {name}.",
}

# Category-based asset description templates
CATEGORY_DESCRIPTIONS = {
    "stock": "{name} stock price on US exchanges",
    "etf": "{name} ETF price on US exchanges",
    "commodity": "{name} futures price",
}


def _clean_name(name: str, category: str) -> str:
    """Clean instrument name for prompt use."""
    if category == "commodity":
        return name.replace("Futures", "").strip()
    return name


def build_instrument_context(instruments: list[dict]) -> tuple[str, str]:
    """Build dynamic instrument context strings from DB instruments."""
    mapping_parts = []
    symbols = []
    for inst in instruments:
        symbol = inst["symbol"]
        name = inst["name"]
        category = inst["category"]
        symbols.append(symbol)
        mapping_parts.append(f"{symbol}={name} ({category})")

    symbol_mapping = ", ".join(mapping_parts)
    valid_symbols = ", ".join(symbols)
    return symbol_mapping, valid_symbols


def get_role(instrument: dict) -> str:
    """Get sentiment analysis role for an instrument based on its category."""
    category = instrument.get("category", "stock")
    template = CATEGORY_ROLES.get(category, CATEGORY_ROLES["stock"])
    name = _clean_name(instrument["name"], category)
    return template.format(name=name)


def get_asset_description(instrument: dict) -> str:
    """Get asset description for an instrument based on its category."""
    category = instrument.get("category", "stock")
    template = CATEGORY_DESCRIPTIONS.get(category, CATEGORY_DESCRIPTIONS["stock"])
    name = _clean_name(instrument["name"], category)
    return template.format(name=name)


def classify_prompt(title: str, content: str, symbol_mapping: str, valid_symbols: str) -> str:
    """Build the classification + instrument tagging prompt.

    The 1B model needs explicit rules and examples to classify correctly.
    """
    text = f"{title}. {content[:500]}" if content else title
    return f"""You are a financial news classifier. Analyze this article and classify it.

Our tracked instruments: {symbol_mapping}
Valid symbols: {valid_symbols}

Article: "{text}"

Rules:
1. "type": "news" if real financial/political/economic news. "spam" if ads, lifestyle, entertainment, clickbait, or non-news content.
2. "instruments": List ONLY symbols from the valid symbols list that are DIRECTLY mentioned by name or ticker in the article. Do NOT guess. If no tracked instrument is mentioned, use empty list [].
3. "is_macro": true ONLY if the article is about: wars, military conflicts, geopolitics, sanctions, trade policy, central bank decisions (Fed/ECB/BOJ), interest rates, GDP, inflation data, recession fears, tariffs, or global economic policy. false if the article is about: specific company earnings, stock picks, product launches, individual stock analysis, or technical trading.

Respond ONLY with JSON:
{{"type": "news", "instruments": [], "is_macro": false}}"""


def sentiment_prompt(title: str, content: str, role: str, asset_desc: str) -> str:
    """Build instrument-specific sentiment prompt with Western quant bias.

    Heavy guardrails with explicit if-then mapping for the 1B model.
    The model must understand that war/conflict is GOOD for defense/oil/gold.
    """
    text = f"{title}. {content[:500]}" if content else title
    return f"""{role}

You manage a Western investment portfolio. Predict the PRICE DIRECTION of {asset_desc} ONLY.
Do NOT think about the overall stock market. ONLY think about {asset_desc}.

Article: "{text}"

IF the article mentions war, military strikes, Iran, Gulf conflict, Middle East tension:
  → "positive" for defense stocks like RTX (military spending increases)
  → "positive" for oil futures (supply disruption fears push oil prices up)
  → "positive" for gold futures (safe haven demand increases)
  → "negative" for tech stocks like NVDA, GOOGL, AAPL (uncertainty hurts growth)
  → "negative" for tech ETFs like IITU (higher energy costs hurt tech)

IF the article mentions earnings beat, revenue growth, new contracts, upgrades:
  → "very_positive"

IF the article mentions earnings miss, revenue decline, downgrades, layoffs:
  → "very_negative"

IF the article mentions rate cuts, stimulus, strong jobs data:
  → "positive" for stocks and ETFs

IF the article mentions rate hikes, inflation rising, recession:
  → "negative" for stocks and ETFs

IF the article is NOT about {asset_desc} at all:
  → "neutral"

Choose: very_positive, positive, neutral, negative, very_negative

{{"sentiment": "positive", "confidence": 0.7}}"""


def macro_sentiment_prompt(title: str, content: str) -> str:
    """Build macro sentiment prompt — S&P 500 / Nasdaq direction.

    Western quant perspective with aggressive guardrails.
    The key insight: regional wars ≠ Western market crash.
    Only existential threats to Western economy are truly BAD.
    """
    text = f"{title}. {content[:500]}" if content else title
    return f"""You are a macro strategist at a Western investment bank.
Will the S&P 500 INDEX OVERALL go UP or DOWN because of this news?
Do NOT think about individual stocks or commodities. ONLY think about the S&P 500 as a whole.

"{text}"

RULES — follow these exactly:

IF Iran war, Gulf war, Middle East strikes, Israel conflict:
  → MIXED (regional war boosts defense and energy sectors, offsets broader market dip)

IF peace deal, ceasefire, de-escalation:
  → GOOD (reduces uncertainty, markets rally)

IF Fed rate cuts, ECB easing, stimulus:
  → GOOD (cheap money boosts stocks)

IF Fed rate hikes, inflation rising, hawkish central bank:
  → BAD (tighter money hurts stocks)

IF strong GDP, jobs data, consumer spending:
  → GOOD (economic growth lifts stocks)

IF recession warning, unemployment rising, weak GDP:
  → BAD (economic weakness crashes stocks)

IF trade deals, tariff removal:
  → GOOD (free trade boosts corporate profits)

IF new tariffs, trade war escalation:
  → BAD (trade barriers hurt corporate profits)

IF Russia/China direct military threat to NATO:
  → BAD (existential threat to Western order)

IF sanctions on Iran/Russia/enemies:
  → GOOD (Western power projection, no economic damage to West)

IF the news is about a non-Western country only:
  → MIXED (no direct impact on S&P 500)

Answer: GOOD, BAD, or MIXED

{{"sentiment": "MIXED", "confidence": 0.7}}"""
