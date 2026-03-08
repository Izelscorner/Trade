"""Prompt templates for news classification and sentiment analysis.

Western quant analysis perspective — designed for capable models (32B+).
All prompts target structured JSON output.

Key principles:
- We trade US-listed assets: US stocks, US ETFs, gold futures, oil futures
- Sentiment = expected PRICE DIRECTION for each specific asset
- Macro sentiment = impact on S&P 500 / Nasdaq composite
- Western geopolitical bias: what benefits Western power projection is GOOD
- DUAL HORIZON: every sentiment call returns BOTH short-term (1-7 days)
  AND long-term (1-6 months) impact assessments
"""

CLASSIFY_SYSTEM = """You are a financial news classifier for a Western investment firm.
You classify articles and tag which tracked instruments they relate to.
You understand that news irrelevant to finance, markets, or macroeconomics should be filtered as spam.
Always respond with valid JSON."""

SENTIMENT_SYSTEM = """You are a Western quantitative analyst and behavioral finance expert.
You predict asset price direction from news on TWO time horizons:
- SHORT-TERM (1-7 days): immediate market reaction, momentum, sentiment-driven moves
- LONG-TERM (1-6 months): fundamental value impact, structural changes, competitive positioning
You understand that short-term and long-term impacts often diverge.
Always respond with valid JSON."""

MACRO_SYSTEM = """You are a macro strategist at a Western investment bank.
You predict S&P 500 direction on TWO time horizons:
- SHORT-TERM (1-7 days): immediate market reaction, risk-on/risk-off shifts
- LONG-TERM (1-6 months): structural economic impact, policy regime changes
Always respond with valid JSON."""


# Category-based role templates for sentiment analysis
CATEGORY_ROLES = {
    "stock": "You are a Wall Street equity analyst covering {name}. You think like both a day-trader (short-term price action) and a fundamental investor (long-term value).",
    "etf": "You are a Wall Street ETF analyst covering {name}. You understand constituent-level impacts propagate to the ETF with weight-proportional magnitude.",
    "commodity": "You are a commodity futures trader at a Western bank trading {name}. You distinguish between spot-price catalysts (short-term) and supply/demand structural shifts (long-term).",
}

# Category-based asset description templates
CATEGORY_DESCRIPTIONS = {
    "stock": "{name} stock price on US exchanges",
    "etf": "{name} ETF price on US exchanges",
    "commodity": "{name} futures price",
}


def _clean_name(name: str, category: str) -> str:
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
    return ", ".join(mapping_parts), ", ".join(symbols)


def get_role(instrument: dict) -> str:
    category = instrument.get("category", "stock")
    template = CATEGORY_ROLES.get(category, CATEGORY_ROLES["stock"])
    name = _clean_name(instrument["name"], category)
    return template.format(name=name)


def get_asset_description(instrument: dict) -> str:
    category = instrument.get("category", "stock")
    template = CATEGORY_DESCRIPTIONS.get(category, CATEGORY_DESCRIPTIONS["stock"])
    name = _clean_name(instrument["name"], category)
    return template.format(name=name)


def classify_prompt(title: str, content: str, symbol_mapping: str, valid_symbols: str) -> str:
    text = f"{title}. {content[:800]}" if content else title
    return f"""Classify this news article.

Tracked instruments: {symbol_mapping}
Valid symbols: {valid_symbols}

Article: "{text}"

Return JSON with:
- "type": "news" if real financial/political/economic/macroeconomic news. "spam" if ads, lifestyle, entertainment, clickbait, sports, celebrity gossip, or non-finance non-market non-economic news.
- "instruments": list of symbols from the valid symbols list that are DIRECTLY mentioned by name or ticker. Empty list [] if none. For ETFs, also tag the ETF if a major constituent is mentioned.
- "is_macro": true if about wars, military conflicts, geopolitics, sanctions, trade policy, central bank decisions, interest rates, GDP, inflation, recession, tariffs, or global economic policy. false if about specific company earnings, stock picks, product launches, or individual stock analysis.

{{"type": "news", "instruments": [], "is_macro": false}}"""


def batch_classify_prompt(articles: list[dict], symbol_mapping: str, valid_symbols: str) -> str:
    """Build a single prompt to classify multiple articles in one API call."""
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

    return f"""Classify these {len(articles)} news articles. Return ALL results as a JSON array.

Tracked instruments: {symbol_mapping}
Valid symbols: {valid_symbols}

Articles:{articles_text}
For EACH article return a JSON object with:
- "id": the article id string (copy exactly from input)
- "type": "news" if real financial/political/economic/macroeconomic news. "spam" if ads, lifestyle, entertainment, clickbait, sports, celebrity gossip, or non-finance content.
- "instruments": list of valid symbols DIRECTLY mentioned. [] if none. For ETFs, also tag the ETF symbol if a major constituent company is mentioned.
- "is_macro": true if about wars, geopolitics, sanctions, central banks, GDP, inflation, tariffs. false otherwise.

Respond with: {{"results": [{{"id": "...", "type": "news", "instruments": [], "is_macro": false}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def batch_sentiment_prompt(articles: list[dict], role: str, asset_desc: str) -> str:
    """Build a single prompt for DUAL-HORIZON sentiment analysis on multiple articles for one instrument.

    Returns both short-term (1-7 day) and long-term (1-6 month) sentiment per article.
    """
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

    return f"""{role}

You ONLY care about the PRICE of {asset_desc}. For each article, assess price impact on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate market reaction. Consider: earnings surprises, analyst upgrades/downgrades, momentum, trader sentiment, news-driven volatility, short squeezes, panic selling.

**LONG-TERM (1-6 months):** Fundamental value shift. Consider: competitive moat changes, market share dynamics, regulatory impact, structural demand shifts, management quality signals, sector rotation, capex implications.

Articles:{articles_text}
CRITICAL RULES — think about PRICE IMPACT, not morality:
- A piece of news CAN be positive short-term but negative long-term (e.g., cost-cutting layoffs boost margins now but signal declining revenue).
- A piece of news CAN be negative short-term but positive long-term (e.g., heavy R&D spending hurts near-term earnings but builds future moat).
- Wars/conflicts: evaluate ONLY on defense spending, supply chains, commodity prices — not human suffering.
- If irrelevant to the asset's price on a given horizon, assign neutral for that horizon.

Choose for each horizon: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0 for each.

Respond with: {{"results": [{{"id": "...", "short_sentiment": "neutral", "short_confidence": 0.5, "long_sentiment": "neutral", "long_confidence": 0.5}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def batch_macro_sentiment_prompt(articles: list[dict]) -> str:
    """Build a single prompt for DUAL-HORIZON macro sentiment on multiple articles."""
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

    return f"""You are a cold, rational S&P 500 index strategist. For each article, assess impact on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate risk-on/risk-off reaction. Consider: VIX implications, flight-to-safety, momentum, headline-driven algorithmic trading, institutional positioning.

**LONG-TERM (1-6 months):** Structural economic impact. Consider: interest rate trajectory, inflation persistence, GDP growth outlook, corporate earnings power, fiscal/monetary policy regime, geopolitical risk premium.

Articles:{articles_text}
RULES — S&P 500 PRICE IMPACT ONLY:
- Short-term and long-term impacts OFTEN diverge. A rate hike is bad short-term but may be good long-term if it controls inflation.
- Geopolitical events: penalize only if they cause direct US economic shocks. Distant conflicts are often neutral to mixed for the US index.
- Separate knee-jerk market reactions (short-term) from fundamental shifts (long-term).

Respond with: {{"results": [{{"id": "...", "short_sentiment": "BAD", "short_confidence": 0.8, "long_sentiment": "MIXED", "long_confidence": 0.5}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def sentiment_prompt(title: str, content: str, role: str, asset_desc: str) -> str:
    """Single-article dual-horizon sentiment (fallback)."""
    text = f"{title}. {content[:800]}" if content else title
    return f"""{role}

You ONLY care about the PRICE of {asset_desc}. Assess price impact on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate market reaction — momentum, sentiment, volatility.
**LONG-TERM (1-6 months):** Fundamental value shift — moat, market share, structural demand.

Article: "{text}"

CRITICAL RULES — think about PRICE IMPACT, not morality:
- Short-term and long-term CAN and OFTEN DO diverge.
- Wars/conflicts: evaluate ONLY on defense spending, supply chains, commodity prices.
- If irrelevant to this asset, assign neutral.

Choose for each: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0 for each.

{{"short_sentiment": "positive", "short_confidence": 0.7, "long_sentiment": "neutral", "long_confidence": 0.5}}"""


def macro_sentiment_prompt(title: str, content: str) -> str:
    """Single-article dual-horizon macro sentiment (fallback)."""
    text = f"{title}. {content[:800]}" if content else title
    return f"""You are a cold, rational S&P 500 strategist. Assess this news on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate risk-on/risk-off reaction.
**LONG-TERM (1-6 months):** Structural economic impact on US equities.

"{text}"

RULES — S&P 500 PRICE IMPACT ONLY:
- Short-term and long-term often diverge (rate hikes: bad short-term, good long-term if inflation controlled).
- Geopolitical events: penalize only if direct US economic shock.

{{"short_sentiment": "BAD", "short_confidence": 0.8, "long_sentiment": "MIXED", "long_confidence": 0.5}}"""


def etf_constituent_prompt(etf_name: str, etf_symbol: str) -> str:
    """Prompt to identify ETF constituents and their weights."""
    return f"""What are the top 10 holdings of the {etf_name} (ticker: {etf_symbol}) ETF?

Return a JSON array of the top holdings with their approximate portfolio weight percentages.
Only include holdings you are confident about. Use the stock ticker symbol.

Respond with: {{"constituents": [{{"symbol": "AAPL", "name": "Apple Inc.", "weight_percent": 18.5}}, ...]}}"""
