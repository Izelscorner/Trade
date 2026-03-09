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

CLASSIFY_SYSTEM = """You are a financial news classifier for a Western investment firm trading US-listed assets.
You classify articles and tag which tracked instruments they relate to.
You filter spam (non-finance content) and correctly distinguish between macro-level news and company-specific news.
Always respond with valid JSON."""

SENTIMENT_SYSTEM = """You predict PRICE DIRECTION of a specific asset. You ONLY care about the PRICE going UP or DOWN.
IMPORTANT: For commodities (OIL, GOLD), wars and supply disruptions make the PRICE GO UP — that is POSITIVE.
Always respond with valid JSON only. No explanations."""

MACRO_SYSTEM = """You predict S&P 500 INDEX direction from news events. Be decisive — wars, inflation, rate changes are NOT neutral.
Always respond with valid JSON only. No explanations."""


# Category-based role templates for sentiment analysis
CATEGORY_ROLES = {
    "stock": "You are a Wall Street equity analyst covering {name}. You think like both a day-trader (short-term price action) and a fundamental investor (long-term value).",
    "etf": "You are a Wall Street ETF analyst covering {name}. You understand constituent-level impacts propagate to the ETF with weight-proportional magnitude.",
    "commodity": "You are a commodity futures trader at a Western bank trading {name}. You ONLY care about the PRICE of this commodity going UP or DOWN. Supply disruptions = PRICE UP = positive. Demand destruction = PRICE DOWN = negative. You distinguish between spot-price catalysts (short-term) and supply/demand structural shifts (long-term).",
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
- "type": "news" if real financial/political/economic/macroeconomic news with enough detail for analysis. "spam" if ads, lifestyle, entertainment, clickbait, sports, celebrity gossip, non-finance content, OR if too vague/generic for meaningful financial insight. Also "spam" for foreign domestic politics with no US market impact.
- "instruments": list of symbols from the valid symbols list that are DIRECTLY mentioned by name or ticker. Empty list [] if none. For ETFs, also tag the ETF if a major constituent is mentioned. Tag GOLD for safe-haven/geopolitical-risk articles. Tag OIL for oil supply/price articles.
- "is_macro": true ONLY if about forces affecting the BROAD MARKET — wars, geopolitics, sanctions, central banks, interest rates, GDP, inflation, tariffs, oil supply disruption. false if primarily about a specific company (earnings, products, stock picks, analyst ratings) even if from a general news feed.

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
- "type": "news" if real financial/political/economic/macroeconomic news with sufficient detail for analysis. "spam" if: ads, lifestyle, entertainment, clickbait, sports, celebrity gossip, non-finance content, OR if the title+content is too vague/generic to derive any meaningful financial insight (e.g. just a headline with no substance, auto-generated SEO filler, or aggregator summaries that only repeat the title). Also "spam" for: foreign domestic politics with no US market impact (e.g. Japan cabinet polls, UK domestic policy debates).
- "instruments": list of valid symbols DIRECTLY mentioned by name or ticker. [] if none. For ETFs, also tag the ETF symbol if a major constituent company is mentioned. Tag GOLD for safe-haven/geopolitical-risk articles. Tag OIL for oil supply/price articles.
- "is_macro": true ONLY if the article is about forces that affect the BROAD MARKET or economy — wars, geopolitics, sanctions, central banks, interest rates, GDP, inflation, tariffs, trade policy, oil supply disruptions, recession risk. Set false if the article is primarily about a specific company — even if it appears in a general news feed. Examples: "Apple iPhone launch" = false (company product). "Fed raises rates" = true. "Nvidia earnings beat" = false. "Oil surges to $100 on Iran war" = true. "Top stocks to buy" = false. "Global recession fears" = true.

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


def batch_macro_sentiment_prompt(articles: list[dict]) -> str:
    """Build a single prompt for DUAL-HORIZON macro sentiment on multiple articles."""
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

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


def sentiment_prompt(title: str, content: str, role: str, asset_desc: str) -> str:
    """Single-article dual-horizon sentiment (fallback)."""
    text = f"{title}. {content[:800]}" if content else title
    return f"""{role}

Predict: does this push the PRICE of {asset_desc} UP or DOWN?

Article: "{text}"

RULES:
- "upgrade"/"buy"/"overweight"/"PT raised" → positive. "downgrade"/"sell"/"PT cut" → negative
- Fund buys/sells shares → neutral, confidence 0.1-0.2
- OIL: war/supply disruption → positive. Demand drop → negative
- GOLD: crisis/war/rate cuts → positive. Rate hikes → negative
- Not about this asset → neutral, confidence 0.1-0.3
- Directly about this asset → be decisive

Labels: very_positive, positive, neutral, negative, very_negative

{{"short_sentiment": "...", "short_confidence": 0.7, "long_sentiment": "...", "long_confidence": 0.5}}"""


def macro_sentiment_prompt(title: str, content: str) -> str:
    """Single-article dual-horizon macro sentiment (fallback)."""
    text = f"{title}. {content[:800]}" if content else title
    return f"""Assess this news impact on S&P 500 INDEX PRICE.

"{text}"

RULES:
- War, oil spike, inflation → negative. Rate cuts, stimulus, strong GDP → positive
- Company-specific news → neutral, confidence 0.1-0.2
- DO NOT default to neutral if the event affects markets

Labels: very_positive, positive, neutral, negative, very_negative

{{"short_sentiment": "...", "short_confidence": 0.8, "long_sentiment": "...", "long_confidence": 0.6}}"""


SECTOR_SENTIMENT_SYSTEM = """You predict impact on a GICS sector's stock prices. Be decisive — regulatory changes and industry trends are NOT neutral.
Always respond with valid JSON only. No explanations."""


def sector_classify_prompt(instruments: list[dict]) -> str:
    """Prompt to classify instruments by GICS sector."""
    inst_text = "\n".join(
        f'  - symbol="{i["symbol"]}", name="{i["name"]}", category="{i["category"]}"'
        for i in instruments
    )
    return f"""Classify each instrument by its GICS sector. These are US-listed assets.

Instruments:
{inst_text}

Valid sectors: technology, financials, healthcare, consumer_discretionary, consumer_staples, communication, energy, industrials, materials, utilities, real_estate

Rules:
- Stocks: assign the primary GICS sector based on the company's main business.
- ETFs: assign the sector that best represents the ETF's focus. Broad-market ETFs (like S&P 500) should be null.
- Commodities: GOLD/silver → materials. OIL/natural gas → energy.
- If unsure, use the most specific sector that fits.

Respond with: {{"results": [{{"symbol": "AAPL", "sector": "technology"}}, {{"symbol": "VOO", "sector": null}}, ...]}}
Include ALL instruments in the results array."""


def batch_sector_sentiment_prompt(articles: list[dict], sector: str) -> str:
    """Build a prompt for dual-horizon sentiment analysis on sector news."""
    sector_display = sector.replace("_", " ").title()
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

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


def etf_constituent_prompt(etf_name: str, etf_symbol: str) -> str:
    """Prompt to identify ETF constituents and their weights."""
    return f"""What are the top 10 holdings of the {etf_name} (ticker: {etf_symbol}) ETF?

Return a JSON array of the top holdings with their approximate portfolio weight percentages.
Only include holdings you are confident about. Use the stock ticker symbol.

Respond with: {{"constituents": [{{"symbol": "AAPL", "name": "Apple Inc.", "weight_percent": 18.5}}, ...]}}"""
