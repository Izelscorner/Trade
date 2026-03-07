"""Prompt templates for news classification and sentiment analysis.

Western quant analysis perspective — designed for capable models (32B+).
All prompts target structured JSON output.

Key principles:
- We trade US-listed assets: US stocks, US ETFs, gold futures, oil futures
- Sentiment = expected PRICE DIRECTION for each specific asset
- Macro sentiment = impact on S&P 500 / Nasdaq composite
- Western geopolitical bias: what benefits Western power projection is GOOD
"""

CLASSIFY_SYSTEM = """You are a financial news classifier for a Western investment firm.
You classify articles and tag which tracked instruments they relate to.
Always respond with valid JSON."""

SENTIMENT_SYSTEM = """You are a Western quantitative analyst predicting asset price direction from news.
Always respond with valid JSON."""

MACRO_SYSTEM = """You are a macro strategist at a Western investment bank predicting S&P 500 direction.
Always respond with valid JSON."""


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
- "type": "news" if real financial/political/economic news. "spam" if ads, lifestyle, entertainment, clickbait, or non-news.
- "instruments": list of symbols from the valid symbols list that are DIRECTLY mentioned by name or ticker. Empty list [] if none.
- "is_macro": true if about wars, military conflicts, geopolitics, sanctions, trade policy, central bank decisions, interest rates, GDP, inflation, recession, tariffs, or global economic policy. false if about specific company earnings, stock picks, product launches, or individual stock analysis.

{{"type": "news", "instruments": [], "is_macro": false}}"""


def batch_classify_prompt(articles: list[dict], symbol_mapping: str, valid_symbols: str) -> str:
    """Build a single prompt to classify multiple articles in one API call.

    Returns a JSON object: {"results": [{...}, ...]} where each item matches
    the single-article classify schema plus an "id" field for correlation.
    """
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
- "type": "news" or "spam"
- "instruments": list of valid symbols DIRECTLY mentioned. [] if none.
- "is_macro": true if about wars, geopolitics, sanctions, central banks, GDP, inflation, tariffs. false otherwise.

Respond with: {{"results": [{{"id": "...", "type": "news", "instruments": [], "is_macro": false}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def batch_sentiment_prompt(articles: list[dict], role: str, asset_desc: str) -> str:
    """Build a single prompt for sentiment analysis on multiple articles for one instrument.

    Each article entry has: id, title, content.
    Returns: {"results": [{"id": "...", "sentiment": "...", "confidence": 0.0}, ...]}
    """
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

    return f"""{role}

You ONLY care about the STOCK PRICE of {asset_desc}. You are a cold, rational trader. Will each news item make the price go UP or DOWN?

Articles:{articles_text}
CRITICAL RULES — think about PRICE IMPACT, not morality:
- Evaluate the news as a purely quantitative analyst.
- Focus exclusively on how the information affects the expected future cash flows, risk premium, or institutional demand for this specific asset.
- Strip away all ethical, social, or human-interest biases. E.g., wars/conflicts should only be evaluated on whether they increase defense spending, disrupt supply chains, or spike commodity prices.
- If the article is irrelevant to the asset's price, assign neutral.

Choose: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0.

Respond with: {{"results": [{{"id": "...", "sentiment": "neutral", "confidence": 0.5}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def batch_macro_sentiment_prompt(articles: list[dict]) -> str:
    """Build a single prompt for macro sentiment on multiple articles.

    Returns: {"results": [{"id": "...", "sentiment": "VERY_BAD"|"BAD"|"MIXED"|"GOOD"|"VERY_GOOD", "confidence": 0.0}, ...]}
    """
    articles_text = ""
    for i, art in enumerate(articles):
        title = art["title"]
        content = art.get("content") or art.get("summary") or ""
        text = f"{title}. {content[:200]}" if content else title
        articles_text += f'\n{i + 1}. id="{art["id"]}": "{text[:350]}"\n'

    return f"""You are a cold, rational S&P 500 index trader. For each article, will the news move the S&P 500 UP or DOWN?
You do NOT care about human suffering or ethics. ONLY about US stock market price direction.

Articles:{articles_text}
RULES — S&P 500 PRICE IMPACT ONLY:
- Evaluate the news purely on its macroeconomic implications for US equities (interest rates, systemic liquidity, inflation, GDP growth, corporate earnings power).
- Strip out human suffering or ethical biases. Geopolitical events (wars, tension) should only be penalized if they cause a direct US economic shock (e.g., severe inflation spike via oil, immediate supply chain halt, direct US/NATO homeland threat). Otherwise, local distant conflicts are generally mixed or neutral to the US index.
- Assess whether the news indicates an easing environment (good for stocks) or a tightening/risk-off environment (bad for stocks).

Respond with: {{"results": [{{"id": "...", "sentiment": "BAD", "confidence": 0.8}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def sentiment_prompt(title: str, content: str, role: str, asset_desc: str) -> str:
    text = f"{title}. {content[:800]}" if content else title
    return f"""{role}

You ONLY care about the STOCK PRICE of {asset_desc}. You do NOT care about world peace, human suffering, or ethics. You are a cold, rational trader. Will this news make the price go UP or DOWN?

Article: "{text}"

CRITICAL RULES — think about PRICE IMPACT, not morality:
- Evaluate the news as a purely quantitative analyst.
- Focus exclusively on how the information affects the expected future cash flows, risk premium, or institutional demand for this specific asset.
- Strip away all ethical, social, or human-interest biases. E.g., wars/conflicts should only be evaluated on whether they increase defense spending, disrupt supply chains, or spike commodity prices.
- If the article is irrelevant to the asset's price, assign neutral.

Choose: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0.

{{"sentiment": "positive", "confidence": 0.7}}"""


def macro_sentiment_prompt(title: str, content: str) -> str:
    text = f"{title}. {content[:800]}" if content else title
    return f"""You are a cold, rational S&P 500 index trader. Will this news move the S&P 500 UP or DOWN?
You do NOT care about human suffering or ethics. ONLY about US stock market price direction.

"{text}"

RULES — S&P 500 PRICE IMPACT ONLY:
- Evaluate the news purely on its macroeconomic implications for US equities (interest rates, systemic liquidity, inflation, GDP growth, corporate earnings power).
- Strip out human suffering or ethical biases. Geopolitical events (wars, tension) should only be penalized if they cause a direct US economic shock (e.g., severe inflation spike via oil, immediate supply chain halt, direct US/NATO homeland threat). Otherwise, local distant conflicts are generally mixed or neutral to the US index.
- Assess whether the news indicates an easing environment (good for stocks) or a tightening/risk-off environment (bad for stocks).

{{"sentiment": "BAD", "confidence": 0.8}}"""
