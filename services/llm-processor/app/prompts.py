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

SENTIMENT_SYSTEM = """You are a Western quantitative analyst and behavioral finance expert.
You predict the PRICE DIRECTION of a specific asset from news on TWO time horizons:
- SHORT-TERM (1-7 days): immediate market reaction, momentum, sentiment-driven moves
- LONG-TERM (1-6 months): fundamental value impact, structural changes, competitive positioning
You always think about what happens to the PRICE of the specific asset being analyzed — not the economy in general.
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

You ONLY care about the PRICE of {asset_desc}. For each article, predict whether it will push the PRICE UP or DOWN on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate market reaction. Consider: earnings surprises, analyst upgrades/downgrades, momentum, trader sentiment, news-driven volatility, sector rotation, panic buying/selling.

**LONG-TERM (1-6 months):** Fundamental value shift. Consider: competitive moat changes, market share dynamics, regulatory impact, structural demand shifts, management quality signals, capex implications, margin trajectory.

Articles:{articles_text}
CRITICAL RULES — think ONLY about what happens to the PRICE of {asset_desc}:
1. ANALYST RATINGS: "overweight", "outperform", "buy", "upgrade", "price target raised" = POSITIVE. "underweight", "sell", "downgrade", "price target cut" = NEGATIVE. Never invert these.
2. INSTITUTIONAL TRADES: Small fund buy/sell of shares (e.g. "buys 5,000 shares", "sells 10,000 shares") = neutral with LOW confidence (0.1-0.2). These are routine rebalancing and do not move prices.
3. COMMODITY PRICES: For OIL/GOLD futures — supply disruptions, wars, and geopolitical crises that reduce supply are POSITIVE for the commodity PRICE on BOTH horizons. Do NOT confuse "bad for economy" with "bad for commodity price". When oil supply is threatened, oil PRICE goes UP — that is positive for OIL futures. Only assign negative if there is clear DEMAND DESTRUCTION or supply increase.
4. SAFE HAVENS: Geopolitical crises, wars, market fear = POSITIVE for GOLD price (flight to safety). Rate cuts = POSITIVE for GOLD. Rate hikes = NEGATIVE for GOLD.
5. DEFENSE STOCKS: Wars, military conflicts, defense spending increases = POSITIVE for defense stocks (RTX, etc.).
6. If the article is NOT DIRECTLY about this specific asset and has no clear causal link, assign neutral with LOW confidence (0.1-0.3).
7. If the article IS directly about this asset, be DECISIVE — avoid neutral when there is a clear directional signal.
8. Short-term and long-term CAN diverge (e.g., R&D spending: negative short-term, positive long-term).
9. Oil price surges are NEGATIVE for most stocks (input cost inflation) but POSITIVE for oil/energy companies.

Choose for each horizon: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0 for each (higher = more certain of direction).

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

    return f"""You are a cold, rational S&P 500 index strategist. For each article, assess impact on the S&P 500 INDEX PRICE on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate risk-on/risk-off reaction. Consider: VIX implications, flight-to-safety flows, momentum reversal, headline-driven algorithmic trading, institutional repositioning.

**LONG-TERM (1-6 months):** Structural economic impact. Consider: interest rate trajectory changes, inflation persistence or moderation, GDP growth outlook revision, corporate earnings power shifts, fiscal/monetary policy regime changes, geopolitical risk premium repricing.

Articles:{articles_text}
RULES — S&P 500 INDEX PRICE IMPACT ONLY:
- Short-term and long-term OFTEN diverge. A rate hike is negative short-term but may be positive long-term if it controls inflation.
- Wars/conflicts that affect oil supply, trade routes, or US defense spending have REAL impacts — do NOT default to neutral.
- Inflation persistence, rate trajectory shifts, and fiscal policy changes are almost NEVER neutral long-term.
- If an article is about a SPECIFIC COMPANY (product launch, earnings, analyst rating) and NOT about broad market forces, assign neutral with LOW confidence (0.1-0.2) — individual stocks don't move the index.
- Only use "neutral" when the article genuinely has no directional S&P 500 impact.
- Be decisive: if an event shifts probabilities even modestly, assign positive/negative — not neutral.
- Set confidence proportional to how directly the event impacts the S&P 500. War affecting oil supply = high confidence. Foreign domestic politics = low confidence.

Choose for each horizon: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0 for each.

Respond with: {{"results": [{{"id": "...", "short_sentiment": "negative", "short_confidence": 0.8, "long_sentiment": "negative", "long_confidence": 0.6}}, ...]}}
Include ALL {len(articles)} articles in the results array."""


def sentiment_prompt(title: str, content: str, role: str, asset_desc: str) -> str:
    """Single-article dual-horizon sentiment (fallback)."""
    text = f"{title}. {content[:800]}" if content else title
    return f"""{role}

You ONLY care about the PRICE of {asset_desc}. Predict whether this will push the PRICE UP or DOWN on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate market reaction — momentum, sentiment, volatility.
**LONG-TERM (1-6 months):** Fundamental value shift — moat, market share, structural demand.

Article: "{text}"

CRITICAL RULES — think ONLY about what happens to the PRICE of {asset_desc}:
- ANALYST RATINGS: "overweight"/"buy"/"upgrade" = POSITIVE. "underweight"/"sell"/"downgrade" = NEGATIVE. Never invert.
- INSTITUTIONAL TRADES: Small fund buy/sell of shares = neutral with LOW confidence (0.1-0.2).
- COMMODITY PRICES: Supply disruptions are POSITIVE for commodity prices. Don't confuse "bad for economy" with "bad for commodity price".
- SAFE HAVENS: Crises = POSITIVE for GOLD (flight to safety).
- If NOT DIRECTLY about this asset, assign neutral with LOW confidence (0.1-0.3).
- If DIRECTLY about this asset, be decisive — avoid neutral when there's a clear signal.

Choose for each: very_positive, positive, neutral, negative, very_negative
Set confidence 0.0-1.0 for each.

{{"short_sentiment": "positive", "short_confidence": 0.7, "long_sentiment": "neutral", "long_confidence": 0.5}}"""


def macro_sentiment_prompt(title: str, content: str) -> str:
    """Single-article dual-horizon macro sentiment (fallback)."""
    text = f"{title}. {content[:800]}" if content else title
    return f"""You are a cold, rational S&P 500 strategist. Assess this news impact on the S&P 500 INDEX PRICE on TWO horizons:

**SHORT-TERM (1-7 days):** Immediate risk-on/risk-off reaction.
**LONG-TERM (1-6 months):** Structural economic impact on US equities.

"{text}"

RULES — S&P 500 INDEX PRICE IMPACT ONLY:
- Short-term and long-term often diverge (rate hikes: negative short-term, positive long-term if inflation controlled).
- Wars/conflicts affecting oil supply, trade routes, or defense spending have REAL impacts — not neutral.
- If about a SPECIFIC COMPANY (not broad market forces), assign neutral with LOW confidence.
- Be decisive: if it shifts probabilities even modestly, assign positive/negative.

Choose for each: very_positive, positive, neutral, negative, very_negative

{{"short_sentiment": "negative", "short_confidence": 0.8, "long_sentiment": "negative", "long_confidence": 0.6}}"""


def etf_constituent_prompt(etf_name: str, etf_symbol: str) -> str:
    """Prompt to identify ETF constituents and their weights."""
    return f"""What are the top 10 holdings of the {etf_name} (ticker: {etf_symbol}) ETF?

Return a JSON array of the top holdings with their approximate portfolio weight percentages.
Only include holdings you are confident about. Use the stock ticker symbol.

Respond with: {{"constituents": [{{"symbol": "AAPL", "name": "Apple Inc.", "weight_percent": 18.5}}, ...]}}"""
