Good question. Let me map your current data sources against what consolidated API providers offer for your specific needs:

Your current data needs:

Data Current Source Cost
Live + Historical Prices yfinance Free (unreliable, scraping)
Fundamentals (P/E, ROE, D/E, PEG) FMP free tier Free (250 req/day, some 402s)
Macro Indicators (DXY, 10Y, GDP, Brent) FRED Free
Financial News RSS feeds (Google/Yahoo/FT) Free (scraping, no API guarantees)
Technical Indicators Computed in-house N/A
Top 3 consolidation options:

1. FMP Paid ($29/mo Starter or $79/mo Growth)
   You already use this — upgrading eliminates most gaps:

Prices: Real-time + historical + intraday (replaces yfinance)
Fundamentals: Full coverage, no 402 errors on paid tier
News: /stable/news endpoint with sentiment — could replace RSS scraping
Economic data: /stable/economic has Treasury yields, GDP, CPI, unemployment (partial FRED replacement)
ETF holdings: /stable/etf-holder (replaces LLM-based constituent detection)
Missing: DXY index, Brent crude as economic series (still need FRED for those, or fetch via prices)
Rate limit: 300 req/min on Starter, 750 on Growth 2. Polygon.io ($29/mo Starter, $79/mo Developer)
Best price data quality in this price range:

Prices: WebSocket real-time streaming, excellent historical, 1-min+ intraday
News: Aggregated financial news with tickers tagged
Fundamentals: Via reference/financials endpoints (vX)
Missing: Macro economic indicators entirely — still need FRED
Rate limit: Unlimited on paid tiers
Strength: WebSocket streaming would replace your 60s polling loop 3. Alpha Vantage ($49.99/mo Premium)
Most complete single-provider coverage:

Prices: Real-time + historical + intraday
Fundamentals: Full income statement, balance sheet, ratios
News: /query?function=NEWS_SENTIMENT with AI sentiment scores
Economic indicators: REAL_GDP, TREASURY_YIELD, FEDERAL_FUNDS_RATE, CPI, UNEMPLOYMENT
Technical indicators: Pre-computed RSI, MACD, SMA, etc. (could replace your technical-analysis service)
Missing: DXY, Brent crude as economic series
Rate limit: 75 req/min (tight for 15+ instruments with multiple data types)
My recommendation
FMP Growth ($79/mo) is the most practical path because:

You're already integrated — just upgrade the API key, no code rewrite
Eliminates yfinance (the most fragile dependency — it's scraping, not an API)
Eliminates RSS scraping — FMP news is structured with tickers pre-tagged
No more 402 errors on paid tier
750 req/min easily handles 15+ instruments with room to scale to hundreds
Keep FRED for the 4 macro series (it's free, reliable, and purpose-built for that data)
If price data quality is the #1 priority (you mentioned price performance is very important), then Polygon.io Developer ($79/mo) is superior — their WebSocket streaming and tick-level data are institutional grade. But you'd still need FMP for fundamentals and FRED for macro, so it's less of a consolidation play.

The worst-value option would be trying to cover everything with multiple cheap/free APIs — the reliability and maintenance cost of managing 4+ integrations outweighs paying $79/mo for one solid provider.

Want me to prototype a migration to any of these?
