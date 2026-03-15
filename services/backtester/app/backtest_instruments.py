"""Instrument search queries for backtester sentiment fetching.

All 50 instruments are seeded in the DB via 01_init.sql.
No extended/backtest-only logic — production and backtester share the same instrument set.
"""

# Search queries for Google News RSS sentiment fetching.
# Aligned with production news-fetcher: "{TICKER} stock" + "{name} stock market"
# These extend the base ASSET_QUERIES in historical_sentiment.py for the 35 non-original instruments.
EXTRA_ASSET_QUERIES: dict[str, list[str]] = {
    # Technology
    "MSFT":   ["MSFT stock", "Microsoft Corporation stock market"],
    "AMD":    ["AMD stock", "Advanced Micro Devices stock market"],
    "CRM":    ["CRM stock", "Salesforce Inc stock market"],
    # Financials
    "JPM":    ["JPM stock", "JPMorgan Chase stock market"],
    "GS":     ["GS stock", "Goldman Sachs stock market"],
    "BAC":    ["BAC stock", "Bank of America stock market"],
    "V":      ["V stock Visa", "Visa Inc stock market"],
    "MA":     ["MA stock Mastercard", "Mastercard stock market"],
    # Healthcare
    "JNJ":    ["JNJ stock", "Johnson Johnson stock market"],
    "UNH":    ["UNH stock", "UnitedHealth Group stock market"],
    "PFE":    ["PFE stock", "Pfizer Inc stock market"],
    # Consumer Discretionary
    "AMZN":   ["AMZN stock", "Amazon stock market"],
    "HD":     ["HD stock", "Home Depot stock market"],
    "NKE":    ["NKE stock", "Nike Inc stock market"],
    # Consumer Staples
    "PG":     ["PG stock", "Procter Gamble stock market"],
    "KO":     ["KO stock", "Coca-Cola stock market"],
    "COST":   ["COST stock", "Costco stock market"],
    # Communication
    "META":   ["META stock", "Meta Platforms stock market"],
    "DIS":    ["DIS stock", "Walt Disney stock market"],
    # Energy
    "CVX":    ["CVX stock", "Chevron Corporation stock market"],
    "COP":    ["COP stock", "ConocoPhillips stock market"],
    "SLB":    ["SLB stock", "Schlumberger stock market"],
    # Industrials
    "CAT":    ["CAT stock", "Caterpillar Inc stock market"],
    "BA":     ["BA stock", "Boeing Company stock market"],
    "GE":     ["GE stock", "GE Aerospace stock market"],
    # Materials
    "LIN":    ["LIN stock", "Linde plc stock market"],
    "FCX":    ["FCX stock", "Freeport McMoRan stock market"],
    # Utilities
    "NEE":    ["NEE stock", "NextEra Energy stock market"],
    "DUK":    ["DUK stock", "Duke Energy stock market"],
    # Real Estate
    "AMT":    ["AMT stock", "American Tower stock market"],
    "PLD":    ["PLD stock", "Prologis Inc stock market"],
    # ETFs
    "QQQ":    ["QQQ ETF", "Invesco QQQ Trust stock market"],
    "IWM":    ["IWM ETF", "iShares Russell 2000 ETF stock market"],
    "XLF":    ["XLF ETF", "Financial Select Sector SPDR stock market"],
    "XLE":    ["XLE ETF", "Energy Select Sector SPDR stock market"],
    # Commodities
    "SILVER": ["silver price futures", "silver bullion precious metals"],
    "NATGAS": ["natural gas price futures", "natural gas energy LNG"],
}
