"""RSS feed definitions grouped by category and fetch interval."""

# Feeds fetched every 10 seconds (main loop)
MAIN_FEEDS: dict[str, list[dict[str, str]]] = {
    "macro_markets": [
        {"source": "FT Global Economy", "url": "https://www.ft.com/global-economy?format=rss"},
        {"source": "WSJ World News", "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml"},
        {"source": "WSJ Markets", "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"},
        # Google News macro
        {"source": "Google News: Global Economy", "url": "https://news.google.com/rss/search?q=global%20economy&hl=en-GB&gl=GB&ceid=GB:en"},
        {"source": "Google News: Macro Economy", "url": "https://news.google.com/rss/search?q=macro%20economy&hl=en-GB&gl=GB&ceid=GB:en"},
        {"source": "Google News: Latest", "url": "https://news.google.com/rss/search?q=latest&hl=en-GB&gl=GB&ceid=GB:en"},
        {"source": "Google News: Breaking", "url": "https://news.google.com/rss/search?q=breaking&hl=en-GB&gl=GB&ceid=GB:en"},
        # Institutional
        {"source": "Thomson Reuters", "url": "https://ir.thomsonreuters.com/rss/news-releases.xml?items=15"},
        {"source": "Chatham House News", "url": "https://www.chathamhouse.org/path/news-releases.xml"},
        {"source": "Chatham House Analysis", "url": "https://www.chathamhouse.org/path/83/feed.xml"},
        {"source": "Chatham House Events", "url": "https://www.chathamhouse.org/path/events.xml"},
        # NASDAQ Trader
        {"source": "NASDAQ Headlines", "url": "https://www.nasdaqtrader.com/rss.aspx?feed=currentheadlines&categorylist=0"},
        {"source": "NASDAQ Trading Halts", "url": "https://www.nasdaqtrader.com/rss.aspx?feed=currentheadlines&categorylist=2,6,7"},
        {"source": "NASDAQ System Status", "url": "https://www.nasdaqtrader.com/rss.aspx?feed=currentheadlines&categorylist=11,12,13"},
    ],
    "macro_politics": [
        {"source": "BBC World News", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
        {"source": "The Diplomat", "url": "https://thediplomat.com/feed/"},
        {"source": "Foreign Policy", "url": "https://foreignpolicy.com/feed/"},
        {"source": "Geopolitical Futures", "url": "https://geopoliticalfutures.com/feed"},
    ],
    "macro_conflict": [
        {"source": "War on the Rocks", "url": "https://warontherocks.com/feed/"},
    ],
}

# Feeds fetched every 30 seconds (slower loop — higher volume sources)
SLOW_FEEDS: dict[str, list[dict[str, str]]] = {
    "macro_markets": [
        {"source": "MarketWatch Top Stories", "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories"},
    ],
}

# Asset-specific feeds are built dynamically per instrument:
#   Yahoo Finance: https://finance.yahoo.com/rss/headline?s={TICKER}
#   Google News:   https://news.google.com/rss/search?q={TICKER}+stock

MACRO_CATEGORIES = {"macro_markets", "macro_politics", "macro_conflict"}

# Sector-specific feeds — GICS sector classification
SECTOR_FEEDS: dict[str, list[dict[str, str]]] = {
    "sector_technology": [
        {"source": "Google News: Technology", "url": "https://news.google.com/rss/search?q=technology+sector+OR+tech+industry+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Semiconductors", "url": "https://news.google.com/rss/search?q=semiconductor+industry+OR+chips+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Technology", "url": "https://finance.yahoo.com/rss/headline?s=XLK,VGT,SMH"},
    ],
    "sector_financials": [
        {"source": "Google News: Financials", "url": "https://news.google.com/rss/search?q=financial+sector+OR+banking+industry+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Wall Street", "url": "https://news.google.com/rss/search?q=wall+street+banks+OR+finance+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Financials", "url": "https://finance.yahoo.com/rss/headline?s=XLF,VFH,KRE"},
    ],
    "sector_healthcare": [
        {"source": "Google News: Healthcare", "url": "https://news.google.com/rss/search?q=healthcare+sector+OR+pharma+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Biotech", "url": "https://news.google.com/rss/search?q=biotech+industry+OR+pharmaceuticals+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Healthcare", "url": "https://finance.yahoo.com/rss/headline?s=XLV,VHT,XBI"},
    ],
    "sector_consumer_discretionary": [
        {"source": "Google News: Consumer Discretionary", "url": "https://news.google.com/rss/search?q=consumer+discretionary+OR+retail+sector+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Retail", "url": "https://news.google.com/rss/search?q=retail+industry+OR+consumer+spending+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Cons. Disc.", "url": "https://finance.yahoo.com/rss/headline?s=XLY,VCR,XRT"},
    ],
    "sector_consumer_staples": [
        {"source": "Google News: Consumer Staples", "url": "https://news.google.com/rss/search?q=consumer+staples+OR+food+and+beverage+industry+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Consumer Goods", "url": "https://news.google.com/rss/search?q=consumer+goods+OR+household+products+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Cons. Staples", "url": "https://finance.yahoo.com/rss/headline?s=XLP,VDC,KXI"},
    ],
    "sector_communication": [
        {"source": "Google News: Communication", "url": "https://news.google.com/rss/search?q=communication+services+OR+telecom+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Media", "url": "https://news.google.com/rss/search?q=media+industry+OR+streaming+services+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Communication", "url": "https://finance.yahoo.com/rss/headline?s=XLC,VOX"},
    ],
    "sector_energy": [
        {"source": "Google News: Energy", "url": "https://news.google.com/rss/search?q=energy+sector+OR+oil+and+gas+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Renewables", "url": "https://news.google.com/rss/search?q=renewable+energy+OR+clean+energy+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Energy", "url": "https://finance.yahoo.com/rss/headline?s=XLE,VDE,XOP,ICLN"},
    ],
    "sector_industrials": [
        {"source": "Google News: Industrials", "url": "https://news.google.com/rss/search?q=industrials+sector+OR+manufacturing+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Aerospace", "url": "https://news.google.com/rss/search?q=defense+industry+OR+aerospace+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Industrials", "url": "https://finance.yahoo.com/rss/headline?s=XLI,VIS,ITA"},
    ],
    "sector_materials": [
        {"source": "Google News: Materials", "url": "https://news.google.com/rss/search?q=materials+sector+OR+mining+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Commodities", "url": "https://news.google.com/rss/search?q=commodities+OR+metals+industry+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Materials", "url": "https://finance.yahoo.com/rss/headline?s=XLB,VAW,GDX"},
    ],
    "sector_utilities": [
        {"source": "Google News: Utilities", "url": "https://news.google.com/rss/search?q=utilities+sector+OR+electricity+grid+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Power", "url": "https://news.google.com/rss/search?q=power+generation+OR+water+utilities+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Utilities", "url": "https://finance.yahoo.com/rss/headline?s=XLU,VPU"},
    ],
    "sector_real_estate": [
        {"source": "Google News: Real Estate", "url": "https://news.google.com/rss/search?q=real+estate+sector+OR+commercial+real+estate+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Housing", "url": "https://news.google.com/rss/search?q=housing+market+OR+REITs+when:7d&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Yahoo Finance: Real Estate", "url": "https://finance.yahoo.com/rss/headline?s=XLRE,VNQ"},
    ],
}

SECTOR_CATEGORIES = set(SECTOR_FEEDS.keys())

ALL_CATEGORIES = MACRO_CATEGORIES | SECTOR_CATEGORIES | {"asset_specific"}
