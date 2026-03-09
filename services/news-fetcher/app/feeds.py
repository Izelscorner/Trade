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
        {"source": "Google News: Technology Sector", "url": "https://news.google.com/rss/search?q=technology+sector+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Semiconductor Industry", "url": "https://news.google.com/rss/search?q=semiconductor+industry&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_financials": [
        {"source": "Google News: Financial Sector", "url": "https://news.google.com/rss/search?q=financial+sector+banking+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Banking Industry", "url": "https://news.google.com/rss/search?q=banking+industry+wall+street&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_healthcare": [
        {"source": "Google News: Healthcare Sector", "url": "https://news.google.com/rss/search?q=healthcare+sector+pharma+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Biotech Industry", "url": "https://news.google.com/rss/search?q=biotech+pharmaceutical+industry&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_consumer_discretionary": [
        {"source": "Google News: Consumer Discretionary", "url": "https://news.google.com/rss/search?q=consumer+discretionary+retail+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Retail Industry", "url": "https://news.google.com/rss/search?q=retail+industry+consumer+spending&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_consumer_staples": [
        {"source": "Google News: Consumer Staples", "url": "https://news.google.com/rss/search?q=consumer+staples+sector+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Food & Beverage Industry", "url": "https://news.google.com/rss/search?q=food+beverage+industry+consumer+goods&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_communication": [
        {"source": "Google News: Communication Services", "url": "https://news.google.com/rss/search?q=communication+services+sector+media+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Telecom & Media", "url": "https://news.google.com/rss/search?q=telecom+media+streaming+industry&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_energy": [
        {"source": "Google News: Energy Sector", "url": "https://news.google.com/rss/search?q=energy+sector+oil+gas+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Oil & Gas Industry", "url": "https://news.google.com/rss/search?q=oil+gas+industry+energy+transition&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_industrials": [
        {"source": "Google News: Industrials Sector", "url": "https://news.google.com/rss/search?q=industrials+sector+manufacturing+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Defense & Aerospace", "url": "https://news.google.com/rss/search?q=defense+aerospace+industry&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_materials": [
        {"source": "Google News: Materials Sector", "url": "https://news.google.com/rss/search?q=materials+sector+mining+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Mining & Commodities", "url": "https://news.google.com/rss/search?q=mining+commodities+metals+industry&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_utilities": [
        {"source": "Google News: Utilities Sector", "url": "https://news.google.com/rss/search?q=utilities+sector+stocks+electricity&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Power & Utilities", "url": "https://news.google.com/rss/search?q=power+utilities+renewable+energy+grid&hl=en-US&gl=US&ceid=US:en"},
    ],
    "sector_real_estate": [
        {"source": "Google News: Real Estate Sector", "url": "https://news.google.com/rss/search?q=real+estate+sector+REIT+stocks&hl=en-US&gl=US&ceid=US:en"},
        {"source": "Google News: Housing & Commercial", "url": "https://news.google.com/rss/search?q=housing+market+commercial+real+estate&hl=en-US&gl=US&ceid=US:en"},
    ],
}

SECTOR_CATEGORIES = set(SECTOR_FEEDS.keys())

ALL_CATEGORIES = MACRO_CATEGORIES | SECTOR_CATEGORIES | {"asset_specific"}
