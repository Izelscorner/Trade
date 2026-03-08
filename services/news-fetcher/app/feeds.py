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

ALL_CATEGORIES = MACRO_CATEGORIES | {"asset_specific"}
