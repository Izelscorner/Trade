"""RSS feed definitions grouped by category."""

FEEDS: dict[str, list[dict[str, str]]] = {
    "macro_markets": [
        {"source": "Trading Economics", "url": "https://tradingeconomics.com/rss/"},
        {"source": "FT Global Economy", "url": "https://www.ft.com/global-economy?format=rss"},
        {"source": "WSJ World News", "url": "https://feeds.a.dj.com/rss/RSSWorldNews.xml"},
        {"source": "WSJ Markets", "url": "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"},
    ],
    "macro_politics": [
        {"source": "BBC World News", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
        {"source": "The Diplomat", "url": "https://thediplomat.com/feed/"},
        {"source": "Foreign Policy", "url": "https://foreignpolicy.com/feed/"},
        {"source": "Geopolitical Futures", "url": "https://geopoliticalfutures.com/feed"},
    ],
    "macro_conflict": [
        {"source": "War on the Rocks", "url": "https://warontherocks.com/feed/"},
        {"source": "Crisis Group", "url": "https://www.crisisgroup.org/rss-0"},
    ],
}

MACRO_CATEGORIES = {"macro_markets", "macro_politics", "macro_conflict"}
