"""RSS feed definitions grouped by category."""

FEEDS: dict[str, list[dict[str, str]]] = {
    "us_politics": [
        {"source": "NPR Politics", "url": "https://feeds.npr.org/1014/rss.xml"},
        {"source": "Politico", "url": "https://rss.politico.com/politics-news.xml"},
        {"source": "Fox News Politics", "url": "https://moxie.foxnews.com/google-publisher/politics.xml"},
        {"source": "CNN Politics", "url": "http://rss.cnn.com/rss/cnn_allpolitics.rss"},
        {"source": "CBS News Politics", "url": "https://www.cbsnews.com/latest/rss/politics"},
        {"source": "HuffPost Politics", "url": "https://www.huffpost.com/section/politics/feed"},
    ],
    "uk_politics": [
        {"source": "BBC UK Politics", "url": "http://feeds.bbci.co.uk/news/politics/rss.xml"},
        {"source": "The Guardian UK Politics", "url": "https://www.theguardian.com/politics/rss"},
        {"source": "Sky News Politics", "url": "https://feeds.skynews.com/feeds/rss/politics.xml"},
        {"source": "The Independent UK Politics", "url": "https://www.independent.co.uk/news/uk/politics/rss"},
        {"source": "HuffPost UK Politics", "url": "https://www.huffingtonpost.co.uk/news/politics/feed"},
    ],
    "us_finance": [
        {"source": "CNBC Finance", "url": "https://search.cnbc.com/rs/search/view.html?partnerId=2000&id=10000664"},
        {"source": "Yahoo Finance", "url": "https://finance.yahoo.com/news/rss"},
        {"source": "MarketWatch", "url": "http://feeds.marketwatch.com/marketwatch/topstories/"},
        {"source": "Investing.com", "url": "https://www.investing.com/rss/news.rss"},
        {"source": "CNN Business", "url": "http://rss.cnn.com/rss/money_latest.rss"},
    ],
    "uk_finance": [
        {"source": "BBC Business", "url": "http://feeds.bbci.co.uk/news/business/rss.xml"},
        {"source": "City A.M.", "url": "https://www.cityam.com/feed/"},
        {"source": "The Guardian Business", "url": "https://www.theguardian.com/business/rss"},
        {"source": "This is Money", "url": "https://www.thisismoney.co.uk/money/index.rss"},
        {"source": "Sky News Business", "url": "https://feeds.skynews.com/feeds/rss/business.xml"},
    ],
}

POLITICS_CATEGORIES = {"us_politics", "uk_politics"}
FINANCE_CATEGORIES = {"us_finance", "uk_finance"}
