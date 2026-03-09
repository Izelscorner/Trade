# Trade AI System Optimization & Strategy

This document outlines the core requirements, strategic goals, and technical specifications for the Trade AI sentiment analysis system.

---

## 🎯 Core Objectives

### 1. Accuracy & Re-evaluation

- **Expert Analysis:** Act as a finance, data, and behavioral science expert to evaluate previous data analysis and prompt improvements.
- **Refinement:** Re-evaluate current datasets to ensure categorizations (Asset-specific vs. Macro-economic) are absolutely correct.
- **Sentiment Fidelity:** Ensure sentiment analysis is purely deterministic and accurately reflects the impact on specific assets and the broader macro economy.
- **Decision Support:** Continuous adjustment of prompts to ensure the system is optimal and highly accurate for financial decision-making.

### 2. Large-Scale Scaling & Efficiency

- **Optimization:** Streamline LLM and news fetching calls. Avoid redundant processing for already categorized or graded news.
- **Scalability:** Transition from 15 instruments to hundreds.
- **Dynamic Fetching:**
  - High-volume instruments (focused) should fetch deeper historical data.
  - Low-priority instruments should fetch fewer stories to ensure even distribution.
- **Rate Limit Management:** Prevent `429` errors through intelligent distribution of requests while maintaining continuous processing.
- **Switching Logic:** Move to the next instrument once confidence levels for long/short signals are sufficiently high.

---

## 🛠 Features & UI Requirements

### 1. Sectors & Mathematical Impact

- **Categorization:** Automatically categorize instruments by sector using LLM (performed once at seeding/creation).
- **Sector Sentiment:** Fetch sector-specific news and calculate sentiment impact.
- **Mathematical Modeling:** Quantitatively integrate sector sentiment into individual instrument grades to reflect real-world correlation.
- **UI Filters:** Add a sector filter in the asset page to group related instruments.

### 2. System Dashboard

- Create a dedicated page for monitoring real-time processing.
- Display:
  - Currently processing news.
  - Grading status.
  - Queued items.
  - System health metrics.

### 3. Breaking News Mechanism

- **Detection:** Implement a specialized detector for highly rare but high-impact "Breaking News" events (e.g., massive regulatory shifts or disruptive technology announcements).
- **Impact:** These events can override standard sentiment scoring (up to 100% influence).
- **UI Alerts:** Display persistent notifications for breaking news events that can only be dismissed manually via an "X" button.

---

## 📰 News Source Directory

### Sector-Specific Feeds

| Sector                  | Focus                         | RSS Feed URL                                                                                                                                                |
| :---------------------- | :---------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Information Tech**    | Semiconductors, AI, Software  | [MIT Tech Review](https://www.technologyreview.com/feed/), [Slashdot](https://rss.slashdot.org/Slashdot/slashdotMain)                                       |
| **Financials**          | Banks, Asset Management       | [American Banker](https://www.americanbanker.com/feed), [BIS Press](https://www.bis.org/about/rss_press.xml)                                                |
| **Health Care**         | Pharma, Biotech               | [STAT News](https://www.statnews.com/feed/), [FDA Press](https://www.fda.gov/about-fda/fda-newsroom/press-announcements/rss.xml)                            |
| **Cons. Discretionary** | Retail, Luxury, Travel        | [Retail Dive](https://www.retaildive.com/feeds/news/), [Skift](https://www.skift.com/feed/)                                                                 |
| **Cons. Staples**       | Staples, Household            | [Food Navigator](https://www.foodnavigator-usa.com/service/content/feed/84724), [Beverage Daily](https://www.beverage-daily.com/service/content/feed/84723) |
| **Comm. Services**      | Big Tech, Media               | [Hollywood Reporter](https://www.hollywoodreporter.com/c/business/feed/), [Light Reading](https://www.lightreading.com/rss.asp)                             |
| **Energy**              | Oil, Gas, Renewables          | [OilPrice.com](https://oilprice.com/rss/main), [IEA News](https://www.iea.org/newsroom/news/rss.xml)                                                        |
| **Industrials**         | Defense, Machinery, Logistics | [Defense News](https://www.defensenews.com/arc/outboundfeeds/rss/), [SupplyChainBrain](https://www.supplychainbrain.com/rss/articles)                       |
| **Materials**           | Mining, Chemicals             | [Mining.com](https://www.mining.com/feed/), [Chemical Week](https://www.chemweek.com/rss/news)                                                              |
| **Utilities**           | Electric, Gas, Water          | [Utility Dive](https://www.utilitydive.com/feeds/news/), [EIA Today](https://www.eia.gov/about/rss/todayinenergy.xml)                                       |
| **Real Estate**         | REITs, Property               | [Nareit](https://www.reit.com/news/rss), [The Real Deal](https://www.therealdeal.com/feed/)                                                                 |

### Macro & Market-Wide Feeds

- **Investing.com:** [General](https://www.investing.com/rss/news.rss) | [Economy/Macro](https://www.investing.com/rss/news_285.rss)
- **MarketWatch (Bulletin):** [Feed](http://feeds.marketwatch.com/marketwatch/bulletins)
- **Financial Times (Global Economy):** [Feed](https://www.ft.com/global-economy?format=rss)

### Asset-Specific Feeds

- **Yahoo Finance:** `https://finance.yahoo.com/rss/headline?s=TICKER` (Example: [NVDA](https://finance.yahoo.com/rss/headline?s=NVDA))
- **SEC Filings (EDGAR 8-K):** [Feed](https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&owner=include&output=atom)
- **PR Newswire:** [Feed](https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss)

### Social Media (via RSS.app)

- [Twitter Feed 1](https://rss.app/feeds/v1.1/lwBUtawbS88Lde9Z.json)
- [Twitter Feed 2](https://rss.app/feeds/v1.1/u5Ogu89DyLL0US5n.json)

---

## 📊 Summary Logic Table

| Feed Type          | Frequency    | Use Case                                         | Sentiment Weight  |
| :----------------- | :----------- | :----------------------------------------------- | :---------------- |
| **Investing.com**  | < 1 min      | Immediate price volatility triggers              | Short-Term: High  |
| **SEC 8-K Feed**   | Real-time    | Definitive asset-specific shocks (M&A, CEO exit) | Instant: Critical |
| **Yahoo (Ticker)** | 5-10 mins    | General retail buzz and analyst upgrades         | Moderate          |
| **FT / Economist** | Hourly/Daily | Structural macro "narrative" changes             | Long-Term: High   |

---

## ⚖️ Final System Constraints

- **Validation:** Only show news that has been sentiment-graded and is available in the database.
- **Consistency:** Ensure UI counts (Macro vs. Asset vs. Sector) accurately reflect the database state.
- **Queue Management:** Do not include items currently in the fetching or processing queues in the total sentiment scores.
- **Time Intervals:** Maintain distinct short-term and long-term sentiment scoring windows.
