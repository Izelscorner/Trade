# TradeSignal - Investment Analysis Platform

## AI Assistant Directive

When working on this project, Claude must embody four expert personas simultaneously:
1. **Finance Expert** — Deep knowledge of markets, asset classes, derivatives, portfolio theory, and trading mechanics. Understands how news events translate to price movements for specific instruments.
2. **Data Scientist** — Rigorous statistical thinking, signal processing, time-series analysis, confidence intervals, and mathematical modeling. Challenges assumptions with data.
3. **Behavioural Scientist** — Understands cognitive biases (herding, anchoring, recency bias, contrarian signals), market psychology, and how sentiment translates to trading behavior.
4. **Senior Software Architect** — Clean, maintainable, production-grade code. Docker-first, async Python, TypeScript/React best practices. Security-conscious, performance-aware.

Every decision — from prompt engineering to scoring math to UI design — must be evaluated through all four lenses. When these perspectives conflict, document the trade-off explicitly.

## Project Overview

A Docker-based, multi-service trading signal and investment analysis platform that provides real-time and daily analysis for stocks, ETFs, and commodities. The system aggregates news sentiment, technical indicators, and live pricing into a mathematically rigorous buy-confidence scoring system. Output is a sigmoid-scaled percentage (0-100%) per instrument, with actionable labels (Strong Buy to Strong Sell).

## Architecture

All services run as Docker containers orchestrated via Docker Compose. Two isolated networks: `internal` (DB-only access) and `egress` (external API/feed access). All containers are hardened (non-root, read-only fs, cap-drop ALL, resource limits).

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         Docker Network (Isolated)                                │
│                                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────────┐           │
│  │  Frontend     │    │  Backend API │    │  PostgreSQL               │           │
│  │  React/Vite   │────│  FastAPI     │────│  Data Store               │           │
│  │  :3000        │    │  :8000       │    │  :5432                    │           │
│  └──────────────┘    └──────┬───────┘    └───────────────────────────┘           │
│                             │                       ▲                            │
│           ┌─────────────────┼───────────────────────┤                            │
│           │                 │                       │                            │
│  ┌────────┴─────┐    ┌─────┴────────┐    ┌──────────┴──────────────┐             │
│  │ News Fetcher │    │  Technical   │    │  Live Price / Historical │             │
│  │ Service      │    │  Analysis    │    │  Data Service            │             │
│  │ (Python)     │    │  (Python)    │    │  (Python/yfinance)       │             │
│  └──────────────┘    └──────────────┘    └──────────────────────────┘             │
│                                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                        │
│  │ NVIDIA NIM   │◄───│  LLM         │    │  Grading     │                        │
│  │ (Remote API) │    │  Processor   │    │  Service     │                        │
│  │ Qwen 122B    │    │              │    │  (Python)    │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Services

### 1. PostgreSQL Database Service

- **Port:** 5432
- **Image:** `postgres:16-alpine`
- **Purpose:** Central data store for all services.
- **Key Tables:** `instruments`, `news_articles`, `sentiment_scores`, `news_instrument_map`, `technical_indicators`, `grades`, `macro_sentiment`, `historical_prices`, `live_prices`, `intraday_prices`, `portfolio`, `processing_priority`, `etf_constituents`, `news_fetch_history`.
- **Key Flags:** `news_articles.ollama_processed` gates which articles are displayed and graded. `is_macro` and `is_asset_specific` classify article type.
- **Schema Notes:**
  - `grades.overall_grade` is `VARCHAR(20)` — stores action labels like "Strong Buy", "Neutral", "Sell".
  - `grades.overall_score` is `NUMERIC(7,4)` — composite score in [-3, 3].
  - `grades.details` is `JSONB` — contains `buy_confidence`, `action`, group scores, effective weights, confidence metrics, `consensus_adjustment`.
  - `sentiment_scores.label` — short-term sentiment label. `sentiment_scores.long_term_label` — long-term sentiment label.
  - `sentiment_scores.positive/negative/neutral` — probability distribution derived from LLM label.
  - `news_articles.macro_sentiment_label` — short-term macro label. `news_articles.macro_long_term_label` — long-term macro label.
  - `macro_sentiment.term` — `'short'` or `'long'`, separate rolling aggregates per horizon.
  - `etf_constituents` — maps ETF instruments to their underlying holdings with percentage weights (auto-populated via LLM).
  - `news_fetch_history` — URL hash-based dedup table to prevent refetching filtered articles across restarts.
  - `news_instrument_map.relevance_score` — weight-proportional relevance for ETF constituent articles (default 1.0 for direct, 0.02-0.23 for constituent propagation).

### 2. News Fetching Service (Python)

- **Purpose:** Fetches news via RSS feeds and Yahoo/Google per-asset search. Stores articles with `ollama_processed = false` for downstream processing. Title + summary only (no URL scraping).
- **Feed Categories:**
  - `macro_markets` — FT Global Economy, WSJ World News, WSJ Markets, Google News (Global Economy, Macro Economy, Latest, Breaking), Thomson Reuters, Chatham House, NASDAQ Headlines/Trading Halts/System Status
  - `macro_politics` — BBC World News, The Diplomat, Foreign Policy, Geopolitical Futures
  - `macro_conflict` — War on the Rocks
  - `asset_specific` — Yahoo Finance + Google News per tracked instrument (built dynamically)
  - **Slow feeds** (separate loop): MarketWatch Top Stories
- **Loops (5 concurrent):**
  - `main_loop()` — every 2 minutes (`MAIN_INTERVAL=120`), fetches all macro feeds + all instrument-specific feeds concurrently (semaphore=5). Checks for prioritized instruments first. For prioritized ETFs, also fetches news for all tracked constituent instruments. Refreshes instrument list every 10 cycles (~20 min).
  - `slow_loop()` — every 3 minutes (`SLOW_INTERVAL=180`), high-volume feeds (MarketWatch).
  - `new_asset_news_loop()` — every 2 minutes, fast-tracks instruments with zero articles.
  - `etf_constituents_loop()` — every 10 minutes (`ETF_CONSTITUENT_INTERVAL=600`), fetches news for untracked ETF constituents (e.g., MSFT, AVGO, CRM) and maps articles to parent ETF.
  - `cleanup_loop()` — every 15 minutes, removes stale news (180d macro, 30d asset).
- **Deduplication:** MD5 URL hashing (in-memory cache + `news_fetch_history` table for persistence across restarts) + `rapidfuzz` (C++ backend) fuzzy title matching (85%+ ratio, 90%+ partial ratio for titles >30 chars). Publisher suffixes stripped before comparison.

### 3. LLM Processor Service (Python)

- **Purpose:** Unified AI pipeline. Polls the database every 3 seconds (`PROCESS_INTERVAL=3`) for unprocessed articles and runs them through the Qwen 3.5 122B model via the NVIDIA API for classification and context-aware sentiment analysis.
- **Processing Pipeline (per batch of up to 30 articles):**
  1. **Pre-filter** — `is_low_quality_article()` deterministically removes articles with titles <15 chars, combined text <40 chars, title=summary aggregator filler, or SEO patterns. Saves API calls.
  2. **Batch Classification + Instrument Tagging** (12 articles per API call) — Returns `{type: "news"|"spam", instruments: ["AAPL", ...], is_macro: true|false}`. Spam/non-finance/foreign-domestic-politics articles are deleted.
  3. **Deterministic Post-Processing** — Regex-based rules correct LLM errors:
     - **Macro feed articles**: trusts LLM `is_macro=false` if content matches asset-specific patterns AND no macro patterns found. Forces `is_macro=true` if macro patterns present. Otherwise trusts LLM judgment (no longer blindly forces all macro feed articles to macro).
     - Asset-specific feed articles are NOT macro unless macro patterns match (wars, sanctions, GDP, central banks, etc.)
     - Titles with stock/earnings/analyst patterns override LLM macro classification
     - Direct keyword/ticker mentions in text add instruments the LLM missed
     - **Commodity keyword expansion** — OIL keywords include: oil price/futures/market, crude oil, brent, wti, barrel, opec, strait of hormuz, fuel depots, energy crisis, petrol. GOLD keywords include: gold price/futures/market, bullion, safe haven, flight to safety.
     - **ETF Constituent Propagation** — News about a constituent (e.g., NVDA) auto-tags the parent ETF (e.g., IITU) with weight-proportional relevance score
  4. **DUAL-HORIZON Sentiment Analysis** (8 articles per API call per instrument) — Category-specific role prompting returns BOTH short-term (1-7d) and long-term (1-6mo) sentiment per article: `{short_sentiment, short_confidence, long_sentiment, long_confidence}`.
     - **Explicit rules prevent common errors**: analyst ratings (overweight/buy/upgrade) always positive; institutional trades (small fund buy/sell) neutral with low confidence; commodity supply disruptions positive for commodity PRICE (not confused with bad-for-economy); safe haven assets (GOLD) positive during crises; defense stocks (RTX) positive during wars.
  5. **DUAL-HORIZON Macro Sentiment Aggregation** (5 articles per API call) — Computes both short-term and long-term S&P 500 impact. Company-specific articles assigned neutral with low confidence.
- **ETF Constituent Tracking:** On startup, uses LLM to identify top holdings of each ETF instrument with percentage weights. Stored in `etf_constituents` table. Hardcoded fallback for IITU. News about constituents auto-propagates to parent ETF with weight-based relevance.
- **Sentiment Labels:** `very_positive`, `positive`, `neutral`, `negative`, `very_negative`.
- **Sentiment-to-Probability Map:** `very_positive` → {pos: 0.90, neg: 0.02, neu: 0.08}, `positive` → {0.70, 0.05, 0.25}, `neutral` → {0.15, 0.15, 0.70}, `negative` → {0.05, 0.70, 0.25}, `very_negative` → {0.02, 0.90, 0.08}.
- **API Call & Rate Limiting:** NVIDIA Hosted API `qwen/qwen3.5-122b-a10b`, temperature 0.0 (deterministic). `asyncio.Lock` enforcing 2.0-second minimum delay between requests (`RATE_LIMIT_DELAY=2.0`). Concurrency limit 1 (`CONCURRENCY_LIMIT=1`). Token budgets: 100/article classify, 70/article sentiment, 80/article macro, +150 overhead. `INTER_CHUNK_DELAY=1.0s` between sub-batch chunks.
- **Category-Specific Roles:**
  - Stock: "Wall Street equity analyst covering {name}. Day-trader + fundamental investor."
  - ETF: "Wall Street ETF analyst covering {name}. Constituent-level impacts propagate with weight-proportional magnitude."
  - Commodity: "Commodity futures trader. Supply disruptions = PRICE UP = positive. Demand destruction = PRICE DOWN = negative."

### 4. Technical Analysis Service (Python/pandas)

- **Purpose:** Compute 18 institutional-grade technical indicators from historical OHLCV data.
- **Indicator Suite (grouped by function):**
  - **Trend (6):** SMA_50, SMA_200, EMA_20, EMA_CROSS (Golden/Death Cross), MACD, ICHIMOKU
  - **Momentum (4):** RSI (Wilder's smoothing), STOCHASTIC, WILLIAMS_R, CCI
  - **Volume (3):** OBV, VWAP (20-day rolling), MFI (volume-weighted RSI)
  - **Levels (2):** SUPPORT_RESISTANCE (20-day range), FIBONACCI (60-day retracement)
  - **Volatility (1):** BOLLINGER Bands
  - **Modifiers (2, not scored directly):** ADX (trend strength multiplier), ATR (volatility risk dampener)
- **Signal Types:** `strong_buy`, `buy`, `neutral`, `sell`, `strong_sell`

### 5. Live & Historical Price Services (yfinance)

- **Purpose:** Manages market data. Historical data is incremental and never overwritten.
- **Live Price Fetch Interval:** 60 seconds with dedup guard.
- **Intraday:** 5-minute candles for 1-day charts.

### 6. Grading Service (Python) — Mathematical Model

- **Purpose:** Synthesizes all signals into a buy-confidence percentage (0-100%) with actionable recommendation labels.
- **Update Frequency:** Full regrade every 60 seconds; incremental on data changes every 10 seconds. Concurrent grading of all instruments via `asyncio.gather`.
- **Change Detection:** Polls for new sentiment scores, live prices, technical indicators, and macro sentiment since last check. Macro changes trigger regrade of ALL instruments.
- **Priority:** `processing_priority` table ensures prioritized instruments are graded first.
- **Output:** `buy_confidence` in (0, 100), `action` label, composite score in [-3, 3].

#### 6a. Technical Score — Group-Based Weighted Average

The 18 indicators are bucketed into 5 groups. Each group's score is the mean of its member indicator signals. Groups are then combined via category/term-specific weights to eliminate correlation bias (e.g., 4 momentum oscillators don't quadruple-count momentum).

**Group weights by category/term:**

| Category  | Term  | Trend | Momentum | Volume | Levels | Volatility |
|-----------|-------|-------|----------|--------|--------|------------|
| Stock     | Short | 28%   | 30%      | 20%    | 16%    | 6%         |
| Stock     | Long  | 38%   | 20%      | 18%    | 18%    | 6%         |
| ETF       | Short | 30%   | 25%      | 22%    | 17%    | 6%         |
| ETF       | Long  | 42%   | 18%      | 18%    | 16%    | 6%         |
| Commodity | Short | 25%   | 28%      | 20%    | 20%    | 7%         |
| Commodity | Long  | 35%   | 20%      | 18%    | 20%    | 7%         |

**ADX modifier:** When ADX < 20 (no trend), Trend group dampened x0.70. When ADX > 40 (strong trend), amplified x1.25.

**ATR risk dampener:** ATR% > 5% → x0.65. ATR% > 3.5% → x0.80. ATR% > 2.5% → x0.92. Reflects that high volatility reduces signal reliability.

**Data completeness:** Fraction of indicator slots with actual data → used to reduce effective weight via group-level re-normalization.

#### 6b. Sentiment Score — DUAL-HORIZON, Term-Aware

Sentiment is **term-aware**: short-term grades use `label` (short-term sentiment), long-term grades use `long_term_label`. Each horizon has tailored parameters reflecting different behavioral dynamics:

| Parameter       | Short-Term (1-7d)      | Long-Term (1-6mo)      |
|----------------|------------------------|------------------------|
| Half-life      | 12 hours               | 168 hours (7 days)     |
| Window         | 2 days                 | 30 days                |
| Full confidence| 20 non-neutral articles | 40 non-neutral articles |

**Relevance weighting:** ETF constituent articles have `relevance_score` proportional to holding weight (e.g., NVDA at 23.1% → 0.231). Direct ETF news = 1.0. Relevance multiplies the decay weight.

**Behavioral Science Edge Cases:**
- **Contrarian Dampening (×0.85):** When >80% of non-neutral articles agree on direction, apply dampening. Rationale: herd behavior signals increased mean-reversion risk. Requires ≥3 non-neutral articles.
- **Priced-In Detection (×0.90):** When consensus is >75% AND average article age >48h, the signal is likely already priced in (Efficient Market Hypothesis). Combined with contrarian: minimum multiplier 0.70.

Confidence uses **logarithmic ramp** `log(1+n)/log(1+N)` where N is the `full_confidence_at` parameter per term. Effective count is `non_neutral_weighted_count * 2` (capped at `full_at * 2`).

#### 6c. Macro Score — DUAL-HORIZON, Term-Aware

Macro sentiment is term-aware with separate aggregation windows:

| Parameter  | Short-Term           | Long-Term            |
|-----------|----------------------|----------------------|
| Half-life | 24 hours             | 648 hours (27 days)  |
| Window    | 72 hours (3 days)    | 4320 hours (180 days)|

Short-term macro captures immediate risk-on/off shifts. Long-term macro captures structural policy regime changes over 6 months. Confidence is log-scaled on total article count (`full_at=10`, capped at 30).

#### 6d. Composite Weighting — Confidence-Adjusted

Nominal weights per category and term:

| Category  | Term  | Technical | Sentiment | Macro |
|-----------|-------|-----------|-----------|-------|
| Stock     | Short | 50%       | 30%       | 20%   |
| Stock     | Long  | 35%       | 30%       | 35%   |
| ETF       | Short | 45%       | 25%       | 30%   |
| ETF       | Long  | 30%       | 25%       | 45%   |
| Commodity | Short | 45%       | 30%       | 25%   |
| Commodity | Long  | 30%       | 30%       | 40%   |

**Confidence adjustment:** Each sub-signal's effective weight is `nominal_weight * (0.5 + 0.5 * confidence)`. When sentiment has zero articles (confidence=0), its weight drops to 50% of nominal instead of anchoring the composite at zero. Weights are then renormalized.

#### 6e. Sigmoid Buy-Confidence

The composite score in [-3, 3] is mapped to buy-confidence in (0, 100) via sigmoid:

```
buy_confidence = 100 / (1 + e^(-1.5 * score))
```

| Score | Buy Confidence | Action      |
|-------|---------------|-------------|
| +3.0  | 95%           | Strong Buy  |
| +1.5  | 82%           | Strong Buy  |
| +0.7  | 65%           | Buy         |
| 0.0   | 50%           | Neutral     |
| -0.7  | 35%           | Slight Sell |
| -1.5  | 18%           | Sell        |
| -3.0  | 5%            | Strong Sell |

**Action thresholds:** >=78% Strong Buy, >=63% Buy, >=54% Slight Buy, >=46% Neutral, >=37% Slight Sell, >=22% Sell, <22% Strong Sell.

### 7. Backend API (FastAPI)

- **Port:** 8000
- **Key Endpoints:**
  - `GET /api/v1/dashboard` — All instruments with prices, grades (includes `short_term_score`, `long_term_score`)
  - `GET /api/v1/dashboard/macro` — Global macro sentiment
  - `GET /api/v1/dashboard/macro/news` — Recent macro news with sentiment
  - `GET /api/v1/news` — Filtered news (by category, instrument_id). Only returns `ollama_processed=true` articles with sentiment scores (`JOIN`, not `LEFT JOIN`).
  - `GET /api/v1/grades?instrument_id=&term=` — Latest grades with details JSON
  - `GET /api/v1/grades/history/{id}?term=&limit=` — Grade history
  - `GET /api/v1/ai-analysis/{id}` — On-demand deep analysis using system grades + news context
  - `GET /api/v1/ai-analysis/independent/{id}` — Pure LLM knowledge-based analysis
  - `GET /api/v1/config` — Returns NIM model name
  - `POST /api/v1/instruments` — Add new instruments dynamically
  - `GET/POST/DELETE /api/v1/portfolio` — User portfolio/watchlist management
  - `POST /api/v1/news/prioritize/{id}` — Priority-process an instrument's unprocessed news
  - `GET /health` — Health check
- **WebSocket (`/api/v1/ws/updates`):** Subscription-based real-time updates.
  - Clients send `{"subscribe": {"page": "dashboard"|"asset_detail"|"asset_list"|"portfolio"|"news", "instrument_ids": [...], "category": "..."}}`
  - Server pushes: `live_prices`, `news_updates`, `grade_updates`, `technical_updates`, `macro_sentiment_updates` — filtered per subscription.
  - Navigating to asset_detail auto-writes to `processing_priority` table to fast-track that instrument's news through the LLM pipeline.
- **Background Tasks (lifespan):** 5 broadcast tasks run concurrently — live prices, latest news, latest grades, technical indicators, macro sentiment.

### 8. Frontend (React/Vite/Tailwind 4)

- **Port:** 3000 (Vite dev server, proxied to backend at :8000)
- **Routes:** `/` Dashboard, `/assets` AssetList, `/portfolio` Portfolio, `/news` News, `/asset/:id` AssetDetail.
- **Buy-Confidence Display:** Primary metric is a percentage (0-100%) with sigmoid-derived action labels. Radial SVG gauge on asset detail, percentage pills on dashboard cards.
- **Technical Panel:** Indicators grouped by category (Trend, Momentum, Volume, Levels, Volatility, Modifiers) with per-group average scores. ADX/ATR called out as modifiers, not directly scored.
- **Grade Detail:** Center-origin score bars (red/green from midpoint), confidence metrics per sub-signal, effective weight display, ATR risk factor annotation, consensus dampening indicator (amber warning when herd behavior detected), technical group breakdown grid. Sentiment bars show term-specific decay rates.
- **News Feed:** Dual-horizon sentiment badges per article — shows short-term (ST) and long-term (LT) labels when they differ, highlighting divergent impacts.
- **News Categories:** Markets, Politics, Conflict, Asset (single global view).
- **Macro Sentiment:** Displayed as dual-horizon (short-term + long-term) global indicator.
- **State Management:** Jotai + Jotai Query for atomic state. `@tanstack/react-query` for some data.
- **Key Components:**
  - `GradeBadge` — Percentage pill + action label, continuous color gradient (emerald to amber to red).
  - `GradeDetail` — SVG confidence gauge, sub-score bars with consensus dampening indicator, group breakdown, effective weights.
  - `InstrumentCard` — Confidence pills for short/long term, composite confidence bar.
  - `TechnicalPanel` — Grouped indicator list with group score summary row.
  - `AIAnalysisModal` — LLM-powered analysis (system-integrated and independent modes).
  - `MacroSentimentCard` — Dual-horizon global macro sentiment display (short-term + long-term rows).
  - `PriceChart` — Historical OHLCV chart with day selector (1D/5D/1M/3M/1Y).
  - `NewsFeed` — Article list with dual-horizon sentiment badges.
  - `SignalBadge` — buy/sell/neutral signal pill.
  - `PriceChange` — Price delta display.
  - `CategoryFilter` — News category filter tabs.
  - `Navbar` — Navigation bar.
  - `Skeletons` — Loading skeleton components.

---

## Data Flow

The system operates as a **unidirectional predictive pipeline**:

1. **Ingestion Layer:** `news-fetcher` fetches RSS/search feeds (5 concurrent loops), `price-fetcher` fetches market data. Articles stored with `ollama_processed = false`. URL hashes cached in-memory and persisted to `news_fetch_history` for dedup across restarts. Fuzzy title matching via rapidfuzz prevents cross-source duplicates.
2. **AI Processing Layer:** `llm-processor` polls every 3s for unprocessed articles (batch of 30). Pre-filters low-quality articles deterministically. Batch-classifies (12/call), applies deterministic post-processing for tagging (including commodity keyword expansion and ETF constituent propagation), and batch-scores DUAL-HORIZON sentiment (8/call per instrument) with explicit rules for analyst ratings, institutional trades, commodity prices, safe havens, and defense stocks. Articles marked `ollama_processed = true`.
3. **Signal Layer:** `technical-analysis` computes 18 indicators (trend/momentum/volume/levels/volatility) from price data.
4. **Synthesis Layer:** `grading` combines signals via group-based weighted averaging with category/term-specific profiles, term-aware time-decayed sentiment (with behavioral science consensus dampening), confidence-adjusted composite weights, sigmoid buy-confidence output. Short-term grades use short-term sentiment/macro; long-term grades use long-term sentiment/macro. Change detection triggers incremental regrading every 10s; full regrade every 60s.
5. **Presentation Layer:** `backend` API serves only processed/scored articles via REST + WebSocket. `frontend` displays buy-confidence percentages, group breakdowns, and macro sentiment with real-time updates.

## Project Structure

```
Trade/
├── CLAUDE.md
├── docker-compose.yml
├── .env                             # POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, DATABASE_URL, NIM_API_KEY
├── services/
│   ├── postgres/init/01_init.sql    # Full DB schema + seed instruments
│   ├── backend/                     # FastAPI REST API + WebSocket + AI analysis
│   │   └── app/
│   │       ├── main.py              # Lifespan, routes, WS background tasks, extra table migration
│   │       ├── api/                 # Endpoint routers (dashboard, grades, news, prices, technical, ai_analysis, portfolio, instruments, ws)
│   │       ├── core/db.py           # async SQLAlchemy session
│   │       └── schemas.py           # Pydantic response schemas
│   ├── news-fetcher/                # RSS/search feed ingestion
│   │   └── app/
│   │       ├── main.py              # 5 async loops (main, slow, new_asset, etf_constituents, cleanup)
│   │       ├── fetcher.py           # Feed parsing (title+summary only, no URL scraping)
│   │       ├── feeds.py             # Feed URL definitions (MAIN_FEEDS, SLOW_FEEDS, MACRO_CATEGORIES)
│   │       ├── store.py             # Article persistence, MD5 hash dedup, fuzzy dedup, cleanup
│   │       ├── instruments.py       # Instrument DB queries
│   │       └── db.py
│   ├── llm-processor/               # Unified AI classification + sentiment (NVIDIA API / Qwen 122B)
│   │   └── app/
│   │       ├── main.py              # Polling loop
│   │       ├── processor.py         # Batch processing, pre-filter, post-processing rules, ETF constituents
│   │       ├── prompts.py           # All LLM prompt templates (classify, sentiment, macro, ETF constituent)
│   │       ├── nim_client.py        # AsyncOpenAI client for NVIDIA NIM API with rate limiting
│   │       └── db.py
│   ├── technical-analysis/          # 18-indicator suite
│   │   └── app/
│   │       ├── main.py              # Polling loop
│   │       ├── indicators.py        # All indicator calculations (pandas/numpy)
│   │       └── db.py
│   ├── price-fetcher/               # yfinance live + historical + intraday
│   └── grading/                     # Signal synthesis -> buy-confidence
│       └── app/
│           ├── main.py              # Polling loop with change detection + priority support
│           ├── scorer.py            # Mathematical model (group scoring, time-decay, consensus dampening, sigmoid)
│           └── db.py
└── frontend/                        # React/Vite/Tailwind UI
    └── src/
        ├── main.tsx
        ├── App.tsx                  # Router (/, /assets, /portfolio, /news, /asset/:id)
        ├── types/index.ts           # TypeScript types + scoreToBuyConfidence() + buyConfidenceToAction()
        ├── api/client.ts            # Centralized fetch/post/delete wrappers
        ├── ws.ts                    # WebSocket subscription manager
        ├── atoms/index.ts           # Jotai atoms + jotai-query atoms
        ├── hooks/usePortfolio.ts
        ├── pages/
        │   ├── Dashboard.tsx
        │   ├── AssetList.tsx
        │   ├── AssetDetail.tsx      # Main instrument page
        │   ├── News.tsx
        │   └── Portfolio.tsx
        └── components/
            ├── GradeBadge.tsx        # Percentage pill + action label
            ├── GradeDetail.tsx       # SVG gauge, sub-score bars, group breakdown
            ├── InstrumentCard.tsx    # Dashboard card with confidence badges
            ├── TechnicalPanel.tsx    # Grouped indicator display
            ├── SignalBadge.tsx       # buy/sell/neutral signal pill
            ├── NewsFeed.tsx          # Article list with dual-horizon sentiment badges
            ├── PriceChart.tsx        # OHLCV chart with day selector
            ├── PriceChange.tsx       # Price delta display
            ├── MacroSentimentCard.tsx # Dual-horizon macro sentiment
            ├── AIAnalysisModal.tsx   # LLM-powered analysis (system + independent)
            ├── CategoryFilter.tsx   # News category filter tabs
            ├── Navbar.tsx
            └── Skeletons.tsx        # Loading skeleton components
```

## Tracked Instruments

| Symbol | Name                       | Category  | yfinance |
|--------|----------------------------|-----------|----------|
| RTX    | RTX Corporation            | Stock     | RTX      |
| NVDA   | NVIDIA Corporation         | Stock     | NVDA     |
| GOOGL  | Alphabet Inc.              | Stock     | GOOGL    |
| AAPL   | Apple Inc.                 | Stock     | AAPL     |
| TSLA   | Tesla, Inc.                | Stock     | TSLA     |
| PLTR   | Palantir Technologies Inc. | Stock     | PLTR     |
| LLY    | Eli Lilly and Company      | Stock     | LLY      |
| NVO    | Novo Nordisk A/S           | Stock     | NVO      |
| WMT    | Walmart Inc.               | Stock     | WMT      |
| XOM    | Exxon Mobil Corporation    | Stock     | XOM      |
| IITU   | iShares US Technology ETF  | ETF       | IITU.L   |
| SMH    | VanEck Semiconductor ETF   | ETF       | SMH      |
| VOO    | Vanguard S&P 500 ETF       | ETF       | VOO      |
| GOLD   | Gold Futures               | Commodity | GC=F     |
| OIL    | Crude Oil Futures          | Commodity | CL=F     |

New instruments can be added dynamically via `POST /api/v1/instruments`.

## Docker & Security

- **Networks:** `internal` (bridge, internal-only — DB access), `egress` (bridge — internet access for APIs/feeds).
- **All containers:** `read_only: true`, `cap_drop: ALL`, `security_opt: no-new-privileges:true`, resource limits (512M/1CPU typical, pids limit 100).
- **Frontend:** Vite dev server (hot-reload via volume mount `./frontend:/app`), 1G memory limit, `read_only: false`.
- **Postgres:** `cap_add: CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID`, tmpfs for `/tmp` and `/run/postgresql`.
- **Price-fetcher:** `HOME=/tmp`, `XDG_CACHE_HOME=/tmp` (yfinance cache in writable tmpfs).
- **Environment:** All secrets via `.env` file (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, DATABASE_URL, NIM_API_KEY).

## Key Technical Decisions

- **Sigmoid buy-confidence** replaces letter grades — mathematically honest probability representation that asymptotically approaches but never reaches 0% or 100%.
- **Group-based indicator averaging** eliminates correlation bias — 4 momentum oscillators get one collective vote, not 4x weight.
- **ADX/ATR as modifiers** — ADX amplifies/dampens trend signals based on trend strength; ATR reduces overall confidence in high-volatility environments. Neither is scored as a directional signal.
- **DUAL-HORIZON sentiment** — LLM returns both short-term (1-7d) and long-term (1-6mo) sentiment per article. Short-term grades use short-term sentiment, long-term grades use long-term sentiment. Different decay rates per horizon.
- **Behavioral science consensus dampening** — Contrarian signal at >80% agreement (×0.85), priced-in detection for stale consensus >48h (×0.90). Prevents herd behavior from dominating grades.
- **ETF constituent-aware tagging** — News about ETF holdings auto-propagates to parent ETF with weight-proportional relevance, so NVDA news at 23% weight impacts IITU accordingly. Untracked constituents (MSFT, AVGO, CRM, etc.) have their own dedicated fetch loop.
- **Exponential time-decay** — Term-specific: short-term sentiment 12h half-life, long-term 168h (7d); short-term macro 24h, long-term macro 648h (27d).
- **Logarithmic confidence** — diminishing returns on article count, more information-theoretically sound than linear ramp.
- **Confidence-adjusted composite weights** — sub-signals with low data reduce to 50% nominal weight instead of anchoring composite at zero.
- **NVIDIA Hosted API (NIM)** for classification and sentiment — `qwen/qwen3.5-122b-a10b`, temperature 0, batch processing with 2.0s rate limiter.
- **100% LLM-driven Sentiment with explicit guardrails** — The LLM evaluates price impacts, but prompts include numbered rules to prevent common errors: analyst rating inversion, commodity price confusion (bad-for-economy ≠ bad-for-commodity-price), institutional trade noise, safe-haven logic.
- **Deterministic Classification Correction** — Regex rules correct LLM tagging errors. Macro feed articles are no longer blindly forced to macro — LLM judgment trusted when content is clearly company-specific. Asset-specific feed articles stripped of macro flag unless macro patterns match.
- **Multi-layer dedup** — MD5 URL hashing (in-memory + persistent DB table) + rapidfuzz fuzzy title matching + publisher suffix stripping. Prevents the same story from multiple sources from polluting sentiment.
- **yfinance** for all market data (live + historical) — free, no API key required.
- **Jotai + Jotai Query** + `@tanstack/react-query` for state management.
- **Tailwind 4+** with CSS-first configuration.
- **FastAPI** for all Python HTTP services — async, fast, auto-docs.
- **pandas/numpy** for technical analysis indicator calculations.
- **rapidfuzz** for fuzzy deduplication in news fetcher — C++ backend for performance.

## Improvement Roadmap

- [ ] **Shared Models Package:** Move SQLAlchemy models and shared schemas into a local `common` package mounted as a volume to reduce boilerplate.
- [ ] **Message Broker:** Introduce Redis/RabbitMQ for news processing to replace DB polling for better scalability.
- [ ] **Caching:** Add Redis caching layer in the Backend API for high-traffic assets.
- [ ] **Testing:** Implement cross-service integration tests for the grading logic.
- [ ] **Divergence Detection:** OBV/price divergence detection (price up + OBV down = bearish divergence) as a separate signal.
- [ ] **Regime Detection:** Volatility regime detection (low-vol to breakout prediction) using ATR trends.
- [ ] **Grade History Charting:** Time-series chart of buy-confidence % over days/weeks on asset detail page.
