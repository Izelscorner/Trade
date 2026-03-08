# TradeSignal - Investment Analysis Platform

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
│  │ Qwen 122B    │    │  :8003       │    │  (Python)    │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Services

### 1. PostgreSQL Database Service

- **Port:** 5432
- **Image:** `postgres:16-alpine`
- **Purpose:** Central data store for all services.
- **Key Tables:** `instruments`, `news_articles`, `sentiment_scores`, `news_instrument_map`, `technical_indicators`, `grades`, `macro_sentiment`, `historical_prices`, `live_prices`, `intraday_prices`, `portfolio`, `processing_priority`, `etf_constituents`.
- **Key Flags:** `news_articles.ollama_processed` gates which articles are displayed and graded. `is_macro` and `is_asset_specific` classify article type.
- **Schema Notes:**
  - `grades.overall_grade` is `VARCHAR(20)` — stores action labels like "Strong Buy", "Neutral", "Sell".
  - `grades.overall_score` is `NUMERIC(7,4)` — composite score in [-3, 3].
  - `grades.details` is `JSONB` — contains `buy_confidence`, `action`, group scores, effective weights, confidence metrics, `consensus_adjustment`.
  - `sentiment_scores.label` — short-term sentiment label. `sentiment_scores.long_term_label` — long-term sentiment label.
  - `news_articles.macro_sentiment_label` — short-term macro label. `news_articles.macro_long_term_label` — long-term macro label.
  - `macro_sentiment.term` — `'short'` or `'long'`, separate rolling aggregates per horizon.
  - `etf_constituents` — maps ETF instruments to their underlying holdings with percentage weights (auto-populated via LLM).

### 2. News Fetching Service (Python)

- **Purpose:** Fetches news via RSS feeds and Yahoo/Google per-asset search. Stores articles with `ollama_processed = false` for downstream processing.
- **Feed Categories:**
  - `macro_markets` — Trading Economics, FT Global Economy, WSJ World News, WSJ Markets
  - `macro_politics` — Geopolitical Futures, Foreign Policy, The Diplomat
  - `macro_conflict` — Crisis Group, Al Jazeera Global, War on the Rocks
  - `asset_specific` — Yahoo Finance + Google News per tracked instrument
- **Loops:**
  - `macro_loop()` — every 5 minutes, fetches all 3 macro categories
  - `instruments_loop()` — every 15 minutes, fetches Yahoo/Google per asset
  - `new_asset_news_loop()` — every 2 minutes, fast-tracks instruments with zero articles
  - `cleanup_loop()` — every 15 minutes, removes stale news (30d macro, 90d asset)
- **Deduplication:** `rapidfuzz` (C++ backend) for title/summary similarity checks (90%+ partial ratio).
- **Content Scraping:** Fetches full article content from links with paywall detection and HTML sanitization.

### 3. LLM Processor Service (Python/FastAPI)

- **Port:** 8003
- **Purpose:** Unified AI pipeline. Polls the database for unprocessed articles and runs them through the Qwen 3.5 122B model via the NVIDIA API for classification and context-aware sentiment analysis.
- **Processing Pipeline (per article):**
  1. **Classification + Instrument Tagging** (single LLM call) — Returns `{type: "news"|"spam", instruments: ["AAPL", ...], is_macro: true|false}`. Spam/non-finance articles are deleted.
  2. **Deterministic Post-Processing** — Regex-based rules correct LLM errors:
     - Asset-specific feed articles are NOT macro unless macro patterns match (wars, sanctions, GDP, etc.)
     - Titles with stock/earnings patterns override LLM macro classification
     - Macro patterns (geopolitical conflicts, central banks) force `is_macro = true`
     - Foreign tickers in titles are validated against tracked instruments
     - Over-tagged instruments (>2) are filtered to only directly mentioned names
     - **ETF Constituent Propagation** — News about a constituent (e.g., NVDA) auto-tags the parent ETF (e.g., IITU) with weight-proportional relevance score
  3. **DUAL-HORIZON Sentiment Analysis** (per-instrument LLM call) — Role-based prompting returns BOTH short-term (1-7d) and long-term (1-6mo) sentiment per article: `{short_sentiment, short_confidence, long_sentiment, long_confidence}`.
  4. **DUAL-HORIZON Macro Sentiment Aggregation** — Computes both short-term and long-term macro sentiment and stores separate records in `macro_sentiment` table.
- **ETF Constituent Tracking:** On startup, uses LLM to identify top holdings of each ETF instrument with percentage weights. Stored in `etf_constituents` table. News about constituents auto-propagates to parent ETF with weight-based relevance.
- **API Call & Rate Limiting:** Uses the NVIDIA Hosted API for the `qwen/qwen3.5-122b-a10b` model. A strict global rate limiter (`asyncio.Lock` enforcing a 1.5-second minimum delay between outgoing requests) guarantees the app never exceeds the hard limit of 40 RPM.
- **Batch Processing Strategy:** Batch size 20, temperature 0.0 (deterministic), strict JSON formatting. Articles are grouped into sub-batches (8 for classify, 6 for sentiment, 3 for macro) and sent as single API prompts expecting JSON array responses.
- **Sentiment Labels:** `very_positive`, `positive`, `neutral`, `negative`, `very_negative`.

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
- **Update Frequency:** Full regrade every 60 seconds; incremental on data changes every 10 seconds.
- **Output:** `buy_confidence` in (0, 100), `action` label, composite score in [-3, 3].

#### 6a. Technical Score — Group-Based Weighted Average

The 18 indicators are bucketed into 5 groups. Each group's score is the mean of its member indicator signals. Groups are then combined via category/term-specific weights to eliminate correlation bias (e.g., 4 momentum oscillators don't quadruple-count momentum).

**Group weights (stock short-term example):** Trend 28%, Momentum 30%, Volume 20%, Levels 16%, Volatility 6%.

**ADX modifier:** When ADX < 20 (no trend), Trend group dampened x0.70. When ADX > 40 (strong trend), amplified x1.25.

**ATR risk dampener:** ATR% > 5% -> overall tech score x0.65. ATR% > 3.5% -> x0.80. Reflects that high volatility reduces signal reliability.

**Data completeness:** Fraction of indicator slots with actual data -> used to reduce effective weight of sparse technicals.

#### 6b. Sentiment Score — DUAL-HORIZON, Term-Aware

Sentiment is now **term-aware**: short-term grades use `label` (short-term sentiment), long-term grades use `long_term_label`. Each horizon has tailored parameters reflecting different behavioral dynamics:

| Parameter       | Short-Term (1-7d)     | Long-Term (1-6mo)     |
|----------------|-----------------------|-----------------------|
| Half-life      | 24 hours              | 96 hours (4 days)     |
| Window         | 3 days                | 14 days               |
| Full confidence| 20 non-neutral articles| 40 non-neutral articles|

**Behavioral Science Edge Cases:**
- **Contrarian Dampening (×0.85):** When >80% of non-neutral articles agree on direction, apply dampening. Rationale: herd behavior signals increased mean-reversion risk (Expert Trader + Behavioral Scientist).
- **Priced-In Detection (×0.90):** When consensus is high AND average article age >48h, the signal is likely already priced in (Efficient Market Hypothesis). Combined with contrarian: minimum multiplier 0.70.
- **Mathematical Integrity:** Dampening is multiplicative, only activates at extreme consensus (>3 non-neutral articles), preserves signal linearity in normal range (Expert Mathematician).

Confidence still uses **logarithmic ramp** `log(1+n)/log(1+N)` where N is the full_at parameter per term.

#### 6c. Macro Score — DUAL-HORIZON, Term-Aware

Macro sentiment is now term-aware with separate aggregation windows:

| Parameter  | Short-Term           | Long-Term            |
|-----------|----------------------|----------------------|
| Half-life | 6 hours              | 24 hours             |
| Window    | 12 hours             | 48 hours             |

Short-term macro captures immediate risk-on/off shifts. Long-term macro captures structural policy regime changes. Confidence is log-scaled on total article count.

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
- **WebSocket (`/api/v1/ws/updates`):** Subscription-based real-time updates.
  - Clients send `{"subscribe": {"page": "dashboard"|"asset_detail"|"asset_list"|"portfolio"|"news", "instrument_ids": [...], "category": "..."}}`
  - Server pushes: `live_prices`, `news_updates`, `grade_updates`, `technical_updates`, `macro_sentiment_updates` — filtered per subscription.
  - Navigating to asset_detail auto-writes to `processing_priority` table to fast-track that instrument's news through the LLM pipeline.

### 8. Frontend (React/Vite/Tailwind 4)

- **Port:** 3000 (nginx proxy to backend at :8000)
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

---

## Data Flow

The system operates as a **unidirectional predictive pipeline**:

1. **Ingestion Layer:** `news-fetcher` fetches RSS/search feeds, `price-fetcher` fetches market data. Articles stored with `ollama_processed = false`.
2. **AI Processing Layer:** `llm-processor` polls for unprocessed articles, batch-classifies them (N articles -> 1 NVIDIA API call -> JSON array), applies deterministic post-processing for tagging (including ETF constituent propagation), and batch-scores DUAL-HORIZON sentiment (short-term + long-term) purely via LLM. Articles marked `ollama_processed = true`.
3. **Signal Layer:** `technical-analysis` computes 18 indicators (trend/momentum/volume/levels/volatility) from price data.
4. **Synthesis Layer:** `grading` combines signals via group-based weighted averaging, term-aware time-decayed sentiment (with behavioral science consensus dampening), sigmoid buy-confidence output. Short-term grades use short-term sentiment/macro; long-term grades use long-term sentiment/macro. Result: percentage + action label per instrument per term.
5. **Presentation Layer:** `backend` API serves only processed/scored articles. `frontend` displays buy-confidence percentages, group breakdowns, and macro sentiment.

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
│   │       ├── main.py              # Lifespan, routes, WS background tasks
│   │       ├── api/                 # Endpoint routers (dashboard, grades, news, prices, technical, ai_analysis, portfolio, ws)
│   │       ├── core/db.py           # async SQLAlchemy session
│   │       └── schemas.py           # Pydantic response schemas
│   ├── news-fetcher/                # RSS/search feed ingestion
│   │   └── app/
│   │       ├── main.py              # Async loop orchestration
│   │       ├── fetcher.py           # Feed parsing, content scraping, dedup
│   │       └── feeds.py             # Feed URL definitions
│   ├── llm-processor/               # Unified AI classification + sentiment (NVIDIA API / Qwen 122B)
│   ├── technical-analysis/          # 18-indicator suite
│   │   └── app/
│   │       ├── main.py              # Polling loop
│   │       ├── indicators.py        # All indicator calculations (pandas/numpy)
│   │       └── db.py
│   ├── price-fetcher/               # yfinance live + historical + intraday
│   └── grading/                     # Signal synthesis -> buy-confidence
│       └── app/
│           ├── main.py              # Polling loop with change detection
│           ├── scorer.py            # Mathematical model (group scoring, time-decay, sigmoid)
│           └── db.py
└── frontend/                        # React/Vite/Tailwind UI
    └── src/
        ├── main.tsx
        ├── App.tsx                  # Router
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
            ├── NewsFeed.tsx
            ├── PriceChart.tsx
            ├── PriceChange.tsx
            ├── MacroSentimentCard.tsx
            ├── AIAnalysisModal.tsx
            ├── CategoryFilter.tsx
            ├── Navbar.tsx
            └── Skeletons.tsx
```

## Tracked Instruments

| Symbol | Name                      | Category  | yfinance |
|--------|---------------------------|-----------|----------|
| RTX    | RTX Corporation           | Stock     | RTX      |
| NVDA   | NVIDIA Corporation        | Stock     | NVDA     |
| GOOGL  | Alphabet Inc.             | Stock     | GOOGL    |
| AAPL   | Apple Inc.                | Stock     | AAPL     |
| IITU   | iShares US Technology ETF | ETF       | IITU.L   |
| GOLD   | Gold Futures              | Commodity | GC=F     |
| OIL    | Crude Oil Futures         | Commodity | CL=F     |

New instruments can be added dynamically via `POST /api/v1/instruments`.

## Docker & Security

- **Networks:** `internal` (bridge, internal-only — DB access), `egress` (bridge — internet access for APIs/feeds).
- **All containers:** `read_only: true`, `cap_drop: ALL`, `security_opt: no-new-privileges:true`, resource limits (512M/1CPU typical).
- **Frontend:** nginx, `cap_add: NET_BIND_SERVICE` only, 256M/0.5CPU.
- **Postgres:** `cap_add: CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID`, tmpfs for `/tmp` and `/run/postgresql`.
- **Environment:** All secrets via `.env` file (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, DATABASE_URL, NIM_API_KEY).

## Key Technical Decisions

- **Sigmoid buy-confidence** replaces letter grades — mathematically honest probability representation that asymptotically approaches but never reaches 0% or 100%.
- **Group-based indicator averaging** eliminates correlation bias — 4 momentum oscillators get one collective vote, not 4x weight.
- **ADX/ATR as modifiers** — ADX amplifies/dampens trend signals based on trend strength; ATR reduces overall confidence in high-volatility environments. Neither is scored as a directional signal.
- **DUAL-HORIZON sentiment** — LLM returns both short-term (1-7d) and long-term (1-6mo) sentiment per article. Short-term grades use short-term sentiment, long-term grades use long-term sentiment. Different decay rates per horizon.
- **Behavioral science consensus dampening** — Contrarian signal at >80% agreement (×0.85), priced-in detection for stale consensus (×0.90). Prevents herd behavior from dominating grades.
- **ETF constituent-aware tagging** — News about ETF holdings auto-propagates to parent ETF with weight-proportional relevance, so NVDA news at 23% weight impacts IITU accordingly.
- **Exponential time-decay** — Term-specific: short-term sentiment 24h half-life, long-term 96h; short-term macro 6h, long-term macro 24h.
- **Logarithmic confidence** — diminishing returns on article count, more information-theoretically sound than linear ramp.
- **Confidence-adjusted composite weights** — sub-signals with low data reduce to 50% nominal weight instead of anchoring composite at zero.
- **NVIDIA Hosted API (NIM)** for classification and sentiment — `qwen/qwen3.5-122b-a10b`, temperature 0, batch processing with 1.5s rate limiter for 40 RPM.
- **100% LLM-driven Sentiment** — The LLM is trusted to quantitatively evaluate price impacts directly based on future cash flows and macro environments without hardcoded biases.
- **Deterministic Classification Correction** — Regex rules still override basic model tagging hallucinations (e.g., classifying a company specific article as macro, or making up non-tracked tags).
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
