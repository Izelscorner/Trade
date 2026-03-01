# TradeSignal - Investment Analysis Platform

## Project Overview

A Docker-based, multi-service trading signal and investment analysis platform that provides real-time and daily analysis for stocks, ETFs, and commodities. The system aggregates news sentiment, technical indicators, and live pricing into a unified grading system for investment instruments.

## Architecture

All services run as Docker containers orchestrated via Docker Compose.

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
│  │ Ollama       │◄───│  Ollama      │    │  Grading     │                        │
│  │ (Llama 3.2)  │    │  Processor   │    │  Service     │                        │
│  │ :11434       │    │  :8003       │    │  (Python)    │                        │
│  └──────────────┘    └──────────────┘    └──────────────┘                        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## Services

### 1. PostgreSQL Database Service

- **Port:** 5432
- **Purpose:** Central data store for all services.
- **Key Tables:** instruments, news_articles, sentiment_scores, news_instrument_map, technical_indicators, grades, macro_sentiment, historical_prices, live_prices.
- **Key Flags:** `news_articles.ollama_processed` gates which articles are displayed and graded. `is_macro` and `is_asset_specific` classify article type.

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

### 3. Ollama Service (LLM Runtime)

- **Port:** 11434
- **Image:** `ollama/ollama:latest`
- **Model:** `llama3.2:1b` — pulled automatically on startup
- **Purpose:** Serves the LLM for classification and sentiment analysis via HTTP API.
- **Resources:** 4GB memory limit, 4 CPUs.

### 4. Ollama Processor Service (Python/FastAPI)

- **Port:** 8003
- **Purpose:** Unified AI pipeline that replaces the old Relevance (DistilBERT) and Sentiment (FinBERT) services. Polls the database for unprocessed articles and runs them through Llama 3.2 1B for classification and contextual sentiment analysis.
- **Processing Pipeline (per article):**
  1. **Classification + Instrument Tagging** (single LLM call) — Returns `{type: "news"|"spam", instruments: ["AAPL", ...], is_macro: true|false}`. Spam articles are deleted.
  2. **Deterministic Post-Processing** — Regex-based rules correct LLM errors:
     - Asset-specific feed articles are NOT macro unless macro patterns match (wars, sanctions, GDP, etc.)
     - Titles with stock/earnings patterns override LLM macro classification
     - Macro patterns (geopolitical conflicts, central banks) force `is_macro = true`
     - Foreign tickers in titles are validated against tracked instruments
     - Over-tagged instruments (>2) are filtered to only directly mentioned names
  3. **Contextual Sentiment Analysis** (per-instrument LLM call) — Role-based prompting (e.g., "You are a gold commodity trader") with chain-of-thought reasoning. Returns `{sentiment, confidence}`.
  4. **Macro Sentiment Aggregation** — After each batch, computes average sentiment from all macro articles (last 24h) and stores in `macro_sentiment` table with `region = 'global'`.
- **Configuration:** Batch size 20, process interval 15s, temperature 0.0 (deterministic), JSON format mode enabled.
- **Sentiment Labels:** very_positive, positive, neutral, negative, very_negative — mapped to probability distributions (positive/negative/neutral) for compatibility with the grading system.

### 5. Technical Analysis Service (Python/pandas)

- **Purpose:** Compute trend (SMA/MACD), momentum (RSI), and volatility signals per instrument.

### 6. Live & Historical Price Services (yfinance)

- **Purpose:** Manages market data. Historical data is incremental and never overwritten.

### 7. Grading Service (Python)

- **Purpose:** Aggregates technical, sentiment, and macro signals into investment grades.
- **Update Frequency:** 60 seconds.
- **Grade Scale:** A+ (strong bullish) to F (bearish), mapped from composite score [-1.0, 1.0].
- **Weighting by category and term:**
  - **Stocks:** Short (50% Tech, 30% Sentiment, 20% Macro) / Long (35% Tech, 30% Sentiment, 35% Macro)
  - **ETFs:** Short (45% Tech, 25% Sentiment, 30% Macro) / Long (30% Tech, 25% Sentiment, 45% Macro)
  - **Commodities:** Short (45% Tech, 30% Sentiment, 25% Macro) / Long (30% Tech, 30% Sentiment, 40% Macro)
- **Sentiment Score:** 3-day rolling window, weighted by label. Falls back to macro category news if no instrument-specific sentiment exists.
- **Macro Score:** Latest global macro sentiment from last 4 hours.
- **Only uses articles where `ollama_processed = true`.**

### 8. Backend API (FastAPI)

- **Port:** 8000
- **Key Endpoints:**
  - `GET /api/v1/dashboard` — All instruments with prices, grades
  - `GET /api/v1/dashboard/macro` — Global macro sentiment (single entry, `region = 'global'`)
  - `GET /api/v1/dashboard/macro/news` — Recent macro news with sentiment
  - `GET /api/v1/news` — Filtered news (by category, instrument_id). Only returns Ollama-processed articles with sentiment scores (uses `JOIN sentiment_scores`, not `LEFT JOIN`).
  - `GET /api/v1/ai-analysis/{id}` — Gemini-powered deep analysis using system grades + news context
  - `GET /api/v1/ai-analysis/independent/{id}` — Pure Gemini knowledge-based analysis
- **WebSocket:** Real-time updates for prices, news, grades, macro sentiment.

### 9. Frontend (React/Vite/Tailwind 4)

- **Port:** 3000
- **News Categories:** Markets, Politics, Conflict, Asset (no US/UK split — single global view)
- **Macro Sentiment:** Displayed as single global indicator.
- **State Management:** Jotai + Jotai Query for atomic state.

---

## Data Flow

The system operates as a **unidirectional predictive pipeline**:

1. **Ingestion Layer:** `news-fetcher` fetches RSS/search feeds, `price-fetcher` fetches market data. Articles stored with `ollama_processed = false`.
2. **AI Processing Layer:** `ollama-processor` polls for unprocessed articles, classifies them (spam/news, instruments, macro), runs contextual sentiment analysis via Llama 3.2 1B, and applies deterministic post-processing rules to correct LLM errors. Articles marked `ollama_processed = true`.
3. **Signal Layer:** `technical-analysis` computes trend/momentum/volatility indicators from price data.
4. **Synthesis Layer:** `grading` combines sentiment, technical, and macro signals into weighted investment grades (A+ to F).
5. **Presentation Layer:** `backend` API serves only processed/scored articles. `frontend` displays grades, news, and macro sentiment.

## Project Structure

```
Trade/
├── CLAUDE.md
├── docker-compose.yml
├── services/
│   ├── postgres/                    # Database init scripts
│   ├── backend/                     # FastAPI REST API + Gemini AI analysis
│   ├── news-fetcher/                # RSS/search feed ingestion
│   ├── ollama-processor/            # Unified AI classification + sentiment (Llama 3.2)
│   ├── technical-analysis/          # SMA/MACD/RSI computation
│   ├── price-fetcher/               # yfinance live + historical prices
│   └── grading/                     # Signal aggregation into grades
└── frontend/                        # React/Vite/Tailwind UI
```

## Tracked Instruments

| Symbol | Name                      | Category  |
| ------ | ------------------------- | --------- |
| RTX    | RTX Corporation           | Stock     |
| NVDA   | NVIDIA Corporation        | Stock     |
| GOOGL  | Alphabet Inc.             | Stock     |
| AAPL   | Apple Inc.                | Stock     |
| IITU   | iShares US Technology ETF | ETF       |
| GC=F   | Gold Futures              | Commodity |
| CL=F   | Crude Oil Futures         | Commodity |

## Key Technical Decisions

- **Ollama + Llama 3.2 1B** for classification and sentiment — runs locally, no API keys, deterministic at temperature 0. Combined classification+tagging in one LLM call, contextual sentiment with role-based prompting per instrument.
- **Deterministic post-processing** — Regex rules override LLM classification errors (asset vs macro, instrument tagging validation). Necessary because a 1B model makes frequent classification mistakes.
- **yfinance** for all market data (live + historical) — free, no API key required.
- **Gemini API** for deep AI analysis — used only in backend for on-demand instrument analysis, not for batch processing.
- **Jotai + Jotai Query** over Redux/React Query — lighter, atomic state management.
- **Tailwind 4+** — latest version with CSS-first configuration.
- **FastAPI** for all Python HTTP services — async, fast, auto-docs.
- **pandas** as core data manipulation library for technical analysis.
- **rapidfuzz** for fuzzy deduplication in news fetcher — C++ backend for performance.

## Improvement Roadmap

- [ ] **Shared Models Package:** Move SQLAlchemy models and shared schemas into a local `common` package mounted as a volume to reduce boilerplate.
- [ ] **Message Broker:** Introduce Redis/RabbitMQ for news processing to replace DB polling for better scalability.
- [ ] **Caching:** Add Redis caching layer in the Backend API for high-traffic assets.
- [ ] **Testing:** Implement cross-service integration tests for the grading logic.
