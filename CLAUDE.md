# TradeSignal - Investment Analysis Platform

## Project Overview

A Docker-based, multi-service trading signal and investment analysis platform that provides real-time and daily analysis for stocks, ETFs, and commodities. The system aggregates news sentiment, technical indicators, and live pricing into a unified grading system for investment instruments.

## Architecture

All services run as Docker containers orchestrated via Docker Compose.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Docker Network (Isolated)                    │
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────────┐ │
│  │  Frontend     │  │  Backend API │  │  PostgreSQL               │ │
│  │  React/Vite   │──│  FastAPI     │──│  Data Store               │ │
│  │  :3000        │  │  :8000       │  │  :5432                    │ │
│  └──────────────┘  └──────┬───────┘  └───────────────────────────┘ │
│                           │                       ▲                 │
│         ┌─────────────────┼───────────────────────┤                 │
│         │                 │                       │                 │
│  ┌──────┴───────┐  ┌─────┴────────┐  ┌──────────┴──────────────┐  │
│  │ News Fetcher │  │  Technical   │  │  Live Price / Historical │  │
│  │  Service     │  │  Analysis    │  │  Data Service            │  │
│  │  (Python)    │  │  (Python)    │  │  (Python/yfinance)       │  │
│  └──────┬───────┘  └──────────────┘  └─────────────────────────┘  │
│         │                                                          │
│  ┌──────┴───────┐  ┌──────────────┐                                │
│  │  Sentiment   │  │  Grading     │                                │
│  │  Analysis    │  │  Service     │                                │
│  │  (FinBERT)   │  │  (Python)    │                                │
│  │  :8001       │  │              │                                │
│  └──────────────┘  └──────────────┘                                │
└─────────────────────────────────────────────────────────────────────┘
```

## Services

### 1. PostgreSQL Database Service
- **Port:** 5432 (internal only, not exposed to host in production)
- **Purpose:** Central data store for all services
- **Tables:** instruments, historical_prices, live_prices, news_articles, sentiment_scores, technical_indicators, grades

### 2. News Fetching Service (Python)
- **Purpose:** Continuously fetches and processes RSS feeds for political and financial news
- **Macro/Global Sentiment Sources (rolling 24h window, no historical storage):**
  - USA Politics: NPR, Politico, Fox News, CNN, CBS News, HuffPost
  - UK Politics: BBC, The Guardian, Sky News, The Independent, HuffPost UK
- **Instrument-Specific Financial News (up to 30 days, continuously updated):**
  - USA Finance: CNBC, Yahoo Finance, MarketWatch, Investing.com, CNN Business
  - UK Finance: BBC Business, City A.M., The Guardian Business, This is Money, Sky News Business
- **RSS Feed URLs:**
  - `https://feeds.npr.org/1014/rss.xml`
  - `https://rss.politico.com/politics-news.xml`
  - `https://moxie.foxnews.com/google-publisher/politics.xml`
  - `http://rss.cnn.com/rss/cnn_allpolitics.rss`
  - `https://www.cbsnews.com/latest/rss/politics`
  - `https://www.huffpost.com/section/politics/feed`
  - `http://feeds.bbci.co.uk/news/politics/rss.xml`
  - `https://www.theguardian.com/politics/rss`
  - `https://feeds.skynews.com/feeds/rss/politics.xml`
  - `https://www.independent.co.uk/news/uk/politics/rss`
  - `https://www.huffingtonpost.co.uk/news/politics/feed`
  - `https://search.cnbc.com/rs/search/view.html?partnerId=2000&id=10000664`
  - `https://finance.yahoo.com/news/rss`
  - `http://feeds.marketwatch.com/marketwatch/topstories/`
  - `https://www.investing.com/rss/news.rss`
  - `http://rss.cnn.com/rss/money_latest.rss`
  - `http://feeds.bbci.co.uk/news/business/rss.xml`
  - `https://www.cityam.com/feed/`
  - `https://www.theguardian.com/business/rss`
  - `https://www.thisismoney.co.uk/money/index.rss`
  - `https://feeds.skynews.com/feeds/rss/business.xml`

### 3. Sentiment Analysis Service (Python/FastAPI)
- **Port:** 8001 (internal)
- **Model:** ProsusAI/FinBERT from HuggingFace (`https://huggingface.co/ProsusAI/finbert`)
- **Framework:** FastAPI serving the model with transformers/torch
- **Purpose:** Analyze news text and return sentiment scores (positive/negative/neutral)

### 4. Technical Analysis Service (Python/pandas)
- **Purpose:** Compute technical indicators from historical and current price data
- **Indicators to implement:**
  - **Trend:** SMA, EMA, MACD
  - **Momentum/Oscillators:** RSI, Stochastic Oscillator
  - **Volatility:** Bollinger Bands
  - **Volume:** On-Balance Volume (OBV)
  - **Chart Patterns:** Head and Shoulders, Support/Resistance, Double Tops/Bottoms, Flags, Pennants, Cup and Handle
- **Input:** OHLCV (Open, High, Low, Close, Volume) candlestick data
- **Output:** Indicator values and signal interpretations per instrument

### 5. Live Price Fetching Service (Python/yfinance)
- **Purpose:** Fetch real-time and after-hours pricing data
- **Data Source:** yfinance
- **Tracked Instruments (initial set):**
  - Stocks: RTX, NVDA (NVIDIA), GOOGL (Google), AAPL (Apple)
  - ETFs: IITU
  - Commodities: GC=F (Gold), CL=F (Oil)
- **Behavior:** Continuously updates the database with latest prices; shows after-hours or active status

### 6. Historical Data Fetching Service (Python/yfinance)
- **Purpose:** Fetch and store historical OHLCV data
- **Data Source:** yfinance
- **Same instrument set as Live Price service**
- **CRITICAL RULE:** Once historical data exists in DB, NEVER re-fetch or replace old data. Only accumulate new data from the last stored date forward. Merge new data from the live price service.

### 7. Grading Service (Python)
- **Purpose:** Produce investment grades for each instrument combining:
  - Macro sentiment (from political news sentiment)
  - Instrument-specific news sentiment
  - Technical analysis signals
- **Output:** Long-term and short-term grades per instrument with exact timestamp of when the grade was determined
- **Grade components must be individually visible:** technical score, sentiment score, macro score

### 8. Backend API Service (Python/FastAPI)
- **Port:** 8000
- **Purpose:** Central API serving data to the frontend
- **Serves:** instrument data, grades, news, prices, technical indicators

### 9. Frontend Application
- **Port:** 3000
- **Stack:** React, TypeScript, Tailwind CSS 4+, Vite, Jotai (state), Jotai Query (data fetching)
- **Pages:**
  - **Dashboard:** Top graded and worst graded assets with grade display per instrument
  - **Asset List:** All assets categorized (All / Stocks / ETFs / Commodities) with grade per instrument
  - **Individual Asset Detail:** Relevant news with links, detailed grading scores (technical, sentiment, macro, asset-specific), long-term and short-term grades

---

## Docker Security Rules (MANDATORY)

All containers MUST follow these security hardening rules. Never skip or weaken these for convenience.

### 1. Shrink the Attack Surface (Image Level)

- **Use minimal base images ONLY:** Use `python:3.12-slim` or `python:3.12-alpine` for Python services. Use `node:22-alpine` for the frontend build. Use `gcr.io/distroless/*` for final production stages where possible.
- **NEVER use:** `ubuntu`, `debian`, `:latest` tags, or full OS images.
- **Multi-stage builds:** Always use multi-stage Dockerfiles. Build dependencies go in a builder stage; only runtime artifacts are copied to the final minimal image.
- **Vulnerability scanning:** All images must be scannable with Trivy, Grype, or Docker Scout. Integrate scanning into CI/CD.

### 2. Strip Away Privileges (Runtime Level)

- **Non-root user REQUIRED in every Dockerfile:**
  ```dockerfile
  RUN addgroup -S appgroup && adduser -S appuser -G appgroup
  USER appuser
  ```
- **Read-only filesystem:** Run containers with `--read-only` flag. Mount `tmpfs` to `/tmp` and any other directories that require write access.
- **No privilege escalation:** Always use `--security-opt no-new-privileges:true` on every container.

### 3. Fortify the Kernel Boundary (Host Level)

- **Drop ALL capabilities:** Use `cap_drop: ["ALL"]` in docker-compose. Only add back specific capabilities if absolutely required (document why).
- **User namespaces:** Enable userns-remap in Docker daemon configuration.
- **Seccomp profiles:** Use Docker's default seccomp profile at minimum. Create custom profiles for production.

### 4. Contain Resources and Network Access

- **Resource limits on EVERY container:**
  ```yaml
  deploy:
    resources:
      limits:
        memory: 512M
        cpus: "1.0"
  ```
  Adjust per service (e.g., sentiment/FinBERT may need more memory).
- **Isolated Docker network:** All services communicate on an internal-only Docker network. Only the frontend and backend API expose ports to the host.
- **Restrict egress:** Services that do not need internet access (DB, grading, technical analysis) must be on an isolated network with no external access. Only news fetcher, price fetcher, and sentiment model downloader need outbound access.
- **No `privileged: true`** ever.
- **No `network_mode: host`** ever.

### 5. Secrets Management

- Use Docker secrets or environment files with restricted permissions (never hardcode credentials).
- `.env` files must be in `.gitignore` and never committed.
- Database passwords, API keys, etc. must use Docker secrets in production.

---

## Development Rules

### General
- All code must be linted and formatted before committing
- Python services use: Python 3.12+, FastAPI, SQLAlchemy (async), alembic for migrations
- Use `asyncpg` as the PostgreSQL driver
- Type hints are mandatory in all Python code
- Frontend uses strict TypeScript (no `any` types)

### Docker Compose
- Single `docker-compose.yml` at project root for development
- Separate `docker-compose.prod.yml` for production overrides
- Health checks on every service
- Proper dependency ordering with `depends_on` and health conditions
- Named volumes for PostgreSQL data persistence

### Database
- All schema changes through Alembic migrations
- Never modify the database schema manually
- Use UUID primary keys for all tables
- Timestamps in UTC always

### API Design
- RESTful endpoints
- All responses use consistent JSON envelope: `{ "data": ..., "error": ..., "timestamp": ... }`
- API versioning: `/api/v1/...`

### Frontend
- Component-based architecture
- Jotai atoms for state management
- Jotai Query for server state / data fetching
- Tailwind CSS 4+ for styling (no CSS-in-JS, no external component libraries unless discussed)
- All API calls go through a central API client module

## Project Structure

```
Trade/
├── CLAUDE.md
├── docker-compose.yml
├── docker-compose.prod.yml
├── .env.example
├── .gitignore
├── services/
│   ├── postgres/
│   │   └── init/                    # DB init scripts
│   ├── backend/                     # FastAPI backend API
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── api/
│   │   │   ├── models/
│   │   │   ├── schemas/
│   │   │   ├── services/
│   │   │   └── config.py
│   │   └── alembic/
│   ├── news-fetcher/                # News RSS fetching service
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   ├── sentiment/                   # FinBERT sentiment analysis
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   ├── technical-analysis/          # TA indicators and patterns
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   ├── price-fetcher/               # Live + historical price data
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── app/
│   └── grading/                     # Investment grading engine
│       ├── Dockerfile
│       ├── requirements.txt
│       └── app/
└── frontend/                        # React/TS/Vite frontend
    ├── Dockerfile
    ├── package.json
    ├── tsconfig.json
    ├── vite.config.ts
    ├── tailwind.config.ts
    └── src/
        ├── App.tsx
        ├── main.tsx
        ├── atoms/                   # Jotai atoms
        ├── api/                     # API client
        ├── components/
        ├── pages/
        │   ├── Dashboard.tsx
        │   ├── AssetList.tsx
        │   └── AssetDetail.tsx
        └── types/
```

## Tracked Instruments

| Symbol | Name | Category |
|--------|------|----------|
| RTX | RTX Corporation | Stock |
| NVDA | NVIDIA Corporation | Stock |
| GOOGL | Alphabet Inc. | Stock |
| AAPL | Apple Inc. | Stock |
| IITU | iShares US Technology ETF | ETF |
| GC=F | Gold Futures | Commodity |
| CL=F | Crude Oil Futures | Commodity |

## Key Technical Decisions
- **yfinance** for all market data (live + historical) - free, no API key required
- **FinBERT** for financial sentiment - domain-specific NLP model from HuggingFace
- **Jotai + Jotai Query** over Redux/React Query - lighter, atomic state management
- **Tailwind 4+** - latest version with CSS-first configuration
- **FastAPI** for all Python HTTP services - async, fast, auto-docs
- **pandas** as core data manipulation library for technical analysis
