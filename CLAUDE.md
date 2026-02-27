# TradeSignal - Investment Analysis Platform

## Project Overview

A Docker-based, multi-service trading signal and investment analysis platform that provides real-time and daily analysis for stocks, ETFs, and commodities. The system aggregates news sentiment, technical indicators, and live pricing into a unified grading system for investment instruments.

## Architecture

All services run as Docker containers orchestrated via Docker Compose.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Docker Network (Isolated)                                │
│                                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌───────────────────────────┐          │
│  │  Frontend     │    │  Backend API │    │  PostgreSQL               │          │
│  │  React/Vite   │────│  FastAPI     │────│  Data Store               │          │
│  │  :3000        │    │  :8000       │    │  :5432                    │          │
│  └──────────────┘    └──────┬───────┘    └───────────────────────────┘          │
│                             │                       ▲                           │
│           ┌─────────────────┼───────────────────────┤                           │
│           │                 │                       │                           │
│  ┌────────┴─────┐    ┌─────┴────────┐    ┌──────────┴──────────────┐            │
│  │ News Fetcher │──┐ │  Technical   │    │  Live Price / Historical │            │
│  │ Service      │  │ │  Analysis    │    │  Data Service            │            │
│  │ (Python)     │  │ │  (Python)    │    │  (Python/yfinance)       │            │
│  └────────┬─────┘  │ └──────────────┘    └──────────────────────────┘            │
│           │        │                                                            │
│  ┌────────▼─────┐  │ ┌──────────────┐    ┌──────────────┐                       │
│  │ Relevance    │◄─┘ │  Sentiment   │    │  Grading     │                       │
│  │ AI Service   │    │  Analysis    │    │  Service     │                       │
│  │ (Zero-Shot)  │    │  (FinBERT)   │    │  (Python)    │                       │
│  │ :8002        │    │  :8001       │    │              │                       │
│  └──────────────┘    └──────────────┘    └──────────────┘                       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Services

### 1. PostgreSQL Database Service

- **Port:** 5432
- **Purpose:** Central data store for all services.
- **Tables:** instruments, historical_prices, live_prices, news_articles, sentiment_scores, technical_indicators, grades, macro_sentiment.

### 2. News Fetching Service (Python)

- **Purpose:** Fetches news via RSS and performs initial validation.
- **Improved Logic:**
  - **Relevance Gate:** Before storage, all articles are sent to the AI Relevance service.
  - **Fuzzy Deduplication:** Uses `rapidfuzz` (C++ backend) for high-performance title/summary similarity checks (90%+ partial ratio).
  - **Keyword Filtering:** Aggressive anti-lifestyle/viral filtering + finance/politics keyword enforcement.
  - **Targeted Search:** Google News queries use full asset names for higher relevance.

### 3. Relevance AI Service (Python/FastAPI) - NEW

- **Port:** 8002
- **Model:** `typeform/distilbert-base-uncased-mnli` (Zero-Shot Classification).
- **Purpose:** Context-aware filtering of news.
- **Categories:**
  - **Macro/Finance:** Blocks lifestyle, entertainment, and general noise.
  - **Asset-Specific:** Verifies the news is actually about the company or its direct industry.

### 4. Sentiment Analysis Service (Python/FastAPI)

- **Port:** 8001
- **Model:** ProsusAI/FinBERT.
- **Real-Time Optimization:** Continuous backlog drainage (1s sleep on full batches) to ensure real-time news scoring.

### 5. Technical Analysis Service (Python/pandas)

- **Purpose:** Compute trend (SMA/MACD), momentum (RSI), and volatility signals.

### 6. Live & Historical Price Services (yfinance)

- **Purpose:** Manages market data. Historical data is incremental and never overwritten.

### 7. Grading Service (Python)

- **Purpose:** Aggregates all signals into weights.
- **Update Frequency:** 60 seconds (Real-time tracking of market shifts).
- **Weighting:**
  - **Short-Term (5d):** 50% Technicals, 30% Sentiment, 20% Macro.
  - **Long-Term (30d):** 35% Technicals, 30% Sentiment, 35% Macro.

---

## Deep Analysis & Project Structure

### Data Flow Analysis

The system operates as a **unidirectional predictive pipeline**:

1. **Ingestion Layer:** `news-fetcher` and `price-fetcher` bring raw data into the system.
2. **Quality Layer:** `relevance` AI filters out garbage news before it even hits the permanent DB. `rapidfuzz` prevents data pollution from duplicate reports.
3. **Inference Layer:** `sentiment` and `technical-analysis` transform raw text/prices into normalized scores (-1.0 to 1.0).
4. **Synthesis Layer:** `grading` combines these normalized vectors into actionable investment signals (A+ to F).
5. **Presentation Layer:** `backend` API and `frontend` React app visualize the intelligence.

### Strengths

- **Service Isolation:** Crashing one service (e.g., Sentiment) doesn't stop pricing or news ingestion.
- **AI-Native Filtering:** Solving the "garbage in, garbage out" problem with zero-shot classification.
- **Performance-Balanced:** Using `rapidfuzz` for string matching and `FinBERT` for sentiment ensures high throughput even on limited hardware.

### Improvement Roadmap

- [ ] **Shared Models Package:** Move SQLAlchemy models and shared schemas into a local `common` package mounted as a volume to reduce boilerplate.
- [ ] **Message Broker:** Introduce Redis/RabbitMQ for news processing to replace DB polling for better scalability.
- [ ] **Caching:** Add Redis caching layer in the Backend API for high-traffic assets.
- [ ] **Testing:** Implement cross-service integration tests for the grading logic.

## Project Structure

```
Trade/
├── CLAUDE.md
├── docker-compose.yml
├── services/
│   ├── postgres/
│   ├── backend/
│   ├── news-fetcher/
│   ├── relevance/                   # AI Relevance filter (Zero-Shot)
│   ├── sentiment/                   # AI Sentiment (FinBERT)
│   ├── technical-analysis/
│   ├── price-fetcher/
│   └── grading/
└── frontend/
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

- **yfinance** for all market data (live + historical) - free, no API key required
- **FinBERT** for financial sentiment - domain-specific NLP model from HuggingFace
- **Jotai + Jotai Query** over Redux/React Query - lighter, atomic state management
- **Tailwind 4+** - latest version with CSS-first configuration
- **FastAPI** for all Python HTTP services - async, fast, auto-docs
- **pandas** as core data manipulation library for technical analysis
