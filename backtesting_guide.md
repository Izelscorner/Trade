# Backtesting & Calibration Operations Manual

This guide outlines the end-to-end workflow for managing historical data, running backtests, and recalibrating weights for the production grading service.

---

### Required Variables
The backtester relies on the same `.env` file as production but requires additional API keys for historical data:
- `NIM_API_KEY`: **Critical** for historical news sentiment analysis.
- `FRED_API_KEY`: Required for macro indicator historical retrieval.

### Infrastructure Management
The backtest compose file now includes its own `postgres` definition, allowing it to start all dependencies automatically.

### Automated Startup
Simply run any `backtester` command. Docker Compose will automatically start `postgres` if it is not already running:
```bash
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main status
```

---

## 1. Environment Setup

### Required Variables
The backtester relies on the same `.env` file as production but requires additional API keys for historical data:
- `NIM_API_KEY`: **Critical** for historical news sentiment analysis.
- `FRED_API_KEY`: Required for macro indicator historical retrieval.

---

## 2. Historical Data Sourcing

### Phase A: Price & Macro Data
The `price-fetcher` and `fundamentals-fetcher` in the production compose file should be used to populate the initial database.
```bash
docker compose up -d price-fetcher fundamentals-fetcher
```

### Phase B: News Sentiment Fetching
This is a one-time (or periodic) process to build the historical sentiment cache. It uses Google News search to simulate "past" news feeds.

```bash
# Start fetching historical sentiment (Mon-Fri)
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main fetch-sentiment
```
> [!TIP]
> Use `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main status` to monitor cache progress.

---

## 3. Running Simulations

Simulations reconstruct grades for every trading day in your backtest window.

### Step 1: Execute Grade Reconstruction
```bash
# Short-term (targets 5-day horizon)
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main backtest --term short

# Long-term (targets 20-day horizon)
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main backtest --term long --no-sentiment
```

### Step 2: Validate Results
Check the `backtest_grades` and `backtest_returns` tables to ensure scores and forward returns are aligning correctly.

---

## 4. Visual Performance Analysis

To generate a comprehensive HTML report with interactive charts (Equity Curve, Alpha Distributions):
```bash
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main report
```
The report will be saved to `./backtest_reports/report_[TIMESTAMP].html`. Open this file in any browser to view detailed analytics.

---

## 5. Calibration & Deployment

### Step 1: Optimize Weights
Run the optimizer to find the weights that maximize the Sharpe ratio:
```bash
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main calibrate --term short
```

### Step 2: Apply to Production
The `patch` command modifies the production logic file (`services/grading/app/scorer.py`) with the new weights.
```bash
# Verify the patch (dry-run)
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main patch --dry-run

# Commit the patch
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main patch
```
Finally, restart production services to use the new weights:
```bash
docker compose up -d --build grading
```

---

## Summary Checklist

| Action | Command | Frequency |
| :--- | :--- | :--- |
| **Store Data** | `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main fetch-sentiment` | Once per new instrument/year |
| **Verify Status** | `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main status` | As needed |
| **Run Simulation**| `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main backtest` | Whenever logic changes |
| **Generate Report**| `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main report` | After simulation |
| **Calibrate** | `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main calibrate` | After simulation |
| **Deploy** | `docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main patch` | Monthly or as markets shift |
