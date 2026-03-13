# Backtesting & Calibration Operations Manual

This guide outlines the end-to-end workflow for managing historical data, running backtests, and recalibrating weights for the production grading service.

---

## 1. Environment Setup

### Required Variables
The backtester relies on the same `.env` file as production but requires additional API keys for historical data:
- `NIM_API_KEY`: **Critical** for historical news sentiment analysis.
- `FRED_API_KEY`: Required for macro indicator historical retrieval.

### Infrastructure Management
The backtest compose file includes its own `postgres` definition, allowing it to start all dependencies automatically.
```bash
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main status
```

---

## 2. Historical Data Sourcing

### Phase A: Price & Macro Data
The `price-fetcher` and `fundamentals-fetcher` should be used to populate the initial database.
```bash
docker compose up -d price-fetcher fundamentals-fetcher
```

### Phase B: News Sentiment Fetching
Uses Google News search to simulate "past" news feeds.
```bash
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main fetch-sentiment
```

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

### Asset Category Differentiation
The backtester automatically differentiates between **Stocks**, **ETFs**, and **Commodities**. It uses specialized weight profiles for each:
- **Stocks**: Large-cap and individual equtiies.
- **ETFs**: Diversified basket instruments.
- **Commodities**: Physical or derivative commodity instruments (uses 0% fundamental weight).

---

## 4. Visual Performance Analysis

### Step 4: Generate Performance Report
Generate a premium interactive HTML dashboard to visualize equity curves and signal quality.

```bash
# Standard Portfolio Weighted report
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main report --strategy portfolio --term short

# High-Conviction Top Pick report
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main report --strategy top_pick --term short
```

Options:
-   `--strategy`: 
    -   `portfolio`: (Default) Diversified allocation across all 15 instruments based on their relative scores.
    -   `top_pick`: High-octane concentration. Simulates picking ONLY the single highest-graded asset for that day and holding for the specified term.
-   `--term`: 
    -   `short`: Performance validated against 5-day forward returns.
    -   `long`: Performance validated against 20-day forward returns.
Reports are saved to `./backtest_reports/report_[TIMESTAMP].html`.

---

## 5. Calibration & Deployment

### Step 1: Optimize Weights
The optimizer finds weights that maximize the Sharpe ratio specifically for each **Category** and **Term**.
```bash
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main calibrate --term short
```

### Step 2: Apply to Production
The `patch` command synchronizes optimized weights to **both** production (`scorer.py`) and backtester (`simulator.py`).
```bash
# Verify the patch (dry-run)
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main patch --dry-run

# Commit the patch
docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main patch
```
Finally, restart production services:
```bash
docker compose restart grading
```

---

---

## 6. How the Simulation Works (Methodology)

Our backtester is "production-faithful," meaning it reconstructs the exact mathematical state the system would have seen at any point in history.

### A. Historical State Reconstruction
For every date in the simulation grid, the engine gathers three types of "Point-in-Time" data:
1.  **Prices**: Loads historical candles leading up to the target date. Indicators (RSI, MACD, etc.) are calculated using *only* data available up to that moment.
2.  **Factored News Sentiment**: Uses a cached repository of daily LLM-analyzed news. It applies an **exponential time-decay** (half-life of 12-24 hours) to news articles preceding the simulation date, simulating how "fresh" news impacts market sentiment.
3.  **Fundamentals**: Sources quarterly balance sheet data and checks for "freshness." If a financial report is more than 90 days old, its confidence score is automatically ramped down.

### B. Signal Generation & Grading
Once the data is reconstructed, the engine runs the **Production Scorer** logic:
-   **Five Sub-Signals**: Calculates Technical, Sentiment, Macro, Sector, and Fundamental scores.
-   **Weighted Sum**: Multiplies each signal by its category-specific weight (e.g., Commodities have 0% fundamental weight).
-   **Overall Grade**: Produces a final `overall_score` between `-3.0` (Strong Sell) and `+3.0` (Strong Buy).

### C. Performance Aggregation (Equity Curve)
To calculate the total returns shown in the report, the system simulates a **Cross-Sectional Factor Portfolio**:
1.  **Universe Allocation**: Every instrument in your watchlist (currently 15) is part of the portfolio every day.
2.  **Score-Based Weighting**: Your capital is divided among these 15 assets according to their `overall_score`. 
    -   A $3.0$ score = $100\%$ allocation to that asset (relative to its share of the portfolio).
    -   A $-3.0$ score = $100\%$ Short position.
3.  **Daily Attribution**: Because returns (e.g., 20-day) are overlapping, the system attributes **1/20th** of the total move to each simulated day. This prevents "double-counting" and provides a realistic, non-leveraged 1.0x NAV growth curve.

### D. Calibration Loop
The Calibration process uses these results to "back-solve" for the best weights:
-   It varies the weights (within bounds like 5% - 60%) to see which combination would have maximized the **Sharpe Ratio** (Return / Volatility) over the historical period.
-   **Validation**: It tests these new weights on a separate "holdout" period (recent data) to ensure they wasn't just "lucky" (overfitting).
