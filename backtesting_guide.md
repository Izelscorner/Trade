# Backtesting & Calibration Operations Manual

End-to-end workflow for historical data management, backtesting, weight calibration, and trading strategy evaluation.

---

## 1. Environment Setup

### Required Variables
The backtester uses the same `.env` file as production:
- `NIM_API_KEY`: **Critical** — powers historical news sentiment analysis via Qwen 122B.
- `FRED_API_KEY`: Required for macro indicator historical retrieval.

### Quick Start
```bash
# Check status of all data and caches
docker compose run --rm backtester python -m app.main status

# Full automated pipeline (fetch → backtest → calibrate → patch → report)
docker compose run --rm backtester python -m app.main run-all
```

---

## 2. Historical Data Sourcing

### Phase A: Price & Fundamental Data
The `price-fetcher` and `fundamentals-fetcher` populate historical OHLCV and quarterly financials.
```bash
docker compose up -d price-fetcher fundamentals-fetcher
```

Fundamentals from yfinance are **cached persistently** in a Docker volume (`backtest_cache`), so subsequent backtest runs skip the API call entirely.

### Phase B: News Sentiment Fetching (Google News + NIM LLM)
Fetches daily news for **3 signal types** via Google News RSS with date operators, then scores each article through the NIM/Qwen 122B LLM with dual-horizon sentiment (same model and prompts as production):

| Signal Type | Coverage | Daily Items |
|-------------|----------|-------------|
| **Asset** | 15 instruments (per-ticker news) | 15 × 825 trading days |
| **Macro** | Federal Reserve, GDP, geopolitics, markets | 1 × 825 trading days |
| **Sector** | 11 GICS sectors (technology, financials, etc.) | 11 × 825 trading days |

```bash
# Pre-fetch all sentiment (resumes from cache, ~5-6 hours first run)
docker compose run --rm backtester python -m app.main fetch-sentiment

# Monitor progress
docker compose run --rm backtester python -m app.main status
```

The fetch is **fully idempotent** — it skips any `(type, key, date)` already in `backtest_sentiment_cache`. Safe to interrupt and resume.

---

## 3. Running Simulations

Simulations reconstruct grades for every sampled trading day in the backtest window (default: 2023-01-01 → 2026-03-01).

### Sentiment Mode
The backtester stores grades with a `sentiment_mode` column (`on` or `off`), so **both variants coexist** in the database without overwriting each other.

- **With sentiment** (`sentiment_mode='on'`): Uses cached sentiment/macro/sector scores from `backtest_sentiment_cache`.
- **Without sentiment** (`sentiment_mode='off'`): Zeros out sentiment/macro/sector — tests technical + fundamentals only.

### Execute Grade Reconstruction
```bash
# With sentiment (uses cached data, no API calls)
docker compose run --rm backtester python -m app.main backtest --term short
docker compose run --rm backtester python -m app.main backtest --term long

# Without sentiment (technical + fundamentals only)
docker compose run --rm backtester python -m app.main backtest --term short --no-sentiment
docker compose run --rm backtester python -m app.main backtest --term long --no-sentiment

# Force re-compute all grades (even if they exist in DB)
docker compose run --rm backtester python -m app.main backtest --term short --force

# Fetch NEW sentiment from Google News + NIM (slow, only when expanding coverage)
docker compose run --rm backtester python -m app.main backtest --term short --fetch-sentiment
```

### What Gets Computed Per Date
For each `(instrument, trading_day)`, the backtester computes **5 sub-signals** using only data available at that moment (no look-ahead bias):

| Sub-Signal | Source | Scoring |
|------------|--------|---------|
| **Technical** | 18 indicators from historical OHLCV (sliced at cutoff) | Group-weighted average + ADX/ATR modifiers + divergence dampener |
| **Sentiment** | Daily Google News + NIM LLM (dual-horizon) | Exponential decay (12h/168h half-life), consensus dampening |
| **Macro** | Daily macro news + NIM LLM (dual-horizon) | Decay-weighted mean × 3.0 × confidence |
| **Sector** | Daily GICS sector news + NIM LLM (dual-horizon) | Decay-weighted mean × 3.0 × confidence |
| **Fundamentals** | yfinance quarterly P/E, ROE, D/E, PEG | Sector-relative thresholds, freshness confidence |

All parameters, decay rates, confidence formulas, and weight profiles are **exact copies** of production `scorer.py`.

### Asset Category Differentiation
- **Stocks**: Full 5-signal composite with sector-relative fundamental thresholds.
- **ETFs**: Higher macro weight, lower technical weight for long-term.
- **Commodities**: 0% fundamentals weight (no P/E for futures contracts).

---

## 4. Trading Strategies

The backtester includes **8 parameterized trading strategies** that use **non-overlapping holding periods** to produce honest return calculations.

### Non-Overlapping Methodology
- **Short-term (5-day hold)**: Enter position, hold for 5 trading days (1 calendar week), then re-evaluate. ~165 trades over 3 years.
- **Long-term (20-day hold)**: Enter position, hold for 20 trading days (~4 calendar weeks), then re-evaluate. ~41 trades over 3 years.
- Weekends are excluded — holding periods are in **trading days only**.
- Each position earns the **full forward return** for the period (no daily slicing or overlapping windows).

### List All Strategies
```bash
docker compose run --rm backtester python -m app.main list-strategies
```

### Available Strategies

| Strategy | Description | What It Tests |
|----------|-------------|---------------|
| `portfolio` | Score-weighted all-instrument portfolio | Overall signal utility |
| `top_pick` | 100% in highest-scoring instrument per period | Best-case signal accuracy |
| `high_conviction` | Only trade when buy-confidence >= threshold | Quality over quantity |
| `top_n` | Equal-weight top N instruments per period | Diversified signal quality |
| `long_short` | Long top 20%, short bottom 20% | Pure alpha (market-neutral) |
| `sector_rotation` | Best instrument per sector, equal-weight | Sector-level signal value |
| `contrarian` | Buy bottom quintile | Mean-reversion detection |
| `risk_adjusted` | Score / volatility position sizing | Risk-parity efficiency |

### Strategy Parameters

| Parameter | Flag | Default | Used By |
|-----------|------|---------|---------|
| Confidence threshold | `--threshold` | 60.0 | `high_conviction` |
| Top N instruments | `--top-n` | 3 | `top_n` |
| Long/short percentile | `--long-pct` | 0.20 | `long_short`, `contrarian` |

---

## 5. Performance Reports

Generate interactive HTML dashboards with equity curves, quintile analysis, and per-instrument statistics.

### Single Strategy Report
```bash
# Top pick, short-term (5-day hold), with sentiment
docker compose run --rm backtester python -m app.main report --strategy top_pick --term short

# High conviction (>= 70% confidence), long-term (20-day hold)
docker compose run --rm backtester python -m app.main report --strategy high_conviction --term long --threshold 70

# Without sentiment (uses no-sentiment grades)
docker compose run --rm backtester python -m app.main report --strategy top_pick --term short --no-sentiment
```

### All Strategies at Once
```bash
docker compose run --rm backtester python -m app.main report-all --term short
docker compose run --rm backtester python -m app.main report-all --term short --no-sentiment
```

### Summary Table
After every report generation, a formatted summary table is printed to the console:
```
==============================================================================================================
  Strategy             Horizon      Sentiment              Return      $1K->   Sharpe   Win Rate   Trades
  -------------------- ------------ ------------------ ---------- ---------- -------- ---------- --------
  high_conviction      5-day hold   with sentiment        +358.2%      4,582     2.19      68.5%    89 (11%)
  top_pick             5-day hold   with sentiment       +1646.9%     17,469     2.00      61.8%   165 (20%)
  high_conviction      5-day hold   without sentiment     +183.8%      2,838     1.80      61.8%    76 (9%)
  top_pick             20-day hold  with sentiment        +457.7%      5,577     1.57      65.9%    41 (5%)
==============================================================================================================
```

Results are **sorted by Sharpe ratio** (highest first). When running `report-all` or `run-all`, all strategies appear in a single combined table.

### Report Output
Reports are saved to `./backtest_reports/` with descriptive filenames:
```
report_top_pick_5day_short_with_sentiment.html
report_high_conviction_20day_long_without_sentiment.html
```

Each report includes:
- **KPI cards**: Cumulative return, $1,000 investment result, Sharpe ratio, win rate, trade count & exposure %
- **Equity curve**: Cumulative return over the simulation period (non-overlapping periods)
- **Quintile analysis**: Average forward return by score quintile (should be monotonically increasing)
- **Score distribution**: Histogram of overall scores
- **Instrument table**: Per-ticker mean score, avg return, information coefficient, win rate
- **Weight profiles**: Nominal composite weights per category

---

## 6. Calibration & Deployment

### Step 1: Optimize Weights
The optimizer finds weights that maximize Sharpe ratio for each `(category, term)` combination using scipy SLSQP.

```bash
docker compose run --rm backtester python -m app.main calibrate --term short
docker compose run --rm backtester python -m app.main calibrate --term long
```

Walk-forward validation: trains on months 1–18, validates on months 19–24. Only applies new weights if validation Sharpe improves by ≥ 10%.

### Step 2: Apply to Production
```bash
# Preview changes (dry-run)
docker compose run --rm backtester python -m app.main patch --dry-run

# Apply to scorer.py
docker compose run --rm backtester python -m app.main patch
```

### Step 3: Restart Production
```bash
docker compose restart grading
```

---

## 7. Data Backup & Restore

Historical sentiment data takes **5-6 hours** to fetch via Google News + NIM LLM. **Always export after a successful fetch** so you can restore without re-fetching.

### Export All Backtest Data
```bash
docker compose run --rm backtester python -m app.main export-data
```

Creates `./backtest_reports/backtest_data_export.sql` (~27 MB). This file persists on your host filesystem and survives `docker compose down -v`.

### Restore After DB Reset
```bash
# After docker compose down -v (which destroys all volumes):
docker compose up -d postgres
# Wait for postgres to be healthy (~5 seconds), then:
docker compose run --rm backtester python -m app.main import-data --file /reports/backtest_data_export.sql
```

### Create a Safe Backup Copy
```bash
cp backtest_reports/backtest_data_export.sql ~/backtest_backup_$(date +%Y%m%d).sql
```

### What Gets Exported

| Table | Contents | Typical Size |
|-------|----------|--------------|
| `backtest_sentiment_cache` | Daily sentiment scores (asset + macro + sector) | ~16,000 rows |
| `backtest_grades` | Simulated grades per (instrument, date, term, sentiment_mode) | ~36,000 rows |
| `backtest_returns` | Forward 5-day and 20-day returns | ~9,000 rows |
| `calibration_runs` | Calibration results (weights before/after, Sharpe) | ~12 rows |

### Important Notes
- **Export after every successful sentiment fetch** — this is the most expensive data to regenerate.
- The SQL dump file is in `./backtest_reports/` which is a host-mounted volume, so it survives `docker compose down -v`.
- Fundamentals from yfinance are cached in a Docker volume (`backtest_cache`). This volume is destroyed by `docker compose down -v` but not by `docker compose down`.
- Import is idempotent — it uses `DELETE FROM` before inserting, so running it twice is safe.

---

## 8. How the Simulation Works (Methodology)

The backtester is **production-faithful** — it reconstructs the exact mathematical state the production system would have computed at any historical date.

### A. Historical State Reconstruction
For every date in the simulation grid, the engine gathers point-in-time data:
1. **Prices**: Historical OHLCV sliced at cutoff. All 18 technical indicators computed using only data available at that moment.
2. **News Sentiment**: Daily LLM-analyzed news from `backtest_sentiment_cache`. Exponential time-decay applied (12h half-life for short-term, 168h for long-term). Consensus dampening (×0.85 at >80% agreement, ×0.90 for stale consensus >48h).
3. **Macro Sentiment**: Same LLM pipeline for macro news (Federal Reserve, GDP, geopolitics). Decay: 24h/648h half-life.
4. **Sector Sentiment**: Per-GICS-sector news. Decay: 36h/240h half-life.
5. **Fundamentals**: yfinance quarterly data (most recent filing before cutoff). Freshness confidence: 1.0 within 48h, linear decay to 0.3 at 30 days.

### B. Signal Generation & Grading
- **Five Sub-Signals**: Technical, Sentiment, Sector, Macro, Fundamentals — each in [-3, 3].
- **Confidence-Adjusted Weights**: Each sub-signal's weight = `nominal × (0.1 + 0.9 × confidence)`.
- **Composite Score**: Weighted sum / total effective weight → clipped to [-3, 3].
- **Buy Confidence**: `100 / (1 + e^(-1.5 × score))` — sigmoid mapping to (0, 100)%.

### C. Forward Returns
- **Short-term**: 5 trading day close-to-close return from grade date.
- **Long-term**: 20 trading day close-to-close return from grade date.

### D. Non-Overlapping Holding Periods
Strategies use **non-overlapping windows** to avoid inflated returns from overlapping forward-return compounding:
- Entry dates are spaced by the holding period (every 5th or 20th trading day).
- Each position earns its full forward return (not divided by horizon days).
- The equity curve compounds only entry-date returns: `(1 + r1) × (1 + r2) × ... - 1`.
- Sharpe ratio annualized by periods per year: 52 for short-term (weekly), 13 for long-term (monthly).

### E. Calibration Loop
scipy SLSQP optimizer varies weights (bounds: 5%–60%, sum=1.0) to maximize Sharpe ratio. Commodity fundamentals forced to 0%. Validated on holdout period to prevent overfitting.

---

## 9. Useful SQL Queries

```sql
-- Check sentiment cache coverage
SELECT type, COUNT(*) as cached, MIN(date), MAX(date)
FROM backtest_sentiment_cache GROUP BY type;

-- Check per-symbol asset coverage
SELECT key, COUNT(*) as days FROM backtest_sentiment_cache
WHERE type='asset' GROUP BY key ORDER BY key;

-- Check grades by sentiment mode
SELECT term, sentiment_mode, COUNT(*), ROUND(AVG(sentiment_score::float)::numeric, 3) as avg_sent
FROM backtest_grades GROUP BY term, sentiment_mode ORDER BY term, sentiment_mode;

-- Grade quintile vs forward return (signal quality check)
SELECT width_bucket(overall_score, -3, 3, 5) as quintile,
       COUNT(*) as n,
       ROUND(AVG(br.return_20d)::numeric*100, 2) as avg_20d_pct
FROM backtest_grades bg
JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
WHERE bg.term = 'short' AND bg.sentiment_mode = 'on' AND br.return_20d IS NOT NULL
GROUP BY quintile ORDER BY quintile;

-- Pearson correlation per category
SELECT bg.term, i.category, COUNT(*) as n,
       ROUND(CORR(bg.overall_score, br.return_20d)::numeric, 4) as pearson_corr
FROM backtest_grades bg
JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
JOIN instruments i ON i.id = bg.instrument_id
WHERE br.return_20d IS NOT NULL AND bg.sentiment_mode = 'on'
GROUP BY bg.term, i.category ORDER BY bg.term, i.category;

-- Calibration results
SELECT category, term, sharpe_before, sharpe_after,
       directional_accuracy_before, directional_accuracy_after,
       n_samples FROM calibration_runs ORDER BY created_at DESC;

-- Compare sentiment impact on grades
SELECT sentiment_mode,
       AVG(ABS(sentiment_score::float))::numeric(7,3) as avg_abs_sent,
       AVG(ABS(macro_score::float))::numeric(7,3) as avg_abs_macro,
       AVG(ABS(sector_score::float))::numeric(7,3) as avg_abs_sector
FROM backtest_grades WHERE term='short'
GROUP BY sentiment_mode;
```
