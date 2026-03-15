# Backtesting Guide

## Quick Start

```bash
# Check what data you have
docker compose run --rm backtester python -m app.main status

# Run the backtest (fetches prices + fundamentals automatically on first run)
docker compose run --rm backtester python -m app.main backtest

# Generate all strategy reports + summary
docker compose run --rm backtester python -m app.main report-all

# Full automated pipeline (sentiment + backtest + calibrate + patch + reports)
docker compose run --rm backtester python -m app.main run-all
```

---

## 1. Data Pipeline

The backtester needs three types of historical data. Prices and fundamentals are fetched automatically when you run `backtest`. Sentiment requires a separate fetch step.

### Prices (OHLCV)

Fetched from yfinance with `period="max"` — full history since each instrument's inception. Cached as parquet files in the `backtest_cache` Docker volume. Production `price-fetcher` now also uses `period="max"` for initial fetch, so both systems have identical price depth.

### Fundamentals (Quarterly Financial Statements)

Primary source is **SEC EDGAR** (free, no API key, 18+ years of quarterly data for US stocks). Falls back to yfinance (~5 quarters) for non-US filers (NVO) and ETFs.

From EDGAR, the backtester extracts:
- **Diluted EPS** — for P/E ratio (price / trailing 4Q EPS)
- **Net Income** — for ROE (TTM net income / book equity) and PEG
- **Total Revenue** — for revenue growth (YoY TTM), used to soften negative P/E for high-growth loss-makers
- **Stockholders Equity** — for ROE and D/E
- **Long-Term Debt** — for D/E ratio

ETF fundamentals are computed as weighted averages of constituent metrics. Commodities have no fundamentals (0% weight).

Cached as pickle files in the `backtest_cache` Docker volume.

### Sentiment (Google News + NIM LLM)

Daily news sentiment for 3 signal types, scored through the same Qwen 122B model and prompts as production:

| Type | What | Count |
|------|------|-------|
| Asset | Per-instrument news (52 tickers) | 52 x ~1600 trading days |
| Macro | Fed, GDP, geopolitics, markets | 1 x ~1600 trading days |
| Sector | 11 GICS sectors | 11 x ~1600 trading days |

```bash
# Fetch sentiment separately (5-6 hours first run, resumes from cache)
docker compose run --rm backtester python -m app.main fetch-sentiment

# Or fetch as part of backtest
docker compose run --rm backtester python -m app.main backtest --fetch-sentiment
```

The fetch is fully idempotent — safe to interrupt and resume.

> **You can run backtests without sentiment.** The "No Sentiment" variations (technical + fundamentals only) work fine. Sentiment variations will show zero sentiment/macro/sector scores until you fetch the data.

---

## 2. Running Backtests

Every `backtest` run automatically produces **4 variations**:

| Variation | Hold Period | Signals Used |
|-----------|-------------|-------------|
| Short-Term + Sentiment | 5 trading days | All 5 (technical + sentiment + sector + macro + fundamentals) |
| Short-Term, No Sentiment | 5 trading days | Technical + fundamentals only |
| Long-Term + Sentiment | 20 trading days | All 5 |
| Long-Term, No Sentiment | 20 trading days | Technical + fundamentals only |

```bash
# Standard run (skips existing grades, uses cached data)
docker compose run --rm backtester python -m app.main backtest

# Force re-compute all grades
docker compose run --rm backtester python -m app.main backtest --force
```

### What Happens Per (Instrument, Date)

For each trading day in the backtest range (default 2020-01-01 to 2026-03-01), using only data available at that date (no look-ahead bias):

1. **Technical score** — 18 indicators computed from OHLCV sliced at cutoff date, grouped into 5 categories (trend, momentum, volume, levels, volatility), weighted by category/term profile, modified by ADX (trend strength) and ATR (volatility risk)
2. **Sentiment score** — exponential time-decay of article sentiment labels, with contrarian dampening at >80% agreement and priced-in detection for stale consensus
3. **Macro score** — same decay-weighted approach on macro news
4. **Sector score** — same on GICS sector news
5. **Fundamentals score** — P/E, ROE, D/E, PEG from quarterly filings, sector-relative thresholds, freshness-based confidence

Sub-scores computed once per term. "No Sentiment" mode zeros out sentiment/macro/sector. Both modes stored in DB with `sentiment_mode` column.

### Production Fidelity

All scoring parameters are identical to production `scorer.py`:
- Same 18 technical indicators with same thresholds
- Same composite weight profiles per category/term
- Same confidence adjustment: `effective_weight = nominal x (0.1 + 0.9 x confidence)`
- Same sigmoid: `buy_confidence = 100 / (1 + e^(-1.5 x score))`
- Same action labels (Strong Buy >= 78%, Buy >= 63%, etc.)

---

## 3. Trading Strategies

10 strategies with **non-overlapping holding periods** (5-day short, 20-day long) for honest return calculation.

```bash
docker compose run --rm backtester python -m app.main list-strategies
```

| Strategy | What It Does | What It Tests |
|----------|-------------|---------------|
| `portfolio` | Score-weighted all-instrument portfolio | Overall signal utility |
| `top_pick` | 100% in highest-scoring instrument each period | Best-case signal accuracy |
| `high_conviction` | Only trade when confidence >= threshold | Quality filtering |
| `top_n` | Equal-weight top N instruments each period | Diversified signal quality |
| `long_short` | Long top 20%, short bottom 20% | Pure alpha (market-neutral) |
| `sector_rotation` | Best instrument per sector, equal-weight | Sector signal value |
| `contrarian` | Buy bottom quintile | Mean-reversion detection |
| `risk_adjusted` | Score / volatility position sizing | Risk-parity efficiency |
| `momentum` | Top N by trailing return (ignores scores) | Does momentum beat signals? |
| `random` | Random N picks (100 Monte Carlo avg) | Does the signal beat chance? |

### Strategy Parameters

| Flag | Default | Used By |
|------|---------|---------|
| `--threshold` | 60.0 | `high_conviction` |
| `--top-n` | 3 | `top_n` |
| `--long-pct` | 0.20 | `long_short`, `contrarian` |
| `--cost-bps` | 0.0 | All strategies (transaction cost) |

---

## 4. Reports

### Single Strategy Report
```bash
docker compose run --rm backtester python -m app.main report --strategy portfolio
docker compose run --rm backtester python -m app.main report --strategy high_conviction --threshold 70 --cost-bps 5
```

Each report is one HTML file with all 4 variations side by side:
- Best variation highlighted with KPI cards
- Comparison table (return, Sharpe, win rate, alpha, trades)
- Overlaid equity curves (all 4 on one chart)
- Per-instrument stats (mean score, avg return, IC, win rate)
- Collapsible trade logs per variation

### All Reports + Summary
```bash
docker compose run --rm backtester python -m app.main report-all
docker compose run --rm backtester python -m app.main report-all --cost-bps 5
```

Generates 10 strategy reports + 1 summary report = 11 HTML files in `./backtest_reports/`.

The summary report (`report_summary.html`) ranks all strategies x variations by alpha and includes an automated conclusion analyzing whether sentiment helps and which horizon wins.

---

## 5. Weight Optimization

The calibrator finds optimal composite weights (technical, sentiment, sector, macro, fundamentals) that maximize predictive power.

### How It Works

1. Loads all backtest grades + forward returns from DB
2. Splits by category (stock/ETF/commodity) and term (short/long)
3. For each combo, runs scipy SLSQP optimizer with 20 random restarts
4. **Objective:** minimize `-(0.7 x IC + 0.3 x Sharpe)` where IC = Spearman rank correlation
5. **Constraints:** weights sum to 1.0, each weight in [7%, 45%], commodity fundamentals = 0%
6. **Validation:** 18-month training / 6-month holdout split. Only applies if holdout improvement >= 0.20

```bash
# Optimize weights for short-term grades
docker compose run --rm backtester python -m app.main calibrate --term short

# Optimize weights for long-term grades
docker compose run --rm backtester python -m app.main calibrate --term long
```

### Apply to Production

```bash
# Preview what would change
docker compose run --rm backtester python -m app.main patch --dry-run

# Apply optimized weights to scorer.py + simulator.py
docker compose run --rm backtester python -m app.main patch

# Restart grading service to use new weights
docker compose restart grading
```

The patcher uses regex to find and replace `COMPOSITE_WEIGHT_PROFILES` in both `scorer.py` and `simulator.py`, keeping them in sync. Old weights are backed up in the `calibration_runs` DB table.

### Walk-Forward Validation

Tests whether calibrated weights generalize out-of-sample using rolling windows:

```bash
docker compose run --rm backtester python -m app.main walk-forward --term short
docker compose run --rm backtester python -m app.main walk-forward --term long
```

- **Train:** 12 months rolling window
- **Test:** 3 months rolling window (stepped forward by 3 months each iteration)
- Compares default vs calibrated weights on unseen test data
- Reports Sharpe, IC, alpha for each window

### Optimization Workflow

The recommended flow for tuning weights:

```bash
# 1. Run full backtest with all data
docker compose run --rm backtester python -m app.main backtest

# 2. Generate baseline reports to see current performance
docker compose run --rm backtester python -m app.main report-all

# 3. Run calibration for both terms
docker compose run --rm backtester python -m app.main calibrate --term short
docker compose run --rm backtester python -m app.main calibrate --term long

# 4. Walk-forward test to verify calibrated weights generalize
docker compose run --rm backtester python -m app.main walk-forward --term short
docker compose run --rm backtester python -m app.main walk-forward --term long

# 5. If walk-forward looks good, preview changes
docker compose run --rm backtester python -m app.main patch --dry-run

# 6. Apply and regenerate reports to compare
docker compose run --rm backtester python -m app.main patch
docker compose run --rm backtester python -m app.main backtest --force
docker compose run --rm backtester python -m app.main report-all

# 7. Restart production grading
docker compose restart grading
```

---

## 6. Data Backup & Restore

Sentiment data takes 5-6 hours to fetch. Always export after a successful fetch.

```bash
# Export all backtest tables to SQL dump
docker compose run --rm backtester python -m app.main export-data

# Restore after docker compose down -v
docker compose up -d postgres
# Wait for postgres to be healthy, then:
docker compose run --rm backtester python -m app.main import-data --file /reports/backtest_data_export.sql

# Keep a safe copy
cp backtest_reports/backtest_data_export.sql ~/backtest_backup_$(date +%Y%m%d).sql
```

Exports to `./backtest_reports/backtest_data_export.sql` (~27 MB). This persists on your host filesystem and survives `docker compose down -v`.

**What gets exported:** `backtest_sentiment_cache` (sentiment scores), `backtest_grades` (simulated grades), `backtest_returns` (forward returns), `calibration_runs` (calibration results).

**What lives in Docker volumes only:** Price OHLCV parquet cache and fundamentals pickle cache (in `backtest_cache` volume). These are fast to regenerate from yfinance/EDGAR but destroyed by `docker compose down -v`.

---

## 7. Configuration

Environment variables in `.env` or overridden at runtime:

| Variable | Default | Purpose |
|----------|---------|---------|
| `BACKTEST_START` | `2020-01-01` | Start of backtest range |
| `BACKTEST_END` | `2026-03-01` | End of backtest range |
| `SAMPLE_EVERY_N_DAYS` | `1` | Sample interval (1=daily, 5=weekly) |
| `NIM_API_KEY` | — | Required for sentiment fetching |
| `FRED_API_KEY` | — | Required for macro indicators |
| `SCORER_PY_PATH` | `/scorer/scorer.py` | Production scorer path (Docker volume mount) |

---

## 8. CLI Reference

| Command | Description |
|---------|-------------|
| `status` | Show cache coverage, data sources, grade counts |
| `backtest` | Run all 4 variations (auto-fetches prices + fundamentals) |
| `backtest --fetch-sentiment` | Fetch missing sentiment before backtesting |
| `backtest --force` | Re-compute all grades even if they exist |
| `fetch-sentiment` | Pre-fetch historical sentiment (resumes from cache) |
| `report --strategy NAME` | Generate multi-variation report for one strategy |
| `report-all` | Generate all strategy reports + summary |
| `report-all --cost-bps 5` | Include transaction costs (basis points) |
| `list-strategies` | Show available trading strategies |
| `calibrate --term short\|long` | Optimize composite weights |
| `walk-forward --term short\|long` | Walk-forward out-of-sample validation |
| `patch` | Apply calibrated weights to production scorer.py |
| `patch --dry-run` | Preview weight changes without writing |
| `run-all` | Full pipeline: sentiment -> backtest -> calibrate -> patch -> reports |
| `export-data` | Export backtest tables to SQL dump |
| `import-data --file PATH` | Import backtest data from SQL dump |

---

## 9. Useful SQL Queries

```sql
-- Grade coverage by variation (all 4 should have equal counts)
SELECT term, sentiment_mode, COUNT(*) as grades
FROM backtest_grades GROUP BY term, sentiment_mode ORDER BY term, sentiment_mode;

-- Signal quality: score quintile vs forward return
SELECT width_bucket(overall_score, -3, 3, 5) as quintile,
       COUNT(*) as n,
       ROUND(AVG(br.return_20d)::numeric*100, 2) as avg_20d_pct
FROM backtest_grades bg
JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
WHERE bg.term = 'short' AND bg.sentiment_mode = 'on' AND br.return_20d IS NOT NULL
GROUP BY quintile ORDER BY quintile;

-- Predictive power by category
SELECT bg.term, i.category,
       ROUND(CORR(bg.overall_score, br.return_20d)::numeric, 4) as pearson_corr
FROM backtest_grades bg
JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
JOIN instruments i ON i.id = bg.instrument_id
WHERE br.return_20d IS NOT NULL AND bg.sentiment_mode = 'on'
GROUP BY bg.term, i.category ORDER BY bg.term, i.category;

-- Sentiment vs no-sentiment sub-score comparison
SELECT sentiment_mode,
       AVG(ABS(sentiment_score::float))::numeric(7,3) as avg_abs_sent,
       AVG(ABS(macro_score::float))::numeric(7,3) as avg_abs_macro,
       AVG(ABS(sector_score::float))::numeric(7,3) as avg_abs_sector
FROM backtest_grades WHERE term='short'
GROUP BY sentiment_mode;

-- Calibration results history
SELECT category, term, sharpe_before, sharpe_after,
       directional_accuracy_before, directional_accuracy_after,
       n_samples FROM calibration_runs ORDER BY created_at DESC;
```
