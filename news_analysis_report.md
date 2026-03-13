# News Data Loading Analysis

I have reviewed the news loading mechanisms for both production (live) and backtesting environments.

## 1. Production (Live News) Coverage
The `news-fetcher` service provides robust coverage:
- **Instruments**: All 15 active instruments (AAPL, GOLD, GOOGL, IITU, LLY, NVDA, NVO, OIL, PLTR, RTX, SMH, TSLA, VOO, WMT, XOM) are continuously tracked.
- **Sources**: Yahoo Finance RSS and Google News RSS queries per-asset.
- **Macro/Sector**: Extensive feeds for global economy, markets, politics, and 11 GICS sectors.
- **ETF-Specific**: Automatically crawls news for constituents of VOO, SMH, and IITU to improve ETF sentiment accuracy.

## 2. Backtest (Historical News) Coverage
The `backtester` service uses Google News search with date operators to simulate historical feeds. There are **significant gaps** in the current dataset:

| Category | Coverage Status | Missing/Partial Data |
| :--- | :--- | :--- |
| **Asset News** | ⚠️ Partial | **Missing (0 days)**: TSLA, VOO, WMT, XOM <br> **Partial**: SMH (488 days vs 825 for others) |
| **Macro News** | ❌ Missing | **Total Gap**: 0 days cached in `backtest_sentiment_cache` |
| **Sector News** | ❌ Missing | **Total Gap**: 0 days cached in `backtest_sentiment_cache` |

## 3. Evaluation: "Is it enough?"
- **For Production**: **Yes.** The system is well-covered and uses redundant sources (Yahoo + Google).
- **For Backtesting**: **No.** Current backtests for TSLA, VOO, WMT, and XOM are likely falling back to default/neutral sentiment values. All instruments are currently missing the **Macro** and **Sector** signals in backtesting, which significantly degrades the accuracy of historical simulation compared to the production grading logic.

## 4. Recommended Actions
1. **Resume Fetch**: Execute the sentiment fetcher specifically for the missing instruments and macro/sector types.
   ```bash
   docker compose -f docker-compose.backtest.yml run --rm backtester python -m app.main fetch-sentiment
   ```
2. **Verify SMH**: Investigate why `SMH` has 337 fewer days of data than `NVDA` or `AAPL` despite being a mature ETF.
3. **Re-run Backtests**: Once the cache is populated, re-run `backtest --term short --force` to ensure grades incorporate the newly available sentiment signals.

---

# Implementation Plan: Fill Gaps in Historical News Data

I will populate the missing historical sentiment data for the identified instruments and macro/sector signals to ensure backtests are accurate and consistent with production logic.

## Proposed Changes

### [Backtester]
Populate the `backtest_sentiment_cache` for:
- **Instruments**: TSLA, VOO, WMT, XOM (Full coverage)
- **Sector**: 11 GICS sectors (Full coverage)
- **Macro**: Global macro signals (Full coverage)
- **Refinement**: SMH (Fill remaining 337 days)

I will achieve this by running the `fetch-sentiment` command from the backtester service. Since the fetcher is idempotent, it will skip already cached data and only fetch the missing items.

## Verification Plan

### Automated Verification
- Run `python -m app.main status` in the backtester container to verify the `backtest_sentiment_cache` row counts match the expected total.
- Query the database directly to confirm `type` counts for 'macro' and 'sector' are now non-zero.

### Manual Verification
- Review a sample of the newly cached sentiment scores in the database to ensure they are being processed correctly by the LLM.
