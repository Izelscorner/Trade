"""Backtest engine: run retroactive grade simulation over a date grid.

Pipeline:
1. Load historical OHLCV from DB for all instruments.
2. Load historical fundamental data from yfinance once per instrument.
3. Load daily sentiment cache (asset + macro + sector) from backtest_sentiment_cache.
4. For each (instrument, trading_day): compute all 5 sub-scores, simulate grade.
5. Compute forward returns (5-day, 20-day) from historical prices.
6. Store results in backtest_grades and backtest_returns tables.

Signal fidelity vs production:
  - Technical:     exact copy (18 indicators, same groups, ADX/ATR modifiers, divergence dampener)
  - Sentiment:     production-faithful (daily buckets, same decay params, consensus dampening)
  - Macro:         production-faithful (LLM news sentiment, same MACRO_PARAMS decay)
  - Sector:        production-faithful (11 GICS sectors, LLM news, same SECTOR_PARAMS decay)
  - Fundamentals:  production-faithful (yfinance quarterly, freshness confidence)
"""

import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import text

from .config import BACKTEST_END, BACKTEST_START, SAMPLE_EVERY_N_DAYS
from .db import async_session
from .historical_fundamentals import (
    calc_fundamentals_score_for_date,
    fetch_fundamentals_history,
)
from .historical_sentiment import (
    fetch_all_historical_sentiment,
    get_asset_sentiment_for_date,
    get_macro_sentiment_for_date,
    get_sector_sentiment_for_date,
    load_sentiment_cache,
)
from .historical_tech import calc_technical_score, get_historical_ohlcv
from .simulator import COMPOSITE_WEIGHT_PROFILES, SubScores, simulate_grade

logger = logging.getLogger(__name__)


async def load_instruments() -> list[dict]:
    """Load all active instruments from DB."""
    async with async_session() as session:
        result = await session.execute(
            text(
                "SELECT id, symbol, name, category, sector, yfinance_symbol "
                "FROM instruments WHERE is_active = true ORDER BY symbol"
            )
        )
        return [dict(r._mapping) for r in result.fetchall()]


async def store_backtest_grade(
    instrument_id: str,
    symbol: str,
    d: date,
    term: str,
    overall_score: float,
    technical_score: float,
    technical_conf: float,
    sentiment_score: float,
    sentiment_conf: float,
    macro_score: float,
    macro_conf: float,
    sector_score: float,
    sector_conf: float,
    fundamentals_score: float,
    fundamentals_conf: float,
    weights: dict,
    sentiment_mode: str = "on",
) -> None:
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO backtest_grades
                  (instrument_id, symbol, date, term, overall_score,
                   technical_score, technical_conf,
                   sentiment_score, sentiment_conf,
                   macro_score, macro_conf,
                   sector_score, sector_conf,
                   fundamentals_score, fundamentals_conf,
                   weights, sentiment_mode)
                VALUES
                  (:iid, :sym, :date, :term, :overall,
                   :tech, :tech_conf,
                   :sent, :sent_conf,
                   :macro, :macro_conf,
                   :sector, :sec_conf,
                   :fund, :fund_conf,
                   CAST(:weights AS jsonb), :smode)
                ON CONFLICT (instrument_id, date, term, sentiment_mode) DO UPDATE
                  SET overall_score      = EXCLUDED.overall_score,
                      technical_score    = EXCLUDED.technical_score,
                      technical_conf      = EXCLUDED.technical_conf,
                      sentiment_score    = EXCLUDED.sentiment_score,
                      sentiment_conf      = EXCLUDED.sentiment_conf,
                      macro_score        = EXCLUDED.macro_score,
                      macro_conf          = EXCLUDED.macro_conf,
                      sector_score       = EXCLUDED.sector_score,
                      sector_conf         = EXCLUDED.sector_conf,
                      fundamentals_score = EXCLUDED.fundamentals_score,
                      fundamentals_conf   = EXCLUDED.fundamentals_conf,
                      weights            = EXCLUDED.weights
            """),
            {
                "iid": instrument_id, "sym": symbol, "date": d, "term": term,
                "overall": overall_score,
                "tech": technical_score, "tech_conf": technical_conf,
                "sent": sentiment_score, "sent_conf": sentiment_conf,
                "macro": macro_score, "macro_conf": macro_conf,
                "sector": sector_score, "sec_conf": sector_conf,
                "fund": fundamentals_score, "fund_conf": fundamentals_conf,
                "weights": str(weights).replace("'", '"'),
                "smode": sentiment_mode,
            },
        )
        await session.commit()


async def store_backtest_return(
    instrument_id: str,
    symbol: str,
    d: date,
    return_5d: float | None,
    return_20d: float | None,
) -> None:
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO backtest_returns (instrument_id, symbol, date, return_5d, return_20d)
                VALUES (:iid, :sym, :date, :r5, :r20)
                ON CONFLICT (instrument_id, date) DO UPDATE
                  SET return_5d  = EXCLUDED.return_5d,
                      return_20d = EXCLUDED.return_20d
            """),
            {"iid": instrument_id, "sym": symbol, "date": d, "r5": return_5d, "r20": return_20d},
        )
        await session.commit()


def get_trading_days(start: date, end: date, every_n: int = 5) -> list[date]:
    """Generate trading days (weekdays only) sampled every N days."""
    days = []
    current = start
    count = 0
    while current <= end:
        if current.weekday() < 5:
            if count % every_n == 0:
                days.append(current)
            count += 1
        current += timedelta(days=1)
    return days


def get_forward_return(ohlcv_df: pd.DataFrame, d: date, horizon_days: int) -> float | None:
    """Get forward close-to-close return from date d over horizon_days trading days."""
    cutoff = pd.Timestamp(d)
    future_rows = ohlcv_df[ohlcv_df.index > cutoff]
    if len(future_rows) < horizon_days:
        return None
    start_close = ohlcv_df[ohlcv_df.index <= cutoff]["close"].iloc[-1]
    end_close = float(future_rows["close"].iloc[horizon_days - 1])
    if start_close == 0:
        return None
    return round((end_close - start_close) / start_close, 6)


async def run_backtest(
    term: str = "short",
    fetch_sentiment: bool = True,
    skip_existing: bool = True,
    ignore_sentiment: bool = False,
) -> list[dict]:
    """Run the full backtest over all instruments and trading days.

    Args:
        term:             'short' (5-day forward return) or 'long' (20-day).
        fetch_sentiment:  If True, fetch any missing sentiment from Google News + NIM.
                          If False, use only what's already cached.
        skip_existing:    If True, skip dates already in backtest_grades.
        ignore_sentiment: If True, set all sentiment/macro/sector scores to 0.0.

    Returns list of result dicts for calibration.
    """
    sentiment_mode = "off" if ignore_sentiment else "on"
    start_date = date.fromisoformat(BACKTEST_START)
    end_date = date.fromisoformat(BACKTEST_END)

    logger.info("Loading instruments...")
    instruments = await load_instruments()
    logger.info("Found %d instruments", len(instruments))

    trading_days = get_trading_days(start_date, end_date, SAMPLE_EVERY_N_DAYS)
    logger.info(
        "Backtest grid: %d trading days from %s to %s (term=%s)",
        len(trading_days), start_date, end_date, term,
    )

    # --- Fetch missing sentiment (idempotent, resumes from cache) ---
    if fetch_sentiment:
        logger.info("Fetching missing historical sentiment (Google News + NIM)...")
        await fetch_all_historical_sentiment(instruments, start_date, end_date)

    # --- Load full sentiment cache into memory ---
    logger.info("Loading sentiment cache into memory...")
    sentiment_cache = await load_sentiment_cache()

    # --- Process each instrument ---
    all_results = []

    for instrument in instruments:
        iid = str(instrument["id"])
        symbol = instrument["symbol"]
        category = instrument["category"]
        sector = instrument["sector"]
        yf_symbol = instrument["yfinance_symbol"]

        logger.info("[%s] Loading OHLCV...", symbol)
        ohlcv_df = await get_historical_ohlcv(iid)
        if ohlcv_df.empty:
            logger.warning("[%s] No OHLCV data, skipping", symbol)
            continue

        logger.info("[%s] Fetching fundamentals history (%s)...", symbol, yf_symbol)
        fund_data = fetch_fundamentals_history(yf_symbol)

        cat_key = category.lower()
        nominal_weights = (
            COMPOSITE_WEIGHT_PROFILES
            .get(cat_key, COMPOSITE_WEIGHT_PROFILES["stock"])
            .get(term, {})
        )

        instrument_results = []
        skipped = 0

        for d in trading_days:
            if skip_existing:
                async with async_session() as session:
                    existing = await session.execute(
                        text("SELECT 1 FROM backtest_grades WHERE instrument_id=:iid AND date=:d AND term=:t AND sentiment_mode=:sm"),
                        {"iid": iid, "d": d, "t": term, "sm": sentiment_mode},
                    )
                    if existing.fetchone():
                        skipped += 1
                        continue

            # --- Technical score (exact production copy) ---
            tech_score, tech_conf = calc_technical_score(ohlcv_df, d, category, term)

            # --- Asset sentiment score (production-faithful) ---
            sent_score, sent_conf = get_asset_sentiment_for_date(symbol, d, sentiment_cache, term)

            # --- Macro sentiment score (production-faithful, replaces FRED momentum) ---
            macro_score, macro_conf = get_macro_sentiment_for_date(d, sentiment_cache, term)

            # --- Sector sentiment score (production-faithful, was hardcoded 0.0) ---
            sector_score, sector_conf = get_sector_sentiment_for_date(sector, d, sentiment_cache, term)

            if ignore_sentiment:
                sent_score, sent_conf = 0.0, 0.5
                macro_score, macro_conf = 0.0, 0.5
                sector_score, sector_conf = 0.0, 0.5

            # --- Fundamentals score + freshness confidence ---
            price_at_date = None
            price_rows = ohlcv_df[ohlcv_df.index <= pd.Timestamp(d)]
            if not price_rows.empty:
                price_at_date = float(price_rows["close"].iloc[-1])

            fund_score, fund_conf = calc_fundamentals_score_for_date(
                fund_data, d, price_at_date, sector, category, ohlcv_df, term
            )

            sub: SubScores = {
                "technical":       tech_score,
                "technical_conf":  tech_conf,
                "sentiment":       sent_score,
                "sentiment_conf":  sent_conf,
                "sector":          sector_score,
                "sector_conf":     sector_conf,
                "macro":           macro_score,
                "macro_conf":      macro_conf,
                "fundamentals":    fund_score,
                "fundamentals_conf": fund_conf,
            }

            grade = simulate_grade(sub, category, term)

            # --- Forward returns ---
            return_5d  = get_forward_return(ohlcv_df, d, 5)
            return_20d = get_forward_return(ohlcv_df, d, 20)

            await store_backtest_grade(
                iid, symbol, d, term,
                grade["overall_score"],
                tech_score, tech_conf,
                sent_score, sent_conf,
                macro_score, macro_conf,
                sector_score, sector_conf,
                fund_score, fund_conf,
                nominal_weights,
                sentiment_mode=sentiment_mode,
            )
            if return_5d is not None or return_20d is not None:
                await store_backtest_return(iid, symbol, d, return_5d, return_20d)

            row = {
                "instrument_id":   iid,
                "symbol":          symbol,
                "date":            d,
                "category":        category,
                "overall_score":   grade["overall_score"],
                "technical":       tech_score,
                "technical_conf":  tech_conf,
                "sentiment":       sent_score,
                "sentiment_conf":  sent_conf,
                "sector":          sector_score,
                "sector_conf":     sector_conf,
                "macro":           macro_score,
                "macro_conf":      macro_conf,
                "fundamentals":    fund_score,
                "fundamentals_conf": fund_conf,
                "return_5d":       return_5d,
                "return_20d":      return_20d,
            }
            instrument_results.append(row)

        logger.info(
            "[%s] Done: %d simulated, %d skipped",
            symbol, len(instrument_results), skipped,
        )
        all_results.extend(instrument_results)

    logger.info("Backtest complete. Total rows: %d", len(all_results))
    return all_results


async def load_backtest_results(term: str = "short", sentiment_mode: str = "on") -> list[dict]:
    """Load existing backtest results from DB for calibration."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT bg.instrument_id, bg.symbol, bg.date,
                       bg.overall_score,
                       bg.technical_score, bg.technical_conf,
                       bg.sentiment_score, bg.sentiment_conf,
                       bg.macro_score, bg.macro_conf,
                       bg.sector_score, bg.sector_conf,
                       bg.fundamentals_score, bg.fundamentals_conf,
                       br.return_5d, br.return_20d,
                       i.category
                FROM backtest_grades bg
                JOIN instruments i ON i.id = bg.instrument_id
                LEFT JOIN backtest_returns br
                    ON br.instrument_id = bg.instrument_id AND br.date = bg.date
                WHERE bg.term = :term AND bg.sentiment_mode = :smode
                  AND br.return_20d IS NOT NULL
                ORDER BY bg.date ASC
            """),
            {"term": term, "smode": sentiment_mode},
        )
        rows = result.fetchall()

    results = []
    for r in rows:
        results.append({
            "instrument_id":     str(r.instrument_id),
            "symbol":            r.symbol,
            "date":              r.date,
            "category":          r.category,
            "overall_score":     float(r.overall_score or 0),
            "technical":         float(r.technical_score or 0),
            "technical_conf":    float(r.technical_conf or 1.0),
            "sentiment":         float(r.sentiment_score or 0),
            "sentiment_conf":    float(r.sentiment_conf or 0.5),
            "sector":            float(r.sector_score or 0),
            "sector_conf":       float(r.sector_conf or 0.5),
            "macro":             float(r.macro_score or 0),
            "macro_conf":        float(r.macro_conf or 1.0),
            "fundamentals":      float(r.fundamentals_score or 0),
            "fundamentals_conf": float(r.fundamentals_conf or 0.7),
            "return_5d":         float(r.return_5d) if r.return_5d is not None else None,
            "return_20d":        float(r.return_20d) if r.return_20d is not None else None,
        })
    return results
