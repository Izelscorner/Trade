"""Backtest engine: run retroactive grade simulation over a date grid.

Pipeline:
1. Load historical OHLCV from DB for all instruments.
2. Load historical fundamental data from yfinance once per instrument.
3. Load article-level sentiment cache (asset + macro + sector) from backtest_articles.
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
import yfinance as yf
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
from .historical_tech import calc_technical_score, get_historical_ohlcv, precompute_indicators
from .simulator import COMPOSITE_WEIGHT_PROFILES, SubScores, simulate_grade

logger = logging.getLogger(__name__)


async def load_instruments(extended: bool = False) -> list[dict]:
    """Load all instruments from DB (all 50 are seeded in 01_init.sql).

    Args:
        extended: Accepted for API compatibility but ignored — all instruments
                  share the same table.
    """
    async with async_session() as session:
        result = await session.execute(
            text(
                "SELECT id, symbol, name, category, sector, yfinance_symbol "
                "FROM instruments WHERE is_active = true ORDER BY symbol"
            )
        )
        return [dict(r._mapping) for r in result.fetchall()]


_GRADE_INSERT_SQL = text("""
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
""")

_RETURN_INSERT_SQL = text("""
    INSERT INTO backtest_returns (instrument_id, symbol, date, return_5d, return_20d)
    VALUES (:iid, :sym, :date, :r5, :r20)
    ON CONFLICT (instrument_id, date) DO UPDATE
      SET return_5d  = EXCLUDED.return_5d,
          return_20d = EXCLUDED.return_20d
""")

BATCH_SIZE = 500  # Flush every N rows


async def flush_batch(grade_batch: list[dict], return_batch: list[dict]) -> None:
    """Flush accumulated grade and return rows to DB in a single transaction."""
    if not grade_batch and not return_batch:
        return
    async with async_session() as session:
        for params in grade_batch:
            await session.execute(_GRADE_INSERT_SQL, params)
        for params in return_batch:
            await session.execute(_RETURN_INSERT_SQL, params)
        await session.commit()


def make_grade_params(
    instrument_id: str, symbol: str, d: date, term: str,
    overall_score: float,
    technical_score: float, technical_conf: float,
    sentiment_score: float, sentiment_conf: float,
    macro_score: float, macro_conf: float,
    sector_score: float, sector_conf: float,
    fundamentals_score: float, fundamentals_conf: float,
    weights: dict, sentiment_mode: str = "on",
) -> dict:
    return {
        "iid": instrument_id, "sym": symbol, "date": d, "term": term,
        "overall": overall_score,
        "tech": technical_score, "tech_conf": technical_conf,
        "sent": sentiment_score, "sent_conf": sentiment_conf,
        "macro": macro_score, "macro_conf": macro_conf,
        "sector": sector_score, "sec_conf": sector_conf,
        "fund": fundamentals_score, "fund_conf": fundamentals_conf,
        "weights": str(weights).replace("'", '"'),
        "smode": sentiment_mode,
    }


def make_return_params(
    instrument_id: str, symbol: str, d: date,
    return_5d: float | None, return_20d: float | None,
) -> dict:
    return {"iid": instrument_id, "sym": symbol, "date": d, "r5": return_5d, "r20": return_20d}


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
    """Get forward close-to-close return from date d over horizon_days trading days.

    Uses searchsorted for O(log n) date lookup instead of full index scan.
    """
    cutoff = pd.Timestamp(d)
    idx = ohlcv_df.index
    pos = idx.searchsorted(cutoff, side="right") - 1
    if pos < 0:
        return None
    future_pos = pos + horizon_days
    if future_pos >= len(idx):
        return None
    start_close = float(ohlcv_df["close"].iloc[pos])
    end_close = float(ohlcv_df["close"].iloc[future_pos])
    if start_close == 0:
        return None
    return round((end_close - start_close) / start_close, 6)


async def run_backtest(
    fetch_sentiment: bool = True,
    skip_existing: bool = True,
) -> int:
    """Run the full backtest over all instruments and trading days.

    Computes all 4 variations (short/long × with/without sentiment) in one pass
    per instrument for efficiency. Sub-scores are computed once per term, then
    stored for both sentiment modes.

    Args:
        fetch_sentiment:  If True, fetch any missing sentiment from Google News + NIM.
                          If False, use only what's already cached.
        skip_existing:    If True, skip dates where all 4 grades already exist.

    Returns total number of grade rows stored.
    """
    TERMS = ("short", "long")
    MODES = ("on", "off")

    start_date = date.fromisoformat(BACKTEST_START)
    end_date = date.fromisoformat(BACKTEST_END)

    logger.info("Loading instruments...")
    instruments = await load_instruments()
    logger.info("Found %d instruments", len(instruments))

    trading_days = get_trading_days(start_date, end_date, SAMPLE_EVERY_N_DAYS)
    logger.info(
        "Backtest grid: %d trading days × %d instruments × 4 variations (short/long × sentiment on/off)",
        len(trading_days), len(instruments),
    )

    # --- Fetch missing sentiment (idempotent, resumes from cache) ---
    if fetch_sentiment:
        logger.info("Fetching missing historical sentiment (Google News + NIM)...")
        await fetch_all_historical_sentiment(instruments, start_date, end_date)

    # --- Load full sentiment cache into memory ---
    logger.info("Loading sentiment cache into memory...")
    sentiment_cache = await load_sentiment_cache()

    # --- Process each instrument ---
    total_stored = 0

    for instrument in instruments:
        iid = str(instrument["id"])
        symbol = instrument["symbol"]
        category = instrument["category"]
        sector = instrument["sector"]
        yf_symbol = instrument["yfinance_symbol"]

        logger.info("[%s] Loading OHLCV...", symbol)
        ohlcv_df = await get_historical_ohlcv(iid, yfinance_symbol=yf_symbol)
        # If DB data doesn't cover the backtest range, supplement with yfinance
        if not ohlcv_df.empty:
            earliest_data = ohlcv_df.index[0].date()
            if earliest_data > start_date:
                logger.info("[%s] DB data starts %s, fetching full history from yfinance...", symbol, earliest_data)
                yf_df = await get_historical_ohlcv(iid, yfinance_symbol=yf_symbol, use_yfinance=True)
                if not yf_df.empty:
                    ohlcv_df = yf_df
        if ohlcv_df.empty:
            logger.warning("[%s] No OHLCV data, skipping", symbol)
            continue
        # Ensure timezone-naive index (yfinance parquet cache may have tz)
        if ohlcv_df.index.tz is not None:
            ohlcv_df.index = ohlcv_df.index.tz_localize(None)

        logger.info("[%s] Fetching fundamentals history (%s)...", symbol, yf_symbol)
        fund_data = fetch_fundamentals_history(yf_symbol, ticker_symbol=symbol)
        # For GOLD: pass DXY data for inverse adjustment (production scorer.py lines 871-888)
        if symbol == "GOLD":
            fund_data["is_gold"] = True
            if "dxy_df" not in fund_data:
                try:
                    dxy_ticker = yf.Ticker("DX-Y.NYB")
                    dxy_df = dxy_ticker.history(period="max")
                    if not dxy_df.empty:
                        dxy_df.columns = [c.lower() for c in dxy_df.columns]
                        if dxy_df.index.tz is not None:
                            dxy_df.index = dxy_df.index.tz_localize(None)
                        fund_data["dxy_df"] = dxy_df
                        logger.info("[GOLD] DXY data loaded: %d rows", len(dxy_df))
                except Exception as e:
                    logger.warning("[GOLD] Failed to fetch DXY data: %s", e)

        cat_key = category.lower()

        # Pre-compute all technical indicators once (vectorized, ~1000x faster)
        logger.info("[%s] Pre-computing technical indicators...", symbol)
        precomputed = precompute_indicators(ohlcv_df)

        # Batch-load existing grades for efficient skip checking
        existing_grades: set[tuple] = set()
        if skip_existing:
            async with async_session() as session:
                result = await session.execute(
                    text("SELECT date, term, sentiment_mode FROM backtest_grades WHERE instrument_id=:iid"),
                    {"iid": iid},
                )
                existing_grades = {(r.date, r.term, r.sentiment_mode) for r in result.fetchall()}

        stored = 0
        skipped = 0
        grade_batch: list[dict] = []
        return_batch: list[dict] = []

        for d in trading_days:
            # Skip if all 4 combos already exist
            if skip_existing and all((d, t, m) in existing_grades for t in TERMS for m in MODES):
                skipped += 1
                continue

            # Price at date (shared across terms) — use searchsorted for speed
            cutoff_ts = pd.Timestamp(d)
            pos = ohlcv_df.index.searchsorted(cutoff_ts, side="right") - 1
            price_at_date = float(ohlcv_df["close"].iloc[pos]) if pos >= 0 else None

            # Forward returns (stored once per instrument+date, shared across variations)
            return_5d = get_forward_return(ohlcv_df, d, 5)
            return_20d = get_forward_return(ohlcv_df, d, 20)
            if return_5d is not None or return_20d is not None:
                return_batch.append(make_return_params(iid, symbol, d, return_5d, return_20d))

            for term in TERMS:
                # Skip if both modes already exist for this term
                if skip_existing and (d, term, "on") in existing_grades and (d, term, "off") in existing_grades:
                    continue

                nominal_weights = (
                    COMPOSITE_WEIGHT_PROFILES
                    .get(cat_key, COMPOSITE_WEIGHT_PROFILES["stock"])
                    .get(term, {})
                )

                # Compute sub-scores once per term
                tech_score, tech_conf = calc_technical_score(ohlcv_df, d, category, term, precomputed=precomputed)
                sent_score, sent_conf = get_asset_sentiment_for_date(symbol, d, sentiment_cache, term)
                macro_score, macro_conf = get_macro_sentiment_for_date(d, sentiment_cache, term)
                sector_score, sector_conf = get_sector_sentiment_for_date(sector, d, sentiment_cache, term)
                fund_score, fund_conf = calc_fundamentals_score_for_date(
                    fund_data, d, price_at_date, sector, category, ohlcv_df, term
                )

                # --- With sentiment (mode="on") ---
                if not (skip_existing and (d, term, "on") in existing_grades):
                    sub: SubScores = {
                        "technical": tech_score, "technical_conf": tech_conf,
                        "sentiment": sent_score, "sentiment_conf": sent_conf,
                        "sector": sector_score, "sector_conf": sector_conf,
                        "macro": macro_score, "macro_conf": macro_conf,
                        "fundamentals": fund_score, "fundamentals_conf": fund_conf,
                    }
                    grade = simulate_grade(sub, category, term)
                    grade_batch.append(make_grade_params(
                        iid, symbol, d, term, grade["overall_score"],
                        tech_score, tech_conf, sent_score, sent_conf,
                        macro_score, macro_conf, sector_score, sector_conf,
                        fund_score, fund_conf, nominal_weights, sentiment_mode="on",
                    ))
                    stored += 1

                # --- Without sentiment (mode="off") ---
                # Mirrors production scorer.py "pure score" branch: force
                # sentiment/sector/macro confidence to 0.0 so their effective
                # weight collapses to nominal × 0.1 (the floor).  This must
                # match what mode="on" produces when the sentiment cache is
                # empty (also conf=0.0), ensuring identical results when no
                # sentiment data exists.
                if not (skip_existing and (d, term, "off") in existing_grades):
                    sub_off: SubScores = {
                        "technical": tech_score, "technical_conf": tech_conf,
                        "sentiment": 0.0, "sentiment_conf": 0.0,
                        "sector": 0.0, "sector_conf": 0.0,
                        "macro": 0.0, "macro_conf": 0.0,
                        "fundamentals": fund_score, "fundamentals_conf": fund_conf,
                    }
                    grade_off = simulate_grade(sub_off, category, term)
                    grade_batch.append(make_grade_params(
                        iid, symbol, d, term, grade_off["overall_score"],
                        tech_score, tech_conf, 0.0, 0.0,
                        0.0, 0.0, 0.0, 0.0,
                        fund_score, fund_conf, nominal_weights, sentiment_mode="off",
                    ))
                    stored += 1

            # Flush batch periodically
            if len(grade_batch) >= BATCH_SIZE:
                await flush_batch(grade_batch, return_batch)
                grade_batch.clear()
                return_batch.clear()

        # Flush remaining
        await flush_batch(grade_batch, return_batch)
        grade_batch.clear()
        return_batch.clear()

        logger.info("[%s] Done: %d grades stored, %d dates fully skipped", symbol, stored, skipped)
        total_stored += stored

    logger.info("Backtest complete. Total grades stored: %d", total_stored)
    return total_stored


async def load_backtest_results(term: str = "short", sentiment_mode: str = "on") -> list[dict]:
    """Load existing backtest results from DB for calibration."""
    all_instruments = await load_instruments()
    sym_to_cat = {inst["symbol"]: inst["category"] for inst in all_instruments}

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
                       br.return_5d, br.return_20d
                FROM backtest_grades bg
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
        category = sym_to_cat.get(r.symbol, "Stock")
        results.append({
            "instrument_id":     str(r.instrument_id),
            "symbol":            r.symbol,
            "date":              r.date,
            "category":          category,
            "overall_score":     float(r.overall_score) if r.overall_score is not None else 0.0,
            "technical":         float(r.technical_score) if r.technical_score is not None else 0.0,
            "technical_conf":    float(r.technical_conf) if r.technical_conf is not None else 1.0,
            "sentiment":         float(r.sentiment_score) if r.sentiment_score is not None else 0.0,
            "sentiment_conf":    float(r.sentiment_conf) if r.sentiment_conf is not None else 0.0,
            "sector":            float(r.sector_score) if r.sector_score is not None else 0.0,
            "sector_conf":       float(r.sector_conf) if r.sector_conf is not None else 0.0,
            "macro":             float(r.macro_score) if r.macro_score is not None else 0.0,
            "macro_conf":        float(r.macro_conf) if r.macro_conf is not None else 0.0,
            "fundamentals":      float(r.fundamentals_score) if r.fundamentals_score is not None else 0.0,
            "fundamentals_conf": float(r.fundamentals_conf) if r.fundamentals_conf is not None else 0.0,
            "return_5d":         float(r.return_5d) if r.return_5d is not None else None,
            "return_20d":        float(r.return_20d) if r.return_20d is not None else None,
        })
    return results
