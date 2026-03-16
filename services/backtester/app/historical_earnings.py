"""Historical earnings calendar for backtester.

Fetches earnings dates from yfinance and creates a binary avoidance signal.
From the quant analysis: 4,400 extreme-move events (>10% in 20 days) around
earnings have IC = 0.006 — essentially random. Skipping these improves Sharpe.

Implementation:
  - Fetch earnings dates from yfinance (covers ~3 years)
  - Supplement with SEC EDGAR quarterly filing dates (covers 6+ years)
  - For each backtest date, check if within ±3 trading days of an earnings report
  - Return a confidence dampener: 0.0 near earnings (skip), 1.0 otherwise
"""

import logging
import os
import pickle
from datetime import date, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_EARNINGS_CACHE_DIR = "/cache/earnings"
_FUND_CACHE_DIR = "/cache/fundamentals"

# Number of trading days before/after earnings to flag
_EARNINGS_BUFFER_DAYS = 3


# Sector-specific earnings report lag from quarter-end (calendar days).
# Large-cap tech reports ~25-30d after quarter end, healthcare ~35d,
# financials ~30d, industrials ~50d. A fixed +45d offset is wrong for
# tech stocks (NVDA, AAPL, GOOGL) where the actual window is 2-3 weeks earlier.
_SECTOR_EARNINGS_LAG = {
    "technology": 28,
    "communication": 28,
    "financials": 30,
    "consumer_discretionary": 35,
    "healthcare": 35,
    "consumer_staples": 40,
    "energy": 40,
    "industrials": 50,
    "materials": 45,
    "utilities": 45,
    "real_estate": 45,
}
_DEFAULT_EARNINGS_LAG = 45


def _extract_earnings_from_fundamentals_cache(
    yf_symbol: str,
    sector: str | None = None,
) -> list[date]:
    """Extract approximate earnings dates from cached fundamentals data.

    SEC EDGAR quarterly income statement dates correspond to quarter-end dates.
    Earnings are typically reported after a sector-specific lag.
    Tech (NVDA, AAPL) ~28d, industrials (RTX) ~50d, default 45d.

    This provides 6+ years of coverage (18+ quarters from EDGAR).
    """
    safe_name = yf_symbol.replace("=", "_").replace("/", "_")
    cache_path = os.path.join(_FUND_CACHE_DIR, f"{safe_name}.pkl")

    if not os.path.exists(cache_path):
        return []

    try:
        with open(cache_path, "rb") as f:
            data = pickle.load(f)

        income = data.get("income")
        if income is None or income.empty:
            return []

        lag_days = _SECTOR_EARNINGS_LAG.get(
            (sector or "").lower(), _DEFAULT_EARNINGS_LAG
        )

        # Income statement columns are quarter-end dates
        quarter_dates = pd.to_datetime(income.columns)
        earnings_dates = [(d + pd.Timedelta(days=lag_days)).date() for d in quarter_dates]
        return sorted(set(earnings_dates))

    except Exception:
        return []


def fetch_earnings_dates(yf_symbol: str, ticker_symbol: str | None = None) -> list[date]:
    """Fetch historical earnings dates for an instrument.

    Sources (merged and deduplicated):
      1. yfinance earnings_dates (~12 dates, 3 years)
      2. SEC EDGAR quarterly filing dates (~18+ dates, 6+ years, estimated +45d lag)

    Args:
        yf_symbol: yfinance symbol (e.g. "AAPL", "GC=F")
        ticker_symbol: Human-readable symbol for logging

    Returns sorted list of earnings dates covering the full backtest range.
    """
    display_sym = ticker_symbol or yf_symbol
    os.makedirs(_EARNINGS_CACHE_DIR, exist_ok=True)
    safe_name = yf_symbol.replace("=", "_").replace("/", "_")
    cache_path = os.path.join(_EARNINGS_CACHE_DIR, f"{safe_name}.pkl")

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                dates = pickle.load(f)
            if dates:
                logger.info("[%s] Loaded %d earnings dates from cache (%s → %s)",
                            display_sym, len(dates), dates[0], dates[-1])
                return dates
        except Exception:
            pass

    # Commodities and some ETFs don't have earnings
    if "=" in yf_symbol or yf_symbol.endswith(".L"):
        logger.info("[%s] No earnings dates (commodity/non-US ETF)", display_sym)
        with open(cache_path, "wb") as f:
            pickle.dump([], f)
        return []

    all_dates: set[date] = set()

    # Source 1: yfinance earnings_dates (~3 years of actual report dates)
    try:
        ticker = yf.Ticker(yf_symbol)
        earnings_df = ticker.earnings_dates
        if earnings_df is not None and not earnings_df.empty:
            yf_dates = [d.date() for d in earnings_df.index]
            all_dates.update(yf_dates)
            logger.info("[%s] yfinance: %d earnings dates", display_sym, len(yf_dates))
    except Exception as e:
        logger.debug("[%s] yfinance earnings_dates failed: %s", display_sym, e)

    # Source 2: EDGAR quarterly filing dates (estimated, 6+ years)
    edgar_dates = _extract_earnings_from_fundamentals_cache(yf_symbol)
    if edgar_dates:
        # Deduplicate: if an EDGAR-estimated date is within 10 days of a yfinance date,
        # prefer the yfinance date (it's the actual report date)
        for ed in edgar_dates:
            is_dup = any(abs((ed - yd).days) < 10 for yd in all_dates)
            if not is_dup:
                all_dates.add(ed)
        logger.info("[%s] EDGAR: %d additional quarterly dates", display_sym, len(edgar_dates))

    dates = sorted(all_dates)

    if dates:
        logger.info("[%s] Total earnings dates: %d (%s → %s)",
                    display_sym, len(dates), dates[0], dates[-1])
    else:
        logger.info("[%s] No earnings data available", display_sym)

    with open(cache_path, "wb") as f:
        pickle.dump(dates, f)
    return dates


def _is_near_earnings(
    target_date: date,
    earnings_dates: list[date],
    buffer_days: int = _EARNINGS_BUFFER_DAYS,
    trading_days: list[date] | None = None,
) -> bool:
    """Check if target_date is within ±buffer_days trading days of any earnings date.

    When a trading_days calendar is provided, counts actual trading days for
    precision (important for tech earnings where the window is tight).
    Falls back to a calendar-day heuristic when no calendar is available.
    """
    if not earnings_dates:
        return False

    if trading_days is not None:
        # Use actual trading day counting for precision
        td_set = set(trading_days)
        try:
            td_sorted = sorted(trading_days)
            target_idx = None
            for i, d in enumerate(td_sorted):
                if d >= target_date:
                    target_idx = i
                    break
            if target_idx is None:
                target_idx = len(td_sorted) - 1

            for ed in earnings_dates:
                ed_idx = None
                for i, d in enumerate(td_sorted):
                    if d >= ed:
                        ed_idx = i
                        break
                if ed_idx is None:
                    ed_idx = len(td_sorted) - 1
                if abs(target_idx - ed_idx) <= buffer_days:
                    return True
            return False
        except Exception:
            pass  # Fall through to calendar heuristic

    # Fallback: calendar day heuristic
    # 3 trading days ≈ 5 calendar days (accounts for weekends)
    cal_buffer = int(buffer_days * 7 / 5) + 1
    for ed in earnings_dates:
        diff = abs((target_date - ed).days)
        if diff <= cal_buffer:
            return True
    return False


def calc_earnings_score(
    target_date: date,
    earnings_dates: list[date],
    category: str = "stock",
) -> tuple[float, float]:
    """Compute earnings proximity score for a backtest date.

    This signal acts as a confidence dampener rather than a directional signal.
    Near earnings: score = 0 (neutral), confidence = 0.0 (dampens overall grade).
    Away from earnings: score = 0 (neutral), confidence = 1.0 (no effect).

    Commodities have no earnings → always returns (0.0, 1.0).

    Returns (score ∈ [-3, 3], confidence ∈ [0, 1]).
    """
    if category == "commodity":
        return 0.0, 1.0

    if not earnings_dates:
        # No earnings data → assume stock, but don't penalize (no data)
        return 0.0, 0.8

    near = _is_near_earnings(target_date, earnings_dates)

    if near:
        # Near earnings: score is neutral, confidence drops to signal
        # "don't trust the model right now"
        return 0.0, 0.0
    else:
        return 0.0, 1.0


def calc_earnings_confidence_modifier(
    target_date: date,
    earnings_dates: list[date],
    category: str = "stock",
) -> float:
    """Get a multiplier for overall confidence based on earnings proximity.

    Returns:
        1.0 if away from earnings (proceed normally)
        0.3 if within ±3 days of earnings (heavily dampen but don't zero out)
        1.0 for commodities (no earnings)
    """
    if category == "commodity":
        return 1.0

    if not earnings_dates:
        return 1.0

    if _is_near_earnings(target_date, earnings_dates):
        return 0.3  # Heavily dampen but don't completely skip

    return 1.0
