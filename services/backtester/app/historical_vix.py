"""Historical VIX data for backtester regime filtering.

Fetches ^VIX from yfinance (2020-2026+) and provides:
  1. VIX regime score: maps VIX level to [-3, 3] as a contrarian signal
  2. VIX confidence modifier: dampens overall confidence when VIX is extreme

Key finding from quant analysis: model IC drops from +0.067 (moderate vol)
to -0.004 (high vol quintile). VIX regime gating can lift IC from 0.03 to 0.05+.
"""

import logging
import os
import pickle
from datetime import date

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_VIX_CACHE_PATH = "/cache/vix_history.pkl"

# VIX level → contrarian score mapping
# Low VIX = complacency risk, High VIX = fear = contrarian buy opportunity
# But extreme VIX also means model predictions are unreliable
_VIX_SCORE_MAP = [
    # (upper_bound, score)
    (12,  -0.8),   # Extreme complacency → slightly bearish (correction risk)
    (15,  -0.3),   # Low vol, complacent → mildly bearish
    (18,   0.0),   # Normal range → neutral
    (22,   0.3),   # Slightly elevated → mild opportunity
    (25,   0.8),   # Elevated fear → good contrarian entry
    (30,   1.2),   # High fear → strong contrarian buy
    (35,   1.5),   # Very high fear → very strong contrarian
    (45,   1.0),   # Extreme crisis → still contrarian but less reliable
    (999,  0.5),   # Panic (>45) → model breaks, reduce conviction
]


def _clip(v: float, lo=-3.0, hi=3.0) -> float:
    return max(lo, min(hi, v))


def fetch_vix_history() -> pd.DataFrame:
    """Fetch full VIX history from yfinance with persistent cache.

    Returns DataFrame indexed by date with 'close' column (VIX level).
    Covers 2019-01-01 onward to ensure full backtest coverage.
    """
    if os.path.exists(_VIX_CACHE_PATH):
        try:
            with open(_VIX_CACHE_PATH, "rb") as f:
                df = pickle.load(f)
            if len(df) > 100:
                logger.info("Loaded VIX cache: %d rows (%s → %s)",
                            len(df), df.index[0].date(), df.index[-1].date())
                return df
        except Exception:
            pass

    logger.info("Fetching VIX history from yfinance (^VIX)...")
    ticker = yf.Ticker("^VIX")
    df = ticker.history(start="2019-01-01", end="2026-12-31")

    if df.empty:
        logger.warning("No VIX data returned from yfinance")
        return pd.DataFrame(columns=["close"])

    df.columns = [c.lower() for c in df.columns]
    if df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    # Keep only close price
    df = df[["close"]].copy()
    df = df.sort_index()

    os.makedirs(os.path.dirname(_VIX_CACHE_PATH), exist_ok=True)
    with open(_VIX_CACHE_PATH, "wb") as f:
        pickle.dump(df, f)
    logger.info("Cached VIX history: %d rows (%s → %s)",
                len(df), df.index[0].date(), df.index[-1].date())
    return df


def _get_vix_at_date(vix_df: pd.DataFrame, target_date: date) -> float | None:
    """Get VIX close on or before target_date."""
    if vix_df.empty:
        return None
    cutoff = pd.Timestamp(target_date)
    pos = vix_df.index.searchsorted(cutoff, side="right") - 1
    if pos < 0:
        return None
    return float(vix_df["close"].iloc[pos])


def calc_vix_score(vix_df: pd.DataFrame, target_date: date) -> tuple[float, float]:
    """Compute VIX regime score for a backtest date.

    Returns (score ∈ [-3, 3], confidence ∈ [0, 1]).
    Score is contrarian: elevated VIX = buying opportunity.
    Confidence is always high (VIX is always available and reliable).
    """
    vix_level = _get_vix_at_date(vix_df, target_date)
    if vix_level is None:
        return 0.0, 0.0

    # Map VIX level to score
    score = 0.0
    for upper, s in _VIX_SCORE_MAP:
        if vix_level <= upper:
            score = s
            break

    # Also factor in VIX rate of change (spike detection)
    # Look back 5 trading days for VIX change
    cutoff = pd.Timestamp(target_date)
    lookback = cutoff - pd.Timedelta(days=7)
    vix_window = vix_df[(vix_df.index >= lookback) & (vix_df.index <= cutoff)]

    if len(vix_window) >= 2:
        vix_start = float(vix_window["close"].iloc[0])
        vix_end = float(vix_window["close"].iloc[-1])
        if vix_start > 0:
            vix_change_pct = (vix_end - vix_start) / vix_start
            # Sharp VIX spike (>30% in a week):
            #   - If VIX < 40: contrarian buy opportunity (panic selling)
            #   - If VIX >= 40: model is broken (IC = -0.004), no signal
            # This prevents the classic contrarian trap in genuine crashes
            # where mean-reversion fails (2008, March 2020 initial leg).
            if vix_change_pct > 0.30:
                if vix_level < 40:
                    score += 0.5
                else:
                    score = 0.0  # Override: too much uncertainty
            elif vix_change_pct > 0.15:
                if vix_level < 40:
                    score += 0.2
            # VIX collapsing (>20% drop) = returning calm = risk-on confirmation
            elif vix_change_pct < -0.20:
                score += 0.3

    score = _clip(score)

    # Confidence: VIX data is always clean and available
    # Reduce confidence slightly during extreme VIX (model less reliable)
    if vix_level > 40:
        confidence = 0.7
    elif vix_level > 30:
        confidence = 0.85
    else:
        confidence = 1.0

    return round(score, 4), round(confidence, 4)


def calc_vix_confidence_modifier(vix_df: pd.DataFrame, target_date: date) -> float:
    """Get a confidence dampening factor based on VIX regime.

    Returns a multiplier ∈ [0.5, 1.0] that can be applied to overall confidence.
    When VIX > 30, the model's predictions are statistically unreliable (IC = -0.004).
    """
    vix_level = _get_vix_at_date(vix_df, target_date)
    if vix_level is None:
        return 1.0

    if vix_level <= 20:
        return 1.0       # Normal: full confidence in model
    if vix_level <= 25:
        return 0.95      # Slightly elevated: minor dampening
    if vix_level <= 30:
        return 0.85      # High vol: model starts degrading
    if vix_level <= 40:
        return 0.70      # Very high: model significantly degraded
    return 0.50           # Extreme: model essentially broken
