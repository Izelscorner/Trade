"""Historical price momentum signal for backtester.

Computes trailing return momentum from existing OHLCV data — no external API needed.
Academic IC: +0.03 to +0.05 cross-sectionally (one of the strongest known factors).

Specifically addresses model failure on momentum stocks:
  NVDA (IC = -0.16), META (-0.15), GOOGL (-0.10) — the model inverts on these
  because it lacks a trend-following signal.

Three momentum windows:
  - 1-month (20 trading days): short-term momentum / mean-reversion
  - 3-month (60 trading days): intermediate trend
  - 12-1 month (252-20 days): classic Jegadeesh-Titman momentum (skip recent month)
"""

import logging
from datetime import date

import pandas as pd

logger = logging.getLogger(__name__)


def _clip(v: float, lo=-3.0, hi=3.0) -> float:
    return max(lo, min(hi, v))


def _trailing_return(ohlcv_df: pd.DataFrame, target_date: date,
                     lookback_days: int, skip_days: int = 0) -> float | None:
    """Compute trailing return: close[t-skip] / close[t-lookback] - 1.

    Args:
        ohlcv_df: OHLCV DataFrame indexed by date.
        target_date: Reference date.
        lookback_days: Start of return window (trading days before target).
        skip_days: Skip the most recent N trading days (for 12-1mo momentum).

    Returns fractional return or None if insufficient data.
    """
    cutoff = pd.Timestamp(target_date)
    idx = ohlcv_df.index
    pos = idx.searchsorted(cutoff, side="right") - 1

    if pos < 0:
        return None

    end_pos = pos - skip_days
    start_pos = pos - lookback_days

    if start_pos < 0 or end_pos < 0 or end_pos >= len(idx):
        return None

    start_price = float(ohlcv_df["close"].iloc[start_pos])
    end_price = float(ohlcv_df["close"].iloc[end_pos])

    if start_price <= 0:
        return None

    return (end_price - start_price) / start_price


def _score_momentum(ret: float | None, term: str = "short", trailing_vol: float | None = None) -> float:
    """Map a trailing return to a momentum score in [-3, 3].

    For short-term: moderate momentum is positive (trend-following).
    For long-term: uses 12-1mo academic momentum factor.

    The dead zone scales with asset volatility when trailing_vol is provided.
    Low-vol assets (WMT, utilities) have a narrower dead zone so small but
    meaningful moves are captured. High-vol assets (NVDA, TSLA) get a wider
    dead zone to filter noise.

    Dead zone: ±max(2%, min(8%, trailing_vol × 1.5)), default ±5% if no vol data.

    Scoring is piecewise linear:
      ret < -20%  → -1.5 (strong down-trend, but contrarian dampens)
      ret -20% to -dz  → -1.0 to 0.0
      ret -dz to +dz   → 0.0 (dead zone — no signal)
      ret +dz to +20%  → 0.0 to +1.0
      ret +20% to +50% → +1.0 to +2.0
      ret > +50%        → +2.0 (capped — extreme moves less predictive)
    """
    if ret is None:
        return 0.0

    # Clamp extreme returns
    ret = max(-0.80, min(0.80, ret))

    # Volatility-scaled dead zone
    if trailing_vol is not None and trailing_vol > 0:
        dz = max(0.02, min(0.08, trailing_vol * 1.5))
    else:
        dz = 0.05

    if -dz <= ret <= dz:
        return 0.0  # Dead zone

    if ret > 0.50:
        return 2.0
    if ret > 0.20:
        return 1.0 + (ret - 0.20) / 0.30 * 1.0
    if ret > dz:
        return (ret - dz) / (0.20 - dz) * 1.0 if 0.20 > dz else 1.0

    # Negative returns
    if ret < -0.50:
        return -1.5
    if ret < -0.20:
        return -1.0 - (abs(ret) - 0.20) / 0.30 * 0.5
    # -0.20 to -dz
    return -(abs(ret) - dz) / (0.20 - dz) * 1.0 if 0.20 > dz else -1.0


def calc_momentum_score(
    ohlcv_df: pd.DataFrame,
    target_date: date,
    term: str = "short",
) -> tuple[float, float]:
    """Compute momentum score for a backtest date.

    Short-term: weighted average of 1-month and 3-month momentum.
    Long-term: 12-1 month Jegadeesh-Titman momentum (skip recent month).

    Returns (score ∈ [-3, 3], confidence ∈ [0, 1]).
    """
    if ohlcv_df.empty:
        return 0.0, 0.0

    # Compute trailing volatility for dead zone scaling
    # Uses 60-day trailing return std (annualized would be *sqrt(252) but we
    # want per-period vol to match the return magnitude)
    trailing_vol = None
    cutoff = pd.Timestamp(target_date)
    pos = ohlcv_df.index.searchsorted(cutoff, side="right") - 1
    if pos >= 20:
        close_window = ohlcv_df["close"].iloc[max(0, pos - 60):pos + 1]
        if len(close_window) >= 20:
            daily_rets = close_window.pct_change().dropna()
            if len(daily_rets) >= 10:
                # Monthly vol ≈ daily std × sqrt(20) for a ~20-day dead zone comparison
                trailing_vol = float(daily_rets.std() * (20 ** 0.5))

    if term == "short":
        # Short-term: blend 1-month (60%) + 3-month (40%)
        ret_1m = _trailing_return(ohlcv_df, target_date, lookback_days=20)
        ret_3m = _trailing_return(ohlcv_df, target_date, lookback_days=60)

        score_1m = _score_momentum(ret_1m, term, trailing_vol)
        score_3m = _score_momentum(ret_3m, term, trailing_vol)

        # Confidence based on data availability
        has_1m = ret_1m is not None
        has_3m = ret_3m is not None

        if has_1m and has_3m:
            score = score_1m * 0.6 + score_3m * 0.4
            confidence = 1.0
        elif has_1m:
            score = score_1m
            confidence = 0.7
        elif has_3m:
            score = score_3m
            confidence = 0.5
        else:
            return 0.0, 0.0

    else:
        # Long-term: 12-1 month Jegadeesh-Titman momentum
        # Skip the most recent month (reversal effect) and look back 12 months
        ret_12_1 = _trailing_return(ohlcv_df, target_date, lookback_days=252, skip_days=20)
        # Also compute 6-month momentum as supplement
        ret_6m = _trailing_return(ohlcv_df, target_date, lookback_days=126, skip_days=20)

        has_12 = ret_12_1 is not None
        has_6 = ret_6m is not None

        if has_12 and has_6:
            score = _score_momentum(ret_12_1, term, trailing_vol) * 0.6 + _score_momentum(ret_6m, term, trailing_vol) * 0.4
            confidence = 1.0
        elif has_12:
            score = _score_momentum(ret_12_1, term, trailing_vol)
            confidence = 0.8
        elif has_6:
            score = _score_momentum(ret_6m, term, trailing_vol)
            confidence = 0.6
        else:
            return 0.0, 0.0

    return round(_clip(score), 4), round(confidence, 4)
