"""Fetch FRED historical macro indicators and convert to a macro score.

For each backtest date, pulls FRED data as-of that date (no look-ahead bias)
and converts to a scalar score on [-3, 3] reflecting broad macro conditions.

Scoring logic (simplified, direction-based):
  - 10Y yield: falling yield → positive (easing), rising → negative (tightening)
  - DXY: falling dollar → positive (risk-on), rising → negative (risk-off)
  - Brent Crude: falling → positive (lower costs for most economy), rising → negative

We use trailing 30-day momentum for each indicator to get a direction signal.
"""

import logging
import math
from datetime import date, timedelta

import httpx

from .config import FRED_API_KEY

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# FRED series to fetch
MACRO_SERIES = {
    "treasury_10y": "DGS10",
    "dxy":          "DTWEXBGS",
    "brent_crude":  "DCOILBRENTEU",
}


async def _fetch_fred_range(
    series_id: str,
    start_date: date,
    end_date: date,
    client: httpx.AsyncClient,
) -> list[tuple[date, float]]:
    """Fetch FRED observations for a series between start_date and end_date.

    Returns list of (obs_date, value) tuples, sorted ascending.
    Returns empty list on any failure.
    """
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "asc",
        "observation_start": start_date.isoformat(),
        "observation_end": end_date.isoformat(),
        "limit": 1000,
    }
    try:
        resp = await client.get(FRED_BASE, params=params, timeout=20.0)
        if resp.status_code != 200:
            logger.warning("[FRED:%s] HTTP %d", series_id, resp.status_code)
            return []
        data = resp.json()
        result = []
        for obs in data.get("observations", []):
            val = obs.get("value", ".")
            if val not in (".", None, ""):
                try:
                    result.append((date.fromisoformat(obs["date"]), float(val)))
                except (ValueError, KeyError):
                    continue
        return result
    except Exception:
        logger.exception("[FRED:%s] fetch error", series_id)
        return []


async def fetch_macro_history(
    start_date: date,
    end_date: date,
    client: httpx.AsyncClient,
) -> dict[str, list[tuple[date, float]]]:
    """Fetch all macro series for the full backtest date range.

    Returns dict: {series_name: [(date, value), ...]} sorted ascending.
    Call once before the backtest loop; pass the result to calc_macro_score_for_date().
    """
    result = {}
    for name, series_id in MACRO_SERIES.items():
        observations = await _fetch_fred_range(series_id, start_date, end_date, client)
        result[name] = observations
        logger.info("[FRED] %s: %d observations", name, len(observations))
    return result


def calc_macro_score_for_date(
    macro_history: dict[str, list[tuple[date, float]]],
    target_date: date,
    lookback_days: int = 30,
) -> float:
    """Compute a macro score for target_date using trailing momentum from FRED data.

    Uses only data available up to target_date (no look-ahead).
    Returns score ∈ [-3, 3].
    """
    lookback_start = target_date - timedelta(days=lookback_days)
    scores = []

    for name, observations in macro_history.items():
        # Get values in the lookback window up to target_date
        window = [(d, v) for d, v in observations if lookback_start <= d <= target_date]
        if len(window) < 2:
            continue

        oldest_val = window[0][1]
        latest_val = window[-1][1]

        if oldest_val == 0:
            continue

        pct_change = (latest_val - oldest_val) / abs(oldest_val)

        # Direction → score contribution
        # 10Y yield: FALLING = good (easing) → positive score
        # DXY: FALLING = risk-on → positive score
        # Brent crude: FALLING = lower input costs → positive score (broadly)
        # All three: rate of change maps to score, each 2% move = 1 score point, capped at ±2
        direction_score = _clip(pct_change / 0.02, lo=-2.0, hi=2.0)

        if name == "treasury_10y":
            scores.append(-direction_score)  # inverted: falling yield = positive
        elif name == "dxy":
            scores.append(-direction_score)  # inverted: falling DXY = positive (risk-on)
        elif name == "brent_crude":
            scores.append(-direction_score)  # inverted: falling crude = positive (lower costs)

    if not scores:
        return 0.0

    raw = sum(scores) / len(scores)
    # Confidence: 1.0 if all 3 signals present, scale down otherwise
    confidence = len(scores) / 3.0
    final = _clip(raw * confidence)
    return round(final, 4)


def _clip(v: float, lo: float = -3.0, hi: float = 3.0) -> float:
    return max(lo, min(hi, v))
