"""FRED API client for macro economic indicators.

Fetches: DXY (US Dollar Index), 10-Year Treasury Yield, Global GDP Growth, Brent Crude Oil.

FRED Series IDs:
  - DTWEXBGS: Trade Weighted U.S. Dollar Index (Broad, Goods and Services) — proxy for DXY
  - DGS10: 10-Year Treasury Constant Maturity Rate
  - NYGDPPCAPKDWLD: World GDP per capita growth (annual, lagging) — proxy for global GDP growth
  - DCOILBRENTEU: Brent Crude Oil Price (daily)
"""

import logging
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

# Series mapping: our name → FRED series ID
MACRO_SERIES = {
    "dxy": "DTWEXBGS",
    "treasury_10y": "DGS10",
    "gdp_growth": "A191RL1Q225SBEA",
    "brent_crude": "DCOILBRENTEU",
}

# Human-readable labels
MACRO_LABELS = {
    "dxy": "US Dollar Index (DXY)",
    "treasury_10y": "10-Year Treasury Yield",
    "gdp_growth": "Global GDP Growth",
    "brent_crude": "Brent Crude Oil",
}

# Units for display
MACRO_UNITS = {
    "dxy": "index",
    "treasury_10y": "%",
    "gdp_growth": "%",
    "brent_crude": "$/bbl",
}


async def fetch_fred_series(
    series_id: str, api_key: str, client: httpx.AsyncClient, lookback_days: int = 30
) -> float | None:
    """Fetch the latest observation for a FRED series.

    Returns the most recent non-null value or None on failure.
    """
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    # GDP series is quarterly — need larger lookback
    effective_lookback = 365 if series_id == "A191RL1Q225SBEA" else lookback_days
    start_date = (datetime.utcnow() - timedelta(days=effective_lookback)).strftime("%Y-%m-%d")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 10,
        "observation_start": start_date,
        "observation_end": end_date,
    }

    try:
        resp = await client.get(FRED_BASE, params=params, timeout=15.0)

        if resp.status_code == 429:
            logger.warning("[FRED:%s] Rate limited (429)", series_id)
            return None
        if resp.status_code != 200:
            logger.warning("[FRED:%s] HTTP %d", series_id, resp.status_code)
            return None

        data = resp.json()
        observations = data.get("observations", [])

        # Find first non-null observation (FRED uses "." for missing)
        for obs in observations:
            val = obs.get("value", ".")
            if val != "." and val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue

        logger.info("[FRED:%s] No valid observations found", series_id)
        return None

    except httpx.TimeoutException:
        logger.warning("[FRED:%s] Request timed out", series_id)
        return None
    except Exception:
        logger.exception("[FRED:%s] Fetch error", series_id)
        return None


async def fetch_all_macro_indicators(
    api_key: str, client: httpx.AsyncClient
) -> dict[str, float | None]:
    """Fetch all 4 macro indicators from FRED.

    Returns dict: {dxy: float|None, treasury_10y: float|None, gdp_growth: float|None, brent_crude: float|None}
    """
    results = {}
    for name, series_id in MACRO_SERIES.items():
        val = await fetch_fred_series(series_id, api_key, client)
        results[name] = val
        if val is not None:
            logger.info("[FRED] %s = %.4f", MACRO_LABELS[name], val)
        else:
            logger.warning("[FRED] %s = no data", MACRO_LABELS[name])

    return results
