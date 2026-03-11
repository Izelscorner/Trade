"""FMP API client for fundamental metrics (P/E, ROE, D/E, PEG).

Uses the FMP Stable API endpoints:
  - /stable/ratios   → P/E, D/E, PEG
  - /stable/key-metrics → ROE
"""

import logging

import httpx

logger = logging.getLogger(__name__)

FMP_STABLE = "https://financialmodelingprep.com/stable"


def _safe_float(val) -> float | None:
    """Convert to float, return None for 0.0/invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0.0 else None
    except (ValueError, TypeError):
        return None


async def _fetch_json(
    url: str, params: dict, client: httpx.AsyncClient, symbol: str
) -> dict | None:
    """Fetch a single JSON object from FMP stable API."""
    try:
        resp = await client.get(url, params=params, timeout=15.0)
        if resp.status_code == 429:
            logger.warning("[%s] FMP rate limited (429)", symbol)
            return None
        if resp.status_code != 200:
            logger.warning("[%s] FMP HTTP %d for %s", symbol, resp.status_code, url)
            return None
        data = resp.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        return data[0]
    except httpx.TimeoutException:
        logger.warning("[%s] FMP request timed out", symbol)
        return None
    except Exception:
        logger.exception("[%s] FMP fetch error", symbol)
        return None


async def fetch_ratios(
    symbol: str, api_key: str, client: httpx.AsyncClient
) -> dict | None:
    """Fetch key financial ratios for a symbol from FMP Stable API.

    Uses 2 API calls per symbol:
      1. /stable/ratios → priceToEarningsRatio, debtToEquityRatio, priceToEarningsGrowthRatio
      2. /stable/key-metrics → returnOnEquity

    Returns dict with pe_ratio, roe, de_ratio, peg_ratio or None on failure.
    """
    params = {"symbol": symbol, "apikey": api_key}

    # Fetch ratios (P/E, D/E, PEG)
    ratios = await _fetch_json(f"{FMP_STABLE}/ratios", params, client, symbol)

    # Fetch key-metrics (ROE)
    key_metrics = await _fetch_json(f"{FMP_STABLE}/key-metrics", params, client, symbol)

    if not ratios and not key_metrics:
        logger.info("[%s] No FMP data available", symbol)
        return None

    result = {
        "pe_ratio": _safe_float(ratios.get("priceToEarningsRatio")) if ratios else None,
        "roe": _safe_float(key_metrics.get("returnOnEquity")) if key_metrics else None,
        "de_ratio": _safe_float(ratios.get("debtToEquityRatio")) if ratios else None,
        "peg_ratio": _safe_float(ratios.get("priceToEarningsGrowthRatio")) if ratios else None,
    }

    # Need at least one valid metric
    if all(v is None for v in result.values()):
        logger.info("[%s] All FMP metrics are null/zero", symbol)
        return None

    logger.info(
        "[%s] FMP ratios: P/E=%.2f ROE=%.4f D/E=%.2f PEG=%.2f",
        symbol,
        result["pe_ratio"] or 0,
        result["roe"] or 0,
        result["de_ratio"] or 0,
        result["peg_ratio"] or 0,
    )
    return result
