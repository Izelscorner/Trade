import asyncio
import logging
import random
import time
from curl_cffi import requests as requests_cffi
import yfinance as yf

logger = logging.getLogger(__name__)


def _safe_float(val) -> float | None:
    """Convert to float, return None for invalid."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# Per-symbol cache to avoid redundant calls (Fundamentals change slowly)
_CACHE = {}  # symbol -> (timestamp, metrics)
# For price data, use a much shorter TTL
_PRICE_CACHE = {} # symbol -> (timestamp, price)
CACHE_TTL = 43200  # 12 hours
PRICE_TTL = 300    # 5 minutes for price data


def _get_price_from_info(info: dict) -> float | None:
    """Extract current price from yfinance info dict."""
    return (
        info.get("currentPrice") or 
        info.get("regularMarketPrice") or 
        info.get("navPrice") or
        info.get("priceHint") # Fallback
    )


def _fetch_yf_price(symbol: str) -> float | None:
    """Synchronous price fetch using curl_cffi."""
    now = time.time()
    if symbol in _PRICE_CACHE:
        ts, price = _PRICE_CACHE[symbol]
        if now - ts < PRICE_TTL:
            return price

    try:
        with requests_cffi.Session(impersonate="chrome") as session:
            ticker = yf.Ticker(symbol, session=session)
            # Use fast_info if possible as it is lighter, but info is more reliable for some fields
            info = ticker.info
            price = _get_price_from_info(info)
            if price:
                _PRICE_CACHE[symbol] = (now, price)
                return price
            return None
    except Exception:
        return None


def _fetch_yf_info(symbol: str) -> dict | None:
    """Synchronous yfinance fetch using curl_cffi for browser impersonation."""
    now = time.time()
    if symbol in _CACHE:
        ts, data = _CACHE[symbol]
        if now - ts < CACHE_TTL:
            return data

    try:
        # Use curl_cffi to mimic a real browser TLS fingerprint
        # This is a key solution for recent yfinance 429 errors
        with requests_cffi.Session(impersonate="chrome") as session:
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            
            if not info or not isinstance(info, dict):
                return None
            
            _CACHE[symbol] = (now, info)
            return info
    except Exception as e:
        err_str = str(e).lower()
        if "rate limit" in err_str or "429" in err_str or "too many requests" in err_str:
            logger.warning("[%s] yfinance rate limited (429)", symbol)
            return None
        logger.exception("[%s] yfinance fetch error", symbol)
        return None


async def fetch_ticker_price(symbol: str) -> float | None:
    """Fetch current price for a ticker using yfinance."""
    return await asyncio.to_thread(_fetch_yf_price, symbol)


async def fetch_ratios(symbol: str) -> dict | None:
    """Fetch key financial ratios for a symbol using yfinance.

    Returns dict with pe_ratio, roe, de_ratio, peg_ratio or None on failure.
    """
    info = await asyncio.to_thread(_fetch_yf_info, symbol)
    if not info:
        logger.info("[%s] No yfinance data available", symbol)
        return None

    result = {
        "pe_ratio": _safe_float(info.get("trailingPE")),
        "roe": _safe_float(info.get("returnOnEquity")),
        "de_ratio": _safe_float(info.get("debtToEquity")),
        "peg_ratio": _safe_float(info.get("pegRatio") or info.get("trailingPegRatio")),
        # Revenue growth (YoY) for negative P/E nuance scoring.
        # Yahoo Finance returns this as a decimal: 0.25 = 25% growth.
        "revenue_growth": _safe_float(info.get("revenueGrowth")),
    }

    # If PEG is still missing, search for anything with 'peg' in it
    if result["peg_ratio"] is None:
        peg_keys = [k for k in info.keys() if "peg" in k.lower()]
        if peg_keys:
            logger.debug("[%s] Potential PEG keys found: %s", symbol, peg_keys)
            for pk in peg_keys:
                val = _safe_float(info.get(pk))
                if val is not None:
                    result["peg_ratio"] = val
                    break

    # Need at least one valid metric
    if all(v is None for v in result.values()):
        logger.info("[%s] All yfinance metrics are null/zero", symbol)
        return None

    logger.info(
        "[%s] yfinance ratios: P/E=%.2f ROE=%.4f D/E=%.2f PEG=%.2f",
        symbol,
        result["pe_ratio"] or 0,
        result["roe"] or 0,
        result["de_ratio"] or 0,
        result["peg_ratio"] or 0,
    )
    return result
