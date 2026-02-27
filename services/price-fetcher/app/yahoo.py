"""Direct Yahoo Finance API client — bypasses yfinance library issues in Docker."""

import json
import logging
import urllib.request
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BASE_URL = "https://query2.finance.yahoo.com/v8/finance/chart"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def fetch_chart(symbol: str, period: str = "5d", interval: str = "1d") -> dict | None:
    """Fetch chart data from Yahoo Finance API directly."""
    url = f"{BASE_URL}/{symbol}?range={period}&interval={interval}"
    req = urllib.request.Request(url, headers=HEADERS)

    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
    except Exception:
        logger.exception("[%s] Failed to fetch from Yahoo Finance API", symbol)
        return None

    result = data.get("chart", {}).get("result")
    if not result:
        error = data.get("chart", {}).get("error")
        if error:
            logger.warning("[%s] Yahoo API error: %s", symbol, error.get("description", error))
        return None

    return result[0]


def parse_historical(chart_data: dict) -> list[dict]:
    """Parse chart data into historical price rows."""
    timestamps = chart_data.get("timestamp", [])
    quotes = chart_data.get("indicators", {}).get("quote", [{}])[0]

    opens = quotes.get("open", [])
    highs = quotes.get("high", [])
    lows = quotes.get("low", [])
    closes = quotes.get("close", [])
    volumes = quotes.get("volume", [])

    rows = []
    for i, ts in enumerate(timestamps):
        close = closes[i] if i < len(closes) else None
        if close is None:
            continue  # Skip days with no data

        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        rows.append({
            "date": dt.date(),
            "open": float(opens[i]) if opens[i] is not None else float(close),
            "high": float(highs[i]) if highs[i] is not None else float(close),
            "low": float(lows[i]) if lows[i] is not None else float(close),
            "close": float(close),
            "volume": int(volumes[i]) if volumes[i] is not None else 0,
        })

    return rows


def parse_live_price(chart_data: dict) -> dict | None:
    """Parse chart data into a live price dict."""
    meta = chart_data.get("meta", {})
    price = meta.get("regularMarketPrice")
    prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")

    if price is None:
        return None

    # Determine market status
    state = meta.get("currentTradingPeriod", {})
    market_status = "closed"
    now_ts = datetime.now(timezone.utc).timestamp()

    regular = state.get("regular", {})
    pre = state.get("pre", {})
    post = state.get("post", {})

    if regular.get("start", 0) <= now_ts <= regular.get("end", 0):
        market_status = "active"
    elif pre.get("start", 0) <= now_ts <= pre.get("end", 0):
        market_status = "pre_market"
    elif post.get("start", 0) <= now_ts <= post.get("end", 0):
        market_status = "after_hours"

    change_amount = None
    change_percent = None
    if prev_close and prev_close > 0:
        change_amount = price - prev_close
        change_percent = (change_amount / prev_close) * 100

    return {
        "price": float(price),
        "prev_close": float(prev_close) if prev_close else None,
        "change_amount": change_amount,
        "change_percent": change_percent,
        "market_status": market_status,
    }
