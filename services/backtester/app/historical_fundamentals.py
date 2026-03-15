"""Reconstruct historical fundamental metrics from yfinance quarterly data.

For each backtest date, uses only the most recent quarterly filing available
before that date (no look-ahead bias).

Metrics: P/E, ROE, D/E (debt-to-equity), PEG.

P/E is computed as: price[date] / trailing_EPS_from_quarterly_income_stmt
ROE = net_income_ttm / avg_book_equity
D/E = total_debt / total_equity
PEG = (P/E) / EPS_growth_rate_yoy
"""

import logging
from datetime import date

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# The scorer's metric scoring functions (duplicated from scorer.py for self-containment)
_SECTOR_PE_THRESHOLDS = {
    "technology":            (10, 18, 25, 38, 55, 80),
    "communication":         (10, 16, 22, 35, 50, 75),
    "consumer_discretionary": (8, 15, 20, 35, 55, 80),
    "healthcare":            (10, 16, 22, 35, 50, 70),
    "financials":            (5,  8, 12, 18, 25, 40),
    "industrials":           (8, 13, 18, 28, 40, 60),
    "consumer_staples":      (8, 13, 18, 25, 35, 50),
    "energy":                (5,  8, 12, 20, 30, 50),
    "materials":             (6, 10, 15, 22, 35, 50),
    "utilities":             (5,  8, 12, 18, 25, 40),
    "real_estate":           (8, 14, 20, 30, 45, 65),
    None:                    (8, 15, 18, 28, 45, 65),
}

_SECTOR_DE_THRESHOLDS = {
    "financials": (1.0, 3.0, 6.0, 10.0, 15.0),
    "utilities":  (0.5, 1.0, 2.0,  4.0,  6.0),
    "real_estate": (0.5, 1.0, 2.0, 4.0,  6.0),
    "energy":     (0.3, 0.7, 1.5,  3.0,  5.0),
    None:         (0.3, 0.7, 1.5,  3.0,  5.0),
}

_FUND_WEIGHTS = {"pe_ratio": 0.30, "roe": 0.25, "de_ratio": 0.20, "peg_ratio": 0.25}


def _clip(v: float, lo=-3.0, hi=3.0) -> float:
    return max(lo, min(hi, v))


def _score_pe(pe, sector=None, revenue_growth=None) -> float:
    if pe is None:
        return 0.0
    if pe < 0:
        if revenue_growth and revenue_growth > 0.20:
            return -0.5
        elif revenue_growth and revenue_growth > 0.10:
            return -1.5
        return -2.5
    t = _SECTOR_PE_THRESHOLDS.get(sector, _SECTOR_PE_THRESHOLDS[None])
    dv, attr, fl, fh, exp, vexp = t
    if pe <= dv:
        return 2.0
    if pe <= attr:
        return 1.5
    if pe <= fh:
        return 0.5
    if pe <= exp:
        return -0.5
    if pe <= vexp:
        return -1.5
    return -2.5


def _score_roe(roe, sector=None) -> float:
    if roe is None:
        return 0.0
    if roe < 0:
        return -2.0
    if roe <= 0.05:
        return -1.0
    if roe <= 0.10:
        return 0.0
    if roe <= 0.20:
        return 1.0
    if roe <= 0.35:
        return 2.0
    return 2.5


def _score_de(de, sector=None) -> float:
    if de is None:
        return 0.0
    if de < 0:
        return -2.0
    t = _SECTOR_DE_THRESHOLDS.get(sector, _SECTOR_DE_THRESHOLDS[None])
    vc, c, m, h, e = t
    if de <= vc:
        return 2.0
    if de <= c:
        return 1.0
    if de <= m:
        return 0.0
    if de <= h:
        return -1.0
    if de <= e:
        return -1.5
    return -2.0


def _score_peg(peg, sector=None) -> float:
    if peg is None:
        return 0.0
    if peg < 0:
        return -1.5
    if peg <= 0.5:
        return 2.5
    if peg <= 1.0:
        return 1.5
    if peg <= 1.5:
        return 0.5
    if peg <= 2.5:
        return -0.5
    return -1.5


def _latest_before(series: pd.Series, cutoff: pd.Timestamp):
    """Return the most recent value in series with index <= cutoff, or None."""
    valid = series[series.index <= cutoff].dropna()
    if valid.empty:
        return None
    return float(valid.iloc[-1])


_FUND_CACHE_DIR = "/cache/fundamentals"

# Minimum acceptable quarters for EDGAR data to be considered useful
_MIN_EDGAR_QUARTERS = 8


def fetch_fundamentals_history(yf_symbol: str, ticker_symbol: str | None = None) -> dict:
    """Fetch quarterly financials with persistent cache.

    Primary source: SEC EDGAR (18+ years of quarterly data, free, no API key).
    Fallback: yfinance (only ~5 quarters but covers non-US filers).

    Args:
        yf_symbol: yfinance symbol (e.g. "AAPL", "IITU.L", "GC=F")
        ticker_symbol: Stock ticker for SEC lookup (e.g. "AAPL"). If None,
                       derived from yf_symbol by stripping suffixes.

    Returns a dict with DataFrames for income statement and balance sheet,
    indexed by quarter date. Call once per instrument before the backtest loop.
    Caches to /cache/fundamentals/ so subsequent runs skip the API call.
    """
    import os
    import pickle

    from .edgar_client import fetch_edgar_fundamentals

    os.makedirs(_FUND_CACHE_DIR, exist_ok=True)
    safe_name = yf_symbol.replace("=", "_").replace("/", "_")
    cache_path = os.path.join(_FUND_CACHE_DIR, f"{safe_name}.pkl")

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                data = pickle.load(f)
            source = data.get("source", "yfinance")
            inc_cols = len(data.get("income", pd.DataFrame()).columns) if data.get("income") is not None else 0
            bal_cols = len(data.get("balance", pd.DataFrame()).columns) if data.get("balance") is not None else 0
            logger.info("Loaded fundamentals cache for %s (source=%s, income=%d, balance=%d)",
                        yf_symbol, source, inc_cols, bal_cols)
            # If cached from old yfinance-only run with few quarters, re-fetch from EDGAR
            if source != "edgar" and inc_cols < _MIN_EDGAR_QUARTERS:
                logger.info("Cache has only %d quarters (yfinance) — trying EDGAR upgrade", inc_cols)
            else:
                return data
        except Exception:
            pass

    # Derive ticker for SEC lookup
    sec_ticker = ticker_symbol
    if sec_ticker is None:
        # Strip yfinance suffixes: "IITU.L" -> "IITU", "GC=F" -> skip
        if "=" not in yf_symbol and "^" not in yf_symbol:
            sec_ticker = yf_symbol.split(".")[0]

    # Try SEC EDGAR first (deep history)
    if sec_ticker:
        edgar_data = fetch_edgar_fundamentals(sec_ticker)
        if edgar_data is not None:
            inc_cols = len(edgar_data["income"].columns) if edgar_data["income"] is not None else 0
            if inc_cols >= _MIN_EDGAR_QUARTERS:
                with open(cache_path, "wb") as f:
                    pickle.dump(edgar_data, f)
                logger.info("Cached EDGAR fundamentals for %s (%d income quarters)", yf_symbol, inc_cols)
                return edgar_data
            else:
                logger.info("[%s] EDGAR returned only %d quarters, falling back to yfinance", yf_symbol, inc_cols)

    # Fallback: yfinance (~5 quarters, covers non-US filers)
    try:
        ticker = yf.Ticker(yf_symbol)
        income = ticker.quarterly_income_stmt
        balance = ticker.quarterly_balance_sheet
        data = {"income": income, "balance": balance, "symbol": yf_symbol, "source": "yfinance"}
        with open(cache_path, "wb") as f:
            pickle.dump(data, f)
        inc_cols = len(income.columns) if income is not None else 0
        logger.info("Cached yfinance fundamentals for %s (%d income quarters)", yf_symbol, inc_cols)
        return data
    except Exception:
        logger.exception("Failed to fetch fundamentals for %s", yf_symbol)
        return {"income": None, "balance": None, "symbol": yf_symbol, "source": "none"}


def _calc_commodity_supply_demand_score(
    ohlcv_df: pd.DataFrame,
    target_date: date,
    term: str = "short",
    is_gold: bool = False,
    dxy_df: pd.DataFrame | None = None,
) -> tuple[float, float]:
    """Commodity supply-demand trend calculation — aligned with production scorer.py.

    Short-term: 2-day price trend.
    Long-term: 10-day price trend.
    For GOLD: DXY rising = negative for gold (inverse relationship) — production lines 871-888.
    """
    import math
    import pandas as pd
    cutoff = pd.Timestamp(target_date)
    window_days = 2 if term == "short" else 10

    window_start = cutoff - pd.Timedelta(days=window_days)
    df_window = ohlcv_df[(ohlcv_df.index >= window_start) & (ohlcv_df.index <= cutoff)]

    if len(df_window) < 2:
        return 0.0, 0.0

    oldest_val = float(df_window["close"].iloc[0])
    latest_val = float(df_window["close"].iloc[-1])

    if oldest_val == 0:
        return 0.0, 0.0

    pct_change = (latest_val - oldest_val) / oldest_val
    raw_score = _clip(pct_change / 0.02, lo=-2.0, hi=2.0)

    # DXY inverse signal for GOLD (production scorer.py lines 871-888)
    dxy_adjustment = 0.0
    if is_gold and dxy_df is not None and not dxy_df.empty:
        dxy_window = dxy_df[(dxy_df.index >= window_start) & (dxy_df.index <= cutoff)]
        if len(dxy_window) >= 2:
            dxy_oldest = float(dxy_window["close"].iloc[0])
            dxy_latest = float(dxy_window["close"].iloc[-1])
            if dxy_oldest != 0:
                dxy_change = (dxy_latest - dxy_oldest) / dxy_oldest
                dxy_adjustment = _clip(-dxy_change / 0.01 * 0.5, lo=-1.0, hi=1.0)
                raw_score = _clip(raw_score + dxy_adjustment, lo=-2.0, hi=2.0)

    n_points = len(df_window)
    confidence = min(1.0, math.log(1 + n_points) / math.log(1 + 8))

    return round(_clip(raw_score * confidence), 4), round(confidence, 4)


def _fundamentals_freshness_confidence(quarters_df_index: "pd.DatetimeIndex", target_date: date) -> float:
    """Mirrors production get_fundamentals_score() freshness confidence.

    Production: 1.0 within 48h of fetch, linear decay to 0.3 at 30 days, 0.0 beyond.
    For backtest, we estimate data age as days since the most recent quarterly filing.
    Quarterly data is typically filed 30–90 days after quarter end; we use 45-day lag.
    """
    import pandas as pd
    cutoff = pd.Timestamp(target_date)
    valid = quarters_df_index[quarters_df_index <= cutoff].sort_values()
    if len(valid) == 0:
        return 0.0
    latest_quarter = valid[-1]
    # Approximate filing date: quarter end + 45 days
    filing_date = latest_quarter + pd.Timedelta(days=45)
    age_days = max(0, (cutoff - filing_date).days)
    if age_days <= 2:        # within 48h of "fetch" (analogous to freshly fetched FMP data)
        return 1.0
    if age_days >= 120:      # beyond ~4 months → confidence 0 (next quarter due)
        return 0.0
    # Linear decay: 1.0 at day 2 → 0.3 at day 30 → 0.0 at day 120
    if age_days <= 30:
        return 1.0 - (age_days - 2) * (0.7 / 28)
    return max(0.0, 0.3 - (age_days - 30) * (0.3 / 90))


def calc_fundamentals_score_for_date(
    fund_data: dict,
    target_date: date,
    price_at_date: float | None,
    sector: str | None = None,
    category: str = "stock",
    ohlcv_df: pd.DataFrame | None = None,
    term: str = "short",
) -> tuple[float, float]:
    """Compute fundamentals score + freshness confidence for a specific backtest date.

    Returns (score ∈ [-3, 3], confidence ∈ [0, 1]).
    Commodities use supply-demand price trend signal instead of accounting ratios.
    Uses only data available up to target_date (no look-ahead bias).
    """
    if category == "commodity":
        if ohlcv_df is None:
            return 0.0, 0.0
        return _calc_commodity_supply_demand_score(
            ohlcv_df, target_date, term,
            is_gold=fund_data.get("is_gold", False),
            dxy_df=fund_data.get("dxy_df"),
        )

    income: pd.DataFrame | None = fund_data.get("income")
    balance: pd.DataFrame | None = fund_data.get("balance")

    if income is None or balance is None:
        return 0.0, 0.0
    if income.empty or balance.empty:
        return 0.0, 0.0

    cutoff = pd.Timestamp(target_date)

    try:
        # Transpose so index = dates, columns = line items
        inc = income.T.copy()
        bal = balance.T.copy()

        # Ensure datetime index, sorted ascending (EDGAR can be descending)
        inc.index = pd.to_datetime(inc.index)
        bal.index = pd.to_datetime(bal.index)
        inc = inc.sort_index()
        bal = bal.sort_index()

        # --- P/E ---
        pe = None
        if price_at_date is not None:
            # Trailing EPS: sum of diluted EPS over last 4 quarters before cutoff
            eps_col = None
            for col in ["Diluted EPS", "Basic EPS", "EPS"]:
                if col in inc.columns:
                    eps_col = col
                    break
            if eps_col:
                eps_series = inc[eps_col][inc.index <= cutoff].dropna()
                if len(eps_series) >= 4:
                    trailing_eps = float(eps_series.iloc[-4:].sum())
                    if trailing_eps > 0:
                        pe = price_at_date / trailing_eps

        # --- ROE ---
        roe = None
        ni_col = None
        for col in ["Net Income", "Net Income Common Stockholders"]:
            if col in inc.columns:
                ni_col = col
                break

        eq_col = None
        for col in ["Stockholders Equity", "Common Stock Equity", "Total Equity Gross Minority Interest"]:
            if col in bal.columns:
                eq_col = col
                break

        if ni_col and eq_col:
            ni_series = inc[ni_col][inc.index <= cutoff].dropna()
            eq_series = bal[eq_col][bal.index <= cutoff].dropna()
            if len(ni_series) >= 4 and not eq_series.empty:
                net_income_ttm = float(ni_series.iloc[-4:].sum())
                book_equity = float(eq_series.iloc[-1])
                if book_equity > 0:
                    roe = net_income_ttm / book_equity

        # --- D/E ---
        de = None
        debt_col = None
        for col in ["Total Debt", "Long Term Debt And Capital Lease Obligation"]:
            if col in bal.columns:
                debt_col = col
                break
        if debt_col and eq_col:
            debt = _latest_before(bal[debt_col], cutoff)
            equity = _latest_before(bal[eq_col], cutoff)
            if debt is not None and equity and equity > 0:
                de = debt / equity

        # --- Revenue Growth (YoY) ---
        revenue_growth = None
        rev_col = None
        for col in ["Total Revenue", "Revenue"]:
            if col in inc.columns:
                rev_col = col
                break
        if rev_col:
            rev_series = inc[rev_col][inc.index <= cutoff].dropna()
            if len(rev_series) >= 8:
                rev_ttm_now = float(rev_series.iloc[-4:].sum())
                rev_ttm_prev = float(rev_series.iloc[-8:-4].sum())
                if rev_ttm_prev and rev_ttm_prev != 0:
                    revenue_growth = (rev_ttm_now - rev_ttm_prev) / abs(rev_ttm_prev)

        # --- PEG ---
        peg = None
        if pe is not None and pe > 0 and ni_col:
            ni_series = inc[ni_col][inc.index <= cutoff].dropna()
            if len(ni_series) >= 8:
                ttm_now = float(ni_series.iloc[-4:].sum())
                ttm_prev = float(ni_series.iloc[-8:-4].sum())
                if ttm_prev and ttm_prev != 0:
                    eps_growth = (ttm_now - ttm_prev) / abs(ttm_prev)
                    if eps_growth > 0:
                        peg = pe / (eps_growth * 100)

        # Score each metric
        metrics = {"pe_ratio": pe, "roe": roe, "de_ratio": de, "peg_ratio": peg}
        weighted_sum = 0.0
        weight_total = 0.0

        scored = {
            "pe_ratio": _score_pe(pe, sector, revenue_growth),
            "roe": _score_roe(roe, sector),
            "de_ratio": _score_de(de, sector),
            "peg_ratio": _score_peg(peg, sector),
        }

        for key, s in scored.items():
            if metrics[key] is not None:
                weighted_sum += s * _FUND_WEIGHTS[key]
                weight_total += _FUND_WEIGHTS[key]

        if weight_total == 0:
            return 0.0, 0.0

        raw_score = weighted_sum / weight_total

        # Freshness confidence — mirrors production get_fundamentals_score()
        inc_t = income.T
        inc_t.index = pd.to_datetime(inc_t.index)
        confidence = _fundamentals_freshness_confidence(inc_t.index, target_date)

        return round(_clip(raw_score), 4), round(confidence, 4)

    except Exception:
        logger.exception("Fundamentals calc error for %s on %s", fund_data.get("symbol"), target_date)
        return 0.0, 0.0
