"""SEC EDGAR XBRL client for deep historical quarterly fundamentals.

Fetches quarterly financial data from the SEC EDGAR companyfacts API.
Free, no API key required, 18+ years of quarterly data for US-listed companies.

Returns DataFrames in the same format as yfinance (rows=line items, columns=dates)
so calc_fundamentals_score_for_date() works unchanged.
"""

import json
import logging
import re
import time
import urllib.request
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

_USER_AGENT = "TradeSignal Backtester research@tradesignal.local"

# SEC rate limit: 10 requests/second — we add a small delay between calls
_MIN_REQUEST_INTERVAL = 0.15
_last_request_time = 0.0

# ── Ticker → CIK mapping (cached after first load) ──
_ticker_cik_map: dict[str, int] | None = None


def _load_ticker_cik_map() -> dict[str, int]:
    """Load SEC's master ticker→CIK mapping file."""
    global _ticker_cik_map
    if _ticker_cik_map is not None:
        return _ticker_cik_map

    url = "https://www.sec.gov/files/company_tickers.json"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        _throttle()
        resp = urllib.request.urlopen(req, timeout=30)
        data = json.loads(resp.read())
        _ticker_cik_map = {entry["ticker"]: entry["cik_str"] for entry in data.values()}
        logger.info("Loaded SEC ticker→CIK map: %d tickers", len(_ticker_cik_map))
        return _ticker_cik_map
    except Exception:
        logger.exception("Failed to load SEC ticker→CIK map")
        _ticker_cik_map = {}
        return _ticker_cik_map


def get_cik(ticker: str) -> int | None:
    """Look up CIK number for a ticker symbol."""
    mapping = _load_ticker_cik_map()
    cik = mapping.get(ticker.upper())
    return int(cik) if cik else None


def _throttle():
    """Respect SEC's 10 req/s rate limit."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _last_request_time = time.time()


def _fetch_company_facts(cik: int) -> dict | None:
    """Fetch the full companyfacts JSON from EDGAR."""
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        _throttle()
        resp = urllib.request.urlopen(req, timeout=30)
        return json.loads(resp.read())
    except Exception:
        logger.exception("Failed to fetch EDGAR facts for CIK %d", cik)
        return None


def _extract_quarterly_flow(
    facts: dict, concept: str, unit: str = "USD"
) -> list[tuple[str, float]]:
    """Extract quarterly flow values (income stmt items) using CY frame tags.

    Flow items use frames like CY2023Q2 (no trailing 'I').
    For missing quarters (e.g. fiscal Q4 reported in 10-K), derives from
    annual - sum(other 3 quarters).
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    concept_data = us_gaap.get(concept, {})
    entries = concept_data.get("units", {}).get(unit, [])
    if not entries:
        return []

    # Collect CY quarterly entries (CYxxxxQn, no trailing I)
    cy_q_pattern = re.compile(r"^CY(\d{4})Q([1-4])$")
    quarterly: dict[str, tuple[str, float]] = {}  # "2023Q2" -> (end_date, value)

    for e in entries:
        frame = e.get("frame", "")
        m = cy_q_pattern.match(frame)
        if m:
            key = f"{m.group(1)}Q{m.group(2)}"
            quarterly[key] = (e["end"], e["val"])

    # Collect annual entries to fill gaps
    cy_annual_pattern = re.compile(r"^CY(\d{4})$")
    annuals: dict[str, float] = {}
    for e in entries:
        frame = e.get("frame", "")
        m = cy_annual_pattern.match(frame)
        if m:
            annuals[m.group(1)] = e["val"]

    # Fill missing quarters: Q_missing = Annual - sum(other 3)
    for year_str, annual_val in annuals.items():
        quarters_present = []
        quarters_missing = []
        for q in range(1, 5):
            key = f"{year_str}Q{q}"
            if key in quarterly:
                quarters_present.append((q, quarterly[key][1]))
            else:
                quarters_missing.append(q)

        if len(quarters_missing) == 1 and len(quarters_present) == 3:
            missing_q = quarters_missing[0]
            derived_val = annual_val - sum(v for _, v in quarters_present)
            # Approximate end date for the missing quarter
            q_end_months = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
            end_date = f"{year_str}-{q_end_months[missing_q]}"
            key = f"{year_str}Q{missing_q}"
            quarterly[key] = (end_date, derived_val)

    # Sort by quarter and return
    result = []
    for key in sorted(quarterly.keys()):
        end_date, val = quarterly[key]
        result.append((end_date, val))
    return result


def _extract_quarterly_instant(
    facts: dict, concept: str, unit: str = "USD"
) -> list[tuple[str, float]]:
    """Extract quarterly instant/balance-sheet values using CY frame tags.

    Balance sheet items use frames like CY2023Q2I (trailing 'I' for instant).
    """
    us_gaap = facts.get("facts", {}).get("us-gaap", {})
    concept_data = us_gaap.get(concept, {})
    entries = concept_data.get("units", {}).get(unit, [])
    if not entries:
        return []

    cy_qi_pattern = re.compile(r"^CY(\d{4})Q([1-4])I$")
    quarterly: dict[str, tuple[str, float]] = {}

    for e in entries:
        frame = e.get("frame", "")
        m = cy_qi_pattern.match(frame)
        if m:
            key = f"{m.group(1)}Q{m.group(2)}"
            quarterly[key] = (e["end"], e["val"])

    result = []
    for key in sorted(quarterly.keys()):
        end_date, val = quarterly[key]
        result.append((end_date, val))
    return result


def fetch_edgar_fundamentals(ticker: str) -> dict | None:
    """Fetch quarterly fundamentals from SEC EDGAR for a US-listed ticker.

    Returns dict with 'income' and 'balance' DataFrames in yfinance format
    (rows=line items, columns=dates as Timestamps), or None if not available.
    """
    cik = get_cik(ticker)
    if cik is None:
        logger.info("[%s] No SEC CIK found — not a US filer", ticker)
        return None

    facts = _fetch_company_facts(cik)
    if facts is None:
        return None

    # ── Income statement items (flow/quarterly) ──
    # Try multiple concept names (companies may use different ones)
    eps_data = (
        _extract_quarterly_flow(facts, "EarningsPerShareDiluted", "USD/shares")
        or _extract_quarterly_flow(facts, "EarningsPerShareBasic", "USD/shares")
    )
    ni_data = (
        _extract_quarterly_flow(facts, "NetIncomeLoss")
        or _extract_quarterly_flow(facts, "ProfitLoss")
    )
    # Revenue: try common concept names in priority order
    revenue_data = (
        _extract_quarterly_flow(facts, "Revenues")
        or _extract_quarterly_flow(facts, "RevenueFromContractWithCustomerExcludingAssessedTax")
        or _extract_quarterly_flow(facts, "SalesRevenueNet")
        or _extract_quarterly_flow(facts, "RevenuesNetOfInterestExpense")
    )

    if not eps_data and not ni_data:
        logger.warning("[%s] EDGAR: no EPS or Net Income data found", ticker)
        return None

    # Build income DataFrame: collect all unique dates
    income_dates = set()
    for date_str, _ in eps_data:
        income_dates.add(date_str)
    for date_str, _ in ni_data:
        income_dates.add(date_str)
    for date_str, _ in revenue_data:
        income_dates.add(date_str)

    income_dates_sorted = sorted(income_dates)
    income_ts = [pd.Timestamp(d) for d in income_dates_sorted]

    income_dict: dict[str, list[float | None]] = {
        "Diluted EPS": [None] * len(income_dates_sorted),
        "Net Income": [None] * len(income_dates_sorted),
        "Total Revenue": [None] * len(income_dates_sorted),
    }

    date_idx = {d: i for i, d in enumerate(income_dates_sorted)}
    for date_str, val in eps_data:
        income_dict["Diluted EPS"][date_idx[date_str]] = val
    for date_str, val in ni_data:
        income_dict["Net Income"][date_idx[date_str]] = val
    for date_str, val in revenue_data:
        income_dict["Total Revenue"][date_idx[date_str]] = val

    income_df = pd.DataFrame(income_dict, index=income_ts).T
    # yfinance format: columns sorted newest first
    income_df = income_df[sorted(income_df.columns, reverse=True)]

    # ── Balance sheet items (instant/quarterly) ──
    equity_data = (
        _extract_quarterly_instant(facts, "StockholdersEquity")
        or _extract_quarterly_instant(facts, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
    )
    debt_data = (
        _extract_quarterly_instant(facts, "LongTermDebtNoncurrent")
        or _extract_quarterly_instant(facts, "LongTermDebt")
        or _extract_quarterly_instant(facts, "DebtInstrumentCarryingAmount")
    )

    balance_dates = set()
    for date_str, _ in equity_data:
        balance_dates.add(date_str)
    for date_str, _ in debt_data:
        balance_dates.add(date_str)

    if not balance_dates:
        logger.warning("[%s] EDGAR: no balance sheet data found", ticker)
        return None

    balance_dates_sorted = sorted(balance_dates)
    balance_ts = [pd.Timestamp(d) for d in balance_dates_sorted]

    balance_dict: dict[str, list[float | None]] = {
        "Stockholders Equity": [None] * len(balance_dates_sorted),
        "Total Debt": [None] * len(balance_dates_sorted),
    }

    date_idx_bal = {d: i for i, d in enumerate(balance_dates_sorted)}
    for date_str, val in equity_data:
        balance_dict["Stockholders Equity"][date_idx_bal[date_str]] = val
    for date_str, val in debt_data:
        balance_dict["Total Debt"][date_idx_bal[date_str]] = val

    balance_df = pd.DataFrame(balance_dict, index=balance_ts).T
    balance_df = balance_df[sorted(balance_df.columns, reverse=True)]

    logger.info(
        "[%s] EDGAR: %d income quarters (EPS: %d, NI: %d, Rev: %d), %d balance quarters (Equity: %d, Debt: %d)",
        ticker, len(income_df.columns), len(eps_data), len(ni_data), len(revenue_data),
        len(balance_df.columns), len(equity_data), len(debt_data),
    )

    return {"income": income_df, "balance": balance_df, "symbol": ticker, "source": "edgar"}
