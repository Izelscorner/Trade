"""Trading strategy definitions for backtest evaluation.

Each strategy uses NON-OVERLAPPING holding periods to avoid inflated returns
from overlapping forward-return windows.

For short-term (5-day): enter position on day 0, hold for 5 trading days,
then re-evaluate and enter next position. ~165 trades over 825 days.

For long-term (20-day): enter on day 0, hold for 20 trading days,
then re-evaluate. ~41 trades over 825 days.

Available strategies:
  portfolio        — Score-weighted all-instrument portfolio
  top_pick         — 100% concentrated in highest-scoring instrument per period
  high_conviction  — Only trade when buy-confidence >= threshold (default 60%)
  top_n            — Equal-weight top N instruments per period (default N=3)
  long_short       — Long top quintile, short bottom quintile (market-neutral)
  sector_rotation  — Best instrument per sector, equal-weight across sectors
  contrarian       — Buy bottom quintile (mean-reversion hypothesis)
  risk_adjusted    — Position size proportional to score / volatility

All strategies are parameterized via `StrategyParams`.
"""

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class StrategyParams:
    """Parameters shared across strategies. CLI args map here."""

    threshold: float = 60.0           # buy-confidence % threshold (high_conviction)
    top_n: int = 3                    # number of instruments (top_n strategy)
    lookback_vol: int = 20            # trailing days for vol estimate (risk_adjusted)
    long_pct: float = 0.20            # top/bottom percentile (long_short)
    short_enabled: bool = True        # allow short leg (long_short)


def _sigmoid(score: float, k: float = 1.5) -> float:
    return 100.0 / (1.0 + math.exp(-k * score))


def _holding_period(term: str) -> int:
    """Return the number of trading days per holding period."""
    return 5 if term == "short" else 20


def _get_entry_dates(dates: list, holding_days: int) -> list:
    """Select non-overlapping entry dates spaced by holding_days.

    Given sorted unique dates, pick every Nth date as an entry point.
    The position is held for N days, then the next entry is made.
    """
    sorted_dates = sorted(dates)
    entry_dates = []
    i = 0
    while i < len(sorted_dates):
        entry_dates.append(sorted_dates[i])
        i += holding_days
    return entry_dates


# ---------------------------------------------------------------------------
# Strategy implementations — all use non-overlapping holding periods
# ---------------------------------------------------------------------------

def strategy_portfolio(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Score-weighted portfolio across all instruments, non-overlapping periods.

    At each entry date, allocate weight = score / 3.0 to each instrument.
    Hold for the full period and earn the full forward return (not divided).
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)
    ret_col = "return_val"

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        weights = day_df["overall_score"] / 3.0
        w_sum = weights.abs().sum()
        if w_sum == 0:
            continue
        # Normalize weights so total exposure = 1.0
        norm_weights = weights / w_sum
        period_ret = (norm_weights * day_df[ret_col]).sum()
        # Assign full period return to the entry date
        out.loc[day_df.index, "daily_strat_ret"] = (
            norm_weights * day_df[ret_col]
        )

    return out


def strategy_top_pick(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """100% in highest-scoring instrument, held for the full period.

    Non-overlapping: pick top instrument, earn its full 5d/20d return,
    then re-evaluate and pick again.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        best_idx = day_df["overall_score"].idxmax()
        out.loc[best_idx, "daily_strat_ret"] = day_df.loc[best_idx, "return_val"]

    return out


def strategy_high_conviction(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Only trade when buy-confidence >= threshold (default 60%).

    At each entry date, filter instruments by sigmoid(score) >= threshold.
    Equal-weight qualifying instruments and hold for the full period.
    If nothing qualifies, sit in cash (0 return).
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        buy_conf = day_df["overall_score"].apply(_sigmoid)
        qualified = day_df[buy_conf >= params.threshold]
        if qualified.empty:
            continue
        n = len(qualified)
        out.loc[qualified.index, "daily_strat_ret"] = qualified["return_val"] / n

    return out


def strategy_top_n(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Equal-weight top N instruments per period (default N=3).

    Non-overlapping periods. Diversified version of top_pick.
    """
    n = params.top_n
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        top_n_df = day_df.nlargest(min(n, len(day_df)), "overall_score")
        actual_n = len(top_n_df)
        out.loc[top_n_df.index, "daily_strat_ret"] = top_n_df["return_val"] / actual_n

    return out


def strategy_long_short(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Long top quintile, short bottom quintile (market-neutral).

    Non-overlapping periods. Hedges out market beta.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if len(day_df) < 5:
            continue
        n_select = max(1, int(len(day_df) * params.long_pct))
        sorted_g = day_df.sort_values("overall_score", ascending=False)

        long_idx = sorted_g.head(n_select).index
        short_idx = sorted_g.tail(n_select).index

        # Long leg
        out.loc[long_idx, "daily_strat_ret"] = out.loc[long_idx, "return_val"] / n_select
        # Short leg (profit when they fall)
        if params.short_enabled:
            out.loc[short_idx, "daily_strat_ret"] -= out.loc[short_idx, "return_val"] / n_select

    return out


def strategy_sector_rotation(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Best instrument per sector, equal-weight across sectors.

    Non-overlapping periods. Prevents concentration in a single sector.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    group_col = "category" if "sector" not in df.columns else "sector"
    if group_col == "sector" and df["sector"].isna().all():
        group_col = "category"

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        best_per_sector = day_df.loc[day_df.groupby(group_col)["overall_score"].idxmax()]
        n_sectors = len(best_per_sector)
        if n_sectors == 0:
            continue
        out.loc[best_per_sector.index, "daily_strat_ret"] = (
            best_per_sector["return_val"] / n_sectors
        )

    return out


def strategy_contrarian(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Buy bottom quintile (mean-reversion hypothesis).

    Non-overlapping periods. Tests whether oversold instruments bounce back.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if len(day_df) < 5:
            continue
        n_select = max(1, int(len(day_df) * params.long_pct))
        bottom = day_df.nsmallest(n_select, "overall_score")
        out.loc[bottom.index, "daily_strat_ret"] = bottom["return_val"] / n_select

    return out


def strategy_risk_adjusted(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Position size = score / trailing volatility (risk-parity inspired).

    Non-overlapping periods. Higher score + lower vol = larger position.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    # Pre-compute trailing vol per symbol
    out = out.sort_values(["symbol", "date"])
    out["abs_return"] = out["return_val"].abs()
    out["trail_vol"] = out.groupby("symbol")["abs_return"].transform(
        lambda x: x.rolling(params.lookback_vol, min_periods=5).std()
    ).fillna(out["abs_return"].std())
    out["trail_vol"] = out["trail_vol"].clip(lower=0.005)

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        raw_weight = day_df["overall_score"] / day_df["trail_vol"]
        w_sum = raw_weight.abs().sum()
        if w_sum < 1e-6:
            continue
        norm_weight = raw_weight / w_sum
        out.loc[day_df.index, "daily_strat_ret"] = norm_weight * day_df["return_val"]

    return out


# ---------------------------------------------------------------------------
# Strategy registry
# ---------------------------------------------------------------------------

STRATEGIES: dict[str, dict] = {
    "portfolio": {
        "fn": strategy_portfolio,
        "desc": "Score-weighted all-instrument portfolio",
    },
    "top_pick": {
        "fn": strategy_top_pick,
        "desc": "100% in highest-scoring instrument per period",
    },
    "high_conviction": {
        "fn": strategy_high_conviction,
        "desc": "Only trade when buy-confidence >= threshold (default 60%)",
    },
    "top_n": {
        "fn": strategy_top_n,
        "desc": "Equal-weight top N instruments per period (default N=3)",
    },
    "long_short": {
        "fn": strategy_long_short,
        "desc": "Long top quintile, short bottom quintile (market-neutral)",
    },
    "sector_rotation": {
        "fn": strategy_sector_rotation,
        "desc": "Best instrument per sector, equal-weight across sectors",
    },
    "contrarian": {
        "fn": strategy_contrarian,
        "desc": "Buy bottom quintile (mean-reversion test)",
    },
    "risk_adjusted": {
        "fn": strategy_risk_adjusted,
        "desc": "Score / volatility position sizing (risk-parity inspired)",
    },
}


def list_strategies() -> str:
    """Return formatted list of available strategies."""
    lines = ["\nAvailable strategies:\n"]
    for name, info in STRATEGIES.items():
        lines.append(f"  {name:20s} — {info['desc']}")
    lines.append("")
    lines.append("Parameters:")
    lines.append("  --threshold FLOAT    Buy-confidence threshold (high_conviction, default 60)")
    lines.append("  --top-n INT          Number of instruments (top_n, default 3)")
    lines.append("  --long-pct FLOAT     Percentile for long/short legs (long_short/contrarian, default 0.20)")
    lines.append("")
    return "\n".join(lines)


def apply_strategy(
    df: pd.DataFrame,
    strategy: str,
    term: str,
    params: StrategyParams | None = None,
) -> pd.DataFrame:
    """Dispatch to the named strategy function.

    Returns the DataFrame augmented with `daily_strat_ret`.
    """
    if strategy not in STRATEGIES:
        raise ValueError(f"Unknown strategy '{strategy}'. Use --list-strategies to see options.")
    if params is None:
        params = StrategyParams()
    return STRATEGIES[strategy]["fn"](df, term, params)
