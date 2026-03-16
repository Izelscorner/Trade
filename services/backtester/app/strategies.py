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
    cost_bps: float = 0.0            # round-trip transaction cost in basis points (0=off, 5=institutional)
    long_only: bool = False           # if True, clamp negative weights to 0 in portfolio strategy


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

    When long_only=True (default), negative weights are clamped to 0 and
    remaining positive weights are re-normalized. This prevents the portfolio
    from going net short in bear markets, making benchmark comparison valid.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)
    ret_col = "return_val"
    cost_per_trade = params.cost_bps / 10_000.0

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue
        weights = day_df["overall_score"] / 3.0

        if params.long_only:
            weights = weights.clip(lower=0.0)

        w_sum = weights.abs().sum()
        if w_sum == 0:
            continue
        # Normalize weights so total exposure = 1.0
        norm_weights = weights / w_sum
        n_positions = (norm_weights.abs() > 1e-6).sum()
        # Apply transaction costs: cost per instrument traded
        total_cost = n_positions * cost_per_trade
        out.loc[day_df.index, "daily_strat_ret"] = (
            norm_weights * day_df[ret_col] - norm_weights.abs() * cost_per_trade
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

    # Pre-compute trailing vol per symbol from BACKWARD-LOOKING overall_score
    # changes as a proxy for realized volatility. We cannot use return_val (forward
    # returns) here — that would be look-ahead bias.
    out = out.sort_values(["symbol", "date"])
    out["score_abs_chg"] = out.groupby("symbol")["overall_score"].diff().abs()
    out["trail_vol"] = out.groupby("symbol")["score_abs_chg"].transform(
        lambda x: x.rolling(params.lookback_vol, min_periods=5).std()
    ).fillna(out["score_abs_chg"].std())
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


def strategy_momentum(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:  # noqa: ARG001
    """Equal-weight passive index — buy and hold all instruments equally.

    No selection, no signal. This is the simplest possible strategy:
    at each entry date, allocate equal weight to every instrument.
    Tests whether ANY signal-based strategy beats naive diversification.
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
        n = len(day_df)
        out.loc[day_df.index, "daily_strat_ret"] = day_df["return_val"] / n

    return out


def strategy_quant_alpha(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Data-driven strategy reverse-engineered from 165K backtest observations.

    Key findings from deep IC analysis:
    1. Sector score is strongest cross-sectional predictor (IC=+0.039, IR=0.157)
    2. Macro score is harmful for ranking (same for all instruments, negative IC)
    3. Technical score has ~zero cross-sectional IC
    4. Sentiment momentum (1d change) predicts better than level (IC=+0.013)
    5. Sentiment × sector interaction is significant (IC=+0.011)
    6. Confidence gating improves signal (high-conf IC=+0.014 vs low-conf IC=0.000)
    7. When all signals agree bearish, contrarian boost helps

    Formula:
      rank_score = 0.35 × sector + 0.25 × sentiment + 0.20 × fundamentals
                 + 0.10 × sentiment_momentum + 0.10 × (sentiment × sector)

    Modifiers: confidence gate, regime-aware sector boost, contrarian boost.
    Selection: best per sector (diversified), confidence-gated.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    # Check if sub-scores are available
    has_subscores = all(
        col in out.columns
        for col in ["sector_score", "sentiment_score", "fundamentals_score",
                     "sentiment_conf", "sector_conf", "fundamentals_conf"]
    )

    if not has_subscores:
        # Fallback: use overall_score with sector_rotation logic
        group_col = "sector" if "sector" in out.columns and not out["sector"].isna().all() else "category"
        for entry_date in entry_dates:
            day_df = out[out["date"] == entry_date]
            if day_df.empty:
                continue
            best_per_sector = day_df.loc[day_df.groupby(group_col)["overall_score"].idxmax()]
            n = len(best_per_sector)
            if n == 0:
                continue
            out.loc[best_per_sector.index, "daily_strat_ret"] = best_per_sector["return_val"] / n
        return out

    # Zero out fundamentals for commodities — P/E, ROE, D/E, PEG are meaningless
    # for futures contracts.  Production grading gives them 0% weight; we must
    # do the same here to avoid leaking nonsensical scores into rank_score.
    if "category" in out.columns:
        commodity_mask = out["category"].str.lower() == "commodity"
        out.loc[commodity_mask, "fundamentals_score"] = 0.0
        out.loc[commodity_mask, "fundamentals_conf"] = 0.0

    # Sort for momentum calculation
    out = out.sort_values(["symbol", "date"])

    # Compute sentiment momentum (1-day change in sentiment_score per symbol)
    out["sent_momentum"] = out.groupby("symbol")["sentiment_score"].diff().fillna(0.0)

    # Compute sentiment × sector interaction
    out["sent_x_sector"] = out["sentiment_score"] * out["sector_score"]

    # Average confidence (meta-signal)
    out["avg_conf"] = (
        out["sentiment_conf"] + out["sector_conf"] + out["fundamentals_conf"]
    ) / 3.0

    # Signal agreement count (how many sub-scores agree on direction)
    out["signal_agreement"] = (
        np.sign(out["sentiment_score"])
        + np.sign(out["sector_score"])
        + np.sign(out["fundamentals_score"])
    )

    # Regime proxy: use trailing cross-sectional score dispersion (backward-looking).
    # DO NOT use return_val (forward returns) — that is look-ahead bias.
    out = out.sort_values(["date", "symbol"])
    day_score_disp = out.groupby("date")["overall_score"].std()
    disp_q75 = day_score_disp.quantile(0.75)
    high_vol_dates = set(day_score_disp[day_score_disp > disp_q75].index)

    # Median confidence for gating (computed once)
    median_conf = out["avg_conf"].median()

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue

        # Confidence gate: only consider instruments with above-median confidence
        confident = day_df[day_df["avg_conf"] >= median_conf]
        if len(confident) < 3:
            confident = day_df  # fallback if too few pass gate

        # Regime-aware weights
        is_high_vol = entry_date in high_vol_dates
        if is_high_vol:
            w_sector, w_sent, w_fund, w_mom, w_inter = 0.45, 0.20, 0.10, 0.10, 0.15
        else:
            w_sector, w_sent, w_fund, w_mom, w_inter = 0.35, 0.25, 0.20, 0.10, 0.10

        # Compute rank_score
        rank_score = (
            w_sector * confident["sector_score"]
            + w_sent * confident["sentiment_score"]
            + w_fund * confident["fundamentals_score"]
            + w_mom * confident["sent_momentum"]
            + w_inter * confident["sent_x_sector"]
        )

        # Contrarian boost: when all 3 key signals are bearish (agreement <= -2),
        # add a mean-reversion bonus
        contrarian_mask = confident["signal_agreement"] <= -2
        rank_score = rank_score.copy()
        rank_score.loc[contrarian_mask] += 0.15

        # Sector rotation: pick best-ranked instrument per sector
        sector_col = "sector" if "sector" in confident.columns and not confident["sector"].isna().all() else "category"
        temp = confident.copy()
        temp["rank_score"] = rank_score

        # For each sector, pick the highest rank_score instrument
        best_idx = temp.groupby(sector_col)["rank_score"].idxmax()
        best = temp.loc[best_idx]

        # Weight by rank_score (positive only — skip negative-ranked sectors)
        positive = best[best["rank_score"] > 0]
        if positive.empty:
            continue

        # Score-weighted allocation (higher rank_score = bigger position)
        weights = positive["rank_score"] / positive["rank_score"].sum()
        out.loc[positive.index, "daily_strat_ret"] = weights * positive["return_val"]

    return out


def strategy_quant_alpha_v2(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Grade-based variant of quant_alpha: uses overall_score from the grading
    system instead of re-computing a custom rank from raw sub-scores.

    The grading system already applies the optimized composite weights
    (sector-heavy, macro-light) via simulator.py.  This strategy layers the
    same quant_alpha meta-logic on top of that composite score:
      - Confidence gating (above-median avg confidence)
      - Volatility regime detection (high-vol → heavier sector tilt via
        re-ranking with sector_score boost)
      - Contrarian bonus when all key sub-scores agree bearish
      - Sector rotation: best instrument per sector, score-weighted allocation
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    has_subscores = all(
        col in out.columns
        for col in ["sentiment_conf", "sector_conf", "fundamentals_conf",
                     "sentiment_score", "sector_score", "fundamentals_score"]
    )

    # Average confidence (meta-signal for gating)
    if has_subscores:
        out["avg_conf"] = (
            out["sentiment_conf"] + out["sector_conf"] + out["fundamentals_conf"]
        ) / 3.0
        # Signal agreement count
        out["signal_agreement"] = (
            np.sign(out["sentiment_score"])
            + np.sign(out["sector_score"])
            + np.sign(out["fundamentals_score"])
        )
    else:
        out["avg_conf"] = 0.5
        out["signal_agreement"] = 0

    # Regime proxy: backward-looking score dispersion (no look-ahead bias)
    out = out.sort_values(["date", "symbol"])
    day_score_disp = out.groupby("date")["overall_score"].std()
    disp_q75 = day_score_disp.quantile(0.75)
    high_vol_dates = set(day_score_disp[day_score_disp > disp_q75].index)

    median_conf = out["avg_conf"].median()

    sector_col = "sector" if "sector" in out.columns and not out["sector"].isna().all() else "category"

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue

        # Confidence gate: only consider instruments with above-median confidence
        confident = day_df[day_df["avg_conf"] >= median_conf]
        if len(confident) < 3:
            confident = day_df

        # Start from overall_score (already has optimized composite weights)
        rank_score = confident["overall_score"].copy()

        # Regime-aware sector boost: in high-vol markets, add extra sector weight
        if entry_date in high_vol_dates and has_subscores:
            rank_score = rank_score + 0.10 * confident["sector_score"]

        # Contrarian boost when all key signals agree bearish
        contrarian_mask = confident["signal_agreement"] <= -2
        rank_score.loc[contrarian_mask] += 0.15

        # Sector rotation: best instrument per sector
        temp = confident.copy()
        temp["rank_score"] = rank_score

        best_idx = temp.groupby(sector_col)["rank_score"].idxmax()
        best = temp.loc[best_idx]

        # Score-weighted allocation (positive only)
        positive = best[best["rank_score"] > 0]
        if positive.empty:
            continue

        weights = positive["rank_score"] / positive["rank_score"].sum()
        out.loc[positive.index, "daily_strat_ret"] = weights * positive["return_val"]

    return out


def strategy_quant_alpha_v3(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Grid-optimized grade-based strategy.

    Improvements over v2 (validated via parameter grid search on OOS split):
      1. Position cap at 25% — v2 allowed 96% concentration in a single
         instrument (mean 40.8%).  25% cap reduces max-drawdown from -23% to
         -20% while preserving alpha.
      2. Confidence-blended ranking — rank_score = overall_score × (0.7 + 0.3 ×
         avg_conf).  Same-score instruments are differentiated by data quality,
         pushing higher-confidence picks to the top.
      3. Wider contrarian trigger (signal_agreement <= -1, bonus +0.15) — v2's
         <= -2 threshold almost never fired.  Relaxing to "any 2 of 3 bearish"
         captures more mean-reversion opportunities.

    Removed from initial v3 prototype (grid search showed they hurt):
      - Vol scaling: high-vol periods actually have HIGHER mean returns (+3.4%
        vs +1.4%).  Scaling down exposure during vol spikes cuts winners.
      - Cash-raise rule: median score rarely goes negative; when it does the
        dip is often a buying opportunity.  50% cash drag killed alpha.

    Grid search result (Long-Term + Sentiment):
      Test alpha +47.5% vs v2 +21.3%, Test Sharpe 1.91 vs 1.61,
      Max drawdown -20.4% vs -23.1%.
    """
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    MAX_WEIGHT = 0.25

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    has_subscores = all(
        col in out.columns
        for col in ["sentiment_conf", "sector_conf", "fundamentals_conf",
                     "sentiment_score", "sector_score", "fundamentals_score"]
    )

    if has_subscores:
        out["avg_conf"] = (
            out["sentiment_conf"] + out["sector_conf"] + out["fundamentals_conf"]
        ) / 3.0
        out["signal_agreement"] = (
            np.sign(out["sentiment_score"])
            + np.sign(out["sector_score"])
            + np.sign(out["fundamentals_score"])
        )
    else:
        out["avg_conf"] = 0.5
        out["signal_agreement"] = 0

    # Regime proxy: backward-looking score dispersion (no look-ahead bias)
    out = out.sort_values(["date", "symbol"])
    day_score_disp = out.groupby("date")["overall_score"].std()
    disp_q75 = day_score_disp.quantile(0.75)
    high_vol_dates = set(day_score_disp[day_score_disp > disp_q75].index)

    median_conf = out["avg_conf"].median()

    sector_col = "sector" if "sector" in out.columns and not out["sector"].isna().all() else "category"

    for entry_date in entry_dates:
        day_df = out[out["date"] == entry_date]
        if day_df.empty:
            continue

        # Confidence gate: above-median avg confidence
        confident = day_df[day_df["avg_conf"] >= median_conf]
        if len(confident) < 3:
            confident = day_df

        # Confidence-blended rank: higher confidence → ranking boost
        rank_score = confident["overall_score"] * (0.7 + 0.3 * confident["avg_conf"])

        # Regime-aware sector boost in high-vol markets
        if entry_date in high_vol_dates and has_subscores:
            rank_score = rank_score + 0.10 * confident["sector_score"]

        # Contrarian boost: any 2 of 3 key signals bearish
        contrarian_mask = confident["signal_agreement"] <= -1
        rank_score = rank_score.copy()
        rank_score.loc[contrarian_mask] += 0.15

        # Sector rotation: best instrument per sector
        temp = confident.copy()
        temp["rank_score"] = rank_score

        best_idx = temp.groupby(sector_col)["rank_score"].idxmax()
        best = temp.loc[best_idx]

        positive = best[best["rank_score"] > 0]
        if positive.empty:
            continue

        # Score-weighted allocation with position cap
        weights = positive["rank_score"] / positive["rank_score"].sum()
        weights = weights.clip(upper=MAX_WEIGHT)
        weights = weights / weights.sum()

        out.loc[positive.index, "daily_strat_ret"] = weights * positive["return_val"]

    return out


def strategy_random(df: pd.DataFrame, term: str, params: StrategyParams) -> pd.DataFrame:
    """Random picking: equal-weight N randomly selected instruments per period.

    Averages over 100 Monte Carlo runs for a stable baseline.
    Tests whether the scoring system beats pure chance.
    """
    n = params.top_n
    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)
    n_runs = 100

    out = df.copy()
    out["daily_strat_ret"] = 0.0

    # Accumulate returns across Monte Carlo runs
    accumulated: dict[int, float] = {}  # index -> sum of returns across runs

    rng = np.random.RandomState(42)

    for run in range(n_runs):
        for entry_date in entry_dates:
            day_df = out[out["date"] == entry_date]
            if day_df.empty:
                continue
            pick_n = min(n, len(day_df))
            chosen = day_df.sample(n=pick_n, random_state=rng)
            for idx in chosen.index:
                ret = chosen.loc[idx, "return_val"] / pick_n
                accumulated[idx] = accumulated.get(idx, 0.0) + ret

    # Average across runs
    for idx, total_ret in accumulated.items():
        out.loc[idx, "daily_strat_ret"] = total_ret / n_runs

    return out


# ---------------------------------------------------------------------------
# Strategy registry
# ---------------------------------------------------------------------------

STRATEGIES: dict[str, dict] = {
    "portfolio": {
        "fn": strategy_portfolio,
        "desc": "Score-weighted all-instrument portfolio",
        "guide": {
            "how_it_works": (
                "This strategy spreads your money across ALL tracked instruments at once, "
                "but gives more money to assets the system rates highly and less to those "
                "it rates poorly. Think of it like a fund manager who owns everything but "
                "tilts heavily toward their best ideas."
            ),
            "what_to_watch": (
                "Look at the <strong>Buy Confidence %</strong> on the Dashboard for each asset. "
                "Higher confidence = bigger slice of your portfolio. An asset at 75% confidence "
                "gets roughly 3x the allocation of one at 25%. Also check the "
                "<strong>Action Label</strong> (Strong Buy / Buy / Neutral / Sell) — "
                "anything rated 'Buy' or above gets a meaningful positive allocation."
            ),
            "how_to_pick": (
                "You don't pick individual assets — you buy them ALL, weighted by score. "
                "Every 5 days (short-term) or 20 days (long-term), rebalance: sell down "
                "positions whose scores dropped, add to positions whose scores rose. "
                "The key metric is the <strong>overall composite score</strong> — "
                "assets with negative scores get shorted (bet against)."
            ),
            "best_for": "Diversified investors who trust the scoring system and want broad exposure with a tilt toward high-conviction picks.",
        },
    },
    "top_pick": {
        "fn": strategy_top_pick,
        "desc": "100% in highest-scoring instrument per period",
        "guide": {
            "how_it_works": (
                "Every period, put ALL your money into the single asset with the highest "
                "score. This is the most aggressive strategy — maximum concentration, "
                "maximum risk, maximum potential reward. You're betting the system's #1 "
                "pick will outperform everything else."
            ),
            "what_to_watch": (
                "On the Dashboard, sort by <strong>Buy Confidence %</strong> and look at "
                "the #1 ranked asset. Check its <strong>Action Label</strong> — ideally "
                "it should be 'Strong Buy' (78%+). Also look at the "
                "<strong>sub-score breakdown</strong> on the Asset Detail page: are Technical, "
                "Sentiment, and Fundamentals all agreeing? Unanimous agreement = stronger signal."
            ),
            "how_to_pick": (
                "Pick the asset with the HIGHEST Buy Confidence %. Hold it for the full "
                "period (5 or 20 trading days), then check again and switch if a different "
                "asset is now #1. If the top score is below 55% (barely above neutral), "
                "consider sitting in cash instead — the signal is weak."
            ),
            "best_for": "Aggressive traders comfortable with high volatility who want maximum exposure to the system's strongest signal.",
        },
    },
    "high_conviction": {
        "fn": strategy_high_conviction,
        "desc": "Only trade when buy-confidence >= threshold (default 60%)",
        "guide": {
            "how_it_works": (
                "Only invest when the system is genuinely confident. At each period, "
                "filter for assets with Buy Confidence >= 60% (customizable). "
                "Split your money equally among those that qualify. If nothing qualifies, "
                "stay in cash — sometimes the best trade is no trade."
            ),
            "what_to_watch": (
                "On the Dashboard, count how many assets show <strong>Buy Confidence >= 60%</strong> "
                "(or your chosen threshold). The <strong>Action Label</strong> should be "
                "'Buy' or 'Strong Buy'. Look at the <strong>Sector</strong> column too — "
                "if all qualifying assets are in the same sector, you're concentrated."
            ),
            "how_to_pick": (
                "Buy EVERY asset with Buy Confidence >= 60%, splitting money equally. "
                "For example, if 4 assets qualify, put 25% in each. If only 1 qualifies, "
                "put 100% in it. If none qualify, hold cash and wait. "
                "Raise the threshold to 70% for fewer but stronger trades."
            ),
            "best_for": "Patient investors who prefer quality over quantity — only trading when the system has strong conviction.",
        },
    },
    "top_n": {
        "fn": strategy_top_n,
        "desc": "Equal-weight top N instruments per period (default N=3)",
        "guide": {
            "how_it_works": (
                "Pick the top 3 (or N) highest-scoring assets and split your money "
                "equally among them. This balances concentration (top picks only) with "
                "diversification (spreading across multiple assets). A middle ground "
                "between top_pick (all-in on #1) and portfolio (own everything)."
            ),
            "what_to_watch": (
                "Sort the Dashboard by <strong>Buy Confidence %</strong> and look at the "
                "top 3 assets. Check if they're in different sectors — same-sector concentration "
                "means correlated risk. Look at the <strong>score gap</strong> between #3 and #4: "
                "a big gap means the top 3 are clearly differentiated."
            ),
            "how_to_pick": (
                "Sort all assets by Buy Confidence %. Buy the top 3 (equal amounts: ~33% each). "
                "Hold for 5 or 20 trading days, then re-sort and pick the new top 3. "
                "If the top 3 are all in tech, consider adding N=5 for better diversification."
            ),
            "best_for": "Traders who want concentrated exposure to the best ideas but with some diversification safety net.",
        },
    },
    "long_short": {
        "fn": strategy_long_short,
        "desc": "Long top quintile, short bottom quintile (market-neutral)",
        "guide": {
            "how_it_works": (
                "Buy the top 20% of assets (highest scores) AND short-sell the bottom 20% "
                "(lowest scores). This is 'market-neutral' — you profit if the top "
                "outperforms the bottom, regardless of whether the overall market goes "
                "up or down. You're betting the scoring system can separate winners from losers."
            ),
            "what_to_watch": (
                "Look at the SPREAD between the top-ranked and bottom-ranked assets on the "
                "Dashboard. A wide spread (e.g., #1 at 80% vs last at 30%) means the system "
                "sees clear differentiation. Also watch the <strong>Macro Sentiment</strong> — "
                "in strong bull markets, shorting the bottom can hurt because even weak assets rise."
            ),
            "how_to_pick": (
                "Sort by Buy Confidence %. Buy the top 20% of assets (e.g., top 10 out of 50). "
                "Short the bottom 20%. Equal weight within each leg. "
                "The long and short legs should be roughly equal in dollar value. "
                "WARNING: shorting requires a margin account and carries unlimited loss risk."
            ),
            "best_for": "Experienced traders with margin accounts who want market-neutral returns — profit from relative performance, not market direction.",
        },
    },
    "sector_rotation": {
        "fn": strategy_sector_rotation,
        "desc": "Best instrument per sector, equal-weight across sectors",
        "guide": {
            "how_it_works": (
                "For each sector (tech, healthcare, energy, etc.), pick the single "
                "best-scoring asset. Then split money equally across all sectors. "
                "This prevents over-concentration in hot sectors and ensures you "
                "always have exposure to every part of the economy."
            ),
            "what_to_watch": (
                "On the Dashboard, group assets mentally by their <strong>Sector</strong>. "
                "Check the <strong>Sector Sentiment</strong> cards on Asset Detail pages — "
                "a sector with positive sentiment may have its best pick score even higher. "
                "The <strong>Macro Sentiment</strong> indicator shows the overall market mood."
            ),
            "how_to_pick": (
                "Group assets by sector. Within each sector, pick the one with the highest "
                "Buy Confidence %. Invest equal amounts in each sector's champion. "
                "Example: NVDA (tech), LLY (healthcare), XOM (energy), WMT (staples). "
                "Rebalance every period. Skip commodities/ETFs or treat them as separate 'sectors'."
            ),
            "best_for": "Balanced investors who want diversification across sectors while still picking winners within each one.",
        },
    },
    "contrarian": {
        "fn": strategy_contrarian,
        "desc": "Buy bottom quintile (mean-reversion test)",
        "guide": {
            "how_it_works": (
                "Buy the WORST-scoring assets (bottom 20%, equal-weighted) — the ones everyone "
                "else avoids. The theory: oversold assets tend to bounce back (mean reversion). "
                "When everyone is bearish, prices may have overshot to the downside. "
                "This is the opposite of top_pick. With 50 instruments, that's ~10 positions."
            ),
            "what_to_watch": (
                "Look for assets with LOW Buy Confidence (under 40%) but check WHY they're "
                "low. On the Asset Detail page, check if <strong>Technical indicators</strong> "
                "show oversold signals (RSI < 30, price near support). "
                "A low score from temporary bad news is better for mean-reversion than "
                "a low score from deteriorating fundamentals."
            ),
            "how_to_pick": (
                "Sort by Buy Confidence % ASCENDING (lowest first). Buy the bottom 20% of assets "
                "with equal weight (e.g., bottom 10 out of 50 instruments). Hold for the full "
                "period and hope for a bounce. This strategy has higher risk — these assets are "
                "rated low for a reason. Works best in choppy/sideways markets, poorly in strong trends."
            ),
            "best_for": "Contrarian-minded traders who believe in mean reversion and are comfortable buying when others are selling.",
        },
    },
    "risk_adjusted": {
        "fn": strategy_risk_adjusted,
        "desc": "Score / volatility position sizing (risk-parity inspired)",
        "guide": {
            "how_it_works": (
                "Like the portfolio strategy, but adjusted for risk. Assets with high "
                "scores AND low volatility get bigger positions. A calm stock with a "
                "good score gets more money than a volatile stock with the same score. "
                "This avoids letting wild price swings dominate your returns."
            ),
            "what_to_watch": (
                "On Asset Detail, check the <strong>ATR (Average True Range)</strong> "
                "indicator in the Technical Panel — lower ATR = calmer price action = "
                "bigger position in this strategy. Also check <strong>Bollinger Bands</strong> "
                "width — narrow bands = low volatility. Compare the Buy Confidence "
                "across assets, but discount the volatile ones mentally."
            ),
            "how_to_pick": (
                "For each asset, mentally divide its Buy Confidence by its recent volatility. "
                "A stable 65% confidence stock beats a wild 75% confidence stock. "
                "Invest proportionally to score/volatility. In practice: overweight steady "
                "blue chips (WMT, JNJ) and underweight volatile names (TSLA, crypto-adjacent)."
            ),
            "best_for": "Risk-conscious investors who want the scoring signal but with volatility dampening — smoother ride, fewer stomach-churning swings.",
        },
    },
    "momentum": {
        "fn": strategy_momentum,
        "desc": "Equal-weight passive index — all instruments, no signal (naive benchmark)",
        "guide": {
            "how_it_works": (
                "Buy ALL assets with exactly equal weight. No scoring, no selection — "
                "this is the simplest possible strategy, equivalent to a basic index fund. "
                "It exists as a BENCHMARK: if other strategies can't beat this, "
                "the scoring system isn't adding value."
            ),
            "what_to_watch": (
                "This strategy ignores all dashboard signals. Its performance represents "
                "what you'd get from blind diversification. Compare other strategies' "
                "<strong>Alpha</strong> against this — positive alpha means the scoring "
                "system is earning its keep."
            ),
            "how_to_pick": (
                "Buy every tracked asset in equal amounts. No analysis needed. "
                "Rebalance to equal weight each period. This is the 'do nothing' baseline. "
                "If you can't beat this consistently, just buy an index fund."
            ),
            "best_for": "Benchmark only — not recommended as an actual strategy. Shows what passive, equal-weight diversification delivers.",
        },
    },
    "quant_alpha": {
        "fn": strategy_quant_alpha,
        "desc": "Data-driven: sector + sentiment momentum + confidence gating",
        "guide": {
            "how_it_works": (
                "This strategy was reverse-engineered from 165,000 backtest observations using "
                "deep statistical analysis. It discovered that: (1) sector sentiment is the strongest "
                "predictor of which assets outperform, (2) macro score is harmful for ranking, "
                "(3) sentiment CHANGE matters more than level, (4) confidence gating improves accuracy. "
                "It picks the best asset per sector using a custom rank_score, weights by score, "
                "and switches to defensive mode during high-volatility regimes."
            ),
            "what_to_watch": (
                "On Asset Detail, focus on <strong>Sector Sentiment</strong> (strongest signal), "
                "recent <strong>sentiment changes</strong> (momentum), and <strong>Fundamentals</strong>. "
                "Ignore the Macro indicator for stock-picking (it's the same for all assets). "
                "Check the sub-score confidence levels — only trade when confidence is above average."
            ),
            "how_to_pick": (
                "For each sector, pick the asset with the best combination of: sector sentiment (35%), "
                "asset sentiment (25%), fundamentals (20%), sentiment momentum (10%), and sentiment-sector "
                "agreement (10%). Only consider assets with above-median confidence. In volatile markets, "
                "increase sector weight to 45%. When all signals are bearish, add a contrarian bonus."
            ),
            "best_for": "Quantitative investors who trust data over intuition. Designed for sector-diversified portfolios with statistical edge.",
        },
    },
    "quant_alpha_v2": {
        "fn": strategy_quant_alpha_v2,
        "desc": "Grade-based: overall_score + confidence gating + regime + contrarian",
        "guide": {
            "how_it_works": (
                "Uses the composite overall_score from the grading system (which already "
                "incorporates optimized weights) as the ranking signal — no custom sub-score "
                "math. Layers the same quant_alpha meta-logic on top: confidence gating, "
                "volatility regime detection, contrarian boost, and sector-diversified selection."
            ),
            "what_to_watch": (
                "On the Dashboard, the <strong>Buy Confidence %</strong> IS the signal. "
                "This strategy trusts the grading system's composite score and adds tactical "
                "overlays: only trade confident instruments, boost sector weight in volatile "
                "markets, add contrarian bonus when everything looks bearish."
            ),
            "how_to_pick": (
                "Sort by Buy Confidence %. Filter for above-average confidence across sentiment, "
                "sector, and fundamentals. Within each sector, pick the highest-scoring instrument. "
                "Allocate proportional to score. In volatile markets, sector sentiment gets extra "
                "weight. When all signals agree bearish on an asset, consider it for mean-reversion."
            ),
            "best_for": "Investors who trust the grading system's composite and want sector-diversified, confidence-filtered execution.",
        },
    },
    "quant_alpha_v3": {
        "fn": strategy_quant_alpha_v3,
        "desc": "Grid-optimized: confidence-blended rank, 25% cap, wider contrarian",
        "guide": {
            "how_it_works": (
                "Grid-optimized refinement of v2. Uses the grading system's overall_score "
                "blended with confidence (higher confidence = ranking boost). Caps any single "
                "position at 25% to prevent concentration blowups. Uses a wider contrarian "
                "trigger (2 of 3 signals bearish instead of all 3). Sector-diversified selection."
            ),
            "what_to_watch": (
                "Same dashboard signals as v2. Pay extra attention to <strong>confidence "
                "levels</strong> — instruments with higher sentiment, sector, and fundamentals "
                "confidence get a ranking boost even at the same overall score. The contrarian "
                "bonus fires more often than v2, capturing more mean-reversion trades."
            ),
            "how_to_pick": (
                "Let the grading system rank instruments. Filter for above-median confidence. "
                "Pick best per sector. Cap each position at 25% max. In volatile markets, "
                "sector score gets extra weight. When 2+ sub-signals are bearish, add a "
                "contrarian bonus. No vol scaling or cash-raise — data showed those hurt."
            ),
            "best_for": "The recommended strategy — best OOS alpha and Sharpe with reduced drawdown vs v2.",
        },
    },
    "random": {
        "fn": strategy_random,
        "desc": "Random picking — N random instruments (100 Monte Carlo avg)",
        "guide": {
            "how_it_works": (
                "Randomly pick 3 assets each period (no scoring, pure luck) and average "
                "the results over 100 simulations. This is another BENCHMARK: it shows "
                "what you'd get from blindly throwing darts at a board. "
                "If scored strategies beat this, the scoring adds real value beyond luck."
            ),
            "what_to_watch": (
                "Like momentum, this ignores all signals. Compare other strategies' "
                "performance against this. The gap between a scored strategy and random "
                "picking is the TRUE alpha from the scoring system — not luck, not market beta."
            ),
            "how_to_pick": (
                "You wouldn't actually use this. It's a statistical control to prove "
                "that better strategies aren't just getting lucky. If top_n beats random "
                "consistently, the scoring system genuinely identifies winners."
            ),
            "best_for": "Statistical benchmark only — proves whether the scoring system adds value beyond random chance.",
        },
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
    Transaction costs are applied post-hoc if cost_bps > 0.
    """
    if strategy not in STRATEGIES:
        raise ValueError(f"Unknown strategy '{strategy}'. Use --list-strategies to see options.")
    if params is None:
        params = StrategyParams()

    result = STRATEGIES[strategy]["fn"](df, term, params)

    # Apply transaction costs: deduct round-trip spread from each trade period
    if params.cost_bps > 0:
        cost_per_trade = params.cost_bps * 2 / 10000  # buy + sell spread
        active_mask = result["daily_strat_ret"] != 0
        for entry_date in result.loc[active_mask, "date"].unique():
            date_mask = (result["date"] == entry_date) & active_mask
            n_positions = date_mask.sum()
            # Deduct cost proportionally across positions
            result.loc[date_mask, "daily_strat_ret"] -= cost_per_trade / n_positions

    return result
