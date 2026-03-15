"""Walk-forward out-of-sample testing.

Splits the backtest period into rolling windows:
  - Train on 12 months of data
  - Test on the next 3 months
  - Roll forward by 3 months

For each window:
  1. Calibrate weights on train data
  2. Re-score test data with calibrated weights
  3. Apply strategy, compute KPIs on test period
  4. Compare against default weights and benchmark

This tests whether the signal is stable across time periods and
whether weight optimization generalizes out-of-sample.
"""

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from .calibrator import run_all_calibrations
from .simulator import COMPOSITE_WEIGHT_PROFILES, simulate_grade

logger = logging.getLogger(__name__)

TRAIN_MONTHS = 12
TEST_MONTHS = 3


@dataclass
class WindowResult:
    """Results for a single walk-forward window."""
    window_num: int
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    n_train: int
    n_test: int
    # Default weights performance
    default_return: float
    default_sharpe: float
    default_ic: float
    # Calibrated weights performance
    calibrated_return: float
    calibrated_sharpe: float
    calibrated_ic: float
    # Benchmark (equal-weight)
    benchmark_return: float
    # Alpha
    default_alpha: float
    calibrated_alpha: float


def _sharpe(returns: np.ndarray, periods_per_year: float = 52.0) -> float:
    """Annualized Sharpe ratio."""
    if len(returns) < 2 or np.std(returns, ddof=1) == 0:
        return 0.0
    return float(np.mean(returns) / np.std(returns, ddof=1) * np.sqrt(periods_per_year))


def _ic(scores: np.ndarray, returns: np.ndarray) -> float:
    """Information coefficient (Spearman rank correlation)."""
    if len(scores) < 5:
        return 0.0
    try:
        r, _ = spearmanr(scores, returns)
        return float(r) if not np.isnan(r) else 0.0
    except Exception:
        return 0.0


def _compute_returns_with_weights(
    rows: list[dict],
    weights: dict[str, float] | None,
    term: str,
    holding_days: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Re-score rows with given weights and compute strategy returns.

    Returns (scores, forward_returns, period_returns for top-3 strategy).
    """
    ret_key = "return_5d" if term == "short" else "return_20d"

    scored = []
    for row in rows:
        sub_scores = {
            "technical": row["technical"],
            "technical_conf": row["technical_conf"],
            "sentiment": row["sentiment"],
            "sentiment_conf": row["sentiment_conf"],
            "sector": row["sector"],
            "sector_conf": row["sector_conf"],
            "macro": row["macro"],
            "macro_conf": row["macro_conf"],
            "fundamentals": row["fundamentals"],
            "fundamentals_conf": row["fundamentals_conf"],
        }
        grade = simulate_grade(sub_scores, row["category"], term, weight_override=weights)
        fwd_ret = row.get(ret_key)
        if fwd_ret is not None:
            scored.append({
                "date": row["date"],
                "symbol": row["symbol"],
                "score": grade["overall_score"],
                "return": fwd_ret,
            })

    if not scored:
        return np.array([]), np.array([]), np.array([])

    df = pd.DataFrame(scored).sort_values("date")
    scores = df["score"].values
    returns = df["return"].values

    # Compute top-3 strategy returns (non-overlapping periods)
    dates = sorted(df["date"].unique())
    entry_dates = []
    i = 0
    while i < len(dates):
        entry_dates.append(dates[i])
        i += holding_days

    period_returns = []
    for ed in entry_dates:
        day_df = df[df["date"] == ed]
        if len(day_df) < 3:
            continue
        top3 = day_df.nlargest(3, "score")
        period_ret = top3["return"].mean()
        period_returns.append(period_ret)

    return scores, returns, np.array(period_returns)


def _benchmark_returns(rows: list[dict], term: str, holding_days: int) -> np.ndarray:
    """Equal-weight benchmark returns for the same periods."""
    ret_key = "return_5d" if term == "short" else "return_20d"
    data = [{"date": r["date"], "return": r[ret_key]} for r in rows if r.get(ret_key) is not None]
    if not data:
        return np.array([])

    df = pd.DataFrame(data)
    dates = sorted(df["date"].unique())
    entry_dates = []
    i = 0
    while i < len(dates):
        entry_dates.append(dates[i])
        i += holding_days

    returns = []
    for ed in entry_dates:
        day_df = df[df["date"] == ed]
        if day_df.empty:
            continue
        returns.append(day_df["return"].mean())
    return np.array(returns)


def run_walk_forward(
    all_rows: list[dict],
    term: str = "short",
    train_months: int = TRAIN_MONTHS,
    test_months: int = TEST_MONTHS,
) -> list[WindowResult]:
    """Run walk-forward analysis on backtest data.

    Args:
        all_rows: Full backtest results (from load_backtest_results).
        term: 'short' or 'long'.
        train_months: Training window in months.
        test_months: Test window in months (also the roll step).

    Returns list of WindowResult for each window.
    """
    if not all_rows:
        return []

    holding_days = 5 if term == "short" else 20
    periods_per_year = 52.0 if term == "short" else 13.0

    # Get date range
    all_dates = sorted(set(r["date"] for r in all_rows))
    first_date = all_dates[0]
    last_date = all_dates[-1]

    logger.info(
        "Walk-forward: %s to %s, train=%dmo, test=%dmo, term=%s",
        first_date, last_date, train_months, test_months, term,
    )

    # Generate windows
    windows = []
    window_num = 1
    train_start = first_date

    while True:
        train_end = train_start + timedelta(days=train_months * 30)
        test_start = train_end + timedelta(days=1)
        test_end = test_start + timedelta(days=test_months * 30)

        if test_end > last_date:
            break

        # Split rows
        train_rows = [r for r in all_rows if train_start <= r["date"] <= train_end]
        test_rows = [r for r in all_rows if test_start <= r["date"] <= test_end]

        if len(train_rows) < 100 or len(test_rows) < 20:
            train_start = train_start + timedelta(days=test_months * 30)
            continue

        logger.info(
            "Window %d: train %s→%s (%d rows), test %s→%s (%d rows)",
            window_num, train_start, train_end, len(train_rows),
            test_start, test_end, len(test_rows),
        )

        # --- Default weights: test set performance ---
        def_scores, def_returns, def_period_rets = _compute_returns_with_weights(
            test_rows, None, term, holding_days
        )
        default_return = float((1 + def_period_rets).prod() - 1) if len(def_period_rets) > 0 else 0.0
        default_sharpe = _sharpe(def_period_rets, periods_per_year)
        default_ic = _ic(def_scores, def_returns)

        # --- Calibrate on train set ---
        try:
            cal_results = run_all_calibrations(train_rows)
        except Exception as e:
            logger.warning("Calibration failed for window %d: %s", window_num, e)
            train_start = train_start + timedelta(days=test_months * 30)
            window_num += 1
            continue

        # Find the best calibrated weights for this category/term combo
        cal_key = None
        for key, result in cal_results.items():
            if result.get("term") == term:
                cal_key = key
                break

        if cal_key and cal_results[cal_key].get("status") == "apply":
            cal_weights = cal_results[cal_key]["weights_after"]
        else:
            cal_weights = None  # Use default

        # --- Calibrated weights: test set performance ---
        cal_scores, cal_returns, cal_period_rets = _compute_returns_with_weights(
            test_rows, cal_weights, term, holding_days
        )
        cal_return = float((1 + cal_period_rets).prod() - 1) if len(cal_period_rets) > 0 else 0.0
        cal_sharpe = _sharpe(cal_period_rets, periods_per_year)
        cal_ic = _ic(cal_scores, cal_returns)

        # --- Benchmark ---
        bench_rets = _benchmark_returns(test_rows, term, holding_days)
        bench_return = float((1 + bench_rets).prod() - 1) if len(bench_rets) > 0 else 0.0

        windows.append(WindowResult(
            window_num=window_num,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            n_train=len(train_rows),
            n_test=len(test_rows),
            default_return=default_return,
            default_sharpe=default_sharpe,
            default_ic=default_ic,
            calibrated_return=cal_return,
            calibrated_sharpe=cal_sharpe,
            calibrated_ic=cal_ic,
            benchmark_return=bench_return,
            default_alpha=default_return - bench_return,
            calibrated_alpha=cal_return - bench_return,
        ))

        train_start = train_start + timedelta(days=test_months * 30)
        window_num += 1

    return windows


def print_walk_forward_results(results: list[WindowResult]) -> None:
    """Print formatted walk-forward results table."""
    if not results:
        print("\nNo walk-forward windows generated.")
        return

    print(f"\n{'=' * 130}")
    print(f"  Walk-Forward Out-of-Sample Results (top-3 strategy)")
    print(f"{'=' * 130}")
    print(f"  {'Win':>4} {'Test Period':<25} {'N':>5} "
          f"{'Default':>10} {'Calib':>10} {'Bench':>10} "
          f"{'Def Alpha':>10} {'Cal Alpha':>10} "
          f"{'Def IC':>8} {'Cal IC':>8}")
    print(f"  {'-'*4} {'-'*25} {'-'*5} "
          f"{'-'*10} {'-'*10} {'-'*10} "
          f"{'-'*10} {'-'*10} "
          f"{'-'*8} {'-'*8}")

    for w in results:
        period = f"{w.test_start} → {w.test_end}"
        print(
            f"  {w.window_num:>4} {period:<25} {w.n_test:>5} "
            f"{w.default_return*100:>+9.1f}% "
            f"{w.calibrated_return*100:>+9.1f}% "
            f"{w.benchmark_return*100:>+9.1f}% "
            f"{w.default_alpha*100:>+9.1f}% "
            f"{w.calibrated_alpha*100:>+9.1f}% "
            f"{w.default_ic:>+7.3f} "
            f"{w.calibrated_ic:>+7.3f}"
        )

    # Summary stats
    def_alphas = [w.default_alpha for w in results]
    cal_alphas = [w.calibrated_alpha for w in results]
    def_ics = [w.default_ic for w in results]

    n_positive_alpha = sum(1 for a in def_alphas if a > 0)
    avg_alpha = np.mean(def_alphas) * 100
    avg_ic = np.mean(def_ics)

    print(f"\n  {'Summary':}")
    print(f"    Windows with positive alpha: {n_positive_alpha}/{len(results)} ({100*n_positive_alpha/len(results):.0f}%)")
    print(f"    Avg default alpha: {avg_alpha:+.2f}%")
    print(f"    Avg calibrated alpha: {np.mean(cal_alphas)*100:+.2f}%")
    print(f"    Avg default IC: {avg_ic:+.4f}")
    print(f"    Calibration beats default: {sum(1 for d, c in zip(def_alphas, cal_alphas) if c > d)}/{len(results)} windows")
    print(f"{'=' * 130}\n")
