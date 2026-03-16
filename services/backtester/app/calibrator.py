"""Weight calibration using scipy.optimize.

Optimizes composite signal weights to maximize a blended objective
(cross-sectional IC + Sharpe) on a training set (past 18 months).
Validates on holdout set (most recent 6 months).

One calibration run per (category, term) combination.

Statistical fixes applied:
  - Sharpe uses directional PnL (sign(score) * return), not score * return
  - Annualization factor matches holding period (252/5 for short, 252/20 for long)
  - Cross-sectional IC (daily Spearman averaged) used in objective, not pooled IC
  - Risk-free rate subtracted from PnL
"""

import json
import logging
import math
from datetime import date, timedelta

import numpy as np
from scipy.optimize import minimize
from sqlalchemy import text

from .db import async_session
from .simulator import (
    PRODUCTION_WEIGHT_PROFILES,
    EXPERIMENTAL_WEIGHT_PROFILES,
    PRODUCTION_WEIGHT_KEYS,
    EXPERIMENTAL_WEIGHT_KEYS,
    SimMode,
    compute_composite_with_weights,
    _get_profiles_and_keys,
)

logger = logging.getLogger(__name__)

# Calibration constraints
MIN_WEIGHT = 0.07   # No signal < 7% — prevents zeroing out signals
MAX_WEIGHT = 0.45   # No signal > 45% — prevents over-concentration
MIN_SHARPE_IMPROVEMENT = 0.20  # Only update weights if validation metric improves by this much

# Approximate annualized risk-free rate over 2020-2026 (avg of ~0% to ~5%)
RISK_FREE_ANNUAL = 0.03


def _sharpe(
    scores: list[float],
    returns: list[float],
    holding_days: int = 5,
) -> float:
    """Compute annualized Sharpe ratio of a directional signal strategy.

    PnL per period = sign(score) * return - rf_per_period.
    Uses unit position sizing (long $1 if score > 0, short $1 if score < 0,
    flat if score == 0) for scale-invariant measurement.

    Annualization: sqrt(periods_per_year) where periods_per_year = 252 / holding_days.
    """
    if len(scores) < 10:
        return 0.0

    rf_per_period = RISK_FREE_ANNUAL * holding_days / 252.0

    pnl = []
    for s, r in zip(scores, returns):
        if abs(s) < 1e-6:
            pnl.append(-rf_per_period)  # Flat = earn nothing minus rf
        else:
            direction = 1.0 if s > 0 else -1.0
            pnl.append(direction * r - rf_per_period)

    mean_pnl = np.mean(pnl)
    std_pnl = np.std(pnl, ddof=1)
    if std_pnl == 0:
        return 0.0

    periods_per_year = 252.0 / holding_days
    return float(mean_pnl / std_pnl * math.sqrt(periods_per_year))


def _directional_accuracy(scores: list[float], returns: list[float]) -> float:
    """% of signals where sign(score) == sign(return)."""
    if not scores:
        return 0.5
    correct = sum(1 for s, r in zip(scores, returns) if s * r > 0)
    return correct / len(scores)


def _information_coefficient(scores: list[float], returns: list[float]) -> float:
    """Pooled Spearman rank correlation between scores and returns.

    Kept for reporting but NOT used in the optimization objective.
    Use _cross_sectional_ic() for the objective instead.
    """
    if len(scores) < 5:
        return 0.0
    from scipy.stats import spearmanr
    corr, _ = spearmanr(scores, returns)
    return float(corr) if not math.isnan(corr) else 0.0


def _cross_sectional_ic(
    rows: list[dict],
    scores: list[float],
    return_col: str = "return_20d",
    min_instruments: int = 5,
) -> float:
    """Compute mean daily cross-sectional IC (the correct measure for ranking signals).

    For each date with >= min_instruments observations, compute Spearman rank
    correlation between scores and returns cross-sectionally. Return the mean.

    This avoids confounding time-series correlation with cross-sectional signal quality.
    """
    from scipy.stats import spearmanr
    from collections import defaultdict

    date_groups: dict[date, list[tuple[float, float]]] = defaultdict(list)
    for row, score in zip(rows, scores):
        ret = row.get(return_col)
        if ret is not None:
            date_groups[row["date"]].append((score, ret))

    daily_ics = []
    for d, pairs in date_groups.items():
        if len(pairs) < min_instruments:
            continue
        s_vals, r_vals = zip(*pairs)
        corr, _ = spearmanr(s_vals, r_vals)
        if not math.isnan(corr):
            daily_ics.append(corr)

    if not daily_ics:
        return 0.0
    return float(np.mean(daily_ics))


def _filter_rows(rows: list[dict], category: str, term_return_col: str = "return_20d") -> list[dict]:
    """Filter rows by category, remove rows with missing returns."""
    target_cat = category.lower()
    return [
        r for r in rows
        if r.get("category", "").lower() == target_cat and r.get(term_return_col) is not None
    ]


def _train_test_split(rows: list[dict], holdout_months: int = 6) -> tuple[list[dict], list[dict]]:
    """Split rows into training and holdout (most recent holdout_months)."""
    if not rows:
        return [], []
    dates = [r["date"] for r in rows]
    max_date = max(dates)
    holdout_start = max_date - timedelta(days=holdout_months * 30)
    train = [r for r in rows if r["date"] < holdout_start]
    test = [r for r in rows if r["date"] >= holdout_start]
    return train, test


def calibrate_weights(
    rows: list[dict],
    category: str,
    term: str,
    return_col: str = "return_20d",
    mode: SimMode = "production",
) -> dict:
    """Run scipy.optimize to find weights maximizing blended objective on training set.

    Objective: 70% cross-sectional IC + 30% Sharpe (directional, annualized).

    Returns a dict with optimized weights, metrics before/after calibration,
    and whether the weights should be applied (based on validation improvement).
    """
    profiles, weight_keys = _get_profiles_and_keys(mode)

    # Get current (baseline) weights
    cat_key = category.lower()
    profile = profiles.get(cat_key, profiles["stock"])
    current_weights = profile.get(term, profile["short"]).copy()

    holding_days = 5 if term == "short" else 20

    # Filter and split
    category_rows = _filter_rows(rows, category, return_col)
    if len(category_rows) < 30:
        logger.warning("[%s/%s] Only %d rows — skipping calibration", category, term, len(category_rows))
        return {
            "category": category,
            "term": term,
            "status": "skipped_insufficient_data",
            "weights_before": current_weights,
            "weights_after": current_weights,
            "n_samples": len(category_rows),
        }

    train_rows, test_rows = _train_test_split(category_rows)
    if len(train_rows) < 20:
        logger.warning("[%s/%s] Only %d training rows — skipping", category, term, len(train_rows))
        return {
            "category": category,
            "term": term,
            "status": "skipped_insufficient_train_data",
            "weights_before": current_weights,
            "weights_after": current_weights,
            "n_samples": len(category_rows),
        }

    train_returns = [r[return_col] for r in train_rows]
    test_returns = [r[return_col] for r in test_rows] if test_rows else []

    # Baseline metrics
    baseline_scores_train = compute_composite_with_weights(train_rows, current_weights, term, mode=mode)
    baseline_sharpe_train = _sharpe(baseline_scores_train, train_returns, holding_days)
    baseline_da_train = _directional_accuracy(baseline_scores_train, train_returns)

    baseline_scores_test = compute_composite_with_weights(test_rows, current_weights, term, mode=mode) if test_rows else []
    baseline_sharpe_test = _sharpe(baseline_scores_test, test_returns, holding_days) if test_rows else 0.0

    baseline_ic_train = _cross_sectional_ic(train_rows, baseline_scores_train, return_col)
    baseline_ic_test = _cross_sectional_ic(test_rows, baseline_scores_test, return_col) if test_rows else 0.0

    # Also compute pooled IC for reporting
    baseline_pooled_ic_train = _information_coefficient(baseline_scores_train, train_returns)
    baseline_pooled_ic_test = _information_coefficient(baseline_scores_test, test_returns) if test_rows else 0.0

    logger.info(
        "[%s/%s] Baseline — train Sharpe: %.3f, xsIC: %.4f (pooled: %.4f), DA: %.1f%%, "
        "test Sharpe: %.3f, xsIC: %.4f (pooled: %.4f)",
        category, term, baseline_sharpe_train, baseline_ic_train, baseline_pooled_ic_train,
        baseline_da_train * 100,
        baseline_sharpe_test, baseline_ic_test, baseline_pooled_ic_test,
    )

    # Blended objective: 70% cross-sectional IC + 30% Sharpe
    # Cross-sectional IC is the proper measure for ranking signals
    def neg_objective(weights_arr: np.ndarray) -> float:
        w = dict(zip(weight_keys, weights_arr))
        scores = compute_composite_with_weights(train_rows, w, term, mode=mode)
        sharpe = _sharpe(scores, train_returns, holding_days)
        xs_ic = _cross_sectional_ic(train_rows, scores, return_col)
        return -(0.7 * xs_ic + 0.3 * sharpe)

    # Bounds: each weight in [MIN_WEIGHT, MAX_WEIGHT]
    # For commodity, fundamentals must be 0; earnings also 0 in experimental mode
    bounds = []
    x0 = []
    for key in weight_keys:
        w0 = current_weights.get(key, 0.0)
        if category == "commodity" and key in ("fundamentals", "earnings"):
            bounds.append((0.0, 0.0))
            x0.append(0.0)
        else:
            bounds.append((MIN_WEIGHT, MAX_WEIGHT))
            x0.append(max(MIN_WEIGHT, min(MAX_WEIGHT, w0)))

    # Normalize x0 to sum to 1.0
    x0_sum = sum(x0)
    x0 = [v / x0_sum for v in x0]

    # Constraints: weights sum to 1.0
    constraints = [{"type": "eq", "fun": lambda w: sum(w) - 1.0}]

    try:
        # Strategy 1: SLSQP with proper step size (local optimizer)
        result_slsqp = minimize(
            neg_objective,
            x0=x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"ftol": 1e-6, "eps": 1e-4, "maxiter": 500, "disp": False},
        )

        # Strategy 2: Multi-start with random initial points on the simplex
        best_result = result_slsqp
        n_restarts = 20
        rng = np.random.default_rng(42)

        locked_keys = set()
        if category == "commodity":
            locked_keys = {"fundamentals", "earnings"}

        for _ in range(n_restarts):
            # Random point on the simplex via Dirichlet distribution
            free_count = sum(1 for k in weight_keys if k not in locked_keys)
            raw = list(rng.dirichlet([1.0] * free_count))
            x_rand = []
            ri = 0
            for k in weight_keys:
                if k in locked_keys:
                    x_rand.append(0.0)
                else:
                    x_rand.append(raw[ri])
                    ri += 1

            # Clip to bounds
            x_rand = [max(b[0], min(b[1], v)) for v, b in zip(x_rand, bounds)]
            x_sum = sum(x_rand)
            if x_sum > 0:
                x_rand = [v / x_sum for v in x_rand]

            trial = minimize(
                neg_objective,
                x0=x_rand,
                method="SLSQP",
                bounds=bounds,
                constraints=constraints,
                options={"ftol": 1e-6, "eps": 1e-4, "maxiter": 500, "disp": False},
            )
            if trial.fun < best_result.fun:
                best_result = trial

        result = best_result

        if not result.success:
            logger.warning("[%s/%s] Optimizer did not converge: %s", category, term, result.message)

        optimized_weights = dict(zip(weight_keys, result.x))
        # Round to 4 decimal places, ensure still sums to 1.0 within tolerance
        optimized_weights = {k: round(v, 4) for k, v in optimized_weights.items()}

    except Exception:
        logger.exception("[%s/%s] Optimization failed", category, term)
        optimized_weights = current_weights

    # Validation metrics
    opt_scores_train = compute_composite_with_weights(train_rows, optimized_weights, term, mode=mode)
    opt_sharpe_train = _sharpe(opt_scores_train, train_returns, holding_days)
    opt_da_train = _directional_accuracy(opt_scores_train, train_returns)
    opt_ic_train = _cross_sectional_ic(train_rows, opt_scores_train, return_col)

    opt_scores_test = compute_composite_with_weights(test_rows, optimized_weights, term, mode=mode) if test_rows else []
    opt_sharpe_test = _sharpe(opt_scores_test, test_returns, holding_days) if test_rows else 0.0
    opt_ic_test = _cross_sectional_ic(test_rows, opt_scores_test, return_col) if test_rows else 0.0

    logger.info(
        "[%s/%s] Optimized — train Sharpe: %.3f, xsIC: %.4f, DA: %.1f%%, test Sharpe: %.3f, xsIC: %.4f",
        category, term, opt_sharpe_train, opt_ic_train, opt_da_train * 100, opt_sharpe_test, opt_ic_test,
    )

    # Decide whether to apply: blended metric (IC + Sharpe) must improve on test set
    if test_rows:
        baseline_blend = 0.7 * baseline_ic_test + 0.3 * baseline_sharpe_test
        opt_blend = 0.7 * opt_ic_test + 0.3 * opt_sharpe_test
        blend_delta = opt_blend - baseline_blend
    else:
        baseline_blend = 0.7 * baseline_ic_train + 0.3 * baseline_sharpe_train
        opt_blend = 0.7 * opt_ic_train + 0.3 * opt_sharpe_train
        blend_delta = opt_blend - baseline_blend

    should_apply = blend_delta >= MIN_SHARPE_IMPROVEMENT

    logger.info(
        "[%s/%s] Decision: %s (blended delta: %.4f, Sharpe delta: %.3f, xsIC delta: %.4f, threshold: %.3f)",
        category, term,
        "APPLY" if should_apply else "KEEP CURRENT",
        blend_delta,
        opt_sharpe_test - baseline_sharpe_test if test_rows else opt_sharpe_train - baseline_sharpe_train,
        opt_ic_test - baseline_ic_test if test_rows else opt_ic_train - baseline_ic_train,
        MIN_SHARPE_IMPROVEMENT,
    )

    return {
        "category": category,
        "term": term,
        "status": "apply" if should_apply else "no_improvement",
        "weights_before": current_weights,
        "weights_after": optimized_weights if should_apply else current_weights,
        "sharpe_before": round(baseline_sharpe_test or baseline_sharpe_train, 4),
        "sharpe_after": round(opt_sharpe_test or opt_sharpe_train, 4),
        "ic_before": round(baseline_ic_test or baseline_ic_train, 4),
        "ic_after": round(opt_ic_test or opt_ic_train, 4),
        "directional_accuracy_before": round(baseline_da_train, 4),
        "directional_accuracy_after": round(opt_da_train, 4),
        "n_train": len(train_rows),
        "n_test": len(test_rows),
        "n_samples": len(category_rows),
    }


async def save_calibration_run(cal_result: dict) -> None:
    """Store calibration run result in the calibration_runs table."""
    import json

    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO calibration_runs
                  (term, category, weights_before, weights_after,
                   sharpe_before, sharpe_after,
                   directional_accuracy_before, directional_accuracy_after,
                   n_samples)
                VALUES
                  (:term, :cat,
                   CAST(:wb AS jsonb), CAST(:wa AS jsonb),
                   :sb, :sa, :dab, :daa, :n)
            """),
            {
                "term": cal_result["term"],
                "cat": cal_result["category"],
                "wb": json.dumps(cal_result["weights_before"]),
                "wa": json.dumps(cal_result["weights_after"]),
                "sb": cal_result.get("sharpe_before"),
                "sa": cal_result.get("sharpe_after"),
                "dab": cal_result.get("directional_accuracy_before"),
                "daa": cal_result.get("directional_accuracy_after"),
                "n": cal_result.get("n_samples"),
            },
        )
        await session.commit()
    logger.info("[DB] Saved calibration run for %s/%s", cal_result["category"], cal_result["term"])


def run_all_calibrations(
    backtest_rows: list[dict],
    mode: SimMode = "production",
) -> dict[tuple[str, str], dict]:
    """Run calibration for all (category, term) combinations.

    Returns dict: {(category, term): calibration_result}.
    """
    combinations = [
        ("stock",     "short", "return_5d"),
        ("stock",     "long",  "return_20d"),
        ("etf",       "short", "return_5d"),
        ("etf",       "long",  "return_20d"),
        ("commodity", "short", "return_5d"),
        ("commodity", "long",  "return_20d"),
    ]

    results = {}
    for category, term, return_col in combinations:
        logger.info("=== Calibrating %s/%s (%s) [mode=%s] ===", category, term, return_col, mode)
        result = calibrate_weights(backtest_rows, category, term, return_col=return_col, mode=mode)
        results[(category, term)] = result

        # Print summary
        print(f"\n{'=' * 60}")
        print(f"  {category.upper()} / {term.upper()} [{mode}]")
        print(f"  Status: {result['status']}")
        print(f"  Samples: {result.get('n_samples', 0)}")
        print(f"  Sharpe before: {result.get('sharpe_before', 0):.3f}")
        print(f"  Sharpe after:  {result.get('sharpe_after', 0):.3f}")
        print(f"  xsIC before:   {result.get('ic_before', 0):.4f}")
        print(f"  xsIC after:    {result.get('ic_after', 0):.4f}")
        print(f"  DA before: {(result.get('directional_accuracy_before', 0)*100):.1f}%")
        print(f"  DA after:  {(result.get('directional_accuracy_after', 0)*100):.1f}%")
        if result["status"] == "apply":
            print(f"  NEW WEIGHTS:")
            for k, v in result["weights_after"].items():
                old = result["weights_before"].get(k, 0)
                delta = v - old
                print(f"    {k:12s}: {old:.3f} -> {v:.3f}  ({delta:+.3f})")
        else:
            print(f"  -> Keeping current weights (insufficient improvement)")

    return results
