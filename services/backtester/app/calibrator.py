"""Weight calibration using scipy.optimize.

Optimizes composite signal weights (technical, sentiment, sector, macro, fundamentals)
to maximize Sharpe ratio on a training set (past 18 months).
Validates on holdout set (most recent 6 months).

One calibration run per (category, term) combination.
"""

import json
import logging
import math
from datetime import date, timedelta

import numpy as np
from scipy.optimize import minimize
from sqlalchemy import text

from .db import async_session
from .simulator import COMPOSITE_WEIGHT_PROFILES, WEIGHT_KEYS, compute_composite_with_weights

logger = logging.getLogger(__name__)

# Calibration constraints
MIN_WEIGHT = 0.05   # No signal < 5%
MAX_WEIGHT = 0.60   # No signal > 60%
MIN_SHARPE_IMPROVEMENT = 0.10  # Only update weights if validation Sharpe improves by this much


def _sharpe(scores: list[float], returns: list[float]) -> float:
    """Compute annualized Sharpe ratio of a long-only signal strategy.

    Signal = overall_score (used as position size direction proxy).
    PnL = score * forward_return for each date.
    Sharpe = mean(pnl) / std(pnl) * sqrt(252 / sample_frequency).
    """
    if len(scores) < 10:
        return 0.0
    pnl = [s * r for s, r in zip(scores, returns)]
    mean_pnl = np.mean(pnl)
    std_pnl = np.std(pnl)
    if std_pnl == 0:
        return 0.0
    # Annualization factor: assume weekly sampling (52 samples/year)
    return float(mean_pnl / std_pnl * math.sqrt(52))


def _directional_accuracy(scores: list[float], returns: list[float]) -> float:
    """% of signals where sign(score) == sign(return)."""
    if not scores:
        return 0.5
    correct = sum(1 for s, r in zip(scores, returns) if s * r > 0)
    return correct / len(scores)


def _information_coefficient(scores: list[float], returns: list[float]) -> float:
    """Spearman rank correlation between scores and returns."""
    if len(scores) < 5:
        return 0.0
    from scipy.stats import spearmanr
    corr, _ = spearmanr(scores, returns)
    return float(corr) if not math.isnan(corr) else 0.0


def _filter_rows(rows: list[dict], category: str, term_return_col: str = "return_20d") -> list[dict]:
    """Filter rows by category, remove rows with missing returns."""
    return [r for r in rows if r.get("category") == category and r.get(term_return_col) is not None]


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
) -> dict:
    """Run scipy.optimize to find weights maximizing Sharpe on training set.

    Returns a dict with optimized weights, metrics before/after calibration,
    and whether the weights should be applied (based on validation improvement).
    """
    # Get current (baseline) weights
    profile = COMPOSITE_WEIGHT_PROFILES.get(category, COMPOSITE_WEIGHT_PROFILES["stock"])
    current_weights = profile.get(term, profile["short"]).copy()

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
    baseline_scores_train = compute_composite_with_weights(train_rows, current_weights, term)
    baseline_sharpe_train = _sharpe(baseline_scores_train, train_returns)
    baseline_da_train = _directional_accuracy(baseline_scores_train, train_returns)

    baseline_scores_test = compute_composite_with_weights(test_rows, current_weights, term) if test_rows else []
    baseline_sharpe_test = _sharpe(baseline_scores_test, test_returns) if test_rows else 0.0

    logger.info(
        "[%s/%s] Baseline — train Sharpe: %.3f, DA: %.1f%%, test Sharpe: %.3f",
        category, term, baseline_sharpe_train, baseline_da_train * 100, baseline_sharpe_test,
    )

    # Optimization objective: maximize Sharpe on training set
    def neg_sharpe(weights_arr: np.ndarray) -> float:
        w = dict(zip(WEIGHT_KEYS, weights_arr))
        scores = compute_composite_with_weights(train_rows, w, term)
        return -_sharpe(scores, train_returns)

    # Constraints: weights sum to 1.0; commodity has 0% fundamentals
    constraints = [{"type": "eq", "fun": lambda w: sum(w) - 1.0}]

    # Bounds: each weight in [MIN_WEIGHT, MAX_WEIGHT]
    # For commodity, fundamentals must be 0
    bounds = []
    x0 = []
    for key in WEIGHT_KEYS:
        w0 = current_weights.get(key, 0.0)
        if category == "commodity" and key == "fundamentals":
            bounds.append((0.0, 0.0))
            x0.append(0.0)
        else:
            bounds.append((MIN_WEIGHT, MAX_WEIGHT))
            x0.append(max(MIN_WEIGHT, min(MAX_WEIGHT, w0)))

    # Normalize x0 to sum to 1.0
    x0_sum = sum(x0)
    x0 = [v / x0_sum for v in x0]

    try:
        result = minimize(
            neg_sharpe,
            x0=x0,
            method="SLSQP",
            bounds=bounds,
            constraints=constraints,
            options={"ftol": 1e-9, "maxiter": 500, "disp": False},
        )

        if not result.success:
            logger.warning("[%s/%s] Optimizer did not converge: %s", category, term, result.message)

        optimized_weights = dict(zip(WEIGHT_KEYS, result.x))
        # Round to 4 decimal places, ensure still sums to 1.0 within tolerance
        optimized_weights = {k: round(v, 4) for k, v in optimized_weights.items()}

    except Exception:
        logger.exception("[%s/%s] Optimization failed", category, term)
        optimized_weights = current_weights

    # Validation metrics
    opt_scores_train = compute_composite_with_weights(train_rows, optimized_weights, term)
    opt_sharpe_train = _sharpe(opt_scores_train, train_returns)
    opt_da_train = _directional_accuracy(opt_scores_train, train_returns)

    opt_scores_test = compute_composite_with_weights(test_rows, optimized_weights, term) if test_rows else []
    opt_sharpe_test = _sharpe(opt_scores_test, test_returns) if test_rows else 0.0

    logger.info(
        "[%s/%s] Optimized — train Sharpe: %.3f, DA: %.1f%%, test Sharpe: %.3f",
        category, term, opt_sharpe_train, opt_da_train * 100, opt_sharpe_test,
    )

    # Decide whether to apply (only if test Sharpe improves by threshold)
    should_apply = bool(
        test_rows and (opt_sharpe_test - baseline_sharpe_test) >= MIN_SHARPE_IMPROVEMENT
    ) or (
        # If no test data, apply if training improvement is significant
        not test_rows and (opt_sharpe_train - baseline_sharpe_train) >= MIN_SHARPE_IMPROVEMENT
    )

    logger.info(
        "[%s/%s] Decision: %s (test Sharpe delta: %.3f, threshold: %.3f)",
        category, term,
        "APPLY" if should_apply else "KEEP CURRENT",
        opt_sharpe_test - baseline_sharpe_test if test_rows else opt_sharpe_train - baseline_sharpe_train,
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


def run_all_calibrations(backtest_rows: list[dict]) -> dict[tuple[str, str], dict]:
    """Run calibration for all (category, term) combinations.

    Returns dict: {(category, term): calibration_result}.
    """
    combinations = [
        ("stock",     "short"),
        ("stock",     "long"),
        ("etf",       "short"),
        ("etf",       "long"),
        ("commodity", "short"),
        ("commodity", "long"),
    ]

    results = {}
    for category, term in combinations:
        logger.info("=== Calibrating %s/%s ===", category, term)
        result = calibrate_weights(backtest_rows, category, term)
        results[(category, term)] = result

        # Print summary
        print(f"\n{'=' * 60}")
        print(f"  {category.upper()} / {term.upper()}")
        print(f"  Status: {result['status']}")
        print(f"  Samples: {result.get('n_samples', 0)}")
        print(f"  Sharpe before: {result.get('sharpe_before', 0):.3f}")
        print(f"  Sharpe after:  {result.get('sharpe_after', 0):.3f}")
        print(f"  DA before: {(result.get('directional_accuracy_before', 0)*100):.1f}%")
        print(f"  DA after:  {(result.get('directional_accuracy_after', 0)*100):.1f}%")
        if result["status"] == "apply":
            print(f"  ✓ NEW WEIGHTS:")
            for k, v in result["weights_after"].items():
                old = result["weights_before"].get(k, 0)
                delta = v - old
                print(f"    {k:12s}: {old:.3f} → {v:.3f}  ({delta:+.3f})")
        else:
            print(f"  → Keeping current weights (insufficient improvement)")

    return results
