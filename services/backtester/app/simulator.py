"""Grade simulator: reconstruct the composite grade for a given (instrument, date).

Mirrors the logic in grading/app/scorer.py but takes pre-computed sub-scores
as explicit inputs rather than querying live DB tables.

Key design: all sub-scores are passed in; this function only does the weighting math.
This makes calibration easy — we can call this with different weight vectors.
"""

import math
from typing import TypedDict

# Composite weight profiles (same as scorer.py COMPOSITE_WEIGHT_PROFILES)
COMPOSITE_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {"technical": 0.10, "sentiment": 0.23, "sector": 0.11, "macro": 0.16, "fundamentals": 0.40},
        "long":  {"technical": 0.24, "sentiment": 0.20, "sector": 0.12, "macro": 0.24, "fundamentals": 0.20},
    },
    "etf": {
        "short": {"technical": 0.38, "sentiment": 0.18, "sector": 0.14, "macro": 0.23, "fundamentals": 0.07},
        "long":  {"technical": 0.20, "sentiment": 0.17, "sector": 0.15, "macro": 0.33, "fundamentals": 0.15},
    },
    "commodity": {
        "short": {"technical": 0.42, "sentiment": 0.25, "sector": 0.10, "macro": 0.23, "fundamentals": 0.0},
        "long":  {"technical": 0.28, "sentiment": 0.25, "sector": 0.12, "macro": 0.35, "fundamentals": 0.0},
    },
}

WEIGHT_KEYS = ["technical", "sentiment", "sector", "macro", "fundamentals"]


class SubScores(TypedDict):
    technical: float
    technical_conf: float
    sentiment: float
    sentiment_conf: float
    sector: float
    sector_conf: float
    macro: float
    macro_conf: float
    fundamentals: float
    fundamentals_conf: float


def _clip(v: float, lo=-3.0, hi=3.0) -> float:
    return max(lo, min(hi, v))


def _sigmoid(score: float, k: float = 1.5) -> float:
    return 100.0 / (1.0 + math.exp(-k * score))


def simulate_grade(
    sub_scores: SubScores,
    category: str = "stock",
    term: str = "short",
    weight_override: dict[str, float] | None = None,
) -> dict:
    """Compute composite grade from pre-computed sub-scores.

    Args:
        sub_scores: Dict with {signal}_score and {signal}_conf keys.
        category: 'stock', 'etf', or 'commodity'.
        term: 'short' or 'long'.
        weight_override: Optional dict overriding nominal weights for calibration.

    Returns dict with overall_score, buy_confidence, and effective_weights.
    """
    if weight_override is not None:
        weights = weight_override
    else:
        cat_key = category.lower()
        profile = COMPOSITE_WEIGHT_PROFILES.get(cat_key, COMPOSITE_WEIGHT_PROFILES["stock"])
        weights = profile.get(term, profile["short"])

    # Confidence-adjusted effective weights (same formula as scorer.py grade_instrument)
    effective_weights = {
        "technical":    weights.get("technical", 0)    * (0.1 + 0.9 * sub_scores["technical_conf"]),
        "sentiment":    weights.get("sentiment", 0)    * (0.1 + 0.9 * sub_scores["sentiment_conf"]),
        "sector":       weights.get("sector", 0)       * (0.1 + 0.9 * sub_scores["sector_conf"]),
        "macro":        weights.get("macro", 0)        * (0.1 + 0.9 * sub_scores["macro_conf"]),
        "fundamentals": weights.get("fundamentals", 0) * (0.1 + 0.9 * sub_scores["fundamentals_conf"]),
    }

    w_sum = sum(effective_weights.values())
    if w_sum == 0:
        return {"overall_score": 0.0, "buy_confidence": 50.0, "effective_weights": effective_weights}

    overall = (
        sub_scores["technical"]    * effective_weights["technical"]
        + sub_scores["sentiment"]  * effective_weights["sentiment"]
        + sub_scores["sector"]     * effective_weights["sector"]
        + sub_scores["macro"]      * effective_weights["macro"]
        + sub_scores["fundamentals"] * effective_weights["fundamentals"]
    ) / w_sum

    overall = round(_clip(overall), 4)
    buy_confidence = round(_sigmoid(overall), 1)

    return {
        "overall_score": overall,
        "buy_confidence": buy_confidence,
        "effective_weights": {k: round(v / w_sum, 4) for k, v in effective_weights.items()},
    }


def compute_composite_with_weights(
    rows: list[dict],
    weights: dict[str, float],
    term: str,
) -> list[float]:
    """Compute overall_score for a list of backtest rows using given weights.

    Used by the calibrator's objective function.
    Each row is a dict with keys: technical, sentiment, sector, macro, fundamentals,
    {signal}_conf keys, category.
    """
    scores = []
    for row in rows:
        sub: SubScores = {
            "technical":        row["technical"],
            "technical_conf":   row["technical_conf"],
            "sentiment":        row["sentiment"],
            "sentiment_conf":   row["sentiment_conf"],
            "sector":           row.get("sector", 0.0),
            "sector_conf":      row.get("sector_conf", 0.0),
            "macro":            row["macro"],
            "macro_conf":       row["macro_conf"],
            "fundamentals":     row["fundamentals"],
            "fundamentals_conf": row.get("fundamentals_conf", 0.0),
        }
        result = simulate_grade(sub, row.get("category", "stock"), term, weight_override=weights)
        scores.append(result["overall_score"])
    return scores
