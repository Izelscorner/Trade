"""Grade simulator: reconstruct the composite grade for a given (instrument, date).

Mirrors the logic in grading/app/scorer.py but takes pre-computed sub-scores
as explicit inputs rather than querying live DB tables.

Key design: all sub-scores are passed in; this function only does the weighting math.
This makes calibration easy — we can call this with different weight vectors.

Dual-mode operation:
  - "production": 5-signal composite matching scorer.py exactly (default)
  - "experimental": 8-signal composite with Tier 1 signals (VIX, momentum, earnings)

Extended with Tier 1 signals: VIX regime, price momentum, earnings avoidance.
"""

import math
from typing import Literal, TypedDict

# ---------------------------------------------------------------------------
# Production weight profiles — exact mirror of scorer.py COMPOSITE_WEIGHT_PROFILES
# These are the 5-signal weights deployed in the live grading service.
# ---------------------------------------------------------------------------
PRODUCTION_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {"technical": 0.05, "sentiment": 0.25, "sector": 0.30, "macro": 0.05, "fundamentals": 0.35},
        "long":  {"technical": 0.03, "sentiment": 0.15, "sector": 0.40, "macro": 0.02, "fundamentals": 0.40},
    },
    "etf": {
        "short": {"technical": 0.05, "sentiment": 0.20, "sector": 0.35, "macro": 0.05, "fundamentals": 0.35},
        "long":  {"technical": 0.02, "sentiment": 0.10, "sector": 0.45, "macro": 0.03, "fundamentals": 0.40},
    },
    "commodity": {
        "short": {"technical": 0.10, "sentiment": 0.30, "sector": 0.35, "macro": 0.25, "fundamentals": 0.0},
        "long":  {"technical": 0.05, "sentiment": 0.25, "sector": 0.40, "macro": 0.30, "fundamentals": 0.0},
    },
}

PRODUCTION_WEIGHT_KEYS = ["technical", "sentiment", "sector", "macro", "fundamentals"]

# ---------------------------------------------------------------------------
# Experimental weight profiles — 8 signals (5 original + 3 Tier 1).
# Weights derived from deep IC analysis on 165K backtest observations (2020-2026).
# VIX, momentum, earnings are new Tier 1 signals added for regime filtering,
# trend-following, and earnings noise avoidance.
# ---------------------------------------------------------------------------
EXPERIMENTAL_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {
            "technical": 0.04, "sentiment": 0.20, "sector": 0.24, "macro": 0.04,
            "fundamentals": 0.28, "vix": 0.06, "momentum": 0.10, "earnings": 0.04,
        },
        "long": {
            "technical": 0.03, "sentiment": 0.12, "sector": 0.32, "macro": 0.02,
            "fundamentals": 0.32, "vix": 0.04, "momentum": 0.12, "earnings": 0.03,
        },
    },
    "etf": {
        "short": {
            "technical": 0.04, "sentiment": 0.16, "sector": 0.28, "macro": 0.04,
            "fundamentals": 0.28, "vix": 0.06, "momentum": 0.10, "earnings": 0.04,
        },
        "long": {
            "technical": 0.02, "sentiment": 0.08, "sector": 0.36, "macro": 0.03,
            "fundamentals": 0.32, "vix": 0.04, "momentum": 0.12, "earnings": 0.03,
        },
    },
    "commodity": {
        "short": {
            "technical": 0.08, "sentiment": 0.24, "sector": 0.28, "macro": 0.20,
            "fundamentals": 0.0, "vix": 0.08, "momentum": 0.12, "earnings": 0.0,
        },
        "long": {
            "technical": 0.04, "sentiment": 0.20, "sector": 0.32, "macro": 0.24,
            "fundamentals": 0.0, "vix": 0.06, "momentum": 0.14, "earnings": 0.0,
        },
    },
}

EXPERIMENTAL_WEIGHT_KEYS = [
    "technical", "sentiment", "sector", "macro", "fundamentals",
    "vix", "momentum", "earnings",
]

# Active profiles — set by mode. Default to production for valid backtesting.
COMPOSITE_WEIGHT_PROFILES = PRODUCTION_WEIGHT_PROFILES
WEIGHT_KEYS = PRODUCTION_WEIGHT_KEYS

# Original hand-tuned weight profiles (pre-IC-optimization, 5 signals only).
# Preserved for re-testing after sentiment data is enriched via deepfill.
ORIGINAL_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {"technical": 0.43, "sentiment": 0.23, "sector": 0.11, "macro": 0.16, "fundamentals": 0.07},
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

# IC-optimized weights (5 signals only, pre-Tier-1).
# Kept for A/B comparison after Tier 1 signals are calibrated.
IC_OPTIMIZED_5SIGNAL_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {"technical": 0.05, "sentiment": 0.25, "sector": 0.30, "macro": 0.05, "fundamentals": 0.35},
        "long":  {"technical": 0.03, "sentiment": 0.15, "sector": 0.40, "macro": 0.02, "fundamentals": 0.40},
    },
    "etf": {
        "short": {"technical": 0.05, "sentiment": 0.20, "sector": 0.35, "macro": 0.05, "fundamentals": 0.35},
        "long":  {"technical": 0.02, "sentiment": 0.10, "sector": 0.45, "macro": 0.03, "fundamentals": 0.40},
    },
    "commodity": {
        "short": {"technical": 0.10, "sentiment": 0.30, "sector": 0.35, "macro": 0.25, "fundamentals": 0.0},
        "long":  {"technical": 0.05, "sentiment": 0.25, "sector": 0.40, "macro": 0.30, "fundamentals": 0.0},
    },
}


SimMode = Literal["production", "experimental"]


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
    vix: float
    vix_conf: float
    momentum: float
    momentum_conf: float
    earnings: float
    earnings_conf: float


def _clip(v: float, lo=-3.0, hi=3.0) -> float:
    return max(lo, min(hi, v))


def _sigmoid(score: float, k: float = 1.5) -> float:
    """Production scorer.py rounds to 1 decimal place."""
    return round(100.0 / (1.0 + math.exp(-k * score)), 1)


def _get_profiles_and_keys(mode: SimMode) -> tuple[dict, list[str]]:
    """Return the weight profiles and signal keys for the given mode."""
    if mode == "experimental":
        return EXPERIMENTAL_WEIGHT_PROFILES, EXPERIMENTAL_WEIGHT_KEYS
    return PRODUCTION_WEIGHT_PROFILES, PRODUCTION_WEIGHT_KEYS


def simulate_grade(
    sub_scores: SubScores,
    category: str = "stock",
    term: str = "short",
    weight_override: dict[str, float] | None = None,
    mode: SimMode = "production",
) -> dict:
    """Compute composite grade from pre-computed sub-scores.

    Args:
        sub_scores: Dict with {signal}_score and {signal}_conf keys.
        category: 'stock', 'etf', or 'commodity'.
        term: 'short' or 'long'.
        weight_override: Optional dict overriding nominal weights for calibration.
        mode: 'production' (5-signal, matches scorer.py) or 'experimental' (8-signal).

    Returns dict with overall_score, buy_confidence, and effective_weights.
    """
    profiles, weight_keys = _get_profiles_and_keys(mode)

    if weight_override is not None:
        weights = weight_override
        # Infer keys from the override dict
        weight_keys = [k for k in weight_keys if k in weights] or list(weights.keys())
    else:
        cat_key = category.lower()
        profile = profiles.get(cat_key, profiles["stock"])
        weights = profile.get(term, profile["short"])

    # Confidence-adjusted effective weights (same formula as scorer.py grade_instrument)
    effective_weights = {}
    for key in weight_keys:
        nominal = weights.get(key, 0)
        conf = sub_scores.get(f"{key}_conf", 0.0)
        effective_weights[key] = nominal * (0.1 + 0.9 * conf)

    w_sum = sum(effective_weights.values())
    if w_sum == 0:
        return {"overall_score": 0.0, "buy_confidence": 50.0, "effective_weights": effective_weights}

    overall = sum(
        sub_scores.get(key, 0.0) * effective_weights[key]
        for key in weight_keys
    ) / w_sum

    # Confidence-blended score dampening (present in both production scorer.py
    # and experimental mode):
    # Low-confidence instruments are dampened toward neutral.
    avg_conf = (
        sub_scores.get("sentiment_conf", 0.0)
        + sub_scores.get("sector_conf", 0.0)
        + sub_scores.get("fundamentals_conf", 0.0)
    ) / 3.0
    overall = overall * (0.7 + 0.3 * avg_conf)

    # --- Experimental-only dampening layers ---
    # These do NOT exist in production scorer.py and must be gated by mode.
    if mode == "experimental":
        # VIX regime dampening: when VIX confidence is low (extreme regime),
        # reduce overall score conviction
        vix_conf = sub_scores.get("vix_conf", 1.0)
        if vix_conf < 0.9:
            overall = overall * (0.8 + 0.2 * vix_conf)

        # Earnings proximity dampening: when near earnings, reduce conviction
        earnings_conf = sub_scores.get("earnings_conf", 1.0)
        if earnings_conf < 0.5:
            overall = overall * (0.3 + 0.7 * earnings_conf)

    overall = _clip(overall)
    buy_confidence = _sigmoid(overall)

    return {
        "overall_score": overall,
        "buy_confidence": buy_confidence,
        "effective_weights": {k: v / w_sum for k, v in effective_weights.items()},
    }


def compute_composite_with_weights(
    rows: list[dict],
    weights: dict[str, float],
    term: str,
    mode: SimMode = "production",
) -> list[float]:
    """Compute overall_score for a list of backtest rows using given weights.

    Used by the calibrator's objective function.
    Each row is a dict with keys: technical, sentiment, sector, macro, fundamentals,
    vix, momentum, earnings, {signal}_conf keys, category.
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
            "vix":              row.get("vix", 0.0),
            "vix_conf":         row.get("vix_conf", 0.0),
            "momentum":         row.get("momentum", 0.0),
            "momentum_conf":    row.get("momentum_conf", 0.0),
            "earnings":         row.get("earnings", 0.0),
            "earnings_conf":    row.get("earnings_conf", 1.0),
        }
        result = simulate_grade(sub, row.get("category", "stock"), term, weight_override=weights, mode=mode)
        scores.append(result["overall_score"])
    return scores
