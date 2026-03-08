"""Grading engine: combines technical, sentiment, and macro scores into grades.

Mathematical Model
==================
1. Technical Score — group-based weighted average to eliminate correlation bias.
   18 indicators are bucketed into 5 groups (Trend, Momentum, Volume, Levels,
   Volatility).  Each group's score is averaged independently and then combined
   via category/term-specific group weights.  ADX acts as a trend-strength
   multiplier for the Trend group; ATR % acts as a volatility risk dampener on
   the overall technical score.

2. Sentiment Score — exponential time-decay weighted mean.
   Recent articles carry exponentially more weight (48-hour half-life).
   Confidence uses a logarithmic ramp (not linear) so it saturates properly:
   log(1+n) / log(1+N_full).

3. Macro Score — time-decayed aggregate of up to 20 records from the last 12 h,
   rather than a single point-in-time snapshot.

4. Final Buy Confidence — sigmoid transformation of the weighted composite score.
   score ∈ [-3, 3] → sigmoid(k·score) × 100 ∈ (0, 100).
   k = 1.5 gives sensible spread:
     score  0.0  →  50 %   (neutral)
     score  1.5  →  82 %   (buy zone)
     score  3.0  →  95 %   (strong buy)
     score −1.5  →  18 %   (sell zone)
     score −3.0  →   5 %   (strong sell)
"""

import json
import logging
import math
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Discrete signal → numeric score
# ---------------------------------------------------------------------------

SIGNAL_SCORES = {
    "strong_buy":  3.0,
    "buy":         1.5,
    "neutral":     0.0,
    "sell":       -1.5,
    "strong_sell": -3.0,
}

# Sentiment label → directional score
SENTIMENT_SCORES = {
    "very positive":  3.0,
    "positive":       1.5,
    "neutral":        0.0,
    "negative":      -1.5,
    "very negative": -3.0,
}

# ---------------------------------------------------------------------------
# Indicator group definitions
# ---------------------------------------------------------------------------
# ADX and ATR are NOT in any averaging group — they are used as modifiers.

INDICATOR_GROUPS = {
    "trend": ["SMA_50", "SMA_200", "EMA_20", "EMA_CROSS", "MACD", "ICHIMOKU"],
    "momentum": ["RSI", "STOCHASTIC", "WILLIAMS_R", "CCI"],
    "volume": ["OBV", "VWAP", "MFI"],
    "levels": ["SUPPORT_RESISTANCE", "FIBONACCI"],
    "volatility": ["BOLLINGER"],
}

# Build reverse lookup: indicator_name → group
INDICATOR_TO_GROUP: dict[str, str] = {}
for _grp, _inds in INDICATOR_GROUPS.items():
    for _ind in _inds:
        INDICATOR_TO_GROUP[_ind] = _grp

# Total indicator slots (for data-completeness confidence)
TOTAL_INDICATOR_SLOTS = sum(len(v) for v in INDICATOR_GROUPS.values()) + 2  # +2 for ADX, ATR

# Group weight profiles per asset category and term
# Weights across the 5 groups must sum to 1.0
GROUP_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {"trend": 0.28, "momentum": 0.30, "volume": 0.20, "levels": 0.16, "volatility": 0.06},
        "long":  {"trend": 0.38, "momentum": 0.20, "volume": 0.18, "levels": 0.18, "volatility": 0.06},
    },
    "etf": {
        "short": {"trend": 0.30, "momentum": 0.25, "volume": 0.22, "levels": 0.17, "volatility": 0.06},
        "long":  {"trend": 0.42, "momentum": 0.18, "volume": 0.18, "levels": 0.16, "volatility": 0.06},
    },
    "commodity": {
        "short": {"trend": 0.25, "momentum": 0.28, "volume": 0.20, "levels": 0.20, "volatility": 0.07},
        "long":  {"trend": 0.35, "momentum": 0.20, "volume": 0.18, "levels": 0.20, "volatility": 0.07},
    },
}

# Composite (technical vs sentiment vs macro) weight profiles
COMPOSITE_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
    "stock": {
        "short": {"technical": 0.50, "sentiment": 0.30, "macro": 0.20},
        "long":  {"technical": 0.35, "sentiment": 0.30, "macro": 0.35},
    },
    "etf": {
        "short": {"technical": 0.45, "sentiment": 0.25, "macro": 0.30},
        "long":  {"technical": 0.30, "sentiment": 0.25, "macro": 0.45},
    },
    "commodity": {
        "short": {"technical": 0.45, "sentiment": 0.30, "macro": 0.25},
        "long":  {"technical": 0.30, "sentiment": 0.30, "macro": 0.40},
    },
}


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _log_confidence(count: int, full_at: int = 10) -> float:
    """Logarithmic confidence ramp — better than linear for sparse data.

    log(1+n) / log(1+N) captures diminishing returns correctly:
        n=1  →  30 %,  n=3  →  54 %,  n=5  →  68 %,  n=10  →  100 %
    """
    if count <= 0:
        return 0.0
    return min(1.0, math.log(1 + count) / math.log(1 + full_at))


def _sigmoid_confidence(score: float, k: float = 1.5) -> float:
    """Map [-3, 3] composite score to (0, 100) buy-confidence via sigmoid.

    The sigmoid ensures:
    - Exactly 50 % at score = 0 (true neutral).
    - Asymptotically approaches 0 % / 100 % at extremes.
    - k = 1.5 keeps reasonable spread across the realistic [-2, 2] range.
    """
    return round(100.0 / (1.0 + math.exp(-k * score)), 1)


def _action_label(confidence: float) -> str:
    """Map buy-confidence percentage to an actionable recommendation."""
    if confidence >= 78:
        return "Strong Buy"
    elif confidence >= 63:
        return "Buy"
    elif confidence >= 54:
        return "Slight Buy"
    elif confidence >= 46:
        return "Neutral"
    elif confidence >= 37:
        return "Slight Sell"
    elif confidence >= 22:
        return "Sell"
    else:
        return "Strong Sell"


def _clip(value: float, lo: float = -3.0, hi: float = 3.0) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Technical score — group-based weighted average
# ---------------------------------------------------------------------------

async def get_technical_score(
    instrument_id: str,
    lookback_days: int = 5,
    category: str = "stock",
    term: str = "short",
) -> tuple[float, dict]:
    """Compute a group-weighted technical score with ADX and ATR modifiers.

    Returns (score ∈ [-3, 3], details_dict).
    """
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=lookback_days)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (indicator_name) indicator_name, signal, value
                FROM technical_indicators
                WHERE instrument_id = :iid AND date >= :cutoff
                ORDER BY indicator_name, date DESC
            """),
            {"iid": instrument_id, "cutoff": cutoff},
        )
        rows = result.fetchall()

    if not rows:
        return 0.0, {"data_completeness": 0.0}

    # Map indicator → signal score
    raw: dict[str, dict] = {}
    for row in rows:
        raw[row.indicator_name] = {
            "signal": row.signal,
            "score": SIGNAL_SCORES.get(row.signal, 0.0),
            "value": row.value if isinstance(row.value, dict) else {},
        }

    # Extract special modifiers (ADX, ATR)
    adx_multiplier = 1.0
    atr_risk_factor = 1.0

    if "ADX" in raw:
        adx_sig = raw["ADX"]["signal"]
        adx_val = raw["ADX"].get("value", {})
        adx_value = adx_val.get("adx", 0) if isinstance(adx_val, dict) else 0
        if adx_sig == "neutral" or adx_value < 20:
            adx_multiplier = 0.70  # Weak trend — dampen trend group
        elif adx_sig in ("strong_buy", "strong_sell") or adx_value > 40:
            adx_multiplier = 1.25  # Strong trend confirmed — amplify

    if "ATR" in raw:
        atr_val = raw["ATR"].get("value", {})
        atr_pct = atr_val.get("atr_percent", 2.0) if isinstance(atr_val, dict) else 2.0
        if atr_pct > 5.0:
            atr_risk_factor = 0.65   # Extreme volatility → high uncertainty
        elif atr_pct > 3.5:
            atr_risk_factor = 0.80   # High volatility → reduced confidence
        elif atr_pct > 2.5:
            atr_risk_factor = 0.92   # Elevated volatility

    # Compute per-group scores
    group_profile = GROUP_WEIGHT_PROFILES.get(category, GROUP_WEIGHT_PROFILES["stock"])
    group_weights = group_profile.get(term, group_profile["short"])

    group_scores: dict[str, dict] = {}
    for group_name, indicators in INDICATOR_GROUPS.items():
        present = [raw[ind] for ind in indicators if ind in raw]
        if not present:
            group_scores[group_name] = {"score": 0.0, "count": 0, "indicators": {}}
            continue
        group_avg = sum(p["score"] for p in present) / len(present)
        # Apply ADX modifier only to the trend group
        if group_name == "trend":
            group_avg *= adx_multiplier
        group_scores[group_name] = {
            "score": round(_clip(group_avg), 4),
            "count": len(present),
            "indicators": {ind: raw[ind]["signal"] for ind in indicators if ind in raw},
        }

    # Weighted combination of group scores
    total_weight = 0.0
    weighted_sum = 0.0
    for group_name, gdata in group_scores.items():
        if gdata["count"] == 0:
            continue
        w = group_weights.get(group_name, 0.0)
        # Re-normalise weight by group data completeness
        completeness = gdata["count"] / len(INDICATOR_GROUPS[group_name])
        effective_w = w * completeness
        weighted_sum += gdata["score"] * effective_w
        total_weight += effective_w

    raw_tech = weighted_sum / total_weight if total_weight > 0 else 0.0

    # ATR risk dampening on final technical score
    final_tech = _clip(raw_tech * atr_risk_factor)

    # Data completeness: how many of the 18 regular indicators are present
    group_ind_count = sum(1 for n in raw if n not in ("ADX", "ATR"))
    total_group_slots = sum(len(v) for v in INDICATOR_GROUPS.values())
    data_completeness = group_ind_count / total_group_slots if total_group_slots else 0.0

    return round(final_tech, 4), {
        "group_scores": group_scores,
        "adx_multiplier": round(adx_multiplier, 2),
        "atr_risk_factor": round(atr_risk_factor, 2),
        "data_completeness": round(data_completeness, 3),
        "raw_tech_score": round(raw_tech, 4),
        "adx": raw.get("ADX", {}).get("signal"),
        "atr_pct": raw.get("ATR", {}).get("value", {}).get("atr_percent") if isinstance(raw.get("ATR", {}).get("value"), dict) else None,
    }


# ---------------------------------------------------------------------------
# Sentiment score — exponential time-decay weighted mean
# ---------------------------------------------------------------------------

async def get_sentiment_score(instrument_id: str) -> tuple[float, dict]:
    """Compute instrument sentiment with exponential time-decay weighting.

    Half-life of 48 hours: an article from 2 days ago has 50 % the weight
    of a fresh article.  Confidence uses a log ramp over non-neutral article
    count (full confidence at 10 non-neutral articles).
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    now = datetime.now(timezone.utc)
    half_life_hours = 48.0
    decay_lambda = math.log(2) / half_life_hours  # = ln(2) / T½

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT s.label, a.published_at
                FROM sentiment_scores s
                JOIN news_instrument_map m ON m.article_id = s.article_id
                JOIN news_articles a ON a.id = m.article_id
                WHERE m.instrument_id = :iid
                AND a.ollama_processed = true
                AND a.published_at >= :cutoff
                ORDER BY a.published_at DESC
            """),
            {"iid": instrument_id, "cutoff": cutoff},
        )
        rows = result.fetchall()

    total_articles = len(rows)
    if total_articles == 0:
        return 0.0, {"articles": 0, "confidence": 0.0}

    # Count labels for display
    label_counts: dict[str, int] = {}
    for r in rows:
        label_counts[r.label] = label_counts.get(r.label, 0) + 1

    # Exponential time-decay weighted sentiment
    weighted_sum = 0.0
    weight_total = 0.0
    non_neutral_weighted_count = 0.0  # effective count for confidence

    for r in rows:
        if r.label == "neutral":
            continue
        score = SENTIMENT_SCORES.get(r.label, 0.0)
        pub = r.published_at
        if pub is not None:
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (now - pub).total_seconds() / 3600.0)
        else:
            age_hours = half_life_hours  # default to 1 half-life if unknown

        decay_weight = math.exp(-decay_lambda * age_hours)
        weighted_sum += score * decay_weight
        weight_total += decay_weight
        non_neutral_weighted_count += decay_weight

    if weight_total == 0.0:
        return 0.0, {"articles": total_articles, "labels": label_counts, "confidence": 0.0}

    mean = weighted_sum / weight_total

    # Effective article count: normalise weighted count by max single-article weight
    effective_count = non_neutral_weighted_count  # sum of decay weights
    # For confidence: treat each weight as fraction of a fresh article
    # 10 fresh articles = full confidence
    confidence = _log_confidence(min(round(effective_count * 2), 20), full_at=10)

    effective = _clip(mean * confidence)

    return round(effective, 4), {
        "articles": total_articles,
        "non_neutral": sum(1 for r in rows if r.label != "neutral"),
        "labels": label_counts,
        "mean": round(mean, 4),
        "confidence": round(confidence, 4),
        "decay_half_life_h": half_life_hours,
    }


# ---------------------------------------------------------------------------
# Macro score — time-decayed aggregate of recent records
# ---------------------------------------------------------------------------

async def get_macro_score() -> tuple[float, dict]:
    """Aggregate macro sentiment from up to 20 records in the past 12 h.

    Uses exponential time-decay (6-hour half-life) so the latest reading
    dominates but historical context modulates the signal.  Confidence is
    log-scaled on effective article coverage.
    """
    half_life_hours = 6.0
    decay_lambda = math.log(2) / half_life_hours
    now = datetime.now(timezone.utc)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT score, article_count, calculated_at, label
                FROM macro_sentiment
                WHERE calculated_at >= NOW() - INTERVAL '12 hours'
                ORDER BY calculated_at DESC
                LIMIT 20
            """)
        )
        rows = result.fetchall()

    if not rows:
        return 0.0, {"records": 0}

    weighted_sum = 0.0
    weight_total = 0.0
    total_articles = 0

    for row in rows:
        calc_at = row.calculated_at
        if calc_at.tzinfo is None:
            calc_at = calc_at.replace(tzinfo=timezone.utc)
        age_hours = max(0.0, (now - calc_at).total_seconds() / 3600.0)
        decay_weight = math.exp(-decay_lambda * age_hours)

        score = _clip(float(row.score))
        weighted_sum += score * decay_weight
        weight_total += decay_weight
        total_articles += row.article_count or 0

    if weight_total == 0.0:
        return 0.0, {"records": len(rows), "articles": total_articles}

    macro_mean = weighted_sum / weight_total
    confidence = _log_confidence(min(total_articles, 30), full_at=10)
    effective = _clip(macro_mean * confidence)

    return round(effective, 4), {
        "records": len(rows),
        "articles": total_articles,
        "mean": round(macro_mean, 4),
        "confidence": round(confidence, 4),
        "latest_label": rows[0].label,
        "decay_half_life_h": half_life_hours,
    }


# ---------------------------------------------------------------------------
# Final composite grading
# ---------------------------------------------------------------------------

async def grade_instrument(
    instrument_id: str,
    symbol: str,
    term: str = "short",
    category: str = "stock",
) -> dict | None:
    """Compute a mathematically rigorous grade for one instrument.

    Returns a dict ready for DB insertion; also embeds buy_confidence and
    action_label in the details JSON so the frontend can display them.
    """
    lookback = 5 if term == "short" else 30

    technical_score, tech_details = await get_technical_score(
        instrument_id, lookback, category, term
    )
    sentiment_score, sent_details = await get_sentiment_score(instrument_id)
    macro_score, macro_details = await get_macro_score()

    # Composite weights
    profile = COMPOSITE_WEIGHT_PROFILES.get(category, COMPOSITE_WEIGHT_PROFILES["stock"])
    weights = profile.get(term, profile["short"])

    # Confidence-weighted composite: if a sub-signal has near-zero confidence,
    # reduce its effective weight so it doesn't anchor the result at zero.
    tech_conf = tech_details.get("data_completeness", 1.0)
    sent_conf = sent_details.get("confidence", 0.0)
    macro_conf = macro_details.get("confidence", 0.0)

    effective_weights = {
        "technical": weights["technical"] * (0.5 + 0.5 * tech_conf),
        "sentiment": weights["sentiment"] * (0.5 + 0.5 * sent_conf),
        "macro":     weights["macro"]     * (0.5 + 0.5 * macro_conf),
    }
    w_sum = sum(effective_weights.values())
    if w_sum == 0:
        overall = 0.0
    else:
        overall = (
            technical_score * effective_weights["technical"]
            + sentiment_score * effective_weights["sentiment"]
            + macro_score * effective_weights["macro"]
        ) / w_sum

    overall = round(_clip(overall), 4)

    # Buy confidence via sigmoid and actionable label
    buy_confidence = _sigmoid_confidence(overall)
    action = _action_label(buy_confidence)

    now = datetime.now(timezone.utc)

    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "term": term,
        "overall_grade": action,           # human-readable action label
        "overall_score": overall,
        "technical_score": technical_score,
        "sentiment_score": sentiment_score,
        "macro_score": macro_score,
        "details": json.dumps({
            "weights": weights,
            "effective_weights": {k: round(v / w_sum, 4) for k, v in effective_weights.items()} if w_sum else weights,
            "buy_confidence": buy_confidence,
            "action": action,
            "technical": tech_details,
            "sentiment": sent_details,
            "macro": macro_details,
        }),
        "graded_at": now,
    }


async def store_grade(grade: dict) -> None:
    """Store a computed grade."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO grades (instrument_id, term, overall_grade, overall_score,
                    technical_score, sentiment_score, macro_score, details, graded_at)
                VALUES (:instrument_id, :term, :overall_grade, :overall_score,
                    :technical_score, :sentiment_score, :macro_score, CAST(:details AS jsonb), :graded_at)
            """),
            grade,
        )
        await session.commit()
