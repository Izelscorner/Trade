"""Grading engine: combines technical, sentiment, and macro scores into grades.

Mathematical Model
==================
1. Technical Score — group-based weighted average to eliminate correlation bias.
   18 indicators are bucketed into 5 groups (Trend, Momentum, Volume, Levels,
   Volatility).  Each group's score is averaged independently and then combined
   via category/term-specific group weights.  ADX acts as a trend-strength
   multiplier for the Trend group; ATR % acts as a volatility risk dampener on
   the overall technical score.

2. Sentiment Score — DUAL-HORIZON, term-aware.
   Short-term grades use short_term sentiment labels (label column).
   Long-term grades use long_term sentiment labels (long_term_label column).
   Each uses exponential time-decay with horizon-appropriate half-lives:
     - Short-term: 24-hour half-life, 3-day window (captures immediate reaction)
     - Long-term: 96-hour half-life, 14-day window (captures fundamental shifts)
   Confidence uses a logarithmic ramp (not linear) so it saturates properly.

   BEHAVIORAL SCIENCE EDGE CASES:
   a) Contrarian dampening: When >80% of non-neutral articles agree on direction,
      apply 0.85x dampener — herd behavior signals increased mean-reversion risk.
   b) Priced-in detection: When consensus is extreme AND articles are >48h old on
      average, the signal is likely already priced in — apply additional 0.9x.

3. Macro Score — DUAL-HORIZON, term-aware.
   Short-term: 6-hour half-life, 12h window (immediate risk-on/off).
   Long-term: 24-hour half-life, 48h window (structural policy shifts).

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

# Composite (technical vs sentiment vs sector vs macro vs fundamentals) weight profiles
# 5-signal composite: fundamentals add valuation/profitability/growth assessment.
# Commodities get 0% fundamentals (no P/E, ROE, etc. for futures).
# Short-term: fundamentals matter less (market moves on sentiment/technicals).
# Long-term: fundamentals matter more (value investing, mean reversion to fair value).
COMPOSITE_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {
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

# ---------------------------------------------------------------------------
# Term-specific sentiment parameters
# ---------------------------------------------------------------------------
# From behavioral science: short-term sentiment reflects immediate market
# psychology (recency bias, momentum), long-term reflects fundamental anchoring.

SENTIMENT_PARAMS = {
    "short": {
        "half_life_hours": 12.0,    # 12h half-life: rapid decay for shock value
        "window_days": 2,           # 2-day lookback (0 - 48 hours)
        "full_confidence_at": 20,   # 20 non-neutral articles = full confidence
    },
    "long": {
        "half_life_hours": 168.0,   # 7-day half-life (1 week): steady institutional tone
        "window_days": 30,          # 30-day lookback
        "full_confidence_at": 40,   # 40 non-neutral articles = full confidence
    },
}

MACRO_PARAMS = {
    "short": {
        "half_life_hours": 24.0,    # 24h half-life: immediate shock value
        "window_hours": 72,         # 72h lookback (1 - 3 days)
    },
    "long": {
        "half_life_hours": 648.0,   # 27-day half-life (~10% weight after 3 months)
        "window_hours": 4320,       # 180 days lookback (~6 months)
    },
}

# Sector sentiment parameters — between asset-level and macro-level decay rates.
# Sector dynamics are slower than individual asset sentiment but faster than macro policy shifts.
SECTOR_PARAMS = {
    "short": {
        "half_life_hours": 36.0,   # Increase half-life for slightly more persistence
        "window_hours": 120,       # 5-day lookback (from 2d)
    },
    "long": {
        "half_life_hours": 240.0,  # 10-day half-life: structural industry shifts
        "window_hours": 720,       # 30 days lookback
    },
}


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _log_confidence(count: int, full_at: int = 20) -> float:
    """Logarithmic confidence ramp — better than linear for sparse data.

    log(1+n) / log(1+N) captures diminishing returns correctly:
        n=1  →  23 %,  n=5  →  54 %,  n=10  →  77 %,  n=20  →  100 %
    """
    if count <= 0:
        return 0.0
    return min(1.0, math.log(1 + count) / math.log(1 + full_at))


def _sigmoid_confidence(score: float, k: float = 1.5) -> float:
    """Map [-3, 3] composite score to (0, 100) buy-confidence via sigmoid."""
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
# Behavioral science: consensus dampening
# ---------------------------------------------------------------------------

def _consensus_adjustment(labels: list[str], avg_age_hours: float) -> float:
    """Apply behavioral science adjustments for herd behavior and priced-in signals.

    Expert Trader: Markets are contrarian at extremes — unanimous bullishness
    often precedes corrections (and vice versa).

    Expert Behavioral Scientist: Herding bias means when everyone agrees,
    the information is likely already reflected in price (EMH weak form).

    Expert Mathematician: We model this as a dampening factor that activates
    only at extreme consensus (>80% agreement), preserving signal linearity
    in the normal range.

    Returns a multiplier in [0.7, 1.0].
    """
    if not labels:
        return 1.0

    non_neutral = [l for l in labels if l != "neutral"]
    if len(non_neutral) < 3:
        return 1.0  # Too few articles to detect consensus

    positive_count = sum(1 for l in non_neutral if "positive" in l)
    negative_count = sum(1 for l in non_neutral if "negative" in l)
    dominant = max(positive_count, negative_count)
    agreement_ratio = dominant / len(non_neutral)

    multiplier = 1.0

    # Contrarian dampening: >80% agreement = herd signal
    if agreement_ratio > 0.80:
        multiplier *= 0.85
        logger.debug("Consensus dampening: %.0f%% agreement → ×0.85", agreement_ratio * 100)

    # Priced-in detection: high consensus + old average age
    if agreement_ratio > 0.75 and avg_age_hours > 48:
        multiplier *= 0.90
        logger.debug("Priced-in dampening: %.0f%% agreement + %.0fh avg age → ×0.90",
                      agreement_ratio * 100, avg_age_hours)

    return max(0.7, multiplier)


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
# Sentiment score — DUAL-HORIZON, term-aware
# ---------------------------------------------------------------------------

async def get_sentiment_score(instrument_id: str, term: str = "short") -> tuple[float, dict]:
    """Compute instrument sentiment with term-appropriate parameters.

    Short-term grades use the `label` column (short-term sentiment).
    Long-term grades use the `long_term_label` column (long-term sentiment).

    Each horizon has its own decay rate and lookback window, reflecting
    different behavioral dynamics:
    - Short-term: recency bias dominates, fast decay, narrow window
    - Long-term: anchoring bias dominates, slow decay, wide window
    """
    params = SENTIMENT_PARAMS.get(term, SENTIMENT_PARAMS["short"])
    half_life_hours = params["half_life_hours"]
    window_days = params["window_days"]
    full_at = params["full_confidence_at"]

    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    now = datetime.now(timezone.utc)
    decay_lambda = math.log(2) / half_life_hours

    # Choose the appropriate label column based on term
    label_col = "s.label" if term == "short" else "COALESCE(s.long_term_label, s.label)"

    async with async_session() as session:
        result = await session.execute(
            text(f"""
                SELECT {label_col} AS sentiment_label, a.published_at,
                       COALESCE(m.relevance_score, 1.0) AS relevance_score
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
        return 0.0, {"articles": 0, "confidence": 0.0, "term": term}

    # Count labels for display
    label_counts: dict[str, int] = {}
    all_labels: list[str] = []
    for r in rows:
        lbl = r.sentiment_label or "neutral"
        label_counts[lbl] = label_counts.get(lbl, 0) + 1
        all_labels.append(lbl)

    # Exponential time-decay weighted sentiment
    # For ETFs, relevance_score (0-1) from constituent weight proportionally
    # scales the impact: direct ETF news = 1.0, NVDA at 23.1% = 0.231
    weighted_sum = 0.0
    weight_total = 0.0
    non_neutral_weighted_count = 0.0
    total_age_hours = 0.0
    non_neutral_count = 0

    for r in rows:
        lbl = r.sentiment_label or "neutral"
        if lbl == "neutral":
            continue
        score = SENTIMENT_SCORES.get(lbl, 0.0)
        relevance = float(r.relevance_score) if r.relevance_score else 1.0
        pub = r.published_at
        if pub is not None:
            if pub.tzinfo is None:
                pub = pub.replace(tzinfo=timezone.utc)
            age_hours = max(0.0, (now - pub).total_seconds() / 3600.0)
        else:
            age_hours = half_life_hours

        decay_weight = math.exp(-decay_lambda * age_hours) * relevance
        weighted_sum += score * decay_weight
        weight_total += decay_weight
        non_neutral_weighted_count += decay_weight
        total_age_hours += age_hours
        non_neutral_count += 1

    if weight_total == 0.0:
        return 0.0, {"articles": total_articles, "labels": label_counts, "confidence": 0.0, "term": term}

    mean = weighted_sum / weight_total
    avg_age_hours = total_age_hours / non_neutral_count if non_neutral_count > 0 else 0.0

    # Effective article count for confidence
    effective_count = non_neutral_weighted_count
    confidence = _log_confidence(min(round(effective_count * 2), full_at * 2), full_at=full_at)

    # Behavioral science: consensus dampening
    consensus_mult = _consensus_adjustment(all_labels, avg_age_hours)

    effective = _clip(mean * confidence * consensus_mult)

    return round(effective, 4), {
        "articles": total_articles,
        "non_neutral": non_neutral_count,
        "labels": label_counts,
        "mean": round(mean, 4),
        "confidence": round(confidence, 4),
        "consensus_adjustment": round(consensus_mult, 3),
        "avg_age_hours": round(avg_age_hours, 1),
        "decay_half_life_h": half_life_hours,
        "term": term,
    }


# ---------------------------------------------------------------------------
# Macro score — DUAL-HORIZON, term-aware
# ---------------------------------------------------------------------------

async def get_macro_score(term: str = "short") -> tuple[float, dict]:
    """Aggregate macro sentiment with term-appropriate parameters.

    Short-term: 6h half-life, 12h window — captures immediate risk-on/off.
    Long-term: 24h half-life, 48h window — captures structural policy shifts.
    """
    params = MACRO_PARAMS.get(term, MACRO_PARAMS["short"])
    half_life_hours = params["half_life_hours"]
    window_hours = params["window_hours"]

    decay_lambda = math.log(2) / half_life_hours
    now = datetime.now(timezone.utc)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT score, article_count, calculated_at, label
                FROM macro_sentiment
                WHERE term = :term
                AND calculated_at >= NOW() - INTERVAL '1 hour' * :window_h
                ORDER BY calculated_at DESC
                LIMIT 20
            """),
            {"term": term, "window_h": window_hours},
        )
        rows = result.fetchall()

    if not rows:
        return 0.0, {"records": 0, "term": term}

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
        return 0.0, {"records": len(rows), "articles": total_articles, "term": term}

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
        "term": term,
    }


# ---------------------------------------------------------------------------
# Sector score — DUAL-HORIZON, term-aware
# ---------------------------------------------------------------------------

async def get_sector_score(sector: str | None, term: str = "short") -> tuple[float, dict]:
    """Aggregate sector sentiment with term-appropriate parameters.

    Sector sentiment captures industry-level dynamics that are more specific than
    broad macro but broader than individual asset sentiment. Uses exponential
    time-decay with horizon-appropriate half-lives.

    Returns (0.0, minimal_details) if sector is None (e.g., broad-market ETFs).
    """
    if not sector:
        return 0.0, {"sector": None, "term": term, "records": 0}

    params = SECTOR_PARAMS.get(term, SECTOR_PARAMS["short"])
    half_life_hours = params["half_life_hours"]
    window_hours = params["window_hours"]

    decay_lambda = math.log(2) / half_life_hours
    now = datetime.now(timezone.utc)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT score, article_count, calculated_at, label
                FROM sector_sentiment
                WHERE sector = :sector
                AND term = :term
                AND calculated_at >= NOW() - INTERVAL '1 hour' * :window_h
                ORDER BY calculated_at DESC
                LIMIT 20
            """),
            {"sector": sector, "term": term, "window_h": window_hours},
        )
        rows = result.fetchall()

    if not rows:
        return 0.0, {"sector": sector, "records": 0, "term": term}

    weighted_sum = 0.0
    weight_total = 0.0
    # Use latest record's article_count (not sum across records, which inflates)
    latest_article_count = rows[0].article_count or 0

    for row in rows:
        calc_at = row.calculated_at
        if calc_at.tzinfo is None:
            calc_at = calc_at.replace(tzinfo=timezone.utc)
        age_hours = max(0.0, (now - calc_at).total_seconds() / 3600.0)
        decay_weight = math.exp(-decay_lambda * age_hours)

        # Sector scores from update_sector_sentiment() are on [-1, 1] scale
        # (SENTIMENT_MULTIPLIERS). Scale to [-3, 3] to match other sub-signals.
        score = _clip(float(row.score) * 3.0)
        weighted_sum += score * decay_weight
        weight_total += decay_weight

    if weight_total == 0.0:
        return 0.0, {"sector": sector, "records": len(rows), "articles": latest_article_count, "term": term}

    sector_mean = weighted_sum / weight_total
    confidence = _log_confidence(min(latest_article_count, 20), full_at=8)
    effective = _clip(sector_mean * confidence)

    return round(effective, 4), {
        "sector": sector,
        "records": len(rows),
        "articles": latest_article_count,
        "mean": round(sector_mean, 4),
        "confidence": round(confidence, 4),
        "latest_label": rows[0].label,
        "decay_half_life_h": half_life_hours,
        "term": term,
    }


# ---------------------------------------------------------------------------
# Fundamentals score — piecewise-linear metric scoring
# ---------------------------------------------------------------------------
# Finance Expert: Institutional valuation standards.
# P/E 15-22 = fair value (S&P 500 historical median ~16-18).
# ROE 10-20% = solid profitability. >35% = exceptional (or leverage).
# D/E 0-0.3 = conservative. >3.0 = high leverage risk.
# PEG 0.5-1.0 = Peter Lynch "ideal" growth-at-reasonable-price.

def _score_pe(pe: float | None) -> float:
    """Score P/E ratio on [-3, 3]. Lower is better (to a point)."""
    if pe is None:
        return 0.0
    if pe < 0:
        return -2.5  # Negative earnings
    if pe <= 8:
        return 2.0   # Deep value
    if pe <= 15:
        return 1.5   # Attractive value
    if pe <= 22:
        return 0.5   # Fair value
    if pe <= 35:
        return -0.5  # Expensive
    if pe <= 60:
        return -1.5  # Very expensive
    return -2.5       # Extreme overvaluation


def _score_roe(roe: float | None) -> float:
    """Score ROE on [-3, 3]. Higher is better."""
    if roe is None:
        return 0.0
    if roe < 0:
        return -2.0  # Loss-making
    if roe <= 0.05:
        return -1.0  # Poor
    if roe <= 0.10:
        return 0.0   # Mediocre
    if roe <= 0.20:
        return 1.0   # Good
    if roe <= 0.35:
        return 2.0   # Very good
    return 2.5        # Exceptional


def _score_de(de: float | None) -> float:
    """Score D/E ratio on [-3, 3]. Lower is better."""
    if de is None:
        return 0.0
    if de < 0:
        return -2.0  # Negative equity
    if de <= 0.3:
        return 2.0   # Very conservative
    if de <= 0.7:
        return 1.0   # Conservative
    if de <= 1.5:
        return 0.0   # Moderate
    if de <= 3.0:
        return -1.0  # High leverage
    return -2.0       # Extreme leverage


def _score_peg(peg: float | None) -> float:
    """Score PEG ratio on [-3, 3]. 0.5-1.0 is ideal (Peter Lynch)."""
    if peg is None:
        return 0.0
    if peg < 0:
        return -1.5  # Negative growth or negative earnings
    if peg <= 0.5:
        return 2.5   # Exceptional growth-value
    if peg <= 1.0:
        return 1.5   # Ideal
    if peg <= 1.5:
        return 0.5   # Fair
    if peg <= 2.5:
        return -0.5  # Overpriced for growth
    return -1.5       # Very overpriced for growth


# Metric weights within fundamentals sub-signal
_FUND_WEIGHTS = {"pe_ratio": 0.30, "roe": 0.25, "de_ratio": 0.20, "peg_ratio": 0.25}
_FUND_SCORERS = {"pe_ratio": _score_pe, "roe": _score_roe, "de_ratio": _score_de, "peg_ratio": _score_peg}


async def get_fundamentals_score(
    instrument_id: str, category: str = "stock"
) -> tuple[float, dict]:
    """Compute fundamentals score from latest fundamental_metrics row.

    Returns (score in [-3, 3], details_dict).
    Commodities always return 0.0 (no fundamentals for futures).
    """
    if category == "commodity":
        return 0.0, {"category": "commodity", "confidence": 0.0}

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT pe_ratio, roe, de_ratio, peg_ratio, fetched_at
                FROM fundamental_metrics
                WHERE instrument_id = :iid
                ORDER BY fetched_at DESC
                LIMIT 1
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()

    if not row:
        return 0.0, {"confidence": 0.0, "has_data": False}

    # Score each metric
    metrics = {
        "pe_ratio": float(row.pe_ratio) if row.pe_ratio is not None else None,
        "roe": float(row.roe) if row.roe is not None else None,
        "de_ratio": float(row.de_ratio) if row.de_ratio is not None else None,
        "peg_ratio": float(row.peg_ratio) if row.peg_ratio is not None else None,
    }

    metric_scores = {}
    weighted_sum = 0.0
    weight_total = 0.0
    for key, scorer in _FUND_SCORERS.items():
        s = scorer(metrics[key])
        metric_scores[key] = {"value": metrics[key], "score": round(s, 2)}
        if metrics[key] is not None:
            weighted_sum += s * _FUND_WEIGHTS[key]
            weight_total += _FUND_WEIGHTS[key]

    if weight_total == 0:
        return 0.0, {"confidence": 0.0, "has_data": False}

    raw_score = weighted_sum / weight_total

    # Freshness-based confidence: 1.0 within 48h, linear decay to 0.3 at 30 days, 0 beyond
    now = datetime.now(timezone.utc)
    fetched = row.fetched_at
    if fetched.tzinfo is None:
        fetched = fetched.replace(tzinfo=timezone.utc)
    age_hours = (now - fetched).total_seconds() / 3600.0

    if age_hours <= 48:
        confidence = 1.0
    elif age_hours <= 720:  # 30 days
        confidence = 1.0 - 0.7 * ((age_hours - 48) / (720 - 48))
    else:
        confidence = 0.0

    effective = _clip(raw_score * confidence)

    return round(effective, 4), {
        "has_data": True,
        "metrics": metric_scores,
        "raw_score": round(raw_score, 4),
        "confidence": round(confidence, 4),
        "age_hours": round(age_hours, 1),
        "fetched_at": fetched.isoformat(),
    }


# ---------------------------------------------------------------------------
# Final composite grading
# ---------------------------------------------------------------------------

async def grade_instrument(
    instrument_id: str,
    symbol: str,
    term: str = "short",
    category: str = "stock",
    sector: str | None = None,
) -> dict | None:
    """Compute a mathematically rigorous grade for one instrument.

    Now term-aware: short-term grades use short-term sentiment/macro/sector,
    long-term grades use long-term sentiment/macro/sector. This prevents
    short-term noise from contaminating long-term views and vice versa.

    5-signal composite: Technical + Sentiment + Sector + Macro + Fundamentals.

    Returns a dict ready for DB insertion; also embeds buy_confidence and
    action_label in the details JSON so the frontend can display them.
    """
    lookback = 5 if term == "short" else 30

    technical_score, tech_details = await get_technical_score(
        instrument_id, lookback, category, term
    )
    sentiment_score, sent_details = await get_sentiment_score(instrument_id, term)
    sector_score, sector_details = await get_sector_score(sector, term)
    macro_score, macro_details = await get_macro_score(term)
    fundamentals_score, fund_details = await get_fundamentals_score(instrument_id, category)

    # Composite weights
    profile = COMPOSITE_WEIGHT_PROFILES.get(category, COMPOSITE_WEIGHT_PROFILES["stock"])
    weights = profile.get(term, profile["short"])

    # Confidence-weighted composite: if a sub-signal has near-zero confidence,
    # reduce its effective weight so it doesn't anchor the result at zero.
    tech_conf = tech_details.get("data_completeness", 1.0)
    sent_conf = sent_details.get("confidence", 0.0)
    sector_conf = sector_details.get("confidence", 0.0) if sector else 0.0
    macro_conf = macro_details.get("confidence", 0.0)
    fund_conf = fund_details.get("confidence", 0.0)

    effective_weights = {
        "technical":    weights["technical"]    * (0.5 + 0.5 * tech_conf),
        "sentiment":    weights["sentiment"]    * (0.5 + 0.5 * sent_conf),
        "sector":       weights["sector"]       * (0.5 + 0.5 * sector_conf),
        "macro":        weights["macro"]        * (0.5 + 0.5 * macro_conf),
        "fundamentals": weights["fundamentals"] * (0.5 + 0.5 * fund_conf),
    }
    w_sum = sum(effective_weights.values())
    if w_sum == 0:
        overall = 0.0
    else:
        overall = (
            technical_score * effective_weights["technical"]
            + sentiment_score * effective_weights["sentiment"]
            + sector_score * effective_weights["sector"]
            + macro_score * effective_weights["macro"]
            + fundamentals_score * effective_weights["fundamentals"]
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
        "sector_score": sector_score,
        "fundamentals_score": fundamentals_score,
        "details": json.dumps({
            "weights": weights,
            "effective_weights": {k: round(v / w_sum, 4) for k, v in effective_weights.items()} if w_sum else weights,
            "buy_confidence": buy_confidence,
            "action": action,
            "technical": tech_details,
            "sentiment": sent_details,
            "sector": sector_details,
            "macro": macro_details,
            "fundamentals": fund_details,
        }),
        "graded_at": now,
    }


async def store_grade(grade: dict) -> None:
    """Store a computed grade."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO grades (instrument_id, term, overall_grade, overall_score,
                    technical_score, sentiment_score, macro_score, sector_score, fundamentals_score, details, graded_at)
                VALUES (:instrument_id, :term, :overall_grade, :overall_score,
                    :technical_score, :sentiment_score, :macro_score, :sector_score, :fundamentals_score, CAST(:details AS jsonb), :graded_at)
            """),
            grade,
        )
        await session.commit()
