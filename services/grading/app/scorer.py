"""Grading engine: combines technical, sentiment, and macro scores into grades."""

import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

# Signal to numeric score mapping
SIGNAL_SCORES = {
    "strong_buy": 1.0,
    "buy": 0.5,
    "neutral": 0.0,
    "sell": -0.5,
    "strong_sell": -1.0,
}

# Sentiment label to multiplier mapping
SENTIMENT_MULTIPLIERS = {
    "very positive": 1.0,
    "positive": 0.5,
    "neutral": 0.0,
    "negative": -0.5,
    "very negative": -1.0,
}

# Score to letter grade mapping — symmetric thresholds
def score_to_grade(score: float) -> str:
    """Convert a -1.0 to 1.0 score to a letter grade.

    Symmetric distribution with wider neutral zone:
        A+ : [0.75, 1.0]   — strong bullish
        A  : [0.50, 0.75)  — bullish
        B+ : [0.25, 0.50)  — moderately bullish
        B  : [0.10, 0.25)  — slightly bullish
        C  : [-0.10, 0.10) — neutral
        D  : [-0.25, -0.10) — slightly bearish
        D- : [-0.50, -0.25) — moderately bearish
        F  : [-1.0, -0.50)  — bearish
    """
    if score >= 0.75:
        return "A+"
    elif score >= 0.50:
        return "A"
    elif score >= 0.25:
        return "B+"
    elif score >= 0.10:
        return "B"
    elif score >= -0.10:
        return "C"
    elif score >= -0.25:
        return "D"
    elif score >= -0.50:
        return "D-"
    else:
        return "F"


async def get_technical_score(instrument_id: str, lookback_days: int = 5) -> tuple[float, dict]:
    """Get aggregate technical analysis score. lookback_days controls short vs long term."""
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
        return 0.0, {}

    total = 0.0
    details = {}
    for row in rows:
        score = SIGNAL_SCORES.get(row.signal, 0.0)
        total += score
        details[row.indicator_name] = {"signal": row.signal, "score": score}

    avg = max(-1.0, min(1.0, total / len(rows)))
    return round(avg, 4), details



def _confidence(article_count: int, full_confidence_at: int = 10) -> float:
    """Confidence ramps from 0.0 to 1.0 based on article count.

    At 5 articles: 0.50, at 10+: 1.0.
    """
    return min(1.0, article_count / full_confidence_at)


async def get_sentiment_score(instrument_id: str, min_articles: int = 1) -> tuple[float, dict]:
    """Get instrument-specific sentiment score from financial news (last 7 days).

    Uses mean of per-article scores, scaled by confidence (article count).
    Mean correctly reflects the full distribution — median collapses to 0 when
    neutral articles are the majority even if clear positive/negative signal exists.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT s.label
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

        if len(rows) >= min_articles:
            label_counts = {}
            for r in rows:
                label_counts[r.label] = label_counts.get(r.label, 0) + 1

            # Exclude neutral — they carry no directional signal
            scored = [SENTIMENT_MULTIPLIERS.get(r.label, 0.0) for r in rows
                      if r.label != "neutral"]
            if not scored:
                return 0.0, {"articles": len(rows), "labels": label_counts}

            n = len(scored)
            mean = sum(scored) / n
            conf = _confidence(n)

            # Scale mean by confidence — fewer articles dampens toward neutral
            effective = mean * conf

            return round(max(-1.0, min(1.0, effective)), 4), {
                "articles": len(rows),
                "scored": n,
                "labels": label_counts,
                "mean": round(mean, 4),
                "confidence": round(conf, 4),
            }

    return 0.0, {}


async def get_macro_score(min_articles: int = 1) -> tuple[float, dict]:
    """Get latest macro sentiment score (from politics + finance news).

    Returns (0.0, {}) if fewer than min_articles were used to compute the
    macro sentiment — not enough data for a reliable signal.
    """
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT region, score, label, article_count, calculated_at
                FROM macro_sentiment
                WHERE calculated_at >= NOW() - INTERVAL '12 hours'
                ORDER BY calculated_at DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()

    if not row:
        return 0.0, {}

    if row.article_count < min_articles:
        return 0.0, {"global": {"articles": row.article_count, "below_minimum": True}}

    median = max(-1.0, min(1.0, float(row.score)))
    conf = _confidence(row.article_count)
    effective = median * conf
    details = {
        "global": {
            "score": round(effective, 4),
            "median": round(median, 4),
            "confidence": round(conf, 4),
            "label": row.label,
            "articles": row.article_count,
        }
    }

    return round(effective, 4), details


WEIGHT_PROFILES = {
    "stock": {
        "short": {"technical": 0.30, "sentiment": 0.45, "macro": 0.25},
        "long":  {"technical": 0.35, "sentiment": 0.30, "macro": 0.35},
    },
    "etf": {
        "short": {"technical": 0.30, "sentiment": 0.40, "macro": 0.30},
        "long":  {"technical": 0.30, "sentiment": 0.25, "macro": 0.45},
    },
    "commodity": {
        "short": {"technical": 0.30, "sentiment": 0.45, "macro": 0.25},
        "long":  {"technical": 0.30, "sentiment": 0.30, "macro": 0.40},
    },
}


async def grade_instrument(
    instrument_id: str, symbol: str, term: str = "short", category: str = "stock",
) -> dict | None:
    """Compute a full grade for an instrument.

    term: 'short' uses recent 5-day TA, 'long' uses 30-day TA.
    category: 'stock', 'etf', or 'commodity' — affects weight profile.
    """
    lookback = 5 if term == "short" else 30

    technical_score, tech_details = await get_technical_score(instrument_id, lookback)
    sentiment_score, sent_details = await get_sentiment_score(instrument_id)
    macro_score, macro_details = await get_macro_score()

    # Use category-specific weights for optimal signal weighting
    profile = WEIGHT_PROFILES.get(category, WEIGHT_PROFILES["stock"])
    weights = profile.get(term, profile["short"])

    overall = (
        technical_score * weights["technical"]
        + sentiment_score * weights["sentiment"]
        + macro_score * weights["macro"]
    )
    overall = round(max(-1.0, min(1.0, overall)), 4)
    grade = score_to_grade(overall)

    now = datetime.now(timezone.utc)

    return {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "term": term,
        "overall_grade": grade,
        "overall_score": overall,
        "technical_score": technical_score,
        "sentiment_score": sentiment_score,
        "macro_score": macro_score,
        "details": json.dumps({
            "weights": weights,
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
