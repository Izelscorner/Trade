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

# Score to letter grade mapping
def score_to_grade(score: float) -> str:
    """Convert a -1.0 to 1.0 score to a letter grade."""
    if score >= 0.7:
        return "A+"
    elif score >= 0.5:
        return "A"
    elif score >= 0.3:
        return "B+"
    elif score >= 0.1:
        return "B"
    elif score >= -0.1:
        return "C"
    elif score >= -0.3:
        return "D"
    elif score >= -0.5:
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

    avg = total / len(rows)
    return round(avg, 4), details


async def get_sentiment_score(instrument_id: str) -> tuple[float, dict]:
    """Get instrument-specific sentiment score from financial news (last 3 days)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=3)

    async with async_session() as session:
        # Direct instrument-mapped articles
        result = await session.execute(
            text("""
                SELECT s.label, COUNT(*) as cnt
                FROM sentiment_scores s
                JOIN news_instrument_map m ON m.article_id = s.article_id
                JOIN news_articles a ON a.id = m.article_id
                WHERE m.instrument_id = :iid
                AND a.published_at >= :cutoff
                GROUP BY s.label
            """),
            {"iid": instrument_id, "cutoff": cutoff},
        )
        rows = result.fetchall()

        if rows:
            total_score = 0.0
            total_cnt = 0
            label_details = {}
            for r in rows:
                cnt = int(r.cnt)
                label_details[r.label] = cnt
                total_cnt += cnt
                total_score += SENTIMENT_MULTIPLIERS.get(r.label, 0.0) * cnt
            
            if total_cnt > 0:
                net = total_score / total_cnt
                return round(net, 4), {"articles": total_cnt, "labels": label_details}

        # Fallback: use general finance news sentiment
        result = await session.execute(
            text("""
                SELECT s.label, COUNT(*) as cnt
                FROM sentiment_scores s
                JOIN news_articles a ON a.id = s.article_id
                WHERE a.category IN ('us_finance', 'uk_finance')
                AND a.published_at >= :cutoff
                GROUP BY s.label
            """),
            {"cutoff": cutoff},
        )
        rows = result.fetchall()

        if rows:
            total_score = 0.0
            total_cnt = 0
            for r in rows:
                cnt = int(r.cnt)
                total_cnt += cnt
                total_score += SENTIMENT_MULTIPLIERS.get(r.label, 0.0) * cnt
            
            if total_cnt > 0:
                net = total_score / total_cnt
                return round(net, 4), {"articles": total_cnt, "source": "general_finance"}

    return 0.0, {}


async def get_macro_score() -> tuple[float, dict]:
    """Get latest macro sentiment score (from politics + finance news).

    Uses DISTINCT ON to get the latest entry per region, ensuring we always
    have the freshest macro sentiment.
    """
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (region) region, score, label, article_count, calculated_at
                FROM macro_sentiment
                WHERE calculated_at >= NOW() - INTERVAL '4 hours'
                ORDER BY region, calculated_at DESC
            """)
        )
        rows = result.fetchall()

    if not rows:
        return 0.0, {}

    total_score = 0.0
    details = {}
    for row in rows:
        total_score += float(row.score)
        details[row.region] = {
            "score": float(row.score),
            "label": row.label,
            "articles": row.article_count,
        }

    avg = total_score / len(rows)
    return round(avg, 4), details


# Category-specific weight profiles for institutional-grade grading.
# Commodities are more macro-driven (geopolitics, central bank policy).
# Stocks are more sentiment/news-driven in the short term.
# ETFs blend characteristics of their underlying assets.
WEIGHT_PROFILES = {
    "stock": {
        "short": {"technical": 0.45, "sentiment": 0.35, "macro": 0.20},
        "long":  {"technical": 0.35, "sentiment": 0.30, "macro": 0.35},
    },
    "etf": {
        "short": {"technical": 0.45, "sentiment": 0.30, "macro": 0.25},
        "long":  {"technical": 0.30, "sentiment": 0.30, "macro": 0.40},
    },
    "commodity": {
        "short": {"technical": 0.40, "sentiment": 0.20, "macro": 0.40},
        "long":  {"technical": 0.25, "sentiment": 0.25, "macro": 0.50},
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
    overall = round(overall, 4)
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
