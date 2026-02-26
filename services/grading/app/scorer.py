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
    """Get instrument-specific sentiment score from financial news."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    async with async_session() as session:
        # Direct instrument-mapped articles
        result = await session.execute(
            text("""
                SELECT AVG(s.positive) as avg_pos, AVG(s.negative) as avg_neg, COUNT(*) as cnt
                FROM sentiment_scores s
                JOIN news_instrument_map m ON m.article_id = s.article_id
                WHERE m.instrument_id = :iid
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()

        if row and row.cnt and row.cnt > 0:
            net = float(row.avg_pos) - float(row.avg_neg)
            return round(net, 4), {"articles": int(row.cnt), "avg_positive": float(row.avg_pos), "avg_negative": float(row.avg_neg)}

        # Fallback: use general finance news sentiment
        result = await session.execute(
            text("""
                SELECT AVG(s.positive) as avg_pos, AVG(s.negative) as avg_neg, COUNT(*) as cnt
                FROM sentiment_scores s
                JOIN news_articles a ON a.id = s.article_id
                WHERE a.category IN ('us_finance', 'uk_finance')
                AND a.published_at >= :cutoff
            """),
            {"cutoff": cutoff},
        )
        row = result.fetchone()

        if row and row.cnt and row.cnt > 0:
            net = float(row.avg_pos) - float(row.avg_neg)
            return round(net, 4), {"articles": int(row.cnt), "source": "general_finance"}

    return 0.0, {}


async def get_macro_score() -> tuple[float, dict]:
    """Get latest macro sentiment score."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT region, score, label, article_count, calculated_at
                FROM macro_sentiment
                WHERE calculated_at >= NOW() - INTERVAL '2 hours'
                ORDER BY calculated_at DESC
                LIMIT 2
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


async def grade_instrument(instrument_id: str, symbol: str, term: str = "short") -> dict | None:
    """Compute a full grade for an instrument.

    term: 'short' uses recent 5-day TA, 'long' uses 30-day TA.
    """
    lookback = 5 if term == "short" else 30

    technical_score, tech_details = await get_technical_score(instrument_id, lookback)
    sentiment_score, sent_details = await get_sentiment_score(instrument_id)
    macro_score, macro_details = await get_macro_score()

    # Weighted combination:
    # Short-term: heavier on technicals and sentiment
    # Long-term: more balanced, macro matters more
    if term == "short":
        weights = {"technical": 0.50, "sentiment": 0.30, "macro": 0.20}
    else:
        weights = {"technical": 0.35, "sentiment": 0.30, "macro": 0.35}

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
                    :technical_score, :sentiment_score, :macro_score, :details::jsonb, :graded_at)
            """),
            grade,
        )
        await session.commit()
