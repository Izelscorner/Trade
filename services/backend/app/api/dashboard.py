"""Dashboard API endpoints - aggregated data for the frontend dashboard."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, DashboardInstrumentSchema, MacroSentimentSchema, SectorSentimentSchema

router = APIRouter()


@router.get("", response_model=APIResponse)
async def get_dashboard():
    """Get full dashboard data: all instruments with latest prices and grades."""
    async with async_session() as session:
        # All instruments with latest prices
        result = await session.execute(
            text("""
                SELECT
                    i.id, i.symbol, i.name, i.category, i.sector,
                    lp.price, lp.change_amount, lp.change_percent, lp.market_status,
                    gs.overall_grade as short_grade, gs.overall_score as short_score,
                    gs.pure_grade as short_pure_grade, gs.pure_score as short_pure_score,
                    gl.overall_grade as long_grade, gl.overall_score as long_score,
                    gl.pure_grade as long_pure_grade, gl.pure_score as long_pure_score,
                    COALESCE(gs.graded_at, gl.graded_at) as graded_at
                FROM instruments i
                LEFT JOIN LATERAL (
                    SELECT price, change_amount, change_percent, market_status
                    FROM live_prices WHERE instrument_id = i.id
                    ORDER BY fetched_at DESC LIMIT 1
                ) lp ON true
                LEFT JOIN LATERAL (
                    SELECT overall_grade, overall_score, pure_grade, pure_score, graded_at
                    FROM grades WHERE instrument_id = i.id AND term = 'short'
                    ORDER BY graded_at DESC LIMIT 1
                ) gs ON true
                LEFT JOIN LATERAL (
                    SELECT overall_grade, overall_score, pure_grade, pure_score, graded_at
                    FROM grades WHERE instrument_id = i.id AND term = 'long'
                    ORDER BY graded_at DESC LIMIT 1
                ) gl ON true
                WHERE i.is_active = true
                ORDER BY i.symbol
            """)
        )
        rows = result.fetchall()

    instruments = [
        DashboardInstrumentSchema(
            id=str(r.id),
            symbol=r.symbol,
            name=r.name,
            category=r.category,
            sector=r.sector,
            price=float(r.price) if r.price is not None else None,
            change_amount=float(r.change_amount) if r.change_amount is not None else None,
            change_percent=float(r.change_percent) if r.change_percent is not None else None,
            market_status=r.market_status,
            short_term_grade=r.short_grade,
            short_term_score=float(r.short_score) if r.short_score is not None else None,
            long_term_grade=r.long_grade,
            long_term_score=float(r.long_score) if r.long_score is not None else None,
            short_term_pure_grade=r.short_pure_grade,
            short_term_pure_score=float(r.short_pure_score) if r.short_pure_score is not None else None,
            long_term_pure_grade=r.long_pure_grade,
            long_term_pure_score=float(r.long_pure_score) if r.long_pure_score is not None else None,
            graded_at=r.graded_at,
        )
        for r in rows
    ]

    return APIResponse(
        data=[inst.model_dump() for inst in instruments],
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/macro", response_model=APIResponse)
async def get_macro_sentiment():
    """Get latest global macro sentiment (both short-term and long-term)."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (term) region, term, score, label, article_count, calculated_at
                FROM macro_sentiment
                ORDER BY term, calculated_at DESC
            """)
        )
        rows = result.fetchall()

    sentiments = []
    for r in rows:
        raw_score = float(r.score)
        confidence = min(1.0, r.article_count / 10)
        effective_score = round(raw_score * confidence, 4)
        if effective_score > 0.25:
            label = "positive"
        elif effective_score < -0.25:
            label = "negative"
        else:
            label = "neutral"
        sentiments.append(
            MacroSentimentSchema(
                region=r.region,
                term=r.term if hasattr(r, "term") else "short",
                score=effective_score,
                label=label,
                article_count=r.article_count,
                calculated_at=r.calculated_at,
            )
        )
    return APIResponse(data=[s.model_dump() for s in sentiments], timestamp=datetime.now(timezone.utc))


@router.get("/macro/news", response_model=APIResponse)
async def get_macro_news():
    """Get all non-neutral macro news used to calculate macro sentiment.

    Returns macro-tagged news articles with their sentiment scores,
    ordered by recency. Excludes neutral-only articles.
    """
    from ..schemas import NewsArticleSchema, SentimentSchema

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                       a.is_macro, a.is_asset_specific, a.published_at,
                       COALESCE(a.macro_sentiment_label, 'neutral') as macro_sentiment_label,
                       a.macro_long_term_label
                FROM news_articles a
                WHERE a.is_macro = true
                AND a.ollama_processed = true
                AND (
                    COALESCE(a.macro_sentiment_label, 'neutral') != 'neutral'
                    OR COALESCE(a.macro_long_term_label, 'neutral') != 'neutral'
                )
                ORDER BY a.published_at DESC
            """)
        )
        rows = result.fetchall()

    # Map macro sentiment labels to probability distributions for display
    _MACRO_PROBS = {
        "very positive": (0.90, 0.02, 0.08),
        "positive": (0.70, 0.05, 0.25),
        "neutral": (0.15, 0.15, 0.70),
        "negative": (0.05, 0.70, 0.25),
        "very negative": (0.02, 0.90, 0.08),
    }

    articles = []
    for r in rows:
        sentiment = None
        label = r.macro_sentiment_label
        lt_label = getattr(r, "macro_long_term_label", None)
        if label:
            pos, neg, neu = _MACRO_PROBS.get(label, (0.15, 0.15, 0.70))
            sentiment = SentimentSchema(
                positive=pos,
                negative=neg,
                neutral=neu,
                label=label,
                long_term_label=lt_label,
            )
        articles.append(
            NewsArticleSchema(
                id=str(r.id),
                title=r.title,
                link=r.link,
                summary=r.summary,
                source=r.source,
                category=r.category,
                is_macro=r.is_macro,
                is_asset_specific=r.is_asset_specific,
                published_at=r.published_at,
                sentiment=sentiment,
            ).model_dump()
        )

    return APIResponse(data=articles, timestamp=datetime.now(timezone.utc))


@router.get("/sector", response_model=APIResponse)
async def get_sector_sentiment(sector: str | None = None):
    """Get latest sector sentiment (both short-term and long-term).

    If sector is provided, returns sentiment for that sector only.
    Otherwise returns latest sentiment for all sectors.
    """
    async with async_session() as session:
        if sector:
            result = await session.execute(
                text("""
                    SELECT DISTINCT ON (term) sector, term, score, label, article_count, calculated_at
                    FROM sector_sentiment
                    WHERE sector = :sector
                    ORDER BY term, calculated_at DESC
                """),
                {"sector": sector},
            )
        else:
            result = await session.execute(
                text("""
                    SELECT DISTINCT ON (sector, term) sector, term, score, label, article_count, calculated_at
                    FROM sector_sentiment
                    ORDER BY sector, term, calculated_at DESC
                """)
            )
        rows = result.fetchall()

    sentiments = []
    for r in rows:
        raw_score = float(r.score)
        confidence = min(1.0, r.article_count / 8)
        effective_score = round(raw_score * confidence, 4)
        # Sector scores are on [-1, 1] scale (from SENTIMENT_MULTIPLIERS),
        # so use proportionally smaller thresholds than macro's ±0.25
        if effective_score > 0.08:
            label = "positive"
        elif effective_score < -0.08:
            label = "negative"
        else:
            label = "neutral"
        sentiments.append(
            SectorSentimentSchema(
                sector=r.sector,
                term=r.term,
                score=effective_score,
                label=label,
                article_count=r.article_count,
                calculated_at=r.calculated_at,
            )
        )
    return APIResponse(data=[s.model_dump() for s in sentiments], timestamp=datetime.now(timezone.utc))
