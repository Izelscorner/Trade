"""Dashboard API endpoints - aggregated data for the frontend dashboard."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, DashboardInstrumentSchema, MacroSentimentSchema

router = APIRouter()


@router.get("", response_model=APIResponse)
async def get_dashboard():
    """Get full dashboard data: all instruments with latest prices and grades."""
    async with async_session() as session:
        # All instruments with latest prices
        result = await session.execute(
            text("""
                SELECT
                    i.id, i.symbol, i.name, i.category,
                    lp.price, lp.change_amount, lp.change_percent, lp.market_status,
                    gs.overall_grade as short_grade, gs.overall_score as short_score,
                    gl.overall_grade as long_grade, gl.overall_score as long_score,
                    COALESCE(gs.graded_at, gl.graded_at) as graded_at
                FROM instruments i
                LEFT JOIN LATERAL (
                    SELECT price, change_amount, change_percent, market_status
                    FROM live_prices WHERE instrument_id = i.id
                    ORDER BY fetched_at DESC LIMIT 1
                ) lp ON true
                LEFT JOIN LATERAL (
                    SELECT overall_grade, overall_score, graded_at
                    FROM grades WHERE instrument_id = i.id AND term = 'short'
                    ORDER BY graded_at DESC LIMIT 1
                ) gs ON true
                LEFT JOIN LATERAL (
                    SELECT overall_grade, overall_score, graded_at
                    FROM grades WHERE instrument_id = i.id AND term = 'long'
                    ORDER BY graded_at DESC LIMIT 1
                ) gl ON true
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
            price=float(r.price) if r.price is not None else None,
            change_amount=float(r.change_amount) if r.change_amount is not None else None,
            change_percent=float(r.change_percent) if r.change_percent is not None else None,
            market_status=r.market_status,
            short_term_grade=r.short_grade,
            short_term_score=float(r.short_score) if r.short_score is not None else None,
            long_term_grade=r.long_grade,
            long_term_score=float(r.long_score) if r.long_score is not None else None,
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
    """Get latest macro sentiment for US and UK."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (region)
                    region, score, label, article_count, calculated_at
                FROM macro_sentiment
                ORDER BY region, calculated_at DESC
            """)
        )
        rows = result.fetchall()

    sentiments = [
        MacroSentimentSchema(
            region=r.region,
            score=float(r.score),
            label=r.label,
            article_count=r.article_count,
            calculated_at=r.calculated_at,
        )
        for r in rows
    ]
    return APIResponse(data=[s.model_dump() for s in sentiments], timestamp=datetime.now(timezone.utc))


@router.get("/macro/news", response_model=APIResponse)
async def get_macro_news(limit: int = 30):
    """Get latest macro-economic and political news that drives macro sentiment.

    Returns both political and financial news articles with their sentiment
    scores, ordered by recency. This is the news that feeds into the
    macro sentiment calculation.
    """
    from ..schemas import NewsArticleSchema, SentimentSchema

    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT a.id, a.title, a.link, a.summary, a.source, a.category, a.published_at,
                       s.positive, s.negative, s.neutral, s.label
                FROM news_articles a
                LEFT JOIN sentiment_scores s ON s.article_id = a.id
                WHERE a.category IN ('us_politics', 'uk_politics', 'us_finance', 'uk_finance')
                ORDER BY a.published_at DESC
                LIMIT :limit
            """),
            {"limit": min(limit, 100)},
        )
        rows = result.fetchall()

    articles = []
    for r in rows:
        sentiment = None
        if r.label is not None:
            sentiment = SentimentSchema(
                positive=float(r.positive),
                negative=float(r.negative),
                neutral=float(r.neutral),
                label=r.label,
            )
        articles.append(
            NewsArticleSchema(
                id=str(r.id),
                title=r.title,
                link=r.link,
                summary=r.summary,
                source=r.source,
                category=r.category,
                published_at=r.published_at,
                sentiment=sentiment,
            ).model_dump()
        )

    return APIResponse(data=articles, timestamp=datetime.now(timezone.utc))
