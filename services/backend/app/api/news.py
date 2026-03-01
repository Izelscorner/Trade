"""News API endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, NewsArticleSchema, SentimentSchema

router = APIRouter()


@router.get("", response_model=APIResponse)
async def list_news(
    category: str | None = None,
    instrument_id: str | None = None,
    limit: int = 50,
):
    """Get news articles with sentiment scores.

    Filter by category (macro_markets, macro_politics, macro_conflict, asset_specific)
    or by instrument_id for mapped articles.
    Only returns articles that have been processed by Ollama.
    """
    params: dict = {"limit": min(limit, 200)}

    if instrument_id:
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                   a.is_macro, a.is_asset_specific, a.published_at,
                   s.positive, s.negative, s.neutral, s.label
            FROM news_articles a
            JOIN news_instrument_map m ON m.article_id = a.id
            JOIN sentiment_scores s ON s.article_id = a.id
            WHERE m.instrument_id = :iid
            AND a.ollama_processed = true
            ORDER BY a.published_at DESC
            LIMIT :limit
        """
        params["iid"] = instrument_id
    elif category:
        if category == "macro":
            query = """
                SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                       a.is_macro, a.is_asset_specific, a.published_at,
                       a.macro_sentiment_label as label
                FROM news_articles a
                WHERE a.is_macro = true
                AND a.ollama_processed = true
                AND a.macro_sentiment_label IS NOT NULL
                ORDER BY a.published_at DESC
                LIMIT :limit
            """
        else:
            query = """
                SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                       a.is_macro, a.is_asset_specific, a.published_at,
                       s.positive, s.negative, s.neutral, s.label
                FROM news_articles a
                JOIN sentiment_scores s ON s.article_id = a.id
                WHERE a.category = :cat
                AND a.ollama_processed = true
                ORDER BY a.published_at DESC
                LIMIT :limit
            """
            params["cat"] = category
    else:
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                   a.is_macro, a.is_asset_specific, a.published_at,
                   s.positive, s.negative, s.neutral, s.label
            FROM news_articles a
            JOIN sentiment_scores s ON s.article_id = a.id
            WHERE a.ollama_processed = true
            ORDER BY a.published_at DESC
            LIMIT :limit
        """

    async with async_session() as session:
        result = await session.execute(text(query), params)
        rows = result.fetchall()

    # Macro label to probability mapping (for macro queries that use macro_sentiment_label)
    _MACRO_PROBS = {
        "very positive": (0.90, 0.02, 0.08),
        "positive": (0.70, 0.05, 0.25),
        "neutral": (0.15, 0.15, 0.70),
        "negative": (0.05, 0.70, 0.25),
        "very negative": (0.02, 0.90, 0.08),
    }
    is_macro_query = category == "macro"

    articles = []
    for r in rows:
        sentiment = None
        if r.label is not None:
            if is_macro_query:
                pos, neg, neu = _MACRO_PROBS.get(r.label, (0.15, 0.15, 0.70))
                sentiment = SentimentSchema(
                    positive=pos, negative=neg, neutral=neu, label=r.label,
                )
            else:
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
                is_macro=r.is_macro,
                is_asset_specific=r.is_asset_specific,
                published_at=r.published_at,
                sentiment=sentiment,
            ).model_dump()
        )

    return APIResponse(data=articles, timestamp=datetime.now(timezone.utc))


@router.post("/prioritize/{instrument_id}")
async def prioritize_instrument(instrument_id: str):
    """Request the processor to prioritize an instrument's unprocessed articles.

    Called when a user clicks on an asset in the frontend, so they see
    that instrument's news processed first.
    """
    async with async_session() as session:
        # Check unprocessed article count for this instrument
        result = await session.execute(
            text("""
                SELECT count(*) FROM news_articles a
                JOIN news_instrument_map nim ON nim.article_id = a.id
                WHERE nim.instrument_id = :iid AND a.ollama_processed = false
            """),
            {"iid": instrument_id},
        )
        unprocessed = result.scalar() or 0

        if unprocessed == 0:
            return {"status": "ok", "message": "All articles already processed", "unprocessed": 0}

        await session.execute(
            text("""
                INSERT INTO processing_priority (instrument_id, requested_at)
                VALUES (:iid, NOW())
                ON CONFLICT (instrument_id) DO UPDATE SET requested_at = NOW()
            """),
            {"iid": instrument_id},
        )
        await session.commit()

    return {"status": "ok", "message": f"Prioritized {unprocessed} articles", "unprocessed": unprocessed}
