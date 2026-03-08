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
    Only returns articles that have been processed by the LLM Processor.
    """
    params: dict = {"limit": min(limit, 200)}

    if instrument_id:
        # Asset page: show asset-perspective sentiment from sentiment_scores
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                   a.is_macro, a.is_asset_specific, a.published_at,
                   s.positive, s.negative, s.neutral, s.label,
                   a.macro_sentiment_label
            FROM news_articles a
            JOIN news_instrument_map m ON m.article_id = a.id
            JOIN sentiment_scores s ON s.article_id = a.id
            WHERE m.instrument_id = :iid
            AND a.ollama_processed = true
            ORDER BY a.published_at DESC
            LIMIT :limit
        """
        params["iid"] = instrument_id
        use_asset_sentiment = True
    elif category:
        if category == "macro":
            query = """
                SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                       a.is_macro, a.is_asset_specific, a.published_at,
                       a.macro_sentiment_label
                FROM news_articles a
                WHERE a.is_macro = true
                AND a.ollama_processed = true
                AND a.macro_sentiment_label IS NOT NULL
                ORDER BY a.published_at DESC
                LIMIT :limit
            """
            use_asset_sentiment = False
        else:
            # Category view: return both sentiments, use macro for macro articles
            query = """
                SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                       a.is_macro, a.is_asset_specific, a.published_at,
                       s.positive, s.negative, s.neutral, s.label,
                       a.macro_sentiment_label
                FROM news_articles a
                LEFT JOIN sentiment_scores s ON s.article_id = a.id
                WHERE a.category = :cat
                AND a.ollama_processed = true
                AND (s.article_id IS NOT NULL OR a.macro_sentiment_label IS NOT NULL)
                ORDER BY a.published_at DESC
                LIMIT :limit
            """
            params["cat"] = category
            use_asset_sentiment = False
    else:
        # All news: return both sentiments, use macro for macro articles
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category,
                   a.is_macro, a.is_asset_specific, a.published_at,
                   s.positive, s.negative, s.neutral, s.label,
                   a.macro_sentiment_label
            FROM news_articles a
            LEFT JOIN sentiment_scores s ON s.article_id = a.id
            WHERE a.ollama_processed = true
            AND (s.article_id IS NOT NULL OR a.macro_sentiment_label IS NOT NULL)
            ORDER BY a.published_at DESC
            LIMIT :limit
        """
        use_asset_sentiment = False

    async with async_session() as session:
        result = await session.execute(text(query), params)
        rows = result.fetchall()

    # Macro label to probability mapping
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
        macro_label = getattr(r, "macro_sentiment_label", None)
        score_label = getattr(r, "label", None)

        if use_asset_sentiment:
            # Asset page: always use asset-perspective sentiment_scores
            if score_label is not None:
                sentiment = SentimentSchema(
                    positive=float(r.positive),
                    negative=float(r.negative),
                    neutral=float(r.neutral),
                    label=score_label,
                )
        elif r.is_macro and macro_label:
            # Macro article in general view: use macro perspective
            pos, neg, neu = _MACRO_PROBS.get(macro_label, (0.15, 0.15, 0.70))
            sentiment = SentimentSchema(
                positive=pos, negative=neg, neutral=neu, label=macro_label,
            )
        elif score_label is not None:
            # Non-macro article: use asset-perspective sentiment
            sentiment = SentimentSchema(
                positive=float(r.positive),
                negative=float(r.negative),
                neutral=float(r.neutral),
                label=score_label,
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

        # Clear all existing priorities — only one instrument prioritized at a time
        await session.execute(text("DELETE FROM processing_priority"))
        await session.execute(
            text("""
                INSERT INTO processing_priority (instrument_id, requested_at)
                VALUES (:iid, NOW())
            """),
            {"iid": instrument_id},
        )
        await session.commit()

    return {"status": "ok", "message": f"Prioritized {unprocessed} articles", "unprocessed": unprocessed}
