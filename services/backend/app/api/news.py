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

    Filter by category (us_politics, uk_politics, us_finance, uk_finance)
    or by instrument_id for mapped articles.
    """
    params: dict = {"limit": min(limit, 200)}

    if instrument_id:
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category, a.published_at,
                   s.positive, s.negative, s.neutral, s.label
            FROM news_articles a
            JOIN news_instrument_map m ON m.article_id = a.id
            LEFT JOIN sentiment_scores s ON s.article_id = a.id
            WHERE m.instrument_id = :iid
            ORDER BY a.published_at DESC
            LIMIT :limit
        """
        params["iid"] = instrument_id
    elif category:
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category, a.published_at,
                   s.positive, s.negative, s.neutral, s.label
            FROM news_articles a
            LEFT JOIN sentiment_scores s ON s.article_id = a.id
            WHERE a.category = :cat
            ORDER BY a.published_at DESC
            LIMIT :limit
        """
        params["cat"] = category
    else:
        query = """
            SELECT a.id, a.title, a.link, a.summary, a.source, a.category, a.published_at,
                   s.positive, s.negative, s.neutral, s.label
            FROM news_articles a
            LEFT JOIN sentiment_scores s ON s.article_id = a.id
            ORDER BY a.published_at DESC
            LIMIT :limit
        """

    async with async_session() as session:
        result = await session.execute(text(query), params)
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
