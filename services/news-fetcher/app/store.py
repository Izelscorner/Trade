"""Persist articles to the database."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)


async def upsert_articles(articles: list[dict]) -> int:
    """Insert articles, skipping duplicates. Returns count of newly inserted."""
    if not articles:
        return 0

    inserted = 0
    async with async_session() as session:
        for article in articles:
            try:
                result = await session.execute(
                    text("""
                        INSERT INTO news_articles (title, link, summary, source, category, published_at)
                        VALUES (:title, :link, :summary, :source, :category, :published_at)
                        ON CONFLICT (title, source) DO NOTHING
                        RETURNING id
                    """),
                    article,
                )
                if result.fetchone():
                    inserted += 1
            except Exception:
                logger.exception("Failed to insert article: %s", article["title"][:80])
                await session.rollback()
                continue
        await session.commit()

    logger.info("Inserted %d new articles out of %d fetched", inserted, len(articles))
    return inserted


async def cleanup_old_politics_news() -> int:
    """Remove political news older than 24 hours (macro sentiment is rolling)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    async with async_session() as session:
        # Delete sentiment scores for articles that will be removed
        await session.execute(
            text("""
                DELETE FROM sentiment_scores
                WHERE article_id IN (
                    SELECT id FROM news_articles
                    WHERE category IN ('us_politics', 'uk_politics')
                    AND published_at < :cutoff
                )
            """),
            {"cutoff": cutoff},
        )
        result = await session.execute(
            text("""
                DELETE FROM news_articles
                WHERE category IN ('us_politics', 'uk_politics')
                AND published_at < :cutoff
            """),
            {"cutoff": cutoff},
        )
        await session.commit()
        count = result.rowcount
        if count:
            logger.info("Cleaned up %d old political news articles", count)
        return count


async def cleanup_old_finance_news() -> int:
    """Remove financial news older than 30 days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    async with async_session() as session:
        await session.execute(
            text("""
                DELETE FROM sentiment_scores
                WHERE article_id IN (
                    SELECT id FROM news_articles
                    WHERE category IN ('us_finance', 'uk_finance')
                    AND published_at < :cutoff
                )
            """),
            {"cutoff": cutoff},
        )
        await session.execute(
            text("""
                DELETE FROM news_instrument_map
                WHERE article_id IN (
                    SELECT id FROM news_articles
                    WHERE category IN ('us_finance', 'uk_finance')
                    AND published_at < :cutoff
                )
            """),
            {"cutoff": cutoff},
        )
        result = await session.execute(
            text("""
                DELETE FROM news_articles
                WHERE category IN ('us_finance', 'uk_finance')
                AND published_at < :cutoff
            """),
            {"cutoff": cutoff},
        )
        await session.commit()
        count = result.rowcount
        if count:
            logger.info("Cleaned up %d old financial news articles", count)
        return count
