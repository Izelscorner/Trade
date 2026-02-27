"""Background processor: analyzes unscored news articles and updates macro sentiment."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session
from .model import analyze_batch

logger = logging.getLogger(__name__)

PROCESS_INTERVAL = 30  # 30 seconds - fast scoring for near-real-time sentiment


async def get_unscored_articles(limit: int = 50) -> list[dict]:
    """Get articles that haven't been scored yet."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT a.id, a.title, a.summary, a.category
                FROM news_articles a
                LEFT JOIN sentiment_scores s ON s.article_id = a.id
                WHERE s.id IS NULL
                ORDER BY a.published_at DESC
                LIMIT :limit
            """),
            {"limit": limit},
        )
        return [
            {"id": str(r.id), "title": r.title, "summary": r.summary, "category": r.category}
            for r in result.fetchall()
        ]


async def store_scores(article_scores: list[tuple[str, dict]]) -> None:
    """Store sentiment scores in the database."""
    async with async_session() as session:
        for article_id, score in article_scores:
            try:
                await session.execute(
                    text("""
                        INSERT INTO sentiment_scores (article_id, positive, negative, neutral, label)
                        VALUES (:aid, :positive, :negative, :neutral, :label)
                        ON CONFLICT (article_id) DO NOTHING
                    """),
                    {
                        "aid": article_id,
                        "positive": score["positive"],
                        "negative": score["negative"],
                        "neutral": score["neutral"],
                        "label": score["label"],
                    },
                )
            except Exception:
                logger.exception("Failed to store score for article %s", article_id)
                await session.rollback()
                continue
        await session.commit()


async def update_macro_sentiment() -> None:
    """Calculate aggregate macro sentiment from political AND financial news.

    Macro sentiment combines both political climate and economic/financial news
    to provide a holistic view of the macro environment affecting markets.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    # Each region uses BOTH politics AND finance news for a true macro view
    region_categories = [
        ("us", ("us_politics", "us_finance")),
        ("uk", ("uk_politics", "uk_finance")),
    ]

    async with async_session() as session:
        for region, categories in region_categories:
            placeholders = ", ".join(f":cat{i}" for i in range(len(categories)))
            params = {f"cat{i}": c for i, c in enumerate(categories)}
            params["cutoff"] = cutoff

            result = await session.execute(
                text(f"""
                    SELECT
                        AVG(s.positive) as avg_positive,
                        AVG(s.negative) as avg_negative,
                        AVG(s.neutral) as avg_neutral,
                        COUNT(*) as cnt
                    FROM sentiment_scores s
                    JOIN news_articles a ON a.id = s.article_id
                    WHERE a.category IN ({placeholders})
                    AND a.published_at >= :cutoff
                """),
                params,
            )
            row = result.fetchone()

            if not row or not row.cnt:
                continue

            avg_pos = float(row.avg_positive)
            avg_neg = float(row.avg_negative)
            scores = {"positive": avg_pos, "negative": avg_neg, "neutral": float(row.avg_neutral)}
            label = max(scores, key=scores.get)
            net_score = avg_pos - avg_neg

            await session.execute(
                text("""
                    INSERT INTO macro_sentiment (region, score, label, article_count, calculated_at)
                    VALUES (:region, :score, :label, :count, NOW())
                """),
                {"region": region, "score": net_score, "label": label, "count": row.cnt},
            )

        # Cleanup: keep only last 100 entries per region to prevent table bloat
        await session.execute(
            text("""
                DELETE FROM macro_sentiment
                WHERE id NOT IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY region ORDER BY calculated_at DESC) as rn
                        FROM macro_sentiment
                    ) ranked WHERE rn <= 100
                )
            """)
        )

        await session.commit()
    logger.info("Macro sentiment updated (politics + finance)")


async def process_loop() -> None:
    """Main processing loop."""
    while True:
        try:
            articles = await get_unscored_articles(limit=50)
            if articles:
                texts = [
                    f"{a['title']}. {a['summary']}" if a["summary"] else a["title"]
                    for a in articles
                ]
                scores = analyze_batch(texts)
                pairs = [(a["id"], s) for a, s in zip(articles, scores)]
                await store_scores(pairs)
                logger.info("Scored %d articles", len(pairs))

            await update_macro_sentiment()

            # If we fetched a full batch, process the next batch immediately to drain backlog
            if articles and len(articles) == 50:
                await asyncio.sleep(1)
                continue

        except Exception:
            logger.exception("Error in sentiment processing loop")

        await asyncio.sleep(PROCESS_INTERVAL)
