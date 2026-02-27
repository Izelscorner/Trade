import asyncio
import logging
import os
from sqlalchemy import text
from app.db import async_session
from app.model import analyze_batch, analyze_asset_specific_batch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrator")

async def migrate():
    async with async_session() as session:
        # Get all articles to re-score
        query = """
            SELECT a.id, a.title, a.summary, a.content, a.category, i.name as asset_name
            FROM news_articles a
            LEFT JOIN news_instrument_map m ON m.article_id = a.id
            LEFT JOIN instruments i ON m.instrument_id = i.id
            ORDER BY a.published_at DESC
        """
        result = await session.execute(text(query))
        articles = [
            {
                "id": str(r.id),
                "title": r.title,
                "summary": r.summary,
                "content": getattr(r, "content", None),
                "category": r.category,
                "asset_name": getattr(r, "asset_name", None),
            }
            for r in result.fetchall()
        ]
        
        logger.info(f"Loaded {len(articles)} articles to re-evaluate sentiment.")

        # Clear existing scores to cleanly re-insert
        await session.execute(text("TRUNCATE TABLE sentiment_scores RESTART IDENTITY"))
        await session.commit()
        logger.info("Cleared existing sentiment scores.")
        
        # Batch processing
        batch_size = 50
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i+batch_size]
            
            asset_specific = [a for a in batch if a["category"] == "asset_specific" and a.get("asset_name")]
            others = [a for a in batch if not (a["category"] == "asset_specific" and a.get("asset_name"))]
            
            pairs = []
            
            def build_text(a):
                title = a.get("title") or ""
                summary = a.get("summary") or ""
                content = a.get("content") or ""
                if content:
                    return f"{title}. {content}"
                elif summary:
                    return f"{title}. {summary}"
                return title

            if others:
                texts = [build_text(a) for a in others]
                scores = analyze_batch(texts)
                pairs.extend([(a["id"], s) for a, s in zip(others, scores)])
            
            if asset_specific:
                texts = [build_text(a) for a in asset_specific]
                asset_names = [a["asset_name"] for a in asset_specific]
                scores = analyze_asset_specific_batch(texts, asset_names)
                pairs.extend([(a["id"], s) for a, s in zip(asset_specific, scores)])
            
            # Store scores
            for article_id, score in pairs:
                try:
                    async with session.begin_nested():
                        await session.execute(
                            text("""
                                INSERT INTO sentiment_scores (article_id, positive, negative, neutral, label)
                                VALUES (:aid, :positive, :negative, :neutral, :label)
                                ON CONFLICT (article_id) DO UPDATE SET
                                    positive = EXCLUDED.positive,
                                    negative = EXCLUDED.negative,
                                    neutral = EXCLUDED.neutral,
                                    label = EXCLUDED.label
                            """),
                            {
                                "aid": article_id,
                                "positive": score["positive"],
                                "negative": score["negative"],
                                "neutral": score["neutral"],
                                "label": score["label"],
                            }
                        )
                except Exception as e:
                    logger.warning(f"Failed to save score for {article_id}: {e}")
            
            await session.commit()
            logger.info(f"Processed batch {i} to {i+len(batch)} of {len(articles)}")

        logger.info("Migration complete.")

if __name__ == "__main__":
    asyncio.run(migrate())
