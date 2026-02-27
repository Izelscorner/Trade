import asyncio
import logging
from sqlalchemy import text
from app.db import async_session
from app.model import check_relevance

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrator")

async def migrate():
    async with async_session() as session:
        # Get all articles with instrument info
        query = """
            SELECT a.id, a.title, a.summary, a.category, i.name as asset_name
            FROM news_articles a
            LEFT JOIN news_instrument_map m ON a.id = m.article_id
            LEFT JOIN instruments i ON m.instrument_id = i.id
        """
        result = await session.execute(text(query))
        articles = result.fetchall()
        
        logger.info(f"Loaded {len(articles)} articles to evaluate relevance.")
        
        to_delete = []
        for row in articles:
            rid, title, summary, category, asset_name = row
            # Filter non-null strings
            title_str = title or ""
            summary_str = summary or ""
            
            # Use model to check relevance
            res = check_relevance(title_str, summary_str, category, asset_name)
            
            if not res["is_relevant"]:
                logger.debug(f"Irrelevant {rid}: '{title_str[:50]}' (Reason: {res['reason']})")
                to_delete.append(rid)
                
        if to_delete:
            logger.info(f"Found {len(to_delete)} irrelevant articles to delete.")
            for i in range(0, len(to_delete), 500):
                chunk = to_delete[i:i+500]
                await session.execute(
                    text("DELETE FROM sentiment_scores WHERE article_id = ANY(:ids)"),
                    {"ids": chunk}
                )
                await session.execute(
                    text("DELETE FROM news_instrument_map WHERE article_id = ANY(:ids)"),
                    {"ids": chunk}
                )
                await session.execute(
                    text("DELETE FROM news_articles WHERE id = ANY(:ids)"),
                    {"ids": chunk}
                )
                await session.commit()
                logger.info(f"Deleted chunk {i} to {i+len(chunk)}")
        else:
            logger.info("No irrelevant articles found to delete.")

if __name__ == "__main__":
    asyncio.run(migrate())
