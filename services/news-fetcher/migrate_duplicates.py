import asyncio
import logging
from app.db import async_session
from sqlalchemy import text
from rapidfuzz import fuzz

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrator")

async def migrate():
    async with async_session() as session:
        # Get all articles ordered by id desc (newest first)
        result = await session.execute(text("SELECT id, title, summary FROM news_articles ORDER BY id DESC"))
        articles = result.fetchall()
        
        logger.info(f"Loaded {len(articles)} articles for deduplication check")
        
        to_delete = []
        kept = []
        
        for row in articles:
            rid, title, summary = row
            title_lower = (title or "").lower()
            summary_lower = (summary or "").lower()
            
            is_dup = False
            for k_rid, k_title, k_summary in kept:
                # 1. Exact match
                if title_lower == k_title:
                    is_dup = True
                    break
                
                # 2. RapidFuzz check
                if fuzz.partial_ratio(title_lower, k_title) > 90 or fuzz.ratio(title_lower, k_title) > 85:
                    is_dup = True
                    break
                
                # 3. Summary check
                if summary_lower and k_summary and len(summary_lower) > 50 and len(k_summary) > 50:
                    if fuzz.ratio(summary_lower[:500], k_summary[:500]) > 85:
                        is_dup = True
                        break
                        
            if is_dup:
                to_delete.append(rid)
            else:
                kept.append((rid, title_lower, summary_lower))
                
        if to_delete:
            logger.info(f"Found {len(to_delete)} duplicate articles to delete.")
            
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
            logger.info("No duplicates found to delete.")

if __name__ == "__main__":
    asyncio.run(migrate())
