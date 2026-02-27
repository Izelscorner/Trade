"""Persist articles to the database."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

_migrated = False


async def ensure_content_column():
    """Add content column to news_articles if it doesn't exist."""
    global _migrated
    if _migrated:
        return
    async with async_session() as session:
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS content TEXT"
        ))
        await session.commit()
    _migrated = True
    logger.info("Ensured content column exists on news_articles")


async def upsert_articles(articles: list[dict]) -> int:
    """Insert articles, skipping duplicates, and map to an instrument if provided."""
    if not articles:
        return 0

    await ensure_content_column()

    inserted = 0
    import aiohttp
    
    # Check relevance via the new local AI service before inserting
    relevant_articles = []
    
    # Sub-batching articles to avoid huge payloads and long timeouts
    CHUNK_SIZE = 20
    article_chunks = [articles[i:i + CHUNK_SIZE] for i in range(0, len(articles), CHUNK_SIZE)]
    
    try:
        # Increase timeout significantly for AI workloads
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(timeout=timeout) as http_sess:
            for chunk in article_chunks:
                payload = {
                    "articles": [
                        {
                            "title": a["title"],
                            "summary": a["summary"],
                            "category": a["category"],
                            "asset_name": a.get("asset_name")
                        } for a in chunk
                    ]
                }
                try:
                    async with http_sess.post("http://relevance:8002/check/batch", json=payload) as resp:
                        if resp.status == 200:
                            results = await resp.json()
                            for article, res in zip(chunk, results):
                                if res.get("is_relevant"):
                                    relevant_articles.append(article)
                                else:
                                    logger.info("Filtered irrelevant article: '%s' (Reason: %s, Score: %.2f)", 
                                                article["title"][:50], res.get("reason"), res.get("score", 0))
                        else:
                            logger.error("Relevance service returned %d, skipping relevance check for this chunk", resp.status)
                            relevant_articles.extend(chunk) # Fallback pass for this chunk
                except Exception as e:
                    logger.error("Error checking relevance for chunk: %s", e)
                    relevant_articles.extend(chunk) # Fallback pass
    except Exception:
        logger.exception("Failed to connect to relevance service")
        relevant_articles = articles # Total fallback pass
        
    articles = relevant_articles
    if not articles:
        return 0

    async with async_session() as session:
        # Load recent articles to avoid fuzzy duplicates efficiently
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        recent_res = await session.execute(
            text("SELECT id, title, summary FROM news_articles WHERE published_at >= :cutoff ORDER BY id DESC LIMIT 1500"),
            {"cutoff": cutoff}
        )
        recent_db_articles = [(r[0], (r[1] or "").lower(), (r[2] or "").lower()) for r in recent_res.fetchall()]

        seen_in_batch = []

        for article in articles:
            try:
                title_lower = (article["title"] or "").lower()
                summary_lower = (article["summary"] or "").lower()

                # Fuzzy duplicate detection
                matched_id = None

                # Check against already inserted in this batch + recent from DB
                all_to_check = seen_in_batch + recent_db_articles

                for rid, rtitle, rsummary in all_to_check:
                    # Match by exact title
                    if title_lower == rtitle:
                        matched_id = rid
                        break

                    # Skip fuzzy matching on very short titles (too many false positives)
                    if len(title_lower) < 20 or len(rtitle) < 20:
                        continue

                    try:
                        from rapidfuzz import fuzz

                        title_ratio = fuzz.ratio(title_lower, rtitle)

                        # High full-match threshold catches same story from different sources
                        if title_ratio > 88:
                            matched_id = rid
                            break

                        # Partial ratio only for substantial titles to avoid
                        # "AAPL earnings beat" matching "NVDA earnings beat"
                        if len(title_lower) > 40 and len(rtitle) > 40:
                            if fuzz.partial_ratio(title_lower, rtitle) > 92:
                                matched_id = rid
                                break

                        # Summary dedup: same article republished with different headline
                        if summary_lower and rsummary and len(summary_lower) > 100 and len(rsummary) > 100:
                            if fuzz.ratio(summary_lower[:500], rsummary[:500]) > 88:
                                matched_id = rid
                                break
                    except ImportError:
                        # Fallback to simple matching if rapidfuzz not available
                        if (len(title_lower) > 30 and len(rtitle) > 30) and (title_lower in rtitle or rtitle in title_lower):
                            matched_id = rid
                            break

                        t1_words = set(title_lower.split())
                        t2_words = set(rtitle.split())
                        if len(t1_words) > 5 and len(t2_words) > 5:
                            overlap = len(t1_words.intersection(t2_words))
                            if overlap / max(len(t1_words), len(t2_words)) > 0.75:
                                matched_id = rid
                                break

                        if summary_lower and rsummary and len(summary_lower) > 100 and len(rsummary) > 100:
                            if summary_lower == rsummary:
                                matched_id = rid
                                break

                if matched_id:
                    row = [matched_id]
                else:
                    # Insert new article (content is temporary, cleared after sentiment scoring)
                    result = await session.execute(
                        text("""
                            INSERT INTO news_articles (title, link, summary, content, source, category, published_at)
                            VALUES (:title, :link, :summary, :content, :source, :category, :published_at)
                            ON CONFLICT (title, source) DO NOTHING
                            RETURNING id
                        """),
                        {
                            "title": article["title"],
                            "link": article["link"],
                            "summary": article["summary"],
                            "content": article.get("content"),
                            "source": article["source"],
                            "category": article["category"],
                            "published_at": article["published_at"],
                        }
                    )
                    row = result.fetchone()
                    if row:
                        inserted += 1
                        seen_in_batch.append((row[0], title_lower, summary_lower))
                
                # 2. Map to instrument if it has one
                if row and article.get("instrument_id"):
                    await session.execute(
                        text("""
                            INSERT INTO news_instrument_map (article_id, instrument_id)
                            VALUES (:aid, :iid)
                            ON CONFLICT DO NOTHING
                        """),
                        {"aid": row[0], "iid": article["instrument_id"]}
                    )
                    
            except Exception:
                logger.exception("Failed to insert article: %s", article["title"][:80])
                await session.rollback()
                continue
                
        await session.commit()

    if inserted > 0:
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
                    WHERE category IN ('us_finance', 'uk_finance', 'asset_specific')
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
                    WHERE category IN ('us_finance', 'uk_finance', 'asset_specific')
                    AND published_at < :cutoff
                )
            """),
            {"cutoff": cutoff},
        )
        result = await session.execute(
            text("""
                DELETE FROM news_articles
                WHERE category IN ('us_finance', 'uk_finance', 'asset_specific')
                AND published_at < :cutoff
            """),
            {"cutoff": cutoff},
        )
        await session.commit()
        count = result.rowcount
        if count:
            logger.info("Cleaned up %d old financial/asset news articles", count)
        return count
