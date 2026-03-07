"""Persist articles to the database."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

_migrated = False


async def ensure_schema():
    """Migrate news_articles table to new schema if needed."""
    global _migrated
    if _migrated:
        return
    async with async_session() as session:
        # Add new columns if they don't exist
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS content TEXT"
        ))
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS is_macro BOOLEAN NOT NULL DEFAULT false"
        ))
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS is_asset_specific BOOLEAN NOT NULL DEFAULT false"
        ))
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS ollama_processed BOOLEAN NOT NULL DEFAULT false"
        ))
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS macro_sentiment_label VARCHAR(30)"
        ))
        # Drop old category constraint and add new one
        await session.execute(text(
            "ALTER TABLE news_articles DROP CONSTRAINT IF EXISTS news_articles_category_check"
        ))
        await session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE news_articles ADD CONSTRAINT news_articles_category_check
                    CHECK (category IN ('macro_markets', 'macro_politics', 'macro_conflict', 'asset_specific'));
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;
        """))
        # Drop old region constraint on macro_sentiment and add new one
        await session.execute(text(
            "ALTER TABLE macro_sentiment DROP CONSTRAINT IF EXISTS macro_sentiment_region_check"
        ))
        # Create partial index for unprocessed articles if not exists
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_news_articles_unprocessed
            ON news_articles(ollama_processed, fetched_at DESC)
            WHERE ollama_processed = false
        """))
        await session.commit()
    _migrated = True
    logger.info("Schema migration complete")


async def upsert_articles(articles: list[dict]) -> int:
    """Insert articles, skipping duplicates, and map to an instrument if provided.

    Articles are inserted with ollama_processed=false. The llm-processor
    service will pick them up for classification and sentiment analysis.
    """
    if not articles:
        return 0

    await ensure_schema()

    inserted = 0

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

                all_to_check = seen_in_batch + recent_db_articles

                for rid, rtitle, rsummary in all_to_check:
                    if title_lower == rtitle:
                        matched_id = rid
                        break

                    if len(title_lower) < 20 or len(rtitle) < 20:
                        continue

                    try:
                        from rapidfuzz import fuzz

                        title_ratio = fuzz.ratio(title_lower, rtitle)

                        if title_ratio > 90:
                            matched_id = rid
                            break

                        if len(title_lower) > 50 and len(rtitle) > 50:
                            if fuzz.partial_ratio(title_lower, rtitle) > 95:
                                matched_id = rid
                                break

                        if summary_lower and rsummary and len(summary_lower) > 100 and len(rsummary) > 100:
                            if fuzz.ratio(summary_lower[:500], rsummary[:500]) > 90:
                                matched_id = rid
                                break
                    except ImportError:
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
                    seen_in_batch.append((matched_id, title_lower, summary_lower))
                else:
                    # Determine initial flags based on category
                    is_macro = article["category"] in ("macro_markets", "macro_politics", "macro_conflict")
                    is_asset = article["category"] == "asset_specific"

                    result = await session.execute(
                        text("""
                            INSERT INTO news_articles (title, link, summary, content, source, category,
                                                       is_macro, is_asset_specific, ollama_processed, published_at)
                            VALUES (:title, :link, :summary, :content, :source, :category,
                                    :is_macro, :is_asset_specific, false, :published_at)
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
                            "is_macro": is_macro,
                            "is_asset_specific": is_asset,
                            "published_at": article["published_at"],
                        }
                    )
                    row = result.fetchone()
                    if row:
                        inserted += 1
                        seen_in_batch.append((row[0], title_lower, summary_lower))

                # Map to instrument — also for duplicates so existing articles link to new instruments
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


# Rolling window caps — keep only the N most recent articles per bucket.
# When new articles push past the cap, oldest are evicted automatically.
MACRO_CAP_PER_CATEGORY = 75   # 75 × 3 categories = 225 macro articles max
ASSET_CAP_PER_INSTRUMENT = 50  # 50 × 7 instruments = 350 asset articles max


async def _delete_articles(session, ids: list) -> int:
    """Cascade-delete articles by ID (scores → map → articles)."""
    if not ids:
        return 0
    id_list = ", ".join(f"'{i}'" for i in ids)
    await session.execute(text(f"DELETE FROM sentiment_scores WHERE article_id IN ({id_list})"))
    await session.execute(text(f"DELETE FROM news_instrument_map WHERE article_id IN ({id_list})"))
    result = await session.execute(text(f"DELETE FROM news_articles WHERE id IN ({id_list})"))
    return result.rowcount


async def cleanup_old_macro_news() -> int:
    """Keep only the MACRO_CAP_PER_CATEGORY most recent articles per macro category.

    Newer articles always displace older ones — true rolling window.
    """
    total_deleted = 0
    categories = ("macro_markets", "macro_politics", "macro_conflict")
    async with async_session() as session:
        for cat in categories:
            result = await session.execute(
                text("""
                    SELECT id FROM news_articles
                    WHERE category = :cat AND is_asset_specific = false
                    ORDER BY published_at DESC
                    OFFSET :cap
                """),
                {"cat": cat, "cap": MACRO_CAP_PER_CATEGORY},
            )
            old_ids = [str(r.id) for r in result.fetchall()]
            deleted = await _delete_articles(session, old_ids)
            if deleted:
                logger.info("Trimmed %d old %s articles (cap=%d)", deleted, cat, MACRO_CAP_PER_CATEGORY)
                total_deleted += deleted
        await session.commit()
    return total_deleted


async def cleanup_old_asset_news() -> int:
    """Keep only the ASSET_CAP_PER_INSTRUMENT most recent articles per instrument.

    Newer articles always displace older ones — true rolling window.
    """
    total_deleted = 0
    async with async_session() as session:
        # Get all instrument IDs
        inst_result = await session.execute(text("SELECT id FROM instruments"))
        instrument_ids = [str(r.id) for r in inst_result.fetchall()]

        for inst_id in instrument_ids:
            result = await session.execute(
                text("""
                    SELECT a.id FROM news_articles a
                    JOIN news_instrument_map m ON m.article_id = a.id
                    WHERE m.instrument_id = :iid
                    AND a.category = 'asset_specific'
                    ORDER BY a.published_at DESC
                    OFFSET :cap
                """),
                {"iid": inst_id, "cap": ASSET_CAP_PER_INSTRUMENT},
            )
            old_ids = [str(r.id) for r in result.fetchall()]
            deleted = await _delete_articles(session, old_ids)
            if deleted:
                logger.info("Trimmed %d old asset articles for instrument %s (cap=%d)", deleted, inst_id, ASSET_CAP_PER_INSTRUMENT)
                total_deleted += deleted
        await session.commit()
    return total_deleted
