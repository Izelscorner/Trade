"""Persist articles to the database."""

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

_migrated = False

# In-memory cache of known URL hashes — avoids DB round-trip for every article.
# Populated once from DB on first call, then kept in sync as new articles arrive.
_known_hashes: set[str] = set()
_hash_cache_loaded = False


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
        # Drop old category constraint and add new one (includes sector categories)
        await session.execute(text(
            "ALTER TABLE news_articles DROP CONSTRAINT IF EXISTS news_articles_category_check"
        ))
        await session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE news_articles ADD CONSTRAINT news_articles_category_check
                    CHECK (category IN (
                        'macro_markets', 'macro_politics', 'macro_conflict', 'asset_specific',
                        'sector_technology', 'sector_financials', 'sector_healthcare',
                        'sector_consumer_discretionary', 'sector_consumer_staples',
                        'sector_communication', 'sector_energy', 'sector_industrials',
                        'sector_materials', 'sector_utilities', 'sector_real_estate'
                    ));
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;
        """))
        # Add sector column to instruments if not exists
        await session.execute(text(
            "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS sector VARCHAR(50)"
        ))
        # Create sector_sentiment table if not exists
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS sector_sentiment (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                sector VARCHAR(50) NOT NULL,
                term VARCHAR(10) NOT NULL DEFAULT 'short',
                score NUMERIC(7, 6) NOT NULL,
                label VARCHAR(10) NOT NULL,
                article_count INT NOT NULL DEFAULT 0,
                calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_sector_sentiment_sector_term
            ON sector_sentiment(sector, term, calculated_at DESC)
        """))
        # Add sector_score column to grades if not exists
        await session.execute(text(
            "ALTER TABLE grades ADD COLUMN IF NOT EXISTS sector_score NUMERIC(7, 4) NOT NULL DEFAULT 0"
        ))
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
        # Create fetch history table to prevent refetching filtered articles
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS news_fetch_history (
                url_hash VARCHAR(32) PRIMARY KEY,
                title VARCHAR(500),
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.commit()
    _migrated = True
    logger.info("Schema migration complete")


async def _ensure_hash_cache(session) -> None:
    """Load recent URL hashes from DB on first call.

    Only loads last 30 days — older articles are already deduped by the
    DB UNIQUE constraint on (title, source). This keeps memory bounded
    while still providing fast O(1) lookups for recent articles.
    On restart, this reloads from DB so no hashes are lost.
    """
    global _known_hashes, _hash_cache_loaded
    if _hash_cache_loaded:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    result = await session.execute(
        text("SELECT url_hash FROM news_fetch_history WHERE fetched_at >= :cutoff"),
        {"cutoff": cutoff},
    )
    _known_hashes = {r[0] for r in result.fetchall()}
    _hash_cache_loaded = True
    logger.info("Loaded %d URL hashes into memory cache (last 30 days)", len(_known_hashes))


async def upsert_articles(articles: list[dict]) -> int:
    """Insert articles, skipping duplicates, and map to an instrument if provided.

    Articles are inserted with ollama_processed=false. The llm-processor
    service will pick them up for classification and sentiment analysis.
    """
    if not articles:
        return 0

    import hashlib
    import re

    await ensure_schema()

    inserted = 0

    async with async_session() as session:
        await _ensure_hash_cache(session)

        # Load recent titles for fuzzy dedup (only titles, not full rows)
        cutoff = datetime.now(timezone.utc) - timedelta(days=3)
        recent_res = await session.execute(
            text("SELECT url_hash, title FROM news_fetch_history WHERE fetched_at >= :cutoff ORDER BY fetched_at DESC LIMIT 1500"),
            {"cutoff": cutoff}
        )
        recent_db_articles = [(r[0], (r[1] or "").lower(), "") for r in recent_res.fetchall()]

        seen_in_batch = []

        for article in articles:
            try:
                title_lower = (article["title"] or "").lower()
                summary_lower = (article["summary"] or "").lower()

                # Compute MD5 hash for the article using its link
                link = article.get("link") or ""
                hash_input = link if link else f"{title_lower}-{article.get('source', '')}"
                url_hash = hashlib.md5(hash_input.encode('utf-8')).hexdigest()

                # Fast in-memory hash check — no DB round-trip
                if url_hash in _known_hashes:
                    continue

                # Fuzzy duplicate detection
                matched_id = None

                all_to_check = seen_in_batch + recent_db_articles

                for rid, rtitle, rsummary in all_to_check:
                    # Clean publisher suffixes for a pure comparison
                    clean_title = re.sub(r'\s+[-|]\s+[a-zA-Z\s\.]+$', '', title_lower).strip()
                    clean_rtitle = re.sub(r'\s+[-|]\s+[a-zA-Z\s\.]+$', '', rtitle).strip()

                    if title_lower == rtitle or clean_title == clean_rtitle:
                        matched_id = rid
                        break

                    if len(title_lower) < 20 or len(rtitle) < 20:
                        continue

                    try:
                        from rapidfuzz import fuzz

                        title_ratio = fuzz.ratio(clean_title, clean_rtitle)

                        if title_ratio > 85:
                            matched_id = rid
                            break

                        if len(clean_title) > 30 and len(clean_rtitle) > 30:
                            if fuzz.partial_ratio(clean_title, clean_rtitle) > 90:
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
                    logger.debug("Skipping fuzzy duplicate article: %s", title_lower[:80])
                    # Record hash so we skip it instantly next time (memory + DB)
                    _known_hashes.add(url_hash)
                    await session.execute(
                        text("INSERT INTO news_fetch_history (url_hash, title) VALUES (:h, :t) ON CONFLICT DO NOTHING"),
                        {"h": url_hash, "t": article["title"][:500]}
                    )
                    continue

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
                    logger.info("Stored new article: %s", article["title"][:80])
                    seen_in_batch.append((row[0], title_lower, summary_lower))
                
                # Record to fetch history so we never fetch it again (memory + DB)
                _known_hashes.add(url_hash)
                await session.execute(
                    text("""
                        INSERT INTO news_fetch_history (url_hash, title)
                        VALUES (:url_hash, :title)
                        ON CONFLICT (url_hash) DO NOTHING
                    """),
                    {"url_hash": url_hash, "title": article["title"][:500]}
                )

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
    """Remove macro articles older than 180 days (6 months)."""
    total_deleted = 0
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT id FROM news_articles
                WHERE category IN ('macro_markets', 'macro_politics', 'macro_conflict')
                AND is_asset_specific = false
                AND published_at < NOW() - INTERVAL '180 days'
            """)
        )
        old_ids = [str(r.id) for r in result.fetchall()]
        total_deleted = await _delete_articles(session, old_ids)
        if total_deleted:
            logger.info("Cleaned up %d macro articles older than 180 days", total_deleted)
        await session.commit()
    return total_deleted


async def cleanup_old_asset_news() -> int:
    """Remove asset-specific articles older than 30 days."""
    total_deleted = 0
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT id FROM news_articles
                WHERE category = 'asset_specific'
                AND published_at < NOW() - INTERVAL '30 days'
            """)
        )
        old_ids = [str(r.id) for r in result.fetchall()]
        total_deleted = await _delete_articles(session, old_ids)
        if total_deleted:
            logger.info("Cleaned up %d asset articles older than 30 days", total_deleted)
        await session.commit()
    return total_deleted


async def cleanup_old_sector_news() -> int:
    """Remove sector news articles older than 90 days."""
    total_deleted = 0
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT id FROM news_articles
                WHERE category LIKE 'sector_%%'
                AND published_at < NOW() - INTERVAL '90 days'
            """)
        )
        old_ids = [str(r.id) for r in result.fetchall()]
        total_deleted = await _delete_articles(session, old_ids)
        if total_deleted:
            logger.info("Cleaned up %d sector articles older than 90 days", total_deleted)
        await session.commit()
    return total_deleted
