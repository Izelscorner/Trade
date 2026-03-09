"""LLM Processor Service - classifies and scores news articles using NVIDIA NIM.

Uses batch API calls: N articles → 1 NIM request → JSON array of results,
dramatically reducing API call frequency.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional

from .db import async_session
from .nim_client import check_health, close_client, _call_with_retry
from .processor import (
    get_unprocessed_articles,
    get_unprocessed_queue_depth,
    get_adaptive_batch_size,
    get_instruments,
    process_batch,
    update_macro_sentiment,
    update_sector_sentiment,
    assign_sectors,
    cleanup_priority,
    build_name_lookup,
    populate_etf_constituents,
    PROCESS_INTERVAL,
)
from .prompts import build_instrument_context

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("llm-processor")


async def wait_for_nim(max_retries: int = 30, delay: int = 5) -> bool:
    """Wait for NVIDIA NIM API to be reachable."""
    for attempt in range(max_retries):
        if await check_health():
            logger.info("NVIDIA NIM API is reachable")
            return True
        if attempt % 3 == 0:
            logger.info("Waiting for NVIDIA NIM API (attempt %d/%d)...", attempt + 1, max_retries)
        await asyncio.sleep(delay)
    logger.error("NVIDIA NIM API not reachable after %d attempts", max_retries)
    return False


async def ensure_schema():
    """Ensure required columns and tables exist."""
    async with async_session() as session:
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS macro_sentiment_label VARCHAR(30)"
        ))
        await session.execute(text(
            "ALTER TABLE news_articles ADD COLUMN IF NOT EXISTS macro_long_term_label VARCHAR(30)"
        ))
        await session.execute(text(
            "ALTER TABLE sentiment_scores ADD COLUMN IF NOT EXISTS long_term_label VARCHAR(30)"
        ))
        await session.execute(text(
            "ALTER TABLE sentiment_scores ADD COLUMN IF NOT EXISTS long_term_confidence NUMERIC(7,6) DEFAULT 0.5"
        ))
        await session.execute(text(
            "ALTER TABLE macro_sentiment ADD COLUMN IF NOT EXISTS term VARCHAR(10) DEFAULT 'short'"
        ))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS processing_priority (
                instrument_id UUID PRIMARY KEY REFERENCES instruments(id) ON DELETE CASCADE,
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS etf_constituents (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                etf_instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
                constituent_symbol VARCHAR(20) NOT NULL,
                constituent_name VARCHAR(255) NOT NULL,
                weight_percent NUMERIC(7, 4) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(etf_instrument_id, constituent_symbol)
            )
        """))
        await session.execute(text("""
            UPDATE news_articles
            SET macro_sentiment_label = 'neutral'
            WHERE is_macro = true AND ollama_processed = true AND macro_sentiment_label IS NULL
        """))
        # Sector support
        await session.execute(text(
            "ALTER TABLE instruments ADD COLUMN IF NOT EXISTS sector VARCHAR(50)"
        ))
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
        await session.execute(text(
            "ALTER TABLE grades ADD COLUMN IF NOT EXISTS sector_score NUMERIC(7, 4) NOT NULL DEFAULT 0"
        ))
        # Expand category constraint for sector news
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
        # Reset any sector articles that were marked as processed but missing sentiment labels (stuck from old versions)
        await session.execute(text("""
            UPDATE news_articles 
            SET ollama_processed = false 
            WHERE category LIKE 'sector_%%' 
            AND ollama_processed = true 
            AND macro_sentiment_label IS NULL
        """))
        await session.commit()
    logger.info("Schema check complete")


async def macro_sentiment_loop() -> None:
    """Independent loop that recalculates global macro sentiment every 30 seconds."""
    await asyncio.sleep(10)
    logger.info("Macro sentiment loop started (30s interval)")
    while True:
        try:
            await update_macro_sentiment()
        except Exception:
            logger.exception("Error updating macro sentiment")
        await asyncio.sleep(30)


async def sector_sentiment_loop() -> None:
    """Independent loop that recalculates sector sentiment every 60 seconds."""
    await asyncio.sleep(20)
    logger.info("Sector sentiment loop started (60s interval)")
    while True:
        try:
            await update_sector_sentiment()
        except Exception:
            logger.exception("Error updating sector sentiment")
        await asyncio.sleep(60)


async def process_loop() -> None:
    """Main processing loop - picks up unprocessed articles and runs them through NIM in batches."""
    await ensure_schema()

    if not await wait_for_nim():
        logger.error("Cannot start processing without NVIDIA NIM API. Retrying in 60s...")
        await asyncio.sleep(60)
        asyncio.create_task(process_loop())
        return

    # Load instruments and build dynamic context
    instruments = await get_instruments()
    instrument_ids = {inst["symbol"]: inst["id"] for inst in instruments}
    instruments_by_symbol = {inst["symbol"]: inst for inst in instruments}
    valid_symbols = set(instrument_ids.keys())
    symbol_mapping, valid_symbols_str = build_instrument_context(instruments)
    name_lookup = build_name_lookup(instruments)
    refresh_counter = 0

    logger.info("Loaded %d instruments: %s", len(instruments), ", ".join(sorted(valid_symbols)))

    # Populate ETF constituent data for ETF instruments
    etf_instruments = [i for i in instruments if i["category"] == "etf"]
    if etf_instruments:
        await populate_etf_constituents(etf_instruments)

    # Assign GICS sectors to instruments that don't have one
    await assign_sectors(instruments)

    # Append constituents to valid_symbols_str so the LLM is allowed to tag them
    from .processor import _ETF_CONSTITUENTS
    all_syms = set(valid_symbols)
    for constituents in _ETF_CONSTITUENTS.values():
        all_syms.update(constituents.keys())
    valid_symbols_str = ", ".join(sorted(all_syms))

    while True:
        try:
            refresh_counter += 1
            # Refresh instruments every 10 cycles (~50s) instead of 100.
            # This allows new assets to be categorized and tagged much faster.
            if refresh_counter % 10 == 0:
                instruments = await get_instruments()
                instrument_ids = {inst["symbol"]: inst["id"] for inst in instruments}
                instruments_by_symbol = {inst["symbol"]: inst for inst in instruments}
                valid_symbols = set(instrument_ids.keys())
                symbol_mapping, valid_symbols_str = build_instrument_context(instruments)
                name_lookup = build_name_lookup(instruments)

                # Populate ETF constituent data for new ETFs
                etf_instruments = [i for i in instruments if i["category"] == "etf"]
                if etf_instruments:
                    await populate_etf_constituents(etf_instruments)

                # Assign sectors for any new instruments
                await assign_sectors(instruments)

                # Append constituents again after refresh
                all_syms = set(valid_symbols)
                for constituents in _ETF_CONSTITUENTS.values():
                    all_syms.update(constituents.keys())
                valid_symbols_str = ", ".join(sorted(all_syms))

                logger.info("Refreshed instruments: %s", valid_symbols_str)

            # Fetch batch of unprocessed articles
            queue_depth = await get_unprocessed_queue_depth()
            batch_size = get_adaptive_batch_size(queue_depth)
            articles = await get_unprocessed_articles(limit=batch_size)

            if articles:
                logger.info("Processing %d articles (queue=%d) — 15 concurrent small-batch calls...",
                            len(articles), queue_depth)
                try:
                    await process_batch(
                        articles,
                        instrument_ids, valid_symbols, instruments, instruments_by_symbol,
                        symbol_mapping, valid_symbols_str, name_lookup,
                    )
                except Exception:
                    logger.exception("Error in process_batch")
                    if not await check_health():
                        logger.warning("NIM API not healthy after batch error, backing off 30s...")
                        await asyncio.sleep(30)
                        await wait_for_nim(max_retries=12, delay=10)

                await cleanup_priority()

        except Exception:
            logger.exception("Error in processing loop")

        await asyncio.sleep(PROCESS_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("LLM Processor Service starting...")
    process_task = asyncio.create_task(process_loop())
    macro_task = asyncio.create_task(macro_sentiment_loop())
    sector_task = asyncio.create_task(sector_sentiment_loop())
    yield
    process_task.cancel()
    macro_task.cancel()
    sector_task.cancel()
    for t in (process_task, macro_task, sector_task):
        try:
            await t
        except asyncio.CancelledError:
            pass
    await close_client()


app = FastAPI(title="LLM Processor Service", lifespan=lifespan)


@app.get("/health")
async def health():
    nim_ready = await check_health()
    return {"status": "ok" if nim_ready else "waiting_for_nim", "nim": nim_ready}


class ChatRequest(BaseModel):
    messages: list[dict]
    max_tokens: int = 2000

@app.post("/v1/chat/completions")
async def chat_completions_proxy(req: ChatRequest):
    """Internal proxy to share the global API rate limiter."""
    content = await _call_with_retry(
        messages=req.messages,
        max_tokens=req.max_tokens,
        max_attempts=4,
        response_format=None
    )
    return {"content": content}
