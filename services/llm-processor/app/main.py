"""LLM Processor Service - classifies and scores news articles using NVIDIA NIM.

Uses batch API calls: N articles → 1 NIM request → JSON array of results,
dramatically reducing API call frequency.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from sqlalchemy import text

from .db import async_session
from .nim_client import check_health, close_client
from .processor import (
    get_unprocessed_articles,
    get_instruments,
    process_batch,
    update_macro_sentiment,
    cleanup_priority,
    build_name_lookup,
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
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS processing_priority (
                instrument_id UUID PRIMARY KEY REFERENCES instruments(id) ON DELETE CASCADE,
                requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            UPDATE news_articles
            SET macro_sentiment_label = 'neutral'
            WHERE is_macro = true AND ollama_processed = true AND macro_sentiment_label IS NULL
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

    while True:
        try:
            refresh_counter += 1
            if refresh_counter % 50 == 0:
                instruments = await get_instruments()
                instrument_ids = {inst["symbol"]: inst["id"] for inst in instruments}
                instruments_by_symbol = {inst["symbol"]: inst for inst in instruments}
                valid_symbols = set(instrument_ids.keys())
                symbol_mapping, valid_symbols_str = build_instrument_context(instruments)
                name_lookup = build_name_lookup(instruments)
                logger.info("Refreshed instruments: %s", ", ".join(sorted(valid_symbols)))

            articles = await get_unprocessed_articles()

            if articles:
                logger.info("Batch-processing %d articles...", len(articles))
                try:
                    await process_batch(
                        articles,
                        instrument_ids, valid_symbols, instruments, instruments_by_symbol,
                        symbol_mapping, valid_symbols_str, name_lookup,
                    )
                except Exception:
                    logger.exception("Error in process_batch")
                    # Check if API is still healthy
                    if not await check_health():
                        logger.warning("NIM API not healthy after batch error, backing off 30s...")
                        await asyncio.sleep(30)
                        await wait_for_nim(max_retries=12, delay=10)

                await cleanup_priority()

                # If we got a full batch, continue immediately
                if len(articles) >= 20:
                    await asyncio.sleep(1)
                    continue

        except Exception:
            logger.exception("Error in processing loop")

        await asyncio.sleep(PROCESS_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("LLM Processor Service starting...")
    process_task = asyncio.create_task(process_loop())
    macro_task = asyncio.create_task(macro_sentiment_loop())
    yield
    process_task.cancel()
    macro_task.cancel()
    for t in (process_task, macro_task):
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
