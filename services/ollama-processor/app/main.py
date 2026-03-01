"""Ollama Processor Service - classifies and scores news articles using Llama 3.2.

Replaces both the old relevance (DistilBERT) and sentiment (FinBERT) services
with a single Ollama-backed processor.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from sqlalchemy import text

from .db import async_session
from .ollama_client import check_health, close_session
from .processor import (
    get_unprocessed_articles,
    get_instruments,
    process_article,
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
logger = logging.getLogger("ollama-processor")


async def wait_for_ollama(max_retries: int = 60, delay: int = 5) -> bool:
    """Wait for Ollama to be ready with the model loaded."""
    for attempt in range(max_retries):
        if await check_health():
            logger.info("Ollama is ready with model loaded")
            return True
        if attempt % 6 == 0:
            logger.info("Waiting for Ollama to be ready (attempt %d/%d)...", attempt + 1, max_retries)
        await asyncio.sleep(delay)
    logger.error("Ollama did not become ready after %d attempts", max_retries)
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
        await session.commit()
    logger.info("Schema check complete")


async def macro_sentiment_loop() -> None:
    """Independent loop that recalculates global macro sentiment every 30 seconds.

    Decoupled from article processing so grading always has fresh macro data,
    regardless of how fast articles are being processed.
    """
    await asyncio.sleep(10)  # Brief startup delay
    logger.info("Macro sentiment loop started (30s interval)")
    while True:
        try:
            await update_macro_sentiment()
        except Exception:
            logger.exception("Error updating macro sentiment")
        await asyncio.sleep(30)


async def process_loop() -> None:
    """Main processing loop - picks up unprocessed articles and runs them through Ollama."""
    await ensure_schema()

    # Wait for Ollama to be ready
    if not await wait_for_ollama():
        logger.error("Cannot start processing without Ollama. Retrying in 60s...")
        await asyncio.sleep(60)
        asyncio.create_task(process_loop())
        return

    # Load instruments and build dynamic context (refresh periodically)
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
            # Refresh instruments every 50 cycles (~12 minutes) to pick up new assets
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
                logger.info("Processing %d unprocessed articles...", len(articles))
                consecutive_failures = 0
                for article in articles:
                    try:
                        await process_article(
                            article, instrument_ids, valid_symbols,
                            instruments, instruments_by_symbol,
                            symbol_mapping, valid_symbols_str, name_lookup,
                        )
                    except Exception:
                        logger.exception("Failed to process article: %s", article["title"][:60])
                        consecutive_failures += 1
                        # If Ollama is down, stop burning through the batch
                        if consecutive_failures >= 3:
                            logger.warning("Multiple consecutive failures, Ollama may be down. Backing off...")
                            await asyncio.sleep(30)
                            # Re-check health before continuing
                            if not await check_health():
                                logger.warning("Ollama is not healthy, waiting for recovery...")
                                await wait_for_ollama(max_retries=12, delay=10)
                            consecutive_failures = 0
                            break

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
    logger.info("Ollama Processor Service starting...")
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
    await close_session()


app = FastAPI(title="Ollama Processor Service", lifespan=lifespan)


@app.get("/health")
async def health():
    ollama_ready = await check_health()
    return {"status": "ok" if ollama_ready else "waiting_for_ollama", "ollama": ollama_ready}
