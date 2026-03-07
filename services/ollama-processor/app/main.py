"""Cerebras Processor Service - classifies and scores news articles using Cerebras API.

Replaces the old Ollama-based processor with a remote Cerebras API call
for much faster throughput and better model quality.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from sqlalchemy import text

from .db import async_session
from .cerebras_client import check_health, close_client
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


async def wait_for_cerebras(max_retries: int = 30, delay: int = 5) -> bool:
    """Wait for Cerebras API to be reachable."""
    for attempt in range(max_retries):
        if await check_health():
            logger.info("Cerebras API is reachable")
            return True
        if attempt % 3 == 0:
            logger.info("Waiting for Cerebras API (attempt %d/%d)...", attempt + 1, max_retries)
        await asyncio.sleep(delay)
    logger.error("Cerebras API not reachable after %d attempts", max_retries)
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
    """Main processing loop - picks up unprocessed articles and runs them through Cerebras."""
    await ensure_schema()

    if not await wait_for_cerebras():
        logger.error("Cannot start processing without Cerebras API. Retrying in 60s...")
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

    # Limit parallel Cerebras requests to avoid rate limiting (free tier)
    sem = asyncio.Semaphore(2)

    async def process_with_semaphore(article: dict) -> None:
        async with sem:
            await process_article(
                article, instrument_ids, valid_symbols,
                instruments, instruments_by_symbol,
                symbol_mapping, valid_symbols_str, name_lookup,
            )

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
                logger.info("Processing %d unprocessed articles...", len(articles))

                results = await asyncio.gather(
                    *[process_with_semaphore(a) for a in articles],
                    return_exceptions=True,
                )

                failures = [r for r in results if isinstance(r, Exception)]
                for exc in failures:
                    logger.exception("Failed to process article: %s", exc)

                if len(failures) >= 3:
                    logger.warning("Multiple failures in batch, API may be down. Backing off...")
                    await asyncio.sleep(30)
                    if not await check_health():
                        logger.warning("Cerebras API not healthy, waiting for recovery...")
                        await wait_for_cerebras(max_retries=12, delay=10)

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
    logger.info("Cerebras Processor Service starting...")
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


app = FastAPI(title="Cerebras Processor Service", lifespan=lifespan)


@app.get("/health")
async def health():
    cerebras_ready = await check_health()
    return {"status": "ok" if cerebras_ready else "waiting_for_cerebras", "cerebras": cerebras_ready}
