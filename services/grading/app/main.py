"""Grading Service - combines all signals into investment grades.

Polls for data changes (new sentiment scores, price updates, technical indicators)
and retriggers grading immediately when new data arrives.
"""

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import text

from .db import async_session
from .scorer import grade_instrument, store_grade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("grading")

POLL_INTERVAL = 10  # Check for changes every 10 seconds
FULL_REGRADE_INTERVAL = 60  # Full regrade every 60 seconds per spec


async def get_instruments() -> list[dict]:
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category FROM instruments ORDER BY symbol")
        )
        return [{"id": str(r.id), "symbol": r.symbol, "name": r.name, "category": r.category} for r in result.fetchall()]


async def detect_changes(since: datetime) -> dict:
    """Check if any grading inputs have changed since the given timestamp.

    Returns dict with sets of instrument IDs that need regrading and
    whether macro data changed (affects all instruments).
    """
    changed_instruments: set[str] = set()
    macro_changed = False

    async with async_session() as session:
        # Check for new sentiment scores since last grade
        result = await session.execute(
            text("""
                SELECT DISTINCT COALESCE(m.instrument_id::text, '__macro__') as iid
                FROM sentiment_scores s
                LEFT JOIN news_instrument_map m ON m.article_id = s.article_id
                WHERE s.analyzed_at >= :since
            """),
            {"since": since},
        )
        for row in result.fetchall():
            if row.iid == "__macro__":
                macro_changed = True
            else:
                changed_instruments.add(row.iid)

        # Check for new live prices since last grade
        result = await session.execute(
            text("""
                SELECT DISTINCT instrument_id::text as iid
                FROM live_prices
                WHERE fetched_at >= :since
            """),
            {"since": since},
        )
        for row in result.fetchall():
            changed_instruments.add(row.iid)

        # Check for new technical indicators since last grade
        result = await session.execute(
            text("""
                SELECT DISTINCT instrument_id::text as iid
                FROM technical_indicators
                WHERE calculated_at >= :since
            """),
            {"since": since},
        )
        for row in result.fetchall():
            changed_instruments.add(row.iid)

        # Check for new macro sentiment
        result = await session.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM macro_sentiment
                WHERE calculated_at >= :since
            """),
            {"since": since},
        )
        row = result.fetchone()
        if row and row.cnt > 0:
            macro_changed = True

    return {"instruments": changed_instruments, "macro_changed": macro_changed}


async def get_priority_instrument_id() -> str | None:
    """Get the currently prioritized instrument ID from the processing_priority table."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT instrument_id::text FROM processing_priority ORDER BY requested_at DESC LIMIT 1")
        )
        row = result.fetchone()
        return row.instrument_id if row else None


async def grading_loop() -> None:
    """Main grading loop - detects changes and regrades affected instruments."""
    # Wait for other services to populate initial data
    await asyncio.sleep(30)

    instruments = await get_instruments()
    last_check = datetime.now(timezone.utc)
    last_full_regrade = datetime.min.replace(tzinfo=timezone.utc)

    # Initial full grading pass
    logger.info("Running initial full grading pass...")
    for inst in instruments:
        for term in ("short", "long"):
            try:
                grade = await grade_instrument(inst["id"], inst["symbol"], term, inst["category"])
                if grade:
                    await store_grade(grade)
                    logger.info(
                        "[%s] %s-term grade: %s (score=%.4f)",
                        inst["symbol"], term, grade["overall_grade"], grade["overall_score"],
                    )
            except Exception:
                logger.exception("[%s] Failed to grade (%s-term)", inst["symbol"], term)
    last_full_regrade = datetime.now(timezone.utc)
    logger.info("Initial grading complete")

    while True:
        await asyncio.sleep(POLL_INTERVAL)

        try:
            now = datetime.now(timezone.utc)
            time_since_full = (now - last_full_regrade).total_seconds()

            # Periodic full regrade
            if time_since_full >= FULL_REGRADE_INTERVAL:
                instruments = await get_instruments()
                for inst in instruments:
                    for term in ("short", "long"):
                        try:
                            grade = await grade_instrument(inst["id"], inst["symbol"], term, inst["category"])
                            if grade:
                                await store_grade(grade)
                        except Exception:
                            logger.exception("[%s] Failed to grade (%s-term)", inst["symbol"], term)
                last_full_regrade = now
                last_check = now
                logger.info("Full regrade complete")
                continue

            # Check for incremental changes
            changes = await detect_changes(last_check)
            last_check = now

            if not changes["instruments"] and not changes["macro_changed"]:
                continue

            # Determine which instruments to regrade
            instruments_to_grade = []
            if changes["macro_changed"]:
                # Macro affects all instruments
                instruments = await get_instruments()
                instruments_to_grade = instruments
                logger.info("Macro data changed — regrading all instruments")
            else:
                # Only regrade instruments with new data
                instruments = await get_instruments()
                instruments_to_grade = [i for i in instruments if i["id"] in changes["instruments"]]

            # Check for prioritized instrument and ensure it's graded first
            priority_id = await get_priority_instrument_id()
            if priority_id:
                instruments_to_grade.sort(key=lambda i: 0 if i["id"] == priority_id else 1)

            if not instruments_to_grade:
                continue

            for inst in instruments_to_grade:
                for term in ("short", "long"):
                    try:
                        grade = await grade_instrument(inst["id"], inst["symbol"], term, inst["category"])
                        if grade:
                            await store_grade(grade)
                            logger.info(
                                "[%s] %s-term regrade: %s (score=%.4f, tech=%.4f, sent=%.4f, macro=%.4f)",
                                inst["symbol"], term, grade["overall_grade"],
                                grade["overall_score"], grade["technical_score"],
                                grade["sentiment_score"], grade["macro_score"],
                            )
                    except Exception:
                        logger.exception("[%s] Failed to regrade (%s-term)", inst["symbol"], term)

        except Exception:
            logger.exception("Error in grading loop")


async def main() -> None:
    logger.info("Grading Service starting...")
    await grading_loop()


if __name__ == "__main__":
    asyncio.run(main())
