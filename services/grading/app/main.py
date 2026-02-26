"""Grading Service - combines all signals into investment grades."""

import asyncio
import logging

from sqlalchemy import text

from .db import async_session
from .scorer import grade_instrument, store_grade

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("grading")

GRADING_INTERVAL = 1800  # 30 minutes


async def get_instruments() -> list[dict]:
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category FROM instruments ORDER BY symbol")
        )
        return [{"id": str(r.id), "symbol": r.symbol, "name": r.name, "category": r.category} for r in result.fetchall()]


async def grading_loop() -> None:
    """Main grading loop - grades all instruments for both short and long term."""
    # Wait for other services to populate data first
    await asyncio.sleep(120)

    while True:
        try:
            instruments = await get_instruments()
            for inst in instruments:
                for term in ("short", "long"):
                    try:
                        grade = await grade_instrument(inst["id"], inst["symbol"], term)
                        if grade:
                            await store_grade(grade)
                            logger.info(
                                "[%s] %s-term grade: %s (score=%.4f, tech=%.4f, sent=%.4f, macro=%.4f)",
                                inst["symbol"], term, grade["overall_grade"],
                                grade["overall_score"], grade["technical_score"],
                                grade["sentiment_score"], grade["macro_score"],
                            )
                    except Exception:
                        logger.exception("[%s] Failed to grade (%s-term)", inst["symbol"], term)
        except Exception:
            logger.exception("Error in grading loop")

        await asyncio.sleep(GRADING_INTERVAL)


async def main() -> None:
    logger.info("Grading Service starting...")
    await grading_loop()


if __name__ == "__main__":
    asyncio.run(main())
