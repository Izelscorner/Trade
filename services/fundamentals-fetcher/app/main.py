"""Fundamentals Fetcher Service — FMP stock metrics + FRED macro indicators.

Two daily fetch cycles:
1. FMP: P/E, ROE, D/E, PEG for stocks + weighted-average for ETFs via constituents.
2. FRED: DXY, 10Y Treasury, GDP Growth, Brent Crude for macro grading correlation.

Runs once on startup, then sleeps until next 06:00 UTC daily.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta

import httpx
from sqlalchemy import text

from .db import async_session
from .fetcher import fetch_ratios
from .fred_client import fetch_all_macro_indicators, MACRO_LABELS, MACRO_UNITS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("fundamentals-fetcher")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
CALL_DELAY = 2.0  # seconds between API calls
MAX_FMP_CALLS = 120  # safety cap (250/day limit, 2 calls per symbol)
CLEANUP_KEEP = 30  # keep last N records per instrument


async def _ensure_tables():
    """Create tables if they don't exist (migration safety)."""
    async with async_session() as session:
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS fundamental_metrics (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
                pe_ratio NUMERIC(12, 4),
                roe NUMERIC(12, 6),
                de_ratio NUMERIC(12, 4),
                peg_ratio NUMERIC(12, 4),
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_fundamental_metrics_instrument
            ON fundamental_metrics(instrument_id, fetched_at DESC)
        """))
        await session.execute(text("""
            CREATE TABLE IF NOT EXISTS macro_indicators (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                indicator_name VARCHAR(50) NOT NULL,
                value NUMERIC(16, 6) NOT NULL,
                label VARCHAR(100) NOT NULL,
                unit VARCHAR(20) NOT NULL DEFAULT '',
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_macro_indicators_name_fetched
            ON macro_indicators(indicator_name, fetched_at DESC)
        """))
        # Add fundamentals_score to grades if missing
        await session.execute(text("""
            DO $$ BEGIN
                ALTER TABLE grades ADD COLUMN fundamentals_score NUMERIC(7, 4) DEFAULT 0;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$
        """))
        await session.commit()
    logger.info("Tables ensured")


async def get_instruments() -> list[dict]:
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, category, yfinance_symbol FROM instruments WHERE is_active = true")
        )
        return [
            {"id": str(r.id), "symbol": r.symbol, "category": r.category, "yfinance": r.yfinance_symbol}
            for r in result.fetchall()
        ]


async def get_etf_constituents(etf_id: str) -> list[dict]:
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT constituent_symbol, weight_percent
                FROM etf_constituents
                WHERE etf_instrument_id = :eid
                ORDER BY weight_percent DESC
            """),
            {"eid": etf_id},
        )
        return [{"symbol": r.constituent_symbol, "weight": float(r.weight_percent)} for r in result.fetchall()]


async def store_fundamentals(instrument_id: str, metrics: dict) -> None:
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO fundamental_metrics (instrument_id, pe_ratio, roe, de_ratio, peg_ratio)
                VALUES (:iid, :pe, :roe, :de, :peg)
            """),
            {
                "iid": instrument_id,
                "pe": metrics.get("pe_ratio"),
                "roe": metrics.get("roe"),
                "de": metrics.get("de_ratio"),
                "peg": metrics.get("peg_ratio"),
            },
        )
        await session.commit()


async def store_macro_indicator(name: str, value: float, label: str, unit: str) -> None:
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO macro_indicators (indicator_name, value, label, unit)
                VALUES (:name, :value, :label, :unit)
            """),
            {"name": name, "value": value, "label": label, "unit": unit},
        )
        await session.commit()


def _compute_etf_fundamentals(
    constituent_data: list[tuple[dict, float]],
) -> dict | None:
    """Compute weighted-average fundamentals for an ETF from its constituents.

    Each item is (metrics_dict, weight_percent).
    Requires >= 30% of total weight to have data for a metric to be included.
    """
    total_weight = sum(w for _, w in constituent_data)
    if total_weight == 0:
        return None

    result = {}
    for metric_key in ("pe_ratio", "roe", "de_ratio", "peg_ratio"):
        weighted_sum = 0.0
        weight_sum = 0.0
        for metrics, weight in constituent_data:
            val = metrics.get(metric_key)
            if val is not None:
                weighted_sum += val * weight
                weight_sum += weight

        if weight_sum >= total_weight * 0.30:
            result[metric_key] = weighted_sum / weight_sum
        else:
            result[metric_key] = None

    if all(v is None for v in result.values()):
        return None
    return result


async def cleanup_old_records():
    """Keep only the last CLEANUP_KEEP records per instrument."""
    async with async_session() as session:
        await session.execute(text(f"""
            DELETE FROM fundamental_metrics
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY fetched_at DESC) as rn
                    FROM fundamental_metrics
                ) sub WHERE rn <= {CLEANUP_KEEP}
            )
        """))
        await session.execute(text(f"""
            DELETE FROM macro_indicators
            WHERE id NOT IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (PARTITION BY indicator_name ORDER BY fetched_at DESC) as rn
                    FROM macro_indicators
                ) sub WHERE rn <= {CLEANUP_KEEP}
            )
        """))
        await session.commit()


async def fetch_fmp_cycle(instruments: list[dict]) -> None:
    """Fetch FMP fundamentals for stocks, compute weighted averages for ETFs."""
    if not FMP_API_KEY:
        logger.warning("FMP_API_KEY not set — skipping stock fundamentals")
        return

    stocks = [i for i in instruments if i["category"] == "stock"]
    etfs = [i for i in instruments if i["category"] == "etf"]
    call_count = 0

    # Collect all symbols we need (stocks + unique ETF constituents)
    constituent_cache: dict[str, dict | None] = {}

    async with httpx.AsyncClient() as client:
        # Fetch stock fundamentals
        for inst in stocks:
            if call_count >= MAX_FMP_CALLS:
                logger.warning("FMP call cap reached (%d)", MAX_FMP_CALLS)
                break
            metrics = await fetch_ratios(inst["symbol"], FMP_API_KEY, client)
            call_count += 1
            if metrics:
                await store_fundamentals(inst["id"], metrics)
                constituent_cache[inst["symbol"]] = metrics
            await asyncio.sleep(CALL_DELAY)

        # For ETFs: fetch constituents, fetch missing constituent data, compute weighted avg
        for etf in etfs:
            constituents = await get_etf_constituents(etf["id"])
            if not constituents:
                logger.info("[%s] No ETF constituents found", etf["symbol"])
                continue

            constituent_data: list[tuple[dict, float]] = []
            for const in constituents:
                sym = const["symbol"]
                if sym not in constituent_cache:
                    if call_count >= MAX_FMP_CALLS:
                        logger.warning("FMP call cap reached (%d)", MAX_FMP_CALLS)
                        break
                    metrics = await fetch_ratios(sym, FMP_API_KEY, client)
                    call_count += 1
                    constituent_cache[sym] = metrics
                    await asyncio.sleep(CALL_DELAY)

                cached = constituent_cache.get(sym)
                if cached:
                    constituent_data.append((cached, const["weight"]))

            etf_metrics = _compute_etf_fundamentals(constituent_data)
            if etf_metrics:
                await store_fundamentals(etf["id"], etf_metrics)
                logger.info(
                    "[%s] ETF weighted fundamentals: P/E=%.2f ROE=%.4f D/E=%.2f PEG=%.2f",
                    etf["symbol"],
                    etf_metrics.get("pe_ratio") or 0,
                    etf_metrics.get("roe") or 0,
                    etf_metrics.get("de_ratio") or 0,
                    etf_metrics.get("peg_ratio") or 0,
                )

    logger.info("FMP cycle complete: %d API calls", call_count)


async def fetch_fred_cycle() -> None:
    """Fetch FRED macro indicators and store them."""
    if not FRED_API_KEY:
        logger.warning("FRED_API_KEY not set — skipping macro indicators")
        return

    async with httpx.AsyncClient() as client:
        indicators = await fetch_all_macro_indicators(FRED_API_KEY, client)

    stored = 0
    for name, value in indicators.items():
        if value is not None:
            await store_macro_indicator(
                name=name,
                value=value,
                label=MACRO_LABELS[name],
                unit=MACRO_UNITS[name],
            )
            stored += 1

    logger.info("FRED cycle complete: %d/%d indicators stored", stored, len(indicators))


def _seconds_until_next_run(hour: int = 6) -> float:
    """Calculate seconds until next target hour UTC."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def main() -> None:
    logger.info("Fundamentals Fetcher Service starting...")

    # Wait for DB to be ready
    await asyncio.sleep(15)
    await _ensure_tables()

    while True:
        try:
            instruments = await get_instruments()
            logger.info("Starting fetch cycle for %d instruments", len(instruments))

            await fetch_fmp_cycle(instruments)
            await fetch_fred_cycle()
            await cleanup_old_records()

            sleep_secs = _seconds_until_next_run(hour=6)
            logger.info("Cycle complete. Next run in %.0f seconds (%.1f hours)", sleep_secs, sleep_secs / 3600)
            await asyncio.sleep(sleep_secs)

        except Exception:
            logger.exception("Error in fetch cycle")
            await asyncio.sleep(300)  # retry in 5 min on error


if __name__ == "__main__":
    asyncio.run(main())
