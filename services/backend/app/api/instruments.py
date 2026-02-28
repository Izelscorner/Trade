"""Instruments API endpoints."""

import asyncio
import json
import logging
import urllib.request
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, InstrumentSchema, CreateInstrumentsRequest

logger = logging.getLogger(__name__)

router = APIRouter()

QUOTE_TYPE_MAP = {
    "EQUITY": "stock",
    "ETF": "etf",
    "FUTURE": "commodity",
    "CRYPTOCURRENCY": "commodity",
}

YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def _resolve_symbol_yahoo(sym: str) -> dict | None:
    """Resolve symbol name and type via direct Yahoo Finance API (no yfinance library)."""
    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{sym}?range=1d&interval=1d"
    req = urllib.request.Request(url, headers=YAHOO_HEADERS)
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        data = json.loads(resp.read())
    except Exception:
        logger.exception("[%s] Yahoo Finance API request failed", sym)
        return None

    result = data.get("chart", {}).get("result")
    if not result:
        return None

    meta = result[0].get("meta", {})
    short_name = meta.get("shortName") or meta.get("longName") or meta.get("symbol") or sym
    quote_type = meta.get("instrumentType") or meta.get("quoteType", "EQUITY")
    return {"name": short_name, "quote_type": quote_type}


@router.get("", response_model=APIResponse)
async def list_instruments(category: str | None = None):
    """Get all instruments, optionally filtered by category."""
    async with async_session() as session:
        if category:
            result = await session.execute(
                text("SELECT id, symbol, name, category FROM instruments WHERE category = :cat ORDER BY symbol"),
                {"cat": category},
            )
        else:
            result = await session.execute(
                text("SELECT id, symbol, name, category FROM instruments ORDER BY symbol")
            )
        rows = result.fetchall()

    instruments = [
        InstrumentSchema(
            id=str(r.id),
            symbol=r.symbol,
            name=r.name,
            category=r.category,
        )
        for r in rows
    ]
    return APIResponse(data=[i.model_dump() for i in instruments], timestamp=datetime.now(timezone.utc))


@router.post("", response_model=APIResponse)
async def add_instruments(body: CreateInstrumentsRequest):
    """Add new instruments by comma-separated ticker symbols, resolved via Yahoo Finance."""
    raw_symbols = [s.strip() for s in body.symbols.split(",") if s.strip()]
    if not raw_symbols:
        return APIResponse(error="No symbols provided", timestamp=datetime.now(timezone.utc))

    created = []
    skipped = []
    loop = asyncio.get_event_loop()

    for sym in raw_symbols:
        try:
            resolved = await loop.run_in_executor(None, _resolve_symbol_yahoo, sym)
            if resolved:
                name = resolved["name"]
                quote_type = resolved["quote_type"]
            else:
                name = sym
                quote_type = "EQUITY"

            category = QUOTE_TYPE_MAP.get(quote_type, "stock")
            display_symbol = sym.upper()
            yfinance_symbol = sym

            async with async_session() as session:
                result = await session.execute(
                    text("""
                        INSERT INTO instruments (symbol, name, category, yfinance_symbol)
                        VALUES (:symbol, :name, :category, :yf_symbol)
                        ON CONFLICT (symbol) DO NOTHING
                        RETURNING id, symbol, name, category
                    """),
                    {
                        "symbol": display_symbol,
                        "name": name,
                        "category": category,
                        "yf_symbol": yfinance_symbol,
                    },
                )
                row = result.fetchone()
                await session.commit()

                if row:
                    created.append({
                        "id": str(row.id),
                        "symbol": row.symbol,
                        "name": row.name,
                        "category": row.category,
                    })
                    logger.info("Created instrument: %s (%s) [%s]", display_symbol, name, category)
                else:
                    skipped.append(display_symbol)
                    logger.info("Skipped existing instrument: %s", display_symbol)

        except Exception:
            logger.exception("Failed to resolve symbol: %s", sym)
            skipped.append(sym)

    return APIResponse(
        data={"created": created, "skipped": skipped},
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/{instrument_id}", response_model=APIResponse)
async def get_instrument(instrument_id: str):
    """Get a single instrument by ID."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, symbol, name, category FROM instruments WHERE id = :iid"),
            {"iid": instrument_id},
        )
        row = result.fetchone()

    if not row:
        return APIResponse(error="Instrument not found", timestamp=datetime.now(timezone.utc))

    inst = InstrumentSchema(
        id=str(row.id),
        symbol=row.symbol,
        name=row.name,
        category=row.category,
    )
    return APIResponse(data=inst.model_dump(), timestamp=datetime.now(timezone.utc))
