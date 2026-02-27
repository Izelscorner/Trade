"""Prices API endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, LivePriceSchema, HistoricalPriceSchema

router = APIRouter()


@router.get("/live", response_model=APIResponse)
async def live_prices():
    """Get latest live price for each instrument."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (lp.instrument_id)
                    lp.id, lp.instrument_id, i.symbol, i.name,
                    lp.price, lp.change_amount, lp.change_percent,
                    lp.market_status, lp.fetched_at
                FROM live_prices lp
                JOIN instruments i ON i.id = lp.instrument_id
                ORDER BY lp.instrument_id, lp.fetched_at DESC
            """)
        )
        rows = result.fetchall()

    prices = [
        LivePriceSchema(
            id=str(r.id),
            instrument_id=str(r.instrument_id),
            symbol=r.symbol,
            name=r.name,
            price=float(r.price),
            change_amount=float(r.change_amount) if r.change_amount is not None else None,
            change_percent=float(r.change_percent) if r.change_percent is not None else None,
            market_status=r.market_status,
            fetched_at=r.fetched_at,
        )
        for r in rows
    ]
    return APIResponse(data=[p.model_dump() for p in prices], timestamp=datetime.now(timezone.utc))


@router.get("/live/{instrument_id}", response_model=APIResponse)
async def live_price_for_instrument(instrument_id: str):
    """Get latest live price for a specific instrument."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT lp.id, lp.instrument_id, i.symbol, i.name,
                    lp.price, lp.change_amount, lp.change_percent,
                    lp.market_status, lp.fetched_at
                FROM live_prices lp
                JOIN instruments i ON i.id = lp.instrument_id
                WHERE lp.instrument_id = :iid
                ORDER BY lp.fetched_at DESC
                LIMIT 1
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()

    if not row:
        return APIResponse(error="No live price data", timestamp=datetime.now(timezone.utc))

    price = LivePriceSchema(
        id=str(row.id),
        instrument_id=str(row.instrument_id),
        symbol=row.symbol,
        name=row.name,
        price=float(row.price),
        change_amount=float(row.change_amount) if row.change_amount is not None else None,
        change_percent=float(row.change_percent) if row.change_percent is not None else None,
        market_status=row.market_status,
        fetched_at=row.fetched_at,
    )
    return APIResponse(data=price.model_dump(), timestamp=datetime.now(timezone.utc))


@router.get("/historical/{instrument_id}", response_model=APIResponse)
async def historical_prices(instrument_id: str, days: int = 365):
    """Get historical price data for an instrument."""
    async with async_session() as session:
        if days == 1:
            # Query live prices for the last 24 hours (1D chart)
            result = await session.execute(
                text("""
                    SELECT fetched_at as date, price as open, price as high, price as low, price as close, 0 as volume
                    FROM live_prices
                    WHERE instrument_id = :iid
                    AND fetched_at >= NOW() - INTERVAL '1 day'
                    ORDER BY fetched_at ASC
                """),
                {"iid": instrument_id},
            )
            rows = result.fetchall()
            prices = [
                HistoricalPriceSchema(
                    date=r.date.isoformat(),
                    open=float(r.open),
                    high=float(r.high),
                    low=float(r.low),
                    close=float(r.close),
                    volume=int(r.volume),
                )
                for r in rows
            ]
            return APIResponse(data=[p.model_dump() for p in prices], timestamp=datetime.now(timezone.utc))

        else:
            # Standard historical daily data
            result = await session.execute(
                text("""
                    SELECT date, open, high, low, close, volume
                    FROM historical_prices
                    WHERE instrument_id = :iid
                    AND date >= CURRENT_DATE - :days * INTERVAL '1 day'
                    ORDER BY date ASC
                """),
                {"iid": instrument_id, "days": days},
            )
            rows = result.fetchall()

            prices = [
                HistoricalPriceSchema(
                    date=str(r.date),
                    open=float(r.open),
                    high=float(r.high),
                    low=float(r.low),
                    close=float(r.close),
                    volume=int(r.volume),
                )
                for r in rows
            ]
            return APIResponse(data=[p.model_dump() for p in prices], timestamp=datetime.now(timezone.utc))
