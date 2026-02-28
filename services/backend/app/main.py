"""TradeSignal Backend API Service."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.instruments import router as instruments_router
from .api.grades import router as grades_router
from .api.news import router as news_router
from .api.prices import router as prices_router
from .api.technical import router as technical_router
from .api.dashboard import router as dashboard_router
from .api.ai_analysis import router as ai_analysis_router
from .api.portfolio import router as portfolio_router


from .api.ws import (
    router as ws_router, 
    broadcast_live_prices,
    broadcast_latest_news,
    broadcast_latest_grades,
    broadcast_technical_indicators,
    broadcast_macro_sentiment
)
import asyncio

async def _ensure_extra_tables():
    """Create tables that may not exist in older DBs (migration for existing DBs)."""
    from .core.db import async_session as _session
    from sqlalchemy import text as _text
    async with _session() as session:
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS portfolio (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
                added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(instrument_id)
            )
        """))
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS intraday_prices (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                instrument_id UUID NOT NULL REFERENCES instruments(id) ON DELETE CASCADE,
                timestamp TIMESTAMPTZ NOT NULL,
                open NUMERIC(20, 6) NOT NULL,
                high NUMERIC(20, 6) NOT NULL,
                low NUMERIC(20, 6) NOT NULL,
                close NUMERIC(20, 6) NOT NULL,
                volume BIGINT NOT NULL DEFAULT 0,
                UNIQUE (instrument_id, timestamp)
            )
        """))
        await session.execute(_text("""
            CREATE INDEX IF NOT EXISTS idx_intraday_prices_instrument_ts
            ON intraday_prices(instrument_id, timestamp DESC)
        """))
        await session.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _ensure_extra_tables()
    # Start all background tasks
    tasks = [
        asyncio.create_task(broadcast_live_prices()),
        asyncio.create_task(broadcast_latest_news()),
        asyncio.create_task(broadcast_latest_grades()),
        asyncio.create_task(broadcast_technical_indicators()),
        asyncio.create_task(broadcast_macro_sentiment()),
    ]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

app = FastAPI(
    title="TradeSignal API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dashboard_router, prefix="/api/v1/dashboard", tags=["dashboard"])
app.include_router(instruments_router, prefix="/api/v1/instruments", tags=["instruments"])
app.include_router(grades_router, prefix="/api/v1/grades", tags=["grades"])
app.include_router(news_router, prefix="/api/v1/news", tags=["news"])
app.include_router(prices_router, prefix="/api/v1/prices", tags=["prices"])
app.include_router(technical_router, prefix="/api/v1/technical", tags=["technical"])
app.include_router(ai_analysis_router, prefix="/api/v1/ai-analysis", tags=["ai-analysis"])
app.include_router(portfolio_router, prefix="/api/v1/portfolio", tags=["portfolio"])
app.include_router(ws_router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok"}
