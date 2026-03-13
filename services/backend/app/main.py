"""TradeSignal Backend API Service."""

import os
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
from .api.fundamentals import router as fundamentals_router
from .api.settings import router as settings_router


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
        # Sector infrastructure migrations
        await session.execute(_text("""
            DO $$ BEGIN
                ALTER TABLE instruments ADD COLUMN sector VARCHAR(50);
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$
        """))
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS sector_sentiment (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                sector VARCHAR(50) NOT NULL,
                term VARCHAR(10) NOT NULL DEFAULT 'short' CHECK (term IN ('short', 'long')),
                score NUMERIC(7, 6) NOT NULL,
                label VARCHAR(10) NOT NULL CHECK (label IN ('positive', 'negative', 'neutral')),
                article_count INT NOT NULL DEFAULT 0,
                calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(_text("""
            CREATE INDEX IF NOT EXISTS idx_sector_sentiment_sector_term
            ON sector_sentiment(sector, term, calculated_at DESC)
        """))
        await session.execute(_text("""
            DO $$ BEGIN
                ALTER TABLE grades ADD COLUMN sector_score NUMERIC(7, 4) DEFAULT 0;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$
        """))
        await session.execute(_text("""
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
        await session.execute(_text("""
            CREATE INDEX IF NOT EXISTS idx_fundamental_metrics_instrument
            ON fundamental_metrics(instrument_id, fetched_at DESC)
        """))
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS macro_indicators (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                indicator_name VARCHAR(50) NOT NULL,
                value NUMERIC(16, 6) NOT NULL,
                label VARCHAR(100) NOT NULL,
                unit VARCHAR(20) NOT NULL DEFAULT '',
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(_text("""
            CREATE INDEX IF NOT EXISTS idx_macro_indicators_name_fetched
            ON macro_indicators(indicator_name, fetched_at DESC)
        """))
        await session.execute(_text("""
            DO $$ BEGIN
                ALTER TABLE grades ADD COLUMN fundamentals_score NUMERIC(7, 4) DEFAULT 0;
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$
        """))
        await session.execute(_text("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key VARCHAR(50) PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        await session.execute(_text("""
            DO $$ BEGIN
                ALTER TABLE grades ADD COLUMN IF NOT EXISTS pure_score NUMERIC(7, 4);
                ALTER TABLE grades ADD COLUMN IF NOT EXISTS pure_grade VARCHAR(20);
            EXCEPTION WHEN duplicate_column THEN NULL;
            END $$
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
app.include_router(fundamentals_router, prefix="/api/v1/fundamentals", tags=["fundamentals"])
app.include_router(settings_router, prefix="/api/v1/settings", tags=["settings"])
app.include_router(ws_router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/v1/config")
async def get_config():
    return {"data": {"nim_model": os.getenv("NIM_MODEL", "qwen/qwen3.5-122b-a10b")}}
