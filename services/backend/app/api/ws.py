"""WebSocket server for streaming live prices."""

import asyncio
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import LivePriceSchema

logger = logging.getLogger(__name__)

router = APIRouter()

# Simple connection manager for broadcasting
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()


async def broadcast_live_prices():
    """Background task to broadcast live prices every few seconds."""
    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(2)
                continue

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
                ).model_dump()
                for r in rows
            ]
            
            msg = json.dumps({
                "type": "live_prices",
                "data": prices,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, default=str)
            
            await manager.broadcast(msg)

        except Exception:
            logger.exception("Error in broadcast_live_prices")
            
        await asyncio.sleep(5)


async def broadcast_latest_news():
    """Background task to broadcast latest news updates."""
    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(5)
                continue
            
            async with async_session() as session:
                result = await session.execute(
                    text("""
                        SELECT n.id, n.title, n.link, n.summary, n.source, n.category, n.published_at,
                               s.positive, s.negative, s.neutral, s.label,
                               m.instrument_id
                        FROM news_articles n
                        LEFT JOIN sentiment_scores s ON n.id = s.article_id
                        LEFT JOIN news_instrument_map m ON n.id = m.article_id
                        ORDER BY n.published_at DESC
                        LIMIT 50
                    """)
                )
                rows = result.fetchall()

            news = [
                {
                    "id": str(r.id),
                    "title": r.title,
                    "link": r.link,
                    "summary": r.summary,
                    "source": r.source,
                    "category": r.category,
                    "instrument_id": str(r.instrument_id) if r.instrument_id else None,
                    "published_at": r.published_at.isoformat() if r.published_at else None,
                    "sentiment": {
                        "positive": float(r.positive) if r.positive is not None else 0,
                        "negative": float(r.negative) if r.negative is not None else 0,
                        "neutral": float(r.neutral) if r.neutral is not None else 0,
                        "label": r.label or "neutral"
                    }
                }
                for r in rows
            ]

            msg = json.dumps({
                "type": "news_updates",
                "data": news,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            await manager.broadcast(msg)

        except Exception:
            logger.exception("Error in broadcast_latest_news")
        
        await asyncio.sleep(5)


async def broadcast_latest_grades():
    """Background task to broadcast latest grades. Polls every 5s, only sends on change."""
    _last_grade_ids: set[str] = set()

    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(5)
                continue

            async with async_session() as session:
                result = await session.execute(
                    text("""
                        SELECT g.*, i.symbol, i.name
                        FROM grades g
                        JOIN instruments i ON i.id = g.instrument_id
                        WHERE (g.instrument_id, g.term, g.graded_at) IN (
                            SELECT instrument_id, term, MAX(graded_at)
                            FROM grades
                            GROUP BY instrument_id, term
                        )
                        ORDER BY g.graded_at DESC
                        LIMIT 30
                    """)
                )
                rows = result.fetchall()

            grades = [
                {
                    "id": str(r.id),
                    "instrument_id": str(r.instrument_id),
                    "symbol": r.symbol,
                    "name": r.name,
                    "term": r.term,
                    "overall_grade": r.overall_grade,
                    "overall_score": float(r.overall_score),
                    "technical_score": float(r.technical_score),
                    "sentiment_score": float(r.sentiment_score),
                    "macro_score": float(r.macro_score),
                    "graded_at": r.graded_at.isoformat()
                }
                for r in rows
            ]

            # Only broadcast if grades actually changed
            current_ids = {g["id"] for g in grades}
            if current_ids != _last_grade_ids:
                _last_grade_ids = current_ids
                msg = json.dumps({
                    "type": "grade_updates",
                    "data": grades,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
                await manager.broadcast(msg)

        except Exception:
            logger.exception("Error in broadcast_latest_grades")

        await asyncio.sleep(5)


async def broadcast_technical_indicators():
    """Background task to broadcast latest technical indicators."""
    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(5)
                continue

            async with async_session() as session:
                result = await session.execute(
                    text("""
                        SELECT t.indicator_name, t.value, t.signal, t.calculated_at, i.symbol, i.id as instrument_id
                        FROM technical_indicators t
                        JOIN instruments i ON i.id = t.instrument_id
                        ORDER BY t.calculated_at DESC
                        LIMIT 20
                    """)
                )
                rows = result.fetchall()

            indicators = [
                {
                    "instrument_id": str(r.instrument_id),
                    "symbol": r.symbol,
                    "indicator_name": r.indicator_name,
                    "value": json.loads(r.value) if isinstance(r.value, str) else r.value,
                    "signal": r.signal,
                    "calculated_at": r.calculated_at.isoformat()
                }
                for r in rows
            ]

            msg = json.dumps({
                "type": "technical_updates",
                "data": indicators,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            await manager.broadcast(msg)

        except Exception:
            logger.exception("Error in broadcast_technical_indicators")
        
        await asyncio.sleep(20)


async def broadcast_macro_sentiment():
    """Background task to broadcast latest macro sentiment."""
    while True:
        try:
            if not manager.active_connections:
                await asyncio.sleep(10)
                continue

            async with async_session() as session:
                result = await session.execute(
                    text("""
                        SELECT DISTINCT ON (region) *
                        FROM macro_sentiment
                        ORDER BY region, calculated_at DESC
                    """)
                )
                rows = result.fetchall()

            sentiment = [
                {
                    "region": r.region,
                    "score": float(r.score),
                    "label": r.label,
                    "article_count": r.article_count,
                    "calculated_at": r.calculated_at.isoformat()
                }
                for r in rows
            ]

            msg = json.dumps({
                "type": "macro_sentiment_updates",
                "data": sentiment,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            await manager.broadcast(msg)

        except Exception:
            logger.exception("Error in broadcast_macro_sentiment")
        
        await asyncio.sleep(20)


@router.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    """Unified WebSocket endpoint for prices, news, grades, and technicals."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
