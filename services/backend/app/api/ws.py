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
            
            # Serialize datetime inside model_dump manually or let json handle iso
            msg = json.dumps({
                "type": "live_prices",
                "data": prices,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }, default=str)
            
            await manager.broadcast(msg)

        except Exception:
            logger.exception("Error in broadcast_live_prices")
            
        await asyncio.sleep(5)  # Broadcast every 5 seconds


@router.websocket("/ws/prices")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # wait for messages (optional ping/pong handling)
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
