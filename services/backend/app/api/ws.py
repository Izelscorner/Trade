"""WebSocket server with subscription-based filtering.

Clients send JSON subscription messages to declare what they need:
    { "subscribe": { "page": "dashboard" } }
    { "subscribe": { "page": "asset_detail", "instrument_ids": ["uuid-here"] } }
    { "subscribe": { "page": "asset_list" } }
    { "subscribe": { "page": "news", "region": "us", "category": "politics" } }

The server only sends data relevant to each client's subscription.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from dataclasses import dataclass, field

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import LivePriceSchema

logger = logging.getLogger(__name__)

router = APIRouter()


@dataclass
class ClientSubscription:
    """Tracks what a single client is subscribed to."""
    page: str = "dashboard"  # dashboard | asset_detail | asset_list | news
    instrument_ids: list[str] = field(default_factory=list)  # for asset_detail
    region: str | None = None  # for news filtering
    category: str | None = None  # for news filtering


class ConnectionManager:
    def __init__(self):
        self.connections: dict[WebSocket, ClientSubscription] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections[websocket] = ClientSubscription()

    def disconnect(self, websocket: WebSocket):
        self.connections.pop(websocket, None)

    def update_subscription(self, websocket: WebSocket, sub: dict):
        """Update a client's subscription based on their message."""
        if websocket not in self.connections:
            return
        cs = self.connections[websocket]
        cs.page = sub.get("page", "dashboard")
        cs.instrument_ids = sub.get("instrument_ids", [])
        cs.region = sub.get("region")
        cs.category = sub.get("category")
        logger.debug("Client subscription updated: page=%s, instruments=%s", cs.page, cs.instrument_ids)

    def get_clients_for(self, data_type: str) -> list[tuple[WebSocket, ClientSubscription]]:
        """Return clients that need a specific data type based on their page."""
        results = []
        for ws, sub in self.connections.items():
            if data_type == "live_prices":
                # All pages except news need prices
                if sub.page != "news":
                    results.append((ws, sub))
            elif data_type == "news_updates":
                # Dashboard, news page, and asset_detail need news
                if sub.page in ("dashboard", "news", "asset_detail"):
                    results.append((ws, sub))
            elif data_type == "grade_updates":
                # Dashboard, asset_detail, and asset_list need grades
                if sub.page in ("dashboard", "asset_detail", "asset_list"):
                    results.append((ws, sub))
            elif data_type == "technical_updates":
                # Only asset_detail needs technical indicators
                if sub.page == "asset_detail":
                    results.append((ws, sub))
            elif data_type == "macro_sentiment_updates":
                # Dashboard and news page need macro sentiment
                if sub.page in ("dashboard", "news"):
                    results.append((ws, sub))
        return results

    async def send_to(self, websocket: WebSocket, message: str):
        """Send a message to a single client, disconnecting on failure."""
        try:
            await websocket.send_text(message)
        except Exception:
            self.disconnect(websocket)

    @property
    def has_connections(self) -> bool:
        return len(self.connections) > 0


manager = ConnectionManager()


async def broadcast_live_prices():
    """Broadcast live prices — filtered per client subscription."""
    while True:
        try:
            if not manager.has_connections:
                await asyncio.sleep(2)
                continue

            clients = manager.get_clients_for("live_prices")
            if not clients:
                await asyncio.sleep(5)
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

            all_prices = [
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

            ts = datetime.now(timezone.utc).isoformat()

            for ws, sub in clients:
                if sub.page == "asset_detail" and sub.instrument_ids:
                    # Only send prices for subscribed instruments
                    filtered = [p for p in all_prices if p["instrument_id"] in sub.instrument_ids]
                else:
                    # Dashboard / asset_list get all prices
                    filtered = all_prices

                if filtered:
                    msg = json.dumps({
                        "type": "live_prices",
                        "data": filtered,
                        "timestamp": ts,
                    }, default=str)
                    await manager.send_to(ws, msg)

        except Exception:
            logger.exception("Error in broadcast_live_prices")

        await asyncio.sleep(5)


async def broadcast_latest_news():
    """Broadcast news — filtered per client subscription."""
    while True:
        try:
            if not manager.has_connections:
                await asyncio.sleep(5)
                continue

            clients = manager.get_clients_for("news_updates")
            if not clients:
                await asyncio.sleep(5)
                continue

            async with async_session() as session:
                result = await session.execute(
                    text("""
                        SELECT DISTINCT ON (n.id)
                            n.id, n.title, n.link, n.summary, n.source, n.category, n.published_at,
                            s.positive, s.negative, s.neutral, s.label,
                            m.instrument_id
                        FROM news_articles n
                        LEFT JOIN sentiment_scores s ON n.id = s.article_id
                        LEFT JOIN news_instrument_map m ON n.id = m.article_id
                        ORDER BY n.id, n.published_at DESC
                        LIMIT 100
                    """)
                )
                rows = result.fetchall()

            all_news = [
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

            # Sort by published_at descending
            all_news.sort(key=lambda a: a["published_at"] or "", reverse=True)

            ts = datetime.now(timezone.utc).isoformat()
            macro_categories = {"us_politics", "uk_politics", "us_finance", "uk_finance"}

            for ws, sub in clients:
                if sub.page == "asset_detail" and sub.instrument_ids:
                    # Only news mapped to this instrument
                    filtered = [n for n in all_news if n["instrument_id"] in sub.instrument_ids][:30]
                elif sub.page == "news":
                    # Filter by region/category if set
                    filtered = all_news
                    if sub.region:
                        filtered = [n for n in filtered if n["category"].startswith(f"{sub.region}_")]
                    if sub.category:
                        if sub.category == "macro":
                            filtered = [n for n in filtered if n["category"] in macro_categories]
                        else:
                            filtered = [n for n in filtered if n["category"].endswith(f"_{sub.category}")]
                    filtered = filtered[:100]
                elif sub.page == "dashboard":
                    filtered = all_news[:50]
                else:
                    filtered = all_news[:50]

                if filtered:
                    msg = json.dumps({
                        "type": "news_updates",
                        "data": filtered,
                        "timestamp": ts,
                    })
                    await manager.send_to(ws, msg)

        except Exception:
            logger.exception("Error in broadcast_latest_news")

        await asyncio.sleep(5)


async def broadcast_latest_grades():
    """Broadcast grades — filtered per client subscription."""
    _last_grade_ids: set[str] = set()

    while True:
        try:
            if not manager.has_connections:
                await asyncio.sleep(5)
                continue

            clients = manager.get_clients_for("grade_updates")
            if not clients:
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

            all_grades = [
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
            current_ids = {g["id"] for g in all_grades}
            if current_ids == _last_grade_ids:
                await asyncio.sleep(5)
                continue
            _last_grade_ids = current_ids

            ts = datetime.now(timezone.utc).isoformat()

            for ws, sub in clients:
                if sub.page == "asset_detail" and sub.instrument_ids:
                    filtered = [g for g in all_grades if g["instrument_id"] in sub.instrument_ids]
                else:
                    filtered = all_grades

                if filtered:
                    msg = json.dumps({
                        "type": "grade_updates",
                        "data": filtered,
                        "timestamp": ts,
                    })
                    await manager.send_to(ws, msg)

        except Exception:
            logger.exception("Error in broadcast_latest_grades")

        await asyncio.sleep(5)


async def broadcast_technical_indicators():
    """Broadcast technical indicators — only to asset_detail clients."""
    while True:
        try:
            if not manager.has_connections:
                await asyncio.sleep(5)
                continue

            clients = manager.get_clients_for("technical_updates")
            if not clients:
                await asyncio.sleep(20)
                continue

            # Collect all instrument_ids that any client needs
            needed_ids = set()
            for _, sub in clients:
                needed_ids.update(sub.instrument_ids)

            if not needed_ids:
                await asyncio.sleep(20)
                continue

            async with async_session() as session:
                # Only fetch technicals for instruments someone is viewing
                placeholders = ", ".join(f":id{i}" for i in range(len(needed_ids)))
                params = {f"id{i}": iid for i, iid in enumerate(needed_ids)}
                result = await session.execute(
                    text(f"""
                        SELECT DISTINCT ON (t.instrument_id, t.indicator_name)
                            t.indicator_name, t.value, t.signal, t.calculated_at,
                            i.symbol, i.id as instrument_id
                        FROM technical_indicators t
                        JOIN instruments i ON i.id = t.instrument_id
                        WHERE t.instrument_id IN ({placeholders})
                        ORDER BY t.instrument_id, t.indicator_name, t.calculated_at DESC
                    """),
                    params,
                )
                rows = result.fetchall()

            all_indicators = [
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

            ts = datetime.now(timezone.utc).isoformat()

            for ws, sub in clients:
                filtered = [ind for ind in all_indicators if ind["instrument_id"] in sub.instrument_ids]
                if filtered:
                    msg = json.dumps({
                        "type": "technical_updates",
                        "data": filtered,
                        "timestamp": ts,
                    })
                    await manager.send_to(ws, msg)

        except Exception:
            logger.exception("Error in broadcast_technical_indicators")

        await asyncio.sleep(20)


async def broadcast_macro_sentiment():
    """Broadcast macro sentiment — only to dashboard and news clients."""
    while True:
        try:
            if not manager.has_connections:
                await asyncio.sleep(10)
                continue

            clients = manager.get_clients_for("macro_sentiment_updates")
            if not clients:
                await asyncio.sleep(20)
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

            for ws, _ in clients:
                await manager.send_to(ws, msg)

        except Exception:
            logger.exception("Error in broadcast_macro_sentiment")

        await asyncio.sleep(20)


@router.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    """Unified WebSocket endpoint with subscription support.

    Clients send: { "subscribe": { "page": "...", "instrument_ids": [...] } }
    to declare what data they need. Defaults to dashboard subscription.
    """
    await manager.connect(websocket)
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
                if "subscribe" in msg:
                    manager.update_subscription(websocket, msg["subscribe"])
            except (json.JSONDecodeError, KeyError):
                pass  # Ignore malformed messages
    except WebSocketDisconnect:
        manager.disconnect(websocket)
