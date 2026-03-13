"""Settings API endpoints."""

from datetime import datetime, timezone
import logging

from fastapi import APIRouter
from sqlalchemy import text
from pydantic import BaseModel

from ..core.db import async_session
from ..schemas import APIResponse

logger = logging.getLogger(__name__)

router = APIRouter()

class SettingUpdate(BaseModel):
    value: bool

@router.get("", response_model=APIResponse)
async def get_settings():
    """Get all global system settings."""
    async with async_session() as session:
        result = await session.execute(
            text("SELECT key, value FROM system_settings")
        )
        rows = result.fetchall()
    
    settings = {row.key: row.value for row in rows}
    # Ensure default if table/row missing
    if "sentiment_enabled" not in settings:
        settings["sentiment_enabled"] = True
        
    return APIResponse(data=settings, timestamp=datetime.now(timezone.utc))

@router.post("/sentiment", response_model=APIResponse)
async def toggle_sentiment(body: SettingUpdate):
    """Enable or disable sentiment globally."""
    async with async_session() as session:
        await session.execute(
            text("""
                INSERT INTO system_settings (key, value)
                VALUES ('sentiment_enabled', :val)
                ON CONFLICT (key) DO UPDATE SET value = :val, updated_at = NOW()
            """),
            {"val": body.value}
        )
        await session.commit()
    
    logger.info("Global sentiment toggled: %s", body.value)
    return APIResponse(data={"sentiment_enabled": body.value}, timestamp=datetime.now(timezone.utc))
