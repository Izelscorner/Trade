"""Database connection for fundamentals-fetcher service."""

import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://tradesignal:change_me_in_production@postgres:5432/tradesignal",
)

engine = create_async_engine(DATABASE_URL, echo=False, pool_size=3, max_overflow=2)
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
