import os
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

DATABASE_URL = os.environ["DATABASE_URL"]

engine = create_async_engine(DATABASE_URL, pool_size=5, max_overflow=5)
async_session = async_sessionmaker(engine, expire_on_commit=False)
