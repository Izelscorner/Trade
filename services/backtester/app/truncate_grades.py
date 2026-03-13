import asyncio
from sqlalchemy import text
from app.db import engine

async def main():
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE backtest_grades;"))
    print("✓ Truncated backtest_grades")

if __name__ == "__main__":
    asyncio.run(main())
