#!/bin/bash
# Run historical sentiment fetch from host (bypasses Docker Google News 503 block)
# Usage: ./run_fetch_sentiment.sh [START_DATE] [END_DATE]
#   Defaults: 2020-01-01 to 2026-03-01

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV="$SCRIPT_DIR/.venv"

# Load .env
set -a
source "$PROJECT_DIR/.env"
set +a

# Replace Docker hostname with localhost for host access
export DATABASE_URL="${DATABASE_URL/@postgres:/@localhost:}"
export DYLD_LIBRARY_PATH="/Users/main/homebrew/opt/openssl@3/lib"

# Force NVIDIA API (not local backend which .env may set)
export NIM_BASE_URL="https://integrate.api.nvidia.com/v1"

# Date range from args or defaults
export BACKTEST_START="${1:-2020-01-01}"
export BACKTEST_END="${2:-2026-03-01}"

echo "=== Historical Sentiment Fetch (Host Mode) ==="
echo "Period: $BACKTEST_START → $BACKTEST_END"
echo "DB: localhost:5432"
echo ""

cd "$SCRIPT_DIR"
exec "$VENV/bin/python" -c "
import asyncio, sys, os, logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s: %(message)s')
logging.getLogger('httpx').setLevel(logging.WARNING)

from datetime import date
from app.historical_sentiment import fetch_all_historical_sentiment, ASSET_QUERIES
from sqlalchemy import text
from app.db import async_session

async def main():
    start = date.fromisoformat(os.environ['BACKTEST_START'])
    end = date.fromisoformat(os.environ['BACKTEST_END'])

    # Load instruments from DB
    async with async_session() as s:
        r = await s.execute(text('SELECT symbol, name, category FROM instruments ORDER BY id'))
        instruments = [{'symbol': row.symbol, 'name': row.name, 'category': row.category} for row in r.fetchall()]

    # Filter to instruments that have queries defined
    instruments = [i for i in instruments if i['symbol'] in ASSET_QUERIES]
    print(f'Fetching sentiment for {len(instruments)} instruments: {[i[\"symbol\"] for i in instruments]}')

    await fetch_all_historical_sentiment(instruments, start, end)

asyncio.run(main())
"
