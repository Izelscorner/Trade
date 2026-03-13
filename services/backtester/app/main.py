"""Backtester CLI runner.

Commands:
  fetch-sentiment   — Pre-fetch historical news sentiment (Google News + NIM LLM)
  status            — Show cache and backtest progress
  backtest          — Run retroactive grade simulation over historical date grid
  calibrate         — Optimize composite signal weights using backtest results
  patch             — Apply optimized weights to scorer.py (requires SCORER_PY_PATH mount)
  run-all           — Full pipeline: fetch-sentiment → backtest → calibrate → patch

Usage examples (via docker compose):
  docker compose run --rm backtester python -m app.main fetch-sentiment
  docker compose run --rm backtester python -m app.main status
  docker compose run --rm backtester python -m app.main backtest --term short
  docker compose run --rm backtester python -m app.main backtest --term long --no-sentiment
  docker compose run --rm backtester python -m app.main calibrate --term short
  docker compose run --rm backtester python -m app.main patch --dry-run
  docker compose run --rm backtester python -m app.main run-all

Sentiment source: Google News RSS + NIM/Qwen 122B (same model as production).
Covers all 15 instruments + macro + 11 GICS sectors. No API limits. Resumes from cache.
"""

import argparse
import asyncio
import logging
import sys
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtester")


async def cmd_fetch_sentiment(args) -> None:
    from .backtest_engine import load_instruments
    from .config import BACKTEST_END, BACKTEST_START
    from .historical_sentiment import fetch_all_historical_sentiment

    instruments = await load_instruments()
    start = date.fromisoformat(BACKTEST_START)
    end = date.fromisoformat(BACKTEST_END)

    logger.info(
        "Fetching historical sentiment for %d instruments (%s → %s)",
        len(instruments), start, end,
    )
    await fetch_all_historical_sentiment(instruments, start, end, concurrency=3)
    logger.info("fetch-sentiment complete.")


async def cmd_status(args) -> None:
    from .backtest_engine import load_instruments
    from .config import BACKTEST_END, BACKTEST_START
    from .db import async_session
    from .historical_sentiment import SECTOR_QUERIES, _all_weekdays
    from sqlalchemy import text

    instruments = await load_instruments()
    start = date.fromisoformat(BACKTEST_START)
    end = date.fromisoformat(BACKTEST_END)
    trading_days = _all_weekdays(start, end)
    total_days = len(trading_days)

    n_assets = len(instruments)
    n_sectors = len(SECTOR_QUERIES)
    # asset + macro(1) + sector
    total_needed = total_days * (n_assets + 1 + n_sectors)

    async with async_session() as session:
        cached_r = await session.execute(
            text("SELECT type, COUNT(*) as n FROM backtest_sentiment_cache GROUP BY type")
        )
        cached_by_type = {r.type: r.n for r in cached_r.fetchall()}

    cached_asset  = cached_by_type.get("asset",  0)
    cached_macro  = cached_by_type.get("macro",  0)
    cached_sector = cached_by_type.get("sector", 0)
    cached_total  = cached_asset + cached_macro + cached_sector
    remaining     = max(0, total_needed - cached_total)

    print(f"\n{'=' * 60}")
    print(f"  Sentiment Cache Status (Google News + NIM LLM)")
    print(f"{'=' * 60}")
    print(f"  Trading days in range:  {total_days} ({start} → {end})")
    print(f"  Instruments:            {n_assets}")
    print(f"  Sectors:                {n_sectors}")
    print(f"  Total items needed:     {total_needed}")
    print(f"  Cached — asset:         {cached_asset} / {total_days * n_assets}")
    print(f"  Cached — macro:         {cached_macro} / {total_days}")
    print(f"  Cached — sector:        {cached_sector} / {total_days * n_sectors}")
    print(f"  Total cached:           {cached_total}")
    print(f"  Remaining:              {remaining}")
    print(f"{'=' * 60}\n")

    async with async_session() as session:
        bg = await session.execute(text("SELECT COUNT(*) FROM backtest_grades"))
        br = await session.execute(text("SELECT COUNT(*) FROM backtest_returns"))
        cr = await session.execute(text("SELECT COUNT(*) FROM calibration_runs"))

    print(f"  backtest_grades rows:   {bg.scalar() or 0}")
    print(f"  backtest_returns rows:  {br.scalar() or 0}")
    print(f"  calibration_runs rows:  {cr.scalar() or 0}\n")

    # Signal quality preview if grades exist
    async with async_session() as session:
        qr = await session.execute(text("""
            SELECT width_bucket(bg.overall_score::float, -3, 3, 5) as q,
                   COUNT(*) as n,
                   ROUND(AVG(br.return_20d)::numeric * 100, 2) as avg_20d
            FROM backtest_grades bg
            JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
            WHERE bg.term = 'short' AND br.return_20d IS NOT NULL
            GROUP BY q ORDER BY q
        """))
        quintiles = qr.fetchall()

    if quintiles:
        print(f"  Short-term grade quintile → avg 20-day return:")
        for row in quintiles:
            bar = "▓" * max(0, int((row.avg_20d or 0) * 10 + 10))
            print(f"    Q{row.q}: {row.n:4d} grades, {row.avg_20d:+.2f}%  {bar}")
        print()


async def cmd_backtest(args) -> None:
    from .backtest_engine import run_backtest

    logger.info(
        "Starting backtest (term=%s, fetch_sentiment=%s)...",
        args.term, not args.no_sentiment,
    )
    results = await run_backtest(
        term=args.term,
        fetch_sentiment=not args.no_sentiment,
        skip_existing=not args.force,
    )
    logger.info("Backtest done. %d grade rows produced.", len(results))


async def cmd_calibrate(args) -> None:
    from .backtest_engine import load_backtest_results
    from .calibrator import run_all_calibrations, save_calibration_run

    logger.info("Loading backtest results from DB...")
    rows = await load_backtest_results(term=args.term)
    logger.info("Loaded %d rows for calibration", len(rows))

    if not rows:
        logger.error("No backtest data found. Run 'backtest' first.")
        sys.exit(1)

    results = run_all_calibrations(rows)

    for result in results.values():
        await save_calibration_run(result)

    logger.info("Calibration complete.")


async def cmd_patch(args) -> None:
    from .backtest_engine import load_backtest_results
    from .calibrator import run_all_calibrations
    from .patch_weights import patch_scorer_py

    logger.info("Loading backtest results for patching...")
    short_rows = await load_backtest_results(term="short")
    long_rows  = await load_backtest_results(term="long")
    all_rows   = short_rows + long_rows

    if not all_rows:
        logger.error("No backtest data found. Run 'backtest' and 'calibrate' first.")
        sys.exit(1)

    results = run_all_calibrations(all_rows)
    patch_scorer_py(results, dry_run=args.dry_run)


async def cmd_run_all(args) -> None:
    from .backtest_engine import load_backtest_results, run_backtest
    from .calibrator import run_all_calibrations, save_calibration_run
    from .patch_weights import patch_scorer_py

    logger.info("=== Step 1: Backtest (short-term) ===")
    await run_backtest(term="short", fetch_sentiment=True, skip_existing=True)

    logger.info("=== Step 2: Backtest (long-term) — reuses cached sentiment ===")
    await run_backtest(term="long", fetch_sentiment=False, skip_existing=True)

    logger.info("=== Step 3: Load all results ===")
    short_rows = await load_backtest_results("short")
    long_rows  = await load_backtest_results("long")
    all_rows   = short_rows + long_rows
    if not all_rows:
        logger.error("No backtest data produced. Check instrument OHLCV coverage.")
        sys.exit(1)

    logger.info("=== Step 4: Calibrate weights ===")
    cal_results = run_all_calibrations(all_rows)
    for result in cal_results.values():
        await save_calibration_run(result)

    logger.info("=== Step 5: Patch scorer.py ===")
    patch_scorer_py(cal_results, dry_run=False)

    logger.info("=== Pipeline complete ===")


def main():
    parser = argparse.ArgumentParser(
        description="TradeSignal Backtester & Weight Calibrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # fetch-sentiment
    subparsers.add_parser(
        "fetch-sentiment",
        help="Pre-fetch historical sentiment (Google News + NIM, resumes from cache)",
    )

    # status
    subparsers.add_parser("status", help="Show cache and backtest progress")

    # backtest
    p_bt = subparsers.add_parser("backtest", help="Run retroactive grade simulation")
    p_bt.add_argument("--term", choices=["short", "long"], default="short")
    p_bt.add_argument(
        "--no-sentiment", action="store_true",
        help="Skip sentiment fetch (use cached data only)",
    )
    p_bt.add_argument(
        "--force", action="store_true",
        help="Re-compute even if grades already exist in DB",
    )

    # calibrate
    p_cal = subparsers.add_parser("calibrate", help="Optimize signal weights from backtest results")
    p_cal.add_argument("--term", choices=["short", "long"], default="short")

    # patch
    p_patch = subparsers.add_parser("patch", help="Apply optimized weights to scorer.py")
    p_patch.add_argument("--dry-run", action="store_true", help="Print changes without writing")

    # run-all
    subparsers.add_parser("run-all", help="Full pipeline: sentiment → backtest → calibrate → patch")

    args = parser.parse_args()

    cmd_map = {
        "fetch-sentiment": cmd_fetch_sentiment,
        "status":          cmd_status,
        "backtest":        cmd_backtest,
        "calibrate":       cmd_calibrate,
        "patch":           cmd_patch,
        "run-all":         cmd_run_all,
    }

    asyncio.run(cmd_map[args.command](args))


if __name__ == "__main__":
    main()
