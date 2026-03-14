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


def _print_summary_table(results: list[dict]) -> None:
    """Print a formatted results summary table to stdout."""
    if not results:
        return

    print(f"\n{'=' * 140}")
    print(f"  {'Strategy':<20} {'Horizon':<12} {'Sentiment':<18} {'Return':>10} {'Bench':>10} {'Alpha':>10} {'$1K->':>10} {'Sharpe':>8} {'Win Rate':>10} {'Trades':>8}")
    print(f"  {'-'*20} {'-'*12} {'-'*18} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*8}")

    for r in sorted(results, key=lambda x: x.get("kpis", {}).get("alpha", 0), reverse=True):
        k = r.get("kpis", {})
        if not k:
            continue
        cum = k.get("cum_return", 0) * 100
        bench = k.get("bench_return", 0) * 100
        alpha = k.get("alpha", 0) * 100
        final = k.get("final_value", "N/A")
        sharpe = k.get("sharpe", 0)
        win = k.get("win_rate", 0) * 100
        trades = k.get("n_trades", 0)
        exposure = k.get("exposure_pct", 0)

        print(
            f"  {r.get('strategy', '?'):<20} "
            f"{r.get('horizon', '?'):<12} "
            f"{r.get('sentiment_mode', '?'):<18} "
            f"{cum:>+9.1f}% "
            f"{bench:>+9.1f}% "
            f"{alpha:>+9.1f}% "
            f"{final:>10} "
            f"{sharpe:>8.2f} "
            f"{win:>9.1f}% "
            f"{trades:>5} ({exposure:.0f}%)"
        )

    print(f"{'=' * 140}\n")


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

    ignore_sent = args.no_sentiment
    # Don't fetch new sentiment unless explicitly requested
    fetch_sent = args.fetch_sentiment

    logger.info(
        "Starting backtest (term=%s, fetch_sentiment=%s, ignore_sentiment=%s)...",
        args.term, fetch_sent, ignore_sent,
    )
    results = await run_backtest(
        term=args.term,
        fetch_sentiment=fetch_sent,
        skip_existing=not args.force,
        ignore_sentiment=ignore_sent,
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


async def cmd_report(args) -> None:
    from .report_generator import generate_backtest_report
    from .strategies import StrategyParams

    sentiment_mode = "with sentiment"
    if args.no_sentiment:
        sentiment_mode = "without sentiment"

    params = StrategyParams(
        threshold=args.threshold,
        top_n=args.top_n,
        long_pct=args.long_pct,
    )

    result = await generate_backtest_report(
        strategy=args.strategy,
        term=args.term,
        sentiment_mode=sentiment_mode,
        strategy_params=params,
    )
    if result.get("kpis"):
        _print_summary_table([result])


async def cmd_list_strategies(args) -> None:
    from .strategies import list_strategies
    print(list_strategies())


async def cmd_report_all(args) -> None:
    from .report_generator import generate_backtest_report
    from .strategies import STRATEGIES, StrategyParams

    sentiment_mode = "with sentiment"
    if args.no_sentiment:
        sentiment_mode = "without sentiment"

    params = StrategyParams()
    results = []
    for name in STRATEGIES:
        logger.info("=== Generating report: %s ===", name)
        result = await generate_backtest_report(
            strategy=name,
            term=args.term,
            sentiment_mode=sentiment_mode,
            strategy_params=params,
        )
        if result.get("kpis"):
            results.append(result)
    logger.info("All %d strategy reports generated.", len(STRATEGIES))
    _print_summary_table(results)


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

    logger.info("=== Step 6: Generate Performance Reports (all strategies) ===")
    from .report_generator import generate_backtest_report
    from .strategies import STRATEGIES, StrategyParams
    params = StrategyParams()
    results = []
    for name in STRATEGIES:
        for t in ("short", "long"):
            result = await generate_backtest_report(strategy=name, term=t, strategy_params=params)
            if result.get("kpis"):
                results.append(result)

    _print_summary_table(results)
    logger.info("=== Pipeline complete ===")


async def cmd_export_data(args) -> None:
    """Export backtest-related tables to SQL dump file."""
    import os
    from .db import async_session
    from sqlalchemy import text

    tables = ["backtest_sentiment_cache", "backtest_grades", "backtest_returns", "calibration_runs"]
    out_dir = "/reports"
    dump_path = os.path.join(out_dir, f"backtest_data_export.sql")

    with open(dump_path, "w") as f:
        f.write("-- TradeSignal Backtest Data Export\n")
        f.write(f"-- Generated: {__import__('datetime').datetime.now().isoformat()}\n\n")

        for table in tables:
            logger.info("Exporting %s...", table)
            async with async_session() as session:
                # Get column names
                cols_r = await session.execute(text(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' ORDER BY ordinal_position"
                ))
                cols = [r[0] for r in cols_r.fetchall()]
                if not cols:
                    logger.warning("Table %s not found, skipping", table)
                    continue

                col_list = ", ".join(cols)
                count_r = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                total = count_r.scalar()
                logger.info("  %s: %d rows", table, total)

                f.write(f"-- Table: {table} ({total} rows)\n")
                f.write(f"DELETE FROM {table};\n")

                # Batch export
                batch = 500
                for offset in range(0, total, batch):
                    rows_r = await session.execute(text(
                        f"SELECT {col_list} FROM {table} ORDER BY 1 OFFSET {offset} LIMIT {batch}"
                    ))
                    rows = rows_r.fetchall()
                    for row in rows:
                        vals = []
                        for v in row:
                            if v is None:
                                vals.append("NULL")
                            elif isinstance(v, str):
                                vals.append("'" + v.replace("'", "''") + "'")
                            elif isinstance(v, (dict,)):
                                vals.append("'" + str(v).replace("'", "''") + "'::jsonb")
                            elif isinstance(v, (__import__('datetime').date, __import__('datetime').datetime)):
                                vals.append(f"'{v}'")
                            else:
                                vals.append(str(v))
                        f.write(f"INSERT INTO {table} ({col_list}) VALUES ({', '.join(vals)});\n")
                f.write("\n")

    logger.info("Export complete: %s", dump_path)


async def cmd_import_data(args) -> None:
    """Import backtest data from SQL dump file."""
    import os
    from .db import async_session
    from sqlalchemy import text

    dump_path = args.file
    if not os.path.exists(dump_path):
        logger.error("File not found: %s", dump_path)
        sys.exit(1)

    logger.info("Importing from %s...", dump_path)
    with open(dump_path, "r") as f:
        sql = f.read()

    # Split into individual statements and execute
    statements = [s.strip() for s in sql.split(";\n") if s.strip() and not s.strip().startswith("--")]
    logger.info("Found %d SQL statements", len(statements))

    async with async_session() as session:
        batch_size = 100
        for i in range(0, len(statements), batch_size):
            batch = statements[i:i + batch_size]
            for stmt in batch:
                try:
                    await session.execute(text(stmt))
                except Exception as e:
                    logger.warning("Statement failed: %s... — %s", stmt[:80], e)
            await session.commit()
            if (i + batch_size) % 1000 == 0:
                logger.info("  Processed %d / %d statements", min(i + batch_size, len(statements)), len(statements))

    logger.info("Import complete.")


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
        help="Zero out sentiment/macro/sector scores (technical+fundamentals only)",
    )
    p_bt.add_argument(
        "--fetch-sentiment", action="store_true",
        help="Fetch missing sentiment from Google News + NIM (slow, only when needed)",
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

    # report
    p_rep = subparsers.add_parser("report", help="Generate HTML performance report")
    p_rep.add_argument(
        "--strategy",
        choices=["portfolio", "top_pick", "high_conviction", "top_n",
                 "long_short", "sector_rotation", "contrarian", "risk_adjusted"],
        default="portfolio",
        help="Trading strategy to evaluate (use 'list-strategies' to see descriptions)",
    )
    p_rep.add_argument("--term", choices=["short", "long"], default="short")
    p_rep.add_argument("--no-sentiment", action="store_true", help="Label report as 'without sentiment'")
    p_rep.add_argument("--threshold", type=float, default=60.0, help="Buy-confidence threshold for high_conviction (default 60)")
    p_rep.add_argument("--top-n", type=int, default=3, help="Number of instruments for top_n strategy (default 3)")
    p_rep.add_argument("--long-pct", type=float, default=0.20, help="Percentile for long_short/contrarian legs (default 0.20)")

    # report-all: run all strategies at once
    p_repall = subparsers.add_parser("report-all", help="Generate reports for ALL strategies at once")
    p_repall.add_argument("--term", choices=["short", "long"], default="short")
    p_repall.add_argument("--no-sentiment", action="store_true")

    # list-strategies
    subparsers.add_parser("list-strategies", help="Show all available trading strategies")

    # run-all
    subparsers.add_parser("run-all", help="Full pipeline: sentiment → backtest → calibrate → patch → report")

    # export-data
    subparsers.add_parser("export-data", help="Export backtest tables to SQL dump (saved to /reports/)")

    # import-data
    p_import = subparsers.add_parser("import-data", help="Import backtest data from SQL dump")
    p_import.add_argument("--file", default="/reports/backtest_data_export.sql", help="Path to SQL dump file")

    args = parser.parse_args()

    cmd_map = {
        "fetch-sentiment":  cmd_fetch_sentiment,
        "status":           cmd_status,
        "backtest":         cmd_backtest,
        "calibrate":        cmd_calibrate,
        "patch":            cmd_patch,
        "report":           cmd_report,
        "report-all":       cmd_report_all,
        "list-strategies":  cmd_list_strategies,
        "run-all":          cmd_run_all,
        "export-data":      cmd_export_data,
        "import-data":      cmd_import_data,
    }

    asyncio.run(cmd_map[args.command](args))


if __name__ == "__main__":
    main()
