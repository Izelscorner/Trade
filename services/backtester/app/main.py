"""Backtester CLI runner.

Commands:
  fetch-sentiment   — Pre-fetch historical news sentiment (Google News + NIM LLM)
  status            — Show cache and backtest progress
  backtest          — Run retroactive grade simulation (all 4 variations automatically)
  calibrate         — Optimize composite signal weights using backtest results
  patch             — Apply optimized weights to scorer.py (requires SCORER_PY_PATH mount)
  report            — Generate multi-variation HTML report for one strategy
  report-all        — Generate reports for ALL strategies + summary report
  list-strategies   — Print available strategies
  walk-forward      — Out-of-sample test (12mo train, 3mo test, rolling)
  run-all           — Full pipeline: fetch-sentiment → backtest → calibrate → patch → report-all

Usage examples (via docker compose):
  docker compose run --rm backtester python -m app.main fetch-sentiment
  docker compose run --rm backtester python -m app.main status
  docker compose run --rm backtester python -m app.main backtest
  docker compose run --rm backtester python -m app.main backtest --fetch-sentiment
  docker compose run --rm backtester python -m app.main report --strategy top_n
  docker compose run --rm backtester python -m app.main report-all
  docker compose run --rm backtester python -m app.main report-all --cost-bps 5
  docker compose run --rm backtester python -m app.main run-all

Each strategy report contains all 4 variations:
  - Short-term + Sentiment
  - Short-term (No Sentiment)
  - Long-term + Sentiment
  - Long-term (No Sentiment)

report-all generates one report per strategy + a summary report ranking all
strategies × variations, highlighting the best overall configuration.
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

    # Flatten: each strategy has multiple variations
    flat = []
    for r in results:
        for v in r.get("variations", []):
            flat.append({
                "strategy": r.get("strategy", "?"),
                "label": v.get("label", "?"),
                "kpis": v.get("kpis", {}),
            })

    if not flat:
        return

    print(f"\n{'=' * 150}")
    print(f"  {'Strategy':<20} {'Variation':<30} {'Return':>10} {'Bench':>10} {'Alpha':>10} {'$1K->':>10} {'Sharpe':>8} {'Win Rate':>10} {'Trades':>8}")
    print(f"  {'-'*20} {'-'*30} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*8}")

    for r in sorted(flat, key=lambda x: x.get("kpis", {}).get("alpha", 0), reverse=True):
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
            f"{r.get('label', '?'):<30} "
            f"{cum:>+9.1f}% "
            f"{bench:>+9.1f}% "
            f"{alpha:>+9.1f}% "
            f"{final:>10} "
            f"{sharpe:>8.2f} "
            f"{win:>9.1f}% "
            f"{trades:>5} ({exposure:.0f}%)"
        )

    print(f"{'=' * 150}\n")


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
    import glob as glob_mod
    import os

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
    total_needed = total_days * (n_assets + 1 + n_sectors)

    async with async_session() as session:
        cached_r = await session.execute(
            text("SELECT type, COUNT(DISTINCT (key, date)) as n FROM backtest_articles GROUP BY type")
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

    # ── Price OHLCV Cache (parquet files in /cache/ohlcv) ──
    ohlcv_dir = "/cache/ohlcv"
    ohlcv_files = sorted(glob_mod.glob(os.path.join(ohlcv_dir, "*.parquet"))) if os.path.isdir(ohlcv_dir) else []
    # Build expected set from instruments
    expected_ohlcv = set()
    inst_by_yf = {}
    for inst in instruments:
        yf_sym = inst.get("yfinance_symbol") or inst.get("symbol")
        if yf_sym:
            safe = yf_sym.replace("=", "_").replace("/", "_")
            expected_ohlcv.add(safe)
            inst_by_yf[safe] = inst["symbol"]

    cached_ohlcv = {}
    for fp in ohlcv_files:
        name = os.path.basename(fp).replace(".parquet", "")
        try:
            import pandas as pd
            df = pd.read_parquet(fp)
            cached_ohlcv[name] = {
                "rows": len(df),
                "start": str(df.index[0].date()) if len(df) > 0 else "?",
                "end": str(df.index[-1].date()) if len(df) > 0 else "?",
            }
        except Exception:
            cached_ohlcv[name] = {"rows": 0, "start": "?", "end": "?"}

    print(f"{'=' * 60}")
    print(f"  Price OHLCV Cache (/cache/ohlcv parquet files)")
    print(f"{'=' * 60}")
    print(f"  Expected instruments:   {len(expected_ohlcv)}")
    print(f"  Cached files:           {len(cached_ohlcv)}")
    missing_ohlcv = expected_ohlcv - set(cached_ohlcv.keys())
    print(f"  Missing:                {len(missing_ohlcv)}")
    if missing_ohlcv:
        missing_syms = [inst_by_yf.get(s, s) for s in sorted(missing_ohlcv)]
        print(f"    → {', '.join(missing_syms[:15])}{'...' if len(missing_syms) > 15 else ''}")
    if cached_ohlcv:
        print(f"\n  {'Symbol':<12} {'yfinance':<14} {'Rows':>6} {'Start':>12} {'End':>12}")
        print(f"  {'-'*12} {'-'*14} {'-'*6} {'-'*12} {'-'*12}")
        for safe_name in sorted(cached_ohlcv.keys()):
            info = cached_ohlcv[safe_name]
            sym = inst_by_yf.get(safe_name, "?")
            print(f"  {sym:<12} {safe_name:<14} {info['rows']:>6} {info['start']:>12} {info['end']:>12}")
    print()

    # ── Fundamentals Cache (pickle files in /cache/fundamentals) ──
    fund_dir = "/cache/fundamentals"
    fund_files = sorted(glob_mod.glob(os.path.join(fund_dir, "*.pkl"))) if os.path.isdir(fund_dir) else []

    # Expected: stocks and ETFs only (commodities have no fundamentals)
    expected_fund = set()
    fund_by_yf = {}
    for inst in instruments:
        if inst.get("category", "").lower() == "commodity":
            continue
        yf_sym = inst.get("yfinance_symbol") or inst.get("symbol")
        if yf_sym:
            safe = yf_sym.replace("=", "_").replace("/", "_")
            expected_fund.add(safe)
            fund_by_yf[safe] = inst["symbol"]

    cached_fund = {}
    for fp in fund_files:
        name = os.path.basename(fp).replace(".pkl", "")
        try:
            import pickle
            with open(fp, "rb") as f:
                data = pickle.load(f)
            income = data.get("income")
            balance = data.get("balance")
            source = data.get("source", "yfinance")
            n_income = len(income.columns) if income is not None and not income.empty else 0
            n_balance = len(balance.columns) if balance is not None and not balance.empty else 0
            # Date range from income columns
            inc_start = inc_end = "?"
            if income is not None and not income.empty:
                import pandas as pd
                dates = sorted(pd.to_datetime(income.columns))
                inc_start = str(dates[0].date())
                inc_end = str(dates[-1].date())
            cached_fund[name] = {
                "income_quarters": n_income, "balance_quarters": n_balance,
                "source": source, "start": inc_start, "end": inc_end,
            }
        except Exception:
            cached_fund[name] = {
                "income_quarters": 0, "balance_quarters": 0,
                "source": "?", "start": "?", "end": "?",
            }

    n_edgar = sum(1 for v in cached_fund.values() if v["source"] == "edgar")
    n_yf = sum(1 for v in cached_fund.values() if v["source"] == "yfinance")

    print(f"{'=' * 60}")
    print(f"  Fundamentals Cache (/cache/fundamentals pickle files)")
    print(f"{'=' * 60}")
    print(f"  Expected (stocks+ETFs): {len(expected_fund)}")
    print(f"  Cached files:           {len(cached_fund)}  (EDGAR: {n_edgar}, yfinance: {n_yf})")
    missing_fund = expected_fund - set(cached_fund.keys())
    print(f"  Missing:                {len(missing_fund)}")
    if missing_fund:
        missing_syms = [fund_by_yf.get(s, s) for s in sorted(missing_fund)]
        print(f"    → {', '.join(missing_syms[:15])}{'...' if len(missing_syms) > 15 else ''}")
    if cached_fund:
        print(f"\n  {'Symbol':<12} {'Source':<8} {'Income Q':>8} {'Balance Q':>9} {'Start':>12} {'End':>12}")
        print(f"  {'-'*12} {'-'*8} {'-'*8} {'-'*9} {'-'*12} {'-'*12}")
        for safe_name in sorted(cached_fund.keys()):
            info = cached_fund[safe_name]
            sym = fund_by_yf.get(safe_name, "?")
            print(f"  {sym:<12} {info['source']:<8} {info['income_quarters']:>8} {info['balance_quarters']:>9} {info['start']:>12} {info['end']:>12}")
    print()

    # ── Backtest Results ──
    async with async_session() as session:
        bg = await session.execute(text("SELECT COUNT(*) FROM backtest_grades"))
        br = await session.execute(text("SELECT COUNT(*) FROM backtest_returns"))
        cr = await session.execute(text("SELECT COUNT(*) FROM calibration_runs"))

    print(f"{'=' * 60}")
    print(f"  Backtest Results")
    print(f"{'=' * 60}")
    print(f"  backtest_grades rows:   {bg.scalar() or 0}")
    print(f"  backtest_returns rows:  {br.scalar() or 0}")
    print(f"  calibration_runs rows:  {cr.scalar() or 0}")

    # Grade coverage per variation
    async with async_session() as session:
        var_r = await session.execute(text("""
            SELECT term, sentiment_mode, COUNT(*) as n
            FROM backtest_grades
            GROUP BY term, sentiment_mode
            ORDER BY term, sentiment_mode
        """))
        var_rows = var_r.fetchall()

    if var_rows:
        print(f"\n  Grade coverage by variation:")
        for r in var_rows:
            print(f"    {r.term:>5} / sentiment={'on' if r.sentiment_mode == 'on' else 'off':>3}: {r.n:,} grades")
    print()


async def cmd_backtest(args) -> None:
    from .backtest_engine import run_backtest

    logger.info("Starting backtest (all 4 variations: short/long × sentiment on/off)...")
    total = await run_backtest(
        fetch_sentiment=args.fetch_sentiment,
        skip_existing=not args.force,
    )
    logger.info("Backtest done. %d grade rows produced.", total)


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
    from .report_generator import generate_strategy_report
    from .strategies import StrategyParams

    params = StrategyParams(
        threshold=args.threshold,
        top_n=args.top_n,
        long_pct=args.long_pct,
        cost_bps=args.cost_bps,
    )

    result = await generate_strategy_report(
        strategy=args.strategy,
        strategy_params=params,
    )
    if result.get("variations"):
        _print_summary_table([result])
        logger.info("Report saved: %s", result.get("filepath", ""))


async def cmd_list_strategies(args) -> None:
    from .strategies import list_strategies
    print(list_strategies())


async def cmd_report_all(args) -> None:
    from .report_generator import generate_strategy_report, generate_summary_report, generate_walk_forward_report
    from .strategies import STRATEGIES, StrategyParams

    params = StrategyParams(cost_bps=args.cost_bps)
    strategy_names = list(STRATEGIES.keys())
    include_wf = getattr(args, "walk_forward", False)
    all_results = []

    for name in STRATEGIES:
        result = await generate_strategy_report(
            strategy=name,
            strategy_params=params,
            strategy_names=strategy_names,
            include_walk_forward=include_wf,
        )
        if result.get("variations"):
            all_results.append(result)

    logger.info("Generated %d strategy reports.", len(all_results))
    _print_summary_table(all_results)

    # Generate walk-forward report if requested
    wf_report = ""
    if include_wf:
        from .backtest_engine import load_backtest_results
        from .walk_forward import run_walk_forward

        wf_term = getattr(args, "wf_term", "short")
        logger.info("Running walk-forward analysis (term=%s)...", wf_term)
        rows = await load_backtest_results(term=wf_term)
        if rows:
            wf_results = run_walk_forward(rows, term=wf_term)
            if wf_results:
                wf_report = generate_walk_forward_report(
                    wf_results, term=wf_term, strategy_names=strategy_names,
                )
                logger.info("Walk-forward report: %s", wf_report)
        else:
            logger.warning("No backtest data for walk-forward. Skipping.")

    # Generate summary report (index.html)
    summary_path = await generate_summary_report(all_results, include_walk_forward=include_wf)
    logger.info("Summary report: %s", summary_path)
    total = len(all_results) + 1 + (1 if wf_report else 0)
    logger.info("Total reports: %d strategy + 1 summary%s = %d",
                len(all_results), " + 1 walk-forward" if wf_report else "", total)


async def cmd_walk_forward(args) -> None:
    from .backtest_engine import load_backtest_results
    from .report_generator import generate_walk_forward_report
    from .walk_forward import print_walk_forward_results, run_walk_forward

    logger.info("Loading backtest results for walk-forward analysis...")
    rows = await load_backtest_results(term=args.term)
    logger.info("Loaded %d rows", len(rows))

    if not rows:
        logger.error("No backtest data found. Run 'backtest' first.")
        sys.exit(1)

    results = run_walk_forward(rows, term=args.term)
    print_walk_forward_results(results)

    # Also generate HTML report
    filepath = generate_walk_forward_report(results, term=args.term)
    if filepath:
        logger.info("Walk-forward HTML report: %s", filepath)


async def cmd_run_all(args) -> None:
    from .backtest_engine import load_backtest_results, run_backtest
    from .calibrator import run_all_calibrations, save_calibration_run
    from .patch_weights import patch_scorer_py

    logger.info("=== Step 1: Backtest (all 4 variations) ===")
    await run_backtest(fetch_sentiment=True, skip_existing=True)

    logger.info("=== Step 2: Load results for calibration ===")
    short_rows = await load_backtest_results("short")
    long_rows  = await load_backtest_results("long")
    all_rows   = short_rows + long_rows
    if not all_rows:
        logger.error("No backtest data produced. Check instrument OHLCV coverage.")
        sys.exit(1)

    logger.info("=== Step 3: Calibrate weights ===")
    cal_results = run_all_calibrations(all_rows)
    for result in cal_results.values():
        await save_calibration_run(result)

    logger.info("=== Step 4: Patch scorer.py ===")
    patch_scorer_py(cal_results, dry_run=False)

    logger.info("=== Step 5: Generate all strategy reports + summary ===")
    from .report_generator import generate_strategy_report, generate_summary_report, generate_walk_forward_report
    from .strategies import STRATEGIES, StrategyParams

    params = StrategyParams()
    strategy_names = list(STRATEGIES.keys())
    include_wf = getattr(args, "walk_forward", False)
    all_results = []
    for name in STRATEGIES:
        result = await generate_strategy_report(
            strategy=name, strategy_params=params,
            strategy_names=strategy_names, include_walk_forward=include_wf,
        )
        if result.get("variations"):
            all_results.append(result)

    _print_summary_table(all_results)

    # Walk-forward report
    wf_report = ""
    if include_wf:
        from .walk_forward import run_walk_forward
        logger.info("=== Step 5b: Walk-forward analysis ===")
        wf_term = getattr(args, "wf_term", "short")
        wf_rows = await load_backtest_results(term=wf_term)
        if wf_rows:
            wf_results = run_walk_forward(wf_rows, term=wf_term)
            if wf_results:
                wf_report = generate_walk_forward_report(
                    wf_results, term=wf_term, strategy_names=strategy_names,
                )

    summary_path = await generate_summary_report(all_results, include_walk_forward=include_wf)
    logger.info("Summary report: %s", summary_path)
    total = len(all_results) + 1 + (1 if wf_report else 0)
    logger.info("=== Pipeline complete (%d strategy reports + 1 summary%s) ===",
                len(all_results), " + 1 walk-forward" if wf_report else "")


async def cmd_export_data(args) -> None:
    """Export backtest-related tables to SQL dump file."""
    import os
    from .db import async_session
    from sqlalchemy import text

    tables = [
        "backtest_articles", "backtest_sentiment_cache", "backtest_av_cache",
        "backtest_grades", "backtest_returns", "calibration_runs",
    ]
    out_dir = "/reports"
    dump_path = os.path.join(out_dir, f"backtest_data_export.sql")

    with open(dump_path, "w") as f:
        f.write("-- TradeSignal Backtest Data Export\n")
        f.write(f"-- Generated: {__import__('datetime').datetime.now().isoformat()}\n\n")

        for table in tables:
            logger.info("Exporting %s...", table)
            async with async_session() as session:
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

                batch = 500
                for offset in range(0, total, batch):
                    rows_r = await session.execute(text(
                        f"SELECT {col_list} FROM {table} ORDER BY 1 OFFSET {offset} LIMIT {batch}"
                    ))
                    rows = rows_r.fetchall()
                    import json as _json
                    import uuid as _uuid
                    from decimal import Decimal as _Decimal
                    for row in rows:
                        vals = []
                        for v in row:
                            if v is None:
                                vals.append("NULL")
                            elif isinstance(v, bool):
                                vals.append("TRUE" if v else "FALSE")
                            elif isinstance(v, str):
                                vals.append("'" + v.replace("'", "''") + "'")
                            elif isinstance(v, _uuid.UUID):
                                vals.append(f"'{v}'")
                            elif isinstance(v, dict):
                                vals.append("'" + _json.dumps(v).replace("'", "''") + "'::jsonb")
                            elif isinstance(v, (__import__('datetime').date, __import__('datetime').datetime)):
                                vals.append(f"'{v}'")
                            elif isinstance(v, _Decimal):
                                vals.append(str(v))
                            elif isinstance(v, (int, float)):
                                vals.append(str(v))
                            else:
                                vals.append("'" + str(v).replace("'", "''") + "'")
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

    # backtest — runs all 4 variations automatically
    p_bt = subparsers.add_parser(
        "backtest",
        help="Run retroactive grade simulation (all 4 variations: short/long × sentiment on/off)",
    )
    p_bt.add_argument(
        "--fetch-sentiment", action="store_true",
        help="Fetch missing sentiment from Google News + NIM before backtesting (slow, only when needed)",
    )
    p_bt.add_argument(
        "--force", action="store_true",
        help="Re-compute even if grades already exist in DB",
    )

    # calibrate (keeps --term since calibration is per-term)
    p_cal = subparsers.add_parser("calibrate", help="Optimize signal weights from backtest results")
    p_cal.add_argument("--term", choices=["short", "long"], default="short")

    # patch
    p_patch = subparsers.add_parser("patch", help="Apply optimized weights to scorer.py")
    p_patch.add_argument("--dry-run", action="store_true", help="Print changes without writing")

    # report — generates multi-variation report for one strategy
    p_rep = subparsers.add_parser(
        "report",
        help="Generate multi-variation HTML report for one strategy (4 variations: short/long × sentiment on/off)",
    )
    p_rep.add_argument(
        "--strategy",
        choices=["portfolio", "top_pick", "high_conviction", "top_n",
                 "long_short", "sector_rotation", "contrarian", "risk_adjusted",
                 "momentum", "random"],
        default="portfolio",
        help="Trading strategy to evaluate (use 'list-strategies' to see descriptions)",
    )
    p_rep.add_argument("--threshold", type=float, default=60.0, help="Buy-confidence threshold for high_conviction (default 60)")
    p_rep.add_argument("--top-n", type=int, default=3, help="Number of instruments for top_n strategy (default 3)")
    p_rep.add_argument("--long-pct", type=float, default=0.20, help="Percentile for long_short/contrarian legs (default 0.20)")
    p_rep.add_argument("--cost-bps", type=float, default=0.0, help="Round-trip transaction cost in basis points (default 0, suggest 5-10)")

    # report-all: all strategies + summary
    p_repall = subparsers.add_parser(
        "report-all",
        help="Generate reports for ALL strategies + summary report (each with 4 variations)",
    )
    p_repall.add_argument("--cost-bps", type=float, default=0.0, help="Round-trip transaction cost in basis points (default 0, suggest 5-10)")
    p_repall.add_argument("--walk-forward", action="store_true", help="Include walk-forward out-of-sample report")
    p_repall.add_argument("--wf-term", choices=["short", "long"], default="short", help="Term for walk-forward analysis (default short)")

    # list-strategies
    subparsers.add_parser("list-strategies", help="Show all available trading strategies")

    # walk-forward (keeps --term since it's per-term analysis)
    p_wf = subparsers.add_parser("walk-forward", help="Walk-forward out-of-sample test (12mo train, 3mo test)")
    p_wf.add_argument("--term", choices=["short", "long"], default="short")

    # run-all
    p_runall = subparsers.add_parser("run-all", help="Full pipeline: sentiment → backtest → calibrate → patch → report-all")
    p_runall.add_argument("--walk-forward", action="store_true", help="Include walk-forward out-of-sample report")
    p_runall.add_argument("--wf-term", choices=["short", "long"], default="short", help="Term for walk-forward analysis (default short)")

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
        "walk-forward":     cmd_walk_forward,
        "run-all":          cmd_run_all,
        "export-data":      cmd_export_data,
        "import-data":      cmd_import_data,
    }

    asyncio.run(cmd_map[args.command](args))


if __name__ == "__main__":
    main()
