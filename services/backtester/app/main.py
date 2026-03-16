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
  deepfill          — Enrich sparse dates with broader queries (assets + macro + sectors)
  run-all           — Full pipeline: fetch-sentiment → backtest → calibrate → patch → report-all

Usage examples (via docker compose):
  docker compose run --rm backtester python -m app.main fetch-sentiment
  docker compose run --rm backtester python -m app.main status
  docker compose run --rm backtester python -m app.main backtest
  docker compose run --rm backtester python -m app.main backtest --fetch-sentiment
  docker compose run --rm backtester python -m app.main report --strategy top_n
  docker compose run --rm backtester python -m app.main report-all
  docker compose run --rm backtester python -m app.main report-all --cost-bps 5
  docker compose run --rm backtester python -m app.main export-data
  docker compose run --rm backtester python -m app.main import-data
  docker compose run --rm backtester python -m app.main import-data --file /reports/backtest_full_export.tar.gz

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

    mode = getattr(args, "mode", "production")
    logger.info("Starting backtest [mode=%s] (all 4 variations: short/long x sentiment on/off)...", mode)
    total = await run_backtest(
        fetch_sentiment=args.fetch_sentiment,
        skip_existing=not args.force,
        mode=mode,
    )
    logger.info("Backtest done. %d grade rows produced.", total)


async def cmd_calibrate(args) -> None:
    from .backtest_engine import load_backtest_results
    from .calibrator import run_all_calibrations, save_calibration_run

    mode = getattr(args, "mode", "production")
    logger.info("Loading backtest results from DB [mode=%s]...", mode)
    rows = await load_backtest_results(term=args.term)
    logger.info("Loaded %d rows for calibration", len(rows))

    if not rows:
        logger.error("No backtest data found. Run 'backtest' first.")
        sys.exit(1)

    results = run_all_calibrations(rows, mode=mode)

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


async def cmd_deepfill(args) -> None:
    from .backtest_engine import load_instruments
    from .config import BACKTEST_END, BACKTEST_START
    from .historical_sentiment import deepfill_historical_sentiment

    instruments = await load_instruments()
    start = date.fromisoformat(BACKTEST_START)
    end = date.fromisoformat(BACKTEST_END)

    logger.info(
        "Deepfill: enriching sparse coverage for %d instruments + macro + sectors (%s → %s, min=%d)",
        len(instruments), start, end, args.min_articles,
    )
    await deepfill_historical_sentiment(
        instruments, start, end, min_articles=args.min_articles,
    )
    logger.info("Deepfill complete.")


async def cmd_run_all(args) -> None:
    from .backtest_engine import load_backtest_results, run_backtest
    from .calibrator import run_all_calibrations, save_calibration_run
    from .patch_weights import patch_scorer_py

    mode = getattr(args, "mode", "production")
    logger.info("=== Step 1: Backtest [mode=%s] (all 4 variations) ===", mode)
    await run_backtest(fetch_sentiment=True, skip_existing=True, mode=mode)

    logger.info("=== Step 2: Load results for calibration ===")
    short_rows = await load_backtest_results("short")
    long_rows  = await load_backtest_results("long")
    all_rows   = short_rows + long_rows
    if not all_rows:
        logger.error("No backtest data produced. Check instrument OHLCV coverage.")
        sys.exit(1)

    logger.info("=== Step 3: Calibrate weights [mode=%s] ===", mode)
    cal_results = run_all_calibrations(all_rows, mode=mode)
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


async def cmd_oos_test(args) -> None:
    """Out-of-sample split test: train on first period, test on second.

    Default split: train 2020-01-01 to 2023-12-31, test 2024-01-01 to 2026-03-15.
    Runs every strategy on both halves and prints comparison table.
    """
    import numpy as np
    import pandas as pd
    from .report_generator import get_raw_results, compute_benchmark, VARIATIONS
    from .strategies import STRATEGIES, StrategyParams, apply_strategy, _get_entry_dates, _holding_period

    split_date = pd.Timestamp(args.split)
    params = StrategyParams(cost_bps=args.cost_bps)

    strategy_names = list(STRATEGIES.keys())

    print(f"\n{'=' * 170}")
    print(f"  OUT-OF-SAMPLE SPLIT TEST  —  Train: before {args.split}  |  Test: {args.split} onward")
    print(f"{'=' * 170}")
    print(
        f"  {'Strategy':<20} {'Variation':<30} "
        f"{'Train Ret':>10} {'Train Alpha':>12} {'Train Sharpe':>12} "
        f"{'Test Ret':>10} {'Test Alpha':>12} {'Test Sharpe':>12} "
        f"{'Decay':>8}"
    )
    print(
        f"  {'-'*20} {'-'*30} "
        f"{'-'*10} {'-'*12} {'-'*12} "
        f"{'-'*10} {'-'*12} {'-'*12} "
        f"{'-'*8}"
    )

    rows = []

    for var in VARIATIONS:
        term = var["term"]
        smode = var["smode"]
        label = var["label"]

        df = await get_raw_results(term=term, sentiment_mode=smode)
        if df.empty:
            continue
        df = df.dropna(subset=["overall_score", "return_val"])
        df["date"] = pd.to_datetime(df["date"])

        df_train = df[df["date"] < split_date].copy()
        df_test = df[df["date"] >= split_date].copy()

        if df_train.empty or df_test.empty:
            continue

        holding = _holding_period(term)
        periods_per_year = 52.0 if term == "short" else 13.0

        for strat_name in strategy_names:
            for split_label, split_df in [("train", df_train), ("test", df_test)]:
                strat_df = apply_strategy(split_df.copy(), strat_name, term, params)
                period_rets = strat_df.groupby("date")["daily_strat_ret"].sum()
                active = period_rets[period_rets != 0]

                total_ret = float((1 + active).prod() - 1) if len(active) > 0 else 0.0

                if len(active) > 1 and active.std() > 0:
                    sharpe = float(active.mean() / active.std() * np.sqrt(periods_per_year))
                else:
                    sharpe = 0.0

                # Benchmark
                bench_rets = compute_benchmark(strat_df, term=term)
                bench_total = float((1 + bench_rets).prod() - 1) if len(bench_rets) > 0 else 0.0
                alpha = total_ret - bench_total

                if split_label == "train":
                    train_ret, train_alpha, train_sharpe = total_ret, alpha, sharpe
                else:
                    test_ret, test_alpha, test_sharpe = total_ret, alpha, sharpe

            # Compute decay (how much alpha drops from train to test)
            if abs(train_alpha) > 0.001:
                decay = (train_alpha - test_alpha) / abs(train_alpha) * 100
            else:
                decay = 0.0

            rows.append({
                "strategy": strat_name, "label": label,
                "train_ret": train_ret, "train_alpha": train_alpha, "train_sharpe": train_sharpe,
                "test_ret": test_ret, "test_alpha": test_alpha, "test_sharpe": test_sharpe,
                "decay": decay,
            })

            print(
                f"  {strat_name:<20} {label:<30} "
                f"{train_ret*100:>+9.1f}% {train_alpha*100:>+11.1f}% {train_sharpe:>11.2f} "
                f"{test_ret*100:>+9.1f}% {test_alpha*100:>+11.1f}% {test_sharpe:>11.2f} "
                f"{decay:>+7.0f}%"
            )

    print(f"{'=' * 170}")

    # Summary: rank by test alpha
    if rows:
        print(f"\n  TOP 10 BY TEST-SET ALPHA (out-of-sample performance):")
        print(f"  {'-'*100}")
        sorted_rows = sorted(rows, key=lambda x: x["test_alpha"], reverse=True)
        for i, r in enumerate(sorted_rows[:10], 1):
            print(
                f"  {i:>2}. {r['strategy']:<20} {r['label']:<30} "
                f"Test: {r['test_ret']*100:>+7.1f}% (α {r['test_alpha']*100:>+7.1f}%) "
                f"Train: {r['train_ret']*100:>+7.1f}% (α {r['train_alpha']*100:>+7.1f}%) "
                f"Decay: {r['decay']:>+5.0f}%"
            )
        print()

        # Flag strategies where test alpha is negative but train alpha was positive (overfit)
        overfit = [r for r in rows if r["train_alpha"] > 0.05 and r["test_alpha"] < 0]
        if overfit:
            print(f"  ⚠ OVERFIT WARNING — positive train alpha but negative test alpha:")
            for r in sorted(overfit, key=lambda x: x["train_alpha"], reverse=True)[:5]:
                print(f"    {r['strategy']:<20} {r['label']:<30} Train α: {r['train_alpha']*100:>+.1f}%  Test α: {r['test_alpha']*100:>+.1f}%")
            print()


async def cmd_export_data(args) -> None:
    """Export all backtest data to a compressed archive.

    Covers:
      DB tables  — backtest_articles, backtest_sentiment_cache, backtest_av_cache,
                   backtest_grades, backtest_returns, calibration_runs
                   (exported as CSV via PostgreSQL COPY for speed)
      File cache — /cache/ohlcv/*.parquet  (price OHLCV)
                   /cache/fundamentals/*.pkl
                   /cache/earnings/*.pkl
                   /cache/vix_history.pkl

    Output: /reports/backtest_full_export.tar.gz + manifest JSON
    """
    import json
    import os
    import shutil
    import tarfile
    import tempfile
    from datetime import datetime

    import asyncpg

    from .config import DATABASE_URL

    tables = [
        "backtest_articles",
        "backtest_sentiment_cache",
        "backtest_av_cache",
        "backtest_grades",
        "backtest_returns",
        "calibration_runs",
    ]
    cache_dirs = [
        ("/cache/ohlcv",        "cache/ohlcv"),
        ("/cache/fundamentals", "cache/fundamentals"),
        ("/cache/earnings",     "cache/earnings"),
    ]
    cache_files = [("/cache/vix_history.pkl", "cache/vix_history.pkl")]

    out_dir = "/reports"
    archive_path = os.path.join(out_dir, "backtest_full_export.tar.gz")

    dsn = (
        DATABASE_URL
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("postgres+asyncpg://", "postgresql://")
    )

    manifest: dict = {
        "generated": datetime.now().isoformat(),
        "tables": {},
        "cache": {},
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        db_dir = os.path.join(tmpdir, "db")
        os.makedirs(db_dir)

        # ── 1. Export each DB table as CSV via PostgreSQL COPY ────────────────
        conn = await asyncpg.connect(dsn)
        try:
            for table in tables:
                csv_path = os.path.join(db_dir, f"{table}.csv")
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                logger.info("Exporting DB table %-30s  %d rows", table, count)
                with open(csv_path, "wb") as fh:
                    await conn.copy_from_query(
                        f"SELECT * FROM {table}",
                        output=fh,
                        format="csv",
                        header=True,
                    )
                size_kb = os.path.getsize(csv_path) / 1024
                manifest["tables"][table] = {"rows": count, "size_kb": round(size_kb, 1)}
        finally:
            await conn.close()

        # ── 2. Bundle into tar.gz ─────────────────────────────────────────────
        with tarfile.open(archive_path, "w:gz") as tar:
            # DB CSVs
            for table in tables:
                csv_path = os.path.join(db_dir, f"{table}.csv")
                if os.path.exists(csv_path):
                    tar.add(csv_path, arcname=f"db/{table}.csv")

            # Cache directories
            for src_dir, arc_name in cache_dirs:
                if os.path.isdir(src_dir):
                    files = os.listdir(src_dir)
                    total_bytes = sum(
                        os.path.getsize(os.path.join(src_dir, f)) for f in files
                    )
                    tar.add(src_dir, arcname=arc_name)
                    manifest["cache"][arc_name] = {
                        "files": len(files),
                        "size_kb": round(total_bytes / 1024, 1),
                    }
                    logger.info("Bundled %-35s  %d files  (%.1f KB)",
                                src_dir, len(files), total_bytes / 1024)
                else:
                    logger.warning("Cache dir not found, skipping: %s", src_dir)

            # Single cache files
            for src_file, arc_name in cache_files:
                if os.path.exists(src_file):
                    tar.add(src_file, arcname=arc_name)
                    size_kb = os.path.getsize(src_file) / 1024
                    manifest["cache"][arc_name] = {"files": 1, "size_kb": round(size_kb, 1)}
                    logger.info("Bundled %-35s  (%.1f KB)", src_file, size_kb)
                else:
                    logger.warning("Cache file not found, skipping: %s", src_file)

            # Write manifest into archive
            manifest_bytes = json.dumps(manifest, indent=2).encode()
            import io
            manifest_info = tarfile.TarInfo(name="manifest.json")
            manifest_info.size = len(manifest_bytes)
            tar.addfile(manifest_info, io.BytesIO(manifest_bytes))

    archive_mb = os.path.getsize(archive_path) / 1024 / 1024

    # Also write manifest alongside archive for quick inspection
    manifest_path = archive_path.replace(".tar.gz", "_manifest.json")
    with open(manifest_path, "w") as fh:
        json.dump(manifest, fh, indent=2)

    logger.info("Export complete: %s  (%.1f MB)", archive_path, archive_mb)
    logger.info("Manifest:        %s", manifest_path)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"  Backtest Full Export — {manifest['generated']}")
    print(f"{'=' * 60}")
    print(f"  Archive: {archive_path}  ({archive_mb:.1f} MB compressed)")
    print(f"\n  DB Tables:")
    for t, info in manifest["tables"].items():
        print(f"    {t:<30} {info['rows']:>8,} rows  ({info['size_kb']:.0f} KB)")
    print(f"\n  Cache Files:")
    for name, info in manifest["cache"].items():
        print(f"    {name:<35} {info['files']:>4} files  ({info['size_kb']:.0f} KB)")
    print(f"{'=' * 60}\n")


async def cmd_import_data(args) -> None:
    """Import backtest data from a full export archive (.tar.gz).

    Restores all DB tables (via PostgreSQL COPY) and all cache files.
    Each table is truncated before import — existing data is replaced.
    """
    import io
    import json
    import os
    import shutil
    import tarfile
    import tempfile

    import asyncpg

    from .config import DATABASE_URL

    tables = [
        "backtest_articles",
        "backtest_sentiment_cache",
        "backtest_av_cache",
        "backtest_grades",
        "backtest_returns",
        "calibration_runs",
    ]
    # Truncation order respects FK constraints (children first)
    truncate_order = [
        "backtest_returns",
        "backtest_grades",
        "backtest_articles",
        "backtest_sentiment_cache",
        "backtest_av_cache",
        "calibration_runs",
    ]
    cache_restore = [
        ("cache/ohlcv",        "/cache/ohlcv"),
        ("cache/fundamentals", "/cache/fundamentals"),
        ("cache/earnings",     "/cache/earnings"),
    ]
    vix_restore = ("cache/vix_history.pkl", "/cache/vix_history.pkl")

    archive_path = args.file
    if not os.path.exists(archive_path):
        logger.error("File not found: %s", archive_path)
        sys.exit(1)

    if not archive_path.endswith(".tar.gz"):
        logger.error("Expected a .tar.gz archive produced by export-data. Got: %s", archive_path)
        sys.exit(1)

    dsn = (
        DATABASE_URL
        .replace("postgresql+asyncpg://", "postgresql://")
        .replace("postgres+asyncpg://", "postgresql://")
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        logger.info("Extracting %s...", archive_path)
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(tmpdir)

        # Print manifest if present
        manifest_path = os.path.join(tmpdir, "manifest.json")
        if os.path.exists(manifest_path):
            with open(manifest_path) as fh:
                manifest = json.load(fh)
            logger.info("Archive generated: %s", manifest.get("generated", "?"))

        # ── 1. Restore DB tables ──────────────────────────────────────────────
        conn = await asyncpg.connect(dsn)
        try:
            # Truncate in safe order
            for table in truncate_order:
                csv_path = os.path.join(tmpdir, "db", f"{table}.csv")
                if not os.path.exists(csv_path):
                    continue
                logger.info("Truncating %s...", table)
                await conn.execute(f"TRUNCATE TABLE {table} CASCADE")

            # Import via COPY
            for table in tables:
                csv_path = os.path.join(tmpdir, "db", f"{table}.csv")
                if not os.path.exists(csv_path):
                    logger.warning("No CSV for table %s in archive, skipping", table)
                    continue
                logger.info("Importing %s...", table)
                with open(csv_path, "rb") as fh:
                    result = await conn.copy_to_table(
                        table,
                        source=fh,
                        format="csv",
                        header=True,
                    )
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                logger.info("  %s: %s  →  %d rows now in DB", table, result, count)
        finally:
            await conn.close()

        # ── 2. Restore cache files ────────────────────────────────────────────
        for arc_dir, dst_dir in cache_restore:
            src = os.path.join(tmpdir, arc_dir)
            if not os.path.isdir(src):
                logger.warning("Cache dir not found in archive: %s", arc_dir)
                continue
            os.makedirs(dst_dir, exist_ok=True)
            n = 0
            for fname in os.listdir(src):
                shutil.copy2(os.path.join(src, fname), os.path.join(dst_dir, fname))
                n += 1
            logger.info("Restored %-30s  %d files → %s", arc_dir, n, dst_dir)

        vix_src = os.path.join(tmpdir, vix_restore[0])
        if os.path.exists(vix_src):
            shutil.copy2(vix_src, vix_restore[1])
            logger.info("Restored vix_history.pkl → %s", vix_restore[1])

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
    p_bt.add_argument(
        "--mode", choices=["production", "experimental"], default="production",
        help="Scoring mode: 'production' (5-signal, matches scorer.py) or 'experimental' (8-signal with VIX/momentum/earnings)",
    )

    # calibrate (keeps --term since calibration is per-term)
    p_cal = subparsers.add_parser("calibrate", help="Optimize signal weights from backtest results")
    p_cal.add_argument("--term", choices=["short", "long"], default="short")
    p_cal.add_argument(
        "--mode", choices=["production", "experimental"], default="production",
        help="Scoring mode for weight optimization",
    )

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
                 "momentum", "quant_alpha", "quant_alpha_v2", "quant_alpha_v3", "random"],
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
    p_runall = subparsers.add_parser("run-all", help="Full pipeline: sentiment -> backtest -> calibrate -> patch -> report-all")
    p_runall.add_argument("--walk-forward", action="store_true", help="Include walk-forward out-of-sample report")
    p_runall.add_argument("--wf-term", choices=["short", "long"], default="short", help="Term for walk-forward analysis (default short)")
    p_runall.add_argument(
        "--mode", choices=["production", "experimental"], default="production",
        help="Scoring mode for backtest + calibration",
    )

    # oos-test (out-of-sample split test)
    p_oos = subparsers.add_parser(
        "oos-test",
        help="Out-of-sample split test: train before split date, test after",
    )
    p_oos.add_argument("--split", default="2024-01-01", help="Split date (default 2024-01-01)")
    p_oos.add_argument("--cost-bps", type=float, default=0.0, help="Round-trip transaction cost in basis points")

    # deepfill
    p_deep = subparsers.add_parser(
        "deepfill",
        help="Enrich sparse coverage with broader queries (assets + macro + sectors)",
    )
    p_deep.add_argument(
        "--min-articles", type=int, default=3,
        help="Target minimum articles per (type, key, date). Dates below this are re-fetched (default 3)",
    )

    # export-data
    subparsers.add_parser(
        "export-data",
        help="Export all backtest data (DB tables + OHLCV/fundamentals/earnings cache) to /reports/backtest_full_export.tar.gz",
    )

    # import-data
    p_import = subparsers.add_parser(
        "import-data",
        help="Import backtest data from a .tar.gz archive produced by export-data",
    )
    p_import.add_argument("--file", default="/reports/backtest_full_export.tar.gz", help="Path to .tar.gz archive")

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
        "oos-test":         cmd_oos_test,
        "deepfill":         cmd_deepfill,
        "export-data":      cmd_export_data,
        "import-data":      cmd_import_data,
    }

    asyncio.run(cmd_map[args.command](args))


if __name__ == "__main__":
    main()
