"""Deep quantitative analysis of backtest signals."""

import asyncio
import logging
import warnings

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

from .db import async_session

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(message)s")


async def analyze():
    async with async_session() as s:
        r = await s.execute(text("""
            SELECT bg.symbol, bg.date, bg.term,
                   bg.overall_score,
                   bg.technical_score, bg.technical_conf,
                   bg.sentiment_score, bg.sentiment_conf,
                   bg.macro_score, bg.macro_conf,
                   bg.sector_score, bg.sector_conf,
                   bg.fundamentals_score, bg.fundamentals_conf,
                   COALESCE(bg.vix_score, 0) as vix_score,
                   COALESCE(bg.vix_conf, 0) as vix_conf,
                   COALESCE(bg.momentum_score, 0) as momentum_score,
                   COALESCE(bg.momentum_conf, 0) as momentum_conf,
                   COALESCE(bg.earnings_score, 0) as earnings_score,
                   COALESCE(bg.earnings_conf, 0) as earnings_conf,
                   br.return_5d, br.return_20d,
                   i.category, COALESCE(i.sector, 'none') as sector
            FROM backtest_grades bg
            JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
            JOIN instruments i ON i.id = bg.instrument_id
            WHERE bg.sentiment_mode = 'on'
              AND br.return_5d IS NOT NULL AND br.return_20d IS NOT NULL
            ORDER BY bg.date, bg.symbol
        """))
        rows = r.fetchall()

    cols = [
        'symbol', 'date', 'term', 'overall_score',
        'technical_score', 'technical_conf',
        'sentiment_score', 'sentiment_conf',
        'macro_score', 'macro_conf',
        'sector_score', 'sector_conf',
        'fundamentals_score', 'fundamentals_conf',
        'vix_score', 'vix_conf',
        'momentum_score', 'momentum_conf',
        'earnings_score', 'earnings_conf',
        'return_5d', 'return_20d', 'category', 'sector',
    ]
    df = pd.DataFrame(rows, columns=cols)
    df['date'] = pd.to_datetime(df['date'])
    for c in cols[3:22]:
        df[c] = df[c].astype(float)

    short = df[df['term'] == 'short'].copy()
    long_ = df[df['term'] == 'long'].copy()

    print(f"=== DATASET: {len(short)} short, {len(long_)} long ===")
    print(f"Symbols: {short['symbol'].nunique()}, Range: {short['date'].min().date()} to {short['date'].max().date()}")

    signals = [
        'overall_score', 'technical_score', 'sentiment_score',
        'macro_score', 'sector_score', 'fundamentals_score',
        'vix_score', 'momentum_score', 'earnings_score',
    ]

    # ═══════════════════════════════════════════════════════════════
    # 1. RAW IC
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("1. RAW SIGNAL IC (Spearman with forward returns)")
    print("=" * 80)
    for tn, tdf, rc in [("SHORT 5d", short, "return_5d"), ("LONG 20d", long_, "return_20d")]:
        print(f"\n--- {tn} ---")
        for sig in signals:
            v = tdf[[sig, rc]].dropna()
            ic, pv = stats.spearmanr(v[sig], v[rc])
            m = "***" if pv < 0.001 else "**" if pv < 0.01 else "*" if pv < 0.05 else "ns"
            print(f"  {sig:25s}  IC={ic:+.4f}  p={pv:.1e}  {m}")

    # ═══════════════════════════════════════════════════════════════
    # 2. IC BY CATEGORY
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("2. IC BY CATEGORY")
    print("=" * 80)
    for cat in ["stock", "etf", "commodity"]:
        print(f"\n--- {cat.upper()} ---")
        for tn, tdf, rc in [("5d", short, "return_5d"), ("20d", long_, "return_20d")]:
            cdf = tdf[tdf["category"] == cat]
            print(f"  {tn} (n={len(cdf)}):")
            for sig in signals:
                v = cdf[[sig, rc]].dropna()
                if len(v) < 30:
                    continue
                ic, pv = stats.spearmanr(v[sig], v[rc])
                if abs(ic) > 0.015:
                    m = "***" if pv < 0.001 else "**" if pv < 0.01 else "*" if pv < 0.05 else "ns"
                    tag = " USEFUL" if abs(ic) > 0.05 else ""
                    print(f"    {sig:25s}  IC={ic:+.4f} {m}{tag}")

    # ═══════════════════════════════════════════════════════════════
    # 3. IC BY YEAR
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("3. IC BY YEAR (signal stability)")
    print("=" * 80)
    short["year"] = short["date"].dt.year
    for yr in sorted(short["year"].unique()):
        ydf = short[short["year"] == yr]
        parts = []
        for sig in signals:
            v = ydf[[sig, "return_5d"]].dropna()
            if len(v) > 50:
                ic, _ = stats.spearmanr(v[sig], v["return_5d"])
                parts.append(f"{sig.replace('_score', ''):>12s}={ic:+.3f}")
        print(f"  {yr}: " + "  ".join(parts))

    # ═══════════════════════════════════════════════════════════════
    # 4. DAILY CROSS-SECTIONAL IC
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("4. DAILY CROSS-SECTIONAL IC (the real test)")
    print("=" * 80)
    for tn, tdf, rc in [("SHORT 5d", short, "return_5d"), ("LONG 20d", long_, "return_20d")]:
        print(f"\n--- {tn} ---")
        print(f"  {'Signal':25s} {'Mean IC':>10} {'Std IC':>10} {'IR':>8} {'Hit%':>8}")
        for sig in signals:
            day_ics = []
            for d, grp in tdf.groupby("date"):
                v = grp[[sig, rc]].dropna()
                if len(v) >= 10:
                    ic, _ = stats.spearmanr(v[sig], v[rc])
                    day_ics.append(ic)
            if day_ics:
                m = np.mean(day_ics)
                s = np.std(day_ics)
                ir = m / s if s > 0 else 0
                hr = np.mean([1 if x > 0 else 0 for x in day_ics])
                print(f"  {sig:25s} {m:+10.4f} {s:10.4f} {ir:8.3f} {hr:7.1%}")

    # ═══════════════════════════════════════════════════════════════
    # 5. QUINTILE ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("5. QUINTILE RETURNS (non-linear structure)")
    print("=" * 80)
    for sig in signals:
        for tn, tdf, rc in [("5d", short, "return_5d")]:
            v = tdf[[sig, rc]].dropna()
            try:
                v = v.copy()
                v["q"] = pd.qcut(v[sig], 5, labels=["Q1low", "Q2", "Q3", "Q4", "Q5high"], duplicates="drop")
                qr = v.groupby("q")[rc].agg(["mean", "count"])
                spread = 0
                if "Q5high" in qr.index and "Q1low" in qr.index:
                    spread = qr.loc["Q5high", "mean"] - qr.loc["Q1low", "mean"]
                print(f"\n  {sig} ({tn}) — Q5-Q1 spread: {spread * 100:+.2f}%")
                for q in qr.index:
                    print(f"    {q}: ret={qr.loc[q, 'mean'] * 100:+.3f}%  n={qr.loc[q, 'count']:.0f}")
            except Exception as e:
                print(f"  {sig}: {e}")

    # ═══════════════════════════════════════════════════════════════
    # 6. INTERACTION EFFECTS
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("6. SIGNAL INTERACTIONS (where does combining help?)")
    print("=" * 80)

    for tn, tdf, rc in [("SHORT 5d", short, "return_5d")]:
        tdf = tdf.copy()
        tdf["sent_x_sector"] = tdf["sentiment_score"] * tdf["sector_score"]
        tdf["sent_x_macro"] = tdf["sentiment_score"] * tdf["macro_score"]
        tdf["tech_x_fund"] = tdf["technical_score"] * tdf["fundamentals_score"]
        tdf["sector_x_macro"] = tdf["sector_score"] * tdf["macro_score"]
        tdf["sent_x_fund"] = tdf["sentiment_score"] * tdf["fundamentals_score"]
        tdf["signal_agreement"] = (
            np.sign(tdf["technical_score"])
            + np.sign(tdf["sentiment_score"])
            + np.sign(tdf["sector_score"])
            + np.sign(tdf["macro_score"])
            + np.sign(tdf["fundamentals_score"])
        )
        tdf["abs_agreement"] = tdf["signal_agreement"].abs()
        # Confidence-weighted score
        tdf["conf_weighted"] = (
            tdf["technical_score"] * tdf["technical_conf"]
            + tdf["sentiment_score"] * tdf["sentiment_conf"]
            + tdf["sector_score"] * tdf["sector_conf"]
            + tdf["macro_score"] * tdf["macro_conf"]
            + tdf["fundamentals_score"] * tdf["fundamentals_conf"]
        )
        # Sector relative score (how does this asset rank vs its sector peers?)
        tdf["sector_rank"] = tdf.groupby(["date", "sector"])["overall_score"].rank(pct=True)
        # Cross-sectional z-score
        tdf["zscore"] = tdf.groupby("date")["overall_score"].transform(lambda x: (x - x.mean()) / x.std())

        interactions = [
            "sent_x_sector", "sent_x_macro", "tech_x_fund", "sector_x_macro",
            "sent_x_fund", "signal_agreement", "abs_agreement",
            "conf_weighted", "sector_rank", "zscore",
        ]
        print(f"\n--- {tn} ---")
        for sig in interactions:
            v = tdf[[sig, rc]].dropna()
            ic, pv = stats.spearmanr(v[sig], v[rc])
            m = "***" if pv < 0.001 else "**" if pv < 0.01 else "*" if pv < 0.05 else "ns"
            print(f"  {sig:25s}  IC={ic:+.4f}  p={pv:.1e}  {m}")

    # ═══════════════════════════════════════════════════════════════
    # 7. REGIME ANALYSIS
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("7. REGIME ANALYSIS (signal in high vs low vol?)")
    print("=" * 80)

    for tn, tdf, rc in [("SHORT 5d", short, "return_5d")]:
        daily_vol = tdf.groupby("date")[rc].std()
        high_vol_dates = set(daily_vol[daily_vol > daily_vol.quantile(0.75)].index)
        low_vol_dates = set(daily_vol[daily_vol <= daily_vol.quantile(0.25)].index)

        tdf_hi = tdf[tdf["date"].isin(high_vol_dates)]
        tdf_lo = tdf[tdf["date"].isin(low_vol_dates)]

        print(f"\n  High vol (top 25%, n={len(tdf_hi)}):")
        for sig in signals:
            v = tdf_hi[[sig, rc]].dropna()
            if len(v) > 100:
                ic, _ = stats.spearmanr(v[sig], v[rc])
                print(f"    {sig:25s}  IC={ic:+.4f}")

        print(f"\n  Low vol (bottom 25%, n={len(tdf_lo)}):")
        for sig in signals:
            v = tdf_lo[[sig, rc]].dropna()
            if len(v) > 100:
                ic, _ = stats.spearmanr(v[sig], v[rc])
                print(f"    {sig:25s}  IC={ic:+.4f}")

    # ═══════════════════════════════════════════════════════════════
    # 8. SECTOR-LEVEL IC
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("8. SECTOR-LEVEL IC (where does the signal work?)")
    print("=" * 80)

    for sec in sorted(short["sector"].unique()):
        sdf = short[short["sector"] == sec]
        if len(sdf) < 200:
            continue
        parts = []
        for sig in signals:
            v = sdf[[sig, "return_5d"]].dropna()
            ic, pv = stats.spearmanr(v[sig], v["return_5d"])
            if abs(ic) > 0.02:
                star = "*" if pv < 0.05 else ""
                parts.append(f"{sig.replace('_score', '')}={ic:+.3f}{star}")
        if parts:
            print(f"  {sec:25s} (n={len(sdf)}): {' '.join(parts)}")

    # ═══════════════════════════════════════════════════════════════
    # 9. CONFIDENCE AS META-SIGNAL
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("9. CONFIDENCE AS META-SIGNAL")
    print("=" * 80)

    for tn, tdf, rc in [("SHORT 5d", short, "return_5d")]:
        tdf = tdf.copy()
        tdf["avg_conf"] = (
            tdf["technical_conf"]
            + tdf["sentiment_conf"]
            + tdf["macro_conf"]
            + tdf["sector_conf"]
            + tdf["fundamentals_conf"]
        ) / 5.0
        hi_conf = tdf[tdf["avg_conf"] > tdf["avg_conf"].quantile(0.75)]
        lo_conf = tdf[tdf["avg_conf"] <= tdf["avg_conf"].quantile(0.25)]

        print(f"  High confidence (top 25%, n={len(hi_conf)}):")
        for sig in signals:
            v = hi_conf[[sig, rc]].dropna()
            ic, _ = stats.spearmanr(v[sig], v[rc])
            print(f"    {sig:25s}  IC={ic:+.4f}")

        print(f"  Low confidence (bottom 25%, n={len(lo_conf)}):")
        for sig in signals:
            v = lo_conf[[sig, rc]].dropna()
            ic, _ = stats.spearmanr(v[sig], v[rc])
            print(f"    {sig:25s}  IC={ic:+.4f}")

    # ═══════════════════════════════════════════════════════════════
    # 10. MOMENTUM OF SCORE (does change in score predict returns?)
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("10. SCORE MOMENTUM (does change in score predict?)")
    print("=" * 80)

    for tn, tdf, rc in [("SHORT 5d", short, "return_5d")]:
        tdf = tdf.sort_values(["symbol", "date"])
        for sig in signals:
            tdf[f"{sig}_chg"] = tdf.groupby("symbol")[sig].diff()
            tdf[f"{sig}_chg5"] = tdf.groupby("symbol")[sig].diff(5)

        chg_signals = [f"{s}_chg" for s in signals] + [f"{s}_chg5" for s in signals]
        print(f"\n--- {tn} ---")
        for sig in chg_signals:
            v = tdf[[sig, rc]].dropna()
            if len(v) < 100:
                continue
            ic, pv = stats.spearmanr(v[sig], v[rc])
            m = "***" if pv < 0.001 else "**" if pv < 0.01 else "*" if pv < 0.05 else "ns"
            if abs(ic) > 0.01:
                print(f"  {sig:30s}  IC={ic:+.4f}  {m}")

    # ═══════════════════════════════════════════════════════════════
    # 11. EXTREME SCORES (do very high/low scores predict better?)
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 80)
    print("11. EXTREME SCORES ANALYSIS")
    print("=" * 80)

    for tn, tdf, rc in [("SHORT 5d", short, "return_5d"), ("LONG 20d", long_, "return_20d")]:
        print(f"\n--- {tn} ---")
        for threshold in [1.0, 1.5, 2.0]:
            high = tdf[tdf["overall_score"] > threshold]
            low = tdf[tdf["overall_score"] < -threshold]
            mid = tdf[tdf["overall_score"].abs() <= 0.3]
            print(f"  Score > {threshold}: mean_ret={high[rc].mean() * 100:+.3f}% (n={len(high)})")
            print(f"  Score < -{threshold}: mean_ret={low[rc].mean() * 100:+.3f}% (n={len(low)})")
            print(f"  |Score| <= 0.3:  mean_ret={mid[rc].mean() * 100:+.3f}% (n={len(mid)})")
            if len(high) > 0 and len(low) > 0:
                spread = high[rc].mean() - low[rc].mean()
                print(f"  SPREAD: {spread * 100:+.3f}%")
            print()


def main():
    asyncio.run(analyze())


if __name__ == "__main__":
    main()
