"""Backtest HTML Report Generator.

Fetches data from Postgres, calculates metrics, and generates a premium
interactive HTML dashboard using Plotly and Jinja2.
"""

import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from jinja2 import Template
from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TradeSignal Backtest Report - {{ timestamp }}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-2.24.1.min.js"></script>
    <style>
        :root {
            --bg-color: #0f172a;
            --card-bg: #1e293b;
            --accent-primary: #38bdf8;
            --accent-secondary: #818cf8;
            --text-main: #f1f5f9;
            --text-dim: #94a3b8;
            --border-color: #334155;
            --success: #22c55e;
            --danger: #ef4444;
            --neutral: #64748b;
        }

        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-main);
            margin: 0;
            padding: 0;
            line-height: 1.6;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 2rem;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 3rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1rem;
        }

        h1 {
            font-size: 1.875rem;
            font-weight: 700;
            background: linear-gradient(to right, var(--accent-primary), var(--accent-secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin: 0;
        }

        .timestamp {
            color: var(--text-dim);
            font-size: 0.875rem;
        }

        /* KPI Cards */
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 3rem;
        }

        .kpi-card {
            background-color: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 1rem;
            padding: 1.5rem;
            text-align: center;
            transition: transform 0.2s ease;
        }

        .kpi-card:hover {
            transform: translateY(-4px);
            border-color: var(--accent-primary);
        }

        .kpi-label {
            color: var(--text-dim);
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 0.5rem;
        }

        .kpi-value {
            font-size: 1.5rem;
            font-weight: 700;
        }

        .kpi-value.success { color: var(--success); }
        .kpi-value.danger { color: var(--danger); }

        /* Sections */
        .section {
            background-color: var(--card-bg);
            border: 1px solid var(--border-color);
            border-radius: 1rem;
            padding: 2rem;
            margin-bottom: 2rem;
        }

        .section-title {
            font-size: 1.25rem;
            font-weight: 600;
            margin-bottom: 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .chart-container {
            width: 100%;
            height: 450px;
        }

        /* Tables */
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 1rem;
        }

        th {
            text-align: left;
            color: var(--text-dim);
            font-size: 0.75rem;
            text-transform: uppercase;
            padding: 0.75rem;
            border-bottom: 1px solid var(--border-color);
        }

        td {
            padding: 0.75rem;
            border-bottom: 1px solid var(--border-color);
            font-size: 0.875rem;
        }

        .trend-up { color: var(--success); }
        .trend-down { color: var(--danger); }

        /* Tooltips */
        .tooltip {
            position: relative;
            display: inline-block;
            cursor: help;
            margin-left: 4px;
            color: var(--accent-primary);
            font-size: 0.875rem;
        }

        .tooltip .tooltip-text {
            visibility: hidden;
            width: 240px;
            background-color: var(--card-bg);
            color: var(--text-main);
            text-align: left;
            border: 1px solid var(--accent-primary);
            border-radius: 8px;
            padding: 12px;
            position: absolute;
            z-index: 10;
            bottom: 125%;
            left: 50%;
            margin-left: -120px;
            opacity: 0;
            transition: opacity 0.3s;
            font-size: 0.75rem;
            line-height: 1.4;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.4);
            pointer-events: none;
        }

        .tooltip:hover .tooltip-text {
            visibility: visible;
            opacity: 1;
        }

        /* Metric Guide Table */
        .guide-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1.5rem;
        }

        .guide-item {
            border-left: 2px solid var(--accent-primary);
            padding-left: 1rem;
            margin-bottom: 1rem;
        }

        .guide-term {
            font-weight: 700;
            color: var(--accent-primary);
            font-size: 0.875rem;
            margin-bottom: 0.25rem;
        }

        .guide-desc {
            font-size: 0.8125rem;
            color: var(--text-dim);
        }

        footer {
            text-align: center;
            padding: 2rem;
            color: var(--text-dim);
            font-size: 0.75rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>TradeSignal Analysis: {{ strategy }} {{ sentiment_label }}</h1>
            <div class="timestamp">
                <div>Strategy: {{ strategy_desc }}</div>
                <div>Horizon: {{ horizon }} ({{ term }}-term grades) | Generated: {{ timestamp }}</div>
            </div>
        </header>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">
                    Total Return
                    <span class="tooltip">ⓘ<span class="tooltip-text">Total percentage growth of the portfolio over the simulation period, compounded across all holding periods.</span></span>
                </div>
                <div class="kpi-value success">
                    {{ "%.2f"|format(kpis.cum_return * 100) }}%
                </div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">
                    $1,000 Investment →
                    <span class="tooltip">ⓘ<span class="tooltip-text">If you invested $1,000 at the start of the backtest period, this is your portfolio value at the end.</span></span>
                </div>
                <div class="kpi-value" style="color: {{ 'var(--success)' if kpis.cum_return > 0 else 'var(--danger)' }}">
                    ${{ kpis.final_value }}
                </div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">
                    Sharpe Ratio
                    <span class="tooltip">ⓘ<span class="tooltip-text">Efficiency of profit (Return / Volatility). Higher is better. >1.0 is good, >2.0 is institutional grade.</span></span>
                </div>
                <div class="kpi-value">{{ "%.2f"|format(kpis.sharpe) }}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">
                    Win Rate
                    <span class="tooltip">ⓘ<span class="tooltip-text">Percentage of days with positive return attribution. Confirms consistency of prediction.</span></span>
                </div>
                <div class="kpi-value">{{ "%.1f"|format(kpis.win_rate * 100) }}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">
                    Trade Days / Exposure
                    <span class="tooltip">ⓘ<span class="tooltip-text">Number of days with active positions / percentage of total simulation days with exposure.</span></span>
                </div>
                <div class="kpi-value">{{ kpis.n_trades }} ({{ "%.0f"|format(kpis.exposure_pct) }}%)</div>
            </div>
        </div>

        <div class="kpi-grid" style="grid-template-columns: repeat(3, 1fr);">
            <div class="kpi-card" style="border-color: #f59e0b;">
                <div class="kpi-label">
                    Benchmark Return (Equal-Weight B&H)
                    <span class="tooltip">ⓘ<span class="tooltip-text">What you'd earn by equally weighting ALL instruments each period with no signal — just buy everything. This is what you need to beat.</span></span>
                </div>
                <div class="kpi-value" style="color: #f59e0b;">
                    {{ "%.2f"|format(kpis.bench_return * 100) }}% → ${{ kpis.bench_final }}
                </div>
            </div>
            <div class="kpi-card" style="border-color: #f59e0b;">
                <div class="kpi-label">
                    Benchmark Sharpe
                </div>
                <div class="kpi-value" style="color: #f59e0b;">{{ "%.2f"|format(kpis.bench_sharpe) }}</div>
            </div>
            <div class="kpi-card" style="border-color: {{ 'var(--success)' if kpis.alpha > 0 else 'var(--danger)' }};">
                <div class="kpi-label">
                    Alpha (Strategy − Benchmark)
                    <span class="tooltip">ⓘ<span class="tooltip-text">Extra return generated by the scoring signal vs. naive equal-weight. Positive = signal adds value. Negative = you're better off buying everything equally.</span></span>
                </div>
                <div class="kpi-value {{ 'success' if kpis.alpha > 0 else 'danger' }}">
                    {{ "%+.2f"|format(kpis.alpha * 100) }}%
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Equity Curve (Composite Strategy)</div>
            <div id="chart-equity" class="chart-container"></div>
        </div>

        <div class="kpi-grid" style="grid-template-columns: 1fr 1fr;">
            <div class="section">
                <div class="section-title">Signal Quality (Quintile Analysis)</div>
                <div id="chart-quintiles" class="chart-container" style="height: 350px;"></div>
            </div>
            <div class="section">
                <div class="section-title">Score Distribution</div>
                <div id="chart-dist" class="chart-container" style="height: 350px;"></div>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Investment Weight Profiles (Nominal)</div>
            <p style="color: var(--text-dim); font-size: 0.875rem; margin-bottom: 1rem;">
                These are the base weights used for each category. Final results may vary based on confidence-adjustment logic.
            </p>
            <table>
                <thead>
                    <tr>
                        <th>Category</th>
                        <th>Technical</th>
                        <th>Sentiment</th>
                        <th>Sector</th>
                        <th>Macro</th>
                        <th>Fundamentals</th>
                    </tr>
                </thead>
                <tbody>
                    {% for cat, profiles in weights.items() %}
                    <tr>
                        <td><strong>{{ cat|capitalize }}</strong></td>
                        <td>{{ "%.0f"|format(profiles[term].technical * 100) }}%</td>
                        <td>{{ "%.0f"|format(profiles[term].sentiment * 100) }}%</td>
                        <td>{{ "%.0f"|format(profiles[term].sector * 100) }}%</td>
                        <td>{{ "%.0f"|format(profiles[term].macro * 100) }}%</td>
                        <td>{{ "%.0f"|format(profiles[term].fundamentals * 100) }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="section">
            <div class="section-title">
                Instrument Performance ({{ "Forward 20D" if term == "long" else "Forward 5D" }})
                <span class="tooltip">ⓘ<span class="tooltip-text">Success of the grading system broken down by specific tickers. Use this to find 'blind spots' in the logic.</span></span>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Mean Score</th>
                        <th>Avg Return</th>
                        <th>
                            Info. Coeff.
                            <span class="tooltip">ⓘ<span class="tooltip-text">Information Coefficient: Correlation between the grade and future return (-1 to 1). Highly positive means the grades 'predicted' the moves.</span></span>
                        </th>
                        <th>Win Rate</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in instrument_stats %}
                    <tr>
                        <td><strong>{{ row.symbol }}</strong></td>
                        <td>{{ "%.2f"|format(row.avg_score) }}</td>
                        <td class="{{ 'trend-up' if row.avg_return > 0 else 'trend-down' }}">
                            {{ "%.2f"|format(row.avg_return * 100) }}%
                        </td>
                        <td>{{ "%.3f"|format(row.ic) }}</td>
                        <td>{{ "%.1f"|format(row.win_rate * 100) }}%</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="section">
            <div class="section-title">
                Trade Log ($1,000 Starting Capital)
                <span class="tooltip">ⓘ<span class="tooltip-text">Chronological record of every trade. Shows how $1,000 compounds through each {{ "5" if term == "short" else "20" }}-day holding period.</span></span>
            </div>
            <div style="max-height: 600px; overflow-y: auto;">
            <table>
                <thead>
                    <tr>
                        <th>#</th>
                        <th>Entry Date</th>
                        <th>Instrument</th>
                        <th>Score</th>
                        <th>Return</th>
                        <th>Invested</th>
                        <th>Returned</th>
                        <th>P&L</th>
                    </tr>
                </thead>
                <tbody>
                    {% for trade in trade_log %}
                    <tr>
                        <td style="color: var(--text-dim);">{{ loop.index }}</td>
                        <td>{{ trade.date }}</td>
                        <td><strong>{{ trade.symbol }}</strong></td>
                        <td>{{ "%.2f"|format(trade.score) }}</td>
                        <td class="{{ 'trend-up' if trade.return_pct > 0 else 'trend-down' }}">
                            {{ "%+.2f"|format(trade.return_pct) }}%
                        </td>
                        <td>${{ trade.invested }}</td>
                        <td>${{ trade.returned }}</td>
                        <td class="{{ 'trend-up' if trade.pnl > 0 else 'trend-down' }}">
                            {{ trade.pnl_fmt }}$
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
            </div>
        </div>

        <div class="section">
            <div class="section-title">Metric Guide & Legend</div>
            <div class="guide-grid">
                <div class="guide-item">
                    <div class="guide-term">Sharpe Ratio</div>
                    <div class="guide-desc">The primary measure of risk-adjusted performance. A ratio of 4.6 means the strategy earned 4.6 units of return for every unit of "volatility" (risk) taken. It proves the consistency of the algorithm.</div>
                </div>
                <div class="guide-item">
                    <div class="guide-term">Information Coefficient (IC)</div>
                    <div class="guide-desc">The Spearman correlation between our scores and forward returns. An IC > 0.05 is considered very high alpha for quantitative trading models.</div>
                </div>
                <div class="guide-item">
                    <div class="guide-term">Win Rate</div>
                    <div class="guide-desc">How often the Dailyized Attribution was positive. In a 'Top Pick' strategy, this reflects the accuracy of the #1 ranked asset across all days.</div>
                </div>
                <div class="guide-item">
                    <div class="guide-term">Nominal Weights</div>
                    <div class="guide-desc">The 'Base' importance of each signal. Note that these are further adjusted in real-time by 'Confidence' which measures data availability/quality.</div>
                </div>
            </div>
        </div>

        <footer>
            TradeSignal Backtester | Production-Faithful Simulation Engine
        </footer>
    </div>

    <script>
        const darkLayout = {
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            font: { color: '#94a3b8', family: 'Inter' },
            xaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155' },
            yaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155' },
            margin: { t: 20, r: 20, l: 40, b: 40 }
        };

        // Equity Chart
        const equityData = {{ charts.equity_data | safe }};
        Plotly.newPlot('chart-equity', equityData.data, {
            ...darkLayout,
            xaxis: { ...darkLayout.xaxis, title: 'Simulation Date' },
            yaxis: { ...darkLayout.yaxis, title: 'Total Return (%)' }
        });

        // Quintile Chart
        const quintileData = {{ charts.quintile_data | safe }};
        Plotly.newPlot('chart-quintiles', quintileData.data, {
            ...darkLayout,
            xaxis: { ...darkLayout.xaxis, title: 'Overall Score Quintile' },
            yaxis: { ...darkLayout.yaxis, title: 'Avg Forward Return' }
        });

        // Score Distribution
        const distData = {{ charts.dist_data | safe }};
        Plotly.newPlot('chart-dist', distData.data, {
            ...darkLayout,
            xaxis: { ...darkLayout.xaxis, title: 'Overall Score' },
            yaxis: { ...darkLayout.yaxis, title: 'Frequency' }
        });
    </script>
</body>
</html>
"""


async def get_raw_results(term: str = "short", sentiment_mode: str = "on") -> pd.DataFrame:
    """Fetch raw backtest grades and returns for analysis."""
    async with async_session() as session:
        # Note: we use return_5d for short term, return_20d for long term
        ret_col = "return_5d" if term == "short" else "return_20d"
        result = await session.execute(
            text(f"""
            SELECT bg.symbol, bg.date, bg.overall_score, br.{ret_col} as return_val,
                   i.category, i.sector
            FROM backtest_grades bg
            JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
            JOIN instruments i ON i.id = bg.instrument_id
            WHERE bg.term = :term AND bg.sentiment_mode = :smode
            ORDER BY bg.date ASC
        """),
            {"term": term, "smode": sentiment_mode}
        )
        rows = result.fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows])


def calculate_kpis(df: pd.DataFrame, term: str = "short") -> dict:
    """Calculate aggregate performance metrics from a strategy-processed DataFrame.

    Expects `daily_strat_ret` column already computed by apply_strategy().
    With non-overlapping periods, each entry date has a full period return.
    """
    if df.empty or "daily_strat_ret" not in df.columns:
        return {"cum_return": 0, "final_value": "1,000", "sharpe": 0, "win_rate": 0, "n_samples": 0, "n_trades": 0, "exposure_pct": 0}

    # Sum returns per date (multiple instruments may contribute)
    period_rets = df.groupby("date")["daily_strat_ret"].sum()

    # Only entry dates have non-zero returns
    active_rets = period_rets[period_rets != 0]
    n_trades = len(active_rets)
    n_total_periods = len(period_rets)

    # Cumulative return: compound period returns
    total_ret = float((1 + active_rets).prod() - 1) if n_trades > 0 else 0.0

    # Sharpe: annualize based on periods per year
    # Short-term (5-day hold) = ~52 periods/year, Long-term (20-day hold) = ~13 periods/year
    periods_per_year = 52.0 if term == "short" else 13.0
    if n_trades > 1:
        mean_ret = float(active_rets.mean())
        std_ret = float(active_rets.std())
        sharpe = (mean_ret / std_ret * np.sqrt(periods_per_year)) if std_ret > 0 else 0.0
    else:
        sharpe = 0.0

    final_val = 1000 * (1 + total_ret)
    return {
        "cum_return": total_ret,
        "final_value": f"{final_val:,.0f}",
        "sharpe": float(sharpe),
        "win_rate": float((active_rets > 0).mean()) if n_trades > 0 else 0.0,
        "n_samples": len(df),
        "n_trades": n_trades,
        "exposure_pct": float(n_trades / n_total_periods * 100) if n_total_periods > 0 else 0.0,
    }


def generate_trade_log(df: pd.DataFrame, term: str = "short", initial_investment: float = 1000.0) -> list[dict]:
    """Generate a chronological trade log showing each position with compounding portfolio value.

    For strategies that invest in multiple instruments per period (e.g., portfolio, top_n),
    all instruments for that entry date are shown as a group.
    """
    holding_days = 5 if term == "short" else 20

    # Get per-instrument returns on entry dates (non-zero daily_strat_ret)
    trades_df = df[df["daily_strat_ret"] != 0][["date", "symbol", "overall_score", "return_val", "daily_strat_ret"]].copy()
    if trades_df.empty:
        return []

    trades_df = trades_df.sort_values("date")

    # Sum returns per date to get the period return (handles multi-instrument strategies)
    period_rets = trades_df.groupby("date")["daily_strat_ret"].sum().sort_index()

    # Build compounding portfolio value series
    portfolio_value = initial_investment
    date_to_invested = {}
    date_to_returned = {}
    for d, period_ret in period_rets.items():
        date_to_invested[d] = portfolio_value
        portfolio_value = portfolio_value * (1 + period_ret)
        date_to_returned[d] = portfolio_value

    # Build trade log entries
    log = []
    for _, row in trades_df.iterrows():
        d = row["date"]
        invested = date_to_invested.get(d, 0)
        returned = date_to_returned.get(d, 0)
        pnl = returned - invested
        log.append({
            "date": d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d),
            "symbol": row["symbol"],
            "score": float(row["overall_score"]),
            "return_pct": float(row["return_val"] * 100),
            "invested": f"{invested:,.2f}",
            "returned": f"{returned:,.2f}",
            "pnl": pnl,
            "pnl_fmt": f"{pnl:+,.2f}",
            "holding_days": holding_days,
        })

    return log


def generate_instrument_stats(df: pd.DataFrame) -> list[dict]:
    """Calculate stats per instrument."""
    from scipy.stats import spearmanr

    stats = []
    for sym, group in df.groupby("symbol"):
        ic, _ = spearmanr(group["overall_score"], group["return_val"])
        stats.append(
            {
                "symbol": sym,
                "avg_score": float(group["overall_score"].mean()),
                "avg_return": float(group["return_val"].mean()),
                "ic": float(ic) if not np.isnan(ic) else 0.0,
                "win_rate": float((group["overall_score"] * group["return_val"] > 0).mean()),
            }
        )
    return sorted(stats, key=lambda x: x["ic"], reverse=True)


def compute_benchmark(df: pd.DataFrame, term: str = "short") -> pd.Series:
    """Compute equal-weight buy-and-hold benchmark using same non-overlapping periods.

    At each entry date, equal-weight all instruments (1/N).
    This represents "no signal, just buy everything equally".
    """
    from .strategies import _get_entry_dates, _holding_period

    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    benchmark_rets = {}
    for entry_date in entry_dates:
        day_df = df[df["date"] == entry_date]
        if day_df.empty:
            continue
        # Equal weight across all instruments
        avg_ret = day_df["return_val"].mean()
        benchmark_rets[entry_date] = avg_ret

    bench_series = pd.Series(benchmark_rets).sort_index()
    return bench_series


def generate_charts(df: pd.DataFrame, term: str = "short") -> dict:
    """Generate Plotly chart data as JSON strings."""

    # 1. Equity Curve (strategy + benchmark)
    # Sum returns per date (non-overlapping periods), only compound active dates
    period_rets = df.groupby("date")["daily_strat_ret"].sum()
    active_rets = period_rets[period_rets != 0]
    cum_ret = (1 + active_rets).cumprod() - 1

    # Benchmark: equal-weight buy-and-hold
    bench_rets = compute_benchmark(df, term=term)
    bench_cum = (1 + bench_rets).cumprod() - 1

    equity_fig = go.Figure()
    equity_fig.add_trace(go.Scatter(
        x=cum_ret.index, y=cum_ret.values * 100,
        mode="lines", name="Strategy",
        line=dict(color="#38bdf8", width=2),
        fill="tozeroy", fillcolor="rgba(56, 189, 248, 0.1)",
    ))
    equity_fig.add_trace(go.Scatter(
        x=bench_cum.index, y=bench_cum.values * 100,
        mode="lines", name="Benchmark (Equal-Weight B&H)",
        line=dict(color="#f59e0b", width=2, dash="dash"),
    ))

    # 2. Quintiles
    df["quintile"] = pd.qcut(df["overall_score"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
    q_stats = df.groupby("quintile")["return_val"].mean()
    quintile_fig = px.bar(x=q_stats.index, y=q_stats.values, color=q_stats.values, color_continuous_scale="RdYlGn")

    # 3. Distribution
    dist_fig = px.histogram(df, x="overall_score", nbins=30)
    dist_fig.update_traces(marker_color="#818cf8")

    return {
        "equity_data": equity_fig.to_json(),
        "quintile_data": quintile_fig.to_json(),
        "dist_data": dist_fig.to_json(),
    }


async def generate_backtest_report(
    strategy: str = "portfolio",
    term: str = "short",
    sentiment_mode: str = "with sentiment",
    strategy_params: "StrategyParams | None" = None,
) -> dict:
    """Run full reporting pipeline and save HTML.

    Returns dict with filepath and KPIs for summary table.
    """
    from .strategies import STRATEGIES, StrategyParams, apply_strategy

    if strategy_params is None:
        strategy_params = StrategyParams()

    strategy_desc = STRATEGIES.get(strategy, {}).get("desc", strategy)
    horizon = "5-day hold" if term == "short" else "20-day hold"

    logger.info(
        "Generating report: strategy=%s (%s), term=%s (%s), mode=%s",
        strategy, strategy_desc, term, horizon, sentiment_mode,
    )

    db_smode = "off" if "without" in sentiment_mode else "on"
    df = await get_raw_results(term=term, sentiment_mode=db_smode)
    if df.empty:
        logger.error(f"No results found for term '{term}', sentiment_mode='{db_smode}'. Run backtest first.")
        return {"filepath": "", "kpis": {}}

    # Ensure no None values
    df = df.dropna(subset=["overall_score", "return_val"])
    df["overall_score"] = df["overall_score"].astype(float)
    df["return_val"] = df["return_val"].astype(float)

    if df.empty:
        logger.warning("No valid samples found.")
        return {"filepath": "", "kpis": {}}

    # Apply strategy
    df_strat = apply_strategy(df, strategy, term, strategy_params)

    kpis = calculate_kpis(df_strat, term=term)
    inst_stats = generate_instrument_stats(df)
    chart_data = generate_charts(df_strat, term=term)
    trade_log = generate_trade_log(df_strat, term=term)

    # Benchmark KPIs
    bench_rets = compute_benchmark(df_strat, term=term)
    bench_total = float((1 + bench_rets).prod() - 1) if len(bench_rets) > 0 else 0.0
    bench_final = 1000 * (1 + bench_total)
    periods_per_year = 52.0 if term == "short" else 13.0
    if len(bench_rets) > 1:
        bench_sharpe = float(bench_rets.mean() / bench_rets.std() * np.sqrt(periods_per_year)) if bench_rets.std() > 0 else 0.0
    else:
        bench_sharpe = 0.0
    alpha_ret = kpis["cum_return"] - bench_total
    kpis["bench_return"] = bench_total
    kpis["bench_final"] = f"{bench_final:,.0f}"
    kpis["bench_sharpe"] = bench_sharpe
    kpis["alpha"] = alpha_ret

    from .simulator import COMPOSITE_WEIGHT_PROFILES

    template = Template(HTML_TEMPLATE)
    html = template.render(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        strategy=strategy,
        strategy_desc=strategy_desc,
        term=term,
        horizon=horizon,
        kpis=kpis,
        instrument_stats=inst_stats,
        charts=chart_data,
        trade_log=trade_log,
        weights=COMPOSITE_WEIGHT_PROFILES,
        sentiment_label=f"({sentiment_mode})" if sentiment_mode else "",
        params=strategy_params,
    )

    report_dir = os.environ.get("REPORT_DIR", "/reports")
    if not os.path.exists(report_dir):
        os.makedirs(report_dir, exist_ok=True)

    mode_slug = sentiment_mode.replace(" ", "_")
    horizon_slug = "5day" if term == "short" else "20day"
    filename = f"report_{strategy}_{horizon_slug}_{term}_{mode_slug}.html"
    filepath = os.path.join(report_dir, filename)

    with open(filepath, "w") as f:
        f.write(html)

    logger.info(f"Report generated: {filepath}")
    return {
        "filepath": filepath,
        "strategy": strategy,
        "term": term,
        "horizon": horizon,
        "sentiment_mode": sentiment_mode,
        "kpis": kpis,
    }
