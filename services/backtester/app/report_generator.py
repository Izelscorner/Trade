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
            <h1>TradeSignal Analysis: {{ strategy|capitalize }} ({{ term|capitalize }}) {{ sentiment_label }}</h1>
            <div class="timestamp">Generated: {{ timestamp }}</div>
        </header>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">
                    Cumulative Return
                    <span class="tooltip">ⓘ<span class="tooltip-text">Total percentage growth of the portfolio over the simulation period. Represents the 'Top Line' growth.</span></span>
                </div>
                <div class="kpi-value success">
                    {{ "%.2f"|format(kpis.cum_return * 100) }}%
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
                    Samples (Grades)
                    <span class="tooltip">ⓘ<span class="tooltip-text">Total number of instrument signal snapshots evaluated in this backtest run.</span></span>
                </div>
                <div class="kpi-value">{{ kpis.n_samples }}</div>
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
            yaxis: { ...darkLayout.yaxis, title: 'Cumulative Return (%)' }
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


async def get_raw_results(term: str = "short") -> pd.DataFrame:
    """Fetch raw backtest grades and returns for analysis."""
    async with async_session() as session:
        # Note: we use return_5d for short term, return_20d for long term
        ret_col = "return_5d" if term == "short" else "return_20d"
        result = await session.execute(
            text(f"""
            SELECT bg.symbol, bg.date, bg.overall_score, br.{ret_col} as return_val
            FROM backtest_grades bg
            JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
            WHERE bg.term = :term
            ORDER BY bg.date ASC
        """),
            {"term": term}
        )
        rows = result.fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows])


def calculate_kpis(df: pd.DataFrame, strategy: str = "portfolio") -> dict:
    """Calculate aggregate performance metrics.
    
    Strategies:
    - 'portfolio': Weighted across all assets based on score.
    - 'top_pick' : Concentrated on the single highest graded asset each day.
    """
    if df.empty:
        return {"cum_return": 0, "sharpe": 0, "win_rate": 0, "n_samples": 0}

    if strategy == "top_pick":
        # Strategy: Pick the highest graded asset for each date
        # (Using idxmax to find index of max score per date group)
        top_idx = df.groupby("date")["overall_score"].idxmax()
        df_strat = df.loc[top_idx].copy()
        # For Top Pick, we assume 100% position in that one asset
        # Note: We still divide by horizon (5 or 20) in attribution if we want daily curve,
        # but for Top Pick "wait and sell" it's simpler to show the trade result on the exit date.
        # However, for the Equity Curve to be continuous, we'll keep the attribution model.
        df_strat["daily_strat_ret"] = df_strat["return_val"] / 20.0 # Approximation
    else:
        # Default: Portfolio Weighted
        df["weight"] = df["overall_score"] / 3.0
        df["daily_strat_ret"] = df["weight"] * (df["return_val"] / 20.0)
        df_strat = df

    daily_rets = df_strat.groupby("date")["daily_strat_ret"].mean()

    # Cumulative return
    total_ret = (1 + daily_rets).prod() - 1
    
    std = daily_rets.std()
    sharpe = (daily_rets.mean() / std * np.sqrt(252)) if std > 0 else 0

    return {
        "cum_return": float(total_ret),
        "sharpe": float(sharpe),
        "win_rate": float((df_strat["daily_strat_ret"] > 0).mean()),
        "n_samples": len(df_strat),
    }


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


def generate_charts(df: pd.DataFrame) -> dict:
    """Generate Plotly chart data as JSON strings."""

    # 1. Equity Curve
    # Uses pre-calculated daily_strat_ret from generate_backtest_report
    daily_rets = df.groupby("date")["daily_strat_ret"].mean()
    cum_ret = (1 + daily_rets).cumprod() - 1
    equity_fig = px.line(x=cum_ret.index, y=cum_ret.values * 100)
    equity_fig.update_traces(line_color="#38bdf8", fill="tozeroy", fillcolor="rgba(56, 189, 248, 0.1)")

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


async def generate_backtest_report(strategy: str = "portfolio", term: str = "short", sentiment_mode: str = "with sentiment") -> str:
    """Run full reporting pipeline and save HTML."""
    logger.info(f"Generating Backtest Report (strategy={strategy}, term={term}, mode={sentiment_mode})...")

    df = await get_raw_results(term=term)
    if df.empty:
        logger.error(f"No results found for term '{term}'. Run backtest first.")
        return ""

    # Ensure no None values
    df = df.dropna(subset=["overall_score", "return_val"])
    df["overall_score"] = df["overall_score"].astype(float)
    df["return_val"] = df["return_val"].astype(float)

    if df.empty:
        logger.warning("No valid samples found.")
        return ""

    kpis = calculate_kpis(df, strategy=strategy)
    inst_stats = generate_instrument_stats(df)
    
    # Recalculate daily_strat_ret for chart generation
    if strategy == "top_pick":
        top_idx = df.groupby("date")["overall_score"].idxmax()
        df_strat = df.loc[top_idx].copy()
        df_strat["daily_strat_ret"] = df_strat["return_val"] / (5.0 if term == "short" else 20.0)
    else:
        df["weight"] = df["overall_score"] / 3.0
        df["daily_strat_ret"] = df["weight"] * (df["return_val"] / (5.0 if term == "short" else 20.0))
        df_strat = df

    chart_data = generate_charts(df_strat)

    from .simulator import COMPOSITE_WEIGHT_PROFILES

    template = Template(HTML_TEMPLATE)
    html = template.render(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        strategy=strategy,
        term=term,
        kpis=kpis,
        instrument_stats=inst_stats,
        charts=chart_data,
        weights=COMPOSITE_WEIGHT_PROFILES,
        sentiment_label=f"({sentiment_mode})" if sentiment_mode else "",
    )

    report_dir = "/app/reports"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir, exist_ok=True)

    mode_slug = sentiment_mode.replace(" ", "_")
    filename = f"report_{strategy}_{term}_{mode_slug}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    filepath = os.path.join(report_dir, filename)
    latest_path = os.path.join(report_dir, f"latest_report_{strategy}_{term}_{mode_slug}.html")

    with open(filepath, "w") as f:
        f.write(html)

    # Link latest
    # (In Docker, symlinks to host mounted volumes can be tricky, so just write twice)
    with open(latest_path, "w") as f:
        f.write(html)

    logger.info(f"✓ Report generated: {filepath}")
    return filepath
