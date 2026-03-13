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
            <h1>TradeSignal Analysis Details</h1>
            <div class="timestamp">Generated: {{ timestamp }}</div>
        </header>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Cumulative Return</div>
                <div class="kpi-value {{ 'success' if kpis.total_return > 0 else 'danger' }}">
                    {{ "%.2f"|format(kpis.total_return * 100) }}%
                </div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Sharpe Ratio</div>
                <div class="kpi-value">{{ "%.2f"|format(kpis.sharpe) }}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Win Rate</div>
                <div class="kpi-value">{{ "%.1f"|format(kpis.win_rate * 100) }}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Samples (Grades)</div>
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
            <div class="section-title">Instrument Performance (Forward 20D)</div>
            <table>
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Mean Score</th>
                        <th>Avg 20D Return</th>
                        <th>Information Coeff.</th>
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


async def get_raw_results() -> pd.DataFrame:
    """Fetch raw backtest grades and returns for analysis."""
    async with async_session() as session:
        result = await session.execute(
            text("""
            SELECT bg.symbol, bg.date, bg.overall_score, br.return_20d, br.return_5d
            FROM backtest_grades bg
            JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
            WHERE bg.term = 'short'
            ORDER BY bg.date ASC
        """)
        )
        rows = result.fetchall()
    return pd.DataFrame([dict(r._mapping) for r in rows])


def calculate_kpis(df: pd.DataFrame) -> dict:
    """Calculate aggregate performance metrics."""
    if df.empty:
        return {"total_return": 0, "sharpe": 0, "win_rate": 0, "n_samples": 0}

    # Simplified strategy: long if score > 0.5, short if score < -0.5 (scaled by score)
    df["strat_ret"] = df["overall_score"] * df["return_20d"]
    daily_rets = df.groupby("date")["strat_ret"].mean()

    total_ret = (1 + daily_rets).prod() - 1
    sharpe = (daily_rets.mean() / daily_rets.std() * np.sqrt(52)) if daily_rets.std() != 0 else 0

    return {
        "total_return": float(total_ret),
        "sharpe": float(sharpe),
        "win_rate": float((df["strat_ret"] > 0).mean()),
        "n_samples": len(df),
    }


def generate_instrument_stats(df: pd.DataFrame) -> list[dict]:
    """Calculate stats per instrument."""
    from scipy.stats import spearmanr

    stats = []
    for sym, group in df.groupby("symbol"):
        ic, _ = spearmanr(group["overall_score"], group["return_20d"])
        stats.append(
            {
                "symbol": sym,
                "avg_score": float(group["overall_score"].mean()),
                "avg_return": float(group["return_20d"].mean()),
                "ic": float(ic) if not np.isnan(ic) else 0.0,
                "win_rate": float((group["overall_score"] * group["return_20d"] > 0).mean()),
            }
        )
    return sorted(stats, key=lambda x: x["ic"], reverse=True)


def generate_charts(df: pd.DataFrame) -> dict:
    """Generate Plotly chart data as JSON strings."""

    # 1. Equity Curve
    daily_rets = df.groupby("date")["strat_ret"].mean()
    cum_ret = (1 + daily_rets).cumprod() - 1
    equity_fig = px.line(x=cum_ret.index, y=cum_ret.values * 100)
    equity_fig.update_traces(line_color="#38bdf8", fill="tozeroy", fillcolor="rgba(56, 189, 248, 0.1)")

    # 2. Quintiles
    df["quintile"] = pd.qcut(df["overall_score"], 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"])
    q_stats = df.groupby("quintile")["return_20d"].mean()
    quintile_fig = px.bar(x=q_stats.index, y=q_stats.values, color=q_stats.values, color_continuous_scale="RdYlGn")

    # 3. Distribution
    dist_fig = px.histogram(df, x="overall_score", nbins=30)
    dist_fig.update_traces(marker_color="#818cf8")

    return {
        "equity_data": equity_fig.to_json(),
        "quintile_data": quintile_fig.to_json(),
        "dist_data": dist_fig.to_json(),
    }


async def generate_backtest_report() -> str:
    """Run full reporting pipeline and save HTML."""
    logger.info("Generating Backtest Report...")

    df = await get_raw_results()
    if df.empty:
        logger.error("No backtest results found in DB. Run backtest first.")
        return ""

    # Ensure no None values in critical columns for calculation
    df = df.dropna(subset=["overall_score", "return_20d"])

    # Convert Decimals to float if any (SQLAlchemy might return Decimals)
    df["overall_score"] = df["overall_score"].astype(float)
    df["return_20d"] = df["return_20d"].astype(float)

    if df.empty:
        logger.warning("No valid samples with both scores and forward returns found.")
        return ""

    # Calculate strategy returns for equity curve
    df["strat_ret"] = df["overall_score"] * df["return_20d"]

    kpis = calculate_kpis(df)
    inst_stats = generate_instrument_stats(df)
    chart_data = generate_charts(df)

    template = Template(HTML_TEMPLATE)
    html = template.render(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        kpis=kpis,
        instrument_stats=inst_stats,
        charts=chart_data,
    )

    report_dir = "/app/reports"
    if not os.path.exists(report_dir):
        os.makedirs(report_dir, exist_ok=True)

    filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    filepath = os.path.join(report_dir, filename)
    latest_path = os.path.join(report_dir, "latest_report.html")

    with open(filepath, "w") as f:
        f.write(html)

    # Link latest
    # (In Docker, symlinks to host mounted volumes can be tricky, so just write twice)
    with open(latest_path, "w") as f:
        f.write(html)

    logger.info(f"✓ Report generated: {filepath}")
    return filepath
