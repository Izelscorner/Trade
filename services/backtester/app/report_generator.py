"""Backtest HTML Report Generator.

Generates multi-variation strategy reports (4 variations each) and a
cross-strategy summary report. Each strategy report compares:
  - Short-term + Sentiment
  - Short-term (No Sentiment)
  - Long-term + Sentiment
  - Long-term (No Sentiment)

The summary report ranks all strategies by their best variation.
"""

import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from jinja2 import Template
from sqlalchemy import text

from .db import async_session

logger = logging.getLogger(__name__)

# The 4 variations every strategy is evaluated against
VARIATIONS = [
    {"term": "short", "smode": "on",  "label": "Short-Term + Sentiment",     "slug": "short_sentiment"},
    {"term": "short", "smode": "off", "label": "Short-Term (No Sentiment)",   "slug": "short_no_sentiment"},
    {"term": "long",  "smode": "on",  "label": "Long-Term + Sentiment",      "slug": "long_sentiment"},
    {"term": "long",  "smode": "off", "label": "Long-Term (No Sentiment)",    "slug": "long_no_sentiment"},
]

VARIATION_COLORS = ["#38bdf8", "#818cf8", "#22c55e", "#f472b6"]

# ---------------------------------------------------------------------------
# Shared CSS used across all report templates
# ---------------------------------------------------------------------------
SHARED_CSS = """
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
    --best-glow: rgba(34, 197, 94, 0.15);
}
body {
    font-family: 'Inter', sans-serif;
    background-color: var(--bg-color);
    color: var(--text-main);
    margin: 0; padding: 0; line-height: 1.6;
}
.container { max-width: 1300px; margin: 0 auto; padding: 2rem; }
header {
    display: flex; justify-content: space-between; align-items: center;
    margin-bottom: 2rem; border-bottom: 1px solid var(--border-color); padding-bottom: 1rem;
}
h1 {
    font-size: 1.875rem; font-weight: 700;
    background: linear-gradient(to right, var(--accent-primary), var(--accent-secondary));
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin: 0;
}
h2 { font-size: 1.25rem; font-weight: 600; margin-top: 2rem; margin-bottom: 1rem; }
.timestamp { color: var(--text-dim); font-size: 0.875rem; }
.section {
    background-color: var(--card-bg); border: 1px solid var(--border-color);
    border-radius: 1rem; padding: 2rem; margin-bottom: 2rem;
}
.section-title {
    font-size: 1.25rem; font-weight: 600; margin-bottom: 1.5rem;
    display: flex; align-items: center; gap: 0.5rem;
}
.chart-container { width: 100%; height: 450px; }
table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
th {
    text-align: left; color: var(--text-dim); font-size: 0.75rem;
    text-transform: uppercase; padding: 0.75rem;
    border-bottom: 1px solid var(--border-color);
}
td { padding: 0.75rem; border-bottom: 1px solid var(--border-color); font-size: 0.875rem; }
.trend-up { color: var(--success); }
.trend-down { color: var(--danger); }
.best-row { background-color: var(--best-glow); }
.best-badge {
    display: inline-block; background: var(--success); color: #000;
    font-size: 0.625rem; font-weight: 700; padding: 2px 8px;
    border-radius: 4px; text-transform: uppercase; margin-left: 8px;
}
.kpi-grid {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 1rem; margin-bottom: 2rem;
}
.kpi-card {
    background-color: var(--card-bg); border: 1px solid var(--border-color);
    border-radius: 1rem; padding: 1.25rem; text-align: center;
}
.kpi-label {
    color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;
    letter-spacing: 0.05em; margin-bottom: 0.25rem;
}
.kpi-value { font-size: 1.25rem; font-weight: 700; }
.kpi-value.success { color: var(--success); }
.kpi-value.danger { color: var(--danger); }
.tooltip { position: relative; display: inline-block; cursor: help; margin-left: 4px; color: var(--accent-primary); font-size: 0.875rem; }
.tooltip .tooltip-text {
    visibility: hidden; width: 240px; background-color: var(--card-bg);
    color: var(--text-main); text-align: left; border: 1px solid var(--accent-primary);
    border-radius: 8px; padding: 12px; position: absolute; z-index: 10;
    bottom: 125%; left: 50%; margin-left: -120px; opacity: 0;
    transition: opacity 0.3s; font-size: 0.75rem; line-height: 1.4;
    box-shadow: 0 10px 15px -3px rgba(0,0,0,0.4); pointer-events: none;
}
.tooltip:hover .tooltip-text { visibility: visible; opacity: 1; }
details { margin-bottom: 1rem; }
details summary {
    cursor: pointer; padding: 0.75rem; background: var(--card-bg);
    border: 1px solid var(--border-color); border-radius: 0.5rem;
    color: var(--text-main); font-weight: 600;
}
details[open] summary { border-radius: 0.5rem 0.5rem 0 0; }
details .detail-body {
    padding: 1rem; border: 1px solid var(--border-color);
    border-top: none; border-radius: 0 0 0.5rem 0.5rem;
}
footer { text-align: center; padding: 2rem; color: var(--text-dim); font-size: 0.75rem; }
/* Navigation bar */
.site-nav {
    position: sticky; top: 0; z-index: 100;
    background: rgba(15, 23, 42, 0.95); backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--border-color);
    padding: 0.5rem 2rem; display: flex; align-items: center; gap: 1.5rem;
    font-size: 0.8rem; flex-wrap: wrap;
}
.site-nav a {
    color: var(--text-dim); text-decoration: none; padding: 0.4rem 0.75rem;
    border-radius: 0.375rem; transition: all 0.15s;
}
.site-nav a:hover { color: var(--text-main); background: var(--card-bg); }
.site-nav a.nav-active { color: var(--accent-primary); background: rgba(56,189,248,0.1); font-weight: 600; }
.site-nav .nav-brand {
    color: var(--accent-primary); font-weight: 700; font-size: 0.85rem;
    margin-right: 0.5rem; text-decoration: none; padding: 0;
}
.site-nav .nav-sep { color: var(--border-color); user-select: none; }
"""

def _build_nav_html(
    active: str = "",
    strategy_names: list[str] | None = None,
    include_walk_forward: bool = False,
) -> str:
    """Build the sticky navigation bar HTML.

    Args:
        active: The current page identifier (e.g. 'summary', 'walk_forward', or a strategy name).
        strategy_names: List of strategy names to include as links.
        include_walk_forward: Whether to include a walk-forward link.
    """
    links = []
    # Home / summary
    cls = ' class="nav-active"' if active == "summary" else ""
    links.append(f'<a href="index.html"{cls}>Summary</a>')

    if strategy_names:
        links.append('<span class="nav-sep">|</span>')
        for name in strategy_names:
            cls = ' class="nav-active"' if active == name else ""
            links.append(f'<a href="report_{name}.html"{cls}>{name.replace("_", " ").title()}</a>')

    if include_walk_forward:
        links.append('<span class="nav-sep">|</span>')
        cls = ' class="nav-active"' if active == "walk_forward" else ""
        links.append(f'<a href="report_walk_forward.html"{cls}>Walk-Forward</a>')

    return '<nav class="site-nav"><a class="nav-brand" href="index.html">TradeSignal Backtester</a>' + "".join(links) + "</nav>"


# ---------------------------------------------------------------------------
# Multi-Variation Strategy Report Template
# ---------------------------------------------------------------------------
STRATEGY_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ strategy }} Strategy Report - {{ timestamp }}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-2.24.1.min.js"></script>
    <style>{{ css }}</style>
</head>
<body>
{{ nav_html | safe }}
<div class="container">
    <header>
        <h1>{{ strategy_name }}</h1>
        <div class="timestamp">
            <div>{{ strategy_desc }}</div>
            <div>Backtest Period: {{ backtest_start }} to {{ backtest_end }} | 4-Variation Comparison | Generated: {{ timestamp }}</div>
        </div>
    </header>

    <!-- Strategy Guide -->
    {% if guide %}
    <div class="section" style="border-color: var(--accent-primary);">
        <div class="section-title" style="color: var(--accent-primary);">How This Strategy Works</div>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; margin-bottom: 1rem;">
            <div>
                <div style="color: var(--accent-primary); font-weight: 600; font-size: 0.8rem; text-transform: uppercase; margin-bottom: 0.5rem;">The Idea</div>
                <p style="color: var(--text-dim); font-size: 0.875rem; line-height: 1.7; margin: 0;">{{ guide.how_it_works }}</p>
            </div>
            <div>
                <div style="color: var(--accent-primary); font-weight: 600; font-size: 0.8rem; text-transform: uppercase; margin-bottom: 0.5rem;">What to Watch on Dashboard</div>
                <p style="color: var(--text-dim); font-size: 0.875rem; line-height: 1.7; margin: 0;">{{ guide.what_to_watch }}</p>
            </div>
        </div>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem;">
            <div>
                <div style="color: var(--accent-primary); font-weight: 600; font-size: 0.8rem; text-transform: uppercase; margin-bottom: 0.5rem;">How to Pick Assets Each Period</div>
                <p style="color: var(--text-dim); font-size: 0.875rem; line-height: 1.7; margin: 0;">{{ guide.how_to_pick }}</p>
            </div>
            <div>
                <div style="color: var(--accent-primary); font-weight: 600; font-size: 0.8rem; text-transform: uppercase; margin-bottom: 0.5rem;">Best For</div>
                <p style="color: var(--text-dim); font-size: 0.875rem; line-height: 1.7; margin: 0;">{{ guide.best_for }}</p>
            </div>
        </div>
    </div>
    {% endif %}

    <!-- KPI Explainer -->
    <div class="section">
        <details>
            <summary style="font-size: 0.9rem;">Understanding the Numbers (click to expand)</summary>
            <div class="detail-body" style="color: var(--text-dim); font-size: 0.85rem; line-height: 1.8;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                    <div><strong style="color: var(--text-main);">Total Return</strong> — How much your $1,000 grew (or shrank) over the entire backtest period. +100% means it doubled.</div>
                    <div><strong style="color: var(--text-main);">$1K &rarr;</strong> — What $1,000 invested at the start would be worth now. Shows the actual dollar outcome.</div>
                    <div><strong style="color: var(--text-main);">Sharpe Ratio</strong> — Return per unit of risk. Above 1.0 is good, above 2.0 is excellent. A high return with wild swings scores lower than a steady return.</div>
                    <div><strong style="color: var(--text-main);">Alpha</strong> — How much this strategy beat (or trailed) a simple equal-weight benchmark. Positive alpha = the scoring system added value.</div>
                    <div><strong style="color: var(--text-main);">Win Rate</strong> — Percentage of trading periods that made money. 60%+ is solid. Even great strategies lose 40% of the time.</div>
                    <div><strong style="color: var(--text-main);">Benchmark</strong> — The return from blindly buying all assets equally. Your strategy should beat this, otherwise just buy an index.</div>
                    <div><strong style="color: var(--text-main);">Trades (Exposure %)</strong> — Number of trading periods with active positions. 100% exposure = always invested, lower = sometimes in cash.</div>
                    <div><strong style="color: var(--text-main);">Info. Coefficient (IC)</strong> — Correlation between the score and the actual return. Positive IC = the scoring system correctly predicts direction.</div>
                </div>
            </div>
        </details>
    </div>

    <!-- Best variation banner -->
    <div class="section" style="border-color: var(--success); background: var(--best-glow);">
        <div class="section-title" style="color: var(--success);">Best Variation: {{ best.label }}</div>
        <div class="kpi-grid">
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Total Return</div>
                <div class="kpi-value {{ 'success' if best.kpis.cum_return > 0 else 'danger' }}">
                    {{ "%.2f"|format(best.kpis.cum_return * 100) }}%
                </div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">$1,000 →</div>
                <div class="kpi-value {{ 'success' if best.kpis.cum_return > 0 else 'danger' }}">
                    ${{ best.kpis.final_value }}
                </div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Sharpe Ratio</div>
                <div class="kpi-value">{{ "%.2f"|format(best.kpis.sharpe) }}</div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Alpha</div>
                <div class="kpi-value {{ 'success' if best.kpis.alpha > 0 else 'danger' }}">
                    {{ "%+.2f"|format(best.kpis.alpha * 100) }}%
                </div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Win Rate</div>
                <div class="kpi-value">{{ "%.1f"|format(best.kpis.win_rate * 100) }}%</div>
            </div>
        </div>
    </div>

    <!-- Variation comparison table -->
    <div class="section">
        <div class="section-title">Variation Comparison</div>
        <table>
            <thead>
                <tr>
                    <th>Variation</th>
                    <th>Return</th>
                    <th>$1K →</th>
                    <th>Sharpe</th>
                    <th>Win Rate</th>
                    <th>Alpha</th>
                    <th>Benchmark</th>
                    <th>Trades</th>
                </tr>
            </thead>
            <tbody>
            {% for v in variations %}
                <tr class="{{ 'best-row' if v.is_best else '' }}">
                    <td>
                        <strong>{{ v.label }}</strong>
                        {% if v.is_best %}<span class="best-badge">Best</span>{% endif %}
                    </td>
                    <td class="{{ 'trend-up' if v.kpis.cum_return > 0 else 'trend-down' }}">
                        {{ "%+.2f"|format(v.kpis.cum_return * 100) }}%
                    </td>
                    <td>${{ v.kpis.final_value }}</td>
                    <td>{{ "%.2f"|format(v.kpis.sharpe) }}</td>
                    <td>{{ "%.1f"|format(v.kpis.win_rate * 100) }}%</td>
                    <td class="{{ 'trend-up' if v.kpis.alpha > 0 else 'trend-down' }}">
                        {{ "%+.2f"|format(v.kpis.alpha * 100) }}%
                    </td>
                    <td style="color: #f59e0b;">{{ "%.2f"|format(v.kpis.bench_return * 100) }}%</td>
                    <td>{{ v.kpis.n_trades }} ({{ "%.0f"|format(v.kpis.exposure_pct) }}%)</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>

    <!-- Overlaid equity curves -->
    <div class="section">
        <div class="section-title">Equity Curves (All Variations)</div>
        <div id="chart-equity-all" class="chart-container"></div>
    </div>

    <!-- Instrument performance for best variation -->
    <div class="section">
        <div class="section-title">
            Instrument Performance ({{ best.label }})
            <span class="tooltip">ⓘ<span class="tooltip-text">Per-instrument breakdown for the best-performing variation.</span></span>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Symbol</th>
                    <th>Mean Score</th>
                    <th>Avg Return</th>
                    <th>Info. Coeff.</th>
                    <th>Win Rate</th>
                </tr>
            </thead>
            <tbody>
                {% for row in best.instrument_stats %}
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

    <!-- Per-variation trade logs (collapsible) -->
    {% for v in variations %}
    <details {{ 'open' if v.is_best else '' }}>
        <summary>{{ v.label }} — Trade Log ($1,000 Starting Capital){% if v.is_best %}<span class="best-badge">Best</span>{% endif %}</summary>
        <div class="detail-body">
            <div class="kpi-grid" style="margin-bottom: 1rem;">
                <div class="kpi-card">
                    <div class="kpi-label">Return</div>
                    <div class="kpi-value {{ 'success' if v.kpis.cum_return > 0 else 'danger' }}">{{ "%+.2f"|format(v.kpis.cum_return * 100) }}%</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">$1K →</div>
                    <div class="kpi-value">${{ v.kpis.final_value }}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Sharpe</div>
                    <div class="kpi-value">{{ "%.2f"|format(v.kpis.sharpe) }}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Alpha</div>
                    <div class="kpi-value {{ 'success' if v.kpis.alpha > 0 else 'danger' }}">{{ "%+.2f"|format(v.kpis.alpha * 100) }}%</div>
                </div>
            </div>
            <div style="max-height: 500px; overflow-y: auto;">
            <table>
                <thead>
                    <tr><th>#</th><th>Date</th><th>Instrument</th><th>Score</th><th>Return</th><th>Portfolio</th><th>Invested</th><th>Returned</th><th>P&L</th></tr>
                </thead>
                <tbody>
                {% for trade in v.trade_log[:200] %}
                    <tr>
                        <td style="color: var(--text-dim);">{{ loop.index }}</td>
                        <td>{{ trade.date }}</td>
                        <td><strong>{{ trade.symbol }}</strong></td>
                        <td>{{ "%.2f"|format(trade.score) }}</td>
                        <td class="{{ 'trend-up' if trade.return_pct > 0 else 'trend-down' }}">{{ "%+.2f"|format(trade.return_pct) }}%</td>
                        <td style="color: var(--text-dim);">${{ trade.portfolio_value }}</td>
                        <td>${{ trade.invested }}</td>
                        <td>${{ trade.returned }}</td>
                        <td class="{{ 'trend-up' if trade.pnl > 0 else 'trend-down' }}">{{ trade.pnl_fmt }}$</td>
                    </tr>
                {% endfor %}
                </tbody>
            </table>
            </div>
        </div>
    </details>
    {% endfor %}

    <footer>TradeSignal Backtester | Multi-Variation Strategy Report</footer>
</div>

<script>
    const darkLayout = {
        paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)',
        font: { color: '#94a3b8', family: 'Inter' },
        xaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155', title: 'Date' },
        yaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155', title: 'Cumulative Return (%)' },
        margin: { t: 30, r: 20, l: 50, b: 50 },
        legend: { bgcolor: 'rgba(0,0,0,0)', font: { color: '#94a3b8' } }
    };
    const eqData = {{ equity_json | safe }};
    Plotly.newPlot('chart-equity-all', eqData.data, { ...darkLayout, ...eqData.layout });
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# Summary Report Template
# ---------------------------------------------------------------------------
SUMMARY_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Backtest Summary Report - {{ timestamp }}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>{{ css }}</style>
</head>
<body>
{{ nav_html | safe }}
<div class="container">
    <header>
        <h1>Backtest Summary &mdash; All Strategies</h1>
        <div class="timestamp">
            <div>Backtest Period: {{ backtest_start }} to {{ backtest_end }}</div>
            <div>{{ n_strategies }} strategies × 4 variations = {{ n_total }} configurations | Generated: {{ timestamp }}</div>
        </div>
    </header>

    <!-- Overall winner -->
    <div class="section" style="border-color: var(--success); background: var(--best-glow);">
        <div class="section-title" style="color: var(--success);">Overall Best: {{ winner.strategy_name }} — {{ winner.label }}</div>
        <div class="kpi-grid">
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Total Return</div>
                <div class="kpi-value success">{{ "%.2f"|format(winner.kpis.cum_return * 100) }}%</div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">$1,000 →</div>
                <div class="kpi-value success">${{ winner.kpis.final_value }}</div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Sharpe</div>
                <div class="kpi-value">{{ "%.2f"|format(winner.kpis.sharpe) }}</div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Alpha</div>
                <div class="kpi-value success">{{ "%+.2f"|format(winner.kpis.alpha * 100) }}%</div>
            </div>
            <div class="kpi-card" style="border-color: var(--success);">
                <div class="kpi-label">Win Rate</div>
                <div class="kpi-value">{{ "%.1f"|format(winner.kpis.win_rate * 100) }}%</div>
            </div>
        </div>
    </div>

    <!-- Best variation per strategy -->
    <div class="section">
        <div class="section-title">Best Variation Per Strategy (Ranked by Alpha)</div>
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Strategy</th>
                    <th>Best Variation</th>
                    <th>Return</th>
                    <th>$1K →</th>
                    <th>Sharpe</th>
                    <th>Alpha</th>
                    <th>Win Rate</th>
                    <th>Trades</th>
                </tr>
            </thead>
            <tbody>
            {% for r in ranked_strategies %}
                <tr class="{{ 'best-row' if loop.index == 1 else '' }}">
                    <td><strong>#{{ loop.index }}</strong></td>
                    <td><a href="report_{{ r.strategy_name }}.html" style="color: var(--accent-primary); text-decoration: none;"><strong>{{ r.strategy_name }}</strong></a></td>
                    <td>{{ r.label }}</td>
                    <td class="{{ 'trend-up' if r.kpis.cum_return > 0 else 'trend-down' }}">
                        {{ "%+.2f"|format(r.kpis.cum_return * 100) }}%
                    </td>
                    <td>${{ r.kpis.final_value }}</td>
                    <td>{{ "%.2f"|format(r.kpis.sharpe) }}</td>
                    <td class="{{ 'trend-up' if r.kpis.alpha > 0 else 'trend-down' }}">
                        {{ "%+.2f"|format(r.kpis.alpha * 100) }}%
                    </td>
                    <td>{{ "%.1f"|format(r.kpis.win_rate * 100) }}%</td>
                    <td>{{ r.kpis.n_trades }}</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>

    <!-- Full matrix: all strategies × all variations -->
    <div class="section">
        <div class="section-title">Full Matrix (All Strategies × All Variations, Ranked by Alpha)</div>
        <div style="max-height: 800px; overflow-y: auto;">
        <table>
            <thead>
                <tr>
                    <th>Rank</th>
                    <th>Strategy</th>
                    <th>Variation</th>
                    <th>Return</th>
                    <th>$1K →</th>
                    <th>Sharpe</th>
                    <th>Alpha</th>
                    <th>Win Rate</th>
                    <th>Bench Return</th>
                    <th>Trades</th>
                </tr>
            </thead>
            <tbody>
            {% for r in all_ranked %}
                <tr class="{{ 'best-row' if loop.index <= 3 else '' }}">
                    <td><strong>#{{ loop.index }}</strong></td>
                    <td><a href="report_{{ r.strategy_name }}.html" style="color: var(--accent-primary); text-decoration: none;"><strong>{{ r.strategy_name }}</strong></a></td>
                    <td>{{ r.label }}</td>
                    <td class="{{ 'trend-up' if r.kpis.cum_return > 0 else 'trend-down' }}">
                        {{ "%+.2f"|format(r.kpis.cum_return * 100) }}%
                    </td>
                    <td>${{ r.kpis.final_value }}</td>
                    <td>{{ "%.2f"|format(r.kpis.sharpe) }}</td>
                    <td class="{{ 'trend-up' if r.kpis.alpha > 0 else 'trend-down' }}">
                        {{ "%+.2f"|format(r.kpis.alpha * 100) }}%
                    </td>
                    <td>{{ "%.1f"|format(r.kpis.win_rate * 100) }}%</td>
                    <td style="color: #f59e0b;">{{ "%.2f"|format(r.kpis.bench_return * 100) }}%</td>
                    <td>{{ r.kpis.n_trades }} ({{ "%.0f"|format(r.kpis.exposure_pct) }}%)</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
        </div>
    </div>

    <!-- Conclusion -->
    <div class="section">
        <div class="section-title">Conclusion</div>
        <p style="color: var(--text-dim); font-size: 0.9rem; line-height: 1.8;">
            Across <strong>{{ n_strategies }} strategies</strong> and <strong>4 variations</strong>
            ({{ n_total }} total configurations), the best overall performer is
            <strong style="color: var(--success);">{{ winner.strategy_name }}</strong>
            using <strong>{{ winner.label }}</strong>, achieving
            <strong class="trend-up">{{ "%+.2f"|format(winner.kpis.alpha * 100) }}% alpha</strong>
            over the equal-weight benchmark with a Sharpe ratio of
            <strong>{{ "%.2f"|format(winner.kpis.sharpe) }}</strong>.
        </p>
        <p style="color: var(--text-dim); font-size: 0.9rem; line-height: 1.8;">
            {% if sentiment_lift > 0 %}
            Sentiment-enhanced variations outperformed no-sentiment versions in
            <strong>{{ sentiment_wins }}/{{ n_strategies }}</strong> strategies (avg alpha lift:
            <strong class="trend-up">{{ "%+.2f"|format(sentiment_lift * 100) }}%</strong>),
            confirming that the NIM/Qwen sentiment pipeline adds meaningful signal.
            {% else %}
            Sentiment did not consistently improve results. No-sentiment versions matched or
            outperformed in <strong>{{ n_strategies - sentiment_wins }}/{{ n_strategies }}</strong>
            strategies, suggesting the scoring system's technical + fundamentals backbone is
            the primary value driver.
            {% endif %}
        </p>
        <p style="color: var(--text-dim); font-size: 0.9rem; line-height: 1.8;">
            {% if long_wins > short_wins %}
            Long-term horizons produced better risk-adjusted returns in
            <strong>{{ long_wins }}/{{ n_strategies }}</strong> strategies,
            consistent with the expectation that signal quality improves over longer holding periods.
            {% elif short_wins > long_wins %}
            Short-term horizons outperformed in
            <strong>{{ short_wins }}/{{ n_strategies }}</strong> strategies,
            suggesting the grading system captures timely alpha that dissipates over longer periods.
            {% else %}
            Short-term and long-term horizons were evenly matched, indicating robust signal
            quality across holding periods.
            {% endif %}
        </p>
    </div>

    <footer>TradeSignal Backtester | Cross-Strategy Summary Report</footer>
</div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Data helpers (unchanged)
# ---------------------------------------------------------------------------

async def get_raw_results(term: str = "short", sentiment_mode: str = "on") -> pd.DataFrame:
    """Fetch raw backtest grades and returns for analysis."""
    from .backtest_engine import load_instruments

    all_instruments = await load_instruments(extended=True)
    sym_map = {inst["symbol"]: {"category": inst["category"], "sector": inst.get("sector")}
               for inst in all_instruments}

    async with async_session() as session:
        ret_col = "return_5d" if term == "short" else "return_20d"
        result = await session.execute(
            text(f"""
            SELECT bg.symbol, bg.date, bg.overall_score, br.{ret_col} as return_val
            FROM backtest_grades bg
            JOIN backtest_returns br ON br.instrument_id = bg.instrument_id AND br.date = bg.date
            WHERE bg.term = :term AND bg.sentiment_mode = :smode
              AND br.{ret_col} IS NOT NULL
            ORDER BY bg.date ASC
        """),
            {"term": term, "smode": sentiment_mode}
        )
        rows = result.fetchall()

    df = pd.DataFrame([dict(r._mapping) for r in rows])
    if not df.empty:
        # Ensure numeric columns are float (Postgres returns Decimal)
        df["overall_score"] = df["overall_score"].astype(float)
        df["return_val"] = df["return_val"].astype(float)
        # Cap returns at [-1.0, +5.0] — returns below -100% are artifacts
        # (e.g., oil futures going negative in Apr 2020)
        df["return_val"] = df["return_val"].clip(lower=-1.0, upper=5.0)
        df["category"] = df["symbol"].map(lambda s: sym_map.get(s, {}).get("category", "Stock"))
        df["sector"] = df["symbol"].map(lambda s: sym_map.get(s, {}).get("sector"))
    return df


def calculate_kpis(df: pd.DataFrame, term: str = "short") -> dict:
    """Calculate aggregate performance metrics from a strategy-processed DataFrame."""
    if df.empty or "daily_strat_ret" not in df.columns:
        return {"cum_return": 0, "final_value": "1,000", "sharpe": 0, "win_rate": 0,
                "n_samples": 0, "n_trades": 0, "exposure_pct": 0,
                "bench_return": 0, "bench_final": "1,000", "bench_sharpe": 0, "alpha": 0}

    period_rets = df.groupby("date")["daily_strat_ret"].sum()
    active_rets = period_rets[period_rets != 0]
    n_trades = len(active_rets)
    n_total_periods = len(period_rets)

    total_ret = float((1 + active_rets).prod() - 1) if n_trades > 0 else 0.0
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
        # Benchmark fields filled in by _compute_variation_data
        "bench_return": 0, "bench_final": "1,000", "bench_sharpe": 0, "alpha": 0,
    }


def generate_trade_log(df: pd.DataFrame, term: str = "short", initial_investment: float = 1000.0) -> list[dict]:
    """Generate a chronological trade log with compounding portfolio value.

    Each instrument on a given entry date shows its own allocated capital and
    individual PnL based on its strategy weight (daily_strat_ret), not the
    portfolio-level total.
    """
    holding_days = 5 if term == "short" else 20
    trades_df = df[df["daily_strat_ret"] != 0][
        ["date", "symbol", "overall_score", "return_val", "daily_strat_ret"]
    ].copy()
    if trades_df.empty:
        return []

    trades_df = trades_df.sort_values("date")
    period_rets = trades_df.groupby("date")["daily_strat_ret"].sum().sort_index()

    # Track portfolio value through time
    portfolio_value = initial_investment
    date_to_portfolio = {}
    for d, period_ret in period_rets.items():
        date_to_portfolio[d] = portfolio_value
        portfolio_value = portfolio_value * (1 + period_ret)

    log = []
    for _, row in trades_df.iterrows():
        d = row["date"]
        port_val = date_to_portfolio.get(d, initial_investment)
        # Per-instrument allocation and PnL
        instrument_alloc = port_val * abs(row["daily_strat_ret"] / row["return_val"]) if row["return_val"] != 0 else 0.0
        instrument_pnl = port_val * row["daily_strat_ret"]
        instrument_returned = instrument_alloc + instrument_pnl
        # Position return = PnL / Invested (aligns sign with P&L for both
        # long and short positions, unlike raw instrument return which
        # inverts for shorts).
        position_return_pct = float(instrument_pnl / instrument_alloc * 100) if instrument_alloc > 0 else 0.0
        log.append({
            "date": d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d),
            "symbol": row["symbol"],
            "score": float(row["overall_score"]),
            "return_pct": position_return_pct,
            "invested": f"{instrument_alloc:,.2f}",
            "returned": f"{instrument_returned:,.2f}",
            "pnl": instrument_pnl,
            "pnl_fmt": f"{instrument_pnl:+,.2f}",
            "holding_days": holding_days,
            "portfolio_value": f"{port_val:,.2f}",
        })
    return log


def generate_instrument_stats(df: pd.DataFrame) -> list[dict]:
    """Calculate stats per instrument."""
    from scipy.stats import spearmanr

    stats = []
    for sym, group in df.groupby("symbol"):
        ic, _ = spearmanr(group["overall_score"], group["return_val"])
        stats.append({
            "symbol": sym,
            "avg_score": float(group["overall_score"].mean()),
            "avg_return": float(group["return_val"].mean()),
            "ic": float(ic) if not np.isnan(ic) else 0.0,
            "win_rate": float((group["overall_score"] * group["return_val"] > 0).mean()),
        })
    return sorted(stats, key=lambda x: x["ic"], reverse=True)


def compute_benchmark(df: pd.DataFrame, term: str = "short") -> pd.Series:
    """Compute equal-weight buy-and-hold benchmark using same non-overlapping periods."""
    from .strategies import _get_entry_dates, _holding_period

    holding = _holding_period(term)
    all_dates = sorted(df["date"].unique())
    entry_dates = _get_entry_dates(all_dates, holding)

    benchmark_rets = {}
    for entry_date in entry_dates:
        day_df = df[df["date"] == entry_date]
        if day_df.empty:
            continue
        benchmark_rets[entry_date] = day_df["return_val"].mean()

    return pd.Series(benchmark_rets).sort_index()


# ---------------------------------------------------------------------------
# Internal: compute data for a single variation
# ---------------------------------------------------------------------------

async def _compute_variation_data(
    strategy: str,
    var: dict,
    strategy_params: "StrategyParams",
) -> dict | None:
    """Compute all report data for a single (term × sentiment_mode) variation.

    Returns dict with kpis, equity curve data, instrument stats, trade log,
    or None if no data available.
    """
    from .strategies import apply_strategy

    term = var["term"]
    smode = var["smode"]
    label = var["label"]

    df = await get_raw_results(term=term, sentiment_mode=smode)
    if df.empty:
        logger.warning("No data for %s (term=%s, smode=%s)", strategy, term, smode)
        return None

    df = df.dropna(subset=["overall_score", "return_val"])
    df["overall_score"] = df["overall_score"].astype(float)
    df["return_val"] = df["return_val"].astype(float)
    if df.empty:
        return None

    df_strat = apply_strategy(df, strategy, term, strategy_params)
    kpis = calculate_kpis(df_strat, term=term)

    # Benchmark
    bench_rets = compute_benchmark(df_strat, term=term)
    bench_total = float((1 + bench_rets).prod() - 1) if len(bench_rets) > 0 else 0.0
    bench_final = 1000 * (1 + bench_total)
    periods_per_year = 52.0 if term == "short" else 13.0
    if len(bench_rets) > 1 and bench_rets.std() > 0:
        bench_sharpe = float(bench_rets.mean() / bench_rets.std() * np.sqrt(periods_per_year))
    else:
        bench_sharpe = 0.0
    kpis["bench_return"] = bench_total
    kpis["bench_final"] = f"{bench_final:,.0f}"
    kpis["bench_sharpe"] = bench_sharpe
    kpis["alpha"] = kpis["cum_return"] - bench_total

    # Equity curve series
    period_rets = df_strat.groupby("date")["daily_strat_ret"].sum()
    active_rets = period_rets[period_rets != 0]
    cum_ret = (1 + active_rets).cumprod() - 1 if len(active_rets) > 0 else pd.Series(dtype=float)

    return {
        "term": term,
        "smode": smode,
        "label": label,
        "slug": var["slug"],
        "kpis": kpis,
        "equity_dates": [str(d) for d in cum_ret.index] if not cum_ret.empty else [],
        "equity_values": [float(v * 100) for v in cum_ret.values] if not cum_ret.empty else [],
        "instrument_stats": generate_instrument_stats(df),
        "trade_log": generate_trade_log(df_strat, term=term),
        "is_best": False,  # set later
    }


# ---------------------------------------------------------------------------
# Public: generate multi-variation strategy report
# ---------------------------------------------------------------------------

async def generate_strategy_report(
    strategy: str,
    strategy_params: "StrategyParams | None" = None,
    strategy_names: list[str] | None = None,
    include_walk_forward: bool = False,
) -> dict:
    """Generate a single HTML report for one strategy with all 4 variations.

    Returns dict with filepath, strategy name, and list of variation results.
    """
    from .strategies import STRATEGIES, StrategyParams

    if strategy_params is None:
        strategy_params = StrategyParams()

    strategy_desc = STRATEGIES.get(strategy, {}).get("desc", strategy)
    logger.info("=== Generating multi-variation report: %s ===", strategy)

    # Compute all 4 variations
    variations_data = []
    for var in VARIATIONS:
        data = await _compute_variation_data(strategy, var, strategy_params)
        if data is not None:
            variations_data.append(data)

    if not variations_data:
        logger.error("No data for any variation of %s. Run backtest first.", strategy)
        return {"filepath": "", "variations": []}

    # Determine best variation by alpha
    best_idx = max(range(len(variations_data)), key=lambda i: variations_data[i]["kpis"]["alpha"])
    variations_data[best_idx]["is_best"] = True
    best = variations_data[best_idx]

    # Build overlaid equity curve chart
    equity_fig = go.Figure()
    for i, v in enumerate(variations_data):
        color = VARIATION_COLORS[i % len(VARIATION_COLORS)]
        width = 3 if v["is_best"] else 1.5
        equity_fig.add_trace(go.Scatter(
            x=v["equity_dates"], y=v["equity_values"],
            mode="lines", name=v["label"],
            line=dict(color=color, width=width),
        ))
    equity_fig.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Render HTML
    from .config import BACKTEST_START, BACKTEST_END
    strategy_guide = STRATEGIES.get(strategy, {}).get("guide")
    nav_html = _build_nav_html(active=strategy, strategy_names=strategy_names, include_walk_forward=include_walk_forward)
    template = Template(STRATEGY_REPORT_TEMPLATE)
    html = template.render(
        css=SHARED_CSS,
        nav_html=nav_html,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        backtest_start=BACKTEST_START,
        backtest_end=BACKTEST_END,
        strategy_name=strategy,
        strategy_desc=strategy_desc,
        guide=strategy_guide,
        best=best,
        variations=variations_data,
        equity_json=equity_fig.to_json(),
    )

    report_dir = os.environ.get("REPORT_DIR", "/reports")
    os.makedirs(report_dir, exist_ok=True)
    filename = f"report_{strategy}.html"
    filepath = os.path.join(report_dir, filename)

    with open(filepath, "w") as f:
        f.write(html)

    logger.info("Report saved: %s", filepath)
    return {
        "filepath": filepath,
        "strategy": strategy,
        "strategy_name": strategy,
        "strategy_desc": strategy_desc,
        "variations": variations_data,
        "best": best,
    }


# ---------------------------------------------------------------------------
# Public: generate cross-strategy summary report
# ---------------------------------------------------------------------------

async def generate_summary_report(
    strategy_results: list[dict],
    include_walk_forward: bool = False,
) -> str:
    """Generate the summary report comparing all strategies.

    Args:
        strategy_results: List of dicts returned by generate_strategy_report().
        include_walk_forward: Whether to include walk-forward link in nav.

    Returns filepath of the summary report.
    """
    # Build flat list: each entry = one strategy × one variation
    all_entries = []
    for sr in strategy_results:
        for v in sr.get("variations", []):
            all_entries.append({
                "strategy_name": sr["strategy_name"],
                "strategy_desc": sr.get("strategy_desc", ""),
                "label": v["label"],
                "slug": v["slug"],
                "kpis": v["kpis"],
            })

    if not all_entries:
        logger.error("No data for summary report.")
        return ""

    # Rank all by alpha
    all_ranked = sorted(all_entries, key=lambda e: e["kpis"]["alpha"], reverse=True)

    # Best variation per strategy
    best_per_strategy = {}
    for entry in all_ranked:
        name = entry["strategy_name"]
        if name not in best_per_strategy:
            best_per_strategy[name] = entry
    ranked_strategies = sorted(best_per_strategy.values(), key=lambda e: e["kpis"]["alpha"], reverse=True)

    winner = all_ranked[0]
    n_strategies = len(best_per_strategy)

    # Sentiment analysis: for each strategy, compare best "on" vs best "off"
    sentiment_wins = 0
    sentiment_lifts = []
    short_wins = 0
    long_wins = 0
    for sr in strategy_results:
        vars_by_key = {(v["term"], v["smode"]): v for v in sr.get("variations", [])}
        # Best sentiment alpha per term
        for t in ("short", "long"):
            on = vars_by_key.get((t, "on"))
            off = vars_by_key.get((t, "off"))
            if on and off:
                sentiment_lifts.append(on["kpis"]["alpha"] - off["kpis"]["alpha"])
        # Count which term wins per strategy
        best_v = sr.get("best")
        if best_v:
            if best_v["term"] == "short":
                short_wins += 1
            else:
                long_wins += 1

    # Count strategies where sentiment helps (at least one term improved)
    for sr in strategy_results:
        vars_by_key = {(v["term"], v["smode"]): v for v in sr.get("variations", [])}
        sent_better = False
        for t in ("short", "long"):
            on = vars_by_key.get((t, "on"))
            off = vars_by_key.get((t, "off"))
            if on and off and on["kpis"]["alpha"] > off["kpis"]["alpha"]:
                sent_better = True
        if sent_better:
            sentiment_wins += 1

    avg_sentiment_lift = np.mean(sentiment_lifts) if sentiment_lifts else 0.0

    # Render
    from .config import BACKTEST_START, BACKTEST_END
    all_strategy_names = [sr["strategy_name"] for sr in strategy_results]
    nav_html = _build_nav_html(active="summary", strategy_names=all_strategy_names, include_walk_forward=include_walk_forward)
    template = Template(SUMMARY_REPORT_TEMPLATE)
    html = template.render(
        css=SHARED_CSS,
        nav_html=nav_html,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        backtest_start=BACKTEST_START,
        backtest_end=BACKTEST_END,
        n_strategies=n_strategies,
        n_total=len(all_entries),
        winner=winner,
        ranked_strategies=ranked_strategies,
        all_ranked=all_ranked,
        sentiment_wins=sentiment_wins,
        sentiment_lift=avg_sentiment_lift,
        short_wins=short_wins,
        long_wins=long_wins,
    )

    report_dir = os.environ.get("REPORT_DIR", "/reports")
    os.makedirs(report_dir, exist_ok=True)
    filepath = os.path.join(report_dir, "index.html")

    with open(filepath, "w") as f:
        f.write(html)

    logger.info("Summary report saved: %s", filepath)
    return filepath


# ---------------------------------------------------------------------------
# Walk-Forward Report Template
# ---------------------------------------------------------------------------
WALK_FORWARD_REPORT_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Walk-Forward Out-of-Sample Report - {{ timestamp }}</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>{{ css }}</style>
</head>
<body>
{{ nav_html | safe }}
<div class="container">
    <header>
        <h1>Walk-Forward Out-of-Sample Analysis</h1>
        <div class="timestamp">
            <div>{{ n_windows }} rolling windows | Train: {{ train_months }}mo, Test: {{ test_months }}mo | Term: {{ term }}</div>
            <div>Generated: {{ timestamp }}</div>
        </div>
    </header>

    <!-- KPI Explainer -->
    <div class="section">
        <details>
            <summary style="font-size: 0.9rem;">What is Walk-Forward Testing? (click to expand)</summary>
            <div class="detail-body" style="color: var(--text-dim); font-size: 0.85rem; line-height: 1.8;">
                <p>Walk-forward testing is the gold standard for validating a trading signal. Instead of optimizing
                weights on all data and then testing on the same data (which overfits), we:</p>
                <ol>
                    <li><strong>Train</strong> on {{ train_months }} months of historical data to calibrate signal weights</li>
                    <li><strong>Test</strong> on the next {{ test_months }} months that the model has never seen</li>
                    <li><strong>Roll forward</strong> by {{ test_months }} months and repeat</li>
                </ol>
                <p>This mimics real trading: you only ever trade on data the model hasn't been trained on.
                If the signal works out-of-sample, it's more likely to work in live trading.</p>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 1rem;">
                    <div><strong style="color: var(--text-main);">Default Return</strong> — Performance using the system's built-in signal weights (no optimization).</div>
                    <div><strong style="color: var(--text-main);">Calibrated Return</strong> — Performance using weights optimized on the training window.</div>
                    <div><strong style="color: var(--text-main);">Benchmark</strong> — Equal-weight return (buying all assets equally). Alpha = strategy return minus benchmark.</div>
                    <div><strong style="color: var(--text-main);">Info. Coefficient (IC)</strong> — Spearman rank correlation between predicted score and actual return. Positive = signal has predictive power.</div>
                </div>
            </div>
        </details>
    </div>

    <!-- Summary KPIs -->
    <div class="section" style="border-color: {{ '#22c55e' if avg_default_alpha > 0 else '#ef4444' }};">
        <div class="section-title">Summary</div>
        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Windows with +Alpha</div>
                <div class="kpi-value {{ 'success' if pct_positive > 50 else 'danger' }}">{{ n_positive }}/{{ n_windows }} ({{ "%.0f"|format(pct_positive) }}%)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Avg Default Alpha</div>
                <div class="kpi-value {{ 'success' if avg_default_alpha > 0 else 'danger' }}">{{ "%+.2f"|format(avg_default_alpha * 100) }}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Avg Calibrated Alpha</div>
                <div class="kpi-value {{ 'success' if avg_calibrated_alpha > 0 else 'danger' }}">{{ "%+.2f"|format(avg_calibrated_alpha * 100) }}%</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Avg Default IC</div>
                <div class="kpi-value {{ 'success' if avg_default_ic > 0 else 'danger' }}">{{ "%+.4f"|format(avg_default_ic) }}</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Calibration Beats Default</div>
                <div class="kpi-value">{{ cal_beats_default }}/{{ n_windows }}</div>
            </div>
        </div>
    </div>

    <!-- Per-window results table -->
    <div class="section">
        <div class="section-title">Per-Window Results (Top-3 Strategy)</div>
        <table>
            <thead>
                <tr>
                    <th>Window</th>
                    <th>Test Period</th>
                    <th>N</th>
                    <th>Default Return</th>
                    <th>Calibrated Return</th>
                    <th>Benchmark</th>
                    <th>Default Alpha</th>
                    <th>Calibrated Alpha</th>
                    <th>Default IC</th>
                    <th>Calibrated IC</th>
                </tr>
            </thead>
            <tbody>
            {% for w in windows %}
                <tr class="{{ 'best-row' if w.default_alpha > 0 else '' }}">
                    <td><strong>#{{ w.window_num }}</strong></td>
                    <td>{{ w.test_start }} &rarr; {{ w.test_end }}</td>
                    <td>{{ w.n_test }}</td>
                    <td class="{{ 'trend-up' if w.default_return > 0 else 'trend-down' }}">{{ "%+.1f"|format(w.default_return * 100) }}%</td>
                    <td class="{{ 'trend-up' if w.calibrated_return > 0 else 'trend-down' }}">{{ "%+.1f"|format(w.calibrated_return * 100) }}%</td>
                    <td style="color: #f59e0b;">{{ "%+.1f"|format(w.benchmark_return * 100) }}%</td>
                    <td class="{{ 'trend-up' if w.default_alpha > 0 else 'trend-down' }}">{{ "%+.1f"|format(w.default_alpha * 100) }}%</td>
                    <td class="{{ 'trend-up' if w.calibrated_alpha > 0 else 'trend-down' }}">{{ "%+.1f"|format(w.calibrated_alpha * 100) }}%</td>
                    <td>{{ "%+.3f"|format(w.default_ic) }}</td>
                    <td>{{ "%+.3f"|format(w.calibrated_ic) }}</td>
                </tr>
            {% endfor %}
            </tbody>
        </table>
    </div>

    <!-- Alpha bar chart (pure HTML/CSS) -->
    <div class="section">
        <div class="section-title">Default Alpha by Window</div>
        <div style="display: flex; flex-direction: column; gap: 4px;">
        {% for w in windows %}
            <div style="display: flex; align-items: center; gap: 8px;">
                <span style="width: 24px; text-align: right; color: var(--text-dim); font-size: 0.75rem;">{{ w.window_num }}</span>
                {% if w.default_alpha >= 0 %}
                <div style="display: flex; width: 100%; height: 18px;">
                    <div style="width: 50%;"></div>
                    <div style="width: {{ (w.default_alpha * 100 / max_abs_alpha * 50)|round(1) if max_abs_alpha > 0 else 0 }}%; background: var(--success); border-radius: 0 4px 4px 0; min-width: 2px;"></div>
                </div>
                {% else %}
                <div style="display: flex; width: 100%; height: 18px; justify-content: flex-end;">
                    <div style="width: {{ (-w.default_alpha * 100 / max_abs_alpha * 50)|round(1) if max_abs_alpha > 0 else 0 }}%; background: var(--danger); border-radius: 4px 0 0 4px; min-width: 2px; margin-left: auto; margin-right: 0;"></div>
                    <div style="width: 50%;"></div>
                </div>
                {% endif %}
                <span style="width: 60px; text-align: right; color: var(--text-dim); font-size: 0.75rem;">{{ "%+.1f"|format(w.default_alpha * 100) }}%</span>
            </div>
        {% endfor %}
        </div>
    </div>

    <!-- Conclusion -->
    <div class="section">
        <div class="section-title">Interpretation</div>
        <p style="color: var(--text-dim); font-size: 0.9rem; line-height: 1.8;">
            Across <strong>{{ n_windows }} out-of-sample windows</strong>,
            the default signal weights produced positive alpha in
            <strong class="{{ 'trend-up' if pct_positive >= 50 else 'trend-down' }}">{{ n_positive }}/{{ n_windows }} ({{ "%.0f"|format(pct_positive) }}%)</strong>
            of windows, with an average alpha of
            <strong class="{{ 'trend-up' if avg_default_alpha > 0 else 'trend-down' }}">{{ "%+.2f"|format(avg_default_alpha * 100) }}%</strong>
            per window.
        </p>
        <p style="color: var(--text-dim); font-size: 0.9rem; line-height: 1.8;">
            {% if pct_positive >= 60 %}
            The signal demonstrates consistent out-of-sample alpha generation, suggesting genuine predictive power
            rather than overfitting. The scoring system adds value beyond simple equal-weight investing.
            {% elif pct_positive >= 45 %}
            The signal shows directional skill but is inconsistent across market regimes. The scoring system
            provides marginal value, likely strongest in trending markets. Adding sentiment data may improve stability.
            {% else %}
            The signal struggles to generate consistent out-of-sample alpha. This may indicate overfitting to
            in-sample patterns, insufficient data diversity, or that the current instrument universe is too
            correlated for the scoring system to differentiate effectively. Sentiment and sector data may help.
            {% endif %}
        </p>
        <p style="color: var(--text-dim); font-size: 0.9rem; line-height: 1.8;">
            {% if cal_beats_default > n_windows // 2 %}
            Weight calibration improved performance in <strong>{{ cal_beats_default }}/{{ n_windows }}</strong> windows,
            suggesting the calibrator extracts useful signal from historical patterns.
            {% else %}
            Weight calibration improved performance in only <strong>{{ cal_beats_default }}/{{ n_windows }}</strong> windows,
            suggesting the default weights are already well-tuned or that optimization overfits the training period.
            {% endif %}
        </p>
    </div>

    <footer>TradeSignal Backtester | Walk-Forward Out-of-Sample Report</footer>
</div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Public: generate walk-forward HTML report
# ---------------------------------------------------------------------------

def generate_walk_forward_report(
    results: "list",
    term: str = "short",
    train_months: int = 12,
    test_months: int = 3,
    strategy_names: list[str] | None = None,
) -> str:
    """Generate an HTML walk-forward report from WindowResult list.

    Args:
        results: List of WindowResult dataclasses from walk_forward.run_walk_forward().
        term: 'short' or 'long'.
        train_months: Training window months.
        test_months: Test window months.
        strategy_names: Strategy names for nav bar links.

    Returns filepath of the generated report.
    """
    if not results:
        logger.warning("No walk-forward results to generate report from.")
        return ""

    # Compute summary stats
    def_alphas = [w.default_alpha for w in results]
    cal_alphas = [w.calibrated_alpha for w in results]
    def_ics = [w.default_ic for w in results]

    n_positive = sum(1 for a in def_alphas if a > 0)
    n_windows = len(results)
    pct_positive = 100 * n_positive / n_windows if n_windows > 0 else 0
    avg_default_alpha = float(np.mean(def_alphas))
    avg_calibrated_alpha = float(np.mean(cal_alphas))
    avg_default_ic = float(np.mean(def_ics))
    cal_beats_default = sum(1 for d, c in zip(def_alphas, cal_alphas) if c > d)
    max_abs_alpha = max(abs(a) for a in def_alphas) if def_alphas else 1.0

    # Convert WindowResult dataclasses to dicts for Jinja2
    windows = []
    for w in results:
        windows.append({
            "window_num": w.window_num,
            "test_start": str(w.test_start),
            "test_end": str(w.test_end),
            "n_test": w.n_test,
            "default_return": w.default_return,
            "calibrated_return": w.calibrated_return,
            "benchmark_return": w.benchmark_return,
            "default_alpha": w.default_alpha,
            "calibrated_alpha": w.calibrated_alpha,
            "default_ic": w.default_ic,
            "calibrated_ic": w.calibrated_ic,
        })

    nav_html = _build_nav_html(active="walk_forward", strategy_names=strategy_names, include_walk_forward=True)
    template = Template(WALK_FORWARD_REPORT_TEMPLATE)
    html = template.render(
        css=SHARED_CSS,
        nav_html=nav_html,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        term=term,
        train_months=train_months,
        test_months=test_months,
        n_windows=n_windows,
        n_positive=n_positive,
        pct_positive=pct_positive,
        avg_default_alpha=avg_default_alpha,
        avg_calibrated_alpha=avg_calibrated_alpha,
        avg_default_ic=avg_default_ic,
        cal_beats_default=cal_beats_default,
        max_abs_alpha=max_abs_alpha,
        windows=windows,
    )

    report_dir = os.environ.get("REPORT_DIR", "/reports")
    os.makedirs(report_dir, exist_ok=True)
    filepath = os.path.join(report_dir, "report_walk_forward.html")

    with open(filepath, "w") as f:
        f.write(html)

    logger.info("Walk-forward report saved: %s", filepath)
    return filepath
