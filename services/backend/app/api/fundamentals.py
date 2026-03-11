"""Fundamentals & Macro Indicators API endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse

router = APIRouter()

# Sector-relative P/E fair ranges (matches scorer.py thresholds)
_SECTOR_PE_RANGES: dict[str | None, tuple[float, float]] = {
    "technology":             (18, 38),
    "communication":          (16, 35),
    "consumer_discretionary": (15, 35),
    "healthcare":             (16, 35),
    "financials":             (8, 18),
    "industrials":            (13, 28),
    "consumer_staples":       (13, 25),
    "energy":                 (8, 20),
    "materials":              (10, 22),
    "utilities":              (8, 18),
    "real_estate":            (14, 30),
}
_DEFAULT_PE_RANGE = (15, 25)

# Sector-relative D/E fair ranges (matches scorer.py thresholds)
_SECTOR_DE_RANGES: dict[str | None, tuple[float, float]] = {
    "financials":  (1.0, 6.0),
    "utilities":   (0.5, 2.0),
    "real_estate": (0.5, 2.0),
    "energy":      (0.3, 1.5),
}
_DEFAULT_DE_RANGE = (0.3, 1.5)

# Macro indicator config
_MACRO_CONFIG = {
    "dxy": {
        "expected_range": "95 – 108",
        "direction": "range",
        "direction_text": "Stable is ideal for equities",
        "impact": "Strong $ hurts exporters & EM; weak $ boosts commodities",
        "good_zone": [95, 108],
        "warn_zone": [90, 115],
    },
    "treasury_10y": {
        "expected_range": "3.5% – 4.25%",
        "direction": "down",
        "direction_text": "Lower is better for stocks",
        "impact": "Higher yields compete with equities for capital",
        "good_zone": [3.5, 4.25],
        "warn_zone": [3.0, 5.0],
    },
    "gdp_growth": {
        "expected_range": "2.0% – 3.5%",
        "direction": "up",
        "direction_text": "Higher is better (expansion)",
        "impact": "Strong growth supports earnings; too high may trigger rate hikes",
        "good_zone": [2.0, 3.5],
        "warn_zone": [1.0, 5.0],
    },
    "brent_crude": {
        "expected_range": "$65 – $85",
        "direction": "range",
        "direction_text": "Moderate is best for growth",
        "impact": "High oil raises costs & inflation; low oil signals weak demand",
        "good_zone": [65, 85],
        "warn_zone": [50, 100],
    },
}


def _build_metric_config(sector: str | None) -> dict:
    """Build metric-level config with sector-relative ranges."""
    pe_range = _SECTOR_PE_RANGES.get(sector, _DEFAULT_PE_RANGE)
    de_range = _SECTOR_DE_RANGES.get(sector, _DEFAULT_DE_RANGE)

    return {
        "pe_ratio": {
            "label": "P/E Ratio",
            "sublabel": "Valuation",
            "expected_range": f"{pe_range[0]:.0f} – {pe_range[1]:.0f}",
            "direction": "lower",
            "direction_text": "Lower is better (cheaper)",
            "good": pe_range[1],
            "fair": pe_range[1] * 1.5,
        },
        "roe": {
            "label": "ROE",
            "sublabel": "Profitability",
            "expected_range": "10% – 25%",
            "direction": "higher",
            "direction_text": "Higher is better (profitable)",
            "good": 0.15,
            "fair": 0.08,
        },
        "de_ratio": {
            "label": "D/E Ratio",
            "sublabel": "Financial Health",
            "expected_range": f"{de_range[0]:.1f} – {de_range[1]:.1f}",
            "direction": "lower",
            "direction_text": "Lower is better (less debt)",
            "good": de_range[1],
            "fair": de_range[1] * 2,
        },
        "peg_ratio": {
            "label": "PEG Ratio",
            "sublabel": "Growth vs Price",
            "expected_range": "0.5 – 1.5",
            "direction": "range",
            "direction_text": "0.5–1.0 ideal (Peter Lynch)",
            "range_good": [0.5, 1.5],
        },
    }


@router.get("/{instrument_id}", response_model=APIResponse)
async def get_fundamentals(instrument_id: str):
    """Get latest fundamental metrics for an instrument with sector-relative config."""
    async with async_session() as session:
        # Fetch metrics and instrument sector in one go
        result = await session.execute(
            text("""
                SELECT fm.pe_ratio, fm.roe, fm.de_ratio, fm.peg_ratio, fm.fetched_at,
                       i.sector, i.category
                FROM fundamental_metrics fm
                JOIN instruments i ON i.id = fm.instrument_id
                WHERE fm.instrument_id = :iid
                ORDER BY fm.fetched_at DESC
                LIMIT 1
            """),
            {"iid": instrument_id},
        )
        row = result.fetchone()

    if not row:
        # Still return config even without data, so FE can show ranges
        # Try to get sector from instrument
        async with async_session() as session:
            inst = await session.execute(
                text("SELECT sector FROM instruments WHERE id = :iid"),
                {"iid": instrument_id},
            )
            inst_row = inst.fetchone()
        sector = inst_row.sector if inst_row else None
        return APIResponse(
            data={
                "pe_ratio": None,
                "roe": None,
                "de_ratio": None,
                "peg_ratio": None,
                "fetched_at": None,
                "sector": sector,
                "config": _build_metric_config(sector),
            },
            timestamp=datetime.now(timezone.utc),
        )

    sector = row.sector

    return APIResponse(
        data={
            "pe_ratio": float(row.pe_ratio) if row.pe_ratio is not None else None,
            "roe": float(row.roe) if row.roe is not None else None,
            "de_ratio": float(row.de_ratio) if row.de_ratio is not None else None,
            "peg_ratio": float(row.peg_ratio) if row.peg_ratio is not None else None,
            "fetched_at": row.fetched_at.isoformat() if row.fetched_at else None,
            "sector": sector,
            "config": _build_metric_config(sector),
        },
        timestamp=datetime.now(timezone.utc),
    )


@router.get("/macro/indicators", response_model=APIResponse)
async def get_macro_indicators():
    """Get latest macro economic indicators with range/direction config."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT DISTINCT ON (indicator_name)
                    indicator_name, value, label, unit, fetched_at
                FROM macro_indicators
                ORDER BY indicator_name, fetched_at DESC
            """)
        )
        rows = result.fetchall()

    indicators = []
    for r in rows:
        cfg = _MACRO_CONFIG.get(r.indicator_name, {})
        indicators.append({
            "name": r.indicator_name,
            "value": float(r.value),
            "label": r.label,
            "unit": r.unit,
            "fetched_at": r.fetched_at.isoformat() if r.fetched_at else None,
            "config": cfg,
        })

    return APIResponse(data=indicators, timestamp=datetime.now(timezone.utc))
