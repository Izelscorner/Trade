"""Grades API endpoints."""

import json
from datetime import datetime, timezone

from fastapi import APIRouter
from sqlalchemy import text

from ..core.db import async_session
from ..schemas import APIResponse, GradeSchema

router = APIRouter()


@router.get("", response_model=APIResponse)
async def list_grades(instrument_id: str | None = None, term: str | None = None):
    """Get latest grades, optionally filtered by instrument and/or term."""
    conditions = []
    params: dict = {}

    if instrument_id:
        conditions.append("g.instrument_id = :iid")
        params["iid"] = instrument_id
    if term:
        conditions.append("g.term = :term")
        params["term"] = term

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    async with async_session() as session:
        result = await session.execute(
            text(f"""
                SELECT DISTINCT ON (g.instrument_id, g.term)
                    g.id, g.instrument_id, i.symbol, i.name, g.term,
                    g.overall_grade, g.overall_score,
                    g.pure_grade, g.pure_score,
                    g.technical_score, g.sentiment_score, g.macro_score, COALESCE(g.sector_score, 0) as sector_score,
                    COALESCE(g.fundamentals_score, 0) as fundamentals_score,
                    g.details, g.graded_at
                FROM grades g
                JOIN instruments i ON i.id = g.instrument_id
                {where_clause}
                ORDER BY g.instrument_id, g.term, g.graded_at DESC
            """),
            params,
        )
        rows = result.fetchall()

    grades = [
        GradeSchema(
            id=str(r.id),
            instrument_id=str(r.instrument_id),
            symbol=r.symbol,
            name=r.name,
            term=r.term,
            overall_grade=r.overall_grade,
            overall_score=float(r.overall_score),
            pure_score=float(r.pure_score) if r.pure_score is not None else None,
            pure_grade=r.pure_grade,
            technical_score=float(r.technical_score),
            sentiment_score=float(r.sentiment_score),
            macro_score=float(r.macro_score),
            sector_score=float(r.sector_score),
            fundamentals_score=float(r.fundamentals_score),
            details=json.loads(r.details) if isinstance(r.details, str) else r.details,
            graded_at=r.graded_at,
        )
        for r in rows
    ]
    return APIResponse(data=[g.model_dump() for g in grades], timestamp=datetime.now(timezone.utc))


@router.get("/history/{instrument_id}", response_model=APIResponse)
async def grade_history(instrument_id: str, term: str = "short", limit: int = 30):
    """Get grade history for an instrument."""
    async with async_session() as session:
        result = await session.execute(
            text("""
                SELECT g.id, g.instrument_id, i.symbol, i.name, g.term,
                    g.overall_grade, g.overall_score,
                    g.pure_grade, g.pure_score,
                    g.technical_score, g.sentiment_score, g.macro_score, COALESCE(g.sector_score, 0) as sector_score,
                    COALESCE(g.fundamentals_score, 0) as fundamentals_score,
                    g.details, g.graded_at
                FROM grades g
                JOIN instruments i ON i.id = g.instrument_id
                WHERE g.instrument_id = :iid AND g.term = :term
                ORDER BY g.graded_at DESC
                LIMIT :limit
            """),
            {"iid": instrument_id, "term": term, "limit": limit},
        )
        rows = result.fetchall()

    grades = [
        GradeSchema(
            id=str(r.id),
            instrument_id=str(r.instrument_id),
            symbol=r.symbol,
            name=r.name,
            term=r.term,
            overall_grade=r.overall_grade,
            overall_score=float(r.overall_score),
            pure_score=float(r.pure_score) if r.pure_score is not None else None,
            pure_grade=r.pure_grade,
            technical_score=float(r.technical_score),
            sentiment_score=float(r.sentiment_score),
            macro_score=float(r.macro_score),
            sector_score=float(r.sector_score),
            fundamentals_score=float(r.fundamentals_score),
            details=json.loads(r.details) if isinstance(r.details, str) else r.details,
            graded_at=r.graded_at,
        )
        for r in rows
    ]
    return APIResponse(data=[g.model_dump() for g in grades], timestamp=datetime.now(timezone.utc))
