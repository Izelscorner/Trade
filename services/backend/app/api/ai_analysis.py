"""AI Analysis API endpoints using Gemini."""

import os
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
from sqlalchemy import text
import google.generativeai as genai

from ..core.db import async_session
from ..schemas import APIResponse

router = APIRouter()

# Configure Gemini
api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)

@router.get("/{instrument_id}", response_model=APIResponse)
async def get_ai_analysis(instrument_id: str):
    """Generate a deep AI analysis for an instrument using Gemini."""
    if not api_key:
        return APIResponse(error="Gemini API key not configured", timestamp=datetime.now(timezone.utc))

    async with async_session() as session:
        # 1. Get Instrument Info
        res = await session.execute(
            text("SELECT symbol, name, category FROM instruments WHERE id = :iid"),
            {"iid": instrument_id}
        )
        instrument = res.fetchone()
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument not found")

        # 2. Get Latest News (last 10 articles)
        res = await session.execute(
            text("""
                SELECT n.title, n.summary, s.label as sentiment,
                       (s.positive - s.negative) as score
                FROM news_articles n
                JOIN news_instrument_map m ON n.id = m.article_id
                LEFT JOIN sentiment_scores s ON n.id = s.article_id
                WHERE m.instrument_id = :iid
                ORDER BY n.published_at DESC
                LIMIT 10
            """),
            {"iid": instrument_id}
        )
        news = res.fetchall()

        # 3. Get Technical Indicators
        res = await session.execute(
            text("""
                SELECT indicator_name, value, signal, calculated_at
                FROM technical_indicators
                WHERE instrument_id = :iid
                ORDER BY calculated_at DESC
                LIMIT 20
            """),
            {"iid": instrument_id}
        )
        technicals = res.fetchall()

        # 4. Get Current Grades
        res = await session.execute(
            text("""
                SELECT term, overall_grade as grade, overall_score as score, technical_score, sentiment_score, macro_score
                FROM grades
                WHERE instrument_id = :iid
                ORDER BY graded_at DESC
                LIMIT 2
            """),
            {"iid": instrument_id}
        )
        grades = res.fetchall()

    # Construct Prompt
    news_context = "\n".join([
        f"- {n.title} (Sentiment: {n.sentiment}, Score: {n.score})"
        for n in news
    ])
    
    tech_context = "\n".join([
        f"- {t.indicator_name}: {t.value} (Signal: {t.signal})"
        for t in technicals
    ])

    grade_context = "\n".join([
        f"- {g.term.upper()} Term: Grade {g.grade} (Score: {g.score}, Tech: {g.technical_score}, Sent: {g.sentiment_score}, Macro: {g.macro_score})"
        for g in grades
    ])

    prompt = f"""
Deeply analyze {instrument.name} ({instrument.symbol}) for long-term and short-term investment.

CONTEXT DATA:

CURRENT SYSTEM GRADES:
{grade_context}

LATEST NEWS & SENTIMENT:
{news_context}

TECHNICAL INDICATORS:
{tech_context}

INSTRUCTIONS:
1. Provide a comprehensive executive summary.
2. Analyze short-term potential (next 1-2 weeks) based on technicals and recent sentiment.
3. Analyze long-term potential (6-12 months) based on fundamental context and macro sentiment.
4. Identify key risks and opportunities.
5. Provide a final "AI Recommendation" (Strong Buy, Buy, Hold, Sell, Strong Sell).

Format the output in clean Markdown.
"""

    try:
        model = genai.GenerativeModel('gemini-flash-latest')
        response = model.generate_content(prompt)
        analysis_text = response.text
        
        return APIResponse(
            data={"analysis": analysis_text},
            timestamp=datetime.now(timezone.utc)
        )
    except Exception as e:
        return APIResponse(
            error=f"Gemini Analysis Error: {str(e)}",
            timestamp=datetime.now(timezone.utc)
        )
@router.get("/independent/{instrument_id}", response_model=APIResponse)
async def get_independent_ai_analysis(instrument_id: str):
    """Generate an independent AI analysis for an instrument using Gemini's knowledge."""
    if not api_key:
        return APIResponse(error="Gemini API key not configured", timestamp=datetime.now(timezone.utc))

    async with async_session() as session:
        res = await session.execute(
            text("SELECT symbol, name FROM instruments WHERE id = :iid"),
            {"iid": instrument_id}
        )
        instrument = res.fetchone()
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument not found")

    prompt = f"Deeply analyze {instrument.name} ({instrument.symbol}) asset for long term and short term investment."

    try:
        model = genai.GenerativeModel('gemini-flash-latest')
        response = model.generate_content(prompt)
        analysis_text = response.text
        
        return APIResponse(
            data={"analysis": analysis_text},
            timestamp=datetime.now(timezone.utc)
        )
    except Exception as e:
        return APIResponse(
            error=f"Gemini Independent Analysis Error: {str(e)}",
            timestamp=datetime.now(timezone.utc)
        )
