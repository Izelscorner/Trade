"""AI Analysis API endpoints using Gemini."""

import os
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
from sqlalchemy import text
import httpx

from ..core.db import async_session
from ..schemas import APIResponse

router = APIRouter()

# Route requests through the internal LLM processor proxy to share the 40 RPM global lock
LLM_PROCESSOR_URL = "http://llm-processor:8003/v1/chat/completions"

async def _call_llm_proxy(messages: list[dict], max_tokens: int = 2000) -> str:
    """Call the internal LLM Processor proxy to get AI completions."""
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.post(
            LLM_PROCESSOR_URL,
            json={"messages": messages, "max_tokens": max_tokens}
        )
        response.raise_for_status()
        data = response.json()
        return data.get("content", "")

@router.get("/{instrument_id}", response_model=APIResponse)
async def get_ai_analysis(instrument_id: str):
    """Generate a deep AI analysis for an instrument using the NVIDIA API."""

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
                JOIN sentiment_scores s ON n.id = s.article_id
                WHERE m.instrument_id = :iid
                AND n.ollama_processed = true
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
        messages = [
            {"role": "system", "content": "You are a senior quantitative analyst and portfolio manager at a top-tier Western investment bank. You provide crisp, actionable, and deeply analytical investment advice."},
            {"role": "user", "content": prompt}
        ]
        analysis_text = await _call_llm_proxy(messages, max_tokens=2000)
        
        return APIResponse(
            data={"analysis": analysis_text},
            timestamp=datetime.now(timezone.utc)
        )
    except Exception as e:
        return APIResponse(
            error=f"NVIDIA Analysis Error: {str(e)}",
            timestamp=datetime.now(timezone.utc)
        )
@router.get("/independent/{instrument_id}", response_model=APIResponse)
async def get_independent_ai_analysis(instrument_id: str):
    """Generate an independent AI analysis for an instrument using the NVIDIA API's knowledge base."""

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
        messages = [
            {"role": "system", "content": "You are a senior quantitative analyst. You provide detailed investment dossiers based purely on your fundamental knowledge of global assets."},
            {"role": "user", "content": prompt}
        ]
        analysis_text = await _call_llm_proxy(messages, max_tokens=2000)
        
        return APIResponse(
            data={"analysis": analysis_text},
            timestamp=datetime.now(timezone.utc)
        )
    except Exception as e:
        return APIResponse(
            error=f"NVIDIA Independent Analysis Error: {str(e)}",
            timestamp=datetime.now(timezone.utc)
        )
