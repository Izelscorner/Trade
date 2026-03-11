"""Pydantic response schemas for the API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Consistent JSON Envelope ---
class APIResponse(BaseModel):
    data: Any = None
    error: str | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# --- Instruments ---
class InstrumentSchema(BaseModel):
    id: str
    symbol: str
    name: str
    category: str
    sector: str | None = None


# --- Sentiment (defined before NewsArticleSchema) ---
class SentimentSchema(BaseModel):
    positive: float
    negative: float
    neutral: float
    label: str
    long_term_label: str | None = None


# --- News ---
class NewsArticleSchema(BaseModel):
    id: str
    title: str
    link: str | None = None
    summary: str | None = None
    source: str
    category: str
    is_macro: bool = False
    is_asset_specific: bool = False
    published_at: datetime | None = None
    sentiment: SentimentSchema | None = None


# --- Grades ---
class GradeSchema(BaseModel):
    id: str
    instrument_id: str
    symbol: str
    name: str
    term: str
    overall_grade: str
    overall_score: float
    technical_score: float
    sentiment_score: float
    macro_score: float
    sector_score: float = 0.0
    fundamentals_score: float = 0.0
    details: dict | None = None
    graded_at: datetime


# --- Prices ---
class LivePriceSchema(BaseModel):
    id: str
    instrument_id: str
    symbol: str
    name: str
    price: float
    change_amount: float | None = None
    change_percent: float | None = None
    market_status: str
    fetched_at: datetime


class HistoricalPriceSchema(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int


# --- Technical ---
class TechnicalIndicatorSchema(BaseModel):
    indicator_name: str
    value: dict
    signal: str
    date: str
    calculated_at: datetime


# --- Dashboard ---
class DashboardInstrumentSchema(BaseModel):
    id: str
    symbol: str
    name: str
    category: str
    sector: str | None = None
    price: float | None = None
    change_amount: float | None = None
    change_percent: float | None = None
    market_status: str | None = None
    short_term_grade: str | None = None
    short_term_score: float | None = None
    long_term_grade: str | None = None
    long_term_score: float | None = None
    graded_at: datetime | None = None


class MacroSentimentSchema(BaseModel):
    region: str
    term: str = "short"
    score: float
    label: str
    article_count: int
    calculated_at: datetime


class SectorSentimentSchema(BaseModel):
    sector: str
    term: str = "short"
    score: float
    label: str
    article_count: int
    calculated_at: datetime


# --- Add Instruments ---
class CreateInstrumentsRequest(BaseModel):
    symbols: str
