"""Sentiment Analysis Service - FinBERT-based financial sentiment with FastAPI."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from .model import analyze_sentiment, analyze_batch, get_model_and_tokenizer
from .processor import process_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("sentiment")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Pre-load model on startup
    logger.info("Pre-loading FinBERT model...")
    get_model_and_tokenizer()
    logger.info("Model loaded, starting background processor...")
    task = asyncio.create_task(process_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Sentiment Analysis Service", lifespan=lifespan)


class TextRequest(BaseModel):
    text: str


class BatchRequest(BaseModel):
    texts: list[str]


class SentimentResponse(BaseModel):
    positive: float
    negative: float
    neutral: float
    label: str


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/analyze", response_model=SentimentResponse)
async def analyze(request: TextRequest):
    """Analyze sentiment of a single text."""
    result = analyze_sentiment(request.text)
    return result


@app.post("/analyze/batch", response_model=list[SentimentResponse])
async def analyze_many(request: BatchRequest):
    """Analyze sentiment of multiple texts."""
    results = analyze_batch(request.texts)
    return results
