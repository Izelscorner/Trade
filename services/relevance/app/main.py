import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel
from .model import check_relevance, get_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("relevance")

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Pre-loading zero-shot classification model...")
    get_pipeline()
    logger.info("Model loaded successfully.")
    yield

app = FastAPI(title="Relevance Checking Service", lifespan=lifespan)

class RelevanceRequest(BaseModel):
    title: str
    summary: str
    category: str
    asset_name: str | None = None

class RelevanceResponse(BaseModel):
    is_relevant: bool
    score: float
    reason: str | None = None

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/check", response_model=RelevanceResponse)
async def check(request: RelevanceRequest):
    """Check if the text is relevant based on the category and asset_name."""
    result = check_relevance(
        title=request.title,
        summary=request.summary,
        category=request.category,
        asset_name=request.asset_name
    )
    return RelevanceResponse(**result)

class BatchRelevanceRequest(BaseModel):
    articles: list[RelevanceRequest]

@app.post("/check/batch", response_model=list[RelevanceResponse])
async def check_batch(request: BatchRelevanceRequest):
    """Check relevance for a batch of articles."""
    # Process sequentially for simplicity, or parallel
    from .model import check_relevance_batch
    titles = [a.title for a in request.articles]
    summaries = [a.summary for a in request.articles]
    categories = [a.category for a in request.articles]
    asset_names = [a.asset_name for a in request.articles]
    
    results = check_relevance_batch(titles, summaries, categories, asset_names)
    return [RelevanceResponse(**r) for r in results]
