"""FinBERT model wrapper for financial sentiment analysis."""

import logging
from functools import lru_cache

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

MODEL_NAME = "ProsusAI/finbert"
LABELS = ["positive", "negative", "neutral"]


@lru_cache(maxsize=1)
def get_model_and_tokenizer():
    """Load FinBERT model and tokenizer (cached singleton)."""
    logger.info("Loading FinBERT model: %s", MODEL_NAME)
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    model.eval()
    logger.info("FinBERT model loaded successfully")
    return model, tokenizer


def analyze_sentiment(text: str) -> dict:
    """Analyze sentiment of a single text string.

    Returns dict with positive, negative, neutral scores and label.
    """
    model, tokenizer = get_model_and_tokenizer()

    # Truncate to model max length
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=512, padding=True)

    with torch.no_grad():
        outputs = model(**inputs)
        probs = torch.nn.functional.softmax(outputs.logits, dim=-1)

    scores = probs[0].tolist()
    label_idx = scores.index(max(scores))

    return {
        "positive": round(scores[0], 6),
        "negative": round(scores[1], 6),
        "neutral": round(scores[2], 6),
        "label": LABELS[label_idx],
    }


def analyze_batch(texts: list[str], batch_size: int = 16) -> list[dict]:
    """Analyze sentiment for a batch of texts."""
    model, tokenizer = get_model_and_tokenizer()
    results = []

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        inputs = tokenizer(batch, return_tensors="pt", truncation=True, max_length=512, padding=True)

        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.nn.functional.softmax(outputs.logits, dim=-1)

        for prob in probs:
            scores = prob.tolist()
            label_idx = scores.index(max(scores))
            results.append({
                "positive": round(scores[0], 6),
                "negative": round(scores[1], 6),
                "neutral": round(scores[2], 6),
                "label": LABELS[label_idx],
            })

    return results
