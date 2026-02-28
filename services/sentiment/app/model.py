"""FinBERT model wrapper for financial sentiment analysis."""

import logging
from functools import lru_cache

import os
import json
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import google.generativeai as genai

api_key = os.getenv("GEMINI_API_KEY")
if api_key:
    genai.configure(api_key=api_key)


logger = logging.getLogger(__name__)

MODEL_NAME = "ProsusAI/finbert"
LABELS = ["positive", "negative", "neutral"]

def map_to_5_scale(pos: float, neg: float) -> str:
    score = pos - neg
    if score >= 0.5:
        return "very positive"
    elif score >= 0.1:
        return "positive"
    elif score >= -0.1:
        return "neutral"
    elif score >= -0.5:
        return "negative"
    else:
        return "very negative"



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
    pos, neg, neu = round(scores[0], 6), round(scores[1], 6), round(scores[2], 6)
    
    return {
        "positive": pos,
        "negative": neg,
        "neutral": neu,
        "label": map_to_5_scale(pos, neg),
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
            pos, neg, neu = round(scores[0], 6), round(scores[1], 6), round(scores[2], 6)
            results.append({
                "positive": pos,
                "negative": neg,
                "neutral": neu,
                "label": map_to_5_scale(pos, neg),
            })

    return results

def analyze_asset_specific_batch(texts: list[str], asset_names: list[str]) -> list[dict]:
    """Analyze sentiment specifically targeting the given asset using Gemini."""
    if not api_key:
        return analyze_batch(texts)
    
    results = []
    # Format prompt for Gemini
    prompt = "Analyze the sentiment of the following news articles SPECIFICALLY with respect to the target asset. Determine if the news affects the asset positively or negatively.\n\n"
    for i, (text, asset) in enumerate(zip(texts, asset_names)):
        prompt += f"Article {i}:\nTarget Asset: {asset}\nNews: {text}\n\n"
        
    prompt += """Provide the output as a JSON array of objects, one for each article, in the exact same order.
Each object MUST have the following keys:
- "label": exactly one of ["very positive", "positive", "neutral", "negative", "very negative"] based on the impact on the Target Asset.
- "positive_prob": a float between 0 and 1 representing positive probability (if very positive, e.g. 0.9)
- "negative_prob": a float between 0 and 1 representing negative probability
- "neutral_prob": a float between 0 and 1 representing neutral probability
Note: positive_prob + negative_prob + neutral_prob must equal 1.0.

JSON array:"""

    try:
        model = genai.GenerativeModel('gemini-flash-latest')
        response = model.generate_content(prompt)
        text = response.text
        # Clean markdown json blocks if present
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
            
        data = json.loads(text.strip())
        if len(data) != len(texts):
            logger.warning("Gemini returned %d results for %d texts, falling back to FinBERT", len(data), len(texts))
            return analyze_batch(texts)
        for item in data:
            p = float(item["positive_prob"])
            n = float(item["negative_prob"])
            u = float(item["neutral_prob"])
            # Normalize probabilities to sum to 1.0
            total = p + n + u
            if total > 0:
                p, n, u = p / total, n / total, u / total
            else:
                p, n, u = 0.0, 0.0, 1.0
            results.append({
                "positive": round(p, 6),
                "negative": round(n, 6),
                "neutral": round(u, 6),
                "label": item["label"]
            })
    except Exception as e:
        logger.error(f"Gemini API error during sentiment batch fallback to finbert: {e}")
        return analyze_batch(texts)
        
    return results
