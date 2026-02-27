import logging
from transformers import pipeline

logger = logging.getLogger(__name__)

# Very lightweight model for zero-shot classification
MODEL_NAME = "typeform/distilbert-base-uncased-mnli"
_pipeline = None

def get_pipeline():
    global _pipeline
    if _pipeline is None:
        logger.info(f"Loading zero-shot model: {MODEL_NAME}")
        _pipeline = pipeline("zero-shot-classification", model=MODEL_NAME)
        logger.info("Model loaded successfully")
    return _pipeline

def check_relevance(title: str, summary: str, category: str, asset_name: str | None = None) -> dict:
    classifier = get_pipeline()
    
    # Text to analyze
    # DistilBERT max length is 512, truncate cautiously (zero-shot focuses on title & start of summary)
    text = f"{title}. {summary}"[:1000]
    
    if category in ('us_politics', 'uk_politics', 'us_finance', 'uk_finance'):
        # Needs to be political or finance related
        labels = ["finance and economics", "politics and government", "lifestyle and entertainment", "irrelevant or spam"]
        result = classifier(text, candidate_labels=labels)
        
        # Check top label
        top_label = result['labels'][0]
        score = result['scores'][0]
        
        is_relevant = top_label in ["finance and economics", "politics and government"]
        
        # Check if the score is somewhat definitive, if it's very close we can be conservative
        if is_relevant and score < 0.4:
            # Not confident
            pass

        # Alternatively just check if lifestyle is very high
        lifestyle_idx = result['labels'].index("lifestyle and entertainment")
        if result['scores'][lifestyle_idx] > 0.5:
            is_relevant = False
            
        return {
            "is_relevant": is_relevant,
            "score": result['scores'][0],
            "reason": top_label
        }
        
    elif category == "asset_specific":
        # Needs to be related to the asset, competitors, or supply chain
        if not asset_name:
            # Fallback
            labels = ["business and finance news", "lifestyle and entertainment"]
            res = classifier(text, candidate_labels=labels)
            return {"is_relevant": res['labels'][0] == "business and finance news", "score": res['scores'][0], "reason": res['labels'][0]}
            
        labels = [f"about {asset_name} or its industry", "completely unrelated to business", "lifestyle and entertainment"]
        
        result = classifier(text, candidate_labels=labels)
        top_label = result['labels'][0]
        score = result['scores'][0]
        
        is_relevant = top_label == f"about {asset_name} or its industry"
        
        # If lifestyle is highest, reject
        if result['labels'][0] == "lifestyle and entertainment" or result['labels'][0] == "completely unrelated to business":
            is_relevant = False

        return {
            "is_relevant": is_relevant,
            "score": score,
            "reason": top_label
        }
        
def check_relevance_batch(titles: list[str], summaries: list[str], categories: list[str], asset_names: list[str | None]) -> list[dict]:
    # Very naive batch loop for simplicity, zero-shot can be batched but labels change per category
    return [check_relevance(t, s, c, a) for t, s, c, a in zip(titles, summaries, categories, asset_names)]
