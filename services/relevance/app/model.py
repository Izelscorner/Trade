import logging
from transformers import pipeline

logger = logging.getLogger(__name__)

# DeBERTa-v3-base zeroshot: best speed/accuracy balance for zero-shot on CPU.
# ~3x faster than BART-large-MNLI with comparable accuracy on our label set.
MODEL_NAME = "MoritzLaurer/deberta-v3-base-zeroshot-v2.0"
_pipeline = None

# BART-MNLI handles broader labels well; keeping them specific still helps precision
MACRO_LABELS = [
    "macroeconomics, broad financial markets, and economic policy",
    "politics, government, war, and geopolitical conflicts",
    "individual company or stock news",
    "celebrity gossip and entertainment",
]
MACRO_RELEVANT = {"macroeconomics, broad financial markets, and economic policy", "politics, government, war, and geopolitical conflicts"}
MACRO_STOCK_LABEL = "individual company or stock news"
MACRO_ENTERTAINMENT_LABEL = "celebrity gossip and entertainment"


def get_pipeline():
    global _pipeline
    if _pipeline is None:
        logger.info(f"Loading zero-shot model: {MODEL_NAME}")
        _pipeline = pipeline("zero-shot-classification", model=MODEL_NAME)
        logger.info("Model loaded successfully")
    return _pipeline


def _is_macro_relevant(res: dict, candidate_labels: list[str]) -> bool:
    """Determine relevance for macro (politics/finance) articles.

    Macro feeds should only contain broad economic/market news and politics.
    Individual company or stock-specific news belongs in asset_specific, not macro.
    """
    top_label = res['labels'][0]

    # Get scores by label
    scores = dict(zip(res['labels'], res['scores']))
    macro_finance_score = scores.get("macroeconomics, broad financial markets, and economic policy", 0)
    politics_score = scores.get("politics, government, war, and geopolitical conflicts", 0)
    stock_score = scores.get(MACRO_STOCK_LABEL, 0)
    entertainment_score = scores.get(MACRO_ENTERTAINMENT_LABEL, 0)
    relevant_combined = macro_finance_score + politics_score

    # Reject if this is clearly about a single stock/company, not macro
    if top_label == MACRO_STOCK_LABEL and stock_score > 0.4:
        return False
    if stock_score > 0.5 and relevant_combined < 0.4:
        return False

    # Accept if combined macro relevant score is strong
    if relevant_combined > 0.45:
        return True

    # Accept if top label is macro relevant
    if top_label in MACRO_RELEVANT:
        return True

    # Reject entertainment
    if entertainment_score > 0.5 and relevant_combined < 0.25:
        return False

    # Default: only accept if top label is macro relevant
    return top_label in MACRO_RELEVANT


def _is_asset_relevant(res: dict, candidate_labels: list[str]) -> bool:
    """Determine relevance for asset-specific articles."""
    top_label = res['labels'][0]

    if "business and finance news" in candidate_labels:
        return top_label == "business and finance news"

    # Asset-specific labels
    asset_label = candidate_labels[0]  # e.g. "about NVIDIA or its industry"
    scores = dict(zip(res['labels'], res['scores']))
    asset_score = scores.get(asset_label, 0)
    entertainment_score = scores.get("celebrity gossip and entertainment", 0)

    # Accept if asset label wins or has reasonable score
    if top_label == asset_label:
        return True
    if asset_score > 0.35:
        return True

    # Reject only if entertainment clearly dominates
    if entertainment_score > 0.5 and asset_score < 0.25:
        return False

    # If top label is unrelated, reject
    if top_label in ["completely unrelated to business", "celebrity gossip and entertainment"]:
        return False

    return True


def check_relevance(title: str, summary: str, category: str, asset_name: str | None = None) -> dict:
    classifier = get_pipeline()

    text = f"{title}. {summary}"[:1000]

    if category in ('us_politics', 'uk_politics', 'us_finance', 'uk_finance'):
        labels = list(MACRO_LABELS)
        result = classifier(text, candidate_labels=labels)

        is_relevant = _is_macro_relevant(result, labels)

        return {
            "is_relevant": is_relevant,
            "score": result['scores'][0],
            "reason": result['labels'][0]
        }

    elif category == "asset_specific":
        if not asset_name:
            labels = ["business and finance news", "celebrity gossip and entertainment"]
            res = classifier(text, candidate_labels=labels)
            return {
                "is_relevant": res['labels'][0] == "business and finance news",
                "score": res['scores'][0],
                "reason": res['labels'][0],
            }

        labels = [
            f"about {asset_name} or its industry",
            "completely unrelated to business",
            "celebrity gossip and entertainment",
        ]
        result = classifier(text, candidate_labels=labels)

        is_relevant = _is_asset_relevant(result, labels)

        return {
            "is_relevant": is_relevant,
            "score": result['scores'][0],
            "reason": result['labels'][0]
        }


def check_relevance_batch(titles: list[str], summaries: list[str], categories: list[str], asset_names: list[str | None]) -> list[dict]:
    """Optimized batch relevance checking using transformers native batching."""
    classifier = get_pipeline()

    texts = [f"{t}. {s}"[:1000] for t, s in zip(titles, summaries)]

    results = [None] * len(titles)

    # Group by labels to use native batching
    groups = {}
    for i, (cat, asset) in enumerate(zip(categories, asset_names)):
        if cat in ('us_politics', 'uk_politics', 'us_finance', 'uk_finance'):
            labels = tuple(MACRO_LABELS)
        elif cat == "asset_specific":
            if not asset:
                labels = ("business and finance news", "celebrity gossip and entertainment")
            else:
                labels = (
                    f"about {asset} or its industry",
                    "completely unrelated to business",
                    "celebrity gossip and entertainment",
                )
        else:
            labels = ("general news", "celebrity gossip and entertainment")

        if labels not in groups:
            groups[labels] = []
        groups[labels].append(i)

    for labels, indices in groups.items():
        group_texts = [texts[i] for i in indices]
        candidate_labels = list(labels)

        batch_results = classifier(group_texts, candidate_labels=candidate_labels, batch_size=len(group_texts))

        if not isinstance(batch_results, list):
            batch_results = [batch_results]

        for i, res in zip(indices, batch_results):
            top_label = res['labels'][0]
            score = res['scores'][0]

            # Determine relevance based on label set
            if MACRO_ENTERTAINMENT_LABEL in candidate_labels and "macroeconomics, broad financial markets, and economic policy" in candidate_labels:
                is_relevant = _is_macro_relevant(res, candidate_labels)
            elif "business and finance news" in candidate_labels or any("about" in l for l in candidate_labels):
                is_relevant = _is_asset_relevant(res, candidate_labels)
            else:
                is_relevant = top_label == "general news"

            results[i] = {
                "is_relevant": is_relevant,
                "score": score,
                "reason": top_label
            }

    return results
