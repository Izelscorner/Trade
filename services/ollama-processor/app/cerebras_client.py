"""Async Cerebras API client using OpenAI-compatible endpoint."""

import json
import logging
import os

from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY", "")
CEREBRAS_MODEL = os.getenv("CEREBRAS_MODEL", "gpt-oss-120b")

_client: AsyncOpenAI | None = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url="https://api.cerebras.ai/v1",
            api_key=CEREBRAS_API_KEY,
        )
    return _client


async def close_client():
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def generate_json(prompt: str, system: str = "", max_tokens: int = 200) -> dict | None:
    """Call Cerebras API and parse the response as JSON.

    Uses response_format=json_object for reliable structured output.
    """
    client = get_client()

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    try:
        response = await client.chat.completions.create(
            model=CEREBRAS_MODEL,
            messages=messages,
            max_completion_tokens=max_tokens,
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or ""
    except Exception:
        logger.exception("Cerebras API call failed")
        return None

    if not raw:
        return None

    raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        logger.warning("No JSON object in Cerebras response: %s", raw[:200])
        return None

    try:
        return json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        logger.warning("Failed to parse Cerebras JSON: %s", raw[:200])
        return None


async def check_health() -> bool:
    """Verify Cerebras API is reachable."""
    client = get_client()
    try:
        await client.models.list()
        return True
    except Exception:
        logger.warning("Cerebras API health check failed")
        return False
