"""Async NVIDIA NIM API client for local Qwen model.

NIM provides an OpenAI-compatible API. This client leverages AsyncOpenAI
to interact with the local NIM instance.
"""

import asyncio
import json
import logging
import os
import time

from httpx import Timeout
from openai import AsyncOpenAI, APIStatusError

logger = logging.getLogger(__name__)

# NVIDIA Hosted API Configuration
NIM_BASE_URL = os.getenv("NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")
NIM_MODEL = os.getenv("NIM_MODEL", "qwen/qwen3.5-122b-a10b")

# Local NIM inference usually doesn't require an API key after the model is loaded.
# However, we allow reading it from NIM_API_KEY if required by the infrastructure.
NIM_API_KEY = os.getenv("NIM_API_KEY", "not-needed")

# Rate limiter - NVIDIA Hosted API limits us to 40 requests/minute.
# We enforce a strict 2.0 second delay between any two outgoing requests globally.
CONCURRENCY_LIMIT = int(os.getenv("NIM_CONCURRENCY", "1"))
_semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

# Global rate limit state
_rate_limit_lock = asyncio.Lock()
_last_request_time = 0.0
RATE_LIMIT_DELAY = 2.0  # seconds (=> max 30 requests / 60 seconds)

_client: AsyncOpenAI | None = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=NIM_BASE_URL,
            api_key=NIM_API_KEY,
            max_retries=0,
            timeout=Timeout(120.0, connect=10.0),
        )
    return _client


async def close_client():
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def _enforce_rate_limit():
    """Ensure at least RATE_LIMIT_DELAY seconds pass between consecutive API calls globally."""
    global _last_request_time
    async with _rate_limit_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            await asyncio.sleep(RATE_LIMIT_DELAY - elapsed)
        _last_request_time = time.time()


async def _call_with_retry(
    messages: list[dict],
    max_tokens: int,
    max_attempts: int = 4,
    response_format: dict | None = {"type": "json_object"}
) -> str | None:
    """Fire a chat completion to NVIDIA NIM."""
    client = get_client()
    
    for attempt in range(max_attempts):
        await _enforce_rate_limit()
        async with _semaphore:
            try:
                response = await client.chat.completions.create(
                    model=NIM_MODEL,
                    messages=messages,
                    max_completion_tokens=max_tokens,
                    temperature=0.0,
                    # NIM supports json_object if the model is capable
                    response_format=response_format,
                )
                return response.choices[0].message.content or ""

            except APIStatusError as exc:
                if exc.status_code >= 500 or exc.status_code == 429:
                    wait = 5.0 * (2 ** attempt)
                    logger.warning(
                        "NIM %d on attempt %d/%d — sleeping %.1fs",
                        exc.status_code, attempt + 1, max_attempts, wait,
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error("NIM API error %d: %s", exc.status_code, exc)
                    return None

            except Exception:
                logger.exception("NIM API call failed (attempt %d/%d)", attempt + 1, max_attempts)
                if attempt < max_attempts - 1:
                    await asyncio.sleep(5.0 * (2 ** attempt))

    logger.error("NIM API failed after %d attempts", max_attempts)
    return None


def _extract_json_object(raw: str) -> dict | None:
    """Find and parse the first JSON object in a string."""
    raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        return None
    try:
        return json.loads(raw[start:end + 1])
    except json.JSONDecodeError:
        return None


def _extract_json_array_from_object(raw: str) -> list | None:
    """Parse a JSON object and return the first list value found."""
    raw = raw.strip()
    try:
        obj = json.loads(raw)
        for v in obj.values():
            if isinstance(v, list):
                return v
    except json.JSONDecodeError:
        pass

    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(raw[start:end + 1])
        except json.JSONDecodeError:
            pass
            
    # Partial recovery logic for truncated arrays
    if start != -1:
        partial = raw[start:]
        results = []
        depth = 0
        obj_start = None
        for i, ch in enumerate(partial):
            if ch == "{":
                if depth == 0:
                    obj_start = i
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and obj_start is not None:
                    chunk = partial[obj_start:i + 1]
                    try:
                        results.append(json.loads(chunk))
                    except json.JSONDecodeError:
                        pass
                    obj_start = None
        if results:
            logger.info("NIM salvaged %d items from truncated response", len(results))
            return results

    return None


async def generate_json(prompt: str, system: str = "", max_tokens: int = 300) -> dict | None:
    """Call NIM API and parse the response as a JSON object."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    raw = await _call_with_retry(messages, max_tokens)
    if not raw:
        return None

    result = _extract_json_object(raw)
    if result is None:
        logger.warning("No JSON object in NIM response: %s", raw[:200])
    return result


async def generate_json_array(prompt: str, system: str = "", max_tokens: int = 2000) -> list | None:
    """Call NIM API and parse the response as a JSON array."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    raw = await _call_with_retry(messages, max_tokens)
    if not raw:
        return None

    result = _extract_json_array_from_object(raw)
    if result is None:
        logger.warning("Failed to parse NIM batch JSON: %s", raw[:300])
    return result


async def check_health() -> bool:
    """Verify NVIDIA NIM API is reachable."""
    client = get_client()
    try:
        # Simplest health check is to list models or a tiny completion
        # Fast API /health or /v1/models
        await client.models.list()
        return True
    except Exception:
        return False
