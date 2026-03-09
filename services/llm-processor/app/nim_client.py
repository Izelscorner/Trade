"""Async NVIDIA NIM API client for local Qwen model.

NIM provides an OpenAI-compatible API. This client leverages AsyncOpenAI
to interact with the local NIM instance.

Scaling features:
  - Token bucket rate limiter: smoothly distributes up to 40 requests/minute
    while allowing short bursts (up to 3 concurrent requests).
  - Configurable concurrency via NIM_CONCURRENCY env var (default 3).
  - Adaptive backoff on 429/500 errors with jitter.
"""

import asyncio
import json
import logging
import os
import random
import time

from httpx import Timeout
from openai import AsyncOpenAI, APIStatusError

logger = logging.getLogger(__name__)

# NVIDIA Hosted API Configuration
NIM_BASE_URL = os.getenv("NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")
NIM_MODEL = os.getenv("NIM_MODEL", "qwen/qwen3-next-80b-a3b-instruct")

# Local NIM inference usually doesn't require an API key after the model is loaded.
# However, we allow reading it from NIM_API_KEY if required by the infrastructure.
NIM_API_KEY = os.getenv("NIM_API_KEY", "not-needed")

# Rate limiting — start aggressive, back off on 429s via token bucket drain.
RATE_LIMIT_RPM = int(os.getenv("NIM_RATE_LIMIT_RPM", "120"))    # requests per minute
CONCURRENCY_LIMIT = int(os.getenv("NIM_CONCURRENCY", "20"))     # parallel in-flight requests

# Token bucket state
_bucket_lock = asyncio.Lock()
_bucket_tokens = float(RATE_LIMIT_RPM)  # start full
_bucket_max = float(RATE_LIMIT_RPM)
_bucket_last_refill = 0.0
_bucket_refill_rate = RATE_LIMIT_RPM / 60.0  # tokens per second

_semaphore: asyncio.Semaphore | None = None

# Track API call metrics for adaptive batching
_api_call_count = 0
_api_call_window_start = 0.0
_consecutive_429s = 0

_client: AsyncOpenAI | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    return _semaphore


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            base_url=NIM_BASE_URL,
            api_key=NIM_API_KEY,
            max_retries=0,
            timeout=Timeout(300.0, connect=15.0),
        )
    return _client


async def close_client():
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def _acquire_token():
    """Acquire one token from the token bucket, waiting if necessary.

    The token bucket refills at RATE_LIMIT_RPM tokens per minute (spread evenly
    per second). This allows short bursts while capping sustained throughput.
    """
    global _bucket_tokens, _bucket_last_refill

    while True:
        async with _bucket_lock:
            now = time.monotonic()
            if _bucket_last_refill == 0.0:
                _bucket_last_refill = now

            # Refill tokens based on elapsed time
            elapsed = now - _bucket_last_refill
            refill = elapsed * _bucket_refill_rate
            _bucket_tokens = min(_bucket_max, _bucket_tokens + refill)
            _bucket_last_refill = now

            if _bucket_tokens >= 1.0:
                _bucket_tokens -= 1.0
                return

            # Calculate wait time for next token
            wait = (1.0 - _bucket_tokens) / _bucket_refill_rate

        await asyncio.sleep(wait)


def get_api_metrics() -> dict:
    """Return current API call metrics for adaptive batching decisions."""
    global _api_call_count, _api_call_window_start, _consecutive_429s
    now = time.monotonic()
    window = now - _api_call_window_start if _api_call_window_start > 0 else 60.0
    rpm = (_api_call_count / window * 60.0) if window > 0 else 0.0
    return {
        "current_rpm": round(rpm, 1),
        "max_rpm": RATE_LIMIT_RPM,
        "concurrency": CONCURRENCY_LIMIT,
        "consecutive_429s": _consecutive_429s,
        "headroom_pct": round(max(0, (RATE_LIMIT_RPM - rpm) / RATE_LIMIT_RPM * 100), 1),
    }


async def _call_with_retry(
    messages: list[dict],
    max_tokens: int,
    max_attempts: int = 4,
    response_format: dict | None = {"type": "json_object"}
) -> str | None:
    """Fire a chat completion to NVIDIA NIM with token bucket rate limiting."""
    global _api_call_count, _api_call_window_start, _consecutive_429s
    client = get_client()
    sem = _get_semaphore()

    for attempt in range(max_attempts):
        await _acquire_token()
        async with sem:
            try:
                # Track API call metrics
                now = time.monotonic()
                if _api_call_window_start == 0.0 or (now - _api_call_window_start) > 60.0:
                    _api_call_count = 0
                    _api_call_window_start = now
                _api_call_count += 1

                response = await client.chat.completions.create(
                    model=NIM_MODEL,
                    messages=messages,
                    max_completion_tokens=max_tokens,
                    temperature=0.0,
                    # NIM supports json_object if the model is capable
                    response_format=response_format,
                )
                _consecutive_429s = 0
                return response.choices[0].message.content or ""

            except APIStatusError as exc:
                if exc.status_code == 429:
                    _consecutive_429s += 1
                    # Exponential backoff with jitter, more aggressive for repeated 429s
                    base_wait = 3.0 * (2 ** min(attempt, 3))
                    jitter = random.uniform(0, base_wait * 0.3)
                    wait = base_wait + jitter
                    # If we're getting many 429s, slow down the bucket
                    if _consecutive_429s >= 3:
                        global _bucket_tokens
                        async with _bucket_lock:
                            _bucket_tokens = max(0, _bucket_tokens - 2)
                    logger.warning(
                        "NIM 429 (attempt %d/%d, consecutive=%d) — sleeping %.1fs",
                        attempt + 1, max_attempts, _consecutive_429s, wait,
                    )
                    await asyncio.sleep(wait)
                elif exc.status_code >= 500:
                    wait = 5.0 * (2 ** attempt) + random.uniform(0, 2)
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
