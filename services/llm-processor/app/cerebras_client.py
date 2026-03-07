"""Async Cerebras API client with rate limiting and exponential backoff.

Rate limit strategy:
  - Track a token-bucket style leaky window: max RATE_LIMIT_RPM calls per minute.
  - Before every call, acquire a slot from the rate limiter (wait if window is full).
  - On 429: extract Retry-After header, sleep exactly that long, then re-queue.
  - On other transient errors (5xx, network): exponential backoff with jitter.
"""

import asyncio
import json
import logging
import os
import time

from openai import AsyncOpenAI, RateLimitError, APIStatusError

logger = logging.getLogger(__name__)

CEREBRAS_API_KEY = os.getenv("CEREBRAS_API_KEY", "")
CEREBRAS_MODEL = os.getenv("CEREBRAS_MODEL", "gpt-oss-120b")

# ── Rate limiter ──────────────────────────────────────────────────────────────
# Cerebras free tier: 30 RPM.  We target 20 RPM to leave headroom.
RATE_LIMIT_RPM = int(os.getenv("CEREBRAS_RPM", "20"))
_MIN_INTERVAL = 60.0 / RATE_LIMIT_RPM   # seconds between requests
_last_request_time: float = 0.0
_rate_lock = asyncio.Lock()
# ─────────────────────────────────────────────────────────────────────────────

_client: AsyncOpenAI | None = None


def get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        # Disable the SDK's own retry logic — we handle it ourselves so we can
        # respect the rate limiter between attempts.
        _client = AsyncOpenAI(
            base_url="https://api.cerebras.ai/v1",
            api_key=CEREBRAS_API_KEY,
            max_retries=0,          # we retry manually
        )
    return _client


async def close_client():
    global _client
    if _client is not None:
        await _client.close()
        _client = None


async def _acquire_rate_slot() -> None:
    """Block until it is safe to fire the next request under the RPM budget."""
    global _last_request_time
    async with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_request_time
        wait = _MIN_INTERVAL - elapsed
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_time = time.monotonic()


async def _call_with_retry(
    messages: list[dict],
    max_tokens: int,
    max_attempts: int = 4,
) -> str | None:
    """Fire a chat completion, retrying on 429 / transient errors.

    Returns the raw string content from the model, or None on failure.
    Uses explicit exponential backoff so we always respect the rate limiter
    between attempts instead of letting the SDK pause the event loop blindly.
    """
    client = get_client()
    base_delay = 5.0   # seconds

    for attempt in range(max_attempts):
        await _acquire_rate_slot()
        try:
            response = await client.chat.completions.create(
                model=CEREBRAS_MODEL,
                messages=messages,
                max_completion_tokens=max_tokens,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            return response.choices[0].message.content or ""

        except RateLimitError as exc:
            # Try to honour the Retry-After header if provided
            retry_after = _parse_retry_after(exc)
            if retry_after is None:
                retry_after = base_delay * (2 ** attempt)
            # Add small jitter (±10 %)
            jitter = retry_after * 0.1
            wait = retry_after + jitter
            logger.warning(
                "Cerebras 429 on attempt %d/%d — sleeping %.1fs before retry",
                attempt + 1, max_attempts, wait,
            )
            await asyncio.sleep(wait)

        except APIStatusError as exc:
            # 5xx transient errors
            if exc.status_code >= 500:
                wait = base_delay * (2 ** attempt)
                logger.warning(
                    "Cerebras %d on attempt %d/%d — sleeping %.1fs",
                    exc.status_code, attempt + 1, max_attempts, wait,
                )
                await asyncio.sleep(wait)
            else:
                logger.error("Cerebras API error %d: %s", exc.status_code, exc)
                return None

        except Exception:
            logger.exception("Cerebras API call failed (attempt %d/%d)", attempt + 1, max_attempts)
            if attempt < max_attempts - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))

    logger.error("Cerebras API failed after %d attempts", max_attempts)
    return None


def _parse_retry_after(exc: RateLimitError) -> float | None:
    """Try to extract a numeric Retry-After value from the exception."""
    try:
        # The response headers are on exc.response
        header = exc.response.headers.get("retry-after") or exc.response.headers.get("x-ratelimit-reset-requests")
        if header:
            return float(header)
    except Exception:
        pass
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
    """Parse a JSON object and return the first list value found.

    Also attempts partial recovery when the array is truncated — extracts
    all complete JSON objects from within the truncated array so that
    valid items are not discarded.
    """
    raw = raw.strip()

    # Happy path: full valid JSON
    try:
        obj = json.loads(raw)
        for v in obj.values():
            if isinstance(v, list):
                return v
    except json.JSONDecodeError:
        pass

    # Try extracting a complete array substring
    start = raw.find("[")
    end = raw.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(raw[start:end + 1])
        except json.JSONDecodeError:
            pass

    # Partial recovery: the array was truncated, fish out complete {...} objects
    if start != -1:
        partial = raw[start:]
        # Find all complete JSON objects (balanced braces)
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
            logger.info("Partial batch recovery: salvaged %d items from truncated response", len(results))
            return results

    return None


async def generate_json(prompt: str, system: str = "", max_tokens: int = 200) -> dict | None:
    """Call Cerebras API and parse the response as a JSON object."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    raw = await _call_with_retry(messages, max_tokens)
    if not raw:
        return None

    result = _extract_json_object(raw)
    if result is None:
        logger.warning("No JSON object in Cerebras response: %s", raw[:200])
    return result


async def generate_json_array(prompt: str, system: str = "", max_tokens: int = 2000) -> list | None:
    """Call Cerebras API and parse the response as a JSON array.

    The prompt should ask the model to return {"results": [...]} because
    Cerebras json_object mode requires a top-level object.
    """
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    # Use _call_with_retry directly so we can inspect finish_reason
    client = get_client()
    base_delay = 5.0
    max_attempts = 4

    raw: str | None = None
    finish_reason: str = "unknown"

    for attempt in range(max_attempts):
        await _acquire_rate_slot()
        try:
            response = await client.chat.completions.create(
                model=CEREBRAS_MODEL,
                messages=messages,
                max_completion_tokens=max_tokens,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content or ""
            finish_reason = response.choices[0].finish_reason or "unknown"
            break

        except RateLimitError as exc:
            retry_after = _parse_retry_after(exc)
            wait = (retry_after or base_delay * (2 ** attempt)) * 1.1
            logger.warning("Cerebras 429 on attempt %d/%d — sleeping %.1fs", attempt + 1, max_attempts, wait)
            await asyncio.sleep(wait)

        except APIStatusError as exc:
            if exc.status_code >= 500:
                wait = base_delay * (2 ** attempt)
                logger.warning("Cerebras %d on attempt %d/%d — sleeping %.1fs", exc.status_code, attempt + 1, max_attempts, wait)
                await asyncio.sleep(wait)
            else:
                logger.error("Cerebras API error %d: %s", exc.status_code, exc)
                return None

        except Exception:
            logger.exception("Cerebras batch call failed (attempt %d/%d)", attempt + 1, max_attempts)
            if attempt < max_attempts - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))

    if not raw:
        return None

    if finish_reason == "length":
        logger.warning("Cerebras batch response truncated (finish_reason=length) — increase max_tokens or reduce batch size. Raw: %s", raw[:200])

    result = _extract_json_array_from_object(raw)
    if result is None:
        logger.warning("Failed to parse Cerebras batch JSON (finish_reason=%s): %s", finish_reason, raw[:300])
    return result


async def check_health() -> bool:
    """Verify Cerebras API is reachable."""
    client = get_client()
    try:
        await client.models.list()
        return True
    except Exception:
        logger.warning("Cerebras API health check failed")
        return False
