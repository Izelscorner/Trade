"""Optimized async Ollama HTTP client for Llama 3.2 1B."""

import json
import logging
import os
import re

import aiohttp

logger = logging.getLogger(__name__)

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")

# Persistent session for connection pooling
_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    """Get or create a persistent aiohttp session for Ollama calls."""
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=60),
            connector=aiohttp.TCPConnector(limit=4, keepalive_timeout=300),
        )
    return _session


async def close_session():
    """Close the persistent session."""
    global _session
    if _session and not _session.closed:
        await _session.close()
        _session = None


async def generate(
    prompt: str,
    max_tokens: int = 100,
    temperature: float = 0.0,
    force_json: bool = False,
) -> str:
    """Call Ollama generate API and return the response text.

    Args:
        prompt: The prompt to send.
        max_tokens: Maximum tokens to generate (keep low for classification).
        temperature: 0.0 for deterministic output.
        force_json: If True, use Ollama's JSON format mode.

    Returns:
        Raw response text from the model.
    """
    session = await get_session()

    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": max_tokens,
            "temperature": temperature,
            "top_p": 0.9,
            "repeat_penalty": 1.1,
            "num_ctx": 2048,
            "num_thread": 2,
        },
    }
    if force_json:
        payload["format"] = "json"

    try:
        async with session.post(f"{OLLAMA_URL}/api/generate", json=payload) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                logger.error("Ollama returned status %d: %s", resp.status, error_text[:200])
                return ""
            data = await resp.json()
            return data.get("response", "").strip()
    except Exception:
        logger.exception("Ollama API call failed")
        return ""


async def generate_json(prompt: str, max_tokens: int = 100) -> dict | None:
    """Call Ollama and parse the response as JSON.

    Uses Ollama's JSON format mode for reliable output, with fallback parsing.
    """
    raw = await generate(prompt, max_tokens=max_tokens, temperature=0.0, force_json=True)

    if not raw:
        return None

    # Strip markdown code blocks if present
    if "```json" in raw:
        raw = raw.split("```json")[1].split("```")[0]
    elif "```" in raw:
        raw = raw.split("```")[1].split("```")[0]

    # Try to find JSON object in the response
    raw = raw.strip()

    # Find the first { and last } to extract JSON
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1:
        logger.warning("No JSON object found in Ollama response: %s", raw[:200])
        return None

    json_str = raw[start:end + 1]

    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        # Try to recover truncated JSON (common with token limits)
        recovered = _recover_truncated_json(raw[start:])
        if recovered:
            return recovered
        logger.warning("Failed to parse Ollama JSON response: %s", json_str[:200])
        return None


def _recover_truncated_json(raw: str) -> dict | None:
    """Try to recover a truncated JSON classification response.

    Extracts type, is_macro, and instruments from partial JSON using regex.
    """
    try:
        # Extract "type"
        type_match = re.search(r'"type"\s*:\s*"(\w+)"', raw)
        article_type = type_match.group(1) if type_match else "news"

        # Extract "is_macro"
        macro_match = re.search(r'"is_macro"\s*:\s*(true|false)', raw)
        is_macro = macro_match.group(1) == "true" if macro_match else False

        # Extract instruments — try to find symbol keys in dicts or strings
        instruments = []
        # Match symbols in dict format: {"AAPL": "...", "NVDA": "..."}
        dict_symbols = re.findall(r'"([A-Z]{2,5})"\s*:', raw)
        # Filter out known JSON keys
        json_keys = {"type", "instruments", "is_macro", "sentiment", "confidence"}
        dict_symbols = [s for s in dict_symbols if s not in json_keys]

        if dict_symbols:
            instruments = dict_symbols
        else:
            # Match symbols in list format: ["AAPL", "NVDA"]
            list_symbols = re.findall(r'"([A-Z]{2,5})"', raw)
            instruments = [s for s in list_symbols if s not in json_keys and s not in {"news", "spam"}]

        return {"type": article_type, "instruments": instruments, "is_macro": is_macro}
    except Exception:
        return None


async def check_health() -> bool:
    """Check if Ollama is running and the model is available."""
    try:
        session = await get_session()
        async with session.get(f"{OLLAMA_URL}/api/tags") as resp:
            if resp.status != 200:
                return False
            data = await resp.json()
            models = [m.get("name", "") for m in data.get("models", [])]
            return any(MODEL in m for m in models)
    except Exception:
        return False
