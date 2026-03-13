"""Configuration from environment variables."""

import os

DATABASE_URL: str = os.environ["DATABASE_URL"]
FRED_API_KEY: str = os.environ.get("FRED_API_KEY", "")

# NIM (NVIDIA API) — same as production llm-processor
NIM_API_KEY: str = os.environ.get("NIM_API_KEY", "")
NIM_BASE_URL: str = os.environ.get("NIM_BASE_URL", "https://integrate.api.nvidia.com/v1")
NIM_MODEL: str = os.environ.get("NIM_MODEL", "qwen/qwen3.5-122b-a10b")


# Backtest date range
BACKTEST_START: str = os.environ.get("BACKTEST_START", "2023-01-01")
BACKTEST_END: str = os.environ.get("BACKTEST_END", "2026-03-01")

# How many days between sampled backtest dates (5 = weekly trading days)
SAMPLE_EVERY_N_DAYS: int = int(os.environ.get("SAMPLE_EVERY_N_DAYS", "5"))

# Path to scorer.py for patching (set via Docker volume mount)
SCORER_PY_PATH: str = os.environ.get("SCORER_PY_PATH", "/scorer/scorer.py")
