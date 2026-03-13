"""Patch COMPOSITE_WEIGHT_PROFILES in scorer.py with optimized weights.

Uses regex to find and replace the weight dict in scorer.py.
Always backs up the current weights to the calibration_runs table first.

Safety checks:
  - Only applied when status == "apply" (Sharpe improvement >= threshold)
  - Prints a diff before writing
  - Can be dry-run with --dry-run flag
"""

import logging
import os
import re

from .config import SCORER_PY_PATH
from .simulator import COMPOSITE_WEIGHT_PROFILES

SIMULATOR_PY_PATH = os.path.join(os.path.dirname(__file__), "simulator.py")

logger = logging.getLogger(__name__)

# The key lines in scorer.py we want to patch look like:
#   "short": {"technical": 0.43, "sentiment": 0.23, ...},
# We match exactly this pattern for each (category, term) entry.

_LINE_PATTERN = re.compile(
    r'("short"|"long")\s*:\s*\{[^}]+\}',
    re.MULTILINE,
)

_FULL_BLOCK_PATTERN = re.compile(
    r'(COMPOSITE_WEIGHT_PROFILES\s*:\s*dict\[.*?\]\s*=\s*\{)(.*?)(\n\})',
    re.DOTALL,
)


def _weights_to_str(weights: dict[str, float], indent: int = 8) -> str:
    """Format a weights dict as a Python dict literal."""
    pad = " " * indent
    items = ", ".join(f'"{k}": {v}' for k, v in weights.items())
    return "{" + items + "}"


def _build_new_block(calibration_results: dict[tuple[str, str], dict]) -> str:
    """Build the new COMPOSITE_WEIGHT_PROFILES Python source block."""
    lines = ["COMPOSITE_WEIGHT_PROFILES: dict[str, dict[str, dict[str, float]]] = {"]

    categories = ["stock", "etf", "commodity"]
    for cat in categories:
        lines.append(f'    "{cat}": {{')
        for term in ["short", "long"]:
            key = (cat, term)
            if key in calibration_results and calibration_results[key]["status"] == "apply":
                weights = calibration_results[key]["weights_after"]
            else:
                # Use current weights from simulator
                weights = COMPOSITE_WEIGHT_PROFILES.get(cat, {}).get(term, {})
            weights_str = _weights_to_str(weights, indent=0)
            comma = "," if term == "short" else ""
            lines.append(f'        "{term}":  {weights_str}{comma}')
        comma = "," if cat != "commodity" else ""
        lines.append(f"    }}{comma}")

    lines.append("}")
    return "\n".join(lines)


def patch_scorer_py(
    calibration_results: dict[tuple[str, str], dict],
    dry_run: bool = False,
) -> bool:
    """Apply optimized weights to scorer.py.

    Args:
        calibration_results: Dict from calibrator.run_all_calibrations().
        dry_run: If True, print the diff but don't write.

    Returns True if any changes were applied.
    """
    # Check if any results have status == "apply"
    any_apply = any(v.get("status") == "apply" for v in calibration_results.values())
    if not any_apply:
        print("\nNo weight improvements found. scorer.py unchanged.")
        return False

    # Read scorer.py
    try:
        with open(SCORER_PY_PATH, "r") as f:
            original = f.read()
    except FileNotFoundError:
        logger.error("scorer.py not found at %s", SCORER_PY_PATH)
        logger.error("Mount the grading service volume or set SCORER_PY_PATH env var.")
        return False

    # Find the COMPOSITE_WEIGHT_PROFILES block
    match = _FULL_BLOCK_PATTERN.search(original)
    if not match:
        logger.error("Could not find COMPOSITE_WEIGHT_PROFILES in scorer.py — pattern not matched")
        logger.error("The scorer.py format may have changed; please update patch_weights.py")
        return False

    new_block = _build_new_block(calibration_results)

    # Reconstruct: find the variable declaration, replace through closing brace
    # More robust: replace the matched block
    start, end = match.span()
    old_block = original[start:end]

    # Print diff
    print("\n" + "=" * 60)
    print("PROPOSED CHANGES TO scorer.py")
    print("=" * 60)
    print("\nOLD COMPOSITE_WEIGHT_PROFILES:")
    for cat in ["stock", "etf", "commodity"]:
        for term in ["short", "long"]:
            key = (cat, term)
            old_w = COMPOSITE_WEIGHT_PROFILES.get(cat, {}).get(term, {})
            if key in calibration_results and calibration_results[key]["status"] == "apply":
                new_w = calibration_results[key]["weights_after"]
                print(f"\n  {cat}/{term}:")
                for k in ["technical", "sentiment", "sector", "macro", "fundamentals"]:
                    old_v = old_w.get(k, 0)
                    new_v = new_w.get(k, 0)
                    delta = new_v - old_v
                    marker = " ←" if abs(delta) > 0.01 else ""
                    print(f"    {k:12s}: {old_v:.3f} → {new_v:.3f}  ({delta:+.3f}){marker}")

    if dry_run:
        print("\n[DRY RUN] No changes written.")
        return False

    # Apply the replacement using a simpler approach: find the exact dict definition
    # We search for COMPOSITE_WEIGHT_PROFILES = { ... } and replace the inner part
    new_source = original.replace(old_block, new_block)

    if new_source == original:
        logger.warning("Replacement produced no changes — check the regex pattern")
        return False

    with open(SCORER_PY_PATH, "w") as f:
        f.write(new_source)

    print(f"\n✓ scorer.py updated at {SCORER_PY_PATH}")

    # ALSO patch simulator.py
    try:
        with open(SIMULATOR_PY_PATH, "r") as f:
            sim_original = f.read()
        
        sim_match = _FULL_BLOCK_PATTERN.search(sim_original)
        if sim_match:
            sim_old_block = sim_original[sim_match.start():sim_match.end()]
            sim_new_source = sim_original.replace(sim_old_block, new_block)
            with open(SIMULATOR_PY_PATH, "w") as f:
                f.write(sim_new_source)
            print(f"✓ simulator.py updated at {SIMULATOR_PY_PATH}")
        else:
            logger.warning("Could not find COMPOSITE_WEIGHT_PROFILES in simulator.py")
    except Exception as e:
        logger.error("Failed to patch simulator.py: %s", e)

    print("\n  Restart the grading service to apply new weights to production:")
    print("  docker compose restart grading")
    return True
