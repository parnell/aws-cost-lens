"""
Cost Explorer (``ce``) API fixtures: JSON from ``GetCostAndUsage``.

Pagination files are one live month split across two pages for merge tests.

Re-capture: ``uv run python tests/fixtures/capture_ce_fixtures.py`` (needs AWS creds).
"""

import json
from pathlib import Path

_FIXTURE_DIR = Path(__file__).resolve().parent


def load_ce_fixture(name: str) -> dict:
    """Load ``{name}.json`` from this folder (``name`` without ``.json`` suffix)."""
    path = _FIXTURE_DIR / f"{name}.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)
