#!/usr/bin/env python3
"""
Re-generate ``ce_get_cost_and_usage_*.json`` from live ``ce:GetCostAndUsage`` calls.

Requires Cost Explorer-enabled AWS credentials. Run from repo root::

    uv run python tests/fixtures/capture_ce_fixtures.py
"""

from __future__ import annotations

import json
from pathlib import Path

import boto3

from aws_cost_lens.core import CE_METRICS_BUNDLE, resolve_effective_metric

_FIXTURE_DIR = Path(__file__).resolve().parent


def _strip_meta(o: object) -> object:
    if isinstance(o, dict):
        return {k: _strip_meta(v) for k, v in o.items() if k != "ResponseMetadata"}
    if isinstance(o, list):
        return [_strip_meta(v) for v in o]
    return o


def main() -> None:
    ce = boto3.client("ce")
    time_period = {"Start": "2026-03-01", "End": "2026-04-01"}
    resp = ce.get_cost_and_usage(
        TimePeriod=time_period,
        Granularity="MONTHLY",
        Metrics=list(CE_METRICS_BUNDLE),
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
    )
    resp = _strip_meta(resp)
    monthly_path = _FIXTURE_DIR / "ce_get_cost_and_usage_monthly_by_service.json"
    monthly_path.write_text(json.dumps(resp, indent=2), encoding="utf-8")

    period = resp["ResultsByTime"][0]
    groups = period["Groups"]
    mid = max(1, len(groups) // 2)
    g1, g2 = groups[:mid], groups[mid:]

    p1 = {
        "GroupDefinitions": resp.get("GroupDefinitions", []),
        "DimensionValueAttributes": resp.get("DimensionValueAttributes", []),
        "ResultsByTime": [
            {
                "TimePeriod": period["TimePeriod"],
                "Total": period.get("Total") or {},
                "Groups": g1,
                "Estimated": period.get("Estimated", False),
            }
        ],
        "NextPageToken": "fixture-derived-page-2",
    }
    p2 = {
        "GroupDefinitions": resp.get("GroupDefinitions", []),
        "DimensionValueAttributes": resp.get("DimensionValueAttributes", []),
        "ResultsByTime": [
            {
                "TimePeriod": period["TimePeriod"],
                "Total": {},
                "Groups": g2,
                "Estimated": period.get("Estimated", False),
            }
        ],
    }
    (_FIXTURE_DIR / "ce_get_cost_and_usage_pagination_page1.json").write_text(
        json.dumps(p1, indent=2), encoding="utf-8"
    )
    (_FIXTURE_DIR / "ce_get_cost_and_usage_pagination_page2.json").write_text(
        json.dumps(p2, indent=2), encoding="utf-8"
    )

    auto = resolve_effective_metric(resp["ResultsByTime"], "auto")
    print(f"Wrote {monthly_path.name} ({len(groups)} service groups)")
    print(f"Wrote pagination page1 ({len(g1)} groups) + page2 ({len(g2)} groups)")
    print(f"resolve_effective_metric(..., 'auto') -> {auto} (update test if this changes)")


if __name__ == "__main__":
    main()
