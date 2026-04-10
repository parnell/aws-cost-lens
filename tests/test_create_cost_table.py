"""Tests for create_cost_table."""

import io
from unittest.mock import patch

from rich.console import Console

from aws_cost_lens.core import create_cost_table


def _table_text(table) -> str:
    buf = io.StringIO()
    Console(file=buf, width=120, force_terminal=True, color_system=None).print(table)
    return buf.getvalue()


def _period(groups, start="2026-03-01"):
    return {
        "TimePeriod": {"Start": start, "End": "2026-04-01"},
        "Groups": groups,
    }


def test_create_cost_table_skips_sub_cent_when_not_show_all():
    period = _period(
        [
            {
                "Keys": ["AmazonCloudWatch"],
                "Metrics": {"UnblendedCost": {"Amount": "0.005", "Unit": "USD"}},
            },
            {
                "Keys": ["AmazonS3"],
                "Metrics": {"UnblendedCost": {"Amount": "10.00", "Unit": "USD"}},
            },
        ]
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_cost_table(period, console_width=80, group_by="SERVICE", limit=0)

    assert table.row_count == 1
    assert next(iter(table.columns[0].cells)) == "AmazonS3"


def test_create_cost_table_show_all_includes_small_amounts():
    period = _period(
        [
            {
                "Keys": ["A"],
                "Metrics": {"UnblendedCost": {"Amount": "0.001", "Unit": "USD"}},
            },
        ]
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_cost_table(
            period, console_width=80, group_by="SERVICE", limit=0, show_all=True
        )

    assert table.row_count == 1


def test_create_cost_table_empty_groups_not_in_progress():
    period = {"TimePeriod": {"Start": "2026-01-01", "End": "2026-02-01"}, "Groups": []}
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_cost_table(period, console_width=80, group_by="SERVICE", limit=0)

    assert table.row_count == 1
    assert "No data found" in _table_text(table)
