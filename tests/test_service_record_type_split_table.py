"""Tests for RECORD_TYPE-split SERVICE monthly tables."""

import io
from unittest.mock import patch

from rich.console import Console

from aws_cost_lens.core import _find_matching_period
from aws_cost_lens.summary_bars import (
    BEDROCK_SUBTOTAL_LABEL,
    OTHER_SERVICES_ROW_LABEL,
    _service_rec_coverage_bar,
    build_bedrock_split_table,
    create_service_record_type_split_table,
    is_bedrock_service,
    partition_bedrock_maps,
)


def _table_text(table) -> str:
    buf = io.StringIO()
    Console(file=buf, width=120, force_terminal=True, color_system=None).print(table)
    return buf.getvalue()


def _period(groups_u, groups_c, start="2026-04-01"):
    tp = {"Start": start, "End": "2026-05-01"}
    return (
        {"TimePeriod": tp, "Groups": groups_u},
        {"TimePeriod": tp, "Groups": groups_c},
    )


def test_split_table_merges_usage_and_credits_per_service():
    pu, pc = _period(
        [
            {
                "Keys": ["Amazon ECS"],
                "Metrics": {"UnblendedCost": {"Amount": "100.00", "Unit": "USD"}},
            },
            {
                "Keys": ["Amazon S3"],
                "Metrics": {"UnblendedCost": {"Amount": "50.00", "Unit": "USD"}},
            },
        ],
        [
            {
                "Keys": ["Amazon ECS"],
                "Metrics": {"UnblendedCost": {"Amount": "-80.00", "Unit": "USD"}},
            },
            {
                "Keys": ["AWS Data Transfer"],
                "Metrics": {"UnblendedCost": {"Amount": "-70.00", "Unit": "USD"}},
            },
        ],
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_service_record_type_split_table(
            pu,
            pc,
            console_width=100,
            top=0,
            show_all=False,
            granularity="MONTHLY",
            metric="UnblendedCost",
            record_type_for_period={"Usage": 150.0, "Credit": -150.0, "Refund": 0.0},
            verbose=False,
        )

    text = _table_text(table)
    assert "Amazon ECS" in text
    assert "100.00" in text or "$100.00" in text
    assert "80.00" in text or "70.00" in text
    assert table.row_count == 3


def test_cost_filter_combines_small_services_into_other_row():
    """Per-service gross weight max(u, |c|) below threshold rolls into 'All other services'."""
    pu, pc = _period(
        [
            {
                "Keys": ["Amazon ECS"],
                "Metrics": {"UnblendedCost": {"Amount": "100.00", "Unit": "USD"}},
            },
            {
                "Keys": ["Amazon S3"],
                "Metrics": {"UnblendedCost": {"Amount": "50.00", "Unit": "USD"}},
            },
            {
                "Keys": ["AWS Data Transfer"],
                "Metrics": {"UnblendedCost": {"Amount": "0", "Unit": "USD"}},
            },
        ],
        [
            {
                "Keys": ["Amazon ECS"],
                "Metrics": {"UnblendedCost": {"Amount": "-80.00", "Unit": "USD"}},
            },
            {
                "Keys": ["AWS Data Transfer"],
                "Metrics": {"UnblendedCost": {"Amount": "-70.00", "Unit": "USD"}},
            },
        ],
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_service_record_type_split_table(
            pu,
            pc,
            console_width=100,
            top=0,
            show_all=False,
            granularity="MONTHLY",
            metric="UnblendedCost",
            record_type_for_period=None,
            verbose=False,
            cost_filter_min=60.0,
        )

    text = _table_text(table)
    assert OTHER_SERVICES_ROW_LABEL in text
    assert "Amazon ECS" in text
    assert "AWS Data Transfer" in text
    assert "Amazon S3" not in text
    assert table.row_count == 3


def test_cost_filter_with_top_sends_overflow_major_into_other():
    pu, pc = _period(
        [
            {
                "Keys": ["ZebraSvc"],
                "Metrics": {"UnblendedCost": {"Amount": "100.00", "Unit": "USD"}},
            },
            {
                "Keys": ["YakSvc"],
                "Metrics": {"UnblendedCost": {"Amount": "90.00", "Unit": "USD"}},
            },
            {
                "Keys": ["XraySvc"],
                "Metrics": {"UnblendedCost": {"Amount": "80.00", "Unit": "USD"}},
            },
        ],
        [{"Keys": ["ZebraSvc"], "Metrics": {"UnblendedCost": {"Amount": "0", "Unit": "USD"}}}],
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_service_record_type_split_table(
            pu,
            pc,
            console_width=100,
            top=1,
            show_all=False,
            granularity="MONTHLY",
            metric="UnblendedCost",
            cost_filter_min=10.0,
        )
    text = _table_text(table)
    assert "ZebraSvc" in text
    assert "YakSvc" not in text
    assert "XraySvc" not in text
    assert OTHER_SERVICES_ROW_LABEL in text
    assert table.row_count == 2


def _svc(name: str, amount: str) -> dict:
    return {
        "Keys": [name],
        "Metrics": {"UnblendedCost": {"Amount": amount, "Unit": "USD"}},
    }


def test_separate_bedrock_keeps_small_bedrock_lines_and_subtotals_them():
    """A Bedrock line below the cost filter stays listed, with a billable subtotal."""
    pu, pc = _period(
        [
            _svc("Amazon Bedrock", "12.00"),
            _svc("Claude 3 Haiku (Amazon Bedrock Edition)", "8.00"),
            _svc("Amazon ECS", "100.00"),
            _svc("Amazon S3", "5.00"),
        ],
        [
            _svc("Amazon Bedrock", "-2.00"),
            _svc("Amazon ECS", "-10.00"),
        ],
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_service_record_type_split_table(
            pu,
            pc,
            console_width=100,
            top=0,
            show_all=False,
            granularity="MONTHLY",
            metric="UnblendedCost",
            cost_filter_min=50.0,
            separate_bedrock=True,
        )

    names = [getattr(cell, "plain", str(cell)) for cell in table.columns[0]._cells]
    assert names[0] == "Amazon Bedrock"
    assert names[1] == "Claude 3 Haiku (Amazon Bedrock Edition)"
    assert BEDROCK_SUBTOTAL_LABEL in names[2]
    assert "Amazon ECS" in names
    assert "Amazon S3" not in names
    assert OTHER_SERVICES_ROW_LABEL in names
    assert "$20.00" in str(table.columns[1]._cells[2])
    assert "$-2.00" in str(table.columns[2]._cells[2])


def test_without_separate_bedrock_small_bedrock_folds_into_other():
    pu, pc = _period(
        [
            _svc("Amazon Bedrock", "12.00"),
            _svc("Amazon ECS", "100.00"),
        ],
        [],
    )
    with patch("aws_cost_lens.core.should_show_in_progress", return_value=False):
        table = create_service_record_type_split_table(
            pu,
            pc,
            console_width=100,
            top=0,
            show_all=False,
            granularity="MONTHLY",
            metric="UnblendedCost",
            cost_filter_min=50.0,
        )
    names = [getattr(cell, "plain", str(cell)) for cell in table.columns[0]._cells]
    assert "Amazon Bedrock" not in names
    assert OTHER_SERVICES_ROW_LABEL in names
    assert BEDROCK_SUBTOTAL_LABEL not in names


def test_bedrock_split_table_nets_usage_plus_credits():
    table = build_bedrock_split_table(
        [
            ("October 2026", False, 20.0, -2.0, 100.0, -10.0),
            ("November 2026", True, 5.0, 0.0, 1.0, 0.0),
        ]
    )
    text = _table_text(table)
    assert "Bedrock vs other" in text
    assert "Bedrock net" in text
    assert "$23.00" in text
    assert "$91.00" in text
    assert "(MTD)" in text
    assert "client agreement" in text


def test_is_bedrock_service_matches_marketplace_editions_only():
    assert is_bedrock_service("Amazon Bedrock")
    assert is_bedrock_service("Amazon Bedrock AgentCore")
    assert is_bedrock_service("Claude 3 Haiku (Amazon Bedrock Edition)")
    assert not is_bedrock_service("Amazon ECS")
    assert not is_bedrock_service("AWS Marketplace")


def test_partition_bedrock_maps_sums_family_and_leaves_the_rest():
    bedrock_usage, bedrock_credit, other_usage, other_credit = partition_bedrock_maps(
        {
            "Amazon Bedrock": 12.0,
            "Claude 3 Haiku (Amazon Bedrock Edition)": 8.0,
            "Amazon ECS": 100.0,
        },
        {"Amazon Bedrock": -2.0, "Amazon ECS": -10.0},
    )
    assert (bedrock_usage, bedrock_credit, other_usage, other_credit) == (20.0, -2.0, 100.0, -10.0)


def test_coverage_bar_fully_covered_is_single_color_at_max_scale():
    mx = 1157.57
    t = _service_rec_coverage_bar(1157.57, -1157.57, 120, mx)
    plain = t.plain
    assert "—" not in plain
    assert len(plain) == 40
    assert "█" * 40 == plain


def test_coverage_bar_partially_covered_splits_segments():
    t = _service_rec_coverage_bar(100.0, -30.0, 120, max_magnitude=100.0)
    assert len(t.plain) == 40
    assert t.plain.count("█") == 40


def test_coverage_bar_usage_only_all_out_of_pocket():
    t = _service_rec_coverage_bar(50.0, 0.0, 120, max_magnitude=100.0)
    assert t.plain == "█" * 20


def test_coverage_bar_scales_down_when_not_max_row():
    t = _service_rec_coverage_bar(50.0, 0.0, 120, max_magnitude=1000.0)
    assert len(t.plain) == 2


def test_find_matching_period_falls_back_to_empty_groups():
    canonical = {"TimePeriod": {"Start": "2026-04-01", "End": "2026-05-01"}, "Groups": []}
    other = {
        "TimePeriod": {"Start": "2026-03-01", "End": "2026-04-01"},
        "Groups": [{"Keys": ["X"]}],
    }
    got = _find_matching_period([other], canonical)
    assert got["Groups"] == []
    assert got["TimePeriod"] == canonical["TimePeriod"]
