"""Linked-account split and the multi-account grand total bar."""

import io
from unittest.mock import patch

from rich.console import Console

from aws_cost_lens.core import (
    collapse_linked_account_groups,
    format_linked_account_label,
    split_by_linked_account,
)
from aws_cost_lens.summary_bars import (
    AccountGrandSlice,
    account_color_pair,
    build_accounts_grand_total_table,
    build_monthly_summary_table,
    create_service_record_type_split_table,
    multi_account_coverage_bar,
)


def _table_text(table) -> str:
    buf = io.StringIO()
    Console(file=buf, width=120, force_terminal=True, color_system=None).print(table)
    return buf.getvalue()


def _span_color(style) -> str:
    if isinstance(style, str):
        return style
    color = getattr(style, "color", None)
    if color is None:
        return ""
    return str(getattr(color, "name", None) or color)


def _span_link(style) -> str | None:
    if isinstance(style, str):
        return None
    return getattr(style, "link", None)


def _group(keys: list[str], amount: str) -> dict:
    return {
        "Keys": keys,
        "Metrics": {"UnblendedCost": {"Amount": amount, "Unit": "USD"}},
    }


def _payload(*groups: dict, start: str = "2024-01-01") -> dict:
    return {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": start, "End": "2024-02-01"},
                "Groups": list(groups),
            }
        ]
    }


def test_split_by_linked_account_keeps_service_key():
    payload = _payload(
        _group(["111111111111", "Amazon S3"], "10"),
        _group(["222222222222", "Amazon EC2"], "4"),
        _group(["111111111111", "Amazon EC2"], "6"),
    )
    by_account = split_by_linked_account(payload)
    assert set(by_account) == {"111111111111", "222222222222"}
    keys = [g["Keys"] for g in by_account["111111111111"]["ResultsByTime"][0]["Groups"]]
    assert keys == [["Amazon S3"], ["Amazon EC2"]]
    assert by_account["222222222222"]["ResultsByTime"][0]["Groups"][0]["Keys"] == ["Amazon EC2"]


def test_split_includes_empty_period_for_account_missing_that_month():
    payload = {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2024-01-01", "End": "2024-02-01"},
                "Groups": [_group(["111111111111", "Amazon S3"], "10")],
            },
            {
                "TimePeriod": {"Start": "2024-02-01", "End": "2024-03-01"},
                "Groups": [_group(["222222222222", "Amazon S3"], "3")],
            },
        ]
    }
    by_account = split_by_linked_account(payload)
    assert by_account["111111111111"]["ResultsByTime"][1]["Groups"] == []
    assert by_account["222222222222"]["ResultsByTime"][0]["Groups"] == []


def test_collapse_sums_record_types_across_accounts():
    payload = _payload(
        _group(["111111111111", "Usage"], "100"),
        _group(["222222222222", "Usage"], "25"),
        _group(["111111111111", "Credit"], "-40"),
    )
    collapsed = collapse_linked_account_groups(payload)
    groups = collapsed["ResultsByTime"][0]["Groups"]
    amounts = {tuple(g["Keys"]): float(g["Metrics"]["UnblendedCost"]["Amount"]) for g in groups}
    assert amounts["Usage",] == 125
    assert amounts["Credit",] == -40


def test_format_linked_account_label():
    assert format_linked_account_label("111", {"111": "prod"}) == "prod (111)"
    assert format_linked_account_label("111", {}) == "111"


def test_account_color_pairs_are_distinct_greens_and_reds():
    first = account_color_pair(0)
    second = account_color_pair(1)
    assert first == ("green", "red")
    assert second[0] != first[0]
    assert second[1] != first[1]
    assert "green" in second[0]
    assert "red" in second[1]


def test_grand_total_bar_stacks_each_accounts_colors():
    bar = multi_account_coverage_bar(
        [
            (100.0, -100.0, "green", "red"),
            (100.0, 0.0, "bright_green", "bright_red"),
        ],
        120,
        200.0,
    )
    styles = [(span.style, span.end - span.start) for span in bar.spans]
    assert styles == [("green", 20), ("bright_red", 20)]


def test_bar_section_hover_names_account_and_cost():
    """Option-hover (OSC 8) on a section: that account and usage + credits."""
    bar = multi_account_coverage_bar(
        [
            (100.0, -40.0, "green", "red", "prod (111111111111)"),
            (25.0, 0.0, "bright_green", "bright_red", "dev (222222222222)"),
        ],
        120,
        125.0,
    )
    links = [_span_link(span.style) for span in bar.spans]
    prod = "u:account:prod-(111111111111),cost-(usage-and-credits):$60.00"
    dev = "u:account:dev-(222222222222),cost-(usage-and-credits):$25.00"
    assert links[0] == prod
    assert links[1] == prod
    assert links[-1] == dev
    assert _span_color(bar.spans[0].style) == "green"
    assert _span_color(bar.spans[-1].style) == "bright_red"


def test_service_bar_stacks_account_colors():
    usage = {
        "TimePeriod": {"Start": "2024-01-01", "End": "2024-02-01"},
        "Groups": [
            {"Keys": ["Amazon EC2"], "Metrics": {"UnblendedCost": {"Amount": "200", "Unit": "USD"}}}
        ],
    }
    credit = {
        "TimePeriod": {"Start": "2024-01-01", "End": "2024-02-01"},
        "Groups": [
            {
                "Keys": ["Amazon EC2"],
                "Metrics": {"UnblendedCost": {"Amount": "-100", "Unit": "USD"}},
            }
        ],
    }
    table = create_service_record_type_split_table(
        usage,
        credit,
        console_width=120,
        top=0,
        show_all=False,
        granularity="MONTHLY",
        metric="UnblendedCost",
        service_account_slices={
            "Amazon EC2": [
                (100.0, -100.0, "green", "red"),
                (100.0, 0.0, "bright_green", "bright_red"),
            ]
        },
    )
    bar = table.columns[3]._cells[0]
    styles = [span.style for span in bar.spans]
    assert "green" in styles
    assert "bright_red" in styles


def test_monthly_summary_bars_stack_account_colors():
    table = build_monthly_summary_table(
        [("January 2024", 85.0, False, 125.0, -40.0)],
        85.0,
        125.0,
        -40.0,
        120,
        False,
        account_bars=[
            [
                (100.0, -40.0, "green", "red"),
                (25.0, 0.0, "bright_green", "bright_red"),
            ]
        ],
    )
    grand_bar = table.columns[4]._cells[0]
    month_bar = table.columns[4]._cells[1]
    assert "green" in [span.style for span in grand_bar.spans]
    assert "bright_red" in [span.style for span in grand_bar.spans]
    assert "bright_green" in [span.style for span in month_bar.spans] or "bright_red" in [
        span.style for span in month_bar.spans
    ]


def test_accounts_grand_total_table_names_accounts_and_total():
    table = build_accounts_grand_total_table(
        [
            AccountGrandSlice("111111111111", "prod (111111111111)", 60, 100, -40, "green", "red"),
            AccountGrandSlice(
                "222222222222", "dev (222222222222)", 25, 25, 0, "bright_green", "bright_red"
            ),
        ],
        120,
        "2024-01-01",
        "2024-02-01",
    )
    text = _table_text(table)
    assert "Grand total by account" in text
    assert "2024-01-01 to 2024-02-01" in text
    assert "prod (111111111111)" in text
    assert "dev (222222222222)" in text
    assert "GRAND TOTAL" in text
    assert "Accounts included" in text
    grand_bar = table.columns[4]._cells[-1]
    colors = [_span_color(span.style) for span in grand_bar.spans]
    assert "green" in colors
    assert "bright_red" in colors
    links = [_span_link(span.style) for span in grand_bar.spans]
    assert "u:account:prod-(111111111111),cost-(usage-and-credits):$60.00" in links
    assert "u:account:dev-(222222222222),cost-(usage-and-credits):$25.00" in links
    assert "Hold Option" in text


def _linked_payload(rows: list[tuple[str, str, str]]) -> dict:
    return _payload(*[_group([account, name], amount) for account, name, amount in rows])


def test_simple_report_lists_each_account_then_grand_total():
    from aws_cost_lens.core import analyze_costs_simple

    payloads = {
        "simple:service": _linked_payload(
            [
                ("111111111111", "Amazon EC2", "60"),
                ("222222222222", "Amazon S3", "25"),
            ]
        ),
        "simple:service+record_usage": _linked_payload(
            [
                ("111111111111", "Amazon EC2", "100"),
                ("222222222222", "Amazon S3", "25"),
            ]
        ),
        "simple:service+record_credit": _linked_payload(
            [("111111111111", "Amazon EC2", "-40")]
        ),
        "simple:record_type": _linked_payload(
            [
                ("111111111111", "Usage", "100"),
                ("111111111111", "Credit", "-40"),
                ("222222222222", "Usage", "25"),
            ]
        ),
    }
    group_bys: list = []

    def fake_get_cost_data(*args, **kwargs):
        group_bys.append(args[3])
        return payloads[kwargs["ce_api_label"]]

    buf = io.StringIO()
    console = Console(file=buf, width=120, force_terminal=True, color_system="standard")

    with (
        patch("aws_cost_lens.core.get_cost_data", side_effect=fake_get_cost_data),
        patch("aws_cost_lens.core.Console", return_value=console),
        patch("aws_cost_lens.core.get_account_header_markup", return_value="Account: payer"),
        patch("aws_cost_lens.core.print_amazon_credits_section"),
        patch(
            "aws_cost_lens.core.lookup_organization_account_names",
            return_value={"111111111111": "prod", "222222222222": "dev"},
        ),
    ):
        analyze_costs_simple(
            "2024-01-01",
            "2024-02-01",
            None,
            metric_preference="unblended",
            reconcile=False,
            verbose=False,
        )

    text = buf.getvalue()
    assert group_bys[0] == ["LINKED_ACCOUNT", "SERVICE"]
    assert ["LINKED_ACCOUNT", "RECORD_TYPE"] in group_bys
    assert "Accounts included" in text
    assert "prod (111111111111)" in text
    assert "dev (222222222222)" in text
    assert "Grand total by account" in text
    assert "2024-01-01 to 2024-02-01" in text
    assert "all accounts" in text
    assert "GRAND TOTAL" in text
    assert "Amazon EC2" in text
    assert "Amazon S3" in text
    assert "u:account:prod-(111111111111),cost-(usage-and-credits):$60.00" in text
    assert "u:account:dev-(222222222222),cost-(usage-and-credits):$25.00" in text


def test_simple_report_single_account_skips_cross_account_total():
    from aws_cost_lens.core import analyze_costs_simple

    payloads = {
        "simple:service": _linked_payload([("111111111111", "Amazon EC2", "60")]),
        "simple:service+record_usage": _linked_payload([("111111111111", "Amazon EC2", "100")]),
        "simple:service+record_credit": _linked_payload([("111111111111", "Amazon EC2", "-40")]),
        "simple:record_type": _linked_payload(
            [
                ("111111111111", "Usage", "100"),
                ("111111111111", "Credit", "-40"),
            ]
        ),
    }

    def fake_get_cost_data(*args, **kwargs):
        return payloads[kwargs["ce_api_label"]]

    buf = io.StringIO()
    console = Console(file=buf, width=120, force_terminal=False, color_system=None, no_color=True)
    with (
        patch("aws_cost_lens.core.get_cost_data", side_effect=fake_get_cost_data),
        patch("aws_cost_lens.core.Console", return_value=console),
        patch("aws_cost_lens.core.get_account_header_markup", return_value="Account: only"),
        patch("aws_cost_lens.core.print_amazon_credits_section"),
        patch("aws_cost_lens.core.lookup_organization_account_names") as lookup,
    ):
        analyze_costs_simple(
            "2024-01-01",
            "2024-02-01",
            None,
            metric_preference="unblended",
            reconcile=False,
        )

    text = buf.getvalue()
    assert "Amazon EC2" in text
    assert "Monthly Summary" in text
    assert "Grand total by account" not in text
    assert "Accounts included" not in text
    lookup.assert_not_called()
