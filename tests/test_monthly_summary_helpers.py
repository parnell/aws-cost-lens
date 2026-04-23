"""Tests for monthly summary formatting helpers."""

from aws_cost_lens.summary_bars import _format_net_usd, _monthly_summary_bar


def test_format_net_usd_near_zero():
    assert _format_net_usd(0.0) == "$0.00"
    assert _format_net_usd(0.002) == "$0.00"
    assert _format_net_usd(-0.002) == "$0.00"


def test_format_net_usd_non_trivial():
    assert _format_net_usd(12.34) == "$12.34"
    assert _format_net_usd(-0.50) == "$-0.50"


def test_monthly_summary_bar_scales_by_abs():
    bar = _monthly_summary_bar(-10.0, 10.0, 80)
    assert "(max)" in bar
    bar2 = _monthly_summary_bar(5.0, 10.0, 80)
    assert "50.0%" in bar2


def test_monthly_summary_bar_empty_when_no_scale():
    assert _monthly_summary_bar(1.0, 0.0, 80) == ""
