"""Tests for RECORD_TYPE monthly summary bars (scaled, green/red coverage split)."""

from aws_cost_lens.summary_bars import (
    _monthly_summary_rec_max_magnitude,
    _service_rec_coverage_bar,
)


def test_coverage_bar_green_red_split_at_full_scale():
    """$134 usage + $-100 credits → ~75% credit-cover (green) vs ~25% OOP (red) of gross usage."""
    t = _service_rec_coverage_bar(134.15, -100.0, 120, max_magnitude=134.15)
    assert len(t.spans) == 2
    assert t.spans[0].style == "green" and t.spans[0].end - t.spans[0].start == 30
    assert t.spans[1].style == "red" and t.spans[1].end - t.spans[1].start == 10
    assert t.plain == "█" * 40


def test_summary_max_magnitude_includes_grand_total():
    totals = [
        ("Jan", 0.0, False, 100.0, -100.0),
        ("Feb", 0.0, False, 500.0, -500.0),
    ]
    assert _monthly_summary_rec_max_magnitude(totals, 600.0, -600.0) == 600.0


def test_summary_month_row_shorter_than_grand():
    mx = _monthly_summary_rec_max_magnitude(
        [
            ("Mar", 0.0, False, 3000.0, -3000.0),
            ("Apr", 0.0, False, 1000.0, -1000.0),
        ],
        4000.0,
        -4000.0,
    )
    assert mx == 4000.0
    t_grand = _service_rec_coverage_bar(4000.0, -4000.0, 120, mx)
    t_month = _service_rec_coverage_bar(1000.0, -1000.0, 120, mx)
    assert len(t_grand.plain) == 40
    assert len(t_month.plain) == 10


def test_bar_suppressed_when_display_would_be_zero_noise():
    t = _service_rec_coverage_bar(0.002, -0.002, 120, max_magnitude=1.0)
    assert t.plain.strip() == "—"


def test_bar_suppressed_when_both_zero():
    t = _service_rec_coverage_bar(0.0, 0.0, 120, max_magnitude=1.0)
    assert t.plain.strip() == "—"
