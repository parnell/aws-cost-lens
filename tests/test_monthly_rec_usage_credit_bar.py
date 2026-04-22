"""Tests for RECORD_TYPE monthly summary bar (net vs credits, near-zero handling)."""

from aws_cost_lens.core import _monthly_rec_usage_credit_bar


def test_bar_uses_net_and_credit_magnitude_not_gross_usage():
    """$134 usage + $-100 credits → net $34.15 vs $100 credit → ~1/4 red (not ~57% gross-of-usage)."""
    t = _monthly_rec_usage_credit_bar(134.15, -100.0, 120)
    # Width 40: net/(net+|credit|) = 34.15/134.15 ≈ 25% red → 10 red, 30 green (not 23+17).
    assert len(t.spans) == 2
    assert t.spans[0].style == "red" and t.spans[0].end - t.spans[0].start == 10
    assert t.spans[1].style == "green" and t.spans[1].end - t.spans[1].start == 30
    assert t.plain == "█" * 40


def test_bar_suppressed_when_display_would_be_zero_noise():
    """Sub-cent floats that round to $0.00 must not produce a 50/50 bar."""
    t = _monthly_rec_usage_credit_bar(0.002, -0.002, 120)
    assert t.plain.strip() == "—"


def test_bar_suppressed_when_both_zero():
    t = _monthly_rec_usage_credit_bar(0.0, 0.0, 120)
    assert t.plain.strip() == "—"
