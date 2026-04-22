"""Tests for RECORD_TYPE monthly summary bar (net vs credits, near-zero handling)."""

from aws_cost_lens.core import _monthly_rec_usage_credit_bar


def test_bar_uses_net_and_credit_magnitude_not_gross_usage():
    """$134 usage + $-100 credits → ~25% paid / ~75% credits (not ~57% / ~43% gross split)."""
    t = _monthly_rec_usage_credit_bar(134.15, -100.0, 120)
    plain = t.plain
    assert "25% paid" in plain or "26% paid" in plain
    assert "74% credits" in plain or "75% credits" in plain


def test_bar_suppressed_when_display_would_be_zero_noise():
    """Sub-cent floats that round to $0.00 must not produce a 50/50 bar."""
    t = _monthly_rec_usage_credit_bar(0.002, -0.002, 120)
    assert t.plain.strip() == "—"


def test_bar_suppressed_when_both_zero():
    t = _monthly_rec_usage_credit_bar(0.0, 0.0, 120)
    assert t.plain.strip() == "—"
