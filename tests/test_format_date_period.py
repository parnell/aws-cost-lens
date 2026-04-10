"""Tests for format_date_period."""

import pytest

from aws_cost_lens.core import format_date_period


@pytest.mark.parametrize(
    ("date_str", "granularity", "expected_substring"),
    [
        ("2026-03-01", "MONTHLY", "March 2026"),
        ("2026-01-15", "DAILY", "Jan 15, 2026"),
        ("2026-07-04T00:00:00Z", "HOURLY", "Jul 04, 2026"),
    ],
)
def test_format_date_period_common_cases(date_str, granularity, expected_substring):
    assert expected_substring in format_date_period(date_str, granularity)


def test_format_date_period_invalid_returns_original():
    bad = "not-a-date"
    assert format_date_period(bad) == bad
