"""Tests for should_show_in_progress."""

from datetime import datetime

from aws_cost_lens.core import should_show_in_progress


def test_positive_spend_never_in_progress():
    assert should_show_in_progress("2026-03-01", 100.0) is False
    assert should_show_in_progress("2026-03-01", 0.02) is False


def test_current_month_zero_spend_is_in_progress(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-04-01", 0.0) is True


def test_past_month_with_zero_spend_not_in_progress(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-03-01", 0.0) is False


def test_future_month_marked_in_progress(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-05-01", 0.0) is True


def test_previous_month_early_in_current_month_is_in_progress(monkeypatch):
    class _FixedDateTime:
        _now = datetime(2026, 4, 3, 10, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
    assert should_show_in_progress("2026-03-01", 0.0) is True


def test_previous_month_after_day_five_not_in_progress(monkeypatch):
    class _FixedDateTime:
        _now = datetime(2026, 4, 10, 10, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
    assert should_show_in_progress("2026-03-01", 0.0) is False


def test_january_previous_december_rollover(monkeypatch):
    class _FixedDateTime:
        _now = datetime(2026, 1, 3, 10, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
    assert should_show_in_progress("2025-12-01", 0.0) is True
