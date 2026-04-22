"""Tests for should_show_in_progress."""

from datetime import datetime

from aws_cost_lens.core import should_show_in_progress


def test_past_closed_month_not_in_progress(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-03-01") is False


def test_current_month_is_mtd(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-04-01") is True


def test_future_month_incomplete(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-05-01") is True


def test_previous_month_early_in_current_month_is_incomplete(monkeypatch):
    class _FixedDateTime:
        _now = datetime(2026, 4, 3, 10, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
    assert should_show_in_progress("2026-03-01") is True


def test_previous_month_after_day_five_not_incomplete(monkeypatch):
    class _FixedDateTime:
        _now = datetime(2026, 4, 10, 10, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
    assert should_show_in_progress("2026-03-01") is False


def test_january_previous_december_rollover(monkeypatch):
    class _FixedDateTime:
        _now = datetime(2026, 1, 3, 10, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
    assert should_show_in_progress("2025-12-01") is True


def test_daily_today_incomplete(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-04-10", "DAILY") is True


def test_daily_yesterday_complete(fixed_datetime_for_april_2026):
    assert should_show_in_progress("2026-04-09", "DAILY") is False
