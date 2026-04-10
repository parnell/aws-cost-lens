"""Shared pytest fixtures."""

from datetime import datetime

import pytest


@pytest.fixture
def fixed_datetime_for_april_2026(monkeypatch):
    """Patch aws_cost_lens.core.datetime so now() is 2026-04-10."""

    class _FixedDateTime:
        _now = datetime(2026, 4, 10, 12, 0, 0)
        strptime = datetime.strptime

        @classmethod
        def now(cls):
            return cls._now

    monkeypatch.setattr("aws_cost_lens.core.datetime", _FixedDateTime)
