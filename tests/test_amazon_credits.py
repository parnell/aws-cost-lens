"""Tests for Amazon Credits (Billing GetCredits) inventory."""

import io
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from botocore.exceptions import ClientError
from rich.console import Console

from aws_cost_lens.amazon_credits import (
    GET_CREDITS_MAX_LOOKBACK_DAYS,
    clamp_get_credits_window,
    credit_balance_usd,
    fetch_amazon_credits,
    parse_credit_records,
    print_amazon_credits_section,
)


def _client_error(code: str, message: str = "denied") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": message}}, "GetCredits")


def _credit_payload():
    return {
        "credits": [
            {
                "creditId": "111",
                "accountId": "123456789012",
                "creditType": "Promotion",
                "creditStatus": "ENABLED",
                "description": "AWS promotional credit",
                "initialAmount": {"currencyCode": "USD", "currencyAmount": "-5000.00"},
                "remainingAmount": {"currencyCode": "USD", "currencyAmount": "-1234.50"},
                "estimatedAmount": {"currencyCode": "USD", "currencyAmount": "-1100.00"},
                "startDate": datetime(2026, 1, 1, tzinfo=UTC),
                "endDate": datetime(2026, 12, 31, tzinfo=UTC),
                "applicableProductNames": ["Amazon EC2", "Amazon S3", "AWS Lambda", "Amazon RDS"],
            },
            {
                "creditId": "222",
                "accountId": "123456789012",
                "creditType": "Promotion",
                "creditStatus": "ENABLED",
                "description": "Small leftover",
                "initialAmount": {"currencyCode": "USD", "currencyAmount": "100.00"},
                "remainingAmount": {"currencyCode": "USD", "currencyAmount": "0.00"},
                "endDate": datetime(2026, 6, 1, tzinfo=UTC),
                "exhaustDate": datetime(2026, 5, 15, tzinfo=UTC),
                "applicableProductNames": [],
            },
        ]
    }


def test_credit_balance_usd_treats_signed_and_unsigned_as_remaining():
    assert credit_balance_usd(-1234.5) == 1234.5
    assert credit_balance_usd(1234.5) == 1234.5
    assert credit_balance_usd(None) == 0.0


def test_clamp_window_caps_start_to_one_year_and_end_to_now():
    now = datetime(2026, 8, 21, 15, 0, tzinfo=UTC)
    start, end = clamp_get_credits_window("2024-01-01", "2026-08-22", now=now)
    assert start == now - timedelta(days=GET_CREDITS_MAX_LOOKBACK_DAYS)
    assert end == now


def test_clamp_window_keeps_recent_start():
    now = datetime(2026, 8, 21, tzinfo=UTC)
    start, end = clamp_get_credits_window("2026-02-21", "2026-08-21", now=now)
    assert start.date().isoformat() == "2026-02-21"
    assert end == now


def test_parse_credit_records_sorts_by_remaining_and_abs_balances():
    rows = parse_credit_records(_credit_payload(), payer_aggregated=True)
    assert len(rows) == 2
    assert rows[0].description == "AWS promotional credit"
    assert rows[0].remaining == 1234.5
    assert rows[0].initial == 5000.0
    assert rows[0].estimated == 1100.0
    assert rows[0].payer_aggregated is True
    assert rows[1].remaining == 0.0
    assert "Amazon EC2" in rows[0].products


def test_fetch_retries_without_payer_flag_on_access_denied():
    client = MagicMock()
    client.get_credits.side_effect = [
        _client_error("AccessDeniedException"),
        _credit_payload(),
    ]
    credits, err, meta = fetch_amazon_credits(
        "2026-02-21",
        "2026-08-21",
        account_id="123456789012",
        billing_client=client,
        now=datetime(2026, 8, 21, tzinfo=UTC),
    )
    assert err is None
    assert len(credits) == 2
    assert meta["payer_aggregated"] is False
    assert client.get_credits.call_count == 2
    assert client.get_credits.call_args_list[0].kwargs["payerAccountFlag"] is True
    assert "payerAccountFlag" not in client.get_credits.call_args_list[1].kwargs


def test_fetch_uses_payer_aggregation_when_allowed():
    client = MagicMock()
    client.get_credits.return_value = _credit_payload()
    credits, err, meta = fetch_amazon_credits(
        "2026-02-21",
        "2026-08-21",
        account_id="123456789012",
        billing_client=client,
        now=datetime(2026, 8, 21, tzinfo=UTC),
    )
    assert err is None
    assert meta["payer_aggregated"] is True
    assert credits[0].remaining == 1234.5
    client.get_credits.assert_called_once()


def test_fetch_missing_get_credits_is_a_message_not_an_exception():
    class _BillingWithoutGetCredits:
        pass

    credits, err, _meta = fetch_amazon_credits(
        "2026-02-21",
        "2026-08-21",
        account_id="123456789012",
        billing_client=_BillingWithoutGetCredits(),
        now=datetime(2026, 8, 21, tzinfo=UTC),
    )
    assert credits == []
    assert err is not None
    assert "1.43.41" in err


def test_fetch_access_denied_both_attempts():
    client = MagicMock()
    client.get_credits.side_effect = [
        _client_error("AccessDeniedException", "no billing"),
        _client_error("AccessDeniedException", "still no"),
    ]
    credits, err, _meta = fetch_amazon_credits(
        "2026-02-21",
        "2026-08-21",
        account_id="123456789012",
        billing_client=client,
        now=datetime(2026, 8, 21, tzinfo=UTC),
    )
    assert credits == []
    assert "billing:GetCredits" in err


def test_print_section_renders_remaining_and_json_summary():
    client = MagicMock()
    client.get_credits.return_value = _credit_payload()
    buf = io.StringIO()
    console = Console(file=buf, width=140, force_terminal=True, color_system=None)
    summary: dict = {}
    print_amazon_credits_section(
        console,
        "2026-02-21",
        "2026-08-21",
        out_summary=summary,
        now=datetime(2026, 8, 21, tzinfo=UTC),
        billing_client=client,
        account_id="123456789012",
    )
    text = buf.getvalue()
    assert "Amazon Credits remaining" in text
    assert "1234.50" in text
    assert "AWS promotional credit" in text
    assert summary["amazon_credits"]["remaining_total"] == 1234.5
    assert summary["amazon_credits"]["error"] is None
    assert len(summary["amazon_credits"]["credits"]) == 2


def test_print_section_empty_is_quiet():
    client = MagicMock()
    client.get_credits.return_value = {"credits": []}
    buf = io.StringIO()
    console = Console(file=buf, width=80, force_terminal=True, color_system=None)
    print_amazon_credits_section(
        console,
        "2026-02-21",
        "2026-08-21",
        now=datetime(2026, 8, 21, tzinfo=UTC),
        billing_client=client,
        account_id="123456789012",
    )
    assert "none returned" in buf.getvalue()


def test_build_table_via_print_shows_expiry_and_products():
    client = MagicMock()
    client.get_credits.return_value = _credit_payload()
    buf = io.StringIO()
    console = Console(file=buf, width=160, force_terminal=True, color_system=None)
    print_amazon_credits_section(
        console,
        "2026-02-21",
        "2026-08-21",
        now=datetime(2026, 8, 21, tzinfo=UTC),
        billing_client=client,
        account_id="123456789012",
    )
    text = buf.getvalue()
    assert "2026-12-31" in text
    assert "Amazon EC2" in text
    assert "exhausted" in text
    assert "TOTAL remaining" in text
