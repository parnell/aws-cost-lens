"""Tests for get_cost_data (mocked AWS API)."""

from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from aws_cost_lens.core import (
    CE_METRICS_BUNDLE,
    _merge_cost_and_usage_pages,
    get_cost_data,
    normalize_ce_time_period_bound,
)
from tests.fixtures import load_ce_fixture


@pytest.fixture
def mock_ce_response():
    """Fixture ``ce_get_cost_and_usage_monthly_by_service`` (live CE capture)."""
    return deepcopy(load_ce_fixture("ce_get_cost_and_usage_monthly_by_service"))


def test_get_cost_data_passes_service_filter_and_returns_response(mock_ce_response):
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.return_value = mock_ce_response

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        result = get_cost_data(
            "2026-03-01",
            "2026-04-01",
            "cloudwatch",
            "SERVICE",
            granularity="MONTHLY",
        )

    assert result == mock_ce_response
    mock_client.get_cost_and_usage.assert_called_once()
    call_kw = mock_client.get_cost_and_usage.call_args.kwargs
    assert call_kw["Filter"] == {
        "Dimensions": {
            "Key": "SERVICE",
            "Values": ["AmazonCloudWatch"],
        }
    }
    assert call_kw["Metrics"] == CE_METRICS_BUNDLE
    assert call_kw["Granularity"] == "MONTHLY"
    assert call_kw["GroupBy"] == [{"Type": "DIMENSION", "Key": "SERVICE"}]


def test_get_cost_data_group_by_list(mock_ce_response):
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.return_value = mock_ce_response

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        get_cost_data(
            "2026-03-01",
            "2026-04-01",
            "cloudwatch",
            ["SERVICE", "USAGE_TYPE"],
        )

    call_kw = mock_client.get_cost_and_usage.call_args.kwargs
    assert call_kw["GroupBy"] == [
        {"Type": "DIMENSION", "Key": "SERVICE"},
        {"Type": "DIMENSION", "Key": "USAGE_TYPE"},
    ]


def test_get_cost_data_region_and_service_uses_and_filter(mock_ce_response):
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.return_value = mock_ce_response

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        get_cost_data(
            "2026-03-01",
            "2026-04-01",
            "s3",
            "SERVICE",
            region="us-east-1",
        )

    call_kw = mock_client.get_cost_and_usage.call_args.kwargs
    assert "And" in call_kw["Filter"]
    assert len(call_kw["Filter"]["And"]) == 2


def test_get_cost_data_record_type_filter_and_with_service(mock_ce_response):
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.return_value = mock_ce_response

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        get_cost_data(
            "2026-03-01",
            "2026-04-01",
            "s3",
            "SERVICE",
            record_type_values=["Credit"],
        )

    call_kw = mock_client.get_cost_and_usage.call_args.kwargs
    and_parts = call_kw["Filter"]["And"]
    assert len(and_parts) == 2
    assert {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Credit"]}} in and_parts
    assert {"Dimensions": {"Key": "SERVICE", "Values": ["AmazonS3"]}} in and_parts


def test_normalize_ce_time_period_bound_flexible_dates():
    assert normalize_ce_time_period_bound("2026/03/01") == "2026-03-01"
    assert normalize_ce_time_period_bound("2026-03-01T14:30:00Z") == "2026-03-01T14:30:00Z"
    assert normalize_ce_time_period_bound("Mar 1, 2026") == "2026-03-01"


def test_get_cost_data_normalizes_flexible_time_period(mock_ce_response):
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.return_value = mock_ce_response

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        get_cost_data(
            "2026/03/01",
            "2026/04/01",
            None,
            None,
            granularity="MONTHLY",
        )

    tp = mock_client.get_cost_and_usage.call_args.kwargs["TimePeriod"]
    assert tp == {"Start": "2026-03-01", "End": "2026-04-01"}


def test_get_cost_data_ungrouped_omits_group_by(mock_ce_response):
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.return_value = mock_ce_response

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        get_cost_data("2026-03-01", "2026-04-01", None, None, granularity="MONTHLY")

    call_kw = mock_client.get_cost_and_usage.call_args.kwargs
    assert "GroupBy" not in call_kw
    assert call_kw["Metrics"] == CE_METRICS_BUNDLE


def test_get_cost_data_paginates_and_merges_groups():
    page1 = load_ce_fixture("ce_get_cost_and_usage_pagination_page1")
    page2 = load_ce_fixture("ce_get_cost_and_usage_pagination_page2")
    expected = len(page1["ResultsByTime"][0]["Groups"]) + len(
        page2["ResultsByTime"][0]["Groups"]
    )
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.side_effect = [page1, page2]

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        result = get_cost_data("2026-03-01", "2026-04-01", None, "SERVICE")

    assert len(result["ResultsByTime"][0]["Groups"]) == expected
    assert mock_client.get_cost_and_usage.call_count == 2
    second_call = mock_client.get_cost_and_usage.call_args_list[1].kwargs
    assert second_call["NextPageToken"] == page1["NextPageToken"]


def test_merge_cost_and_usage_pages_appends_later_days():
    """A shorter next page is the following days, not more groups on the first days."""
    page1 = {
        "GroupDefinitions": [{"Type": "DIMENSION", "Key": "SERVICE"}],
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-02"},
                "Total": {},
                "Groups": [{"Keys": ["Oct 1"]}],
            },
            {
                "TimePeriod": {"Start": "2026-10-22", "End": "2026-10-23"},
                "Total": {},
                "Groups": [{"Keys": ["Oct 22"]}],
            },
        ],
        "NextPageToken": "page-2",
    }
    page2 = {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2026-10-23", "End": "2026-10-24"},
                "Total": {},
                "Groups": [{"Keys": ["Oct 23"]}],
            },
            {
                "TimePeriod": {"Start": "2026-10-01", "End": "2026-10-02"},
                "Total": {},
                "Groups": [{"Keys": ["Oct 1 more"]}],
                "Estimated": True,
            },
            {
                "TimePeriod": {"Start": "2026-10-29", "End": "2026-10-30"},
                "Total": {},
                "Groups": [{"Keys": ["Oct 29"]}],
                "Estimated": True,
            },
        ],
    }

    merged = _merge_cost_and_usage_pages([page1, page2])

    assert "NextPageToken" not in merged
    assert [period["TimePeriod"]["Start"] for period in merged["ResultsByTime"]] == [
        "2026-10-01",
        "2026-10-22",
        "2026-10-23",
        "2026-10-29",
    ]
    assert [group["Keys"] for group in merged["ResultsByTime"][0]["Groups"]] == [
        ["Oct 1"],
        ["Oct 1 more"],
    ]
    assert merged["ResultsByTime"][0]["Estimated"] is True
    assert [group["Keys"] for group in merged["ResultsByTime"][1]["Groups"]] == [["Oct 22"]]
    assert [group["Keys"] for group in merged["ResultsByTime"][2]["Groups"]] == [["Oct 23"]]
    assert page1["ResultsByTime"][0]["Groups"] == [{"Keys": ["Oct 1"]}]


def test_resolve_effective_metric_auto_picks_largest_total():
    from aws_cost_lens.core import resolve_effective_metric

    payload = load_ce_fixture("ce_get_cost_and_usage_monthly_by_service")
    results = payload["ResultsByTime"]
    assert resolve_effective_metric(results, "auto") == "BlendedCost"


def test_resolve_effective_metric_forced_unblended():
    from aws_cost_lens.core import resolve_effective_metric

    payload = load_ce_fixture("ce_get_cost_and_usage_monthly_by_service")
    results = payload["ResultsByTime"]
    assert resolve_effective_metric(results, "unblended") == "UnblendedCost"


def test_resolve_effective_metric_forced_net_amortized():
    from aws_cost_lens.core import resolve_effective_metric

    payload = load_ce_fixture("ce_get_cost_and_usage_monthly_by_service")
    results = payload["ResultsByTime"]
    assert resolve_effective_metric(results, "net-amortized") == "NetAmortizedCost"
