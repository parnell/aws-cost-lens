"""Tests for get_cost_data (mocked AWS API)."""

from unittest.mock import MagicMock, patch

import pytest

from aws_cost_lens.core import CE_METRICS_BUNDLE, get_cost_data


@pytest.fixture
def mock_ce_response():
    return {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2026-03-01", "End": "2026-04-01"},
                "Total": {},
                "Groups": [
                    {
                        "Keys": ["AmazonCloudWatch"],
                        "Metrics": {"UnblendedCost": {"Amount": "42.50", "Unit": "USD"}},
                    }
                ],
            }
        ]
    }


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


def test_get_cost_data_paginates_and_merges_groups():
    page1 = {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2026-03-01", "End": "2026-04-01"},
                "Groups": [
                    {
                        "Keys": ["AmazonS3"],
                        "Metrics": {"UnblendedCost": {"Amount": "1.00", "Unit": "USD"}},
                    }
                ],
            }
        ],
        "NextPageToken": "next",
    }
    page2 = {
        "ResultsByTime": [
            {
                "TimePeriod": {"Start": "2026-03-01", "End": "2026-04-01"},
                "Groups": [
                    {
                        "Keys": ["AmazonEC2"],
                        "Metrics": {"UnblendedCost": {"Amount": "2.00", "Unit": "USD"}},
                    }
                ],
            }
        ],
    }
    mock_client = MagicMock()
    mock_client.get_cost_and_usage.side_effect = [page1, page2]

    with patch("aws_cost_lens.core.boto3.client", return_value=mock_client):
        result = get_cost_data("2026-03-01", "2026-04-01", None, "SERVICE")

    assert len(result["ResultsByTime"][0]["Groups"]) == 2
    assert mock_client.get_cost_and_usage.call_count == 2
    second_call = mock_client.get_cost_and_usage.call_args_list[1].kwargs
    assert second_call["NextPageToken"] == "next"


def test_resolve_effective_metric_auto_picks_largest_total():
    from aws_cost_lens.core import resolve_effective_metric

    results = [
        {
            "TimePeriod": {"Start": "2026-03-01", "End": "2026-04-01"},
            "Total": {
                "UnblendedCost": {"Amount": "0", "Unit": "USD"},
                "BlendedCost": {"Amount": "100.00", "Unit": "USD"},
                "NetUnblendedCost": {"Amount": "0", "Unit": "USD"},
            },
            "Groups": [],
        }
    ]
    assert resolve_effective_metric(results, "auto") == "BlendedCost"


def test_resolve_effective_metric_forced_unblended():
    from aws_cost_lens.core import resolve_effective_metric

    results = [
        {
            "TimePeriod": {"Start": "2026-03-01", "End": "2026-04-01"},
            "Total": {
                "UnblendedCost": {"Amount": "0", "Unit": "USD"},
                "BlendedCost": {"Amount": "100.00", "Unit": "USD"},
            },
            "Groups": [],
        }
    ]
    assert resolve_effective_metric(results, "unblended") == "UnblendedCost"
