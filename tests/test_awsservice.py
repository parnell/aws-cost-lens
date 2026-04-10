"""Tests for AWSService name normalization."""

import pytest

from aws_cost_lens.core import AWSService


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("cloudwatch", "AmazonCloudWatch"),
        ("CLOUDWATCH", "AmazonCloudWatch"),
        ("AmazonCloudWatch", "AmazonCloudWatch"),
        ("s3", "AmazonS3"),
        ("ec2", "AmazonEC2"),
        ("api-gateway", "AmazonApiGateway"),
        ("apigateway", "AmazonApiGateway"),
        ("UnknownServiceName", "UnknownServiceName"),
    ],
)
def test_get_service_resolves_aliases_and_aws_names(name, expected):
    assert AWSService.get_service(name) == expected


@pytest.mark.parametrize(
    ("aws_name", "expected_alias"),
    [
        ("AmazonCloudWatch", "cloudwatch"),
        ("AmazonS3", "s3"),
        ("AmazonRDS", "rds"),
        ("NotInEnum", None),
    ],
)
def test_get_alias_returns_common_name_or_none(aws_name, expected_alias):
    assert AWSService.get_alias(aws_name) == expected_alias
