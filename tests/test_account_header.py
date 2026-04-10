"""Tests for account header line (STS / IAM / Organizations)."""

from unittest.mock import MagicMock, patch

from aws_cost_lens.core import get_account_header_markup


def _mock_boto3_clients(sts_account: str, org_name=None, iam_aliases=None):
    sts = MagicMock()
    sts.get_caller_identity.return_value = {"Account": sts_account}

    def client(name, **kwargs):
        if name == "sts":
            return sts
        if name == "organizations":
            o = MagicMock()
            if org_name is not None:
                o.describe_account.return_value = {
                    "Account": {"Name": org_name, "Id": sts_account},
                }
            else:
                o.describe_account.side_effect = Exception("not in org")
            return o
        if name == "iam":
            i = MagicMock()
            i.list_account_aliases.return_value = {"AccountAliases": list(iam_aliases or [])}
            return i
        raise AssertionError(f"unexpected client {name!r}")

    return client


def test_account_header_shows_org_name_and_id():
    with patch(
        "aws_cost_lens.core.boto3.client",
        side_effect=_mock_boto3_clients("123456789012", org_name="Scaffold"),
    ):
        line = get_account_header_markup()

    assert "Scaffold" in line
    assert "123456789012" in line


def test_account_header_falls_back_to_iam_alias():
    with patch(
        "aws_cost_lens.core.boto3.client",
        side_effect=_mock_boto3_clients("111111111111", org_name=None, iam_aliases=["prod-alias"]),
    ):
        line = get_account_header_markup()

    assert "prod-alias" in line
    assert "111111111111" in line


def test_account_header_id_only_when_no_name():
    with patch(
        "aws_cost_lens.core.boto3.client",
        side_effect=_mock_boto3_clients("999999999999", org_name=None, iam_aliases=[]),
    ):
        line = get_account_header_markup()

    assert "999999999999" in line
    assert "Account:" in line


def test_account_header_credentials_error():
    with patch("aws_cost_lens.core.boto3.client", side_effect=Exception("no creds")):
        line = get_account_header_markup()

    assert "unable to resolve" in line.lower()
