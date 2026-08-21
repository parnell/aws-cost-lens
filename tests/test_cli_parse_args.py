"""CLI argument parsing."""

import sys
from unittest.mock import patch


def test_parse_args_detailed_flag(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["aws-cost-lens", "--detailed", "--service", "s3"])
    from aws_cost_lens.cli import parse_args

    args = parse_args()
    assert args.detailed is True
    assert args.service == "s3"


def test_parse_args_defaults(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["aws-cost-lens"])
    from aws_cost_lens.cli import parse_args

    args = parse_args()
    assert args.service is None
    assert args.detailed is False
    assert args.granularity == "MONTHLY"
    assert args.metric == "unblended"
    assert args.reconcile is True
    assert args.verbose is False
    assert args.profile is None


def test_parse_args_profile(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["aws-cost-lens", "--profile", "billing-admin"])
    from aws_cost_lens.cli import parse_args

    assert parse_args().profile == "billing-admin"


def test_configure_aws_profile_sets_default_session():
    from aws_cost_lens.cli import configure_aws_profile

    with patch("aws_cost_lens.cli.boto3.setup_default_session") as setup:
        configure_aws_profile("billing-admin")
    setup.assert_called_once_with(profile_name="billing-admin")


def test_configure_aws_profile_noop_when_omitted():
    from aws_cost_lens.cli import configure_aws_profile

    with patch("aws_cost_lens.cli.boto3.setup_default_session") as setup:
        configure_aws_profile(None)
    setup.assert_not_called()


def test_parse_args_verbose(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["aws-cost-lens", "--verbose"])
    from aws_cost_lens.cli import parse_args

    assert parse_args().verbose is True


def test_parse_args_no_reconcile(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["aws-cost-lens", "--no-reconcile"])
    from aws_cost_lens.cli import parse_args

    assert parse_args().reconcile is False


def test_parse_args_out_json(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["aws-cost-lens", "--out", "json"])
    from aws_cost_lens.cli import parse_args

    assert parse_args().out == "json"
