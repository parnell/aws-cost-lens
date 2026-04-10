"""CLI argument parsing."""

import sys


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
    assert args.metric == "auto"
