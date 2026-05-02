#!/usr/bin/env python3
"""
AWS Cost Lens - Command Line Interface

Main entry point for the AWS Cost Lens CLI tool.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from aws_cost_lens.core import (
    analyze_costs_detailed,
    analyze_costs_simple,
    ce_api_json_default,
    list_available_services,
)

METRIC_CHOICES = (
    "auto",
    "unblended",
    "blended",
    "net-unblended",
    "amortized",
    "net-amortized",
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Analyze AWS costs")
    parser.add_argument(
        "-s", "--start-date", help="Start date (YYYY-MM-DD), defaults to 6 months ago"
    )
    parser.add_argument(
        "-e",
        "--end-date",
        help=(
            "End date (YYYY-MM-DD). Cost Explorer uses an exclusive end; omitting this defaults "
            "to tomorrow so totals include usage through today (same window as most console views)."
        ),
    )
    parser.add_argument(
        "--service",
        default=None,
        help=(
            "Filter by specific AWS service (e.g., cloudwatch, AmazonCloudWatch, s3, ec2). "
            "If not specified, all services will be shown."
        ),
    )
    parser.add_argument(
        "--group-by",
        choices=["SERVICE", "USAGE_TYPE", "REGION"],
        default="USAGE_TYPE",
        help="How to group the costs (default: USAGE_TYPE)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="Show only top N services/usage types (default: 0 for all)",
    )
    parser.add_argument(
        "--cost-filter",
        type=float,
        default=0.0,
        metavar="USD",
        help=(
            "Monthly service bar tables (simple mode): hide per-service rows when "
            "max(Usage, |Credits|) is below this USD amount; combine those rows into "
            "'All other services'. 0 disables (default)."
        ),
    )
    parser.add_argument(
        "--all-services", action="store_true", help="Show all services instead of just CloudWatch"
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Show detailed breakdown by SERVICE and USAGE_TYPE",
    )
    parser.add_argument(
        "--simple",
        action="store_true",
        help="(Default behavior) Show simplified service-level breakdown",
    )
    parser.add_argument(
        "--region", 
        help="Filter results to a specific AWS region (e.g., us-east-1)"
    )
    parser.add_argument(
        "--show-region", 
        action="store_true", 
        help="Include region breakdown in detailed analysis",
        dest="show_region"
    )
    parser.add_argument(
        "--list-services", action="store_true", help="List all available AWS services and exit"
    )
    parser.add_argument(
        "--show-all",
        action="store_true",
        help="Show all items including those with zero costs in display",
    )
    parser.add_argument(
        "--granularity",
        choices=["DAILY", "MONTHLY", "HOURLY"],
        default="MONTHLY",
        help="Time granularity for the cost analysis (HOURLY only works for the last 14 days)",
    )
    parser.add_argument("--version", action="store_true", help="Show version information and exit")
    parser.add_argument(
        "--metric",
        choices=METRIC_CHOICES,
        default="unblended",
        help=(
            "Cost Explorer metric: default unblended (typical usage charges). Use auto to pick "
            "the bundled metric with the largest |total|, or force blended, net-unblended, "
            "amortized, or net-amortized"
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help=(
            "Extra output: per-table bar/RECORD_TYPE captions, monthly summary legend, and the "
            "reconciliation section (ungrouped CE + RECORD_TYPE table). Default is a quieter view."
        ),
    )
    parser.add_argument(
        "--no-reconcile",
        action="store_false",
        dest="reconcile",
        default=True,
        help=(
            "When used with --verbose, skip reconciliation CE calls and that section entirely. "
            "Ignored when not verbose (reconciliation is verbose-only)."
        ),
    )
    parser.add_argument(
        "--out",
        choices=["json"],
        default=None,
        help=(
            "After the run, write to logs/ce-api-<timestamp>.json: `calls` "
            "(raw get_cost_and_usage), and `summary` (RECORD_TYPE rollups, "
            "credit by SERVICE and USAGE_TYPE, hints vs Billing console). "
            "Use to inspect exact API payloads."
        ),
    )
    return parser.parse_args()


def main():
    """Main entry point for the CLI."""
    try:
        args = parse_args()

        # Show version if requested
        if args.version:
            from aws_cost_lens import __version__

            print(f"AWS Cost Lens version {__version__}")
            return 0

        # Default to 6 months ago if no start date provided
        if args.start_date:
            start_date = args.start_date
        else:
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

        # CE TimePeriod.End is exclusive; default to tomorrow so "through today" is included.
        if args.end_date:
            end_date = args.end_date
        else:
            end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        ce_api_dump: Optional[list[dict]] = [] if args.out == "json" else None
        ce_out_summary: Optional[dict] = {} if args.out == "json" else None

        def _write_api_json(mode: str) -> None:
            if ce_api_dump is None:
                return
            log_dir = Path("logs")
            log_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%dT%H%M%S")
            out_path = log_dir / f"ce-api-{stamp}.json"
            payload = {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "mode": mode,
                "start_date": start_date,
                "end_date": end_date,
                "argv": sys.argv,
                "calls": ce_api_dump,
                "summary": ce_out_summary,
            }
            out_path.write_text(
                json.dumps(payload, indent=2, default=ce_api_json_default) + "\n",
                encoding="utf-8",
            )
            print(f"Wrote Cost Explorer API dump to {out_path.resolve()}", file=sys.stderr)

        # If user requested to list services, do that and exit
        if args.list_services:
            list_available_services(
                start_date, end_date, args.region, args.metric, ce_api_dump=ce_api_dump
            )
            if ce_out_summary is not None:
                ce_out_summary["status"] = "list_services_only"
            _write_api_json("list_services")
            return 0

        # Use the service parameter directly - if None, it will show all services
        service = args.service

        # Use detailed view if requested, otherwise use simple view as default
        if args.detailed:
            analyze_costs_detailed(
                start_date=start_date,
                end_date=end_date,
                service=service,
                top=args.top,
                show_region=args.show_region,
                show_all=args.show_all,
                granularity=args.granularity,
                region=args.region,
                metric_preference=args.metric,
                reconcile=args.reconcile,
                verbose=args.verbose,
                ce_api_dump=ce_api_dump,
                out_summary=ce_out_summary,
            )
            _write_api_json("detailed")
        else:
            analyze_costs_simple(
                start_date=start_date,
                end_date=end_date,
                service=service,
                top=args.top,
                show_all=args.show_all,
                granularity=args.granularity,
                region=args.region,
                metric_preference=args.metric,
                reconcile=args.reconcile,
                verbose=args.verbose,
                ce_api_dump=ce_api_dump,
                out_summary=ce_out_summary,
                cost_filter_min=args.cost_filter,
            )
            _write_api_json("simple")

        return 0
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e!s}")
        return 1


# This ensures the function works both when imported and when run directly
def entry_point():
    """Entry point for the command-line script."""
    sys.exit(main())


if __name__ == "__main__":
    entry_point()
