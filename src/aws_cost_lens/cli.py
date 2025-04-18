#!/usr/bin/env python3
"""
AWS Cost Lens - Command Line Interface

Main entry point for the AWS Cost Lens CLI tool.
"""

import argparse
import sys
from datetime import datetime, timedelta

from aws_cost_lens.core import (
    analyze_costs_detailed,
    analyze_costs_simple,
    list_available_services,
)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Analyze AWS costs")
    parser.add_argument(
        "-s", "--start-date", help="Start date (YYYY-MM-DD), defaults to 6 months ago"
    )
    parser.add_argument("-e", "--end-date", help="End date (YYYY-MM-DD), defaults to today")
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
        "--region", action="store_true", help="Include region breakdown in detailed analysis"
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

        # Default to today if no end date provided
        if args.end_date:
            end_date = args.end_date
        else:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # If user requested to list services, do that and exit
        if args.list_services:
            list_available_services(start_date, end_date)
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
                region=args.region,
                show_all=args.show_all,
                granularity=args.granularity,
            )
        else:
            analyze_costs_simple(
                start_date=start_date,
                end_date=end_date,
                service=service,
                top=args.top,
                show_all=args.show_all,
                granularity=args.granularity,
            )

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
