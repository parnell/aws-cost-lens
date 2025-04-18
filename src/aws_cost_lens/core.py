"""
AWS Cost Analyzer Core Module

Core functionality for displaying AWS costs by service and usage type with rich formatting.
"""

import sys
from datetime import datetime
from enum import Enum
from typing import NamedTuple

import boto3
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table


class ServiceInfo(NamedTuple):
    """Container for AWS service information."""

    aws_name: str
    aliases: list[str]


class AWSService(Enum):
    """AWS service names with their common aliases."""

    CLOUDWATCH = ServiceInfo("AmazonCloudWatch", ["cloudwatch"])
    S3 = ServiceInfo("AmazonS3", ["s3"])
    EC2 = ServiceInfo("AmazonEC2", ["ec2"])
    LAMBDA = ServiceInfo("AWSLambda", ["lambda"])
    DYNAMODB = ServiceInfo("AmazonDynamoDB", ["dynamodb"])
    RDS = ServiceInfo("AmazonRDS", ["rds"])
    ROUTE53 = ServiceInfo("AmazonRoute53", ["route53"])
    SNS = ServiceInfo("AmazonSNS", ["sns"])
    SQS = ServiceInfo("AmazonSQS", ["sqs"])
    ELB = ServiceInfo("AWSELB", ["elb"])
    EFS = ServiceInfo("AmazonEFS", ["efs"])
    API_GATEWAY = ServiceInfo("AmazonApiGateway", ["apigateway", "api-gateway"])
    ECR = ServiceInfo("AmazonECR", ["ecr", "fargate"])
    EKS = ServiceInfo("AmazonEKS", ["eks"])
    GLACIER = ServiceInfo("AmazonGlacier", ["glacier"])
    REDSHIFT = ServiceInfo("AmazonRedshift", ["redshift"])
    CLOUDFRONT = ServiceInfo("AmazonCloudFront", ["cloudfront"])
    VPC = ServiceInfo("AmazonVPC", ["vpc"])

    @property
    def aws_name(self) -> str:
        """Get the AWS service name."""
        return self.value.aws_name

    @property
    def aliases(self) -> list[str]:
        """Get the service aliases."""
        return self.value.aliases

    @classmethod
    def get_service(cls, name: str) -> str:
        """Get the AWS service name from a service name or alias."""
        name_lower = name.lower()

        # Try to match directly to AWS name
        for service in cls:
            if name_lower == service.aws_name.lower():
                return service.aws_name

        # Try to match to enum name
        try:
            return cls[name.upper()].aws_name
        except KeyError:
            pass

        # Try to match to aliases
        for service in cls:
            if name_lower in service.aliases:
                return service.aws_name

        # Return the original if no match found
        return name

    @classmethod
    def get_alias(cls, service_name: str) -> str | None:
        """Get a human-friendly alias for an AWS service name."""
        for service in cls:
            if service.aws_name == service_name and service.aliases:
                return service.aliases[0]
        return None


def get_cost_data(
    start_date: str,
    end_date: str,
    service: str | None,
    group_by: str | list[str],
    granularity: str = "MONTHLY",
) -> dict:
    """Fetch cost data from AWS Cost Explorer API."""
    console = Console()

    with Progress() as progress:
        task = progress.add_task(
            f"[cyan]Fetching AWS costs{f' for {service}' if service else ''}...",
            total=1,
        )

        try:
            ce_client = boto3.client("ce")

            # Format dates correctly for HOURLY granularity
            # AWS Cost Explorer API expects timestamps in ISO 8601 format for HOURLY
            if granularity == "HOURLY":
                # Convert dates to datetime objects
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
                end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

                # Check if range is within 14 days
                days_diff = (end_datetime - start_datetime).days

                today = datetime.now()
                days_from_today_start = (today - start_datetime).days

                if days_diff > 14 or days_from_today_start > 14:
                    console.print(
                        "[yellow]Warning: HOURLY granularity is only available for the past 14 days.[/yellow]"
                    )
                    console.print("[yellow]Falling back to DAILY granularity.[/yellow]")
                    granularity = "DAILY"
                else:
                    # Add time component and convert to ISO 8601 format
                    # For start date, use 00:00:00Z (start of day)
                    # For end date, use 23:59:59Z (end of day)
                    start_date = start_datetime.strftime("%Y-%m-%dT00:00:00Z")

                    # If end date is today, use current time, otherwise use end of day
                    if (end_datetime.date() - today.date()).days == 0:
                        end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        end_date = end_datetime.strftime("%Y-%m-%dT23:59:59Z")

            # Base request parameters
            request_params = {
                "TimePeriod": {"Start": start_date, "End": end_date},
                "Granularity": granularity,
                "Metrics": ["BlendedCost"],
            }

            # Handle single string or list of group_by values
            if isinstance(group_by, str):
                request_params["GroupBy"] = [{"Type": "DIMENSION", "Key": group_by}]
            else:
                request_params["GroupBy"] = [
                    {"Type": "DIMENSION", "Key": key} for key in group_by
                ]

            # Add service filter if specified
            if service:
                # Normalize the service name
                normalized_service = AWSService.get_service(service)
                request_params["Filter"] = {
                    "Dimensions": {"Key": "SERVICE", "Values": [normalized_service]}
                }

            response = ce_client.get_cost_and_usage(**request_params)

            progress.update(task, advance=1)
            return response

        except Exception as e:
            progress.stop()
            console.print(f"[bold red]Error:[/bold red] {str(e)}")
            sys.exit(1)


def list_available_services(start_date: str, end_date: str) -> None:
    """List all available AWS services that have cost data."""
    console = Console()

    with Progress() as progress:
        task = progress.add_task("[cyan]Fetching available AWS services...", total=1)

        try:
            ce_client = boto3.client("ce")

            # Build request parameters
            request_params = {
                "TimePeriod": {"Start": start_date, "End": end_date},
                "Granularity": "MONTHLY",
                "Metrics": ["BlendedCost"],
                "GroupBy": [
                    {"Type": "DIMENSION", "Key": "SERVICE"},
                ],
            }

            response = ce_client.get_cost_and_usage(**request_params)

            progress.update(task, advance=1)

            # Extract unique service names
            services = set()
            for period in response.get("ResultsByTime", []):
                for group in period.get("Groups", []):
                    service_name = group["Keys"][0]
                    services.add(service_name)

            # Sort and display services
            services = sorted(list(services))

            if not services:
                console.print(
                    "[yellow]No services found with cost data in the specified time range.[/yellow]"
                )
                return

            table = Table(title="Available AWS Services")
            table.add_column("Service Name", style="cyan")
            table.add_column("Common Name/Alias", style="green")

            for service in services:
                alias = AWSService.get_alias(service)
                table.add_row(service, alias or "")

            console.print(table)
            console.print(f"\n[dim]Found {len(services)} services.[/dim]")

        except Exception as e:
            progress.stop()
            console.print(f"[bold red]Error:[/bold red] {str(e)}")
            sys.exit(1)


def format_date_period(date_str: str, granularity: str = "MONTHLY") -> str:
    """Format date string based on granularity."""
    try:
        # Handle ISO 8601 format (used by HOURLY granularity)
        if "T" in date_str:
            date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
            if granularity == "HOURLY":
                return date_obj.strftime("%b %d, %Y %H:%M")
            else:
                return date_obj.strftime("%b %d, %Y")
        # Handle YYYY-MM-DD format
        else:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            if granularity == "MONTHLY":
                return date_obj.strftime("%B %Y")
            elif granularity == "DAILY":
                return date_obj.strftime("%b %d, %Y")
            else:
                return date_obj.strftime("%b %d, %Y")
    except ValueError:
        # If formatting fails, return original string
        return date_str


def create_cost_table(
    period_data: dict,
    console_width: int,
    group_by: str,
    limit: int,
    show_all: bool = False,
    granularity: str = "MONTHLY",
) -> Table:
    """Create a rich table for a single time period."""
    period_start = period_data["TimePeriod"]["Start"]
    period_display = format_date_period(period_start, granularity)

    if granularity == "DAILY":
        title_prefix = "Daily"
    elif granularity == "HOURLY":
        title_prefix = "Hourly"
    else:
        title_prefix = "Monthly"

    title = f"{title_prefix} {period_display} Costs"

    # Choose column title based on grouping
    group_titles = {
        "SERVICE": "Service",
        "USAGE_TYPE": "Usage Type",
        "REGION": "Region",
    }
    group_title = group_titles.get(group_by, "Item")

    # Find all costs to prepare item count
    costs = []
    for group in period_data["Groups"]:
        name = group["Keys"][0]
        amount = float(group["Metrics"]["BlendedCost"]["Amount"])
        costs.append((name, amount))

    # Calculate displayed vs total count
    total_count = len(costs)
    non_zero_count = sum(1 for _, amount in costs if amount >= 0.01)
    zero_count = total_count - non_zero_count

    # Create title with count information
    if show_all or zero_count == 0:
        title = f"{title_prefix} {period_display} Costs"
    else:
        title = f"{title_prefix} {period_display} Costs [dim]• Showing {non_zero_count} of {total_count} items (hidden: {zero_count} zero-cost items)[/dim]"

    table = Table(title=title, expand=True)
    table.add_column(group_title, style="cyan")
    table.add_column("Cost", justify="right", style="green")
    table.add_column("Distribution (% of max)", ratio=1)

    # Check if there's any data
    if not period_data.get("Groups"):
        table.add_row("No data found", "$0.00", "")
        return table

    # Sort by cost (highest first)
    costs.sort(key=lambda x: x[1], reverse=True)

    # Apply limit if specified
    if limit > 0:
        costs = costs[:limit]

    # Find max cost for bar scaling
    max_cost = max([cost for _, cost in costs], default=0)

    # Add rows
    for name, amount in costs:
        # Skip zero-cost items unless show_all is True
        if amount < 0.01 and not show_all:
            continue

        # Calculate bar length (max is console width / 2)
        max_bar_width = console_width / 2
        bar_width = 0
        if max_cost > 0:
            # Ensure the top item gets the full bar width
            percentage = (amount / max_cost) * 100
            bar_width = round((amount / max_cost) * max_bar_width)

        # Create a progress bar with percentage
        bar = "█" * bar_width

        # For items that are a fraction of max cost, add percentage label
        if max_cost > 0:
            percentage = (amount / max_cost) * 100
            # Only add percentage if not 100%
            if percentage < 100:
                bar = f"{bar} {percentage:.1f}%"
            else:
                bar = f"{bar} (max)"

        table.add_row(name, f"${amount:.2f}", bar)

    # If --show-all is being used, add a footnote
    if show_all and zero_count > 0:
        table.caption = f"Showing all items including {zero_count} with $0.00 cost"

    # Add caption explaining the distribution
    if table.caption:
        table.caption += (
            "\nDistribution bars show relative cost compared to the highest item"
        )
    else:
        table.caption = (
            "Distribution bars show relative cost compared to the highest item"
        )

    return table


def analyze_costs_detailed(
    start_date: str,
    end_date: str,
    service: str | None,
    top: int,
    show_region: bool,
    show_all: bool,
    granularity: str = "MONTHLY",
) -> None:
    """Analyze costs with detailed breakdown by SERVICE, USAGE_TYPE, and optionally REGION."""
    console = Console()

    # Determine the service name to display
    display_service = service
    if service:
        normalized_service = AWSService.get_service(service)
        if normalized_service != service:
            display_service = f"{service} ({normalized_service})"

    title = "AWS Detailed Cost Analysis"
    if display_service:
        title += f" - {display_service}"

    console.print(Panel(f"[bold]{title}[/bold]\n{start_date} to {end_date}"))

    # Process each grouping type
    console.print("\n[bold]Analyzing by USAGE_TYPE with SERVICE information[/bold]")

    # Get cost data with both SERVICE and USAGE_TYPE groupings
    cost_data = get_cost_data(
        start_date, end_date, service, ["SERVICE", "USAGE_TYPE"], granularity
    )

    # Check if we got any data
    has_data = False
    for period in cost_data.get("ResultsByTime", []):
        if period.get("Groups"):
            has_data = True
            break

    if not has_data:
        console.print("[yellow]No data found for the specified parameters.[/yellow]")
        return

    # Display costs for each month
    for period in cost_data["ResultsByTime"]:
        month_start = period["TimePeriod"]["Start"]
        month_display = format_date_period(month_start, granularity)

        # Count items
        costs = []
        for group in period["Groups"]:
            # Extract service and usage type from the keys
            keys = group["Keys"]
            service_name = keys[0]
            usage_type = keys[1]

            amount = float(group["Metrics"]["BlendedCost"]["Amount"])
            costs.append((service_name, usage_type, amount))

        # Calculate displayed vs total count
        total_count = len(costs)
        non_zero_count = sum(1 for _, _, amount in costs if amount >= 0.01)
        zero_count = total_count - non_zero_count

        # Create title with count information
        if show_all or zero_count == 0:
            title = f"{month_display} Costs"
        else:
            title = f"{month_display} Costs [dim]• Showing {non_zero_count} of {total_count} items (hidden: {zero_count} zero-cost items)[/dim]"

        table = Table(title=title, expand=True)
        table.add_column("Service", style="cyan")
        table.add_column("Usage Type", style="green")
        table.add_column("Cost", justify="right")
        table.add_column("Distribution (% of max)", ratio=1)

        # Sort by cost (highest first)
        costs.sort(key=lambda x: x[2], reverse=True)

        # Apply limit if specified
        if top > 0:
            costs = costs[:top]

        # Find max cost for bar scaling
        max_cost = max([cost for _, _, cost in costs], default=0)

        # Add rows
        for service_name, usage_type, amount in costs:
            # Skip zero-cost items unless show_all is True
            if amount < 0.01 and not show_all:
                continue

            # Calculate bar length (max is console width / 2)
            max_bar_width = console.width / 2
            bar_width = 0
            if max_cost > 0:
                # Ensure the top item gets the full bar width
                percentage = (amount / max_cost) * 100
                bar_width = round((amount / max_cost) * max_bar_width)

            # Create a progress bar with percentage
            bar = "█" * bar_width

            # For items that are a fraction of max cost, add percentage label
            if max_cost > 0:
                percentage = (amount / max_cost) * 100
                # Only add percentage if not 100%
                if percentage < 100:
                    bar = f"{bar} {percentage:.1f}%"
                else:
                    bar = f"{bar} (max)"

            table.add_row(service_name, usage_type, f"${amount:.2f}", bar)

        # Add caption explaining the distribution
        table.caption = (
            "Distribution bars show relative cost compared to the highest item"
        )

        console.print(table)

    # If region breakdown is requested, add that too
    if show_region:
        console.print("\n[bold]Analyzing by REGION[/bold]")
        region_data = get_cost_data(
            start_date, end_date, service, "REGION", granularity
        )

        # Display costs for each month
        for period in region_data["ResultsByTime"]:
            # Create and display monthly table
            table = create_cost_table(
                period, console.width, "REGION", top, show_all, granularity
            )
            console.print(table)

    # Display cost breakdown insights
    console.print("\n[bold]Cost Summary by Month[/bold]")

    # Get service-level data for summary
    service_data = get_cost_data(start_date, end_date, service, "SERVICE", granularity)
    monthly_totals = []
    grand_total = 0.0

    for period in service_data["ResultsByTime"]:
        monthly_total = 0.0
        for group in period.get("Groups", []):
            monthly_total += float(group["Metrics"]["BlendedCost"]["Amount"])

        # Add other costs not grouped (if any)
        if "Total" in period and "BlendedCost" in period["Total"]:
            monthly_total = float(period["Total"]["BlendedCost"]["Amount"])

        grand_total += monthly_total
        month_name = format_date_period(period["TimePeriod"]["Start"], granularity)
        monthly_totals.append((month_name, monthly_total))

    # Display summary table
    summary_table = Table(title="Monthly Summary", expand=True)
    summary_table.add_column("Month", style="cyan")
    summary_table.add_column("Total Cost", justify="right", style="green")
    summary_table.add_column("Distribution (% of max)", ratio=1)

    # Find max monthly total for bar scaling
    max_monthly_total = max([total for _, total in monthly_totals], default=0)

    for month, total in monthly_totals:
        # Calculate bar length
        max_bar_width = console.width / 2
        bar_width = 0
        if max_monthly_total > 0:
            # Ensure the top month gets the full bar width
            percentage = (total / max_monthly_total) * 100
            bar_width = round((total / max_monthly_total) * max_bar_width)

        # Create a progress bar with percentage
        bar = "█" * bar_width

        # For months that are a fraction of max cost, add percentage label
        if max_monthly_total > 0:
            percentage = (total / max_monthly_total) * 100
            # Only add percentage if not 100%
            if percentage < 100:
                bar = f"{bar} {percentage:.1f}%"
            else:
                bar = f"{bar} (max)"

        summary_table.add_row(month, f"${total:.2f}", bar)

    # Add grand total row without bar
    summary_table.add_row("GRAND TOTAL", f"${grand_total:.2f}", "", style="bold")

    console.print(summary_table)


def get_cost_reduction_tip(service_name: str) -> str | None:
    """Get cost reduction tip for specific services."""
    tips = {
        AWSService.CLOUDWATCH.aws_name: "Consider optimizing log retention, reducing alarms, or consolidating dashboards",
        AWSService.S3.aws_name: "Review storage classes, lifecycle policies, and delete unnecessary objects",
        AWSService.RDS.aws_name: "Consider reserved instances, stop unused instances, or optimize instance size",
        AWSService.EC2.aws_name: "Use Spot/Reserved instances, right-size instances, or terminate unused resources",
        AWSService.LAMBDA.aws_name: "Optimize memory allocation, reduce duration, or consolidate functions",
        AWSService.DYNAMODB.aws_name: "Review provisioned capacity, use on-demand when appropriate",
        AWSService.ECR.aws_name: "Clean up unused container images and review lifecycle policies",
        AWSService.ROUTE53.aws_name: "Review hosted zones and resource record sets",
        AWSService.SNS.aws_name: "Review notification volume and optimize topic/subscription patterns",
        AWSService.SQS.aws_name: "Review queue usage and message volume",
        AWSService.ELB.aws_name: "Consolidate load balancers and remove unused ones",
        AWSService.EFS.aws_name: "Review file system usage and move infrequently accessed data to lower-cost tiers",
        AWSService.API_GATEWAY.aws_name: "Implement caching and review API call volume",
    }

    # Check for exact matches
    if service_name in tips:
        return tips[service_name]

    # Check for prefix matches
    for prefix, tip in tips.items():
        if service_name.startswith(prefix):
            return tip

    return None


def analyze_costs_simple(
    start_date: str,
    end_date: str,
    service: str | None,
    top: int,
    show_all: bool,
    granularity: str = "MONTHLY",
) -> None:
    """Simple cost analysis view."""
    console = Console()

    # Determine the service name to display
    display_service = service
    if service:
        normalized_service = AWSService.get_service(service)
        if normalized_service != service:
            display_service = f"{service} ({normalized_service})"

    if not service:
        title = "AWS Cost Analysis"
    else:
        title = f"AWS {display_service} Cost Analysis"

    console.print(Panel(f"[bold]{title}[/bold]\n{start_date} to {end_date}"))

    # Get cost data from AWS Cost Explorer using SERVICE grouping for simple view
    cost_data = get_cost_data(start_date, end_date, service, "SERVICE", granularity)

    # Check if we got any data
    has_data = False
    for period in cost_data.get("ResultsByTime", []):
        if period.get("Groups"):
            has_data = True
            break

    if not has_data:
        console.print(
            "[bold yellow]No cost data found for the specified parameters.[/bold yellow]"
        )
        return

    # Display costs for each month
    grand_total = 0.0
    monthly_totals = []

    for period in cost_data["ResultsByTime"]:
        # Calculate monthly total
        monthly_total = 0.0
        for group in period.get("Groups", []):
            monthly_total += float(group["Metrics"]["BlendedCost"]["Amount"])

        # Add other costs not grouped (if any)
        if "Total" in period and "BlendedCost" in period["Total"]:
            monthly_total = float(period["Total"]["BlendedCost"]["Amount"])

        grand_total += monthly_total
        month_name = format_date_period(period["TimePeriod"]["Start"], granularity)
        monthly_totals.append((month_name, monthly_total))

        # Create and display monthly table
        table = create_cost_table(
            period, console.width, "SERVICE", top, show_all, granularity
        )
        console.print(table)

    # Display summary table
    summary_table = Table(title="Monthly Summary", expand=True)
    summary_table.add_column("Month", style="cyan")
    summary_table.add_column("Total Cost", justify="right", style="green")
    summary_table.add_column("Distribution (% of max)", ratio=1)

    # Find max monthly total for bar scaling
    max_monthly_total = max([total for _, total in monthly_totals], default=0)

    for month, total in monthly_totals:
        # Calculate bar length
        max_bar_width = console.width / 2
        bar_width = 0
        if max_monthly_total > 0:
            # Ensure the top month gets the full bar width
            percentage = (total / max_monthly_total) * 100
            bar_width = round((total / max_monthly_total) * max_bar_width)

        # Create a progress bar with percentage
        bar = "█" * bar_width

        # For months that are a fraction of max cost, add percentage label
        if max_monthly_total > 0:
            percentage = (total / max_monthly_total) * 100
            # Only add percentage if not 100%
            if percentage < 100:
                bar = f"{bar} {percentage:.1f}%"
            else:
                bar = f"{bar} (max)"

        summary_table.add_row(month, f"${total:.2f}", bar)

    # Add grand total row without bar
    summary_table.add_row("GRAND TOTAL", f"${grand_total:.2f}", "", style="bold")

    console.print(summary_table)

    # Display cost breakdown insights
    if grand_total > 0:
        console.print("\n[bold]Cost Breakdown Insights:[/bold]")

        # Aggregate costs by group type
        item_costs = {}
        for period in cost_data["ResultsByTime"]:
            for group in period.get("Groups", []):
                item_name = group["Keys"][0]
                amount = float(group["Metrics"]["BlendedCost"]["Amount"])

                if item_name not in item_costs:
                    item_costs[item_name] = 0
                item_costs[item_name] += amount

        # Sort by total cost
        sorted_items = sorted(item_costs.items(), key=lambda x: x[1], reverse=True)
        top_items = sorted_items[:5]  # Top 5 cost items

        for item_name, amount in top_items:
            percentage = (amount / grand_total) * 100
            console.print(
                f"• [cyan]{item_name}[/cyan]: ${amount:.2f} ([bold]{percentage:.1f}%[/bold] of total costs)"
            )

            # Provide tips for known services
            tip = get_cost_reduction_tip(item_name)
            if tip:
                console.print(f"  [yellow]Tip:[/yellow] {tip}")
