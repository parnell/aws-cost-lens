"""
AWS Cost Analyzer Core Module

Core functionality for displaying AWS costs by service and usage type with rich formatting.
"""

import sys
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, NamedTuple

import boto3
from flexible_datetime import flex_datetime
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from tqdm import tqdm

from .amazon_credits import print_amazon_credits_section
from .summary_bars import (
    AccountGrandSlice,
    _format_net_usd,
    _format_usage_credit_cells,
    _rich_usd_positive_red_negative_green,
    _rich_usd_record_type_row,
    _rich_usd_signed_bold,
    _service_usage_credit_bar,
    _split_usage_credit,
    account_color_pair,
    build_accounts_grand_total_table,
    build_monthly_summary_table,
    create_service_record_type_split_table,
)

# AWS Cost Explorer limits
MAX_HOURLY_GRANULARITY_DAYS = 14


def _parse_ce_time_bound_to_utc(value: str) -> datetime:
    """Parse a Cost Explorer TimePeriod bound using flexible-datetime (returns UTC, no micros)."""
    dt = flex_datetime(value).to_datetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.replace(microsecond=0)


def normalize_ce_time_period_bound(value: str) -> str:
    """
    Parse a start/end string and return a CE-friendly value: ``YYYY-MM-DD`` at UTC midnight,
    else ``YYYY-MM-DDTHH:MM:SSZ``.
    """
    dt = _parse_ce_time_bound_to_utc(value)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# Request these in one GetCostAndUsage call. Unblended / Blended / NetUnblended are the
# classic cash-style metrics; Amortized / NetAmortized spread RI and Savings Plans commitment
# across usage (often what the Cost Management console shows by default).
CE_METRICS_BUNDLE = [
    "UnblendedCost",
    "BlendedCost",
    "NetUnblendedCost",
    "AmortizedCost",
    "NetAmortizedCost",
]

# Fallback display metric if every bundled metric sums to zero.
DEFAULT_COST_METRIC = "UnblendedCost"

# CLI-friendly names -> Cost Explorer API metric names (see GetCostAndUsage Metrics).
# Does not include "auto" (handled separately).
COST_METRIC_ALIASES: dict[str, str] = {
    "unblended": "UnblendedCost",
    "blended": "BlendedCost",
    "net-unblended": "NetUnblendedCost",
    "amortized": "AmortizedCost",
    "net-amortized": "NetAmortizedCost",
}

# When ``metric_preference`` is ``auto``, we pick the metric with the largest |total| across
# the response. Tie-break prefers console-like amortized metrics, then unblended-style.
_METRIC_AUTO_ORDER = [
    "NetAmortizedCost",
    "AmortizedCost",
    "UnblendedCost",
    "NetUnblendedCost",
    "BlendedCost",
]


def get_account_header_markup() -> str:
    """
    Rich markup line identifying the caller's AWS account: name (when known) and 12-digit ID.

    Tries Organizations account name (matches console account name in many orgs), then IAM
    account alias, then the account ID alone.
    """
    try:
        account_id = boto3.client("sts").get_caller_identity().get("Account") or ""
    except Exception:
        return "[yellow]Account: unable to resolve (check AWS credentials)[/yellow]"

    if not account_id:
        return "[yellow]Account: unknown[/yellow]"

    name: str | None = None
    try:
        org = boto3.client("organizations")
        acc = org.describe_account(AccountId=account_id).get("Account") or {}
        raw = (acc.get("Name") or "").strip()
        if raw:
            name = raw
    except Exception:
        pass

    if not name:
        try:
            aliases = boto3.client("iam").list_account_aliases().get("AccountAliases") or []
            if aliases:
                name = aliases[0]
        except Exception:
            pass

    if name:
        return f"Account: [cyan]{name}[/cyan] [dim]({account_id})[/dim]"
    return f"Account: [cyan]{account_id}[/cyan]"


def _merge_cost_and_usage_pages(pages: list[dict]) -> dict:
    """Merge paginated get_cost_and_usage responses (extra Groups per time period)."""
    if not pages:
        return {}
    merged = pages[0]
    for page in pages[1:]:
        for i, period in enumerate(page.get("ResultsByTime", [])):
            merged["ResultsByTime"][i]["Groups"].extend(period.get("Groups", []))
    return merged


def _fetch_cost_and_usage_paginated(ce_client, request_params: dict) -> dict:
    """Call get_cost_and_usage until NextPageToken is exhausted."""
    pages: list[dict] = []
    next_token: str | None = None
    while True:
        params = dict(request_params)
        if next_token:
            params["NextPageToken"] = next_token
        page = ce_client.get_cost_and_usage(**params)
        pages.append(page)
        next_token = page.get("NextPageToken")
        if not next_token:
            break
    return _merge_cost_and_usage_pages(pages)


def _period_metric_total(period: dict, metric: str) -> float:
    """Total for one metric in a time period (prefer API Total, else sum of groups)."""
    t = period.get("Total") or {}
    block = t.get(metric)
    if block and block.get("Amount") not in (None, ""):
        return float(block["Amount"])
    return sum(_metric_amount_raw(g, metric) for g in period.get("Groups", []))


def _period_charges_and_credits(period: dict, metric: str) -> tuple[float, float]:
    """
    Split group amounts into positive charges vs credits/refunds (negative lines).

    Returns (charges, credits) where credits is a non-positive sum (e.g. -60.81).
    """
    charges = 0.0
    credits = 0.0
    for g in period.get("Groups", []):
        a = _metric_amount_raw(g, metric)
        if a > 0:
            charges += a
        elif a < 0:
            credits += a
    return charges, credits


def rollup_net_charges_credits(results_by_time: list, metric: str) -> tuple[float, float, float]:
    """Net total, sum of positive lines, sum of negative lines across all periods."""
    net = 0.0
    charges = 0.0
    credits = 0.0
    for period in results_by_time:
        net += _period_metric_total(period, metric)
        c, cr = _period_charges_and_credits(period, metric)
        charges += c
        credits += cr
    return net, charges, credits


def rollup_record_type_totals(results_by_time: list, metric: str) -> dict[str, float]:
    """Sum amounts by Cost Explorer RECORD_TYPE (Usage, Credit, Tax, Refund, …)."""
    out: dict[str, float] = {}
    for period in results_by_time:
        for g in period.get("Groups", []):
            key = (g.get("Keys") or ["?"])[0]
            out[key] = out.get(key, 0.0) + _metric_amount_raw(g, metric)
    return out


def _build_ce_request_filter(
    service: str | None,
    region: str | None,
    record_type_values: list[str] | None = None,
) -> dict | None:
    """
    Build a ``GetCostAndUsage`` ``Filter`` from optional service, region, and RECORD_TYPE values.
    """
    parts: list[dict] = []
    if record_type_values:
        parts.append({"Dimensions": {"Key": "RECORD_TYPE", "Values": list(record_type_values)}})
    if service:
        normalized = AWSService.get_service(service)
        parts.append({"Dimensions": {"Key": "SERVICE", "Values": [normalized]}})
    if region:
        parts.append({"Dimensions": {"Key": "REGION", "Values": [region]}})
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return {"And": parts}


def _metric_amount_raw(group: dict, metric: str) -> float:
    """Parse one metric from a group; missing keys or amounts are 0."""
    block = (group.get("Metrics") or {}).get(metric)
    if not block or block.get("Amount") in (None, ""):
        return 0.0
    return float(block["Amount"])


def resolve_effective_metric(results_by_time: list, metric_preference: str) -> str:
    """
    Choose which Cost Explorer metric to display.

    ``metric_preference`` is ``auto`` or a key in COST_METRIC_ALIASES (e.g. ``unblended``).
    """
    if metric_preference != "auto":
        return COST_METRIC_ALIASES[metric_preference]
    totals = {m: 0.0 for m in CE_METRICS_BUNDLE}
    for period in results_by_time:
        for m in CE_METRICS_BUNDLE:
            totals[m] += abs(_period_metric_total(period, m))
    return max(_METRIC_AUTO_ORDER, key=lambda k: (totals[k], -_METRIC_AUTO_ORDER.index(k)))


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


def ce_api_json_default(obj: Any) -> Any:
    """Default for :func:`json.dumps` when serializing Cost Explorer (boto3) payloads."""
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__!s} is not JSON serializable")


def get_cost_data(
    start_date: str,
    end_date: str,
    service: str | None,
    group_by: str | list[str] | None,
    granularity: str = "MONTHLY",
    region: str | None = None,
    record_type_values: list[str] | None = None,
    ce_api_dump: list[dict] | None = None,
    ce_api_label: str = "get_cost_data",
) -> dict:
    """
    Fetch cost data from AWS Cost Explorer API.

    If ``group_by`` is ``None``, ``GetCostAndUsage`` is called **without** ``GroupBy`` so each
    period includes the API ``Total`` block (official per-period totals for the bundled metrics).
    """
    console = Console()

    label = f" for {service}" if service else ""
    desc = (
        f"Fetching AWS account totals{label}"
        if group_by is None
        else f"Fetching AWS costs{label}"
    )
    with tqdm(total=1, desc=desc, unit="", leave=False, dynamic_ncols=True) as pbar:
        try:
            ce_client = boto3.client("ce")

            start_date = normalize_ce_time_period_bound(start_date)
            end_date = normalize_ce_time_period_bound(end_date)

            # Format dates correctly for HOURLY granularity
            # AWS Cost Explorer API expects timestamps in ISO 8601 format for HOURLY
            if granularity == "HOURLY":
                # Convert dates to datetime objects (bounds are already CE-normalized strings)
                start_datetime = _parse_ce_time_bound_to_utc(start_date)
                end_datetime = _parse_ce_time_bound_to_utc(end_date)

                today = datetime.now().astimezone()
                start_local = start_datetime.astimezone(today.tzinfo)
                end_local = end_datetime.astimezone(today.tzinfo)

                # Check if range is within 14 days
                days_diff = (end_local - start_local).days
                days_from_today_start = (today - start_local).days

                if (
                    days_diff > MAX_HOURLY_GRANULARITY_DAYS
                    or days_from_today_start > MAX_HOURLY_GRANULARITY_DAYS
                ):
                    console.print(
                        f"[yellow]Warning: HOURLY granularity is only available for the past "
                        f"{MAX_HOURLY_GRANULARITY_DAYS} days.[/yellow]"
                    )
                    console.print("[yellow]Falling back to DAILY granularity.[/yellow]")
                    granularity = "DAILY"
                else:
                    # Add time component and convert to ISO 8601 format
                    # For start date, use 00:00:00Z (start of day)
                    # For end date, use 23:59:59Z (end of day)
                    start_date = start_datetime.strftime("%Y-%m-%dT00:00:00Z")

                    # If end date is today, use current time, otherwise use end of day
                    if (end_local.date() - today.date()).days == 0:
                        end_date = today.strftime("%Y-%m-%dT%H:%M:%SZ")
                    else:
                        end_date = end_local.strftime("%Y-%m-%dT23:59:59Z")

            # Base request parameters
            request_params = {
                "TimePeriod": {"Start": start_date, "End": end_date},
                "Granularity": granularity,
                "Metrics": list(CE_METRICS_BUNDLE),
            }

            if group_by is not None:
                if isinstance(group_by, str):
                    request_params["GroupBy"] = [{"Type": "DIMENSION", "Key": group_by}]
                else:
                    request_params["GroupBy"] = [
                        {"Type": "DIMENSION", "Key": key} for key in group_by
                    ]

            ce_filter = _build_ce_request_filter(service, region, record_type_values)
            if ce_filter is not None:
                request_params["Filter"] = ce_filter

            response = _fetch_cost_and_usage_paginated(ce_client, request_params)

            pbar.update(1)
            if ce_api_dump is not None:
                ce_api_dump.append({"label": ce_api_label, "response": response})
            return response

        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e!s}")
            sys.exit(1)


def _append_credit_attribution_dumps(
    start_date: str,
    end_date: str,
    service: str | None,
    region: str | None,
    granularity: str,
    display_metric: str,
    credit_row_total: float,
    ce_api_dump: list[dict],
) -> tuple[dict[str, float], dict[str, float]]:
    """
    Extra CE calls: ``RECORD_TYPE=Credit`` with ``GroupBy`` SERVICE and USAGE_TYPE. Returns rollups
    for ``display_metric``; skips when total Credit is negligible.
    """
    if abs(credit_row_total) < 0.005:
        return {}, {}
    svc = get_cost_data(
        start_date,
        end_date,
        service,
        "SERVICE",
        granularity,
        region,
        record_type_values=["Credit"],
        ce_api_dump=ce_api_dump,
        ce_api_label="credit:by_service",
    )
    uty = get_cost_data(
        start_date,
        end_date,
        service,
        "USAGE_TYPE",
        granularity,
        region,
        record_type_values=["Credit"],
        ce_api_dump=ce_api_dump,
        ce_api_label="credit:by_usage_type",
    )
    s_tot = rollup_record_type_totals(svc.get("ResultsByTime", []), display_metric)
    u_tot = rollup_record_type_totals(uty.get("ResultsByTime", []), display_metric)
    return s_tot, u_tot


def _fill_json_out_summary(
    out: dict,
    start_date: str,
    end_date: str,
    display_metric: str,
    rt_payload: dict,
    credit_by_service: dict[str, float],
    credit_by_usage_type: dict[str, float],
) -> None:
    """Fill ``--out json`` `summary` block: RECORD_TYPE by period, credit split, vs
    Billing UI hint."""
    per: list[dict] = []
    for period in rt_payload.get("ResultsByTime", []):
        p_rt = rollup_record_type_totals([period], display_metric)
        per.append(
            {
                "TimePeriod": period.get("TimePeriod"),
                "Estimated": period.get("Estimated"),
                "RecordType": p_rt,
                "net_from_record_type_rows": float(sum(p_rt.values())),
            }
        )

    mtd: dict | None = None
    for row in reversed(per):
        p_rt = row.get("RecordType") or {}
        if any(abs(v) >= 0.005 for v in p_rt.values()):
            mtd = row
            break
    if mtd is None and per:
        mtd = per[-1]

    out["start_date"] = start_date
    out["end_date"] = end_date
    out["display_metric"] = display_metric
    out["per_period_record_type"] = per
    out["record_type_rollup"] = rollup_record_type_totals(
        rt_payload.get("ResultsByTime", []), display_metric
    )
    out["credit_allocations"] = {
        "by_service": credit_by_service,
        "by_usage_type": credit_by_usage_type,
    }

    if mtd and isinstance(mtd.get("RecordType"), dict):
        mrt: dict = mtd["RecordType"]
        u = float(mrt.get("Usage", 0.0))
        c = float(mrt.get("Credit", 0.0)) + float(mrt.get("Refund", 0.0))
        tax = float(mrt.get("Tax", 0.0))
        out["most_recent_in_range"] = {
            "TimePeriod": mtd.get("TimePeriod"),
            "Estimated": mtd.get("Estimated"),
            "record_type_usage": u,
            "record_type_credits_and_refunds": c,
            "record_type_tax": tax,
            "implied_net_after_credits_refunds_and_tax": u + c + tax,
        }
    out["cost_management_ui_hint"] = (
        'The Billing home "Cost summary" / "Month-to-date cost" figure is often the same order of '
        "magnitude as **RECORD_TYPE=Usage** (gross) for the month so far, using the console's "
        "default cost type; it may not list **RECORD_TYPE=Credit** on that card. In Cost "
        "Explorer, net is reflected across line types: Usage + Credit + Refund + Tax "
        "(and any other record types in your data). For per-invoice or grant names, use "
        "Billing → Credits or Cost & Usage Reports, not `GetCostAndUsage`."
    )
    mri = out.get("most_recent_in_range")
    if (
        mri
        and isinstance(mri, dict)
        and "record_type_usage" in mri
        and "implied_net_after_credits_refunds_and_tax" in mri
    ):
        ug = float(mri["record_type_usage"])
        nt = float(mri["implied_net_after_credits_refunds_and_tax"])
        out["one_line_mtd_reconciliation"] = (
            f"Gross (RECORD_TYPE Usage) ≈ ${ug:.2f}; net (Usage+Credit+Refund+Tax) ≈ "
            f'${nt:.2f} for the window. The Billing "Month-to-date cost" number is often close to '
            f"**gross**; credits are easy to miss on that card. Ungrouped Cost Explorer (see "
            f"`reconcile:ungrouped_total` in `calls`) should match **net** ≈ ${nt:.2f}."
        )


def list_available_services(
    start_date: str,
    end_date: str,
    region: str | None = None,
    metric_preference: str = "auto",
    ce_api_dump: list[dict] | None = None,
) -> None:
    """List all available AWS services that have cost data."""
    console = Console()
    console.print(Panel(get_account_header_markup(), title="AWS account"))
    console.print()

    with tqdm(
        total=1,
        desc="Fetching available AWS services...",
        unit="",
        leave=False,
        dynamic_ncols=True,
    ) as pbar:
        try:
            ce_client = boto3.client("ce")

            # Build request parameters
            request_params = {
                "TimePeriod": {"Start": start_date, "End": end_date},
                "Granularity": "MONTHLY",
                "Metrics": list(CE_METRICS_BUNDLE),
                "GroupBy": [
                    {"Type": "DIMENSION", "Key": "SERVICE"},
                ],
            }

            # Add region filter if specified
            if region:
                request_params["Filter"] = {"Dimensions": {"Key": "REGION", "Values": [region]}}

            response = _fetch_cost_and_usage_paginated(ce_client, request_params)

            pbar.update(1)
            if ce_api_dump is not None:
                ce_api_dump.append({"label": "list_services:by_service", "response": response})

            chosen = resolve_effective_metric(response.get("ResultsByTime", []), metric_preference)

            # Extract unique service names (non-negligible cost under chosen metric)
            services = set()
            for period in response.get("ResultsByTime", []):
                for group in period.get("Groups", []):
                    if abs(_metric_amount_raw(group, chosen)) < 0.000001:
                        continue
                    service_name = group["Keys"][0]
                    services.add(service_name)

            # Sort and display services
            services = sorted(list(services))

            if not services:
                console.print(
                    "[yellow]No services found with cost data in the specified time range."
                    "[/yellow]"
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
            console.print(f"[bold red]Error:[/bold red] {e!s}")
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


def _metric_amount(group: dict, metric: str) -> float:
    """Parse cost amount for the requested Cost Explorer metric."""
    return _metric_amount_raw(group, metric)


def create_cost_table(
    period_data: dict,
    console_width: int,
    group_by: str,
    limit: int,
    show_all: bool = False,
    granularity: str = "MONTHLY",
    metric: str = DEFAULT_COST_METRIC,
    record_type_for_period: dict[str, float] | None = None,
    verbose: bool = False,
) -> Table:
    """Create a rich table for a single time period.

    When costs mix large credits on one SERVICE line with smaller usage lines, a single |max|
    makes bars misleading. We scale **positive** rows against the largest positive line and
    **negative** rows against the largest |negative| line.

    ``record_type_for_period`` (Usage / Credit / …) adds a caption tying SERVICE rows to
    CE's billing-style RECORD_TYPE totals for the same window.
    """
    period_start = period_data["TimePeriod"]["Start"]
    period_display = format_date_period(period_start, granularity)

    if granularity == "DAILY":
        title_prefix = "Daily"
    elif granularity == "HOURLY":
        title_prefix = "Hourly"
    else:
        title_prefix = "Monthly"

    title = f"{title_prefix} {period_display} Costs"

    # Calendar-based provisional period (MTD etc.), not tied to whether net spend is $0.
    is_in_progress = should_show_in_progress(period_start, granularity)

    # Modify title for in-progress months
    if is_in_progress:
        title = f"{title_prefix} {period_display} Costs [yellow](In Progress)[/yellow]"

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
        amount = _metric_amount(group, metric)
        costs.append((name, amount))

    # Calculate displayed vs total count
    total_count = len(costs)
    non_zero_count = sum(1 for _, amount in costs if abs(amount) >= 0.01)
    zero_count = total_count - non_zero_count

    # Create title with count information
    if show_all or zero_count == 0:
        if not is_in_progress:
            title = f"{title_prefix} {period_display} Costs"
        else:
            title = f"{title_prefix} {period_display} Costs [yellow](In Progress)[/yellow]"
    else:
        if not is_in_progress:
            title = (
                f"{title_prefix} {period_display} Costs "
                f"[dim]• Showing {non_zero_count} of {total_count} items "
                f"(hidden: {zero_count} zero-cost items)[/dim]"
            )
        else:
            title = (
                f"{title_prefix} {period_display} Costs [yellow](In Progress)[/yellow] "
                f"[dim]• Showing {non_zero_count} of {total_count} items "
                f"(hidden: {zero_count} zero-cost items)[/dim]"
            )

    table = Table(title=title, expand=True)
    table.add_column(group_title, style="cyan")
    table.add_column("Usage", justify="right", style="red")
    table.add_column("Credits", justify="right", style="green")
    table.add_column("Bar (red=paid · green=credits)", ratio=1)

    # Check if there's any data
    if not period_data.get("Groups"):
        if is_in_progress:
            table.add_row(
                "In Progress",
                "[yellow]Data not yet available[/yellow]",
                "",
                "",
                "",
            )
        else:
            table.add_row("No data found", "$0.00", "—", "—", "")
        return table

    # Sort by cost (highest first)
    costs.sort(key=lambda x: x[1], reverse=True)

    # Apply limit if specified
    if limit > 0:
        costs = costs[:limit]

    displayed = [(n, a) for n, a in costs if show_all or abs(a) >= 0.01]
    pos_amts = [a for _, a in displayed if a > 0.01]
    neg_amts = [a for _, a in displayed if a < -0.01]
    max_pos = max(pos_amts, default=0.0)
    max_neg = max((abs(a) for a in neg_amts), default=0.0)

    # Add rows
    for name, amount in costs:
        # Skip negligible amounts unless show_all is True (include credits / negatives)
        if abs(amount) < 0.01 and not show_all:
            continue

        u, c = _split_usage_credit(amount)
        usage_s, cred_s = _format_usage_credit_cells(amount)
        bar_cell = _service_usage_credit_bar(u, c, max_pos, max_neg)

        table.add_row(name, usage_s, cred_s, bar_cell)

    # If --show-all is being used, add a footnote
    if show_all and zero_count > 0:
        table.caption = f"Showing all items including {zero_count} with $0.00 cost"

    if verbose:
        cap_bits = [
            "Bar: [red]red[/red] = usage (paid) vs largest usage in table; "
            "[green]green[/green] = |credits| vs largest |credit| line "
            "(separate scales side by side)."
        ]
        if group_by == "SERVICE" and record_type_for_period:
            u = record_type_for_period.get("Usage")
            cr = record_type_for_period.get("Credit", 0.0) + record_type_for_period.get(
                "Refund", 0.0
            )
            if u is not None and abs(u) >= 0.005:
                cap_bits.append(
                    "RECORD_TYPE for this period: Usage "
                    f"[red]{_format_net_usd(u)}[/red] • Credits/refunds "
                    f"[green]{_format_net_usd(cr)}[/green] "
                    "(SERVICE rows allocate credits unevenly)."
                )
        if table.caption:
            table.caption += "\n" + " ".join(cap_bits)
        else:
            table.caption = " ".join(cap_bits)

    # Add in-progress note to the caption if applicable
    if is_in_progress:
        if table.caption:
            table.caption += (
                "\n[yellow]Note: This month is still in progress. Data may be incomplete.[/yellow]"
            )
        else:
            table.caption = (
                "[yellow]Note: This month is still in progress. Data may be incomplete.[/yellow]"
            )

    return table


def _find_matching_period(results_by_time: list, canonical: dict) -> dict:
    """Return the CE period whose start matches ``canonical`` (date-normalized), or empty groups."""
    want = _period_start_date(canonical["TimePeriod"]["Start"])
    for p in results_by_time:
        if _period_start_date(p["TimePeriod"]["Start"]) == want:
            return p
    return {"TimePeriod": canonical["TimePeriod"], "Groups": []}


def _period_service_amount_map(period: dict, metric: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for group in period.get("Groups") or []:
        keys = group.get("Keys") or []
        if not keys:
            continue
        name = keys[0]
        out[name] = out.get(name, 0.0) + _metric_amount_raw(group, metric)
    return out


def _period_start_date(period_date: str) -> str:
    """Normalize CE TimePeriod.Start (possibly ISO) to YYYY-MM-DD."""
    if "T" in period_date:
        return period_date.split("T", 1)[0][:10]
    return period_date[:10]


def should_show_in_progress(period_date: str, granularity: str = "MONTHLY") -> bool:
    """
    Whether this Cost Explorer bucket is still provisional (MTD, today, future, or invoice lag).

    This must not depend on dollar totals: a current month can net to $0 after credits and is
    still month-to-date. Previously we treated any positive total as "complete", which hid MTD
    labels and dropped totals from the summary.
    """
    raw = _period_start_date(period_date)
    period_dt = datetime.strptime(raw, "%Y-%m-%d")
    today = datetime.now()

    if granularity == "HOURLY":
        if period_dt.date() > today.date():
            return True
        return period_dt.date() == today.date()

    if granularity == "DAILY":
        if period_dt.date() > today.date():
            return True
        return period_dt.date() == today.date()

    # MONTHLY: compare calendar (year, month) only — do not use datetime ordering on
    # period-start-at-midnight vs "today.replace(day=1)" which keeps the clock time.
    p_month = (period_dt.year, period_dt.month)
    t_month = (today.year, today.month)

    if p_month > t_month:
        return True
    if p_month == t_month:
        return True

    # Closed past months — except the month immediately before today during invoice finalization
    months_diff = (t_month[0] - p_month[0]) * 12 + (t_month[1] - p_month[1])
    if months_diff == 1 and today.day <= 5:
        return True
    return False


def analyze_costs_detailed(
    start_date: str,
    end_date: str,
    service: str | None,
    top: int,
    show_region: bool = False,
    show_all: bool = False,
    granularity: str = "MONTHLY",
    region: str | None = None,
    metric_preference: str = "auto",
    reconcile: bool = True,
    verbose: bool = False,
    ce_api_dump: list[dict] | None = None,
    out_summary: dict | None = None,
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

    if region:
        title += f" in {region}"

    console.print(
        Panel(
            f"{get_account_header_markup()}\n[bold]{title}[/bold]\n{start_date} to {end_date}"
        )
    )

    # Process each grouping type
    console.print("\n[bold]Analyzing by USAGE_TYPE with SERVICE information[/bold]")

    # Get cost data with both SERVICE and USAGE_TYPE groupings
    cost_data = get_cost_data(
        start_date,
        end_date,
        service,
        ["SERVICE", "USAGE_TYPE"],
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="detailed:service+usage_type",
    )

    # Check if we got any data
    has_data = False
    for period in cost_data.get("ResultsByTime", []):
        if period.get("Groups"):
            has_data = True
            break

    if not has_data:
        if out_summary is not None:
            out_summary["status"] = "no_cost_data"
        console.print("[yellow]No data found for the specified parameters.[/yellow]")
        return

    display_metric = resolve_effective_metric(cost_data["ResultsByTime"], metric_preference)
    if metric_preference == "auto":
        console.print(
            f"[dim]Using Cost Explorer metric: {display_metric} "
            f"(auto picks the largest |total| across bundled CE metrics)[/dim]"
        )

    rt_payload = get_cost_data(
        start_date,
        end_date,
        service,
        "RECORD_TYPE",
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="detailed:record_type",
    )
    rt_by_type = rollup_record_type_totals(rt_payload["ResultsByTime"], display_metric)

    cr_svc: dict[str, float] = {}
    cr_uty: dict[str, float] = {}
    if ce_api_dump is not None:
        cr_svc, cr_uty = _append_credit_attribution_dumps(
            start_date,
            end_date,
            service,
            region,
            granularity,
            display_metric,
            float(rt_by_type.get("Credit", 0.0)),
            ce_api_dump,
        )
        if out_summary is not None and (
            cr_svc
            or cr_uty
            or abs(float(rt_by_type.get("Credit", 0.0))) >= 0.005
        ):
            console.print(
                "[dim]JSON dump: added credit:by_service and credit:by_usage_type "
                "(RECORD_TYPE=Credit).[/dim]"
            )

    net_roll, charges_roll, credits_roll = rollup_net_charges_credits(
        cost_data["ResultsByTime"], display_metric
    )
    console.print(
        "[dim]Rollup (by SERVICE rows):[/dim] "
        f"net {_rich_usd_positive_red_negative_green(net_roll)} "
        f"[dim]• gross charges[/dim] [red]{_format_net_usd(charges_roll)}[/red] "
        f"[dim]• credits/refunds[/dim] [green]{_format_net_usd(credits_roll)}[/green]"
    )
    rt_parts = []
    for key in ("Usage", "Credit", "Refund", "Tax", "Distributor Discount"):
        if key in rt_by_type and abs(rt_by_type[key]) >= 0.005:
            rt_parts.append(f"{key} {_rich_usd_record_type_row(key, rt_by_type[key])}")
    if rt_parts:
        console.print(
            '[dim]RECORD_TYPE (Billing home / CE "Usage vs credits" style): '
            + " • ".join(rt_parts)
            + "[/dim]"
        )
    print_amazon_credits_section(
        console, start_date, end_date, out_summary=out_summary
    )

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

            amount = _metric_amount(group, display_metric)
            costs.append((service_name, usage_type, amount))

        # Calculate displayed vs total count
        total_count = len(costs)
        non_zero_count = sum(1 for _, _, amount in costs if abs(amount) >= 0.01)
        zero_count = total_count - non_zero_count

        # Create title with count information
        if show_all or zero_count == 0:
            title = f"{month_display} Costs"
        else:
            title = (
                f"{month_display} Costs "
                f"[dim]• Showing {non_zero_count} of {total_count} items "
                f"(hidden: {zero_count} zero-cost items)[/dim]"
            )

        table = Table(title=title, expand=True)
        table.add_column("Service", style="cyan")
        table.add_column("Usage Type", style="dim")
        table.add_column("Usage", justify="right", style="red")
        table.add_column("Credits", justify="right", style="green")
        table.add_column("Bar (red=paid · green=credits)", ratio=1)

        # Sort by cost (highest first)
        costs.sort(key=lambda x: x[2], reverse=True)

        # Apply limit if specified
        if top > 0:
            costs = costs[:top]

        displayed_amts = [c for _, _, c in costs if show_all or abs(c) >= 0.01]
        pos_amts = [a for a in displayed_amts if a > 0.01]
        neg_amts = [a for a in displayed_amts if a < -0.01]
        max_pos = max(pos_amts, default=0.0)
        max_neg = max((abs(a) for a in neg_amts), default=0.0)

        # Add rows
        for service_name, usage_type, amount in costs:
            # Skip negligible amounts unless show_all is True (include credits / negatives)
            if abs(amount) < 0.01 and not show_all:
                continue

            u, c = _split_usage_credit(amount)
            usage_s, cred_s = _format_usage_credit_cells(amount)
            bar_cell = _service_usage_credit_bar(u, c, max_pos, max_neg)

            table.add_row(service_name, usage_type, usage_s, cred_s, bar_cell)

        if verbose:
            table.caption = (
                "Same bar style as SERVICE: [red]red[/red]=paid vs max, "
                "[green]green[/green]=credits vs max |credit|."
            )

        console.print(table)

    # If region breakdown is requested, add that too
    if show_region:
        console.print("\n[bold]Analyzing by REGION[/bold]")
        region_data = get_cost_data(
            start_date,
            end_date,
            service,
            "REGION",
            granularity,
            region,
            ce_api_dump=ce_api_dump,
            ce_api_label="detailed:region",
        )

        # Display costs for each month
        for period in region_data["ResultsByTime"]:
            # Create and display monthly table
            table = create_cost_table(
                period,
                console.width,
                "REGION",
                top,
                show_all,
                granularity,
                display_metric,
                verbose=verbose,
            )
            console.print(table)

    # Display cost breakdown insights
    console.print("\n[bold]Cost Summary by Month[/bold]")

    # Get service-level data for summary
    service_data = get_cost_data(
        start_date,
        end_date,
        service,
        "SERVICE",
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="detailed:service_summary",
    )
    rt_periods_d = rt_payload.get("ResultsByTime", [])
    monthly_totals = []
    grand_total = 0.0
    grand_usage_rt = 0.0
    grand_cred_rt = 0.0

    for i, period in enumerate(service_data["ResultsByTime"]):
        monthly_total = _period_metric_total(period, display_metric)

        period_start = period["TimePeriod"]["Start"]
        month_name = format_date_period(period_start, granularity)

        p_rt = (
            rollup_record_type_totals([rt_periods_d[i]], display_metric)
            if i < len(rt_periods_d)
            else {}
        )
        usage_rt = p_rt.get("Usage", 0.0)
        cred_rt = p_rt.get("Credit", 0.0) + p_rt.get("Refund", 0.0)

        incomplete = should_show_in_progress(period_start, granularity)
        grand_total += monthly_total
        grand_usage_rt += usage_rt
        grand_cred_rt += cred_rt
        monthly_totals.append((month_name, monthly_total, incomplete, usage_rt, cred_rt))

    # Display summary table
    summary_caption = (
        "[dim]Net = SERVICE lines; Usage / Credits = RECORD_TYPE (Billing MTD). "
        "Bar length scales to the largest month or grand total; "
        "[green]green[/green] = usage covered by credits; [red]red[/red] = out-of-pocket.[/dim]"
    )
    console.print(
        build_monthly_summary_table(
            monthly_totals,
            grand_total,
            grand_usage_rt,
            grand_cred_rt,
            console.width,
            verbose,
            summary_caption if verbose else "",
        )
    )

    if reconcile and verbose:
        print_ce_reconciliation(
            console,
            start_date,
            end_date,
            service,
            region,
            granularity,
            metric_preference,
            ce_api_dump=ce_api_dump,
        )

    if out_summary is not None:
        _fill_json_out_summary(
            out_summary, start_date, end_date, display_metric, rt_payload, cr_svc, cr_uty
        )


def get_cost_reduction_tip(service_name: str) -> str | None:
    """Get cost reduction tip for specific services."""
    tips = {
        AWSService.CLOUDWATCH.aws_name: (
            "Consider optimizing log retention, reducing alarms, or consolidating dashboards"
        ),
        AWSService.S3.aws_name: (
            "Review storage classes, lifecycle policies, and delete unnecessary objects"
        ),
        AWSService.RDS.aws_name: (
            "Consider reserved instances, stop unused instances, or optimize instance size"
        ),
        AWSService.EC2.aws_name: (
            "Use Spot/Reserved instances, right-size instances, or terminate unused resources"
        ),
        AWSService.LAMBDA.aws_name: (
            "Optimize memory allocation, reduce duration, or consolidate functions"
        ),
        AWSService.DYNAMODB.aws_name: (
            "Review provisioned capacity, use on-demand when appropriate"
        ),
        AWSService.ECR.aws_name: (
            "Clean up unused container images and review lifecycle policies"
        ),
        AWSService.ROUTE53.aws_name: "Review hosted zones and resource record sets",
        AWSService.SNS.aws_name: (
            "Review notification volume and optimize topic/subscription patterns"
        ),
        AWSService.SQS.aws_name: "Review queue usage and message volume",
        AWSService.ELB.aws_name: "Consolidate load balancers and remove unused ones",
        AWSService.EFS.aws_name: (
            "Review file system usage and move infrequently accessed data to lower-cost tiers"
        ),
        AWSService.API_GATEWAY.aws_name: ("Implement caching and review API call volume"),
    }

    # Check for exact matches
    if service_name in tips:
        return tips[service_name]

    # Check for prefix matches
    for prefix, tip in tips.items():
        if service_name.startswith(prefix):
            return tip

    return None


_RECON_METRIC_DISPLAY: dict[str, str] = {
    "UnblendedCost": "Unblended",
    "BlendedCost": "Blended",
    "NetUnblendedCost": "Net unblended",
    "AmortizedCost": "Amortized",
    "NetAmortizedCost": "Net amortized",
}


def print_ce_reconciliation(
    console: Console,
    start_date: str,
    end_date: str,
    service: str | None,
    region: str | None,
    granularity: str,
    metric_preference: str,
    ce_api_dump: list[dict] | None = None,
) -> None:
    """
    Show official CE ``Total`` lines (no GROUP BY) and a ``RECORD_TYPE`` breakdown.

    AWS does not publish a separate public API for the Billing console "Cost summary" card or
    finalized invoices; Cost Explorer is the supported usage/cost API. Invoice-style detail is
    available via Cost & Usage Reports (S3) for accounts that enable CUR.
    """
    console.print("\n[bold]Reconciliation — Cost Explorer API only[/bold]")
    console.print(
        '[dim]There is no separate boto3 "billing dashboard" total. These rows are still '
        "``ce:GetCostAndUsage``. Enable Cost & Usage Reports to S3 for invoice/CUR-aligned "
        "data.[/dim]"
    )

    totals_payload = get_cost_data(
        start_date,
        end_date,
        service,
        None,
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="reconcile:ungrouped_total",
    )
    periods = totals_payload.get("ResultsByTime") or []
    if not periods:
        console.print("[yellow]No ungrouped Cost Explorer data for this range.[/yellow]")
        return

    rt_payload = get_cost_data(
        start_date,
        end_date,
        service,
        "RECORD_TYPE",
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="reconcile:record_type",
    )
    rt_periods = rt_payload.get("ResultsByTime", [])
    dm_rt = resolve_effective_metric(rt_periods, metric_preference)
    dm_ungrouped = resolve_effective_metric(periods, metric_preference)
    dm_label = _RECON_METRIC_DISPLAY.get(dm_ungrouped, dm_ungrouped)

    cred_by_period_start: dict[str, float] = {}
    usage_by_period_start: dict[str, float] = {}
    for period in rt_periods:
        ts = period["TimePeriod"]["Start"]
        p_rt = rollup_record_type_totals([period], dm_rt)
        cred_by_period_start[ts] = float(p_rt.get("Credit", 0.0)) + float(
            p_rt.get("Refund", 0.0)
        )
        usage_by_period_start[ts] = float(p_rt.get("Usage", 0.0))

    t_api = Table(
        title=f"Ungrouped Cost Explorer (net = API Total, {dm_label} — same as this run)",
        expand=True,
    )
    t_api.add_column("Period", style="cyan", no_wrap=True)
    t_api.add_column(f"Net ({dm_label})", justify="right")
    t_api.add_column("Usage", justify="right", style="red")
    t_api.add_column("Credits", justify="right", style="green")
    t_api.caption = (
        f"[dim]Per-period [bold]Net[/bold] uses {dm_ungrouped!s} on the ungrouped CE response. "
        f"RECORD_TYPE [bold]Usage[/bold] / [bold]Credits[/bold] (Credit + Refund) are gross "
        f"components (often large and offsetting; net is small). Unblended $0.00 in old-style "
        f"tables is common when the account nets to ~$0; use Net and the Usage / Credits "
        f"columns.[/dim]"
    )

    for period in periods:
        ts = period["TimePeriod"]["Start"]
        net = _period_metric_total(period, dm_ungrouped)
        u = usage_by_period_start.get(ts, 0.0)
        cr = cred_by_period_start.get(ts, 0.0)
        t_api.add_row(
            format_date_period(ts, granularity),
            _rich_usd_positive_red_negative_green(net),
            _format_net_usd(u),
            _format_net_usd(cr),
        )

    console.print(t_api)

    agg: dict[str, float] = {}
    for period in rt_payload.get("ResultsByTime", []):
        for g in period.get("Groups", []):
            key = (g.get("Keys") or ["?"])[0]
            agg[key] = agg.get(key, 0.0) + _metric_amount_raw(g, dm_rt)

    console.print(f"[dim]RECORD_TYPE breakdown uses metric: {dm_rt}[/dim]")
    rt_table = Table(title="By RECORD_TYPE (aggregated)", expand=True)
    rt_table.add_column("RECORD_TYPE", style="cyan")
    rt_table.add_column("Amount", justify="right")
    for k in sorted(agg.keys(), key=lambda x: abs(agg[x]), reverse=True):
        rt_table.add_row(k, _rich_usd_record_type_row(k, agg[k]))
    total_rt = float(sum(agg.values()))
    rt_table.add_row("[bold]Total[/bold]", _rich_usd_signed_bold(total_rt))
    console.print(rt_table)


def split_by_linked_account(payload: dict) -> dict[str, dict]:
    """
    Split a ``GetCostAndUsage`` payload whose first group key is ``LINKED_ACCOUNT``.

    Each account's payload looks like the second group key was the only ``GroupBy``
    (``SERVICE`` or ``RECORD_TYPE``). Every account gets every time period, with empty
    groups when that account had no rows in the period.
    """
    periods = payload.get("ResultsByTime") or []
    account_ids: list[str] = []
    seen: set[str] = set()
    for period in periods:
        for group in period.get("Groups") or []:
            keys = list(group.get("Keys") or [])
            if len(keys) < 2 or not keys[0] or keys[0] in seen:
                continue
            seen.add(keys[0])
            account_ids.append(keys[0])
    if not account_ids:
        return {}

    out: dict[str, dict] = {account_id: {"ResultsByTime": []} for account_id in account_ids}
    for period in periods:
        buckets: dict[str, list] = {account_id: [] for account_id in account_ids}
        for group in period.get("Groups") or []:
            keys = list(group.get("Keys") or [])
            if len(keys) < 2 or keys[0] not in buckets:
                continue
            cloned = dict(group)
            cloned["Keys"] = keys[1:]
            buckets[keys[0]].append(cloned)
        for account_id in account_ids:
            new_period = {key: value for key, value in period.items() if key != "Groups"}
            new_period["Groups"] = buckets[account_id]
            out[account_id]["ResultsByTime"].append(new_period)
    return out


def _sum_metric_into(dest: dict, src: dict) -> None:
    """Add Cost Explorer metric amounts from ``src`` into ``dest``."""
    for name, block in src.items():
        if not isinstance(block, dict):
            continue
        raw = block.get("Amount")
        if raw in (None, ""):
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        unit = block.get("Unit") or "USD"
        prev = dest.get(name) or {}
        prev_raw = prev.get("Amount")
        if prev_raw not in (None, ""):
            val += float(prev_raw)
        dest[name] = {"Amount": str(val), "Unit": unit}


def collapse_linked_account_groups(payload: dict) -> dict:
    """Drop the ``LINKED_ACCOUNT`` key and sum the remaining group (family totals)."""
    merged_periods: list[dict] = []
    for period in payload.get("ResultsByTime") or []:
        by_key: dict[tuple, dict] = {}
        order: list[tuple] = []
        for group in period.get("Groups") or []:
            keys = list(group.get("Keys") or [])
            rest = tuple(keys[1:] if len(keys) >= 2 else keys)
            if rest not in by_key:
                order.append(rest)
                by_key[rest] = {"Keys": list(rest), "Metrics": {}}
            _sum_metric_into(by_key[rest]["Metrics"], group.get("Metrics") or {})
        new_period = {key: value for key, value in period.items() if key != "Groups"}
        new_period["Groups"] = [by_key[key] for key in order]
        merged_periods.append(new_period)
    return {"ResultsByTime": merged_periods}


def lookup_organization_account_names(account_ids: list[str]) -> dict[str, str]:
    """
    Map account IDs to Organizations account names.

    Returns whatever names ``ListAccounts`` allows. A member identity often cannot list
    the organization; callers then show the account ID alone.
    """
    wanted = {account_id for account_id in account_ids if account_id}
    names: dict[str, str] = {}
    if not wanted:
        return names
    try:
        org = boto3.client("organizations")
        token: str | None = None
        while True:
            kwargs: dict[str, Any] = {}
            if token:
                kwargs["NextToken"] = token
            page = org.list_accounts(**kwargs)
            for acc in page.get("Accounts") or []:
                aid = str(acc.get("Id") or "")
                name = str(acc.get("Name") or "")
                if aid in wanted and name:
                    names[aid] = name
            token = page.get("NextToken")
            if not token or wanted <= names.keys():
                break
    except Exception:
        return names
    return names


def format_linked_account_label(account_id: str, names: dict[str, str]) -> str:
    """``Name (123456789012)`` when the name is known, otherwise the account ID."""
    name = names.get(account_id)
    if name:
        return f"{name} ({account_id})"
    return account_id


def _account_ids_in_order(*mappings: dict[str, dict]) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for mapping in mappings:
        for account_id in mapping:
            if account_id and account_id not in seen:
                seen.add(account_id)
                ids.append(account_id)
    return ids


def _payload_metric_sum(payload: dict, metric: str) -> float:
    return sum(
        _period_metric_total(period, metric) for period in payload.get("ResultsByTime") or []
    )


def _present_simple_account(
    console: Console,
    *,
    cost_data: dict,
    usage_svc_data: dict,
    credit_svc_data: dict,
    rt_payload: dict,
    display_metric: str,
    start_date: str,
    end_date: str,
    top: int,
    show_all: bool,
    granularity: str,
    verbose: bool,
    cost_filter_min: float,
    out_summary: dict | None,
    print_credits: bool,
    print_table_note: bool,
    account_label: str | None = None,
    green_style: str = "green",
    red_style: str = "red",
) -> tuple[float, float, float]:
    """Print one account's simple report. Returns ``(net, usage, credits)``."""
    rt_periods_list = rt_payload.get("ResultsByTime", [])
    rt_by_type = rollup_record_type_totals(rt_periods_list, display_metric)

    net_roll, charges_roll, credits_roll = rollup_net_charges_credits(
        cost_data.get("ResultsByTime") or [], display_metric
    )
    console.print(
        "[dim]Rollup (by SERVICE rows):[/dim] "
        f"net {_rich_usd_positive_red_negative_green(net_roll)} "
        f"[dim]• gross charges[/dim] [red]{_format_net_usd(charges_roll)}[/red] "
        f"[dim]• credits/refunds[/dim] [green]{_format_net_usd(credits_roll)}[/green]"
    )
    rt_parts = []
    for key in ("Usage", "Credit", "Refund", "Tax", "Distributor Discount"):
        if key in rt_by_type and abs(rt_by_type[key]) >= 0.005:
            rt_parts.append(f"{key} {_rich_usd_record_type_row(key, rt_by_type[key])}")
    if rt_parts:
        console.print(
            '[dim]RECORD_TYPE (Billing home / CE "Usage vs credits" style): '
            + " • ".join(rt_parts)
            + "[/dim]"
        )
    if print_table_note:
        console.print(
            "[dim]Monthly service tables use RECORD_TYPE filters (Usage vs Credit+Refund per "
            "service) so column totals align with Usage / Credits in the summary.[/dim]"
        )
    if print_credits:
        print_amazon_credits_section(console, start_date, end_date, out_summary=out_summary)

    grand_total = 0.0
    grand_usage_rt = 0.0
    grand_cred_rt = 0.0
    monthly_totals = []

    cost_periods = cost_data.get("ResultsByTime") or []
    usage_periods = usage_svc_data.get("ResultsByTime", [])
    credit_periods = credit_svc_data.get("ResultsByTime", [])
    for i, period in enumerate(cost_periods):
        monthly_total = _period_metric_total(period, display_metric)

        p_rt = (
            rollup_record_type_totals([rt_periods_list[i]], display_metric)
            if i < len(rt_periods_list)
            else {}
        )
        usage_rt = p_rt.get("Usage", 0.0)
        cred_rt = p_rt.get("Credit", 0.0) + p_rt.get("Refund", 0.0)

        pu = _find_matching_period(usage_periods, period)
        pc = _find_matching_period(credit_periods, period)
        table = create_service_record_type_split_table(
            pu,
            pc,
            console.width,
            top,
            show_all,
            granularity,
            display_metric,
            record_type_for_period=p_rt,
            verbose=verbose,
            cost_filter_min=cost_filter_min,
            account_label=account_label,
            green_style=green_style,
            red_style=red_style,
        )
        console.print(table)

        period_start = period["TimePeriod"]["Start"]
        month_name = format_date_period(period_start, granularity)

        incomplete = should_show_in_progress(period_start, granularity)
        grand_total += monthly_total
        grand_usage_rt += usage_rt
        grand_cred_rt += cred_rt
        monthly_totals.append((month_name, monthly_total, incomplete, usage_rt, cred_rt))

    summary_caption = (
        "[dim]Net = sum of net SERVICE lines (usage and credits merged per service). "
        "Usage / Credits = Cost Explorer RECORD_TYPE (Billing MTD). "
        "Monthly service tables above match these columns (gross per service). "
        "Bar length scales to the largest month or grand total; "
        "[green]green[/green] = usage covered by credits; [red]red[/red] = out-of-pocket.[/dim]"
    )
    summary_title = "Monthly Summary"
    if account_label:
        summary_title = f"Monthly Summary · {escape(account_label)}"
    console.print(
        build_monthly_summary_table(
            monthly_totals,
            grand_total,
            grand_usage_rt,
            grand_cred_rt,
            console.width,
            verbose,
            summary_caption if verbose else "",
            title=summary_title,
            green_style=green_style,
            red_style=red_style,
            account_label=account_label or "",
        )
    )

    u_rt = float(rt_by_type.get("Usage", 0.0))
    item_usage: dict[str, float] = {}
    for period in usage_periods:
        for group in period.get("Groups", []):
            item_name = group["Keys"][0]
            amount = _metric_amount(group, display_metric)
            item_usage[item_name] = item_usage.get(item_name, 0.0) + amount

    insight_denom = u_rt if u_rt > 0.01 else max(sum(item_usage.values()), 0.01)
    sorted_pos = sorted(
        [(k, v) for k, v in item_usage.items() if v > 0.01],
        key=lambda x: x[1],
        reverse=True,
    )
    top_items = sorted_pos[:5]

    if insight_denom > 0.01 and top_items:
        console.print("\n[bold]Cost Breakdown Insights:[/bold]")
        denom_s = _format_net_usd(insight_denom)
        console.print(
            f"[dim]Percentages are of RECORD_TYPE Usage over the range (~{denom_s}).[/dim]"
        )

        pct_basis = "RECORD_TYPE Usage"
        for item_name, amount in top_items:
            percentage = (amount / insight_denom) * 100
            console.print(
                f"• [cyan]{item_name}[/cyan]: [red]${amount:.2f}[/red] "
                f"([bold]{percentage:.1f}%[/bold] of {pct_basis})"
            )

            tip = get_cost_reduction_tip(item_name)
            if tip:
                console.print(f"  [yellow]Tip:[/yellow] {tip}")

    return grand_total, grand_usage_rt, grand_cred_rt


def _service_maps_to_period(canonical: dict, maps: list[dict[str, float]], metric: str) -> dict:
    """One CE period whose groups are service totals summed across ``maps``."""
    totals: dict[str, float] = {}
    for amounts in maps:
        for name, amount in amounts.items():
            totals[name] = totals.get(name, 0.0) + amount
    groups = [
        {"Keys": [name], "Metrics": {metric: {"Amount": str(amount), "Unit": "USD"}}}
        for name, amount in totals.items()
    ]
    return {"TimePeriod": canonical["TimePeriod"], "Groups": groups}


def _print_combined_multi_account_tables(
    console: Console,
    account_ids: list[str],
    by_cost: dict[str, dict],
    by_usage: dict[str, dict],
    by_credit: dict[str, dict],
    by_rt: dict[str, dict],
    display_metric: str,
    top: int,
    show_all: bool,
    granularity: str,
    verbose: bool,
    cost_filter_min: float,
    color_legend: str,
    account_labels: list[str] | None = None,
) -> None:
    """
    Service tables and a monthly summary for every linked account at once.

    Each service bar and each month bar stacks that account's green and red.
    """
    anchor: list[dict] = []
    for account_id in account_ids:
        periods = (by_cost.get(account_id) or {}).get("ResultsByTime") or []
        if periods:
            anchor = periods
            break
    if not anchor:
        return

    console.print()
    console.print(Rule("[bold]All accounts[/bold]"))
    console.print(
        "[dim]Each service and each month stacks every account's green (credits cover usage) "
        "and red (out-of-pocket). Hold Option on a bar section to see that account and its "
        "cost (usage and credits).[/dim]"
    )
    if color_legend:
        console.print(color_legend)

    monthly_totals: list[tuple] = []
    account_bars: list[list[tuple]] = []
    grand_total = 0.0
    grand_usage = 0.0
    grand_cred = 0.0
    labels = list(account_labels) if account_labels is not None else list(account_ids)

    for period in anchor:
        per_maps: list[tuple[dict[str, float], dict[str, float], str, str, str]] = []
        month_slices: list[tuple] = []
        rt_sum: dict[str, float] = {}
        net = 0.0
        for index, account_id in enumerate(account_ids):
            green_style, red_style = account_color_pair(index)
            label = labels[index] if index < len(labels) else account_id
            cost_periods = (by_cost.get(account_id) or {}).get("ResultsByTime") or []
            usage_periods = (by_usage.get(account_id) or {}).get("ResultsByTime") or []
            credit_periods = (by_credit.get(account_id) or {}).get("ResultsByTime") or []
            rt_periods = (by_rt.get(account_id) or {}).get("ResultsByTime") or []
            usage_map = _period_service_amount_map(
                _find_matching_period(usage_periods, period), display_metric
            )
            credit_map = _period_service_amount_map(
                _find_matching_period(credit_periods, period), display_metric
            )
            per_maps.append((usage_map, credit_map, green_style, red_style, label))
            matched_rt = _find_matching_period(rt_periods, period)
            p_rt = rollup_record_type_totals([matched_rt], display_metric)
            for key, amount in p_rt.items():
                rt_sum[key] = rt_sum.get(key, 0.0) + amount
            usage_rt = p_rt.get("Usage", 0.0)
            cred_rt = p_rt.get("Credit", 0.0) + p_rt.get("Refund", 0.0)
            month_slices.append((usage_rt, cred_rt, green_style, red_style, label))
            net += _period_metric_total(
                _find_matching_period(cost_periods, period), display_metric
            )

        service_names: set[str] = set()
        for usage_map, credit_map, _green, _red, _label in per_maps:
            service_names |= set(usage_map) | set(credit_map)
        service_account_slices = {
            name: [
                (
                    usage_map.get(name, 0.0),
                    credit_map.get(name, 0.0),
                    green_style,
                    red_style,
                    label,
                )
                for usage_map, credit_map, green_style, red_style, label in per_maps
            ]
            for name in service_names
        }
        merged_usage = _service_maps_to_period(
            period, [usage_map for usage_map, _, _, _, _ in per_maps], display_metric
        )
        merged_credit = _service_maps_to_period(
            period, [credit_map for _, credit_map, _, _, _ in per_maps], display_metric
        )
        console.print(
            create_service_record_type_split_table(
                merged_usage,
                merged_credit,
                console.width,
                top,
                show_all,
                granularity,
                display_metric,
                record_type_for_period=rt_sum,
                verbose=verbose,
                cost_filter_min=cost_filter_min,
                account_label="all accounts",
                service_account_slices=service_account_slices,
            )
        )

        usage_rt = rt_sum.get("Usage", 0.0)
        cred_rt = rt_sum.get("Credit", 0.0) + rt_sum.get("Refund", 0.0)
        period_start = period["TimePeriod"]["Start"]
        incomplete = should_show_in_progress(period_start, granularity)
        grand_total += net
        grand_usage += usage_rt
        grand_cred += cred_rt
        monthly_totals.append(
            (
                format_date_period(period_start, granularity),
                net,
                incomplete,
                usage_rt,
                cred_rt,
            )
        )
        account_bars.append(month_slices)

    summary_caption = (
        "[dim]Net = sum of net SERVICE lines across accounts. "
        "Usage / Credits = Cost Explorer RECORD_TYPE (Billing MTD). "
        "Each month bar stacks every account's green and red.[/dim]"
    )
    console.print(
        build_monthly_summary_table(
            monthly_totals,
            grand_total,
            grand_usage,
            grand_cred,
            console.width,
            verbose,
            summary_caption if verbose else "",
            title="Monthly Summary · all accounts",
            account_bars=account_bars,
            color_legend=color_legend,
        )
    )


def analyze_costs_simple(
    start_date: str,
    end_date: str,
    service: str | None,
    top: int = 0,
    show_all: bool = False,
    granularity: str = "MONTHLY",
    region: str | None = None,
    metric_preference: str = "auto",
    reconcile: bool = True,
    verbose: bool = False,
    ce_api_dump: list[dict] | None = None,
    out_summary: dict | None = None,
    cost_filter_min: float = 0.0,
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

    if region:
        title += f" in {region}"

    console.print(
        Panel(
            f"{get_account_header_markup()}\n[bold]{title}[/bold]\n{start_date} to {end_date}"
        )
    )

    # Group by linked account first so one set of calls can be split per account.
    # A single visible account still renders the same report as before.
    cost_data = get_cost_data(
        start_date,
        end_date,
        service,
        ["LINKED_ACCOUNT", "SERVICE"],
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="simple:service",
    )
    usage_svc_data = get_cost_data(
        start_date,
        end_date,
        service,
        ["LINKED_ACCOUNT", "SERVICE"],
        granularity,
        region,
        record_type_values=["Usage"],
        ce_api_dump=ce_api_dump,
        ce_api_label="simple:service+record_usage",
    )
    credit_svc_data = get_cost_data(
        start_date,
        end_date,
        service,
        ["LINKED_ACCOUNT", "SERVICE"],
        granularity,
        region,
        record_type_values=["Credit", "Refund"],
        ce_api_dump=ce_api_dump,
        ce_api_label="simple:service+record_credit",
    )

    # Check if we got any data
    has_data = False
    for period in cost_data.get("ResultsByTime", []):
        if period.get("Groups"):
            has_data = True
            break

    if not has_data:
        if out_summary is not None:
            out_summary["status"] = "no_cost_data"
        console.print(
            "[bold yellow]No cost data found for the specified parameters.[/bold yellow]"
        )
        return

    display_metric = resolve_effective_metric(cost_data["ResultsByTime"], metric_preference)
    if metric_preference == "auto":
        console.print(
            f"[dim]Using Cost Explorer metric: {display_metric} "
            f"(auto picks the largest |total| across bundled CE metrics)[/dim]"
        )

    rt_payload = get_cost_data(
        start_date,
        end_date,
        service,
        ["LINKED_ACCOUNT", "RECORD_TYPE"],
        granularity,
        region,
        ce_api_dump=ce_api_dump,
        ce_api_label="simple:record_type",
    )
    family_rt = collapse_linked_account_groups(rt_payload)
    rt_by_type = rollup_record_type_totals(family_rt["ResultsByTime"], display_metric)

    cr_svc: dict[str, float] = {}
    cr_uty: dict[str, float] = {}
    if ce_api_dump is not None:
        cr_svc, cr_uty = _append_credit_attribution_dumps(
            start_date,
            end_date,
            service,
            region,
            granularity,
            display_metric,
            float(rt_by_type.get("Credit", 0.0)),
            ce_api_dump,
        )
        if out_summary is not None and (
            cr_svc
            or cr_uty
            or abs(float(rt_by_type.get("Credit", 0.0))) >= 0.005
        ):
            console.print(
                "[dim]JSON dump: added credit:by_service and credit:by_usage_type "
                "(RECORD_TYPE=Credit).[/dim]"
            )

    by_cost = split_by_linked_account(cost_data)
    by_usage = split_by_linked_account(usage_svc_data)
    by_credit = split_by_linked_account(credit_svc_data)
    by_rt = split_by_linked_account(rt_payload)
    account_ids = _account_ids_in_order(by_cost, by_usage, by_credit, by_rt)
    if not account_ids:
        by_cost = {"": cost_data}
        by_usage = {"": usage_svc_data}
        by_credit = {"": credit_svc_data}
        by_rt = {"": family_rt}
        account_ids = [""]

    def _usage_sort_key(account_id: str) -> tuple[float, str]:
        payload = by_usage.get(account_id) or {"ResultsByTime": []}
        return (-_payload_metric_sum(payload, display_metric), account_id)

    account_ids.sort(key=_usage_sort_key)
    multi = len(account_ids) > 1 and all(account_ids)
    names = lookup_organization_account_names(account_ids) if multi else {}

    present_kwargs = {
        "cost_data": cost_data,
        "usage_svc_data": usage_svc_data,
        "credit_svc_data": credit_svc_data,
        "rt_payload": family_rt,
        "display_metric": display_metric,
        "start_date": start_date,
        "end_date": end_date,
        "top": top,
        "show_all": show_all,
        "granularity": granularity,
        "verbose": verbose,
        "cost_filter_min": cost_filter_min,
        "out_summary": out_summary,
        "print_credits": True,
        "print_table_note": True,
    }

    slices: list[AccountGrandSlice] = []
    if not multi:
        aid = account_ids[0]
        present_kwargs["cost_data"] = by_cost.get(aid) or {"ResultsByTime": []}
        present_kwargs["usage_svc_data"] = by_usage.get(aid) or {"ResultsByTime": []}
        present_kwargs["credit_svc_data"] = by_credit.get(aid) or {"ResultsByTime": []}
        present_kwargs["rt_payload"] = by_rt.get(aid) or family_rt
        _present_simple_account(console, **present_kwargs)
    else:
        swatches = []
        for index, aid in enumerate(account_ids):
            green_style, red_style = account_color_pair(index)
            label = format_linked_account_label(aid, names)
            swatches.append(f"[{green_style}]█[/][{red_style}]█[/] {escape(label)}")
        console.print("[bold]Accounts included:[/bold] " + " · ".join(swatches))
        color_legend = "[dim]Accounts: " + " · ".join(swatches) + ".[/dim]"
        console.print(
            "[dim]Each account below is its own report. Its service and month bars use that "
            "account's green and red. The all-accounts tables stack those colors on each "
            "service and each month.[/dim]"
        )
        console.print(
            "[dim]Monthly service tables use RECORD_TYPE filters (Usage vs Credit+Refund per "
            "service) so column totals align with Usage / Credits in the summary.[/dim]"
        )
        print_amazon_credits_section(console, start_date, end_date, out_summary=out_summary)
        present_kwargs["print_credits"] = False
        present_kwargs["print_table_note"] = False
        present_kwargs["out_summary"] = None
        for index, aid in enumerate(account_ids):
            green_style, red_style = account_color_pair(index)
            label = format_linked_account_label(aid, names)
            console.print()
            console.print(
                Rule(
                    f"[{green_style}]█[/][{red_style}]█[/] [bold]{escape(label)}[/bold]"
                )
            )
            present_kwargs["cost_data"] = by_cost.get(aid) or {"ResultsByTime": []}
            present_kwargs["usage_svc_data"] = by_usage.get(aid) or {"ResultsByTime": []}
            present_kwargs["credit_svc_data"] = by_credit.get(aid) or {"ResultsByTime": []}
            present_kwargs["rt_payload"] = by_rt.get(aid) or {"ResultsByTime": []}
            present_kwargs["account_label"] = label
            present_kwargs["green_style"] = green_style
            present_kwargs["red_style"] = red_style
            net, usage_rt, cred_rt = _present_simple_account(console, **present_kwargs)
            slices.append(
                AccountGrandSlice(
                    account_id=aid,
                    label=label,
                    net=net,
                    usage=usage_rt,
                    credit=cred_rt,
                    green_style=green_style,
                    red_style=red_style,
                )
            )
        _print_combined_multi_account_tables(
            console,
            account_ids,
            by_cost,
            by_usage,
            by_credit,
            by_rt,
            display_metric,
            top,
            show_all,
            granularity,
            verbose,
            cost_filter_min,
            color_legend,
            [format_linked_account_label(aid, names) for aid in account_ids],
        )
        console.print()
        console.print(
            build_accounts_grand_total_table(slices, console.width, start_date, end_date)
        )

    if reconcile and verbose:
        print_ce_reconciliation(
            console,
            start_date,
            end_date,
            service,
            region,
            granularity,
            metric_preference,
            ce_api_dump=ce_api_dump,
        )

    if out_summary is not None:
        _fill_json_out_summary(
            out_summary, start_date, end_date, display_metric, family_rt, cr_svc, cr_uty
        )
        if slices:
            out_summary["linked_accounts"] = [
                {
                    "account_id": sl.account_id,
                    "label": sl.label,
                    "net": sl.net,
                    "usage": sl.usage,
                    "credits": sl.credit,
                }
                for sl in slices
            ]
