"""
Rich text bars and tables for monthly cost summaries (usage vs credits, scaling, green/red split).
"""

from __future__ import annotations

from rich.table import Table
from rich.text import Text


def _format_net_usd(value: float) -> str:
    """Format a net dollar amount; near-zero floats print as $0.00."""
    if abs(value) < 0.005:
        return "$0.00"
    return f"${value:.2f}"


def _rich_usd_positive_red_negative_green(value: float) -> str:
    """Rich markup: charges / net spend positive → red; credits / net negative → green."""
    s = _format_net_usd(value)
    if abs(value) < 0.005:
        return f"[dim]{s}[/dim]"
    if value > 0:
        return f"[red]{s}[/red]"
    return f"[green]{s}[/green]"


def _rich_usd_signed_bold(value: float) -> str:
    """Bold Total / grand-total style with the same red / green sign convention."""
    s = _format_net_usd(value)
    if abs(value) < 0.005:
        return f"[bold dim]{s}[/bold dim]"
    if value > 0:
        return f"[bold red]{s}[/bold red]"
    return f"[bold green]{s}[/bold green]"


def _rich_usd_record_type_row(record_type: str, value: float) -> str:
    """RECORD_TYPE line item: cost-like rows red; Credit / Refund rows green."""
    s = _format_net_usd(value)
    if abs(value) < 0.005:
        return f"[dim]{s}[/dim]"
    if record_type in ("Credit", "Refund"):
        return f"[green]{s}[/green]"
    return f"[red]{s}[/red]"


def _split_usage_credit(amount: float) -> tuple[float, float]:
    """Split a signed SERVICE line into positive usage vs non-positive credits (CE convention)."""
    if amount > 0.01:
        return amount, 0.0
    if amount < -0.01:
        return 0.0, amount
    return 0.0, 0.0


def _format_usage_credit_cells(amount: float) -> tuple[str, str]:
    """Two display cells: Usage (≥0) and Credits (≤0 or —)."""
    u, c = _split_usage_credit(amount)
    if abs(amount) < 0.005:
        return "$0.00", "—"
    usage_s = _format_net_usd(u) if u > 0 else "—"
    cred_s = _format_net_usd(c) if c < 0 else "—"
    return usage_s, cred_s


def _service_usage_credit_bar(
    usage: float,
    credit: float,
    max_pos: float,
    max_neg: float,
    half_chars: int = 20,
) -> Text:
    """
    One bar cell: **red** = usage (what you paid) vs table max usage; **green** =
    |credit| vs max |credit|.
    """
    t = Text()
    wrote = False
    if max_pos > 1e-12 and usage > 1e-12:
        n = max(1, min(half_chars, round((usage / max_pos) * half_chars)))
        t.append("█" * n, style="red")
        wrote = True
    if max_neg > 1e-12 and credit < -1e-12:
        n = max(1, min(half_chars, round((abs(credit) / max_neg) * half_chars)))
        if wrote:
            t.append(" ", style="dim")
        t.append("█" * n, style="green")
        wrote = True
    if not wrote:
        t.append("—", style="dim")
    return t


def _service_rec_row_magnitude(usage: float, credit: float, near: float = 0.005) -> float:
    """Scale weight for RECORD_TYPE bars: gross usage, or |credit| when usage is negligible."""
    u = float(usage)
    c = float(credit)
    credit_mag = max(0.0, -c) if c < -near else 0.0
    if u >= near:
        return u
    if credit_mag >= near:
        return credit_mag
    return 0.0


def _service_rec_coverage_bar(
    usage: float,
    credit: float,
    console_width: int,
    max_magnitude: float,
) -> Text:
    """
    One **stacked** bar per service or month row (RECORD_TYPE split / monthly summary).
    **Bar length** scales to the largest row in the same table (``max_magnitude``). Within that
    length: **green** = usage covered by credits; **red** = out-of-pocket
    (``max(0, usage + credit)``).
    """
    near = 0.005
    u = float(usage)
    c = float(credit)
    credit_mag = max(0.0, -c) if c < -near else 0.0
    oop = max(0.0, u + c)
    covered = min(u, credit_mag) if u >= near else 0.0
    mag_for_len = _service_rec_row_magnitude(u, c, near=near)

    t = Text()
    w = max(12, min(40, int(console_width / 3)))
    scale = float(max_magnitude) if max_magnitude >= near else 1.0
    row_len = max(1, min(w, int(round(w * (mag_for_len / scale)))))

    if u >= near:
        n_cov = int(round(row_len * (covered / u)))
        n_oop = row_len - n_cov
        if oop >= near and covered >= near:
            if n_cov == 0:
                n_cov = 1
                n_oop = row_len - 1
            elif n_oop == 0:
                n_oop = 1
                n_cov = row_len - 1
        elif covered < near:
            n_cov, n_oop = 0, row_len
        elif oop < near:
            n_cov, n_oop = row_len, 0
        if n_cov > 0:
            t.append("█" * n_cov, style="green")
        if n_oop > 0:
            t.append("█" * n_oop, style="red")
        return t

    if credit_mag >= near:
        t.append("█" * row_len, style="green")
        return t

    t.append("—", style="dim")
    return t


def _monthly_summary_rec_max_magnitude(
    monthly_totals: list[tuple],
    grand_usage_rt: float,
    grand_cred_rt: float,
) -> float:
    """Largest REC gross weight among month rows and grand total (same scale as service tables)."""
    mags: list[float] = []
    for row in monthly_totals:
        usage_rt, cred_rt = row[3], row[4]
        mags.append(_service_rec_row_magnitude(usage_rt, cred_rt))
    mags.append(_service_rec_row_magnitude(grand_usage_rt, grand_cred_rt))
    max_mag = max(mags, default=0.0)
    return max_mag if max_mag >= 0.005 else 1.0


def _monthly_summary_bar(total: float, max_abs: float, console_width: int) -> str:
    """Rich bar for a monthly net total; scales by magnitude so negatives do not break layout."""
    if max_abs < 1e-9:
        return ""
    max_bar_width = console_width / 2
    bar_width = max(0, round((abs(total) / max_abs) * max_bar_width))
    bar = "█" * bar_width
    pct = (abs(total) / max_abs) * 100
    if pct < 100 - 1e-9:
        return f"{bar} {pct:.1f}%"
    return f"{bar} (max)"


def build_monthly_summary_table(
    monthly_totals: list[tuple],
    grand_total: float,
    grand_usage_rt: float,
    grand_cred_rt: float,
    console_width: int,
    verbose: bool,
    verbose_caption: str = "",
) -> Table:
    """
    "Monthly Summary" table with net, RECORD_TYPE usage/credits, and stacked coverage bar column.
    """
    summary_table = Table(title="Monthly Summary", expand=True)
    summary_table.add_column("Month", style="cyan")
    summary_table.add_column("Net", justify="right")
    summary_table.add_column("Usage", justify="right", style="red")
    summary_table.add_column("Credits", justify="right", style="green")
    summary_table.add_column("Bar (green=credits cover · red=you pay)", ratio=1)

    summary_max_mag = _monthly_summary_rec_max_magnitude(
        monthly_totals, grand_usage_rt, grand_cred_rt
    )

    summary_table.add_row(
        "[bold]GRAND TOTAL[/bold]",
        _rich_usd_signed_bold(grand_total),
        f"[bold red]{_format_net_usd(grand_usage_rt)}[/bold red]",
        f"[bold green]{_format_net_usd(grand_cred_rt)}[/bold green]",
        _service_rec_coverage_bar(
            grand_usage_rt, grand_cred_rt, console_width, summary_max_mag
        ),
    )

    for month, total, incomplete, usage_rt, cred_rt in monthly_totals:
        label = f"{month} [dim](MTD)[/dim]" if incomplete else month
        bar = _service_rec_coverage_bar(usage_rt, cred_rt, console_width, summary_max_mag)
        summary_table.add_row(
            label,
            _rich_usd_positive_red_negative_green(total),
            _format_net_usd(usage_rt),
            _format_net_usd(cred_rt),
            bar,
        )

    if verbose and verbose_caption:
        summary_table.caption = verbose_caption
    return summary_table


OTHER_SERVICES_ROW_LABEL = "All other services"


def create_service_record_type_split_table(
    usage_period: dict,
    credit_period: dict,
    console_width: int,
    top: int,
    show_all: bool,
    granularity: str,
    metric: str,
    record_type_for_period: dict[str, float] | None = None,
    verbose: bool = False,
    cost_filter_min: float = 0.0,
) -> Table:
    """
    One monthly table: **Usage** from ``RECORD_TYPE=Usage`` × SERVICE; **Credits** from
    ``RECORD_TYPE=Credit`` and ``Refund`` × SERVICE. Row amounts sum (by column) to the same
    RECORD_TYPE totals as :func:`rollup_record_type_totals` for that month, apart from line items
    not allocated to SERVICE (e.g. some tax rows).

    When ``cost_filter_min`` > 0, each per-service gross weight ``max(usage, |credits|)`` is
    compared to that threshold: rows at or above it stay separate (subject to ``top``); the rest
    are summed into one **All other services** row so table totals stay complete.
    """
    from .core import (  # late import: this module is imported by core at package load
        _period_service_amount_map,
        format_date_period,
        should_show_in_progress,
    )

    usage_map = _period_service_amount_map(usage_period, metric)
    credit_map = _period_service_amount_map(credit_period, metric)

    period_start = usage_period["TimePeriod"]["Start"]
    period_display = format_date_period(period_start, granularity)

    if granularity == "DAILY":
        title_prefix = "Daily"
    elif granularity == "HOURLY":
        title_prefix = "Hourly"
    else:
        title_prefix = "Monthly"

    is_in_progress = should_show_in_progress(period_start, granularity)

    rows: list[tuple[str, float, float]] = []
    for name in sorted(set(usage_map) | set(credit_map)):
        u = float(usage_map.get(name, 0.0))
        c = float(credit_map.get(name, 0.0))
        rows.append((name, u, c))

    def _row_weight(item: tuple[str, float, float]) -> float:
        _, u, c = item
        return max(u, abs(c))

    def _row_eligible_for_display(u: float, c: float) -> bool:
        return show_all or abs(u) >= 0.01 or abs(c) >= 0.01

    rows.sort(key=_row_weight, reverse=True)

    total_count = len(rows)
    non_zero_count = sum(1 for _, u, c in rows if abs(u) >= 0.01 or abs(c) >= 0.01)
    zero_count = total_count - non_zero_count

    cf = float(cost_filter_min) if cost_filter_min and cost_filter_min > 0 else 0.0
    filter_caption = ""
    if cf > 0.0:
        major: list[tuple[str, float, float]] = []
        minor: list[tuple[str, float, float]] = []
        for name, u, c in rows:
            if not _row_eligible_for_display(u, c):
                continue
            if _row_weight((name, u, c)) >= cf:
                major.append((name, u, c))
            else:
                minor.append((name, u, c))
        if top > 0:
            shown_major = major[:top]
            overflow_major = major[top:]
        else:
            shown_major = major
            overflow_major = []
        into_other = minor + overflow_major
        agg_u = sum(x[1] for x in into_other)
        agg_c = sum(x[2] for x in into_other)
        rows = list(shown_major)
        if into_other:
            rows.append((OTHER_SERVICES_ROW_LABEL, agg_u, agg_c))
        filter_caption = (
            f"[dim]Per-service lines shown when max(Usage, |Credits|) ≥ {_format_net_usd(cf)}; "
            f"smaller lines are combined into '{OTHER_SERVICES_ROW_LABEL}'.[/dim]"
        )
    elif top > 0:
        rows = rows[:top]

    if show_all or zero_count == 0:
        title_core = f"{title_prefix} {period_display} Costs"
    else:
        title_core = (
            f"{title_prefix} {period_display} Costs "
            f"[dim]• Showing {non_zero_count} of {total_count} services "
            f"(hidden: {zero_count} near-zero gross lines)[/dim]"
        )

    if is_in_progress:
        title = f"{title_core} [yellow](In Progress)[/yellow]"
    else:
        title = title_core

    title += " [dim]· by service (RECORD_TYPE Usage / Credits)[/dim]"
    if filter_caption:
        title = f"{title}\n{filter_caption}"

    table = Table(title=title, expand=True)
    table.add_column("Service", style="cyan")
    table.add_column("Usage", justify="right", style="red")
    table.add_column("Credits", justify="right", style="green")
    table.add_column("Bar (green=credits cover · red=you pay)", ratio=1)

    visible = [
        (n, u, c)
        for n, u, c in rows
        if show_all or abs(u) >= 0.01 or abs(c) >= 0.01 or n == OTHER_SERVICES_ROW_LABEL
    ]
    max_mag = max(
        (_service_rec_row_magnitude(u, c) for _, u, c in visible),
        default=0.0,
    )
    if max_mag < 0.005:
        max_mag = 1.0

    for name, u, c in rows:
        if (
            not show_all
            and abs(u) < 0.01
            and abs(c) < 0.01
            and name != OTHER_SERVICES_ROW_LABEL
        ):
            continue
        usage_s = _format_net_usd(u) if u >= 0.005 else "—"
        cred_s = _format_net_usd(c) if c <= -0.005 else "—"
        bar_cell = _service_rec_coverage_bar(u, c, console_width, max_mag)
        table.add_row(name, usage_s, cred_s, bar_cell)

    if show_all and zero_count > 0:
        table.caption = f"Showing all services including {zero_count} with near-zero Usage and Credits"

    if verbose:
        cap_bits = [
            "Columns: [red]Usage[/red] = SERVICE × RECORD_TYPE Usage; "
            "[green]Credits[/green] = SERVICE × (Credit + Refund). "
            "Bar: length scales to the largest service/credit line in the table; "
            "[green]green[/green] = usage covered by credits; [red]red[/red] = out-of-pocket."
        ]
        if record_type_for_period:
            u = record_type_for_period.get("Usage")
            cr = record_type_for_period.get("Credit", 0.0) + record_type_for_period.get(
                "Refund", 0.0
            )
            if u is not None and abs(float(u)) >= 0.005:
                cap_bits.append(
                    "Period RECORD_TYPE totals: Usage "
                    f"[red]{_format_net_usd(float(u))}[/red] • Credits/refunds "
                    f"[green]{_format_net_usd(float(cr))}[/green]."
                )
        join = " ".join(cap_bits)
        table.caption = f"{table.caption}\n{join}" if table.caption else join

    if is_in_progress:
        note = "[yellow]Note: This month is still in progress. Data may be incomplete.[/yellow]"
        table.caption = f"{table.caption}\n{note}" if table.caption else note

    return table
