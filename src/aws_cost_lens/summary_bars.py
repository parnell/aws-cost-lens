"""
Rich text bars and tables for monthly cost summaries (usage vs credits, scaling, green/red split).
"""

from __future__ import annotations

from typing import NamedTuple

from rich.markup import escape
from rich.style import Style
from rich.table import Table
from rich.text import Text

# One pair per linked account: green = usage covered by credits, red = out-of-pocket.
# The first pair matches the single-account bars. Later pairs stay in the green and red
# families so a grand total can show which account each segment belongs to.
ACCOUNT_COLOR_PAIRS: tuple[tuple[str, str], ...] = (
    ("green", "red"),
    ("bright_green", "bright_red"),
    ("dark_green", "dark_red"),
    ("spring_green3", "indian_red"),
    ("sea_green3", "red3"),
    ("chartreuse3", "orange_red1"),
    ("green3", "indian_red1"),
    ("green1", "red1"),
)


class AccountGrandSlice(NamedTuple):
    """One linked account's totals for the cross-account grand total table."""

    account_id: str
    label: str
    net: float
    usage: float
    credit: float
    green_style: str
    red_style: str


def account_color_pair(index: int) -> tuple[str, str]:
    """Green and red styles for linked account ``index`` (0 = the default pair)."""
    n = len(ACCOUNT_COLOR_PAIRS)
    if index < n:
        return ACCOUNT_COLOR_PAIRS[index]
    step = index - n
    g = 90 + (step * 47) % 150
    r = 90 + (step * 53) % 150
    return (f"rgb(30,{g},70)", f"rgb({r},35,45)")


def _format_net_usd(value: float) -> str:
    """Format a net dollar amount; near-zero floats print as $0.00."""
    if abs(value) < 0.005:
        return "$0.00"
    return f"${value:.2f}"


# iTerm2 (also Ghostty, Kitty, WezTerm) shows an OSC 8 target when you hold Option.
# A dummy ``u:`` scheme is required; bare text is not a URL, so the balloon stays empty.
# Spaces become hyphens and fields are comma-separated so the balloon stays readable.
_HOVER_SCHEME = "u:"
_HOVER_SEP = ","
_HOVER_GAP = "-"
_HOVER_KEEP = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:$+!*'()@/"
)

# (usage, credit, green style, red style) plus an optional account label for the hover.
CoverageSlice = tuple[float, float, str, str] | tuple[float, float, str, str, str]


def _hover_token(detail: str) -> str:
    """One URI-safe tooltip field: spaces and other junk become hyphens."""
    cleaned = (
        detail.replace("\x1b", " ")
        .replace("\x07", " ")
        .replace("\n", " ")
        .replace("\r", " ")
        .strip()
    )
    out: list[str] = []
    for ch in cleaned:
        if ch in _HOVER_KEEP:
            out.append(ch)
        elif out and out[-1] != _HOVER_GAP:
            out.append(_HOVER_GAP)
    return "".join(out).strip(_HOVER_GAP)


def _hover_url(*parts: str) -> str:
    """Dummy-scheme URL so Option-hover shows ``parts`` in the balloon."""
    tokens = [token for part in parts if (token := _hover_token(part))]
    return f"{_HOVER_SCHEME}{_HOVER_SEP.join(tokens)}"


def _account_section_hover_url(label: str, usage: float, credit: float) -> str | None:
    """Tooltip for one account's bar section: account, and usage + credits."""
    if not label.strip():
        return None
    return _hover_url(
        f"account:{label}",
        f"cost (usage and credits):{_format_net_usd(usage + credit)}",
    )


def _iter_slice_fields(item: tuple) -> tuple[float, float, str, str, str]:
    """Read a coverage slice. The account label is optional."""
    label = str(item[4]) if len(item) > 4 and item[4] else ""
    return float(item[0]), float(item[1]), str(item[2]), str(item[3]), label


def _append_block(text: Text, count: int, color: str, link: str | None) -> None:
    """Append ``count`` bar glyphs. ``link`` is the Option-hover target."""
    if count <= 0:
        return
    if link:
        text.append("█" * count, style=Style(color=color, link=link))
    else:
        text.append("█" * count, style=color)


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
    green_style: str = "green",
    red_style: str = "red",
    account_label: str = "",
) -> Text:
    """
    One **stacked** bar per service or month row (RECORD_TYPE split / monthly summary).
    **Bar length** scales to the largest row in the same table (``max_magnitude``). Within that
    length: **green** = usage covered by credits; **red** = out-of-pocket
    (``max(0, usage + credit)``). ``green_style`` / ``red_style`` pick the shades.

    When ``account_label`` is set, holding Option on the bar shows that account and the
    row's cost (usage + credits).
    """
    near = 0.005
    u = float(usage)
    c = float(credit)
    credit_mag = max(0.0, -c) if c < -near else 0.0
    oop = max(0.0, u + c)
    covered = min(u, credit_mag) if u >= near else 0.0
    mag_for_len = _service_rec_row_magnitude(u, c, near=near)
    link = _account_section_hover_url(account_label, u, c)

    t = Text()
    w = max(12, min(40, int(console_width / 3)))
    scale = float(max_magnitude) if max_magnitude >= near else 1.0
    row_len = max(1, min(w, round(w * (mag_for_len / scale))))

    if u >= near:
        n_cov = round(row_len * (covered / u))
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
        _append_block(t, n_cov, green_style, link)
        _append_block(t, n_oop, red_style, link)
        return t

    if credit_mag >= near:
        _append_block(t, row_len, green_style, link)
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


def _coverage_styled_parts(
    usage: float,
    credit: float,
    green_style: str,
    red_style: str,
    label: str = "",
) -> list[tuple[float, str, str | None]]:
    """Dollar segments: green (covered or credit-only), then red (out-of-pocket).

    Each segment carries the same Option-hover link: this account and its cost
    (usage + credits).
    """
    near = 0.005
    u = float(usage)
    c = float(credit)
    credit_mag = max(0.0, -c) if c < -near else 0.0
    oop = max(0.0, u + c)
    covered = min(u, credit_mag) if u >= near else 0.0
    link = _account_section_hover_url(label, u, c)
    parts: list[tuple[float, str, str | None]] = []
    if u >= near:
        if covered >= near:
            parts.append((covered, green_style, link))
        if oop >= near:
            parts.append((oop, red_style, link))
        return parts
    if credit_mag >= near:
        parts.append((credit_mag, green_style, link))
    return parts


def _allocate_bar_counts(total: int, weights: list[float]) -> list[int]:
    """Split ``total`` characters across ``weights``. Positive weights get a block when it fits."""
    if total <= 0 or not weights:
        return [0] * len(weights)
    weight_sum = sum(weights)
    if weight_sum <= 0:
        return [0] * len(weights)
    raw = [total * (w / weight_sum) for w in weights]
    counts = [int(x) for x in raw]
    remainder = total - sum(counts)
    order = sorted(
        range(len(weights)),
        key=lambda i: (raw[i] - counts[i], weights[i]),
        reverse=True,
    )
    for i in range(remainder):
        counts[order[i % len(order)]] += 1
    if total >= len(weights):
        for i, weight in enumerate(weights):
            if weight > 0 and counts[i] == 0:
                donor = max(range(len(counts)), key=lambda j: counts[j])
                if counts[donor] > 1:
                    counts[donor] -= 1
                    counts[i] = 1
    return counts


def _bar_from_styled_parts(
    parts: list[tuple[float, str, str | None]],
    console_width: int,
    max_magnitude: float,
) -> Text:
    """Scale ``parts`` into one bar. Length is their sum against ``max_magnitude``."""
    near = 0.005
    visible = [(amount, style, link) for amount, style, link in parts if amount >= near]
    total = sum(amount for amount, _, _ in visible)
    t = Text()
    if total < near:
        t.append("—", style="dim")
        return t
    width = max(12, min(40, int(console_width / 3)))
    scale = float(max_magnitude) if max_magnitude >= near else 1.0
    row_len = max(1, min(width, round(width * (total / scale))))
    counts = _allocate_bar_counts(row_len, [amount for amount, _, _ in visible])
    for n, (_, style, link) in zip(counts, visible, strict=True):
        _append_block(t, n, style, link)
    if not t.plain:
        t.append("—", style="dim")
    return t


def _styled_slices_magnitude(slices: list[CoverageSlice]) -> float:
    """Bar weight for one row: sum of each account's covered and out-of-pocket dollars."""
    total = 0.0
    for item in slices:
        usage, credit, green_style, red_style, label = _iter_slice_fields(item)
        total += sum(
            amount
            for amount, _style, _link in _coverage_styled_parts(
                usage, credit, green_style, red_style, label
            )
        )
    return total


def _sum_aligned_slices(groups: list[list[CoverageSlice]]) -> list[CoverageSlice]:
    """Sum usage and credits account-by-account. Each group is aligned to the same accounts."""
    if not groups:
        return []
    summed: list[CoverageSlice] = []
    width = len(groups[0])
    for index in range(width):
        usage = 0.0
        credit = 0.0
        green_style = ""
        red_style = ""
        label = ""
        for group in groups:
            u, c, green_style, red_style, label = _iter_slice_fields(group[index])
            usage += u
            credit += c
        summed.append((usage, credit, green_style, red_style, label))
    return summed


def multi_account_coverage_bar(
    slices: list[CoverageSlice],
    console_width: int,
    max_magnitude: float,
) -> Text:
    """
    Grand-total bar: each account contributes its green (credits cover) and red (you pay).

    ``slices`` entries are ``(usage, credit, green_style, red_style)`` in display order.
    A fifth field, when present, is the account label. Holding Option on that account's
    green or red section shows the account and its cost (usage + credits).
    """
    parts: list[tuple[float, str, str | None]] = []
    for item in slices:
        usage, credit, green_style, red_style, label = _iter_slice_fields(item)
        parts.extend(_coverage_styled_parts(usage, credit, green_style, red_style, label))
    return _bar_from_styled_parts(parts, console_width, max_magnitude)


def build_monthly_summary_table(
    monthly_totals: list[tuple],
    grand_total: float,
    grand_usage_rt: float,
    grand_cred_rt: float,
    console_width: int,
    verbose: bool,
    verbose_caption: str = "",
    title: str = "Monthly Summary",
    green_style: str = "green",
    red_style: str = "red",
    account_bars: list[list[CoverageSlice]] | None = None,
    color_legend: str = "",
    account_label: str = "",
) -> Table:
    """
    "Monthly Summary" table with net, RECORD_TYPE usage/credits, and stacked coverage bar column.

    When ``account_bars`` is set, each entry lines up with ``monthly_totals`` and is a list of
    ``(usage, credit, green_style, red_style)`` per linked account, optionally with that
    account's label. Month bars and the grand total bar then stack those account colors.
    Holding Option on a section shows the account and its cost (usage + credits).

    ``account_label`` is the single account for a non-stacked bar.
    """
    summary_table = Table(title=title, expand=True)
    summary_table.add_column("Month", style="cyan")
    summary_table.add_column("Net", justify="right")
    summary_table.add_column("Usage", justify="right", style="red")
    summary_table.add_column("Credits", justify="right", style="green")
    if account_bars is not None:
        bar_heading = "Bar (each account's green = credits cover · its red = you pay)"
    else:
        bar_heading = "Bar (green=credits cover · red=you pay)"
    summary_table.add_column(bar_heading, ratio=1)

    grand_slices: list[CoverageSlice] = []
    if account_bars is not None:
        grand_slices = _sum_aligned_slices(account_bars)
        mags = [_styled_slices_magnitude(slices) for slices in account_bars]
        mags.append(_styled_slices_magnitude(grand_slices))
        summary_max_mag = max(mags, default=0.0)
        if summary_max_mag < 0.005:
            summary_max_mag = 1.0
        grand_bar = multi_account_coverage_bar(
            grand_slices, console_width, summary_max_mag
        )
    else:
        summary_max_mag = _monthly_summary_rec_max_magnitude(
            monthly_totals, grand_usage_rt, grand_cred_rt
        )
        grand_bar = _service_rec_coverage_bar(
            grand_usage_rt,
            grand_cred_rt,
            console_width,
            summary_max_mag,
            green_style=green_style,
            red_style=red_style,
            account_label=account_label,
        )

    summary_table.add_row(
        "[bold]GRAND TOTAL[/bold]",
        _rich_usd_signed_bold(grand_total),
        f"[bold red]{_format_net_usd(grand_usage_rt)}[/bold red]",
        f"[bold green]{_format_net_usd(grand_cred_rt)}[/bold green]",
        grand_bar,
    )

    for index, (month, total, incomplete, usage_rt, cred_rt) in enumerate(monthly_totals):
        label = f"{month} [dim](MTD)[/dim]" if incomplete else month
        if account_bars is not None and index < len(account_bars):
            bar = multi_account_coverage_bar(
                account_bars[index], console_width, summary_max_mag
            )
        else:
            bar = _service_rec_coverage_bar(
                usage_rt,
                cred_rt,
                console_width,
                summary_max_mag,
                green_style=green_style,
                red_style=red_style,
                account_label=account_label,
            )
        summary_table.add_row(
            label,
            _rich_usd_positive_red_negative_green(total),
            _format_net_usd(usage_rt),
            _format_net_usd(cred_rt),
            bar,
        )

    captions: list[str] = []
    if color_legend:
        captions.append(color_legend)
    if verbose and verbose_caption:
        captions.append(verbose_caption)
    if captions:
        summary_table.caption = "\n".join(captions)
    return summary_table


def build_accounts_grand_total_table(
    accounts: list[AccountGrandSlice],
    console_width: int,
    start_date: str,
    end_date: str,
) -> Table:
    """
    Cross-account grand total. Each account row uses that account's green and red; the
    GRAND TOTAL bar stacks those shades so every account stays visible in the total.
    The title includes the same ``start_date`` to ``end_date`` window as the report header.
    """
    table = Table(
        title=(
            "Grand total by account "
            f"[dim]· {escape(start_date)} to {escape(end_date)}[/dim]"
        ),
        expand=True,
    )
    table.add_column("Account", style="cyan")
    table.add_column("Net", justify="right")
    table.add_column("Usage", justify="right")
    table.add_column("Credits", justify="right")
    table.add_column("Bar (each account's green = credits cover · its red = you pay)", ratio=1)

    grand_usage = sum(account.usage for account in accounts)
    grand_cred = sum(account.credit for account in accounts)
    grand_net = sum(account.net for account in accounts)
    max_mag = sum(_service_rec_row_magnitude(account.usage, account.credit) for account in accounts)
    if max_mag < 0.005:
        max_mag = 1.0

    for account in accounts:
        usage_s = f"[{account.red_style}]{_format_net_usd(account.usage)}[/]"
        cred_s = f"[{account.green_style}]{_format_net_usd(account.credit)}[/]"
        bar = _service_rec_coverage_bar(
            account.usage,
            account.credit,
            console_width,
            max_mag,
            green_style=account.green_style,
            red_style=account.red_style,
            account_label=account.label,
        )
        table.add_row(
            escape(account.label),
            _rich_usd_signed_bold(account.net),
            usage_s,
            cred_s,
            bar,
        )

    grand_bar = multi_account_coverage_bar(
        [
            (
                account.usage,
                account.credit,
                account.green_style,
                account.red_style,
                account.label,
            )
            for account in accounts
        ],
        console_width,
        max_mag,
    )
    table.add_row(
        "[bold]GRAND TOTAL[/bold]",
        _rich_usd_signed_bold(grand_net),
        f"[bold]{_format_net_usd(grand_usage)}[/bold]",
        f"[bold]{_format_net_usd(grand_cred)}[/bold]",
        grand_bar,
    )
    legend = " · ".join(
        f"[{account.green_style}]█[/][{account.red_style}]█[/] {escape(account.label)}"
        for account in accounts
    )
    table.caption = (
        "[dim]Accounts included: "
        f"{legend}. Each account has its own green (usage covered by credits) and red "
        "(out-of-pocket). The grand total bar stacks those colors. "
        "Hold Option on a bar section to see that account and its cost "
        "(usage and credits).[/dim]"
    )
    return table


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
    account_label: str | None = None,
    green_style: str = "green",
    red_style: str = "red",
    service_account_slices: dict[str, list[CoverageSlice]] | None = None,
) -> Table:
    """
    One monthly table: **Usage** from ``RECORD_TYPE=Usage`` by SERVICE; **Credits** from
    ``RECORD_TYPE=Credit`` and ``Refund`` by SERVICE. Row amounts sum (by column) to the same
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

    rows: list[tuple[str, float, float, list[CoverageSlice] | None]] = []
    for name in sorted(set(usage_map) | set(credit_map)):
        u = float(usage_map.get(name, 0.0))
        c = float(credit_map.get(name, 0.0))
        slices = None
        if service_account_slices is not None:
            slices = list(service_account_slices.get(name) or [])
        rows.append((name, u, c, slices))

    def _row_weight(
        item: tuple[str, float, float, list[CoverageSlice] | None],
    ) -> float:
        _, u, c, _slices = item
        return max(u, abs(c))

    def _row_eligible_for_display(u: float, c: float) -> bool:
        return show_all or abs(u) >= 0.01 or abs(c) >= 0.01

    rows.sort(key=_row_weight, reverse=True)

    total_count = len(rows)
    non_zero_count = sum(1 for _, u, c, _slices in rows if abs(u) >= 0.01 or abs(c) >= 0.01)
    zero_count = total_count - non_zero_count

    cf = float(cost_filter_min) if cost_filter_min and cost_filter_min > 0 else 0.0
    filter_caption = ""
    if cf > 0.0:
        major: list[tuple[str, float, float, list[CoverageSlice] | None]] = []
        minor: list[tuple[str, float, float, list[CoverageSlice] | None]] = []
        for name, u, c, slices in rows:
            if not _row_eligible_for_display(u, c):
                continue
            if _row_weight((name, u, c, slices)) >= cf:
                major.append((name, u, c, slices))
            else:
                minor.append((name, u, c, slices))
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
            slice_groups = [slices for _, _, _, slices in into_other if slices]
            agg_slices = _sum_aligned_slices(slice_groups) if slice_groups else None
            rows.append((OTHER_SERVICES_ROW_LABEL, agg_u, agg_c, agg_slices))
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
    if account_label:
        title += f" [dim]· {escape(account_label)}[/dim]"
    if filter_caption:
        title = f"{title}\n{filter_caption}"

    table = Table(title=title, expand=True)
    table.add_column("Service", style="cyan")
    table.add_column("Usage", justify="right", style="red")
    table.add_column("Credits", justify="right", style="green")
    if service_account_slices is not None:
        bar_heading = "Bar (each account's green = credits cover · its red = you pay)"
    else:
        bar_heading = "Bar (green=credits cover · red=you pay)"
    table.add_column(bar_heading, ratio=1)

    def _row_magnitude(
        usage: float,
        credit: float,
        slices: list[CoverageSlice] | None,
    ) -> float:
        if slices:
            return _styled_slices_magnitude(slices)
        return _service_rec_row_magnitude(usage, credit)

    visible = [
        (n, u, c, slices)
        for n, u, c, slices in rows
        if show_all or abs(u) >= 0.01 or abs(c) >= 0.01 or n == OTHER_SERVICES_ROW_LABEL
    ]
    max_mag = max(
        (_row_magnitude(u, c, slices) for _, u, c, slices in visible),
        default=0.0,
    )
    if max_mag < 0.005:
        max_mag = 1.0

    for name, u, c, slices in rows:
        if (
            not show_all
            and abs(u) < 0.01
            and abs(c) < 0.01
            and name != OTHER_SERVICES_ROW_LABEL
        ):
            continue
        usage_s = _format_net_usd(u) if u >= 0.005 else "—"
        cred_s = _format_net_usd(c) if c <= -0.005 else "—"
        if slices:
            bar_cell = multi_account_coverage_bar(slices, console_width, max_mag)
        else:
            bar_cell = _service_rec_coverage_bar(
                u,
                c,
                console_width,
                max_mag,
                green_style=green_style,
                red_style=red_style,
                account_label=account_label or "",
            )
        table.add_row(name, usage_s, cred_s, bar_cell)

    if show_all and zero_count > 0:
        table.caption = (
            f"Showing all services including {zero_count} with near-zero Usage and Credits"
        )

    if verbose:
        cap_bits = [
            "Columns: [red]Usage[/red] = SERVICE by RECORD_TYPE Usage; "
            "[green]Credits[/green] = SERVICE by (Credit + Refund). "
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
