"""
Amazon Credits inventory from the Billing ``GetCredits`` API.

Cost Explorer ``RECORD_TYPE=Credit`` is *applied* usage. This module is the Billing console
Credits page: remaining / estimated balance, expiration, and applicable products.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from flexible_datetime import flex_datetime
from rich.console import Console
from rich.table import Table
from rich.text import Text

from .summary_bars import _format_net_usd

# Billing is a global (us-east-1) API. startDate must be past and ≤ ~1 year ago.
BILLING_API_REGION = "us-east-1"
GET_CREDITS_MAX_LOOKBACK_DAYS = 364
_NEAR = 0.005
_FAR_FUTURE = datetime(9999, 12, 31, tzinfo=UTC)


@dataclass(frozen=True)
class AmazonCredit:
    """One credit grant from ``billing.get_credits``."""

    credit_id: str
    account_id: str
    description: str
    credit_type: str
    status: str
    remaining: float
    estimated: float | None
    initial: float
    currency: str
    start: datetime | None
    end: datetime | None
    exhaust: datetime | None
    products: tuple[str, ...] = field(default_factory=tuple)
    payer_aggregated: bool = False


def clamp_get_credits_window(
    start_date: str,
    end_date: str,
    *,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    """
    Fit a Cost Explorer window to GetCredits constraints.

    ``startDate`` must be in the past and not more than one year before now.
    ``endDate`` must not be in the future and must be on or after ``startDate``.
    """
    clock = now if now is not None else datetime.now(UTC)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=UTC)
    else:
        clock = clock.astimezone(UTC)

    start = _as_utc(start_date)
    end = _as_utc(end_date)
    oldest = clock - timedelta(days=GET_CREDITS_MAX_LOOKBACK_DAYS)
    if start < oldest:
        start = oldest
    if start >= clock:
        start = clock - timedelta(days=1)

    if end > clock:
        end = clock
    if end < start:
        end = clock
    return start, end


def _as_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = flex_datetime(value).to_datetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.replace(microsecond=0)


def _parse_money(block: dict | None) -> float | None:
    if not block:
        return None
    raw = block.get("currencyAmount")
    if raw in (None, ""):
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def credit_balance_usd(amount: float | None) -> float:
    """Unused credit as a non-negative USD amount (Billing console remaining-balance style)."""
    if amount is None:
        return 0.0
    return abs(float(amount))


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    if isinstance(value, str) and value.strip():
        try:
            return _as_utc(value)
        except (TypeError, ValueError):
            return None
    return None


def parse_credit_records(
    payload: dict,
    *,
    payer_aggregated: bool = False,
) -> list[AmazonCredit]:
    """Turn a GetCredits response into :class:`AmazonCredit` rows."""
    rows: list[AmazonCredit] = []
    for raw in payload.get("credits") or []:
        initial_block = raw.get("initialAmount") or {}
        remaining = _parse_money(raw.get("remainingAmount"))
        estimated = _parse_money(raw.get("estimatedAmount"))
        initial = _parse_money(initial_block)
        products = tuple(str(p) for p in (raw.get("applicableProductNames") or []) if p)
        desc = (raw.get("description") or "").strip() or str(raw.get("creditId") or "")
        rows.append(
            AmazonCredit(
                credit_id=str(raw.get("creditId") or ""),
                account_id=str(raw.get("accountId") or ""),
                description=desc,
                credit_type=str(raw.get("creditType") or ""),
                status=str(raw.get("creditStatus") or ""),
                remaining=credit_balance_usd(remaining),
                estimated=None if estimated is None else credit_balance_usd(estimated),
                initial=credit_balance_usd(initial),
                currency=str(initial_block.get("currencyCode") or "USD"),
                start=_to_datetime(raw.get("startDate")),
                end=_to_datetime(raw.get("endDate")),
                exhaust=_to_datetime(raw.get("exhaustDate")),
                products=products,
                payer_aggregated=payer_aggregated,
            )
        )
    rows.sort(key=lambda c: (-c.remaining, c.end or _FAR_FUTURE, c.description))
    return rows


def _caller_account_id() -> str | None:
    try:
        account_id = boto3.client("sts").get_caller_identity().get("Account") or ""
    except Exception:
        return None
    return str(account_id) if account_id else None


def fetch_amazon_credits(
    start_date: str,
    end_date: str,
    *,
    account_id: str | None = None,
    billing_client: Any | None = None,
    now: datetime | None = None,
) -> tuple[list[AmazonCredit], str | None, dict]:
    """
    Call ``billing.get_credits``.

    Returns ``(credits, error_markup_or_none, meta)``. Failures are returned as a message so the
    rest of the cost report can continue. ``meta`` includes the window and whether payer
    aggregation was used.
    """
    meta: dict[str, Any] = {}
    acct = account_id or _caller_account_id()
    if not acct:
        return [], "Unable to resolve AWS account ID for Amazon Credits.", meta

    start, end = clamp_get_credits_window(start_date, end_date, now=now)
    meta["account_id"] = acct
    meta["start"] = start.isoformat()
    meta["end"] = end.isoformat()

    try:
        client = billing_client or boto3.client("billing", region_name=BILLING_API_REGION)
    except Exception as exc:
        return [], f"Amazon Credits unavailable (Billing client): {exc}", meta

    if not hasattr(client, "get_credits"):
        return (
            [],
            "Amazon Credits need boto3 ≥ 1.43.41 (Billing GetCredits). Upgrade boto3 and retry.",
            meta,
        )

    kwargs: dict[str, Any] = {"accountId": acct, "startDate": start, "endDate": end}
    aggregated = False
    try:
        payload = client.get_credits(**kwargs, payerAccountFlag=True)
        aggregated = True
    except AttributeError:
        return (
            [],
            "Amazon Credits need boto3 ≥ 1.43.41 (Billing GetCredits). Upgrade boto3 and retry.",
            meta,
        )
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "")
        if code not in {
            "AccessDeniedException",
            "AccessDenied",
            "ValidationException",
            "UnauthorizedOperation",
        }:
            return [], _format_credits_api_error(exc), meta
        try:
            payload = client.get_credits(**kwargs)
        except (BotoCoreError, ClientError) as retry_exc:
            return [], _format_credits_api_error(retry_exc), meta
    except BotoCoreError as exc:
        return [], _format_credits_api_error(exc), meta

    meta["payer_aggregated"] = aggregated
    credits = parse_credit_records(payload, payer_aggregated=aggregated)
    return credits, None, meta


def _format_credits_api_error(exc: BaseException) -> str:
    if isinstance(exc, ClientError):
        err = (exc.response or {}).get("Error") or {}
        code = err.get("Code") or "ClientError"
        msg = err.get("Message") or str(exc)
        if code in {"AccessDeniedException", "AccessDenied"}:
            return (
                "Amazon Credits skipped: this identity needs [bold]billing:GetCredits[/bold] "
                f"in {BILLING_API_REGION} (and IAM access to Billing must be enabled). {msg}"
            )
        return f"Amazon Credits unavailable ({code}): {msg}"
    return f"Amazon Credits unavailable: {exc}"


def _fmt_day(dt: datetime | None) -> str:
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d")


def _expiry_cell(credit: AmazonCredit, *, now: datetime) -> str:
    if credit.exhaust is not None and credit.remaining < _NEAR:
        return f"[dim]exhausted {_fmt_day(credit.exhaust)}[/dim]"
    if credit.end is None:
        return "—"
    days = (credit.end.date() - now.date()).days
    label = _fmt_day(credit.end)
    if days < 0:
        return f"[red]{label} (expired)[/red]"
    if days <= 30:
        return f"[yellow]{label} ({days}d)[/yellow]"
    return f"{label} [dim]({days}d)[/dim]"


def _products_cell(products: tuple[str, ...], *, limit: int = 3) -> str:
    if not products:
        return "[dim]all eligible services[/dim]"
    shown = list(products[:limit])
    extra = len(products) - limit
    text = ", ".join(shown)
    if extra > 0:
        text += f" [dim]+{extra}[/dim]"
    return text


def _credit_label(credit: AmazonCredit) -> str:
    bits = [credit.description]
    extra: list[str] = []
    if credit.credit_type:
        extra.append(credit.credit_type)
    if credit.status and credit.status != "ENABLED":
        extra.append(credit.status)
    if extra:
        bits.append(f"[dim]({' · '.join(extra)})[/dim]")
    return f"{' '.join(bits)}\n{_products_cell(credit.products)}"


def _remaining_vs_initial_bar(remaining: float, initial: float, width: int = 12) -> Text:
    t = Text()
    i = abs(initial)
    r = abs(remaining)
    if i < _NEAR:
        t.append("—", style="dim")
        return t
    n_rem = max(0, min(width, round(width * (r / i))))
    n_used = width - n_rem
    if n_rem:
        t.append("█" * n_rem, style="green")
    if n_used:
        t.append("█" * n_used, style="dim")
    return t


def credits_to_summary_dicts(credits: list[AmazonCredit]) -> list[dict[str, Any]]:
    """JSON-friendly credit rows for ``--out json``."""
    out: list[dict[str, Any]] = []
    for c in credits:
        out.append(
            {
                "credit_id": c.credit_id,
                "account_id": c.account_id,
                "description": c.description,
                "credit_type": c.credit_type,
                "status": c.status,
                "remaining": c.remaining,
                "estimated": c.estimated,
                "initial": c.initial,
                "currency": c.currency,
                "start": c.start.isoformat() if c.start else None,
                "end": c.end.isoformat() if c.end else None,
                "exhaust": c.exhaust.isoformat() if c.exhaust else None,
                "applicable_products": list(c.products),
            }
        )
    return out


def build_amazon_credits_table(
    credits: list[AmazonCredit],
    *,
    now: datetime | None = None,
    payer_aggregated: bool = False,
) -> Table:
    """Rich table of Amazon Credit grants (remaining inventory)."""
    clock = now if now is not None else datetime.now(UTC)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=UTC)

    scope = "consolidated family" if payer_aggregated else "this account"
    table = Table(
        title=f"Amazon Credits [dim]· Billing inventory ({scope})[/dim]",
        expand=True,
    )
    table.add_column("Credit", style="cyan", min_width=36)
    table.add_column("Remaining", justify="right", style="green", no_wrap=True)
    table.add_column("Initial", justify="right", no_wrap=True)
    table.add_column("Expires", no_wrap=True)
    table.add_column("Left", no_wrap=True)

    rem_total = 0.0
    init_total = 0.0
    for c in credits:
        rem_total += c.remaining
        init_total += c.initial
        table.add_row(
            _credit_label(c),
            _format_net_usd(c.remaining),
            _format_net_usd(c.initial),
            _expiry_cell(c, now=clock),
            _remaining_vs_initial_bar(c.remaining, c.initial),
        )

    table.add_row(
        "[bold]TOTAL remaining[/bold]",
        f"[bold green]{_format_net_usd(rem_total)}[/bold green]",
        f"[bold]{_format_net_usd(init_total)}[/bold]",
        "",
        _remaining_vs_initial_bar(rem_total, init_total),
    )
    table.caption = (
        "[dim]Inventory from [bold]billing:GetCredits[/bold] (Billing → Credits), not Cost "
        "Explorer. Remaining is unused balance; estimated remaining is in the line above. "
        "RECORD_TYPE Credit in the monthly tables is already applied to usage. "
        "Bar: [green]green[/green] = still available, dim = already used versus initial.[/dim]"
    )
    return table


def print_amazon_credits_section(
    console: Console,
    start_date: str,
    end_date: str,
    *,
    out_summary: dict | None = None,
    now: datetime | None = None,
    billing_client: Any | None = None,
    account_id: str | None = None,
) -> None:
    """Fetch and print Amazon Credits; never raises (errors are printed as warnings)."""
    credits, err, meta = fetch_amazon_credits(
        start_date,
        end_date,
        account_id=account_id,
        billing_client=billing_client,
        now=now,
    )
    remaining = sum(c.remaining for c in credits)
    estimated = sum(c.estimated or 0.0 for c in credits)
    if out_summary is not None:
        out_summary["amazon_credits"] = {
            "error": err,
            "meta": meta,
            "remaining_total": remaining,
            "estimated_total": estimated,
            "credits": credits_to_summary_dicts(credits),
        }

    if err:
        console.print(f"[yellow]{err}[/yellow]")
        return

    if not credits:
        console.print(
            "[dim]Amazon Credits: none returned by Billing GetCredits for this window.[/dim]"
        )
        return

    n = len(credits)
    grant_word = "grant" if n == 1 else "grants"
    console.print(
        "[dim]Amazon Credits remaining[/dim] "
        f"[green]{_format_net_usd(remaining)}[/green]"
        f"[dim] · estimated {_format_net_usd(estimated)} · {n} {grant_word}[/dim]"
    )
    console.print(
        build_amazon_credits_table(
            credits,
            now=now,
            payer_aggregated=bool(meta.get("payer_aggregated")),
        )
    )
