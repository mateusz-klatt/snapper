"""Operator CLI for bounded read-only candle data-quality audits."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import NoReturn
from typing import cast

import typer

from snapper.application.data_quality.candle_audit_service import CandleAuditReport
from snapper.application.data_quality.candle_audit_service import CandleAuditRequest
from snapper.application.data_quality.candle_audit_service import CandleAuditRequestError
from snapper.application.data_quality.candle_audit_service import CandleAuditTargetError
from snapper.application.data_quality.candle_audit_service import audit_candle_window
from snapper.application.data_quality.candle_audit_service import candle_audit_report_document
from snapper.application.data_quality.candle_audit_service import validate_candle_audit_request
from snapper.config.settings import get_bootstrap_settings
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.data.repository import dispose_repositories
from snapper.data.repository import get_repository


def _now_utc() -> datetime:
    """Capture the command's single current-truth horizon."""
    return datetime.now(UTC)


def _parse_aware_utc(value: str, label: str) -> datetime:
    """Parse one offset-bearing ISO timestamp and normalize it to UTC."""
    try:
        moment = datetime.fromisoformat(value)
    except (OverflowError, ValueError) as error:
        raise CandleAuditRequestError(f"{label} must be an ISO 8601 datetime") from error
    if moment.tzinfo is None:
        raise CandleAuditRequestError(f"{label} must include a UTC offset")
    try:
        return moment.astimezone(UTC)
    except (OverflowError, ValueError) as error:
        raise CandleAuditRequestError(f"{label} cannot be normalized to UTC") from error


def _build_request(
    target: tuple[str, str],
    timeframe: str,
    window: tuple[str, str],
    anchor_offset_seconds: int,
) -> CandleAuditRequest:
    """Build and validate one request without opening the repository."""
    exchange, symbol = target
    try:
        venue = ExchangeEnum(exchange)
    except ValueError as error:
        raise CandleAuditRequestError(f"unsupported exchange: {exchange!r}") from error
    request = CandleAuditRequest(
        exchange=cast(AllExchange, venue.value),
        symbol=symbol,
        timeframe=timeframe,
        window_start=_parse_aware_utc(window[0], "window start"),
        window_end=_parse_aware_utc(window[1], "window end"),
        as_of=_now_utc(),
        anchor_offset_seconds=anchor_offset_seconds,
    )
    return validate_candle_audit_request(request)


async def _run_audit(request: CandleAuditRequest) -> CandleAuditReport:
    """Run one validated audit against the configured repository."""
    bootstrap = get_bootstrap_settings()
    repository = get_repository(bootstrap.db_url)
    try:
        return await audit_candle_window(repository, request)
    finally:
        await dispose_repositories()


def _emit_human(report: CandleAuditReport) -> None:
    """Emit the report in a stable line-oriented human format."""
    document = candle_audit_report_document(report)
    ordered_fields = (
        "status",
        "exchange",
        "symbol",
        "timeframe",
        "window_start",
        "window_end",
        "as_of",
        "gap_policy",
        "split_threshold",
        "anchor_offset_seconds",
        "slots_expected",
        "candles_scanned",
        "oldest_open_at",
        "newest_open_at",
        "anomaly_count",
    )
    for field in ordered_fields:
        typer.echo(f"{field}: {document[field]}")
    for anomaly in report.anomalies:
        typer.echo(f"anomaly: {anomaly.open_at.isoformat()} {anomaly.type.value} {anomaly.detail}")


def _emit_report(report: CandleAuditReport, json_output: bool) -> None:
    """Emit one report in the operator-selected deterministic format."""
    if json_output:
        typer.echo(
            json.dumps(
                candle_audit_report_document(report),
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
        return
    _emit_human(report)


def _fatal(message: str) -> NoReturn:
    """Emit one safe incomplete-audit refusal and exit three."""
    typer.echo(f"audit incomplete: {message}", err=True)
    raise typer.Exit(code=3)


def audit_candles(
    target: Annotated[
        tuple[str, str],
        typer.Option(
            "--target",
            help="Exact EXCHANGE SYMBOL pair owning the persisted candle series.",
        ),
    ],
    timeframe: Annotated[
        str,
        typer.Option("--timeframe", "-t", help="1m, 5m, 15m, 30m, 1h, 4h, or 1d."),
    ],
    window: Annotated[
        tuple[str, str],
        typer.Option(
            "--window",
            help="Inclusive expected candle opens: START END, both with UTC offsets.",
        ),
    ],
    anchor_offset_seconds: Annotated[
        int,
        typer.Option(
            "--anchor-offset-seconds",
            help="Explicit intraday grid offset from the UNIX epoch.",
        ),
    ] = 0,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit one stable JSON document."),
    ] = False,
) -> None:
    """Audit one bounded persisted candle window without mutating data.

    Every missing internal slot is reported because the command deliberately
    applies the explicit ``unsuppressed`` gap policy. For session markets, pass
    a window covering one expected trading session; the command never guesses a
    calendar from an exchange such as Polygon, which mixes asset classes.

    Exit status is zero for a clean complete audit, one for a completed audit
    with anomalies, two for invalid command usage, and three when the audit
    could not be completed.

    Args:
        target: Exact exchange and native-symbol pair.
        timeframe: Candle interval understood by the pure auditor.
        window: Inclusive expected first and last candle opens.
        anchor_offset_seconds: Explicit intraday venue-grid offset in seconds.
        json_output: Whether to emit one deterministic JSON document.

    Raises:
        typer.BadParameter: When the request is invalid before database use.
        typer.Exit: With one for anomalies or three for an incomplete audit.
    """
    try:
        request = _build_request(target, timeframe, window, anchor_offset_seconds)
    except CandleAuditRequestError as error:
        raise typer.BadParameter(str(error)) from error
    try:
        report = asyncio.run(_run_audit(request))
    except CandleAuditTargetError as error:
        _fatal(str(error))
    except CandleAuditRequestError as error:
        _fatal(str(error))
    except Exception:
        _fatal("repository read failed")
    _emit_report(report, json_output)
    if report.anomalies:
        raise typer.Exit(code=1)
