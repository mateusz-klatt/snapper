"""Bounded read-only service for auditing one persisted candle window.

The pure candle auditor deliberately treats an empty sequence as clean and can
only see gaps between rows it receives. This service adds the operator-facing
window contract: a known native symbol, inclusive and closed UTC boundaries,
bounded materialization, explicit unsuppressed gap semantics, and fail-closed
findings for empty, boundary-missing, or incomplete data.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

from snapper.application.data_quality.candle_audit import CandleAnomaly
from snapper.application.data_quality.candle_audit import CandleAnomalyType
from snapper.application.data_quality.candle_audit import audit_candle_series
from snapper.application.data_quality.candle_audit import candle_timeframe_seconds
from snapper.application.data_quality.candle_audit import is_candle_open_on_grid
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleWindowQuery

CANDLE_AUDIT_SCHEMA_VERSION = 1
MAX_CANDLE_AUDIT_SLOTS = 100_000
_SPLIT_THRESHOLD = 0.30

type CandleAuditStatus = Literal["clean", "anomalies"]
type CandleAuditGapPolicy = Literal["unsuppressed"]


class CandleAuditRequestError(ValueError):
    """Raised when an audit request cannot name one safe bounded window."""


class CandleAuditTargetError(LookupError):
    """Raised when the requested native symbol is not active at ``as_of``."""


@dataclass(frozen=True, slots=True)
class CandleAuditRequest:
    """One reproducible persisted candle-window audit request."""

    exchange: AllExchange
    symbol: str
    timeframe: str
    window_start: datetime
    window_end: datetime
    as_of: datetime
    anchor_offset_seconds: int = 0


@dataclass(frozen=True, slots=True)
class CandleAuditReport:
    """Deterministic result of a complete bounded candle-window audit."""

    request: CandleAuditRequest
    slots_expected: int
    candles_scanned: int
    oldest_open_at: datetime | None
    newest_open_at: datetime | None
    anomalies: tuple[CandleAnomaly, ...]
    status: CandleAuditStatus
    gap_policy: CandleAuditGapPolicy = "unsuppressed"
    split_threshold: float = _SPLIT_THRESHOLD
    anchor_offset_seconds: int = 0
    schema_version: int = CANDLE_AUDIT_SCHEMA_VERSION


def _as_utc(moment: datetime) -> datetime:
    """Normalize one aware datetime to UTC."""
    try:
        return moment.astimezone(UTC)
    except (OverflowError, ValueError) as error:
        raise CandleAuditRequestError("datetime cannot be normalized to UTC") from error


def _slot_count(start: datetime, end: datetime, interval_seconds: int) -> int:
    """Return the inclusive number of expected slots in one aligned window."""
    return (end - start) // timedelta(seconds=interval_seconds) + 1


def _validated_interval_seconds(request: CandleAuditRequest) -> int:
    """Return the timeframe interval after validating its anchor offset."""
    try:
        interval_seconds = candle_timeframe_seconds(request.timeframe)
    except ValueError as error:
        raise CandleAuditRequestError(str(error)) from error
    if not 0 <= request.anchor_offset_seconds < interval_seconds:
        raise CandleAuditRequestError(
            "anchor offset must be non-negative and smaller than the timeframe"
        )
    if request.timeframe == "1d" and request.anchor_offset_seconds != 0:
        raise CandleAuditRequestError("daily audits require an anchor offset of zero")
    return interval_seconds


def _normalized_request_times(
    request: CandleAuditRequest,
) -> tuple[datetime, datetime, datetime]:
    """Return aware request coordinates normalized to UTC."""
    if (
        request.window_start.tzinfo is None
        or request.window_end.tzinfo is None
        or request.as_of.tzinfo is None
    ):
        raise CandleAuditRequestError("window boundaries and as_of must include UTC offsets")
    return (
        _as_utc(request.window_start),
        _as_utc(request.window_end),
        _as_utc(request.as_of),
    )


def _validate_window_geometry(
    request: CandleAuditRequest,
    start: datetime,
    end: datetime,
    interval_seconds: int,
) -> int:
    """Validate ordering, grid alignment, span, and daily scope."""
    if start > end:
        raise CandleAuditRequestError("window start must not follow window end")
    if request.timeframe != "1d" and (
        not is_candle_open_on_grid(
            start,
            request.timeframe,
            anchor_offset_seconds=request.anchor_offset_seconds,
        )
        or not is_candle_open_on_grid(
            end,
            request.timeframe,
            anchor_offset_seconds=request.anchor_offset_seconds,
        )
    ):
        raise CandleAuditRequestError("window boundaries must land on the timeframe grid")
    interval = timedelta(seconds=interval_seconds)
    if (end - start) % interval != timedelta(0):
        raise CandleAuditRequestError("window boundaries must span whole timeframe intervals")
    slots_expected = _slot_count(start, end, interval_seconds)
    if request.timeframe == "1d" and slots_expected > 1:
        raise CandleAuditRequestError(
            "multi-slot daily audits require a calendar-aware expected-open contract"
        )
    return slots_expected


def validate_candle_audit_request(request: CandleAuditRequest) -> CandleAuditRequest:
    """Validate and UTC-normalize one bounded audit request before database use.

    Args:
        request: Candidate request assembled by a caller.

    Returns:
        An equivalent request with all datetimes normalized to UTC.

    Raises:
        CandleAuditRequestError: If the symbol, timeframe, boundaries, horizon,
            or materialization bound is unsafe or ambiguous.
    """
    if not request.symbol or request.symbol != request.symbol.strip():
        raise CandleAuditRequestError("symbol must be non-empty and have no surrounding whitespace")
    interval_seconds = _validated_interval_seconds(request)
    start, end, as_of = _normalized_request_times(request)
    slots_expected = _validate_window_geometry(request, start, end, interval_seconds)
    interval = timedelta(seconds=interval_seconds)
    if as_of < end or as_of - end < interval:
        raise CandleAuditRequestError("window end must name a candle closed by as_of")
    if slots_expected > MAX_CANDLE_AUDIT_SLOTS:
        raise CandleAuditRequestError(
            f"window requests {slots_expected} slots; maximum is {MAX_CANDLE_AUDIT_SLOTS}"
        )
    return CandleAuditRequest(
        exchange=request.exchange,
        symbol=request.symbol,
        timeframe=request.timeframe,
        window_start=start,
        window_end=end,
        as_of=as_of,
        anchor_offset_seconds=request.anchor_offset_seconds,
    )


def _boundary_anomalies(
    request: CandleAuditRequest,
    oldest_open_at: datetime | None,
    newest_open_at: datetime | None,
) -> list[CandleAnomaly]:
    """Return fail-closed findings for empty or missing window boundaries."""
    if oldest_open_at is None or newest_open_at is None:
        return [
            CandleAnomaly(
                CandleAnomalyType.EMPTY_WINDOW,
                request.window_start,
                "no visible candles in the requested inclusive window",
            )
        ]
    anomalies: list[CandleAnomaly] = []
    if oldest_open_at != request.window_start:
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.WINDOW_BOUNDARY_GAP,
                request.window_start,
                f"expected first open_at {request.window_start.isoformat()}, "
                f"observed {oldest_open_at.isoformat()}",
            )
        )
    if newest_open_at != request.window_end:
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.WINDOW_BOUNDARY_GAP,
                request.window_end,
                f"expected last open_at {request.window_end.isoformat()}, "
                f"observed {newest_open_at.isoformat()}",
            )
        )
    return anomalies


def _incomplete_anomalies(candles: Sequence[CandleRow]) -> list[CandleAnomaly]:
    """Return one finding for every non-final row in the closed window."""
    return [
        CandleAnomaly(
            CandleAnomalyType.INCOMPLETE_CANDLE,
            _as_utc(row["open_at"]),
            "persisted candle is not complete",
        )
        for row in candles
        if not row["complete"]
    ]


def build_candle_audit_report(
    request: CandleAuditRequest,
    candles: Sequence[CandleRow],
) -> CandleAuditReport:
    """Build one deterministic report from an already-read candle window.

    Args:
        request: Validated coordinates and knowledge horizon for the audit.
        candles: Visible rows returned for the inclusive window.

    Returns:
        A complete report containing sorted core and window-level findings.
    """
    request = validate_candle_audit_request(request)
    open_times = [_as_utc(row["open_at"]) for row in candles]
    oldest_open_at = min(open_times, default=None)
    newest_open_at = max(open_times, default=None)
    anomalies = audit_candle_series(
        candles,
        request.timeframe,
        split_threshold=_SPLIT_THRESHOLD,
        anchor_offset_seconds=request.anchor_offset_seconds,
        expected_gap=None,
    )
    anomalies.extend(_incomplete_anomalies(candles))
    anomalies.extend(_boundary_anomalies(request, oldest_open_at, newest_open_at))
    ordered = tuple(
        sorted(anomalies, key=lambda item: (item.open_at, item.type.value, item.detail))
    )
    interval_seconds = candle_timeframe_seconds(request.timeframe)
    return CandleAuditReport(
        request=request,
        slots_expected=_slot_count(request.window_start, request.window_end, interval_seconds),
        candles_scanned=len(candles),
        oldest_open_at=oldest_open_at,
        newest_open_at=newest_open_at,
        anomalies=ordered,
        status="anomalies" if ordered else "clean",
        anchor_offset_seconds=request.anchor_offset_seconds,
    )


async def audit_candle_window(
    repository: Repository,
    request: CandleAuditRequest,
) -> CandleAuditReport:
    """Read and audit one known native-symbol window without mutating storage.

    Args:
        repository: Repository used only for symbol and candle reads.
        request: Bounded audit request.

    Returns:
        The completed deterministic audit report.

    Raises:
        CandleAuditRequestError: If the request or returned materialization is unsafe.
        CandleAuditTargetError: If the native symbol is not active at ``as_of``.
    """
    request = validate_candle_audit_request(request)
    candles = await repository.get_candle_window_for_active_symbol(
        CandleWindowQuery(
            native_symbol=request.symbol,
            timeframe=request.timeframe,
            window_start=request.window_start,
            window_end=request.window_end,
            exchange=request.exchange,
            as_of=request.as_of,
            limit=MAX_CANDLE_AUDIT_SLOTS + 1,
        )
    )
    if candles is None:
        raise CandleAuditTargetError(
            f"symbol {request.symbol!r} is not active on {request.exchange!r} at as_of"
        )
    if len(candles) > MAX_CANDLE_AUDIT_SLOTS:
        raise CandleAuditRequestError("repository returned a truncated or oversized audit window")
    return build_candle_audit_report(request, candles)


def candle_audit_report_document(report: CandleAuditReport) -> JsonObject:
    """Return the stable machine-readable document for one completed audit.

    Args:
        report: Completed report to serialize.

    Returns:
        JSON-shaped data with stable field names and ISO timestamps.
    """
    anomaly_documents: list[JsonValue] = [
        {
            "type": anomaly.type.value,
            "open_at": anomaly.open_at.isoformat(),
            "detail": anomaly.detail,
        }
        for anomaly in report.anomalies
    ]
    return {
        "schema_version": report.schema_version,
        "status": report.status,
        "exchange": report.request.exchange,
        "symbol": report.request.symbol,
        "timeframe": report.request.timeframe,
        "window_start": report.request.window_start.isoformat(),
        "window_end": report.request.window_end.isoformat(),
        "as_of": report.request.as_of.isoformat(),
        "gap_policy": report.gap_policy,
        "split_threshold": report.split_threshold,
        "anchor_offset_seconds": report.anchor_offset_seconds,
        "slots_expected": report.slots_expected,
        "candles_scanned": report.candles_scanned,
        "oldest_open_at": (
            report.oldest_open_at.isoformat() if report.oldest_open_at is not None else None
        ),
        "newest_open_at": (
            report.newest_open_at.isoformat() if report.newest_open_at is not None else None
        ),
        "anomaly_count": len(report.anomalies),
        "anomalies": anomaly_documents,
    }
