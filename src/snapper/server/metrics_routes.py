"""``/api/metrics/*`` — operator observability surface.

Two route families, both gated by ``Permission.READ_SYSTEM_STATUS``:

* ``/notifications`` — iOS Push Foundation ops metrics (BE-3c §D11).
  DB-derived outbox counters + per-status totals so an oncall
  dashboard can tell at a glance whether the sidecar is keeping up
  or whether deliveries are piling up in the retry queue.

* ``/system`` + ``/system/history`` + ``/system/tracemalloc/{start,stop}``
  — process-level health metrics sampled by
  :class:`SystemMetricsSnapshotter` into an in-memory ring buffer.
  Surfaces CPU, memory, threads, fds, asyncio, gc, cgroup, saturation
  %, and DB-internal pool counters so the operator can see the
  gradient toward exhaustion before a crash.

Latency percentiles (``apns_p99_latency_ms``) + sidecar heartbeat
(``sidecar_heartbeat_seconds_since``) from the plan wishlist are
deferred — they need cross-process state (the sidecar runs in its
own process under ``snapper notify``). A follow-up plan will wire
those via a small heartbeat + histogram settings table.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import status

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.api.schemas.db_stats import DbStatsData
from snapper.api.schemas.db_stats import DbStatsResponse
from snapper.api.schemas.db_stats import TableStatsItem
from snapper.api.schemas.retention import RetentionPolicyResult
from snapper.api.schemas.retention import RetentionRunData
from snapper.api.schemas.retention import RetentionRunResponse
from snapper.api.schemas.system_metrics import AsyncioMetrics
from snapper.api.schemas.system_metrics import CpuMetrics
from snapper.api.schemas.system_metrics import DbInternalMetrics
from snapper.api.schemas.system_metrics import GcMetrics
from snapper.api.schemas.system_metrics import LimitsMetrics
from snapper.api.schemas.system_metrics import MemoryMetrics
from snapper.api.schemas.system_metrics import ProcessMetrics
from snapper.api.schemas.system_metrics import SaturationMetrics
from snapper.api.schemas.system_metrics import SystemMetricsData
from snapper.api.schemas.system_metrics import SystemMetricsHistoryItem
from snapper.api.schemas.system_metrics import SystemMetricsHistoryResponse
from snapper.api.schemas.system_metrics import SystemMetricsResponse
from snapper.api.schemas.system_metrics import TracemallocState
from snapper.api.schemas.system_metrics import TracemallocStateResponse
from snapper.application.db_stats.snapshotter import DbStatsSnapshot
from snapper.application.db_stats.snapshotter import DbStatsSnapshotter
from snapper.application.db_stats.snapshotter import TableStats
from snapper.application.retention.scheduler import RetentionScheduler
from snapper.application.retention.service import RetentionPolicyRunResult
from snapper.application.retention.service import RetentionRunSummary
from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot
from snapper.application.system_metrics.snapshotter import SystemMetricsSnapshotter
from snapper.application.system_metrics.tracemalloc_controller import DEFAULT_DURATION_SECONDS
from snapper.application.system_metrics.tracemalloc_controller import clamp_duration
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/metrics", tags=["metrics"])

_REST_STREAM = "rest.metrics"
_DEFAULT_HISTORY_LIMIT = 720
_SNAPSHOTTER_UNAVAILABLE_DETAIL = "system metrics snapshotter not available"
_RETENTION_UNAVAILABLE_DETAIL = "retention scheduler not available"
_RETENTION_DISABLED_DETAIL = "retention scheduler disabled"
_RETENTION_NOT_YET_RUN_DETAIL = "retention scheduler not yet run"
_DB_STATS_UNAVAILABLE_DETAIL = "DB metrics snapshotter not initialized"
_DB_STATS_DISABLED_DETAIL = "DB metrics snapshotter disabled via DB_METRICS_DISABLED"
_DB_STATS_NOT_YET_RUN_DETAIL = "DB metrics snapshotter has not completed a sample yet"


class NotificationMetricsData(StrictDataSchema[Literal["notification_metrics"]]):
    """DB-derived counters for the notify sidecar's outbox (§D11).

    All counts aggregate over active ``alert_deliveries`` rows
    (``known_to = KNOWN_TO_MAX``) — the SCD2 predecessor versions are
    excluded so the numbers reflect the current authoritative state
    of each delivery.

    Attributes:
        delivery_success_total: Deliveries in the ``sent`` terminal state.
        delivery_failed_total: Deliveries in the ``failed`` terminal state
            (exhausted retry budget).
        delivery_410_unregistered_total: Deliveries terminated via an
            APNs 410 ``BadDeviceToken`` response.
        delivery_cancelled_scope_total: Deliveries transitioned to
            ``cancelled_scope`` after an ``admin.scope_revoked`` or
            ``admin.user_deactivated`` event.
        outbox_queued_depth: Number of deliveries still in ``queued``
            status — backlog for the retry loop.
    """

    type: Literal["notification_metrics"] = "notification_metrics"
    delivery_success_total: int
    delivery_failed_total: int
    delivery_410_unregistered_total: int
    delivery_cancelled_scope_total: int
    outbox_queued_depth: int


class NotificationMetricsResponse(
    PayloadResponse[Literal["notification_metrics_response"], NotificationMetricsData]
):
    """Envelope-wrapped response for ``GET /api/metrics/notifications``."""

    type: Literal["notification_metrics_response"] = "notification_metrics_response"


@router.get("/notifications")
async def get_notification_metrics(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> NotificationMetricsResponse:
    """Return current notify-sidecar outbox counters (§D11).

    Args:
        request: FastAPI request — provides REST tracker.
        _principal: Authenticated caller (permission guard already
            enforced at dependency resolution; the bound value is
            discarded because the counters are user-agnostic).
        repo: Repository dependency.

    Returns:
        ``NotificationMetricsResponse`` with zero-defaults for every
        status absent from the current DB snapshot.
    """
    counts = await repo.count_deliveries_by_status()
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    payload = NotificationMetricsData(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        delivery_success_total=counts.get("sent", 0),
        delivery_failed_total=counts.get("failed", 0),
        delivery_410_unregistered_total=counts.get("unregistered", 0),
        delivery_cancelled_scope_total=counts.get("cancelled_scope", 0),
        outbox_queued_depth=counts.get("queued", 0),
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return NotificationMetricsResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=payload,
    )


def _next_provenance(tracker: SequenceTracker) -> tuple[str, int, datetime, str]:
    """Mint a fresh envelope ``(session_id, sequence_id, timestamp, public_id)``."""
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, ts, pid


def _normalize_utc_bound(value: datetime) -> datetime:
    """Normalize a datetime query param to a UTC-aware bound.

    FastAPI / Pydantic accepts naive ISO-8601 strings (e.g.
    ``2026-05-01T12:00:00`` without offset) and produces a naive
    ``datetime``. Comparing such values with the UTC-aware
    ``bus_time`` field on snapshots raises
    ``TypeError: can't compare offset-naive and offset-aware datetimes``
    inside :meth:`MetricsRingBuffer.slice`. Treat naive inputs as UTC
    (matches :func:`snapper.server.app._normalize_utc`) so the route
    layer never propagates such a TypeError.

    Args:
        value: Caller-supplied datetime bound, possibly naive.

    Returns:
        A UTC-aware ``datetime`` safe to compare against
        ``snapshot["bus_time"]``.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _resolve_snapshotter(request: Request) -> SystemMetricsSnapshotter:
    """Pull the snapshotter singleton or raise 503 if unavailable.

    The lifespan startup hook in :mod:`snapper.server.app` assigns the
    attribute ONLY after a successful :meth:`SystemMetricsSnapshotter.start`
    call. If startup raised, the attribute is left absent and routes
    fall through to 503 (B22 — no half-initialized object can bypass
    this fallback).
    """
    snapshotter: SystemMetricsSnapshotter | None = getattr(
        request.app.state, "system_metrics_snapshotter", None
    )
    if snapshotter is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_SNAPSHOTTER_UNAVAILABLE_DETAIL,
        )
    return snapshotter


def _build_system_metrics_data(
    snapshot: SystemMetricsSnapshot,
    *,
    session_id: str,
    sequence_id: int,
    public_id: str,
    timestamp: datetime,
) -> SystemMetricsData:
    """Map an in-memory :class:`SystemMetricsSnapshot` to the wire schema."""
    return SystemMetricsData(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        bus_time=snapshot["bus_time"],
        process=ProcessMetrics(**snapshot["process"]),
        cpu=CpuMetrics(**snapshot["cpu"]),
        memory=MemoryMetrics(**snapshot["memory"]),
        asyncio=AsyncioMetrics(**snapshot["asyncio"]),
        gc=GcMetrics(
            collections_gen0=snapshot["gc"]["collections_per_gen"][0],
            collections_gen1=snapshot["gc"]["collections_per_gen"][1],
            collections_gen2=snapshot["gc"]["collections_per_gen"][2],
            uncollectable=snapshot["gc"]["uncollectable"],
            current_objects=snapshot["gc"]["current_objects"],
        ),
        limits=LimitsMetrics(**snapshot["limits"]),
        saturation=SaturationMetrics(**snapshot["saturation"]),
        db_internal=DbInternalMetrics(**snapshot["db_internal"]),
        tracemalloc_active=snapshot["tracemalloc_active"],
        cgroup_version=snapshot["cgroup_version"],
    )


def _build_system_metrics_history_item(
    snapshot: SystemMetricsSnapshot,
    *,
    session_id: str,
    sequence_id: int,
    public_id: str,
    timestamp: datetime,
) -> SystemMetricsHistoryItem:
    """Map an in-memory snapshot to the history-list item wire schema."""
    return SystemMetricsHistoryItem(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        bus_time=snapshot["bus_time"],
        process=ProcessMetrics(**snapshot["process"]),
        cpu=CpuMetrics(**snapshot["cpu"]),
        memory=MemoryMetrics(**snapshot["memory"]),
        asyncio=AsyncioMetrics(**snapshot["asyncio"]),
        gc=GcMetrics(
            collections_gen0=snapshot["gc"]["collections_per_gen"][0],
            collections_gen1=snapshot["gc"]["collections_per_gen"][1],
            collections_gen2=snapshot["gc"]["collections_per_gen"][2],
            uncollectable=snapshot["gc"]["uncollectable"],
            current_objects=snapshot["gc"]["current_objects"],
        ),
        limits=LimitsMetrics(**snapshot["limits"]),
        saturation=SaturationMetrics(**snapshot["saturation"]),
        db_internal=DbInternalMetrics(**snapshot["db_internal"]),
        tracemalloc_active=snapshot["tracemalloc_active"],
        cgroup_version=snapshot["cgroup_version"],
    )


@router.get("/system")
async def get_system_metrics(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
) -> SystemMetricsResponse:
    """Return the most recent ``SystemMetricsSnapshot`` from the ring buffer.

    Cold-start contract: :meth:`SystemMetricsSnapshotter.start` takes
    one eager sample BEFORE returning, so a successfully-started
    singleton always has at least one snapshot when the first request
    arrives. If the snapshotter failed to start at lifespan time, the
    attribute is absent and this route returns 503.

    Args:
        request: FastAPI request — provides app state + REST tracker.
        _principal: Authenticated caller (permission gate enforced at
            dependency resolution; the bound value is discarded because
            the metrics are user-agnostic).

    Returns:
        :class:`SystemMetricsResponse` envelope wrapping the latest
        :class:`SystemMetricsData` payload.

    Raises:
        HTTPException: 503 when the snapshotter singleton is missing.
    """
    snapshotter = _resolve_snapshotter(request)
    snapshot = await snapshotter.current_snapshot()
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_SNAPSHOTTER_UNAVAILABLE_DETAIL,
        )
    tracker: SequenceTracker = request.app.state.rest_tracker
    payload_sid, payload_seq, payload_ts, payload_pid = _next_provenance(tracker)
    payload = _build_system_metrics_data(
        snapshot,
        session_id=payload_sid,
        sequence_id=payload_seq,
        public_id=payload_pid,
        timestamp=payload_ts,
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return SystemMetricsResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=payload,
    )


@router.get("/system/history")
async def get_system_metrics_history(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
    since: Annotated[datetime | None, Query()] = None,
    until: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=100000)] = _DEFAULT_HISTORY_LIMIT,
) -> SystemMetricsHistoryResponse:
    """Return a windowed slice of the snapshot history buffer.

    Args:
        request: FastAPI request — provides app state + REST tracker.
        _principal: Authenticated caller (permission gate at dependency
            resolution).
        since: Inclusive lower bound on snapshot ``bus_time`` (ISO-8601
            UTC). ``None`` means "from the start of the buffer".
        until: Inclusive upper bound. ``None`` means "to now".
        limit: Maximum snapshots returned (most-recent N within the
            window). Default 720 (1h at the standard 5s sampling
            interval); max 100000.

    Returns:
        :class:`SystemMetricsHistoryResponse` envelope wrapping a
        chronologically-ordered list of
        :class:`SystemMetricsHistoryItem` payloads.

    Raises:
        HTTPException: 503 when the snapshotter singleton is missing.
    """
    snapshotter = _resolve_snapshotter(request)
    lower = _normalize_utc_bound(since) if since is not None else datetime.min.replace(tzinfo=UTC)
    upper = _normalize_utc_bound(until) if until is not None else datetime.max.replace(tzinfo=UTC)
    snapshots = await snapshotter.history(lower, upper, limit)
    tracker: SequenceTracker = request.app.state.rest_tracker
    items: list[SystemMetricsHistoryItem] = []
    for snap in snapshots:
        sid, seq, ts, pid = _next_provenance(tracker)
        items.append(
            _build_system_metrics_history_item(
                snap,
                session_id=sid,
                sequence_id=seq,
                public_id=pid,
                timestamp=ts,
            )
        )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return SystemMetricsHistoryResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=items,
        count=len(items),
    )


@router.post("/system/tracemalloc/start")
async def post_system_metrics_tracemalloc_start(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    duration_s: Annotated[float, Query(gt=0)] = DEFAULT_DURATION_SECONDS,
) -> TracemallocStateResponse:
    """Arm Python tracemalloc with an auto-stop deadline.

    ``duration_s`` is clamped to ``(0, MAX_DURATION_SECONDS]`` (default
    600s, hard max 3600s). Calling while already armed REPLACES the
    deadline.

    Args:
        request: FastAPI request.
        _principal: Authenticated caller (permission gate).
        _csrf: CSRF token validation (cookie auth) — Bearer-auth
            requests bypass per
            :func:`snapper.auth.dependencies.validate_csrf_token`.
        duration_s: Auto-stop deadline before tracemalloc is disarmed.

    Returns:
        :class:`TracemallocStateResponse` with ``active=True`` and the
        clamped ``requested_duration_seconds``.

    Raises:
        HTTPException: 503 when the snapshotter singleton is missing.
    """
    snapshotter = _resolve_snapshotter(request)
    clamped = clamp_duration(duration_s)
    await snapshotter.tracemalloc.start(clamped)
    tracker: SequenceTracker = request.app.state.rest_tracker
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return TracemallocStateResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=TracemallocState(
            active=snapshotter.tracemalloc.is_active(),
            requested_duration_seconds=clamped,
        ),
    )


@router.post("/system/tracemalloc/stop")
async def post_system_metrics_tracemalloc_stop(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
) -> TracemallocStateResponse:
    """Disarm tracemalloc + cancel any pending auto-stop deadline.

    Args:
        request: FastAPI request.
        _principal: Authenticated caller (permission gate).
        _csrf: CSRF token validation (cookie auth) — Bearer-auth
            requests bypass per
            :func:`snapper.auth.dependencies.validate_csrf_token`.

    Returns:
        :class:`TracemallocStateResponse` with ``active=False`` and
        ``requested_duration_seconds=None``.

    Raises:
        HTTPException: 503 when the snapshotter singleton is missing.
    """
    snapshotter = _resolve_snapshotter(request)
    await snapshotter.tracemalloc.stop()
    tracker: SequenceTracker = request.app.state.rest_tracker
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return TracemallocStateResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=TracemallocState(
            active=snapshotter.tracemalloc.is_active(),
            requested_duration_seconds=None,
        ),
    )


@router.get("/retention")
async def get_retention_metrics(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
) -> RetentionRunResponse:
    """Return the most recent retention-scheduler run summary.

    Args:
        request: FastAPI request — provides app state + REST tracker.
        _principal: Authenticated caller (permission gate enforced at
            dependency resolution; the bound value is discarded because
            the metrics are user-agnostic).

    Returns:
        :class:`RetentionRunResponse` envelope wrapping the latest
        :class:`RetentionRunData` payload — per-policy outcomes for
        the most recent scheduler tick.

    Raises:
        HTTPException: 503 when the scheduler is missing, disabled, or
            has not yet run a tick (cold-start window).
    """
    scheduler = _resolve_retention_scheduler(request)
    if scheduler.disabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_RETENTION_DISABLED_DETAIL,
        )
    summary = scheduler.last_run_summary
    if summary is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_RETENTION_NOT_YET_RUN_DETAIL,
        )
    tracker: SequenceTracker = request.app.state.rest_tracker
    payload_sid, payload_seq, payload_ts, payload_pid = _next_provenance(tracker)
    payload = _build_retention_run_data(
        summary,
        session_id=payload_sid,
        sequence_id=payload_seq,
        public_id=payload_pid,
        timestamp=payload_ts,
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return RetentionRunResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=payload,
    )


def _resolve_retention_scheduler(request: Request) -> RetentionScheduler:
    """Pull the retention scheduler singleton or raise 503 if unavailable.

    The lifespan startup hook in :mod:`snapper.server.app` assigns
    ``app.state.retention_scheduler`` ONLY after a successful
    :meth:`RetentionScheduler.start` call (B22 — no half-initialized
    object can bypass the route layer's 503 fallback).

    Args:
        request: FastAPI request whose ``app.state`` holds the
            singleton (or pre-set ``None`` on startup failure).

    Returns:
        The attached :class:`RetentionScheduler` instance.

    Raises:
        HTTPException: 503 when no scheduler is attached.
    """
    scheduler: RetentionScheduler | None = getattr(request.app.state, "retention_scheduler", None)
    if scheduler is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_RETENTION_UNAVAILABLE_DETAIL,
        )
    return scheduler


def _build_retention_run_data(
    summary: RetentionRunSummary,
    *,
    session_id: str,
    sequence_id: int,
    public_id: str,
    timestamp: datetime,
) -> RetentionRunData:
    """Map an in-memory :class:`RetentionRunSummary` to the wire schema.

    Args:
        summary: Source summary captured by the scheduler.
        session_id: Envelope provenance — payload session.
        sequence_id: Envelope provenance — payload sequence.
        public_id: Envelope provenance — payload UUID7.
        timestamp: Envelope provenance — payload timestamp.

    Returns:
        Wire-strict :class:`RetentionRunData` payload.
    """
    return RetentionRunData(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        run_started_at=summary["run_started_at"],
        run_completed_at=summary["run_completed_at"],
        dry_run=summary["dry_run"],
        results=[_build_retention_policy_result(r) for r in summary["results"]],
    )


def _build_retention_policy_result(
    result: RetentionPolicyRunResult,
) -> RetentionPolicyResult:
    """Map an in-memory :class:`RetentionPolicyRunResult` to the wire schema.

    Args:
        result: Source per-policy result captured by the service.

    Returns:
        Wire-strict :class:`RetentionPolicyResult` body.
    """
    return RetentionPolicyResult(
        table=result["table"],
        retain_days=result["retain_days"],
        backlog_lookback_days=result["backlog_lookback_days"],
        day_start=result["day_start"],
        day_end=result["day_end"],
        archived_rows=result["archived_rows"],
        purged_rows=result["purged_rows"],
        files_written=result["files_written"],
        error=result["error"],
    )


@router.get("/db/tables")
async def get_db_table_stats(
    request: Request,
    _principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_SYSTEM_STATUS)),
    ],
) -> DbStatsResponse:
    """Return the most recent per-table row-count snapshot (Cluster B).

    Args:
        request: FastAPI request — provides app state + REST tracker.
        _principal: Authenticated caller (permission gate enforced at
            dependency resolution; the bound value is discarded
            because the metrics are user-agnostic).

    Returns:
        :class:`DbStatsResponse` envelope wrapping the latest
        :class:`DbStatsData` payload — per-table counters from the
        most recent sampler tick.

    Raises:
        HTTPException: 503 when the snapshotter is missing (helper
            failed to attach), disabled (operator opt-out), or has
            not completed a sample yet (cold-start window). Only the
            cold-start case carries a ``Retry-After`` header — the
            other two states are not transient.
    """
    snapshotter = _resolve_db_stats_snapshotter(request)
    if snapshotter.disabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_DB_STATS_DISABLED_DETAIL,
        )
    snapshot = snapshotter.latest_snapshot
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_DB_STATS_NOT_YET_RUN_DETAIL,
            headers={"Retry-After": str(int(snapshotter.interval_seconds))},
        )
    tracker: SequenceTracker = request.app.state.rest_tracker
    payload_sid, payload_seq, payload_ts, payload_pid = _next_provenance(tracker)
    payload = _build_db_stats_data(
        snapshot,
        session_id=payload_sid,
        sequence_id=payload_seq,
        public_id=payload_pid,
        timestamp=payload_ts,
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(tracker)
    return DbStatsResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=payload,
    )


def _resolve_db_stats_snapshotter(request: Request) -> DbStatsSnapshotter:
    """Pull the DB-stats snapshotter singleton or raise 503.

    The lifespan startup hook in :mod:`snapper.server.app` assigns
    ``app.state.db_stats_snapshotter`` ONLY after a successful
    :meth:`DbStatsSnapshotter.start` call (B22 — no half-initialized
    object can bypass the route layer's 503 fallback).

    Args:
        request: FastAPI request whose ``app.state`` holds the
            singleton (or pre-set ``None`` on startup failure).

    Returns:
        The attached :class:`DbStatsSnapshotter` instance.

    Raises:
        HTTPException: 503 when no snapshotter is attached.
    """
    snapshotter: DbStatsSnapshotter | None = getattr(
        request.app.state, "db_stats_snapshotter", None
    )
    if snapshotter is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_DB_STATS_UNAVAILABLE_DETAIL,
        )
    return snapshotter


def _build_db_stats_data(
    snapshot: DbStatsSnapshot,
    *,
    session_id: str,
    sequence_id: int,
    public_id: str,
    timestamp: datetime,
) -> DbStatsData:
    """Map an in-memory :class:`DbStatsSnapshot` to the wire schema.

    Args:
        snapshot: Source snapshot captured by the sampler.
        session_id: Envelope provenance — payload session.
        sequence_id: Envelope provenance — payload sequence.
        public_id: Envelope provenance — payload UUID7.
        timestamp: Envelope provenance — payload timestamp.

    Returns:
        Wire-strict :class:`DbStatsData` payload.
    """
    return DbStatsData(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        snapshot_started_at=snapshot.snapshot_started_at,
        snapshot_completed_at=snapshot.snapshot_completed_at,
        interval_seconds=snapshot.interval_seconds,
        tables=[_build_table_stats_item(row) for row in snapshot.tables],
    )


def _build_table_stats_item(row: TableStats) -> TableStatsItem:
    """Map an in-memory :class:`TableStats` row to the wire schema.

    Args:
        row: Source per-table counters captured by the sampler.

    Returns:
        Wire-strict :class:`TableStatsItem` body.
    """
    return TableStatsItem(
        table=row.table,
        table_kind=row.table_kind,
        total=row.total,
        current=row.current,
        closed=row.closed,
        archivable=row.archivable,
        is_stale=row.is_stale,
        last_sampled_at=row.last_sampled_at,
    )
