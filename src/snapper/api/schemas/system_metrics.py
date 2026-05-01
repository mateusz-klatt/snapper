"""Pydantic schemas for the ``/api/metrics/system*`` REST surface.

Mirrors the in-memory :mod:`snapper.application.system_metrics.snapshot_types`
TypedDicts as wire-strict Pydantic bodies. Splitting the in-memory types
from the API schemas keeps the sampler hot-path free of Pydantic
validation overhead while preserving strict typing on the wire.

Field semantics (units, ``None`` cases, cross-field invariants) match
the TypedDicts; see :mod:`snapper.application.system_metrics.snapshot_types`
for the authoritative descriptions.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema


class ProcessMetrics(StrictBody):
    """Process-level identifying + lifecycle counters.

    Attributes:
        pid: Process ID.
        uptime_seconds: Seconds since :class:`SystemMetricsSnapshotter`
            instance creation (monotonic clock).
        status: psutil process status (``"running"``, ``"sleeping"`` etc.).
        num_threads: Current thread count.
        num_fds: Open file descriptor count.
        num_connections: Open network connection count (TCP/UDP).
    """

    pid: int
    uptime_seconds: float
    status: str
    num_threads: int
    num_fds: int
    num_connections: int


class CpuMetrics(StrictBody):
    """CPU usage + cgroup quota / throttling counters.

    Attributes:
        process_percent: ``psutil.Process.cpu_percent`` reading at sample
            time.
        user_time_seconds: Cumulative user-mode CPU time.
        system_time_seconds: Cumulative system-mode CPU time.
        cgroup_quota_microseconds: cgroup CPU quota microseconds per
            period; ``None`` on hosts without cgroup or when unlimited.
        cgroup_throttled_count: Cumulative throttle event count from
            ``cpu.stat`` ``nr_throttled``; ``None`` when cgroup absent.
    """

    process_percent: float
    user_time_seconds: float
    system_time_seconds: float
    cgroup_quota_microseconds: int | None
    cgroup_throttled_count: int | None


class MemoryMetrics(StrictBody):
    """Memory usage + container saturation.

    ``python_traced_bytes`` and ``native_bytes`` are populated only when
    tracemalloc is active. ``native_bytes = max(0, rss - python_traced)``
    is the "native dark matter" diagnostic. When tracemalloc is inactive
    both are ``None`` (semantically meaningful — "not measured").
    """

    rss_bytes: int
    rss_peak_bytes: int
    vms_bytes: int
    python_traced_bytes: int | None
    native_bytes: int | None
    cgroup_limit_bytes: int | None
    cgroup_current_bytes: int | None
    saturation_pct: float | None


class AsyncioMetrics(StrictBody):
    """asyncio task counts via ``asyncio.all_tasks()`` snapshot."""

    active_tasks: int
    pending_tasks: int


class GcMetrics(StrictBody):
    """Garbage-collector counters from ``gc.get_stats`` + ``gc.get_count``.

    The three generation counters are flat integer fields (rather than a
    ``tuple[int, int, int]``) because the JSON-schema-driven frontend +
    iOS code generators erase fixed-length tuple constraints into
    ``unknown[]`` / ``[AnyCodable]``, losing the three-int contract on
    the wire client. Flat fields preserve the contract end-to-end.

    Attributes:
        collections_gen0: Cumulative GC collection count for generation 0.
        collections_gen1: Cumulative GC collection count for generation 1.
        collections_gen2: Cumulative GC collection count for generation 2.
        uncollectable: Cumulative count of objects that GC could not free.
        current_objects: Sum of ``gc.get_count()`` across all generations
            at sample time.
    """

    collections_gen0: int
    collections_gen1: int
    collections_gen2: int
    uncollectable: int
    current_objects: int


class LimitsMetrics(StrictBody):
    """Process resource limits via ``resource.getrlimit``."""

    rlimit_nproc: int
    rlimit_nofile: int
    rlimit_as_bytes: int


class SaturationMetrics(StrictBody):
    """Saturation as percentage of resource limit (0.0-1.0).

    ``None`` for any field whose denominator is unlimited
    (``RLIM_INFINITY``) or zero.
    """

    threads_pct: float | None
    fds_pct: float | None


class DbInternalMetrics(StrictBody):
    """SQLAlchemy / aiosqlite pool counters.

    ``aiosqlite_live_connections`` is the value of
    ``len(_live_aiosqlite_connections)`` at sample time — atomic read,
    no iteration. Each live aiosqlite Connection corresponds to one OS
    thread under NullPool semantics.
    """

    aiosqlite_live_connections: int
    pool_size: int | None
    pool_checked_out: int | None


class SystemMetricsData(StrictDataSchema[Literal["system_metrics"]]):
    """Latest sampled snapshot returned by ``GET /api/metrics/system``.

    ``bus_time`` is the sampler's UTC timestamp at sample creation
    (distinct from the envelope ``timestamp`` which is the response
    minting time).

    Attributes:
        type: Payload item type discriminator.
        bus_time: UTC timestamp of the underlying sample.
        process: Process-level counters.
        cpu: CPU usage + cgroup throttle counters.
        memory: Memory + container saturation.
        asyncio: asyncio task counts.
        gc: Garbage-collector counters.
        limits: ``rlimit`` soft limits.
        saturation: % toward exhaustion for thread / fd budgets.
        db_internal: SQLAlchemy / aiosqlite pool counters.
        tracemalloc_active: ``True`` iff Python tracemalloc is currently
            tracing.
        cgroup_version: ``"v1"`` / ``"v2"`` when detected; ``None`` on
            hosts without cgroup.
    """

    type: Literal["system_metrics"] = "system_metrics"
    bus_time: datetime
    process: ProcessMetrics
    cpu: CpuMetrics
    memory: MemoryMetrics
    asyncio: AsyncioMetrics
    gc: GcMetrics
    limits: LimitsMetrics
    saturation: SaturationMetrics
    db_internal: DbInternalMetrics
    tracemalloc_active: bool
    cgroup_version: Literal["v1", "v2"] | None


class SystemMetricsHistoryItem(StrictDataSchema[Literal["system_metrics_history_item"]]):
    """One snapshot row in the history list response.

    Same field set as :class:`SystemMetricsData`, separate type tag so
    consumers can dispatch on the discriminator.
    """

    type: Literal["system_metrics_history_item"] = "system_metrics_history_item"
    bus_time: datetime
    process: ProcessMetrics
    cpu: CpuMetrics
    memory: MemoryMetrics
    asyncio: AsyncioMetrics
    gc: GcMetrics
    limits: LimitsMetrics
    saturation: SaturationMetrics
    db_internal: DbInternalMetrics
    tracemalloc_active: bool
    cgroup_version: Literal["v1", "v2"] | None


class TracemallocState(StrictBody):
    """Operator-facing tracemalloc state after an arm / disarm call.

    Attributes:
        active: Whether tracemalloc is tracing after this call.
        requested_duration_seconds: For arm calls, the clamped auto-stop
            deadline applied (default 600, max 3600). ``None`` for
            disarm calls.
    """

    active: bool
    requested_duration_seconds: float | None


class SystemMetricsResponse(PayloadResponse[Literal["system_metrics_response"], SystemMetricsData]):
    """Envelope-wrapped response for ``GET /api/metrics/system``."""

    type: Literal["system_metrics_response"] = "system_metrics_response"


class SystemMetricsHistoryResponse(
    PayloadListResponse[Literal["system_metrics_history_response"], SystemMetricsHistoryItem]
):
    """Envelope-wrapped response for ``GET /api/metrics/system/history``."""

    type: Literal["system_metrics_history_response"] = "system_metrics_history_response"


class TracemallocStateResponse(
    PayloadResponse[Literal["tracemalloc_state_response"], TracemallocState]
):
    """Envelope-wrapped response for the tracemalloc arm / disarm routes."""

    type: Literal["tracemalloc_state_response"] = "tracemalloc_state_response"
