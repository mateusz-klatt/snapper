"""TypedDicts describing the in-memory shape of a system-metrics snapshot.

Pure data types with no behavior — the snapshotter writes these into the
ring buffer; the route layer maps them to Pydantic schemas at the API
boundary. Splitting in-memory types from API schemas keeps the sampler
hot-path free of Pydantic validation overhead while preserving strict
typing on the wire.
"""

from datetime import datetime
from typing import Literal
from typing import TypedDict


class ProcessMetrics(TypedDict):
    """Process-level identifying + lifecycle counters."""

    pid: int
    uptime_seconds: float
    status: str
    num_threads: int
    num_fds: int
    num_connections: int


class CpuMetrics(TypedDict):
    """CPU usage + cgroup quota / throttling counters.

    ``cgroup_quota`` and ``cgroup_throttled`` are populated only when
    cgroup detection succeeds; ``None`` on dev hosts without cgroup.
    """

    process_percent: float
    user_time_seconds: float
    system_time_seconds: float
    cgroup_quota_microseconds: int | None
    cgroup_throttled_count: int | None


class MemoryMetrics(TypedDict):
    """Memory usage + container saturation.

    ``python_traced`` and ``native_bytes`` are populated only when
    tracemalloc is active. ``native_bytes = rss - python_traced`` is the
    "native dark matter" diagnostic — bytes held by non-Python heap
    sources (numpy / pandas / zmq / aiosqlite native cache /
    pydantic-core Rust). When tracemalloc is inactive, both fields are
    ``None`` (semantically meaningful — "not measured" vs "measured zero").
    ``cgroup_*`` fields populated only when cgroup detection succeeds.
    ``saturation_pct`` is ``cgroup_current / cgroup_limit`` when both
    available; ``None`` otherwise.
    """

    rss_bytes: int
    rss_peak_bytes: int
    vms_bytes: int
    python_traced_bytes: int | None
    native_bytes: int | None
    cgroup_limit_bytes: int | None
    cgroup_current_bytes: int | None
    saturation_pct: float | None


class AsyncioMetrics(TypedDict):
    """asyncio task counts via ``asyncio.all_tasks()`` snapshot."""

    active_tasks: int
    pending_tasks: int


class GcMetrics(TypedDict):
    """Garbage-collector counters from ``gc.get_stats()`` + ``gc.get_count()``."""

    collections_per_gen: tuple[int, int, int]
    uncollectable: int
    current_objects: int


class LimitsMetrics(TypedDict):
    """Process resource limits via ``resource.getrlimit``."""

    rlimit_nproc: int
    rlimit_nofile: int
    rlimit_as_bytes: int


class SaturationMetrics(TypedDict):
    """Saturation as percentage of resource limit.

    The gradient operators actually need to see (vs raw counts) — % of
    exhaustion. ``threads_pct = num_threads / rlimit_nproc``,
    ``fds_pct = num_fds / rlimit_nofile``. ``None`` for any field whose
    denominator is unlimited (RLIM_INFINITY) or zero.
    """

    threads_pct: float | None
    fds_pct: float | None


class DbInternalMetrics(TypedDict):
    """SQLAlchemy / aiosqlite pool counters.

    ``aiosqlite_live_connections`` is the value of
    ``len(_live_aiosqlite_connections)`` at sample time — atomic read,
    no iteration. Each live aiosqlite Connection corresponds to one OS
    thread under NullPool semantics; this metric is the diagnostic for
    aiosqlite thread leaks.
    ``pool_size`` and ``pool_checked_out`` are populated when the engine
    uses a queue pool (PG / DB_POOL_MODE=queue); ``None`` under NullPool.
    """

    aiosqlite_live_connections: int
    pool_size: int | None
    pool_checked_out: int | None


class SystemMetricsSnapshot(TypedDict):
    """One sampled snapshot held in the ring buffer.

    ``bus_time`` is the sampler's UTC timestamp at sample creation —
    used by the history endpoint to filter by ``since``/``until`` ISO
    timestamps. ``cgroup_version`` is ``"v1"`` / ``"v2"`` when detection
    succeeds; ``None`` on dev hosts.
    """

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
