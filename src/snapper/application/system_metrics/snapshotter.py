"""SystemMetricsSnapshotter singleton + sampler task.

Holds the :class:`MetricsRingBuffer` + :class:`TracemallocController`,
runs an asyncio sample loop on a configurable interval (default 5s),
writes one :class:`SystemMetricsSnapshot` per tick to the buffer.

Lifecycle: :meth:`start` takes ONE eager synchronous sample BEFORE
returning, so a successfully-started singleton always has >=1 snapshot
in the buffer when the first request arrives. The sample loop runs
after. :meth:`stop` cancels + drains the loop task, then awaits the
tracemalloc controller's stop.

Sampler hot-path invariants:

  * ``_live_aiosqlite_connections`` is read via atomic ``len(...)``
    ONLY — never iterate, never copy. The dict is mutated by
    SQLAlchemy connect/close hooks; iteration would race.
  * Cgroup file reads are wrapped in ``OSError`` handling so a host
    without cgroup degrades to ``cgroup_*=None`` instead of crashing
    the loop.
  * Tracemalloc reads ``traced_bytes()`` cheaply when active; ``None``
    when inactive (no ``tracemalloc.start()`` overhead paid).

Configuration: ``SYSTEM_METRICS_INTERVAL_SECONDS`` (default 5),
``SYSTEM_METRICS_HISTORY_CAP`` (default 17280), and disk-pressure
threshold settings read directly from ``os.environ.get(...)`` in
:meth:`__init__`.
"""

import asyncio
import contextlib
import gc
import logging
import os
import shutil
from datetime import UTC
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Final
from typing import Protocol
from uuid import uuid7

import psutil
from sqlalchemy.ext.asyncio import AsyncEngine

from snapper.application.portfolio.fx_conversion_shadow import fx_shadow_pin_metrics
from snapper.application.system_metrics.cgroup import CgroupReading
from snapper.application.system_metrics.cgroup import read_cgroup
from snapper.application.system_metrics.ring_buffer import DEFAULT_HISTORY_CAP
from snapper.application.system_metrics.ring_buffer import MetricsRingBuffer
from snapper.application.system_metrics.snapshot_types import AsyncioMetrics
from snapper.application.system_metrics.snapshot_types import CpuMetrics
from snapper.application.system_metrics.snapshot_types import DbInternalMetrics
from snapper.application.system_metrics.snapshot_types import DiskMetrics
from snapper.application.system_metrics.snapshot_types import GcMetrics
from snapper.application.system_metrics.snapshot_types import LimitsMetrics
from snapper.application.system_metrics.snapshot_types import MemoryMetrics
from snapper.application.system_metrics.snapshot_types import ProcessMetrics
from snapper.application.system_metrics.snapshot_types import SaturationMetrics
from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot
from snapper.application.system_metrics.tracemalloc_controller import TracemallocController
from snapper.core.json_types import JsonObject
from snapper.core.types import HealthStatus
from snapper.core.types import HealthStatusEnum
from snapper.data.repository import _live_aiosqlite_connections
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.topics.builders import heartbeat_topic

logger = logging.getLogger(__name__)


class _ResourceModule(Protocol):
    """Subset of the POSIX ``resource`` module used by the sampler."""

    RLIMIT_NPROC: int
    RLIMIT_NOFILE: int
    RLIMIT_AS: int
    RLIM_INFINITY: int

    def getrlimit(self, resource: int) -> tuple[int, int]:
        """Return soft and hard process limits for the given resource."""


class _HeartbeatSequenceTracker(Protocol):
    """Subset of ``SequenceTracker`` needed for heartbeat provenance."""

    @property
    def session_id(self) -> str:
        """Return the publisher session id."""

    def next_sequence(self, stream: str) -> int:
        """Return the next transport sequence for a topic."""


class _HeartbeatPublisher(Protocol):
    """Subset of ``MessagePublisher`` needed by the snapshotter."""

    @property
    def tracker(self) -> _HeartbeatSequenceTracker:
        """Return the publisher's shared sequence tracker."""

    async def send(self, stream_key: str, data: HeartbeatData) -> None:
        """Send one complete heartbeat frame."""


_resource: _ResourceModule | None = None
try:
    import resource as _resource_module

    _resource = _resource_module
except ImportError:
    _resource = None

DEFAULT_INTERVAL_SECONDS: Final = 5.0
DEFAULT_DISK_FREE_WARN_BYTES: Final = 20 * 1024**3
DEFAULT_DISK_FREE_CRIT_BYTES: Final = 10 * 1024**3
DEFAULT_DISK_MOUNT_PATH: Final = "/"
_INTERVAL_ENV_VAR: Final = "SYSTEM_METRICS_INTERVAL_SECONDS"
_HISTORY_CAP_ENV_VAR: Final = "SYSTEM_METRICS_HISTORY_CAP"
SYSTEM_METRICS_DISK_FREE_WARN_BYTES: Final = "SYSTEM_METRICS_DISK_FREE_WARN_BYTES"
SYSTEM_METRICS_DISK_FREE_CRIT_BYTES: Final = "SYSTEM_METRICS_DISK_FREE_CRIT_BYTES"
SYSTEM_METRICS_DISK_MOUNT_PATH: Final = "SYSTEM_METRICS_DISK_MOUNT_PATH"
_DISK_HEARTBEAT_COMPONENT: Final = "host"
_DISK_HEARTBEAT_NAME: Final = "disk"
ENV_VARS: Final[frozenset[str]] = frozenset(
    {
        _INTERVAL_ENV_VAR,
        _HISTORY_CAP_ENV_VAR,
        SYSTEM_METRICS_DISK_FREE_WARN_BYTES,
        SYSTEM_METRICS_DISK_FREE_CRIT_BYTES,
        SYSTEM_METRICS_DISK_MOUNT_PATH,
    }
)
"""Public allowlist of env vars this module reads via ``os.environ``.

Consumed by :mod:`snapper.config.env_contract` to validate ``.env`` keys
against the union of every subsystem's contract.
"""


def _resolve_interval(env_value: str | None) -> float:
    """Coerce the ``SYSTEM_METRICS_INTERVAL_SECONDS`` env var to a positive float.

    Empty / unset / unparseable / non-positive values fall back to the
    default 5.0s.
    """
    if env_value is None or env_value.strip() == "":
        return DEFAULT_INTERVAL_SECONDS
    try:
        value = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if value <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return value


def _resolve_history_cap(env_value: str | None) -> int:
    """Coerce the ``SYSTEM_METRICS_HISTORY_CAP`` env var to a positive int.

    Empty / unset / unparseable / non-positive values fall back to the
    default 17280 (24h at 5s).
    """
    if env_value is None or env_value.strip() == "":
        return DEFAULT_HISTORY_CAP
    try:
        value = int(env_value)
    except ValueError:
        return DEFAULT_HISTORY_CAP
    if value <= 0:
        return DEFAULT_HISTORY_CAP
    return value


def _resolve_positive_bytes(env_value: str | None, default: int) -> int:
    """Coerce a byte-count env var to a positive integer."""
    if env_value is None or env_value.strip() == "":
        return default
    try:
        value = int(env_value)
    except ValueError:
        return default
    if value <= 0:
        return default
    return value


def _resolve_disk_mount_path(env_value: str | None) -> str:
    """Coerce the disk mount path env var, defaulting to host root."""
    if env_value is None:
        return DEFAULT_DISK_MOUNT_PATH
    value = env_value.strip()
    if value == "":
        return DEFAULT_DISK_MOUNT_PATH
    return value


class SystemMetricsSnapshotter:
    """Process-level metrics sampler + ring-buffer history.

    Singleton owned by the FastAPI app via lifespan hook. Routes pull
    the latest snapshot via :meth:`current_snapshot` and the windowed
    history via :meth:`history`.
    """

    def __init__(
        self,
        *,
        interval_seconds: float | None = None,
        history_cap: int | None = None,
        disk_free_warn_bytes: int | None = None,
        disk_free_crit_bytes: int | None = None,
        disk_mount_path: str | None = None,
        process: psutil.Process | None = None,
        tracemalloc_controller: TracemallocController | None = None,
        history_buffer: MetricsRingBuffer | None = None,
        msg_publisher: _HeartbeatPublisher | None = None,
        engine: AsyncEngine | None = None,
    ) -> None:
        """Wire dependencies.

        Args:
            interval_seconds: Sample interval. ``None`` reads
                ``SYSTEM_METRICS_INTERVAL_SECONDS`` env var or the
                default 5.0s.
            history_cap: Ring buffer cap. ``None`` reads
                ``SYSTEM_METRICS_HISTORY_CAP`` env var or the default
                17280 (24h at 5s).
            disk_free_warn_bytes: Warning threshold for free bytes on
                the data partition. ``None`` reads
                ``SYSTEM_METRICS_DISK_FREE_WARN_BYTES`` or defaults to
                20 GiB.
            disk_free_crit_bytes: Critical threshold for free bytes on
                the data partition. ``None`` reads
                ``SYSTEM_METRICS_DISK_FREE_CRIT_BYTES`` or defaults to
                10 GiB.
            disk_mount_path: Mount path sampled by ``shutil.disk_usage``.
                ``None`` reads ``SYSTEM_METRICS_DISK_MOUNT_PATH`` or
                defaults to ``/``.
            process: psutil.Process handle. ``None`` uses the current
                process.
            tracemalloc_controller: Override for tests; ``None``
                builds a fresh one.
            history_buffer: Override for tests; ``None`` builds one
                with ``history_cap``.
            msg_publisher: Optional ZMQ publisher for host disk heartbeat
                frames. ``None`` disables bus publication while keeping
                local metrics and logs intact.
            engine: The app's primary async DB engine, injected so the
                DB-pool tile reports THAT pool's utilization authoritatively
                rather than guessing from the set of all live engines.
                ``None`` (tests / metrics-only) leaves pool metrics ``None``.
        """
        if interval_seconds is None:
            interval_seconds = _resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        if history_cap is None:
            history_cap = _resolve_history_cap(os.environ.get(_HISTORY_CAP_ENV_VAR))
        if disk_free_warn_bytes is None:
            disk_free_warn_bytes = _resolve_positive_bytes(
                os.environ.get(SYSTEM_METRICS_DISK_FREE_WARN_BYTES),
                DEFAULT_DISK_FREE_WARN_BYTES,
            )
        if disk_free_crit_bytes is None:
            disk_free_crit_bytes = _resolve_positive_bytes(
                os.environ.get(SYSTEM_METRICS_DISK_FREE_CRIT_BYTES),
                DEFAULT_DISK_FREE_CRIT_BYTES,
            )
        if disk_mount_path is None:
            disk_mount_path = _resolve_disk_mount_path(
                os.environ.get(SYSTEM_METRICS_DISK_MOUNT_PATH)
            )
        self._interval_seconds = interval_seconds
        self._disk_free_warn_bytes = disk_free_warn_bytes
        self._disk_free_crit_bytes = disk_free_crit_bytes
        self._disk_mount_path = disk_mount_path
        self._process = process or psutil.Process()
        self._tracemalloc = tracemalloc_controller or TracemallocController()
        self._history = history_buffer or MetricsRingBuffer(maxlen=history_cap)
        self._msg_publisher = msg_publisher
        self._engine = engine
        self._disk_heartbeat_sequence = 0
        self._sampler_task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._start_time = monotonic()

    @property
    def interval_seconds(self) -> float:
        """Configured sample interval.

        Returns:
            Seconds between sampler ticks.
        """
        return self._interval_seconds

    @property
    def tracemalloc(self) -> TracemallocController:
        """Expose the tracemalloc controller for the route layer.

        Returns:
            The :class:`TracemallocController` instance owned by the
            snapshotter; route handlers call ``start()`` / ``stop()``
            on it to arm or disarm tracemalloc tracing.
        """
        return self._tracemalloc

    async def start(self) -> None:
        """Take one eager sample THEN spawn the sampler loop.

        Eager sample is the cold-start contract: routes can return the
        latest snapshot immediately after lifespan startup completes
        without racing the first 5s tick.
        """
        snapshot = self._build_snapshot()
        await self._history.append(snapshot)
        self._log_disk_pressure(snapshot["disk"])
        await self._publish_disk_heartbeat(snapshot["disk"])
        self._stopping.clear()
        self._sampler_task = asyncio.create_task(self._sampler_loop())

    async def stop(self) -> None:
        """Cancel + await the sampler loop, then disarm tracemalloc."""
        self._stopping.set()
        task = self._sampler_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._sampler_task = None
        await self._tracemalloc.stop()

    async def current_snapshot(self) -> SystemMetricsSnapshot | None:
        """Return the latest snapshot in the buffer, or ``None`` if empty.

        Returns:
            Most recent :class:`SystemMetricsSnapshot`, or ``None`` if
            the snapshotter hasn't run its eager-sample yet.
        """
        return await self._history.latest()

    async def history(
        self,
        since: datetime,
        until: datetime,
        limit: int,
    ) -> list[SystemMetricsSnapshot]:
        """Slice the history buffer by ``[since, until]`` window + ``limit``.

        Args:
            since: Inclusive lower bound on snapshot ``bus_time``.
            until: Inclusive upper bound on snapshot ``bus_time``.
            limit: Maximum snapshots returned (most-recent N within the
                window).

        Returns:
            Filtered list, possibly empty, ordered oldest-first.
        """
        return await self._history.slice(since, until, limit)

    async def _sampler_loop(self) -> None:
        """Sleep ``interval`` then sample; repeat until cancelled.

        :class:`asyncio.CancelledError` propagates from ``asyncio.sleep``
        naturally — no explicit catch needed; the task transitions to
        CANCELLED state for the caller's ``stop()`` to observe.
        """
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            snapshot = self._build_snapshot()
            await self._history.append(snapshot)
            self._log_disk_pressure(snapshot["disk"])
            await self._publish_disk_heartbeat(snapshot["disk"])

    async def _publish_disk_heartbeat(self, disk: DiskMetrics) -> None:
        """Publish the host/disk heartbeat into the existing alert pipeline."""
        publisher = self._msg_publisher
        if publisher is None:
            return
        topic = heartbeat_topic(_DISK_HEARTBEAT_COMPONENT, _DISK_HEARTBEAT_NAME)
        try:
            tracker = publisher.tracker
            self._disk_heartbeat_sequence += 1
            meta: JsonObject = {
                "mount_path": disk["mount_path"],
                "free_bytes": disk["free_bytes"],
                "total_bytes": disk["total_bytes"],
                "percent_used": disk["percent_used"],
                "disk_low": disk["disk_low"],
                "disk_critical": disk["disk_critical"],
            }
            frame = HeartbeatData(
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                component=f"{_DISK_HEARTBEAT_COMPONENT}.{_DISK_HEARTBEAT_NAME}",
                sequence=self._disk_heartbeat_sequence,
                status=disk["status"],
                lag_ms=0,
                meta=meta,
            )
            await publisher.send(topic, frame)
        except Exception:
            logger.exception("disk heartbeat publish failed")

    def _build_snapshot(self) -> SystemMetricsSnapshot:
        """Sample every metric group + assemble the snapshot.

        Synchronous because every read is a /proc / cgroup / Python
        stdlib call that completes in microseconds; offloading to a
        thread would add overhead without latency reduction. The
        sampler loop awaits the surrounding ``asyncio.sleep``, so the
        event loop stays responsive between ticks.
        """
        cgroup_reading, cgroup_version = read_cgroup()
        process_metrics = self._sample_process_metrics()
        cpu_metrics = self._sample_cpu_metrics(cgroup_reading)
        memory_metrics = self._sample_memory_metrics(cgroup_reading)
        asyncio_metrics = self._sample_asyncio_metrics()
        gc_metrics = self._sample_gc_metrics()
        limits_metrics = self._sample_limits_metrics()
        saturation_metrics = self._sample_saturation_metrics(
            process_metrics=process_metrics,
            limits_metrics=limits_metrics,
        )
        db_internal_metrics = self._sample_db_internal_metrics()
        disk_metrics = self._sample_disk_metrics()
        fx_metrics = fx_shadow_pin_metrics()
        return SystemMetricsSnapshot(
            bus_time=datetime.now(UTC),
            process=process_metrics,
            cpu=cpu_metrics,
            memory=memory_metrics,
            asyncio=asyncio_metrics,
            gc=gc_metrics,
            limits=limits_metrics,
            saturation=saturation_metrics,
            db_internal=db_internal_metrics,
            disk=disk_metrics,
            fx_shadow_pins={
                "creation": fx_metrics.creation,
                "reuse": fx_metrics.reuse,
                "conflict": fx_metrics.conflict,
                "upgrade_required": fx_metrics.upgrade_required,
                "mismatch": fx_metrics.mismatch,
                "failure": fx_metrics.failure,
                "dropped": fx_metrics.dropped,
            },
            tracemalloc_active=self._tracemalloc.is_active(),
            cgroup_version=cgroup_version,
        )

    def _sample_process_metrics(self) -> ProcessMetrics:
        """psutil-driven process counters."""
        with self._process.oneshot():
            num_connections = len(self._process.net_connections(kind="inet"))
            return ProcessMetrics(
                pid=self._process.pid,
                uptime_seconds=monotonic() - self._start_time,
                status=self._process.status(),
                num_threads=self._process.num_threads(),
                num_fds=self._sample_num_fds(),
                num_connections=num_connections,
            )

    def _sample_num_fds(self) -> int:
        """Return POSIX file descriptor count, or zero when psutil lacks it."""
        try:
            return int(self._process.num_fds())
        except AttributeError:
            return 0

    def _sample_cpu_metrics(self, cgroup_reading: CgroupReading | None) -> CpuMetrics:
        """CPU usage + cgroup quota / throttled counters.

        ``cpu_percent(interval=None)`` is non-blocking — returns the
        delta since the last call. The first call after process start
        returns 0.0 (psutil documented behavior); subsequent calls
        return real percentages.
        """
        with self._process.oneshot():
            cpu_times = self._process.cpu_times()
            percent = self._process.cpu_percent(interval=None)
        cgroup_quota: int | None = None
        cgroup_throttled: int | None = None
        if cgroup_reading is not None:
            cgroup_quota_raw = cgroup_reading["cpu_quota_microseconds"]
            cgroup_quota = None if cgroup_quota_raw == -1 else cgroup_quota_raw
            cgroup_throttled = cgroup_reading["cpu_throttled_count"]
        return CpuMetrics(
            process_percent=percent,
            user_time_seconds=cpu_times.user,
            system_time_seconds=cpu_times.system,
            cgroup_quota_microseconds=cgroup_quota,
            cgroup_throttled_count=cgroup_throttled,
        )

    def _sample_memory_metrics(self, cgroup_reading: CgroupReading | None) -> MemoryMetrics:
        """Memory + container saturation %.

        ``rss_peak`` from ``/proc/self/status`` ``VmHWM`` field —
        psutil's ``memory_info()`` does NOT expose this. Fall back to
        ``rss`` if /proc is unavailable (non-Linux).
        """
        with self._process.oneshot():
            mem = self._process.memory_info()
        rss = mem.rss
        rss_peak = self._read_vm_hwm() or rss
        traced = self._tracemalloc.traced_bytes()
        native_bytes: int | None = None
        if traced is not None:
            native_bytes = max(0, rss - traced)
        cgroup_limit: int | None = None
        cgroup_current: int | None = None
        saturation_pct: float | None = None
        if cgroup_reading is not None:
            cgroup_limit_raw = cgroup_reading["memory_max_bytes"]
            cgroup_limit = None if cgroup_limit_raw == -1 else cgroup_limit_raw
            cgroup_current = cgroup_reading["memory_current_bytes"]
            if cgroup_limit is not None and cgroup_current is not None and cgroup_limit > 0:
                saturation_pct = cgroup_current / cgroup_limit
        return MemoryMetrics(
            rss_bytes=rss,
            rss_peak_bytes=rss_peak,
            vms_bytes=mem.vms,
            python_traced_bytes=traced,
            native_bytes=native_bytes,
            cgroup_limit_bytes=cgroup_limit,
            cgroup_current_bytes=cgroup_current,
            saturation_pct=saturation_pct,
        )

    @staticmethod
    def _read_vm_hwm() -> int | None:
        """Parse ``VmHWM`` (peak resident set size) from ``/proc/self/status``.

        Linux-specific. Returns ``None`` on non-Linux dev hosts.
        Format: ``"VmHWM:    12345 kB"``.
        """
        try:
            text = (Path("/proc/self/status")).read_text(encoding="ascii")
        except OSError:
            return None
        for line in text.splitlines():
            if line.startswith("VmHWM:"):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return int(parts[1]) * 1024
                    except ValueError:
                        return None
        return None

    @staticmethod
    def _sample_asyncio_metrics() -> AsyncioMetrics:
        """Asyncio task counts via ``asyncio.all_tasks()`` snapshot."""
        try:
            tasks = asyncio.all_tasks()
        except RuntimeError:
            return AsyncioMetrics(active_tasks=0, pending_tasks=0)
        pending = sum(1 for t in tasks if not t.done())
        return AsyncioMetrics(
            active_tasks=len(tasks),
            pending_tasks=pending,
        )

    @staticmethod
    def _sample_gc_metrics() -> GcMetrics:
        """Gc counters from ``gc.get_stats()`` + ``gc.get_count()``."""
        stats = gc.get_stats()
        if len(stats) >= 3:
            collections = (
                int(stats[0].get("collections", 0)),
                int(stats[1].get("collections", 0)),
                int(stats[2].get("collections", 0)),
            )
            uncollectable = sum(int(s.get("uncollectable", 0)) for s in stats)
        else:
            collections = (0, 0, 0)
            uncollectable = 0
        current = sum(gc.get_count())
        return GcMetrics(
            collections_per_gen=collections,
            uncollectable=uncollectable,
            current_objects=current,
        )

    @staticmethod
    def _sample_limits_metrics() -> LimitsMetrics:
        """Resource limits via ``resource.getrlimit``.

        Returns the soft limit (``resource.getrlimit`` returns
        ``(soft, hard)``). The :mod:`resource` stdlib module is
        POSIX-only; the module-level guarded import sets
        :data:`_resource` to ``None`` on Windows so developer tooling
        can import the snapshotter without crashing. Production target
        is Linux; the ``_resource is None`` branch returns
        zero-defaulted limits.
        """
        if _resource is None:
            return LimitsMetrics(rlimit_nproc=0, rlimit_nofile=0, rlimit_as_bytes=0)
        nproc = _resource.getrlimit(_resource.RLIMIT_NPROC)
        nofile = _resource.getrlimit(_resource.RLIMIT_NOFILE)
        as_ = _resource.getrlimit(_resource.RLIMIT_AS)
        return LimitsMetrics(
            rlimit_nproc=nproc[0],
            rlimit_nofile=nofile[0],
            rlimit_as_bytes=as_[0],
        )

    @staticmethod
    def _sample_saturation_metrics(
        *,
        process_metrics: ProcessMetrics,
        limits_metrics: LimitsMetrics,
    ) -> SaturationMetrics:
        """Saturation as % of resource limit.

        ``RLIM_INFINITY`` (-1) collapses to ``None`` — division by
        unlimited has no operator-meaningful "%". On Windows the
        module-level :data:`_resource` is ``None``; the static
        ``RLIM_INFINITY`` constant is unavailable, so both saturation
        fields collapse to ``None``.
        """
        if _resource is None:
            return SaturationMetrics(threads_pct=None, fds_pct=None)
        threads_pct: float | None = None
        if limits_metrics["rlimit_nproc"] not in (_resource.RLIM_INFINITY, 0):
            threads_pct = process_metrics["num_threads"] / limits_metrics["rlimit_nproc"]
        fds_pct: float | None = None
        if limits_metrics["rlimit_nofile"] not in (_resource.RLIM_INFINITY, 0):
            fds_pct = process_metrics["num_fds"] / limits_metrics["rlimit_nofile"]
        return SaturationMetrics(
            threads_pct=threads_pct,
            fds_pct=fds_pct,
        )

    def _sample_pool_metrics(self) -> tuple[int | None, int | None]:
        """Read ``(pool_size, checked_out)`` from the injected primary pool.

        Reads the INJECTED primary engine's pool authoritatively — no guessing
        among the set of all live engines, so a secondary/replica engine can
        never shadow the primary in the operator tile. A ``QueuePool``
        (asyncpg/Postgres and file-backed SQLite) exposes ``size()`` /
        ``checkedout()``; an in-memory ``StaticPool`` / ``NullPool`` — and the
        no-engine case (tests / metrics-only startup) — does not, so those
        collapse to ``(None, None)``. This is why the prod (Postgres) "DB Pool"
        tile was blank: the previous sampler hardcoded both to ``None``.

        Returns:
            ``(pool_size, checked_out)``, or ``(None, None)`` when no engine is
            injected or its pool exposes no queue-pool counters.
        """
        if self._engine is None:
            return None, None
        pool = self._engine.sync_engine.pool
        size = getattr(pool, "size", None)
        checked_out = getattr(pool, "checkedout", None)
        if callable(size) and callable(checked_out):
            return size(), checked_out()
        return None, None

    def _sample_db_internal_metrics(self) -> DbInternalMetrics:
        """SQLAlchemy + aiosqlite pool counters.

        ``aiosqlite_live_connections`` is read via atomic ``len(...)``
        — DICT iteration would race with SQLAlchemy connect / close
        hooks; it is a SQLite-only tracker that stays ``0`` on Postgres.
        ``pool_size`` / ``pool_checked_out`` come from the injected
        primary engine's queue pool (asyncpg/Postgres and file-backed
        SQLite); an in-memory ``StaticPool`` / ``NullPool`` leaves them
        ``None``.
        """
        live = len(_live_aiosqlite_connections)
        pool_size, pool_checked_out = self._sample_pool_metrics()
        return DbInternalMetrics(
            aiosqlite_live_connections=live,
            pool_size=pool_size,
            pool_checked_out=pool_checked_out,
        )

    def _sample_disk_metrics(self) -> DiskMetrics:
        """Sample free space on the configured data-partition mount."""
        try:
            usage = shutil.disk_usage(self._disk_mount_path)
        except OSError:
            return DiskMetrics(
                mount_path=self._disk_mount_path,
                total_bytes=None,
                used_bytes=None,
                free_bytes=None,
                percent_used=None,
                disk_low=False,
                disk_critical=False,
                status=HealthStatusEnum.WARNING,
            )
        disk_critical = usage.free < self._disk_free_crit_bytes
        disk_low = disk_critical or usage.free < self._disk_free_warn_bytes
        percent_used = usage.used / usage.total * 100 if usage.total > 0 else 0.0
        status: HealthStatus = HealthStatusEnum.HEALTHY
        if disk_critical:
            status = HealthStatusEnum.ERROR
        elif disk_low:
            status = HealthStatusEnum.WARNING
        return DiskMetrics(
            mount_path=self._disk_mount_path,
            total_bytes=usage.total,
            used_bytes=usage.used,
            free_bytes=usage.free,
            percent_used=percent_used,
            disk_low=disk_low,
            disk_critical=disk_critical,
            status=status,
        )

    @staticmethod
    def _log_disk_pressure(disk: DiskMetrics) -> None:
        """Emit structured disk-pressure logs while free space is degraded."""
        if disk["status"] == HealthStatusEnum.ERROR:
            logger.error(
                "disk_critical mount=%s free_bytes=%s total_bytes=%s percent_used=%s",
                disk["mount_path"],
                disk["free_bytes"],
                disk["total_bytes"],
                disk["percent_used"],
            )
        elif disk["status"] == HealthStatusEnum.WARNING:
            logger.warning(
                "disk_low mount=%s free_bytes=%s total_bytes=%s percent_used=%s",
                disk["mount_path"],
                disk["free_bytes"],
                disk["total_bytes"],
                disk["percent_used"],
            )
