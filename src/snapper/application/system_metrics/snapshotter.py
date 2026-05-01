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
``SYSTEM_METRICS_HISTORY_CAP`` (default 17280) read directly from
``os.environ.get(...)`` in :meth:`__init__` — no ``AppSettings``
extension this iteration.
"""

import asyncio
import contextlib
import gc
import os
from datetime import UTC
from datetime import datetime
from pathlib import Path
from time import monotonic
from typing import Any
from typing import Final

import psutil

from snapper.application.system_metrics.cgroup import CgroupReading
from snapper.application.system_metrics.cgroup import read_cgroup
from snapper.application.system_metrics.ring_buffer import DEFAULT_HISTORY_CAP
from snapper.application.system_metrics.ring_buffer import MetricsRingBuffer
from snapper.application.system_metrics.snapshot_types import AsyncioMetrics
from snapper.application.system_metrics.snapshot_types import CpuMetrics
from snapper.application.system_metrics.snapshot_types import DbInternalMetrics
from snapper.application.system_metrics.snapshot_types import GcMetrics
from snapper.application.system_metrics.snapshot_types import LimitsMetrics
from snapper.application.system_metrics.snapshot_types import MemoryMetrics
from snapper.application.system_metrics.snapshot_types import ProcessMetrics
from snapper.application.system_metrics.snapshot_types import SaturationMetrics
from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot
from snapper.application.system_metrics.tracemalloc_controller import TracemallocController
from snapper.data.repository import _live_aiosqlite_connections

_resource: Any = None
try:
    import resource as _resource
except ImportError:
    _resource = None

DEFAULT_INTERVAL_SECONDS: Final = 5.0
_INTERVAL_ENV_VAR: Final = "SYSTEM_METRICS_INTERVAL_SECONDS"
_HISTORY_CAP_ENV_VAR: Final = "SYSTEM_METRICS_HISTORY_CAP"


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
        process: psutil.Process | None = None,
        tracemalloc_controller: TracemallocController | None = None,
        history_buffer: MetricsRingBuffer | None = None,
    ) -> None:
        """Wire dependencies.

        Args:
            interval_seconds: Sample interval. ``None`` reads
                ``SYSTEM_METRICS_INTERVAL_SECONDS`` env var or the
                default 5.0s.
            history_cap: Ring buffer cap. ``None`` reads
                ``SYSTEM_METRICS_HISTORY_CAP`` env var or the default
                17280 (24h at 5s).
            process: psutil.Process handle. ``None`` uses the current
                process.
            tracemalloc_controller: Override for tests; ``None``
                builds a fresh one.
            history_buffer: Override for tests; ``None`` builds one
                with ``history_cap``.
        """
        if interval_seconds is None:
            interval_seconds = _resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        if history_cap is None:
            history_cap = _resolve_history_cap(os.environ.get(_HISTORY_CAP_ENV_VAR))
        self._interval_seconds = interval_seconds
        self._process = process or psutil.Process()
        self._tracemalloc = tracemalloc_controller or TracemallocController()
        self._history = history_buffer or MetricsRingBuffer(maxlen=history_cap)
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

    @staticmethod
    def _sample_db_internal_metrics() -> DbInternalMetrics:
        """SQLAlchemy + aiosqlite pool counters.

        ``aiosqlite_live_connections`` is read via atomic ``len(...)``
        — DICT iteration would race with SQLAlchemy connect / close
        hooks. ``pool_size`` / ``pool_checked_out`` are populated when
        the live engine uses a queue pool (PG / DB_POOL_MODE=queue);
        ``None`` under NullPool (file-backed SQLite default).
        """
        live = len(_live_aiosqlite_connections)
        pool_size: int | None = None
        pool_checked_out: int | None = None
        return DbInternalMetrics(
            aiosqlite_live_connections=live,
            pool_size=pool_size,
            pool_checked_out=pool_checked_out,
        )
