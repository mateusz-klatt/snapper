"""Tests for the system metrics ring buffer."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.system_metrics.ring_buffer import DEFAULT_HISTORY_CAP
from snapper.application.system_metrics.ring_buffer import MetricsRingBuffer
from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot
from snapper.core.types import HealthStatusEnum


class TestRingBuffer:
    """Tests for MetricsRingBuffer."""

    def _snapshot(self, seconds: int) -> SystemMetricsSnapshot:
        bus_time = datetime(2026, 4, 30, 12, 0, tzinfo=UTC) + timedelta(seconds=seconds)
        return SystemMetricsSnapshot(
            bus_time=bus_time,
            process={
                "pid": 1,
                "uptime_seconds": float(seconds),
                "status": "running",
                "num_threads": 1,
                "num_fds": 2,
                "num_connections": 0,
            },
            cpu={
                "process_percent": 0.0,
                "user_time_seconds": 0.0,
                "system_time_seconds": 0.0,
                "cgroup_quota_microseconds": None,
                "cgroup_throttled_count": None,
            },
            memory={
                "rss_bytes": 100,
                "rss_peak_bytes": 100,
                "vms_bytes": 200,
                "python_traced_bytes": None,
                "native_bytes": None,
                "cgroup_limit_bytes": None,
                "cgroup_current_bytes": None,
                "saturation_pct": None,
            },
            asyncio={"active_tasks": 0, "pending_tasks": 0},
            gc={"collections_per_gen": (0, 0, 0), "uncollectable": 0, "current_objects": 0},
            limits={"rlimit_nproc": 100, "rlimit_nofile": 100, "rlimit_as_bytes": 1000},
            saturation={"threads_pct": None, "fds_pct": None},
            db_internal={
                "aiosqlite_live_connections": 0,
                "pool_size": None,
                "pool_checked_out": None,
            },
            disk={
                "mount_path": "/",
                "total_bytes": 1000,
                "used_bytes": 500,
                "free_bytes": 500,
                "percent_used": 50.0,
                "disk_low": False,
                "disk_critical": False,
                "status": HealthStatusEnum.HEALTHY,
            },
            fx_shadow_pins={
                "creation": 0,
                "reuse": 0,
                "conflict": 0,
                "upgrade_required": 0,
                "mismatch": 0,
                "failure": 0,
            },
            tracemalloc_active=False,
            cgroup_version=None,
        )

    async def _append_snapshots(self, buffer: MetricsRingBuffer, count: int) -> None:
        for index in range(count):
            await buffer.append(self._snapshot(index))
            await asyncio.sleep(0)

    async def _read_snapshots(self, buffer: MetricsRingBuffer, count: int) -> None:
        for _index in range(count):
            await buffer.latest()
            await buffer.size()
            await asyncio.sleep(0)

    @pytest.mark.parametrize("maxlen", [0, -1])
    def test_maxlen_must_be_positive(self, maxlen: int) -> None:
        """Covered by test body."""
        with pytest.raises(ValueError, match="maxlen must be positive"):
            MetricsRingBuffer(maxlen=maxlen)

    def test_maxlen_property_defaults_and_overrides(self) -> None:
        """Covered by test body."""
        assert MetricsRingBuffer().maxlen == DEFAULT_HISTORY_CAP
        assert MetricsRingBuffer(maxlen=3).maxlen == 3

    async def test_latest_returns_none_when_empty(self) -> None:
        """Covered by test body."""
        assert await MetricsRingBuffer(maxlen=2).latest() is None

    async def test_append_latest_and_size(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=3)
        first = self._snapshot(1)
        second = self._snapshot(2)

        await buffer.append(first)
        await buffer.append(second)

        assert await buffer.latest() == second
        assert await buffer.size() == 2

    async def test_evicts_oldest_snapshot_at_capacity(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=2)

        await buffer.append(self._snapshot(1))
        await buffer.append(self._snapshot(2))
        await buffer.append(self._snapshot(3))

        latest = await buffer.latest()
        history = await buffer.slice(
            datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
            datetime(2026, 4, 30, 12, 1, tzinfo=UTC),
            10,
        )

        assert latest == self._snapshot(3)
        assert history == [self._snapshot(2), self._snapshot(3)]

    async def test_slice_filters_window_and_clamps_from_end(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=10)
        for seconds in range(6):
            await buffer.append(self._snapshot(seconds))

        matched = await buffer.slice(
            datetime(2026, 4, 30, 12, 0, 1, tzinfo=UTC),
            datetime(2026, 4, 30, 12, 0, 4, tzinfo=UTC),
            2,
        )

        assert matched == [self._snapshot(3), self._snapshot(4)]

    async def test_slice_returns_all_matches_when_within_limit(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=10)
        for seconds in range(3):
            await buffer.append(self._snapshot(seconds))

        matched = await buffer.slice(
            datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
            datetime(2026, 4, 30, 12, 0, 2, tzinfo=UTC),
            10,
        )

        assert matched == [self._snapshot(0), self._snapshot(1), self._snapshot(2)]

    async def test_slice_handles_empty_buffer_and_rejects_non_positive_limit(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=2)
        since = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
        until = datetime(2026, 4, 30, 12, 1, tzinfo=UTC)

        assert await buffer.slice(since, until, 1) == []
        with pytest.raises(ValueError, match="limit must be positive"):
            await buffer.slice(since, until, 0)

    async def test_concurrent_read_and_write_smoke(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=10)

        await asyncio.gather(
            self._append_snapshots(buffer, 20),
            self._read_snapshots(buffer, 20),
        )

        assert await buffer.size() == 10
        assert await buffer.latest() == self._snapshot(19)
