"""Tests for the system metrics snapshotter."""

import asyncio
import builtins
import gc
import importlib
import logging
import tracemalloc
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import psutil
import pytest

from snapper.application.system_metrics import snapshotter
from snapper.application.system_metrics.cgroup import CgroupReading
from snapper.application.system_metrics.ring_buffer import DEFAULT_HISTORY_CAP
from snapper.application.system_metrics.ring_buffer import MetricsRingBuffer
from snapper.application.system_metrics.snapshot_types import DiskMetrics
from snapper.application.system_metrics.snapshot_types import LimitsMetrics
from snapper.application.system_metrics.snapshot_types import ProcessMetrics
from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot
from snapper.application.system_metrics.snapshotter import DEFAULT_DISK_FREE_CRIT_BYTES
from snapper.application.system_metrics.snapshotter import DEFAULT_DISK_FREE_WARN_BYTES
from snapper.application.system_metrics.snapshotter import DEFAULT_DISK_MOUNT_PATH
from snapper.application.system_metrics.snapshotter import DEFAULT_INTERVAL_SECONDS
from snapper.application.system_metrics.snapshotter import SystemMetricsSnapshotter
from snapper.application.system_metrics.snapshotter import _resolve_disk_mount_path
from snapper.application.system_metrics.snapshotter import _resolve_history_cap
from snapper.application.system_metrics.snapshotter import _resolve_interval
from snapper.application.system_metrics.snapshotter import _resolve_positive_bytes
from snapper.application.system_metrics.tracemalloc_controller import TracemallocController
from snapper.core.types import HealthStatus
from snapper.core.types import HealthStatusEnum
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import HeartbeatData


class _FakeResourceModule:
    """Deterministic stand-in for the POSIX ``resource`` module."""

    RLIMIT_NPROC: int = 1
    RLIMIT_NOFILE: int = 2
    RLIMIT_AS: int = 3
    RLIM_INFINITY: int = -1

    def getrlimit(self, limit: int) -> tuple[int, int]:
        """Return deterministic soft/hard limits for the requested resource id."""
        values = {
            self.RLIMIT_NPROC: (11, 12),
            self.RLIMIT_NOFILE: (21, 22),
            self.RLIMIT_AS: (31, 32),
        }
        return values[limit]


class TestSnapshotter:
    """Tests for SystemMetricsSnapshotter."""

    @pytest.fixture(autouse=True)
    def _reset_tracemalloc_state(self) -> Generator[None]:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        yield
        if tracemalloc.is_tracing():
            tracemalloc.stop()

    def _make_process(
        self,
        *,
        rss: int = 1000,
        vms: int = 2000,
        cpu_percent: float = 42.5,
    ) -> MagicMock:
        process = MagicMock(spec=psutil.Process)
        process.pid = 321
        process.oneshot.return_value.__enter__.return_value = None
        process.oneshot.return_value.__exit__.return_value = None
        process.net_connections.return_value = [object(), object()]
        process.status.return_value = "running"
        process.num_threads.return_value = 5
        if hasattr(process, "num_fds"):
            process.num_fds.return_value = 10
        process.cpu_times.return_value = SimpleNamespace(user=1.25, system=0.75)
        process.cpu_percent.return_value = cpu_percent
        process.memory_info.return_value = SimpleNamespace(rss=rss, vms=vms)
        return process

    def _make_tracemalloc(self, *, active: bool, traced: int | None) -> MagicMock:
        controller = MagicMock(spec=TracemallocController)
        controller.is_active.return_value = active
        controller.traced_bytes.return_value = traced
        controller.stop = AsyncMock()
        return controller

    def _make_msg_publisher(self) -> MagicMock:
        """Return a mock MessagePublisher with real sequence tracking."""
        publisher = MagicMock()
        publisher.tracker = SequenceTracker()
        publisher.send = AsyncMock()
        return publisher

    def _make_snapshotter(
        self,
        *,
        interval_seconds: float = 5.0,
        history_cap: int = 10,
        disk_free_warn_bytes: int = 1,
        disk_free_crit_bytes: int = 1,
        disk_mount_path: str = "/",
        process: MagicMock | None = None,
        tracemalloc_controller: MagicMock | None = None,
        history_buffer: MetricsRingBuffer | None = None,
        msg_publisher: MessagePublisher | None = None,
    ) -> SystemMetricsSnapshotter:
        return SystemMetricsSnapshotter(
            interval_seconds=interval_seconds,
            history_cap=history_cap,
            disk_free_warn_bytes=disk_free_warn_bytes,
            disk_free_crit_bytes=disk_free_crit_bytes,
            disk_mount_path=disk_mount_path,
            process=process or self._make_process(),
            tracemalloc_controller=tracemalloc_controller
            or self._make_tracemalloc(active=False, traced=None),
            history_buffer=history_buffer,
            msg_publisher=msg_publisher,
        )

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
            tracemalloc_active=False,
            cgroup_version=None,
        )

    def _patch_read_cgroup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        reading: CgroupReading | None,
        version: Literal["v1", "v2"] | None,
    ) -> None:
        monkeypatch.setattr(snapshotter, "read_cgroup", lambda: (reading, version))

    def _disk_metrics(self, status: HealthStatus) -> DiskMetrics:
        """Return a minimal disk metrics block for logging tests."""
        return DiskMetrics(
            mount_path="/",
            total_bytes=1000,
            used_bytes=500,
            free_bytes=500,
            percent_used=50.0,
            disk_low=status != HealthStatusEnum.HEALTHY,
            disk_critical=status == HealthStatusEnum.ERROR,
            status=status,
        )

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_INTERVAL_SECONDS),
            ("", DEFAULT_INTERVAL_SECONDS),
            ("   ", DEFAULT_INTERVAL_SECONDS),
            ("garbage", DEFAULT_INTERVAL_SECONDS),
            ("-1", DEFAULT_INTERVAL_SECONDS),
            ("0", DEFAULT_INTERVAL_SECONDS),
            ("0.25", 0.25),
        ],
    )
    def test_resolve_interval(self, env_value: str | None, expected: float) -> None:
        """Covered by test body."""
        assert _resolve_interval(env_value) == pytest.approx(expected)

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_HISTORY_CAP),
            ("", DEFAULT_HISTORY_CAP),
            ("   ", DEFAULT_HISTORY_CAP),
            ("garbage", DEFAULT_HISTORY_CAP),
            ("-1", DEFAULT_HISTORY_CAP),
            ("0", DEFAULT_HISTORY_CAP),
            ("25", 25),
        ],
    )
    def test_resolve_history_cap(self, env_value: str | None, expected: int) -> None:
        """Covered by test body."""
        assert _resolve_history_cap(env_value) == expected

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, 123),
            ("", 123),
            ("   ", 123),
            ("garbage", 123),
            ("-1", 123),
            ("0", 123),
            ("4096", 4096),
        ],
    )
    def test_resolve_positive_bytes(self, env_value: str | None, expected: int) -> None:
        """Covered by test body."""
        assert _resolve_positive_bytes(env_value, 123) == expected

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_DISK_MOUNT_PATH),
            ("", DEFAULT_DISK_MOUNT_PATH),
            ("   ", DEFAULT_DISK_MOUNT_PATH),
            ("/data", "/data"),
            ("  /data  ", "/data"),
        ],
    )
    def test_resolve_disk_mount_path(self, env_value: str | None, expected: str) -> None:
        """Covered by test body."""
        assert _resolve_disk_mount_path(env_value) == expected

    def test_init_reads_environment_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""
        monkeypatch.setenv("SYSTEM_METRICS_INTERVAL_SECONDS", "0.25")
        monkeypatch.setenv("SYSTEM_METRICS_HISTORY_CAP", "3")
        monkeypatch.setenv("SYSTEM_METRICS_DISK_FREE_WARN_BYTES", "4096")
        monkeypatch.setenv("SYSTEM_METRICS_DISK_FREE_CRIT_BYTES", "2048")
        monkeypatch.setenv("SYSTEM_METRICS_DISK_MOUNT_PATH", "  /data  ")

        metrics = SystemMetricsSnapshotter(process=self._make_process())

        assert metrics.interval_seconds == pytest.approx(0.25)
        assert metrics._history.maxlen == 3
        assert metrics._disk_free_warn_bytes == 4096
        assert metrics._disk_free_crit_bytes == 2048
        assert metrics._disk_mount_path == "/data"

    def test_init_overrides_take_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""
        monkeypatch.setenv("SYSTEM_METRICS_INTERVAL_SECONDS", "9")
        monkeypatch.setenv("SYSTEM_METRICS_HISTORY_CAP", "9")
        monkeypatch.setenv("SYSTEM_METRICS_DISK_FREE_WARN_BYTES", "9")
        monkeypatch.setenv("SYSTEM_METRICS_DISK_FREE_CRIT_BYTES", "9")
        monkeypatch.setenv("SYSTEM_METRICS_DISK_MOUNT_PATH", "/env")

        metrics = SystemMetricsSnapshotter(
            interval_seconds=0.5,
            history_cap=2,
            disk_free_warn_bytes=111,
            disk_free_crit_bytes=55,
            disk_mount_path="/override",
            process=self._make_process(),
        )

        assert metrics.interval_seconds == pytest.approx(0.5)
        assert metrics._history.maxlen == 2
        assert metrics._disk_free_warn_bytes == 111
        assert metrics._disk_free_crit_bytes == 55
        assert metrics._disk_mount_path == "/override"

    def test_init_accepts_dependency_overrides(self) -> None:
        """Covered by test body."""
        process = self._make_process()
        controller = self._make_tracemalloc(active=True, traced=123)
        buffer = MetricsRingBuffer(maxlen=4)

        metrics = SystemMetricsSnapshotter(
            interval_seconds=0.5,
            history_cap=99,
            process=process,
            tracemalloc_controller=controller,
            history_buffer=buffer,
        )

        assert metrics._process is process
        assert metrics.tracemalloc is controller
        assert metrics._history is buffer
        assert metrics.interval_seconds == pytest.approx(0.5)

    async def test_start_takes_eager_sample(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""
        metrics = self._make_snapshotter(interval_seconds=30.0)
        self._patch_read_cgroup(monkeypatch, None, None)
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: None)

        await metrics.start()
        latest = await metrics.current_snapshot()
        await metrics.stop()

        assert latest is not None
        assert await metrics._history.size() == 1

    async def test_start_publishes_eager_disk_heartbeat(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cold-start sample publishes host/disk when a publisher is wired."""
        publisher = self._make_msg_publisher()
        metrics = self._make_snapshotter(
            interval_seconds=30.0,
            msg_publisher=cast(MessagePublisher, publisher),
        )
        disk = self._disk_metrics(HealthStatusEnum.WARNING)
        sample = self._snapshot(1)
        sample["disk"] = disk

        def build_snapshot() -> SystemMetricsSnapshot:
            """Return the prebuilt warning snapshot."""
            return sample

        monkeypatch.setattr(metrics, "_build_snapshot", build_snapshot)

        await metrics.start()
        await metrics.stop()

        publisher.send.assert_awaited_once()
        topic, frame = publisher.send.await_args.args
        assert topic == "system.heartbeats.host.disk"
        assert isinstance(frame, HeartbeatData)
        assert frame.component == "host.disk"
        assert frame.status == HealthStatusEnum.WARNING
        assert frame.sequence == 1
        assert frame.sequence_id == 1
        assert frame.session_id == publisher.tracker.session_id
        assert frame.lag_ms == 0
        assert frame.meta["mount_path"] == "/"
        assert frame.meta["disk_low"] is True

    async def test_sampler_loop_publishes_disk_heartbeat_each_tick(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The background sampler publishes a host/disk frame per tick."""

        class OneTickStopper:
            """Return ``False`` for one full sampler iteration, then stop."""

            def __init__(self) -> None:
                """Initialize the call counter."""
                self.calls = 0

            def is_set(self) -> bool:
                """Stop after the top of the second loop iteration."""
                self.calls += 1
                return self.calls > 2

        async def no_sleep(delay: float) -> None:
            """Replace real sleep so the sampler tick is deterministic."""

        publisher = self._make_msg_publisher()
        metrics = self._make_snapshotter(
            interval_seconds=30.0,
            msg_publisher=cast(MessagePublisher, publisher),
        )
        disk = self._disk_metrics(HealthStatusEnum.ERROR)
        sample = self._snapshot(2)
        sample["disk"] = disk

        def build_snapshot() -> SystemMetricsSnapshot:
            """Return the prebuilt error snapshot."""
            return sample

        monkeypatch.setattr(snapshotter.asyncio, "sleep", no_sleep)
        monkeypatch.setattr(metrics, "_build_snapshot", build_snapshot)
        monkeypatch.setattr(metrics, "_stopping", OneTickStopper())

        await metrics._sampler_loop()

        publisher.send.assert_awaited_once()
        topic, frame = publisher.send.await_args.args
        assert topic == "system.heartbeats.host.disk"
        assert isinstance(frame, HeartbeatData)
        assert frame.status == HealthStatusEnum.ERROR
        assert frame.sequence == 1

    async def test_sampler_loop_appends_additional_snapshots(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        metrics = self._make_snapshotter(interval_seconds=0.05, history_cap=10)
        self._patch_read_cgroup(monkeypatch, None, None)
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: None)

        await metrics.start()
        await asyncio.sleep(0.2)
        size = await metrics._history.size()
        await metrics.stop()

        assert size >= 2

    async def test_stop_cancels_sampler_task(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""
        controller = self._make_tracemalloc(active=False, traced=None)
        metrics = self._make_snapshotter(
            interval_seconds=30.0,
            tracemalloc_controller=controller,
        )
        self._patch_read_cgroup(monkeypatch, None, None)
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: None)

        await metrics.start()
        task = metrics._sampler_task
        await metrics.stop()

        assert task is not None
        assert task.cancelled()
        assert metrics._sampler_task is None
        controller.stop.assert_awaited_once()

    async def test_stop_never_started_is_noop(self) -> None:
        """Covered by test body."""
        controller = self._make_tracemalloc(active=False, traced=None)
        metrics = self._make_snapshotter(tracemalloc_controller=controller)

        await metrics.stop()

        assert metrics._sampler_task is None
        controller.stop.assert_awaited_once()

    async def test_stop_clears_completed_sampler_task(self) -> None:
        """Covered by test body."""
        metrics = self._make_snapshotter()
        task = asyncio.create_task(asyncio.sleep(0))
        await task
        metrics._sampler_task = task

        await metrics.stop()

        assert metrics._sampler_task is None

    async def test_history_delegates_to_buffer(self) -> None:
        """Covered by test body."""
        buffer = MetricsRingBuffer(maxlen=3)
        await buffer.append(self._snapshot(1))
        await buffer.append(self._snapshot(2))
        metrics = self._make_snapshotter(history_buffer=buffer)

        history = await metrics.history(
            datetime(2026, 4, 30, 12, 0, 1, tzinfo=UTC),
            datetime(2026, 4, 30, 12, 0, 2, tzinfo=UTC),
            1,
        )

        assert history == [self._snapshot(2)]

    def test_sample_db_internal_reports_live_aiosqlite_connection_count(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        monkeypatch.setattr(snapshotter, "_live_aiosqlite_connections", {object(): object()})

        metrics = SystemMetricsSnapshotter._sample_db_internal_metrics()

        assert metrics == {
            "aiosqlite_live_connections": 1,
            "pool_size": None,
            "pool_checked_out": None,
        }

    def test_sample_disk_metrics_reports_healthy_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        monkeypatch.setattr(
            snapshotter.shutil,
            "disk_usage",
            lambda path: SimpleNamespace(
                total=40 * 1024**3,
                used=10 * 1024**3,
                free=30 * 1024**3,
            ),
        )
        metrics = self._make_snapshotter(
            disk_free_warn_bytes=DEFAULT_DISK_FREE_WARN_BYTES,
            disk_free_crit_bytes=DEFAULT_DISK_FREE_CRIT_BYTES,
            disk_mount_path="/data",
        )._sample_disk_metrics()

        assert metrics["mount_path"] == "/data"
        assert metrics["disk_low"] is False
        assert metrics["disk_critical"] is False
        assert metrics["status"] == HealthStatusEnum.HEALTHY
        assert metrics["percent_used"] == pytest.approx(25.0)

    def test_sample_disk_metrics_reports_warning_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        monkeypatch.setattr(
            snapshotter.shutil,
            "disk_usage",
            lambda path: SimpleNamespace(
                total=40 * 1024**3,
                used=25 * 1024**3,
                free=15 * 1024**3,
            ),
        )
        metrics = self._make_snapshotter(
            disk_free_warn_bytes=DEFAULT_DISK_FREE_WARN_BYTES,
            disk_free_crit_bytes=DEFAULT_DISK_FREE_CRIT_BYTES,
        )._sample_disk_metrics()

        assert metrics["disk_low"] is True
        assert metrics["disk_critical"] is False
        assert metrics["status"] == HealthStatusEnum.WARNING

    def test_sample_disk_metrics_reports_critical_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        monkeypatch.setattr(
            snapshotter.shutil,
            "disk_usage",
            lambda path: SimpleNamespace(
                total=40 * 1024**3,
                used=35 * 1024**3,
                free=5 * 1024**3,
            ),
        )
        metrics = self._make_snapshotter(
            disk_free_warn_bytes=DEFAULT_DISK_FREE_WARN_BYTES,
            disk_free_crit_bytes=DEFAULT_DISK_FREE_CRIT_BYTES,
        )._sample_disk_metrics()

        assert metrics["disk_low"] is True
        assert metrics["disk_critical"] is True
        assert metrics["status"] == HealthStatusEnum.ERROR

    def test_sample_disk_metrics_critical_implies_low_with_inverted_thresholds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        monkeypatch.setattr(
            snapshotter.shutil,
            "disk_usage",
            lambda path: SimpleNamespace(
                total=40 * 1024**3,
                used=25 * 1024**3,
                free=15 * 1024**3,
            ),
        )
        metrics = self._make_snapshotter(
            disk_free_warn_bytes=10 * 1024**3,
            disk_free_crit_bytes=20 * 1024**3,
        )._sample_disk_metrics()

        assert metrics["disk_critical"] is True
        assert metrics["disk_low"] is True
        assert metrics["status"] == HealthStatusEnum.ERROR

    def test_sample_disk_metrics_handles_zero_total(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""
        monkeypatch.setattr(
            snapshotter.shutil,
            "disk_usage",
            lambda path: SimpleNamespace(total=0, used=0, free=0),
        )
        metrics = self._make_snapshotter(
            disk_free_warn_bytes=DEFAULT_DISK_FREE_WARN_BYTES,
            disk_free_crit_bytes=DEFAULT_DISK_FREE_CRIT_BYTES,
        )._sample_disk_metrics()

        assert metrics["percent_used"] == pytest.approx(0.0)
        assert metrics["status"] == HealthStatusEnum.ERROR

    def test_sample_disk_metrics_handles_oserror(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""

        def raise_oserror(path: str) -> SimpleNamespace:
            """Raise to simulate an unreadable mount path."""
            raise OSError(f"cannot read {path}")

        monkeypatch.setattr(snapshotter.shutil, "disk_usage", raise_oserror)
        metrics = self._make_snapshotter(disk_mount_path="/missing")._sample_disk_metrics()

        assert metrics == DiskMetrics(
            mount_path="/missing",
            total_bytes=None,
            used_bytes=None,
            free_bytes=None,
            percent_used=None,
            disk_low=False,
            disk_critical=False,
            status=HealthStatusEnum.WARNING,
        )

    async def test_publish_disk_heartbeat_without_publisher_is_noop(self) -> None:
        """Publisher-less snapshotters keep metrics local and emit no frame."""
        metrics = self._make_snapshotter()

        await metrics._publish_disk_heartbeat(self._disk_metrics(HealthStatusEnum.HEALTHY))

        assert metrics._disk_heartbeat_sequence == 0

    @pytest.mark.parametrize(
        "status",
        [
            HealthStatusEnum.HEALTHY,
            HealthStatusEnum.WARNING,
            HealthStatusEnum.ERROR,
        ],
    )
    async def test_publish_disk_heartbeat_carries_disk_status(self, status: HealthStatus) -> None:
        """Healthy, warning, and error states are preserved on the bus frame."""
        publisher = self._make_msg_publisher()
        metrics = self._make_snapshotter(msg_publisher=cast(MessagePublisher, publisher))

        await metrics._publish_disk_heartbeat(self._disk_metrics(status))

        publisher.send.assert_awaited_once()
        topic, frame = publisher.send.await_args.args
        assert topic == "system.heartbeats.host.disk"
        assert isinstance(frame, HeartbeatData)
        assert frame.status == status
        assert frame.component == "host.disk"
        assert frame.sequence == 1

    async def test_publish_disk_heartbeat_send_failure_is_nonfatal(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A ZMQ send failure does not break metrics sampling."""
        publisher = self._make_msg_publisher()
        publisher.send = AsyncMock(side_effect=RuntimeError("send failed"))
        metrics = self._make_snapshotter(msg_publisher=cast(MessagePublisher, publisher))

        with caplog.at_level(logging.ERROR, logger=snapshotter.__name__):
            await metrics._publish_disk_heartbeat(self._disk_metrics(HealthStatusEnum.ERROR))

        assert metrics._disk_heartbeat_sequence == 1
        assert "disk heartbeat publish failed" in caplog.text

    def test_log_disk_pressure_ignores_healthy_status(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Covered by test body."""
        with caplog.at_level(logging.WARNING, logger=snapshotter.__name__):
            SystemMetricsSnapshotter._log_disk_pressure(
                self._disk_metrics(HealthStatusEnum.HEALTHY)
            )

        assert [record for record in caplog.records if record.name == snapshotter.__name__] == []

    def test_log_disk_pressure_warns_for_low_status(self, caplog: pytest.LogCaptureFixture) -> None:
        """Covered by test body."""
        with caplog.at_level(logging.WARNING, logger=snapshotter.__name__):
            SystemMetricsSnapshotter._log_disk_pressure(
                self._disk_metrics(HealthStatusEnum.WARNING)
            )

        records = [record for record in caplog.records if record.name == snapshotter.__name__]
        assert len(records) == 1
        assert records[0].levelno == logging.WARNING
        assert records[0].getMessage() == (
            "disk_low mount=/ free_bytes=500 total_bytes=1000 percent_used=50.0"
        )

    def test_log_disk_pressure_errors_for_critical_status(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Covered by test body."""
        with caplog.at_level(logging.WARNING, logger=snapshotter.__name__):
            SystemMetricsSnapshotter._log_disk_pressure(self._disk_metrics(HealthStatusEnum.ERROR))

        records = [record for record in caplog.records if record.name == snapshotter.__name__]
        assert len(records) == 1
        assert records[0].levelno == logging.ERROR
        assert records[0].getMessage() == (
            "disk_critical mount=/ free_bytes=500 total_bytes=1000 percent_used=50.0"
        )

    def test_sample_process_metrics_returns_zero_when_num_fds_is_unavailable(self) -> None:
        """Windows psutil builds do not expose ``Process.num_fds``."""
        process = self._make_process()
        if hasattr(process, "num_fds"):
            del process.num_fds
        metrics = self._make_snapshotter(process=process)._sample_process_metrics()

        assert metrics["num_fds"] == 0

    def test_build_snapshot_without_cgroup_and_inactive_tracemalloc(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        process = self._make_process(rss=1000, vms=2000)
        controller = self._make_tracemalloc(active=False, traced=None)
        metrics = self._make_snapshotter(process=process, tracemalloc_controller=controller)
        self._patch_read_cgroup(monkeypatch, None, None)
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: None)

        snapshot = metrics._build_snapshot()

        assert snapshot["cgroup_version"] is None
        assert snapshot["tracemalloc_active"] is False
        assert snapshot["memory"]["python_traced_bytes"] is None
        assert snapshot["memory"]["native_bytes"] is None
        assert snapshot["memory"]["rss_peak_bytes"] == 1000
        assert snapshot["cpu"]["cgroup_quota_microseconds"] is None
        assert snapshot["memory"]["cgroup_limit_bytes"] is None
        assert snapshot["disk"]["mount_path"] == "/"
        assert snapshot["disk"]["status"] == HealthStatusEnum.HEALTHY

    def test_build_snapshot_reports_native_bytes_when_tracemalloc_is_active(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        process = self._make_process(rss=1000, vms=2000)
        controller = self._make_tracemalloc(active=True, traced=250)
        metrics = self._make_snapshotter(process=process, tracemalloc_controller=controller)
        self._patch_read_cgroup(monkeypatch, None, None)
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: 1200)

        snapshot = metrics._build_snapshot()

        assert snapshot["tracemalloc_active"] is True
        assert snapshot["memory"]["python_traced_bytes"] == 250
        assert snapshot["memory"]["native_bytes"] == 750
        assert snapshot["memory"]["rss_peak_bytes"] == 1200

    def test_build_snapshot_maps_cgroup_max_sentinel(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Covered by test body."""
        reading = CgroupReading(
            memory_max_bytes=-1,
            memory_current_bytes=600,
            cpu_quota_microseconds=-1,
            cpu_throttled_count=4,
        )
        metrics = self._make_snapshotter()
        self._patch_read_cgroup(monkeypatch, reading, "v2")
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: None)

        snapshot = metrics._build_snapshot()

        assert snapshot["cgroup_version"] == "v2"
        assert snapshot["cpu"]["cgroup_quota_microseconds"] is None
        assert snapshot["cpu"]["cgroup_throttled_count"] == 4
        assert snapshot["memory"]["cgroup_limit_bytes"] is None
        assert snapshot["memory"]["cgroup_current_bytes"] == 600
        assert snapshot["memory"]["saturation_pct"] is None

    def test_build_snapshot_maps_finite_cgroup_limits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        reading = CgroupReading(
            memory_max_bytes=400,
            memory_current_bytes=100,
            cpu_quota_microseconds=200,
            cpu_throttled_count=3,
        )
        metrics = self._make_snapshotter()
        self._patch_read_cgroup(monkeypatch, reading, "v1")
        monkeypatch.setattr(metrics, "_read_vm_hwm", lambda: None)

        snapshot = metrics._build_snapshot()

        assert snapshot["cgroup_version"] == "v1"
        assert snapshot["cpu"]["cgroup_quota_microseconds"] == 200
        assert snapshot["cpu"]["cgroup_throttled_count"] == 3
        assert snapshot["memory"]["cgroup_limit_bytes"] == 400
        assert snapshot["memory"]["cgroup_current_bytes"] == 100
        assert snapshot["memory"]["saturation_pct"] == pytest.approx(0.25)

    def test_sample_asyncio_metrics_without_running_loop(self) -> None:
        """Covered by test body."""
        metrics = SystemMetricsSnapshotter._sample_asyncio_metrics()

        assert metrics == {"active_tasks": 0, "pending_tasks": 0}

    async def test_sample_asyncio_metrics_under_running_loop(self) -> None:
        """Covered by test body."""
        metrics = SystemMetricsSnapshotter._sample_asyncio_metrics()

        assert metrics["active_tasks"] >= 1
        assert metrics["pending_tasks"] >= 1

    def test_sample_gc_metrics_with_three_generations_and_short_fallback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        monkeypatch.setattr(
            gc,
            "get_stats",
            lambda: [
                {"collections": 1, "uncollectable": 2},
                {"collections": 3, "uncollectable": 4},
                {"collections": 5, "uncollectable": 6},
            ],
        )
        monkeypatch.setattr(gc, "get_count", lambda: (7, 8, 9))

        full = SystemMetricsSnapshotter._sample_gc_metrics()

        monkeypatch.setattr(gc, "get_stats", lambda: [{"collections": 1}])
        short = SystemMetricsSnapshotter._sample_gc_metrics()

        assert full == {
            "collections_per_gen": (1, 3, 5),
            "uncollectable": 12,
            "current_objects": 24,
        }
        assert short == {
            "collections_per_gen": (0, 0, 0),
            "uncollectable": 0,
            "current_objects": 24,
        }

    def test_sample_limits_metrics_matches_resource_getrlimit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        resource_module = _FakeResourceModule()
        monkeypatch.setattr(snapshotter, "_resource", resource_module)

        metrics = SystemMetricsSnapshotter._sample_limits_metrics()

        assert metrics == {
            "rlimit_nproc": 11,
            "rlimit_nofile": 21,
            "rlimit_as_bytes": 31,
        }

    def test_sample_saturation_metrics_handles_finite_infinite_and_zero_limits(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""
        resource_module = _FakeResourceModule()
        monkeypatch.setattr(snapshotter, "_resource", resource_module)
        process_metrics = ProcessMetrics(
            pid=1,
            uptime_seconds=1.0,
            status="running",
            num_threads=5,
            num_fds=10,
            num_connections=0,
        )
        finite_limits = LimitsMetrics(
            rlimit_nproc=20,
            rlimit_nofile=40,
            rlimit_as_bytes=resource_module.RLIM_INFINITY,
        )
        infinite_limits = LimitsMetrics(
            rlimit_nproc=resource_module.RLIM_INFINITY,
            rlimit_nofile=resource_module.RLIM_INFINITY,
            rlimit_as_bytes=resource_module.RLIM_INFINITY,
        )
        zero_limits = LimitsMetrics(
            rlimit_nproc=0,
            rlimit_nofile=0,
            rlimit_as_bytes=resource_module.RLIM_INFINITY,
        )

        finite = SystemMetricsSnapshotter._sample_saturation_metrics(
            process_metrics=process_metrics,
            limits_metrics=finite_limits,
        )
        infinite = SystemMetricsSnapshotter._sample_saturation_metrics(
            process_metrics=process_metrics,
            limits_metrics=infinite_limits,
        )
        zero = SystemMetricsSnapshotter._sample_saturation_metrics(
            process_metrics=process_metrics,
            limits_metrics=zero_limits,
        )

        assert finite["threads_pct"] == pytest.approx(0.25)
        assert finite["fds_pct"] == pytest.approx(0.25)
        assert infinite == {"threads_pct": None, "fds_pct": None}
        assert zero == {"threads_pct": None, "fds_pct": None}

    def test_read_vm_hwm_handles_errors_and_parses_valid_line(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Covered by test body."""

        def raise_oserror(
            path: Path, encoding: str | None = None, errors: str | None = None
        ) -> str:
            raise OSError(f"cannot read {path} {encoding} {errors}")

        def missing_line(path: Path, encoding: str | None = None, errors: str | None = None) -> str:
            return f"Name:\tpython\n{path.name} {encoding} {errors}\n"

        def malformed_line(
            path: Path, encoding: str | None = None, errors: str | None = None
        ) -> str:
            return f"VmHWM:\n{path.name} {encoding} {errors}\n"

        def unparseable_line(
            path: Path, encoding: str | None = None, errors: str | None = None
        ) -> str:
            return f"VmHWM:\tnope kB\n{path.name} {encoding} {errors}\n"

        def valid_line(path: Path, encoding: str | None = None, errors: str | None = None) -> str:
            return f"Name:\tpython\nVmHWM:\t2 kB\n{path.name} {encoding} {errors}\n"

        monkeypatch.setattr(Path, "read_text", raise_oserror)
        assert SystemMetricsSnapshotter._read_vm_hwm() is None
        monkeypatch.setattr(Path, "read_text", missing_line)
        assert SystemMetricsSnapshotter._read_vm_hwm() is None
        monkeypatch.setattr(Path, "read_text", malformed_line)
        assert SystemMetricsSnapshotter._read_vm_hwm() is None
        monkeypatch.setattr(Path, "read_text", unparseable_line)
        assert SystemMetricsSnapshotter._read_vm_hwm() is None
        monkeypatch.setattr(Path, "read_text", valid_line)
        assert SystemMetricsSnapshotter._read_vm_hwm() == 2048

    async def test_sampler_loop_exits_when_stopping_already_set(self) -> None:
        """Covered by test body."""
        metrics = self._make_snapshotter(interval_seconds=0.01)
        metrics._stopping.set()

        await metrics._sampler_loop()

        assert await metrics._history.size() == 0

    async def test_sampler_loop_exits_after_sleep_when_stopping_set(self) -> None:
        """Covered by test body."""
        metrics = self._make_snapshotter(interval_seconds=0.05)
        task = asyncio.create_task(metrics._sampler_loop())

        await asyncio.sleep(0.01)
        metrics._stopping.set()
        await task

        assert await metrics._history.size() == 0

    async def test_tracemalloc_auto_stop_handles_external_stop(self) -> None:
        """Covered by test body."""
        controller = TracemallocController()

        await controller.start(0.1)
        tracemalloc.stop()
        await asyncio.sleep(0.2)

        assert controller.is_active() is False
        assert controller._auto_stop_task is None


class TestPosixResourceFallback:
    """Coverage for the POSIX-only ``resource`` stdlib guarded import."""

    def test_resource_import_error_falls_back_to_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Reload the module with ``import resource`` patched to raise.

        Given: the ``resource`` stdlib module is unavailable (e.g. on
            Windows where it is POSIX-only),
        When: the snapshotter module is reloaded so the top-level
            try/except is re-executed,
        Then: the module-level ``_resource`` symbol is ``None``.
        """
        original_import = builtins.__import__

        def mock_import(name: str, *args: object, **kwargs: object) -> object:
            """Raise ImportError for resource; pass-through for everything else."""
            if name == "resource":
                raise ImportError("simulated Windows: resource not available")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", mock_import)
        importlib.reload(snapshotter)
        try:
            assert snapshotter._resource is None
        finally:
            monkeypatch.setattr("builtins.__import__", original_import)
            importlib.reload(snapshotter)

    def test_sample_limits_returns_zeros_when_resource_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Windows fallback path on the limits sampler.

        Given: ``snapshotter._resource`` is ``None``,
        When: ``_sample_limits_metrics`` is called,
        Then: every limit field collapses to ``0``.
        """
        monkeypatch.setattr(snapshotter, "_resource", None)
        limits = SystemMetricsSnapshotter._sample_limits_metrics()
        assert limits == LimitsMetrics(rlimit_nproc=0, rlimit_nofile=0, rlimit_as_bytes=0)

    def test_sample_saturation_returns_none_pcts_when_resource_is_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Windows fallback path on the saturation sampler.

        Given: ``snapshotter._resource`` is ``None``,
        When: ``_sample_saturation_metrics`` is called with arbitrary
            process + limits inputs,
        Then: both ``threads_pct`` and ``fds_pct`` collapse to ``None``.
        """
        monkeypatch.setattr(snapshotter, "_resource", None)
        saturation = SystemMetricsSnapshotter._sample_saturation_metrics(
            process_metrics=ProcessMetrics(
                pid=1,
                uptime_seconds=0.0,
                status="running",
                num_threads=8,
                num_fds=64,
                num_connections=0,
            ),
            limits_metrics=LimitsMetrics(rlimit_nproc=4096, rlimit_nofile=8192, rlimit_as_bytes=-1),
        )
        assert saturation["threads_pct"] is None
        assert saturation["fds_pct"] is None
