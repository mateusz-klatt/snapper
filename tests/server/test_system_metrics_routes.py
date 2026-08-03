"""Tests for ``/api/metrics/system*`` REST endpoints + lifespan startup helpers.

Covers:

* Handler-level mapping snapshot → wire schema for current + history.
* Window + limit filtering via the ring buffer.
* Tracemalloc arm / disarm + clamp semantics.
* 401 unauthenticated + 403 missing CSRF + 503 missing snapshotter
  via :class:`fastapi.testclient.TestClient`.
* Lifespan startup helper: failure leaves the attribute absent
  (fail-closed); successful start attaches the singleton. Stop helper
  tolerates partial-init (no attribute).
"""

import asyncio
import contextlib
import inspect
import tracemalloc
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Literal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi.testclient import TestClient

from snapper.application.system_metrics.ring_buffer import MetricsRingBuffer
from snapper.application.system_metrics.snapshot_types import AsyncioMetrics as AsyncioMetricsTD
from snapper.application.system_metrics.snapshot_types import CpuMetrics as CpuMetricsTD
from snapper.application.system_metrics.snapshot_types import (
    DbInternalMetrics as DbInternalMetricsTD,
)
from snapper.application.system_metrics.snapshot_types import DiskMetrics as DiskMetricsTD
from snapper.application.system_metrics.snapshot_types import GcMetrics as GcMetricsTD
from snapper.application.system_metrics.snapshot_types import LimitsMetrics as LimitsMetricsTD
from snapper.application.system_metrics.snapshot_types import MemoryMetrics as MemoryMetricsTD
from snapper.application.system_metrics.snapshot_types import ProcessMetrics as ProcessMetricsTD
from snapper.application.system_metrics.snapshot_types import (
    SaturationMetrics as SaturationMetricsTD,
)
from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot
from snapper.application.system_metrics.snapshotter import SystemMetricsSnapshotter
from snapper.application.system_metrics.tracemalloc_controller import MAX_DURATION_SECONDS
from snapper.application.system_metrics.tracemalloc_controller import TracemallocController
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import HealthStatusEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import _start_system_metrics_snapshotter
from snapper.server.app import _stop_system_metrics_snapshotter
from snapper.server.app import create_app
from snapper.server.metrics_routes import _SNAPSHOTTER_UNAVAILABLE_DETAIL
from snapper.server.metrics_routes import get_system_metrics
from snapper.server.metrics_routes import get_system_metrics_history
from snapper.server.metrics_routes import post_system_metrics_tracemalloc_start
from snapper.server.metrics_routes import post_system_metrics_tracemalloc_stop
from tests.helpers.fastapi_routes import iter_fastapi_route_paths


def _build_synthetic_snapshot(
    *,
    bus_time: datetime,
    cgroup_version: Literal["v1", "v2"] | None = "v2",
    tracemalloc_active: bool = False,
) -> SystemMetricsSnapshot:
    """Return a fully-populated synthetic snapshot for buffer fixtures."""
    return SystemMetricsSnapshot(
        bus_time=bus_time,
        process=ProcessMetricsTD(
            pid=12345,
            uptime_seconds=42.5,
            status="running",
            num_threads=8,
            num_fds=64,
            num_connections=2,
        ),
        cpu=CpuMetricsTD(
            process_percent=12.5,
            user_time_seconds=1.0,
            system_time_seconds=0.5,
            cgroup_quota_microseconds=100000,
            cgroup_throttled_count=0,
        ),
        memory=MemoryMetricsTD(
            rss_bytes=100_000_000,
            rss_peak_bytes=110_000_000,
            vms_bytes=200_000_000,
            python_traced_bytes=None,
            native_bytes=None,
            cgroup_limit_bytes=2_000_000_000,
            cgroup_current_bytes=120_000_000,
            saturation_pct=0.06,
        ),
        asyncio=AsyncioMetricsTD(active_tasks=4, pending_tasks=1),
        gc=GcMetricsTD(
            collections_per_gen=(10, 5, 1),
            uncollectable=0,
            current_objects=42,
        ),
        limits=LimitsMetricsTD(
            rlimit_nproc=4096,
            rlimit_nofile=8192,
            rlimit_as_bytes=-1,
        ),
        saturation=SaturationMetricsTD(
            threads_pct=8 / 4096,
            fds_pct=64 / 8192,
        ),
        db_internal=DbInternalMetricsTD(
            aiosqlite_live_connections=3,
            pool_size=None,
            pool_checked_out=None,
        ),
        disk=DiskMetricsTD(
            mount_path="/",
            total_bytes=40 * 1024**3,
            used_bytes=25 * 1024**3,
            free_bytes=15 * 1024**3,
            percent_used=62.5,
            disk_low=True,
            disk_critical=False,
            status=HealthStatusEnum.WARNING,
        ),
        fx_shadow_pins={
            "creation": 1,
            "reuse": 2,
            "conflict": 3,
            "upgrade_required": 4,
            "mismatch": 5,
            "failure": 6,
            "dropped": 7,
        },
        tracemalloc_active=tracemalloc_active,
        cgroup_version=cgroup_version,
    )


async def _populate_buffer(
    snapshotter: SystemMetricsSnapshotter,
    snapshots: list[SystemMetricsSnapshot],
) -> None:
    """Append the given snapshots to the snapshotter's underlying ring buffer.

    Reaches into the private buffer directly because the snapshotter's
    public ``start()`` would also spawn the sampler loop, which the
    handler-level tests do not want running.
    """
    buffer: MetricsRingBuffer = snapshotter._history
    for snap in snapshots:
        await buffer.append(snap)


def _make_snapshotter(
    *,
    history_cap: int = 1024,
    tracemalloc_controller: TracemallocController | None = None,
) -> SystemMetricsSnapshotter:
    """Build a snapshotter with deterministic dependencies (no sampler running)."""
    return SystemMetricsSnapshotter(
        interval_seconds=0.05,
        history_cap=history_cap,
        process=MagicMock(),
        tracemalloc_controller=tracemalloc_controller or TracemallocController(),
    )


def _make_request_with_snapshotter(
    snapshotter: SystemMetricsSnapshotter | None,
) -> Request:
    """Return a FastAPI ``Request`` whose ``app.state`` has the given snapshotter."""
    req = MagicMock(spec=Request)
    state = SimpleNamespace(rest_tracker=SequenceTracker())
    if snapshotter is not None:
        state.system_metrics_snapshotter = snapshotter
    req.app.state = state
    return req


def _role_principal(role: UserRole) -> AuthPrincipal:
    """Return a principal for one named permission set."""
    return AuthPrincipal(
        username=role.value,
        role=role,
        user_public_id=f"{role.value}-1",
    )


def _viewer_principal() -> AuthPrincipal:
    """Return a VIEWER principal with read-only system visibility."""
    return _role_principal(UserRole.VIEWER)


def _runtime_diagnostics_principal() -> AuthPrincipal:
    """Return an OPERATOR principal allowed to mutate runtime diagnostics."""
    return _role_principal(UserRole.OPERATOR)


@pytest.fixture(autouse=True)
def _reset_tracemalloc_state() -> Generator[None]:
    """Ensure tracemalloc is OFF before + after every test in this module."""
    if tracemalloc.is_tracing():
        tracemalloc.stop()
    yield
    if tracemalloc.is_tracing():
        tracemalloc.stop()


class TestGetSystemMetrics:
    """Handler-level tests for ``GET /api/metrics/system``."""

    @pytest.mark.asyncio
    async def test_returns_latest_snapshot(self) -> None:
        """The latest pre-populated snapshot maps onto the wire envelope."""
        snapshotter = _make_snapshotter()
        bus_time = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        await _populate_buffer(snapshotter, [_build_synthetic_snapshot(bus_time=bus_time)])

        response = await get_system_metrics(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_viewer_principal(),
        )

        assert response.type == "system_metrics_response"
        assert response.payload.type == "system_metrics"
        assert response.payload.bus_time == bus_time
        assert response.payload.process.pid == 12345
        assert response.payload.cpu.process_percent == pytest.approx(12.5)
        assert response.payload.memory.rss_bytes == 100_000_000
        assert response.payload.asyncio.active_tasks == 4
        assert response.payload.gc.collections_gen0 == 10
        assert response.payload.gc.collections_gen1 == 5
        assert response.payload.gc.collections_gen2 == 1
        assert response.payload.limits.rlimit_nproc == 4096
        assert response.payload.saturation.threads_pct == pytest.approx(8 / 4096)
        assert response.payload.db_internal.aiosqlite_live_connections == 3
        assert response.payload.disk.status == HealthStatusEnum.WARNING
        assert response.payload.disk.free_bytes == 15 * 1024**3
        assert response.payload.tracemalloc_active is False
        assert response.payload.cgroup_version == "v2"

    @pytest.mark.asyncio
    async def test_returns_503_when_snapshotter_attribute_missing(self) -> None:
        """Routes return 503 when no snapshotter is attached to ``app.state``."""
        with pytest.raises(HTTPException) as exc:
            await get_system_metrics(
                request=_make_request_with_snapshotter(None),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _SNAPSHOTTER_UNAVAILABLE_DETAIL

    @pytest.mark.asyncio
    async def test_returns_503_when_buffer_empty(self) -> None:
        """An empty buffer (no eager sample) still falls through to 503."""
        snapshotter = _make_snapshotter()

        with pytest.raises(HTTPException) as exc:
            await get_system_metrics(
                request=_make_request_with_snapshotter(snapshotter),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503

    def test_endpoint_requires_read_system_status_permission(self) -> None:
        """Route signature binds ``require_permission(READ_SYSTEM_STATUS)``."""
        signature = inspect.signature(get_system_metrics)
        principal_annotation = signature.parameters["_principal"].annotation
        guard_closure = principal_annotation.__metadata__[0].dependency
        bound_permissions = [
            cell.cell_contents
            for cell in (guard_closure.__closure__ or [])
            if hasattr(cell, "cell_contents")
        ]
        assert Permission.READ_SYSTEM_STATUS in bound_permissions


class TestGetSystemMetricsHistory:
    """Handler-level tests for ``GET /api/metrics/system/history``."""

    @pytest.mark.asyncio
    async def test_filters_by_iso_timestamp_range(self) -> None:
        """Window query returns exactly the snapshots whose ``bus_time`` is inside."""
        snapshotter = _make_snapshotter()
        base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        snapshots = [
            _build_synthetic_snapshot(bus_time=base + timedelta(seconds=i * 5)) for i in range(5)
        ]
        await _populate_buffer(snapshotter, snapshots)

        response = await get_system_metrics_history(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_viewer_principal(),
            since=base + timedelta(seconds=5),
            until=base + timedelta(seconds=15),
            limit=720,
        )

        assert response.count == 3
        assert len(response.payload) == 3
        assert response.payload[0].disk.status == HealthStatusEnum.WARNING
        assert response.payload[0].disk.free_bytes == 15 * 1024**3
        bus_times = [item.bus_time for item in response.payload]
        assert bus_times == [
            base + timedelta(seconds=5),
            base + timedelta(seconds=10),
            base + timedelta(seconds=15),
        ]

    @pytest.mark.asyncio
    async def test_respects_limit(self) -> None:
        """The most-recent N within the window are returned when limit < window size."""
        snapshotter = _make_snapshotter(history_cap=200)
        base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        snapshots = [
            _build_synthetic_snapshot(bus_time=base + timedelta(seconds=i * 5)) for i in range(100)
        ]
        await _populate_buffer(snapshotter, snapshots)

        response = await get_system_metrics_history(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_viewer_principal(),
            since=None,
            until=None,
            limit=10,
        )

        assert response.count == 10
        assert response.payload[-1].bus_time == base + timedelta(seconds=99 * 5)

    @pytest.mark.asyncio
    async def test_default_window_returns_full_buffer(self) -> None:
        """Omitted since/until + large limit returns every buffered snapshot."""
        snapshotter = _make_snapshotter()
        base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        snapshots = [
            _build_synthetic_snapshot(bus_time=base + timedelta(seconds=i)) for i in range(3)
        ]
        await _populate_buffer(snapshotter, snapshots)

        response = await get_system_metrics_history(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_viewer_principal(),
            since=None,
            until=None,
            limit=720,
        )

        assert response.count == 3

    @pytest.mark.asyncio
    async def test_returns_503_when_snapshotter_missing(self) -> None:
        """History endpoint also falls through to 503 when singleton absent."""
        with pytest.raises(HTTPException) as exc:
            await get_system_metrics_history(
                request=_make_request_with_snapshotter(None),
                _principal=_viewer_principal(),
                since=None,
                until=None,
                limit=720,
            )
        assert exc.value.status_code == 503

    @pytest.mark.asyncio
    async def test_naive_datetime_bounds_are_normalized_to_utc(self) -> None:
        """Naive ``since`` / ``until`` are treated as UTC, not 500."""
        snapshotter = _make_snapshotter()
        base = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        snapshots = [
            _build_synthetic_snapshot(bus_time=base + timedelta(seconds=i * 5)) for i in range(3)
        ]
        await _populate_buffer(snapshotter, snapshots)

        naive_since = datetime(2026, 5, 1, 12, 0, 0)
        naive_until = datetime(2026, 5, 1, 12, 0, 5)

        response = await get_system_metrics_history(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_viewer_principal(),
            since=naive_since,
            until=naive_until,
            limit=720,
        )

        assert response.count == 2


class TestPostTracemallocStart:
    """Handler-level tests for ``POST /api/metrics/system/tracemalloc/start``."""

    @pytest.mark.asyncio
    async def test_arms_tracking(self) -> None:
        """Calling start arms tracemalloc and returns ``active=True``."""
        controller = TracemallocController()
        snapshotter = _make_snapshotter(tracemalloc_controller=controller)

        response = await post_system_metrics_tracemalloc_start(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_runtime_diagnostics_principal(),
            _csrf=None,
            duration_s=1.0,
        )

        assert response.payload.active is True
        assert response.payload.requested_duration_seconds == pytest.approx(1.0)
        assert controller.is_active() is True
        await controller.stop()

    @pytest.mark.asyncio
    async def test_clamps_duration_to_max_3600s(self) -> None:
        """``duration_s`` beyond the cap is clamped to ``MAX_DURATION_SECONDS``."""
        controller = TracemallocController()
        snapshotter = _make_snapshotter(tracemalloc_controller=controller)

        response = await post_system_metrics_tracemalloc_start(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_runtime_diagnostics_principal(),
            _csrf=None,
            duration_s=99999.0,
        )

        assert response.payload.requested_duration_seconds == pytest.approx(MAX_DURATION_SECONDS)
        await controller.stop()

    @pytest.mark.asyncio
    async def test_auto_stops_after_duration(self) -> None:
        """After ``duration_s`` elapses, tracemalloc auto-disarms."""
        controller = TracemallocController()
        snapshotter = _make_snapshotter(tracemalloc_controller=controller)

        await post_system_metrics_tracemalloc_start(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_runtime_diagnostics_principal(),
            _csrf=None,
            duration_s=0.05,
        )
        assert controller.is_active() is True
        await asyncio.sleep(0.2)
        assert controller.is_active() is False

    @pytest.mark.asyncio
    async def test_returns_503_when_snapshotter_missing(self) -> None:
        """Tracemalloc start falls through to 503 when singleton absent."""
        with pytest.raises(HTTPException) as exc:
            await post_system_metrics_tracemalloc_start(
                request=_make_request_with_snapshotter(None),
                _principal=_runtime_diagnostics_principal(),
                _csrf=None,
                duration_s=1.0,
            )
        assert exc.value.status_code == 503


class TestPostTracemallocStop:
    """Handler-level tests for ``POST /api/metrics/system/tracemalloc/stop``."""

    @pytest.mark.asyncio
    async def test_disarms_active_tracking(self) -> None:
        """Stop disarms a previously-armed controller and reports ``active=False``."""
        controller = TracemallocController()
        snapshotter = _make_snapshotter(tracemalloc_controller=controller)
        await controller.start(60.0)
        assert controller.is_active() is True

        response = await post_system_metrics_tracemalloc_stop(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_runtime_diagnostics_principal(),
            _csrf=None,
        )

        assert response.payload.active is False
        assert response.payload.requested_duration_seconds is None
        assert controller.is_active() is False

    @pytest.mark.asyncio
    async def test_stop_when_inactive_is_idempotent(self) -> None:
        """Stop on an already-inactive controller still returns ``active=False``."""
        controller = TracemallocController()
        snapshotter = _make_snapshotter(tracemalloc_controller=controller)

        response = await post_system_metrics_tracemalloc_stop(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_runtime_diagnostics_principal(),
            _csrf=None,
        )

        assert response.payload.active is False

    @pytest.mark.asyncio
    async def test_returns_503_when_snapshotter_missing(self) -> None:
        """Tracemalloc stop falls through to 503 when singleton absent."""
        with pytest.raises(HTTPException) as exc:
            await post_system_metrics_tracemalloc_stop(
                request=_make_request_with_snapshotter(None),
                _principal=_runtime_diagnostics_principal(),
                _csrf=None,
            )
        assert exc.value.status_code == 503


def _build_app_with_snapshotter(
    *,
    auth_override: bool,
    csrf_override: bool,
    pre_populate: bool = True,
    role: UserRole = UserRole.VIEWER,
) -> tuple[object, SystemMetricsSnapshotter]:
    """Build a fresh app with a synthetic snapshotter on ``state``.

    Bypasses lifespan startup — :class:`TestClient` is constructed
    without the ``with`` context manager so the heavy process-startup
    machinery never fires.
    """
    app = create_app()
    if auth_override:
        app.dependency_overrides[require_authentication] = lambda: _role_principal(role)
    if csrf_override:
        app.dependency_overrides[validate_csrf_token] = lambda: None
    snapshotter = _make_snapshotter()
    if pre_populate:
        bus_time = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        asyncio.run(_populate_buffer(snapshotter, [_build_synthetic_snapshot(bus_time=bus_time)]))
    app.state.system_metrics_snapshotter = snapshotter
    return app, snapshotter


class TestRouteAuthAndCsrf:
    """Integration tests via :class:`TestClient` for auth + CSRF + 503 fallthrough."""

    def test_get_metrics_returns_200_for_viewer(self) -> None:
        """VIEWER role passes the READ_SYSTEM_STATUS gate (positive case)."""
        app, _ = _build_app_with_snapshotter(auth_override=True, csrf_override=True)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/system")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 200
        body = response.json()
        assert body["type"] == "system_metrics_response"
        assert body["payload"]["type"] == "system_metrics"
        assert body["payload"]["disk"]["status"] == "warning"
        assert body["payload"]["disk"]["free_bytes"] == 15 * 1024**3

    def test_get_metrics_returns_401_without_auth(self) -> None:
        """No auth override → no Authorization header → 401 from auth dep."""
        app, _ = _build_app_with_snapshotter(auth_override=False, csrf_override=True)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/system")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 401

    def test_get_metrics_returns_503_when_snapshotter_missing(self) -> None:
        """No snapshotter on ``app.state`` → 503 with the expected detail."""
        app, _ = _build_app_with_snapshotter(auth_override=True, csrf_override=True)
        del app.state.system_metrics_snapshotter
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/system")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 503
        assert response.json()["detail"] == _SNAPSHOTTER_UNAVAILABLE_DETAIL

    def test_history_returns_200_for_viewer(self) -> None:
        """History endpoint also works for the VIEWER role."""
        app, _ = _build_app_with_snapshotter(auth_override=True, csrf_override=True)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/system/history?limit=10")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 200

    def test_tracemalloc_routes_require_authentication(self) -> None:
        """Without auth override, tracemalloc start/stop both return 401."""
        app, _ = _build_app_with_snapshotter(auth_override=False, csrf_override=True)
        client = TestClient(app)
        try:
            start_response = client.post("/api/metrics/system/tracemalloc/start")
            stop_response = client.post("/api/metrics/system/tracemalloc/stop")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert start_response.status_code == 401
        assert stop_response.status_code == 401

    @pytest.mark.parametrize(
        ("role", "expected_status"),
        [
            pytest.param(UserRole.AI_RESEARCHER, 403, id="ai-researcher-denied"),
            pytest.param(UserRole.AI_REVIEWER, 200, id="ai-reviewer-allowed"),
            pytest.param(UserRole.AI_DELEGATE, 200, id="ai-delegate-allowed"),
            pytest.param(UserRole.VIEWER, 403, id="viewer-denied"),
            pytest.param(UserRole.OPERATOR, 200, id="operator-allowed"),
            pytest.param(UserRole.ADMIN, 200, id="admin-allowed"),
        ],
    )
    def test_tracemalloc_role_permission_matrix(
        self,
        role: UserRole,
        expected_status: int,
    ) -> None:
        """Start and stop mutations follow MANAGE_RUNTIME_DIAGNOSTICS.

        Given: Each named permission set and valid CSRF,
        When: The principal starts and stops tracemalloc,
        Then: Granted principals receive 200 and denied principals receive 403.
        """
        app, _ = _build_app_with_snapshotter(
            auth_override=True,
            csrf_override=True,
            role=role,
        )
        client = TestClient(app)
        try:
            start_response = client.post("/api/metrics/system/tracemalloc/start?duration_s=1")
            stop_response = client.post("/api/metrics/system/tracemalloc/stop")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert start_response.status_code == expected_status
        assert stop_response.status_code == expected_status

    def test_tracemalloc_routes_bind_runtime_diagnostics_permission(self) -> None:
        """Both mutation signatures bind MANAGE_RUNTIME_DIAGNOSTICS."""
        for handler in (
            post_system_metrics_tracemalloc_start,
            post_system_metrics_tracemalloc_stop,
        ):
            signature = inspect.signature(handler)
            principal_annotation = signature.parameters["_principal"].annotation
            guard_closure = principal_annotation.__metadata__[0].dependency
            bound_permissions = [
                cell.cell_contents
                for cell in (guard_closure.__closure__ or [])
                if hasattr(cell, "cell_contents")
            ]
            assert Permission.MANAGE_RUNTIME_DIAGNOSTICS in bound_permissions

    def test_tracemalloc_routes_require_csrf(self) -> None:
        """Without CSRF override + no Bearer header, POST returns 403.

        With a Bearer-token authenticated request CSRF is bypassed; this
        test exercises the cookie-auth path by leaving CSRF live and
        sending no CSRF cookie / header.
        """
        app, _ = _build_app_with_snapshotter(
            auth_override=True,
            csrf_override=False,
            role=UserRole.OPERATOR,
        )
        client = TestClient(app)
        try:
            response = client.post("/api/metrics/system/tracemalloc/start")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 403


class TestRouteRegistration:
    """Assert routes mounted at the expected paths."""

    def test_routes_mounted_under_api_metrics_system(self) -> None:
        """All four new routes appear under ``/api/metrics/system*``."""
        app = create_app()
        paths = iter_fastapi_route_paths(app)
        assert "/api/metrics/system" in paths
        assert "/api/metrics/system/history" in paths
        assert "/api/metrics/system/tracemalloc/start" in paths
        assert "/api/metrics/system/tracemalloc/stop" in paths


class TestStartSystemMetricsSnapshotterHelper:
    """Lifespan startup helper — fail-closed attribute-absent contract."""

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When ``start()`` raises, no half-initialized object is attached."""

        class FailingSnapshotter:
            """Stand-in that fails its eager-sample call."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept and ignore constructor kwargs (engine / publisher)."""

            async def start(self) -> None:
                """Raise to simulate cgroup / psutil setup failure."""
                raise RuntimeError("synthetic startup failure")

        monkeypatch.setattr(
            "snapper.server.app.SystemMetricsSnapshotter",
            FailingSnapshotter,
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_system_metrics_snapshotter(app)

        assert not hasattr(app.state, "system_metrics_snapshotter")

    @pytest.mark.asyncio
    async def test_start_success_attaches_singleton(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On success, the singleton is attached after start completes."""
        started = AsyncMock()

        class SucceedingSnapshotter:
            """Stand-in with a bookkept ``start()`` to assert call ordering."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept and ignore constructor kwargs (engine / publisher)."""

            async def start(self) -> None:
                """Record the call without doing real I/O."""
                await started()

        monkeypatch.setattr(
            "snapper.server.app.SystemMetricsSnapshotter",
            SucceedingSnapshotter,
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_system_metrics_snapshotter(app)

        assert started.await_count == 1
        assert isinstance(app.state.system_metrics_snapshotter, SucceedingSnapshotter)

    @pytest.mark.asyncio
    async def test_start_wires_publisher_when_provided(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Production startup passes the shared bus publisher to the snapshotter."""
        publisher = MagicMock()
        constructed_publishers: list[object | None] = []

        class SucceedingSnapshotter:
            """Stand-in that records constructor publisher injection."""

            def __init__(
                self, *, engine: object | None = None, msg_publisher: object | None = None
            ) -> None:
                """Capture the publisher passed by the startup helper."""
                constructed_publishers.append(msg_publisher)

            async def start(self) -> None:
                """Complete startup without doing real I/O."""

        monkeypatch.setattr(
            "snapper.server.app.SystemMetricsSnapshotter",
            SucceedingSnapshotter,
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_system_metrics_snapshotter(app, msg_publisher=publisher)

        assert constructed_publishers == [publisher]
        assert isinstance(app.state.system_metrics_snapshotter, SucceedingSnapshotter)

    @pytest.mark.asyncio
    async def test_start_injects_primary_engine_from_db_url(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A db_url resolves the cached primary engine and injects it into the snapshotter."""
        primary_engine = object()
        constructed_engines: list[object | None] = []

        class SucceedingSnapshotter:
            """Stand-in that records the engine passed by the startup helper."""

            def __init__(
                self, *, engine: object | None = None, msg_publisher: object | None = None
            ) -> None:
                """Capture the engine the helper resolved from the db_url."""
                constructed_engines.append(engine)

            async def start(self) -> None:
                """Complete startup without doing real I/O."""

        monkeypatch.setattr(
            "snapper.server.app.SystemMetricsSnapshotter",
            SucceedingSnapshotter,
        )
        monkeypatch.setattr(
            "snapper.server.app.get_repository",
            lambda _db_url: SimpleNamespace(engine=primary_engine),
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_system_metrics_snapshotter(app, db_url="sqlite+aiosqlite:///./x.db")

        assert constructed_engines == [primary_engine]


class TestStopSystemMetricsSnapshotterHelper:
    """Lifespan shutdown helper — tolerates partial-init."""

    @pytest.mark.asyncio
    async def test_stop_when_attribute_absent_is_noop(self) -> None:
        """Shutdown helper exits silently when no snapshotter was attached."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_system_metrics_snapshotter(app)

    @pytest.mark.asyncio
    async def test_stop_when_attached_invokes_stop(self) -> None:
        """When attached, the helper awaits the snapshotter's ``stop()`` method."""
        stop_mock = AsyncMock()
        app = SimpleNamespace(
            state=SimpleNamespace(system_metrics_snapshotter=SimpleNamespace(stop=stop_mock))
        )

        await _stop_system_metrics_snapshotter(app)

        assert stop_mock.await_count == 1
