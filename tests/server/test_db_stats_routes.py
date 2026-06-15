"""Tests for ``GET /api/metrics/db/tables``.

Covers:

* Handler-level mapping snapshot → wire schema (envelope + per-table
  rows round-trip).
* 503 fallthrough when the snapshotter attribute is absent / pre-set
  ``None`` / disabled / has not yet run a sample. Only the cold-start
  case carries a ``Retry-After`` header.
* TestClient integration: 200 for VIEWER, 401 without auth.
* Route registration assertion.
"""

import contextlib
import inspect
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi.testclient import TestClient

from snapper.application.db_stats.snapshotter import DbStatsSnapshot
from snapper.application.db_stats.snapshotter import TableStats
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.metrics_routes import _DB_STATS_DISABLED_DETAIL
from snapper.server.metrics_routes import _DB_STATS_NOT_YET_RUN_DETAIL
from snapper.server.metrics_routes import _DB_STATS_UNAVAILABLE_DETAIL
from snapper.server.metrics_routes import get_db_table_stats
from tests.helpers.fastapi_routes import iter_fastapi_route_paths


def _viewer_principal() -> AuthPrincipal:
    """Return a VIEWER principal — has ``READ_SYSTEM_STATUS``."""
    return AuthPrincipal(
        username="viewer",
        role=UserRole.VIEWER,
        user_public_id="viewer-1",
    )


def _build_snapshot() -> DbStatsSnapshot:
    """Return a populated :class:`DbStatsSnapshot` with one event + one state row.

    Both wire branches exercised: event row with ``current``/``closed``
    null, state row with ``current``/``closed`` populated.
    """
    started = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    completed = datetime(2026, 5, 1, 12, 0, 5, tzinfo=UTC)
    return DbStatsSnapshot(
        snapshot_started_at=started,
        snapshot_completed_at=completed,
        interval_seconds=60,
        tables=(
            TableStats(
                table="orders",
                table_kind="state",
                total=12,
                current=8,
                closed=4,
                archivable=None,
                is_stale=False,
                last_sampled_at=started,
            ),
            TableStats(
                table="telemetry",
                table_kind="event",
                total=1234,
                current=None,
                closed=None,
                archivable=789,
                is_stale=True,
                last_sampled_at=started,
            ),
        ),
    )


def _make_request_with_snapshotter(snapshotter: Any | None) -> Request:
    """Return a FastAPI ``Request`` whose ``app.state`` has the given snapshotter."""
    req = MagicMock(spec=Request)
    state = SimpleNamespace(rest_tracker=SequenceTracker())
    if snapshotter is not None:
        state.db_stats_snapshotter = snapshotter
    req.app.state = state
    return req


class TestGetDbStatsHandler:
    """Handler-level coverage for the route."""

    @pytest.mark.asyncio
    async def test_returns_snapshot_when_snapshotter_active(self) -> None:
        """Latest snapshot is mapped onto the wire envelope."""
        snapshotter = SimpleNamespace(
            disabled=False,
            interval_seconds=60,
            latest_snapshot=_build_snapshot(),
        )

        response = await get_db_table_stats(
            request=_make_request_with_snapshotter(snapshotter),
            _principal=_viewer_principal(),
        )

        assert response.type == "db_stats_response"
        assert response.payload.type == "db_stats"
        assert response.payload.interval_seconds == 60
        assert response.payload.snapshot_started_at == datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        assert len(response.payload.tables) == 2
        orders = response.payload.tables[0]
        assert orders.table == "orders"
        assert orders.table_kind == "state"
        assert orders.total == 12
        assert orders.current == 8
        assert orders.closed == 4
        assert orders.archivable is None
        assert orders.is_stale is False
        telemetry = response.payload.tables[1]
        assert telemetry.table == "telemetry"
        assert telemetry.table_kind == "event"
        assert telemetry.current is None
        assert telemetry.closed is None
        assert telemetry.archivable == 789
        assert telemetry.is_stale is True

    @pytest.mark.asyncio
    async def test_returns_503_when_snapshotter_attribute_missing(self) -> None:
        """No snapshotter attached → 503 with the unavailable detail (no Retry-After)."""
        with pytest.raises(HTTPException) as exc:
            await get_db_table_stats(
                request=_make_request_with_snapshotter(None),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _DB_STATS_UNAVAILABLE_DETAIL
        assert exc.value.headers is None or "Retry-After" not in (exc.value.headers or {})

    @pytest.mark.asyncio
    async def test_returns_503_when_snapshotter_disabled(self) -> None:
        """Snapshotter in disabled state → 503 with the disabled detail (no Retry-After)."""
        snapshotter = SimpleNamespace(
            disabled=True,
            interval_seconds=60,
            latest_snapshot=None,
        )

        with pytest.raises(HTTPException) as exc:
            await get_db_table_stats(
                request=_make_request_with_snapshotter(snapshotter),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _DB_STATS_DISABLED_DETAIL
        assert exc.value.headers is None or "Retry-After" not in (exc.value.headers or {})

    @pytest.mark.asyncio
    async def test_returns_503_when_no_sample_yet_with_dynamic_retry_after(self) -> None:
        """Cold-start window → 503 distinct detail + ``Retry-After`` echoing interval."""
        snapshotter = SimpleNamespace(
            disabled=False,
            interval_seconds=60,
            latest_snapshot=None,
        )

        with pytest.raises(HTTPException) as exc:
            await get_db_table_stats(
                request=_make_request_with_snapshotter(snapshotter),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _DB_STATS_NOT_YET_RUN_DETAIL
        assert exc.value.headers is not None
        assert exc.value.headers["Retry-After"] == "60"

    @pytest.mark.asyncio
    async def test_retry_after_reflects_configured_interval(self) -> None:
        """``Retry-After`` derives from the interval, not a hardcoded constant."""
        snapshotter = SimpleNamespace(
            disabled=False,
            interval_seconds=300,
            latest_snapshot=None,
        )
        with pytest.raises(HTTPException) as exc:
            await get_db_table_stats(
                request=_make_request_with_snapshotter(snapshotter),
                _principal=_viewer_principal(),
            )
        assert exc.value.headers is not None
        assert exc.value.headers["Retry-After"] == "300"

    def test_endpoint_requires_read_system_status_permission(self) -> None:
        """Route signature binds ``require_permission(READ_SYSTEM_STATUS)``."""
        signature = inspect.signature(get_db_table_stats)
        principal_annotation = signature.parameters["_principal"].annotation
        guard_closure = principal_annotation.__metadata__[0].dependency
        bound_permissions = [
            cell.cell_contents
            for cell in (guard_closure.__closure__ or [])
            if hasattr(cell, "cell_contents")
        ]
        assert Permission.READ_SYSTEM_STATUS in bound_permissions


def _build_app_with_snapshotter(
    *,
    snapshotter: Any,
    auth_override: bool,
) -> Any:
    """Build a fresh app with the given DB-stats snapshotter attached."""
    app = create_app()
    if auth_override:
        app.dependency_overrides[require_authentication] = _viewer_principal
    app.state.db_stats_snapshotter = snapshotter
    return app


class TestGetDbStatsViaTestClient:
    """End-to-end path through FastAPI dependency resolution."""

    def test_returns_200_for_viewer(self) -> None:
        """VIEWER role passes the permission gate."""
        snapshotter = SimpleNamespace(
            disabled=False,
            interval_seconds=60,
            latest_snapshot=_build_snapshot(),
        )
        app = _build_app_with_snapshotter(snapshotter=snapshotter, auth_override=True)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/db/tables")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 200
        body = response.json()
        assert body["type"] == "db_stats_response"
        assert body["payload"]["type"] == "db_stats"
        assert len(body["payload"]["tables"]) == 2

    def test_returns_401_without_auth(self) -> None:
        """No auth override → no Authorization header → 401."""
        snapshotter = SimpleNamespace(
            disabled=False,
            interval_seconds=60,
            latest_snapshot=_build_snapshot(),
        )
        app = _build_app_with_snapshotter(snapshotter=snapshotter, auth_override=False)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/db/tables")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 401


class TestRouteRegistration:
    """Assert route is mounted at the expected path."""

    def test_route_mounted_at_api_metrics_db_tables(self) -> None:
        """``/api/metrics/db/tables`` appears in the FastAPI routes table."""
        app = create_app()
        paths = iter_fastapi_route_paths(app)
        assert "/api/metrics/db/tables" in paths
