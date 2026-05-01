"""Tests for ``GET /api/metrics/retention``.

Covers:

* Handler-level mapping summary → wire schema (envelope + per-policy
  results round-trip).
* 503 fallthrough when the scheduler attribute is absent / pre-set
  ``None`` / disabled / has not yet run a tick.
* TestClient integration: 200 for VIEWER, 401 without auth.
* Route registration assertion (SC#14).
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

from snapper.application.retention.service import RetentionPolicyRunResult
from snapper.application.retention.service import RetentionRunSummary
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.metrics_routes import _RETENTION_DISABLED_DETAIL
from snapper.server.metrics_routes import _RETENTION_NOT_YET_RUN_DETAIL
from snapper.server.metrics_routes import _RETENTION_UNAVAILABLE_DETAIL
from snapper.server.metrics_routes import get_retention_metrics


def _viewer_principal() -> AuthPrincipal:
    """Return a VIEWER principal — has ``READ_SYSTEM_STATUS``."""
    return AuthPrincipal(
        username="viewer",
        role=UserRole.VIEWER,
        user_public_id="viewer-1",
    )


def _build_summary() -> RetentionRunSummary:
    """Return a populated :class:`RetentionRunSummary` with two per-policy results.

    One success + one error so the wire mapping is exercised on both
    branches.
    """
    started = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    completed = datetime(2026, 5, 1, 12, 0, 5, tzinfo=UTC)
    return RetentionRunSummary(
        run_started_at=started,
        run_completed_at=completed,
        dry_run=False,
        results=[
            RetentionPolicyRunResult(
                table="telemetry",
                retain_days=1,
                backlog_lookback_days=30,
                day_start="2026-03-30",
                day_end="2026-04-29",
                archived_rows=120,
                purged_rows=120,
                files_written=31,
                error=None,
            ),
            RetentionPolicyRunResult(
                table="telemetry",
                retain_days=1,
                backlog_lookback_days=30,
                day_start=None,
                day_end=None,
                archived_rows=0,
                purged_rows=0,
                files_written=0,
                error="synthetic boom",
            ),
        ],
    )


def _make_request_with_scheduler(
    scheduler: Any | None,
) -> Request:
    """Return a FastAPI ``Request`` whose ``app.state`` has the given scheduler."""
    req = MagicMock(spec=Request)
    state = SimpleNamespace(rest_tracker=SequenceTracker())
    if scheduler is not None:
        state.retention_scheduler = scheduler
    req.app.state = state
    return req


class TestGetRetentionMetricsHandler:
    """Handler-level coverage for the route."""

    @pytest.mark.asyncio
    async def test_returns_summary_when_scheduler_active(self) -> None:
        """Latest summary is mapped onto the wire envelope."""
        scheduler = SimpleNamespace(
            disabled=False,
            last_run_summary=_build_summary(),
        )

        response = await get_retention_metrics(
            request=_make_request_with_scheduler(scheduler),
            _principal=_viewer_principal(),
        )

        assert response.type == "retention_run_response"
        assert response.payload.type == "retention_run"
        assert response.payload.dry_run is False
        assert response.payload.run_started_at == datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
        assert len(response.payload.results) == 2
        first = response.payload.results[0]
        assert first.table == "telemetry"
        assert first.archived_rows == 120
        assert first.purged_rows == 120
        assert first.error is None
        second = response.payload.results[1]
        assert second.error == "synthetic boom"
        assert second.day_start is None
        assert second.day_end is None

    @pytest.mark.asyncio
    async def test_returns_503_when_scheduler_attribute_missing(self) -> None:
        """No scheduler attached → 503 with the unavailable detail."""
        with pytest.raises(HTTPException) as exc:
            await get_retention_metrics(
                request=_make_request_with_scheduler(None),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _RETENTION_UNAVAILABLE_DETAIL

    @pytest.mark.asyncio
    async def test_returns_503_when_scheduler_disabled(self) -> None:
        """Scheduler in disabled state → 503 with the disabled detail."""
        scheduler = SimpleNamespace(disabled=True, last_run_summary=None)

        with pytest.raises(HTTPException) as exc:
            await get_retention_metrics(
                request=_make_request_with_scheduler(scheduler),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _RETENTION_DISABLED_DETAIL

    @pytest.mark.asyncio
    async def test_returns_503_when_no_run_yet(self) -> None:
        """Scheduler attached but cold-start window → 503 distinct detail."""
        scheduler = SimpleNamespace(disabled=False, last_run_summary=None)

        with pytest.raises(HTTPException) as exc:
            await get_retention_metrics(
                request=_make_request_with_scheduler(scheduler),
                _principal=_viewer_principal(),
            )
        assert exc.value.status_code == 503
        assert exc.value.detail == _RETENTION_NOT_YET_RUN_DETAIL

    def test_endpoint_requires_read_system_status_permission(self) -> None:
        """Route signature binds ``require_permission(READ_SYSTEM_STATUS)``."""
        signature = inspect.signature(get_retention_metrics)
        principal_annotation = signature.parameters["_principal"].annotation
        guard_closure = principal_annotation.__metadata__[0].dependency
        bound_permissions = [
            cell.cell_contents
            for cell in (guard_closure.__closure__ or [])
            if hasattr(cell, "cell_contents")
        ]
        assert Permission.READ_SYSTEM_STATUS in bound_permissions


def _build_app_with_scheduler(
    *,
    scheduler: Any,
    auth_override: bool,
) -> Any:
    """Build a fresh app with the given retention scheduler attached.

    Returns the app; caller wraps in :class:`TestClient` without the
    ``with`` context manager so heavy lifespan startup never fires.
    """
    app = create_app()
    if auth_override:
        app.dependency_overrides[require_authentication] = lambda: _viewer_principal()
    app.state.retention_scheduler = scheduler
    return app


class TestGetRetentionMetricsViaTestClient:
    """End-to-end path through FastAPI dependency resolution."""

    def test_returns_200_for_viewer(self) -> None:
        """VIEWER role passes the permission gate."""
        scheduler = SimpleNamespace(
            disabled=False,
            last_run_summary=_build_summary(),
        )
        app = _build_app_with_scheduler(scheduler=scheduler, auth_override=True)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/retention")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 200
        body = response.json()
        assert body["type"] == "retention_run_response"
        assert body["payload"]["type"] == "retention_run"
        assert len(body["payload"]["results"]) == 2

    def test_returns_401_without_auth(self) -> None:
        """No auth override → no Authorization header → 401."""
        scheduler = SimpleNamespace(
            disabled=False,
            last_run_summary=_build_summary(),
        )
        app = _build_app_with_scheduler(scheduler=scheduler, auth_override=False)
        client = TestClient(app)
        try:
            response = client.get("/api/metrics/retention")
        finally:
            with contextlib.suppress(Exception):
                client.close()
        assert response.status_code == 401


class TestRouteRegistration:
    """SC#14 — assert route is mounted at the expected path."""

    def test_route_mounted_at_api_metrics_retention(self) -> None:
        """``/api/metrics/retention`` appears in the FastAPI routes table."""
        app = create_app()
        paths = {r.path for r in app.routes if hasattr(r, "path")}
        assert "/api/metrics/retention" in paths
