"""Tests for ``GET /api/metrics/notifications`` (BE-3c §D11)."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.metrics_routes import get_notification_metrics


def _make_request() -> Request:
    """Return a FastAPI ``Request`` mock with a real tracker attached."""
    req = MagicMock(spec=Request)
    req.app.state.rest_tracker = SequenceTracker()
    return req


def _admin_principal() -> AuthPrincipal:
    """Return an admin principal (holds ``READ_SYSTEM_STATUS``)."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="admin-1",
    )


class TestNotificationMetrics:
    """DB-derived counter aggregation + zero-default behaviour."""

    @pytest.mark.asyncio
    async def test_returns_all_metric_fields(self) -> None:
        """Each status the repo reports gets mapped to its counter field."""
        repo = MagicMock()
        repo.count_deliveries_by_status = AsyncMock(
            return_value={
                "sent": 42,
                "failed": 3,
                "unregistered": 7,
                "cancelled_scope": 2,
                "queued": 11,
            }
        )

        response = await get_notification_metrics(
            request=_make_request(),
            _principal=_admin_principal(),
            repo=repo,
        )

        assert response.payload.delivery_success_total == 42
        assert response.payload.delivery_failed_total == 3
        assert response.payload.delivery_410_unregistered_total == 7
        assert response.payload.delivery_cancelled_scope_total == 2
        assert response.payload.outbox_queued_depth == 11

    @pytest.mark.asyncio
    async def test_empty_state_returns_zeros(self) -> None:
        """An empty repo aggregate maps to zero on every counter."""
        repo = MagicMock()
        repo.count_deliveries_by_status = AsyncMock(return_value={})

        response = await get_notification_metrics(
            request=_make_request(),
            _principal=_admin_principal(),
            repo=repo,
        )

        assert response.payload.delivery_success_total == 0
        assert response.payload.delivery_failed_total == 0
        assert response.payload.delivery_410_unregistered_total == 0
        assert response.payload.delivery_cancelled_scope_total == 0
        assert response.payload.outbox_queued_depth == 0

    @pytest.mark.asyncio
    async def test_unknown_status_ignored(self) -> None:
        """A repo that reports an extra status doesn't blow up the response."""
        repo = MagicMock()
        repo.count_deliveries_by_status = AsyncMock(return_value={"sent": 5, "future_status": 999})

        response = await get_notification_metrics(
            request=_make_request(),
            _principal=_admin_principal(),
            repo=repo,
        )

        assert response.payload.delivery_success_total == 5
        assert response.payload.delivery_failed_total == 0

    def test_endpoint_requires_read_system_status_permission(self) -> None:
        """Route signature binds ``require_permission(READ_SYSTEM_STATUS)`` via Annotated.

        Sanity check on the handler's dependency annotation — the real
        enforcement happens inside FastAPI's dependency resolution,
        covered by the auth-dependency test suite. This guards
        against a silent removal of the guard in a future refactor.
        """
        import inspect

        from snapper.auth.domain.permissions import Permission

        signature = inspect.signature(get_notification_metrics)
        principal_annotation = signature.parameters["_principal"].annotation
        metadata = principal_annotation.__metadata__
        assert len(metadata) == 1
        guard_closure = metadata[0].dependency
        bound_permissions = [
            cell.cell_contents
            for cell in (guard_closure.__closure__ or [])
            if hasattr(cell, "cell_contents")
        ]
        assert Permission.READ_SYSTEM_STATUS in bound_permissions
