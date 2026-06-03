"""Route tests for :mod:`snapper.server.market_feed_health_routes`.

Covers the single ``GET /api/market/feed-health`` handler: payload
projection from repository rows, the optional ``exchange`` filter
pass-through, and the ``READ_SYSTEM_STATUS`` RBAC binding.
"""

import inspect
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import InstrumentFeedHealthRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.market_feed_health_routes import get_market_feed_health

_T0 = datetime(2026, 6, 3, 12, 0, tzinfo=UTC)


def _principal() -> AuthPrincipal:
    """Build an ADMIN principal for the RBAC-gated handler."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000099",
    )


def _make_request() -> Request:
    """Build a mock :class:`Request` carrying the REST tracker on state."""
    request = MagicMock(spec=Request)
    request.app.state.rest_tracker = SequenceTracker()
    return request


def _repo_returning(rows: list[InstrumentFeedHealthRow]) -> MagicMock:
    """Build a repository mock whose feed-health method returns ``rows``."""
    repo = MagicMock()
    repo.list_instrument_feed_health = AsyncMock(return_value=rows)
    return repo


def _row() -> InstrumentFeedHealthRow:
    """Build one feed-health read row."""
    return InstrumentFeedHealthRow(
        coordinator="coord-0",
        exchange="kraken",
        channel="ticker",
        symbol="BTC/USD",
        status="confirmed",
        requested_at=_T0,
        confirmed_at=_T0,
        last_seen_data_at=_T0,
        last_error=None,
        retry_count=0,
        snapshot_at=_T0,
    )


class TestFeedHealthRoute:
    """``get_market_feed_health`` projects rows onto the envelope."""

    @pytest.mark.asyncio
    async def test_returns_projected_rows_unfiltered(self) -> None:
        """Repository rows project onto the payload with no exchange filter."""
        repo = _repo_returning([_row()])
        result = await get_market_feed_health(
            request=_make_request(),
            _user=_principal(),
            repo=repo,
        )
        assert result.type == "market_feed_health"
        assert result.payload.exchange is None
        assert result.payload.fresh_within_seconds is None
        assert len(result.payload.rows) == 1
        entry = result.payload.rows[0]
        assert entry.coordinator == "coord-0"
        assert entry.exchange == "kraken"
        assert entry.symbol == "BTC/USD"
        assert entry.status == "confirmed"
        assert entry.confirmed_at == _T0
        repo.list_instrument_feed_health.assert_awaited_once_with(
            exchange=None, fresh_within_seconds=None
        )

    @pytest.mark.asyncio
    async def test_exchange_filter_passed_through(self) -> None:
        """The exchange query reaches the repository and echoes into the payload."""
        repo = _repo_returning([])
        result = await get_market_feed_health(
            request=_make_request(),
            _user=_principal(),
            repo=repo,
            exchange="kraken_futures",
        )
        assert result.payload.rows == []
        assert result.payload.exchange == "kraken_futures"
        repo.list_instrument_feed_health.assert_awaited_once_with(
            exchange="kraken_futures", fresh_within_seconds=None
        )

    @pytest.mark.asyncio
    async def test_fresh_within_seconds_passed_through(self) -> None:
        """The staleness filter reaches the repository and echoes into the payload."""
        repo = _repo_returning([])
        result = await get_market_feed_health(
            request=_make_request(),
            _user=_principal(),
            repo=repo,
            fresh_within_seconds=120,
        )
        assert result.payload.fresh_within_seconds == 120
        repo.list_instrument_feed_health.assert_awaited_once_with(
            exchange=None, fresh_within_seconds=120
        )

    def test_endpoint_binds_read_system_status_permission(self) -> None:
        """The route signature binds ``require_permission(READ_SYSTEM_STATUS)``."""
        signature = inspect.signature(get_market_feed_health)
        principal_annotation = signature.parameters["_user"].annotation
        guard_closure = principal_annotation.__metadata__[0].dependency
        bound_permissions = [
            cell.cell_contents
            for cell in (guard_closure.__closure__ or [])
            if hasattr(cell, "cell_contents")
        ]
        assert Permission.READ_SYSTEM_STATUS in bound_permissions

    def test_read_system_status_permission_rejects_unprivileged_principal(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A principal without ``READ_SYSTEM_STATUS`` receives HTTP 403."""
        restricted_permissions: dict[UserRole, set[Permission]] = {UserRole.ADMIN: set()}
        monkeypatch.setattr("snapper.auth.dependencies.ROLE_PERMISSIONS", restricted_permissions)
        permission_checker = require_permission(Permission.READ_SYSTEM_STATUS)
        with pytest.raises(HTTPException) as exc:
            permission_checker(_principal())
        assert exc.value.status_code == status.HTTP_403_FORBIDDEN
        assert exc.value.detail == "Permission 'read:system_status' required"
