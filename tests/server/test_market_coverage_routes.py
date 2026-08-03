"""Route tests for :mod:`snapper.server.market_coverage_routes`.

Covers the single ``GET /api/market/coverage`` handler: payload
projection from repository rows, default + custom freshness windows,
and the ``READ_SYSTEM_STATUS`` RBAC binding.
"""

import inspect
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from annotated_types import Gt
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import MarketDataCoverageRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.market_coverage_routes import get_market_data_coverage


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


def _repo_returning(rows: list[MarketDataCoverageRow]) -> MagicMock:
    """Build a repository mock whose coverage method returns ``rows``."""
    repo = MagicMock()
    repo.get_market_data_coverage = AsyncMock(return_value=rows)
    return repo


class TestCoverageRoute:
    """``get_market_data_coverage`` projects rows onto the envelope."""

    @pytest.mark.asyncio
    async def test_returns_projected_rows_with_default_windows(self) -> None:
        """Repository rows project onto the payload with the default windows."""
        rows: list[MarketDataCoverageRow] = [
            MarketDataCoverageRow(
                exchange="kraken",
                instruments=4,
                fresh_ticks=1,
                fresh_candles=1,
                gated_off=1,
                dark=2,
            )
        ]
        repo = _repo_returning(rows)
        result = await get_market_data_coverage(
            request=_make_request(),
            _user=_principal(),
            repo=repo,
        )
        assert result.type == "market_data_coverage"
        assert result.payload.tick_window_seconds == 600
        assert result.payload.candle_window_seconds == 1800
        assert len(result.payload.exchanges) == 1
        entry = result.payload.exchanges[0]
        assert entry.exchange == "kraken"
        assert entry.instruments == 4
        assert entry.dark == 2
        repo.get_market_data_coverage.assert_awaited_once_with(
            tick_window_seconds=600, candle_window_seconds=1800
        )

    @pytest.mark.asyncio
    async def test_custom_windows_passed_through(self) -> None:
        """Custom query windows reach the repository and echo into the payload."""
        repo = _repo_returning([])
        result = await get_market_data_coverage(
            request=_make_request(),
            _user=_principal(),
            repo=repo,
            tick_window_seconds=120,
            candle_window_seconds=300,
        )
        assert result.payload.exchanges == []
        assert result.payload.tick_window_seconds == 120
        assert result.payload.candle_window_seconds == 300
        repo.get_market_data_coverage.assert_awaited_once_with(
            tick_window_seconds=120, candle_window_seconds=300
        )

    def test_window_params_declare_strictly_positive_bound(self) -> None:
        """Both freshness windows are declared ``Query(gt=0)`` so 0/negative are rejected (422)."""
        signature = inspect.signature(get_market_data_coverage)
        for name in ("tick_window_seconds", "candle_window_seconds"):
            query_meta = signature.parameters[name].annotation.__metadata__[0]
            gt_constraints = [m for m in query_meta.metadata if isinstance(m, Gt)]
            assert gt_constraints and gt_constraints[0].gt == 0

    def test_endpoint_binds_read_system_status_permission(self) -> None:
        """The route signature binds ``require_permission(READ_SYSTEM_STATUS)``."""
        signature = inspect.signature(get_market_data_coverage)
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
        monkeypatch.setattr(
            "snapper.auth.domain.permissions.ROLE_PERMISSIONS", restricted_permissions
        )
        permission_checker = require_permission(Permission.READ_SYSTEM_STATUS)
        error_arg_1 = _principal()
        with pytest.raises(HTTPException) as exc:
            permission_checker(error_arg_1)
        assert exc.value.status_code == status.HTTP_403_FORBIDDEN
        assert exc.value.detail == "Permission 'read:system_status' required"
