"""Tests for position cycle admin endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


NOW = datetime(2026, 4, 13, 12, 0, 0, tzinfo=UTC)


def _make_cycle_row(
    public_id: str = "cycle-1",
    shard_key: str = "kraken.BTC-USD.live.waaaa",
    opened_at: datetime | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Build a PositionCycleRow dict for route tests."""
    base: dict[str, Any] = {
        "public_id": public_id,
        "timestamp": NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "instrument_public_id": "inst-btc",
        "exchange": "kraken",
        "mode": "live",
        "shard_key": shard_key,
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "direction": "long",
        "max_qty": 1.0,
        "status": "open",
        "opened_at": opened_at or NOW - timedelta(hours=100),
        "closed_at": None,
        "opening_command_public_id": None,
        "closing_command_public_id": None,
    }
    base.update(overrides)
    return base


def _create_client(mock_repo: Any) -> TestClient:
    """Create test client with auth bypassed and mock repo."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_admin", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestListOpenCycles:
    """Tests for GET /api/position-cycles/open."""

    def test_list_open_cycles_returns_cycles(self) -> None:
        """Given open cycles exist, When GET /open, Then list returned."""
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(return_value=[_make_cycle_row()])
        client = _create_client(repo)
        response = client.get("/api/position-cycles/open")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 1
        assert data["payload"][0]["cycle_public_id"] == "cycle-1"
        assert data["payload"][0]["age_hours"] > 0

    def test_list_open_cycles_empty(self) -> None:
        """Given no open cycles, When GET /open, Then empty list."""
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get("/api/position-cycles/open")
        assert response.status_code == 200
        assert response.json()["count"] == 0

    def test_list_open_cycles_with_min_age(self) -> None:
        """Given min_age_hours param, When GET /open, Then repo called with filter."""
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get("/api/position-cycles/open?min_age_hours=48")
        assert response.status_code == 200
        call_kwargs = repo.get_all_open_position_cycles.call_args
        assert call_kwargs.kwargs.get("opened_before") is not None


class TestCloseOrphanCycle:
    """Tests for POST /api/position-cycles/close-orphan."""

    def test_close_orphan_success(self) -> None:
        """Given open cycle, When POST /close-orphan, Then cycle closed."""
        repo = AsyncMock()
        repo.close_position_cycle = AsyncMock(return_value=42)
        client = _create_client(repo)
        response = client.post("/api/position-cycles/close-orphan?cycle_public_id=cycle-1")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["closed_count"] == 1
        assert "cycle-1" in data["payload"]["closed_cycle_ids"]

    def test_close_orphan_not_found(self) -> None:
        """Given no matching cycle, When POST /close-orphan, Then 404."""
        repo = AsyncMock()
        repo.close_position_cycle = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/position-cycles/close-orphan?cycle_public_id=nonexistent")
        assert response.status_code == 404


class TestSweepOrphans:
    """Tests for POST /api/position-cycles/sweep-orphans."""

    def test_sweep_orphans_closes_old_cycles(self) -> None:
        """Given old open cycles, When POST /sweep-orphans, Then all closed."""
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(
            return_value=[
                _make_cycle_row("cycle-old-1"),
                _make_cycle_row("cycle-old-2"),
            ]
        )
        repo.close_position_cycle = AsyncMock(return_value=99)
        client = _create_client(repo)
        response = client.post("/api/position-cycles/sweep-orphans?min_age_hours=72")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["closed_count"] == 2
        assert "cycle-old-1" in data["payload"]["closed_cycle_ids"]
        assert "cycle-old-2" in data["payload"]["closed_cycle_ids"]

    def test_sweep_orphans_no_old_cycles(self) -> None:
        """Given no old cycles, When POST /sweep-orphans, Then empty result."""
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.post("/api/position-cycles/sweep-orphans")
        assert response.status_code == 200
        assert response.json()["payload"]["closed_count"] == 0

    def test_sweep_orphans_default_threshold(self) -> None:
        """Default min_age_hours is 72 when not specified."""
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(return_value=[])
        client = _create_client(repo)
        client.post("/api/position-cycles/sweep-orphans")
        call_kwargs = repo.get_all_open_position_cycles.call_args
        assert call_kwargs.kwargs.get("opened_before") is not None

    def test_sweep_orphans_skips_failed_close(self) -> None:
        """Cycle that fails to close is excluded from results.

        Given: two cycles, one fails close (returns None),
        When: sweep runs,
        Then: only successfully closed cycle in results.
        """
        repo = AsyncMock()
        repo.get_all_open_position_cycles = AsyncMock(
            return_value=[
                _make_cycle_row("cycle-ok"),
                _make_cycle_row("cycle-fail"),
            ]
        )
        repo.close_position_cycle = AsyncMock(side_effect=[42, None])
        client = _create_client(repo)
        response = client.post("/api/position-cycles/sweep-orphans?min_age_hours=1")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["closed_count"] == 1
        assert "cycle-ok" in data["payload"]["closed_cycle_ids"]
        assert "cycle-fail" not in data["payload"]["closed_cycle_ids"]

    def test_sweep_orphans_rejects_negative_age(self) -> None:
        """Negative min_age_hours is rejected with 400.

        Given: min_age_hours=-1,
        When: POST /sweep-orphans,
        Then: 400 bad request.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        response = client.post("/api/position-cycles/sweep-orphans?min_age_hours=-1")
        assert response.status_code == 400

    def test_sweep_orphans_rejects_zero_age(self) -> None:
        """Zero min_age_hours is rejected (minimum 1 hour).

        Given: min_age_hours=0,
        When: POST /sweep-orphans,
        Then: 400 bad request.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        response = client.post("/api/position-cycles/sweep-orphans?min_age_hours=0")
        assert response.status_code == 400


def _create_operator_client(mock_repo: Any) -> TestClient:
    """Create test client with OPERATOR role (not ADMIN)."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_operator", role=UserRole.OPERATOR)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestPermissions:
    """Tests for admin-only permission enforcement."""

    def test_operator_cannot_list_cycles(self) -> None:
        """Operator role lacks MANAGE_USERS, so GET /open returns 403.

        Given: user with OPERATOR role,
        When: GET /api/position-cycles/open,
        Then: 403 forbidden.
        """
        repo = AsyncMock()
        client = _create_operator_client(repo)
        response = client.get("/api/position-cycles/open")
        assert response.status_code == 403

    def test_operator_cannot_sweep(self) -> None:
        """Operator role lacks MANAGE_USERS, so POST /sweep-orphans returns 403.

        Given: user with OPERATOR role,
        When: POST /api/position-cycles/sweep-orphans,
        Then: 403 forbidden.
        """
        repo = AsyncMock()
        client = _create_operator_client(repo)
        response = client.post("/api/position-cycles/sweep-orphans")
        assert response.status_code == 403
