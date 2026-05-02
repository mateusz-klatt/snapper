"""Tests for instrument capability and venue fee schedule REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import InstrumentOrderCapabilityRow
from snapper.data.repository_types import VenueFeeScheduleRow
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


def _make_capability(
    exchange: str = "kraken",
    instrument_public_id: str = "inst-1",
) -> InstrumentOrderCapabilityRow:
    return InstrumentOrderCapabilityRow(
        public_id="cap-1",
        timestamp=_ts(),
        session_id="s1",
        sequence_id=1,
        instrument_public_id=instrument_public_id,
        exchange=exchange,
        supported_order_types=["market", "limit"],
        supports_post_only=True,
        supports_reduce_only=True,
        supports_amend_in_place=False,
        supports_native_stop_loss=True,
        supports_native_take_profit=True,
        supports_trailing_stop_client_side=True,
        supports_market_making=False,
        supports_short_selling=True,
        supports_leverage=True,
        max_leverage_long=5.0,
        max_leverage_short=3.0,
        min_notional=10.0,
        max_order_size=1000.0,
        top_of_book_quality="realtime",
    )


def _make_fee_schedule(
    exchange: str = "kraken",
    fee_tier: str = "default",
) -> VenueFeeScheduleRow:
    return VenueFeeScheduleRow(
        public_id="fee-1",
        timestamp=_ts(),
        session_id="s1",
        sequence_id=1,
        exchange=exchange,
        instrument_public_id=None,
        fee_tier=fee_tier,
        maker_bps=16.0,
        taker_bps=26.0,
        min_volume_30d=None,
        currency="USD",
    )


def _create_client(mock_repo: Any) -> TestClient:
    """Create test client with auth bypassed and mock repository injected."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestGetInstrumentCapabilities:
    """Tests for GET /api/instrument-capabilities."""

    def test_returns_list(self) -> None:
        """Given capabilities in DB, When requesting, Then 200 with payload."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(return_value=[_make_capability()])
        client = _create_client(repo)
        response = client.get("/api/instrument-capabilities")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "instrument_capability_list"
        assert data["count"] == 1
        item = data["payload"][0]
        assert item["exchange"] == "kraken"
        assert item["supported_order_types"] == ["market", "limit"]
        assert item["supports_post_only"] is True
        assert item["max_leverage_long"] == 5.0
        assert item["top_of_book_quality"] == "realtime"
        client.close()

    def test_empty(self) -> None:
        """Given no capabilities, When requesting, Then 200 with empty list."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get("/api/instrument-capabilities")
        assert response.status_code == 200
        assert response.json()["count"] == 0
        assert response.json()["payload"] == []
        client.close()

    def test_exchange_filter(self) -> None:
        """Given exchange filter, When requesting, Then filter forwarded to repo."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(return_value=[])
        client = _create_client(repo)
        client.get("/api/instrument-capabilities?exchange=kraken")
        repo.get_instrument_capabilities.assert_called_once()
        call_kwargs = repo.get_instrument_capabilities.call_args[1]
        assert call_kwargs["exchange"] == "kraken"
        client.close()

    def test_instrument_filter(self) -> None:
        """Given instrument filter, When requesting, Then filter forwarded to repo."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(return_value=[])
        client = _create_client(repo)
        client.get("/api/instrument-capabilities?instrument_public_id=inst-99")
        call_kwargs = repo.get_instrument_capabilities.call_args[1]
        assert call_kwargs["instrument_public_id"] == "inst-99"
        client.close()

    def test_as_of_forwarded(self) -> None:
        """Given as_of param, When requesting, Then forwarded to repo as_of."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(return_value=[])
        client = _create_client(repo)
        client.get("/api/instrument-capabilities?as_of=2026-06-01T00:00:00Z")
        call_kwargs = repo.get_instrument_capabilities.call_args[1]
        assert call_kwargs["as_of"] == datetime(2026, 6, 1, tzinfo=UTC)
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, When requesting, Then 500."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/instrument-capabilities")
        assert response.status_code == 500
        client.close()

    def test_provenance_fields(self) -> None:
        """Response carries provenance envelope fields."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(return_value=[_make_capability()])
        client = _create_client(repo)
        data = client.get("/api/instrument-capabilities").json()
        assert "session_id" in data
        assert "sequence_id" in data
        assert "public_id" in data
        assert "timestamp" in data
        client.close()

    def test_http_exception_passthrough(self) -> None:
        """Given repo raises HTTPException, When requesting, Then re-raised as-is."""
        repo = AsyncMock()
        repo.get_instrument_capabilities = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="forbidden")
        )
        client = _create_client(repo)
        response = client.get("/api/instrument-capabilities")
        assert response.status_code == 403
        client.close()


class TestGetVenueFeeSchedules:
    """Tests for GET /api/venue-fee-schedules."""

    def test_returns_list(self) -> None:
        """Given fee schedules in DB, When requesting, Then 200 with payload."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(return_value=[_make_fee_schedule()])
        client = _create_client(repo)
        response = client.get("/api/venue-fee-schedules")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "venue_fee_schedule_list"
        assert data["count"] == 1
        item = data["payload"][0]
        assert item["exchange"] == "kraken"
        assert item["fee_tier"] == "default"
        assert item["maker_bps"] == 16.0
        assert item["taker_bps"] == 26.0
        assert item["currency"] == "USD"
        client.close()

    def test_empty(self) -> None:
        """Given no fee schedules, When requesting, Then 200 with empty list."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get("/api/venue-fee-schedules")
        assert response.status_code == 200
        assert response.json()["count"] == 0
        assert response.json()["payload"] == []
        client.close()

    def test_as_of_forwarded(self) -> None:
        """Given as_of param, When requesting, Then forwarded to repo as_of."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(return_value=[])
        client = _create_client(repo)
        client.get("/api/venue-fee-schedules?as_of=2026-06-01T00:00:00Z")
        call_kwargs = repo.get_venue_fee_schedules.call_args[1]
        assert call_kwargs["as_of"] == datetime(2026, 6, 1, tzinfo=UTC)
        client.close()

    def test_exchange_filter(self) -> None:
        """Given exchange filter, When requesting, Then filter forwarded to repo."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(return_value=[])
        client = _create_client(repo)
        client.get("/api/venue-fee-schedules?exchange=walutomat")
        call_kwargs = repo.get_venue_fee_schedules.call_args[1]
        assert call_kwargs["exchange"] == "walutomat"
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, When requesting, Then 500."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/venue-fee-schedules")
        assert response.status_code == 500
        client.close()

    def test_nullable_fields(self) -> None:
        """Fee schedule with null instrument and min_volume renders correctly."""
        repo = AsyncMock()
        row = _make_fee_schedule()
        repo.get_venue_fee_schedules = AsyncMock(return_value=[row])
        client = _create_client(repo)
        data = client.get("/api/venue-fee-schedules").json()
        item = data["payload"][0]
        assert item["instrument_public_id"] is None
        assert item["min_volume_30d"] is None
        client.close()

    def test_provenance_fields(self) -> None:
        """Response carries provenance envelope fields."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(return_value=[_make_fee_schedule()])
        client = _create_client(repo)
        data = client.get("/api/venue-fee-schedules").json()
        assert "session_id" in data
        assert "sequence_id" in data
        assert "public_id" in data
        assert "timestamp" in data
        client.close()

    def test_http_exception_passthrough(self) -> None:
        """Given repo raises HTTPException, When requesting, Then re-raised as-is."""
        repo = AsyncMock()
        repo.get_venue_fee_schedules = AsyncMock(
            side_effect=HTTPException(status_code=403, detail="forbidden")
        )
        client = _create_client(repo)
        response = client.get("/api/venue-fee-schedules")
        assert response.status_code == 403
        client.close()
