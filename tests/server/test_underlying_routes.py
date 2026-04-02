"""Tests for underlying asset REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import InstrumentUnderlyingRow
from snapper.data.repository_types import UnderlyingAssetRow
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC)


def _make_underlying(ticker: str = "SPX", name: str = "S&P 500") -> UnderlyingAssetRow:
    return UnderlyingAssetRow(
        public_id="ua-1",
        ticker=ticker,
        name=name,
        asset_class="index",
        sector="US Large Cap",
        description=None,
        timestamp=_ts(),
        session_id="s1",
        sequence_id=1,
        instrument_count=1,
    )


def _make_instrument_row(
    native_symbol: str = "ESM6-CME",
    exchange: str = "kraken_equities",
) -> InstrumentUnderlyingRow:
    return InstrumentUnderlyingRow(
        public_id="ium-1",
        instrument_public_id="inst-1",
        underlying_public_id="ua-1",
        relationship_type="derivative",
        contract_family="ES",
        native_symbol=native_symbol,
        exchange=exchange,
        asset_type="index",
        timestamp=_ts(),
        session_id="s1",
        sequence_id=1,
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


class TestGetUnderlyings:
    """Tests for GET /api/underlyings."""

    def test_returns_list(self) -> None:
        """Given underlyings in DB, When requesting, Then 200 with payload."""
        repo = AsyncMock()
        repo.get_underlying_assets = AsyncMock(return_value=[_make_underlying()])
        client = _create_client(repo)
        response = client.get("/api/underlyings")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "underlying_asset_list"
        assert data["count"] == 1
        assert data["payload"][0]["ticker"] == "SPX"
        assert data["payload"][0]["instrument_count"] == 1
        client.close()

    def test_empty(self) -> None:
        """Given no underlyings, When requesting, Then 200 with empty list."""
        repo = AsyncMock()
        repo.get_underlying_assets = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get("/api/underlyings")
        assert response.status_code == 200
        assert response.json()["count"] == 0
        assert response.json()["payload"] == []
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, When requesting, Then 500."""
        repo = AsyncMock()
        repo.get_underlying_assets = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/underlyings")
        assert response.status_code == 500
        client.close()


class TestGetUnderlyingInstruments:
    """Tests for GET /api/underlyings/{ticker}/instruments."""

    def test_returns_instruments(self) -> None:
        """Given valid ticker, When requesting, Then 200 with instruments."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_instruments_by_underlying = AsyncMock(return_value=[_make_instrument_row()])
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/instruments")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "underlying_instrument_list"
        assert data["count"] == 1
        assert data["payload"][0]["native_symbol"] == "ESM6-CME"
        assert data["payload"][0]["relationship_type"] == "derivative"
        assert data["payload"][0]["contract_family"] == "ES"
        client.close()

    def test_not_found(self) -> None:
        """Given nonexistent ticker, When requesting, Then 404."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/underlyings/NOPE/instruments")
        assert response.status_code == 404
        assert "NOPE" in response.json()["detail"]
        client.close()

    def test_filter_relationship_type(self) -> None:
        """Given relationship_type param, When requesting, Then filters."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_instruments_by_underlying = AsyncMock(return_value=[_make_instrument_row()])
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/instruments?relationship_type=derivative")
        assert response.status_code == 200
        repo.get_instruments_by_underlying.assert_called_once()
        call_kwargs = repo.get_instruments_by_underlying.call_args
        assert call_kwargs.kwargs.get("relationship_types") == ["derivative"]
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, When requesting, Then 500."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/instruments")
        assert response.status_code == 500
        client.close()
