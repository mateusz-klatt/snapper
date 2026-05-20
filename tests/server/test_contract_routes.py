"""Tests for front-month and contract listing REST API endpoints."""

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
from snapper.data.repository_types import InstrumentContractRow
from snapper.data.repository_types import InstrumentFrontMonthRow
from snapper.data.repository_types import UnderlyingAssetRow
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    """Return a fixed timestamp for deterministic test data."""
    return datetime(2026, 1, 1, tzinfo=UTC)


def _make_underlying(ticker: str = "SPX") -> UnderlyingAssetRow:
    """Build an UnderlyingAssetRow fixture."""
    return UnderlyingAssetRow(
        public_id="ua-1",
        ticker=ticker,
        name={"en": "S&P 500"},
        asset_class="index",
        sector=None,
        description=None,
        timestamp=_ts(),
        session_id="s1",
        sequence_id=1,
        instrument_count=2,
    )


def _make_front_month_row() -> InstrumentFrontMonthRow:
    """Build an InstrumentFrontMonthRow fixture for front-month tests."""
    return InstrumentFrontMonthRow(
        instrument_public_id="inst-1",
        native_symbol="ESM6-CME",
        exchange="kraken_equities",
        expiry_at=datetime(2026, 6, 20, 16, 30, tzinfo=UTC),
        relationship_type="derivative",
        contract_family="ES",
    )


def _make_contract_row(
    ipid: str = "inst-1",
    native_symbol: str = "ESM6-CME",
    expiry_at: datetime | None = None,
    family: str | None = "ES",
    is_front: bool = False,
) -> InstrumentContractRow:
    """Build an InstrumentContractRow fixture for contract-list tests."""
    return InstrumentContractRow(
        instrument_public_id=ipid,
        native_symbol=native_symbol,
        exchange="kraken_equities",
        expiry_at=expiry_at or datetime(2026, 6, 20, tzinfo=UTC),
        instrument_kind="future",
        relationship_type="derivative",
        contract_family=family,
        is_front_month=is_front,
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


class TestGetFrontMonth:
    """Tests for GET /api/underlyings/{ticker}/front-month."""

    def test_returns_front_month(self) -> None:
        """Given valid ticker with active futures, return 200 with front-month payload."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_front_month_instrument = AsyncMock(return_value=_make_front_month_row())
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/front-month")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "front_month"
        payload = data["payload"]
        assert payload["native_symbol"] == "ESM6-CME"
        assert payload["exchange"] == "kraken_equities"
        assert payload["relationship_type"] == "derivative"
        assert payload["contract_family"] == "ES"
        assert "expiry_at" in payload
        assert "instrument_public_id" in payload
        client.close()

    def test_underlying_not_found(self) -> None:
        """Given nonexistent ticker, return 404 with ticker in detail."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/underlyings/NOPE/front-month")
        assert response.status_code == 404
        assert "NOPE" in response.json()["detail"]
        client.close()

    def test_no_active_contracts(self) -> None:
        """Given ticker exists but no active futures, return 404."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_front_month_instrument = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/front-month")
        assert response.status_code == 404
        assert "SPX" in response.json()["detail"]
        client.close()

    def test_with_exchange_filter(self) -> None:
        """Given exchange query param, verify repo called with exchange filter."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_front_month_instrument = AsyncMock(return_value=_make_front_month_row())
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/front-month?exchange=kraken_equities")
        assert response.status_code == 200
        repo.get_front_month_instrument.assert_called_once()
        call_kwargs = repo.get_front_month_instrument.call_args
        assert call_kwargs.kwargs.get("exchange") == "kraken_equities"
        client.close()

    def test_with_contract_family_filter(self) -> None:
        """Given contract_family query param, verify repo called with contract_family filter."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_front_month_instrument = AsyncMock(return_value=_make_front_month_row())
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/front-month?contract_family=ES")
        assert response.status_code == 200
        repo.get_front_month_instrument.assert_called_once()
        call_kwargs = repo.get_front_month_instrument.call_args
        assert call_kwargs.kwargs.get("contract_family") == "ES"
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, return 500."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/front-month")
        assert response.status_code == 500
        client.close()


class TestGetContracts:
    """Tests for GET /api/underlyings/{ticker}/contracts."""

    def test_returns_contract_list(self) -> None:
        """Given valid ticker with contracts, return 200 with contract list payload."""
        rows = [
            _make_contract_row(ipid="inst-1", native_symbol="ESM6-CME", is_front=True),
            _make_contract_row(
                ipid="inst-2",
                native_symbol="ESU6-CME",
                expiry_at=datetime(2026, 9, 19, tzinfo=UTC),
                is_front=False,
            ),
        ]
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_contracts_for_underlying = AsyncMock(return_value=rows)
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/contracts")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "contract_list"
        assert data["count"] == 2
        first = data["payload"][0]
        assert first["native_symbol"] == "ESM6-CME"
        assert first["is_front_month"] is True
        second = data["payload"][1]
        assert second["native_symbol"] == "ESU6-CME"
        assert second["is_front_month"] is False
        assert "instrument_kind" in first
        assert "relationship_type" in first
        assert "contract_family" in first
        client.close()

    def test_underlying_not_found(self) -> None:
        """Given nonexistent ticker, return 404 with ticker in detail."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/underlyings/NOPE/contracts")
        assert response.status_code == 404
        assert "NOPE" in response.json()["detail"]
        client.close()

    def test_empty_contracts(self) -> None:
        """Given ticker with no contracts, return 200 with empty payload."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_contracts_for_underlying = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/contracts")
        assert response.status_code == 200
        data = response.json()
        assert data["count"] == 0
        assert data["payload"] == []
        client.close()

    def test_include_expired_param(self) -> None:
        """Given include_expired=true, verify repo called with include_expired=True."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())
        repo.get_contracts_for_underlying = AsyncMock(return_value=[_make_contract_row()])
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/contracts?include_expired=true")
        assert response.status_code == 200
        repo.get_contracts_for_underlying.assert_called_once()
        call_kwargs = repo.get_contracts_for_underlying.call_args
        assert call_kwargs.kwargs.get("include_expired") is True
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, return 500."""
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/underlyings/SPX/contracts")
        assert response.status_code == 500
        client.close()
