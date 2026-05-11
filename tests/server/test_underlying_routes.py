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
from snapper.data.repository_types import InstrumentRelatedRow
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


def _make_related(
    native_symbol: str,
    exchange: str,
    relationship_type: str,
    *,
    is_selected: bool = False,
    contract_family: str | None = None,
    asset_type: str = "crypto",
    instrument_public_id: str | None = None,
) -> InstrumentRelatedRow:
    """Build an ``InstrumentRelatedRow`` fixture for related-route tests."""
    return InstrumentRelatedRow(
        instrument_public_id=instrument_public_id or f"inst-{native_symbol}-{exchange}",
        native_symbol=native_symbol,
        exchange=exchange,
        relationship_type=relationship_type,
        contract_family=contract_family,
        asset_type=asset_type,
        is_selected=is_selected,
    )


class TestGetRelatedInstruments:
    """Tests for GET /api/instruments/{exchange}/{native_symbol}/related."""

    def test_returns_grouped_payload(self) -> None:
        """Given a mapped symbol with siblings, When requesting, Then 200 + groups.

        Asserts EXACT/DERIVATIVE groups appear in fixed order with the
        UI-facing labels and the selected chip is flagged + sorted first
        within EXACT.
        """
        underlying = _make_underlying("BTC", "Bitcoin")
        related = [
            _make_related("BTC-USD-PERP", "kraken_futures", "derivative", contract_family="BTC"),
            _make_related("BTC-USD", "kraken", "exact", is_selected=True),
            _make_related("BTC-EUR", "kraken", "exact"),
        ]
        repo = AsyncMock()
        repo.get_related_instruments_for_symbol = AsyncMock(return_value=(underlying, related))
        client = _create_client(repo)
        response = client.get("/api/instruments/kraken/BTC-USD/related")
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "related_instruments"
        payload = data["payload"]
        assert payload["selected"] == {"exchange": "kraken", "native_symbol": "BTC-USD"}
        assert payload["underlying"]["ticker"] == "BTC"
        rels = [g["relationship_type"] for g in payload["groups"]]
        assert rels == ["exact", "derivative"]
        labels = {g["relationship_type"]: g["label"] for g in payload["groups"]}
        assert labels["exact"] == "Same underlying"
        assert labels["derivative"] == "Derivatives"
        exact_items = payload["groups"][0]["items"]
        assert exact_items[0]["native_symbol"] == "BTC-USD"
        assert exact_items[0]["is_selected"] is True
        client.close()

    def test_derivative_group_sorted_by_contract_family_then_symbol(self) -> None:
        """Given mixed derivative siblings, When requesting, Then sorted deterministically."""
        underlying = _make_underlying("ES", "S&P E-Mini")
        related = [
            _make_related("MESM6-CME", "kraken_equities", "derivative", contract_family="MES"),
            _make_related("ESZ5-CME", "kraken_equities", "derivative", contract_family="ES"),
            _make_related("ESM6-CME", "kraken_equities", "derivative", contract_family="ES"),
        ]
        repo = AsyncMock()
        repo.get_related_instruments_for_symbol = AsyncMock(return_value=(underlying, related))
        client = _create_client(repo)
        response = client.get("/api/instruments/kraken_equities/ESM6-CME/related")
        assert response.status_code == 200
        items = response.json()["payload"]["groups"][0]["items"]
        assert [r["native_symbol"] for r in items] == ["ESM6-CME", "ESZ5-CME", "MESM6-CME"]
        client.close()

    def test_proxy_group_renders_with_label(self) -> None:
        """Given proxy mapping, When requesting, Then proxy group with label 'Proxies'."""
        underlying = _make_underlying("GLD", "Gold proxy")
        related = [
            _make_related("XAU-USD", "kraken", "proxy", is_selected=True),
            _make_related("GC-USD-FUT", "kraken_futures", "proxy", contract_family="GC"),
        ]
        repo = AsyncMock()
        repo.get_related_instruments_for_symbol = AsyncMock(return_value=(underlying, related))
        client = _create_client(repo)
        response = client.get("/api/instruments/kraken/XAU-USD/related")
        assert response.status_code == 200
        groups = response.json()["payload"]["groups"]
        assert len(groups) == 1
        assert groups[0]["relationship_type"] == "proxy"
        assert groups[0]["label"] == "Proxies"
        client.close()

    def test_orphan_returns_empty_groups_with_null_underlying(self) -> None:
        """Given orphan (unknown or unmapped) symbol, When requesting, Then 200 + null + []."""
        repo = AsyncMock()
        repo.get_related_instruments_for_symbol = AsyncMock(return_value=(None, []))
        client = _create_client(repo)
        response = client.get("/api/instruments/kraken/UNKNOWN-USD/related")
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["underlying"] is None
        assert payload["groups"] == []
        assert payload["selected"] == {"exchange": "kraken", "native_symbol": "UNKNOWN-USD"}
        client.close()

    def test_as_of_query_is_forwarded(self) -> None:
        """Given as_of query param, When requesting, Then repo receives that timestamp."""
        repo = AsyncMock()
        repo.get_related_instruments_for_symbol = AsyncMock(return_value=(None, []))
        client = _create_client(repo)
        response = client.get("/api/instruments/kraken/BTC-USD/related?as_of=2026-03-15T00:00:00Z")
        assert response.status_code == 200
        args, _ = repo.get_related_instruments_for_symbol.call_args
        assert args[0] == "kraken"
        assert args[1] == "BTC-USD"
        assert args[2] == datetime(2026, 3, 15, tzinfo=UTC)
        client.close()

    def test_database_error(self) -> None:
        """Given repo error, When requesting, Then 500."""
        repo = AsyncMock()
        repo.get_related_instruments_for_symbol = AsyncMock(side_effect=Exception("DB down"))
        client = _create_client(repo)
        response = client.get("/api/instruments/kraken/BTC-USD/related")
        assert response.status_code == 500
        client.close()
