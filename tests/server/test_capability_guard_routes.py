"""End-to-end regression tests for the capability-guard on submit routes.

Each test opts out of the ``bypass_capability_guard`` fixture via the
``@pytest.mark.capability_guard`` marker so the real
``snapper.server._capability_guard.require_tradable`` is exercised.
``is_tradeable`` is stubbed at the module-import path
(``snapper.server._capability_guard.is_tradeable``) to model both
tradable + market-data-only capability rows without seeding the
symbol-mapper cache.

Covers all three submit routes:
- ``POST /api/orders``
- ``POST /api/execution-plans`` (bracket create)
- ``POST /api/trailing-stops``
"""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    return datetime(2026, 4, 21, 12, 0, 0, tzinfo=UTC)


_INSTRUMENT_UUID = "00000000-0000-7000-8000-0000000000e1"


def _make_cycle_row(exchange: str = "kraken_equities") -> dict[str, Any]:
    """Return a minimal open PositionCycle row for bracket/trailing-stop guard tests.

    ``instrument_public_id`` is a real UUID-shape string so the guard's
    ``resolve_native_symbol`` helper routes through the repository UUID
    path — the repo mock then resolves it to the native TradFi symbol
    that ``is_tradeable`` is asked about.
    """
    now = _ts()
    return {
        "public_id": "cycle-1",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "exchange": exchange,
        "instrument_public_id": _INSTRUMENT_UUID,
        "mode": "live",
        "shard_key": f"{exchange}:MNQM6-CME:live",
        "status": "open",
        "direction": "long",
        "opened_at": now,
        "max_qty": 1.0,
        "version": 1,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
    }


def _create_client(mock_repo: Any) -> TestClient:
    """Create a TestClient with auth bypassed and mock repository injected."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.settings = MagicMock()

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


@pytest.mark.capability_guard
class TestOrderRouteCapabilityGuard:
    """``POST /api/orders`` rejects market-data-only instruments with 422."""

    def test_market_data_only_instrument_returns_422(self) -> None:
        """TradFi submit is rejected before any trade_command is inserted.

        Given: ``is_tradeable`` returns False (``can_trade=False``),
        When: the client POSTs an order for ``MNQM6-CME`` on
            ``kraken_equities``,
        Then: HTTP 422 with structured detail ``error_code=instrument_market_data_only``;
            ``repo.insert_execution_plan`` is never called (the guard
            fires before any plan or trade_command write).
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        body = {
            "type": "create_order_command",
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "req-1",
            "timestamp": _ts().isoformat(),
            "payload": {
                "instrument": "MNQM6-CME",
                "instrument_public_id": "MNQM6-CME",
                "exchange": "kraken_equities",
                "mode": "live",
                "side": "buy",
                "order_type": "limit",
                "quantity": 1.0,
                "price": 23950.0,
                "wallet_public_id": "wallet-1",
            },
        }
        client = _create_client(repo)
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=False,
        ):
            response = client.post("/api/orders", json=body)
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["error_code"] == "instrument_market_data_only"
        assert detail["symbol"] == "MNQM6-CME"
        assert detail["exchange"] == "kraken_equities"
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()


@pytest.mark.capability_guard
class TestBracketCapabilityGuard:
    """``POST /api/execution-plans`` rejects market-data-only cycles with 422."""

    def test_market_data_only_cycle_returns_422(self) -> None:
        """Bracket create on a TradFi cycle is rejected before insert.

        Given: an open cycle on ``kraken_equities`` and a repo whose
            ``get_symbol_for_instrument`` resolves the cycle's instrument
            UUID to ``MNQM6-CME``, plus ``is_tradeable`` returning False,
        When: the client POSTs a bracket create,
        Then: HTTP 422 with ``error_code=instrument_market_data_only``
            and ``repo.insert_execution_plan`` is never called.
        """
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_symbol_for_instrument = AsyncMock(return_value="MNQM6-CME")
        body = {
            "type": "create_bracket_command",
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "req-1",
            "timestamp": _ts().isoformat(),
            "payload": {
                "position_cycle_public_id": "cycle-1",
                "sl_price": 23000.0,
                "tp_price": 25000.0,
            },
        }
        client = _create_client(repo)
        executor = MagicMock()
        executor._check_capabilities = AsyncMock(return_value=[])
        client.app.state.plan_executor = executor
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=False,
        ):
            response = client.post("/api/execution-plans", json=body)
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["error_code"] == "instrument_market_data_only"
        assert detail["symbol"] == "MNQM6-CME"
        repo.insert_execution_plan.assert_not_called()
        client.close()


@pytest.mark.capability_guard
class TestTrailingStopCapabilityGuard:
    """``POST /api/trailing-stops`` rejects market-data-only cycles with 422."""

    def test_market_data_only_cycle_returns_422(self) -> None:
        """Trailing-stop create on a TradFi cycle is rejected before insert.

        Given: an open cycle on ``kraken_equities`` that resolves to
            ``MNQM6-CME`` and ``is_tradeable`` returning False,
        When: the client POSTs a trailing-stop create,
        Then: HTTP 422 with ``error_code=instrument_market_data_only``
            and ``repo.insert_execution_plan`` is never called.
        """
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_symbol_for_instrument = AsyncMock(return_value="MNQM6-CME")
        body = {
            "type": "create_trailing_stop_command",
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "req-1",
            "timestamp": _ts().isoformat(),
            "payload": {
                "position_cycle_public_id": "cycle-1",
                "trailing_pct": 1.0,
            },
        }
        client = _create_client(repo)
        executor = MagicMock()
        executor._check_capabilities = AsyncMock(return_value=[])
        client.app.state.plan_executor = executor
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=False,
        ):
            response = client.post("/api/trailing-stops", json=body)
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["error_code"] == "instrument_market_data_only"
        assert detail["symbol"] == "MNQM6-CME"
        repo.insert_execution_plan.assert_not_called()
        client.close()
