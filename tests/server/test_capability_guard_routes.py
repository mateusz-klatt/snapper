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

    def test_forged_instrument_public_id_is_canonicalised_server_side(self) -> None:
        """A forged ``instrument_public_id`` cannot bypass the guard.

        Given: a body where ``instrument`` is a tradable symbol (passes
            the guard) but ``instrument_public_id`` is a DIFFERENT value
            the client attempts to smuggle in (e.g., a market-data-only
            instrument UUID),
        When: the route canonicalises ``instrument_public_id`` via
            ``repo.get_instrument_public_id_by_symbol(body.instrument,
            body.exchange)``,
        Then: the client-provided ``instrument_public_id`` is ignored;
            the resolved Instrument.public_id from the (symbol, exchange)
            pair flows into the trade-command insert instead. Closes the
            final-gate regression where a forged identifier could bypass
            canonicalisation.
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_instrument_public_id_by_symbol = AsyncMock(
            return_value="00000000-0000-7000-8000-000000000abc"
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.list_active_wallets = AsyncMock(
            return_value=[
                {
                    "public_id": "wallet-1",
                    "label": "main",
                    "description": None,
                    "is_paper": False,
                    "timestamp": _ts(),
                    "session_id": "s1",
                    "sequence_id": 1,
                }
            ]
        )
        repo.list_active_wallet_credentials = AsyncMock(
            return_value=[
                {
                    "public_id": "cred-1",
                    "wallet_public_id": "wallet-1",
                    "exchange": "kraken",
                    "credential_type": "api",
                }
            ]
        )
        repo.get_execution_plan = AsyncMock(
            return_value={
                "public_id": "plan-1",
                "timestamp": _ts(),
                "session_id": "s1",
                "sequence_id": 1,
                "plan_type": "manual_once",
                "created_by_user_id": "test_user",
                "created_by_strategy": None,
                "created_via": "api",
                "instrument_public_id": "00000000-0000-7000-8000-000000000abc",
                "exchange": "kraken",
                "mode": "live",
                "shard_key": "kraken:BTC-USD:live",
                "wallet_public_id": "wallet-1",
                "operator_public_id": None,
                "total_quantity": 0.5,
                "filled_quantity": 0.0,
                "side": "buy",
                "parent_plan_public_id": None,
                "position_cycle_public_id": None,
                "params": {"order_type": "limit", "side": "buy", "price": 50000.0},
                "status": "active",
                "created_at": _ts(),
                "started_at": None,
                "completed_at": None,
                "expires_at": None,
                "cancel_requested_at": None,
                "last_evaluated_at": None,
                "last_error": None,
                "idempotency_key": None,
            }
        )
        body = {
            "type": "create_order_command",
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "req-1",
            "timestamp": _ts().isoformat(),
            "payload": {
                "instrument": "BTC-USD",
                "instrument_public_id": "00000000-0000-7000-8000-0000000000f1",
                "exchange": "kraken",
                "mode": "live",
                "side": "buy",
                "order_type": "limit",
                "quantity": 0.5,
                "price": 50000.0,
                "wallet_public_id": "wallet-1",
            },
        }
        client = _create_client(repo)
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=True,
        ):
            response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        call = repo.get_instrument_public_id_by_symbol.await_args
        assert call is not None
        assert call.kwargs["native_symbol"] == "BTC-USD"
        assert call.kwargs["exchange"] == "kraken"
        assert call.kwargs["as_of"] is not None
        plan_insert_call = repo.insert_execution_plan.await_args
        assert plan_insert_call is not None
        persisted_plan_row = plan_insert_call.args[0]
        assert persisted_plan_row["instrument_public_id"] == (
            "00000000-0000-7000-8000-000000000abc"
        )
        client.close()

    def test_create_order_rejects_unknown_instrument_after_tradable_check(self) -> None:
        """Unresolvable ``(symbol, exchange)`` pairs return 422 unknown_instrument.

        Given: a body where ``is_tradeable`` returns True (mapper cache
            has the capability) but the Instrument row does not exist in
            the repository (cache/DB drift),
        When: canonicalisation runs after the guard,
        Then: the route surfaces HTTP 422 with
            ``error_code=unknown_instrument`` rather than silently
            persisting a symbol-as-UUID value.
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        body = {
            "type": "create_order_command",
            "session_id": "s1",
            "sequence_id": 1,
            "public_id": "req-1",
            "timestamp": _ts().isoformat(),
            "payload": {
                "instrument": "PHANTOM-USD",
                "instrument_public_id": "PHANTOM-USD",
                "exchange": "kraken",
                "mode": "live",
                "side": "buy",
                "order_type": "limit",
                "quantity": 0.5,
                "price": 1.0,
                "wallet_public_id": "wallet-1",
            },
        }
        client = _create_client(repo)
        with patch(
            "snapper.server._capability_guard.is_tradeable",
            return_value=True,
        ):
            response = client.post("/api/orders", json=body)
        assert response.status_code == 422
        detail = response.json()["detail"]
        assert detail["error_code"] == "unknown_instrument"
        assert detail["symbol"] == "PHANTOM-USD"
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
