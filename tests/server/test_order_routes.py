"""Tests for manual order creation and cancellation REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

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
    return datetime(2026, 4, 10, tzinfo=UTC)


def _make_plan_row(
    public_id: str = "plan-1",
    status: str = "active",
    *,
    with_child_order: bool = False,
) -> dict[str, Any]:
    now = _ts()
    params: dict[str, Any] = {"order_type": "limit", "side": "buy", "price": 50000.0}
    if with_child_order:
        params.update(
            {
                "child_client_order_id": "cid-child-1",
                "native_instrument": "BTC-USD",
                "venue_order_type": "limit",
            }
        )
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": "manual_once",
        "created_by_user_id": "test_user",
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": "inst-1",
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
        "params": params,
        "status": status,
        "created_at": now,
        "started_at": None,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": None,
    }


def _create_order_body() -> dict[str, Any]:
    return {
        "type": "create_order_command",
        "session_id": "s1",
        "sequence_id": 1,
        "public_id": "req-1",
        "timestamp": _ts().isoformat(),
        "payload": {
            "instrument": "BTC-USD",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 50000.0,
            "wallet_public_id": "wallet-1",
        },
    }


def _cancel_order_body() -> dict[str, Any]:
    return {
        "type": "cancel_order_command",
        "session_id": "s1",
        "sequence_id": 1,
        "public_id": "req-2",
        "timestamp": _ts().isoformat(),
        "payload": {"reason": "changed mind"},
    }


def _create_client(
    mock_repo: Any,
    allow_manual_orders: bool = True,
) -> TestClient:
    """Create test client with auth bypassed and mock repository injected.

    Args:
        mock_repo: AsyncMock repository.
        allow_manual_orders: Value for the settings flag.

    Returns:
        TestClient with overrides applied.
    """
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    mock_settings = MagicMock()
    mock_settings.allow_manual_orders = allow_manual_orders
    app.state.settings = mock_settings

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestCreateOrder:
    """Tests for POST /api/orders."""

    def test_create_order_disabled_returns_403(self) -> None:
        """Given allow_manual_orders=False, When creating, Then 403."""
        repo = AsyncMock()
        client = _create_client(repo, allow_manual_orders=False)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 403
        assert "disabled" in response.json()["detail"].lower()
        client.close()

    def test_create_order_success(self) -> None:
        """Given valid order params, When creating, Then 200 with plan."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "execution_plan_response"
        assert data["payload"]["plan_type"] == "manual_once"
        assert data["payload"]["status"] == "active"
        client.close()

    def test_create_order_invalid_params(self) -> None:
        """Given limit order without price, When creating, Then 422."""
        repo = AsyncMock()
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "limit"
        body["payload"]["price"] = None
        response = client.post("/api/orders", json=body)
        assert response.status_code == 422
        client.close()

    def test_create_order_idempotency_conflict(self) -> None:
        """Given duplicate idempotency key, When creating, Then 409."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(side_effect=Exception("UNIQUE constraint failed"))
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 409
        client.close()

    def test_create_order_command_failure_marks_plan_failed(self) -> None:
        """Given trade command insert fails, When creating, Then plan marked failed."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(side_effect=Exception("DB error"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 500
        repo.update_execution_plan_status.assert_called_once()
        call_kwargs = repo.update_execution_plan_status.call_args[1]
        assert call_kwargs["new_status"] == "failed"
        client.close()

    def test_create_order_wallet_not_accessible(self) -> None:
        """Given restricted wallet, When creating as OPERATOR, Then 403."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "other-wallet"}]
        )

        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        mock_settings = MagicMock()
        mock_settings.allow_manual_orders = True
        app.state.settings = mock_settings

        def skip_csrf() -> None:
            return None

        def skip_auth() -> AuthPrincipal:
            return AuthPrincipal(
                username="op_user",
                role=UserRole.OPERATOR,
                operator_public_ids=["op-1"],
            )

        app.dependency_overrides[validate_csrf_token] = skip_csrf
        app.dependency_overrides[require_authentication] = skip_auth
        app.dependency_overrides[get_repository_dependency] = lambda: repo
        client = TestClient(app)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 403
        client.close()

    def test_create_order_generic_plan_error(self) -> None:
        """Given unexpected plan insert error, When creating, Then 500."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(side_effect=Exception("unexpected"))
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 500
        client.close()

    def test_create_order_plan_not_found_after_insert(self) -> None:
        """Given plan insert succeeds but GET fails, When creating, Then 500."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=None)
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 500
        client.close()

    def test_create_order_stop_with_stop_price(self) -> None:
        """Given stop order with stop_price, When creating, Then 200 and stop_price in params."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "stop"
        body["payload"]["price"] = None
        body["payload"]["stop_price"] = 48000.0
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        assert plan_insert["params"]["stop_price"] == 48000.0
        client.close()

    def test_create_order_with_leverage(self) -> None:
        """Given order with leverage, When creating, Then leverage in params."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["leverage"] = 5
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        assert plan_insert["params"]["leverage"] == 5
        client.close()

    def test_create_order_market_no_price(self) -> None:
        """Given market order, When creating without price, Then 200."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "market"
        body["payload"].pop("price", None)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        client.close()


class TestCancelOrder:
    """Tests for POST /api/orders/{id}/cancel."""

    def test_cancel_active_plan(self) -> None:
        """Given active plan, When cancelling, Then cancel_requested."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(
            side_effect=[
                _make_plan_row(status="active"),
                _make_plan_row(status="cancel_requested"),
            ]
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["status"] == "cancel_requested"
        client.close()

    def test_cancel_not_found(self) -> None:
        """Given nonexistent plan, When cancelling, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders/nonexistent/cancel", json=_cancel_order_body())
        assert response.status_code == 404
        client.close()

    def test_cancel_already_terminal(self) -> None:
        """Given completed plan, When cancelling, Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="completed"))
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 409
        client.close()

    def test_cancel_plan_updated_but_not_found(self) -> None:
        """Given update succeeds but GET returns None, Then 500."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(
            side_effect=[
                _make_plan_row(status="active"),
                None,
            ]
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 500
        client.close()

    def test_cancel_update_returns_none_concurrent(self) -> None:
        """Given race condition (plan closed between read and update), Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="active"))
        repo.update_execution_plan_status = AsyncMock(return_value=None)
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 409
        client.close()

    def test_cancel_with_child_order_emits_cancel_command(self) -> None:
        """Given plan with child_client_order_id, Then cancel TradeCommand is inserted.

        Verifies the cancel flow emits a venue-facing cancel command with
        ``command_type='cancel'`` and the child order's ``client_order_id``
        so the outbox dispatcher publishes OrderCancelData to the venue.
        Also asserts the command hydrates the venue-assigned
        ``exchange_order_id`` looked up via the repository.
        """
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(
            side_effect=[
                _make_plan_row(status="active", with_child_order=True),
                _make_plan_row(status="cancel_requested", with_child_order=True),
            ]
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-42")
        repo.insert_trade_command = AsyncMock(return_value=(42, "cmd-cancel"))
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 200
        repo.insert_trade_command.assert_called_once()
        inserted = repo.insert_trade_command.call_args[0][0]
        assert inserted["command_type"] == "cancel"
        assert inserted["client_order_id"] == "cid-child-1"
        assert inserted["plan_public_id"] == "plan-1"
        assert inserted["exchange_order_id"] == "ex-42"
        client.close()

    def test_cancel_before_venue_ack_emits_command_with_null_exchange_id(self) -> None:
        """Cancel before venue ACK passes a null exchange_order_id through.

        When the active Order row has not yet been assigned an exchange
        id the repository lookup returns ``None``; the cancel command
        must still be inserted so the venue adapter (paper/Kraken) can
        fall back to cancelling by ``client_order_id``.
        """
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(
            side_effect=[
                _make_plan_row(status="active", with_child_order=True),
                _make_plan_row(status="cancel_requested", with_child_order=True),
            ]
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
        repo.insert_trade_command = AsyncMock(return_value=(42, "cmd-cancel"))
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 200
        inserted = repo.insert_trade_command.call_args[0][0]
        assert inserted["exchange_order_id"] is None
        client.close()

    def test_cancel_by_client_order_id_resolves_plan(self) -> None:
        """Given a child client_order_id, When cancelling, Then lookup + cancel."""
        repo = AsyncMock()
        repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-1")
        repo.get_execution_plan = AsyncMock(
            side_effect=[
                _make_plan_row(status="active", with_child_order=True),
                _make_plan_row(status="cancel_requested", with_child_order=True),
            ]
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.insert_trade_command = AsyncMock(return_value=(42, "cmd-cancel"))
        client = _create_client(repo)
        response = client.post(
            "/api/orders/by-client-order-id/cid-child-1/cancel",
            json=_cancel_order_body(),
        )
        assert response.status_code == 200
        repo.get_plan_public_id_for_client_order_id.assert_awaited_once_with("cid-child-1")
        client.close()

    def test_cancel_by_client_order_id_not_found(self) -> None:
        """Given unknown client_order_id, Then 404."""
        repo = AsyncMock()
        repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post(
            "/api/orders/by-client-order-id/unknown-cid/cancel",
            json=_cancel_order_body(),
        )
        assert response.status_code == 404
        client.close()

    def test_cancel_with_child_order_insert_failure_marks_plan_failed(self) -> None:
        """When the cancel TradeCommand insert fails, plan transitions to failed.

        Previously the insert exception was silently swallowed and the
        plan was left stuck in ``cancel_requested`` with no venue
        cancel ever sent. Phase 1.5 review fix: on insert failure the
        service now transitions the plan to ``failed`` with a
        ``last_error`` and returns HTTP 500 so the caller learns the
        cancel did not reach the venue.
        """
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(
            side_effect=[
                _make_plan_row(status="active", with_child_order=True),
                _make_plan_row(status="cancel_requested", with_child_order=True),
            ]
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-42")
        repo.insert_trade_command = AsyncMock(side_effect=Exception("DB error"))
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 500
        statuses = [
            call.kwargs["new_status"] for call in repo.update_execution_plan_status.await_args_list
        ]
        assert "cancel_requested" in statuses
        assert "failed" in statuses
        client.close()
