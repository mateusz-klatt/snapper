"""Tests for manual order creation and cancellation REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.dependencies import get_caps_enforcer_dependency


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


def _cancel_body() -> dict[str, Any]:
    """Return a minimal CancelOrderCommand envelope for cap-violation tests."""
    return {
        "type": "cancel_order_command",
        "session_id": "s1",
        "sequence_id": 1,
        "public_id": "req-cancel",
        "timestamp": _ts().isoformat(),
        "payload": {"reason": "unit-test"},
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


def _create_client(mock_repo: Any) -> TestClient:
    """Create test client with auth bypassed and mock repository injected.

    Args:
        mock_repo: AsyncMock repository.

    Returns:
        TestClient with overrides applied.
    """
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    mock_settings = MagicMock()
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

    def test_create_order_caps_violation_returns_422(self) -> None:
        """``TradingCapsEnforcer.guard`` rejection maps to 422 caps_violation.

        Given: a stubbed enforcer whose ``guard()`` raises
            ``CapsViolationError('max_open_orders')``,
        When: the client POSTs a valid order,
        Then: response is HTTP 422 carrying the structured error
            body per §9.2 — verifies the cap-violation branch in
            ``create_order``.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)

        class _CapsGuard:
            async def __aenter__(self) -> None:
                raise CapsViolationError("max_open_orders", attempted=6.0, limit=5.0)

            async def __aexit__(self, *_args: Any) -> None:
                """No-op exit — raise happens before yield."""

        stub_enforcer = MagicMock(spec=TradingCapsEnforcer)
        stub_enforcer.guard = MagicMock(return_value=_CapsGuard())

        client = _create_client(repo)
        client.app.dependency_overrides[get_caps_enforcer_dependency] = lambda: stub_enforcer
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 422
        body = response.json()
        assert body["detail"]["error_code"] == "caps_violation"
        assert body["detail"]["cap_type"] == "max_open_orders"
        client.close()

    def test_cancel_order_caps_violation_returns_422(self) -> None:
        """Cancel-path ``CapsViolationError`` maps to 422 caps_violation.

        Given: an active plan with a live child order and a stubbed
            enforcer whose ``guard()`` raises
            ``CapsViolationError('max_cancels_per_minute')``,
        When: the client POSTs to the cancel endpoint,
        Then: response is HTTP 422 carrying the structured error
            body — verifies the cap-violation branch in
            ``_cancel_plan``.
        """
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_id"] = "child-1"
        plan["params"]["native_instrument"] = "BTC-USD"
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")

        class _CapsGuard:
            async def __aenter__(self) -> None:
                raise CapsViolationError("max_cancels_per_minute", attempted=11.0, limit=10.0)

            async def __aexit__(self, *_args: Any) -> None:
                """No-op exit — raise happens before yield."""

        stub_enforcer = MagicMock(spec=TradingCapsEnforcer)
        stub_enforcer.guard = MagicMock(return_value=_CapsGuard())

        client = _create_client(repo)
        client.app.dependency_overrides[get_caps_enforcer_dependency] = lambda: stub_enforcer
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_body())
        assert response.status_code == 422
        body = response.json()
        assert body["detail"]["error_code"] == "caps_violation"
        assert body["detail"]["cap_type"] == "max_cancels_per_minute"
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


def _claimed_outcome(plan_row: dict[str, Any]) -> dict[str, Any]:
    """Build a ``CancelClaimResult`` for the ``"claimed"`` (CAS-winner) outcome.

    Mirrors the typed dict shape :meth:`Repository.claim_execution_plan_cancel`
    returns when the plan transitioned cleanly from ``active`` to
    ``cancel_requested`` under the FOR UPDATE lock.
    """
    return {"outcome": "claimed", "plan": plan_row}


def _not_found_outcome() -> dict[str, Any]:
    """Build a ``CancelClaimResult`` for the ``"not_found"`` race-loser case."""
    return {"outcome": "not_found", "plan": None}


class TestCancelOrder:
    """Tests for POST /api/orders/{id}/cancel."""

    def test_cancel_active_plan(self) -> None:
        """Given active plan, When cancelling, Then cancel_requested."""
        active_row = _make_plan_row(status="active")
        cancel_requested_row = _make_plan_row(status="cancel_requested")
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, cancel_requested_row])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
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

    def test_cancel_returns_403_when_caller_out_of_wallet_scope(self) -> None:
        """Non-admin caller missing the plan's wallet → 403 ``Wallet not accessible``.

        REST does NOT collapse :class:`PlanScopeError` to 404 like MCP
        does (anti-enumeration is an MCP-specific contract); the legacy
        REST contract surfaces 403 distinctly so the operator can tell
        "wallet not yours" from "wallet does not exist."
        """
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="active"))
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-other"}]
        )
        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        app.state.settings = MagicMock()

        def skip_csrf() -> None:
            return None

        def operator_principal() -> AuthPrincipal:
            return AuthPrincipal(
                username="operator",
                role=UserRole.OPERATOR,
                user_public_id="operator-1",
                operator_public_ids=["op-other"],
            )

        app.dependency_overrides[validate_csrf_token] = skip_csrf
        app.dependency_overrides[require_authentication] = operator_principal
        app.dependency_overrides[get_repository_dependency] = lambda: repo
        client = TestClient(app)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 403
        client.close()

    def test_cancel_plan_updated_but_not_found(self) -> None:
        """Given claim succeeds but reload GET returns None, Then 500."""
        active_row = _make_plan_row(status="active")
        cancel_requested_row = _make_plan_row(status="cancel_requested")
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, None])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 500
        client.close()

    def test_cancel_update_returns_none_concurrent(self) -> None:
        """Given a CAS race (plan disappeared between read and claim), Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="active"))
        repo.claim_execution_plan_cancel = AsyncMock(return_value=_not_found_outcome())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 409
        client.close()

    def test_cancel_with_child_concurrent_status_change_returns_409(self) -> None:
        """409 path inside the caps-guarded cancel branch (with child_client_order_id).

        Given: a plan with a child_client_order_id whose
            ``claim_execution_plan_cancel`` returns ``"not_found"``
            (race loser) under the caps guard,
        When: the cancel endpoint is hit and the caps guard admits (no
            caps row → unbounded),
        Then: the route raises HTTP 409 inside the guard via
            :class:`PlanConcurrentChangeError`.
        """
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(
            return_value=_make_plan_row(status="active", with_child_order=True)
        )
        repo.claim_execution_plan_cancel = AsyncMock(return_value=_not_found_outcome())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-42")
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
        ``exchange_order_id`` looked up via the repository AND the
        ``source_surface`` is stamped as ``"rest"`` so audit can
        distinguish REST cancels from MCP cancels.
        """
        active_row = _make_plan_row(status="active", with_child_order=True)
        cancel_requested_row = _make_plan_row(status="cancel_requested", with_child_order=True)
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, cancel_requested_row])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
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
        assert inserted["source_surface"] == "rest"
        client.close()

    def test_cancel_before_venue_ack_emits_command_with_null_exchange_id(self) -> None:
        """Cancel before venue ACK passes a null exchange_order_id through."""
        active_row = _make_plan_row(status="active", with_child_order=True)
        cancel_requested_row = _make_plan_row(status="cancel_requested", with_child_order=True)
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, cancel_requested_row])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
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
        active_row = _make_plan_row(status="active", with_child_order=True)
        cancel_requested_row = _make_plan_row(status="cancel_requested", with_child_order=True)
        repo = AsyncMock()
        repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-1")
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, cancel_requested_row])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
        repo.insert_trade_command = AsyncMock(return_value=(42, "cmd-cancel"))
        client = _create_client(repo)
        response = client.post(
            "/api/orders/by-client-order-id/cid-child-1/cancel",
            json=_cancel_order_body(),
        )
        assert response.status_code == 200
        repo.get_plan_public_id_for_client_order_id.assert_awaited_once_with(
            "cid-child-1",
            as_of=ANY,
        )
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

        After the Phase 3.5 unification the SCD2 ``cancel_requested``
        transition lives inside :meth:`Repository.claim_execution_plan_cancel`
        (the FOR UPDATE-locked CAS); the compensate-to-failed step still
        uses the legacy :meth:`Repository.update_execution_plan_status`
        because compensation does not need the cancel-key claim. We
        assert both are called: claim once for the cancel transition,
        then update_status with ``failed`` for the compensation.
        """
        active_row = _make_plan_row(status="active", with_child_order=True)
        cancel_requested_row = _make_plan_row(status="cancel_requested", with_child_order=True)
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, cancel_requested_row])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-42")
        repo.insert_trade_command = AsyncMock(side_effect=Exception("DB error"))
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 500
        repo.claim_execution_plan_cancel.assert_awaited_once()
        statuses = [
            call.kwargs["new_status"] for call in repo.update_execution_plan_status.await_args_list
        ]
        assert "failed" in statuses
        client.close()

    def test_cancel_compensation_failure_still_raises_500(self) -> None:
        """If both insert and compensation update fail, route still returns 500.

        After Phase 3.5: a second failure in the compensating ``failed``
        transition must not mask the original cancel-insert failure.
        The plan is stranded in ``cancel_requested`` and the
        PlanExecutorService recovery loop re-emits the cancel on next
        startup.
        """
        active_row = _make_plan_row(status="active", with_child_order=True)
        cancel_requested_row = _make_plan_row(status="cancel_requested", with_child_order=True)
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[active_row, cancel_requested_row])
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value=_claimed_outcome(cancel_requested_row)
        )
        compensation_calls: list[str] = []

        async def _update_status(**kwargs: Any) -> int | None:
            compensation_calls.append(kwargs["new_status"])
            raise RuntimeError("compensation DB error")

        repo.update_execution_plan_status = _update_status
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-42")
        repo.insert_trade_command = AsyncMock(side_effect=Exception("primary DB error"))
        client = _create_client(repo)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 500
        assert compensation_calls == ["failed"]
        client.close()


class TestCreateOrderAiReviewCitation:
    """Plan D Phase 2 #10 ``ai_review_public_id`` body field on POST /api/orders."""

    def test_valid_citation_threads_through_to_caps_enforcer_guard(self) -> None:
        """Plan D Phase 2 #10 — valid citation lands on the caps-guard submission.

        ``ai_review_public_id`` is a runtime-only signal on
        :class:`TradeCommandSubmission` consumed by
        :meth:`TradingCapsEnforcer.guard` to fire
        ``bus.caps_violation_after_ai_approve`` on cap reject; it is
        intentionally NOT persisted on the trade_command row. The test
        captures the submission via the enforcer to verify the field
        threaded through ``order_routes.create_order``.

        Given: a body whose ``ai_review_public_id`` matches a
            resolved_approved row owned by the caller on the same
            wallet,
        When: the client POSTs the order,
        Then: the citation passes validation AND the
            :class:`TradeCommandSubmission` handed to
            ``enforcer.guard`` carries ``ai_review_public_id``.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": "review-ok-1",
                "user_public_id": "test_user",
                "wallet_public_id": "wallet-1",
                "status": "resolved_approved",
            }
        )
        captured: dict[str, Any] = {}

        class _Ctx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *_args: Any) -> None:
                return None

        def _guard_capturing(submission: Any) -> _Ctx:
            captured["submission"] = submission
            return _Ctx()

        enforcer = MagicMock(spec=TradingCapsEnforcer)
        enforcer.guard = MagicMock(side_effect=_guard_capturing)
        body = _create_order_body()
        body["payload"]["ai_review_public_id"] = "review-ok-1"
        client = _create_client(repo)
        client.app.dependency_overrides[get_caps_enforcer_dependency] = lambda: enforcer
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        repo.get_ai_review.assert_awaited_once_with("review-ok-1")
        assert captured["submission"].ai_review_public_id == "review-ok-1"
        client.close()

    def test_unknown_citation_returns_403(self) -> None:
        """Plan D Phase 2 #10 R1 — citing an unknown ai_review_public_id returns 403.

        Given: a body whose ``ai_review_public_id`` does not exist
            (caller fabricated the value to attempt a fanout-spam
            attack),
        When: the client POSTs the order,
        Then: response is HTTP 403 and no trade-command insert fires.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_ai_review = AsyncMock(return_value=None)
        body = _create_order_body()
        body["payload"]["ai_review_public_id"] = "ghost-review"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 403
        assert "not found" in response.json()["detail"]
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_citation_owned_by_other_user_returns_403(self) -> None:
        """Plan D Phase 2 #10 R1 — citing another user's review returns 403.

        Given: a body whose ``ai_review_public_id`` row exists but is
            owned by a different user (the cross-user fanout-spam
            attack),
        When: the client POSTs the order,
        Then: response is HTTP 403 carrying the "owner mismatch"
            detail and no trade-command insert fires.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": "review-stranger",
                "user_public_id": "u-OTHER",
                "wallet_public_id": "wallet-1",
                "status": "resolved_approved",
            }
        )
        body = _create_order_body()
        body["payload"]["ai_review_public_id"] = "review-stranger"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 403
        assert "owner mismatch" in response.json()["detail"]
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()
