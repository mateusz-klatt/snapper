"""Tests for trailing stop REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.plans.trailing_stop import TrailingStopEvaluator
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.dependencies import get_caps_enforcer_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    return datetime(2026, 4, 13, tzinfo=UTC)


def _make_cycle_row(
    public_id: str = "cycle-1",
    status: str = "open",
    direction: str = "long",
) -> dict[str, Any]:
    now = _ts()
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "instrument_public_id": "inst-1",
        "exchange": "kraken_futures",
        "mode": "paper",
        "shard_key": "kraken_futures.BTC-USD.paper",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "direction": direction,
        "max_qty": 1.0,
        "status": status,
        "opened_at": now,
        "closed_at": None,
        "opening_command_public_id": None,
        "closing_command_public_id": None,
    }


def _make_plan_row(
    public_id: str = "ts-1",
    status: str = "armed",
) -> dict[str, Any]:
    now = _ts()
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": "trailing_stop",
        "created_by_user_id": "test",
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": "inst-1",
        "exchange": "kraken_futures",
        "mode": "paper",
        "shard_key": "kraken_futures.BTC-USD.paper",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "total_quantity": 1.0,
        "filled_quantity": 0.0,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": "cycle-1",
        "params": {
            "native_instrument": "BTC-USD",
            "trailing_pct": 5.0,
            "min_lock_pct": 0.0,
            "entry_price": 50000.0,
        },
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


def _create_body(
    trailing_pct: float = 5.0,
    min_lock_pct: float = 0.0,
) -> dict[str, Any]:
    return {
        "type": "create_trailing_stop_command",
        "session_id": "s1",
        "sequence_id": 1,
        "public_id": "req-1",
        "timestamp": _ts().isoformat(),
        "payload": {
            "position_cycle_public_id": "cycle-1",
            "trailing_pct": trailing_pct,
            "min_lock_pct": min_lock_pct,
        },
    }


def _cancel_body() -> dict[str, Any]:
    return {
        "type": "cancel_trailing_stop_command",
        "session_id": "s1",
        "sequence_id": 1,
        "public_id": "req-2",
        "timestamp": _ts().isoformat(),
        "payload": {"reason": "changed mind"},
    }


def _create_client(mock_repo: Any) -> TestClient:
    """Create test client with auth bypassed, mock repo, and mock plan executor."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    mock_settings = MagicMock()
    app.state.settings = mock_settings
    app.state.rest_tracker = SequenceTracker()

    mock_executor = MagicMock()
    mock_executor._check_capabilities = AsyncMock(return_value=[])
    mock_executor._register_plan = MagicMock()
    mock_executor._unregister_plan = MagicMock()
    mock_executor._extract_child_ids = MagicMock(return_value=[])
    mock_executor.plans = {}
    mock_executor.evaluators = {}
    app.state.plan_executor = mock_executor

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestCreateTrailingStop:
    """Tests for POST /api/trailing-stops."""

    def test_create_success(self) -> None:
        """Given valid params and open cycle, Then 200 with armed trailing stop."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "ts-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["plan_type"] == "trailing_stop"
        assert data["payload"]["status"] == "armed"

    def test_create_closed_cycle_returns_409(self) -> None:
        """Given closed cycle, Then 409."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(
            return_value=_make_cycle_row(status="closed")
        )
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 409

    def test_create_invalid_trailing_pct_returns_422(self) -> None:
        """Given trailing_pct=0, Then 422 validation error."""
        repo = AsyncMock()
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body(trailing_pct=0))
        assert response.status_code == 422

    def test_create_no_position_returns_422(self) -> None:
        """Given no open position for cycle, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(return_value=[])
        cycle = _make_cycle_row()
        cycle["max_qty"] = 0.0
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle)
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 422

    def test_create_duplicate_returns_409(self) -> None:
        """Given unique constraint violation, Then 409."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        repo.insert_execution_plan = AsyncMock(side_effect=Exception("UNIQUE constraint failed"))
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 409

    def test_create_generic_insert_failure_returns_500(self) -> None:
        """Given non-unique insert failure, Then 500."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        repo.insert_execution_plan = AsyncMock(side_effect=RuntimeError("connection lost"))
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 500

    def test_create_no_average_price_returns_422(self) -> None:
        """Given position without average_price, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": None,
                }
            ]
        )
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 422
        assert "entry price" in response.json()["detail"].lower()

    def test_create_plan_not_found_after_insert(self) -> None:
        """Given plan inserted but not readable, Then 500."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "ts-1"))
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 500

    def test_create_decision_failure_does_not_fail_request(self) -> None:
        """Given decision insert fails, Then 200 still returned (logged only)."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "ts-1"))
        repo.insert_execution_plan_decision = AsyncMock(side_effect=RuntimeError("DB down"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 200

    def test_create_skips_non_matching_positions(self) -> None:
        """Given positions for different instruments, Then correct one used."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "ETH-USD",
                    "mode": "paper",
                    "quantity": 2.0,
                    "average_price": 3000.0,
                },
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                },
            ]
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "ts-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 200

    def test_create_missing_capability_returns_422(self) -> None:
        """Given venue missing reduce_only, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        client = _create_client(repo)

        app = client.app
        executor = app.state.plan_executor
        executor._check_capabilities = AsyncMock(return_value=["supports_reduce_only"])

        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 422
        assert "capabilities" in response.json()["detail"].lower()

    def test_create_executor_unavailable_returns_503(self) -> None:
        """Given no plan executor, Then 503."""
        repo = AsyncMock()
        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        app.state.rest_tracker = SequenceTracker()
        app.state.plan_executor = None

        def skip_csrf() -> None:
            return None

        def skip_auth() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf
        app.dependency_overrides[require_authentication] = skip_auth
        app.dependency_overrides[get_repository_dependency] = lambda: repo
        client = TestClient(app)
        response = client.post("/api/trailing-stops", json=_create_body())
        assert response.status_code == 503

    def test_create_with_min_lock_warning(self) -> None:
        """Given min_lock_pct > 0, Then decision evidence has warning."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_instrument_capabilities = AsyncMock(return_value=[{"supports_reduce_only": True}])
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "ts-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.post("/api/trailing-stops", json=_create_body(min_lock_pct=5.0))
        assert response.status_code == 200
        decision_call = repo.insert_execution_plan_decision.call_args
        evidence = decision_call.kwargs["row"]["evidence"]
        assert "warning" in evidence


class TestCancelTrailingStop:
    """Tests for POST /api/trailing-stops/{id}/cancel."""

    def test_cancel_armed_success(self) -> None:
        """Given armed trailing stop, When cancel, Then 200 with cancelled status."""
        repo = AsyncMock()
        plan = _make_plan_row(status="armed")
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")

        cancelled_plan = dict(plan)
        cancelled_plan["status"] = "cancelled"
        repo.get_execution_plan = AsyncMock(side_effect=[plan, cancelled_plan])

        client = _create_client(repo)
        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 200

    def test_cancel_not_found_returns_404(self) -> None:
        """Given nonexistent plan, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/trailing-stops/nonexistent/cancel", json=_cancel_body())
        assert response.status_code == 404

    def test_cancel_active_with_children_emits_cancel_command(self) -> None:
        """Given active plan with child orders, Then cancel_requested + cancel command."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["child-1"]

        cancelled_plan = dict(plan)
        cancelled_plan["status"] = "cancel_requested"
        repo.get_execution_plan = AsyncMock(side_effect=[plan, cancelled_plan])
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-ord-1")
        repo.insert_trade_command = AsyncMock(return_value=1)

        client = _create_client(repo)
        executor = client.app.state.plan_executor
        executor._extract_child_ids = MagicMock(return_value=["child-1"])

        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 200
        repo.insert_trade_command.assert_called_once()
        cmd = repo.insert_trade_command.call_args.args[0]
        assert cmd["command_type"] == "cancel"

    def test_cancel_concurrent_status_change_returns_409(self) -> None:
        """Given concurrent status change, Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="armed"))
        repo.update_execution_plan_status = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 409

    def test_cancel_child_command_insert_fails_returns_500(self) -> None:
        """Given cancel command insert failure, Then 500 + plan set to failed."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["child-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("DB down"))

        client = _create_client(repo)
        executor = client.app.state.plan_executor
        executor._extract_child_ids = MagicMock(return_value=["child-1"])

        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 500

    def test_cancel_updated_plan_not_found_returns_500(self) -> None:
        """Given plan updated but not readable after, Then 500."""
        repo = AsyncMock()
        plan = _make_plan_row(status="armed")
        repo.get_execution_plan = AsyncMock(side_effect=[plan, None])
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        client = _create_client(repo)
        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 500

    def test_cancel_venue_lookup_failure_continues(self) -> None:
        """Given exchange order ID lookup fails, Then cancel proceeds with None."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["child-1"]

        cancelled_plan = dict(plan)
        cancelled_plan["status"] = "cancel_requested"
        repo.get_execution_plan = AsyncMock(side_effect=[plan, cancelled_plan])
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(
            side_effect=RuntimeError("lookup failed")
        )
        repo.insert_trade_command = AsyncMock(return_value=1)

        client = _create_client(repo)
        executor = client.app.state.plan_executor
        executor._extract_child_ids = MagicMock(return_value=["child-1"])

        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 200
        cmd = repo.insert_trade_command.call_args.args[0]
        assert cmd["exchange_order_id"] is None

    def test_cancel_compensation_failure_still_raises_500(self) -> None:
        """Given compensation to failed also fails, Then 500."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["child-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(
            side_effect=[1, RuntimeError("compensation failed")]
        )
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("DB down"))

        client = _create_client(repo)
        executor = client.app.state.plan_executor
        executor._extract_child_ids = MagicMock(return_value=["child-1"])

        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 500

    def test_cancel_child_without_native_instrument_skipped(self) -> None:
        """Given child without native_instrument in params, Then child skipped."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"] = {"child_client_order_ids": ["child-1"]}

        cancelled_plan = dict(plan)
        cancelled_plan["status"] = "cancel_requested"
        repo.get_execution_plan = AsyncMock(side_effect=[plan, cancelled_plan])
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")

        client = _create_client(repo)
        executor = client.app.state.plan_executor
        executor._extract_child_ids = MagicMock(return_value=["child-1"])

        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 200
        repo.insert_trade_command.assert_not_called()

    def test_cancel_terminal_returns_409(self) -> None:
        """Given completed plan, Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="completed"))
        client = _create_client(repo)
        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 409

    def test_cancel_caps_violation_returns_422(self) -> None:
        """TradingCapsEnforcer rejection maps to 422 caps_violation.

        Given: an active trailing stop with a live child order and a
            stubbed :class:`TradingCapsEnforcer` whose ``guard()``
            raises ``CapsViolationError('max_cancels_per_minute')``,
        When: the client POSTs to the cancel endpoint,
        Then: the response is HTTP 422 with a JSON detail carrying
            ``error_code='caps_violation'`` + the enforcer's
            ``cap_type``/``attempted``/``limit`` — proves the §9.2
            error mapping in the cancel path is wired end-to-end.
        """
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["child-1"]
        cancelled_plan = dict(plan)
        cancelled_plan["status"] = "cancel_requested"
        repo.get_execution_plan = AsyncMock(side_effect=[plan, cancelled_plan])
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")

        stub_enforcer = MagicMock(spec=TradingCapsEnforcer)

        class _GuardCtx:
            async def __aenter__(self) -> None:
                raise CapsViolationError("max_cancels_per_minute", attempted=11.0, limit=10.0)

            async def __aexit__(self, *_args: Any) -> None:
                """Re-raise path exits via context manager protocol."""

        stub_enforcer.guard = MagicMock(return_value=_GuardCtx())

        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["child-1"])
        client.app.dependency_overrides[get_caps_enforcer_dependency] = lambda: stub_enforcer
        response = client.post("/api/trailing-stops/ts-1/cancel", json=_cancel_body())
        assert response.status_code == 422
        body = response.json()
        assert body["detail"]["error_code"] == "caps_violation"
        assert body["detail"]["cap_type"] == "max_cancels_per_minute"


class TestGetTrailingStop:
    """Tests for GET /api/trailing-stops/{id}."""

    def test_get_success(self) -> None:
        """Given existing plan, Then 200."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/ts-1")
        assert response.status_code == 200
        assert response.json()["payload"]["plan_type"] == "trailing_stop"

    def test_get_not_found_returns_404(self) -> None:
        """Given nonexistent plan, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/nonexistent")
        assert response.status_code == 404

    def test_get_wrong_plan_type_returns_404(self) -> None:
        """Given bracket plan accessed via trailing-stop route, Then 404."""
        repo = AsyncMock()
        bracket_plan = _make_plan_row()
        bracket_plan["plan_type"] = "bracket"
        repo.get_execution_plan = AsyncMock(return_value=bracket_plan)
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/ts-1")
        assert response.status_code == 404


class TestGetTrailingStopByCycle:
    """Tests for GET /api/trailing-stops/by-cycle/{id}."""

    def test_by_cycle_no_trailing_stop(self) -> None:
        """Given no active trailing stop for cycle, Then message payload."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/by-cycle/cycle-1")
        assert response.status_code == 200
        assert response.json()["payload"] == "none"

    def test_by_cycle_not_found_returns_404(self) -> None:
        """Given nonexistent cycle, Then 404."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/by-cycle/nonexistent")
        assert response.status_code == 404

    def test_by_cycle_with_active_trailing_stop(self) -> None:
        """Given active trailing stop for cycle, Then state payload."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        plan = _make_plan_row()
        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        app.state.rest_tracker = SequenceTracker()

        mock_executor = MagicMock()
        mock_executor.plans = {"ts-1": plan}
        mock_executor.evaluators = {}
        app.state.plan_executor = mock_executor

        def skip_csrf() -> None:
            return None

        def skip_auth() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf
        app.dependency_overrides[require_authentication] = skip_auth
        app.dependency_overrides[get_repository_dependency] = lambda: repo

        client = TestClient(app)
        response = client.get("/api/trailing-stops/by-cycle/cycle-1")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["plan_public_id"] == "ts-1"
        assert data["payload"]["trailing_pct"] == 5.0
        assert data["payload"]["peak_price"] == 0.0
        assert data["payload"]["current_stop"] == 0.0

    def test_by_cycle_no_match_different_cycle_id(self) -> None:
        """Given trailing stop for different cycle, Then message payload."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        plan = _make_plan_row()
        plan["position_cycle_public_id"] = "cycle-other"

        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        app.state.rest_tracker = SequenceTracker()

        mock_executor = MagicMock()
        mock_executor.plans = {"ts-1": plan}
        app.state.plan_executor = mock_executor

        def skip_csrf() -> None:
            return None

        def skip_auth() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf
        app.dependency_overrides[require_authentication] = skip_auth
        app.dependency_overrides[get_repository_dependency] = lambda: repo

        client = TestClient(app)
        response = client.get("/api/trailing-stops/by-cycle/cycle-1")
        assert response.status_code == 200
        assert response.json()["payload"] == "none"

    def test_by_cycle_with_real_evaluator(self) -> None:
        """Given trailing stop with real evaluator state, Then peak/stop returned."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        plan = _make_plan_row()

        evaluator = TrailingStopEvaluator()
        evaluator._state["ts-1"] = {"peak_price": 55000.0, "current_stop": 52250.0}

        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        app.state.rest_tracker = SequenceTracker()

        mock_executor = MagicMock()
        mock_executor.plans = {"ts-1": plan}
        mock_executor.evaluators = {"ts-1": evaluator}
        app.state.plan_executor = mock_executor

        def skip_csrf() -> None:
            return None

        def skip_auth() -> AuthPrincipal:
            return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

        app.dependency_overrides[validate_csrf_token] = skip_csrf
        app.dependency_overrides[require_authentication] = skip_auth
        app.dependency_overrides[get_repository_dependency] = lambda: repo

        client = TestClient(app)
        response = client.get("/api/trailing-stops/by-cycle/cycle-1")
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["peak_price"] == 55000.0
        assert data["payload"]["current_stop"] == 52250.0


class TestListDecisions:
    """Tests for GET /api/trailing-stops/{id}/decisions."""

    def test_list_decisions_success(self) -> None:
        """Given plan with decisions, Then decisions returned."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_execution_plan_decisions = AsyncMock(return_value=[{"decision_type": "created"}])
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/ts-1/decisions")
        assert response.status_code == 200
        assert response.json()["count"] == 1

    def test_list_decisions_not_found(self) -> None:
        """Given nonexistent plan, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/trailing-stops/nonexistent/decisions")
        assert response.status_code == 404
