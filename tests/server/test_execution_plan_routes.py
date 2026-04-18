"""Tests for bracket execution plan REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
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
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.execution_plan_routes import _resolve_average_price


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    return datetime(2026, 4, 12, tzinfo=UTC)


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
    public_id: str = "bracket-1",
    status: str = "armed",
    plan_type: str = "bracket",
) -> dict[str, Any]:
    now = _ts()
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": plan_type,
        "created_by_user_id": "test_user",
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
        "params": {"native_instrument": "BTC-USD", "sl_price": 48000.0},
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


def _create_bracket_body(
    sl_price: float | None = 48000.0,
    tp_price: float | None = 52000.0,
) -> dict[str, Any]:
    return {
        "type": "create_bracket_command",
        "session_id": "s1",
        "sequence_id": 1,
        "public_id": "req-1",
        "timestamp": _ts().isoformat(),
        "payload": {
            "position_cycle_public_id": "cycle-1",
            "sl_price": sl_price,
            "tp_price": tp_price,
        },
    }


def _cancel_bracket_body() -> dict[str, Any]:
    return {
        "type": "cancel_bracket_command",
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
    app.state.plan_executor = mock_executor

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestCreateBracket:
    """Tests for POST /api/execution-plans."""

    def test_create_bracket_success(self) -> None:
        """Given valid params and open cycle, Then 200 with armed bracket."""
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
        repo.insert_execution_plan = AsyncMock(return_value=(1, "bracket-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 200
        data = response.json()
        assert data["payload"]["plan_type"] == "bracket"
        assert data["payload"]["status"] == "armed"
        client.close()

    def test_create_bracket_no_legs_422(self) -> None:
        """Given both legs None, Then 422."""
        repo = AsyncMock()
        client = _create_client(repo)
        body = _create_bracket_body(sl_price=None, tp_price=None)
        response = client.post("/api/execution-plans", json=body)
        assert response.status_code == 422
        client.close()

    def test_create_bracket_closed_cycle_409(self) -> None:
        """Given closed cycle, Then 409."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(
            return_value=_make_cycle_row(status="closed")
        )
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 409
        client.close()

    def test_create_bracket_missing_capability_422(self) -> None:
        """Given venue without supports_reduce_only, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        client = _create_client(repo)
        client.app.state.plan_executor._check_capabilities = AsyncMock(
            return_value=["supports_reduce_only"]
        )
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 422
        assert "supports_reduce_only" in response.json()["detail"]
        client.close()

    def test_create_bracket_duplicate_409(self) -> None:
        """Given duplicate bracket on same cycle, Then 409."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        repo.insert_execution_plan = AsyncMock(side_effect=Exception("UNIQUE constraint"))
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 409
        client.close()

    def test_create_bracket_wrong_side_sl_422(self) -> None:
        """Given SL above entry for long position, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        client = _create_client(repo)
        body = _create_bracket_body(sl_price=51000.0, tp_price=None)
        response = client.post("/api/execution-plans", json=body)
        assert response.status_code == 422
        assert "below" in response.json()["detail"].lower()
        client.close()

    def test_create_bracket_wrong_side_tp_long_422(self) -> None:
        """Given TP below entry for long position, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        client = _create_client(repo)
        body = _create_bracket_body(sl_price=None, tp_price=49000.0)
        response = client.post("/api/execution-plans", json=body)
        assert response.status_code == 422
        assert "above" in response.json()["detail"].lower()
        client.close()

    def test_create_bracket_wrong_side_sl_short_422(self) -> None:
        """Given SL below entry for short position, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(
            return_value=_make_cycle_row(direction="short")
        )
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": -1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        client = _create_client(repo)
        body = _create_bracket_body(sl_price=49000.0, tp_price=None)
        response = client.post("/api/execution-plans", json=body)
        assert response.status_code == 422
        assert "above" in response.json()["detail"].lower()
        client.close()

    def test_create_bracket_wrong_side_tp_short_422(self) -> None:
        """Given TP above entry for short position, Then 422."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(
            return_value=_make_cycle_row(direction="short")
        )
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": -1.0,
                    "average_price": 50000.0,
                }
            ]
        )
        client = _create_client(repo)
        body = _create_bracket_body(sl_price=None, tp_price=51000.0)
        response = client.post("/api/execution-plans", json=body)
        assert response.status_code == 422
        assert "below" in response.json()["detail"].lower()
        client.close()

    def test_create_bracket_zero_qty_422(self) -> None:
        """Given position with zero quantity, Then 422."""
        repo = AsyncMock()
        cycle = _make_cycle_row()
        cycle["max_qty"] = 0.0
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle)
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": 0.0,
                    "average_price": 50000.0,
                }
            ]
        )
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 422
        assert "no open position" in response.json()["detail"].lower()
        client.close()

    def test_create_bracket_server_error_500(self) -> None:
        """Given unexpected DB error on insert, Then 500."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        repo.insert_execution_plan = AsyncMock(side_effect=RuntimeError("unexpected"))
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 500
        client.close()

    def test_create_bracket_plan_not_found_after_insert_500(self) -> None:
        """Given plan insert succeeds but get returns None, Then 500."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        repo.insert_execution_plan = AsyncMock(return_value=(1, "bracket-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 500
        client.close()

    def test_create_bracket_no_executor_503(self) -> None:
        """Given plan_executor is None (API-only mode), Then 503."""
        repo = AsyncMock()
        client = _create_client(repo)
        client.app.state.plan_executor = None
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 503
        client.close()

    def test_create_bracket_no_position_falls_back_to_max_qty(self) -> None:
        """Given no matching position, Then falls back to cycle.max_qty."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
        repo.get_positions = AsyncMock(return_value=[])
        repo.insert_execution_plan = AsyncMock(return_value=(1, "bracket-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.post("/api/execution-plans", json=_create_bracket_body())
        assert response.status_code == 200
        client.close()

    def test_create_bracket_short_tp_only_success(self) -> None:
        """Given short cycle with valid TP only, Then 200 and TP is persisted."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(
            return_value=_make_cycle_row(direction="short")
        )
        repo.get_positions = AsyncMock(
            return_value=[
                {
                    "exchange": "kraken_futures",
                    "instrument": "ETH-USD",
                    "mode": "paper",
                    "quantity": -5.0,
                    "average_price": 2000.0,
                },
                {
                    "exchange": "kraken_futures",
                    "instrument": "BTC-USD",
                    "mode": "paper",
                    "quantity": -1.0,
                    "average_price": 50000.0,
                },
            ]
        )
        repo.insert_execution_plan = AsyncMock(return_value=(1, "bracket-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        plan = _make_plan_row()
        plan["side"] = "sell"
        repo.get_execution_plan = AsyncMock(return_value=plan)
        client = _create_client(repo)

        response = client.post(
            "/api/execution-plans", json=_create_bracket_body(sl_price=None, tp_price=49000.0)
        )

        assert response.status_code == 200
        plan_row = repo.insert_execution_plan.await_args.args[0]
        assert plan_row["side"] == "sell"
        assert plan_row["params"] == {
            "native_instrument": "BTC-USD",
            "tp_price": 49000.0,
        }
        client.close()

    def test_create_bracket_long_sl_only_success(self) -> None:
        """Given long cycle with valid SL only, Then 200 and SL is persisted."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        repo.insert_execution_plan = AsyncMock(return_value=(1, "bracket-1"))
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)

        response = client.post(
            "/api/execution-plans", json=_create_bracket_body(sl_price=49000.0, tp_price=None)
        )

        assert response.status_code == 200
        plan_row = repo.insert_execution_plan.await_args.args[0]
        assert plan_row["params"] == {
            "native_instrument": "BTC-USD",
            "sl_price": 49000.0,
        }
        client.close()

    def test_create_bracket_decision_logging_failure_still_returns_200(self) -> None:
        """Given decision insert fails, Then bracket creation still succeeds."""
        repo = AsyncMock()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=_make_cycle_row())
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
        repo.insert_execution_plan = AsyncMock(return_value=(1, "bracket-1"))
        repo.insert_execution_plan_decision = AsyncMock(side_effect=RuntimeError("decision failed"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)

        response = client.post("/api/execution-plans", json=_create_bracket_body())

        assert response.status_code == 200
        client.close()


class TestCancelBracket:
    """Tests for POST /api/execution-plans/{id}/cancel."""

    def test_cancel_armed_bracket_200(self) -> None:
        """Given armed bracket (no children), Then directly cancelled."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="armed"))
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        client = _create_client(repo)
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 200
        update_call = repo.update_execution_plan_status.await_args
        assert update_call[1]["new_status"] == "cancelled"
        client.close()

    def test_cancel_active_bracket_emits_cancel_commands(self) -> None:
        """Given active bracket with children, Then cancel_requested + cancel commands."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["cid-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["cid-1"])
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 200
        update_call = repo.update_execution_plan_status.await_args
        assert update_call[1]["new_status"] == "cancel_requested"
        repo.insert_trade_command.assert_awaited_once()
        client.close()

    def test_cancel_bracket_caps_violation_returns_422(self) -> None:
        """Cancel path surfaces ``CapsViolationError`` as HTTP 422.

        Given: an active bracket with a live child order and a
            stubbed enforcer whose ``guard()`` raises
            ``CapsViolationError('max_cancels_per_minute')``,
        When: the client POSTs to the cancel endpoint,
        Then: response is HTTP 422 carrying the §9.2 caps_violation
            body — verifies the cap-violation branch in
            ``cancel_bracket``.
        """
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["cid-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")

        class _CapsGuard:
            async def __aenter__(self) -> None:
                raise CapsViolationError("max_cancels_per_minute", attempted=11.0, limit=10.0)

            async def __aexit__(self, *_args: Any) -> None:
                """No-op exit — raise happens before yield."""

        stub_enforcer = MagicMock(spec=TradingCapsEnforcer)
        stub_enforcer.guard = MagicMock(return_value=_CapsGuard())

        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["cid-1"])
        client.app.dependency_overrides[get_caps_enforcer_dependency] = lambda: stub_enforcer
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 422
        body = response.json()
        assert body["detail"]["error_code"] == "caps_violation"
        assert body["detail"]["cap_type"] == "max_cancels_per_minute"
        client.close()

    def test_cancel_terminal_bracket_409(self) -> None:
        """Given already-terminal bracket, Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="completed"))
        client = _create_client(repo)
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 409
        client.close()

    def test_cancel_not_found_404(self) -> None:
        """Given nonexistent bracket, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post(
            "/api/execution-plans/nonexistent/cancel", json=_cancel_bracket_body()
        )
        assert response.status_code == 404
        client.close()

    def test_cancel_no_executor_503(self) -> None:
        """Given plan_executor is None, Then 503."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        client.app.state.plan_executor = None
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 503
        client.close()

    def test_cancel_concurrent_status_change_409(self) -> None:
        """Given concurrent status change, Then 409."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(status="armed"))
        repo.update_execution_plan_status = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 409
        client.close()

    def test_cancel_command_insert_failure_500(self) -> None:
        """Given cancel command insert fails, Then 500 + plan compensated to failed."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["cid-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
        repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("DB"))
        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["cid-1"])
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 500
        client.close()

    def test_cancel_plan_not_found_after_update_500(self) -> None:
        """Given plan update succeeds but re-read returns None, Then 500."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(side_effect=[_make_plan_row(status="armed"), None])
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        client = _create_client(repo)
        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())
        assert response.status_code == 500
        client.close()

    def test_cancel_active_bracket_without_native_instrument_skips_cancel_insert(self) -> None:
        """Given active bracket without native_instrument, Then cancel emits nothing and still succeeds."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"] = {"child_client_order_ids": ["cid-1"]}
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["cid-1"])

        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())

        assert response.status_code == 200
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_cancel_active_bracket_venue_lookup_error_is_logged_and_cancel_continues(self) -> None:
        """Given venue lookup failure, Then cancel command is still emitted."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["cid-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(return_value=1)
        repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(
            side_effect=RuntimeError("lookup failed")
        )
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["cid-1"])

        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())

        assert response.status_code == 200
        repo.insert_trade_command.assert_awaited_once()
        client.close()

    def test_cancel_command_insert_failure_with_failed_compensation_still_returns_500(self) -> None:
        """Given cancel insert and failed-compensation both fail, Then route still returns 500."""
        repo = AsyncMock()
        plan = _make_plan_row(status="active")
        plan["params"]["child_client_order_ids"] = ["cid-1"]
        repo.get_execution_plan = AsyncMock(return_value=plan)
        repo.update_execution_plan_status = AsyncMock(side_effect=[1, RuntimeError("comp failed")])
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
        repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("DB"))
        client = _create_client(repo)
        client.app.state.plan_executor._extract_child_ids = MagicMock(return_value=["cid-1"])

        response = client.post("/api/execution-plans/bracket-1/cancel", json=_cancel_bracket_body())

        assert response.status_code == 500
        client.close()


class TestResolveAveragePrice:
    """Tests for _resolve_average_price helper."""

    def test_returns_none_when_no_position_matches_cycle(self) -> None:
        """Given no matching position row, Then None is returned."""
        cycle = _make_cycle_row()
        positions = [
            {
                "exchange": "kraken_futures",
                "instrument": "ETH-USD",
                "mode": "paper",
                "average_price": 2000.0,
            }
        ]

        assert _resolve_average_price(positions, cycle) is None

    def test_returns_none_when_matching_position_average_price_is_not_positive(self) -> None:
        """Given matching row with non-positive average_price, Then None is returned."""
        cycle = _make_cycle_row()
        positions = [
            {
                "exchange": "kraken_futures",
                "instrument": "BTC-USD",
                "mode": "paper",
                "average_price": 0.0,
            }
        ]

        assert _resolve_average_price(positions, cycle) is None


class TestGetBracket:
    """Tests for GET /api/execution-plans/{id}."""

    def test_get_bracket_200(self) -> None:
        """Given existing bracket, Then 200 with plan data."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        client = _create_client(repo)
        response = client.get("/api/execution-plans/bracket-1")
        assert response.status_code == 200
        assert response.json()["payload"]["plan_type"] == "bracket"
        client.close()

    def test_get_bracket_not_found_404(self) -> None:
        """Given nonexistent bracket, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/execution-plans/nonexistent")
        assert response.status_code == 404
        client.close()


class TestListDecisions:
    """Tests for GET /api/execution-plans/{id}/decisions."""

    def test_list_decisions_200(self) -> None:
        """Given plan with decisions, Then 200 with list."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_execution_plan_decisions = AsyncMock(
            return_value=[
                {
                    "public_id": "dec-1",
                    "decision_type": "bracket_created",
                    "decided_at": _ts(),
                    "trigger_type": "api",
                    "evidence": {},
                    "reason": "test",
                    "decision_importance": "action",
                }
            ]
        )
        client = _create_client(repo)
        response = client.get("/api/execution-plans/bracket-1/decisions")
        assert response.status_code == 200
        assert response.json()["count"] == 1
        client.close()

    def test_list_decisions_not_found_404(self) -> None:
        """Given nonexistent plan, Then 404."""
        repo = AsyncMock()
        repo.get_execution_plan = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get("/api/execution-plans/nonexistent/decisions")
        assert response.status_code == 404
        client.close()
