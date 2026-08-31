"""Tests for manual order creation and cancellation REST API endpoints."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import ANY
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import Guard
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import get_repository_dependency
from snapper.server.order_routes import router as order_router


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


def _wallet_row(public_id: str, *, is_paper: bool = False) -> dict[str, object]:
    """Build the wallet row shape used by wallet resolution tests."""
    return {"public_id": public_id, "is_paper": is_paper}


def _arm_execution_venue(
    repo: AsyncMock,
    *,
    wallet_public_id: str = "wallet-1",
    is_paper: bool = False,
    credential_exchanges: tuple[str, ...] = ("kraken",),
) -> None:
    """Arm ``repo`` with the wallet and credential rows the venue check reads.

    :func:`snapper.server.order_routes._resolve_execution_venue` runs after
    wallet resolution and before the AI-review citation gate. It calls
    ``list_active_wallets`` to confirm the resolved wallet exists and match
    its ``is_paper`` flag against ``mode == 'paper'``, then
    ``list_active_wallet_credentials`` to require an active credential for
    the effective execution venue. Any create-order test that reaches this
    step must supply both rows or the route rejects with HTTP 400.

    Args:
        repo: The AsyncMock repository armed in place.
        wallet_public_id: Public id of the single active wallet row, which
            must equal the wallet the route resolves for the request.
        is_paper: The wallet row's ``is_paper`` flag; must equal
            ``mode == 'paper'`` for the mode/wallet check to pass.
        credential_exchanges: Exchanges for which an active credential row
            exists on ``wallet_public_id``; must include the effective
            venue (``'paper'`` under paper mode, else the request exchange).
    """
    repo.list_active_wallets = AsyncMock(
        return_value=[
            {
                "public_id": wallet_public_id,
                "label": "main",
                "description": None,
                "is_paper": is_paper,
                "timestamp": _ts(),
                "session_id": "s1",
                "sequence_id": 1,
            }
        ]
    )
    repo.list_active_wallet_credentials = AsyncMock(
        return_value=[
            {
                "public_id": f"cred-{index}",
                "wallet_public_id": wallet_public_id,
                "exchange": exchange,
                "credential_type": "api",
            }
            for index, exchange in enumerate(credential_exchanges, start=1)
        ]
    )


def _snapshot_row(
    *,
    ts: datetime | None = None,
    bid: float | None = 100.0,
    ask: float | None = 101.0,
    last: float | None = 100.5,
) -> dict[str, object]:
    """Build a ``MarketSnapshotRow`` for paper reference-price tests.

    Only ``ts`` (recency ordering) and the ``bid``/``ask``/``last`` price
    legs are read by
    :func:`snapper.server.order_routes._resolve_paper_reference_price`; the
    remaining depth/OHLC fields round out the row shape.
    """
    return {
        "ts": ts if ts is not None else _ts(),
        "instrument_public_id": "inst-1",
        "bid": bid,
        "bid_volume": 1.0,
        "ask": ask,
        "ask_volume": 1.0,
        "last": last,
        "volume": 10.0,
        "vwap": 100.4,
        "low": 99.0,
        "high": 102.0,
    }


def _candle_row(
    *,
    close: float = 250.0,
    timestamp: datetime | None = None,
) -> dict[str, object]:
    """Build a ``CandleRow`` for paper reference-price candle-fallback tests.

    ``timestamp`` drives the ≤120 s freshness gate and ``close`` is the
    fallback reference price; the rest complete the 1m candle row shape.
    """
    anchor = timestamp if timestamp is not None else datetime.now(UTC)
    return {
        "open_at": anchor,
        "timeframe": "1m",
        "open": 249.0,
        "high": 251.0,
        "low": 248.0,
        "close": close,
        "volume": 5.0,
        "vwap": None,
        "trades": None,
        "source": "native",
        "complete": True,
        "public_id": "candle-1",
        "timestamp": anchor,
        "session_id": "s1",
        "sequence_id": 1,
    }


def _paper_market_body(*, side: str = "buy") -> dict[str, Any]:
    """Return a paper-mode MARKET create-order body (no limit price).

    Paper MARKET orders trigger the reference-price resolver; the request
    exchange stays kraken so the venue remaps to ``paper`` while kraken
    remains the market-data source.
    """
    body = _create_order_body()
    body["payload"]["mode"] = "paper"
    body["payload"]["order_type"] = "market"
    body["payload"]["side"] = side
    body["payload"].pop("price", None)
    return body


def _create_order_repo(plan_wallet_public_id: str = "wallet-1") -> AsyncMock:
    """Build a repository mock for successful create-order tests."""
    repo = AsyncMock()
    plan_row = _make_plan_row()
    plan_row["wallet_public_id"] = plan_wallet_public_id
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
    repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
    repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
    repo.update_execution_plan_status = AsyncMock(return_value=2)
    repo.get_execution_plan = AsyncMock(return_value=plan_row)
    _arm_execution_venue(repo, wallet_public_id=plan_wallet_public_id)
    return repo


def _operator_principal(operator_public_ids: list[str] | None = None) -> AuthPrincipal:
    """Build the operator principal used by create-order scope tests."""
    return AuthPrincipal(
        username="op_user",
        role=UserRole.OPERATOR,
        operator_public_ids=operator_public_ids if operator_public_ids is not None else ["op-1"],
    )


def _stub_guard_payload() -> Guard:
    """Build the Guard payload an admitting enforcer stub yields.

    Returns:
        A :class:`Guard` with a placeholder submission and a NULL
        admission notional, matching what the route reads after entry.
    """
    return Guard(
        submission=TradeCommandSubmission(
            user_public_id="test_user",
            operator_public_id=None,
            wallet_public_id="wallet-1",
            instrument_public_id=None,
            command_type="create",
            side="buy",
            order_type="market",
            quantity=None,
            price=None,
            source_surface="rest",
            idempotency_key=None,
        ),
        assigned_public_id="guard-pid",
        submitted_notional_usd=123.45,
    )


class _AdmitCapsGuard:
    """Async context manager that admits cap-guarded submissions."""

    async def __aenter__(self) -> Guard:
        """Enter without rejecting the submission.

        Returns:
            The stub :class:`Guard` payload the route reads.
        """
        return _stub_guard_payload()

    async def __aexit__(self, *_args: object) -> None:
        """Exit without suppressing exceptions."""
        return None


def _admit_caps_enforcer() -> MagicMock:
    """Build a caps enforcer mock that admits submissions."""
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    enforcer.guard = MagicMock(return_value=_AdmitCapsGuard())
    return enforcer


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


def _build_order_test_app() -> FastAPI:
    """Build a slim app containing only order routes."""
    app = FastAPI()
    app.router.lifespan_context = _noop_lifespan
    app.state.settings = MagicMock()
    app.state.rest_tracker = SequenceTracker()
    app.include_router(order_router, prefix="/api")
    return app


def _create_client_with_principal(
    mock_repo: object,
    principal: AuthPrincipal,
) -> TestClient:
    """Create test client with auth bypassed for the supplied principal.

    Args:
        mock_repo: AsyncMock repository.
        principal: Auth principal returned by the auth dependency.

    Returns:
        TestClient with overrides applied.
    """
    app = _build_order_test_app()

    def skip_csrf() -> None:
        return None

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = lambda: principal
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    app.dependency_overrides[get_caps_enforcer_dependency] = _admit_caps_enforcer
    return TestClient(app)


def _create_client(mock_repo: Any) -> TestClient:
    """Create test client with ADMIN auth bypassed and mock repository injected."""
    return _create_client_with_principal(
        mock_repo,
        AuthPrincipal(username="test_user", role=UserRole.ADMIN),
    )


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
        _arm_execution_venue(repo)
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
            body — verifies the cap-violation branch in
            ``create_order``.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)

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
        _arm_execution_venue(repo)
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
        _arm_execution_venue(repo)
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
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "other-wallet"}]
        )
        client = _create_client_with_principal(repo, _operator_principal())
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 403
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_create_order_resolves_omitted_wallet_for_single_live_operator_wallet(
        self,
    ) -> None:
        """Omitted wallet resolves only when one live wallet is accessible."""
        repo = _create_order_repo(plan_wallet_public_id="wallet-live")
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-live")]
        )
        client = _create_client_with_principal(repo, _operator_principal())
        body = _create_order_body()
        body["payload"].pop("wallet_public_id")
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert plan_insert["wallet_public_id"] == "wallet-live"
        assert cmd_insert["wallet_public_id"] == "wallet-live"
        assert repo.list_accessible_wallets_for_operators.await_count == 2
        client.close()

    def test_create_order_omitted_wallet_with_multiple_live_wallets_returns_400(
        self,
    ) -> None:
        """Multiple live candidates reject instead of silently picking."""
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-a"), _wallet_row("wallet-b")]
        )
        client = _create_client_with_principal(repo, _operator_principal())
        body = _create_order_body()
        body["payload"].pop("wallet_public_id")
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"] == "specify wallet_public_id; 2 wallets accessible"
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_create_order_omitted_wallet_with_no_wallets_returns_400(self) -> None:
        """Zero live candidates reject before any order row is written."""
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        client = _create_client_with_principal(repo, _operator_principal())
        body = _create_order_body()
        body["payload"].pop("wallet_public_id")
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"] == "specify wallet_public_id; 0 wallets accessible"
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_create_order_omitted_live_wallet_with_only_paper_wallet_returns_400(
        self,
    ) -> None:
        """A live order never binds the caller's single paper wallet."""
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-paper", is_paper=True)]
        )
        client = _create_client_with_principal(repo, _operator_principal())
        body = _create_order_body()
        body["payload"].pop("wallet_public_id")
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"] == "specify wallet_public_id; 0 wallets accessible"
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_create_order_omitted_live_wallet_without_operator_context_returns_400(
        self,
    ) -> None:
        """Live autolookup fails closed when the caller has no operator context."""
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock()
        client = _create_client_with_principal(repo, _operator_principal([]))
        body = _create_order_body()
        body["payload"].pop("wallet_public_id")
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"] == "specify wallet_public_id; 0 wallets accessible"
        repo.list_accessible_wallets_for_operators.assert_not_called()
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_create_order_explicit_wallet_stays_unchanged_with_multiple_wallets(
        self,
    ) -> None:
        """Explicit in-scope wallet bypasses autolookup and still writes that wallet."""
        repo = _create_order_repo()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-1"), _wallet_row("wallet-2")]
        )
        client = _create_client_with_principal(repo, _operator_principal())
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 200
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert plan_insert["wallet_public_id"] == "wallet-1"
        assert cmd_insert["wallet_public_id"] == "wallet-1"
        assert repo.list_accessible_wallets_for_operators.await_count == 1
        client.close()

    def test_create_order_generic_plan_error(self) -> None:
        """Given unexpected plan insert error, When creating, Then 500."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(side_effect=Exception("unexpected"))
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)
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
        _arm_execution_venue(repo)
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
        _arm_execution_venue(repo)
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

    def test_create_order_stop_persists_core_vocabulary_and_trigger(self) -> None:
        """The durable command row speaks CORE and carries the trigger (#156).

        Given: a stop order with a stop_price,
        When: created via POST /api/orders,
        Then: the trade-command insert stores order_type='stop' (NOT the
            wire value that stranded rows CREATED) plus stop_price, and
            the plan params no longer write the legacy venue_order_type.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "stop"
        body["payload"]["price"] = None
        body["payload"]["stop_price"] = 48000.0
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert cmd_insert["order_type"] == "stop"
        assert cmd_insert["stop_price"] == 48000.0
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        assert plan_insert["params"]["order_type"] == "stop"
        assert "venue_order_type" not in plan_insert["params"]
        client.close()

    def test_create_order_stop_limit_persists_core_vocabulary_and_both_prices(self) -> None:
        """A stop_limit command keeps CORE type, limit leg and trigger (#156).

        Given: a stop_limit order with price and stop_price,
        When: created via POST /api/orders,
        Then: the command row stores order_type='stop_limit' with both
            prices, so the outbox payload and the venue submit carry the
            full trigger semantics.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "stop_limit"
        body["payload"]["price"] = 47900.0
        body["payload"]["stop_price"] = 48000.0
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert cmd_insert["order_type"] == "stop_limit"
        assert cmd_insert["price"] == 47900.0
        assert cmd_insert["stop_price"] == 48000.0
        client.close()

    def test_create_order_with_leverage(self) -> None:
        """Given order with leverage, When creating, Then leverage in params."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        repo.get_execution_plan = AsyncMock(return_value=_make_plan_row())
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)
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
        _arm_execution_venue(repo)
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "market"
        body["payload"].pop("price", None)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        client.close()

    def test_create_order_market_with_price_returns_422(self) -> None:
        """Given a market order carrying a price, When creating, Then 422.

        Given: a market-order body whose ``price`` is set,
        When: the client POSTs the order,
        Then: the route funnels the evaluator ValueError to HTTP 422 and
            the detail carries "must not carry price"; the guard runs
            before persistence so no order row is written.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        body = _create_order_body()
        body["payload"]["order_type"] = "market"
        body["payload"]["price"] = 50000.0
        response = client.post("/api/orders", json=body)
        assert response.status_code == 422
        assert "must not carry price" in response.json()["detail"]
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_create_order_unknown_instrument_returns_422(self) -> None:
        """Given an unresolvable instrument, When creating, Then 422 unknown_instrument.

        Given: an instrument that clears the capability guard but resolves
            to no active Instrument row (``get_instrument_public_id_by_symbol``
            returns None),
        When: the client POSTs the order,
        Then: the route rejects with HTTP 422 error_code
            ``unknown_instrument`` and no order row is written.
        """
        repo = AsyncMock()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 422
        assert response.json()["detail"]["error_code"] == "unknown_instrument"
        repo.insert_execution_plan.assert_not_called()
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
        principal = AuthPrincipal(
            username="operator",
            role=UserRole.OPERATOR,
            user_public_id="operator-1",
            operator_public_ids=["op-other"],
        )
        client = _create_client_with_principal(repo, principal)
        response = client.post("/api/orders/plan-1/cancel", json=_cancel_order_body())
        assert response.status_code == 403
        client.close()

    def test_cancel_plan_updated_but_not_found(self) -> None:
        """Given claim succeeds but reload GET returns None, Then 500 + legacy detail.

        REST distinguishes the rare post-cancel reload disappearance
        from a generic emit failure by mapping
        :class:`PlanPostCancelReloadError` to the legacy
        ``"Plan updated but not found"`` 500 detail.
        :class:`PlanCancelEmitError` keeps the generic
        ``"Failed to emit cancel command"`` detail.
        """
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
        assert response.json()["detail"] == "Plan updated but not found"
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

        The SCD2 ``cancel_requested`` transition lives inside
        :meth:`Repository.claim_execution_plan_cancel`
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
        assert response.json()["detail"] == "Failed to emit cancel command"
        repo.claim_execution_plan_cancel.assert_awaited_once()
        statuses = [
            call.kwargs["new_status"] for call in repo.update_execution_plan_status.await_args_list
        ]
        assert "failed" in statuses
        client.close()

    def test_cancel_compensation_failure_still_raises_500(self) -> None:
        """If both insert and compensation update fail, route still returns 500.

        A second failure in the compensating ``failed`` transition
        must not mask the original cancel-insert failure. The plan is
        stranded in ``cancel_requested`` and the PlanExecutorService
        recovery loop re-emits the cancel on next startup.
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
    """``ai_review_public_id`` body field on POST /api/orders."""

    def test_valid_citation_threads_through_to_caps_enforcer_guard(self) -> None:
        """Valid citation lands on the caps-guard submission.

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
        _arm_execution_venue(repo)
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": "review-ok-1",
                "user_public_id": "test_user",
                "wallet_public_id": "wallet-1",
                "instrument_public_id": "inst-1",
                "status": "resolved_approved",
            }
        )
        captured: dict[str, Any] = {}

        class _Ctx:
            async def __aenter__(self) -> Guard:
                return _stub_guard_payload()

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
        cmd_row = repo.insert_trade_command.call_args.args[0]
        assert cmd_row["ai_review_public_id"] == "review-ok-1"
        assert cmd_row["submitted_notional_usd"] == 123.45
        client.close()

    def test_unknown_citation_returns_403(self) -> None:
        """Citing an unknown ai_review_public_id returns 403.

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
        _arm_execution_venue(repo)
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

    def test_citation_for_a_different_instrument_returns_403(self) -> None:
        """An approval names one instrument and authorizes only that one.

        Given: an approved review owned by the caller, on the caller's wallet,
            but issued for a DIFFERENT instrument than the route resolved,
        When: the client POSTs the order,
        Then: response is HTTP 403 naming the instrument mismatch and no
            trade-command insert fires.

        The REST create-order route is the main manual-order path, and it was
        the one call site the instrument binding did not reach: the validator
        gained the argument and only the MCP tool was updated. Until this test,
        a review approving one instrument authorized an order in any other
        through ``POST /api/orders``, which is the exact hole the binding
        exists to close.

        The instrument compared is the one the ROUTE resolved from
        ``(native_symbol, exchange)``, never ``body.instrument_public_id`` —
        binding an approval to a value the same caller supplies would bind
        nothing.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": "review-other-instrument",
                "user_public_id": "test_user",
                "wallet_public_id": "wallet-1",
                "instrument_public_id": "inst-SOMETHING-ELSE",
                "status": "resolved_approved",
            }
        )
        body = _create_order_body()
        body["payload"]["ai_review_public_id"] = "review-other-instrument"
        client = _create_client(repo)

        response = client.post("/api/orders", json=body)

        assert response.status_code == 403
        detail = response.json()["detail"]
        assert "instrument mismatch" in detail
        assert "inst-SOMETHING-ELSE" in detail
        assert "inst-1" in detail
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_a_forged_instrument_in_the_body_cannot_satisfy_the_citation(self) -> None:
        """The binding must read the route's instrument, not the caller's.

        Given: an approved review issued for one instrument, and a body whose
            ``instrument_public_id`` is forged to match that review while the
            symbol resolves server-side to a DIFFERENT instrument,
        When: the client POSTs the order,
        Then: response is HTTP 403 naming the instrument mismatch.

        This is the assertion that makes the binding worth anything. Comparing
        the review against ``body.instrument_public_id`` would compare a
        caller-supplied value with a caller-chosen citation — the attacker
        controls both sides, so the check would pass for any pairing they like.
        The route already canonicalises the instrument from
        ``(native_symbol, exchange)``; the citation must be held to that.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
        _arm_execution_venue(repo)
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-RESOLVED")
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": "review-forged",
                "user_public_id": "test_user",
                "wallet_public_id": "wallet-1",
                "instrument_public_id": "inst-FORGED",
                "status": "resolved_approved",
            }
        )
        body = _create_order_body()
        body["payload"]["ai_review_public_id"] = "review-forged"
        body["payload"]["instrument_public_id"] = "inst-FORGED"
        client = _create_client(repo)

        response = client.post("/api/orders", json=body)

        assert response.status_code == 403
        detail = response.json()["detail"]
        assert "instrument mismatch" in detail
        assert "inst-RESOLVED" in detail
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_citation_owned_by_other_user_returns_403(self) -> None:
        """Citing another user's review returns 403.

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
        _arm_execution_venue(repo)
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


def _venue_failure_repo() -> AsyncMock:
    """Build a repo that reaches ``_resolve_execution_venue`` then rejects there.

    Instrument resolution is stubbed so the request clears the capability
    guard and the instrument lookup and lands on the venue check; the
    execution-plan and trade-command inserts stay unarmed so each matrix
    violation can assert that neither write fired.
    """
    repo = AsyncMock()
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
    repo.insert_execution_plan = AsyncMock()
    repo.insert_trade_command = AsyncMock()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=None)
    return repo


class TestExecutionVenueMatrix:
    """Branch coverage for ``order_routes._resolve_execution_venue``.

    Exercises every arm of the paper-routing matrix the venue check
    enforces between wallet resolution and the AI-review citation gate:
    the paper-exchange guard, wallet existence, the mode/wallet paper
    parity check in both directions, the effective-venue credential
    requirement, and the two success shapes — paper remap (venue coerced
    to ``paper`` with the request exchange preserved as
    ``source_exchange``) and live passthrough.
    """

    def test_paper_exchange_requires_paper_mode_returns_400(self) -> None:
        """Reject exchange='paper' with mode='live' ahead of instrument resolution.

        Given: a body pairing exchange='paper' with mode='live' against a
            repo whose ``get_instrument_public_id_by_symbol`` returns None
            (an unknown instrument that would otherwise 422),
        When: the client POSTs the order,
        Then: the pure-literal paper-mode guard rejects with error_code
            ``paper_exchange_requires_paper_mode`` and HTTP 400 (NOT the
            422 ``unknown_instrument`` the later resolution would raise),
            proving the guard runs first — before instrument resolution
            and before the wallet catalogue is touched, so
            ``list_active_wallets`` is never called and no order row is
            written.
        """
        repo = _venue_failure_repo()
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        body = _create_order_body()
        body["payload"]["exchange"] = "paper"
        body["payload"]["mode"] = "live"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "paper_exchange_requires_paper_mode"
        repo.list_active_wallets.assert_not_called()
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_unknown_wallet_returns_400(self) -> None:
        """Reject when the resolved wallet is absent from the active catalogue.

        Given: a live order whose wallet clears scope resolution but the
            active-wallet catalogue holds only a different wallet id,
        When: the client POSTs the order,
        Then: the venue check rejects with error_code ``unknown_wallet``
            and no order row is written.
        """
        repo = _venue_failure_repo()
        _arm_execution_venue(repo, wallet_public_id="wallet-other")
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "unknown_wallet"
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_mode_wallet_mismatch_paper_mode_live_wallet_returns_400(self) -> None:
        """Reject a paper-mode order that resolves to a live wallet.

        Given: a paper-mode order whose resolved wallet is a live wallet
            (``is_paper`` False),
        When: the client POSTs the order,
        Then: the venue check rejects with error_code
            ``mode_wallet_mismatch`` and no order row is written.
        """
        repo = _venue_failure_repo()
        _arm_execution_venue(repo, is_paper=False)
        body = _create_order_body()
        body["payload"]["mode"] = "paper"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "mode_wallet_mismatch"
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_mode_wallet_mismatch_live_mode_paper_wallet_returns_400(self) -> None:
        """Reject a live-mode order that resolves to a paper wallet.

        Given: a live-mode order whose resolved wallet is a paper wallet
            (``is_paper`` True) — the mirror of the paper-mode mismatch,
        When: the client POSTs the order,
        Then: the venue check rejects with error_code
            ``mode_wallet_mismatch`` and no order row is written.
        """
        repo = _venue_failure_repo()
        _arm_execution_venue(repo, is_paper=True)
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "mode_wallet_mismatch"
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_wallet_credential_missing_returns_400(self) -> None:
        """Reject a paper order whose wallet lacks a paper credential.

        Given: a paper-mode order on a paper wallet whose only active
            credential is for a different venue (kraken, not the paper
            venue the order remaps to),
        When: the client POSTs the order,
        Then: the venue check rejects with error_code
            ``wallet_credential_missing`` for the ``paper`` venue because
            no executor consumes the command, and no order row is written.
        """
        repo = _venue_failure_repo()
        _arm_execution_venue(repo, is_paper=True, credential_exchanges=("kraken",))
        body = _create_order_body()
        body["payload"]["mode"] = "paper"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error_code"] == "wallet_credential_missing"
        assert detail["exchange"] == "paper"
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_paper_mode_remaps_venue_and_records_source_exchange(self) -> None:
        """Remap a valid paper order to the paper venue and preserve the source.

        Given: a paper-mode order on a paper wallet holding a ``paper``
            credential while sourcing kraken market data,
        When: the client POSTs the order,
        Then: the response is 200 and the persisted plan and command rows
            carry exchange='paper' while the plan params preserve the
            request exchange as ``source_exchange`` and the shard key is
            the canonical paper-venue key.
        """
        repo = _create_order_repo()
        _arm_execution_venue(repo, is_paper=True, credential_exchanges=("paper",))
        body = _create_order_body()
        body["payload"]["mode"] = "paper"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert plan_insert["exchange"] == "paper"
        assert plan_insert["params"]["source_exchange"] == "kraken"
        assert plan_insert["shard_key"] == "paper.BTC-USD.paper.wwallet1"
        assert cmd_insert["exchange"] == "paper"
        client.close()

    def test_live_mode_passes_through_without_source_exchange(self) -> None:
        """Pass a valid live order straight through with no venue remap.

        Given: a live-mode kraken order on a live wallet holding a kraken
            credential,
        When: the client POSTs the order,
        Then: the response is 200 and the persisted plan keeps
            exchange='kraken' with the canonical live shard key and no
            ``source_exchange`` remap key, and the command row keeps
            exchange='kraken'.
        """
        repo = _create_order_repo()
        _arm_execution_venue(repo, is_paper=False, credential_exchanges=("kraken",))
        client = _create_client(repo)
        response = client.post("/api/orders", json=_create_order_body())
        assert response.status_code == 200
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert plan_insert["exchange"] == "kraken"
        assert plan_insert["shard_key"] == "kraken.BTC-USD.live.wwallet1"
        assert "source_exchange" not in plan_insert["params"]
        assert cmd_insert["exchange"] == "kraken"
        client.close()


class TestCreateOrderModifierRefusals:
    """REST refuses execution modifiers the resolved venue's client would drop.

    Until 2026-08-06 this route persisted and submitted `leverage`, `post_only`
    and `reduce_only` with no venue check at all, while the MCP surface refused
    them. An order refused for an autonomous agent and accepted for a human is
    the worst possible split, so both now share one decision through
    ``application.trade.execution_modifiers``; only the wire shape differs.
    """

    def test_post_only_is_refused_where_the_client_drops_it(self) -> None:
        """A venue that never sends the flag refuses rather than submitting without it.

        Given: a `post_only` order routed to `walutomat`,
        When: the client POSTs it,
        Then: HTTP 400 `order_flags_unsupported`, and neither the plan nor the
            command row is written.
        """
        repo = _create_order_repo()
        _arm_execution_venue(repo, credential_exchanges=("kraken", "walutomat"))
        body = _create_order_body()
        body["payload"]["exchange"] = "walutomat"
        body["payload"]["post_only"] = True
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        detail = response.json()["detail"]
        assert detail["error_code"] == "order_flags_unsupported"
        assert detail["flags"] == ["post_only"]
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
        client.close()

    def test_the_refusal_never_claims_the_exchange_lacks_the_feature(self) -> None:
        """Wording matters: Snapper does not send it, the venue may well accept it.

        Given: a `reduce_only` order routed to `walutomat`,
        When: the client POSTs it,
        Then: the reason says Snapper does not send the flag, and does NOT say
            the venue does not support it.

        Kraken spot accepts all three modifiers on its API; what varies is which
        of them our client forwards. A caller told the exchange lacks a feature
        goes looking for a different venue when the real remedy is a different
        client, so the distinction is load-bearing rather than stylistic.
        """
        repo = _create_order_repo()
        _arm_execution_venue(repo, credential_exchanges=("kraken", "walutomat"))
        body = _create_order_body()
        body["payload"]["exchange"] = "walutomat"
        body["payload"]["reduce_only"] = True
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        detail = response.json()["detail"]
        assert "Snapper does not send" in detail["reason"]
        assert "does not support" not in detail["reason"]
        client.close()

    def test_post_only_is_refused_on_a_taker_order_type(self) -> None:
        """Maker-only cannot apply to an order that must take liquidity.

        Given: a `post_only` market order routed to `kraken`, a venue that DOES
            honour the flag,
        When: the client POSTs it,
        Then: HTTP 400 `post_only_order_type_unsupported`.

        The venue set alone is too coarse a gate here: kraken passes it, and the
        flag would then be persisted on an order that cannot rest on the book.
        """
        repo = _create_order_repo()
        body = _create_order_body()
        body["payload"]["order_type"] = "market"
        body["payload"].pop("price")
        body["payload"]["post_only"] = True
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "post_only_order_type_unsupported"
        client.close()

    def test_spot_reduce_only_without_leverage_is_refused(self) -> None:
        """Cash spot has no margin position for the clamp to bind to.

        Given: a `reduce_only` order on `kraken` with no leverage,
        When: the client POSTs it,
        Then: HTTP 400 `reduce_only_requires_margin`.
        """
        repo = _create_order_repo()
        body = _create_order_body()
        body["payload"]["reduce_only"] = True
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "reduce_only_requires_margin"
        client.close()

    def test_an_honoured_modifier_is_not_refused(self) -> None:
        """The gate refuses what is dropped, and only that.

        Given: a `post_only` limit order on `kraken`, which forwards it,
        When: the client POSTs it,
        Then: the request is not refused by the modifier gate.

        Pinned because a fail-closed rule that also blocks working orders would
        be a worse defect than the silent drop it replaced.
        """
        repo = _create_order_repo()
        body = _create_order_body()
        body["payload"]["post_only"] = True
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        if response.status_code == 400:
            assert "error_code" not in response.json().get("detail", {}) or response.json()[
                "detail"
            ]["error_code"] not in {
                "order_flags_unsupported",
                "post_only_order_type_unsupported",
                "reduce_only_requires_margin",
                "leverage_not_an_order_parameter",
            }
        client.close()


class TestPaperReferencePrice:
    """Branch coverage for ``order_routes._resolve_paper_reference_price``.

    A manual paper MARKET order carries no limit price, but the paper
    simulator rejects a priceless fill, so ``create_order`` attaches a
    fresh reference price at create time. These tests exercise every
    resolution arm: the side-aware snapshot leg, the snapshot→candle
    fall-through, the candle freshness gate, the hard ``no_reference_price``
    rejection, and the LIMIT short-circuit that skips pricing entirely.
    """

    def _paper_market_repo(self) -> AsyncMock:
        """Build a repo that clears the paper venue and reaches pricing."""
        repo = _create_order_repo()
        _arm_execution_venue(repo, is_paper=True, credential_exchanges=("paper",))
        return repo

    def test_paper_market_buy_uses_snapshot_ask(self) -> None:
        """Price a paper MARKET buy at the freshest snapshot ask.

        Given: a paper MARKET buy whose freshest snapshot has a positive
            ask,
        When: the client POSTs the order,
        Then: the trade-command price and the plan's ``reference_price``
            param are the ask, confirming the buy side prices at the ask.
        """
        repo = self._paper_market_repo()
        repo.get_market_snapshots = AsyncMock(
            return_value=[_snapshot_row(bid=100.0, ask=101.0, last=100.5)]
        )
        repo.get_candles = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="buy"))
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        assert cmd_insert["price"] == 101.0
        assert plan_insert["params"]["reference_price"] == 101.0
        repo.get_candles.assert_not_called()
        client.close()

    def test_paper_market_sell_uses_snapshot_bid(self) -> None:
        """Price a paper MARKET sell at the freshest snapshot bid.

        Given: a paper MARKET sell whose freshest snapshot has a positive
            bid,
        When: the client POSTs the order,
        Then: the trade-command price is the bid, confirming the sell side
            prices at the bid (the mirror of the buy-at-ask rule).
        """
        repo = self._paper_market_repo()
        repo.get_market_snapshots = AsyncMock(
            return_value=[_snapshot_row(bid=100.0, ask=101.0, last=100.5)]
        )
        repo.get_candles = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="sell"))
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert cmd_insert["price"] == 100.0
        client.close()

    def test_paper_market_picks_freshest_snapshot(self) -> None:
        """Select the newest snapshot by ``ts`` when several are returned.

        Given: two snapshots for the instrument with different ``ts`` and
            asks,
        When: the client POSTs a paper MARKET buy,
        Then: the price is the ask of the newest snapshot, confirming the
            resolver orders by ``ts`` rather than trusting list order.
        """
        repo = self._paper_market_repo()
        older = _snapshot_row(ts=_ts() - timedelta(seconds=10), ask=90.0)
        newer = _snapshot_row(ts=_ts(), ask=105.0)
        repo.get_market_snapshots = AsyncMock(return_value=[older, newer])
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="buy"))
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert cmd_insert["price"] == 105.0
        client.close()

    def test_paper_market_falls_through_to_candle_when_snapshot_unusable(self) -> None:
        """Fall through to a fresh candle close when snapshot legs are unusable.

        Given: a snapshot whose bid, ask and last are all None,
        When: the client POSTs a paper MARKET buy and a fresh 1m candle
            exists,
        Then: the price is the candle close — the resolver exhausts the
            snapshot legs then uses the candle fallback.
        """
        repo = self._paper_market_repo()
        repo.get_market_snapshots = AsyncMock(
            return_value=[_snapshot_row(bid=None, ask=None, last=None)]
        )
        repo.get_candles = AsyncMock(
            return_value=[_candle_row(close=250.0, timestamp=datetime.now(UTC))]
        )
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="buy"))
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        assert cmd_insert["price"] == 250.0
        assert plan_insert["params"]["reference_price"] == 250.0
        client.close()

    def test_paper_market_uses_candle_when_no_snapshot(self) -> None:
        """Use a fresh candle close when no snapshot is returned at all.

        Given: an empty snapshot list and a fresh 1m candle,
        When: the client POSTs a paper MARKET buy,
        Then: the price is the candle close, covering the no-snapshot
            branch into the candle fallback.
        """
        repo = self._paper_market_repo()
        repo.get_market_snapshots = AsyncMock(return_value=[])
        repo.get_candles = AsyncMock(
            return_value=[_candle_row(close=300.0, timestamp=datetime.now(UTC))]
        )
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="buy"))
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        assert cmd_insert["price"] == 300.0
        client.close()

    def test_paper_market_stale_candle_returns_400(self) -> None:
        """Reject when the only candle is older than the 120 s freshness gate.

        Given: no snapshot and a 1m candle whose timestamp is 300 s old,
        When: the client POSTs a paper MARKET buy,
        Then: the response is 400 ``no_reference_price`` and no order row
            is written — a stale candle is not a valid fill reference.
        """
        repo = _venue_failure_repo()
        _arm_execution_venue(repo, is_paper=True, credential_exchanges=("paper",))
        repo.get_market_snapshots = AsyncMock(return_value=[])
        repo.get_candles = AsyncMock(
            return_value=[
                _candle_row(close=250.0, timestamp=datetime.now(UTC) - timedelta(seconds=300))
            ]
        )
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="buy"))
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "no_reference_price"
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_paper_market_no_price_source_returns_400(self) -> None:
        """Reject when neither a snapshot nor a candle is available.

        Given: an empty snapshot list and an empty candle list,
        When: the client POSTs a paper MARKET buy,
        Then: the response is 400 ``no_reference_price`` and no order row
            is written, covering the no-candle rejection arm.
        """
        repo = _venue_failure_repo()
        _arm_execution_venue(repo, is_paper=True, credential_exchanges=("paper",))
        repo.get_market_snapshots = AsyncMock(return_value=[])
        repo.get_candles = AsyncMock(return_value=[])
        client = _create_client(repo)
        response = client.post("/api/orders", json=_paper_market_body(side="buy"))
        assert response.status_code == 400
        assert response.json()["detail"]["error_code"] == "no_reference_price"
        repo.insert_execution_plan.assert_not_called()
        client.close()

    def test_paper_limit_order_skips_reference_price(self) -> None:
        """Keep the limit price and never resolve a reference for LIMIT paper orders.

        Given: a paper LIMIT order carrying an explicit price,
        When: the client POSTs the order,
        Then: the trade-command price is the limit price, the snapshot
            lookup is never called, and no ``reference_price`` param is
            written — pricing is skipped for non-MARKET orders.
        """
        repo = self._paper_market_repo()
        repo.get_market_snapshots = AsyncMock(return_value=[_snapshot_row()])
        body = _create_order_body()
        body["payload"]["mode"] = "paper"
        client = _create_client(repo)
        response = client.post("/api/orders", json=body)
        assert response.status_code == 200
        cmd_insert = repo.insert_trade_command.call_args[0][0]
        plan_insert = repo.insert_execution_plan.call_args[0][0]
        assert cmd_insert["price"] == 50000.0
        assert "reference_price" not in plan_insert["params"]
        repo.get_market_snapshots.assert_not_called()
        client.close()
