"""Direct REST create-order coverage for strict wallet autolookup."""

from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from pydantic import ValidationError
from starlette.types import Scope

import snapper.server.order_routes as order_routes
from snapper.api.schemas.orders import CreateOrderBody
from snapper.api.schemas.orders import CreateOrderCommand
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_NOW = datetime(2026, 4, 10, tzinfo=UTC)


class _AdmitCapsGuard:
    """Async context manager that admits cap-guarded submissions."""

    async def __aenter__(self) -> None:
        """Enter without rejecting the submission."""
        return None

    async def __aexit__(self, *_args: object) -> None:
        """Exit without suppressing exceptions."""
        return None


class _RejectCapsGuard:
    """Async context manager that rejects cap-guarded submissions."""

    async def __aenter__(self) -> None:
        """Raise a caps violation on guard entry."""
        raise CapsViolationError("max_open_orders", attempted=6.0, limit=5.0)

    async def __aexit__(self, *_args: object) -> None:
        """Exit without suppressing exceptions."""
        return None


class _CancelRaiser:
    """Callable cancel-service replacement that raises one configured exception."""

    def __init__(self, exc: Exception) -> None:
        """Store the exception that the replacement service method raises."""
        self._exc = exc

    async def __call__(self, **kwargs: object) -> ExecutionPlanRow:
        """Raise the configured exception when the cancel service is invoked."""
        del kwargs
        raise self._exc


def _make_request() -> Request:
    """Build a request carrying the REST sequence tracker."""
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    scope: Scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/orders",
        "headers": [],
        "app": app,
    }
    return Request(scope)


def _make_command(
    *,
    wallet_public_id: str | None,
    mode: str = "live",
    order_type: str = "limit",
    price: float | None = 50000.0,
    stop_price: float | None = None,
    leverage: int | None = None,
    ai_review_public_id: str | None = None,
) -> CreateOrderCommand:
    """Build a manual order command for direct route invocation."""
    return CreateOrderCommand(
        session_id="s1",
        sequence_id=1,
        public_id="req-1",
        timestamp=_NOW,
        payload=CreateOrderBody(
            instrument="BTC-USD",
            instrument_public_id="inst-1",
            exchange="kraken",
            mode=mode,
            side="buy",
            order_type=order_type,
            quantity=0.5,
            price=price,
            stop_price=stop_price,
            leverage=leverage,
            wallet_public_id=wallet_public_id,
            ai_review_public_id=ai_review_public_id,
        ),
    )


def _make_unvalidated_command(
    *,
    wallet_public_id: str | None,
    mode: str = "live",
) -> CreateOrderCommand:
    """Build a command while bypassing Pydantic field validators."""
    body = CreateOrderBody.model_construct(
        instrument="BTC-USD",
        instrument_public_id="inst-1",
        exchange="kraken",
        mode=mode,
        side="buy",
        order_type="limit",
        quantity=0.5,
        price=50000.0,
        stop_price=None,
        leverage=None,
        wallet_public_id=wallet_public_id,
        ai_review_public_id=None,
    )
    return CreateOrderCommand.model_construct(
        session_id="s1",
        sequence_id=1,
        public_id="req-1",
        timestamp=_NOW,
        payload=body,
    )


def _make_principal(operator_public_ids: list[str] | None = None) -> AuthPrincipal:
    """Build an operator principal for REST wallet-scope checks."""
    return AuthPrincipal(
        username="operator",
        role=UserRole.OPERATOR,
        user_public_id="user-1",
        operator_public_ids=operator_public_ids if operator_public_ids is not None else ["op-1"],
        primary_operator_public_id="op-1",
    )


def _make_wallet(public_id: str, *, is_paper: bool = False) -> WalletRow:
    """Build the wallet row consumed by the shared resolver."""
    return {
        "public_id": public_id,
        "label": public_id,
        "description": None,
        "is_paper": is_paper,
        "timestamp": _NOW,
        "session_id": "s1",
        "sequence_id": 1,
    }


def _make_plan_row(wallet_public_id: str) -> ExecutionPlanRow:
    """Build the plan row returned after REST order creation."""
    params: JsonObject = {
        "order_type": "limit",
        "side": "buy",
        "price": 50000.0,
        "child_client_order_id": "cid-child-1",
        "native_instrument": "BTC-USD",
    }
    return {
        "public_id": "plan-1",
        "timestamp": _NOW,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": "manual_once",
        "created_by_user_id": "user-1",
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": "inst-1",
        "exchange": "kraken",
        "mode": "live",
        "shard_key": "kraken.BTC-USD.live.wallet",
        "wallet_public_id": wallet_public_id,
        "operator_public_id": None,
        "total_quantity": 0.5,
        "filled_quantity": 0.0,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": None,
        "params": params,
        "status": "active",
        "created_at": _NOW,
        "started_at": _NOW,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": None,
        "cancel_idempotency_key": None,
    }


def _make_repo(*, plan_wallet_public_id: str = "wallet-1") -> AsyncMock:
    """Build a repository mock for direct create-order calls."""
    repo = AsyncMock()
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
    repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
    repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-1"))
    repo.update_execution_plan_status = AsyncMock(return_value=2)
    repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(plan_wallet_public_id))
    return repo


def _make_enforcer() -> TradingCapsEnforcer:
    """Build a pass-through caps enforcer mock."""
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    enforcer.guard = MagicMock(return_value=_AdmitCapsGuard())
    return cast(TradingCapsEnforcer, enforcer)


def _make_rejecting_enforcer() -> TradingCapsEnforcer:
    """Build a caps enforcer mock that rejects submissions."""
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    enforcer.guard = MagicMock(return_value=_RejectCapsGuard())
    return cast(TradingCapsEnforcer, enforcer)


@pytest.mark.parametrize("wallet_public_id", ["", "   "])
def test_create_order_schema_rejects_blank_wallet_public_id(
    wallet_public_id: str,
) -> None:
    """Blank REST wallet IDs fail at schema validation.

    Given: an explicit empty or whitespace-only ``wallet_public_id``,
    When: the REST create-order command is parsed,
    Then: Pydantic rejects the request before route-level wallet lookup.
    """
    with pytest.raises(ValidationError) as exc_info:
        _make_command(wallet_public_id=wallet_public_id)

    assert "wallet_public_id must not be blank" in str(exc_info.value)


@pytest.mark.asyncio
async def test_create_order_omitted_wallet_resolves_single_live_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Single live accessible wallet is bound when the request omits a wallet.

    Given: an operator principal with exactly one accessible live wallet,
    When: the create-order handler receives no ``wallet_public_id``,
    Then: the resolved wallet is persisted on the plan and command rows.
    """
    repo = _make_repo(plan_wallet_public_id="wallet-live")
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-live")]
    )
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    response = await order_routes.create_order(
        request=_make_request(),
        principal=_make_principal(),
        _csrf=None,
        command=_make_command(wallet_public_id=None),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    plan_insert = repo.insert_execution_plan.await_args.args[0]
    cmd_insert = repo.insert_trade_command.await_args.args[0]
    assert response.payload.wallet_public_id == "wallet-live"
    assert plan_insert["wallet_public_id"] == "wallet-live"
    assert cmd_insert["wallet_public_id"] == "wallet-live"
    assert repo.list_accessible_wallets_for_operators.await_count == 2


@pytest.mark.asyncio
async def test_create_order_with_ai_review_citation_uses_resolved_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AI-review citation validation receives the resolved wallet.

    Given: an omitted-wallet order with one live wallet and a valid citation,
    When: the create-order handler validates the citation,
    Then: the review lookup is checked against the resolved wallet ID.
    """
    repo = _make_repo(plan_wallet_public_id="wallet-live")
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-live")]
    )
    repo.get_ai_review = AsyncMock(
        return_value={
            "public_id": "review-1",
            "user_public_id": "user-1",
            "wallet_public_id": "wallet-live",
            "status": "resolved_approved",
        }
    )
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    await order_routes.create_order(
        request=_make_request(),
        principal=_make_principal(),
        _csrf=None,
        command=_make_command(wallet_public_id=None, ai_review_public_id="review-1"),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    repo.get_ai_review.assert_awaited_once_with("review-1")


@pytest.mark.asyncio
async def test_create_order_unknown_ai_review_citation_returns_403(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AI-review citation failures map to HTTP 403.

    Given: an omitted-wallet order whose cited AI review is unknown,
    When: the create-order handler validates the citation,
    Then: the handler raises HTTP 403 before inserting order rows.
    """
    repo = _make_repo(plan_wallet_public_id="wallet-live")
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-live")]
    )
    repo.get_ai_review = AsyncMock(return_value=None)
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id=None, ai_review_public_id="review-missing"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 403
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_omitted_wallet_rejects_multiple_live_wallets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple live candidates are a 400 and no order rows are written.

    Given: an operator principal with two accessible live wallets,
    When: the create-order handler receives no ``wallet_public_id``,
    Then: the handler raises HTTP 400 without inserting plan or command rows.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-a"), _make_wallet("wallet-b")]
    )
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id=None),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "specify wallet_public_id; 2 wallets accessible"
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_omitted_wallet_rejects_zero_live_wallets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zero live candidates are a 400 and no order rows are written.

    Given: an operator principal with no accessible live wallets,
    When: the create-order handler receives no ``wallet_public_id``,
    Then: the handler raises HTTP 400 without inserting plan or command rows.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id=None),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "specify wallet_public_id; 0 wallets accessible"
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_omitted_live_wallet_rejects_single_paper_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live order never binds a single accessible paper wallet.

    Given: an operator principal with exactly one accessible paper wallet,
    When: the create-order handler receives a live order with no wallet,
    Then: the handler raises HTTP 400 without binding the paper wallet.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-paper", is_paper=True)]
    )
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id=None),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "specify wallet_public_id; 0 wallets accessible"
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_omitted_live_wallet_rejects_missing_operator_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live autolookup fails closed when the principal has no operators.

    Given: an operator principal whose operator set is empty,
    When: the create-order handler receives a live order with no wallet,
    Then: the handler raises HTTP 400 before querying accessible wallets.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock()
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal([]),
            _csrf=None,
            command=_make_command(wallet_public_id=None),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "specify wallet_public_id; 0 wallets accessible"
    repo.list_accessible_wallets_for_operators.assert_not_called()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("wallet_public_id", "wallets"),
    [
        ("", [_make_wallet("wallet-live")]),
        ("   ", [_make_wallet("wallet-a"), _make_wallet("wallet-b")]),
    ],
)
async def test_create_order_blank_wallet_rejects_before_autolookup(
    wallet_public_id: str,
    wallets: list[WalletRow],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Blank REST wallet IDs do not enter single-wallet autolookup.

    Given: a blank explicit wallet ID and either one or many accessible wallets,
    When: a defensively constructed create-order command reaches the handler,
    Then: the handler returns HTTP 400 without querying wallets or writing rows.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=wallets)
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_unvalidated_command(wallet_public_id=wallet_public_id),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "wallet_public_id must not be blank"
    repo.list_accessible_wallets_for_operators.assert_not_called()
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_explicit_wallet_bypasses_autolookup_but_keeps_scope_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit in-scope wallet writes unchanged even when multiple wallets exist.

    Given: an operator principal with multiple accessible live wallets,
    When: the create-order handler receives an explicit in-scope wallet,
    Then: that wallet is persisted after the existing scope validation runs.
    """
    repo = _make_repo(plan_wallet_public_id="wallet-1")
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-1"), _make_wallet("wallet-2")]
    )
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    await order_routes.create_order(
        request=_make_request(),
        principal=_make_principal(),
        _csrf=None,
        command=_make_command(wallet_public_id="wallet-1"),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    plan_insert = repo.insert_execution_plan.await_args.args[0]
    cmd_insert = repo.insert_trade_command.await_args.args[0]
    assert plan_insert["wallet_public_id"] == "wallet-1"
    assert cmd_insert["wallet_public_id"] == "wallet-1"
    assert repo.list_accessible_wallets_for_operators.await_count == 1


@pytest.mark.asyncio
async def test_create_order_invalid_params_return_422(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Manual-order validation failures map to HTTP 422.

    Given: a limit order with no limit price,
    When: the create-order handler validates manual-order params,
    Then: the handler raises HTTP 422 before repository writes.
    """
    repo = _make_repo()
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1", price=None),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 422
    repo.insert_execution_plan.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_unknown_instrument_returns_422(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown canonical instrument lookup maps to HTTP 422.

    Given: the capability guard admits a symbol that cannot be canonicalized,
    When: the create-order handler resolves the instrument public ID,
    Then: the handler raises HTTP 422 before wallet resolution or writes.
    """
    repo = _make_repo()
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    repo.list_accessible_wallets_for_operators = AsyncMock()
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 422
    repo.list_accessible_wallets_for_operators.assert_not_called()
    repo.insert_execution_plan.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_stop_with_leverage_persists_optional_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Optional stop and leverage params are persisted on the plan.

    Given: a stop order with stop price and leverage but no limit price,
    When: the create-order handler builds plan params,
    Then: stop price and leverage are present and price is absent.
    """
    repo = _make_repo(plan_wallet_public_id="wallet-1")
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_make_wallet("wallet-1")])
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    await order_routes.create_order(
        request=_make_request(),
        principal=_make_principal(),
        _csrf=None,
        command=_make_command(
            wallet_public_id="wallet-1",
            order_type="stop",
            price=None,
            stop_price=49000.0,
            leverage=3,
        ),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    plan_insert = repo.insert_execution_plan.await_args.args[0]
    assert "price" not in plan_insert["params"]
    assert plan_insert["params"]["stop_price"] == 49000.0
    assert plan_insert["params"]["leverage"] == 3


@pytest.mark.asyncio
async def test_create_order_plan_insert_unique_conflict_returns_409(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan idempotency conflicts map to HTTP 409.

    Given: the execution-plan insert raises a duplicate-key error,
    When: the create-order handler attempts to persist the plan,
    Then: the handler raises HTTP 409 and does not insert a command.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_make_wallet("wallet-1")])
    repo.insert_execution_plan = AsyncMock(side_effect=RuntimeError("duplicate key"))
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 409
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_plan_insert_generic_error_returns_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic plan insert failures map to HTTP 500.

    Given: the execution-plan insert raises an unexpected error,
    When: the create-order handler attempts to persist the plan,
    Then: the handler raises HTTP 500 and does not insert a command.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_make_wallet("wallet-1")])
    repo.insert_execution_plan = AsyncMock(side_effect=RuntimeError("database down"))
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 500
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_command_insert_failure_compensates_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Command insert failures compensate the plan and return HTTP 500.

    Given: the execution-plan insert succeeds but command insert fails,
    When: the create-order handler persists the command,
    Then: the plan is marked failed and HTTP 500 is raised.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_make_wallet("wallet-1")])
    repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("command insert failed"))
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 500
    failed_update = repo.update_execution_plan_status.await_args.kwargs
    assert failed_update["new_status"] == "failed"


@pytest.mark.asyncio
async def test_create_order_caps_violation_returns_422(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Caps violations map to HTTP 422.

    Given: the caps enforcer rejects the submission,
    When: the create-order handler enters the caps guard,
    Then: the handler raises HTTP 422 without inserting rows.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_make_wallet("wallet-1")])
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_rejecting_enforcer(),
        )

    assert exc_info.value.status_code == 422
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_order_plan_missing_after_insert_returns_500(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Post-insert plan reload failure maps to HTTP 500.

    Given: plan and command inserts succeed but plan reload returns none,
    When: the create-order handler loads the response payload,
    Then: the handler raises HTTP 500.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[_make_wallet("wallet-1")])
    repo.get_execution_plan = AsyncMock(return_value=None)
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 500


@pytest.mark.asyncio
async def test_cancel_order_wrapper_returns_cancel_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cancel route wrapper delegates to the shared cancel flow.

    Given: the cancel service returns an updated plan,
    When: the by-plan-id cancel route is invoked directly,
    Then: an execution-plan response is built for that plan.
    """

    async def _cancel_ok(**kwargs: object) -> ExecutionPlanRow:
        del kwargs
        return _make_plan_row("wallet-1")

    repo = AsyncMock()
    monkeypatch.setattr(order_routes.PlansCancelService, "cancel_by_plan_public_id", _cancel_ok)

    response = await order_routes.cancel_order(
        request=_make_request(),
        plan_public_id="plan-1",
        principal=_make_principal(),
        _csrf=None,
        command=MagicMock(),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    assert response.payload.public_id == "plan-1"
    assert response.payload.wallet_public_id == "wallet-1"


@pytest.mark.asyncio
async def test_cancel_plan_maps_domain_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancel-service domain errors map to REST HTTP responses.

    Given: each known cancel-service domain exception,
    When: the shared cancel helper catches it,
    Then: the helper raises the corresponding HTTP status and detail.
    """
    cases: list[tuple[Exception, int, str]] = [
        (order_routes.PlanNotFoundError("plan-1"), 404, "Execution plan not found"),
        (order_routes.PlanScopeError("plan-1"), 403, "Wallet not accessible"),
        (
            order_routes.PlanAlreadyTerminalError("plan-1", "completed"),
            409,
            "Plan already in terminal status: completed",
        ),
        (
            order_routes.PlanCancelInProgressError("plan-1"),
            409,
            "Plan status changed concurrently",
        ),
        (
            order_routes.PlanConcurrentChangeError("plan-1"),
            409,
            "Plan status changed concurrently",
        ),
        (
            order_routes.PlanCancelIdempotencyKeyMismatchError("plan-1"),
            409,
            "Plan status changed concurrently",
        ),
        (
            CapsViolationError("max_cancels_per_minute", attempted=11.0, limit=10.0),
            422,
            "caps_violation",
        ),
        (
            order_routes.PlanPostCancelReloadError("plan-1"),
            500,
            "Plan updated but not found",
        ),
        (
            order_routes.PlanCancelEmitError("plan-1", RuntimeError("emit failed")),
            500,
            "Failed to emit cancel command",
        ),
    ]

    for exc, status_code, detail_fragment in cases:
        monkeypatch.setattr(
            order_routes.PlansCancelService,
            "cancel_by_plan_public_id",
            _CancelRaiser(exc),
        )
        with pytest.raises(HTTPException) as exc_info:
            await order_routes._cancel_plan(
                repo=AsyncMock(),
                tracker=SequenceTracker(),
                principal=_make_principal(),
                plan_public_id="plan-1",
                caps_enforcer=_make_enforcer(),
            )
        assert exc_info.value.status_code == status_code
        assert detail_fragment in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_cancel_by_client_order_id_not_found_returns_404() -> None:
    """Unknown client order ID maps to HTTP 404.

    Given: no plan is linked to the supplied client order ID,
    When: the by-client-order-id cancel route is invoked,
    Then: the route raises HTTP 404 before calling the cancel service.
    """
    repo = AsyncMock()
    repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value=None)

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.cancel_order_by_client_order_id(
            request=_make_request(),
            client_order_id="missing-client-order",
            principal=_make_principal(),
            _csrf=None,
            command=MagicMock(),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_cancel_by_client_order_id_delegates_to_cancel_flow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Known client order ID delegates to the shared cancel flow.

    Given: a client order ID resolves to a plan public ID,
    When: the by-client-order-id cancel route is invoked,
    Then: the route returns the shared cancel response for that plan.
    """

    async def _cancel_ok(**kwargs: object) -> ExecutionPlanRow:
        del kwargs
        return _make_plan_row("wallet-1")

    repo = AsyncMock()
    repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-1")
    monkeypatch.setattr(order_routes.PlansCancelService, "cancel_by_plan_public_id", _cancel_ok)

    response = await order_routes.cancel_order_by_client_order_id(
        request=_make_request(),
        client_order_id="client-order-1",
        principal=_make_principal(),
        _csrf=None,
        command=MagicMock(),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    assert response.payload.public_id == "plan-1"


@pytest.mark.asyncio
async def test_create_order_explicit_wallet_out_of_scope_returns_403(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit out-of-scope wallet keeps the existing 403 scope rejection.

    Given: an operator principal without access to the explicit wallet,
    When: the create-order handler receives that explicit wallet,
    Then: the handler raises HTTP 403 without inserting plan or command rows.
    """
    repo = _make_repo()
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_make_wallet("wallet-other")]
    )
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await order_routes.create_order(
            request=_make_request(),
            principal=_make_principal(),
            _csrf=None,
            command=_make_command(wallet_public_id="wallet-1"),
            repo=repo,
            caps_enforcer=_make_enforcer(),
        )

    assert exc_info.value.status_code == 403
    repo.insert_execution_plan.assert_not_awaited()
    repo.insert_trade_command.assert_not_awaited()
