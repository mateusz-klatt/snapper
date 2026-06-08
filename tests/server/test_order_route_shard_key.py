"""Shard-key tests for the manual order REST route."""

from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from starlette.types import Scope

import snapper.server.order_routes as order_routes
from snapper.api.schemas.orders import CreateOrderBody
from snapper.api.schemas.orders import CreateOrderCommand
from snapper.application.engine.service import compute_shard_key
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionMode
from snapper.core.types import OrderExchange
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_NOW = datetime(2026, 4, 10, tzinfo=UTC)
_WALLET_PUBLIC_ID = "01968a3b-7c4d-7e0f-8a1b-2c3d4e5f6a7b"


class _AdmitCapsGuard:
    """Async context manager that admits cap-guarded submissions."""

    async def __aenter__(self) -> None:
        """Enter without rejecting the submission."""
        return None

    async def __aexit__(self, *_args: object) -> None:
        """Exit without suppressing exceptions."""
        return None


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


def _make_command() -> CreateOrderCommand:
    """Build a valid manual order command envelope."""
    return CreateOrderCommand(
        session_id="s1",
        sequence_id=1,
        public_id="req-1",
        timestamp=_NOW,
        payload=CreateOrderBody(
            instrument="BTC-USD",
            instrument_public_id="inst-1",
            exchange="kraken",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=0.5,
            price=50000.0,
            wallet_public_id=_WALLET_PUBLIC_ID,
        ),
    )


def _make_plan_row(shard_key: str) -> ExecutionPlanRow:
    """Build the plan row returned after REST order creation."""
    params: JsonObject = {
        "order_type": "limit",
        "side": "buy",
        "price": 50000.0,
        "child_client_order_id": "cid-child-1",
        "native_instrument": "BTC-USD",
        "venue_order_type": "limit",
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
        "shard_key": shard_key,
        "wallet_public_id": _WALLET_PUBLIC_ID,
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


def _make_enforcer() -> TradingCapsEnforcer:
    """Build a pass-through caps enforcer mock."""
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    enforcer.guard = MagicMock(return_value=_AdmitCapsGuard())
    return cast(TradingCapsEnforcer, enforcer)


@pytest.mark.asyncio
async def test_create_order_uses_wallet_aware_canonical_shard_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REST create order persists the canonical wallet-aware shard key.

    Given: a valid manual order with ``wallet_public_id`` set,
    When: ``create_order`` inserts its execution-plan and trade-command rows,
    Then: both rows use the same wallet-aware key as
        :func:`compute_shard_key`.
    """
    expected_shard_key = compute_shard_key(
        instrument="BTC-USD",
        exchange=cast(OrderExchange, "kraken"),
        mode=cast(ExecutionMode, "live"),
        wallet_public_id=_WALLET_PUBLIC_ID,
        strategy_tag=None,
    )
    repo = AsyncMock()
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
    repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-1"))
    repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-1"))
    repo.update_execution_plan_status = AsyncMock(return_value=2)
    repo.get_execution_plan = AsyncMock(return_value=_make_plan_row(expected_shard_key))
    monkeypatch.setattr(order_routes, "require_tradable", AsyncMock())

    response = await order_routes.create_order(
        request=_make_request(),
        principal=AuthPrincipal(
            username="test_user",
            role=UserRole.ADMIN,
            user_public_id="user-1",
        ),
        _csrf=None,
        command=_make_command(),
        repo=repo,
        caps_enforcer=_make_enforcer(),
    )

    plan_insert = repo.insert_execution_plan.await_args.args[0]
    cmd_insert = repo.insert_trade_command.await_args.args[0]
    assert response.payload.public_id == "plan-1"
    assert expected_shard_key != "kraken.BTC-USD.live"
    assert plan_insert["shard_key"] == expected_shard_key
    assert cmd_insert["shard_key"] == expected_shard_key
