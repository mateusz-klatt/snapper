"""Coverage-oriented tests for  enforcer wiring branches.

Exercises the code paths added by the wiring that are not
covered by the existing engine/plan/REST route tests: the
``caps_enforcer is None`` fallback branches + the helper method
signatures + the lazy-construction path in
``PlanExecutorService.start``.
"""

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.plans.service import PlanExecutorService
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.trade.caps_enforcer import Guard
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.core.types import ExchangeEnum
from snapper.data.repository import SQLAlchemyRepository


def test_trader_coordinator_build_caps_enforcer_none_for_non_sqlalchemy_repo() -> None:
    """Non-SQLAlchemy repository causes ``_build_caps_enforcer`` to return ``None``.

    Given: a :class:`TraderCoordinator` whose ``self.repository`` is
        a :class:`MagicMock` (test fixture pattern),
    When: ``_build_caps_enforcer`` is invoked directly,
    Then: ``None`` is returned — the coordinator preserves the
        legacy no-enforcement path so existing unit tests
        stay byte-identical.
    """
    coord = TraderCoordinator()
    coord.repository = MagicMock()
    assert coord._build_caps_enforcer() is None


def test_trader_coordinator_build_caps_enforcer_returns_instance_for_sqlalchemy() -> None:
    """SQLAlchemy repository drives real :class:`TradingCapsEnforcer` construction.

    Given: a :class:`TraderCoordinator` whose ``self.repository`` is
        a real :class:`SQLAlchemyRepository` (in-memory aiosqlite),
    When: ``_build_caps_enforcer`` is invoked,
    Then: a :class:`TradingCapsEnforcer` is returned — covers the
        happy-path branch where the enforcer + USDConverter get
        wired together for production.
    """
    coord = TraderCoordinator()
    coord.repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    enforcer = coord._build_caps_enforcer()
    assert isinstance(enforcer, TradingCapsEnforcer)


def _stub_plan_guard() -> Guard:
    """Build the Guard payload the plan-service guard stubs yield.

    Returns:
        A :class:`Guard` with a placeholder submission and a NULL
        admission notional, matching what ``_emit_trade_command``
        reads after context entry.
    """
    return Guard(
        submission=TradeCommandSubmission(
            user_public_id=None,
            operator_public_id=None,
            wallet_public_id="w1",
            instrument_public_id="inst-1",
            command_type="submit",
            side="buy",
            order_type="market",
            quantity=None,
            price=None,
            source_surface="rest",
            idempotency_key=None,
        ),
        assigned_public_id="guard-pid",
        submitted_notional_usd=55.5,
    )


@pytest.mark.asyncio
async def test_trading_engine_send_order_with_enforcer_exercises_guard_branch() -> None:
    """``_send_order`` wraps the insert in ``guard_service_principal`` when enforcer set.

    Given: a :class:`TradingEngineService` constructed with a stub
        ``caps_enforcer`` and a stub repository,
    When: ``_send_order`` runs,
    Then: the enforcer's ``guard_service_principal`` is awaited
        exactly once and the insert happens inside the yielded
        ``Guard`` — covers engine/service.py lines 482-483 (the
        with-enforcer branch).
    """
    guard_calls = {"n": 0}

    class _GuardCtx:
        async def __aenter__(self) -> Guard:
            guard_calls["n"] += 1
            return Guard(
                submission=TradeCommandSubmission(
                    user_public_id=None,
                    operator_public_id=None,
                    wallet_public_id=None,
                    instrument_public_id=None,
                    command_type="submit",
                    side="buy",
                    order_type="market",
                    quantity=None,
                    price=None,
                    source_surface="strategy",
                    idempotency_key=None,
                ),
                assigned_public_id="stub-uuid7",
            )

        async def __aexit__(self, *_args: Any) -> None:
            """No-op exit; guard releases on context close."""

    stub_enforcer = MagicMock(spec=TradingCapsEnforcer)
    stub_enforcer.guard_service_principal = MagicMock(return_value=_GuardCtx())

    stub_repo = AsyncMock()
    stub_repo.insert_trade_command = AsyncMock(return_value=(1, "pid"))

    publisher = MagicMock()
    publisher.tracker.session_id = "sess"
    publisher.tracker.next_sequence = MagicMock(return_value=1)
    publisher.send = AsyncMock()

    engine = TradingEngineService(
        "BTC-USD",
        execution_socket=publisher,
        exchange=ExchangeEnum.PAPER,
        repository=stub_repo,
        outbox=None,
        caps_enforcer=stub_enforcer,
    )
    await engine._send_order(
        side="buy",
        size=1.0,
        price=100.0,
        reason="engine-buy",
        signaled_at=None,
        leverage=None,
        reduce_only=False,
    )
    assert guard_calls["n"] == 1
    stub_repo.insert_trade_command.assert_awaited_once()


@pytest.mark.asyncio
async def test_plan_executor_start_lazy_constructs_caps_enforcer() -> None:
    """``PlanExecutorService.start`` lazy-constructs enforcer when absent.

    Given: a :class:`PlanExecutorService` constructed with no
        caps_enforcer AND its repository is a
        :class:`SQLAlchemyRepository`,
    When: ``start()`` is invoked (via direct call bypassing the
        full subscriber wiring),
    Then: the service builds a :class:`TradingCapsEnforcer` +
        :class:`USDConverter` pair from the repo, covering
        plans/service.py lines 142-144. Subscriber / recovery /
        run-loop are stubbed to isolate the lazy-construction
    branch.
    """
    real_repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")

    service = PlanExecutorService()
    service.repository = real_repo
    service._setup_subscriber = MagicMock()
    service._setup_publisher = MagicMock()
    service._recover_plans = AsyncMock()
    service._run_loop = AsyncMock()

    assert service._caps_enforcer is None
    await service.start()
    assert isinstance(service._caps_enforcer, TradingCapsEnforcer)


@pytest.mark.asyncio
async def test_plan_executor_emit_trade_command_service_principal_path() -> None:
    """``_emit_trade_command`` routes to ``guard_service_principal`` when no user.

    Given: a :class:`PlanExecutorService` with a stub enforcer and
        a plan row whose ``created_by_user_id`` is None,
    When: ``_emit_trade_command`` is invoked,
    Then: the enforcer's ``guard_service_principal`` is used (not
        ``guard``) — covers the service-principal branch of
        plans/service.py ``_emit_trade_command``.
    """
    bypass_calls = {"n": 0}

    class _BypassCtx:
        async def __aenter__(self) -> Guard:
            bypass_calls["n"] += 1
            return _stub_plan_guard()

        async def __aexit__(self, *_args: Any) -> None:
            """No-op exit — service-principal path skips caps."""

    class _GuardCtx:
        async def __aenter__(self) -> None:
            raise AssertionError("guard() should not fire for created_by_user_id=None")

        async def __aexit__(self, *_args: Any) -> None:
            """Unreachable on the bypass branch."""

    stub_enforcer = MagicMock(spec=TradingCapsEnforcer)
    stub_enforcer.guard_service_principal = MagicMock(return_value=_BypassCtx())
    stub_enforcer.guard = MagicMock(return_value=_GuardCtx())

    service = PlanExecutorService(caps_enforcer=stub_enforcer)
    service.repository = MagicMock()
    service.repository.insert_trade_command = AsyncMock(return_value=(1, "pid"))

    plan_row: dict[str, Any] = {
        "public_id": "plan-1",
        "created_by_user_id": None,
        "wallet_public_id": "w1",
        "operator_public_id": None,
        "instrument_public_id": "inst-1",
        "side": "buy",
    }
    await service._emit_trade_command(
        row={"shard_key": "kraken.BTC-USD.live"},
        plan=plan_row,
        command_type="submit",
        side="buy",
        order_type="market",
        quantity=1.0,
    )
    assert bypass_calls["n"] == 1
    inserted = service.repository.insert_trade_command.await_args.args[0]
    assert inserted["submitted_notional_usd"] == 55.5


@pytest.mark.asyncio
async def test_plan_executor_emit_trade_command_direct_path_when_no_enforcer() -> None:
    """``_emit_trade_command`` calls ``insert_trade_command`` directly when enforcer is None.

    Given: a :class:`PlanExecutorService` with
        ``caps_enforcer=None`` (legacy test fixture),
    When: ``_emit_trade_command`` is invoked,
    Then: the insert happens without any enforcer wrap — covers
        the plans/service.py legacy-compat branch.
    """
    service = PlanExecutorService()
    service._caps_enforcer = None
    service.repository = MagicMock()
    service.repository.insert_trade_command = AsyncMock(return_value=(1, "pid"))

    plan_row: dict[str, Any] = {"public_id": "plan-1", "created_by_user_id": None}
    await service._emit_trade_command(
        row={"shard_key": "kraken.BTC-USD.live"},
        plan=plan_row,
        command_type="submit",
        side="buy",
        order_type="market",
        quantity=1.0,
    )
    service.repository.insert_trade_command.assert_awaited_once()


@pytest.mark.asyncio
async def test_plan_executor_emit_trade_command_user_bound_path_uses_guard() -> None:
    """``_emit_trade_command`` routes to ``guard()`` when plan has a user.

    Given: a stub enforcer + a plan row with non-None
        ``created_by_user_id``,
    When: ``_emit_trade_command`` is invoked,
    Then: the enforcer's ``guard()`` method is used (not the
        service-principal bypass) — covers the user-bound branch
        of ``_emit_trade_command``.
    """
    guard_calls = {"n": 0}

    class _GuardCtx:
        async def __aenter__(self) -> Guard:
            guard_calls["n"] += 1
            return _stub_plan_guard()

        async def __aexit__(self, *_args: Any) -> None:
            """No-op exit — guard holds per-user lock across block."""

    stub_enforcer = MagicMock(spec=TradingCapsEnforcer)
    stub_enforcer.guard = MagicMock(return_value=_GuardCtx())

    service = PlanExecutorService(caps_enforcer=stub_enforcer)
    service.repository = MagicMock()
    service.repository.insert_trade_command = AsyncMock(return_value=(1, "pid"))

    plan_row: dict[str, Any] = {
        "public_id": "plan-1",
        "created_by_user_id": "user-1",
        "wallet_public_id": "w1",
        "operator_public_id": "op-1",
        "instrument_public_id": "inst-1",
        "side": "buy",
    }
    await service._emit_trade_command(
        row={"shard_key": "kraken.BTC-USD.live"},
        plan=plan_row,
        command_type="submit",
        side="buy",
        order_type="market",
        quantity=1.0,
    )
    assert guard_calls["n"] == 1
    inserted = service.repository.insert_trade_command.await_args.args[0]
    assert inserted["submitted_notional_usd"] == 55.5


def _usd_converter_unused_marker() -> USDConverter | None:
    """Keep :class:`USDConverter` import live so mypy accepts the re-export."""
    return None
