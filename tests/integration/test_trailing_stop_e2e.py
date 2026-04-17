"""End-to-end paper-mode integration tests for the TrailingStop plan flow.

These tests exercise the shipped TrailingStopEvaluator end-to-end against
a live ZMQ broker and SQLite DB, without mocking any of the runtime path:

1. Insert a ``position_cycles`` + ``execution_plans`` + capability row
   directly via the repository.
2. Spin up a real ``PlanExecutorService`` so recovery picks up the plan
   and installs the evaluator + symbol index.
3. Publish ``TickData`` frames on ``market.{exchange}.{native}.ticks``.
4. Assert the expected ``TradeCommand`` row, ``ExecutionPlanDecision``
   row, and plan-row transitions (or their absence for the negative
   scenarios).

All tests are gated behind the ``integration`` pytest marker and excluded
from the default ``make test`` run via ``pyproject.toml``. Run via::

    make test-integration

The attach path deliberately bypasses the FastAPI HTTP layer and uses
``service._register_plan`` implicitly (via ``_recover_plans`` on startup)
because the unit tests at ``tests/server/test_trailing_stop_routes.py``
already cover the HTTP ingress; these E2Es cover the runtime path the
unit suites cannot reach: ZMQ tick delivery, real ``_clock_loop`` sweep,
and the ``_write_checkpoints`` + ``restore_from_checkpoint`` round-trip.
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

import pytest
import pytest_asyncio
import zmq
import zmq.asyncio
from sqlalchemy import select

import snapper.config.settings as snapper_settings
from snapper.application.plans.service import PlanExecutorService
from snapper.data.models import ExecutionPlan
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import TradeCommand
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.messaging.infrastructure.broker import ZmqBrokerThread
from snapper.messaging.schemas.data import TickData
from tests.integration.conftest import _free_tcp_port
from tests.integration.conftest import _patch_settings_for_e2e

pytestmark = pytest.mark.integration


_EXCHANGE = "kraken_futures"
_MODE = "paper"


def _now() -> datetime:
    return datetime.now(UTC)


@pytest_asyncio.fixture
async def trailing_stop_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[dict[str, Any]]:
    """Spin up broker + repo handle; tests own PlanExecutorService lifetime.

    Each test must start (and stop) its own ``PlanExecutorService`` so
    the plan row can be inserted BEFORE the service's recovery runs.
    """
    xsub_port = _free_tcp_port()
    xpub_port = _free_tcp_port()
    xsub_endpoint = f"tcp://127.0.0.1:{xsub_port}"
    xpub_endpoint = f"tcp://127.0.0.1:{xpub_port}"
    _patch_settings_for_e2e(monkeypatch, xsub_endpoint, xpub_endpoint)

    broker = ZmqBrokerThread(xsub_endpoint=xsub_endpoint, xpub_endpoint=xpub_endpoint)
    broker.start()

    settings = snapper_settings.get_settings()
    repo = get_repository(settings.db_url)
    client_context = zmq.asyncio.Context()

    try:
        yield {
            "broker": broker,
            "repo": repo,
            "xsub_endpoint": xsub_endpoint,
            "xpub_endpoint": xpub_endpoint,
            "client_context": client_context,
        }
    finally:
        with contextlib.suppress(Exception):
            broker.stop()
        with contextlib.suppress(Exception):
            client_context.term()


async def _seed_capability_row(repo: Any, *, instrument_public_id: str) -> None:
    """Insert an InstrumentOrderCapability row with supports_reduce_only=True.

    The trailing-stop ``_check_capabilities`` gate fails the plan unless
    this row exists at dispatch time.
    """
    now = _now()
    async with repo.session() as s:
        s.add(
            InstrumentOrderCapability(
                instrument_public_id=instrument_public_id,
                exchange=_EXCHANGE,
                supported_order_types=["market", "limit"],
                supports_post_only=False,
                supports_reduce_only=True,
                supports_amend_in_place=False,
                supports_native_stop_loss=False,
                supports_native_take_profit=False,
                supports_trailing_stop_client_side=False,
                supports_market_making=False,
                supports_short_selling=True,
                supports_leverage=True,
                max_leverage_long=5.0,
                max_leverage_short=5.0,
                min_notional=None,
                max_order_size=None,
                top_of_book_quality="realtime",
                timestamp=now,
                session_id=f"e2e-cap-{uuid7().hex[:8]}",
                sequence_id=1,
            )
        )
        await s.commit()


async def _seed_open_cycle(
    repo: Any,
    *,
    direction: str,
    native_instrument: str,
    instrument_public_id: str,
    wallet_public_id: str,
    shard_key: str,
    max_qty: float,
) -> str:
    """Insert an open position_cycles row and return its public_id."""
    now = _now()
    row: dict[str, Any] = {
        "instrument_public_id": instrument_public_id,
        "exchange": _EXCHANGE,
        "mode": _MODE,
        "shard_key": shard_key,
        "wallet_public_id": wallet_public_id,
        "operator_public_id": None,
        "direction": direction,
        "max_qty": max_qty,
        "status": "open",
        "opened_at": now,
        "closed_at": None,
        "opening_command_public_id": None,
        "closing_command_public_id": None,
        "session_id": f"e2e-cycle-{uuid7().hex[:8]}",
        "sequence_id": 1,
        "timestamp": now,
    }
    _id, public_id = await repo.insert_position_cycle(row)
    return str(public_id)


async def _seed_trailing_stop_plan(
    repo: Any,
    *,
    cycle_public_id: str,
    instrument_public_id: str,
    native_instrument: str,
    wallet_public_id: str,
    shard_key: str,
    side: str,
    entry_price: float,
    total_quantity: float,
    trailing_pct: float,
    min_lock_pct: float,
) -> str:
    """Insert an armed trailing_stop ExecutionPlan row and return its public_id."""
    now = _now()
    row: dict[str, Any] = {
        "plan_type": "trailing_stop",
        "created_by_user_id": "e2e-user",
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": instrument_public_id,
        "exchange": _EXCHANGE,
        "mode": _MODE,
        "shard_key": shard_key,
        "wallet_public_id": wallet_public_id,
        "operator_public_id": None,
        "total_quantity": total_quantity,
        "side": side,
        "params": {
            "native_instrument": native_instrument,
            "trailing_pct": trailing_pct,
            "min_lock_pct": min_lock_pct,
            "entry_price": entry_price,
            "leverage": None,
        },
        "status": "armed",
        "created_at": now,
        "parent_plan_public_id": None,
        "position_cycle_public_id": cycle_public_id,
        "expires_at": None,
        "idempotency_key": f"e2e-{uuid7().hex}",
        "session_id": f"e2e-plan-{uuid7().hex[:8]}",
        "sequence_id": 1,
        "timestamp": now,
    }
    _id, plan_public_id = await repo.insert_execution_plan(row)
    return str(plan_public_id)


async def _start_service(service: PlanExecutorService) -> asyncio.Task[None]:
    """Start the service's ``start()`` coroutine in a background task."""
    task = asyncio.create_task(service.start())
    await asyncio.sleep(1.5)
    return task


async def _stop_service(service: PlanExecutorService, task: asyncio.Task[None]) -> None:
    """Cleanly stop the service and cancel its background task."""
    await service.stop()
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(task, timeout=2.0)


async def _publish_tick(
    ctx: zmq.asyncio.Context,
    xsub_endpoint: str,
    *,
    native_instrument: str,
    last: float,
) -> None:
    """Publish a TickData frame on market.{exchange}.{native}.ticks."""
    pub = ctx.socket(zmq.PUB)
    pub.connect(xsub_endpoint)
    await asyncio.sleep(0.2)
    topic = f"market.{_EXCHANGE}.{native_instrument}.ticks"
    tick = TickData(
        public_id=str(uuid7()),
        timestamp=_now(),
        session_id="e2e-tick",
        sequence_id=1,
        instrument=native_instrument,
        exchange=_EXCHANGE,
        volume=0.0,
        bid=last,
        ask=last,
        last=last,
    )
    await pub.send_multipart([topic.encode(), tick.to_json().encode()])
    await asyncio.sleep(0.2)
    pub.setsockopt(zmq.LINGER, 0)
    pub.close()


async def _wait_for_plan_status(
    repo: Any,
    plan_public_id: str,
    target: str,
    *,
    timeout_s: float = 5.0,
) -> dict[str, Any]:
    """Poll until the plan row reaches ``target`` status or timeout fires."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    last: dict[str, Any] | None = None
    while True:
        plan = await repo.get_execution_plan(plan_public_id, as_of=_now())
        if plan is not None:
            last = dict(plan)
            if plan["status"] == target:
                return last
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"Plan {plan_public_id} did not reach status={target!r} "
                f"within {timeout_s}s; last={last}"
            )
        await asyncio.sleep(0.1)


async def _get_trade_commands_for_plan(
    repo: Any,
    plan_public_id: str,
) -> list[dict[str, Any]]:
    """Return active TradeCommand rows whose correlation_id matches the plan."""
    now = _now()
    async with repo.session() as s:
        result = await s.execute(
            select(TradeCommand)
            .where(
                TradeCommand.correlation_id == plan_public_id,
                *where_active(TradeCommand, now),
            )
            .order_by(TradeCommand.created_at)
        )
        rows: list[dict[str, Any]] = []
        for cmd in result.scalars().all():
            rows.append(
                {
                    "public_id": cmd.public_id,
                    "command_type": cmd.command_type,
                    "strategy_id": cmd.strategy_id,
                    "correlation_id": cmd.correlation_id,
                    "side": cmd.side,
                    "order_type": cmd.order_type,
                    "quantity": cmd.quantity,
                    "reduce_only": cmd.reduce_only,
                    "status": cmd.status,
                    "idempotency_key": cmd.idempotency_key,
                    "plan_public_id": cmd.plan_public_id,
                    "instrument": cmd.instrument,
                }
            )
        return rows


async def _get_active_execution_plan_row(repo: Any, plan_public_id: str) -> Any:
    """Return the ORM row for the currently active version of a plan."""
    now = _now()
    async with repo.session() as s:
        stmt = select(ExecutionPlan).where(
            ExecutionPlan.public_id == plan_public_id,
            *where_active(ExecutionPlan, now),
        )
        result = await s.execute(stmt)
        return result.scalars().first()


def _unique_ids(suffix: str) -> dict[str, str]:
    """Generate a set of unique identifiers for an E2E scenario."""
    token = uuid7().hex
    native = f"BTC-{suffix}-{token[:6].upper()}"
    return {
        "native_instrument": native,
        "instrument_public_id": f"inst-{suffix}-{token[:8]}",
        "wallet_public_id": f"wallet-{suffix}-{token[:8]}",
        "shard_key": f"{_EXCHANGE}.{native}.{_MODE}",
    }


class TestTrailingStopE2E:
    """Five end-to-end scenarios covering the trailing stop runtime path."""

    async def test_trailing_stop_long_ratchet_then_breach(
        self, trailing_stop_stack: dict[str, Any]
    ) -> None:
        """Long position ratchets peak on 100->120, fires close at 113.

        Scenario:
            entry=100, trailing_pct=5, min_lock_pct=0.
            Ticks: 100 -> 120 -> 113.

        Expectation:
            After tick=120, peak=120 and stop=114 but no close (last>stop).
            Tick=113 breaches stop; a single reduce_only market SELL
            TradeCommand row is written with correlation_id=plan_public_id
            and the plan transitions armed -> active.
        """
        stack = trailing_stop_stack
        ids = _unique_ids("long")
        await _seed_capability_row(stack["repo"], instrument_public_id=ids["instrument_public_id"])
        cycle_public_id = await _seed_open_cycle(
            stack["repo"],
            direction="long",
            native_instrument=ids["native_instrument"],
            instrument_public_id=ids["instrument_public_id"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            max_qty=1.0,
        )
        plan_public_id = await _seed_trailing_stop_plan(
            stack["repo"],
            cycle_public_id=cycle_public_id,
            instrument_public_id=ids["instrument_public_id"],
            native_instrument=ids["native_instrument"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            side="buy",
            entry_price=100.0,
            total_quantity=1.0,
            trailing_pct=5.0,
            min_lock_pct=0.0,
        )
        service = PlanExecutorService()
        service.repository = stack["repo"]
        task = await _start_service(service)
        try:
            assert plan_public_id in service.plans
            for last in (100.0, 120.0, 113.0):
                await _publish_tick(
                    stack["client_context"],
                    stack["xsub_endpoint"],
                    native_instrument=ids["native_instrument"],
                    last=last,
                )
            await _wait_for_plan_status(stack["repo"], plan_public_id, "active")
            commands = await _get_trade_commands_for_plan(stack["repo"], plan_public_id)
            assert len(commands) == 1
            cmd = commands[0]
            assert cmd["strategy_id"] == "trailing_stop"
            assert cmd["correlation_id"] == plan_public_id
            assert cmd["plan_public_id"] == plan_public_id
            assert cmd["side"] == "sell"
            assert cmd["order_type"] == "market"
            assert cmd["quantity"] == pytest.approx(1.0)
            assert cmd["reduce_only"] is True
            assert cmd["status"] == "created"
            assert cmd["idempotency_key"] == f"{plan_public_id}:0"
            assert cmd["instrument"] == ids["native_instrument"]
            decisions = await stack["repo"].list_execution_plan_decisions(
                plan_public_id=plan_public_id, as_of=_now(), limit=50
            )
            emit = [d for d in decisions if d["decision_type"] == "command_emitted"]
            assert len(emit) == 1
            assert emit[0]["reason"] == "trailing_stop_hit"
            assert emit[0]["trigger_type"] == "tick"
            assert emit[0]["decision_importance"] == "action"
        finally:
            await _stop_service(service, task)

    async def test_trailing_stop_short_ratchet_then_breach(
        self, trailing_stop_stack: dict[str, Any]
    ) -> None:
        """Short position ratchets peak on 100->80, fires close at 85.

        Expectation:
            Reduce-only market BUY TradeCommand on tick=85 (stop=84).
        """
        stack = trailing_stop_stack
        ids = _unique_ids("short")
        await _seed_capability_row(stack["repo"], instrument_public_id=ids["instrument_public_id"])
        cycle_public_id = await _seed_open_cycle(
            stack["repo"],
            direction="short",
            native_instrument=ids["native_instrument"],
            instrument_public_id=ids["instrument_public_id"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            max_qty=1.0,
        )
        plan_public_id = await _seed_trailing_stop_plan(
            stack["repo"],
            cycle_public_id=cycle_public_id,
            instrument_public_id=ids["instrument_public_id"],
            native_instrument=ids["native_instrument"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            side="sell",
            entry_price=100.0,
            total_quantity=1.0,
            trailing_pct=5.0,
            min_lock_pct=0.0,
        )
        service = PlanExecutorService()
        service.repository = stack["repo"]
        task = await _start_service(service)
        try:
            for last in (100.0, 80.0, 85.0):
                await _publish_tick(
                    stack["client_context"],
                    stack["xsub_endpoint"],
                    native_instrument=ids["native_instrument"],
                    last=last,
                )
            await _wait_for_plan_status(stack["repo"], plan_public_id, "active")
            commands = await _get_trade_commands_for_plan(stack["repo"], plan_public_id)
            assert len(commands) == 1
            cmd = commands[0]
            assert cmd["side"] == "buy"
            assert cmd["reduce_only"] is True
            assert cmd["order_type"] == "market"
            assert cmd["quantity"] == pytest.approx(1.0)
        finally:
            await _stop_service(service, task)

    async def test_trailing_stop_min_lock_gate(self, trailing_stop_stack: dict[str, Any]) -> None:
        """min_lock_pct=5 on long entry=100 blocks trailing while peak<105.

        Scenario:
            Ticks 100 -> 103 -> 101 all sit below entry*1.05, so the
            trailing stop never arms and no close is emitted.

        Expectation:
            Plan stays armed; zero TradeCommand rows for the plan.
        """
        stack = trailing_stop_stack
        ids = _unique_ids("minlock")
        await _seed_capability_row(stack["repo"], instrument_public_id=ids["instrument_public_id"])
        cycle_public_id = await _seed_open_cycle(
            stack["repo"],
            direction="long",
            native_instrument=ids["native_instrument"],
            instrument_public_id=ids["instrument_public_id"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            max_qty=1.0,
        )
        plan_public_id = await _seed_trailing_stop_plan(
            stack["repo"],
            cycle_public_id=cycle_public_id,
            instrument_public_id=ids["instrument_public_id"],
            native_instrument=ids["native_instrument"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            side="buy",
            entry_price=100.0,
            total_quantity=1.0,
            trailing_pct=5.0,
            min_lock_pct=5.0,
        )
        service = PlanExecutorService()
        service.repository = stack["repo"]
        task = await _start_service(service)
        try:
            for last in (100.0, 103.0, 101.0):
                await _publish_tick(
                    stack["client_context"],
                    stack["xsub_endpoint"],
                    native_instrument=ids["native_instrument"],
                    last=last,
                )
            await asyncio.sleep(0.5)
            plan = await stack["repo"].get_execution_plan(plan_public_id, as_of=_now())
            assert plan is not None
            assert plan["status"] == "armed"
            commands = await _get_trade_commands_for_plan(stack["repo"], plan_public_id)
            assert commands == []
        finally:
            await _stop_service(service, task)

    async def test_trailing_stop_checkpoint_survives_restart(
        self, trailing_stop_stack: dict[str, Any]
    ) -> None:
        """Checkpoint persists peak/stop; post-restart evaluator re-engages.

        Scenario:
            - Start service, ratchet peak to 120 via ticks 100, 120.
            - Force a checkpoint via ``await service._write_checkpoints()``.
            - Stop the service; start a fresh PlanExecutorService.
            - Publish tick=115 — peak stays 120, stop stays 114, no close.
            - Publish tick=113 — stop breached, close command emitted.

        Expectation:
            State survives the round-trip; a SELL reduce_only market
            TradeCommand lands only after the post-restart 113 tick.
        """
        stack = trailing_stop_stack
        ids = _unique_ids("ckpt")
        await _seed_capability_row(stack["repo"], instrument_public_id=ids["instrument_public_id"])
        cycle_public_id = await _seed_open_cycle(
            stack["repo"],
            direction="long",
            native_instrument=ids["native_instrument"],
            instrument_public_id=ids["instrument_public_id"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            max_qty=1.0,
        )
        plan_public_id = await _seed_trailing_stop_plan(
            stack["repo"],
            cycle_public_id=cycle_public_id,
            instrument_public_id=ids["instrument_public_id"],
            native_instrument=ids["native_instrument"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            side="buy",
            entry_price=100.0,
            total_quantity=1.0,
            trailing_pct=5.0,
            min_lock_pct=0.0,
        )
        first = PlanExecutorService()
        first.repository = stack["repo"]
        first_task = await _start_service(first)
        try:
            for last in (100.0, 120.0):
                await _publish_tick(
                    stack["client_context"],
                    stack["xsub_endpoint"],
                    native_instrument=ids["native_instrument"],
                    last=last,
                )
            await asyncio.sleep(0.6)
            pre_state = first.evaluators[plan_public_id].build_checkpoint_state(
                first.plans[plan_public_id]
            )
            assert pre_state["peak_price"] == pytest.approx(120.0)
            assert pre_state["current_stop"] == pytest.approx(114.0)
            await first._write_checkpoints()
        finally:
            await _stop_service(first, first_task)
        commands_pre = await _get_trade_commands_for_plan(stack["repo"], plan_public_id)
        assert commands_pre == []

        second = PlanExecutorService()
        second.repository = stack["repo"]
        second_task = await _start_service(second)
        try:
            assert plan_public_id in second.evaluators
            restored = second.evaluators[plan_public_id].build_checkpoint_state(
                second.plans[plan_public_id]
            )
            assert restored["peak_price"] == pytest.approx(120.0)
            assert restored["current_stop"] == pytest.approx(114.0)

            await _publish_tick(
                stack["client_context"],
                stack["xsub_endpoint"],
                native_instrument=ids["native_instrument"],
                last=115.0,
            )
            await asyncio.sleep(0.4)
            plan_mid = await stack["repo"].get_execution_plan(plan_public_id, as_of=_now())
            assert plan_mid is not None
            assert plan_mid["status"] == "armed"
            assert await _get_trade_commands_for_plan(stack["repo"], plan_public_id) == []

            await _publish_tick(
                stack["client_context"],
                stack["xsub_endpoint"],
                native_instrument=ids["native_instrument"],
                last=113.0,
            )
            await _wait_for_plan_status(stack["repo"], plan_public_id, "active")
            commands = await _get_trade_commands_for_plan(stack["repo"], plan_public_id)
            assert len(commands) == 1
            assert commands[0]["side"] == "sell"
            assert commands[0]["reduce_only"] is True
        finally:
            await _stop_service(second, second_task)

    async def test_trailing_stop_cycle_close_sweeps_plan(
        self, trailing_stop_stack: dict[str, Any]
    ) -> None:
        """Cycle close triggers the 1Hz sweep to cancel the armed plan.

        Scenario:
            Attach trailing stop; close the cycle via
            ``close_position_cycle``; wait one full clock-loop iteration.

        Expectation:
            Plan transitions to ``cancelled`` with reason
            ``cycle_closed_externally`` and no TradeCommand row is emitted
            (sweep does not send a close order — the cycle is already
            flat).
        """
        stack = trailing_stop_stack
        ids = _unique_ids("sweep")
        await _seed_capability_row(stack["repo"], instrument_public_id=ids["instrument_public_id"])
        cycle_public_id = await _seed_open_cycle(
            stack["repo"],
            direction="long",
            native_instrument=ids["native_instrument"],
            instrument_public_id=ids["instrument_public_id"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            max_qty=1.0,
        )
        plan_public_id = await _seed_trailing_stop_plan(
            stack["repo"],
            cycle_public_id=cycle_public_id,
            instrument_public_id=ids["instrument_public_id"],
            native_instrument=ids["native_instrument"],
            wallet_public_id=ids["wallet_public_id"],
            shard_key=ids["shard_key"],
            side="buy",
            entry_price=100.0,
            total_quantity=1.0,
            trailing_pct=5.0,
            min_lock_pct=0.0,
        )
        service = PlanExecutorService()
        service.repository = stack["repo"]
        task = await _start_service(service)
        try:
            assert plan_public_id in service.plans
            now = _now()
            closed_row_id = await stack["repo"].close_position_cycle(
                cycle_public_id=cycle_public_id,
                closed_at=now,
                closing_command_public_id=None,
                bus_time=now,
                session_id=f"e2e-sweep-{uuid7().hex[:8]}",
                sequence_id=99,
            )
            assert closed_row_id is not None
            cancelled = await _wait_for_plan_status(
                stack["repo"], plan_public_id, "cancelled", timeout_s=6.0
            )
            assert cancelled["last_error"] == "cycle_closed_externally"
            commands = await _get_trade_commands_for_plan(stack["repo"], plan_public_id)
            assert commands == []
            decisions = await stack["repo"].list_execution_plan_decisions(
                plan_public_id=plan_public_id, as_of=_now(), limit=50
            )
            sweep_decisions = [
                d for d in decisions if d["decision_type"] == "cycle_closed_externally"
            ]
            assert len(sweep_decisions) == 1
            assert sweep_decisions[0]["trigger_type"] == "clock"
            assert sweep_decisions[0]["new_status"] == "cancelled"
        finally:
            await _stop_service(service, task)
