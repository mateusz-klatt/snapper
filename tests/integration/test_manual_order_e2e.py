"""End-to-end paper-mode integration tests for the Manual Orders flow.

These tests exercise the Phase 1.5 happy path of the Execution Plans
framework end-to-end without mocking ZMQ transports:

1. Insert a ``manual_once`` ``ExecutionPlan`` directly in the DB.
2. Spin up a real ``PlanExecutorService`` against a live ZMQ broker so
   recovery picks up the freshly inserted plan.
3. Publish an ``ExecutionData`` frame on
   ``orders.events.paper.BTC-USD.executed``.
4. Assert the plan is re-read from DB as ``completed`` with the expected
   ``filled_quantity``.

All tests are gated behind the ``integration`` pytest marker and excluded
from the default ``make test`` / ``make check-all`` run via
``pyproject.toml [tool.pytest.ini_options] addopts``. Run via::

    make test-integration
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

import snapper.config.settings as snapper_settings
from snapper.application.plans.service import PlanExecutorService
from snapper.data.repository import get_repository
from snapper.messaging.infrastructure.broker import ZmqBrokerThread
from snapper.messaging.schemas.data import ExecutionData
from tests.integration.conftest import _free_tcp_port
from tests.integration.conftest import _patch_settings_for_e2e

pytestmark = pytest.mark.integration


_INSTRUMENT_PUBLIC_ID = "inst-btc-usd"
_NATIVE_INSTRUMENT = "BTC-USD"
_EXCHANGE = "paper"
_MODE = "live"
_WALLET = "wallet-e2e"


@pytest_asyncio.fixture
async def plan_broker_stack(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[dict[str, Any]]:
    """Spin up a standalone ZMQ broker + shared repo for plan executor tests.

    Does NOT start the PlanExecutorService — tests own startup so the
    plan row can be inserted before the service's recovery phase runs.

    Yields:
        Dict with the ``broker``, a ``repo`` handle (shared with the
        service via ``get_repository(settings.db_url)``), the
        ``xsub_endpoint`` for publishing test frames, and a
        ``client_context`` for ad-hoc PUB sockets.
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


async def _insert_plan(
    repo: Any,
    *,
    child_client_order_id: str,
    total_quantity: float = 1.0,
) -> str:
    """Insert a ``manual_once`` plan and return the DB-assigned public_id."""
    now = datetime.now(UTC)
    plan_row = {
        "plan_type": "manual_once",
        "created_by_user_id": "e2e-user",
        "created_via": "api",
        "instrument_public_id": _INSTRUMENT_PUBLIC_ID,
        "exchange": _EXCHANGE,
        "mode": _MODE,
        "shard_key": f"{_EXCHANGE}.{_NATIVE_INSTRUMENT}.{_MODE}",
        "wallet_public_id": _WALLET,
        "operator_public_id": None,
        "total_quantity": total_quantity,
        "side": "buy",
        "params": {
            "order_type": "market",
            "side": "buy",
            "child_client_order_id": child_client_order_id,
            "native_instrument": _NATIVE_INSTRUMENT,
            "venue_order_type": "market",
        },
        "status": "active",
        "created_at": now,
        "session_id": "e2e-session",
        "sequence_id": 1,
        "timestamp": now,
    }
    _id, plan_public_id = await repo.insert_execution_plan(plan_row)
    return str(plan_public_id)


async def _start_service_in_background(
    service: PlanExecutorService,
) -> asyncio.Task[None]:
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


async def _publish_execution(
    ctx: zmq.asyncio.Context,
    xsub_endpoint: str,
    *,
    client_order_id: str,
    size: float,
    status: str,
) -> None:
    """Publish an ExecutionData frame on the executed topic."""
    pub = ctx.socket(zmq.PUB)
    pub.connect(xsub_endpoint)
    await asyncio.sleep(0.2)
    topic = f"orders.events.{_EXCHANGE}.{_NATIVE_INSTRUMENT}.executed"
    now = datetime.now(UTC)
    execution = ExecutionData(
        public_id=str(uuid7()),
        timestamp=now,
        session_id="e2e-session",
        sequence_id=1,
        client_order_id=client_order_id,
        instrument=_NATIVE_INSTRUMENT,
        exchange=_EXCHANGE,
        side="buy",
        size=size,
        price=50000.0,
        last_size=size,
        last_price=50000.0,
        fee=0.0,
        fee_asset="USD",
        status=status,
        executed_at=now,
        wallet_public_id=_WALLET,
    )
    await pub.send_multipart([topic.encode(), execution.to_json().encode()])
    await asyncio.sleep(0.1)
    pub.setsockopt(zmq.LINGER, 0)
    pub.close()


async def _wait_for_plan_status(
    repo: Any,
    plan_public_id: str,
    target_status: str,
    *,
    timeout_s: float = 5.0,
) -> dict[str, Any]:
    """Poll the plan row until it reaches ``target_status`` or timeout fires."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    while True:
        plan = await repo.get_execution_plan(plan_public_id, as_of=datetime.now(UTC))
        if plan is not None and plan["status"] == target_status:
            return dict(plan)
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"Plan {plan_public_id} did not reach status={target_status!r} "
                f"within {timeout_s}s; last={plan}"
            )
        await asyncio.sleep(0.1)


async def _wait_for_plan_filled(
    repo: Any,
    plan_public_id: str,
    target_filled: float,
    *,
    timeout_s: float = 5.0,
) -> dict[str, Any]:
    """Poll until plan.filled_quantity reaches target or timeout fires."""
    deadline = asyncio.get_event_loop().time() + timeout_s
    last: dict[str, Any] | None = None
    while True:
        plan = await repo.get_execution_plan(plan_public_id, as_of=datetime.now(UTC))
        if plan is not None:
            last = dict(plan)
            if float(plan["filled_quantity"]) >= target_filled - 1e-9:
                return last
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"Plan {plan_public_id} did not reach filled>={target_filled} "
                f"within {timeout_s}s; last={last}"
            )
        await asyncio.sleep(0.1)


class TestManualOrderE2E:
    """Paper-mode Manual Order happy path."""

    async def test_plan_completes_on_full_fill(self, plan_broker_stack: dict[str, Any]) -> None:
        """Full fill drives the plan to ``completed`` via live ZMQ delivery.

        Scenario:
            - ``manual_once`` plan is inserted with ``total_quantity=1.0``
              BEFORE the service is started.
            - Service starts → recovers the plan → subscribes to
              ``orders.events.``.
            - Test publishes a full fill on the executed topic.

        Expectation:
            - Plan row transitions to ``status="completed"`` with
              ``filled_quantity=1.0`` and is deregistered from the
              service's in-memory map.
        """
        stack = plan_broker_stack
        child_client_order_id = str(uuid7())
        plan_public_id = await _insert_plan(
            stack["repo"],
            child_client_order_id=child_client_order_id,
            total_quantity=1.0,
        )
        service = PlanExecutorService()
        service.repository = stack["repo"]
        service_task = await _start_service_in_background(service)
        try:
            assert plan_public_id in service.plans

            await _publish_execution(
                stack["client_context"],
                stack["xsub_endpoint"],
                client_order_id=child_client_order_id,
                size=1.0,
                status="filled",
            )
            completed = await _wait_for_plan_status(stack["repo"], plan_public_id, "completed")
            assert completed["filled_quantity"] == pytest.approx(1.0)
            assert plan_public_id not in service.plans
        finally:
            await _stop_service(service, service_task)

    async def test_plan_stays_active_on_partial_fill(
        self, plan_broker_stack: dict[str, Any]
    ) -> None:
        """Partial fill updates ``filled_quantity`` but keeps plan active.

        Scenario:
            - ``manual_once`` plan is inserted with ``total_quantity=2.0``
              BEFORE the service is started.
            - Service starts → recovers the plan.
            - Test publishes a single fill with ``last_size=1.0``.

        Expectation:
            - Plan row stays ``status="active"`` with
              ``filled_quantity=1.0`` and is still registered in the
              service's in-memory map (ready for more fills).
        """
        stack = plan_broker_stack
        child_client_order_id = str(uuid7())
        plan_public_id = await _insert_plan(
            stack["repo"],
            child_client_order_id=child_client_order_id,
            total_quantity=2.0,
        )
        service = PlanExecutorService()
        service.repository = stack["repo"]
        service_task = await _start_service_in_background(service)
        try:
            assert plan_public_id in service.plans

            await _publish_execution(
                stack["client_context"],
                stack["xsub_endpoint"],
                client_order_id=child_client_order_id,
                size=1.0,
                status="partial",
            )
            plan = await _wait_for_plan_filled(stack["repo"], plan_public_id, target_filled=1.0)
            assert plan["status"] == "active"
            assert plan["filled_quantity"] == pytest.approx(1.0)
            assert plan_public_id in service.plans
        finally:
            await _stop_service(service, service_task)
