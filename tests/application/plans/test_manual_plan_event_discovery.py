"""Discover manual plans created after the executor's startup snapshot."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.plans.service import PlanExecutorService
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData


def _manual_plan() -> ExecutionPlanRow:
    """Return the persisted shape of a REST-created manual plan."""
    now = datetime(2026, 11, 9, tzinfo=UTC)
    return ExecutionPlanRow(
        public_id="manual-plan",
        timestamp=now,
        session_id="rest-session",
        sequence_id=1,
        plan_type="manual_once",
        created_by_user_id="operator",
        created_by_strategy=None,
        created_via="api",
        instrument_public_id="xrp-eur",
        exchange="kraken",
        mode="live",
        shard_key="manual-shard",
        wallet_public_id="wallet",
        operator_public_id=None,
        total_quantity=4.07,
        filled_quantity=0.0,
        side="buy",
        parent_plan_public_id=None,
        position_cycle_public_id=None,
        params={"child_client_order_id": "manual-child", "native_instrument": "XRP/EUR"},
        status="active",
        created_at=now,
        started_at=now,
        completed_at=None,
        expires_at=None,
        cancel_requested_at=None,
        last_evaluated_at=None,
        last_error=None,
        idempotency_key=None,
        cancel_idempotency_key=None,
    )


@pytest.fixture
def executor() -> tuple[PlanExecutorService, AsyncMock]:
    """Build an empty runtime with a durable manual child-to-plan link."""
    repository = AsyncMock()
    repository.get_plan_public_id_for_client_order_id.return_value = "manual-plan"
    repository.get_execution_plan.return_value = _manual_plan()
    with (
        patch("snapper.application.plans.service.get_settings", return_value=MagicMock()),
        patch("snapper.application.plans.service.get_repository", return_value=repository),
    ):
        return PlanExecutorService(), repository


@pytest.mark.asyncio
async def test_manual_fill_discovers_and_completes_new_plan(
    executor: tuple[PlanExecutorService, AsyncMock],
) -> None:
    """The first fill indexes a plan created after startup and persists completion.

    Given a durable manual plan absent from the startup registry,
    When its complete fill reaches the running plan service,
    Then completion is persisted and no replacement command is emitted.
    """
    service, repository = executor
    now = datetime(2026, 11, 9, tzinfo=UTC)
    execution = ExecutionData(
        public_id="fill",
        timestamp=now,
        session_id="venue-session",
        sequence_id=1,
        client_order_id="manual-child",
        instrument="XRP/EUR",
        exchange="kraken",
        side="buy",
        size=4.07,
        price=1.0,
        last_size=4.07,
        last_price=1.0,
        fee=0.0,
        fee_asset="EUR",
        status="filled",
        executed_at=now,
    )

    await service._handle_execution(execution)

    repository.get_plan_public_id_for_client_order_id.assert_awaited_once_with(
        "manual-child", as_of=repository.get_execution_plan.await_args.kwargs["as_of"]
    )
    update = repository.update_execution_plan_status.await_args.kwargs
    assert update["public_id"] == "manual-plan"
    assert update["new_status"] == "completed"
    assert update["filled_quantity"] == 4.07
    assert service.plans == {}
    assert service._client_order_id_index == {}
    repository.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
async def test_manual_expiry_discovers_and_expires_new_plan(
    executor: tuple[PlanExecutorService, AsyncMock],
) -> None:
    """A dispatch expiry can terminalize a manual plan absent at startup.

    Given a manual child created after the service recovered its startup plans,
    When the coordinator publishes its local dispatch expiry,
    Then the service discovers its parent and persists the expired terminal state.
    """
    service, repository = executor
    now = datetime(2026, 11, 9, tzinfo=UTC)
    order = OrderData(
        public_id="manual-child",
        timestamp=now,
        session_id="coordinator-session",
        sequence_id=1,
        client_order_id="manual-child",
        instrument="XRP/EUR",
        exchange="kraken",
        side="buy",
        status="expired",
        order_type="market",
        size=4.07,
        filled_size=0.0,
        created_at=now,
    )

    await service._handle_order_status(order)

    update = repository.update_execution_plan_status.await_args.kwargs
    assert update["public_id"] == "manual-plan"
    assert update["new_status"] == "expired"
    assert service._client_order_id_index == {}
    repository.insert_trade_command.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["expired", "cancelled", "rejected"])
async def test_lightweight_event_frame_reaches_manual_plan(
    executor: tuple[PlanExecutorService, AsyncMock], event_type: str
) -> None:
    """Decode TTL/cancel bus events and preserve the live plan on cancel rejection.

    Given a lightweight lifecycle event for an unindexed manual child,
    When the service parses and dispatches the published frame,
    Then cancellation or expiry terminates its plan while cancel rejection leaves it active.
    """
    service, repository = executor
    now = datetime(2026, 11, 9, tzinfo=UTC)
    event = OrderEventData.model_validate(
        {
            "public_id": "manual-child",
            "timestamp": now,
            "session_id": "coordinator-session",
            "sequence_id": 1,
            "exchange_order_id": "",
            "client_order_id": "manual-child",
            "exchange": "kraken",
            "instrument": "XRP/EUR",
            "event": event_type,
            "reason": "expired by outbox dispatch TTL",
        }
    )
    topic = f"orders.events.kraken.XRP/EUR.{event_type}"

    message = service._parse_incoming_message(topic, event.publish_to(topic).decode("utf-8"))
    assert isinstance(message, OrderEventData)
    await service._dispatch_incoming_message(topic, message)

    if event_type == "rejected":
        repository.update_execution_plan_status.assert_not_awaited()
        assert service._client_order_id_index == {"manual-child": "manual-plan"}
    else:
        update = repository.update_execution_plan_status.await_args.kwargs
        assert update["public_id"] == "manual-plan"
        assert update["new_status"] == event_type
        assert service._client_order_id_index == {}


@pytest.mark.asyncio
async def test_discovery_indexes_manual_child_and_reuses_index(
    executor: tuple[PlanExecutorService, AsyncMock],
) -> None:
    """Repeated child events resolve through the index after the first lookup.

    Given a durable manual child link absent from the running service,
    When discovery runs twice for the same child identifier,
    Then the first lookup installs the evaluator and the second reuses its index.
    """
    service, repository = executor

    await service._load_manual_plan_for_child("manual-child")
    await service._load_manual_plan_for_child("manual-child")

    assert service._client_order_id_index == {"manual-child": "manual-plan"}
    assert isinstance(service.evaluators["manual-plan"], ManualOnceEvaluator)
    repository.get_plan_public_id_for_client_order_id.assert_awaited_once()
    repository.get_execution_plan.assert_awaited_once()


@pytest.mark.asyncio
async def test_discovery_ignores_child_without_durable_plan(
    executor: tuple[PlanExecutorService, AsyncMock],
) -> None:
    """Unrelated engine orders do not create a manual evaluator.

    Given an engine child without a durable execution-plan link,
    When manual-plan discovery receives its identifier,
    Then no plan lookup or evaluator registration follows.
    """
    service, repository = executor
    repository.get_plan_public_id_for_client_order_id.return_value = None

    await service._load_manual_plan_for_child("unrelated-child")

    repository.get_execution_plan.assert_not_awaited()
    assert service.plans == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["missing", "completed", "bracket"])
async def test_discovery_ignores_unavailable_manual_plan(
    executor: tuple[PlanExecutorService, AsyncMock], kind: str
) -> None:
    """Missing, terminal, or stateful plans are not installed as manual plans.

    Given a child link whose parent is missing, completed, or a stateful bracket,
    When the service attempts to discover a newly created manual plan,
    Then it leaves the active plan registry and child index unchanged.
    """
    service, repository = executor
    plan = _manual_plan()
    if kind == "missing":
        repository.get_execution_plan.return_value = None
    elif kind == "completed":
        plan["status"] = "completed"
        repository.get_execution_plan.return_value = plan
    else:
        plan["plan_type"] = "bracket"
        repository.get_execution_plan.return_value = plan

    await service._load_manual_plan_for_child("manual-child")

    assert service.plans == {}
    assert service._client_order_id_index == {}
