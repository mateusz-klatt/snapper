"""Refuse historical material drift before any late queued CREATE can reach a venue."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update

import snapper.application.trade.execution_products as product_policy
from snapper.application.trade.command_request import order_request_from_command
from snapper.core.command_material import COMMAND_MATERIAL_CONFLICT_EVENT
from snapper.core.command_material import TradeCommandMaterialChangedError
from snapper.core.command_material import TradeCommandMaterialUnavailableError
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderExchange
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.base import PendingOrderState
from snapper.messaging.schemas.data import OrderCancelData
from tests.helpers.command_material import MATERIAL_WALLET
from tests.helpers.command_material import active_command
from tests.helpers.command_material import seed_command_history


class MaterialExecutor(ExchangeExecutorService[ExchangeClientBase]):
    """Exercise actual CREATE/cancel boundaries with an injected transport double."""

    def __init__(self, repository: SQLAlchemyRepository, client: ExchangeClientBase) -> None:
        """Bind a real private repository without starting transports or a service."""
        super().__init__(wallet_public_id=MATERIAL_WALLET)
        self.repository = repository
        self.exchange_client = client
        self.actual_exchange: OrderExchange = "kraken"

    def _create_exchange_client(self) -> ExchangeClientBase:
        """Refuse accidental transport creation in this boundary-only regression."""
        raise AssertionError("Material regressions cannot start external clients")

    def _get_exchange_name(self) -> OrderExchange:
        """Return the actual execution venue rather than a queued frame assertion."""
        return self.actual_exchange


@pytest.fixture
async def executor_material_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose a private SQLite schema for actual history checks."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'executor-material.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def _client() -> ExchangeClientBase:
    """Return a transport double that records every attempted venue operation."""
    client = MagicMock(spec=ExchangeClientBase)
    client.create_order = AsyncMock(
        return_value=ExchangeOrderSnapshot(
            id="venue-created",
            client_order_id="healthy",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=100.0,
            status=ExchangeOrderStatusEnum.OPEN,
            filled=0.0,
            remaining=1.0,
            timestamp=0.0,
        )
    )
    client.find_order_by_client_id = AsyncMock()
    return cast(ExchangeClientBase, client)


async def test_late_queued_false_frame_refuses_before_stale_lookup_or_any_venue_status(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical maker-only intent remains protected even when active recovery rebuilt False.

    Given: A late queued frame comes from the active False successor of an original True intent,
    When: The actual executor processes that stale frame twice,
    Then: No CREATE, absence lookup, rejection or SCD2 retry is attempted and history remains exact.
    """
    command_id = await seed_command_history(
        executor_material_repository, "lost-maker", (True, False)
    )
    request = order_request_from_command(
        await active_command(executor_material_repository, "lost-maker")
    )
    assert request.post_only is False
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    publish = AsyncMock(return_value=True)
    stale = AsyncMock(side_effect=AssertionError("Material conflict precedes stale verification"))
    record = AsyncMock()
    monkeypatch.setattr(executor, "_publish_order_status", publish)
    monkeypatch.setattr(executor, "_reject_if_stale", stale)
    monkeypatch.setattr(executor, "_record_venue_event", record)
    await executor._process_order(request)
    await executor._process_order(request)
    cast(AsyncMock, client.create_order).assert_not_awaited()
    cast(AsyncMock, client.find_order_by_client_id).assert_not_awaited()
    publish.assert_not_awaited()
    record.assert_not_awaited()
    stale.assert_not_awaited()
    assert executor.pending_orders == {}
    async with executor_material_repository.session() as session:
        rows = (
            await session.scalars(
                select(TradeCommand)
                .where(TradeCommand.public_id == command_id)
                .order_by(TradeCommand.id)
            )
        ).all()
        assert [row.post_only for row in rows] == [True, False]
        assert len(rows) == 2
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 1


async def test_direct_last_boundary_rechecks_history_without_client_acquisition(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Calling the final CREATE method directly cannot bypass immutable material proof.

    Given: A corrupt historical command bypasses earlier dispatch preflight,
    When: The last CREATE boundary is called directly,
    Then: Its dedicated conflict exception names the actual command UUID before client acquisition.
    """
    command_id = await seed_command_history(
        executor_material_repository, "direct-late", (True, False)
    )
    request = order_request_from_command(
        await active_command(executor_material_repository, "direct-late")
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    acquire = MagicMock(side_effect=AssertionError("No client acquisition for material conflict"))
    monkeypatch.setattr(executor, "_require_exchange_client", acquire)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    with pytest.raises(TradeCommandMaterialChangedError) as refused:
        await executor._execute_live_order(request)
    assert refused.value.command_public_id == command_id
    acquire.assert_not_called()


async def test_final_recheck_failure_uses_specific_nonterminal_path_after_successful_preflight(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A proof failure appearing after preflight cannot become a false venue rejection.

    Given: Initial preflight passes but final CREATE proof becomes unavailable,
    When: The executor reaches the final recheck,
    Then: It sends no order, emits no rejection and performs no generic recovery-status write.
    """
    await seed_command_history(executor_material_repository, "between-checks", (False,))
    request = order_request_from_command(
        await active_command(executor_material_repository, "between-checks")
    )
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    proof = AsyncMock(side_effect=[None, TradeCommandMaterialUnavailableError(None, "proof lost")])
    publish = AsyncMock(return_value=True)
    record = AsyncMock()
    monkeypatch.setattr(executor, "_require_consistent_command_material", proof)
    monkeypatch.setattr(executor, "_is_duplicate_submit", AsyncMock(return_value=False))
    monkeypatch.setattr(executor, "_reject_if_stale", AsyncMock(return_value=False))
    monkeypatch.setattr(executor, "_is_live_trading_interlocked", AsyncMock(return_value=False))
    monkeypatch.setattr(executor, "_publish_order_status", publish)
    monkeypatch.setattr(executor, "_record_venue_event", record)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    monkeypatch.setattr(
        "snapper.messaging.executors.base.is_tradeable", lambda _instrument, _exchange: True
    )
    await executor._process_order(request)
    assert [entry.args[1] for entry in publish.await_args_list] == [OrderEventEnum.SUBMITTED]
    cast(AsyncMock, client.create_order).assert_not_awaited()
    record.assert_not_awaited()
    assert executor.pending_orders == {}


async def test_marker_append_failure_never_falls_through_to_create_or_terminal_status(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lost local evidence persistence cannot grant CREATE permission.

    Given: Historical material differs and the durable barrier write fails,
    When: A late frame reaches the executor,
    Then: It is consumed before any status or venue action and no history version is appended.
    """
    command_id = await seed_command_history(
        executor_material_repository, "disk-failure", (True, False)
    )
    request = order_request_from_command(
        await active_command(executor_material_repository, "disk-failure")
    )
    monkeypatch.setattr(
        executor_material_repository,
        "_append_command_material_barrier",
        AsyncMock(side_effect=OSError("disk unavailable")),
    )
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    publish = AsyncMock()
    monkeypatch.setattr(executor, "_publish_order_status", publish)
    await executor._process_order(request)
    cast(AsyncMock, client.create_order).assert_not_awaited()
    cast(AsyncMock, client.find_order_by_client_id).assert_not_awaited()
    publish.assert_not_awaited()
    async with executor_material_repository.session() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(TradeCommand)
                .where(TradeCommand.public_id == command_id)
            )
            == 2
        )
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 0


async def test_distinct_new_false_intent_reaches_existing_create_transport(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A blocked logical UUID does not ban an independently authored false intent.

    Given: An old command is quarantined and a separate new UUID requests ordinary execution,
    When: The new command reaches final CREATE with matching material,
    Then: Its existing client transport runs once with post_only=False.
    """
    old = await seed_command_history(executor_material_repository, "old-conflict", (True, False))
    with pytest.raises(TradeCommandMaterialChangedError):
        await executor_material_repository.require_consistent_trade_command_material(old)
    new = await seed_command_history(executor_material_repository, "healthy", (False,))
    request = order_request_from_command(
        await active_command(executor_material_repository, "healthy")
    )
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    assert await executor._execute_live_order(request) == "venue-created"
    send = cast(AsyncMock, client.create_order)
    send.assert_awaited_once()
    assert send.await_args is not None
    assert send.await_args.args[0].post_only is False
    assert old != new


async def test_actual_paper_create_requires_matching_durable_material(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paper execution proves durable intent before its simulated balance can change.

    Given: A Paper command whose isolated mode and material match the actual executor,
    When: Its final CREATE boundary is called,
    Then: The same durable proof runs before the existing simulator transport.
    """
    command_id = await seed_command_history(
        executor_material_repository, "paper-source", (False,), exchange="paper"
    )
    request = order_request_from_command(
        await active_command(executor_material_repository, "paper-source", "paper")
    )
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    executor.actual_exchange = "paper"
    proof = AsyncMock(wraps=executor_material_repository.require_consistent_trade_command_material)
    monkeypatch.setattr(
        executor_material_repository, "require_consistent_trade_command_material", proof
    )
    assert await executor._execute_live_order(request) == "venue-created"
    proof.assert_awaited_once_with(command_id)


async def test_actual_paper_corrupted_material_is_not_a_proof_bypass(
    executor_material_repository: SQLAlchemyRepository,
) -> None:
    """A simulator cannot replay changed request material into its cash bookkeeping.

    Given: Historical Paper intent changed its maker-only modifier,
    When: The actual Paper final CREATE boundary is called,
    Then: Its exact logical command is quarantined and no simulated order is sent.
    """
    command_id = await seed_command_history(
        executor_material_repository, "paper-conflict", (True, False), exchange="paper"
    )
    request = order_request_from_command(
        await active_command(executor_material_repository, "paper-conflict", "paper")
    )
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    executor.actual_exchange = "paper"
    with pytest.raises(TradeCommandMaterialChangedError) as refused:
        await executor._execute_live_order(request)
    assert refused.value.command_public_id == command_id
    cast(AsyncMock, client.create_order).assert_not_awaited()


async def test_corrupted_command_does_not_block_existing_order_cancellation(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancel remains a venue repair action even when CREATE material is unavailable.

    Given: A quarantined command and an existing venue order,
    When: The real executor cancellation method receives that order identity,
    Then: Cancellation reaches the injected client without consulting CREATE material.
    """
    await seed_command_history(executor_material_repository, "cancel-existing", (True, False))
    client = _client()
    cancel = AsyncMock(
        return_value=ExchangeOrderSnapshot(
            id="existing-venue-order",
            client_order_id="cancel-existing",
            symbol="BTC-USD",
            side=OrderSideEnum.BUY,
            type=ExchangeOrderTypeEnum.LIMIT,
            amount=1.0,
            price=100.0,
            status=ExchangeOrderStatusEnum.CANCELED,
            filled=0.0,
            remaining=1.0,
            timestamp=0.0,
        )
    )
    monkeypatch.setattr(client, "cancel_order", cancel)
    executor = MaterialExecutor(executor_material_repository, client)
    proof = AsyncMock(side_effect=AssertionError("Cancel cannot use CREATE proof"))
    monkeypatch.setattr(executor, "_require_consistent_command_material", proof)
    monkeypatch.setattr(executor, "_publish_cancel_event", AsyncMock())
    await executor._process_cancel(
        OrderCancelData(
            public_id="cancel-material-command",
            timestamp=datetime.now(UTC),
            session_id="material-cancel",
            sequence_id=1,
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="existing-venue-order",
            client_order_id="cancel-existing",
            wallet_public_id=MATERIAL_WALLET,
        )
    )
    cast(AsyncMock, client.cancel_order).assert_awaited_once_with("existing-venue-order", "BTC-USD")
    proof.assert_not_awaited()


@pytest.mark.parametrize("missing", [True, False])
async def test_missing_or_unreadable_durable_command_cannot_reach_create(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    missing: bool,
) -> None:
    """Absence and read failure are explicit proof failures at a real durable executor boundary.

    Given: A queued frame cannot resolve its actual durable create command,
    When: The final executor proof runs,
    Then: It raises unavailable without calling the client or pretending the order was rejected.
    """
    await seed_command_history(executor_material_repository, "unresolved-proof", (False,))
    request = order_request_from_command(
        await active_command(executor_material_repository, "unresolved-proof")
    )
    getter = (
        AsyncMock(return_value=None)
        if missing
        else AsyncMock(side_effect=OSError("database unavailable"))
    )
    monkeypatch.setattr(
        executor_material_repository, "get_active_create_command_by_client_order_id", getter
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(request)
    assert refused.value.command_public_id is None


@pytest.mark.parametrize(
    ("command_change", "request_change", "actual_wallet"),
    [
        ({}, {}, ""),
        ({"wallet_public_id": "another-wallet"}, {}, MATERIAL_WALLET),
        ({}, {"wallet_public_id": "another-wallet"}, MATERIAL_WALLET),
        ({"exchange": "walutomat"}, {}, MATERIAL_WALLET),
        ({}, {"exchange": "walutomat"}, MATERIAL_WALLET),
        ({"mode": "paper"}, {}, MATERIAL_WALLET),
        ({}, {"mode": "paper"}, MATERIAL_WALLET),
    ],
)
async def test_actual_execution_scope_must_match_durable_intent_and_queued_frame(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    command_change: dict[str, object],
    request_change: dict[str, object],
    actual_wallet: str,
) -> None:
    """The stored logical UUID does not authorize a different account, venue or mode.

    Given: Durable identity, queued identity or actual executor ownership disagrees,
    When: The CREATE proof is required,
    Then: It refuses that exact logical command before material reinterpretation or a venue call.
    """
    command_id = await seed_command_history(
        executor_material_repository, "scope-mismatch", (False,)
    )
    command = await active_command(executor_material_repository, "scope-mismatch")
    request = order_request_from_command(command).model_copy(update=request_change)
    returned = {**command, **command_change}
    monkeypatch.setattr(
        executor_material_repository,
        "get_active_create_command_by_client_order_id",
        AsyncMock(return_value=returned),
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    executor.wallet_public_id = actual_wallet
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(request)
    assert refused.value.command_public_id == command_id
    assert refused.value.reason == "execution_scope_mismatch"


async def test_mismatching_queued_modifier_does_not_quarantine_the_healthy_original_intent(
    executor_material_repository: SQLAlchemyRepository,
) -> None:
    """A malformed frame cannot rewrite or permanently quarantine a separately proven intent.

    Given: Stored False material is consistent but one queued frame asserts True,
    When: The executor checks that frame,
    Then: The frame is refused while the valid durable intent retains its original permission.
    """
    command_id = await seed_command_history(
        executor_material_repository, "frame-mismatch", (False,)
    )
    request = order_request_from_command(
        await active_command(executor_material_repository, "frame-mismatch")
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(
            request.model_copy(update={"post_only": True})
        )
    assert refused.value.command_public_id == command_id
    assert "queued_request_material_mismatch" in refused.value.reason
    assert (await executor_material_repository.get_undispatched_commands(datetime.now(UTC)))[0][
        "public_id"
    ] == command_id
    async with executor_material_repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 0


async def test_invalid_recovered_vocabulary_preserves_known_command_identity_in_unavailable_error(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed reconstruction cannot become a generic ValueError followed by a fresh CREATE.

    Given: A proof boundary returns an unsupported stored request vocabulary,
    When: The queued request is checked,
    Then: The dedicated unavailable error retains its actual logical UUID.
    """
    command_id = await seed_command_history(
        executor_material_repository, "bad-vocabulary", (False,)
    )
    command = await active_command(executor_material_repository, "bad-vocabulary")
    request = order_request_from_command(command)
    changed = {**command, "side": "unrecognized-side"}
    monkeypatch.setattr(
        executor_material_repository,
        "require_consistent_trade_command_material",
        AsyncMock(return_value=changed),
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(request)
    assert refused.value.command_public_id == command_id
    assert refused.value.reason == "durable_request_proof_unavailable"


async def test_non_sql_repository_contract_requires_explicit_matching_material(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The proof contract applies to every backend without a type-based permission shortcut.

    Given: A Repository double explicitly returns the actual matching durable command,
    When: Its executor performs a final CREATE check,
    Then: Both lineage and material assertions run before the existing transport.
    """
    command_id = await seed_command_history(executor_material_repository, "contract", (False,))
    command = await active_command(executor_material_repository, "contract")
    request = order_request_from_command(command)
    repository = MagicMock(spec=Repository)
    repository.get_active_create_command_by_client_order_id = AsyncMock(return_value=command)
    repository.require_consistent_trade_command_material = AsyncMock(return_value=command)
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    executor.repository = repository
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    assert await executor._execute_live_order(request) == "venue-created"
    repository.get_active_create_command_by_client_order_id.assert_awaited_once_with(
        "contract", "kraken"
    )
    repository.require_consistent_trade_command_material.assert_awaited_once_with(command_id)


async def test_missing_repository_is_explicitly_unavailable_before_create(
    executor_material_repository: SQLAlchemyRepository,
) -> None:
    """An uninitialized dependency is a hold rather than permission or venue rejection.

    Given: A queued command reaches an executor without its repository dependency,
    When: The durable material assertion runs,
    Then: It raises dedicated unavailable and retains no invented logical identity.
    """
    await seed_command_history(executor_material_repository, "no-repository", (False,))
    request = order_request_from_command(
        await active_command(executor_material_repository, "no-repository")
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    executor.repository = None
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(request)
    assert refused.value.command_public_id is None
    assert refused.value.reason == "durable_execution_repository_unavailable"


async def test_material_conflict_keeps_venue_absence_from_becoming_a_false_rejection(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Venue reconciliation can observe absence without rewriting a conflicted intent as rejected.

    Given: A historically mutated durable command and a venue lookup returning no snapshot,
    When: The actual dispatched-command verification runs twice,
    Then: It records the local barrier and never calls the absence terminalization handler.
    """
    command_id = await seed_command_history(
        executor_material_repository, "held-absence", (True, False)
    )
    command = await active_command(executor_material_repository, "held-absence")
    client = _client()
    cast(AsyncMock, client.find_order_by_client_id).return_value = None
    executor = MaterialExecutor(executor_material_repository, client)
    reject_absence = AsyncMock(side_effect=AssertionError("Conflict cannot infer venue rejection"))
    monkeypatch.setattr(executor, "_handle_absent_dispatched_command", reject_absence)
    await executor._verify_one_dispatched_command(command)
    await executor._verify_one_dispatched_command(command)
    assert cast(AsyncMock, client.find_order_by_client_id).await_count == 2
    cast(AsyncMock, client.create_order).assert_not_awaited()
    reject_absence.assert_not_awaited()
    async with executor_material_repository.session() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(TradeCommand)
                .where(TradeCommand.public_id == command_id)
            )
            == 2
        )


async def test_material_barrier_does_not_suppress_positive_venue_snapshot_adoption(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Actual venue facts stay observable even when local request history cannot authorize CREATE.

    Given: A conflicted command whose venue order is found,
    When: Dispatched-command reconciliation observes the real order,
    Then: The normal adoption path receives the snapshot without using CREATE permission.
    """
    await seed_command_history(executor_material_repository, "found-real-order", (True, False))
    command = await active_command(executor_material_repository, "found-real-order")
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    snapshot = ExchangeOrderSnapshot(
        id="actual-found-order",
        client_order_id="found-real-order",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=100.0,
        status=ExchangeOrderStatusEnum.OPEN,
        filled=0.0,
        remaining=1.0,
        timestamp=0.0,
    )
    cast(AsyncMock, client.find_order_by_client_id).return_value = snapshot
    adopt = AsyncMock()
    proof = AsyncMock(
        side_effect=AssertionError("Positive venue truth cannot require CREATE proof")
    )
    monkeypatch.setattr(executor, "_adopt_dispatched_snapshot", adopt)
    monkeypatch.setattr(executor, "_require_consistent_command_material", proof)
    await executor._verify_one_dispatched_command(command)
    adopt.assert_awaited_once()
    proof.assert_not_awaited()
    cast(AsyncMock, client.create_order).assert_not_awaited()


async def test_parked_ambiguous_order_retains_tracking_when_material_changes_before_absence(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A parked request cannot infer rejection after its durable intent changes.

    Given: A proven maker-only request is parked before a false successor is persisted,
    When: Ambiguous verification observes two consecutive authoritative absences,
    Then: It preserves the parked entry and records only the local material barrier.
    """
    command_id = await seed_command_history(
        executor_material_repository, "parked-before-change", (True,)
    )
    command = await active_command(executor_material_repository, "parked-before-change")
    request = order_request_from_command(command)
    client = _client()
    lookup = cast(AsyncMock, client.find_order_by_client_id)
    lookup.return_value = None
    executor = MaterialExecutor(executor_material_repository, client)
    await executor._require_consistent_command_material(request)
    pending = PendingOrderState(request=request, submit_ambiguous=True)
    executor.pending_orders[request.client_order_id] = pending
    changed_at = command["timestamp"] + timedelta(seconds=1)
    async with executor_material_repository.session() as session:
        await session.execute(
            update(TradeCommand)
            .where(TradeCommand.public_id == command_id, TradeCommand.known_to == KNOWN_TO_MAX)
            .values(known_to=changed_at)
        )
        session.add(
            TradeCommand(
                public_id=command_id,
                command_type="create",
                shard_key=command["shard_key"],
                wallet_public_id=MATERIAL_WALLET,
                exchange="kraken",
                instrument="BTC-USD",
                mode="live",
                strategy_id="material-regression",
                client_order_id="parked-before-change",
                venue_client_id="parked-before-change",
                side="buy",
                order_type="limit",
                quantity=1.0,
                price=100.0,
                post_only=False,
                reduce_only=False,
                status="dispatched",
                created_at=command["created_at"],
                correlation_id=command_id,
                session_id="material-test",
                sequence_id=2,
                timestamp=changed_at,
            )
        )
        await session.commit()
    publish = AsyncMock(return_value=True)
    record = AsyncMock()
    monkeypatch.setattr(executor, "_publish_order_status", publish)
    monkeypatch.setattr(executor, "_record_venue_event", record)
    monkeypatch.setattr(
        "snapper.messaging.executors.base._AMBIGUOUS_VERIFY_DELAYS_RESPONSE_LOST_S",
        (0.0, 0.0, 0.0),
    )
    assert await executor._verify_ambiguous_submit(request, pending, None) is False
    assert lookup.await_count == 2
    assert executor.pending_orders[request.client_order_id] is pending
    assert pending.submit_ambiguous is True
    publish.assert_not_awaited()
    record.assert_not_awaited()
    cast(AsyncMock, client.create_order).assert_not_awaited()
    async with executor_material_repository.session() as session:
        versions = (
            await session.scalars(
                select(TradeCommand)
                .where(TradeCommand.public_id == command_id)
                .order_by(TradeCommand.id)
            )
        ).all()
        assert [version.post_only for version in versions] == [True, False]
        assert list(await session.scalars(select(VenueEvent.event_type))) == [
            COMMAND_MATERIAL_CONFLICT_EVENT
        ]


async def test_paper_queued_strategy_identity_must_match_persisted_shard_material(
    executor_material_repository: SQLAlchemyRepository,
) -> None:
    """Paper strategy partitioning is execution material because it selects simulated bookkeeping.

    Given: A healthy Paper command and a queued frame asserting another strategy tag,
    When: Its final durable proof runs,
    Then: The frame is refused without creating a simulator order or corrupting stored history.
    """
    command_id = await seed_command_history(
        executor_material_repository, "paper-tag", (False,), exchange="paper"
    )
    command = await active_command(executor_material_repository, "paper-tag", "paper")
    request = order_request_from_command(command).model_copy(update={"strategy_tag": "another-tag"})
    executor = MaterialExecutor(executor_material_repository, _client())
    executor.actual_exchange = "paper"
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(request)
    assert refused.value.command_public_id == command_id
    assert refused.value.reason == "queued_request_material_mismatch: strategy_tag"


async def test_material_proof_cannot_substitute_another_logical_command_uuid(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical request fields cannot make another logical command's assertion authoritative.

    Given: Lineage resolves an actual UUID but its proof boundary returns a different one,
    When: The executor requires durable material,
    Then: It refuses with the original known UUID instead of authorizing a substituted command.
    """
    command_id = await seed_command_history(
        executor_material_repository, "wrong-proof-uuid", (False,)
    )
    command = await active_command(executor_material_repository, "wrong-proof-uuid")
    request = order_request_from_command(command)
    changed = {**command, "public_id": "00000000-0000-7000-8000-000000000212"}
    monkeypatch.setattr(
        executor_material_repository,
        "require_consistent_trade_command_material",
        AsyncMock(return_value=changed),
    )
    executor = MaterialExecutor(executor_material_repository, _client())
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._require_consistent_command_material(request)
    assert refused.value.command_public_id == command_id
    assert refused.value.reason == "material_proof_command_identity_mismatch"


@pytest.mark.parametrize("actual_exchange", ["kraken", "paper"])
async def test_wrong_execution_mode_holds_process_and_final_create_without_a_venue_claim(
    executor_material_repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    actual_exchange: OrderExchange,
) -> None:
    """An asserted mode cannot turn a real executor into Paper or a Paper executor into live.

    Given: A valid durable command and a queued frame whose mode contradicts the actual venue,
    When: Both the processing path and direct final CREATE boundary receive that frame,
    Then: No SDK call, rejection, venue event or command revision is fabricated.
    """
    command_id = await seed_command_history(
        executor_material_repository, "wrong-execution-mode", (False,), exchange=actual_exchange
    )
    command = await active_command(
        executor_material_repository, "wrong-execution-mode", actual_exchange
    )
    wrong_mode = "live" if actual_exchange == "paper" else "paper"
    request = order_request_from_command(command).model_copy(update={"mode": wrong_mode})
    client = _client()
    executor = MaterialExecutor(executor_material_repository, client)
    executor.actual_exchange = actual_exchange
    publish = AsyncMock()
    record = AsyncMock()
    monkeypatch.setattr(executor, "_publish_order_status", publish)
    monkeypatch.setattr(executor, "_record_venue_event", record)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    await executor._process_order(request)
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await executor._execute_live_order(request)
    assert refused.value.command_public_id == command_id
    assert refused.value.reason == "execution_scope_mismatch"
    cast(AsyncMock, client.create_order).assert_not_awaited()
    cast(AsyncMock, client.find_order_by_client_id).assert_not_awaited()
    publish.assert_not_awaited()
    record.assert_not_awaited()
    async with executor_material_repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 1
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 0
