"""Exercise actual explicit-scope journal lifetimes on signed sources and a real local bus."""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal
from typing import cast
from uuid import uuid7

import pytest
import zmq
import zmq.asyncio
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text

import snapper.application.trade.financial_publication_lifecycle as lifecycle_module
from snapper.api.schemas.base import StrictDataSchema
from snapper.application.portfolio.versioned_contract_account_projection import (
    VersionedContractAccountProjection,
)
from snapper.application.trade.financial_publication_lifecycle import FinancialPublicationLifecycle
from snapper.application.trade.financial_publication_lifecycle import (
    FinancialPublicationLifecycleError,
)
from snapper.application.trade.financial_publication_lifecycle import (
    FinancialPublicationLifecycleOptions,
)
from snapper.application.trade.financial_publication_lifecycle import (
    FinancialPublicationLifecycleStatus,
)
from snapper.application.trade.versioned_financial_publication import VersionedFinancialPublish
from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_triggers import install_contract_accounting_table_triggers
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.models import Base
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.versioned_financial_publication_delivery import (
    record_versioned_financial_publication_delivery,
)
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from snapper.messaging.schemas.messages import parse_message
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_cash
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3

pytestmark = pytest.mark.asyncio
projection_scenario = shared_projection_scenario


@dataclass(frozen=True, slots=True)
class _ActualPublicationBus:
    """Own a real TCP XPUB and independent tap while the lifecycle owns its separate SUB context."""

    publisher: MessagePublisher
    endpoint: str
    tap: ValidatedSubscriber
    subscriptions: asyncio.Task[None]
    sends: list[str]


@asynccontextmanager
async def _publication_bus(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_ActualPublicationBus]:
    """Gate the original publisher only on actual XPUB subscription observations.

    Args:
        monkeypatch: Owner of an observational wrapper that delegates to the actual original send.

    Yields:
        Real original typed publisher and tap, with an actual two-prefix subscription barrier.
    """
    context = zmq.asyncio.Context()
    xpub = context.socket(zmq.XPUB)
    xpub.setsockopt(zmq.XPUB_VERBOSE, 1)
    port = xpub.bind_to_random_port("tcp://127.0.0.1")
    endpoint = f"tcp://127.0.0.1:{port}"
    raw_tap = context.socket(zmq.SUB)
    raw_tap.connect(endpoint)
    tap = ValidatedSubscriber(raw_tap)
    tap.subscribe("accounting.")
    assert await xpub.recv() == b"\x01accounting."
    publisher = MessagePublisher(ValidatedPublisher(xpub), SequenceTracker())
    sends: list[str] = []
    original_send = publisher.send

    async def observe_owner_subscriptions() -> None:
        """Observe the actual original owner subscription frames before permitting its first send."""
        frames: set[bytes] = set()
        while len(frames) < 2:
            frame = await xpub.recv()
            assert frame.startswith(b"\x01accounting.")
            frames.add(frame)

    async def after_subscription(
        topic: str, message: StrictDataSchema[str], *, flags: int = 0
    ) -> None:
        """Preserve the original actual send after the real transport acknowledgement barrier."""
        await asyncio.shield(observer)
        await original_send(topic, message, flags=flags)
        sends.append(topic)

    observer = asyncio.create_task(observe_owner_subscriptions())
    try:
        with monkeypatch.context() as patch:
            patch.setattr(publisher, "send", after_subscription)
            yield _ActualPublicationBus(publisher, endpoint, tap, observer, sends)
    finally:
        observer.cancel()
        await asyncio.gather(observer, return_exceptions=True)
        tap.close()
        publisher.close()
        context.destroy(linger=0)


def _owner(
    repository: AccountingRepository, scope: ExecutionAccountScope, bus: _ActualPublicationBus
) -> FinancialPublicationLifecycle:
    """Use an explicit original source scope and a long cadence controlled by real refresh requests.

    Args:
        repository: Actual source-backed fixture repository.
        scope: Original source-bound explicit financial book, never inferred from an absent intent.
        bus: Real internal bus whose original publisher remains borrowed.

    Returns:
        The actual lifecycle under test with no fake projection or publisher implementation.
    """
    return FinancialPublicationLifecycle(
        repository,
        scope,
        bus.publisher,
        FinancialPublicationLifecycleOptions(bus.endpoint, timedelta(hours=1)),
    )


async def _count(repository: AccountingRepository, model: type[Base]) -> int:
    """Count actual committed rows from an independent database session.

    Args:
        repository: Actual isolated financial repository.
        model: Existing real financial relation selected by this test.

    Returns:
        The actual persisted row count.
    """
    async with repository.session() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


def _observe_receive(monkeypatch: pytest.MonkeyPatch) -> tuple[asyncio.Event, list[str]]:
    """Observe complete real source-bound projection receives without replacing their implementation.

    Args:
        monkeypatch: Actual fixture lifetime for the delegating observation wrapper.

    Returns:
        A real-completion barrier and original publication identities that were actually consumed.
    """
    original = VersionedContractAccountProjection.receive_message
    received = asyncio.Event()
    identities: list[str] = []

    async def observe(
        projection: VersionedContractAccountProjection,
        topic: str,
        message: FinancialPublicationData,
    ) -> None:
        """Delegate the complete original graph/SQL receive before reporting actual completion."""
        await original(projection, topic, message)
        identities.append(message.public_id)
        received.set()

    monkeypatch.setattr(VersionedContractAccountProjection, "receive_message", observe)
    return received, identities


async def _send_intent(bus: _ActualPublicationBus, intent: BoundFinancialPublication) -> None:
    """Send an actual retained legacy intent using the real original borrowed publisher.

    Args:
        bus: Actual ready original local publisher.
        intent: Genuine committed financial publication from its full source fixture.
    """
    message = FinancialPublicationData(
        public_id=intent.public_id,
        wallet_public_id=intent.payload.scope.wallet_public_id,
        session_id=bus.publisher.session_id,
        sequence_id=bus.publisher.tracker.next_sequence(intent.topic),
        timestamp=datetime.now(UTC),
        payload=intent.payload,
    )
    await bus.publisher.send(intent.topic, message)


async def test_actual_legacy_start_receive_and_stop_preserve_the_borrowed_publisher(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise the complete dedicated lifecycle with a real persisted legacy publication.

    Given: Actual original financial sources commit one position and one recoverable publication.
    When: The owner hydrates, drains and receives on the real local bus, then stops and joins.
    Then: Its view matches retained facts, one receipt persists, and the original publisher still sends.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    received, identities = _observe_receive(monkeypatch)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await received.wait()
            assert identities == [intent.public_id]
            snapshot = owner.snapshot
            assert snapshot is not None and snapshot.readiness == "held"
            assert (
                snapshot.positions[0].latest_event_public_id
                == intent.payload.position_event_public_id
            )
            assert await _count(repository, FinancialPublicationDelivery) == 1
            original_topic, original_wire = await bus.tap.recv_multipart()
            assert parse_message(original_wire.decode(), topic=original_topic).sequence_id == 1
            await owner.stop()
            assert (
                owner.status is FinancialPublicationLifecycleStatus.CLOSED
                and owner.snapshot is None
            )
            await _send_intent(bus, intent)
            topic, wire = await bus.tap.recv_multipart()
            parsed = parse_message(wire.decode(), topic=topic)
            assert isinstance(parsed, FinancialPublicationData) and parsed.sequence_id == 2
            assert owner.last_snapshot is not None
        finally:
            await owner.stop()


async def test_actual_native_v3_publication_is_consumed_without_legacy_execution_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve actual admitted native source references through the dedicated new owner.

    Given: The genuine signed HTTP and authenticated SDK fixture creates an admitted V3 position.
    When: Its original intent is drained and consumed by the real lifecycle and full projection.
    Then: The exact native position hydrates and its unchanged journal is not booked a second time.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        intent = (await record_native_position_scenario_v3(scenario)).publication
        received, identities = _observe_receive(monkeypatch)
        async with _publication_bus(monkeypatch) as bus:
            owner = _owner(scenario.source.repository, intent.payload.scope, bus)
            try:
                await owner.start()
                await received.wait()
                snapshot = owner.snapshot
                assert (
                    snapshot is not None
                    and snapshot.positions[0].latest_event_public_id
                    == intent.payload.position_event_public_id
                )
                assert identities == [intent.public_id]
                assert await _count(scenario.source.repository, ContractAccountingEvent) == 1
                assert await _count(scenario.source.repository, FinancialPublicationDelivery) == 1
            finally:
                await owner.stop()


async def test_explicit_cash_only_book_hydrates_without_any_publication_or_delivery_marker(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read actual retained cash instead of inferring book contents from the publication queue.

    Given: A real source-bound cash event exists in an explicit scope with no position notification.
    When: The dedicated lifecycle starts with that original typed scope.
    Then: It hydrates the actual native currency journal although no intent or receipt exists.
    """
    await record_projection_cash(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = projection_scenario.fill.admission.authority.scope
    assert await _count(repository, FinancialPublicationIntent) == 0
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, scope, bus)
        try:
            await owner.start()
            await bus.subscriptions
            snapshot = owner.snapshot
            assert snapshot is not None and snapshot.positions == ()
            assert snapshot.native_cash[0].balance == Decimal("99.75")
            assert snapshot.inventory_status == "unproven" and snapshot.readiness == "held"
            assert bus.sends == [] and await _count(repository, FinancialPublicationDelivery) == 0
        finally:
            await owner.stop()


async def test_restart_hydrates_a_book_whose_original_publication_was_already_delivered(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep restart hydration independent from pending intent discovery.

    Given: A real position's original local-return receipt already exists before startup.
    When: A new actual owner starts for that explicit original scope.
    Then: It reads the retained position with no new publisher send or receipt replacement.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    now = datetime.now(UTC)
    tracker = SequenceTracker()
    await record_versioned_financial_publication_delivery(
        repository,
        intent.public_id,
        now,
        SnapshotWriteProvenance(
            public_id=str(uuid7()), session_id=tracker.session_id, sequence_id=1, timestamp=now
        ),
    )
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await bus.subscriptions
            snapshot = owner.snapshot
            assert (
                snapshot is not None
                and snapshot.positions[0].latest_event_public_id
                == intent.payload.position_event_public_id
            )
            assert bus.sends == [] and await _count(repository, FinancialPublicationDelivery) == 1
        finally:
            await owner.stop()


async def test_requested_refresh_reads_new_cash_without_a_bus_notification(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repair missed or nonexistent notifications using a complete real journal read.

    Given: The real owner hydrated a retained position before a separate cash event committed.
    When: Its periodic worker is woken and invokes the original full journal refresh.
    Then: The current view includes actual cash despite the absence of any new position publication.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    received, _identities = _observe_receive(monkeypatch)
    refreshed = asyncio.Event()
    original_refresh = VersionedContractAccountProjection.refresh
    count = 0

    async def observe_refresh(
        projection: VersionedContractAccountProjection,
    ) -> ContractAccountSnapshot:
        """Observe original actual journal completion and preserve its original returned snapshot."""
        nonlocal count
        snapshot = await original_refresh(projection)
        count += 1
        if count > 1:
            refreshed.set()
        return snapshot

    monkeypatch.setattr(VersionedContractAccountProjection, "refresh", observe_refresh)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await received.wait()
            before = owner.snapshot
            assert before is not None and before.native_cash == ()
            await record_projection_cash(projection_scenario)
            owner.request_refresh()
            await refreshed.wait()
            after = owner.snapshot
            assert after is not None and after.native_cash[0].balance == Decimal("99.75")
            assert after.positions == before.positions and after.as_of >= before.as_of
            assert await _count(repository, FinancialPublicationIntent) == 1
        finally:
            await owner.stop()


async def _restore_event_digest(repository: SQLAlchemyRepository, digest: str) -> None:
    """Mutate a real isolated journal row through controlled restore and reinstate actual guards.

    Args:
        repository: Actual isolated source-backed SQLite repository with one event.
        digest: Original or deliberately damaged exact digest string.
    """
    async with repository.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER contract_accounting_events_reject_update"))
        await connection.execute(
            text("UPDATE contract_accounting_events SET content_digest = :digest"),
            {"digest": digest},
        )
        await connection.run_sync(
            lambda actual: install_contract_accounting_table_triggers(
                actual, "contract_accounting_events"
            )
        )


async def test_actual_bad_journal_refresh_stays_stale_until_a_genuine_successful_read(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve the old read clock after a genuine failed source rebind.

    Given: An actual owner has hydrated a real position before its isolated stored digest is damaged.
    When: Full refresh fails, the real row is repaired, and another full refresh eventually succeeds.
    Then: Current view remains absent until success, while the old view retains its original as_of.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    received, _identities = _observe_receive(monkeypatch)
    failed = asyncio.Event()
    succeeded = asyncio.Event()
    original_refresh = VersionedContractAccountProjection.refresh

    async def observe_refresh(
        projection: VersionedContractAccountProjection,
    ) -> ContractAccountSnapshot:
        """Preserve actual database failures and successful full reads while recording their barriers."""
        try:
            snapshot = await original_refresh(projection)
        except Exception:
            failed.set()
            raise
        if failed.is_set():
            succeeded.set()
        return snapshot

    monkeypatch.setattr(VersionedContractAccountProjection, "refresh", observe_refresh)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await received.wait()
            before = owner.snapshot
            assert before is not None
            async with repository.session() as session:
                digest = (
                    await session.scalars(select(ContractAccountingEvent.content_digest))
                ).one()
            await _restore_event_digest(repository, "0" * 64)
            owner.request_refresh()
            await failed.wait()
            assert (
                owner.status is FinancialPublicationLifecycleStatus.STALE and owner.snapshot is None
            )
            assert owner.last_snapshot is not None and owner.last_snapshot.as_of == before.as_of
            await _restore_event_digest(repository, digest)
            assert owner.snapshot is None and owner.last_snapshot == before
            owner.request_refresh()
            await succeeded.wait()
            assert owner.status is FinancialPublicationLifecycleStatus.ACTIVE
            assert owner.snapshot is not None and owner.snapshot.positions == before.positions
        finally:
            await owner.stop()


@dataclass(slots=True)
class _ShutdownObservation:
    """Track actual callback barriers, original cancellation and worker completion at socket close."""

    worker: Literal["receive", "refresh"]
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    received: asyncio.Event = field(default_factory=asyncio.Event)
    finished: asyncio.Event = field(default_factory=asyncio.Event)
    closes: list[ValidatedSubscriber] = field(default_factory=list)
    joined_at_close: list[bool] = field(default_factory=list)
    worker_tasks: tuple[asyncio.Task[None], ...] = ()
    callback_cancellations: list[asyncio.CancelledError] = field(default_factory=list)

    async def hold_completed_callback(self) -> None:
        """Keep actual completed SQL work owned until release after its original cancellation."""
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError as error:
            self.callback_cancellations.append(error)
            self.cancelled.set()
            await self.release.wait()
        finally:
            self.finished.set()


def _observe_shutdown_callbacks(
    monkeypatch: pytest.MonkeyPatch, observation: _ShutdownObservation
) -> Callable[[ValidatedSubscriber], None]:
    """Wrap actual SQL callbacks and return an observer that delegates original socket disposal.

    Args:
        monkeypatch: Lifetime owner of wrappers around actual projection methods.
        observation: Original barriers and actual worker identities from the test's lifecycle.

    Returns:
        The close observer installed only inside the actual receive-resource lifetime.
    """
    original_receive = VersionedContractAccountProjection.receive_message
    original_refresh = VersionedContractAccountProjection.refresh
    original_close = ValidatedSubscriber.close

    async def block_after_actual_receive(
        projection: VersionedContractAccountProjection,
        topic: str,
        message: FinancialPublicationData,
    ) -> None:
        """Delegate complete graph hydration before gating the real receive worker.

        Args:
            projection: Actual original projection owned by the running lifecycle.
            topic: Original separately received ZMQ topic.
            message: Actual original publication envelope from the real bus.
        """
        await original_receive(projection, topic, message)
        observation.received.set()
        if observation.worker == "receive":
            await observation.hold_completed_callback()

    async def block_after_actual_refresh(
        projection: VersionedContractAccountProjection,
    ) -> ContractAccountSnapshot:
        """Delegate the original full journal read before gating only an actual owned worker.

        Args:
            projection: Actual original projection performing its genuine SQL refresh.

        Returns:
            The same actual complete journal snapshot after its callback is released.
        """
        snapshot = await original_refresh(projection)
        if observation.worker == "refresh" and asyncio.current_task() in observation.worker_tasks:
            await observation.hold_completed_callback()
        return snapshot

    def observe_close(subscriber: ValidatedSubscriber) -> None:
        """Preserve actual disposal and record callback and worker completion before it.

        Args:
            subscriber: Actual lifecycle-owned receive socket reaching its original close method.
        """
        observation.closes.append(subscriber)
        observation.joined_at_close.append(
            observation.finished.is_set() and all(task.done() for task in observation.worker_tasks)
        )
        original_close(subscriber)

    monkeypatch.setattr(
        VersionedContractAccountProjection, "receive_message", block_after_actual_receive
    )
    monkeypatch.setattr(VersionedContractAccountProjection, "refresh", block_after_actual_refresh)
    return observe_close


@pytest.mark.parametrize("worker", ["receive", "refresh"])
async def test_shutdown_double_cancellation_never_closes_receive_resources_before_callback_join(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
    worker: Literal["receive", "refresh"],
) -> None:
    """Hold actual completed SQL receive work across cancellation to test real resource ownership.

    Given: An actual receive or refresh callback finished SQL work but deliberately awaits release.
    When: Stop cancels that callback and the same external stop waiter is cancelled twice.
    Then: Cleanup joins both real workers after callback release before closing its owned SUB.

    Args:
        projection_scenario: Actual source-bound isolated financial journal and repository.
        monkeypatch: Lifetime owner of wrappers that preserve original reads and socket disposal.
        worker: Actual owned receive or refresh callback that consumes its first cancellation.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    observation = _ShutdownObservation(worker)
    observe_close = _observe_shutdown_callbacks(monkeypatch, observation)
    async with _publication_bus(monkeypatch) as bus:
        with monkeypatch.context() as patch:
            patch.setattr(ValidatedSubscriber, "close", observe_close)
            owner = _owner(repository, intent.payload.scope, bus)
            try:
                await owner.start()
                observation.worker_tasks = tuple(owner._workers)
                assert len(observation.worker_tasks) == 2
                if worker == "refresh":
                    await observation.received.wait()
                    owner.request_refresh()
                await observation.entered.wait()
                await _cancel_stop_twice(owner, observation.cancelled)
                assert (
                    owner.status is FinancialPublicationLifecycleStatus.UNRESOLVED
                    and observation.closes == []
                )
                assert not observation.finished.is_set() and any(
                    not task.done() for task in observation.worker_tasks
                )
                assert len(observation.callback_cancellations) == 1
                observation.release.set()
                await owner.stop()
                assert (
                    owner.status is FinancialPublicationLifecycleStatus.CLOSED
                    and len(observation.closes) == 1
                )
                assert observation.finished.is_set() and observation.joined_at_close == [True]
            finally:
                observation.release.set()
                await owner.stop()


async def _cancel_stop_twice(
    owner: FinancialPublicationLifecycle, callback_cancelled: asyncio.Event
) -> None:
    """Cancel the same actual external stop waiter twice while cleanup still owns a blocked callback.

    Args:
        owner: Actual live lifecycle whose original callback remains blocked.
        callback_cancelled: Barrier set only by actual cancellation entering that original callback.
    """
    first_cancelled = asyncio.Event()

    async def wait_twice() -> None:
        """Resume the same external caller into a second real shielded stop wait after its first cancellation."""
        try:
            await owner.stop()
        except asyncio.CancelledError:
            first_cancelled.set()
            await owner.stop()

    waiter = asyncio.create_task(wait_twice())
    try:
        await callback_cancelled.wait()
        waiter.cancel()
        await first_cancelled.wait()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


async def test_stop_from_actual_receive_task_refuses_self_join_and_external_stop_still_works(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise actual callback task identity rather than assuming a newly created cleanup task is safe.

    Given: The real source-bound callback runs inside the lifecycle's actual dedicated receive task.
    When: It asks that same owner to stop before returning from the original callback.
    Then: Stop refuses a dependency cycle and an external caller later joins and closes normally.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    completed = asyncio.Event()
    original_receive = VersionedContractAccountProjection.receive_message
    owner: FinancialPublicationLifecycle | None = None

    async def try_self_stop(
        projection: VersionedContractAccountProjection,
        topic: str,
        message: FinancialPublicationData,
    ) -> None:
        """Invoke the actual owner from the actual receive task, then preserve full original hydration."""
        assert owner is not None
        with pytest.raises(FinancialPublicationLifecycleError, match="self_join_refused"):
            await owner.stop()
        await original_receive(projection, topic, message)
        completed.set()

    monkeypatch.setattr(VersionedContractAccountProjection, "receive_message", try_self_stop)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await completed.wait()
            assert owner.status is FinancialPublicationLifecycleStatus.ACTIVE
            await owner.stop()
            await owner.wait()
            assert owner.status is FinancialPublicationLifecycleStatus.CLOSED
        finally:
            await owner.stop()


async def test_untrusted_same_wallet_other_profile_is_filtered_before_actual_projection_receive(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep literal profiles distinct even though their instrument routing prefixes are identical.

    Given: A genuine explicit book is active and a deliberately untrusted message names another profile.
    When: Real ZMQ carries that negative message immediately before the genuine original publication.
    Then: Only the original actual book reaches the full projection; no foreign profile is certified.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    original_receive = VersionedContractAccountProjection.receive_message
    completed = asyncio.Event()
    attempts: list[str] = []

    async def observe_attempt(
        projection: VersionedContractAccountProjection,
        topic: str,
        message: FinancialPublicationData,
    ) -> None:
        """Record actual entry before delegating the original complete source-bound receive."""
        attempts.append(message.public_id)
        await original_receive(projection, topic, message)
        completed.set()

    monkeypatch.setattr(VersionedContractAccountProjection, "receive_message", observe_attempt)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await completed.wait()
            completed.clear()
            wrong_scope = ExecutionAccountScope.model_validate(
                {
                    **intent.payload.scope.model_dump(mode="python"),
                    "margin_account": "untrusted-other-profile",
                }
            )
            wrong_payload = intent.payload.model_copy(update={"scope": wrong_scope})
            wrong_id = SequenceTracker().session_id
            negative = FinancialPublicationData(
                public_id=wrong_id,
                wallet_public_id=wrong_scope.wallet_public_id,
                session_id=bus.publisher.session_id,
                sequence_id=bus.publisher.tracker.next_sequence(intent.topic),
                timestamp=datetime.now(UTC),
                payload=wrong_payload,
            )
            await bus.publisher.send(intent.topic, negative)
            await _send_intent(bus, intent)
            await completed.wait()
            assert attempts == [intent.public_id, intent.public_id] and wrong_id not in attempts
            assert owner.snapshot is not None and owner.snapshot.scope == intent.payload.scope
        finally:
            await owner.stop()


async def test_actual_invalid_socket_protocol_fails_closed_and_joins_startup(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normalize a real local transport setup error without replacing the underlying socket implementation.

    Given: A genuine explicit source scope and an unusable original ZMQ protocol endpoint.
    When: The actual lifecycle attempts its own real socket connection.
    Then: Startup reports a fixed error and closes only its own receive resources after joining startup.
    """
    scope = projection_scenario.fill.admission.authority.scope
    repository = projection_scenario.fill.admission.repository
    async with _publication_bus(monkeypatch) as bus:
        owner = FinancialPublicationLifecycle(
            repository,
            scope,
            bus.publisher,
            FinancialPublicationLifecycleOptions(
                "unsupported-local-protocol://financial-publication"
            ),
        )
        with pytest.raises(FinancialPublicationLifecycleError, match="startup_failed") as caught:
            await owner.start()
        assert caught.value.__context__ is None
        assert owner.status is FinancialPublicationLifecycleStatus.CLOSED and owner.snapshot is None
        assert owner.last_snapshot is None and bus.sends == []
        await owner.stop()


async def test_actual_owner_requires_one_start_and_refuses_refresh_after_closed(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a stopped owner's resource lifetime separate from another fresh instance.

    Given: An actual source-backed explicit book and its newly constructed real lifecycle.
    When: Refresh is requested before start and the unstarted owner is stopped before a restart attempt.
    Then: No wildcard discovery or resource reuse occurs and each invalid lifecycle operation refuses.
    """
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(
            projection_scenario.fill.admission.repository,
            projection_scenario.fill.admission.authority.scope,
            bus,
        )
        with pytest.raises(FinancialPublicationLifecycleError, match="not_running"):
            owner.request_refresh()
        assert owner.snapshot is None and owner.last_snapshot is None
        await owner.stop()
        with pytest.raises(FinancialPublicationLifecycleError, match="start_once"):
            await owner.start()
        with pytest.raises(FinancialPublicationLifecycleError, match="not_running"):
            owner.request_refresh()
        await owner.wait()


async def test_late_actual_startup_return_cannot_spawn_unjoined_workers_after_shutdown(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Wait for a real publication/receipt operation that returns only after consuming cancellation.

    Given: Startup completed the actual source-bound drain and receipt commit before a gated late return.
    When: Shutdown cancels startup but the operation consumes that cancellation and returns after release.
    Then: Startup refuses activation, no late workers survive, and the owned SUB closes only after joining it.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_drain = lifecycle_module.publish_pending_versioned_financial_notifications
    original_close = ValidatedSubscriber.close
    closes: list[ValidatedSubscriber] = []

    async def late_original_return(
        actual_repository: AccountingRepository,
        scope: ExecutionAccountScope,
        publish: VersionedFinancialPublish,
        tracker: SequenceTracker,
    ) -> int:
        """Call the real send/commit implementation before gating its original late operation return."""
        result = await original_drain(actual_repository, scope, publish, tracker)
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        return result

    def observe_close(subscriber: ValidatedSubscriber) -> None:
        """Preserve original disposal and record its actual order relative to startup termination."""
        closes.append(subscriber)
        original_close(subscriber)

    monkeypatch.setattr(
        lifecycle_module, "publish_pending_versioned_financial_notifications", late_original_return
    )
    async with _publication_bus(monkeypatch) as bus:
        with monkeypatch.context() as patch:
            patch.setattr(ValidatedSubscriber, "close", observe_close)
            owner = _owner(repository, intent.payload.scope, bus)
            starting = asyncio.create_task(owner.start())
            stopping: asyncio.Task[None] | None = None
            try:
                await entered.wait()
                stopping = asyncio.create_task(owner.stop())
                await cancelled.wait()
                assert closes == [] and owner.status is FinancialPublicationLifecycleStatus.CLOSING
                release.set()
                await stopping
                with pytest.raises(FinancialPublicationLifecycleError, match="start_closed"):
                    await starting
                assert (
                    owner.status is FinancialPublicationLifecycleStatus.CLOSED and len(closes) == 1
                )
                names = {
                    "financial-publication-startup",
                    "financial-publication-receive",
                    "financial-publication-refresh",
                }
                assert not any(task.get_name() in names for task in asyncio.all_tasks())
                assert await _count(repository, FinancialPublicationDelivery) == 1
            finally:
                release.set()
                await owner.stop()
                tasks = [starting] if stopping is None else [starting, stopping]
                await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("broker", ["", " ", None, True])
async def test_invalid_local_transport_policy_never_creates_a_receive_owner(broker: object) -> None:
    """Keep local endpoint validation strict without treating a configured string as account evidence.

    Given: An absent, blank or incorrectly typed broker endpoint.
    When: Local lifecycle options are constructed.
    Then: They refuse before any real socket, source read or publisher use.
    """
    with pytest.raises(ValueError, match="broker_required"):
        FinancialPublicationLifecycleOptions(cast(str, broker))


@pytest.mark.parametrize("interval", [timedelta(0), timedelta(seconds=-1), None, 1])
async def test_invalid_refresh_cadence_never_claims_current_source_state(interval: object) -> None:
    """Refuse coerced or nonpositive local cadence values before installation.

    Given: A missing, scalar or nonpositive refresh duration.
    When: The explicit local options are constructed.
    Then: The policy refuses without any source clock or financial readiness inference.
    """
    with pytest.raises(ValueError, match="interval_invalid"):
        FinancialPublicationLifecycleOptions("tcp://127.0.0.1:1", cast(timedelta, interval))
