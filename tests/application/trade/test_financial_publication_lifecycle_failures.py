"""Exercise real publication ownership failures without replacing journal or bus behavior."""

import asyncio
import inspect
from collections.abc import Coroutine
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import Context
from datetime import UTC
from datetime import datetime
from typing import Literal
from uuid import uuid7

import pytest
from sqlalchemy import select

import snapper.application.trade.financial_publication_lifecycle as lifecycle_module
from snapper.application.portfolio.versioned_contract_account_projection import (
    VersionedContractAccountProjection,
)
from snapper.application.trade.financial_publication_lifecycle import (
    FinancialPublicationLifecycleError,
)
from snapper.application.trade.financial_publication_lifecycle import (
    FinancialPublicationLifecycleStatus,
)
from snapper.application.trade.versioned_financial_publication import VersionedFinancialPublish
from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.models import ContractAccountingEvent
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from tests.application.trade.test_financial_publication_lifecycle import _observe_receive
from tests.application.trade.test_financial_publication_lifecycle import _owner
from tests.application.trade.test_financial_publication_lifecycle import _publication_bus
from tests.application.trade.test_financial_publication_lifecycle import _restore_event_digest
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario

pytestmark = pytest.mark.asyncio
projection_scenario = shared_projection_scenario


@contextmanager
def _refuse_named_task(
    monkeypatch: pytest.MonkeyPatch, selected: str, failure: RuntimeError
) -> Iterator[list[Coroutine[object, object, object]]]:
    """Reject one actual ownership transfer while delegating every unrelated task allocation.

    Args:
        monkeypatch: Lifetime owner restoring the actual task creator after the boundary refusal.
        selected: Exact lifecycle task name whose original coroutine cannot be scheduled.
        failure: Original task-allocation error that the caller must receive unchanged.

    Yields:
        Actual refused coroutines for independent closed-state inspection.
    """
    original = asyncio.create_task
    refused: list[Coroutine[object, object, object]] = []

    def create[T](
        coroutine: Coroutine[object, object, T],
        *,
        name: str | None = None,
        context: Context | None = None,
        eager_start: bool | None = None,
    ) -> asyncio.Task[T]:
        """Preserve actual task creation except at the explicitly refused lifecycle handoff.

        Args:
            coroutine: Original operation awaiting actual task ownership.
            name: Original task label supplied by its production caller.
            context: Original context retained by the real asyncio creator.
            eager_start: Original eager-start selection retained by the real creator.

        Returns:
            The original task for any operation outside the refused ownership boundary.
        """
        if name == selected:
            refused.append(coroutine)
            raise failure
        return original(coroutine, name=name, context=context, eager_start=eager_start)

    with monkeypatch.context() as patch:
        patch.setattr(asyncio, "create_task", create)
        yield refused


@pytest.mark.parametrize("stage", ["startup", "receive", "refresh"])
async def test_actual_task_allocation_refusal_closes_unowned_coroutine_and_preserves_error(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
    stage: Literal["startup", "receive", "refresh"],
) -> None:
    """Keep rejected startup and worker coroutines out of an unresolved resource lifetime.

    Given: A real committed journal and an actual local bus await lifecycle startup.
    When: The actual task creator rejects one named startup or worker ownership transfer.
    Then: The same error escapes, the refused coroutine closes, and owned resources are joined.

    Args:
        projection_scenario: Actual source-bound isolated journal and repository.
        monkeypatch: Owner of the narrowly scoped task-allocation refusal.
        stage: Actual lifecycle task allocation refused by the runtime boundary.
    """
    intent = await record_projection_position(projection_scenario)
    failure = RuntimeError("isolated_publication_task_allocation_refused")
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        try:
            with (
                _refuse_named_task(
                    monkeypatch, "financial-publication-" + stage, failure
                ) as refused,
                pytest.raises(RuntimeError) as caught,
            ):
                await owner.start()
            assert caught.value is failure and len(refused) == 1
            assert inspect.getcoroutinestate(refused[0]) == inspect.CORO_CLOSED
            if stage == "startup":
                assert owner.status is FinancialPublicationLifecycleStatus.UNRESOLVED
                assert owner.failure_code == "financial_publication_start_task_failed"
                with pytest.raises(FinancialPublicationLifecycleError, match="owner_unresolved"):
                    await owner.wait()
                assert bus.sends == []
            else:
                assert owner.status is FinancialPublicationLifecycleStatus.CLOSED
                assert bus.sends == [intent.topic]
            assert owner._workers == set() and owner._subscriber is None
        finally:
            await owner.stop()


async def test_close_task_refusal_retains_actual_live_resources_until_a_real_retry(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not report resource disposal when no cleanup task accepted ownership.

    Given: The real owner has hydrated and consumed its original publication.
    When: Cleanup task creation fails before the original close coroutine can run.
    Then: The coroutine closes but live resources remain unresolved until a successful retry joins them.

    Args:
        projection_scenario: Actual source-backed journal and local financial repository.
        monkeypatch: Lifetime of the real-completion observer and task-boundary refusal.
    """
    intent = await record_projection_position(projection_scenario)
    received, _ = _observe_receive(monkeypatch)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await received.wait()
            context, subscriber = owner._context, owner._subscriber
            assert context is not None and subscriber is not None
            failure = RuntimeError("isolated_close_task_allocation_refused")
            with (
                _refuse_named_task(monkeypatch, "financial-publication-close", failure) as refused,
                pytest.raises(RuntimeError) as caught,
            ):
                await owner.stop()
            assert caught.value is failure and len(refused) == 1
            assert inspect.getcoroutinestate(refused[0]) == inspect.CORO_CLOSED
            assert owner.status is FinancialPublicationLifecycleStatus.UNRESOLVED
            assert owner.failure_code == "financial_publication_close_task_failed"
            assert owner._subscriber is subscriber and not context.closed
            assert any(not task.done() for task in owner._workers)
        finally:
            await owner.stop()
        assert owner.status is FinancialPublicationLifecycleStatus.CLOSED and context.closed


async def test_actual_socket_close_failure_leaves_joined_cleanup_unresolved(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain a real cleanup failure without claiming the owned socket or context was disposed.

    Given: Actual journal consumption completed and the owner retains its two real workers.
    When: The original socket-disposal boundary refuses after those workers have been joined.
    Then: Stop exposes a fixed failure, retains the original task error and never reports closed.

    Args:
        projection_scenario: Actual source-backed isolated financial graph.
        monkeypatch: Lifetime of an isolated failure at the real socket close boundary.
    """
    intent = await record_projection_position(projection_scenario)
    received, _ = _observe_receive(monkeypatch)
    original_close = ValidatedSubscriber.close
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        await owner.start()
        await received.wait()
        context, subscriber = owner._context, owner._subscriber
        assert context is not None and subscriber is not None
        workers = tuple(owner._workers)
        failure = OSError("isolated_socket_disposal_refused")

        def refuse_close(actual: ValidatedSubscriber) -> None:
            """Refuse only the original owned socket after observing genuine worker termination.

            Args:
                actual: Actual socket arriving at its original disposal boundary.
            """
            if actual is subscriber:
                assert all(task.done() for task in workers)
                raise failure
            original_close(actual)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(ValidatedSubscriber, "close", refuse_close)
                with pytest.raises(
                    FinancialPublicationLifecycleError, match="cleanup_failed"
                ) as caught:
                    await owner.stop()
                assert caught.value.__context__ is None
            assert owner.status is FinancialPublicationLifecycleStatus.UNRESOLVED
            assert owner.failure_code == "financial_publication_cleanup_failed"
            assert owner._cleanup is not None and owner._cleanup.exception() is failure
            assert owner._subscriber is subscriber and not context.closed
            with pytest.raises(FinancialPublicationLifecycleError, match="owner_unresolved"):
                await owner.wait()
        finally:
            original_close(subscriber)
            context.destroy(linger=0)


async def test_original_start_cancellation_joins_actual_startup_before_resource_close(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the original start cancellation while joining the actual unfinished startup callback.

    Given: The real startup drain committed its receipt and awaits an observed return barrier.
    When: The external start waiter is cancelled while the original startup task remains owned.
    Then: Startup is cancelled and joined, resources close, and the original caller cancellation escapes.

    Args:
        projection_scenario: Actual committed financial source graph and repository.
        monkeypatch: Lifetime of a delegating barrier after actual publication and receipt commit.
    """
    intent = await record_projection_position(projection_scenario)
    original = lifecycle_module.publish_pending_versioned_financial_notifications
    entered, release, joined = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def hold_return(
        repository: AccountingRepository,
        scope: ExecutionAccountScope,
        publish: VersionedFinancialPublish,
        tracker: SequenceTracker,
    ) -> int:
        """Retain the real committed result until cancellation joins its original startup operation.

        Args:
            repository: Actual independent financial transaction provider.
            scope: Original complete account scope.
            publish: Original borrowed real publisher callback.
            tracker: Original transport provenance allocator.

        Returns:
            The original result only when the real callback is released without cancellation.
        """
        result = await original(repository, scope, publish, tracker)
        entered.set()
        try:
            await release.wait()
        finally:
            joined.set()
        return result

    monkeypatch.setattr(
        lifecycle_module, "publish_pending_versioned_financial_notifications", hold_return
    )
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        starting = asyncio.create_task(owner.start())
        try:
            await entered.wait()
            startup = owner._startup
            assert startup is not None and not startup.done()
            starting.cancel("original_start_cancellation")
            with pytest.raises(asyncio.CancelledError) as caught:
                await starting
            assert caught.value.args == ("original_start_cancellation",)
            assert startup.cancelled() and joined.is_set()
            assert owner.status is FinancialPublicationLifecycleStatus.CLOSED
            assert owner._subscriber is None and owner._workers == set()
        finally:
            release.set()
            await owner.stop()
            await asyncio.gather(starting, return_exceptions=True)


async def test_real_receive_failure_enters_backoff_without_claiming_a_current_snapshot(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse an actual unbound publication identity received over the real socket.

    Given: A genuine original publication has already hydrated the active owner.
    When: The real bus delivers the same valid payload under an absent durable publication UUID.
    Then: The genuine SQL reader refuses, receive enters backoff and only the historical view remains.

    Args:
        projection_scenario: Actual source-backed journal and retained publication.
        monkeypatch: Lifetime of observers delegating every real receive and SQL read.
    """
    intent = await record_projection_position(projection_scenario)
    original = VersionedContractAccountProjection.receive_message
    received, failed = asyncio.Event(), asyncio.Event()

    async def observe(
        projection: VersionedContractAccountProjection,
        topic: str,
        message: FinancialPublicationData,
    ) -> None:
        """Observe the original durable reader's actual completion or refusal.

        Args:
            projection: Actual lifecycle-owned original projection.
            topic: Independently received original frame topic.
            message: Actual typed envelope, whose durable identity still needs proof.
        """
        try:
            await original(projection, topic, message)
        except Exception:
            failed.set()
            raise
        received.set()

    monkeypatch.setattr(VersionedContractAccountProjection, "receive_message", observe)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        try:
            await owner.start()
            await received.wait()
            before = owner.last_snapshot
            message = FinancialPublicationData(
                public_id=str(uuid7()),
                wallet_public_id=intent.payload.scope.wallet_public_id,
                session_id=bus.publisher.session_id,
                sequence_id=bus.publisher.tracker.next_sequence(intent.topic),
                timestamp=datetime.now(UTC),
                payload=intent.payload,
            )
            await bus.publisher.send(intent.topic, message)
            await failed.wait()
            assert owner.failure_code == "financial_publication_receive_failed"
            assert owner.status is FinancialPublicationLifecycleStatus.STALE
            assert owner.snapshot is None and owner.last_snapshot == before
            assert any(not task.done() for task in owner._workers)
        finally:
            await owner.stop()


async def test_late_waiter_cancellation_preserves_actual_closed_state(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Do not relabel completed cleanup when cancellation reaches its external waiter late.

    Given: A real running owner has consumed its original journal notification.
    When: Original socket close wakes a controller before the shielded stop waiter resumes.
    Then: That waiter receives cancellation but actual completed resource disposal remains closed.

    Args:
        projection_scenario: Actual committed financial graph and repository.
        monkeypatch: Lifetime of the observer preserving original actual socket close.
    """
    intent = await record_projection_position(projection_scenario)
    received, _ = _observe_receive(monkeypatch)
    closed = asyncio.Event()
    original = ValidatedSubscriber.close
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        await owner.start()
        await received.wait()
        subscriber = owner._subscriber

        def observe_close(actual: ValidatedSubscriber) -> None:
            """Observe disposal before the cleanup task schedules its external shield completion.

            Args:
                actual: Original socket being closed by the actual cleanup operation.
            """
            original(actual)
            if actual is subscriber:
                closed.set()

        stopping: asyncio.Task[None] | None = None
        try:
            with monkeypatch.context() as patch:
                patch.setattr(ValidatedSubscriber, "close", observe_close)
                stopping = asyncio.create_task(owner.stop())
                await closed.wait()
                assert owner.status is FinancialPublicationLifecycleStatus.CLOSED
                stopping.cancel("late_stop_cancellation")
                with pytest.raises(asyncio.CancelledError) as caught:
                    await stopping
                assert caught.value.args == ("late_stop_cancellation",)
            assert owner.status is FinancialPublicationLifecycleStatus.CLOSED
            assert owner._context is None and owner._subscriber is None
        finally:
            await owner.stop()
            if stopping is not None:
                await asyncio.gather(stopping, return_exceptions=True)


async def test_unexpected_real_worker_loss_stays_unresolved_after_a_later_sql_refusal(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve supervision failure when another actual worker later encounters damaged history.

    Given: The real owner hydrated a journal and a requested refresh awaits its genuine SQL read.
    When: Its receive task is unexpectedly cancelled and the released refresh refuses a damaged row.
    Then: Worker loss remains unresolved, never relabelled as merely stale or newly current.

    Args:
        projection_scenario: Actual isolated financial source and immutable journal.
        monkeypatch: Lifetime of a delegating barrier before the actual refresh read.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    received, _ = _observe_receive(monkeypatch)
    refreshing, release, failed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = VersionedContractAccountProjection.refresh
    refresh_worker: asyncio.Task[None] | None = None

    async def observe_refresh(
        projection: VersionedContractAccountProjection,
    ) -> ContractAccountSnapshot:
        """Release the real owned SQL refresh only after observing the independent worker failure.

        Args:
            projection: Actual projection performing its original full SQL read.

        Returns:
            The original actual snapshot only if its complete durable graph still verifies.
        """
        if asyncio.current_task() is refresh_worker:
            refreshing.set()
            await release.wait()
        try:
            return await original(projection)
        except Exception:
            failed.set()
            raise

    monkeypatch.setattr(VersionedContractAccountProjection, "refresh", observe_refresh)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(repository, intent.payload.scope, bus)
        digest: str | None = None
        try:
            await owner.start()
            await received.wait()
            before = owner.last_snapshot
            refresh_worker = next(
                task
                for task in owner._workers
                if task.get_name() == "financial-publication-refresh"
            )
            receive_worker = next(
                task
                for task in owner._workers
                if task.get_name() == "financial-publication-receive"
            )
            async with repository.session() as session:
                digest = (
                    await session.scalars(select(ContractAccountingEvent.content_digest))
                ).one()
            await _restore_event_digest(repository, "0" * 64)
            owner.request_refresh()
            await refreshing.wait()
            receive_worker.cancel("unexpected_receive_owner_loss")
            with pytest.raises(FinancialPublicationLifecycleError, match="owner_unresolved"):
                await owner.wait()
            assert receive_worker.cancelled()
            assert owner.failure_code == "financial_publication_worker_stopped"
            release.set()
            await failed.wait()
            assert owner.status is FinancialPublicationLifecycleStatus.UNRESOLVED
            assert owner.failure_code == "financial_publication_worker_stopped"
            assert owner.snapshot is None and owner.last_snapshot == before
        finally:
            release.set()
            await owner.stop()
            if digest is not None:
                await _restore_event_digest(repository, digest)


async def test_eager_cleanup_joins_a_real_completed_worker_before_its_done_callback(
    projection_scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Handle the genuine completed-task window without cancelling an already finished worker.

    Given: Actual SQL receive completed and its original task reports cancellation before done callbacks.
    When: The external controller invokes cleanup using the standard Python eager task factory.
    Then: Cleanup joins that already completed worker and the remaining live worker before disposal.

    Args:
        projection_scenario: Actual isolated financial source graph and publication.
        monkeypatch: Lifetime of the delegating receive cancellation observer.
    """
    intent = await record_projection_position(projection_scenario)
    entered, cancelled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = VersionedContractAccountProjection.receive_message

    async def observe_receive(
        projection: VersionedContractAccountProjection,
        topic: str,
        message: FinancialPublicationData,
    ) -> None:
        """Expose the actual cancellation boundary before the task schedules its done callbacks.

        Args:
            projection: Actual original projection performing complete SQL receive.
            topic: Original independent transport topic.
            message: Genuine original publication envelope received on the real bus.
        """
        await original(projection, topic, message)
        entered.set()
        try:
            await release.wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(VersionedContractAccountProjection, "receive_message", observe_receive)
    async with _publication_bus(monkeypatch) as bus:
        owner = _owner(projection_scenario.fill.admission.repository, intent.payload.scope, bus)
        loop = asyncio.get_running_loop()
        factory = loop.get_task_factory()
        try:
            await owner.start()
            await entered.wait()
            workers = tuple(owner._workers)
            receive = next(
                task for task in workers if task.get_name() == "financial-publication-receive"
            )
            receive.cancel("completed_before_done_callback")
            await cancelled.wait()
            assert receive.done() and receive in owner._workers
            loop.set_task_factory(asyncio.eager_task_factory)
            await owner.stop()
            assert all(task.done() for task in workers)
            assert owner.status is FinancialPublicationLifecycleStatus.CLOSED
            assert owner._subscriber is None and owner._context is None
        finally:
            loop.set_task_factory(factory)
            release.set()
            await owner.stop()
