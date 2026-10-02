"""Observe coordinator listener ownership with inert event-gated collaborators."""

import asyncio
import json
from collections.abc import AsyncIterator
from collections.abc import Coroutine
from collections.abc import Generator
from contextvars import Context
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from snapper.application.engine import trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.config.app import AppSettings
from snapper.core.partitioning import ShardOwnership
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.schemas.data import SignalData


@dataclass
class _Worker:
    """Expose explicit entry, release and finalization rendezvous."""

    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    finalizing: asyncio.Event = field(default_factory=asyncio.Event)
    finish: asyncio.Event = field(default_factory=asyncio.Event)
    finalized: asyncio.Event = field(default_factory=asyncio.Event)
    error: BaseException | None = None
    hold_finalizer: bool = False
    finalizer_error: RuntimeError | None = None
    task: asyncio.Task[object] | None = None

    async def run(self) -> None:
        """Stay inert until released or cancelled and expose cleanup completion."""
        self.task = asyncio.current_task()
        self.entered.set()
        try:
            await self.release.wait()
            if self.error is not None:
                raise self.error
        finally:
            self.finalizing.set()
            if self.hold_finalizer:
                await self.finish.wait()
            self.finalized.set()
            if self.finalizer_error is not None:
                await _turn()
                raise self.finalizer_error

    def stop(self) -> None:
        """Support optional-worker stop without treating it as completed cleanup."""
        self.release.set()


@dataclass
class _Stream:
    """Gate actual listener receives without transport or order processing."""

    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    marker: asyncio.Event = field(default_factory=asyncio.Event)
    frames: asyncio.Queue[tuple[str, bytes]] = field(default_factory=asyncio.Queue)
    error: BaseException | None = None

    async def recv_multipart(self) -> tuple[str, bytes]:
        """Release an injected receive failure or queued schema payload."""
        self.entered.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error
        return await self.frames.get()


@dataclass
class _Harness:
    """Keep actual start, listener and loop while isolating external effects."""

    coordinator: TraderCoordinator
    stream: _Stream
    health: _Worker
    funding: _Worker
    listener_entered: asyncio.Event = field(default_factory=asyncio.Event)
    listener_release: asyncio.Event = field(default_factory=asyncio.Event)
    listener_finished: asyncio.Event = field(default_factory=asyncio.Event)
    listener: asyncio.Task[object] | None = None
    listener_error: BaseException | None = None
    owner: asyncio.Task[None] | None = None
    extra_workers: list[_Worker] = field(default_factory=list)
    observed_tasks: list[asyncio.Task[object]] = field(default_factory=list)

    def start(self) -> asyncio.Task[None]:
        """Retain the real start task for pre-cleanup outcome assertions."""
        self.owner = asyncio.create_task(self.coordinator.start())
        return self.owner

    async def ready(self) -> None:
        """Bound the initial rendezvous independently from behavior assertions."""
        async with asyncio.timeout(2):
            await self.listener_entered.wait()
            await self.health.entered.wait()
            await self.funding.entered.wait()

    async def emergency_cleanup(self) -> None:
        """Clean test-owned tasks only after the tested observations are complete."""
        workers = [self.health, self.funding, *self.extra_workers]
        for worker in workers:
            worker.finish.set()
        tasks = [task for task in [self.owner, self.listener] if task is not None]
        tasks.extend(worker.task for worker in workers if worker.task is not None)
        tasks.extend(self.observed_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        async with asyncio.timeout(2):
            await asyncio.gather(*tasks, return_exceptions=True)
        assert all(task.done() for task in tasks)


def _inert_coordinator(monkeypatch: pytest.MonkeyPatch) -> TraderCoordinator:
    """Construct normal lifecycle state while replacing service boundaries."""
    settings = MagicMock(spec=AppSettings)
    settings.db_url = "sqlite+aiosqlite:///:memory:"
    repository = MagicMock()
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings)
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: repository)
    coordinator = TraderCoordinator()
    for name in (
        "_initialize_settings",
        "_recover_engine_state_with_retry",
        "_recover_paired_execution_leg_fills",
        "_recover_paired_execution_guard_state",
    ):
        monkeypatch.setattr(coordinator, name, AsyncMock())
    for name in (
        "_setup_external_execution",
        "_setup_trading_components",
        "_setup_signal_subscriber",
        "_setup_trade_services",
    ):
        monkeypatch.setattr(coordinator, name, MagicMock())
    monkeypatch.setattr(coordinator, "_build_ownership", lambda: ShardOwnership(0, 1))
    monkeypatch.setattr(coordinator, "_build_caps_enforcer", lambda: None)
    return coordinator


@pytest.fixture
async def lifecycle(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Harness]:
    """Provide event gates and guarantee emergency cleanup after every assertion."""
    coordinator = _inert_coordinator(monkeypatch)
    harness = _Harness(coordinator, _Stream(), _Worker(), _Worker())
    harness.listener_release.set()
    subscriber = MagicMock()
    subscriber.recv_multipart = harness.stream.recv_multipart
    coordinator.signal_subscriber = subscriber
    monkeypatch.setattr(coordinator, "_signal_health_monitor", harness.health.run)
    monkeypatch.setattr(coordinator, "_funding_accrual_loop", harness.funding.run)
    actual_listener = coordinator._listen_signals

    async def observed_listener() -> None:
        """Record actual listener outcome without changing it or asserting inside it."""
        harness.listener = asyncio.current_task()
        harness.listener_entered.set()
        try:
            await harness.listener_release.wait()
            await actual_listener()
        except BaseException as exc:
            harness.listener_error = exc
            raise
        finally:
            harness.listener_finished.set()

    monkeypatch.setattr(coordinator, "_listen_signals", observed_listener)
    try:
        yield harness
    finally:
        await harness.emergency_cleanup()


async def _outcome(owner: asyncio.Task[None]) -> BaseException | None:
    """Observe owner completion without making the watchdog cancel production work."""
    done, _ = await asyncio.wait({owner}, timeout=1)
    assert owner in done, "coordinator remained pending after its required child completed"
    try:
        await owner
    except BaseException as exc:
        return exc
    return None


async def _turn() -> None:
    """Yield to queued callbacks without relying on timed sleeps."""
    event = asyncio.Event()
    asyncio.get_running_loop().call_soon(event.set)
    await event.wait()


async def test_healthy_listener_processes_marker_and_cancellation_joins_workers(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Establish that actual intake and inert worker gates run correctly.

    Given: A real coordinator with an inert settings frame and event-gated siblings,
    When: The listener consumes the marker and its owner is cancelled,
    Then: Intake was live and every started child has finalized before owner completion.
    """
    monkeypatch.setattr(
        lifecycle.coordinator, "_handle_settings_update", lambda _raw: lifecycle.stream.marker.set()
    )
    lifecycle.stream.frames.put_nowait(("system.settings", b"marker"))
    lifecycle.stream.release.set()
    owner = lifecycle.start()
    await lifecycle.ready()
    async with asyncio.timeout(2):
        await lifecycle.stream.marker.wait()
    assert not owner.done()
    owner.cancel()
    assert isinstance(await _outcome(owner), asyncio.CancelledError)
    assert lifecycle.listener_finished.is_set()
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()


async def test_receive_failure_reaches_owner_unchanged(lifecycle: _Harness) -> None:
    """Expose actual receive failure instead of leaving dead intake hidden.

    Given: An entered listener and two pending sibling workers,
    When: Receive raises a specific operational RuntimeError,
    Then: The owner raises that same object after every child has finalized.
    """
    error = RuntimeError("receive failed")
    lifecycle.stream.error = error
    owner = lifecycle.start()
    await lifecycle.ready()
    lifecycle.stream.release.set()
    assert await _outcome(owner) is error
    assert lifecycle.listener_error is error
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()


def _null_wallet_payload() -> bytes:
    """Serialize an otherwise valid signal with explicitly invalid null wallet."""
    now = datetime(2026, 10, 1, tzinfo=UTC)
    signal = SignalData(
        public_id="lifecycle-probe",
        timestamp=now,
        fired_at=now,
        session_id="01975a8b-3c7d-7000-8000-abcdef123456",
        sequence_id=1,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        strength=0.5,
        reason="probe",
    )
    payload = signal.model_dump(mode="json")
    payload["wallet_public_id"] = None
    return json.dumps(payload).encode()


@pytest.mark.parametrize("payload", [b"{", _null_wallet_payload()], ids=["brace", "null-wallet"])
async def test_actual_schema_failure_keeps_original_identity(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    """Preserve actual parsing errors through listener logging and owner teardown.

    Given: Malformed JSON or an explicit null wallet reaches the real schema parser,
    When: The parser produces a recorded ValidationError,
    Then: Listener and owner expose that exact object without logging replacement.
    """
    errors: list[ValidationError] = []
    actual_parse = SignalData.from_json

    def parse(raw: str) -> SignalData:
        """Transparently record the parser's original exception before rethrowing."""
        try:
            return actual_parse(raw)
        except ValidationError as exc:
            errors.append(exc)
            raise

    monkeypatch.setattr(SignalData, "from_json", staticmethod(parse))
    lifecycle.stream.frames.put_nowait(("signals.kraken.BTC-USD.live", payload))
    owner = lifecycle.start()
    await lifecycle.ready()
    lifecycle.stream.release.set()
    async with asyncio.timeout(2):
        await lifecycle.listener_finished.wait()
    assert len(errors) == 1
    assert lifecycle.listener_error is errors[0]
    assert await _outcome(owner) is errors[0]


async def test_required_listener_normal_return_fails_owner(lifecycle: _Harness) -> None:
    """Reject silent loss of the only required intake worker.

    Given: A coordinator has no signal subscriber while siblings remain active,
    When: Its actual listener returns normally,
    Then: The owner fails explicitly and joins its remaining workers.
    """
    lifecycle.coordinator.signal_subscriber = None
    owner = lifecycle.start()
    await lifecycle.ready()
    assert isinstance(await _outcome(owner), RuntimeError)
    assert lifecycle.listener_error is None
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()


async def test_optional_worker_normal_return_keeps_listener_running(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain nonfatal optional completion while proving successor intake works.

    Given: An outbox worker and required listener are running,
    When: The outbox returns normally and a settings marker then arrives,
    Then: The marker is consumed and the owner remains pending until cancellation.
    """
    outbox = _Worker()
    lifecycle.extra_workers.append(outbox)
    monkeypatch.setattr(lifecycle.coordinator, "outbox", outbox)
    monkeypatch.setattr(
        lifecycle.coordinator, "_handle_settings_update", lambda _raw: lifecycle.stream.marker.set()
    )
    owner = lifecycle.start()
    await lifecycle.ready()
    async with asyncio.timeout(2):
        await outbox.entered.wait()
        outbox.release.set()
        await outbox.finalized.wait()
        lifecycle.stream.frames.put_nowait(("system.settings", b"after-outbox"))
        lifecycle.stream.release.set()
        await lifecycle.stream.marker.wait()
    await _turn()
    assert not owner.done()
    owner.cancel()
    assert isinstance(await _outcome(owner), asyncio.CancelledError)


async def test_failure_cancels_all_children_before_waiting_for_finalizers(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cancel all pending children and await every gated finalizer.

    Given: Funding and outbox workers each hold a finalization gate,
    When: A health worker raises while listener and siblings are pending,
    Then: Both finalizers start before either is released and owner waits for both.
    """
    error = RuntimeError("health failed")
    lifecycle.health.error = error
    lifecycle.funding.hold_finalizer = True
    outbox = _Worker(hold_finalizer=True)
    lifecycle.extra_workers.append(outbox)
    monkeypatch.setattr(lifecycle.coordinator, "outbox", outbox)
    owner = lifecycle.start()
    await lifecycle.ready()
    async with asyncio.timeout(2):
        await outbox.entered.wait()
    lifecycle.health.release.set()
    reached = asyncio.create_task(outbox.finalizing.wait())
    lifecycle.observed_tasks.append(cast(asyncio.Task[object], reached))
    done, _ = await asyncio.wait({reached}, timeout=1)
    assert reached in done, "later sibling was not cancelled while earlier finalizer waited"
    assert lifecycle.funding.finalizing.is_set()
    assert not owner.done()
    lifecycle.funding.finish.set()
    await _turn()
    assert not owner.done()
    outbox.finish.set()
    assert await _outcome(owner) is error
    assert outbox.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()
    assert lifecycle.listener_finished.is_set()


@pytest.mark.parametrize("factory", ["scanner", "reconciliation"])
async def test_partial_factory_failure_joins_every_created_task(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch, factory: str
) -> None:
    """Keep ownership through ordinary partial child construction failure.

    Given: Real task factories use inert SQL collaborators and worker constructors,
    When: Scanner setup or the second reconciliation constructor raises,
    Then: The original failure escapes only after all already-created tasks are joined.
    """
    error = RuntimeError(f"{factory} constructor failed")
    repository = MagicMock(spec=SQLAlchemyRepository)
    lifecycle.coordinator.repository = repository
    scanner = _Worker()
    lifecycle.extra_workers.append(scanner)
    calls: list[str] = []

    def create_scanner(**_kwargs: object) -> _Worker:
        """Fail scanner setup or return an inert registered worker."""
        if factory == "scanner":
            raise error
        return scanner

    def create_reconciliation(**kwargs: object) -> _Worker:
        """Fail a later constructor after production created its first task."""
        calls.append(str(kwargs["exchange_name"]))
        if len(calls) == 2:
            raise error
        worker = _Worker()
        lifecycle.extra_workers.append(worker)
        return worker

    original_create = asyncio.create_task
    created: list[asyncio.Task[None]] = []

    def create_task(
        coro: Coroutine[object, object, None],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[None]:
        """Track real task handles including children cancelled before first entry."""
        task = original_create(coro, name=name, context=context)
        created.append(task)
        return task

    monkeypatch.setattr(trader_module, "PairedExecutionGuardScanner", create_scanner)
    monkeypatch.setattr(trader_module, "ReconciliationLoop", create_reconciliation)
    monkeypatch.setattr(trader_module.asyncio, "create_task", create_task)
    owner = lifecycle.start()
    try:
        assert await _outcome(owner) is error
        assert len(created) >= (4 if factory == "scanner" else 6)
        assert all(task.done() for task in created), "created children survived owner failure"
    finally:
        lifecycle.observed_tasks.extend(cast(list[asyncio.Task[object]], created))


@pytest.mark.parametrize(
    "priority", ["listener", "worker", "normal-listener", "cancelled-listener"]
)
async def test_simultaneous_done_children_choose_stable_primary(
    lifecycle: _Harness, priority: str
) -> None:
    """Select real failures by listener then creation order before synthetic exit.

    Given: Listener and sibling outcomes are released in reverse priority order,
    When: All selected children finish in the same runnable callback batch,
    Then: Listener failure wins, otherwise health failure beats funding and normal exit.
    """
    listener_error = RuntimeError("listener primary")
    health_error = RuntimeError("health primary")
    lifecycle.health.error = health_error
    lifecycle.funding.error = RuntimeError("funding secondary")
    lifecycle.listener_release.clear()
    if priority == "normal-listener":
        lifecycle.coordinator.signal_subscriber = None
    elif priority == "cancelled-listener":
        lifecycle.stream.error = asyncio.CancelledError("independent listener cancellation")
    else:
        lifecycle.stream.error = listener_error
    owner = lifecycle.start()
    await lifecycle.ready()
    lifecycle.funding.release.set()
    lifecycle.health.release.set()
    if priority != "worker":
        lifecycle.stream.release.set()
        lifecycle.listener_release.set()
    expected = listener_error if priority == "listener" else health_error
    assert await _outcome(owner) is expected
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()
    assert lifecycle.listener_finished.is_set()


async def test_independent_child_cancellation_remains_cancellation(lifecycle: _Harness) -> None:
    """Keep a child cancellation distinct from a synthesized normal-return failure.

    Given: The actual listener's receive operation raises CancelledError,
    When: Its owner observes the independently cancelled child,
    Then: The owner propagates cancellation and joins both sibling workers.
    """
    lifecycle.stream.error = asyncio.CancelledError("receive independently cancelled")
    owner = lifecycle.start()
    await lifecycle.ready()
    lifecycle.stream.release.set()
    assert isinstance(await _outcome(owner), asyncio.CancelledError)
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()


async def test_real_factories_create_and_join_all_healthy_workers(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Establish that inert constructors satisfy both actual production factories.

    Given: A SQL repository double and healthy inert scanner/reconciliation constructors,
    When: Actual start creates every configured worker and the owner is cancelled,
    Then: Each constructed worker entered and finalized through production cleanup.
    """
    lifecycle.coordinator.repository = MagicMock(spec=SQLAlchemyRepository)

    def create_worker(**_kwargs: object) -> _Worker:
        """Retain every inert instance constructed by either real factory."""
        worker = _Worker()
        lifecycle.extra_workers.append(worker)
        return worker

    scanner_factory = MagicMock(side_effect=create_worker)
    reconciliation_factory = MagicMock(side_effect=create_worker)
    monkeypatch.setattr(trader_module, "PairedExecutionGuardScanner", scanner_factory)
    monkeypatch.setattr(trader_module, "ReconciliationLoop", reconciliation_factory)
    owner = lifecycle.start()
    await lifecycle.ready()
    async with asyncio.timeout(2):
        for worker in lifecycle.extra_workers:
            await worker.entered.wait()
    scanner_factory.assert_called_once()
    assert reconciliation_factory.call_count > 1
    assert len(lifecycle.extra_workers) == 1 + reconciliation_factory.call_count
    assert not owner.done()
    owner.cancel()
    assert isinstance(await _outcome(owner), asyncio.CancelledError)
    assert all(worker.finalized.is_set() for worker in lifecycle.extra_workers)
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()


class _ObservedTask(asyncio.Task[None]):
    """Preserve real task semantics while recording public outcome retrieval."""

    outcome_reads: int = 0

    def __await__(self) -> Generator[object]:
        """Count completed await retrieval, including an exception raised by the await."""
        try:
            return (yield from super().__await__())
        finally:
            if self.done():
                self.outcome_reads += 1

    def exception(self) -> BaseException | None:
        """Count an exception retrieval before delegating to the real Task method."""
        self.outcome_reads += 1
        return super().exception()

    def result(self) -> None:
        """Count a result retrieval before delegating to the real Task method."""
        self.outcome_reads += 1
        return super().result()


def _observed_task_factory(
    loop: asyncio.AbstractEventLoop,
    coro: Coroutine[object, object, None],
    *,
    name: str | None = None,
    context: Context | None = None,
) -> _ObservedTask:
    """Build an ordinary scheduled task with transparent retrieval counters."""
    return _ObservedTask(coro, loop=loop, name=name, context=context)


async def test_completed_secondary_failure_is_retrieved_before_harness_cleanup(
    lifecycle: _Harness,
) -> None:
    """Observe secondary exception retrieval independently from emergency gather.

    Given: Real task instances record public exception and result retrieval calls,
    When: Health and funding both fail before the coordinator observes completion,
    Then: Health stays primary and funding's error is retrieved before fixture teardown.
    """
    primary = RuntimeError("health primary before secondary")
    secondary = RuntimeError("already completed funding secondary")
    lifecycle.health.error = primary
    lifecycle.funding.error = secondary
    loop = asyncio.get_running_loop()
    prior_factory = loop.get_task_factory()
    loop.set_task_factory(_observed_task_factory)
    try:
        owner = lifecycle.start()
        await lifecycle.ready()
    finally:
        loop.set_task_factory(prior_factory)
    secondary_task = lifecycle.funding.task
    assert isinstance(secondary_task, _ObservedTask)
    assert secondary_task.outcome_reads == 0
    lifecycle.health.release.set()
    lifecycle.funding.release.set()
    result = await _outcome(owner)
    assert secondary_task.done()
    assert (
        secondary_task.outcome_reads > 0
    ), "production never retrieved the completed sibling error"
    assert result is primary
    assert lifecycle.listener_finished.is_set()
    assert lifecycle.health.finalized.is_set()
    assert lifecycle.funding.finalized.is_set()


async def test_async_finalizer_failure_keeps_primary_and_joins_other_children(
    lifecycle: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep an earlier operational failure through an asynchronous cleanup failure.

    Given: Health fails, funding raises after an async finalizer step and outbox holds cleanup,
    When: Production begins joining all owned children,
    Then: Funding's cleanup error cannot replace health or bypass the outbox finalizer gate.
    """
    primary = RuntimeError("health operational primary")
    cleanup_error = RuntimeError("funding asynchronous finalizer failure")
    lifecycle.health.error = primary
    lifecycle.funding.finalizer_error = cleanup_error
    outbox = _Worker(hold_finalizer=True)
    lifecycle.extra_workers.append(outbox)
    monkeypatch.setattr(lifecycle.coordinator, "outbox", outbox)
    owner = lifecycle.start()
    await lifecycle.ready()
    async with asyncio.timeout(2):
        await outbox.entered.wait()
    lifecycle.health.release.set()
    funding_task = lifecycle.funding.task
    assert funding_task is not None
    done, _ = await asyncio.wait({funding_task}, timeout=1)
    assert funding_task in done, "funding never completed its throwing finalizer"
    assert funding_task.exception() is cleanup_error
    assert not owner.done(), "cleanup failure ended the owner before joining the gated sibling"
    async with asyncio.timeout(2):
        await outbox.finalizing.wait()
    assert not outbox.finalized.is_set()
    outbox.finish.set()
    assert await _outcome(owner) is primary
    assert outbox.finalized.is_set()
    assert outbox.task is not None
    assert outbox.task.done()
    assert lifecycle.listener_finished.is_set()
    assert lifecycle.health.finalized.is_set()
