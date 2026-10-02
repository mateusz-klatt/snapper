"""Coordinator lifetime contracts with inert sockets and normally constructed owners."""

import asyncio
from collections.abc import Coroutine
from collections.abc import Generator
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import zmq
import zmq.asyncio

from snapper.application.engine import trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.config.app import AppSettings
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber


class ResourceSocket:
    """Record local socket outcomes without creating a native resource."""

    def __init__(self, context: ResourceContext) -> None:
        """Attach the socket to its exclusively owned fake context.

        Args:
            context: Owning inert context for this socket.
        """
        self.context = context
        self.closed = False
        self.close_attempts = 0
        self.linger: int | None = None

    def setsockopt(self, option: int, value: int) -> None:
        """Apply inert options and inject failure after raw acquisition.

        Args:
            option: ZMQ option identifier accepted by the fake.
            value: Inert option or subscription value.
        """
        if self.closed:
            raise RuntimeError("option on closed fake socket")
        if option in (zmq.SNDHWM, zmq.RCVHWM):
            self.context.fail("hwm")
        if option == zmq.LINGER:
            self.linger = value

    def setsockopt_string(self, option: int, value: str) -> None:
        """Accept real validated-wrapper subscription setup.

        Args:
            option: ZMQ option identifier accepted by the fake.
            value: Inert option or subscription value.
        """
        if self.closed:
            raise RuntimeError("subscription on closed fake socket")

    def connect(self, address: str) -> None:
        """Inject connect failures without resolving any address.

        Args:
            address: Inert endpoint recorded by production setup.
        """
        self.context.fail("connect")

    async def recv_multipart(self, flags: int = 0) -> list[bytes]:
        """Supply actual listener frames and expose intake cancellation.

        Args:
            flags: Receive flags forwarded by the real validated subscriber.

        Returns:
            Queued multipart frame for the actual subscriber.
        """
        harness = self.context.harness
        harness.record_child()
        harness.listening.set()
        try:
            frame = await harness.frames.get()
        except asyncio.CancelledError as error:
            harness.listener_cancellation = error
            raise
        if isinstance(frame, RuntimeError):
            raise frame
        return frame

    def close(self, linger: int | None = None) -> None:
        """Model idempotent closure and a close error before successful release.

        Args:
            linger: Requested pending-message linger setting.
        """
        if self.closed:
            return
        if not self.context.harness.emergency and self.context.harness.first_close_children is None:
            self.context.harness.first_close_children = tuple(
                task.done() for task in self.context.harness.children
            )
        self.close_attempts += 1
        if linger is not None:
            self.linger = linger
        if self.context.harness.close_failure == self.context.index:
            raise self.context.harness.cleanup_error
        self.closed = True
        self.context.harness.events.append(f"closed-{self.context.index}")


class ResourceContext:
    """Support both close/term and destroy without prescribing a helper choice."""

    def __init__(self, harness: ResourceHarness, index: int) -> None:
        """Retain observations even when destruction fails.

        Args:
            harness: Per-test owner, gates and resource observations.
            index: Context acquisition order within the coordinator.
        """
        self.harness = harness
        self.index = index
        self.sockets: list[ResourceSocket] = []
        self.closed = False
        self.destroy_attempts = 0
        self.term_attempts = 0
        self.unsafe_terms = 0

    def fail(self, stage: str) -> None:
        """Raise the exact operational error at the selected setup boundary.

        Args:
            stage: Acquisition boundary selected for an injected failure.
        """
        if self.harness.failure_stage == f"{self.index}-{stage}":
            raise self.harness.primary

    def socket(self, kind: int) -> ResourceSocket:
        """Acquire a raw socket before production constructs its wrapper.

        Args:
            kind: Socket type requested by production setup.

        Returns:
            New inert raw socket owned by this context.
        """
        self.fail("socket")
        socket = ResourceSocket(self)
        self.sockets.append(socket)
        return socket

    def term(self) -> None:
        """Refuse unsafe termination immediately instead of blocking a test."""
        self.term_attempts += 1
        if any(not socket.closed for socket in self.sockets):
            self.unsafe_terms += 1
            raise AssertionError("term called with an open owned socket")
        if self.harness.destroy_failure == self.index:
            raise self.harness.cleanup_error
        self.closed = True
        self.harness.events.append(f"terminated-{self.index}")

    def destroy(self, linger: int | None = None) -> None:
        """Implement ordinary pyzmq close-before-term and fail-fast destruction.

        Args:
            linger: Requested pending-message linger setting.
        """
        if self.closed:
            return
        self.destroy_attempts += 1
        for socket in self.sockets:
            if not socket.closed:
                if linger is not None:
                    socket.setsockopt(zmq.LINGER, linger)
                socket.close()
        self.term()


class ResourceWorker:
    """Hold a worker finalizer so tests can observe joined cleanup ordering."""

    def __init__(self, name: str, harness: ResourceHarness) -> None:
        """Create entry and cleanup rendezvous for one owned child.

        Args:
            name: Worker completion event label.
            harness: Per-test owner, gates and resource observations.
        """
        self.name = name
        self.harness = harness
        self.entered = asyncio.Event()
        self.finalizing = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.finished = False
        self.interrupted = False

    async def run(self) -> None:
        """Remain active until cancellation, then finish only after release."""
        self.harness.record_child()
        self.entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.finalizing.set()
            try:
                await self.release.wait()
                self.finished = True
                self.harness.events.append(self.name)
            except asyncio.CancelledError:
                self.interrupted = True
                raise


class ResourceHarness:
    """Patch external acquisition while retaining real coordinator composition."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Construct the owner normally with a shared inert repository.

        Args:
            monkeypatch: Per-test patch scope for external collaborators.
        """
        self.contexts: list[ResourceContext] = []
        self.tasks: list[asyncio.Task[None]] = []
        self.children: set[asyncio.Task[object]] = set()
        self.events: list[str] = []
        self.emergency = False
        self.first_close_children: tuple[bool, ...] | None = None
        self.frames: asyncio.Queue[list[bytes] | RuntimeError] = asyncio.Queue()
        self.listening = asyncio.Event()
        self.marker = asyncio.Event()
        self.listener_cancellation: asyncio.CancelledError | None = None
        self.primary = RuntimeError("original startup or intake failure")
        self.cleanup_error = RuntimeError("owned resource cleanup failure")
        self.failure_stage = ""
        self.close_failure: int | None = None
        self.destroy_failure: int | None = None
        self.repository = MagicMock(dialect_name="postgresql")
        settings = MagicMock(db_url="sqlite+aiosqlite:///:memory:")
        settings.coordinator_instance_id = 0
        settings.coordinator_instance_count = 1
        settings.zmq_broker_xsub = "inert://publisher"
        settings.zmq_broker_xpub = "inert://subscriber"
        monkeypatch.setattr(trader_module, "get_settings", lambda: settings)
        monkeypatch.setattr(trader_module, "get_repository", lambda url: self.repository)
        monkeypatch.setattr(trader_module, "_bootstrap_settings", settings)
        monkeypatch.setattr(trader_module.zmq.asyncio, "Context", self.context)
        monkeypatch.setattr(trader_module, "ValidatedPublisher", self.publisher)
        monkeypatch.setattr(trader_module, "ValidatedSubscriber", self.subscriber)
        self.owner = TraderCoordinator(settings=cast(AppSettings, settings))
        self.workers = [
            ResourceWorker("health-finished", self),
            ResourceWorker("funding-finished", self),
        ]
        self.patch_workers(monkeypatch)

    def patch_workers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replace domain work with inert event gates without replacing ownership.

        Args:
            monkeypatch: Per-test patch scope for external collaborators.
        """
        monkeypatch.setattr(self.owner, "_recover_engine_state_with_retry", AsyncMock())
        monkeypatch.setattr(self.owner, "_recover_paired_execution_leg_fills", AsyncMock())
        monkeypatch.setattr(self.owner, "_recover_paired_execution_guard_state", AsyncMock())
        monkeypatch.setattr(self.owner, "_signal_health_monitor", self.workers[0].run)
        monkeypatch.setattr(self.owner, "_funding_accrual_loop", self.workers[1].run)
        monkeypatch.setattr(self.owner, "_create_guard_scanner_task", lambda: None)
        monkeypatch.setattr(self.owner, "_create_reconciliation_tasks", lambda: [])
        monkeypatch.setattr(
            self.owner, "_handle_settings_update", lambda payload: self.marker.set()
        )

    def context(self) -> ResourceContext:
        """Allocate an inert owned context or fail acquisition.

        Returns:
            New inert context retained for cleanup observations.
        """
        index = len(self.contexts)
        if self.failure_stage == f"{index}-context":
            raise self.primary
        context = ResourceContext(self, index)
        self.contexts.append(context)
        return context

    def publisher(self, socket: ResourceSocket) -> ValidatedPublisher:
        """Use the actual validated publisher unless construction is the fault.

        Args:
            socket: Inert raw socket passed to the actual validated wrapper.

        Returns:
            Actual validated publisher wrapping the inert socket.
        """
        socket.context.fail("wrapper")
        return ValidatedPublisher(cast(zmq.asyncio.Socket, socket))

    def subscriber(self, socket: ResourceSocket) -> ValidatedSubscriber:
        """Use the actual validated subscriber unless construction is the fault.

        Args:
            socket: Inert raw socket passed to the actual validated wrapper.

        Returns:
            Actual validated subscriber wrapping the inert socket.
        """
        socket.context.fail("wrapper")
        return ValidatedSubscriber(cast(zmq.asyncio.Socket, socket))

    def spawn(self, coroutine: Coroutine[object, object, None]) -> asyncio.Task[None]:
        """Record every harness-owned task for emergency teardown.

        Args:
            coroutine: Harness-owned coroutine to schedule and later join.

        Returns:
            Recorded task for bounded teardown.
        """
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def running(self) -> asyncio.Task[None]:
        """Prove actual intake handles a healthy marker before inducing failure.

        Returns:
            Still-running owner after intake readiness is established.
        """
        task = self.spawn(self.owner.start())
        await asyncio.wait_for(self.listening.wait(), 1)
        for worker in self.workers:
            await asyncio.wait_for(worker.entered.wait(), 1)
        await self.frames.put([b"system.settings", b"{}"])
        await asyncio.wait_for(self.marker.wait(), 1)
        assert not task.done()
        return task

    def assert_released(self) -> None:
        """Assert resources closed without disposing the shared repository."""
        assert self.contexts
        assert all(context.closed for context in self.contexts)
        assert all(
            socket.closed and socket.linger == 0
            for context in self.contexts
            for socket in context.sockets
        )
        assert not any(context.unsafe_terms for context in self.contexts)
        self.repository.dispose.assert_not_called()
        self.repository.close.assert_not_called()
        self.repository.engine.dispose.assert_not_called()

    def assert_joined_before_close(self) -> None:
        """Require worker completion before the first socket release."""
        assert self.first_close_children is not None
        assert len(self.first_close_children) == 3
        assert all(self.first_close_children), "an owned child was still running at socket close"
        first_close = min(self.events.index(f"closed-{context.index}") for context in self.contexts)
        assert all(worker.finished and not worker.interrupted for worker in self.workers)
        assert all(self.events.index(worker.name) < first_close for worker in self.workers)

    def record_child(self) -> None:
        """Track children only for emergency harness cleanup, never production ownership."""
        task = asyncio.current_task()
        assert task is not None
        self.children.add(task)

    async def cleanup(self) -> None:
        """Separate bounded emergency test teardown from production observations."""
        self.emergency = True
        for worker in self.workers:
            worker.release.set()
        tasks = set(self.tasks) | self.children
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=1)
            for task in pending:
                task.cancel()
            if pending:
                _, pending = await asyncio.wait(pending, timeout=1)
            assert not pending, "emergency cleanup could not join harness tasks"
            await asyncio.gather(*tasks, return_exceptions=True)
        self.close_failure = self.destroy_failure = None
        for context in self.contexts:
            context.destroy(linger=0)


@pytest.fixture
def resources(monkeypatch: pytest.MonkeyPatch) -> Generator[ResourceHarness]:
    """Expose an inert constructor fixture with per-test monkeypatch lifetime."""
    yield ResourceHarness(monkeypatch)


async def outcome(task: asyncio.Task[None]) -> BaseException | None:
    """Observe a bounded task completion without injecting a watchdog cancel.

    Args:
        task: Owner or stop task whose completion is observed without cancellation.

    Returns:
        The exact observed task exception, or None for normal completion.
    """
    done, _ = await asyncio.wait({task}, timeout=1)
    assert done, "coordinator did not complete within the test watchdog"
    try:
        task.result()
    except BaseException as error:
        return error
    return None


async def rendezvous() -> None:
    """Allow ready cancellation callbacks to run without a timing sleep."""
    event = asyncio.Event()
    asyncio.get_running_loop().call_soon(event.set)
    await event.wait()


@pytest.mark.parametrize(
    "stage",
    [
        "0-socket",
        "0-hwm",
        "0-connect",
        "0-wrapper",
        "1-context",
        "1-socket",
        "1-hwm",
        "1-connect",
        "1-wrapper",
    ],
)
async def test_partial_setup_releases_every_acquired_resource(
    resources: ResourceHarness, stage: str
) -> None:
    """An original setup error escapes only after all acquired resources close.

    Given: A normally constructed owner with an injected acquisition failure,
    When: Actual start fails after acquiring some owned resources,
    Then: The same primary escapes after every acquired resource is released.
    """
    resources.failure_stage = stage
    try:
        assert await outcome(resources.spawn(resources.owner.start())) is resources.primary
        resources.assert_released()
    finally:
        await resources.cleanup()


@pytest.mark.parametrize(
    "method",
    [
        "_recover_engine_state_with_retry",
        "_recover_paired_execution_leg_fills",
        "_recover_paired_execution_guard_state",
    ],
)
async def test_recovery_failure_releases_resources(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    """Every recovery stage remains inside the coordinator resource lifetime.

    Given: Both socket setups succeed and a selected recovery stage fails,
    When: The real owner executes recovery before intake,
    Then: The exact failure escapes after resource cleanup without starting intake.
    """
    monkeypatch.setattr(resources.owner, method, AsyncMock(side_effect=resources.primary))
    try:
        assert await outcome(resources.spawn(resources.owner.start())) is resources.primary
        resources.assert_released()
        assert not resources.listening.is_set()
    finally:
        await resources.cleanup()


async def test_healthy_stop_joins_workers_and_is_repeatable(resources: ResourceHarness) -> None:
    """A healthy intake stops before release and repeated external stop is harmless.

    Given: Actual intake handles a healthy marker with both siblings running,
    When: External stop is requested and then called twice more,
    Then: Workers finish before resource closure and repeated stop remains harmless.
    """
    try:
        owner = await resources.running()
        await resources.owner.stop()
        result = await outcome(owner)
        assert result is None or isinstance(result, asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
        await resources.owner.stop()
        await resources.owner.stop()
        resources.assert_released()
    finally:
        await resources.cleanup()


async def test_stop_before_start_does_not_poison_later_cleanup(resources: ResourceHarness) -> None:
    """A no-resource stop does not disable finalization of a later real start.

    Given: An unstarted coordinator receives an early stop request,
    When: It later starts healthy intake and is cancelled,
    Then: Its later lifetime still joins children and releases owned resources.
    """
    try:
        await resources.owner.stop()
        owner = await resources.running()
        owner.cancel("first cancellation")
        assert isinstance(await outcome(owner), asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_external_stop_during_recovery_waits_for_owner(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Stop during awaited startup joins that startup before closing its sockets.

    Given: Recovery has entered a gated asynchronous startup operation,
    When: External stop requests shutdown while recovery finalization is held,
    Then: Stop waits for startup completion before closing resources.
    """
    entered = asyncio.Event()
    finalizing = asyncio.Event()
    release = asyncio.Event()

    async def recovery() -> None:
        """Expose the startup finalizer as a deterministic ownership boundary."""
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
            resources.events.append("recovery-finished")

    monkeypatch.setattr(resources.owner, "_recover_engine_state_with_retry", recovery)
    try:
        owner = resources.spawn(resources.owner.start())
        await asyncio.wait_for(entered.wait(), 1)
        stopping = resources.spawn(resources.owner.stop())
        await asyncio.wait_for(finalizing.wait(), 1)
        assert not stopping.done()
        assert not any(
            socket.closed for context in resources.contexts for socket in context.sockets
        )
        release.set()
        assert await outcome(stopping) is None
        assert isinstance(await outcome(owner), (asyncio.CancelledError, type(None)))
        resources.assert_released()
        assert resources.events.index("recovery-finished") < resources.events.index("closed-0")
    finally:
        release.set()
        await resources.cleanup()


async def test_concurrent_stop_does_not_recancel_joining_owner(resources: ResourceHarness) -> None:
    """Additional stop callers wait for one cleanup without interrupting children.

    Given: Healthy intake owns a worker with a held finalizer,
    When: Three external stop callers request the same shutdown,
    Then: All callers wait without interrupting finalization or closing resources early.
    """
    worker = resources.workers[0]
    worker.release.clear()
    try:
        owner = await resources.running()
        stops = [resources.spawn(resources.owner.stop())]
        await asyncio.wait_for(worker.finalizing.wait(), 1)
        stops.extend(resources.spawn(resources.owner.stop()) for _ in range(2))
        await rendezvous()
        assert not worker.interrupted
        assert not any(task.done() for task in stops)
        assert not any(
            socket.closed for context in resources.contexts for socket in context.sockets
        )
        worker.release.set()
        for task in stops:
            assert await outcome(task) is None
        assert isinstance(await outcome(owner), (asyncio.CancelledError, type(None)))
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


@pytest.mark.parametrize("primary_failure", [False, True])
async def test_joined_cleanup_preserves_primary_through_three_cancels(
    resources: ResourceHarness, primary_failure: bool
) -> None:
    """Bounded repeated cancellation cannot replace the primary or cut off joining.

    Given: Healthy intake has an operational failure or first owner cancellation,
    When: Further cancellations arrive while a child finalizer is gated,
    Then: Joining finishes and the original error or first cancellation survives.
    """
    worker = resources.workers[0]
    worker.release.clear()
    try:
        owner = await resources.running()
        if primary_failure:
            await resources.frames.put(resources.primary)
        else:
            owner.cancel("first cancellation")
        await asyncio.wait_for(worker.finalizing.wait(), 1)
        for index in range(3 if primary_failure else 2):
            owner.cancel(f"later cancellation {index}")
            await rendezvous()
            assert not owner.done()
            assert not worker.interrupted
        worker.release.set()
        result = await outcome(owner)
        if primary_failure:
            assert result is resources.primary
        else:
            assert isinstance(result, asyncio.CancelledError)
            assert result.args == ("first cancellation",)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


@pytest.mark.parametrize("failure_kind", ["close", "destroy"])
async def test_cleanup_failure_retains_primary_and_attempts_other_context(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch, failure_kind: str
) -> None:
    """Cleanup failure is contained without unsafe term or a blind cleanup retry.

    Given: Recovery fails and one owned context cannot be fully cleaned up,
    When: Automatic cleanup attempts resource release,
    Then: The primary survives and the other context closes without unsafe retries.
    """
    if failure_kind == "close":
        resources.close_failure = 1
    else:
        resources.destroy_failure = 1
    monkeypatch.setattr(
        resources.owner,
        "_recover_engine_state_with_retry",
        AsyncMock(side_effect=resources.primary),
    )
    try:
        assert await outcome(resources.spawn(resources.owner.start())) is resources.primary
        publisher, subscriber = resources.contexts
        assert publisher.closed
        assert not subscriber.closed
        assert not any(context.unsafe_terms for context in resources.contexts)
        assert_failed_cleanup_attempts(subscriber, failure_kind)
        assert resources.owner.zmq_context is subscriber
        if failure_kind == "close":
            assert resources.owner.signal_subscriber is not None
    finally:
        await resources.cleanup()


async def test_healthy_intake_control_reaches_real_loop(resources: ResourceHarness) -> None:
    """Control: real constructed owner receives a marker and owns live children.

    Given: A constructed coordinator has inert sockets and event-gated siblings,
    When: Actual intake handles a marker before ordinary owner cancellation,
    Then: All three mandatory children ran and the siblings finish cancellation.
    """
    try:
        owner = await resources.running()
        assert len(resources.contexts) == 2
        assert len(resources.children) == 3
        assert not any(context.closed for context in resources.contexts)
        owner.cancel("control shutdown")
        assert isinstance(await outcome(owner), asyncio.CancelledError)
        assert all(worker.finished for worker in resources.workers)
    finally:
        await resources.cleanup()


async def test_idle_setup_and_explicit_stop_control(resources: ResourceHarness) -> None:
    """Control: real setup wrappers and stop can release normally acquired resources.

    Given: Actual setup constructs validated wrappers around inert sockets,
    When: Explicit stop runs without an active owner task,
    Then: Both local contexts and sockets are released without shared disposal.
    """
    try:
        resources.owner._setup_external_execution()
        resources.owner._setup_trading_components()
        resources.owner._setup_signal_subscriber()
        await resources.owner.stop()
        resources.assert_released()
    finally:
        await resources.cleanup()


async def test_recovery_cancellation_retains_exact_primary(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A known cancellation object survives automatic startup resource cleanup.

    Given: Recovery raises a known cancellation object after socket setup,
    When: The real start lifetime unwinds,
    Then: That exact cancellation escapes after automatic resource cleanup.
    """
    first = asyncio.CancelledError("known first cancellation")
    monkeypatch.setattr(
        resources.owner, "_recover_engine_state_with_retry", AsyncMock(side_effect=first)
    )
    try:
        assert await outcome(resources.spawn(resources.owner.start())) is first
        resources.assert_released()
    finally:
        await resources.cleanup()


async def test_repeated_idle_stop_is_idempotent(resources: ResourceHarness) -> None:
    """Repeated explicit stop does not set options on already closed sockets.

    Given: Real setup acquires both local sockets without starting intake,
    When: Explicit stop is called twice,
    Then: No operation touches an already closed socket and resources remain released.
    """
    try:
        resources.owner._setup_external_execution()
        resources.owner._setup_signal_subscriber()
        await resources.owner.stop()
        await resources.owner.stop()
        resources.assert_released()
    finally:
        await resources.cleanup()


@pytest.mark.parametrize("failure_kind", ["close", "destroy"])
async def test_explicit_stop_reports_cleanup_failure_without_retry(
    resources: ResourceHarness, failure_kind: str
) -> None:
    """Without an operational primary, cleanup failure is observable after other cleanup.

    Given: Idle setup owns both contexts and one cleanup operation fails,
    When: Explicit stop runs without an earlier operational failure,
    Then: Cleanup failure is observable and the other context is attempted safely.
    """
    resources.owner._setup_external_execution()
    resources.owner._setup_signal_subscriber()
    if failure_kind == "close":
        resources.close_failure = 1
    else:
        resources.destroy_failure = 1
    try:
        result = await outcome(resources.spawn(resources.owner.stop()))
        assert result is resources.cleanup_error
        publisher, subscriber = resources.contexts
        assert publisher.closed
        assert not subscriber.closed
        assert_failed_cleanup_attempts(subscriber, failure_kind)
        assert resources.owner.zmq_context is subscriber
        if failure_kind == "close":
            assert resources.owner.signal_subscriber is not None
    finally:
        await resources.cleanup()


def assert_failed_cleanup_attempts(context: ResourceContext, failure_kind: str) -> None:
    """Prove one actual failed attempt without permitting unsafe termination.

    Args:
        context: Context whose cleanup operation was injected to fail.
        failure_kind: Raw close or final context-termination failure.
    """
    assert not context.closed
    assert context.destroy_attempts <= 1
    assert not context.unsafe_terms
    assert len(context.sockets) == 1
    socket = context.sockets[0]
    assert socket.close_attempts == 1
    if failure_kind == "close":
        assert not socket.closed
        assert context.term_attempts == 0
    else:
        assert socket.closed
        assert context.term_attempts == 1


@pytest.mark.parametrize("failure_kind", ["close", "destroy"])
@pytest.mark.parametrize("operational_primary", [False, True])
async def test_repeated_stop_retains_cleanup_failure_without_retry(
    resources: ResourceHarness,
    monkeypatch: pytest.MonkeyPatch,
    failure_kind: str,
    operational_primary: bool,
) -> None:
    """Retain an observable failed cleanup across later explicit stop calls.

    Given: One owned context fails cleanup, with or without an operational primary,
    When: Further explicit stop callers observe the same coordinator lifetime,
    Then: The cleanup error remains identical and no close or term is retried.
    """
    if failure_kind == "close":
        resources.close_failure = 1
    else:
        resources.destroy_failure = 1
    monkeypatch.setattr(
        resources.owner,
        "_recover_engine_state_with_retry",
        AsyncMock(side_effect=resources.primary),
    )
    try:
        if operational_primary:
            assert await outcome(resources.spawn(resources.owner.start())) is resources.primary
        else:
            resources.owner._setup_external_execution()
            resources.owner._setup_signal_subscriber()
            assert await outcome(resources.spawn(resources.owner.stop())) is resources.cleanup_error
        publisher, subscriber = resources.contexts
        assert publisher.closed
        assert_failed_cleanup_attempts(subscriber, failure_kind)
        assert resources.owner.zmq_context is subscriber
        if failure_kind == "close":
            assert resources.owner.signal_subscriber is not None
        for _ in range(2):
            assert await outcome(resources.spawn(resources.owner.stop())) is resources.cleanup_error
            assert_failed_cleanup_attempts(subscriber, failure_kind)
        assert resources.owner.zmq_context is subscriber
        if failure_kind == "close":
            assert resources.owner.signal_subscriber is not None
    finally:
        await resources.cleanup()


async def test_later_stop_does_not_rethrow_cleaned_up_operational_error(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Separate the failed owner's outcome from a later harmless stop request.

    Given: Recovery raises an operational error and automatic cleanup succeeds,
    When: A later external caller requests stop twice,
    Then: Stop succeeds without rethrowing the old operational primary.
    """
    monkeypatch.setattr(
        resources.owner,
        "_recover_engine_state_with_retry",
        AsyncMock(side_effect=resources.primary),
    )
    try:
        assert await outcome(resources.spawn(resources.owner.start())) is resources.primary
        resources.assert_released()
        for _ in range(2):
            assert await outcome(resources.spawn(resources.owner.stop())) is None
        resources.assert_released()
    finally:
        await resources.cleanup()


async def test_owned_child_can_request_stop_without_join_cycle(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An owned worker can request shutdown without awaiting its own join.

    Given: Actual intake and funding run beside a health child that can request stop,
    When: That child awaits the coordinator's stop method,
    Then: Owner cleanup completes and all children finish before socket closure.
    """
    request = asyncio.Event()
    health = resources.workers[0]

    async def requesting_child() -> None:
        """Request stop as an actual owned child and record finalization."""
        resources.record_child()
        health.entered.set()
        try:
            await request.wait()
            await resources.owner.stop()
        finally:
            health.finished = True
            resources.events.append(health.name)

    monkeypatch.setattr(resources.owner, "_signal_health_monitor", requesting_child)
    try:
        owner = await resources.running()
        request.set()
        result = await outcome(owner)
        assert result is None or isinstance(result, asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_owner_can_request_stop_during_recovery(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A direct owner stop request ends startup without awaiting the owner itself.

    Given: Actual start is awaiting an inert recovery operation,
    When: Recovery asks the coordinator to stop from its own owner task,
    Then: Startup ends without beginning intake and acquired resources close.
    """

    async def recovery() -> None:
        """Request shutdown from the owner task itself."""
        await resources.owner.stop()

    monkeypatch.setattr(resources.owner, "_recover_engine_state_with_retry", recovery)
    try:
        result = await outcome(resources.spawn(resources.owner.start()))
        assert result is None or isinstance(result, asyncio.CancelledError)
        assert not resources.listening.is_set()
        resources.assert_released()
    finally:
        await resources.cleanup()


async def test_cancelled_stop_caller_waits_and_keeps_first_cancellation(
    resources: ResourceHarness,
) -> None:
    """Cancellation of an external stop caller does not abandon owned teardown.

    Given: Stop has cancelled a healthy owner's children and one finalizer is held,
    When: The stop caller receives three cancellation requests before release,
    Then: It waits for joined resource cleanup and propagates its first cancellation.
    """
    worker = resources.workers[0]
    worker.release.clear()
    try:
        owner = await resources.running()
        stopping = resources.spawn(resources.owner.stop())
        await asyncio.wait_for(worker.finalizing.wait(), 1)
        for message in (
            "first stop cancellation",
            "second stop cancellation",
            "third stop cancellation",
        ):
            stopping.cancel(message)
            await rendezvous()
            assert not stopping.done()
            assert not worker.interrupted
            assert not any(
                socket.closed for context in resources.contexts for socket in context.sockets
            )
        worker.release.set()
        result = await outcome(stopping)
        assert isinstance(result, asyncio.CancelledError)
        assert result.args == ("first stop cancellation",)
        owner_result = await outcome(owner)
        assert owner_result is None or isinstance(owner_result, asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_ambient_handled_exception_cannot_hide_cleanup_failure(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ambient exception handling is not an operational primary for this lifetime.

    Given: The caller is handling an unrelated error and the coordinator loop returns,
    When: The coordinator's own resource cleanup fails,
    Then: Its cleanup error is raised rather than suppressed by the ambient exception.
    """
    ambient = RuntimeError("unrelated caller exception")
    resources.close_failure = 1
    monkeypatch.setattr(resources.owner, "_run_trading_loop", AsyncMock())
    try:
        try:
            raise ambient
        except RuntimeError:
            with pytest.raises(RuntimeError) as raised:
                await resources.owner.start()
        assert raised.value is resources.cleanup_error
        assert resources.contexts[0].closed
        assert_failed_cleanup_attempts(resources.contexts[1], "close")
    finally:
        await resources.cleanup()


@pytest.mark.parametrize("after_completion", [False, True])
async def test_duplicate_start_rejected_without_replacing_lifetime(
    resources: ResourceHarness, after_completion: bool
) -> None:
    """A coordinator instance owns one start lifetime and rejects a second start.

    Given: A normally constructed coordinator has started once,
    When: Start is called again while running or after that lifetime completes,
    Then: The second call fails without acquiring resources or disrupting the first.
    """
    try:
        owner = await resources.running()
        if after_completion:
            owner.cancel("finish first lifetime")
            assert isinstance(await outcome(owner), asyncio.CancelledError)
            resources.assert_released()
        assert isinstance(await outcome(resources.spawn(resources.owner.start())), RuntimeError)
        assert len(resources.contexts) == 2
        if not after_completion:
            assert not owner.done()
            resources.marker.clear()
            await resources.frames.put([b"system.settings", b"{}"])
            await asyncio.wait_for(resources.marker.wait(), 1)
            owner.cancel("finish first lifetime")
            assert isinstance(await outcome(owner), asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_first_start_refused_while_idle_cleanup_is_pending(
    resources: ResourceHarness,
) -> None:
    """A pending idle stop cannot be overwritten by the first start.

    Given: An idle stop has been scheduled immediately before the first start,
    When: Start runs before the scheduled cleanup task can finish,
    Then: It refuses without acquisition, and a later first start remains usable.
    """
    try:
        stopping = resources.spawn(resources.owner.stop())
        refused = resources.spawn(resources.owner.start())
        result = await outcome(refused)
        assert isinstance(result, RuntimeError)
        assert not resources.contexts
        assert await outcome(stopping) is None
        owner = await resources.running()
        owner.cancel("finish first permitted start")
        assert isinstance(await outcome(owner), asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_first_start_retains_failed_idle_cleanup(resources: ResourceHarness) -> None:
    """An unsuccessful idle cleanup blocks start without losing owned resources.

    Given: Idle setup owns sockets and stop fails to close the subscriber socket,
    When: The first start is requested after that failed cleanup,
    Then: The same cleanup error escapes without new contexts or an implicit retry.
    """
    resources.owner._setup_external_execution()
    resources.owner._setup_signal_subscriber()
    resources.close_failure = 1
    try:
        assert await outcome(resources.spawn(resources.owner.stop())) is resources.cleanup_error
        failed_context = resources.contexts[1]
        assert await outcome(resources.spawn(resources.owner.start())) is resources.cleanup_error
        assert len(resources.contexts) == 2
        assert resources.owner.zmq_context is failed_context
        assert resources.owner.signal_subscriber is not None
        assert_failed_cleanup_attempts(failed_context, "close")
        assert not resources.listening.is_set()
    finally:
        await resources.cleanup()


async def test_stop_caller_cancellation_at_cleanup_completion_is_preserved(
    resources: ResourceHarness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Completion in the same turn cannot swallow the stop caller's cancellation.

    Given: Idle stop is awaiting context cleanup on the same event loop,
    When: Final context termination cancels the stop caller just before returning,
    Then: All resources close and that caller still receives its first cancellation.
    """
    resources.owner._setup_external_execution()
    resources.owner._setup_signal_subscriber()
    context = resources.contexts[0]
    actual_term = context.term

    def terminate_and_cancel_caller() -> None:
        """Cancel the awaiting caller at the final synchronous resource boundary."""
        actual_term()
        stopping.cancel("cancel at cleanup completion")

    monkeypatch.setattr(context, "term", terminate_and_cancel_caller)
    try:
        stopping = resources.spawn(resources.owner.stop())
        result = await outcome(stopping)
        assert isinstance(result, asyncio.CancelledError)
        assert result.args == ("cancel at cleanup completion",)
        resources.assert_released()
        assert await outcome(resources.spawn(resources.owner.stop())) is None
    finally:
        await resources.cleanup()


async def test_stop_does_not_repeat_pending_owner_cancellation(resources: ResourceHarness) -> None:
    """Stop joins an already requested owner cancellation without requesting another.

    Given: Healthy intake owns three started children and receives caller cancellation,
    When: External stop is awaited immediately before cancellation can be delivered,
    Then: The original request remains the only cancellation and cleanup joins all children.
    """
    try:
        owner = await resources.running()
        owner.cancel("original pending owner cancellation")
        await resources.owner.stop()
        result = await outcome(owner)
        assert isinstance(result, asyncio.CancelledError)
        assert result.args == ("original pending owner cancellation",)
        assert owner.cancelling() == 1
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_handled_earlier_cancellation_does_not_disable_external_stop(
    resources: ResourceHarness,
) -> None:
    """A handled cancellation from earlier work does not prevent this lifetime stopping.

    Given: A task handles cancellation before directly awaiting coordinator start,
    When: Actual intake becomes healthy and another task requests stop,
    Then: Stop terminates this lifetime and joins children before releasing resources.
    """
    prior_work_entered = asyncio.Event()

    async def continued_owner() -> None:
        """Handle cancellation from prior work without altering task counters."""
        try:
            prior_work_entered.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        await resources.owner.start()

    try:
        owner = resources.spawn(continued_owner())
        await asyncio.wait_for(prior_work_entered.wait(), 1)
        owner.cancel("handled cancellation from prior work")
        await asyncio.wait_for(resources.listening.wait(), 1)
        for worker in resources.workers:
            await asyncio.wait_for(worker.entered.wait(), 1)
        await resources.frames.put([b"system.settings", b"{}"])
        await asyncio.wait_for(resources.marker.wait(), 1)
        assert owner.cancelling() == 1
        stopping = resources.spawn(resources.owner.stop())
        assert await outcome(stopping) is None
        result = await outcome(owner)
        assert result is None or isinstance(result, asyncio.CancelledError)
        resources.assert_released()
        resources.assert_joined_before_close()
    finally:
        await resources.cleanup()


async def test_pending_preentry_cancellation_prevents_resource_setup(
    resources: ResourceHarness,
) -> None:
    """A cancellation requested before entry is delivered before resource acquisition.

    Given: An owner requests its own cancellation immediately before awaiting start,
    When: Start enters its protected lifetime without any intervening suspension,
    Then: The first cancellation escapes and no sockets or workers are acquired.
    """

    async def cancelled_owner() -> None:
        """Request genuine task cancellation and immediately enter the coordinator."""
        current = asyncio.current_task()
        assert current is not None
        current.cancel("pending cancellation before start")
        await resources.owner.start()

    try:
        result = await outcome(resources.spawn(cancelled_owner()))
        assert isinstance(result, asyncio.CancelledError)
        assert result.args == ("pending cancellation before start",)
        assert not resources.contexts
        assert not resources.children
        assert not resources.listening.is_set()
    finally:
        await resources.cleanup()


async def test_external_stop_during_entry_after_handled_cancellation_skips_setup(
    resources: ResourceHarness,
) -> None:
    """An entry-time stop request survives the handled-cancellation boundary.

    Given: Earlier cancellation wakes a task that handles it and enters start,
    When: External stop runs in the next ready callback before startup proceeds,
    Then: The stop request is honored without acquiring sockets or starting children.
    """
    prior_work_entered = asyncio.Event()

    async def continued_owner() -> None:
        """Enter a new lifetime after handling an unrelated cancellation."""
        try:
            prior_work_entered.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        await resources.owner.start()

    try:
        owner = resources.spawn(continued_owner())
        await asyncio.wait_for(prior_work_entered.wait(), 1)
        owner.cancel("handled before entry-time stop")
        stopping = resources.spawn(resources.owner.stop())
        assert await outcome(stopping) is None
        assert isinstance(await outcome(owner), asyncio.CancelledError)
        assert not resources.contexts
        assert not resources.children
        assert not resources.listening.is_set()
    finally:
        await resources.cleanup()
