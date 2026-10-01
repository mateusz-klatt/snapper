"""Deterministic ownership and failure precedence for notification workers."""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from loguru import logger
from typer.testing import CliRunner

from snapper.application.notify.apns_client import ApnsClientPool
from snapper.application.notify.portfolio_drift_recovery import PortfolioDriftRecoveryScanner
from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.sidecar import NotifySidecar
from snapper.cli import app as cli_app
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber


@dataclass
class _Worker:
    """Expose worker entry, completion and independently gated finalization."""

    entered: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    finalizing: asyncio.Event = field(default_factory=asyncio.Event)
    finalized: asyncio.Event = field(default_factory=asyncio.Event)
    cleanup_release: asyncio.Event = field(default_factory=asyncio.Event)
    failure: RuntimeError | None = None
    cleanup_failure: RuntimeError | None = None
    stop_on_return: asyncio.Event | None = None

    async def run(self) -> None:
        """Wait for an explicit outcome and record completion of cleanup."""
        self.entered.set()
        try:
            await self.release.wait()
            if self.failure is not None:
                raise self.failure
        finally:
            self.finalizing.set()
            await self.cleanup_release.wait()
            self.finalized.set()
            if self.cleanup_failure is not None:
                raise self.cleanup_failure

    async def receive(self) -> tuple[str, bytes]:
        """Adapt the same controlled worker to the subscriber interface."""
        await self.run()
        if self.stop_on_return is not None:
            self.stop_on_return.set()
        return "unused", b""


@dataclass
class _Harness:
    """Hold the actual sidecar and injected workers without external services."""

    receive: _Worker = field(default_factory=_Worker)
    retry: _Worker = field(default_factory=_Worker)
    scanner: MagicMock = field(
        default_factory=lambda: MagicMock(spec=PortfolioDriftRecoveryScanner)
    )
    sidecar: NotifySidecar = field(init=False)
    start: asyncio.Task[None] = field(init=False)

    def configure(self) -> None:
        """Wire collaborators without starting the real sidecar lifecycle."""
        subscriber = MagicMock(spec=ValidatedSubscriber)
        subscriber.recv_multipart = self.receive.receive
        self.scanner.start = AsyncMock()
        self.scanner.stop = AsyncMock()
        self.sidecar = NotifySidecar(
            subscriber=cast(ValidatedSubscriber, subscriber),
            repo=cast(Repository, MagicMock(spec=Repository)),
            apns=cast(ApnsClientPool, MagicMock(spec=ApnsClientPool)),
            apns_topic="test.invalid",
            tracker=SequenceTracker(),
            publisher=cast(MessagePublisher, MagicMock(spec=MessagePublisher)),
            registry=RuleRegistry(),
            portfolio_drift_recovery_scanner=cast(PortfolioDriftRecoveryScanner, self.scanner),
        )
        self.sidecar._drain_outbox = AsyncMock()
        self.sidecar._process_retry_queue_loop = self.retry.run

    def build(self) -> None:
        """Configure the sidecar and start its actual lifecycle."""
        self.configure()
        self.start = asyncio.create_task(self.sidecar.start())

    async def cleanup(self, baseline: set[asyncio.Task[object]]) -> None:
        """Drain every new task even when the behavior under test leaks ownership."""
        self.receive.cleanup_release.set()
        self.retry.cleanup_release.set()
        self.sidecar._stop_event.set()
        if not self.start.done():
            self.start.cancel()
        await asyncio.gather(self.start, return_exceptions=True)
        remaining = asyncio.all_tasks() - baseline
        for task in remaining:
            if not task.done():
                task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)
        if self.sidecar._retry_task is not None:
            await asyncio.gather(self.sidecar._retry_task, return_exceptions=True)


@asynccontextmanager
async def _running(
    *, gated_cleanup: bool = False, receive_exits: bool = False
) -> AsyncIterator[_Harness]:
    """Rendezvous on both workers and always clean them after the assertion."""
    baseline = asyncio.all_tasks()
    harness = _Harness()
    if not gated_cleanup:
        harness.receive.cleanup_release.set()
        harness.retry.cleanup_release.set()
    harness.build()
    if receive_exits:
        harness.sidecar._receive_loop = harness.receive.run
    try:
        await asyncio.wait_for(harness.receive.entered.wait(), 1)
        await asyncio.wait_for(harness.retry.entered.wait(), 1)
        yield harness
    finally:
        await harness.cleanup(baseline)


async def _completed(task: asyncio.Task[None]) -> bool:
    """Bound a missing lifecycle reaction without using sleeps for synchronization."""
    done, _ = await asyncio.wait({task}, timeout=0.25)
    return task in done


@pytest.mark.asyncio
async def test_retry_failure_surfaces_while_receive_is_blocked() -> None:
    """A failed retry worker fails its owning start task.

    Given a blocked receive and a retry worker,
    When the retry worker raises a specific error,
    Then start raises that same error after owned cleanup completes.
    """
    async with _running() as harness:
        failure = RuntimeError("retry failed")
        harness.retry.failure = failure
        harness.retry.release.set()
        await asyncio.wait_for(harness.retry.finalized.wait(), 1)
        assert await _completed(harness.start), "start ignored failed retry worker"
        assert harness.start.exception() is failure
        assert harness.receive.finalized.is_set()
        harness.scanner.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_unexpected_retry_return_surfaces() -> None:
    """Unexpected successful retry completion cannot leave partial service running.

    Given blocked receive and no stop request,
    When retry returns normally,
    Then start raises a fatal lifecycle error.
    """
    async with _running() as harness:
        harness.retry.release.set()
        await asyncio.wait_for(harness.retry.finalized.wait(), 1)
        assert await _completed(harness.start), "start ignored unexpected retry exit"
        assert isinstance(harness.start.exception(), RuntimeError)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True], ids=["normal-stop", "external-cancellation"])
async def test_start_awaits_owned_finalizers(cancel: bool) -> None:
    """Start remains pending while owned task finalizers are blocked.

    Given blocked receive and retry tasks with gated finalizers,
    When stop or cancellation requests teardown,
    Then start waits for both finalizers before completing.
    """
    async with _running(gated_cleanup=True) as harness:
        if cancel:
            harness.start.cancel()
        else:
            asyncio.create_task(harness.sidecar.stop())
        finalizing = asyncio.create_task(harness.retry.finalizing.wait())
        try:
            done, _ = await asyncio.wait(
                {harness.start, finalizing}, timeout=1, return_when=asyncio.FIRST_COMPLETED
            )
            assert done, "teardown did not begin"
            assert not harness.start.done(), "start completed before owned finalizers"
            harness.receive.cleanup_release.set()
            harness.retry.cleanup_release.set()
            outcomes = await asyncio.wait_for(
                asyncio.gather(harness.start, return_exceptions=True), 1
            )
            assert isinstance(outcomes[0], asyncio.CancelledError) if cancel else outcomes == [None]
            assert harness.receive.finalized.is_set()
            assert harness.retry.finalized.is_set()
        finally:
            finalizing.cancel()
            await asyncio.gather(finalizing, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("fails", [False, True], ids=["successful-retry", "failed-retry"])
async def test_retry_outcome_wins_simultaneous_stop(fails: bool) -> None:
    """A stop request permits successful completion but cannot hide a retry failure.

    Given blocked receive and retry workers,
    When stop and retry completion become ready together,
    Then retry failure remains primary while successful retry allows normal stop.
    """
    async with _running() as harness:
        failure = RuntimeError("simultaneous retry failure") if fails else None
        harness.retry.failure = failure
        harness.retry.release.set()
        harness.sidecar._stop_event.set()
        assert await _completed(harness.start)
        assert harness.start.exception() is failure
        assert harness.receive.finalized.is_set()
        assert harness.retry.finalized.is_set()


@pytest.mark.asyncio
async def test_retry_failure_has_priority_over_simultaneous_receive_failure() -> None:
    """Simultaneous worker errors have stable retry-first precedence.

    Given receive and retry workers with distinct failures,
    When both outcomes are released without an intervening yield,
    Then start raises the retry error and retrieves both worker results.
    """
    async with _running() as harness:
        retry_failure = RuntimeError("primary retry error")
        harness.retry.failure = retry_failure
        harness.receive.failure = RuntimeError("secondary receive error")
        harness.receive.release.set()
        harness.retry.release.set()
        assert await _completed(harness.start)
        assert harness.start.exception() is retry_failure
        assert harness.receive.finalized.is_set()
        assert harness.retry.finalized.is_set()


@pytest.mark.asyncio
async def test_independent_retry_cancellation_propagates() -> None:
    """An independently cancelled retry is not a healthy service.

    Given an active sidecar without a stop request,
    When the retry task is cancelled independently,
    Then start propagates cancellation after joining all owned tasks.
    """
    async with _running() as harness:
        assert harness.sidecar._retry_task is not None
        harness.sidecar._retry_task.cancel()
        assert await _completed(harness.start)
        assert harness.start.cancelled()
        assert harness.receive.finalized.is_set()
        assert harness.retry.finalized.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True], ids=["retry-error", "external-cancellation"])
async def test_cleanup_failures_preserve_primary_outcome(cancel: bool) -> None:
    """Secondary teardown failures cannot replace the original failure or cancellation.

    Given scanner and receive cleanup that both fail,
    When retry fails or start is externally cancelled,
    Then all cleanup completes and the original outcome survives repeated stop.
    """
    async with _running() as harness:
        primary = RuntimeError("primary retry error")
        harness.receive.cleanup_failure = RuntimeError("secondary receive cleanup")
        harness.scanner.stop.side_effect = RuntimeError("secondary scanner cleanup")
        if cancel:
            harness.start.cancel()
        else:
            harness.retry.failure = primary
            harness.retry.release.set()
        assert await _completed(harness.start)
        if cancel:
            assert harness.start.cancelled()
        else:
            assert harness.start.exception() is primary
        assert harness.receive.finalized.is_set()
        assert harness.retry.finalized.is_set()
        harness.scanner.stop.assert_awaited_once()
        await harness.sidecar.stop()
        await harness.sidecar.stop()
        harness.scanner.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_normal_stop_reports_cleanup_failure_after_joining_workers() -> None:
    """A cleanup failure remains visible when there was no earlier primary error.

    Given healthy workers and a scanner whose stop fails,
    When normal stop is requested,
    Then start raises that cleanup error after both workers have finalized.
    """
    async with _running() as harness:
        failure = RuntimeError("scanner stop failed")
        harness.scanner.stop.side_effect = failure
        await harness.sidecar.stop()
        assert await _completed(harness.start)
        assert harness.start.exception() is failure
        assert harness.receive.finalized.is_set()
        assert harness.retry.finalized.is_set()
        await harness.sidecar.stop()
        harness.scanner.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_partial_scanner_start_failure_is_cleaned_once() -> None:
    """A partially started scanner is cleaned even before workers are spawned.

    Given scanner startup raises a specific error,
    When sidecar start fails and the CLI-style finally calls stop,
    Then the startup error survives and scanner cleanup runs exactly once.
    """
    baseline = asyncio.all_tasks()
    harness = _Harness()
    harness.build()
    failure = RuntimeError("scanner startup failed")
    harness.scanner.start.side_effect = failure
    try:
        with pytest.raises(RuntimeError) as caught:
            try:
                await harness.start
            finally:
                await harness.sidecar.stop()
        assert caught.value is failure
        harness.scanner.stop.assert_awaited_once()
        assert not harness.retry.entered.is_set()
        assert not harness.receive.entered.is_set()
    finally:
        await harness.cleanup(baseline)


def dispatch_call(worker: _Worker) -> Callable[[str, bytes, datetime], Awaitable[None]]:
    """Adapt a controlled worker to the timestamped dispatch interface."""

    async def dispatch(_topic: str, _payload: bytes, _now: datetime) -> None:
        """Block dispatch until explicitly released or cancelled."""
        await worker.run()

    return dispatch


@pytest.mark.asyncio
async def test_retry_failure_interrupts_blocked_dispatch() -> None:
    """Retry supervision remains active during message dispatch.

    Given the receive worker is blocked inside a dispatched message,
    When the retry worker fails,
    Then start raises that failure after the dispatch finalizer completes.
    """
    async with _running() as harness:
        dispatch = _Worker()
        dispatch.cleanup_release.set()
        harness.sidecar._dispatch = AsyncMock(side_effect=dispatch_call(dispatch))
        harness.receive.release.set()
        await asyncio.wait_for(dispatch.entered.wait(), 1)
        failure = RuntimeError("retry failed during dispatch")
        harness.retry.failure = failure
        harness.retry.release.set()
        assert await _completed(harness.start)
        assert harness.start.exception() is failure
        assert dispatch.finalized.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_primary", [False, True])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_cancellation_during_scanner_cleanup_preserves_primary(
    has_primary: bool, cleanup_fails: bool
) -> None:
    """Cancellation during cleanup preserves the established primary outcome.

    Given normal stop or retry failure has entered blocked scanner cleanup,
    When start is cancelled and scanner cleanup finishes or fails,
    Then start joins cleanup and preserves retry failure or the new cancellation.
    """
    async with _running() as harness:
        entered = asyncio.Event()
        release = asyncio.Event()
        finished = asyncio.Event()

        async def stop_scanner() -> None:
            """Expose scanner cleanup that fails after its release rendezvous."""
            entered.set()
            await release.wait()
            finished.set()
            if cleanup_fails:
                raise RuntimeError("scanner cleanup after cancellation")

        harness.scanner.stop.side_effect = stop_scanner
        primary = RuntimeError("retry error before cancellation")
        try:
            if has_primary:
                harness.retry.failure = primary
                harness.retry.release.set()
            else:
                await harness.sidecar.stop()
            await asyncio.wait_for(entered.wait(), 1)
            harness.start.cancel()
            release.set()
            assert await _completed(harness.start)
            if has_primary:
                assert harness.start.exception() is primary
            else:
                assert harness.start.cancelled()
            assert finished.is_set()
            assert harness.receive.finalized.is_set()
            assert harness.retry.finalized.is_set()
            await harness.sidecar.stop()
            harness.scanner.stop.assert_awaited_once()
        finally:
            release.set()


def _stub_cli_dependencies(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace every CLI transport and configuration boundary with inert collaborators."""
    bootstrap = SimpleNamespace(
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xsub="tcp://127.0.0.1:7500",
        zmq_broker_xpub="tcp://127.0.0.1:7501",
    )
    apns_config = SimpleNamespace(topic="test.invalid", environment="sandbox")
    monkeypatch.setattr(cli_app, "get_bootstrap_settings", lambda: bootstrap)
    monkeypatch.setattr(cli_app, "get_repository", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(cli_app, "get_settings_service", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr(cli_app, "load_apns_config", MagicMock(return_value=apns_config))
    monkeypatch.setattr(cli_app, "build_apns_client_pool", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(cli_app, "ValidatedSubscriber", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(cli_app, "apply_hwm", MagicMock())
    context = MagicMock()
    monkeypatch.setattr(cli_app.zmq.asyncio, "Context", MagicMock(return_value=context))
    return context


def test_cli_final_stop_preserves_retry_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """The actual CLI finally cannot rethrow an already-observed scanner cleanup failure.

    Given an actual sidecar whose retry and scanner cleanup both fail,
    When the notify CLI awaits start and subsequently calls stop in its finally,
    Then the retry error remains visible and scanner cleanup runs only once.
    """
    context = _stub_cli_dependencies(monkeypatch)
    harness = _Harness()
    primary = RuntimeError("CLI primary retry failure")

    def make_sidecar(**_kwargs: object) -> NotifySidecar:
        """Inject controlled workers while retaining the actual sidecar implementation."""
        harness.configure()
        harness.receive.cleanup_release.set()
        harness.retry.cleanup_release.set()
        harness.retry.failure = primary
        harness.retry.release.set()
        harness.scanner.stop.side_effect = RuntimeError("CLI secondary scanner failure")
        return harness.sidecar

    monkeypatch.setattr(cli_app, "NotifySidecar", make_sidecar)
    result = CliRunner().invoke(cli_app.app, ["notify"])
    assert result.exit_code == 1
    assert result.exception is primary
    harness.scanner.stop.assert_awaited_once()
    assert harness.retry.finalized.is_set()
    context.term.assert_called_once()
    assert context.socket.return_value.close.call_count == 2


@pytest.mark.asyncio
async def test_unexpected_receive_worker_return_surfaces() -> None:
    """An unexpected receive worker exit is fatal independently of retry health.

    Given an active retry worker and no stop request,
    When the receive worker returns normally,
    Then start raises a receive lifecycle error and joins retry.
    """
    async with _running(receive_exits=True) as harness:
        harness.receive.release.set()
        assert await _completed(harness.start)
        failure = harness.start.exception()
        assert isinstance(failure, RuntimeError)
        assert "receive" in str(failure)
        assert harness.retry.finalized.is_set()


@pytest.mark.asyncio
async def test_secondary_cleanup_diagnostics_exclude_exception_payloads(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Cleanup diagnostics expose stage and class without exception payloads.

    Given secondary errors containing device-token and database-DSN sentinels,
    When retry failure triggers receive and scanner cleanup,
    Then diagnostics contain both stages but neither secret nor a traceback.
    """
    token = "device-token-secret-sentinel"
    dsn = "postgresql://private-user:private-password@private-host/database"
    handler = logger.add(caplog.handler, format="{message}", level="ERROR")
    try:
        async with _running() as harness:
            harness.receive.cleanup_failure = RuntimeError(token)
            harness.scanner.stop.side_effect = RuntimeError(dsn)
            harness.retry.failure = RuntimeError("primary retry failure")
            harness.retry.release.set()
            assert await _completed(harness.start)
            assert harness.start.exception() is harness.retry.failure
    finally:
        logger.remove(handler)
    messages = [record.getMessage() for record in caplog.records]
    assert any("stage=worker" in message and "RuntimeError" in message for message in messages)
    assert any("stage=scanner" in message and "RuntimeError" in message for message in messages)
    assert all(token not in message and dsn not in message for message in messages)
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.asyncio
async def test_stop_arriving_with_received_frame_prevents_dispatch() -> None:
    """A frame received during shutdown cannot start new dispatch work.

    Given a receive already in flight,
    When stop becomes set before that receive returns its frame,
    Then start joins workers without dispatching the received payload.
    """
    async with _running() as harness:
        dispatch = AsyncMock()
        harness.sidecar._dispatch = dispatch
        harness.receive.stop_on_return = harness.sidecar._stop_event
        harness.receive.release.set()
        assert await _completed(harness.start)
        assert harness.start.exception() is None
        dispatch.assert_not_awaited()
        assert harness.retry.finalized.is_set()
