"""Behavioural tests for a whole runner under server-authoritative control.

The control gate is proven in isolation elsewhere. What these tests pin is what
a complete runner does with it: that it stays inert until the server grants
consult duty, that a hold stops work rather than merely recording an intention,
that holding never looks like dying, and that neither a reconnect nor a replayed
frame can hand duty back to a runner nobody re-authorized.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from pydantic import SecretStr

from snapper_delegate.consult import BoundedConsultRunner
from snapper_delegate.consult import ConsultOutcome
from snapper_delegate.consult import ConsultResult
from snapper_delegate.consult import ReviewConsultContext
from snapper_delegate.control_plane import PendingReview
from snapper_delegate.control_plane import WsToken
from snapper_delegate.delegate_control import ControlDirective
from snapper_delegate.delegate_control import ControlState
from snapper_delegate.delegate_control import control_topic
from snapper_delegate.review_inbox import InboxOfferOutcome
from snapper_delegate.review_inbox import ReviewInbox
from snapper_delegate.runner import DelegateRunner
from snapper_delegate.runner import RunnerRuntime
from snapper_delegate.runner import RunnerTuning
from snapper_delegate.wake_client import AiReviewRequestFrame
from snapper_delegate.wake_client import WakeCallbacks
from snapper_delegate.wake_client import WakeClient

_NOW = datetime(2026, 8, 3, 12, 0, tzinfo=UTC)
_FUTURE = _NOW + timedelta(minutes=5)

type _WakeAction = Callable[[WakeCallbacks], Awaitable[None]]


class _FakeControlClient:
    """Answer identity and catch-up requests without a control plane."""

    def __init__(self, pending: list[PendingReview] | None = None) -> None:
        """Retain the catch-up snapshot every sweep observes."""
        self.pending = list(pending or [])
        self.pending_calls = 0

    async def fetch_delegate_identity(self) -> str:
        """Return the identity this runner authenticated as.

        Returns:
            The public identifier the runner works under.
        """
        return "delegate-own"

    async def list_pending_reviews(self, limit: int = 100) -> list[PendingReview]:
        """Return the catch-up snapshot and record that a sweep happened.

        Args:
            limit: Maximum number of rows the runner asked for.

        Returns:
            The scripted pending reviews.
        """
        del limit
        self.pending_calls += 1
        return list(self.pending)

    async def mint_ws_token(self) -> WsToken:
        """Return one harmless one-shot WebSocket credential.

        Returns:
            A credential no fake transport ever presents.
        """
        return WsToken(SecretStr("ws-token"), _FUTURE)

    def read_access_token(self) -> SecretStr:
        """Return one harmless bearer credential.

        Returns:
            A token no fake transport ever presents.
        """
        return SecretStr("access-token")

    async def aclose(self) -> None:
        """Close nothing, because this double owns no resources."""


class _FakeWakeClient:
    """Run scripted wake sessions and expose prompt closure."""

    def __init__(self, actions: list[_WakeAction]) -> None:
        """Retain the sessions this client runs in order."""
        self.actions = list(actions)
        self.closed = asyncio.Event()
        self.run_calls = 0

    async def run(self, callbacks: WakeCallbacks) -> None:
        """Run the next scripted session or wait for closure.

        Args:
            callbacks: Lifecycle hooks the runner supplied.
        """
        self.run_calls += 1
        if self.actions:
            await self.actions.pop(0)(callbacks)
            return
        await self.closed.wait()

    async def close(self) -> None:
        """Release every wait blocked on closure."""
        self.closed.set()


class _RecordingConsultRunner:
    """Record consulted reviews and optionally hold one consult open."""

    def __init__(self, gate: asyncio.Event | None = None) -> None:
        """Retain the optional gate that defers one in-flight consult."""
        self.reviews: list[str] = []
        self.started = asyncio.Event()
        self._gate = gate

    async def run(self, context: ReviewConsultContext) -> ConsultResult:
        """Record one consult and submit once any in-flight gate releases.

        Args:
            context: The review context the runner handed over.

        Returns:
            A submitted consult outcome.
        """
        self.reviews.append(context.review_public_id)
        self.started.set()
        if self._gate is not None:
            await self._gate.wait()
        return ConsultResult(ConsultOutcome.SUBMITTED)


def _pending(review_id: str) -> PendingReview:
    """Build one selected pending catch-up row.

    Args:
        review_id: Public identifier for the pending review.

    Returns:
        A row the catch-up sweep would offer to the inbox.
    """
    return PendingReview(
        review_public_id=review_id,
        selected_delegate_public_id="delegate-own",
        wallet_public_id=f"wallet-{review_id}",
        dispatch_version=1,
        status="pending",
        deadline=_FUTURE,
        fanout_after=_NOW - timedelta(seconds=1),
        instrument=None,
        signal_envelope=None,
    )


def _request(review_id: str) -> AiReviewRequestFrame:
    """Build one selected WebSocket review request.

    Args:
        review_id: Public identifier for the requested review.

    Returns:
        A wake addressed to this runner.
    """
    return AiReviewRequestFrame(
        type="ai_review.request",
        session_id="session-1",
        sequence_id=1,
        public_id=f"public-{review_id}",
        timestamp=_NOW,
        topic="ai_reviews.request",
        review_public_id=review_id,
        user_public_id="user-1",
        strategy_public_id="strategy-1",
        wallet_public_id=f"wallet-{review_id}",
        instrument_public_id="instrument-1",
        selected_delegate_public_id="delegate-own",
        deadline=_FUTURE,
        signal_envelope={"side": "buy"},
        instrument_metadata={"symbol": "BTC/USD"},
        dispatch_version=2,
    )


def _context(review_id: str) -> ReviewConsultContext:
    """Build one selected consult context ready for immediate work.

    Args:
        review_id: Public identifier for the queued review.

    Returns:
        A context the inbox queues without a fanout hold.
    """
    return ReviewConsultContext(
        review_public_id=review_id,
        selected_delegate_public_id="delegate-own",
        wallet_public_id=f"wallet-{review_id}",
        dispatch_version=1,
        deadline=_FUTURE,
        fanout_after=_NOW - timedelta(seconds=1),
        signal_envelope={"side": "buy"},
        instrument_metadata={"symbol": "BTC/USD"},
    )


def _runner(
    control: _FakeControlClient,
    wake: _FakeWakeClient,
    consult: _RecordingConsultRunner,
) -> DelegateRunner:
    """Build one configured runner whose consult path only records work.

    Args:
        control: Control-plane double answering identity and catch-up.
        wake: Wake-boundary double running scripted sessions.
        consult: Consult double standing in for the bounded model path.

    Returns:
        A runner ready to start against hermetic seams.
    """
    runner = DelegateRunner(
        model_alias="review-model",
        base_url="https://model.invalid",
        api_key_file="/missing/model-key",
        delegate_token_file="/missing/delegate-token",
        max_tool_rounds=3,
        snapper_base_url="https://snapper.invalid",
        runtime=RunnerRuntime(
            control_client=control,
            wake_client=wake,
            clock=lambda: _NOW,
            tuning=RunnerTuning(identity_backoff_base_seconds=0.0),
        ),
    )
    runner._consult_runner = cast(BoundedConsultRunner, consult)
    return runner


async def _grant_duty(callbacks: WakeCallbacks, revision: int) -> None:
    """Subscribe one session and grant it consult duty at one revision.

    Args:
        callbacks: Lifecycle hooks the runner supplied to the wake boundary.
        revision: Monotonic control revision the server states for this grant.
    """
    await callbacks.connection_state(True)
    await callbacks.subscribed()
    assert callbacks.control is not None
    await callbacks.control(ControlDirective(ControlState.ACTIVE, revision))


async def _wait_until(predicate: Callable[[], bool]) -> None:
    """Yield boundedly until one deterministic condition becomes true.

    Args:
        predicate: Condition the runner is expected to reach.
    """
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


@pytest.mark.asyncio
async def test_a_runner_that_hears_no_directive_does_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cold boot stays held until the server states what the runner may do.

    Given a session that subscribes and wakes the runner but states no control,
    When the runner runs that whole session,
    Then it sweeps no catch-up work, consults nothing, and refuses the wake, so
    a restart can never spend a model call on duty nobody granted.
    """
    control = _FakeControlClient([_pending("review-catch-up")])
    consult = _RecordingConsultRunner()
    accepted: list[bool] = []

    async def _silent_session(callbacks: WakeCallbacks) -> None:
        """Subscribe and wake the runner without ever stating control."""
        await callbacks.connection_state(True)
        await callbacks.subscribed()
        await callbacks.heartbeat()
        accepted.append(await callbacks.frame(_request("review-cold-boot")))
        await wake.closed.wait()

    wake = _FakeWakeClient([_silent_session])
    runner = _runner(control, wake, consult)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())

    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["consults_skipped"] == 1)
    status = runner.get_status()
    await runner.stop()
    await task

    assert accepted == [False]
    assert control.pending_calls == 0
    assert consult.reviews == []
    assert status["consults_processed"] == 0
    assert status["accepts_consults"] is False


@pytest.mark.asyncio
async def test_a_hold_finishes_current_work_and_drops_what_is_queued(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hold is drain-current and accept-nothing-new rather than a hard stop.

    Given one consult in flight and another queued behind it under granted duty,
    When the server holds the runner,
    Then the in-flight consult still finishes, the queued review is dropped
    without ever being consulted, and the next wake is refused.
    """
    gate = asyncio.Event()
    consult = _RecordingConsultRunner(gate)
    control = _FakeControlClient()
    accepted: list[bool] = []

    async def _holding_session(callbacks: WakeCallbacks) -> None:
        """Queue work behind an in-flight consult, then hold the runner."""
        await _grant_duty(callbacks, 1)
        await callbacks.frame(_request("review-in-flight"))
        await consult.started.wait()
        await callbacks.frame(_request("review-queued"))
        assert callbacks.control is not None
        await callbacks.control(ControlDirective(ControlState.ON_HOLD, 2))
        accepted.append(await callbacks.frame(_request("review-after-hold")))
        gate.set()
        await wake.closed.wait()

    wake = _FakeWakeClient([_holding_session])
    runner = _runner(control, wake, consult)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())

    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["consults_processed"] == 1)
    status = runner.get_status()
    await runner.stop()
    await task

    assert consult.reviews == ["review-in-flight"]
    assert accepted == [False]
    assert status["control_state"] == "on_hold"
    assert status["accepts_consults"] is False


@pytest.mark.asyncio
async def test_a_held_runner_keeps_proving_that_it_is_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Holding a runner pauses its duty without faking a dead delegate.

    Given a runner the server has explicitly held,
    When its session keeps pinging and delivers a consult wake,
    Then heartbeats keep counting on a connected, subscribed socket while the
    wake is refused, because eligibility keys on liveness and resume must be
    instant rather than waiting for a reconnect.
    """
    control = _FakeControlClient()
    consult = _RecordingConsultRunner()
    accepted: list[bool] = []

    async def _held_session(callbacks: WakeCallbacks) -> None:
        """Hold the runner and then keep the liveness ping going."""
        await callbacks.connection_state(True)
        await callbacks.subscribed()
        assert callbacks.control is not None
        await callbacks.control(ControlDirective(ControlState.ON_HOLD, 4))
        await callbacks.heartbeat()
        accepted.append(await callbacks.frame(_request("review-while-held")))
        await callbacks.heartbeat()
        await callbacks.heartbeat()
        await wake.closed.wait()

    wake = _FakeWakeClient([_held_session])
    runner = _runner(control, wake, consult)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())

    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["heartbeat_count"] == 3)
    status = runner.get_status()
    await runner.stop()
    await task

    assert accepted == [False]
    assert status["ws_connected"] is True
    assert status["state"] == "subscribed"
    assert status["control_revision"] == 4
    assert consult.reviews == []
    assert control.pending_calls == 0


@pytest.mark.asyncio
async def test_a_reconnect_holds_the_runner_until_control_is_restated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A runner that was working does not resume itself after a reconnect.

    Given a runner granted duty on one session that then loses its socket,
    When the next session subscribes without control and only later grants it,
    Then the wake arriving before the fresh directive is refused and only the
    one after it is consulted, because the state may have changed while the
    runner was not listening.
    """
    control = _FakeControlClient()
    consult = _RecordingConsultRunner()
    accepted: list[bool] = []

    async def _first_session(callbacks: WakeCallbacks) -> None:
        """Work under granted duty, then lose the connection."""
        await _grant_duty(callbacks, 1)
        await callbacks.frame(_request("review-before-drop"))
        await _wait_until(lambda: runner.get_status()["consults_processed"] == 1)
        await callbacks.connection_state(False)

    async def _second_session(callbacks: WakeCallbacks) -> None:
        """Reconnect with unknown control, then receive a fresh grant."""
        await callbacks.connection_state(True)
        await callbacks.subscribed()
        assert callbacks.control is not None
        await callbacks.control(None)
        accepted.append(await callbacks.frame(_request("review-while-unknown")))
        await callbacks.control(ControlDirective(ControlState.ACTIVE, 2))
        await callbacks.frame(_request("review-after-regrant"))
        await wake.closed.wait()

    wake = _FakeWakeClient([_first_session, _second_session])
    runner = _runner(control, wake, consult)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())

    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["consults_processed"] == 2)
    await runner.stop()
    await task

    assert accepted == [False]
    assert consult.reviews == ["review-before-drop", "review-after-regrant"]
    assert control.pending_calls == 2


@pytest.mark.asyncio
async def test_a_stale_directive_cannot_resume_a_held_runner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control only moves forward, and the runner reports what is really in force.

    Given a runner held at revision five,
    When a delayed grant from revision four arrives,
    Then the runner stays held, answers with the hold rather than the frame it
    just saw, runs no catch-up for the superseded grant, and refuses the wake
    that follows.
    """
    control = _FakeControlClient()
    consult = _RecordingConsultRunner()
    echoed: list[ControlDirective | None] = []
    accepted: list[bool] = []

    async def _replaying_session(callbacks: WakeCallbacks) -> None:
        """Hold the runner, then replay a superseded grant behind it."""
        await _grant_duty(callbacks, 2)
        await _wait_until(lambda: control.pending_calls == 1)
        assert callbacks.control is not None
        await callbacks.control(ControlDirective(ControlState.ON_HOLD, 5))
        echoed.append(await callbacks.control(ControlDirective(ControlState.ACTIVE, 4)))
        accepted.append(await callbacks.frame(_request("review-after-stale")))
        await wake.closed.wait()

    wake = _FakeWakeClient([_replaying_session])
    runner = _runner(control, wake, consult)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())

    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["consults_skipped"] == 1)
    status = runner.get_status()
    await runner.stop()
    await task

    assert echoed == [ControlDirective(ControlState.ON_HOLD, 5)]
    assert accepted == [False]
    assert control.pending_calls == 1
    assert consult.reviews == []
    assert status["control_revision"] == 5


@pytest.mark.asyncio
async def test_control_events_before_the_inbox_exists_leave_the_runner_held() -> None:
    """Directives need no queue to exist in order to bind the runner.

    Given a runner whose inbox has not been built yet,
    When its socket drops and a hold arrives with no queue to drain,
    Then both are absorbed without failure and the first wake after the inbox
    appears is still refused, so an early directive cannot be lost by arriving
    before the queue it would have drained.
    """
    runner = _runner(_FakeControlClient(), _FakeWakeClient([]), _RecordingConsultRunner())

    await runner._on_connection_state(False)
    applied = await runner._on_control(ControlDirective(ControlState.ON_HOLD, 1))
    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW)
    runner._inbox = inbox
    accepted = await runner._on_wake_frame(_request("review-early"))
    await inbox.close()

    assert applied == ControlDirective(ControlState.ON_HOLD, 1)
    assert accepted is False
    assert runner.get_status()["consults_skipped"] == 1


@pytest.mark.asyncio
async def test_work_that_outlives_its_session_is_dropped_before_the_model_runs() -> None:
    """Duty is re-checked when work leaves the queue, not only when it enters.

    Given a review queued under granted duty and a hold that reached the runner
    after the session owning that queue was torn down,
    When a worker later takes the review off that queue,
    Then it is dropped without a model call, because a consult the operator has
    already stopped must not start just because it was queued in time.
    """
    consult = _RecordingConsultRunner()
    runner = _runner(_FakeControlClient(), _FakeWakeClient([]), consult)
    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW)
    runner._inbox = inbox

    await runner._on_control(ControlDirective(ControlState.ACTIVE, 1))
    assert await inbox.offer(_context("review-queued")) is InboxOfferOutcome.QUEUED
    runner._inbox = None
    await runner._on_control(ControlDirective(ControlState.ON_HOLD, 2))
    worker = asyncio.create_task(runner._consult_worker(inbox))
    await _wait_until(lambda: runner.get_status()["consults_skipped"] == 1)
    await inbox.close()
    await worker

    assert consult.reviews == []


@pytest.mark.asyncio
async def test_the_production_wake_client_learns_which_control_topic_to_watch() -> None:
    """A resolved identity is pushed into the client that addresses control.

    Given a production wake client built before identity resolution finished,
    When the runner binds the identity it authenticated as,
    Then that client subscribes to the delegate-scoped control topic, without
    which the runner would take its boot state and never hear a live hold.
    """
    runner = _runner(_FakeControlClient(), _FakeWakeClient([]), _RecordingConsultRunner())
    client = WakeClient("https://snapper.invalid", _FakeControlClient())
    runner._wake_client = client

    runner._bind_control_identity("delegate-own")

    assert control_topic("delegate-own") in client._subscribe_topics()
