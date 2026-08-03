"""Hermetic lifecycle tests for the delegate consult runner."""

import asyncio
import signal
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from pydantic import SecretStr

import snapper_delegate.runner as runner_module
from snapper.core.json_types import JsonObject
from snapper_delegate.chat_completions import ApiKeyFileError
from snapper_delegate.chat_completions import AssistantChatMessage
from snapper_delegate.chat_completions import ChatCompletionChoice
from snapper_delegate.chat_completions import ChatCompletionRequest
from snapper_delegate.chat_completions import ChatCompletionResponse
from snapper_delegate.chat_completions import ChatCompletionResult
from snapper_delegate.chat_completions import ChatCompletionSuccess
from snapper_delegate.chat_completions import ChatCompletionUsage
from snapper_delegate.chat_completions import ChatFunctionCall
from snapper_delegate.chat_completions import ChatFunctionDefinition
from snapper_delegate.chat_completions import ChatRole
from snapper_delegate.chat_completions import ChatTool
from snapper_delegate.chat_completions import ChatToolCall
from snapper_delegate.consult import BoundedConsultRunner
from snapper_delegate.consult import ConsultOutcome
from snapper_delegate.consult import ConsultResult
from snapper_delegate.consult import ReviewConsultContext
from snapper_delegate.control_plane import ControlPlaneError
from snapper_delegate.control_plane import ControlPlaneErrorKind
from snapper_delegate.control_plane import PendingReview
from snapper_delegate.control_plane import WsToken
from snapper_delegate.delegate_control import ControlDirective
from snapper_delegate.delegate_control import ControlState
from snapper_delegate.mcp_bridge import MCPToolCallResult
from snapper_delegate.mcp_bridge import MCPToolCallSuccess
from snapper_delegate.mcp_bridge import MCPToolCatalogResult
from snapper_delegate.mcp_bridge import MCPToolCatalogSuccess
from snapper_delegate.review_inbox import ReviewInbox
from snapper_delegate.runner import DelegateRunner
from snapper_delegate.runner import RunnerChatClient
from snapper_delegate.runner import RunnerControlClient
from snapper_delegate.runner import RunnerRuntime
from snapper_delegate.runner import RunnerState
from snapper_delegate.runner import RunnerTuning
from snapper_delegate.runner import RunnerWakeClient
from snapper_delegate.wake_client import AiReviewDecisionAckFrame
from snapper_delegate.wake_client import AiReviewRequestFrame
from snapper_delegate.wake_client import WakeCallbacks
from snapper_delegate.wake_client import WakeFrame

_NOW = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
_FUTURE = _NOW + timedelta(minutes=5)
_TOOL_CONTENT = '{"success":true,"error_code":null,"message":"Decision recorded","details":null}'


type _IdentityResult = str | ControlPlaneError
type _PendingResult = list[PendingReview] | ControlPlaneError
type _WakeAction = Callable[[WakeCallbacks], Awaitable[None]]
type _ConsultAction = ConsultResult | Exception


class _MutableMonotonic:
    """Expose deterministic mutable monotonic time."""

    def __init__(self, value: float = 100.0) -> None:
        """Initialize the visible instant."""
        self.value = value

    def __call__(self) -> float:
        """Return the visible instant."""
        return self.value


class _FakeControlClient:
    """Provide scripted control-plane responses and closure state."""

    def __init__(
        self,
        identities: list[_IdentityResult] | None = None,
        pending: list[_PendingResult] | None = None,
        close_error: bool = False,
    ) -> None:
        """Retain scripted identity, pending, and shutdown outcomes."""
        self.identities = list(identities or ["delegate-own"])
        self.pending = list(pending or [])
        self.close_error = close_error
        self.identity_calls = 0
        self.pending_calls = 0
        self.token_reads = 0
        self.close_calls = 0

    async def fetch_delegate_identity(self) -> str:
        """Consume one identity response."""
        self.identity_calls += 1
        if not self.identities:
            return "delegate-own"
        result = self.identities.pop(0)
        if isinstance(result, ControlPlaneError):
            raise result
        return result

    async def list_pending_reviews(self, limit: int = 100) -> list[PendingReview]:
        """Consume one pending sweep response."""
        assert limit == 100
        self.pending_calls += 1
        if not self.pending:
            return []
        result = self.pending.pop(0)
        if isinstance(result, ControlPlaneError):
            raise result
        return result

    async def mint_ws_token(self) -> WsToken:
        """Return a harmless one-shot credential for protocol completeness."""
        return WsToken(SecretStr("ws-token"), _FUTURE)

    def read_access_token(self) -> SecretStr:
        """Return one harmless token and record the read."""
        self.token_reads += 1
        return SecretStr("access-token")

    async def aclose(self) -> None:
        """Record closure and optionally fail."""
        self.close_calls += 1
        if self.close_error:
            raise RuntimeError("control close failed")


class _BlockingIdentityControlClient(_FakeControlClient):
    """Block identity resolution until cancellation is observable."""

    def __init__(self) -> None:
        """Initialize identity request controls."""
        super().__init__()
        self.identity_started = asyncio.Event()
        self.identity_cancelled = asyncio.Event()
        self._identity_release = asyncio.Event()

    async def fetch_delegate_identity(self) -> str:
        """Wait indefinitely unless the owning runner cancels the request."""
        self.identity_calls += 1
        self.identity_started.set()
        try:
            await self._identity_release.wait()
        except asyncio.CancelledError:
            self.identity_cancelled.set()
            raise
        return "delegate-own"


class _BlockingSweepControlClient(_FakeControlClient):
    """Block the startup pending sweep until cancellation is observable."""

    def __init__(self) -> None:
        """Initialize pending request controls."""
        super().__init__()
        self.sweep_started = asyncio.Event()
        self.sweep_cancelled = asyncio.Event()
        self._sweep_release = asyncio.Event()

    async def list_pending_reviews(self, limit: int = 100) -> list[PendingReview]:
        """Wait indefinitely unless the owning runner cancels the sweep."""
        assert limit == 100
        self.pending_calls += 1
        self.sweep_started.set()
        try:
            await self._sweep_release.wait()
        except asyncio.CancelledError:
            self.sweep_cancelled.set()
            raise
        return []


class _FakeWakeClient:
    """Run scripted wake turns and expose prompt closure."""

    def __init__(
        self,
        actions: list[_WakeAction] | None = None,
        close_error: bool = False,
    ) -> None:
        """Retain scripted run actions and shutdown behavior."""
        self.actions = list(actions or [])
        self.close_error = close_error
        self.closed = asyncio.Event()
        self.run_calls = 0
        self.close_calls = 0

    async def run(self, callbacks: WakeCallbacks) -> None:
        """Consume an action or wait for closure."""
        self.run_calls += 1
        if self.actions:
            await self.actions.pop(0)(callbacks)
            return
        await self.closed.wait()

    async def close(self) -> None:
        """Release active waits and optionally fail."""
        self.close_calls += 1
        self.closed.set()
        if self.close_error:
            raise RuntimeError("wake close failed")


class _FakeMCPBridge:
    """Expose one decision tool and record successful invocations."""

    def __init__(self) -> None:
        """Initialize catalog and call records."""
        self.list_tokens: list[str] = []
        self.calls: list[tuple[str, str, JsonObject]] = []

    async def list_tools(self, access_token: str) -> MCPToolCatalogResult:
        """Return the decision tool catalog."""
        self.list_tokens.append(access_token)
        return MCPToolCatalogSuccess(
            tools=[
                ChatTool(
                    type="function",
                    function=ChatFunctionDefinition(
                        name="submit_ai_review_decision",
                        description="Submit a review decision",
                        parameters={"type": "object"},
                    ),
                )
            ]
        )

    async def call_tool(
        self,
        access_token: str,
        name: str,
        arguments: JsonObject,
    ) -> MCPToolCallResult:
        """Record one successful tool call."""
        self.calls.append((access_token, name, arguments))
        return MCPToolCallSuccess(
            content=_TOOL_CONTENT,
            error_code=None,
            message="Decision recorded",
            details=None,
        )


class _SubmittingChatClient:
    """Submit the active review from every chat request."""

    def __init__(self, close_error: bool = False) -> None:
        """Initialize request and closure records."""
        self.requests: list[ChatCompletionRequest] = []
        self.close_error = close_error
        self.close_calls = 0

    async def complete(self, request: ChatCompletionRequest) -> ChatCompletionResult:
        """Return a valid decision call bound to the request context."""
        self.requests.append(request)
        encoded_context = request.messages[1].content or "{}"
        context = ReviewConsultContext.model_validate_json(encoded_context)
        call = ChatToolCall(
            id=f"call-{context.review_public_id}",
            type="function",
            function=ChatFunctionCall(
                name="submit_ai_review_decision",
                arguments=(
                    f'{{"review_id":"{context.review_public_id}",'
                    '"decision":"approve","rationale":"Within limits"}'
                ),
            ),
        )
        return _completion(call)

    async def aclose(self) -> None:
        """Record closure and optionally fail."""
        self.close_calls += 1
        if self.close_error:
            raise RuntimeError("chat close failed")


class _ScriptedConsultRunner:
    """Return or raise scripted consult actions."""

    def __init__(self, actions: list[_ConsultAction]) -> None:
        """Retain ordered consult actions."""
        self.actions = list(actions)
        self.contexts: list[ReviewConsultContext] = []

    async def run(self, context: ReviewConsultContext) -> ConsultResult:
        """Consume one consult action."""
        self.contexts.append(context)
        result = self.actions.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _completion(call: ChatToolCall) -> ChatCompletionSuccess:
    """Wrap one assistant decision call in a completion response."""
    return ChatCompletionSuccess(
        completion=ChatCompletionResponse(
            choices=[
                ChatCompletionChoice(
                    index=0,
                    message=AssistantChatMessage(
                        role=ChatRole.ASSISTANT,
                        content=None,
                        tool_calls=[call],
                    ),
                    finish_reason="tool_calls",
                )
            ],
            usage=ChatCompletionUsage(
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
            ),
        )
    )


def _pending(review_id: str = "review-rest") -> PendingReview:
    """Build one selected pending REST row."""
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


def _context(review_id: str = "review-context") -> ReviewConsultContext:
    """Build one selected consult context."""
    return ReviewConsultContext(
        review_public_id=review_id,
        selected_delegate_public_id="delegate-own",
        wallet_public_id=f"wallet-{review_id}",
        dispatch_version=1,
        deadline=_FUTURE,
        fanout_after=_NOW + timedelta(seconds=30),
        signal_envelope={"side": "buy"},
        instrument_metadata={"symbol": "BTC/USD"},
    )


def _request(
    review_id: str = "review-wake",
    selected_delegate: str = "delegate-own",
) -> AiReviewRequestFrame:
    """Build one rich WebSocket review request."""
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
        selected_delegate_public_id=selected_delegate,
        deadline=_FUTURE,
        signal_envelope={"side": "buy", "confidence": 0.8},
        instrument_metadata={"symbol": "BTC/USD"},
        dispatch_version=2,
    )


def _ack(review_id: str = "review-foreign") -> AiReviewDecisionAckFrame:
    """Build one decision acknowledgement."""
    return AiReviewDecisionAckFrame(
        type="ai_review.decision_ack",
        session_id="session-1",
        sequence_id=2,
        public_id=f"ack-{review_id}",
        timestamp=_NOW,
        topic="ai_reviews.decision_ack",
        review_public_id=review_id,
        user_public_id="user-1",
        strategy_public_id="strategy-1",
        wallet_public_id=f"wallet-{review_id}",
        instrument_public_id="instrument-1",
        responding_delegate_public_id="delegate-other",
        decision="reject",
        new_status="rejected",
        resolution_mode="selected_delegate",
        rationale="Risk too high",
        dispatch_version=2,
    )


def _runner(runtime: RunnerRuntime, configured: bool = True) -> DelegateRunner:
    """Build a lifecycle runner around injected seams."""
    return DelegateRunner(
        model_alias="review-model",
        base_url="https://model.invalid",
        endpoint_path="/v1beta/compat/chat/completions",
        api_key_file="/missing/model-key",
        delegate_token_file="/missing/delegate-token",
        max_tool_rounds=3,
        snapper_base_url="https://snapper.invalid" if configured else "",
        runtime=runtime,
    )


async def _wait_until(predicate: Callable[[], bool]) -> None:
    """Yield boundedly until one deterministic condition becomes true."""
    for _ in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition did not become true")


async def _open_session_with_duty(callbacks: WakeCallbacks, revision: int) -> None:
    """Subscribe one session and grant it consult duty at one revision.

    Args:
        callbacks: Lifecycle hooks the runner supplied to the wake boundary.
        revision: Monotonic control revision the server states for this grant.
    """
    await callbacks.connection_state(True)
    await callbacks.subscribed()
    assert callbacks.control is not None
    await callbacks.control(ControlDirective(ControlState.ACTIVE, revision))


def _submitted_contexts(chat: _SubmittingChatClient) -> dict[str, ReviewConsultContext]:
    """Return every consult context submitted through the model boundary.

    Args:
        chat: The recording chat-completions double used by one runner.

    Returns:
        Submitted contexts keyed by review public identifier.
    """
    contexts = [
        ReviewConsultContext.model_validate_json(request.messages[1].content or "{}")
        for request in chat.requests
    ]
    return {context.review_public_id: context for context in contexts}


@pytest.mark.asyncio
async def test_configured_runner_sweeps_wakes_submits_and_routes_foreign_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Granted consult duty reaches bounded MCP submission across a reconnect.

    Given a configured runner with pending, selected, foreign, and acknowledged work,
    When consult duty is granted on one session and again after a reconnect,
    Then eligible reviews submit once and lifecycle state and counters remain coherent.
    """
    first = _pending("review-startup")
    ignored = first.model_copy(update={"review_public_id": "review-ignored", "status": "done"})
    reconnect = _pending("review-reconnect").model_copy(
        update={"instrument": "ETH/USD", "signal_envelope": {"side": "sell"}}
    )
    control = _FakeControlClient(pending=[[first, ignored], [reconnect]])
    bridge = _FakeMCPBridge()
    chat = _SubmittingChatClient()
    foreign = _request("review-foreign", "delegate-other")

    async def _granted_session(callbacks: WakeCallbacks) -> None:
        """Grant consult duty, deliver one wake turn, then lose the connection."""
        await _open_session_with_duty(callbacks, 1)
        await callbacks.heartbeat()
        await callbacks.frame(_request())
        await callbacks.frame(foreign)
        await callbacks.frame(_ack())
        await _wait_until(lambda: runner.get_status()["consults_processed"] == 2)
        await callbacks.connection_state(False)

    async def _regranted_session(callbacks: WakeCallbacks) -> None:
        """Re-grant consult duty on the next session and wait for closure."""
        await _open_session_with_duty(callbacks, 2)
        await wake.closed.wait()

    wake = _FakeWakeClient([_granted_session, _regranted_session])
    runtime = RunnerRuntime(
        control_client=control,
        wake_client=wake,
        mcp_bridge=bridge,
        chat_client=chat,
        clock=lambda: _NOW,
        tuning=RunnerTuning(identity_backoff_base_seconds=0.0),
    )
    runner = _runner(runtime)
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())

    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["consults_processed"] == 3)

    status = runner.get_status()
    assert status["state"] == "subscribed"
    assert status["ws_connected"] is True
    assert status["heartbeat_count"] == 1
    assert status["last_wake_at"] == _NOW.isoformat()
    assert status["consults_skipped"] == 0
    assert control.pending_calls == 2
    assert len(chat.requests) == 3
    assert len(bridge.calls) == 3
    contexts_by_id = _submitted_contexts(chat)
    assert set(contexts_by_id) == {"review-startup", "review-reconnect", "review-wake"}
    assert contexts_by_id["review-startup"].signal_envelope == {}
    assert contexts_by_id["review-startup"].instrument_metadata == {}
    assert contexts_by_id["review-reconnect"].instrument_metadata == {"instrument": "ETH/USD"}
    assert contexts_by_id["review-wake"].user_public_id == "user-1"
    assert runner._inbox is not None
    assert "review-foreign" not in runner._inbox._entries

    await runner.stop()
    await task
    assert runner.get_status()["state"] == "stopped"
    assert wake.close_calls >= 1
    assert chat.close_calls == 1
    assert control.close_calls == 1


@pytest.mark.asyncio
async def test_identity_retries_fail_closed_and_stops_without_fetching() -> None:
    """Identity errors and blanks retry while an existing stop returns inaction.

    Given scripted identity failures, blank identities, and a later valid identity,
    When resolution retries with bounded backoff or observes an existing stop,
    Then transient failures stay local and stopped resolution performs no fetch.
    """
    unavailable = ControlPlaneError(ControlPlaneErrorKind.TRANSPORT)
    control = _FakeControlClient([unavailable, "  ", " delegate-own "])
    runtime = RunnerRuntime(
        control_client=control,
        tuning=RunnerTuning(
            identity_backoff_base_seconds=0.0,
            identity_backoff_cap_seconds=0.0,
        ),
    )
    runner = _runner(runtime)

    assert await runner._resolve_delegate_identity() == "delegate-own"
    assert control.identity_calls == 3

    stopped_control = _FakeControlClient()
    stopped = _runner(RunnerRuntime(control_client=stopped_control))
    stopped._stop_event.set()
    assert await stopped._resolve_delegate_identity() is None
    assert stopped_control.identity_calls == 0

    wake = _FakeWakeClient()
    configured = _runner(
        RunnerRuntime(
            control_client=stopped_control,
            wake_client=wake,
            mcp_bridge=_FakeMCPBridge(),
            chat_client=_SubmittingChatClient(),
        )
    )
    configured._stop_event.set()
    await configured._configured_loop()
    assert configured._inbox is None


@pytest.mark.asyncio
async def test_blocked_identity_is_cancelled_by_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop cancels an indefinitely blocked identity fetch before wake startup.

    Given identity resolution is blocked indefinitely,
    When runner shutdown is requested,
    Then the identity task is cancelled and wake processing never starts.
    """
    initial_tasks = asyncio.all_tasks()
    control = _BlockingIdentityControlClient()
    wake = _FakeWakeClient()
    runner = _runner(
        RunnerRuntime(
            control_client=control,
            wake_client=wake,
            mcp_bridge=_FakeMCPBridge(),
            chat_client=_SubmittingChatClient(),
        )
    )
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())
    task = asyncio.create_task(runner.start())
    await control.identity_started.wait()

    async with asyncio.timeout(0.5):
        await runner.stop()
        await task

    assert control.identity_cancelled.is_set()
    assert wake.run_calls == 0
    assert runner.get_status()["state"] == "stopped"
    assert asyncio.all_tasks() == initial_tasks


@pytest.mark.asyncio
async def test_identity_fetch_external_cancellation_propagates() -> None:
    """Task ownership cancellation remains distinct from lifecycle shutdown.

    Given identity resolution is awaiting an externally owned task,
    When that task is cancelled without setting the runner stop event,
    Then cancellation propagates instead of being mistaken for normal shutdown.
    """
    initial_tasks = asyncio.all_tasks()
    control = _BlockingIdentityControlClient()
    runner = _runner(RunnerRuntime(control_client=control))
    task = asyncio.create_task(runner._fetch_identity_or_stop())
    await control.identity_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert control.identity_cancelled.is_set()
    assert asyncio.all_tasks() == initial_tasks


@pytest.mark.asyncio
async def test_identity_stop_wins_when_fetch_completes_concurrently() -> None:
    """A set stop event wins even when identity completion occurs in the same turn.

    Given identity fetch completion races with an already signalled stop,
    When the resolution turn observes both outcomes,
    Then shutdown wins and no delegate identity is admitted.
    """

    class _StoppingControlClient(_FakeControlClient):
        """Set lifecycle stop immediately before returning identity."""

        async def fetch_delegate_identity(self) -> str:
            """Complete identity while making stop authoritative."""
            runner._stop_event.set()
            return "delegate-own"

    control = _StoppingControlClient()
    runner = _runner(RunnerRuntime(control_client=control))
    assert await runner._fetch_identity_or_stop() is None


@pytest.mark.asyncio
async def test_blocked_startup_sweep_runs_with_wake_and_is_cancelled_on_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Duty catch-up cannot delay the session and is drained during shutdown.

    Given the pending-review sweep started by a duty grant blocks indefinitely,
    When the granting session keeps running and the runner is stopped,
    Then heartbeats still arrive and the sweep is cancelled and drained.
    """
    initial_tasks = asyncio.all_tasks()
    control = _BlockingSweepControlClient()

    async def _granting_session(callbacks: WakeCallbacks) -> None:
        """Grant consult duty, then keep proving liveness until closure."""
        await callbacks.connection_state(True)
        await callbacks.subscribed()
        assert callbacks.control is not None
        await callbacks.control(ControlDirective(ControlState.ACTIVE, 1))
        await callbacks.heartbeat()
        await wake.closed.wait()

    wake = _FakeWakeClient([_granting_session])
    runner = _runner(
        RunnerRuntime(
            control_client=control,
            wake_client=wake,
            mcp_bridge=_FakeMCPBridge(),
            chat_client=_SubmittingChatClient(),
        )
    )
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())
    task = asyncio.create_task(runner.start())
    await control.sweep_started.wait()
    await _wait_until(lambda: runner.get_status()["heartbeat_count"] == 1)

    assert runner.get_status()["ws_connected"] is True
    async with asyncio.timeout(0.5):
        await runner.stop()
        await task

    assert control.sweep_cancelled.is_set()
    assert runner.get_status()["state"] == "stopped"
    assert asyncio.all_tasks() == initial_tasks


@pytest.mark.asyncio
async def test_wake_returns_and_errors_are_retried_until_stop() -> None:
    """Unexpected wake returns and exceptions stay within the retry loop.

    Given wake runs that return unexpectedly or raise recoverable exceptions,
    When the configured runner supervises successive wake turns,
    Then it retries both outcomes until an explicit stop terminates the loop.
    """
    actions: list[_WakeAction] = []

    async def _return(callbacks: WakeCallbacks) -> None:
        """Return without a session failure."""
        del callbacks

    async def _fail(callbacks: WakeCallbacks) -> None:
        """Raise one recoverable wake failure."""
        del callbacks
        raise RuntimeError("wake failed")

    async def _stop(callbacks: WakeCallbacks) -> None:
        """Stop the owning retry loop."""
        del callbacks
        runner._stop_event.set()

    actions.extend([_return, _fail, _stop])
    wake = _FakeWakeClient(actions)
    runner = _runner(
        RunnerRuntime(
            wake_client=wake,
            tuning=RunnerTuning(identity_backoff_base_seconds=0.0),
        )
    )
    callbacks = WakeCallbacks(_noop_connection, _noop_callback, _noop_frame, _noop_callback)

    await runner._run_wake_until_stopped(callbacks)

    assert wake.run_calls == 3


async def _noop_connection(connected: bool) -> None:
    """Accept one unused connection state."""
    del connected


async def _noop_callback() -> None:
    """Provide one inert asynchronous callback."""


async def _noop_frame(frame: WakeFrame) -> bool:
    """Accept one unused wake frame.

    Args:
        frame: The wake this inert consumer ignores.

    Returns:
        Whether responsibility was taken, which is never for an inert consumer.
    """
    del frame
    return False


@pytest.mark.asyncio
async def test_pending_sweep_errors_filters_and_normalizes_sparse_rows() -> None:
    """Sweep failures stay local and non-pending rows never enter the inbox.

    Given a failed sweep followed by sparse pending and non-pending review rows,
    When startup catch-up normalizes and offers the snapshot,
    Then failures stay local and only valid pending contexts reach the inbox.
    """
    error = ControlPlaneError(
        ControlPlaneErrorKind.HTTP_STATUS,
        status_code=422,
        error_code="not_a_delegate",
    )
    control = _FakeControlClient(pending=[error])
    runner = _runner(RunnerRuntime(control_client=control, clock=lambda: _NOW))
    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW)
    runner._inbox = inbox

    await runner._sweep_pending()
    assert inbox._entries == {}

    sparse = runner_module._pending_context(_pending())
    rich = runner_module._pending_context(
        _pending("review-rich").model_copy(
            update={"instrument": "BTC/USD", "signal_envelope": {"score": 7}}
        )
    )
    assert sparse.signal_envelope == {}
    assert sparse.instrument_metadata == {}
    assert rich.signal_envelope == {"score": 7}
    assert rich.instrument_metadata == {"instrument": "BTC/USD"}
    await inbox.close()


@pytest.mark.asyncio
async def test_offer_context_counts_expiry_capacity_and_closed_drops() -> None:
    """Only terminal local inbox drops increment the skipped counter.

    Given contexts producing expiry, capacity, closure, and nonterminal inbox outcomes,
    When the runner offers each context,
    Then only locally terminal drops increment the skipped consultation counter.
    """
    runner = _runner(RunnerRuntime(clock=lambda: _NOW))
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    runner._inbox = inbox

    await runner._offer_context(_context("review-ready"))
    assert runner._consults_skipped == 0
    await runner._offer_context(_context("review-capacity"))
    expired = _context("review-expired").model_copy(update={"deadline": _NOW})
    await runner._offer_context(expired)
    assert runner._consults_skipped == 2
    await inbox.close()
    await runner._offer_context(_context("review-closed"))
    assert runner._consults_skipped == 3

    runner._inbox = None
    await runner._offer_context(_context("review-no-inbox"))
    assert runner._consults_skipped == 3


@pytest.mark.asyncio
async def test_wake_callbacks_cover_missing_inbox_ack_and_status_transitions() -> None:
    """Callbacks update status while acknowledgements cancel held foreign work.

    Given connection callbacks, a missing inbox, and held foreign review work,
    When status events, wakes, heartbeats, and decision acknowledgements arrive,
    Then lifecycle state updates and the matching held work is cancelled safely.
    """
    control = _FakeControlClient()
    runner = _runner(RunnerRuntime(control_client=control, clock=lambda: _NOW))

    await runner._on_wake_frame(_request("review-without-inbox"))
    assert runner._last_wake_at == _NOW.isoformat()

    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW)
    runner._inbox = inbox
    await runner._on_control(ControlDirective(ControlState.ACTIVE, 1))
    await runner._on_wake_frame(_request("review-foreign", "delegate-other"))
    assert "review-foreign" in inbox._entries
    await runner._on_wake_frame(_ack())
    await asyncio.sleep(0)
    assert "review-foreign" not in inbox._entries

    await runner._on_connection_state(False)
    assert runner._state is RunnerState.STOPPED
    runner._running = True
    await runner._on_connection_state(False)
    assert runner._state.value == "connecting"
    runner._quota_degraded = True
    runner._state = RunnerState.DEGRADED
    await runner._on_connection_state(True)
    assert runner._state is RunnerState.DEGRADED
    await runner._on_subscribed()
    assert runner._ws_connected is True
    assert runner._state is RunnerState.DEGRADED
    await runner._on_heartbeat()
    assert runner._heartbeat_count == 1
    await inbox.close()


@pytest.mark.asyncio
async def test_consult_outcomes_quota_cooldown_exception_and_recovery() -> None:
    """Consult outcomes update counters while quota cooldown preserves liveness.

    Given submitted, skipped, quota-exhausted, exceptional, and recovered consult turns,
    When contexts are processed across the configured quota cooldown,
    Then counters and degraded state advance without stopping later consultations.
    """
    monotonic = _MutableMonotonic()
    runner = _runner(RunnerRuntime(monotonic=monotonic, clock=lambda: _NOW))
    scripted = _ScriptedConsultRunner(
        [
            ConsultResult(ConsultOutcome.SKIPPED),
            ConsultResult(ConsultOutcome.QUOTA_DEGRADED),
            RuntimeError("consult failed"),
            ConsultResult(ConsultOutcome.SUBMITTED),
        ]
    )
    runner._consult_runner = cast(BoundedConsultRunner, scripted)

    await runner._process_consult(_context("review-skipped"))
    await runner._process_consult(_context("review-quota"))
    assert runner._consults_skipped == 2
    assert runner._quota_degraded is True
    assert runner._state is RunnerState.DEGRADED

    await runner._process_consult(_context("review-cooldown"))
    assert runner._consults_skipped == 3
    assert len(scripted.contexts) == 2

    monotonic.value += 61.0
    await runner._process_consult(_context("review-error"))
    assert runner._quota_degraded is False
    assert runner._state is RunnerState.CONNECTING
    assert runner._consults_skipped == 4

    await runner._process_consult(_context("review-submitted"))
    assert runner._consults_processed == 1


@pytest.mark.asyncio
async def test_missing_chat_key_skips_consult_without_blocking_liveness() -> None:
    """A missing model credential yields a counted local skip.

    Given consultation work but no readable chat-completions credential,
    When the runner tries to initialize the consult path,
    Then it records a local skip while preserving runner liveness.
    """

    def _missing_key(
        base_url: str,
        api_key_file: str,
        endpoint_path: str,
    ) -> RunnerChatClient:
        """Raise the typed credential outcome."""
        del base_url, api_key_file, endpoint_path
        raise ApiKeyFileError("missing key")

    runtime = RunnerRuntime(
        control_client=_FakeControlClient(),
        mcp_bridge=_FakeMCPBridge(),
        chat_factory=_missing_key,
        clock=lambda: _NOW,
    )
    runner = _runner(runtime)

    assert runner._get_consult_runner() is None
    await runner._process_consult(_context())
    assert runner._consults_skipped == 1


@pytest.mark.asyncio
async def test_consult_worker_returns_on_closed_inbox() -> None:
    """Closing an empty inbox cleanly terminates its consult worker.

    Given a consult worker blocked on an empty inbox,
    When the inbox is closed,
    Then the worker returns cleanly without fabricating work or failures.
    """
    runner = _runner(RunnerRuntime())
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    await inbox.close()
    await runner._consult_worker(inbox)

    runner._stop_event.set()
    fresh = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    await runner._consult_worker(fresh)
    await fresh.close()


@pytest.mark.asyncio
async def test_consult_worker_contains_factory_bug_and_processes_later_context() -> None:
    """One unexpected context failure cannot terminate the serial consult worker.

    Given one context triggers an unexpected consult-factory failure before valid work,
    When the serial worker consumes both contexts,
    Then it contains the first failure and still processes the later context.
    """
    attempts = 0
    chat = _SubmittingChatClient()

    def _flaky_factory(
        base_url: str,
        api_key_file: str,
        endpoint_path: str,
    ) -> RunnerChatClient:
        """Raise once before returning a usable model client."""
        nonlocal attempts
        del base_url, api_key_file, endpoint_path
        attempts += 1
        if attempts == 1:
            raise RuntimeError("factory bug")
        return chat

    runner = _runner(
        RunnerRuntime(
            control_client=_FakeControlClient(),
            mcp_bridge=_FakeMCPBridge(),
            chat_factory=_flaky_factory,
            clock=lambda: _NOW,
        )
    )
    inbox = ReviewInbox("delegate-own", 2, clock=lambda: _NOW)
    runner._inbox = inbox
    await runner._on_control(ControlDirective(ControlState.ACTIVE, 1))
    worker = asyncio.create_task(runner._consult_worker(inbox))
    await inbox.offer(_context("review-factory-error"))
    await inbox.offer(_context("review-after-error"))
    await _wait_until(lambda: runner._consults_skipped == 1 and runner._consults_processed == 1)
    await inbox.close()
    await worker

    assert attempts == 2
    assert len(chat.requests) == 1


@pytest.mark.asyncio
async def test_signal_size_unicode_failure_is_log_safe() -> None:
    """Malformed Unicode cannot prevent an otherwise successful consultation.

    Given a signal envelope containing malformed Unicode for size logging,
    When an otherwise valid consultation is processed,
    Then logging remains safe and the successful outcome is still counted.
    """
    runner = _runner(RunnerRuntime(clock=lambda: _NOW))
    scripted = _ScriptedConsultRunner([ConsultResult(ConsultOutcome.SUBMITTED)])
    runner._consult_runner = cast(BoundedConsultRunner, scripted)
    context = _context("review-unicode").model_copy(
        update={"signal_envelope": {"malformed": "\ud800"}}
    )

    assert runner_module._safe_signal_size(context) is None
    await runner._process_consult(context)

    assert runner._consults_processed == 1
    assert runner._consults_skipped == 0


@pytest.mark.asyncio
async def test_stop_and_shutdown_contain_all_client_close_failures() -> None:
    """Wake, model, and control close failures do not mask later cleanup.

    Given wake, model, and control clients whose close operations fail,
    When stop and shutdown traverse every owned resource,
    Then each failure is contained and all later cleanup is still attempted.
    """
    control = _FakeControlClient(close_error=True)
    wake = _FakeWakeClient(close_error=True)
    chat = _SubmittingChatClient(close_error=True)
    runner = _runner(
        RunnerRuntime(
            control_client=control,
            wake_client=wake,
            mcp_bridge=_FakeMCPBridge(),
            chat_client=chat,
        )
    )
    inbox = ReviewInbox("delegate-own", 1, clock=lambda: _NOW)
    runner._inbox = inbox

    await runner.stop()
    assert runner._stop_event.is_set()
    await runner._shutdown_resources()

    assert wake.close_calls == 2
    assert chat.close_calls == 1
    assert control.close_calls == 1
    assert await inbox.get() is None


@pytest.mark.asyncio
async def test_required_client_guards_and_initialized_values() -> None:
    """Required-client accessors expose missing initialization as programming errors.

    Given a runner before and after its injected clients are initialized,
    When required-client guards and accessors are evaluated,
    Then missing dependencies fail explicitly and configured values remain exact.
    """
    empty = _runner(RunnerRuntime())
    with pytest.raises(RuntimeError, match="Control client"):
        empty._required_control()
    with pytest.raises(RuntimeError, match="Wake client"):
        empty._required_wake()
    with pytest.raises(RuntimeError, match="MCP bridge"):
        empty._required_mcp()

    control = _FakeControlClient()
    wake = _FakeWakeClient()
    bridge = _FakeMCPBridge()
    runner = _runner(RunnerRuntime(control_client=control, wake_client=wake, mcp_bridge=bridge))
    assert runner._required_control() is control
    assert runner._required_wake() is wake
    assert runner._required_mcp() is bridge
    await runner._close_wake_client()
    assert wake.close_calls == 1


@pytest.mark.asyncio
async def test_double_start_is_rejected_and_idle_runner_stops_promptly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live runner rejects duplicate ownership and remains promptly stoppable.

    Given one idle runner task already owns the lifecycle,
    When a second start is attempted and stop is requested,
    Then duplicate ownership is rejected and the original task stops promptly.
    """
    monkeypatch.setattr(DelegateRunner, "_install_stop_signal_handlers", lambda self: ())
    monkeypatch.setattr(runner_module, "_HEARTBEAT_INTERVAL_SECONDS", 600.0)
    runner = _runner(RunnerRuntime(), configured=False)
    task = asyncio.create_task(runner.start())
    await _wait_until(lambda: runner.get_status()["running"] is True)

    with pytest.raises(RuntimeError, match="already running"):
        await runner.start()

    await runner.stop()
    await task
    assert runner.get_status()["state"] == "stopped"


@pytest.mark.asyncio
async def test_idle_timeout_and_signal_stop_close_active_wake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Idle timeouts heartbeat again while signal stop closes active wake state.

    Given idle heartbeat waits and an installed termination callback with active wake,
    When timeouts recur and the signal callback requests shutdown,
    Then heartbeats continue until active wake state is closed and stopped.
    """
    runner = _runner(RunnerRuntime(), configured=False)
    waits = 0

    async def _idle_wait(awaitable: Awaitable[bool], timeout: float) -> bool:
        """Raise once, then stop the heartbeat loop without elapsed time."""
        nonlocal waits
        assert timeout == runner_module._HEARTBEAT_INTERVAL_SECONDS
        coroutine = cast(object, awaitable)
        close = getattr(coroutine, "close")
        close()
        waits += 1
        if waits == 1:
            raise TimeoutError
        runner._stop_event.set()
        return True

    monkeypatch.setattr(asyncio, "wait_for", _idle_wait)
    await runner._idle_heartbeat_loop()
    assert runner._heartbeat_count == 2

    no_wake = _runner(RunnerRuntime())
    no_wake._request_signal_stop()
    assert no_wake._stop_event.is_set()

    wake = _FakeWakeClient()
    with_wake = _runner(RunnerRuntime(wake_client=wake))
    with_wake._request_signal_stop()
    signal_task = with_wake._signal_close_task
    assert signal_task is not None
    with_wake._request_signal_stop()
    assert with_wake._signal_close_task is signal_task
    await with_wake._close_wake_resources()
    assert with_wake._stop_event.is_set()
    assert wake.close_calls == 1
    assert with_wake._signal_close_task is None


@pytest.mark.asyncio
async def test_wait_backoff_signal_and_value_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Backoff, wait, signal, clock, context, and size helpers cover boundaries.

    Given boundary values for retry timing, stop waits, signals, clocks, and payloads,
    When the runner's deterministic helper functions evaluate them,
    Then each result is bounded, normalized, and fail-closed where required.
    """
    runner = _runner(RunnerRuntime())
    await runner._wait_for_stop(0.0)
    runner._stop_event.set()
    await runner._wait_for_stop(10.0)

    async def _timeout(awaitable: Awaitable[bool], timeout: float) -> bool:
        """Close one event wait and raise the timeout outcome."""
        assert timeout == 2.0
        coroutine = cast(object, awaitable)
        close = getattr(coroutine, "close")
        close()
        raise TimeoutError

    runner._stop_event.clear()
    monkeypatch.setattr(asyncio, "wait_for", _timeout)
    await runner._wait_for_stop(2.0)

    tuning = RunnerTuning(
        identity_backoff_base_seconds=2.0,
        identity_backoff_cap_seconds=9.0,
    )
    assert runner_module._identity_backoff(0, tuning) == 2.0
    assert runner_module._identity_backoff(99, tuning) == 9.0
    assert runner_module._signal_size(
        _context().model_copy(update={"signal_envelope": {"symbol": "Ł"}})
    ) == len('{"symbol":"Ł"}'.encode())
    assert runner_module._utc_now().utcoffset() == timedelta(0)

    wake_context = runner_module._wake_context(
        _request(),
        RunnerTuning(fanout_delay_seconds=12.0),
    )
    assert wake_context.fanout_after == _NOW + timedelta(seconds=12)
    assert wake_context.instrument_public_id == "instrument-1"


def test_default_client_initialization_and_chat_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing clients are built once through production constructor seams.

    Given a configured runner without injected production clients,
    When client initialization and the default chat factory are invoked repeatedly,
    Then each dependency is constructed once with the expected production arguments.
    """
    control = _FakeControlClient()
    wake = _FakeWakeClient()
    bridge = _FakeMCPBridge()
    chat = _SubmittingChatClient()
    calls: list[tuple[str, ...]] = []

    def _control_factory(base_url: str, token_file: str) -> RunnerControlClient:
        """Record default control-client construction."""
        calls.append((base_url, token_file))
        return control

    def _bridge_factory(base_url: str) -> _FakeMCPBridge:
        """Record default MCP construction."""
        calls.append(("mcp", base_url))
        return bridge

    def _wake_factory(
        base_url: str,
        credentials: RunnerControlClient,
        *,
        delegate_public_id: str | None = None,
    ) -> RunnerWakeClient:
        """Record default wake construction including the control identity."""
        assert credentials is control
        calls.append(("wake", base_url))
        return wake

    def _chat_factory(
        base_url: str,
        api_key_file: str,
        *,
        endpoint_path: str,
    ) -> _SubmittingChatClient:
        """Record default chat construction."""
        calls.append((base_url, api_key_file, endpoint_path))
        return chat

    monkeypatch.setattr(runner_module, "SnapperControlClient", _control_factory)
    monkeypatch.setattr(runner_module, "MCPBridge", _bridge_factory)
    monkeypatch.setattr(runner_module, "WakeClient", _wake_factory)
    monkeypatch.setattr(runner_module, "ChatCompletionsClient", _chat_factory)
    runner = _runner(RunnerRuntime())

    runner._initialize_clients()
    runner._initialize_clients()
    consult = runner._get_consult_runner()

    assert runner._required_control() is control
    assert runner._required_wake() is wake
    assert runner._required_mcp() is bridge
    assert consult is not None
    assert runner._get_consult_runner() is consult
    assert calls == [
        ("https://snapper.invalid", "/missing/delegate-token"),
        ("mcp", "https://snapper.invalid"),
        ("wake", "https://snapper.invalid"),
        (
            "https://model.invalid",
            "/missing/model-key",
            "/v1beta/compat/chat/completions",
        ),
    ]


def test_signal_handler_runtime_error_is_fail_soft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unsupported loop signal registration returns an empty handler set.

    Given an event loop that rejects signal-handler registration,
    When the runner installs its termination handlers,
    Then the runtime error is contained and no handlers are reported as installed.
    """

    class _SignalLoop:
        """Reject managed signal registration."""

        def add_signal_handler(
            self,
            stop_signal: signal.Signals,
            callback: Callable[[], None],
        ) -> None:
            """Raise the supported fail-soft registration error."""
            del stop_signal, callback
            raise RuntimeError("signals unavailable")

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _SignalLoop())
    runner = _runner(RunnerRuntime())
    assert runner._install_stop_signal_handlers() == ()


def test_signal_handlers_install_and_remove(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Supported loops install and remove the managed termination handler.

    Given an event loop that supports managed termination signals,
    When the runner installs and later removes its handlers,
    Then each supported signal is registered and removed through the loop API.
    """
    callbacks: dict[signal.Signals, Callable[[], None]] = {}
    removed: list[signal.Signals] = []

    class _SignalLoop:
        """Record signal handler lifecycle calls."""

        def add_signal_handler(
            self,
            stop_signal: signal.Signals,
            callback: Callable[[], None],
        ) -> None:
            """Retain one managed callback."""
            callbacks[stop_signal] = callback

        def remove_signal_handler(self, stop_signal: signal.Signals) -> bool:
            """Record one managed callback removal."""
            removed.append(stop_signal)
            return True

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _SignalLoop())
    runner = _runner(RunnerRuntime())
    installed = runner._install_stop_signal_handlers()
    runner._remove_stop_signal_handlers(installed)

    assert callbacks == {signal.SIGTERM: runner._request_signal_stop}
    assert removed == [signal.SIGTERM]
