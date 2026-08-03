"""Long-lived delegate lifecycle joining wake, consult, and MCP boundaries."""

import asyncio
import json
import signal
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from enum import StrEnum
from typing import NotRequired
from typing import Protocol
from typing import TypedDict
from typing import Unpack

from loguru import logger
from pydantic import SecretStr

from snapper_delegate.chat_completions import ApiKeyFileError
from snapper_delegate.chat_completions import ChatCompletionsClient
from snapper_delegate.consult import BoundedConsultRunner
from snapper_delegate.consult import ChatCompletionClient
from snapper_delegate.consult import ConsultConfiguration
from snapper_delegate.consult import ConsultOutcome
from snapper_delegate.consult import ReviewConsultContext
from snapper_delegate.control_plane import ControlPlaneError
from snapper_delegate.control_plane import PendingReview
from snapper_delegate.control_plane import SnapperControlClient
from snapper_delegate.control_plane import WsToken
from snapper_delegate.delegate_control import ControlDirective
from snapper_delegate.delegate_control import ControlGate
from snapper_delegate.delegate_control import ControlState
from snapper_delegate.json_types import JsonValue
from snapper_delegate.mcp_bridge import MCPBridge
from snapper_delegate.mcp_bridge import MCPBridgeClient
from snapper_delegate.review_inbox import InboxOfferOutcome
from snapper_delegate.review_inbox import ReviewInbox
from snapper_delegate.wake_client import AiReviewDecisionAckFrame
from snapper_delegate.wake_client import AiReviewRequestFrame
from snapper_delegate.wake_client import WakeCallbacks
from snapper_delegate.wake_client import WakeClient
from snapper_delegate.wake_client import WakeFrame

_HEARTBEAT_INTERVAL_SECONDS = 30.0


class RunnerState(StrEnum):
    """Externally visible delegate lifecycle states."""

    IDLE = "idle"
    CONNECTING = "connecting"
    SUBSCRIBED = "subscribed"
    DEGRADED = "degraded"
    STOPPED = "stopped"


class RunnerControlClient(Protocol):
    """Structural control-plane surface used by the lifecycle."""

    async def fetch_delegate_identity(self) -> str:
        """Return the authenticated delegate identifier.

        Returns:
            The nonempty public identifier for the authenticated delegate.
        """
        ...

    async def list_pending_reviews(self, limit: int = 100) -> list[PendingReview]:
        """Return the caller's bounded catch-up snapshot.

        Args:
            limit: Maximum number of pending reviews requested.

        Returns:
            The pending reviews visible to the authenticated delegate.
        """
        ...

    async def mint_ws_token(self) -> WsToken:
        """Mint one one-shot WebSocket credential.

        Returns:
            The token and expiry used for one WebSocket authentication.
        """
        ...

    def read_access_token(self) -> SecretStr:
        """Read one current access credential.

        Returns:
            The current bearer token with secret-safe representation.
        """
        ...

    async def aclose(self) -> None:
        """Close owned HTTP resources."""
        ...


class RunnerWakeClient(Protocol):
    """Structural reconnecting wake-client surface used by the lifecycle."""

    async def run(self, callbacks: WakeCallbacks) -> None:
        """Run reconnecting sessions until closed.

        Args:
            callbacks: Nonblocking lifecycle and frame callbacks.
        """
        ...

    async def close(self) -> None:
        """Close active wake resources."""
        ...


class RunnerChatClient(ChatCompletionClient, Protocol):
    """Chat-completions surface with explicit lifetime closure."""

    async def aclose(self) -> None:
        """Close owned vendor HTTP resources."""
        ...


def _default_chat_factory(
    base_url: str,
    api_key_file: str,
    endpoint_path: str,
) -> RunnerChatClient:
    """Build the production standard chat-completions client."""
    return ChatCompletionsClient(
        base_url,
        api_key_file,
        endpoint_path=endpoint_path,
    )


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC wall clock."""
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RunnerTuning:
    """Configure bounded local retry, queue, fanout, and quota behavior."""

    quota_cooloff_seconds: float = 60.0
    identity_backoff_base_seconds: float = 1.0
    identity_backoff_cap_seconds: float = 30.0
    inbox_capacity: int = 100
    fanout_delay_seconds: float = 30.0


@dataclass(frozen=True, slots=True)
class RunnerRuntime:
    """Group injectable network clients and deterministic time seams."""

    control_client: RunnerControlClient | None = None
    wake_client: RunnerWakeClient | None = None
    mcp_bridge: MCPBridgeClient | None = None
    chat_client: RunnerChatClient | None = None
    chat_factory: Callable[[str, str, str], RunnerChatClient] = _default_chat_factory
    clock: Callable[[], datetime] = _utc_now
    monotonic: Callable[[], float] = time.monotonic
    tuning: RunnerTuning = RunnerTuning()


class DelegateRunnerParameters(TypedDict):
    """Type keyword-only runner options without exceeding function arg limits."""

    max_tool_rounds: int
    endpoint_path: NotRequired[str]
    snapper_base_url: NotRequired[str]
    runtime: NotRequired[RunnerRuntime]


class DelegateRunner:
    """Run a fail-closed delegate that remains live across recoverable failures."""

    def __init__(
        self,
        model_alias: str,
        base_url: str,
        api_key_file: str,
        delegate_token_file: str,
        **parameters: Unpack[DelegateRunnerParameters],
    ) -> None:
        """Initialize an inert delegate lifecycle and its local status."""
        self.model_alias = model_alias
        self.base_url = base_url
        self.endpoint_path = parameters.get("endpoint_path", "/v1/chat/completions")
        self.api_key_file = api_key_file
        self.delegate_token_file = delegate_token_file
        self.max_tool_rounds = parameters["max_tool_rounds"]
        self.snapper_base_url = parameters.get("snapper_base_url", "")
        self._runtime = parameters.get("runtime", RunnerRuntime())
        self._stop_event = asyncio.Event()
        self._state = RunnerState.STOPPED
        self._running = False
        self._ws_connected = False
        self._heartbeat_count = 0
        self._last_wake_at: str | None = None
        self._consults_processed = 0
        self._consults_skipped = 0
        self._quota_degraded = False
        self._quota_retry_at = 0.0
        self._control_client = self._runtime.control_client
        self._wake_client = self._runtime.wake_client
        self._mcp_bridge = self._runtime.mcp_bridge
        self._chat_client = self._runtime.chat_client
        self._consult_runner: BoundedConsultRunner | None = None
        self._inbox: ReviewInbox | None = None
        self._startup_sweep_task: asyncio.Task[None] | None = None
        self._control = ControlGate()
        self._signal_close_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Run until stopped while containing transport, auth, and quota failures."""
        if self._running:
            raise RuntimeError("Delegate runner is already running")
        self._stop_event.clear()
        self._running = True
        stop_signals: tuple[signal.Signals, ...] = ()
        try:
            stop_signals = self._install_stop_signal_handlers()
            logger.info("Delegate runner started")
            if not self.snapper_base_url:
                await self._idle_heartbeat_loop()
            else:
                await self._configured_loop()
        finally:
            await self._shutdown_resources()
            self._running = False
            self._ws_connected = False
            self._state = RunnerState.STOPPED
            self._remove_stop_signal_handlers(stop_signals)
            logger.info("Delegate runner stopped")

    async def stop(self) -> None:
        """Request prompt shutdown and close active network waits."""
        self._stop_event.set()
        await self._cancel_startup_sweep()
        await self._close_wake_resources()
        if self._inbox is not None:
            await self._inbox.close()
        await asyncio.sleep(0)

    def get_status(self) -> dict[str, object]:
        """Return JSON-compatible lifecycle, liveness, control, and consult counters.

        Control state is reported because held-versus-active is the fact an
        operator most needs after pausing a runner, and reading it out of logs
        is not a check anyone can make quickly during an incident.

        Returns:
            A snapshot of externally visible runner state and counters.
        """
        applied = self._control.applied
        return {
            "state": self._state.value,
            "running": self._running,
            "ws_connected": self._ws_connected,
            "heartbeat_count": self._heartbeat_count,
            "last_wake_at": self._last_wake_at,
            "consults_processed": self._consults_processed,
            "consults_skipped": self._consults_skipped,
            "quota_degraded": self._quota_degraded,
            "accepts_consults": self._control.accepts_consults,
            "control_state": applied.state.value if applied is not None else None,
            "control_revision": applied.revision if applied is not None else None,
        }

    async def _configured_loop(self) -> None:
        """Resolve identity, sweep pending work, and run wake plus consult tasks."""
        self._state = RunnerState.CONNECTING
        self._initialize_clients()
        delegate_public_id = await self._resolve_delegate_identity()
        if delegate_public_id is None:
            return
        self._bind_control_identity(delegate_public_id)
        inbox = ReviewInbox(
            delegate_public_id,
            self._runtime.tuning.inbox_capacity,
            self._runtime.clock,
        )
        self._inbox = inbox
        worker = asyncio.create_task(self._consult_worker(inbox))
        callbacks = WakeCallbacks(
            connection_state=self._on_connection_state,
            subscribed=self._on_subscribed,
            frame=self._on_wake_frame,
            heartbeat=self._on_heartbeat,
            control=self._on_control,
        )
        try:
            await self._run_wake_until_stopped(callbacks)
        finally:
            await self._cancel_startup_sweep()
            await inbox.close()
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            self._inbox = None

    def _bind_control_identity(self, delegate_public_id: str) -> None:
        """Give the wake client the identity that addresses its control topic.

        Identity is only known after the control plane answers, but the wake
        client is built before that so it can be injected in tests. Rebuilding
        it here would discard an injected double, and leaving it unbound would
        silently drop the delegate-scoped subscription: the runner would take
        its boot state and then never hear a live hold. So the identity is
        pushed into the existing client instead.

        Args:
            delegate_public_id: Identity this runner authenticated as.
        """
        client = self._wake_client
        if isinstance(client, WakeClient):
            client.bind_delegate_identity(delegate_public_id)

    def _initialize_clients(self) -> None:
        """Build only missing production clients without reading credential files."""
        if self._control_client is None:
            self._control_client = SnapperControlClient(
                self.snapper_base_url,
                self.delegate_token_file,
            )
        if self._mcp_bridge is None:
            self._mcp_bridge = MCPBridge(self.snapper_base_url)
        if self._wake_client is None:
            self._wake_client = WakeClient(
                self.snapper_base_url,
                self._control_client,
            )

    async def _resolve_delegate_identity(self) -> str | None:
        """Retry identity fail-closed with capped exponential backoff."""
        attempt = 0
        while not self._stop_event.is_set():
            identity = await self._fetch_identity_or_stop()
            if identity is None:
                return None
            if identity.strip():
                return identity.strip()
            logger.warning("Delegate identity unavailable; retrying")
            delay = _identity_backoff(attempt, self._runtime.tuning)
            attempt += 1
            await self._wait_for_stop(delay)
        return None

    async def _fetch_identity_or_stop(self) -> str | None:
        """Fetch one identity while making the stop event authoritative."""
        identity_task = asyncio.create_task(self._required_control().fetch_delegate_identity())
        stop_task = asyncio.create_task(self._stop_event.wait())
        try:
            await asyncio.wait(
                (identity_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._stop_event.is_set():
                return None
            try:
                return await identity_task
            except ControlPlaneError:
                return ""
        finally:
            identity_task.cancel()
            stop_task.cancel()
            await asyncio.gather(identity_task, stop_task, return_exceptions=True)

    async def _run_wake_until_stopped(self, callbacks: WakeCallbacks) -> None:
        """Keep an unexpectedly returning wake boundary alive until shutdown."""
        while not self._stop_event.is_set():
            try:
                await self._required_wake().run(callbacks)
            except Exception:
                logger.warning("Delegate wake client failed; retrying")
            if not self._stop_event.is_set():
                await self._wait_for_stop(self._runtime.tuning.identity_backoff_base_seconds)

    async def _sweep_pending(self) -> None:
        """Offer each still-pending catch-up row without failing the wake session."""
        try:
            pending = await self._required_control().list_pending_reviews()
        except ControlPlaneError as error:
            logger.warning(
                "Delegate pending sweep unavailable status={} code={}",
                error.status_code,
                error.error_code,
            )
            return
        for item in pending:
            if item.status != "pending":
                continue
            await self._offer_context(_pending_context(item))

    async def _consult_worker(self, inbox: ReviewInbox) -> None:
        """Process eligible consults serially without blocking WebSocket liveness.

        Duty is re-checked after the queue hands work over, not only before it
        was queued. A hold can land while a context sits waiting, and starting
        it then would spend a model call the operator had already stopped.
        """
        while not self._stop_event.is_set():
            context = await inbox.get()
            if context is None:
                return
            if not self._control.accepts_consults:
                self._consults_skipped += 1
                logger.info("Delegate was held before this consult started; dropping it")
                continue
            try:
                await self._process_consult(context)
            except Exception:
                logger.warning(
                    "Delegate consult context failed review_id={}",
                    context.review_public_id,
                )
                self._consults_skipped += 1

    async def _process_consult(self, context: ReviewConsultContext) -> None:
        """Apply quota cool-off and record one bounded consult outcome."""
        if self._quota_degraded and self._runtime.monotonic() < self._quota_retry_at:
            self._consults_skipped += 1
            return
        if self._quota_degraded:
            self._quota_degraded = False
            self._set_operational_state()
        consult_runner = self._get_consult_runner()
        if consult_runner is None:
            self._consults_skipped += 1
            return
        logger.info(
            "Processing delegate review review_id={} signal_size={}",
            context.review_public_id,
            _safe_signal_size(context),
        )
        try:
            result = await consult_runner.run(context)
        except Exception:
            logger.warning(
                "Delegate consult failed review_id={}",
                context.review_public_id,
            )
            self._consults_skipped += 1
            return
        if result.outcome is ConsultOutcome.SUBMITTED:
            self._consults_processed += 1
            return
        self._consults_skipped += 1
        if result.outcome is ConsultOutcome.QUOTA_DEGRADED:
            self._quota_degraded = True
            self._quota_retry_at = (
                self._runtime.monotonic() + self._runtime.tuning.quota_cooloff_seconds
            )
            self._state = RunnerState.DEGRADED

    def _get_consult_runner(self) -> BoundedConsultRunner | None:
        """Build chat orchestration lazily so missing keys never block liveness."""
        if self._consult_runner is not None:
            return self._consult_runner
        if self._chat_client is None:
            try:
                self._chat_client = self._runtime.chat_factory(
                    self.base_url,
                    self.api_key_file,
                    self.endpoint_path,
                )
            except ApiKeyFileError:
                logger.warning("Delegate model credential is unavailable")
                return None
        self._consult_runner = BoundedConsultRunner(
            ConsultConfiguration(self.model_alias, self.max_tool_rounds),
            self._chat_client,
            self._required_mcp(),
            self._required_control(),
            self._runtime.clock,
        )
        return self._consult_runner

    async def _on_connection_state(self, connected: bool) -> None:
        """Reflect physical WebSocket state without overriding quota degradation.

        Losing the socket also forgets the applied directive: the state may
        change while the runner is not listening, so the next session must
        re-learn it before any consult is accepted. Forgetting the directive is
        not enough on its own, though — work already queued would still be
        waiting for a worker that no longer knows it is held. So the drop drains
        that queue too, which makes the returned-to-held state mean the same
        thing as an ordered hold rather than a weaker version of it.
        """
        self._ws_connected = connected
        if not connected:
            self._control.reset_for_reconnect()
            await self._cancel_startup_sweep()
            inbox = self._inbox
            if inbox is not None:
                await inbox.drain_pending()
        if self._running and not self._quota_degraded:
            self._state = RunnerState.CONNECTING
        await asyncio.sleep(0)

    async def _on_control(self, directive: ControlDirective | None) -> ControlDirective | None:
        """Adopt one server directive and start or stop consult duty.

        The return value is what the client echoes, and it is deliberately the
        directive now in force rather than the one just received. A stale or
        replayed revision is refused here, and echoing it would announce a
        superseded revision as applied; reporting the real applied state instead
        keeps the echo honest and lets any later frame repair an echo that was
        lost in transit.

        Gaining duty runs the catch-up sweep that subscription no longer runs,
        so pending work is picked up exactly when the runner is allowed to do
        it. That sweep is an owned background task rather than an inline await:
        catch-up is a network call on the same task that delivers wakes and
        heartbeats, and a slow control plane must never make a live runner look
        dead. Losing duty cancels that catch-up and clears queued work, because
        the hold contract is drain-current and accept-nothing-new: anything
        merely waiting is dropped rather than answered late.
        """
        changed = self._control.apply(directive)
        applied = self._control.applied
        if directive is None:
            logger.warning("Delegate control state unknown; remaining held")
            return applied
        if not changed:
            return applied
        if directive.state is ControlState.ACTIVE:
            logger.info("Delegate control state active at revision {}", directive.revision)
            await self._start_catch_up_sweep()
            return applied
        logger.warning("Delegate held at revision {}; declining new consults", directive.revision)
        await self._cancel_startup_sweep()
        inbox = self._inbox
        if inbox is not None:
            await inbox.drain_pending()
        return applied

    async def _start_catch_up_sweep(self) -> None:
        """Own one background catch-up sweep, replacing any earlier one.

        Duty can be granted again after a hold or a reconnect, so an earlier
        sweep is superseded rather than left to race the new one.
        """
        await self._cancel_startup_sweep()
        self._startup_sweep_task = asyncio.create_task(self._sweep_pending())

    async def _on_subscribed(self) -> None:
        """Mark a healthy subscription without assuming consult duty is granted.

        A fresh socket is not an accepting delegate. The catch-up sweep is the
        first thing that would spend tokens on work, so it waits for a control
        directive rather than riding on subscription alone.
        """
        self._ws_connected = True
        self._set_operational_state()

    async def _on_heartbeat(self) -> None:
        """Count one successfully sent application liveness ping."""
        self._heartbeat_count += 1
        await asyncio.sleep(0)

    async def _on_wake_frame(self, frame: WakeFrame) -> bool:
        """Route request wakes and acknowledgements, reporting what was taken on.

        The boolean is what decides whether the client remembers this wake as
        delivered. Anything the runner refused — because it is held, or because
        the inbox had no room for it — is reported as not taken, so an identical
        re-push from the server is treated as a fresh chance rather than as a
        replay of work that was never actually done.

        Args:
            frame: One decoded server wake.

        Returns:
            Whether the runner accepted responsibility for this frame.
        """
        self._last_wake_at = self._runtime.clock().isoformat()
        inbox = self._inbox
        if inbox is None:
            return False
        if isinstance(frame, AiReviewDecisionAckFrame):
            await inbox.acknowledge(frame.review_public_id, frame.dispatch_version)
            return True
        if not self._control.accepts_consults:
            self._consults_skipped += 1
            logger.info("Delegate is held; declining consult wake")
            return False
        return await self._offer_context(_wake_context(frame, self._runtime.tuning))

    async def _offer_context(self, context: ReviewConsultContext) -> bool:
        """Offer one normalized context and count terminal local drops.

        Args:
            context: Normalized consultation context to queue.

        Returns:
            Whether the inbox took the context on rather than dropping it.
        """
        inbox = self._inbox
        if inbox is None:
            return False
        outcome = await inbox.offer(context)
        if outcome in (
            InboxOfferOutcome.EXPIRED,
            InboxOfferOutcome.CAPACITY,
            InboxOfferOutcome.CLOSED,
        ):
            self._consults_skipped += 1
            return False
        return True

    def _set_operational_state(self) -> None:
        """Select subscribed or degraded state from current quota status."""
        if self._quota_degraded:
            self._state = RunnerState.DEGRADED
        elif self._ws_connected:
            self._state = RunnerState.SUBSCRIBED
        else:
            self._state = RunnerState.CONNECTING

    async def _idle_heartbeat_loop(self) -> None:
        """Remain safely idle for managed configurations without a Snapper origin."""
        self._state = RunnerState.IDLE
        while not self._stop_event.is_set():
            self._heartbeat_count += 1
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=_HEARTBEAT_INTERVAL_SECONDS,
                )
            except TimeoutError:
                continue

    async def _wait_for_stop(self, delay: float) -> None:
        """Wait through local backoff while remaining promptly stoppable."""
        if delay <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
        except TimeoutError:
            return

    async def _shutdown_resources(self) -> None:
        """Close every owned or injected async client without masking shutdown."""
        await self._cancel_startup_sweep()
        if self._inbox is not None:
            await self._inbox.close()
        await self._close_wake_resources()
        if self._chat_client is not None:
            try:
                await self._chat_client.aclose()
            except Exception:
                logger.warning("Delegate model client close failed")
        if self._control_client is not None:
            try:
                await self._control_client.aclose()
            except Exception:
                logger.warning("Delegate control client close failed")

    async def _cancel_startup_sweep(self) -> None:
        """Cancel and drain the runner-owned startup catch-up task."""
        task = self._startup_sweep_task
        if task is None:
            return
        self._startup_sweep_task = None
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _close_wake_resources(self) -> None:
        """Drain signal-owned closure or close an initialized wake client directly."""
        signal_task = self._signal_close_task
        if signal_task is not None:
            self._signal_close_task = None
            await asyncio.gather(signal_task, return_exceptions=True)
            return
        if self._wake_client is not None:
            await self._close_wake_client()

    async def _close_wake_client(self) -> None:
        """Close the wake client while containing transport cleanup failures."""
        try:
            await self._required_wake().close()
        except Exception:
            logger.warning("Delegate wake client close failed")

    def _required_control(self) -> RunnerControlClient:
        """Return the initialized control client or expose a programming error."""
        if self._control_client is None:
            raise RuntimeError("Control client is not initialized")
        return self._control_client

    def _required_wake(self) -> RunnerWakeClient:
        """Return the initialized wake client or expose a programming error."""
        if self._wake_client is None:
            raise RuntimeError("Wake client is not initialized")
        return self._wake_client

    def _required_mcp(self) -> MCPBridgeClient:
        """Return the initialized MCP bridge or expose a programming error."""
        if self._mcp_bridge is None:
            raise RuntimeError("MCP bridge is not initialized")
        return self._mcp_bridge

    def _request_signal_stop(self) -> None:
        """Translate a process signal into stop state and active wake closure."""
        self._stop_event.set()
        if self._wake_client is not None and self._signal_close_task is None:
            self._signal_close_task = asyncio.create_task(self._close_wake_client())

    def _install_stop_signal_handlers(self) -> tuple[signal.Signals, ...]:
        """Route managed-process termination signals through the stop event."""
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for stop_signal in (signal.SIGTERM,):
            try:
                loop.add_signal_handler(stop_signal, self._request_signal_stop)
            except RuntimeError:
                continue
            installed.append(stop_signal)
        return tuple(installed)

    @staticmethod
    def _remove_stop_signal_handlers(stop_signals: tuple[signal.Signals, ...]) -> None:
        """Remove lifecycle signal handlers installed for this runner."""
        loop = asyncio.get_running_loop()
        for stop_signal in stop_signals:
            loop.remove_signal_handler(stop_signal)


def _identity_backoff(attempt: int, tuning: RunnerTuning) -> float:
    """Return a capped exponential identity retry delay."""
    exponent = min(attempt, 16)
    exponential = tuning.identity_backoff_base_seconds * float(2**exponent)
    return min(exponential, tuning.identity_backoff_cap_seconds)


def _pending_context(item: PendingReview) -> ReviewConsultContext:
    """Normalize one sparse REST catch-up row into consult context."""
    metadata: dict[str, JsonValue] = {}
    if item.instrument is not None:
        metadata["instrument"] = item.instrument
    return ReviewConsultContext(
        review_public_id=item.review_public_id,
        selected_delegate_public_id=item.selected_delegate_public_id,
        wallet_public_id=item.wallet_public_id,
        dispatch_version=item.dispatch_version,
        deadline=item.deadline,
        fanout_after=item.fanout_after,
        signal_envelope=item.signal_envelope or {},
        instrument_metadata=metadata,
    )


def _wake_context(
    frame: AiReviewRequestFrame,
    tuning: RunnerTuning,
) -> ReviewConsultContext:
    """Normalize one rich WebSocket request and derive its fanout boundary."""
    return ReviewConsultContext(
        review_public_id=frame.review_public_id,
        selected_delegate_public_id=frame.selected_delegate_public_id,
        wallet_public_id=frame.wallet_public_id,
        dispatch_version=frame.dispatch_version,
        deadline=frame.deadline,
        fanout_after=frame.timestamp + timedelta(seconds=tuning.fanout_delay_seconds),
        signal_envelope=frame.signal_envelope,
        instrument_metadata=frame.instrument_metadata,
        user_public_id=frame.user_public_id,
        strategy_public_id=frame.strategy_public_id,
        instrument_public_id=frame.instrument_public_id,
    )


def _signal_size(context: ReviewConsultContext) -> int:
    """Return compact UTF-8 signal size for value-safe operational logging."""
    encoded = json.dumps(
        context.signal_envelope,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return len(encoded.encode("utf-8"))


def _safe_signal_size(context: ReviewConsultContext) -> int | None:
    """Return a log-safe signal size even for malformed Unicode or JSON values."""
    try:
        return _signal_size(context)
    except (TypeError, ValueError):
        return None
