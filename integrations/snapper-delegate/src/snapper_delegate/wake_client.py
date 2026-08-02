"""Authenticated reconnecting WebSocket wake client for AI review frames."""

import asyncio
import json
import random
import uuid
from collections import OrderedDict
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Literal
from typing import Protocol
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from loguru import logger
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr
from pydantic import ValidationError
from websockets.asyncio.client import connect

from snapper_delegate.control_plane import WsToken
from snapper_delegate.json_types import JsonObject
from snapper_delegate.json_types import JsonValue

_RAW_FRAME_LIMIT_BYTES = 64 * 1024
_SIGNAL_ENVELOPE_LIMIT_BYTES = 16 * 1024
_TOPICS: list[JsonValue] = ["ai_reviews."]
_FRAME_MODEL_CONFIG = ConfigDict(
    extra="allow",
    frozen=True,
    strict=True,
    validate_default=True,
)


class WakeCredentials(Protocol):
    """Supply rotating credentials for WebSocket authentication."""

    async def mint_ws_token(self) -> WsToken:
        """Mint one fresh single-use WebSocket credential.

        Returns:
            The one-shot token and its expiry timestamp.
        """
        ...

    def read_access_token(self) -> SecretStr:
        """Read the current access bearer for one upgrade request.

        Returns:
            The current bearer token with secret-safe representation.
        """
        ...


class WakeSocket(Protocol):
    """Narrow asynchronous WebSocket surface used by one session."""

    async def send(self, message: str) -> None:
        """Send one encoded client frame.

        Args:
            message: Serialized text frame to transmit.
        """
        ...

    async def recv(self) -> str | bytes:
        """Receive one encoded server frame.

        Returns:
            The next text or binary frame from the server.
        """
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Close the socket gracefully.

        Args:
            code: WebSocket close status code.
            reason: Public close reason sent to the peer.
        """
        ...


@dataclass(frozen=True, slots=True)
class WakeConnectRequest:
    """Describe one authenticated WebSocket upgrade without repr secrets."""

    url: str
    additional_headers: dict[str, str] = field(repr=False)


class WakeConnectFactory(Protocol):
    """Open one WebSocket connection for an injected upgrade request."""

    async def __call__(self, request: WakeConnectRequest) -> WakeSocket:
        """Return one connected asynchronous socket."""
        ...


class _FrameModel(BaseModel):
    """Preserve JSON extension fields received from compatible servers."""

    model_config = _FRAME_MODEL_CONFIG

    __pydantic_extra__: dict[str, JsonValue] = Field(init=False)


class _ServerFrame(_FrameModel):
    """Parse the common type discriminator before specialized validation."""

    type: str = Field(min_length=1)


class _EnvelopeFrame(_ServerFrame):
    """Parse provenance fields required on server data frames."""

    session_id: str = Field(min_length=1)
    sequence_id: int = Field(ge=0)
    public_id: str = Field(min_length=1)
    timestamp: datetime
    topic: str | None


class AiReviewRequestFrame(_EnvelopeFrame):
    """Represent one WebSocket wake for a pending AI review."""

    type: Literal["ai_review.request"]
    review_public_id: str = Field(min_length=1)
    user_public_id: str = Field(min_length=1)
    strategy_public_id: str = Field(min_length=1)
    wallet_public_id: str = Field(min_length=1)
    instrument_public_id: str = Field(min_length=1)
    selected_delegate_public_id: str = Field(min_length=1)
    deadline: datetime
    signal_envelope: JsonObject
    instrument_metadata: JsonObject
    dispatch_version: int = Field(ge=0)


class AiReviewDecisionAckFrame(_EnvelopeFrame):
    """Represent one WebSocket acknowledgement for a terminal AI review."""

    type: Literal["ai_review.decision_ack"]
    review_public_id: str = Field(min_length=1)
    user_public_id: str = Field(min_length=1)
    strategy_public_id: str = Field(min_length=1)
    wallet_public_id: str = Field(min_length=1)
    instrument_public_id: str = Field(min_length=1)
    responding_delegate_public_id: str = Field(min_length=1)
    decision: str = Field(min_length=1)
    new_status: str = Field(min_length=1)
    resolution_mode: str = Field(min_length=1)
    rationale: str | None
    dispatch_version: int = Field(ge=0)


class _SubscriptionFrame(_ServerFrame):
    """Represent the shared subscription result response."""

    type: Literal["subscription_success"]
    action: str
    status: str
    topics: list[str]
    denied_topics: list[str] = []


type WakeFrame = AiReviewRequestFrame | AiReviewDecisionAckFrame
type _DecodedFrame = _ServerFrame | _SubscriptionFrame | WakeFrame


@dataclass(frozen=True, slots=True)
class WakeCallbacks:
    """Group nonblocking lifecycle hooks supplied by the delegate runner."""

    connection_state: Callable[[bool], Awaitable[None]]
    subscribed: Callable[[], Awaitable[None]]
    frame: Callable[[WakeFrame], Awaitable[None]]
    heartbeat: Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class WakeClientConfig:
    """Configure bounded protocol waits, liveness, reconnect, and dedup state."""

    heartbeat_interval_seconds: float = 7.0
    handshake_timeout_seconds: float = 15.0
    reauth_timeout_seconds: float = 15.0
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 30.0
    backoff_jitter_fraction: float = 0.25
    dedup_capacity: int = 10_000
    random_value: Callable[[], float] = field(default=random.random, repr=False)


class EnvelopeMinter:
    """Mint strict client provenance with process-stable sequencing."""

    def __init__(self, clock: Callable[[], datetime] | None = None) -> None:
        """Initialize one process session and monotonic sequence."""
        self._clock = clock or _utc_now
        self._session_id = str(uuid.uuid7())
        self._sequence_id = 0

    def next(self) -> JsonObject:
        """Return a fresh strict envelope for one client-to-server frame.

        Returns:
            New provenance with a stable session and incremented sequence.
        """
        self._sequence_id += 1
        return {
            "session_id": self._session_id,
            "sequence_id": self._sequence_id,
            "public_id": str(uuid.uuid7()),
            "timestamp": self._clock().isoformat(),
            "topic": None,
        }


class WakeSessionError(RuntimeError):
    """Signal a credential-free recoverable WebSocket session failure."""


async def default_wake_connect(request: WakeConnectRequest) -> WakeSocket:
    """Open one production WebSocket without implicit proxy or protocol pings.

    Args:
        request: Authenticated URL and headers for the connection upgrade.

    Returns:
        The connected asynchronous WebSocket.
    """
    return await connect(
        request.url,
        additional_headers=request.additional_headers,
        proxy=None,
        ping_interval=None,
        max_size=_RAW_FRAME_LIMIT_BYTES,
    )


class WakeClient:
    """Maintain one authenticated, subscribed, and live wake connection."""

    def __init__(
        self,
        snapper_base_url: str,
        credentials: WakeCredentials,
        *,
        connect_factory: WakeConnectFactory | None = None,
        config: WakeClientConfig | None = None,
        envelope_minter: EnvelopeMinter | None = None,
    ) -> None:
        """Initialize an inert reconnecting client with injectable network seams."""
        self._url = _websocket_url(snapper_base_url)
        self._credentials = credentials
        self._connect_factory = connect_factory or default_wake_connect
        self._config = config or WakeClientConfig()
        if self._config.dedup_capacity < 1:
            raise ValueError("dedup_capacity must be positive")
        self._minter = envelope_minter or EnvelopeMinter()
        self._closed = asyncio.Event()
        self._active_socket: WakeSocket | None = None
        self._session_task: asyncio.Task[None] | None = None
        self._dedup: OrderedDict[str, int] = OrderedDict()
        self._reconnect_attempt = 0
        self._running = False

    async def run(self, callbacks: WakeCallbacks) -> None:
        """Reconnect until a clean close while containing session failures.

        Args:
            callbacks: Nonblocking lifecycle and frame callbacks.
        """
        if self._running:
            raise RuntimeError("Wake client is already running")
        self._running = True
        try:
            await self._run_forever(callbacks)
        finally:
            self._running = False
            await self._close_active()

    async def close(self) -> None:
        """Stop reconnect waits and close the active socket promptly."""
        self._closed.set()
        await self._close_active()

    async def _run_forever(self, callbacks: WakeCallbacks) -> None:
        """Run recoverable sessions with jittered exponential backoff."""
        while not self._closed.is_set():
            session_task = asyncio.create_task(self._run_session(callbacks))
            self._session_task = session_task
            try:
                await self._await_session_or_close(session_task)
            except Exception:
                logger.warning("Delegate wake session failed; reconnecting")
            finally:
                if self._session_task is session_task:
                    self._session_task = None
            if self._closed.is_set():
                return
            delay = _backoff_seconds(self._reconnect_attempt, self._config)
            self._reconnect_attempt += 1
            await self._wait_for_close(delay)

    async def _await_session_or_close(self, session_task: asyncio.Task[None]) -> None:
        """Await one session while giving lifecycle closure ownership of shutdown."""
        close_task = asyncio.create_task(self._closed.wait())
        try:
            await asyncio.wait(
                (session_task, close_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not self._closed.is_set():
                await session_task
        finally:
            session_task.cancel()
            close_task.cancel()
            await asyncio.gather(session_task, close_task, return_exceptions=True)

    async def _run_session(self, callbacks: WakeCallbacks) -> None:
        """Authenticate, subscribe, sweep, and stream one socket session."""
        ws_token = await self._credentials.mint_ws_token()
        if self._closed.is_set():
            return
        access_token = self._credentials.read_access_token().get_secret_value()
        request = WakeConnectRequest(
            url=self._url,
            additional_headers={"Authorization": f"Bearer {access_token}"},
        )
        socket = await self._connect_factory(request)
        self._active_socket = socket
        await self._notify_connection(callbacks, True)
        try:
            await self._handshake(socket, ws_token.value)
            self._reconnect_attempt = 0
            subscription_task = asyncio.create_task(self._notify_subscribed(callbacks))
            try:
                await self._stream(socket, callbacks)
            finally:
                subscription_task.cancel()
                await asyncio.gather(subscription_task, return_exceptions=True)
        finally:
            if self._active_socket is socket:
                self._active_socket = None
            await _safe_socket_close(socket)
            await self._notify_connection(callbacks, False)

    async def _handshake(self, socket: WakeSocket, ws_token: SecretStr) -> None:
        """Complete auth only on auth_complete and require a healthy subscribe ack."""
        async with asyncio.timeout(self._config.handshake_timeout_seconds):
            await self._wait_for_type(socket, "auth_required")
            await self._send(
                socket,
                {"type": "authenticate", "ws_token": ws_token.get_secret_value()},
            )
            await self._wait_for_type(socket, "auth_complete")
            await self._send(socket, {"type": "subscribe", "topics": _TOPICS})
            subscription = await self._wait_for_type(socket, "subscription_success")
        if not _healthy_subscription(subscription):
            raise WakeSessionError("subscription rejected")

    async def _wait_for_type(self, socket: WakeSocket, expected: str) -> _DecodedFrame:
        """Wait for one control type while ignoring malformed and future frames."""
        while True:
            frame = await self._receive(socket)
            if frame is None:
                continue
            if frame.type in ("auth_failed", "auth_expired"):
                raise WakeSessionError("authentication failed")
            if frame.type == expected:
                return frame

    async def _stream(self, socket: WakeSocket, callbacks: WakeCallbacks) -> None:
        """Receive frames while independent ping and reauthentication tasks run."""
        ping_task = asyncio.create_task(self._ping_loop(socket, callbacks))
        reauth_task: asyncio.Task[None] | None = None
        reauth_ok = asyncio.Event()
        try:
            while not self._closed.is_set():
                frame = await self._receive(socket)
                if frame is None:
                    continue
                if frame.type in ("auth_expired", "auth_failed"):
                    raise WakeSessionError("authentication expired")
                if frame.type == "reauth_required" and (reauth_task is None or reauth_task.done()):
                    reauth_ok.clear()
                    reauth_task = asyncio.create_task(self._reauthenticate(socket, reauth_ok))
                    continue
                if frame.type == "reauth_ok":
                    reauth_ok.set()
                    continue
                if isinstance(frame, AiReviewRequestFrame | AiReviewDecisionAckFrame):
                    await self._deliver(frame, callbacks)
        finally:
            tasks = [ping_task]
            if reauth_task is not None:
                tasks.append(reauth_task)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _reauthenticate(self, socket: WakeSocket, reauth_ok: asyncio.Event) -> None:
        """Mint and send one in-socket reauthentication or close on failure."""
        try:
            ws_token = await self._credentials.mint_ws_token()
            await self._send(
                socket,
                {"type": "reauth", "ws_token": ws_token.value.get_secret_value()},
            )
            await asyncio.wait_for(
                reauth_ok.wait(),
                timeout=self._config.reauth_timeout_seconds,
            )
        except Exception:
            await _safe_socket_close(socket, 4001, "reauthentication failed")

    async def _ping_loop(self, socket: WakeSocket, callbacks: WakeCallbacks) -> None:
        """Send strict application pings frequently enough for delegate admission."""
        while not self._closed.is_set():
            try:
                await asyncio.wait_for(
                    self._closed.wait(),
                    timeout=self._config.heartbeat_interval_seconds,
                )
                return
            except TimeoutError:
                try:
                    await self._send(socket, {"type": "ping"})
                    await self._notify_heartbeat(callbacks)
                except Exception:
                    await _safe_socket_close(socket)
                    return

    async def _deliver(self, frame: WakeFrame, callbacks: WakeCallbacks) -> None:
        """Deliver only newer review frame versions and commit dedup afterward."""
        key = f"{frame.type}:{frame.review_public_id}"
        previous = self._dedup.get(key)
        if previous is not None and frame.dispatch_version <= previous:
            return
        try:
            await callbacks.frame(frame)
        except Exception:
            logger.warning(
                "Delegate wake delivery failed for review_id={}",
                frame.review_public_id,
            )
            return
        if key in self._dedup:
            self._dedup.pop(key)
        elif len(self._dedup) >= self._config.dedup_capacity:
            self._dedup.popitem(last=False)
        self._dedup[key] = frame.dispatch_version

    async def _receive(self, socket: WakeSocket) -> _DecodedFrame | None:
        """Receive and locally validate one bounded server frame."""
        raw = await socket.recv()
        return _decode_frame(raw)

    async def _send(self, socket: WakeSocket, payload: JsonObject) -> None:
        """Attach the mandatory strict envelope and send one compact JSON frame."""
        frame = {**payload, **self._minter.next()}
        await socket.send(json.dumps(frame, separators=(",", ":")))

    async def _wait_for_close(self, delay: float) -> None:
        """Wait through reconnect backoff while remaining promptly stoppable."""
        if delay <= 0:
            await asyncio.sleep(0)
            return
        try:
            await asyncio.wait_for(self._closed.wait(), timeout=delay)
        except TimeoutError:
            return

    async def _close_active(self) -> None:
        """Close the current socket if one exists."""
        socket = self._active_socket
        if socket is not None:
            await _safe_socket_close(socket)

    @staticmethod
    async def _notify_connection(callbacks: WakeCallbacks, connected: bool) -> None:
        """Contain status callback failures inside the wake boundary."""
        try:
            await callbacks.connection_state(connected)
        except Exception:
            logger.warning("Delegate wake connection callback failed")

    @staticmethod
    async def _notify_subscribed(callbacks: WakeCallbacks) -> None:
        """Contain reconnect sweep callback failures inside the wake boundary."""
        try:
            await callbacks.subscribed()
        except Exception:
            logger.warning("Delegate wake subscription callback failed")

    @staticmethod
    async def _notify_heartbeat(callbacks: WakeCallbacks) -> None:
        """Contain heartbeat counter callback failures inside the wake boundary."""
        try:
            await callbacks.heartbeat()
        except Exception:
            logger.warning("Delegate wake heartbeat callback failed")


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC wall clock."""
    return datetime.now(UTC)


def _websocket_url(base_url: str) -> str:
    """Convert one HTTP control origin to the fixed WebSocket endpoint."""
    parsed = urlsplit(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    return urlunsplit((scheme, parsed.netloc, "/api/ws", "", ""))


def _healthy_subscription(frame: _DecodedFrame) -> bool:
    """Accept only a nonempty subscribed or partial subscribe result."""
    return (
        isinstance(frame, _SubscriptionFrame)
        and frame.action == "subscribe"
        and frame.status in ("subscribed", "partial")
        and bool(frame.topics)
    )


def _decode_frame(raw: str | bytes) -> _DecodedFrame | None:
    """Parse a bounded known frame and silently discard malformed or unknown data."""
    try:
        size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
    except UnicodeError:
        return None
    if size > _RAW_FRAME_LIMIT_BYTES:
        logger.warning("Dropping oversized delegate wake frame size={}", size)
        return None
    try:
        discriminator = _ServerFrame.model_validate_json(raw)
    except ValidationError:
        logger.warning("Dropping malformed delegate wake frame")
        return None
    model = _frame_model(discriminator.type)
    if model is None:
        return discriminator
    try:
        frame = model.model_validate_json(raw)
    except ValidationError:
        logger.warning("Dropping malformed known delegate wake frame")
        return None
    if (
        isinstance(frame, AiReviewRequestFrame)
        and _signal_size(frame) > _SIGNAL_ENVELOPE_LIMIT_BYTES
    ):
        logger.warning(
            "Dropping oversized review signal review_id={} size={}",
            frame.review_public_id,
            _signal_size(frame),
        )
        return None
    return frame


def _frame_model(type_name: str) -> type[_DecodedFrame] | None:
    """Return the specialized local model for one known discriminator."""
    if type_name == "ai_review.request":
        return AiReviewRequestFrame
    if type_name == "ai_review.decision_ack":
        return AiReviewDecisionAckFrame
    if type_name == "subscription_success":
        return _SubscriptionFrame
    return None


def _signal_size(frame: AiReviewRequestFrame) -> int:
    """Return the UTF-8 size of one signal envelope without retaining it."""
    encoded = json.dumps(
        frame.signal_envelope,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return len(encoded.encode("utf-8"))


def _backoff_seconds(attempt: int, config: WakeClientConfig) -> float:
    """Return capped exponential reconnect delay with symmetric jitter."""
    exponent = min(attempt, 16)
    exponential = config.backoff_base_seconds * float(2**exponent)
    base = min(exponential, config.backoff_cap_seconds)
    jitter = base * config.backoff_jitter_fraction * (config.random_value() * 2 - 1)
    delay: float = max(0.0, base + jitter)
    return delay


async def _safe_socket_close(
    socket: WakeSocket,
    code: int = 1000,
    reason: str = "client shutdown",
) -> None:
    """Contain close-handshake failures inside the transport boundary."""
    try:
        await socket.close(code, reason)
    except Exception:
        logger.warning("Delegate wake socket close failed")
