"""Persistent APNs HTTP/2 connection pool for iOS Push Foundation.

Wraps ``aioapns.APNs`` with a small, opinionated surface: one
``ApnsClientPool`` instance owns one sandbox client + one production
client (sharing the same PEM private key, only the ``use_sandbox``
flag differs) and routes sends by the target device's recorded
``env`` column. Constructed inside an async context (the
``aioapns.APNs`` constructor calls ``asyncio.get_event_loop()``
internally and raises on Py 3.12+ if invoked outside a running loop).
Returned by ``build_apns_client_pool`` rather than a classmethod so
tests can fake out both ends without needing an event loop.
"""

import asyncio
import contextlib
import errno
import socket
import ssl
from dataclasses import dataclass
from typing import Final

import httpx
from aioapns import APNs as _AioApns
from aioapns import NotificationRequest
from aioapns.common import NotificationResult
from aioapns.common import PushType
from aioapns.connection import APNsBaseClientProtocol
from aioapns.connection import APNsBaseConnectionPool
from aioapns.connection import APNsDevelopmentClientProtocol
from aioapns.connection import APNsKeyConnectionPool
from aioapns.connection import APNsProductionClientProtocol
from aioapns.connection import APNsTLSClientProtocol
from aioapns.exceptions import ConnectionClosed as ApnsConnectionClosed
from aioapns.exceptions import ConnectionError as ApnsConnectionError
from aioapns.exceptions import MaxAttemptsExceeded as ApnsMaxAttemptsExceeded
from h2.exceptions import ProtocolError

from snapper.application.notify.apns_config import ApnsConfig
from snapper.core.json_types import JsonObject

_NETWORK_ERROR_TYPES: Final[tuple[type[BaseException], ...]] = (
    ApnsMaxAttemptsExceeded,
    ApnsConnectionError,
    ApnsConnectionClosed,
    TimeoutError,
    ConnectionError,
    socket.gaierror,
    httpx.TransportError,
)

_GENERIC_NETWORK_WRAPPERS: Final[tuple[type[BaseException], ...]] = (
    ApnsMaxAttemptsExceeded,
    ApnsConnectionError,
)

_NETWORK_OS_ERRNOS: Final[frozenset[int]] = frozenset(
    {
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.ETIMEDOUT,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
    }
)


def _next_exception(exc: BaseException) -> BaseException | None:
    """Return the explicit ``__cause__`` or unsuppressed ``__context__`` of ``exc``."""
    if exc.__cause__ is not None:
        return exc.__cause__
    if not exc.__suppress_context__:
        return exc.__context__
    return None


def _is_ssl_node(exc: BaseException) -> bool:
    """Return whether ``exc`` or any nested argument exception is an ``ssl.SSLError``."""
    if isinstance(exc, ssl.SSLError):
        return True
    return any(isinstance(arg, ssl.SSLError) for arg in exc.args)


def _matches_network_exception(exc: BaseException) -> bool:
    """Return whether one exception node matches a transient network outage."""
    if isinstance(exc, _NETWORK_ERROR_TYPES):
        return True
    return isinstance(exc, OSError) and exc.errno in _NETWORK_OS_ERRNOS


def is_network_delivery_error(exc: BaseException) -> bool:
    """Classify whether a delivery exception belongs to the network outage class.

    Walks ``exc`` along ``__cause__`` and unsuppressed ``__context__``
    (respecting ``__suppress_context__``). Treats ``ssl.SSLError`` (including
    ``ssl.SSLCertVerificationError``) anywhere in the chain or nested
    exception args as non-network so permanent TLS faults do not retry for
    hours. Defers generic ``aioapns`` exhaustion/connection wrappers
    (``MaxAttemptsExceeded``, ``ConnectionError``) to their concrete deeper
    cause when one is available, while still treating bare wrappers as
    network-class.

    Args:
        exc: Exception raised while sending an APNs notification.

    Returns:
        ``True`` for transient network/transport outage errors that should retry
        until expiry; ``False`` for non-network, filesystem, and TLS errors.
    """
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        chain.append(current)
        seen.add(id(current))
        current = _next_exception(current)
    if any(_is_ssl_node(node) for node in chain):
        return False
    for index, node in enumerate(chain):
        has_deeper_cause = index + 1 < len(chain)
        if has_deeper_cause and isinstance(node, _GENERIC_NETWORK_WRAPPERS):
            continue
        if _matches_network_exception(node):
            return True
    return False


@dataclass(frozen=True, slots=True)
class ApnsSendResult:
    """Small, stable result envelope the sidecar logs and persists.

    The sidecar maps a subset of ``NotificationResult`` into this
    shape so the retry policy doesn't leak the
    ``aioapns``-specific response class into the outbox code path.

    Attributes:
        status_code: Numeric HTTP/2 status from APNs
            (200, 410, 429, 500, 503, etc.).
        status: Coarse status string (``success``, ``unregistered``,
            ``config_error``, ``throttled``, ``server_error``, ``other``,
            ``unknown``).
        apns_id: The server-minted ``apns-id`` echoed back on success,
            used to correlate with Apple's delivery diagnostics.
            Empty string when absent.
        description: APNs error reason (e.g. ``BadDeviceToken``) or
            empty string on success.

    Note:
        aioapns 4.0 does not expose the ``Retry-After`` header on
        429 responses; the sidecar applies its own backoff schedule
        for all throttled / server-error cases rather than
        reading Apple's hint. This is an acceptable trade-off because
        Apple's hint is advisory and the schedule is already
        exponential with a 5-minute cap.
    """

    status_code: int
    status: str
    apns_id: str
    description: str


class ApnsClientPool:
    """Two-client pool (sandbox + production) sharing one PEM key.

    Constructed inside an async context so ``aioapns``' internal
    ``asyncio.get_event_loop()`` call resolves to the running loop
    (compatibility note for Py 3.12+).

    Args:
        sandbox_client: Optional sandbox-bound ``aioapns.APNs``.
            ``None`` when the config's environment is ``production``
            only.
        production_client: Optional production-bound ``aioapns.APNs``.
            ``None`` when the config's environment is ``sandbox``
            only.

    Construction via ``build_apns_client_pool`` is preferred outside
    tests — it applies the PEM-shared, env-scoped defaults.
    """

    def __init__(
        self,
        sandbox_client: _AioApns | None,
        production_client: _AioApns | None,
    ) -> None:
        """Hold the two env-scoped aioapns clients (either may be None)."""
        if sandbox_client is None and production_client is None:
            raise ValueError(
                "ApnsClientPool needs at least one of sandbox_client "
                "or production_client — both None makes send() fail."
            )
        self._sandbox = sandbox_client
        self._production = production_client

    def _select_client(self, env: str) -> _AioApns:
        """Pick the client that matches the target device's env.

        Args:
            env: ``sandbox`` or ``prod`` (matches
                ``NotificationDevice.env``).

        Returns:
            The matching ``aioapns.APNs`` instance.

        Raises:
            ValueError: If the configured pool doesn't cover the
                requested env (e.g. device registered for ``prod``
                but only the sandbox client was built).
        """
        if env == "sandbox":
            if self._sandbox is None:
                raise ValueError(
                    "ApnsClientPool has no sandbox client but a "
                    "sandbox device send was requested."
                )
            return self._sandbox
        if env == "prod":
            if self._production is None:
                raise ValueError(
                    "ApnsClientPool has no production client but a "
                    "production device send was requested."
                )
            return self._production
        raise ValueError(f"Unknown APNs env {env!r} — must be 'sandbox' or 'prod'.")

    async def send(
        self,
        env: str,
        device_token: str,
        payload: JsonObject,
        apns_topic: str,
        priority: int = 10,
        push_type: str = "alert",
        collapse_id: str | None = None,
    ) -> ApnsSendResult:
        """Send one push notification through the env-matched client.

        Args:
            env: ``sandbox`` or ``prod`` — the device's recorded env.
            device_token: 64-hex APNs device token.
            payload: Full APNs ``aps``-dict payload (includes
                ``{"aps": {...}, ...custom}``). Caller is
                responsible for sizing — see APNs 4KB limit.
            apns_topic: Usually identical to the app bundle id
                (``ApnsConfig.topic``). Forwarded as the
                ``apns-topic`` HTTP/2 header.
            priority: APNs priority (5 = throttleable, 10 =
                immediate). Defaults to 10 per safety-critical
                delivery contract; non-critical senders may lower.
            push_type: APNs push type (``alert`` | ``background`` |
                ``voip`` | ...). Defaults to ``alert``.
            collapse_id: Optional APNs collapse-id — groups
                notifications with the same id so iOS shows only the
                latest. Maps 1:1 onto ``AlertEventData.thread_key``
                at the sidecar mapping layer.

        Returns:
            ``ApnsSendResult`` mapping the aioapns response into the
            coarse-grained shape the outbox persists.
        """
        request = NotificationRequest(
            device_token=device_token,
            message=payload,
            notification_id=None,
            time_to_live=None,
            push_type=PushType(push_type),
            priority=priority,
            collapse_key=collapse_id,
            apns_topic=apns_topic,
        )
        client = self._select_client(env)
        result: NotificationResult = await client.send_notification(request)
        return _map_result(result)


def _map_result(result: NotificationResult) -> ApnsSendResult:
    """Translate an ``aioapns`` ``NotificationResult`` into our envelope.

    Args:
        result: The raw ``aioapns`` response.

    Returns:
        An ``ApnsSendResult`` using coarse-grained ``status`` strings
        the outbox code path pattern-matches on.
    """
    description = result.description or ""
    status_code = _status_code_from(result.status)
    coarse = _coarse_status(status_code, description)
    notification_id = result.notification_id or ""
    return ApnsSendResult(
        status_code=status_code,
        status=coarse,
        apns_id=notification_id,
        description=description,
    )


def _status_code_from(status: str) -> int:
    """Map aioapns' string ``status`` onto the canonical HTTP/2 code.

    ``aioapns`` exposes the status as the numeric HTTP code rendered
    as a string (e.g. ``'200'``, ``'410'``, ``'429'``) — we cast it
    back to int so the retry policy can switch on the same
    integer buckets regardless of the underlying library. Unknown /
    empty strings collapse to 0 (handled as ``other`` coarse status).
    """
    try:
        return int(status)
    except (TypeError, ValueError):
        return 0


_UNREGISTERED_400_DESCRIPTIONS: Final[frozenset[str]] = frozenset(
    {
        "BadDeviceToken",
    }
)
"""APNs HTTP 400 reasons proving a single device token is permanently invalid."""

_CONFIG_ERROR_400_DESCRIPTIONS: Final[frozenset[str]] = frozenset(
    {
        "DeviceTokenNotForTopic",
    }
)
"""APNs HTTP 400 reasons indicating a server-side topic/bundle mismatch.

Never auto-deactivate devices on these reasons: a misconfigured ``apns-topic``
returns them for every valid device in the fleet.
"""


def _coarse_status(status_code: int, description: str) -> str:
    """Derive the coarse outbox status from the numeric HTTP/2 code and reason.

    Apple APNs returns HTTP 410 for ``Unregistered`` and HTTP 400
    ``BadDeviceToken`` for a permanently invalid device token; both map to
    ``unregistered`` so the sidecar deactivates that single unusable device
    immediately instead of exhausting its retry budget into ``failed``.
    HTTP 400 ``DeviceTokenNotForTopic`` maps to ``config_error`` so a
    misconfigured ``apns-topic`` never deactivates healthy devices.

    Args:
        status_code: Numeric HTTP/2 status code from APNs.
        description: APNs reason string (empty on 200).

    Returns:
        Coarse outbox status string (``success``, ``unregistered``,
        ``config_error``, ``throttled``, ``server_error``, ``other``, or
        ``unknown``).
    """
    if status_code == 200:
        return "success"
    if status_code == 410 or (status_code == 400 and description in _UNREGISTERED_400_DESCRIPTIONS):
        return "unregistered"
    if status_code == 400 and description in _CONFIG_ERROR_400_DESCRIPTIONS:
        return "config_error"
    if status_code == 429:
        return "throttled"
    if 500 <= status_code < 600:
        return "server_error"
    if status_code > 0 or description:
        return "other"
    return "unknown"


class _TimeoutSafeProtocolMixin(APNsTLSClientProtocol):
    """Prevent cancelled request futures from raising ``InvalidStateError`` or leaking slots.

    When an outer ``asyncio.wait_for`` times out, Python cancels the awaited
    ``Future`` in ``self.requests``. Stock ``aioapns`` calls ``set_result`` /
    ``set_exception`` without checking ``done()``, raising ``InvalidStateError``
    inside ``data_received`` before ``on_stream_ended`` can release
    ``free_channels``. This mixin prunes done futures before response and
    disconnect callbacks, cleans up stream mappings on ``StreamEnded``, and
    resets the stream + releases the slot once if cancelled before
    ``StreamEnded`` arrives.
    """

    def _prune_done_requests(self) -> None:
        """Drop already-completed or cancelled request futures before aioapns touches them."""
        done_ids = [nid for nid, fut in self.requests.items() if fut.done()]
        for nid in done_ids:
            self.requests.pop(nid, None)
            self.request_statuses.pop(nid, None)

    def _drop_stream_mapping(self, notification_id: str) -> int | None:
        """Pop and return the active ``stream_id`` for ``notification_id``, if any."""
        for stream_id, nid in list(self.request_streams.items()):
            if nid == notification_id:
                self.request_streams.pop(stream_id, None)
                return stream_id
        return None

    async def send_notification(self, request: NotificationRequest) -> NotificationResult:
        """Release the acquired stream slot once if cancelled before ``StreamEnded``."""
        try:
            return await super().send_notification(request)
        except asyncio.CancelledError:
            self.requests.pop(request.notification_id, None)
            self.request_statuses.pop(request.notification_id, None)
            stream_id = self._drop_stream_mapping(request.notification_id)
            if stream_id is not None:
                with contextlib.suppress(ProtocolError):
                    self.conn.reset_stream(stream_id)
                if self.transport is not None and not self.transport.is_closing():
                    self.transport.write(self.conn.data_to_send())
                self.free_channels.release()
            raise

    def on_response_received(self, headers: dict[bytes, bytes]) -> None:
        """Ignore late response headers for cancelled or unknown requests."""
        self._prune_done_requests()
        notification_id = headers.get(b"apns-id", b"").decode("utf8")
        if notification_id not in self.requests:
            return
        super().on_response_received(headers)

    def on_data_received(self, raw_data: bytes, stream_id: int) -> None:
        """Ignore late error payloads for cancelled requests without losing ``stream_id``."""
        self._prune_done_requests()
        notification_id = self.request_streams.get(stream_id)
        if notification_id is not None and notification_id not in self.requests:
            return
        super().on_data_received(raw_data, stream_id)

    def on_stream_ended(self, stream_id: int) -> None:
        """Forget the stream mapping when ``StreamEnded`` releases the slot."""
        notification_id = self.request_streams.pop(stream_id, None)
        if notification_id is not None:
            self.requests.pop(notification_id, None)
            self.request_statuses.pop(notification_id, None)
        super().on_stream_ended(stream_id)

    def connection_lost(self, exc: Exception | None) -> None:
        """Clear done futures and stream maps so disconnect never skips channel destroy."""
        self._prune_done_requests()
        self.request_streams.clear()
        self.request_statuses.clear()
        super().connection_lost(exc)


class _TimeoutSafeDevelopmentClientProtocol(
    _TimeoutSafeProtocolMixin, APNsDevelopmentClientProtocol
):
    """Sandbox APNs HTTP/2 protocol with timeout-safe stream slot cleanup."""


class _TimeoutSafeProductionClientProtocol(_TimeoutSafeProtocolMixin, APNsProductionClientProtocol):
    """Production APNs HTTP/2 protocol with timeout-safe stream slot cleanup."""


class _HardenedKeyConnectionPool(APNsKeyConnectionPool):
    """Key-auth APNs connection pool that propagates SSL/TLS failures directly."""

    async def acquire(self) -> APNsBaseClientProtocol:
        """Surface ``ssl.SSLError`` causes directly instead of retrying into ``MaxAttemptsExceeded``."""
        try:
            return await super().acquire()
        except ApnsConnectionError as exc:
            cause = exc.__cause__ if exc.__cause__ is not None else exc.__context__
            if isinstance(cause, ssl.SSLError):
                raise cause from exc
            raise


def _configure_apns_client(client: _AioApns, *, use_sandbox: bool) -> None:
    """Wire the timeout-safe protocol and SSL-preserving pool onto real ``aioapns`` clients."""
    pool = getattr(client, "pool", None)
    if not isinstance(pool, APNsKeyConnectionPool):
        return
    hardened: APNsBaseConnectionPool = _HardenedKeyConnectionPool(
        key=pool.key,
        key_id=pool.key_id,
        team_id=pool.team_id,
        topic=pool.apns_topic or "",
        max_connections=pool.max_connections,
        max_connection_attempts=pool.max_connection_attempts,
        use_sandbox=use_sandbox,
        ssl_context=pool.ssl_context,
        proxy_host=pool.proxy_host,
        proxy_port=pool.proxy_port,
    )
    hardened.protocol_class = (
        _TimeoutSafeDevelopmentClientProtocol
        if use_sandbox
        else _TimeoutSafeProductionClientProtocol
    )
    client.pool = hardened


def build_apns_client_pool(config: ApnsConfig) -> ApnsClientPool:
    """Build an ``ApnsClientPool`` from a hydrated ``ApnsConfig``.

    Must be called from within a running asyncio event loop — see the
    note on ``aioapns.APNs`` constructor behavior under Py 3.12+.

    Args:
        config: Hydrated config (PEM string already decoded from
            base64).

    Returns:
        Ready-to-use ``ApnsClientPool``. Sandbox client is built when
        the config's environment is ``sandbox`` or
        ``sandbox_and_production``; production client likewise for
        ``production`` or ``sandbox_and_production``. Both PEM keys
        are shared (only ``use_sandbox`` differs).
    """
    sandbox: _AioApns | None = None
    production: _AioApns | None = None
    env = config.environment
    if env in {"sandbox", "sandbox_and_production"}:
        sandbox = _AioApns(
            key=config.private_key_pem,
            key_id=config.key_id,
            team_id=config.team_id,
            topic=config.topic,
            use_sandbox=True,
        )
        _configure_apns_client(sandbox, use_sandbox=True)
    if env in {"production", "sandbox_and_production"}:
        production = _AioApns(
            key=config.private_key_pem,
            key_id=config.key_id,
            team_id=config.team_id,
            topic=config.topic,
            use_sandbox=False,
        )
        _configure_apns_client(production, use_sandbox=False)
    return ApnsClientPool(sandbox_client=sandbox, production_client=production)
