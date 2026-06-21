"""Monkeypatches for python-kraken-sdk to honor HTTP 429 Retry-After header.

The SDK ignores the ``Retry-After`` header returned by Cloudflare
on HTTP 429 handshake failures, which produces a self-amplifying reconnect
cascade until ``MaxReconnectError``. The patches in this module:

1. Wrap the local ``connect`` reference inside
   ``kraken.spot.websocket.connectors`` so the WebSocket handshake context
   manager observes ``InvalidStatus`` 429 responses (the websockets-16
   exception class) and stashes the parsed ``Retry-After`` keyed by the
   current connector instance.
2. Replace ``ConnectSpotWebsocketBase.__get_reconnect_wait`` with a
   version that consumes the stashed Retry-After before falling back to
   the SDK's exponential backoff.
3. Patch ``ConnectSpotWebsocketBase.__run`` to stamp the connector's
   identity on a ``ContextVar`` for the lifetime of the run coroutine,
   so the connect shim can attribute each 429 to the right connector.
4. Patch ``ConnectSpotWebsocketBase.__init__`` to register the connector
   with its owning publisher (carried through ``_CURRENT_PUBLISHER``) and
   arrange a ``weakref.finalize`` for automatic unregistration when the
   SDK garbage-collects the connector instance.
5. Patch ``ConnectSpotWebsocketBase.__reconnect`` so each reconnect attempt
   notifies the owning publisher's reconnect-storm watchdog before
   delegating to the SDK's original reconnect logic.

Public API:

* :func:`apply_kraken_retry_after_honoring` — idempotently install the
  patches at process startup. Call once after import.
* :func:`apply_kraken_ws_teardown_hardening` — idempotently install the
  hardened reconnect loops (interruptible backoff + child reaping) on the
  Spot AND Futures connector classes. Applied at import time by the Kraken
  venue implementation modules so executor processes are covered too.
* :func:`force_close_ws_client` — last-resort teardown for a WS client
  whose bounded ``close()`` timed out or raised: cancels connector run
  tasks and closes the leaked aiohttp session directly (#143).
* :data:`_CURRENT_PUBLISHER` — publishers set this ``ContextVar`` inside
  their startup coroutine so any ``ConnectSpotWebsocketBase`` instance
  constructed during that span is associated with them.
* :func:`get_registered_publisher` — return the publisher associated with
  a given connector id, or ``None``.
"""

import asyncio
import contextvars
import weakref
from collections.abc import Callable
from typing import Any
from typing import Final

import kraken.futures.websocket as _kraken_futures_ws
import kraken.spot.websocket.connectors as _kraken_connectors
import websockets.asyncio.client as _ws_client
from kraken.exceptions import MaxReconnectError
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.spot.websocket.connectors import ConnectSpotWebsocket
from kraken.spot.websocket.connectors import ConnectSpotWebsocketBase
from kraken.utils.utils import WSState
from loguru import logger
from websockets.exceptions import ConnectionClosed
from websockets.exceptions import InvalidStatus
from websockets.exceptions import ProxyError

from snapper.core.json_types import JsonObject
from snapper.infrastructure.network.egress_context import current_egress_identity
from snapper.infrastructure.network.egress_context import resolve_egress_traffic
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_reservation import EgressReservation
from snapper.utils.logging import is_file_sink_ready

_RETRY_AFTER_MIN_SECONDS: Final[float] = 1.0
"""Floor on Retry-After honoring — protects against zero/negative headers."""

_RETRY_AFTER_MAX_SECONDS: Final[float] = 900.0
"""Hard cap on Retry-After honoring (15 min sanity ceiling)."""

_RECONNECT_WINDOW_S: Final[float] = 60.0
"""Sliding window for the publisher-side reconnect-storm watchdog."""

_RECONNECT_LIMIT: Final[int] = 5
"""Maximum allowed reconnect attempts within ``_RECONNECT_WINDOW_S``."""

_WS_PING_INTERVAL_S: Final[float] = 30.0
"""Default WebSocket ping interval (seconds) injected into every Kraken
handshake when the caller does not specify one. Matches the python-kraken-sdk
default; set explicitly so transport keepalive does not silently depend on a
library default and applies on both the egress (pool) and direct paths."""

_WS_PING_TIMEOUT_S: Final[float] = 10.0
"""WebSocket pong deadline (seconds), enforced over any caller value.

A silently dead TCP connection (a yanked-internet outage with no close frame)
surfaces as a ``ConnectionClosed`` within roughly
``_WS_PING_INTERVAL_S + _WS_PING_TIMEOUT_S`` so the SDK reconnect path and the
publisher liveness watchdog engage in tens of seconds instead of waiting out
the multi-minute app-level message-silence threshold."""

_WS_CLOSE_TIMEOUT_S: Final[float] = 10.0
"""Closing-handshake deadline (seconds), enforced over any caller value, so a
half-open socket cannot stall teardown during a reconnect."""

_CLOSE_CODE_BACKOFF_S: Final[dict[int, float]] = {
    1008: 15.0,
    1011: 10.0,
    1012: 30.0,
    1013: 60.0,
}
"""Custom reconnect backoff (seconds) keyed by WebSocket close code.

Kraken-specific close codes observed in production (2026-05-21 incident):

* ``1008`` — Policy Violation; Kraken's per-user WebSocket rate limit
  fired after the SDK cascaded reconnects too aggressively. 15 s.
* ``1011`` — Internal Server Error from the WS endpoint; back off
  10 s and retry.
* ``1012`` — Service Restart; Kraken WebSocket infrastructure is doing
  a graceful restart. Documented behaviour, occurs periodically. The
  SDK's default exponential is too aggressive here and triggers the
  1008 cascade. 30 s lets Kraken finish restarting cleanly.
* ``1013`` — Try Again Later; Kraken's "trading engine unavailable"
  signal. Longer back-off because it usually indicates an internal
  Kraken outage. 60 s.

Any other code falls back to the SDK's original exponential backoff.
"""

_PATCH_APPLIED: list[bool] = [False]
"""Single-element list flag tracking whether the patch is installed."""

_FUTURES_PATCH_APPLIED: list[bool] = [False]
"""Single-element list flag tracking whether the Futures pool-routing rebind is installed."""

_ALREADY_SUBSCRIBED_PATCH_APPLIED: list[bool] = [False]
"""Single-element list flag tracking whether the Already-subscribed filter is installed."""

_PATCH_LOGGED: list[bool] = [False]
"""Single-element list flag tracking whether the Retry-After patch's
``applied`` INFO confirmation has been emitted post-sink-ready."""

_FUTURES_PATCH_LOGGED: list[bool] = [False]
"""Single-element list flag tracking whether the Futures pool-routing
patch's ``applied`` INFO confirmation has been emitted post-sink-ready."""

_ALREADY_SUBSCRIBED_PATCH_LOGGED: list[bool] = [False]
"""Single-element list flag tracking whether the Already-subscribed
filter patch's ``applied`` INFO confirmation has been emitted
post-sink-ready."""

_RESUBSCRIBE_PACE_PATCH_APPLIED: list[bool] = [False]
"""Single-element list flag tracking whether the reconnect re-subscribe
pacing patch is installed."""

_RESUBSCRIBE_PACE_PATCH_LOGGED: list[bool] = [False]
"""Single-element list flag tracking whether the re-subscribe pacing
patch's ``applied`` INFO confirmation has been emitted post-sink-ready."""

_TEARDOWN_PATCH_APPLIED: list[bool] = [False]
"""Single-element list flag tracking whether the WS teardown hardening
(interruptible reconnect backoff + child reaping) is installed."""

_TEARDOWN_PATCH_LOGGED: list[bool] = [False]
"""Single-element list flag tracking whether the teardown hardening
patch's ``applied`` INFO confirmation has been emitted post-sink-ready."""

_RECONNECT_BACKOFF_POLL_S: Final[float] = 0.5
"""Poll interval (seconds) at which the hardened reconnect backoff rechecks
``keep_alive``. Bounds teardown latency during a pending backoff to this
value instead of the full exponential wait (up to ~3 minutes late in the
SDK's schedule), without changing the SDK's stop signalling (a bare bool
flag with no event to wait on)."""

_RECONNECT_CHILD_REAP_TIMEOUT_S: Final[float] = 5.0
"""Upper bound (seconds) on draining cancelled reconnect child tasks and
force-closed connector run tasks. Cancellation of these tasks normally
completes within one event-loop tick; the bound exists so a pathologically
uncancellable task cannot wedge a teardown path, and overruns are logged."""

_FORCE_CLOSE_CONNECTOR_ATTRS: Final[tuple[str, str, str]] = ("_pub_conn", "_priv_conn", "_conn")
"""Connector slot attributes probed by :func:`force_close_ws_client` —
``_pub_conn``/``_priv_conn`` on ``SpotWSClientBase``, ``_conn`` on
``FuturesWSClient``."""

_FORCE_CLOSE_SESSION_ATTRS: Final[tuple[str, str]] = (
    "_SpotAsyncClient__session",
    "_FuturesAsyncClient__session",
)
"""Name-mangled aiohttp session attributes probed by
:func:`force_close_ws_client` — ``SpotAsyncClient`` and
``FuturesAsyncClient`` each create one ``aiohttp.ClientSession`` in their
constructors and only close it in ``close()``."""

_RESUBSCRIBE_PACE_S: Final[float] = 0.2
"""Seconds slept between consecutive per-subscription re-subscribes while
the kraken-sdk recovers its subscription cache after a reconnect.

The SDK's stock ``ConnectSpotWebsocket._recover_subscriptions`` replays the
entire locally-tracked per-symbol cache in a tight no-delay loop. On a large
universe (~340 Kraken Equities ticker+trade subscriptions) that single burst
exceeds Kraken's per-connection subscribe message-rate limit ("Exceeded msg
rate"); the rejected re-subscribes leave channels un-ACKed and the stream
goes dark (observed 2026-06-05: a load-induced reconnect darkened the
Equities ticker ~17 min). 0.2 s (5 msg/s) keeps recovery under the limit with
margin — the Spot path already paces its 100-symbol chunk messages 0.1 s
apart — while still recovering ~340 subscriptions in well under two minutes.
The app-level health-loop re-subscribes are separately paced by
``retry_subscribe_spacing_s``; this constant governs only the SDK's own
reconnect replay path, which those app-level guards do not cover."""

_ALREADY_SUBSCRIBED_ERROR: Final[str] = "Already subscribed"
"""Kraken Spot WS server response 'error' value emitted when subscribing
to a feed already present in the SDK's local subscription cache.

Benign: fires on the race between our publisher health-loop's replay
path and the SDK's existing in-memory subscription set. Real subscribe
failures (e.g. ``Invalid arguments``, unknown symbol) use a different
``error`` string and must remain visible at WARNING level."""

_PENDING_RETRY_AFTER_S: dict[int, float] = {}
"""Maps ``id(connector_self)`` to its pending Retry-After deadline in seconds."""

_LAST_CLOSE_CODE: dict[int, int] = {}
"""Maps ``id(connector_self)`` to the close code from the last received Close frame.

Populated by :func:`_patched_run` when ``websockets.exceptions.ConnectionClosed``
bubbles out of the SDK's run loop. Consumed and popped by
:func:`_patched_get_reconnect_wait` so each disconnect informs exactly one
reconnect backoff decision. ``ConnectionClosed.rcvd.code`` carries the
server-sent close code (Kraken's 1012 service-restart, 1008 rate limit,
1013 trading-engine-unavailable, etc.).
"""

_CONNECTOR_PUBLISHERS: dict[int, Any] = {}
"""Maps ``id(ConnectSpotWebsocketBase)`` to the owning publisher instance.

Populated by :func:`_patched_init` from the ``_CURRENT_PUBLISHER`` ContextVar
and pruned by ``weakref.finalize`` when the SDK garbage-collects the
connector instance.
"""

_CURRENT_CONNECTOR_ID: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "_kraken_current_connector_id", default=None
)
"""ContextVar carrying ``id(self)`` from patched ``__run`` into the connect shim."""

_CURRENT_PUBLISHER: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "_kraken_current_publisher", default=None
)
"""ContextVar carrying the owning publisher instance into patched ``__init__``."""

_ORIGINAL_GET_RECONNECT_WAIT: Any = getattr(
    ConnectSpotWebsocketBase, "_ConnectSpotWebsocketBase__get_reconnect_wait"
)
_ORIGINAL_RUN: Any = getattr(ConnectSpotWebsocketBase, "_ConnectSpotWebsocketBase__run")
_ORIGINAL_INIT: Any = ConnectSpotWebsocketBase.__init__


def _parse_retry_after(headers: Any) -> float | None:
    """Extract Retry-After from a websockets-16 InvalidStatus response headers object.

    Args:
        headers: A ``websockets.http11.Headers`` instance or any
            mapping-like object exposing ``get(key)``. ``None`` is
            tolerated and yields ``None``.

    Returns:
        The parsed Retry-After value in seconds, clamped to
        ``[_RETRY_AFTER_MIN_SECONDS, _RETRY_AFTER_MAX_SECONDS]``, or
        ``None`` if the header is missing or unparseable.
    """
    if headers is None:
        return None
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except TypeError, ValueError:
        return None
    if seconds <= 0:
        return None
    return min(max(seconds, _RETRY_AFTER_MIN_SECONDS), _RETRY_AFTER_MAX_SECONDS)


def get_registered_publisher(connector_id: int) -> Any:
    """Return the publisher registered against ``connector_id`` or ``None``.

    Args:
        connector_id: ``id()`` of a ``ConnectSpotWebsocketBase`` instance,
            matching the key written by :func:`_patched_init`.

    Returns:
        The publisher object owning the connector, or ``None`` when no
        publisher is registered for that id (e.g. the connector was
        constructed outside any publisher's startup ContextVar span).
    """
    return _CONNECTOR_PUBLISHERS.get(connector_id)


def _unregister_connector(connector_id: int) -> None:
    """Drop the registry entries for a connector. Wired via ``weakref.finalize``.

    Clears every dict keyed by the connector id so a long-running
    publisher cannot accumulate stale entries across reconnect-and-rebuild
    cycles.
    """
    _CONNECTOR_PUBLISHERS.pop(connector_id, None)
    _PENDING_RETRY_AFTER_S.pop(connector_id, None)
    _LAST_CLOSE_CODE.pop(connector_id, None)


_CLOSE_1015_QUARANTINE_S: Final[float] = 60.0
"""Default quarantine duration for a Cloudflare 1015 close-frame, in seconds.

Picked as a conservative midpoint of the 60-600 s Retry-After values
observed during the 2026-05-21 incident. Operator-visible quarantine
deadline is recorded on ``RouteState.last_close_1015_at``.
"""

_WS_CONNECT_ERROR_QUARANTINE_S: Final[float] = 30.0
"""Quarantine duration (seconds) for a WS connect-level failure on a proxy route.

Applied when opening the handshake through a SOCKS route raises a TCP/SOCKS
connect error — timeout, connection refused, or a silent blackhole (the egress
tunnel went dark mid-flight). Without quarantining, the SDK's reconnect loop
re-reserves and re-dials the same dead route every attempt and never fails over
to another tunnel or to the direct fallback. Thirty seconds is shorter than the
1015 quarantine because a connect timeout is often a transient tunnel hiccup
worth retrying sooner, while still long enough to let the pool route around a
genuinely down tunnel across several reconnect attempts. Direct routes are never
quarantined here — a connect error with no proxy means the exchange or the local
uplink is down, not the route, and the pool would only fall back to that same
direct route anyway."""

_PRIVATE_DIRECT_CONNECT_ERROR_QUARANTINE_S: Final[float] = 15.0
"""Short quarantine for private direct WS connect errors.

Executor-owned private traffic normally reserves direct egress first. If the
direct handshake itself fails, briefly quarantining direct lets the next
reconnect attempt reserve the configured private fallback route. Public direct
traffic keeps the previous behaviour and is never quarantined for connect
errors.
"""


class _ConnectShim:
    """Async context manager wrapping the SDK's ``connect(...)`` call.

    Reservation lifecycle for a single handshake:

    1. ``__init__`` (sync): reserve a route from the pool when
       enabled, merge the proxy kwarg, enforce transport keepalive
       (``ping_timeout``/``close_timeout``, default ``ping_interval``),
       then build the underlying ``original_connect(...)`` context
       manager. Synchronous failure of ``original_connect`` releases
       the reservation.
    2. ``__aenter__``: opens the handshake. On HTTP 429,
       quarantine the active route (pool path) or stash the
       Retry-After (legacy path), release, re-raise. On a TCP/SOCKS
       connect-level failure (timeout, refused, or blackhole) through a
       proxy route, quarantine the route with ``reason="ws-connect-error"``
       so the next reconnect fails over instead of re-dialing the dead
       tunnel; release, re-raise.
    3. Caller's ``async with`` body runs.
    4. ``__aexit__``: if the exit carries
       ``ConnectionClosed(rcvd.code=1015)`` (Cloudflare
       close-after-handshake), quarantine the route with
       ``reason="close-1015"``. Other close codes do NOT
       quarantine — those are handled by the per-close-code
       backoff path (1008/1011/1012/1013 → tuned backoff, not
       egress-route fault). Then release the reservation in ``finally``.
    """

    def __init__(self, original_connect: Any, *args: Any, **kwargs: Any) -> None:
        self._reservation: EgressReservation | None = None
        self._traffic_class = "public"
        pool = get_egress_pool()
        if pool is not None and pool.size() > 0:
            exchange_name, traffic_class = resolve_egress_traffic()
            self._traffic_class = traffic_class
            self._reservation = pool.reserve(
                exchange=exchange_name,
                purpose="websocket",
                traffic_class=traffic_class,
            )
            kwargs = {**kwargs, **self._reservation.websocket_kwargs()}
        kwargs = {
            "ping_interval": _WS_PING_INTERVAL_S,
            **kwargs,
            "ping_timeout": _WS_PING_TIMEOUT_S,
            "close_timeout": _WS_CLOSE_TIMEOUT_S,
        }
        try:
            self._cm = original_connect(*args, **kwargs)
        except BaseException:
            if self._reservation is not None:
                self._reservation.release()
                self._reservation = None
            raise

    async def __aenter__(self) -> Any:
        try:
            return await self._cm.__aenter__()
        except InvalidStatus as exc:
            self._handle_handshake_429(exc)
            if self._reservation is not None:
                self._reservation.release()
                self._reservation = None
            raise
        except (OSError, ProxyError) as exc:
            self._maybe_quarantine_on_connect_error(exc)
            if self._reservation is not None:
                self._reservation.release()
                self._reservation = None
            raise
        except BaseException:
            if self._reservation is not None:
                self._reservation.release()
                self._reservation = None
            raise

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
        try:
            return await self._cm.__aexit__(exc_type, exc, tb)
        finally:
            self._maybe_quarantine_on_close(exc)
            if self._reservation is not None:
                self._reservation.release()
                self._reservation = None

    def _handle_handshake_429(self, exc: InvalidStatus) -> None:
        """Record Retry-After on 429 — route-scoped or legacy global."""
        response = getattr(exc, "response", None)
        if response is None or getattr(response, "status_code", None) != 429:
            return
        retry_after = _parse_retry_after(getattr(response, "headers", None))
        if retry_after is None:
            return
        if self._reservation is not None:
            logger.warning(
                "kraken WS handshake 429 on route '{}'; Retry-After={}s (pool-scoped quarantine)",
                self._reservation.route_id,
                retry_after,
            )
            self._reservation.quarantine(retry_after, reason="http-429")
            return
        connector_id = _CURRENT_CONNECTOR_ID.get()
        if connector_id is None:
            return
        logger.warning(
            "kraken WS handshake 429; Retry-After={}s (connector={})",
            retry_after,
            connector_id,
        )
        _PENDING_RETRY_AFTER_S[connector_id] = retry_after

    def _maybe_quarantine_on_connect_error(self, exc: BaseException) -> None:
        """Quarantine the active proxy route on a WS connect-level failure.

        Fires from ``__aenter__`` when opening the handshake raises a
        TCP/SOCKS connect error (timeout, refused, or blackhole). Quarantining
        the route makes the SDK's next reconnect re-reserve a different route
        (or the direct fallback) instead of re-dialing the same dead tunnel.
        Public direct routes (``proxy_url is None``) are left untouched — a
        connect error with no proxy points at the exchange or the local uplink,
        and the public pool's only fallback would be that same direct route.
        Private direct routes quarantine briefly so the next reconnect can use
        the configured private fallback route.

        Args:
            exc: The connect-level exception raised by ``__aenter__``.
        """
        if self._reservation is None:
            return
        if self._reservation.proxy_url is None:
            if self._traffic_class != "private":
                return
            logger.warning(
                "kraken WS private direct connect error on route '{}' ({}: {}); "
                "quarantining for {}s",
                self._reservation.route_id,
                type(exc).__name__,
                exc,
                _PRIVATE_DIRECT_CONNECT_ERROR_QUARANTINE_S,
            )
            self._reservation.quarantine(
                _PRIVATE_DIRECT_CONNECT_ERROR_QUARANTINE_S,
                reason="ws-connect-error",
            )
            return
        logger.warning(
            "kraken WS connect error on route '{}' ({}: {}); quarantining for {}s",
            self._reservation.route_id,
            type(exc).__name__,
            exc,
            _WS_CONNECT_ERROR_QUARANTINE_S,
        )
        self._reservation.quarantine(_WS_CONNECT_ERROR_QUARANTINE_S, reason="ws-connect-error")

    def _maybe_quarantine_on_close(self, exc: BaseException | None) -> None:
        """Quarantine the active route on Cloudflare close-frame 1015."""
        if self._reservation is None:
            return
        if not isinstance(exc, ConnectionClosed):
            return
        rcvd = getattr(exc, "rcvd", None)
        code = getattr(rcvd, "code", None)
        if code != 1015:
            return
        logger.warning(
            "kraken WS close 1015 on route '{}'; quarantining for {}s",
            self._reservation.route_id,
            _CLOSE_1015_QUARANTINE_S,
        )
        self._reservation.quarantine(_CLOSE_1015_QUARANTINE_S, reason="close-1015")


def _wrap_connect_factory(original_connect: Any) -> Any:
    """Return a connect-shaped factory that consults the egress pool and 429s."""

    def _connect_shim(*args: Any, **kwargs: Any) -> _ConnectShim:
        return _ConnectShim(original_connect, *args, **kwargs)

    return _connect_shim


async def _patched_run(self: ConnectSpotWebsocketBase, event: asyncio.Event) -> None:
    """Stamp ``_CURRENT_CONNECTOR_ID`` and capture WebSocket close codes.

    The connect shim reads the ContextVar inside its ``__aenter__`` to
    attribute the 429 response to the right connector. Resetting the
    token in the ``finally`` clause guarantees the ContextVar does not
    leak across coroutine restarts.

    Beyond the ContextVar work this also intercepts
    ``websockets.exceptions.ConnectionClosed`` (including subclasses
    ``ConnectionClosedError`` and ``ConnectionClosedOK``) and stashes
    the server-sent close code in ``_LAST_CLOSE_CODE`` keyed by
    ``id(self)``. The patched ``__get_reconnect_wait`` reads the stash
    to pick a Kraken-aware backoff: code 1012 (service restart) →
    30 s, 1008 (rate limit) → 15 s, 1013 (trading engine unavailable)
    → 60 s. The exception is re-raised so the SDK's reconnect path
    continues to fire.
    """
    token = _CURRENT_CONNECTOR_ID.set(id(self))
    try:
        await _ORIGINAL_RUN(self, event)
    except ConnectionClosed as exc:
        rcvd = getattr(exc, "rcvd", None)
        code = getattr(rcvd, "code", None)
        if isinstance(code, int):
            _LAST_CLOSE_CODE[id(self)] = code
            logger.info(
                "kraken WS closed by server: code={} reason={!r} (connector={})",
                code,
                getattr(rcvd, "reason", ""),
                id(self),
            )
        raise
    finally:
        _CURRENT_CONNECTOR_ID.reset(token)


def _patched_get_reconnect_wait(self: ConnectSpotWebsocketBase, attempts: int) -> float:
    """Pick reconnect backoff with Kraken + egress-pool awareness.

    Precedence when the pool is disabled (``get_egress_pool() is None``):

    1. **Cloudflare 429 Retry-After** stash from the handshake shim —
       honor the server-sent deadline exactly.
    2. **Kraken WebSocket close code** stash from the last
       ``ConnectionClosed`` exception. Codes 1008/1011/1012/1013 get
       Kraken-tuned constant backoffs from ``_CLOSE_CODE_BACKOFF_S``;
       any other code falls through to the SDK exponential.
    3. **SDK exponential** — original ``random() * min(180, 2**n - 1)
       + 1`` formula, preserved as last-resort behaviour.

    Precedence when the pool is enabled:

    1. **Close-code stash** (close-1015 already quarantines the route
       via the shim; other Kraken close codes still need their tuned
       backoff so the SDK sleeps the right amount before the next
       handshake).
    2. **Pool-aware wait**:
       * If at least one route is non-quarantined → return
         ``_RETRY_AFTER_MIN_SECONDS`` (1 s). The shim's next reserve
         call picks the healthy route, restoring tick flow.
       * If all routes are quarantined → return
         ``pool.earliest_release_in_seconds()`` so the SDK sleeps
         exactly until the first route releases instead of churning.
       * If somehow neither (empty pool) → fall through to SDK
         exponential.
    3. **SDK exponential** — same last-resort.

    When pool is enabled, the legacy ``_PENDING_RETRY_AFTER_S`` stash
    is intentionally NOT consulted — the shim's ``_handle_handshake_429``
    routes 429 captures to ``reservation.quarantine`` instead, so the
    pool's quarantine state is authoritative. This is the core
    pool-routing guarantee: a 429 on direct with a healthy
    SOCKS5 alternate must recover in ~1 s, not the full Retry-After.

    Both stashes are popped on consumption so the next reconnect
    makes a fresh decision.

    Args:
        self: The ``ConnectSpotWebsocketBase`` instance.
        attempts: The current reconnect attempt counter from the SDK.

    Returns:
        The number of seconds to sleep before the next reconnect.
    """
    connector_id = id(self)
    pool = get_egress_pool()
    if pool is None and connector_id in _PENDING_RETRY_AFTER_S:
        retry_after = _PENDING_RETRY_AFTER_S.pop(connector_id)
        logger.info(
            "kraken WS honoring Retry-After: sleeping {}s (connector={})",
            retry_after,
            connector_id,
        )
        return retry_after
    last_code = _LAST_CLOSE_CODE.pop(connector_id, None)
    if last_code is not None and last_code in _CLOSE_CODE_BACKOFF_S:
        wait = _CLOSE_CODE_BACKOFF_S[last_code]
        logger.info(
            "kraken WS close code {} -> backoff {}s (connector={})",
            last_code,
            wait,
            connector_id,
        )
        return wait
    if pool is not None and pool.size() > 0:
        identity = current_egress_identity()
        exchange_name: str
        traffic_class: str
        if identity is not None:
            exchange_name = identity.exchange
            traffic_class = identity.traffic_class
        else:
            publisher = _CONNECTOR_PUBLISHERS.get(connector_id)
            if publisher is not None:
                exchange_name = publisher._get_exchange_name()
                traffic_class = "public"
            else:
                exchange_name, traffic_class = resolve_egress_traffic()
        if pool.has_available(exchange=exchange_name, traffic_class=traffic_class):
            logger.info(
                "kraken WS pool-aware reconnect: healthy route available (connector={})",
                connector_id,
            )
            return _RETRY_AFTER_MIN_SECONDS
        earliest = (
            pool.earliest_release_in_seconds(
                exchange=exchange_name,
                traffic_class=traffic_class,
            )
            or 0.0
        )
        logger.warning(
            "kraken WS pool-aware reconnect: all routes quarantined; "
            "sleeping {}s until earliest release (connector={})",
            earliest,
            connector_id,
        )
        return max(earliest, _RETRY_AFTER_MIN_SECONDS)
    fallback: float = _ORIGINAL_GET_RECONNECT_WAIT(self, attempts)
    return fallback


def _patched_init(self: ConnectSpotWebsocketBase, *args: Any, **kwargs: Any) -> None:
    """Register the connector with the publisher carried in ``_CURRENT_PUBLISHER``.

    The publisher's start coroutine sets ``_CURRENT_PUBLISHER`` to itself
    so any ``ConnectSpotWebsocketBase`` created inside that span gets
    mapped back to its owning publisher. ``weakref.finalize`` prunes the
    registry entry when the SDK garbage-collects the connector.
    """
    _ORIGINAL_INIT(self, *args, **kwargs)
    publisher = _CURRENT_PUBLISHER.get()
    if publisher is not None:
        connector_id = id(self)
        _CONNECTOR_PUBLISHERS[connector_id] = publisher
        weakref.finalize(self, _unregister_connector, connector_id)


async def _interruptible_backoff(connector: Any, wait_s: float) -> None:
    """Sleep up to ``wait_s`` seconds, waking early when the connector stops.

    The SDK's stock reconnect loop sleeps its full exponential backoff in a
    single ``asyncio.sleep`` call, so a teardown that flips ``keep_alive``
    mid-sleep (``stop()``/``close()``) blocks until the backoff expires — up
    to ~3 minutes late in the schedule, which is what pushed the venue
    layer's bounded ``close()`` past its deadline and leaked the aiohttp
    session (#143). Polling ``keep_alive`` every
    ``_RECONNECT_BACKOFF_POLL_S`` bounds that teardown latency without
    changing the SDK's stop signalling (a bare bool flag, no event to wait
    on).

    Args:
        connector: The SDK connector exposing the ``keep_alive`` bool.
        wait_s: Total backoff duration requested by the reconnect schedule.
    """
    remaining = wait_s
    while remaining > 0 and connector.keep_alive:
        step = min(_RECONNECT_BACKOFF_POLL_S, remaining)
        await asyncio.sleep(step)
        remaining -= step


async def _reap_reconnect_children(tasks: list[asyncio.Task[None]]) -> None:
    """Cancel and drain reconnect child tasks, consuming their exceptions.

    Runs in the ``finally`` of :func:`_drive_reconnect_children` so children
    can never outlive their ``__reconnect`` invocation. The SDK's stock loop
    orphans them in two ways: a ``keep_alive`` flip between child creation
    and the ``while keep_alive`` recheck skips the wait entirely, and
    ``asyncio.wait`` never cancels its awaitables when the parent task itself
    is cancelled — the exact failure that forced the first #143 fix attempt
    (cancel ``__run_forever`` from teardown) to be reverted. Exceptions are
    retrieved from every finished child so the event loop never logs
    ``Task exception was never retrieved``.

    Args:
        tasks: The reconnect invocation's child tasks.
    """
    for task in tasks:
        if not task.done():
            task.cancel()
    done, pending = await asyncio.wait(tasks, timeout=_RECONNECT_CHILD_REAP_TIMEOUT_S)
    for task in done:
        if not task.cancelled():
            task.exception()
    for task in pending:
        logger.warning(
            "kraken WS reconnect child {!r} survived cancellation for {}s",
            task,
            _RECONNECT_CHILD_REAP_TIMEOUT_S,
        )


async def _drive_reconnect_children(
    connector: Any,
    tasks: list[asyncio.Task[None]],
    on_child_exception: Callable[[asyncio.Task[None]], None],
) -> None:
    """Run the SDK reconnect wait semantics with guaranteed child reaping.

    Waits on the children with ``FIRST_EXCEPTION`` semantics — the wait
    returns either when a child has raised or when every child is done
    (``asyncio.wait`` does not wake for cancelled-only completions) — then
    routes each finished child's exception through the venue-specific
    handler. The stock SDK wrapped this wait in a ``while keep_alive`` loop,
    but under ``FIRST_EXCEPTION`` semantics the wait can only return with an
    exception present or with no pending children left, so every stock
    iteration beyond the first was either unreachable or a hot spin on an
    already-done task set; a single bounded pass is the faithful shape. The
    ``finally`` reap guarantees no child survives this invocation regardless
    of how it exits (``keep_alive`` flip, child exception, clean completion,
    or cancellation of the parent run task).

    Args:
        connector: The SDK connector exposing the ``keep_alive`` bool.
        tasks: The child tasks spawned for this reconnect attempt.
        on_child_exception: Venue-specific handler invoked once per finished
            child that holds an exception (state transition + SDK-parity log).
    """
    try:
        if not connector.keep_alive:
            return
        finished, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        for task in finished:
            if task.cancelled():
                continue
            if task.exception() is not None:
                on_child_exception(task)
    finally:
        await _reap_reconnect_children(tasks)


async def _patched_reconnect(self: ConnectSpotWebsocketBase) -> None:
    """Hardened replacement for the Spot connector's ``__reconnect``.

    Notifies the registered publisher's reconnect-storm watchdog first. The
    hook ``_on_sdk_reconnect_attempt`` is optional — only
    ``KrakenMarketDataPublisher`` (Spot) implements it; other Kraken
    publishers register via ``_CURRENT_PUBLISHER`` for egress-pool exchange
    tagging only, and executor processes have no registered publisher at all.
    Calling unconditionally would raise ``AttributeError`` and break every
    reconnect cycle for those owners.

    Then mirrors the SDK's reconnect semantics (python-kraken-sdk 3.2.x —
    version-coupled reimplementation, same precedent as
    :func:`_patched_recover_subscriptions`) with the #143 teardown hardening:

    * the backoff sleep polls ``keep_alive`` via
      :func:`_interruptible_backoff` so ``stop()``/``close()`` is not blocked
      for the full exponential backoff;
    * a ``keep_alive`` flip during the backoff returns before any child task
      is spawned (the stock loop spawned and orphaned them);
    * children are always reaped via :func:`_drive_reconnect_children`,
      making cancellation of the connector's run task safe — the stock loop
      leaked ``_recover_subscriptions`` (stuck on ``event.wait()`` forever)
      and an unobserved ``__run`` exception when cancelled.

    SDK-parity log lines are emitted through the SDK module's own logger so
    operational log greps keep working across the patch.

    Args:
        self: The Spot connector instance being reconnected.

    Raises:
        MaxReconnectError: When the reconnect budget is exhausted, matching
            stock behaviour (message included).
    """
    publisher = _CONNECTOR_PUBLISHERS.get(id(self))
    if publisher is not None:
        hook = getattr(publisher, "_on_sdk_reconnect_attempt", None)
        if hook is not None:
            hook()
    self.state = WSState.RECONNECTING
    _kraken_connectors.LOG.info("Websocket start connect/reconnect")
    reconnect_num = getattr(self, "_ConnectSpotWebsocketBase__reconnect_num") + 1
    setattr(self, "_ConnectSpotWebsocketBase__reconnect_num", reconnect_num)
    if reconnect_num >= self.MAX_RECONNECT_NUM:
        raise MaxReconnectError(
            "The Kraken Spot websocket client encountered to many reconnects!",
        )
    wait_s = getattr(self, "_ConnectSpotWebsocketBase__get_reconnect_wait")(reconnect_num)
    await _interruptible_backoff(self, wait_s)
    if not self.keep_alive:
        self.state = WSState.CLOSED
        return
    event: asyncio.Event = asyncio.Event()
    run = getattr(self, "_ConnectSpotWebsocketBase__run")
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(self._recover_subscriptions(event)),
        asyncio.create_task(run(event)),
    ]

    def _on_child_exception(task: asyncio.Task[None]) -> None:
        self.state = WSState.ERRORHANDLING
        _kraken_connectors.LOG.warning(
            "%s got an exception %s\nThe connection will be recovered in the background.",
            task,
            task.exception(),
        )

    await _drive_reconnect_children(self, tasks, _on_child_exception)
    self.state = WSState.CLOSED
    _kraken_connectors.LOG.info("Connection closed!")


async def _patched_futures_reconnect(self: ConnectFuturesWebsocket) -> None:
    """Hardened replacement for the Futures connector's ``__reconnect``.

    Mirrors the SDK's reconnect semantics (python-kraken-sdk 3.2.x —
    version-coupled reimplementation) with the same #143 teardown hardening
    as :func:`_patched_reconnect`: interruptible backoff, no child spawn
    after a ``keep_alive`` flip, and guaranteed child reaping so cancelling
    the connector's run task cannot orphan ``__recover_subscription_req_msg``
    or leave an unobserved ``__run`` exception. The stock behaviour of
    clearing the challenge-ready flag on a child exception is preserved so a
    recovered private connection re-authenticates.

    Args:
        self: The Futures connector instance being reconnected.

    Raises:
        MaxReconnectError: When the reconnect budget is exhausted, matching
            stock behaviour (bare raise).
    """
    self.state = WSState.RECONNECTING
    _kraken_futures_ws.LOG.info("Websocket start connect/reconnect")
    reconnect_num = getattr(self, "_ConnectFuturesWebsocket__reconnect_num") + 1
    setattr(self, "_ConnectFuturesWebsocket__reconnect_num", reconnect_num)
    if reconnect_num >= self.MAX_RECONNECT_NUM:
        raise MaxReconnectError
    wait_s = getattr(self, "_ConnectFuturesWebsocket__get_reconnect_wait")(reconnect_num)
    await _interruptible_backoff(self, wait_s)
    if not self.keep_alive:
        self.state = WSState.CLOSED
        return
    event: asyncio.Event = asyncio.Event()
    recover = getattr(self, "_ConnectFuturesWebsocket__recover_subscription_req_msg")
    run = getattr(self, "_ConnectFuturesWebsocket__run")
    tasks: list[asyncio.Task[None]] = [
        asyncio.create_task(recover(event)),
        asyncio.create_task(run(event)),
    ]

    def _on_child_exception(task: asyncio.Task[None]) -> None:
        self.state = WSState.ERRORHANDLING
        setattr(self, "_ConnectFuturesWebsocket__challenge_ready", False)
        _kraken_futures_ws.LOG.warning(
            "%s got an exception %s\nThe connection will be recovered in the background.",
            task,
            task.exception(),
        )

    await _drive_reconnect_children(self, tasks, _on_child_exception)
    self.state = WSState.CLOSED
    _kraken_futures_ws.LOG.info("Connection closed!")


async def force_close_ws_client(client: Any) -> None:
    """Last-resort teardown for a kraken-sdk WS client after a failed ``close()``.

    The SDK's ``close()`` awaits each connector's ``__run_forever`` task and
    only closes the underlying aiohttp ``ClientSession`` afterwards
    (``super().close()``), so when the venue layer's ``asyncio.timeout``
    bound fires mid-await the session leaks one ``ClientSession`` per
    rebuild cycle and the run task keeps living in the background (#143).
    Venue teardown paths call this helper from their ``TimeoutError`` /
    ``Exception`` handlers to finish the job explicitly:

    1. flip ``keep_alive`` and cancel each connector's run task — safe
       because :func:`apply_kraken_ws_teardown_hardening` (re-applied here
       defensively) guarantees reconnect children are reaped on cancellation;
    2. drain the cancelled run tasks, consuming their exceptions so the
       event loop does not log ``Task exception was never retrieved`` (when
       the caller itself is cancelled mid-drain this consumption is best
       effort — the session close below still runs, which is the part that
       matters);
    3. close the aiohttp session directly through the SDK's name-mangled
       session attribute.

    Never raises — teardown paths must stay non-explosive, so each step is
    individually guarded and failures are logged at WARNING. Cancellation of
    the calling task still propagates, but the session close runs in a
    ``finally`` so even a caller cancelled mid-drain cannot reintroduce the
    leak this helper exists to stop.

    Args:
        client: A ``SpotWSClient`` or ``FuturesWSClient`` instance whose
            bounded ``close()`` timed out or raised.
    """
    apply_kraken_ws_teardown_hardening()
    run_tasks = _cancel_connector_run_tasks(client)
    try:
        await _drain_cancelled_run_tasks(run_tasks)
    finally:
        await _close_leaked_sessions(client)


def _cancel_connector_run_tasks(client: Any) -> list[asyncio.Task[None]]:
    """Flip ``keep_alive`` and cancel the run task on every connector slot.

    Probes the spot (``_pub_conn``/``_priv_conn``) and futures (``_conn``)
    connector attributes; absent slots are skipped. Per-connector failures
    are logged and swallowed so one broken slot cannot block teardown of the
    others.

    Args:
        client: The SDK WS client being force-closed.

    Returns:
        The cancelled, not-yet-done run tasks to drain.
    """
    run_tasks: list[asyncio.Task[None]] = []
    for connector_attr in _FORCE_CLOSE_CONNECTOR_ATTRS:
        connector = getattr(client, connector_attr, None)
        if connector is None:
            continue
        try:
            connector.keep_alive = False
            task = getattr(connector, "task", None)
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
                run_tasks.append(task)
        except Exception as exc:
            logger.warning(
                "kraken WS force-close: connector {} teardown failed: {!r}",
                connector_attr,
                exc,
            )
    return run_tasks


async def _drain_cancelled_run_tasks(run_tasks: list[asyncio.Task[None]]) -> None:
    """Await cancelled run tasks, consuming exceptions; log overruns.

    Args:
        run_tasks: Tasks cancelled by :func:`_cancel_connector_run_tasks`.
    """
    if not run_tasks:
        return
    done, pending = await asyncio.wait(run_tasks, timeout=_RECONNECT_CHILD_REAP_TIMEOUT_S)
    for task in done:
        if not task.cancelled():
            task.exception()
    for task in pending:
        logger.warning(
            "kraken WS force-close: run task {!r} survived cancellation for {}s",
            task,
            _RECONNECT_CHILD_REAP_TIMEOUT_S,
        )


def _consume_task_result(task: asyncio.Task[None]) -> None:
    """Consume a detached task's outcome so the loop never reports it unretrieved.

    Wired as a done callback on a detached session close that outlived a
    cancelled caller — the close keeps running in the background and its
    failure would otherwise surface as ``Task exception was never
    retrieved`` at garbage collection.

    Args:
        task: The detached background task to drain.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.warning(
            "kraken WS force-close: background session close failed: {!r}",
            exc,
        )


async def _close_leaked_sessions(client: Any) -> None:
    """Close the SDK client's aiohttp session directly if still open.

    Probes both name-mangled session attributes (spot and futures client
    hierarchies); already-closed or absent sessions are skipped. The close
    runs as an owned task awaited through ``asyncio.wait`` so a caller
    cancelled mid-close cannot abort it half-way and reintroduce the leak:
    ``asyncio.wait`` never cancels its awaitables (the very property that
    bites the stock SDK reconnect, used deliberately here) and — unlike
    ``asyncio.shield`` on Python 3.14 — installs no loop-level
    ``_log_on_exception`` callback, so a background failure after caller
    cancellation is drained solely by :func:`_consume_task_result` instead
    of also surfacing as ``exception in shielded future``. A close failure
    or an internally cancelled close is logged and swallowed — teardown must
    stay non-explosive.

    Args:
        client: The SDK WS client being force-closed.
    """
    for session_attr in _FORCE_CLOSE_SESSION_ATTRS:
        session = getattr(client, session_attr, None)
        if session is None or getattr(session, "closed", True):
            continue
        close_task: asyncio.Task[None] = asyncio.ensure_future(session.close())
        try:
            await asyncio.wait([close_task])
        except asyncio.CancelledError:
            close_task.add_done_callback(_consume_task_result)
            raise
        if close_task.cancelled():
            logger.warning(
                "kraken WS force-close: aiohttp session close was cancelled internally",
            )
        elif close_task.exception() is not None:
            logger.warning(
                "kraken WS force-close: aiohttp session close failed: {!r}",
                close_task.exception(),
            )
        else:
            logger.info("kraken WS force-close: leaked aiohttp session closed")


def apply_kraken_ws_teardown_hardening() -> None:
    """Install the hardened reconnect loops on both connector classes (idempotent).

    Rebinds Spot's ``__reconnect`` to :func:`_patched_reconnect` (the same
    object :func:`apply_kraken_retry_after_honoring` installs — publisher
    processes apply both, executor processes reach this one through the
    Kraken venue implementation modules, which call it at import time) and
    Futures' ``__reconnect`` to :func:`_patched_futures_reconnect`. Every
    process that can tear down a kraken-sdk WS client therefore gets
    interruptible backoffs and reconnect-child reaping, which is the
    precondition that makes :func:`force_close_ws_client` safe to cancel run
    tasks (#143 — the first fix attempt was reverted exactly because parent
    cancellation orphaned the reconnect children).
    """
    if _TEARDOWN_PATCH_APPLIED[0]:
        return
    setattr(
        ConnectSpotWebsocketBase,
        "_ConnectSpotWebsocketBase__reconnect",
        _patched_reconnect,
    )
    setattr(
        ConnectFuturesWebsocket,
        "_ConnectFuturesWebsocket__reconnect",
        _patched_futures_reconnect,
    )
    _TEARDOWN_PATCH_APPLIED[0] = True
    log_kraken_sdk_patches_status()


def apply_kraken_retry_after_honoring() -> None:
    """Apply the Retry-After patch (idempotent — safe to call multiple times).

    Five rebinds are performed on the ``ConnectSpotWebsocketBase`` class:

    * ``__get_reconnect_wait`` — consults ``_PENDING_RETRY_AFTER_S`` first.
    * ``__run`` — stamps the ``_CURRENT_CONNECTOR_ID`` ContextVar.
    * ``__init__`` — registers the connector with its owning publisher.
    * ``__reconnect`` — notifies the owning publisher's watchdog and applies
      the #143 teardown hardening (interruptible backoff + child reaping;
      same function :func:`apply_kraken_ws_teardown_hardening` installs).
    * The bare ``connect`` name imported into the SDK's connectors module
      is rebound to a ``_ConnectShim`` factory so the handshake observes
      ``InvalidStatus`` 429 responses.

    The rebind targets are intentionally the SDK's local references, not
    ``websockets.connect`` — the SDK imports ``connect`` directly from
    ``websockets.asyncio.client`` so rebinding the top-level alias would
    have no effect on the SDK's handshake path.
    """
    if _PATCH_APPLIED[0]:
        return
    setattr(
        ConnectSpotWebsocketBase,
        "_ConnectSpotWebsocketBase__get_reconnect_wait",
        _patched_get_reconnect_wait,
    )
    setattr(ConnectSpotWebsocketBase, "_ConnectSpotWebsocketBase__run", _patched_run)
    setattr(
        ConnectSpotWebsocketBase,
        "_ConnectSpotWebsocketBase__reconnect",
        _patched_reconnect,
    )
    setattr(ConnectSpotWebsocketBase, "__init__", _patched_init)
    setattr(_kraken_connectors, "connect", _wrap_connect_factory(_ws_client.connect))
    _PATCH_APPLIED[0] = True
    log_kraken_sdk_patches_status()


def apply_kraken_futures_pool_routing() -> None:
    """Rebind ``kraken.futures.websocket.connect`` so Futures WS opts into the egress pool.

    Idempotent — safe to call multiple times.

    Unlike the Spot patch this is a narrower rebind: only the
    ``connect`` reference inside ``kraken.futures.websocket`` (which
    ``ConnectFuturesWebsocket.__run`` uses as ``async with
    connect(...)``) is wrapped in the ``_ConnectShim`` factory. The
    Retry-After / close-code / reconnect-storm machinery from the
    Spot patch is NOT mirrored here:

    * The Futures SDK has its own ``ConnectFuturesWebsocket`` class
      with a separate ``__get_reconnect_wait`` and reconnect loop
      that is not subject to the same Cloudflare 429 cascade pattern
      observed on the Spot path.
    * The Futures publisher does not implement the
      ``_on_sdk_reconnect_attempt`` watchdog hook, so registering
      Futures connectors via ``_patched_init`` would add no benefit.

    What this rebind DOES give the Futures path:

    * The connect shim consults ``get_egress_pool()`` and reserves a
      route when the pool is enabled, then merges ``{"proxy": None}``
      (direct) or ``{"proxy": "socks5h://..."}`` (SOCKS5) into the
      kwargs passed to ``websockets.connect``.
    * The reservation tag is read from the ``_CURRENT_PUBLISHER``
      ContextVar (set by ``KrakenFuturesMarketDataPublisher.start``),
      so per-exchange ``allowed_exchanges`` pins on egress_pool
      routes apply to Futures the same way they apply to Spot and
      Equities.
    * The 429-on-handshake quarantine path through
      ``__aenter__`` ➜ ``_handle_handshake_429`` is shared. A
      Cloudflare 429 on the Futures handshake will quarantine the
      borrowed route, NOT poison a global retry-after stash.
    """
    if _FUTURES_PATCH_APPLIED[0]:
        return
    setattr(_kraken_futures_ws, "connect", _wrap_connect_factory(_ws_client.connect))
    _FUTURES_PATCH_APPLIED[0] = True
    log_kraken_sdk_patches_status()


def _patched_manage_subscriptions(self: ConnectSpotWebsocketBase, message: JsonObject) -> None:
    """Replacement for ``ConnectSpotWebsocket._manage_subscriptions``.

    Mirrors the SDK's original logic but downgrades the benign
    ``{'error': 'Already subscribed'}`` subscribe response from WARNING
    to DEBUG. The SDK emits a single ``LOG.warning(message)`` for every
    non-success subscribe response — the race between our publisher
    health-loop's replay path and the SDK's existing subscription cache
    drowns real failures (invalid symbol, malformed args) under tens of
    thousands of benign races at WARNING level. Real subscription errors
    keep their WARNING.

    Name mangling on the SDK's private helpers (``__transform_subscription``,
    ``__append_subscription``, ``__remove_subscription``) is bypassed via
    runtime ``getattr`` against the mangled names — those helpers are
    defined on the ``ConnectSpotWebsocket`` subclass, not the base, so
    the typed ``self`` annotation does not surface them statically.
    """
    transform: Any = getattr(self, "_ConnectSpotWebsocket__transform_subscription")
    append: Any = getattr(self, "_ConnectSpotWebsocket__append_subscription")
    remove: Any = getattr(self, "_ConnectSpotWebsocket__remove_subscription")
    if message.get("method") == "subscribe":
        if message.get("success") and message.get("result"):
            transformed = transform(subscription=message)
            append(subscription=transformed["result"])
        elif message.get("error") == _ALREADY_SUBSCRIBED_ERROR:
            logger.debug("kraken-sdk subscribe race (already subscribed): {}", message)
        else:
            logger.warning("kraken-sdk subscribe failed: {}", message)
    elif message.get("method") == "unsubscribe":
        if message.get("success") and message.get("result"):
            transformed = transform(subscription=message)
            remove(subscription=transformed["result"])
        else:
            logger.warning("kraken-sdk unsubscribe failed: {}", message)


def apply_kraken_already_subscribed_filter() -> None:
    """Apply the Already-subscribed filter patch (idempotent).

    Replaces ``ConnectSpotWebsocket._manage_subscriptions`` with
    :func:`_patched_manage_subscriptions` so benign
    ``Already subscribed`` race-condition warnings from the SDK become
    DEBUG. Real subscription failures (invalid symbol, malformed args)
    keep their WARNING level.

    Background: production logs accumulated 208 866 WARNING records in
    9 h (88 % of all WARNINGs) from the SDK's blanket ``LOG.warning``
    on every non-success subscribe response. The publisher health loop
    re-subscribes to feeds the SDK already has cached → server replies
    ``{'error': 'Already subscribed'}`` → SDK warns → log noise floods
    out real signals.
    """
    if _ALREADY_SUBSCRIBED_PATCH_APPLIED[0]:
        return
    setattr(ConnectSpotWebsocket, "_manage_subscriptions", _patched_manage_subscriptions)
    _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] = True
    log_kraken_sdk_patches_status()


async def _patched_recover_subscriptions(self: ConnectSpotWebsocket, event: asyncio.Event) -> None:
    """Paced replacement for ``ConnectSpotWebsocket._recover_subscriptions``.

    The SDK's stock implementation replays the entire locally-tracked
    per-symbol subscription cache in a tight loop with no inter-message
    spacing. On a large universe (~340 Kraken Equities ticker+trade
    subscriptions) a single reconnect bursts ~340 subscribe messages
    back-to-back, exceeding Kraken's per-connection subscribe message-rate
    limit ("Exceeded msg rate"); the rejected re-subscribes leave those
    channels un-ACKed and the stream goes dark with only slow recovery
    (observed 2026-06-05 — a load-induced reconnect darkened the Equities
    ticker for ~17 min). The app-level health-loop re-subscribes are already
    paced by ``retry_subscribe_spacing_s``, but that guard does not cover
    this SDK reconnect replay path.

    This replacement preserves the SDK's recovery semantics — re-subscribe
    every tracked subscription, in order, once the readiness event is set —
    but sleeps ``_RESUBSCRIBE_PACE_S`` between consecutive sends so the burst
    stays under the rate limit. The cache is snapshotted before iterating so
    a concurrent ``_manage_subscriptions`` append cannot disturb the sweep,
    and the SDK's per-symbol ``OK`` log plus full-cache dump are collapsed to
    a single count-based summary to keep reconnects quiet.

    Args:
        self: The bound ``ConnectSpotWebsocket`` connector being recovered.
        event: Readiness event the connector sets once the socket is open.

    Returns:
        None.

    Raises:
        None.
    """
    scope = "authenticated" if self.is_auth else "public"
    subscriptions = list(self._subscriptions)
    total = len(subscriptions)
    logger.info("kraken-sdk recover {} subscriptions ({}): waiting", scope, total)
    await event.wait()
    for index, subscription in enumerate(subscriptions):
        await self.client.subscribe(params=subscription)
        if index < total - 1:
            await asyncio.sleep(_RESUBSCRIBE_PACE_S)
    logger.info("kraken-sdk recover {} subscriptions ({}): done", scope, total)


def apply_kraken_resubscribe_pacing() -> None:
    """Install the paced reconnect re-subscribe patch (idempotent).

    Replaces ``ConnectSpotWebsocket._recover_subscriptions`` with
    :func:`_patched_recover_subscriptions` so the SDK's post-reconnect cache
    replay paces its per-symbol subscribe sends by ``_RESUBSCRIBE_PACE_S``
    instead of bursting them, keeping recovery under Kraken's per-connection
    subscribe message-rate limit and preventing the reconnect-storm channel
    darkening observed 2026-06-05.

    Returns:
        None.

    Raises:
        None.
    """
    if _RESUBSCRIBE_PACE_PATCH_APPLIED[0]:
        return
    setattr(ConnectSpotWebsocket, "_recover_subscriptions", _patched_recover_subscriptions)
    _RESUBSCRIBE_PACE_PATCH_APPLIED[0] = True
    log_kraken_sdk_patches_status()


def log_kraken_sdk_patches_status() -> None:
    """Emit ``applied`` confirmations for kraken-sdk patches that are active.

    Decoupled from the ``apply_*`` functions so the boot log captures
    one INFO record per active patch regardless of when the patch was
    installed relative to ``setup_logging``. The eager-import path
    (``snapper.cli.app:142`` imports
    ``snapper.messaging.publishers.kraken`` at top-level which calls
    ``apply_kraken_retry_after_honoring`` and
    ``apply_kraken_already_subscribed_filter`` at module level) runs
    BEFORE :func:`snapper.utils.logging.setup_logging` has installed
    the loguru file sink. The lazy-import path (``importlib.import_module``
    inside the process_manager spawner for futures / equities publishers)
    runs AFTER it.

    The function uses two levels of gating to handle both paths
    correctly:

    * ``is_file_sink_ready()`` — early return until ``setup_logging``
      has wired the file sink. Without this guard, eager invocations
      from ``apply_*`` would emit to stderr only and the boot log file
      would still miss the confirmation.
    * ``_*_PATCH_LOGGED`` flags — record per-patch emission state so
      multiple invocations (one per ``apply_*`` call PLUS the explicit
      hook in :func:`snapper.__main__.main`) never produce duplicate
      ``applied`` records.

    Call sites:

    * :func:`snapper.__main__.main` and :func:`snapper.server.process_runner.main`
      call this immediately after ``setup_logging`` to flush any
      ``applied`` confirmations the eager-import path queued at module
      load time.
    * Each ``apply_*`` function calls this at the end of its installation
      block, so the lazy-import path produces its confirmation
      immediately after the patch is installed without waiting for an
      external call.

    The function is read-only with respect to ``_*_PATCH_APPLIED`` —
    it only inspects the installation flags, never sets them.
    """
    if not is_file_sink_ready():
        return
    if _PATCH_APPLIED[0] and not _PATCH_LOGGED[0]:
        logger.info("kraken-sdk Retry-After honoring applied")
        _PATCH_LOGGED[0] = True
    if _FUTURES_PATCH_APPLIED[0] and not _FUTURES_PATCH_LOGGED[0]:
        logger.info("kraken-sdk futures pool routing applied")
        _FUTURES_PATCH_LOGGED[0] = True
    if _ALREADY_SUBSCRIBED_PATCH_APPLIED[0] and not _ALREADY_SUBSCRIBED_PATCH_LOGGED[0]:
        logger.info("kraken-sdk Already-subscribed filter applied")
        _ALREADY_SUBSCRIBED_PATCH_LOGGED[0] = True
    if _RESUBSCRIBE_PACE_PATCH_APPLIED[0] and not _RESUBSCRIBE_PACE_PATCH_LOGGED[0]:
        logger.info("kraken-sdk reconnect re-subscribe pacing applied")
        _RESUBSCRIBE_PACE_PATCH_LOGGED[0] = True
    if _TEARDOWN_PATCH_APPLIED[0] and not _TEARDOWN_PATCH_LOGGED[0]:
        logger.info("kraken-sdk WS teardown hardening applied")
        _TEARDOWN_PATCH_LOGGED[0] = True
