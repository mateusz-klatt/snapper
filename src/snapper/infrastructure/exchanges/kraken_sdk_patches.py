"""Monkeypatches for python-kraken-sdk to honor HTTP 429 Retry-After header.

Per ``proprietary/plans/plan_2026_05_21_kraken_429_retry_after_egress_pool.md``
Phase A. The SDK ignores the ``Retry-After`` header returned by Cloudflare
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
* :data:`_CURRENT_PUBLISHER` — publishers set this ``ContextVar`` inside
  their startup coroutine so any ``ConnectSpotWebsocketBase`` instance
  constructed during that span is associated with them.
* :func:`get_registered_publisher` — return the publisher associated with
  a given connector id, or ``None``.
"""

import asyncio
import contextvars
import weakref
from typing import Any
from typing import Final

import kraken.futures.websocket as _kraken_futures_ws
import kraken.spot.websocket.connectors as _kraken_connectors
import websockets.asyncio.client as _ws_client
from kraken.spot.websocket.connectors import ConnectSpotWebsocket
from kraken.spot.websocket.connectors import ConnectSpotWebsocketBase
from loguru import logger
from websockets.exceptions import ConnectionClosed
from websockets.exceptions import InvalidStatus

from snapper.core.json_types import JsonObject
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
_ORIGINAL_RECONNECT: Any = getattr(ConnectSpotWebsocketBase, "_ConnectSpotWebsocketBase__reconnect")


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
    except (TypeError, ValueError):
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


class _ConnectShim:
    """Async context manager wrapping the SDK's ``connect(...)`` call.

    Reservation lifecycle for a single handshake:

    1. ``__init__`` (sync): reserve a route from the pool when
       enabled, merge proxy kwarg, then build the underlying
       ``original_connect(...)`` context manager. Synchronous
       failure of ``original_connect`` releases the reservation.
    2. ``__aenter__``: opens the handshake. On HTTP 429,
       quarantine the active route (pool path) or stash the
       Retry-After (legacy path), release, re-raise.
    3. Caller's ``async with`` body runs.
    4. ``__aexit__``: if the exit carries
       ``ConnectionClosed(rcvd.code=1015)`` (Cloudflare
       close-after-handshake), quarantine the route with
       ``reason="close-1015"``. Other close codes do NOT
       quarantine — those are Phase A.3's job (1008/1011/1012/1013
       → per-close-code backoff, not egress-route fault).
       Then release the reservation in ``finally``.
    """

    def __init__(self, original_connect: Any, *args: Any, **kwargs: Any) -> None:
        self._reservation: EgressReservation | None = None
        pool = get_egress_pool()
        if pool is not None and pool.size() > 0:
            publisher = _CURRENT_PUBLISHER.get()
            exchange_name = publisher._get_exchange_name() if publisher is not None else "kraken"
            self._reservation = pool.reserve(
                exchange=exchange_name,
                purpose="websocket",
            )
            kwargs = {**kwargs, **self._reservation.websocket_kwargs()}
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
                "kraken WS handshake 429 on route '{}'; "
                "Retry-After={}s (pool-scoped quarantine)",
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

    Phase A.3 precedence (pool disabled, ``get_egress_pool() is None``):

    1. **Cloudflare 429 Retry-After** stash from the handshake shim —
       honor the server-sent deadline exactly.
    2. **Kraken WebSocket close code** stash from the last
       ``ConnectionClosed`` exception. Codes 1008/1011/1012/1013 get
       Kraken-tuned constant backoffs from ``_CLOSE_CODE_BACKOFF_S``;
       any other code falls through to the SDK exponential.
    3. **SDK exponential** — original ``random() * min(180, 2**n - 1)
       + 1`` formula, preserved as last-resort behaviour.

    Phase B' precedence (pool enabled):

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
    Phase B' acceptance criterion: a 429 on direct with a healthy
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
        if pool.has_available(exchange="kraken"):
            logger.info(
                "kraken WS pool-aware reconnect: healthy route available (connector={})",
                connector_id,
            )
            return _RETRY_AFTER_MIN_SECONDS
        earliest = pool.earliest_release_in_seconds(exchange="kraken") or 0.0
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


async def _patched_reconnect(self: ConnectSpotWebsocketBase) -> None:
    """Notify the registered publisher before delegating to the SDK reconnect.

    The publisher hook ``_on_sdk_reconnect_attempt`` is optional —
    only ``KrakenMarketDataPublisher`` (Spot) implements the
    reconnect-storm watchdog. Other Kraken publishers
    (``KrakenEquitiesMarketDataPublisher`` etc.) register themselves
    via ``_CURRENT_PUBLISHER`` for egress-pool exchange tagging but
    do not need the watchdog and intentionally omit the hook.
    Calling unconditionally would raise ``AttributeError`` and break
    every reconnect cycle for those publishers.
    """
    publisher = _CONNECTOR_PUBLISHERS.get(id(self))
    if publisher is not None:
        hook = getattr(publisher, "_on_sdk_reconnect_attempt", None)
        if hook is not None:
            hook()
    await _ORIGINAL_RECONNECT(self)


def apply_kraken_retry_after_honoring() -> None:
    """Apply the Retry-After patch (idempotent — safe to call multiple times).

    Five rebinds are performed on the ``ConnectSpotWebsocketBase`` class:

    * ``__get_reconnect_wait`` — consults ``_PENDING_RETRY_AFTER_S`` first.
    * ``__run`` — stamps the ``_CURRENT_CONNECTOR_ID`` ContextVar.
    * ``__init__`` — registers the connector with its owning publisher.
    * ``__reconnect`` — notifies the owning publisher's watchdog.
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
      observed on the Spot path (Phase A.3 incident 2026-05-21).
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
