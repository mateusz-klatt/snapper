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

import kraken.spot.websocket.connectors as _kraken_connectors
import websockets.asyncio.client as _ws_client
from kraken.spot.websocket.connectors import ConnectSpotWebsocketBase
from loguru import logger
from websockets.exceptions import InvalidStatus

_RETRY_AFTER_MIN_SECONDS: Final[float] = 1.0
"""Floor on Retry-After honoring — protects against zero/negative headers."""

_RETRY_AFTER_MAX_SECONDS: Final[float] = 900.0
"""Hard cap on Retry-After honoring (15 min sanity ceiling)."""

_RECONNECT_WINDOW_S: Final[float] = 60.0
"""Sliding window for the publisher-side reconnect-storm watchdog."""

_RECONNECT_LIMIT: Final[int] = 5
"""Maximum allowed reconnect attempts within ``_RECONNECT_WINDOW_S``."""

_PATCH_APPLIED: list[bool] = [False]
"""Single-element list flag tracking whether the patch is installed."""

_PENDING_RETRY_AFTER_S: dict[int, float] = {}
"""Maps ``id(connector_self)`` to its pending Retry-After deadline in seconds."""

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
    """Drop the registry entry. Wired via ``weakref.finalize``."""
    _CONNECTOR_PUBLISHERS.pop(connector_id, None)


def _wrap_connect_factory(original_connect: Any) -> Any:
    """Return a connect-shaped factory that captures Retry-After on 429.

    ``websockets.asyncio.client.connect`` is an async context manager
    factory. To preserve that API the factory returns a ``_ConnectShim``
    exposing ``__aenter__`` / ``__aexit__`` that delegates to the original
    context manager. On 429 the shim:

    1. Parses ``response.headers['Retry-After']`` from the
       ``websockets.exceptions.InvalidStatus`` exception.
    2. Looks up the current connector's identity via
       ``_CURRENT_CONNECTOR_ID`` (set by the patched ``__run``) and stashes
       the deadline in ``_PENDING_RETRY_AFTER_S``.
    3. Re-raises so the SDK proceeds to its reconnect path, where the
       patched ``__get_reconnect_wait`` will read the stash.

    Args:
        original_connect: The ``websockets.asyncio.client.connect``
            callable to delegate to.

    Returns:
        A class with the same constructor signature suitable for
        rebinding as ``kraken.spot.websocket.connectors.connect``.
    """

    class _ConnectShim:
        """Async context manager wrapping the SDK's ``connect(...)`` call."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._cm = original_connect(*args, **kwargs)

        async def __aenter__(self) -> Any:
            try:
                return await self._cm.__aenter__()
            except InvalidStatus as exc:
                response = getattr(exc, "response", None)
                if response is not None and getattr(response, "status_code", None) == 429:
                    retry_after = _parse_retry_after(getattr(response, "headers", None))
                    connector_id = _CURRENT_CONNECTOR_ID.get()
                    if retry_after is not None and connector_id is not None:
                        logger.warning(
                            "kraken WS handshake 429; Retry-After={}s (connector={})",
                            retry_after,
                            connector_id,
                        )
                        _PENDING_RETRY_AFTER_S[connector_id] = retry_after
                raise

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any:
            return await self._cm.__aexit__(exc_type, exc, tb)

    return _ConnectShim


async def _patched_run(self: ConnectSpotWebsocketBase, event: asyncio.Event) -> None:
    """Stamp ``_CURRENT_CONNECTOR_ID`` for the lifetime of the run coroutine.

    The connect shim reads the ContextVar inside its ``__aenter__`` to
    attribute the 429 response to the right connector. Resetting the
    token in the ``finally`` clause guarantees the ContextVar does not
    leak across coroutine restarts.
    """
    token = _CURRENT_CONNECTOR_ID.set(id(self))
    try:
        await _ORIGINAL_RUN(self, event)
    finally:
        _CURRENT_CONNECTOR_ID.reset(token)


def _patched_get_reconnect_wait(self: ConnectSpotWebsocketBase, attempts: int) -> float:
    """Honor stashed Retry-After before falling back to the SDK exponential.

    Args:
        self: The ``ConnectSpotWebsocketBase`` instance.
        attempts: The current reconnect attempt counter from the SDK.

    Returns:
        The number of seconds to sleep before the next reconnect.
        When a Retry-After is stashed for this connector the stashed
        value is popped and returned. Otherwise the SDK's original
        exponential backoff is preserved.
    """
    connector_id = id(self)
    if connector_id in _PENDING_RETRY_AFTER_S:
        retry_after = _PENDING_RETRY_AFTER_S.pop(connector_id)
        logger.info(
            "kraken WS honoring Retry-After: sleeping {}s (connector={})",
            retry_after,
            connector_id,
        )
        return retry_after
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
    """Notify the registered publisher before delegating to the SDK reconnect."""
    publisher = _CONNECTOR_PUBLISHERS.get(id(self))
    if publisher is not None:
        publisher._on_sdk_reconnect_attempt()
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
    logger.info("kraken-sdk Retry-After honoring applied")
