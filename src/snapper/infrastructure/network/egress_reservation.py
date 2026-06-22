"""Reservation handle for a borrowed egress route.

A ``EgressReservation`` is what ``EgressPool.reserve()`` returns. It
carries enough state for callers to:

* Inject the right ``proxy=`` kwarg into ``websockets.connect(...)``
  via ``websocket_kwargs()``.
* Mark the underlying route quarantined when the handshake fails
  with HTTP 429 or the WS close-frame carries code 1015 via
  ``quarantine(retry_after_s, reason=...)``.
* Decrement the route's in-use counter exactly once via ``release()``.

Lifecycle correctness comes from explicit ``release()`` calls in the
SDK shim's three failure sites (sync-raise in ``__init__``,
any-exception in ``__aenter__``, finally in ``__aexit__``). A
``weakref.finalize`` is wired as a defensive fallback for code paths
that forget to release; on explicit release the finalizer is
detached so it cannot fire a second decrement after GC.
"""

import weakref
from abc import ABC
from abc import abstractmethod
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

from loguru import logger

QuarantineReason = Literal["http-429", "close-1015", "http-connect-error", "ws-connect-error"]
TrafficClass = Literal["public", "private"]
ConnectionKind = Literal["ws", "rest"]


class EgressPoolBase(ABC):
    """Abstract base satisfied by ``EgressPool``.

    Defined here (not in ``egress_pool``) so ``EgressReservation`` can
    type its pool reference without importing the concrete
    ``EgressPool`` class — that import would create a cycle
    (``egress_pool`` imports ``EgressReservation`` to construct them
    on ``reserve``).

    Abstract methods are the minimal surface ``EgressReservation`` and
    ``_finalize_release`` need to call back into the pool.
    """

    @abstractmethod
    def _decrement_in_use(
        self,
        route_id: str,
        exchange: str,
        traffic_class: TrafficClass,
        connection_kind: ConnectionKind,
        target_host: str | None,
    ) -> None:
        """Decrement ``in_use_count`` for the named route."""

    @abstractmethod
    def _quarantine_route(
        self,
        route_id: str,
        deadline: datetime,
        reason: QuarantineReason,
    ) -> None:
        """Mark the named route quarantined until ``deadline``."""


def _finalize_release(
    pool: EgressPoolBase,
    route_id: str,
    exchange: str,
    traffic_class: TrafficClass,
    connection_kind: ConnectionKind,
    target_host: str | None,
) -> None:
    """Defensive release fired by ``weakref.finalize`` on GC.

    Runs only when ``EgressReservation.release()`` was NOT called
    before the reservation became unreachable. ``pool``, ``route_id``,
    ``exchange``, and ``traffic_class`` are captured by value so the
    closure does not keep the reservation alive.

    Args:
        pool: The ``EgressPool`` that issued the reservation.
        route_id: The route whose ``in_use_count`` should be
            decremented.
        exchange: Exchange tuple member to clear from active status.
        traffic_class: Traffic-class tuple member to clear from active
            status.
        connection_kind: Connection kind tuple member to clear from
            active host status.
        target_host: Sanitized hostname tuple member to clear from
            active host status.
    """
    pool._decrement_in_use(route_id, exchange, traffic_class, connection_kind, target_host)


class EgressReservation:
    """Borrowed-route handle returned by ``EgressPool.reserve()``.

    Attributes:
        route_id: The id of the route this reservation borrows.
        proxy_url: ``None`` for direct routes; the SOCKS5 URL for
            socks5 routes.
        exchange: Exchange name used to reserve the route.
        traffic_class: Traffic class used to reserve the route.
        connection_kind: Connection kind used for host-aware observability.
        target_host: Sanitized target hostname, or ``None`` when unknown
            or invalid.
    """

    def __init__(
        self,
        pool: EgressPoolBase,
        route_id: str,
        proxy_url: str | None,
        exchange: str,
        traffic_class: TrafficClass,
        connection_kind: ConnectionKind,
        target_host: str | None,
    ) -> None:
        """Create a reservation. Called by ``EgressPool.reserve``.

        The pool is responsible for incrementing ``in_use_count``
        BEFORE handing the reservation to the caller; this
        constructor only stores references.

        Args:
            pool: The owning ``EgressPool``.
            route_id: The reserved route's id.
            proxy_url: The reserved route's ``proxy_url`` (None for direct).
            exchange: Exchange name associated with this reservation.
            traffic_class: Traffic class associated with this reservation.
            connection_kind: ``"ws"`` for WebSocket reservations or
                ``"rest"`` for REST reservations.
            target_host: Sanitized target hostname, or ``None`` when unknown.
        """
        self.route_id = route_id
        self.proxy_url = proxy_url
        self.exchange = exchange
        self.traffic_class = traffic_class
        self.connection_kind = connection_kind
        self.target_host = target_host
        self._pool = pool
        self._released = False
        self._finalizer = weakref.finalize(
            self,
            _finalize_release,
            pool,
            route_id,
            exchange,
            traffic_class,
            connection_kind,
            target_host,
        )

    def websocket_kwargs(self) -> dict[str, str | None]:
        """Return kwargs for ``websockets.connect(...)``.

        Direct routes inject ``{"proxy": None}`` (explicitly
        overriding the websockets-16 default of ``proxy=True`` that
        would auto-detect ``HTTPS_PROXY``). SOCKS5 routes inject
        ``{"proxy": "socks5h://..."}``.

        Returns:
            A dict suitable for ``{**existing_kwargs, **kwargs}``
            merging into the SDK's ``websockets.connect`` call.
            Always contains exactly one key, ``proxy``, whose value
            is ``None`` for direct routes or the SOCKS5 URL string
            for socks5 routes.
        """
        if self.proxy_url is None:
            return {"proxy": None}
        return {"proxy": self.proxy_url}

    def release(self) -> None:
        """Decrement the route's ``in_use_count`` exactly once.

        Idempotent: repeated calls are no-ops. Detaches the
        ``weakref.finalize`` so the defensive GC path cannot fire a
        second decrement.
        """
        if self._released:
            return
        self._released = True
        self._finalizer.detach()
        self._pool._decrement_in_use(
            self.route_id,
            self.exchange,
            self.traffic_class,
            self.connection_kind,
            self.target_host,
        )

    def quarantine(
        self,
        retry_after_s: float,
        *,
        reason: QuarantineReason,
    ) -> None:
        """Mark the borrowed route quarantined for ``retry_after_s`` s.

        Idempotent: extends the existing deadline if a longer one is
        passed; never shortens. Operator-visible at INFO level once
        per quarantine event.

        Args:
            retry_after_s: Seconds from now until the route may be
                picked again. Negative or zero values are clamped to
                a minimum of 1 s to avoid no-op quarantines.
            reason: ``http-429`` for handshake 429,
                ``close-1015`` for WS close-frame 1015,
                ``http-connect-error`` for HTTP connect-level
                failure raised by ``PooledAsyncTransport``,
                ``ws-connect-error`` for a WS connect-level failure
                (TCP/SOCKS timeout, refused, or blackhole) raised
                while opening the handshake through a proxy route.
        """
        if retry_after_s < 1.0:
            retry_after_s = 1.0
        new_deadline = datetime.now(UTC) + timedelta(seconds=retry_after_s)
        self._pool._quarantine_route(self.route_id, new_deadline, reason)
        logger.info(
            "egress_pool: route '{}' quarantined for {}s (reason={})",
            self.route_id,
            retry_after_s,
            reason,
        )
