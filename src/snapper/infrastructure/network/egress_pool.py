"""Process-wide registry of egress routes with quarantine state.

The pool is a singleton populated by ``configure_egress_pool`` during
startup; ``get_egress_pool`` returns the configured instance or
``None``.

DNS resolution, ``python-socks`` availability, and SOCKS5 listener
checks are done asynchronously in ``initialize_egress_pool``, which
is invoked from the FastAPI lifespan BEFORE publishers come up. The
pool itself performs no I/O on construction — that work lives in the
preflight.

All pool mutation is guarded by a single ``threading.RLock`` because
the connect-shim's ``__init__`` is synchronous; ``asyncio.Lock``
would require wrapping every reserve call in a coroutine. The
critical sections are bookkeeping-only (microseconds).
"""

import asyncio
import contextlib
import importlib.util
import json
import os
import socket
import threading
from collections import OrderedDict
from collections.abc import Collection
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from time import monotonic
from typing import Final
from typing import Literal
from urllib.parse import urlsplit

from loguru import logger

from snapper.application.services.settings import SettingsService
from snapper.infrastructure.network.egress_exceptions import AllRoutesQuarantinedError
from snapper.infrastructure.network.egress_models import EgressActiveReservationSnapshot
from snapper.infrastructure.network.egress_models import EgressConnectionSnapshot
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import EgressPoolStatusSnapshot
from snapper.infrastructure.network.egress_models import EgressRouteStatusSnapshot
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_models import RouteSelection
from snapper.infrastructure.network.egress_models import RouteSnapshot
from snapper.infrastructure.network.egress_models import RouteState
from snapper.infrastructure.network.egress_reservation import ConnectionKind
from snapper.infrastructure.network.egress_reservation import EgressPoolBase
from snapper.infrastructure.network.egress_reservation import EgressReservation
from snapper.infrastructure.network.egress_reservation import QuarantineReason
from snapper.infrastructure.network.egress_reservation import TrafficClass

_PYTHON_SOCKS_WARNING_LOGGED: list[bool] = [False]
"""One-shot flag to throttle the ``python-socks not installed`` warning."""

_MAX_REST_HOSTS_PER_ROUTE = 16
_HOST_FORBIDDEN_CHARS = frozenset("/\\?#@")
ConnectionKey = tuple[str, TrafficClass, ConnectionKind, str]
_PreflightDisposition = Literal["keep", "disable", "quarantine"]

_PREFLIGHT_QUARANTINE_S: Final[float] = 30.0
"""Initial retry delay for a transient startup failure, matching WS connect failures."""


def normalize_egress_target_host(target_host: str | None) -> str | None:
    """Return a safe lowercase hostname for egress observability.

    Args:
        target_host: Candidate hostname from a parsed URL or request object.

    Returns:
        Lowercase host string, or ``None`` when the candidate is empty,
        contains control characters, whitespace, or URL/path separators.
    """
    if target_host is None:
        return None
    for char in target_host:
        codepoint = ord(char)
        if codepoint < 32 or codepoint == 127:
            return None
    host = target_host.strip().lower()
    if not host:
        return None
    for char in host:
        if char.isspace() or char in _HOST_FORBIDDEN_CHARS:
            return None
    return host


def _connection_kind_for_purpose(purpose: Literal["websocket", "http"]) -> ConnectionKind:
    """Return the default connection kind for a reservation purpose.

    Args:
        purpose: Reservation purpose supplied by the caller.

    Returns:
        ``"ws"`` for WebSocket reservations and ``"rest"`` for HTTP ones.
    """
    if purpose == "websocket":
        return "ws"
    return "rest"


def _new_public_direct_state(route_ids: Collection[str]) -> RouteState:
    """Create a dev direct route with its own counters and quarantine state."""
    base = "public-dev-direct"
    route_id = base
    suffix = 0
    while route_id in route_ids:
        suffix += 1
        route_id = f"{base}-{suffix}"
    return RouteState(config=RouteConfig(id=route_id, kind="direct"), enabled=True)


class EgressPool(EgressPoolBase):
    """Process-wide registry of egress routes with quarantine state.

    Not instantiated directly outside tests — use
    ``configure_egress_pool(config)`` or ``initialize_egress_pool``.

    The pool guarantees ``in_use_count`` never goes negative even
    under multi-fire scenarios (explicit release racing the weakref
    finalizer). Quarantine deadlines are extend-only — never
    shortened by a subsequent shorter retry_after.
    """

    def __init__(self, config: EgressPoolConfig, *, public_direct_allowed: bool = False) -> None:
        """Build the pool from a validated config.

        Args:
            config: The top-level ``EgressPoolConfig`` payload, already
                validated by Pydantic. Startup preflight may disable
                routes with permanent configuration errors.
            public_direct_allowed: Explicit dev-host permission to use a
                separate direct route when public VPNs are unavailable.
        """
        self._config = config
        self._lock = threading.RLock()
        self._states: dict[str, RouteState] = {
            route.id: RouteState(config=route, enabled=route.enabled) for route in config.routes
        }
        self._public_direct: RouteState | None = None
        if public_direct_allowed:
            self._public_direct = _new_public_direct_state(self._states.keys())
            self._states[self._public_direct.config.id] = self._public_direct
        self._public_direct_episodes: set[str] = set()
        self._public_wait_started: dict[str, float] = {}
        self._public_wait_seconds: dict[str, float] = {}
        self._active_reservations: dict[str, dict[tuple[str, TrafficClass], int]] = {
            route_id: {} for route_id in self._states
        }
        self._active_connection_counts: dict[str, dict[ConnectionKey, int]] = {
            route_id: {} for route_id in self._states
        }
        self._rest_last_seen: dict[str, OrderedDict[ConnectionKey, datetime]] = {
            route_id: OrderedDict() for route_id in self._states
        }

    def size(self) -> int:
        """Total number of enabled routes (quarantined or not).

        Returns:
            Count of routes with ``enabled=True``.
        """
        with self._lock:
            return sum(1 for s in self._states.values() if s.enabled)

    def has_available(self, exchange: str | None = None, traffic_class: str = "public") -> bool:
        """Report whether the requested traffic class can reserve a healthy route.

        Public traffic can use healthy public VPNs or its explicitly enabled
        dev fallback. A public exchange name identifies the wait episode.
        Private availability follows the direct and private fallback policy.
        """
        now = datetime.now(UTC)
        with self._lock:
            if traffic_class == "private":
                return self._pick_private_locked(now) is not None
            vpn_available = any(
                self._is_public_vpn(state) and self._is_available_locked(state, now)
                for state in self._states.values()
            )
            available = vpn_available or self._available_public_direct_locked(now) is not None
            if exchange is not None:
                if vpn_available:
                    self._public_direct_episodes.discard(exchange)
                if available:
                    self._finish_public_wait_locked(exchange)
                else:
                    self._begin_public_wait_locked(exchange)
            return available

    def earliest_release_in_seconds(
        self, exchange: str | None = None, traffic_class: str = "public"
    ) -> float | None:
        """Minimum seconds until any quarantine deadline expires.

        Args:
            exchange: Exchange requesting the retry deadline. Any eligible
                public VPN can end the wait.
            traffic_class: Private considers enabled direct routes and the
                configured private fallback. Public considers public VPN routes
                and the dev direct route when enabled.

        Returns:
            ``None`` if no matching route is currently quarantined.
            ``0.0`` if the earliest matching deadline is already in
            the past. Otherwise the positive number of seconds until
            release.
        """
        now = datetime.now(UTC)
        effective_traffic_class: TrafficClass = (
            "private" if traffic_class == "private" else "public"
        )
        with self._lock:
            if effective_traffic_class == "private":
                fallback_route_id = self._config.private_fallback_route_id
                deadlines = [
                    s.quarantine_until
                    for s in self._states.values()
                    if s.enabled
                    and s.quarantine_until is not None
                    and (self._is_private_direct(s) or s.config.id == fallback_route_id)
                ]
            else:
                deadlines = [
                    s.quarantine_until
                    for s in self._states.values()
                    if s.enabled
                    and s.quarantine_until is not None
                    and (self._is_public_vpn(s) or s is self._public_direct)
                ]
        if not deadlines:
            return None
        earliest = min(deadlines)
        delta = (earliest - now).total_seconds()
        if delta < 0.0:
            return 0.0
        return delta

    def reserve(
        self,
        *,
        exchange: str,
        purpose: Literal["websocket", "http"],
        preferred_route: str | None = None,
        traffic_class: str = "public",
        target_host: str | None = None,
        connection_kind: ConnectionKind | None = None,
    ) -> EgressReservation:
        """Pick the best route and return a borrowed handle.

        Public selection uses healthy VPN routes outside the private fallback.
        A healthy preferred VPN
        wins when allowed for the exchange. Otherwise allowed VPNs are ranked
        by ``(priority, in_use_count)``. If none are available, rank any healthy
        eligible VPN by the same rule and log the pin override. If no eligible
        VPN is available, use the dev direct route when allowed and healthy.
        Otherwise raise under either ``on_all_quarantined`` policy.

        Private selection policy:

        * Pick the first healthy direct route.
        * If no direct route is healthy, pick
          ``private_fallback_route_id`` when configured and healthy,
          ignoring that route's ``allowed_exchanges``.
        * If neither route is healthy, apply ``on_all_quarantined``.

        Args:
            exchange: The exchange name used to filter routes by
                ``RouteConfig.allowed_exchanges`` when ranking public routes.
            purpose: ``"websocket"`` or ``"http"``; included in
                route-exhaustion error messages.
            preferred_route: Optional route id hint.
            traffic_class: ``"public"`` uses public VPN routes and the opt-in dev fallback.
                ``"private"`` uses the direct
                then configured fallback policy. Any other value is
                treated as ``"public"``.
            target_host: Parsed target hostname for read-only observability.
            connection_kind: ``"ws"`` or ``"rest"``. When omitted, the
                value is derived from ``purpose``.

        Returns:
            A fresh ``EgressReservation`` with the route's
            ``in_use_count`` already incremented.

        Raises:
            AllRoutesQuarantinedError: When the corresponding policy
                provides no route.
        """
        now = datetime.now(UTC)
        effective_traffic_class: TrafficClass = (
            "private" if traffic_class == "private" else "public"
        )
        effective_connection_kind = connection_kind or _connection_kind_for_purpose(purpose)
        normalized_host = normalize_egress_target_host(target_host)
        with self._lock:
            selection = self._pick_locked(preferred_route, exchange, now, effective_traffic_class)
            if effective_traffic_class == "public":
                selection = self._complete_public_selection_locked(
                    selection, exchange, purpose, now
                )
            elif selection is None:
                selection = self._exhausted_private_selection_locked(exchange, purpose)
            selection.state.in_use_count += 1
            selection.state.last_pick_at = now
            route_config = selection.state.config
            route_id = route_config.id
            self._increment_active_reservation_locked(route_id, exchange, effective_traffic_class)
            self._increment_connection_locked(
                route_id,
                exchange,
                effective_traffic_class,
                effective_connection_kind,
                normalized_host,
                now,
            )
            route_kind = route_config.kind
            proxy_url = route_config.proxy_url
            is_fallback = selection.is_fallback
        if purpose == "websocket":
            logger.info(
                "egress_pool: selected route exchange={} traffic_class={} purpose={} "
                "route_id={} route_kind={} is_fallback={} proxy_url={} pid={} host={}",
                exchange,
                effective_traffic_class,
                purpose,
                route_id,
                route_kind,
                is_fallback,
                proxy_url or "direct",
                os.getpid(),
                socket.gethostname(),
            )
        return EgressReservation(
            pool=self,
            route_id=route_id,
            proxy_url=proxy_url,
            exchange=exchange,
            traffic_class=effective_traffic_class,
            connection_kind=effective_connection_kind,
            target_host=normalized_host,
        )

    def _begin_public_wait_locked(self, exchange: str) -> None:
        """Start one wait episode per exchange, including concurrent callers."""
        if exchange not in self._public_wait_started:
            self._public_wait_started[exchange] = monotonic()
            logger.warning("egress_pool: public VPN wait started exchange={}", exchange)

    def _available_public_direct_locked(self, now: datetime) -> RouteState | None:
        """Return the dedicated dev direct route when it can be used."""
        direct = self._public_direct
        if direct is not None and self._is_available_locked(direct, now):
            return direct
        return None

    def _complete_public_selection_locked(
        self,
        vpn: RouteSelection | None,
        exchange: str,
        purpose: Literal["websocket", "http"],
        now: datetime,
    ) -> RouteSelection:
        """Resolve public exhaustion and log the first direct reservation in an episode."""
        if vpn is not None:
            self._public_direct_episodes.discard(exchange)
            self._finish_public_wait_locked(exchange)
            return vpn
        direct = self._available_public_direct_locked(now)
        if direct is None:
            self._begin_public_wait_locked(exchange)
            raise AllRoutesQuarantinedError(
                f"no public VPN route available (exchange={exchange}, purpose={purpose})"
            )
        self._finish_public_wait_locked(exchange)
        if exchange not in self._public_direct_episodes:
            self._public_direct_episodes.add(exchange)
            logger.warning(
                "egress_pool: public direct fallback exchange={} route_id={}. "
                "public market data may leave from the host's own IP",
                exchange,
                direct.config.id,
            )
        return RouteSelection(state=direct, is_fallback=True)

    def _finish_public_wait_locked(self, exchange: str) -> None:
        """Record elapsed and cumulative wait seconds when a VPN becomes usable."""
        started = self._public_wait_started.pop(exchange, None)
        if started is None:
            return
        elapsed = monotonic() - started
        total = self._public_wait_seconds.get(exchange, 0.0) + elapsed
        self._public_wait_seconds[exchange] = total
        logger.warning(
            "egress_pool: public VPN wait ended exchange={} wait_seconds={} "
            "public_vpn_wait_seconds_total={}",
            exchange,
            elapsed,
            total,
        )

    def _exhausted_private_selection_locked(
        self, exchange: str, purpose: Literal["websocket", "http"]
    ) -> RouteSelection:
        """Apply the private all-quarantined policy with the pool lock held."""
        scope = f"(exchange={exchange}, traffic_class=private, purpose={purpose})"
        if self._config.on_all_quarantined == "raise":
            raise AllRoutesQuarantinedError(f"all egress routes quarantined {scope}")
        fallback = self._fallback_direct_locked()
        if fallback is None:
            raise AllRoutesQuarantinedError(
                f"all egress routes quarantined and no direct fallback available {scope}"
            )
        return RouteSelection(state=fallback, is_fallback=True)

    def reserve_private_direct(
        self,
        *,
        exchange: str,
        purpose: Literal["websocket", "http"],
        target_host: str | None = None,
        connection_kind: ConnectionKind = "rest",
    ) -> EgressReservation:
        """Reserve the enabled direct route for private direct-only REST traffic.

        This is intentionally narrower than ``reserve(traffic_class="private")``:
        order-mutating REST calls must remain direct even when the direct route
        is quarantined for WebSocket/private-read selection, so this method
        never selects the configured PL fallback.

        Args:
            exchange: Exchange name attached to the active reservation map.
            purpose: ``"websocket"`` or ``"http"`` for diagnostics.
            target_host: Parsed target hostname for read-only observability.
            connection_kind: ``"ws"`` or ``"rest"`` host bucket.

        Returns:
            Direct-route reservation with ``traffic_class="private"``.

        Raises:
            AllRoutesQuarantinedError: When no enabled direct route exists.
        """
        now = datetime.now(UTC)
        normalized_host = normalize_egress_target_host(target_host)
        with self._lock:
            direct = self._fallback_direct_locked()
            if direct is None:
                raise AllRoutesQuarantinedError(
                    f"no direct egress route available "
                    f"(exchange={exchange}, traffic_class=private, purpose={purpose})"
                )
            direct.in_use_count += 1
            direct.last_pick_at = now
            route_id = direct.config.id
            proxy_url = direct.config.proxy_url
            self._increment_active_reservation_locked(route_id, exchange, "private")
            self._increment_connection_locked(
                route_id,
                exchange,
                "private",
                connection_kind,
                normalized_host,
                now,
            )
        return EgressReservation(
            pool=self,
            route_id=route_id,
            proxy_url=proxy_url,
            exchange=exchange,
            traffic_class="private",
            connection_kind=connection_kind,
            target_host=normalized_host,
        )

    def reserve_private_fallback(
        self,
        *,
        exchange: str,
        target_host: str | None = None,
        connection_kind: ConnectionKind = "rest",
    ) -> EgressReservation | None:
        """Reserve the configured private fallback route if it is healthy.

        Used only after a private idempotent REST read proves a direct
        pre-send connect failure. The fallback route bypasses
        ``allowed_exchanges`` just like the WebSocket private selector,
        but this method never falls back to direct and never applies
        ``on_all_quarantined``.

        Args:
            exchange: Exchange name attached to the active reservation map.
            target_host: Parsed target hostname for read-only observability.
            connection_kind: ``"ws"`` or ``"rest"`` host bucket.

        Returns:
            Fallback-route reservation, or ``None`` when no configured and
            healthy fallback exists.
        """
        now = datetime.now(UTC)
        normalized_host = normalize_egress_target_host(target_host)
        with self._lock:
            fallback_route_id = self._config.private_fallback_route_id
            if fallback_route_id is None:
                return None
            fallback = self._states.get(fallback_route_id)
            if fallback is None or not self._is_available_locked(fallback, now):
                return None
            fallback.in_use_count += 1
            fallback.last_pick_at = now
            route_id = fallback.config.id
            proxy_url = fallback.config.proxy_url
            self._increment_active_reservation_locked(route_id, exchange, "private")
            self._increment_connection_locked(
                route_id,
                exchange,
                "private",
                connection_kind,
                normalized_host,
                now,
            )
        return EgressReservation(
            pool=self,
            route_id=route_id,
            proxy_url=proxy_url,
            exchange=exchange,
            traffic_class="private",
            connection_kind=connection_kind,
            target_host=normalized_host,
        )

    def snapshot(self) -> list[RouteSnapshot]:
        """Read-only view of route states for tests + observability.

        Returns:
            A list of ``RouteSnapshot`` value objects, one per route,
            in insertion order. Snapshots are frozen so callers
            cannot reach back into the pool's mutable state.
        """
        with self._lock:
            return [
                RouteSnapshot(
                    id=s.config.id,
                    kind=s.config.kind,
                    proxy_url=s.config.proxy_url,
                    priority=s.config.priority,
                    enabled=s.enabled,
                    quarantine_until=s.quarantine_until,
                    in_use_count=s.in_use_count,
                    last_handshake_429_at=s.last_handshake_429_at,
                    last_close_1015_at=s.last_close_1015_at,
                )
                for s in self._states.values()
            ]

    def status_snapshot(self) -> EgressPoolStatusSnapshot:
        """Return an operator status snapshot of the egress pool.

        Returns:
            A Pydantic snapshot containing pool-level policy and one
            row per route with metadata, quarantine state, in-use count,
            and unique active reservation traffic tuples.
        """
        now = datetime.now(UTC)
        with self._lock:
            routes: list[EgressRouteStatusSnapshot] = []
            private_on_fallback = False
            for state in self._states.values():
                active_reservations = self._active_snapshot_locked(state.config.id)
                connections = self._connection_snapshot_locked(state.config.id)
                if state.config.kind != "direct" and any(
                    item.traffic_class == "private" for item in active_reservations
                ):
                    private_on_fallback = True
                quarantine_seconds_remaining = self._quarantine_remaining_locked(state, now)
                routes.append(
                    EgressRouteStatusSnapshot(
                        id=state.config.id,
                        kind=state.config.kind,
                        proxy_url=state.config.proxy_url,
                        region=state.config.region,
                        exit_ip=state.config.exit_ip,
                        provider=state.config.provider,
                        priority=state.config.priority,
                        allowed_exchanges=list(state.config.allowed_exchanges),
                        enabled=state.enabled,
                        quarantined=(
                            quarantine_seconds_remaining is not None
                            and quarantine_seconds_remaining > 0.0
                        ),
                        quarantine_seconds_remaining=quarantine_seconds_remaining,
                        in_use_count=state.in_use_count,
                        active_reservations=active_reservations,
                        connections=connections,
                    )
                )
            return EgressPoolStatusSnapshot(
                enabled=self._config.enabled,
                on_all_quarantined=self._config.on_all_quarantined,
                private_fallback_route_id=self._config.private_fallback_route_id,
                private_on_fallback=private_on_fallback,
                routes=routes,
            )

    def _decrement_in_use(
        self,
        route_id: str,
        exchange: str,
        traffic_class: TrafficClass,
        connection_kind: ConnectionKind,
        target_host: str | None,
    ) -> None:
        """Decrement a route's ``in_use_count`` (clamped to >= 0).

        Called by ``EgressReservation.release`` and by the
        ``weakref.finalize`` defensive path. Safe to call multiple
        times — clamping ensures the count cannot go negative.
        """
        with self._lock:
            state = self._states.get(route_id)
            if state is None:
                return
            state.in_use_count = max(0, state.in_use_count - 1)
            self._decrement_active_reservation_locked(route_id, exchange, traffic_class)
            self._decrement_connection_locked(
                route_id,
                exchange,
                traffic_class,
                connection_kind,
                target_host,
            )

    def _quarantine_route(
        self,
        route_id: str,
        deadline: datetime,
        reason: QuarantineReason,
    ) -> None:
        """Mark a route quarantined until ``deadline``. Extend-only.

        The reason-specific timestamp markers are recorded only for the cause
        they name: ``http-429`` sets ``last_handshake_429_at`` and ``close-1015``
        sets ``last_close_1015_at``. Startup preflight, connect, and proxy
        failures only advance ``quarantine_until``. The failure logs
        carry the cause.
        """
        with self._lock:
            state = self._states.get(route_id)
            if state is None:
                return
            existing = state.quarantine_until
            if existing is None or deadline > existing:
                state.quarantine_until = deadline
            if reason == "http-429":
                state.last_handshake_429_at = datetime.now(UTC)
            elif reason == "close-1015":
                state.last_close_1015_at = datetime.now(UTC)

    def _is_available_locked(self, state: RouteState, now: datetime) -> bool:
        """Return True if ``state`` is enabled and not quarantined as of ``now``.

        Pool lock MUST be held.
        """
        if not state.enabled:
            return False
        if state.quarantine_until is None:
            return True
        return state.quarantine_until <= now

    def _increment_active_reservation_locked(
        self,
        route_id: str,
        exchange: str,
        traffic_class: TrafficClass,
    ) -> None:
        """Increment the active reservation tuple count for a route.

        Pool lock MUST be held.
        """
        reservations = self._active_reservations.get(route_id)
        if reservations is None:
            return
        key = (exchange, traffic_class)
        reservations[key] = reservations.get(key, 0) + 1

    def _decrement_active_reservation_locked(
        self,
        route_id: str,
        exchange: str,
        traffic_class: TrafficClass,
    ) -> None:
        """Decrement the active reservation tuple count for a route.

        Pool lock MUST be held.
        """
        reservations = self._active_reservations.get(route_id)
        if reservations is None:
            return
        key = (exchange, traffic_class)
        count = reservations.get(key)
        if count is None:
            return
        if count <= 1:
            del reservations[key]
            return
        reservations[key] = count - 1

    def _increment_connection_locked(
        self,
        route_id: str,
        exchange: str,
        traffic_class: TrafficClass,
        connection_kind: ConnectionKind,
        target_host: str | None,
        observed_at: datetime,
    ) -> None:
        """Increment the active host tuple count for a route.

        Pool lock MUST be held.
        """
        if target_host is None:
            return
        connections = self._active_connection_counts.get(route_id)
        if connections is None:
            return
        key = (exchange, traffic_class, connection_kind, target_host)
        connections[key] = connections.get(key, 0) + 1
        if connection_kind == "rest":
            self._record_rest_last_seen_locked(route_id, key, observed_at)

    def _decrement_connection_locked(
        self,
        route_id: str,
        exchange: str,
        traffic_class: TrafficClass,
        connection_kind: ConnectionKind,
        target_host: str | None,
    ) -> None:
        """Decrement the active host tuple count for a route.

        Pool lock MUST be held.
        """
        if target_host is None:
            return
        connections = self._active_connection_counts.get(route_id)
        if connections is None:
            return
        key = (exchange, traffic_class, connection_kind, target_host)
        count = connections.get(key)
        if count is None:
            return
        if count <= 1:
            del connections[key]
            return
        connections[key] = count - 1

    def _record_rest_last_seen_locked(
        self,
        route_id: str,
        key: ConnectionKey,
        observed_at: datetime,
    ) -> None:
        """Record one REST host observation, capped by route.

        Pool lock MUST be held.
        """
        hosts = self._rest_last_seen.get(route_id)
        if hosts is None:
            return
        hosts.pop(key, None)
        hosts[key] = observed_at
        while len(hosts) > _MAX_REST_HOSTS_PER_ROUTE:
            hosts.popitem(last=False)

    def _active_snapshot_locked(self, route_id: str) -> list[EgressActiveReservationSnapshot]:
        """Return sorted unique active reservation tuples for a route.

        Pool lock MUST be held.
        """
        reservations = self._active_reservations.get(route_id, {})
        return [
            EgressActiveReservationSnapshot(exchange=exchange, traffic_class=traffic_class)
            for exchange, traffic_class in sorted(reservations)
        ]

    def _connection_snapshot_locked(self, route_id: str) -> list[EgressConnectionSnapshot]:
        """Return sorted connection rows for a route.

        Pool lock MUST be held.
        """
        connections = self._active_connection_counts.get(route_id, {})
        rows = [
            EgressConnectionSnapshot(
                host=host,
                kind=connection_kind,
                exchange=exchange,
                traffic_class=traffic_class,
                count=count,
                last_seen_at=None,
            )
            for (exchange, traffic_class, connection_kind, host), count in sorted(
                connections.items(),
                key=lambda item: (item[0][3], item[0][2], item[0][0], item[0][1]),
            )
            if connection_kind == "ws"
        ]
        rest_seen = self._rest_last_seen.get(route_id, OrderedDict())
        rows.extend(
            EgressConnectionSnapshot(
                host=host,
                kind=connection_kind,
                exchange=exchange,
                traffic_class=traffic_class,
                count=connections.get((exchange, traffic_class, connection_kind, host), 0),
                last_seen_at=last_seen_at,
            )
            for (exchange, traffic_class, connection_kind, host), last_seen_at in sorted(
                rest_seen.items(),
                key=lambda item: (item[0][3], item[0][2], item[0][0], item[0][1]),
            )
        )
        return rows

    @staticmethod
    def _quarantine_remaining_locked(state: RouteState, now: datetime) -> float | None:
        """Return remaining quarantine seconds for a route.

        Pool lock MUST be held.
        """
        if state.quarantine_until is None:
            return None
        remaining = (state.quarantine_until - now).total_seconds()
        if remaining < 0.0:
            return 0.0
        return remaining

    def _pick_locked(
        self,
        preferred_route: str | None,
        exchange: str,
        now: datetime,
        traffic_class: TrafficClass,
    ) -> RouteSelection | None:
        """Pick a route with the pool lock held, preferring allowed VPNs for public."""
        if traffic_class == "private":
            return self._pick_private_locked(now)
        available = [
            state
            for state in self._states.values()
            if self._is_public_vpn(state) and self._is_available_locked(state, now)
        ]
        if not available:
            return None
        preferred = next((state for state in available if state.config.id == preferred_route), None)
        if preferred is not None and self._exchange_allows_locked(preferred, exchange):
            return RouteSelection(state=preferred, is_fallback=False)
        pick = min(
            available,
            key=lambda state: (
                not self._exchange_allows_locked(state, exchange),
                state.config.priority,
                state.in_use_count,
            ),
        )
        if not self._exchange_allows_locked(pick, exchange):
            logger.warning(
                "egress_pool: using unpinned public VPN route exchange={} route_id={}",
                exchange,
                pick.config.id,
            )
        return RouteSelection(state=pick, is_fallback=False)

    def _is_public_vpn(self, state: RouteState) -> bool:
        """Identify VPN routes eligible for public selection and retry timing."""
        return (
            state.config.kind == "socks5"
            and state.config.id != self._config.private_fallback_route_id
        )

    def _is_private_direct(self, state: RouteState) -> bool:
        """Identify configured direct routes, excluding the dedicated dev route."""
        return state.config.kind == "direct" and state is not self._public_direct

    def _pick_private_locked(self, now: datetime) -> RouteSelection | None:
        """Return the private direct route or configured fallback route.

        Private executor traffic is deliberately narrower than public
        routing: a healthy direct route wins regardless of priority, and
        the configured private fallback is considered only when direct is
        unavailable. The private fallback ignores ``allowed_exchanges``.

        Args:
            now: Current UTC timestamp used for quarantine checks.

        Returns:
            The selected route, or ``None`` when neither private route is
            available.
        """
        for state in self._states.values():
            if self._is_private_direct(state) and self._is_available_locked(state, now):
                return RouteSelection(state=state, is_fallback=False)
        fallback_route_id = self._config.private_fallback_route_id
        if fallback_route_id is None:
            return None
        fallback = self._states[fallback_route_id]
        if self._is_available_locked(fallback, now):
            return RouteSelection(state=fallback, is_fallback=True)
        return None

    @staticmethod
    def _exchange_allows_locked(state: RouteState, exchange: str) -> bool:
        """Return True if ``state``'s ``allowed_exchanges`` permits ``exchange``.

        Empty ``allowed_exchanges`` (the back-compatible default) means
        the route serves any exchange. Non-empty means preferred exchanges.

        Pool lock MUST be held (called only from ``_pick_locked``).
        """
        allowed = state.config.allowed_exchanges
        return not allowed or exchange in allowed

    def _fallback_direct_locked(self) -> RouteState | None:
        """Return the first enabled direct route, even if quarantined.

        Pool lock MUST be held.
        """
        for state in self._states.values():
            if state.enabled and self._is_private_direct(state):
                return state
        return None


_POOL_HOLDER: list[EgressPool | None] = [None]
"""Single-cell list holding the module-level singleton.

A list-of-one is used (rather than a bare module global) so the
mutator helpers ``configure_egress_pool`` / ``reset_egress_pool`` do
not need a ``global`` statement (ruff PLW0603 forbids ``global``).
The list is mutated in place; the contained ``EgressPool | None``
identity is the source of truth read by ``get_egress_pool``.
"""


_EMPTY_PUBLIC_POOL = EgressPool(EgressPoolConfig())
"""Refuse public reservations and track waiting when no pool is configured."""

_UNPOOLED_PUBLIC_HOLDER: list[EgressPool] = [_EMPTY_PUBLIC_POOL]
"""Process-local public routing when there is no configured VPN pool."""


def initialize_public_egress_policy(settings_service: SettingsService) -> bool:
    """Load the dev-only opt-in independently of the configured VPN pool."""
    allowed = settings_service.get_setting("public_direct_allowed") is True
    _UNPOOLED_PUBLIC_HOLDER[0] = (
        EgressPool(EgressPoolConfig(), public_direct_allowed=True)
        if allowed
        else _EMPTY_PUBLIC_POOL
    )
    if allowed:
        logger.warning(
            "egress_pool: dev-only public_direct_allowed=true. "
            "public market data may leave from the host's own IP"
        )
    return allowed


def public_egress_pool(pool: EgressPool | None) -> EgressPool:
    """Resolve the configured pool or the process's unpooled public policy."""
    return pool if pool is not None else _UNPOOLED_PUBLIC_HOLDER[0]


def configure_egress_pool(
    config: EgressPoolConfig,
    *,
    initial_quarantine_route_ids: Sequence[str] = (),
    public_direct_allowed: bool = False,
) -> EgressPool | None:
    """Install the module-level singleton from a validated config.

    Idempotent. If ``config.enabled is False``, clears the singleton
    and returns ``None``. Otherwise replaces any existing singleton
    with a fresh ``EgressPool``.

    Args:
        config: The validated config to install.
        initial_quarantine_route_ids: Enabled routes with transient preflight
            failures. Their retry deadlines start when the pool is installed.
        public_direct_allowed: Whether public traffic can use the dev fallback.

    Returns:
        The new ``EgressPool`` instance, or ``None`` when disabled.
    """
    if not config.enabled:
        _POOL_HOLDER[0] = None
        logger.info("egress_pool: disabled")
        return None
    pool = EgressPool(config, public_direct_allowed=public_direct_allowed)
    deadline = datetime.now(UTC) + timedelta(seconds=_PREFLIGHT_QUARANTINE_S)
    for route_id in initial_quarantine_route_ids:
        pool._quarantine_route(route_id, deadline, "preflight-connect-error")
    _POOL_HOLDER[0] = pool
    logger.info(
        "egress_pool: configured with {} route(s), on_all_quarantined={}",
        len(config.routes),
        config.on_all_quarantined,
    )
    return pool


def get_egress_pool() -> EgressPool | None:
    """Return the configured pool singleton, or ``None`` if not set.

    Returns:
        The ``EgressPool`` instance previously installed by
        ``configure_egress_pool`` / ``initialize_egress_pool``, or
        ``None`` if no pool is configured (i.e. the setting is
        absent, malformed, or has ``enabled=False``).
    """
    return _POOL_HOLDER[0]


def reset_egress_pool() -> None:
    """Clear the singleton. Test fixture only."""
    _POOL_HOLDER[0] = None
    _UNPOOLED_PUBLIC_HOLDER[0] = _EMPTY_PUBLIC_POOL
    _PYTHON_SOCKS_WARNING_LOGGED[0] = False


async def initialize_egress_pool(
    settings_service: SettingsService,
) -> EgressPool | None:
    """Async preflight + configure the pool from the ``egress_pool`` setting.

    Validate the cached setting and probe SOCKS5 routes asynchronously.
    Missing dependencies and unusable proxy URLs disable affected routes.
    DNS, TCP, and SOCKS5 greeting failures seed an initial quarantine so
    callers can retry after the deadline using the installed pool.

    Args:
        settings_service: The initialized ``SettingsService`` whose
            cache holds the ``egress_pool`` setting.

    Returns:
        The new ``EgressPool`` instance, or ``None`` for an absent, invalid,
        or disabled setting.
    """
    public_direct_allowed = initialize_public_egress_policy(settings_service)
    raw = settings_service.get_setting("egress_pool")
    if raw is None:
        logger.info("egress_pool: setting not present — disabled")
        return None
    parsed = _parse_egress_pool_setting(raw)
    if parsed is None:
        return None
    try:
        config = EgressPoolConfig.model_validate(parsed)
    except (ValueError, TypeError) as exc:
        logger.error(
            "egress_pool: config validation failed — disabling. error={}",
            exc,
        )
        return None
    effective, quarantine_route_ids = await _preflight_routes(config)
    return configure_egress_pool(
        effective,
        initial_quarantine_route_ids=quarantine_route_ids,
        public_direct_allowed=public_direct_allowed,
    )


async def safely_initialize_egress_pool(settings_service: SettingsService) -> None:
    """Run the egress-pool preflight without crashing the caller on failure.

    The API lifespan and publisher startup call this once per process.
    The normal initializer handles configuration errors and transient
    connectivity failures. This wrapper logs unexpected failures and
    preserves a previously installed pool.

    Args:
        settings_service: Initialized SettingsService whose cache holds the
            ``egress_pool`` setting.

    Returns:
        None.
    """
    try:
        await initialize_egress_pool(settings_service)
    except Exception:
        logger.exception("egress_pool: preflight failed; no VPN routes installed")


def _parse_egress_pool_setting(
    raw: object,
) -> dict[str, object] | None:
    """Parse the cached setting value to a dict, or ``None`` on error.

    Accepts dict (already-parsed JsonValue) and string (raw JSON) for
    forward compatibility. Logs and returns ``None`` on any parse
    error.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error(
                "egress_pool: setting JSON parse failed — disabling. error={}",
                exc,
            )
            return None
        if not isinstance(value, dict):
            logger.error("egress_pool: setting JSON did not decode to dict — disabling")
            return None
        return value
    logger.error(
        "egress_pool: setting value is unexpected type {!r} — disabling",
        type(raw).__name__,
    )
    return None


async def _preflight_routes(config: EgressPoolConfig) -> tuple[EgressPoolConfig, list[str]]:
    """Return the effective config and route IDs with transient preflight failures."""
    python_socks_ok = importlib.util.find_spec("python_socks") is not None
    if not python_socks_ok and not _PYTHON_SOCKS_WARNING_LOGGED[0]:
        any_socks = any(r.kind == "socks5" for r in config.routes)
        if any_socks:
            logger.warning(
                "egress_pool: python-socks not installed — all socks5 routes will be auto-disabled"
            )
            _PYTHON_SOCKS_WARNING_LOGGED[0] = True
    new_routes: list[RouteConfig] = []
    quarantine_route_ids: list[str] = []
    loop = asyncio.get_running_loop()
    for route in config.routes:
        disposition = await _preflight_one_route(loop, route, python_socks_ok)
        new_route = route
        if disposition == "disable":
            new_route = route.model_copy(update={"enabled": False})
        elif disposition == "quarantine":
            quarantine_route_ids.append(route.id)
        new_routes.append(new_route)
    effective = EgressPoolConfig(
        enabled=config.enabled,
        on_all_quarantined=config.on_all_quarantined,
        private_fallback_route_id=config.private_fallback_route_id,
        routes=new_routes,
    )
    return effective, quarantine_route_ids


_SOCKS5_PROBE_TIMEOUT_S: Final[float] = 2.0
"""Timeout for each DNS, TCP, and SOCKS5 preflight operation."""

_SOCKS5_PROBE_GREETING: Final[bytes] = b"\x05\x01\x00"
"""SOCKS5 greeting: version=5, nmethods=1, methods=[NO_AUTH]."""

_SOCKS5_PROBE_EXPECTED_REPLY: Final[bytes] = b"\x05\x00"
"""Expected SOCKS5 greeting reply: version=5, method=NO_AUTH selected."""


async def _preflight_one_route(
    loop: asyncio.AbstractEventLoop,
    route: RouteConfig,
    python_socks_ok: bool,
) -> _PreflightDisposition:
    """Classify one route as unchanged, permanently disabled, or retryable."""
    if route.kind == "direct":
        return "keep"
    if not route.enabled:
        return "keep"
    if not python_socks_ok:
        return "disable"
    if route.proxy_url is None:
        return "disable"
    host = _extract_proxy_host(route.proxy_url)
    if host is None:
        logger.warning(
            "egress_pool: route '{}' proxy_url has no parseable host — auto-disabling",
            route.id,
        )
        return "disable"
    port = _extract_proxy_port(route.proxy_url)
    if port is None:
        logger.warning(
            "egress_pool: route '{}' proxy_url has no parseable port — auto-disabling",
            route.id,
        )
        return "disable"
    try:
        await asyncio.wait_for(loop.getaddrinfo(host, None), timeout=_SOCKS5_PROBE_TIMEOUT_S)
    except OSError as exc:
        logger.warning(
            "egress_pool: route '{}' DNS lookup of {} failed — quarantining. error={}",
            route.id,
            host,
            exc,
        )
        return "quarantine"
    socks5_reachable = await _probe_socks5_greeting(host, port, route.id)
    return "keep" if socks5_reachable else "quarantine"


async def _probe_socks5_greeting(host: str, port: int, route_id: str) -> bool:
    r"""Verify a SOCKS5 listener actually speaks SOCKS5 with NO_AUTH.

    Opens a TCP socket to ``(host, port)`` with a short timeout, sends
    the canonical SOCKS5 greeting (``\x05\x01\x00`` — version 5, one
    method offered: NO_AUTH), and waits for the matching reply
    (``\x05\x00`` — version 5, NO_AUTH selected). Any other reply or
    any IO failure means the listener is either absent, broken, or
    not SOCKS5-NO_AUTH.

    Returns ``True`` on success, ``False`` on any failure (with a
    logged warning identifying the route).
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=_SOCKS5_PROBE_TIMEOUT_S,
        )
    except OSError as exc:
        logger.warning(
            "egress_pool: route '{}' SOCKS5 listener unreachable at {}:{} — quarantining. error={}",
            route_id,
            host,
            port,
            exc,
        )
        return False
    try:
        try:
            writer.write(_SOCKS5_PROBE_GREETING)
            await asyncio.wait_for(writer.drain(), timeout=_SOCKS5_PROBE_TIMEOUT_S)
            reply = await asyncio.wait_for(reader.readexactly(2), timeout=_SOCKS5_PROBE_TIMEOUT_S)
        except (OSError, asyncio.IncompleteReadError) as exc:
            logger.warning(
                "egress_pool: route '{}' SOCKS5 greeting failed at {}:{} — quarantining. error={}",
                route_id,
                host,
                port,
                exc,
            )
            return False
        if reply != _SOCKS5_PROBE_EXPECTED_REPLY:
            logger.warning(
                "egress_pool: route '{}' SOCKS5 listener at {}:{} returned "
                "unexpected greeting {!r} — quarantining",
                route_id,
                host,
                port,
                reply,
            )
            return False
        return True
    finally:
        writer.close()
        with contextlib.suppress(OSError, ConnectionError):
            await writer.wait_closed()


def _extract_proxy_port(proxy_url: str) -> int | None:
    """Extract the port from a ``socks5h://host:port`` URL.

    Returns ``None`` if the URL has no explicit port. We require an
    explicit port because the sidecar's SOCKS5 listeners are bound
    to per-tunnel port numbers; defaulting to anything would mask
    operator misconfiguration.
    """
    try:
        parts = urlsplit(proxy_url)
    except ValueError:
        return None
    try:
        port = parts.port
    except ValueError:
        return None
    return port


def _extract_proxy_host(proxy_url: str) -> str | None:
    """Extract the host portion from a ``socks5h://host:port`` URL.

    Returns ``None`` on parse failure. Uses ``urllib.parse.urlsplit``
    so authentication credentials and IPv6 brackets are handled
    consistently with the rest of the codebase.
    """
    try:
        parts = urlsplit(proxy_url)
    except ValueError:
        return None
    host = parts.hostname
    if host is None or host == "":
        return None
    return host
