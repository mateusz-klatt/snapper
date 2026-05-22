"""Process-wide registry of egress routes with quarantine state.

Phase B' of plan_2026_05_21_phase_b_prime_egress_multiplexer. The
pool is a singleton populated by ``configure_egress_pool`` during
startup; ``get_egress_pool`` returns the configured instance or
``None``.

DNS resolution and ``python-socks`` availability checks are done
asynchronously in ``initialize_egress_pool``, which is invoked from
the FastAPI lifespan BEFORE publishers come up. The pool itself
performs no I/O on construction — that work lives in the
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
import threading
from datetime import UTC
from datetime import datetime
from typing import Final
from typing import Literal
from urllib.parse import urlsplit

from loguru import logger

from snapper.application.services.settings import SettingsService
from snapper.infrastructure.network.egress_exceptions import AllRoutesQuarantinedError
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_models import RouteSelection
from snapper.infrastructure.network.egress_models import RouteSnapshot
from snapper.infrastructure.network.egress_models import RouteState
from snapper.infrastructure.network.egress_reservation import EgressPoolBase
from snapper.infrastructure.network.egress_reservation import EgressReservation
from snapper.infrastructure.network.egress_reservation import QuarantineReason

_PYTHON_SOCKS_WARNING_LOGGED: list[bool] = [False]
"""One-shot flag to throttle the ``python-socks not installed`` warning."""


class EgressPool(EgressPoolBase):
    """Process-wide registry of egress routes with quarantine state.

    Not instantiated directly outside tests — use
    ``configure_egress_pool(config)`` or ``initialize_egress_pool``.

    The pool guarantees ``in_use_count`` never goes negative even
    under multi-fire scenarios (explicit release racing the weakref
    finalizer). Quarantine deadlines are extend-only — never
    shortened by a subsequent shorter retry_after.
    """

    def __init__(self, config: EgressPoolConfig) -> None:
        """Build the pool from a validated config.

        Args:
            config: The top-level ``EgressPoolConfig`` payload, already
                validated by Pydantic. ``initialize_egress_pool`` may
                have set ``RouteConfig.enabled=False`` on routes that
                failed preflight; the caller is responsible for
                producing the final config object.
        """
        self._config = config
        self._lock = threading.RLock()
        self._states: dict[str, RouteState] = {
            route.id: RouteState(config=route, enabled=route.enabled) for route in config.routes
        }

    def size(self) -> int:
        """Total number of enabled routes (quarantined or not).

        Returns:
            Count of routes with ``enabled=True``.
        """
        with self._lock:
            return sum(1 for s in self._states.values() if s.enabled)

    def has_available(self, exchange: str | None = None) -> bool:
        """Return True if any route is enabled, not quarantined, and serves ``exchange``.

        Args:
            exchange: When set, only consider routes whose
                ``allowed_exchanges`` is empty or contains this name.
                When ``None`` (legacy callers / observability code),
                every enabled+healthy route counts regardless of
                ``allowed_exchanges``. Phase B'.5 callers that gate
                retry timing on per-exchange availability (e.g. the
                Kraken connect shim's ``_patched_get_reconnect_wait``)
                MUST pass the exchange so a Walutomat-pinned route
                cannot falsely look healthy to Kraken.

        Returns:
            ``True`` when at least one matching route is enabled and
            either never quarantined or its quarantine deadline is
            already in the past. ``False`` when every matching route
            is still inside its quarantine window — or when no route
            serves ``exchange`` at all.
        """
        now = datetime.now(UTC)
        with self._lock:
            return any(
                self._is_available_locked(s, now)
                and (exchange is None or self._exchange_allows_locked(s, exchange))
                for s in self._states.values()
            )

    def earliest_release_in_seconds(self, exchange: str | None = None) -> float | None:
        """Minimum seconds until any quarantine deadline expires.

        Args:
            exchange: When set, only consider routes whose
                ``allowed_exchanges`` is empty or contains this name.
                Mirrors :meth:`has_available` — callers gating retry
                wait on per-exchange release time MUST pass the
                exchange so the deadline reflects an
                exchange-eligible release, not a release for a
                differently-pinned route.

        Returns:
            ``None`` if no matching route is currently quarantined.
            ``0.0`` if the earliest matching deadline is already in
            the past. Otherwise the positive number of seconds until
            release.
        """
        now = datetime.now(UTC)
        with self._lock:
            deadlines = [
                s.quarantine_until
                for s in self._states.values()
                if s.enabled
                and s.quarantine_until is not None
                and (exchange is None or self._exchange_allows_locked(s, exchange))
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
    ) -> EgressReservation:
        """Pick the best route and return a borrowed handle.

        Selection policy:

        * If ``preferred_route`` is supplied AND that route is
          available, pick it.
        * Otherwise pick the lowest ``(priority, in_use_count)``
          tuple among available routes.
        * If no route is available:
            - ``on_all_quarantined == "wait"`` (default): fall back
              to the first enabled direct route (always exists per
              ``EgressPoolConfig`` validation). The selection's
              ``is_fallback`` flag is set so the patched
              ``__get_reconnect_wait`` knows to translate this into
              a single long sleep via
              ``earliest_release_in_seconds()``.
            - ``on_all_quarantined == "raise"``: raise
              ``AllRoutesQuarantinedError``.

        Args:
            exchange: The exchange name (informational; not used by
                v1 selection but reserved for future per-exchange
                policy).
            purpose: ``"websocket"`` or ``"http"`` (informational in
                v1).
            preferred_route: Optional route id hint.

        Returns:
            A fresh ``EgressReservation`` with the route's
            ``in_use_count`` already incremented.

        Raises:
            AllRoutesQuarantinedError: When no route is available
                and ``on_all_quarantined == "raise"``.
        """
        now = datetime.now(UTC)
        with self._lock:
            selection = self._pick_locked(preferred_route, exchange, now)
            if selection is None:
                if self._config.on_all_quarantined == "raise":
                    raise AllRoutesQuarantinedError(
                        f"all egress routes quarantined "
                        f"(exchange={exchange}, purpose={purpose})"
                    )
                fallback = self._fallback_direct_locked()
                if fallback is None:
                    raise AllRoutesQuarantinedError(
                        f"all egress routes quarantined and no direct "
                        f"fallback available "
                        f"(exchange={exchange}, purpose={purpose})"
                    )
                selection = RouteSelection(state=fallback, is_fallback=True)
            selection.state.in_use_count += 1
            selection.state.last_pick_at = now
        return EgressReservation(
            pool=self,
            route_id=selection.state.config.id,
            proxy_url=selection.state.config.proxy_url,
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

    def _decrement_in_use(self, route_id: str) -> None:
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

    def _quarantine_route(
        self,
        route_id: str,
        deadline: datetime,
        reason: QuarantineReason,
    ) -> None:
        """Mark a route quarantined until ``deadline``. Extend-only."""
        with self._lock:
            state = self._states.get(route_id)
            if state is None:
                return
            existing = state.quarantine_until
            if existing is None or deadline > existing:
                state.quarantine_until = deadline
            if reason == "http-429":
                state.last_handshake_429_at = datetime.now(UTC)
            else:
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

    def _pick_locked(
        self,
        preferred_route: str | None,
        exchange: str,
        now: datetime,
    ) -> RouteSelection | None:
        """Return the best available route for this exchange or None.

        Filters by ``allowed_exchanges`` so routes pinned to specific
        exchanges (e.g. ``["walutomat"]``) are skipped when the caller
        is reserving for a different exchange. ``allowed_exchanges=()``
        means the route serves any exchange (back-compatible default).

        Pool lock MUST be held.
        """
        if preferred_route is not None:
            preferred = self._states.get(preferred_route)
            if (
                preferred is not None
                and self._is_available_locked(preferred, now)
                and self._exchange_allows_locked(preferred, exchange)
            ):
                return RouteSelection(state=preferred, is_fallback=False)
        available = [
            s
            for s in self._states.values()
            if self._is_available_locked(s, now) and self._exchange_allows_locked(s, exchange)
        ]
        if not available:
            return None
        available.sort(key=lambda s: (s.config.priority, s.in_use_count))
        return RouteSelection(state=available[0], is_fallback=False)

    @staticmethod
    def _exchange_allows_locked(state: RouteState, exchange: str) -> bool:
        """Return True if ``state``'s ``allowed_exchanges`` permits ``exchange``.

        Empty ``allowed_exchanges`` (the back-compatible default) means
        the route serves any exchange. Non-empty means strict allow-list.

        Pool lock MUST be held (called only from ``_pick_locked``).
        """
        allowed = state.config.allowed_exchanges
        return not allowed or exchange in allowed

    def _fallback_direct_locked(self) -> RouteState | None:
        """Return the first enabled direct route, even if quarantined.

        Pool lock MUST be held.
        """
        for state in self._states.values():
            if state.enabled and state.config.kind == "direct":
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


def configure_egress_pool(config: EgressPoolConfig) -> EgressPool | None:
    """Install the module-level singleton from a validated config.

    Idempotent. If ``config.enabled is False``, clears the singleton
    and returns ``None``. Otherwise replaces any existing singleton
    with a fresh ``EgressPool``.

    Args:
        config: The validated config to install.

    Returns:
        The new ``EgressPool`` instance, or ``None`` when disabled.
    """
    if not config.enabled:
        _POOL_HOLDER[0] = None
        logger.info("egress_pool: disabled")
        return None
    pool = EgressPool(config)
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
    _PYTHON_SOCKS_WARNING_LOGGED[0] = False


async def initialize_egress_pool(
    settings_service: SettingsService,
) -> EgressPool | None:
    """Async preflight + configure the pool from the ``egress_pool`` setting.

    Invoked from the FastAPI lifespan AFTER ``_initialize_settings_service``
    and BEFORE publishers come up. The preflight runs DNS lookups
    asynchronously so the event loop is not blocked.

    The preflight:

    1. Reads the setting from the sync cache via
       ``settings_service.get_setting("egress_pool")``.
    2. Parses ``dict | str | None`` defensively (``JsonValue`` may be
       either depending on how the row was loaded).
    3. Validates via ``EgressPoolConfig.model_validate``. On any
       parse or validation error, logs + returns ``None``
       (disabled-equivalent).
    4. For each SOCKS5 route, awaits
       ``loop.getaddrinfo(host, None)``. Routes that fail DNS or
       have no ``proxy_url`` (defensive) are auto-disabled in the
       effective config copy.
    5. Checks ``importlib.util.find_spec("python_socks")``; when
       missing AND any SOCKS5 route is present, those routes are
       auto-disabled with a single throttled warning.
    6. Calls ``configure_egress_pool(effective_config)`` to install
       the singleton.

    Args:
        settings_service: The initialized ``SettingsService`` whose
            cache holds the ``egress_pool`` setting.

    Returns:
        The new ``EgressPool`` instance, or ``None`` if disabled /
        malformed / preflight reduced the config to a no-op.
    """
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
    effective = await _preflight_routes(config)
    return configure_egress_pool(effective)


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


async def _preflight_routes(config: EgressPoolConfig) -> EgressPoolConfig:
    """Validate SOCKS5 routes asynchronously and return an effective config.

    Per-route checks:

    * If the route is SOCKS5 and ``python-socks`` is missing, disable.
    * If the route is SOCKS5 and DNS resolution of the proxy host
      fails, disable.

    Returns a NEW config object (does not mutate the input) with
    ``RouteConfig.enabled`` overridden to ``False`` on routes that
    failed preflight.
    """
    python_socks_ok = importlib.util.find_spec("python_socks") is not None
    if not python_socks_ok and not _PYTHON_SOCKS_WARNING_LOGGED[0]:
        any_socks = any(r.kind == "socks5" for r in config.routes)
        if any_socks:
            logger.warning(
                "egress_pool: python-socks not installed — "
                "all socks5 routes will be auto-disabled"
            )
            _PYTHON_SOCKS_WARNING_LOGGED[0] = True
    new_routes: list[RouteConfig] = []
    loop = asyncio.get_running_loop()
    for route in config.routes:
        new_route = await _preflight_one_route(loop, route, python_socks_ok)
        new_routes.append(new_route)
    return EgressPoolConfig(
        enabled=config.enabled,
        on_all_quarantined=config.on_all_quarantined,
        routes=new_routes,
    )


_SOCKS5_PROBE_TIMEOUT_S: Final[float] = 2.0
"""Per-route TCP+SOCKS5 probe timeout used during startup preflight."""

_SOCKS5_PROBE_GREETING: Final[bytes] = b"\x05\x01\x00"
"""SOCKS5 greeting: version=5, nmethods=1, methods=[NO_AUTH]."""

_SOCKS5_PROBE_EXPECTED_REPLY: Final[bytes] = b"\x05\x00"
"""Expected SOCKS5 greeting reply: version=5, method=NO_AUTH selected."""


async def _preflight_one_route(
    loop: asyncio.AbstractEventLoop,
    route: RouteConfig,
    python_socks_ok: bool,
) -> RouteConfig:
    """Run async DNS + python-socks + SOCKS5-greeting checks for one route.

    The SOCKS5 greeting probe (added in v4 of the egress sidecar plan)
    closes a gap where the sidecar container was healthy but a tunnel's
    listener was absent — DNS would resolve, the pool would admit the
    route, and the shim would keep picking the broken route because
    generic proxy-connection failures do not quarantine.

    Returns a new ``RouteConfig`` with ``enabled=False`` if the route
    failed preflight; otherwise the input route (frozen Pydantic
    model — return identity is safe).
    """
    if route.kind == "direct":
        return route
    if not route.enabled:
        return route
    if not python_socks_ok:
        return route.model_copy(update={"enabled": False})
    if route.proxy_url is None:
        return route.model_copy(update={"enabled": False})
    host = _extract_proxy_host(route.proxy_url)
    if host is None:
        logger.warning(
            "egress_pool: route '{}' proxy_url has no parseable host — auto-disabling",
            route.id,
        )
        return route.model_copy(update={"enabled": False})
    port = _extract_proxy_port(route.proxy_url)
    if port is None:
        logger.warning(
            "egress_pool: route '{}' proxy_url has no parseable port — auto-disabling",
            route.id,
        )
        return route.model_copy(update={"enabled": False})
    try:
        await loop.getaddrinfo(host, None)
    except OSError as exc:
        logger.warning(
            "egress_pool: route '{}' DNS lookup of {} failed — auto-disabling. error={}",
            route.id,
            host,
            exc,
        )
        return route.model_copy(update={"enabled": False})
    socks5_reachable = await _probe_socks5_greeting(host, port, route.id)
    if not socks5_reachable:
        return route.model_copy(update={"enabled": False})
    return route


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
    except (TimeoutError, OSError) as exc:
        logger.warning(
            "egress_pool: route '{}' SOCKS5 listener unreachable at {}:{} "
            "— auto-disabling. error={}",
            route_id,
            host,
            port,
            exc,
        )
        return False
    try:
        writer.write(_SOCKS5_PROBE_GREETING)
        try:
            await asyncio.wait_for(writer.drain(), timeout=_SOCKS5_PROBE_TIMEOUT_S)
            reply = await asyncio.wait_for(reader.readexactly(2), timeout=_SOCKS5_PROBE_TIMEOUT_S)
        except (TimeoutError, OSError, asyncio.IncompleteReadError) as exc:
            logger.warning(
                "egress_pool: route '{}' SOCKS5 greeting failed at {}:{} "
                "— auto-disabling. error={}",
                route_id,
                host,
                port,
                exc,
            )
            return False
        if reply != _SOCKS5_PROBE_EXPECTED_REPLY:
            logger.warning(
                "egress_pool: route '{}' SOCKS5 listener at {}:{} returned "
                "unexpected greeting {!r} — auto-disabling",
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
