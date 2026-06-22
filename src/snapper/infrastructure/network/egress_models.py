"""Pydantic + dataclass models for the egress route registry.

Three shapes live here:

* ``RouteConfig`` — frozen Pydantic model for one egress route as
  declared in the ``egress_pool`` setting. Validates that ``direct``
  routes carry no ``proxy_url`` and ``socks5`` routes carry exactly
  one ``socks5h://`` URL (the ``socks5://`` variant is rejected to
  avoid DNS leaking through the local resolver).
* ``RouteState`` — mutable per-route runtime state owned by the
  ``EgressPool`` and guarded by the pool's internal lock.
* ``EgressPoolConfig`` — top-level setting payload combining
  ``enabled``, ``on_all_quarantined`` policy, the private fallback
  route id, and the route list.
* ``EgressPoolStatusSnapshot`` — read-only operator status projection
  returned by ``EgressPool.status_snapshot`` and exposed through the
  backend health route.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Annotated
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.types import ExchangeEnum


def _coerce_str_sequence_to_tuple(value: object) -> tuple[str, ...] | object:
    """Pydantic before-validator for ``tuple[str, ...]`` fed from JSON.

    Operators write ``egress_pool`` as JSON; ``json.loads`` produces
    Python ``list`` for JSON arrays. With ``ConfigDict(strict=True)``
    Pydantic would reject ``list`` for a ``tuple[str, ...]`` field.
    Coerces ``list[str]`` (or ``tuple[str, ...]``) into the expected
    ``tuple[str, ...]`` and lets anything else fall through so Pydantic
    emits its standard error. Mirrors the helper in
    ``egress_tunnel_models.py`` rather than importing it to keep the
    network-layer module graph acyclic (sibling files, no upward dep).
    """
    if isinstance(value, list | tuple) and all(isinstance(item, str) for item in value):
        return tuple(value)
    return value


StringSequence = Annotated[tuple[str, ...], BeforeValidator(_coerce_str_sequence_to_tuple)]
"""Type alias for route fields that accept JSON arrays of strings."""


class RouteConfig(BaseModel):
    """Frozen configuration for one egress route.

    Direct routes describe the host's default outbound interface and
    MUST carry ``proxy_url is None``. SOCKS5 routes name an
    application-layer proxy endpoint and MUST carry a non-empty
    ``proxy_url`` beginning with ``socks5h://``.

    The ``socks5://`` scheme (local DNS resolution) is rejected by
    the validator because it leaks the WS hostname through the
    container's resolver, bypassing the tunnel. ``socks5h://`` (with
    the trailing ``h``) sends the DNS query through the SOCKS server
    itself, keeping the lookup inside the tunnel.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    id: str = Field(min_length=1, max_length=64)
    kind: Literal["direct", "socks5"]
    proxy_url: str | None = None
    region: str | None = None
    exit_ip: str | None = None
    provider: str | None = None
    priority: int = 0
    enabled: bool = True
    allowed_exchanges: StringSequence = Field(
        default=(),
        description=(
            "Exchanges this route may serve. Empty tuple = any exchange "
            "(back-compatible default). Non-empty restricts the pool to "
            "this route only when ``reserve(exchange=...)`` is called with "
            "a matching value. The direct-fallback path in "
            "``EgressPool._fallback_direct_locked()`` IGNORES this "
            "constraint by design — direct egress is always the "
            "last-resort route. Operators who want per-exchange deny on "
            "direct should set ``enabled=false``, not ``allowed_exchanges``."
        ),
    )

    @model_validator(mode="after")
    def _check_proxy_url(self) -> Self:
        """Reject inconsistent ``(kind, proxy_url)`` tuples.

        Direct routes must have ``proxy_url is None``; SOCKS5 routes
        must have a non-empty ``socks5h://`` URL.
        """
        if self.kind == "direct" and self.proxy_url is not None:
            raise ValueError("direct route must not define proxy_url")
        if self.kind == "socks5":
            if self.proxy_url is None or self.proxy_url == "":
                raise ValueError("socks5 route requires proxy_url")
            if not self.proxy_url.startswith("socks5h://"):
                raise ValueError(
                    "socks5 route proxy_url must start with socks5h:// "
                    "(socks5:// is rejected to prevent DNS leakage)"
                )
        return self

    def websocket_proxy_kwarg(self) -> dict[str, str | None]:
        """Kwargs to merge into ``websockets.connect(...)``.

        Returns:
            ``{"proxy": None}`` for ``direct`` routes (explicitly
            overriding the websockets-16 default of ``proxy=True``
            which would auto-detect ``HTTPS_PROXY`` env vars — the
            pool MUST NOT silently fall through to environment proxies)
            and ``{"proxy": self.proxy_url}`` for ``socks5`` routes.
        """
        if self.kind == "direct":
            return {"proxy": None}
        return {"proxy": self.proxy_url}


@dataclass
class RouteState:
    """Mutable runtime state for one route.

    All mutation is guarded by ``EgressPool``'s internal lock; callers
    never touch this directly. The fields exist as a separate
    dataclass (not on ``RouteConfig`` itself) so the Pydantic config
    can stay frozen and import-time validation does not have to round-trip
    through ``model_copy``.

    Attributes:
        config: The immutable route config.
        enabled: Effective enabled flag. Mirrors ``config.enabled``
            unless overridden by preflight (e.g. DNS failure or
            missing ``python-socks``).
        quarantine_until: Deadline before which the route MUST NOT be
            picked. ``None`` means the route is healthy.
        in_use_count: Number of outstanding ``EgressReservation``
            handles. Clamped to ``>= 0`` by the pool.
        last_pick_at: Last time ``reserve`` returned this route.
            Informational only.
        last_handshake_429_at: Last time the route quarantined with
            ``reason="http-429"``. Informational only.
        last_close_1015_at: Last time the route quarantined with
            ``reason="close-1015"``. Informational only.
    """

    config: RouteConfig
    enabled: bool = True
    quarantine_until: datetime | None = None
    in_use_count: int = 0
    last_pick_at: datetime | None = None
    last_handshake_429_at: datetime | None = None
    last_close_1015_at: datetime | None = None


class EgressPoolConfig(BaseModel):
    """Top-level ``egress_pool`` setting payload.

    Attributes:
        enabled: Master toggle. ``False`` (default) skips pool
            wiring entirely — the connect shim short-circuits to
            direct-connection behaviour.
        on_all_quarantined: Policy when every route is quarantined.
            ``wait`` (default): pool returns the direct route so the
            patched ``__get_reconnect_wait`` translates the situation
            into a single long sleep until the earliest release.
            ``raise``: pool raises ``AllRoutesQuarantinedError`` so
            the reconnect watchdog handles the storm.
        private_fallback_route_id: Optional route id used when private
            executor traffic cannot use a healthy direct route. The
            route must be declared in ``routes`` when set.
        routes: List of declared routes. When ``enabled=True``, at
            least one ``direct`` route with ``enabled=True`` MUST be
            present so the pool always has a fallback.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = False
    on_all_quarantined: Literal["wait", "raise"] = "wait"
    private_fallback_route_id: str | None = None
    routes: list[RouteConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_direct_route_when_enabled(self) -> Self:
        """When enabled, require at least one enabled direct route.

        Pure-SOCKS5 configurations are rejected at the schema layer
        so the operator cannot ship a pool without a guaranteed
        fallback. Disabled pools may have any routes (or none).
        """
        if not self.enabled:
            return self
        has_direct = any(r.kind == "direct" and r.enabled for r in self.routes)
        if not has_direct:
            raise ValueError(
                "egress_pool with enabled=True requires at least one "
                "enabled direct route as fallback"
            )
        return self

    @model_validator(mode="after")
    def _check_private_fallback_route_exists(self) -> Self:
        """Reject a private fallback id that does not name a declared route.

        Returns:
            The validated config.

        Raises:
            ValueError: When ``private_fallback_route_id`` is set but no
                route with that id exists.
        """
        if self.private_fallback_route_id is None:
            return self
        route_ids = {route.id for route in self.routes}
        if self.private_fallback_route_id not in route_ids:
            raise ValueError(
                f"private_fallback_route_id {self.private_fallback_route_id!r} "
                "does not name a declared route"
            )
        return self

    @model_validator(mode="after")
    def _check_allowed_exchanges_membership(self) -> Self:
        """Reject typo'd exchange names in any route's allowed_exchanges.

        Every entry across every route's ``allowed_exchanges`` must
        match an ``ExchangeEnum`` *value* (e.g. ``"walutomat"``, not
        ``"WALUTOMAT"``). Catches operator typos like ``"krakeen"`` at
        config-load time instead of silently rendering a route
        unreachable (which would then quietly fall back to direct).
        """
        known = {member.value for member in ExchangeEnum}
        for route in self.routes:
            for name in route.allowed_exchanges:
                if name not in known:
                    raise ValueError(
                        f"route {route.id!r} declares unknown exchange "
                        f"{name!r} in allowed_exchanges; must be one of "
                        f"{sorted(known)}"
                    )
        return self


@dataclass(frozen=True)
class RouteSnapshot:
    """Read-only snapshot of a route's state for tests + observability.

    Returned by ``EgressPool.snapshot``. Mirrors ``RouteState``
    minus the mutable identity so callers cannot reach back into
    pool internals.
    """

    id: str
    kind: Literal["direct", "socks5"]
    proxy_url: str | None
    priority: int
    enabled: bool
    quarantine_until: datetime | None
    in_use_count: int
    last_handshake_429_at: datetime | None
    last_close_1015_at: datetime | None


class EgressActiveReservationSnapshot(BaseModel):
    """Currently reserved traffic tuple for one egress route.

    Attributes:
        exchange: Exchange name resolved by the egress context.
        traffic_class: Explicit traffic class used for selection.
        container: Process or container identity that reported the reservation.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    exchange: str
    traffic_class: Literal["public", "private"]
    container: str = ""


class EgressConnectionSnapshot(BaseModel):
    """Observed target host connection state for one egress route.

    Attributes:
        host: Lowercase target hostname only, without path, query,
            headers, body, or credential material.
        kind: Connection kind, WebSocket or REST.
        exchange: Exchange name resolved by the egress context.
        traffic_class: Explicit traffic class used for selection.
        container: Process or container identity that reported the connection.
        count: Currently open reservations for this host tuple.
        last_seen_at: Latest REST observation timestamp, or ``None``
            for live WebSocket counters.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    host: str
    kind: Literal["ws", "rest"]
    exchange: str
    traffic_class: Literal["public", "private"]
    container: str = ""
    count: int
    last_seen_at: datetime | None = None


class EgressRouteStatusSnapshot(BaseModel):
    """Operator status projection for one route.

    Attributes:
        id: Route id from the configured egress pool.
        kind: Route kind, either direct or socks5.
        region: Optional operator-provided region label.
        exit_ip: Optional operator-provided observed exit IP.
        provider: Optional operator-provided provider label.
        priority: Public-selection priority.
        allowed_exchanges: Exchanges this route may serve for public traffic.
        enabled: Effective route enabled flag after preflight.
        quarantined: True when the route is inside a quarantine window.
        quarantine_seconds_remaining: Seconds until quarantine release,
            ``None`` when the route has no quarantine deadline.
        in_use_count: Number of outstanding reservation handles.
        active_reservations: Unique exchange and traffic-class pairs
            currently reserved on the route.
        connections: Target host connection counters and REST last-seen rows.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    id: str
    kind: Literal["direct", "socks5"]
    region: str | None = None
    exit_ip: str | None = None
    provider: str | None = None
    priority: int
    allowed_exchanges: list[str] = Field(default_factory=list)
    enabled: bool
    quarantined: bool
    quarantine_seconds_remaining: float | None
    in_use_count: int
    active_reservations: list[EgressActiveReservationSnapshot] = Field(default_factory=list)
    connections: list[EgressConnectionSnapshot] = Field(default_factory=list)


class EgressPoolStatusSnapshot(BaseModel):
    """Operator status projection for the full egress pool.

    Attributes:
        enabled: True when the singleton pool is configured and active.
        on_all_quarantined: Pool policy, or ``None`` when disabled.
        private_fallback_route_id: Configured private fallback route id.
        private_on_fallback: True when any private reservation is active
            on a non-direct route.
        routes: Per-route status rows in configured order.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)

    enabled: bool
    on_all_quarantined: Literal["wait", "raise"] | None = None
    private_fallback_route_id: str | None = None
    private_on_fallback: bool = False
    routes: list[EgressRouteStatusSnapshot] = Field(default_factory=list)


@dataclass(frozen=True)
class RouteSelection:
    """Result of an internal ``_pick`` lookup.

    Fields:
        state: The ``RouteState`` chosen by the pool's selection
            policy. Callers MUST NOT mutate this directly; the pool
            increments ``in_use_count`` inside the same critical
            section that created the selection.
        is_fallback: ``True`` when the pool selected a configured
            fallback route or fell back to the direct route under
            ``on_all_quarantined="wait"`` because no healthy route was
            available.
    """

    state: RouteState
    is_fallback: bool = field(default=False)
