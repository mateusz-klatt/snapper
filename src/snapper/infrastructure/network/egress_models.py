"""Pydantic + dataclass models for the egress route registry.

Phase B' of plan_2026_05_21_phase_b_prime_egress_multiplexer. Three
shapes live here:

* ``RouteConfig`` — frozen Pydantic model for one egress route as
  declared in the ``egress_pool`` setting. Validates that ``direct``
  routes carry no ``proxy_url`` and ``socks5`` routes carry exactly
  one ``socks5h://`` URL (the ``socks5://`` variant is rejected to
  avoid DNS leaking through the local resolver).
* ``RouteState`` — mutable per-route runtime state owned by the
  ``EgressPool`` and guarded by the pool's internal lock.
* ``EgressPoolConfig`` — top-level setting payload combining
  ``enabled``, ``on_all_quarantined`` policy, and the route list.
"""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator


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
    priority: int = 0
    enabled: bool = True

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
            which would auto-detect ``HTTPS_PROXY`` env vars — Phase
            B' MUST NOT silently fall through to environment proxies)
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
            Phase A behaviour.
        on_all_quarantined: Policy when every route is quarantined.
            ``wait`` (default): pool returns the direct route so the
            patched ``__get_reconnect_wait`` translates the situation
            into a single long sleep until the earliest release.
            ``raise``: pool raises ``AllRoutesQuarantinedError`` so
            the Phase A.2 watchdog handles the storm.
        routes: List of declared routes. When ``enabled=True``, at
            least one ``direct`` route with ``enabled=True`` MUST be
            present so the pool always has a fallback.
    """

    model_config = ConfigDict(extra="forbid", strict=True)

    enabled: bool = False
    on_all_quarantined: Literal["wait", "raise"] = "wait"
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


@dataclass(frozen=True)
class RouteSelection:
    """Result of an internal ``_pick`` lookup.

    Fields:
        state: The ``RouteState`` chosen by the pool's selection
            policy. Callers MUST NOT mutate this directly; the pool
            increments ``in_use_count`` inside the same critical
            section that created the selection.
        is_fallback: ``True`` when the pool fell back to the direct
            route under ``on_all_quarantined="wait"`` because no
            healthy route was available.
    """

    state: RouteState
    is_fallback: bool = field(default=False)
