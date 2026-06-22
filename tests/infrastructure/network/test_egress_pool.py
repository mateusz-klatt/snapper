"""Unit tests for ``EgressPool`` selection, quarantine, and singleton state."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger

from snapper.infrastructure.network import egress_pool as pool_module
from snapper.infrastructure.network.egress_exceptions import AllRoutesQuarantinedError
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.infrastructure.network.egress_pool import _extract_proxy_host
from snapper.infrastructure.network.egress_pool import _extract_proxy_port
from snapper.infrastructure.network.egress_pool import _parse_egress_pool_setting
from snapper.infrastructure.network.egress_pool import _preflight_one_route
from snapper.infrastructure.network.egress_pool import _preflight_routes
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.infrastructure.network.egress_pool import initialize_egress_pool
from snapper.infrastructure.network.egress_pool import normalize_egress_target_host
from snapper.infrastructure.network.egress_pool import reset_egress_pool
from snapper.infrastructure.network.egress_pool import safely_initialize_egress_pool


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    """Reset module-level singleton state between tests."""
    reset_egress_pool()
    yield
    reset_egress_pool()


def _two_route_config(
    *,
    direct_priority: int = 0,
    socks_priority: int = 10,
    enabled: bool = True,
    on_all_quarantined: str = "wait",
) -> EgressPoolConfig:
    """Helper — build a config with one direct + one socks5 route."""
    return EgressPoolConfig(
        enabled=enabled,
        on_all_quarantined=on_all_quarantined,
        routes=[
            RouteConfig(id="default", kind="direct", priority=direct_priority),
            RouteConfig(
                id="wg-uk-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1081",
                priority=socks_priority,
            ),
        ],
    )


class TestSize:
    """Tests for ``EgressPool.size``."""

    def test_size_counts_enabled_routes(self) -> None:
        """Spec — size() counts routes with enabled=True.

        Given a pool with 2 enabled routes,
        When size() is called,
        Then it returns 2.
        """
        pool = EgressPool(_two_route_config())
        assert pool.size() == 2

    def test_size_excludes_disabled_routes(self) -> None:
        """Spec — size() ignores disabled routes.

        Given a pool with one enabled and one disabled route,
        When size() is called,
        Then it returns 1.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="d", kind="direct"),
                RouteConfig(
                    id="s",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    enabled=False,
                ),
            ],
        )
        pool = EgressPool(config)
        assert pool.size() == 1


class TestReserveSelection:
    """Tests for ``EgressPool.reserve`` selection policy."""

    def test_reserve_picks_lowest_priority(self) -> None:
        """Spec — lowest priority wins when all healthy.

        Given direct(priority=0) + socks5(priority=10), both healthy,
        When reserve is called with no preference,
        Then the direct route (priority=0) is picked.
        """
        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert reservation.route_id == "default"

    def test_reserve_skips_quarantined_route(self) -> None:
        """Spec — quarantined routes are skipped.

        Given direct(priority=0) quarantined + socks5(priority=10) healthy,
        When reserve is called,
        Then the socks5 route is picked.
        """
        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        reservation.quarantine(120.0, reason="http-429")
        reservation.release()
        next_reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert next_reservation.route_id == "wg-uk-1"

    def test_reserve_respects_preferred_route(self) -> None:
        """Spec — preferred_route overrides priority order.

        Given two healthy routes and preferred_route="wg-uk-1",
        When reserve is called,
        Then wg-uk-1 is picked even though direct has lower priority.
        """
        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="wg-uk-1",
        )
        assert reservation.route_id == "wg-uk-1"

    def test_reserve_falls_back_when_preferred_quarantined(self) -> None:
        """Spec — quarantined preferred route falls through to priority.

        Given a preferred socks5 route that is quarantined,
        When reserve is called,
        Then the direct route is picked.
        """
        pool = EgressPool(_two_route_config())
        first = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="wg-uk-1",
        )
        first.quarantine(60.0, reason="http-429")
        first.release()
        second = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="wg-uk-1",
        )
        assert second.route_id == "default"

    def test_reserve_ignores_unknown_preferred_route(self) -> None:
        """Spec — unknown preferred_route is ignored.

        Given preferred_route="nonexistent" and a healthy direct route,
        When reserve is called,
        Then the direct route is picked.
        """
        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="nonexistent",
        )
        assert reservation.route_id == "default"

    def test_reserve_balances_in_use_count_within_priority(self) -> None:
        """Spec — equal-priority routes are balanced by in_use_count.

        Given two direct routes with priority=0,
        When reserved three times in succession,
        Then the picks alternate so neither route holds more than half.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="d1", kind="direct", priority=0),
                RouteConfig(id="d2", kind="direct", priority=0),
            ],
        )
        pool = EgressPool(config)
        first = pool.reserve(exchange="kraken", purpose="websocket")
        second = pool.reserve(exchange="kraken", purpose="websocket")
        third = pool.reserve(exchange="kraken", purpose="websocket")
        ids = {first.route_id, second.route_id, third.route_id}
        assert ids == {"d1", "d2"}

    def test_reserve_records_last_pick_at(self) -> None:
        """Spec — pick updates RouteState.last_pick_at and increments in_use_count.

        Given a fresh pool,
        When reserve is called and the reservation is held,
        Then snapshot()[0].in_use_count is 1 (proves the increment ran).

        The reservation MUST be bound to a local — dropping it would
        let the weakref finalizer fire and decrement back to 0.
        """
        pool = EgressPool(_two_route_config())
        before = pool.snapshot()
        assert before[0].in_use_count == 0
        held = pool.reserve(exchange="kraken", purpose="websocket")
        after = pool.snapshot()
        assert after[0].in_use_count == 1
        held.release()


class TestReserveExchangeFilter:
    """Tests for per-exchange route selection via ``allowed_exchanges``."""

    @staticmethod
    def _pl_pinned_config() -> EgressPoolConfig:
        """Helper — direct fallback + IE/UK open + PL pinned to walutomat.

        Mirrors the per-exchange routing shape used in production.
        """
        return EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="ie",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    priority=10,
                ),
                RouteConfig(
                    id="uk",
                    kind="socks5",
                    proxy_url="socks5h://x:1082",
                    priority=10,
                ),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    priority=10,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )

    def test_pool_picks_pinned_route_when_only_match(self) -> None:
        """Spec — pl1 is the unique allowed route → it must be picked.

        Pool has direct(p=100) + pl(p=10, allowed=["walutomat"]).
        Reserve(walutomat): IE/UK absent, direct is higher priority,
        so pl MUST be the selection. Proves the filter actually
        admits pinned routes for their listed exchange (not merely
        that the filter fails to exclude pl).
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    priority=10,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )
        pool = EgressPool(config)
        reservation = pool.reserve(exchange="walutomat", purpose="websocket")
        assert reservation.route_id == "pl"

    def test_pool_skips_pinned_route_for_other_exchange(self) -> None:
        """Spec — pl route is NEVER selected when reserving for kraken.

        With pl pinned to ``allowed_exchanges=("walutomat",)`` and IE/UK
        open, repeated reserve(exchange="kraken") calls must rotate
        between IE and UK only — pl is filtered out before sorting.
        """
        pool = EgressPool(self._pl_pinned_config())
        picks: set[str] = set()
        for _ in range(6):
            r = pool.reserve(exchange="kraken", purpose="websocket")
            picks.add(r.route_id)
            r.release()
        assert "pl" not in picks
        assert picks.issubset({"ie", "uk"})

    def test_pool_falls_back_to_direct_when_only_pinned_route_disallows(self) -> None:
        """Spec — direct fallback ignores ``allowed_exchanges`` constraint.

        Given a pool whose only socks5 route pins itself to
        ``["walutomat"]`` and is then quarantined, reserving for
        ``"kraken"`` falls back to the direct route even though the
        direct route's ``allowed_exchanges`` is empty (any-exchange)
        — direct is always the last-resort path per the documented
        invariant.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    priority=10,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )
        pool = EgressPool(config)
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert reservation.route_id == "default"

    def test_preferred_route_respects_allowed_exchanges(self) -> None:
        """Spec — preferred_route pointing at a disallowing route is skipped.

        Even when the caller explicitly requests
        ``preferred_route="pl"``, if ``allowed_exchanges`` excludes
        the current exchange the selector skips it and falls through
        to normal priority+in_use_count picking.
        """
        pool = EgressPool(self._pl_pinned_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="pl",
        )
        assert reservation.route_id != "pl"
        assert reservation.route_id in {"default", "ie", "uk"}

    def test_open_route_serves_any_exchange(self) -> None:
        """Spec — default empty ``allowed_exchanges`` means any exchange.

        IE/UK routes with no ``allowed_exchanges`` field at all
        (empty tuple default) are eligible for any exchange. This is
        the back-compat path: existing prod seed entries continue to
        work without any migration.
        """
        pool = EgressPool(self._pl_pinned_config())
        for exchange in ("kraken", "kraken_futures", "polygon"):
            r = pool.reserve(exchange=exchange, purpose="websocket")
            assert r.route_id in {"ie", "uk"}
            r.release()


class TestPrivateTrafficSelection:
    """Tests for private direct-first routing and fallback semantics."""

    @staticmethod
    def _private_config(on_all_quarantined: str = "wait") -> EgressPoolConfig:
        """Build direct + public VPN + PL private fallback config.

        Args:
            on_all_quarantined: Pool policy to install.

        Returns:
            EgressPoolConfig used by private routing tests.
        """
        return EgressPoolConfig(
            enabled=True,
            on_all_quarantined=on_all_quarantined,
            private_fallback_route_id="pl",
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="ie",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    priority=10,
                ),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    priority=5,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )

    def test_public_selection_keeps_existing_allow_list_policy(self) -> None:
        """Spec — public traffic ignores the private fallback policy.

        Given: direct is lower priority than public VPN routes and PL is
            pinned to Walutomat,
        When: public Kraken traffic reserves a route,
        Then: it uses the existing public selector and picks the IE route.
        """
        pool = EgressPool(self._private_config())
        reservation = pool.reserve(exchange="kraken", purpose="websocket")
        assert reservation.route_id == "ie"
        reservation.release()

    def test_unknown_traffic_class_defaults_to_public(self) -> None:
        """Spec — unknown traffic classes are treated as public.

        Given: a private fallback config,
        When: a caller passes an unrecognized traffic class,
        Then: the public route is selected rather than private direct.
        """
        pool = EgressPool(self._private_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="mystery",
        )
        assert reservation.route_id == "ie"
        reservation.release()

    def test_private_selection_picks_direct_first(self) -> None:
        """Spec — private traffic uses direct before public VPN routes.

        Given: a healthy direct route and lower-priority SOCKS routes,
        When: private Kraken traffic reserves a route,
        Then: direct is selected regardless of public priority order.
        """
        pool = EgressPool(self._private_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        assert reservation.route_id == "default"
        assert reservation.proxy_url is None
        reservation.release()

    def test_private_selection_falls_back_to_pl_when_direct_quarantined(self) -> None:
        """Spec — private fallback bypasses the route allow-list.

        Given: direct is quarantined and PL is publicly pinned to Walutomat,
        When: private Kraken traffic reserves a route,
        Then: PL is selected even though public Kraken traffic could not use it.
        """
        pool = EgressPool(self._private_config())
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(120.0, reason="http-429")
        direct.release()

        fallback = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )

        assert fallback.route_id == "pl"
        assert fallback.proxy_url == "socks5h://x:1084"
        fallback.release()

    def test_private_wait_mode_uses_all_quarantined_direct_fallback(self) -> None:
        """Spec — private wait mode ignores healthy public-only routes.

        Given: direct and PL private fallback are quarantined while IE is healthy,
        When: private traffic reserves in wait mode,
        Then: the pool returns direct under the all-quarantined wait policy.
        """
        pool = EgressPool(self._private_config(on_all_quarantined="wait"))
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(120.0, reason="http-429")
        direct.release()
        pl = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        pl.quarantine(120.0, reason="http-429")
        pl.release()

        fallback = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )

        assert fallback.route_id == "default"
        fallback.release()

    def test_private_raise_mode_raises_when_direct_and_pl_unavailable(self) -> None:
        """Spec — private raise mode raises when private routes are unavailable.

        Given: direct and PL private fallback are quarantined while IE is healthy,
        When: private traffic reserves in raise mode,
        Then: AllRoutesQuarantinedError is raised.
        """
        pool = EgressPool(self._private_config(on_all_quarantined="raise"))
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(120.0, reason="http-429")
        direct.release()
        pl = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        pl.quarantine(120.0, reason="http-429")
        pl.release()

        with pytest.raises(AllRoutesQuarantinedError):
            pool.reserve(exchange="kraken", purpose="websocket", traffic_class="private")

    def test_private_has_available_and_earliest_release_use_private_routes(self) -> None:
        """Spec — private availability ignores public-only healthy routes.

        Given: direct and PL are quarantined while IE remains healthy,
        When: private availability and release time are queried,
        Then: availability is False and release time is based on private routes.
        """
        pool = EgressPool(self._private_config())
        assert pool.earliest_release_in_seconds(exchange="kraken", traffic_class="private") is None
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(30.0, reason="http-429")
        direct.release()
        pl = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        pl.quarantine(90.0, reason="http-429")
        pl.release()

        assert pool.has_available(exchange="kraken", traffic_class="private") is False
        assert pool.has_available(exchange="kraken", traffic_class="public") is True
        earliest = pool.earliest_release_in_seconds(exchange="kraken", traffic_class="private")
        assert earliest is not None
        assert 27.0 <= earliest <= 32.0

    def test_private_without_fallback_honors_wait_mode(self) -> None:
        """Spec — missing private fallback does not invent a private route.

        Given: no ``private_fallback_route_id`` is configured and direct is quarantined,
        When: private traffic reserves in wait mode,
        Then: the all-quarantined direct fallback is returned.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="ie",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    priority=10,
                ),
            ],
        )
        pool = EgressPool(config)
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(120.0, reason="http-429")
        direct.release()

        fallback = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )

        assert fallback.route_id == "default"
        fallback.release()

    def test_private_direct_reservation_ignores_quarantine_without_pl_fallback(self) -> None:
        """Spec — direct-only private reservation never selects the PL fallback.

        Given: direct is quarantined and PL is healthy,
        When: private direct-only REST traffic reserves a route,
        Then: the direct route is still selected and the active private
            reservation is recorded on direct, not PL.
        """
        pool = EgressPool(self._private_config())
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(120.0, reason="http-429")
        direct.release()

        reservation = pool.reserve_private_direct(exchange="kraken", purpose="http")
        snapshot = pool.status_snapshot()

        assert reservation.route_id == "default"
        assert [(r.exchange, r.traffic_class) for r in snapshot.routes[0].active_reservations] == [
            ("kraken", "private")
        ]
        assert snapshot.routes[2].active_reservations == []
        reservation.release()

    def test_private_direct_reservation_raises_without_enabled_direct_route(self) -> None:
        """Spec — direct-only private reservation fails when direct is absent.

        Given: a disabled pool object contains only a SOCKS5 route,
        When: private direct-only REST traffic reserves a route,
        Then: the pool raises instead of selecting a VPN fallback.
        """
        config = EgressPoolConfig(
            enabled=False,
            routes=[
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                ),
            ],
        )
        pool = EgressPool(config)

        with pytest.raises(AllRoutesQuarantinedError, match="no direct egress route"):
            pool.reserve_private_direct(exchange="kraken", purpose="http")

    def test_private_fallback_reservation_uses_configured_healthy_route_only(self) -> None:
        """Spec — fallback-only reservation selects PL only while it is healthy.

        Given: PL is configured as the private fallback,
        When: fallback-only REST traffic reserves a route and then PL is quarantined,
        Then: the first call returns PL and the second returns None.
        """
        pool = EgressPool(self._private_config())

        fallback = pool.reserve_private_fallback(exchange="kraken", purpose="http")
        assert fallback is not None
        assert fallback.route_id == "pl"
        assert fallback.proxy_url == "socks5h://x:1084"
        fallback.quarantine(120.0, reason="http-429")
        fallback.release()

        assert pool.reserve_private_fallback(exchange="kraken", purpose="http") is None

    def test_private_fallback_reservation_returns_none_when_unconfigured(self) -> None:
        """Spec — fallback-only reservation does not infer a fallback route.

        Given: a pool has direct and public VPN routes but no private fallback id,
        When: fallback-only REST traffic asks for a route,
        Then: None is returned instead of selecting the public VPN.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="ie",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    priority=10,
                ),
            ],
        )
        pool = EgressPool(config)

        assert pool.reserve_private_fallback(exchange="kraken", purpose="http") is None

    def test_selection_log_emitted_for_websocket_reserve(self) -> None:
        """Spec — reserve emits one structured WS selection log.

        Given: a private reserve call,
        When: a WebSocket route is selected,
        Then: the log line includes exchange, traffic class, route, fallback,
            proxy, pid, and host fields.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda message: messages.append(str(message)), level="INFO")
        try:
            pool = EgressPool(self._private_config())
            reservation = pool.reserve(
                exchange="kraken",
                purpose="websocket",
                traffic_class="private",
            )
            reservation.release()
        finally:
            logger.remove(sink_id)

        joined = "\n".join(messages)
        assert "egress_pool: selected route" in joined
        assert "exchange=kraken" in joined
        assert "traffic_class=private" in joined
        assert "purpose=websocket" in joined
        assert "route_id=default" in joined
        assert "route_kind=direct" in joined
        assert "is_fallback=False" in joined
        assert "proxy_url=direct" in joined
        assert "pid=" in joined
        assert "host=" in joined


class TestStatusSnapshot:
    """Tests for the operator egress pool status snapshot."""

    @staticmethod
    def _status_config() -> EgressPoolConfig:
        """Build a mixed route config for status snapshot tests.

        Returns:
            EgressPoolConfig with direct, IE, and PL routes.
        """
        return EgressPoolConfig(
            enabled=True,
            private_fallback_route_id="pl",
            routes=[
                RouteConfig(
                    id="default",
                    kind="direct",
                    priority=100,
                    region="host",
                    exit_ip="198.51.100.11",
                    provider="isp",
                ),
                RouteConfig(
                    id="ie",
                    kind="socks5",
                    proxy_url="socks5h://ie:1081",
                    priority=10,
                    allowed_exchanges=("kraken",),
                    region="ie-dub",
                    exit_ip="203.0.113.20",
                    provider="wireguard-ie",
                ),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://pl:1084",
                    priority=5,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )

    def test_status_snapshot_reports_mixed_routes_and_active_rows(self) -> None:
        """Spec — status snapshot exposes route metadata, health, and reservations.

        Given direct and SOCKS5 routes with mixed metadata and one
            quarantined route,
        When status_snapshot is called with active public/private holds,
        Then the snapshot includes pool policy, route metadata,
            quarantine state, and active reservation tuples.
        """
        pool = EgressPool(self._status_config())
        direct_private = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        pool._quarantine_route(
            "ie",
            datetime.now(UTC) + timedelta(seconds=60),
            "http-429",
        )
        pl_public = pool.reserve(exchange="walutomat", purpose="websocket")

        snapshot = pool.status_snapshot()

        assert snapshot.enabled is True
        assert snapshot.on_all_quarantined == "wait"
        assert snapshot.private_fallback_route_id == "pl"
        assert snapshot.private_on_fallback is False
        direct = snapshot.routes[0]
        ie = snapshot.routes[1]
        pl = snapshot.routes[2]
        assert direct.id == "default"
        assert direct.kind == "direct"
        assert direct.region == "host"
        assert direct.exit_ip == "198.51.100.11"
        assert direct.provider == "isp"
        assert direct.quarantined is False
        assert direct.quarantine_seconds_remaining is None
        assert direct.in_use_count == 1
        assert [(r.exchange, r.traffic_class) for r in direct.active_reservations] == [
            ("kraken", "private")
        ]
        assert ie.id == "ie"
        assert ie.kind == "socks5"
        assert ie.region == "ie-dub"
        assert ie.exit_ip == "203.0.113.20"
        assert ie.provider == "wireguard-ie"
        assert ie.allowed_exchanges == ["kraken"]
        assert ie.quarantined is True
        assert ie.quarantine_seconds_remaining is not None
        assert 55.0 <= ie.quarantine_seconds_remaining <= 60.0
        assert pl.id == "pl"
        assert pl.region is None
        assert pl.exit_ip is None
        assert pl.provider is None
        assert pl.allowed_exchanges == ["walutomat"]
        assert [(r.exchange, r.traffic_class) for r in pl.active_reservations] == [
            ("walutomat", "public")
        ]
        direct_private.release()
        pl_public.release()

    def test_active_reservation_map_counts_duplicate_tuples(self) -> None:
        """Spec — duplicate active tuples remain visible until all holds release.

        Given two outstanding reservations for the same route/exchange/class,
        When each reservation is released,
        Then in_use_count decrements each time while the active tuple
            remains until the last release.
        """
        pool = EgressPool(_two_route_config())
        first = pool.reserve(exchange="kraken", purpose="websocket")
        second = pool.reserve(exchange="kraken", purpose="websocket")

        held = pool.status_snapshot().routes[0]
        assert held.in_use_count == 2
        assert [(r.exchange, r.traffic_class) for r in held.active_reservations] == [
            ("kraken", "public")
        ]

        first.release()
        one_left = pool.status_snapshot().routes[0]
        assert one_left.in_use_count == 1
        assert [(r.exchange, r.traffic_class) for r in one_left.active_reservations] == [
            ("kraken", "public")
        ]

        second.release()
        empty = pool.status_snapshot().routes[0]
        assert empty.in_use_count == 0
        assert empty.active_reservations == []

    def test_private_on_fallback_true_when_private_hold_is_not_direct(self) -> None:
        """Spec — private_on_fallback tracks active private holds on fallback.

        Given direct is quarantined and PL is the private fallback,
        When private traffic reserves the fallback route,
        Then status_snapshot reports private_on_fallback=True until release.
        """
        pool = EgressPool(self._status_config())
        direct = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        direct.quarantine(120.0, reason="http-429")
        direct.release()

        fallback = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            traffic_class="private",
        )
        snapshot = pool.status_snapshot()

        assert fallback.route_id == "pl"
        assert snapshot.private_on_fallback is True
        pl = snapshot.routes[2]
        assert [(r.exchange, r.traffic_class) for r in pl.active_reservations] == [
            ("kraken", "private")
        ]

        fallback.release()
        assert pool.status_snapshot().private_on_fallback is False

    def test_http_reserve_updates_status_without_websocket_log_branch(self) -> None:
        """Spec — HTTP reservations update status through the non-WS branch.

        Given an HTTP route reservation,
        When status_snapshot is inspected,
        Then the active tuple is visible and release clears it.
        """
        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(exchange="kraken", purpose="http")

        held = pool.status_snapshot().routes[0]
        assert held.in_use_count == 1
        assert [(r.exchange, r.traffic_class) for r in held.active_reservations] == [
            ("kraken", "public")
        ]

        reservation.release()
        assert pool.status_snapshot().routes[0].active_reservations == []

    def test_websocket_connection_counts_group_by_target_host(self) -> None:
        """Spec — WebSocket reservations count live holds per target host.

        Given two WS reservations for the same route and hostname,
        When status_snapshot is inspected before and after releases,
        Then the connection count decrements with the reservation lifecycle.
        """
        pool = EgressPool(_two_route_config())
        first = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            target_host="WS.KRAKEN.COM",
            connection_kind="ws",
        )
        second = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            target_host="ws.kraken.com",
            connection_kind="ws",
        )

        held = pool.status_snapshot().routes[0].connections
        assert [
            (item.host, item.kind, item.exchange, item.traffic_class, item.count) for item in held
        ] == [("ws.kraken.com", "ws", "kraken", "public", 2)]
        assert held[0].last_seen_at is None

        first.release()
        one_left = pool.status_snapshot().routes[0].connections
        assert one_left[0].count == 1

        second.release()
        assert pool.status_snapshot().routes[0].connections == []

    def test_rest_connection_records_last_seen_after_release(self) -> None:
        """Spec — REST reservations leave a last-seen host row after release.

        Given a REST reservation for a target host,
        When it is released immediately after the call,
        Then the route keeps the host with count zero and a timestamp.
        """
        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="http",
            target_host="API.KRAKEN.COM",
            connection_kind="rest",
        )

        active = pool.status_snapshot().routes[0].connections
        assert len(active) == 1
        assert active[0].host == "api.kraken.com"
        assert active[0].kind == "rest"
        assert active[0].count == 1
        assert active[0].last_seen_at is not None
        last_seen_at = active[0].last_seen_at

        reservation.release()
        released = pool.status_snapshot().routes[0].connections
        assert len(released) == 1
        assert released[0].count == 0
        assert released[0].last_seen_at == last_seen_at

    def test_target_host_sanitization_rejects_paths_queries_and_controls(self) -> None:
        """Spec — host tracking accepts only lowercase hostnames.

        Given clean hosts and unsafe URL-like or control-character inputs,
        When host normalization and reservation tracking run,
        Then only the safe hostname is retained in snapshots.
        """
        assert normalize_egress_target_host(" API.KRAKEN.COM ") == "api.kraken.com"
        assert normalize_egress_target_host("") is None
        assert normalize_egress_target_host("api.kraken.com/private/Balance") is None
        assert normalize_egress_target_host("api.kraken.com?txid=order-1") is None
        assert normalize_egress_target_host("api.kraken.com\n") is None

        pool = EgressPool(_two_route_config())
        reservation = pool.reserve(
            exchange="kraken",
            purpose="http",
            target_host="api.kraken.com/private/Balance?txid=order-1",
            connection_kind="rest",
        )

        assert pool.status_snapshot().routes[0].connections == []
        reservation.release()

    def test_rest_last_seen_hosts_are_capped_per_route(self) -> None:
        """Spec — REST last-seen host rows are capped per route.

        Given more than sixteen distinct REST target hosts on one route,
        When each reservation is released,
        Then the oldest host is evicted and the newest sixteen remain.
        """
        pool = EgressPool(_two_route_config())
        for index in range(17):
            reservation = pool.reserve(
                exchange="kraken",
                purpose="http",
                target_host=f"api-{index}.kraken.com",
                connection_kind="rest",
            )
            reservation.release()

        hosts = [item.host for item in pool.status_snapshot().routes[0].connections]
        assert len(hosts) == 16
        assert "api-0.kraken.com" not in hosts
        assert "api-16.kraken.com" in hosts

    def test_status_snapshot_marks_expired_quarantine_as_not_quarantined(self) -> None:
        """Spec — expired quarantine deadlines report zero remaining seconds.

        Given a route whose quarantine_until is already in the past,
        When status_snapshot is called,
        Then quarantined is False and remaining seconds is 0.0.
        """
        pool = EgressPool(_two_route_config())
        pool._quarantine_route(
            "default",
            datetime.now(UTC) - timedelta(seconds=10),
            "http-429",
        )

        route = pool.status_snapshot().routes[0]

        assert route.quarantined is False
        assert route.quarantine_seconds_remaining == 0.0

    def test_quarantine_deadline_is_extend_only_and_records_close_marker(self) -> None:
        """Spec — shorter later quarantines do not shorten route quarantine.

        Given: a route is already quarantined until a later deadline,
        When: a shorter close-1015 quarantine is recorded,
        Then: the deadline remains extended and the close marker is captured.
        """
        pool = EgressPool(_two_route_config())
        later = datetime.now(UTC) + timedelta(seconds=60)
        earlier = datetime.now(UTC) + timedelta(seconds=10)

        pool._quarantine_route("default", later, "http-429")
        pool._quarantine_route("default", earlier, "close-1015")
        route = pool.snapshot()[0]

        assert route.quarantine_until == later
        assert route.last_handshake_429_at is not None
        assert route.last_close_1015_at is not None

    def test_active_reservation_decrement_ignores_missing_tuple(self) -> None:
        """Spec — defensive active-map cleanup ignores unknown tuples.

        Given: no active reservation exists for a route/exchange/class tuple,
        When: the cleanup hook is invoked for that tuple,
        Then: the status snapshot remains empty and no count goes negative.
        """
        pool = EgressPool(_two_route_config())

        with pool._lock:
            pool._decrement_active_reservation_locked("default", "kraken", "public")

        route = pool.status_snapshot().routes[0]
        assert route.in_use_count == 0
        assert route.active_reservations == []


class TestReserveAllQuarantined:
    """Tests for ``EgressPool.reserve`` when no route is available."""

    def test_wait_mode_falls_back_to_direct_route(self) -> None:
        """Spec — on_all_quarantined="wait" falls back to direct even when quarantined.

        Given all routes quarantined,
        When reserve is called with on_all_quarantined="wait",
        Then the direct route is returned (the SDK will sleep via
        earliest_release_in_seconds rather than retry immediately).
        """
        pool = EgressPool(_two_route_config(on_all_quarantined="wait"))
        for _ in range(2):
            reservation = pool.reserve(exchange="kraken", purpose="websocket")
            reservation.quarantine(120.0, reason="http-429")
            reservation.release()
        fallback = pool.reserve(exchange="kraken", purpose="websocket")
        assert fallback.route_id == "default"

    def test_raise_mode_raises_on_all_quarantined(self) -> None:
        """Spec — on_all_quarantined="raise" raises AllRoutesQuarantinedError.

        Given all routes quarantined and on_all_quarantined="raise",
        When reserve is called,
        Then AllRoutesQuarantinedError fires.
        """
        pool = EgressPool(_two_route_config(on_all_quarantined="raise"))
        for _ in range(2):
            reservation = pool.reserve(exchange="kraken", purpose="websocket")
            reservation.quarantine(120.0, reason="http-429")
            reservation.release()
        with pytest.raises(AllRoutesQuarantinedError):
            pool.reserve(exchange="kraken", purpose="websocket")

    def test_wait_mode_raises_when_no_direct_route(self) -> None:
        """Spec — wait mode without an enabled direct route also raises.

        Disabled-direct + quarantined-socks5 pool: there is nothing
        to fall back to even in wait mode. Defensive — the
        EgressPoolConfig validator should already reject this at
        config time, but the pool itself must also be safe if
        constructed directly (e.g. tests).
        """
        config = EgressPoolConfig(
            enabled=False,
            routes=[
                RouteConfig(
                    id="s",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                )
            ],
        )
        pool = EgressPool(config)
        only = pool.reserve(exchange="kraken", purpose="websocket")
        only.quarantine(120.0, reason="http-429")
        only.release()
        with pytest.raises(AllRoutesQuarantinedError):
            pool.reserve(exchange="kraken", purpose="websocket")


class TestHasAvailable:
    """Tests for ``EgressPool.has_available``."""

    def test_returns_true_when_route_healthy(self) -> None:
        """Spec — has_available() is True when any route is healthy.

        Given a fresh pool,
        When has_available() is called,
        Then True is returned.
        """
        pool = EgressPool(_two_route_config())
        assert pool.has_available() is True

    def test_returns_false_when_all_quarantined(self) -> None:
        """Spec — has_available() is False when all enabled routes quarantined.

        Given a pool with all routes inside their quarantine window,
        When has_available() is called,
        Then False is returned.
        """
        pool = EgressPool(_two_route_config())
        for _ in range(2):
            reservation = pool.reserve(exchange="kraken", purpose="websocket")
            reservation.quarantine(120.0, reason="http-429")
            reservation.release()
        assert pool.has_available() is False

    def test_returns_false_for_disabled_only(self) -> None:
        """Spec — disabled routes are not "available".

        Given a pool with only disabled routes,
        When has_available() is called,
        Then False is returned.
        """
        config = EgressPoolConfig(
            enabled=False,
            routes=[
                RouteConfig(id="d", kind="direct", enabled=False),
            ],
        )
        pool = EgressPool(config)
        assert pool.has_available() is False

    def test_filters_by_exchange_for_pinned_routes(self) -> None:
        """Spec — has_available(exchange='kraken') ignores Walutomat-pinned routes.

        Pool has direct(disabled) + pl(socks5, healthy, allowed=['walutomat']).
        Without an exchange filter, has_available() reports True
        because pl is healthy. With exchange='kraken', the filter
        excludes pl → False, which is the correct signal to the
        Kraken reconnect path that there is NO healthy Kraken-eligible
        route and it should sleep until release, not retry in 1s.
        """
        config = EgressPoolConfig(
            enabled=False,
            routes=[
                RouteConfig(id="d", kind="direct", enabled=False),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    priority=10,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )
        pool = EgressPool(config)
        assert pool.has_available() is True
        assert pool.has_available(exchange="kraken") is False
        assert pool.has_available(exchange="walutomat") is True


class TestDefensiveBranches:
    """Tests for defensive ``state is None`` early returns.

    These branches defend against pathological scenarios where a
    route_id is unknown to the pool (e.g. a reservation outliving a
    pool reconfiguration). The shim never hits these paths in
    practice; they exist for safety.
    """

    def test_decrement_in_use_ignores_unknown_route(self) -> None:
        """Spec — _decrement_in_use of an unknown route_id is a no-op.

        Given a pool with one route,
        When _decrement_in_use is called with a route_id that does
            not exist,
        Then the call returns without error and existing counts are
            unchanged.
        """
        pool = EgressPool(_two_route_config())
        pool._decrement_in_use("nonexistent-route", "kraken", "public", "ws", None)
        snapshot = pool.snapshot()
        for snap in snapshot:
            assert snap.in_use_count == 0

    def test_active_reservation_helpers_ignore_unknown_route(self) -> None:
        """Spec — active reservation helpers ignore unknown route ids.

        Given a pool with one configured route,
        When active reservation helpers receive a missing route id,
        Then they return without mutating visible route status.
        """
        pool = EgressPool(_two_route_config())
        pool._increment_active_reservation_locked("missing-route", "kraken", "public")
        pool._decrement_active_reservation_locked("missing-route", "kraken", "public")

        snapshot = pool.status_snapshot()

        assert all(route.active_reservations == [] for route in snapshot.routes)

    def test_connection_helpers_ignore_unknown_or_missing_rows(self) -> None:
        """Spec — connection helper safety branches ignore missing state.

        Given a pool with one route,
        When connection helpers receive an unknown route or missing key,
        Then they return without mutating visible route status.
        """
        pool = EgressPool(_two_route_config())
        observed_at = datetime.now(UTC)
        pool._increment_connection_locked(
            "missing-route",
            "kraken",
            "public",
            "ws",
            "ws.kraken.com",
            observed_at,
        )
        pool._decrement_connection_locked(
            "missing-route",
            "kraken",
            "public",
            "ws",
            "ws.kraken.com",
        )
        pool._decrement_connection_locked(
            "default",
            "kraken",
            "public",
            "ws",
            "ws.kraken.com",
        )
        pool._record_rest_last_seen_locked(
            "missing-route",
            ("kraken", "public", "rest", "api.kraken.com"),
            observed_at,
        )

        assert all(route.connections == [] for route in pool.status_snapshot().routes)

    def test_quarantine_route_ignores_unknown_route(self) -> None:
        """Spec — _quarantine_route of an unknown route_id is a no-op.

        Given a pool with one route,
        When _quarantine_route is called with a route_id that does
            not exist,
        Then the call returns without error and no existing quarantine
            timestamps change.
        """
        pool = EgressPool(_two_route_config())
        pool._quarantine_route(
            "nonexistent-route",
            datetime.now(UTC) + timedelta(seconds=60),
            "http-429",
        )
        snapshot = pool.snapshot()
        for snap in snapshot:
            assert snap.quarantine_until is None


class TestEarliestReleaseInSeconds:
    """Tests for ``EgressPool.earliest_release_in_seconds``."""

    def test_returns_none_when_no_quarantines(self) -> None:
        """Spec — returns None when no route is quarantined.

        Given a fresh pool with no quarantines,
        When earliest_release_in_seconds() is called,
        Then None is returned.
        """
        pool = EgressPool(_two_route_config())
        assert pool.earliest_release_in_seconds() is None

    def test_returns_smallest_deadline(self) -> None:
        """Spec — returns the minimum of all quarantine deadlines.

        Given two routes quarantined for 30 s and 90 s,
        When earliest_release_in_seconds() is called,
        Then the result is approximately 30 (allow ±2 s for test latency).
        """
        pool = EgressPool(_two_route_config())
        first = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="default",
        )
        first.quarantine(30.0, reason="http-429")
        first.release()
        second = pool.reserve(
            exchange="kraken",
            purpose="websocket",
            preferred_route="wg-uk-1",
        )
        second.quarantine(90.0, reason="http-429")
        second.release()
        earliest = pool.earliest_release_in_seconds()
        assert earliest is not None
        assert 27.0 <= earliest <= 32.0

    def test_returns_zero_when_deadline_already_passed(self) -> None:
        """Spec — past deadlines yield 0.0, not negative.

        Given a quarantine deadline already in the past,
        When earliest_release_in_seconds() is called,
        Then 0.0 is returned.
        """
        pool = EgressPool(_two_route_config())
        pool._quarantine_route(
            "default",
            datetime.now(UTC) - timedelta(seconds=10),
            "http-429",
        )
        result = pool.earliest_release_in_seconds()
        assert result == 0.0

    def test_filters_by_exchange_for_pinned_routes(self) -> None:
        """Spec — earliest_release_in_seconds(exchange) ignores other-exchange pins.

        When a Walutomat-pinned route is quarantined and the only
        Kraken-eligible direct fallback has no deadline, asking for
        the Kraken-eligible release time must return ``None`` (no
        Kraken-eligible route in quarantine), NOT the pl deadline.
        The Kraken reconnect path needs this so it doesn't sleep
        until a Walutomat route releases when that release has no
        bearing on Kraken availability.
        """
        config = EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="default", kind="direct", priority=100),
                RouteConfig(
                    id="pl",
                    kind="socks5",
                    proxy_url="socks5h://x:1084",
                    priority=10,
                    allowed_exchanges=("walutomat",),
                ),
            ],
        )
        pool = EgressPool(config)
        pool._quarantine_route(
            "pl",
            datetime.now(UTC) + timedelta(seconds=120),
            "http-429",
        )
        assert pool.earliest_release_in_seconds() is not None
        assert pool.earliest_release_in_seconds(exchange="walutomat") is not None
        assert pool.earliest_release_in_seconds(exchange="kraken") is None


class TestModuleSingletonState:
    """Tests for ``configure_egress_pool`` + ``get_egress_pool`` + ``reset``."""

    def test_get_returns_none_before_configure(self) -> None:
        """Spec — get_egress_pool() returns None when no pool is configured.

        Given a fresh process (singleton reset by fixture),
        When get_egress_pool() is called,
        Then None is returned.
        """
        assert get_egress_pool() is None

    def test_configure_with_disabled_config_clears_singleton(self) -> None:
        """Spec — configure_egress_pool(enabled=False) sets singleton to None.

        Given a previously configured pool,
        When configure is called with enabled=False,
        Then get_egress_pool() returns None.
        """
        configure_egress_pool(_two_route_config())
        assert get_egress_pool() is not None
        configure_egress_pool(EgressPoolConfig(enabled=False, routes=[]))
        assert get_egress_pool() is None

    def test_configure_replaces_singleton(self) -> None:
        """Spec — successive configures replace the singleton.

        Given a configured pool,
        When configure is called again with a different config,
        Then get_egress_pool() returns the new instance.
        """
        configure_egress_pool(_two_route_config(direct_priority=0))
        first = get_egress_pool()
        configure_egress_pool(_two_route_config(direct_priority=5))
        second = get_egress_pool()
        assert first is not second

    def test_reset_egress_pool_clears(self) -> None:
        """Spec — reset_egress_pool() removes the singleton.

        Given a configured pool,
        When reset_egress_pool() is called,
        Then get_egress_pool() returns None.
        """
        configure_egress_pool(_two_route_config())
        assert get_egress_pool() is not None
        reset_egress_pool()
        assert get_egress_pool() is None


class TestParseEgressPoolSetting:
    """Tests for ``_parse_egress_pool_setting`` boundary handling."""

    def test_dict_input_passes_through(self) -> None:
        """Spec — dict input is returned verbatim.

        Given an already-parsed dict from the settings cache,
        When _parse_egress_pool_setting is called,
        Then the dict is returned unchanged.
        """
        parsed = _parse_egress_pool_setting({"enabled": False})
        assert parsed == {"enabled": False}

    def test_string_input_is_decoded(self) -> None:
        """Spec — JSON string input is decoded to a dict.

        Given a JSON string,
        When _parse_egress_pool_setting is called,
        Then the decoded dict is returned.
        """
        parsed = _parse_egress_pool_setting('{"enabled": true, "routes": []}')
        assert parsed == {"enabled": True, "routes": []}

    def test_malformed_json_returns_none(self) -> None:
        """Spec — invalid JSON returns None (loader logs the error).

        Given a malformed JSON string,
        When _parse_egress_pool_setting is called,
        Then None is returned.
        """
        parsed = _parse_egress_pool_setting("{not json")
        assert parsed is None

    def test_non_dict_json_returns_none(self) -> None:
        """Spec — JSON that decodes to a non-dict value returns None.

        Given a JSON array,
        When _parse_egress_pool_setting is called,
        Then None is returned.
        """
        parsed = _parse_egress_pool_setting("[1, 2, 3]")
        assert parsed is None

    def test_unexpected_type_returns_none(self) -> None:
        """Spec — unknown value types are rejected.

        Given an int (which a misconfigured setting could yield),
        When _parse_egress_pool_setting is called,
        Then None is returned.
        """
        parsed = _parse_egress_pool_setting(42)
        assert parsed is None


class TestExtractProxyHost:
    """Tests for ``_extract_proxy_host`` URL parsing."""

    def test_extract_socks5h_host(self) -> None:
        """Spec — extract host from a standard socks5h URL.

        Given proxy_url="socks5h://snapper-egress:1081",
        When _extract_proxy_host is called,
        Then "snapper-egress" is returned.
        """
        assert _extract_proxy_host("socks5h://snapper-egress:1081") == "snapper-egress"

    def test_extract_socks5h_host_with_credentials(self) -> None:
        """Spec — credentials in URL do not poison the host extraction.

        Given proxy_url="socks5h://u:p@host:1081",
        When _extract_proxy_host is called,
        Then "host" is returned (urlsplit drops the userinfo).
        """
        assert _extract_proxy_host("socks5h://u:p@host:1081") == "host"

    def test_extract_returns_none_when_no_host(self) -> None:
        """Spec — URLs without a host component return None.

        Given proxy_url="socks5h:///path",
        When _extract_proxy_host is called,
        Then None is returned.
        """
        assert _extract_proxy_host("socks5h:///") is None

    def test_extract_returns_none_on_parse_error(self) -> None:
        """Spec — malformed URLs that raise ValueError yield None.

        Given an invalid IPv6 URL (urlsplit raises ValueError),
        When _extract_proxy_host is called,
        Then None is returned (the except path is taken).
        """
        assert _extract_proxy_host("socks5h://[invalid::ipv6") is None


@pytest.mark.asyncio
class TestPreflightRoutes:
    """Async preflight tests — DNS lookup, python-socks check, route override."""

    async def test_direct_routes_pass_through_unchanged(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — direct routes need no preflight work.

        Given a config with one direct route AND a SOCKS5 route
            whose DNS + SOCKS5 greeting are mocked to succeed,
        When _preflight_routes runs,
        Then the direct route survives untouched (enabled=True).
        """

        async def fake_getaddrinfo(*args: object, **kwargs: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            class _R:
                async def readexactly(self, _n: int) -> bytes:
                    return b"\x05\x00"

            class _W:
                def write(self, _d: bytes) -> None: ...
                async def drain(self) -> None: ...
                def close(self) -> None: ...
                async def wait_closed(self) -> None: ...

            return _R(), _W()

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        direct_route = next(r for r in effective.routes if r.kind == "direct")
        assert direct_route.enabled is True

    async def test_socks5_route_auto_disabled_when_python_socks_missing(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — missing python_socks disables all SOCKS5 routes.

        Given importlib.util.find_spec returns None for "python_socks",
        When _preflight_routes runs on a config with a SOCKS5 route,
        Then the SOCKS5 route's effective enabled flag is False.
        """

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return None
            return MagicMock()

        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False

    async def test_socks5_route_auto_disabled_on_dns_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — DNS resolution failure disables the SOCKS5 route.

        Given asyncio.getaddrinfo raises OSError for the socks5 host
            AND python_socks is importable (otherwise the route would
            be auto-disabled BEFORE the DNS check runs),
        When _preflight_routes runs,
        Then the socks5 route's effective enabled flag is False.
        """

        async def fake_getaddrinfo(*args: object, **kwargs: object) -> list[object]:
            raise OSError("simulated DNS failure")

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False

    async def test_socks5_route_with_resolvable_host_survives(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        r"""Spec — resolvable SOCKS5 host + healthy listener keeps enabled=True.

        Given successful getaddrinfo + SOCKS5 greeting (b"\x05\x00")
            + python_socks importable,
        When _preflight_routes runs,
        Then the socks5 route stays enabled.
        """

        async def fake_getaddrinfo(*args: object, **kwargs: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            class _R:
                async def readexactly(self, _n: int) -> bytes:
                    return b"\x05\x00"

            class _W:
                def write(self, _d: bytes) -> None: ...
                async def drain(self) -> None: ...
                def close(self) -> None: ...
                async def wait_closed(self) -> None: ...

            return _R(), _W()

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is True

    async def test_no_warning_when_socks5_missing_and_no_socks_routes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — pure-direct config with python-socks missing is silent.

        Given python_socks is not importable AND the config has no
            SOCKS5 routes,
        When _preflight_routes runs,
        Then no warning is logged and the direct routes survive.
            Covers the branch where ``any_socks`` is False so the
            warning block is skipped.
        """

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return None
            return MagicMock()

        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = EgressPoolConfig(
            enabled=True,
            routes=[RouteConfig(id="d", kind="direct")],
        )
        effective = await _preflight_routes(config)
        assert all(r.enabled for r in effective.routes)

    async def test_already_disabled_route_passes_through(self) -> None:
        """Spec — preflight does not re-enable explicitly disabled routes.

        Given a SOCKS5 route with enabled=False already,
        When _preflight_routes runs,
        Then the route stays disabled (no DNS lookup attempted).
        """
        config = EgressPoolConfig(
            enabled=False,
            routes=[
                RouteConfig(
                    id="s",
                    kind="socks5",
                    proxy_url="socks5h://x:1081",
                    enabled=False,
                )
            ],
        )
        effective = await _preflight_routes(config)
        assert effective.routes[0].enabled is False


@pytest.mark.asyncio
class TestSocks5GreetingProbe:
    """Tests for the SOCKS5 listener reachability probe.

    Closes the degraded-ready gap: even when DNS resolves to the
    sidecar host, the pool must NOT admit a route whose TCP listener
    is absent or doesn't speak SOCKS5.
    """

    async def test_socks5_route_with_reachable_listener_stays_enabled(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        r"""Spec — when the SOCKS5 listener responds correctly, route stays enabled.

        Given a SOCKS5 route whose listener replies with the canonical
            ``\x05\x00`` greeting reply,
        When _preflight_one_route runs,
        Then the route's enabled flag stays True.
        """

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            class _R:
                async def readexactly(self, n: int) -> bytes:
                    return b"\x05\x00"

            class _W:
                def write(self, _data: bytes) -> None: ...
                async def drain(self) -> None: ...
                def close(self) -> None: ...
                async def wait_closed(self) -> None: ...

            return _R(), _W()

        async def fake_getaddrinfo(*_a: object, **_kw: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is True

    async def test_socks5_route_auto_disabled_when_listener_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — TCP connection refused → route auto-disabled.

        Given asyncio.open_connection raises ConnectionRefusedError on
            the SOCKS5 probe AND DNS resolves AND python_socks installed,
        When _preflight_one_route runs,
        Then the route's enabled flag is False (closes the
            degraded-ready gap).
        """

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            raise ConnectionRefusedError("simulated closed listener")

        async def fake_getaddrinfo(*_a: object, **_kw: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False

    async def test_socks5_route_auto_disabled_on_probe_timeout(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — open_connection timeout → route auto-disabled.

        Given asyncio.open_connection never returns (hangs past
            _SOCKS5_PROBE_TIMEOUT_S),
        When _preflight_one_route runs,
        Then the route is auto-disabled.
        """

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            await asyncio.sleep(10.0)
            return MagicMock(), MagicMock()

        async def fake_getaddrinfo(*_a: object, **_kw: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        monkeypatch.setattr(pool_module, "_SOCKS5_PROBE_TIMEOUT_S", 0.01)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False

    async def test_socks5_route_auto_disabled_on_wrong_greeting_reply(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        r"""Spec — non-SOCKS5 reply → route auto-disabled.

        Given the listener accepts the TCP connection but replies with
            something other than ``\x05\x00`` (e.g. an HTTP server
            mistakenly listening on the SOCKS5 port),
        When _preflight_one_route runs,
        Then the route is auto-disabled.
        """

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            class _R:
                async def readexactly(self, n: int) -> bytes:
                    return b"HT"

            class _W:
                def write(self, _data: bytes) -> None: ...
                async def drain(self) -> None: ...
                def close(self) -> None: ...
                async def wait_closed(self) -> None: ...

            return _R(), _W()

        async def fake_getaddrinfo(*_a: object, **_kw: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False

    async def test_socks5_route_auto_disabled_on_short_greeting_reply(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — listener closes mid-greeting → route auto-disabled.

        Given the listener accepts the TCP connection but closes before
            sending the full 2-byte greeting reply,
        When _preflight_one_route runs,
        Then the route is auto-disabled (IncompleteReadError path).
        """

        async def fake_open_connection(host: object, port: object) -> tuple[Any, Any]:
            class _R:
                async def readexactly(self, n: int) -> bytes:
                    raise asyncio.IncompleteReadError(partial=b"\x05", expected=n)

            class _W:
                def write(self, _data: bytes) -> None: ...
                async def drain(self) -> None: ...
                def close(self) -> None: ...
                async def wait_closed(self) -> None: ...

            return _R(), _W()

        async def fake_getaddrinfo(*_a: object, **_kw: object) -> list[object]:
            return [("af_inet", "sock_stream", 0, "", ("127.0.0.1", 1081))]

        loop = MagicMock()
        loop.getaddrinfo = fake_getaddrinfo

        def fake_get_running_loop() -> MagicMock:
            return loop

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.asyncio, "get_running_loop", fake_get_running_loop)
        monkeypatch.setattr(pool_module.asyncio, "open_connection", fake_open_connection)
        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False

    async def test_socks5_route_auto_disabled_when_proxy_url_has_no_port(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — proxy_url without explicit port → route auto-disabled.

        We require an explicit port because the sidecar binds per-tunnel
        port numbers; defaulting would mask operator misconfig. We can't
        construct a portless URL through RouteConfig validation, so this
        test mutates the route post-construction.

        Given a socks5h URL whose extracted port is None,
        When _preflight_one_route runs,
        Then the route is auto-disabled with a logged warning.
        """

        def fake_find_spec(name: str) -> object | None:
            if name == "python_socks":
                return MagicMock()
            return None

        monkeypatch.setattr(pool_module.importlib.util, "find_spec", fake_find_spec)
        monkeypatch.setattr(pool_module, "_extract_proxy_port", lambda _url: None)
        config = _two_route_config()
        effective = await _preflight_routes(config)
        socks_route = next(r for r in effective.routes if r.kind == "socks5")
        assert socks_route.enabled is False


class TestExtractProxyPort:
    """Tests for ``_extract_proxy_port`` URL parsing."""

    def test_extract_explicit_port(self) -> None:
        """Spec — extract numeric port from standard socks5h URL.

        Given proxy_url="socks5h://snapper-egress:1081",
        When _extract_proxy_port is called,
        Then 1081 is returned.
        """
        assert _extract_proxy_port("socks5h://snapper-egress:1081") == 1081

    def test_extract_returns_none_when_no_port(self) -> None:
        """Spec — URL without explicit port returns None.

        Given proxy_url="socks5h://host",
        When _extract_proxy_port is called,
        Then None is returned (operator must specify the port).
        """
        assert _extract_proxy_port("socks5h://host") is None

    def test_extract_returns_none_on_invalid_port(self) -> None:
        """Spec — invalid (out-of-range) port → None.

        Given proxy_url with a port value urllib cannot parse,
        When _extract_proxy_port is called,
        Then None is returned.
        """
        assert _extract_proxy_port("socks5h://host:99999999999999999") is None

    def test_extract_returns_none_on_urlsplit_error(self) -> None:
        """Spec — URL that triggers urlsplit ValueError → None.

        Given an invalid IPv6 URL (urlsplit raises ValueError),
        When _extract_proxy_port is called,
        Then None is returned (the except path is taken).
        """
        assert _extract_proxy_port("socks5h://[invalid::ipv6") is None


@pytest.mark.asyncio
class TestPreflightOneRoute:
    """Branch-coverage tests for ``_preflight_one_route`` edge cases."""

    async def test_direct_route_skips_dns(self) -> None:
        """Spec — direct route returns identity, no DNS lookup."""
        route = RouteConfig(id="d", kind="direct")
        result = await _preflight_one_route(MagicMock(), route, True)
        assert result is route

    async def test_socks5_with_no_proxy_url_when_python_socks_ok(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — socks5 with missing proxy_url at preflight is disabled.

        This branch is defensive — the Pydantic validator rejects
        socks5 routes without proxy_url, but if a future change ever
        bypasses validation, preflight catches the missing URL.
        """
        route = RouteConfig(
            id="s",
            kind="socks5",
            proxy_url="socks5h://x:1081",
        )
        broken = route.model_copy(update={"proxy_url": None})
        loop = MagicMock()
        result = await _preflight_one_route(loop, broken, True)
        assert result.enabled is False

    async def test_socks5_with_unparseable_proxy_url(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — proxy_url whose host can't be extracted disables the route.

        Given proxy_url that yields no host (via urlsplit), preflight
        auto-disables the route without raising.
        """
        route = RouteConfig(
            id="s",
            kind="socks5",
            proxy_url="socks5h://x:1081",
        )
        broken = route.model_copy(update={"proxy_url": "socks5h:///"})
        loop = MagicMock()
        result = await _preflight_one_route(loop, broken, True)
        assert result.enabled is False


@pytest.mark.asyncio
class TestInitializeEgressPool:
    """End-to-end tests for ``initialize_egress_pool``."""

    async def test_returns_none_when_setting_missing(self) -> None:
        """Spec — missing setting key disables the pool.

        Given a settings service with no egress_pool key,
        When initialize_egress_pool runs,
        Then it returns None and get_egress_pool() is None.
        """
        service = MagicMock()
        service.get_setting = MagicMock(return_value=None)
        result = await initialize_egress_pool(service)
        assert result is None
        assert get_egress_pool() is None

    async def test_returns_none_when_setting_is_disabled(self) -> None:
        """Spec — explicit enabled=False disables the pool.

        Given a service returning enabled=False,
        When initialize_egress_pool runs,
        Then it returns None.
        """
        service = MagicMock()
        service.get_setting = MagicMock(return_value={"enabled": False, "routes": []})
        result = await initialize_egress_pool(service)
        assert result is None

    async def test_returns_pool_when_enabled(self) -> None:
        """Spec — well-formed enabled config produces a pool.

        Given an enabled config with one direct route,
        When initialize_egress_pool runs,
        Then a pool with size=1 is returned + matches get_egress_pool().
        """
        service = MagicMock()
        service.get_setting = MagicMock(
            return_value={
                "enabled": True,
                "routes": [{"id": "d", "kind": "direct"}],
            }
        )
        result = await initialize_egress_pool(service)
        assert result is not None
        assert result.size() == 1
        assert get_egress_pool() is result

    async def test_returns_none_on_invalid_json_string(self) -> None:
        """Spec — malformed JSON string returns None.

        Given a setting value that fails json.loads,
        When initialize_egress_pool runs,
        Then None is returned and the singleton stays cleared.
        """
        service = MagicMock()
        service.get_setting = MagicMock(return_value="{not json")
        result = await initialize_egress_pool(service)
        assert result is None
        assert get_egress_pool() is None

    async def test_returns_none_on_validation_error(self) -> None:
        """Spec — Pydantic validation failure disables the pool.

        Given a config that fails EgressPoolConfig validation
        (enabled=True with no direct route),
        When initialize_egress_pool runs,
        Then None is returned.
        """
        service = MagicMock()
        service.get_setting = MagicMock(
            return_value={
                "enabled": True,
                "routes": [
                    {
                        "id": "s",
                        "kind": "socks5",
                        "proxy_url": "socks5h://x:1081",
                    }
                ],
            }
        )
        result = await initialize_egress_pool(service)
        assert result is None

    async def test_accepts_setting_value_as_json_string(self) -> None:
        """Spec — JSON-string-formatted setting value is decoded.

        Given a setting whose value is a JSON string (as stored in DB),
        When initialize_egress_pool runs,
        Then the pool is built successfully.
        """
        payload = json.dumps(
            {
                "enabled": True,
                "routes": [{"id": "d", "kind": "direct"}],
            }
        )
        service = MagicMock()
        service.get_setting = MagicMock(return_value=payload)
        result = await initialize_egress_pool(service)
        assert result is not None
        assert result.size() == 1


class TestSafelyInitializeEgressPool:
    """Best-effort wrapper shared by the API lifespan and feed publishers."""

    @pytest.mark.asyncio
    async def test_invokes_initialize_with_service(self) -> None:
        """Verify the wrapper runs the preflight with the settings_service.

        Given: A settings service and a successful preflight,
        When: safely_initialize_egress_pool is awaited,
        Then: initialize_egress_pool is awaited once with that service.
        """
        service = MagicMock()
        with patch.object(
            pool_module, "initialize_egress_pool", new_callable=AsyncMock
        ) as init_mock:
            await safely_initialize_egress_pool(service)
        init_mock.assert_awaited_once_with(service)

    @pytest.mark.asyncio
    async def test_swallows_preflight_exception(self) -> None:
        """Verify a failed preflight is logged but never propagates.

        Given: initialize_egress_pool raises,
        When: safely_initialize_egress_pool is awaited,
        Then: No exception escapes (callers fall back to direct egress).
        """
        with patch.object(
            pool_module,
            "initialize_egress_pool",
            new_callable=AsyncMock,
            side_effect=RuntimeError("simulated preflight failure"),
        ):
            await safely_initialize_egress_pool(MagicMock())
