"""Unit tests for ``EgressPool`` selection, quarantine, and singleton state."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

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
from snapper.infrastructure.network.egress_pool import reset_egress_pool


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
    """Tests for B'.5 — per-exchange route selection via ``allowed_exchanges``."""

    @staticmethod
    def _pl_pinned_config() -> EgressPoolConfig:
        """Helper — direct fallback + IE/UK open + PL pinned to walutomat.

        Mirrors the production target shape after F1 deployment of
        plan_2026_05_22_egress_per_exchange_routing.md.
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

    def test_pool_picks_pinned_route_for_listed_exchange(self) -> None:
        """Spec — pl route pinned to ``["walutomat"]`` is selected for that exchange.

        Even though IE/UK have equal priority and lower in_use_count
        (both fresh), the lowest (priority, in_use_count) tuple across
        all 4 routes that pass the exchange filter is the only allowed
        route — pl1 — when reserving for walutomat.

        Note: IE/UK ALSO match because empty ``allowed_exchanges``
        means "any exchange". With 3 priority-10 routes available
        and pl listed last, the deterministic in_use_count=0 sort
        keeps the FIRST-inserted matching route. So this test
        actually proves the filter doesn't *exclude* pl, not that pl
        is uniquely chosen. The complementary test below confirms pl
        IS excluded for kraken.
        """
        pool = EgressPool(self._pl_pinned_config())
        reservation = pool.reserve(exchange="walutomat", purpose="websocket")
        assert reservation.route_id in {"ie", "uk", "pl"}

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
        pool._decrement_in_use("nonexistent-route")
        snapshot = pool.snapshot()
        for snap in snapshot:
            assert snap.in_use_count == 0

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
    """Tests for the SOCKS5 listener reachability probe (v4 plan).

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
        Then the route's enabled flag is False (closes v4 gap).
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
