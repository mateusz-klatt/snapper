"""Unit tests for :class:`PooledAsyncTransport`.

Covers:

* Pool-disabled fast path (no reservation).
* ContextVar exchange-tag pickup with default-tag fallback.
* Underlying-transport caching across requests on the same route.
* SOCKS5 proxy URL injection into the transport factory.
* Connect-error quarantine + re-raise + still-released reservation.
* ConnectTimeout same handling as ConnectError.
* Unrelated exception passthrough with reservation still released.
* ``aclose`` closes every cached transport.
"""

import contextlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import httpx
import pytest

from snapper.infrastructure.network.egress_context import _CURRENT_PUBLISHER
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool
from snapper.infrastructure.network.pooled_httpx_transport import _HTTP_CONNECT_ERROR_QUARANTINE_S
from snapper.infrastructure.network.pooled_httpx_transport import PooledAsyncTransport


class _FakeTransport:
    """Stand-in for :class:`httpx.AsyncHTTPTransport` used by tests.

    Captures construction kwargs + records each request it handles
    so assertions can verify which underlying transport saw which
    request without spinning up a real HTTP stack.
    """

    def __init__(
        self,
        *,
        proxy: str | None = None,
        side_effect: Exception | None = None,
    ) -> None:
        self.proxy = proxy
        self.handled_requests: list[httpx.Request] = []
        self.aclose_called = False
        self._side_effect = side_effect

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._side_effect is not None:
            raise self._side_effect
        self.handled_requests.append(request)
        return httpx.Response(200, request=request)

    async def aclose(self) -> None:
        self.aclose_called = True


class _RecordingFactory:
    """Factory captures every ``proxy=`` value passed by the transport."""

    def __init__(self, side_effect: Exception | None = None) -> None:
        self.calls: list[str | None] = []
        self.created: list[_FakeTransport] = []
        self._side_effect = side_effect

    def __call__(self, *, proxy: str | None = None) -> _FakeTransport:
        self.calls.append(proxy)
        transport = _FakeTransport(proxy=proxy, side_effect=self._side_effect)
        self.created.append(transport)
        return transport


class TestPooledAsyncTransport:
    """Behavioural spec for the egress-pool-aware httpx transport."""

    @pytest.fixture(autouse=True)
    def _reset_pool(self) -> Any:
        """Reset the singleton + ContextVar before/after each test."""
        reset_egress_pool()
        yield
        reset_egress_pool()
        with contextlib.suppress(LookupError):
            _CURRENT_PUBLISHER.set(None)

    @staticmethod
    def _make_request() -> httpx.Request:
        return httpx.Request("GET", "https://example.com/")

    @pytest.mark.asyncio
    async def test_pool_disabled_uses_direct_transport_without_reservation(self) -> None:
        """Spec — when the pool is None or empty, fall through to a direct transport.

        Given ``get_egress_pool()`` returns None (singleton uninitialised),
        When a request is handled,
        Then the underlying transport is built with ``proxy=None``
        and the request is forwarded — no ``pool.reserve`` call
        because there is no pool to reserve from.
        """
        factory = _RecordingFactory()
        transport = PooledAsyncTransport(transport_factory=factory)

        response = await transport.handle_async_request(self._make_request())

        assert response.status_code == 200
        assert factory.calls == [None]
        assert factory.created[0].handled_requests == [response.request]

    @pytest.mark.asyncio
    async def test_pool_socks5_route_injects_proxy_url(self) -> None:
        """Spec — when the pool has a SOCKS5 route, its ``proxy_url`` is passed to the factory.

        Given a pool with a SOCKS5 route at lower priority than direct,
        And the direct route is quarantined so the SOCKS5 route wins selection,
        When a request is handled,
        Then the underlying transport is built with the SOCKS5 URL
        in ``proxy=``.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=0),
            RouteConfig(
                id="wg-pl-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1084",
                priority=10,
            ),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")

        factory = _RecordingFactory()
        transport = PooledAsyncTransport(transport_factory=factory)

        await transport.handle_async_request(self._make_request())

        assert factory.calls == ["socks5h://snapper-egress:1084"]

    @pytest.mark.asyncio
    async def test_request_host_is_recorded_without_path_or_query(self) -> None:
        """Spec — HTTPX request host is recorded as a REST connection host.

        Given a request URL with path and query data,
        When the pooled transport reserves and releases a route,
        Then the egress snapshot keeps only the lowercase hostname.
        """
        pool = configure_egress_pool(
            EgressPoolConfig(
                enabled=True,
                routes=[RouteConfig(id="default", kind="direct", priority=0)],
            )
        )
        assert pool is not None
        factory = _RecordingFactory()
        transport = PooledAsyncTransport(transport_factory=factory)
        request = httpx.Request(
            "GET",
            "https://API.WALUTOMAT.PL/private/order?orderId=secret",
        )

        await transport.handle_async_request(request)

        connection = pool.status_snapshot().routes[0].connections[0]
        assert connection.host == "api.walutomat.pl"
        assert connection.kind == "rest"
        assert connection.count == 0
        assert connection.last_seen_at is not None

    @pytest.mark.asyncio
    async def test_default_exchange_tag_used_when_no_publisher_context(self) -> None:
        """Spec — falls back to ``default_exchange_tag`` when ``_CURRENT_PUBLISHER`` is None.

        Given the pool has a SOCKS5 route restricted to
        ``allowed_exchanges=("kraken",)``,
        And ``_CURRENT_PUBLISHER`` is unset,
        And the transport's ``default_exchange_tag="kraken"``,
        When a request is handled,
        Then the SOCKS5 route is picked (allow-list permits the
        default tag) — proves the fallback path is consulted, not a
        hardcoded literal.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=0),
            RouteConfig(
                id="wg-pl-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1084",
                priority=10,
                allowed_exchanges=("kraken",),
            ),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")

        factory = _RecordingFactory()
        transport = PooledAsyncTransport(
            default_exchange_tag="kraken",
            transport_factory=factory,
        )

        await transport.handle_async_request(self._make_request())

        assert factory.calls == ["socks5h://snapper-egress:1084"]

    @pytest.mark.asyncio
    async def test_publisher_context_var_overrides_default_tag(self) -> None:
        """Spec — when ``_CURRENT_PUBLISHER`` is set, its ``_get_exchange_name()`` wins.

        Given the pool has a SOCKS5 route restricted to
        ``allowed_exchanges=("walutomat",)``,
        And ``_CURRENT_PUBLISHER`` is set to a publisher with
        ``_get_exchange_name() -> "walutomat"``,
        And the transport's ``default_exchange_tag`` is the unrelated
        ``"kraken"`` (which would be REJECTED by the allow-list),
        When a request is handled,
        Then the SOCKS5 route is picked — proving the ContextVar is
        consulted ahead of the default.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=0),
            RouteConfig(
                id="wg-pl-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1084",
                priority=10,
                allowed_exchanges=("walutomat",),
            ),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")

        mock_publisher = MagicMock()
        mock_publisher._get_exchange_name.return_value = "walutomat"
        token = _CURRENT_PUBLISHER.set(mock_publisher)
        try:
            factory = _RecordingFactory()
            transport = PooledAsyncTransport(
                default_exchange_tag="kraken",
                transport_factory=factory,
            )
            await transport.handle_async_request(self._make_request())
        finally:
            _CURRENT_PUBLISHER.reset(token)

        assert factory.calls == ["socks5h://snapper-egress:1084"]

    @pytest.mark.asyncio
    async def test_transport_caching_across_requests_on_same_route(self) -> None:
        """Spec — successive requests on the same route share one underlying transport.

        Given the pool has a single direct route,
        When two requests are handled in succession,
        Then ``transport_factory`` is invoked exactly ONCE and both
        requests pass through the same underlying transport
        instance. This keeps the httpx connection pool warm and
        avoids per-request TCP setup cost.
        """
        routes = [RouteConfig(id="default", kind="direct", priority=0)]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None

        factory = _RecordingFactory()
        transport = PooledAsyncTransport(transport_factory=factory)

        await transport.handle_async_request(self._make_request())
        await transport.handle_async_request(self._make_request())

        assert factory.calls == [None]
        assert len(factory.created) == 1
        assert len(factory.created[0].handled_requests) == 2

    @pytest.mark.asyncio
    async def test_connect_error_quarantines_route_and_reraises(self) -> None:
        """Spec — ``httpx.ConnectError`` quarantines the borrowed route, then re-raises.

        Given a pool with a SOCKS5 route + a direct fallback,
        And the SOCKS5 route is preferred (lower priority),
        And the underlying transport raises ``httpx.ConnectError``,
        When a request is handled,
        Then the SOCKS5 route is marked quarantined for
        ``_HTTP_CONNECT_ERROR_QUARANTINE_S`` seconds, the exception
        propagates, and the reservation is released so a subsequent
        request hits the direct fallback.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=100),
            RouteConfig(
                id="wg-pl-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1084",
                priority=10,
            ),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None

        connect_err = httpx.ConnectError("simulated tunnel down")
        factory = _RecordingFactory(side_effect=connect_err)
        transport = PooledAsyncTransport(transport_factory=factory)

        connect_error_request = self._make_request()
        with pytest.raises(httpx.ConnectError, match="simulated tunnel down"):
            await transport.handle_async_request(connect_error_request)

        snap_by_id = {snap.id: snap for snap in pool.snapshot()}
        assert snap_by_id["wg-pl-1"].quarantine_until is not None
        assert snap_by_id["wg-pl-1"].in_use_count == 0
        assert snap_by_id["default"].quarantine_until is None

    @pytest.mark.asyncio
    async def test_connect_timeout_is_treated_as_connect_error(self) -> None:
        """Spec — ``httpx.ConnectTimeout`` follows the same path as ``ConnectError``.

        Given the same setup as the connect-error test,
        But the underlying transport raises ``httpx.ConnectTimeout``,
        Then the route is quarantined identically.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=100),
            RouteConfig(
                id="wg-pl-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1084",
                priority=10,
            ),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None

        timeout = httpx.ConnectTimeout("connect timed out")
        factory = _RecordingFactory(side_effect=timeout)
        transport = PooledAsyncTransport(transport_factory=factory)

        connect_timeout_request = self._make_request()
        with pytest.raises(httpx.ConnectTimeout, match="connect timed out"):
            await transport.handle_async_request(connect_timeout_request)

        snap_by_id = {snap.id: snap for snap in pool.snapshot()}
        assert snap_by_id["wg-pl-1"].quarantine_until is not None

    @pytest.mark.asyncio
    async def test_non_connect_error_propagates_without_quarantine(self) -> None:
        """Spec — application-level errors (e.g. HTTP 5xx surfaced) do NOT quarantine.

        Given the underlying transport raises ``httpx.ReadError``
        (a stream-level fault that is NOT a connect failure — it
        means the connect succeeded and the route is healthy),
        When a request is handled,
        Then the exception propagates BUT the route is not
        quarantined and the reservation is still released.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=0),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None

        read_err = httpx.ReadError("simulated read failure")
        factory = _RecordingFactory(side_effect=read_err)
        transport = PooledAsyncTransport(transport_factory=factory)

        read_error_request = self._make_request()
        with pytest.raises(httpx.ReadError, match="simulated read failure"):
            await transport.handle_async_request(read_error_request)

        snap = pool.snapshot()[0]
        assert snap.quarantine_until is None
        assert snap.in_use_count == 0

    @pytest.mark.asyncio
    async def test_aclose_closes_every_cached_transport(self) -> None:
        """Spec — ``aclose`` calls ``aclose`` on every cached underlying transport.

        Given the transport has built underlyings for two distinct
        proxy URLs over the lifetime of an AsyncClient,
        When ``aclose`` is awaited,
        Then both underlyings receive ``aclose`` and the cache is
        cleared so a subsequent ``aclose`` is a no-op.
        """
        routes = [
            RouteConfig(id="default", kind="direct", priority=10),
            RouteConfig(
                id="wg-pl-1",
                kind="socks5",
                proxy_url="socks5h://snapper-egress:1084",
                priority=10,
            ),
        ]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None

        factory = _RecordingFactory()
        transport = PooledAsyncTransport(transport_factory=factory)

        await transport.handle_async_request(self._make_request())
        pool._quarantine_route("default", datetime.now(UTC) + timedelta(seconds=600), "http-429")
        await transport.handle_async_request(self._make_request())

        assert len(factory.created) == 2
        await transport.aclose()
        assert all(t.aclose_called for t in factory.created)
        assert transport._transports == {}

        await transport.aclose()


class TestQuarantineConstant:
    """The connect-error quarantine duration must be a positive finite float."""

    def test_quarantine_constant_is_positive_finite(self) -> None:
        """Spec — guard against accidental zero / negative / NaN constant.

        Given the module-level constant,
        When inspected,
        Then it is a finite positive float — short enough to avoid
        wedging a recovered route out indefinitely, long enough to
        avoid hot-looping on a flapping tunnel.
        """
        assert isinstance(_HTTP_CONNECT_ERROR_QUARANTINE_S, float)
        assert _HTTP_CONNECT_ERROR_QUARANTINE_S > 0
        assert _HTTP_CONNECT_ERROR_QUARANTINE_S < 600


class TestWalutomatPublisherPattern:
    """Smoke that the ContextVar / publisher pattern is wired through to httpx requests."""

    @pytest.fixture(autouse=True)
    def _reset_pool(self) -> Any:
        reset_egress_pool()
        yield
        reset_egress_pool()

    @pytest.mark.asyncio
    async def test_publisher_get_exchange_name_via_async_mock(self) -> None:
        """Spec — an AsyncMock-style publisher mock still has its sync method consulted.

        Given a mock publisher with a synchronous ``_get_exchange_name``
        method (the real publisher's method is sync),
        And the mock is stamped into ``_CURRENT_PUBLISHER`` from an
        async context,
        When a request is handled,
        Then the sync method is called exactly once per request —
        no accidental ``await`` of a coroutine, no caching that
        would skip subsequent invocations.
        """
        routes = [RouteConfig(id="default", kind="direct", priority=0)]
        pool = configure_egress_pool(
            EgressPoolConfig(enabled=True, routes=routes, on_all_quarantined="wait")
        )
        assert pool is not None

        mock_publisher = MagicMock()
        mock_publisher._get_exchange_name = MagicMock(return_value="walutomat")
        not_awaited = AsyncMock()
        mock_publisher.unused_coroutine_attr = not_awaited

        token = _CURRENT_PUBLISHER.set(mock_publisher)
        try:
            factory = _RecordingFactory()
            transport = PooledAsyncTransport(transport_factory=factory)
            await transport.handle_async_request(httpx.Request("GET", "https://x/"))
            await transport.handle_async_request(httpx.Request("GET", "https://y/"))
        finally:
            _CURRENT_PUBLISHER.reset(token)

        assert mock_publisher._get_exchange_name.call_count == 2
        not_awaited.assert_not_awaited()
