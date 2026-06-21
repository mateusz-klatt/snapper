"""Tests for Kraken REST egress routing and order-safety policy."""

import errno
import threading
from collections.abc import Callable
from typing import Literal
from unittest.mock import MagicMock

import ccxt
import pytest
import requests

from snapper.infrastructure.exchanges.contracts import AccountBalance
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.implementations import kraken_futures as futures_module
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import KrakenRestOperationKind
from snapper.infrastructure.exchanges.kraken_rest_egress import RestProxyTarget
from snapper.infrastructure.exchanges.kraken_rest_egress import ccxt_proxy_target
from snapper.infrastructure.exchanges.kraken_rest_egress import classify_kraken_rest_operation
from snapper.infrastructure.exchanges.kraken_rest_egress import is_provable_presend_connect_error
from snapper.infrastructure.exchanges.kraken_rest_egress import route_kraken_rest_sync_call
from snapper.infrastructure.exchanges.kraken_rest_egress import spot_sdk_proxy_target
from snapper.infrastructure.network.egress_context import current_egress_identity
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool


class _FakeCcxtClient:
    """Minimal CCXT-like client with mutable requests proxy state."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.proxies: dict[str, str] = {}
        self.load_markets: Callable[[], dict[str, object]] = lambda: {}
        self.fetch_ticker: Callable[[str], dict[str, object]] = lambda _symbol: {}
        self.fetch_balance: Callable[[], dict[str, object]] = lambda: {}
        self.create_order: Callable[..., dict[str, object]] = lambda *_args, **_kwargs: {}


class _FakeFuturesTradeClient:
    """Minimal Futures SDK-like trade client with private session fields."""

    def __init__(self) -> None:
        self._FuturesClient__session = requests.Session()
        self._FuturesClient__proxy = None
        self.create_order: Callable[..., dict[str, object]] = lambda **_kwargs: {}


class _FakeSpotSdkClient:
    """Minimal Spot SDK-like client with private session fields."""

    def __init__(self) -> None:
        self._SpotClient__session = requests.Session()
        self._SpotClient__proxy = None


class _FakeClientWithoutProxyAttribute:
    """Minimal client whose SDK proxy attribute is absent until routing applies it."""

    def __init__(self) -> None:
        self.session = requests.Session()


class _FakeClientWithoutRequestsSession:
    """Minimal client whose session attribute is not a requests session."""

    def __init__(self) -> None:
        self.session = object()
        self.proxies: dict[str, str] = {}


@pytest.fixture(autouse=True)
def _reset_egress_pool() -> None:
    """Reset the egress pool around each Kraken REST egress test."""
    reset_egress_pool()
    yield
    reset_egress_pool()


def _private_fallback_config() -> EgressPoolConfig:
    """Build the production-shaped direct, public VPN, and PL fallback pool.

    Returns:
        EgressPoolConfig with direct plus IE and PL SOCKS routes.
    """
    return EgressPoolConfig(
        enabled=True,
        private_fallback_route_id="pl",
        routes=[
            RouteConfig(id="default", kind="direct", priority=100),
            RouteConfig(
                id="ie",
                kind="socks5",
                proxy_url="socks5h://ie:1081",
                priority=10,
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


def _wrapped_presend_error() -> RuntimeError:
    """Build a wrapped pre-send connect error.

    Returns:
        RuntimeError whose cause chain proves a connect timeout.
    """
    cause = requests.exceptions.ConnectTimeout("connect timeout")
    error = RuntimeError("sdk wrapper")
    error.__cause__ = cause
    return error


def _wrapped_ccxt_presend_error() -> ccxt.NetworkError:
    """Build a CCXT network wrapper around a pre-send connect error.

    Returns:
        ccxt.NetworkError whose cause chain proves a connect timeout.
    """
    cause = requests.exceptions.ConnectTimeout("connect timeout")
    error = ccxt.NetworkError("wrapped connect timeout")
    error.__cause__ = cause
    return error


def _proxy_snapshot(session: requests.Session) -> dict[str, str]:
    """Return the session proxy mapping as a plain dict.

    Args:
        session: requests session inspected during a routed call.

    Returns:
        Current proxy mapping.
    """
    return {str(key): str(value) for key, value in session.proxies.items()}


@pytest.mark.parametrize(
    ("kind", "traffic_class", "idempotency", "fallback_allowed"),
    [
        ("public_read", "public", "idempotent_read", False),
        ("private_idempotent_read", "private", "idempotent_read", True),
        ("private_mutation", "private", "mutation", False),
    ],
)
def test_classify_kraken_rest_operation_policy(
    kind: KrakenRestOperationKind,
    traffic_class: Literal["public", "private"],
    idempotency: Literal["idempotent_read", "mutation"],
    fallback_allowed: bool,
) -> None:
    """Spec — Kraken REST operation classes encode routing and safety policy.

    Given: each supported operation kind,
    When: it is classified,
    Then: public/private traffic class, idempotency, and fallback policy match.
    """
    result = classify_kraken_rest_operation(kind, operation=f"{kind}_call")

    assert result.operation == f"{kind}_call"
    assert result.traffic_class == traffic_class
    assert result.idempotency == idempotency
    assert result.private_fallback_allowed is fallback_allowed


def test_presend_detector_is_conservative() -> None:
    """Spec — only provable pre-send connect failures are fallback-eligible.

    Given: wrapped connect timeout, OS connect errno, and generic connection errors,
    When: the pre-send detector evaluates them,
    Then: only errors that prove a failed connection setup are accepted.
    """
    assert is_provable_presend_connect_error(_wrapped_presend_error()) is True
    assert (
        is_provable_presend_connect_error(OSError(errno.ENETUNREACH, "network unreachable")) is True
    )
    assert (
        is_provable_presend_connect_error(
            requests.exceptions.ConnectionError("connection reset after send")
        )
        is False
    )


def test_public_rest_routes_through_pool_vpn_and_restores_proxy_state() -> None:
    """Spec — public Kraken REST reads reserve the public VPN route.

    Given: IE is the lowest-priority public route,
    When: a public read is routed,
    Then: the call runs with the IE proxy and the session proxy state is restored.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    client.session.proxies.update({"http": "http://before", "https": "http://before"})
    client.session.trust_env = True
    seen_proxies: list[dict[str, str]] = []
    seen_identity: list[tuple[str, str]] = []

    def sync_call() -> str:
        identity = current_egress_identity()
        assert identity is not None
        seen_identity.append((identity.exchange, identity.traffic_class))
        seen_proxies.append(_proxy_snapshot(client.session))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="fetch_ticker",
        kind="public_read",
        target=ccxt_proxy_target(client),
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert seen_identity == [("kraken", "public")]
    assert seen_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    assert client.session.proxies == {"http": "http://before", "https": "http://before"}
    assert client.session.trust_env is True


def test_private_identity_public_rest_read_runs_direct_without_pool_reservation() -> None:
    """Spec — executor-scoped public REST reads bypass the VPN pool.

    Given: a public REST call runs under an explicit private executor identity,
    When: the helper routes that public read,
    Then: the call runs direct and no egress route is reserved.
    """
    pool = configure_egress_pool(_private_fallback_config())
    assert pool is not None
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []
    seen_identity: list[tuple[str, str, str]] = []

    def sync_call() -> str:
        identity = current_egress_identity()
        assert identity is not None
        seen_identity.append((identity.exchange, identity.traffic_class, identity.owner))
        seen_proxies.append(_proxy_snapshot(client.session))
        return "ok"

    with egress_identity(
        exchange="kraken",
        traffic_class="private",
        owner="executor",
        operation="client_lifecycle",
    ):
        result = route_kraken_rest_sync_call(
            exchange="kraken",
            operation="load_markets",
            kind="public_read",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert result == "ok"
    assert seen_identity == [("kraken", "private", "executor")]
    assert seen_proxies == [{}]
    assert [
        (route.id, route.in_use_count, route.active_reservations)
        for route in pool.status_snapshot().routes
    ] == [
        ("default", 0, []),
        ("ie", 0, []),
        ("pl", 0, []),
    ]


def test_public_rest_without_pool_runs_direct_with_public_identity() -> None:
    """Spec — public reads stay public and direct when the pool is disabled.

    Given: no egress pool singleton is configured,
    When: a public REST read is routed,
    Then: the call runs without a proxy but still carries public identity.
    """
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []
    seen_identity: list[tuple[str, str]] = []

    def sync_call() -> str:
        identity = current_egress_identity()
        assert identity is not None
        seen_identity.append((identity.exchange, identity.traffic_class))
        seen_proxies.append(_proxy_snapshot(client.session))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="fetch_ticker",
        kind="public_read",
        target=ccxt_proxy_target(client),
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert seen_identity == [("kraken", "public")]
    assert seen_proxies == [{}]


@pytest.mark.asyncio
async def test_spot_connect_load_markets_under_private_identity_never_reserves_pool() -> None:
    """Spec — executor-scoped Spot connect cannot depend on the VPN pool.

    Given: Kraken Spot ``connect`` is called under executor private identity,
    When: ``load_markets`` runs through the public-read classification,
    Then: the load runs direct, observes the executor identity, and leaves
        every pool route unreserved.
    """
    pool = configure_egress_pool(_private_fallback_config())
    assert pool is not None
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    fake_ccxt = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []
    seen_identity: list[tuple[str, str, str]] = []

    def load_markets() -> dict[str, object]:
        identity = current_egress_identity()
        assert identity is not None
        seen_identity.append((identity.exchange, identity.traffic_class, identity.owner))
        seen_proxies.append(_proxy_snapshot(fake_ccxt.session))
        return {}

    fake_ccxt.load_markets = MagicMock(side_effect=load_markets)
    client._ccxt_client = fake_ccxt
    try:
        with egress_identity(
            exchange="kraken",
            traffic_class="private",
            owner="executor",
            operation="client_lifecycle",
        ):
            await client.connect()
    finally:
        client._shutdown_rest_pool()

    assert seen_identity == [("kraken", "private", "executor")]
    assert seen_proxies == [{}]
    assert [
        (route.id, route.in_use_count, route.active_reservations)
        for route in pool.status_snapshot().routes
    ] == [
        ("default", 0, []),
        ("ie", 0, []),
        ("pl", 0, []),
    ]
    fake_ccxt.load_markets.assert_called_once_with()


def test_public_rest_presend_connect_error_quarantines_and_releases_route() -> None:
    """Spec — public REST connect failures quarantine only the selected route.

    Given: a public read selected the IE route,
    When: the blocking call proves a pre-send connect failure,
    Then: IE is briefly quarantined and the active reservation is released.
    """
    pool = configure_egress_pool(_private_fallback_config())
    assert pool is not None
    client = _FakeCcxtClient()

    def sync_call() -> str:
        raise _wrapped_presend_error()

    with pytest.raises(RuntimeError, match="sdk wrapper"):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="fetch_ticker",
            kind="public_read",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    snapshot = pool.status_snapshot()
    assert snapshot.routes[1].id == "ie"
    assert snapshot.routes[1].quarantined is True
    assert snapshot.routes[1].in_use_count == 0
    assert snapshot.routes[1].active_reservations == []


def test_public_rest_ambiguous_error_releases_without_quarantine() -> None:
    """Spec — ambiguous public REST errors do not poison a VPN route.

    Given: a public read selected the IE route,
    When: the blocking call raises a generic connection error,
    Then: the route reservation is released without a connect quarantine.
    """
    pool = configure_egress_pool(_private_fallback_config())
    assert pool is not None
    client = _FakeCcxtClient()

    def sync_call() -> str:
        raise requests.exceptions.ConnectionError("connection reset after send")

    with pytest.raises(requests.exceptions.ConnectionError):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="fetch_ticker",
            kind="public_read",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    snapshot = pool.status_snapshot()
    assert snapshot.routes[1].id == "ie"
    assert snapshot.routes[1].quarantined is False
    assert snapshot.routes[1].in_use_count == 0
    assert snapshot.routes[1].active_reservations == []


def test_private_idempotent_read_falls_back_once_after_presend_connect_error() -> None:
    """Spec — private reads use direct first and PL only after a proven pre-send error.

    Given: direct then PL private routes,
    When: the direct read raises a wrapped connect timeout,
    Then: exactly one fallback attempt runs through PL.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_proxies.append(_proxy_snapshot(client.session))
        if len(seen_proxies) == 1:
            raise _wrapped_presend_error()
        return "read-ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="fetch_balance",
        kind="private_idempotent_read",
        target=ccxt_proxy_target(client),
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "read-ok"
    assert seen_proxies == [
        {},
        {"http": "socks5h://pl:1084", "https": "socks5h://pl:1084"},
    ]


def test_private_identity_private_read_preserves_executor_identity() -> None:
    """Spec — private read routing preserves the active executor identity.

    Given: a private idempotent read runs under executor private identity,
    When: the helper routes the call,
    Then: the call remains direct and sees the executor identity.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    seen_identity: list[tuple[str, str, str]] = []
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        identity = current_egress_identity()
        assert identity is not None
        seen_identity.append((identity.exchange, identity.traffic_class, identity.owner))
        seen_proxies.append(_proxy_snapshot(client.session))
        return "read-ok"

    with egress_identity(
        exchange="kraken",
        traffic_class="private",
        owner="executor",
        operation="client_lifecycle",
    ):
        result = route_kraken_rest_sync_call(
            exchange="kraken",
            operation="fetch_balance",
            kind="private_idempotent_read",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert result == "read-ok"
    assert seen_identity == [("kraken", "private", "executor")]
    assert seen_proxies == [{}]


def test_private_idempotent_read_ambiguous_error_does_not_fallback() -> None:
    """Spec — private read fallback is denied for ambiguous transport errors.

    Given: PL fallback is configured,
    When: the direct read raises a generic connection error,
    Then: the error is re-raised after one direct attempt with no fallback.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_proxies.append(_proxy_snapshot(client.session))
        raise requests.exceptions.ConnectionError("connection reset after send")

    with pytest.raises(requests.exceptions.ConnectionError):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="fetch_balance",
            kind="private_idempotent_read",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert seen_proxies == [{}]


def test_private_idempotent_read_presend_error_without_pool_has_no_fallback() -> None:
    """Spec — private read fallback is not invented when no pool is configured.

    Given: the egress pool singleton is absent,
    When: a private read raises a pre-send connect error,
    Then: the original direct failure is re-raised after a single attempt.
    """
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_proxies.append(_proxy_snapshot(client.session))
        raise _wrapped_presend_error()

    with pytest.raises(RuntimeError, match="sdk wrapper"):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="fetch_balance",
            kind="private_idempotent_read",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert seen_proxies == [{}]


def test_private_mutation_presend_error_is_not_retried_or_rerouted() -> None:
    """Spec — private mutations do not use fallback even for pre-send errors.

    Given: PL is configured as private fallback,
    When: a mutation raises a wrapped connect timeout,
    Then: the mutation is attempted once on direct and the error propagates.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_proxies.append(_proxy_snapshot(client.session))
        raise _wrapped_presend_error()

    with pytest.raises(RuntimeError, match="sdk wrapper"):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="create_order",
            kind="private_mutation",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert seen_proxies == [{}]


def test_private_identity_mutation_preserves_no_retry_safety() -> None:
    """Spec — executor-scoped mutations remain direct single-attempt failures.

    Given: a private mutation runs under executor private identity,
    When: the mutation raises a pre-send connect error,
    Then: the error propagates after one direct attempt with no fallback.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    seen_identity: list[tuple[str, str, str]] = []
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        identity = current_egress_identity()
        assert identity is not None
        seen_identity.append((identity.exchange, identity.traffic_class, identity.owner))
        seen_proxies.append(_proxy_snapshot(client.session))
        raise _wrapped_presend_error()

    with (
        egress_identity(
            exchange="kraken",
            traffic_class="private",
            owner="executor",
            operation="client_lifecycle",
        ),
        pytest.raises(RuntimeError, match="sdk wrapper"),
    ):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="create_order",
            kind="private_mutation",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert seen_identity == [("kraken", "private", "executor")]
    assert seen_proxies == [{}]


def test_private_mutation_ambiguous_connection_error_is_not_retried_or_rerouted() -> None:
    """Spec — ambiguous mutation network errors remain single-attempt direct failures.

    Given: a mutation raises a generic connection error,
    When: the helper routes it,
    Then: no PL fallback is attempted because the send may have happened.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_proxies.append(_proxy_snapshot(client.session))
        raise requests.exceptions.ConnectionError("connection reset after send")

    with pytest.raises(requests.exceptions.ConnectionError):
        route_kraken_rest_sync_call(
            exchange="kraken",
            operation="cancel_order",
            kind="private_mutation",
            target=ccxt_proxy_target(client),
            proxy_lock=threading.RLock(),
            sync_call=sync_call,
        )

    assert seen_proxies == [{}]


def test_proxy_target_without_sdk_proxy_attr_restores_session_state() -> None:
    """Spec — clients without an SDK proxy attr still route through session state.

    Given: a target exposes only a requests session and a non-bool trust_env value,
    When: a public read is routed through the VPN,
    Then: session proxies are scoped and the original trust_env value is preserved.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    client.session.proxies.update({"http": "http://before"})
    object.__setattr__(client.session, "trust_env", "manual")
    target = RestProxyTarget(
        client=client,
        session_attr="session",
        proxy_attr=None,
        proxy_attr_shape="mapping",
    )
    seen_proxies: list[dict[str, str]] = []
    seen_trust_env: list[object] = []

    def sync_call() -> str:
        seen_proxies.append(_proxy_snapshot(client.session))
        seen_trust_env.append(getattr(client.session, "trust_env"))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="fetch_ticker",
        kind="public_read",
        target=target,
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert seen_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    assert seen_trust_env == ["manual"]
    assert client.session.proxies == {"http": "http://before"}
    assert getattr(client.session, "trust_env") == "manual"


def test_proxy_target_with_non_mapping_session_proxies_uses_sdk_proxy_only() -> None:
    """Spec — malformed session proxy state cannot block SDK-level proxy scoping.

    Given: a client session exposes a non-mapping proxies attribute,
    When: a public read is routed through the VPN,
    Then: the SDK proxy attribute is scoped and restored without touching the session.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeCcxtClient()
    object.__setattr__(client.session, "proxies", ())
    seen_sdk_proxies: list[dict[str, str]] = []
    seen_session_proxies: list[object] = []

    def sync_call() -> str:
        seen_sdk_proxies.append(dict(client.proxies))
        seen_session_proxies.append(getattr(client.session, "proxies"))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="fetch_ticker",
        kind="public_read",
        target=ccxt_proxy_target(client),
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert seen_sdk_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    assert seen_session_proxies == [()]
    assert client.proxies == {}
    assert getattr(client.session, "proxies") == ()


def test_proxy_target_restores_absent_sdk_proxy_attribute() -> None:
    """Spec — routing does not leave new proxy attributes on SDK clients.

    Given: a client had no SDK-level proxy attribute before routing,
    When: a direct private mutation is scoped,
    Then: the temporary proxy attribute is removed after the call.
    """
    client = _FakeClientWithoutProxyAttribute()
    target = RestProxyTarget(
        client=client,
        session_attr="session",
        proxy_attr="proxies",
        proxy_attr_shape="mapping",
    )
    had_proxy_attr_during_call: list[bool] = []

    def sync_call() -> str:
        had_proxy_attr_during_call.append(hasattr(client, "proxies"))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="create_order",
        kind="private_mutation",
        target=target,
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert had_proxy_attr_during_call == [True]
    assert not hasattr(client, "proxies")


def test_spot_sdk_proxy_target_scopes_url_proxy_and_session_proxy() -> None:
    """Spec — Spot SDK clients receive URL-shaped SDK proxy values.

    Given: the Spot SDK uses a private URL proxy field and requests session,
    When: a public REST read is routed through the VPN,
    Then: both proxy surfaces are set for the call and restored afterward.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeSpotSdkClient()
    session = getattr(client, "_SpotClient__session")
    assert isinstance(session, requests.Session)
    seen_sdk_proxy: list[object] = []
    seen_session_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_sdk_proxy.append(getattr(client, "_SpotClient__proxy"))
        seen_session_proxies.append(_proxy_snapshot(session))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="get_orders_info",
        kind="public_read",
        target=spot_sdk_proxy_target(client),
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert seen_sdk_proxy == ["socks5h://ie:1081"]
    assert seen_session_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    assert getattr(client, "_SpotClient__proxy") is None
    assert session.proxies == {}


def test_proxy_target_without_requests_session_scopes_sdk_proxy_only() -> None:
    """Spec — missing requests session does not block SDK proxy scoping.

    Given: a client exposes an SDK proxy field but no requests session,
    When: a public REST read is routed through the VPN,
    Then: the SDK proxy is scoped and restored without session mutation.
    """
    configure_egress_pool(_private_fallback_config())
    client = _FakeClientWithoutRequestsSession()
    seen_sdk_proxies: list[dict[str, str]] = []

    def sync_call() -> str:
        seen_sdk_proxies.append(dict(client.proxies))
        return "ok"

    result = route_kraken_rest_sync_call(
        exchange="kraken",
        operation="fetch_ticker",
        kind="public_read",
        target=ccxt_proxy_target(client),
        proxy_lock=threading.RLock(),
        sync_call=sync_call,
    )

    assert result == "ok"
    assert seen_sdk_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    assert client.proxies == {}


@pytest.mark.asyncio
async def test_spot_public_get_ticker_routes_via_public_pool_vpn() -> None:
    """Spec — Spot public ticker REST uses the public VPN selector.

    Given: a Spot client with a synchronous CCXT ticker method,
    When: get_ticker is called,
    Then: the CCXT call runs through the IE public proxy.
    """
    configure_egress_pool(_private_fallback_config())
    client = KrakenExchangeClient()
    fake_ccxt = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def fetch_ticker(symbol: str) -> dict[str, object]:
        seen_proxies.append(_proxy_snapshot(fake_ccxt.session))
        return {
            "symbol": symbol,
            "bid": 1.0,
            "ask": 2.0,
            "last": 1.5,
            "timestamp": 1_000.0,
        }

    fake_ccxt.fetch_ticker = MagicMock(side_effect=fetch_ticker)
    client._ccxt_client = fake_ccxt
    try:
        ticker = await client.get_ticker("BTC-USD")
    finally:
        client._shutdown_rest_pool()

    assert ticker.symbol == "BTC-USD"
    assert seen_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    fake_ccxt.fetch_ticker.assert_called_once_with("BTC/USD")


@pytest.mark.asyncio
async def test_spot_private_balance_read_uses_direct_then_pl_presend_fallback() -> None:
    """Spec — Spot private balance read gets one PL fallback after direct pre-send failure.

    Given: fetch_balance fails once with a wrapped connect timeout,
    When: get_balance is called,
    Then: it attempts direct first and PL second.
    """
    configure_egress_pool(_private_fallback_config())
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    fake_ccxt = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def fetch_balance() -> dict[str, object]:
        seen_proxies.append(_proxy_snapshot(fake_ccxt.session))
        if len(seen_proxies) == 1:
            raise _wrapped_ccxt_presend_error()
        return {"USD": {"free": 1.0, "used": 2.0, "total": 3.0}}

    fake_ccxt.fetch_balance = MagicMock(side_effect=fetch_balance)
    client._ccxt_client = fake_ccxt
    try:
        balances = await client.get_balance("USD")
    finally:
        client._shutdown_rest_pool()

    assert balances == {"USD": AccountBalance(currency="USD", free=1.0, used=2.0, total=3.0)}
    assert seen_proxies == [
        {},
        {"http": "socks5h://pl:1084", "https": "socks5h://pl:1084"},
    ]
    assert fake_ccxt.fetch_balance.call_count == 2


@pytest.mark.asyncio
async def test_spot_order_mutation_network_error_attempts_once_direct() -> None:
    """Spec — Spot create_order never retries or reroutes mutation network errors.

    Given: create_order raises a CCXT network error with a pre-send cause,
    When: create_order is called,
    Then: the existing ambiguous-order path receives one direct attempt only.
    """
    configure_egress_pool(_private_fallback_config())
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    fake_ccxt = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def create_order(*_args: object, **_kwargs: object) -> dict[str, object]:
        seen_proxies.append(_proxy_snapshot(fake_ccxt.session))
        raise _wrapped_ccxt_presend_error()

    fake_ccxt.create_order = MagicMock(side_effect=create_order)
    client._ccxt_client = fake_ccxt
    request = ExchangeOrderRequest(
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.MARKET,
        amount=0.1,
        client_order_id="client-safe-1",
    )
    try:
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(request)
    finally:
        client._shutdown_rest_pool()

    assert seen_proxies == [{}]
    assert fake_ccxt.create_order.call_count == 1


@pytest.mark.asyncio
async def test_futures_public_get_ticker_routes_via_public_pool_vpn() -> None:
    """Spec — Futures public ticker REST uses the public VPN selector.

    Given: a Futures client with a synchronous CCXT ticker method,
    When: get_ticker is called,
    Then: the CCXT call runs through the IE public proxy.
    """
    configure_egress_pool(_private_fallback_config())
    client = KrakenFuturesExchangeClient(sandbox=True)
    fake_ccxt = _FakeCcxtClient()
    seen_proxies: list[dict[str, str]] = []

    def fetch_ticker(symbol: str) -> dict[str, object]:
        seen_proxies.append(_proxy_snapshot(fake_ccxt.session))
        return {"symbol": symbol, "bid": 1.0, "ask": 2.0, "last": 1.5, "timestamp": 1_000.0}

    fake_ccxt.fetch_ticker = MagicMock(side_effect=fetch_ticker)
    client._ccxt_client = fake_ccxt
    try:
        ticker = await client.get_ticker("PF_XBTUSD")
    finally:
        client._shutdown_rest_pool()

    assert ticker.symbol == "PF_XBTUSD"
    assert seen_proxies == [{"http": "socks5h://ie:1081", "https": "socks5h://ie:1081"}]
    fake_ccxt.fetch_ticker.assert_called_once_with("PF_XBTUSD")


@pytest.mark.asyncio
async def test_futures_order_mutation_network_error_attempts_once_direct(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec — Futures create_order never retries or reroutes mutation network errors.

    Given: the native Futures trade client raises a transport error,
    When: create_order is called,
    Then: the existing ambiguous-order path receives one direct attempt only.
    """
    configure_egress_pool(_private_fallback_config())
    client = KrakenFuturesExchangeClient(
        sandbox=True,
        api_key="key",
        api_secret="secret",
    )
    trade_client = _FakeFuturesTradeClient()
    session = getattr(trade_client, "_FuturesClient__session")
    assert isinstance(session, requests.Session)
    seen_proxies: list[dict[str, str]] = []

    def create_order(**_kwargs: object) -> dict[str, object]:
        seen_proxies.append(_proxy_snapshot(session))
        raise requests.exceptions.ConnectionError("connection reset after send")

    trade_client.create_order = MagicMock(side_effect=create_order)
    client._trade_client = trade_client
    monkeypatch.setattr(
        futures_module,
        "native_to_kraken_futures_ws",
        lambda _symbol: "PF_XBTUSD",
    )
    request = ExchangeOrderRequest(
        symbol="BTC-USD-PERP",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.MARKET,
        amount=1.0,
        client_order_id="client-safe-fut-1",
    )
    try:
        with pytest.raises(AmbiguousOrderSubmitError):
            await client.create_order(request)
    finally:
        client._shutdown_rest_pool()

    assert seen_proxies == [{}]
    assert trade_client.create_order.call_count == 1
