"""Regression tests for in-process market-data publisher connection retries."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.implementations import kraken_equities
from snapper.infrastructure.network import egress_pool
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.network.egress_exceptions import AllRoutesQuarantinedError
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.messaging.publishers import startup


@pytest.mark.parametrize("deadline, expected", [(None, 1.0), (0.25, 1.0), (12.0, 12.0)])
async def test_publisher_startup_retries_public_refusal_in_process(
    deadline: float | None, expected: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A typed refusal waits and retries the same client without a process exit.

    Given: A publisher's first public connect attempt raises AllRoutesQuarantinedError,
    When: The startup helper handles the refusal,
    Then: It waits for the public release deadline with a one-second floor and
        retries the same client in process.
    """
    client = MagicMock(spec=ExchangeClientBase)
    client.exchange_name = "kraken_futures"
    client.connect.side_effect = [AllRoutesQuarantinedError("no VPN"), None]
    pool = MagicMock(spec=EgressPool)
    pool.has_available.side_effect = [False, True]
    pool.earliest_release_in_seconds.return_value = deadline
    sleep = AsyncMock()
    monkeypatch.setattr(egress_pool, "get_egress_pool", lambda: pool)
    monkeypatch.setattr(egress_pool.asyncio, "sleep", sleep)

    with egress_identity(
        exchange="kraken_futures", traffic_class="public", owner="test", operation="startup"
    ):
        await startup.connect_publisher_exchange(client)

    assert client.connect.await_count == 2
    pool.earliest_release_in_seconds.assert_called_once_with(
        exchange="kraken_futures", traffic_class="public"
    )
    sleep.assert_awaited_once_with(expected)


async def test_publisher_startup_keeps_private_refusal_outside_public_wait() -> None:
    """A private policy refusal retains its existing propagation behavior.

    Given: A connection attempt scoped to private traffic that raises a route refusal,
    When: The startup helper handles the refusal,
    Then: The original exception propagates without a public egress retry.
    """
    client = MagicMock(spec=ExchangeClientBase)
    client.exchange_name = "kraken"
    client.connect.side_effect = AllRoutesQuarantinedError("no VPN")
    with (
        egress_identity(
            exchange="kraken", traffic_class="private", owner="test", operation="startup"
        ),
        pytest.raises(AllRoutesQuarantinedError, match="no VPN"),
    ):
        await startup.connect_publisher_exchange(client)


async def test_equities_feed_waits_through_preflight_quarantine_and_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same client waits thirty seconds and opens one WS with no teardown.

    Given: An Equities client starting with its only public VPN in preflight quarantine,
    When: Connection startup waits for the 30-second quarantine to expire,
    Then: The SDK is created and started once after release, without a teardown.
    """
    initial = datetime(2026, 11, 27, tzinfo=UTC)
    clock = MagicMock(wraps=datetime)
    clock.now.return_value = initial
    monkeypatch.setattr(egress_pool, "datetime", clock)
    monkeypatch.setattr(egress_pool, "_POOL_HOLDER", [None])
    pool = egress_pool.configure_egress_pool(
        EgressPoolConfig(
            enabled=True,
            routes=[
                RouteConfig(id="direct", kind="direct"),
                RouteConfig(id="vpn", kind="socks5", proxy_url="socks5h://egress:1081"),
            ],
        ),
        initial_quarantine_route_ids=["vpn"],
    )
    assert pool is not None
    release = initial + timedelta(seconds=30)
    sdk = MagicMock(start=AsyncMock(), close=AsyncMock())
    factory = MagicMock(return_value=sdk)
    monkeypatch.setattr(kraken_equities, "SpotWSClient", factory)
    client = kraken_equities.KrakenEquitiesExchangeClient()

    async def advance(seconds: float) -> None:
        """Simulate the quarantine expiring before SDK startup."""
        assert seconds == 30.0
        assert pool.is_waiting_for_public_vpn("kraken_equities")
        factory.assert_not_called()
        assert client._ws_connection_generation == 0
        clock.now.return_value = release

    sleep = AsyncMock(side_effect=advance)
    monkeypatch.setattr(egress_pool.asyncio, "sleep", sleep)
    await startup.connect_publisher_exchange(client)
    await client._ensure_ws_connected()

    sleep.assert_awaited_once_with(30.0)
    factory.assert_called_once()
    sdk.start.assert_awaited_once()
    sdk.close.assert_not_awaited()
    assert not pool.is_waiting_for_public_vpn("kraken_equities")
