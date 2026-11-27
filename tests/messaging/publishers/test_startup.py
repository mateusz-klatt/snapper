"""Regression tests for in-process market-data publisher connection retries."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.network.egress_exceptions import AllRoutesQuarantinedError
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.messaging.publishers import startup


@pytest.mark.parametrize("deadline, expected", [(None, 1.0), (0.25, 1.0), (12.0, 12.0)])
async def test_publisher_startup_retries_public_refusal_in_process(
    deadline: float | None, expected: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A typed refusal waits and retries the same client without a process exit."""
    client = MagicMock(spec=ExchangeClientBase)
    client.connect.side_effect = [AllRoutesQuarantinedError("no VPN"), None]
    pool = MagicMock(spec=EgressPool)
    pool.earliest_release_in_seconds.return_value = deadline
    sleep = AsyncMock()
    monkeypatch.setattr(startup, "get_egress_pool", lambda: pool)
    monkeypatch.setattr(startup.asyncio, "sleep", sleep)

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
    """A private policy refusal retains its existing propagation behavior."""
    client = MagicMock(spec=ExchangeClientBase)
    client.connect.side_effect = AllRoutesQuarantinedError("no VPN")
    with (
        egress_identity(
            exchange="kraken", traffic_class="private", owner="test", operation="startup"
        ),
        pytest.raises(AllRoutesQuarantinedError, match="no VPN"),
    ):
        await startup.connect_publisher_exchange(client)
