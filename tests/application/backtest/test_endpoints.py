"""Tests for ephemeral backtest broker allocation."""

import asyncio
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.backtest.endpoints import _LiveBrokerCollisionError
from snapper.application.backtest.endpoints import allocate_replay_endpoints


@pytest.mark.asyncio
class TestAllocateReplayEndpoints:
    """allocate_replay_endpoints provides isolated per-run brokers."""

    @pytest.mark.timeout(10)
    async def test_returns_started_broker_with_resolved_endpoints(self) -> None:
        """Broker is running, endpoints are concrete, xpub_verbose is on."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            assert broker.running
            assert broker.xpub_verbose
            assert endpoints.xsub.startswith("tcp://127.0.0.1:")
            assert endpoints.xpub.startswith("tcp://127.0.0.1:")
            assert ":0" not in endpoints.xsub
            assert ":0" not in endpoints.xpub
            assert endpoints.xsub != endpoints.xpub
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_two_concurrent_runs_get_distinct_endpoints(self) -> None:
        """Two replay brokers do not collide on ephemeral allocation."""
        broker_a, ep_a = await allocate_replay_endpoints()
        broker_b, ep_b = await allocate_replay_endpoints()
        try:
            assert ep_a.xsub != ep_b.xsub
            assert ep_a.xpub != ep_b.xpub
        finally:
            await asyncio.wait_for(broker_a.stop(), timeout=2.0)
            await asyncio.wait_for(broker_b.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_collision_with_live_endpoints_is_rejected(self) -> None:
        """If the OS hands back the live xsub/xpub, allocation fails loudly."""
        probe_broker, probe_endpoints = await allocate_replay_endpoints()
        forced_xsub = probe_endpoints.xsub
        forced_xpub = probe_endpoints.xpub
        await asyncio.wait_for(probe_broker.stop(), timeout=2.0)
        fake_settings = MagicMock()
        fake_settings.zmq_broker_xsub = forced_xsub
        fake_settings.zmq_broker_xpub = forced_xpub
        stub = _LiveBrokerStub(forced_xsub, forced_xpub)
        with (
            patch(
                "snapper.application.backtest.endpoints.get_settings",
                return_value=fake_settings,
            ),
            patch(
                "snapper.application.backtest.endpoints.ZmqBrokerProcess",
                return_value=stub,
            ),
            pytest.raises(_LiveBrokerCollisionError),
        ):
            await allocate_replay_endpoints()
        assert stub.stop_called


class _LiveBrokerStub:
    """Test stub mimicking ZmqBrokerProcess for the collision test path."""

    def __init__(self, xsub: str, xpub: str) -> None:
        self.xsub_endpoint = xsub
        self.xpub_endpoint = xpub
        self.xpub_verbose = True
        self.stop_called = False

    async def start(self) -> None:
        """No-op; stub presents pre-set endpoints to trigger collision check."""

    async def stop(self) -> None:
        """Mark cleanup so the collision branch can assert the broker was stopped."""
        self.stop_called = True
