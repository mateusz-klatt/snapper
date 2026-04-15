"""Tests for ZmqBrokerProcess XPUB_VERBOSE extension.

Covers the subscription-tracking observed-set and ``wait_for_subscription``
helper used by the backtest ZMQ replay engine to confirm end-to-end SUB→
broker wiring before issuing the echo-ack handshake.
"""

import asyncio

import pytest
import zmq
import zmq.asyncio

from snapper.messaging.infrastructure.broker import ZmqBrokerProcess


async def _start_broker(*, xpub_verbose: bool) -> ZmqBrokerProcess:
    """Start a broker on ephemeral local ports."""
    broker = ZmqBrokerProcess(
        xsub_endpoint="tcp://127.0.0.1:0",
        xpub_endpoint="tcp://127.0.0.1:0",
        xpub_verbose=xpub_verbose,
    )
    await asyncio.wait_for(broker.start(), timeout=2.0)
    return broker


@pytest.mark.asyncio
class TestBrokerXpubVerbose:
    """XPUB_VERBOSE observed-set + wait_for_subscription behaviour."""

    @pytest.mark.timeout(10)
    async def test_default_verbose_off_observed_set_none(self) -> None:
        """Default constructor leaves verbose off and observed-set None."""
        broker = await _start_broker(xpub_verbose=False)
        try:
            assert broker._observed_subscriptions is None
            assert broker._observation_changed is None
            with pytest.raises(RuntimeError, match="xpub_verbose=True"):
                await broker.wait_for_subscription(b"market.", timeout=0.1)
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_subscribe_appears_in_observed_set(self) -> None:
        """SUB subscription is observed and wait_for_subscription returns fast."""
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(broker.xpub_endpoint)
        try:
            sub.setsockopt(zmq.SUBSCRIBE, b"market.foo")
            loop = asyncio.get_running_loop()
            t0 = loop.time()
            await broker.wait_for_subscription(b"market.", timeout=2.0)
            elapsed = loop.time() - t0
            assert elapsed < 0.5
            assert broker._observed_subscriptions is not None
            assert broker._observed_subscriptions.get(b"market.foo") == 1
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_no_subscription_times_out_with_observed_in_message(self) -> None:
        """Missing prefix raises TimeoutError listing the observed-set."""
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(broker.xpub_endpoint)
        try:
            sub.setsockopt(zmq.SUBSCRIBE, b"system.x")
            await broker.wait_for_subscription(b"system.", timeout=2.0)
            with pytest.raises(TimeoutError) as exc_info:
                await broker.wait_for_subscription(b"missing.", timeout=0.2)
            assert "missing." in str(exc_info.value)
            assert "observed=" in str(exc_info.value)
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_unsubscribe_removes_topic(self) -> None:
        """Unsubscribe drops the topic from the observed-set."""
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(broker.xpub_endpoint)
        try:
            sub.setsockopt(zmq.SUBSCRIBE, b"market.gone")
            await broker.wait_for_subscription(b"market.", timeout=2.0)
            sub.setsockopt(zmq.UNSUBSCRIBE, b"market.gone")
            assert broker._observed_subscriptions is not None
            for _ in range(50):
                if b"market.gone" not in broker._observed_subscriptions:
                    break
                await asyncio.sleep(0.02)
            assert b"market.gone" not in broker._observed_subscriptions
            with pytest.raises(TimeoutError):
                await broker.wait_for_subscription(b"market.", timeout=0.2)
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_many_subscriptions_no_overflow(self) -> None:
        """Many distinct subscriptions populate the observed-set without crash."""
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(broker.xpub_endpoint)
        n = 1000
        try:
            for i in range(n):
                sub.setsockopt(zmq.SUBSCRIBE, f"market.t{i}".encode())
            await broker.wait_for_subscription(f"market.t{n - 1}".encode(), timeout=3.0)
            assert broker._observed_subscriptions is not None
            assert len(broker._observed_subscriptions) == n
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_concurrent_waiters_both_resolve(self) -> None:
        """Two concurrent wait_for_subscription calls both resolve."""
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(broker.xpub_endpoint)
        try:
            waiter_a = asyncio.create_task(broker.wait_for_subscription(b"market.", timeout=3.0))
            waiter_b = asyncio.create_task(broker.wait_for_subscription(b"market.", timeout=3.0))
            await asyncio.sleep(0.05)
            sub.setsockopt(zmq.SUBSCRIBE, b"market.btc")
            await asyncio.wait_for(asyncio.gather(waiter_a, waiter_b), timeout=3.0)
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_resolve_endpoint_handles_zero_and_star_ports(self) -> None:
        """OS-assigned port placeholders are resolved to concrete tcp endpoints."""
        broker_zero = await _start_broker(xpub_verbose=False)
        try:
            assert broker_zero.xsub_endpoint != "tcp://127.0.0.1:0"
            assert broker_zero.xpub_endpoint != "tcp://127.0.0.1:0"
            assert broker_zero.xsub_endpoint.startswith("tcp://127.0.0.1:")
            assert broker_zero.xpub_endpoint.startswith("tcp://127.0.0.1:")
        finally:
            await asyncio.wait_for(broker_zero.stop(), timeout=2.0)
        broker_star = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:*",
            xpub_endpoint="tcp://127.0.0.1:*",
        )
        await asyncio.wait_for(broker_star.start(), timeout=2.0)
        try:
            assert broker_star.xsub_endpoint.startswith("tcp://127.0.0.1:")
            assert not broker_star.xsub_endpoint.endswith(":*")
        finally:
            await asyncio.wait_for(broker_star.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_resolve_endpoint_no_op_for_inproc(self) -> None:
        """inproc:// endpoints are not rewritten by LAST_ENDPOINT lookup."""
        broker = ZmqBrokerProcess(
            xsub_endpoint="inproc://xsub-resolve-test",
            xpub_endpoint="inproc://xpub-resolve-test",
        )
        await asyncio.wait_for(broker.start(), timeout=2.0)
        try:
            assert broker.xsub_endpoint == "inproc://xsub-resolve-test"
            assert broker.xpub_endpoint == "inproc://xpub-resolve-test"
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_no_lost_wakeup_on_simultaneous_subscription(self) -> None:
        """Subscription that arrives while waiter is mid-arming is not lost.

        Stresses the race where the producer notifies between a waiter's
        pre-check and its suspend point; if the broker uses set()+clear()
        on a plain Event the waiter would block past the change. With a
        Condition the producer's notify is serialized through the same lock
        the waiter holds across the check/wait boundary.
        """
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        sub = ctx.socket(zmq.SUB)
        sub.connect(broker.xpub_endpoint)
        try:

            async def waiter() -> None:
                await broker.wait_for_subscription(b"market.race", timeout=3.0)

            for _ in range(10):
                w = asyncio.create_task(waiter())
                await asyncio.sleep(0)
                sub.setsockopt(zmq.SUBSCRIBE, b"market.race")
                await asyncio.wait_for(w, timeout=3.0)
                sub.setsockopt(zmq.UNSUBSCRIBE, b"market.race")
                assert broker._observed_subscriptions is not None
                for _ in range(100):
                    if b"market.race" not in broker._observed_subscriptions:
                        break
                    await asyncio.sleep(0.01)
                assert b"market.race" not in broker._observed_subscriptions
        finally:
            sub.setsockopt(zmq.LINGER, 0)
            sub.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_data_message_passes_through_when_verbose(self) -> None:
        """Non-subscription frames flow XSUB→XPUB normally with verbose on."""
        broker = await _start_broker(xpub_verbose=True)
        ctx = zmq.asyncio.Context()
        pub = ctx.socket(zmq.PUB)
        sub = ctx.socket(zmq.SUB)
        pub.connect(broker.xsub_endpoint)
        sub.connect(broker.xpub_endpoint)
        try:
            sub.setsockopt(zmq.SUBSCRIBE, b"market.")
            await broker.wait_for_subscription(b"market.", timeout=2.0)
            await pub.send_multipart([b"market.btc", b"hello"])
            topic, payload = await asyncio.wait_for(sub.recv_multipart(), timeout=2.0)
            assert topic == b"market.btc"
            assert payload == b"hello"
        finally:
            for s in (pub, sub):
                s.setsockopt(zmq.LINGER, 0)
                s.close()
            ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)
