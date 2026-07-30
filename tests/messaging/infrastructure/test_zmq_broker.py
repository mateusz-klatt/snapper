"""Tests for ZMQ message broker implementations."""

import asyncio
import threading
import time
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import zmq
import zmq.asyncio
from loguru import logger as loguru_logger

from snapper.config.settings import get_settings
from snapper.messaging.infrastructure.broker import ZmqBrokerProcess
from snapper.messaging.infrastructure.broker import ZmqBrokerThread
from snapper.messaging.schemas.data import TickData


@pytest.mark.asyncio
class TestZMQBroker:
    """Tests for ZMQ broker process functionality."""

    @pytest.mark.timeout(15)
    async def test_broker_start_stop(self) -> None:
        """Test ZmqBrokerProcess start and stop lifecycle.

        Given: A stopped ZmqBrokerProcess,
        When: Started and then stopped,
        Then: Running state toggles appropriately.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7800", xpub_endpoint="tcp://127.0.0.1:7801"
        )
        assert not broker.running
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            assert broker.running
            await asyncio.wait_for(broker.start(), timeout=2.0)
            assert broker.running
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)
            assert not broker.running
        await asyncio.wait_for(broker.stop(), timeout=2.0)
        assert not broker.running

    @pytest.mark.timeout(10)
    async def test_broker_get_status(self) -> None:
        """Test ZmqBrokerProcess get_status.

        Given: A ZmqBrokerProcess,
        When: get_status is called,
        Then: Returns dict with running state and endpoints.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7802", xpub_endpoint="tcp://127.0.0.1:7803"
        )
        status = broker.get_status()
        assert status["running"] is False
        assert status["xsub_endpoint"] == "tcp://127.0.0.1:7802"
        assert status["xpub_endpoint"] == "tcp://127.0.0.1:7803"
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            status = broker.get_status()
            assert status["running"] is True
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(20)
    async def test_broker_message_forwarding(self) -> None:
        """Test ZmqBrokerProcess forwards messages.

        Given: A running broker with publisher and subscriber,
        When: Publisher sends message to XSUB,
        Then: Subscriber receives message from XPUB.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7804", xpub_endpoint="tcp://127.0.0.1:7805"
        )
        context = None
        pub_socket = None
        sub_socket = None
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            await asyncio.sleep(0.1)
            context = zmq.asyncio.Context()
            pub_socket = context.socket(zmq.PUB)
            pub_socket.connect("tcp://127.0.0.1:7804")
            sub_socket = context.socket(zmq.SUB)
            sub_socket.connect("tcp://127.0.0.1:7805")
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "TEST")
            await asyncio.sleep(0.1)
            try:
                await asyncio.wait_for(
                    pub_socket.send_multipart([b"TEST", b"hello world"]), timeout=1.0
                )
                topic, payload = await asyncio.wait_for(sub_socket.recv_multipart(), timeout=2.0)
                assert topic == b"TEST"
                assert payload == b"hello world"
            except TimeoutError:
                pytest.fail("Message not forwarded through broker")
        finally:
            if pub_socket:
                pub_socket.close()
            if sub_socket:
                sub_socket.close()
            if context:
                context.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_broker_proxy_error_handling(self) -> None:
        """Test broker handles proxy errors gracefully.

        Given: A running broker,
        When: Started and stopped,
        Then: No proxy errors cause crash.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7806", xpub_endpoint="tcp://127.0.0.1:7807"
        )
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            assert broker.running
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)


class TestZMQBrokerSync:
    """Tests for synchronous ZMQ broker thread functionality."""

    def test_sync_broker_start_stop(self) -> None:
        """Test ZmqBrokerThread synchronous start and stop.

        Given: A stopped ZmqBrokerThread,
        When: Started and stopped synchronously,
        Then: Running state toggles appropriately.
        """
        broker = ZmqBrokerThread(
            xsub_endpoint="tcp://127.0.0.1:7808", xpub_endpoint="tcp://127.0.0.1:7809"
        )
        assert not broker.running
        try:
            broker.start()
            assert broker.running
            broker.start()
            assert broker.running
        finally:
            broker.stop()
            assert not broker.running
        broker.stop()
        assert not broker.running

    def test_sync_broker_get_status(self) -> None:
        """Test ZmqBrokerThread get_status.

        Given: A ZmqBrokerThread,
        When: get_status is called,
        Then: Returns BrokerStatus with running state and endpoints.
        """
        broker = ZmqBrokerThread(
            xsub_endpoint="tcp://127.0.0.1:7810", xpub_endpoint="tcp://127.0.0.1:7811"
        )
        status = broker.get_status()
        assert status.running is False
        assert status.xsub_endpoint == "tcp://127.0.0.1:7810"
        assert status.xpub_endpoint == "tcp://127.0.0.1:7811"
        try:
            broker.start()
            status = broker.get_status()
            assert status.running is True
        finally:
            broker.stop()

    def test_sync_broker_proxy_functionality(self) -> None:
        """Test ZmqBrokerThread proxy thread is alive.

        Given: A running ZmqBrokerThread,
        When: Proxy thread is accessed,
        Then: Thread exists and is alive.
        """
        broker = ZmqBrokerThread(
            xsub_endpoint="tcp://127.0.0.1:7812", xpub_endpoint="tcp://127.0.0.1:7813"
        )
        try:
            broker.start()
            time.sleep(0.1)
            assert broker.proxy_thread is not None
            assert broker.proxy_thread.is_alive()
        finally:
            broker.stop()


@pytest.mark.asyncio
class TestZMQBrokerAdditionalCoverage:
    """Additional coverage tests for ZMQ broker."""

    @pytest.mark.timeout(15)
    async def test_get_default_parameters_from_settings(self) -> None:
        """Test get_default_parameters returns settings values.

        Given: Application settings,
        When: get_default_parameters is called,
        Then: Returns xsub and xpub endpoints from settings.
        """
        settings = get_settings()
        kwargs = ZmqBrokerProcess.get_default_parameters(settings)
        assert "xsub_endpoint" in kwargs
        assert "xpub_endpoint" in kwargs
        assert kwargs["xsub_endpoint"] == settings.zmq_broker_bind_xsub
        assert kwargs["xpub_endpoint"] == settings.zmq_broker_bind_xpub

    @pytest.mark.timeout(15)
    async def test_get_default_parameters_uses_bind_endpoints(self) -> None:
        """Broker binds the dedicated bind endpoints, not the connect ones.

        Given: settings whose bind endpoints differ from the connect
            endpoints (the cross-container case),
        When: get_default_parameters is called,
        Then: the broker is configured to bind the bind endpoints
            (``tcp://0.0.0.0:*``), distinct from the hostname connectors
            target.
        """
        settings = SimpleNamespace(
            zmq_broker_bind_xsub="tcp://0.0.0.0:7500",
            zmq_broker_bind_xpub="tcp://0.0.0.0:7501",
            zmq_broker_xsub="tcp://snapper:7500",
            zmq_broker_xpub="tcp://snapper:7501",
        )
        kwargs = ZmqBrokerProcess.get_default_parameters(cast(Any, settings))
        assert kwargs["xsub_endpoint"] == "tcp://0.0.0.0:7500"
        assert kwargs["xpub_endpoint"] == "tcp://0.0.0.0:7501"

    @pytest.mark.timeout(15)
    async def test_broker_cleanup_with_none_sockets(self) -> None:
        """Test broker stop with uninitialized sockets.

        Given: A broker that was never started,
        When: Stop is called,
        Then: No error occurs.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7820", xpub_endpoint="tcp://127.0.0.1:7821"
        )
        await broker.stop()
        assert not broker.running

    @pytest.mark.timeout(15)
    async def test_broker_proxy_loop_timeout_handling(self) -> None:
        """Test broker proxy loop handles timeouts.

        Given: A running broker,
        When: Proxy loop experiences timeout,
        Then: Broker continues running.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:0",
            xpub_endpoint="tcp://127.0.0.1:0",
        )
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            await asyncio.sleep(1.5)
            assert broker.running
        finally:
            await broker.stop()

    @pytest.mark.timeout(15)
    async def test_broker_poll_sockets(self) -> None:
        """Test broker polls sockets correctly.

        Given: A running broker,
        When: Sockets are polled,
        Then: Broker remains running.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7824", xpub_endpoint="tcp://127.0.0.1:7825"
        )
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            await asyncio.sleep(0.2)
            assert broker.running
        finally:
            await broker.stop()

    @pytest.mark.timeout(15)
    async def test_broker_zmq_again_exception_handling(self) -> None:
        """Test broker handles zmq.Again exception.

        Given: A running broker,
        When: ZMQ raises Again exception,
        Then: Broker continues running.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7826", xpub_endpoint="tcp://127.0.0.1:7827"
        )
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            await asyncio.sleep(0.5)
            assert broker.running
        finally:
            await broker.stop()

    @pytest.mark.timeout(15)
    async def test_broker_get_status(self) -> None:
        """Test broker status before and after start.

        Given: A broker,
        When: Status checked before and after start,
        Then: Running state reflects actual state.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7828", xpub_endpoint="tcp://127.0.0.1:7829"
        )
        status = broker.get_status()
        assert status["running"] is False
        assert status["xsub_endpoint"] == "tcp://127.0.0.1:7828"
        assert status["xpub_endpoint"] == "tcp://127.0.0.1:7829"
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            status = broker.get_status()
            assert status["running"] is True
        finally:
            await broker.stop()

    @pytest.mark.timeout(15)
    async def test_broker_double_stop(self) -> None:
        """Test broker handles double stop.

        Given: A running broker,
        When: Stop is called twice,
        Then: No error occurs and broker is stopped.
        """
        broker = ZmqBrokerProcess(
            xsub_endpoint="tcp://127.0.0.1:7830", xpub_endpoint="tcp://127.0.0.1:7831"
        )
        try:
            await asyncio.wait_for(broker.start(), timeout=2.0)
            await broker.stop()
            await broker.stop()
            assert not broker.running
        except TimeoutError:
            pass


@pytest.mark.asyncio
class TestZMQPubSub:
    """Tests for ZMQ pub/sub pattern functionality."""

    @pytest.mark.timeout(15)
    async def test_basic_pub_sub(self) -> None:
        """Test basic ZMQ pub/sub pattern.

        Given: A publisher and subscriber connected directly,
        When: Publisher sends message on subscribed topic,
        Then: Subscriber receives the message.
        """
        context = zmq.asyncio.Context()
        try:
            pub_socket = context.socket(zmq.PUB)
            pub_socket.setsockopt(zmq.LINGER, 0)
            pub_port = pub_socket.bind_to_random_port("tcp://127.0.0.1")
            pub_endpoint = f"tcp://127.0.0.1:{pub_port}"
            sub_socket = context.socket(zmq.SUB)
            sub_socket.setsockopt(zmq.LINGER, 0)
            sub_socket.connect(pub_endpoint)
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "BTCUSD")
            await asyncio.sleep(0.1)
            test_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="BTCUSD",
                exchange="kraken",
                volume=0.1,
                last=50000.0,
            )
            await pub_socket.send_multipart([b"BTCUSD", test_msg.to_json().encode("utf-8")])
            try:
                topic_bytes, payload_bytes = await asyncio.wait_for(
                    sub_socket.recv_multipart(), timeout=1.0
                )
                topic = topic_bytes.decode("utf-8")
                payload = payload_bytes.decode("utf-8")
                assert topic == "BTCUSD"
                received_msg = TickData.from_json(payload)
                assert isinstance(received_msg, TickData)
                assert received_msg.instrument == "BTCUSD"
                assert received_msg.last == pytest.approx(50000.0)
                assert received_msg.volume == pytest.approx(0.1)
            except TimeoutError:
                pytest.fail("Did not receive message within timeout")
        finally:
            pub_socket.close()
            sub_socket.close()
            context.term()

    @pytest.mark.timeout(15)
    async def test_multi_topic_subscription(self) -> None:
        """Test multi-topic ZMQ subscription.

        Given: A subscriber subscribed to multiple topics,
        When: Publisher sends messages on various topics,
        Then: Subscriber only receives subscribed topics.
        """
        context = zmq.asyncio.Context()
        try:
            pub_socket = context.socket(zmq.PUB)
            pub_socket.setsockopt(zmq.LINGER, 0)
            pub_port = pub_socket.bind_to_random_port("tcp://127.0.0.1")
            pub_endpoint = f"tcp://127.0.0.1:{pub_port}"
            sub_socket = context.socket(zmq.SUB)
            sub_socket.setsockopt(zmq.LINGER, 0)
            sub_socket.connect(pub_endpoint)
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "BTCUSD")
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "ETHUSD")
            await asyncio.sleep(0.1)
            btc_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="BTCUSD",
                exchange="kraken",
                volume=0.1,
                last=50000.0,
            )
            eth_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="ETHUSD",
                exchange="kraken",
                volume=1.0,
                last=3000.0,
            )
            other_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="ADAUSD",
                exchange="kraken",
                volume=100.0,
                last=0.5,
            )
            await pub_socket.send_multipart([b"BTCUSD", btc_msg.to_json().encode("utf-8")])
            await pub_socket.send_multipart([b"ETHUSD", eth_msg.to_json().encode("utf-8")])
            await pub_socket.send_multipart([b"ADAUSD", other_msg.to_json().encode("utf-8")])
            received_topics = []
            try:
                for _ in range(2):
                    topic_bytes, _payload_bytes = await asyncio.wait_for(
                        sub_socket.recv_multipart(), timeout=1.0
                    )
                    topic = topic_bytes.decode("utf-8")
                    received_topics.append(topic)
                assert "BTCUSD" in received_topics
                assert "ETHUSD" in received_topics
                assert len(received_topics) == 2
            except TimeoutError:
                pytest.fail(f"Did not receive all expected messages. Got: {received_topics}")
        finally:
            pub_socket.close()
            sub_socket.close()
            context.term()

    @pytest.mark.timeout(10)
    async def test_no_subscription_no_messages(self) -> None:
        """Test subscriber without subscriptions receives nothing.

        Given: A subscriber with no subscriptions,
        When: Publisher sends messages,
        Then: Subscriber does not receive any message.
        """
        context = zmq.asyncio.Context()
        try:
            pub_socket = context.socket(zmq.PUB)
            pub_socket.setsockopt(zmq.LINGER, 0)
            pub_port = pub_socket.bind_to_random_port("tcp://127.0.0.1")
            pub_endpoint = f"tcp://127.0.0.1:{pub_port}"
            sub_socket = context.socket(zmq.SUB)
            sub_socket.setsockopt(zmq.LINGER, 0)
            sub_socket.connect(pub_endpoint)
            await asyncio.sleep(0.1)
            test_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="BTCUSD",
                exchange="kraken",
                volume=0.1,
                last=50000.0,
            )
            await pub_socket.send_multipart([b"BTCUSD", test_msg.to_json().encode("utf-8")])
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub_socket.recv_multipart(), timeout=0.5)
        finally:
            pub_socket.close()
            sub_socket.close()
            context.term()

    @pytest.mark.timeout(15)
    async def test_late_subscriber(self) -> None:
        """Test late subscriber misses early messages.

        Given: A publisher sending messages,
        When: Subscriber connects after some messages,
        Then: Subscriber only receives messages after connecting.
        """
        context = zmq.asyncio.Context()
        try:
            pub_socket = context.socket(zmq.PUB)
            pub_socket.setsockopt(zmq.LINGER, 0)
            pub_port = pub_socket.bind_to_random_port("tcp://127.0.0.1")
            pub_endpoint = f"tcp://127.0.0.1:{pub_port}"
            await asyncio.sleep(0.1)
            early_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="BTCUSD",
                exchange="kraken",
                volume=0.1,
                last=49000.0,
            )
            await pub_socket.send_multipart([b"BTCUSD", early_msg.to_json().encode("utf-8")])
            sub_socket = context.socket(zmq.SUB)
            sub_socket.setsockopt(zmq.LINGER, 0)
            sub_socket.connect(pub_endpoint)
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "BTCUSD")
            await asyncio.sleep(0.1)
            late_msg = TickData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                instrument="BTCUSD",
                exchange="kraken",
                volume=0.1,
                last=50000.0,
            )
            await pub_socket.send_multipart([b"BTCUSD", late_msg.to_json().encode("utf-8")])
            try:
                _topic_bytes, payload_bytes = await asyncio.wait_for(
                    sub_socket.recv_multipart(), timeout=1.0
                )
                received_msg = TickData.from_json(payload_bytes.decode("utf-8"))
                assert isinstance(received_msg, TickData)
                assert received_msg.last == pytest.approx(50000.0)
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(sub_socket.recv_multipart(), timeout=0.5)
            except TimeoutError:
                pytest.fail("Did not receive expected message")
        finally:
            pub_socket.close()
            sub_socket.close()
            context.term()


class DummyContext(SimpleNamespace):
    """Test stub for ZMQ context."""

    def __init__(self) -> None:
        """Initialize dummy context."""
        self.socket_called: list[tuple[int, str]] = []

    def socket(self, sock_type: int) -> SimpleNamespace:
        """Create dummy socket.

        Args:
            sock_type: Socket type constant.

        Returns:
            SimpleNamespace stub for socket.
        """
        s = SimpleNamespace()
        s.bind = lambda endpoint: self.socket_called.append((sock_type, endpoint))
        s.close = lambda: None
        s.setsockopt = lambda opt, val: None
        s.recv_multipart = AsyncMock()
        s.send_multipart = AsyncMock()
        return s

    def term(self) -> None:
        """Terminate the context stub."""


@pytest.mark.asyncio
async def test_start_warns_when_running(caplog: pytest.LogCaptureFixture) -> None:
    """Test start on already running broker is no-op.

    Given: A broker with running=True,
    When: Start is called,
    Then: Broker remains running.
    """
    broker = ZmqBrokerProcess(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    broker.running = True
    await broker.start()
    assert broker.running


@pytest.mark.asyncio
async def test_stop_without_running_is_noop() -> None:
    """Test stop on non-running broker is no-op.

    Given: A broker that is not running,
    When: Stop is called,
    Then: No error occurs.
    """
    broker = ZmqBrokerProcess(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    await broker.stop()


@pytest.mark.asyncio
async def test_stop_running_without_resources() -> None:
    """Test stop with running=True but no resources.

    Given: A broker with running=True but no sockets,
    When: Stop is called,
    Then: Running is set to False.
    """
    broker: Any = ZmqBrokerProcess()
    broker.running = True
    await broker.stop()
    assert not broker.running


def test_thread_stop_handles_alive_and_resources() -> None:
    """Test thread broker stop handles live thread and resources.

    Given: A thread broker with active thread and sockets,
    When: Stop is called,
    Then: Thread is joined and sockets are closed.
    """
    broker = ZmqBrokerThread(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    broker.running = True

    class DummyThread:
        def __init__(self) -> None:
            self.join_called = False

        def is_alive(self) -> bool:
            return True

        def join(self, timeout: float | None = None) -> None:
            self.join_called = True

    class DummySocket:
        def __init__(self) -> None:
            self.closed = False

        def setsockopt(self, _option: int, _value: int) -> None:
            """Socket option intentionally ignored in stub."""
            pass

        def close(self) -> None:
            self.closed = True

    class DummyCtx:
        def __init__(self) -> None:
            self.terminated = False

        def term(self) -> None:
            self.terminated = True

    broker.proxy_thread = cast(Any, DummyThread())
    broker.xsub_socket = cast(Any, DummySocket())
    broker.xpub_socket = cast(Any, DummySocket())
    broker.context = cast(Any, DummyCtx())
    broker.stop()
    assert not broker.running
    assert cast(Any, broker.proxy_thread).join_called
    assert cast(Any, broker.xsub_socket).closed and cast(Any, broker.xpub_socket).closed
    assert cast(Any, broker.context).terminated


def test_thread_stop_when_thread_not_alive() -> None:
    """Test thread broker stop when thread is not alive.

    Given: A thread broker with dead thread,
    When: Stop is called,
    Then: Join is not called.
    """
    broker = ZmqBrokerThread(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    broker.running = True

    class DummyThread:
        def __init__(self) -> None:
            self.join_called = False

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            self.join_called = True

    broker.proxy_thread = cast(Any, DummyThread())
    broker.stop()
    assert not cast(Any, broker.proxy_thread).join_called
    assert not broker.running


class DummySocketMessaging:
    """Test stub for ZMQ socket with message queuing."""

    def __init__(self, name: str):
        """Initialize dummy socket for messaging tests.

        Args:
            name: Socket name for identification.
        """
        self.name = name
        self.bound_endpoint: str | None = None
        self.closed = False
        self.recv_queue: list[list[bytes]] = []
        self.sent: list[list[bytes]] = []

    def bind(self, endpoint: str) -> None:
        """Bind socket to endpoint.

        Args:
            endpoint: Endpoint to bind to.
        """
        self.bound_endpoint = endpoint

    async def recv_multipart(self, *args: Any, **kwargs: Any) -> list[bytes]:
        """Receive multipart message from queue with NOBLOCK semantics.

        Mirrors ``zmq.asyncio.Socket.recv_multipart(zmq.NOBLOCK)``: raises
        ``zmq.Again`` when the queue is empty so the broker's batch-drain
        loop can exit cleanly instead of seeing a generic ``RuntimeError``
        that propagates up to ``_proxy_loop``'s exception handler and
        kills the proxy.

        Args:
            *args: Ignored ZMQ flags.
            **kwargs: Ignored keyword arguments.

        Returns:
            List of message parts from queue.

        Raises:
            zmq.Again: When the queue is empty (NOBLOCK semantics).
        """
        if not self.recv_queue:
            raise zmq.Again()
        return self.recv_queue.pop(0)

    async def send_multipart(self, message: list[bytes]) -> None:
        """Send multipart message stub.

        Args:
            message: Message parts to send.
        """
        self.sent.append(message)

    def setsockopt(self, _option: int, _value: int) -> None:
        """Set socket option stub."""
        pass

    def close(self) -> None:
        """Close the socket."""
        self.closed = True


class DummyContextMessaging:
    """Test stub for ZMQ context with socket creation."""

    def __init__(self) -> None:
        """Initialize dummy context for messaging tests."""
        self.terminated = False

    def socket(self, socket_type: int) -> DummySocketMessaging:
        """Create dummy socket.

        Args:
            socket_type: Socket type constant.

        Returns:
            DummySocketMessaging instance.
        """
        return DummySocketMessaging(f"socket-{socket_type}")

    def term(self) -> None:
        """Terminate the context."""
        self.terminated = True


def test_resolve_endpoint_returns_configured_when_socket_has_no_getsockopt() -> None:
    """Test fallback resolution without getsockopt support.

    Given: A socket stub without a getsockopt attribute.
    When: _resolve_endpoint is called for an OS-assigned tcp endpoint.
    Then: The configured endpoint is returned unchanged.
    """
    configured = "tcp://127.0.0.1:0"
    assert ZmqBrokerProcess._resolve_endpoint(object(), configured) == configured


def test_resolve_endpoint_handles_string_and_unknown_getsockopt_values() -> None:
    """Test endpoint resolution for string and unknown LAST_ENDPOINT values.

    Given: Socket stubs returning different LAST_ENDPOINT value types.
    When: _resolve_endpoint inspects the getsockopt result.
    Then: String endpoints are returned and unknown types fall back to configured.
    """

    class _SocketStub:
        def __init__(self, value: object) -> None:
            self.value = value

        def getsockopt(self, opt: int) -> object:
            assert opt == zmq.LAST_ENDPOINT
            return self.value

    configured = "tcp://127.0.0.1:0"
    assert ZmqBrokerProcess._resolve_endpoint(_SocketStub("tcp://127.0.0.1:5555"), configured) == (
        "tcp://127.0.0.1:5555"
    )
    assert ZmqBrokerProcess._resolve_endpoint(_SocketStub(1234), configured) == configured


def test_handle_subscription_frame_records_direct_subscribe() -> None:
    """Test direct subscribe tracking in verbose mode.

    Given: A verbose broker with an empty observed subscription map.
    When: A subscribe frame is forwarded through _handle_subscription_frame.
    Then: The topic count is recorded and the frame is sent to XSUB.
    """
    broker = ZmqBrokerProcess("inproc://xsub", "inproc://xpub", xpub_verbose=True)
    broker.xsub_socket = MagicMock()
    broker._observed_subscriptions = {}
    broker._observation_changed = threading.Condition()

    handled = broker._handle_subscription_frame([b"\x01market.topic"])

    assert handled is True
    assert broker._observed_subscriptions == {b"market.topic": 1}
    broker.xsub_socket.send_multipart.assert_called_once_with([b"\x01market.topic"])


def test_handle_subscription_frame_decrements_without_removing_multi_subscriber_topic() -> None:
    """Test unsubscribe handling for multiply subscribed topics.

    Given: A verbose broker that has observed the same topic twice.
    When: An unsubscribe frame is forwarded for that topic.
    Then: The topic count is decremented instead of being removed.
    """
    broker = ZmqBrokerProcess("inproc://xsub", "inproc://xpub", xpub_verbose=True)
    broker.xsub_socket = MagicMock()
    broker._observed_subscriptions = {b"market.topic": 2}
    broker._observation_changed = threading.Condition()

    handled = broker._handle_subscription_frame([b"\x00market.topic"])

    assert handled is True
    assert broker._observed_subscriptions == {b"market.topic": 1}


def test_handle_subscription_frame_returns_false_for_non_subscription_payload() -> None:
    """Test non-subscription payload handling in verbose mode.

    Given: A verbose broker with subscription tracking enabled.
    When: _handle_subscription_frame receives a payload without subscribe metadata.
    Then: It returns False and does not forward the frame to XSUB.
    """
    broker = ZmqBrokerProcess("inproc://xsub", "inproc://xpub", xpub_verbose=True)
    broker.xsub_socket = MagicMock()
    broker._observed_subscriptions = {}
    broker._observation_changed = threading.Condition()

    handled = broker._handle_subscription_frame([b"market.topic"])

    assert handled is False
    broker.xsub_socket.send_multipart.assert_not_called()


def test_proxy_loop_returns_early_when_sockets_unset() -> None:
    """Test ``_proxy_loop`` exits immediately when sockets are not bound.

    Given: A broker with ``xsub_socket`` and ``xpub_socket`` set to None
        (not started, or stop already ran).
    When: ``_proxy_loop`` is called directly.
    Then: It returns without touching ``zmq.Poller`` or raising.
    """
    broker = ZmqBrokerProcess()
    broker.xsub_socket = None
    broker.xpub_socket = None
    broker._proxy_loop()


def test_proxy_loop_handles_context_terminated_outer_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test ``_proxy_loop`` outer ``zmq.ContextTerminated`` catch.

    Given: A broker with sockets present but ``zmq.Poller.register``
        raising ``zmq.ContextTerminated`` (e.g. context torn down
        between ``_sync_start`` and the thread first scheduling).
    When: ``_proxy_loop`` runs.
    Then: It catches the exception and exits cleanly (no logger.error).
    """
    broker = ZmqBrokerProcess()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()

    class _BadPoller:
        """Poller stub raising on register to exercise outer catch."""

        def register(self, *_args: Any, **_kwargs: Any) -> None:
            """Raise ContextTerminated like a torn-down context would."""
            raise zmq.ContextTerminated()

        def poll(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, int]]:
            """Unused — register raises first."""
            return []

    monkeypatch.setattr(zmq, "Poller", lambda: _BadPoller())
    broker._proxy_loop()


def test_proxy_loop_logs_unexpected_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test ``_proxy_loop`` logs unexpected exceptions and exits.

    Given: A broker whose poller raises a generic ``RuntimeError`` on poll.
    When: ``_proxy_loop`` runs.
    Then: The exception is caught by the outer handler and logged at
        ERROR level via loguru. Uses a loguru list-sink because pytest's
        ``caplog`` only sees stdlib ``logging`` records and loguru does
        not propagate by default.
    """
    broker = ZmqBrokerProcess()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()

    class _BoomPoller:
        """Poller stub raising on poll to exercise generic handler."""

        def register(self, *_args: Any, **_kwargs: Any) -> None:
            """No-op."""

        def poll(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, int]]:
            """Raise a generic error to trigger logger.error path."""
            raise RuntimeError("boom")

    monkeypatch.setattr(zmq, "Poller", lambda: _BoomPoller())
    captured: list[str] = []
    handler_id = loguru_logger.add(captured.append, format="{message}", level="ERROR")
    try:
        broker._proxy_loop()
    finally:
        loguru_logger.remove(handler_id)
    assert any("Broker proxy error" in line for line in captured)


def test_proxy_loop_handles_context_terminated_during_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test ``_proxy_loop`` inner ``zmq.ContextTerminated`` during ``poll()``.

    Given: A broker whose poller raises ``zmq.ContextTerminated`` on the
        first poll iteration (after successful register) — mirrors a
        normal shutdown where ``context.term()`` interrupts the poll.
    When: ``_proxy_loop`` runs.
    Then: The inner handler returns cleanly without logging an error.
    """
    broker = ZmqBrokerProcess()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()

    class _ShuttingDownPoller:
        """Poller stub raising ContextTerminated mid-poll."""

        def register(self, *_args: Any, **_kwargs: Any) -> None:
            """No-op."""

        def poll(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, int]]:
            """Mirror context.term() interrupting an in-flight poll."""
            raise zmq.ContextTerminated()

    monkeypatch.setattr(zmq, "Poller", lambda: _ShuttingDownPoller())
    broker._proxy_loop()


def test_drain_xsub_to_xpub_returns_when_sockets_unset() -> None:
    """Test ``_drain_xsub_to_xpub`` is a no-op when sockets are None.

    Given: A broker with ``xsub_socket`` set to None.
    When: ``_drain_xsub_to_xpub`` is called directly.
    Then: It returns without touching anything.
    """
    broker = ZmqBrokerProcess()
    broker.xsub_socket = None
    broker.xpub_socket = MagicMock()
    broker._drain_xsub_to_xpub()


def test_drain_xpub_to_xsub_returns_when_sockets_unset() -> None:
    """Test ``_drain_xpub_to_xsub`` is a no-op when sockets are None.

    Given: A broker with ``xpub_socket`` set to None.
    When: ``_drain_xpub_to_xsub`` is called directly.
    Then: It returns without touching anything.
    """
    broker = ZmqBrokerProcess()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = None
    broker._drain_xpub_to_xsub()


def test_wait_for_subscription_blocking_returns_when_state_unset() -> None:
    """Test ``_wait_for_subscription_blocking`` is a no-op without verbose state.

    Given: A non-verbose broker (no observed_subscriptions / condition).
    When: ``_wait_for_subscription_blocking`` is called directly.
    Then: It returns without blocking or raising.
    """
    broker = ZmqBrokerProcess()
    broker._observed_subscriptions = None
    broker._observation_changed = None
    broker._wait_for_subscription_blocking(b"any.")


def test_zmq_broker_thread_proxy_loop_forwards_real_sockets() -> None:
    """Test ``ZmqBrokerThread._proxy_loop`` forwards a real PUB->SUB round-trip.

    Given: A ``ZmqBrokerThread`` started on ephemeral tcp endpoints.
    When: A PUB connects to XSUB and sends a frame, a SUB connects to
        XPUB and reads it.
    Then: The SUB receives the frame, exercising the CLI threaded
        broker's forwarding hot path.

    Exercises the CLI-side threaded broker (used by ``cli/app.py``)
    via real sockets so the legacy class keeps its production
    behaviour after the production class refactor.
    """
    broker = ZmqBrokerThread(
        xsub_endpoint="tcp://127.0.0.1:7820", xpub_endpoint="tcp://127.0.0.1:7821"
    )
    try:
        broker.start()
        time.sleep(0.1)
        context = zmq.Context()
        pub = context.socket(zmq.PUB)
        pub.connect("tcp://127.0.0.1:7820")
        sub = context.socket(zmq.SUB)
        sub.connect("tcp://127.0.0.1:7821")
        sub.setsockopt_string(zmq.SUBSCRIBE, "TOPIC")
        time.sleep(0.2)
        pub.send_multipart([b"TOPIC", b"payload"])
        sub.setsockopt(zmq.RCVTIMEO, 2000)
        topic, payload = sub.recv_multipart()
        assert topic == b"TOPIC"
        assert payload == b"payload"
        pub.close()
        sub.close()
        context.term()
    finally:
        broker.stop()


def test_zmq_broker_thread_proxy_loop_returns_when_sockets_unset() -> None:
    """Test ``ZmqBrokerThread._proxy_loop`` early-returns when sockets are None.

    Given: A ``ZmqBrokerThread`` instance with ``xsub_socket=None``.
    When: ``_proxy_loop`` is called directly.
    Then: It returns without raising.
    """
    broker = ZmqBrokerThread()
    broker.xsub_socket = None
    broker.xpub_socket = None
    broker._proxy_loop()


def test_zmq_broker_thread_proxy_loop_handles_zmq_again(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test ``ZmqBrokerThread._proxy_loop`` handles ``zmq.Again`` in inner try.

    Given: A ``ZmqBrokerThread`` with a poller that raises ``zmq.Again``
        on first poll, then stop is signalled before the next iteration.
    When: ``_proxy_loop`` runs.
    Then: The ``zmq.Again`` is swallowed by the inner ``continue`` branch
        and the loop exits when ``_stop_event`` is set.
    """
    broker = ZmqBrokerThread()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()

    class _AgainPoller:
        """Poller stub raising Again once then signalling stop."""

        def __init__(self) -> None:
            """Track invocations so we can stop after the first poll."""
            self.calls = 0

        def register(self, *_args: Any, **_kwargs: Any) -> None:
            """No-op."""

        def poll(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, int]]:
            """Raise Again to exercise the continue branch, then signal stop."""
            self.calls += 1
            broker._stop_event.set()
            raise zmq.Again()

    monkeypatch.setattr(zmq, "Poller", lambda: _AgainPoller())
    broker._proxy_loop()


def test_zmq_broker_thread_proxy_loop_handles_context_terminated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test ``ZmqBrokerThread._proxy_loop`` catches ``zmq.ContextTerminated``.

    Given: A ``ZmqBrokerThread`` with a poller raising ContextTerminated.
    When: ``_proxy_loop`` runs.
    Then: The exception is swallowed by the outer ``pass`` branch.
    """
    broker = ZmqBrokerThread()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()

    class _TerminatedPoller:
        """Poller stub raising ContextTerminated on register."""

        def register(self, *_args: Any, **_kwargs: Any) -> None:
            """Raise to exercise the outer ContextTerminated branch."""
            raise zmq.ContextTerminated()

        def poll(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, int]]:
            """Unused."""
            return []

    monkeypatch.setattr(zmq, "Poller", lambda: _TerminatedPoller())
    broker._proxy_loop()


def test_zmq_broker_thread_proxy_loop_logs_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test ``ZmqBrokerThread._proxy_loop`` logs unexpected exceptions.

    Given: A ``ZmqBrokerThread`` with a poller raising ``RuntimeError`` on register.
    When: ``_proxy_loop`` runs.
    Then: The exception is logged at ERROR via loguru and the loop exits.
    """
    broker = ZmqBrokerThread()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()

    class _BoomPoller:
        """Poller stub raising RuntimeError on register."""

        def register(self, *_args: Any, **_kwargs: Any) -> None:
            """Raise to exercise the generic Exception handler."""
            raise RuntimeError("thread-boom")

        def poll(self, *_args: Any, **_kwargs: Any) -> list[tuple[Any, int]]:
            """Unused."""
            return []

    monkeypatch.setattr(zmq, "Poller", lambda: _BoomPoller())
    captured: list[str] = []
    handler_id = loguru_logger.add(captured.append, format="{message}", level="ERROR")
    try:
        broker._proxy_loop()
    finally:
        loguru_logger.remove(handler_id)
    assert any("Broker proxy error" in line for line in captured)
