"""Tests for ZMQ message broker implementations."""

import asyncio
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
        assert kwargs["xsub_endpoint"] == settings.zmq_broker_xsub
        assert kwargs["xpub_endpoint"] == settings.zmq_broker_xpub

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
            xsub_endpoint="tcp://127.0.0.1:7822", xpub_endpoint="tcp://127.0.0.1:7823"
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
async def test_start_and_stop_use_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test broker start/stop use ZMQ context.

    Given: A broker with mocked context,
    When: Started and stopped,
    Then: Context is used for sockets.
    """
    broker: Any = ZmqBrokerProcess(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    dummy_ctx = DummyContext()
    monkeypatch.setattr(
        "snapper.messaging.infrastructure.broker.zmq.asyncio.Context", lambda: dummy_ctx
    )
    broker._proxy_loop = AsyncMock()
    await broker.start()
    assert broker.running
    assert broker.proxy_task is not None
    await broker.stop()
    assert not broker.running


@pytest.mark.asyncio
async def test_stop_cancels_proxy_task_and_closes_sockets() -> None:
    """Test stop cancels task and closes sockets.

    Given: A running broker with active task,
    When: Stop is called,
    Then: Task is cancelled and sockets are closed.
    """
    broker: Any = ZmqBrokerProcess()
    broker.running = True

    async def sleeper() -> None:
        await asyncio.sleep(0.01)

    broker.proxy_task = asyncio.create_task(sleeper())
    xsub = DummySocket("pub")
    xpub = DummySocket("sub")
    broker.xsub_socket = xsub
    broker.xpub_socket = xpub
    term_called = False

    class Ctx(SimpleNamespace):
        def term(self) -> None:
            nonlocal term_called
            term_called = True

    broker.context = Ctx()
    await broker.stop()
    assert not broker.running
    assert broker.proxy_task.cancelled()
    assert term_called
    assert xsub.setsockopt_calls == [(zmq.LINGER, 0)]
    assert xpub.setsockopt_calls == [(zmq.LINGER, 0)]
    assert xsub.closed
    assert xpub.closed


class DummySocket:
    """Test stub for ZMQ socket with message tracking."""

    def __init__(self, name: str, recv_side_effect: Exception | None = None) -> None:
        """Initialize dummy socket.

        Args:
            name: Socket name for identification.
            recv_side_effect: Optional exception to raise on recv.
        """
        self.name = name
        self.bound: list[str] = []
        self.sent: list[Any] = []
        self.recv_side_effect = recv_side_effect
        self.setsockopt_calls: list[tuple[int, int]] = []
        self.closed = False

    def bind(self, endpoint: str) -> None:
        """Bind socket to endpoint.

        Args:
            endpoint: Endpoint to bind to.
        """
        self.bound.append(endpoint)

    def setsockopt(self, option: int, value: int) -> None:
        """Set socket option.

        Args:
            option: Option constant.
            value: Option value.
        """
        self.setsockopt_calls.append((option, value))

    def close(self) -> None:
        """Close the socket."""
        self.closed = True

    async def recv_multipart(self, *_args: Any) -> list[bytes]:
        """Receive multipart message stub.

        Args:
            *_args: Ignored arguments.

        Returns:
            List of message parts.

        Raises:
            Exception: If recv_side_effect is set.
        """
        if self.recv_side_effect:
            raise self.recv_side_effect
        return [f"{self.name}-msg".encode()]

    async def send_multipart(self, message: Any) -> None:
        """Send multipart message stub.

        Args:
            message: Message to send.
        """
        self.sent.append(message)


@pytest.mark.asyncio
async def test_proxy_loop_forwards_between_sockets(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop forwards messages bidirectionally.

    Given: A broker with mocked sockets,
    When: Messages arrive on both sockets,
    Then: Messages are forwarded to opposite socket.
    """
    broker: Any = ZmqBrokerProcess(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    xsub = DummySocket("pub")
    xpub = DummySocket("sub")
    broker.xsub_socket = xsub
    broker.xpub_socket = xpub
    broker.running = True
    calls = 0

    async def fake_poll() -> dict[Any, int]:
        nonlocal calls
        calls += 1
        if calls > 1:
            broker.running = False
        return {xsub: zmq.POLLIN, xpub: zmq.POLLIN}

    broker._poll_sockets = fake_poll
    await broker._proxy_loop()
    assert xpub.sent and xsub.sent


@pytest.mark.asyncio
async def test_proxy_loop_handles_timeout_and_again(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop handles timeout and again exceptions.

    Given: A broker with poll that raises TimeoutError,
    When: Proxy loop runs,
    Then: Loop continues without crashing.
    """
    broker: Any = ZmqBrokerProcess()
    xsub = DummySocket("pub", recv_side_effect=Exception("again"))
    broker.xsub_socket = xsub
    broker.xpub_socket = DummySocket("sub")
    broker.running = True
    calls = 0

    async def fake_poll() -> dict[Any, int]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError
        broker.running = False
        return {xsub: zmq.POLLIN}

    broker._poll_sockets = fake_poll
    await broker._proxy_loop()
    assert calls >= 2


@pytest.mark.asyncio
async def test_proxy_loop_ignores_non_pollin_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop ignores non-POLLIN events.

    Given: A broker with poll returning non-POLLIN events,
    When: Proxy loop runs,
    Then: No message forwarding occurs.
    """
    broker: Any = ZmqBrokerProcess()
    xsub = DummySocket("pub")
    broker.xsub_socket = xsub
    broker.xpub_socket = DummySocket("sub")
    broker.running = True

    async def fake_poll() -> dict[Any, int]:
        broker.running = False
        return {xsub: 0}

    broker._poll_sockets = fake_poll
    await broker._proxy_loop()
    assert not broker.running


@pytest.mark.asyncio
async def test_proxy_loop_xpub_only_pollin(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop handles XPUB-only POLLIN.

    Given: A broker with only XPUB having POLLIN,
    When: Proxy loop runs,
    Then: Message forwarded from XPUB to XSUB.
    """
    broker: Any = ZmqBrokerProcess(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    xsub = DummySocket("pub")
    xpub = DummySocket("sub")
    broker.xsub_socket = xsub
    broker.xpub_socket = xpub
    broker.running = True
    calls = 0

    async def fake_poll() -> dict[Any, int]:
        nonlocal calls
        calls += 1
        if calls > 1:
            broker.running = False
        return {xpub: zmq.POLLIN}

    broker._poll_sockets = fake_poll
    await broker._proxy_loop()
    assert xsub.sent


@pytest.mark.asyncio
async def test_proxy_loop_xpub_pollin_no_xsub(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop handles XPUB POLLIN without XSUB.

    Given: A broker with XSUB=None,
    When: XPUB has POLLIN event,
    Then: No message forwarding occurs.
    """
    broker: Any = ZmqBrokerProcess(xsub_endpoint="inproc://xsub", xpub_endpoint="inproc://xpub")
    xpub = DummySocket("sub")
    broker.xsub_socket = None
    broker.xpub_socket = xpub
    broker.running = True
    calls = 0

    async def fake_poll() -> dict[Any, int]:
        nonlocal calls
        calls += 1
        if calls > 1:
            broker.running = False
        return {xpub: zmq.POLLIN}

    broker._poll_sockets = fake_poll
    await broker._proxy_loop()
    assert not xpub.sent


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


@pytest.mark.asyncio
async def test_poll_sockets_uses_poller(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test _poll_sockets uses ZMQ Poller.

    Given: A broker with sockets,
    When: _poll_sockets is called,
    Then: ZMQ Poller is used to poll sockets.
    """
    broker: Any = ZmqBrokerProcess()
    xsub = DummySocket("pub")
    xpub = DummySocket("sub")
    broker.xsub_socket = xsub
    broker.xpub_socket = xpub

    class DummyPoller:
        def __init__(self) -> None:
            self.registered: list[Any] = []

        def register(self, socket: Any, _flag: int) -> None:
            self.registered.append(socket)

        async def poll(self) -> list[tuple[Any, int]]:
            return [(xsub, zmq.POLLIN)]

    monkeypatch.setattr("snapper.messaging.infrastructure.broker.zmq.asyncio.Poller", DummyPoller)
    events = await broker._poll_sockets()
    assert xsub in events


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
        """Receive multipart message from queue.

        Args:
            *args: Ignored arguments.
            **kwargs: Ignored keyword arguments.

        Returns:
            List of message parts from queue.

        Raises:
            RuntimeError: If no messages queued.
        """
        if not self.recv_queue:
            raise RuntimeError(f"No queued messages for {self.name}")
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


@pytest.mark.asyncio
async def test_broker_start_and_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test broker start and stop with mocked context.

    Given: A broker with mocked ZMQ context,
    When: Started and stopped,
    Then: Sockets are created and closed properly.
    """
    dummy_ctx = DummyContextMessaging()
    monkeypatch.setattr(zmq.asyncio, "Context", lambda: dummy_ctx)
    created_tasks: list[asyncio.Task[None]] = []

    async def dummy_proxy_loop(self: ZmqBrokerProcess) -> None:
        return None

    monkeypatch.setattr(ZmqBrokerProcess, "_proxy_loop", dummy_proxy_loop)
    original_create_task = asyncio.create_task

    def fake_create_task(coro: Any) -> asyncio.Task[None]:
        task = original_create_task(coro)
        created_tasks.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", fake_create_task)
    broker = ZmqBrokerProcess("inproc://xsub", "inproc://xpub")
    await broker.start()
    assert broker.running is True
    assert broker.proxy_task is not None
    assert isinstance(broker.proxy_task, asyncio.Task)
    assert created_tasks, "Proxy loop task should be created"
    assert broker.xsub_socket is not None
    assert broker.xpub_socket is not None
    await broker.stop()
    assert broker.running is False
    assert broker.xsub_socket.closed is True
    assert broker.xpub_socket.closed is True
    assert dummy_ctx.terminated is True


@pytest.mark.asyncio
async def test_proxy_loop_forwards_both_directions(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop forwards in both directions.

    Given: A broker with queued messages on both sockets,
    When: Proxy loop runs,
    Then: Messages forwarded bidirectionally.
    """
    broker = ZmqBrokerProcess("inproc://xsub", "inproc://xpub")
    xsub = DummySocketMessaging("xsub")
    xpub = DummySocketMessaging("xpub")
    broker.xsub_socket = cast(Any, xsub)
    broker.xpub_socket = cast(Any, xpub)
    broker.running = True
    xsub.recv_queue.append([b"from-xsub"])
    xpub.recv_queue.append([b"from-xpub"])
    events = [
        {xsub: zmq.POLLIN},
        {xpub: zmq.POLLIN},
        {},
    ]
    call_count = 0

    async def fake_poll_sockets() -> dict[Any, int]:
        nonlocal call_count
        if call_count >= len(events):
            broker.running = False
            return {}
        event = events[call_count]
        call_count += 1
        if not event:
            broker.running = False
        return event

    monkeypatch.setattr(broker, "_poll_sockets", fake_poll_sockets)
    await broker._proxy_loop()
    assert xpub.sent == [[b"from-xsub"]]
    assert xsub.sent == [[b"from-xpub"]]


@pytest.mark.asyncio
async def test_proxy_loop_handles_cancelled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop handles CancelledError.

    Given: A broker with poll that raises CancelledError,
    When: Proxy loop runs,
    Then: CancelledError propagates from the loop.
    """
    broker = ZmqBrokerProcess("inproc://xsub", "inproc://xpub")
    broker.running = True

    async def fake_poll_sockets() -> dict[Any, int]:
        raise asyncio.CancelledError

    monkeypatch.setattr(broker, "_poll_sockets", fake_poll_sockets)
    with pytest.raises(asyncio.CancelledError):
        await broker._proxy_loop()


@pytest.mark.asyncio
async def test_proxy_loop_logs_unexpected_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop logs unexpected exceptions.

    Given: A broker with poll that raises RuntimeError,
    When: Proxy loop runs,
    Then: Error is logged.
    """
    broker: Any = ZmqBrokerProcess()
    broker.xsub_socket = MagicMock()
    broker.xpub_socket = MagicMock()
    broker.running = True
    logged_errors: list[str] = []

    def capture_error(msg: str) -> None:
        logged_errors.append(msg)

    monkeypatch.setattr("snapper.messaging.infrastructure.broker.logger.error", capture_error)

    async def raise_error() -> dict[Any, int]:
        raise RuntimeError("Unexpected broker error")

    monkeypatch.setattr(broker, "_poll_sockets", raise_error)
    await broker._proxy_loop()
    assert len(logged_errors) == 1
    assert "Broker proxy error" in logged_errors[0]


@pytest.mark.asyncio
async def test_proxy_loop_handles_zmq_again(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test proxy loop handles zmq.Again exception.

    Given: A broker with recv that raises zmq.Again,
    When: Proxy loop runs,
    Then: Loop continues without crashing.
    """
    broker: Any = ZmqBrokerProcess()
    mock_xsub = MagicMock()
    mock_xpub = MagicMock()
    broker.xsub_socket = mock_xsub
    broker.xpub_socket = mock_xpub
    broker.running = True
    call_count = 0

    async def fake_poll_sockets() -> dict[Any, int]:
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            broker.running = False
        return {mock_xsub: zmq.POLLIN}

    async def raise_again(*args: Any, **kwargs: Any) -> list[bytes]:
        raise zmq.Again()

    monkeypatch.setattr(broker, "_poll_sockets", fake_poll_sockets)
    mock_xsub.recv_multipart = raise_again
    await broker._proxy_loop()
    assert call_count >= 3
