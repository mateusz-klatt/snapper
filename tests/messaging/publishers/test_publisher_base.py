"""Tests for the base market data publisher service."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import zmq

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import TickData

TEST_DB_URL = "sqlite:///:memory:"


def zmq_socket_stub(**kwargs: Any) -> SimpleNamespace:
    """Create a stub ZMQ socket for testing."""
    return SimpleNamespace(setsockopt=lambda o, v: None, **kwargs)


class DummyClient(SimpleNamespace):
    """Test stub for exchange client."""

    async def connect(self) -> None:
        """Connect to exchange."""
        ...

    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        ...

    async def subscribe_candles(self, symbols: list[str], timeframe: str) -> AsyncIterator[Any]:
        """Subscribe to candle updates."""
        if False:
            yield None

    async def subscribe_ticks(self, symbols: list[str]) -> AsyncIterator[Any]:
        """Subscribe to tick updates."""
        if False:
            yield None

    async def subscribe_trades(self, symbols: list[str]) -> AsyncIterator[Any]:
        """Subscribe to trade updates."""
        if False:
            yield None


class DummyPublisher(MarketDataPublisherService[Any]):
    """Test stub for MarketDataPublisherService."""

    def _create_exchange_client(self) -> DummyClient:
        return DummyClient()

    def _get_exchange_name(self) -> str:
        return "kraken"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        return symbols


class StubValidatedPublisher(SimpleNamespace):
    """Test stub for validated publisher socket."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.send_multipart = AsyncMock()
        self.close = lambda: None


class StubValidatedSubscriber(SimpleNamespace):
    """Test stub for validated subscriber socket."""

    def __init__(self) -> None:
        """Initialize the instance."""
        super().__init__()
        self.recv_multipart = AsyncMock()
        self.subscribe = Mock()
        self.close = lambda: None


@pytest.mark.asyncio
async def test_start_logs_warning_when_running(caplog: pytest.LogCaptureFixture) -> None:
    """Test start returns when already running.

    Given: A publisher that is already running,
    When: Start is called,
    Then: Returns None without starting again.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    caplog.set_level("WARNING")
    result = await pub.start()
    assert result is None


@pytest.mark.asyncio
async def test_start_warns_on_symbol_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start warns when symbols exceed limit.

    Given: A publisher with more symbols than connection limit,
    When: Started,
    Then: Publisher starts with warning.
    """
    pub: Any = DummyPublisher(symbols=["A", "B", "C"])
    pub._get_max_symbols_per_connection = Mock(return_value=1)
    mock_repo = SimpleNamespace(get_latest_candle_ids=AsyncMock(return_value={}))
    monkeypatch.setattr("snapper.messaging.publishers.base.get_repository", lambda _url: mock_repo)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_with_service",
        lambda _svc: pub.settings,
    )

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None, close=lambda: None, setsockopt=lambda o, v: None
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.publishers.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedPublisher",
        lambda sock: SimpleNamespace(
            close=sock.close, send_multipart=AsyncMock(), setsockopt=sock.setsockopt
        ),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedSubscriber",
        lambda sock: SimpleNamespace(
            subscribe=lambda *_: None,
            recv_multipart=AsyncMock(),
            close=sock.close,
            setsockopt=sock.setsockopt,
        ),
    )
    dummy_client = cast(Any, DummyClient())
    dummy_client.connect = AsyncMock()
    pub._create_exchange_client = lambda: dummy_client
    pub.settings.timeframes = []
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._heartbeat_loop = AsyncMock()
    pub._symbol_aliases_loop = AsyncMock()
    pub._tick_loop = AsyncMock()
    pub._trade_loop = AsyncMock()
    pub._candle_loop = AsyncMock()
    await pub.start()
    await pub.stop()


@pytest.mark.asyncio
async def test_tick_loop_handles_unknown_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop handles unknown symbols.

    Given: A tick loop receiving unknown symbol,
    When: Tick arrives for unknown symbol,
    Then: No error occurs.
    """
    pub: Any = DummyPublisher(symbols=["KNOWN"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="UNKNOWN",
            volume=1.0,
            last=1.0,
            exchange="kraken",
        )
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["KNOWN"])


@pytest.mark.asyncio
async def test_tick_loop_handles_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop handles exceptions.

    Given: A tick loop with failing generator,
    When: Generator raises exception,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["KNOWN"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        for _ in []:
            yield
        raise RuntimeError("boom")

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["KNOWN"])


@pytest.mark.asyncio
async def test_trade_loop_publishes_and_saves(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade loop publishes and saves trades.

    Given: A trade loop with incoming trades,
    When: Trade arrives,
    Then: Message is published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            price=100.0,
            quantity=1.0,
            side="buy",
            timestamp=datetime.now(UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited()


@pytest.mark.asyncio
async def test_candle_loop_handles_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test candle loop handles errors.

    Given: A candle loop with failing generator,
    When: Generator raises exception,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        for _ in []:
            yield
        raise RuntimeError("boom")

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
    await pub._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
async def test_publish_message_skips_when_not_running() -> None:
    """Test publish skips when not running.

    Given: A publisher that is not running,
    When: Publish message is called,
    Then: Message is not sent.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.msg_publisher = AsyncMock()
    pub.running = False
    await pub._publish_message(
        "topic",
        CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="i",
            volume=1.0,
            timeframe="1m",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            exchange="kraken",
            open_at=datetime.now(UTC),
        ),
    )
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_message_errors_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    """Test publish errors are logged.

    Given: A publisher with failing send,
    When: Publish fails,
    Then: Error is logged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    failing = AsyncMock()
    failing.send.side_effect = RuntimeError("boom")
    pub.msg_publisher = failing
    pub.running = True
    await pub._publish_message(
        "topic",
        CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="i",
            volume=1.0,
            timeframe="1m",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            exchange="kraken",
            open_at=datetime.now(UTC),
        ),
    )
    failing.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_publish_heartbeat_when_running() -> None:
    """Test heartbeat published when running.

    Given: A running publisher,
    When: Heartbeat is published,
    Then: Message is sent.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.msg_publisher = AsyncMock()
    pub.running = True
    msg = HeartbeatData(
        session_id="",
        sequence_id=0,
        component="c",
        sequence=1,
        status="healthy",
        lag_ms=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    await pub._publish_heartbeat("heartbeat.c", msg)
    pub.msg_publisher.send.assert_awaited()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_invokes_invalidation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test symbol aliases loop invalidates cache.

    Given: A publisher receiving symbol alias update,
    When: Update message arrives,
    Then: Symbol cache is invalidated.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return "system.symbol_aliases", b"{}"
        pub.running = False
        await asyncio.sleep(0)
        return "noop", b"{}"

    subscriber.recv_multipart.side_effect = recv
    pub.subscriber = subscriber
    pub.running = True
    invalidate_mock = MagicMock()
    monkeypatch.setattr(pub, "_invalidate_symbol_cache", invalidate_mock)
    await pub._symbol_aliases_loop()
    invalidate_mock.assert_called_once()


@pytest.mark.asyncio
async def test_save_to_db_handles_non_candle_and_missing_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test save_to_db handles non-candle messages.

    Given: A publisher with optional repository,
    When: Non-candle or candle message is saved,
    Then: Only candle messages are persisted.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    msg = TickData(
        session_id="",
        sequence_id=0,
        instrument="i",
        volume=0.0,
        last=1.0,
        exchange="kraken",
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    await pub._save_to_db("i", msg)
    repo = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")), upsert_candles=AsyncMock()
    )
    pub.repository = repo
    resolve_mock = AsyncMock(return_value="fake-spid")
    monkeypatch.setattr("snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock)
    candle = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        volume=1.0,
        timeframe="1m",
        open=100.0,
        high=100.0,
        low=100.0,
        close=100.0,
        vwap=None,
        trades=None,
        timestamp=datetime.now(tz=UTC),
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    await pub._save_to_db("BTC-USD", candle)
    resolve_mock.assert_awaited_once_with(repo, "BTC-USD")
    call_kwargs = repo.ensure_instrument.call_args.kwargs
    assert call_kwargs["symbol_public_id"] == "fake-spid"
    assert call_kwargs["exchange"] == "kraken"
    assert call_kwargs["session_id"] != ""
    assert call_kwargs["sequence_id"] >= 1
    repo.upsert_candles.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_disconnects_and_closes() -> None:
    """Test stop disconnects and closes resources.

    Given: A running publisher,
    When: Stop is called,
    Then: Client disconnected, sockets closed.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    disconnect = AsyncMock()
    pub._exchange_client = SimpleNamespace(disconnect=disconnect)
    pub.publisher = zmq_socket_stub(close=Mock())
    pub.subscriber = zmq_socket_stub(close=Mock())
    term = Mock()
    pub.context = SimpleNamespace(term=term)
    await pub.stop()
    disconnect.assert_awaited_once()
    assert pub._exchange_client is None
    assert pub.publisher.close.called
    assert pub.subscriber.close.called
    assert term.called


@pytest.mark.asyncio
async def test_start_handles_cancelled_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test start handles cancelled tasks.

    Given: A publisher with tasks that get cancelled,
    When: Tasks are cancelled during gather,
    Then: Publisher handles cancellation gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._get_max_symbols_per_connection = Mock(return_value=0)
    mock_repo = SimpleNamespace(get_latest_candle_ids=AsyncMock(return_value={}))
    monkeypatch.setattr("snapper.messaging.publishers.base.get_repository", lambda _url: mock_repo)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_service",
        AsyncMock(return_value=SimpleNamespace()),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.get_settings_with_service",
        lambda _svc: pub.settings,
    )

    class DummySock(SimpleNamespace):
        def __init__(self) -> None:
            super().__init__(
                connect=lambda *_: None, close=lambda: None, setsockopt=lambda o, v: None
            )

    class DummyCtx:
        def socket(self, *_args: Any) -> DummySock:
            return DummySock()

        def term(self) -> None:
            return None

    monkeypatch.setattr("snapper.messaging.publishers.base.zmq.asyncio.Context", lambda: DummyCtx())
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedPublisher",
        lambda sock: SimpleNamespace(
            close=sock.close, send_multipart=AsyncMock(), setsockopt=sock.setsockopt
        ),
    )
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.ValidatedSubscriber",
        lambda sock: SimpleNamespace(
            subscribe=lambda *_: None,
            recv_multipart=AsyncMock(),
            close=sock.close,
            setsockopt=sock.setsockopt,
        ),
    )
    client: Any = DummyClient()
    client.connect = AsyncMock()
    client.subscribe_candles = AsyncMock()
    client.subscribe_ticks = AsyncMock()
    client.subscribe_trades = AsyncMock()
    pub._create_exchange_client = lambda: client
    pub.settings.timeframes = []
    pub.settings.zmq_heartbeat_interval_ms = 0

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    pub._heartbeat_loop = noop
    pub._symbol_aliases_loop = noop
    pub._tick_loop = noop
    pub._trade_loop = noop
    pub._candle_loop = noop
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(asyncio, "create_task", lambda coro: loop.create_task(coro))

    async def raise_cancel(*_args: Any, **_kwargs: Any) -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr(asyncio, "gather", raise_cancel)
    with pytest.raises(asyncio.CancelledError):
        await pub.start()
    await pub.stop()


@pytest.mark.asyncio
async def test_candle_loop_returns_without_client() -> None:
    """Test candle loop returns without client.

    Given: A publisher without exchange client,
    When: Candle loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
async def test_tick_loop_returns_without_client() -> None:
    """Test tick loop returns without client.

    Given: A publisher without exchange client,
    When: Tick loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._tick_loop(["BTC-USD"])


@pytest.mark.asyncio
async def test_trade_loop_returns_without_client() -> None:
    """Test trade loop returns without client.

    Given: A publisher without exchange client,
    When: Trade loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._trade_loop(["BTC-USD"])


@pytest.mark.asyncio
async def test_tick_loop_breaks_when_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop breaks when stopped.

    Given: A publisher that is not running,
    When: Tick message arrives,
    Then: Message is not published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(symbol="BTC-USD", last=1.0, volume=1.0, bid=0.0, ask=0.0)

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_tick_loop_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test tick loop processes message.

    Given: A running publisher with tick data,
    When: Tick arrives,
    Then: Message published and timestamp updated.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(symbol="BTC-USD", last=10.0, volume=5.0, bid=1.0, ask=2.0)
        pub.running = False

    pub._exchange_client.subscribe_ticks = lambda symbols: gen()
    await pub._tick_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_awaited_once()
    assert pub._last_data_timestamps["BTC-USD"] > 0


@pytest.mark.asyncio
async def test_trade_loop_handles_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade loop handles exception.

    Given: A trade loop with failing generator,
    When: Generator raises exception,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        for _ in []:
            yield
        raise RuntimeError("boom")

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])


@pytest.mark.asyncio
async def test_heartbeat_loop_runs_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test heartbeat loop runs once.

    Given: A running publisher with heartbeat config,
    When: Heartbeat loop runs,
    Then: Heartbeat is published and sequence incremented.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_data_timestamps["BTC-USD"] = datetime.now(UTC).timestamp() * 1000
    publish = AsyncMock()

    async def publish_and_stop(topic: str, message: HeartbeatData) -> None:
        pub.running = False
        await publish(topic, message)

    monkeypatch.setattr(pub, "_publish_heartbeat", publish_and_stop)
    await pub._heartbeat_loop()
    publish.assert_awaited_once()
    assert pub.heartbeat_seq == 1


@pytest.mark.asyncio
async def test_publish_heartbeat_skips_when_not_running() -> None:
    """Test heartbeat skips when not running.

    Given: A publisher that is not running,
    When: Heartbeat is published,
    Then: Message is not sent.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.msg_publisher = AsyncMock()
    pub.running = False
    await pub._publish_heartbeat(
        "heartbeat.c",
        HeartbeatData(
            session_id="",
            sequence_id=0,
            component="c",
            sequence=1,
            status="healthy",
            lag_ms=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    )
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_heartbeat_logs_errors() -> None:
    """Test heartbeat logs errors.

    Given: A publisher with failing send,
    When: Heartbeat publish fails,
    Then: Error is logged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    failing = AsyncMock()
    failing.send.side_effect = RuntimeError("boom")
    pub.msg_publisher = failing
    pub.running = True
    await pub._publish_heartbeat(
        "heartbeat.c",
        HeartbeatData(
            session_id="",
            sequence_id=0,
            component="c",
            sequence=1,
            status="healthy",
            lag_ms=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        ),
    )
    failing.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_save_to_db_logs_errors() -> None:
    """Test save_to_db logs errors.

    Given: A publisher with failing repository,
    When: DB save fails,
    Then: Error is logged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(side_effect=RuntimeError("db fail")),
        upsert_candles=AsyncMock(),
    )
    candle = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        volume=1.0,
        timeframe="1m",
        open=10.0,
        high=10.0,
        low=10.0,
        close=10.0,
        timestamp=datetime.now(tz=UTC),
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    await pub._save_to_db("BTC-USD", candle)


@pytest.mark.asyncio
async def test_symbol_aliases_loop_without_subscriber() -> None:
    """Test symbol aliases loop without subscriber.

    Given: A publisher without subscriber,
    When: Symbol mappings loop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub._symbol_aliases_loop()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_outer_exception() -> None:
    """Test symbol aliases loop handles outer exception.

    Given: A publisher with failing running check,
    When: Exception raised in loop,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.subscriber = StubValidatedSubscriber()

    class Boom:
        def __bool__(self) -> bool:
            raise RuntimeError("boom")

    pub.running = Boom()
    await pub._symbol_aliases_loop()


def test_init_warns_when_no_symbols() -> None:
    """Test init with empty symbols.

    Given: Empty symbols list,
    When: Publisher is created,
    Then: No error occurs.
    """
    DummyPublisher(symbols=[])


@pytest.mark.asyncio
async def test_invalidate_symbol_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test invalidate symbol cache.

    Given: A publisher with symbol mapper,
    When: Cache invalidation is triggered,
    Then: Symbol mapper cache is invalidated.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    triggered = Mock()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.SymbolMapperService.get_instance",
        lambda: SimpleNamespace(trigger_cache_invalidation=triggered),
    )
    pub._invalidate_symbol_cache()
    triggered.assert_called_once_with(fail_fast=False)


@pytest.mark.asyncio
async def test_stop_noop_when_not_running() -> None:
    """Test stop is no-op when not running.

    Given: A publisher that is not running,
    When: Stop is called,
    Then: Returns immediately.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    await pub.stop()


@pytest.mark.asyncio
async def test_stop_skips_missing_resources() -> None:
    """Test stop skips missing resources.

    Given: A running publisher without resources,
    When: Stop is called,
    Then: No error occurs.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    await pub.stop()


@pytest.mark.asyncio
async def test_candle_loop_processes_message(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test candle loop processes message.

    Given: A running publisher with candle data,
    When: Candle arrives,
    Then: Message published and saved to DB.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")),
        upsert_candles=AsyncMock(),
    )
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            vwap=1.2,
            volume=10.0,
            trades=5,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
    await pub._candle_loop(["BTC-USD"], "1m")
    pub.msg_publisher.send.assert_awaited_once()
    pub._ensure_instrument.assert_awaited()
    pub.repository.upsert_candles.assert_awaited_once()
    assert pub._last_data_timestamps["BTC-USD"] > 0


@pytest.mark.asyncio
async def test_trade_loop_breaks_when_not_running(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test trade loop breaks when not running.

    Given: A publisher that is not running,
    When: Trade arrives,
    Then: Message is not published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub.msg_publisher = AsyncMock()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD", price=1.0, quantity=1.0, side="buy", timestamp=datetime.now(UTC)
        )

    pub._exchange_client.subscribe_trades = lambda symbols: gen()
    await pub._trade_loop(["BTC-USD"])
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_heartbeat_loop_handles_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test heartbeat loop handles errors.

    Given: A publisher with failing heartbeat publish,
    When: Heartbeat fails,
    Then: Exception is handled gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0
    pub._last_data_timestamps["BTC-USD"] = datetime.now(UTC).timestamp() * 1000

    async def raise_error(*_args: Any, **_kwargs: Any) -> None:
        pub.running = False
        raise RuntimeError("hb failure")

    monkeypatch.setattr(pub, "_publish_heartbeat", raise_error)
    await pub._heartbeat_loop()


@pytest.mark.asyncio
async def test_save_to_db_invalid_symbol_logs_warning() -> None:
    """Test save_to_db logs warning for invalid symbol.

    Given: A publisher saving invalid symbol,
    When: Save is called,
    Then: Warning is logged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    candle = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BAD",
        volume=1.0,
        timeframe="1m",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        timestamp=datetime.now(tz=UTC),
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    await pub._save_to_db("INVALID", candle)


@pytest.mark.asyncio
async def test_save_to_db_returns_when_ensure_instrument_none() -> None:
    """Verify _save_to_db returns early when _ensure_instrument yields None.

    Given: A publisher whose _ensure_instrument resolves to None,
    When: _save_to_db is called,
    Then: No candle is persisted and the method returns without error.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.repository = SimpleNamespace(upsert_candles=AsyncMock())
    candle = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        volume=1.0,
        timeframe="1m",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        timestamp=datetime.now(tz=UTC),
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    with patch.object(pub, "_ensure_instrument", new=AsyncMock(return_value=None)):
        await pub._save_to_db("BTC-USD", candle)
    pub.repository.upsert_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_save_to_db_uses_cached_instrument() -> None:
    """Verify save_to_db uses cached instrument ID.

    Given a publisher with a cached instrument ID,
    When _save_to_db is called for the cached symbol,
    Then the cached ID is used without querying the database.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._instrument_cache["BTC-USD"] = (7, "inst-pub-7")
    pub.repository = SimpleNamespace(upsert_candles=AsyncMock())
    candle = CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument="BTC-USD",
        volume=1.0,
        timeframe="1m",
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        timestamp=datetime.now(tz=UTC),
        exchange="kraken",
        open_at=datetime.now(UTC),
    )
    await pub._save_to_db("BTC-USD", candle)
    pub.repository.upsert_candles.assert_awaited_once()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_retries_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify symbol aliases loop retries after error.

    Given a publisher running the symbol aliases loop,
    When recv_multipart raises a RuntimeError,
    Then the loop retries after sleeping.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    pub.subscriber = subscriber
    pub.running = True
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("failure")
        pub.running = False
        return ("noop", b"{}")

    subscriber.recv_multipart.side_effect = recv
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await pub._symbol_aliases_loop()
    assert calls == 2


def test_get_status() -> None:
    """Verify get_status returns expected publisher info.

    Given a publisher instance,
    When get_status is called,
    Then the returned dict contains symbols and exchange name.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    status = pub.get_status()
    assert status["symbols"] == ["BTC-USD"]
    assert status["exchange"] == "kraken"


def test_get_max_symbols_per_connection_default() -> None:
    """Verify default max symbols per connection is zero.

    Given a publisher instance,
    When _get_max_symbols_per_connection is called,
    Then it returns zero as the default value.
    """
    assert DummyPublisher(symbols=["BTC-USD"])._get_max_symbols_per_connection() == 0


@pytest.mark.asyncio
async def test_candle_loop_breaks_when_stopped() -> None:
    """Verify candle loop exits when running flag is cleared.

    Given a publisher with running=True,
    When the running flag is cleared during iteration,
    Then the candle loop exits gracefully.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")),
        upsert_candles=AsyncMock(),
    )
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            volume=1.0,
            trades=1,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
    await pub._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
async def test_candle_loop_stops_when_running_false() -> None:
    """Verify candle loop does not publish when not running.

    Given a publisher with running=False,
    When _candle_loop is invoked,
    Then no messages are published.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = False
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace(
        ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")),
        upsert_candles=AsyncMock(),
    )
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=1.0,
            low=1.0,
            close=1.0,
            vwap=1.0,
            volume=1.0,
            trades=1,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
    await pub._candle_loop(["BTC-USD"], "1m")
    pub.msg_publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_heartbeat_loop_breaks_after_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify heartbeat loop exits after running flag cleared during sleep.

    Given a publisher with running=True,
    When the running flag is cleared during asyncio.sleep,
    Then the heartbeat loop exits without publishing.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.settings.zmq_heartbeat_interval_ms = 0

    async def sleep_and_stop(_delay: float) -> None:
        pub.running = False

    monkeypatch.setattr(asyncio, "sleep", sleep_and_stop)
    pub._publish_heartbeat = AsyncMock()
    await pub._heartbeat_loop()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_handles_timeout() -> None:
    """Verify symbol aliases loop handles timeout gracefully.

    Given a publisher running the symbol aliases loop,
    When recv_multipart raises a TimeoutError,
    Then the loop continues without crashing.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    pub.subscriber = subscriber
    pub.running = True
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError()
        pub.running = False
        return ("noop", b"{}")

    subscriber.recv_multipart.side_effect = recv
    await pub._symbol_aliases_loop()


@pytest.mark.asyncio
async def test_symbol_aliases_loop_handles_settings_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify symbol mappings loop processes settings updates.

    Given a publisher running the symbol mappings loop,
    When a settings update message is received,
    Then _handle_settings_update is invoked.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    subscriber = StubValidatedSubscriber()
    calls = 0

    async def recv() -> tuple[str, bytes]:
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                "system.settings",
                b'{"type":"setting_changed","session_id":"","sequence_id":0,"key":"foo","value":"bar"}',
            )
        pub.running = False
        await asyncio.sleep(0)
        return "noop", b"{}"

    subscriber.recv_multipart.side_effect = recv
    pub.subscriber = subscriber
    pub.running = True
    handle_mock = MagicMock()
    monkeypatch.setattr(pub, "_handle_settings_update", handle_mock)
    await pub._symbol_aliases_loop()
    handle_mock.assert_called_once()


@pytest.mark.asyncio
async def test_handle_settings_update_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify successful settings update handling.

    Given a valid settings update payload,
    When _handle_settings_update is called,
    Then the settings cache is updated with the new value.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    envelope = SettingChangedData(
        session_id="",
        sequence_id=0,
        key="test_key",
        value="test_value",
        category="test",
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    payload = envelope.to_json().encode()
    mock_instance = SimpleNamespace(_cache={}, _parse_value=lambda v: v)
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.SettingsService.get_instance",
        lambda: mock_instance,
    )
    pub._handle_settings_update(payload, "kraken")
    assert mock_instance._cache["test_key"] == "test_value"


@pytest.mark.asyncio
async def test_handle_settings_update_invalid_json() -> None:
    """Verify invalid JSON is handled without error.

    Given a payload containing invalid JSON,
    When _handle_settings_update is called,
    Then no exception is raised.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub._handle_settings_update(b"not-json", "kraken")


@pytest.mark.asyncio
async def test_handle_settings_update_no_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify settings update without service instance is handled.

    Given SettingsService.get_instance returns None,
    When _handle_settings_update is called,
    Then no exception is raised.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    envelope = SettingChangedData(
        session_id="",
        sequence_id=0,
        key="test_key",
        value="test_value",
        category="test",
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    payload = envelope.to_json().encode()
    monkeypatch.setattr(
        "snapper.messaging.publishers.base.SettingsService.get_instance",
        lambda: None,
    )
    pub._handle_settings_update(payload, "kraken")


class TestFeedPublisherCoverage:
    """Tests for FeedPublisher coverage scenarios."""

    @patch("snapper.config.settings.get_settings")
    def test_initialization(self, mock_get_settings: MagicMock) -> None:
        """Verify publisher initializes with correct default state.

        Given: Mocked settings with ZMQ endpoint,
        When: KrakenMarketDataPublisher is created,
        Then: Symbols stored and running=False.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "EUR-USD", "BTC-EUR"])
        assert publisher.symbols == ["BTC-USD", "EUR-USD", "BTC-EUR"]
        assert publisher.running is False
        assert publisher.heartbeat_seq == 0
        assert publisher.repository is None

    @patch("snapper.config.settings.get_settings")
    def test_get_status(self, mock_get_settings: MagicMock) -> None:
        """Verify status returns current publisher state.

        Given: A publisher instance,
        When: get_status is called,
        Then: Returns dict with running, symbols, broker_endpoint, heartbeat.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        status = publisher.get_status()
        assert status["running"] is False
        assert status["symbols"] == ["BTC-USD"]
        assert "broker_endpoint" in status
        assert status["heartbeat_seq"] == 0

    @pytest.mark.asyncio
    @patch("snapper.data.repository.get_repository")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.KrakenExchangeClient")
    @patch("snapper.application.services.settings.get_settings_service")
    @patch("snapper.config.settings.get_settings_with_service")
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.base.zmq.asyncio.Context")
    async def test_start_creates_sockets(
        self,
        mock_context_class: MagicMock,
        mock_get_settings: MagicMock,
        mock_get_settings_with_service: MagicMock,
        mock_get_settings_service: MagicMock,
        mock_exchange_client_class: MagicMock,
        mock_get_repository: MagicMock,
    ) -> None:
        """Verify start creates required ZMQ sockets.

        Given: A publisher with mocked dependencies,
        When: start is called,
        Then: PUB and SUB sockets are created.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = mock_settings
        mock_context = MagicMock()
        mock_socket = MagicMock()
        mock_context.socket.return_value = mock_socket
        mock_context_class.return_value = mock_context
        mock_exchange_client = AsyncMock()
        mock_exchange_client_class.return_value = mock_exchange_client
        mock_repo = MagicMock()
        mock_repo.get_latest_candle_ids = AsyncMock(return_value={})
        mock_get_repository.return_value = mock_repo
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        with (
            patch.object(publisher, "_heartbeat_loop", new=AsyncMock()),
            patch.object(publisher, "_symbol_aliases_loop", new=AsyncMock()),
            patch(
                "snapper.application.services.settings.zmq.asyncio.Context",
                return_value=mock_context,
            ),
            patch.object(publisher, "_candle_loop", new=AsyncMock()),
            patch.object(publisher, "_tick_loop", new=AsyncMock()),
            patch.object(publisher, "_trade_loop", new=AsyncMock()),
            patch.object(publisher, "_create_exchange_client", return_value=mock_exchange_client),
        ):
            start_task = asyncio.create_task(publisher.start())
            await asyncio.sleep(0.01)
            publisher.running = False
            await start_task
        assert mock_context.socket.call_count >= 2
        socket_calls = [call[0][0] for call in mock_context.socket.call_args_list]
        assert zmq.PUB in socket_calls
        assert zmq.SUB in socket_calls
        mock_exchange_client.connect.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.services.settings.get_settings_service")
    @patch("snapper.config.settings.get_settings_with_service")
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.messaging.publishers.base.zmq.asyncio.Context")
    async def test_start_with_lvc_enabled(
        self,
        mock_context_class: MagicMock,
        mock_get_settings: MagicMock,
        mock_get_settings_with_service: MagicMock,
        mock_get_settings_service: MagicMock,
    ) -> None:
        """Verify publisher stops correctly with LVC enabled.

        Given: A running publisher with LVC,
        When: stop is called,
        Then: Sockets closed and context terminated.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = TEST_DB_URL
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        mock_settings_service = AsyncMock()
        mock_get_settings_service.return_value = mock_settings_service
        mock_get_settings_with_service.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        mock_pub_socket = MagicMock()
        mock_context = MagicMock()
        publisher.publisher = mock_pub_socket
        publisher.context = mock_context
        publisher.running = True
        await publisher.stop()
        assert publisher.running is False
        mock_pub_socket.close.assert_called_once()
        mock_context.term.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_start_already_running(self, mock_get_settings: MagicMock) -> None:
        """Verify start returns when already running.

        Given: A publisher that is already running,
        When: start is called,
        Then: Returns without creating new context.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        await publisher.start()
        assert publisher.context is None

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_stop_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify stop handles non-running publisher.

        Given: A publisher that is not running,
        When: stop is called,
        Then: Returns without error.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = False
        await publisher.stop()

    @patch("snapper.config.settings.get_settings")
    def test_get_default_kwargs(self, mock_get_settings: MagicMock) -> None:
        """Verify get_default_kwargs extracts symbols from settings.

        Given: Settings with instruments.kraken symbols,
        When: get_default_kwargs is called,
        Then: Returns dict with symbols list.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD", "EUR-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        kwargs = KrakenMarketDataPublisher.get_default_kwargs(mock_settings)
        assert kwargs["symbols"] == ["BTC-USD", "EUR-USD"]

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_when_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message sends when running.

        Given: A running publisher with mock socket,
        When: _publish_message is called,
        Then: Message is sent via msg_publisher.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publisher_any.msg_publisher = AsyncMock()
        message = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTC-USD",
            exchange="kraken",
            volume=1.0,
            last=100.0,
        )
        await publisher_any._publish_message("market.kraken.BTC-USD.ticks", message)
        publisher_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message skips when not running.

        Given: A non-running publisher,
        When: _publish_message is called,
        Then: Message is not sent.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = False
        publisher_any.msg_publisher = AsyncMock()
        message = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTC-USD",
            exchange="kraken",
            volume=1.0,
            last=100.0,
        )
        await publisher_any._publish_message("market.kraken.BTC-USD.ticks", message)
        publisher_any.msg_publisher.send.assert_not_called()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("snapper.config.settings.get_settings")
    async def test_heartbeat_loop_publishes(
        self,
        mock_get_settings: MagicMock,
        mock_sleep: AsyncMock,
    ) -> None:
        """Verify heartbeat loop publishes heartbeats.

        Given: A running publisher with symbols,
        When: _heartbeat_loop runs,
        Then: Heartbeats are published with symbol metadata.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 10
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publisher_any.symbols = ["BTC-USD"]
        publisher_any._last_data_timestamps["BTC-USD"] = datetime.now(UTC).timestamp() * 1000 - 25
        publish_heartbeat_mock = AsyncMock()

        async def publish_side_effect(topic: str, message: HeartbeatData) -> None:
            assert "symbols" in message.meta
            assert "BTC-USD" in message.meta["symbols"]
            assert message.meta["symbol_count"] == 1
            publisher_any.running = False

        publish_heartbeat_mock.side_effect = publish_side_effect
        publisher_any._publish_heartbeat = publish_heartbeat_mock

        async def noop_sleep(_delay: float) -> None:
            return None

        mock_sleep.side_effect = noop_sleep
        await publisher_any._heartbeat_loop()
        publish_heartbeat_mock.assert_awaited_once()
        assert publisher_any.heartbeat_seq == 1

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_publish_heartbeat_when_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_heartbeat sends when running.

        Given: A running publisher with mock socket,
        When: _publish_heartbeat is called,
        Then: Heartbeat message is sent via msg_publisher.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publisher_any.msg_publisher = AsyncMock()
        heartbeat = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="feed.kraken.BTC-USD",
            sequence=1,
            status="healthy",
            lag_ms=0,
        )
        await publisher_any._publish_heartbeat("heartbeat.feed.kraken.BTC-USD", heartbeat)
        publisher_any.msg_publisher.send.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_candle_loop_processes_messages(self, mock_get_settings: MagicMock) -> None:
        """Verify candle loop processes candle messages.

        Given: A running publisher with mock exchange client,
        When: _candle_loop processes candles,
        Then: Messages are published and saved to DB.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publish_mock = AsyncMock()
        save_mock = AsyncMock()
        publisher_any._publish_message = publish_mock
        publisher_any._save_to_db = save_mock

        async def generator() -> AsyncIterator[CandleUpdate]:
            yield CandleUpdate(
                symbol="BTC-USD",
                open=99.0,
                high=105.0,
                low=98.0,
                close=101.0,
                vwap=100.0,
                trades=3,
                volume=12.0,
                interval_begin=datetime.now(UTC),
                interval=1,
            )
            yield CandleUpdate(
                symbol="BTC-USD",
                open=101.0,
                high=103.0,
                low=100.0,
                close=102.0,
                vwap=101.5,
                trades=2,
                volume=1.5,
                interval_begin=datetime.now(UTC),
                interval=1,
            )

        async def candle_stream(
            _symbols: list[str], _timeframe: str
        ) -> AsyncIterator[CandleUpdate]:
            async for item in generator():
                yield item

        ensure_instrument_mock = AsyncMock(return_value=(1, "inst-pub-1"))
        publisher_any._ensure_instrument = ensure_instrument_mock
        publisher_any._exchange_client = cast(
            Any,
            SimpleNamespace(subscribe_candles=candle_stream),
        )
        await publisher_any._candle_loop(["BTC-USD"], "1m")
        assert publish_mock.await_count == 2
        assert save_mock.await_count == 2
        assert "BTC-USD" in publisher_any._last_data_timestamps

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_tick_loop_processes_tick_message(self, mock_get_settings: MagicMock) -> None:
        """Verify tick loop processes tick messages.

        Given: A running publisher with mock exchange client,
        When: _tick_loop processes ticks,
        Then: Tick messages are published with correct topic.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publish_mock = AsyncMock()
        publisher_any._publish_message = publish_mock

        async def generator() -> AsyncIterator[TickerUpdate]:
            yield TickerUpdate(
                symbol="BTC-USD",
                bid=122.0,
                bid_qty=1.0,
                ask=124.0,
                ask_qty=1.5,
                last=123.0,
                volume=4.0,
                vwap=122.5,
                low=119.0,
                high=125.0,
                change=3.0,
                change_pct=2.5,
            )

        async def tick_stream(_symbols: list[str]) -> AsyncIterator[TickerUpdate]:
            async for item in generator():
                yield item

        publisher_any._exchange_client = cast(
            Any,
            SimpleNamespace(subscribe_ticks=tick_stream),
        )
        await publisher_any._tick_loop(["BTC-USD"])
        publish_mock.assert_awaited_once()
        assert "BTC-USD" in publisher_any._last_data_timestamps

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_trade_loop_processes_trade_messages(self, mock_get_settings: MagicMock) -> None:
        """Verify trade loop processes trade messages.

        Given: A running publisher with mock exchange client,
        When: _trade_loop processes trades,
        Then: Trade messages are published with correct topic.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True
        publish_mock = AsyncMock()
        publisher_any._publish_message = publish_mock

        async def generator() -> AsyncIterator[TradeUpdate]:
            yield TradeUpdate(
                symbol="BTC-USD",
                side="buy",
                quantity=0.5,
                price=130.0,
                ord_type="market",
                trade_id=12345,
                timestamp=datetime.now(UTC),
            )
            yield TradeUpdate(
                symbol="BTC-USD",
                side="sell",
                quantity=1.0,
                price=129.0,
                ord_type="market",
                trade_id=12346,
                timestamp=datetime.now(UTC),
            )

        async def trade_stream(_symbols: list[str]) -> AsyncIterator[TradeUpdate]:
            async for item in generator():
                yield item

        publisher_any._exchange_client = cast(
            Any,
            SimpleNamespace(subscribe_trades=trade_stream),
        )
        await publisher_any._trade_loop(["BTC-USD"])
        assert publish_mock.await_count == 2
        assert "BTC-USD" in publisher_any._last_data_timestamps

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    async def test_save_to_db_inserts_candle(
        self,
        mock_get_settings: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify save_to_db inserts candle to database.

        Given: A publisher with mock repository,
        When: _save_to_db is called with candle message,
        Then: Instrument and candle are upserted.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.db_url = "sqlite:///:memory:"
        mock_get_settings.return_value = mock_settings
        mock_repository = SimpleNamespace(
            ensure_instrument=AsyncMock(return_value=(42, "inst-pub-42")),
            upsert_candles=AsyncMock(),
        )
        resolve_mock = AsyncMock(return_value="fake-spid")
        monkeypatch.setattr(
            "snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock
        )
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.repository = mock_repository
        publisher_any = cast(Any, publisher)
        bar_message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTC-USD",
            exchange="kraken",
            volume=5.0,
            timeframe="1m",
            open=108.0,
            high=112.0,
            low=107.0,
            close=110.0,
            vwap=109.5,
            trades=7,
            open_at=datetime.now(UTC),
        )
        await publisher_any._save_to_db("BTC-USD", bar_message)
        resolve_mock.assert_awaited_once_with(mock_repository, "BTC-USD")
        call_kwargs = mock_repository.ensure_instrument.call_args.kwargs
        assert call_kwargs["symbol_public_id"] == "fake-spid"
        assert call_kwargs["exchange"] == "kraken"
        assert call_kwargs["session_id"] != ""
        assert call_kwargs["sequence_id"] >= 1
        mock_repository.upsert_candles.assert_awaited_once()
        await publisher_any._save_to_db("BTC-USD", bar_message)
        mock_repository.ensure_instrument.assert_awaited_once()
        assert publisher_any._instrument_cache["BTC-USD"] == "inst-pub-42"

    @pytest.mark.asyncio
    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.symbols.mapper.SymbolMapperService.get_instance")
    async def test_symbol_aliases_loop_refreshes_cache(
        self,
        mock_get_instance: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify symbol aliases loop refreshes cache.

        Given: A running publisher with symbol mapper,
        When: _symbol_aliases_loop receives message,
        Then: Cache invalidation is triggered.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_db_mapper = MagicMock()
        mock_get_instance.return_value = mock_db_mapper
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher_any = cast(Any, publisher)
        publisher_any.running = True

        async def recv_stub() -> tuple[str, bytes]:
            publisher_any.running = False
            return ("system.symbol_aliases", b"{}")

        publisher_any.subscriber = SimpleNamespace(recv_multipart=recv_stub)
        await publisher_any._symbol_aliases_loop()
        mock_db_mapper.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)


class DummyRepository:
    """Test stub for database repository."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.instrument_calls: list[dict[str, Any]] = []
        self.candle_calls: list[list[dict[str, Any]]] = []
        self._next_id = 100

    async def ensure_instrument(self, **kwargs: Any) -> tuple[int, str]:
        """Upsert instrument to repository."""
        self.instrument_calls.append(kwargs)
        current_id = self._next_id
        self._next_id += 1
        return (current_id, f"inst-pub-{current_id}")

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        """Upsert candles to repository."""
        self.candle_calls.append(rows)
        return len(rows)


def _build_bar_message(instrument: str) -> CandleData:
    return CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument=instrument,
        exchange="kraken",
        volume=12.5,
        timeframe="1m",
        open=1.2,
        high=1.3,
        low=1.1,
        close=1.24,
        vwap=1.23,
        trades=7,
        open_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_save_to_db_caches_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify instrument caching in database save operations.

    Given a publisher with a repository,
    When _save_to_db is called for a new symbol,
    Then the instrument ID is cached for subsequent calls.
    """
    repo = DummyRepository()

    def _native_to_ws(symbol: str) -> str:
        return symbol.replace("-", "/")

    monkeypatch.setattr(
        "snapper.infrastructure.symbols.functions.native_to_kraken_websocket", _native_to_ws
    )
    resolve_mock = AsyncMock(return_value="fake-spid")
    monkeypatch.setattr("snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock)
    publisher = KrakenMarketDataPublisher(symbols=["EUR-USD"])
    publisher.repository = repo
    bar_message = _build_bar_message("EUR-USD")
    await publisher._save_to_db("EUR-USD", bar_message)
    resolve_mock.assert_awaited_once_with(repo, "EUR-USD")
    assert len(repo.instrument_calls) == 1
    call = repo.instrument_calls[0]
    assert call["symbol_public_id"] == "fake-spid"
    assert call["exchange"] == "kraken"
    assert call["session_id"] == publisher._tracker.session_id
    assert call["sequence_id"] == 1
    assert len(repo.candle_calls) == 1
    assert repo.candle_calls[0][0]["instrument_public_id"] == "inst-pub-100"
    cache = publisher._instrument_cache
    assert cache["EUR-USD"] == "inst-pub-100"
    repo.instrument_calls.clear()
    await publisher._save_to_db("EUR-USD", bar_message)
    assert repo.instrument_calls == []
    assert len(repo.candle_calls) == 2


@pytest.mark.asyncio
async def test_save_to_db_handles_invalid_symbol(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Verify invalid symbol is logged and skipped.

    Given a publisher with a repository,
    When _save_to_db is called with an invalid symbol,
    Then the operation is skipped and a warning is logged.
    """
    repo = DummyRepository()

    def _native_to_ws(symbol: str) -> str:
        return symbol.replace("-", "/")

    monkeypatch.setattr(
        "snapper.infrastructure.symbols.functions.native_to_kraken_websocket", _native_to_ws
    )
    publisher = KrakenMarketDataPublisher(symbols=["EUR-USD"])
    publisher.repository = repo
    invalid_message = _build_bar_message("INVALID")
    with caplog.at_level("WARNING"):
        await publisher._save_to_db("INVALID", invalid_message)
    assert repo.instrument_calls == []
    assert repo.candle_calls == []


class PublisherSocketStub:
    """Test stub for publisher socket."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize the instance."""
        self.calls: list[tuple[str, bytes]] = []
        self.error = error

    async def send_multipart(self, topic: str, payload: bytes) -> None:
        """Send multipart message."""
        if self.error is not None:
            raise self.error
        self.calls.append((topic, payload))


class MessagePublisherStub:
    """Test stub for MessagePublisher wrapper."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize the instance."""
        self.calls: list[tuple[str, Any]] = []
        self.error = error

    async def send(self, stream_key: str, data: Any, *, flags: int = 0) -> None:
        """Send a message."""
        if self.error is not None:
            raise self.error
        self.calls.append((stream_key, data))


@pytest.mark.asyncio
class TestFeedPublisherCandleLoop:
    """Tests for FeedPublisher candle loop functionality."""

    @patch("snapper.config.settings.get_settings")
    async def test_candle_loop_processes_candles(
        self,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop processes and publishes candles.

        Given: A running publisher with mock exchange client,
        When: _candle_loop processes candles,
        Then: Bar messages are published with correct topics.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        mock_exchange_client = SimpleNamespace()
        btc_candle = CandleUpdate(
            symbol="BTC-USD",
            open=49900.0,
            high=50100.0,
            low=49800.0,
            close=50000.0,
            vwap=50000.0,
            trades=42,
            volume=100.5,
            interval_begin=datetime.now(UTC),
            interval=1,
        )
        eth_candle = CandleUpdate(
            symbol="ETH-USD",
            open=2990.0,
            high=3010.0,
            low=2980.0,
            close=3000.0,
            vwap=3000.0,
            trades=100,
            volume=500.0,
            interval_begin=datetime.now(UTC),
            interval=1,
        )

        async def mock_subscribe(symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
            yield btc_candle
            yield eth_candle

        mock_exchange_client.subscribe_candles = mock_subscribe
        publisher_any._exchange_client = mock_exchange_client
        published_messages: list[tuple[str, CandleData]] = []

        async def publish_stub(topic: str, message: CandleData) -> None:
            published_messages.append((topic, message))

        saved_payloads: list[tuple[str, CandleData]] = []

        async def save_stub(symbol: str, envelope: CandleData) -> None:
            saved_payloads.append((symbol, envelope))

        publisher_any._publish_message = publish_stub
        publisher_any._save_to_db = save_stub
        publisher_any._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
        await publisher_any._candle_loop(
            ["BTC-USD", "ETH-USD"],
            "1m",
        )
        assert len(published_messages) == 2
        first_topic, btc_msg = published_messages[0]
        assert first_topic == "market.kraken.BTC-USD.candles.1m"
        assert isinstance(btc_msg, CandleData)
        assert btc_msg.type == "candle"
        assert btc_msg.instrument == "BTC-USD"
        assert btc_msg.close == pytest.approx(50000.0)
        assert btc_msg.volume == pytest.approx(100.5)
        assert btc_msg.open == pytest.approx(49900.0)
        assert btc_msg.high == pytest.approx(50100.0)
        assert btc_msg.low == pytest.approx(49800.0)
        assert btc_msg.vwap == pytest.approx(50000.0)
        assert btc_msg.trades == 42
        second_topic, eth_msg = published_messages[1]
        assert second_topic == "market.kraken.ETH-USD.candles.1m"
        assert isinstance(eth_msg, CandleData)
        assert eth_msg.type == "candle"
        assert eth_msg.instrument == "ETH-USD"
        assert eth_msg.close == pytest.approx(3000.0)
        assert len(saved_payloads) == 2
        assert "BTC-USD" in publisher_any._last_data_timestamps
        assert "ETH-USD" in publisher_any._last_data_timestamps

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.KrakenExchangeClient")
    async def test_candle_loop_stops_when_not_running(
        self,
        mock_client_class: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop stops when publisher is not running.

        Given: A publisher that becomes not running,
        When: _candle_loop is executing,
        Then: Loop terminates gracefully.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_exchange_client = SimpleNamespace()

        async def mock_subscribe(symbols: list[str], timeframe: str) -> AsyncIterator[CandleUpdate]:
            while True:
                await asyncio.sleep(0.1)
                yield CandleUpdate(
                    symbol="BTC-USD",
                    open=50000.0,
                    high=50100.0,
                    low=49900.0,
                    close=50050.0,
                    vwap=50000.0,
                    trades=10,
                    volume=100.0,
                    interval_begin=datetime.now(UTC),
                    interval=1,
                )

        mock_exchange_client.subscribe_candles = mock_subscribe
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        publisher_any._exchange_client = mock_exchange_client
        published_topics: list[str] = []

        async def publish_stub(topic: str, message: CandleData) -> None:
            published_topics.append(topic)

        publisher_any._publish_message = publish_stub
        publisher_any._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
        task = asyncio.create_task(publisher_any._candle_loop(["BTC-USD"], "1m"))
        await asyncio.sleep(0.05)
        publisher.running = False
        await asyncio.wait_for(task, timeout=1.0)
        assert not published_topics

    @patch("snapper.config.settings.get_settings")
    @patch("snapper.infrastructure.exchanges.implementations.kraken.KrakenExchangeClient")
    async def test_candle_loop_handles_exception(
        self,
        mock_client_class: MagicMock,
        mock_get_settings: MagicMock,
    ) -> None:
        """Verify candle loop handles exceptions gracefully.

        Given: A publisher with failing subscription,
        When: _candle_loop encounters error,
        Then: Exception is handled without crash.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        mock_exchange_client = SimpleNamespace()

        async def mock_subscribe_error(
            symbols: list[str], timeframe: str
        ) -> AsyncIterator[CandleUpdate]:
            for _ in []:
                yield
            raise RuntimeError("Subscription failed")

        mock_exchange_client.subscribe_candles = mock_subscribe_error
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        publisher_any._exchange_client = mock_exchange_client
        publisher_any._ensure_instrument = AsyncMock(return_value=(1, "inst-pub-1"))
        await publisher_any._candle_loop(["BTC-USD"], "1m")


@pytest.mark.asyncio
class TestFeedPublisherPublishMessage:
    """Tests for FeedPublisher publish message functionality."""

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_sends_multipart(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message sends multipart message.

        Given: A running publisher with socket stub,
        When: _publish_message is called with candle message,
        Then: Message is sent with correct topic and payload.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        msg_pub = MessagePublisherStub()
        publisher_any.msg_publisher = msg_pub
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=49900.0,
            high=50100.0,
            low=49800.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )
        assert len(msg_pub.calls) == 1
        sent_topic, sent_message = msg_pub.calls[0]
        assert sent_topic == "market.kraken.BTC-USD.candles.1m"
        assert sent_message.type == "candle"
        assert sent_message.instrument == "BTC-USD"

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_not_running(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message skips when not running.

        Given: A non-running publisher,
        When: _publish_message is called,
        Then: No message is sent.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = False
        publisher_any = cast(Any, publisher)
        msg_pub = MessagePublisherStub()
        publisher_any.msg_publisher = msg_pub
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=50000.0,
            high=50000.0,
            low=50000.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )
        assert not msg_pub.calls

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_no_socket(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message handles missing socket.

        Given: A running publisher with no socket,
        When: _publish_message is called,
        Then: No error is raised.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher.msg_publisher = None
        publisher_any = cast(Any, publisher)
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=50000.0,
            high=50000.0,
            low=50000.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )

    @patch("snapper.config.settings.get_settings")
    async def test_publish_message_handles_exception(self, mock_get_settings: MagicMock) -> None:
        """Verify publish_message handles send exception.

        Given: A running publisher with failing socket,
        When: _publish_message is called,
        Then: Exception is handled gracefully.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_get_settings.return_value = mock_settings
        publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
        publisher.running = True
        publisher_any = cast(Any, publisher)
        msg_pub = MessagePublisherStub(error=RuntimeError("Send failed"))
        publisher_any.msg_publisher = msg_pub
        message = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=100.5,
            timeframe="1m",
            open=50000.0,
            high=50000.0,
            low=50000.0,
            close=50000.0,
            open_at=datetime.now(UTC),
        )
        await publisher_any._publish_message(
            "market.kraken.BTC-USD.candles.1m",
            message,
        )
        assert msg_pub.calls == []


@pytest.mark.asyncio
async def test_resolve_candle_public_id_cache_hit() -> None:
    """Verify _resolve_candle_public_id returns cached UUID on open_at match.

    Given: A publisher with a pre-populated _candle_id_cache entry,
    When: _resolve_candle_public_id is called with the same open_at,
    Then: The cached UUID is returned unchanged.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    pub._candle_id_cache[("inst-pub-1", "1m")] = (ts, "existing-uuid")
    result = pub._resolve_candle_public_id("inst-pub-1", "1m", ts)
    assert result == "existing-uuid"


@pytest.mark.asyncio
async def test_resolve_candle_public_id_different_open_at_generates_new() -> None:
    """Verify _resolve_candle_public_id generates new UUID when open_at differs.

    Given: A publisher with a cached candle ID entry,
    When: _resolve_candle_public_id is called with a different open_at,
    Then: A new UUID is generated (not the cached one).
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    ts_old = datetime(2026, 1, 1, tzinfo=UTC)
    ts_new = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    pub._candle_id_cache[("inst-pub-1", "1m")] = (ts_old, "existing-uuid")
    result = pub._resolve_candle_public_id("inst-pub-1", "1m", ts_new)
    assert result != "existing-uuid"
    assert pub._candle_id_cache[("inst-pub-1", "1m")][0] == ts_new


@pytest.mark.asyncio
async def test_candle_loop_skips_publish_when_ensure_instrument_returns_none() -> None:
    """Verify _candle_loop skips publishing when instrument resolution fails.

    Given: A publisher whose _ensure_instrument returns None,
    When: _candle_loop processes a candle,
    Then: _publish_message is never called.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    pub.running = True
    pub.msg_publisher = AsyncMock()
    pub.repository = SimpleNamespace()
    pub._exchange_client = SimpleNamespace()

    async def gen() -> AsyncIterator[Any]:
        yield SimpleNamespace(
            symbol="BTC-USD",
            open=1.0,
            high=2.0,
            low=0.5,
            close=1.5,
            vwap=1.2,
            volume=10.0,
            trades=5,
            interval_begin=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        )
        pub.running = False

    pub._exchange_client.subscribe_candles = lambda symbols, timeframe: gen()
    pub._ensure_instrument = AsyncMock(return_value=None)
    pub._publish_message = AsyncMock()
    await pub._candle_loop(["BTC-USD"], "1m")
    pub._publish_message.assert_not_awaited()


@pytest.mark.asyncio
@patch(
    "snapper.messaging.publishers.base.resolve_symbol_public_id",
    new_callable=AsyncMock,
    return_value=None,
)
async def test_ensure_instrument_returns_none_for_unsplittable_symbol(
    _mock_resolve: AsyncMock,
) -> None:
    """Verify _ensure_instrument returns None when symbol cannot be resolved.

    Given: A publisher with a symbol that has no active Symbol row,
    When: _ensure_instrument is called,
    Then: None is returned.
    """
    pub: Any = DummyPublisher(symbols=["INVALID"])
    pub.repository = SimpleNamespace(ensure_instrument=AsyncMock(return_value=(1, "inst-pub-1")))
    result = await pub._ensure_instrument("INVALID")
    assert result is None


@pytest.mark.asyncio
async def test_ensure_instrument_resolves_and_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _ensure_instrument resolves instrument and caches result.

    Given: A publisher with a valid symbol and mock repository,
    When: _ensure_instrument is called twice with the same symbol,
    Then: The repository is called only once and result is cached.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    mock_upsert = AsyncMock(return_value=(42, "inst-pub-42"))
    pub.repository = SimpleNamespace(ensure_instrument=mock_upsert)
    resolve_mock = AsyncMock(return_value="fake-spid")
    monkeypatch.setattr("snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock)
    first = await pub._ensure_instrument("BTC-USD")
    second = await pub._ensure_instrument("BTC-USD")
    assert first == "inst-pub-42"
    assert second == "inst-pub-42"
    resolve_mock.assert_awaited_once_with(pub.repository, "BTC-USD")
    call_kwargs = mock_upsert.call_args.kwargs
    assert call_kwargs["symbol_public_id"] == "fake-spid"
    assert call_kwargs["exchange"] == "kraken"
    assert call_kwargs["session_id"] != ""
    assert call_kwargs["sequence_id"] >= 1


@pytest.mark.asyncio
async def test_ensure_instrument_returns_none_when_symbol_not_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _ensure_instrument returns None when active Symbol is missing.

    Given: A publisher with a splittable symbol,
    When: symbol resolution returns None,
    Then: The instrument upsert is skipped.
    """
    pub: Any = DummyPublisher(symbols=["BTC-USD"])
    mock_upsert = AsyncMock(return_value=(42, "inst-pub-42"))
    pub.repository = SimpleNamespace(ensure_instrument=mock_upsert)
    resolve_mock = AsyncMock(return_value=None)
    monkeypatch.setattr("snapper.messaging.publishers.base.resolve_symbol_public_id", resolve_mock)
    result = await pub._ensure_instrument("BTC-USD")
    assert result is None
    resolve_mock.assert_awaited_once_with(pub.repository, "BTC-USD")
    mock_upsert.assert_not_awaited()
