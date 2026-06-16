"""Tests for strategy framework and trading signal generation."""

import asyncio
import csv
import gc
import json
import time
import warnings
from collections.abc import Callable
from collections.abc import Coroutine
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import SupportsIndex
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pandas as pd
import pytest
import zmq

from snapper.application.ai_review.service import AiReviewDecisionOutcome
from snapper.application.services.signals.service import signal_service
from snapper.cli.app import _alembic_cfg
from snapper.core.types import AiReviewStatusEnum
from snapper.core.types import OrderExchange
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.core.types import TradeSideEnum
from snapper.infrastructure.historical.polygon.loader import GroupedDailyRow
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.executors.kraken import KrakenOrderExecutor
from snapper.messaging.infrastructure.broker import ZmqBrokerThread
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData
from snapper.messaging.topics.validation import _validate_admin_topic
from snapper.messaging.topics.validation import _validate_orders_commands_topic
from snapper.messaging.topics.validation import _validate_signal_topic
from snapper.messaging.topics.validation import _validate_system_topic
from snapper.messaging.topics.validation import validate_topic
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import CompositeStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.base import StrategySignalResult
from snapper.strategies.base import _db_row_to_warmup_candle
from snapper.strategies.base import _grouped_row_to_warmup_candle
from snapper.strategies.base import _native_to_polygon_crypto_ticker
from snapper.strategies.cointegration import _FET_RENDER_DEFAULT_CONFIG
from snapper.strategies.cointegration import CointegrationPairs
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.factory import StrategyNotFoundError
from snapper.strategies.macd import MACDCrossover
from snapper.strategies.rsi import RSIReversion


@pytest.fixture(autouse=True)
def mock_signal_store(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace signal_service.store_signal with a no-op for all strategy tests.

    All emit_signal tests use mock publishers; calling real store_signal would
    attempt a live database write that is out of scope for unit tests.
    """
    monkeypatch.setattr(signal_service, "store_signal", AsyncMock(return_value=""))


def make_candle_envelope(
    instrument: str = "BTC-USD",
    close: float = 50000.0,
    ts: float | None = None,
    exchange: str = "kraken",
) -> CandleData:
    """Create a CandleData with default test values."""
    return CandleData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        instrument=instrument,
        timeframe="1h",
        open=close - 100,
        high=close + 100,
        low=close - 200,
        close=close,
        volume=1000.0,
        exchange=exchange,
        timestamp=datetime.fromtimestamp(ts, tz=UTC) if ts else datetime.now(UTC),
        open_at=datetime.fromtimestamp(ts, tz=UTC) if ts else datetime.now(UTC),
    )


_TEST_CANDLE_BASE_TS = datetime(2024, 1, 1, tzinfo=UTC).timestamp()


def _next_candle_ts(strategy: BaseStrategy, instrument: str) -> float:
    """Return a per-leg monotonic UTC-day timestamp for a fed candle.

    Stamps each leg's i-th fed bar at ``BASE + i*1d`` (a per-instrument counter on
    the strategy instance), so two legs fed in lockstep share the SAME ``open_at``
    per step and thus align under the cointegration open_at-aligned spread. The
    counter is independent of buffer pruning (it never repeats) and is per-instance
    (no cross-test leakage).

    Args:
        strategy: Strategy whose per-leg counter is advanced.
        instrument: The leg being fed.

    Returns:
        A UTC-day Unix timestamp for the next bar of this leg.
    """
    counters: dict[str, int] = strategy.__dict__.setdefault("_test_candle_seq", {})
    index = counters.get(instrument, 0)
    counters[instrument] = index + 1
    return _TEST_CANDLE_BASE_TS + index * 86400.0


async def feed_bar_returning_group(
    strategy: BaseStrategy,
    instrument: str,
    close: float,
    exchange: str = "kraken",
) -> StrategySignalResult:
    """Feed a candle and return the RAW callback result (list / single / None).

    Unlike :func:`feed_bar_to_strategy` (which collapses a multi-leg list
    to its primary leg so single-signal assertions keep working), this
    returns the full group so multi-leg emission can be asserted leg by
    leg.
    """
    candle = make_candle_envelope(
        instrument, close, ts=_next_candle_ts(strategy, instrument), exchange=exchange
    )
    if instrument not in strategy.candle_buffer:
        strategy.candle_buffer[instrument] = []
    strategy.candle_buffer[instrument].append(candle)
    max_buffer_size = strategy.params.get("buffer_size", 100)
    if len(strategy.candle_buffer[instrument]) > max_buffer_size:
        strategy.candle_buffer[instrument].pop(0)
    return await strategy.on_candle(instrument, candle)


async def feed_bar_to_strategy(
    strategy: BaseStrategy,
    instrument: str,
    close: float,
    exchange: str = "kraken",
) -> StrategySignal | None:
    """Feed a single candle and return the primary resulting signal.

    Collapses the multi-leg return contract to a single signal for the
    many single-signal assertions: a returned ``list[StrategySignal]``
    yields its first (primary / current-instrument) leg, an empty list
    yields ``None``, and a single signal / ``None`` passes through.
    """
    result = await feed_bar_returning_group(strategy, instrument, close, exchange)
    if isinstance(result, list):
        return result[0] if result else None
    return result


async def feed_closes_to_strategy(
    strategy: BaseStrategy,
    instrument: str,
    closes: list[float],
    exchange: str = "kraken",
) -> StrategySignal | None:
    """Feed multiple close prices to strategy sequentially."""
    signal: StrategySignal | None = None
    for close in closes:
        signal = await feed_bar_to_strategy(strategy, instrument, close, exchange)
    return signal


def prefill_candle_buffer(
    strategy: BaseStrategy,
    instrument: str,
    closes: list[float],
    exchange: str = "kraken",
) -> None:
    """Populate strategy candle buffer without triggering signals."""
    if instrument not in strategy.candle_buffer:
        strategy.candle_buffer[instrument] = []
    for close in closes:
        candle = make_candle_envelope(
            instrument, close, ts=_next_candle_ts(strategy, instrument), exchange=exchange
        )
        strategy.candle_buffer[instrument].append(candle)
    max_buffer_size = strategy.params.get("buffer_size", 100)
    while len(strategy.candle_buffer[instrument]) > max_buffer_size:
        strategy.candle_buffer[instrument].pop(0)


class TestStrategyConfig:
    """Test suite for StrategyConfig validation and creation."""

    def test_valid_config(self) -> None:
        """Verify StrategyConfig creates with valid parameters.

        Given: Valid configuration parameters,
        When: StrategyConfig is instantiated,
        Then: All attributes are correctly assigned.
        """
        config = StrategyConfig(
            name="test_strategy",
            strategy_class="TestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
            exchange="paper",
            params={"param1": 10},
        )
        assert config.name == "test_strategy"
        assert config.inputs == ["market.kraken.BTC-USD.candles"]
        assert config.outputs == ["BTC-USD"]
        assert config.exchange == "paper"
        assert config.params == {"param1": 10}

    def test_empty_name_raises_error(self) -> None:
        """Verify empty name raises ValueError.

        Given: Configuration with empty name,
        When: StrategyConfig is instantiated,
        Then: ValueError with 'name cannot be empty' is raised.
        """
        with pytest.raises(ValueError, match="name cannot be empty"):
            StrategyConfig(
                name="",
                strategy_class="TestStrategy",
                inputs=["market.kraken.BTC-USD.candles"],
                outputs=["BTC-USD"],
            )

    def test_empty_inputs_raises_error(self) -> None:
        """Verify empty inputs list raises ValueError.

        Given: Configuration with empty inputs list,
        When: StrategyConfig is instantiated,
        Then: ValueError about requiring inputs is raised.
        """
        with pytest.raises(ValueError, match="must have at least one input"):
            StrategyConfig(
                name="test_strategy",
                strategy_class="TestStrategy",
                inputs=[],
                outputs=["BTC-USD"],
            )

    def test_empty_outputs_raise_error(self) -> None:
        """Verify empty outputs list raises ValueError.

        Given: Configuration with empty outputs list,
        When: StrategyConfig is instantiated,
        Then: ValueError about requiring outputs is raised.
        """
        with pytest.raises(ValueError, match="must define at least one output instrument"):
            StrategyConfig(
                name="test_strategy",
                strategy_class="TestStrategy",
                inputs=["market.kraken.BTC-USD.candles"],
                outputs=[],
            )

    def test_config_with_multiple_inputs(self) -> None:
        """Verify StrategyConfig accepts multiple inputs.

        Given: Configuration with three input topics,
        When: StrategyConfig is instantiated,
        Then: All inputs are preserved.
        """
        config = StrategyConfig(
            name="multi_input",
            strategy_class="CompositeStrategy",
            inputs=[
                "market.kraken.BTC-USD.candles",
                "market.kraken.ETH-USD.candles",
                "signals.kraken.macd",
            ],
            outputs=["BTC-USD"],
        )
        assert len(config.inputs) == 3
        assert "signals.kraken.macd" in config.inputs

    def test_invalid_exchange_raises_error(self) -> None:
        """Verify invalid exchange raises ValueError.

        Given: Configuration with unsupported exchange,
        When: StrategyConfig is instantiated,
        Then: ValueError about exchange validation is raised.
        """
        with pytest.raises(ValueError, match="exchange must be one of"):
            StrategyConfig(
                name="test_strategy",
                strategy_class="TestStrategy",
                inputs=["market.kraken.BTC-USD.candles"],
                outputs=["BTC-USD"],
                exchange="invalid_exchange",
            )

    def test_paper_input_requires_paper_exchange(self) -> None:
        """Verify paper input requires paper exchange.

        Given: Configuration with paper input but kraken exchange,
        When: StrategyConfig is instantiated,
        Then: ValueError about paper input consistency is raised.
        """
        with pytest.raises(ValueError, match="Paper/replay input data MUST use exchange='paper'"):
            StrategyConfig(
                name="test_strategy",
                strategy_class="TestStrategy",
                inputs=["market.paper.kraken.BTC-USD.candles"],
                outputs=["BTC-USD"],
                exchange="kraken",
            )


class TestSignal:
    """Test suite for StrategySignal dataclass."""

    def test_signal_creation(self) -> None:
        """Verify StrategySignal creates with all fields properly set.

        Given: StrategySignal parameters including metadata,
        When: StrategySignal is instantiated,
        Then: All attributes are correctly assigned.
        """
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            reason="MACD crossover",
        )
        assert signal.instrument == "BTC-USD"
        assert signal.side == "buy"
        assert signal.strength == pytest.approx(0.8)
        assert signal.price == pytest.approx(50000.0)
        assert signal.reason == "MACD crossover"


class MockStrategy(BaseStrategy):
    """Mock strategy implementation for testing base class behavior."""

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize the instance."""
        super().__init__(config)
        self.candles_received: list[tuple[str, CandleData]] = []
        self.signals_emitted: list[StrategySignal] = []
        self._trigger_signal: bool = False

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle data and return optional signal."""
        self.candles_received.append((instrument, candle))
        if self._trigger_signal:
            return StrategySignal(
                instrument=instrument,
                side="buy",
                strength=0.8,
                price=candle.close,
                reason="test trigger",
            )
        return None

    async def reset(self) -> None:
        """Reset strategy state."""
        self.candles_received.clear()
        self.signals_emitted.clear()
        self._trigger_signal = False

    async def _subscribe_inputs(self) -> None:
        """No-op subscription management for test strategy."""
        pass

    async def _unsubscribe_inputs(self) -> None:
        """No-op subscription management for test strategy."""
        pass

    async def emit_signal(self, signal: StrategySignal) -> None:
        """Emit a trading signal."""
        self.signals_emitted.append(signal)


class TestBaseStrategy:
    """Test suite for BaseStrategy core functionality."""

    @pytest.mark.asyncio
    async def test_strategy_initialization(self) -> None:
        """Verify BaseStrategy initializes with config values.

        Given: StrategyConfig with name, inputs, outputs, params,
        When: MockStrategy is instantiated,
        Then: All attributes are correctly assigned.
        """
        config = StrategyConfig(
            name="mock_strategy",
            strategy_class="MockStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
            exchange="paper",
            params={"test_param": 123},
        )
        strategy = MockStrategy(config)
        assert strategy.name == "mock_strategy"
        assert strategy.inputs == ["market.kraken.BTC-USD.candles"]
        assert strategy.outputs == ["BTC-USD"]
        assert strategy.output_topics == ["signals.paper.BTC-USD.mock_strategy"]
        assert strategy.params == {"test_param": 123}
        assert strategy._running is False

    @pytest.mark.asyncio
    async def test_strategy_start_stop(self) -> None:
        """Verify start/stop lifecycle toggles running state.

        Given: MockStrategy instance,
        When: start() then stop() are called,
        Then: _running toggles True then False.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="MockStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = MockStrategy(config)
        await strategy.start()
        assert strategy._running is True
        await strategy.stop()
        assert strategy._running is False

    @pytest.mark.asyncio
    async def test_on_candle_processing(self) -> None:
        """Verify on_candle processes candles and optionally returns signals.

        Given: MockStrategy with _trigger_signal flag,
        When: on_candle called with and without trigger,
        Then: StrategySignal returned only when triggered.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="MockStrategy",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        strategy = MockStrategy(config)
        candle = make_candle_envelope("BTC-USD", 50000.0)
        signal = await strategy.on_candle("BTC-USD", candle)
        assert signal is None
        assert len(strategy.candles_received) == 1
        strategy._trigger_signal = True
        signal = await strategy.on_candle("BTC-USD", candle)
        assert signal is not None
        assert signal.instrument == "BTC-USD"
        assert signal.side == "buy"
        assert len(strategy.candles_received) == 2

    @pytest.mark.asyncio
    async def test_strategy_processes_all_instruments(self) -> None:
        """Verify strategy processes candles from multiple instruments.

        Given: MockStrategy,
        When: on_candle called for BTC-USD and ETH-USD,
        Then: Both bars are received and stored.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="MockStrategy",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        strategy = MockStrategy(config)
        bar1 = make_candle_envelope("BTC-USD", 50000.0)
        await strategy.on_candle("BTC-USD", bar1)
        assert len(strategy.candles_received) == 1
        bar2 = make_candle_envelope("ETH-USD", 3000.0)
        await strategy.on_candle("ETH-USD", bar2)
        assert len(strategy.candles_received) == 2


class DummyRawSocket:
    """Mock raw ZMQ socket for testing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.closed = False

    def connect(self, addr: str) -> None:
        """Connect to the specified address."""
        self.addr = addr

    def setsockopt_string(self, *_args: Any, **_kwargs: Any) -> None:
        """Set socket option as string."""

    def close(self) -> None:
        """Close the socket."""
        self.closed = True


class DummyPublisher:
    """Mock publisher for testing signal emission."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self._raw_socket = DummyRawSocket()
        self.sent: list[tuple[str, bytes, int]] = []

    async def send_multipart(self, topic: str, payload: bytes, *, flags: int = 0) -> None:
        """Send multipart message."""
        self.sent.append((topic, payload, flags))

    def setsockopt(self, _option: int, _value: int) -> None:
        """Set socket option."""
        pass

    def close(self) -> None:
        """Close the socket."""
        self._raw_socket.close()


class DummySubscriber:
    """Mock subscriber for testing message reception."""

    def __init__(self, strategy: BaseStrategy, messages: list[tuple[str, bytes]]):
        """Initialize the instance."""
        self.strategy = strategy
        self._messages = list(messages)
        self._raw_socket = DummyRawSocket()
        self.subscribed: list[str] = []

    def subscribe(self, pattern: str) -> None:
        """Subscribe to a topic pattern."""
        self.subscribed.append(pattern)

    def close(self) -> None:
        """Close the socket."""
        self._raw_socket.close()

    async def recv_multipart(self) -> tuple[str, bytes]:
        """Receive multipart message."""
        if not self._messages:
            self.strategy._running = False
            await asyncio.sleep(0)
            return "system.replay.end", b"{}"
        topic, payload = self._messages.pop(0)
        if not self._messages:
            self.strategy._running = False
        return topic, payload


class DummySubSocket:
    """Mock SUB socket for testing subscriptions."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.subscribed: list[str] = []

    def connect(self, addr: str) -> None:
        """Connect to the specified address."""
        self.addr = addr

    def setsockopt(self, _option: int, _value: int) -> None:
        """Set integer socket option."""

    def setsockopt_string(self, _option: Any, pattern: str) -> None:
        """Set socket option as string."""
        self.subscribed.append(pattern)


class DummyPubSocket:
    """Mock PUB socket for testing publishing."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.connected: list[str] = []
        self.closed = False

    def connect(self, addr: str) -> None:
        """Connect to the specified address."""
        self.connected.append(addr)

    def close(self) -> None:
        """Close the socket."""
        self.closed = True

    def setsockopt(self, option: int, value: int) -> None:
        """Set socket option."""
        pass


class DummyZMQContext:
    """Mock ZMQ context for testing socket creation."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.pub_socket = DummyPubSocket()
        self.sub_socket = DummySubSocket()
        self.terminated = False

    def socket(self, socket_type: Any) -> Any:
        """Create a socket of the specified type."""
        if socket_type == 1:
            return self.pub_socket
        return self.sub_socket

    def term(self) -> None:
        """Terminate the context."""
        self.terminated = True


class FakeStrategy(BaseStrategy):
    """Fake strategy that records candle reception and emits signals."""

    def __init__(self, config: StrategyConfig, publisher: DummyPublisher | None = None) -> None:
        """Initialize the instance."""
        super().__init__(config)
        if publisher is not None:
            self.publisher = cast(ValidatedPublisher, publisher)
        self.reset_called = False
        self.received: list[tuple[str, CandleData]] = []

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle data and return optional signal."""
        self.received.append((instrument, candle))
        return StrategySignal(
            instrument=instrument,
            side="buy",
            strength=0.5,
            reason="test",
            price=candle.close,
        )

    async def reset(self) -> None:
        """Reset strategy state."""
        self.reset_called = True


def _strategy_config(**overrides: Any) -> StrategyConfig:
    base: dict[str, Any] = {
        "name": "test_strategy",
        "strategy_class": "FakeStrategy",
        "inputs": ["market.kraken.BTC-USD.candles.1h"],
        "outputs": ["BTC-USD"],
        "exchange": "kraken",
        "params": {"buffer_size": 5},
    }
    base.update(overrides)
    return StrategyConfig(**base)


@pytest.mark.asyncio
async def test_listen_loop_handles_system_messages_and_emits_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify listen loop handles system messages and emits signals.

    Given: Strategy with mocked subscriber and messages,
    When: Listen loop processes system and market messages,
    Then: Cache invalidation triggered and signal emitted.
    """
    invalidate_calls: list[str] = []

    class _Mapper:
        def trigger_cache_invalidation(self, *, fail_fast: bool) -> None:
            invalidate_calls.append(str(fail_fast))

    monkeypatch.setattr("snapper.strategies.system_events._get_db_mapper", lambda: _Mapper())
    heartbeat = HeartbeatData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        component="feed.kraken",
        sequence=1,
        status="healthy",
        lag_ms=12,
        meta={"symbol_count": 3},
    )
    candle = make_candle_envelope("BTC-USD", 101.0, ts=123.0, exchange="kraken")
    messages = [
        ("system.symbol_aliases", b"{}"),
        (
            "system.heartbeats.feed.kraken",
            heartbeat.to_json().encode(),
        ),
        (
            "system.replay.start",
            json.dumps(
                {
                    "type": "replay_start",
                    "session_id": "",
                    "sequence_id": 0,
                    "public_id": "test-pid",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ).encode(),
        ),
        (
            "system.replay.end",
            json.dumps(
                {
                    "type": "replay_end",
                    "session_id": "",
                    "sequence_id": 0,
                    "public_id": "test-pid",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ).encode(),
        ),
        (
            "market.kraken.BTC-USD.candles.1h",
            candle.to_json().encode(),
        ),
    ]
    strategy = FakeStrategy(
        _strategy_config(exchange="paper", inputs=["market.paper.kraken.BTC-USD.candles.1h"])
    )
    strategy._running = True
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    strategy.subscriber = cast(ValidatedSubscriber, DummySubscriber(strategy, messages))
    await strategy._listen_loop()
    assert invalidate_calls == ["False"]
    assert strategy.reset_called is True
    assert strategy._feed_heartbeats["kraken"]["status"] == "healthy"
    assert strategy._last_data_ts == pytest.approx(123.0)
    assert strategy.received[0][0] == "BTC-USD"
    mock_msg_publisher.send.assert_called_once()
    published_signal: SignalData = mock_msg_publisher.send.call_args[0][1]
    assert published_signal.instrument == "BTC-USD"
    assert published_signal.fired_at.timestamp() == pytest.approx(123.0)


@pytest.mark.asyncio
async def test_listen_loop_returns_when_no_subscriber() -> None:
    """Verify listen loop returns early without subscriber.

    Given: Strategy with no subscriber set,
    When: _listen_loop called,
    Then: Loop returns without processing.
    """
    strategy = FakeStrategy(_strategy_config())
    strategy._running = True
    await strategy._listen_loop()
    assert strategy._running is True


@pytest.mark.asyncio
async def test_default_handlers_return_none() -> None:
    """Verify default on_candle/on_tick/on_trade handlers return None.

    Given: Minimal strategy with default handlers,
    When: on_candle, on_tick, on_trade called,
    Then: All return None.
    """

    class MinimalStrategy(BaseStrategy):
        async def reset(self) -> None:
            """No-op reset for test strategy."""
            pass

    strategy = MinimalStrategy(_strategy_config())
    candle = make_candle_envelope("BTC-USD", 50000.0)
    assert await strategy.on_candle("BTC-USD", candle) is None
    tick = TickData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        volume=100.0,
        bid=50000.0,
        ask=50001.0,
        exchange="kraken",
    )
    assert await strategy.on_tick("BTC-USD", tick) is None
    trade = TradeData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        price=50000.0,
        volume=1.0,
        exchange="kraken",
    )
    assert await strategy.on_trade("BTC-USD", trade) is None


@pytest.mark.asyncio
async def test_emit_signal_validates_outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify emit_signal validates instrument is in outputs.

    Given: Strategy with BTC-USD output,
    When: emit_signal called with valid instrument,
    Then: StrategySignal is published without error.
    """
    strategy = FakeStrategy(_strategy_config(exchange="paper"))
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    strategy._last_data_ts = 321.0
    await strategy.emit_signal(
        StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.4,
            reason="paper",
            price=10.0,
        )
    )
    mock_msg_publisher.send.assert_called_once()


@pytest.mark.asyncio
async def test_emit_signal_no_outcome_keeps_attribution_fields_none() -> None:
    """emit_signal without outcome keeps attribution None.

    Given: a strategy emitting a non-AI signal (no ``outcome`` kwarg),
    When: emit_signal publishes the SignalData envelope,
    Then: both ``ai_review_public_id`` and
        ``ai_review_dispatch_version`` on the published envelope are
        ``None``. Pins the regression: non-AI strategy emits stay
        byte-identical for downstream consumers (the new optional
        kwarg has no effect when omitted).
    """
    strategy = FakeStrategy(_strategy_config(exchange="paper"))
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    strategy._last_data_ts = 200.0
    await strategy.emit_signal(
        StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.5,
            reason="non-ai",
            price=10.0,
        )
    )
    mock_msg_publisher.send.assert_called_once()
    sent_envelope = mock_msg_publisher.send.call_args.args[1]
    assert sent_envelope.ai_review_public_id is None
    assert sent_envelope.ai_review_dispatch_version is None


@pytest.mark.asyncio
async def test_emit_signal_outcome_stamps_attribution_on_envelope() -> None:
    """emit_signal(outcome=) stamps both attribution fields.

    Given: a strategy that just received an
        :class:`AiReviewDecisionOutcome` from a successful CONSULT
        round,
    When: emit_signal is called with ``outcome=decision``,
    Then: the published SignalData envelope carries both
        ``ai_review_public_id == decision.review_public_id`` and
        ``ai_review_dispatch_version == decision.dispatch_version``,
        threading the AI attribution end-to-end through the ZMQ wire.
    """
    strategy = FakeStrategy(_strategy_config(exchange="paper"))
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    strategy._last_data_ts = 200.0
    decision = AiReviewDecisionOutcome(
        review_public_id="rev-emit-attr",
        status=AiReviewStatusEnum.RESOLVED_APPROVED,
        resolution_mode=None,
        decision=None,
        rationale=None,
        dispatch_version=11,
        responding_delegate_public_id=None,
    )
    await strategy.emit_signal(
        StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.5,
            reason="ai-attributed",
            price=10.0,
        ),
        outcome=decision,
    )
    sent_envelope = mock_msg_publisher.send.call_args.args[1]
    assert sent_envelope.ai_review_public_id == "rev-emit-attr"
    assert sent_envelope.ai_review_dispatch_version == 11


@pytest.mark.asyncio
async def test_emit_signal_persists_with_stamped_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify emit_signal persists signal with provenance from the stamped publish result.

    Given: Strategy with a mock publisher that returns a stamped SignalData,
    When: emit_signal is called,
    Then: signal_service.store_signal is called with the stamped session_id and sequence_id.
    """
    strategy = FakeStrategy(_strategy_config(exchange="paper"))
    stamped = SignalData(
        instrument="BTC-USD",
        exchange="paper",
        strategy_name="test",
        side="buy",
        strength=0.5,
        reason="test",
        session_id="test-session-123",
        sequence_id=7,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        fired_at=datetime.now(UTC),
    )
    mock_publisher = MagicMock()
    mock_publisher.send = AsyncMock(return_value=stamped)
    mock_publisher.tracker = SequenceTracker()
    mock_publisher.tracker._session_id = "test-session-123"
    mock_publisher.session_id = mock_publisher.tracker.session_id
    strategy.msg_publisher = mock_publisher
    strategy._last_data_ts = 100.0

    captured: list[dict[str, Any]] = []

    async def capture_store(
        sig: StrategySignal,
        exchange: str,
        session_id: str = "",
        sequence_id: int = 0,
        strategy_name: str | None = None,
        price: float | None = None,
        public_id: str | None = None,
        timestamp: datetime | None = None,
        tracker: object = None,
        wallet_public_id: str | None = None,
        operator_public_id: str | None = None,
        paired_group_id: str | None = None,
    ) -> str:
        captured.append(
            {
                "session_id": session_id,
                "sequence_id": sequence_id,
                "public_id": public_id,
                "timestamp": timestamp,
                "tracker": tracker,
            }
        )
        return "signal-pid"

    monkeypatch.setattr(signal_service, "store_signal", capture_store)

    await strategy.emit_signal(
        StrategySignal(instrument="BTC-USD", side="buy", strength=0.5, reason="test", price=1.0)
    )

    assert len(captured) == 1
    assert captured[0]["session_id"] == "test-session-123"
    assert captured[0]["sequence_id"] == 1
    assert captured[0]["public_id"] is not None
    assert captured[0]["timestamp"] is not None
    assert captured[0]["tracker"] is strategy._tracker


@pytest.mark.asyncio
async def test_listen_loop_ignores_non_market_topics() -> None:
    """Verify listen loop ignores non-market topics.

    Given: Strategy receiving signal topic message,
    When: _listen_loop processes message,
    Then: No bars processed and no signals emitted.
    """
    strategy = FakeStrategy(_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]))
    strategy._running = True
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    messages = [
        (
            "signals.paper.BTC-USD.other",
            json.dumps({"ts": "456.0", "value": 1}).encode(),
        )
    ]
    strategy.subscriber = cast(ValidatedSubscriber, DummySubscriber(strategy, messages))
    await strategy._listen_loop()
    assert strategy._last_data_ts is None
    mock_msg_publisher.send.assert_not_called()
    assert len(strategy.received) == 0


@pytest.mark.asyncio
async def test_listen_loop_handles_tick_data() -> None:
    """Verify listen loop routes tick data to on_tick handler.

    Given: TickStrategy subscribed to tick topic,
    When: Tick message received,
    Then: on_tick handler invoked with correct data.
    """
    received_ticks: list[tuple[str, TickData]] = []

    class TickStrategy(BaseStrategy):
        async def reset(self) -> None:
            """No-op reset for test strategy."""
            pass

        async def on_tick(self, instrument: str, tick: TickData) -> None:
            received_ticks.append((instrument, tick))

    strategy = TickStrategy(_strategy_config(inputs=["market.kraken.BTC-USD.ticks"]))
    strategy._running = True
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    tick = TickData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        volume=100.0,
        bid=50000.0,
        ask=50001.0,
        exchange="kraken",
    )
    messages = [("market.kraken.BTC-USD.ticks", tick.to_json().encode())]
    strategy.subscriber = cast(ValidatedSubscriber, DummySubscriber(strategy, messages))
    await strategy._listen_loop()
    assert len(received_ticks) == 1
    assert received_ticks[0][0] == "BTC-USD"
    assert received_ticks[0][1].bid == pytest.approx(50000.0)
    assert strategy._last_data_ts is not None


@pytest.mark.asyncio
async def test_listen_loop_handles_trade_data() -> None:
    """Verify listen loop routes trade data to on_trade handler.

    Given: TradeStrategy subscribed to trade topic,
    When: Trade message received,
    Then: on_trade handler invoked with correct data.
    """
    received_trades: list[tuple[str, TradeData]] = []

    class TradeStrategy(BaseStrategy):
        async def reset(self) -> None:
            """No-op reset for test strategy."""
            pass

        async def on_trade(self, instrument: str, trade: TradeData) -> None:
            received_trades.append((instrument, trade))

    strategy = TradeStrategy(_strategy_config(inputs=["market.kraken.BTC-USD.trades"]))
    strategy._running = True
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
    strategy.msg_publisher = mock_msg_publisher
    trade = TradeData(
        session_id="",
        sequence_id=0,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        instrument="BTC-USD",
        price=50000.0,
        volume=1.0,
        exchange="kraken",
    )
    messages = [("market.kraken.BTC-USD.trades", trade.to_json().encode())]
    strategy.subscriber = cast(ValidatedSubscriber, DummySubscriber(strategy, messages))
    await strategy._listen_loop()
    assert len(received_trades) == 1
    assert received_trades[0][0] == "BTC-USD"
    assert received_trades[0][1].price == pytest.approx(50000.0)
    assert strategy._last_data_ts is not None


@pytest.mark.asyncio
async def test_dispatch_market_data_unknown_topic_returns_empty() -> None:
    """Verify dispatcher returns an empty group for unsupported market topic type.

    Given: Strategy instance and unsupported market topic suffix,
    When: _dispatch_market_data is called,
    Then: Method returns an empty list without raising.
    """
    strategy = FakeStrategy(_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]))
    result = await strategy._dispatch_market_data("market.kraken.BTC-USD.book", "BTC-USD", "{}")
    assert result == []


class TestSignalGroupNormalization:
    """Fail-closed group preflight for the multi-leg signal contract."""

    @staticmethod
    def _paper_strategy() -> FakeStrategy:
        """Two-output PAPER strategy for group-validation tests."""
        return FakeStrategy(
            _strategy_config(
                inputs=[
                    "market.paper.kraken.BTC-USD.candles.1h",
                    "market.paper.kraken.ETH-USD.candles.1h",
                ],
                outputs=["BTC-USD", "ETH-USD"],
                exchange="paper",
            )
        )

    @staticmethod
    def _signal(instrument: str, side: str = "buy") -> StrategySignal:
        """Build a minimal signal for one instrument."""
        return StrategySignal(instrument=instrument, side=side, strength=1.0, reason="r", price=1.0)

    def test_none_returns_empty_group(self) -> None:
        """None collapses to an empty group."""
        s = self._paper_strategy()
        assert s._normalize_signal_group(None) == []

    def test_single_signal_wrapped_in_list(self) -> None:
        """A single signal is wrapped into a one-element group."""
        s = self._paper_strategy()
        sig = self._signal("BTC-USD")
        assert s._normalize_signal_group(sig) == [sig]

    def test_valid_multi_leg_group_order_preserved(self) -> None:
        """A valid multi-leg list passes through in declared order."""
        s = self._paper_strategy()
        legs = [self._signal("BTC-USD", "sell"), self._signal("ETH-USD", "buy")]
        assert s._normalize_signal_group(legs) == legs

    def test_duplicate_instrument_raises(self) -> None:
        """Two legs on the same instrument fail closed."""
        s = self._paper_strategy()
        legs = [self._signal("BTC-USD"), self._signal("BTC-USD")]
        with pytest.raises(ValueError, match="duplicate instrument"):
            s._normalize_signal_group(legs)

    def test_instrument_not_in_outputs_raises(self) -> None:
        """A leg whose instrument is not a configured output fails closed."""
        s = self._paper_strategy()
        legs = [self._signal("BTC-USD"), self._signal("SOL-USD")]
        with pytest.raises(ValueError, match="not allowed"):
            s._normalize_signal_group(legs)

    def test_non_signal_element_raises(self) -> None:
        """A non-StrategySignal element fails closed."""
        s = self._paper_strategy()
        with pytest.raises(TypeError, match="every leg"):
            s._normalize_signal_group(cast(Any, [self._signal("BTC-USD"), "nope"]))

    def test_non_signal_non_list_return_raises(self) -> None:
        """A return that is neither None, signal, nor list fails closed."""
        s = self._paper_strategy()
        with pytest.raises(TypeError, match="callback must return"):
            s._normalize_signal_group(cast(Any, 123))

    @staticmethod
    def _live_strategy() -> FakeStrategy:
        """Two-output LIVE (non-paper) strategy for the guard-gate tests."""
        return FakeStrategy(
            _strategy_config(
                inputs=["market.kraken.BTC-USD.candles.1h"],
                outputs=["BTC-USD", "ETH-USD"],
                exchange="kraken",
            )
        )

    def test_live_multi_leg_blocked_when_guard_disabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fail-closed: a live multi-leg group is refused while the guard is off."""
        monkeypatch.setattr(
            "snapper.strategies.base._bootstrap_settings.paired_execution_guard_enabled",
            False,
        )
        s = self._live_strategy()
        legs = [self._signal("BTC-USD", "buy"), self._signal("ETH-USD", "sell")]
        with pytest.raises(ValueError, match="PAIRED_EXECUTION_GUARD_ENABLED"):
            s._normalize_signal_group(legs)

    def test_live_multi_leg_allowed_when_guard_enabled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A live multi-leg group is allowed once the guard flag is enabled."""
        monkeypatch.setattr(
            "snapper.strategies.base._bootstrap_settings.paired_execution_guard_enabled",
            True,
        )
        s = self._live_strategy()
        legs = [self._signal("BTC-USD", "buy"), self._signal("ETH-USD", "sell")]
        assert s._normalize_signal_group(legs) == legs


@pytest.mark.asyncio
async def test_emit_signal_auto_timestamp_and_setup_publisher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify emit_signal auto-timestamps and sets up publisher.

    Given: Strategy without publisher and no last_data_ts,
    When: emit_signal called,
    Then: Publisher setup and wall time used for timestamp.
    """
    strategy = FakeStrategy(_strategy_config())
    strategy._last_data_ts = None
    captured_time = 123.456
    mock_msg_publisher = MagicMock()
    mock_msg_publisher.send = AsyncMock()
    mock_msg_publisher.tracker = SequenceTracker()
    mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id

    async def fake_setup(self: BaseStrategy) -> None:
        self.msg_publisher = mock_msg_publisher

    monkeypatch.setattr(BaseStrategy, "_setup_publisher", fake_setup)
    monkeypatch.setattr(time, "time", lambda: captured_time)
    await strategy.emit_signal(
        StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            reason="auto-ts",
            price=200.0,
        )
    )
    mock_msg_publisher.send.assert_called_once()
    published_signal: SignalData = mock_msg_publisher.send.call_args[0][1]
    assert published_signal.fired_at.timestamp() == captured_time
    assert published_signal.instrument == "BTC-USD"
    with pytest.raises(ValueError, match="not allowed"):
        await strategy.emit_signal(
            StrategySignal(
                instrument="ETH-USD",
                side="sell",
                strength=0.2,
                reason="invalid",
                price=20.0,
            )
        )


@pytest.mark.asyncio
async def test_start_stop_lifecycle_invokes_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify start/stop lifecycle invokes subscribe/unsubscribe hooks.

    Given: Strategy with mocked lifecycle hooks,
    When: start() then stop() called,
    Then: Subscribe, setup, and unsubscribe hooks invoked.
    """
    strategy = FakeStrategy(_strategy_config())
    subscribe_called = False
    unsubscribe_called = False
    setup_called = False

    async def _fake_subscribe(self: BaseStrategy) -> None:
        nonlocal subscribe_called
        subscribe_called = True
        self.subscriber = cast(ValidatedSubscriber, DummySubscriber(self, []))

    async def _fake_unsubscribe(self: BaseStrategy) -> None:
        nonlocal unsubscribe_called
        unsubscribe_called = True
        self.subscriber = None

    async def _fake_setup(self: BaseStrategy) -> None:
        nonlocal setup_called
        setup_called = True
        self.publisher = cast(ValidatedPublisher, DummyPublisher())

    async def _fake_heartbeat_loop(self: BaseStrategy) -> None:
        return None

    monkeypatch.setattr(BaseStrategy, "_subscribe_inputs", _fake_subscribe)
    monkeypatch.setattr(BaseStrategy, "_unsubscribe_inputs", _fake_unsubscribe)
    monkeypatch.setattr(BaseStrategy, "_setup_publisher", _fake_setup)
    monkeypatch.setattr(BaseStrategy, "_heartbeat_loop", _fake_heartbeat_loop)
    await strategy.start()
    assert strategy.is_running is True
    assert subscribe_called is True
    assert setup_called is True
    assert strategy._heartbeat_task is not None
    await strategy.stop()
    assert unsubscribe_called is True
    assert strategy.is_running is False
    assert strategy.zmq_context is None
    assert strategy.publisher is None


@pytest.mark.asyncio
async def test_subscribe_inputs_deduplicates_feed_heartbeats(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify subscribe_inputs deduplicates heartbeat subscriptions.

    Given: Strategy with two inputs from same exchange,
    When: _subscribe_inputs called,
    Then: Only one heartbeat subscription per exchange.
    """
    config = _strategy_config(
        inputs=["market.kraken.BTC-USD.candles.1h", "market.kraken.ETH-USD.candles.5m"]
    )
    strategy = FakeStrategy(config)
    context = DummyZMQContext()
    strategy.zmq_context = cast(Any, context)
    monkeypatch.setattr("snapper.strategies.base.zmq", type("_Z", (), {"PUB": 1, "SUB": 2}))
    await strategy._subscribe_inputs()
    assert strategy.subscriber is not None
    assert context.sub_socket.subscribed.count("system.heartbeats.feed.kraken") == 1
    assert "system.symbol_aliases" in context.sub_socket.subscribed
    assert "system.settings" in context.sub_socket.subscribed


@pytest.mark.asyncio
async def test_subscribe_inputs_paper_heartbeat_includes_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify paper market inputs subscribe to source-specific heartbeat.

    Given: Strategy with paper market input including source_exchange,
    When: _subscribe_inputs called,
    Then: Subscribes to system.heartbeats.feed.paper.{source}.
    """
    config = _strategy_config(
        inputs=["market.paper.kraken.BTC-USD.candles.1h"],
        exchange="paper",
    )
    strategy = FakeStrategy(config)
    context = DummyZMQContext()
    strategy.zmq_context = cast(Any, context)
    monkeypatch.setattr("snapper.strategies.base.zmq", type("_Z", (), {"PUB": 1, "SUB": 2}))
    await strategy._subscribe_inputs()
    assert "system.heartbeats.feed.paper.kraken" in context.sub_socket.subscribed
    assert "system.heartbeats.feed.paper" not in context.sub_socket.subscribed


@pytest.mark.asyncio
async def test_heartbeat_loop_emits_and_evaluates_health(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify heartbeat loop emits status and evaluates feed health.

    Given: Strategy with feed heartbeat data,
    When: _heartbeat_loop runs,
    Then: Heartbeat emitted with feed health info.
    """
    strategy = FakeStrategy(_strategy_config())

    published_messages: list[Any] = []

    async def capture_and_stop(topic: str, data: Any, **kwargs: Any) -> None:
        published_messages.append(data)
        strategy._running = False

    mock_msg_pub = AsyncMock(side_effect=capture_and_stop)
    strategy.msg_publisher = MagicMock()
    strategy.msg_publisher.send = mock_msg_pub
    strategy.msg_publisher.tracker = SequenceTracker()
    strategy.msg_publisher.session_id = strategy.msg_publisher.tracker.session_id
    strategy._running = True
    strategy.last_data_timestamp = time.time() - 5
    strategy._feed_heartbeats = {
        "kraken": {
            "timestamp": time.time() - 1,
            "status": "ok",
            "lag_ms": 10,
            "component": "feed",
            "symbol_count": 1,
        }
    }

    async def fast_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    await strategy._heartbeat_loop()
    hb_data = published_messages[0]
    assert hb_data.component == f"strategy.{strategy.name}"
    assert hb_data.status == "warning"
    assert hb_data.meta["feed_health"]["kraken"]["healthy"] is True


class SimpleTestStrategy(BaseStrategy):
    """Simple test strategy for lifecycle and signal testing."""

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize the instance."""
        super().__init__(config)
        self.signals_generated: list[StrategySignal] = []
        self.candles_processed: list[tuple[str, CandleData]] = []
        self._generate_signal: bool = False

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle data and return optional signal."""
        self.candles_processed.append((instrument, candle))
        if self._generate_signal:
            signal = StrategySignal(
                instrument=instrument,
                side="buy",
                strength=0.7,
                price=candle.close,
                reason="Test signal",
            )
            self.signals_generated.append(signal)
            return signal
        return None

    async def reset(self) -> None:
        """Reset strategy state."""
        self.signals_generated.clear()
        self.candles_processed.clear()
        self._generate_signal = False


class ReplayAwareStrategy(SimpleTestStrategy):
    """Strategy that tracks reset calls during replay."""

    def __init__(self, config: StrategyConfig):
        """Initialize the instance."""
        super().__init__(config)
        self.reset_count: int = 0

    async def reset(self) -> None:
        """Reset strategy state."""
        self.reset_count += 1
        await super().reset()


class DummyPublisherV2:
    """Mock publisher V2 that stops strategy on send."""

    def __init__(self, strategy: BaseStrategy):
        """Initialize the instance."""
        self._strategy = strategy
        self.sent: list[tuple[str, bytes]] = []

    async def send_multipart(self, topic: str, payload: bytes) -> None:
        """Send multipart message."""
        self.sent.append((topic, payload))
        self._strategy._running = False


@pytest.fixture
def strategy_config() -> StrategyConfig:
    """Provide a default strategy configuration for testing."""
    return StrategyConfig(
        name="test_strategy",
        strategy_class="SimpleTestStrategy",
        inputs=["market.kraken.BTC-USD.candles.1m"],
        outputs=["BTC-USD"],
        params={},
    )


class TestSubscribeInputs:
    """Test suite for strategy input subscription."""

    @pytest.mark.asyncio
    async def test_subscribe_inputs_with_broker(self, strategy_config: StrategyConfig) -> None:
        """Verify subscribe_inputs creates SUB socket with broker.

        Given: Strategy with ZMQ context,
        When: _subscribe_inputs called,
        Then: SUB socket created and connected.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        strategy.zmq_context = mock_context
        await strategy._subscribe_inputs()
        assert strategy.subscriber is not None
        mock_context.socket.assert_called_once_with(zmq.SUB)
        assert mock_socket.connect.called

    @pytest.mark.asyncio
    async def test_subscribe_inputs_without_broker(self, strategy_config: StrategyConfig) -> None:
        """Verify subscribe_inputs connects to feed address without broker.

        Given: Strategy with use_broker=False,
        When: _subscribe_inputs called,
        Then: Socket connects to feed_addr directly.
        """
        strategy_config.params["use_broker"] = False
        strategy_config.params["feed_addr"] = "tcp://127.0.0.1:9999"
        strategy = SimpleTestStrategy(strategy_config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        strategy.zmq_context = mock_context
        await strategy._subscribe_inputs()
        mock_socket.connect.assert_called()
        call_args = mock_socket.connect.call_args[0][0]
        assert "9999" in call_args

    @pytest.mark.asyncio
    async def test_subscribe_inputs_signal_topic(self) -> None:
        """Verify subscribe_inputs handles signal topic inputs.

        Given: Strategy with signal topic input,
        When: _subscribe_inputs called,
        Then: Subscriber created for signal topic.
        """
        config = StrategyConfig(
            name="composite",
            strategy_class="SimpleTestStrategy",
            inputs=["signals.paper.BTC-USD.simple"],
            outputs=["BTC-USD"],
            params={},
        )
        strategy = SimpleTestStrategy(config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        strategy.zmq_context = mock_context
        await strategy._subscribe_inputs()
        assert strategy.subscriber is not None

    @pytest.mark.asyncio
    async def test_subscribe_inputs_already_subscribed(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify subscribe_inputs returns early if already subscribed.

        Given: Strategy with existing subscriber,
        When: _subscribe_inputs called,
        Then: Existing subscriber preserved.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy.subscriber = MagicMock()
        await strategy._subscribe_inputs()
        assert strategy.subscriber is not None

    @pytest.mark.asyncio
    async def test_subscribe_inputs_creates_listen_task(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify subscribe_inputs creates listen task.

        Given: Strategy with ZMQ context,
        When: _subscribe_inputs called,
        Then: _listen_task is created.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket

        async def mock_listen_loop() -> None:
            """Intentionally empty mock implementation."""
            pass

        with (
            patch("zmq.asyncio.Context", return_value=mock_context),
            patch.object(strategy, "_listen_loop", side_effect=mock_listen_loop),
        ):
            strategy.zmq_context = mock_context
            await strategy._subscribe_inputs()
            assert strategy._listen_task is not None
            await strategy.stop()

    @pytest.mark.asyncio
    async def test_subscribe_inputs_auto_replay_subscription(self) -> None:
        """Verify paper input auto-subscribes to replay topics.

        Given: Strategy with paper input,
        When: _subscribe_inputs called,
        Then: system.replay. topic subscribed.
        """
        config = StrategyConfig(
            name="paper_subscriber",
            strategy_class="SimpleTestStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
            params={},
        )
        strategy = SimpleTestStrategy(config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket

        async def noop_listen() -> None:
            return None

        with patch.object(strategy, "_listen_loop", side_effect=noop_listen):
            strategy.zmq_context = mock_context
            await strategy._subscribe_inputs()
        mock_socket.setsockopt_string.assert_any_call(zmq.SUBSCRIBE, "system.replay.")

    @pytest.mark.asyncio
    async def test_subscribe_inputs_deduplicates_heartbeat_exchanges(self) -> None:
        """Verify subscribe_inputs deduplicates heartbeat exchanges.

        Given: Strategy with multiple inputs from same exchange,
        When: _subscribe_inputs called,
        Then: Only one heartbeat subscription per exchange.
        """
        config = StrategyConfig(
            name="multi_input",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles.1m", "market.kraken.ETH-USD.candles.1m"],
            outputs=["BTC-USD"],
            params={},
        )
        strategy = SimpleTestStrategy(config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket

        async def noop_listen() -> None:
            return None

        with patch.object(strategy, "_listen_loop", side_effect=noop_listen):
            strategy.zmq_context = mock_context
            await strategy._subscribe_inputs()
        heartbeat_calls = [
            call_args
            for call_args in mock_socket.setsockopt_string.call_args_list
            if "system.heartbeats.feed.kraken" in str(call_args)
        ]
        assert len(heartbeat_calls) == 1

    @pytest.mark.asyncio
    async def test_subscribe_inputs_skips_malformed_market_topic(self) -> None:
        """Verify malformed market topic skips heartbeat subscription.

        Given: Strategy with malformed market topic,
        When: _subscribe_inputs called,
        Then: No heartbeat subscription created.
        """

        class ShortSplitTopic(str):
            def split(self, sep: str | None = None, maxsplit: SupportsIndex = -1) -> list[str]:
                return [self]

        config = StrategyConfig(
            name="malformed_market",
            strategy_class="SimpleTestStrategy",
            inputs=[ShortSplitTopic("market.stub")],
            outputs=["BTC-USD"],
            params={},
        )
        strategy = SimpleTestStrategy(config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        stub_subscriber = MagicMock()

        async def noop_listen() -> None:
            return None

        with (
            patch("snapper.strategies.base.ValidatedSubscriber", return_value=stub_subscriber),
            patch.object(strategy, "_listen_loop", side_effect=noop_listen),
        ):
            strategy.zmq_context = mock_context
            await strategy._subscribe_inputs()
        heartbeat_calls = [
            call_args
            for call_args in stub_subscriber.subscribe.call_args_list
            if "system.heartbeats.feed" in str(call_args)
        ]
        assert heartbeat_calls == []


class TestUnsubscribeInputs:
    """Test suite for strategy input unsubscription."""

    @pytest.mark.asyncio
    async def test_unsubscribe_inputs_closes_subscriber(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify unsubscribe_inputs closes subscriber socket.

        Given: Strategy with active subscriber,
        When: _unsubscribe_inputs called,
        Then: Subscriber closed and set to None.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_subscriber = MagicMock()
        strategy.subscriber = mock_subscriber
        await strategy._unsubscribe_inputs()
        mock_subscriber.close.assert_called_once()
        assert strategy.subscriber is None

    @pytest.mark.asyncio
    async def test_unsubscribe_inputs_no_subscriber(self, strategy_config: StrategyConfig) -> None:
        """Verify unsubscribe_inputs handles no subscriber gracefully.

        Given: Strategy with no subscriber,
        When: _unsubscribe_inputs called,
        Then: No error raised.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy.subscriber = None
        await strategy._unsubscribe_inputs()
        assert strategy.subscriber is None

    @pytest.mark.asyncio
    async def test_unsubscribe_inputs_awaits_async_close(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify unsubscribe_inputs awaits async subscriber close methods.

        Given: Strategy with subscriber exposing AsyncMock close,
        When: _unsubscribe_inputs is called,
        Then: Close is awaited and subscriber is cleared.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_subscriber = MagicMock()
        mock_subscriber.close = AsyncMock()
        strategy.subscriber = mock_subscriber
        await strategy._unsubscribe_inputs()
        mock_subscriber.close.assert_awaited_once()
        assert strategy.subscriber is None

    @pytest.mark.asyncio
    async def test_unsubscribe_inputs_tolerates_resource_without_close(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify unsubscribe_inputs tolerates subscriber-like objects without close.

        Given: Strategy with subscriber object that exposes no close method,
        When: _unsubscribe_inputs is called,
        Then: Cleanup still succeeds and subscriber is cleared.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy.subscriber = cast(Any, object())
        await strategy._unsubscribe_inputs()
        assert strategy.subscriber is None


class TestLifecycle:
    """Test suite for strategy lifecycle management."""

    @pytest.mark.asyncio
    async def test_start_initializes_context_and_heartbeat(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify start initializes ZMQ context and heartbeat task.

        Given: Strategy instance,
        When: start() called,
        Then: ZMQ context, subscriber, publisher, heartbeat task created.
        """
        strategy = SimpleTestStrategy(strategy_config)
        heartbeat_task = MagicMock()
        heartbeat_task.done.return_value = False

        def fake_create_task(coro: Coroutine[Any, Any, Any]) -> Any:
            coro.close()
            return heartbeat_task

        with (
            patch.object(strategy, "_subscribe_inputs", new_callable=AsyncMock) as subscribe_inputs,
            patch.object(strategy, "_setup_publisher", new_callable=AsyncMock) as setup_publisher,
            patch("zmq.asyncio.Context", return_value=MagicMock()) as ctx_factory,
            patch("asyncio.create_task", side_effect=fake_create_task) as create_task,
        ):
            await strategy.start()
        ctx_factory.assert_called_once()
        subscribe_inputs.assert_awaited_once()
        setup_publisher.assert_awaited_once()
        create_task.assert_called_once()
        assert strategy._heartbeat_task is heartbeat_task

    @pytest.mark.asyncio
    async def test_start_reuses_existing_zmq_context(self, strategy_config: StrategyConfig) -> None:
        """Verify start reuses existing ZMQ context.

        Given: Strategy with existing zmq_context,
        When: start() called,
        Then: Existing context preserved.
        """
        strategy = SimpleTestStrategy(strategy_config)
        existing_context = MagicMock()
        strategy.zmq_context = existing_context
        heartbeat_task = MagicMock()
        heartbeat_task.done.return_value = False

        def fake_create_task(coro: Coroutine[Any, Any, Any]) -> Any:
            coro.close()
            return heartbeat_task

        with (
            patch.object(strategy, "_subscribe_inputs", new_callable=AsyncMock),
            patch.object(strategy, "_setup_publisher", new_callable=AsyncMock),
            patch("zmq.asyncio.Context") as ctx_factory,
            patch("asyncio.create_task", side_effect=fake_create_task),
        ):
            await strategy.start()
        ctx_factory.assert_not_called()
        assert strategy.zmq_context is existing_context

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks_and_cleans_up(self, strategy_config: StrategyConfig) -> None:
        """Verify stop cancels tasks and cleans up resources.

        Given: Strategy with running tasks and sockets,
        When: stop() called,
        Then: Tasks cancelled, sockets closed, context terminated.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        mock_subscriber = MagicMock()
        mock_subscriber.close = MagicMock()
        strategy.subscriber = mock_subscriber
        mock_publisher = MagicMock()
        mock_publisher.close = MagicMock()
        strategy.publisher = mock_publisher
        strategy.zmq_context = MagicMock()
        strategy.zmq_context.term = MagicMock()

        async def never() -> None:
            await asyncio.sleep(10)

        strategy._heartbeat_task = asyncio.create_task(never())
        strategy._listen_task = asyncio.create_task(never())
        await strategy.stop()
        mock_subscriber.close.assert_called_once()
        mock_publisher.setsockopt.assert_called_once()
        mock_publisher.close.assert_called_once()
        assert strategy.subscriber is None
        assert strategy.publisher is None
        assert strategy.zmq_context is None
        assert strategy._heartbeat_task.cancelled()
        assert strategy._listen_task.cancelled()

    def test_del_cleans_up_resources(self, strategy_config: StrategyConfig) -> None:
        """Verify __del__ releases sockets, tasks, and context."""
        strategy = SimpleTestStrategy(strategy_config)
        heartbeat_task = MagicMock()
        heartbeat_task.done.return_value = False
        listen_task = MagicMock()
        listen_task.done.return_value = False
        mock_subscriber = MagicMock()
        mock_publisher = MagicMock()
        mock_context = MagicMock()
        strategy._heartbeat_task = heartbeat_task
        strategy._listen_task = listen_task
        strategy.subscriber = mock_subscriber
        strategy.publisher = mock_publisher
        strategy.zmq_context = mock_context
        strategy.__del__()
        heartbeat_task.cancel.assert_called_once()
        listen_task.cancel.assert_called_once()
        mock_subscriber.close.assert_called_once()
        mock_publisher.close.assert_called_once()
        mock_context.destroy.assert_called_once_with(linger=0)
        assert strategy.subscriber is None
        assert strategy.publisher is None
        assert strategy.zmq_context is None

    def test_del_without_resources_is_noop(self, strategy_config: StrategyConfig) -> None:
        """Verify __del__ tolerates strategies without allocated resources."""
        strategy = SimpleTestStrategy(strategy_config)
        strategy.__del__()
        assert strategy.subscriber is None
        assert strategy.publisher is None
        assert strategy.zmq_context is None

    def test_del_tolerates_resources_without_close_method(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify __del__ tolerates resource-like objects without close methods."""
        strategy = SimpleTestStrategy(strategy_config)
        strategy.subscriber = cast(Any, object())
        strategy.publisher = cast(Any, object())
        strategy.__del__()
        assert strategy.subscriber is None
        assert strategy.publisher is None

    def test_del_closes_async_mock_resources_without_warning(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify __del__ suppresses async close coroutine warnings.

        Given: Strategy with AsyncMock-based subscriber and publisher close methods,
        When: __del__ runs and garbage collection is forced,
        Then: No RuntimeWarning is emitted for unawaited close coroutines.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_subscriber = MagicMock()
        mock_subscriber.close = AsyncMock()
        mock_publisher = MagicMock()
        mock_publisher.close = AsyncMock()
        strategy.subscriber = mock_subscriber
        strategy.publisher = mock_publisher
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            strategy.__del__()
            gc.collect()
        runtime_warnings = [
            warning for warning in caught if issubclass(warning.category, RuntimeWarning)
        ]
        assert runtime_warnings == []
        mock_subscriber.close.assert_called_once()
        mock_publisher.close.assert_called_once()

    def test_del_falls_back_to_term_when_destroy_missing(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify __del__ calls zmq_context.term() when destroy is absent.

        Given: Strategy with zmq_context that has no destroy attribute,
        When: __del__ is called,
        Then: Falls back to zmq_context.term().
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_context = SimpleNamespace(term=MagicMock())
        strategy.zmq_context = mock_context
        strategy.__del__()
        mock_context.term.assert_called_once()
        assert strategy.zmq_context is None


class TestListenLoop:
    """Test suite for strategy message listening loop."""

    @pytest.mark.asyncio
    async def test_listen_loop_no_subscriber(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop returns early without subscriber.

        Given: Strategy with no subscriber,
        When: _listen_loop called,
        Then: No bars processed.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy.subscriber = None
        strategy._running = True
        await strategy._listen_loop()
        assert len(strategy.candles_processed) == 0

    @pytest.mark.asyncio
    async def test_listen_loop_receives_market_data(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop processes market data messages.

        Given: Strategy with mocked subscriber sending candle data,
        When: _listen_loop runs,
        Then: Bar processed and received in candles_processed.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        candle = make_candle_envelope("BTC-USD", 50000.0, exchange="kraken")
        mock_recv = AsyncMock(
            side_effect=[
                (
                    "market.kraken.BTC-USD.candles.1h",
                    candle.to_json().encode(),
                ),
                asyncio.CancelledError(),
            ]
        )
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert len(strategy.candles_processed) == 1
        instrument, received_candle = strategy.candles_processed[0]
        assert instrument == "BTC-USD"
        assert received_candle.close == pytest.approx(50000.0)

    @pytest.mark.asyncio
    async def test_listen_loop_receives_signal_data(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop receives but doesn't process signal data.

        Given: Strategy receiving signal topic message,
        When: _listen_loop runs,
        Then: StrategySignal not processed as candle.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        signal_data = {
            "instrument": "BTC-USD",
            "side": "buy",
            "strength": 0.9,
            "price": 51000.0,
        }
        mock_recv = AsyncMock(
            side_effect=[
                ("signals.kraken.macd_btc", json.dumps(signal_data).encode()),
                asyncio.CancelledError(),
            ]
        )
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert len(strategy.candles_processed) == 0

    @pytest.mark.asyncio
    async def test_listen_loop_buffers_candles(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop buffers candles for instrument.

        Given: Strategy receiving multiple candle messages,
        When: _listen_loop processes messages,
        Then: Candle buffer populated with bars.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        bars = [
            make_candle_envelope("BTC-USD", 100.0, exchange="kraken"),
            make_candle_envelope("BTC-USD", 101.0, exchange="kraken"),
            make_candle_envelope("BTC-USD", 102.0, exchange="kraken"),
        ]
        mock_recv = AsyncMock(
            side_effect=[
                ("market.kraken.BTC-USD.candles.1h", bars[0].to_json().encode()),
                ("market.kraken.BTC-USD.candles.1h", bars[1].to_json().encode()),
                ("market.kraken.BTC-USD.candles.1h", bars[2].to_json().encode()),
                asyncio.CancelledError(),
            ]
        )
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert len(strategy.candle_buffer["BTC-USD"]) == 3
        assert len(strategy.candles_processed) == 3
        assert strategy.candle_buffer["BTC-USD"][-1].close == pytest.approx(102.0)

    @pytest.mark.asyncio
    async def test_listen_loop_respects_buffer_size(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop respects configured buffer_size.

        Given: Strategy with buffer_size=2,
        When: 4 bars received,
        Then: Buffer contains only last 2 bars.
        """
        strategy_config.params["buffer_size"] = 2
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        bars = [
            make_candle_envelope("BTC-USD", 100.0, exchange="kraken"),
            make_candle_envelope("BTC-USD", 101.0, exchange="kraken"),
            make_candle_envelope("BTC-USD", 102.0, exchange="kraken"),
            make_candle_envelope("BTC-USD", 103.0, exchange="kraken"),
        ]
        mock_recv = AsyncMock(
            side_effect=[
                ("market.kraken.BTC-USD.candles.1h", bars[0].to_json().encode()),
                ("market.kraken.BTC-USD.candles.1h", bars[1].to_json().encode()),
                ("market.kraken.BTC-USD.candles.1h", bars[2].to_json().encode()),
                ("market.kraken.BTC-USD.candles.1h", bars[3].to_json().encode()),
                asyncio.CancelledError(),
            ]
        )
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert len(strategy.candle_buffer["BTC-USD"]) == 2
        assert strategy.candle_buffer["BTC-USD"][0].close == pytest.approx(102.0)
        assert strategy.candle_buffer["BTC-USD"][1].close == pytest.approx(103.0)

    @pytest.mark.asyncio
    async def test_listen_loop_emits_signals(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop calls on_candle handler.

        Given: Strategy receiving candle message,
        When: _listen_loop processes message,
        Then: on_candle invoked and candle recorded.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        strategy._generate_signal = False
        candle = make_candle_envelope("BTC-USD", 50000.0, exchange="kraken")
        mock_recv = AsyncMock(
            side_effect=[
                ("market.kraken.BTC-USD.candles.1h", candle.to_json().encode()),
                asyncio.CancelledError(),
            ]
        )
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert len(strategy.candles_processed) == 1
        instrument, received_candle = strategy.candles_processed[0]
        assert instrument == "BTC-USD"
        assert received_candle.close == pytest.approx(50000.0)

    @pytest.mark.asyncio
    async def test_listen_loop_handles_exception(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop handles exceptions gracefully.

        Given: Strategy with subscriber raising error,
        When: _listen_loop runs,
        Then: Strategy stops running.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        mock_recv = AsyncMock(side_effect=ValueError("Test error"))
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        await strategy._listen_loop()
        assert strategy._running is False

    @pytest.mark.asyncio
    async def test_listen_loop_handles_replay_start(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop handles replay.start message.

        Given: Strategy receiving replay.start message,
        When: _listen_loop processes message,
        Then: Strategy reset called and timestamp updated.
        """
        strategy_config.inputs = ["market.paper.kraken.BTC-USD.candles"]
        strategy = ReplayAwareStrategy(strategy_config)
        strategy._running = True
        replay_payload = json.dumps(
            {
                "type": "replay_start",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
                "started_at": "2024-01-01T00:02:03.450000+00:00",
            }
        ).encode()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[("system.replay.start", replay_payload), asyncio.CancelledError()]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy.reset_count == 1
        assert strategy._last_data_ts == pytest.approx(1704067323.45)

    @pytest.mark.asyncio
    async def test_listen_loop_handles_replay_end(self, strategy_config: StrategyConfig) -> None:
        """Verify listen_loop handles replay.end message.

        Given: Strategy receiving replay.end message,
        When: _listen_loop processes message,
        Then: _last_data_ts reset to None.
        """
        strategy_config.inputs = ["market.paper.kraken.BTC-USD.candles"]
        strategy = ReplayAwareStrategy(strategy_config)
        strategy._running = True
        strategy._last_data_ts = 55.5
        replay_end_payload = json.dumps(
            {
                "type": "replay_end",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
            }
        ).encode()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[("system.replay.end", replay_end_payload), asyncio.CancelledError()]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy.reset_count == 0
        assert strategy._last_data_ts is None

    @pytest.mark.asyncio
    async def test_listen_loop_stops_when_not_running(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify listen_loop exits when _running is False.

        Given: Strategy with _running=False,
        When: _listen_loop called,
        Then: Loop exits without processing.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = False
        mock_recv = AsyncMock()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = mock_recv
        strategy.subscriber = mock_subscriber
        await strategy._listen_loop()
        assert mock_recv.call_count == 0

    @pytest.mark.asyncio
    async def test_listen_loop_updates_last_ts_from_market(self) -> None:
        """Verify listen_loop updates _last_data_ts from market data.

        Given: Strategy receiving candle with timestamp,
        When: _listen_loop processes candle,
        Then: _last_data_ts updated to candle timestamp.
        """
        config = StrategyConfig(
            name="ts_market",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        candle = make_candle_envelope("BTC-USD", 100.0, ts=42.5, exchange="kraken")
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("market.kraken.BTC-USD.candles.1h", candle.to_json().encode()),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy._last_data_ts == pytest.approx(42.5)

    @pytest.mark.asyncio
    async def test_listen_loop_ignores_signal_topics(self) -> None:
        """Verify listen_loop ignores signal topics for timestamp.

        Given: Strategy receiving signal topic message,
        When: _listen_loop processes message,
        Then: _last_data_ts unchanged.
        """
        config = StrategyConfig(
            name="ts_signal",
            strategy_class="SimpleTestStrategy",
            inputs=["signals.paper.BTC-USD.input"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 100.0
        signal_payload = json.dumps({"ts": "321.9", "value": 1}).encode()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("signals.paper.BTC-USD.input", signal_payload),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy._last_data_ts == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_listen_loop_triggers_symbol_alias_refresh(
        self, strategy_config: StrategyConfig, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify listen_loop triggers cache invalidation on symbol_aliases.

        Given: Strategy receiving symbol_aliases message,
        When: _listen_loop processes message,
        Then: DB mapper cache invalidation triggered.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        mapper = MagicMock()
        monkeypatch.setattr("snapper.strategies.system_events._get_db_mapper", lambda: mapper)
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[("system.symbol_aliases", b"{}"), asyncio.CancelledError()]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        mapper.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)

    @pytest.mark.asyncio
    async def test_listen_loop_records_feed_heartbeat(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify listen_loop records feed heartbeat data.

        Given: Strategy receiving feed heartbeat message,
        When: _listen_loop processes message,
        Then: _feed_heartbeats updated with status and lag.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        heartbeat = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="feed.kraken",
            sequence=1,
            status="healthy",
            lag_ms=42,
            meta={"symbol_count": 7},
        )
        heartbeat_payload = heartbeat.to_json().encode()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("system.heartbeats.feed.kraken", heartbeat_payload),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert "kraken" in strategy._feed_heartbeats
        heartbeat_state = strategy._feed_heartbeats["kraken"]
        assert heartbeat_state["status"] == "healthy"
        assert heartbeat_state["lag_ms"] == 42
        assert heartbeat_state["symbol_count"] == 7


class TestEmitSignal:
    """Test suite for signal emission."""

    @pytest.mark.asyncio
    async def test_emit_signal_creates_publisher(self, strategy_config: StrategyConfig) -> None:
        """Verify emit_signal creates publisher on first call.

        Given: Strategy without publisher,
        When: emit_signal called,
        Then: PUB socket created.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_socket = MagicMock()
        mock_socket.send_multipart = AsyncMock(return_value=None)
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            reason="Test",
        )
        with patch("zmq.asyncio.Context", return_value=mock_context):
            strategy.zmq_context = mock_context
            await strategy.emit_signal(signal)
        assert strategy.publisher is not None
        mock_context.socket.assert_called_once_with(zmq.PUB)

    @pytest.mark.asyncio
    async def test_emit_signal_connects_to_broker(self, strategy_config: StrategyConfig) -> None:
        """Verify emit_signal connects publisher to broker.

        Given: Strategy with use_broker=True,
        When: emit_signal called,
        Then: Socket connect called.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy.params["use_broker"] = True
        mock_socket = MagicMock()
        mock_socket.send_multipart = AsyncMock(return_value=None)
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            reason="Test",
        )
        with patch("zmq.asyncio.Context", return_value=mock_context):
            strategy.zmq_context = mock_context
            await strategy.emit_signal(signal)
        mock_socket.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_emit_signal_binds_without_broker(self, strategy_config: StrategyConfig) -> None:
        """Verify emit_signal connects to address without broker.

        Given: Strategy without broker,
        When: emit_signal called,
        Then: Socket connects to tcp:// address.
        """
        mock_subscriber_socket = MagicMock()
        mock_subscriber_socket.subscribe = MagicMock()
        mock_subscriber_socket.close = MagicMock()
        mock_publisher_socket = MagicMock()
        mock_publisher_socket.send_multipart = AsyncMock(return_value=None)
        mock_publisher_socket.connect = MagicMock()
        mock_context = MagicMock()

        def socket_factory(socket_type: int) -> MagicMock:
            if socket_type == zmq.SUB:
                return mock_subscriber_socket
            elif socket_type == zmq.PUB:
                return mock_publisher_socket
            return MagicMock()

        mock_context.socket.side_effect = socket_factory

        async def mock_listen_loop() -> None:
            """Intentionally empty mock implementation."""
            pass

        with patch("zmq.asyncio.Context", return_value=mock_context):
            strategy = SimpleTestStrategy(strategy_config)
            with patch.object(strategy, "_listen_loop", side_effect=mock_listen_loop):
                try:
                    strategy.zmq_context = mock_context
                    signal = StrategySignal(
                        instrument="BTC-USD",
                        side="buy",
                        strength=0.8,
                        price=50000.0,
                        reason="Test",
                    )
                    await strategy.emit_signal(signal)
                    mock_publisher_socket.connect.assert_called_once()
                    connect_addr = mock_publisher_socket.connect.call_args[0][0]
                    assert "tcp://" in connect_addr
                finally:
                    await strategy.stop()

    @pytest.mark.asyncio
    async def test_emit_signal_publishes_to_output_topic(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify emit_signal publishes to correct output topic.

        Given: Strategy with publisher,
        When: emit_signal called with signal,
        Then: StrategySignal published to output topic.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            reason="Test signal",
        )
        await strategy.emit_signal(signal)
        mock_msg_publisher.send.assert_called_once()
        published_signal: SignalData = mock_msg_publisher.send.call_args[0][1]
        assert published_signal.instrument == "BTC-USD"
        assert published_signal.side == "buy"
        assert published_signal.strength == pytest.approx(0.8)
        assert published_signal.price == pytest.approx(50000.0)
        assert published_signal.reason == "Test signal"
        assert published_signal.fired_at is not None
        assert published_signal.strategy_name == "test_strategy"

    @pytest.mark.asyncio
    async def test_emit_signal_reuses_existing_publisher(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify emit_signal reuses existing publisher.

        Given: Strategy with existing publisher,
        When: emit_signal called,
        Then: Same publisher instance used.
        """
        strategy = SimpleTestStrategy(strategy_config)
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="sell",
            strength=0.5,
            price=49000.0,
            reason="Test",
        )
        await strategy.emit_signal(signal)
        assert strategy.msg_publisher is mock_msg_publisher

    @pytest.mark.asyncio
    async def test_emit_signal_reuses_publisher_and_propagates_ts(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify emit_signal propagates _last_data_ts to signal.

        Given: Strategy with _last_data_ts set,
        When: emit_signal called,
        Then: StrategySignal timestamp matches _last_data_ts.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._last_data_ts = 1234.5
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            price=50001.0,
            reason="Timestamp propagation",
        )
        await strategy.emit_signal(signal)
        assert signal.timestamp == datetime.fromtimestamp(1234.5, tz=UTC)
        mock_msg_publisher.send.assert_awaited_once()
        published_signal: SignalData = mock_msg_publisher.send.call_args[0][1]
        assert published_signal.exchange == "paper"
        assert published_signal.strategy_name == "test_strategy"
        assert published_signal.fired_at is not None

    @pytest.mark.asyncio
    async def test_emit_signal_live_topic_and_timestamp(self) -> None:
        """Verify emit_signal uses live topic for non-paper exchange.

        Given: Strategy with kraken exchange,
        When: emit_signal called,
        Then: StrategySignal published to signals.kraken.BTC-USD.live topic.
        """
        config = StrategyConfig(
            name="live_strategy",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
            exchange="kraken",
        )
        strategy = SimpleTestStrategy(config)
        strategy._last_data_ts = 777.7
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            price=50500.0,
            reason="Live test",
        )
        await strategy.emit_signal(signal)
        assert signal.timestamp == datetime.fromtimestamp(777.7, tz=UTC)
        mock_msg_publisher.send.assert_awaited_once()
        published_signal: SignalData = mock_msg_publisher.send.call_args[0][1]
        assert published_signal.exchange == "kraken"

    @pytest.mark.asyncio
    async def test_emit_signal_preserves_existing_timestamp(self) -> None:
        """Verify emit_signal preserves pre-set signal timestamp.

        Given: StrategySignal with explicit timestamp,
        When: emit_signal called,
        Then: Original timestamp preserved.
        """
        config = StrategyConfig(
            name="test_strategy",
            strategy_class="SimpleTestStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._last_data_ts = 999.0
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            price=50500.0,
            reason="Explicit timestamp test",
            timestamp=datetime.fromtimestamp(123456789.0, tz=UTC),
        )
        await strategy.emit_signal(signal)
        assert signal.timestamp == datetime.fromtimestamp(123456789.0, tz=UTC)

    @pytest.mark.asyncio
    async def test_emit_signal_skips_publish_when_no_publisher(self) -> None:
        """Verify emit_signal skips publish when no publisher available.

        Given: Strategy with publisher=None and no-op setup,
        When: emit_signal called,
        Then: No error raised.
        """
        config = StrategyConfig(
            name="test_strategy",
            strategy_class="SimpleTestStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy.msg_publisher = None

        async def noop_setup() -> None:
            """Intentionally empty mock implementation."""
            pass

        strategy._setup_publisher = noop_setup
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            price=50500.0,
            reason="No publisher test",
            timestamp=datetime.fromtimestamp(123.0, tz=UTC),
        )
        await strategy.emit_signal(signal)

    @pytest.mark.asyncio
    async def test_emit_signal_validates_outputs(self, strategy_config: StrategyConfig) -> None:
        """Verify emit_signal rejects signal for non-output instrument.

        Given: Strategy with outputs=[ETH-USD],
        When: emit_signal called with BTC-USD instrument,
        Then: ValueError raised.
        """
        strategy_config.outputs = ["ETH-USD"]
        strategy = SimpleTestStrategy(strategy_config)
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.5,
            price=101.0,
            reason="mismatched instrument",
        )
        with pytest.raises(ValueError):
            await strategy.emit_signal(signal)


class TestStop:
    """Test suite for strategy stop functionality."""

    @pytest.mark.asyncio
    async def test_stop_cancels_listen_task(self, strategy_config: StrategyConfig) -> None:
        """Verify stop cancels _listen_task.

        Given: Strategy with running _listen_task,
        When: stop() called,
        Then: Task cancelled and _running=False.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True

        async def dummy_task() -> None:
            """Simulate a long-running task for cancellation testing."""
            await asyncio.sleep(100)

        task = asyncio.create_task(dummy_task())
        strategy._listen_task = task
        await strategy.stop()
        assert task.cancelled() or task.done()
        assert strategy._running is False

    @pytest.mark.asyncio
    async def test_stop_closes_publisher(self, strategy_config: StrategyConfig) -> None:
        """Verify stop closes publisher socket.

        Given: Strategy with publisher,
        When: stop() called,
        Then: setsockopt and close called, publisher=None.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        setsockopt_calls: list[tuple[int, int]] = []
        close_called = False

        def mock_setsockopt(opt: int, val: int) -> None:
            setsockopt_calls.append((opt, val))

        def mock_close() -> None:
            nonlocal close_called
            close_called = True

        mock_publisher = MagicMock()
        mock_publisher.setsockopt = mock_setsockopt
        mock_publisher.close = mock_close
        strategy.publisher = mock_publisher
        await strategy.stop()
        assert len(setsockopt_calls) == 1
        assert setsockopt_calls[0][0] == 17
        assert setsockopt_calls[0][1] == 0
        assert close_called
        assert strategy.publisher is None

    @pytest.mark.asyncio
    async def test_stop_awaits_async_publisher_close(self, strategy_config: StrategyConfig) -> None:
        """Verify stop awaits async publisher close methods.

        Given: Strategy with publisher exposing AsyncMock close,
        When: stop() is called,
        Then: Close is awaited and publisher is cleared.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True
        mock_publisher = MagicMock()
        mock_publisher.setsockopt = MagicMock()
        mock_publisher.close = AsyncMock()
        strategy.publisher = mock_publisher
        await strategy.stop()
        mock_publisher.setsockopt.assert_called_once_with(zmq.LINGER, 0)
        mock_publisher.close.assert_awaited_once()
        assert strategy.publisher is None

    @pytest.mark.asyncio
    async def test_stop_handles_completed_task(self, strategy_config: StrategyConfig) -> None:
        """Verify stop handles already-completed task.

        Given: Strategy with completed _listen_task,
        When: stop() called,
        Then: No error, task remains done but not cancelled.
        """
        strategy = SimpleTestStrategy(strategy_config)
        strategy._running = True

        async def completed_task() -> None:
            """Immediately-completing coroutine for test."""

        task = asyncio.create_task(completed_task())
        await asyncio.sleep(0.01)
        assert task.done()
        strategy._listen_task = task
        await strategy.stop()
        assert task.done()
        assert not task.cancelled()


class TestBaseStrategyProperties:
    """Test suite for BaseStrategy property accessors."""

    def test_is_running_property(self, strategy_config: StrategyConfig) -> None:
        """Verify is_running property reflects _running state.

        Given: Strategy instance,
        When: _running toggled,
        Then: is_running property matches.
        """
        strategy = SimpleTestStrategy(strategy_config)
        assert strategy.is_running is False
        strategy._running = True
        assert strategy.is_running is True


class TestCompositeStrategy:
    """Test suite for CompositeStrategy functionality."""

    @pytest.mark.asyncio
    async def test_composite_strategy_initialization(self) -> None:
        """Verify CompositeStrategy initializes with sub-strategies.

        Given: CompositeStrategy config and sub-strategy,
        When: CompositeStrategy instantiated,
        Then: sub_strategies list contains sub-strategy.
        """

        class TestCompositeStrategy(CompositeStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

        config = StrategyConfig(
            name="composite",
            strategy_class="TestCompositeStrategy",
            inputs=["signals.paper.BTC-USD.macd", "signals.paper.ETH-USD.rsi"],
            outputs=["BTC-USD"],
        )
        sub_config1 = StrategyConfig(
            name="macd",
            strategy_class="SimpleTestStrategy",
            inputs=["BTC-USD:1h"],
            outputs=["BTC-USD"],
        )
        sub1 = SimpleTestStrategy(sub_config1)
        composite = TestCompositeStrategy(config, sub_strategies=[sub1])
        assert len(composite.sub_strategies) == 1
        assert composite.sub_strategies[0] is sub1

    @pytest.mark.asyncio
    async def test_composite_add_sub_strategy(self) -> None:
        """Verify adding sub-strategy to composite."""

        class TestCompositeStrategy(CompositeStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

        config = StrategyConfig(
            name="composite",
            strategy_class="TestCompositeStrategy",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        composite = TestCompositeStrategy(config)
        sub_config = StrategyConfig(
            name="macd",
            strategy_class="SimpleTestStrategy",
            inputs=["BTC-USD:1h"],
            outputs=["BTC-USD"],
        )
        sub = SimpleTestStrategy(sub_config)
        await composite.add_sub_strategy(sub)
        assert len(composite.sub_strategies) == 1
        assert composite.sub_strategies[0] is sub

    @pytest.mark.asyncio
    async def test_composite_add_sub_strategy_invalid_output(self) -> None:
        """Verify add_sub_strategy rejects non-matching outputs.

        Given: CompositeStrategy with BTC-USD input,
        When: add_sub_strategy with ETH-USD output,
        Then: ValueError raised.
        """

        class TestCompositeStrategy(CompositeStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

        config = StrategyConfig(
            name="composite",
            strategy_class="TestCompositeStrategy",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        composite = TestCompositeStrategy(config)
        sub_config = StrategyConfig(
            name="rsi",
            strategy_class="SimpleTestStrategy",
            inputs=["BTC-USD:1h"],
            outputs=["ETH-USD"],
        )
        sub = SimpleTestStrategy(sub_config)
        with pytest.raises(ValueError, match="None of sub-strategy output topics"):
            await composite.add_sub_strategy(sub)

    @pytest.mark.asyncio
    async def test_composite_strategy_with_no_sub_strategies(self) -> None:
        """Verify CompositeStrategy works with no sub-strategies.

        Given: CompositeStrategy config without sub_strategies,
        When: CompositeStrategy instantiated,
        Then: sub_strategies list is empty.
        """

        class TestCompositeStrategy(CompositeStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

        config = StrategyConfig(
            name="composite",
            strategy_class="TestCompositeStrategy",
            inputs=["macd"],
            outputs=["BTC-USD"],
        )
        composite = TestCompositeStrategy(config)
        assert len(composite.sub_strategies) == 0


class TestReplayHandling:
    """Test suite for historical replay handling."""

    @pytest.mark.asyncio
    async def test_listen_loop_handles_replay_start(self) -> None:
        """Verify listen_loop handles replay.start message.

        Given: Strategy receiving replay.start with timestamp,
        When: _listen_loop processes message,
        Then: reset called and _last_data_ts set.
        """
        config = StrategyConfig(
            name="replay_strategy",
            strategy_class="ReplayAwareStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = ReplayAwareStrategy(config)
        strategy._running = True
        replay_payload = json.dumps(
            {
                "type": "replay_start",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
                "started_at": "2024-01-01T00:02:03.456000+00:00",
            }
        ).encode()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("system.replay.start", replay_payload),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy.reset_count == 1
        assert strategy._last_data_ts == pytest.approx(1704067323.456)

    @pytest.mark.asyncio
    async def test_listen_loop_handles_replay_end(self) -> None:
        """Verify listen_loop handles replay.end message.

        Given: Strategy receiving replay.start then replay.end,
        When: _listen_loop processes messages,
        Then: _last_data_ts reset to None.
        """
        config = StrategyConfig(
            name="replay_strategy",
            strategy_class="ReplayAwareStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = ReplayAwareStrategy(config)
        strategy._running = True
        replay_start = json.dumps(
            {
                "type": "replay_start",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
                "started_at": "2024-01-01T00:00:50.000000+00:00",
            }
        ).encode()
        replay_end = json.dumps(
            {
                "type": "replay_end",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
            }
        ).encode()
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("system.replay.start", replay_start),
                ("system.replay.end", replay_end),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy.reset_count == 1
        assert strategy._last_data_ts is None

    @pytest.mark.asyncio
    async def test_listen_loop_replay_end_branch(self) -> None:
        """Verify replay.end resets _last_data_ts.

        Given: Strategy with _last_data_ts set,
        When: replay.end received,
        Then: _last_data_ts reset to None.
        """
        config = StrategyConfig(
            name="replay_branch",
            strategy_class="ReplayAwareStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = ReplayAwareStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 777.0
        replay_end = json.dumps(
            {
                "type": "replay_end",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
            }
        ).encode()

        class StubSubscriber:
            def __init__(self) -> None:
                self.sent = False

            def close(self) -> None:
                """No-op close for test stub."""
                pass

            async def recv_multipart(self) -> tuple[str, bytes]:
                if not self.sent:
                    self.sent = True
                    return "system.replay.end", replay_end
                strategy._running = False
                await asyncio.sleep(0)
                return "market.paper.kraken.BTC-USD.candles", b"{}"

        strategy.subscriber = cast(Any, StubSubscriber())
        await strategy._listen_loop()
        assert strategy._last_data_ts is None

    @pytest.mark.asyncio
    async def test_listen_loop_replay_end_single_message(self) -> None:
        """Verify replay.end logs replay ended message.

        Given: Strategy receiving only replay.end,
        When: _listen_loop processes message,
        Then: 'Replay ended' logged and _last_data_ts reset.
        """
        config = StrategyConfig(
            name="replay_end_only",
            strategy_class="ReplayAwareStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = ReplayAwareStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 99.0
        replay_end_payload = json.dumps(
            {
                "type": "replay_end",
                "session_id": "",
                "sequence_id": 0,
                "public_id": "test-pid",
                "timestamp": "2024-01-01T00:00:00Z",
            }
        ).encode()

        class SingleMessageSubscriber:
            def __init__(self) -> None:
                self.sent = False

            def close(self) -> None:
                """No-op close for test stub."""
                pass

            async def recv_multipart(self) -> tuple[str, bytes]:
                if not self.sent:
                    self.sent = True
                    strategy._running = False
                    return "system.replay.end", replay_end_payload
                raise AssertionError("Unexpected extra recv_multipart call")

        strategy.subscriber = cast(Any, SingleMessageSubscriber())
        with patch("snapper.strategies.system_events.logger.info") as mock_info:
            await strategy._listen_loop()
        assert strategy._last_data_ts is None
        mock_info.assert_called_once()
        assert "Replay ended" in mock_info.call_args.args[0]

    @pytest.mark.asyncio
    async def test_listen_loop_ignores_unknown_system_message(self) -> None:
        """Verify listen_loop ignores unknown system messages.

        Given: Strategy receiving unknown system message,
        When: _listen_loop processes message,
        Then: _last_data_ts unchanged.
        """
        config = StrategyConfig(
            name="replay_unknown",
            strategy_class="ReplayAwareStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = ReplayAwareStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 88.0
        unknown_payload = json.dumps({"foo": "bar"}).encode()

        class UnknownSystemSubscriber:
            def __init__(self) -> None:
                self.calls = 0

            def close(self) -> None:
                """No-op close for test stub."""
                pass

            async def recv_multipart(self) -> tuple[str, bytes]:
                self.calls += 1
                if self.calls == 1:
                    return "system.replay.unknown", unknown_payload
                strategy._running = False
                return "market.paper.kraken.BTC-USD.candles", b"{}"

        strategy.subscriber = cast(Any, UnknownSystemSubscriber())
        await strategy._listen_loop()
        assert strategy._last_data_ts == pytest.approx(88.0)


class TestHeartbeatLoop:
    """Test suite for heartbeat emission loop."""

    def _make_strategy(self) -> ReplayAwareStrategy:
        config = StrategyConfig(
            name="replay_strategy",
            strategy_class="ReplayAwareStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        return ReplayAwareStrategy(config)

    async def _run_heartbeat(
        self, strategy: ReplayAwareStrategy, current_time: float
    ) -> tuple[str, dict[str, Any], list[float]]:
        published_messages: list[Any] = []
        sleep_calls: list[float] = []

        async def capture_and_stop(topic: str, data: Any, **kwargs: Any) -> None:
            published_messages.append(data)
            strategy._running = False

        mock_msg_pub = AsyncMock(side_effect=capture_and_stop)
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = mock_msg_pub
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        strategy._running = True

        async def fake_sleep(duration: float) -> None:
            sleep_calls.append(duration)

        with (
            patch("snapper.strategies.health.asyncio.sleep", new=fake_sleep),
            patch("snapper.strategies.health.time.time", return_value=current_time),
        ):
            await strategy._heartbeat_loop()
        assert len(published_messages) == 1
        hb: HeartbeatData = published_messages[0]
        topic = f"system.heartbeats.{hb.component}"
        payload_data = json.loads(hb.to_json())
        return topic, payload_data, sleep_calls

    @pytest.mark.asyncio
    async def test_heartbeat_loop_sends_feed_health(self) -> None:
        """Verify heartbeat loop includes feed health info.

        Given: Strategy with feed heartbeat data,
        When: _heartbeat_loop runs,
        Then: Published heartbeat includes feed_health meta.
        """
        strategy = self._make_strategy()
        strategy.last_data_timestamp = 999.5
        strategy._feed_heartbeats = {
            "kraken": {
                "timestamp": 999.0,
                "status": "ok",
                "lag_ms": 30,
                "component": "feed.kraken",
                "symbol_count": 2,
            }
        }
        topic, payload, sleep_calls = await self._run_heartbeat(strategy, current_time=1000.0)
        assert topic.startswith("system.heartbeats.strategy.")
        feed_health = payload["meta"]["feed_health"]
        assert feed_health["kraken"]["healthy"] is True
        assert sleep_calls == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_heartbeat_loop_status_ok(self) -> None:
        """Verify heartbeat status is healthy when lag is low.

        Given: Strategy with recent data timestamp,
        When: _heartbeat_loop runs,
        Then: Status is 'healthy' with correct lag_ms.
        """
        strategy = self._make_strategy()
        strategy.last_data_timestamp = 100.0
        topic, payload, sleep_calls = await self._run_heartbeat(strategy, current_time=101.5)
        assert topic == f"system.heartbeats.strategy.{strategy.name}"
        assert payload["status"] == "healthy"
        assert payload["lag_ms"] == 1500
        assert payload["component"] == f"strategy.{strategy.name}"
        assert sleep_calls == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_heartbeat_loop_status_warn(self) -> None:
        """Verify heartbeat status is warning when lag is moderate.

        Given: Strategy with 4-second data lag,
        When: _heartbeat_loop runs,
        Then: Status is 'warning'.
        """
        strategy = self._make_strategy()
        strategy.last_data_timestamp = 100.0
        _, payload, _ = await self._run_heartbeat(strategy, current_time=104.0)
        assert payload["status"] == "warning"
        assert payload["lag_ms"] == 4000

    @pytest.mark.asyncio
    async def test_heartbeat_loop_status_error(self) -> None:
        """Verify heartbeat status is error when lag is high.

        Given: Strategy with 12.5-second data lag,
        When: _heartbeat_loop runs,
        Then: Status is 'error'.
        """
        strategy = self._make_strategy()
        strategy.last_data_timestamp = 100.0
        _, payload, _ = await self._run_heartbeat(strategy, current_time=112.5)
        assert payload["status"] == "error"
        assert payload["lag_ms"] == 12500

    @pytest.mark.asyncio
    async def test_heartbeat_loop_handles_publish_error(self) -> None:
        """Verify heartbeat loop logs error on publish failure.

        Given: Strategy with failing publisher,
        When: _heartbeat_loop runs,
        Then: 'Heartbeat error' logged.
        """
        strategy = self._make_strategy()
        strategy.last_data_timestamp = 100.0
        strategy._running = True

        class FailingMsgPublisher:
            """Test stub that fails on send."""

            def __init__(self, owner: ReplayAwareStrategy):
                """Initialize the instance."""
                self.owner = owner
                self.calls = 0
                self.tracker = SequenceTracker()
                self.session_id = self.tracker.session_id

            async def send(self, topic: str, data: Any, **kwargs: Any) -> None:
                """Raise error on send."""
                self.calls += 1
                self.owner._running = False
                raise RuntimeError("publisher failure")

        failing_msg_publisher = FailingMsgPublisher(strategy)
        strategy.msg_publisher = cast(Any, failing_msg_publisher)

        async def fake_sleep(duration: float) -> None:
            return None

        with (
            patch("snapper.strategies.health.logger.exception") as mock_error,
            patch("snapper.strategies.health.asyncio.sleep", new=fake_sleep),
            patch("snapper.strategies.health.time.time", return_value=120.0),
        ):
            await strategy._heartbeat_loop()
        assert failing_msg_publisher.calls == 1
        assert strategy._running is False
        mock_error.assert_called_once()
        assert "Heartbeat error" in mock_error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_heartbeat_loop_cancelled(self) -> None:
        """Verify heartbeat loop logs cancellation.

        Given: Running heartbeat loop,
        When: Task cancelled,
        Then: 'Heartbeat loop cancelled' logged.
        """
        strategy = self._make_strategy()
        strategy._running = True
        real_sleep = asyncio.sleep

        async def fast_sleep(duration: float) -> None:
            await real_sleep(0)

        with (
            patch("snapper.strategies.health.logger.info") as mock_info,
            patch("snapper.strategies.health.asyncio.sleep", new=fast_sleep),
        ):
            task = asyncio.create_task(strategy._heartbeat_loop())
            await real_sleep(0)
            await real_sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        mock_info.assert_called_once()
        assert "Heartbeat loop cancelled" in mock_info.call_args.args[0]


class TestSetupPublisher:
    """Test suite for publisher setup."""

    @pytest.mark.asyncio
    async def test_setup_publisher_early_return_when_exists(self) -> None:
        """Verify _setup_publisher returns early if publisher exists.

        Given: Strategy with existing publisher,
        When: _setup_publisher called,
        Then: Existing publisher preserved.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        mock_publisher = MagicMock()
        mock_msg_publisher = MagicMock()
        strategy.publisher = mock_publisher
        strategy.msg_publisher = mock_msg_publisher
        strategy.zmq_context = MagicMock()
        await strategy._setup_publisher()
        assert strategy.publisher is mock_publisher
        assert strategy.msg_publisher is mock_msg_publisher


class TestSubscribeInputsBranches:
    """Test suite for subscription edge cases."""

    @pytest.mark.asyncio
    async def test_subscribe_non_market_topic_skips_heartbeat(self) -> None:
        """Verify non-market topic skips heartbeat subscription.

        Given: Strategy with signal topic input,
        When: _subscribe_inputs called,
        Then: No heartbeat subscription created.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        strategy.zmq_context = mock_context
        subscribed_topics: list[str] = []

        class MockValidatedSubscriber:
            def __init__(self, socket: Any) -> None:
                """Intentionally empty stub for testing."""
                pass

            def subscribe(self, topic: str) -> None:
                subscribed_topics.append(topic)

        with patch("snapper.strategies.base.ValidatedSubscriber", MockValidatedSubscriber):
            await strategy._subscribe_inputs()
        assert "signals.paper.BTC-USD.macd" in subscribed_topics
        assert "system.symbol_aliases" in subscribed_topics
        assert "system.settings" in subscribed_topics
        assert not any("system.heartbeats.feed" in t for t in subscribed_topics)

    @pytest.mark.asyncio
    async def test_subscribe_short_topic_skips_heartbeat(self) -> None:
        """Verify short topic skips heartbeat subscription.

        Given: Strategy with minimal market topic,
        When: _subscribe_inputs called,
        Then: Topic subscribed but no heartbeat.
        """
        config = object.__new__(StrategyConfig)
        config.name = "test"
        config.strategy_class = "SimpleTestStrategy"
        config.inputs = ["market."]
        config.outputs = ["BTC-USD"]
        config.exchange = "paper"
        config.params = {}
        strategy = SimpleTestStrategy(config)
        strategy._is_paper_input = False
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        strategy.zmq_context = mock_context
        subscribed_topics: list[str] = []

        class MockValidatedSubscriber:
            def __init__(self, socket: Any) -> None:
                """Intentionally empty stub for testing."""
                pass

            def subscribe(self, topic: str) -> None:
                subscribed_topics.append(topic)

        with patch("snapper.strategies.base.ValidatedSubscriber", MockValidatedSubscriber):
            await strategy._subscribe_inputs()
        assert "market." in subscribed_topics

    @pytest.mark.asyncio
    async def test_subscribe_single_segment_topic_skips_heartbeat(self) -> None:
        """Verify single-segment topic skips heartbeat subscription.

        Given: Strategy with single-segment 'market' topic,
        When: _subscribe_inputs called,
        Then: Topic subscribed but no heartbeat.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        strategy.zmq_context = mock_context
        subscribed_topics: list[str] = []

        class MockValidatedSubscriber:
            def __init__(self, socket: Any) -> None:
                """Intentionally empty stub for testing."""
                pass

            def subscribe(self, topic: str) -> None:
                subscribed_topics.append(topic)

        with patch("snapper.strategies.base.ValidatedSubscriber", MockValidatedSubscriber):
            await strategy._subscribe_inputs()
        assert "market" in subscribed_topics
        assert not any("system.heartbeats.feed" in t for t in subscribed_topics)


class TestHeartbeatWithFeedHealth:
    """Test suite for heartbeat with feed health info."""

    @pytest.mark.asyncio
    async def test_heartbeat_includes_feed_health_info(self) -> None:
        """Verify heartbeat includes feed health metadata.

        Given: Strategy with feed heartbeats recorded,
        When: _heartbeat_loop runs,
        Then: Published heartbeat has feed_health in meta.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy.last_data_timestamp = time.time()
        strategy._feed_heartbeats = {
            "kraken": {
                "timestamp": time.time() - 1.0,
                "status": "healthy",
                "lag_ms": 50,
            }
        }
        published_messages: list[Any] = []

        class CapturingMsgPublisher:
            """Test stub that captures published messages."""

            def __init__(self, owner: SimpleTestStrategy) -> None:
                """Initialize the instance."""
                self.owner = owner
                self.tracker = SequenceTracker()
                self.session_id = self.tracker.session_id

            async def send(self, topic: str, data: Any, **kwargs: Any) -> None:
                """Capture sent data and stop."""
                published_messages.append(data)
                self.owner._running = False

        strategy.msg_publisher = cast(Any, CapturingMsgPublisher(strategy))
        with patch("snapper.strategies.health.asyncio.sleep", new_callable=AsyncMock):
            await strategy._heartbeat_loop()
        assert len(published_messages) >= 1
        hb: HeartbeatData = published_messages[0]
        feed_health = hb.meta["feed_health"]
        assert feed_health is not None
        assert isinstance(feed_health, dict)
        assert "kraken" in feed_health


class TestHeartbeatNoPublisher:
    """Test suite for heartbeat without publisher."""

    @pytest.mark.asyncio
    async def test_heartbeat_no_publisher_skips_send(self) -> None:
        """Verify heartbeat loop skips send when no publisher.

        Given: Strategy without publisher,
        When: _heartbeat_loop runs,
        Then: Loop continues without error.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy.last_data_timestamp = time.time()
        strategy.msg_publisher = None
        iterations = 0

        async def count_and_stop(_duration: float) -> None:
            nonlocal iterations
            iterations += 1
            if iterations >= 2:
                strategy._running = False

        with patch("snapper.strategies.health.asyncio.sleep", count_and_stop):
            await strategy._heartbeat_loop()
        assert iterations >= 2


class TestListenLoopSystemMessages:
    """Test suite for system message handling in listen loop."""

    @pytest.mark.asyncio
    async def test_listen_loop_symbol_aliases_refresh(self) -> None:
        """Verify symbol_aliases triggers cache invalidation.

        Given: Strategy receiving symbol_aliases message,
        When: _listen_loop processes message,
        Then: DB mapper cache invalidation triggered.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("system.symbol_aliases", json.dumps({"updated": True}).encode()),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with (
            patch("snapper.strategies.system_events._get_db_mapper") as mock_mapper,
            pytest.raises(asyncio.CancelledError),
        ):
            mock_mapper_instance = MagicMock()
            mock_mapper.return_value = mock_mapper_instance
            await strategy._listen_loop()
            mock_mapper_instance.trigger_cache_invalidation.assert_called_once_with(fail_fast=False)

    @pytest.mark.asyncio
    async def test_listen_loop_settings_update(self) -> None:
        """Verify settings update handled by handler.

        Given: Strategy receiving settings message,
        When: _listen_loop processes message,
        Then: _handle_settings_update invoked.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                (
                    "system.settings",
                    json.dumps(
                        {
                            "type": "setting_changed",
                            "session_id": "",
                            "sequence_id": 0,
                            "public_id": "test-pid",
                            "timestamp": "2024-01-01T00:00:00Z",
                            "key": "foo",
                            "value": "bar",
                            "category": "test",
                        }
                    ).encode(),
                ),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        handle_mock = MagicMock()
        strategy._handle_settings_update = handle_mock
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        handle_mock.assert_called_once()

    def test_handle_settings_update_success(self) -> None:
        """Verify settings update updates cache.

        Given: Strategy and SettingsService,
        When: _handle_settings_update called,
        Then: Cache updated with parsed value.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        envelope = SettingChangedData(
            session_id="",
            sequence_id=0,
            key="test_key",
            value="test_value",
            category="test",
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with patch("snapper.strategies.system_events.SettingsService.get_instance") as mock_service:
            mock_instance = MagicMock()
            mock_instance._parse_value.return_value = "test_value"
            mock_instance._cache = {}
            mock_service.return_value = mock_instance
            strategy._handle_settings_update(envelope)
            mock_instance._parse_value.assert_called_once_with("test_value")
            assert mock_instance._cache["test_key"] == "test_value"

    def test_handle_settings_update_no_instance(self) -> None:
        """Verify settings update handles no service instance.

        Given: SettingsService.get_instance returns None,
        When: _handle_settings_update called,
        Then: No error raised.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        envelope = SettingChangedData(
            session_id="",
            sequence_id=0,
            key="test_key",
            value="test_value",
            category="test",
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with patch("snapper.strategies.system_events.SettingsService.get_instance") as mock_service:
            mock_service.return_value = None
            strategy._handle_settings_update(envelope)

    def test_handle_settings_update_exception(self) -> None:
        """Verify settings update logs exception on error.

        Given: SettingsService._parse_value raises error,
        When: _handle_settings_update called,
        Then: 'Error handling settings update' logged.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        envelope = SettingChangedData(
            session_id="",
            sequence_id=0,
            key="test_key",
            value="test_value",
            category="test",
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with (
            patch("snapper.strategies.system_events.SettingsService.get_instance") as mock_service,
            patch("snapper.strategies.system_events.logger.exception") as mock_error,
        ):
            mock_instance = MagicMock()
            mock_instance._parse_value.side_effect = RuntimeError("parse failed")
            mock_service.return_value = mock_instance
            strategy._handle_settings_update(envelope)
        mock_error.assert_called_once()
        assert "Error handling settings update" in mock_error.call_args.args[0]
        assert "parse failed" in mock_error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_listen_loop_feed_heartbeat_tracking(self) -> None:
        """Verify feed heartbeat stored in _feed_heartbeats.

        Given: Strategy receiving feed heartbeat,
        When: _listen_loop processes message,
        Then: _feed_heartbeats updated with status and lag.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy._feed_heartbeats = {}
        heartbeat = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="feed_kraken",
            sequence=1,
            status="healthy",
            lag_ms=25,
            meta={"symbol_count": 200},
        )
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                (
                    "system.heartbeats.feed.kraken",
                    heartbeat.to_json().encode(),
                ),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert "kraken" in strategy._feed_heartbeats
        assert strategy._feed_heartbeats["kraken"]["status"] == "healthy"
        assert strategy._feed_heartbeats["kraken"]["lag_ms"] == 25
        assert strategy._feed_heartbeats["kraken"]["symbol_count"] == 200


class TestListenLoopSignalTimestamp:
    """Test suite for signal timestamp handling."""

    @pytest.mark.asyncio
    async def test_listen_loop_signal_topics_are_ignored(self) -> None:
        """Verify signal topics do not update _last_data_ts.

        Given: Strategy with signal topic input,
        When: StrategySignal message received,
        Then: _last_data_ts unchanged.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 500.0
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                (
                    "signals.paper.BTC-USD.macd",
                    json.dumps({"ts": 1700000000.5, "side": "buy"}).encode(),
                ),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy._last_data_ts == pytest.approx(500.0)

    @pytest.mark.asyncio
    async def test_listen_loop_signal_does_not_update_ts(self) -> None:
        """Verify signal messages don't update timestamp.

        Given: Strategy with existing _last_data_ts,
        When: StrategySignal message received,
        Then: _last_data_ts unchanged.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 999.0
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                (
                    "signals.paper.BTC-USD.macd",
                    json.dumps({"side": "buy", "strength": 0.8}).encode(),
                ),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy._last_data_ts == pytest.approx(999.0)

    @pytest.mark.asyncio
    async def test_listen_loop_signal_with_non_numeric_ts_ignored(self) -> None:
        """Verify non-numeric signal ts is ignored.

        Given: Strategy receiving signal with array ts,
        When: _listen_loop processes message,
        Then: _last_data_ts unchanged.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._running = True
        strategy._last_data_ts = 888.0
        mock_subscriber = MagicMock()
        mock_subscriber.recv_multipart = AsyncMock(
            side_effect=[
                (
                    "signals.paper.BTC-USD.macd",
                    json.dumps({"ts": [1, 2, 3], "side": "buy"}).encode(),
                ),
                asyncio.CancelledError(),
            ]
        )
        strategy.subscriber = mock_subscriber
        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()
        assert strategy._last_data_ts == pytest.approx(888.0)


class TestEmitSignalTimestamp:
    """Test suite for emit signal timestamp behavior."""

    @pytest.mark.asyncio
    async def test_emit_signal_uses_replay_ts_over_wall_time(self) -> None:
        """Verify emit_signal uses _last_data_ts for timestamp.

        Given: Strategy with _last_data_ts set,
        When: emit_signal called without timestamp,
        Then: StrategySignal timestamp uses _last_data_ts.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._last_data_ts = 1700000000.0
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            price=50000.0,
            reason="test",
            timestamp=None,
        )
        await strategy.emit_signal(signal)
        assert signal.timestamp == datetime.fromtimestamp(1700000000.0, tz=UTC)

    @pytest.mark.asyncio
    async def test_emit_signal_uses_wall_time_when_no_replay_ts(self) -> None:
        """Verify emit_signal uses wall time when no replay ts.

        Given: Strategy with _last_data_ts=None,
        When: emit_signal called,
        Then: StrategySignal timestamp uses current time.
        """
        config = StrategyConfig(
            name="test",
            strategy_class="SimpleTestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        strategy = SimpleTestStrategy(config)
        strategy._last_data_ts = None
        mock_msg_publisher = MagicMock()
        mock_msg_publisher.send = AsyncMock()
        mock_msg_publisher.tracker = SequenceTracker()
        mock_msg_publisher.session_id = mock_msg_publisher.tracker.session_id
        strategy.msg_publisher = mock_msg_publisher
        signal = StrategySignal(
            instrument="BTC-USD",
            side="buy",
            strength=0.9,
            price=50000.0,
            reason="test",
            timestamp=None,
        )
        before = datetime.now(UTC)
        await strategy.emit_signal(signal)
        after = datetime.now(UTC)
        assert signal.timestamp is not None
        assert before <= signal.timestamp <= after


class TestCompositeReset:
    """Test suite for CompositeStrategy reset functionality."""

    @pytest.mark.asyncio
    async def test_composite_reset_calls_sub_strategy_reset(self) -> None:
        """Verify reset propagates to sub-strategies."""

        class TestComposite(CompositeStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

        config = StrategyConfig(
            name="composite",
            strategy_class="TestComposite",
            inputs=["signals.paper.BTC-USD.macd"],
            outputs=["BTC-USD"],
        )
        composite = TestComposite(config)
        sub_config = StrategyConfig(
            name="macd",
            strategy_class="SimpleTestStrategy",
            inputs=["market.paper.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
        )
        sub = SimpleTestStrategy(sub_config)
        sub.candles_processed = [("BTC-USD", make_candle_envelope("BTC-USD", 100.0))]
        await composite.add_sub_strategy(sub)
        await composite.reset()
        assert sub.candles_processed == []


class TestTopicValidationPhase4:
    """Test suite for topic validation edge cases."""

    def test_validate_system_heartbeats_dot_ending_non_heartbeats(self) -> None:
        """Verify system topic with trailing dot but not heartbeat fails.

        Given: system.other. topic,
        When: _validate_system_topic called,
        Then: Validation fails.
        """
        valid, err = _validate_system_topic("system.other.")
        assert not valid

    def test_validate_system_heartbeats_format_invalid(self) -> None:
        """Verify unknown system topic format fails.

        Given: system.unknown. topic,
        When: _validate_system_topic called,
        Then: Validation fails.
        """
        valid, err = _validate_system_topic("system.unknown.")
        assert not valid

    def test_validate_system_symbol_aliases_extra_segments(self) -> None:
        """Verify symbol_aliases with extra segments fails.

        Given: system.symbol_aliases.extra topic,
        When: _validate_system_topic called,
        Then: Error mentions 'must have exactly 2 segments'.
        """
        valid, err = _validate_system_topic("system.symbol_aliases.extra")
        assert not valid
        assert "must have exactly 2 segments" in err

    def test_validate_orders_commands_topic_invalid_command(self) -> None:
        """Verify invalid order command fails validation.

        Given: orders.commands topic with invalid command type,
        When: _validate_orders_commands_topic called,
        Then: Error mentions 'Invalid order command'.
        """
        valid, err = _validate_orders_commands_topic("orders.commands.kraken.BTC-USD.invalid")
        assert not valid
        assert "Invalid order command" in err

    def test_validate_admin_topic_empty_resource(self) -> None:
        """Verify admin topic with empty resource fails.

        Given: admin. topic,
        When: _validate_admin_topic called,
        Then: Error mentions '2 segments'.
        """
        valid, err = _validate_admin_topic("admin.")
        assert not valid
        assert "2 segments" in err.lower()

    def test_validate_admin_topic_wrong_category(self) -> None:
        """Verify non-admin topic fails admin validation.

        Given: system.users topic,
        When: _validate_admin_topic called,
        Then: Error mentions 'admin'.
        """
        valid, err = _validate_admin_topic("system.users")
        assert not valid
        assert "admin" in err.lower()

    def test_validate_signal_strategy_id_empty(self) -> None:
        """Verify signal topic with empty strategy ID fails.

        Given: signals.paper.BTC-USD. topic,
        When: _validate_signal_topic called,
        Then: Validation fails.
        """
        valid, err = _validate_signal_topic("signals.paper.BTC-USD.")
        assert not valid

    def test_market_topic_candles_missing_timeframe(self) -> None:
        """Verify candles topic without timeframe fails.

        Given: market.kraken.BTC-USD.candles topic,
        When: validate_topic called,
        Then: Error mentions 'timeframe'.
        """
        valid, err = validate_topic("market.kraken.BTC-USD.candles")
        assert not valid
        assert "timeframe" in err.lower()

    def test_validate_system_heartbeats_invalid_component(self) -> None:
        """Verify invalid heartbeat component type fails.

        Given: system.heartbeats.invalid.kraken topic,
        When: _validate_system_topic called,
        Then: Error mentions 'Invalid heartbeat component type'.
        """
        valid, err = _validate_system_topic("system.heartbeats.invalid.kraken")
        assert not valid
        assert "Invalid heartbeat component type" in err

    def test_validate_system_heartbeats_executor_wrong_segments(self) -> None:
        """Verify executor heartbeat with wrong segments fails.

        Given: system.heartbeats.executor topic,
        When: _validate_system_topic called,
        Then: Validation fails.
        """
        valid, err = _validate_system_topic("system.heartbeats.executor")
        assert not valid

    def test_validate_system_heartbeats_feed_wrong_segments(self) -> None:
        """Verify feed heartbeat without exchange fails.

        Given: system.heartbeats.feed topic,
        When: _validate_system_topic called,
        Then: Error mentions segment requirements.
        """
        valid, err = _validate_system_topic("system.heartbeats.feed")
        assert not valid
        assert "4 seg" in err


class TestExecutorBasePhase4:
    """Test suite for executor base class edge cases."""

    def test_get_default_parameters_advertises_wallet_public_id(self) -> None:
        """``get_default_parameters`` advertises wallet param.

        Given: ExchangeExecutorService,
        When: ``get_default_parameters`` called,
        Then: Returns ``{"wallet_public_id": ""}`` so the process
            launcher knows the parameter exists for per-wallet
            spawning.
        """
        mock_settings = MagicMock()
        result = ExchangeExecutorService.get_default_parameters(mock_settings)
        assert result == {"wallet_public_id": ""}

    @pytest.mark.asyncio
    async def test_publish_heartbeat_error_handling(self) -> None:
        """Verify _publish_heartbeat handles connection errors.

        Given: KrakenOrderExecutor with failing publisher,
        When: _publish_heartbeat called,
        Then: No exception raised.
        """
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7601"
        mock_settings.zmq_heartbeat_interval_ms = 1000
        mock_settings.paper_initial_cash_usd = 10000.0
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        with patch("snapper.config.settings.get_settings", return_value=mock_settings):
            executor = KrakenOrderExecutor()
            executor.running = True
            mock_msg_publisher = MagicMock()
            mock_msg_publisher.send = AsyncMock(side_effect=Exception("Connection failed"))
            executor.msg_publisher = mock_msg_publisher
            heartbeat = HeartbeatData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime.now(UTC),
                component="test.executor",
                sequence=1,
                status="healthy",
                lag_ms=0,
            )
            await executor._publish_heartbeat("system.heartbeats.test.executor", heartbeat)


class TestCliAppPhase4:
    """Test suite for CLI app edge cases."""

    def test_alembic_cfg_not_found_raises(self) -> None:
        """Verify missing alembic.ini raises RuntimeError.

        Given: Path.exists returns False,
        When: _alembic_cfg called,
        Then: RuntimeError raised with 'alembic.ini not found'.
        """
        with (
            patch.object(Path, "exists", return_value=False),
            pytest.raises(RuntimeError, match="alembic.ini not found"),
        ):
            _alembic_cfg("sqlite:///:memory:")


class TestBrokerProxyLoop:
    """Test suite for broker proxy loop."""

    def test_proxy_loop_no_sockets_returns_early(self) -> None:
        """Verify _proxy_loop returns early with no sockets.

        Given: ZmqBrokerThread with no sockets,
        When: _proxy_loop called,
        Then: Loop exits without polling.
        """
        broker = ZmqBrokerThread.__new__(ZmqBrokerThread)
        broker.xsub_socket = None
        broker.xpub_socket = None
        broker._stop_event = MagicMock()
        broker._proxy_loop()
        broker._stop_event.is_set.assert_not_called()

    def test_proxy_loop_forwards_from_xsub_to_xpub(self) -> None:
        """Verify proxy forwards XSUB to XPUB.

        Given: Message on XSUB socket,
        When: _proxy_loop polls,
        Then: Message forwarded to XPUB.
        """
        broker = ZmqBrokerThread.__new__(ZmqBrokerThread)
        broker._stop_event = MagicMock()
        broker._stop_event.is_set.side_effect = [False, True]
        mock_xsub = MagicMock()
        mock_xpub = MagicMock()
        broker.xsub_socket = mock_xsub
        broker.xpub_socket = mock_xpub
        mock_poller = MagicMock()
        mock_poller.poll.return_value = [(mock_xsub, zmq.POLLIN)]
        mock_xsub.recv_multipart.return_value = [b"topic", b"data"]
        with patch("zmq.Poller", return_value=mock_poller):
            broker._proxy_loop()
        mock_xsub.recv_multipart.assert_called_once_with(zmq.NOBLOCK)
        mock_xpub.send_multipart.assert_called_once_with([b"topic", b"data"])

    def test_proxy_loop_forwards_from_xpub_to_xsub(self) -> None:
        """Verify proxy forwards XPUB subscriptions to XSUB.

        Given: Subscription on XPUB socket,
        When: _proxy_loop polls,
        Then: Subscription forwarded to XSUB.
        """
        broker = ZmqBrokerThread.__new__(ZmqBrokerThread)
        broker._stop_event = MagicMock()
        broker._stop_event.is_set.side_effect = [False, True]
        mock_xsub = MagicMock()
        mock_xpub = MagicMock()
        broker.xsub_socket = mock_xsub
        broker.xpub_socket = mock_xpub
        mock_poller = MagicMock()
        mock_poller.poll.return_value = [(mock_xpub, zmq.POLLIN)]
        mock_xpub.recv_multipart.return_value = [b"\x01subscription"]
        with patch("zmq.Poller", return_value=mock_poller):
            broker._proxy_loop()
        mock_xpub.recv_multipart.assert_called_once_with(zmq.NOBLOCK)
        mock_xsub.send_multipart.assert_called_once_with([b"\x01subscription"])

    def test_proxy_loop_handles_zmq_again(self) -> None:
        """Verify proxy handles zmq.Again exception.

        Given: recv_multipart raises zmq.Again,
        When: _proxy_loop polls,
        Then: Loop continues without error.
        """
        broker = ZmqBrokerThread.__new__(ZmqBrokerThread)
        broker._stop_event = MagicMock()
        broker._stop_event.is_set.side_effect = [False, False, True]
        mock_xsub = MagicMock()
        mock_xpub = MagicMock()
        broker.xsub_socket = mock_xsub
        broker.xpub_socket = mock_xpub
        mock_poller = MagicMock()
        mock_poller.poll.side_effect = [
            [(mock_xsub, zmq.POLLIN)],
            [],
        ]
        mock_xsub.recv_multipart.side_effect = zmq.Again()
        with patch("zmq.Poller", return_value=mock_poller):
            broker._proxy_loop()

    def test_proxy_loop_handles_context_terminated(self) -> None:
        """Verify proxy handles ContextTerminated exception.

        Given: poll raises ContextTerminated,
        When: _proxy_loop runs,
        Then: Loop exits cleanly.
        """
        broker = ZmqBrokerThread.__new__(ZmqBrokerThread)
        broker._stop_event = MagicMock()
        broker._stop_event.is_set.return_value = False
        mock_xsub = MagicMock()
        mock_xpub = MagicMock()
        broker.xsub_socket = mock_xsub
        broker.xpub_socket = mock_xpub
        mock_poller = MagicMock()
        mock_poller.poll.side_effect = zmq.ContextTerminated("Context terminated")
        with patch("zmq.Poller", return_value=mock_poller):
            broker._proxy_loop()

    def test_proxy_loop_logs_error_on_exception(self) -> None:
        """Verify proxy logs error on unexpected exception.

        Given: poll raises RuntimeError,
        When: _proxy_loop runs,
        Then: Error logged.
        """
        broker = ZmqBrokerThread.__new__(ZmqBrokerThread)
        broker._stop_event = MagicMock()
        broker._stop_event.is_set.return_value = False
        mock_xsub = MagicMock()
        mock_xpub = MagicMock()
        broker.xsub_socket = mock_xsub
        broker.xpub_socket = mock_xpub
        mock_poller = MagicMock()
        mock_poller.poll.side_effect = RuntimeError("Unexpected error")
        with (
            patch("zmq.Poller", return_value=mock_poller),
            patch("snapper.messaging.infrastructure.broker.logger") as mock_logger,
        ):
            broker._proxy_loop()
            mock_logger.error.assert_called()


class TestBaseStrategyValidation:
    """Test suite for BaseStrategy output validation."""

    def test_validate_outputs_non_tradeable_rejects(self) -> None:
        """Verify non-tradeable instruments are rejected (fail-fast).

        Given: StrategyConfig where is_tradeable returns False for outputs,
        When: _validate_output_instruments called,
        Then: ValueError raised with 'not tradeable' message.
        """
        with patch(
            "snapper.strategies.models.is_tradeable",
            return_value=False,
        ):
            config = object.__new__(StrategyConfig)
            config.name = "test"
            config.strategy_class = "Test"
            config.inputs = ["market.kraken.UNKNOWN-PAIR.candles.1h"]
            config.outputs = ["UNKNOWN-PAIR"]
            config.exchange = "kraken"
            config.params = {}
            with pytest.raises(ValueError, match="not tradeable on kraken"):
                config._validate_output_instruments()

    def test_validate_outputs_paper_accepts_known_symbols(self) -> None:
        """Verify paper exchange accepts symbols that have aliases.

        Given: StrategyConfig with exchange='paper' and known symbol,
        When: _validate_output_instruments called,
        Then: No error (symbol exists in forward maps).
        """
        config = object.__new__(StrategyConfig)
        config.name = "test"
        config.strategy_class = "Test"
        config.inputs = ["market.paper.kraken.BTC-USD.candles.1h"]
        config.outputs = ["BTC-USD"]
        config.exchange = "paper"
        config.params = {}
        config._validate_output_instruments()

    def test_validate_outputs_paper_rejects_unknown_symbols(self) -> None:
        """Verify paper exchange rejects symbols without any aliases.

        Given: StrategyConfig with exchange='paper' and unknown symbol,
        When: _validate_output_instruments called,
        Then: ValueError raised.
        """
        config = object.__new__(StrategyConfig)
        config.name = "test"
        config.strategy_class = "Test"
        config.inputs = ["market.paper.kraken.ANYTHING-USD.candles.1h"]
        config.outputs = ["ANYTHING-USD"]
        config.exchange = "paper"
        config.params = {}
        with pytest.raises(ValueError, match="not tradeable on paper"):
            config._validate_output_instruments()

    def test_validate_outputs_all_tradeable_passes(self) -> None:
        """Verify all-tradeable outputs pass without error.

        Given: StrategyConfig where all outputs are tradeable,
        When: _validate_output_instruments called,
        Then: No error raised.
        """
        with patch(
            "snapper.strategies.models.is_tradeable",
            return_value=True,
        ):
            config = object.__new__(StrategyConfig)
            config.name = "test"
            config.strategy_class = "Test"
            config.inputs = ["market.kraken.BTC-USD.candles.1h"]
            config.outputs = ["BTC-USD"]
            config.exchange = "kraken"
            config.params = {}
            config._validate_output_instruments()


class TestBaseStrategyFeedHeartbeat:
    """Test suite for feed heartbeat tracking."""

    def test_track_feed_heartbeat(self) -> None:
        """Verify feed heartbeat tracking stores data.

        Given: Strategy with empty _feed_heartbeats,
        When: Heartbeat data added,
        Then: _feed_heartbeats contains status and symbol_count.
        """

        class TestStrategy(BaseStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

            async def reset(self) -> None:
                """No-op reset for test strategy."""
                pass

        strategy = object.__new__(TestStrategy)
        strategy.name = "test"
        strategy._feed_heartbeats = {}
        exchange = "kraken"
        message: dict[str, Any] = {
            "status": "healthy",
            "lag_ms": 50,
            "component": "feed_kraken",
            "details": {"symbol_count": 100},
        }
        details = message.get("details", {})
        strategy._feed_heartbeats[exchange] = {
            "timestamp": time.time(),
            "status": message.get("status", "unknown"),
            "lag_ms": message.get("lag_ms", 0),
            "component": message.get("component", "unknown"),
            "symbol_count": details.get("symbol_count", 0) if isinstance(details, dict) else 0,
        }
        assert "kraken" in strategy._feed_heartbeats
        assert strategy._feed_heartbeats["kraken"]["status"] == "healthy"
        assert strategy._feed_heartbeats["kraken"]["symbol_count"] == 100


class TestBaseStrategyEmitSignal:
    """Test suite for emit signal validation."""

    @pytest.mark.asyncio
    async def test_emit_signal_invalid_topic_raises(self) -> None:
        """Verify emit_signal raises for non-output instrument.

        Given: Strategy with BTC-USD output,
        When: emit_signal called with ETH-USD instrument,
        Then: ValueError raised with 'not allowed'.
        """

        class TestStrategy(BaseStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

            async def reset(self) -> None:
                """No-op reset for test strategy."""
                pass

        strategy = object.__new__(TestStrategy)
        strategy.name = "test"
        strategy.exchange = "paper"
        strategy.outputs = ["BTC-USD"]
        strategy.output_topics = ["signals.paper.BTC-USD.test"]
        strategy.msg_publisher = None
        strategy._last_data_ts = None
        signal = StrategySignal(
            instrument="ETH-USD",
            side="buy",
            strength=0.8,
            price=1800.0,
            reason="test",
        )
        with pytest.raises(ValueError, match="not allowed"):
            await strategy.emit_signal(signal)


class TestCompositeStrategyAddSubStrategy:
    """Test suite for adding sub-strategies."""

    @pytest.mark.asyncio
    async def test_add_sub_strategy_no_matching_topics_raises(self) -> None:
        """Verify add_sub_strategy raises for non-matching topics.

        Given: CompositeStrategy with BTC-USD input,
        When: add_sub_strategy with ETH-USD output,
        Then: ValueError raised with 'None of sub-strategy output topics'.
        """

        class TestCompositeStrategy(CompositeStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

        class TestSubStrategy(BaseStrategy):
            async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
                return None

            async def reset(self) -> None:
                """No-op reset for test strategy."""
                pass

        composite = object.__new__(TestCompositeStrategy)
        composite.name = "composite"
        composite.inputs = ["signals.paper.BTC-USD.test"]
        composite.sub_strategies = []
        sub_strategy = object.__new__(TestSubStrategy)
        sub_strategy.name = "other"
        sub_strategy.output_topics = ["signals.paper.ETH-USD.other"]
        with pytest.raises(ValueError, match="None of sub-strategy output topics"):
            await composite.add_sub_strategy(sub_strategy)


@pytest.fixture
def coint_strategy_config() -> StrategyConfig:
    """Provide a cointegration pairs strategy configuration."""
    return StrategyConfig(
        name="cointegration_btc_eth",
        strategy_class="CointegrationPairs",
        inputs=[
            "market.paper.kraken.BTC-USD.candles.1h",
            "market.paper.kraken.ETH-USD.candles.1h",
        ],
        outputs=["BTC-USD", "ETH-USD"],
        exchange="paper",
        params={
            "beta": 0.05,
            "entry_threshold": 2.0,
            "exit_threshold": 0.5,
            "lookback_window": 50,
            "min_data_points": 30,
        },
    )


@pytest.fixture
def strategy(coint_strategy_config: StrategyConfig) -> CointegrationPairs:
    """Provide a CointegrationPairs strategy instance for testing."""
    return CointegrationPairs(config=coint_strategy_config)


class TestCointegrationInitialization:
    """Test suite for CointegrationPairs initialization."""

    def test_initialization_with_valid_config(self, strategy: CointegrationPairs) -> None:
        """Verify CointegrationPairs initializes with valid config.

        Given: Valid StrategyConfig with 2 inputs,
        When: CointegrationPairs instantiated,
        Then: All parameters correctly assigned.
        """
        assert strategy.name == "cointegration_btc_eth"
        assert strategy.beta == pytest.approx(0.05)
        assert strategy.entry_threshold == pytest.approx(2.0)
        assert strategy.exit_threshold == pytest.approx(0.5)
        assert strategy.lookback_window == 50
        assert strategy.min_data_points == 30
        assert strategy.instrument1 == "BTC-USD"
        assert strategy.instrument2 == "ETH-USD"

    def test_required_candle_history_is_lookback_window(self, strategy: CointegrationPairs) -> None:
        """required_candle_history reports the lookback window for warmup prefill.

        Given: a cointegration strategy with lookback_window 50,
        When: required_candle_history() is queried,
        Then: it returns 50 so the warmup prefetches a full spread window.
        """
        assert strategy.required_candle_history() == 50
        assert strategy._position is None
        assert len(strategy.candle_buffer) == 0

    def test_initialization_with_single_non_synthetic_input_raises_error(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify single non-synthetic input raises ValueError.

        Given: Config with one regular ZMQ topic input (live process
            shape requires exactly two leg topics),
        When: CointegrationPairs instantiated,
        Then: ValueError naming both supported invocation shapes.
        """
        strategy_config.inputs = ["market.paper.kraken.BTC-USD.candles.1h"]
        with pytest.raises(ValueError, match="2 inputs.*1 synthetic"):
            CointegrationPairs(config=strategy_config)

    def test_initialization_with_three_inputs_raises_error(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify three inputs raises ValueError.

        Given: Config with three inputs (neither shape accepts more than
            two),
        When: CointegrationPairs instantiated,
        Then: ValueError naming both supported invocation shapes.
        """
        strategy_config.inputs = [
            "market.paper.kraken.BTC-USD.candles.1h",
            "market.paper.kraken.ETH-USD.candles.1h",
            "market.paper.kraken.SOL-USD.candles.1h",
        ]
        with pytest.raises(ValueError, match="2 inputs.*1 synthetic"):
            CointegrationPairs(config=strategy_config)

    def test_initialization_direct_db_synthetic_input(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify direct-DB backtest invocation shape is accepted.

        Given: Config with a single synthetic input + two output
            instruments — the shape ``DirectDbEngine`` produces,
        When: CointegrationPairs instantiated,
        Then: instrument1 and instrument2 are read from outputs,
            no ValueError.
        """
        strategy_config.inputs = ["candles.kraken.synthetic.1d"]
        strategy_config.outputs = ["FET-USD", "RENDER-USD"]
        strategy = CointegrationPairs(config=strategy_config)
        assert strategy.instrument1 == "FET-USD"
        assert strategy.instrument2 == "RENDER-USD"

    def test_initialization_synthetic_input_requires_two_outputs(
        self, strategy_config: StrategyConfig
    ) -> None:
        """Verify direct-DB shape with wrong outputs count raises.

        Given: Synthetic input but outputs has only one instrument,
        When: CointegrationPairs instantiated,
        Then: ValueError naming both supported invocation shapes.
        """
        strategy_config.inputs = ["candles.kraken.synthetic.1d"]
        strategy_config.outputs = ["FET-USD"]
        with pytest.raises(ValueError, match="2 inputs.*1 synthetic"):
            CointegrationPairs(config=strategy_config)

    @pytest.mark.asyncio
    async def test_entry_returns_both_legs_as_a_group(
        self, coint_strategy_config: StrategyConfig
    ) -> None:
        """Verify spread entry returns BOTH legs as a single ordered group.

        Given: A CointegrationPairs strategy with two legs buffered to
            min_data_points + lookback_window with prices that diverge
            enough on the last candle to push the z-score past the
            entry threshold,
        When: ``on_candle`` is called for instrument1,
        Then: it returns ``[primary, hedge]`` — the primary leg for
            instrument1 followed by the hedge leg for instrument2 with the
            opposite side — and there is no side-channel queue.
        """
        coint_strategy_config.params = {
            "beta": 1.0,
            "entry_threshold": 1.5,
            "exit_threshold": 0.5,
            "lookback_window": 30,
            "min_data_points": 30,
        }
        strategy = CointegrationPairs(config=coint_strategy_config)
        for _ in range(40):
            await feed_bar_to_strategy(strategy, "BTC-USD", 100.0)
            await feed_bar_to_strategy(strategy, "ETH-USD", 100.0)

        await feed_bar_to_strategy(strategy, "ETH-USD", 100.0)
        group = await feed_bar_returning_group(strategy, "BTC-USD", 130.0)

        assert isinstance(group, list)
        assert [sig.instrument for sig in group] == ["BTC-USD", "ETH-USD"]
        assert group[0].side == TradeSideEnum.SELL
        assert group[1].side == TradeSideEnum.BUY
        assert not hasattr(strategy, "_pending_signals")

    def test_extract_instrument_from_topic(self) -> None:
        """Verify _extract_instrument parses topic correctly.

        Given: Market topic string,
        When: _extract_instrument called,
        Then: Instrument symbol extracted.
        """
        assert (
            CointegrationPairs._extract_instrument("market.paper.kraken.BTC-USD.candles.1h")
            == "BTC-USD"
        )
        assert (
            CointegrationPairs._extract_instrument("market.kraken.ETH-USD.candles.5m") == "ETH-USD"
        )
        assert CointegrationPairs._extract_instrument("BTC-USD") == "BTC-USD"


class TestCointegrationDataProcessing:
    """Test suite for cointegration data processing."""

    @pytest.mark.asyncio
    async def test_on_candle_with_unknown_instrument(self, strategy: CointegrationPairs) -> None:
        """Verify unknown instrument returns None.

        Given: Strategy configured for BTC-USD and ETH-USD,
        When: on_candle called with UNKNOWN-USD,
        Then: StrategySignal is None.
        """
        signal = await feed_bar_to_strategy(strategy, "UNKNOWN-USD", 50000.0)
        assert signal is None

    @pytest.mark.asyncio
    async def test_on_candle_with_empty_buffer(self, strategy: CointegrationPairs) -> None:
        """Verify empty buffer returns None.

        Given: Strategy with no candle history,
        When: on_candle called,
        Then: StrategySignal is None.
        """
        signal = await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0)
        assert signal is None

    @pytest.mark.asyncio
    async def test_on_candle_with_empty_candle_buffer_list(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify empty candle_buffer list returns None.

        Given: Strategy with empty list in candle_buffer,
        When: on_candle called,
        Then: StrategySignal is None and list still empty.
        """
        strategy.candle_buffer["BTC-USD"] = []
        candle = make_candle_envelope("BTC-USD", 50000.0)
        signal = await strategy.on_candle("BTC-USD", candle)
        assert signal is None
        assert strategy.candle_buffer["BTC-USD"] == []

    @pytest.mark.asyncio
    async def test_on_candle_builds_candle_buffer(self, strategy: CointegrationPairs) -> None:
        """Verify on_candle populates candle_buffer.

        Given: Strategy receiving bars,
        When: on_candle called,
        Then: candle_buffer contains bars.
        """
        await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0)
        assert "BTC-USD" in strategy.candle_buffer
        assert len(strategy.candle_buffer["BTC-USD"]) == 1
        assert strategy.candle_buffer["BTC-USD"][0].close == pytest.approx(50000.0)

    @pytest.mark.asyncio
    async def test_on_candle_returns_none_with_single_instrument_data(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify single instrument data returns None.

        Given: Strategy with only BTC-USD data,
        When: on_candle called,
        Then: StrategySignal is None (needs both instruments).
        """
        closes = [50000.0 + i for i in range(35)]
        signal = await feed_closes_to_strategy(strategy, "BTC-USD", closes)
        assert signal is None

    @pytest.mark.asyncio
    async def test_on_candle_returns_none_with_insufficient_data_points(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify insufficient data points returns None.

        Given: Strategy with less than min_data_points,
        When: on_candle called,
        Then: StrategySignal is None.
        """
        for i in range(20):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i * 0.05)
        assert len(strategy.candle_buffer["BTC-USD"]) == 20
        assert len(strategy.candle_buffer["ETH-USD"]) == 20
        signal = await feed_bar_to_strategy(strategy, "BTC-USD", 50020.0)
        assert signal is None

    @pytest.mark.asyncio
    async def test_on_candle_respects_buffer_size(self, strategy: CointegrationPairs) -> None:
        """Verify candle_buffer respects buffer_size limit.

        Given: Strategy with buffer_size=50,
        When: 60 bars fed,
        Then: Buffer contains only last 50 bars.
        """
        strategy.params["buffer_size"] = 50
        closes = [50000.0 + i for i in range(60)]
        await feed_closes_to_strategy(strategy, "BTC-USD", closes)
        assert len(strategy.candle_buffer["BTC-USD"]) == 50
        assert strategy.candle_buffer["BTC-USD"][0].close == pytest.approx(50010.0)


class TestCointegrationSignalGeneration:
    """Test suite for cointegration signal generation."""

    @pytest.mark.asyncio
    async def test_entry_signal_short_spread_high_zscore(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify high z-score triggers short spread.

        Given: Sufficient price history,
        When: BTC rises above mean,
        Then: StrategySignal to sell BTC (short spread).
        """
        for i in range(35):
            btc_price = 50000.0 + i * 100
            eth_price = 3000.0 + i * 1
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        btc_high = 60000.0
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        signal_btc = await feed_bar_to_strategy(strategy, "BTC-USD", btc_high)
        assert signal_btc is not None
        assert signal_btc.instrument == "BTC-USD"
        assert signal_btc.side == "sell"
        assert signal_btc.strength > 0
        assert "short spread" in signal_btc.reason.lower()
        assert strategy._position == "short_spread"

    @pytest.mark.asyncio
    async def test_entry_signal_long_spread_low_zscore(self, strategy: CointegrationPairs) -> None:
        """Verify low z-score triggers long spread.

        Given: Sufficient price history,
        When: BTC drops below mean,
        Then: StrategySignal to buy BTC (long spread).
        """
        for i in range(35):
            btc_price = 50000.0 - i * 100
            eth_price = 3000.0 + i * 5
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        btc_low = 45000.0
        await feed_bar_to_strategy(strategy, "ETH-USD", 3175.0)
        signal_btc = await feed_bar_to_strategy(strategy, "BTC-USD", btc_low)
        assert signal_btc is not None
        assert signal_btc.instrument == "BTC-USD"
        assert signal_btc.side == "buy"
        assert signal_btc.strength > 0
        assert "long spread" in signal_btc.reason.lower()
        assert strategy._position == "long_spread"

    def test_entry_skipped_when_partner_price_missing(self, strategy: CointegrationPairs) -> None:
        """No naked leg: a triggered entry with no partner price stays flat.

        Given: a flat strategy whose partner (instrument2) candle buffer
            is empty,
        When: ``_generate_signal_from_zscore`` resolves an entry decision
            (z-score well past the threshold),
        Then: it returns ``None`` (no signal group built) and leaves
            ``_position`` unchanged, so a single naked leg is never emitted
            or internally entered.
        """
        strategy.candle_buffer["ETH-USD"] = []
        result = strategy._generate_signal_from_zscore(100.0, "BTC-USD", 100.0)
        assert result is None
        assert strategy._position is None

    @pytest.mark.asyncio
    async def test_entry_signal_instrument2_hedge(self, strategy: CointegrationPairs) -> None:
        """Verify ETH hedge signal after BTC entry.

        Given: Short spread position opened on BTC,
        When: ETH candle received,
        Then: Hedge signal for ETH with beta-adjusted strength.
        """
        for i in range(35):
            btc_price = 50000.0 + i * 100
            eth_price = 3000.0 + i * 1
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        group = await feed_bar_returning_group(strategy, "BTC-USD", 60000.0)
        assert strategy._position == "short_spread"
        assert isinstance(group, list)
        eth_leg = next(sig for sig in group if sig.instrument == "ETH-USD")
        assert eth_leg.side == "buy"
        assert 0 < eth_leg.strength < 0.1
        assert "hedge" in eth_leg.reason.lower()

    @pytest.mark.asyncio
    async def test_exit_signal_short_spread_reversion(self, strategy: CointegrationPairs) -> None:
        """Verify exit signal on spread reversion.

        Given: Short spread position open,
        When: Z-score reverts toward zero,
        Then: Exit signal with zero strength.
        """
        for i in range(35):
            btc_price = 50000.0 + i * 100
            eth_price = 3000.0 + i * 1
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        signal = await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        assert strategy._position == "short_spread"
        for i in range(15):
            btc_price = 60000.0 - i * 300
            eth_price = 3035.0 + i * 5
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            signal = await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
            if signal and signal.strength == pytest.approx(0.0):
                assert "exit" in signal.reason.lower()
                break

    @pytest.mark.asyncio
    async def test_exit_signal_short_spread_instrument1(self, strategy: CointegrationPairs) -> None:
        """Verify BTC exit signal on short spread.

        Given: Short spread position open,
        When: BTC price drops significantly,
        Then: Exit signal on BTC to close position.
        """
        for i in range(35):
            btc_price = 50000.0 + i * 100
            eth_price = 3000.0 + i * 1
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        assert strategy._position == "short_spread"
        for i in range(10):
            await feed_bar_to_strategy(strategy, "ETH-USD", 3036.0 + i * 10)
        for i in range(20):
            btc_price = 55000.0 - i * 500
            signal = await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            if (
                signal
                and signal.strength == pytest.approx(0.0)
                and "Exit short spread" in signal.reason
            ):
                assert signal.instrument == "BTC-USD"
                assert signal.side == "buy"
                break
        assert strategy._position is None

    @pytest.mark.asyncio
    async def test_exit_signal_long_spread_reversion(self, strategy: CointegrationPairs) -> None:
        """Verify exit signal on long spread reversion.

        Given: Long spread position open,
        When: Z-score reverts toward zero,
        Then: Exit signal with zero strength.
        """
        for i in range(35):
            btc_price = 50000.0 - i * 100
            eth_price = 3000.0 + i * 5
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3175.0)
        await feed_bar_to_strategy(strategy, "BTC-USD", 45000.0)
        assert strategy._position == "long_spread"
        for i in range(10):
            btc_price = 45000.0 + i * 500
            eth_price = 3200.0 - i * 10
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            signal = await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
            if signal and signal.strength == pytest.approx(0.0):
                break
        assert strategy._position is None

    @pytest.mark.asyncio
    async def test_long_spread_no_exit_when_zscore_still_low(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify no exit when z-score still low.

        Given: Long spread position open,
        When: Z-score remains below threshold,
        Then: No signal, position stays open.
        """
        for i in range(35):
            btc_price = 50000.0 - i * 100
            eth_price = 3000.0 + i * 5
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3175.0)
        await feed_bar_to_strategy(strategy, "BTC-USD", 45000.0)
        assert strategy._position == "long_spread"
        await feed_bar_to_strategy(strategy, "BTC-USD", 44000.0)
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3300.0)
        assert signal is None
        assert strategy._position == "long_spread"

    @pytest.mark.asyncio
    async def test_no_signal_when_zscore_in_neutral_zone(
        self, strategy: CointegrationPairs
    ) -> None:
        """Verify no signal in neutral z-score zone.

        Given: Prices moving proportionally,
        When: Z-score stays near zero,
        Then: No entry signal generated.
        """
        for i in range(35):
            btc_price = 50000.0 + i * 10
            eth_price = 3000.0 + i * 0.5
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            signal = await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        assert signal is None
        assert strategy._position is None

    @pytest.mark.asyncio
    async def test_no_signal_when_spread_std_is_zero(self, strategy: CointegrationPairs) -> None:
        """Verify no signal when std is zero.

        Given: Constant prices (zero std),
        When: on_candle called,
        Then: No signal (cannot compute z-score).
        """
        for _ in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0)
            signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0)
        assert signal is None

    @pytest.mark.asyncio
    async def test_first_arriving_leg_none_completing_leg_emits(
        self, strategy: CointegrationPairs
    ) -> None:
        """The 1st-arriving bar of a period returns None; the completing bar emits.

        Given: 35 aligned BTC/ETH days,
        When: the BTC bar for the entry day arrives first, then the ETH bar,
        Then: BTC returns None (unpaired) and ETH completes the day and signals.
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        first = await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        assert first is None
        assert strategy._position is None
        second = await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        assert second is not None
        assert strategy._position == "short_spread"

    @pytest.mark.asyncio
    async def test_partner_ahead_blocks_stale_signal(self, strategy: CointegrationPairs) -> None:
        """A leg racing ahead blocks a stale signal so prices stay same-day.

        Given: aligned history, then BTC races two days ahead while ETH lags,
        When: the lagging ETH bar for an older day arrives,
        Then: no signal fires (BTC, not ETH's day, is the latest seen) and the
            position stays flat — the order can never price legs on different days.
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        await feed_bar_to_strategy(strategy, "BTC-USD", 99999.0)
        signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        assert signal is None
        assert strategy._position is None

    @pytest.mark.asyncio
    async def test_self_lagging_behind_partner_does_not_emit(
        self, strategy: CointegrationPairs
    ) -> None:
        """Symmetric guard: ETH ahead blocks a stale BTC bar from emitting.

        Given: aligned history, then ETH races two days ahead while BTC lags,
        When: the lagging BTC bar for an older day arrives,
        Then: no signal fires (ETH is the latest seen) and the position stays flat.
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3036.0)
        signal = await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        assert signal is None
        assert strategy._position is None

    @pytest.mark.asyncio
    async def test_revised_same_day_bar_does_not_double_act(
        self, strategy: CointegrationPairs
    ) -> None:
        """A revised bar for an already-signaled day cannot produce a 2nd same-day action.

        Given: a short_spread entered on the latest day,
        When: a revised (re-published) candle for that SAME open_at arrives — even
            one whose price would flip the z-score,
        Then: no second signal fires and the position is unchanged (one decision
            per day; guards the A3-warmup/live same-day collision).
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        assert strategy._position == "short_spread"
        signaled_open_at = strategy._last_signal_open_at
        assert signaled_open_at is not None
        revised = make_candle_envelope("ETH-USD", 999999.0, ts=signaled_open_at.timestamp())
        strategy.candle_buffer["ETH-USD"][-1] = revised
        result = await strategy.on_candle("ETH-USD", revised)
        assert result is None
        assert strategy._position == "short_spread"

    @pytest.mark.asyncio
    async def test_no_signal_on_warmup_day_republish(self, strategy: CointegrationPairs) -> None:
        """A live re-publish of a day at/under the warmup high-water mark does not signal.

        Given: aligned history and a warmup high-water mark at the latest buffered
            day (as the A3 prefill sets),
        When: a live bar re-publishes that warmup day with a price that would flip
            the z-score,
        Then: no signal fires (prefilled past days are context, not tradeable; this
            blocks a mixed warmup/live same-day entry) and the position stays flat.
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        warmup_day = strategy.candle_buffer["BTC-USD"][-1].open_at
        strategy._warmup_through_open_at = warmup_day
        revised = make_candle_envelope("BTC-USD", 999999.0, ts=warmup_day.timestamp())
        strategy.candle_buffer["BTC-USD"][-1] = revised
        result = await strategy.on_candle("BTC-USD", revised)
        assert result is None
        assert strategy._position is None


class TestCointegrationReset:
    """Test suite for cointegration strategy reset."""

    @pytest.mark.asyncio
    async def test_reset_clears_state(self, strategy: CointegrationPairs) -> None:
        """Verify reset clears strategy state.

        Given: Strategy with position and buffer data,
        When: reset called,
        Then: Position is None.
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        await feed_bar_to_strategy(strategy, "ETH-USD", 3035.0)
        await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        assert strategy._position is not None
        assert len(strategy.candle_buffer) > 0
        await strategy.reset()
        assert strategy._position is None


class TestCointegrationEdgeCases:
    """Test suite for cointegration edge cases."""

    @pytest.mark.asyncio
    async def test_signal_strength_capped_at_one(self, strategy: CointegrationPairs) -> None:
        """Verify signal strength capped at 1.0.

        Given: Extreme z-score conditions,
        When: StrategySignal generated,
        Then: Strength does not exceed 1.0.
        """
        for i in range(35):
            btc_price = 50000.0 + i * 1000
            eth_price = 3000.0 + i * 1
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
        signal = await feed_bar_to_strategy(strategy, "BTC-USD", 100000.0)
        if signal:
            assert signal.strength <= 1.0

    @pytest.mark.asyncio
    async def test_beta_adjustment_in_hedge_signals(self, strategy: CointegrationPairs) -> None:
        """Verify beta adjusts hedge signal strength.

        Given: BTC entry signal generated,
        When: ETH hedge signal follows,
        Then: ETH strength < BTC strength (beta < 1).
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        btc_signal = await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        eth_signal = await feed_bar_to_strategy(strategy, "ETH-USD", 3030.0)
        if btc_signal and eth_signal:
            assert eth_signal.strength < btc_signal.strength
            assert eth_signal.strength > 0

    @pytest.mark.asyncio
    async def test_exit_signal_has_zero_strength(self, strategy: CointegrationPairs) -> None:
        """Verify exit signals have zero strength.

        Given: Position open and reverting,
        When: Exit condition met,
        Then: StrategySignal strength is 0.0.
        """
        for i in range(35):
            await feed_bar_to_strategy(strategy, "BTC-USD", 50000.0 + i * 100)
            await feed_bar_to_strategy(strategy, "ETH-USD", 3000.0 + i)
        await feed_bar_to_strategy(strategy, "BTC-USD", 60000.0)
        for i in range(15):
            btc_price = 60000.0 - i * 300
            eth_price = 3030.0 + i * 5
            await feed_bar_to_strategy(strategy, "BTC-USD", btc_price)
            signal = await feed_bar_to_strategy(strategy, "ETH-USD", eth_price)
            if signal and "exit" in signal.reason.lower():
                assert signal.strength == pytest.approx(0.0)
                return


INSTRUMENT_BTC = "market.kraken.BTCUSD.candles"
INSTRUMENT_ETH = "market.kraken.ETHUSD.candles"


@pytest.mark.asyncio
async def test_macd_bullish_crossover_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify bullish crossover generates buy signal.

    Given: MACD histogram crosses above zero,
    When: Second candle after crossover,
    Then: Buy signal with positive strength.
    """
    config = StrategyConfig(
        name="test_macd_bull",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_BTC],
        outputs=["BTC-USD"],
        params={"fast": 12, "slow": 26, "signal_period": 9},
    )
    strategy = MACDCrossover(config)
    closes: list[float] = [100.0 + i for i in range(40)]
    call_state = {"count": 0}

    def fake_macd(
        series: pd.Series, fast: int, slow: int, signal_period: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        call_state["count"] += 1
        index = pd.RangeIndex(len(series))
        macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
        signal_series = macd_series.copy()
        hist_value = 0.3 if call_state["count"] > 1 else -0.3
        hist_series = pd.Series([hist_value] * len(series), index=index, dtype=float)
        return macd_series, signal_series, hist_series

    monkeypatch.setattr("snapper.strategies.macd.macd", fake_macd)
    prefill_candle_buffer(strategy, INSTRUMENT_BTC, closes)
    first_signal = await feed_bar_to_strategy(strategy, INSTRUMENT_BTC, 140.0)
    assert first_signal is None
    signal = await feed_bar_to_strategy(strategy, INSTRUMENT_BTC, 141.0)
    assert signal is not None
    assert signal.instrument == INSTRUMENT_BTC
    assert signal.side == "buy"
    assert 0.0 < signal.strength <= 1.0
    assert "MACD bull cross" in signal.reason


@pytest.mark.asyncio
async def test_macd_bearish_crossover(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify bearish crossover generates sell signal.

    Given: MACD histogram crosses below zero,
    When: Second candle after crossover,
    Then: Sell signal with positive strength.
    """
    config = StrategyConfig(
        name="test_macd_bear",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_BTC],
        outputs=["BTC-USD"],
        params={"fast": 12, "slow": 26, "signal_period": 9},
    )
    strategy = MACDCrossover(config)
    closes: list[float] = [200.0 - i for i in range(40)]
    call_state = {"count": 0}

    def fake_macd(
        series: pd.Series, fast: int, slow: int, signal_period: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        call_state["count"] += 1
        index = pd.RangeIndex(len(series))
        macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
        signal_series = macd_series.copy()
        hist_value = -0.3 if call_state["count"] > 1 else 0.3
        hist_series = pd.Series([hist_value] * len(series), index=index, dtype=float)
        return macd_series, signal_series, hist_series

    monkeypatch.setattr("snapper.strategies.macd.macd", fake_macd)
    prefill_candle_buffer(strategy, INSTRUMENT_BTC, closes)
    first_signal = await feed_bar_to_strategy(strategy, INSTRUMENT_BTC, 160.0)
    assert first_signal is None
    signal = await feed_bar_to_strategy(strategy, INSTRUMENT_BTC, 159.0)
    assert signal is not None
    assert signal.instrument == INSTRUMENT_BTC
    assert signal.side == "sell"
    assert 0.0 < signal.strength <= 1.0
    assert "MACD bear cross" in signal.reason


@pytest.mark.asyncio
async def test_macd_wrong_instrument_ignored() -> None:
    """Verify wrong instrument returns None.

    Given: MACD strategy for BTC-USD,
    When: ETH-USD bars fed,
    Then: StrategySignal is None.
    """
    config = StrategyConfig(
        name="test_macd_filter",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_BTC],
        outputs=["BTC-USD"],
        params={"fast": 12, "slow": 26, "signal_period": 9},
    )
    strategy = MACDCrossover(config)
    closes: list[float] = [100.0 + i for i in range(50)]
    signal = await feed_closes_to_strategy(strategy, INSTRUMENT_ETH, closes)
    assert signal is None


@pytest.mark.asyncio
async def test_macd_reason_contains_params(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify signal reason contains MACD params.

    Given: MACD strategy with custom params,
    When: StrategySignal generated,
    Then: Reason string includes fast, slow, signal values.
    """
    config = StrategyConfig(
        name="test_metadata",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_BTC],
        outputs=["BTC-USD"],
        params={"fast": 8, "slow": 21, "signal_period": 5},
    )
    strategy = MACDCrossover(config)
    closes: list[float] = [100.0 + i for i in range(40)]
    call_state = {"count": 0}

    def fake_macd(
        series: pd.Series, fast: int, slow: int, signal_period: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        call_state["count"] += 1
        index = pd.RangeIndex(len(series))
        macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
        signal_series = macd_series.copy()
        hist_value = 0.4 if call_state["count"] > 1 else -0.4
        hist_series = pd.Series([hist_value] * len(series), index=index, dtype=float)
        return macd_series, signal_series, hist_series

    monkeypatch.setattr("snapper.strategies.macd.macd", fake_macd)
    prefill_candle_buffer(strategy, INSTRUMENT_BTC, closes)
    initial = await feed_bar_to_strategy(strategy, INSTRUMENT_BTC, 140.0)
    assert initial is None
    signal = await feed_bar_to_strategy(strategy, INSTRUMENT_BTC, 141.0)
    assert signal is not None
    assert "fast=8" in signal.reason
    assert "slow=21" in signal.reason
    assert "signal=5" in signal.reason


@pytest.mark.asyncio
async def test_macd_reset_clears_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify reset clears MACD state.

    Given: MACD strategy with history,
    When: reset called,
    Then: Internal histogram state cleared.
    """
    config = StrategyConfig(
        name="test_macd_reset",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_BTC],
        outputs=["BTC-USD"],
        params={"fast": 12, "slow": 26, "signal_period": 9},
    )
    strategy = MACDCrossover(config)
    closes: list[float] = [100.0 + i for i in range(40)]

    def fake_macd(
        series: pd.Series, fast: int, slow: int, signal_period: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        index = pd.RangeIndex(len(series))
        macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
        signal_series = macd_series.copy()
        hist_series = pd.Series([0.5] * len(series), index=index, dtype=float)
        return macd_series, signal_series, hist_series

    monkeypatch.setattr("snapper.strategies.macd.macd", fake_macd)
    await feed_closes_to_strategy(strategy, INSTRUMENT_BTC, closes)
    assert len(strategy._last_hist) > 0
    await strategy.reset()
    assert len(strategy._last_hist) == 0


INSTRUMENT_BTC = "market.kraken.BTCUSD.candles"


def _stub_macd_no_crossover(
    series: pd.Series, fast: int, slow: int, signal_period: int
) -> tuple[pd.Series, pd.Series, pd.Series]:
    index = pd.RangeIndex(len(series))
    macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
    signal_series = macd_series.copy()
    hist_series = pd.Series([0.3] * len(series), index=index, dtype=float)
    return macd_series, signal_series, hist_series


@pytest.fixture
def macd_strategy() -> MACDCrossover:
    """Create MACD strategy for tests."""
    config = StrategyConfig(
        name="test_macd",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_BTC],
        outputs=["BTC-USD"],
        params={"fast": 12, "slow": 26, "signal_period": 9},
    )
    return MACDCrossover(config)


@pytest.mark.asyncio
async def test_macd_no_crossover_returns_none(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify no crossover returns None.

    Given: MACD histogram stable positive,
    When: Multiple bars processed,
    Then: No signal generated.
    """
    closes = [100.0 + i * 0.5 for i in range(50)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_no_crossover)
    first_signal = await feed_closes_to_strategy(macd_strategy, INSTRUMENT_BTC, closes)
    assert first_signal is None
    second_signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 125.5)
    assert second_signal is None


@pytest.mark.asyncio
async def test_macd_histogram_stays_negative_no_crossover(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify negative histogram without crossover.

    Given: MACD histogram stays negative,
    When: No sign change occurs,
    Then: No signal generated.
    """
    closes = [100.0 + i * 0.5 for i in range(50)]

    def _stub_negative_hist(
        series: pd.Series, fast: int, slow: int, signal_period: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        index = pd.RangeIndex(len(series))
        macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
        signal_series = macd_series.copy()
        hist_series = pd.Series([-0.2] * len(series), index=index, dtype=float)
        return macd_series, signal_series, hist_series

    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_negative_hist)
    await feed_closes_to_strategy(macd_strategy, INSTRUMENT_BTC, closes)
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 125.5)
    assert signal is None


INSTRUMENT_BTC = "market.kraken.BTCUSD.candles"
INSTRUMENT_ETH = "market.kraken.ETHUSD.candles"


def _stub_macd_sequence(
    values: list[float],
) -> Callable[[pd.Series, int, int, int], tuple[pd.Series, pd.Series, pd.Series]]:
    call_state = {"index": 0}

    def _fake_macd(
        series: pd.Series, fast: int, slow: int, signal_period: int
    ) -> tuple[pd.Series, pd.Series, pd.Series]:
        index = pd.RangeIndex(len(series))
        macd_series = pd.Series([0.0] * len(series), index=index, dtype=float)
        signal_series = macd_series.copy()
        idx = min(call_state["index"], len(values) - 1)
        hist_series = pd.Series([values[idx]] * len(series), index=index, dtype=float)
        call_state["index"] += 1
        return macd_series, signal_series, hist_series

    return _fake_macd


@pytest.mark.asyncio
async def test_macd_initialization(macd_strategy: MACDCrossover) -> None:
    """Verify MACD initialization sets parameters.

    Given: Strategy created with config,
    When: Strategy instantiated,
    Then: Parameters correctly assigned.
    """
    assert macd_strategy.fast == 12
    assert macd_strategy.slow == 26
    assert macd_strategy.signal_period == 9
    assert macd_strategy.name == "test_macd"


@pytest.mark.asyncio
async def test_macd_insufficient_data(macd_strategy: MACDCrossover) -> None:
    """Verify insufficient data returns None.

    Given: Only two bars,
    When: on_candle called,
    Then: StrategySignal is None.
    """
    closes = [100.0, 101.0]
    signal = await feed_closes_to_strategy(macd_strategy, INSTRUMENT_BTC, closes)
    assert signal is None


@pytest.mark.asyncio
async def test_macd_bullish_crossover(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify bullish crossover with fixture.

    Given: MACD histogram goes from negative to positive,
    When: Crossover detected,
    Then: Buy signal generated.
    """
    closes = [100.0 + i * 0.5 for i in range(50)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([-0.2, 0.3]))
    prefill_candle_buffer(macd_strategy, INSTRUMENT_BTC, closes)
    first = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 125.0)
    assert first is None
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 125.5)
    assert signal is not None
    assert signal.instrument == INSTRUMENT_BTC
    assert signal.side == "buy"
    assert 0.0 <= signal.strength <= 1.0


@pytest.mark.asyncio
async def test_macd_bearish_crossover_with_fixture(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify bearish crossover with fixture.

    Given: MACD histogram goes from positive to negative,
    When: Crossover detected,
    Then: Sell signal generated.
    """
    closes = [200.0 - i * 0.5 for i in range(50)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([0.2, -0.3]))
    prefill_candle_buffer(macd_strategy, INSTRUMENT_BTC, closes)
    first = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 175.0)
    assert first is None
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 175.5)
    assert signal is not None
    assert signal.instrument == INSTRUMENT_BTC
    assert signal.side == "sell"


@pytest.mark.asyncio
async def test_macd_neutral_market(macd_strategy: MACDCrossover) -> None:
    """Verify neutral market behavior.

    Given: Choppy sideways price data,
    When: Bars processed,
    Then: StrategySignal None or valid side.
    """
    closes = [100.0 + (i % 10 - 5) * 0.1 for i in range(50)]
    signal = await feed_closes_to_strategy(macd_strategy, INSTRUMENT_BTC, closes)
    assert signal is None or signal.side in ["buy", "sell"]


@pytest.mark.asyncio
async def test_macd_signal_emission(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify signal emission on crossover.

    Given: Trending price data,
    When: Crossover occurs,
    Then: StrategySignal has correct instrument.
    """
    closes = [100.0 + i * 2.0 for i in range(50)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([-0.5, 0.5]))
    prefill_candle_buffer(macd_strategy, INSTRUMENT_BTC, closes)
    _ = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 199.0)
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 200.0)
    assert signal is not None
    assert signal.instrument == INSTRUMENT_BTC


@pytest.mark.asyncio
async def test_macd_custom_parameters() -> None:
    """Verify custom MACD parameters.

    Given: Config with custom fast/slow/signal,
    When: Strategy created,
    Then: Parameters correctly set.
    """
    config = StrategyConfig(
        name="custom_macd",
        strategy_class="MACDCrossover",
        inputs=[INSTRUMENT_ETH],
        outputs=["ETH-USD"],
        params={
            "fast": 8,
            "slow": 21,
            "signal_period": 5,
        },
    )
    strategy = MACDCrossover(config)
    assert strategy.fast == 8
    assert strategy.slow == 21
    assert strategy.signal_period == 5


@pytest.mark.asyncio
async def test_macd_missing_closes(macd_strategy: MACDCrossover) -> None:
    """Verify missing closes returns None.

    Given: Empty candle buffer,
    When: Single candle,
    Then: StrategySignal is None.
    """
    candle = make_candle_envelope(INSTRUMENT_BTC, 100.0)
    signal = await macd_strategy.on_candle(INSTRUMENT_BTC, candle)
    assert signal is None


@pytest.mark.asyncio
async def test_macd_empty_closes(macd_strategy: MACDCrossover) -> None:
    """Verify empty closes returns None.

    Given: No prior data,
    When: Single candle,
    Then: StrategySignal is None.
    """
    candle = make_candle_envelope(INSTRUMENT_BTC, 100.0)
    signal = await macd_strategy.on_candle(INSTRUMENT_BTC, candle)
    assert signal is None


@pytest.mark.asyncio
async def test_macd_single_close(macd_strategy: MACDCrossover) -> None:
    """Verify single close returns None.

    Given: Only one candle,
    When: on_candle called,
    Then: StrategySignal is None.
    """
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 100.0)
    assert signal is None


@pytest.mark.asyncio
async def test_macd_volatile_data(macd_strategy: MACDCrossover) -> None:
    """Verify volatile data handling.

    Given: Highly volatile price series,
    When: Bars processed,
    Then: StrategySignal None or valid strength.
    """
    closes: list[float] = []
    price = 100.0
    for i in range(50):
        price *= 1.05 if i % 2 == 0 else 0.95
        closes.append(price)
    signal = await feed_closes_to_strategy(macd_strategy, INSTRUMENT_BTC, closes)
    assert signal is None or isinstance(signal.strength, float)


@pytest.mark.asyncio
async def test_macd_wrong_instrument(macd_strategy: MACDCrossover) -> None:
    """Verify wrong instrument returns None.

    Given: MACD for BTC,
    When: ETH candles fed,
    Then: StrategySignal is None.
    """
    closes = [100.0 + i for i in range(50)]
    signal = await feed_closes_to_strategy(macd_strategy, INSTRUMENT_ETH, closes)
    assert signal is None


@pytest.mark.asyncio
async def test_macd_reset_state(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify reset clears internal state.

    Given: Strategy with crossover signal,
    When: reset and re-feed,
    Then: No immediate signal.
    """
    closes = [100.0 + i for i in range(50)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([-0.4, 0.4]))
    prefill_candle_buffer(macd_strategy, INSTRUMENT_BTC, closes)
    _ = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 149.0)
    pre_reset_signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 150.0)
    assert pre_reset_signal is not None
    await macd_strategy.reset()
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([0.4]))
    post_reset_signal = await feed_closes_to_strategy(macd_strategy, INSTRUMENT_BTC, closes)
    assert post_reset_signal is None


@pytest.mark.asyncio
async def test_macd_actual_bullish_crossover(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify actual bullish crossover.

    Given: Downtrend followed by uptrend,
    When: Histogram crosses above zero,
    Then: Buy signal with metadata.
    """
    closes = [200.0 - i * 0.8 for i in range(30)] + [176.0 + i * 2.0 for i in range(30)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([-0.4, 0.5]))
    prefill_candle_buffer(macd_strategy, INSTRUMENT_BTC, closes)
    _ = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 235.0)
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 236.0)
    assert signal is not None
    assert signal.side == "buy"
    assert "MACD bull cross" in signal.reason
    assert f"fast={macd_strategy.fast}" in signal.reason
    assert f"slow={macd_strategy.slow}" in signal.reason


@pytest.mark.asyncio
async def test_macd_actual_bearish_crossover(
    macd_strategy: MACDCrossover, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify actual bearish crossover.

    Given: Uptrend followed by downtrend,
    When: Histogram crosses below zero,
    Then: Sell signal with metadata.
    """
    closes = [100.0 + i * 0.8 for i in range(30)] + [124.0 - i * 2.0 for i in range(30)]
    monkeypatch.setattr("snapper.strategies.macd.macd", _stub_macd_sequence([0.4, -0.5]))
    prefill_candle_buffer(macd_strategy, INSTRUMENT_BTC, closes)
    _ = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 65.0)
    signal = await feed_bar_to_strategy(macd_strategy, INSTRUMENT_BTC, 66.0)
    assert signal is not None
    assert signal.side == "sell"
    assert "MACD bear cross" in signal.reason


def test_paper_exchange_accepts_known_symbols() -> None:
    """Verify paper exchange accepts symbols that have aliases.

    Given: Paper exchange config with known instruments,
    When: StrategyConfig is created,
    Then: Config accepted (symbols exist in forward maps).
    """
    config = StrategyConfig(
        name="test_paper",
        strategy_class="TestStrategy",
        inputs=["market.paper.kraken.BTC-USD.candles"],
        outputs=["BTC-USD", "ETH-USD", "EUR-PLN"],
        exchange="paper",
    )
    assert config.outputs == ["BTC-USD", "ETH-USD", "EUR-PLN"]


def test_paper_exchange_rejects_unknown_symbols() -> None:
    """Verify paper exchange rejects symbols without any aliases.

    Given: Paper exchange config with unknown instrument,
    When: StrategyConfig is created,
    Then: ValueError raised.
    """
    with pytest.raises(ValueError, match="not tradeable on paper"):
        StrategyConfig(
            name="test_paper_unknown",
            strategy_class="TestStrategy",
            inputs=["market.paper.kraken.UNKNOWN-SYMBOL.candles"],
            outputs=["UNKNOWN-SYMBOL"],
            exchange="paper",
        )


def test_walutomat_rejects_non_fx_instruments() -> None:
    """Verify Walutomat rejects non-FX.

    Given: Walutomat exchange,
    When: Non-FX instrument (BTC-USD),
    Then: ValueError raised.
    """
    with pytest.raises(ValueError, match="not tradeable on walutomat"):
        StrategyConfig(
            name="test_walutomat_invalid",
            strategy_class="TestStrategy",
            inputs=["market.walutomat.BTC-USD.ticks"],
            outputs=["BTC-USD"],
            exchange="walutomat",
        )


def test_walutomat_accepts_fx_pairs() -> None:
    """Verify Walutomat accepts FX pairs.

    Given: Walutomat exchange,
    When: FX pair like EUR-PLN,
    Then: Config accepted.
    """
    config = StrategyConfig(
        name="test_walutomat_fx",
        strategy_class="TestStrategy",
        inputs=["market.walutomat.EUR-PLN.ticks"],
        outputs=["EUR-PLN"],
        exchange="walutomat",
    )
    assert config.outputs == ["EUR-PLN"]


def test_kraken_rejects_invalid_symbols() -> None:
    """Verify Kraken rejects invalid symbols.

    Given: Kraken exchange,
    When: Invalid pair,
    Then: ValueError raised.
    """
    with pytest.raises(ValueError, match="not tradeable on kraken"):
        StrategyConfig(
            name="test_kraken_invalid",
            strategy_class="TestStrategy",
            inputs=["market.kraken.INVALID-USD.candles"],
            outputs=["INVALID-USD"],
            exchange="kraken",
        )


def test_kraken_accepts_valid_symbols() -> None:
    """Verify Kraken accepts valid symbols.

    Given: Kraken exchange,
    When: Valid BTC-USD,
    Then: Config accepted.
    """
    config = StrategyConfig(
        name="test_kraken_valid",
        strategy_class="TestStrategy",
        inputs=["market.kraken.BTC-USD.candles.1h"],
        outputs=["BTC-USD"],
        exchange="kraken",
    )
    assert config.outputs == ["BTC-USD"]


def test_multiple_outputs_all_must_be_valid() -> None:
    """Verify all outputs must be valid.

    Given: Multiple outputs,
    When: One is invalid,
    Then: ValueError raised.
    """
    with pytest.raises(ValueError, match="not tradeable on kraken"):
        StrategyConfig(
            name="test_multi_invalid",
            strategy_class="TestStrategy",
            inputs=["market.kraken.BTC-USD.candles.1h"],
            outputs=["BTC-USD", "INVALID-PAIR", "ETH-USD"],
            exchange="kraken",
        )


TEST_INSTRUMENT = "ETH-USD"


class TestRSIReversion:
    """Test suite for RSI reversion strategy."""

    @pytest.fixture
    def rsi_strategy(self) -> RSIReversion:
        """Create RSI strategy for tests."""
        config = StrategyConfig(
            name="test_rsi",
            strategy_class="RSIReversion",
            inputs=["market.kraken.ETH-USD.candles.1h"],
            outputs=["ETH-USD"],
            params={"period": 14, "upper": 70.0, "lower": 30.0, "cooldown": 0},
        )
        return RSIReversion(config)

    @pytest.fixture
    def config(self) -> StrategyConfig:
        """Create RSI config for tests."""
        return StrategyConfig(
            name="test_rsi",
            strategy_class="RSIReversion",
            inputs=["market.kraken.ETH-USD.candles.1h"],
            outputs=["ETH-USD"],
            params={"period": 14, "upper": 70.0, "lower": 30.0, "cooldown": 0},
        )

    @pytest.fixture
    def strategy(self, config: StrategyConfig) -> RSIReversion:
        """Create RSI strategy from config."""
        return RSIReversion(config)

    @pytest.mark.asyncio
    async def test_initialization(self, strategy: RSIReversion) -> None:
        """Verify RSI initialization sets parameters.

        Given: Config with period/upper/lower/cooldown,
        When: Strategy created,
        Then: Parameters correctly assigned.
        """
        assert strategy.period == 14
        assert strategy.upper == pytest.approx(70.0)
        assert strategy.lower == pytest.approx(30.0)
        assert strategy.cooldown == 0
        assert strategy._cool == {}

    @pytest.mark.asyncio
    async def test_buy_signal_oversold(self, strategy: RSIReversion) -> None:
        """Verify oversold generates buy signal.

        Given: Declining prices,
        When: RSI below lower threshold,
        Then: Buy signal with strength 1.0.
        """
        closes = [100.0] * 10 + [95.0, 90.0, 85.0, 80.0, 75.0]
        signal = await feed_closes_to_strategy(strategy, TEST_INSTRUMENT, closes)
        assert signal is not None
        assert signal.instrument == TEST_INSTRUMENT
        assert signal.side == "buy"
        assert signal.strength == pytest.approx(1.0)
        assert "RSI" in signal.reason
        assert "period=14" in signal.reason

    @pytest.mark.asyncio
    async def test_sell_signal_overbought(self, strategy: RSIReversion) -> None:
        """Verify overbought generates sell signal.

        Given: Rising prices,
        When: RSI above upper threshold,
        Then: Sell signal with strength 1.0.
        """
        closes = [100.0] * 10 + [105.0, 110.0, 115.0, 120.0, 125.0]
        signal = await feed_closes_to_strategy(strategy, TEST_INSTRUMENT, closes)
        assert signal is not None
        assert signal.instrument == TEST_INSTRUMENT
        assert signal.side == "sell"
        assert signal.strength == pytest.approx(1.0)
        assert "RSI" in signal.reason
        assert "period=14" in signal.reason

    @pytest.mark.asyncio
    async def test_no_signal_neutral(self, strategy: RSIReversion) -> None:
        """Verify neutral RSI returns None.

        Given: Sideways prices,
        When: RSI between thresholds,
        Then: StrategySignal is None.
        """
        closes = [
            100.0,
            101.0,
            99.0,
            102.0,
            98.0,
            101.0,
            100.0,
            99.0,
            101.0,
            100.0,
            101.0,
            99.0,
            100.0,
            101.0,
            100.0,
            99.0,
            100.0,
            101.0,
            100.0,
            99.0,
        ]
        signal = await feed_closes_to_strategy(strategy, TEST_INSTRUMENT, closes)
        assert signal is None

    @pytest.mark.asyncio
    async def test_cooldown(self, strategy: RSIReversion) -> None:
        """Verify cooldown prevents rapid signals.

        Given: StrategySignal generated with cooldown=2,
        When: Conditions persist,
        Then: Next signal after cooldown bars.
        """
        strategy.cooldown = 2
        closes = [100.0] * 10 + [95.0, 90.0, 85.0, 80.0, 75.0]
        signal1 = await feed_closes_to_strategy(strategy, TEST_INSTRUMENT, closes)
        assert signal1 is not None
        assert signal1.side == "buy"
        signal2 = await feed_bar_to_strategy(strategy, TEST_INSTRUMENT, 75.0)
        assert signal2 is None
        signal3 = await feed_bar_to_strategy(strategy, TEST_INSTRUMENT, 75.0)
        assert signal3 is None
        signal4 = await feed_bar_to_strategy(strategy, TEST_INSTRUMENT, 75.0)
        assert signal4 is not None

    @pytest.mark.asyncio
    async def test_insufficient_data(self, strategy: RSIReversion) -> None:
        """Verify insufficient data returns None.

        Given: Only three bars,
        When: on_candle called,
        Then: StrategySignal is None.
        """
        closes = [100.0, 101.0, 102.0]
        signal = await feed_closes_to_strategy(strategy, TEST_INSTRUMENT, closes)
        assert signal is None

    @pytest.mark.asyncio
    async def test_wrong_instrument(self, strategy: RSIReversion) -> None:
        """Verify different instrument still works.

        Given: RSI configured for ETH-USD,
        When: BTC-USD bars fed,
        Then: StrategySignal still generated (RSI is multi-instrument).
        """
        closes = [100.0] * 10 + [95.0, 90.0, 85.0, 80.0, 75.0]
        signal = await feed_closes_to_strategy(strategy, "BTC-USD", closes)
        assert signal is not None

    @pytest.mark.asyncio
    async def test_multi_instrument_cooldown(self, strategy: RSIReversion) -> None:
        """Verify cooldown is per-instrument.

        Given: Two instruments,
        When: Both trigger signals,
        Then: Cooldown tracked separately.
        """
        strategy.cooldown = 1
        strategy.inputs = [
            "market.kraken.ETH-USD.candles.1h",
            "market.kraken.BTC-USD.candles.1h",
        ]
        eth_closes = [100.0] * 10 + [95.0, 90.0, 85.0, 80.0, 75.0]
        btc_closes = [200.0] * 10 + [195.0, 190.0, 185.0, 180.0, 175.0]
        eth_signal1 = await feed_closes_to_strategy(strategy, "ETH-USD", eth_closes)
        assert eth_signal1 is not None
        btc_signal1 = await feed_closes_to_strategy(strategy, "BTC-USD", btc_closes)
        assert btc_signal1 is not None
        eth_signal2 = await feed_bar_to_strategy(strategy, "ETH-USD", 75.0)
        assert eth_signal2 is None
        btc_signal2 = await feed_bar_to_strategy(strategy, "BTC-USD", 175.0)
        assert btc_signal2 is None

    @pytest.mark.asyncio
    async def test_custom_parameters(self) -> None:
        """Verify custom RSI parameters.

        Given: Custom period/upper/lower,
        When: Strategy created,
        Then: Parameters correctly set.
        """
        config = StrategyConfig(
            name="custom_rsi",
            strategy_class="RSIReversion",
            inputs=["market.kraken.ETH-USD.candles.1h"],
            outputs=["ETH-USD"],
            params={"period": 10, "upper": 75.0, "lower": 25.0},
        )
        strategy = RSIReversion(config)
        assert strategy.period == 10
        assert strategy.upper == pytest.approx(75.0)
        assert strategy.lower == pytest.approx(25.0)

    @pytest.mark.asyncio
    async def test_reset_clears_cooldown(self, strategy: RSIReversion) -> None:
        """Verify reset clears cooldown state.

        Given: Strategy with active cooldown,
        When: reset called,
        Then: Cooldown dict empty.
        """
        strategy._cool[TEST_INSTRUMENT] = 3
        await strategy.reset()
        assert strategy._cool == {}


def _valid_config_kwargs() -> dict[str, Any]:
    return {
        "name": "test_strategy",
        "strategy_class": "ExampleStrategy",
        "inputs": ["market.kraken.BTC-USD.candles"],
        "outputs": ["BTC-USD"],
        "exchange": "kraken",
        "params": {},
    }


def test_strategy_config_allows_paper_flow() -> None:
    """Verify paper exchange flow allowed.

    Given: Paper input,
    When: exchange='paper',
    Then: Config accepted.
    """
    kwargs = _valid_config_kwargs()
    kwargs["inputs"] = ["market.paper.kraken.BTC-USD.candles"]
    kwargs["exchange"] = "paper"
    StrategyConfig(**kwargs)


def test_strategy_config_rejects_paper_output_mismatch() -> None:
    """Verify paper input with live exchange rejected.

    Given: Paper input,
    When: exchange='kraken',
    Then: ValueError raised.
    """
    kwargs = _valid_config_kwargs()
    kwargs["inputs"] = ["market.paper.kraken.BTC-USD.candles"]
    kwargs["exchange"] = "kraken"
    with pytest.raises(ValueError, match="Paper/replay input data MUST use exchange='paper'"):
        StrategyConfig(**kwargs)


def test_strategy_config_rejects_mixed_inputs() -> None:
    """Verify mixed paper/live inputs rejected.

    Given: Both paper and live inputs,
    When: Creating config,
    Then: ValueError raised.
    """
    kwargs = _valid_config_kwargs()
    kwargs["inputs"] = [
        "market.paper.kraken.BTC-USD.candles",
        "market.kraken.BTC-USD.candles",
    ]
    kwargs["exchange"] = "paper"
    with pytest.raises(ValueError, match="Cannot mix paper/replay and live inputs"):
        StrategyConfig(**kwargs)


def test_strategy_config_requires_non_empty_fields() -> None:
    """Verify non-empty fields required.

    Given: Empty name/inputs/outputs,
    When: Creating config,
    Then: ValueError raised.
    """
    kwargs = _valid_config_kwargs()
    kwargs["name"] = ""
    with pytest.raises(ValueError, match="Strategy name cannot be empty"):
        StrategyConfig(**kwargs)
    kwargs = _valid_config_kwargs()
    kwargs["inputs"] = []
    with pytest.raises(ValueError, match="must have at least one input"):
        StrategyConfig(**kwargs)
    kwargs = _valid_config_kwargs()
    kwargs["outputs"] = []
    with pytest.raises(ValueError, match="must define at least one output instrument"):
        StrategyConfig(**kwargs)


class _TestStrategy(BaseStrategy):
    """Test strategy implementation for exchange validation tests."""

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle data and return optional signal."""
        return None

    async def reset(self) -> None:
        """Reset strategy state."""
        return None


def test_invalid_exchange_raises_error() -> None:
    """Verify invalid exchange raises error.

    Given: Unknown exchange name,
    When: Creating config,
    Then: ValueError raised.
    """
    with pytest.raises(ValueError, match="exchange must be one of"):
        StrategyConfig(
            name="test_strategy",
            strategy_class="_TestStrategy",
            inputs=["market.kraken.BTC-USD.candles"],
            outputs=["BTC-USD"],
            exchange="invalid_exchange",
        )


class MockMACDCrossover(BaseStrategy):
    """Mock MACD crossover strategy for factory testing."""

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle data and return optional signal."""
        return None

    async def reset(self) -> None:
        """Reset strategy state."""
        pass


class MockRSIReversion(BaseStrategy):
    """Mock RSI reversion strategy for factory testing."""

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle data and return optional signal."""
        return None

    async def reset(self) -> None:
        """Reset strategy state."""
        pass


class TestStrategyFactory:
    """Test suite for StrategyFactory."""

    @pytest.fixture
    def factory(self) -> StrategyFactory:
        """Create factory with mock strategies."""
        factory = StrategyFactory()
        factory.register_strategy_class("MACDCrossover", MockMACDCrossover)
        factory.register_strategy_class("RSIReversion", RSIReversion)
        return factory

    def test_create_strategy_success(self, factory: StrategyFactory) -> None:
        """Verify successful strategy creation.

        Given: Factory with registered class,
        When: create_strategy called,
        Then: Strategy created with correct name/inputs/outputs.
        """
        config = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
            params={"fast": 12, "slow": 26},
        )
        strategy = factory.create_strategy(config)
        assert strategy.name == "macd_btc"
        assert strategy.inputs == ["market.kraken.BTC-USD.candles.1m"]
        assert strategy.output_topics == ["signals.paper.BTC-USD.macd_btc"]

    def test_create_multiple_strategies_different_outputs(self, factory: StrategyFactory) -> None:
        """Verify multiple strategies with different outputs.

        Given: Factory with registered class,
        When: Multiple strategies created,
        Then: Each has unique output topics.
        """
        config1 = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        config2 = StrategyConfig(
            name="macd_eth",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.ETH-USD.candles.1m"],
            outputs=["ETH-USD"],
        )
        strategy1 = factory.create_strategy(config1)
        strategy2 = factory.create_strategy(config2)
        assert strategy1.output_topics == ["signals.paper.BTC-USD.macd_btc"]
        assert strategy2.output_topics == ["signals.paper.ETH-USD.macd_eth"]

    def test_duplicate_output_raises_error(self, factory: StrategyFactory) -> None:
        """Verify duplicate output raises error.

        Given: Strategy with output topic,
        When: Second strategy with same output,
        Then: ValueError raised.
        """
        config1 = StrategyConfig(
            name="macd_btc_fast",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
            exchange="kraken",
        )
        factory.create_strategy(config1)
        config2 = StrategyConfig(
            name="macd_btc_slow",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
            exchange="kraken",
        )
        with pytest.raises(
            ValueError,
            match=r"Output topic 'signals\.kraken\.BTC-USD\.live' already used",
        ):
            factory.create_strategy(config2)

    def test_duplicate_name_raises_error(self, factory: StrategyFactory) -> None:
        """Verify duplicate name raises error.

        Given: Strategy with name 'macd_btc',
        When: Second strategy with same name,
        Then: ValueError raised.
        """
        config1 = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        factory.create_strategy(config1)
        config2 = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.ETH-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        with pytest.raises(ValueError, match="already exists"):
            factory.create_strategy(config2)

    def test_unknown_strategy_class_raises_error(self, factory: StrategyFactory) -> None:
        """Verify unknown class raises error.

        Given: Config with unregistered class,
        When: create_strategy called,
        Then: KeyError raised.
        """
        config = StrategyConfig(
            name="unknown",
            strategy_class="NonExistentStrategy",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        with pytest.raises(KeyError, match="Unknown strategy class"):
            factory.create_strategy(config)

    @pytest.mark.asyncio
    async def test_start_strategy_success(self, factory: StrategyFactory) -> None:
        """Verify start_strategy sets running flag.

        Given: Created strategy,
        When: start_strategy called,
        Then: Strategy _running is True.
        """
        config = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        factory.create_strategy(config)
        await factory.start_strategy("macd_btc")
        strategy = factory.get_strategy("macd_btc")
        assert strategy._running is True

    @pytest.mark.asyncio
    async def test_start_nonexistent_strategy_raises_error(self, factory: StrategyFactory) -> None:
        """Verify start nonexistent raises error.

        Given: Empty factory,
        When: start_strategy called,
        Then: StrategyNotFoundError raised.
        """
        with pytest.raises(StrategyNotFoundError):
            await factory.start_strategy("nonexistent")

    @pytest.mark.asyncio
    async def test_stop_strategy_success(self, factory: StrategyFactory) -> None:
        """Verify stop_strategy removes strategy.

        Given: Running strategy,
        When: stop_strategy called,
        Then: Strategy removed from factory.
        """
        config = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        factory.create_strategy(config)
        await factory.start_strategy("macd_btc")
        await factory.stop_strategy("macd_btc")
        with pytest.raises(StrategyNotFoundError):
            factory.get_strategy("macd_btc")

    @pytest.mark.asyncio
    async def test_stop_all_strategies(self, factory: StrategyFactory) -> None:
        """Verify stop_all removes all strategies.

        Given: Multiple running strategies,
        When: stop_all called,
        Then: All strategies removed.
        """
        configs = [
            StrategyConfig(
                name="macd_btc",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.BTC-USD.candles.1m"],
                outputs=["BTC-USD"],
            ),
            StrategyConfig(
                name="macd_eth",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.ETH-USD.candles.1m"],
                outputs=["ETH-USD"],
            ),
        ]
        for config in configs:
            factory.create_strategy(config)
            await factory.start_strategy(config.name)
        await factory.stop_all()
        with pytest.raises(StrategyNotFoundError):
            factory.get_strategy("macd_btc")
        with pytest.raises(StrategyNotFoundError):
            factory.get_strategy("macd_eth")

    def test_list_strategies(self, factory: StrategyFactory) -> None:
        """Verify list_strategies returns all.

        Given: Two strategies created,
        When: list_strategies called,
        Then: Both returned with details.
        """
        config1 = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
            params={"fast": 12},
        )
        config2 = StrategyConfig(
            name="macd_eth",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.ETH-USD.candles.1m"],
            outputs=["ETH-USD"],
            params={"fast": 8},
        )
        factory.create_strategy(config1)
        factory.create_strategy(config2)
        strategies = factory.list_strategies()
        assert len(strategies) == 2
        assert "macd_btc" in strategies
        assert "macd_eth" in strategies
        assert strategies["macd_btc"]["inputs"] == ["market.kraken.BTC-USD.candles.1m"]
        assert strategies["macd_btc"]["outputs"] == ["BTC-USD"]
        assert strategies["macd_btc"]["params"] == {"fast": 12}

    def test_validate_no_output_conflicts_success(self, factory: StrategyFactory) -> None:
        """Verify no conflicts detected.

        Given: Strategies with different outputs,
        When: validate_no_output_conflicts called,
        Then: Empty error list.
        """
        configs = [
            StrategyConfig(
                name="macd_btc",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.BTC-USD.candles.1m"],
                outputs=["BTC-USD"],
            ),
            StrategyConfig(
                name="macd_eth",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.ETH-USD.candles.1m"],
                outputs=["ETH-USD"],
            ),
        ]
        errors = factory.validate_no_output_conflicts(configs)
        assert errors == []

    def test_validate_output_conflicts_detected(self, factory: StrategyFactory) -> None:
        """Verify conflicts detected.

        Given: Two strategies with same output,
        When: validate_no_output_conflicts called,
        Then: Error list with conflict.
        """
        configs = [
            StrategyConfig(
                name="macd_btc_1",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.BTC-USD.candles.1m"],
                outputs=["BTC-USD"],
                exchange="kraken",
            ),
            StrategyConfig(
                name="macd_btc_2",
                strategy_class="MACDCrossover",
                inputs=["BTC-USD:15m"],
                outputs=["BTC-USD"],
                exchange="kraken",
            ),
            StrategyConfig(
                name="macd_eth",
                strategy_class="MACDCrossover",
                inputs=["market.kraken.ETH-USD.candles.1m"],
                outputs=["ETH-USD"],
                exchange="kraken",
            ),
        ]
        errors = factory.validate_no_output_conflicts(configs)
        assert len(errors) == 1
        assert "macd_btc_1" in errors[0]
        assert "macd_btc_2" in errors[0]
        assert "signals.kraken.BTC-USD.live" in errors[0]

    def test_get_strategy_success(self, factory: StrategyFactory) -> None:
        """Verify get_strategy returns strategy.

        Given: Strategy created,
        When: get_strategy called,
        Then: Strategy returned.
        """
        config = StrategyConfig(
            name="macd_btc",
            strategy_class="MACDCrossover",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
        )
        factory.create_strategy(config)
        strategy = factory.get_strategy("macd_btc")
        assert strategy.name == "macd_btc"

    def test_get_nonexistent_strategy_raises_error(self, factory: StrategyFactory) -> None:
        """Verify get nonexistent raises error.

        Given: Empty factory,
        When: get_strategy called,
        Then: StrategyNotFoundError raised.
        """
        with pytest.raises(StrategyNotFoundError):
            factory.get_strategy("nonexistent")

    def test_create_rsi_strategy(self, factory: StrategyFactory) -> None:
        """Verify RSI strategy creation.

        Given: RSIReversion registered,
        When: create_strategy called,
        Then: Strategy created with correct props.
        """
        config = StrategyConfig(
            name="rsi_btc",
            strategy_class="RSIReversion",
            inputs=["market.kraken.BTC-USD.candles.1m"],
            outputs=["BTC-USD"],
            params={"period": 14, "upper": 70.0, "lower": 30.0},
        )
        strategy = factory.create_strategy(config)
        assert strategy.name == "rsi_btc"
        assert strategy.inputs == ["market.kraken.BTC-USD.candles.1m"]
        assert strategy.output_topics == ["signals.paper.BTC-USD.rsi_btc"]


def test_check_gap_parsed_handles_invalid_json() -> None:
    """Verify _check_gap_parsed silently handles invalid JSON.

    Given: A strategy instance,
    When: _check_gap_parsed is called with non-JSON payload,
    Then: No exception is raised.
    """
    strategy = FakeStrategy(_strategy_config())
    strategy._check_gap_parsed("test.topic", "not-json")


def test_check_gap_parsed_handles_valid_message() -> None:
    """Verify _check_gap_parsed extracts provenance from a typed message.

    Given: A strategy with a gap detector,
    When: _check_gap_parsed receives valid TickData JSON,
    Then: Gap detector processes the message.
    """
    strategy = FakeStrategy(_strategy_config())
    tick = TickData(
        instrument="BTC-USD",
        exchange="kraken",
        volume=1.0,
        session_id="abc",
        sequence_id=1,
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
    )
    strategy._check_gap_parsed("market.kraken.BTC-USD.ticks", tick.to_json())
    assert strategy._gap_detector.stats.mid_stream_joins == 0


class _PairedTestStrategy(BaseStrategy):
    """Minimal strategy declaring a simultaneous policy for paired-emission tests."""

    PAIRED_EXECUTION_POLICY = PairedExecutionPolicyEnum.SIMULTANEOUS

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignalResult:
        """Unused: the group-emission path is driven directly in these tests."""
        await asyncio.sleep(0)
        return None

    async def reset(self) -> None:
        """Reset strategy state (no-op for these emission tests)."""
        await asyncio.sleep(0)


def _paired_mock_publisher(strategy: BaseStrategy) -> MagicMock:
    """Wire a strategy with a mock publisher and return it."""
    publisher = MagicMock()
    publisher.send = AsyncMock()
    publisher.tracker = SequenceTracker()
    publisher.session_id = publisher.tracker.session_id
    strategy.msg_publisher = publisher
    strategy._last_data_ts = 200.0
    return publisher


class TestPairedGroupEmission:
    """BaseStrategy stamps the paired-group descriptor on multi-leg emissions."""

    @pytest.mark.asyncio
    async def test_single_leg_group_carries_no_descriptor(self) -> None:
        """A one-leg group emits a standalone signal with no descriptor.

        Given: a strategy whose group has exactly one leg,
        When: the group is emitted,
        Then: the published envelope leaves every paired_group_* field None.
        """
        strategy = _PairedTestStrategy(_strategy_config(exchange="paper", name="paired"))
        publisher = _paired_mock_publisher(strategy)
        await strategy._emit_signal_group(
            [
                StrategySignal(
                    instrument="BTC-USD", side="buy", strength=0.5, reason="solo", price=10.0
                )
            ]
        )
        publisher.send.assert_called_once()
        envelope = publisher.send.call_args.args[1]
        assert envelope.paired_group_id is None
        assert envelope.paired_group_size is None
        assert envelope.paired_group_index is None
        assert envelope.paired_group_policy is None
        assert envelope.paired_group_key is None

    @pytest.mark.asyncio
    async def test_empty_group_is_a_noop(self) -> None:
        """An empty group publishes nothing.

        Given: a strategy and an empty (no-signal) group,
        When: the group is emitted,
        Then: no envelope is published.
        """
        strategy = _PairedTestStrategy(_strategy_config(exchange="paper", name="paired"))
        publisher = _paired_mock_publisher(strategy)
        await strategy._emit_signal_group([])
        publisher.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_multi_leg_group_stamps_shared_descriptor(self) -> None:
        """A multi-leg group shares one id/key/policy with per-leg indices.

        Given: a simultaneous strategy emitting a two-leg group,
        When: the group is emitted,
        Then: both envelopes carry the same paired_group_id, size, policy and
            canonical key, with indices 0 and 1 in declared order, and the
            persisted signals carry the same paired_group_id.
        """
        strategy = _PairedTestStrategy(
            _strategy_config(exchange="paper", name="paired", outputs=["BTC-USD", "ETH-USD"])
        )
        publisher = _paired_mock_publisher(strategy)
        await strategy._emit_signal_group(
            [
                StrategySignal(
                    instrument="BTC-USD", side="buy", strength=0.5, reason="leg0", price=10.0
                ),
                StrategySignal(
                    instrument="ETH-USD", side="sell", strength=0.5, reason="leg1", price=20.0
                ),
            ]
        )
        assert publisher.send.call_count == 2
        first = publisher.send.call_args_list[0].args[1]
        second = publisher.send.call_args_list[1].args[1]
        assert first.paired_group_id == second.paired_group_id
        assert first.paired_group_id
        assert first.paired_group_size == 2
        assert second.paired_group_size == 2
        assert first.paired_group_index == 0
        assert second.paired_group_index == 1
        assert first.paired_group_policy == "simultaneous"
        assert second.paired_group_policy == "simultaneous"
        expected_key = "paper:BTC-USD:paper|paper:ETH-USD:paper"
        assert first.paired_group_key == expected_key
        assert second.paired_group_key == expected_key
        store = signal_service.store_signal
        assert store.await_count == 2
        assert store.await_args_list[0].kwargs["paired_group_id"] == first.paired_group_id

    @pytest.mark.asyncio
    async def test_multi_leg_without_policy_fails_closed(self) -> None:
        """A multi-leg group with no declared policy raises before any publish.

        Given: a strategy that does NOT declare PAIRED_EXECUTION_POLICY,
        When: a two-leg group is emitted,
        Then: a ValueError is raised and nothing is published, so an
            undeclared group can never arm.
        """
        strategy = FakeStrategy(_strategy_config(exchange="paper", name="nopolicy"))
        publisher = _paired_mock_publisher(strategy)
        with pytest.raises(ValueError, match="PAIRED_EXECUTION_POLICY"):
            await strategy._emit_signal_group(
                [
                    StrategySignal(
                        instrument="BTC-USD", side="buy", strength=0.5, reason="l0", price=1.0
                    ),
                    StrategySignal(
                        instrument="ETH-USD", side="sell", strength=0.5, reason="l1", price=2.0
                    ),
                ]
            )
        publisher.send.assert_not_called()

    def test_execution_mode_reflects_exchange(self) -> None:
        """Execution mode is paper for the paper exchange and live otherwise.

        Given: a paper strategy and a live (kraken) strategy,
        When: _execution_mode is queried,
        Then: it returns 'paper' and 'live' respectively, matching the engine
            mode stamped on each paired-execution leg.
        """
        paper = _PairedTestStrategy(_strategy_config(exchange="paper", name="p"))
        live = _PairedTestStrategy(_strategy_config(exchange="kraken", name="l"))
        assert paper._execution_mode() == "paper"
        assert live._execution_mode() == "live"


def _grouped_row(close: float, *, ticker: str = "X:FETUSD", day: date) -> GroupedDailyRow:
    """Build a GroupedDailyRow whose close == open == high == low for tests."""
    return GroupedDailyRow(
        ticker=ticker,
        open=Decimal(str(close)),
        high=Decimal(str(close)),
        low=Decimal(str(close)),
        close=Decimal(str(close)),
        volume=Decimal("10"),
        vwap=Decimal(str(close)),
        total_trades=5,
        closing_timestamp=datetime(day.year, day.month, day.day, 23, 59, 59, 999000, tzinfo=UTC),
    )


def _warmup_candle(close: float, *, day: date, instrument: str = "FET-USD") -> CandleData:
    """Project a grouped row to a warmup CandleData with the given day's open_at."""
    return _grouped_row_to_warmup_candle(
        _grouped_row(close, day=day), instrument=instrument, exchange="kraken", sequence_id=0
    )


def _write_warmup_cache(
    cache_root: Path, days: int, *, tickers: tuple[str, ...] = ("X:FETUSD",), end: date
) -> None:
    """Write `days` grouped-daily cache CSVs (all `tickers` per day) ending at `end`."""
    for offset in range(days):
        current = end - timedelta(days=offset)
        path = cache_root / "grouped" / "crypto" / str(current.year) / f"{current.isoformat()}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        closing = datetime(current.year, current.month, current.day, 23, 59, 59, 999000, tzinfo=UTC)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(
                [
                    "ticker",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "vwap",
                    "total_trades",
                    "closing_timestamp",
                ]
            )
            for ticker in tickers:
                writer.writerow([ticker, 1.0, 1.0, 1.0, 1.0, 10.0, 1.0, 5, closing.isoformat()])


class _WarmupStrategy(MockStrategy):
    """MockStrategy that declares a warm-up requirement via the ``warmup_n`` param."""

    def required_candle_history(self) -> int:
        """Return the configured warm-up bar count (0 when unset)."""
        return int(self.params.get("warmup_n", 0))


def _db_candle_row(close: float, *, day: date, volume: float = 10.0) -> dict[str, Any]:
    """Build a persisted 1d CandleRow dict (00:00 UTC open_at) for warmup DB tests."""
    open_at = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return {
        "open_at": open_at,
        "timeframe": "1d",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": volume,
        "vwap": close,
        "trades": 5,
        "source": "synthesized",
        "complete": True,
        "public_id": "00000000-0000-7000-8000-0000000000aa",
        "timestamp": open_at,
        "session_id": "seed",
        "sequence_id": 1,
    }


class _StubWarmupRepo:
    """Repository stub returning configured 1d rows per instrument (DB-first warmup).

    The warmup uses the shared process-cached repo and must NOT dispose it, so this
    stub deliberately exposes no engine/dispose surface.
    """

    def __init__(self, rows_by_symbol: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self._rows = rows_by_symbol or {}

    async def get_candles(
        self,
        *,
        instrument: str,
        timeframe: str,
        start: Any,
        end: Any,
        exchange: Any,
        as_of: Any,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        rows = sorted(
            self._rows.get(instrument, []), key=lambda r: r["open_at"], reverse=order == "desc"
        )
        return rows[:limit] if limit is not None else rows


class TestWarmupPrefill:
    """A3-smoke: strategy candle-buffer warmup from the Polygon daily cache."""

    @pytest.fixture(autouse=True)
    def _empty_db_plane(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default the DB-first warmup to an empty plane so these tests hit the cache.

        Slice 5 made warmup DB-first; with an empty persisted plane the loader
        falls back to the Polygon cache, preserving the pre-slice-5 assertions
        here while exercising the DB-short fallback path.
        """
        monkeypatch.setattr("snapper.strategies.base.get_repository", lambda url: _StubWarmupRepo())

    def test_native_to_polygon_crypto_ticker(self) -> None:
        """A BASE-QUOTE symbol maps to X:{BASE}{QUOTE}; non-pairs map to None."""
        assert _native_to_polygon_crypto_ticker("FET-USD") == "X:FETUSD"
        assert _native_to_polygon_crypto_ticker("RENDER-USD") == "X:RENDERUSD"
        assert _native_to_polygon_crypto_ticker("BTCUSD") is None
        assert _native_to_polygon_crypto_ticker("A-B-C") is None
        assert _native_to_polygon_crypto_ticker("FET-") is None

    def test_grouped_row_projection_floors_open_at_to_utc_day(self) -> None:
        """The projection floors closing_timestamp to 00:00 UTC and stamps fields."""
        candle = _warmup_candle(2.5, day=date(2024, 2, 16))
        assert candle.open_at == datetime(2024, 2, 16, tzinfo=UTC)
        assert candle.timeframe == "1d"
        assert candle.exchange == "kraken"
        assert candle.instrument == "FET-USD"
        assert candle.close == 2.5
        assert candle.vwap == 2.5
        assert candle.trades == 5
        assert isinstance(candle.public_id, str)
        assert candle.session_id == "warmup"

    def test_buffer_candle_upserts_same_open_at(self) -> None:
        """A candle with an existing open_at replaces it; distinct ones append."""
        strat = _WarmupStrategy(_strategy_config(name="w"))
        first = _warmup_candle(1.0, day=date(2024, 2, 1))
        replacement = _warmup_candle(9.0, day=date(2024, 2, 1))
        second = _warmup_candle(2.0, day=date(2024, 2, 2))
        strat._buffer_candle("FET-USD", first)
        strat._buffer_candle("FET-USD", second)
        strat._buffer_candle("FET-USD", replacement)
        buffer = strat.candle_buffer["FET-USD"]
        assert [c.close for c in buffer] == [9.0, 2.0]

    def test_buffer_candle_prunes_to_buffer_size(self) -> None:
        """Appending beyond buffer_size drops the oldest bar."""
        strat = _WarmupStrategy(_strategy_config(name="w", params={"buffer_size": 2}))
        for offset in range(4):
            strat._buffer_candle(
                "FET-USD", _warmup_candle(float(offset), day=date(2024, 2, 1 + offset))
            )
        buffer = strat.candle_buffer["FET-USD"]
        assert [c.close for c in buffer] == [2.0, 3.0]

    def test_buffer_candle_inserts_out_of_order_ascending(self) -> None:
        """A stale older bar is inserted in order, keeping the buffer ascending."""
        strat = _WarmupStrategy(_strategy_config(name="w"))
        strat._buffer_candle("FET-USD", _warmup_candle(2.0, day=date(2024, 2, 2)))
        strat._buffer_candle("FET-USD", _warmup_candle(3.0, day=date(2024, 2, 3)))
        strat._buffer_candle("FET-USD", _warmup_candle(1.0, day=date(2024, 2, 1)))
        buffer = strat.candle_buffer["FET-USD"]
        assert [c.open_at for c in buffer] == sorted(c.open_at for c in buffer)
        assert [c.close for c in buffer] == [1.0, 2.0, 3.0]

    def _warmup_config(self, **params: Any) -> StrategyConfig:
        """Build a crypto-opt-in warmup strategy config with the given params."""
        base = {"warmup_n": 3, "warmup_market_type": "crypto"}
        base.update(params)
        return _strategy_config(
            name="w",
            exchange="paper",
            inputs=["market.paper.kraken.FET-USD.candles.1d"],
            params=base,
        )

    @pytest.mark.asyncio
    async def test_warmup_fills_buffer_for_paper_1d_input(self, tmp_path: Path) -> None:
        """A 1d paper input warms the buffer with source-exchange candles."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 3, end=end)
        strat = _WarmupStrategy(self._warmup_config(polygon_cache_root=str(tmp_path)))
        await strat._warmup_candle_buffer()
        buffer = strat.candle_buffer["FET-USD"]
        assert len(buffer) == 3
        assert all(c.exchange == "kraken" for c in buffer)
        assert all(c.timeframe == "1d" for c in buffer)

    @pytest.mark.asyncio
    async def test_warmup_aligns_multi_leg_buffers(self, tmp_path: Path) -> None:
        """A two-leg pair warms both legs to the SAME aligned UTC days."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 3, tickers=("X:FETUSD", "X:RENDERUSD"), end=end)
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                exchange="paper",
                inputs=[
                    "market.paper.kraken.FET-USD.candles.1d",
                    "market.paper.kraken.RENDER-USD.candles.1d",
                ],
                params={
                    "warmup_n": 3,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path),
                },
            )
        )
        await strat._warmup_candle_buffer()
        fet = [c.open_at for c in strat.candle_buffer["FET-USD"]]
        render = [c.open_at for c in strat.candle_buffer["RENDER-USD"]]
        assert len(fet) == 3
        assert fet == render

    @pytest.mark.asyncio
    async def test_fet_render_default_config_opts_into_crypto_warmup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The registered FET/RENDER preset is warmup-ready by default."""

        def _is_tradeable(_instrument: str, _exchange: object) -> bool:
            return True

        monkeypatch.setattr("snapper.strategies.models.is_tradeable", _is_tradeable)
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 60, tickers=("X:FETUSD", "X:RENDERUSD"), end=end)
        params = dict(cast(dict[str, object], _FET_RENDER_DEFAULT_CONFIG["params"]))
        params["polygon_cache_root"] = str(tmp_path)
        config = StrategyConfig(
            name=cast(str, _FET_RENDER_DEFAULT_CONFIG["name"]),
            strategy_class="CointegrationPairs",
            inputs=list(cast(list[str], _FET_RENDER_DEFAULT_CONFIG["inputs"])),
            outputs=list(cast(list[str], _FET_RENDER_DEFAULT_CONFIG["outputs"])),
            exchange=cast(OrderExchange, _FET_RENDER_DEFAULT_CONFIG["exchange"]),
            params=params,
        )
        strat = CointegrationPairs(config)
        await strat._warmup_candle_buffer()
        fet = [c.open_at for c in strat.candle_buffer["FET-USD"]]
        render = [c.open_at for c in strat.candle_buffer["RENDER-USD"]]
        assert len(fet) == 60
        assert fet == render

    @pytest.mark.asyncio
    async def test_warmup_uneven_multi_leg_installs_nothing(self, tmp_path: Path) -> None:
        """If one leg has no cache, NO leg is warmed (keeps the spread aligned)."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 3, tickers=("X:FETUSD",), end=end)
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                exchange="paper",
                inputs=[
                    "market.paper.kraken.FET-USD.candles.1d",
                    "market.paper.kraken.RENDER-USD.candles.1d",
                ],
                params={
                    "warmup_n": 3,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path),
                },
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_insufficient_aligned_installs_nothing(self, tmp_path: Path) -> None:
        """Legs with fewer than `count` common days warm nothing (live-only)."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 2, tickers=("X:FETUSD",), end=end)
        _write_warmup_cache(tmp_path, 2, tickers=("X:RENDERUSD",), end=end - timedelta(days=5))
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                exchange="paper",
                inputs=[
                    "market.paper.kraken.FET-USD.candles.1d",
                    "market.paper.kraken.RENDER-USD.candles.1d",
                ],
                params={
                    "warmup_n": 2,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path),
                },
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_requires_opt_in_market_type(self, tmp_path: Path) -> None:
        """Without warmup_market_type the cache is never read (opt-in, crypto-scoped)."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 3, end=end)
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                exchange="paper",
                inputs=["market.paper.kraken.FET-USD.candles.1d"],
                params={"warmup_n": 3, "polygon_cache_root": str(tmp_path)},
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_skips_when_buffer_smaller_than_lookback(self, tmp_path: Path) -> None:
        """A buffer_size below the required count skips warmup (no silent truncation)."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 5, end=end)
        strat = _WarmupStrategy(
            self._warmup_config(warmup_n=5, buffer_size=3, polygon_cache_root=str(tmp_path))
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_bad_buffer_size_type_is_caught(self, tmp_path: Path) -> None:
        """A misconfigured (non-int) buffer_size never crashes startup (fail-open)."""
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        _write_warmup_cache(tmp_path, 3, end=end)
        strat = _WarmupStrategy(
            self._warmup_config(buffer_size="oops", polygon_cache_root=str(tmp_path))
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_skips_non_1d_input(self, tmp_path: Path) -> None:
        """A non-1d candle input is not warmed (relies on live fill)."""
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                inputs=["market.kraken.FET-USD.candles.1h"],
                params={
                    "warmup_n": 3,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path),
                },
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_missing_cache_warns_no_crash(self, tmp_path: Path) -> None:
        """A missing cache leaves the buffer empty and does not raise."""
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                inputs=["market.kraken.FET-USD.candles.1d"],
                params={
                    "warmup_n": 5,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path / "empty"),
                },
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_unknown_symbol_warns_no_crash(self, tmp_path: Path) -> None:
        """An instrument that is not a BASE-QUOTE pair is skipped without crashing."""
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                inputs=["market.kraken.BTCUSD.candles.1d"],
                params={
                    "warmup_n": 5,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path),
                },
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_skips_non_market_input(self, tmp_path: Path) -> None:
        """A non-market input topic is skipped by the warmup loop."""
        strat = _WarmupStrategy(
            _strategy_config(
                name="w",
                inputs=["candles.kraken.synthetic.1d"],
                params={
                    "warmup_n": 3,
                    "warmup_market_type": "crypto",
                    "polygon_cache_root": str(tmp_path),
                },
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_loader_exception_is_caught(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unexpected loader error is caught (live-only fallback), never raised."""

        def _boom(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("cache read blew up")

        monkeypatch.setattr("snapper.strategies.base.load_recent_grouped_daily", _boom)
        strat = _WarmupStrategy(self._warmup_config(polygon_cache_root=str(tmp_path)))
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_warmup_zero_history_is_noop(self, tmp_path: Path) -> None:
        """A zero warm-up requirement skips the cache entirely."""
        strat = _WarmupStrategy(self._warmup_config(warmup_n=0, polygon_cache_root=str(tmp_path)))
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}

    @pytest.mark.asyncio
    async def test_start_warms_before_subscribe(self) -> None:
        """start() warms the candle buffer BEFORE subscribing (no live-frame race)."""
        strat = _WarmupStrategy(_strategy_config(name="w"))
        order: list[str] = []
        strat._warmup_candle_buffer = AsyncMock(side_effect=lambda: order.append("warmup"))
        strat._subscribe_inputs = AsyncMock(side_effect=lambda: order.append("subscribe"))
        strat._setup_publisher = AsyncMock(side_effect=lambda: order.append("publisher"))
        strat._heartbeat_loop = AsyncMock()
        await strat.start()
        await strat.stop()
        assert order[0] == "warmup"
        assert order.index("warmup") < order.index("subscribe")


class TestWarmupDbFirst:
    """Phase 3 slice 5: warmup reads the persisted 1d plane first (canonical)."""

    def test_db_row_to_warmup_candle_projection(self) -> None:
        """The DB-row projection stamps the warmup envelope and 1d open_at.

        Given: a persisted 1d CandleRow,
        When: _db_row_to_warmup_candle projects it,
        Then: it carries the row's open_at, OHLCV, the warmup session id and 1d TF.
        """
        candle = _db_row_to_warmup_candle(
            _db_candle_row(2.5, day=date(2024, 2, 16)),
            instrument="FET-USD",
            exchange="kraken",
            sequence_id=0,
        )
        assert candle.open_at == datetime(2024, 2, 16, tzinfo=UTC)
        assert candle.timeframe == "1d"
        assert candle.exchange == "kraken"
        assert candle.instrument == "FET-USD"
        assert candle.close == 2.5
        assert candle.session_id == "warmup"

    def _db_config(self, *, inputs: list[str], exchange: str, cache_root: Path) -> StrategyConfig:
        """Build a crypto-opt-in warmup config for the DB-first tests."""
        return _strategy_config(
            name="w",
            exchange=exchange,
            inputs=inputs,
            params={
                "warmup_n": 3,
                "warmup_market_type": "crypto",
                "polygon_cache_root": str(cache_root),
            },
        )

    @pytest.mark.asyncio
    async def test_db_first_warms_from_persisted_plane(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A populated DB warms the buffer canonically (no cache touched).

        Given: the persisted plane holds the full 1d history for the leg,
        When: warmup runs (no Polygon cache written),
        Then: the buffer is filled from the DB under the leg's live venue.
        """
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        rows = [_db_candle_row(float(i), day=end - timedelta(days=2 - i)) for i in range(3)]
        monkeypatch.setattr(
            "snapper.strategies.base.get_repository",
            lambda url: _StubWarmupRepo({"FET-USD": rows}),
        )
        strat = _WarmupStrategy(
            self._db_config(
                inputs=["market.paper.kraken.FET-USD.candles.1d"],
                exchange="paper",
                cache_root=tmp_path,
            )
        )
        await strat._warmup_candle_buffer()
        buffer = strat.candle_buffer["FET-USD"]
        assert [c.close for c in buffer] == [0.0, 1.0, 2.0]
        assert all(c.exchange == "kraken" for c in buffer)

    @pytest.mark.asyncio
    async def test_db_first_non_paper_leg_uses_topic_exchange(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A live (non-paper) leg reads + stamps under the topic's own exchange.

        Given: a live kraken 1d input and a populated DB,
        When: warmup runs,
        Then: the leg resolves under parsed.exchange (kraken) and warms from the DB.
        """
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        rows = [_db_candle_row(float(i), day=end - timedelta(days=2 - i)) for i in range(3)]
        monkeypatch.setattr(
            "snapper.strategies.base.get_repository",
            lambda url: _StubWarmupRepo({"FET-USD": rows}),
        )
        strat = _WarmupStrategy(
            self._db_config(
                inputs=["market.kraken.FET-USD.candles.1d"], exchange="kraken", cache_root=tmp_path
            )
        )
        await strat._warmup_candle_buffer()
        buffer = strat.candle_buffer["FET-USD"]
        assert len(buffer) == 3
        assert all(c.exchange == "kraken" for c in buffer)

    @pytest.mark.asyncio
    async def test_db_first_multi_leg_aligned(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Both legs warm from the DB on shared days.

        Given: the DB holds aligned 1d history for two legs,
        When: warmup runs,
        Then: both buffers are installed from the DB.
        """
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        days = [end - timedelta(days=2 - i) for i in range(3)]
        rows = {
            "FET-USD": [_db_candle_row(float(i), day=days[i]) for i in range(3)],
            "RENDER-USD": [_db_candle_row(float(i) + 10, day=days[i]) for i in range(3)],
        }
        monkeypatch.setattr(
            "snapper.strategies.base.get_repository", lambda url: _StubWarmupRepo(rows)
        )
        strat = _WarmupStrategy(
            self._db_config(
                inputs=[
                    "market.paper.kraken.FET-USD.candles.1d",
                    "market.paper.kraken.RENDER-USD.candles.1d",
                ],
                exchange="paper",
                cache_root=tmp_path,
            )
        )
        await strat._warmup_candle_buffer()
        assert len(strat.candle_buffer["FET-USD"]) == 3
        assert len(strat.candle_buffer["RENDER-USD"]) == 3

    @pytest.mark.asyncio
    async def test_db_short_falls_back_to_cache(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A short persisted plane falls back to the Polygon cache bootstrap.

        Given: the DB holds fewer than `count` rows for the leg but the cache is full,
        When: warmup runs,
        Then: the buffer is warmed from the Polygon cache (non-canonical fallback).
        """
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        short_rows = [_db_candle_row(0.0, day=end)]
        monkeypatch.setattr(
            "snapper.strategies.base.get_repository",
            lambda url: _StubWarmupRepo({"FET-USD": short_rows}),
        )
        _write_warmup_cache(tmp_path, 3, end=end)
        strat = _WarmupStrategy(
            self._db_config(
                inputs=["market.paper.kraken.FET-USD.candles.1d"],
                exchange="paper",
                cache_root=tmp_path,
            )
        )
        await strat._warmup_candle_buffer()
        assert len(strat.candle_buffer["FET-USD"]) == 3

    @pytest.mark.asyncio
    async def test_db_first_misaligned_installs_nothing_without_cache_fallback(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A populated-but-misaligned DB installs nothing and does NOT use the cache.

        Given: both legs have >= count DB rows but on disjoint days, and a full cache,
        When: warmup runs,
        Then: no buffer is installed (live-only) and the cache is never consulted
            (the canonical DB source is used; stale cache must not mask misalignment).
        """
        end = (datetime.now(UTC) - timedelta(days=1)).date()
        fet_days = [end - timedelta(days=2 - i) for i in range(3)]
        render_days = [end - timedelta(days=12 - i) for i in range(3)]
        rows = {
            "FET-USD": [_db_candle_row(float(i), day=fet_days[i]) for i in range(3)],
            "RENDER-USD": [_db_candle_row(float(i), day=render_days[i]) for i in range(3)],
        }
        monkeypatch.setattr(
            "snapper.strategies.base.get_repository", lambda url: _StubWarmupRepo(rows)
        )
        _write_warmup_cache(tmp_path, 5, tickers=("X:FETUSD", "X:RENDERUSD"), end=end)
        strat = _WarmupStrategy(
            self._db_config(
                inputs=[
                    "market.paper.kraken.FET-USD.candles.1d",
                    "market.paper.kraken.RENDER-USD.candles.1d",
                ],
                exchange="paper",
                cache_root=tmp_path,
            )
        )
        await strat._warmup_candle_buffer()
        assert strat.candle_buffer == {}
