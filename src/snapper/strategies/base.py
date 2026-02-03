"""Base strategy framework.

This module provides the abstract base classes and data structures for
implementing trading strategies with ZMQ-based messaging.
"""

import asyncio
import contextlib
import logging
import time
from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Any

import zmq
import zmq.asyncio

from snapper.application.services.settings import SettingsService
from snapper.config.settings import get_bootstrap_settings
from snapper.core.types import TradeSide
from snapper.infrastructure.symbols.functions import TradingExchange
from snapper.infrastructure.symbols.functions import _get_db_mapper
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import get_available_kraken_symbols
from snapper.infrastructure.symbols.functions import get_available_symbols
from snapper.infrastructure.symbols.functions import get_available_walutomat_symbols
from snapper.infrastructure.symbols.functions import get_available_zonda_symbols
from snapper.interface.websocket.schemas import HealthStatus
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.messages import BarEnvelope
from snapper.messaging.schemas.messages import HeartbeatEnvelope
from snapper.messaging.schemas.messages import ReplayEndEnvelope
from snapper.messaging.schemas.messages import ReplayStartEnvelope
from snapper.messaging.schemas.messages import SettingChangedEnvelope
from snapper.messaging.schemas.messages import SignalEnvelope
from snapper.messaging.schemas.messages import TickEnvelope
from snapper.messaging.schemas.messages import TradeEnvelope

logger = logging.getLogger(__name__)
_bootstrap_settings = get_bootstrap_settings()
_bootstrap_settings = get_bootstrap_settings()


@dataclass
class Signal:
    """Trading signal emitted by a strategy.

    Attributes:
        instrument: The trading instrument symbol.
        side: Trade direction ('buy' or 'sell').
        strength: Signal strength from 0.0 to 1.0.
        reason: Human-readable reason for the signal.
        price: Price at which signal was generated.
        timestamp: Signal generation timestamp (Unix epoch).
        metadata: Additional signal metadata.
    """

    instrument: str
    side: TradeSide
    strength: float
    reason: str
    price: float
    timestamp: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _get_exchange_instrument_resolver(
    exchange: str,
) -> tuple[Callable[[], list[str]], str] | None:
    """Look up the instrument resolver and label for an exchange.

    Args:
        exchange: Exchange name to look up.

    Returns:
        Tuple of (resolver_callable, label) or None if exchange unknown.
    """
    resolvers: dict[str, tuple[Callable[[], list[str]], str]] = {
        "paper": (get_available_symbols, "Paper (all exchanges)"),
        "walutomat": (get_available_walutomat_symbols, "Walutomat (FX pairs only)"),
        "zonda": (get_available_zonda_symbols, "Zonda"),
        "kraken": (get_available_kraken_symbols, "Kraken"),
    }
    return resolvers.get(exchange)


@dataclass
class StrategyConfig:
    """Configuration for a trading strategy.

    Attributes:
        name: Unique strategy instance name.
        strategy_class: Name of the strategy class to instantiate.
        inputs: List of ZMQ topics to subscribe to.
        outputs: List of output instruments for signals.
        exchange: Target exchange for order execution.
        params: Strategy-specific parameters.
    """

    name: str
    strategy_class: str
    inputs: list[str]
    outputs: list[str]
    exchange: TradingExchange = "paper"
    params: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def _is_paper_or_replay(topic: str) -> bool:
        """Check if a topic string refers to paper or replay data.

        Args:
            topic: Input topic string.

        Returns:
            True if the topic contains 'paper' or 'replay'.
        """
        lowered = topic.lower()
        return "paper" in lowered or "replay" in lowered

    def _validate_paper_inputs(self) -> None:
        """Validate paper/replay input consistency with exchange setting."""
        has_paper_input = any(self._is_paper_or_replay(inp) for inp in self.inputs)
        if not has_paper_input:
            return
        if self.exchange != "paper":
            raise ValueError(
                f"Strategy {self.name}: Paper/replay input data MUST use exchange='paper'. "
                f"Found inputs: {self.inputs}, exchange: {self.exchange}"
            )
        all_paper = all(self._is_paper_or_replay(inp) for inp in self.inputs)
        if not all_paper:
            raise ValueError(
                f"Strategy {self.name}: Cannot mix paper/replay and live inputs. "
                f"All inputs must be paper/replay OR all must be live (no mixing). "
                f"Found inputs: {self.inputs}"
            )

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.name:
            raise ValueError("Strategy name cannot be empty")
        if not self.inputs:
            raise ValueError(f"Strategy {self.name} must have at least one input")
        if not self.outputs:
            raise ValueError(f"Strategy {self.name} must define at least one output instrument")
        valid_exchanges = get_available_exchanges()
        if self.exchange not in valid_exchanges:
            raise ValueError(
                f"Strategy {self.name}: exchange must be one of {valid_exchanges}, "
                f"got '{self.exchange}'"
            )
        self._validate_output_instruments()
        self._validate_paper_inputs()

    def _validate_output_instruments(self) -> None:
        """Validate that output instruments are valid for the exchange."""
        resolver = _get_exchange_instrument_resolver(self.exchange)
        if resolver is None:
            return
        get_instruments, exchange_label = resolver
        valid_instruments = get_instruments()
        if not valid_instruments:
            logger.warning(
                f"Strategy {self.name}: No symbols loaded for {exchange_label}, "
                "skipping output validation (DB may be empty)"
            )
            return
        invalid_instruments = [inst for inst in self.outputs if inst not in valid_instruments]
        if invalid_instruments:
            raise ValueError(
                f"Strategy {self.name}: Invalid output instruments for {exchange_label}: "
                f"{invalid_instruments}. Valid instruments: {valid_instruments[:20]}..."
            )


class BaseStrategy(ABC):
    """Abstract base class for trading strategies.

    Provides ZMQ-based market data subscription, signal publishing,
    heartbeat monitoring, and lifecycle management.

    Attributes:
        config: Strategy configuration.
        name: Strategy instance name.
        inputs: List of input topics.
        outputs: List of output instruments.
        exchange: Target exchange.
        params: Strategy parameters.
        output_topics: Generated output topic names.
        candle_buffer: Buffer of recent candles per instrument.
    """

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize the strategy.

        Args:
            config: Strategy configuration.
        """
        self.config = config
        self.name = config.name
        self.inputs = config.inputs
        self.outputs = config.outputs
        self.exchange = config.exchange
        self.params = config.params
        self._running = False
        self.output_topics: list[str] = []
        for instrument in self.outputs:
            if self.exchange == "paper":
                topic = f"signals.paper.{instrument}.{self.name}"
            else:
                topic = f"signals.{self.exchange}.{instrument}.live"
            self.output_topics.append(topic)
        logger.info(
            f"Strategy {self.name} initialized with {len(self.output_topics)} output topics: {self.output_topics}"
        )
        self.zmq_context: zmq.asyncio.Context | None = None
        self.subscriber: ValidatedSubscriber | None = None
        self.publisher: ValidatedPublisher | None = None
        self.candle_buffer: dict[str, list[BarEnvelope]] = {}
        self._listen_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self.last_data_timestamp: float = time.time()
        self.heartbeat_seq: int = 0
        self._feed_heartbeats: dict[str, dict[str, Any]] = {}
        self._last_data_ts: float | None = None
        self._is_paper_input = any(StrategyConfig._is_paper_or_replay(inp) for inp in self.inputs)

    @property
    def is_running(self) -> bool:
        """Check if the strategy is currently running.

        Returns:
            True if the strategy is running, False otherwise.
        """
        return self._running

    async def on_bar(self, instrument: str, bar: BarEnvelope) -> Signal | None:
        """Handle incoming bar/candle data.

        Args:
            instrument: The instrument symbol.
            bar: The bar envelope with OHLCV data.

        Returns:
            Optional signal if strategy logic triggers.
        """
        await asyncio.sleep(0)
        return None

    async def on_tick(self, instrument: str, tick: TickEnvelope) -> Signal | None:
        """Handle incoming tick data.

        Args:
            instrument: The instrument symbol.
            tick: The tick envelope with bid/ask data.

        Returns:
            Optional signal if strategy logic triggers.
        """
        await asyncio.sleep(0)
        return None

    async def on_trade(self, instrument: str, trade: TradeEnvelope) -> Signal | None:
        """Handle incoming trade data.

        Args:
            instrument: The instrument symbol.
            trade: The trade envelope with trade data.

        Returns:
            Optional signal if strategy logic triggers.
        """
        await asyncio.sleep(0)
        return None

    @abstractmethod
    async def reset(self) -> None:
        """Reset strategy state for replay or reinitialization."""
        raise NotImplementedError

    async def _setup_publisher(self) -> None:
        """Set up the ZMQ publisher socket for signal emission."""
        if not self.publisher:
            assert self.zmq_context is not None, "ZMQ context must be initialized in start()"
            raw_pub_socket = self.zmq_context.socket(zmq.PUB)
            broker_addr = _bootstrap_settings.zmq_broker_xsub
            logger.info(f"Strategy {self.name}: Connecting publisher to broker {broker_addr}")
            raw_pub_socket.connect(broker_addr)
            self.publisher = ValidatedPublisher(raw_pub_socket)
            logger.info(f"Strategy {self.name}: Publisher initialized successfully")
        await asyncio.sleep(0)

    async def start(self) -> None:
        """Start the strategy and begin processing market data."""
        self._running = True
        if not self.zmq_context:
            self.zmq_context = zmq.asyncio.Context()
            logger.info(f"Strategy {self.name}: ZMQ context created")
        await self._subscribe_inputs()
        await self._setup_publisher()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _handle_settings_update(self, envelope: SettingChangedEnvelope) -> None:
        """Handle dynamic settings update from ZMQ.

        Args:
            envelope: Settings change envelope with key and value.
        """
        try:
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(envelope.value)
                settings_service._cache[envelope.key] = parsed_value
                logger.info(f"Strategy {self.name}: Setting {envelope.key} updated via ZMQ event")
        except Exception as e:
            logger.error(f"Strategy {self.name}: Error handling settings update: {e}")

    async def stop(self) -> None:
        """Stop the strategy and clean up resources."""
        self._running = False
        await self._unsubscribe_inputs()
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        if self.publisher:
            self.publisher.setsockopt(zmq.LINGER, 0)
            self.publisher.close()
            self.publisher = None
        if self.zmq_context:
            self.zmq_context.term()
            self.zmq_context = None

    def _connect_subscriber_socket(self, raw_sub_socket: zmq.asyncio.Socket) -> None:
        """Connect the subscriber socket to broker or direct feed.

        Args:
            raw_sub_socket: ZMQ subscriber socket to connect.
        """
        use_broker = self.params.get("use_broker", True)
        if use_broker:
            broker_addr = _bootstrap_settings.zmq_broker_xpub
            logger.info(f"Strategy {self.name}: Connecting subscriber to broker {broker_addr}")
            raw_sub_socket.connect(broker_addr)
        else:
            feed_addr = self.params.get("feed_addr", "tcp://127.0.0.1:5555")
            logger.info(f"Strategy {self.name}: Connecting subscriber to feed {feed_addr}")
            raw_sub_socket.connect(feed_addr)

    def _subscribe_feed_heartbeats(self) -> None:
        """Subscribe to heartbeat topics for exchanges in input market topics."""
        assert self.subscriber is not None
        subscribed_exchanges: set[str] = set()
        for topic in self.inputs:
            if not topic.startswith("market."):
                continue
            parts = topic.split(".")
            if len(parts) < 2:
                continue
            exchange = parts[1]
            if exchange in subscribed_exchanges:
                continue
            heartbeat_topic = f"system.heartbeats.feed.{exchange}"
            logger.info(
                f"Strategy {self.name}: Subscribing to {heartbeat_topic} for feed health monitoring"
            )
            self.subscriber.subscribe(heartbeat_topic)
            subscribed_exchanges.add(exchange)

    async def _subscribe_inputs(self) -> None:
        """Subscribe to input topics via ZMQ."""
        if self.subscriber:
            await asyncio.sleep(0)
            return
        assert self.zmq_context is not None, "ZMQ context must be initialized in start()"
        raw_sub_socket = self.zmq_context.socket(zmq.SUB)
        self._connect_subscriber_socket(raw_sub_socket)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        for topic in self.inputs:
            logger.info(f"Strategy {self.name}: Subscribing to {topic}")
            self.subscriber.subscribe(topic)
        logger.info(f"Strategy {self.name}: Subscribing to system.symbol_mappings")
        self.subscriber.subscribe("system.symbol_mappings")
        logger.info(f"Strategy {self.name}: Subscribing to system.settings")
        self.subscriber.subscribe("system.settings")
        self._subscribe_feed_heartbeats()
        if self._is_paper_input:
            logger.info(
                f"Strategy {self.name}: Auto-subscribing to system.replay. (paper input detected)"
            )
            self.subscriber.subscribe("system.replay.")
        self._listen_task = asyncio.create_task(self._listen_loop())
        await asyncio.sleep(0)

    async def _unsubscribe_inputs(self) -> None:
        """Unsubscribe from all input topics."""
        if self.subscriber:
            self.subscriber.close()
            self.subscriber = None
        await asyncio.sleep(0)

    def _handle_system_heartbeat(self, topic_str: str, payload_str: str) -> None:
        """Handle feed heartbeat system message.

        Args:
            topic_str: The ZMQ topic string.
            payload_str: The JSON payload string.
        """
        exchange = topic_str.replace("system.heartbeats.feed.", "")
        heartbeat = HeartbeatEnvelope.from_json(payload_str)
        self._feed_heartbeats[exchange] = {
            "timestamp": time.time(),
            "status": heartbeat.status,
            "lag_ms": heartbeat.lag_ms,
            "component": heartbeat.component,
            "symbol_count": heartbeat.meta.get("symbol_count", 0),
        }
        logger.debug(
            f"Strategy {self.name}: Received heartbeat from feed.{exchange}, "
            f"status={heartbeat.status}, lag={heartbeat.lag_ms}ms, "
            f"symbols={heartbeat.meta.get('symbol_count', 0)}"
        )

    def _handle_symbol_mappings_update(self) -> None:
        """Handle symbol_mappings system message by refreshing cache."""
        logger.info(f"Strategy {self.name}: Received symbol_mappings update, refreshing cache")
        _get_db_mapper().trigger_cache_invalidation(fail_fast=False)

    async def _handle_replay_start(self, payload_str: str) -> None:
        """Handle replay start system message.

        Args:
            payload_str: The JSON payload string.
        """
        replay_envelope = ReplayStartEnvelope.from_json(payload_str)
        logger.info(f"Strategy {self.name}: Replay started, resetting state")
        await self.reset()
        self._last_data_ts = (
            replay_envelope.started_at.timestamp() if replay_envelope.started_at else None
        )

    def _handle_replay_end(self, payload_str: str) -> None:
        """Handle replay end system message.

        Args:
            payload_str: The JSON payload string.
        """
        ReplayEndEnvelope.from_json(payload_str)
        logger.info(f"Strategy {self.name}: Replay ended")
        self._last_data_ts = None

    async def _handle_system_message(self, topic_str: str, payload_str: str) -> None:
        """Handle a system-category message.

        Args:
            topic_str: The ZMQ topic string.
            payload_str: The JSON payload string.
        """
        if topic_str == "system.symbol_mappings":
            self._handle_symbol_mappings_update()
        elif topic_str == "system.settings":
            envelope = SettingChangedEnvelope.from_json(payload_str)
            self._handle_settings_update(envelope)
        elif topic_str.startswith("system.heartbeats.feed."):
            self._handle_system_heartbeat(topic_str, payload_str)
        elif topic_str == "system.replay.start":
            await self._handle_replay_start(payload_str)
        elif topic_str == "system.replay.end":
            self._handle_replay_end(payload_str)

    async def _listen_loop(self) -> None:
        """Main loop for receiving and processing market data."""
        if not self.subscriber:
            logger.error(f"Strategy {self.name}: No subscriber socket in listen loop")
            return
        logger.info(f"Strategy {self.name}: Starting listen loop")
        try:
            while self._running:
                topic_str, payload = await self.subscriber.recv_multipart()
                payload_str = payload.decode()
                self.last_data_timestamp = time.time()
                if topic_str.startswith("system."):
                    await self._handle_system_message(topic_str, payload_str)
                    continue
                if topic_str.startswith("market."):
                    parts = topic_str.split(".")
                    instrument = parts[2] if len(parts) > 2 else topic_str
                    signal = await self._dispatch_market_data(topic_str, instrument, payload_str)
                    if signal:
                        await self.emit_signal(signal)
        except asyncio.CancelledError:
            logger.info(f"Strategy {self.name}: Listen loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Strategy {self.name}: Error in listen loop: {e}", exc_info=True)
            self._running = False

    async def _handle_candle_data(self, instrument: str, payload: str) -> Signal | None:
        """Handle incoming candle bar data.

        Args:
            instrument: The instrument symbol.
            payload: The JSON payload string.

        Returns:
            Optional signal from the bar handler.
        """
        bar = BarEnvelope.from_json(payload)
        self._last_data_ts = bar.timestamp.timestamp()
        if instrument not in self.candle_buffer:
            self.candle_buffer[instrument] = []
        self.candle_buffer[instrument].append(bar)
        max_buffer_size = self.params.get("buffer_size", 100)
        if len(self.candle_buffer[instrument]) > max_buffer_size:
            self.candle_buffer[instrument].pop(0)
        return await self.on_bar(instrument, bar)

    async def _dispatch_market_data(
        self, topic: str, instrument: str, payload: str
    ) -> Signal | None:
        """Dispatch market data to appropriate handler.

        Args:
            topic: The ZMQ topic string.
            instrument: The instrument symbol.
            payload: The JSON payload string.

        Returns:
            Optional signal from the handler.
        """
        if ".candles." in topic:
            return await self._handle_candle_data(instrument, payload)
        if ".ticks" in topic:
            tick = TickEnvelope.from_json(payload)
            self._last_data_ts = tick.timestamp.timestamp()
            return await self.on_tick(instrument, tick)
        if ".trades" in topic:
            trade = TradeEnvelope.from_json(payload)
            self._last_data_ts = trade.timestamp.timestamp()
            return await self.on_trade(instrument, trade)
        logger.warning(f"Strategy {self.name}: Unknown market data topic type: {topic}")
        return None

    async def emit_signal(self, signal: Signal) -> None:
        """Emit a trading signal to the output topic.

        Args:
            signal: The signal to emit.

        Raises:
            ValueError: If signal instrument is not in configured outputs.
        """
        if signal.timestamp is None:
            signal.timestamp = self._last_data_ts or time.time()
        if self.exchange == "paper":
            topic = f"signals.paper.{signal.instrument}.{self.name}"
        else:
            topic = f"signals.{self.exchange}.{signal.instrument}.live"
        if topic not in self.output_topics:
            raise ValueError(
                f"Strategy {self.name}: Signal for instrument '{signal.instrument}' not allowed. "
                f"Instrument not in configured outputs: {self.outputs}. "
                f"Generated topic: {topic}"
            )
        if not self.publisher:
            await self._setup_publisher()
        signal_envelope = SignalEnvelope(
            instrument=signal.instrument,
            side=signal.side,
            strength=signal.strength,
            reason=signal.reason,
            price=signal.price,
            exchange=self.exchange,
            strategy_name=self.name,
            timestamp=datetime.fromtimestamp(signal.timestamp, tz=UTC),
            meta=signal.metadata,
        )
        payload_bytes = signal_envelope.to_json().encode("utf-8")
        if self.publisher is not None:
            await self.publisher.send_multipart(topic, payload_bytes)
        logger.debug(
            f"Strategy {self.name}: Signal {signal.side.upper()} {signal.instrument} "
            f"(strength={signal.strength:.2f}, price={signal.price:.2f}) -> {topic}"
        )

    @staticmethod
    def _classify_health_status(lag_ms: int) -> HealthStatus:
        """Classify health status based on data lag.

        Args:
            lag_ms: Milliseconds since last data received.

        Returns:
            Health status string.
        """
        if lag_ms < 2000:
            return "healthy"
        if lag_ms < 10000:
            return "warning"
        return "error"

    def _build_feed_health(self) -> dict[str, dict[str, Any]] | None:
        """Build feed health summary from cached heartbeats.

        Returns:
            Feed health dict or None if no heartbeats collected.
        """
        if not self._feed_heartbeats:
            return None
        current_time = time.time()
        feed_health: dict[str, dict[str, Any]] = {}
        for feed_key, heartbeat in self._feed_heartbeats.items():
            age_ms = int((current_time - heartbeat["timestamp"]) * 1000)
            feed_health[feed_key] = {
                "status": heartbeat["status"],
                "lag_ms": heartbeat["lag_ms"],
                "heartbeat_age_ms": age_ms,
                "healthy": age_ms < 5000,
            }
        return feed_health

    def _build_heartbeat_envelope(self, lag_ms: int) -> HeartbeatEnvelope:
        """Build a heartbeat envelope with current strategy state.

        Args:
            lag_ms: Milliseconds since last data received.

        Returns:
            HeartbeatEnvelope ready for publishing.
        """
        return HeartbeatEnvelope(
            component=f"strategy_{self.name}",
            sequence=self.heartbeat_seq,
            status=self._classify_health_status(lag_ms),
            lag_ms=lag_ms,
            meta={
                "inputs": self.inputs,
                "outputs": self.outputs,
                "output_topics": self.output_topics,
                "running": self._running,
                "feed_health": self._build_feed_health(),
            },
        )

    async def _heartbeat_loop(self) -> None:
        """Background loop for emitting strategy heartbeats."""
        await asyncio.sleep(1.0)
        try:
            while self._running:
                await asyncio.sleep(2.0)
                try:
                    self.heartbeat_seq += 1
                    lag_ms = int((time.time() - self.last_data_timestamp) * 1000)
                    hb_msg = self._build_heartbeat_envelope(lag_ms)
                    if self.publisher:
                        topic = f"system.heartbeats.strategy.{self.name}"
                        payload = hb_msg.to_json().encode()
                        await self.publisher.send_multipart(topic, payload)
                except Exception as e:
                    logger.error(f"Strategy {self.name}: Heartbeat error: {e}")
        except asyncio.CancelledError:
            logger.info(f"Strategy {self.name}: Heartbeat loop cancelled")
            raise


class CompositeStrategy(BaseStrategy):
    """Strategy that combines multiple sub-strategies.

    Allows hierarchical strategy composition where sub-strategy
    outputs can be used as inputs to the composite.

    Attributes:
        sub_strategies: List of child strategies.
    """

    def __init__(self, config: StrategyConfig, sub_strategies: list[BaseStrategy] | None = None):
        """Initialize composite strategy.

        Args:
            config: Strategy configuration.
            sub_strategies: Optional list of child strategies.
        """
        super().__init__(config)
        self.sub_strategies = sub_strategies or []

    async def add_sub_strategy(self, strategy: BaseStrategy) -> None:
        """Add a sub-strategy to the composite.

        Args:
            strategy: The strategy to add.

        Raises:
            ValueError: If sub-strategy outputs don't match composite inputs.
        """
        if not any(topic in self.inputs for topic in strategy.output_topics):
            raise ValueError(
                f"None of sub-strategy output topics {strategy.output_topics} found in composite inputs {self.inputs}"
            )
        self.sub_strategies.append(strategy)
        await asyncio.sleep(0)

    async def reset(self) -> None:
        """Reset composite and all sub-strategies for replay."""
        for sub_strategy in self.sub_strategies:
            await sub_strategy.reset()
        logger.info(f"CompositeStrategy {self.name} and all sub-strategies reset for replay")
