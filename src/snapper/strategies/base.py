"""Base strategy framework.

This module provides the abstract base classes for implementing
trading strategies with ZMQ-based messaging.

Data models (StrategySignal, StrategyConfig) are defined in
snapper.strategies.models and re-exported here for backwards
compatibility.
"""

import asyncio
import contextlib
import logging
import time
from abc import ABC
from abc import abstractmethod
from datetime import UTC
from datetime import datetime
from typing import Any

import zmq
import zmq.asyncio

from snapper.api.schemas.base import StrictDataSchema
from snapper.application.services.signals.service import signal_service
from snapper.config.settings import get_bootstrap_settings
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import parse_market_topic
from snapper.messaging.topics.builders import signal_topic
from snapper.strategies.health import StrategyHealthMonitor
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal
from snapper.strategies.system_events import SystemMessageRouter

__all__ = ["BaseStrategy", "CompositeStrategy", "StrategySignal", "StrategyConfig"]


logger = logging.getLogger(__name__)
_bootstrap_settings = get_bootstrap_settings()


async def _close_resource_async(resource: Any) -> None:
    """Close a strategy resource and await async close methods when needed.

    Args:
        resource: Resource exposing a close method.
    """
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    result = close()
    if asyncio.iscoroutine(result):
        await result


def _close_resource_sync(resource: Any) -> None:
    """Close a strategy resource during sync cleanup paths.

    Args:
        resource: Resource exposing a close method.
    """
    close = getattr(resource, "close", None)
    if not callable(close):
        return
    result = close()
    if asyncio.iscoroutine(result):
        result.close()


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
                self.output_topics.append(signal_topic(self.exchange, instrument, self.name))
            else:
                self.output_topics.append(signal_topic(self.exchange, instrument, "live"))
        logger.info(
            f"Strategy {self.name} initialized with {len(self.output_topics)} output topics: {self.output_topics}"
        )
        self.zmq_context: zmq.asyncio.Context | None = None
        self.subscriber: ValidatedSubscriber | None = None
        self.publisher: ValidatedPublisher | None = None
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self._gap_detector: GapDetector = GapDetector(f"strategy.{config.name}")
        self.candle_buffer: dict[str, list[CandleData]] = {}
        self._listen_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self.last_data_timestamp: float = time.time()
        self.heartbeat_seq: int = 0
        self._feed_heartbeats: dict[str, dict[str, Any]] = {}
        self._last_data_ts: float | None = None
        self._is_paper_input = any(StrategyConfig._is_paper_or_replay(inp) for inp in self.inputs)
        self._health_monitor = StrategyHealthMonitor(self)
        self._system_router = SystemMessageRouter(self)

    @property
    def is_running(self) -> bool:
        """Check if the strategy is currently running.

        Returns:
            True if the strategy is running, False otherwise.
        """
        return self._running

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Handle incoming candle data.

        Args:
            instrument: The instrument symbol.
            candle: The candle data with OHLCV data.

        Returns:
            Optional signal if strategy logic triggers.
        """
        await asyncio.sleep(0)
        return None

    async def on_tick(self, instrument: str, tick: TickData) -> StrategySignal | None:
        """Handle incoming tick data.

        Args:
            instrument: The instrument symbol.
            tick: The tick data with bid/ask data.

        Returns:
            Optional signal if strategy logic triggers.
        """
        await asyncio.sleep(0)
        return None

    async def on_trade(self, instrument: str, trade: TradeData) -> StrategySignal | None:
        """Handle incoming trade data.

        Args:
            instrument: The instrument symbol.
            trade: The trade data with trade details.

        Returns:
            Optional signal if strategy logic triggers.
        """
        await asyncio.sleep(0)
        return None

    @abstractmethod
    async def reset(self) -> None:
        """Reset strategy state for replay or reinitialization."""
        ...

    async def _setup_publisher(self) -> None:
        """Set up the ZMQ publisher socket for signal emission."""
        if not self.publisher:
            assert self.zmq_context is not None, "ZMQ context must be initialized in start()"
            raw_pub_socket = self.zmq_context.socket(zmq.PUB)
            apply_hwm(raw_pub_socket, sndhwm=HWM_MARKET_DATA)
            broker_addr = _bootstrap_settings.zmq_broker_xsub
            logger.info(f"Strategy {self.name}: Connecting publisher to broker {broker_addr}")
            raw_pub_socket.connect(broker_addr)
            self.publisher = ValidatedPublisher(raw_pub_socket)
            self.msg_publisher = MessagePublisher(self.publisher, self._tracker)
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

    def _handle_settings_update(self, envelope: SettingChangedData) -> None:
        """Handle dynamic settings update from ZMQ.

        Args:
            envelope: Settings change data with key and value.
        """
        self._system_router.handle_settings_update(envelope)

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
            await _close_resource_async(self.publisher)
            self.publisher = None
        if self.zmq_context:
            self.zmq_context.term()
            self.zmq_context = None

    def __del__(self) -> None:
        """Attempt to release ZMQ resources during garbage collection."""
        heartbeat_task = getattr(self, "_heartbeat_task", None)
        if heartbeat_task is not None and not heartbeat_task.done():
            with contextlib.suppress(Exception):
                heartbeat_task.cancel()
        listen_task = getattr(self, "_listen_task", None)
        if listen_task is not None and not listen_task.done():
            with contextlib.suppress(Exception):
                listen_task.cancel()
        subscriber = getattr(self, "subscriber", None)
        if subscriber is not None:
            with contextlib.suppress(Exception):
                _close_resource_sync(subscriber)
            self.subscriber = None
        publisher = getattr(self, "publisher", None)
        if publisher is not None:
            with contextlib.suppress(Exception):
                _close_resource_sync(publisher)
            self.publisher = None
        zmq_context = getattr(self, "zmq_context", None)
        if zmq_context is not None:
            destroy = getattr(zmq_context, "destroy", None)
            if callable(destroy):
                with contextlib.suppress(Exception):
                    destroy(linger=0)
            else:
                with contextlib.suppress(Exception):
                    zmq_context.term()
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
        subscribed_heartbeats: set[str] = set()
        for topic in self.inputs:
            if not topic.startswith("market."):
                continue
            parsed = parse_market_topic(topic)
            if parsed is None:
                continue
            if parsed.exchange == "paper" and parsed.source_exchange:
                heartbeat_topic = f"system.heartbeats.feed.paper.{parsed.source_exchange}"
            else:
                heartbeat_topic = f"system.heartbeats.feed.{parsed.exchange}"
            if heartbeat_topic in subscribed_heartbeats:
                continue
            logger.info(
                f"Strategy {self.name}: Subscribing to {heartbeat_topic} for feed health monitoring"
            )
            self.subscriber.subscribe(heartbeat_topic)
            subscribed_heartbeats.add(heartbeat_topic)

    async def _subscribe_inputs(self) -> None:
        """Subscribe to input topics via ZMQ."""
        if self.subscriber:
            await asyncio.sleep(0)
            return
        assert self.zmq_context is not None, "ZMQ context must be initialized in start()"
        raw_sub_socket = self.zmq_context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_MARKET_DATA)
        self._connect_subscriber_socket(raw_sub_socket)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        for topic in self.inputs:
            logger.info(f"Strategy {self.name}: Subscribing to {topic}")
            self.subscriber.subscribe(topic)
        logger.info(f"Strategy {self.name}: Subscribing to system.symbol_aliases")
        self.subscriber.subscribe("system.symbol_aliases")
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
            await _close_resource_async(self.subscriber)
            self.subscriber = None
        await asyncio.sleep(0)

    def _handle_system_heartbeat(self, topic_str: str, payload_str: str) -> None:
        """Handle feed heartbeat system message.

        Args:
            topic_str: The ZMQ topic string.
            payload_str: The JSON payload string.
        """
        self._system_router.handle_system_heartbeat(topic_str, payload_str)

    def _handle_symbol_aliases_update(self) -> None:
        """Handle symbol_aliases system message by refreshing cache."""
        self._system_router.handle_symbol_aliases_update()

    async def _handle_replay_start(self, payload_str: str) -> None:
        """Handle replay start system message.

        Args:
            payload_str: The JSON payload string.
        """
        await self._system_router.handle_replay_start(payload_str)

    def _handle_replay_end(self, payload_str: str) -> None:
        """Handle replay end system message.

        Args:
            payload_str: The JSON payload string.
        """
        self._system_router.handle_replay_end(payload_str)

    async def _handle_system_message(self, topic_str: str, payload_str: str) -> None:
        """Route a system message to the appropriate handler.

        Args:
            topic_str: The ZMQ topic string.
            payload_str: The JSON payload string.
        """
        if topic_str == "system.symbol_aliases":
            self._handle_symbol_aliases_update()
        elif topic_str == "system.settings":
            envelope = SettingChangedData.from_json(payload_str)
            self._handle_settings_update(envelope)
        elif topic_str.startswith("system.heartbeats.feed."):
            self._handle_system_heartbeat(topic_str, payload_str)
        elif topic_str == "system.replay.start":
            await self._handle_replay_start(payload_str)
        elif topic_str == "system.replay.end":
            self._handle_replay_end(payload_str)

    def _check_gap_parsed(self, topic: str, payload_str: str) -> None:
        """Run gap detection using typed message parsing.

        Parses the payload via parse_message to get session_id and sequence_id.
        Silently skips messages that fail parsing (e.g. unknown types).
        """
        try:
            msg = parse_message(payload_str)
            self._gap_detector.check(topic, msg.session_id, msg.sequence_id)
        except MessageParseError:
            pass

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
                self._check_gap_parsed(topic_str, payload_str)
                if topic_str.startswith("system."):
                    await self._handle_system_message(topic_str, payload_str)
                    continue
                if topic_str.startswith("market."):
                    parsed = parse_market_topic(topic_str)
                    if parsed is None:
                        logger.warning(f"Strategy {self.name}: Malformed market topic: {topic_str}")
                        continue
                    instrument = parsed.instrument
                    signal = await self._dispatch_market_data(topic_str, instrument, payload_str)
                    if signal:
                        await self.emit_signal(signal)
        except asyncio.CancelledError:
            logger.info(f"Strategy {self.name}: Listen loop cancelled")
            raise
        except Exception as e:
            logger.error(f"Strategy {self.name}: Error in listen loop: {e}", exc_info=True)
            self._running = False

    async def _handle_candle_data(self, instrument: str, payload: str) -> StrategySignal | None:
        """Handle incoming candle data.

        Args:
            instrument: The instrument symbol.
            payload: The JSON payload string.

        Returns:
            Optional signal from the candle handler.
        """
        candle = CandleData.from_json(payload)
        self._last_data_ts = candle.open_at.timestamp()
        if instrument not in self.candle_buffer:
            self.candle_buffer[instrument] = []
        self.candle_buffer[instrument].append(candle)
        max_buffer_size = self.params.get("buffer_size", 100)
        if len(self.candle_buffer[instrument]) > max_buffer_size:
            self.candle_buffer[instrument].pop(0)
        return await self.on_candle(instrument, candle)

    async def _dispatch_market_data(
        self, topic: str, instrument: str, payload: str
    ) -> StrategySignal | None:
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
            tick = TickData.from_json(payload)
            self._last_data_ts = tick.timestamp.timestamp()
            return await self.on_tick(instrument, tick)
        if ".trades" in topic:
            trade = TradeData.from_json(payload)
            self._last_data_ts = (trade.executed_at or trade.timestamp).timestamp()
            return await self.on_trade(instrument, trade)
        logger.warning(f"Strategy {self.name}: Unknown market data topic type: {topic}")
        return None

    async def emit_signal(self, signal: StrategySignal) -> None:
        """Emit a trading signal to the output topic.

        Args:
            signal: The signal to emit.

        Raises:
            ValueError: If signal instrument is not in configured outputs.
        """
        if signal.timestamp is None:
            ts = self._last_data_ts or time.time()
            signal.timestamp = datetime.fromtimestamp(ts, tz=UTC)
        if self.exchange == "paper":
            topic = signal_topic(self.exchange, signal.instrument, self.name)
        else:
            topic = signal_topic(self.exchange, signal.instrument, "live")
        if topic not in self.output_topics:
            raise ValueError(
                f"Strategy {self.name}: Signal for instrument '{signal.instrument}' not allowed. "
                f"Instrument not in configured outputs: {self.outputs}. "
                f"Generated topic: {topic}"
            )
        if not self.msg_publisher:
            await self._setup_publisher()
        signal_envelope = SignalData(
            instrument=signal.instrument,
            side=signal.side,
            strength=signal.strength,
            reason=signal.reason,
            price=signal.price,
            exchange=self.exchange,
            strategy_name=self.name,
            fired_at=signal.timestamp or datetime.now(UTC),
        )

        stamped: StrictDataSchema | None = None
        if self.msg_publisher is not None:
            stamped = await self.msg_publisher.publish(signal_envelope)
        await signal_service.store_signal(
            signal,
            exchange=self.exchange,
            strategy_name=self.name,
            price=signal.price,
            session_id=stamped.session_id if stamped else None,
            sequence_id=stamped.sequence_id if stamped else None,
            public_id=stamped.public_id if stamped else None,
            timestamp=stamped.timestamp if stamped else None,
            tracker=self._tracker,
        )
        logger.debug(
            f"Strategy {self.name}: Signal {signal.side.upper()} {signal.instrument} "
            f"(strength={signal.strength:.2f}, price={signal.price:.2f}) -> {topic}"
        )

    async def _heartbeat_loop(self) -> None:
        """Background loop for emitting strategy heartbeats."""
        await self._health_monitor.heartbeat_loop()


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
