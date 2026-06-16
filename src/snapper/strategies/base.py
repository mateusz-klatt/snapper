"""Base strategy framework.

This module provides the abstract base classes for implementing
trading strategies with ZMQ-based messaging.

Data models (StrategySignal, StrategyConfig) are defined in
snapper.strategies.models and re-exported here for compatibility with
existing imports.
"""

import asyncio
import contextlib
import logging
import time
from abc import ABC
from abc import abstractmethod
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast
from uuid import NAMESPACE_DNS
from uuid import uuid5
from uuid import uuid7

import zmq
import zmq.asyncio

from snapper.application.ai_review.service import AiReviewDecisionOutcome
from snapper.application.services.signals.service import signal_service
from snapper.config.settings import get_bootstrap_settings
from snapper.core.paired_execution import compute_paired_group_key
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionMode
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketDataTypeEnum
from snapper.core.types import PairedExecutionPolicy
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleRow
from snapper.infrastructure.historical.polygon.loader import GroupedDailyRow
from snapper.infrastructure.historical.polygon.loader import load_recent_grouped_daily
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
from snapper.strategies.models import StrategySignalResult
from snapper.strategies.system_events import SystemMessageRouter

__all__ = [
    "BaseStrategy",
    "CompositeStrategy",
    "StrategySignal",
    "StrategySignalResult",
    "StrategyConfig",
]


logger = logging.getLogger(__name__)
_bootstrap_settings = get_bootstrap_settings()

_WARMUP_CANDLE_NAMESPACE = uuid5(NAMESPACE_DNS, "snapper.strategies.warmup.candle")
"""Namespace for deterministic warmup-candle ``public_id`` values."""

_WARMUP_SESSION_ID = "warmup"
"""``session_id`` stamped on warmup-prefilled candles (provenance marker)."""

_DEFAULT_POLYGON_CACHE_ROOT = "data/polygon/cache"
"""Default Polygon cache root for warmup prefill (override via params)."""


def _native_to_polygon_crypto_ticker(native_symbol: str) -> str | None:
    """Derive the Polygon REST crypto ticker for a native ``BASE-QUOTE`` symbol.

    Pure transform (``FET-USD`` -> ``X:FETUSD``) — the live ``SymbolMapperService``
    is populated from ``system.symbol_aliases`` and is empty at warmup time, so
    the ticker must be derived without it.

    Args:
        native_symbol: Native ``BASE-QUOTE`` symbol.

    Returns:
        The ``X:{BASE}{QUOTE}`` ticker, or ``None`` if the symbol is not a single
        ``BASE-QUOTE`` pair.
    """
    parts = native_symbol.split("-")
    if len(parts) != 2 or not parts[0] or not parts[1]:
        return None
    return f"X:{parts[0].upper()}{parts[1].upper()}"


def _grouped_row_to_warmup_candle(
    row: GroupedDailyRow, *, instrument: str, exchange: str, sequence_id: int
) -> CandleData:
    """Project a cached grouped-daily row to a warmup :class:`CandleData`.

    ``open_at`` is the UTC day START (``closing_timestamp`` floored to 00:00 UTC),
    matching the synthesized live 1d boundary so the warmup series is continuous
    with live bars. The envelope fields are synthetic (this bar never crossed the
    bus): a deterministic ``public_id`` keyed by ``exchange|instrument|1d|open_at``,
    ``session_id="warmup"``.

    Args:
        row: Cached grouped-daily row.
        instrument: Native instrument symbol (the candle-buffer key).
        exchange: Market-data exchange for the candle envelope (source exchange
            for paper inputs).
        sequence_id: Monotonic sequence id within the warmup batch.

    Returns:
        A 1d :class:`CandleData` suitable for the strategy candle buffer.
    """
    open_at = row.closing_timestamp.astimezone(UTC).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    open_at_ms = int(open_at.timestamp() * 1000)
    public_id = str(uuid5(_WARMUP_CANDLE_NAMESPACE, f"{exchange}|{instrument}|1d|{open_at_ms}"))
    return CandleData(
        public_id=public_id,
        timestamp=open_at,
        session_id=_WARMUP_SESSION_ID,
        sequence_id=sequence_id,
        instrument=instrument,
        exchange=cast(MarketDataExchange, exchange),
        timeframe="1d",
        open_at=open_at,
        open=float(row.open),
        high=float(row.high),
        low=float(row.low),
        close=float(row.close),
        volume=float(row.volume),
        vwap=float(row.vwap) if row.vwap is not None else None,
        trades=row.total_trades,
    )


def _db_row_to_warmup_candle(
    row: CandleRow, *, instrument: str, exchange: str, sequence_id: int
) -> CandleData:
    """Project a persisted 1d :class:`CandleRow` to a warmup :class:`CandleData`.

    The DB-first counterpart of :func:`_grouped_row_to_warmup_candle` (Phase 3
    slice 5). ``open_at`` is the persisted canonical 1d boundary (00:00 UTC),
    already aligned with the synthesized live 1d bars, so the warmed series is
    continuous with what live ``on_candle`` will receive. Uses the same
    deterministic ``public_id`` (``exchange|instrument|1d|open_at``) and
    ``session_id="warmup"`` envelope as the cache path.

    Args:
        row: Persisted 1d candle row from the repository.
        instrument: Native instrument symbol (the candle-buffer key).
        exchange: Market-data exchange for the candle envelope.
        sequence_id: Monotonic sequence id within the warmup batch.

    Returns:
        A 1d :class:`CandleData` suitable for the strategy candle buffer.
    """
    open_at = row["open_at"]
    open_at_ms = int(open_at.timestamp() * 1000)
    public_id = str(uuid5(_WARMUP_CANDLE_NAMESPACE, f"{exchange}|{instrument}|1d|{open_at_ms}"))
    return CandleData(
        public_id=public_id,
        timestamp=open_at,
        session_id=_WARMUP_SESSION_ID,
        sequence_id=sequence_id,
        instrument=instrument,
        exchange=cast(MarketDataExchange, exchange),
        timeframe="1d",
        open_at=open_at,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        vwap=row["vwap"],
        trades=row["trades"],
    )


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

    PAIRED_EXECUTION_POLICY: PairedExecutionPolicy | None = None
    """Paired-execution policy for multi-leg emissions, or ``None``.

    A strategy that can return a ``list[StrategySignal]`` (a leg group) MUST
    declare the policy that governs the group so the paired-execution arming
    barrier knows how to coordinate the legs (``"simultaneous"`` arms all legs
    together; ``"sequential_handoff"`` arms leg N+1 only after leg N is
    terminal). Single-leg strategies leave this ``None``; emitting a multi-leg
    group with no declared policy is a fail-closed error.
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
        self.output_topics: list[str] = [
            self._signal_topic_for(instrument) for instrument in self.outputs
        ]
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
        self._warmup_through_open_at: datetime | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self.last_data_timestamp: float = time.time()
        self.heartbeat_seq: int = 0
        self._feed_heartbeats: dict[str, dict[str, Any]] = {}
        self._last_data_ts: float | None = None
        self._is_paper_input = any(StrategyConfig._is_paper_or_replay(inp) for inp in self.inputs)
        self._health_monitor = StrategyHealthMonitor(self)
        self._system_router = SystemMessageRouter(self)

    def _signal_topic_for(self, instrument: str) -> str:
        """Return the output topic a signal on ``instrument`` publishes to.

        Single source of truth for the PAPER-vs-live topic shape, shared
        by ``output_topics`` construction, group validation, and
        ``emit_signal`` so the allowed-output check can never drift from
        the actual publish topic.

        Args:
            instrument: The instrument symbol.

        Returns:
            The signal topic string. PAPER routes carry the strategy
            name; live routes carry the ``"live"`` discriminator.
        """
        if self.exchange == ExchangeEnum.PAPER:
            return signal_topic(self.exchange, instrument, self.name)
        return signal_topic(self.exchange, instrument, "live")

    def _normalize_signal_group(self, result: StrategySignalResult) -> list[StrategySignal]:
        """Normalize and group-validate a callback return, fail-closed.

        Converts the union return contract (``None`` / single
        :class:`StrategySignal` / ``list[StrategySignal]``) into a flat
        list, then enforces the multi-leg group invariants BEFORE any
        signal is published or recorded. Validation raises rather than
        emitting a partial group, so a malformed multi-leg return can
        never leave one leg of a spread naked.

        Args:
            result: The value returned by ``on_candle`` / ``on_tick`` /
                ``on_trade``.

        Returns:
            The validated signals in strategy-declared order; empty when
            the callback returned ``None`` or an empty list.

        Raises:
            TypeError: If the return value, or any element of a returned
                list, is not a :class:`StrategySignal`.
            ValueError: If two legs share an instrument, or a leg's
                instrument does not map to a configured output topic.
        """
        if result is None:
            return []
        if isinstance(result, StrategySignal):
            signals = [result]
        elif isinstance(result, list):
            signals = result
        else:
            raise TypeError(
                f"Strategy {self.name}: callback must return None, StrategySignal, "
                f"or list[StrategySignal]; got {type(result).__name__}"
            )
        seen: set[str] = set()
        for signal in signals:
            if not isinstance(signal, StrategySignal):
                raise TypeError(
                    f"Strategy {self.name}: every leg must be a StrategySignal; "
                    f"got {type(signal).__name__}"
                )
            if signal.instrument in seen:
                raise ValueError(
                    f"Strategy {self.name}: duplicate instrument '{signal.instrument}' "
                    f"in one signal group"
                )
            seen.add(signal.instrument)
            topic = self._signal_topic_for(signal.instrument)
            if topic not in self.output_topics:
                raise ValueError(
                    f"Strategy {self.name}: signal for instrument '{signal.instrument}' "
                    f"not allowed; not in configured outputs: {self.outputs} "
                    f"(generated topic: {topic})"
                )
        if (
            len(signals) > 1
            and self.exchange != ExchangeEnum.PAPER
            and not _bootstrap_settings.paired_execution_guard_enabled
        ):
            raise ValueError(
                f"Strategy {self.name}: refusing to emit a {len(signals)}-leg signal group on "
                f"live exchange '{self.exchange}'. Multi-leg emission is not venue-execution "
                f"atomic on its own, so one leg could reject or fill late and leave one-sided "
                f"exposure. Set PAIRED_EXECUTION_GUARD_ENABLED=true only once the paired-execution "
                f"guard is deployed. Paper multi-leg is always allowed."
            )
        return signals

    @property
    def is_running(self) -> bool:
        """Check if the strategy is currently running.

        Returns:
            True if the strategy is running, False otherwise.
        """
        return self._running

    def required_candle_history(self) -> int:
        """Return number of candle bars needed for indicator warm-up.

        Override in subclasses that need historical candles to compute
        indicators before generating signals. Default: 0 (no warm-up).

        Returns:
            Number of warm-up bars required.
        """
        return 0

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignalResult:
        """Handle incoming candle data.

        Args:
            instrument: The instrument symbol.
            candle: The candle data with OHLCV data.

        Returns:
            ``None``, a single :class:`StrategySignal`, or a
            ``list[StrategySignal]`` (multiple legs emitted together, in
            strategy-declared order). Single-leg strategies may keep the
            narrower ``StrategySignal | None`` return.
        """
        await asyncio.sleep(0)
        return None

    async def on_tick(self, instrument: str, tick: TickData) -> StrategySignalResult:
        """Handle incoming tick data.

        Args:
            instrument: The instrument symbol.
            tick: The tick data with bid/ask data.

        Returns:
            ``None``, a single :class:`StrategySignal`, or a
            ``list[StrategySignal]`` for multi-leg emission.
        """
        await asyncio.sleep(0)
        return None

    async def on_trade(self, instrument: str, trade: TradeData) -> StrategySignalResult:
        """Handle incoming trade data.

        Args:
            instrument: The instrument symbol.
            trade: The trade data with trade details.

        Returns:
            ``None``, a single :class:`StrategySignal`, or a
            ``list[StrategySignal]`` for multi-leg emission.
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
        await self._warmup_candle_buffer()
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
            if parsed.exchange == ExchangeEnum.PAPER and parsed.source_exchange:
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
        Passes ``wallet_public_id`` (when the typed message
        carries one) so per-wallet streams on the same topic do not
        interleave into false gaps.
        """
        try:
            msg = parse_message(payload_str)
            wallet_public_id = getattr(msg, "wallet_public_id", "") or ""
            self._gap_detector.check(
                topic, msg.session_id, msg.sequence_id, wallet_public_id=wallet_public_id
            )
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
                    signals = await self._dispatch_market_data(topic_str, instrument, payload_str)
                    await self._emit_signal_group(signals)
        except asyncio.CancelledError:
            logger.info(f"Strategy {self.name}: Listen loop cancelled")
            raise
        except Exception as e:
            logger.exception(f"Strategy {self.name}: Error in listen loop: {e}")
            self._running = False

    async def _handle_candle_data(self, instrument: str, payload: str) -> list[StrategySignal]:
        """Handle incoming candle data.

        Args:
            instrument: The instrument symbol.
            payload: The JSON payload string.

        Returns:
            The validated signal group from ``on_candle`` (empty when the
            handler produced no signal). The group is normalized and
            fail-closed-validated before return, so callers receive only
            publishable signals.
        """
        candle = CandleData.from_json(payload)
        self._last_data_ts = candle.open_at.timestamp()
        self._buffer_candle(instrument, candle)
        return self._normalize_signal_group(await self.on_candle(instrument, candle))

    def _buffer_candle(self, instrument: str, candle: CandleData) -> None:
        """Insert a candle into the per-instrument buffer, upserting by ``open_at``.

        A candle whose ``open_at`` already exists in the buffer REPLACES the
        existing bar (the live synthesized day-D bar collides with the warmup's
        last day-D bar; a re-emit of an existing window collides too), so the
        indicator series never carries a duplicate ``open_at``. Otherwise the
        candle is appended and the buffer is pruned to ``params["buffer_size"]``.
        Used by both live candle handling and warmup prefill.

        Args:
            instrument: Buffer key (native instrument symbol).
            candle: The candle to insert.
        """
        buffer = self.candle_buffer.setdefault(instrument, [])
        for index, existing in enumerate(buffer):
            if existing.open_at == candle.open_at:
                buffer[index] = candle
                return
            if existing.open_at > candle.open_at:
                buffer.insert(index, candle)
                break
        else:
            buffer.append(candle)
        max_buffer_size = self.params.get("buffer_size", 100)
        if len(buffer) > max_buffer_size:
            buffer.pop(0)

    async def _warmup_candle_buffer(self) -> None:
        """Prefill the candle buffer with date-aligned historical bars before live.

        Called from :meth:`start` BEFORE :meth:`_subscribe_inputs` (so no live
        frame can race the buffer). OPT-IN and crypto-scoped: runs only when the
        strategy declares :meth:`required_candle_history` > 0 AND configures
        ``params["warmup_market_type"] == "crypto"`` — the opt-in keeps a
        non-crypto strategy from ever loading a crypto ``X:`` ticker.

        DB-FIRST (Phase 3 slice 5, §3.7): warms from the persisted ``1d`` plane
        (``get_candles`` under the leg's live venue) when it holds the full
        history, so the warmed series is continuous with the live synthesized 1d
        bars and the read path is single-source. Only when the persisted plane is
        short for some leg (e.g. pre-backfill) does it fall back to the Polygon
        crypto daily cache as a non-canonical bootstrap (logged as such).

        ALIGNED ALL-OR-NOTHING across legs: every 1d candle input is loaded, and
        buffers are installed ONLY if all legs share at least ``count`` common UTC
        days. A multi-leg strategy whose spread aligns legs BY POSITION (e.g. a
        cointegration PAIR) would otherwise get a date-MISALIGNED spread — and
        false signals on a live money path — if one leg warmed and another did
        not, so on any shortfall the warmup installs NOTHING and falls back to
        symmetric live-only fill (the pre-A3 behaviour). A warmup error never
        crashes startup. Requires ``buffer_size >= required_candle_history`` so the
        warmed window is not silently truncated below the lookback.
        """
        count = self.required_candle_history()
        if count <= 0:
            return
        if self.params.get("warmup_market_type") != "crypto":
            return
        try:
            buffer_cap = int(self.params.get("buffer_size", 100))
            if buffer_cap < count:
                logger.warning(
                    f"Strategy {self.name}: buffer_size {buffer_cap} < required warmup {count}; "
                    "skipping warmup (live-only) — raise buffer_size to at least the lookback"
                )
                return
            legs: list[Any] = []
            for topic in self.inputs:
                if not topic.startswith("market."):
                    continue
                parsed = parse_market_topic(topic)
                if (
                    parsed is None
                    or parsed.data_type != MarketDataTypeEnum.CANDLES
                    or parsed.timeframe != "1d"
                ):
                    continue
                legs.append(parsed)
            if not legs:
                return
            if await self._install_db_warmup(legs, count, buffer_cap):
                return
            as_of = (datetime.now(UTC) - timedelta(days=1)).date()
            self._install_cache_warmup(legs, count, buffer_cap, as_of)
        except Exception as exc:
            logger.warning(
                f"Strategy {self.name}: candle warmup failed ({exc}); "
                "continuing with live-only warmup"
            )

    async def _install_db_warmup(self, legs: list[Any], count: int, buffer_cap: int) -> bool:
        """Try the canonical DB-first warmup: install aligned 1d buffers from the repo.

        Reads each leg's persisted 1d history under the venue its live read
        resolves (so the warmed series is continuous with the live synthesized 1d
        bars). Installs the date-aligned buffers ONLY when EVERY leg has at least
        ``count`` persisted rows. Returns False — signalling a Polygon-cache
        bootstrap fallback — only when the persisted plane is SHORT for some leg
        (e.g. pre-backfill); a genuine misalignment of a fully-populated plane
        still installs nothing (live-only) rather than masking it with stale cache.

        Uses the process-wide cached repository (:func:`get_repository`) — the
        same shared engine pool the rest of the process (e.g. signal persistence)
        uses — and does NOT dispose it; the pool is owned by the global lifecycle
        (:func:`dispose_repositories`), so disposing here would close it for all.

        Args:
            legs: Parsed 1d candle-input topics.
            count: Required (aligned) warm-up bar count per leg.
            buffer_cap: Maximum buffered bars per instrument.

        Returns:
            True when the DB held every leg (canonical source used, even if the
            aligned intersection then fell short); False when a leg was short, so
            the caller should fall back to the Polygon cache bootstrap.
        """
        repo = get_repository(_bootstrap_settings.db_url)
        loaded: dict[str, list[CandleData]] = {}
        for parsed in legs:
            bars = await self._load_warmup_leg_db(repo, parsed, count)
            if len(bars) < count:
                return False
            loaded[parsed.instrument] = bars
        self._install_aligned_buffers(loaded, count, buffer_cap, "the DB (canonical)")
        return True

    def _install_cache_warmup(
        self, legs: list[Any], count: int, buffer_cap: int, as_of: date
    ) -> None:
        """Install the Polygon-cache bootstrap warmup (non-canonical fallback).

        Used only when the persisted plane is not yet populated for some leg.
        Loads each leg from the Polygon crypto cache and installs date-aligned
        buffers, or installs nothing (live-only) on any shortfall to keep the
        spread aligned. Logged as a non-canonical bootstrap so an operator can see
        the DB was not the source.

        Args:
            legs: Parsed 1d candle-input topics.
            count: Required (aligned) warm-up bar count per leg.
            buffer_cap: Maximum buffered bars per instrument.
            as_of: Last complete UTC day to load up to (inclusive).
        """
        cache_root = Path(self.params.get("polygon_cache_root", _DEFAULT_POLYGON_CACHE_ROOT))
        loaded: dict[str, list[CandleData]] = {}
        for parsed in legs:
            bars = self._load_warmup_leg(parsed, cache_root, count, as_of)
            if not bars:
                logger.warning(
                    f"Strategy {self.name}: no cached daily warmup for {parsed.instrument}; "
                    "skipping warmup for ALL legs (live-only) to keep the spread aligned"
                )
                return
            loaded[parsed.instrument] = bars
        self._install_aligned_buffers(
            loaded, count, buffer_cap, "the Polygon cache (non-canonical bootstrap)"
        )

    def _install_aligned_buffers(
        self,
        loaded: dict[str, list[CandleData]],
        count: int,
        buffer_cap: int,
        source_label: str,
    ) -> None:
        """Install date-aligned 1d buffers from pre-loaded legs, or install nothing.

        Shared by the DB-first and Polygon-cache warmup paths. Installs buffers
        ONLY if all legs share at least ``count`` common UTC days, else installs
        nothing (live-only) to keep a multi-leg spread date-aligned.

        Args:
            loaded: ``{instrument: ascending 1d bars}`` per leg (each >= count).
            count: Required (aligned) warm-up bar count.
            buffer_cap: Maximum buffered bars per instrument.
            source_label: Human-readable source for the warm log.
        """
        open_at_sets = [{bar.open_at for bar in bars} for bars in loaded.values()]
        common = set.intersection(*open_at_sets)
        if len(common) < count:
            logger.warning(
                f"Strategy {self.name}: only {len(common)} aligned warmup days across "
                f"{len(loaded)} leg(s) (need {count}); skipping warmup (live-only)"
            )
            return
        keep = set(sorted(common)[-buffer_cap:])
        for instrument, bars in loaded.items():
            self.candle_buffer[instrument] = [bar for bar in bars if bar.open_at in keep]
        self._warmup_through_open_at = max(keep)
        logger.info(
            f"Strategy {self.name}: warmed {len(keep)} aligned daily bars for "
            f"{sorted(loaded)} from {source_label}"
        )

    async def _load_warmup_leg_db(
        self, repo: Repository, parsed: Any, count: int
    ) -> list[CandleData]:
        """Load one leg's warm-up candles from the persisted 1d plane.

        Reads under the venue the leg's live read resolves (PAPER legs use their
        ``source_exchange``), so the warmed bars match the live synthesized 1d
        plane. The current forming day is never persisted (1d closes at the next
        00:00 UTC), so the most-recent ``count`` rows end at the last complete day.

        Args:
            repo: Repository handle.
            parsed: Parsed 1d candle-input topic.
            count: Number of daily bars to load.

        Returns:
            Ascending warm-up candles (empty/short when the persisted plane lacks
            ``count`` rows for the leg).
        """
        instrument = parsed.instrument
        if parsed.exchange == ExchangeEnum.PAPER and parsed.source_exchange:
            exchange = parsed.source_exchange
        else:
            exchange = parsed.exchange
        rows = await repo.get_candles(
            instrument=instrument,
            timeframe="1d",
            start=None,
            end=None,
            exchange=exchange,
            as_of=datetime.now(UTC),
            limit=count,
            order="desc",
        )
        return [
            _db_row_to_warmup_candle(
                row, instrument=instrument, exchange=str(exchange), sequence_id=index
            )
            for index, row in enumerate(reversed(rows))
        ]

    def _load_warmup_leg(
        self, parsed: Any, cache_root: Path, count: int, as_of: date
    ) -> list[CandleData]:
        """Load one leg's warm-up candles from the Polygon crypto cache.

        Args:
            parsed: Parsed 1d candle-input topic.
            cache_root: Polygon cache root path.
            count: Number of daily bars to load.
            as_of: Last complete UTC day to load up to (inclusive).

        Returns:
            Ascending warm-up candles (empty when the symbol is not a crypto pair
            or the cache has no matching rows).
        """
        instrument = parsed.instrument
        if parsed.exchange == ExchangeEnum.PAPER and parsed.source_exchange:
            exchange = parsed.source_exchange
        else:
            exchange = parsed.exchange
        ticker = _native_to_polygon_crypto_ticker(instrument)
        if ticker is None:
            return []
        rows = load_recent_grouped_daily(cache_root, ticker, count, as_of)
        return [
            _grouped_row_to_warmup_candle(
                row, instrument=instrument, exchange=str(exchange), sequence_id=index
            )
            for index, row in enumerate(rows)
        ]

    async def _dispatch_market_data(
        self, topic: str, instrument: str, payload: str
    ) -> list[StrategySignal]:
        """Dispatch market data to appropriate handler.

        Args:
            topic: The ZMQ topic string.
            instrument: The instrument symbol.
            payload: The JSON payload string.

        Returns:
            The validated signal group from the matching handler (empty
            when no handler matched or the handler produced no signal).
        """
        if ".candles." in topic:
            return await self._handle_candle_data(instrument, payload)
        if ".ticks" in topic:
            tick = TickData.from_json(payload)
            self._last_data_ts = tick.timestamp.timestamp()
            return self._normalize_signal_group(await self.on_tick(instrument, tick))
        if ".trades" in topic:
            trade = TradeData.from_json(payload)
            self._last_data_ts = (trade.executed_at or trade.timestamp).timestamp()
            return self._normalize_signal_group(await self.on_trade(instrument, trade))
        logger.warning(f"Strategy {self.name}: Unknown market data topic type: {topic}")
        return []

    def _execution_mode(self) -> ExecutionMode:
        """Return the execution mode implied by the strategy exchange.

        Returns:
            ``"paper"`` for the paper exchange, otherwise ``"live"``. The
            value mirrors the engine ``mode`` that the coordinator stamps on
            each paired-execution leg, so the canonical ``group_key`` computed
            here matches the leg-set the arming barrier validates.
        """
        if self.exchange == ExchangeEnum.PAPER:
            return ExecutionModeEnum.PAPER
        return ExecutionModeEnum.LIVE

    def _resolve_paired_execution_policy(self) -> PairedExecutionPolicy:
        """Return the declared paired-execution policy, failing closed.

        Returns:
            The strategy's declared :attr:`PAIRED_EXECUTION_POLICY`.

        Raises:
            ValueError: If the strategy emits a multi-leg group without
                declaring a policy, so an undeclared group can never arm.
        """
        policy = self.PAIRED_EXECUTION_POLICY
        if policy is None:
            raise ValueError(
                f"Strategy {self.name}: emitting a multi-leg signal group requires "
                f"PAIRED_EXECUTION_POLICY to be declared on the strategy class"
            )
        return policy

    async def _emit_signal_group(self, signals: list[StrategySignal]) -> None:
        """Emit a validated signal group, stamping the paired-group descriptor.

        A single-leg group is emitted unchanged (no paired-group descriptor).
        A multi-leg group is assigned ONE ``paired_group_id`` (UUID7) and one
        canonical ``paired_group_key`` (sorted ``{exchange}:{instrument}:{mode}``
        tokens) shared verbatim across every leg, with the per-leg index in
        strategy-declared order, so the paired-execution arming barrier can
        coordinate the legs across coordinators. The group id is generated
        exactly once per group so the legs share a group identity.

        Args:
            signals: The validated signal group in strategy-declared order
                (empty when the callback produced no signal).
        """
        if not signals:
            return
        if len(signals) == 1:
            await self.emit_signal(signals[0])
            return
        group_id = str(uuid7())
        policy = self._resolve_paired_execution_policy()
        mode = self._execution_mode()
        group_key = compute_paired_group_key(
            [(self.exchange, signal.instrument, mode) for signal in signals]
        )
        size = len(signals)
        for index, signal in enumerate(signals):
            await self.emit_signal(
                signal,
                paired_group_id=group_id,
                paired_group_size=size,
                paired_group_index=index,
                paired_group_policy=policy,
                paired_group_key=group_key,
            )

    async def emit_signal(
        self,
        signal: StrategySignal,
        *,
        outcome: AiReviewDecisionOutcome | None = None,
        paired_group_id: str | None = None,
        paired_group_size: int | None = None,
        paired_group_index: int | None = None,
        paired_group_policy: PairedExecutionPolicy | None = None,
        paired_group_key: str | None = None,
    ) -> None:
        """Emit a trading signal to the output topic.

        Args:
            signal: The signal to emit.
            outcome: Optional :class:`AiReviewDecisionOutcome` from a
                successful ``create_ai_review_and_await`` call. When
                set, the AI review attribution
                (``review_public_id`` + ``dispatch_version``) is
                stamped onto the published :class:`SignalData` so the
                trader-coordinator's attribution-aware caps gate can
                run for the strategy emit. The ``dispatch_version`` is
                transport-only end-to-end. Default ``None`` keeps
                non-AI strategy emits byte-identical for downstream
                consumers.
            paired_group_id: Shared group identity for a multi-leg
                emission, or ``None`` for a standalone signal. When set,
                the remaining ``paired_group_*`` arguments must all be set
                too (enforced by the :class:`SignalData` validator) and are
                stamped onto the envelope and the persisted signal so the
                paired-execution arming barrier can coordinate the legs.
            paired_group_size: Total leg count of the group.
            paired_group_index: This leg's index in strategy-declared
                order, ``0 <= index < paired_group_size``.
            paired_group_policy: The group's coordination policy.
            paired_group_key: Canonical sorted ``{exchange}:{instrument}:{mode}``
                leg-set key, identical across all legs of the group.

        Raises:
            ValueError: If signal instrument is not in configured outputs.
        """
        if signal.timestamp is None:
            ts = self._last_data_ts or time.time()
            signal.timestamp = datetime.fromtimestamp(ts, tz=UTC)
        topic = self._signal_topic_for(signal.instrument)
        if topic not in self.output_topics:
            raise ValueError(
                f"Strategy {self.name}: Signal for instrument '{signal.instrument}' not allowed. "
                f"Instrument not in configured outputs: {self.outputs}. "
                f"Generated topic: {topic}"
            )
        if not self.msg_publisher:
            await self._setup_publisher()
        tracker = self.msg_publisher.tracker if self.msg_publisher else self._tracker
        now = signal.timestamp or datetime.now(UTC)
        signal_envelope = SignalData(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(topic),
            instrument=signal.instrument,
            side=signal.side,
            strength=signal.strength,
            reason=signal.reason,
            price=signal.price,
            exchange=self.exchange,
            strategy_name=self.name,
            fired_at=now,
            wallet_public_id=self.config.wallet_public_id,
            operator_public_id=self.config.operator_public_id or None,
            ai_review_public_id=outcome.review_public_id if outcome is not None else None,
            ai_review_dispatch_version=(outcome.dispatch_version if outcome is not None else None),
            paired_group_id=paired_group_id,
            paired_group_size=paired_group_size,
            paired_group_index=paired_group_index,
            paired_group_policy=paired_group_policy,
            paired_group_key=paired_group_key,
        )

        if self.msg_publisher is not None:
            await self.msg_publisher.send(topic, signal_envelope)
        await signal_service.store_signal(
            signal,
            exchange=self.exchange,
            strategy_name=self.name,
            price=signal.price,
            session_id=signal_envelope.session_id,
            sequence_id=signal_envelope.sequence_id,
            public_id=signal_envelope.public_id,
            timestamp=signal_envelope.timestamp,
            tracker=self._tracker,
            wallet_public_id=self.config.wallet_public_id or None,
            operator_public_id=self.config.operator_public_id or None,
            paired_group_id=paired_group_id,
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
