"""Trader coordinator module.

This module provides the central trading coordination through TraderCoordinator.
It is the single point of entry for all trading signals in the system - there
should be exactly ONE TraderCoordinator running per deployment.

The coordinator:
- Subscribes to ZMQ signal topics from strategy publishers
- Creates and manages TradingEngineService instances per instrument
- Handles settings updates and symbol mapping changes
- Monitors signal health and execution fills
"""

import asyncio
import contextlib
import time
from typing import Any
from typing import cast
from typing import get_args

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.engine.config import EngineConfigModel
from snapper.application.engine.service import TradingEngineService
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.application.services.settings import SettingsService
from snapper.config.settings import AppSettings
from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings
from snapper.core.types import OrderExchange
from snapper.data.repository import get_repository
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import parse_order_event_topic
from snapper.messaging.topics.builders import parse_signal_topic

_bootstrap_settings = get_bootstrap_settings()


@register_process(
    "trader_coordinator",
    description="Trading coordinator",
    priority=40,
    role=ProcessRoleEnum.CORE,
    tags=("trading", "signals", "risk"),
    enabled=True,
    mode="thread",
    args=[],
)
class TraderCoordinator(RegisterableProcess):
    """Central trading coordinator - ONE instance per system.

    The TraderCoordinator is the heart of the trading system. It:
    - Subscribes to strategy signal topics via ZMQ
    - Dynamically creates TradingEngineService for each instrument/exchange
    - Routes signals to appropriate engines
    - Handles system events (settings changes, symbol mapping updates)
    - Monitors execution fills and order status updates

    Only ONE TraderCoordinator should run per deployment to ensure
    consistent position tracking and risk management.

    Attributes:
        settings: Application settings.
        signal_topics: List of ZMQ topics to subscribe for signals.
        repository: Database repository for instrument persistence.
        engines: Dict mapping engine_key to TradingEngineService instances.
        zmq_context: ZMQ context for signal subscription.
        signal_subscriber: ZMQ subscriber socket.
        last_signal_time: Dict tracking last signal time per engine.
        execution_publisher: ZMQ publisher for order requests.
    """

    def __init__(
        self,
        signal_topics: list[str] | None = None,
    ):
        """Initialize the trader coordinator.

        Args:
            signal_topics: List of ZMQ topic prefixes to subscribe to.
                Defaults to ["signals."] to receive all strategy signals.
        """
        self.settings = get_settings()
        self.signal_topics = signal_topics or ["signals."]
        self.repository = get_repository(self.settings.db_url)
        self.engines: dict[str, TradingEngineService] = {}
        self.zmq_context: zmq.asyncio.Context | None = None
        self.signal_subscriber: ValidatedSubscriber | None = None
        self.last_signal_time: dict[str, float] = {}
        self.execution_context: zmq.Context[Any] | None = None
        self.execution_publisher: ValidatedPublisher | None = None
        self._current_topic: str = ""

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Get default constructor kwargs from settings.

        Args:
            settings: Application settings instance.

        Returns:
            Dict with default kwargs for TraderCoordinator.
        """
        return {
            "signal_topics": ["signals."],
        }

    def __repr__(self) -> str:
        """Return string representation."""
        return f"TraderCoordinator(signal_topics={self.signal_topics})"

    async def start(self) -> None:
        """Start the trader coordinator.

        Sets up ZMQ connections, initializes trading components,
        and enters the main trading loop.
        """
        logger.info("Starting ZMQ Signal TraderCoordinator (Central - ONE per system)")
        logger.info(f"Signal Topics: {self.signal_topics}")
        self._setup_external_execution()
        self._setup_trading_components()
        self._setup_signal_subscriber()
        await self._run_trading_loop()

    def _handle_settings_update(self, payload: bytes) -> None:
        """Handle settings change event from ZMQ.

        Updates local settings cache when a setting is changed elsewhere
        in the system.

        Args:
            payload: JSON-encoded settings change data.
        """
        try:
            message = SettingChangedData.from_json(payload.decode("utf-8"))
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(message.value)
                settings_service._cache[message.key] = parsed_value
                logger.info(f"ZMQTrader: Setting {message.key} updated via ZMQ event")
        except Exception as e:
            logger.error(f"ZMQTrader: Error handling settings update: {e}")

    def _dispatch_order_event(self, topic: str, payload: bytes) -> None:
        """Dispatch order event to appropriate handler based on message type.

        Uses parse_message() to determine message type and routes accordingly:
        - ExecutionData -> _handle_execution_fill
        - OrderData -> _handle_order_status
        - OrderEventData -> _handle_order_event

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.executed").
            payload: JSON-encoded message data.
        """
        try:
            msg = parse_message(payload.decode("utf-8"))
        except MessageParseError as e:
            logger.error(f"ZMQTrader: Invalid orders.events payload on {topic}: {e}")
            return
        if isinstance(msg, ExecutionData):
            self._handle_execution_fill(topic, msg)
        elif isinstance(msg, OrderData):
            self._handle_order_status(topic, msg)
        elif isinstance(msg, OrderEventData):
            self._handle_order_event(topic, msg)
        else:
            logger.debug(f"ZMQTrader: Ignoring orders.events message type={msg.type} on {topic}")

    def _handle_execution_fill(self, topic: str, fill: ExecutionData) -> None:
        """Handle execution fill event from ZMQ.

        Logs fill information when an order is executed.
        Performs invariant checks: topic exchange/instrument must match payload.

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.executed").
            fill: Parsed execution fill data.
        """
        parsed = parse_order_event_topic(topic)
        if parsed is None:
            logger.debug(f"ZMQTrader: Ignoring malformed fill topic: {topic}")
            return
        if fill.exchange != parsed.exchange or fill.instrument != parsed.instrument:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic '{parsed.exchange}/{parsed.instrument}' "
                f"!= payload '{fill.exchange}/{fill.instrument}'"
            )
            return
        logger.info(
            f"ZMQTrader: Fill received - {fill.client_order_id} "
            f"(exchange_order_id={fill.exchange_order_id}, trade_id={fill.trade_id}) "
            f"{fill.side} {fill.size}@{fill.price} {fill.instrument} on {parsed.exchange}"
        )

    def _handle_order_status(self, topic: str, order_status: OrderData) -> None:
        """Handle order status event from ZMQ.

        Logs order status changes (submitted, accepted, rejected, etc.).
        Performs invariant checks:
        - topic exchange/instrument must match payload
        - topic suffix must match payload status

        For 'rejected' status, the log includes message type to disambiguate
        submit rejection (OrderData) vs cancel/replace rejection
        (OrderEventData).

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.accepted").
            order_status: Parsed order status data.
        """
        parsed = parse_order_event_topic(topic)
        if parsed is None:
            logger.debug(f"ZMQTrader: Ignoring malformed order status topic: {topic}")
            return
        if order_status.exchange != parsed.exchange or order_status.instrument != parsed.instrument:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic '{parsed.exchange}/{parsed.instrument}' "
                f"!= payload '{order_status.exchange}/{order_status.instrument}'"
            )
            return
        if order_status.status != parsed.suffix:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic suffix '{parsed.suffix}' "
                f"!= payload status '{order_status.status}', dropping message"
            )
            return
        if parsed.suffix == "rejected":
            logger.info(
                f"ZMQTrader: Order status [OrderData] - {order_status.client_order_id} "
                f"{parsed.suffix} (submit rejection) {order_status.instrument} "
                f"on {parsed.exchange}"
            )
        else:
            logger.info(
                f"ZMQTrader: Order status - {order_status.client_order_id} {parsed.suffix} "
                f"{order_status.instrument} on {parsed.exchange}"
            )

    def _handle_order_event(self, topic: str, order_event: OrderEventData) -> None:
        """Handle lightweight order event from ZMQ (cancel/replace confirmations).

        Logs cancel/replace event confirmations (cancelled, replaced, rejected).
        Performs invariant checks (all are hard drops on violation):
        - topic exchange/instrument must match payload
        - topic suffix must match payload event

        For 'rejected' event, the log includes message type to disambiguate
        cancel/replace rejection (OrderEventData) vs submit rejection
        (OrderData).

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.cancelled").
            order_event: Parsed order event data.
        """
        parsed = parse_order_event_topic(topic)
        if parsed is None:
            logger.debug(f"ZMQTrader: Ignoring malformed order event topic: {topic}")
            return
        if order_event.exchange != parsed.exchange or order_event.instrument != parsed.instrument:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic '{parsed.exchange}/{parsed.instrument}' "
                f"!= payload '{order_event.exchange}/{order_event.instrument}'"
            )
            return
        if order_event.event != parsed.suffix:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic suffix '{parsed.suffix}' "
                f"!= payload event '{order_event.event}', dropping message"
            )
            return
        if parsed.suffix == "rejected":
            logger.info(
                f"ZMQTrader: Order event [OrderEventData] - {order_event.client_order_id} "
                f"{parsed.suffix} (cancel/replace rejection) {order_event.instrument} "
                f"on {parsed.exchange}"
            )
        else:
            logger.info(
                f"ZMQTrader: Order event - {order_event.client_order_id} {parsed.suffix} "
                f"{order_event.instrument} on {parsed.exchange}"
            )

    async def stop(self) -> None:
        """Stop the trader coordinator and cleanup resources.

        Closes all ZMQ sockets and terminates contexts.
        """
        logger.info("Stopping ZMQ Signal TraderCoordinator")
        if self.signal_subscriber:
            self.signal_subscriber.setsockopt(zmq.LINGER, 0)
            self.signal_subscriber.close()
            self.signal_subscriber = None
        if self.zmq_context:
            self.zmq_context.term()
            self.zmq_context = None
        if self.execution_publisher:
            self.execution_publisher.setsockopt(zmq.LINGER, 0)
            self.execution_publisher.close()
        if self.execution_context:
            self.execution_context.term()

    def _setup_trading_components(self) -> None:
        """Initialize trading components.

        Validates that execution publisher is ready. Engines are created
        dynamically when signals arrive.
        """
        if not self.execution_publisher:
            raise RuntimeError(
                "execution_publisher not initialized - call _setup_external_execution first"
            )
        logger.info("ZMQTrader: Engines will be created dynamically from incoming signals")

    async def _ensure_instrument(self, instrument: str, exchange: str) -> None:
        """Ensure instrument exists in database.

        Creates or updates the instrument record with base/quote currencies.
        Resolves Symbol.public_id so the Instrument carries the stable
        symbol identity key.

        Args:
            instrument: Symbol string (e.g., "BTC-USD" or "BTC/USD").
            exchange: Exchange name (lowercase).
        """
        parts = instrument.split("-") if "-" in instrument else instrument.split("/")
        base = parts[0] if len(parts) > 0 else instrument
        quote = parts[1] if len(parts) > 1 else "USD"
        symbol_pid = await resolve_symbol_public_id(self.repository, instrument)
        if symbol_pid is None:
            logger.warning(
                f"ZMQTrader: No active Symbol row for {instrument}, skipping instrument upsert"
            )
            return
        await self.repository.upsert_instrument(
            symbol_public_id=symbol_pid,
            symbol=instrument,
            exchange=exchange,
            base=base,
            quote=quote,
            tick_size=0.01,
            lot_size=0.0001,
        )

    def _setup_external_execution(self) -> None:
        """Set up ZMQ publisher for order execution.

        Connects to the broker XSUB endpoint for publishing order requests.
        """
        self.execution_context = zmq.asyncio.Context()
        raw_pub_socket = self.execution_context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_ORDER_FLOW)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.execution_publisher = ValidatedPublisher(raw_pub_socket)
        logger.info(
            f"ZMQTrader: Connected to broker for order publishing: {self.settings.zmq_broker_xsub}"
        )

    def _setup_signal_subscriber(self) -> None:
        """Set up ZMQ subscriber for signals and system events.

        Subscribes to:
        - Signal topics (configurable)
        - system.symbol_aliases (cache invalidation)
        - system.settings (settings updates)
        - orders.events.* (fill notifications and order status updates)
        """
        self.zmq_context = zmq.asyncio.Context()
        raw_sub_socket = self.zmq_context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_ORDER_FLOW)
        broker_addr = _bootstrap_settings.zmq_broker_xpub
        logger.info(f"ZMQTrader: Connecting signal subscriber to broker {broker_addr}")
        raw_sub_socket.connect(broker_addr)
        self.signal_subscriber = ValidatedSubscriber(raw_sub_socket)
        for topic in self.signal_topics:
            logger.info(f"ZMQTrader: Subscribing to {topic}")
            self.signal_subscriber.subscribe(topic)
        logger.info("ZMQTrader: Subscribing to system.symbol_aliases")
        self.signal_subscriber.subscribe("system.symbol_aliases")
        logger.info("ZMQTrader: Subscribing to system.settings")
        self.signal_subscriber.subscribe("system.settings")
        logger.info("ZMQTrader: Subscribing to orders.events. (fills and status updates)")
        self.signal_subscriber.subscribe("orders.events.")
        logger.info("ZMQTrader: Signal subscriber setup complete")

    async def _run_trading_loop(self) -> None:
        """Run the main trading loop.

        Spawns tasks for signal listening and health monitoring.
        Runs until cancelled.
        """
        tasks = [
            asyncio.create_task(self._listen_signals()),
            asyncio.create_task(self._signal_health_monitor()),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Trading loop cancelled")
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    async def _listen_signals(self) -> None:
        """Listen for incoming signals and system events.

        Main message processing loop that routes messages based on topic:
        - Signals → _on_signal()
        - Settings → _handle_settings_update()
        - Order events → _handle_execution_fill() or _handle_order_status()
        """
        if not self.signal_subscriber:
            logger.error("ZMQTrader: No signal subscriber in listen loop")
            return
        logger.info("ZMQTrader: Starting signal listen loop")
        try:
            while True:
                topic_str, msg_bytes = await self.signal_subscriber.recv_multipart()
                if topic_str == "system.symbol_aliases":
                    logger.info("ZMQTrader: Received symbol_aliases update, refreshing cache")
                    SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)
                    continue
                if topic_str == "system.settings":
                    self._handle_settings_update(msg_bytes)
                    continue
                if topic_str.startswith("orders.events."):
                    self._dispatch_order_event(topic_str, msg_bytes)
                    continue
                signal = SignalData.from_json(msg_bytes.decode())
                logger.info(f"ZMQTrader: Received signal from {topic_str}: {signal}")
                self._current_topic = topic_str
                await self._on_signal(signal)
        except asyncio.CancelledError:
            logger.info("ZMQTrader: Signal listen loop cancelled")
            raise
        except Exception as e:
            logger.error(f"ZMQTrader: Error in signal listen loop: {e}", exc_info=True)

    async def _on_signal(self, signal: SignalData) -> None:
        """Process incoming trading signal.

        Extracts exchange and mode from topic, creates engine if needed,
        and executes the signal through the appropriate engine.

        Args:
            signal: Validated signal envelope with instrument, side,
                strength, and price information.
        """
        parsed = parse_signal_topic(self._current_topic)
        if parsed is None:
            logger.warning(f"ZMQTrader: Invalid signal topic format: {self._current_topic}")
            return
        exchange_str = parsed.exchange
        mode = parsed.signal_type
        valid_exchanges = get_args(OrderExchange)
        if exchange_str not in valid_exchanges:
            logger.warning(f"ZMQTrader: Unknown exchange '{exchange_str}' in topic")
            return
        exchange = cast(OrderExchange, exchange_str)
        instrument = signal.instrument
        if not is_tradeable(instrument, exchange):
            logger.warning(
                f"ZMQTrader: instrument {instrument} not tradeable on {exchange}, "
                f"dropping signal"
            )
            return
        side = signal.side
        strength = signal.strength
        price = signal.price
        strategy_name = signal.strategy_name or "unknown"
        if not price or price <= 0:
            logger.warning(f"ZMQTrader: Invalid signal (missing or invalid price): {signal}")
            return
        engine_key = f"{instrument}@{exchange}-{mode}"
        assert (
            self.execution_publisher is not None
        ), "execution_publisher not initialized - _setup_external_execution must be called first"
        if engine_key not in self.engines:
            logger.info(f"ZMQTrader: Creating new engine for {engine_key}")
            await self._ensure_instrument(instrument, exchange=exchange)
            risk = RiskEvaluator(
                RiskConfigModel(
                    r_per_trade=self.settings.risk_r_per_trade,
                    max_leverage=self.settings.risk_max_leverage,
                    max_drawdown=self.settings.risk_max_drawdown,
                )
            )
            specs_map: dict[str, dict[str, float]] = {
                instrument: {"tick_size": 0.01, "lot_size": 0.0001}
            }
            self.engines[engine_key] = TradingEngineService(
                instrument,
                execution_socket=self.execution_publisher,
                risk=risk,
                cfg=EngineConfigModel(),
                instrument_specs=specs_map,
                exchange=exchange,
            )
            self.last_signal_time[engine_key] = 0.0
        self.last_signal_time[engine_key] = time.time()
        desired_units = strength if side == "buy" else 0.0
        logger.info(
            f"ZMQTrader: Processing signal from {strategy_name} - "
            f"{engine_key} {side} (strength={strength:.2f}, price={price:.2f}, "
            f"desired_units={desired_units:.4f})"
        )
        signaled_at = signal.fired_at.timestamp()
        engine = self.engines[engine_key]
        await engine.execute_desired_units(desired_units, price, signaled_at=signaled_at)

    async def _signal_health_monitor(self) -> None:
        """Monitor signal health and warn on signal gaps.

        Periodically checks when last signal was received for each engine
        and logs warnings if no signals received within timeout.
        """
        signal_timeout = 60.0
        while True:
            await asyncio.sleep(5.0)
            current_time = time.time()
            for engine_key in self.engines:
                last_signal = self.last_signal_time.get(engine_key, 0)
                if current_time - last_signal > signal_timeout:
                    logger.debug(f"ZMQTrader: No signals for {engine_key} in {signal_timeout}s")


async def run_zmq_trader(
    signal_topics: list[str] | None = None,
) -> None:
    """Run the ZMQ trader coordinator.

    Convenience function to create and run a TraderCoordinator.

    Args:
        signal_topics: List of ZMQ topic prefixes to subscribe to.
    """
    trader = TraderCoordinator(signal_topics=signal_topics)
    try:
        await trader.start()
    except KeyboardInterrupt:
        logger.info("TraderCoordinator stopped by user")
    finally:
        await trader.stop()
