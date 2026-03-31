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
from datetime import UTC
from datetime import datetime
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
from snapper.application.process_manager.process_parameters import TraderParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.application.services.settings import SettingsService
from snapper.application.trade.balance_service import BalanceService
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService
from snapper.config.settings import AppSettings
from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import OrderType
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.interface.websocket.schemas import TradeSide
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import order_command_topic
from snapper.messaging.topics.builders import parse_order_event_topic
from snapper.messaging.topics.builders import parse_signal_topic

_bootstrap_settings = get_bootstrap_settings()


@register_process(
    "trader_coordinator",
    description="Trading coordinator",
    priority=40,
    role=ProcessRoleEnum.CORE,
    tags=("trading", "signals", "risk"),
    parameters_model=TraderParameters,
    enabled=True,
    mode="thread",
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
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self._gap_detector: GapDetector = GapDetector("trader")
        self._current_topic: str = ""
        self.trade_service: TradeService = TradeService()
        self.balance_service: BalanceService = BalanceService()
        self.outbox: OutboxDispatcher | None = None

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings instance.

        Returns:
            Dict with default parameters for TraderCoordinator.
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
        await self._initialize_settings()
        self._setup_external_execution()
        self._setup_trading_components()
        self._setup_signal_subscriber()
        self._setup_trade_services()
        await self._recover_engine_state()
        await self._run_trading_loop()

    async def _initialize_settings(self) -> None:
        """Upgrade settings to DB-backed instance for runtime access.

        Without this, accessing DB settings like risk_r_per_trade would
        raise RuntimeError because the bootstrap-only AppSettings does
        not have a SettingsService.
        """
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xpub,
        )
        self.settings = get_settings_with_service(settings_service)
        self.repository = get_repository(self.settings.db_url)
        logger.info("ZMQTrader: Settings service initialized with database access")

    async def _recover_engine_state(self) -> None:
        """Rebuild engine confirmed state from DB executions and active orders.

        Phase 1: Replay persisted executions to reconstruct position/portfolio.
        Phase 2: Query active orders across ALL exchanges, create engines
            for orders that have no executions yet, set order_in_flight.
        Phase 3: Detect fill gaps (DB filled_size vs order filled_size)
            and enter degraded read-only mode if cost basis is unrecoverable.
        """
        now = datetime.now(UTC)
        executions = await self._recover_from_executions(now)
        await self._recover_active_orders(now, executions)
        logger.info(
            f"ZMQTrader: Engine recovery complete: "
            f"{len(self.engines)} engines, "
            f"{sum(1 for e in self.engines.values() if e.order_in_flight)} in-flight, "
            f"{sum(1 for e in self.engines.values() if e.read_only)} degraded"
        )

    async def _recover_from_executions(self, now: datetime) -> list[ExecutionRow]:
        """Phase 1: Replay DB executions to rebuild engine state.

        Returns:
            All recovered execution rows (for fill-gap detection in phase 2).
        """
        try:
            executions = await self.repository.get_executions_for_recovery(as_of=now)
        except Exception as e:
            logger.error(f"ZMQTrader: Failed to query executions for recovery: {e}")
            return []
        if not executions:
            logger.info("ZMQTrader: No executions to recover")
            return []
        fills_by_key: dict[str, list[ExecutionRow]] = {}
        for exe in executions:
            key = f"{exe['instrument']}@{exe['exchange']}-live"
            fills_by_key.setdefault(key, []).append(exe)
        for engine_key, fills in fills_by_key.items():
            engine = await self._create_engine_for_recovery(
                fills[0]["instrument"], fills[0]["exchange"]
            )
            if engine is None:
                continue
            for fill_row in fills:
                self._apply_execution_row_to_engine(engine, fill_row)
            self.engines[engine_key] = engine
            self.last_signal_time[engine_key] = time.time()
            logger.info(
                f"ZMQTrader: Recovered {engine_key}: "
                f"pos={engine.position_qty:.6f}, "
                f"entry={engine.entry_price}, "
                f"fills={len(fills)}"
            )
        return executions

    async def _recover_active_orders(self, now: datetime, executions: list[ExecutionRow]) -> None:
        """Phase 2+3: Process active orders across all exchanges."""
        valid_exchanges = get_args(OrderExchange)
        all_active: list[Any] = []
        for exchange_str in valid_exchanges:
            try:
                orders = await self.repository.get_active_orders_for_recovery(
                    exchange=exchange_str, as_of=now
                )
                all_active.extend(orders)
            except Exception as e:
                logger.error(f"ZMQTrader: Failed to query active orders for {exchange_str}: {e}")
        for db_order in all_active:
            instrument = db_order["instrument"]
            exchange_str = db_order["exchange"]
            key = f"{instrument}@{exchange_str}-live"
            if key not in self.engines:
                engine = await self._create_engine_for_recovery(instrument, exchange_str)
                if engine is None:
                    continue
                self.engines[key] = engine
                self.last_signal_time[key] = time.time()
            engine = self.engines[key]
            engine.order_in_flight = True
            client_oid = db_order.get("client_order_id", "")
            engine.pending_client_order_id = client_oid
            engine._in_flight_since = time.monotonic()
            db_filled = float(db_order.get("filled_size", 0.0))
            exec_filled = sum(
                e["size"] for e in executions if e.get("client_order_id") == client_oid
            )
            if db_filled > 0 and abs(db_filled - exec_filled) > 1e-9:
                engine.read_only = True
                logger.warning(
                    f"ZMQTrader: DEGRADED MODE for {key} - fill gap detected: "
                    f"order filled_size={db_filled}, replayed executions={exec_filled}. "
                    f"Cost basis cannot be reconstructed. Manual resolution required."
                )
            else:
                logger.info(
                    f"ZMQTrader: Recovered in-flight order "
                    f"{db_order.get('client_order_id')} for {key} "
                    f"with fresh timeout window"
                )

    async def _create_engine_for_recovery(
        self, instrument: str, exchange_str: str
    ) -> TradingEngineService | None:
        """Create a TradingEngineService for recovery if exchange is valid."""
        valid_exchanges = get_args(OrderExchange)
        if exchange_str not in valid_exchanges:
            return None
        exchange = cast(OrderExchange, exchange_str)
        assert self.msg_publisher is not None
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
        repo_for_engine = (
            self.repository if isinstance(self.repository, SQLAlchemyRepository) else None
        )
        return TradingEngineService(
            instrument,
            execution_socket=self.msg_publisher,
            risk=risk,
            cfg=EngineConfigModel(),
            instrument_specs=specs_map,
            exchange=exchange,
            repository=repo_for_engine,
            outbox=self.outbox,
        )

    @staticmethod
    def _apply_execution_row_to_engine(
        engine: TradingEngineService, fill_row: ExecutionRow
    ) -> None:
        """Apply a single execution row to engine state during recovery.

        Directly updates portfolio and position without going through
        the full apply_fill path (which requires ExecutionData and
        idempotency tracking not needed for DB replay).

        Args:
            engine: Engine to update.
            fill_row: ExecutionRow dict from DB.
        """
        side = fill_row["side"]
        size = fill_row["size"]
        price = fill_row["price"]
        fee = fill_row["fee"]
        engine.portfolio.update_fill(engine.instrument, side, size, price, fee)
        if side == "buy":
            was_flat = engine.position_qty <= 0
            engine.position_qty += size
            if was_flat and engine.position_qty > 0:
                engine.entry_price = price
        else:
            engine.position_qty = max(engine.position_qty - size, 0.0)
            if engine.position_qty <= 0:
                engine.entry_price = None
        trade_id = fill_row.get("trade_id")
        if trade_id:
            engine.seen_exec_ids.add(trade_id)

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

    async def _dispatch_order_event(self, topic: str, payload: bytes) -> None:
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
            await self._handle_execution_fill(topic, msg)
        elif isinstance(msg, OrderData):
            self._handle_order_status(topic, msg)
        elif isinstance(msg, OrderEventData):
            self._handle_order_event(topic, msg)
        else:
            logger.debug(f"ZMQTrader: Ignoring orders.events message type={msg.type} on {topic}")

    def _find_engine_for_fill(self, fill: ExecutionData) -> TradingEngineService | None:
        """Find engine matching an execution fill by client_order_id or instrument.

        Searches engines in two passes:
        1. Exact match on pending_client_order_id (current in-flight order).
        2. Fallback match on instrument + exchange (for late fills after timeout).

        Args:
            fill: Execution fill to match.

        Returns:
            Matching engine or None if no engine found.
        """
        for engine in self.engines.values():
            if engine.pending_client_order_id == fill.client_order_id:
                return engine
        for engine in self.engines.values():
            if engine.instrument == fill.instrument and engine.exchange == fill.exchange:
                return engine
        return None

    async def _handle_execution_fill(self, topic: str, fill: ExecutionData) -> None:
        """Handle execution fill event from ZMQ.

        Finds the matching engine and applies the fill using delta semantics.
        Duplicate fills are silently dropped via idempotency guard.

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
        engine = self._find_engine_for_fill(fill)
        if engine is None:
            logger.info(
                f"ZMQTrader: No engine found for fill {fill.client_order_id} "
                f"{fill.instrument} on {fill.exchange}"
            )
            return
        applied = engine.apply_fill(fill)
        if applied:
            logger.info(
                f"ZMQTrader: Fill applied - {fill.client_order_id} "
                f"{fill.side} {fill.last_size}@{fill.last_price} "
                f"{fill.instrument} on {parsed.exchange} "
                f"(pos={engine.position_qty:.6f}, status={fill.status})"
            )
            await self._sync_fill_to_trade_service(fill, engine)
        else:
            logger.info(
                f"ZMQTrader: Duplicate fill ignored - {fill.client_order_id} "
                f"trade_id={fill.trade_id} {fill.instrument} on {parsed.exchange}"
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
            for engine in self.engines.values():
                if engine.clear_pending_intent(order_status.client_order_id):
                    logger.info(
                        f"ZMQTrader: Cleared in-flight for rejected order "
                        f"{order_status.client_order_id} on {parsed.exchange}"
                    )
                    break
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
        self._sync_status_to_trade_service(order_status, parsed)

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
        if parsed.suffix in ("cancelled", "expired"):
            for engine in self.engines.values():
                if engine.clear_pending_intent(order_event.client_order_id):
                    logger.info(
                        f"ZMQTrader: Cleared in-flight for {parsed.suffix} order "
                        f"{order_event.client_order_id} on {parsed.exchange}"
                    )
                    break
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
        self._sync_order_event_to_trade_service(order_event, parsed)

    async def _sync_fill_to_trade_service(
        self, fill: ExecutionData, engine: TradingEngineService
    ) -> None:
        """Shadow-write fill to TradeService and persist checkpoint.

        Constructs a VenueEventRow-like dict from the ZMQ ExecutionData,
        applies it to TradeService, updates BalanceService, and writes
        a checkpoint to DB for durable recovery.

        Args:
            fill: Execution fill data from ZMQ.
            engine: Engine that processed the fill (for shard_key).
        """
        shard_key = engine._shard_key
        venue_event: VenueEventRow = {
            "id": int(time.monotonic_ns()),
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": fill.session_id,
            "sequence_id": fill.sequence_id,
            "event_type": "fill_observed",
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": fill.exchange,
            "instrument": fill.instrument,
            "mode": engine.mode,
            "exchange_order_id": fill.exchange_order_id,
            "client_order_id": fill.client_order_id,
            "venue_client_id": None,
            "side": fill.side,
            "status": fill.status,
            "fill_price": fill.last_price,
            "fill_size": fill.last_size,
            "cum_fill_size": fill.size,
            "fee": fill.fee,
            "fee_asset": fill.fee_asset,
            "exec_id": None,
            "trade_id": fill.trade_id,
            "error": None,
            "venue_timestamp": fill.executed_at,
            "received_at": datetime.now(UTC),
        }
        self.trade_service.apply_venue_event(venue_event)
        pos = self.trade_service.get_position(shard_key)
        self.balance_service.on_position_changed(
            shard_key=shard_key,
            position_qty=pos.position_qty,
            entry_price=pos.entry_price,
            cash=self.trade_service.get_equity(shard_key),
            peak_equity=self.trade_service.get_peak_equity(shard_key),
            realized_pnl=pos.realized_pnl,
        )
        await self._persist_checkpoint(shard_key)

    def _sync_status_to_trade_service(self, order_status: OrderData, parsed: Any) -> None:
        """Shadow-write order status to TradeService.

        Maps ZMQ OrderData status events (accepted, rejected) to
        synthetic VenueEventRow and applies to TradeService.

        Args:
            order_status: Order status data from ZMQ.
            parsed: Parsed topic with exchange, instrument, suffix.
        """
        mode = (
            ExecutionModeEnum.PAPER
            if parsed.exchange == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )
        shard_key = f"{parsed.exchange}.{parsed.instrument}.{mode}"
        event_type_map = {
            "accepted": "order_accepted",
            "rejected": "order_rejected",
            "submitted": "order_accepted",
        }
        event_type = event_type_map.get(parsed.suffix, "order_accepted")
        venue_event: VenueEventRow = {
            "id": int(time.monotonic_ns()),
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": order_status.session_id,
            "sequence_id": order_status.sequence_id,
            "event_type": event_type,
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": order_status.exchange,
            "instrument": order_status.instrument,
            "mode": mode,
            "exchange_order_id": order_status.exchange_order_id,
            "client_order_id": order_status.client_order_id,
            "venue_client_id": None,
            "side": order_status.side,
            "status": parsed.suffix,
            "fill_price": None,
            "fill_size": None,
            "cum_fill_size": None,
            "fee": None,
            "fee_asset": None,
            "exec_id": None,
            "trade_id": None,
            "error": order_status.error,
            "venue_timestamp": None,
            "received_at": datetime.now(UTC),
        }
        self.trade_service.apply_venue_event(venue_event)

    def _sync_order_event_to_trade_service(self, order_event: OrderEventData, parsed: Any) -> None:
        """Shadow-write order event (cancel/expire) to TradeService.

        Maps ZMQ OrderEventData to synthetic VenueEventRow and applies
        to TradeService for terminal state tracking.

        Args:
            order_event: Order event data from ZMQ.
            parsed: Parsed topic with exchange, instrument, suffix.
        """
        if parsed.suffix not in ("cancelled", "expired"):
            return
        mode = (
            ExecutionModeEnum.PAPER
            if parsed.exchange == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )
        shard_key = f"{parsed.exchange}.{parsed.instrument}.{mode}"
        venue_event: VenueEventRow = {
            "id": int(time.monotonic_ns()),
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": order_event.session_id,
            "sequence_id": order_event.sequence_id,
            "event_type": "order_terminal",
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": order_event.exchange,
            "instrument": order_event.instrument,
            "mode": mode,
            "exchange_order_id": order_event.exchange_order_id,
            "client_order_id": order_event.client_order_id,
            "venue_client_id": None,
            "side": None,
            "status": parsed.suffix,
            "fill_price": None,
            "fill_size": None,
            "cum_fill_size": None,
            "fee": None,
            "fee_asset": None,
            "exec_id": None,
            "trade_id": None,
            "error": None,
            "venue_timestamp": None,
            "received_at": datetime.now(UTC),
        }
        self.trade_service.apply_venue_event(venue_event)

    async def _persist_checkpoint(self, shard_key: str) -> None:
        """Write trade projection checkpoint to DB.

        Persists the current shard state for durable recovery. Silent
        no-op if repository is not SQLAlchemyRepository.

        Args:
            shard_key: Shard to checkpoint.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        snap = self.trade_service.snapshot_for_checkpoint(shard_key)
        now = datetime.now(UTC)
        ep = snap["entry_price"]
        oci = snap["open_command_ids"]
        real_watermark = await self.repository.get_latest_venue_event_id(shard_key)
        try:
            await self.repository.upsert_checkpoint(
                {
                    "shard_key": shard_key,
                    "position_qty": cast(float, snap["position_qty"]),
                    "entry_price": cast(float, ep) if ep is not None else None,
                    "cash": cast(float, snap["cash"]),
                    "peak_equity": cast(float, snap["peak_equity"]),
                    "realized_pnl": cast(float, snap["realized_pnl"]),
                    "turnover": cast(float, snap["turnover"]),
                    "last_venue_event_id": real_watermark,
                    "last_venue_event_at": now if real_watermark is not None else None,
                    "open_command_ids": cast(str, oci) if oci is not None else None,
                    "checkpoint_at": now,
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(f"checkpoint.{shard_key}"),
                    "bus_time": now,
                }
            )
        except Exception:
            logger.exception(f"TraderCoordinator: Failed to persist checkpoint for {shard_key}")

    async def stop(self) -> None:
        """Stop the trader coordinator and cleanup resources.

        Closes all ZMQ sockets and terminates contexts.
        """
        logger.info("Stopping ZMQ Signal TraderCoordinator")
        if self.outbox is not None:
            self.outbox.stop()
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
        now = datetime.now(UTC)
        symbol_pid = await resolve_symbol_public_id(self.repository, instrument, as_of=now)
        if symbol_pid is None:
            logger.warning(
                f"ZMQTrader: No active Symbol row for {instrument}, skipping instrument upsert"
            )
            return
        await self.repository.ensure_instrument(
            symbol_public_id=symbol_pid,
            exchange=exchange,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=now,
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
        self.msg_publisher = MessagePublisher(self.execution_publisher, self._tracker)
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

    def _setup_trade_services(self) -> None:
        """Initialize trade domain services and optional outbox dispatcher.

        TradeService and BalanceService are initialized in __init__.
        OutboxDispatcher is created when use_durable_commands is enabled
        in settings. Otherwise, dual-write mode: engine publishes
        directly to ZMQ while also writing TradeCommand to DB.
        """
        use_durable = getattr(self.settings, "use_durable_commands", False)
        if use_durable and isinstance(self.repository, SQLAlchemyRepository):
            self.outbox = OutboxDispatcher(
                repository=self.repository,
                publish_fn=self._outbox_publish,
                poll_interval=0.05,
            )
            logger.info("TraderCoordinator: durable command mode (outbox active)")
        else:
            logger.info("TraderCoordinator: dual-write mode (direct ZMQ + DB audit)")

    def _create_reconciliation_tasks(self) -> list[asyncio.Task[None]]:
        """Create per-exchange reconciliation loop tasks if durable mode is enabled.

        One ReconciliationLoop per configured exchange, each querying
        TradeCommand.exchange with exact match. Runs in the coordinator
        process sharing the TradeService for circuit breaker state.

        Returns:
            List of asyncio tasks (empty if not in durable mode).
        """
        use_durable = getattr(self.settings, "use_durable_commands", False)
        if not use_durable or not isinstance(self.repository, SQLAlchemyRepository):
            return []
        exchanges: list[str] = list(get_args(OrderExchange))
        tasks: list[asyncio.Task[None]] = []
        for exchange_name in exchanges:
            recon = ReconciliationLoop(
                exchange_name=exchange_name,
                repository=self.repository,
                trade_service=self.trade_service,
                interval_seconds=60.0,
            )
            tasks.append(asyncio.create_task(recon.run()))
        logger.info(f"TraderCoordinator: reconciliation loops enabled for {exchanges}")
        return tasks

    async def _outbox_publish(self, cmd: TradeCommandRow) -> None:
        """Publish a trade command from outbox to ZMQ.

        Converts a TradeCommandRow dict to OrderRequestData and publishes
        to the order command topic.

        Args:
            cmd: TradeCommandRow dict from the outbox dispatcher.
        """
        assert self.msg_publisher is not None
        exchange = cast(OrderExchange, cmd["exchange"])
        topic = order_command_topic(exchange, cmd["instrument"], "submit")
        order = OrderRequestData(
            public_id=cmd["client_order_id"],
            timestamp=datetime.now(UTC),
            session_id=cmd["session_id"],
            sequence_id=cmd["sequence_id"],
            strategy_id=cmd["strategy_id"],
            instrument=cmd["instrument"],
            mode=cast(ExecutionMode, cmd["mode"]),
            side=cast(TradeSide, cmd["side"]),
            order_type=cast(OrderType, cmd["order_type"]),
            quantity=cmd["quantity"],
            price=cmd["price"],
            client_order_id=cmd["client_order_id"],
            exchange=exchange,
        )
        await self.msg_publisher.send(topic, order)

    async def _run_trading_loop(self) -> None:
        """Run the main trading loop.

        Spawns tasks for signal listening and health monitoring.
        Runs until cancelled.
        """
        tasks = [
            asyncio.create_task(self._listen_signals()),
            asyncio.create_task(self._signal_health_monitor()),
        ]
        if self.outbox is not None:
            tasks.append(asyncio.create_task(self.outbox.run()))
        tasks.extend(self._create_reconciliation_tasks())
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
                    await self._dispatch_order_event(topic_str, msg_bytes)
                    continue
                signal = SignalData.from_json(msg_bytes.decode())
                self._gap_detector.check(topic_str, signal.session_id, signal.sequence_id)
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
        shard_key = f"{exchange}.{instrument}.{mode}"
        if self.trade_service.is_halted(shard_key):
            logger.warning(f"ZMQTrader: shard {shard_key} is halted, dropping signal")
            return
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
            assert self.msg_publisher is not None, "MessagePublisher not initialized"

            repo_for_engine = (
                self.repository if isinstance(self.repository, SQLAlchemyRepository) else None
            )
            self.engines[engine_key] = TradingEngineService(
                instrument,
                execution_socket=self.msg_publisher,
                risk=risk,
                cfg=EngineConfigModel(),
                instrument_specs=specs_map,
                exchange=exchange,
                repository=repo_for_engine,
                outbox=self.outbox,
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
