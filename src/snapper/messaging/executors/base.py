"""Base class for order execution services.

Provides common functionality for ZeroMQ-based execution services
that handle order placement and fill reporting.
"""

import asyncio
import time
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from uuid import uuid7

import httpx
import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import SettingsService
from snapper.config.credentials import CredentialResolver
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.types import CancelEventType
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import FillStatus
from snapper.core.types import FillStatusEnum
from snapper.core.types import HealthStatusEnum
from snapper.core.types import OrderCommandEnum
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderEventType
from snapper.core.types import OrderExchange
from snapper.core.types import ReplaceEventType
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import RecordVenueEventParams
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecType
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import to_fill_status
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import heartbeat_topic
from snapper.messaging.topics.builders import order_commands_prefix
from snapper.messaging.topics.builders import order_event_topic
from snapper.messaging.topics.builders import parse_order_command_topic
from snapper.utils.logging import set_log_context

_EXCHANGE_NOT_INIT_MSG = "Exchange client not initialized"


@dataclass
class PendingOrderState:
    """Per-order executor state for tracking orders through their lifecycle.

    Consolidates request data, DB identifiers, and cumulative fill tracking
    into a single model. Used by the executor base for persistence and
    delta fill computation.

    Attributes:
        request: Original order request data.
        db_order_id: Database row ID from _log_order_to_db (for status updates).
        order_public_id: Logical order identity (for execution inserts).
        exchange_order_id: Exchange-assigned order ID (set after ACK).
        last_seen_cum_qty: Running cumulative fill quantity for delta fallback.
    """

    request: OrderRequestData
    db_order_id: int | None = field(default=None)
    order_public_id: str | None = field(default=None)
    exchange_order_id: str | None = field(default=None)
    last_seen_cum_qty: float = field(default=0.0)


class ExchangeExecutorService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for executing orders on exchanges via ZMQ messaging."""

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters for the executor service.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary of default parameters for this executor.
        """
        return {"wallet_public_id": ""}

    def __init__(self, wallet_public_id: str = "") -> None:
        """Initialize the instance.

        Args:
            wallet_public_id: Multi-tenant routing key. When
                non-empty, the executor loads credentials from
                ``wallet_credentials`` via ``CredentialResolver`` at
                startup and drops incoming command messages whose
                ``wallet_public_id`` does not match. When empty (the
                legacy default preserved for tests that instantiate
                concrete executors directly), ``_resolve_credentials``
                is a no-op and ``self._credentials`` stays ``None``;
                those tests must inject ``self._credentials`` directly
                before calling ``start()``, otherwise the concrete
                ``_create_exchange_client`` raises ``RuntimeError``.
        """
        self.settings = get_settings()
        self.wallet_public_id: str = wallet_public_id
        self._credentials: dict[str, str] | None = None
        self.context: zmq.asyncio.Context | None = None
        self.subscriber: ValidatedSubscriber | None = None
        self.publisher: ValidatedPublisher | None = None
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self._gap_detector: GapDetector = GapDetector()
        self.running = False
        self.heartbeat_seq = 0
        self.exchange_client: T | None = None
        self.repository: Repository | None = None
        self.pending_orders: dict[str, PendingOrderState] = {}
        self.client_by_exchange: dict[str, str] = {}
        self.orphaned_executions: dict[str, tuple[ExecutionUpdate, float]] = {}
        self.orphan_ttl_seconds: float = 5.0
        self.orphan_drop_count: int = 0
        self._background_tasks: set[asyncio.Task[None]] = set()

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Exchange client instance for this executor.
        """
        ...

    @abstractmethod
    def _get_exchange_name(self) -> OrderExchange:
        """Return the trading exchange identifier.

        Returns:
            Trading exchange enum value for this executor.
        """
        ...

    async def _initialize_settings(self) -> None:
        """Initialize settings service with database access."""
        self.repository = get_repository(self.settings.db_url)
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xsub,
        )
        self.settings = get_settings_with_service(settings_service)
        logger.info("AppSettings service initialized with database access")

    async def _resolve_credentials(self, exchange_name: OrderExchange) -> None:
        """Load wallet-scoped credentials when wallet_public_id is populated.

        Per-wallet executor instances resolve their credentials from
        ``wallet_credentials`` via ``CredentialResolver`` exactly once
        during startup. When ``self.wallet_public_id`` is empty
        (legacy template path used by tests that instantiate concrete
        executors directly), no lookup happens and ``self._credentials``
        stays ``None``; those tests must inject ``self._credentials``
        directly before ``start()`` or the concrete
        ``_create_exchange_client`` will raise ``RuntimeError``.

        Args:
            exchange_name: Exchange identifier used as the credential
                lookup key (normalized to lowercase by the resolver).

        Raises:
            RuntimeError: When ``self.wallet_public_id`` is set but the
                repository has not yet been initialized.
            CredentialNotFoundError: When no active credential row
                exists for the wallet/exchange pair. Propagated so the
                executor process fails fast at startup.
        """
        if not self.wallet_public_id:
            return
        if self.repository is None:
            raise RuntimeError(
                "ExchangeExecutorService._resolve_credentials requires "
                "repository initialization before credential lookup."
            )
        resolver = CredentialResolver(self.repository)
        self._credentials = await resolver.get_credentials(
            exchange=exchange_name,
            wallet_public_id=self.wallet_public_id,
        )
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: Resolved credentials "
            f"for wallet={self.wallet_public_id}"
        )

    def _is_for_my_wallet(self, msg: Any) -> bool:
        """Filter guard: is this command targeted at my wallet instance?

        Per-wallet executor instances all subscribe to the same
        exchange-prefix topic. Each instance drops commands whose
        ``wallet_public_id`` does not match its own. When
        ``self.wallet_public_id`` is empty (legacy template path),
        every message is accepted — this branch only fires for
        single-wallet template instances and tests that instantiate
        executors directly without populating a wallet.

        Args:
            msg: Parsed command message (OrderRequestData,
                OrderCancelData, or OrderReplaceData). The filter
                reads the optional ``wallet_public_id`` attribute.

        Returns:
            True if the message should be processed by this executor
            instance, False if it must be silently dropped.
        """
        if not self.wallet_public_id:
            return True
        msg_wallet = getattr(msg, "wallet_public_id", "") or ""
        return msg_wallet == self.wallet_public_id

    def _setup_zmq_sockets(self, exchange_name: OrderExchange) -> None:
        """Create and connect ZMQ subscriber and publisher sockets.

        Args:
            exchange_name: Exchange name for topic prefix construction.
        """
        self.context = zmq.asyncio.Context()
        raw_sub_socket = self.context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_ORDER_FLOW)
        raw_sub_socket.connect(self.settings.zmq_broker_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        cmd_prefix = order_commands_prefix(exchange_name)
        self.subscriber.subscribe(cmd_prefix)
        self.subscriber.subscribe("system.symbol_aliases")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: Subscribed to {cmd_prefix}, "
            f"system.symbol_aliases, system.settings from {self.settings.zmq_broker_xpub}"
        )
        raw_pub_socket = self.context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_ORDER_FLOW)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        self.msg_publisher = MessagePublisher(self.publisher, self._tracker)
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: "
            f"Publishing to broker {self.settings.zmq_broker_xsub}"
        )

    async def _recover_pending_orders(self, exchange_name: OrderExchange) -> None:
        """Rebuild pending order state from exchange and database on startup.

        Runs before the order handler loop to ensure in-flight orders from
        a previous session are tracked. This prevents orphaned execution
        events from being dropped.

        Steps:
        1. Query exchange for open orders via get_orders(status=OPEN).
        2. Query DB for active orders via get_active_orders_for_recovery.
        3. Correlate: rebuild PendingOrderState for orders found on exchange.
        4. For DB-active orders missing on exchange: verify via get_order()
           and mark terminal if genuinely absent.

        Args:
            exchange_name: Exchange name for logging and DB queries.
        """
        assert self.exchange_client is not None
        if self.repository is None:
            logger.warning(f"[{exchange_name}] No repository, skipping recovery")
            return
        try:
            exchange_open = await self.exchange_client.get_orders(
                status=ExchangeOrderStatusEnum.OPEN
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Failed to query exchange open orders: {e}")
            exchange_open = []
        exchange_by_id: dict[str, Any] = {o.id: o for o in exchange_open}
        now = datetime.now(UTC)
        try:
            db_active = await self.repository.get_active_orders_for_recovery(
                exchange=exchange_name,
                as_of=now,
                wallet_public_id=self.wallet_public_id,
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Failed to query DB active orders: {e}")
            db_active = []
        recovered = 0
        for db_order in db_active:
            result = await self._recover_single_order(db_order, exchange_by_id, exchange_name)
            if result:
                recovered += 1
        logger.info(
            f"[{exchange_name}] Recovery complete: {recovered} pending orders restored "
            f"from {len(db_active)} DB active / {len(exchange_open)} exchange open"
        )

    async def _recover_single_order(
        self,
        db_order: OrderRow,
        exchange_by_id: dict[str, Any],
        exchange_name: OrderExchange,
    ) -> bool:
        """Attempt to recover a single order from DB into pending state.

        Args:
            db_order: Order row from DB recovery query.
            exchange_by_id: Map of exchange_order_id to ExchangeOrderSnapshot.
            exchange_name: Exchange name for logging.

        Returns:
            True if order was recovered into pending state.
        """
        assert self.exchange_client is not None
        exchange_order_id = db_order.get("exchange_order_id")
        client_order_id = db_order.get("client_order_id", "")
        if not exchange_order_id or not client_order_id:
            return False
        filled = await self._resolve_order_fill_state(
            exchange_order_id, client_order_id, db_order, exchange_by_id, exchange_name
        )
        if filled is None:
            return False
        fake_request = OrderRequestData(
            public_id=db_order["public_id"],
            timestamp=db_order["timestamp"],
            session_id=db_order["session_id"],
            sequence_id=db_order["sequence_id"],
            strategy_id="recovered",
            instrument=db_order["instrument"],
            mode=(
                ExecutionModeEnum.LIVE
                if exchange_name != ExchangeEnum.PAPER
                else ExecutionModeEnum.PAPER
            ),
            side=cast(Any, db_order["side"]),
            order_type=cast(Any, db_order["order_type"]),
            quantity=db_order["size"],
            price=db_order.get("price"),
            client_order_id=client_order_id,
            exchange=exchange_name,
            wallet_public_id=db_order.get("wallet_public_id") or self.wallet_public_id,
            operator_public_id=db_order.get("operator_public_id"),
        )
        pending = PendingOrderState(
            request=fake_request,
            db_order_id=None,
            order_public_id=db_order["public_id"],
            exchange_order_id=exchange_order_id,
            last_seen_cum_qty=filled,
        )
        self.pending_orders[client_order_id] = pending
        self.client_by_exchange[exchange_order_id] = client_order_id
        return True

    async def _resolve_order_fill_state(
        self,
        exchange_order_id: str,
        client_order_id: str,
        db_order: OrderRow,
        exchange_by_id: dict[str, Any],
        exchange_name: OrderExchange,
    ) -> float | None:
        """Resolve the current fill state of an order against the exchange.

        Returns cumulative filled quantity if the order is still active,
        or None if the order is terminal or cannot be verified.

        Args:
            exchange_order_id: Exchange-assigned order ID.
            client_order_id: Client-assigned order ID.
            db_order: DB order row for instrument context.
            exchange_by_id: Pre-fetched exchange open order snapshots.
            exchange_name: Exchange name for logging.

        Returns:
            Cumulative filled quantity, or None if order is terminal/unverifiable.
        """
        assert self.exchange_client is not None
        if exchange_order_id in exchange_by_id:
            snap = exchange_by_id[exchange_order_id]
            return snap.filled or 0.0
        try:
            snap = await self.exchange_client.get_order(
                exchange_order_id, symbol=db_order["instrument"]
            )
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recovery: cannot verify order "
                f"{exchange_order_id} on exchange: {e}"
            )
            return None
        terminal = (
            ExchangeOrderStatusEnum.CLOSED,
            ExchangeOrderStatusEnum.CANCELED,
            ExchangeOrderStatusEnum.EXPIRED,
        )
        if snap.status in terminal:
            await self.exchange_client._log_order_update_to_db(
                db_order_id=db_order["sequence_id"],
                status=snap.status,
            )
            logger.info(
                f"[{exchange_name}] Recovery: order {client_order_id} "
                f"is {snap.status.value} on exchange, updated DB"
            )
            return None
        return snap.filled or 0.0

    async def start(self) -> None:
        """Start the execution service and subscribe to order topics."""
        exchange_name = self._get_exchange_name()
        set_log_context(f"exec:{exchange_name}")
        if self.running:
            logger.warning(f"{exchange_name} execution service already running")
            return
        await self._initialize_settings()
        await self._resolve_credentials(exchange_name)
        self.exchange_client = self._create_exchange_client()
        self.exchange_client.set_tracker(self._tracker)
        self._setup_zmq_sockets(exchange_name)
        supports_ws = self.exchange_client.supports_websocket_executions
        async with self.exchange_client:
            logger.info(
                f"ExchangeExecutorService[{exchange_name}]: "
                f"Exchange client initialized with WebSocket"
            )
            await self._recover_pending_orders(exchange_name)
            self.running = True
            tasks = [
                asyncio.create_task(self._order_handler()),
                asyncio.create_task(self._heartbeat_loop()),
            ]
            if supports_ws:
                tasks.append(asyncio.create_task(self._execution_handler()))
            tasks.append(asyncio.create_task(self._reconciliation_handler()))
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                logger.info(f"ExchangeExecutorService[{exchange_name}] tasks cancelled")
                raise

    def _create_reconciliation_task(
        self, exchange_name: OrderExchange
    ) -> asyncio.Task[None] | None:
        """Reconciliation placeholder — runs from coordinator, not executor.

        Reconciliation needs the shared TradeService (for circuit breaker)
        which lives in the coordinator process. Returns None so executor
        does not run its own isolated reconciliation.

        Args:
            exchange_name: Exchange identifier (unused).

        Returns:
            Always None.
        """
        return None

    async def stop(self) -> None:
        """Stop the execution service and close ZMQ connections."""
        if not self.running:
            return
        self.running = False
        if self.subscriber:
            self.subscriber.setsockopt(zmq.LINGER, 0)
            self.subscriber.close()
        if self.publisher:
            self.publisher.setsockopt(zmq.LINGER, 0)
            self.publisher.close()
        if self.context:
            self.context.term()
        exchange_name = self._get_exchange_name()
        logger.info(f"ExchangeExecutorService[{exchange_name}] stopped")

    async def _dispatch_command(
        self, parsed_suffix: str, payload_str: str, exchange_name: OrderExchange, instrument: str
    ) -> None:
        """Dispatch a parsed order command to its handler.

        Args:
            parsed_suffix: Command suffix (submit, cancel, replace).
            payload_str: JSON payload string.
            exchange_name: Exchange name from topic.
            instrument: Instrument from topic.
        """
        handler_map = {
            OrderCommandEnum.SUBMIT.value: self._handle_submit_command,
            OrderCommandEnum.CANCEL.value: self._handle_cancel_command,
            OrderCommandEnum.REPLACE.value: self._handle_replace_command,
        }
        handler = handler_map.get(parsed_suffix)
        if handler:
            await handler(payload_str, exchange_name, instrument)
        else:
            logger.debug(f"Ignoring unknown command suffix: {parsed_suffix}")

    async def _route_message(
        self, topic_str: str, payload_str: str, exchange_name: OrderExchange, commands_prefix: str
    ) -> None:
        """Route a received ZMQ message to the appropriate handler.

        Args:
            topic_str: ZMQ topic string.
            payload_str: Decoded payload string.
            exchange_name: Exchange name for this executor.
            commands_prefix: Expected command topic prefix.
        """
        if topic_str.startswith(commands_prefix):
            parsed = parse_order_command_topic(topic_str)
            if parsed is None:
                logger.warning(f"Malformed command topic: {topic_str}")
                return
            await self._dispatch_command(
                parsed.suffix, payload_str, exchange_name, parsed.instrument
            )
        elif topic_str == "system.symbol_aliases":
            self._handle_symbol_alias_update(payload_str)
        elif topic_str == "system.settings":
            self._handle_settings_update(payload_str)
        else:
            logger.warning(f"Received message on unexpected topic: {topic_str}")

    async def _order_handler(self) -> None:
        """Process incoming order commands from ZMQ subscription.

        Dispatches to appropriate handler based on command suffix:
        - .submit -> _process_order (OrderRequestData)
        - .cancel -> _process_cancel (OrderCancelData)
        - .replace -> _process_replace (OrderReplaceData)

        Topic invariant enforced: topic parts must match payload fields.
        """
        exchange_name = self._get_exchange_name()
        commands_prefix = order_commands_prefix(exchange_name)
        while self.running:
            try:
                if not self.subscriber:
                    await asyncio.sleep(0.1)
                    continue
                topic_str, payload_bytes = await self.subscriber.recv_multipart()
                payload_str = payload_bytes.decode("utf-8")
                try:
                    parsed_msg = parse_message(payload_str)
                    parsed_wallet = getattr(parsed_msg, "wallet_public_id", "") or ""
                    self._gap_detector.check(
                        topic_str,
                        parsed_msg.session_id,
                        parsed_msg.sequence_id,
                        wallet_public_id=parsed_wallet,
                    )
                except MessageParseError:
                    pass
                await self._route_message(topic_str, payload_str, exchange_name, commands_prefix)
            except Exception as e:
                if self.running:
                    logger.error(f"Error handling order: {e}")

    @staticmethod
    def _validate_command_invariants(
        msg: Any, exchange_name: OrderExchange, topic_instrument: str
    ) -> bool:
        """Validate topic/payload invariants for a command message.

        Args:
            msg: Parsed command message with exchange and instrument fields.
            exchange_name: Expected exchange from topic.
            topic_instrument: Expected instrument from topic.

        Returns:
            True if invariants hold, False otherwise.
        """
        if msg.exchange != exchange_name:
            logger.warning(
                f"Invariant violation: payload exchange '{msg.exchange}' "
                f"!= topic exchange '{exchange_name}'"
            )
            return False
        if msg.instrument != topic_instrument:
            logger.warning(
                f"Invariant violation: payload instrument '{msg.instrument}' "
                f"!= topic instrument '{topic_instrument}'"
            )
            return False
        return True

    async def _handle_submit_command(
        self, payload_str: str, exchange_name: OrderExchange, topic_instrument: str
    ) -> None:
        """Handle submit command from orders.commands.*.*.submit topic.

        Args:
            payload_str: JSON payload string.
            exchange_name: Expected exchange name from topic.
            topic_instrument: Instrument from topic for invariant check.
        """
        try:
            order_msg = parse_message(payload_str)
        except MessageParseError as e:
            logger.warning(f"[{exchange_name}] Invalid submit command payload: {e}")
            return
        if not isinstance(order_msg, OrderRequestData):
            logger.warning(f"Received non-order message on submit topic: {order_msg.type}")
            return
        if not self._is_for_my_wallet(order_msg):
            return
        if self._validate_command_invariants(order_msg, exchange_name, topic_instrument):
            await self._process_order(order_msg)

    async def _handle_cancel_command(
        self, payload_str: str, exchange_name: OrderExchange, topic_instrument: str
    ) -> None:
        """Handle cancel command from orders.commands.*.*.cancel topic.

        Args:
            payload_str: JSON payload string.
            exchange_name: Expected exchange name from topic.
            topic_instrument: Instrument from topic for invariant check.
        """
        try:
            cancel_msg = parse_message(payload_str)
        except MessageParseError as e:
            logger.warning(f"[{exchange_name}] Invalid cancel command payload: {e}")
            return
        if not isinstance(cancel_msg, OrderCancelData):
            logger.warning(f"Received non-cancel message on cancel topic: {cancel_msg.type}")
            return
        if not self._is_for_my_wallet(cancel_msg):
            return
        if self._validate_command_invariants(cancel_msg, exchange_name, topic_instrument):
            await self._process_cancel(cancel_msg)

    async def _handle_replace_command(
        self, payload_str: str, exchange_name: OrderExchange, topic_instrument: str
    ) -> None:
        """Handle replace command from orders.commands.*.*.replace topic.

        Args:
            payload_str: JSON payload string.
            exchange_name: Expected exchange name from topic.
            topic_instrument: Instrument from topic for invariant check.
        """
        try:
            replace_msg = parse_message(payload_str)
        except MessageParseError as e:
            logger.warning(f"[{exchange_name}] Invalid replace command payload: {e}")
            return
        if not isinstance(replace_msg, OrderReplaceData):
            logger.warning(f"Received non-replace message on replace topic: {replace_msg.type}")
            return
        if not self._is_for_my_wallet(replace_msg):
            return
        if self._validate_command_invariants(replace_msg, exchange_name, topic_instrument):
            await self._process_replace(replace_msg)

    async def _process_order(self, order: OrderRequestData) -> None:
        """Submit an order to the exchange and handle the response.

        Order correlation uses two-level mapping:
        - pending_orders: keyed by client_order_id (always present, unique)
        - client_by_exchange: maps exchange_order_id -> client_order_id (after ACK)

        Publishes order events to orders.events.{exchange}.{instrument}.{event}:
        - submitted: Executor accepted command, sending to exchange
        - accepted: Exchange ACK returned order_id (sync REST response)
        - rejected: Exchange rejected order or validation failed
        - executed: Order executed (see _execution_handler for WebSocket fills)

        Note: 'accepted' is published when the exchange REST API returns an order_id,
        confirming the order was received and queued. This is a synchronous response.
        Actual fills come asynchronously via WebSocket execution updates.

        Args:
            order: Order request data containing order details.
        """
        exchange_name = self._get_exchange_name()
        if not is_tradeable(order.instrument, exchange_name):
            logger.warning(
                f"[{exchange_name}] Rejecting order {order.client_order_id}: "
                f"instrument {order.instrument} not tradeable"
            )
            self.pending_orders.pop(order.client_order_id, None)
            await self._publish_order_status(order, OrderEventEnum.REJECTED)
            return
        try:
            self.pending_orders[order.client_order_id] = PendingOrderState(request=order)
            await self._publish_order_status(order, OrderEventEnum.SUBMITTED)
            exchange_order_id = await self._execute_live_order(order)
            if exchange_order_id:
                self.client_by_exchange[exchange_order_id] = order.client_order_id
                self._try_process_orphaned(exchange_order_id, order.client_order_id)
                await self._record_venue_event(
                    {
                        "event_type": "order_accepted",
                        "exchange_name": exchange_name,
                        "instrument": order.instrument,
                        "exchange_order_id": exchange_order_id,
                        "client_order_id": order.client_order_id,
                        "side": order.side,
                        "strategy_tag": order.strategy_tag,
                    }
                )
                await self._publish_order_status(order, OrderEventEnum.ACCEPTED, exchange_order_id)
                logger.info(
                    f"[{exchange_name}] Order {order.client_order_id} "
                    f"accepted as {exchange_order_id}, waiting for execution"
                )
            else:
                logger.warning(
                    f"[{exchange_name}] Order {order.client_order_id} rejected by exchange"
                )
                self.pending_orders.pop(order.client_order_id, None)
                await self._publish_order_status(order, OrderEventEnum.REJECTED)
                await self._record_venue_event(
                    {
                        "event_type": "order_rejected",
                        "exchange_name": exchange_name,
                        "instrument": order.instrument,
                        "client_order_id": order.client_order_id,
                        "side": order.side,
                        "error": "rejected by exchange",
                        "strategy_tag": order.strategy_tag,
                    }
                )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing order {order.client_order_id}: {e}")
            self.pending_orders.pop(order.client_order_id, None)
            await self._publish_order_status(order, OrderEventEnum.REJECTED)
            await self._record_venue_event(
                {
                    "event_type": "order_rejected",
                    "exchange_name": exchange_name,
                    "instrument": order.instrument,
                    "client_order_id": order.client_order_id,
                    "side": order.side,
                    "error": str(e),
                    "strategy_tag": order.strategy_tag,
                }
            )

    async def _process_cancel(self, cancel: OrderCancelData) -> None:
        """Cancel an existing order on the exchange.

        Cleans up pending_orders and client_by_exchange on success.

        Args:
            cancel: Cancel request data containing order ID to cancel.
        """
        exchange_name = self._get_exchange_name()
        try:
            assert self.exchange_client is not None, _EXCHANGE_NOT_INIT_MSG
            result = await self.exchange_client.cancel_order(
                cancel.exchange_order_id, cancel.instrument
            )
            if result and result.status == ExchangeOrderStatusEnum.CANCELED:
                client_id = self.client_by_exchange.pop(cancel.exchange_order_id, None)
                if client_id:
                    pending = self.pending_orders.pop(client_id, None)
                    if pending and pending.db_order_id is not None:
                        assert self.exchange_client is not None
                        await self.exchange_client._log_order_update_to_db(
                            db_order_id=pending.db_order_id,
                            status=ExchangeOrderStatusEnum.CANCELED,
                        )
                await self._publish_cancel_event(cancel, OrderEventEnum.CANCELLED)
                logger.info(
                    f"[{exchange_name}] Order {cancel.exchange_order_id} cancelled successfully"
                )
            else:
                await self._publish_cancel_event(cancel, OrderEventEnum.REJECTED)
                logger.warning(
                    f"[{exchange_name}] Cancel request for {cancel.exchange_order_id} failed"
                )
        except Exception as e:
            logger.error(
                f"[{exchange_name}] Error cancelling order {cancel.exchange_order_id}: {e}"
            )
            await self._publish_cancel_event(cancel, OrderEventEnum.REJECTED)

    async def _process_replace(self, replace: OrderReplaceData) -> None:
        """Replace/modify an existing order on the exchange.

        Note: Many exchanges don't support atomic replace, so this may
        cancel and re-submit the order.

        Args:
            replace: Replace request data containing new order parameters.
        """
        exchange_name = self._get_exchange_name()
        logger.warning(
            f"[{exchange_name}] Order replace not yet implemented for {replace.exchange_order_id}. "
            f"Consider cancel + new order workflow."
        )
        await self._publish_replace_event(replace, OrderEventEnum.REJECTED)

    async def _publish_cancel_event(self, cancel: OrderCancelData, event: CancelEventType) -> None:
        """Publish cancel event to orders.events.*.*.cancelled or rejected.

        Uses lightweight OrderEventData since cancel commands do not carry
        full order details (side/order_type are not needed).

        Args:
            cancel: Original cancel request.
            event: Event type (must be 'cancelled' or 'rejected').
        """
        if not self.msg_publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            topic = order_event_topic(exchange_name, cancel.instrument, event)
            order_event = OrderEventData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=cancel.exchange_order_id,
                client_order_id=cancel.client_order_id,
                exchange=exchange_name,
                instrument=cancel.instrument,
                event=event,
            )
            await self.msg_publisher.send(topic, order_event)
            logger.info(
                f"[{exchange_name}] Published cancel event: {cancel.exchange_order_id} - {event}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing cancel event: {e}")

    async def _publish_replace_event(
        self, replace: OrderReplaceData, event: ReplaceEventType
    ) -> None:
        """Publish replace event to orders.events.*.*.replaced or rejected.

        Uses lightweight OrderEventData since replace commands do not carry
        full order details (only identifiers and new values).

        Args:
            replace: Original replace request.
            event: Event type (must be 'replaced' or 'rejected').
        """
        if not self.msg_publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            topic = order_event_topic(exchange_name, replace.instrument, event)
            order_event = OrderEventData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=replace.exchange_order_id,
                client_order_id=replace.client_order_id,
                exchange=exchange_name,
                instrument=replace.instrument,
                event=event,
            )
            await self.msg_publisher.send(topic, order_event)
            logger.info(
                f"[{exchange_name}] Published replace event: {replace.exchange_order_id} - {event}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing replace event: {e}")

    async def _execute_live_order(self, order: OrderRequestData) -> str | None:
        """Execute an order on the exchange and return the exchange order ID.

        Args:
            order: Order request data containing order details.

        Returns:
            Exchange order ID if successful, None otherwise.
        """
        exchange_name = self._get_exchange_name()
        try:
            order_request = ExchangeOrderRequest(
                symbol=order.instrument,
                side=OrderSideEnum(order.side),
                type=ExchangeOrderTypeEnum(order.order_type),
                amount=float(order.quantity),
                price=float(order.price) if order.price else None,
                client_order_id=order.client_order_id,
                signaled_at=order.signaled_at,
                leverage=order.leverage,
                reduce_only=order.reduce_only,
                wallet_public_id=self.wallet_public_id,
                operator_public_id=order.operator_public_id,
            )
            assert self.exchange_client is not None, _EXCHANGE_NOT_INIT_MSG
            result = await self.exchange_client.create_order(order_request)
            exchange_order_id = result.id if result else None
            if exchange_order_id:
                pending = self.pending_orders.get(order.client_order_id)
                if pending is not None:
                    pending.exchange_order_id = exchange_order_id
                    pending.db_order_id = result.db_order_id
                    pending.order_public_id = result.db_order_public_id
                logger.info(
                    f"[{exchange_name}] ExchangeOrderSnapshot submitted: {order.client_order_id} -> "
                    f"{exchange_order_id}, waiting for execution via WebSocket"
                )
            return exchange_order_id
        except Exception as e:
            logger.error(f"[{exchange_name}] Live execution error: {e}")
            return None

    async def _publish_execution(self, topic: str, fill: ExecutionData) -> None:
        """Send a complete fill notification to ZMQ.

        Args:
            topic: ZMQ topic for routing.
            fill: Complete ExecutionData with provenance set.
        """
        if not self.msg_publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        try:
            await self.msg_publisher.send(topic, fill)
            logger.info(
                f"[{exchange_name}] Published fill: {fill.client_order_id} - "
                f"{fill.size}@{fill.price}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing fill: {e}")

    async def _record_venue_event(self, params: RecordVenueEventParams) -> None:
        """Write a VenueEvent row to DB if repository supports it.

        Silent no-op if repository is not an SQLAlchemyRepository
        or is not set. Errors are logged but do not propagate.

        Args:
            params: Event data including event_type, exchange_name, instrument,
                and optional fill/error fields.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        event_type = params.get("event_type")
        exchange_name = params.get("exchange_name")
        instrument = params.get("instrument")
        if event_type is None or exchange_name is None or instrument is None:
            logger.error("Venue event params missing required identifiers: {}", params)
            return
        mode = (
            ExecutionModeEnum.PAPER
            if exchange_name == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )
        strategy_tag = params.get("strategy_tag")
        base = f"{exchange_name}.{instrument}.{mode}"
        if self.wallet_public_id:
            wallet_short = compute_wallet_short(self.wallet_public_id)
            base = f"{base}.w{wallet_short}"
        shard_key = (
            f"{base}.{strategy_tag}" if mode == ExecutionModeEnum.PAPER and strategy_tag else base
        )
        now = datetime.now(UTC)
        try:
            await self.repository.insert_venue_event(
                {
                    "event_type": event_type,
                    "shard_key": shard_key,
                    "wallet_public_id": self.wallet_public_id,
                    "exchange": exchange_name,
                    "instrument": instrument,
                    "mode": mode,
                    "received_at": now,
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(f"venue.{shard_key}"),
                    "timestamp": now,
                    "exchange_order_id": params.get("exchange_order_id"),
                    "client_order_id": params.get("client_order_id"),
                    "side": params.get("side"),
                    "status": params.get("status"),
                    "fill_price": params.get("fill_price"),
                    "fill_size": params.get("fill_size"),
                    "cum_fill_size": params.get("cum_fill_size"),
                    "fee": params.get("fee"),
                    "fee_asset": params.get("fee_asset"),
                    "exec_id": params.get("exec_id"),
                    "trade_id": params.get("trade_id"),
                    "error": params.get("error"),
                    "venue_timestamp": params.get("venue_timestamp"),
                    "liquidity_role": params.get("liquidity_role") or "unknown",
                }
            )
        except Exception:
            logger.error(
                f"[{exchange_name}] FAIL-CLOSED: venue event {event_type} write failed "
                f"for {instrument}. Durable mode requires all events persisted."
            )
            raise

    async def _reconciliation_handler(self) -> None:
        """Periodic exchange API reconciliation.

        Compares exchange order state with local pending orders.
        Detects fill gaps and disappeared orders. Logs balance mismatches.
        Uses existing execution flow for corrective fills.
        """
        interval = 60.0
        exchange_name = self._get_exchange_name()
        while self.running:
            await asyncio.sleep(interval)
            try:
                await self._reconcile_with_exchange()
            except httpx.HTTPError as exc:
                logger.warning(
                    "[{}] Reconciliation cycle transient HTTP error — will retry "
                    "on next cycle: {}",
                    exchange_name,
                    exc,
                )
            except Exception:
                logger.exception(f"[{exchange_name}] Reconciliation cycle failed")

    async def _reconcile_with_exchange(self) -> None:
        """Run one reconciliation cycle against the exchange API.

        Queries exchange for current order state and balances,
        compares with local pending orders, and processes corrective
        fills for detected gaps. Uses get_order() to verify the actual
        terminal status of disappeared orders (not just absent from
        open set).
        """
        if self.exchange_client is None:
            return
        exchange_name = self._get_exchange_name()
        exchange_orders = await self.exchange_client.get_orders(status=ExchangeOrderStatusEnum.OPEN)
        exchange_by_id = {o.id: o for o in exchange_orders}

        for _eid, pending in tuple(self.pending_orders.items()):
            exchange_oid = pending.exchange_order_id
            if not exchange_oid:
                continue

            if exchange_oid not in exchange_by_id:
                await self._reconcile_disappeared_order(exchange_name, exchange_oid, pending)
            else:
                exchange_order = exchange_by_id[exchange_oid]
                await self._reconcile_fill_gap(exchange_name, exchange_oid, pending, exchange_order)

        balances = await self.exchange_client.get_balance()
        threshold = self.settings.recon_balance_threshold
        for currency, bal in balances.items():
            if abs(bal.free + bal.used - bal.total) > threshold:
                logger.warning(
                    f"[{exchange_name}] Recon: balance mismatch for {currency}: "
                    f"free={bal.free} used={bal.used} total={bal.total} "
                    f"(threshold={threshold})"
                )

    async def _reconcile_disappeared_order(
        self,
        exchange_name: str,
        exchange_oid: str,
        pending: PendingOrderState,
    ) -> None:
        """Handle an order absent from the open-orders snapshot.

        Calls get_order() to determine actual status. Emits any
        remaining fill gap before the terminal event.
        """
        assert self.exchange_client is not None
        try:
            snapshot = await self.exchange_client.get_order(
                exchange_oid, pending.request.instrument
            )
        except Exception:
            logger.warning(
                f"[{exchange_name}] Recon: get_order failed for {exchange_oid}, "
                f"skipping this cycle"
            )
            return

        if snapshot.status not in (
            ExchangeOrderStatusEnum.CLOSED,
            ExchangeOrderStatusEnum.CANCELED,
            ExchangeOrderStatusEnum.EXPIRED,
        ):
            logger.info(
                f"[{exchange_name}] Recon: order {exchange_oid} not in open list "
                f"but status={snapshot.status}, deferring"
            )
            return

        if snapshot.filled > pending.last_seen_cum_qty:
            await self._reconcile_fill_gap(exchange_name, exchange_oid, pending, snapshot)

        terminal_type: ExecType
        terminal_status: ExchangeOrderStatusEnum
        if snapshot.status == ExchangeOrderStatusEnum.CLOSED:
            terminal_type = "filled"
            terminal_status = ExchangeOrderStatusEnum.CLOSED
        elif snapshot.status == ExchangeOrderStatusEnum.EXPIRED:
            terminal_type = "expired"
            terminal_status = ExchangeOrderStatusEnum.EXPIRED
        else:
            terminal_type = "canceled"
            terminal_status = ExchangeOrderStatusEnum.CANCELED

        logger.warning(
            f"[{exchange_name}] Recon: order {exchange_oid} "
            f"disappeared (status={snapshot.status}), "
            f"emitting terminal={terminal_type}"
        )
        corrective = ExecutionUpdate(
            order_id=exchange_oid,
            exec_type=terminal_type,
            symbol=pending.request.instrument,
            side=OrderSideEnum(pending.request.side),
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=terminal_status,
            timestamp=datetime.now(UTC),
        )
        await self._process_execution(corrective)

    async def _reconcile_fill_gap(
        self,
        exchange_name: str,
        exchange_oid: str,
        pending: PendingOrderState,
        exchange_order: ExchangeOrderSnapshot,
    ) -> None:
        """Emit a corrective fill if exchange shows more fills than local.

        Uses the exchange order's reported price as the approximate fill
        price for the corrective ExecutionUpdate.

        Known limitation — market orders without price:
            If the exchange reports a market order whose snapshot has
            ``price=None``, this method logs an ERROR
            (``"no price on market order, skipping corrective fill"``) and
            returns without emitting a corrective fill.

            Rationale: the executor does not subscribe to ticks and thus
            cannot approximate the fill price locally. Cross-process RPC
            against the publisher adds latency + rate-limit cost and still
            drifts relative to the true VWAP. Forcing an approximate price
            would degrade position-projection accuracy silently.

            Impact: the local position projection lags the exchange by the
            gap quantity. Whether the gap recovers on a later iteration
            depends on the order's lifecycle — if the order remains open
            and a later snapshot populates ``price``, the next loop will
            emit the corrective fill; if the order disappears from the
            open-orders snapshot before that, ``_reconcile_disappeared_order``
            calls this method once more before emitting a terminal event,
            after which the order is no longer tracked and the skipped gap
            persists until manually reconciled against the exchange's own
            fill history.

            Operational guidance: the ERROR log is the observability hook.
            If the same ``(exchange, exchange_oid)`` pair appears across
            multiple iterations without clearing, or if the order has
            already reached a terminal state, reconcile the position
            manually against the exchange's fill history. Do NOT
            approximate inside Snapper — the skip is the correct behaviour.
        """
        if exchange_order.filled <= pending.last_seen_cum_qty:
            return
        gap = exchange_order.filled - pending.last_seen_cum_qty
        fill_price = exchange_order.price
        if fill_price is None:
            logger.error(
                f"[{exchange_name}] Recon: fill gap for {exchange_oid} "
                f"but no price on market order, skipping corrective fill"
            )
            return
        logger.warning(
            f"[{exchange_name}] Recon: fill gap for {exchange_oid}: "
            f"exchange={exchange_order.filled} "
            f"local={pending.last_seen_cum_qty}, "
            f"corrective delta={gap} at price~{fill_price}"
        )
        recon_exec_id = f"recon-{exchange_oid}-{time.monotonic_ns()}"
        corrective = ExecutionUpdate(
            order_id=exchange_oid,
            exec_type="trade",
            symbol=pending.request.instrument,
            side=OrderSideEnum(pending.request.side),
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
            timestamp=datetime.now(UTC),
            cum_qty=exchange_order.filled,
            last_qty=gap,
            last_price=fill_price,
            exec_id=recon_exec_id,
        )
        await self._process_execution(corrective)

    async def _execution_handler(self) -> None:
        """Handle execution updates from the exchange WebSocket."""
        exchange_name = self._get_exchange_name()
        if self.exchange_client is None:
            logger.warning(f"ExchangeExecutorService: {_EXCHANGE_NOT_INIT_MSG}")
            return
        if not self.exchange_client.supports_websocket_executions:
            logger.warning(
                "ExchangeExecutorService: WebSocket executions unsupported; skipping handler"
            )
            return
        try:
            async for message in self.exchange_client.subscribe_executions():
                if not self.running:
                    break
                await self._process_execution(message)
        except NotImplementedError:
            logger.info(
                f"[{exchange_name}] Exchange client does not support execution streaming, skipping"
            )
        except Exception as e:
            if self.running:
                logger.error(f"[{exchange_name}] Error in execution handler: {e}")

    def _cleanup_expired_orphans(self) -> None:
        """Remove orphaned executions that exceeded TTL.

        Called from two sites:

        - The scheduled 60s cleanup task — handles steady-state cases
          where no new orphans arrive to trigger lazy eviction.
        - The inline insert path in :meth:`_resolve_execution_order` —
          drops stale entries BEFORE adding a new one so the dict
          stays bounded by the 5s TTL window rather than the 60s
          cleanup-task period. Without this lazy eviction a sustained
          stream of unmapped executions plus a hung cleanup task would
          let ``orphaned_executions`` grow unbounded.
        """
        now = time.monotonic()
        expired_keys = [
            key
            for key, (_, timestamp) in self.orphaned_executions.items()
            if now - timestamp > self.orphan_ttl_seconds
        ]
        for key in expired_keys:
            self.orphaned_executions.pop(key, None)
            self.orphan_drop_count += 1
        if expired_keys:
            exchange_name = self._get_exchange_name()
            logger.info(
                f"[{exchange_name}] Dropped {len(expired_keys)} orphaned executions after TTL "
                f"(total drops: {self.orphan_drop_count})"
            )

    def _try_process_orphaned(self, exchange_order_id: str, client_order_id: str) -> None:
        """Try to process any orphaned execution for a newly mapped order.

        Called after ACK when client_by_exchange mapping is established.

        Args:
            exchange_order_id: Exchange-assigned order ID.
            client_order_id: Client-assigned order ID.
        """
        orphan = self.orphaned_executions.pop(exchange_order_id, None)
        if orphan is not None:
            execution, _ = orphan
            exchange_name = self._get_exchange_name()
            logger.info(
                f"[{exchange_name}] Processing buffered orphan execution for {exchange_order_id}"
            )
            task = asyncio.create_task(self._process_execution(execution))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    def _resolve_execution_order(
        self, execution: ExecutionUpdate, exchange_name: OrderExchange
    ) -> tuple[str, str, OrderRequestData] | None:
        """Resolve execution to its order using two-level correlation.

        Args:
            execution: Execution update from exchange.
            exchange_name: Exchange name for logging.

        Returns:
            Tuple of (exchange_order_id, client_order_id, original_order)
            or None if correlation fails or execution should be skipped.
        """
        exchange_order_id = execution.order_id
        if exchange_order_id is None:
            logger.warning(
                f"[{exchange_name}] Received execution with None order_id, cannot correlate"
            )
            return None
        client_order_id = self.client_by_exchange.get(exchange_order_id)
        if client_order_id is None:
            self._cleanup_expired_orphans()
            already_buffered = exchange_order_id in self.orphaned_executions
            self.orphaned_executions[exchange_order_id] = (execution, time.monotonic())
            if not already_buffered:
                logger.info(
                    f"[{exchange_name}] Buffered orphan execution for {exchange_order_id} "
                    f"(status={execution.exec_type}, awaiting ACK or TTL expiry)"
                )
            return None
        pending = self.pending_orders.get(client_order_id)
        if pending is None:
            logger.warning(
                f"[{exchange_name}] Received execution for unknown client order: "
                f"{client_order_id} (exchange: {exchange_order_id})"
            )
            return None
        return exchange_order_id, client_order_id, pending.request

    async def _handle_cancellation(
        self,
        execution: ExecutionUpdate,
        exchange_order_id: str,
        client_order_id: str,
        exchange_name: OrderExchange,
    ) -> bool:
        """Handle cancelled or expired execution by cleaning up maps.

        Args:
            execution: Execution update.
            exchange_order_id: Exchange-assigned order ID.
            client_order_id: Client-assigned order ID.
            exchange_name: Exchange name for logging.

        Returns:
            True if the execution was a cancellation and was handled.
        """
        if execution.exec_type not in ("canceled", "expired"):
            return False
        pending = self.pending_orders.pop(client_order_id, None)
        self.client_by_exchange.pop(exchange_order_id, None)
        if pending and pending.db_order_id is not None and self.exchange_client is not None:
            status = (
                ExchangeOrderStatusEnum.CANCELED
                if execution.exec_type == "canceled"
                else ExchangeOrderStatusEnum.EXPIRED
            )
            await self.exchange_client._log_order_update_to_db(
                db_order_id=pending.db_order_id, status=status
            )
        instrument = getattr(execution, "symbol", "") or ""
        tag = None
        if pending and pending.request:
            instrument = pending.request.instrument
            tag = pending.request.strategy_tag
        await self._record_venue_event(
            {
                "event_type": "order_terminal",
                "exchange_name": exchange_name,
                "instrument": instrument,
                "exchange_order_id": exchange_order_id,
                "client_order_id": client_order_id,
                "status": execution.exec_type or "cancelled",
                "strategy_tag": tag,
            }
        )
        logger.info(
            f"[{exchange_name}] Order {client_order_id} {execution.exec_type}, "
            f"cleaned up maps (no execution published)"
        )
        return True

    @staticmethod
    def _resolve_fill_quantities(
        execution: ExecutionUpdate,
        prev_cum: float,
    ) -> tuple[float, float, float, float]:
        """Derive cumulative and delta quantities from an execution update.

        Args:
            execution: Execution update from exchange.
            prev_cum: Previously seen cumulative quantity for this order.

        Returns:
            Tuple of (cum_qty, delta_size, delta_price, avg_price).
        """
        if execution.cum_qty is not None:
            cum_qty = execution.cum_qty
        elif execution.last_qty is not None:
            cum_qty = prev_cum + execution.last_qty
        else:
            cum_qty = 0.0
        if execution.last_qty is not None and execution.last_price is not None:
            delta_size = execution.last_qty
            delta_price = execution.last_price
        else:
            delta_size = cum_qty - prev_cum
            delta_price = execution.average_price or 0.0
        avg_price = execution.average_price or delta_price
        return cum_qty, delta_size, delta_price, avg_price

    @staticmethod
    def _determine_fill_status(
        execution: ExecutionUpdate,
        cum_qty: float,
        expected_qty: float,
    ) -> FillStatus:
        """Determine whether execution represents a full or partial fill.

        Args:
            execution: Execution update from exchange.
            cum_qty: Resolved cumulative filled quantity.
            expected_qty: Absolute expected order quantity.

        Returns:
            Fill status literal.
        """
        if execution.cum_qty is None and execution.last_qty is None:
            return to_fill_status(execution)
        tolerance = max(1e-12, expected_qty * 1e-6)
        qty_complete = cum_qty >= expected_qty - tolerance
        exchange_terminal = execution.cum_qty is not None and execution.order_status in (
            ExchangeOrderStatusEnum.CLOSED,
            ExchangeOrderStatusEnum.CANCELED,
        )
        return (
            FillStatusEnum.FILLED if qty_complete or exchange_terminal else FillStatusEnum.PARTIAL
        )

    @staticmethod
    def _resolve_fee(execution: ExecutionUpdate) -> tuple[float, str]:
        """Resolve scalar fee amount and asset from execution data.

        Priority: fee_usd_equiv (USD, incl. negative rebates) then
        non-zero entries from fees breakdown (native currency).

        Args:
            execution: Execution update with optional fee fields.

        Returns:
            Tuple of (fee_amount, fee_asset). Zero fee returns (0.0, "").
        """
        if execution.fee_usd_equiv is not None and abs(execution.fee_usd_equiv) > 1e-12:
            return execution.fee_usd_equiv, "USD"
        if execution.fees:
            nonzero = [e for e in execution.fees if abs(e.quantity) > 1e-12]
            if not nonzero:
                return 0.0, ""
            if len(nonzero) > 1:
                logger.warning(
                    "Multi-asset fees not supported, using first entry: {}",
                    execution.fees,
                )
            entry = nonzero[0]
            return entry.quantity, entry.asset
        return 0.0, ""

    def _build_execution_data(
        self,
        execution: ExecutionUpdate,
        exchange_order_id: str,
        original_order: OrderRequestData,
        exchange_name: OrderExchange,
    ) -> tuple[str, ExecutionData]:
        """Build an ExecutionData from execution and order data.

        Args:
            execution: Execution update from exchange.
            exchange_order_id: Exchange-assigned order ID.
            original_order: Original order request data.
            exchange_name: Exchange name.

        Returns:
            Tuple of (stream_key, ExecutionData) ready for publishing.
        """
        fee_amount, fee_asset = self._resolve_fee(execution)
        now = datetime.now(UTC)
        topic = order_event_topic(exchange_name, original_order.instrument, OrderEventEnum.EXECUTED)
        client_id = original_order.client_order_id
        pending = self.pending_orders.get(client_id)
        prev_cum = pending.last_seen_cum_qty if pending else 0.0
        cum_qty, delta_size, delta_price, avg_price = self._resolve_fill_quantities(
            execution, prev_cum
        )
        status = self._determine_fill_status(
            execution, cum_qty, abs(float(original_order.quantity))
        )
        if pending:
            pending.last_seen_cum_qty = cum_qty
        return topic, ExecutionData(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            trade_id=execution.exec_id,
            exchange_order_id=exchange_order_id,
            client_order_id=client_id,
            instrument=original_order.instrument,
            exchange=exchange_name,
            side=original_order.side,
            size=cum_qty,
            price=avg_price,
            last_size=delta_size,
            last_price=delta_price,
            fee=fee_amount,
            fee_asset=fee_asset,
            status=status,
            executed_at=execution.timestamp,
            wallet_public_id=original_order.wallet_public_id or self.wallet_public_id,
            operator_public_id=original_order.operator_public_id,
            liquidity_role={"m": "maker", "t": "taker"}.get(
                getattr(execution, "liquidity_ind", None) or "", "unknown"
            ),
        )

    async def _process_execution(self, execution: ExecutionUpdate) -> None:
        """Process an execution update and publish fill notification.

        Uses two-level correlation:
        1. Resolve exchange_order_id -> client_order_id via client_by_exchange
        2. Lookup order by client_order_id in pending_orders

        Cancelled/expired executions clean up maps but do not publish execution data.
        Unknown executions are buffered for TTL in case ACK arrives later (race).

        Args:
            execution: Execution update from the exchange WebSocket.
        """
        exchange_name = self._get_exchange_name()
        try:
            self._cleanup_expired_orphans()
            resolved = self._resolve_execution_order(execution, exchange_name)
            if resolved is None:
                return
            exchange_order_id, client_order_id, original_order = resolved
            if await self._handle_cancellation(
                execution, exchange_order_id, client_order_id, exchange_name
            ):
                return
            topic, fill = self._build_execution_data(
                execution, exchange_order_id, original_order, exchange_name
            )
            raw_tid = getattr(execution, "trade_id", None)
            await self._record_venue_event(
                {
                    "event_type": "fill_observed",
                    "exchange_name": exchange_name,
                    "instrument": fill.instrument,
                    "exchange_order_id": exchange_order_id,
                    "client_order_id": client_order_id,
                    "side": fill.side,
                    "status": fill.status,
                    "fill_price": fill.last_price,
                    "fill_size": fill.last_size,
                    "cum_fill_size": fill.size,
                    "fee": fill.fee,
                    "fee_asset": fill.fee_asset,
                    "exec_id": getattr(execution, "exec_id", None),
                    "trade_id": str(raw_tid) if raw_tid else None,
                    "venue_timestamp": getattr(execution, "timestamp", None),
                    "strategy_tag": original_order.strategy_tag,
                    "liquidity_role": fill.liquidity_role,
                }
            )
            await self._publish_execution(topic, fill)
            pending = self.pending_orders.get(client_order_id)
            if pending and self.exchange_client is not None:
                if pending.order_public_id is not None:
                    await self.exchange_client._log_execution_to_db(
                        order_public_id=pending.order_public_id,
                        execution=execution,
                        wallet_public_id=self.wallet_public_id,
                        operator_public_id=pending.request.operator_public_id,
                        delta_size=fill.last_size,
                        delta_price=fill.last_price,
                        fee=fill.fee,
                        fee_asset=fill.fee_asset,
                        status=fill.status,
                    )
                if pending.db_order_id is not None:
                    db_status = (
                        ExchangeOrderStatusEnum.CLOSED
                        if fill.status == FillStatusEnum.FILLED
                        else ExchangeOrderStatusEnum.OPEN
                    )
                    await self.exchange_client._log_order_update_to_db(
                        db_order_id=pending.db_order_id,
                        status=db_status,
                    )
            if fill.status == FillStatusEnum.FILLED:
                self.pending_orders.pop(client_order_id, None)
                self.client_by_exchange.pop(exchange_order_id, None)
                logger.info(
                    f"[{exchange_name}] Order {client_order_id} filled, removed from pending"
                )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing execution: {e}")

    async def _publish_order_status(
        self,
        order: OrderRequestData,
        status: OrderEventType,
        exchange_order_id: str | None = None,
    ) -> None:
        """Publish order status event to the ZMQ topic.

        The status value is used both as the topic suffix and the payload
        status field, ensuring consistency between routing and content.

        Args:
            order: Order request data containing order details.
            status: Event type for topic suffix and payload status field.
            exchange_order_id: Exchange-assigned order ID (if known).
        """
        if not self.msg_publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            topic = order_event_topic(exchange_name, order.instrument, status)
            order_status = OrderData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=exchange_order_id,
                client_order_id=order.client_order_id,
                instrument=order.instrument,
                exchange=exchange_name,
                side=order.side,
                status=status,
                order_type=order.order_type,
                size=order.quantity,
                filled_size=0.0,
                price=order.price,
                created_at=order.timestamp,
                leverage=order.leverage,
                reduce_only=order.reduce_only,
                wallet_public_id=order.wallet_public_id,
                operator_public_id=order.operator_public_id,
                user_public_id=order.user_public_id,
            )
            await self.msg_publisher.send(topic, order_status)
            logger.info(
                f"[{exchange_name}] Published order event: {order.client_order_id} - {status}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing order event: {e}")

    async def _heartbeat_loop(self) -> None:
        """Periodically publish heartbeat messages and cleanup orphans.

        When ``self.wallet_public_id`` is
        populated, the heartbeat topic gains a 5th ``{wallet_short}``
        segment so per-wallet executor instances publish on distinct
        topics (``system.heartbeats.executor.{exchange}.{wallet_short}``).
        The legacy 4-segment format still fires for template-mode
        executors with empty ``wallet_public_id`` (test fixtures that
        have not migrated to the per-wallet path).
        """
        exchange_name = self._get_exchange_name()
        wallet_short = compute_wallet_short(self.wallet_public_id) if self.wallet_public_id else ""
        component = (
            f"executor.{exchange_name}.{wallet_short}"
            if wallet_short
            else f"executor.{exchange_name}"
        )
        while self.running:
            try:
                await asyncio.sleep(self.settings.zmq_heartbeat_interval_ms / 1000.0)
                if not self.running:
                    break
                self._cleanup_expired_orphans()
                self.heartbeat_seq += 1
                lag_ms = 0
                hb_topic = heartbeat_topic("executor", exchange_name, wallet_short=wallet_short)
                hb_msg = HeartbeatData(
                    public_id=str(uuid7()),
                    timestamp=datetime.now(UTC),
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(hb_topic),
                    component=component,
                    sequence=self.heartbeat_seq,
                    status=HealthStatusEnum.HEALTHY,
                    lag_ms=lag_ms,
                    meta={
                        "running": self.running,
                        "exchange": exchange_name,
                        "wallet_public_id": self.wallet_public_id,
                        "broker_xsub": self.settings.zmq_broker_xsub,
                        "broker_xpub": self.settings.zmq_broker_xpub,
                    },
                )
                await self._publish_heartbeat(hb_topic, hb_msg)
            except Exception as e:
                logger.error(f"[{exchange_name}] Execution service heartbeat error: {e}")

    async def _publish_heartbeat(self, topic: str, message: HeartbeatData) -> None:
        """Send a complete heartbeat message to ZMQ.

        Args:
            topic: Heartbeat ZMQ topic string.
            message: Complete HeartbeatData with provenance set.
        """
        if not self.msg_publisher or not self.running:
            return
        try:
            await self.msg_publisher.send(topic, message)
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")

    def _handle_symbol_alias_update(self, payload: str) -> None:
        """Handle symbol alias cache invalidation message.

        Args:
            payload: JSON payload string from ZMQ message.
        """
        exchange_name = self._get_exchange_name()
        try:
            SymbolAliasUpdateData.from_json(payload)
            logger.info(f"[{exchange_name}] Received symbol alias cache invalidation")
            SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)
            logger.debug(f"[{exchange_name}] Symbol alias cache invalidated successfully")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error handling symbol alias update: {e}")

    def _handle_settings_update(self, payload: str) -> None:
        """Handle settings update message and refresh cached settings.

        Args:
            payload: JSON payload string from ZMQ message.
        """
        exchange_name = self._get_exchange_name()
        try:
            envelope = SettingChangedData.from_json(payload)
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(envelope.value)
                settings_service._cache[envelope.key] = parsed_value
                logger.info(f"[{exchange_name}] Setting {envelope.key} updated via ZMQ event")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error handling settings update: {e}")

    def get_status(self) -> dict[str, Any]:
        """Return the current status of the executor service.

        Returns:
            Dictionary containing running state and connection info.
        """
        return {
            "running": self.running,
            "exchange": self._get_exchange_name(),
            "broker_xsub": self.settings.zmq_broker_xsub,
            "broker_xpub": self.settings.zmq_broker_xpub,
            "heartbeat_seq": self.heartbeat_seq,
        }
