"""Base class for order execution services.

Provides common functionality for ZeroMQ-based execution services
that handle order placement and fill reporting.
"""

import asyncio
import random
import time
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict
from collections.abc import AsyncGenerator
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

from snapper.application.engine.service import compute_shard_key
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
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
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

_AMBIGUOUS_VERIFY_TIMEOUT_S = 15.0
"""Bound on a single venue lookup during ambiguous-submit verification.

The lookup runs while the engine's in-flight guard is held; an
unbounded venue call here would silently extend the UNKNOWN window. A
timed-out attempt counts as could-not-verify, never as absence.
"""

_EXEC_STREAM_BACKOFF_INITIAL_S = 1.0
"""Supervisor backoff after the first execution-stream death."""

_EXEC_STREAM_BACKOFF_CAP_S = 60.0
"""Backoff ceiling between execution-stream reconnect attempts.

Kept at the recon-loop period: even while the stream is down, every
fill is at most one recon cycle behind, so retrying the (cheap)
resubscribe more than once a minute buys nothing during a long outage.
"""

_EXEC_STREAM_JITTER_FRACTION = 0.2
"""Relative jitter applied to each supervisor backoff sleep.

Decorrelates reconnect attempts across executor instances after a
shared outage so the venue does not see synchronized resubscribe
bursts on recovery.
"""

_EXEC_STREAM_HEALTHY_RUNTIME_S = 300.0
"""Stream runtime above which the next death restarts backoff from initial.

Without the reset, a stream that lived for hours would inherit the
60s ceiling from an incident long resolved and sit dark a full minute
on its next blip.
"""

_SEEN_EXEC_IDS_MAX = 10_000
"""Bound of the executor-level seen-exec-id LRU (mirrors the engine's
apply_fill LRU). At one fill per second this covers ~3 hours of
lookback — far beyond any venue replay window — while capping memory.
"""


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
        last_seen_cum_qty: COMMITTED cumulative — what has actually been
            published to the engine; advanced only on publish success.
        last_recorded_cum_qty: DURABLE cumulative — the highest
            cum_fill_size successfully written to venue_events for this
            order; advanced on record success (even when the subsequent
            publish fails). The durable fill_size of each new row is the
            gap from THIS watermark, so additive checkpoint replay always
            sums to venue truth regardless of which prior step failed.
        submit_ambiguous: True when the submit failed ambiguously (the
            venue MAY have the order); the entry is parked pending
            venue verification instead of being rejected.
        unknown_published: True once the single UNKNOWN order event has
            been published for this entry (guards duplicate publishes
            across recon touches).
        accept_event_pending: True when the order was accepted by the
            venue but the durable order_accepted venue event failed to
            persist; the recon loop retries the write until it sticks.
        fill_lock: Serializes fill booking for this order across the live
            stream task and the recon task — dedupe gate, delta build,
            durable write, publish, and committed-cumulative advance form
            one atomic section (see ``_book_correlated_fill``). Lifecycle
            is tied to the entry itself, so the lock vanishes with the
            order and cannot leak.
    """

    request: OrderRequestData
    db_order_id: int | None = field(default=None)
    order_public_id: str | None = field(default=None)
    exchange_order_id: str | None = field(default=None)
    last_seen_cum_qty: float = field(default=0.0)
    last_recorded_cum_qty: float = field(default=0.0)
    submit_ambiguous: bool = field(default=False)
    unknown_published: bool = field(default=False)
    accept_event_pending: bool = field(default=False)
    fill_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


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
        self._unhealed_accept_events: dict[str, RecordVenueEventParams] = {}
        self._seen_exec_ids: OrderedDict[str, None] = OrderedDict()
        self._exec_stream_restarts: int = 0
        self._recon_lock = asyncio.Lock()
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
            last_recorded_cum_qty=filled,
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
                tasks.append(asyncio.create_task(self._supervise_execution_stream()))
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

    async def _is_duplicate_submit(self, order: OrderRequestData) -> bool:
        """Detect a replayed submit command before it can touch the venue.

        The outbox can publish the same client_order_id twice: a
        coordinator crash between the ZMQ publish and the bulk
        CREATED→DISPATCHED commit replays the row on restart, and a
        failed bulk write leaves it re-fetchable on the very next 50ms
        tick. Venue-side cl_ord_id dedupe covers only OPEN orders while
        strategy orders are MARKET, so an unguarded replay double-places
        a real position.

        Checks, cheapest first:

        1. in-memory pending entry — same-process replays, including a
           parked-UNKNOWN entry whose ambiguous/fill-tracking state the
           submit path's unconditional overwrite would destroy;
        2. the unhealed-accept queue — accepted order whose pending
           entry a terminal fill already popped;
        3. durable venue-event evidence (``has_order_submit_evidence``)
           — fresh-process crash-replay where memory is empty but an
           accepted/fill/terminal/unknown row exists. ``order_rejected``
           is excluded there so the outbox's legitimate
           retry-after-definitive-reject path still flows.

        A durable-check failure is FAIL-CLOSED: the command is dropped
        as if duplicate. Proceeding on a true duplicate irreversibly
        doubles a MARKET position; dropping a fresh command self-heals
        via the engine's in-flight timeout valve with a NEW id.

        Dropped duplicates publish NOTHING: the outbox transitions the
        row from its own publish success, never from executor events,
        and a synthetic REJECTED here would fabricate terminal state
        for a possibly-live order.

        Args:
            order: The incoming order request.

        Returns:
            True when the command must be dropped as a duplicate.
        """
        exchange_name = self._get_exchange_name()
        cid = order.client_order_id
        if cid in self.pending_orders:
            logger.warning(
                f"[{exchange_name}] Duplicate submit {cid} dropped: already pending "
                f"in this executor (replayed dispatch)"
            )
            return True
        if cid in self._unhealed_accept_events:
            logger.warning(
                f"[{exchange_name}] Duplicate submit {cid} dropped: accepted order "
                f"awaiting durable-event heal"
            )
            return True
        if isinstance(self.repository, SQLAlchemyRepository):
            try:
                if await self.repository.has_order_submit_evidence(cid):
                    logger.warning(
                        f"[{exchange_name}] Duplicate submit {cid} dropped: durable "
                        f"venue-event evidence exists (crash-replayed dispatch)"
                    )
                    return True
            except Exception as e:
                logger.error(
                    f"[{exchange_name}] Duplicate-evidence check failed for {cid}: {e} "
                    f"— FAIL-CLOSED, dropping the command (a true duplicate would "
                    f"double a MARKET position; a fresh command re-emits via the "
                    f"engine timeout valve)"
                )
                return True
        return False

    async def _reject_if_stale(self, order: OrderRequestData) -> bool:
        """Reject a command older than the dispatch TTL before any venue call.

        The executor's half of the staleness control: the order-flow
        socket buffers an outage backlog unbounded (HWM=0),
        so frames can arrive long after the outbox published them. The
        age anchor is ``signaled_at`` — the command row's creation time
        forwarded by the outbox publish — because the frame
        ``timestamp`` is re-stamped at every publish. Runs AFTER the
        duplicate guard on purpose: a stale REPLAY of an accepted order
        must drop silently as a duplicate, never reject — a synthetic
        REJECTED for a live order is exactly the false-reject
        fabrication the UNKNOWN state exists to prevent. Frames
        without ``signaled_at`` (legacy/direct paths) and a disabled
        TTL skip the gate.

        The rejection is VENUE-TRUTH-BACKED: before rejecting, a single
        bounded client-id lookup runs. A found order is ADOPTED (it is
        a replay of a submit that actually placed — e.g. the
        crash-window replay whose ``order_submit_unknown`` durable
        write also failed, leaving no evidence for the duplicate
        guard); a venue that cannot answer (lookup unsupported,
        unreachable, paper) makes the frame DROP silently — rejecting
        on no evidence could fabricate a terminal state for a live
        order, while dropping leaves the row DISPATCHED for the
        reconciler's stale WARN and the engine's timeout valve. Only
        an AUTHORITATIVE absence rejects.

        REJECTED (not the expired suffix) is deliberate: the trader's
        OrderData handler clears engine intent only on ``rejected``;
        the ``expired`` suffix rides the cancel/replace OrderEventData
        channel. The durable ``order_rejected`` row never blocks a
        later legitimate retry because the duplicate guard's evidence
        set excludes rejections.

        Args:
            order: The incoming order request.

        Returns:
            True when the command was consumed here (rejected, adopted,
            or dropped); False when it should proceed to submit.
        """
        ttl = self._resolve_dispatch_ttl()
        if ttl <= 0 or order.signaled_at is None:
            return False
        age_s = (datetime.now(UTC) - order.signaled_at).total_seconds()
        if age_s <= ttl:
            return False
        exchange_name = self._get_exchange_name()
        if self.exchange_client is None:
            logger.warning(
                f"[{exchange_name}] STALE command {order.client_order_id} dropped "
                f"(age {age_s:.1f}s > TTL {ttl:.1f}s; no venue client to verify absence)"
            )
            return True
        try:
            async with asyncio.timeout(_AMBIGUOUS_VERIFY_TIMEOUT_S):
                snapshot = await self.exchange_client.find_order_by_client_id(
                    order.client_order_id, order.instrument
                )
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] STALE command {order.client_order_id} dropped "
                f"(age {age_s:.1f}s > TTL {ttl:.1f}s; venue absence unverifiable: {e}) "
                f"— rejecting without venue truth could fabricate a terminal state"
            )
            return True
        if snapshot is not None:
            logger.warning(
                f"[{exchange_name}] STALE frame {order.client_order_id} is a replay of "
                f"a PLACED order ({snapshot.id}, status={snapshot.status}) — adopting "
                f"instead of rejecting"
            )
            pending = PendingOrderState(request=order)
            self.pending_orders[order.client_order_id] = pending
            await self._adopt_found_order(order, pending, snapshot)
            return True
        logger.warning(
            f"[{exchange_name}] Rejecting STALE command {order.client_order_id}: "
            f"age {age_s:.1f}s exceeds dispatch TTL {ttl:.1f}s and the venue verified "
            f"absence — an outage-backlog MARKET order must not fire into a moved market"
        )
        await self._publish_order_status(order, OrderEventEnum.REJECTED)
        await self._record_venue_event(
            {
                "event_type": "order_rejected",
                "exchange_name": exchange_name,
                "instrument": order.instrument,
                "client_order_id": order.client_order_id,
                "side": order.side,
                "error": f"stale command: age {age_s:.1f}s exceeds dispatch TTL {ttl:.1f}s",
                "strategy_tag": order.strategy_tag,
            }
        )
        return True

    def _resolve_dispatch_ttl(self) -> float:
        """Return the dispatch TTL from settings, tolerating test doubles.

        Test fixtures replace ``self.settings`` with plain mocks whose
        attributes are not floats; treating anything unparseable as
        disabled keeps the gate strictly opt-in.

        Returns:
            TTL seconds, or 0.0 when unset/unparseable (gate disabled).
        """
        try:
            return float(self.settings.trade_command_dispatch_ttl_s)
        except (AttributeError, TypeError, ValueError):
            return 0.0

    async def _process_order(self, order: OrderRequestData) -> None:
        """Submit an order to the exchange and handle the response.

        Order correlation uses two-level mapping:
        - pending_orders: keyed by client_order_id (always present, unique)
        - client_by_exchange: maps exchange_order_id -> client_order_id (after ACK)

        Publishes order events to orders.events.{exchange}.{instrument}.{event}:
        - submitted: Executor accepted command, sending to exchange
        - accepted: Exchange ACK returned order_id (sync REST response)
        - rejected: Exchange DEFINITIVELY rejected the order or
          validation failed before any send
        - unknown: Submit outcome ambiguous (order may exist on the
          venue) — pending entry parked, engine holds in-flight
        - executed: Order executed (see _execution_handler for WebSocket fills)

        Note: 'accepted' is published when the exchange REST API returns an order_id,
        confirming the order was received and queued. This is a synchronous response.
        Actual fills come asynchronously via WebSocket execution updates.
        Acceptance finalization runs outside the submit try/except in
        ``_finalize_accepted_submit`` so a DB blip on a live order can
        never publish REJECTED.

        Args:
            order: Order request data containing order details.
        """
        exchange_name = self._get_exchange_name()
        if await self._is_duplicate_submit(order):
            return
        if await self._reject_if_stale(order):
            return
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
        except AmbiguousOrderSubmitError as e:
            await self._handle_ambiguous_submit(order, e)
            return
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
            return
        if exchange_order_id:
            await self._finalize_accepted_submit(order, exchange_order_id)
        else:
            logger.warning(f"[{exchange_name}] Order {order.client_order_id} rejected by exchange")
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

    async def _finalize_accepted_submit(
        self,
        order: OrderRequestData,
        exchange_order_id: str,
        *,
        flush_orphans: bool = True,
    ) -> None:
        """Record and publish acceptance of a venue-confirmed live order.

        Runs OUTSIDE the submit try/except on purpose: once the venue
        returned an order id the order IS live, so nothing in
        here may publish REJECTED or pop the pending entry. A failed
        durable ``order_accepted`` write is logged CRITICAL and marked
        on the pending entry (``accept_event_pending``) for the recon
        loop to retry — live correctness (engine learns the order is
        accepted, fills correlate via client_by_exchange) takes priority
        over the durable row, which recon heals.

        Orphaned-execution flushing happens AFTER the durable-write
        attempt: a buffered fill can complete the order and pop its
        pending entry, which would make the ``accept_event_pending``
        flag unsettable if the write failed. Callers that immediately
        project the order from a REST snapshot (the terminal-verified
        ambiguous path) pass ``flush_orphans=False`` to skip the
        BACKGROUND flush here and instead drain the orphan buffer
        inline via ``_flush_orphaned_inline`` before reconciling — full
        WS fill fidelity, no interleaving with the synthetic terminal.

        Args:
            order: The original order request.
            exchange_order_id: Venue-assigned order id from the submit.
            flush_orphans: When False, skip orphaned-execution flushing
                for this order (caller projects fills from REST).
        """
        exchange_name = self._get_exchange_name()
        self.client_by_exchange[exchange_order_id] = order.client_order_id
        accept_event: RecordVenueEventParams = {
            "event_type": "order_accepted",
            "exchange_name": exchange_name,
            "instrument": order.instrument,
            "exchange_order_id": exchange_order_id,
            "client_order_id": order.client_order_id,
            "side": order.side,
            "strategy_tag": order.strategy_tag,
        }
        try:
            await self._record_venue_event(accept_event)
        except Exception:
            logger.critical(
                f"[{exchange_name}] Order {order.client_order_id} accepted as "
                f"{exchange_order_id} but the order_accepted venue event failed to "
                f"persist — order is LIVE; recon will retry the durable write"
            )
            self._unhealed_accept_events[order.client_order_id] = accept_event
            pending = self.pending_orders.get(order.client_order_id)
            if pending is not None:
                pending.accept_event_pending = True
        if flush_orphans:
            self._try_process_orphaned(exchange_order_id, order.client_order_id)
        await self._publish_order_status(order, OrderEventEnum.ACCEPTED, exchange_order_id)
        logger.info(
            f"[{exchange_name}] Order {order.client_order_id} "
            f"accepted as {exchange_order_id}, waiting for execution"
        )

    async def _verify_ambiguous_submit(
        self, order: OrderRequestData, pending: PendingOrderState
    ) -> bool:
        """Resolve an ambiguous submit against venue truth by client id.

        Up to three lookups (after 2s/5s/10s — the venue needs a moment
        to materialize an order whose response was lost), each bounded
        to 15s. Outcomes:

        - FOUND: the order is live or was — finalize as accepted; an
          already-terminal snapshot is additionally routed through
          ``_reconcile_disappeared_order`` so its fills and terminal
          event project normally.
        - TWO consecutive authoritative not-found answers: the venue
          never saw the id — now a SAFE definitive rejection. One
          answer is not enough: closed-order listings can lag the
          submit by seconds.
        - Anything else (lookup unsupported, venue unreachable, mixed
          answers): unresolved — the caller parks the order UNKNOWN.

        Blocking is a known, accepted tradeoff: the sequential order
        handler can spend up to ~62s here (3 sleeps + 3 bounded
        lookups) before parking, delaying queued commands for this
        executor. During the outages that produce ambiguity those
        commands would fail venue-side anyway, and resolving the
        current order's truth first is worth more than dispatching the
        next one into the same outage.

        Args:
            order: The original order request.
            pending: The parked pending entry for the order.

        Returns:
            True when the order was resolved (accepted or rejected);
            False when verification could not produce an answer.
        """
        exchange_name = self._get_exchange_name()
        if self.exchange_client is None:
            return False
        not_found_streak = 0
        for delay_s in (2.0, 5.0, 10.0):
            await asyncio.sleep(delay_s)
            try:
                async with asyncio.timeout(_AMBIGUOUS_VERIFY_TIMEOUT_S):
                    snapshot = await self.exchange_client.find_order_by_client_id(
                        order.client_order_id, order.instrument
                    )
            except NotImplementedError:
                logger.warning(
                    f"[{exchange_name}] venue cannot verify orders by client id — "
                    f"parking {order.client_order_id} as UNKNOWN"
                )
                return False
            except Exception as e:
                logger.warning(
                    f"[{exchange_name}] ambiguous-submit verification attempt failed "
                    f"for {order.client_order_id}: {e}"
                )
                not_found_streak = 0
                continue
            if snapshot is not None:
                logger.warning(
                    f"[{exchange_name}] Order {order.client_order_id} VERIFIED on venue "
                    f"as {snapshot.id} (status={snapshot.status}) after ambiguous submit"
                )
                await self._adopt_found_order(order, pending, snapshot)
                return True
            not_found_streak += 1
            if not_found_streak >= 2:
                logger.warning(
                    f"[{exchange_name}] Order {order.client_order_id} verified ABSENT "
                    f"on venue (2 consecutive authoritative answers) — safe to reject"
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
                        "error": "ambiguous submit; venue verified order absent",
                        "strategy_tag": order.strategy_tag,
                    }
                )
                return True
        return False

    async def _adopt_found_order(
        self,
        order: OrderRequestData,
        pending: PendingOrderState,
        snapshot: ExchangeOrderSnapshot,
    ) -> None:
        """Adopt an order the venue confirmed exists under our client id.

        Shared by the ambiguous-submit verifier and the stale-frame
        gate: records the venue id on the pending entry, finalizes
        acceptance, and — for an already-terminal snapshot — drains any
        buffered orphan fill inline before projecting through the
        disappeared-order reconciler (full WS fidelity, no background
        interleaving).

        Args:
            order: The original order request.
            pending: The pending entry tracking the order.
            snapshot: The venue's order snapshot for the client id.
        """
        exchange_name = self._get_exchange_name()
        pending.submit_ambiguous = False
        pending.exchange_order_id = snapshot.id
        is_live = snapshot.status in (
            ExchangeOrderStatusEnum.PENDING,
            ExchangeOrderStatusEnum.PENDING_NEW,
            ExchangeOrderStatusEnum.NEW,
            ExchangeOrderStatusEnum.OPEN,
            ExchangeOrderStatusEnum.PARTIALLY_FILLED,
        )
        await self._finalize_accepted_submit(order, snapshot.id, flush_orphans=is_live)
        if not is_live:
            await self._flush_orphaned_inline(snapshot.id)
            await self._reconcile_disappeared_order(exchange_name, snapshot.id, pending)

    async def _handle_ambiguous_submit(
        self, order: OrderRequestData, error: AmbiguousOrderSubmitError
    ) -> None:
        """Park an ambiguously-failed submit in the UNKNOWN state.

        The venue call failed in a way where the order MAY exist on the
        venue. Publishing REJECTED here would fabricate
        state: the engine would clear its in-flight intent and could
        re-emit a replacement order while the original is live, doubling
        exposure. Instead the pending entry is kept and marked
        ``submit_ambiguous`` and venue verification runs inline: a
        bounded lookup by client id that resolves the order to accepted
        (found — including already-terminal, routed through the
        disappeared-order reconciler) or rejected (two consecutive
        authoritative not-found answers). Only when verification cannot
        resolve — venue unreachable, lookup unsupported — does the
        entry park: a non-terminal ``order_submit_unknown`` venue event
        is recorded and a single UNKNOWN order event is published — the
        engine holds its guard on it and the operator is alerted.

        The durable venue-event write is best-effort here: if it fails
        (likely the same outage), holding the engine guard via the
        UNKNOWN publish matters more than the durable row. The UNKNOWN
        publish itself is retried with short backoff and
        ``unknown_published`` is set ONLY on a confirmed send — a
        swallowed publish failure would leave the engine's in-flight
        timeout free to clear the guard and re-emit while the original
        order may be live. A total publish failure is CRITICAL; the
        recon loop (``_resolve_ambiguous_pending``) re-attempts the
        publish each cycle until a send confirms.

        Args:
            order: The original order request.
            error: The ambiguous failure raised by the venue client.
        """
        exchange_name = self._get_exchange_name()
        pending = self.pending_orders.get(order.client_order_id)
        if pending is None:
            pending = PendingOrderState(request=order)
            self.pending_orders[order.client_order_id] = pending
        pending.submit_ambiguous = True
        logger.error(
            f"[{exchange_name}] Order {order.client_order_id} submit AMBIGUOUS "
            f"(order may exist on venue): {error} (cause: {error.__cause__!r})"
        )
        if await self._verify_ambiguous_submit(order, pending):
            return
        try:
            await self._record_venue_event(
                {
                    "event_type": "order_submit_unknown",
                    "exchange_name": exchange_name,
                    "instrument": order.instrument,
                    "client_order_id": order.client_order_id,
                    "side": order.side,
                    "error": str(error),
                    "strategy_tag": order.strategy_tag,
                }
            )
        except Exception:
            logger.critical(
                f"[{exchange_name}] order_submit_unknown venue event failed to persist "
                f"for {order.client_order_id} — still publishing UNKNOWN to hold the "
                f"engine guard"
            )
        if not pending.unknown_published:
            for delay_s in (0.0, 0.5, 2.0):
                if delay_s:
                    await asyncio.sleep(delay_s)
                if await self._publish_order_status(order, OrderEventEnum.UNKNOWN):
                    pending.unknown_published = True
                    break
            if not pending.unknown_published:
                logger.critical(
                    f"[{exchange_name}] UNKNOWN order event for {order.client_order_id} "
                    f"could not be published after retries — engine guard may clear on "
                    f"timeout; operator must verify this order on the venue"
                )

    async def _process_cancel(self, cancel: OrderCancelData) -> None:
        """Cancel an existing order on the exchange.

        Cleans up pending_orders and client_by_exchange on success. The
        lifecycle pop runs under the order's ``fill_lock``: an
        unserialized pop could yank the entry out from under an in-flight
        fill booking, stranding that fill (uncorrelatable, orphan-TTL
        dropped, invisible to recon once untracked) — the same interleave
        class the stream-side cancellation path serializes against. A
        trailing fill arriving AFTER a completed cancel pop remains
        recon/P0-3 territory: the order is legitimately gone.

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
                client_id = self.client_by_exchange.get(cancel.exchange_order_id)
                holder = self.pending_orders.get(client_id) if client_id else None
                if holder is not None:
                    async with holder.fill_lock:
                        self.client_by_exchange.pop(cancel.exchange_order_id, None)
                        pending = self.pending_orders.pop(client_id, None) if client_id else None
                        if pending and pending.db_order_id is not None:
                            assert self.exchange_client is not None
                            await self.exchange_client._log_order_update_to_db(
                                db_order_id=pending.db_order_id,
                                status=ExchangeOrderStatusEnum.CANCELED,
                            )
                else:
                    self.client_by_exchange.pop(cancel.exchange_order_id, None)
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
        """Reject an order-replace request (replace is not implemented).

        No exchange client implements atomic replace here; the request
        is logged and answered with a REJECTED replace event so the
        caller can fall back to an explicit cancel + new order flow.

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

        Exceptions PROPAGATE to the caller: the previous
        blanket except-return-None coerced every failure — including
        ambiguous network failures where the order may have executed —
        into the definitive-reject path. ``_process_order`` now
        distinguishes ``AmbiguousOrderSubmitError`` (UNKNOWN/verify
        path) from genuine errors (reject path); a ``None``/empty-id
        return remains the definitive venue-side rejection signal.

        Args:
            order: Order request data containing order details.

        Returns:
            Exchange order ID if accepted, None when the venue
            definitively rejected the submit.

        Raises:
            AmbiguousOrderSubmitError: If the venue call failed in a way
                where the order MAY exist on the venue.
            Exception: Any other submit failure (definitive reject).
        """
        exchange_name = self._get_exchange_name()
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

    async def _publish_execution(self, topic: str, fill: ExecutionData) -> bool:
        """Send a complete fill notification to ZMQ.

        Args:
            topic: ZMQ topic for routing.
            fill: Complete ExecutionData with provenance set.

        Returns:
            True when the fill was handed to the publisher without error;
            False when the publisher is unavailable or the send raised.
            The caller's dedupe registration MUST check this — a swallowed
            send failure that still registered the exec id / advanced the
            seen cumulative would make the venue's redelivery look like a
            replay and drop the fill for good.
        """
        if not self.msg_publisher or not self.running:
            return False
        exchange_name = self._get_exchange_name()
        try:
            await self.msg_publisher.send(topic, fill)
            logger.info(
                f"[{exchange_name}] Published fill: {fill.client_order_id} - "
                f"{fill.size}@{fill.price}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing fill: {e}")
            return False
        return True

    async def _record_venue_event(self, params: RecordVenueEventParams) -> None:
        """Write a VenueEvent row to DB if repository supports it.

        Silent no-op if repository is not an SQLAlchemyRepository
        or is not set. Write failures are FAIL-CLOSED: the error is
        logged and RE-RAISED — durable mode requires every venue event
        persisted before the corresponding ZMQ publish, so callers must
        decide per call site whether a failed write may abort the flow
        (it must NOT for an already-accepted live order).

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
        shard_key = compute_shard_key(
            instrument=instrument,
            exchange=cast(OrderExchange, exchange_name),
            mode=mode,
            wallet_public_id=self.wallet_public_id,
            strategy_tag=strategy_tag,
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
        """Run one reconciliation cycle, serialized on ``_recon_lock``.

        Two callers exist: the periodic 60s handler and the stream
        supervisor's post-reconnect heal. Unserialized, two concurrent
        cycles could both observe the same fill gap and double-emit the
        corrective execution — the lock became necessary exactly when the
        second caller appeared.
        """
        async with self._recon_lock:
            await self._reconcile_with_exchange_unlocked()

    async def _post_reconnect_reconcile(self) -> None:
        """Best-effort recon pass before re-entering a respawned stream.

        Heals the dark-window fill/terminal gap promptly (instead of
        waiting up to a full periodic cycle) and orders "recon heals
        first, then resubscribe with no snapshot" — closing the window
        where a corrective fill and a venue replay could interleave.
        Failures are logged and never block the resubscribe: the periodic
        cycle remains the steady-state backstop.
        """
        try:
            await self._reconcile_with_exchange()
        except Exception:
            logger.exception(
                f"[{self._get_exchange_name()}] Post-reconnect reconciliation "
                f"failed - resubscribing anyway"
            )

    async def _reconcile_with_exchange_unlocked(self) -> None:
        """Run one reconciliation cycle against the exchange API.

        Queries exchange for current order state and balances,
        compares with local pending orders, and processes corrective
        fills for detected gaps. Uses get_order() to verify the actual
        terminal status of disappeared orders (not just absent from
        open set). Parked ambiguous-submit entries (no exchange id yet,
        ``submit_ambiguous`` set) get a fresh venue-verification round
        each cycle, and entries whose durable ``order_accepted`` write
        failed (``accept_event_pending``) get the write retried until
        it sticks — the 60s loop is the steady-state resolver for
        everything the inline paths could not settle.
        """
        if self.exchange_client is None:
            return
        exchange_name = self._get_exchange_name()

        for client_order_id in tuple(self._unhealed_accept_events):
            await self._retry_accept_event(client_order_id)

        exchange_orders = await self.exchange_client.get_orders(status=ExchangeOrderStatusEnum.OPEN)
        exchange_by_id = {o.id: o for o in exchange_orders}

        for _eid, pending in tuple(self.pending_orders.items()):
            exchange_oid = pending.exchange_order_id
            if not exchange_oid:
                if pending.submit_ambiguous:
                    await self._resolve_ambiguous_pending(pending)
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

    async def _resolve_ambiguous_pending(self, pending: PendingOrderState) -> None:
        """Run one recon-cycle verification round for a parked entry.

        Reuses the full bounded verification routine
        (``_verify_ambiguous_submit``) — the two-consecutive-absence
        rule needs multiple lookups anyway, and parked entries are rare
        enough that the extra seconds inside the 60s loop are
        acceptable. When the entry stays unresolved AND its UNKNOWN
        publish never confirmed (the park-time publish retries all
        failed), one publish retry per cycle keeps working toward
        holding the engine guard.

        Args:
            pending: The parked ambiguous pending entry.
        """
        order = pending.request
        exchange_name = self._get_exchange_name()
        if await self._verify_ambiguous_submit(order, pending):
            logger.info(
                f"[{exchange_name}] Recon resolved parked ambiguous order "
                f"{order.client_order_id}"
            )
            return
        if not pending.unknown_published:
            if await self._publish_order_status(order, OrderEventEnum.UNKNOWN):
                pending.unknown_published = True
        logger.warning(
            f"[{exchange_name}] Recon: order {order.client_order_id} still UNKNOWN "
            f"(venue verification pending) — retrying next cycle"
        )

    async def _retry_accept_event(self, client_order_id: str) -> None:
        """Retry the durable order_accepted write for an accepted order.

        Heals the row whose write failed during acceptance
        finalization; the order is live (or by now terminal) and was
        already published ACCEPTED, so only the durable side needs
        repair. The retry state lives in ``_unhealed_accept_events`` on
        the executor — NOT on the pending entry — so a terminal fill or
        cancel popping the entry before the write sticks cannot lose
        the retry. Failure keeps the event queued for the next cycle.

        Args:
            client_order_id: Key into ``_unhealed_accept_events``.
        """
        exchange_name = self._get_exchange_name()
        accept_event = self._unhealed_accept_events.get(client_order_id)
        if accept_event is None:
            return
        try:
            await self._record_venue_event(accept_event)
        except Exception:
            logger.warning(
                f"[{exchange_name}] Recon: order_accepted venue event still failing "
                f"for {client_order_id} — retrying next cycle"
            )
            return
        self._unhealed_accept_events.pop(client_order_id, None)
        pending = self.pending_orders.get(client_order_id)
        if pending is not None:
            pending.accept_event_pending = False
        logger.info(
            f"[{exchange_name}] Recon healed the durable order_accepted event for "
            f"{client_order_id}"
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
            ``price=None``, this method first asks the venue for the
            order's OWN fills via
            :meth:`ExchangeClientBase.get_order_fill_vwap` (venue-true
            quantity-weighted average; implemented for Kraken Futures,
            whose snapshots carry only ``limitPrice``). The VWAP is
            trusted ONLY when the returned covered quantity spans the
            order's whole filled quantity within a RELATIVE 1e-6
            tolerance (venue-side decimal rounding and float summation
            can legitimately leave a fully-covered page a few ulps
            short) — a partial fills page (older fills aged out) must
            never price the entire gap. Only when
            that also yields nothing — no usable fills lookup, partial
            coverage, or a failed lookup — does it log an ERROR
            (``"no price on market order, skipping corrective fill"``)
            and return without emitting a corrective fill.

            The CCXT snapshot builder backfills ``price`` from the
            order's executed ``average`` (the venue's VWAP) when the limit
            price is absent and the order has filled, so this skip now
            only fires when the venue reports neither a price, nor an
            executed average, nor per-order fills.

            Rationale: the executor does not subscribe to ticks and thus
            cannot approximate the fill price locally. Cross-process RPC
            against the publisher adds latency + rate-limit cost and still
            drifts relative to the true VWAP. Forcing an approximate price
            would degrade position-projection accuracy silently — so when
            even the venue's own average is missing, the skip is correct.

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
        if fill_price is None and self.exchange_client is not None:
            try:
                vwap_result = await self.exchange_client.get_order_fill_vwap(exchange_oid)
            except Exception as exc:
                logger.warning(
                    f"[{exchange_name}] Recon: fill-VWAP lookup failed for "
                    f"{exchange_oid}, treating as unresolved: {exc}"
                )
                vwap_result = None
            if vwap_result is not None:
                vwap, covered_qty = vwap_result
                if covered_qty >= exchange_order.filled * (1.0 - 1e-6):
                    fill_price = vwap
                else:
                    logger.warning(
                        f"[{exchange_name}] Recon: fills page for {exchange_oid} "
                        f"covers only {covered_qty} of {exchange_order.filled}, "
                        f"refusing a partial-page VWAP"
                    )
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

    async def _sleep_with_jitter(self, delay_s: float) -> None:
        """Sleep ``delay_s`` scaled by ±``_EXEC_STREAM_JITTER_FRACTION``.

        Args:
            delay_s: Base backoff delay in seconds.
        """
        jitter = 1.0 + _EXEC_STREAM_JITTER_FRACTION * (2.0 * random.random() - 1.0)
        await asyncio.sleep(delay_s * jitter)

    async def _supervise_execution_stream(self) -> None:
        """Respawn the private execution stream until shutdown.

        Replaces the previous spawn-once model in which ANY termination of
        :meth:`_execution_handler` — SDK reconnect-budget exhaustion after
        ~2 min (futures) / ~5 min (spot) of outage, a raised error, even a
        clean generator return — left fills permanently dark while the
        process kept reporting RUNNING, until an operator restarted it.

        Death signal is termination itself: a clean return is treated
        exactly like an exception (the spot generator can still end
        cleanly via its inner receive-error break), mirroring the #144
        publisher supervisor. Backoff doubles from
        ``_EXEC_STREAM_BACKOFF_INITIAL_S`` to ``_EXEC_STREAM_BACKOFF_CAP_S``
        with ±``_EXEC_STREAM_JITTER_FRACTION`` jitter and never abandons —
        each attempt is internally bounded by the venue clients' connect
        and send timeouts, so a wedged attempt cannot stall the loop. A
        stream that survived ``_EXEC_STREAM_HEALTHY_RUNTIME_S`` resets the
        backoff so an old incident's ceiling is not inherited by the next
        blip. Every attempt after a death first runs a best-effort
        :meth:`_post_reconnect_reconcile` so the dark-window gap is healed
        BEFORE the snapshot-free resubscribe. Client rebuild and
        resubscribe happen inside the venue generator on re-entry (locked
        ensure-connected, poisoned-slot rebuild); this loop deliberately
        touches no executor maps —
        ``pending_orders``/``client_by_exchange`` survive respawns, which
        is what keeps fill correlation working across the gap.

        Terminal exits: ``running`` cleared (shutdown), ``CancelledError``
        (propagates to ``start()``'s gather), ``NotImplementedError`` (the
        venue cannot stream executions — permanent, the recon loop is the
        only fill source), or a venue client that does not exist /
        advertises no WebSocket executions support.

        ``_exec_stream_restarts`` counts respawns as an observability seam
        for the future honest-heartbeat work (P2-1).
        """
        exchange_name = self._get_exchange_name()
        backoff = _EXEC_STREAM_BACKOFF_INITIAL_S
        while self.running:
            client = self.exchange_client
            if client is None or not client.supports_websocket_executions:
                logger.warning(
                    f"[{exchange_name}] Execution stream unavailable on this venue - "
                    f"supervisor exiting"
                )
                return
            if self._exec_stream_restarts:
                await self._post_reconnect_reconcile()
            started = time.monotonic()
            try:
                await self._execution_handler()
            except asyncio.CancelledError:
                raise
            except NotImplementedError:
                logger.info(
                    f"[{exchange_name}] Exchange client does not support execution "
                    f"streaming - supervisor exiting"
                )
                return
            except Exception as exc:
                logger.warning(
                    f"[{exchange_name}] Execution stream died ({exc!r}) - "
                    f"respawn #{self._exec_stream_restarts + 1} after {backoff:.0f}s backoff"
                )
            else:
                if not self.running:
                    return
                logger.warning(
                    f"[{exchange_name}] Execution stream returned cleanly - "
                    f"respawn #{self._exec_stream_restarts + 1} after {backoff:.0f}s backoff"
                )
            self._exec_stream_restarts += 1
            if time.monotonic() - started >= _EXEC_STREAM_HEALTHY_RUNTIME_S:
                backoff = _EXEC_STREAM_BACKOFF_INITIAL_S
            await self._sleep_with_jitter(backoff)
            backoff = min(backoff * 2.0, _EXEC_STREAM_BACKOFF_CAP_S)

    async def _execution_handler(self) -> None:
        """Consume one execution-stream pass from the exchange WebSocket.

        Single pass by design: any termination — clean generator end or a
        raised error — returns or propagates to
        :meth:`_supervise_execution_stream`, which owns logging, backoff,
        and respawn (the broad swallow that used to live here hid death
        from any caller). The ``finally`` aclose runs the generator's
        finalizers (venue-side unsubscribe, poisoned-slot
        compare-and-clear) deterministically BEFORE the supervisor's next
        attempt, so a stale generator's deferred cleanup can never
        unsubscribe the fills feed out from under a freshly subscribed
        stream. The contract type is ``AsyncIterator`` (not every test
        double is a generator), hence the runtime ``AsyncGenerator``
        check instead of ``contextlib.aclosing``.
        """
        if self.exchange_client is None:
            logger.warning(f"ExchangeExecutorService: {_EXCHANGE_NOT_INIT_MSG}")
            return
        if not self.exchange_client.supports_websocket_executions:
            logger.warning(
                "ExchangeExecutorService: WebSocket executions unsupported; skipping handler"
            )
            return
        stream = self.exchange_client.subscribe_executions()
        try:
            async for message in stream:
                if not self.running:
                    break
                await self._process_execution(message)
        finally:
            if isinstance(stream, AsyncGenerator):
                await stream.aclose()

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

    async def _flush_orphaned_inline(self, exchange_order_id: str) -> None:
        """Process a buffered orphan execution synchronously, in caller order.

        The terminal-verified ambiguous path awaits the
        buffered WS fill BEFORE REST reconciliation: the fill keeps its
        full fidelity (exec id, fees, exact prices) and advances
        ``last_seen_cum_qty``, so the reconciler then emits only the
        remaining gap — no interleaving with the synthetic terminal is
        possible, unlike the background-task flush used on the normal
        acceptance path.

        Args:
            exchange_order_id: Exchange-assigned order ID to flush.
        """
        orphan = self.orphaned_executions.pop(exchange_order_id, None)
        if orphan is not None:
            execution, _ = orphan
            exchange_name = self._get_exchange_name()
            logger.info(
                f"[{exchange_name}] Processing buffered orphan execution inline "
                f"for {exchange_order_id} before REST reconciliation"
            )
            await self._process_execution(execution)

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
        *,
        tracked: bool = False,
    ) -> tuple[float, float, float, float]:
        """Derive cumulative and delta quantities from an execution update.

        For a TRACKED order whose frame carries both the absolute
        cumulative and a venue delta, the published delta is anchored to
        the COMMITTED cumulative (``cum_qty - prev_cum``) rather than the
        venue's ``last_qty``: the engine applies deltas, so every
        published delta must close the distance from what was actually
        published before. When a prior fill's publish failed, the next
        cum-carrying frame thereby absorbs the unpublished gap instead of
        leaving the engine permanently behind venue truth (a divergence
        the recon loop cannot see, because the executor-side cumulative
        matches the venue). The two values coincide in normal operation;
        a divergence is logged. Untracked orders keep the venue delta —
        with no committed cumulative to anchor to, cum-diff deltas would
        repeat-count across frames.

        Args:
            execution: Execution update from exchange.
            prev_cum: Previously seen cumulative quantity for this order.
            tracked: True when the order has a pending entry whose
                committed cumulative advances on successful publish.

        Returns:
            Tuple of (cum_qty, delta_size, delta_price, avg_price).
        """
        if execution.cum_qty is not None:
            cum_qty = execution.cum_qty
        elif execution.last_qty is not None:
            cum_qty = prev_cum + execution.last_qty
        else:
            cum_qty = 0.0
        if (
            tracked
            and execution.cum_qty is not None
            and execution.last_qty is not None
            and execution.last_price is not None
        ):
            delta_size = cum_qty - prev_cum
            delta_price = execution.last_price
            if abs(delta_size - execution.last_qty) > max(1e-12, abs(execution.last_qty) * 1e-6):
                logger.warning(
                    f"Cum-anchored delta {delta_size} diverges from venue last_qty "
                    f"{execution.last_qty} (cum {cum_qty}, committed {prev_cum}) - "
                    f"absorbing unpublished gap"
                )
        elif execution.last_qty is not None and execution.last_price is not None:
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

        ``last_seen_cum_qty`` is deliberately NOT advanced here: it is
        COMMITTED state, advanced by :meth:`_book_correlated_fill` only
        after the publish succeeded, under the order's ``fill_lock``. A
        builder-side advance would leak tentative state to concurrent
        readers and, after a failed publish, make the venue's redelivery
        look non-advancing and drop the fill for good.

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
            execution, prev_cum, tracked=pending is not None
        )
        status = self._determine_fill_status(
            execution, cum_qty, abs(float(original_order.quantity))
        )
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

    def _is_duplicate_fill(self, execution: ExecutionUpdate, client_order_id: str) -> bool:
        """Decide whether a fill frame was already booked and must be dropped.

        Two complementary keys, checked BEFORE :meth:`_build_execution_data`
        mutates ``last_seen_cum_qty``:

        - ``exec_id`` LRU: catches venue redelivery of the same execution
          (at-least-once delivery, replays across resubscribes). Futures
          fills carry ``fill_id`` as exec id, spot carries ``exec_id``;
          recon correctives use unique synthetic ids and never collide.
        - Cumulative-monotonic guard: a fill carrying absolute ``cum_qty``
          that does not ADVANCE the order's seen cumulative is a replay or
          a stale corrective. This closes the cross-key hole exec ids
          cannot: a recon corrective healed the gap under a synthetic id,
          then the real fill (a NEVER-seen exec id) arrives — and equally
          the reverse race, a stale corrective (recon always stamps
          ``cum_qty = exchange.filled``) landing after a live fill already
          advanced the cumulative. Futures live fills carry no ``cum_qty``
          so this guard never misfires there; their replay exposure is
          closed by snapshot suppression at the source plus the exec-id LRU.

        Status-only frames (``last_qty`` is None) always pass — they carry
        no quantity to double-book.

        Args:
            execution: Execution update from the exchange WebSocket.
            client_order_id: Correlated client order id.

        Returns:
            True when the frame must be dropped without publishing.
        """
        if execution.last_qty is None:
            return False
        exec_id = getattr(execution, "exec_id", None)
        if exec_id and exec_id in self._seen_exec_ids:
            logger.debug(f"Skipping duplicate fill {exec_id} for {client_order_id}")
            return True
        pending = self.pending_orders.get(client_order_id)
        if (
            pending is not None
            and execution.cum_qty is not None
            and execution.cum_qty <= pending.last_seen_cum_qty + 1e-12
        ):
            logger.debug(
                f"Skipping non-advancing fill for {client_order_id}: "
                f"cum {execution.cum_qty} <= seen {pending.last_seen_cum_qty}"
            )
            return True
        return False

    def _register_seen_exec_id(self, exec_id: str | None) -> None:
        """Record a PUBLISHED fill's exec id in the bounded LRU.

        Registration happens only after :meth:`_publish_execution`
        succeeded: a fill whose durable venue-event write or publish
        failed must NOT be marked seen, or its redelivery would be dropped
        and the fill lost for good.

        Args:
            exec_id: Venue execution id; falsy values are ignored.
        """
        if not exec_id:
            return
        self._seen_exec_ids[exec_id] = None
        self._seen_exec_ids.move_to_end(exec_id)
        while len(self._seen_exec_ids) > _SEEN_EXEC_IDS_MAX:
            self._seen_exec_ids.popitem(last=False)

    async def _process_execution(self, execution: ExecutionUpdate) -> None:
        """Process an execution update and publish fill notification.

        Uses two-level correlation:
        1. Resolve exchange_order_id -> client_order_id via client_by_exchange
        2. Lookup order by client_order_id in pending_orders

        Cancelled/expired executions clean up maps but do not publish execution data.
        Unknown executions are buffered for TTL in case ACK arrives later (race).

        Fill booking AND cancellation handling for a tracked order run
        under the order's ``fill_lock``: the live stream task, the recon
        task, and the orphan-flush task can all deliver executions for
        the same order. An unserialized interleave lets a later fill read
        tentative (not yet published) cumulative state, and an
        unserialized cancellation can pop the pending entry out from
        under an in-flight booking — the gate, the delta computation, the
        durable write, the publish, the committed-cumulative advance, and
        any lifecycle pop must be one atomic section per order.

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
            holder = self.pending_orders.get(client_order_id)
            if holder is None:
                return
            async with holder.fill_lock:
                if await self._handle_cancellation(
                    execution, exchange_order_id, client_order_id, exchange_name
                ):
                    return
                await self._book_correlated_fill(
                    execution, exchange_order_id, client_order_id, original_order, exchange_name
                )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing execution: {e}")

    async def _book_correlated_fill(
        self,
        execution: ExecutionUpdate,
        exchange_order_id: str,
        client_order_id: str,
        original_order: OrderRequestData,
        exchange_name: OrderExchange,
    ) -> None:
        """Book one correlated execution: dedupe, persist, publish, commit.

        Runs under the order's ``fill_lock`` when the order is tracked.
        ``last_seen_cum_qty`` is COMMITTED state — advanced only after the
        publish succeeded, so it always equals the cumulative the engine
        has actually been told about. A fill whose publish failed commits
        nothing: its redelivery passes the gate, and if a LATER fill for
        the same order arrives first, that fill's cum-anchored delta (see
        :meth:`_resolve_fill_quantities`) absorbs the unpublished gap —
        either way the published deltas sum to the committed cumulative.

        The durable venue-event row's ``fill_size`` anchoring depends on
        what the frame carries. Replay (``TradeService.apply_venue_event``)
        dedupes rows by exec id and sums ``fill_size`` additively across
        DISTINCT fills, so:

        - CUM-CARRYING frames anchor to the DURABLE watermark
          (``last_recorded_cum_qty``): the gap since the last persisted
          row. The venue's raw ``last_qty`` would underbook when the
          predecessor's durable write failed (its quantity exists in no
          row), and the publish-anchored absorbed delta would overbook
          when the predecessor's row DID persist and only its publish
          failed. The watermark advances right after the record succeeds
          — before the publish — because it tracks DB truth, not engine
          truth.
        - DELTA-ONLY frames (futures: no ``cum_qty``) write the venue's
          own ``last_qty``: their ``fill.size`` is fabricated from the
          COMMITTED cumulative, which lags venue truth after a publish
          failure, and a watermark gap computed from a fabricated
          cumulative would write a zero row for a real, distinct fill —
          underbooking replay. Redelivered duplicates of the SAME fill
          re-write rows under the same exec id, which replay dedupes.

        Args:
            execution: Execution update from the exchange WebSocket.
            exchange_order_id: Venue-assigned order id.
            client_order_id: Correlated client order id.
            original_order: Original order request data.
            exchange_name: Exchange identifier.
        """
        if self._is_duplicate_fill(execution, client_order_id):
            return
        topic, fill = self._build_execution_data(
            execution, exchange_order_id, original_order, exchange_name
        )
        raw_tid = getattr(execution, "trade_id", None)
        durable_holder = self.pending_orders.get(client_order_id)
        is_fill_frame = execution.last_qty is not None or execution.cum_qty is not None
        if durable_holder is not None and execution.cum_qty is not None:
            durable_size = max(0.0, fill.size - durable_holder.last_recorded_cum_qty)
        elif execution.last_qty is not None:
            durable_size = execution.last_qty
        else:
            durable_size = fill.last_size
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
                "fill_size": durable_size,
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
        if durable_holder is not None and is_fill_frame:
            durable_holder.last_recorded_cum_qty = max(
                durable_holder.last_recorded_cum_qty, fill.size
            )
        if not await self._publish_execution(topic, fill):
            return
        committed = self.pending_orders.get(client_order_id)
        if committed is not None and (
            execution.last_qty is not None or execution.cum_qty is not None
        ):
            committed.last_seen_cum_qty = max(committed.last_seen_cum_qty, fill.size)
        if execution.last_qty is not None:
            self._register_seen_exec_id(getattr(execution, "exec_id", None))
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
            logger.info(f"[{exchange_name}] Order {client_order_id} filled, removed from pending")

    async def _publish_order_status(
        self,
        order: OrderRequestData,
        status: OrderEventType,
        exchange_order_id: str | None = None,
    ) -> bool:
        """Publish order status event to the ZMQ topic.

        The status value is used both as the topic suffix and the payload
        status field, ensuring consistency between routing and content.

        Args:
            order: Order request data containing order details.
            status: Event type for topic suffix and payload status field.
            exchange_order_id: Exchange-assigned order ID (if known).

        Returns:
            True when the event was handed to the publisher without
            error; False when the publisher is unavailable or the send
            raised. Callers of safety-critical statuses (UNKNOWN) must
            check this — a swallowed publish failure would
            leave the engine unaware that its in-flight guard must hold.
        """
        if not self.msg_publisher or not self.running:
            return False
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
            return True
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing order event: {e}")
            return False

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
