"""Base class for order execution services.

Provides common functionality for ZeroMQ-based execution services
that handle order placement and fill reporting.
"""

import asyncio
from abc import ABC
from abc import abstractmethod
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import SettingsService
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.types import OrderEventType
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.symbols.functions import TradingExchange
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.interface.websocket.schemas import FillStatus
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.messages import FillEnvelope
from snapper.messaging.schemas.messages import HeartbeatEnvelope
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import OrderCancelEnvelope
from snapper.messaging.schemas.messages import OrderEventEnvelope
from snapper.messaging.schemas.messages import OrderReplaceEnvelope
from snapper.messaging.schemas.messages import OrderRequestEnvelope
from snapper.messaging.schemas.messages import OrderStatusEnvelope
from snapper.messaging.schemas.messages import SettingChangedEnvelope
from snapper.messaging.schemas.messages import SymbolMappingUpdateEnvelope
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import parse_order_command_topic
from snapper.utils.logging import set_log_context


class ExchangeExecutorService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for executing orders on exchanges via ZMQ messaging."""

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default kwargs for the executor service.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary of default keyword arguments for this executor.
        """
        return {}

    def __init__(self) -> None:
        """Initialize the instance."""
        self.settings = get_settings()
        self.context: zmq.asyncio.Context | None = None
        self.subscriber: ValidatedSubscriber | None = None
        self.publisher: ValidatedPublisher | None = None
        self.running = False
        self.heartbeat_seq = 0
        self.exchange_client: T | None = None
        self.repository: Any = None
        self.pending_orders: dict[str, OrderRequestEnvelope] = {}

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Exchange client instance for this executor.
        """
        ...

    @abstractmethod
    def _get_exchange_name(self) -> TradingExchange:
        """Return the trading exchange identifier.

        Returns:
            Trading exchange enum value for this executor.
        """
        ...

    async def start(self) -> None:
        """Start the execution service and subscribe to order topics."""
        exchange_name = self._get_exchange_name()
        set_log_context(f"exec:{exchange_name}")
        if self.running:
            logger.warning(f"{exchange_name} execution service already running")
            return
        repository = get_repository(self.settings.db_url)
        self.repository = repository
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xpub,
            self.settings.master_password,
            self.settings.encryption_salt,
        )
        self.settings = get_settings_with_service(settings_service)
        logger.info("AppSettings service initialized with database access")
        self.exchange_client = self._create_exchange_client()
        self.context = zmq.asyncio.Context()
        raw_sub_socket = self.context.socket(zmq.SUB)
        raw_sub_socket.connect(self.settings.zmq_broker_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        commands_prefix = f"orders.commands.{exchange_name}."
        self.subscriber.subscribe(commands_prefix)
        self.subscriber.subscribe("system.symbol_mappings")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: Subscribed to {commands_prefix}, "
            f"system.symbol_mappings, system.settings from {self.settings.zmq_broker_xpub}"
        )
        raw_pub_socket = self.context.socket(zmq.PUB)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: "
            f"Publishing to broker {self.settings.zmq_broker_xsub}"
        )
        supports_ws = self.exchange_client.supports_websocket_executions
        async with self.exchange_client:
            logger.info(
                f"ExchangeExecutorService[{exchange_name}]: "
                f"Exchange client initialized with WebSocket"
            )
            self.running = True
            tasks = [
                asyncio.create_task(self._order_handler()),
                asyncio.create_task(self._heartbeat_loop()),
            ]
            if supports_ws:
                tasks.append(asyncio.create_task(self._execution_handler()))
            try:
                await asyncio.gather(*tasks)
            except asyncio.CancelledError:
                logger.info(f"ExchangeExecutorService[{exchange_name}] tasks cancelled")

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

    async def _order_handler(self) -> None:
        """Process incoming order commands from ZMQ subscription.

        Dispatches to appropriate handler based on command suffix:
        - .submit -> _process_order (OrderRequestEnvelope)
        - .cancel -> _process_cancel (OrderCancelEnvelope)
        - .replace -> _process_replace (OrderReplaceEnvelope)

        Topic invariant enforced: topic parts must match payload fields.
        """
        exchange_name = self._get_exchange_name()
        commands_prefix = f"orders.commands.{exchange_name}."
        while self.running:
            try:
                if self.subscriber:
                    topic_str, payload_bytes = await self.subscriber.recv_multipart()
                    payload_str = payload_bytes.decode("utf-8")
                    if topic_str.startswith(commands_prefix):
                        parsed = parse_order_command_topic(topic_str)
                        if parsed is None:
                            logger.warning(f"Malformed command topic: {topic_str}")
                            continue
                        if parsed.suffix == "submit":
                            await self._handle_submit_command(
                                payload_str, exchange_name, parsed.instrument
                            )
                        elif parsed.suffix == "cancel":
                            await self._handle_cancel_command(
                                payload_str, exchange_name, parsed.instrument
                            )
                        elif parsed.suffix == "replace":
                            await self._handle_replace_command(
                                payload_str, exchange_name, parsed.instrument
                            )
                        else:
                            logger.debug(f"Ignoring unknown command: {topic_str}")
                    elif topic_str == "system.symbol_mappings":
                        await self._handle_symbol_mapping_update(payload_str)
                    elif topic_str == "system.settings":
                        await self._handle_settings_update(payload_str)
                    else:
                        logger.warning(f"Received message on unexpected topic: {topic_str}")
                else:
                    await asyncio.sleep(0.1)
            except Exception as e:
                if self.running:
                    logger.error(f"Error handling order: {e}")

    async def _handle_submit_command(
        self, payload_str: str, exchange_name: str, topic_instrument: str
    ) -> None:
        """Handle submit command from orders.commands.*.*.submit topic.

        Validates topic/payload invariants:
        - payload.exchange == topic exchange (via subscription prefix)
        - payload.instrument == topic instrument

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
        if isinstance(order_msg, OrderRequestEnvelope):
            if order_msg.exchange != exchange_name:
                logger.warning(
                    f"Invariant violation: payload exchange '{order_msg.exchange}' "
                    f"!= topic exchange '{exchange_name}'"
                )
                return
            if order_msg.instrument != topic_instrument:
                logger.warning(
                    f"Invariant violation: payload instrument '{order_msg.instrument}' "
                    f"!= topic instrument '{topic_instrument}'"
                )
                return
            await self._process_order(order_msg)
        else:
            logger.warning(f"Received non-order message on submit topic: {order_msg.type}")

    async def _handle_cancel_command(
        self, payload_str: str, exchange_name: str, topic_instrument: str
    ) -> None:
        """Handle cancel command from orders.commands.*.*.cancel topic.

        Validates topic/payload invariants:
        - payload.exchange == topic exchange (via subscription prefix)
        - payload.instrument == topic instrument

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
        if isinstance(cancel_msg, OrderCancelEnvelope):
            if cancel_msg.exchange != exchange_name:
                logger.warning(
                    f"Invariant violation: payload exchange '{cancel_msg.exchange}' "
                    f"!= topic exchange '{exchange_name}'"
                )
                return
            if cancel_msg.instrument != topic_instrument:
                logger.warning(
                    f"Invariant violation: payload instrument '{cancel_msg.instrument}' "
                    f"!= topic instrument '{topic_instrument}'"
                )
                return
            await self._process_cancel(cancel_msg)
        else:
            logger.warning(f"Received non-cancel message on cancel topic: {cancel_msg.type}")

    async def _handle_replace_command(
        self, payload_str: str, exchange_name: str, topic_instrument: str
    ) -> None:
        """Handle replace command from orders.commands.*.*.replace topic.

        Validates topic/payload invariants:
        - payload.exchange == topic exchange (via subscription prefix)
        - payload.instrument == topic instrument

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
        if isinstance(replace_msg, OrderReplaceEnvelope):
            if replace_msg.exchange != exchange_name:
                logger.warning(
                    f"Invariant violation: payload exchange '{replace_msg.exchange}' "
                    f"!= topic exchange '{exchange_name}'"
                )
                return
            if replace_msg.instrument != topic_instrument:
                logger.warning(
                    f"Invariant violation: payload instrument '{replace_msg.instrument}' "
                    f"!= topic instrument '{topic_instrument}'"
                )
                return
            await self._process_replace(replace_msg)
        else:
            logger.warning(f"Received non-replace message on replace topic: {replace_msg.type}")

    async def _process_order(self, order: OrderRequestEnvelope) -> None:
        """Submit an order to the exchange and handle the response.

        Publishes order events to orders.events.{exchange}.{instrument}.{event}:
        - submitted: Executor accepted command, sending to exchange
        - accepted: Exchange ACK returned order_id (sync REST response)
        - rejected: Exchange rejected order or validation failed
        - fill: Order execution (see _execution_handler for WebSocket fills)

        Note: 'accepted' is published when the exchange REST API returns an order_id,
        confirming the order was received and queued. This is a synchronous response.
        Actual fills come asynchronously via WebSocket execution updates.

        Args:
            order: Order request envelope containing order details.
        """
        exchange_name = self._get_exchange_name()
        try:
            await self._publish_order_status(order, "submitted")
            exchange_order_id = await self._execute_live_order(order)
            if exchange_order_id:
                await self._publish_order_status(order, "accepted", exchange_order_id)
                logger.info(
                    f"[{exchange_name}] Order {order.client_order_id} "
                    f"accepted as {exchange_order_id}, waiting for execution"
                )
            else:
                logger.warning(
                    f"[{exchange_name}] Order {order.client_order_id} rejected by exchange"
                )
                await self._publish_order_status(order, "rejected")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing order {order.client_order_id}: {e}")
            await self._publish_order_status(order, "rejected")

    async def _process_cancel(self, cancel: OrderCancelEnvelope) -> None:
        """Cancel an existing order on the exchange.

        Args:
            cancel: Cancel request envelope containing order ID to cancel.
        """
        exchange_name = self._get_exchange_name()
        try:
            assert self.exchange_client is not None, "Exchange client not initialized"
            result = await self.exchange_client.cancel_order(
                cancel.exchange_order_id, cancel.instrument
            )
            if result and result.status == OrderStatusEnum.CANCELED:
                await self._publish_cancel_event(cancel, "cancelled")
                logger.info(
                    f"[{exchange_name}] Order {cancel.exchange_order_id} cancelled successfully"
                )
            else:
                await self._publish_cancel_event(cancel, "rejected")
                logger.warning(
                    f"[{exchange_name}] Cancel request for {cancel.exchange_order_id} failed"
                )
        except Exception as e:
            logger.error(
                f"[{exchange_name}] Error cancelling order {cancel.exchange_order_id}: {e}"
            )
            await self._publish_cancel_event(cancel, "rejected")

    async def _process_replace(self, replace: OrderReplaceEnvelope) -> None:
        """Replace/modify an existing order on the exchange.

        Note: Many exchanges don't support atomic replace, so this may
        cancel and re-submit the order.

        Args:
            replace: Replace request envelope containing new order parameters.
        """
        exchange_name = self._get_exchange_name()
        logger.warning(
            f"[{exchange_name}] Order replace not yet implemented for {replace.exchange_order_id}. "
            f"Consider cancel + new order workflow."
        )
        await self._publish_replace_event(replace, "rejected")

    async def _publish_cancel_event(
        self, cancel: OrderCancelEnvelope, event: OrderEventType
    ) -> None:
        """Publish cancel event to orders.events.*.*.cancelled or rejected.

        Uses lightweight OrderEventEnvelope since cancel commands don't carry
        full order details (side/order_type are not needed).

        Args:
            cancel: Original cancel request.
            event: Event type (must be 'cancelled' or 'rejected').
        """
        if not self.publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        try:
            topic = f"orders.events.{exchange_name}.{cancel.instrument}.{event}"
            order_event = OrderEventEnvelope(
                exchange_order_id=cancel.exchange_order_id,
                client_order_id=cancel.client_order_id,
                exchange=exchange_name,
                instrument=cancel.instrument,
                event=event,
            )
            await self.publisher.send_multipart(topic, order_event.to_json().encode("utf-8"))
            logger.info(
                f"[{exchange_name}] Published cancel event: {cancel.exchange_order_id} - {event}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing cancel event: {e}")

    async def _publish_replace_event(
        self, replace: OrderReplaceEnvelope, event: OrderEventType
    ) -> None:
        """Publish replace event to orders.events.*.*.replaced or rejected.

        Uses lightweight OrderEventEnvelope since replace commands don't carry
        full order details (only identifiers and new values).

        Args:
            replace: Original replace request.
            event: Event type (must be 'replaced' or 'rejected').
        """
        if not self.publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        try:
            topic = f"orders.events.{exchange_name}.{replace.instrument}.{event}"
            order_event = OrderEventEnvelope(
                exchange_order_id=replace.exchange_order_id,
                client_order_id=replace.client_order_id,
                exchange=exchange_name,
                instrument=replace.instrument,
                event=event,
            )
            await self.publisher.send_multipart(topic, order_event.to_json().encode("utf-8"))
            logger.info(
                f"[{exchange_name}] Published replace event: {replace.exchange_order_id} - {event}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing replace event: {e}")

    async def _execute_live_order(self, order: OrderRequestEnvelope) -> str | None:
        """Execute an order on the exchange and return the exchange order ID.

        Args:
            order: Order request envelope containing order details.

        Returns:
            Exchange order ID if successful, None otherwise.
        """
        exchange_name = self._get_exchange_name()
        try:
            order_request = ExchangeOrderRequest(
                symbol=order.instrument,
                side=OrderSideEnum(order.side),
                type=OrderTypeEnum(order.order_type),
                amount=float(order.quantity),
                price=float(order.price) if order.price else None,
                client_order_id=order.client_order_id,
                signaled_at=order.signaled_at,
            )
            assert self.exchange_client is not None, "Exchange client not initialized"
            result = await self.exchange_client.create_order(order_request)
            exchange_order_id = result.id if result else None
            if exchange_order_id:
                self.pending_orders[exchange_order_id] = order
                logger.info(
                    f"[{exchange_name}] ExchangeOrderSnapshot submitted: {order.client_order_id} -> "
                    f"{exchange_order_id}, waiting for execution via WebSocket"
                )
            return exchange_order_id
        except Exception as e:
            logger.error(f"[{exchange_name}] Live execution error: {e}")
            return None

    async def _publish_fill(self, fill: FillEnvelope) -> None:
        """Publish a fill notification to the ZMQ topic.

        Args:
            fill: Fill envelope containing execution details.
        """
        if not self.publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        try:
            topic = f"orders.events.{exchange_name}.{fill.instrument}.fill"
            await self.publisher.send_multipart(topic, fill.to_json().encode("utf-8"))
            logger.info(
                f"[{exchange_name}] Published fill: {fill.client_order_id} - "
                f"{fill.size}@{fill.price}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing fill: {e}")

    async def _execution_handler(self) -> None:
        """Handle execution updates from the exchange WebSocket."""
        exchange_name = self._get_exchange_name()
        if self.exchange_client is None:
            logger.warning("ExchangeExecutorService: Exchange client not initialized")
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

    async def _process_execution(self, execution: ExecutionUpdate) -> None:
        """Process an execution update and publish fill notification.

        Args:
            execution: Execution update from the exchange WebSocket.
        """
        exchange_name = self._get_exchange_name()
        try:
            exchange_order_id = execution.order_id
            if exchange_order_id not in self.pending_orders:
                logger.warning(
                    f"[{exchange_name}] Received execution for unknown order: {exchange_order_id}"
                )
                return
            original_order = self.pending_orders[exchange_order_id]
            status: FillStatus
            if execution.exec_type == "filled" or execution.order_status == OrderStatusEnum.CLOSED:
                status = "filled"
            elif (
                execution.exec_type == "canceled"
                or execution.order_status == OrderStatusEnum.CANCELED
            ):
                status = "cancelled"
            elif execution.order_status == OrderStatusEnum.OPEN and (execution.cum_qty or 0) > 0:
                status = "partial"
            else:
                status = "filled"
            total_fee = execution.fee_usd_equiv or 0.0
            fee_asset = "USD" if total_fee > 0 else ""
            fill = FillEnvelope(
                trade_id=execution.exec_id,
                exchange_order_id=exchange_order_id,
                client_order_id=original_order.client_order_id,
                instrument=original_order.instrument,
                exchange=exchange_name,
                side=original_order.side,
                size=execution.cum_qty or 0.0,
                price=execution.average_price or 0.0,
                fee=total_fee,
                fee_asset=fee_asset,
                status=status,
            )
            await self._publish_fill(fill)
            if status in ("filled", "cancelled"):
                del self.pending_orders[exchange_order_id]
                logger.info(
                    f"[{exchange_name}] ExchangeOrderSnapshot {original_order.client_order_id} "
                    f"{status}, removed from pending"
                )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing execution: {e}")

    async def _publish_order_status(
        self,
        order: OrderRequestEnvelope,
        status: OrderEventType,
        exchange_order_id: str | None = None,
    ) -> None:
        """Publish order status event to the ZMQ topic.

        The status value is used both as the topic suffix and the payload
        status field, ensuring consistency between routing and content.

        Args:
            order: Order request envelope containing order details.
            status: Event type for topic suffix and payload status field.
            exchange_order_id: Exchange-assigned order ID (if known).
        """
        if not self.publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        try:
            instrument = order.instrument
            topic = f"orders.events.{exchange_name}.{instrument}.{status}"
            order_status = OrderStatusEnvelope(
                exchange_order_id=exchange_order_id,
                client_order_id=order.client_order_id,
                instrument=order.instrument,
                exchange=exchange_name,
                side=order.side,
                status=status,
                order_type=order.order_type,
                size=order.quantity,
                filled_size=0.0 if status == "rejected" else order.quantity,
                price=order.price,
            )
            await self.publisher.send_multipart(topic, order_status.to_json().encode("utf-8"))
            logger.info(
                f"[{exchange_name}] Published order event: {order.client_order_id} - {status}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing order event: {e}")

    async def _heartbeat_loop(self) -> None:
        """Periodically publish heartbeat messages."""
        exchange_name = self._get_exchange_name()
        while self.running:
            try:
                await asyncio.sleep(self.settings.zmq_heartbeat_interval_ms / 1000.0)
                if not self.running:
                    break
                self.heartbeat_seq += 1
                lag_ms = 0
                hb_msg = HeartbeatEnvelope(
                    component=f"executor_{exchange_name}",
                    sequence=self.heartbeat_seq,
                    status="healthy",
                    lag_ms=lag_ms,
                    meta={
                        "running": self.running,
                        "exchange": exchange_name,
                        "broker_xsub": self.settings.zmq_broker_xsub,
                        "broker_xpub": self.settings.zmq_broker_xpub,
                    },
                )
                topic = f"system.heartbeats.executor.{exchange_name}"
                await self._publish_heartbeat(topic, hb_msg)
            except Exception as e:
                logger.error(f"[{exchange_name}] Execution service heartbeat error: {e}")

    async def _publish_heartbeat(self, topic: str, message: HeartbeatEnvelope) -> None:
        """Publish heartbeat message to ZMQ.

        Args:
            topic: ZMQ topic string for the heartbeat.
            message: Heartbeat envelope to publish.
        """
        if not self.publisher or not self.running:
            return
        try:
            await self.publisher.send_multipart(topic, message.to_json().encode("utf-8"))
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")

    async def _handle_symbol_mapping_update(self, payload: str) -> None:
        """Handle symbol mapping cache invalidation message.

        Args:
            payload: JSON payload string from ZMQ message.
        """
        exchange_name = self._get_exchange_name()
        try:
            SymbolMappingUpdateEnvelope.from_json(payload)
            logger.info(f"[{exchange_name}] Received symbol mapping cache invalidation")
            SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)
            logger.debug(f"[{exchange_name}] Symbol mapping cache invalidated successfully")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error handling symbol mapping update: {e}")

    async def _handle_settings_update(self, payload: str) -> None:
        """Handle settings update message and refresh cached settings.

        Args:
            payload: JSON payload string from ZMQ message.
        """
        exchange_name = self._get_exchange_name()
        try:
            envelope = SettingChangedEnvelope.from_json(payload)
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
