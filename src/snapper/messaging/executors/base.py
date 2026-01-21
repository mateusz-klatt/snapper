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
from snapper.messaging.schemas.messages import OrderRequestEnvelope
from snapper.messaging.schemas.messages import OrderStatusEnvelope
from snapper.messaging.schemas.messages import SettingChangedEnvelope
from snapper.messaging.schemas.messages import SymbolMappingUpdateEnvelope
from snapper.messaging.schemas.messages import parse_message
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
        orders_prefix = f"orders.{exchange_name}."
        self.subscriber.subscribe(orders_prefix)
        self.subscriber.subscribe("system.symbol_mappings")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: Subscribed to {orders_prefix}, "
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
        """Process incoming order requests from ZMQ subscription."""
        exchange_name = self._get_exchange_name()
        orders_topic = f"orders.{exchange_name}.requests"
        while self.running:
            try:
                if self.subscriber:
                    topic_str, payload_bytes = await self.subscriber.recv_multipart()
                    payload_str = payload_bytes.decode("utf-8")
                    if topic_str == orders_topic:
                        order_msg = parse_message(payload_str)
                        if isinstance(order_msg, OrderRequestEnvelope):
                            if order_msg.exchange != exchange_name:
                                logger.warning(
                                    f"Received order for wrong exchange: {order_msg.exchange} "
                                    f"(expected {exchange_name})"
                                )
                                continue
                            await self._process_order(order_msg)
                        else:
                            logger.warning(f"Received non-order message: {order_msg.type}")
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

    async def _process_order(self, order: OrderRequestEnvelope) -> None:
        """Submit an order to the exchange and handle the response.

        Args:
            order: Order request envelope containing order details.
        """
        exchange_name = self._get_exchange_name()
        try:
            await self._publish_order_status(order, "submitted")
            exchange_order_id = await self._execute_live_order(order)
            if exchange_order_id:
                logger.info(
                    f"[{exchange_name}] Order {order.client_order_id} "
                    f"submitted as {exchange_order_id}, waiting for execution"
                )
            else:
                rejection = FillEnvelope(
                    id=order.client_order_id,
                    order_id=order.client_order_id,
                    instrument=order.instrument,
                    exchange=exchange_name,
                    side=order.side,
                    size=0.0,
                    price=0.0,
                    fee=0.0,
                    fee_asset="",
                    status="rejected",
                )
                await self._publish_fill(rejection)
                await self._publish_order_status(order, "rejected")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing order {order.client_order_id}: {e}")
            rejection = FillEnvelope(
                id=order.client_order_id,
                order_id=order.client_order_id,
                instrument=order.instrument,
                exchange=exchange_name,
                side=order.side,
                size=0.0,
                price=0.0,
                fee=0.0,
                fee_asset="",
                status="rejected",
            )
            await self._publish_fill(rejection)
            await self._publish_order_status(order, "rejected")
            await self._publish_order_status(order, "rejected")

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
            topic = f"executions.{exchange_name}.{fill.instrument}.fill"
            await self.publisher.send_multipart(topic, fill.to_json().encode("utf-8"))
            logger.info(
                f"[{exchange_name}] Published fill: {fill.order_id} - {fill.size}@{fill.price}"
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
                id=exchange_order_id or original_order.client_order_id,
                order_id=original_order.client_order_id,
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

    async def _publish_order_status(self, order: OrderRequestEnvelope, status: str) -> None:
        """Publish order status update to the ZMQ topic.

        Args:
            order: Order request envelope containing order details.
            status: Current status string for the order.
        """
        if not self.publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        try:
            instrument = order.instrument
            topic = f"orders.{exchange_name}.{instrument}.status"
            order_status = OrderStatusEnvelope(
                id=order.client_order_id,
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
                f"[{exchange_name}] Published order status: {order.client_order_id} - {status}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing order status: {e}")

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
