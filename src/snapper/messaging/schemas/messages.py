"""ZMQ message envelope schemas for inter-process communication.

This module defines typed message envelopes used for communication between
Snapper processes via ZeroMQ pub/sub messaging. Each envelope wraps domain
data (ticks, bars, signals, orders) with metadata for routing and parsing.

The messaging system uses topic-based routing where publishers send to
topics like 'market.kraken.BTC-USD.ticks' and subscribers filter by patterns.

Classes:
    MessageEnvelopeBase: Base class with common envelope fields.
    TickEnvelope: Real-time price tick message.
    BarEnvelope: OHLCV candle bar message.
    TradeEnvelope: Trade execution from market.
    SignalEnvelope: Trading signal from strategy.
    OrderRequestEnvelope: Order request from strategy to executor.
    FillEnvelope: Order fill confirmation.
    OrderStatusEnvelope: Order status update.
    HeartbeatEnvelope: Component health heartbeat.
    SettingChangedEnvelope: Configuration change notification.
    SymbolAliasUpdateEnvelope: Symbol alias cache invalidation.
    ReplayStartEnvelope: Historical data replay start marker.
    ReplayEndEnvelope: Historical data replay end marker.

Functions:
    parse_message: Deserialize JSON string to typed message envelope.
"""

import json
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import Field

from snapper.core.types import OrderEventType
from snapper.core.types import OrderExchange
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.interface.websocket.schemas import HealthStatus
from snapper.interface.websocket.schemas import OrderType
from snapper.interface.websocket.schemas import TradeSide
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import FillData
from snapper.messaging.schemas.data import OrderStatusData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData


class MessageEnvelopeBase(BaseModel):
    """Base class for all ZMQ message envelopes.

    Provides common fields and serialization methods for message transport.
    All message types inherit from this base to ensure consistent handling.

    Attributes:
        type: Message type discriminator for deserialization.
        timestamp: Message creation timestamp (UTC).
        meta: Optional metadata dictionary for extensions.
    """

    type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    meta: dict[str, Any] = Field(default_factory=dict)

    def to_json(self) -> str:
        """Serialize envelope to JSON string for ZMQ transport.

        Returns:
            JSON string representation of the envelope.
        """
        return self.model_dump_json(by_alias=True)

    @classmethod
    def from_json(cls, data: str) -> Self:
        """Deserialize envelope from JSON string.

        Args:
            data: JSON string to parse.

        Returns:
            Typed message envelope instance.
        """
        return cls.model_validate_json(data)


class TickEnvelope(TickData, MessageEnvelopeBase):
    """Real-time price tick message envelope.

    Wraps TickData for transmission over the messaging bus.
    Published by market data publishers on tick updates.

    Attributes:
        type: Fixed as 'tick' for message routing.
    """

    type: Literal["tick"] = "tick"


class BarEnvelope(CandleData, MessageEnvelopeBase):
    """OHLCV candle bar message envelope.

    Wraps CandleData for transmission over the messaging bus.
    Published when a candle closes or during historical replay.

    Attributes:
        type: Fixed as 'bar' for message routing.
    """

    type: Literal["bar"] = "bar"


class TradeEnvelope(TradeData, MessageEnvelopeBase):
    """Trade execution message envelope.

    Wraps TradeData for transmission over the messaging bus.
    Published for each trade that occurs on an exchange.

    Attributes:
        type: Fixed as 'trade' for message routing.
    """

    type: Literal["trade"] = "trade"


MarketDataEnvelope = TickEnvelope | BarEnvelope | TradeEnvelope
"""Union type for all market data envelope types."""


class SignalEnvelope(SignalData, MessageEnvelopeBase):
    """Trading signal message envelope.

    Wraps SignalData for transmission from strategies to executors.
    Contains the recommendation to enter or exit a position.

    Attributes:
        type: Fixed as 'signal' for message routing.
        id: Unique signal identifier (optional).
    """

    type: Literal["signal"] = "signal"
    id: str | None = None


class OrderRequestEnvelope(MessageEnvelopeBase):
    """Order request message from strategy to executor.

    Sent by strategies to request order placement on an exchange.
    Contains all information needed for order creation.
    Published on: orders.commands.{exchange}.{instrument}.submit

    Attributes:
        type: Fixed as 'order_req' for message routing.
        strategy_id: Identifier of the requesting strategy.
        exchange: Target exchange for the order.
        instrument: Trading pair symbol.
        mode: Execution mode ('live' or 'paper').
        side: Order direction ('buy' or 'sell').
        order_type: Type of order ('market', 'limit', etc.).
        quantity: Order size (must be positive).
        price: Limit price (required for limit orders).
        client_order_id: Client-side order identifier.
        signaled_at: Original signal timestamp (optional).
    """

    type: Literal["order_req"] = "order_req"
    strategy_id: str
    exchange: OrderExchange
    instrument: str
    mode: ExecutionMode
    side: TradeSide
    order_type: OrderType
    quantity: float = Field(gt=0)
    price: float | None = None
    client_order_id: str
    signaled_at: datetime | None = None


class OrderCancelEnvelope(MessageEnvelopeBase):
    """Order cancel request message from strategy to executor.

    Sent to request cancellation of an existing order.
    Published on: orders.commands.{exchange}.{instrument}.cancel

    Cancel commands only carry identifying information since
    the executor already has order context from when the order was placed.

    Attributes:
        type: Fixed as 'order_cancel' for message routing.
        exchange: Target exchange for the cancel.
        instrument: Trading pair symbol.
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken's txid/UUID).
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
    """

    type: Literal["order_cancel"] = "order_cancel"
    exchange: OrderExchange
    instrument: str
    exchange_order_id: str
    client_order_id: str


class OrderReplaceEnvelope(MessageEnvelopeBase):
    """Order replace/modify request message from strategy to executor.

    Sent to request modification of an existing order (price/quantity).
    Published on: orders.commands.{exchange}.{instrument}.replace

    Replace commands carry only the fields needed for modification
    (exchange_order_id + new values). The executor has full order context.

    Attributes:
        type: Fixed as 'order_replace' for message routing.
        exchange: Target exchange for the replace.
        instrument: Trading pair symbol.
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken's txid/UUID).
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
        new_quantity: New order quantity (optional).
        new_price: New limit price (optional).
    """

    type: Literal["order_replace"] = "order_replace"
    exchange: OrderExchange
    instrument: str
    exchange_order_id: str
    client_order_id: str
    new_quantity: float | None = None
    new_price: float | None = None


class OrderEventEnvelope(MessageEnvelopeBase):
    """Lightweight order event envelope for cancel/replace confirmations.

    Used for publishing order lifecycle events that don't require full order
    details. This is the preferred envelope for cancel/replace results because
    those commands don't carry side/order_type information.

    Published on: orders.events.{exchange}.{instrument}.{event}
    where event is 'cancelled', 'rejected', 'replaced', etc.

    INVARIANT: The 'event' field MUST match the topic suffix.

    Attributes:
        type: Fixed as 'order_event' for message routing.
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken's txid/UUID).
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
        exchange: Exchange where the order exists.
        instrument: Trading pair symbol.
        event: Event type matching topic suffix (OrderEventType).
        reason: Optional rejection/cancellation reason.
    """

    type: Literal["order_event"] = "order_event"
    exchange_order_id: str
    client_order_id: str
    exchange: OrderExchange
    instrument: str
    event: OrderEventType
    reason: str | None = None


class FillEnvelope(FillData, MessageEnvelopeBase):
    """Order fill message envelope.

    Wraps FillData for transmission from executors.
    Published when an order is filled (fully or partially).

    Attributes:
        type: Fixed as 'fill' for message routing.
        executed_at: Fill execution timestamp.
    """

    type: Literal["fill"] = "fill"
    executed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OrderStatusEnvelope(OrderStatusData, MessageEnvelopeBase):
    """Order status update message envelope.

    Wraps OrderStatusData for transmission from executors.
    Published when order state changes.

    Attributes:
        type: Fixed as 'order_status' for message routing.
        created_at: Order creation timestamp.
    """

    type: Literal["order_status"] = "order_status"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class HeartbeatEnvelope(MessageEnvelopeBase):
    """Component health heartbeat message.

    Published periodically by components to indicate they are alive.
    Used for health monitoring and dead component detection.

    Attributes:
        type: Fixed as 'heartbeat' for message routing.
        component: Name of the sending component.
        sequence: Monotonically increasing sequence number.
        status: Current health status.
        lag_ms: Processing lag in milliseconds.
    """

    type: Literal["heartbeat"] = "heartbeat"
    component: str
    sequence: int
    status: HealthStatus
    lag_ms: int


class SettingChangedEnvelope(MessageEnvelopeBase):
    """Configuration setting change notification.

    Published when a setting is modified in the database.
    Subscribers use this to invalidate caches or reload config.

    Attributes:
        type: Fixed as 'setting_changed' for message routing.
        key: Setting key that changed.
        value: New setting value.
        category: Setting category for grouping.
        updated_by: User who made the change (optional).
    """

    type: Literal["setting_changed"] = "setting_changed"
    key: str
    value: str
    category: str
    updated_by: str | None = None


class SymbolAliasUpdateEnvelope(MessageEnvelopeBase):
    """Symbol alias cache invalidation message.

    Published when symbol aliases are updated in the database.
    Subscribers should clear their symbol mapper caches.

    Attributes:
        type: Fixed as 'symbol_alias_update' for message routing.
        event: Event type (always 'symbol_aliases_updated').
        action: Required action (always 'clear_cache').
    """

    type: Literal["symbol_alias_update"] = "symbol_alias_update"
    event: Literal["symbol_aliases_updated"] = "symbol_aliases_updated"
    action: Literal["clear_cache"] = "clear_cache"


class ReplayStartEnvelope(MessageEnvelopeBase):
    """Historical data replay start marker.

    Sent at the beginning of a historical data replay session.
    Strategies use this to reset state before receiving replayed data.

    Attributes:
        type: Fixed as 'replay_start' for message routing.
        started_at: Replay start timestamp (optional).
    """

    type: Literal["replay_start"] = "replay_start"
    started_at: datetime | None = None


class ReplayEndEnvelope(MessageEnvelopeBase):
    """Historical data replay end marker.

    Sent at the end of a historical data replay session.
    Strategies use this to finalize analysis and generate reports.

    Attributes:
        type: Fixed as 'replay_end' for message routing.
    """

    type: Literal["replay_end"] = "replay_end"


class MessageParseError(Exception):
    """Raised when message deserialization fails.

    Indicates malformed JSON, missing type field, unknown message type,
    or validation errors during parsing.
    """

    pass


MESSAGE_TYPE_MAP: dict[str, type[MessageEnvelopeBase]] = {
    "tick": TickEnvelope,
    "bar": BarEnvelope,
    "trade": TradeEnvelope,
    "signal": SignalEnvelope,
    "order_req": OrderRequestEnvelope,
    "order_cancel": OrderCancelEnvelope,
    "order_replace": OrderReplaceEnvelope,
    "order_event": OrderEventEnvelope,
    "fill": FillEnvelope,
    "heartbeat": HeartbeatEnvelope,
    "setting_changed": SettingChangedEnvelope,
    "order_status": OrderStatusEnvelope,
    "symbol_alias_update": SymbolAliasUpdateEnvelope,
    "replay_start": ReplayStartEnvelope,
    "replay_end": ReplayEndEnvelope,
}
"""Mapping from message type string to envelope class for deserialization."""


def parse_message(data: str) -> MessageEnvelopeBase:
    """Parse JSON string into typed message envelope.

    Deserializes a JSON message string and returns the appropriate
    typed envelope based on the 'type' field discriminator.

    Args:
        data: JSON string containing the serialized message.

    Returns:
        Typed message envelope instance (TickEnvelope, BarEnvelope, etc.).

    Raises:
        MessageParseError: If JSON is invalid, type field is missing,
            message type is unknown, or validation fails.
    """
    try:
        raw_data = json.loads(data)
    except json.JSONDecodeError as e:
        raise MessageParseError(f"Invalid JSON: {e}") from e
    msg_type = raw_data.get("type")
    if not msg_type:
        raise MessageParseError("Message missing 'type' field")
    message_class = MESSAGE_TYPE_MAP.get(msg_type)
    if not message_class:
        raise MessageParseError(f"Unknown message type: {msg_type}")
    try:
        return message_class.model_validate(raw_data)
    except Exception as e:
        raise MessageParseError(f"Failed to validate {msg_type} message: {e}") from e
