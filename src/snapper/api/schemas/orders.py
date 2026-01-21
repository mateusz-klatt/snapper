"""Order schemas for the REST API.

This module defines response schemas for order data returned by
order-related endpoints.
"""

from datetime import datetime

from snapper.api.schemas.base import StrictApiSchema
from snapper.interface.websocket.schemas import OrderStatus as OrderStatusLiteral
from snapper.interface.websocket.schemas import OrderType
from snapper.interface.websocket.schemas import TradeSide


class OrderStatus(StrictApiSchema):
    """Order status response schema.

    Represents the current state of a trading order.

    Attributes:
        id: Unique internal order identifier.
        instrument: Trading instrument symbol.
        exchange: Exchange name (empty string if not specified).
        client_order_id: Client-assigned order ID.
        exchange_order_id: Exchange-assigned order ID.
        created_at: Order creation timestamp.
        updated_at: Last update timestamp.
        side: Order side (buy/sell).
        type: Order type (market/limit).
        price: Limit price (None for market orders).
        size: Order quantity.
        status: Current order status.
        time_in_force: Order time-in-force setting.
        error: Error message if order failed.
    """

    id: int
    instrument: str
    exchange: str = ""
    client_order_id: str | None
    exchange_order_id: str | None
    created_at: datetime
    updated_at: datetime | None
    side: TradeSide
    type: OrderType
    price: float | None
    size: float
    status: OrderStatusLiteral
    time_in_force: str | None = None
    error: str | None = None
