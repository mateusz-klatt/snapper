"""Execution schemas for the REST API.

This module defines response schemas for trade execution records
returned by execution-related endpoints.
"""

from datetime import datetime

from snapper.api.schemas.base import StrictApiSchema
from snapper.core.types import OrderExchange
from snapper.interface.websocket.schemas import TradeSide


class ExecutionRecord(StrictApiSchema):
    """Trade execution record response schema.

    Represents a single trade execution (fill) from an order.

    Attributes:
        id: Unique execution identifier.
        order_id: Related order identifier.
        exec_id: Exchange-assigned execution ID.
        trade_id: Exchange-assigned trade ID.
        timestamp: Wall-clock timestamp when execution was recorded.
        executed_at: Exchange-provided execution time.
        price: Execution price.
        size: Executed quantity.
        fee: Transaction fee.
        fee_asset: Currency of the fee.
        instrument: Trading instrument symbol.
        side: Trade side (buy/sell).
        exchange: Exchange name.
    """

    id: int
    order_id: int
    exec_id: str | None = None
    trade_id: str | None = None
    timestamp: datetime
    executed_at: datetime | None = None
    price: float
    size: float
    fee: float
    fee_asset: str
    instrument: str
    side: TradeSide
    exchange: OrderExchange
