"""Portfolio schemas for the REST API.

This module defines response schemas for portfolio position data
returned by portfolio-related endpoints.
"""

from datetime import datetime

from snapper.api.schemas.base import StrictApiSchema


class PositionSnapshot(StrictApiSchema):
    """Portfolio position snapshot response schema.

    Represents a snapshot of a single position in the portfolio.

    Attributes:
        id: Unique position identifier.
        instrument: Trading instrument symbol.
        exchange: Exchange name.
        quantity: Position size (positive for long, negative for short).
        average_price: Average entry price.
        unrealized_pnl: Unrealized profit/loss.
        realized_pnl: Realized profit/loss.
        updated_at: Last update timestamp.
    """

    id: int
    instrument: str
    exchange: str
    quantity: float
    average_price: float
    unrealized_pnl: float
    realized_pnl: float
    updated_at: datetime
