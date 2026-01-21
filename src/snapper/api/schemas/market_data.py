"""Market data schemas for the REST API.

This module defines response schemas for market data returned by
market data endpoints.
"""

from datetime import datetime

from snapper.api.schemas.base import StrictApiSchema


class CandleSnapshot(StrictApiSchema):
    """OHLCV candle snapshot response schema.

    Represents a single candlestick data point.

    Attributes:
        instrument: Trading instrument symbol.
        timeframe: Candle timeframe (e.g., '1m', '1h', '1d').
        timestamp: Candle open timestamp.
        open: Opening price.
        high: Highest price in the period.
        low: Lowest price in the period.
        close: Closing price.
        volume: Trading volume.
        vwap: Volume-weighted average price (optional).
        trades: Number of trades in the period (optional).
    """

    instrument: str
    timeframe: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    trades: int | None = None
