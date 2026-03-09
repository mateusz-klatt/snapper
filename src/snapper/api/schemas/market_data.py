"""Market data schemas for the REST API.

This module defines response schemas for market data returned by
market data endpoints.
"""

from snapper.api.schemas.base import StrictApiSchema
from snapper.messaging.schemas.data import CandleData


class CandleSnapshot(CandleData, StrictApiSchema):
    """OHLCV candle snapshot response schema.

    Inherits all candle fields from CandleData (instrument, exchange,
    timeframe, open_at, OHLCV, vwap, trades). The StrictApiSchema
    mixin adds strict validation for REST responses.
    """
