"""REST API list response wrappers for market and trading data.

This module defines typed envelope schemas that wrap lists of data items
returned by REST API query endpoints (candles, signals, orders, executions,
positions, exchanges, instruments). Each wrapper inherits StrictDataSchema
and carries provenance fields plus a count for client convenience.
"""

from typing import Literal

from snapper.api.schemas.base import StrictDataSchema
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import PositionData
from snapper.messaging.schemas.data import SignalData


class CandleListResponse(StrictDataSchema):
    """Candle list response wrapper.

    Wraps a list of CandleData items with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of candle data items.
        count: Total number of candles in the response.
    """

    type: Literal["candle_list"] = "candle_list"
    items: list[CandleData]
    count: int


class SignalListResponse(StrictDataSchema):
    """Signal list response wrapper.

    Wraps a list of SignalData items with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of signal data items.
        count: Total number of signals in the response.
    """

    type: Literal["signal_list"] = "signal_list"
    items: list[SignalData]
    count: int


class OrderListResponse(StrictDataSchema):
    """Order list response wrapper.

    Wraps a list of OrderData items with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of order data items.
        count: Total number of orders in the response.
    """

    type: Literal["order_list"] = "order_list"
    items: list[OrderData]
    count: int


class ExecutionListResponse(StrictDataSchema):
    """Execution list response wrapper.

    Wraps a list of ExecutionData items with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of execution data items.
        count: Total number of executions in the response.
    """

    type: Literal["execution_list"] = "execution_list"
    items: list[ExecutionData]
    count: int


class PositionListResponse(StrictDataSchema):
    """Position list response wrapper.

    Wraps a list of PositionData items with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of position data items.
        count: Total number of positions in the response.
    """

    type: Literal["position_list"] = "position_list"
    items: list[PositionData]
    count: int


class ExchangeListResponse(StrictDataSchema):
    """Exchange list response wrapper.

    Wraps a list of exchange name strings with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of exchange name strings.
        count: Total number of exchanges in the response.
    """

    type: Literal["exchange_list"] = "exchange_list"
    items: list[str]
    count: int


class InstrumentListResponse(StrictDataSchema):
    """Instrument list response wrapper.

    Wraps a list of instrument symbol strings with a count.

    Attributes:
        type: Payload item type discriminator.
        items: List of instrument symbol strings.
        count: Total number of instruments in the response.
    """

    type: Literal["instrument_list"] = "instrument_list"
    items: list[str]
    count: int


__all__ = [
    "CandleListResponse",
    "SignalListResponse",
    "OrderListResponse",
    "ExecutionListResponse",
    "PositionListResponse",
    "ExchangeListResponse",
    "InstrumentListResponse",
]
