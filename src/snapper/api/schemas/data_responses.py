"""REST API list response wrappers for market and trading data.

This module defines typed envelope schemas that wrap lists of data items
returned by REST API query endpoints (candles, signals, orders, executions,
positions, exchanges, instruments). Each wrapper inherits PayloadListResponse
and carries provenance fields plus a count for client convenience.
"""

from typing import Literal

from snapper.api.schemas.base import PayloadListResponse
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import PositionData
from snapper.messaging.schemas.data import SignalData


class CandleListResponse(PayloadListResponse[CandleData]):
    """Candle list response wrapper.

    Wraps a list of CandleData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of candle data items.
        count: Total number of candles in the response.
    """

    type: Literal["candle_list"] = "candle_list"


class SignalListResponse(PayloadListResponse[SignalData]):
    """Signal list response wrapper.

    Wraps a list of SignalData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of signal data items.
        count: Total number of signals in the response.
    """

    type: Literal["signal_list"] = "signal_list"


class OrderListResponse(PayloadListResponse[OrderData]):
    """Order list response wrapper.

    Wraps a list of OrderData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of order data items.
        count: Total number of orders in the response.
    """

    type: Literal["order_list"] = "order_list"


class ExecutionListResponse(PayloadListResponse[ExecutionData]):
    """Execution list response wrapper.

    Wraps a list of ExecutionData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of execution data items.
        count: Total number of executions in the response.
    """

    type: Literal["execution_list"] = "execution_list"


class PositionListResponse(PayloadListResponse[PositionData]):
    """Position list response wrapper.

    Wraps a list of PositionData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of position data items.
        count: Total number of positions in the response.
    """

    type: Literal["position_list"] = "position_list"


class ExchangeListResponse(PayloadListResponse[str]):
    """Exchange list response wrapper.

    Wraps a list of exchange name strings with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of exchange name strings.
        count: Total number of exchanges in the response.
    """

    type: Literal["exchange_list"] = "exchange_list"


class InstrumentListResponse(PayloadListResponse[str]):
    """Instrument list response wrapper.

    Wraps a list of instrument symbol strings with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of instrument symbol strings.
        count: Total number of instruments in the response.
    """

    type: Literal["instrument_list"] = "instrument_list"


__all__ = [
    "CandleListResponse",
    "SignalListResponse",
    "OrderListResponse",
    "ExecutionListResponse",
    "PositionListResponse",
    "ExchangeListResponse",
    "InstrumentListResponse",
]
