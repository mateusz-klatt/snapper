"""REST API list response wrappers for market and trading data.

This module defines typed envelope schemas that wrap lists of data items
returned by REST API query endpoints (candles, signals, orders, executions,
positions, exchanges, instruments). Each wrapper inherits PayloadListResponse
and carries provenance fields plus a count for client convenience.
"""

from typing import Literal

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ContinuousCandleData
from snapper.messaging.schemas.data import ContractData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import FrontMonthData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import PositionData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import UnderlyingAssetData
from snapper.messaging.schemas.data import UnderlyingInstrumentData


class CandleListResponse(PayloadListResponse[Literal["candle_list"], CandleData]):
    """Candle list response wrapper.

    Wraps a list of CandleData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of candle data items.
        count: Total number of candles in the response.
    """

    type: Literal["candle_list"] = "candle_list"


class SignalListResponse(PayloadListResponse[Literal["signal_list"], SignalData]):
    """Signal list response wrapper.

    Wraps a list of SignalData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of signal data items.
        count: Total number of signals in the response.
    """

    type: Literal["signal_list"] = "signal_list"


class OrderListResponse(PayloadListResponse[Literal["order_list"], OrderData]):
    """Order list response wrapper.

    Wraps a list of OrderData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of order data items.
        count: Total number of orders in the response.
    """

    type: Literal["order_list"] = "order_list"


class ExecutionListResponse(PayloadListResponse[Literal["execution_list"], ExecutionData]):
    """Execution list response wrapper.

    Wraps a list of ExecutionData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of execution data items.
        count: Total number of executions in the response.
    """

    type: Literal["execution_list"] = "execution_list"


class PositionListResponse(PayloadListResponse[Literal["position_list"], PositionData]):
    """Position list response wrapper.

    Wraps a list of PositionData items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of position data items.
        count: Total number of positions in the response.
    """

    type: Literal["position_list"] = "position_list"


class ExchangeListResponse(PayloadListResponse[Literal["exchange_list"], str]):
    """Exchange list response wrapper.

    Wraps a list of exchange name strings with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of exchange name strings.
        count: Total number of exchanges in the response.
    """

    type: Literal["exchange_list"] = "exchange_list"


class InstrumentListResponse(PayloadListResponse[Literal["instrument_list"], str]):
    """Instrument list response wrapper.

    Wraps a list of instrument symbol strings with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of instrument symbol strings.
        count: Total number of instruments in the response.
    """

    type: Literal["instrument_list"] = "instrument_list"


class UnderlyingAssetListResponse(
    PayloadListResponse[Literal["underlying_asset_list"], UnderlyingAssetData],
):
    """Underlying asset list response wrapper.

    Attributes:
        type: Payload item type discriminator.
        payload: List of underlying asset data items.
        count: Total number of underlying assets in the response.
    """

    type: Literal["underlying_asset_list"] = "underlying_asset_list"


class UnderlyingInstrumentListResponse(
    PayloadListResponse[Literal["underlying_instrument_list"], UnderlyingInstrumentData],
):
    """Underlying instrument list response wrapper.

    Attributes:
        type: Payload item type discriminator.
        payload: List of underlying instrument data items.
        count: Total number of instruments in the response.
    """

    type: Literal["underlying_instrument_list"] = "underlying_instrument_list"


class ContinuousCandleListResponse(
    PayloadListResponse[Literal["continuous_candle_list"], ContinuousCandleData],
):
    """Continuous contract candle list response wrapper.

    Attributes:
        type: Payload item type discriminator.
        payload: List of stitched continuous candle items.
        count: Total number of candles in the response.
    """

    type: Literal["continuous_candle_list"] = "continuous_candle_list"


class FrontMonthResponse(
    PayloadResponse[Literal["front_month"], FrontMonthData],
):
    """Front-month instrument response wrapper.

    Attributes:
        type: Payload type discriminator.
        payload: Front-month instrument data.
    """

    type: Literal["front_month"] = "front_month"


class ContractListResponse(
    PayloadListResponse[Literal["contract_list"], ContractData],
):
    """Contract ladder list response wrapper.

    Attributes:
        type: Payload item type discriminator.
        payload: List of contract data items.
        count: Total number of contracts in the response.
    """

    type: Literal["contract_list"] = "contract_list"


__all__ = [
    "CandleListResponse",
    "ContractListResponse",
    "ExecutionListResponse",
    "ExchangeListResponse",
    "FrontMonthResponse",
    "InstrumentListResponse",
    "OrderListResponse",
    "PositionListResponse",
    "SignalListResponse",
    "UnderlyingAssetListResponse",
    "UnderlyingInstrumentListResponse",
]
