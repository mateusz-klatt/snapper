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
from snapper.messaging.schemas.data import ExecutionPlanDecisionData
from snapper.messaging.schemas.data import FrontMonthData
from snapper.messaging.schemas.data import InstrumentCapabilityData
from snapper.messaging.schemas.data import InstrumentDetailData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import PortfolioAccountState
from snapper.messaging.schemas.data import PositionData
from snapper.messaging.schemas.data import RelatedInstrumentsPayloadData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import UnderlyingAssetData
from snapper.messaging.schemas.data import UnderlyingInstrumentData
from snapper.messaging.schemas.data import VenueFeeScheduleData


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


class PortfolioAccountStateListResponse(
    PayloadListResponse[Literal["portfolio_account_state_list"], PortfolioAccountState]
):
    """Venue account-state list response wrapper (PnL Phase 3).

    Wraps a list of PortfolioAccountState items with a count.

    Attributes:
        type: Payload item type discriminator.
        payload: List of venue account-state items.
        count: Total number of account states in the response.
    """

    type: Literal["portfolio_account_state_list"] = "portfolio_account_state_list"


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


class InstrumentDetailListResponse(
    PayloadListResponse[Literal["instrument_detail_list"], InstrumentDetailData],
):
    """Capability-aware instrument list response wrapper.

    Wraps ``InstrumentDetailData`` items so the frontend can render
    market-data-only badges + disable order-entry for non-tradable
    instruments without a second round-trip.

    Attributes:
        type: Payload item type discriminator.
        payload: List of ``InstrumentDetailData`` items.
        count: Total number of instruments in the response.
    """

    type: Literal["instrument_detail_list"] = "instrument_detail_list"


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


class RelatedInstrumentsResponse(
    PayloadResponse[Literal["related_instruments"], RelatedInstrumentsPayloadData],
):
    """Related-instruments row response for the MarketData header.

    Wraps the ``GET /api/instruments/{exchange}/{native_symbol}/related``
    payload (selected echo + underlying summary + relationship-grouped
    siblings) so the MarketData page can render the chip row below its
    dropdown without an extra round-trip for underlying resolution.

    Attributes:
        type: Payload type discriminator.
        payload: Selected echo + nullable underlying + grouped sibling
            list. ``underlying`` is ``None`` and ``groups`` is empty for
            orphan symbols (symbol exists but has no underlying mapping).
    """

    type: Literal["related_instruments"] = "related_instruments"


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


class ExecutionPlanDecisionListResponse(
    PayloadListResponse[Literal["execution_plan_decision_list"], ExecutionPlanDecisionData],
):
    """Execution-plan decision-audit list response wrapper.

    Returned by ``GET /api/execution-plans/{id}/decisions`` and
    ``GET /api/trailing-stops/{id}/decisions``.

    Attributes:
        type: Payload item type discriminator.
        payload: List of decision rows, newest-first.
        count: Total number of decisions in the response.
    """

    type: Literal["execution_plan_decision_list"] = "execution_plan_decision_list"


class InstrumentCapabilityListResponse(
    PayloadListResponse[Literal["instrument_capability_list"], InstrumentCapabilityData],
):
    """Instrument order capability list response wrapper.

    Attributes:
        type: Payload item type discriminator.
        payload: List of instrument capability data items.
        count: Total number of capabilities in the response.
    """

    type: Literal["instrument_capability_list"] = "instrument_capability_list"


class VenueFeeScheduleListResponse(
    PayloadListResponse[Literal["venue_fee_schedule_list"], VenueFeeScheduleData],
):
    """Venue fee schedule list response wrapper.

    Attributes:
        type: Payload item type discriminator.
        payload: List of fee schedule data items.
        count: Total number of fee schedules in the response.
    """

    type: Literal["venue_fee_schedule_list"] = "venue_fee_schedule_list"


__all__ = [
    "CandleListResponse",
    "ContractListResponse",
    "ExecutionListResponse",
    "ExchangeListResponse",
    "FrontMonthResponse",
    "InstrumentCapabilityListResponse",
    "InstrumentListResponse",
    "OrderListResponse",
    "PositionListResponse",
    "SignalListResponse",
    "UnderlyingAssetListResponse",
    "UnderlyingInstrumentListResponse",
    "VenueFeeScheduleListResponse",
]
