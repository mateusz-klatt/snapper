"""Exchange contracts and data transfer objects.

Defines common data structures for orders, executions, and market data
shared across all exchange implementations.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from enum import StrEnum
from typing import Literal

from snapper.core.types import FillStatus
from snapper.core.types import FillStatusEnum


class OrderSideEnum(Enum):
    """Enumeration of order sides (buy/sell)."""

    BUY = "buy"
    SELL = "sell"


class ExchangeOrderTypeEnum(StrEnum):
    """Exchange-wire-format order type.

    Covers SDK-reported order types across all supported exchanges
    (Kraken, Kraken Futures, Walutomat, Polygon). Distinct
    from ``snapper.core.types.OrderTypeEnum`` (domain trading-core
    type) by design — exchange values include ICEBERG, STOP_LOSS_LIMIT,
    TAKE_PROFIT_LIMIT, TRAILING_STOP_LIMIT, SETTLE_POSITION that the
    trading core never reasons about.
    """

    LIMIT = "limit"
    MARKET = "market"
    ICEBERG = "iceberg"
    STOP_LOSS = "stop-loss"
    STOP_LOSS_LIMIT = "stop-loss-limit"
    TAKE_PROFIT = "take-profit"
    TAKE_PROFIT_LIMIT = "take-profit-limit"
    TRAILING_STOP = "trailing-stop"
    TRAILING_STOP_LIMIT = "trailing-stop-limit"
    SETTLE_POSITION = "settle-position"


class ExchangeOrderStatusEnum(StrEnum):
    """Exchange-wire-format order status.

    Covers SDK-reported order statuses across all supported exchanges.
    Distinct from ``snapper.core.types.OrderStatusEnum`` (domain
    trading-core status) by design — exchange values use American
    spelling (``CANCELED``) and include ``PENDING``, ``CLOSED``,
    ``PENDING_NEW``, ``EXPIRED`` that the trading core reduces to its
    7-state lifecycle. The cross-spelling mapping between domain
    ``CANCELLED`` and exchange ``CANCELED`` is handled in
    ``implementations/kraken.py`` and ``adapters/kraken_futures.py``.
    """

    PENDING = "pending"
    OPEN = "open"
    CLOSED = "closed"
    PENDING_NEW = "pending_new"
    NEW = "new"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    EXPIRED = "expired"


class TimeInForceEnum(Enum):
    """Enumeration of time-in-force policies."""

    GTC = "GTC"
    GTD = "GTD"
    IOC = "IOC"


def to_fill_status(execution: ExecutionUpdate) -> FillStatus:
    """Derive FillStatus from an ExecutionUpdate.

    Single source of truth for mapping exchange-level order status to
    the domain FillStatus used in ExecutionData and the DB executions table.

    Args:
        execution: Execution update from exchange WebSocket or REST.

    Returns:
        'partial' when order is still open with partial fills,
        'filled' otherwise (closed, explicitly filled, or default).
    """
    if execution.order_status == ExchangeOrderStatusEnum.OPEN and (execution.cum_qty or 0) > 0:
        return FillStatusEnum.PARTIAL
    return FillStatusEnum.FILLED


def normalize_order_status(status: ExchangeOrderStatusEnum) -> ExchangeOrderStatusEnum:
    normalization_map = {
        ExchangeOrderStatusEnum.PENDING: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.PENDING_NEW: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.NEW: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.PARTIALLY_FILLED: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.FILLED: ExchangeOrderStatusEnum.CLOSED,
        ExchangeOrderStatusEnum.OPEN: ExchangeOrderStatusEnum.OPEN,
        ExchangeOrderStatusEnum.CLOSED: ExchangeOrderStatusEnum.CLOSED,
        ExchangeOrderStatusEnum.CANCELED: ExchangeOrderStatusEnum.CANCELED,
        ExchangeOrderStatusEnum.EXPIRED: ExchangeOrderStatusEnum.EXPIRED,
    }
    return normalization_map.get(status, status)


@dataclass
class TickerSnapshot:
    """Snapshot of ticker data for a trading symbol."""

    symbol: str
    bid: float
    ask: float
    last: float
    timestamp: float


@dataclass
class OhlcvSnapshot:
    """Snapshot of OHLCV (Open-High-Low-Close-Volume) candle data."""

    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class AccountBalance:
    """Account balance for a single currency."""

    currency: str
    free: float
    used: float
    total: float


@dataclass
class ExchangeOrderRequest:
    """Request parameters for placing an order on an exchange."""

    symbol: str
    side: OrderSideEnum
    type: ExchangeOrderTypeEnum
    amount: float
    price: float | None = None
    stop_price: float | None = None
    client_order_id: str | None = None
    signaled_at: datetime | None = None
    leverage: int | None = None
    reduce_only: bool = False
    post_only: bool = False
    wallet_public_id: str = ""
    operator_public_id: str | None = None


type ExecType = Literal[
    "pending_new",
    "new",
    "trade",
    "filled",
    "iceberg_refill",
    "canceled",
    "expired",
    "amended",
    "restated",
    "status",
]
type LiquidityIndicator = Literal["m", "t"]
__all__ = [
    "to_fill_status",
    "OrderSideEnum",
    "ExchangeOrderTypeEnum",
    "ExchangeOrderStatusEnum",
    "TimeInForceEnum",
    "TickerSnapshot",
    "OhlcvSnapshot",
    "AccountBalance",
    "ExchangeOrderRequest",
    "ExecType",
    "LiquidityIndicator",
    "ExchangeOrderSnapshot",
    "FundingRateSnapshot",
    "OpenPositionSnapshot",
    "TickerUpdate",
    "CandleUpdate",
    "TradeUpdate",
    "ExecutionFeeBreakdown",
    "ExecutionUpdate",
    "InstrumentPairDescriptor",
]


@dataclass
class ExchangeOrderSnapshot:
    """Snapshot of an order's current state on an exchange."""

    id: str
    client_order_id: str | None
    symbol: str
    side: OrderSideEnum
    type: ExchangeOrderTypeEnum
    amount: float
    price: float | None
    status: ExchangeOrderStatusEnum
    filled: float
    remaining: float
    timestamp: float
    fee: float | None = None
    fee_currency: str | None = None
    db_order_id: int | None = None
    db_order_public_id: str | None = None


@dataclass
class TickerUpdate:
    """Real-time ticker update with full market data.

    ``is_delayed`` marks feeds that deliver ticks with an exchange-mandated
    delay (Kraken FCM / TradFi index futures publishes ~10-minute-delayed
    prices to non-subscribed users). Strategies subscribing via ZMQ must
    gate on this flag before treating the price as current.

    ``is_extended_hours`` indicates the tick occurred during extended trading
    hours where applicable (TradFi equities overnight session); ``None``
    means the feed does not distinguish extended-hours ticks.
    """

    symbol: str
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float
    last: float
    volume: float
    vwap: float
    low: float
    high: float
    change: float
    change_pct: float
    is_delayed: bool = False
    is_extended_hours: bool | None = None


@dataclass
class CandleUpdate:
    """Real-time candle update with OHLCV data."""

    symbol: str
    open: float
    high: float
    low: float
    close: float
    vwap: float
    trades: int
    volume: float
    interval_begin: datetime
    interval: int


@dataclass
class TradeUpdate:
    """Real-time trade update from the exchange."""

    symbol: str
    side: str
    quantity: float
    price: float
    ord_type: str
    timestamp: datetime
    trade_id: str | None = None


@dataclass
class ExecutionFeeBreakdown:
    """Fee breakdown for a single asset in an execution."""

    asset: str
    quantity: float


@dataclass
class OrderFillSummary:
    """Venue-true aggregate over an order's own fills.

    Fill-gap reconciliation's price AND fee source for venues whose
    order snapshots lack them: ``vwap`` over the fills the venue
    returned, the ``covered_qty`` those fills span (a partial fills
    page must never price or fee the whole gap), and the summed
    ``fee_total`` in ``fee_currency`` when every fill reported a
    parseable single-currency fee (both None otherwise — an honest
    absence beats a partial sum).
    """

    vwap: float
    covered_qty: float
    fee_total: float | None = None
    fee_currency: str | None = None


@dataclass
class ExecutionUpdate:
    """Real-time execution update for an order."""

    order_id: str
    exec_type: ExecType | None
    symbol: str
    side: OrderSideEnum
    order_type: ExchangeOrderTypeEnum
    order_status: ExchangeOrderStatusEnum
    timestamp: datetime
    cum_qty: float | None = None
    cum_cost: float | None = None
    order_userref: int | None = None
    exec_id: str | None = None
    trade_id: int | None = None
    last_qty: float | None = None
    last_price: float | None = None
    liquidity_ind: LiquidityIndicator | None = None
    cost: float | None = None
    average_price: float | None = None
    fee_usd_equiv: float | None = None
    fees: list[ExecutionFeeBreakdown] | None = None
    cum_fee: float | None = None
    cum_fee_currency: str | None = None
    order_qty: float | None = None
    limit_price: float | None = None
    cash_order_qty: float | None = None
    cl_ord_id: str | None = None
    margin: bool | None = None
    margin_borrow: bool | None = None
    post_only: bool | None = None
    reduce_only: bool | None = None
    time_in_force: TimeInForceEnum | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        """Normalize the instance after initialization."""
        object.__setattr__(self, "order_status", normalize_order_status(self.order_status))


@dataclass
class InstrumentPairDescriptor:
    """Descriptor for a trading instrument pair with precision and limits."""

    symbol: str
    base: str
    quote: str
    status: str
    qty_precision: int
    qty_increment: float
    qty_min: float
    price_precision: int
    price_increment: float
    cost_precision: int
    cost_min: float
    marginable: bool
    has_index: bool
    margin_initial: float | None = None
    position_limit_long: int | None = None
    position_limit_short: int | None = None
    tick_size: float | None = None


@dataclass(frozen=True)
class FundingRateSnapshot:
    """Funding or rollover rate fetched from an exchange.

    Attributes:
        symbol: Native symbol (e.g., ``BTC-USD-PERP``).
        exchange: Exchange name (lowercase).
        rate_type: Funding model identifier (``perpetual_funding``
            or ``spot_margin_rollover``).
        direction: Position direction the rate applies to
            (``long``, ``short``, or ``both``).
        rate: Per-boundary rate as a decimal fraction.
        effective_from: Exchange-side timestamp when this rate
            became effective.
        notional_asset: Quote/settlement currency for the rate.
        source: Provenance tag (``exchange_api``, ``exchange_ws``,
            ``exchange_docs``).
    """

    symbol: str
    exchange: str
    rate_type: str
    direction: str
    rate: float
    effective_from: datetime
    notional_asset: str
    source: str


@dataclass
class OpenPositionSnapshot:
    """Snapshot of an open position on a derivatives exchange."""

    symbol: str
    side: OrderSideEnum
    size: float
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    unrealized_funding: float
    timestamp: datetime
