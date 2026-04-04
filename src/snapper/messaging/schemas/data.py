"""Market data, trading event, and system message schemas for ZMQ messaging.

This module defines Pydantic models representing all entities that flow through
the ZMQ messaging bus. Each entity inherits StrictDataSchema which provides id
(UUID7), type (Literal discriminator), and timestamp (bus creation time).

These schemas serve as the single source of truth for both ZMQ transport and
REST API responses. Domain-specific timestamps (open_at, fired_at, executed_at,
created_at) are separate from the bus timestamp.

Market data classes:
    TickData: Real-time bid/ask/last price snapshot.
    CandleData: OHLCV candle data for a specific timeframe.
    TradeData: Individual trade execution data.

Trading event classes:
    SignalData: Trading signal with direction and strength.
    ExecutionData: Order fill/execution details.
    OrderData: Current order state and fill progress.
    PositionData: Portfolio position snapshot.

Order command classes:
    OrderRequestData: Order submission request.
    OrderCancelData: Order cancellation request.
    OrderReplaceData: Order modification request.
    OrderEventData: Lightweight order lifecycle event.

System message classes:
    HeartbeatData: Component health heartbeat.
    SettingChangedData: Configuration change notification.
    SymbolAliasUpdateData: Symbol alias cache invalidation.
    ReplayStartData: Historical data replay start marker.
    ReplayEndData: Historical data replay end marker.
"""

from datetime import datetime
from typing import Literal
from typing import Self

from pydantic import Field
from pydantic import model_validator

from snapper.api.schemas.base import StrictDataSchema
from snapper.core.json_types import JsonObject
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import OrderEventType
from snapper.core.types import OrderExchange
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.interface.websocket.schemas import FillStatus
from snapper.interface.websocket.schemas import HealthStatus
from snapper.interface.websocket.schemas import OrderType
from snapper.interface.websocket.schemas import TradeSide


class TickData(StrictDataSchema[Literal["tick"]]):
    """Real-time price tick snapshot from an exchange.

    Represents a point-in-time snapshot of bid/ask prices and last trade.
    Used for real-time price monitoring and spread calculations.

    Attributes:
        instrument: Trading pair symbol (e.g., 'BTC-USD').
        exchange: Source exchange producing this tick data.
        volume: Trading volume for the current period.
        bid: Best bid price (highest buy order).
        ask: Best ask price (lowest sell order).
        last: Last traded price.
    """

    type: Literal["tick"] = "tick"
    instrument: str
    exchange: MarketDataExchange
    volume: float
    bid: float | None = None
    ask: float | None = None
    last: float | None = None


class CandleData(StrictDataSchema[Literal["candle"]]):
    """OHLCV candlestick data for technical analysis.

    Represents aggregated price action over a specific timeframe.
    Used by strategies for pattern recognition and indicator calculation.

    Attributes:
        instrument: Trading pair symbol (e.g., 'BTC-USD').
        exchange: Source exchange producing this candle data.
        timeframe: Candle duration (e.g., '1m', '1h', '1d').
        open_at: Exchange-provided candle interval start time.
        open: Opening price of the candle.
        high: Highest price during the candle.
        low: Lowest price during the candle.
        close: Closing price of the candle.
        volume: Total traded volume during the candle.
        vwap: Volume-weighted average price (optional).
        trades: Number of trades in the candle (optional).
    """

    type: Literal["candle"] = "candle"
    instrument: str
    exchange: MarketDataExchange
    timeframe: str
    open_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    trades: int | None = None


class TradeData(StrictDataSchema[Literal["trade"]]):
    """Individual trade execution from the market.

    Represents a single trade that occurred on the exchange.
    Used for trade tape analysis and market activity monitoring.

    Attributes:
        instrument: Trading pair symbol (e.g., 'BTC-USD').
        exchange: Source exchange where the trade occurred.
        executed_at: Exchange-provided trade execution timestamp.
        price: Execution price of the trade.
        volume: Size of the trade.
        side: Trade direction ('buy'/'sell') if available.
        trade_id: Exchange-provided trade identifier for deduplication.
    """

    type: Literal["trade"] = "trade"
    instrument: str
    exchange: MarketDataExchange
    executed_at: datetime | None = None
    price: float
    volume: float
    side: str | None = None
    trade_id: str | None = None


class SignalData(StrictDataSchema[Literal["signal"]]):
    """Trading signal generated by a strategy.

    Represents a recommendation to enter or exit a position.
    Signals are published to the messaging bus for execution.

    Attributes:
        instrument: Target trading pair symbol.
        exchange: Target exchange for execution.
        side: Recommended direction ('buy' or 'sell').
        strength: Signal confidence from 0.0 (weak) to 1.0 (strong).
        reason: Human-readable explanation for the signal.
        price: Suggested entry/exit price (optional).
        strategy_name: Name of the generating strategy (optional).
        fired_at: Domain timestamp when the signal was generated.
    """

    type: Literal["signal"] = "signal"
    instrument: str
    exchange: OrderExchange
    side: TradeSide
    strength: float = Field(ge=0.0, le=1.0)
    reason: str
    price: float | None = None
    strategy_name: str | None = None
    fired_at: datetime

    @model_validator(mode="after")
    def _paper_requires_strategy_name(self) -> Self:
        """Paper signals require strategy_name for topic derivation."""
        if self.exchange == ExchangeEnum.PAPER and not self.strategy_name:
            raise ValueError(
                "Paper signals require strategy_name to be set "
                "for topic derivation (signals.paper.{instrument}.{strategy_name})"
            )
        return self


class ExecutionData(StrictDataSchema[Literal["execution"]]):
    """Order fill/execution details from an exchange.

    Represents a completed or partial fill of an order.
    Contains all information needed for trade tracking and P&L calculation.

    Attributes:
        trade_id: Unique fill/trade ID from exchange (e.g., Kraken exec_id).
            May be None for exchanges that don't provide it.
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken txid).
            May be None if exchange hasn't assigned an ID yet.
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
        instrument: Trading pair symbol.
        exchange: Exchange where the fill occurred.
        side: Trade direction ('buy' or 'sell').
        size: Cumulative filled quantity across all fills for the order.
        price: Cumulative average execution price across all fills.
        last_size: Incremental quantity filled by this execution event (delta).
        last_price: Price of the incremental fill (delta).
        fee: Transaction fee charged.
        fee_asset: Currency of the fee (e.g., 'USD', 'BTC').
        status: Fill status ('filled', 'partial', etc.).
        executed_at: Timestamp of the fill.
    """

    type: Literal["execution"] = "execution"
    trade_id: str | None = None
    exchange_order_id: str | None = None
    client_order_id: str
    instrument: str
    exchange: OrderExchange
    side: TradeSide
    size: float
    price: float
    last_size: float
    last_price: float
    fee: float
    fee_asset: str
    status: FillStatus
    executed_at: datetime


class OrderData(StrictDataSchema[Literal["order"]]):
    """Current state of an order.

    Used for both ZMQ event publishing and REST API responses.
    Published on orders.events.{exchange}.{instrument}.{status} topics.

    INVARIANT: The 'status' field MUST match the topic suffix.

    Attributes:
        exchange_order_id: Exchange-assigned order ID (e.g., Kraken txid).
            May be None before exchange ACK (e.g., for 'submitted' event).
        client_order_id: Our generated order ID (e.g., 'signal-a1b2c3d4').
        instrument: Trading pair symbol.
        exchange: Exchange where the order is placed.
        side: Order direction ('buy' or 'sell').
        status: Event type matching topic suffix (OrderEventType, excludes 'execution').
        order_type: Type of order ('market', 'limit', etc.).
        size: Total order size.
        filled_size: Amount filled so far.
        price: Limit price (for limit orders).
        average_price: Average fill price (for partial fills).
        reason: Optional rejection/failure reason (for 'rejected' status).
        time_in_force: Order time-in-force setting.
        error: Error message if order failed.
        created_at: Order creation timestamp.
        updated_at: Last status update timestamp.
    """

    type: Literal["order"] = "order"
    exchange_order_id: str | None = None
    client_order_id: str
    instrument: str
    exchange: OrderExchange
    mode: ExecutionMode = ExecutionModeEnum.LIVE
    side: TradeSide
    status: str
    order_type: OrderType
    size: float
    filled_size: float
    price: float | None = None
    average_price: float | None = None
    reason: str | None = None
    time_in_force: str | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime | None = None


class PositionData(StrictDataSchema[Literal["position"]]):
    """Portfolio position snapshot.

    Represents a single position in the portfolio.
    Used for both ZMQ event publishing and REST API responses.
    The inherited ``timestamp`` field carries the last-update time.

    Attributes:
        instrument: Trading pair symbol.
        exchange: Exchange where the position is held.
        quantity: Position size (positive for long, negative for short).
        average_price: Average entry price.
        unrealized_pnl: Unrealized profit/loss.
        realized_pnl: Realized profit/loss.
    """

    type: Literal["position"] = "position"
    instrument: str
    exchange: OrderExchange
    mode: ExecutionMode = ExecutionModeEnum.LIVE
    quantity: float
    average_price: float
    unrealized_pnl: float
    realized_pnl: float


class OrderRequestData(StrictDataSchema[Literal["order_request"]]):
    """Order request from strategy to executor.

    Sent by strategies to request order placement on an exchange.
    Contains all information needed for order creation.
    Published on: orders.commands.{exchange}.{instrument}.submit

    Attributes:
        strategy_id: Identifier of the requesting strategy.
        exchange: Target exchange for the order.
        instrument: Trading pair symbol.
        mode: Execution mode ('live' or 'paper').
        side: Order direction ('buy' or 'sell').
        order_type: Type of order ('market', 'limit', etc.).
        quantity: Order size (must be positive).
        price: Limit price (required for limit orders).
        client_order_id: Client-side order identifier.
        signaled_at: Original signal timestamp (optional).
    """

    type: Literal["order_request"] = "order_request"
    strategy_id: str
    exchange: OrderExchange
    instrument: str
    mode: ExecutionMode
    side: TradeSide
    order_type: OrderType
    quantity: float = Field(gt=0)
    price: float | None = None
    client_order_id: str
    signaled_at: datetime | None = None
    strategy_tag: str | None = None


class OrderCancelData(StrictDataSchema[Literal["order_cancel"]]):
    """Order cancel request from strategy to executor.

    Sent to request cancellation of an existing order.
    Published on: orders.commands.{exchange}.{instrument}.cancel

    Attributes:
        exchange: Target exchange for the cancel.
        instrument: Trading pair symbol.
        exchange_order_id: Exchange-assigned order ID.
        client_order_id: Our generated order ID.
    """

    type: Literal["order_cancel"] = "order_cancel"
    exchange: OrderExchange
    instrument: str
    exchange_order_id: str
    client_order_id: str


class OrderReplaceData(StrictDataSchema[Literal["order_replace"]]):
    """Order replace/modify request from strategy to executor.

    Sent to request modification of an existing order (price/quantity).
    Published on: orders.commands.{exchange}.{instrument}.replace

    Attributes:
        exchange: Target exchange for the replace.
        instrument: Trading pair symbol.
        exchange_order_id: Exchange-assigned order ID.
        client_order_id: Our generated order ID.
        new_quantity: New order quantity (optional).
        new_price: New limit price (optional).
    """

    type: Literal["order_replace"] = "order_replace"
    exchange: OrderExchange
    instrument: str
    exchange_order_id: str
    client_order_id: str
    new_quantity: float | None = None
    new_price: float | None = None


class OrderEventData(StrictDataSchema[Literal["order_event"]]):
    """Lightweight order event for cancel/replace confirmations.

    Used for publishing order lifecycle events that don't require full order
    details. Preferred for cancel/replace results because those commands
    don't carry side/order_type information.

    Published on: orders.events.{exchange}.{instrument}.{event}

    INVARIANT: The 'event' field MUST match the topic suffix.

    Attributes:
        exchange_order_id: Exchange-assigned order ID.
        client_order_id: Our generated order ID.
        exchange: Exchange where the order exists.
        instrument: Trading pair symbol.
        event: Event type matching topic suffix (OrderEventType).
        reason: Optional rejection/cancellation reason.
    """

    type: Literal["order_event"] = "order_event"
    exchange_order_id: str
    client_order_id: str
    exchange: OrderExchange
    instrument: str
    event: OrderEventType
    reason: str | None = None


class HeartbeatData(StrictDataSchema[Literal["heartbeat"]]):
    """Component health heartbeat message.

    Published periodically by components to indicate they are alive.
    Used for health monitoring and dead component detection.

    Attributes:
        component: Name of the sending component.
        sequence: Domain-level heartbeat generation count (not transport sequence_id).
        status: Current health status.
        lag_ms: Processing lag in milliseconds.
        meta: Optional metadata dictionary for extensions.
    """

    type: Literal["heartbeat"] = "heartbeat"
    component: str
    sequence: int
    status: HealthStatus
    lag_ms: int
    meta: JsonObject = Field(default={})


class SettingChangedData(StrictDataSchema[Literal["setting_changed"]]):
    """Configuration setting change notification.

    Published when a setting is modified in the database.
    Subscribers use this to invalidate caches or reload config.

    Attributes:
        key: Setting key that changed.
        value: New setting value.
        category: Setting category for grouping.
        updated_by: User who made the change (optional).
    """

    type: Literal["setting_changed"] = "setting_changed"
    key: str
    value: str
    category: str
    updated_by: str | None = None


class SymbolAliasUpdateData(StrictDataSchema[Literal["symbol_alias_update"]]):
    """Symbol alias cache invalidation message.

    Published when symbol aliases are updated in the database.
    Subscribers should clear their symbol mapper caches.

    Attributes:
        event: Event type (always 'symbol_aliases_updated').
        action: Required action (always 'clear_cache').
    """

    type: Literal["symbol_alias_update"] = "symbol_alias_update"
    event: Literal["symbol_aliases_updated"] = "symbol_aliases_updated"
    action: Literal["clear_cache"] = "clear_cache"


class ReplayStartData(StrictDataSchema[Literal["replay_start"]]):
    """Historical data replay start marker.

    Sent at the beginning of a historical data replay session.
    Strategies use this to reset state before receiving replayed data.

    Attributes:
        started_at: Replay start timestamp (optional).
    """

    type: Literal["replay_start"] = "replay_start"
    started_at: datetime | None = None


class ReplayEndData(StrictDataSchema[Literal["replay_end"]]):
    """Historical data replay end marker.

    Sent at the end of a historical data replay session.
    Strategies use this to finalize analysis and generate reports.
    """

    type: Literal["replay_end"] = "replay_end"


class UnderlyingAssetData(StrictDataSchema[Literal["underlying_asset"]]):
    """Underlying asset with instrument count.

    Provenance fields (public_id, session_id, sequence_id, timestamp)
    come from the DB row via UnderlyingAssetRow.

    Attributes:
        ticker: Short code (e.g. 'SPX', 'GOLD').
        name: Canonical name (e.g. 'S&P 500').
        asset_class: Asset type category.
        sector: Optional sector classification.
        instrument_count: Number of instruments mapped to this underlying.
    """

    type: Literal["underlying_asset"] = "underlying_asset"
    ticker: str
    name: str
    asset_class: str
    sector: str | None
    instrument_count: int


class UnderlyingInstrumentData(StrictDataSchema[Literal["underlying_instrument"]]):
    """Instrument mapped to an underlying asset.

    Provenance comes from the InstrumentUnderlyingMapping DB row.

    Attributes:
        instrument_public_id: Public ID of the instrument.
        native_symbol: Symbol as known on the exchange.
        exchange: Exchange identifier.
        asset_type: Asset type of the symbol.
        relationship_type: How instrument relates to underlying.
        contract_family: Futures product root (nullable).
    """

    type: Literal["underlying_instrument"] = "underlying_instrument"
    instrument_public_id: str
    native_symbol: str
    exchange: str
    asset_type: str
    relationship_type: str
    contract_family: str | None


class FrontMonthData(StrictDataSchema[Literal["front_month"]]):
    """Front-month futures contract for an underlying.

    Provenance is minted by the API handler (projection across
    multiple temporal tables, not a single DB row).

    Attributes:
        instrument_public_id: Public ID of the front-month instrument.
        native_symbol: Symbol as known on the exchange.
        exchange: Exchange identifier.
        expiry_at: Contract expiry timestamp (UTC).
        relationship_type: How instrument relates to underlying.
        contract_family: Futures product root (nullable).
    """

    type: Literal["front_month"] = "front_month"
    instrument_public_id: str
    native_symbol: str
    exchange: str
    expiry_at: datetime
    relationship_type: str
    contract_family: str | None


class ContractData(StrictDataSchema[Literal["contract"]]):
    """Futures contract in a contract ladder listing.

    Provenance is minted per item (same pattern as FrontMonthData).

    Attributes:
        instrument_public_id: Public ID of the instrument.
        native_symbol: Symbol as known on the exchange.
        exchange: Exchange identifier.
        expiry_at: Contract expiry timestamp (nullable for perpetuals).
        instrument_kind: Product type (future, perpetual, etc.).
        relationship_type: How instrument relates to underlying.
        contract_family: Futures product root (nullable).
        is_front_month: True if this is the nearest non-expired contract.
    """

    type: Literal["contract"] = "contract"
    instrument_public_id: str
    native_symbol: str
    exchange: str
    expiry_at: datetime | None
    instrument_kind: str | None
    relationship_type: str
    contract_family: str | None
    is_front_month: bool
