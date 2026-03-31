"""Core type aliases for the Snapper trading platform.

This module defines fundamental type aliases used throughout the application
for type-safe trading operations. All types are Literal-based for strict
compile-time and runtime validation.

Type Aliases:
    TradeSide: Direction of a trade (buy/sell).
    OrderType: Type of order to place.
    OrderStatus: Current state of an order in its lifecycle.
    FillStatus: Result of an order fill attempt.
    ExecutionMode: Trading mode (live/paper).
    OrderExchange: Order-capable exchanges.
    MarketSubscribeExchange: Live market feed exchanges.
    MarketDataExchange: Exchanges that produce market data (live or historical).
    AllExchange: All known exchange identifiers.
    HealthStatus: Component health state.
    ComponentStatus: Infrastructure component status.
    ProcessMode: Process execution mode (thread/process).
    StartProcessStatus: Start operation outcome.
    StopProcessStatus: Stop operation outcome.
    ProcessLifecycleType: Process duration type.
    ProcessRoleType: Process role in the system.
    ProcessRunStatusType: Current process execution state.
    IndicatorBackend: Technical indicator computation backend.
    UpsertResult: Outcome of an upsert operation.
    AssetType: Financial asset class category.
    MarketDataType: Market data type for ZMQ topic routing.
    SubscriptionAction: WebSocket subscription action.
    SubscriptionStatus: WebSocket subscription result status.
    SpawnerProcessStatus: Subprocess-level process status.
    AliasChannel: Symbol alias channel type.
    OrderCommand: Order command types for ZMQ topics.
    OrderEvent: Order event types for ZMQ topics.
"""

from enum import StrEnum
from typing import Literal


class ExchangeEnum(StrEnum):
    """All known exchange identifiers across all domains."""

    PAPER = "paper"
    KRAKEN = "kraken"
    KRAKEN_FUTURES = "kraken_futures"
    ZONDA = "zonda"
    WALUTOMAT = "walutomat"
    POLYGON = "polygon"


class AssetTypeEnum(StrEnum):
    """Financial asset class category for symbol catalog entries."""

    CRYPTO = "crypto"
    FOREX = "forex"
    EQUITY = "equity"
    INDEX = "index"


class ExecutionModeEnum(StrEnum):
    """Trading mode: live for real money, paper for simulation."""

    LIVE = "live"
    PAPER = "paper"


class MarketDataTypeEnum(StrEnum):
    """Market data type for ZMQ topic routing."""

    TICKS = "ticks"
    TRADES = "trades"
    CANDLES = "candles"


class AliasChannelEnum(StrEnum):
    """Symbol alias channel type: ws for WebSocket, rest for REST API, ccxt for CCXT library."""

    WS = "ws"
    REST = "rest"
    CCXT = "ccxt"


class TradeSideEnum(StrEnum):
    """Direction of a trade: buy for long entry, sell for short/exit."""

    BUY = "buy"
    SELL = "sell"


class OrderCommandEnum(StrEnum):
    """Order command types for orders.commands.* ZMQ topics."""

    SUBMIT = "submit"
    CANCEL = "cancel"
    REPLACE = "replace"


class HealthStatusEnum(StrEnum):
    """Component health state for monitoring and alerting."""

    HEALTHY = "healthy"
    WARNING = "warning"
    ERROR = "error"


class ProcessModeEnum(StrEnum):
    """Process execution mode: thread for in-process, process for subprocess."""

    THREAD = "thread"
    PROCESS = "process"


class ProcessLifecycleEnum(StrEnum):
    """Process lifecycle type.

    Defines whether a process is designed to run continuously
    or execute once and terminate.
    """

    LONG_RUNNING = "long_running"
    ONE_SHOT = "one_shot"


class ProcessRoleEnum(StrEnum):
    """Process role in the system.

    Categorizes processes by their function: core services,
    maintenance tasks, trading strategies, or backtests.
    """

    CORE = "core"
    TASK = "task"
    STRATEGY = "strategy"
    BACKTEST = "backtest"


class ProcessRunStatusEnum(StrEnum):
    """Current execution state of a managed process."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TradeSide = Literal[TradeSideEnum.BUY, TradeSideEnum.SELL]
"""Direction of a trade: 'buy' for long entry, 'sell' for short/exit."""

OrderType = Literal["market", "limit", "stop", "stop_limit"]
"""Order execution type determining how the order is matched."""

OrderStatus = Literal[
    "new", "submitted", "open", "filled", "partially_filled", "cancelled", "rejected"
]
"""Order lifecycle state from creation through completion or cancellation."""

FillStatus = Literal["filled", "partial"]
"""Result of an order execution: 'filled' for complete, 'partial' for ongoing."""

CancelEventType = Literal["cancelled", "rejected"]
"""Event types for cancel command responses."""

ReplaceEventType = Literal["replaced", "rejected"]
"""Event types for replace command responses."""

OrderEventType = Literal["submitted", "accepted", "rejected", "cancelled", "expired", "replaced"]
"""Order event type for ZMQ topic suffix (non-execution events).

This type MUST match the suffix of the orders.events.{exchange}.{instrument}.{event}
topic for non-execution events. Used by:
- OrderData.status: Full order lifecycle events (submit flow).
- OrderEventData.event: Lightweight cancel/replace confirmations.

Note: 'executed' is NOT in this type. Execution events use ExecutionData (with FillStatus),
not OrderData. This separation ensures clear payload types:
- orders.events.*.*.executed -> ExecutionData
- orders.events.*.*.{submitted|accepted|rejected|expired} -> OrderData
- orders.events.*.*.{cancelled|replaced|rejected} -> OrderEventData

The 'rejected' event may come from either data type:
- OrderData: Submit rejected by executor validation or exchange.
- OrderEventData: Cancel/replace rejected.

Events:
    submitted: Executor accepted command, order sent to exchange (local event).
    accepted: Exchange confirmed order receipt (exchange ACK).
    rejected: Order/cancel/replace was rejected by executor or exchange.
    cancelled: Order was successfully cancelled.
    expired: Order expired due to time-in-force constraint.
    replaced: Order was modified/replaced (cancel + new order).
"""

ExecutionMode = Literal[ExecutionModeEnum.LIVE, ExecutionModeEnum.PAPER]
"""Trading mode: 'live' for real money, 'paper' for simulation."""

OrderExchange = Literal[
    ExchangeEnum.PAPER, ExchangeEnum.KRAKEN, ExchangeEnum.ZONDA, ExchangeEnum.WALUTOMAT
]
"""Exchanges capable of order execution (paper simulator + live venues)."""

MarketSubscribeExchange = Literal[
    ExchangeEnum.KRAKEN, ExchangeEnum.KRAKEN_FUTURES, ExchangeEnum.ZONDA, ExchangeEnum.WALUTOMAT
]
"""Live market feed exchanges (no paper — paper replays from these)."""

MarketDataExchange = Literal[
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.ZONDA,
    ExchangeEnum.WALUTOMAT,
    ExchangeEnum.POLYGON,
]
"""Exchanges that produce market data, live or historical."""

AllExchange = Literal[
    ExchangeEnum.PAPER,
    ExchangeEnum.KRAKEN,
    ExchangeEnum.KRAKEN_FUTURES,
    ExchangeEnum.ZONDA,
    ExchangeEnum.WALUTOMAT,
    ExchangeEnum.POLYGON,
]
"""All known exchange identifiers across all domains."""

HealthStatus = Literal[HealthStatusEnum.HEALTHY, HealthStatusEnum.WARNING, HealthStatusEnum.ERROR]
"""Component health state for monitoring and alerting."""

ComponentStatus = Literal["ok", "error"]
"""Infrastructure component status for ZMQ and WebSocket health checks."""

ProcessMode = Literal[ProcessModeEnum.THREAD, ProcessModeEnum.PROCESS]
"""Process execution mode: 'thread' for in-process, 'process' for subprocess."""

StartProcessStatus = Literal["success", "already_running", "error"]
"""Start operation outcome: success, already running, or error."""

StopProcessStatus = Literal["success", "not_running", "error"]
"""Stop operation outcome: success, not running, or error."""

ProcessLifecycleType = Literal[ProcessLifecycleEnum.LONG_RUNNING, ProcessLifecycleEnum.ONE_SHOT]
"""Process duration: 'long_running' for services, 'one_shot' for tasks."""

ProcessRoleType = Literal[
    ProcessRoleEnum.CORE, ProcessRoleEnum.TASK, ProcessRoleEnum.STRATEGY, ProcessRoleEnum.BACKTEST
]
"""Process role determining its function in the trading system."""

ProcessRunStatusType = Literal[
    ProcessRunStatusEnum.RUNNING,
    ProcessRunStatusEnum.SUCCEEDED,
    ProcessRunStatusEnum.FAILED,
    ProcessRunStatusEnum.CANCELLED,
]
"""Current execution state of a managed process."""

IndicatorBackend = Literal["talib", "python"]
"""Technical indicator computation backend: 'talib' for TA-Lib C library, 'python' for pure Python."""

UpsertResult = Literal["created", "updated", "unchanged"]
"""Outcome of an upsert operation: row was created, updated, or left unchanged."""

AssetType = Literal[
    AssetTypeEnum.CRYPTO, AssetTypeEnum.FOREX, AssetTypeEnum.EQUITY, AssetTypeEnum.INDEX
]
"""Financial asset class category for symbol catalog entries."""

MarketDataType = Literal[
    MarketDataTypeEnum.TICKS, MarketDataTypeEnum.TRADES, MarketDataTypeEnum.CANDLES
]
"""Market data type for ZMQ topic routing (market.{exchange}.{instrument}.{type})."""

SubscriptionAction = Literal["subscribe", "unsubscribe"]
"""WebSocket subscription action: subscribe to or unsubscribe from topics."""

SubscriptionStatus = Literal["subscribed", "unsubscribed", "partial", "denied", "no_topics"]
"""WebSocket subscription result status indicating the outcome of a subscription request."""

SpawnerProcessStatus = Literal["not_running", "running", "stopped", "completed", "error"]
"""Subprocess-level process status for spawner-managed processes."""

AliasChannel = Literal[AliasChannelEnum.WS, AliasChannelEnum.REST, AliasChannelEnum.CCXT]
"""Symbol alias channel type: ws for WebSocket, rest for REST API, ccxt for CCXT library."""

OrderCommand = Literal[OrderCommandEnum.SUBMIT, OrderCommandEnum.CANCEL, OrderCommandEnum.REPLACE]
"""Order command types for orders.commands.* ZMQ topics."""

OrderEvent = Literal[
    "submitted", "accepted", "rejected", "executed", "cancelled", "expired", "replaced"
]
"""Order event types for orders.events.* ZMQ topics."""

__all__ = [
    "AliasChannel",
    "AliasChannelEnum",
    "AllExchange",
    "AssetType",
    "AssetTypeEnum",
    "ComponentStatus",
    "ExchangeEnum",
    "ExecutionMode",
    "ExecutionModeEnum",
    "FillStatus",
    "HealthStatus",
    "HealthStatusEnum",
    "IndicatorBackend",
    "MarketDataExchange",
    "MarketDataType",
    "MarketDataTypeEnum",
    "MarketSubscribeExchange",
    "OrderCommand",
    "OrderCommandEnum",
    "OrderEvent",
    "OrderEventType",
    "OrderExchange",
    "OrderStatus",
    "OrderType",
    "ProcessLifecycleEnum",
    "ProcessLifecycleType",
    "ProcessMode",
    "ProcessModeEnum",
    "ProcessRoleEnum",
    "ProcessRoleType",
    "ProcessRunStatusEnum",
    "ProcessRunStatusType",
    "SpawnerProcessStatus",
    "StartProcessStatus",
    "StopProcessStatus",
    "SubscriptionAction",
    "SubscriptionStatus",
    "TradeSide",
    "TradeSideEnum",
    "UpsertResult",
]
