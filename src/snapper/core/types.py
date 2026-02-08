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
    TradingExchange: Alias for OrderExchange (backward compat).
    MarketSubscribeExchange: Live market feed exchanges.
    ReplaySourceExchange: Valid paper replay source exchanges.
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
    MarketDataType: Type of market data in ZMQ topics.
    SubscriptionAction: WebSocket subscription action.
    SubscriptionStatus: WebSocket subscription result status.
"""

from typing import Literal

TradeSide = Literal["buy", "sell"]
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
"""Order event type for ZMQ topic suffix (non-fill events).

This type MUST match the suffix of the orders.events.{exchange}.{instrument}.{event}
topic for non-fill events. Used by:
- OrderStatusEnvelope.status: Full order lifecycle events (submit flow).
- OrderEventEnvelope.event: Lightweight cancel/replace confirmations.

Note: 'fill' is NOT in this type. Fill events use FillEnvelope (with FillStatus),
not OrderStatusEnvelope. This separation ensures clear payload types:
- orders.events.*.*.fill -> FillEnvelope
- orders.events.*.*.{submitted|accepted|rejected|expired} -> OrderStatusEnvelope
- orders.events.*.*.{cancelled|replaced|rejected} -> OrderEventEnvelope

The 'rejected' event may come from either envelope type:
- OrderStatusEnvelope: Submit rejected by executor validation or exchange.
- OrderEventEnvelope: Cancel/replace rejected.

Events:
    submitted: Executor accepted command, order sent to exchange (local event).
    accepted: Exchange confirmed order receipt (exchange ACK).
    rejected: Order/cancel/replace was rejected by executor or exchange.
    cancelled: Order was successfully cancelled.
    expired: Order expired due to time-in-force constraint.
    replaced: Order was modified/replaced (cancel + new order).
"""

ExecutionMode = Literal["live", "paper"]
"""Trading mode: 'live' for real money, 'paper' for simulation."""

OrderExchange = Literal["paper", "kraken", "zonda", "walutomat"]
"""Exchanges capable of order execution (paper simulator + live venues)."""

TradingExchange = OrderExchange
"""Alias for OrderExchange — backward compatibility during type split."""

MarketSubscribeExchange = Literal["kraken", "zonda", "walutomat"]
"""Live market feed exchanges (no paper — paper replays from these)."""

ReplaySourceExchange = Literal["kraken", "zonda", "walutomat", "polygon"]
"""Valid source exchanges for paper replay (paper excluded — it is the consumer, not source)."""

AllExchange = Literal["paper", "kraken", "zonda", "walutomat", "polygon"]
"""All known exchange identifiers across all domains."""

HealthStatus = Literal["healthy", "unhealthy", "warning", "error"]
"""Component health state for monitoring and alerting."""

ComponentStatus = Literal["ok", "error"]
"""Infrastructure component status for ZMQ and WebSocket health checks."""

ProcessMode = Literal["thread", "process"]
"""Process execution mode: 'thread' for in-process, 'process' for subprocess."""

StartProcessStatus = Literal["success", "already_running", "error"]
"""Start operation outcome: success, already running, or error."""

StopProcessStatus = Literal["success", "not_running", "error"]
"""Stop operation outcome: success, not running, or error."""

ProcessLifecycleType = Literal["long_running", "one_shot"]
"""Process duration: 'long_running' for services, 'one_shot' for tasks."""

ProcessRoleType = Literal["core", "task", "strategy", "backtest"]
"""Process role determining its function in the trading system."""

ProcessRunStatusType = Literal["running", "succeeded", "failed", "cancelled"]
"""Current execution state of a managed process."""

IndicatorBackend = Literal["talib", "python"]
"""Technical indicator computation backend: 'talib' for TA-Lib C library, 'python' for pure Python."""

UpsertResult = Literal["created", "updated", "unchanged"]
"""Outcome of an upsert operation: row was created, updated, or left unchanged."""

AssetType = Literal["crypto", "forex", "equity", "index"]
"""Financial asset class category for symbol catalog entries."""

MarketDataType = Literal["tick", "ticks", "trades", "book", "candles"]
"""Type of market data for ZMQ market.* topics."""

SubscriptionAction = Literal["subscribe", "unsubscribe"]
"""WebSocket subscription action: subscribe to or unsubscribe from topics."""

SubscriptionStatus = Literal["subscribed", "unsubscribed", "partial", "denied", "no_topics"]
"""WebSocket subscription result status indicating the outcome of a subscription request."""

__all__ = [
    "AllExchange",
    "AssetType",
    "ComponentStatus",
    "TradeSide",
    "OrderType",
    "OrderStatus",
    "OrderEventType",
    "OrderExchange",
    "FillStatus",
    "ExecutionMode",
    "MarketDataType",
    "MarketSubscribeExchange",
    "ProcessMode",
    "ReplaySourceExchange",
    "SubscriptionAction",
    "SubscriptionStatus",
    "TradingExchange",
    "HealthStatus",
    "IndicatorBackend",
    "ProcessLifecycleType",
    "ProcessRoleType",
    "ProcessRunStatusType",
    "StartProcessStatus",
    "StopProcessStatus",
    "UpsertResult",
]
