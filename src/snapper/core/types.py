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
    TradingExchange: Supported trading venues.
    HealthStatus: Component health state.
    ProcessLifecycleType: Process duration type.
    ProcessRoleType: Process role in the system.
    ProcessRunStatusType: Current process execution state.
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

TradingExchange = Literal["paper", "kraken", "zonda", "walutomat"]
"""Supported trading venues including paper trading simulator."""

HealthStatus = Literal["healthy", "warning", "error"]
"""Component health state for monitoring and alerting."""

ProcessLifecycleType = Literal["long_running", "one_shot"]
"""Process duration: 'long_running' for services, 'one_shot' for tasks."""

ProcessRoleType = Literal["core", "task", "strategy", "backtest"]
"""Process role determining its function in the trading system."""

ProcessRunStatusType = Literal["running", "succeeded", "failed", "cancelled"]
"""Current execution state of a managed process."""
__all__ = [
    "TradeSide",
    "OrderType",
    "OrderStatus",
    "OrderEventType",
    "FillStatus",
    "ExecutionMode",
    "TradingExchange",
    "HealthStatus",
    "ProcessLifecycleType",
    "ProcessRoleType",
    "ProcessRunStatusType",
]
