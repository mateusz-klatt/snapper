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

FillStatus = Literal["filled", "partial", "rejected", "cancelled"]
"""Result of an order execution attempt."""

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
    "FillStatus",
    "ExecutionMode",
    "TradingExchange",
    "HealthStatus",
    "ProcessLifecycleType",
    "ProcessRoleType",
    "ProcessRunStatusType",
]
