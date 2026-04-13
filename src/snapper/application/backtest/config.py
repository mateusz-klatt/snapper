"""Backtest configuration and fingerprinting.

Defines the configuration schema for launching backtests and a
deterministic fingerprint function for dedup/cache-hit detection.
"""

import hashlib
import json
from datetime import datetime
from enum import StrEnum

from pydantic import field_validator

from snapper.api.schemas.base import StrictBody
from snapper.core.json_types import JsonObject
from snapper.strategies.factory import StrategyFactory


class BacktestExecutionMode(StrEnum):
    """How candle data is fed to the strategy."""

    DIRECT_DB = "direct_db"
    ZMQ_REPLAY = "zmq_replay"


class BacktestFillModel(StrEnum):
    """Simulated fill model for backtest trades."""

    MARKET = "market"


class BacktestConfig(StrictBody):
    """Configuration for launching a backtest run.

    Attributes:
        strategy_class: Registered strategy class name.
        instruments: Map of exchange -> list of instruments.
        start_date: Backtest period start.
        end_date: Backtest period end.
        wallet_public_id: Multi-tenant scoping.
        operator_public_id: Optional operator scope.
        execution_mode: Candle feed mode (Phase 1: direct_db only).
        initial_balance: Starting cash balance.
        strategy_params: Strategy-specific parameters.
        timeframe: Candle timeframe (e.g., "1h", "15m").
        fill_model: Simulated fill model.
        slippage_bps: Slippage in basis points.
        commission_bps: Commission in basis points.
    """

    strategy_class: str
    instruments: dict[str, list[str]]
    start_date: datetime
    end_date: datetime
    wallet_public_id: str
    operator_public_id: str | None = None
    execution_mode: BacktestExecutionMode = BacktestExecutionMode.DIRECT_DB
    initial_balance: float = 10_000.0
    strategy_params: JsonObject = {}
    timeframe: str = "1h"
    fill_model: BacktestFillModel = BacktestFillModel.MARKET
    slippage_bps: float = 0.0
    commission_bps: float = 0.0

    @field_validator("execution_mode")
    @classmethod
    def validate_execution_mode_phase1(cls, v: BacktestExecutionMode) -> BacktestExecutionMode:
        """Phase 1 only supports DIRECT_DB."""
        if v != BacktestExecutionMode.DIRECT_DB:
            raise ValueError(f"Execution mode '{v}' not supported in Phase 1; use 'direct_db'")
        return v

    @field_validator("initial_balance")
    @classmethod
    def validate_initial_balance(cls, v: float) -> float:
        """Initial balance must be positive."""
        if v <= 0:
            raise ValueError("initial_balance must be positive")
        return v

    @field_validator("slippage_bps")
    @classmethod
    def validate_slippage_bps(cls, v: float) -> float:
        """Slippage must be non-negative."""
        if v < 0:
            raise ValueError("slippage_bps must be non-negative")
        return v

    @field_validator("commission_bps")
    @classmethod
    def validate_commission_bps(cls, v: float) -> float:
        """Commission must be non-negative."""
        if v < 0:
            raise ValueError("commission_bps must be non-negative")
        return v

    @field_validator("strategy_class")
    @classmethod
    def validate_strategy_class(cls, v: str) -> str:
        """Validate strategy_class is registered."""
        if v not in StrategyFactory.STRATEGY_CLASSES:
            available = ", ".join(sorted(StrategyFactory.STRATEGY_CLASSES.keys()))
            raise ValueError(
                f"Unknown strategy class '{v}'. Available: {available or 'none registered'}"
            )
        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v: datetime, info: object) -> datetime:
        """Validate end_date is after start_date."""
        data = getattr(info, "data", {})
        start = data.get("start_date")
        if start is not None and v <= start:
            raise ValueError("end_date must be after start_date")
        return v

    @field_validator("instruments")
    @classmethod
    def validate_instruments_non_empty(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        """Validate at least one exchange with one instrument."""
        if not v:
            raise ValueError("instruments must contain at least one exchange")
        for exchange, symbols in v.items():
            if not symbols:
                raise ValueError(f"Exchange '{exchange}' must have at least one instrument")
        return v


def compute_fingerprint(
    config: BacktestConfig,
    snapshot_as_of: datetime | None = None,
    warmup_bars: int = 0,
    buffer_size: int = 0,
) -> str:
    """Compute deterministic SHA-256 fingerprint for a backtest configuration.

    Used for dedup detection: same config + same data snapshot = same results.

    Args:
        config: Backtest configuration.
        snapshot_as_of: Data snapshot timestamp (None = latest).
        warmup_bars: Number of warm-up candle bars.
        buffer_size: Additional buffer bars.

    Returns:
        Hex SHA-256 digest.
    """
    normalized_instruments = {k: sorted(v) for k, v in sorted(config.instruments.items())}
    payload = {
        "strategy_class": config.strategy_class,
        "instruments": normalized_instruments,
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "execution_mode": config.execution_mode,
        "initial_balance": config.initial_balance,
        "strategy_params": config.strategy_params,
        "timeframe": config.timeframe,
        "fill_model": config.fill_model,
        "slippage_bps": config.slippage_bps,
        "commission_bps": config.commission_bps,
        "snapshot_as_of": snapshot_as_of.isoformat() if snapshot_as_of else None,
        "warmup_bars": warmup_bars,
        "buffer_size": buffer_size,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
