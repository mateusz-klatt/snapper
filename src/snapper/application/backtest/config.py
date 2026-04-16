"""Backtest configuration and fingerprinting.

Defines the configuration schema for launching backtests and a
deterministic fingerprint function for dedup/cache-hit detection.
"""

import hashlib
import json
from datetime import datetime
from enum import StrEnum
from typing import Any

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
    cancel_poll_ms: int = 500

    @field_validator("cancel_poll_ms")
    @classmethod
    def validate_cancel_poll_ms(cls, v: int) -> int:
        """Cancel poll interval must be positive and bounded.

        Args:
            v: Poll interval in milliseconds.

        Returns:
            Validated poll interval.
        """
        if v <= 0 or v > 60_000:
            raise ValueError("cancel_poll_ms must be in (0, 60000]")
        return v

    @field_validator("slippage_bps", "commission_bps")
    @classmethod
    def validate_bps_bounds(cls, v: float) -> float:
        """Basis-point fields must be in [0, 500].

        Args:
            v: Basis-point value.

        Returns:
            Validated value.
        """
        if v < 0 or v > 500:
            raise ValueError("bps fields must be in [0, 500]")
        return v

    @field_validator("initial_balance")
    @classmethod
    def validate_initial_balance(cls, v: float) -> float:
        """Initial balance must be positive.

        Args:
            v: Balance value to validate.

        Returns:
            Validated balance.
        """
        if v <= 0:
            raise ValueError("initial_balance must be positive")
        return v

    @field_validator("strategy_class")
    @classmethod
    def validate_strategy_class(cls, v: str) -> str:
        """Validate strategy_class is registered.

        Args:
            v: Strategy class name.

        Returns:
            Validated strategy class name.
        """
        if v not in StrategyFactory.STRATEGY_CLASSES:
            available = ", ".join(sorted(StrategyFactory.STRATEGY_CLASSES.keys()))
            raise ValueError(
                f"Unknown strategy class '{v}'. Available: {available or 'none registered'}"
            )
        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v: datetime, info: object) -> datetime:
        """Validate end_date is after start_date.

        Args:
            v: End date to validate.
            info: Pydantic validation info with prior field values.

        Returns:
            Validated end date.
        """
        data = getattr(info, "data", {})
        start = data.get("start_date")
        if start is not None and v <= start:
            raise ValueError("end_date must be after start_date")
        return v

    @field_validator("instruments")
    @classmethod
    def validate_instruments_non_empty(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        """Validate at least one exchange with one instrument.

        Args:
            v: Instruments dict to validate.

        Returns:
            Validated instruments dict.
        """
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
    *,
    for_pairing: bool = False,
) -> str:
    """Compute deterministic SHA-256 fingerprint for a backtest configuration.

    Used for dedup detection (``for_pairing=False``, the default) and
    for Phase 2c auto-pair comparison (``for_pairing=True``).

    When ``for_pairing=True``, fields that do NOT affect
    comparability are excluded from the serialised payload so two
    runs with the same business config but different execution modes
    or ephemeral knobs hash to the same value:

    - ``execution_mode`` — Direct-DB and ZMQ replay are considered
      comparable at the engine-output level.
    - ``snapshot_as_of`` — per-invocation noise, not a config
      attribute.
    - ``warmup_bars`` / ``buffer_size`` — ephemeral engine knobs.

    ``fill_model``, ``slippage_bps``, and ``commission_bps`` ARE
    included in the pairing hash (R11 sonnet fix) because runs that
    differ only in those three fields are NOT engine-comparable.

    Default path (``for_pairing=False``) preserves the existing
    byte-for-byte fingerprint semantics — every existing caller
    (run provenance, dedup cache, tests) keeps its exact hash.

    Args:
        config: Backtest configuration.
        snapshot_as_of: Data snapshot timestamp (None = latest).
        warmup_bars: Number of warm-up candle bars.
        buffer_size: Additional buffer bars.
        for_pairing: Phase 2c flag — when True, produce the
            pairing-stable hash by omitting execution_mode +
            snapshot_as_of + warmup_bars + buffer_size.

    Returns:
        Hex SHA-256 digest.
    """
    normalized_instruments = {k: sorted(v) for k, v in sorted(config.instruments.items())}
    payload: dict[str, Any] = {
        "strategy_class": config.strategy_class,
        "instruments": normalized_instruments,
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "initial_balance": config.initial_balance,
        "strategy_params": config.strategy_params,
        "timeframe": config.timeframe,
        "fill_model": config.fill_model,
        "slippage_bps": config.slippage_bps,
        "commission_bps": config.commission_bps,
    }
    if not for_pairing:
        payload["execution_mode"] = config.execution_mode
        payload["snapshot_as_of"] = snapshot_as_of.isoformat() if snapshot_as_of else None
        payload["warmup_bars"] = warmup_bars
        payload["buffer_size"] = buffer_size
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
