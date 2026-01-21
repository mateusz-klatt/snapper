"""Trading engine configuration module.

This module provides configuration data structures for the trading engine,
including initial capital, fee settings, and other trading parameters.
"""

from dataclasses import dataclass


@dataclass
class EngineConfigModel:
    """Configuration for individual trading engine instances.

    Defines the initial capital and fee structure for trading operations.
    Each engine instance (one per symbol) uses this configuration to
    manage position sizing and calculate trading costs.

    Attributes:
        initial_cash: Starting capital for the engine in quote currency.
            Defaults to 10,000.
        fee_bps: Trading fee in basis points (1 bps = 0.01%).
            Applied to both buy and sell orders. Defaults to 2.0 bps.

    Example:
        >>> config = EngineConfigModel(initial_cash=50_000, fee_bps=5.0)
        >>> config.fee_bps / 10_000  # Convert to decimal
        0.0005
    """

    initial_cash: float = 10_000.0
    fee_bps: float = 2.0
