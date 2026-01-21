"""RSI reversion strategy.

This module implements a mean-reversion strategy based on the
Relative Strength Index (RSI) indicator.
"""

import pandas as pd
from loguru import logger

from snapper.indicators.ta_lib_adapter import rsi
from snapper.messaging.schemas.messages import BarEnvelope
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import Signal
from snapper.strategies.base import StrategyConfig
from snapper.strategies.decorators import create_strategy_process
from snapper.strategies.decorators import register_strategy


@register_strategy("RSIReversion")
@create_strategy_process(
    process_name="strategy_rsi_eth_1h",
    default_config={
        "name": "rsi_eth_1h",
        "inputs": ["market.kraken.ETH-USD.candles.1h"],
        "outputs": ["ETH-USD"],
        "exchange": "paper",
        "params": {
            "period": 14,
            "upper": 70.0,
            "lower": 30.0,
            "cooldown": 2,
        },
    },
)
class RSIReversion(BaseStrategy):
    """RSI-based mean reversion strategy.

    Generates buy signals when RSI crosses below the lower threshold
    and sell signals when RSI crosses above the upper threshold.

    Attributes:
        period: RSI calculation period.
        upper: Overbought threshold (sell signal).
        lower: Oversold threshold (buy signal).
        cooldown: Bars to wait between signals.
    """

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize RSI strategy.

        Args:
            config: Strategy configuration.
        """
        super().__init__(config)
        self.period = self.params.get("period", 14)
        self.upper = self.params.get("upper", 70.0)
        self.lower = self.params.get("lower", 30.0)
        self.cooldown = self.params.get("cooldown", 0)
        self._cool: dict[str, int] = {}

    async def on_bar(self, instrument: str, bar: BarEnvelope) -> Signal | None:
        """Process incoming bar and generate signal if conditions met.

        Args:
            instrument: The instrument symbol.
            bar: The bar envelope with OHLCV data.

        Returns:
            Buy signal if RSI <= lower, sell signal if RSI >= upper,
            or None if no signal conditions met.
        """
        closes = pd.Series([b.close for b in self.candle_buffer.get(instrument, [])]).astype(float)
        if len(closes) < self.period:
            return None
        current_price = float(closes.iloc[-1])
        r_series = rsi(closes, self.period)
        r = float(r_series.iloc[-1])
        r_prev = float(r_series.iloc[-2]) if len(r_series) >= 2 else r
        c_last = float(closes.iloc[-1])
        c_prev = float(closes.iloc[-2]) if len(closes) >= 2 else c_last
        if instrument not in self._cool:
            self._cool[instrument] = 0
        if self._cool[instrument] > 0:
            self._cool[instrument] -= 1
            return None
        if r <= self.lower or (r_prev <= self.lower and c_last > c_prev):
            self._cool[instrument] = self.cooldown
            return Signal(
                instrument=instrument,
                side="buy",
                strength=1.0,
                price=current_price,
                reason=f"RSI {r:.2f} <= {self.lower}",
                metadata={
                    "period": self.period,
                    "rsi_value": r,
                    "rsi_prev": r_prev,
                    "upper": self.upper,
                    "lower": self.lower,
                },
            )
        if r >= self.upper or (r_prev >= self.upper and c_last < c_prev):
            self._cool[instrument] = self.cooldown
            return Signal(
                instrument=instrument,
                side="sell",
                strength=1.0,
                price=current_price,
                reason=f"RSI {r:.2f} >= {self.upper}",
                metadata={
                    "period": self.period,
                    "rsi_value": r,
                    "rsi_prev": r_prev,
                    "upper": self.upper,
                    "lower": self.lower,
                },
            )
        return None

    async def reset(self) -> None:
        """Reset strategy state for replay."""
        self._cool.clear()
        logger.info(f"Strategy {self.name} state reset for replay")
