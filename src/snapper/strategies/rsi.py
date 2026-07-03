"""RSI reversion strategy.

This module implements a mean-reversion strategy based on the
Relative Strength Index (RSI) indicator.
"""

import pandas as pd
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.indicators.ta_lib_adapter import rsi
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.decorators import create_strategy_process
from snapper.strategies.decorators import register_strategy


@register_strategy("RSIReversion")
@create_strategy_process(
    process_name="strategy_rsi_eth_1h",
    default_config={
        "name": "rsi_eth_1h",
        "inputs": ["market.kraken.ETH-USD.candles.1h"],
        "outputs": ["ETH-USD"],
        "exchange": ExchangeEnum.PAPER,
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

    def required_candle_history(self) -> int:
        """Return the RSI lookback so warm-up makes the first live bar decisive.

        The callback needs ``period`` closes plus the previous-RSI point for
        its cross conditions, so ``period + 1`` warmed bars let the first
        post-warm-up live bar evaluate immediately instead of idling
        ``period`` live periods.

        Returns:
            ``period + 1``.
        """
        return int(self.period) + 1

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle and generate signal if conditions met.

        Args:
            instrument: The instrument symbol.
            candle: The candle data with OHLCV data.

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
            return StrategySignal(
                instrument=instrument,
                side=TradeSideEnum.BUY,
                strength=1.0,
                price=current_price,
                reason=f"RSI {r:.2f} <= {self.lower} (period={self.period}, prev={r_prev:.2f})",
            )
        if r >= self.upper or (r_prev >= self.upper and c_last < c_prev):
            self._cool[instrument] = self.cooldown
            return StrategySignal(
                instrument=instrument,
                side=TradeSideEnum.SELL,
                strength=1.0,
                price=current_price,
                reason=f"RSI {r:.2f} >= {self.upper} (period={self.period}, prev={r_prev:.2f})",
            )
        return None

    async def reset(self) -> None:
        """Reset strategy state for replay."""
        self._cool.clear()
        logger.info(f"Strategy {self.name} state reset for replay")
