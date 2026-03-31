"""MACD crossover strategy.

This module implements a trend-following strategy based on the
MACD (Moving Average Convergence Divergence) indicator.
"""

import pandas as pd
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSideEnum
from snapper.indicators.ta_lib_adapter import macd
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.decorators import create_strategy_process
from snapper.strategies.decorators import register_strategy


@register_strategy("MACDCrossover")
@create_strategy_process(
    process_name="strategy_macd_btc_1h",
    default_config={
        "name": "macd_btc_1h",
        "inputs": ["market.kraken.BTC-USD.candles.1h"],
        "outputs": ["BTC-USD"],
        "exchange": ExchangeEnum.PAPER,
        "params": {
            "fast": 12,
            "slow": 26,
            "signal_period": 9,
        },
    },
)
class MACDCrossover(BaseStrategy):
    """MACD-based trend following strategy.

    Generates buy signals on bullish histogram crossovers (negative to positive)
    and sell signals on bearish crossovers (positive to negative).

    Attributes:
        fast: Fast EMA period.
        slow: Slow EMA period.
        signal_period: Signal line EMA period.
    """

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize MACD strategy.

        Args:
            config: Strategy configuration.
        """
        super().__init__(config)
        self.fast = self.params.get("fast", 12)
        self.slow = self.params.get("slow", 26)
        self.signal_period = self.params.get("signal_period", 9)
        self._last_hist: dict[str, float] = {}

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle and generate signal on histogram crossover.

        Args:
            instrument: The instrument symbol.
            candle: The candle data with OHLCV data.

        Returns:
            Buy signal on bullish crossover, sell signal on bearish,
            or None if no crossover detected.
        """
        closes = pd.Series([b.close for b in self.candle_buffer.get(instrument, [])]).astype(float)
        if len(closes) < self.slow:
            return None
        current_price = float(closes.iloc[-1])
        _, _, hist_series = macd(closes, self.fast, self.slow, self.signal_period)
        hist = float(hist_series.iloc[-1])
        last_hist = self._last_hist.get(instrument)
        self._last_hist[instrument] = hist
        if last_hist is None:
            return None
        if last_hist <= 0 and hist > 0:
            return StrategySignal(
                instrument=instrument,
                side=TradeSideEnum.BUY,
                strength=min(abs(hist) * 10, 1.0),
                price=current_price,
                reason=f"MACD bull cross (hist={hist:.4f}, fast={self.fast}, slow={self.slow}, signal={self.signal_period})",
            )
        if last_hist >= 0 and hist < 0:
            return StrategySignal(
                instrument=instrument,
                side=TradeSideEnum.SELL,
                strength=min(abs(hist) * 10, 1.0),
                price=current_price,
                reason=f"MACD bear cross (hist={hist:.4f}, fast={self.fast}, slow={self.slow}, signal={self.signal_period})",
            )
        return None

    async def reset(self) -> None:
        """Reset strategy state for replay."""
        self._last_hist.clear()
        logger.info(f"Strategy {self.name} state reset for replay")
