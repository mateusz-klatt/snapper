"""Cointegration pairs trading strategy.

This module implements a statistical arbitrage strategy based on
cointegration between two correlated instruments.
"""

import pandas as pd
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.core.types import TradeSide
from snapper.core.types import TradeSideEnum
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.decorators import create_strategy_process
from snapper.strategies.decorators import register_strategy
from snapper.strategies.multi_leg import MultiLegSpreadMixin
from snapper.strategies.multi_leg import _extract_instrument_from_topic
from snapper.strategies.process_wrapper import create_strategy_process as _create_strategy_process


@register_strategy("CointegrationPairs")
@create_strategy_process(
    process_name="strategy_cointegration_btc_eth",
    default_config={
        "name": "cointegration_btc_eth",
        "inputs": [
            "market.paper.kraken.BTC-USD.candles.1h",
            "market.paper.kraken.ETH-USD.candles.1h",
        ],
        "outputs": ["BTC-USD", "ETH-USD"],
        "exchange": ExchangeEnum.PAPER,
        "params": {
            "beta": 0.05,
            "entry_threshold": 2.0,
            "exit_threshold": 0.5,
            "lookback_window": 50,
            "min_data_points": 30,
        },
    },
)
class CointegrationPairs(BaseStrategy, MultiLegSpreadMixin):
    """Pairs trading strategy based on cointegration.

    Trades the spread between two cointegrated instruments,
    entering when spread deviates from mean and exiting on reversion.

    Attributes:
        beta: Hedge ratio between instruments.
        entry_threshold: Z-score threshold for entry (in standard deviations).
        exit_threshold: Z-score threshold for exit.
        lookback_window: Window for spread statistics.
        min_data_points: Minimum data points required.
        instrument1: First instrument symbol — backwards-compatible alias
            for ``self.legs[0]``.
        instrument2: Second instrument symbol — alias for ``self.legs[1]``.
    """

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize cointegration strategy.

        Two valid invocation shapes (resolved via
        :func:`snapper.strategies.multi_leg.resolve_legs`):

        - **Live ZMQ process** — ``inputs`` is a 2-element list of
          market-data candle topics, one per leg.
        - **Direct-DB backtest** — ``DirectDbEngine`` synthesises a
          single ``candles.{exchange}.synthetic.{timeframe}`` input
          and passes the pair instruments through ``outputs``.

        Args:
            config: Strategy configuration.

        Raises:
            ValueError: If neither invocation shape produces exactly
                two pair instruments.
        """
        super().__init__(config)
        self.beta = float(self.params.get("beta", 0.05))
        self.entry_threshold = float(self.params.get("entry_threshold", 2.0))
        self.exit_threshold = float(self.params.get("exit_threshold", 0.5))
        self.lookback_window = int(self.params.get("lookback_window", 50))
        self.min_data_points = int(self.params.get("min_data_points", 30))
        self._position: str | None = None
        self._init_legs(expected_count=2)
        self.instrument1 = self.legs[0]
        self.instrument2 = self.legs[1]
        logger.info(
            f"CointegrationPairs initialized: {self.instrument1} vs {self.instrument2}, "
            f"beta={self.beta}, entry_threshold={self.entry_threshold}sigma"
        )

    @staticmethod
    def _extract_instrument(topic: str) -> str:
        """Backwards-compat alias for :func:`_extract_instrument_from_topic`.

        Kept so existing call sites and unit tests that reach into this
        static method (e.g. ``CointegrationPairs._extract_instrument``)
        keep working after the migration to the multi-leg helper module.

        Args:
            topic: ZMQ topic string.

        Returns:
            Extracted instrument symbol.
        """
        return _extract_instrument_from_topic(topic)

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Process incoming candle and generate spread trading signal.

        Args:
            instrument: The instrument symbol.
            candle: The candle data with OHLCV data.

        Returns:
            StrategySignal based on spread z-score, or None.
        """
        if instrument not in [self.instrument1, self.instrument2]:
            return None
        bars = self.candle_buffer.get(instrument, [])
        if not bars:
            logger.warning(f"No candle data for {instrument}")
            return None
        current_price = bars[-1].close
        prices1_list = [b.close for b in self.candle_buffer.get(self.instrument1, [])]
        prices2_list = [b.close for b in self.candle_buffer.get(self.instrument2, [])]
        if len(prices1_list) < self.min_data_points or len(prices2_list) < self.min_data_points:
            return None
        prices1 = pd.Series(prices1_list[-self.lookback_window :])
        prices2 = pd.Series(prices2_list[-self.lookback_window :])
        spread = prices1 - self.beta * prices2
        spread_mean = spread.mean()
        spread_std = spread.std()
        if spread_std == 0:
            return None
        current_spread = prices1.iloc[-1] - self.beta * prices2.iloc[-1]
        z_score = (current_spread - spread_mean) / spread_std
        logger.debug(
            f"Spread z-score: {z_score:.2f}, position: {self._position}, "
            f"spread: {current_spread:.2f}, mean: {spread_mean:.2f}, std: {spread_std:.2f}"
        )
        signal = self._generate_signal_from_zscore(z_score, instrument, current_price)
        return signal

    def _build_signal(
        self,
        instrument: str,
        price: float,
        z_score: float,
        primary_side: TradeSide,
        hedge_side: TradeSide,
        action: str,
        strength: float,
    ) -> StrategySignal:
        """Build a signal for either the primary or hedge instrument.

        Args:
            instrument: The instrument for the signal.
            price: Current price.
            z_score: Current z-score for the reason string.
            primary_side: Side for instrument1 ('buy' or 'sell').
            hedge_side: Side for instrument2 ('buy' or 'sell').
            action: Description for the reason (e.g., 'Enter short spread').
            strength: StrategySignal strength for instrument1.

        Returns:
            StrategySignal for the given instrument.
        """
        is_primary = instrument == self.instrument1
        side = primary_side if is_primary else hedge_side
        hedge_suffix = "" if is_primary else " hedge"
        actual_strength = strength if is_primary else strength * self.beta
        return StrategySignal(
            instrument=instrument,
            side=side,
            strength=actual_strength,
            price=price,
            reason=f"Cointegration: {action}{hedge_suffix} (z={z_score:.2f}sigma)",
        )

    def _generate_signal_from_zscore(
        self, z_score: float, instrument: str, price: float
    ) -> StrategySignal | None:
        """Generate signal based on spread z-score.

        Returns the signal for the current instrument and queues the
        partner-leg signal via :meth:`emit_paired_signal` so both legs
        of the spread enter and exit together. The partner-leg price
        is read from ``self.candle_buffer`` (last close) — both legs'
        candles for the current timestep have already been buffered
        by the time this method runs.

        Args:
            z_score: Current spread z-score.
            instrument: The instrument for the signal.
            price: Current price.

        Returns:
            Entry or exit signal for the current instrument; the
            partner-leg signal is queued for downstream draining.
        """
        entry_strength = min(abs(z_score) / self.entry_threshold, 1.0)
        if self._position is None:
            if z_score > self.entry_threshold:
                self._position = "short_spread"
                return self._build_paired_entry(
                    instrument,
                    price,
                    z_score,
                    TradeSideEnum.SELL,
                    TradeSideEnum.BUY,
                    "Enter short spread",
                    entry_strength,
                )
            if z_score < -self.entry_threshold:
                self._position = "long_spread"
                return self._build_paired_entry(
                    instrument,
                    price,
                    z_score,
                    TradeSideEnum.BUY,
                    TradeSideEnum.SELL,
                    "Enter long spread",
                    entry_strength,
                )
        elif self._position == "short_spread" and z_score < self.exit_threshold:
            self._position = None
            return self._build_paired_entry(
                instrument,
                price,
                z_score,
                TradeSideEnum.BUY,
                TradeSideEnum.SELL,
                "Exit short spread",
                0.0,
            )
        elif self._position == "long_spread" and z_score > -self.exit_threshold:
            self._position = None
            return self._build_paired_entry(
                instrument,
                price,
                z_score,
                TradeSideEnum.SELL,
                TradeSideEnum.BUY,
                "Exit long spread",
                0.0,
            )
        return None

    def _partner_price(self, current_instrument: str) -> float | None:
        """Return the partner leg's last buffered close, or ``None`` if absent."""
        partner = self.instrument2 if current_instrument == self.instrument1 else self.instrument1
        bars = self.candle_buffer.get(partner, [])
        if not bars:
            return None
        return float(bars[-1].close)

    def _build_paired_entry(
        self,
        instrument: str,
        price: float,
        z_score: float,
        primary_side: TradeSide,
        hedge_side: TradeSide,
        action: str,
        strength: float,
    ) -> StrategySignal:
        """Build the current-instrument signal AND queue the partner-leg signal.

        Mirrors :meth:`_build_signal` but additionally enqueues the
        partner leg via :meth:`emit_paired_signal` so the engine /
        live executor receives both legs in the same timestep.

        Args:
            instrument: The instrument the current ``on_candle`` is
                processing.
            price: Current instrument's last close.
            z_score: Spread z-score (used in the reason string).
            primary_side: Side for ``instrument1``.
            hedge_side: Side for ``instrument2``.
            action: Reason prefix (e.g. ``"Enter short spread"``).
            strength: Signal strength scalar for the primary leg; the
                hedge leg's strength is scaled by ``self.beta``.

        Returns:
            Signal for the current instrument. The partner-leg signal
            is queued and drained by ``batch_processor`` (or the live
            ZMQ executor) immediately after.
        """
        current_signal = self._build_signal(
            instrument, price, z_score, primary_side, hedge_side, action, strength
        )
        partner_price = self._partner_price(instrument)
        if partner_price is not None:
            partner = self.instrument2 if instrument == self.instrument1 else self.instrument1
            self.emit_paired_signal(
                self._build_signal(
                    partner,
                    partner_price,
                    z_score,
                    primary_side,
                    hedge_side,
                    action,
                    strength,
                )
            )
        return current_signal

    async def reset(self) -> None:
        """Reset strategy state for replay."""
        self._position = None
        logger.info(f"Strategy {self.name} reset")


_FET_RENDER_DEFAULT_CONFIG: dict[str, object] = {
    "name": "cointegration_fet_render",
    "inputs": [
        "market.paper.kraken.FET-USD.candles.1d",
        "market.paper.kraken.RENDER-USD.candles.1d",
    ],
    "outputs": ["FET-USD", "RENDER-USD"],
    "exchange": ExchangeEnum.PAPER,
    "params": {
        "beta": 0.257463,
        "entry_threshold": 2.0,
        "exit_threshold": 0.5,
        "lookback_window": 60,
        "min_data_points": 30,
    },
}
"""Forward-test config for the FET-USD / RENDER-USD pair-trade.

Sweet-spot params from the cointegration screening session:

- ``beta=0.257463`` — OLS hedge ratio fit on linear prices in the
  train window 2023-11-09 → 2024-12-31 (the engine uses linear
  prices, not log).
- ``entry_threshold=2.0`` / ``exit_threshold=0.5`` /
  ``lookback_window=60`` — sweet-spot region of the OOS walk-forward
  heatmap (Sharpe 0.7-0.98 across this neighbourhood).

Engine FET/RENDER backtest with these params produced Sharpe 0.61 /
return +60% / max DD -65% over the 2023-11 → 2026-05 period after
the Bug L paired-signal fix. See
``proprietary/plans/plan_2026_05_07_fet_render_forward_test.md``
for the full spec + decision criteria.
"""

_create_strategy_process(
    process_name="strategy_cointegration_fet_render",
    strategy_class="CointegrationPairs",
    default_config=_FET_RENDER_DEFAULT_CONFIG,
)
