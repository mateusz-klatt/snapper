"""Cointegration pairs trading strategy.

This module implements a statistical arbitrage strategy based on
cointegration between two correlated instruments.
"""

from datetime import datetime
from typing import NamedTuple

import pandas as pd
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.core.types import PairedExecutionPolicyEnum
from snapper.core.types import TradeSide
from snapper.core.types import TradeSideEnum
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.base import StrategyConfig
from snapper.strategies.base import StrategySignal
from snapper.strategies.base import StrategySignalResult
from snapper.strategies.decorators import create_strategy_process
from snapper.strategies.decorators import register_strategy
from snapper.strategies.multi_leg import MultiLegSpreadMixin
from snapper.strategies.multi_leg import _extract_instrument_from_topic
from snapper.strategies.process_wrapper import create_strategy_process as _create_strategy_process


class _SpreadDecision(NamedTuple):
    """Resolved entry/exit decision for the spread, independent of leg prices.

    Separates the z-score decision (which sides, what target position)
    from building the paired signals and from committing ``_position``,
    so the position is mutated only after both legs are successfully
    built.

    Attributes:
        new_position: Position to commit on success (``"short_spread"`` /
            ``"long_spread"`` for entries, ``None`` for exits).
        primary_side: Side for ``instrument1``.
        hedge_side: Side for ``instrument2``.
        action: Reason prefix (e.g. ``"Enter short spread"``).
        strength: Primary-leg signal strength scalar.
    """

    new_position: str | None
    primary_side: TradeSide
    hedge_side: TradeSide
    action: str
    strength: float


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
        instrument1: First instrument symbol — compatibility alias for
            ``self.legs[0]``.
        instrument2: Second instrument symbol — alias for ``self.legs[1]``.
    """

    PAIRED_EXECUTION_POLICY = PairedExecutionPolicyEnum.SIMULTANEOUS
    """Both spread legs are armed and dispatched together (simultaneous)."""

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
        self._last_signal_open_at: datetime | None = None
        self._init_legs(expected_count=2)
        self.instrument1 = self.legs[0]
        self.instrument2 = self.legs[1]
        logger.info(
            f"CointegrationPairs initialized: {self.instrument1} vs {self.instrument2}, "
            f"beta={self.beta}, entry_threshold={self.entry_threshold}sigma"
        )

    def required_candle_history(self) -> int:
        """Return the spread lookback window as the warm-up bar requirement.

        The spread z-score needs ``lookback_window`` closes per leg before it can
        fire (`:meth:`_compute_spread_signal``); declaring it here lets
        :meth:`BaseStrategy._warmup_candle_buffer` prefill that many historical
        bars at startup instead of waiting ``lookback_window`` live periods.

        Returns:
            The configured ``lookback_window``.
        """
        return self.lookback_window

    def requires_aligned_warmup(self) -> bool:
        """Return True: the pair spread demands date-aligned warm-up legs.

        The z-score joins legs by ``open_at``; a partially-warmed or
        misaligned leg set would compute a spread over mismatched dates
        and emit false signals on a live money path, so warm-up stays
        ALL-OR-NOTHING (base fail-closes multi-leg anyway; this override
        documents the contract explicitly).

        Returns:
            Always True.
        """
        return True

    def _signal_floor(self) -> datetime | None:
        """Return the latest ``open_at`` no signal may fire on (it or earlier).

        The max of the warmup high-water mark (prefilled past days are context,
        not tradeable — and live re-publishing a warmup day must not emit on
        mixed warmup/live data) and the last-signaled day (one decision per day).

        Returns:
            The suppression floor ``open_at``, or ``None`` when neither is set.
        """
        floors = [
            open_at
            for open_at in (self._warmup_through_open_at, self._last_signal_open_at)
            if open_at is not None
        ]
        return max(floors) if floors else None

    @staticmethod
    def _extract_instrument(topic: str) -> str:
        """Compatibility alias for :func:`_extract_instrument_from_topic`.

        Kept so existing call sites and unit tests that reach into this
        static method (e.g. ``CointegrationPairs._extract_instrument``)
        keep working with the shared multi-leg helper.

        Args:
            topic: ZMQ topic string.

        Returns:
            Extracted instrument symbol.
        """
        return _extract_instrument_from_topic(topic)

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignalResult:
        """Process incoming candle and generate the spread trading signal group.

        Args:
            instrument: The instrument symbol.
            candle: The candle data with OHLCV data.

        The spread is computed on the two legs ALIGNED BY ``open_at`` (the days
        BOTH legs have a bar), never by list position — a live feed delivering one
        leg's bar before the other must not desync the regression. A freshness gate
        then emits only when the triggering candle is BOTH legs' current bar:
        ``candle.open_at == common[-1] == latest_seen`` (the latest day across
        either leg). So a first-arriving (unpaired) bar of a period returns
        ``None``, and — critically — if the OTHER leg has raced ahead to a later
        day, this leg's bar is NOT the latest seen and also returns ``None``, so
        the z-score, spread, AND both legs' order prices (``bars[-1].close`` /
        partner ``bars[-1].close``) are always the SAME executable day. A leg that
        lags then catches up in a burst only emits once both are current (live can
        only execute at the current price). This matches the date-aligned daily
        backtest the strategy was validated on.

        At most ONE signal is emitted per ``open_at`` (the ``_last_signal_open_at``
        guard): a revised / re-published bar for an already-signaled day cannot
        produce a second same-day action (e.g. an entry then an exit), matching the
        backtest's one-decision-per-day and guarding the A3-warmup/live same-day
        collision.

        Returns:
            ``[primary, hedge]`` (both legs of the spread, in
            current-instrument-then-partner order) on entry/exit, or
            ``None`` when no trade triggers, the partner price is unavailable, or
            the triggering candle does not complete the latest common day. Never
            returns a single naked leg.
        """
        if instrument not in [self.instrument1, self.instrument2]:
            return None
        bars = self.candle_buffer.get(instrument, [])
        if not bars:
            logger.warning(f"No candle data for {instrument}")
            return None
        current_price = bars[-1].close
        closes1 = {b.open_at: b.close for b in self.candle_buffer.get(self.instrument1, [])}
        closes2 = {b.open_at: b.close for b in self.candle_buffer.get(self.instrument2, [])}
        common = sorted(closes1.keys() & closes2.keys())
        if len(common) < self.min_data_points:
            return None
        latest_seen = max(closes1.keys() | closes2.keys())
        if candle.open_at != common[-1] or candle.open_at != latest_seen:
            return None
        signal_floor = self._signal_floor()
        if signal_floor is not None and candle.open_at <= signal_floor:
            return None
        window = common[-self.lookback_window :]
        prices1 = pd.Series([closes1[open_at] for open_at in window])
        prices2 = pd.Series([closes2[open_at] for open_at in window])
        spread = prices1 - self.beta * prices2
        spread_mean = spread.mean()
        spread_std = spread.std()
        if spread_std == 0:
            return None
        current_spread = float(prices1.iloc[-1] - self.beta * prices2.iloc[-1])
        z_score = (current_spread - spread_mean) / spread_std
        logger.debug(
            f"Spread z-score: {z_score:.2f}, position: {self._position}, "
            f"spread: {current_spread:.2f}, mean: {spread_mean:.2f}, std: {spread_std:.2f}"
        )
        result = self._generate_signal_from_zscore(z_score, instrument, current_price)
        if result is not None:
            self._last_signal_open_at = candle.open_at
        return result

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

    def _decide_spread_action(self, z_score: float) -> _SpreadDecision | None:
        """Resolve the entry/exit decision from the z-score and position.

        Pure function of ``z_score`` and ``self._position`` — does NOT
        mutate state and does NOT read leg prices. Entry decisions are
        only available when flat; exit decisions only when in the
        matching position.

        Args:
            z_score: Current spread z-score.

        Returns:
            A :class:`_SpreadDecision` when a trade triggers, else
            ``None``.
        """
        entry_strength = min(abs(z_score) / self.entry_threshold, 1.0)
        if self._position is None:
            if z_score > self.entry_threshold:
                return _SpreadDecision(
                    "short_spread",
                    TradeSideEnum.SELL,
                    TradeSideEnum.BUY,
                    "Enter short spread",
                    entry_strength,
                )
            if z_score < -self.entry_threshold:
                return _SpreadDecision(
                    "long_spread",
                    TradeSideEnum.BUY,
                    TradeSideEnum.SELL,
                    "Enter long spread",
                    entry_strength,
                )
            return None
        if self._position == "short_spread" and z_score < self.exit_threshold:
            return _SpreadDecision(
                None, TradeSideEnum.BUY, TradeSideEnum.SELL, "Exit short spread", 0.0
            )
        if self._position == "long_spread" and z_score > -self.exit_threshold:
            return _SpreadDecision(
                None, TradeSideEnum.SELL, TradeSideEnum.BUY, "Exit long spread", 0.0
            )
        return None

    def _generate_signal_from_zscore(
        self, z_score: float, instrument: str, price: float
    ) -> list[StrategySignal] | None:
        """Generate the paired spread signal group based on the z-score.

        Resolves the entry/exit decision, builds BOTH legs, and commits
        ``self._position`` ONLY after the paired list is successfully
        built — so the strategy never internally enters a position it did
        not emit, and never emits a single naked leg. The partner-leg
        price is read from ``self.candle_buffer`` (last close); both legs'
        candles for the current timestep are already buffered by the time
        this method runs.

        Args:
            z_score: Current spread z-score.
            instrument: The instrument the current candle belongs to.
            price: Current instrument's last close.

        Returns:
            ``[current_leg, partner_leg]`` on a triggered, fully-built
            trade; ``None`` when no trade triggers or the partner price
            is missing (in which case ``_position`` is left unchanged).
        """
        decision = self._decide_spread_action(z_score)
        if decision is None:
            return None
        pair = self._build_paired_entry(
            instrument,
            price,
            z_score,
            decision.primary_side,
            decision.hedge_side,
            decision.action,
            decision.strength,
        )
        if pair is not None:
            self._position = decision.new_position
        return pair

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
    ) -> list[StrategySignal] | None:
        """Build BOTH spread legs, or ``None`` if the partner price is missing.

        Returns the current-instrument signal followed by the partner-leg
        signal so the host ``on_candle`` can return both legs and have
        ``BaseStrategy`` emit them atomically. If the partner leg has no
        buffered price yet, returns ``None`` so the strategy emits
        nothing and does not enter a single naked leg.

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
            ``[current_leg_signal, partner_leg_signal]`` in
            current-instrument-then-partner order, or ``None`` when the
            partner price is unavailable.
        """
        partner_price = self._partner_price(instrument)
        if partner_price is None:
            return None
        partner = self.instrument2 if instrument == self.instrument1 else self.instrument1
        return [
            self._build_signal(
                instrument, price, z_score, primary_side, hedge_side, action, strength
            ),
            self._build_signal(
                partner, partner_price, z_score, primary_side, hedge_side, action, strength
            ),
        ]

    async def reset(self) -> None:
        """Reset strategy state for replay."""
        self._position = None
        self._last_signal_open_at = None
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
        "warmup_market_type": "crypto",
        "buffer_size": 100,
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
paired-signal group emission.
"""

_create_strategy_process(
    process_name="strategy_cointegration_fet_render",
    strategy_class="CointegrationPairs",
    default_config=_FET_RENDER_DEFAULT_CONFIG,
)
