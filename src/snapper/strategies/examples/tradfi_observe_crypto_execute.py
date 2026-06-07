"""Reference strategy — observe TradFi index futures, execute on crypto.

**NOT auto-registered.** This module demonstrates the cross-asset
TradFi market-data pattern supported by Snapper: subscribe to
market-data-only instruments (``can_trade=False`` on ``SymbolExchangeCapability``
rows, e.g. Kraken FCM index futures) and emit signals whose target is an
execution-capable instrument (``can_trade=True``) on a different venue.
How to activate a copy of this strategy
1. Copy this file to ``src/snapper/strategies/`` (package root).
2. Add ``@register_strategy("TradFiObserveCryptoExecute")`` and
   ``@create_strategy_process(...)`` decorators.
3. Wire ``inputs`` to the observed TradFi candle topic (for example
   ``market.kraken_equities.MNQM6-CME.candles.1h``) and ``outputs`` to
   the crypto execution instrument (for example ``BTC-USD``).
4. Point ``exchange`` at the execution venue (for example
   ``ExchangeEnum.KRAKEN``).
Safety rails
The 10-minute FCM delay on ``kraken_equities`` ticks propagates onto
  every ``TickData.is_delayed=True``. Any tick-driven variant of this
  pattern MUST gate on that flag. Candle-driven variants (this example)
  do not see ``is_delayed`` but still have the same effective latency
  document the implicit ~10 minute lag in the live runbook.
Emitting a signal with ``instrument`` that is ``can_trade=False``
  would be rejected by the order-entry capability guard
  (``require_tradable`` in ``src/snapper/server/_capability_guard.py``)
  with HTTP 422 ``error_code='instrument_market_data_only'``. This
  example targets a crypto spot instrument in ``_CRYPTO_EXECUTION_SYMBOL``
  so the guard always passes.
The ``outputs`` list on the ``StrategyConfig`` is validated against
  the launching operator's scope grants at startup; ensure the execution
  instrument falls within the operator's granted scope.
The class below is intentionally minimal: a pair of exponential moving
averages over the TradFi series, crossing to generate a BUY/SELL signal.
Real-world cross-asset strategies typically add drift and spread
filters, session-boundary gating, and per-underlying position sizing.
"""

from datetime import UTC
from datetime import datetime
from typing import Any

from snapper.core.types import TradeSideEnum
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

_CRYPTO_EXECUTION_SYMBOL = "BTC-USD"


class TradFiObserveCryptoExecute(BaseStrategy):
    """Illustrative cross-asset strategy — observe TradFi, execute on crypto.

    Consumes candles from a market-data-only TradFi instrument (e.g.
    ``MNQM6-CME`` on ``kraken_equities``) and emits signals targeting a
    crypto instrument (``_CRYPTO_EXECUTION_SYMBOL`` on the strategy's
    configured ``exchange``). Crossover of two EMAs of the observed
    TradFi close series drives the BUY/SELL decision.

    This class is illustrative and not registered with the process
    registry. See module docstring for activation steps.

    Attributes:
        fast_period: Fast-EMA lookback.
        slow_period: Slow-EMA lookback (must be > fast_period).
        min_candles: Minimum observed candles before emitting signals.
    """

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize the reference strategy.

        Args:
            config: Strategy configuration. ``params`` may contain
                ``fast_period`` (int, default 12), ``slow_period`` (int,
                default 26), and ``min_candles`` (int, default 30).
        """
        super().__init__(config)
        params: dict[str, Any] = config.params
        self.fast_period: int = int(params.get("fast_period", 12))
        self.slow_period: int = int(params.get("slow_period", 26))
        self.min_candles: int = int(params.get("min_candles", 30))
        if self.fast_period >= self.slow_period:
            raise ValueError(
                "fast_period must be strictly less than slow_period "
                f"(got fast={self.fast_period}, slow={self.slow_period})"
            )
        self._last_direction: str | None = None

    async def reset(self) -> None:
        """Clear crossover memory so a warm-restart re-emits the first signal.

        Invoked by the strategy runtime on cold reset + at the start of a
        backtest replay loop. Keeping ``_last_direction`` on a live
        restart would suppress the first post-restart signal, which
        would defeat the point of the reset hook.
        """
        self._last_direction = None

    async def on_candle(self, instrument: str, candle: CandleData) -> StrategySignal | None:
        """Compute EMA crossover on the observed TradFi series.

        Emits a BUY signal on fast-over-slow crossover, SELL on the
        inverse. Signal ``instrument`` is always ``_CRYPTO_EXECUTION_SYMBOL``
        so the order-entry capability guard accepts the downstream trade.

        Args:
            instrument: Native symbol of the candle source (TradFi feed).
            candle: Latest candle event for ``instrument``.

        Returns:
            ``StrategySignal`` when a crossover fires, else ``None``.
        """
        closes = [float(b.close) for b in self.candle_buffer.get(instrument, [])]
        if len(closes) < self.min_candles:
            return None
        fast = _ema(closes, self.fast_period)
        slow = _ema(closes, self.slow_period)
        direction: str
        if fast > slow:
            direction = "above"
        elif fast < slow:
            direction = "below"
        else:
            return None
        if direction == self._last_direction:
            return None
        self._last_direction = direction
        side = TradeSideEnum.BUY if direction == "above" else TradeSideEnum.SELL
        return StrategySignal(
            instrument=_CRYPTO_EXECUTION_SYMBOL,
            side=side,
            strength=0.5,
            reason=(
                f"TradFi EMA crossover on {instrument}: "
                f"fast({self.fast_period})={fast:.2f} {direction} "
                f"slow({self.slow_period})={slow:.2f}"
            ),
            price=float(candle.close),
            timestamp=datetime.now(UTC),
        )


def _ema(values: list[float], period: int) -> float:
    """Plain exponential moving average over the tail of ``values``.

    Args:
        values: Ordered series (oldest first).
        period: EMA lookback length.

    Returns:
        EMA value at the last element.
    """
    if len(values) < period:
        return values[-1]
    multiplier = 2.0 / (period + 1.0)
    seed = sum(values[-period:]) / period
    ema_value = seed
    for price in values[-period:]:
        ema_value = (price - ema_value) * multiplier + ema_value
    return ema_value
