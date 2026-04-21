"""Unit tests for the TradFi cross-asset reference strategy."""

from datetime import UTC
from datetime import datetime
from typing import cast

import pytest

from snapper.core.types import ExchangeEnum
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.examples.tradfi_observe_crypto_execute import TradFiObserveCryptoExecute
from snapper.strategies.models import StrategyConfig


def _config(**overrides: object) -> StrategyConfig:
    """Build a minimal StrategyConfig for the reference class."""
    params: dict[str, object] = {
        "fast_period": 3,
        "slow_period": 6,
        "min_candles": 6,
    }
    params.update(cast(dict[str, object], overrides.get("params", {})))

    return StrategyConfig(
        name="ref_tradfi_crypto",
        strategy_class="TradFiObserveCryptoExecute",
        inputs=["market.kraken_equities.MNQM6-CME.candles.1h"],
        outputs=["BTC-USD"],
        exchange=ExchangeEnum.KRAKEN,
        params=params,
    )


def _candle(close: float, seq: int = 1) -> CandleData:
    """Build a minimal CandleData row with a specific close."""
    ts = datetime.now(UTC)

    return CandleData(
        public_id=f"candle-{seq}",
        timestamp=ts,
        session_id="test",
        sequence_id=seq,
        instrument="MNQM6-CME",
        exchange="kraken_equities",
        timeframe="1h",
        open_at=ts,
        open=close,
        high=close,
        low=close,
        close=close,
        volume=0.0,
    )


class TestConstruction:
    """Construction-time invariants for TradFiObserveCryptoExecute."""

    def test_defaults_are_reasonable(self) -> None:
        """Defaults produce a 12/26 EMA crossover with 30-candle warmup.

        Given: an empty ``params`` dict,
        When: the strategy is constructed,
        Then: fallback defaults are populated.
        """
        config = StrategyConfig(
            name="ref_tradfi",
            strategy_class="TradFiObserveCryptoExecute",
            inputs=["market.kraken_equities.MNQM6-CME.candles.1h"],
            outputs=["BTC-USD"],
            exchange=ExchangeEnum.KRAKEN,
        )
        strategy = TradFiObserveCryptoExecute(config)
        assert strategy.fast_period == 12
        assert strategy.slow_period == 26
        assert strategy.min_candles == 30

    def test_fast_must_be_less_than_slow(self) -> None:
        """Construction rejects ``fast_period >= slow_period``.

        Given: a config with fast_period=10 and slow_period=10,
        When: the strategy is constructed,
        Then: ``ValueError`` is raised pointing at the invalid values.
        """
        config = _config(params={"fast_period": 10, "slow_period": 10})
        with pytest.raises(ValueError, match="fast_period must be strictly less"):
            TradFiObserveCryptoExecute(config)


class TestCrossoverBehaviour:
    """Crossover semantics of the reference strategy."""

    @pytest.mark.asyncio
    async def test_emits_signal_targeting_crypto_on_upward_crossover(self) -> None:
        """Rising closes trigger a BUY targeting the crypto execution symbol.

        Given: a monotonically increasing close series,
        When: ``on_candle`` is invoked past ``min_candles``,
        Then: a BUY signal for BTC-USD is emitted (not for the TradFi
            observed instrument).
        """
        strategy = TradFiObserveCryptoExecute(_config())
        buffer = [_candle(100.0 + i, seq=i) for i in range(10)]
        strategy.candle_buffer["MNQM6-CME"] = buffer
        signal = await strategy.on_candle("MNQM6-CME", buffer[-1])
        assert signal is not None
        assert signal.instrument == "BTC-USD"
        assert signal.side == "buy"

    @pytest.mark.asyncio
    async def test_emits_sell_on_downward_crossover(self) -> None:
        """Falling closes trigger a SELL targeting the crypto symbol.

        Given: a monotonically decreasing close series,
        When: ``on_candle`` is invoked past ``min_candles``,
        Then: a SELL signal for BTC-USD is emitted.
        """
        strategy = TradFiObserveCryptoExecute(_config())
        buffer = [_candle(200.0 - i, seq=i) for i in range(10)]
        strategy.candle_buffer["MNQM6-CME"] = buffer
        signal = await strategy.on_candle("MNQM6-CME", buffer[-1])
        assert signal is not None
        assert signal.instrument == "BTC-USD"
        assert signal.side == "sell"

    @pytest.mark.asyncio
    async def test_returns_none_before_min_candles(self) -> None:
        """Warmup period returns ``None`` instead of a signal.

        Given: a candle buffer smaller than ``min_candles``,
        When: ``on_candle`` is invoked,
        Then: the call returns ``None`` and does not emit a signal.
        """
        strategy = TradFiObserveCryptoExecute(_config(params={"min_candles": 50}))
        strategy.candle_buffer["MNQM6-CME"] = [_candle(1.0, seq=i) for i in range(5)]
        assert await strategy.on_candle("MNQM6-CME", _candle(1.0, seq=6)) is None

    @pytest.mark.asyncio
    async def test_no_repeat_signal_within_same_direction(self) -> None:
        """Consecutive same-direction crossovers suppress duplicates.

        Given: a buffer whose EMAs remain in the same crossover state,
        When: ``on_candle`` is invoked twice in a row,
        Then: the second call returns ``None`` (no duplicate BUY).
        """
        strategy = TradFiObserveCryptoExecute(_config())
        buffer = [_candle(100.0 + i, seq=i) for i in range(10)]
        strategy.candle_buffer["MNQM6-CME"] = buffer
        first = await strategy.on_candle("MNQM6-CME", buffer[-1])
        assert first is not None
        second = await strategy.on_candle("MNQM6-CME", buffer[-1])
        assert second is None

    @pytest.mark.asyncio
    async def test_flat_series_returns_none_on_tie(self) -> None:
        """A flat series yields equal EMAs and no signal.

        Given: a constant-price series past ``min_candles``,
        When: ``on_candle`` is invoked,
        Then: ``None`` is returned (fast==slow, no crossover).
        """
        strategy = TradFiObserveCryptoExecute(_config())
        buffer = [_candle(100.0, seq=i) for i in range(10)]
        strategy.candle_buffer["MNQM6-CME"] = buffer
        assert await strategy.on_candle("MNQM6-CME", buffer[-1]) is None


class TestReset:
    """Reset hook clears crossover memory so a warm restart re-emits."""

    @pytest.mark.asyncio
    async def test_reset_clears_last_direction(self) -> None:
        """Reset allows the next on_candle to emit again.

        Given: a strategy that has already emitted a BUY (direction=above),
        When: ``reset`` is awaited,
        Then: the next identical ``on_candle`` emits a fresh BUY
            rather than returning ``None`` (the duplicate-suppression path).
        """
        strategy = TradFiObserveCryptoExecute(_config())
        buffer = [_candle(100.0 + i, seq=i) for i in range(10)]
        strategy.candle_buffer["MNQM6-CME"] = buffer
        first = await strategy.on_candle("MNQM6-CME", buffer[-1])
        assert first is not None
        await strategy.reset()
        after_reset = await strategy.on_candle("MNQM6-CME", buffer[-1])
        assert after_reset is not None


class TestEmaHelper:
    """Direct coverage of the ``_ema`` helper's short-circuit path."""

    @pytest.mark.asyncio
    async def test_short_series_falls_through_to_last_close(self) -> None:
        """Short series returns the tail element and yields a tie in on_candle.

        Given: ``min_candles=3`` allows ``on_candle`` past the warmup guard
            with only 3 candles, while ``fast_period=5`` + ``slow_period=6``
            force ``_ema`` into the ``len(values) < period`` short-circuit
            for BOTH lookbacks,
        When: ``on_candle`` is invoked past ``min_candles``,
        Then: both EMAs collapse to ``closes[-1]`` (line 161 covered),
            ``fast == slow`` triggers the tie branch (line 131 covered),
            and the call returns ``None``.
        """
        strategy = TradFiObserveCryptoExecute(
            _config(params={"fast_period": 5, "slow_period": 6, "min_candles": 3})
        )
        buffer = [_candle(100.0 + i, seq=i) for i in range(3)]
        strategy.candle_buffer["MNQM6-CME"] = buffer
        assert await strategy.on_candle("MNQM6-CME", buffer[-1]) is None
