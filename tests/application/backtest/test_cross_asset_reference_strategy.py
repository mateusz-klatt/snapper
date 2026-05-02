"""End-to-end cross-asset backtest test against the shipped reference strategy.

Proves that ``TradFiObserveCryptoExecute`` (observe MNQU6-CME, execute
BTC-USD) actually trades BTC-USD on the configured target venue
(``kraken``) when backtested under the new
``BacktestConfig.target_execution_exchange`` contract — not on the source
TradFi feed (``kraken_equities``). This is the objective acceptance
criterion for BE-2.

``snapper.strategies.models.is_tradeable`` is patched to True because
``DirectDbEngine.run`` currently hardcodes ``StrategyConfig.outputs`` to
every instrument from ``BacktestConfig.instruments`` (source feed +
target feed combined) and then ``_validate_output_instruments`` rejects
MNQU6-CME as non-tradeable on paper. Widening the output derivation to
only the target instrument is pre-existing drift (non-goal v1 — REST
API + engine output construction stay single-feed; cross-asset runs
targeted via direct BacktestConfig use); this test focuses strictly on
the BE-1 attribution invariants.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

import snapper.application.backtest.batch_processor as batch_processor_module
from snapper.application.backtest.batch_processor import simulate_market_fill
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.result_collector import ResultCollector
from snapper.strategies.examples.tradfi_observe_crypto_execute import TradFiObserveCryptoExecute

NOW = datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)
_SOURCE_EXCHANGE = "kraken_equities"
_SOURCE_INSTRUMENT = "MNQU6-CME"
_TARGET_EXCHANGE = "kraken"
_TARGET_INSTRUMENT = "BTC-USD"
_START = NOW
_END = NOW + timedelta(days=5)


def _candle_row(
    open_at: datetime,
    close: float,
    seq: int,
) -> dict[str, Any]:
    """Build a minimal deterministic CandleRow dict."""
    return {
        "open_at": open_at,
        "timeframe": "1h",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1.0,
        "vwap": None,
        "trades": None,
        "public_id": f"candle-{seq}",
        "timestamp": open_at,
        "session_id": "bt",
        "sequence_id": seq,
    }


def _build_two_feed_fixture() -> tuple[list[dict[str, Any]], list[dict[str, Any]], BacktestConfig]:
    """Build MNQU6-CME + BTC-USD candles + a cross-asset BacktestConfig.

    Design:

    * 5 warmup candles per feed BEFORE start_date (flat MNQU6 close=100
      primes the strategy's candle_buffer + BTC-USD latest_closes).
    * 5 active candles AT/AFTER start_date with rising MNQU6 closes
      so the fast/slow EMA crossover fires BUY on the first active bar.
    * BTC-USD candles at the SAME timestamps so
      ``latest_closes['BTC-USD']`` is already populated at signal fire
      time (no blocked fill — counter == 0 asserted downstream).

    Returns:
        (mnqu6_rows, btc_rows, config) — ready for repository stub +
        engine.run invocation.
    """
    mnqu6_rows: list[dict[str, Any]] = []
    btc_rows: list[dict[str, Any]] = []
    seq = 0
    for hours_offset in range(-5, 0):
        seq += 1
        open_at = NOW + timedelta(hours=hours_offset)
        mnqu6_rows.append(_candle_row(open_at=open_at, close=100.0, seq=seq))
        btc_rows.append(_candle_row(open_at=open_at, close=42_000.0, seq=seq + 100))
    for idx, hours_offset in enumerate(range(0, 5), start=1):
        seq += 1
        open_at = NOW + timedelta(hours=hours_offset)
        mnqu6_rows.append(_candle_row(open_at=open_at, close=100.0 + idx, seq=seq))
        btc_rows.append(_candle_row(open_at=open_at, close=42_000.0, seq=seq + 100))

    config = BacktestConfig(
        strategy_class="TradFiObserveCryptoExecute",
        instruments={
            _SOURCE_EXCHANGE: [_SOURCE_INSTRUMENT],
            _TARGET_EXCHANGE: [_TARGET_INSTRUMENT],
        },
        start_date=_START,
        end_date=_END,
        wallet_public_id="w-bt",
        initial_balance=10_000.0,
        strategy_params={"fast_period": 2, "slow_period": 3, "min_candles": 3},
        timeframe="1h",
        target_execution_exchange=_TARGET_EXCHANGE,
    )
    return mnqu6_rows, btc_rows, config


def _repo_stub(mnqu6_rows: list[dict[str, Any]], btc_rows: list[dict[str, Any]]) -> AsyncMock:
    """Repository stub that returns per-instrument rows per engine call."""

    def fake_get_candles(
        *,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime,
        exchange: str,
        as_of: datetime,
        order: str,
    ) -> list[dict[str, Any]]:
        _ = (timeframe, start, end, exchange, as_of, order)
        if instrument == _SOURCE_INSTRUMENT:
            return mnqu6_rows
        if instrument == _TARGET_INSTRUMENT:
            return btc_rows
        return []

    repo = AsyncMock()
    repo.get_candles = AsyncMock(side_effect=fake_get_candles)
    return repo


class TestCrossAssetReferenceStrategy:
    """BE-2 — reference strategy end-to-end through DirectDbEngine.run()."""

    @pytest.mark.asyncio
    async def test_tradfi_observe_crypto_execute_routes_fill_to_target_venue(
        self,
    ) -> None:
        """MNQU6-CME observation → BTC-USD fill on kraken (not kraken_equities).

        Given: a 2-feed backtest fixture (MNQU6-CME/kraken_equities source
            + BTC-USD/kraken target) with the shipped
            TradFiObserveCryptoExecute registered via patch.dict +
            target_execution_exchange='kraken',
        When: DirectDbEngine.run drives the strategy end-to-end through
            batch_processor.process_time_batch,
        Then:
            * at least one signal fired (EMA crossover on rising MNQU6),
            * every recorded trade carries instrument='BTC-USD' (target),
            * every simulate_market_fill call was invoked with
              exchange='kraken' and instrument='BTC-USD' (proves BE-1
              attribution switch ran on the target, not the source),
            * portfolio holds a BTC-USD position (not MNQU6-CME),
            * cross_asset_blocked_fills == 0 because both feeds are
              primed before the first post-warmup candle.
        """
        fill_calls: list[dict[str, Any]] = []

        def _capture(*args: object, **kwargs: object) -> object:
            fill_calls.append(dict(kwargs))
            return simulate_market_fill(*args, **kwargs)

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"TradFiObserveCryptoExecute": TradFiObserveCryptoExecute},
                clear=False,
            ),
            patch.object(batch_processor_module, "simulate_market_fill", side_effect=_capture),
            patch("snapper.strategies.models.is_tradeable", return_value=True),
        ):
            mnqu6_rows, btc_rows, config = _build_two_feed_fixture()
            engine = DirectDbEngine(_repo_stub(mnqu6_rows, btc_rows), NOW)
            collector = ResultCollector()
            portfolio, latest_closes = await engine.run("run-bt-1", config, collector)

        assert len(collector.signals) >= 1, "expected at least one EMA crossover signal"
        for trade in collector.trades:
            assert (
                trade["instrument"] == _TARGET_INSTRUMENT
            ), f"trade attributed to {trade['instrument']}, expected {_TARGET_INSTRUMENT}"
        assert fill_calls, "simulate_market_fill was never invoked"
        for call_kwargs in fill_calls:
            assert call_kwargs["exchange"] == _TARGET_EXCHANGE, (
                f"fill exchange={call_kwargs['exchange']}, expected "
                f"{_TARGET_EXCHANGE} (cross-asset target)"
            )
            assert (
                call_kwargs["instrument"] == _TARGET_INSTRUMENT
            ), f"fill instrument={call_kwargs['instrument']}, expected {_TARGET_INSTRUMENT}"
        assert _TARGET_INSTRUMENT in portfolio.positions
        assert _SOURCE_INSTRUMENT not in portfolio.positions
        assert latest_closes[_TARGET_INSTRUMENT] == pytest.approx(42_000.0)
        assert collector.cross_asset_blocked_fills == 0, (
            "warmup candles should prime BTC latest_closes before first "
            "signal — missing target close indicates a fixture bug"
        )
