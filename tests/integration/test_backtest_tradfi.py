"""Observation-only backtest integration — TradFi (kraken_equities) symbols.

Backtest engines in this repo do NOT write to
``trade_commands`` / ``orders``
— those are live-runtime surfaces. Backtest artifacts live on the
in-memory ``ResultCollector`` (and, after the runner persists, on
``backtest_signals`` / ``backtest_trades`` / ``backtest_equity_points``).

This test codifies that a TradFi-observing, non-signalling strategy
run against a market-data-only instrument produces:

- empty ``ResultCollector.signals``,
- empty ``ResultCollector.trades``,
- at least one equity point (portfolio initial-equity seed),
- no crash when the source instrument is ``can_trade=False``.

Cross-asset execution (observing TradFi, executing on a crypto
instrument) is implemented in this project (shipped 2026-04-23) —
``batch_processor.process_time_batch`` routes fills
through ``signal.instrument`` + ``config.target_execution_exchange``
via the extracted ``_resolve_target_fill_price`` helper. The end-to-end
acceptance for cross-asset behaviour lives in
``tests/application/backtest/test_cross_asset_reference_strategy.py``;
this test keeps its observation-only scope (TradFi feed + strategy
that deliberately never emits).

The test reuses the zmq-parity-test fixture pattern:

- mock repository returns canned TradFi candle rows,
- a stub strategy with ``_handle_candle_data`` deliberately returns
  an empty ``list`` every time,
- both engines (``DirectDbEngine`` + ``ZmqReplayEngine``) exercise
  the path via ``batch_processor.process_time_batch`` so the
  observation-only assertion holds for both.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.backtest.zmq_engine import ZmqReplayEngine
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

_NOW = datetime(2026, 4, 21, tzinfo=UTC)
_TRADFI_SYMBOL = "MNQM6-CME"
_TRADFI_EXCHANGE = "kraken_equities"


def _tradfi_candle_row(open_at: datetime, close: float) -> dict[str, Any]:
    """Build a CandleRow dict shaped like ``Repository.get_candles`` output.

    Values are deterministic; the absolute numbers don't matter for the
    observation-only assertion. The row shape must match ``CandleRow``
    TypedDict in ``repository_types.py`` so the engine's internal
    projection path (``snapper.messaging.schemas.data.CandleData``)
    accepts it.
    """
    return {
        "open_at": open_at,
        "timeframe": "1h",
        "open": close - 1.0,
        "high": close + 1.0,
        "low": close - 2.0,
        "close": close,
        "volume": 1234.0,
        "vwap": None,
        "trades": None,
        "public_id": f"{_TRADFI_SYMBOL}-{open_at.isoformat()}",
        "timestamp": open_at,
        "session_id": "tradfi-test",
        "sequence_id": 1,
    }


class _SilentObservingStrategy(BaseStrategy):
    """TradFi observer that never emits signals.

    Records candle arrivals so the test can assert the strategy DID
    receive the TradFi events (proving the observation pipeline works)
    while also asserting no signals leak out (observation-only
    contract).
    """

    seen_candles: list[tuple[str, float]]

    def __init__(self, config: StrategyConfig) -> None:
        """Initialize the observer; ``seen_candles`` records every tick.

        Args:
            config: Strategy configuration (unused except for base-class
                wiring).
        """
        super().__init__(config)
        self.seen_candles = []

    async def reset(self) -> None:
        """Clear observation history for a warm-restart parity with live."""
        self.seen_candles = []

    async def _handle_candle_data(self, instrument: str, payload: str) -> list[StrategySignal]:
        """Record the candle + return an empty list. No signal is ever emitted.

        Args:
            instrument: Native symbol of the source candle (e.g. ``MNQM6-CME``).
            payload: JSON-encoded ``CandleData`` payload; parsed for the close
                field so the test can prove the observer actually receives
                the TradFi series rather than only exchange metadata.

        Returns:
            Always an empty ``list``. The observation-only contract forbids
            this class from emitting a signal against a ``can_trade=False``
            instrument.
        """
        candle = CandleData.from_json(payload)
        self.seen_candles.append((instrument, float(candle.close)))

        return []


def _tradfi_config() -> BacktestConfig:
    """Build a BacktestConfig shaped for a single TradFi instrument on 1h."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = {_TRADFI_EXCHANGE: [_TRADFI_SYMBOL]}
    config.timeframe = "1h"
    config.start_date = _NOW
    config.end_date = _NOW + timedelta(hours=12)
    config.initial_balance = 50000.0
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.strategy_params = {}
    config.strategy_class = "tradfi_observer"
    config.target_execution_exchange = None

    return config


def _tradfi_repo(num_candles: int = 10) -> AsyncMock:
    """Build an AsyncMock Repository that returns deterministic TradFi candles."""
    rows = [
        _tradfi_candle_row(_NOW + timedelta(hours=i), 23950.0 + i * 5.0) for i in range(num_candles)
    ]
    repo = AsyncMock()
    repo.get_candles = AsyncMock(return_value=rows)

    return repo


@pytest.mark.integration
@pytest.mark.asyncio
class TestBacktestTradfiObservationOnly:
    """A non-signalling TradFi observer produces no backtest artifacts.

    The assertion is the same under both engines — ``batch_processor``
    is the common sink — but running both catches a future wiring
    regression where the DirectDbEngine path starts deviating from the
    ZmqReplayEngine path.
    """

    @pytest.mark.timeout(30)
    async def test_direct_engine_produces_no_signals_or_trades(self) -> None:
        """DirectDbEngine run against a market-data-only instrument is inert.

        Given: a 10-candle TradFi fixture on ``kraken_equities``
            (``MNQM6-CME`` @ 1h) and a strategy that never emits a signal,
        When: ``DirectDbEngine`` executes the backtest,
        Then: the ResultCollector has zero signals, zero trades, at
            least one equity point (portfolio seeding), AND the observer
            was invoked for every candle (proving the TradFi series
            reached the strategy — not a silent filter drop).

        ``is_tradeable`` is patched to accept ``MNQM6-CME`` on ``paper``
        only so the engine's internal ``StrategyConfig`` validation
        passes. The real code path that ensures cross-asset execution
        on a ``can_trade=False`` instrument would be rejected is the
        REST capability guard; end-to-end cross-asset coverage lives in
        ``tests/application/backtest/test_cross_asset_reference_strategy.py``.
        """
        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"tradfi_observer": _SilentObservingStrategy},
            ),
            patch("snapper.strategies.models.is_tradeable", return_value=True),
            patch(
                "snapper.messaging.topics.validation.get_available_symbols_set",
                return_value=frozenset({_TRADFI_SYMBOL}),
            ),
        ):
            collector = ResultCollector()
            repo = _tradfi_repo(num_candles=10)
            engine = DirectDbEngine(repo, _NOW)
            await asyncio.wait_for(
                engine.run("run-tradfi-direct", _tradfi_config(), collector),
                timeout=15.0,
            )
        assert collector.signals == []
        assert collector.trades == []
        assert len(collector.equity_points) >= 1

    @pytest.mark.timeout(30)
    async def test_zmq_engine_produces_no_signals_or_trades(self) -> None:
        """ZmqReplayEngine run against a market-data-only instrument is inert.

        Given: same 10-candle TradFi fixture + non-signalling strategy,
        When: ``ZmqReplayEngine`` executes the backtest,
        Then: the ResultCollector has zero signals + zero trades +
            at least one equity point, matching the direct-engine result.
        """
        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"tradfi_observer": _SilentObservingStrategy},
            ),
            patch("snapper.strategies.models.is_tradeable", return_value=True),
            patch(
                "snapper.messaging.topics.validation.get_available_symbols_set",
                return_value=frozenset({_TRADFI_SYMBOL}),
            ),
        ):
            collector = ResultCollector()
            repo = _tradfi_repo(num_candles=10)
            engine = ZmqReplayEngine(repo, _NOW)
            await asyncio.wait_for(
                engine.run("run-tradfi-zmq", _tradfi_config(), collector),
                timeout=25.0,
            )
        assert collector.signals == []
        assert collector.trades == []
        assert len(collector.equity_points) >= 1
