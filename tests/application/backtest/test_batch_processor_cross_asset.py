"""Cross-asset attribution tests for batch_processor.process_time_batch.

Locks the BE-1 contract: ``signal.instrument`` drives the recorded
target instrument and ``config.target_execution_exchange`` (when set)
drives the recorded target venue, with byte-identical fallback to
``event.exchange`` / ``event.instrument`` when the carrier is None or
``signal.instrument == event.instrument`` (single-feed legacy path).
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.application.backtest.batch_processor as batch_processor_module
from snapper.application.backtest.batch_processor import CandleEvent
from snapper.application.backtest.batch_processor import process_time_batch
from snapper.application.backtest.batch_processor import simulate_market_fill
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.core.types import TradeSideEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategySignal

NOW = datetime(2026, 4, 22, 12, 0, 0, tzinfo=UTC)
_BUS_TIME = NOW


def _candle_row(
    open_at: datetime,
    close: float = 100.0,
    seq: int = 1,
) -> dict[str, Any]:
    """Build a minimal CandleRow dict."""
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
        "session_id": "s1",
        "sequence_id": seq,
    }


def _config(
    *,
    target_execution_exchange: str | None = None,
    start_date: datetime = NOW,
) -> MagicMock:
    """Build a MagicMock BacktestConfig with the carrier field set."""
    config = MagicMock(spec=BacktestConfig)
    config.timeframe = "1h"
    config.start_date = start_date
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.target_execution_exchange = target_execution_exchange
    return config


def _strategy(signal: StrategySignal | None) -> MagicMock:
    """Stub strategy returning ``signal`` on every candle."""
    strategy = MagicMock(spec=BaseStrategy)
    strategy._handle_candle_data = AsyncMock(return_value=signal)
    return strategy


def _event(
    *,
    source_exchange: str = "kraken_equities",
    source_instrument: str = "MNQU6-CME",
    open_at: datetime = NOW + timedelta(hours=1),
    close: float = 100.0,
) -> CandleEvent:
    """Build a CandleEvent on the source feed."""
    return CandleEvent(
        open_at=open_at,
        exchange=source_exchange,
        instrument=source_instrument,
        row=_candle_row(open_at=open_at, close=close, seq=1),
    )


class TestTargetAttribution:
    """BE-1 target-exchange + target-instrument substitution at fill time."""

    @pytest.mark.asyncio
    async def test_signal_instrument_overrides_event_at_fill(self) -> None:
        """simulate_market_fill receives signal.instrument, not event.instrument.

        Given: a cross-asset event (source MNQU6-CME/kraken_equities),
            a strategy emitting BUY on BTC-USD, BTC-USD close already
            seeded in latest_closes, and target_execution_exchange=kraken,
        When: process_time_batch runs,
        Then: the recorded trade carries instrument=BTC-USD (target)
            and exchange=kraken — NOT MNQU6-CME / kraken_equities.
        """
        signal = StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="cross_asset_buy",
            price=95000.0,
        )
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10_000.0)
        latest_closes: dict[str, float] = {"BTC-USD": 42_000.0}
        await process_time_batch(
            batch=[_event()],
            run_public_id="run-1",
            config=_config(target_execution_exchange="kraken"),
            strategy=_strategy(signal),
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            tracker=SequenceTracker(),
            snapshot_as_of=_BUS_TIME,
        )
        assert len(collector.trades) == 1
        assert collector.trades[0]["instrument"] == "BTC-USD"
        assert portfolio.positions.keys() == {"BTC-USD"}

    @pytest.mark.asyncio
    async def test_target_exchange_defaults_to_event_exchange_when_config_none(
        self,
    ) -> None:
        """Target exchange falls back to event.exchange when carrier is None.

        Given: a single-feed event (BTC-USD/kraken), BUY signal on BTC-USD,
            and target_execution_exchange=None,
        When: process_time_batch runs,
        Then: the fill is attributed to the source candle's exchange —
            simulate_market_fill invoked with ``exchange='kraken'``
            (spy-captured). Closes the : the prior
            assertion only checked the recorded instrument, so a
            venue-attribution regression could have slipped through.
        """
        signal = StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="single_feed",
            price=100.0,
        )
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10_000.0)
        latest_closes: dict[str, float] = {}
        fill_calls: list[dict[str, Any]] = []

        def _capture(*args: object, **kwargs: object) -> object:
            fill_calls.append(dict(kwargs))
            return simulate_market_fill(*args, **kwargs)

        with patch.object(batch_processor_module, "simulate_market_fill", side_effect=_capture):
            await process_time_batch(
                batch=[_event(source_exchange="kraken", source_instrument="BTC-USD")],
                run_public_id="run-1",
                config=_config(target_execution_exchange=None),
                strategy=_strategy(signal),
                portfolio=portfolio,
                latest_closes=latest_closes,
                collector=collector,
                tracker=SequenceTracker(),
                snapshot_as_of=_BUS_TIME,
            )
        assert len(collector.trades) == 1
        assert collector.trades[0]["instrument"] == "BTC-USD"
        assert len(fill_calls) == 1
        assert fill_calls[0]["exchange"] == "kraken"
        assert fill_calls[0]["instrument"] == "BTC-USD"

    @pytest.mark.asyncio
    async def test_missing_target_close_blocks_fill_but_records_signal(self) -> None:
        """Missing target close → no trade + counter bumped, signal still persisted.

        Given: a BUY signal on BTC-USD with an empty latest_closes map,
        When: process_time_batch runs,
        Then: simulate_market_fill is never invoked (assert_not_called
            pins the short-circuit contract), cross_asset_blocked_fills
            == 1, a signal row is persisted with signal.price as the
            recorded price (source-close fallback), and the recorded
            instrument is the target (BTC-USD), not the source
            (MNQU6-CME).
        """
        signal = StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="warmup_overlap",
            price=95000.0,
        )
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10_000.0)
        with patch.object(batch_processor_module, "simulate_market_fill") as mock_fill:
            await process_time_batch(
                batch=[_event()],
                run_public_id="run-1",
                config=_config(target_execution_exchange="kraken"),
                strategy=_strategy(signal),
                portfolio=portfolio,
                latest_closes={},
                collector=collector,
                tracker=SequenceTracker(),
                snapshot_as_of=_BUS_TIME,
            )
        mock_fill.assert_not_called()
        assert collector.trades == []
        assert collector.cross_asset_blocked_fills == 1
        assert len(collector.signals) == 1
        row = collector.signals[0]
        assert row["instrument"] == "BTC-USD"
        assert row["price"] == 95000.0

    @pytest.mark.asyncio
    async def test_happy_path_record_signal_uses_target_close(self) -> None:
        """Happy-path signal row carries target_close as price, target as instrument.

        Given: latest_closes[BTC-USD]=42000.0, strategy emits BUY/BTC-USD
            with signal.price=95000.0 (source-feed close), target_execution_exchange=kraken,
        When: process_time_batch runs,
        Then:
            * the recorded signal's price == 42000.0 (target close from
              latest_closes, NOT signal.price, NOT event.row['close']) and
              instrument == 'BTC-USD',
            * the trade row's signal_public_id == signal row's public_id
              (FK-style linkage preserved across the b4f2c9b helper
              extraction — ),
            * the trade sequence_id < signal sequence_id (trade-then-
              signal ordering maintained under _process_signal / _resolve_
              target_fill_price — ).
        """
        signal = StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="cross_asset_buy",
            price=95000.0,
        )
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10_000.0)
        latest_closes: dict[str, float] = {"BTC-USD": 42_000.0}
        await process_time_batch(
            batch=[_event(close=100.0)],
            run_public_id="run-1",
            config=_config(target_execution_exchange="kraken"),
            strategy=_strategy(signal),
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            tracker=SequenceTracker(),
            snapshot_as_of=_BUS_TIME,
        )
        assert len(collector.signals) == 1
        assert len(collector.trades) == 1
        sig_row = collector.signals[0]
        trade_row = collector.trades[0]
        assert sig_row["instrument"] == "BTC-USD"
        assert sig_row["price"] == pytest.approx(42_000.0)
        assert trade_row["signal_public_id"] == sig_row["public_id"]
        assert trade_row["sequence_id"] < sig_row["sequence_id"]

    @pytest.mark.asyncio
    async def test_single_feed_byte_identical_preserves_source_attribution(
        self,
    ) -> None:
        """Single-feed (signal.instrument == event.instrument) byte-identical path.

        Given: single-feed event (BTC-USD/kraken), BUY signal on BTC-USD,
            target_execution_exchange=None,
        When: process_time_batch runs,
        Then: the recorded signal + trade rows match the pre-v1.2
            behaviour — price equals event.row['close'] (populated into
            latest_closes by the pre-batch scan), and the counter stays
            at 0 (no block).
        """
        signal = StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="single_feed_phase2c",
            price=999.0,
        )
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10_000.0)
        latest_closes: dict[str, float] = {}
        await process_time_batch(
            batch=[_event(source_exchange="kraken", source_instrument="BTC-USD", close=100.0)],
            run_public_id="run-1",
            config=_config(target_execution_exchange=None),
            strategy=_strategy(signal),
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            tracker=SequenceTracker(),
            snapshot_as_of=_BUS_TIME,
        )
        assert collector.cross_asset_blocked_fills == 0
        assert len(collector.signals) == 1
        assert collector.signals[0]["instrument"] == "BTC-USD"
        assert collector.signals[0]["price"] == 100.0
        assert len(collector.trades) == 1
        assert collector.trades[0]["instrument"] == "BTC-USD"


class TestPairedSignalDrain:
    """``process_time_batch`` drains paired signals queued by the strategy."""

    @pytest.mark.asyncio
    async def test_paired_signals_are_processed_alongside_primary(self) -> None:
        """Drained paired signals reach the fill simulation.

        Given: a strategy whose ``_handle_candle_data`` returns a primary
            BUY signal on BTC-USD AND queues a paired SELL signal on
            ETH-USD via ``drain_pending_signals`` (the contract used by
            CointegrationPairs after the Bug L fix),
        When: ``process_time_batch`` runs over a single candle event,
        Then: both signals get fills — the trades collection contains
            one BTC-USD entry and one ETH-USD entry.
        """
        primary = StrategySignal(
            instrument="BTC-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="paired-entry-primary",
            price=42_000.0,
        )
        partner = StrategySignal(
            instrument="ETH-USD",
            side=TradeSideEnum.BUY,
            strength=1.0,
            reason="paired-entry-partner",
            price=2_500.0,
        )
        strategy = MagicMock(spec=BaseStrategy)
        strategy._handle_candle_data = AsyncMock(return_value=primary)
        strategy.drain_pending_signals = MagicMock(side_effect=[[partner], []])

        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10_000.0)
        latest_closes: dict[str, float] = {"BTC-USD": 42_000.0, "ETH-USD": 2_500.0}
        await process_time_batch(
            batch=[
                _event(
                    source_exchange="kraken",
                    source_instrument="BTC-USD",
                    close=42_000.0,
                )
            ],
            run_public_id="run-paired",
            config=_config(target_execution_exchange="kraken"),
            strategy=strategy,
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            tracker=SequenceTracker(),
            snapshot_as_of=_BUS_TIME,
        )

        instruments = {trade["instrument"] for trade in collector.trades}
        assert instruments == {"BTC-USD", "ETH-USD"}
        assert len(collector.signals) == 2
