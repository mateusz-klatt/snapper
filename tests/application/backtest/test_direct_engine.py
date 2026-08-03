"""Tests for DirectDbEngine candle-driven simulation."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.direct_engine import CandleEvent
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.direct_engine import candle_row_to_data
from snapper.application.backtest.direct_engine import iter_sorted_candle_chunks
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategySignal

NOW = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 1, 31, tzinfo=UTC)

MOCK_STRATEGIES: dict[str, Any] = {}


def _candle_row(
    open_at: datetime,
    close: float = 100.0,
    instrument: str = "BTC-USD",
) -> dict[str, Any]:
    """Build a minimal CandleRow dict."""
    return {
        "open_at": open_at,
        "timeframe": "1h",
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": 1000.0,
        "vwap": None,
        "trades": None,
        "public_id": f"candle-{open_at.isoformat()}",
        "timestamp": open_at,
        "session_id": "s1",
        "sequence_id": 1,
    }


class TestCandleEvent:
    """Tests for CandleEvent and candle_row_to_data."""

    def test_candle_row_to_data(self) -> None:
        """CandleEvent converts to CandleData with correct fields."""
        row = _candle_row(NOW, close=50000.0)
        event = CandleEvent(open_at=NOW, exchange="kraken", instrument="BTC-USD", row=row)
        data = candle_row_to_data(event, "1h")
        assert data.instrument == "BTC-USD"
        assert data.close == 50000.0
        assert data.timeframe == "1h"


class TestIterSortedCandleChunks:
    """Tests for iter_sorted_candle_chunks generator."""

    @pytest.mark.asyncio
    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    async def test_empty_candles_yields_nothing(self) -> None:
        """No candles in DB yields no chunks."""
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=[])
        config = MagicMock(spec=BacktestConfig)
        config.instruments = {"kraken": ["BTC-USD"]}
        config.timeframe = "1h"
        config.target_execution_exchange = None
        config.end_date = END

        chunks = []
        async for chunk in iter_sorted_candle_chunks(config, repo, NOW):
            chunks.append(chunk)
        assert chunks == []

    @pytest.mark.asyncio
    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    async def test_single_instrument_sorted(self) -> None:
        """Per-timestamp batches preserve global ascending open_at order.

        After the streaming refactor, ``iter_sorted_candle_chunks``
        yields one batch per unique timestamp instead of one giant
        sorted batch. For two
        candles at distinct timestamps that's two single-event batches,
        emitted earliest-first.
        """
        t1 = NOW
        t2 = NOW + timedelta(hours=1)
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=[_candle_row(t1, 100), _candle_row(t2, 101)])
        config = MagicMock(spec=BacktestConfig)
        config.instruments = {"kraken": ["BTC-USD"]}
        config.timeframe = "1h"
        config.target_execution_exchange = None
        config.end_date = END

        chunks = []
        async for chunk in iter_sorted_candle_chunks(config, repo, NOW):
            chunks.append(chunk)
        assert len(chunks) == 2
        assert len(chunks[0]) == 1
        assert chunks[0][0].open_at == t1
        assert len(chunks[1]) == 1
        assert chunks[1][0].open_at == t2

    @pytest.mark.asyncio
    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    async def test_multi_instrument_interleaved(self) -> None:
        """Candles from multiple instruments are interleaved by time."""
        t1 = NOW
        repo = AsyncMock()
        repo.get_candles = AsyncMock(
            side_effect=[
                [_candle_row(t1, 100)],
                [_candle_row(t1, 200)],
            ]
        )
        config = MagicMock(spec=BacktestConfig)
        config.instruments = {"kraken": ["BTC-USD", "ETH-USD"]}
        config.timeframe = "1h"
        config.target_execution_exchange = None
        config.end_date = END

        chunks = []
        async for chunk in iter_sorted_candle_chunks(config, repo, NOW):
            chunks.append(chunk)
        assert len(chunks) == 1
        assert len(chunks[0]) == 2
        assert chunks[0][0].instrument == "BTC-USD"
        assert chunks[0][1].instrument == "ETH-USD"


class TestDirectDbEngine:
    """Tests for DirectDbEngine.run simulation loop."""

    @pytest.mark.asyncio
    @patch.dict(
        "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
        {"test_strategy": MagicMock},
    )
    async def test_engine_runs_with_no_signals(self) -> None:
        """Engine with no-signal strategy produces zero trades."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_instance.required_candle_history.return_value = 0
        mock_strategy_class.return_value = mock_instance

        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"test_strategy": mock_strategy_class},
        ):
            repo = AsyncMock()
            t1 = NOW + timedelta(hours=1)
            repo.get_candles = AsyncMock(return_value=[_candle_row(t1, 100)])

            config = MagicMock(spec=BacktestConfig)
            config.strategy_class = "test_strategy"
            config.instruments = {"kraken": ["BTC-USD"]}
            config.timeframe = "1h"
            config.target_execution_exchange = None
            config.start_date = NOW
            config.end_date = END
            config.initial_balance = 10000.0
            config.slippage_bps = 0.0
            config.commission_bps = 0.0
            config.strategy_params = {}

            engine = DirectDbEngine(repo, NOW)
            collector = ResultCollector()
            portfolio, closes = await engine.run("run-1", config, collector)

            assert len(collector.trades) == 0
            assert len(collector.signals) == 0
            assert len(collector.equity_points) == 1
            assert portfolio.cash == 10000.0
            assert closes["BTC-USD"] == 100.0

    @pytest.mark.asyncio
    async def test_engine_skips_warmup_signals(self) -> None:
        """Signals during warm-up period are not recorded."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0

        warmup_signal = StrategySignal(
            instrument="BTC-USD", side="buy", strength=1.0, reason="test", price=100.0
        )
        mock_instance._handle_candle_data = AsyncMock(return_value=[warmup_signal])
        mock_strategy_class.return_value = mock_instance

        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"test_strategy": mock_strategy_class},
        ):
            repo = AsyncMock()
            before_start = NOW - timedelta(hours=1)
            repo.get_candles = AsyncMock(return_value=[_candle_row(before_start, 100)])

            config = MagicMock(spec=BacktestConfig)
            config.strategy_class = "test_strategy"
            config.instruments = {"kraken": ["BTC-USD"]}
            config.timeframe = "1h"
            config.target_execution_exchange = None
            config.start_date = NOW
            config.end_date = END
            config.initial_balance = 10000.0
            config.slippage_bps = 0.0
            config.commission_bps = 0.0
            config.strategy_params = {}

            engine = DirectDbEngine(repo, NOW)
            collector = ResultCollector()
            await engine.run("run-1", config, collector)

            assert len(collector.signals) == 0
            assert len(collector.trades) == 0

    @pytest.mark.asyncio
    async def test_engine_processes_signals_and_fills(self) -> None:
        """Engine with buy signal produces trade + signal + equity."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0

        buy_signal = StrategySignal(
            instrument="BTC-USD", side="buy", strength=1.0, reason="test_buy", price=100.0
        )
        mock_instance._handle_candle_data = AsyncMock(return_value=[buy_signal])
        mock_strategy_class.return_value = mock_instance

        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"test_strategy": mock_strategy_class},
        ):
            repo = AsyncMock()
            t1 = NOW + timedelta(hours=1)
            repo.get_candles = AsyncMock(return_value=[_candle_row(t1, 100)])

            config = MagicMock(spec=BacktestConfig)
            config.strategy_class = "test_strategy"
            config.instruments = {"kraken": ["BTC-USD"]}
            config.timeframe = "1h"
            config.target_execution_exchange = None
            config.start_date = NOW
            config.end_date = END
            config.initial_balance = 10000.0
            config.slippage_bps = 0.0
            config.commission_bps = 0.0
            config.strategy_params = {}

            engine = DirectDbEngine(repo, NOW)
            collector = ResultCollector()
            portfolio, closes = await engine.run("run-1", config, collector)

            assert len(collector.signals) == 1
            assert collector.signals[0]["signal_type"] == "buy"
            assert len(collector.trades) == 1
            assert collector.trades[0]["side"] == "buy"
            assert len(collector.equity_points) == 1
            assert collector.trades[0]["signal_public_id"] == collector.signals[0]["public_id"]

    @pytest.mark.asyncio
    async def test_engine_multi_timestamp_batching(self) -> None:
        """Engine processes candles at different timestamps in separate batches."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"test_strategy": mock_strategy_class},
        ):
            repo = AsyncMock()
            t1 = NOW + timedelta(hours=1)
            t2 = NOW + timedelta(hours=2)
            repo.get_candles = AsyncMock(return_value=[_candle_row(t1, 100), _candle_row(t2, 110)])

            config = MagicMock(spec=BacktestConfig)
            config.strategy_class = "test_strategy"
            config.instruments = {"kraken": ["BTC-USD"]}
            config.timeframe = "1h"
            config.target_execution_exchange = None
            config.start_date = NOW
            config.end_date = END
            config.initial_balance = 10000.0
            config.slippage_bps = 0.0
            config.commission_bps = 0.0
            config.strategy_params = {}

            engine = DirectDbEngine(repo, NOW)
            collector = ResultCollector()
            portfolio, closes = await engine.run("run-1", config, collector)

            assert len(collector.equity_points) == 2
            assert closes["BTC-USD"] == 110.0

    @pytest.mark.asyncio
    async def test_engine_runs_across_multiple_chunks(self) -> None:
        """Engine continues processing when the candle iterator yields multiple chunks."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        async def fake_iter_sorted_candle_chunks(
            _config: BacktestConfig,
            _repository: object,
            _snapshot_as_of: datetime,
        ) -> AsyncIterator[list[CandleEvent]]:
            yield []
            yield [
                CandleEvent(
                    open_at=NOW + timedelta(hours=1),
                    exchange="kraken",
                    instrument="BTC-USD",
                    row=_candle_row(NOW + timedelta(hours=1), 100.0),
                )
            ]
            yield [
                CandleEvent(
                    open_at=NOW + timedelta(hours=2),
                    exchange="kraken",
                    instrument="BTC-USD",
                    row=_candle_row(NOW + timedelta(hours=2), 105.0),
                )
            ]

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"test_strategy": mock_strategy_class},
            ),
            patch(
                "snapper.application.backtest.direct_engine.iter_sorted_candle_chunks",
                fake_iter_sorted_candle_chunks,
            ),
        ):
            repo = AsyncMock()

            config = MagicMock(spec=BacktestConfig)
            config.strategy_class = "test_strategy"
            config.instruments = {"kraken": ["BTC-USD"]}
            config.timeframe = "1h"
            config.target_execution_exchange = None
            config.start_date = NOW
            config.end_date = END
            config.initial_balance = 10000.0
            config.slippage_bps = 0.0
            config.commission_bps = 0.0
            config.strategy_params = {}

            engine = DirectDbEngine(repo, NOW)
            collector = ResultCollector()
            portfolio, closes = await engine.run("run-1", config, collector)

            assert mock_instance._handle_candle_data.await_count == 2
            assert len(collector.equity_points) == 2
            assert portfolio.cash == 10000.0
            assert closes["BTC-USD"] == 105.0

    @pytest.mark.asyncio
    async def test_process_time_batch_records_signal_when_fill_is_skipped(self) -> None:
        """Signal artifacts are kept even when no executable fill is produced."""
        strategy = MagicMock(spec=BaseStrategy)
        strategy._handle_candle_data = AsyncMock(
            return_value=[
                StrategySignal(
                    instrument="BTC-USD",
                    side="sell",
                    strength=1.0,
                    reason="close_without_position",
                    price=100.0,
                )
            ]
        )

        config = MagicMock(spec=BacktestConfig)
        config.timeframe = "1h"
        config.target_execution_exchange = None
        config.start_date = NOW
        config.slippage_bps = 0.0
        config.commission_bps = 0.0

        engine = DirectDbEngine(AsyncMock(), NOW)
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10000.0)
        latest_closes: dict[str, float] = {}
        tracker = SequenceTracker()
        batch = [
            CandleEvent(
                open_at=NOW + timedelta(hours=1),
                exchange="kraken",
                instrument="BTC-USD",
                row=_candle_row(NOW + timedelta(hours=1), 100.0),
            )
        ]

        await engine._process_time_batch(
            batch,
            "run-1",
            config,
            strategy,
            portfolio,
            latest_closes,
            collector,
            tracker,
        )

        assert len(collector.signals) == 1
        assert collector.signals[0]["signal_type"] == "sell"
        assert collector.signals[0]["public_id"]
        assert len(collector.trades) == 0
        assert len(collector.equity_points) == 1
        assert portfolio.cash == 10000.0

    @pytest.mark.asyncio
    async def test_empty_data_produces_zero_trades(self) -> None:
        """Empty candle data produces zero artifacts."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"test_strategy": mock_strategy_class},
        ):
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[])

            config = MagicMock(spec=BacktestConfig)
            config.strategy_class = "test_strategy"
            config.instruments = {"kraken": ["BTC-USD"]}
            config.timeframe = "1h"
            config.target_execution_exchange = None
            config.start_date = NOW
            config.end_date = END
            config.initial_balance = 10000.0
            config.strategy_params = {}

            engine = DirectDbEngine(repo, NOW)
            collector = ResultCollector()
            portfolio, closes = await engine.run("run-1", config, collector)

            assert len(collector.trades) == 0
            assert len(collector.equity_points) == 0
            assert portfolio.cash == 10000.0


class TestCooperativeCancel:
    """Cooperative cancellation between time-batches via bt_repo polling."""

    def _build_config(self) -> MagicMock:
        config = MagicMock(spec=BacktestConfig)
        config.strategy_class = "test_strategy"
        config.instruments = {"kraken": ["BTC-USD"]}
        config.timeframe = "1h"
        config.target_execution_exchange = None
        config.start_date = NOW
        config.end_date = END
        config.initial_balance = 10000.0
        config.slippage_bps = 0.0
        config.commission_bps = 0.0
        config.strategy_params = {}
        return config

    def _patch_chunks(self, count: int) -> tuple[Any, list[CandleEvent]]:
        events = [
            CandleEvent(
                open_at=NOW + timedelta(hours=i + 1),
                exchange="kraken",
                instrument="BTC-USD",
                row=_candle_row(NOW + timedelta(hours=i + 1), 100.0 + i),
            )
            for i in range(count)
        ]

        async def fake_iter(
            _config: BacktestConfig,
            _repository: object,
            _snapshot_as_of: datetime,
        ) -> AsyncIterator[list[CandleEvent]]:
            for ev in events:
                yield [ev]

        return fake_iter, events

    @pytest.mark.asyncio
    async def test_cancel_requested_raises_cancelled_error_before_first_batch(self) -> None:
        """cancel_requested set BEFORE the first batch raises before any candle work."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        fake_iter, _ = self._patch_chunks(5)
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": "cancel_requested"})

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"test_strategy": mock_strategy_class},
            ),
            patch(
                "snapper.application.backtest.direct_engine.iter_sorted_candle_chunks",
                fake_iter,
            ),
        ):
            repo = AsyncMock()
            engine = DirectDbEngine(repo, NOW, bt_repo=bt_repo, cancel_poll_ms=0)
            collector = ResultCollector()
            run_config = self._build_config()
            with pytest.raises(asyncio.CancelledError):
                await engine.run("run-1", run_config, collector)
            assert bt_repo.get_run.await_count == 1
            assert mock_instance._handle_candle_data.await_count == 0

    @pytest.mark.asyncio
    async def test_probe_timeout_skips_without_failing_run(self) -> None:
        """Slow get_run is skipped via wait_for and the engine continues."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        fake_iter, events = self._patch_chunks(2)

        async def hanging_get_run(*_args: object, **_kwargs: object) -> object:
            await asyncio.sleep(60)
            return {"status": "running"}

        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(side_effect=hanging_get_run)

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"test_strategy": mock_strategy_class},
            ),
            patch(
                "snapper.application.backtest.direct_engine.iter_sorted_candle_chunks",
                fake_iter,
            ),
            patch(
                "snapper.application.backtest.direct_engine._CANCEL_PROBE_TIMEOUT_S",
                0.05,
            ),
        ):
            repo = AsyncMock()
            engine = DirectDbEngine(repo, NOW, bt_repo=bt_repo, cancel_poll_ms=0)
            collector = ResultCollector()
            await engine.run("run-1", self._build_config(), collector)
            assert mock_instance._handle_candle_data.await_count == len(events)

    @pytest.mark.asyncio
    async def test_probe_timeout_helper_returns_without_cancelling(self) -> None:
        """The direct timeout branch on _maybe_check_cancel returns cleanly."""

        async def hanging_get_run(*_args: object, **_kwargs: object) -> object:
            await asyncio.sleep(60)
            return {"status": "running"}

        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(side_effect=hanging_get_run)
        engine = DirectDbEngine(AsyncMock(), NOW, bt_repo=bt_repo, cancel_poll_ms=0)

        with patch("snapper.application.backtest.direct_engine._CANCEL_PROBE_TIMEOUT_S", 0.01):
            await engine._maybe_check_cancel("run-1")

        assert bt_repo.get_run.await_count == 1

    @pytest.mark.asyncio
    async def test_cancel_probe_short_circuits_repository_polling(self) -> None:
        """An injected CancelProbe bypasses repository polling inside _maybe_check_cancel."""
        cancel_probe = AsyncMock()
        bt_repo = AsyncMock()
        engine = DirectDbEngine(
            AsyncMock(),
            NOW,
            bt_repo=bt_repo,
            cancel_poll_ms=0,
            cancel_probe=cancel_probe,
        )

        await engine._maybe_check_cancel("run-1")

        cancel_probe.check.assert_awaited_once()
        bt_repo.get_run.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_polling_when_bt_repo_is_none(self) -> None:
        """Without bt_repo, engine never polls and runs to completion (CLI path)."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        fake_iter, events = self._patch_chunks(3)

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"test_strategy": mock_strategy_class},
            ),
            patch(
                "snapper.application.backtest.direct_engine.iter_sorted_candle_chunks",
                fake_iter,
            ),
        ):
            repo = AsyncMock()
            engine = DirectDbEngine(repo, NOW, bt_repo=None)
            collector = ResultCollector()
            _, _ = await engine.run("run-1", self._build_config(), collector)
            assert mock_instance._handle_candle_data.await_count == len(events)

    @pytest.mark.asyncio
    async def test_running_status_does_not_cancel(self) -> None:
        """get_run returning 'running' allows the engine to finish normally."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        fake_iter, events = self._patch_chunks(3)
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": "running"})

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"test_strategy": mock_strategy_class},
            ),
            patch(
                "snapper.application.backtest.direct_engine.iter_sorted_candle_chunks",
                fake_iter,
            ),
        ):
            repo = AsyncMock()
            engine = DirectDbEngine(repo, NOW, bt_repo=bt_repo, cancel_poll_ms=0)
            collector = ResultCollector()
            await engine.run("run-1", self._build_config(), collector)
            assert mock_instance._handle_candle_data.await_count == len(events)

    @pytest.mark.asyncio
    async def test_poll_interval_throttles_probes(self) -> None:
        """High cancel_poll_ms suppresses repeated DB probes within the window."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock(spec=BaseStrategy)
        mock_instance.required_candle_history.return_value = 0
        mock_instance._handle_candle_data = AsyncMock(return_value=[])
        mock_strategy_class.return_value = mock_instance

        fake_iter, events = self._patch_chunks(5)
        bt_repo = AsyncMock()
        bt_repo.get_run = AsyncMock(return_value={"status": "running"})

        with (
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"test_strategy": mock_strategy_class},
            ),
            patch(
                "snapper.application.backtest.direct_engine.iter_sorted_candle_chunks",
                fake_iter,
            ),
        ):
            repo = AsyncMock()
            engine = DirectDbEngine(repo, NOW, bt_repo=bt_repo, cancel_poll_ms=60_000)
            collector = ResultCollector()
            await engine.run("run-1", self._build_config(), collector)
            assert bt_repo.get_run.await_count == 1
            assert mock_instance._handle_candle_data.await_count == len(events)
