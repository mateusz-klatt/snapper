"""Tests for DirectDbEngine candle-driven simulation."""

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
        config.end_date = END

        chunks = []
        async for chunk in iter_sorted_candle_chunks(config, repo, NOW):
            chunks.append(chunk)
        assert chunks == []

    @pytest.mark.asyncio
    @patch.dict("snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", MOCK_STRATEGIES)
    async def test_single_instrument_sorted(self) -> None:
        """Candles for one instrument are sorted by open_at."""
        t1 = NOW
        t2 = NOW + timedelta(hours=1)
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=[_candle_row(t2, 101), _candle_row(t1, 100)])
        config = MagicMock(spec=BacktestConfig)
        config.instruments = {"kraken": ["BTC-USD"]}
        config.timeframe = "1h"
        config.end_date = END

        chunks = []
        async for chunk in iter_sorted_candle_chunks(config, repo, NOW):
            chunks.append(chunk)
        assert len(chunks) == 1
        assert chunks[0][0].open_at == t1
        assert chunks[0][1].open_at == t2

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
        mock_instance.on_candle = AsyncMock(return_value=None)
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
        mock_instance.on_candle = AsyncMock(return_value=warmup_signal)
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
    async def test_empty_data_produces_zero_trades(self) -> None:
        """Empty candle data produces zero artifacts."""
        mock_strategy_class = MagicMock()
        mock_instance = MagicMock()
        mock_instance.required_candle_history.return_value = 0
        mock_instance.on_candle = AsyncMock(return_value=None)
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
