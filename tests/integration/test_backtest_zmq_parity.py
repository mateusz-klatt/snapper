"""End-to-end parity: DirectDbEngine vs ZmqReplayEngine produce identical artifacts.

Uses a mock repository serving canned candle rows and a deterministic stub
strategy registered against ``StrategyFactory.STRATEGY_CLASSES``. The
intent is to verify the ZmqReplayEngine wiring delivers candles into the
same ``process_time_batch`` semantics as DirectDbEngine — DB plumbing is
exercised by separate integration tests.
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
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _candle_row(open_at: datetime, close: float, instrument: str = "BTC-USD") -> dict[str, Any]:
    """Build a CandleRow dict with deterministic values."""
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
        "public_id": f"{instrument}-{open_at.isoformat()}",
        "timestamp": open_at,
        "session_id": "s1",
        "sequence_id": 1,
    }


class _DeterministicStrategy(BaseStrategy):
    """Buy at candle 5 and 10 (post-warmup); never emit otherwise."""

    candle_count: dict[str, int]

    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        self.candle_count = {}

    async def reset(self) -> None:
        """Clear per-instrument counters."""
        self.candle_count = {}

    async def _handle_candle_data(self, instrument: str, payload: str) -> StrategySignal | None:
        """Emit a buy on the 5th and 10th candle for the given instrument."""
        self.candle_count[instrument] = self.candle_count.get(instrument, 0) + 1
        idx = self.candle_count[instrument]
        if idx in (5, 10):
            return StrategySignal(
                instrument=instrument,
                side="buy",
                strength=1.0,
                reason="parity-test",
                price=100.0 + idx,
            )
        return None


def _make_config() -> BacktestConfig:
    """Build a BacktestConfig MagicMock matching what both engines inspect."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = {"kraken": ["BTC-USD"]}
    config.timeframe = "1h"
    config.start_date = NOW
    config.end_date = NOW + timedelta(hours=20)
    config.initial_balance = 10000.0
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.strategy_params = {}
    config.strategy_class = "parity_stub"
    return config


def _make_repo(num_candles: int = 12) -> AsyncMock:
    """Build a repository AsyncMock returning ``num_candles`` BTC candles."""
    rows = [_candle_row(NOW + timedelta(hours=i), 100.0 + i) for i in range(num_candles)]
    repo = AsyncMock()
    repo.get_candles = AsyncMock(return_value=rows)
    return repo


def _summarise(collector: ResultCollector) -> dict[str, Any]:
    """Project a collector's artifacts down to the fields parity checks compare."""
    return {
        "signals": [
            (s["signal_time"], s["signal_type"], s["instrument"], s["price"])
            for s in collector.signals
        ],
        "trades": [
            (t["executed_at"], t["side"], t["quantity"], t["price"], t["position_after"])
            for t in collector.trades
        ],
        "equity_points": [(p["point_time"], p["equity"]) for p in collector.equity_points],
        "signal_trade_link": [
            (s["public_id"], t["signal_public_id"])
            for s, t in zip(collector.signals, collector.trades, strict=False)
        ],
    }


@pytest.mark.asyncio
class TestEnginesParity:
    """Both engines produce field-for-field identical artifacts."""

    @pytest.mark.timeout(30)
    async def test_direct_db_and_zmq_replay_match(self) -> None:
        """20-candle BTC fixture: signals/trades/equity match between engines."""
        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"parity_stub": _DeterministicStrategy},
        ):
            direct_collector = ResultCollector()
            direct_repo = _make_repo()
            direct_engine = DirectDbEngine(direct_repo, NOW)
            await asyncio.wait_for(
                direct_engine.run("run-direct", _make_config(), direct_collector),
                timeout=10.0,
            )

            zmq_collector = ResultCollector()
            zmq_repo = _make_repo()
            zmq_engine = ZmqReplayEngine(zmq_repo, NOW)
            await asyncio.wait_for(
                zmq_engine.run("run-zmq", _make_config(), zmq_collector),
                timeout=20.0,
            )

        direct = _summarise(direct_collector)
        zmq = _summarise(zmq_collector)
        assert direct["signals"] == zmq["signals"]
        assert direct["trades"] == zmq["trades"]
        assert direct["equity_points"] == zmq["equity_points"]
        for d_pid, d_link in direct["signal_trade_link"]:
            assert d_pid == d_link
        for z_pid, z_link in zmq["signal_trade_link"]:
            assert z_pid == z_link

    @pytest.mark.timeout(15)
    async def test_zmq_replay_empty_candles_returns_empty_artifacts(self) -> None:
        """ZmqReplayEngine with no candles produces no signals/trades/equity."""
        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"parity_stub": _DeterministicStrategy},
        ):
            collector = ResultCollector()
            repo = _make_repo(num_candles=0)
            engine = ZmqReplayEngine(repo, NOW)
            await asyncio.wait_for(
                engine.run("run-empty", _make_config(), collector),
                timeout=10.0,
            )
        assert collector.signals == []
        assert collector.trades == []
        assert collector.equity_points == []
