"""Unit tests for ZmqReplayEngine — fast-fail + cleanup invariants."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.backtest.zmq_engine import ZmqReplayEngine

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _make_config_with_instruments(instruments: dict[str, list[str]]) -> BacktestConfig:
    """Build a BacktestConfig MagicMock with caller-supplied instruments."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = instruments
    config.timeframe = "1h"
    config.start_date = NOW
    config.end_date = NOW
    config.initial_balance = 10000.0
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.strategy_params = {}
    config.strategy_class = "macd"
    return config


@pytest.mark.asyncio
class TestZmqReplayEngineFastFail:
    """Empty-instrument config rejected before any broker is allocated."""

    @pytest.mark.timeout(10)
    async def test_run_raises_for_empty_instruments_dict(self) -> None:
        """Empty dict → ValueError, no broker consumed."""
        engine = ZmqReplayEngine(AsyncMock(), NOW)
        config = _make_config_with_instruments({})
        with pytest.raises(ValueError, match="empty"):
            await engine.run("run-1", config, ResultCollector())

    @pytest.mark.timeout(10)
    async def test_run_raises_for_empty_instrument_lists(self) -> None:
        """Dict with empty value lists also fast-fails."""
        engine = ZmqReplayEngine(AsyncMock(), NOW)
        config = _make_config_with_instruments({"kraken": []})
        with pytest.raises(ValueError, match="empty"):
            await engine.run("run-1", config, ResultCollector())
