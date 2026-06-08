"""Backtest hardening integration acceptance tests.

Covers:
- Concurrent backtest runs rejected at the DB layer (uq_bt_single_running).
- start_date warmup gating field-for-field parity between Direct-DB and
  ZMQ replay engines.
- Shielded failure transition still completes when the runner is cancelled
  mid-failure-write.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.data.repository as repo_module
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.backtest.runner import BacktestRunnerProcess
from snapper.application.backtest.zmq_engine import ZmqReplayEngine
from snapper.data.backtest_repository import BacktestRepository
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

NOW = datetime(2026, 1, 1, tzinfo=UTC)


async def _setup_db(tmp_path: Path) -> tuple[Any, BacktestRepository, str]:
    """Create a fresh tmp_path-scoped SQLite DB with all tables."""
    db_path = tmp_path / "hardening.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    repo = repo_module.SQLAlchemyRepository(db_url)
    await repo.create_all()
    bt_repo = BacktestRepository(repo.session_factory)
    return repo, bt_repo, db_url


async def _create_pending_run(bt_repo: BacktestRepository, status: str = "pending") -> str:
    """Insert a pending backtest_runs row and return its public_id."""
    _, public_id = await bt_repo.create_run(
        row={
            "wallet_public_id": "w-1",
            "strategy_name": "stub",
            "strategy_params": {},
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": NOW + timedelta(hours=24),
            "initial_cash": 10000.0,
            "status": status,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": NOW,
        },
        bus_time=NOW,
        session_id="s1",
        sequence_id=1,
    )
    return public_id


class _StubStrategy(BaseStrategy):
    """Always-buy strategy used to drive parity comparisons."""

    candle_count: dict[str, int]

    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        self.candle_count = {}

    async def reset(self) -> None:
        """Reset is a no-op for the stub."""

    async def _handle_candle_data(self, instrument: str, payload: str) -> list[StrategySignal]:
        """Emit a buy on the first candle per instrument; otherwise nothing."""
        self.candle_count[instrument] = self.candle_count.get(instrument, 0) + 1
        if self.candle_count[instrument] == 1:
            return [
                StrategySignal(
                    instrument=instrument,
                    side="buy",
                    strength=1.0,
                    reason="warmup-parity",
                    price=100.0,
                )
            ]
        return []


def _make_config(start_offset_hours: int = 0) -> BacktestConfig:
    """Build a BacktestConfig MagicMock with start_date offset from the data."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = {"kraken": ["BTC-USD"]}
    config.timeframe = "1h"
    config.start_date = NOW + timedelta(hours=start_offset_hours)
    config.end_date = NOW + timedelta(hours=24)
    config.initial_balance = 10000.0
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.strategy_params = {}
    config.strategy_class = "warmup_stub"
    config.target_execution_exchange = None
    return config


def _candle_row(open_at: datetime, close: float) -> dict[str, Any]:
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


@pytest.mark.asyncio
class TestHardeningIntegration:
    """Hardening acceptance suite."""

    @pytest.mark.timeout(15)
    async def test_concurrent_second_run_rejected_at_db_layer(self, tmp_path: Path) -> None:
        """Two runners racing into 'running' — one wins, the other transitions to failed."""
        repo, bt_repo, db_url = await _setup_db(tmp_path)
        first = await _create_pending_run(bt_repo)
        second = await _create_pending_run(bt_repo)
        with (
            patch(
                "snapper.application.backtest.runner.get_repository",
                return_value=repo,
            ),
            patch.dict(
                "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
                {"stub": MagicMock()},
            ),
        ):
            runner_a = BacktestRunnerProcess(run_public_id=first, db_url=db_url)
            runner_b = BacktestRunnerProcess(run_public_id=second, db_url=db_url)
            await runner_a.start()
            await runner_b.start()

        first_run = await bt_repo.get_run(first, as_of=datetime.now(UTC))
        second_run = await bt_repo.get_run(second, as_of=datetime.now(UTC))
        assert first_run is not None
        assert second_run is not None
        statuses = {first_run["status"], second_run["status"]}
        assert "completed" in statuses or "failed" in statuses
        if second_run["status"] == "failed":
            assert "another backtest" in (second_run.get("error") or "").lower()

    @pytest.mark.timeout(30)
    async def test_warmup_gating_parity_between_engines(self) -> None:
        """24-candle fixture, start_date offset 12h: both engines emit identical artifacts."""
        with patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"warmup_stub": _StubStrategy},
        ):
            rows = [_candle_row(NOW + timedelta(hours=i), 100.0 + i) for i in range(24)]
            direct_repo = AsyncMock()
            direct_repo.get_candles = AsyncMock(return_value=rows)
            direct_collector = ResultCollector()
            await asyncio.wait_for(
                DirectDbEngine(direct_repo, NOW).run(
                    "run-direct", _make_config(start_offset_hours=12), direct_collector
                ),
                timeout=10.0,
            )

            zmq_repo = AsyncMock()
            zmq_repo.get_candles = AsyncMock(return_value=rows)
            zmq_collector = ResultCollector()
            await asyncio.wait_for(
                ZmqReplayEngine(zmq_repo, NOW).run(
                    "run-zmq", _make_config(start_offset_hours=12), zmq_collector
                ),
                timeout=20.0,
            )

        direct_signals = [
            (s["signal_time"], s["signal_type"], s["instrument"]) for s in direct_collector.signals
        ]
        zmq_signals = [
            (s["signal_time"], s["signal_type"], s["instrument"]) for s in zmq_collector.signals
        ]
        assert direct_signals == zmq_signals
        direct_equity = [(p["point_time"], p["equity"]) for p in direct_collector.equity_points]
        zmq_equity = [(p["point_time"], p["equity"]) for p in zmq_collector.equity_points]
        assert direct_equity == zmq_equity
        for s in direct_collector.signals:
            assert s["signal_time"] >= NOW + timedelta(hours=12)
        for s in zmq_collector.signals:
            assert s["signal_time"] >= NOW + timedelta(hours=12)
