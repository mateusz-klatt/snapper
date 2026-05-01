"""End-to-end integration tests for backtest lifecycle.

Uses a real SQLite database with seeded candle data to verify
the full backtest flow: create → run engine → persist artifacts → complete.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path
from typing import Any
from typing import ClassVar
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.data.repository as repo_module
from snapper.application.backtest.metrics import compute_metrics
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.backtest.runner import BacktestRunnerProcess
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository_types import BacktestResultInsertRow

NOW = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 1, 31, tzinfo=UTC)
MOCK_STRATEGIES: dict[str, Any] = {}


class RegressingRunnerDateTime(datetime):
    """Datetime shim that reproduces a wall-clock regression inside the runner."""

    _calls: ClassVar[int] = 0
    _times: ClassVar[tuple[datetime, ...]] = (
        datetime(2026, 5, 1, 10, 0, 0, 200, tzinfo=UTC),
        datetime(2026, 5, 1, 10, 0, 0, 100, tzinfo=UTC),
    )

    @classmethod
    def reset(cls) -> None:
        """Reset the deterministic clock sequence."""
        cls._calls = 0

    @classmethod
    def now(cls, tz: tzinfo | None = None) -> datetime:
        """Return the next deterministic timestamp."""
        index = min(cls._calls, len(cls._times) - 1)
        cls._calls += 1
        value = cls._times[index]
        if tz is None:
            return value.replace(tzinfo=None)
        return value.astimezone(tz)


async def _setup_db(tmp_path: Path) -> tuple[repo_module.SQLAlchemyRepository, BacktestRepository]:
    """Create fresh SQLite DB with all tables."""
    db_path = tmp_path / "bt_e2e.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"
    repo = repo_module.SQLAlchemyRepository(db_url)
    await repo.create_all()
    bt_repo = BacktestRepository(repo.session_factory)
    return repo, bt_repo


async def _create_pending_run(
    bt_repo: BacktestRepository,
    status: str = "pending",
) -> str:
    """Insert a pending backtest run and return its public_id."""
    _, public_id = await bt_repo.create_run(
        row={
            "wallet_public_id": "w-1",
            "strategy_name": "sma_cross",
            "strategy_params": {"fast": 10},
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": NOW,
            "end_date": END,
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


class TestBacktestE2ELifecycle:
    """Integration tests for full backtest lifecycle."""

    @pytest.mark.asyncio
    async def test_runner_completes_with_empty_candles(self, tmp_path: Path) -> None:
        """Runner with no candle data completes with 0 trades."""
        repo, bt_repo = await _setup_db(tmp_path)
        public_id = await _create_pending_run(bt_repo)
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'bt_e2e.db'}"

        with patch(
            "snapper.application.backtest.runner.get_repository", return_value=repo
        ), patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"sma_cross": MagicMock()},
        ):
            runner = BacktestRunnerProcess(run_public_id=public_id, db_url=db_url)
            await runner.start()

        now = datetime.now(UTC)
        run = await bt_repo.get_run(public_id, as_of=now)
        assert run is not None
        assert run["status"] == "completed"

        result = await bt_repo.get_result(public_id, as_of=now)
        assert result is not None
        assert result["total_trades"] == 0

        events = await bt_repo.get_events(public_id, as_of=now)
        assert len(events) >= 1
        assert events[0]["event_type"] == "run_started"

    @pytest.mark.asyncio
    async def test_runner_failure_records_error(self, tmp_path: Path) -> None:
        """Runner engine failure transitions to failed with error in DB."""
        repo, bt_repo = await _setup_db(tmp_path)
        public_id = await _create_pending_run(bt_repo)
        db_url = f"sqlite+aiosqlite:///{tmp_path / 'bt_e2e.db'}"
        RegressingRunnerDateTime.reset()

        with patch(
            "snapper.application.backtest.runner.get_repository", return_value=repo
        ), patch.dict(
            "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
            {"sma_cross": MagicMock()},
        ), patch(
            "snapper.application.backtest.runner.DirectDbEngine"
        ) as mock_engine_cls, patch(
            "snapper.application.backtest.runner.datetime", RegressingRunnerDateTime
        ):

            mock_engine = AsyncMock()
            mock_engine.run = AsyncMock(side_effect=RuntimeError("candle fetch exploded"))
            mock_engine_cls.return_value = mock_engine

            runner = BacktestRunnerProcess(run_public_id=public_id, db_url=db_url)
            with pytest.raises(RuntimeError, match="candle fetch exploded"):
                await runner.start()

        now = RegressingRunnerDateTime._times[0] + timedelta(seconds=1)
        run = await bt_repo.get_run(public_id, as_of=now)
        assert run is not None
        assert run["status"] == "failed"
        assert "candle fetch exploded" in (run["error"] or "")

        events = await bt_repo.get_events(public_id, as_of=now)
        failed_events = [e for e in events if e["event_type"] == "run_failed"]
        assert len(failed_events) == 1


class TestBacktestReconciliationE2E:
    """Integration tests for boot-time orphan reconciliation."""

    @pytest.mark.asyncio
    async def test_orphan_pending_run_marked_failed(self, tmp_path: Path) -> None:
        """Pending run from crashed process is marked failed at boot."""
        _, bt_repo = await _setup_db(tmp_path)
        public_id = await _create_pending_run(bt_repo, status="pending")

        now = datetime.now(UTC) + timedelta(seconds=5)
        count = await bt_repo.reconcile_stale_runs(
            bus_time=now, session_id="reconcile", sequence_id=1
        )
        assert count == 1

        run = await bt_repo.get_run(public_id, as_of=now + timedelta(seconds=1))
        assert run is not None
        assert run["status"] == "failed"
        assert "Orphaned" in (run["error"] or "")

    @pytest.mark.asyncio
    async def test_orphan_running_run_marked_failed(self, tmp_path: Path) -> None:
        """Running run from crashed process is marked failed at boot."""
        _, bt_repo = await _setup_db(tmp_path)
        public_id = await _create_pending_run(bt_repo, status="pending")

        t1 = NOW + timedelta(seconds=2)
        await bt_repo.update_run_status(
            public_id, "running", bus_time=t1, session_id="s1", sequence_id=2, started_at=t1
        )

        reconcile_time = datetime.now(UTC) + timedelta(seconds=5)
        count = await bt_repo.reconcile_stale_runs(
            bus_time=reconcile_time, session_id="reconcile", sequence_id=10
        )
        assert count == 1

        run = await bt_repo.get_run(public_id, as_of=reconcile_time + timedelta(seconds=1))
        assert run is not None
        assert run["status"] == "failed"

    @pytest.mark.asyncio
    async def test_completed_run_not_affected(self, tmp_path: Path) -> None:
        """Completed run is NOT affected by reconciliation."""
        _, bt_repo = await _setup_db(tmp_path)
        public_id = await _create_pending_run(bt_repo, status="pending")

        t1 = NOW + timedelta(seconds=2)
        await bt_repo.update_run_status(
            public_id, "completed", bus_time=t1, session_id="s1", sequence_id=2, completed_at=t1
        )

        reconcile_time = datetime.now(UTC) + timedelta(seconds=5)
        count = await bt_repo.reconcile_stale_runs(
            bus_time=reconcile_time, session_id="reconcile", sequence_id=10
        )
        assert count == 0


class TestBacktestResultCollectorE2E:
    """Integration test for collector → metrics → persist flow."""

    @pytest.mark.asyncio
    async def test_collector_to_metrics_to_persist(self, tmp_path: Path) -> None:
        """Collector artifacts compute metrics and persist to DB."""
        _, bt_repo = await _setup_db(tmp_path)
        public_id = await _create_pending_run(bt_repo)

        collector = ResultCollector()
        collector.record_signal(
            run_public_id=public_id,
            public_id="00000000-0000-7000-8000-00000000abcd",
            signal_time=NOW,
            signal_type="buy",
            instrument="BTC-USD",
            price=50000.0,
            indicators={"sma": 49000},
            session_id="s1",
            sequence_id=1,
            bus_time=NOW,
        )

        now = datetime.now(UTC)
        await bt_repo.insert_signals_batch(
            collector.signals, bus_time=now, session_id="s1", sequence_id=10
        )

        signals = await bt_repo.get_signals(public_id, as_of=now + timedelta(seconds=1))
        assert len(signals) == 1
        assert signals[0]["signal_type"] == "buy"

        metrics = compute_metrics([], [], initial_balance=10000.0)
        await bt_repo.insert_result(
            BacktestResultInsertRow(
                run_public_id=public_id,
                total_trades=metrics.total_trades,
                winning_trades=metrics.winning_trades,
                losing_trades=metrics.losing_trades,
                total_pnl=metrics.total_pnl,
                max_drawdown=metrics.max_drawdown,
                sharpe_ratio=metrics.sharpe_ratio,
                win_rate=metrics.win_rate,
                profit_factor=metrics.profit_factor,
                final_equity=metrics.final_equity,
                max_equity=metrics.max_equity,
                extra_metrics={},
                session_id="s1",
                sequence_id=20,
                timestamp=now,
            ),
            bus_time=now,
            session_id="s1",
            sequence_id=20,
        )

        result = await bt_repo.get_result(public_id, as_of=now + timedelta(seconds=1))
        assert result is not None
        assert result["total_trades"] == 0
