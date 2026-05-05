"""Tests for BacktestRunnerProcess lifecycle orchestration."""

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
from sqlalchemy.exc import IntegrityError

import snapper.data.repository as repo_module
from snapper.application.backtest.config import BacktestExecutionMode
from snapper.application.backtest.runner import BacktestRunnerProcess
from snapper.application.backtest.runner import noop_publish
from snapper.application.backtest.runner import run_to_config_dict
from snapper.core.types import ExchangeEnum
from snapper.data.backtest_conflict import SINGLE_RUNNING_INDEX
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository_types import BacktestRunRow

NOW = datetime(2026, 4, 14, 12, 0, 0, tzinfo=UTC)


def _make_run_row(
    public_id: str = "run-1",
    status: str = "pending",
    strategy_name: str = "sma_cross",
) -> BacktestRunRow:
    """Build a minimal BacktestRunRow for tests."""
    return BacktestRunRow(
        public_id=public_id,
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        wallet_public_id="w-1",
        operator_public_id=None,
        strategy_name=strategy_name,
        strategy_params={"fast": 10},
        instrument_public_id="BTC-USD",
        instrument=None,
        exchange="kraken",
        mode="paper",
        timeframe="1h",
        start_date=NOW,
        end_date=NOW + timedelta(days=30),
        initial_cash=10000.0,
        status=status,
        execution_mode="direct_db",
        fill_model="market",
        slippage_bps=0.0,
        commission_bps=0.0,
        config_hash=None,
        target_execution_exchange=None,
        created_by_user_id=None,
        started_at=None,
        completed_at=None,
        error=None,
        process_name=None,
    )


class TestRunToConfigDict:
    """Tests for run_to_config_dict helper."""

    def test_converts_flat_row_to_nested_instruments(self) -> None:
        """Flat exchange+instrument → nested dict for BacktestConfig."""
        row = _make_run_row()
        result = run_to_config_dict(row)
        assert result["instruments"] == {"kraken": ["BTC-USD"]}
        assert result["strategy_class"] == "sma_cross"
        assert result["initial_balance"] == 10000.0
        assert result["wallet_public_id"] == "w-1"
        assert result["timeframe"] == "1h"

    def test_preserves_strategy_params(self) -> None:
        """Strategy params round-trip correctly."""
        row = _make_run_row()
        result = run_to_config_dict(row)
        assert result["strategy_params"] == {"fast": 10}

    def test_operator_public_id_none(self) -> None:
        """None operator_public_id passes through."""
        row = _make_run_row()
        result = run_to_config_dict(row)
        assert result["operator_public_id"] is None

    def test_target_execution_exchange_none_passes_through(self) -> None:
        """Default-None target_execution_exchange round-trips."""
        row = _make_run_row()
        result = run_to_config_dict(row)
        assert result["target_execution_exchange"] is None

    def test_target_execution_exchange_persisted_round_trip(self) -> None:
        """Cross-asset target venue round-trips from DB row to config dict."""
        row = _make_run_row()
        row["target_execution_exchange"] = ExchangeEnum.KRAKEN
        result = run_to_config_dict(row)
        assert result["target_execution_exchange"] == ExchangeEnum.KRAKEN


class TestBacktestRunnerProcess:
    """Tests for BacktestRunnerProcess lifecycle."""

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    async def test_run_not_found_aborts(self, mock_get_repo: MagicMock) -> None:
        """Runner aborts gracefully when run is not found."""
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=None)
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="nonexistent", db_url="sqlite://")
            await runner.start()

            mock_bt_repo.insert_event.assert_not_called()
            mock_bt_repo.update_run_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    async def test_cancel_requested_before_start_short_circuits(
        self,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Cancel request received while pending must skip the engine entirely."""
        run = _make_run_row(status="cancel_requested")
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

            mock_engine_cls.assert_not_called()
            mock_bt_repo.insert_event.assert_not_called()
            assert mock_bt_repo.update_run_status.await_count == 1
            kwargs = mock_bt_repo.update_run_status.await_args.kwargs
            assert kwargs["new_status"] == "cancelled"

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_single_running_conflict_marks_failed_without_starting_engine(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Unique-index conflict cleanly fails the run without starting the engine."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config_cls.model_validate.return_value = mock_config

        orig = MagicMock()
        orig.constraint_name = SINGLE_RUNNING_INDEX
        orig.sqlstate = "23505"
        conflict = IntegrityError("UPDATE backtest_runs", {}, orig)

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(side_effect=[conflict, 1])
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

            mock_engine_cls.assert_not_called()
            assert mock_bt_repo.update_run_status.await_count == 2
            first_call = mock_bt_repo.update_run_status.await_args_list[0].kwargs
            second_call = mock_bt_repo.update_run_status.await_args_list[1].kwargs
            assert first_call["new_status"] == "running"
            assert second_call["new_status"] == "failed"
            assert "another backtest is already running" in (second_call["error"] or "")

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_unrelated_integrity_error_is_reraised(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """IntegrityError is re-raised when it is not the single-running conflict."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config_cls.model_validate.return_value = mock_config

        orig = MagicMock()
        orig.constraint_name = "uq_other"
        orig.sqlstate = "23505"
        unrelated = IntegrityError("UPDATE backtest_runs", {}, orig)

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(side_effect=unrelated)
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            with pytest.raises(IntegrityError):
                await runner.start()

        mock_engine_cls.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_happy_path_pending_to_completed(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Pending run transitions to running then completed."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config.initial_balance = 10000.0
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_portfolio = MagicMock()
        mock_engine.run = AsyncMock(return_value=(mock_portfolio, {"BTC-USD": 50000.0}))
        mock_engine_cls.return_value = mock_engine

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo.insert_signals_batch = AsyncMock()
            mock_bt_repo.insert_trades_batch = AsyncMock()
            mock_bt_repo.insert_equity_points_batch = AsyncMock()
            mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

            event_calls = mock_bt_repo.insert_event.call_args_list
            event_types = [call.kwargs["row"]["event_type"] for call in event_calls]
            assert event_types[0] == "run_started"
            warning_events = [
                call for call in event_calls if call.kwargs["row"]["event_type"] == "metric_warning"
            ]
            assert {call.kwargs["row"]["detail"]["metric"] for call in warning_events} == {
                "max_drawdown_duration_seconds",
                "exposure_ratio",
                "turnover_ratio",
            }

            status_calls = mock_bt_repo.update_run_status.call_args_list
            assert len(status_calls) == 2
            assert status_calls[0].kwargs["new_status"] == "running"
            assert status_calls[0].kwargs["started_at"] is not None
            assert status_calls[1].kwargs["new_status"] == "completed"
            assert status_calls[1].kwargs["completed_at"] is not None

            mock_bt_repo.insert_result.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.ZmqReplayEngine")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_zmq_replay_mode_selects_zmq_engine(
        self,
        mock_config_cls: MagicMock,
        mock_direct_engine_cls: MagicMock,
        mock_zmq_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """ZMQ replay runs instantiate ZmqReplayEngine instead of DirectDbEngine."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config.initial_balance = 10000.0
        mock_config.execution_mode = BacktestExecutionMode.ZMQ_REPLAY
        mock_config.cancel_poll_ms = 250
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(return_value=(MagicMock(), {}))
        mock_zmq_engine_cls.return_value = mock_engine

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo.insert_signals_batch = AsyncMock()
            mock_bt_repo.insert_trades_batch = AsyncMock()
            mock_bt_repo.insert_equity_points_batch = AsyncMock()
            mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

        mock_zmq_engine_cls.assert_called_once()
        mock_direct_engine_cls.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_failure_path_records_error(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Engine failure transitions run to failed with error."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(side_effect=RuntimeError("candle fetch failed"))
        mock_engine_cls.return_value = mock_engine

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            with pytest.raises(RuntimeError, match="candle fetch failed"):
                await runner.start()

            status_calls = mock_bt_repo.update_run_status.call_args_list
            assert len(status_calls) == 2
            assert status_calls[0].kwargs["new_status"] == "running"
            assert status_calls[1].kwargs["new_status"] == "failed"
            assert "candle fetch failed" in (status_calls[1].kwargs["error"] or "")

            event_calls = mock_bt_repo.insert_event.call_args_list
            assert len(event_calls) == 2
            assert event_calls[1].kwargs["row"]["event_type"] == "run_failed"
            assert "candle fetch failed" in str(event_calls[1].kwargs["row"]["detail"]["error"])

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_artifacts_persisted_with_metrics(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Collector artifacts and computed metrics are persisted."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config.initial_balance = 10000.0
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_portfolio = MagicMock()
        mock_engine.run = AsyncMock(return_value=(mock_portfolio, {}))
        mock_engine_cls.return_value = mock_engine

        with patch(
            "snapper.application.backtest.runner.BacktestRepository"
        ) as mock_bt_repo_cls, patch(
            "snapper.application.backtest.runner.ResultCollector"
        ) as mock_collector_cls, patch(
            "snapper.application.backtest.runner.compute_metrics"
        ) as mock_compute:
            mock_collector = MagicMock()
            mock_collector.signals = [{"signal": "data"}]
            mock_collector.trades = [{"trade": "data"}]
            mock_collector.equity_points = [{"eq": "data"}]
            mock_collector.cross_asset_blocked_fills = 0
            mock_collector_cls.return_value = mock_collector

            mock_metrics = MagicMock()
            mock_metrics.total_trades = 5
            mock_metrics.winning_trades = 3
            mock_metrics.losing_trades = 2
            mock_metrics.total_pnl = 500.0
            mock_metrics.max_drawdown = 0.1
            mock_metrics.sharpe_ratio = 1.5
            mock_metrics.win_rate = 0.6
            mock_metrics.profit_factor = 3.0
            mock_metrics.final_equity = 10500.0
            mock_metrics.max_equity = 11000.0
            mock_metrics.sortino_ratio = 2.0
            mock_metrics.cagr = 0.5
            mock_metrics.calmar_ratio = 5.0
            mock_metrics.expectancy = 100.0
            mock_metrics.avg_trade_pnl = 100.0
            mock_metrics.max_drawdown_duration_seconds = 3600.0
            mock_metrics.exposure_ratio = 0.8
            mock_metrics.turnover_ratio = 2.5
            mock_metrics.warnings = []
            mock_compute.return_value = mock_metrics

            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo.insert_signals_batch = AsyncMock()
            mock_bt_repo.insert_trades_batch = AsyncMock()
            mock_bt_repo.insert_equity_points_batch = AsyncMock()
            mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

            mock_bt_repo.insert_signals_batch.assert_called_once()
            mock_bt_repo.insert_trades_batch.assert_called_once()
            mock_bt_repo.insert_equity_points_batch.assert_called_once()
            mock_bt_repo.insert_result.assert_called_once()
            result_row = mock_bt_repo.insert_result.call_args.args[0]
            assert result_row["total_trades"] == 5
            assert result_row["cagr"] == 0.5
            assert result_row["sortino_ratio"] == 2.0
            assert result_row["max_drawdown_duration_seconds"] == 3600.0
            assert result_row["exposure_ratio"] == 0.8
            assert result_row["turnover_ratio"] == 2.5
            assert result_row["extra_metrics"] == {}

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_artifacts_persisted_with_cross_asset_blocked_fill_metrics(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Positive blocked-fill counts are persisted into extra_metrics."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config.initial_balance = 10000.0
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_portfolio = MagicMock()
        mock_engine.run = AsyncMock(return_value=(mock_portfolio, {}))
        mock_engine_cls.return_value = mock_engine

        with patch(
            "snapper.application.backtest.runner.BacktestRepository"
        ) as mock_bt_repo_cls, patch(
            "snapper.application.backtest.runner.ResultCollector"
        ) as mock_collector_cls, patch(
            "snapper.application.backtest.runner.compute_metrics"
        ) as mock_compute:
            mock_collector = MagicMock()
            mock_collector.signals = []
            mock_collector.trades = []
            mock_collector.equity_points = []
            mock_collector.cross_asset_blocked_fills = 3
            mock_collector_cls.return_value = mock_collector

            mock_metrics = MagicMock()
            mock_metrics.total_trades = 0
            mock_metrics.winning_trades = 0
            mock_metrics.losing_trades = 0
            mock_metrics.total_pnl = 0.0
            mock_metrics.max_drawdown = 0.0
            mock_metrics.sharpe_ratio = 0.0
            mock_metrics.win_rate = 0.0
            mock_metrics.profit_factor = 0.0
            mock_metrics.final_equity = 10000.0
            mock_metrics.max_equity = 10000.0
            mock_metrics.sortino_ratio = 0.0
            mock_metrics.cagr = 0.0
            mock_metrics.calmar_ratio = 0.0
            mock_metrics.expectancy = 0.0
            mock_metrics.avg_trade_pnl = 0.0
            mock_metrics.max_drawdown_duration_seconds = 0.0
            mock_metrics.exposure_ratio = 0.0
            mock_metrics.turnover_ratio = 0.0
            mock_metrics.warnings = []
            mock_compute.return_value = mock_metrics

            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo.insert_signals_batch = AsyncMock()
            mock_bt_repo.insert_trades_batch = AsyncMock()
            mock_bt_repo.insert_equity_points_batch = AsyncMock()
            mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

            mock_bt_repo.insert_result.assert_called_once()
            result_row = mock_bt_repo.insert_result.call_args.args[0]
            assert result_row["extra_metrics"] == {"cross_asset_blocked_fills": 3}

    def test_get_status_returns_run_id(self) -> None:
        """get_status() includes run_public_id."""
        runner = BacktestRunnerProcess(run_public_id="run-abc", db_url="sqlite://")
        assert runner.get_status() == {"run_public_id": "run-abc"}

    def test_get_default_parameters_empty(self) -> None:
        """Default parameters are empty — all params from API handler."""
        params = BacktestRunnerProcess.get_default_parameters(MagicMock())
        assert params == {}

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_empty_collector_skips_batch_inserts(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Empty collector buffers skip batch insert calls."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config.initial_balance = 10000.0
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_portfolio = MagicMock()
        mock_engine.run = AsyncMock(return_value=(mock_portfolio, {}))
        mock_engine_cls.return_value = mock_engine

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo.insert_signals_batch = AsyncMock()
            mock_bt_repo.insert_trades_batch = AsyncMock()
            mock_bt_repo.insert_equity_points_batch = AsyncMock()
            mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            await runner.start()

            mock_bt_repo.insert_signals_batch.assert_not_called()
            mock_bt_repo.insert_trades_batch.assert_not_called()
            mock_bt_repo.insert_equity_points_batch.assert_not_called()
            mock_bt_repo.insert_result.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_config_validation_failure_transitions_to_failed(
        self,
        mock_config_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """Config validation error before running still marks failed."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config_cls.model_validate.side_effect = ValueError("bad strategy")

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            with pytest.raises(ValueError, match="bad strategy"):
                await runner.start()

            status_calls = mock_bt_repo.update_run_status.call_args_list
            assert len(status_calls) == 1
            assert status_calls[0].kwargs["new_status"] == "failed"
            assert "bad strategy" in (status_calls[0].kwargs["error"] or "")

    @pytest.mark.asyncio
    @patch("snapper.application.backtest.runner.get_repository")
    @patch("snapper.application.backtest.runner.DirectDbEngine")
    @patch("snapper.application.backtest.runner.BacktestConfig")
    async def test_cancellation_transitions_to_cancelled(
        self,
        mock_config_cls: MagicMock,
        mock_engine_cls: MagicMock,
        mock_get_repo: MagicMock,
    ) -> None:
        """CancelledError transitions run to cancelled and re-raises."""
        run = _make_run_row()
        mock_repo = MagicMock()
        mock_repo.session_factory = MagicMock()
        mock_get_repo.return_value = mock_repo

        mock_config = MagicMock()
        mock_config.strategy_class = "sma_cross"
        mock_config_cls.model_validate.return_value = mock_config

        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(side_effect=asyncio.CancelledError())
        mock_engine_cls.return_value = mock_engine

        with patch("snapper.application.backtest.runner.BacktestRepository") as mock_bt_repo_cls:
            mock_bt_repo = AsyncMock()
            mock_bt_repo.get_run = AsyncMock(return_value=run)
            mock_bt_repo.insert_event = AsyncMock(return_value="evt-1")
            mock_bt_repo.update_run_status = AsyncMock(return_value=1)
            mock_bt_repo_cls.return_value = mock_bt_repo

            runner = BacktestRunnerProcess(run_public_id="run-1", db_url="sqlite://")
            with pytest.raises(asyncio.CancelledError):
                await runner.start()

            status_calls = mock_bt_repo.update_run_status.call_args_list
            assert len(status_calls) == 2
            assert status_calls[0].kwargs["new_status"] == "running"
            assert status_calls[1].kwargs["new_status"] == "cancelled"


class TestReconcileStaleRuns:
    """Tests for BacktestRepository.reconcile_stale_runs."""

    @pytest.mark.asyncio
    async def test_no_stale_runs(self, tmp_path: Path) -> None:
        """No stale runs returns 0."""
        repo = await _make_bt_repo(tmp_path)
        count = await repo.reconcile_stale_runs(bus_time=NOW, session_id="s1", sequence_id=1)
        assert count == 0

    @pytest.mark.asyncio
    async def test_pending_run_marked_failed(self, tmp_path: Path) -> None:
        """Pending run is transitioned to failed."""
        repo = await _make_bt_repo(tmp_path)
        _, pid = await repo.create_run(_insert_row(), bus_time=NOW, session_id="s1", sequence_id=1)
        count = await repo.reconcile_stale_runs(
            bus_time=NOW + timedelta(seconds=5),
            session_id="s1",
            sequence_id=10,
        )
        assert count == 1
        run = await repo.get_run(pid, as_of=NOW + timedelta(seconds=10))
        assert run is not None
        assert run["status"] == "failed"
        assert "Orphaned" in (run["error"] or "")

    @pytest.mark.asyncio
    async def test_running_run_marked_failed(self, tmp_path: Path) -> None:
        """Running run is transitioned to failed."""
        repo = await _make_bt_repo(tmp_path)
        _, pid = await repo.create_run(_insert_row(), bus_time=NOW, session_id="s1", sequence_id=1)
        t2 = NOW + timedelta(seconds=2)
        await repo.update_run_status(
            pid, "running", bus_time=t2, session_id="s1", sequence_id=2, started_at=t2
        )
        count = await repo.reconcile_stale_runs(
            bus_time=t2 + timedelta(seconds=5),
            session_id="s1",
            sequence_id=10,
        )
        assert count == 1
        run = await repo.get_run(pid, as_of=t2 + timedelta(seconds=10))
        assert run is not None
        assert run["status"] == "failed"

    @pytest.mark.asyncio
    async def test_completed_run_not_touched(self, tmp_path: Path) -> None:
        """Completed run is not affected by reconciliation."""
        repo = await _make_bt_repo(tmp_path)
        _, pid = await repo.create_run(_insert_row(), bus_time=NOW, session_id="s1", sequence_id=1)
        t2 = NOW + timedelta(seconds=2)
        await repo.update_run_status(
            pid, "completed", bus_time=t2, session_id="s1", sequence_id=2, completed_at=t2
        )
        count = await repo.reconcile_stale_runs(
            bus_time=t2 + timedelta(seconds=5),
            session_id="s1",
            sequence_id=10,
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_cancel_requested_marked_failed(self, tmp_path: Path) -> None:
        """cancel_requested run is swept as failed."""
        repo = await _make_bt_repo(tmp_path)
        _, pid = await repo.create_run(_insert_row(), bus_time=NOW, session_id="s1", sequence_id=1)
        t2 = NOW + timedelta(seconds=2)
        await repo.update_run_status(
            pid, "cancel_requested", bus_time=t2, session_id="s1", sequence_id=2
        )
        count = await repo.reconcile_stale_runs(
            bus_time=t2 + timedelta(seconds=5),
            session_id="s1",
            sequence_id=10,
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_multiple_stale_runs(self, tmp_path: Path) -> None:
        """Multiple stale runs all get reconciled."""
        repo = await _make_bt_repo(tmp_path)
        for i in range(3):
            await repo.create_run(
                _insert_row(),
                bus_time=NOW + timedelta(seconds=i),
                session_id="s1",
                sequence_id=i + 1,
            )
        count = await repo.reconcile_stale_runs(
            bus_time=NOW + timedelta(seconds=10),
            session_id="s1",
            sequence_id=100,
        )
        assert count == 3

    @pytest.mark.asyncio
    async def test_update_returns_none_skips_count(self, tmp_path: Path) -> None:
        """When update_run_status returns None, that row is not counted."""
        repo = await _make_bt_repo(tmp_path)
        await repo.create_run(_insert_row(), bus_time=NOW, session_id="s1", sequence_id=1)
        reconcile_time = NOW + timedelta(seconds=5)
        original_update = repo.update_run_status

        async def _update_returns_none(
            public_id: str, new_status: str, **kwargs: Any
        ) -> int | None:
            """Simulate race condition where row vanishes between select and update."""
            await original_update(public_id, new_status, **kwargs)
            return None

        repo.update_run_status = _update_returns_none
        count = await repo.reconcile_stale_runs(
            bus_time=reconcile_time, session_id="s1", sequence_id=10
        )
        assert count == 0
        repo.update_run_status = original_update


def _insert_row() -> dict[str, Any]:
    """Build a minimal insert row for backtest_runs."""
    return {
        "wallet_public_id": "w-1",
        "strategy_name": "sma",
        "instrument_public_id": "BTC-USD",
        "exchange": "kraken",
        "timeframe": "1h",
        "start_date": NOW,
        "end_date": NOW + timedelta(days=30),
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": NOW,
    }


async def _make_bt_repo(tmp_path: Path) -> BacktestRepository:
    """Create a BacktestRepository backed by a fresh SQLite DB."""
    db_path = tmp_path / "bt.db"
    r = repo_module.SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await r.create_all()
    return BacktestRepository(r.session_factory)


class TestRunnerProgressPublisher:
    """Tests for the Codex F1 fix — runner owns a real ZMQ publisher."""

    def test_override_is_used_verbatim(self) -> None:
        """Test-injected publish override replaces the owned publisher.

        Given:
            A ``progress_publish`` argument supplied to the runner
            constructor.

        When:
            ``_resolve_progress_publish`` is called.

        Then:
            The override is returned unchanged and no owned ZMQ
            context or socket is opened — explicit override wins.
        """

        async def override(_topic: str, _data: Any) -> None:
            await asyncio.sleep(0)

        runner = BacktestRunnerProcess(
            run_public_id="run-x",
            db_url="sqlite://",
            progress_publish=override,
        )
        resolved = runner._resolve_progress_publish()
        assert resolved is override
        assert runner._owned_zmq_context is None
        assert runner._owned_validated_publisher is None
        assert runner._owned_message_publisher is None

    def test_production_path_opens_owned_publisher(self) -> None:
        """Production default constructs a live ZMQ PUB socket.

        Given:
            No override passed (production entry point via
            ``ProcessFactory.start_process`` which cannot serialise a
            callable),

        When:
            ``_resolve_progress_publish`` is called,

        Then:
            An owned ``zmq.asyncio.Context`` + ``ValidatedPublisher`` +
            ``MessagePublisher`` are created and stored on the runner
            so the adapter can forward events through the real
            broker. ``_teardown_owned_publisher`` cleanly releases
            them.
        """
        runner = BacktestRunnerProcess(run_public_id="run-y", db_url="sqlite://")
        adapter = runner._resolve_progress_publish()
        assert adapter is not None
        assert runner._owned_zmq_context is not None
        assert runner._owned_validated_publisher is not None
        assert runner._owned_message_publisher is not None
        runner._teardown_owned_publisher()
        assert runner._owned_zmq_context is None
        assert runner._owned_validated_publisher is None
        assert runner._owned_message_publisher is None

    def test_wiring_failure_falls_back_to_noop_publish(self) -> None:
        """Publisher setup failure degrades to the noop progress sink.

        Given:
            The production path needs to create its own ZMQ publisher,

        When:
            Context construction raises during wiring,

        Then:
            ``_resolve_progress_publish`` returns ``noop_publish`` and
            no owned publisher state is retained on the runner.
        """
        runner = BacktestRunnerProcess(run_public_id="run-fallback", db_url="sqlite://")
        bootstrap = MagicMock()
        bootstrap.zmq_broker_xsub = "tcp://127.0.0.1:5555"

        with (
            patch(
                "snapper.application.backtest.runner.get_bootstrap_settings",
                return_value=bootstrap,
            ),
            patch(
                "snapper.application.backtest.runner.zmq.asyncio.Context",
                side_effect=RuntimeError("boom"),
            ),
        ):
            resolved = runner._resolve_progress_publish()

        assert resolved is noop_publish
        assert runner._owned_zmq_context is None
        assert runner._owned_validated_publisher is None
        assert runner._owned_message_publisher is None

    def test_teardown_without_owned_resources_is_noop(self) -> None:
        """Teardown keeps an uninitialised runner unchanged.

        Given:
            A runner that never opened its owned publisher,

        When:
            ``_teardown_owned_publisher`` is called,

        Then:
            The method returns cleanly and all owned handles stay
            ``None``.
        """
        runner = BacktestRunnerProcess(run_public_id="run-empty", db_url="sqlite://")

        runner._teardown_owned_publisher()

        assert runner._owned_zmq_context is None
        assert runner._owned_validated_publisher is None
        assert runner._owned_message_publisher is None

    @pytest.mark.asyncio
    async def test_publish_swallow_errors_dropped_event(self) -> None:
        """Publish failure downgrades to a log line, runner keeps going.

        Given:
            The owned ``MessagePublisher.send`` raises (e.g. the
            topic validator rejects a malformed wallet segment or
            the broker disappears mid-run),

        When:
            The adapter returned by ``_resolve_progress_publish`` is
            invoked with a payload that would fail validation,

        Then:
            The adapter catches the exception and returns without
            raising, so progress remains best-effort observability
            and a transient broker/topic issue never aborts the
            backtest.
        """
        runner = BacktestRunnerProcess(run_public_id="run-z", db_url="sqlite://")
        try:
            adapter = runner._resolve_progress_publish()
            from snapper.messaging.schemas.data import BacktestProgressData

            payload = BacktestProgressData(
                public_id="p",
                timestamp=NOW,
                session_id="s",
                sequence_id=1,
                run_public_id="r",
                wallet_public_id="w",
                event="started",
                candles_done=0,
                total_candles=None,
                signals_count=0,
                trades_count=0,
                equity=0.0,
                progress_pct=0.0,
            )
            await adapter("definitely.not.a.valid.backtest.topic", payload)
        finally:
            runner._teardown_owned_publisher()
