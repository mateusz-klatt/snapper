"""Tests for PlanExecutorService."""

import asyncio
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.plans.service import PlanExecutorService


def _make_plan_row(
    public_id: str = "plan-1",
    plan_type: str = "manual_once",
    status: str = "active",
) -> dict[str, object]:
    """Create a minimal plan row dict for testing."""
    now = datetime(2026, 4, 10, tzinfo=UTC)
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "plan_type": plan_type,
        "created_by_user_id": "user-1",
        "created_by_strategy": None,
        "created_via": "ui",
        "instrument_public_id": "inst-1",
        "exchange": "kraken",
        "mode": "live",
        "shard_key": "kraken:BTC-USD:live",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "total_quantity": 1.0,
        "filled_quantity": 0.0,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": None,
        "params": {"order_type": "limit", "side": "buy", "price": 50000.0},
        "status": status,
        "created_at": now,
        "started_at": None,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": None,
    }


class TestPlanExecutorService:
    """Tests for PlanExecutorService lifecycle and recovery."""

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_get_evaluator_known_type(self, mock_repo: MagicMock, mock_settings: MagicMock) -> None:
        """Known plan types return correct evaluator instances."""
        service = PlanExecutorService()
        evaluator = service.get_evaluator("manual_once")
        assert isinstance(evaluator, ManualOnceEvaluator)

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_get_evaluator_unknown_type(
        self, mock_repo: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unknown plan types return None."""
        service = PlanExecutorService()
        assert service.get_evaluator("nonexistent") is None

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_get_status_empty(self, mock_repo: MagicMock, mock_settings: MagicMock) -> None:
        """Status shows zero plans when empty."""
        service = PlanExecutorService()
        status = service.get_status()
        assert status["active_plans"] == 0
        assert status["running"] is False

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_get_status_with_plans(self, mock_repo: MagicMock, mock_settings: MagicMock) -> None:
        """Status reflects loaded plans."""
        service = PlanExecutorService()
        service.plans["plan-1"] = _make_plan_row()
        service._running = True
        status = service.get_status()
        assert status["active_plans"] == 1
        assert "manual_once" in status["plan_types"]
        assert status["running"] is True

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_recover_plans_loads_known_types(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Recovery loads plans and instantiates evaluators."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(
            return_value=[_make_plan_row("plan-1", "manual_once")]
        )
        mock_repo.get_latest_plan_checkpoint = AsyncMock(return_value=None)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        assert "plan-1" in service.plans
        assert isinstance(service.evaluators["plan-1"], ManualOnceEvaluator)

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_recover_plans_skips_unknown_types(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Recovery skips plans with unknown evaluator types."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(
            return_value=[_make_plan_row("plan-x", "unknown_type")]
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        assert "plan-x" not in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_recover_plans_restores_checkpoint(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Recovery restores evaluator state from checkpoint."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(return_value=[_make_plan_row()])
        mock_repo.get_latest_plan_checkpoint = AsyncMock(
            return_value={
                "public_id": "cp-1",
                "plan_public_id": "plan-1",
                "state": {"restored": True},
                "last_venue_event_id": 42,
                "last_tick_timestamp": None,
                "checkpoint_at": datetime(2026, 4, 10, tzinfo=UTC),
                "timestamp": datetime(2026, 4, 10, tzinfo=UTC),
                "session_id": "s1",
                "sequence_id": 1,
            }
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        assert "plan-1" in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_stop_sets_running_false(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Stop sets _running to False."""
        service = PlanExecutorService()
        service._running = True
        await service.stop()
        assert service._running is False

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_write_checkpoints(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Checkpoint writing calls repo for each active plan."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_checkpoint = AsyncMock(return_value=(1, "cp-1"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service.plans["plan-1"] = _make_plan_row()
        service.evaluators["plan-1"] = ManualOnceEvaluator()
        await service._write_checkpoints()
        mock_repo.insert_execution_plan_checkpoint.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_write_checkpoints_handles_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Checkpoint errors are logged, not raised."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_checkpoint = AsyncMock(side_effect=Exception("DB error"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service.plans["plan-1"] = _make_plan_row()
        service.evaluators["plan-1"] = ManualOnceEvaluator()
        await service._write_checkpoints()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_start_recovers_and_runs(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Start recovers plans then enters run loop."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(return_value=[])
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        async def _stop_after_start() -> None:
            service._running = False

        with patch.object(service, "_run_loop", side_effect=_stop_after_start):
            await service.start()
        assert not service._running

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_run_loop_exits_when_stopped(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Run loop exits when _running becomes False."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_checkpoint = AsyncMock(return_value=(1, "cp-1"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service._running = True

        original_sleep = asyncio.sleep

        async def _quick_stop(seconds: float) -> None:
            service._running = False
            await original_sleep(0)

        with patch("snapper.application.plans.service.asyncio.sleep", side_effect=_quick_stop):
            await service._run_loop()
        assert not service._running

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_write_checkpoints_skips_orphaned_evaluator(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Checkpoint skips evaluators with no matching plan entry."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_checkpoint = AsyncMock(return_value=(1, "cp-1"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service.evaluators["orphan-1"] = ManualOnceEvaluator()
        await service._write_checkpoints()
        mock_repo.insert_execution_plan_checkpoint.assert_not_called()

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_default_parameters(self, mock_repo: MagicMock, mock_settings: MagicMock) -> None:
        """Default parameters returns empty dict."""
        result = PlanExecutorService.get_default_parameters(MagicMock())
        assert result == {}
