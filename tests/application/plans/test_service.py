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
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import TickData


def _make_plan_row(
    public_id: str = "plan-1",
    plan_type: str = "manual_once",
    status: str = "active",
    total_quantity: float = 1.0,
    filled_quantity: float = 0.0,
    child_client_order_id: str | None = "cid-1",
) -> dict[str, object]:
    """Create a minimal plan row dict for testing."""
    now = datetime(2026, 4, 10, tzinfo=UTC)
    params: dict[str, object] = {
        "order_type": "limit",
        "side": "buy",
        "price": 50000.0,
    }
    if child_client_order_id is not None:
        params["child_client_order_id"] = child_client_order_id
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
        "total_quantity": total_quantity,
        "filled_quantity": filled_quantity,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": None,
        "params": params,
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


def _make_execution(
    client_order_id: str = "cid-1",
    *,
    size: float = 1.0,
    last_size: float = 1.0,
    status: str = "filled",
) -> object:
    """Build an ExecutionData suitable for PlanExecutorService._handle_execution."""
    return ExecutionData(
        public_id="exec-1",
        timestamp=datetime(2026, 4, 10, tzinfo=UTC),
        session_id="s1",
        sequence_id=1,
        client_order_id=client_order_id,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        size=size,
        price=50000.0,
        last_size=last_size,
        last_price=50000.0,
        fee=0.0,
        fee_asset="USD",
        status=status,
        executed_at=datetime(2026, 4, 10, tzinfo=UTC),
    )


def _make_order_status(
    client_order_id: str = "cid-1",
    status: str = "cancelled",
    error: str | None = None,
) -> object:
    """Build an OrderData suitable for PlanExecutorService._handle_order_status."""
    return OrderData(
        public_id="ord-1",
        timestamp=datetime(2026, 4, 10, tzinfo=UTC),
        session_id="s1",
        sequence_id=1,
        client_order_id=client_order_id,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        status=status,
        order_type="limit",
        size=1.0,
        filled_size=0.0,
        price=50000.0,
        average_price=None,
        reason=None,
        time_in_force=None,
        error=error,
        created_at=datetime(2026, 4, 10, tzinfo=UTC),
        updated_at=None,
    )


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
        assert service._watermarks["plan-1"] == 42
        assert service._last_tick_timestamps["plan-1"] is None

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

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_full_fill_completes_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a full fill, plan transitions to completed with filled_quantity."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(total_quantity=1.0)
        service._register_plan(plan, ManualOnceEvaluator())
        execution = _make_execution(size=1.0, last_size=1.0, status="filled")
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_called_once()
        call_kwargs = mock_repo.update_execution_plan_status.call_args[1]
        assert call_kwargs["new_status"] == "completed"
        assert call_kwargs["filled_quantity"] == pytest.approx(1.0)
        assert call_kwargs["completed_at"] is not None
        assert "plan-1" not in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_partial_fill_keeps_plan_active(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a partial fill, plan stays active with updated filled_quantity."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(total_quantity=2.0)
        service._register_plan(plan, ManualOnceEvaluator())
        execution = _make_execution(size=0.5, last_size=0.5, status="partial")
        await service._handle_execution(execution)
        call_kwargs = mock_repo.update_execution_plan_status.call_args[1]
        assert call_kwargs["new_status"] == "active"
        assert call_kwargs["filled_quantity"] == pytest.approx(0.5)
        assert call_kwargs["completed_at"] is None
        assert "plan-1" in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_skips_duplicate_cumulative_fill(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Duplicate/stale cumulative fills do not advance plan state.

        ``ExecutionData.size`` is the cumulative filled quantity. If a
        duplicate frame arrives with ``size == current filled``, the
        service must short-circuit rather than emit another SCD2 write
        (which would log a redundant transition and advance
        ``last_evaluated_at`` for no reason).
        """
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(total_quantity=2.0, filled_quantity=1.0)
        service._register_plan(plan, ManualOnceEvaluator())
        execution = _make_execution(size=1.0, last_size=1.0, status="partial")
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_skips_stale_cumulative_fill(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Out-of-order cumulative frames (smaller than already-seen) are skipped."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(total_quantity=2.0, filled_quantity=1.5)
        service._register_plan(plan, ManualOnceEvaluator())
        execution = _make_execution(size=1.0, last_size=1.0, status="partial")
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_ignores_unknown_client_order_id(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given an execution for an unknown client_order_id, Then no DB write."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        execution = _make_execution(client_order_id="unknown-cid")
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_skips_terminal_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a terminal plan, a late fill does not update the plan."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="completed")
        service._register_plan(plan, ManualOnceEvaluator())
        execution = _make_execution(size=1.0, last_size=1.0)
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_swallows_db_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a DB error during status update, Then the listen loop keeps running."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(side_effect=Exception("DB"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        execution = _make_execution(size=1.0, last_size=1.0)
        await service._handle_execution(execution)
        assert "plan-1" in service.plans
        assert service.plans["plan-1"]["status"] == "active"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_cancelled_transitions_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a cancelled OrderData, Then plan transitions to cancelled."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="cancelled")
        await service._handle_order_status(order_event)
        call_kwargs = mock_repo.update_execution_plan_status.call_args[1]
        assert call_kwargs["new_status"] == "cancelled"
        assert "plan-1" not in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_rejected_marks_plan_failed(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a rejected OrderData with error, Then plan transitions to failed."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="rejected", error="insufficient funds")
        await service._handle_order_status(order_event)
        call_kwargs = mock_repo.update_execution_plan_status.call_args[1]
        assert call_kwargs["new_status"] == "failed"
        assert call_kwargs["last_error"] == "insufficient funds"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_ignores_non_terminal(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given non-terminal OrderData (e.g., accepted), Then no DB write."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="accepted")
        await service._handle_order_status(order_event)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_tick_dispatches_to_matching_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a tick on an instrument+exchange matching a plan, Then evaluator is invoked."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        evaluator = ManualOnceEvaluator()
        service._register_plan(plan, evaluator)
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="inst-1",
            exchange="kraken",
            volume=0.0,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
        )
        await service._handle_tick("market.kraken.inst-1.ticks", tick)
        assert service._last_tick_timestamps["plan-1"] is not None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_setup_subscriber_skips_when_no_broker(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given no broker endpoint on settings, Then subscriber stays None (test-safe)."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service.settings = MagicMock(zmq_broker_xpub=MagicMock())
        service._setup_subscriber()
        assert service._subscriber is None
        assert service._zmq_context is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_setup_subscriber_creates_real_socket(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a real broker endpoint, Then subscriber + context are created."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service.settings = MagicMock(zmq_broker_xpub="tcp://127.0.0.1:0")
        service._setup_subscriber()
        try:
            assert service._subscriber is not None
            assert service._zmq_context is not None
        finally:
            await service.stop()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_stop_closes_subscriber_and_context(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Stop closes the subscriber + terminates the ZMQ context if set."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        sub_mock = MagicMock()
        ctx_mock = MagicMock()
        service._subscriber = sub_mock
        service._zmq_context = ctx_mock
        service._running = True
        await service.stop()
        assert service._running is False
        assert service._subscriber is None
        assert service._zmq_context is None
        sub_mock.close.assert_called_once()
        ctx_mock.term.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_noop_without_subscriber(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given no subscriber, Then _listen_loop returns immediately."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._subscriber = None
        await service._listen_loop()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_routes_execution_to_handler(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A parsed ExecutionData frame is dispatched to _handle_execution."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        handled: list[ExecutionData] = []

        async def _stub(execution: ExecutionData) -> None:
            handled.append(execution)
            service._running = False

        service._handle_execution = _stub
        service._handle_order_status = AsyncMock()
        service._handle_tick = AsyncMock()
        subscriber = AsyncMock()
        execution = _make_execution()
        subscriber.recv_multipart = AsyncMock(
            return_value=(b"orders.events.kraken.BTC-USD.executed", execution.to_json().encode())
        )
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert len(handled) == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_routes_order_status_to_handler(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A parsed OrderData frame is dispatched to _handle_order_status."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        handled: list[OrderData] = []

        async def _stub(order: OrderData) -> None:
            handled.append(order)
            service._running = False

        service._handle_execution = AsyncMock()
        service._handle_order_status = _stub
        service._handle_tick = AsyncMock()
        subscriber = AsyncMock()
        order_msg = _make_order_status(status="cancelled")
        from typing import cast as _cast

        order = _cast(OrderData, order_msg)
        subscriber.recv_multipart = AsyncMock(
            return_value=(
                b"orders.events.kraken.BTC-USD.cancelled",
                order.to_json().encode(),
            )
        )
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert len(handled) == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_routes_tick_to_handler(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A parsed TickData frame is dispatched to _handle_tick."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        routed: list[tuple[str, TickData]] = []

        async def _stub(topic: str, tick: TickData) -> None:
            routed.append((topic, tick))
            service._running = False

        service._handle_execution = AsyncMock()
        service._handle_order_status = AsyncMock()
        service._handle_tick = _stub
        subscriber = AsyncMock()
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="BTC-USD",
            exchange="kraken",
            volume=0.0,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
        )
        subscriber.recv_multipart = AsyncMock(
            return_value=(b"market.kraken.BTC-USD.ticks", tick.to_json().encode())
        )
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert len(routed) == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_skips_unparseable_payload(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a corrupt payload, the loop continues instead of crashing."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        call_count = 0

        async def _recv() -> tuple[bytes, bytes]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return b"orders.events.kraken.BTC-USD.executed", b"not json"
            service._running = False
            return b"orders.events.kraken.BTC-USD.executed", b"{}"

        subscriber = AsyncMock()
        subscriber.recv_multipart = _recv
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert call_count >= 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_continues_after_handler_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A per-message handler exception is logged and the loop keeps running.

        Previously a raised exception was caught only by the outer
        ``except Exception`` handler, which killed the loop while
        ``_running`` stayed ``True`` — making the service look healthy
        while silently stopping to consume events. The loop now wraps
        each message in a try/except so a single bad frame cannot take
        the service down (Phase 1.5 review fix).
        """
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        handler_calls = 0

        async def _boom(_execution: ExecutionData) -> None:
            nonlocal handler_calls
            handler_calls += 1
            if handler_calls >= 2:
                service._running = False
            raise RuntimeError("boom")

        service._handle_execution = _boom
        subscriber = AsyncMock()
        execution = _make_execution()
        subscriber.recv_multipart = AsyncMock(
            return_value=(
                b"orders.events.kraken.BTC-USD.executed",
                execution.to_json().encode(),
            )
        )
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert handler_calls >= 2

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_handler_cancelled_error_unwinds(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A CancelledError from a handler must propagate, not be caught."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()

        async def _cancelled(_execution: ExecutionData) -> None:
            raise asyncio.CancelledError()

        service._handle_execution = _cancelled
        subscriber = AsyncMock()
        execution = _make_execution()
        subscriber.recv_multipart = AsyncMock(
            return_value=(
                b"orders.events.kraken.BTC-USD.executed",
                execution.to_json().encode(),
            )
        )
        service._subscriber = subscriber
        service._running = True
        with pytest.raises(asyncio.CancelledError):
            await service._listen_loop()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_recovers_from_socket_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A transient socket recv failure is logged and the loop retries.

        Phase 1.5 review fix: previously an exception from ``recv_multipart``
        fell out of the outer handler and silently exited the loop.
        """
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._handle_execution = AsyncMock()
        call_count = 0

        async def _recv() -> tuple[bytes, bytes]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient socket failure")
            service._running = False
            execution = _make_execution()
            return b"orders.events.kraken.BTC-USD.executed", execution.to_json().encode()

        subscriber = AsyncMock()
        subscriber.recv_multipart = _recv
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert call_count == 2

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_checkpoint_loop_invokes_write_checkpoints(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """The checkpoint loop writes checkpoints periodically until stopped."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._running = True

        write_calls = 0

        async def _stub_write() -> None:
            nonlocal write_calls
            write_calls += 1
            service._running = False

        service._write_checkpoints = _stub_write

        async def _fast_sleep(_seconds: float) -> None:
            return

        with patch("snapper.application.plans.service.asyncio.sleep", side_effect=_fast_sleep):
            await service._checkpoint_loop()
        assert write_calls == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_plan_missing_from_memory(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given an index entry but no in-memory plan, _handle_execution is a no-op."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service._client_order_id_index["cid-1"] = "plan-1"
        execution = _make_execution()
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_execution_without_evaluator(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """When an evaluator is missing the fill still propagates into the plan."""
        from typing import cast as _cast

        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        public_id = _cast(str, plan["public_id"])
        service.plans[public_id] = _cast(ExecutionPlanRow, plan)
        params = plan["params"] or {}
        child_id = params.get("child_client_order_id") if isinstance(params, dict) else None
        if isinstance(child_id, str):
            service._client_order_id_index[child_id] = public_id
        execution = _make_execution()
        await service._handle_execution(execution)
        mock_repo.update_execution_plan_status.assert_called_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_plan_missing_from_memory(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given an index entry but no plan row, _handle_order_status is a no-op."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service._client_order_id_index["cid-1"] = "plan-1"
        order_event = _make_order_status(status="cancelled")
        await service._handle_order_status(order_event)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_unknown_client_order_id(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given an order status for an unknown client_order_id, Then no DB write."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        order_event = _make_order_status(client_order_id="unknown")
        await service._handle_order_status(order_event)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_skips_terminal_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a terminal plan, venue status events do not update the plan."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="completed")
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="cancelled")
        await service._handle_order_status(order_event)
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_expired_transitions_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given an expired OrderData, plan transitions to expired."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="expired")
        await service._handle_order_status(order_event)
        call_kwargs = mock_repo.update_execution_plan_status.call_args[1]
        assert call_kwargs["new_status"] == "expired"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_order_status_swallows_db_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """DB failure while updating plan status is logged, not re-raised."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(side_effect=Exception("DB"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="cancelled")
        await service._handle_order_status(order_event)
        assert "plan-1" in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_tick_empty_plans_short_circuit(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given no plans, _handle_tick returns without touching state."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="BTC-USD",
            exchange="kraken",
            volume=0.0,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
        )
        await service._handle_tick("market.kraken.BTC-USD.ticks", tick)
        assert service._last_tick_timestamps == {}

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_tick_skips_mismatched_instrument(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Tick on different instrument does not touch unrelated plan state."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="ETH-USD",
            exchange="kraken",
            volume=0.0,
            bid=2000.0,
            ask=2001.0,
            last=2000.5,
        )
        await service._handle_tick("market.kraken.ETH-USD.ticks", tick)
        assert service._last_tick_timestamps.get("plan-1") is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_tick_skips_mismatched_exchange(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Tick on different exchange does not touch plan state."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="inst-1",
            exchange="zonda",
            volume=0.0,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
        )
        await service._handle_tick("market.zonda.inst-1.ticks", tick)
        assert service._last_tick_timestamps.get("plan-1") is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_register_plan_without_child_client_order_id(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans without a stamped child id are indexed by plan id only."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(child_client_order_id=None)
        service._register_plan(plan, ManualOnceEvaluator())
        assert service._client_order_id_index == {}
        assert "plan-1" in service.plans
        service._unregister_plan("plan-1")
        assert "plan-1" not in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unregister_plan_missing_is_safe(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregistering a plan not present is a safe no-op."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._unregister_plan("does-not-exist")
        assert service.plans == {}

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_ignores_unknown_message_type(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Messages that parse but do not match fill/status/tick are skipped."""
        from snapper.messaging.schemas.data import SignalData

        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._handle_execution = AsyncMock()
        service._handle_order_status = AsyncMock()
        service._handle_tick = AsyncMock()
        call_count = 0

        async def _recv() -> tuple[bytes, bytes]:
            nonlocal call_count
            call_count += 1
            service._running = False
            signal = SignalData(
                public_id="s-1",
                timestamp=datetime(2026, 4, 10, tzinfo=UTC),
                session_id="s1",
                sequence_id=1,
                instrument="BTC-USD",
                exchange="kraken",
                side="buy",
                strength=1.0,
                reason="noise",
                price=50000.0,
                strategy_name="noop",
                fired_at=datetime(2026, 4, 10, tzinfo=UTC),
            )
            return b"signals.kraken.BTC-USD.noop", signal.to_json().encode()

        subscriber = AsyncMock()
        subscriber.recv_multipart = _recv
        service._subscriber = subscriber
        service._running = True
        await service._listen_loop()
        assert call_count == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_listen_loop_reraises_cancelled_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """CancelledError from the subscriber is logged and re-raised."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        subscriber = AsyncMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        service._subscriber = subscriber
        service._running = True
        with pytest.raises(asyncio.CancelledError):
            await service._listen_loop()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_checkpoint_loop_reraises_cancelled_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """CancelledError from the checkpoint sleep is logged and re-raised."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._running = True

        async def _cancel_sleep(_seconds: float) -> None:
            raise asyncio.CancelledError()

        with patch(
            "snapper.application.plans.service.asyncio.sleep", side_effect=_cancel_sleep
        ), pytest.raises(asyncio.CancelledError):
            await service._checkpoint_loop()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_handle_tick_skips_plan_without_evaluator(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans without an evaluator do not receive tick dispatches."""
        from typing import cast as _cast

        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        service.plans[_cast(str, plan["public_id"])] = _cast(ExecutionPlanRow, plan)
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="inst-1",
            exchange="kraken",
            volume=0.0,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
        )
        await service._handle_tick("market.kraken.inst-1.ticks", tick)
        assert service._last_tick_timestamps.get("plan-1") is None
