"""Tests for PlanExecutorService."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import cast as _cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.plans.service import _EVALUATOR_REGISTRY
from snapper.application.plans.service import PlanExecutorService
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import TickData
from snapper.server.order_routes import _plan_to_data


def _make_plan_row(
    public_id: str = "plan-1",
    plan_type: str = "manual_once",
    status: str = "active",
    total_quantity: float = 1.0,
    filled_quantity: float = 0.0,
    child_client_order_id: str | None = "cid-1",
    native_instrument: str | None = None,
    venue_order_type: str | None = None,
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
    if native_instrument is not None:
        params["native_instrument"] = native_instrument
    if venue_order_type is not None:
        params["venue_order_type"] = venue_order_type
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
        mock_repo.get_latest_checkpoints_for_plans = AsyncMock(return_value={})
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        assert "plan-1" in service.plans
        assert isinstance(service.evaluators["plan-1"], ManualOnceEvaluator)

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_recover_plans_reemits_stranded_cancel(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A cancel_requested plan with no cancel TradeCommand is re-emitted.

        If the REST cancel route crashed after flipping the plan to
        cancel_requested but before inserting
        the cancel command, the plan would be stranded forever. On
        startup recovery the service now re-emits the cancel.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(
            return_value=[
                _make_plan_row(
                    public_id="plan-stranded",
                    status="cancel_requested",
                    child_client_order_id="cid-strand",
                    native_instrument="BTC-USD",
                    venue_order_type="limit",
                )
            ]
        )
        mock_repo.get_latest_checkpoints_for_plans = AsyncMock(return_value={})
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-stranded")
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=False)
        mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-77")
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-77"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        mock_repo.insert_trade_command.assert_awaited_once()
        inserted = mock_repo.insert_trade_command.await_args[0][0]
        assert inserted["command_type"] == "cancel"
        assert inserted["client_order_id"] == "cid-strand"
        assert inserted["plan_public_id"] == "plan-stranded"
        assert inserted["exchange_order_id"] == "ex-77"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_recover_plans_skips_stranded_cancel_without_metadata(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A cancel_requested plan with no child_client_order_id is a no-op.

        Legacy or test-only plan rows missing ``child_client_order_id``
        or ``native_instrument`` cannot be re-emitted — the service
        leaves them alone rather than crashing.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(
            return_value=[
                _make_plan_row(
                    public_id="plan-old",
                    status="cancel_requested",
                    child_client_order_id=None,
                )
            ]
        )
        mock_repo.get_latest_checkpoints_for_plans = AsyncMock(return_value={})
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_swallows_lookup_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Lookup failures while re-emitting cancels are logged, not raised."""
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(side_effect=Exception("DB"))
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-err",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        await service._reemit_stranded_cancel(plan)
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_swallows_venue_lookup_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Venue-id lookup failure falls back to None exchange_order_id."""
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-x")
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=False)
        mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(side_effect=Exception("DB"))
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-x",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        await service._reemit_stranded_cancel(plan)
        inserted = mock_repo.insert_trade_command.await_args[0][0]
        assert inserted["exchange_order_id"] is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_swallows_insert_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A failing re-emit insert is logged and does not crash recovery."""
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-y")
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=False)
        mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-9")
        mock_repo.insert_trade_command = AsyncMock(side_effect=Exception("DB"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-y",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        await service._reemit_stranded_cancel(plan)

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_skips_when_no_plan_link(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """If the plan_public_id lookup returns None, no cancel is emitted."""
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value=None)
        mock_repo.insert_trade_command = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-z",
            status="cancel_requested",
            native_instrument="BTC-USD",
        )
        await service._reemit_stranded_cancel(plan)
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_skips_on_plan_id_mismatch(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A corrupt child_client_order_id linking to a different plan is skipped.

        Round 3 fix: verify the lookup result matches the recovered
        plan's public_id before emitting a cancel so a mis-stamped child
        id does not cause a cancel to be emitted for the wrong plan.
        """
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-OTHER")
        mock_repo.insert_trade_command = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-mine",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        await service._reemit_stranded_cancel(plan)
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_dedupes_when_pending_cancel_exists(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Recovery does not re-emit a cancel if one is already pending.

        Round 3 fix: every restart while a plan is still in
        cancel_requested would otherwise enqueue a duplicate venue
        cancel. ``has_pending_cancel_command`` is the idempotency
        guard.
        """
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-dedup")
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=True)
        mock_repo.insert_trade_command = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-dedup",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        await service._reemit_stranded_cancel(plan)
        mock_repo.insert_trade_command.assert_not_called()
        mock_repo.has_pending_cancel_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_swallows_dedup_lookup_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A failing dedup lookup does not crash recovery and skips emission."""
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-err")
        mock_repo.has_pending_cancel_command = AsyncMock(side_effect=Exception("DB"))
        mock_repo.insert_trade_command = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-err",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        await service._reemit_stranded_cancel(plan)
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_start_sets_up_subscriber_before_recovery(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """``start()`` wires the ZMQ subscriber BEFORE recovery runs.

        Round 3 fix: if a stranded cancel is re-emitted during
        recovery, the venue can publish the cancelled event before the
        service's subscriber is ready. Subscriber must be up first.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(return_value=[])
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        order: list[str] = []

        def _setup() -> None:
            order.append("setup")

        async def _recover() -> None:
            order.append("recover")

        async def _stop_after_start() -> None:
            service._running = False

        service._setup_subscriber = _setup
        service._recover_plans = _recover
        with patch.object(service, "_run_loop", side_effect=_stop_after_start):
            await service.start()
        assert order == ["setup", "recover"]

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_start_sleeps_for_slow_joiner_when_subscriber_active(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Round 4 fix: start() awaits a slow-joiner sleep after subscriber setup.

        When the subscriber is connected, ``start()`` sleeps briefly
        so the XPUB/XSUB broker can propagate the subscription before
        recovery starts publishing stranded cancels. Without this
        window, the cancelled event can arrive while the broker has
        not yet routed the SUB filter to this socket and the event is
        dropped.
        """
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(return_value=[])
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        def _setup() -> None:
            service._subscriber = MagicMock()

        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def _tracking_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            await real_sleep(0)

        async def _stop_after_start() -> None:
            service._running = False

        service._setup_subscriber = _setup
        with (
            patch(
                "snapper.application.plans.service.asyncio.sleep",
                side_effect=_tracking_sleep,
            ),
            patch.object(service, "_run_loop", side_effect=_stop_after_start),
        ):
            await service.start()
        assert any(s >= 0.5 for s in sleeps)

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_start_skips_slow_joiner_sleep_when_no_subscriber(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """When no subscriber is set up (unit tests), start() skips the sleep."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(return_value=[])
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        def _setup() -> None:
            service._subscriber = None

        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def _tracking_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            await real_sleep(0)

        async def _stop_after_start() -> None:
            service._running = False

        service._setup_subscriber = _setup
        with (
            patch(
                "snapper.application.plans.service.asyncio.sleep",
                side_effect=_tracking_sleep,
            ),
            patch.object(service, "_run_loop", side_effect=_stop_after_start),
        ):
            await service.start()
        assert all(s < 0.5 for s in sleeps)

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
        mock_repo.get_latest_checkpoints_for_plans = AsyncMock(
            return_value={
                "plan-1": {
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
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = ManualOnceEvaluator()
        service._register_plan(plan, evaluator)
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
    async def test_receive_message_frame_returns_none_without_subscriber(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A missing subscriber yields no frame instead of raising."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._subscriber = None
        assert await service._receive_message_frame() is None

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
    async def test_dispatch_incoming_message_ignores_runtime_unsupported_type(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Runtime-unsupported parsed payloads are ignored by dispatch."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._handle_execution = AsyncMock()
        service._handle_order_status = AsyncMock()
        service._handle_tick = AsyncMock()
        signal = SignalData(
            public_id="s-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            strength=1.0,
            reason="ignored",
            price=50000.0,
            strategy_name="noop",
            fired_at=datetime(2026, 4, 10, tzinfo=UTC),
        )
        await service._dispatch_incoming_message(
            "signals.kraken.BTC-USD.noop",
            _cast(ExecutionData | OrderData | TickData, signal),
        )
        service._handle_execution.assert_not_awaited()
        service._handle_order_status.assert_not_awaited()
        service._handle_tick.assert_not_awaited()

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
        the service down.
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

        Previously an exception from ``recv_multipart`` fell out of
        the outer handler and silently exited the loop.
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
        plan = _make_plan_row(native_instrument="BTC-USD")
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
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        tick = TickData(
            public_id="t-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            instrument="BTC-USD",
            exchange="walutomat",
            volume=0.0,
            bid=50000.0,
            ask=50001.0,
            last=50000.5,
        )
        await service._handle_tick("market.walutomat.BTC-USD.ticks", tick)
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
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service.plans[_cast(str, plan["public_id"])] = _cast(ExecutionPlanRow, plan)
        service._runtime_symbol_index.setdefault("kraken:BTC-USD", set()).add("plan-1")
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
        assert service._last_tick_timestamps.get("plan-1") is None


class TestDispatchCommands:
    """Tests for _dispatch_commands plumbing."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_commands_inserts_trade_command(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given evaluator returns a command, Then insert_trade_command is called."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        commands: list[dict[str, object]] = [
            {
                "instrument": "BTC-USD",
                "side": "sell",
                "quantity": 0.5,
                "order_type": "market",
                "reduce_only": True,
                "trigger_type": "tick",
                "reason": "SL triggered",
            }
        ]
        await service._dispatch_commands("plan-1", commands)
        mock_repo.insert_trade_command.assert_awaited_once()
        inserted = mock_repo.insert_trade_command.await_args[0][0]
        assert inserted["side"] == "sell"
        assert inserted["quantity"] == 0.5
        assert inserted["reduce_only"] is True
        assert inserted["plan_public_id"] == "plan-1"
        assert inserted["strategy_id"] == "manual_once"
        mock_repo.revise_execution_plan_params.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_commands_unknown_plan_noop(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a plan_public_id not in the plans dict, Then nothing happens."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._dispatch_commands(
            "nonexistent", [{"instrument": "X", "side": "buy", "quantity": 1}]
        )
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_commands_empty_list_no_insert(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given evaluator returns empty list, Then no trade command inserted."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        await service._dispatch_commands("plan-1", [])
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_commands_updates_client_order_id_index(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a command dispatch, Then _client_order_id_index is updated."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        commands: list[dict[str, object]] = [
            {"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}
        ]
        await service._dispatch_commands("plan-1", commands)
        inserted = mock_repo.insert_trade_command.await_args[0][0]
        cid = inserted["client_order_id"]
        assert service._client_order_id_index[cid] == "plan-1"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_commands_logs_decision(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given a command dispatch, Then a decision audit row is written."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        commands: list[dict[str, object]] = [
            {"instrument": "BTC-USD", "side": "sell", "quantity": 0.5, "reason": "SL hit"}
        ]
        await service._dispatch_commands("plan-1", commands)
        mock_repo.insert_execution_plan_decision.assert_awaited_once()
        dec_row = mock_repo.insert_execution_plan_decision.await_args[1]["row"]
        assert dec_row["decision_type"] == "command_emitted"
        assert dec_row["decision_importance"] == "action"


class TestBracketRegistry:
    """Tests for bracket evaluator registration."""

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_get_evaluator_bracket(self, mock_repo: MagicMock, mock_settings: MagicMock) -> None:
        """get_evaluator('bracket') returns BracketEvaluator."""
        service = PlanExecutorService()
        evaluator = service.get_evaluator("bracket")
        assert isinstance(evaluator, BracketEvaluator)


class TestMultiChildIds:
    """Tests for multi-child ID handling in register/unregister/dispatch."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_register_indexes_child_list(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans with child_client_order_ids list have all IDs indexed."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        _cast(dict, plan["params"])["child_client_order_ids"] = ["cid-a", "cid-b"]
        del _cast(dict, plan["params"])["child_client_order_id"]
        service._register_plan(plan, ManualOnceEvaluator())
        assert service._client_order_id_index["cid-a"] == "plan-1"
        assert service._client_order_id_index["cid-b"] == "plan-1"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unregister_cleans_child_list(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregister removes all IDs from child_client_order_ids list."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        _cast(dict, plan["params"])["child_client_order_ids"] = ["cid-a", "cid-b"]
        del _cast(dict, plan["params"])["child_client_order_id"]
        service._register_plan(plan, ManualOnceEvaluator())
        service._unregister_plan("plan-1")
        assert "cid-a" not in service._client_order_id_index
        assert "cid-b" not in service._client_order_id_index

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_register_falls_back_to_singular(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans with legacy child_client_order_id string still index correctly."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(child_client_order_id="legacy-cid")
        service._register_plan(plan, ManualOnceEvaluator())
        assert service._client_order_id_index["legacy-cid"] == "plan-1"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_stores_child_ids_as_list(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """dispatch_commands writes child_client_order_ids as list."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD", child_client_order_id=None)
        service._register_plan(plan, ManualOnceEvaluator())
        commands: list[dict[str, object]] = [
            {"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}
        ]
        await service._dispatch_commands("plan-1", commands)
        revision_call = mock_repo.revise_execution_plan_params.await_args
        param_updates = revision_call[1]["param_updates"]
        assert "child_client_order_ids" in param_updates
        assert isinstance(param_updates["child_client_order_ids"], list)
        assert len(param_updates["child_client_order_ids"]) == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_mixed_format_register_unions_both_keys(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans with BOTH singular and list child IDs index all of them."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        _cast(dict, plan["params"])["child_client_order_id"] = "legacy-cid"
        _cast(dict, plan["params"])["child_client_order_ids"] = ["new-cid-a", "new-cid-b"]
        service._register_plan(plan, ManualOnceEvaluator())
        assert service._client_order_id_index["legacy-cid"] == "plan-1"
        assert service._client_order_id_index["new-cid-a"] == "plan-1"
        assert service._client_order_id_index["new-cid-b"] == "plan-1"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_mixed_format_unregister_cleans_both_keys(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregister cleans IDs from both singular and list keys."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        _cast(dict, plan["params"])["child_client_order_id"] = "legacy-cid"
        _cast(dict, plan["params"])["child_client_order_ids"] = ["new-cid-a"]
        service._register_plan(plan, ManualOnceEvaluator())
        service._unregister_plan("plan-1")
        assert "legacy-cid" not in service._client_order_id_index
        assert "new-cid-a" not in service._client_order_id_index

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_merges_legacy_into_new_list(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """dispatch_commands includes legacy singular ID when building list."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        _cast(dict, plan["params"])["child_client_order_id"] = "legacy-cid"
        service._register_plan(plan, ManualOnceEvaluator())
        commands: list[dict[str, object]] = [
            {"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}
        ]
        await service._dispatch_commands("plan-1", commands)
        revision_call = mock_repo.revise_execution_plan_params.await_args
        merged = revision_call[1]["param_updates"]["child_client_order_ids"]
        assert "legacy-cid" in merged
        assert len(merged) == 2


class TestTickRoutingSymbolIndex:
    """Tests for _runtime_symbol_index tick routing (deliverable 1.5)."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_register_plan_adds_to_symbol_index(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Registering a plan with native_instrument populates the symbol index."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        assert "plan-1" in service._runtime_symbol_index["kraken:BTC-USD"]

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unregister_plan_removes_from_symbol_index(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregistering a plan removes it from the symbol index."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        service._unregister_plan("plan-1")
        assert "kraken:BTC-USD" not in service._runtime_symbol_index

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_multiple_plans_same_instrument(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Multiple plans on the same instrument all receive ticks."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan1 = _make_plan_row(public_id="plan-1", native_instrument="BTC-USD")
        plan2 = _make_plan_row(public_id="plan-2", native_instrument="BTC-USD")
        service._register_plan(plan1, ManualOnceEvaluator())
        service._register_plan(plan2, ManualOnceEvaluator())
        assert service._runtime_symbol_index["kraken:BTC-USD"] == {"plan-1", "plan-2"}

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_plan_without_native_instrument_not_in_index(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans without native_instrument are not added to symbol index."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        assert len(service._runtime_symbol_index) == 0


class TestClockLoop:
    """Tests for _clock_loop (deliverable 1.2)."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_calls_on_clock_for_active_plans(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Active plans receive on_clock calls."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="active")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_clock = AsyncMock(return_value=[])
        service._register_plan(plan, evaluator)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )
        evaluator.on_clock.assert_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_skips_paused_plans(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Paused plans do not receive on_clock calls."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="paused")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_clock = AsyncMock(return_value=[])
        service._register_plan(plan, evaluator)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )
        evaluator.on_clock.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_skips_terminal_plans(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Terminal plans do not receive on_clock calls."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="completed")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_clock = AsyncMock(return_value=[])
        service._register_plan(plan, evaluator)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )
        evaluator.on_clock.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_exception_does_not_break_loop(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """An exception in on_clock does not crash the loop."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="active")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_clock = AsyncMock(side_effect=RuntimeError("boom"))
        service._register_plan(plan, evaluator)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )


class TestCapabilityGating:
    """Tests for _check_capabilities (deliverable 1.6)."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_manual_once_requires_no_capabilities(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """ManualOnceEvaluator requires no capabilities — always passes."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        missing = await service._check_capabilities("manual_once", "kraken", "inst-1")
        assert missing == []
        mock_repo.get_instrument_capabilities.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unknown_plan_type_returns_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unknown plan type returns a missing capability."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        missing = await service._check_capabilities("nonexistent", "kraken", "inst-1")
        assert len(missing) == 1
        assert "unknown_plan_type" in missing[0]

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_no_capability_rows_returns_all_required(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """No capability data for the instrument → fail-closed (all required returned)."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        class _FakeEvaluator(ManualOnceEvaluator):
            def requires_capabilities(self) -> list[str]:
                return ["supports_reduce_only"]

        _EVALUATOR_REGISTRY["_test_bracket"] = _FakeEvaluator
        try:
            missing = await service._check_capabilities("_test_bracket", "kraken", "inst-1")
            assert missing == ["supports_reduce_only"]
        finally:
            del _EVALUATOR_REGISTRY["_test_bracket"]


class TestExecutionPlanDataFields:
    """Tests for ExecutionPlanData response field additions (deliverable 1.7)."""

    def test_plan_to_data_includes_new_fields(self) -> None:
        """_plan_to_data maps position_cycle_public_id and parent_plan_public_id."""
        plan = _make_plan_row()
        plan["position_cycle_public_id"] = "cycle-99"
        plan["parent_plan_public_id"] = "parent-1"
        data = _plan_to_data(_cast(ExecutionPlanRow, plan))
        assert data.position_cycle_public_id == "cycle-99"
        assert data.parent_plan_public_id == "parent-1"

    def test_plan_to_data_none_when_absent(self) -> None:
        """_plan_to_data returns None for optional fields when not set."""
        plan = _make_plan_row()
        data = _plan_to_data(_cast(ExecutionPlanRow, plan))
        assert data.position_cycle_public_id is None
        assert data.parent_plan_public_id is None


class TestUnregisterPlanSymbolIndex:
    """Tests for _unregister_plan symbol index cleanup."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unregister_removes_last_plan_cleans_key(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregistering the last plan on a key removes the key entirely."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        assert "kraken:BTC-USD" in service._runtime_symbol_index
        service._unregister_plan("plan-1")
        assert "kraken:BTC-USD" not in service._runtime_symbol_index

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unregister_keeps_other_plans_on_same_key(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregistering one plan leaves others on the same key."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan1 = _make_plan_row(public_id="p1", native_instrument="BTC-USD")
        plan2 = _make_plan_row(public_id="p2", native_instrument="BTC-USD")
        service._register_plan(plan1, ManualOnceEvaluator())
        service._register_plan(plan2, ManualOnceEvaluator())
        service._unregister_plan("p1")
        assert service._runtime_symbol_index["kraken:BTC-USD"] == {"p2"}


class TestUnregisterPlanStaleIndex:
    """Tests for unregister with stale symbol index state."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unregister_with_missing_symbol_index_key(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Unregistering a plan whose symbol index key was already removed is safe."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        del service._runtime_symbol_index["kraken:BTC-USD"]
        service._unregister_plan("plan-1")
        assert "plan-1" not in service.plans


class TestGetPlanLock:
    """Tests for _get_plan_lock fallback creation."""

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_get_plan_lock_creates_when_missing(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Lock is created on demand when not pre-registered."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        lock = service._get_plan_lock("unknown-plan")
        assert isinstance(lock, asyncio.Lock)
        assert service._plan_locks["unknown-plan"] is lock


class TestLogDecisionError:
    """Tests for _log_decision exception handling."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_log_decision_swallows_db_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """DB errors in _log_decision are logged, not raised."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(side_effect=Exception("DB down"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._log_decision(
            plan_public_id="plan-1",
            decision_type="test",
            trigger_type="tick",
            reason="test reason",
            importance="action",
        )


class TestSetupPublisher:
    """BE-3b self-bootstrap publisher covers both no-broker + reuse-context paths."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_setup_publisher_skips_when_no_broker_xsub(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """No broker XSUB endpoint → publisher stays None (no-op)."""
        mock_settings.return_value = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub=None,
            zmq_broker_xpub=None,
        )
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()

        service._setup_publisher()

        assert service._publisher is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_setup_publisher_reuses_existing_zmq_context(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """When subscriber already created a context, publisher reuses it."""
        mock_settings.return_value = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub="tcp://127.0.0.1:7500",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
        )
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        pre_existing_ctx = MagicMock()
        pre_existing_ctx.socket = MagicMock(return_value=MagicMock())
        service._zmq_context = pre_existing_ctx

        service._setup_publisher()

        assert service._zmq_context is pre_existing_ctx
        pre_existing_ctx.socket.assert_called_once()
        assert service._publisher is not None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_setup_publisher_closes_raw_socket_on_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A connect failure in ``_setup_publisher`` closes the raw socket.

        BE-3b R1 recommendation #1: if ``raw_pub_socket.connect`` raises
        (e.g. bad broker address), the socket is explicitly closed
        before the exception propagates so the context is not left
        holding a dangling socket.
        """
        mock_settings.return_value = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub="tcp://invalid-broker:9999",
            zmq_broker_xpub="tcp://invalid-broker:9998",
        )
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        bad_ctx = MagicMock()
        bad_socket = MagicMock()
        bad_socket.connect = MagicMock(side_effect=RuntimeError("connect boom"))
        bad_ctx.socket = MagicMock(return_value=bad_socket)
        service._zmq_context = bad_ctx

        with pytest.raises(RuntimeError, match="connect boom"):
            service._setup_publisher()

        bad_socket.close.assert_called_once()
        assert service._publisher is None


class TestLogDecisionPublishesEvent:
    """``_log_decision`` publishes ``plans.decisions.*`` after insert."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_publishes_on_sl_hit(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """``sl_hit`` reason triggers a ``plans.decisions.*`` publish."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="decision-pid-1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        service._publisher = fake_publisher

        await service._log_decision(
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            decision_type="evaluator",
            trigger_type="tick",
            reason="sl_hit",
            importance="action",
        )

        fake_publisher.send_multipart.assert_awaited_once()
        call_kwargs = fake_publisher.send_multipart.await_args.kwargs
        assert call_kwargs["topic"] == "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60307"
        assert b'"reason":"sl_hit"' in call_kwargs["payload"]

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_publishes_on_trailing_stop_hit(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """``trailing_stop_hit`` also fires the publish hook identically."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="decision-pid-2")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        service._publisher = fake_publisher

        await service._log_decision(
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60308",
            decision_type="evaluator",
            trigger_type="tick",
            reason="trailing_stop_hit",
            importance="action",
        )

        fake_publisher.send_multipart.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_publishes_with_correct_plan_public_id_field(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Published payload uses ``plan_public_id`` (Plan v1.12 R10.B-2)."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="decision-pid-3")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        service._publisher = fake_publisher

        await service._log_decision(
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60309",
            decision_type="lifecycle",
            trigger_type="execution",
            reason="Cycle 42 closed before command dispatch",
            importance="transition",
        )

        payload = fake_publisher.send_multipart.await_args.kwargs["payload"]
        assert b'"plan_public_id":"019dbb34-f439-77bd-afa8-ee5321d60309"' in payload
        assert b"execution_plan_public_id" not in payload

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_published_payload_carries_decision_topic(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """The published payload carries ``topic`` matching the decision topic.

        Chokepoint contract: every production publish call site
        routes serialization through ``StrictDataSchema.publish_to`` so
        downstream consumers see the routing key on the payload itself.
        """
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="decision-pid-T")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        service._publisher = fake_publisher

        await service._log_decision(
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60310",
            decision_type="evaluator",
            trigger_type="tick",
            reason="sl_hit",
            importance="action",
        )

        call_kwargs = fake_publisher.send_multipart.await_args.kwargs
        decision_topic = "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60310"
        assert call_kwargs["topic"] == decision_topic
        as_dict = json.loads(call_kwargs["payload"])
        assert as_dict["topic"] == decision_topic

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_publish_failure_logs_warning_does_not_raise(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Publisher exceptions are caught — DB insert is the source of truth."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="decision-pid-4")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        fake_publisher.send_multipart = AsyncMock(side_effect=RuntimeError("broker down"))
        service._publisher = fake_publisher

        await service._log_decision(
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d6030a",
            decision_type="evaluator",
            trigger_type="tick",
            reason="sl_hit",
            importance="action",
        )

        fake_publisher.send_multipart.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_no_publish_when_publisher_unconfigured(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """``_publisher = None`` short-circuits the publish path."""
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="decision-pid-5")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        assert service._publisher is None

        await service._log_decision(
            plan_public_id="plan-x",
            decision_type="evaluator",
            trigger_type="tick",
            reason="sl_hit",
            importance="action",
        )


class TestCheckCapabilitiesWithRows:
    """Tests for _check_capabilities when capability rows exist."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_capability_present_and_true(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Capability flag True in row → empty missing list."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(
            return_value=[{"supports_reduce_only": True}]
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        class _FakeEval(ManualOnceEvaluator):
            def requires_capabilities(self) -> list[str]:
                return ["supports_reduce_only"]

        _EVALUATOR_REGISTRY["_test_cap"] = _FakeEval
        try:
            missing = await service._check_capabilities("_test_cap", "kraken", "inst-1")
            assert missing == []
        finally:
            del _EVALUATOR_REGISTRY["_test_cap"]

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_capability_present_and_false(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Capability flag False in row → returned as missing."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(
            return_value=[{"supports_reduce_only": False}]
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        class _FakeEval(ManualOnceEvaluator):
            def requires_capabilities(self) -> list[str]:
                return ["supports_reduce_only"]

        _EVALUATOR_REGISTRY["_test_cap2"] = _FakeEval
        try:
            missing = await service._check_capabilities("_test_cap2", "kraken", "inst-1")
            assert missing == ["supports_reduce_only"]
        finally:
            del _EVALUATOR_REGISTRY["_test_cap2"]


class TestClockLoopDispatchAndEdgeCases:
    """Tests for clock loop dispatch and edge cases."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_dispatches_commands(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Clock loop dispatches commands returned by on_clock."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="active", native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_clock = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )
        mock_repo.insert_trade_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_skips_plan_without_evaluator(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Clock loop skips plans with no evaluator in the dict."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="active")
        service.plans["plan-1"] = _cast(ExecutionPlanRow, plan)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_dispatch_exception_is_caught(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A raised dispatch during on_clock is logged and does not crash the loop."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="active", native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_clock = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
        service._dispatch_commands = AsyncMock(side_effect=RuntimeError("dispatch boom"))
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )

        service._dispatch_commands.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_loop_cancelled_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Clock loop re-raises CancelledError for clean shutdown."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._running = True
        task = asyncio.create_task(service._clock_loop())
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


class TestTickRoutingStaleIndex:
    """Tests for tick routing when symbol index has stale entries."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_tick_skips_plan_not_in_plans_dict(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Stale symbol index entry (plan removed from dict) does not crash."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._runtime_symbol_index["kraken:BTC-USD"] = {"ghost-plan"}
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


class TestHandleTickStatusGuards:
    """Tests for F1: _handle_tick must skip terminal and paused plans."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_tick_skips_terminal_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Terminal plans do not receive tick dispatches."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="completed", native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_tick = AsyncMock(return_value=[])
        service._register_plan(plan, evaluator)
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
        evaluator.on_tick.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_tick_skips_paused_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Paused plans do not receive tick dispatches."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(status="paused", native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_tick = AsyncMock(return_value=[])
        service._register_plan(plan, evaluator)
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
        evaluator.on_tick.assert_not_awaited()


class TestTickEvaluatorErrorLogging:
    """Tests for F6: evaluator exceptions must be logged, not silently suppressed."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_tick_evaluator_error_logged_not_silent(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """on_tick exception is caught and logged, not silently suppressed."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_tick = AsyncMock(side_effect=RuntimeError("boom"))
        service._register_plan(plan, evaluator)
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

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_execution_evaluator_error_logged_not_silent(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """on_execution exception is caught and logged, not silently suppressed."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_execution = AsyncMock(side_effect=RuntimeError("boom"))
        service._register_plan(plan, evaluator)
        execution = _make_execution()
        await service._handle_execution(_cast(ExecutionData, execution))


class TestDispatchFailureIsolation:
    """Tests for F7: dispatch failure must not break sibling plans."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_tick_dispatch_failure_does_not_break_siblings(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """If dispatch fails for plan A, plan B on the same symbol still gets its tick."""
        mock_repo = AsyncMock()
        call_count = 0

        async def _insert_side_effect(
            row: object, *, ownership: object | None = None
        ) -> tuple[int, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("DB down for plan A")
            return (2, "cmd-2")

        mock_repo.insert_trade_command = AsyncMock(side_effect=_insert_side_effect)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan_a = _make_plan_row(public_id="plan-a", native_instrument="BTC-USD")
        plan_b = _make_plan_row(public_id="plan-b", native_instrument="BTC-USD")
        eval_a = AsyncMock(spec=ManualOnceEvaluator)
        eval_a.on_tick = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        eval_b = AsyncMock(spec=ManualOnceEvaluator)
        eval_b.on_tick = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan_a, eval_a)
        service._register_plan(plan_b, eval_b)
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
        assert mock_repo.insert_trade_command.await_count == 2

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_dispatch_failure_does_not_break_siblings(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """If clock dispatch fails for plan A, plan B still gets its clock tick."""
        mock_repo = AsyncMock()
        call_count = 0

        async def _insert_side_effect(
            row: object, *, ownership: object | None = None
        ) -> tuple[int, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("DB down for plan A")
            return (2, "cmd-2")

        mock_repo.insert_trade_command = AsyncMock(side_effect=_insert_side_effect)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan_a = _make_plan_row(public_id="plan-a", status="active")
        plan_b = _make_plan_row(public_id="plan-b", status="active")
        eval_a = AsyncMock(spec=ManualOnceEvaluator)
        eval_a.on_clock = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        eval_b = AsyncMock(spec=ManualOnceEvaluator)
        eval_b.on_clock = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan_a, eval_a)
        service._register_plan(plan_b, eval_b)
        service._running = True

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(
            service._clock_loop(),
            stop_after_one_tick(),
        )
        assert mock_repo.insert_trade_command.await_count == 2

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_partial_command_insert_failure_keeps_plan_active(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A later command insert failure does not fail the plan after one child was emitted."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        call_count = 0

        async def _insert_side_effect(
            _row: object, *, ownership: object | None = None
        ) -> tuple[int, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("DB down for second command")
            return (1, "cmd-1")

        mock_repo.insert_trade_command = AsyncMock(side_effect=_insert_side_effect)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            status="armed", child_client_order_id=None, native_instrument="BTC-USD"
        )
        service._register_plan(plan, ManualOnceEvaluator())

        await service._dispatch_commands(
            "plan-1",
            [
                {"instrument": "BTC-USD", "side": "sell", "quantity": 0.5},
                {"instrument": "BTC-USD", "side": "sell", "quantity": 0.25},
            ],
        )

        assert service.plans["plan-1"]["status"] == "active"
        assert mock_repo.update_execution_plan_status.await_count == 1
        mock_repo.revise_execution_plan_params.assert_not_called()


class TestOnTickDispatchesCommands:
    """Tests that on_tick return values are captured and dispatched."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_on_tick_commands_dispatched(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given evaluator.on_tick returns commands, Then _dispatch_commands is called."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_tick = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
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
        mock_repo.insert_trade_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_on_tick_empty_no_dispatch(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given evaluator.on_tick returns empty list, Then no command dispatched."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_tick = AsyncMock(return_value=[])
        service._register_plan(plan, evaluator)
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
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_on_tick_dispatch_exception_is_caught(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A raised dispatch during on_tick is logged and the timestamp still advances."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_tick = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
        service._dispatch_commands = AsyncMock(side_effect=RuntimeError("dispatch boom"))
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

        service._dispatch_commands.assert_awaited_once()
        assert "plan-1" in service._last_tick_timestamps


class TestOnExecutionDispatchesCommands:
    """Tests that on_execution return values are captured and dispatched."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_on_execution_commands_dispatched(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given evaluator.on_execution returns commands, Then commands are dispatched."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="dec-1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_execution = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
        execution = _make_execution()
        await service._handle_execution(_cast(ExecutionData, execution))
        mock_repo.insert_trade_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_execution_dispatch_failure_does_not_crash(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """If dispatch fails during execution, plan compensated to failed + fill still updates."""
        mock_repo = AsyncMock()
        mock_repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("DB down"))
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_execution = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
        execution = _make_execution()
        await service._handle_execution(_cast(ExecutionData, execution))
        assert mock_repo.update_execution_plan_status.await_count >= 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_on_execution_dispatch_exception_is_caught(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A raised dispatch during on_execution is logged and does not abort fill processing."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(native_instrument="BTC-USD")
        evaluator = AsyncMock(spec=ManualOnceEvaluator)
        evaluator.on_execution = AsyncMock(
            return_value=[{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        service._register_plan(plan, evaluator)
        service._dispatch_commands = AsyncMock(side_effect=RuntimeError("dispatch boom"))

        await service._handle_execution(_cast(ExecutionData, _make_execution()))

        service._dispatch_commands.assert_awaited_once()
        assert mock_repo.update_execution_plan_status.await_count >= 1


class TestExecutionHelpers:
    """Tests for extracted execution-routing helpers."""

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_resolve_execution_target_rejects_duplicate_cumulative_fill(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Duplicate cumulative fills are filtered before evaluator dispatch."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _cast(ExecutionPlanRow, _make_plan_row(filled_quantity=1.0))
        service._register_plan(plan, ManualOnceEvaluator())

        target = service._resolve_execution_target(_make_execution(size=1.0))

        assert target is None

    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    def test_resolve_execution_target_returns_plan_for_new_fill(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """New cumulative fills return the active plan lookup tuple."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        plan = _cast(ExecutionPlanRow, _make_plan_row(filled_quantity=0.25))
        service._register_plan(plan, ManualOnceEvaluator())

        target = service._resolve_execution_target(_make_execution(size=0.5))

        assert target is not None
        plan_public_id, resolved_plan, incoming_cumulative = target
        assert plan_public_id == "plan-1"
        assert resolved_plan["public_id"] == "plan-1"
        assert incoming_cumulative == pytest.approx(0.5)

    @pytest.mark.parametrize(
        ("total_quantity", "incoming_cumulative", "execution_status", "expected_complete"),
        [
            (1.0, 0.5, "partial", False),
            (1.0, 1.0, "partial", True),
            (2.0, 0.5, "filled", True),
        ],
    )
    def test_classify_fill_update_returns_expected_completion(
        self,
        total_quantity: float,
        incoming_cumulative: float,
        execution_status: str,
        expected_complete: bool,
    ) -> None:
        """Fill classification handles partial, quantity-complete, and venue-complete paths."""
        plan = _cast(ExecutionPlanRow, _make_plan_row(total_quantity=total_quantity))

        new_filled, total, is_complete, new_status = PlanExecutorService._classify_fill_update(
            plan,
            incoming_cumulative,
            execution_status,
        )

        assert new_filled == pytest.approx(incoming_cumulative)
        assert total == pytest.approx(total_quantity)
        assert is_complete is expected_complete
        expected_status = "completed" if expected_complete else "active"
        assert new_status == expected_status


class TestTickHelpers:
    """Tests for extracted tick-routing helpers."""

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            ("active", True),
            ("paused", False),
            ("completed", False),
        ],
    )
    def test_is_tick_dispatchable_respects_plan_status(
        self,
        status: str,
        expected: bool,
    ) -> None:
        """Tick dispatch helper filters paused and terminal plans."""
        plan = _cast(ExecutionPlanRow, _make_plan_row(status=status))
        assert PlanExecutorService._is_tick_dispatchable(plan) is expected
        assert PlanExecutorService._is_tick_dispatchable(None) is False


class TestTransitionPlan:
    """Tests for _transition_plan helper."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_transition_updates_status_in_memory(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """_transition_plan updates in-memory plan status."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="armed", native_instrument="BTC-USD")
        service._register_plan(plan, ManualOnceEvaluator())
        await service._transition_plan("plan-1", "active")
        assert service.plans["plan-1"]["status"] == "active"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_transition_to_terminal_unregisters(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Terminal transition unregisters the plan."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="armed")
        service._register_plan(plan, ManualOnceEvaluator())
        await service._transition_plan("plan-1", "cancelled", "test cancel")
        assert "plan-1" not in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_transition_db_error_logged(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """DB error in transition is logged, plan stays in memory."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(side_effect=RuntimeError("DB"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="armed")
        service._register_plan(plan, ManualOnceEvaluator())
        await service._transition_plan("plan-1", "failed", "test error")
        assert "plan-1" in service.plans


class TestFireTimeCapabilityGating:
    """Tests for fire-time capability check in _dispatch_commands."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_capability_revoked_fails_plan(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Capability revoked between attach and fire → plan fails."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(
            return_value=[{"supports_reduce_only": False}]
        )
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        class _FakeEval(ManualOnceEvaluator):
            def requires_capabilities(self) -> list[str]:
                return ["supports_reduce_only"]

        _EVALUATOR_REGISTRY["_test_cap_fire"] = _FakeEval
        try:
            plan = _make_plan_row(
                plan_type="_test_cap_fire", status="armed", native_instrument="BTC-USD"
            )
            service._register_plan(plan, _FakeEval())
            await service._dispatch_commands(
                "plan-1", [{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
            )
            assert service.plans.get("plan-1") is None
        finally:
            del _EVALUATOR_REGISTRY["_test_cap_fire"]


class TestCycleCloseDispatchGuard:
    """Tests for cycle-close check in _dispatch_commands."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_closed_cycle_cancels_bracket(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Closed cycle before dispatch → bracket cancelled."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "closed", "public_id": "cycle-1"}
        )
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="armed", native_instrument="BTC-USD")
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())
        await service._dispatch_commands(
            "plan-1", [{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        assert service.plans.get("plan-1") is None
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_open_cycle_proceeds_normally(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Open cycle allows dispatch to proceed."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "open", "public_id": "cycle-1"}
        )
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo.revise_execution_plan_params = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            status="armed", native_instrument="BTC-USD", child_client_order_id=None
        )
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())
        await service._dispatch_commands(
            "plan-1", [{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        mock_repo.insert_trade_command.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_cycle_lookup_error_cancels_bracket(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """DB error on cycle lookup → fail-closed, bracket cancelled."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo.get_position_cycle_by_public_id = AsyncMock(side_effect=RuntimeError("DB"))
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="armed", native_instrument="BTC-USD")
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())
        await service._dispatch_commands(
            "plan-1", [{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        assert service.plans.get("plan-1") is None


class TestSweepCycleClosures:
    """Tests for _sweep_cycle_closures clock sweep."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_cancels_armed_bracket_with_closed_cycle(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Armed bracket on closed cycle is cancelled by sweep."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "closed", "public_id": "cycle-1"}
        )
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(plan_type="bracket", status="armed", native_instrument="BTC-USD")
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())
        await service._sweep_cycle_closures()
        assert service.plans.get("plan-1") is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_skips_active_brackets(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Active brackets are NOT cancelled by sweep (need cancel_requested flow)."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(plan_type="bracket", status="active", native_instrument="BTC-USD")
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())
        await service._sweep_cycle_closures()
        assert "plan-1" in service.plans
        mock_repo.get_position_cycle_by_public_id.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_skips_plans_without_cycle(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Plans without position_cycle_public_id are skipped by sweep."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(plan_type="manual_once", status="armed")
        service._register_plan(plan, ManualOnceEvaluator())
        await service._sweep_cycle_closures()
        assert "plan-1" in service.plans

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_keeps_armed_bracket_with_open_cycle(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Armed bracket with an open cycle remains registered."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "open", "public_id": "cycle-1"}
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(plan_type="bracket", status="armed", native_instrument="BTC-USD")
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())

        await service._sweep_cycle_closures()

        assert "plan-1" in service.plans
        mock_repo.update_execution_plan_status.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_cancels_armed_trailing_stop_with_closed_cycle(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Armed trailing stop on closed cycle is cancelled by sweep."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "closed", "public_id": "cycle-1"}
        )
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo.insert_execution_plan_decision = AsyncMock(return_value="d1")
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            plan_type="trailing_stop", status="armed", native_instrument="BTC-USD"
        )
        plan["position_cycle_public_id"] = "cycle-1"
        service._register_plan(plan, ManualOnceEvaluator())
        await service._sweep_cycle_closures()
        assert service.plans.get("plan-1") is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_called_in_clock_loop(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Clock loop calls _sweep_cycle_closures each tick."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service._running = True
        sweep_called = False

        async def _track_sweep() -> None:
            nonlocal sweep_called
            sweep_called = True

        service._sweep_cycle_closures = _track_sweep

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(service._clock_loop(), stop_after_one_tick())
        assert sweep_called


class TestIsCycleOpen:
    """Tests for _is_cycle_open helper."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_open_cycle_returns_true(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Open cycle returns True."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "open", "public_id": "c1"}
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        assert await service._is_cycle_open("c1") is True

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_closed_cycle_returns_false(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Closed cycle returns False."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(
            return_value={"status": "closed", "public_id": "c1"}
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        assert await service._is_cycle_open("c1") is False

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_missing_cycle_returns_false(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Missing cycle returns False."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(return_value=None)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        assert await service._is_cycle_open("nonexistent") is False

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_db_error_returns_false(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """DB error returns False (fail-closed)."""
        mock_repo = AsyncMock()
        mock_repo.get_position_cycle_by_public_id = AsyncMock(side_effect=RuntimeError("DB"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        assert await service._is_cycle_open("c1") is False


class TestTransitionPlanEdgeCases:
    """Edge case tests for _transition_plan and related paths."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_transition_with_last_error_stores_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Transition with last_error updates the in-memory plan."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(status="armed")
        service._register_plan(plan, ManualOnceEvaluator())
        await service._transition_plan("plan-1", "failed", "test error msg")
        assert service.plans.get("plan-1") is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_dispatch_armed_plan_vanishes_after_transition(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """If plan disappears from memory after armed→active transition, dispatch exits."""
        mock_repo = AsyncMock()
        mock_repo.get_instrument_capabilities = AsyncMock(return_value=[])
        mock_repo.get_position_cycle_by_public_id = AsyncMock(return_value=None)
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            status="armed", native_instrument="BTC-USD", child_client_order_id=None
        )
        service._register_plan(plan, ManualOnceEvaluator())

        original_transition = service._transition_plan

        async def _transition_and_remove(pid: str, status: str, error: str | None = None) -> None:
            await original_transition(pid, status, error)
            service.plans.pop(pid, None)

        service._transition_plan = _transition_and_remove
        await service._dispatch_commands(
            "plan-1", [{"instrument": "BTC-USD", "side": "sell", "quantity": 0.5}]
        )
        mock_repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_terminal_transition_without_in_memory_plan_is_safe(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Terminal transition is a no-op for in-memory state when the plan is already absent."""
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=1)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        await service._transition_plan("missing-plan", "completed")

        assert service.plans == {}
        mock_repo.update_execution_plan_status.assert_awaited_once()


class TestSweepEdgeCases:
    """Edge cases for _sweep_cycle_closures."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_sweep_skips_bracket_without_cycle_id(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Armed bracket without position_cycle_public_id is skipped."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(plan_type="bracket", status="armed")
        service._register_plan(plan, ManualOnceEvaluator())
        await service._sweep_cycle_closures()
        assert "plan-1" in service.plans
        mock_repo.get_position_cycle_by_public_id.assert_not_called()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_clock_loop_sweep_error_does_not_crash(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Sweep error in clock loop is caught and logged."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._running = True

        async def _broken_sweep() -> None:
            raise RuntimeError("sweep failed")

        service._sweep_cycle_closures = _broken_sweep

        async def stop_after_one_tick() -> None:
            await asyncio.sleep(0.05)
            service._running = False

        await asyncio.gather(service._clock_loop(), stop_after_one_tick())


class TestStrandedCancelEdgeCases:
    """Edge cases for stranded cancel with native_instrument but no child IDs."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_stranded_cancel_no_child_ids_is_noop(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """cancel_requested plan with native_instrument but no child IDs → no cancel emitted."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(
            return_value=[
                _make_plan_row(
                    public_id="plan-empty",
                    status="cancel_requested",
                    child_client_order_id=None,
                    native_instrument="BTC-USD",
                )
            ]
        )
        mock_repo.get_latest_checkpoints_for_plans = AsyncMock(return_value={})
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        await service._recover_plans()
        mock_repo.insert_trade_command.assert_not_called()


class TestStrandedCancelMultiChild:
    """Tests for _reemit_stranded_cancel with multi-child IDs."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_iterates_all_child_ids(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Stranded cancel re-emits for each child in the list."""
        mock_repo = AsyncMock()
        mock_repo.get_active_execution_plans = AsyncMock(
            return_value=[
                _make_plan_row(
                    public_id="plan-multi",
                    status="cancel_requested",
                    native_instrument="BTC-USD",
                )
            ]
        )
        mock_repo.get_latest_checkpoints_for_plans = AsyncMock(return_value={})
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-multi")
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=False)
        mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan_data = mock_repo.get_active_execution_plans.return_value[0]
        plan_data["params"]["child_client_order_ids"] = ["cid-a", "cid-b"]
        del plan_data["params"]["child_client_order_id"]
        await service._recover_plans()
        assert mock_repo.insert_trade_command.await_count == 2
