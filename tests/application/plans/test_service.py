"""Tests for PlanExecutorService."""

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast as _cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.engine.trader import TraderCoordinator
from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.plans.service import _EVALUATOR_REGISTRY
from snapper.application.plans.service import PlanExecutorService
from snapper.application.plans.settlement import PlanChildOutcome
from snapper.application.plans.settlement import PlanSettlementCandidate
from snapper.application.plans.settlement import PlanSettlementSkipped
from snapper.application.plans.settlement import settle_single_recovered_plan
from snapper.application.trade.cancel_target import CancelTargetUnresolvedError
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import TradeCommand
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import OrderInsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import VenueEventInsertRow
from snapper.data.repository_types import VenueEventRow
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
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-strand": "plan-stranded"}
        )
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
        """Per-child lookup failures while re-emitting cancels are logged, not raised.

        Even if the batched + per-child fallback both fail, recovery
        must not raise. ``linked_plan_id`` resolves to ``None`` for each
        child, so the plan-id mismatch guard skips emission cleanly.
        """
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(side_effect=Exception("DB"))
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
    async def test_reemit_stranded_cancel_falls_back_to_per_child_lookup_on_batch_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A transient batch lookup failure must NOT skip every child of the plan.

        When the batched ``get_plan_public_ids_for_client_order_ids``
        raises, ``_reemit_stranded_cancel`` falls back to per-child
        ``get_plan_public_id_for_client_order_id`` calls so a single
        transient DB error during recovery only impacts the one child
        whose individual lookup also fails — not the entire plan.
        Mirrors the earlier per-child fail-soft contract.
        """
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            side_effect=Exception("transient DB hiccup")
        )
        mock_repo.get_plan_public_id_for_client_order_id = AsyncMock(return_value="plan-fb")
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=False)
        mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-fb"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row(
            public_id="plan-fb",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        )
        params = _cast(dict[str, object], plan["params"])
        params["child_client_order_ids"] = ["cid-fb-a", "cid-fb-b"]
        del params["child_client_order_id"]
        await service._reemit_stranded_cancel(plan)
        assert mock_repo.get_plan_public_id_for_client_order_id.await_count == 2
        assert mock_repo.insert_trade_command.await_count == 2

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_reemit_stranded_cancel_swallows_venue_lookup_failure(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Venue-id lookup failure falls back to None exchange_order_id."""
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-1": "plan-x"}
        )
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
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-1": "plan-y"}
        )
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
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(return_value={})
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

        Verify the lookup result matches the recovered
        plan's public_id before emitting a cancel so a mis-stamped child
        id does not cause a cancel to be emitted for the wrong plan.
        """
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-1": "plan-OTHER"}
        )
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

        Every restart while a plan is still in
        cancel_requested would otherwise enqueue a duplicate venue
        cancel. ``has_pending_cancel_command`` is the idempotency
        guard.
        """
        mock_repo = AsyncMock()
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-1": "plan-dedup"}
        )
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
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-1": "plan-err"}
        )
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

        If a stranded cancel is re-emitted during
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
        """start() awaits a slow-joiner sleep after subscriber setup.

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
    async def test_handle_order_status_ignores_unknown(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Given an UNKNOWN submit outcome, Then the plan is NOT failed.

        The ambiguous UNKNOWN state is non-terminal: the order may
        be live on the venue, so failing the plan would misreport an
        unresolved submit as a terminal outcome.
        """
        mock_repo = AsyncMock()
        mock_repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan = _make_plan_row()
        service._register_plan(plan, ManualOnceEvaluator())
        order_event = _make_order_status(status="unknown")
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

        with (
            patch("snapper.application.plans.service.asyncio.sleep", side_effect=_cancel_sleep),
            pytest.raises(asyncio.CancelledError),
        ):
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
        assert inserted["source_surface"] == "strategy"
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
    """Tests for _runtime_symbol_index tick routing."""

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
    """Tests for _clock_loop."""

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
    """Tests for _check_capabilities."""

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
    """Tests for ExecutionPlanData response field additions."""

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
    """Self-bootstrap publisher covers both no-broker + reuse-context paths."""

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

        If ``raw_pub_socket.connect`` raises
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

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_setup_publisher_creates_zmq_context_when_absent(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """With a broker endpoint and no context yet, one is created."""
        mock_settings.return_value = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub="tcp://127.0.0.1:7500",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
        )
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        assert service._zmq_context is None
        created_ctx = MagicMock()
        created_ctx.socket = MagicMock(return_value=MagicMock())

        with patch("zmq.asyncio.Context", return_value=created_ctx):
            service._setup_publisher()

        assert service._zmq_context is created_ctx
        created_ctx.socket.assert_called_once()
        assert service._publisher is not None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_start_decision_outbox_drainer_creates_task(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """With a publisher and no live task, the drainer loop task starts."""
        mock_settings.return_value = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub=None,
            zmq_broker_xpub=None,
        )
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._publisher = MagicMock()
        assert service._decision_outbox_task is None

        async def _noop_loop() -> None:
            return None

        with patch.object(service, "_process_decision_outbox_loop", _noop_loop):
            service._start_decision_outbox_drainer()
            created = service._decision_outbox_task
            assert created is not None
            await created

        assert service._decision_outbox_task is created


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
        mock_repo.mark_execution_plan_decision_outbox_sent.assert_awaited_once()
        insert_kwargs = mock_repo.insert_execution_plan_decision.await_args.kwargs
        assert insert_kwargs["outbox_event"]["status"] == "pending"
        assert insert_kwargs["outbox_event"]["payload_json"].startswith("{")

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
        """Published payload uses ``plan_public_id``."""
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
        mock_repo.schedule_execution_plan_decision_outbox_retry.assert_awaited_once()
        mock_repo.mark_execution_plan_decision_outbox_sent.assert_not_awaited()

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

        mock_repo.mark_execution_plan_decision_outbox_sent.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_drain_decision_outbox_publishes_ready_row(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Ready outbox rows are republished and marked sent."""
        mock_repo = AsyncMock()
        mock_repo.list_execution_plan_decision_outbox_ready = AsyncMock(
            return_value=[
                {
                    "public_id": "outbox-1",
                    "timestamp": datetime.now(UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "decision_public_id": "decision-1",
                    "plan_public_id": "plan-1",
                    "topic": "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60311",
                    "payload_json": '{"type":"execution_plan_decision_event"}',
                    "status": "pending",
                    "attempt_count": 0,
                    "last_attempt_at": None,
                    "next_attempt_at": None,
                    "sent_at": None,
                    "error_reason": None,
                    "created_at": datetime.now(UTC),
                }
            ]
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        service._publisher = fake_publisher

        await service._drain_decision_outbox_once()

        fake_publisher.send_multipart.assert_awaited_once()
        send_kwargs = fake_publisher.send_multipart.await_args.kwargs
        assert send_kwargs["topic"] == "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60311"
        assert send_kwargs["payload"] == b'{"type":"execution_plan_decision_event"}'
        mock_repo.mark_execution_plan_decision_outbox_sent.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_start_decision_outbox_drainer_skips_existing_task(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Starting the drainer is idempotent when a task is already alive."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._publisher = AsyncMock()
        task = MagicMock()
        task.done.return_value = False
        service._decision_outbox_task = task

        service._start_decision_outbox_drainer()

        assert service._decision_outbox_task is task

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_stop_decision_outbox_drainer_cancels_task(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Stopping the drainer cancels and clears a live retry task."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        task = asyncio.create_task(asyncio.sleep(60.0))
        service._decision_outbox_task = task

        await service._stop_decision_outbox_drainer()

        assert service._decision_outbox_task is None
        assert task.cancelled()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_stop_decision_outbox_drainer_clears_done_task(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Stopping the drainer clears a task that already completed."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        task = asyncio.create_task(asyncio.sleep(0.0))
        await task
        service._decision_outbox_task = task

        await service._stop_decision_outbox_drainer()

        assert service._decision_outbox_task is None

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_process_decision_outbox_loop_invokes_drain(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """The retry loop sleeps and then drains pending rows."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        service._drain_decision_outbox_once = AsyncMock(side_effect=asyncio.CancelledError)

        with (
            patch("snapper.application.plans.service.asyncio.sleep", new=AsyncMock()),
            pytest.raises(asyncio.CancelledError),
        ):
            await service._process_decision_outbox_loop()

        service._drain_decision_outbox_once.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_drain_decision_outbox_tolerates_missing_repo_method(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Legacy test doubles without the outbox method are ignored."""
        mock_repo = AsyncMock()
        mock_repo.list_execution_plan_decision_outbox_ready = AsyncMock(
            side_effect=AttributeError("missing")
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service._publisher = AsyncMock()

        await service._drain_decision_outbox_once()

        service._publisher.send_multipart.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_drain_decision_outbox_logs_read_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Repository read failures do not stop the plan executor."""
        mock_repo = AsyncMock()
        mock_repo.list_execution_plan_decision_outbox_ready = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service._publisher = AsyncMock()

        await service._drain_decision_outbox_once()

        service._publisher.send_multipart.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_publish_decision_outbox_row_skips_without_publisher(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Outbox row publish short-circuits when no publisher is configured."""
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()
        await service._publish_decision_outbox_row(
            {
                "public_id": "outbox-1",
                "timestamp": datetime.now(UTC),
                "session_id": "s1",
                "sequence_id": 1,
                "decision_public_id": "decision-1",
                "plan_public_id": "plan-1",
                "topic": "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60311",
                "payload_json": "{}",
                "status": "pending",
                "attempt_count": 0,
                "last_attempt_at": None,
                "next_attempt_at": None,
                "sent_at": None,
                "error_reason": None,
                "created_at": datetime.now(UTC),
            }
        )

        mock_repo_fn.return_value.mark_execution_plan_decision_outbox_sent.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_publish_decision_outbox_row_schedules_retry_on_send_error(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """A retry-drain publish failure schedules the row again."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        fake_publisher = AsyncMock()
        fake_publisher.send_multipart = AsyncMock(side_effect=RuntimeError("broker down"))
        service._publisher = fake_publisher

        await service._publish_decision_outbox_row(
            {
                "public_id": "outbox-1",
                "timestamp": datetime.now(UTC),
                "session_id": "s1",
                "sequence_id": 1,
                "decision_public_id": "decision-1",
                "plan_public_id": "plan-1",
                "topic": "plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60311",
                "payload_json": "{}",
                "status": "pending",
                "attempt_count": 1,
                "last_attempt_at": None,
                "next_attempt_at": None,
                "sent_at": None,
                "error_reason": None,
                "created_at": datetime.now(UTC),
            }
        )

        mock_repo.schedule_execution_plan_decision_outbox_retry.assert_awaited_once()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_mark_decision_outbox_sent_tolerates_repo_edges(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Sent marking degrades on missing, failing, and race-lost repo methods."""
        mock_repo = AsyncMock()
        mock_repo.mark_execution_plan_decision_outbox_sent = AsyncMock(
            side_effect=[
                AttributeError("missing"),
                RuntimeError("db down"),
                False,
            ]
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        for _ in range(3):
            await service._mark_decision_outbox_sent(
                public_id="outbox-1",
                decision_public_id="decision-1",
                plan_public_id="plan-1",
            )

        assert mock_repo.mark_execution_plan_decision_outbox_sent.await_count == 3

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_schedule_decision_outbox_retry_tolerates_repo_edges(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Retry scheduling degrades on missing, failing, and race-lost repo methods."""
        mock_repo = AsyncMock()
        mock_repo.schedule_execution_plan_decision_outbox_retry = AsyncMock(
            side_effect=[
                AttributeError("missing"),
                RuntimeError("db down"),
                False,
            ]
        )
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        for _ in range(3):
            await service._schedule_decision_outbox_retry(
                public_id="outbox-1",
                decision_public_id="decision-1",
                plan_public_id="plan-1",
                current_attempt_count=0,
                error_reason="broker down",
            )

        assert mock_repo.schedule_execution_plan_decision_outbox_retry.await_count == 3

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_drain_decision_outbox_failure_marks_failed_after_budget(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Exhausted retry rows transition to failed on another publish error."""
        mock_repo = AsyncMock()
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()

        await service._schedule_decision_outbox_retry(
            public_id="outbox-1",
            decision_public_id="decision-1",
            plan_public_id="plan-1",
            current_attempt_count=2,
            error_reason="broker down",
        )

        mock_repo.mark_execution_plan_decision_outbox_failed.assert_awaited_once()
        mock_repo.schedule_execution_plan_decision_outbox_retry.assert_not_awaited()


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
    """Tests that _handle_tick must skip terminal and paused plans."""

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
    """Tests that evaluator exceptions must be logged, not silently suppressed."""

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
    """Tests that dispatch failure must not break sibling plans."""

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


@pytest.fixture
async def sqlite_plan_service(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[PlanExecutorService]:
    """Bind the plan executor to a real SQLite repository with no publisher."""
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repository.create_all()
    monkeypatch.setattr(
        "snapper.application.plans.service.get_repository", MagicMock(return_value=repository)
    )
    monkeypatch.setattr("snapper.application.plans.service.get_settings", MagicMock())
    try:
        yield PlanExecutorService()
    finally:
        await repository.engine.dispose()


async def _register_sqlite_bracket(
    service: PlanExecutorService, born_at: datetime, cycle_id: str | None = None
) -> ExecutionPlanRow:
    """Persist and register an armed bracket for a fire-time safety check."""
    repository = service.repository
    _, plan_id = await repository.insert_execution_plan(
        {
            "plan_type": "bracket",
            "created_via": "api",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": "wallet-1",
            "position_cycle_public_id": cycle_id,
            "total_quantity": 1.0,
            "side": "sell",
            "params": {"native_instrument": "BTC-USD"},
            "status": "armed",
            "created_at": born_at,
            "session_id": "fixture",
            "sequence_id": 1,
            "timestamp": born_at,
        }
    )
    plan = await repository.get_execution_plan(plan_id, as_of=None)
    assert plan is not None
    service._register_plan(plan, BracketEvaluator())
    return plan


async def test_fire_time_capability_gate_refuses_ahead_clock_revocation(
    sqlite_plan_service: PlanExecutorService,
) -> None:
    """Fail an armed bracket using the current capability version.

    Given: Reduce-only support was revoked by a writer ahead of the plan executor.
    When: The executor checks the bracket's required capabilities at dispatch.
    Then: It refuses dispatch and durably marks the bracket failed.
    """
    repository = sqlite_plan_service.repository
    assert isinstance(repository, SQLAlchemyRepository)
    poll_at = datetime.now(UTC)
    born_at = poll_at - timedelta(minutes=1)
    ahead = poll_at + timedelta(minutes=5)
    plan = await _register_sqlite_bracket(sqlite_plan_service, born_at)
    async with repository.session() as session:
        session.add_all(
            [
                InstrumentOrderCapability(
                    instrument_public_id=plan["instrument_public_id"],
                    exchange=plan["exchange"],
                    supported_order_types=["limit"],
                    supports_reduce_only=True,
                    timestamp=born_at,
                    known_to=ahead,
                    session_id="fixture",
                    sequence_id=2,
                ),
                InstrumentOrderCapability(
                    instrument_public_id=plan["instrument_public_id"],
                    exchange=plan["exchange"],
                    supported_order_types=["limit"],
                    supports_reduce_only=False,
                    timestamp=ahead,
                    session_id="fixture",
                    sequence_id=3,
                ),
            ]
        )
        await session.commit()
    historical = await repository.get_instrument_capabilities(poll_at)
    assert [row["supports_reduce_only"] for row in historical] == [True]
    assert await sqlite_plan_service._fail_dispatch_for_missing_capabilities(
        plan["public_id"], plan
    )
    failed = await repository.get_execution_plan(plan["public_id"], as_of=None)
    assert failed is not None and failed["status"] == "failed"
    assert plan["public_id"] not in sqlite_plan_service.plans


@pytest.mark.parametrize("sweep", [False, True])
async def test_closed_cycle_blocks_dispatch_and_sweep_under_clock_skew(
    sqlite_plan_service: PlanExecutorService, sweep: bool
) -> None:
    """Cancel a bracket whose cycle has already closed on an ahead writer.

    Given: An armed plan attached to a cycle whose current row says closed.
    When: Either the dispatch gate or the periodic sweep runs on a slower clock.
    Then: The bracket is durably cancelled even though the old cycle looked open.
    """
    repository = sqlite_plan_service.repository
    poll_at = datetime.now(UTC)
    born_at = poll_at - timedelta(minutes=1)
    ahead = poll_at + timedelta(minutes=5)
    _, cycle_id = await repository.insert_position_cycle(
        {
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": "wallet-1",
            "direction": "long",
            "max_qty": 1.0,
            "status": "open",
            "opened_at": born_at,
            "session_id": "fixture",
            "sequence_id": 2,
            "timestamp": born_at,
        }
    )
    plan = await _register_sqlite_bracket(sqlite_plan_service, born_at, cycle_id)
    assert await repository.close_position_cycle(cycle_id, ahead, None, ahead, "fixture", 3)
    assert await repository.get_position_cycle_statuses_by_public_ids([cycle_id], poll_at) == {
        cycle_id: "open"
    }

    if sweep:
        await sqlite_plan_service._sweep_cycle_closures()
    else:
        assert await sqlite_plan_service._cancel_dispatch_for_closed_cycle(plan["public_id"], plan)

    cancelled = await repository.get_execution_plan(plan["public_id"], as_of=None)
    assert cancelled is not None and cancelled["status"] == "cancelled"
    assert plan["public_id"] not in sqlite_plan_service.plans
    assert await repository.get_undispatched_commands(datetime.now(UTC)) == []


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
        mock_repo.get_position_cycle_statuses_by_public_ids = AsyncMock(
            return_value={"cycle-1": "closed"}
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
        mock_repo.get_position_cycle_statuses_by_public_ids.assert_not_called()

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
        mock_repo.get_position_cycle_statuses_by_public_ids = AsyncMock(
            return_value={"cycle-1": "open"}
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
        mock_repo.get_position_cycle_statuses_by_public_ids = AsyncMock(
            return_value={"cycle-1": "closed"}
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
        mock_repo.get_position_cycle_statuses_by_public_ids.assert_not_called()

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
        mock_repo.get_plan_public_ids_for_client_order_ids = AsyncMock(
            return_value={"cid-a": "plan-multi", "cid-b": "plan-multi"}
        )
        mock_repo.has_pending_cancel_command = AsyncMock(return_value=False)
        mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
        mock_repo.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
        mock_repo.get_order_lifecycle_events = AsyncMock(return_value=[])
        mock_repo.insert_trade_command = AsyncMock(return_value=(1, "cmd"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        plan_data = mock_repo.get_active_execution_plans.return_value[0]
        plan_data["params"]["child_client_order_ids"] = ["cid-a", "cid-b"]
        del plan_data["params"]["child_client_order_id"]
        await service._recover_plans()
        assert mock_repo.insert_trade_command.await_count == 2


_TARGET_ENDED_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def _stranded_repo(target_status: str | None, ended_at: datetime | None) -> AsyncMock:
    """Build a SQLAlchemy-shaped repository for one stranded cancel child.

    Args:
        target_status: Status of the child's create command, ``None`` when absent.
        ended_at: Newest venue evidence time of the child's end, or ``None``.

    Returns:
        The repository double.
    """
    repo = AsyncMock(spec=SQLAlchemyRepository)
    repo.get_plan_public_ids_for_client_order_ids = AsyncMock(return_value={"cid-1": "plan-strand"})
    repo.has_pending_cancel_command = AsyncMock(return_value=False)
    repo.get_pending_cancel_command_for_client_order_id = AsyncMock(return_value=None)
    repo.get_active_create_command_by_client_order_id = AsyncMock(
        return_value=None if target_status is None else {"status": target_status}
    )
    repo.get_latest_venue_event_received_at = AsyncMock(return_value=ended_at)
    repo.get_order_lifecycle_events = AsyncMock(return_value=[])
    repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-1")
    repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-1"))
    return repo


def _stranded_plan() -> ExecutionPlanRow:
    """Build one cancel_requested plan whose child is ``cid-1`` on kraken.

    Returns:
        The plan row.
    """
    return _cast(
        ExecutionPlanRow,
        _make_plan_row(
            public_id="plan-strand",
            status="cancel_requested",
            native_instrument="BTC-USD",
            venue_order_type="limit",
        ),
    )


def _strand_child_create(created_at: datetime) -> TradeCommandInsertRow:
    """Build the dispatched create command of the stranded plan's child ``cid-1``.

    Args:
        created_at: Creation and version time of the command.

    Returns:
        The insert row, linked to plan ``plan-strand``.
    """
    return {
        "command_type": "create",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "manual_once",
        "client_order_id": "cid-1",
        "venue_client_id": "cid-1",
        "side": "buy",
        "order_type": "limit",
        "quantity": 1.0,
        "price": 50000.0,
        "status": "dispatched",
        "created_at": created_at,
        "correlation_id": "plan-strand",
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": created_at,
        "wallet_public_id": "wallet-1",
        "plan_public_id": "plan-strand",
    }


def _strand_child_event(
    event_type: str, received_at: datetime, sequence_id: int
) -> VenueEventInsertRow:
    """Build one venue observation of the stranded plan's child ``cid-1``.

    Args:
        event_type: Venue event type.
        received_at: Observation time.
        sequence_id: Provenance sequence of the row.

    Returns:
        The insert row.
    """
    return {
        "event_type": event_type,
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "received_at": received_at,
        "session_id": "s1",
        "sequence_id": sequence_id,
        "timestamp": received_at,
        "client_order_id": "cid-1",
        "venue_client_id": "cid-1",
        "exchange_order_id": "ex-1",
        "wallet_public_id": "wallet-1",
        "liquidity_role": "unknown",
    }


class TestStrandedCancelEndedTarget:
    """Item G2b: plan recovery does not re-emit a cancel for an ended target."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_falsely_rejected_child_resurrected_and_accepted_is_reemitted(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Re-emit the cancel for a child whose rejection was false.

        Given: the child's dispatched create command in a SQLite repository,
            folded to rejected by a venue rejection and then shown alive by a
            later venue acceptance,
        When: the next reconciliation cycle resurrects it to accepted and plan
            recovery then re-emits stranded cancels,
        Then: the child counts as live and one cancel command for it is stored.
        """
        repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'g2b.db'}")
        await repository.create_all()
        mock_repo_fn.return_value = repository
        created_at = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=20)
        loop = ReconciliationLoop(
            exchange_name="kraken",
            repository=repository,
            trade_service=MagicMock(spec=TradeService),
        )
        try:
            await repository.insert_trade_command(_strand_child_create(created_at))
            await repository.insert_venue_event(
                _strand_child_event("order_rejected", created_at + timedelta(seconds=10), 1)
            )
            await loop._reconcile_cycle()
            rejected = await repository.get_active_create_command_by_client_order_id(
                "cid-1", "kraken"
            )
            await repository.insert_venue_event(
                _strand_child_event("order_accepted", created_at + timedelta(seconds=30), 2)
            )
            await loop._reconcile_cycle()
            resurrected = await repository.get_active_create_command_by_client_order_id(
                "cid-1", "kraken"
            )

            await PlanExecutorService()._reemit_stranded_cancel(_stranded_plan())

            pending = await repository.has_pending_cancel_command("cid-1", as_of=datetime.now(UTC))
        finally:
            await repository.engine.dispose()

        assert rejected is not None and rejected["status"] == "rejected"
        assert resurrected is not None and resurrected["status"] == "accepted"
        assert pending is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("target_status", ["cancelled", "filled", "expired"])
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_ended_target_with_venue_evidence_is_not_reemitted(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, target_status: str
    ) -> None:
        """Skip the cancel when the child's order ended with venue evidence.

        Given: a stranded cancel whose child create command is terminal with
            venue evidence,
        When: plan recovery re-emits stranded cancels,
        Then: no cancel command is inserted, and the lookup is scoped to the
            plan's exchange.
        """
        repo = _stranded_repo(target_status, _TARGET_ENDED_AT)
        mock_repo_fn.return_value = repo
        service = PlanExecutorService()

        await service._reemit_stranded_cancel(_stranded_plan())

        repo.insert_trade_command.assert_not_called()
        repo.get_active_create_command_by_client_order_id.assert_awaited_with("cid-1", "kraken")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("target_status", "ended_at"),
        [
            ("accepted", None),
            ("rejected", None),
            ("failed", None),
            ("cancelled", None),
            (None, None),
        ],
    )
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_target_without_venue_evidenced_end_is_reemitted(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        target_status: str | None,
        ended_at: datetime | None,
    ) -> None:
        """Re-emit the cancel when the child's order may still be live.

        Given: a stranded cancel whose child is accepted again after a false
            rejection, still rejected or failed, cancelled only locally, or
            without any create command,
        When: plan recovery re-emits stranded cancels,
        Then: the cancel command is inserted as before.
        """
        repo = _stranded_repo(target_status, ended_at)
        mock_repo_fn.return_value = repo
        service = PlanExecutorService()

        await service._reemit_stranded_cancel(_stranded_plan())

        repo.insert_trade_command.assert_awaited_once()
        assert repo.insert_trade_command.await_args[0][0]["command_type"] == "cancel"

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_raising_target_lookup_reemits(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Keep the venue-cancel path when create classification is unavailable.

        Given: a stranded cancel whose child create command lookup raises,
        When: plan recovery re-emits stranded cancels,
        Then: the cancel command is inserted without attempting local settlement.
        """
        repo = _stranded_repo("accepted", None)
        repo.get_active_create_command_by_client_order_id = AsyncMock(
            side_effect=RuntimeError("multiple active create/submit commands")
        )
        mock_repo_fn.return_value = repo
        service = PlanExecutorService()

        await service._reemit_stranded_cancel(_stranded_plan())

        repo.insert_trade_command.assert_awaited_once()


@dataclass(frozen=True)
class _StrandedChildSpec:
    """Describe one child and its durable evidence for recovery tests."""

    client_order_id: str
    status: str
    event_types: tuple[str, ...] = ()
    command_exchange_order_id: str | None = None
    order_exchange_order_id: str | None = None
    event_exchange_order_id: str | None = None
    failure_reason: str | None = None


async def _seed_stranded_child(
    repo: SQLAlchemyRepository, plan_pid: str, spec: _StrandedChildSpec
) -> None:
    """Insert one child command, its lifecycle events, and optional Order.

    Args:
        repo: Open SQLite repository.
        plan_pid: Parent execution plan public id.
        spec: Child lifecycle and venue-id fixture specification.
    """
    created_at = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
    create = _strand_child_create(created_at)
    create["client_order_id"] = spec.client_order_id
    create["venue_client_id"] = spec.client_order_id
    create["plan_public_id"] = plan_pid
    create["correlation_id"] = plan_pid
    create["exchange_order_id"] = spec.command_exchange_order_id
    _command_id, command_pid = await repo.insert_trade_command(create)
    if spec.status != "dispatched":
        await repo.advance_trade_command_lifecycle(
            public_id=command_pid,
            expected_status="dispatched",
            new_status=spec.status,
            bus_time=created_at + timedelta(seconds=1),
            session_id="s1",
            sequence_id=2,
            acked_at=(
                created_at + timedelta(seconds=1)
                if spec.status in ("accepted", "partially_filled")
                else None
            ),
            terminal_at=(
                created_at + timedelta(seconds=1)
                if spec.status in ("failed", "rejected", "cancelled", "expired", "filled")
                else None
            ),
            last_error=(
                (spec.failure_reason or "order_interlock_blocked")
                if spec.status == "failed"
                else None
            ),
        )
    for index, event_type in enumerate(spec.event_types):
        event = _strand_child_event(
            event_type, created_at + timedelta(seconds=index + 2), index + 3
        )
        event["client_order_id"] = spec.client_order_id
        event["venue_client_id"] = spec.client_order_id
        event["exchange_order_id"] = spec.event_exchange_order_id
        await repo.insert_venue_event(event)
    if spec.order_exchange_order_id is not None:
        order: OrderInsertRow = {
            "instrument_public_id": "inst-1",
            "client_order_id": spec.client_order_id,
            "exchange_order_id": spec.order_exchange_order_id,
            "created_at": created_at,
            "side": "buy",
            "order_type": "limit",
            "price": 50000.0,
            "size": 1.0,
            "status": "open",
            "session_id": "s1",
            "sequence_id": 20,
            "timestamp": created_at,
            "wallet_public_id": "wallet-1",
            "plan_public_id": plan_pid,
        }
        await repo.insert_order(**order)


async def _seed_stranded_recovery_plan(
    repo: SQLAlchemyRepository, children: list[_StrandedChildSpec]
) -> str:
    """Insert one cancel-requested plan and all specified children.

    Args:
        repo: Open SQLite repository.
        children: Child fixtures linked to the new plan.

    Returns:
        The inserted plan public id.
    """
    created_at = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
    _plan_id, plan_pid = await repo.insert_execution_plan(
        {
            "plan_type": "manual_once",
            "created_via": "api",
            "instrument_public_id": "inst-1",
            "exchange": "kraken",
            "mode": "live",
            "shard_key": "kraken.BTC-USD.live",
            "wallet_public_id": "wallet-1",
            "total_quantity": 1.0,
            "side": "buy",
            "params": {
                "order_type": "limit",
                "side": "buy",
                "child_client_order_ids": [child.client_order_id for child in children],
                "native_instrument": "BTC-USD",
                "venue_order_type": "limit",
            },
            "status": "cancel_requested",
            "created_at": created_at,
            "session_id": "s1",
            "sequence_id": 1,
            "timestamp": created_at,
        }
    )
    for child in children:
        await _seed_stranded_child(repo, plan_pid, child)
    return plan_pid


async def _assert_stranded_reemitted(
    repo: SQLAlchemyRepository, plan_pid: str, child_client_order_id: str
) -> None:
    """Assert that recovery emitted a cancel and left the plan actionable.

    Args:
        repo: Open SQLite repository.
        plan_pid: Recovered plan public id.
        child_client_order_id: Child expected to have a pending cancel.
    """
    pending = await repo.get_pending_cancel_command_for_client_order_id(
        child_client_order_id, datetime.now(UTC)
    )
    plan = await repo.get_execution_plan(plan_pid, as_of=datetime.now(UTC))
    assert pending is not None
    assert plan is not None and plan["status"] == "cancel_requested"


class TestStrandedCancelNeverPlacedTarget:
    """Startup recovery retains retryable cancels for every unsafe shape."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("target_status", ["dispatched", "pending", "sent"])
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_in_flight_create_with_empty_id_still_reemits(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, target_status: str
    ) -> None:
        """Keep the venue-cancel path for creates whose acknowledgement may be in flight.

        Given a dispatched, pending, or sent create with no exchange order id,
        When startup recovery inspects the stranded cancel,
        Then it re-emits the cancel and does not settle the plan locally.
        """
        repo = _stranded_repo(target_status, None)
        repo.get_active_create_command_by_client_order_id = AsyncMock(
            return_value={"status": target_status, "exchange_order_id": None}
        )
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = repo
        service = PlanExecutorService()

        await service._reemit_stranded_cancel(_stranded_plan())

        repo.insert_trade_command.assert_awaited_once()
        repo.update_execution_plan_status.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("id_source", ["command", "order"])
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_failed_create_with_venue_id_reemits(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path, id_source: str
    ) -> None:
        """Keep the venue-cancel path when either durable record has a venue id.

        Given a failed interlock-refused create with an id on its command or Order,
        When startup recovery inspects the stranded plan,
        Then it re-emits the cancel and leaves the plan non-terminal.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'failed_with_id.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                        command_exchange_order_id="ex-1" if id_source == "command" else None,
                        order_exchange_order_id="ex-1" if id_source == "order" else None,
                    )
                ],
            )

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
            inserted = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert inserted is not None and inserted["exchange_order_id"] == "ex-1"
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("child_status", "event_types"),
        [
            ("dispatched", ("order_submit_unknown",)),
            ("dispatched", ("order_accepted",)),
            ("accepted", ()),
            ("partially_filled", ()),
            ("created", ()),
            ("direct_dispatched", ()),
            ("filled", ()),
        ],
    )
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_unconfirmed_or_live_create_with_empty_id_reemits(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
        child_status: str,
        event_types: tuple[str, ...],
    ) -> None:
        """Re-emit for unknown, unfolded-accepted, accepted, and partial children.

        Given an empty-id child with the parametrized status and lifecycle evidence,
        When startup recovery runs against SQLite,
        Then it inserts a cancel and preserves the cancel-requested plan state.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'unsafe_empty_id.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status=child_status,
                        event_types=event_types,
                    )
                ],
            )

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "live_event_type",
        ["order_accepted", "fill_observed", "order_submit_unknown", "order_rejected"],
    )
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_failed_create_with_non_refusal_evidence_reemits(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
        live_event_type: str,
    ) -> None:
        """Re-emit failed creates with live, unknown, or rejected lifecycle evidence.

        Given a failed empty-id create with an interlock refusal and another event,
        When startup recovery runs against SQLite,
        Then it inserts a cancel and leaves the plan non-terminal.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'failed_with_events.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_interlock_blocked", live_event_type),
                    )
                ],
            )

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_placed_child_is_still_reemitted(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Preserve venue cancellation for an accepted child with an exchange id.

        Given a stranded cancel whose accepted child has an exchange order id,
        When startup recovery inspects the child,
        Then it re-emits one cancel and does not settle the plan locally.
        """
        repo = _stranded_repo("accepted", None)
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        mock_repo_fn.return_value = repo
        service = PlanExecutorService()

        await service._reemit_stranded_cancel(_stranded_plan())

        repo.insert_trade_command.assert_awaited_once()
        repo.update_execution_plan_status.assert_not_awaited()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_ack_between_startup_phases_reemits_with_event_only_id(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Carry an ack-only venue id on the cancel inserted later in recovery.

        Given a dispatched child when startup recovery begins,
        When an accepted event commits during child-plan link resolution,
        Then recovery inserts a cancel carrying the event's venue id.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'late_ack_event_id.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="dispatched",
                        event_types=(),
                    )
                ],
            )
            original_lookup = repo.get_plan_public_ids_for_client_order_ids

            async def _ack_then_lookup(
                client_order_ids: list[str], as_of: datetime
            ) -> dict[str, str]:
                ack = _strand_child_event("order_accepted", datetime.now(UTC), 50)
                ack["exchange_order_id"] = "ex-event-only"
                await repo.insert_venue_event(ack)
                return await original_lookup(client_order_ids, as_of=as_of)

            repo.get_plan_public_ids_for_client_order_ids = _ack_then_lookup

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
            inserted = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert inserted is not None and inserted["exchange_order_id"] == "ex-event-only"
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_mixed_live_and_unplaced_children_reemit_only_live_without_settlement(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Cancel only the live child after the safe child survives locked recheck.

        Given one accepted empty-id child and one interlock-only failed child,
        When startup recovery classifies and rechecks both children,
        Then only the live child gets a cancel and the plan remains cancel_requested.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'mixed_children.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(client_order_id="cid-live", status="accepted"),
                    _StrandedChildSpec(
                        client_order_id="cid-unplaced",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                    ),
                ],
            )

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-live")
            unplaced_pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-unplaced", datetime.now(UTC)
            )
            assert unplaced_pending is None
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_breaker_refused_create_settles(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Settle the exact circuit-breaker refusal shape without a cancel.

        Given a failed id-less create with circuit_breaker_open and its sole marker,
        When startup recovery classifies and settles the plan,
        Then the plan is failed and no pending cancel exists.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'breaker_refusal.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_breaker_open",),
                        failure_reason="circuit_breaker_open",
                    )
                ],
            )

            await PlanExecutorService()._recover_plans()

            plan = await repo.get_execution_plan(plan_pid, as_of=datetime.now(UTC))
            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert plan is not None and plan["status"] == "failed"
            assert plan["last_error"] == "circuit_breaker_open"
            assert pending is None
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_real_create_lookup_error_reemits(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Re-emit an id-less held cancel when real classifier lookups fail.

        Given a dispatched unknown-evidence child in SQLite,
        When its create lookup raises during classification and normal recovery,
        Then a retryable unresolved cancel row exists and the plan stays actionable.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'lookup_error.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="dispatched",
                        event_types=("order_submit_unknown",),
                    )
                ],
            )

            async def _lookup_error(_client_order_id: str, _exchange: str) -> None:
                raise RuntimeError("lookup failed")

            repo.get_active_create_command_by_client_order_id = _lookup_error

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_evidence_change_before_cas_reemits(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fall back to a fresh cancel when locked evidence changes before CAS.

        Given an initially safe interlock-only plan,
        When an ack is committed immediately before the real locked settlement,
        Then settlement is refused and a cancel carrying the ack id is inserted.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'cas_evidence_change.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                    )
                ],
            )

            async def _settle_with_late_ack(
                repository: SQLAlchemyRepository,
                candidate: PlanSettlementCandidate,
                *,
                settled_at: datetime,
                session_id: str,
                sequence_id: int,
            ) -> bool:
                ack = _strand_child_event("order_accepted", datetime.now(UTC), 50)
                ack["exchange_order_id"] = "ex-late-cas"
                await repository.insert_venue_event(ack)
                return await settle_single_recovered_plan(
                    repository,
                    candidate,
                    settled_at=settled_at,
                    session_id=session_id,
                    sequence_id=sequence_id,
                )

            monkeypatch.setattr(
                "snapper.application.plans.service.settle_single_recovered_plan",
                _settle_with_late_ack,
            )

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert pending is not None and pending["exchange_order_id"] == "ex-late-cas"
        finally:
            await repo.engine.dispose()

    async def _run_hydration_failure_round(self, repo: SQLAlchemyRepository) -> None:
        """Simulate a transient hydration failure leaving cancel in awaiting_target."""
        original_hydrate = repo.advance_trade_command_lifecycle

        async def _hydration_error(**_kwargs: object) -> None:
            raise RuntimeError("hydration failed")

        repo.advance_trade_command_lifecycle = _hydration_error
        try:
            await PlanExecutorService()._recover_plans()
        finally:
            repo.advance_trade_command_lifecycle = original_hydrate

    async def _count_cancel_public_ids(
        self, repo: SQLAlchemyRepository, client_order_id: str
    ) -> int:
        """Return distinct trade_commands public_ids of type cancel for cid."""
        async with repo.session() as session:
            count = (
                await session.execute(
                    select(func.count(func.distinct(TradeCommand.public_id))).where(
                        TradeCommand.client_order_id == client_order_id,
                        TradeCommand.command_type == "cancel",
                    )
                )
            ).scalar_one()
        return int(count)

    async def _assert_in_place_hydration_and_dispatch(
        self,
        repo: SQLAlchemyRepository,
        coordinator: TraderCoordinator,
        publisher: AsyncMock,
        expected_public_id: str,
    ) -> None:
        """Verify venue-id arrival hydrates existing row in place and dispatches once."""
        orig_insert = repo.insert_trade_command
        insert_spy = AsyncMock(side_effect=orig_insert)
        repo.insert_trade_command = insert_spy
        try:
            await PlanExecutorService()._recover_plans()
        finally:
            repo.insert_trade_command = orig_insert
        assert insert_spy.await_count == 0
        assert await self._count_cancel_public_ids(repo, "cid-1") == 1

        hydrated = await repo.get_pending_cancel_command_for_client_order_id(
            "cid-1", datetime.now(UTC)
        )
        assert hydrated is not None
        assert hydrated["public_id"] == expected_public_id
        assert hydrated["status"] == TradeCommandStatusEnum.CREATED.value
        assert hydrated["exchange_order_id"] == "ex-command-only"
        assert await repo.has_pending_cancel_command("cid-1", datetime.now(UTC)) is True
        undispatched = await repo.get_undispatched_commands(as_of=datetime.now(UTC), limit=10)
        assert [row["public_id"] for row in undispatched] == [expected_public_id]

        dispatcher = OutboxDispatcher(
            repository=repo, publish_fn=coordinator._outbox_publish, batch_size=10
        )
        await dispatcher._dispatch_batch()
        assert publisher.send.await_count == 1
        assert publisher.send.await_args is not None
        sent = publisher.send.await_args.args[1]
        assert sent.exchange_order_id == "ex-command-only"
        assert await self._count_cancel_public_ids(repo, "cid-1") == 1

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_idless_cancel_is_held_then_hydrated_without_duplicate(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Hold an unresolved cancel in awaiting_target and hydrate it in place.

        Given a non-safe child with no venue id,
        When recovery and trader dispatch run before and after a command-only id arrives,
        Then dedup blocks duplicate held inserts, the existing awaiting_target row is
            CAS-hydrated in place without inserting a second cancel, and it dispatches.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'held_cancel.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="dispatched",
                        event_types=("order_submit_unknown",),
                    )
                ],
            )
            publisher = AsyncMock()
            coordinator = TraderCoordinator.__new__(TraderCoordinator)
            coordinator.repository = repo
            coordinator.msg_publisher = publisher

            await PlanExecutorService()._recover_plans()

            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert pending is not None
            assert pending["status"] == TradeCommandStatusEnum.AWAITING_TARGET.value
            assert pending["exchange_order_id"] is None
            assert await self._count_cancel_public_ids(repo, "cid-1") == 1
            assert await repo.get_undispatched_commands(as_of=datetime.now(UTC), limit=10) == []
            assert await repo.has_pending_cancel_command("cid-1", datetime.now(UTC)) is True
            with pytest.raises(CancelTargetUnresolvedError):
                await coordinator._outbox_publish(pending)
            publisher.send.assert_not_awaited()

            await PlanExecutorService()._recover_plans()
            still_pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert still_pending is not None
            assert still_pending["public_id"] == pending["public_id"]
            assert still_pending["status"] == TradeCommandStatusEnum.AWAITING_TARGET.value
            assert await self._count_cancel_public_ids(repo, "cid-1") == 1

            create = await repo.get_active_create_command_by_client_order_id("cid-1", "kraken")
            assert create is not None
            await repo.advance_trade_command_lifecycle(
                public_id=create["public_id"],
                expected_status="dispatched",
                new_status="dispatched",
                bus_time=datetime.now(UTC),
                session_id="s1",
                sequence_id=60,
                exchange_order_id="ex-command-only",
            )
            await self._run_hydration_failure_round(repo)

            unresolved = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert unresolved is not None
            assert unresolved["status"] == TradeCommandStatusEnum.AWAITING_TARGET.value
            assert unresolved["exchange_order_id"] is None

            await self._assert_in_place_hydration_and_dispatch(
                repo, coordinator, publisher, pending["public_id"]
            )
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_settlement_exception_and_reload_error_reemit_original_plan(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Use the original plan when settlement and fallback reload both fail.

        Given: An exact safe plan whose settlement raises,
        When: The fallback plan reload also raises,
        Then: Recovery emits an unresolved retryable cancel using the original row.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'settle_reload_error.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                    )
                ],
            )

            async def _raise_settlement(
                repository: SQLAlchemyRepository,
                candidate: PlanSettlementCandidate,
                *,
                settled_at: datetime,
                session_id: str,
                sequence_id: int,
            ) -> bool:
                raise RuntimeError("settlement failed")

            monkeypatch.setattr(
                "snapper.application.plans.service.settle_single_recovered_plan",
                _raise_settlement,
            )
            original_get_plan = repo.get_execution_plan

            async def _reload_error(public_id: str, as_of: datetime) -> None:
                raise RuntimeError("reload failed")

            repo.get_execution_plan = _reload_error
            try:
                await PlanExecutorService()._recover_plans()
            finally:
                repo.get_execution_plan = original_get_plan

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_second_inspect_evidence_change_reemits(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Re-emit when evidence changes before the second full inspection.

        Given: A plan that is safe during per-child classification,
        When: An ack arrives before the second inspection,
        Then: Local settlement is refused and the cancel carries the new event id.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'second_inspect_change.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                    )
                ],
            )
            original_events = repo.get_order_lifecycle_events
            calls = 0

            async def _events_with_ack(client_order_ids: list[str]) -> list[VenueEventRow]:
                nonlocal calls
                calls += 1
                if calls == 2:
                    ack = _strand_child_event("order_accepted", datetime.now(UTC), 50)
                    ack["exchange_order_id"] = "ex-second-inspect"
                    await repo.insert_venue_event(ack)
                return await original_events(client_order_ids)

            repo.get_order_lifecycle_events = _events_with_ack

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-1")
            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert pending is not None and pending["exchange_order_id"] == "ex-second-inspect"
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("recheck_failure", ["false", "raise"])
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_mixed_locked_recheck_doubt_reemits_safe_child(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
        recheck_failure: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fail mixed-plan locked rechecks to emission on False or exceptions.

        Given: A mixed plan with one initially safe child,
        When: Its locked recheck returns False or raises,
        Then: Both children receive retryable cancels and the plan stays actionable.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'mixed_recheck_doubt.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            plan_pid = await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(client_order_id="cid-live", status="accepted"),
                    _StrandedChildSpec(
                        client_order_id="cid-safe",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                    ),
                ],
            )

            async def _recheck_doubt(
                repository: SQLAlchemyRepository,
                candidate: PlanSettlementCandidate,
            ) -> bool:
                if recheck_failure == "raise":
                    raise RuntimeError("recheck failed")
                return False

            monkeypatch.setattr(
                "snapper.application.plans.service.verify_plan_child_settlement_candidate",
                _recheck_doubt,
            )

            await PlanExecutorService()._recover_plans()

            await _assert_stranded_reemitted(repo, plan_pid, "cid-live")
            await _assert_stranded_reemitted(repo, plan_pid, "cid-safe")
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_hydration_cas_does_not_revert_dispatched_cancel(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Leave DISPATCHED unchanged when hydration loses its CREATED CAS.

        Given: An unresolved cancel and a newly available command-only id,
        When: Dispatch wins immediately before hydration,
        Then: The hydration CAS returns False and the row remains DISPATCHED.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'hydration_cas_race.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="dispatched",
                        event_types=("order_submit_unknown",),
                    )
                ],
            )
            await PlanExecutorService()._recover_plans()
            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert pending is not None
            create = await repo.get_active_create_command_by_client_order_id("cid-1", "kraken")
            assert create is not None
            await repo.advance_trade_command_lifecycle(
                public_id=create["public_id"],
                expected_status="dispatched",
                new_status="dispatched",
                bus_time=datetime.now(UTC),
                session_id="s1",
                sequence_id=60,
                exchange_order_id="ex-hydration-race",
            )
            original_advance = repo.advance_trade_command_lifecycle

            async def _dispatch_before_hydration(**kwargs: object) -> bool:
                bus_time = _cast(datetime, kwargs["bus_time"])
                await repo.update_trade_command_status(
                    public_id=_cast(str, kwargs["public_id"]),
                    new_status="dispatched",
                    bus_time=bus_time,
                    session_id=_cast(str, kwargs["session_id"]),
                    sequence_id=_cast(int, kwargs["sequence_id"]),
                    dispatched_at=bus_time,
                )
                return await original_advance(**kwargs)

            repo.advance_trade_command_lifecycle = _dispatch_before_hydration
            try:
                await PlanExecutorService()._recover_plans()
            finally:
                repo.advance_trade_command_lifecycle = original_advance

            raced = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert raced is not None and raced["status"] == "dispatched"
            assert raced["exchange_order_id"] is None
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_non_sqlalchemy_safe_helpers_fail_to_reemit(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock
    ) -> None:
        """Keep non-SQLAlchemy repositories on the normal emission path.

        Given: A service without SQLAlchemy settlement support,
        When: Locked recheck or settlement helpers are called directly,
        Then: They return no verified children and False respectively.
        """
        mock_repo_fn.return_value = AsyncMock()
        service = PlanExecutorService()

        assert await service._locked_safe_unplaced_children({}) == set()
        assert (
            await service._settle_safe_unplaced_plan(_stranded_plan(), datetime.now(UTC)) is False
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reload_state", ["terminal", "no_native", "no_children"])
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_refused_settlement_reload_guards_do_not_emit_stale_metadata(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
        reload_state: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Stop fallback only for terminal or unemittable reloaded plan versions.

        Given: A real safe plan whose settlement loses its CAS,
        When: Reload returns terminal, metadata-less, or child-less current state,
        Then: Recovery returns without using stale cancel metadata.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'reload_guards.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        try:
            await _seed_stranded_recovery_plan(
                repo,
                [
                    _StrandedChildSpec(
                        client_order_id="cid-1",
                        status="failed",
                        event_types=("order_interlock_blocked",),
                    )
                ],
            )

            async def _refuse_settlement(
                repository: SQLAlchemyRepository,
                candidate: PlanSettlementCandidate,
                *,
                settled_at: datetime,
                session_id: str,
                sequence_id: int,
            ) -> bool:
                return False

            monkeypatch.setattr(
                "snapper.application.plans.service.settle_single_recovered_plan",
                _refuse_settlement,
            )
            original_get_plan = repo.get_execution_plan

            async def _mutated_reload(public_id: str, as_of: datetime) -> ExecutionPlanRow | None:
                current = await original_get_plan(public_id, as_of=as_of)
                assert current is not None
                reloaded = _cast(ExecutionPlanRow, dict(current))
                if reload_state == "terminal":
                    reloaded["status"] = "failed"
                    return reloaded
                params = dict(reloaded.get("params") or {})
                if reload_state == "no_native":
                    params.pop("native_instrument", None)
                else:
                    params.pop("child_client_order_id", None)
                    params.pop("child_client_order_ids", None)
                reloaded["params"] = params
                return reloaded

            repo.get_execution_plan = _mutated_reload

            await PlanExecutorService()._recover_plans()

            repo.get_execution_plan = original_get_plan
            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
            assert pending is None
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_incident_settles_once_and_second_startup_is_noop(
        self, mock_repo_fn: MagicMock, mock_settings: MagicMock, tmp_path: Path
    ) -> None:
        """Settle the exact incident without ever inserting a venue cancel.

        Given a failed id-less create with only its interlock refusal marker,
        When two independent startup recoveries run,
        Then the first settles the plan and the second finds no actionable plan.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'never_placed.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        created_at = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=1)
        try:
            _plan_id, plan_pid = await repo.insert_execution_plan(
                {
                    "plan_type": "manual_once",
                    "created_via": "api",
                    "instrument_public_id": "inst-1",
                    "exchange": "kraken",
                    "mode": "live",
                    "shard_key": "kraken.BTC-USD.live",
                    "wallet_public_id": "wallet-1",
                    "total_quantity": 1.0,
                    "side": "buy",
                    "params": {
                        "order_type": "limit",
                        "side": "buy",
                        "child_client_order_id": "cid-1",
                        "native_instrument": "BTC-USD",
                        "venue_order_type": "limit",
                    },
                    "status": "cancel_requested",
                    "created_at": created_at,
                    "session_id": "s1",
                    "sequence_id": 1,
                    "timestamp": created_at,
                }
            )
            create = _strand_child_create(created_at)
            create["plan_public_id"] = plan_pid
            create["correlation_id"] = plan_pid
            _command_id, create_pid = await repo.insert_trade_command(create)
            await repo.advance_trade_command_lifecycle(
                public_id=create_pid,
                expected_status="dispatched",
                new_status="failed",
                bus_time=created_at + timedelta(seconds=1),
                session_id="s1",
                sequence_id=2,
                terminal_at=created_at + timedelta(seconds=1),
                last_error="order_interlock_blocked",
            )
            interlock = _strand_child_event(
                "order_interlock_blocked", created_at + timedelta(seconds=2), 3
            )
            interlock["exchange_order_id"] = None
            await repo.insert_venue_event(interlock)
            cancel = _strand_child_create(created_at)
            cancel["command_type"] = "cancel"
            cancel["status"] = "failed"
            cancel["plan_public_id"] = plan_pid
            cancel["correlation_id"] = plan_pid
            await repo.insert_trade_command(cancel)

            await PlanExecutorService()._recover_plans()
            recovered = await repo.get_execution_plan(plan_pid, as_of=datetime.now(UTC))
            await PlanExecutorService()._recover_plans()
            pending = await repo.get_pending_cancel_command_for_client_order_id(
                "cid-1", datetime.now(UTC)
            )
        finally:
            await repo.engine.dispose()

        assert recovered is not None and recovered["status"] == "failed"
        assert recovered["last_error"] == "order_interlock_blocked"
        assert pending is None


class TestRecoverySettlementAndCheckpointDedup:
    """Startup child-terminal settlement and unchanged-checkpoint deduplication."""

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_recover_plans_settles_terminal_children_and_seeds_checkpoint_signature(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Settle terminal-child plans on recovery and skip unchanged checkpoints.

        Given: A SQLite repository containing one `active` plan with a `filled`
            child, one `cancel_requested` plan with an `order_interlock_blocked`
            `failed` child, and one `armed` `trailing_stop` plan with a saved
            checkpoint.
        When: `PlanExecutorService._recover_plans()` runs followed by two
            `_write_checkpoints()` cycles (with tick timestamp churn) and one
            state-advancing tick.
        Then: The two terminal-child plans are settled without registration or
            stranded-cancel re-emission, the `trailing_stop` skips unchanged
            checkpoint writes despite tick timestamp updates, and writes exactly
            once when `peak_price` advances.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'recover_settle.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        base_time = datetime(2026, 11, 15, 10, 0, 0, tzinfo=UTC)
        wallet_id = "01960000-0000-7000-8000-000000000101"
        inst_id = "01960000-0000-7000-8000-000000000102"
        try:
            filled_plan_row: ExecutionPlanInsertRow = {
                "plan_type": "manual_once",
                "created_via": "api",
                "instrument_public_id": inst_id,
                "exchange": "kraken",
                "mode": "live",
                "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                "wallet_public_id": wallet_id,
                "total_quantity": 5.0,
                "side": "buy",
                "params": {
                    "order_type": "market",
                    "side": "buy",
                    "child_client_order_id": "cid-rec-fill",
                    "native_instrument": "XRP-EUR",
                },
                "status": "active",
                "created_at": base_time,
                "session_id": "s-rec",
                "sequence_id": 1,
                "timestamp": base_time,
            }
            _id1, filled_pid = await repo.insert_execution_plan(filled_plan_row)
            await repo.insert_trade_command(
                {
                    "command_type": "create",
                    "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                    "exchange": "kraken",
                    "instrument": "XRP-EUR",
                    "mode": "live",
                    "strategy_id": "manual_once",
                    "client_order_id": "cid-rec-fill",
                    "venue_client_id": "cid-rec-fill",
                    "side": "buy",
                    "order_type": "market",
                    "quantity": 5.0,
                    "status": "filled",
                    "created_at": base_time,
                    "correlation_id": filled_pid,
                    "session_id": "s-rec",
                    "sequence_id": 2,
                    "timestamp": base_time,
                    "wallet_public_id": wallet_id,
                    "plan_public_id": filled_pid,
                }
            )
            await repo.insert_venue_event(
                {
                    "event_type": "fill_observed",
                    "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                    "exchange": "kraken",
                    "instrument": "XRP-EUR",
                    "mode": "live",
                    "client_order_id": "cid-rec-fill",
                    "venue_client_id": "cid-rec-fill",
                    "exchange_order_id": "EX-REC-1",
                    "exec_id": "EXEC-REC-1",
                    "side": "buy",
                    "status": "filled",
                    "fill_size": 5.0,
                    "fill_price": 2.1,
                    "cum_fill_size": 5.0,
                    "liquidity_role": "taker",
                    "received_at": base_time + timedelta(seconds=1),
                    "session_id": "s-rec",
                    "sequence_id": 3,
                    "timestamp": base_time + timedelta(seconds=1),
                    "wallet_public_id": wallet_id,
                }
            )

            blocked_plan_row: ExecutionPlanInsertRow = {
                "plan_type": "manual_once",
                "created_via": "api",
                "instrument_public_id": inst_id,
                "exchange": "walutomat",
                "mode": "live",
                "shard_key": f"walutomat.EUR-PLN.live.{wallet_id}",
                "wallet_public_id": wallet_id,
                "total_quantity": 20.0,
                "side": "buy",
                "params": {
                    "order_type": "limit",
                    "side": "buy",
                    "price": 4.3,
                    "child_client_order_id": "cid-rec-block",
                    "native_instrument": "EUR-PLN",
                    "venue_order_type": "limit",
                },
                "status": "cancel_requested",
                "created_at": base_time + timedelta(seconds=5),
                "session_id": "s-rec",
                "sequence_id": 4,
                "timestamp": base_time + timedelta(seconds=5),
            }
            _id2, blocked_pid = await repo.insert_execution_plan(blocked_plan_row)
            _cid_block, block_cmd_pid = await repo.insert_trade_command(
                {
                    "command_type": "create",
                    "shard_key": f"walutomat.EUR-PLN.live.{wallet_id}",
                    "exchange": "walutomat",
                    "instrument": "EUR-PLN",
                    "mode": "live",
                    "strategy_id": "manual_once",
                    "client_order_id": "cid-rec-block",
                    "venue_client_id": "cid-rec-block",
                    "side": "buy",
                    "order_type": "limit",
                    "quantity": 20.0,
                    "price": 4.3,
                    "status": "dispatched",
                    "created_at": base_time + timedelta(seconds=5),
                    "correlation_id": blocked_pid,
                    "session_id": "s-rec",
                    "sequence_id": 5,
                    "timestamp": base_time + timedelta(seconds=5),
                    "wallet_public_id": wallet_id,
                    "plan_public_id": blocked_pid,
                }
            )
            await repo.advance_trade_command_lifecycle(
                public_id=block_cmd_pid,
                expected_status="dispatched",
                new_status="failed",
                bus_time=base_time + timedelta(seconds=6),
                session_id="s-rec",
                sequence_id=6,
                terminal_at=base_time + timedelta(seconds=6),
                last_error="order_interlock_blocked",
            )
            await repo.insert_venue_event(
                {
                    "event_type": "order_interlock_blocked",
                    "shard_key": f"walutomat.EUR-PLN.live.{wallet_id}",
                    "exchange": "walutomat",
                    "instrument": "EUR-PLN",
                    "mode": "live",
                    "client_order_id": "cid-rec-block",
                    "venue_client_id": "cid-rec-block",
                    "side": "buy",
                    "status": "failed",
                    "error": "order_interlock_blocked",
                    "liquidity_role": "unknown",
                    "received_at": base_time + timedelta(seconds=6),
                    "session_id": "s-rec",
                    "sequence_id": 6,
                    "timestamp": base_time + timedelta(seconds=6),
                    "wallet_public_id": wallet_id,
                }
            )

            trail_plan_row: ExecutionPlanInsertRow = {
                "plan_type": "trailing_stop",
                "created_via": "api",
                "instrument_public_id": inst_id,
                "exchange": "kraken",
                "mode": "live",
                "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                "wallet_public_id": wallet_id,
                "total_quantity": 5.0,
                "side": "buy",
                "params": {
                    "entry_price": 2.0,
                    "trailing_pct": 5.0,
                    "native_instrument": "XRP-EUR",
                },
                "status": "armed",
                "created_at": base_time + timedelta(seconds=10),
                "session_id": "s-rec",
                "sequence_id": 7,
                "timestamp": base_time + timedelta(seconds=10),
            }
            _id3, trail_pid = await repo.insert_execution_plan(trail_plan_row)
            await repo.insert_execution_plan_checkpoint(
                plan_public_id=trail_pid,
                state={"peak_price": 2.4, "current_stop": 2.28},
                last_venue_event_id=9,
                checkpoint_at=base_time + timedelta(seconds=12),
                session_id="s-rec",
                sequence_id=8,
                bus_time=base_time + timedelta(seconds=12),
            )

            mock_settings.return_value.plan_recovery_auto_settle_enabled = True
            service = PlanExecutorService()
            await service._recover_plans()

            assert set(service.plans) == {trail_pid}
            filled_settled = await repo.get_execution_plan(filled_pid, as_of=datetime.now(UTC))
            blocked_settled = await repo.get_execution_plan(blocked_pid, as_of=datetime.now(UTC))
            pending_cancel = await repo.has_pending_cancel_command(
                "cid-rec-block", as_of=datetime.now(UTC)
            )
            assert filled_settled is not None and filled_settled["status"] == "completed"
            assert filled_settled["filled_quantity"] == 5.0
            assert blocked_settled is not None and blocked_settled["status"] == "failed"
            assert pending_cancel is False

            service._last_tick_timestamps[trail_pid] = datetime.now(UTC)
            await service._write_checkpoints()
            await service._write_checkpoints()

            async with repo.session() as count_session:
                count_before = int(
                    (
                        await count_session.execute(
                            select(func.count()).select_from(ExecutionPlanCheckpoint)
                        )
                    ).scalar_one()
                )
            assert count_before == 1

            await service._handle_tick(
                "market.kraken.XRP-EUR.tick",
                TickData(
                    public_id="01960000-0000-7000-8000-000000000999",
                    session_id="s-rec",
                    sequence_id=99,
                    instrument="XRP-EUR",
                    exchange="kraken",
                    timestamp=datetime.now(UTC),
                    last=2.6,
                    volume=100.0,
                ),
            )
            await service._write_checkpoints()
            await service._write_checkpoints()

            async with repo.session() as count_session:
                count_after = int(
                    (
                        await count_session.execute(
                            select(func.count()).select_from(ExecutionPlanCheckpoint)
                        )
                    ).scalar_one()
                )
            assert count_after == 2
        finally:
            await repo.engine.dispose()

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_settle_recovered_plan_handles_errors_mismatch_and_drift(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Fail soft during startup settlement when inspection or writes fail.

        Given: A `PlanExecutorService` wired to a `SQLAlchemyRepository` mock.
        When: `_settle_recovered_plan` encounters an inspection error, a
            `plan_link_mismatch` skip, a write exception, and a concurrent
            version drift (`settled is False`).
        Then: Every failure path returns False cleanly without raising.
        """
        repo = MagicMock(spec=SQLAlchemyRepository)
        mock_repo_fn.return_value = repo
        service = PlanExecutorService()
        plan = _cast(ExecutionPlanRow, _make_plan_row("plan-rec-edge"))
        candidate = PlanSettlementCandidate(
            plan_public_id="plan-rec-edge",
            plan_type="manual_once",
            exchange="kraken",
            mode="live",
            shard_key="kraken.BTC-USD.live",
            current_status="active",
            current_session_id="s1",
            current_sequence_id=1,
            current_filled_quantity=0.0,
            target_status="completed",
            filled_quantity=1.0,
            last_error=None,
            children=(
                PlanChildOutcome(
                    client_order_id="cid-1",
                    command_public_id="cmd-1",
                    command_status="filled",
                    terminal_venue_event_id=10,
                    fill_venue_event_id=10,
                    cumulative_fill_size=1.0,
                    last_error=None,
                ),
            ),
        )

        monkeypatch.setattr(
            "snapper.application.plans.service.inspect_plan_settlement",
            AsyncMock(side_effect=RuntimeError("db down")),
        )
        assert await service._settle_recovered_plan(plan) is False

        monkeypatch.setattr(
            "snapper.application.plans.service.inspect_plan_settlement",
            AsyncMock(
                return_value=PlanSettlementSkipped(
                    plan_public_id="plan-rec-edge",
                    plan_type="manual_once",
                    exchange="kraken",
                    mode="live",
                    current_status="active",
                    skip_code="plan_link_mismatch",
                    detail="child cid-1 links to plan 'other'",
                )
            ),
        )
        assert await service._settle_recovered_plan(plan) is False

        monkeypatch.setattr(
            "snapper.application.plans.service.inspect_plan_settlement",
            AsyncMock(return_value=candidate),
        )
        monkeypatch.setattr(
            "snapper.application.plans.service.settle_single_recovered_plan",
            AsyncMock(side_effect=RuntimeError("write failed")),
        )
        assert await service._settle_recovered_plan(plan) is False

        monkeypatch.setattr(
            "snapper.application.plans.service.settle_single_recovered_plan",
            AsyncMock(return_value=False),
        )
        assert await service._settle_recovered_plan(plan) is False

        service.repository = MagicMock()
        assert await service._settle_recovered_plan(plan) is False

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_write_checkpoints_handles_watermark_change_and_concurrent_unregister(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
    ) -> None:
        """Rewrite checkpoints on watermark advance and drop unregistered plans.

        Given: One registered `manual_once` plan in `PlanExecutorService`.
        When: `_write_checkpoints` runs twice with unchanged state/watermark,
            once after watermark advances, and once when `insert_execution_plan_checkpoint`
            concurrently unregisters the plan.
        Then: Checkpoints are written only on change, and concurrent
            unregistration leaves `_last_checkpoint_signatures` empty.
        """
        mock_repo = AsyncMock()
        mock_repo.insert_execution_plan_checkpoint = AsyncMock(return_value=(1, "cp-1"))
        mock_repo_fn.return_value = mock_repo
        service = PlanExecutorService()
        service.plans["plan-1"] = _cast(ExecutionPlanRow, _make_plan_row("plan-1"))
        service.evaluators["plan-1"] = ManualOnceEvaluator()

        await service._write_checkpoints()
        await service._write_checkpoints()
        assert mock_repo.insert_execution_plan_checkpoint.await_count == 1

        service._watermarks["plan-1"] = 42
        await service._write_checkpoints()
        assert mock_repo.insert_execution_plan_checkpoint.await_count == 2

        service._watermarks["plan-1"] = 43

        async def _unregister_on_write(**_kwargs: object) -> tuple[int, str]:
            service._unregister_plan("plan-1")
            return (3, "cp-3")

        mock_repo.insert_execution_plan_checkpoint = AsyncMock(side_effect=_unregister_on_write)
        await service._write_checkpoints()
        assert "plan-1" not in service._last_checkpoint_signatures

    @pytest.mark.asyncio
    @patch("snapper.application.plans.service.get_settings")
    @patch("snapper.application.plans.service.get_repository")
    async def test_default_flag_skips_auto_settlement_and_dispatch_checks_durable_status(
        self,
        mock_repo_fn: MagicMock,
        mock_settings: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Keep startup auto-settlement off by default and fence external CLI settles.

        Given: A SQLite repository with an eligible `manual_once` plan (`filled`
            child) and an `armed` `trailing_stop` plan while
            `plan_recovery_auto_settle_enabled` is `False`.
        When: `_recover_plans()` runs under the default flag, and
            `_prepare_plan_for_dispatch` runs while `trailing_stop` is `armed`,
            after it is externally settled by a coordinator with an ahead clock,
            and for a missing DB plan id.
        Then: `_recover_plans()` leaves the eligible plan unsettled in DB, and
            `_prepare_plan_for_dispatch` allows the `armed` plan once, then
            unregisters and aborts dispatch when durable DB state is terminal or
            absent.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'flag_and_fence.db'}")
        await repo.create_all()
        mock_repo_fn.return_value = repo
        mock_settings.return_value.plan_recovery_auto_settle_enabled = False
        base_time = datetime(2026, 11, 15, 10, 30, 0, tzinfo=UTC)
        wallet_id = "01960000-0000-7000-8000-000000000201"
        inst_id = "01960000-0000-7000-8000-000000000202"
        try:
            _m_id, manual_pid = await repo.insert_execution_plan(
                {
                    "plan_type": "manual_once",
                    "created_via": "api",
                    "instrument_public_id": inst_id,
                    "exchange": "kraken",
                    "mode": "live",
                    "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                    "wallet_public_id": wallet_id,
                    "total_quantity": 5.0,
                    "side": "buy",
                    "params": {
                        "order_type": "market",
                        "side": "buy",
                        "child_client_order_id": "cid-flag-off",
                        "native_instrument": "XRP-EUR",
                    },
                    "status": "active",
                    "created_at": base_time,
                    "session_id": "s-flag",
                    "sequence_id": 1,
                    "timestamp": base_time,
                }
            )
            await repo.insert_trade_command(
                {
                    "command_type": "create",
                    "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                    "exchange": "kraken",
                    "instrument": "XRP-EUR",
                    "mode": "live",
                    "strategy_id": "manual_once",
                    "client_order_id": "cid-flag-off",
                    "venue_client_id": "cid-flag-off",
                    "side": "buy",
                    "order_type": "market",
                    "quantity": 5.0,
                    "status": "filled",
                    "created_at": base_time,
                    "correlation_id": manual_pid,
                    "session_id": "s-flag",
                    "sequence_id": 2,
                    "timestamp": base_time,
                    "wallet_public_id": wallet_id,
                    "plan_public_id": manual_pid,
                }
            )
            _t_id, trail_pid = await repo.insert_execution_plan(
                {
                    "plan_type": "trailing_stop",
                    "created_via": "api",
                    "instrument_public_id": inst_id,
                    "exchange": "kraken",
                    "mode": "live",
                    "shard_key": f"kraken.XRP-EUR.live.{wallet_id}",
                    "wallet_public_id": wallet_id,
                    "total_quantity": 5.0,
                    "side": "buy",
                    "params": {
                        "entry_price": 2.0,
                        "trailing_pct": 5.0,
                        "native_instrument": "XRP-EUR",
                    },
                    "status": "armed",
                    "created_at": base_time + timedelta(seconds=5),
                    "session_id": "s-flag",
                    "sequence_id": 3,
                    "timestamp": base_time + timedelta(seconds=5),
                }
            )

            service = PlanExecutorService()
            service._check_capabilities = AsyncMock(return_value=[])
            await service._recover_plans()

            assert set(service.plans) == {manual_pid, trail_pid}
            still_active = await repo.get_execution_plan(manual_pid, as_of=datetime.now(UTC))
            assert still_active is not None and still_active["status"] == "active"

            prepared_ok = await service._prepare_plan_for_dispatch(trail_pid)
            assert prepared_ok is not None and prepared_ok["status"] == "active"

            before_settlement = datetime.now(UTC)
            settled_at = before_settlement + timedelta(minutes=5)
            await repo.update_execution_plan_status(
                public_id=trail_pid,
                new_status="completed",
                bus_time=settled_at,
                session_id="cli-ext",
                sequence_id=9,
                completed_at=settled_at,
            )
            historical = await repo.get_execution_plan(trail_pid, as_of=before_settlement)
            assert historical is not None and historical["status"] == "active"
            assert await service._prepare_plan_for_dispatch(trail_pid) is None
            assert trail_pid not in service.plans

            ghost_pid = "01960000-0000-7000-8000-000000000299"
            service._register_plan(
                _cast(ExecutionPlanRow, _make_plan_row(ghost_pid)),
                ManualOnceEvaluator(),
            )
            assert await service._prepare_plan_for_dispatch(ghost_pid) is None
            assert ghost_pid not in service.plans
        finally:
            await repo.engine.dispose()
