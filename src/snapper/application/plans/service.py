"""PlanExecutorService — registered process that evaluates execution plans.

Subscribes to tick and execution ZMQ topics and dispatches events to
the correct PlanEvaluator instance for each active plan. Handles plan
lifecycle (create → active → completed/cancelled/failed), periodic
checkpointing, and crash recovery from persisted checkpoints.

Phase 1 MVP: only ManualOnceEvaluator. Other evaluators added in
Phases 2-5.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger

from snapper.application.plans.evaluator import PlanEvaluator
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.data.repository_types import ExecutionPlanRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_EVALUATOR_REGISTRY: dict[str, type[PlanEvaluator]] = {
    "manual_once": ManualOnceEvaluator,
}

_CHECKPOINT_INTERVAL_S = 10.0


@register_process(
    "plan_executor",
    description="ExecutionPlan evaluator runtime",
    priority=45,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "plans", "orders"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class PlanExecutorService(RegisterableProcess):
    """Evaluates active execution plans and emits TradeCommands.

    Phase 1 skeleton: recovery from DB, in-memory plan registry,
    ManualOnceEvaluator. ZMQ subscription and full event loop
    deferred to Phase 1 Day 3+ when POST /api/orders is wired.

    Attributes:
        settings: Application settings.
        repository: Database repository for plan persistence.
        tracker: Sequence tracker for provenance stamping.
        plans: In-memory registry of active plans keyed by public_id.
        evaluators: Evaluator instances keyed by plan public_id.
    """

    def __init__(self) -> None:
        """Initialize the plan executor service."""
        self.settings: AppSettings = get_settings()
        self.repository = get_repository(self.settings.db_url)
        self.tracker = SequenceTracker()
        self.plans: dict[str, ExecutionPlanRow] = {}
        self.evaluators: dict[str, PlanEvaluator] = {}
        self._watermarks: dict[str, int] = {}
        self._last_tick_timestamps: dict[str, datetime | None] = {}
        self._running = False

    async def start(self) -> None:
        """Start the plan executor: recover plans, begin evaluation loop."""
        logger.info("PlanExecutorService starting")
        await self._recover_plans()
        self._running = True
        logger.info(
            "PlanExecutorService started with {} active plans",
            len(self.plans),
        )
        await self._run_loop()

    async def stop(self) -> None:
        """Gracefully stop the plan executor."""
        self._running = False
        logger.info("PlanExecutorService stopped")

    async def _recover_plans(self) -> None:
        """Load all actionable plans from DB and instantiate evaluators."""
        rows = await self.repository.get_active_execution_plans()
        for row in rows:
            plan_type = row["plan_type"]
            evaluator_cls = _EVALUATOR_REGISTRY.get(plan_type)
            if evaluator_cls is None:
                logger.warning(
                    "No evaluator for plan_type={}, skipping plan {}",
                    plan_type,
                    row["public_id"],
                )
                continue
            evaluator = evaluator_cls()
            checkpoint = await self.repository.get_latest_plan_checkpoint(row["public_id"])
            if checkpoint is not None:
                evaluator.restore_from_checkpoint(row, checkpoint["state"])
                self._watermarks[row["public_id"]] = checkpoint["last_venue_event_id"]
                self._last_tick_timestamps[row["public_id"]] = checkpoint["last_tick_timestamp"]
            self.plans[row["public_id"]] = row
            self.evaluators[row["public_id"]] = evaluator
        logger.info("Recovered {} plans from DB", len(self.plans))

    async def _run_loop(self) -> None:
        """Main event loop placeholder.

        Phase 1 Day 3 will wire ZMQ subscription here. For now, the
        skeleton just keeps the process alive and runs periodic
        checkpoint writes.
        """
        while self._running:
            await asyncio.sleep(_CHECKPOINT_INTERVAL_S)
            await self._write_checkpoints()

    async def _write_checkpoints(self) -> None:
        """Persist evaluator state for all active plans."""
        now = datetime.now(UTC)
        for public_id, evaluator in self.evaluators.items():
            plan = self.plans.get(public_id)
            if plan is None:
                continue
            state = evaluator.build_checkpoint_state(plan)
            try:
                await self.repository.insert_execution_plan_checkpoint(
                    plan_public_id=public_id,
                    state=state,
                    last_venue_event_id=self._watermarks.get(public_id, 0),
                    checkpoint_at=now,
                    session_id=self.tracker.session_id,
                    sequence_id=self.tracker.next_sequence("plan_checkpoints"),
                    bus_time=now,
                    last_tick_timestamp=self._last_tick_timestamps.get(public_id),
                )
            except Exception as exc:
                logger.error("Failed to checkpoint plan {}: {}", public_id, exc)

    def get_evaluator(self, plan_type: str) -> PlanEvaluator | None:
        """Instantiate an evaluator for the given plan type.

        Args:
            plan_type: Plan type string.

        Returns:
            New evaluator instance, or None if type not registered.
        """
        cls = _EVALUATOR_REGISTRY.get(plan_type)
        if cls is None:
            return None
        return cls()

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters for the plan executor.

        Args:
            settings: Application settings instance.

        Returns:
            Empty dict (no configurable parameters yet).
        """
        return {}

    def get_status(self) -> dict[str, Any]:
        """Return current plan executor status.

        Returns:
            Dict with active_plans count, plan_types list, and running flag.
        """
        return {
            "active_plans": len(self.plans),
            "plan_types": list({p["plan_type"] for p in self.plans.values()}),
            "running": self._running,
        }
