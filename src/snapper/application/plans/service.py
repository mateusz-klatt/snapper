"""PlanExecutorService — registered process that evaluates execution plans.

Subscribes to tick and execution ZMQ topics and dispatches events to
the correct PlanEvaluator instance for each active plan. Handles plan
lifecycle (create → active → completed/cancelled/failed), periodic
checkpointing, and crash recovery from persisted checkpoints.

Phase 1 MVP: only ManualOnceEvaluator. Other evaluators added in
Phases 2-5.
"""

import asyncio
import contextlib
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import zmq
import zmq.asyncio
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
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message

_EVALUATOR_REGISTRY: dict[str, type[PlanEvaluator]] = {
    "manual_once": ManualOnceEvaluator,
}

_CHECKPOINT_INTERVAL_S = 10.0
_TERMINAL_STATUSES = frozenset({"completed", "cancelled", "failed", "expired"})


@register_process(
    "plan_executor",
    description="ExecutionPlan evaluator runtime",
    priority=45,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "plans", "orders"),
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class PlanExecutorService(RegisterableProcess):
    """Evaluates active execution plans and emits TradeCommands.

    Subscribes to ``market.*.ticks`` and ``orders.events.*`` on the
    broker, recovers active plans from DB on startup, dispatches events
    to per-plan evaluators, propagates fills into plan state, and
    periodically checkpoints evaluator state.

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
        self._client_order_id_index: dict[str, str] = {}
        self._running = False
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None

    async def start(self) -> None:
        """Start the plan executor: recover plans, set up subscriber, run loops."""
        logger.info("PlanExecutorService starting")
        await self._recover_plans()
        self._setup_subscriber()
        self._running = True
        logger.info(
            "PlanExecutorService started with {} active plans",
            len(self.plans),
        )
        await self._run_loop()

    async def stop(self) -> None:
        """Gracefully stop the plan executor."""
        self._running = False
        if self._subscriber is not None:
            with contextlib.suppress(Exception):
                self._subscriber.close()
            self._subscriber = None
        if self._zmq_context is not None:
            with contextlib.suppress(Exception):
                self._zmq_context.term()
            self._zmq_context = None
        logger.info("PlanExecutorService stopped")

    def _setup_subscriber(self) -> None:
        """Create ZMQ subscriber and subscribe to relevant topics.

        Subscribes to ``orders.events.`` (fills + status) and
        ``market.`` (ticks). Short-circuits when the broker XPUB
        endpoint from the injected ``self.settings`` is not a real
        string (e.g., MagicMock in unit tests).
        """
        broker_addr = getattr(self.settings, "zmq_broker_xpub", None)
        if not isinstance(broker_addr, str) or not broker_addr:
            logger.info(
                "PlanExecutorService: no broker XPUB endpoint configured, "
                "skipping ZMQ subscriber setup"
            )
            return
        self._zmq_context = zmq.asyncio.Context()
        raw_sub_socket = self._zmq_context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_ORDER_FLOW)
        logger.info("PlanExecutorService: connecting subscriber to broker {}", broker_addr)
        raw_sub_socket.connect(broker_addr)
        self._subscriber = ValidatedSubscriber(raw_sub_socket)
        self._subscriber.subscribe("orders.events.")
        self._subscriber.subscribe("market.")
        logger.info("PlanExecutorService: subscribed to orders.events. and market.")

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
            self._register_plan(row, evaluator)
        logger.info("Recovered {} plans from DB", len(self.plans))

    def _register_plan(self, row: ExecutionPlanRow, evaluator: PlanEvaluator) -> None:
        """Install ``row``/``evaluator`` in memory and index by child order id.

        Args:
            row: Plan row to register.
            evaluator: Evaluator instance for this plan.
        """
        public_id = row["public_id"]
        self.plans[public_id] = row
        self.evaluators[public_id] = evaluator
        params = row.get("params") or {}
        child_id = params.get("child_client_order_id")
        if isinstance(child_id, str):
            self._client_order_id_index[child_id] = public_id

    def _unregister_plan(self, public_id: str) -> None:
        """Drop a plan from the in-memory registry and its indexes."""
        row = self.plans.pop(public_id, None)
        self.evaluators.pop(public_id, None)
        self._watermarks.pop(public_id, None)
        self._last_tick_timestamps.pop(public_id, None)
        if row is not None:
            params = row.get("params") or {}
            child_id = params.get("child_client_order_id")
            if isinstance(child_id, str):
                self._client_order_id_index.pop(child_id, None)

    async def _run_loop(self) -> None:
        """Run listen and checkpoint tasks until stopped."""
        tasks = [
            asyncio.create_task(self._listen_loop()),
            asyncio.create_task(self._checkpoint_loop()),
        ]
        try:
            while self._running:
                await asyncio.sleep(0.1)
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task

    async def _listen_loop(self) -> None:
        """Main ZMQ dispatch loop routing events to handlers.

        Per-message failures (parse errors, handler exceptions, socket
        recv errors) are caught and logged so a single bad frame can
        never silently stop the service. Only ``asyncio.CancelledError``
        (from shutdown) unwinds the loop.
        """
        if self._subscriber is None:
            logger.info("PlanExecutorService: no subscriber, listen loop inactive")
            return
        logger.info("PlanExecutorService: starting listen loop")
        try:
            while self._running:
                try:
                    topic_bytes, msg_bytes = await self._subscriber.recv_multipart()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("PlanExecutorService: socket recv failed: {}", exc)
                    await asyncio.sleep(0.1)
                    continue
                topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
                payload = msg_bytes.decode() if isinstance(msg_bytes, bytes) else str(msg_bytes)
                try:
                    msg = parse_message(payload)
                except MessageParseError as exc:
                    logger.debug("PlanExecutorService: cannot parse message on {}: {}", topic, exc)
                    continue
                try:
                    if isinstance(msg, ExecutionData):
                        await self._handle_execution(msg)
                    elif isinstance(msg, OrderData):
                        await self._handle_order_status(msg)
                    elif isinstance(msg, TickData):
                        await self._handle_tick(topic, msg)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        "PlanExecutorService: handler failed for topic {}: {}",
                        topic,
                        exc,
                    )
        except asyncio.CancelledError:
            logger.info("PlanExecutorService: listen loop cancelled")
            raise

    async def _checkpoint_loop(self) -> None:
        """Persist evaluator state every ``_CHECKPOINT_INTERVAL_S`` seconds."""
        try:
            while self._running:
                await asyncio.sleep(_CHECKPOINT_INTERVAL_S)
                await self._write_checkpoints()
        except asyncio.CancelledError:
            logger.info("PlanExecutorService: checkpoint loop cancelled")
            raise

    async def _handle_execution(self, execution: ExecutionData) -> None:
        """Propagate a fill into the matching plan's state.

        Looks up the plan via ``client_order_id``. If the cumulative
        fill reaches ``total_quantity`` the plan transitions to
        ``completed``; otherwise the plan is updated to ``active`` with
        the new ``filled_quantity``.

        Args:
            execution: Incoming execution event.
        """
        plan_public_id = self._client_order_id_index.get(execution.client_order_id)
        if plan_public_id is None:
            return
        plan = self.plans.get(plan_public_id)
        if plan is None:
            return
        if plan["status"] in _TERMINAL_STATUSES:
            return
        existing_filled = float(plan.get("filled_quantity", 0.0))
        incoming_cumulative = float(execution.size)
        if incoming_cumulative <= existing_filled + 1e-12:
            return
        evaluator = self.evaluators.get(plan_public_id)
        if evaluator is not None:
            with contextlib.suppress(Exception):
                await evaluator.on_execution(plan, execution)
        new_filled = incoming_cumulative
        total = float(plan["total_quantity"])
        is_complete = new_filled + 1e-9 >= total
        new_status = "completed" if is_complete else "active"
        now = datetime.now(UTC)
        try:
            await self.repository.update_execution_plan_status(
                public_id=plan_public_id,
                new_status=new_status,
                bus_time=now,
                session_id=self.tracker.session_id,
                sequence_id=self.tracker.next_sequence("plan_fills"),
                filled_quantity=new_filled,
                completed_at=now if is_complete else None,
                last_evaluated_at=now,
            )
        except Exception as exc:
            logger.error(
                "PlanExecutorService: failed to update plan {} on fill: {}",
                plan_public_id,
                exc,
            )
            return
        plan_mut: dict[str, Any] = dict(plan)
        plan_mut["filled_quantity"] = new_filled
        plan_mut["status"] = new_status
        if is_complete:
            plan_mut["completed_at"] = now
        self.plans[plan_public_id] = cast(ExecutionPlanRow, plan_mut)
        logger.info(
            "PlanExecutorService: plan {} filled {}/{} → {}",
            plan_public_id,
            new_filled,
            total,
            new_status,
        )
        if is_complete:
            self._unregister_plan(plan_public_id)

    async def _handle_order_status(self, order: OrderData) -> None:
        """Handle a non-fill order event (cancelled/rejected/expired).

        Terminal order-level statuses transition the plan accordingly.
        Non-terminal ones (submitted/accepted/new) are ignored.

        Args:
            order: Incoming order status event.
        """
        plan_public_id = self._client_order_id_index.get(order.client_order_id)
        if plan_public_id is None:
            return
        plan = self.plans.get(plan_public_id)
        if plan is None:
            return
        if plan["status"] in _TERMINAL_STATUSES:
            return
        status = order.status
        new_status: str | None = None
        last_error: str | None = None
        if status == "cancelled":
            new_status = "cancelled"
        elif status in ("rejected", "error"):
            new_status = "failed"
            last_error = order.error or f"venue {status}"
        elif status == "expired":
            new_status = "expired"
        if new_status is None:
            return
        now = datetime.now(UTC)
        try:
            await self.repository.update_execution_plan_status(
                public_id=plan_public_id,
                new_status=new_status,
                bus_time=now,
                session_id=self.tracker.session_id,
                sequence_id=self.tracker.next_sequence("plan_status"),
                last_error=last_error,
                completed_at=now if new_status == "completed" else None,
                last_evaluated_at=now,
            )
        except Exception as exc:
            logger.error(
                "PlanExecutorService: failed to transition plan {} to {}: {}",
                plan_public_id,
                new_status,
                exc,
            )
            return
        logger.info(
            "PlanExecutorService: plan {} venue event {} → plan status {}",
            plan_public_id,
            status,
            new_status,
        )
        self._unregister_plan(plan_public_id)

    async def _handle_tick(self, topic: str, tick: TickData) -> None:
        """Dispatch a tick to evaluators interested in the instrument.

        Phase 1 evaluators (ManualOnce) do not react to ticks so this is
        effectively a no-op, but the plumbing is in place for Phase 2+.

        Args:
            topic: ZMQ topic the tick arrived on.
            tick: Incoming tick data.
        """
        if not self.plans:
            return
        now = datetime.now(UTC)
        for public_id, plan in self.plans.items():
            if plan["instrument_public_id"] != tick.instrument:
                continue
            if plan["exchange"] != tick.exchange:
                continue
            evaluator = self.evaluators.get(public_id)
            if evaluator is None:
                continue
            with contextlib.suppress(Exception):
                await evaluator.on_tick(plan, tick)
            self._last_tick_timestamps[public_id] = now

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
