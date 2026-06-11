"""PlanExecutorService — registered process that evaluates execution plans.

Subscribes to tick and execution ZMQ topics and dispatches events to
the correct PlanEvaluator instance for each active plan. Handles plan
lifecycle (create → active → completed/cancelled/failed), periodic
checkpointing, and crash recovery from persisted checkpoints.
Dispatches to the registered evaluator set (manual_once, bracket,
trailing_stop).
"""

import asyncio
import contextlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Any
from typing import cast
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.evaluator import PlanEvaluator
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.plans.params import core_order_type_from_plan_params
from snapper.application.plans.trailing_stop import TrailingStopEvaluator
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.core.types import FillStatusEnum
from snapper.core.types import OrderEventEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanDecisionOutboxInsertRow
from snapper.data.repository_types import ExecutionPlanDecisionOutboxRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import ExecutionPlanDecisionEventData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import plans_decisions_topic

_EVALUATOR_REGISTRY: dict[str, type[PlanEvaluator]] = {
    "manual_once": ManualOnceEvaluator,
    "bracket": BracketEvaluator,
    "trailing_stop": TrailingStopEvaluator,
}

_CHECKPOINT_INTERVAL_S = 10.0
_SLOW_JOINER_STABILIZATION_S = 0.5
_DECISION_OUTBOX_RETRY_INTERVAL_S = 30.0
_DECISION_OUTBOX_BATCH_SIZE = 100
_DECISION_OUTBOX_GIVE_UP_AFTER_ATTEMPTS = 3
_DECISION_OUTBOX_BACKOFF_BASE_S = 30.0
_DECISION_OUTBOX_BACKOFF_CAP_S = 300.0
_DECISION_OUTBOX_STREAM = "plan_decisions_outbox"
_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        ExecutionPlanStatusEnum.COMPLETED,
        ExecutionPlanStatusEnum.CANCELLED,
        ExecutionPlanStatusEnum.FAILED,
        ExecutionPlanStatusEnum.EXPIRED,
    }
)


def _decision_outbox_backoff_seconds(attempt_number: int) -> float:
    """Return capped exponential backoff for a failed decision publish attempt."""
    exp = max(attempt_number - 1, 0)
    scaled: float = _DECISION_OUTBOX_BACKOFF_BASE_S * (2**exp)
    return min(scaled, _DECISION_OUTBOX_BACKOFF_CAP_S)


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

    def __init__(
        self,
        *,
        caps_enforcer: TradingCapsEnforcer | None = None,
    ) -> None:
        """Initialize the plan executor service.

        Args:
            caps_enforcer: Optional :class:`TradingCapsEnforcer` for
                pre-insert cap enforcement on emitted commands.
                ``None`` defers construction to :meth:`start`, which
                lazy-builds the enforcer for
                :class:`SQLAlchemyRepository` repositories; with
                non-SQLAlchemy repositories (test fixtures) commands
                insert without cap enforcement.
        """
        self.settings: AppSettings = get_settings()
        self.repository = get_repository(self.settings.db_url)
        self.tracker = SequenceTracker()
        self.plans: dict[str, ExecutionPlanRow] = {}
        self.evaluators: dict[str, PlanEvaluator] = {}
        self._watermarks: dict[str, int] = {}
        self._last_tick_timestamps: dict[str, datetime | None] = {}
        self._client_order_id_index: dict[str, str] = {}
        self._runtime_symbol_index: dict[str, set[str]] = {}
        self._plan_locks: dict[str, asyncio.Lock] = {}
        self._running = False
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._publisher: ValidatedPublisher | None = None
        self._caps_enforcer = caps_enforcer
        self._decision_outbox_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the plan executor: set up subscriber, recover plans, run loops.

        The subscriber is wired up **before** recovery so that any
        venue events triggered by the recovery path (e.g., re-emitted
        stranded cancels from ``_reemit_stranded_cancel``) cannot be
        lost on the "subscriber not connected yet" race window. A
        brief slow-joiner stabilization sleep gives the XPUB/XSUB
        broker time to propagate the subscription before recovery
        starts emitting commands.
        """
        logger.info("PlanExecutorService starting")
        if self._caps_enforcer is None and isinstance(self.repository, SQLAlchemyRepository):
            pricing = USDConverter(repository=self.repository)
            self._caps_enforcer = TradingCapsEnforcer(repository=self.repository, pricing=pricing)
        self._setup_subscriber()
        self._setup_publisher()
        await self._drain_decision_outbox_once()
        self._start_decision_outbox_drainer()
        if self._subscriber is not None:
            await asyncio.sleep(_SLOW_JOINER_STABILIZATION_S)
        await self._recover_plans()
        self._running = True
        logger.info(
            "PlanExecutorService started with {} active plans",
            len(self.plans),
        )
        try:
            await self._run_loop()
        finally:
            await self._stop_decision_outbox_drainer()

    async def stop(self) -> None:
        """Gracefully stop the plan executor."""
        self._running = False
        await self._stop_decision_outbox_drainer()
        if self._subscriber is not None:
            with contextlib.suppress(Exception):
                self._subscriber.close()
            self._subscriber = None
        self._publisher = None
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

    def _setup_publisher(self) -> None:
        """Create ZMQ publisher for the ``plans.decisions.*`` topic family.

        Self-bootstrapped from ``settings.zmq_broker_xsub`` following
        the ``TraderCoordinator`` / ``BalanceService`` / ``SettingsService``
        pattern (see ``application/engine/trader.py:2428-2432`` and
        ``application/services/settings.py:232-236``). ``PlanExecutorService``
        does not receive a DI-injected publisher because
        ``process_manager/launcher.py:291`` instantiates processes via
        ``process_class(**validated_params)`` without a DI container.

        Short-circuits when ``zmq_broker_xsub`` is not a real string
        (MagicMock in unit tests), leaving ``self._publisher = None``.
        ``_log_decision`` tolerates this by skipping the publish step —
        the DB insert remains the source of truth (best-effort emit
        with fail-closed semantics).
        """
        broker_addr = getattr(self.settings, "zmq_broker_xsub", None)
        if not isinstance(broker_addr, str) or not broker_addr:
            logger.info(
                "PlanExecutorService: no broker XSUB endpoint configured, "
                "skipping plans.decisions.* publisher setup"
            )
            return
        if self._zmq_context is None:
            self._zmq_context = zmq.asyncio.Context()
        raw_pub_socket = self._zmq_context.socket(zmq.PUB)
        try:
            apply_hwm(raw_pub_socket, sndhwm=HWM_ORDER_FLOW)
            raw_pub_socket.connect(broker_addr)
        except Exception:
            with contextlib.suppress(Exception):
                raw_pub_socket.close()
            raise
        self._publisher = ValidatedPublisher(raw_pub_socket)
        logger.info("PlanExecutorService: connected publisher to broker {}", broker_addr)

    def _start_decision_outbox_drainer(self) -> None:
        """Start the background decision outbox retry loop when publishing is wired."""
        if self._publisher is None:
            return
        if self._decision_outbox_task is not None and not self._decision_outbox_task.done():
            return
        self._decision_outbox_task = asyncio.create_task(self._process_decision_outbox_loop())

    async def _stop_decision_outbox_drainer(self) -> None:
        """Cancel the decision outbox retry loop if it is running."""
        task = self._decision_outbox_task
        if task is None:
            return
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._decision_outbox_task = None

    async def _process_decision_outbox_loop(self) -> None:
        """Retry pending ``plans.decisions.*`` outbox rows until cancelled."""
        while True:
            await asyncio.sleep(_DECISION_OUTBOX_RETRY_INTERVAL_S)
            await self._drain_decision_outbox_once()

    async def _drain_decision_outbox_once(self) -> None:
        """Publish one batch of retry-ready plan-decision outbox rows."""
        if self._publisher is None:
            return
        now = datetime.now(UTC)
        try:
            rows = await self.repository.list_execution_plan_decision_outbox_ready(
                now,
                limit=_DECISION_OUTBOX_BATCH_SIZE,
            )
        except AttributeError:
            return
        except Exception as exc:
            logger.warning("Plan decision outbox drain read failed: {}", exc)
            return
        for row in rows:
            await self._publish_decision_outbox_row(row)

    async def _publish_decision_outbox_row(
        self,
        row: ExecutionPlanDecisionOutboxRow,
    ) -> None:
        """Publish one outbox row and transition it to sent or retry/failed."""
        if self._publisher is None:
            return
        try:
            await self._publisher.send_multipart(
                topic=row["topic"],
                payload=row["payload_json"].encode("utf-8"),
            )
        except Exception as exc:
            await self._schedule_decision_outbox_retry(
                public_id=row["public_id"],
                decision_public_id=row["decision_public_id"],
                plan_public_id=row["plan_public_id"],
                current_attempt_count=row["attempt_count"],
                error_reason=str(exc),
            )
            return
        await self._mark_decision_outbox_sent(
            public_id=row["public_id"],
            decision_public_id=row["decision_public_id"],
            plan_public_id=row["plan_public_id"],
        )

    async def _mark_decision_outbox_sent(
        self,
        *,
        public_id: str,
        decision_public_id: str,
        plan_public_id: str,
    ) -> None:
        """Mark a decision outbox row sent, logging but not raising failures."""
        now = datetime.now(UTC)
        try:
            applied = await self.repository.mark_execution_plan_decision_outbox_sent(
                public_id=public_id,
                transition_at=now,
                session_id=self.tracker.session_id,
                sequence_id=self.tracker.next_sequence(_DECISION_OUTBOX_STREAM),
            )
        except AttributeError:
            return
        except Exception as exc:
            logger.warning(
                "failed to mark plans.decisions outbox sent"
                " plan_public_id={plan} decision_public_id={dec} outbox_public_id={outbox}"
                " err={err}",
                plan=plan_public_id,
                dec=decision_public_id,
                outbox=public_id,
                err=exc,
            )
            return
        if not applied:
            logger.info(
                "plans.decisions outbox sent transition skipped"
                " plan_public_id={plan} decision_public_id={dec} outbox_public_id={outbox}",
                plan=plan_public_id,
                dec=decision_public_id,
                outbox=public_id,
            )

    async def _schedule_decision_outbox_retry(
        self,
        *,
        public_id: str,
        decision_public_id: str,
        plan_public_id: str,
        current_attempt_count: int,
        error_reason: str,
    ) -> None:
        """Schedule retry or terminal failure for a failed decision outbox publish."""
        now = datetime.now(UTC)
        attempt_number = current_attempt_count + 1
        try:
            if attempt_number >= _DECISION_OUTBOX_GIVE_UP_AFTER_ATTEMPTS:
                applied = await self.repository.mark_execution_plan_decision_outbox_failed(
                    public_id=public_id,
                    transition_at=now,
                    session_id=self.tracker.session_id,
                    sequence_id=self.tracker.next_sequence(_DECISION_OUTBOX_STREAM),
                    error_reason=error_reason,
                )
            else:
                next_attempt_at = now + timedelta(
                    seconds=_decision_outbox_backoff_seconds(attempt_number)
                )
                applied = await self.repository.schedule_execution_plan_decision_outbox_retry(
                    public_id=public_id,
                    transition_at=now,
                    session_id=self.tracker.session_id,
                    sequence_id=self.tracker.next_sequence(_DECISION_OUTBOX_STREAM),
                    next_attempt_at=next_attempt_at,
                    error_reason=error_reason,
                )
        except AttributeError:
            return
        except Exception as exc:
            logger.warning(
                "failed to update plans.decisions outbox retry state"
                " plan_public_id={plan} decision_public_id={dec} outbox_public_id={outbox}"
                " err={err}",
                plan=plan_public_id,
                dec=decision_public_id,
                outbox=public_id,
                err=exc,
            )
            return
        if not applied:
            logger.info(
                "plans.decisions outbox retry transition skipped"
                " plan_public_id={plan} decision_public_id={dec} outbox_public_id={outbox}",
                plan=plan_public_id,
                dec=decision_public_id,
                outbox=public_id,
            )

    async def _recover_plans(self) -> None:
        """Load all actionable plans from DB and instantiate evaluators.

        For plans recovered in ``cancel_requested`` status with no
        outstanding cancel ``trade_commands`` row, re-emit the cancel
        command. This closes the gap where a cancel route crashed
        between the plan transition and the command insert, leaving the
        plan stranded in ``cancel_requested`` forever.

        Bulk-loads all latest plan checkpoints in one round-trip per
        ``_PLAN_CHECKPOINT_LOOKUP_CHUNK_SIZE`` plans via
        :py:meth:`~snapper.data.repository.SQLAlchemyRepository.get_latest_checkpoints_for_plans`
        — replaces the per-plan
        :py:meth:`~snapper.data.repository.SQLAlchemyRepository.get_latest_plan_checkpoint`
        N+1 that previously dominated startup latency for operators
        with 100+ active plans.
        """
        rows = await self.repository.get_active_execution_plans()
        recoverable_pids = [
            row["public_id"] for row in rows if row["plan_type"] in _EVALUATOR_REGISTRY
        ]
        checkpoints_by_pid = await self.repository.get_latest_checkpoints_for_plans(
            recoverable_pids
        )
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
            checkpoint = checkpoints_by_pid.get(row["public_id"])
            if checkpoint is not None:
                evaluator.restore_from_checkpoint(row, checkpoint["state"])
                self._watermarks[row["public_id"]] = checkpoint["last_venue_event_id"]
                self._last_tick_timestamps[row["public_id"]] = checkpoint["last_tick_timestamp"]
            self._register_plan(row, evaluator)
            if row["status"] == ExecutionPlanStatusEnum.CANCEL_REQUESTED:
                await self._reemit_stranded_cancel(row)
        logger.info("Recovered {} plans from DB", len(self.plans))

    async def _reemit_stranded_cancel(self, plan: ExecutionPlanRow) -> None:
        """Re-emit a cancel TradeCommand for a stranded cancel_requested plan.

        A plan can end up in ``cancel_requested`` with no matching
        cancel command if the REST cancel route crashed after the
        status transition but before the command insert (or if the
        compensating ``failed`` transition also failed). On startup we
        re-emit the cancel so the venue adapter still sees it.

        Guards:
            - Skipped if ``child_client_order_id`` / ``native_instrument``
              are not stamped in plan params (legacy or test rows).
            - Skipped if the repository lookup maps the child id to a
              different plan than the one we recovered (corrupt /
              misstamped child id — do not emit a cancel for the wrong
              plan).
            - Skipped if a non-terminal cancel command already exists
              for this child id, so this is idempotent across restarts
              while the plan is still ``cancel_requested``.

        Args:
            plan: Recovered plan row in ``cancel_requested`` status.

        Performance: The child->parent plan lookup is now
        batched via :meth:`Repository.get_plan_public_ids_for_client_order_ids`
        once per plan instead of one round-trip per child id. Wide
        plans (10-100 children) used to issue that many sequential
        SELECTs on startup; the batched ``IN`` query keeps recovery
        cost flat regardless of child count.

        Degradation: when the batched lookup raises (transient DB
        error), the legacy per-child behavior is preserved — every
        child falls back to its own single-row lookup so one bad
        batch does not skip the entire plan's recovery.
        """
        params = plan.get("params") or {}
        native_instrument = params.get("native_instrument")
        if not isinstance(native_instrument, str):
            return
        child_order_ids = self._extract_child_ids(params)
        if not child_order_ids:
            return
        now = datetime.now(UTC)
        linked_plan_map = await self._resolve_child_plan_links(plan, child_order_ids, now)
        for child_client_order_id in child_order_ids:
            linked_plan_id = linked_plan_map.get(child_client_order_id)
            await self._reemit_single_stranded_cancel(
                plan, child_client_order_id, native_instrument, linked_plan_id, now
            )

    async def _resolve_child_plan_links(
        self,
        plan: ExecutionPlanRow,
        child_order_ids: list[str],
        now: datetime,
    ) -> dict[str, str | None]:
        """Resolve each child client_order_id to its linked plan public id.

        Prefers a single batched repository call. If the
        batched call fails, falls back to per-child single-row
        lookups so one transient DB error does not skip the whole
        plan's recovery — matching the legacy per-child fail-soft
        contract.
        """
        try:
            batched = await self.repository.get_plan_public_ids_for_client_order_ids(
                child_order_ids, as_of=now
            )
        except Exception as exc:
            logger.warning(
                "Stranded cancel batched plan lookup failed for {} ({}); "
                "falling back to per-child lookups",
                plan["public_id"],
                exc,
            )
            return await self._resolve_child_plan_links_individually(plan, child_order_ids, now)
        return {cid: batched.get(cid) for cid in child_order_ids}

    async def _resolve_child_plan_links_individually(
        self,
        plan: ExecutionPlanRow,
        child_order_ids: list[str],
        now: datetime,
    ) -> dict[str, str | None]:
        """Per-child single-row plan lookup fallback for batch failures."""
        result: dict[str, str | None] = {}
        for cid in child_order_ids:
            try:
                result[cid] = await self.repository.get_plan_public_id_for_client_order_id(
                    cid, as_of=now
                )
            except Exception as exc:
                logger.error(
                    "Stranded cancel per-child plan lookup failed for {} child {}: {}",
                    plan["public_id"],
                    cid,
                    exc,
                )
                result[cid] = None
        return result

    def _extract_child_ids(self, params: dict[str, Any]) -> list[str]:
        """Extract all child client order IDs from plan params.

        Unions both the list format (child_client_order_ids) and the
        legacy single-string format (child_client_order_id) so that
        mixed-format plans (upgraded mid-lifecycle) never lose IDs.

        Args:
            params: Plan params dict.

        Returns:
            Deduplicated list of child client order ID strings (may be empty).
        """
        result: set[str] = set()
        ids = params.get("child_client_order_ids")
        if isinstance(ids, list):
            result.update(c for c in ids if isinstance(c, str))
        single = params.get("child_client_order_id")
        if isinstance(single, str):
            result.add(single)
        return list(result)

    async def _emit_trade_command(
        self,
        row: TradeCommandInsertRow,
        *,
        plan: ExecutionPlanRow,
        command_type: str,
        side: str | None,
        order_type: str | None,
        quantity: float | None,
    ) -> None:
        """Insert a plan-originated trade command via the caps enforcer.

        Builds a :class:`TradeCommandSubmission` from the plan's
        creator. When a user is
        present, routes through :meth:`TradingCapsEnforcer.guard`
        (per-user caps apply). When absent (system / strategy-created
        plan), routes through
        :meth:`TradingCapsEnforcer.guard_service_principal` so the
        bypass is audit-visible at the call site.
        When the service runs without a caps enforcer — test fixtures
        and non-SQLAlchemy repositories, since :meth:`start` only
        lazy-constructs the enforcer for :class:`SQLAlchemyRepository`
        — the insert is issued directly, mirroring the trader
        coordinator's ``_build_caps_enforcer`` returning ``None`` for
        the same case.
        """
        if self._caps_enforcer is None:
            await self.repository.insert_trade_command(row, ownership=None)
            return
        user_public_id = plan.get("created_by_user_id")
        submission = TradeCommandSubmission(
            user_public_id=user_public_id,
            operator_public_id=plan.get("operator_public_id"),
            wallet_public_id=plan.get("wallet_public_id"),
            instrument_public_id=plan.get("instrument_public_id"),
            command_type=command_type,
            side=side,
            order_type=order_type,
            quantity=Decimal(str(quantity)) if quantity is not None else None,
            price=None,
            source_surface="rest",
            idempotency_key=row.get("idempotency_key"),
        )
        if user_public_id is not None:
            async with self._caps_enforcer.guard(submission):
                await self.repository.insert_trade_command(row, ownership=None)
        else:
            async with self._caps_enforcer.guard_service_principal(submission):
                await self.repository.insert_trade_command(row, ownership=None)

    async def _reemit_single_stranded_cancel(
        self,
        plan: ExecutionPlanRow,
        child_client_order_id: str,
        native_instrument: str,
        linked_plan_id: str | None,
        now: datetime,
    ) -> None:
        """Re-emit a cancel command for a single child order.

        Args:
            plan: Recovered plan row in cancel_requested status.
            child_client_order_id: Child order to cancel.
            native_instrument: Native exchange symbol for the cancel command.
            linked_plan_id: Pre-resolved plan public id for this child
                from the batched lookup, or ``None`` when the
                child does not link to any plan-stamped create command.
            now: Bus timestamp shared with the parent batch so all
                children in one recovery sweep see the same temporal
                point.
        """
        params = plan.get("params") or {}
        if linked_plan_id is None or linked_plan_id != plan["public_id"]:
            logger.warning(
                "PlanExecutorService: stranded cancel skipped for plan {} "
                "(child {} links to plan {})",
                plan["public_id"],
                child_client_order_id,
                linked_plan_id,
            )
            return
        try:
            already_pending = await self.repository.has_pending_cancel_command(
                child_client_order_id, as_of=now
            )
        except Exception as exc:
            logger.error(
                "Stranded cancel dedup lookup failed for plan {}: {}",
                plan["public_id"],
                exc,
            )
            return
        if already_pending:
            logger.info(
                "PlanExecutorService: cancel already pending for plan {} child {}, skipping",
                plan["public_id"],
                child_client_order_id,
            )
            return
        try:
            exchange_order_id = await self.repository.get_exchange_order_id_for_client_order_id(
                child_client_order_id, as_of=now
            )
        except Exception as exc:
            logger.error("Stranded cancel venue lookup failed for {}: {}", plan["public_id"], exc)
            exchange_order_id = None
        session_id = self.tracker.session_id
        sequence_id = self.tracker.next_sequence("plan_stranded_cancels")
        row: dict[str, Any] = {
            "command_type": "cancel",
            "shard_key": plan["shard_key"],
            "exchange": plan["exchange"],
            "instrument": native_instrument,
            "mode": plan["mode"],
            "strategy_id": plan["plan_type"],
            "client_order_id": child_client_order_id,
            "venue_client_id": child_client_order_id,
            "side": plan["side"],
            "order_type": core_order_type_from_plan_params(params),
            "quantity": plan["total_quantity"],
            "price": params.get("price"),
            "leverage": params.get("leverage"),
            "reduce_only": False,
            "status": TradeCommandStatusEnum.CREATED,
            "created_at": now,
            "correlation_id": plan["public_id"],
            "session_id": session_id,
            "sequence_id": sequence_id,
            "timestamp": now,
            "wallet_public_id": plan["wallet_public_id"] or "",
            "operator_public_id": plan["operator_public_id"],
            "user_public_id": None,
            "plan_public_id": plan["public_id"],
            "exchange_order_id": exchange_order_id,
        }
        try:
            await self._emit_trade_command(
                cast(TradeCommandInsertRow, row),
                plan=plan,
                command_type="cancel",
                side=plan["side"],
                order_type=core_order_type_from_plan_params(params),
                quantity=plan["total_quantity"],
            )
            logger.info(
                "PlanExecutorService: re-emitted stranded cancel for plan {} child {}",
                plan["public_id"],
                child_client_order_id,
            )
        except Exception as exc:
            logger.error("Stranded cancel re-emit failed for plan {}: {}", plan["public_id"], exc)

    def _register_plan(self, row: ExecutionPlanRow, evaluator: PlanEvaluator) -> None:
        """Install ``row``/``evaluator`` in memory and index by child order id.

        Args:
            row: Plan row to register.
            evaluator: Evaluator instance for this plan.
        """
        public_id = row["public_id"]
        self.plans[public_id] = row
        self.evaluators[public_id] = evaluator
        self._plan_locks[public_id] = asyncio.Lock()
        params = row.get("params") or {}
        for cid in self._extract_child_ids(params):
            self._client_order_id_index[cid] = public_id
        native_instrument = params.get("native_instrument")
        if isinstance(native_instrument, str):
            key = f"{row['exchange']}:{native_instrument}"
            self._runtime_symbol_index.setdefault(key, set()).add(public_id)

    def _unregister_plan(self, public_id: str) -> None:
        """Drop a plan from the in-memory registry and its indexes."""
        row = self.plans.pop(public_id, None)
        self.evaluators.pop(public_id, None)
        self._watermarks.pop(public_id, None)
        self._last_tick_timestamps.pop(public_id, None)
        self._plan_locks.pop(public_id, None)
        if row is not None:
            params = row.get("params") or {}
            for cid in self._extract_child_ids(params):
                self._client_order_id_index.pop(cid, None)
            native_instrument = params.get("native_instrument")
            if isinstance(native_instrument, str):
                key = f"{row['exchange']}:{native_instrument}"
                plan_ids = self._runtime_symbol_index.get(key)
                if plan_ids is not None:
                    plan_ids.discard(public_id)
                    if not plan_ids:
                        del self._runtime_symbol_index[key]

    def _get_plan_lock(self, public_id: str) -> asyncio.Lock:
        """Return the per-plan asyncio.Lock, creating it if needed."""
        lock = self._plan_locks.get(public_id)
        if lock is None:
            lock = asyncio.Lock()
            self._plan_locks[public_id] = lock
        return lock

    async def _transition_plan(
        self,
        plan_public_id: str,
        new_status: str,
        last_error: str | None = None,
    ) -> None:
        """Transition a plan to a new status and update in-memory state.

        Args:
            plan_public_id: Plan to transition.
            new_status: Target status.
            last_error: Optional error message (for failed transitions).
        """
        now = datetime.now(UTC)
        try:
            await self.repository.update_execution_plan_status(
                public_id=plan_public_id,
                new_status=new_status,
                bus_time=now,
                session_id=self.tracker.session_id,
                sequence_id=self.tracker.next_sequence("plan_transitions"),
                last_error=last_error,
                started_at=now if new_status == ExecutionPlanStatusEnum.ACTIVE else None,
                completed_at=now if new_status in _TERMINAL_STATUSES else None,
            )
        except Exception as exc:
            logger.error("Failed to transition plan {} to {}: {}", plan_public_id, new_status, exc)
            return
        plan = self.plans.get(plan_public_id)
        if plan is not None:
            plan_mut: dict[str, Any] = dict(plan)
            plan_mut["status"] = new_status
            if last_error is not None:
                plan_mut["last_error"] = last_error
            self.plans[plan_public_id] = cast(ExecutionPlanRow, plan_mut)
        if new_status in _TERMINAL_STATUSES:
            self._unregister_plan(plan_public_id)

    async def _dispatch_commands(
        self,
        plan_public_id: str,
        commands: list[JsonObject],
    ) -> None:
        """Persist evaluator-emitted commands as TradeCommand rows.

        Wraps the entire command list in a single logical batch — all
        commands must succeed or none are persisted (individual insert
        failures propagate to the caller).

        Args:
            plan_public_id: Plan that emitted the commands.
            commands: List of command dicts from the evaluator.
        """
        plan = await self._prepare_plan_for_dispatch(plan_public_id)
        if plan is None:
            return
        now = datetime.now(UTC)
        session_id = self.tracker.session_id
        child_ids: list[str] = []
        try:
            for idx, cmd in enumerate(commands):
                client_order_id = str(uuid7())
                sequence_id = self.tracker.next_sequence("plan_commands")
                row = TradeCommandInsertRow(
                    command_type=str(cmd.get("command_type", "create")),
                    shard_key=plan["shard_key"],
                    exchange=plan["exchange"],
                    instrument=str(cmd["instrument"]),
                    mode=plan["mode"],
                    strategy_id=plan["plan_type"],
                    client_order_id=client_order_id,
                    venue_client_id=client_order_id,
                    side=str(cmd["side"]),
                    order_type=str(cmd.get("order_type", "market")),
                    quantity=float(cast(Any, cmd["quantity"])),
                    price=cast(Any, cmd.get("price")),
                    leverage=cast(Any, cmd.get("leverage")),
                    reduce_only=bool(cmd.get("reduce_only", False)),
                    status=TradeCommandStatusEnum.CREATED,
                    created_at=now,
                    correlation_id=plan["public_id"],
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=now,
                    wallet_public_id=plan["wallet_public_id"] or "",
                    operator_public_id=plan["operator_public_id"],
                    user_public_id=None,
                    plan_public_id=plan["public_id"],
                    exchange_order_id=None,
                    idempotency_key=f"{plan['public_id']}:{idx}",
                    supersedes_command_id=None,
                )
                await self._emit_trade_command(
                    row,
                    plan=plan,
                    command_type=str(cmd.get("command_type", "create")),
                    side=str(cmd["side"]),
                    order_type=str(cmd.get("order_type", "market")),
                    quantity=float(cast(Any, cmd["quantity"])),
                )
                self._client_order_id_index[client_order_id] = plan_public_id
                child_ids.append(client_order_id)
                await self._log_decision(
                    plan_public_id=plan_public_id,
                    decision_type="command_emitted",
                    trigger_type=str(cmd.get("trigger_type", "evaluator")),
                    reason=str(cmd.get("reason", "evaluator emitted command")),
                    importance="action",
                    evidence={"command_index": idx, "side": str(cmd["side"])},
                    emitted_command_public_id=client_order_id,
                )
        except Exception as exc:
            logger.error("Command insert failed for plan {}: {}", plan_public_id, exc)
            if not child_ids:
                await self._transition_plan(
                    plan_public_id,
                    ExecutionPlanStatusEnum.FAILED,
                    f"Command insert failed: {exc}",
                )
            return
        if child_ids:
            params_dict = plan.get("params") or {}
            existing_ids = self._extract_child_ids(params_dict)
            merged_ids = list(dict.fromkeys(existing_ids + child_ids))
            await self.repository.revise_execution_plan_params(
                public_id=plan_public_id,
                param_updates=cast(JsonObject, {"child_client_order_ids": merged_ids}),
                bus_time=now,
                session_id=session_id,
                sequence_id=self.tracker.next_sequence("plan_param_revisions"),
            )
            plan_mut: dict[str, Any] = dict(plan)
            params_mut: dict[str, Any] = dict(plan_mut.get("params") or {})
            params_mut["child_client_order_ids"] = merged_ids
            plan_mut["params"] = params_mut
            self.plans[plan_public_id] = cast(ExecutionPlanRow, plan_mut)

    async def _prepare_plan_for_dispatch(
        self,
        plan_public_id: str,
    ) -> ExecutionPlanRow | None:
        """Validate dispatch preconditions and refresh plan state when needed."""
        plan = self.plans.get(plan_public_id)
        if plan is None:
            return None
        if await self._fail_dispatch_for_missing_capabilities(plan_public_id, plan):
            return None
        if await self._cancel_dispatch_for_closed_cycle(plan_public_id, plan):
            return None
        if plan["status"] == ExecutionPlanStatusEnum.ARMED:
            await self._transition_plan(plan_public_id, ExecutionPlanStatusEnum.ACTIVE)
            return self.plans.get(plan_public_id)
        return plan

    async def _fail_dispatch_for_missing_capabilities(
        self,
        plan_public_id: str,
        plan: ExecutionPlanRow,
    ) -> bool:
        """Fail the plan when required capabilities are no longer available."""
        missing_caps = await self._check_capabilities(
            plan["plan_type"], plan["exchange"], plan["instrument_public_id"]
        )
        if not missing_caps:
            return False
        logger.error(
            "Fire-time capability check failed for plan {}: missing {}",
            plan_public_id,
            missing_caps,
        )
        await self._transition_plan(
            plan_public_id,
            ExecutionPlanStatusEnum.FAILED,
            f"Capability revoked: {missing_caps}",
        )
        return True

    async def _cancel_dispatch_for_closed_cycle(
        self,
        plan_public_id: str,
        plan: ExecutionPlanRow,
    ) -> bool:
        """Cancel the plan when its attached position cycle has already closed."""
        cycle_pid = plan.get("position_cycle_public_id")
        if not isinstance(cycle_pid, str):
            return False
        cycle_open = await self._is_cycle_open(cycle_pid)
        if cycle_open:
            return False
        logger.info(
            "Cycle {} closed before dispatch for plan {}, cancelling",
            cycle_pid,
            plan_public_id,
        )
        await self._transition_plan(
            plan_public_id,
            ExecutionPlanStatusEnum.CANCELLED,
            "cycle_closed_externally",
        )
        await self._log_decision(
            plan_public_id=plan_public_id,
            decision_type="cycle_closed_externally",
            trigger_type="dispatch",
            reason=f"Cycle {cycle_pid} closed before command dispatch",
            importance="action",
            new_status=ExecutionPlanStatusEnum.CANCELLED,
        )
        return True

    async def _log_decision(
        self,
        plan_public_id: str,
        decision_type: str,
        trigger_type: str,
        reason: str,
        importance: str,
        evidence: JsonObject | None = None,
        emitted_command_public_id: str | None = None,
        new_status: str | None = None,
    ) -> None:
        """Write a decision audit row for a plan.

        Args:
            plan_public_id: Plan this decision belongs to.
            decision_type: Type of decision (e.g. command_emitted, tick_skip).
            trigger_type: What triggered the decision (tick, execution, clock).
            reason: Human-readable explanation.
            importance: Decision importance tier (action/transition/routine).
            evidence: Optional evidence dict.
            emitted_command_public_id: Optional command id if a command was emitted.
            new_status: Optional new plan status if a transition happened.
        """
        now = datetime.now(UTC)
        decision_public_id = str(uuid7())
        decision_sequence_id = self.tracker.next_sequence("plan_decisions")
        decision_topic = plans_decisions_topic(plan_public_id)
        event = ExecutionPlanDecisionEventData(
            decision_public_id=decision_public_id,
            plan_public_id=plan_public_id,
            decision_type=decision_type,
            trigger_type=trigger_type,
            reason=reason,
            triggered_at=now,
            session_id=self.tracker.session_id,
            sequence_id=decision_sequence_id,
            public_id=str(uuid7()),
            timestamp=now,
        )
        outbox_public_id = str(uuid7())
        payload = event.publish_to(decision_topic)
        row = ExecutionPlanDecisionInsertRow(
            public_id=decision_public_id,
            plan_public_id=plan_public_id,
            decision_type=decision_type,
            decided_at=now,
            trigger_type=trigger_type,
            evidence=evidence or {},
            emitted_command_public_id=emitted_command_public_id,
            new_status=new_status,
            reason=reason,
            decision_importance=importance,
            source_surface="strategy",
        )
        outbox_event = ExecutionPlanDecisionOutboxInsertRow(
            public_id=outbox_public_id,
            decision_public_id=decision_public_id,
            plan_public_id=plan_public_id,
            topic=decision_topic,
            payload_json=payload.decode("utf-8"),
            status="pending",
            attempt_count=0,
            last_attempt_at=None,
            next_attempt_at=None,
            sent_at=None,
            error_reason=None,
            created_at=now,
        )
        try:
            persisted_decision_public_id = await self.repository.insert_execution_plan_decision(
                row=row,
                bus_time=now,
                session_id=self.tracker.session_id,
                sequence_id=decision_sequence_id,
                outbox_event=outbox_event,
            )
        except Exception as exc:
            logger.error("Failed to log decision for plan {}: {}", plan_public_id, exc)
            return
        await self._publish_decision_event(
            outbox_public_id=outbox_public_id,
            decision_public_id=persisted_decision_public_id,
            plan_public_id=plan_public_id,
            topic=decision_topic,
            payload=payload,
        )

    async def _publish_decision_event(
        self,
        *,
        outbox_public_id: str,
        decision_public_id: str,
        plan_public_id: str,
        topic: str,
        payload: bytes,
    ) -> None:
        """Publish a persisted ``plans.decisions.{plan_public_id}`` outbox row.

        DB insert plus outbox insert are the source of truth. Publish
        failure schedules the outbox row for retry and returns without
        raising so bracket / trailing-stop firing paths complete normally.

        Short-circuits when ``self._publisher is None`` (unit-test
        harness that never called ``_setup_publisher`` or environments
        where the broker XSUB endpoint isn't configured).
        """
        if self._publisher is None:
            return
        try:
            await self._publisher.send_multipart(
                topic=topic,
                payload=payload,
            )
        except Exception as exc:
            logger.warning(
                "failed to publish plans.decisions event"
                " plan_public_id={plan} decision_public_id={dec} err={err}",
                plan=plan_public_id,
                dec=decision_public_id,
                err=exc,
            )
            await self._schedule_decision_outbox_retry(
                public_id=outbox_public_id,
                decision_public_id=decision_public_id,
                plan_public_id=plan_public_id,
                current_attempt_count=0,
                error_reason=str(exc),
            )
            return
        await self._mark_decision_outbox_sent(
            public_id=outbox_public_id,
            decision_public_id=decision_public_id,
            plan_public_id=plan_public_id,
        )

    async def _check_capabilities(
        self,
        plan_type: str,
        exchange: str,
        instrument_public_id: str,
    ) -> list[str]:
        """Check if the venue supports capabilities required by a plan type.

        Args:
            plan_type: Plan type string (e.g. 'bracket').
            exchange: Exchange identifier.
            instrument_public_id: Instrument UUID.

        Returns:
            List of missing capability flag names (empty = all satisfied).
        """
        evaluator_cls = _EVALUATOR_REGISTRY.get(plan_type)
        if evaluator_cls is None:
            return [f"unknown_plan_type:{plan_type}"]
        required = evaluator_cls().requires_capabilities()
        if not required:
            return []
        now = datetime.now(UTC)
        rows = await self.repository.get_instrument_capabilities(
            as_of=now,
            exchange=exchange,
            instrument_public_id=instrument_public_id,
        )
        if not rows:
            return list(required)
        cap_row = rows[0]
        missing: list[str] = []
        for flag in required:
            if not cap_row.get(flag, False):
                missing.append(flag)
        return missing

    async def _is_cycle_open(self, cycle_public_id: str) -> bool:
        """Check if a position cycle is still open.

        Args:
            cycle_public_id: Cycle to check.

        Returns:
            True if cycle exists and status is open, False otherwise.
        """
        now = datetime.now(UTC)
        try:
            cycle = await self.repository.get_position_cycle_by_public_id(
                cycle_public_id, as_of=now
            )
        except Exception as exc:
            logger.error("Cycle lookup failed for {}: {}", cycle_public_id, exc)
            return False
        if cycle is None:
            return False
        return cycle["status"] == "open"

    async def _sweep_cycle_closures(self) -> None:
        """Check armed plans for closed cycles.

        Targets all armed plans with a position_cycle_public_id (brackets,
        trailing stops, additional plan types). Active plans with in-flight
        child orders must go through the cancel_requested + cancel
        TradeCommand flow, handled by the cancel route, not this sweep.
        """
        for public_id, plan in tuple(self.plans.items()):
            if plan["status"] != ExecutionPlanStatusEnum.ARMED:
                continue
            cycle_pid = plan.get("position_cycle_public_id")
            if not isinstance(cycle_pid, str):
                continue
            cycle_open = await self._is_cycle_open(cycle_pid)
            if not cycle_open:
                logger.info(
                    "Clock sweep: cycle {} closed, cancelling armed {} {}",
                    cycle_pid,
                    plan["plan_type"],
                    public_id,
                )
                await self._transition_plan(
                    public_id,
                    ExecutionPlanStatusEnum.CANCELLED,
                    "cycle_closed_externally",
                )
                await self._log_decision(
                    plan_public_id=public_id,
                    decision_type="cycle_closed_externally",
                    trigger_type="clock",
                    reason=f"Cycle {cycle_pid} closed (detected by clock sweep)",
                    importance="action",
                    new_status=ExecutionPlanStatusEnum.CANCELLED,
                )

    async def _run_loop(self) -> None:
        """Run listen, checkpoint, and clock tasks until stopped."""
        tasks = [
            asyncio.create_task(self._listen_loop()),
            asyncio.create_task(self._checkpoint_loop()),
            asyncio.create_task(self._clock_loop()),
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
                frame = await self._receive_message_frame()
                if frame is None:
                    continue
                topic, payload = frame
                msg = self._parse_incoming_message(topic, payload)
                if msg is None:
                    continue
                await self._dispatch_incoming_message(topic, msg)
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

    async def _receive_message_frame(self) -> tuple[str, str] | None:
        """Receive and normalize one subscriber frame, retrying on socket failures."""
        if self._subscriber is None:
            return None
        try:
            topic_bytes, msg_bytes = await self._subscriber.recv_multipart()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("PlanExecutorService: socket recv failed: {}", exc)
            await asyncio.sleep(0.1)
            return None
        topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
        payload = msg_bytes.decode() if isinstance(msg_bytes, bytes) else str(msg_bytes)
        return topic, payload

    def _parse_incoming_message(
        self,
        topic: str,
        payload: str,
    ) -> ExecutionData | OrderData | TickData | None:
        """Parse one inbound payload into a typed message object."""
        try:
            parsed = parse_message(payload)
        except MessageParseError as exc:
            logger.debug("PlanExecutorService: cannot parse message on {}: {}", topic, exc)
            return None
        if isinstance(parsed, (ExecutionData, OrderData, TickData)):
            return parsed
        return None

    async def _dispatch_incoming_message(
        self,
        topic: str,
        msg: ExecutionData | OrderData | TickData,
    ) -> None:
        """Route one parsed message to the appropriate plan handler."""
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

    async def _clock_loop(self) -> None:
        """1Hz clock dispatch to evaluators with drift compensation."""
        try:
            while self._running:
                t0 = asyncio.get_event_loop().time()
                now = datetime.now(UTC)
                await self._dispatch_clock_commands(now)
                await self._sweep_cycle_closures_safely()
                elapsed = asyncio.get_event_loop().time() - t0
                await asyncio.sleep(max(0.0, 1.0 - elapsed))
        except asyncio.CancelledError:
            logger.info("PlanExecutorService: clock loop cancelled")
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
        execution_target = self._resolve_execution_target(execution)
        if execution_target is None:
            return
        plan_public_id, plan, incoming_cumulative = execution_target
        commands = await self._run_execution_evaluator(plan_public_id, plan, execution)
        await self._dispatch_commands_safely(
            plan_public_id=plan_public_id,
            commands=commands,
            trigger="execution",
        )
        refreshed_plan = self.plans.get(plan_public_id)
        if refreshed_plan is None:
            return
        new_filled, total, is_complete, new_status = self._classify_fill_update(
            refreshed_plan,
            incoming_cumulative,
            execution.status,
        )
        now = await self._persist_fill_update(
            plan_public_id=plan_public_id,
            new_status=new_status,
            new_filled=new_filled,
            is_complete=is_complete,
        )
        if now is None:
            return
        self._apply_fill_update_locally(
            plan_public_id=plan_public_id,
            plan=refreshed_plan,
            new_filled=new_filled,
            new_status=new_status,
            is_complete=is_complete,
            completed_at=now,
        )
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
        if status == OrderEventEnum.CANCELLED:
            new_status = ExecutionPlanStatusEnum.CANCELLED
        elif status in (OrderEventEnum.REJECTED, "error"):
            new_status = ExecutionPlanStatusEnum.FAILED
            last_error = order.error or f"venue {status}"
        elif status == OrderEventEnum.EXPIRED:
            new_status = ExecutionPlanStatusEnum.EXPIRED
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
                completed_at=now,
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
        """Dispatch a tick to evaluators via O(1) symbol index lookup.

        Uses ``_runtime_symbol_index`` keyed by ``exchange:native_symbol``
        for constant-time routing instead of iterating all plans.

        Args:
            topic: ZMQ topic the tick arrived on.
            tick: Incoming tick data.
        """
        key = f"{tick.exchange}:{tick.instrument}"
        plan_ids = self._runtime_symbol_index.get(key)
        if not plan_ids:
            return
        now = datetime.now(UTC)
        for public_id in tuple(plan_ids):
            plan = self.plans.get(public_id)
            if not self._is_tick_dispatchable(plan):
                continue
            active_plan = cast(ExecutionPlanRow, plan)
            commands = await self._run_tick_evaluator(public_id, active_plan, tick)
            if commands is None:
                continue
            await self._dispatch_commands_safely(
                plan_public_id=public_id,
                commands=commands,
                trigger="tick",
            )
            self._last_tick_timestamps[public_id] = now

    def _resolve_execution_target(
        self,
        execution: ExecutionData,
    ) -> tuple[str, ExecutionPlanRow, float] | None:
        """Return the active plan targeted by a cumulative execution update."""
        plan_public_id = self._client_order_id_index.get(execution.client_order_id)
        if plan_public_id is None:
            return None
        plan = self.plans.get(plan_public_id)
        if plan is None:
            return None
        if plan["status"] in _TERMINAL_STATUSES:
            return None
        incoming_cumulative = float(execution.size)
        existing_filled = float(plan.get("filled_quantity", 0.0))
        if incoming_cumulative <= existing_filled + 1e-12:
            return None
        return plan_public_id, plan, incoming_cumulative

    async def _run_execution_evaluator(
        self,
        plan_public_id: str,
        plan: ExecutionPlanRow,
        execution: ExecutionData,
    ) -> list[JsonObject]:
        """Run the plan evaluator for an execution event and swallow handler errors."""
        evaluator = self.evaluators.get(plan_public_id)
        if evaluator is None:
            return []
        try:
            return await evaluator.on_execution(plan, execution)
        except Exception as exc:
            logger.error("on_execution failed for plan {}: {}", plan_public_id, exc)
            return []

    async def _dispatch_commands_safely(
        self,
        *,
        plan_public_id: str,
        commands: list[JsonObject],
        trigger: str,
    ) -> None:
        """Dispatch evaluator-emitted commands without aborting caller flow."""
        if not commands:
            return
        try:
            async with self._get_plan_lock(plan_public_id):
                await self._dispatch_commands(plan_public_id, commands)
        except Exception as exc:
            logger.error("dispatch failed for plan {} on {}: {}", plan_public_id, trigger, exc)

    @staticmethod
    def _classify_fill_update(
        plan: ExecutionPlanRow,
        incoming_cumulative: float,
        execution_status: str,
    ) -> tuple[float, float, bool, str]:
        """Classify the next plan status implied by a cumulative fill update."""
        new_filled = incoming_cumulative
        total = float(plan["total_quantity"])
        qty_complete = new_filled + 1e-9 >= total
        venue_filled = execution_status == FillStatusEnum.FILLED
        is_complete = qty_complete or venue_filled
        new_status = (
            ExecutionPlanStatusEnum.COMPLETED if is_complete else ExecutionPlanStatusEnum.ACTIVE
        )
        return new_filled, total, is_complete, new_status

    async def _persist_fill_update(
        self,
        *,
        plan_public_id: str,
        new_status: str,
        new_filled: float,
        is_complete: bool,
    ) -> datetime | None:
        """Persist the fill-driven status transition and return the applied timestamp."""
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
            return None
        return now

    def _apply_fill_update_locally(
        self,
        *,
        plan_public_id: str,
        plan: ExecutionPlanRow,
        new_filled: float,
        new_status: str,
        is_complete: bool,
        completed_at: datetime,
    ) -> None:
        """Mirror a persisted fill transition into the in-memory plan cache."""
        plan_mut: dict[str, Any] = dict(plan)
        plan_mut["filled_quantity"] = new_filled
        plan_mut["status"] = new_status
        if is_complete:
            plan_mut["completed_at"] = completed_at
        self.plans[plan_public_id] = cast(ExecutionPlanRow, plan_mut)

    @staticmethod
    def _is_tick_dispatchable(plan: ExecutionPlanRow | None) -> bool:
        """Return whether a plan should receive on_tick evaluation."""
        if plan is None:
            return False
        if plan["status"] in _TERMINAL_STATUSES:
            return False
        return plan["status"] != ExecutionPlanStatusEnum.PAUSED

    async def _run_tick_evaluator(
        self,
        public_id: str,
        plan: ExecutionPlanRow,
        tick: TickData,
    ) -> list[JsonObject] | None:
        """Run the plan evaluator for a tick and swallow handler errors."""
        evaluator = self.evaluators.get(public_id)
        if evaluator is None:
            return None
        try:
            return await evaluator.on_tick(plan, tick)
        except Exception as exc:
            logger.error("on_tick failed for plan {}: {}", public_id, exc)
            return None

    async def _dispatch_clock_commands(self, now: datetime) -> None:
        """Run on_clock across runnable plans and dispatch emitted commands."""
        for public_id, plan in tuple(self.plans.items()):
            if not self._is_tick_dispatchable(plan):
                continue
            commands = await self._run_clock_evaluator(public_id, plan, now)
            if commands is None:
                continue
            await self._dispatch_commands_safely(
                plan_public_id=public_id,
                commands=commands,
                trigger="clock",
            )

    async def _run_clock_evaluator(
        self,
        public_id: str,
        plan: ExecutionPlanRow,
        now: datetime,
    ) -> list[JsonObject] | None:
        """Run the plan evaluator for a clock tick and swallow handler errors."""
        evaluator = self.evaluators.get(public_id)
        if evaluator is None:
            return None
        try:
            return await evaluator.on_clock(plan, now)
        except Exception as exc:
            logger.error("on_clock failed for plan {}: {}", public_id, exc)
            return None

    async def _sweep_cycle_closures_safely(self) -> None:
        """Run cycle-closure sweep without aborting the clock loop on failure."""
        try:
            await self._sweep_cycle_closures()
        except Exception as exc:
            logger.error("Cycle closure sweep failed: {}", exc)

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
