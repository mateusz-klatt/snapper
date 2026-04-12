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
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.evaluator import PlanEvaluator
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.json_types import JsonObject
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandInsertRow
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
    "bracket": BracketEvaluator,
}

_CHECKPOINT_INTERVAL_S = 10.0
_SLOW_JOINER_STABILIZATION_S = 0.5
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
        self._runtime_symbol_index: dict[str, set[str]] = {}
        self._plan_locks: dict[str, asyncio.Lock] = {}
        self._running = False
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None

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
        self._setup_subscriber()
        if self._subscriber is not None:
            await asyncio.sleep(_SLOW_JOINER_STABILIZATION_S)
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
        """Load all actionable plans from DB and instantiate evaluators.

        For plans recovered in ``cancel_requested`` status with no
        outstanding cancel ``trade_commands`` row, re-emit the cancel
        command. This closes the gap where a cancel route crashed
        between the plan transition and the command insert, leaving the
        plan stranded in ``cancel_requested`` forever.
        """
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
            if row["status"] == "cancel_requested":
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
        """
        params = plan.get("params") or {}
        native_instrument = params.get("native_instrument")
        if not isinstance(native_instrument, str):
            return
        child_order_ids = self._extract_child_ids(params)
        if not child_order_ids:
            return
        for child_client_order_id in child_order_ids:
            await self._reemit_single_stranded_cancel(
                plan, child_client_order_id, native_instrument
            )

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

    async def _reemit_single_stranded_cancel(
        self,
        plan: ExecutionPlanRow,
        child_client_order_id: str,
        native_instrument: str,
    ) -> None:
        """Re-emit a cancel command for a single child order.

        Args:
            plan: Recovered plan row in cancel_requested status.
            child_client_order_id: Child order to cancel.
            native_instrument: Native exchange symbol for the cancel command.
        """
        params = plan.get("params") or {}
        try:
            linked_plan_id = await self.repository.get_plan_public_id_for_client_order_id(
                child_client_order_id
            )
        except Exception as exc:
            logger.error("Stranded cancel lookup failed for {}: {}", plan["public_id"], exc)
            return
        if linked_plan_id is None or linked_plan_id != plan["public_id"]:
            logger.warning(
                "PlanExecutorService: stranded cancel skipped for plan {} "
                "(child {} links to plan {})",
                plan["public_id"],
                child_client_order_id,
                linked_plan_id,
            )
            return
        now = datetime.now(UTC)
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
            "order_type": str(params.get("venue_order_type", "market")),
            "quantity": plan["total_quantity"],
            "price": params.get("price"),
            "leverage": params.get("leverage"),
            "reduce_only": False,
            "status": "created",
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
            await self.repository.insert_trade_command(cast(Any, row))
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
                started_at=now if new_status == "active" else None,
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
        plan = self.plans.get(plan_public_id)
        if plan is None:
            return
        missing_caps = await self._check_capabilities(
            plan["plan_type"], plan["exchange"], plan["instrument_public_id"]
        )
        if missing_caps:
            logger.error(
                "Fire-time capability check failed for plan {}: missing {}",
                plan_public_id,
                missing_caps,
            )
            await self._transition_plan(
                plan_public_id, "failed", f"Capability revoked: {missing_caps}"
            )
            return
        if plan["status"] == "armed":
            await self._transition_plan(plan_public_id, "active")
            plan = self.plans.get(plan_public_id)
            if plan is None:
                return
        now = datetime.now(UTC)
        session_id = self.tracker.session_id
        child_ids: list[str] = []
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
                status="created",
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
            await self.repository.insert_trade_command(row)
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
        row = ExecutionPlanDecisionInsertRow(
            plan_public_id=plan_public_id,
            decision_type=decision_type,
            decided_at=now,
            trigger_type=trigger_type,
            evidence=evidence or {},
            emitted_command_public_id=emitted_command_public_id,
            new_status=new_status,
            reason=reason,
            decision_importance=importance,
        )
        try:
            await self.repository.insert_execution_plan_decision(
                row=row,
                bus_time=now,
                session_id=self.tracker.session_id,
                sequence_id=self.tracker.next_sequence("plan_decisions"),
            )
        except Exception as exc:
            logger.error("Failed to log decision for plan {}: {}", plan_public_id, exc)

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

    async def _clock_loop(self) -> None:
        """1Hz clock dispatch to evaluators with drift compensation."""
        try:
            while self._running:
                t0 = asyncio.get_event_loop().time()
                now = datetime.now(UTC)
                for public_id, plan in list(self.plans.items()):
                    if plan["status"] in _TERMINAL_STATUSES:
                        continue
                    if plan["status"] == "paused":
                        continue
                    evaluator = self.evaluators.get(public_id)
                    if evaluator is None:
                        continue
                    try:
                        commands = await evaluator.on_clock(plan, now)
                    except Exception as exc:
                        logger.error("on_clock failed for plan {}: {}", public_id, exc)
                        continue
                    if commands:
                        try:
                            async with self._get_plan_lock(public_id):
                                await self._dispatch_commands(public_id, commands)
                        except Exception as exc:
                            logger.error("dispatch failed for plan {} on clock: {}", public_id, exc)
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
        commands: list[JsonObject] = []
        if evaluator is not None:
            try:
                commands = await evaluator.on_execution(plan, execution)
            except Exception as exc:
                logger.error("on_execution failed for plan {}: {}", plan_public_id, exc)
        if commands:
            try:
                async with self._get_plan_lock(plan_public_id):
                    await self._dispatch_commands(plan_public_id, commands)
            except Exception as exc:
                logger.error("dispatch failed for plan {} on execution: {}", plan_public_id, exc)
        new_filled = incoming_cumulative
        total = float(plan["total_quantity"])
        qty_complete = new_filled + 1e-9 >= total
        venue_filled = execution.status == "filled"
        is_complete = qty_complete or venue_filled
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
        for public_id in list(plan_ids):
            plan = self.plans.get(public_id)
            if plan is None:
                continue
            if plan["status"] in _TERMINAL_STATUSES:
                continue
            if plan["status"] == "paused":
                continue
            evaluator = self.evaluators.get(public_id)
            if evaluator is None:
                continue
            commands: list[JsonObject] = []
            try:
                commands = await evaluator.on_tick(plan, tick)
            except Exception as exc:
                logger.error("on_tick failed for plan {}: {}", public_id, exc)
                continue
            if commands:
                try:
                    async with self._get_plan_lock(public_id):
                        await self._dispatch_commands(public_id, commands)
                except Exception as exc:
                    logger.error("dispatch failed for plan {} on tick: {}", public_id, exc)
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
