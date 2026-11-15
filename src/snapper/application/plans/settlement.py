"""Deterministic terminal settlement for execution plans on startup and CLI repair.

Execution plans whose single child create command reached a durable terminal
state while the plan executor missed the live bus transition remain stuck in an
actionable status (`active` or `cancel_requested`). Both startup recovery in
`PlanExecutorService` and the operator `settle-plans` command share the same
evidence inspection and SCD2 status-transition builder defined here:

- Only registered single-order plan types (`manual_once`, `bracket`,
  `trailing_stop`) are eligible for child-terminal settlement.
- Every child client order id recorded in `plan["params"]` must resolve to an
  active `create` or `submit` command on the plan's exchange whose
  `plan_public_id` matches `plan["public_id"]`.
- `rejected` children settle to `failed` only when no `order_accepted` or
  `fill_observed` venue event carries an id higher than the latest
  `order_rejected` event, preserving reconciliation resurrection of false
  venue rejections.
- Plans in `cancel_requested` settle only when durable evidence confirms no
  live venue order remains (venue-evidenced `cancelled`, `filled`, or
  `expired` via `target_end_time`, pre-dispatch outbox TTL expiry without live
  venue events, or pre-venue `order_breaker_open` / `order_interlock_blocked`
  refusal without live venue events), preserving Item G2b stranded-cancel
  re-emission for unconfirmed targets.
"""

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Final
from typing import Protocol
from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.cancel_target import VenueEventTimeReader
from snapper.application.trade.cancel_target import target_end_time
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import ExecutionPlan
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import where_active
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow

SETTLEABLE_PLAN_TYPES: Final[frozenset[str]] = frozenset(
    {
        "manual_once",
        "bracket",
        "trailing_stop",
    }
)
"""Registered single-order plan types eligible for child-terminal settlement."""

_CHILD_TERMINAL_STATUSES: Final[frozenset[str]] = frozenset(
    {
        TradeCommandStatusEnum.FILLED,
        TradeCommandStatusEnum.CANCELLED,
        TradeCommandStatusEnum.EXPIRED,
        TradeCommandStatusEnum.REJECTED,
        TradeCommandStatusEnum.FAILED,
    }
)

_LIVE_EVIDENCE_EVENTS: Final[frozenset[str]] = frozenset(
    {
        "order_accepted",
        "fill_observed",
    }
)

_PRE_VENUE_REFUSAL_EVENTS: Final[frozenset[str]] = frozenset(
    {
        "order_breaker_open",
        "order_interlock_blocked",
    }
)

_OUTBOX_EXPIRY_PREFIX: Final[str] = "expired by outbox dispatch TTL"
_MAX_LAST_ERROR_LENGTH: Final[int] = 1024


class PlanSettlementReader(VenueEventTimeReader, Protocol):
    """Read-only repository contract consumed by execution-plan settlement."""

    async def get_active_execution_plans(self) -> list[ExecutionPlanRow]:
        """Return all currently actionable execution plan versions.

        Returns:
            Active execution plan rows ordered by creation time.
        """

    async def get_active_create_command_by_client_order_id(
        self,
        client_order_id: str,
        exchange: str,
    ) -> TradeCommandRow | None:
        """Return the active create or submit command for a client order id.

        Args:
            client_order_id: Child client order id recorded on the plan.
            exchange: Exchange name scoping the lookup.

        Returns:
            Active command row, or None when absent.
        """

    async def get_order_lifecycle_events(
        self,
        client_order_ids: list[str],
    ) -> list[VenueEventRow]:
        """Return ordered lifecycle venue events for the requested client ids.

        Args:
            client_order_ids: Client order ids to inspect.

        Returns:
            Venue lifecycle events ordered chronologically.
        """

    async def get_max_cumulative_fill_venue_event(
        self,
        client_order_id: str,
    ) -> VenueEventRow | None:
        """Return the highest cumulative fill venue observation for one order.

        Args:
            client_order_id: Client order id to inspect.

        Returns:
            Cumulative fill venue event row, or None when no fills exist.
        """


class PlanSettlementDriftError(ValueError):
    """Raised when durable plan state differs from a previewed settlement set."""


@dataclass(frozen=True, slots=True)
class PlanChildOutcome:
    """Resolved terminal evidence for a single child create command.

    Attributes:
        client_order_id: Child client order identifier.
        command_public_id: Public id of the child `trade_commands` row.
        command_status: Durable terminal status of the child command.
        cumulative_fill_size: Maximum observed cumulative fill quantity.
        last_error: Optional error message recorded on the child command.
    """

    client_order_id: str
    command_public_id: str
    command_status: str
    cumulative_fill_size: float
    last_error: str | None


@dataclass(frozen=True, slots=True)
class PlanSettlementCandidate:
    """Validated settlement transition for one actionable execution plan.

    Attributes:
        plan_public_id: Public id of the execution plan.
        plan_type: Evaluator type of the plan.
        exchange: Lowercase venue identifier.
        mode: Execution mode (`live` or `paper`).
        shard_key: Coordinator shard key of the plan.
        current_status: Current actionable status observed during inspection.
        current_session_id: Session id of the inspected SCD2 version.
        current_sequence_id: Sequence id of the inspected SCD2 version.
        current_filled_quantity: Filled quantity stored on the current version.
        target_status: Terminal status to write (`completed`, `failed`,
            `cancelled`, or `expired`).
        filled_quantity: Updated filled quantity when venue fill evidence
            exceeds zero, otherwise None to preserve the existing value.
        last_error: Error description for `failed` transitions, else None.
        children: Resolved child outcomes in deterministic parameter order.
    """

    plan_public_id: str
    plan_type: str
    exchange: str
    mode: str
    shard_key: str
    current_status: str
    current_session_id: str
    current_sequence_id: int
    current_filled_quantity: float
    target_status: str
    filled_quantity: float | None
    last_error: str | None
    children: tuple[PlanChildOutcome, ...]


@dataclass(frozen=True, slots=True)
class PlanSettlementSkipped:
    """Explanation for why an actionable execution plan is not settled now.

    Attributes:
        plan_public_id: Public id of the execution plan.
        plan_type: Evaluator type of the plan.
        exchange: Lowercase venue identifier.
        mode: Execution mode (`live` or `paper`).
        current_status: Current actionable status of the plan.
        skip_code: Machine-stable category code for skipping settlement.
        detail: Human-readable explanation for operator preview output.
    """

    plan_public_id: str
    plan_type: str
    exchange: str
    mode: str
    current_status: str
    skip_code: str
    detail: str


@dataclass(frozen=True, slots=True)
class PlanSettlementPreview:
    """Complete read-only preview and digest for operator plan settlement.

    Attributes:
        mode: Optional execution mode filter (`live`, `paper`, or None for all).
        candidates: Ordered settlement candidates ready to apply.
        skipped: Ordered actionable plans excluded from settlement.
        digest: Lowercase 64-hex SHA-256 digest binding the scope and candidates.
    """

    mode: str | None
    candidates: tuple[PlanSettlementCandidate, ...]
    skipped: tuple[PlanSettlementSkipped, ...]
    digest: str


@dataclass(frozen=True, slots=True)
class PlanSettlementReceipt:
    """Durable write receipt returned after applying a settlement preview.

    Attributes:
        mode: Optional execution mode filter bound to the preview.
        digest: Verified SHA-256 preview digest.
        settled_public_ids: Public ids settled in transaction order.
        settled_at: UTC timestamp written to `known_to` and `completed_at`.
        session_id: Provenance session id stamped on the new SCD2 versions.
        count: Number of execution plan versions settled.
    """

    mode: str | None
    digest: str
    settled_public_ids: tuple[str, ...]
    settled_at: datetime
    session_id: str
    count: int


def extract_plan_child_ids(params: JsonObject) -> list[str]:
    """Extract deduplicated child client order ids in deterministic order.

    Reads both `child_client_order_ids` (list) and `child_client_order_id`
    (legacy scalar) while preserving first-seen order across restarts and
    digest previews.

    Args:
        params: Execution plan `params` JSON object.

    Returns:
        Ordered list of distinct non-empty child client order ids.
    """
    ordered: list[str] = []
    seen: set[str] = set()
    raw_list = params.get("child_client_order_ids")
    if isinstance(raw_list, list):
        for item in raw_list:
            if isinstance(item, str) and item and item not in seen:
                seen.add(item)
                ordered.append(item)
    single = params.get("child_client_order_id")
    if isinstance(single, str) and single and single not in seen:
        ordered.append(single)
    return ordered


def _skipped(plan: ExecutionPlanRow, skip_code: str, detail: str) -> PlanSettlementSkipped:
    """Construct a `PlanSettlementSkipped` record for a plan row.

    Args:
        plan: Actionable execution plan row being inspected.
        skip_code: Machine-stable skip classification code.
        detail: Operator-facing explanation.

    Returns:
        Frozen skipped inspection record.
    """
    return PlanSettlementSkipped(
        plan_public_id=plan["public_id"],
        plan_type=plan["plan_type"],
        exchange=plan["exchange"],
        mode=plan["mode"],
        current_status=plan["status"],
        skip_code=skip_code,
        detail=detail,
    )


def _has_post_rejection_live_evidence(events: Sequence[VenueEventRow]) -> bool:
    """Return True when live venue evidence post-dates the latest rejection.

    Args:
        events: Lifecycle venue events for the child client order id.

    Returns:
        True when `order_accepted` or `fill_observed` shows the order alive
        after rejection (or without any recorded `order_rejected` event).
    """
    rejection_ids = [row["id"] for row in events if row["event_type"] == "order_rejected"]
    if not rejection_ids:
        return any(row["event_type"] in _LIVE_EVIDENCE_EVENTS for row in events)
    latest_rejection_id = max(rejection_ids)
    return any(
        row["event_type"] in _LIVE_EVIDENCE_EVENTS and row["id"] > latest_rejection_id
        for row in events
    )


def _has_pre_venue_refusal_only(events: Sequence[VenueEventRow]) -> bool:
    """Return True when local breaker/interlock refused before any live venue event.

    Args:
        events: Lifecycle venue events for the child client order id.

    Returns:
        True when a pre-venue refusal event exists and no live venue event exists.
    """
    has_refusal = any(row["event_type"] in _PRE_VENUE_REFUSAL_EVENTS for row in events)
    has_live = any(row["event_type"] in _LIVE_EVIDENCE_EVENTS for row in events)
    return has_refusal and not has_live


async def _cancel_requested_child_confirmed(
    reader: PlanSettlementReader,
    command: TradeCommandRow,
    events: Sequence[VenueEventRow],
) -> bool:
    """Return True when a `cancel_requested` plan's child is provably ended.

    Preserves Item G2b: a `cancel_requested` plan keeps re-emitting its cancel
    unless durable venue evidence (or pre-venue refusal / pre-dispatch outbox
    TTL expiry with zero live venue events) proves no live order remains.

    Args:
        reader: Settlement repository reader.
        command: Active child create command row.
        events: Lifecycle venue events for the child order.

    Returns:
        True when the child cannot still be live on the venue.
    """
    status = command["status"]
    client_order_id = command["client_order_id"]
    if status in (TradeCommandStatusEnum.CANCELLED, TradeCommandStatusEnum.FILLED):
        return await target_end_time(reader, status, client_order_id) is not None
    if status == TradeCommandStatusEnum.EXPIRED:
        if await target_end_time(reader, status, client_order_id) is not None:
            return True
        last_error = command.get("last_error") or ""
        has_live = any(row["event_type"] in _LIVE_EVIDENCE_EVENTS for row in events)
        return last_error.startswith(_OUTBOX_EXPIRY_PREFIX) and not has_live
    if status == TradeCommandStatusEnum.FAILED:
        return _has_pre_venue_refusal_only(events)
    return False


async def _inspect_child_command(
    reader: PlanSettlementReader,
    plan: ExecutionPlanRow,
    client_order_id: str,
) -> PlanChildOutcome | PlanSettlementSkipped:
    """Validate one child order's terminal state for plan settlement.

    Args:
        reader: Settlement repository reader.
        plan: Parent execution plan row.
        client_order_id: Child client order id to inspect.

    Returns:
        `PlanChildOutcome` when eligible, or `PlanSettlementSkipped` otherwise.
    """
    command = await reader.get_active_create_command_by_client_order_id(
        client_order_id, plan["exchange"]
    )
    if command is None:
        return _skipped(
            plan,
            "missing_child_command",
            f"child {client_order_id} has no active create command on {plan['exchange']}",
        )
    linked_plan_id = command.get("plan_public_id")
    if linked_plan_id != plan["public_id"]:
        return _skipped(
            plan,
            "plan_link_mismatch",
            f"child {client_order_id} links to plan {linked_plan_id!r}",
        )
    status = command["status"]
    if status not in _CHILD_TERMINAL_STATUSES:
        return _skipped(
            plan,
            "non_terminal_child",
            f"child {client_order_id} is in status {status}",
        )
    events = await reader.get_order_lifecycle_events([client_order_id])
    if status == TradeCommandStatusEnum.REJECTED and _has_post_rejection_live_evidence(events):
        return _skipped(
            plan,
            "false_rejection_pending_resurrection",
            f"child {client_order_id} has live venue evidence after rejection",
        )
    if plan[
        "status"
    ] == ExecutionPlanStatusEnum.CANCEL_REQUESTED and not await _cancel_requested_child_confirmed(
        reader, command, events
    ):
        return _skipped(
            plan,
            "cancel_requested_unconfirmed_target",
            f"child {client_order_id} status {status} lacks confirmed end for cancel_requested",
        )
    fill_row = await reader.get_max_cumulative_fill_venue_event(client_order_id)
    raw_cum_fill = None if fill_row is None else fill_row["cum_fill_size"]
    cum_fill = raw_cum_fill if raw_cum_fill is not None and raw_cum_fill > 0.0 else 0.0
    return PlanChildOutcome(
        client_order_id=client_order_id,
        command_public_id=command["public_id"],
        command_status=status,
        cumulative_fill_size=cum_fill,
        last_error=command.get("last_error"),
    )


def _derive_target_transition(
    plan: ExecutionPlanRow,
    outcomes: Sequence[PlanChildOutcome],
) -> tuple[str, float | None, str | None]:
    """Derive the terminal plan status, filled quantity, and error text.

    Args:
        plan: Inspected execution plan row.
        outcomes: Validated terminal outcomes for every child order.

    Returns:
        Tuple of `(target_status, filled_quantity, last_error)`.
    """
    total_cum_fill = sum(item.cumulative_fill_size for item in outcomes)
    resolved_fill = max(plan["filled_quantity"], total_cum_fill) if total_cum_fill > 0.0 else None
    statuses = {item.command_status for item in outcomes}
    if statuses == {TradeCommandStatusEnum.FILLED}:
        return ExecutionPlanStatusEnum.COMPLETED, resolved_fill, None
    if TradeCommandStatusEnum.FAILED in statuses or TradeCommandStatusEnum.REJECTED in statuses:
        first_error = next((item.last_error for item in outcomes if item.last_error), None)
        default_error = (
            "venue rejected"
            if TradeCommandStatusEnum.REJECTED in statuses
            else "child command failed"
        )
        error_text = (first_error or default_error)[:_MAX_LAST_ERROR_LENGTH]
        return ExecutionPlanStatusEnum.FAILED, resolved_fill, error_text
    if TradeCommandStatusEnum.CANCELLED in statuses:
        return ExecutionPlanStatusEnum.CANCELLED, resolved_fill, None
    return ExecutionPlanStatusEnum.EXPIRED, resolved_fill, None


async def inspect_plan_settlement(
    reader: PlanSettlementReader,
    plan: ExecutionPlanRow,
) -> PlanSettlementCandidate | PlanSettlementSkipped:
    """Inspect one actionable execution plan for deterministic settlement.

    Args:
        reader: Repository reader providing command and venue-event lookups.
        plan: Actionable execution plan version to inspect.

    Returns:
        `PlanSettlementCandidate` when all guards pass, otherwise
        `PlanSettlementSkipped`.
    """
    if plan["plan_type"] not in SETTLEABLE_PLAN_TYPES:
        return _skipped(
            plan,
            "unsupported_plan_type",
            f"plan_type {plan['plan_type']} is not a single-order plan",
        )
    params = plan.get("params") or {}
    child_ids = extract_plan_child_ids(params)
    if not child_ids:
        return _skipped(
            plan,
            "no_child_orders",
            "plan params carry no child client order ids",
        )
    outcomes: list[PlanChildOutcome] = []
    for client_order_id in child_ids:
        outcome = await _inspect_child_command(reader, plan, client_order_id)
        if isinstance(outcome, PlanSettlementSkipped):
            return outcome
        outcomes.append(outcome)
    target_status, filled_quantity, last_error = _derive_target_transition(plan, outcomes)
    return PlanSettlementCandidate(
        plan_public_id=plan["public_id"],
        plan_type=plan["plan_type"],
        exchange=plan["exchange"],
        mode=plan["mode"],
        shard_key=plan["shard_key"],
        current_status=plan["status"],
        current_session_id=plan["session_id"],
        current_sequence_id=plan["sequence_id"],
        current_filled_quantity=plan["filled_quantity"],
        target_status=target_status,
        filled_quantity=filled_quantity,
        last_error=last_error,
        children=tuple(outcomes),
    )


def _compute_preview_digest(
    mode: str | None,
    candidates: Sequence[PlanSettlementCandidate],
) -> str:
    """Compute a canonical SHA-256 digest over the settlement scope and set.

    Args:
        mode: Optional execution mode filter (`live`, `paper`, or None).
        candidates: Ordered settlement candidates.

    Returns:
        Lowercase 64-character hexadecimal SHA-256 digest.
    """
    payload = {
        "mode": mode,
        "candidates": [
            {
                "plan_public_id": item.plan_public_id,
                "plan_type": item.plan_type,
                "exchange": item.exchange,
                "mode": item.mode,
                "shard_key": item.shard_key,
                "current_status": item.current_status,
                "current_session_id": item.current_session_id,
                "current_sequence_id": item.current_sequence_id,
                "current_filled_quantity": item.current_filled_quantity,
                "target_status": item.target_status,
                "filled_quantity": item.filled_quantity,
                "last_error": item.last_error,
                "children": [
                    {
                        "client_order_id": child.client_order_id,
                        "command_public_id": child.command_public_id,
                        "command_status": child.command_status,
                        "cumulative_fill_size": child.cumulative_fill_size,
                        "last_error": child.last_error,
                    }
                    for child in item.children
                ],
            }
            for item in candidates
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def preview_plan_settlements(
    reader: PlanSettlementReader,
    *,
    mode: str | None = None,
) -> PlanSettlementPreview:
    """Build a deterministic read-only settlement preview and SHA-256 digest.

    Args:
        reader: Settlement repository reader.
        mode: Optional mode filter (`live` or `paper`); None inspects all modes.

    Returns:
        Frozen preview containing candidates, skipped plans, and SHA-256 digest.
    """
    rows = await reader.get_active_execution_plans()
    selected = [row for row in rows if mode is None or row["mode"] == mode]
    selected.sort(key=lambda row: (row["created_at"], row["public_id"]))
    candidates: list[PlanSettlementCandidate] = []
    skipped: list[PlanSettlementSkipped] = []
    for row in selected:
        decision = await inspect_plan_settlement(reader, row)
        if isinstance(decision, PlanSettlementCandidate):
            candidates.append(decision)
        else:
            skipped.append(decision)
    digest = _compute_preview_digest(mode, candidates)
    return PlanSettlementPreview(
        mode=mode,
        candidates=tuple(candidates),
        skipped=tuple(skipped),
        digest=digest,
    )


async def _begin_plan_write_transaction(
    repository: SQLAlchemyRepository,
    session: AsyncSession,
) -> None:
    """Acquire write serialization on SQLite before reading current plan rows.

    Args:
        repository: Target SQLAlchemy repository.
        session: Fresh session whose transaction is starting.
    """
    if repository.dialect_name == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))


def _matches_candidate_version(
    existing: ExecutionPlan,
    candidate: PlanSettlementCandidate,
) -> bool:
    """Return True when a locked ORM row matches the inspected plan version.

    Args:
        existing: Locked current `ExecutionPlan` ORM row.
        candidate: Previewed settlement candidate.

    Returns:
        True when status, provenance, and filled quantity are unchanged.
    """
    return (
        existing.status,
        existing.session_id,
        int(existing.sequence_id),
        float(existing.filled_quantity),
    ) == (
        candidate.current_status,
        candidate.current_session_id,
        candidate.current_sequence_id,
        candidate.current_filled_quantity,
    )


def _build_settled_plan_version(
    existing: ExecutionPlan,
    candidate: PlanSettlementCandidate,
    *,
    settled_at: datetime,
    session_id: str,
    sequence_id: int,
) -> ExecutionPlan:
    """Construct the replacement SCD2 `ExecutionPlan` version for settlement.

    Args:
        existing: Locked current `ExecutionPlan` ORM row being closed.
        candidate: Validated settlement transition.
        settled_at: UTC settlement timestamp.
        session_id: Provenance session id for the new row.
        sequence_id: Provenance sequence id for the new row.

    Returns:
        New `ExecutionPlan` ORM row with terminal status and timestamps.
    """
    return ExecutionPlan(
        public_id=existing.public_id,
        plan_type=existing.plan_type,
        created_by_user_id=existing.created_by_user_id,
        created_by_strategy=existing.created_by_strategy,
        created_via=existing.created_via,
        instrument_public_id=existing.instrument_public_id,
        exchange=existing.exchange,
        mode=existing.mode,
        shard_key=existing.shard_key,
        wallet_public_id=existing.wallet_public_id,
        operator_public_id=existing.operator_public_id,
        total_quantity=existing.total_quantity,
        filled_quantity=(
            candidate.filled_quantity
            if candidate.filled_quantity is not None
            else existing.filled_quantity
        ),
        side=existing.side,
        parent_plan_public_id=existing.parent_plan_public_id,
        position_cycle_public_id=existing.position_cycle_public_id,
        params=existing.params,
        status=candidate.target_status,
        created_at=existing.created_at,
        started_at=existing.started_at,
        completed_at=settled_at,
        expires_at=existing.expires_at,
        cancel_requested_at=existing.cancel_requested_at,
        last_evaluated_at=settled_at,
        last_error=candidate.last_error,
        idempotency_key=existing.idempotency_key,
        cancel_idempotency_key=existing.cancel_idempotency_key,
        session_id=session_id,
        sequence_id=sequence_id,
        timestamp=settled_at,
    )


async def settle_single_recovered_plan(
    repository: SQLAlchemyRepository,
    candidate: PlanSettlementCandidate,
    *,
    settled_at: datetime,
    session_id: str,
    sequence_id: int,
) -> bool:
    """Atomically SCD2-settle one plan during startup recovery if unchanged.

    Re-reads the active `ExecutionPlan` version under write serialization and
    aborts without writing if its status or version provenance changed since
    inspection (for example, a concurrent REST cancel request).

    Args:
        repository: Target SQLAlchemy repository.
        candidate: Validated settlement candidate from startup inspection.
        settled_at: UTC timestamp for SCD2 closure and terminal metadata.
        session_id: Provenance session id of the plan executor.
        sequence_id: Provenance sequence id allocated by the plan executor.

    Returns:
        True when the plan was settled, False if it disappeared or drifted.
    """
    async with repository.session() as session:
        await _begin_plan_write_transaction(repository, session)
        existing = (
            (
                await session.execute(
                    select(ExecutionPlan)
                    .where(
                        ExecutionPlan.public_id == candidate.plan_public_id,
                        *where_active(ExecutionPlan, settled_at),
                    )
                    .with_for_update()
                )
            )
            .scalars()
            .first()
        )
        if existing is None or not _matches_candidate_version(existing, candidate):
            return False
        update_result = await session.execute(
            update(ExecutionPlan)
            .where(
                ExecutionPlan.id == int(existing.id),
                *where_active(ExecutionPlan, settled_at),
            )
            .values(known_to=settled_at)
        )
        closed = cast(CursorResult[tuple[()]], update_result).rowcount
        if closed != 1:
            return False
        session.add(
            _build_settled_plan_version(
                existing,
                candidate,
                settled_at=settled_at,
                session_id=session_id,
                sequence_id=sequence_id,
            )
        )
        await session.commit()
        return True


async def apply_plan_settlements(
    repository: SQLAlchemyRepository,
    preview: PlanSettlementPreview,
    *,
    settled_at: datetime,
    session_id: str,
) -> PlanSettlementReceipt:
    """Revalidate and atomically SCD2-settle all candidates in a preview.

    Recomputes the full preview first to catch child-command or venue-event
    drift, then locks every target `ExecutionPlan` row in one transaction,
    verifies exact version identity, closes the active versions at `settled_at`,
    and inserts their terminal replacements.

    Args:
        repository: Target SQLAlchemy repository.
        preview: Previously inspected preview whose digest was confirmed.
        settled_at: UTC timestamp written to `known_to` and `completed_at`.
        session_id: Provenance session id stamped onto new SCD2 versions.

    Returns:
        `PlanSettlementReceipt` summarizing the committed SCD2 transitions.

    Raises:
        PlanSettlementDriftError: If the recomputed preview digest or locked
            plan versions differ from `preview`, or the closed count mismatches.
    """
    current = await preview_plan_settlements(repository, mode=preview.mode)
    if current.digest != preview.digest:
        raise PlanSettlementDriftError(
            "settlement preview drifted before apply; nothing was written: "
            f"expected={preview.digest} current={current.digest}"
        )
    if not preview.candidates:
        return PlanSettlementReceipt(
            mode=preview.mode,
            digest=preview.digest,
            settled_public_ids=(),
            settled_at=settled_at,
            session_id=session_id,
            count=0,
        )
    expected_ids = [item.plan_public_id for item in preview.candidates]
    async with repository.session() as session:
        await _begin_plan_write_transaction(repository, session)
        locked_rows = (
            (
                await session.execute(
                    select(ExecutionPlan)
                    .where(
                        ExecutionPlan.public_id.in_(expected_ids),
                        *where_active(ExecutionPlan, settled_at),
                    )
                    .order_by(ExecutionPlan.created_at, ExecutionPlan.public_id)
                    .with_for_update()
                )
            )
            .scalars()
            .all()
        )
        locked_by_id = {str(row.public_id): row for row in locked_rows}
        if len(locked_by_id) != len(preview.candidates) or any(
            item.plan_public_id not in locked_by_id
            or not _matches_candidate_version(locked_by_id[item.plan_public_id], item)
            for item in preview.candidates
        ):
            raise PlanSettlementDriftError(
                "execution plan versions changed concurrently; nothing was written"
            )
        physical_ids = [int(locked_by_id[item.plan_public_id].id) for item in preview.candidates]
        update_result = await session.execute(
            update(ExecutionPlan)
            .where(
                ExecutionPlan.id.in_(physical_ids),
                *where_active(ExecutionPlan, settled_at),
            )
            .values(known_to=settled_at)
        )
        closed = cast(CursorResult[tuple[()]], update_result).rowcount
        if closed != len(preview.candidates):
            raise PlanSettlementDriftError(
                "execution plan close count mismatch; transaction rolled back: "
                f"previewed={len(preview.candidates)} closed={closed}"
            )
        for sequence_id, candidate in enumerate(preview.candidates, start=1):
            session.add(
                _build_settled_plan_version(
                    locked_by_id[candidate.plan_public_id],
                    candidate,
                    settled_at=settled_at,
                    session_id=session_id,
                    sequence_id=sequence_id,
                )
            )
        await session.commit()
    return PlanSettlementReceipt(
        mode=preview.mode,
        digest=preview.digest,
        settled_public_ids=tuple(expected_ids),
        settled_at=settled_at,
        session_id=session_id,
        count=len(preview.candidates),
    )
