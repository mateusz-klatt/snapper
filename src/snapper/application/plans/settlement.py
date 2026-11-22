"""Deterministic terminal settlement for execution plans on startup and CLI repair.

Execution plans whose single child create command reached a durable terminal
state while the plan executor missed the live bus transition remain stuck in an
actionable status (`active` or `cancel_requested`). Both startup recovery in
`PlanExecutorService` (when `PLAN_RECOVERY_AUTO_SETTLE_ENABLED=true`) and the
operator `settle-plans` command share the same evidence inspection, digest
binding, and locked SCD2 status-transition builder defined here:

- Only registered single-order plan types (`manual_once`, `bracket`,
  `trailing_stop`) are eligible for child-terminal settlement.
- Every child client order id recorded in `plan["params"]` must resolve to an
  active `create` or `submit` command on the plan's exchange whose
  `plan_public_id` matches `plan["public_id"]`.
- `rejected` children settle to `failed` only when no `order_accepted` or
  `fill_observed` venue event carries an id higher than the latest
  `order_rejected` event, preserving reconciliation resurrection of false
  venue rejections.
- `failed` children settle to `failed` (in both `active` and
  `cancel_requested` plans) only when every lifecycle event is a pre-venue
  `order_breaker_open` or `order_interlock_blocked` refusal. For
  `cancel_requested` plans, both the child command and active Order must also
  lack a venue order id.
- Plans in `cancel_requested` settle only when durable evidence confirms no
  live venue order remains (venue-evidenced `cancelled`, `filled`, or
  `expired` via `target_end_time`, pre-dispatch outbox TTL expiry without live
  venue events, or pre-venue refusal without live venue events), preserving
  Item G2b stranded-cancel re-emission for unconfirmed targets.
- Write transactions lock child `TradeCommand` rows and target `ExecutionPlan`
  rows `FOR UPDATE` (`BEGIN IMMEDIATE` on SQLite) and re-read child status and
  venue lifecycle/fill evidence on that same session before committing.
"""

import hashlib
import json
import sys
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import ModuleType
from typing import Final
from typing import Protocol
from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy import update
from sqlalchemy.engine import URL
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade import cancel_target as cancel_target_module
from snapper.application.trade.cancel_target import VenueEventTimeReader
from snapper.application.trade.cancel_target import target_end_time
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data import plan_settlement_reads as plan_settlement_reads_module
from snapper.data.models import ExecutionPlan
from snapper.data.plan_settlement_reads import SessionPlanSettlementReader
from snapper.data.plan_settlement_reads import begin_plan_settlement_transaction
from snapper.data.plan_settlement_reads import lock_active_execution_plans
from snapper.data.plan_settlement_reads import lock_child_create_commands
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

POSTGRESQL_ALLOWED_URL_OPTIONS: Final[frozenset[str]] = frozenset({"ssl"})
"""Query options permitted on a PostgreSQL settlement target URL."""

RUN_ORDER_REQUIREMENT_NOTE: Final[str] = (
    "run with plan_executor stopped or restart plan_executor immediately after apply"
)
"""Operational run-order requirement recorded in CLI previews and receipts."""

RESIDUAL_VENUE_EVENT_WINDOW_NOTE: Final[str] = (
    "append-only venue_events writers do not lock trade_commands; "
    "TradeCommand and ExecutionPlan FOR UPDATE plus post-lock evidence re-reads "
    "fence command/plan mutations and all previously committed venue_events, "
    "while CLI apply requires plan_executor stopped or restarted immediately"
)
"""Explicit disclosure of the hot-path lock boundary on PostgreSQL."""

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

_FAILED_REFUSAL_EVENT_BY_REASON: Final[dict[str, str]] = {
    "order_interlock_blocked": "order_interlock_blocked",
    "circuit_breaker_open": "order_breaker_open",
}

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

    async def get_exchange_order_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Return the active Order's venue id for a child client order id.

        Args:
            client_order_id: Child client order id recorded on the plan.
            as_of: Temporal point for active-version selection.

        Returns:
            Venue order id, or None when no acknowledged Order version exists.
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


class PlanSettlementConfigError(ValueError):
    """Raised when a target database URL cannot be safely bound to a digest."""


class PlanSettlementDriftError(ValueError):
    """Raised when durable plan or child evidence differs from a preview."""


@dataclass(frozen=True, slots=True)
class PlanChildOutcome:
    """Resolved terminal evidence for a single child create command.

    Attributes:
        client_order_id: Child client order identifier.
        command_public_id: Public id of the child `trade_commands` row.
        command_status: Durable terminal status of the child command.
        terminal_venue_event_id: Qualifying terminal or refusal `venue_events.id`,
            or None for pre-dispatch outbox expiry or local cancel.
        fill_venue_event_id: `venue_events.id` of the max cumulative fill row,
            or None when no cumulative fill observation exists.
        cumulative_fill_size: Maximum observed cumulative fill quantity.
        last_error: Optional error message recorded on the child command.
    """

    client_order_id: str
    command_public_id: str
    command_status: str
    terminal_venue_event_id: int | None
    fill_venue_event_id: int | None
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
        target_database: Credential-free database endpoint bound into `digest`.
        code_revision: Per-module SHA-256 map bound into `digest`.
        candidates: Ordered settlement candidates ready to apply.
        skipped: Ordered actionable plans excluded from settlement.
        digest: Lowercase 64-hex SHA-256 digest binding scope, endpoint,
            code revision, and candidates.
    """

    mode: str | None
    target_database: JsonObject | None
    code_revision: JsonObject | None
    candidates: tuple[PlanSettlementCandidate, ...]
    skipped: tuple[PlanSettlementSkipped, ...]
    digest: str


@dataclass(frozen=True, slots=True)
class PlanSettlementReceipt:
    """Durable write receipt returned after applying a settlement preview.

    Attributes:
        mode: Optional execution mode filter bound to the preview.
        target_database: Credential-free database endpoint bound to the preview.
        code_revision: Per-module SHA-256 map bound to the preview.
        digest: Verified SHA-256 preview digest.
        settled_public_ids: Public ids settled in transaction order.
        settled_candidates: Exact candidate records and child evidence ids applied.
        settled_at: UTC timestamp written to `known_to` and `completed_at`.
        session_id: Provenance session id stamped on the new SCD2 versions.
        count: Number of execution plan versions settled.
    """

    mode: str | None
    target_database: JsonObject | None
    code_revision: JsonObject | None
    digest: str
    settled_public_ids: tuple[str, ...]
    settled_candidates: tuple[PlanSettlementCandidate, ...]
    settled_at: datetime
    session_id: str
    count: int


def settlement_database_endpoint(db_url: str) -> JsonObject:
    """Resolve a credential-free database endpoint suitable for digest binding.

    Args:
        db_url: Configured SQLAlchemy database URL.

    Returns:
        Canonical endpoint dictionary without credentials.

    Raises:
        PlanSettlementConfigError: For unsupported dialects, in-memory/optioned
            SQLite URLs, or incomplete/optioned PostgreSQL URLs.
    """
    url = make_url(db_url)
    dialect = url.get_backend_name()
    if dialect == "sqlite":
        return _sqlite_endpoint(url)
    if dialect == "postgresql":
        return _postgresql_endpoint(url)
    raise PlanSettlementConfigError(f"database dialect {dialect} is not supported")


def _sqlite_endpoint(url: URL) -> JsonObject:
    """Validate and canonicalize a file-backed SQLite database endpoint."""
    if not url.database or url.database == ":memory:" or url.query:
        raise PlanSettlementConfigError(
            "a SQLite target must be a database file without query options"
        )
    return {"dialect": "sqlite", "path": str(Path(url.database).resolve())}


def _postgresql_endpoint(url: URL) -> JsonObject:
    """Validate and canonicalize an explicit PostgreSQL database endpoint."""
    if (
        not url.host
        or "," in url.host
        or url.port is None
        or not 1 <= url.port <= 65535
        or not url.database
    ):
        raise PlanSettlementConfigError(
            "a PostgreSQL target URL must name one host, a port from 1 to 65535 and its database"
        )
    disallowed = sorted(set(url.query) - POSTGRESQL_ALLOWED_URL_OPTIONS)
    if disallowed:
        raise PlanSettlementConfigError(
            f"PostgreSQL target URL options are not allowed: {', '.join(disallowed)}"
        )
    return {
        "dialect": "postgresql",
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def settlement_code_revision(extra_modules: Sequence[ModuleType] = ()) -> JsonObject:
    """Compute SHA-256 hashes of every module governing plan settlement.

    Args:
        extra_modules: Additional caller modules (such as the CLI entrypoint).

    Returns:
        Mapping of module relative path to lowercase 64-hex SHA-256 digest.
    """
    modules = (
        cancel_target_module,
        plan_settlement_reads_module,
        sys.modules[__name__],
        *extra_modules,
    )
    return {
        f"{module.__name__.replace('.', '/')}.py": hashlib.sha256(
            Path(cast(str, module.__file__)).read_bytes()
        ).hexdigest()
        for module in modules
    }


def extract_plan_child_ids(params: Mapping[str, object]) -> list[str]:
    """Extract deduplicated child client order ids in deterministic order.

    Reads both `child_client_order_ids` (list) and `child_client_order_id`
    (legacy scalar) while preserving first-seen order across restarts and
    digest previews.

    Args:
        params: Execution plan `params` mapping.

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
    """Return True when lifecycle history contains only local venue refusals.

    Args:
        events: Lifecycle venue events for the child client order id.

    Returns:
        True only when at least one breaker/interlock refusal exists and every
        lifecycle event is such a refusal.
    """
    return bool(events) and all(row["event_type"] in _PRE_VENUE_REFUSAL_EVENTS for row in events)


def _has_exact_failed_refusal_only(
    command: TradeCommandRow, events: Sequence[VenueEventRow]
) -> bool:
    """Match the stored failure reason to exclusive id-less local refusals.

    Args:
        command: Active failed create or submit command.
        events: Complete lifecycle history for the child.

    Returns:
        True only for the exact interlock/breaker reason and its sole marker type.
    """
    expected_event = _FAILED_REFUSAL_EVENT_BY_REASON.get(command.get("last_error") or "")
    return bool(expected_event and events) and all(
        row["event_type"] == expected_event and not row["exchange_order_id"] for row in events
    )


def _latest_matching_event_id(
    events: Sequence[VenueEventRow],
    event_types: frozenset[str],
    statuses: frozenset[str] | None = None,
) -> int | None:
    """Return the highest `venue_events.id` matching `event_types` and `statuses`.

    Args:
        events: Chronologically ordered lifecycle events for one child order.
        event_types: Qualifying `event_type` values.
        statuses: Optional lowercase `status` filter.

    Returns:
        Highest matching `venue_events.id`, or None when none match.
    """
    for row in reversed(events):
        if row["event_type"] not in event_types:
            continue
        if statuses is not None and (row.get("status") or "").lower() not in statuses:
            continue
        return row["id"]
    return None


def _resolve_terminal_venue_event_id(
    status: str,
    events: Sequence[VenueEventRow],
    fill_event_id: int | None,
) -> int | None:
    """Return the `venue_events.id` witnessing a child command's terminal state.

    Args:
        status: Validated terminal status of the child command.
        events: Lifecycle venue events for the child order.
        fill_event_id: Optional `venue_events.id` of the max cumulative fill row.

    Returns:
        Qualifying terminal or refusal event id, or None for local expiry/cancel.
    """
    if status == TradeCommandStatusEnum.FILLED:
        terminal_id = _latest_matching_event_id(
            events,
            frozenset({"order_terminal"}),
            frozenset({"filled", "closed"}),
        )
        return terminal_id if terminal_id is not None else fill_event_id
    if status == TradeCommandStatusEnum.CANCELLED:
        return _latest_matching_event_id(
            events,
            frozenset({"order_terminal"}),
            frozenset({"canceled", "cancelled"}),
        )
    if status == TradeCommandStatusEnum.EXPIRED:
        return _latest_matching_event_id(
            events,
            frozenset({"order_terminal"}),
            frozenset({"expired"}),
        )
    if status == TradeCommandStatusEnum.REJECTED:
        return _latest_matching_event_id(events, frozenset({"order_rejected"}))
    return _latest_matching_event_id(events, _PRE_VENUE_REFUSAL_EVENTS)


async def _cancel_requested_child_confirmed(
    reader: PlanSettlementReader,
    command: TradeCommandRow,
    events: Sequence[VenueEventRow],
) -> bool:
    """Return True when a `cancel_requested` plan's child is provably ended.

    Preserves Item G2b: a `cancel_requested` plan keeps re-emitting its cancel
    unless durable venue evidence (or pre-venue refusal / pre-dispatch outbox
    TTL expiry with zero live venue events) proves no live order remains.
    `FAILED` children are already restricted to `_has_pre_venue_refusal_only`
    by `_inspect_child_command` before reaching this check.

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
    return status == TradeCommandStatusEnum.FAILED


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
    if status == TradeCommandStatusEnum.FAILED:
        if plan["status"] == ExecutionPlanStatusEnum.CANCEL_REQUESTED:
            order_exchange_order_id = await reader.get_exchange_order_id_for_client_order_id(
                client_order_id, datetime.now(UTC)
            )
            unconfirmed = (
                not _has_exact_failed_refusal_only(command, events)
                or bool(command.get("exchange_order_id"))
                or bool(order_exchange_order_id)
            )
        else:
            unconfirmed = not _has_pre_venue_refusal_only(events)
        if unconfirmed:
            return _skipped(
                plan,
                "unconfirmed_failed_child",
                f"child {client_order_id} lacks exclusive pre-venue refusal evidence "
                "or still has a venue order id",
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
    fill_event_id = None if fill_row is None else fill_row["id"]
    raw_cum_fill = None if fill_row is None else fill_row["cum_fill_size"]
    cum_fill = raw_cum_fill if raw_cum_fill is not None and raw_cum_fill > 0.0 else 0.0
    terminal_event_id = _resolve_terminal_venue_event_id(status, events, fill_event_id)
    return PlanChildOutcome(
        client_order_id=client_order_id,
        command_public_id=command["public_id"],
        command_status=status,
        terminal_venue_event_id=terminal_event_id,
        fill_venue_event_id=fill_event_id,
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
        first_error = plan.get("last_error") or next(
            (item.last_error for item in outcomes if item.last_error), None
        )
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


def serialize_candidate(item: PlanSettlementCandidate) -> JsonObject:
    """Serialize one `PlanSettlementCandidate` to canonical JSON-compatible form.

    Args:
        item: Settlement candidate to serialize.

    Returns:
        JSON-serializable dictionary including child evidence identifiers.
    """
    return {
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
                "terminal_venue_event_id": child.terminal_venue_event_id,
                "fill_venue_event_id": child.fill_venue_event_id,
                "cumulative_fill_size": child.cumulative_fill_size,
                "last_error": child.last_error,
            }
            for child in item.children
        ],
    }


def _compute_preview_digest(
    mode: str | None,
    target_database: JsonObject | None,
    code_revision: JsonObject | None,
    candidates: Sequence[PlanSettlementCandidate],
) -> str:
    """Compute a canonical SHA-256 digest over scope, endpoint, code, and set.

    Args:
        mode: Optional execution mode filter (`live`, `paper`, or None).
        target_database: Credential-free database endpoint bound to the preview.
        code_revision: Per-module SHA-256 revision map bound to the preview.
        candidates: Ordered settlement candidates.

    Returns:
        Lowercase 64-character hexadecimal SHA-256 digest.
    """
    payload = {
        "mode": mode,
        "target_database": target_database,
        "code_revision": code_revision,
        "candidates": [serialize_candidate(item) for item in candidates],
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def preview_plan_settlements(
    reader: PlanSettlementReader,
    *,
    mode: str | None = None,
    target_database: JsonObject | None = None,
    code_revision: JsonObject | None = None,
) -> PlanSettlementPreview:
    """Build a deterministic read-only settlement preview and SHA-256 digest.

    Args:
        reader: Settlement repository reader.
        mode: Optional mode filter (`live` or `paper`); None inspects all modes.
        target_database: Optional credential-free database endpoint to bind.
        code_revision: Optional module SHA-256 revision map to bind.

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
    digest = _compute_preview_digest(mode, target_database, code_revision, candidates)
    return PlanSettlementPreview(
        mode=mode,
        target_database=target_database,
        code_revision=code_revision,
        candidates=tuple(candidates),
        skipped=tuple(skipped),
        digest=digest,
    )


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


def _verify_locked_plans(
    locked_by_id: Mapping[str, ExecutionPlan],
    candidates: Sequence[PlanSettlementCandidate],
) -> None:
    """Ensure every previewed candidate is locked and matches its SCD2 version.

    Args:
        locked_by_id: Locked open-ended `ExecutionPlan` rows keyed by `public_id`.
        candidates: Previewed settlement candidates.

    Raises:
        PlanSettlementDriftError: When any candidate is missing or drifted.
    """
    if len(locked_by_id) != len(candidates) or any(
        item.plan_public_id not in locked_by_id
        or not _matches_candidate_version(locked_by_id[item.plan_public_id], item)
        for item in candidates
    ):
        raise PlanSettlementDriftError(
            "execution plan versions changed concurrently; nothing was written"
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


async def _lock_and_reverify_preview(
    repository: SQLAlchemyRepository,
    session: AsyncSession,
    preview: PlanSettlementPreview,
) -> dict[str, ExecutionPlan]:
    """Lock child commands and plan rows, then recompute and verify `preview`.

    Args:
        repository: Target SQLAlchemy repository.
        session: Open transaction session.
        preview: Preview being applied.

    Returns:
        Mapping of `public_id` to locked `ExecutionPlan` ORM rows.

    Raises:
        PlanSettlementDriftError: If the recomputed digest or locked rows drift.
    """
    await begin_plan_settlement_transaction(session, dialect_name=repository.dialect_name)
    child_orders = [
        (item.exchange, child.client_order_id)
        for item in preview.candidates
        for child in item.children
    ]
    expected_ids = [item.plan_public_id for item in preview.candidates]
    await lock_child_create_commands(session, child_orders=child_orders)
    locked_by_id = await lock_active_execution_plans(session, plan_public_ids=expected_ids)
    session_reader = SessionPlanSettlementReader(repository, session)
    current = await preview_plan_settlements(
        session_reader,
        mode=preview.mode,
        target_database=preview.target_database,
        code_revision=preview.code_revision,
    )
    if current.digest != preview.digest:
        raise PlanSettlementDriftError(
            "settlement preview drifted before apply; nothing was written: "
            f"expected={preview.digest} current={current.digest}"
        )
    _verify_locked_plans(locked_by_id, preview.candidates)
    return locked_by_id


async def settle_single_recovered_plan(
    repository: SQLAlchemyRepository,
    candidate: PlanSettlementCandidate,
    *,
    settled_at: datetime,
    session_id: str,
    sequence_id: int,
) -> bool:
    """Atomically SCD2-settle one plan during startup recovery if unchanged.

    Locks the child `TradeCommand` rows and active `ExecutionPlan` row `FOR
    UPDATE` (`BEGIN IMMEDIATE` on SQLite) and re-runs `inspect_plan_settlement`
    on the bound session before writing, aborting if either the plan version or
    child/venue evidence changed since initial inspection. PostgreSQL uses
    SERIALIZABLE because process startup only provides a bounded async grace
    period, not proof that venue writers remain stopped until recovery ends.

    Args:
        repository: Target SQLAlchemy repository.
        candidate: Validated settlement candidate from startup inspection.
        settled_at: UTC timestamp for SCD2 closure and terminal metadata.
        session_id: Provenance session id of the plan executor.
        sequence_id: Provenance sequence id allocated by the plan executor.

    Returns:
        True when the plan was settled, False if it disappeared or drifted.
    """
    child_orders = [(candidate.exchange, child.client_order_id) for child in candidate.children]
    async with repository.session() as session:
        await begin_plan_settlement_transaction(
            session,
            dialect_name=repository.dialect_name,
            isolation_level="SERIALIZABLE",
        )
        await lock_child_create_commands(session, child_orders=child_orders)
        locked_plans = await lock_active_execution_plans(
            session,
            plan_public_ids=(candidate.plan_public_id,),
        )
        existing = locked_plans.get(candidate.plan_public_id)
        if existing is None or not _matches_candidate_version(existing, candidate):
            return False
        session_reader = SessionPlanSettlementReader(repository, session)
        reverified = await inspect_plan_settlement(
            session_reader,
            repository._plan_row_to_dict(existing),
        )
        if reverified != candidate:
            return False
        effective_settled_at = max(settled_at, existing.timestamp)
        update_result = await session.execute(
            update(ExecutionPlan)
            .where(
                ExecutionPlan.id == int(existing.id),
                *where_active(ExecutionPlan, effective_settled_at),
            )
            .values(known_to=effective_settled_at)
        )
        closed = cast(CursorResult[tuple[()]], update_result).rowcount
        if closed != 1:
            return False
        session.add(
            _build_settled_plan_version(
                existing,
                candidate,
                settled_at=effective_settled_at,
                session_id=session_id,
                sequence_id=sequence_id,
            )
        )
        await session.commit()
        return True


async def verify_plan_child_settlement_candidate(
    repository: SQLAlchemyRepository,
    candidate: PlanSettlementCandidate,
) -> bool:
    """Recheck one safe child under the plan and command locks without writes.

    Args:
        repository: Target SQLAlchemy repository.
        candidate: Single-child settlement candidate from preliminary inspection.

    Returns:
        True only when the locked plan version and all child evidence are unchanged.
    """
    if len(candidate.children) != 1:
        return False
    child = candidate.children[0]
    async with repository.session() as session:
        await begin_plan_settlement_transaction(session, dialect_name=repository.dialect_name)
        await lock_child_create_commands(
            session, child_orders=((candidate.exchange, child.client_order_id),)
        )
        locked_plans = await lock_active_execution_plans(
            session, plan_public_ids=(candidate.plan_public_id,)
        )
        existing = locked_plans.get(candidate.plan_public_id)
        if existing is None or not _matches_candidate_version(existing, candidate):
            return False
        locked_plan = repository._plan_row_to_dict(existing)
        params = dict(locked_plan.get("params") or {})
        params["child_client_order_id"] = child.client_order_id
        params["child_client_order_ids"] = [child.client_order_id]
        single_child_plan = cast(ExecutionPlanRow, {**locked_plan, "params": params})
        reverified = await inspect_plan_settlement(
            SessionPlanSettlementReader(repository, session), single_child_plan
        )
        return reverified == candidate


async def apply_plan_settlements(
    repository: SQLAlchemyRepository,
    preview: PlanSettlementPreview,
    *,
    settled_at: datetime,
    session_id: str,
) -> PlanSettlementReceipt:
    """Lock targets, revalidate evidence in-transaction, and SCD2-settle `preview`.

    Inside a single serialized transaction (`BEGIN IMMEDIATE` on SQLite;
    `READ COMMITTED` with `FOR UPDATE` on child `TradeCommand` and target
    `ExecutionPlan` rows on PostgreSQL), recomputes the full preview and child
    venue-event evidence, verifies exact digest and version identity, closes
    active versions, and inserts terminal replacements.

    Args:
        repository: Target SQLAlchemy repository.
        preview: Previously inspected preview whose digest was confirmed.
        settled_at: UTC timestamp floor written to `known_to` and `completed_at`.
        session_id: Provenance session id stamped onto new SCD2 versions.

    Returns:
        `PlanSettlementReceipt` summarizing the committed SCD2 transitions.

    Raises:
        PlanSettlementDriftError: If the in-transaction preview digest, child
            evidence, locked plan versions, or closed row count drift.
    """
    expected_ids = [item.plan_public_id for item in preview.candidates]
    async with repository.session() as session:
        locked_by_id = await _lock_and_reverify_preview(repository, session, preview)
        if not preview.candidates:
            await session.commit()
            return PlanSettlementReceipt(
                mode=preview.mode,
                target_database=preview.target_database,
                code_revision=preview.code_revision,
                digest=preview.digest,
                settled_public_ids=(),
                settled_candidates=(),
                settled_at=settled_at,
                session_id=session_id,
                count=0,
            )
        effective_settled_at = max(
            [settled_at, *(locked_by_id[pid].timestamp for pid in expected_ids)]
        )
        physical_ids = [int(locked_by_id[pid].id) for pid in expected_ids]
        update_result = await session.execute(
            update(ExecutionPlan)
            .where(
                ExecutionPlan.id.in_(physical_ids),
                *where_active(ExecutionPlan, effective_settled_at),
            )
            .values(known_to=effective_settled_at)
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
                    settled_at=effective_settled_at,
                    session_id=session_id,
                    sequence_id=sequence_id,
                )
            )
        await session.commit()
    return PlanSettlementReceipt(
        mode=preview.mode,
        target_database=preview.target_database,
        code_revision=preview.code_revision,
        digest=preview.digest,
        settled_public_ids=tuple(expected_ids),
        settled_candidates=preview.candidates,
        settled_at=effective_settled_at,
        session_id=session_id,
        count=len(preview.candidates),
    )
