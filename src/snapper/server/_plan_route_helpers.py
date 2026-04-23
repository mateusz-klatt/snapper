"""Shared helpers for execution-plan REST routes."""

import datetime as dt
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import cast
from uuid import uuid7

from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.orders import ExecutionPlanResponse
from snapper.application.plans.service import PlanExecutorService
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import PositionRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionPlanData
from snapper.server.scoping import resolve_target_wallets


@dataclass(frozen=True)
class PlanRouteContext:
    """Common request-scoped timestamps and sequence-tracking state."""

    tracker: SequenceTracker
    stream: str
    now: datetime
    bus_time: datetime
    session_id: str


@dataclass(frozen=True)
class CycleTradingContext:
    """Resolved cycle and live-position context for plan creation."""

    cycle: PositionCycleRow
    positions: list[PositionRow]
    native_instrument: str
    total_quantity: float
    side: str


@dataclass(frozen=True)
class CancelPlanState:
    """Derived cancellation state for a live execution plan."""

    params: JsonObject
    child_ids: tuple[str, ...]
    has_active_children: bool
    new_status: str
    native_instrument: str | None


def build_plan_route_context(request: Request, stream: str) -> PlanRouteContext:
    """Build shared per-request timing and sequencing context.

    Args:
        request: FastAPI request carrying the REST tracker in app state.
        stream: Logical stream name used for REST sequencing.

    Returns:
        Request-scoped timing and sequencing metadata for one route call.
    """
    tracker = cast(SequenceTracker, request.app.state.rest_tracker)
    now = datetime.now(UTC)
    return PlanRouteContext(
        tracker=tracker,
        stream=stream,
        now=now,
        bus_time=dt.datetime.now(dt.UTC),
        session_id=tracker.session_id,
    )


def _native_instrument_from_shard(shard_key: str) -> str:
    """Extract the native instrument symbol from a shard key."""
    return shard_key.split(".")[1]


def find_matching_position(
    positions: list[PositionRow],
    cycle: PositionCycleRow,
) -> PositionRow | None:
    """Return the live position row matching the cycle shard.

    Args:
        positions: Open positions visible to the wallet at the route timestamp.
        cycle: Position cycle being used to create or inspect a plan.

    Returns:
        The matching live position row, or ``None`` when no position matches.
    """
    native_instrument = _native_instrument_from_shard(cycle["shard_key"])
    for position in positions:
        if (
            position["exchange"] == cycle["exchange"]
            and position["mode"] == cycle["mode"]
            and position["instrument"] == native_instrument
        ):
            return position
    return None


def resolve_average_price(
    positions: list[PositionRow],
    cycle: PositionCycleRow,
) -> float | None:
    """Return a positive average entry price for the matching position.

    Args:
        positions: Open positions visible to the wallet at the route timestamp.
        cycle: Position cycle being used to create or inspect a plan.

    Returns:
        A positive average entry price, or ``None`` when it cannot be resolved.
    """
    position = find_matching_position(positions, cycle)
    if position is None:
        return None
    average_price = cast(float | None, position.get("average_price"))
    if average_price is None:
        return None
    average_value = float(average_price)
    if average_value <= 0:
        return None
    return average_value


def _resolve_total_quantity(
    positions: list[PositionRow],
    cycle: PositionCycleRow,
) -> float:
    """Prefer live position size, then fall back to cycle max_qty."""
    position = find_matching_position(positions, cycle)
    if position is not None:
        return abs(position["quantity"])
    return cycle["max_qty"]


async def load_open_accessible_cycle(
    *,
    repo: Repository,
    principal: AuthPrincipal,
    route_context: PlanRouteContext,
    position_cycle_public_id: str,
) -> PositionCycleRow:
    """Load an open, wallet-accessible position cycle.

    Args:
        repo: Repository used for cycle lookup.
        principal: Authenticated caller making the REST request.
        route_context: Request-scoped timing and sequencing metadata.
        position_cycle_public_id: Public id of the target position cycle.

    Returns:
        Open position-cycle row visible to the caller.
    """
    cycle = await repo.get_position_cycle_by_public_id(
        position_cycle_public_id,
        as_of=route_context.now,
    )
    if cycle is None or cycle["status"] != "open":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Position cycle not found or not open",
        )
    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=cycle["wallet_public_id"],
        operator_public_id=cycle["operator_public_id"],
    )
    return cycle


async def load_cycle_trading_context(
    *,
    service: PlanExecutorService,
    repo: Repository,
    route_context: PlanRouteContext,
    cycle: PositionCycleRow,
    capability_name: str,
) -> CycleTradingContext:
    """Load an open, wallet-accessible cycle and current position state.

    Args:
        service: Plan executor used to validate venue capabilities.
        repo: Repository used for cycle and position lookups.
        route_context: Request-scoped timing and sequencing metadata.
        cycle: Open, wallet-accessible cycle resolved by the route.
        capability_name: Capability bundle required for the plan type.

    Returns:
        Resolved cycle, position, and side context for plan creation.
    """
    missing = await service._check_capabilities(
        capability_name,
        cycle["exchange"],
        cycle["instrument_public_id"],
    )
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Venue missing required capabilities: {', '.join(missing)}",
        )
    positions = await repo.get_positions(
        as_of=route_context.now,
        wallet_public_ids=[cycle["wallet_public_id"]],
    )
    total_quantity = _resolve_total_quantity(positions, cycle)
    if total_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="No open position found for this cycle",
        )
    return CycleTradingContext(
        cycle=cycle,
        positions=positions,
        native_instrument=_native_instrument_from_shard(cycle["shard_key"]),
        total_quantity=total_quantity,
        side="buy" if cycle["direction"] == "long" else "sell",
    )


def plan_to_data(plan: ExecutionPlanRow) -> ExecutionPlanData:
    """Project a repository plan row into the response schema.

    Args:
        plan: Repository execution-plan row to serialize for the API response.

    Returns:
        Schema payload containing the plan fields exposed over REST.
    """
    return ExecutionPlanData(
        type="execution_plan",
        public_id=plan["public_id"],
        timestamp=plan["timestamp"],
        session_id=plan["session_id"],
        sequence_id=plan["sequence_id"],
        plan_type=plan["plan_type"],
        status=plan["status"],
        instrument_public_id=plan["instrument_public_id"],
        exchange=plan["exchange"],
        mode=plan["mode"],
        side=plan["side"],
        total_quantity=plan["total_quantity"],
        filled_quantity=plan["filled_quantity"],
        created_at=plan["created_at"],
        created_via=plan["created_via"],
        wallet_public_id=plan["wallet_public_id"],
        operator_public_id=plan["operator_public_id"],
        params=cast(dict[str, object], plan["params"]),
        position_cycle_public_id=plan.get("position_cycle_public_id"),
        parent_plan_public_id=plan.get("parent_plan_public_id"),
        last_error=plan["last_error"],
        idempotency_key=plan["idempotency_key"],
    )


def build_execution_plan_response(
    plan: ExecutionPlanRow,
    route_context: PlanRouteContext,
) -> ExecutionPlanResponse:
    """Build a standard REST response wrapping an execution plan.

    Args:
        plan: Repository execution-plan row to expose.
        route_context: Request-scoped timing and sequencing metadata.

    Returns:
        REST response envelope containing the serialized plan payload.
    """
    return ExecutionPlanResponse(
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(route_context.stream),
        public_id=str(uuid7()),
        timestamp=route_context.bus_time,
        payload=plan_to_data(plan),
    )


async def insert_execution_plan_or_raise(
    *,
    repo: Repository,
    row: ExecutionPlanInsertRow,
    duplicate_detail: str,
    failure_detail: str,
    failure_log: str,
) -> str:
    """Insert a plan row and map duplicate or generic failures to HTTP errors.

    Args:
        repo: Repository used to persist the execution plan.
        row: Insert row to write.
        duplicate_detail: HTTP detail to use for duplicate-key conflicts.
        failure_detail: HTTP detail to use for generic write failures.
        failure_log: Log message prefix for generic write failures.

    Returns:
        Public id of the inserted execution plan.
    """
    try:
        _plan_id, plan_public_id = await repo.insert_execution_plan(row)
        return plan_public_id
    except Exception as exc:
        err_str = str(exc).lower()
        if "unique" in err_str or "duplicate" in err_str:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=duplicate_detail,
            ) from exc
        logger.error("{}: {}", failure_log, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=failure_detail,
        ) from exc


async def get_execution_plan_or_500(
    *,
    repo: Repository,
    plan_public_id: str,
    as_of: datetime,
    detail: str,
) -> ExecutionPlanRow:
    """Load a plan after a successful write, failing hard if missing.

    Args:
        repo: Repository used to load the execution plan.
        plan_public_id: Public id of the plan to reload.
        as_of: Temporal snapshot used for the lookup.
        detail: HTTP detail to raise when the plan cannot be reloaded.

    Returns:
        The reloaded execution-plan row.
    """
    plan = await repo.get_execution_plan(plan_public_id, as_of=as_of)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=detail,
        )
    return plan


async def insert_execution_plan_decision(
    *,
    repo: Repository,
    route_context: PlanRouteContext,
    row: ExecutionPlanDecisionInsertRow,
) -> None:
    """Insert an execution-plan decision stamped with REST provenance.

    Args:
        repo: Repository used to persist the decision row.
        route_context: Request-scoped timing and sequencing metadata.
        row: Decision row to insert.
    """
    await repo.insert_execution_plan_decision(
        row=row,
        bus_time=route_context.bus_time,
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(route_context.stream),
    )


async def insert_execution_plan_decision_best_effort(
    *,
    repo: Repository,
    route_context: PlanRouteContext,
    row: ExecutionPlanDecisionInsertRow,
    failure_log: str,
) -> None:
    """Insert a decision row, logging failures without failing the request.

    Args:
        repo: Repository used to persist the decision row.
        route_context: Request-scoped timing and sequencing metadata.
        row: Decision row to insert.
        failure_log: Log message prefix used when the insert fails.
    """
    try:
        await insert_execution_plan_decision(
            repo=repo,
            route_context=route_context,
            row=row,
        )
    except Exception as exc:
        logger.error("{} {}: {}", failure_log, row["plan_public_id"], exc)


async def load_accessible_execution_plan(
    *,
    repo: Repository,
    principal: AuthPrincipal,
    plan_public_id: str,
    as_of: datetime,
    not_found_detail: str,
    allowed_plan_type: str | None = None,
) -> ExecutionPlanRow:
    """Load a plan by id, validate type when required, and enforce wallet access.

    Args:
        repo: Repository used to load the execution plan.
        principal: Authenticated caller making the REST request.
        plan_public_id: Public id of the plan to load.
        as_of: Temporal snapshot used for the lookup.
        not_found_detail: HTTP detail to use when the plan is absent or invalid.
        allowed_plan_type: Optional plan type the loaded plan must match.

    Returns:
        Wallet-accessible execution-plan row matching the request.
    """
    plan = await repo.get_execution_plan(plan_public_id, as_of=as_of)
    if plan is None or (allowed_plan_type is not None and plan["plan_type"] != allowed_plan_type):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=not_found_detail,
        )
    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=plan["wallet_public_id"],
        operator_public_id=plan.get("operator_public_id"),
    )
    return plan


def ensure_plan_not_terminal(
    *,
    plan: ExecutionPlanRow,
    terminal_statuses: frozenset[str],
) -> None:
    """Reject cancellation of already-terminal plans.

    Args:
        plan: Execution plan being cancelled.
        terminal_statuses: Status set treated as terminal for the route.
    """
    if plan["status"] in terminal_statuses:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Plan already in terminal status: {plan['status']}",
        )


def build_cancel_plan_state(
    *,
    service: PlanExecutorService,
    plan: ExecutionPlanRow,
) -> CancelPlanState:
    """Derive child-order and status-transition state for cancellation.

    Args:
        service: Plan executor used to inspect tracked child orders.
        plan: Execution plan being cancelled.

    Returns:
        Derived cancellation state used by the cancel flow.
    """
    params = plan["params"]
    child_ids = tuple(service._extract_child_ids(cast(dict[str, object], params)))
    has_active_children = len(child_ids) > 0
    native_instrument_value = params.get("native_instrument")
    native_instrument = (
        native_instrument_value if isinstance(native_instrument_value, str) else None
    )
    return CancelPlanState(
        params=params,
        child_ids=child_ids,
        has_active_children=has_active_children,
        new_status=(
            ExecutionPlanStatusEnum.CANCEL_REQUESTED
            if has_active_children
            else ExecutionPlanStatusEnum.CANCELLED
        ),
        native_instrument=native_instrument,
    )


async def request_plan_status_transition(
    *,
    repo: Repository,
    route_context: PlanRouteContext,
    plan_public_id: str,
    new_status: str,
    completed_at: datetime | None,
) -> None:
    """Transition a plan status, rejecting concurrent updates.

    Args:
        repo: Repository used to update the execution plan.
        route_context: Request-scoped timing and sequencing metadata.
        plan_public_id: Public id of the plan being updated.
        new_status: Status requested by the route.
        completed_at: Completion timestamp for terminal transitions, if any.
    """
    new_id = await repo.update_execution_plan_status(
        public_id=plan_public_id,
        new_status=new_status,
        bus_time=route_context.bus_time,
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(route_context.stream),
        cancel_requested_at=route_context.now,
        completed_at=completed_at,
    )
    if new_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Plan status changed concurrently",
        )


def build_cancel_submission(
    *,
    principal: AuthPrincipal,
    plan: ExecutionPlanRow,
    params: JsonObject,
) -> TradeCommandSubmission:
    """Build the cap-enforcer submission for a cancel action.

    Args:
        principal: Authenticated caller requesting cancellation.
        plan: Execution plan being cancelled.
        params: Persisted plan params used to infer venue order metadata.

    Returns:
        Caps-enforcer submission representing the cancel action.
    """
    return TradeCommandSubmission(
        user_public_id=principal.user_public_id,
        operator_public_id=plan.get("operator_public_id"),
        wallet_public_id=plan["wallet_public_id"],
        instrument_public_id=plan.get("instrument_public_id"),
        command_type="cancel",
        side=plan["side"],
        order_type=str(params.get("venue_order_type", "market")),
        quantity=None,
        price=None,
        source_surface="rest",
        idempotency_key=None,
    )


async def _lookup_exchange_order_id(
    *,
    repo: Repository,
    route_context: PlanRouteContext,
    plan_public_id: str,
    child_client_order_id: str,
) -> str | None:
    """Resolve the exchange order id for a child order, logging failures."""
    try:
        return await repo.get_exchange_order_id_for_client_order_id(
            child_client_order_id,
            as_of=route_context.now,
        )
    except Exception as exc:
        logger.error("Cancel venue lookup failed for {}: {}", plan_public_id, exc)
        return None


def _build_cancel_trade_command(
    *,
    principal: AuthPrincipal,
    plan: ExecutionPlanRow,
    route_context: PlanRouteContext,
    params: JsonObject,
    child_client_order_id: str,
    native_instrument: str,
    exchange_order_id: str | None,
) -> TradeCommandInsertRow:
    """Build a venue-facing cancel TradeCommand for one child order."""
    return TradeCommandInsertRow(
        command_type="cancel",
        shard_key=plan["shard_key"],
        exchange=plan["exchange"],
        instrument=native_instrument,
        mode=plan["mode"],
        strategy_id=plan["plan_type"],
        client_order_id=child_client_order_id,
        venue_client_id=child_client_order_id,
        side=plan["side"],
        order_type=str(params.get("venue_order_type", "market")),
        quantity=plan["total_quantity"],
        price=cast(float | None, params.get("price")),
        leverage=cast(int | None, params.get("leverage")),
        reduce_only=False,
        status=TradeCommandStatusEnum.CREATED,
        created_at=route_context.now,
        correlation_id=plan["public_id"],
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(route_context.stream),
        timestamp=route_context.bus_time,
        wallet_public_id=plan["wallet_public_id"],
        operator_public_id=plan.get("operator_public_id"),
        user_public_id=principal.user_public_id or principal.username,
        plan_public_id=plan["public_id"],
        exchange_order_id=exchange_order_id,
    )


async def prepare_cancel_trade_commands(
    *,
    repo: Repository,
    route_context: PlanRouteContext,
    principal: AuthPrincipal,
    plan: ExecutionPlanRow,
    cancel_state: CancelPlanState,
) -> list[tuple[str, TradeCommandInsertRow]]:
    """Build cancel TradeCommands for every active child order.

    Args:
        repo: Repository used for venue-order-id lookups.
        route_context: Request-scoped timing and sequencing metadata.
        principal: Authenticated caller requesting cancellation.
        plan: Execution plan being cancelled.
        cancel_state: Derived cancellation state for the plan.

    Returns:
        Prepared ``(child_client_order_id, cancel_command)`` tuples ready to insert.
    """
    if cancel_state.native_instrument is None:
        return []
    commands: list[tuple[str, TradeCommandInsertRow]] = []
    for child_client_order_id in cancel_state.child_ids:
        exchange_order_id = await _lookup_exchange_order_id(
            repo=repo,
            route_context=route_context,
            plan_public_id=plan["public_id"],
            child_client_order_id=child_client_order_id,
        )
        cancel_command = _build_cancel_trade_command(
            principal=principal,
            plan=plan,
            route_context=route_context,
            params=cancel_state.params,
            child_client_order_id=child_client_order_id,
            native_instrument=cancel_state.native_instrument,
            exchange_order_id=exchange_order_id,
        )
        commands.append((child_client_order_id, cancel_command))
    return commands


def caps_violation_detail(exc: CapsViolationError) -> JsonObject:
    """Project a caps violation into the public HTTP detail shape.

    Args:
        exc: Raised caps violation from the enforcer guard.

    Returns:
        HTTP detail payload exposed by REST routes.
    """
    return {
        "error_code": "caps_violation",
        "cap_type": exc.cap_type,
        "attempted": exc.attempted,
        "limit": exc.limit,
    }


async def handle_cancel_command_insert_failure(
    *,
    repo: Repository,
    route_context: PlanRouteContext,
    plan: ExecutionPlanRow,
    child_client_order_id: str,
    exc: Exception,
) -> None:
    """Compensate a failed cancel-command insert and raise HTTP 500.

    Args:
        repo: Repository used to compensate the plan status.
        route_context: Request-scoped timing and sequencing metadata.
        plan: Execution plan whose child cancel insert failed.
        child_client_order_id: Client order id of the child being cancelled.
        exc: Original insert exception.
    """
    logger.error(
        "Failed to insert cancel command for plan {} child {}: {}",
        plan["public_id"],
        child_client_order_id,
        exc,
    )
    try:
        await repo.update_execution_plan_status(
            public_id=plan["public_id"],
            new_status=ExecutionPlanStatusEnum.FAILED,
            bus_time=route_context.bus_time,
            session_id=route_context.session_id,
            sequence_id=route_context.tracker.next_sequence(route_context.stream),
            last_error=f"Cancel command insert failed: {exc}",
            completed_at=route_context.now,
        )
    except Exception as compensation_exc:
        logger.error(
            "Compensation to failed also failed for plan {}: {}",
            plan["public_id"],
            compensation_exc,
        )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to emit cancel command",
    ) from exc
