"""Trailing stop routes for trailing stop execution plans.

Provides create, cancel, detail, decision-list, and by-cycle endpoints
for trailing stop execution plans. Trailing stops attach to open position
cycles and ratchet the stop price as the market moves favorably.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Literal
from typing import cast

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.api.schemas.orders import ExecutionPlanResponse
from snapper.api.schemas.trailing_stops import TrailingStopCancelCommand
from snapper.api.schemas.trailing_stops import TrailingStopCreateCommand
from snapper.application.plans.service import PlanExecutorService
from snapper.application.plans.trailing_stop import TrailingStopEvaluator
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import PositionRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server._capability_guard import require_tradable
from snapper.server._plan_route_helpers import PlanRouteContext
from snapper.server._plan_route_helpers import build_cancel_plan_state
from snapper.server._plan_route_helpers import build_cancel_submission
from snapper.server._plan_route_helpers import build_execution_plan_response
from snapper.server._plan_route_helpers import build_plan_route_context
from snapper.server._plan_route_helpers import caps_violation_detail
from snapper.server._plan_route_helpers import ensure_plan_not_terminal
from snapper.server._plan_route_helpers import get_execution_plan_or_500
from snapper.server._plan_route_helpers import handle_cancel_command_insert_failure
from snapper.server._plan_route_helpers import insert_execution_plan_decision
from snapper.server._plan_route_helpers import insert_execution_plan_decision_best_effort
from snapper.server._plan_route_helpers import insert_execution_plan_or_raise
from snapper.server._plan_route_helpers import load_accessible_execution_plan
from snapper.server._plan_route_helpers import load_cycle_trading_context
from snapper.server._plan_route_helpers import load_open_accessible_cycle
from snapper.server._plan_route_helpers import prepare_cancel_trade_commands
from snapper.server._plan_route_helpers import request_plan_status_transition
from snapper.server._plan_route_helpers import resolve_average_price
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.scoping import resolve_target_wallets

router = APIRouter(prefix="/trailing-stops", tags=["trailing-stops"])

_REST_STREAM = "trailing_stop_rest"
_EVALUATOR = TrailingStopEvaluator()
_TRAILING_STOP_NOT_FOUND = "Trailing stop plan not found"
_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        ExecutionPlanStatusEnum.COMPLETED,
        ExecutionPlanStatusEnum.CANCELLED,
        ExecutionPlanStatusEnum.FAILED,
        ExecutionPlanStatusEnum.EXPIRED,
    }
)


class TrailingStopStateData(StrictDataSchema[Literal["trailing_stop_state"]]):
    """Live trailing stop state from evaluator memory."""

    type: Literal["trailing_stop_state"] = "trailing_stop_state"
    plan_public_id: str
    status: str
    trailing_pct: float
    min_lock_pct: float
    entry_price: float
    peak_price: float
    current_stop: float
    side: str


class TrailingStopStateResponse(
    PayloadResponse[Literal["trailing_stop_state"], TrailingStopStateData],
):
    """Response wrapping live trailing stop state."""

    type: Literal["trailing_stop_state"] = "trailing_stop_state"


def _get_plan_executor(request: Request) -> PlanExecutorService:
    """Retrieve the PlanExecutorService from app state."""
    executor = getattr(request.app.state, "plan_executor", None)
    if executor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Plan executor not available (API-only mode)",
        )
    return cast(PlanExecutorService, executor)


def _resolve_average_price(
    positions: list[PositionRow],
    cycle: PositionCycleRow,
) -> float | None:
    """Find the average entry price for the position matching a cycle."""
    return resolve_average_price(positions, cycle)


def _validate_trailing_stop_command(command: TrailingStopCreateCommand) -> None:
    """Validate trailing-stop percentages with the evaluator contract."""
    try:
        _EVALUATOR.validate_params(
            {
                "trailing_pct": command.payload.trailing_pct,
                "min_lock_pct": command.payload.min_lock_pct,
                "entry_price": 1.0,
                "native_instrument": "placeholder",
            }
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc


def _resolve_entry_price(
    positions: list[PositionRow],
    cycle: PositionCycleRow,
) -> float:
    """Resolve a positive entry price for the cycle position."""
    average_price = _resolve_average_price(positions, cycle)
    if average_price is None or average_price <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Cannot determine entry price for this position",
        )
    return average_price


def _build_trailing_stop_plan_params(
    command: TrailingStopCreateCommand,
    cycle: PositionCycleRow,
    native_instrument: str,
    entry_price: float,
) -> JsonObject:
    """Build persisted trailing-stop params."""
    cycle_values = cast(dict[str, object], cycle)
    return {
        "native_instrument": native_instrument,
        "trailing_pct": command.payload.trailing_pct,
        "min_lock_pct": command.payload.min_lock_pct,
        "entry_price": entry_price,
        "leverage": cast(int | None, cycle_values.get("leverage")),
    }


def _build_trailing_stop_plan_row(
    *,
    command: TrailingStopCreateCommand,
    principal: AuthPrincipal,
    route_context: PlanRouteContext,
    cycle: PositionCycleRow,
    side: str,
    total_quantity: float,
    params: JsonObject,
) -> ExecutionPlanInsertRow:
    """Build the execution-plan insert row for a trailing stop."""
    return ExecutionPlanInsertRow(
        plan_type="trailing_stop",
        created_by_user_id=principal.user_public_id or principal.username,
        created_via="api",
        instrument_public_id=cycle["instrument_public_id"],
        exchange=cycle["exchange"],
        mode=cycle["mode"],
        shard_key=cycle["shard_key"],
        wallet_public_id=cycle["wallet_public_id"],
        operator_public_id=cycle["operator_public_id"],
        total_quantity=total_quantity,
        side=side,
        params=params,
        status=ExecutionPlanStatusEnum.ARMED,
        created_at=route_context.now,
        position_cycle_public_id=cycle["public_id"],
        idempotency_key=command.payload.idempotency_key,
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(route_context.stream),
        timestamp=route_context.bus_time,
    )


def _build_trailing_stop_created_decision(
    *,
    command: TrailingStopCreateCommand,
    plan_public_id: str,
    route_context: PlanRouteContext,
    entry_price: float,
) -> ExecutionPlanDecisionInsertRow:
    """Build the persisted decision row for trailing-stop creation."""
    evidence: JsonObject = {
        "trailing_pct": command.payload.trailing_pct,
        "min_lock_pct": command.payload.min_lock_pct,
        "entry_price": entry_price,
        "position_cycle_public_id": command.payload.position_cycle_public_id,
    }
    if command.payload.min_lock_pct > 0:
        evidence["warning"] = (
            "Trailing stop will not activate until price moves "
            f"{command.payload.min_lock_pct}% in your favor"
        )
    return ExecutionPlanDecisionInsertRow(
        plan_public_id=plan_public_id,
        decision_type="trailing_stop_created",
        decided_at=route_context.now,
        trigger_type="api",
        evidence=evidence,
        emitted_command_public_id=None,
        new_status=ExecutionPlanStatusEnum.ARMED,
        reason="Trailing stop created via API",
        decision_importance="action",
        source_surface="rest",
    )


def _build_trailing_stop_cancelled_decision(
    *,
    command: TrailingStopCancelCommand,
    plan_public_id: str,
    route_context: PlanRouteContext,
    has_active_children: bool,
    new_status: str,
) -> ExecutionPlanDecisionInsertRow:
    """Build the persisted decision row for trailing-stop cancellation."""
    return ExecutionPlanDecisionInsertRow(
        plan_public_id=plan_public_id,
        decision_type="trailing_stop_cancelled",
        decided_at=route_context.now,
        trigger_type="api",
        evidence={
            "reason": command.payload.reason,
            "had_children": has_active_children,
        },
        emitted_command_public_id=None,
        new_status=new_status,
        reason=command.payload.reason or "Cancelled via API",
        decision_importance="action",
        source_surface="rest",
    )


def _coerce_float_param(params: JsonObject, key: str) -> float:
    """Read a numeric JSON param as float, defaulting to 0.0."""
    value = params.get(key, 0)
    if isinstance(value, (int, float, str)):
        return float(value)
    return 0.0


@router.post("", openapi_extra=openapi_schema(TrailingStopCreateCommand))
async def create_trailing_stop(
    request: Request,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CREATE_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[
        TrailingStopCreateCommand,
        Depends(json_body(TrailingStopCreateCommand)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ExecutionPlanResponse:
    """Create a trailing stop execution plan on an open position cycle.

    Validates the cycle is open, the caller has wallet access, the venue
    supports reduce_only, and trailing_pct is within bounds. The trailing
    stop is created with status=armed and immediately starts watching ticks.

    Args:
        request: FastAPI request (provides REST tracker + app state).
        principal: Authenticated caller holding CREATE_ORDERS.
        _csrf: CSRF token validation.
        command: Trailing stop create command envelope.
        repo: Repository dependency.

    Returns:
        ExecutionPlanResponse wrapping the new armed trailing stop plan.

    Raises:
        HTTPException: 422 if params invalid or capability missing,
            409 if cycle not open or duplicate, 403 if wallet not
            accessible, 503 if executor unavailable.
    """
    service = _get_plan_executor(request)
    route_context = build_plan_route_context(request, _REST_STREAM)

    _validate_trailing_stop_command(command)
    cycle = await load_open_accessible_cycle(
        repo=repo,
        principal=principal,
        route_context=route_context,
        position_cycle_public_id=command.payload.position_cycle_public_id,
    )
    await require_tradable(
        repo,
        cycle["instrument_public_id"],
        cycle["exchange"],
        route_context.now,
    )
    cycle_context = await load_cycle_trading_context(
        service=service,
        repo=repo,
        route_context=route_context,
        cycle=cycle,
        capability_name="trailing_stop",
    )
    entry_price = _resolve_entry_price(
        cycle_context.positions,
        cycle_context.cycle,
    )

    plan_public_id = await insert_execution_plan_or_raise(
        repo=repo,
        row=_build_trailing_stop_plan_row(
            command=command,
            principal=principal,
            route_context=route_context,
            cycle=cycle_context.cycle,
            side=cycle_context.side,
            total_quantity=cycle_context.total_quantity,
            params=_build_trailing_stop_plan_params(
                command,
                cycle_context.cycle,
                cycle_context.native_instrument,
                entry_price,
            ),
        ),
        duplicate_detail="Duplicate trailing stop for this position cycle or idempotency key",
        failure_detail="Failed to create trailing stop",
        failure_log="Failed to create trailing stop plan",
    )
    plan = await get_execution_plan_or_500(
        repo=repo,
        plan_public_id=plan_public_id,
        as_of=route_context.bus_time,
        detail="Trailing stop created but not found",
    )
    service._register_plan(plan, TrailingStopEvaluator())
    await insert_execution_plan_decision_best_effort(
        repo=repo,
        route_context=route_context,
        row=_build_trailing_stop_created_decision(
            command=command,
            plan_public_id=plan_public_id,
            route_context=route_context,
            entry_price=entry_price,
        ),
        failure_log="Decision logging failed for trailing stop",
    )
    return build_execution_plan_response(
        plan,
        route_context,
    )


@router.post(
    "/{plan_public_id}/cancel",
    openapi_extra=openapi_schema(TrailingStopCancelCommand),
)
async def cancel_trailing_stop(
    request: Request,
    plan_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CANCEL_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[
        TrailingStopCancelCommand,
        Depends(json_body(TrailingStopCancelCommand)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    caps_enforcer: Annotated[TradingCapsEnforcer, Depends(get_caps_enforcer_dependency)],
) -> ExecutionPlanResponse:
    """Cancel a trailing stop execution plan.

    Armed trailing stops (no child orders yet) transition directly to cancelled.
    Active trailing stops (child orders in-flight) transition to cancel_requested
    and emit cancel TradeCommands.

    Args:
        request: FastAPI request.
        plan_public_id: Trailing stop plan to cancel.
        principal: Authenticated caller holding CANCEL_ORDERS.
        _csrf: CSRF token validation.
        command: Cancel command envelope.
        repo: Repository dependency.
        caps_enforcer: Per-user :class:`TradingCapsEnforcer` used to
            gate the cancel TradeCommand insert against
            caps (cancel rate limit).

    Returns:
        ExecutionPlanResponse wrapping the updated plan.

    Raises:
        HTTPException: 404 if not found, 409 if already terminal
            403 if wallet not accessible, 503 if executor unavailable.
    """
    service = _get_plan_executor(request)
    route_context = build_plan_route_context(request, _REST_STREAM)

    plan = await load_accessible_execution_plan(
        repo=repo,
        principal=principal,
        plan_public_id=plan_public_id,
        as_of=route_context.now,
        not_found_detail=_TRAILING_STOP_NOT_FOUND,
        allowed_plan_type="trailing_stop",
    )
    ensure_plan_not_terminal(
        plan=plan,
        terminal_statuses=_TERMINAL_STATUSES,
    )
    cancel_state = build_cancel_plan_state(service=service, plan=plan)
    if cancel_state.has_active_children:
        cancel_commands = await prepare_cancel_trade_commands(
            repo=repo,
            route_context=route_context,
            principal=principal,
            plan=plan,
            cancel_state=cancel_state,
        )
        submission = build_cancel_submission(
            principal=principal,
            plan=plan,
            params=cancel_state.params,
        )
        try:
            async with caps_enforcer.guard(submission):
                await request_plan_status_transition(
                    repo=repo,
                    route_context=route_context,
                    plan_public_id=plan_public_id,
                    new_status=cancel_state.new_status,
                    completed_at=None,
                )
                for child_client_order_id, cancel_command in cancel_commands:
                    try:
                        await repo.insert_trade_command(cancel_command, ownership=None)
                    except Exception as exc:
                        await handle_cancel_command_insert_failure(
                            repo=repo,
                            route_context=route_context,
                            plan=plan,
                            child_client_order_id=child_client_order_id,
                            exc=exc,
                        )
        except CapsViolationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=caps_violation_detail(exc),
            ) from exc
    else:
        await request_plan_status_transition(
            repo=repo,
            route_context=route_context,
            plan_public_id=plan_public_id,
            new_status=cancel_state.new_status,
            completed_at=route_context.now,
        )
    await insert_execution_plan_decision(
        repo=repo,
        route_context=route_context,
        row=_build_trailing_stop_cancelled_decision(
            command=command,
            plan_public_id=plan_public_id,
            route_context=route_context,
            has_active_children=cancel_state.has_active_children,
            new_status=cancel_state.new_status,
        ),
    )
    if not cancel_state.has_active_children:
        service._unregister_plan(plan_public_id)
    updated = await get_execution_plan_or_500(
        repo=repo,
        plan_public_id=plan_public_id,
        as_of=route_context.bus_time,
        detail="Plan updated but not found",
    )
    return build_execution_plan_response(
        updated,
        route_context,
    )


@router.get("/{plan_public_id}")
async def get_trailing_stop(
    request: Request,
    plan_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_ORDERS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ExecutionPlanResponse:
    """Retrieve a single trailing stop plan by public_id.

    Args:
        request: FastAPI request.
        plan_public_id: Plan to retrieve.
        principal: Authenticated caller holding READ_ORDERS.
        repo: Repository dependency.

    Returns:
        ExecutionPlanResponse wrapping the plan.

    Raises:
        HTTPException: 404 if not found, 403 if wallet not accessible.
    """
    route_context = build_plan_route_context(request, _REST_STREAM)
    plan = await load_accessible_execution_plan(
        repo=repo,
        principal=principal,
        plan_public_id=plan_public_id,
        as_of=route_context.now,
        not_found_detail=_TRAILING_STOP_NOT_FOUND,
        allowed_plan_type="trailing_stop",
    )
    return build_execution_plan_response(
        plan,
        route_context,
    )


@router.get("/{plan_public_id}/decisions")
async def list_trailing_stop_decisions(
    request: Request,
    plan_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_ORDERS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    importance: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, object]:
    """List decision audit rows for a trailing stop plan.

    Args:
        request: FastAPI request.
        plan_public_id: Plan to query decisions for.
        principal: Authenticated caller holding READ_ORDERS.
        repo: Repository dependency.
        importance: Optional importance filter (action/transition/routine).
        limit: Maximum rows to return.
        offset: Number of rows to skip.

    Returns:
        Dict with decisions list and count.

    Raises:
        HTTPException: 404 if plan not found, 403 if wallet not accessible.
    """
    now = datetime.now(UTC)

    plan = await repo.get_execution_plan(plan_public_id, as_of=now)
    if plan is None or plan["plan_type"] != "trailing_stop":
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_TRAILING_STOP_NOT_FOUND,
        )

    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=plan["wallet_public_id"],
        operator_public_id=plan.get("operator_public_id"),
    )

    decisions = await repo.list_execution_plan_decisions(
        plan_public_id=plan_public_id,
        as_of=now,
        importance=importance,
        limit=limit,
        offset=offset,
    )
    return {"decisions": decisions, "count": len(decisions)}


@router.get("/by-cycle/{cycle_public_id}")
async def get_trailing_stop_by_cycle(
    request: Request,
    cycle_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_ORDERS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> TrailingStopStateResponse | dict[str, object]:
    """Get live trailing stop state for a position cycle.

    Returns the active trailing stop's live evaluator state (peak_price,
    current_stop) if one exists. Returns a message payload if no active
    trailing stop is found.

    Args:
        request: FastAPI request.
        cycle_public_id: Position cycle to look up.
        principal: Authenticated caller holding READ_ORDERS.
        repo: Repository dependency.

    Returns:
        TrailingStopStateResponse with live state, or message payload.
    """
    service = _get_plan_executor(request)
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)
    ts = dt.datetime.now(dt.UTC)

    cycle = await repo.get_position_cycle_by_public_id(cycle_public_id, as_of=now)
    if cycle is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Position cycle not found",
        )
    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=cycle["wallet_public_id"],
        operator_public_id=cycle.get("operator_public_id"),
    )

    for plan in service.plans.values():
        if (
            plan.get("position_cycle_public_id") == cycle_public_id
            and plan["plan_type"] == "trailing_stop"
            and plan["status"] not in _TERMINAL_STATUSES
        ):
            evaluator = service.evaluators.get(plan["public_id"])
            checkpoint: dict[str, float] = {"peak_price": 0.0, "current_stop": 0.0}
            if isinstance(evaluator, TrailingStopEvaluator):
                raw = evaluator.build_checkpoint_state(plan)
                checkpoint = {
                    "peak_price": float(cast(float, raw.get("peak_price", 0.0))),
                    "current_stop": float(cast(float, raw.get("current_stop", 0.0))),
                }

            params = plan["params"]
            state_data = TrailingStopStateData(
                public_id=plan["public_id"],
                timestamp=ts,
                session_id=tracker.session_id,
                sequence_id=0,
                plan_public_id=plan["public_id"],
                status=plan["status"],
                trailing_pct=_coerce_float_param(params, "trailing_pct"),
                min_lock_pct=_coerce_float_param(params, "min_lock_pct"),
                entry_price=_coerce_float_param(params, "entry_price"),
                peak_price=checkpoint["peak_price"],
                current_stop=checkpoint["current_stop"],
                side=plan["side"],
            )
            return TrailingStopStateResponse(
                public_id=plan["public_id"],
                timestamp=ts,
                session_id=tracker.session_id,
                sequence_id=0,
                payload=state_data,
            )

    return {"type": "message", "payload": "none"}
