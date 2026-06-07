"""Execution plan routes for bracket SL/TP orders.

Provides create, cancel, detail, and decision-list endpoints for bracket
execution plans. Brackets attach to open position cycles and fire a
single reduce_only market close when a price threshold is breached.
"""

from typing import Annotated
from typing import cast
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.brackets import BracketCancelCommand
from snapper.api.schemas.brackets import BracketCreateCommand
from snapper.api.schemas.data_responses import ExecutionPlanDecisionListResponse
from snapper.api.schemas.orders import ExecutionPlanResponse
from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.service import PlanExecutorService
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
from snapper.messaging.schemas.data import ExecutionPlanDecisionData
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

router = APIRouter(prefix="/execution-plans", tags=["execution-plans"])

_REST_STREAM = "execution_plan_rest"
_EXECUTION_PLAN_NOT_FOUND = "Execution plan not found"
_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {
        ExecutionPlanStatusEnum.COMPLETED,
        ExecutionPlanStatusEnum.CANCELLED,
        ExecutionPlanStatusEnum.FAILED,
        ExecutionPlanStatusEnum.EXPIRED,
    }
)


def _get_plan_executor(request: Request) -> PlanExecutorService:
    """Retrieve the PlanExecutorService from app state.

    Raises:
        HTTPException: 503 if executor is not available (API-only mode).
    """
    executor = getattr(request.app.state, "plan_executor", None)
    if executor is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Plan executor not available (API-only mode)",
        )
    return cast(PlanExecutorService, executor)


def _validate_bracket_legs(command: BracketCreateCommand) -> None:
    """Require at least one bracket leg."""
    body = command.payload
    if body.sl_price is None and body.tp_price is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="At least one of sl_price or tp_price required",
        )


def _resolve_average_price(
    positions: list[PositionRow],
    cycle: PositionCycleRow,
) -> float | None:
    """Backward-compatible wrapper used by the route tests."""
    return resolve_average_price(positions, cycle)


def _validate_bracket_prices(
    command: BracketCreateCommand,
    side: str,
    average_price: float | None,
) -> None:
    """Validate SL/TP thresholds against the inferred entry price."""
    if average_price is None:
        return
    body = command.payload
    if side == "buy":
        if body.sl_price is not None and body.sl_price >= average_price:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    f"SL price {body.sl_price} must be below entry price "
                    f"{average_price} for long position"
                ),
            )
        if body.tp_price is not None and body.tp_price <= average_price:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=(
                    f"TP price {body.tp_price} must be above entry price "
                    f"{average_price} for long position"
                ),
            )
        return
    if body.sl_price is not None and body.sl_price <= average_price:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"SL price {body.sl_price} must be above entry price "
                f"{average_price} for short position"
            ),
        )
    if body.tp_price is not None and body.tp_price >= average_price:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"TP price {body.tp_price} must be below entry price "
                f"{average_price} for short position"
            ),
        )


def _build_bracket_plan_params(
    command: BracketCreateCommand,
    native_instrument: str,
) -> JsonObject:
    """Build persisted bracket params."""
    body = command.payload
    params: JsonObject = {"native_instrument": native_instrument}
    if body.sl_price is not None:
        params["sl_price"] = body.sl_price
    if body.tp_price is not None:
        params["tp_price"] = body.tp_price
    return params


def _build_bracket_plan_row(
    *,
    command: BracketCreateCommand,
    principal: AuthPrincipal,
    route_context: PlanRouteContext,
    cycle: PositionCycleRow,
    side: str,
    total_quantity: float,
    params: JsonObject,
) -> ExecutionPlanInsertRow:
    """Build the execution-plan insert row for a bracket."""
    return ExecutionPlanInsertRow(
        plan_type="bracket",
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


def _build_bracket_created_decision(
    *,
    command: BracketCreateCommand,
    plan_public_id: str,
    route_context: PlanRouteContext,
) -> ExecutionPlanDecisionInsertRow:
    """Build the persisted decision row for bracket creation."""
    return ExecutionPlanDecisionInsertRow(
        plan_public_id=plan_public_id,
        decision_type="bracket_created",
        decided_at=route_context.now,
        trigger_type="api",
        evidence={
            "sl_price": command.payload.sl_price,
            "tp_price": command.payload.tp_price,
            "position_cycle_public_id": command.payload.position_cycle_public_id,
        },
        emitted_command_public_id=None,
        new_status=ExecutionPlanStatusEnum.ARMED,
        reason="Bracket created via API",
        decision_importance="action",
        source_surface="rest",
    )


def _build_bracket_cancelled_decision(
    *,
    command: BracketCancelCommand,
    plan_public_id: str,
    route_context: PlanRouteContext,
    has_active_children: bool,
    new_status: str,
) -> ExecutionPlanDecisionInsertRow:
    """Build the persisted decision row for bracket cancellation."""
    return ExecutionPlanDecisionInsertRow(
        plan_public_id=plan_public_id,
        decision_type="bracket_cancelled",
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


@router.post("", openapi_extra=openapi_schema(BracketCreateCommand))
async def create_bracket(
    request: Request,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CREATE_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[BracketCreateCommand, Depends(json_body(BracketCreateCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ExecutionPlanResponse:
    """Create a bracket (SL/TP) execution plan on an open position cycle.

    Validates the cycle is open, the caller has wallet access, the venue
    supports reduce_only, price thresholds are on the
    correct side, and at least one leg is present. The bracket is created
    with status=armed and immediately starts watching ticks.

    Args:
        request: FastAPI request (provides REST tracker + app state).
        principal: Authenticated caller holding CREATE_ORDERS.
        _csrf: CSRF token validation.
        command: Bracket create command envelope.
        repo: Repository dependency.

    Returns:
        ExecutionPlanResponse wrapping the new armed bracket plan.

    Raises:
        HTTPException: 422 if params invalid or capability missing,
            409 if cycle not open or duplicate bracket, 403 if wallet
            not accessible, 503 if executor unavailable.
    """
    service = _get_plan_executor(request)
    route_context = build_plan_route_context(request, _REST_STREAM)

    _validate_bracket_legs(command)
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
        capability_name="bracket",
    )
    average_price = _resolve_average_price(
        cycle_context.positions,
        cycle_context.cycle,
    )
    _validate_bracket_prices(
        command,
        cycle_context.side,
        average_price,
    )

    plan_public_id = await insert_execution_plan_or_raise(
        repo=repo,
        row=_build_bracket_plan_row(
            command=command,
            principal=principal,
            route_context=route_context,
            cycle=cycle_context.cycle,
            side=cycle_context.side,
            total_quantity=cycle_context.total_quantity,
            params=_build_bracket_plan_params(
                command,
                cycle_context.native_instrument,
            ),
        ),
        duplicate_detail="Duplicate bracket for this position cycle or idempotency key",
        failure_detail="Failed to create bracket",
        failure_log="Failed to create bracket plan",
    )
    plan = await get_execution_plan_or_500(
        repo=repo,
        plan_public_id=plan_public_id,
        as_of=route_context.bus_time,
        detail="Bracket created but not found",
    )
    service._register_plan(plan, BracketEvaluator())
    await insert_execution_plan_decision_best_effort(
        repo=repo,
        route_context=route_context,
        row=_build_bracket_created_decision(
            command=command,
            plan_public_id=plan_public_id,
            route_context=route_context,
        ),
        failure_log="Decision logging failed for bracket",
    )
    return build_execution_plan_response(
        plan,
        route_context,
    )


@router.post(
    "/{plan_public_id}/cancel",
    openapi_extra=openapi_schema(BracketCancelCommand),
)
async def cancel_bracket(
    request: Request,
    plan_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CANCEL_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[BracketCancelCommand, Depends(json_body(BracketCancelCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    caps_enforcer: Annotated[TradingCapsEnforcer, Depends(get_caps_enforcer_dependency)],
) -> ExecutionPlanResponse:
    """Cancel a bracket execution plan.

    Armed brackets (no child orders yet) transition directly to cancelled.
    Active brackets (child orders in-flight) transition to cancel_requested
    and emit cancel TradeCommands, keeping the plan registered until venue
    terminal events land.

    Args:
        request: FastAPI request.
        plan_public_id: Bracket plan to cancel.
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
        not_found_detail=_EXECUTION_PLAN_NOT_FOUND,
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
        row=_build_bracket_cancelled_decision(
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
async def get_bracket(
    request: Request,
    plan_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.READ_ORDERS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ExecutionPlanResponse:
    """Retrieve a single execution plan by public_id.

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
        not_found_detail=_EXECUTION_PLAN_NOT_FOUND,
    )
    return build_execution_plan_response(
        plan,
        route_context,
    )


@router.get("/{plan_public_id}/decisions")
async def list_bracket_decisions(
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
) -> ExecutionPlanDecisionListResponse:
    """List decision audit rows for an execution plan.

    Args:
        request: FastAPI request.
        plan_public_id: Plan to query decisions for.
        principal: Authenticated caller holding READ_ORDERS.
        repo: Repository dependency.
        importance: Optional importance filter (action/transition/routine).
        limit: Maximum rows to return.
        offset: Number of rows to skip.

    Returns:
        Typed Pydantic envelope wrapping the decision audit rows.

    Raises:
        HTTPException: 404 if plan not found, 403 if wallet not accessible.
    """
    route_context = build_plan_route_context(request, _REST_STREAM)

    plan = await repo.get_execution_plan(plan_public_id, as_of=route_context.now)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_EXECUTION_PLAN_NOT_FOUND,
        )

    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=plan["wallet_public_id"],
        operator_public_id=plan.get("operator_public_id"),
    )

    decisions = await repo.list_execution_plan_decisions(
        plan_public_id=plan_public_id,
        as_of=route_context.now,
        importance=importance,
        limit=limit,
        offset=offset,
    )

    items = [ExecutionPlanDecisionData(**row) for row in decisions]
    return ExecutionPlanDecisionListResponse(
        public_id=str(uuid7()),
        timestamp=route_context.bus_time,
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(route_context.stream),
        payload=items,
        count=len(items),
    )
