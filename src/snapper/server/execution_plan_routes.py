"""Execution plan routes for bracket SL/TP orders.

Provides create, cancel, detail, and decision-list endpoints for bracket
execution plans. Brackets attach to open position cycles and fire a
single reduce_only market close when a price threshold is breached.
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from typing import cast
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.brackets import BracketCancelCommand
from snapper.api.schemas.brackets import BracketCreateCommand
from snapper.api.schemas.orders import ExecutionPlanResponse
from snapper.application.plans.bracket import BracketEvaluator
from snapper.application.plans.service import PlanExecutorService
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import ExecutionPlanStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionPlanData
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.scoping import resolve_target_wallets

router = APIRouter(prefix="/execution-plans", tags=["execution-plans"])

_REST_STREAM = "execution_plan_rest"
_EVALUATOR = BracketEvaluator()
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


def _plan_to_data(plan: dict[str, Any]) -> ExecutionPlanData:
    """Project a plan row dict into the response schema.

    Args:
        plan: ExecutionPlanRow dict from repository.

    Returns:
        ExecutionPlanData with only the schema-declared fields.
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
        params=plan["params"],
        position_cycle_public_id=plan.get("position_cycle_public_id"),
        parent_plan_public_id=plan.get("parent_plan_public_id"),
        last_error=plan["last_error"],
        idempotency_key=plan["idempotency_key"],
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
    supports reduce_only (Decision C1), price thresholds are on the
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
    tracker: SequenceTracker = request.app.state.rest_tracker
    body = command.payload
    now = datetime.now(UTC)
    ts = dt.datetime.now(dt.UTC)

    if body.sl_price is None and body.tp_price is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="At least one of sl_price or tp_price required",
        )

    cycle = await repo.get_position_cycle_by_public_id(body.position_cycle_public_id, as_of=now)
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

    missing = await service._check_capabilities(
        "bracket", cycle["exchange"], cycle["instrument_public_id"]
    )
    if missing:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"Venue missing required capabilities: {', '.join(missing)}",
        )

    side = "buy" if cycle["direction"] == "long" else "sell"

    native_symbol = cycle["shard_key"].split(".")[1]
    positions = await repo.get_positions(as_of=now, wallet_public_ids=[cycle["wallet_public_id"]])
    current_qty = 0.0
    for pos in positions:
        if (
            pos["exchange"] == cycle["exchange"]
            and pos.get("mode", "live") == cycle["mode"]
            and pos["instrument"] == native_symbol
        ):
            current_qty = abs(pos["quantity"])
            break
    if current_qty <= 0:
        current_qty = cycle["max_qty"]

    if current_qty <= 0:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="No open position found for this cycle",
        )

    average_price = _resolve_average_price(positions, cycle)
    if average_price is not None:
        if side == "buy":
            if body.sl_price is not None and body.sl_price >= average_price:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"SL price {body.sl_price} must be below entry price {average_price} for long position",
                )
            if body.tp_price is not None and body.tp_price <= average_price:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"TP price {body.tp_price} must be above entry price {average_price} for long position",
                )
        else:
            if body.sl_price is not None and body.sl_price <= average_price:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"SL price {body.sl_price} must be above entry price {average_price} for short position",
                )
            if body.tp_price is not None and body.tp_price >= average_price:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                    detail=f"TP price {body.tp_price} must be below entry price {average_price} for short position",
                )

    native_symbol = cycle["shard_key"].split(".")[1]

    plan_params: dict[str, Any] = {
        "native_instrument": native_symbol,
    }
    if body.sl_price is not None:
        plan_params["sl_price"] = body.sl_price
    if body.tp_price is not None:
        plan_params["tp_price"] = body.tp_price

    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    pid = str(uuid7())

    try:
        plan_row: ExecutionPlanInsertRow = {
            "plan_type": "bracket",
            "created_by_user_id": principal.user_public_id or principal.username,
            "created_via": "api",
            "instrument_public_id": cycle["instrument_public_id"],
            "exchange": cycle["exchange"],
            "mode": cycle["mode"],
            "shard_key": cycle["shard_key"],
            "wallet_public_id": cycle["wallet_public_id"],
            "operator_public_id": cycle["operator_public_id"],
            "total_quantity": current_qty,
            "side": side,
            "params": plan_params,
            "status": ExecutionPlanStatusEnum.ARMED,
            "created_at": now,
            "position_cycle_public_id": cycle["public_id"],
            "idempotency_key": body.idempotency_key,
            "session_id": sid,
            "sequence_id": seq,
            "timestamp": ts,
        }
        _plan_id, plan_public_id = await repo.insert_execution_plan(plan_row)
    except Exception as exc:
        err_str = str(exc).lower()
        if "unique" in err_str or "duplicate" in err_str:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Duplicate bracket for this position cycle or idempotency key",
            ) from exc
        logger.error("Failed to create bracket plan: {}", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create bracket",
        ) from exc

    plan = await repo.get_execution_plan(plan_public_id, as_of=ts)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Bracket created but not found",
        )

    service._register_plan(cast(Any, plan), BracketEvaluator())

    try:
        await repo.insert_execution_plan_decision(
            row=ExecutionPlanDecisionInsertRow(
                plan_public_id=plan_public_id,
                decision_type="bracket_created",
                decided_at=now,
                trigger_type="api",
                evidence={
                    "sl_price": body.sl_price,
                    "tp_price": body.tp_price,
                    "position_cycle_public_id": body.position_cycle_public_id,
                },
                emitted_command_public_id=None,
                new_status=ExecutionPlanStatusEnum.ARMED,
                reason="Bracket created via API",
                decision_importance="action",
                source_surface="rest",
            ),
            bus_time=ts,
            session_id=sid,
            sequence_id=tracker.next_sequence(_REST_STREAM),
        )
    except Exception as exc:
        logger.error("Decision logging failed for bracket {}: {}", plan_public_id, exc)

    plan_data = _plan_to_data(cast(dict[str, Any], plan))
    return ExecutionPlanResponse(
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=pid,
        timestamp=ts,
        payload=plan_data,
    )


def _resolve_average_price(
    positions: list[Any],
    cycle: Any,
) -> float | None:
    """Find the average entry price for the position matching a cycle.

    Args:
        positions: Position rows from repo.
        cycle: Position cycle row.

    Returns:
        Average price or None if position not found.
    """
    native_symbol = cycle["shard_key"].split(".")[1]
    for pos in positions:
        if (
            pos["exchange"] == cycle["exchange"]
            and pos.get("mode", "live") == cycle["mode"]
            and pos["instrument"] == native_symbol
        ):
            avg = pos.get("average_price")
            if avg is not None and float(avg) > 0:
                return float(avg)
    return None


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
            gate the cancel TradeCommand insert against §3.5
            caps (cancel rate limit).

    Returns:
        ExecutionPlanResponse wrapping the updated plan.

    Raises:
        HTTPException: 404 if not found, 409 if already terminal,
            403 if wallet not accessible, 503 if executor unavailable.
    """
    service = _get_plan_executor(request)
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)
    ts = dt.datetime.now(dt.UTC)
    sid = tracker.session_id

    plan = await repo.get_execution_plan(plan_public_id, as_of=now)
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

    if plan["status"] in _TERMINAL_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Plan already in terminal status: {plan['status']}",
        )

    params = cast(dict[str, Any], plan["params"])
    child_ids = service._extract_child_ids(params)
    has_active_children = len(child_ids) > 0

    new_status: str = (
        ExecutionPlanStatusEnum.CANCEL_REQUESTED
        if has_active_children
        else ExecutionPlanStatusEnum.CANCELLED
    )

    if has_active_children:
        native_instrument = cast(str | None, params.get("native_instrument"))
        cancel_submission = TradeCommandSubmission(
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
        try:
            async with caps_enforcer.guard(cancel_submission):
                new_id = await repo.update_execution_plan_status(
                    public_id=plan_public_id,
                    new_status=new_status,
                    bus_time=ts,
                    session_id=sid,
                    sequence_id=tracker.next_sequence(_REST_STREAM),
                    cancel_requested_at=now,
                    completed_at=(now if new_status == ExecutionPlanStatusEnum.CANCELLED else None),
                )
                if new_id is None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Plan status changed concurrently",
                    )
                for child_cid in child_ids:
                    if native_instrument is None:
                        continue
                    exchange_order_id: str | None = None
                    try:
                        exchange_order_id = await repo.get_exchange_order_id_for_client_order_id(
                            child_cid, as_of=now
                        )
                    except Exception as exc:
                        logger.error("Cancel venue lookup failed for {}: {}", plan_public_id, exc)
                    cancel_cmd = TradeCommandInsertRow(
                        command_type="cancel",
                        shard_key=plan["shard_key"],
                        exchange=plan["exchange"],
                        instrument=native_instrument,
                        mode=plan["mode"],
                        strategy_id=plan["plan_type"],
                        client_order_id=child_cid,
                        venue_client_id=child_cid,
                        side=plan["side"],
                        order_type=str(params.get("venue_order_type", "market")),
                        quantity=plan["total_quantity"],
                        price=cast(Any, params.get("price")),
                        leverage=cast(Any, params.get("leverage")),
                        reduce_only=False,
                        status=TradeCommandStatusEnum.CREATED,
                        created_at=now,
                        correlation_id=plan_public_id,
                        session_id=sid,
                        sequence_id=tracker.next_sequence(_REST_STREAM),
                        timestamp=ts,
                        wallet_public_id=plan["wallet_public_id"] or "",
                        operator_public_id=plan.get("operator_public_id"),
                        user_public_id=principal.user_public_id or principal.username,
                        plan_public_id=plan_public_id,
                        exchange_order_id=exchange_order_id,
                    )
                    try:
                        await repo.insert_trade_command(cancel_cmd, ownership=None)
                    except Exception as exc:
                        logger.error(
                            "Failed to insert cancel command for plan {} child {}: {}",
                            plan_public_id,
                            child_cid,
                            exc,
                        )
                        try:
                            await repo.update_execution_plan_status(
                                public_id=plan_public_id,
                                new_status=ExecutionPlanStatusEnum.FAILED,
                                bus_time=ts,
                                session_id=sid,
                                sequence_id=tracker.next_sequence(_REST_STREAM),
                                last_error=f"Cancel command insert failed: {exc}",
                                completed_at=now,
                            )
                        except Exception as comp_exc:
                            logger.error(
                                "Compensation to failed also failed for plan {}: {}",
                                plan_public_id,
                                comp_exc,
                            )
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to emit cancel command",
                        ) from exc
        except CapsViolationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail={
                    "error_code": "caps_violation",
                    "cap_type": exc.cap_type,
                    "attempted": exc.attempted,
                    "limit": exc.limit,
                },
            ) from exc
    else:
        new_id = await repo.update_execution_plan_status(
            public_id=plan_public_id,
            new_status=new_status,
            bus_time=ts,
            session_id=sid,
            sequence_id=tracker.next_sequence(_REST_STREAM),
            cancel_requested_at=now,
            completed_at=(now if new_status == ExecutionPlanStatusEnum.CANCELLED else None),
        )
        if new_id is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Plan status changed concurrently",
            )

    await repo.insert_execution_plan_decision(
        row=ExecutionPlanDecisionInsertRow(
            plan_public_id=plan_public_id,
            decision_type="bracket_cancelled",
            decided_at=now,
            trigger_type="api",
            evidence={"reason": command.payload.reason, "had_children": has_active_children},
            emitted_command_public_id=None,
            new_status=new_status,
            reason=command.payload.reason or "Cancelled via API",
            decision_importance="action",
            source_surface="rest",
        ),
        bus_time=ts,
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
    )

    if not has_active_children:
        service._unregister_plan(plan_public_id)

    updated = await repo.get_execution_plan(plan_public_id, as_of=ts)
    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Plan updated but not found",
        )

    plan_data = _plan_to_data(cast(dict[str, Any], updated))
    return ExecutionPlanResponse(
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=ts,
        payload=plan_data,
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
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)
    ts = dt.datetime.now(dt.UTC)

    plan = await repo.get_execution_plan(plan_public_id, as_of=now)
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

    plan_data = _plan_to_data(cast(dict[str, Any], plan))
    return ExecutionPlanResponse(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=ts,
        payload=plan_data,
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
) -> dict[str, Any]:
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
        Dict with decisions list and count.

    Raises:
        HTTPException: 404 if plan not found, 403 if wallet not accessible.
    """
    now = datetime.now(UTC)

    plan = await repo.get_execution_plan(plan_public_id, as_of=now)
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
        as_of=now,
        importance=importance,
        limit=limit,
        offset=offset,
    )

    return {
        "decisions": decisions,
        "count": len(decisions),
    }
