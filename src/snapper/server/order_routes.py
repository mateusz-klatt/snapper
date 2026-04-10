"""REST API routes for manual order creation and cancellation.

Provides POST /api/orders for creating manual_once execution plans
that emit a single TradeCommand through the existing executor pipeline.
Cancel and replace operations transition the plan status via SCD2.

All mutations require CREATE_ORDERS or CANCEL_ORDERS permissions
and are scoped to the caller's accessible wallets.
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

from snapper.api.schemas.orders import CancelOrderCommand
from snapper.api.schemas.orders import CreateOrderCommand
from snapper.api.schemas.orders import ExecutionPlanResponse
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionPlanData
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.scoping import resolve_target_wallets

_REST_STREAM = "rest.orders"
_EVALUATOR = ManualOnceEvaluator()

router = APIRouter(prefix="/orders", tags=["orders"])


def _plan_to_data(plan: dict[str, Any]) -> ExecutionPlanData:
    """Build an ExecutionPlanData from a plan row dict.

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
        last_error=plan["last_error"],
        idempotency_key=plan["idempotency_key"],
    )


@router.post("", openapi_extra=openapi_schema(CreateOrderCommand))
async def create_order(
    request: Request,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CREATE_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CreateOrderCommand, Depends(json_body(CreateOrderCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ExecutionPlanResponse:
    """Create a manual order via a manual_once execution plan.

    The endpoint validates order parameters, creates an ExecutionPlan row,
    inserts a TradeCommand for the outbox dispatcher, and returns the plan.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        principal: Authenticated caller holding CREATE_ORDERS.
        _csrf: CSRF token validation.
        command: Create order command envelope.
        repo: Repository dependency.

    Returns:
        ExecutionPlanResponse wrapping the newly-created plan.

    Raises:
        HTTPException: 400 if params invalid, 403 if wallet not accessible,
            409 if idempotency key already used.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    body = command.payload
    now = datetime.now(UTC)

    try:
        _EVALUATOR.validate_params(
            {
                "order_type": body.order_type,
                "side": body.side,
                "price": body.price,
                "stop_price": body.stop_price,
            }
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        ) from exc

    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=body.wallet_public_id,
    )

    shard_key = f"{body.exchange}:{body.instrument}:{body.mode}"
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())

    plan_params: dict[str, Any] = {
        "order_type": body.order_type,
        "side": body.side,
        "time_in_force": body.time_in_force,
        "post_only": body.post_only,
    }
    if body.price is not None:
        plan_params["price"] = body.price
    if body.stop_price is not None:
        plan_params["stop_price"] = body.stop_price
    if body.leverage is not None:
        plan_params["leverage"] = body.leverage

    try:
        plan_row: ExecutionPlanInsertRow = {
            "plan_type": "manual_once",
            "created_by_user_id": principal.username,
            "created_via": "api",
            "instrument_public_id": body.instrument_public_id,
            "exchange": body.exchange,
            "mode": body.mode,
            "shard_key": shard_key,
            "wallet_public_id": body.wallet_public_id,
            "operator_public_id": body.operator_public_id,
            "total_quantity": body.quantity,
            "side": body.side,
            "params": plan_params,
            "status": "active",
            "created_at": now,
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
                detail="Idempotency key already used",
            ) from exc
        logger.error("Failed to create execution plan: {}", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create order",
        ) from exc

    client_order_id = str(uuid7())
    cmd_seq = tracker.next_sequence(_REST_STREAM)
    try:
        cmd_row: TradeCommandInsertRow = {
            "command_type": "create",
            "shard_key": shard_key,
            "exchange": body.exchange,
            "instrument": body.instrument,
            "mode": body.mode,
            "strategy_id": "manual",
            "client_order_id": client_order_id,
            "venue_client_id": client_order_id,
            "side": body.side,
            "order_type": body.order_type,
            "quantity": body.quantity,
            "price": body.price,
            "leverage": body.leverage,
            "reduce_only": body.reduce_only,
            "status": "created",
            "created_at": now,
            "correlation_id": plan_public_id,
            "session_id": sid,
            "sequence_id": cmd_seq,
            "timestamp": ts,
            "wallet_public_id": body.wallet_public_id,
            "operator_public_id": body.operator_public_id,
            "user_public_id": principal.username,
            "plan_public_id": plan_public_id,
        }
        await repo.insert_trade_command(cmd_row)
    except Exception as exc:
        logger.error("Failed to create trade command for plan {}: {}", plan_public_id, exc)
        await repo.update_execution_plan_status(
            public_id=plan_public_id,
            new_status="failed",
            bus_time=ts,
            session_id=sid,
            sequence_id=tracker.next_sequence(_REST_STREAM),
            last_error=f"TradeCommand creation failed: {exc}",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create order command",
        ) from exc

    plan = await repo.get_execution_plan(plan_public_id, as_of=ts)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Plan created but not found",
        )

    plan_data = _plan_to_data(cast(dict[str, Any], plan))
    return ExecutionPlanResponse(
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=pid,
        timestamp=ts,
        payload=plan_data,
    )


@router.post(
    "/{plan_public_id}/cancel",
    openapi_extra=openapi_schema(CancelOrderCommand),
)
async def cancel_order(
    request: Request,
    plan_public_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CANCEL_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CancelOrderCommand, Depends(json_body(CancelOrderCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> ExecutionPlanResponse:
    """Cancel an active execution plan.

    Transitions the plan to cancel_requested status. The PlanExecutorService
    picks up the transition and cancels any in-flight child commands.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        plan_public_id: Plan to cancel (path parameter).
        principal: Authenticated caller holding CANCEL_ORDERS.
        _csrf: CSRF token validation.
        command: Cancel command envelope.
        repo: Repository dependency.

    Returns:
        ExecutionPlanResponse wrapping the updated plan.

    Raises:
        HTTPException: 404 if plan not found, 409 if already terminal.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)
    ts = dt.datetime.now(dt.UTC)
    sid = tracker.session_id

    plan = await repo.get_execution_plan(plan_public_id, as_of=now)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution plan not found",
        )

    terminal = {"completed", "cancelled", "failed", "expired"}
    if plan["status"] in terminal:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Plan already in terminal status: {plan['status']}",
        )

    new_id = await repo.update_execution_plan_status(
        public_id=plan_public_id,
        new_status="cancel_requested",
        bus_time=ts,
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        cancel_requested_at=now,
    )
    if new_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Plan not found or already closed",
        )

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
