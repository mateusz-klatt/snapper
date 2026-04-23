"""REST API routes for manual order creation and cancellation.

Provides POST /api/orders for creating manual_once execution plans
that emit a single TradeCommand through the existing executor pipeline.
Cancel operations transition the plan status via SCD2.

All mutations require CREATE_ORDERS or CANCEL_ORDERS permissions
and are scoped to the caller's accessible wallets.
"""

import datetime as dt
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
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
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionPlanData
from snapper.server._capability_guard import require_tradable
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.scoping import resolve_target_wallets

_REST_STREAM = "rest.orders"
_EVALUATOR = ManualOnceEvaluator()
_ORDER_VALIDATION_RESPONSE: dict[int | str, dict[str, Any]] = {
    422: {"description": "Order request validation failed"}
}

_ORDER_TYPE_MAP: dict[str, str] = {
    "market": "market",
    "limit": "limit",
    "stop": "stop-loss",
    "stop_limit": "stop-loss-limit",
}

router = APIRouter(prefix="/orders", tags=["orders"])
_TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "cancelled", "failed", "expired"})


@dataclass(frozen=True)
class OrderRouteContext:
    """Common request-scoped timestamps and sequence metadata."""

    tracker: SequenceTracker
    now: datetime
    bus_time: datetime
    session_id: str


@dataclass(frozen=True)
class CancelPlanContext:
    """Resolved execution-plan state for a manual cancel request."""

    route_context: OrderRouteContext
    plan: ExecutionPlanRow
    params: JsonObject
    child_client_order_id: str | None
    native_instrument: str | None
    exchange_order_id: str | None


def _plan_to_data(plan: ExecutionPlanRow) -> ExecutionPlanData:
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
        params=cast(dict[str, object], plan["params"]),
        position_cycle_public_id=plan.get("position_cycle_public_id"),
        parent_plan_public_id=plan.get("parent_plan_public_id"),
        last_error=plan["last_error"],
        idempotency_key=plan["idempotency_key"],
    )


def _build_order_route_context(tracker: SequenceTracker) -> OrderRouteContext:
    """Build shared per-request timing and sequencing metadata."""
    now = datetime.now(UTC)
    return OrderRouteContext(
        tracker=tracker,
        now=now,
        bus_time=dt.datetime.now(dt.UTC),
        session_id=tracker.session_id,
    )


def _json_str_param(params: JsonObject, key: str) -> str | None:
    """Return a string JSON param or ``None`` when absent or not a string."""
    value = params.get(key)
    if isinstance(value, str):
        return value
    return None


def _ensure_plan_not_terminal(plan: ExecutionPlanRow) -> None:
    """Reject cancellation of already-terminal plans."""
    if plan["status"] in _TERMINAL_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Plan already in terminal status: {plan['status']}",
        )


async def _load_cancel_plan_context(
    repo: Repository,
    principal: AuthPrincipal,
    plan_public_id: str,
    route_context: OrderRouteContext,
) -> CancelPlanContext:
    """Load, scope-check, and enrich a plan for cancellation."""
    plan = await repo.get_execution_plan(plan_public_id, as_of=route_context.now)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution plan not found",
        )
    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=plan["wallet_public_id"],
    )
    _ensure_plan_not_terminal(plan)
    params = plan["params"]
    child_client_order_id = _json_str_param(params, "child_client_order_id")
    exchange_order_id: str | None = None
    if child_client_order_id is not None:
        exchange_order_id = await repo.get_exchange_order_id_for_client_order_id(
            child_client_order_id,
            as_of=route_context.now,
        )
    return CancelPlanContext(
        route_context=route_context,
        plan=plan,
        params=params,
        child_client_order_id=child_client_order_id,
        native_instrument=_json_str_param(params, "native_instrument"),
        exchange_order_id=exchange_order_id,
    )


def _cancel_requires_trade_command(context: CancelPlanContext) -> bool:
    """Return whether the plan has enough child state to emit a cancel command."""
    return context.child_client_order_id is not None and context.native_instrument is not None


def _build_cancel_submission(
    context: CancelPlanContext,
    principal: AuthPrincipal,
) -> TradeCommandSubmission:
    """Build the caps-enforcer submission for a manual cancel action."""
    order_type = _json_str_param(context.params, "venue_order_type") or "market"
    return TradeCommandSubmission(
        user_public_id=principal.user_public_id,
        operator_public_id=context.plan["operator_public_id"],
        wallet_public_id=context.plan["wallet_public_id"],
        instrument_public_id=context.plan["instrument_public_id"],
        command_type="cancel",
        side=context.plan["side"],
        order_type=order_type,
        quantity=None,
        price=None,
        source_surface="rest",
        idempotency_key=None,
    )


async def _request_cancel_requested_status(
    repo: Repository,
    plan_public_id: str,
    route_context: OrderRouteContext,
) -> None:
    """Transition the plan to ``cancel_requested`` and reject concurrent races."""
    new_id = await repo.update_execution_plan_status(
        public_id=plan_public_id,
        new_status="cancel_requested",
        bus_time=route_context.bus_time,
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(_REST_STREAM),
        cancel_requested_at=route_context.now,
    )
    if new_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Plan status changed concurrently",
        )


def _build_cancel_trade_command(
    context: CancelPlanContext,
    principal: AuthPrincipal,
) -> TradeCommandInsertRow:
    """Build the venue-facing cancel TradeCommand for a child order."""
    child_client_order_id = context.child_client_order_id
    native_instrument = context.native_instrument
    if child_client_order_id is None or native_instrument is None:
        raise ValueError("Cancel command requires child_client_order_id and native_instrument")
    route_context = context.route_context
    return TradeCommandInsertRow(
        command_type="cancel",
        shard_key=context.plan["shard_key"],
        exchange=context.plan["exchange"],
        instrument=native_instrument,
        mode=context.plan["mode"],
        strategy_id="manual",
        client_order_id=child_client_order_id,
        venue_client_id=child_client_order_id,
        side=context.plan["side"],
        order_type=_json_str_param(context.params, "venue_order_type") or "market",
        quantity=context.plan["total_quantity"],
        price=cast(float | None, context.params.get("price")),
        leverage=cast(int | None, context.params.get("leverage")),
        reduce_only=False,
        status=TradeCommandStatusEnum.CREATED,
        created_at=route_context.now,
        correlation_id=context.plan["public_id"],
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(_REST_STREAM),
        timestamp=route_context.bus_time,
        wallet_public_id=context.plan["wallet_public_id"],
        operator_public_id=context.plan["operator_public_id"],
        user_public_id=principal.user_public_id or principal.username,
        plan_public_id=context.plan["public_id"],
        exchange_order_id=context.exchange_order_id,
    )


async def _handle_cancel_command_failure(
    repo: Repository,
    plan_public_id: str,
    route_context: OrderRouteContext,
    exc: Exception,
) -> None:
    """Compensate a failed cancel-command insert and re-raise as HTTP 500."""
    logger.error("Failed to insert cancel command for plan {}: {}", plan_public_id, exc)
    try:
        await repo.update_execution_plan_status(
            public_id=plan_public_id,
            new_status="failed",
            bus_time=route_context.bus_time,
            session_id=route_context.session_id,
            sequence_id=route_context.tracker.next_sequence(_REST_STREAM),
            last_error=f"Cancel command insert failed: {exc}",
        )
    except Exception as compensation_exc:
        logger.error(
            "Failed to compensate plan {} to failed after cancel insert "
            "error: {}; PlanExecutorService recovery re-emits the cancel "
            "on restart",
            plan_public_id,
            compensation_exc,
        )
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="Failed to emit cancel command",
    ) from exc


async def _get_updated_plan_or_500(
    repo: Repository,
    plan_public_id: str,
    bus_time: datetime,
) -> ExecutionPlanRow:
    """Reload the updated plan or raise HTTP 500 if it disappeared."""
    updated = await repo.get_execution_plan(plan_public_id, as_of=bus_time)
    if updated is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Plan updated but not found",
        )
    return updated


def _build_cancel_plan_response(
    plan: ExecutionPlanRow,
    route_context: OrderRouteContext,
) -> ExecutionPlanResponse:
    """Build the REST response envelope for a cancelled manual order plan."""
    return ExecutionPlanResponse(
        session_id=route_context.session_id,
        sequence_id=route_context.tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=route_context.bus_time,
        payload=_plan_to_data(plan),
    )


@router.post(
    "",
    openapi_extra=openapi_schema(CreateOrderCommand),
    responses=_ORDER_VALIDATION_RESPONSE,
)
async def create_order(
    request: Request,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CREATE_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CreateOrderCommand, Depends(json_body(CreateOrderCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    caps_enforcer: Annotated[TradingCapsEnforcer, Depends(get_caps_enforcer_dependency)],
) -> ExecutionPlanResponse:
    """Create a manual order via a manual_once execution plan.

    Creates the plan with status=pending, inserts the TradeCommand
    then transitions to active. On command insert failure the plan
    is marked failed. This two-phase approach prevents orphaned
    active plans without commands.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        principal: Authenticated caller holding CREATE_ORDERS.
        _csrf: CSRF token validation.
        command: Create order command envelope.
        repo: Repository dependency.
        caps_enforcer: Per-user :class:`TradingCapsEnforcer`
            injected by :func:`get_caps_enforcer_dependency`
            wraps the TradeCommand insert with
            meth:`guard` so the caller's caps
            (quantity, open orders, daily USD notional) are
            evaluated before persistence.

    Returns:
        ExecutionPlanResponse wrapping the newly-created plan.

    Raises:
        HTTPException: 422 if params invalid, 403 if wallet not accessible
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
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    await require_tradable(repo, body.instrument, body.exchange, as_of=now)

    resolved_instrument_public_id = await repo.get_instrument_public_id_by_symbol(
        native_symbol=body.instrument,
        exchange=body.exchange,
        as_of=now,
    )
    if resolved_instrument_public_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "error_code": "unknown_instrument",
                "symbol": body.instrument,
                "exchange": body.exchange,
                "reason": (
                    "no active Instrument row resolves for this "
                    "(symbol, exchange) pair; the capability guard accepted "
                    "the symbol but Snapper cannot identify the instrument "
                    "record to persist against"
                ),
            },
        )

    await resolve_target_wallets(
        principal=principal,
        repo=repo,
        wallet_public_id=body.wallet_public_id,
    )

    shard_key = f"{body.exchange}.{body.instrument}.{body.mode}"
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    user_pid = principal.user_public_id or principal.username

    client_order_id = str(uuid7())
    venue_order_type = _ORDER_TYPE_MAP.get(body.order_type, body.order_type)
    plan_params: dict[str, Any] = {
        "order_type": body.order_type,
        "side": body.side,
        "time_in_force": body.time_in_force,
        "post_only": body.post_only,
        "child_client_order_id": client_order_id,
        "native_instrument": body.instrument,
        "venue_order_type": venue_order_type,
    }
    if body.price is not None:
        plan_params["price"] = body.price
    if body.stop_price is not None:
        plan_params["stop_price"] = body.stop_price
    if body.leverage is not None:
        plan_params["leverage"] = body.leverage

    submission = TradeCommandSubmission(
        user_public_id=principal.user_public_id,
        operator_public_id=body.operator_public_id,
        wallet_public_id=body.wallet_public_id,
        instrument_public_id=resolved_instrument_public_id,
        command_type="create",
        side=body.side,
        order_type=venue_order_type,
        quantity=Decimal(str(body.quantity)),
        price=Decimal(str(body.price)) if body.price is not None else None,
        source_surface="rest",
        idempotency_key=body.idempotency_key,
    )
    plan_public_id: str | None = None
    try:
        async with caps_enforcer.guard(submission):
            try:
                plan_row: ExecutionPlanInsertRow = {
                    "plan_type": "manual_once",
                    "created_by_user_id": user_pid,
                    "created_via": "api",
                    "instrument_public_id": resolved_instrument_public_id,
                    "exchange": body.exchange,
                    "mode": body.mode,
                    "shard_key": shard_key,
                    "wallet_public_id": body.wallet_public_id,
                    "operator_public_id": body.operator_public_id,
                    "total_quantity": body.quantity,
                    "side": body.side,
                    "params": plan_params,
                    "status": "pending",
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
                    "order_type": venue_order_type,
                    "quantity": body.quantity,
                    "price": body.price,
                    "leverage": body.leverage,
                    "reduce_only": body.reduce_only,
                    "status": TradeCommandStatusEnum.CREATED,
                    "created_at": now,
                    "correlation_id": plan_public_id,
                    "session_id": sid,
                    "sequence_id": cmd_seq,
                    "timestamp": ts,
                    "wallet_public_id": body.wallet_public_id,
                    "operator_public_id": body.operator_public_id,
                    "user_public_id": user_pid,
                    "plan_public_id": plan_public_id,
                }
                await repo.insert_trade_command(cmd_row, ownership=None)
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
    assert plan_public_id is not None

    await repo.update_execution_plan_status(
        public_id=plan_public_id,
        new_status="active",
        bus_time=ts,
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        started_at=now,
    )

    plan = await repo.get_execution_plan(plan_public_id, as_of=ts)
    if plan is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Plan created but not found",
        )

    plan_data = _plan_to_data(plan)
    return ExecutionPlanResponse(
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=pid,
        timestamp=ts,
        payload=plan_data,
    )


async def _cancel_plan(
    repo: Repository,
    tracker: SequenceTracker,
    principal: AuthPrincipal,
    plan_public_id: str,
    caps_enforcer: TradingCapsEnforcer,
) -> ExecutionPlanResponse:
    """Shared cancel-plan logic used by the by-id and by-client-order-id routes.

    Transitions the plan to ``cancel_requested`` (SCD2), inserts a
    matching cancel ``TradeCommand`` for the active child order, and
    returns the updated plan as a response envelope.

    Args:
        repo: Repository dependency.
        tracker: REST provenance tracker.
        principal: Authenticated caller.
        plan_public_id: Plan to cancel.
        caps_enforcer: Per-user :class:`TradingCapsEnforcer` used to
            gate the cancel TradeCommand insert against
            caps (cancel rate limit).

    Returns:
        ExecutionPlanResponse wrapping the updated plan.

    Raises:
        HTTPException: 404 if plan not found, 403 if wallet not accessible
            409 if already terminal or a concurrent status change lost the race.
    """
    route_context = _build_order_route_context(tracker)
    context = await _load_cancel_plan_context(
        repo=repo,
        principal=principal,
        plan_public_id=plan_public_id,
        route_context=route_context,
    )

    if _cancel_requires_trade_command(context):
        try:
            async with caps_enforcer.guard(_build_cancel_submission(context, principal)):
                await _request_cancel_requested_status(
                    repo=repo,
                    plan_public_id=plan_public_id,
                    route_context=route_context,
                )
                cancel_cmd = _build_cancel_trade_command(context, principal)
                try:
                    await repo.insert_trade_command(cancel_cmd, ownership=None)
                except Exception as exc:
                    await _handle_cancel_command_failure(
                        repo=repo,
                        plan_public_id=plan_public_id,
                        route_context=route_context,
                        exc=exc,
                    )
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
        await _request_cancel_requested_status(
            repo=repo,
            plan_public_id=plan_public_id,
            route_context=route_context,
        )

    updated = await _get_updated_plan_or_500(
        repo=repo,
        plan_public_id=plan_public_id,
        bus_time=route_context.bus_time,
    )
    return _build_cancel_plan_response(
        updated,
        route_context,
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
    caps_enforcer: Annotated[TradingCapsEnforcer, Depends(get_caps_enforcer_dependency)],
) -> ExecutionPlanResponse:
    """Cancel an active execution plan.

    Verifies the caller has access to the plan's wallet before
    transitioning the plan to cancel_requested status and emitting a
    venue-facing cancel ``TradeCommand`` for the active child order.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        plan_public_id: Plan to cancel (path parameter).
        principal: Authenticated caller holding CANCEL_ORDERS.
        _csrf: CSRF token validation.
        command: Cancel command envelope.
        repo: Repository dependency.
        caps_enforcer: Per-user cap enforcer.

    Returns:
        ExecutionPlanResponse wrapping the updated plan.

    Raises:
        HTTPException: 404 if plan not found, 403 if wallet not accessible
            409 if already terminal.
    """
    del command
    tracker: SequenceTracker = request.app.state.rest_tracker
    return await _cancel_plan(
        repo=repo,
        tracker=tracker,
        principal=principal,
        plan_public_id=plan_public_id,
        caps_enforcer=caps_enforcer,
    )


@router.post(
    "/by-client-order-id/{client_order_id}/cancel",
    openapi_extra=openapi_schema(CancelOrderCommand),
)
async def cancel_order_by_client_order_id(
    request: Request,
    client_order_id: str,
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CANCEL_ORDERS)),
    ],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    command: Annotated[CancelOrderCommand, Depends(json_body(CancelOrderCommand))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    caps_enforcer: Annotated[TradingCapsEnforcer, Depends(get_caps_enforcer_dependency)],
) -> ExecutionPlanResponse:
    """Cancel an order by its ``client_order_id``.

    Convenience endpoint for the Orders UI that only knows the child
    order's ``client_order_id``. Resolves to the owning execution plan
    via ``trade_commands.plan_public_id`` and then delegates to the
    shared cancel flow.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        client_order_id: Child order client id to cancel.
        principal: Authenticated caller holding CANCEL_ORDERS.
        _csrf: CSRF token validation.
        command: Cancel command envelope.
        repo: Repository dependency.
        caps_enforcer: Per-user cap enforcer.

    Returns:
        ExecutionPlanResponse wrapping the updated plan.

    Raises:
        HTTPException: 404 if no plan linked to this client_order_id
            403 if wallet not accessible, 409 if already terminal.
    """
    del command
    tracker: SequenceTracker = request.app.state.rest_tracker
    plan_public_id = await repo.get_plan_public_id_for_client_order_id(
        client_order_id,
        as_of=datetime.now(UTC),
    )
    if plan_public_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No execution plan found for this client_order_id",
        )
    return await _cancel_plan(
        repo=repo,
        tracker=tracker,
        principal=principal,
        plan_public_id=plan_public_id,
        caps_enforcer=caps_enforcer,
    )
