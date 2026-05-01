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
from snapper.api.schemas.orders import CreateOrderBody
from snapper.api.schemas.orders import CreateOrderCommand
from snapper.api.schemas.orders import ExecutionPlanResponse
from snapper.application.ai_review.citation import AiReviewCitationError
from snapper.application.ai_review.citation import validate_ai_review_citation
from snapper.application.plans.cancel_service import PlanAlreadyTerminalError
from snapper.application.plans.cancel_service import PlanCancelEmitError
from snapper.application.plans.cancel_service import PlanCancelIdempotencyKeyMismatchError
from snapper.application.plans.cancel_service import PlanCancelInProgressError
from snapper.application.plans.cancel_service import PlanConcurrentChangeError
from snapper.application.plans.cancel_service import PlanNotFoundError
from snapper.application.plans.cancel_service import PlansCancelService
from snapper.application.plans.cancel_service import PlanScopeError
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
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


@dataclass(frozen=True)
class OrderRouteContext:
    """Common request-scoped timestamps and sequence metadata."""

    tracker: SequenceTracker
    now: datetime
    bus_time: datetime
    session_id: str


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


async def _validate_create_order_ai_review_citation(
    *,
    repo: Repository,
    principal: AuthPrincipal,
    body: CreateOrderBody,
) -> None:
    """Plan D Phase 2 #10 R1 — gate ``ai_review_public_id`` citations.

    No-op when ``body.ai_review_public_id`` is None (the default for
    every non-AI-mediated manual order). When set, delegates to
    :func:`validate_ai_review_citation`; an :class:`AiReviewCitationError`
    becomes HTTP 403 so the caller cannot use a forged citation to
    trigger ``bus.caps_violation_after_ai_approve`` fanout to other
    delegates' UIs.
    """
    if body.ai_review_public_id is None:
        return
    try:
        await validate_ai_review_citation(
            repo,
            ai_review_public_id=body.ai_review_public_id,
            expected_user_public_id=principal.user_public_id or principal.username,
            expected_wallet_public_id=body.wallet_public_id,
        )
    except AiReviewCitationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


def _build_order_route_context(tracker: SequenceTracker) -> OrderRouteContext:
    """Build shared per-request timing and sequencing metadata."""
    now = datetime.now(UTC)
    return OrderRouteContext(
        tracker=tracker,
        now=now,
        bus_time=dt.datetime.now(dt.UTC),
        session_id=tracker.session_id,
    )


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

    await _validate_create_order_ai_review_citation(repo=repo, principal=principal, body=body)

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
        ai_review_public_id=body.ai_review_public_id,
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

    Delegates to :meth:`PlansCancelService.cancel_by_plan_public_id`
    (Plan B Phase 3.5 unification — REST + MCP share one cancel-plan
    code path) and maps the service's domain exceptions onto the REST
    HTTP contract:

    * :class:`PlanNotFoundError` → 404
    * :class:`PlanScopeError` → 403 (REST does NOT collapse to 404 like
      MCP does; the existing REST contract surfaces the distinct
      "wallet not accessible" outcome)
    * :class:`PlanAlreadyTerminalError` → 409 with the terminal status
      stamped into the detail
    * :class:`PlanCancelInProgressError` /
      :class:`PlanConcurrentChangeError` /
      :class:`PlanCancelIdempotencyKeyMismatchError` → 409 (the last is
      unreachable for REST today because we pass
      ``idempotency_key=None``, but is mapped defensively so future
      REST callers that opt into idempotency get the conflict envelope)
    * :class:`CapsViolationError` → 422 (unchanged contract)
    * :class:`PlanCancelEmitError` → 500 ``Failed to emit cancel command``

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
        HTTPException: 404 if plan not found, 403 if wallet not accessible,
            409 if already terminal or a concurrent status change lost
            the race, 422 on caps rejection, 500 on cancel-command emit
            failure.
    """
    route_context = _build_order_route_context(tracker)
    try:
        updated = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id=plan_public_id,
            idempotency_key=None,
            principal=principal,
            repo=repo,
            tracker=tracker,
            caps_enforcer=caps_enforcer,
            source_surface="rest",
        )
    except PlanNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution plan not found",
        ) from exc
    except PlanScopeError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Wallet not accessible",
        ) from exc
    except PlanAlreadyTerminalError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Plan already in terminal status: {exc.status}",
        ) from exc
    except (
        PlanCancelInProgressError,
        PlanConcurrentChangeError,
        PlanCancelIdempotencyKeyMismatchError,
    ) as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Plan status changed concurrently",
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
    except PlanCancelEmitError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to emit cancel command",
        ) from exc
    return _build_cancel_plan_response(updated, route_context)


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
