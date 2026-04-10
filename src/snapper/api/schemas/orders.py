"""Pydantic schemas for manual order creation and cancellation endpoints."""

from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.messaging.schemas.data import ExecutionPlanData


class CreateOrderBody(StrictBody):
    """Request body for POST /api/orders (manual order creation).

    Attributes:
        instrument: Native symbol (e.g. BTC-USD, ETH/USD).
        instrument_public_id: Target instrument UUID.
        exchange: Exchange to route the order to.
        mode: Execution mode (live or paper).
        side: Order side (buy or sell).
        order_type: Order type (market, limit, stop, stop_limit).
        quantity: Order quantity (must be positive).
        price: Limit price (required for limit and stop_limit).
        stop_price: Stop trigger price (required for stop and stop_limit).
        time_in_force: Time-in-force policy (default GTC).
        post_only: Post-only flag for maker orders.
        leverage: Optional leverage multiplier.
        reduce_only: Reduce-only flag for closing positions.
        wallet_public_id: Target wallet UUID.
        operator_public_id: Optional operator identity.
        idempotency_key: Optional idempotency key for dedup.
    """

    instrument: str
    instrument_public_id: str
    exchange: str
    mode: Literal["live", "paper"] = "live"
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit", "stop", "stop_limit"]
    quantity: float = Field(gt=0)
    price: float | None = None
    stop_price: float | None = None
    time_in_force: str = "GTC"
    post_only: bool = False
    leverage: int | None = None
    reduce_only: bool = False
    wallet_public_id: str
    operator_public_id: str | None = None
    idempotency_key: str | None = None


class CreateOrderCommand(
    PayloadRequest[Literal["create_order_command"], CreateOrderBody],
):
    """Request envelope for POST /api/orders."""

    type: Literal["create_order_command"] = "create_order_command"


class CancelOrderBody(StrictBody):
    """Request body for POST /api/orders/{id}/cancel.

    Attributes:
        reason: Optional human-readable cancellation reason.
    """

    reason: str | None = None


class CancelOrderCommand(
    PayloadRequest[Literal["cancel_order_command"], CancelOrderBody],
):
    """Request envelope for POST /api/orders/{id}/cancel."""

    type: Literal["cancel_order_command"] = "cancel_order_command"


class ExecutionPlanResponse(
    PayloadResponse[Literal["execution_plan_response"], ExecutionPlanData],
):
    """Singleton wrapper returned by order creation and plan endpoints."""

    type: Literal["execution_plan_response"] = "execution_plan_response"
