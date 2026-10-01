"""Pydantic schemas for manual order creation and cancellation endpoints."""

from typing import Literal

from pydantic import field_validator

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.core.order_numbers import OrderLeverage
from snapper.core.order_numbers import PositiveOrderNumber
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
        quantity: Order quantity (must be positive and finite).
        price: Positive finite limit price (required for limit and stop_limit).
        stop_price: Positive finite stop trigger price (required for stop and stop_limit).
        time_in_force: Time-in-force policy (default GTC).
        post_only: Post-only flag for maker orders.
        leverage: Optional strict positive int32 leverage multiplier.
        reduce_only: Reduce-only flag for closing positions.
        wallet_public_id: Optional target wallet UUID. When omitted,
            the create-order path resolves the caller's single
            accessible wallet for the requested mode.
        operator_public_id: Optional operator identity.
        idempotency_key: Optional idempotency key for dedup.
        ai_review_public_id: Optional UUID7 of the ``ai_reviews`` row that
            AI-approved this trade. When set, a caps rejection inside
            :meth:`TradingCapsEnforcer.guard` triggers a
            ``bus.caps_violation_after_ai_approve`` publish so
            :class:`AiReviewService` can re-fanout the rejection to the
            delegate's UI. Default ``None`` keeps every existing
            manual-order caller untouched.
    """

    instrument: str
    instrument_public_id: str
    exchange: str
    mode: Literal["live", "paper"] = "live"
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit", "stop", "stop_limit"]
    quantity: PositiveOrderNumber
    price: PositiveOrderNumber | None = None
    stop_price: PositiveOrderNumber | None = None
    time_in_force: str = "GTC"
    post_only: bool = False
    leverage: OrderLeverage | None = None
    reduce_only: bool = False
    wallet_public_id: str | None = None
    operator_public_id: str | None = None
    idempotency_key: str | None = None
    ai_review_public_id: str | None = None

    @field_validator("wallet_public_id")
    @classmethod
    def _validate_wallet_public_id_not_blank(cls, value: str | None) -> str | None:
        """Reject explicit blank wallet IDs while preserving omitted autolookup."""
        if value is not None and value.strip() == "":
            raise ValueError("wallet_public_id must not be blank")
        return value


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
