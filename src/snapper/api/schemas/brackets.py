"""Pydantic schemas for bracket execution plan endpoints."""

from typing import Literal

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import StrictBody


class BracketCreateBody(StrictBody):
    """Request body for POST /api/execution-plans (bracket creation).

    Attributes:
        position_cycle_public_id: Target position cycle to protect.
        sl_price: Stop-loss trigger price (optional if tp_price set).
        tp_price: Take-profit trigger price (optional if sl_price set).
        idempotency_key: Optional dedup key for retries.
    """

    position_cycle_public_id: str
    sl_price: float | None = None
    tp_price: float | None = None
    idempotency_key: str | None = None


class BracketCreateCommand(
    PayloadRequest[Literal["create_bracket_command"], BracketCreateBody],
):
    """Request envelope for POST /api/execution-plans."""

    type: Literal["create_bracket_command"] = "create_bracket_command"


class BracketCancelBody(StrictBody):
    """Request body for POST /api/execution-plans/{id}/cancel.

    Attributes:
        reason: Optional human-readable cancellation reason.
    """

    reason: str | None = None


class BracketCancelCommand(
    PayloadRequest[Literal["cancel_bracket_command"], BracketCancelBody],
):
    """Request envelope for POST /api/execution-plans/{id}/cancel."""

    type: Literal["cancel_bracket_command"] = "cancel_bracket_command"
