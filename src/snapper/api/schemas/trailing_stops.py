"""Pydantic schemas for trailing stop execution plan endpoints."""

from typing import Literal

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import StrictBody


class TrailingStopCreateBody(StrictBody):
    """Request body for POST /api/trailing-stops.

    Attributes:
        position_cycle_public_id: Target position cycle to protect.
        trailing_pct: Trailing distance as percentage (e.g., 2.0 = 2%).
        min_lock_pct: Minimum profit % before trailing activates (0 = immediate).
        idempotency_key: Optional dedup key for retries.
    """

    position_cycle_public_id: str
    trailing_pct: float
    min_lock_pct: float = 0.0
    idempotency_key: str | None = None


class TrailingStopCreateCommand(
    PayloadRequest[Literal["create_trailing_stop_command"], TrailingStopCreateBody],
):
    """Request envelope for POST /api/trailing-stops."""

    type: Literal["create_trailing_stop_command"] = "create_trailing_stop_command"


class TrailingStopCancelBody(StrictBody):
    """Request body for POST /api/trailing-stops/{id}/cancel.

    Attributes:
        reason: Optional human-readable cancellation reason.
    """

    reason: str | None = None


class TrailingStopCancelCommand(
    PayloadRequest[Literal["cancel_trailing_stop_command"], TrailingStopCancelBody],
):
    """Request envelope for POST /api/trailing-stops/{id}/cancel."""

    type: Literal["cancel_trailing_stop_command"] = "cancel_trailing_stop_command"
