"""Admin routes for position cycle management.

Provides endpoints for listing open position cycles and closing
orphaned cycles that no longer have a matching trading engine.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Annotated
from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from loguru import logger

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import PositionCycleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/position-cycles", tags=["position-cycles"])


class PositionCycleData(StrictDataSchema[Literal["position_cycle"]]):
    """Position cycle summary for admin display."""

    type: Literal["position_cycle"] = "position_cycle"
    cycle_public_id: str
    shard_key: str
    instrument_public_id: str
    exchange: str
    mode: str
    wallet_public_id: str
    operator_public_id: str | None
    direction: str
    max_qty: float
    opened_at: str
    age_hours: float


class PositionCycleListResponse(
    PayloadListResponse[Literal["position_cycles"], PositionCycleData],
):
    """List of open position cycles."""

    type: Literal["position_cycles"] = "position_cycles"


class OrphanSweepResultData(StrictDataSchema[Literal["orphan_sweep_result"]]):
    """Result of an orphan cycle sweep operation."""

    type: Literal["orphan_sweep_result"] = "orphan_sweep_result"
    closed_count: int
    closed_cycle_ids: list[str]


class OrphanSweepResponse(
    PayloadResponse[Literal["orphan_sweep_result"], OrphanSweepResultData],
):
    """Response from orphan sweep endpoint."""

    type: Literal["orphan_sweep_result"] = "orphan_sweep_result"


def _cycle_to_data(row: PositionCycleRow, now: datetime) -> PositionCycleData:
    """Convert a PositionCycleRow dict to API response data."""
    opened_at = row["opened_at"]
    age = now - opened_at
    return PositionCycleData(
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        cycle_public_id=row["public_id"],
        shard_key=row["shard_key"],
        instrument_public_id=row["instrument_public_id"],
        exchange=row["exchange"],
        mode=row["mode"],
        wallet_public_id=row["wallet_public_id"],
        operator_public_id=row.get("operator_public_id"),
        direction=row["direction"],
        max_qty=row["max_qty"],
        opened_at=opened_at.isoformat(),
        age_hours=round(age.total_seconds() / 3600, 1),
    )


@router.get(
    "/open",
    dependencies=[Depends(require_permission(Permission.MANAGE_USERS))],
)
async def list_open_cycles(
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    min_age_hours: float = 0,
) -> PositionCycleListResponse:
    """List all open position cycles, optionally filtered by minimum age.

    Args:
        principal: Authenticated admin user.
        repo: Database repository.
        min_age_hours: Only show cycles open longer than this (hours).

    Returns:
        List of open cycles with age information.
    """
    now = datetime.now(UTC)
    opened_before = now - timedelta(hours=min_age_hours) if min_age_hours > 0 else None
    rows = await repo.get_all_open_position_cycles(as_of=now, opened_before=opened_before)
    payload = [_cycle_to_data(r, now) for r in rows]
    return PositionCycleListResponse(
        public_id="list",
        timestamp=now,
        session_id="admin",
        sequence_id=0,
        payload=payload,
        count=len(payload),
    )


@router.post(
    "/close-orphan",
    dependencies=[
        Depends(validate_csrf_token),
        Depends(require_permission(Permission.MANAGE_USERS)),
    ],
)
async def close_orphan_cycle(
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    cycle_public_id: str,
) -> OrphanSweepResponse:
    """Close a specific orphaned position cycle.

    Closes a single open cycle identified by its public_id. The operator
    should verify via GET /open that the cycle is genuinely orphaned
    (no active engine) before calling this endpoint.

    Args:
        principal: Authenticated admin user.
        repo: Database repository.
        cycle_public_id: Public ID of the cycle to close.

    Returns:
        Sweep result with the closed cycle ID.
    """
    now = datetime.now(UTC)
    tracker = SequenceTracker()
    result = await repo.close_position_cycle(
        cycle_public_id=cycle_public_id,
        closed_at=now,
        closing_command_public_id=None,
        bus_time=now,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("admin.orphan_sweep"),
    )
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No active open cycle found for public_id={cycle_public_id}",
        )
    logger.info(f"Admin closed orphan cycle {cycle_public_id} (user={principal.user_public_id})")
    return OrphanSweepResponse(
        public_id="sweep",
        timestamp=now,
        session_id=tracker.session_id,
        sequence_id=0,
        payload=OrphanSweepResultData(
            public_id="sweep-result",
            timestamp=now,
            session_id=tracker.session_id,
            sequence_id=0,
            closed_count=1,
            closed_cycle_ids=[cycle_public_id],
        ),
    )


@router.post(
    "/sweep-orphans",
    dependencies=[
        Depends(validate_csrf_token),
        Depends(require_permission(Permission.MANAGE_USERS)),
    ],
)
async def sweep_orphan_cycles(
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    min_age_hours: float = 72,
) -> OrphanSweepResponse:
    """Bulk-close orphaned position cycles older than min_age_hours.

    Closes all open cycles that have been open longer than the specified
    threshold. Default threshold is 72 hours (3 days). The operator
    should review via GET /open?min_age_hours=72 before running this.

    Args:
        principal: Authenticated admin user.
        repo: Database repository.
        min_age_hours: Minimum age in hours for a cycle to be considered orphaned.

    Returns:
        Sweep result with count and list of closed cycle IDs.

    Raises:
        HTTPException: 400 if min_age_hours < 1.
    """
    if min_age_hours < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="min_age_hours must be at least 1",
        )
    now = datetime.now(UTC)
    opened_before = now - timedelta(hours=min_age_hours)
    rows = await repo.get_all_open_position_cycles(as_of=now, opened_before=opened_before)
    tracker = SequenceTracker()
    closed_ids: list[str] = []
    for row in rows:
        result = await repo.close_position_cycle(
            cycle_public_id=row["public_id"],
            closed_at=now,
            closing_command_public_id=None,
            bus_time=now,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("admin.orphan_sweep"),
        )
        if result is not None:
            closed_ids.append(row["public_id"])
    logger.info(
        f"Admin orphan sweep: closed {len(closed_ids)} cycles "
        f"(threshold={min_age_hours}h, user={principal.user_public_id})"
    )
    return OrphanSweepResponse(
        public_id="sweep",
        timestamp=now,
        session_id=tracker.session_id,
        sequence_id=0,
        payload=OrphanSweepResultData(
            public_id="sweep-result",
            timestamp=now,
            session_id=tracker.session_id,
            sequence_id=0,
            closed_count=len(closed_ids),
            closed_cycle_ids=closed_ids,
        ),
    )
