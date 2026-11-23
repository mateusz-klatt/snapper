"""Admin routes for position cycle management.

Provides endpoints for listing open position cycles and closing
orphaned cycles that no longer have a matching trading engine.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from loguru import logger
from pydantic import Field
from pydantic.config import JsonDict

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.application.portfolio.position_display import PositionSpecSteps
from snapper.application.portfolio.position_display import display_for_quantity
from snapper.auth.dependencies import require_any_permission
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_global_read_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import PositionCycleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.position_display import load_position_quote_prices
from snapper.server.position_display import load_position_spec_steps
from snapper.server.position_display import position_dust_threshold

router = APIRouter(prefix="/position-cycles", tags=["position-cycles"])


def _omit_json_schema_default(schema: JsonDict) -> None:
    """Drop a runtime default so generated clients can omit the field."""
    schema.pop("default", None)


class PositionCycleData(StrictDataSchema[Literal["position_cycle"]]):
    """Position cycle summary for admin display.

    Attributes:
        is_dust: True when the cycle peak quantity, priced with the
            matching position's mark or average, lot-rounds to zero from
            a non-zero raw peak, is below the venue minimum, or is below
            the USD notional threshold. An exact zero peak is not dust.
            A cycle with no matching price, USD rate, or known contract
            value uses the lot-round and minimum-size rules only.
        quantity_decimals: Decimal places of the instrument lot step,
            or None when the spec has no positive lot size.
        price_decimals: Decimal places of the instrument tick size,
            or None when the spec has no positive tick size.
    """

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
    is_dust: bool = Field(default=False, json_schema_extra=_omit_json_schema_default)
    quantity_decimals: int | None = None
    price_decimals: int | None = None


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


async def _open_cycle_payloads(
    repo: Repository,
    rows: list[PositionCycleRow],
    now: datetime,
) -> list[PositionCycleData]:
    """Attach dust and precision fields to open cycle rows.

    The peak quantity is classified with the same steps, USD rate, and
    mark-or-average price as the matching open position. An orphan cycle
    has no price, so the notional rule does not apply.
    """
    threshold = position_dust_threshold()
    specs = await load_position_spec_steps(
        repo,
        [row["instrument_public_id"] for row in rows],
        now,
    )
    prices = await load_position_quote_prices(repo, now)
    return [
        _cycle_to_data(
            row,
            now,
            specs.get(row["instrument_public_id"]),
            threshold,
            prices.get(
                (
                    row["instrument_public_id"],
                    row["exchange"],
                    row["mode"],
                    row["wallet_public_id"],
                ),
                (None, None),
            ),
        )
        for row in rows
    ]


def _cycle_to_data(
    row: PositionCycleRow,
    now: datetime,
    steps: PositionSpecSteps | None,
    notional_threshold: Decimal,
    prices: tuple[float | None, float | None],
) -> PositionCycleData:
    """Convert a PositionCycleRow dict to API response data.

    Dust uses the cycle peak quantity, the venue minimum, and the same
    mark or average price the matching position uses. The notional is
    USD. A missing price skips the notional rule.
    """
    opened_at = row["opened_at"]
    age = now - opened_at
    mark_price, average_price = prices
    display = display_for_quantity(
        quantity=row["max_qty"],
        steps=steps,
        notional_threshold=notional_threshold,
        mark_price=mark_price,
        average_price=average_price,
    )
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
        is_dust=display.is_dust,
        quantity_decimals=display.quantity_decimals,
        price_decimals=display.price_decimals,
    )


@router.get("/open")
async def list_open_cycles(
    principal: Annotated[
        AuthPrincipal,
        Depends(require_any_permission(Permission.READ_POSITION_CYCLES, Permission.MANAGE_USERS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    min_age_hours: float = 0,
) -> PositionCycleListResponse:
    """List all open position cycles for an authorized global observer.

    Args:
        principal: Caller granted cycle read permission and global observation.
        repo: Database repository.
        min_age_hours: Only show cycles open longer than this (hours).

    Returns:
        List of open cycles with age information.

    Raises:
        HTTPException: 403 when the caller lacks global read authority.
    """
    if not has_global_read_permission(
        principal.role,
        principal.permissions,
        principal.permission_scope_version,
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Global read authority is required to list position cycles",
        )
    now = datetime.now(UTC)
    opened_before = now - timedelta(hours=min_age_hours) if min_age_hours > 0 else None
    rows = await repo.get_all_open_position_cycles(as_of=now, opened_before=opened_before)
    payload = await _open_cycle_payloads(repo, rows, now)
    return PositionCycleListResponse(
        public_id="list",
        timestamp=now,
        session_id=str(uuid7()),
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
