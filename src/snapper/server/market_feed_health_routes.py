"""REST route for per-symbol feed-health diagnostics.

``GET /api/market/feed-health`` answers the operator question: of the
symbols each publisher subscribed to, which are dark, when each last
received market data, and why. The rows are the persisted current-state
projection of each publisher subprocess's in-memory subscription-health
tracker, surviving restarts (the in-memory tracker does not).

An optional ``exchange`` query filter narrows the listing to one
exchange.

RBAC: the route requires :data:`Permission.READ_SYSTEM_STATUS` — this is
an operational health surface, not a market-data read.
"""

import datetime as dt
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Query
from fastapi import Request

from snapper.api.schemas.market_feed_health import InstrumentFeedHealthRowSchema
from snapper.api.schemas.market_feed_health import MarketFeedHealthPayload
from snapper.api.schemas.market_feed_health import MarketFeedHealthResponse
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/market/feed-health", tags=["market-feed-health"])

_REST_STREAM = "rest.market_feed_health"


def _envelope_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Stamp REST tracker provenance fields onto a response envelope."""
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, pid, ts


@router.get("")
async def get_market_feed_health(
    request: Request,
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    exchange: Annotated[
        str | None, Query(description="Optional exchange filter (lowercase)")
    ] = None,
    fresh_within_seconds: Annotated[
        int | None,
        Query(gt=0, description="Only rows snapshotted within this many seconds (drops stale)"),
    ] = None,
) -> MarketFeedHealthResponse:
    """Return current-state per-symbol feed-health rows.

    Args:
        request: Incoming request, carrying the REST tracker on app state.
        _user: Authenticated principal with ``READ_SYSTEM_STATUS``.
        repo: Repository dependency providing the feed-health rows.
        exchange: Optional exchange filter; ``None`` returns all rows.
        fresh_within_seconds: Optional staleness filter; rows older than
            this (a stopped/shrunk publisher) are dropped. ``None`` returns
            all rows with their ``snapshot_at`` for the caller to judge.

    Returns:
        A :class:`MarketFeedHealthResponse` envelope whose payload lists
        one entry per natural key plus the applied filters.
    """
    rows = await repo.list_instrument_feed_health(
        exchange=exchange, fresh_within_seconds=fresh_within_seconds
    )
    row_schemas = [
        InstrumentFeedHealthRowSchema(
            coordinator=row["coordinator"],
            exchange=row["exchange"],
            channel=row["channel"],
            symbol=row["symbol"],
            status=row["status"],
            requested_at=row["requested_at"],
            confirmed_at=row["confirmed_at"],
            last_seen_data_at=row["last_seen_data_at"],
            last_error=row["last_error"],
            retry_count=row["retry_count"],
            snapshot_at=row["snapshot_at"],
        )
        for row in rows
    ]
    payload = MarketFeedHealthPayload(
        rows=row_schemas, exchange=exchange, fresh_within_seconds=fresh_within_seconds
    )
    sid, seq, pid, ts = _envelope_provenance(request)
    return MarketFeedHealthResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=payload,
    )


__all__: list[Literal["router"]] = ["router"]
