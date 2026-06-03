"""REST route for per-exchange market-data coverage diagnostics.

``GET /api/market/coverage`` answers a single operator question per
exchange: of the instruments that are *currently configured* (active in
the bitemporal ``instruments`` table), how many are actually receiving
fresh market data versus the static configured set, and which are
"dark" — gated on for market data yet producing no recent ticks.

The freshness windows are operator-tunable via query parameters
(``tick_window_seconds`` defaulting to 600s / 10 min and
``candle_window_seconds`` defaulting to 1800s / 30 min), matching the
validated prod reference query's defaults.

RBAC: the route requires :data:`Permission.READ_SYSTEM_STATUS` — this
is an operational health surface, not a market-data read.
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

from snapper.api.schemas.market_coverage import MarketDataCoverageExchange
from snapper.api.schemas.market_coverage import MarketDataCoveragePayload
from snapper.api.schemas.market_coverage import MarketDataCoverageResponse
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/market/coverage", tags=["market-coverage"])

_REST_STREAM = "rest.market_coverage"


def _envelope_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Stamp REST tracker provenance fields onto a response envelope."""
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, pid, ts


@router.get("")
async def get_market_data_coverage(
    request: Request,
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    tick_window_seconds: Annotated[
        int, Query(gt=0, description="Freshness window for ticks, in seconds")
    ] = 600,
    candle_window_seconds: Annotated[
        int, Query(gt=0, description="Freshness window for candles, in seconds")
    ] = 1800,
) -> MarketDataCoverageResponse:
    """Return per-exchange market-data coverage over active instruments.

    Args:
        request: Incoming request, carrying the REST tracker on app state.
        _user: Authenticated principal with ``READ_SYSTEM_STATUS``.
        repo: Repository dependency providing the coverage aggregate.
        tick_window_seconds: Freshness window for ``ticks`` rows.
        candle_window_seconds: Freshness window for ``candles`` rows.

    Returns:
        A :class:`MarketDataCoverageResponse` envelope whose payload
        lists one entry per exchange plus the applied windows.
    """
    rows = await repo.get_market_data_coverage(
        tick_window_seconds=tick_window_seconds,
        candle_window_seconds=candle_window_seconds,
    )
    exchanges = [
        MarketDataCoverageExchange(
            exchange=row["exchange"],
            instruments=row["instruments"],
            fresh_ticks=row["fresh_ticks"],
            fresh_candles=row["fresh_candles"],
            gated_off=row["gated_off"],
            dark=row["dark"],
        )
        for row in rows
    ]
    payload = MarketDataCoveragePayload(
        exchanges=exchanges,
        tick_window_seconds=tick_window_seconds,
        candle_window_seconds=candle_window_seconds,
    )
    sid, seq, pid, ts = _envelope_provenance(request)
    return MarketDataCoverageResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=payload,
    )


__all__: list[Literal["router"]] = ["router"]
