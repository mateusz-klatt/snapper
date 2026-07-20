"""P&L timeline read API (Phase 5A).

Exposes the pure Net-P&L-since-activation engine over REST. ``GET
/api/portfolio/pnl/series`` reconstructs one ``(wallet, mode)`` scope's series
for a from/to window at a chosen granularity, mirroring the structure of
``position_cycle_routes`` (standalone ``APIRouter`` module, permission-gated
dependency, repository injection, ``SequenceTracker`` provenance from
``request.app.state.rest_tracker``) and the wallet-scoping pattern of
``/api/positions`` (:func:`resolve_target_wallets`). Unlike the list endpoints a
single wallet is REQUIRED — there is no all-wallets P&L aggregation in v1 (the
activation epoch is per wallet/mode).

The ``/timeline`` markers endpoint from the plan is deliberately NOT built here;
it is a separate deferred increment.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Annotated
from typing import Final
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.pnl_timeline import PnlInstrumentContributionData
from snapper.api.schemas.pnl_timeline import PnlSeriesData
from snapper.api.schemas.pnl_timeline import PnlSeriesResponse
from snapper.api.schemas.pnl_timeline import PnlTimelinePointData
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.scoping import resolve_target_wallets

router = APIRouter(prefix="/portfolio", tags=["portfolio-timeline"])

_SUPPORTED_GRANULARITIES: Final[frozenset[str]] = frozenset({"1m", "5m", "1h", "1d"})
"""The four granularities the pure builder downsamples the 1m grid to."""

_SUPPORTED_MODES: Final[frozenset[str]] = frozenset({"live", "paper"})
"""Trading modes accepted by the v1 reconstruction endpoint."""

_MIN_SAFE_WINDOW_FROM: Final[datetime] = datetime.min.replace(tzinfo=UTC) + timedelta(minutes=1)
"""Earliest start whose leading candle minute is representable."""

_MAX_SAFE_WINDOW_TO: Final[datetime] = datetime.max.replace(tzinfo=UTC) - timedelta(minutes=1)
"""Latest end whose inclusive minute grid can advance without overflow."""

_PNL_SERIES_STREAM: Final[str] = "rest.portfolio_pnl_series"
"""REST provenance stream the series response draws its sequence id from."""

_INTERNAL_ERROR_DETAIL: Final[str] = "Failed to build P&L series"


def _parse_utc_query_datetime(value: str, parameter_name: str) -> datetime:
    """Parse a query datetime and safely normalize it to UTC.

    A naive datetime is assumed to already be UTC; an aware one is converted.
    Both the grid the engine builds and the candle ``open_at`` marks are UTC, so
    a naive/aware mismatch would silently miss every mark key. Parsing happens
    inside the route so malformed, out-of-range, and offset-normalization
    overflows consistently produce HTTP 400 instead of framework 422 or 500.

    Args:
        value: Raw ISO-8601 query value.
        parameter_name: Query parameter name used in validation errors.

    Returns:
        The equivalent timezone-aware UTC datetime.

    Raises:
        HTTPException: 400 when the value is not a representable ISO datetime.
    """
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{parameter_name} is not a supported ISO-8601 datetime",
        ) from exc


async def _require_matching_wallet_mode(
    repo: Repository,
    wallet_public_id: str,
    mode: str,
    as_of: datetime,
) -> None:
    """Require an active wallet whose paper flag agrees with the requested mode.

    Args:
        repo: Repository providing the active-wallet catalogue.
        wallet_public_id: Scope-checked wallet identifier.
        mode: Validated ``live`` or ``paper`` mode.
        as_of: Current read horizon for the active-wallet lookup.

    Raises:
        HTTPException: 400 when the wallet does not exist at the current horizon
            or its paper flag conflicts with the requested mode.
    """
    wallets = await repo.list_active_wallets(as_of)
    wallet = next((row for row in wallets if row["public_id"] == wallet_public_id), None)
    if wallet is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown wallet_public_id: {wallet_public_id}",
        )
    if wallet["is_paper"] != (mode == "paper"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Wallet {wallet_public_id} is not compatible with mode {mode!r}",
        )


@router.get(
    "/pnl/series",
    responses={
        400: {"description": "Missing or invalid scope/window parameters"},
        500: {"description": _INTERNAL_ERROR_DETAIL},
    },
)
async def get_pnl_series(
    request: Request,
    _auth: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_POSITIONS))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    wallet_public_id: Annotated[str | None, Query(description="Required wallet scope")] = None,
    operator_public_id: Annotated[str | None, Query(description="Optional operator scope")] = None,
    mode: Annotated[str, Query(description="Trading mode scope")] = "live",
    granularity: Annotated[str, Query(description="1m/5m/1h/1d")] = "1m",
    from_time: Annotated[
        str | None, Query(alias="from", description="Inclusive window start (ISO, UTC)")
    ] = None,
    to_time: Annotated[
        str | None, Query(alias="to", description="Inclusive window end (ISO, UTC)")
    ] = None,
) -> PnlSeriesResponse:
    """Reconstruct one wallet/mode scope's Net-P&L-since-activation series.

    Version 1 deliberately serves current truth only. Historical-knowledge
    time travel is out of scope, so the read horizon is captured internally and
    cannot be supplied by the caller.

    Args:
        request: FastAPI request (provides the REST tracker for provenance).
        _auth: Authenticated caller with READ_POSITIONS permission.
        _csrf: CSRF validation (a no-op for GET, kept for parity).
        repo: Database repository.
        wallet_public_id: REQUIRED wallet scope; a single wallet only.
        operator_public_id: Optional operator scope for the authorization check.
        mode: Trading mode scope (``live`` default).
        granularity: One of ``1m`` / ``5m`` / ``1h`` / ``1d``.
        from_time: Required inclusive window start (ISO datetime).
        to_time: Required inclusive window end (ISO datetime).

    Returns:
        A :class:`PnlSeriesResponse` wrapping the decomposed series.

    Raises:
        HTTPException: 400 when a required parameter is missing or the window /
            granularity is invalid; 403 when the wallet is outside the caller's
            accessible set; 500 on an unexpected reconstruction failure.
    """
    if not wallet_public_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="wallet_public_id is required",
        )
    if from_time is None or to_time is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="from and to are required",
        )
    if mode not in _SUPPORTED_MODES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported mode: {mode!r}; expected 'live' or 'paper'",
        )
    if granularity not in _SUPPORTED_GRANULARITIES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported granularity: {granularity!r}",
        )
    window_from = _parse_utc_query_datetime(from_time, "from")
    window_to = _parse_utc_query_datetime(to_time, "to")
    if window_to < window_from:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="to must be greater than or equal to from",
        )
    if window_from < _MIN_SAFE_WINDOW_FROM or window_to > _MAX_SAFE_WINDOW_TO:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Requested window is outside the safely representable timeline range",
        )
    processing_as_of = datetime.now(UTC)
    await resolve_target_wallets(_auth, repo, operator_public_id, wallet_public_id)
    await _require_matching_wallet_mode(repo, wallet_public_id, mode, processing_as_of)
    try:
        result = await build_wallet_pnl_series(
            repo,
            wallet_public_id,
            mode,
            window_from,
            window_to,
            granularity,
            processing_as_of,
        )
        points = [
            PnlTimelinePointData(
                point_time=point.point_time,
                realized_pnl=point.realized_pnl,
                fee_pnl=point.fee_pnl,
                accrual_pnl=point.accrual_pnl,
                unrealized_pnl=point.unrealized_pnl,
                net_pnl=point.net_pnl,
                valuation_status=point.valuation_status,
                per_instrument=[
                    PnlInstrumentContributionData(
                        instrument_public_id=contribution.instrument_public_id,
                        realized_pnl=contribution.realized_pnl,
                        fee_pnl=contribution.fee_pnl,
                        accrual_pnl=contribution.accrual_pnl,
                        unrealized_pnl=contribution.unrealized_pnl,
                    )
                    for contribution in point.per_instrument
                ],
            )
            for point in result.points
        ]
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_PNL_SERIES_STREAM)
        now = datetime.now(UTC)
        payload = PnlSeriesData(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=sid,
            sequence_id=seq,
            wallet_public_id=wallet_public_id,
            mode=mode,
            granularity=result.granularity,
            valuation_ccy=result.valuation_ccy,
            from_time=window_from,
            to_time=window_to,
            as_of=processing_as_of,
            mark_source=PNL_TIMELINE_MARK_SOURCE,
            calc_version=PNL_TIMELINE_CALC_VERSION,
            points=points,
        )
        return PnlSeriesResponse(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=sid,
            sequence_id=seq,
            payload=payload,
        )
    except PnlTimelineWorkBudgetError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        logger.error(f"Failed to build P&L series: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_INTERNAL_ERROR_DETAIL,
        ) from exc
