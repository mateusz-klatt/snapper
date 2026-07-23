"""P&L series and decision-marker timeline read API.

Exposes the pure Net-P&L-since-activation engine over REST. ``GET
/api/portfolio/pnl/series`` reconstructs one ``(wallet, mode)`` scope's series
for a from/to window at a chosen granularity, mirroring the structure of
``position_cycle_routes`` (standalone ``APIRouter`` module, permission-gated
dependency, repository injection, ``SequenceTracker`` provenance from
``request.app.state.rest_tracker``) and the wallet-scoping pattern of
``/api/positions`` (:func:`resolve_target_wallets`). Unlike the list endpoints a
single wallet is REQUIRED — there is no all-wallets P&L aggregation in v1 (the
activation epoch is per wallet/mode).

``GET /api/portfolio/pnl/timeline`` applies the identical scope and reconstruction
rules and augments that series with bounded fill, signal, and AI-decision
markers. Signals and decisions are independent reads so a rejected decision or
one that produced no fill remains visible.
"""

from dataclasses import dataclass
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

from snapper.api.schemas.pnl_timeline import PnlAiDecisionMarkerData
from snapper.api.schemas.pnl_timeline import PnlAttributionContributionData
from snapper.api.schemas.pnl_timeline import PnlFillMarkerData
from snapper.api.schemas.pnl_timeline import PnlFxRateSourceData
from snapper.api.schemas.pnl_timeline import PnlIncompletenessReasonData
from snapper.api.schemas.pnl_timeline import PnlInstrumentContributionData
from snapper.api.schemas.pnl_timeline import PnlSeriesData
from snapper.api.schemas.pnl_timeline import PnlSeriesResponse
from snapper.api.schemas.pnl_timeline import PnlSignalMarkerData
from snapper.api.schemas.pnl_timeline import PnlTimelineData
from snapper.api.schemas.pnl_timeline import PnlTimelineMarkerData
from snapper.api.schemas.pnl_timeline import PnlTimelinePointData
from snapper.api.schemas.pnl_timeline import PnlTimelineResponse
from snapper.application.portfolio.pnl_timeline import PnlTimelineResult
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_CALC_VERSION
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MARK_SOURCE
from snapper.application.portfolio.pnl_timeline_service import PnlFillMarker
from snapper.application.portfolio.pnl_timeline_service import PnlSignalMarker
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineMarker
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_timeline
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

_VALUATION_CCY_LENGTH: Final[int] = 3
"""Required length of an ISO-4217 alphabetic currency code.

Validated by SHAPE rather than against a fixed allowlist: the resolvable set is
whatever our own candle plane can price, and that grows as venues are added. A
well-formed but unpriceable currency is answered with honest withheld points
rather than a 400, so the response still shows everything that IS known."""

_MIN_SAFE_WINDOW_FROM: Final[datetime] = datetime.min.replace(tzinfo=UTC) + timedelta(minutes=1)
"""Earliest start whose leading candle minute is representable."""

_MAX_SAFE_WINDOW_TO: Final[datetime] = datetime.max.replace(tzinfo=UTC) - timedelta(minutes=1)
"""Latest end whose inclusive minute grid can advance without overflow."""

_PNL_SERIES_STREAM: Final[str] = "rest.portfolio_pnl_series"
"""REST provenance stream the series response draws its sequence id from."""

_PNL_TIMELINE_STREAM: Final[str] = "rest.portfolio_pnl_timeline"
"""REST provenance stream for the marker-bearing timeline response."""

_INTERNAL_ERROR_DETAIL: Final[str] = "Failed to build P&L series"

_INTERNAL_TIMELINE_ERROR_DETAIL: Final[str] = "Failed to build P&L timeline"


@dataclass(frozen=True, slots=True)
class _ValidatedTimelineRequest:
    """Validated and scope-authorized reconstruction request values."""

    wallet_public_id: str
    mode: str
    granularity: str
    window_from: datetime
    window_to: datetime
    as_of: datetime
    valuation_ccy: str
    allow_anchor_creation: bool


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
        as_of: Effective read horizon for the active-wallet lookup.

    Raises:
        HTTPException: 400 when the wallet does not exist at the effective horizon
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


async def _validate_timeline_request(
    auth: AuthPrincipal,
    repo: Repository,
    wallet_public_id: str | None,
    operator_public_id: str | None,
    mode: str,
    granularity: str,
    from_time: str | None,
    to_time: str | None,
    as_of: str | None,
    valuation_ccy: str,
) -> _ValidatedTimelineRequest:
    """Validate and authorize the request shared by both P&L endpoints.

    Args:
        auth: Authenticated caller used by wallet-scope resolution.
        repo: Repository providing wallet access and mode metadata.
        wallet_public_id: Required single-wallet scope.
        operator_public_id: Optional operator scope for authorization.
        mode: Requested trading mode.
        granularity: Requested series granularity.
        from_time: Raw inclusive ISO window start.
        to_time: Raw inclusive ISO window end.
        as_of: Optional raw ISO knowledge horizon.
        valuation_ccy: Currency the series is expressed in.

    Returns:
        Normalized UTC values and one effective read horizon shared by all reads.

    Raises:
        HTTPException: 400 for invalid scope/window values or 403 when wallet
            authorization fails.
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
    normalized_ccy = valuation_ccy.strip().upper()
    if len(normalized_ccy) != _VALUATION_CCY_LENGTH or not normalized_ccy.isalpha():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported valuation_ccy: {valuation_ccy!r}; expected a 3-letter code",
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
    effective_as_of = (
        datetime.now(UTC) if as_of is None else _parse_utc_query_datetime(as_of, "as_of")
    )
    if window_to < window_from:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="to must be greater than or equal to from",
        )
    if (
        window_from < _MIN_SAFE_WINDOW_FROM
        or window_to > _MAX_SAFE_WINDOW_TO
        or effective_as_of < _MIN_SAFE_WINDOW_FROM
        or effective_as_of > _MAX_SAFE_WINDOW_TO
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Requested window or as_of is outside the safely representable timeline range",
        )
    if window_to > effective_as_of:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="to must be less than or equal to as_of; shorten the window or move as_of forward",
        )
    await resolve_target_wallets(auth, repo, operator_public_id, wallet_public_id)
    await _require_matching_wallet_mode(repo, wallet_public_id, mode, effective_as_of)
    return _ValidatedTimelineRequest(
        wallet_public_id=wallet_public_id,
        mode=mode,
        granularity=granularity,
        window_from=window_from,
        window_to=window_to,
        as_of=effective_as_of,
        valuation_ccy=normalized_ccy,
        allow_anchor_creation=as_of is None,
    )


def _point_data(result: PnlTimelineResult) -> list[PnlTimelinePointData]:
    """Project the pure series result into strict transport point models."""
    return [
        PnlTimelinePointData(
            point_time=point.point_time,
            realized_pnl=point.realized_pnl,
            fee_pnl=point.fee_pnl,
            accrual_pnl=point.accrual_pnl,
            unrealized_pnl=point.unrealized_pnl,
            net_pnl=point.net_pnl,
            valuation_status=point.valuation_status,
            incompleteness_reasons=[
                PnlIncompletenessReasonData(
                    reason=entry.reason,
                    withholding_tier=entry.withholding_tier,
                    withholding_scope=entry.withholding_scope,
                    trigger_instrument_public_id=entry.trigger_instrument_public_id,
                )
                for entry in point.incompleteness_reasons
            ],
            per_instrument=[
                PnlInstrumentContributionData(
                    instrument_public_id=contribution.instrument_public_id,
                    native_symbol=contribution.native_symbol,
                    exchange=contribution.exchange,
                    realized_pnl=contribution.realized_pnl,
                    fee_pnl=contribution.fee_pnl,
                    accrual_pnl=contribution.accrual_pnl,
                    unrealized_pnl=contribution.unrealized_pnl,
                )
                for contribution in point.per_instrument
            ],
            attribution=[
                PnlAttributionContributionData(
                    origin=contribution.origin,
                    strategy_name=contribution.strategy_name,
                    realized_pnl=contribution.realized_pnl,
                    fee_pnl=contribution.fee_pnl,
                    accrual_pnl=contribution.accrual_pnl,
                    unrealized_pnl=contribution.unrealized_pnl,
                )
                for contribution in point.attribution
            ],
        )
        for point in result.points
    ]


def _marker_data(marker: PnlTimelineMarker) -> PnlTimelineMarkerData:
    """Project one service marker into its discriminated transport model."""
    if isinstance(marker, PnlFillMarker):
        return PnlFillMarkerData(
            marker_time=marker.marker_time,
            instrument_public_id=marker.instrument_public_id,
            side=marker.side,
            size=marker.size,
            price=marker.price,
            execution_public_id=marker.execution_public_id,
            order_public_id=marker.order_public_id,
            outcome=marker.outcome,
            status=marker.status,
        )
    if isinstance(marker, PnlSignalMarker):
        return PnlSignalMarkerData(
            marker_time=marker.marker_time,
            instrument_public_id=marker.instrument_public_id,
            side=marker.side,
            strategy_name=marker.strategy_name,
            strength=marker.strength,
            reason=marker.reason,
            price=marker.price,
            signal_public_id=marker.signal_public_id,
            outcome=marker.outcome,
            status=marker.status,
        )
    return PnlAiDecisionMarkerData(
        marker_time=marker.marker_time,
        instrument_public_id=marker.instrument_public_id,
        strategy_public_id=marker.strategy_public_id,
        review_public_id=marker.review_public_id,
        event_public_id=marker.event_public_id,
        decision=marker.decision,
        rationale=marker.rationale,
        outcome=marker.outcome,
        status=marker.status,
    )


@router.get(
    "/pnl/series",
    responses={
        400: {"description": "Missing or invalid scope/window/horizon parameters"},
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
    as_of: Annotated[
        str | None,
        Query(description="Knowledge horizon for the bitemporal read (ISO, UTC)"),
    ] = None,
    valuation_ccy: Annotated[
        str, Query(description="Currency the series is valued in (3-letter code)")
    ] = "USD",
) -> PnlSeriesResponse:
    """Reconstruct one wallet/mode scope's Net-P&L-since-activation series.

    The optional ``as_of`` value selects the historical knowledge horizon. When
    omitted, one current UTC horizon is captured and shared by every input read.

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
        as_of: Optional bitemporal knowledge horizon (ISO datetime).
        valuation_ccy: Currency to value the series in, defaulting to USD. A scope
            trading natively in another currency resolves fully when asked for in
            that currency, since its marks and flows are then already native.

    Returns:
        A :class:`PnlSeriesResponse` wrapping the decomposed series.

    Raises:
        HTTPException: 400 when a required parameter is missing or the window,
            horizon, or granularity is invalid; 403 when the wallet is outside
            the caller's accessible set; 500 on an unexpected reconstruction
            failure.
    """
    validated = await _validate_timeline_request(
        _auth,
        repo,
        wallet_public_id,
        operator_public_id,
        mode,
        granularity,
        from_time,
        to_time,
        as_of,
        valuation_ccy,
    )
    try:
        result = await build_wallet_pnl_series(
            repo,
            validated.wallet_public_id,
            validated.mode,
            validated.window_from,
            validated.window_to,
            validated.granularity,
            validated.as_of,
            validated.valuation_ccy,
            allow_anchor_creation=validated.allow_anchor_creation,
        )
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_PNL_SERIES_STREAM)
        now = datetime.now(UTC)
        payload = PnlSeriesData(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=sid,
            sequence_id=seq,
            wallet_public_id=validated.wallet_public_id,
            mode=validated.mode,
            granularity=result.granularity,
            valuation_ccy=result.valuation_ccy,
            from_time=validated.window_from,
            to_time=validated.window_to,
            as_of=validated.as_of,
            mark_source=PNL_TIMELINE_MARK_SOURCE,
            rate_sources=[
                PnlFxRateSourceData(
                    source_currency=source.source_currency,
                    valuation_currency=source.valuation_currency,
                    base_currency=source.base_currency,
                    quote_currency=source.quote_currency,
                    exchange=source.exchange,
                )
                for source in result.rate_sources
            ],
            calc_version=PNL_TIMELINE_CALC_VERSION,
            points=_point_data(result),
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


@router.get(
    "/pnl/timeline",
    responses={
        400: {"description": "Missing or invalid scope/window/horizon parameters"},
        500: {"description": _INTERNAL_TIMELINE_ERROR_DETAIL},
    },
)
async def get_pnl_timeline(
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
    as_of: Annotated[
        str | None,
        Query(description="Knowledge horizon for the bitemporal read (ISO, UTC)"),
    ] = None,
    valuation_ccy: Annotated[
        str, Query(description="Currency the series is valued in (3-letter code)")
    ] = "USD",
) -> PnlTimelineResponse:
    """Reconstruct a wallet P&L series with bounded decision markers.

    The endpoint uses the same required wallet, mode consistency,
    authorization, safe-window, granularity, and total-work rules as the
    series-only endpoint. Its effective ``as_of`` horizon is passed unchanged
    through the series, signal, and AI-event reads.

    Args:
        request: FastAPI request providing REST sequence provenance.
        _auth: Authenticated caller with READ_POSITIONS permission.
        _csrf: CSRF validation dependency retained for GET parity.
        repo: Database repository.
        wallet_public_id: Required single-wallet scope.
        operator_public_id: Optional operator scope for authorization.
        mode: Trading mode scope.
        granularity: Requested P&L point granularity.
        from_time: Required inclusive ISO window start.
        to_time: Required inclusive ISO window end.
        as_of: Optional bitemporal knowledge horizon (ISO datetime).
        valuation_ccy: Currency to value the series in, defaulting to USD.

    Returns:
        A flat :class:`PnlTimelineResponse` with series fields and markers.

    Raises:
        HTTPException: 400 for invalid scope/window/horizon or excessive work,
            403 for inaccessible wallet scope, and 500 for unexpected read
            failures.
    """
    validated = await _validate_timeline_request(
        _auth,
        repo,
        wallet_public_id,
        operator_public_id,
        mode,
        granularity,
        from_time,
        to_time,
        as_of,
        valuation_ccy,
    )
    try:
        result = await build_wallet_pnl_timeline(
            repo,
            validated.wallet_public_id,
            validated.mode,
            validated.window_from,
            validated.window_to,
            validated.granularity,
            validated.as_of,
            validated.valuation_ccy,
            allow_anchor_creation=validated.allow_anchor_creation,
        )
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_PNL_TIMELINE_STREAM)
        now = datetime.now(UTC)
        payload = PnlTimelineData(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=sid,
            sequence_id=seq,
            wallet_public_id=validated.wallet_public_id,
            mode=validated.mode,
            granularity=result.series.granularity,
            valuation_ccy=result.series.valuation_ccy,
            from_time=validated.window_from,
            to_time=validated.window_to,
            as_of=validated.as_of,
            mark_source=PNL_TIMELINE_MARK_SOURCE,
            rate_sources=[
                PnlFxRateSourceData(
                    source_currency=source.source_currency,
                    valuation_currency=source.valuation_currency,
                    base_currency=source.base_currency,
                    quote_currency=source.quote_currency,
                    exchange=source.exchange,
                )
                for source in result.series.rate_sources
            ],
            calc_version=PNL_TIMELINE_CALC_VERSION,
            points=_point_data(result.series),
            marker_limit=result.marker_limit,
            markers_truncated=result.markers_truncated,
            markers=[_marker_data(marker) for marker in result.markers],
        )
        return PnlTimelineResponse(
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
        logger.error(f"Failed to build P&L timeline: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=_INTERNAL_TIMELINE_ERROR_DETAIL,
        ) from exc
