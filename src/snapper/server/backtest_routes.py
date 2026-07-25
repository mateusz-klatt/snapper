"""Backtest REST API routes.

Provides CRUD endpoints for backtest runs: create, list, detail,
cancel, rerun, trades, signals, and events. All reads accept ``as_of``
for temporal queries and are wallet-scoped via auth context.
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from typing import cast
from uuid import UUID
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import status
from loguru import logger
from sqlalchemy.exc import IntegrityError as SqlIntegrityError

from snapper.api.schemas.backtest import BacktestCancelCommand
from snapper.api.schemas.backtest import BacktestCompareRequest
from snapper.api.schemas.backtest import BacktestComparisonData
from snapper.api.schemas.backtest import BacktestComparisonDetailResponse
from snapper.api.schemas.backtest import BacktestComparisonDetailResponseData
from snapper.api.schemas.backtest import BacktestComparisonListResponse
from snapper.api.schemas.backtest import BacktestComparisonResponse
from snapper.api.schemas.backtest import BacktestCreateBody
from snapper.api.schemas.backtest import BacktestCreateCommand
from snapper.api.schemas.backtest import BacktestEquityPointInline
from snapper.api.schemas.backtest import BacktestEquityPointListResponse
from snapper.api.schemas.backtest import BacktestEventData
from snapper.api.schemas.backtest import BacktestEventListResponse
from snapper.api.schemas.backtest import BacktestResultInline
from snapper.api.schemas.backtest import BacktestRunData
from snapper.api.schemas.backtest import BacktestRunDetailData
from snapper.api.schemas.backtest import BacktestRunDetailResponse
from snapper.api.schemas.backtest import BacktestRunListResponse
from snapper.api.schemas.backtest import BacktestRunResponse
from snapper.api.schemas.backtest import BacktestSignalData
from snapper.api.schemas.backtest import BacktestSignalListResponse
from snapper.api.schemas.backtest import BacktestStrategyClassListResponse
from snapper.api.schemas.backtest import BacktestTradeData
from snapper.api.schemas.backtest import BacktestTradeListResponse
from snapper.api.schemas.backtest import EquityOverlayPoint
from snapper.api.schemas.backtest import MetricDiffRow
from snapper.api.schemas.backtest import SignalDiffEntry
from snapper.api.schemas.backtest import TradeDiffEntry
from snapper.application.backtest.compare import compute_equity_overlay
from snapper.application.backtest.compare import compute_metrics_diff
from snapper.application.backtest.compare import compute_signals_diff
from snapper.application.backtest.compare import compute_trades_diff
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.config import BacktestExecutionMode
from snapper.application.backtest.config import BacktestFillModel
from snapper.application.backtest.config import compute_fingerprint
from snapper.application.backtest.metrics import PROMOTED_METRIC_NAMES
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.settings import get_settings
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.repository import Repository
from snapper.data.repository_types import BacktestComparisonRow
from snapper.data.repository_types import BacktestResultRow
from snapper.data.repository_types import BacktestRunRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.scoping import ACTIVE_WALLET_REQUIRED_DETAIL
from snapper.server.scoping import require_tradable_active_wallet
from snapper.server.scoping import resolve_readable_active_wallet
from snapper.strategies.factory import StrategyFactory

router = APIRouter(prefix="/backtests", tags=["backtests"])

_REST_STREAM = "backtest_rest"
_CANCELLABLE_STATUSES = frozenset({"pending", "running"})
_NOT_FOUND = "Backtest run not found"
_BACKTEST_REQUEST_CONFLICT = "Backtest request conflict"
_BACKTEST_REQUEST_FAILED = "Backtest request failed"
_COMPARISON_OR_RUN_NOT_FOUND = "Comparison or run not found"


def _bt_repo(repo: Repository) -> BacktestRepository:
    """Build BacktestRepository from the shared session factory."""
    return BacktestRepository(cast(Any, repo).session_factory)


def _resolve_as_of(as_of: datetime | None) -> datetime:
    """Fall back to now() when no as_of is provided."""
    return as_of if as_of is not None else datetime.now(UTC)


def _run_to_data(
    run: BacktestRunRow,
    sid: str,
    seq: int,
) -> BacktestRunData:
    """Project a BacktestRunRow into the lightweight list/event response schema."""
    return BacktestRunData(
        type="backtest_run",
        public_id=run["public_id"],
        timestamp=run["timestamp"],
        session_id=sid,
        sequence_id=seq,
        wallet_public_id=run["wallet_public_id"],
        strategy_name=run["strategy_name"],
        strategy_params=run["strategy_params"],
        instrument_public_id=run["instrument_public_id"],
        instrument=run.get("instrument"),
        exchange=run["exchange"],
        timeframe=run["timeframe"],
        start_date=run["start_date"],
        end_date=run["end_date"],
        initial_cash=run["initial_cash"],
        status=run["status"],
        execution_mode=run["execution_mode"],
        fill_model=run["fill_model"],
        slippage_bps=run["slippage_bps"],
        commission_bps=run["commission_bps"],
        config_hash=run.get("config_hash"),
        target_execution_exchange=run.get("target_execution_exchange"),
        started_at=run.get("started_at"),
        completed_at=run.get("completed_at"),
        error=run.get("error"),
    )


def _run_to_detail_data(
    run: BacktestRunRow,
    sid: str,
    seq: int,
    result: BacktestResultInline | None,
) -> BacktestRunDetailData:
    """Project a BacktestRunRow into the detail schema with optional inline result."""
    return BacktestRunDetailData(
        type="backtest_run",
        public_id=run["public_id"],
        timestamp=run["timestamp"],
        session_id=sid,
        sequence_id=seq,
        wallet_public_id=run["wallet_public_id"],
        strategy_name=run["strategy_name"],
        strategy_params=run["strategy_params"],
        instrument_public_id=run["instrument_public_id"],
        instrument=run.get("instrument"),
        exchange=run["exchange"],
        timeframe=run["timeframe"],
        start_date=run["start_date"],
        end_date=run["end_date"],
        initial_cash=run["initial_cash"],
        status=run["status"],
        execution_mode=run["execution_mode"],
        fill_model=run["fill_model"],
        slippage_bps=run["slippage_bps"],
        commission_bps=run["commission_bps"],
        config_hash=run.get("config_hash"),
        target_execution_exchange=run.get("target_execution_exchange"),
        started_at=run.get("started_at"),
        completed_at=run.get("completed_at"),
        error=run.get("error"),
        result=result,
    )


_NO_ACTIVE_WALLET = ACTIVE_WALLET_REQUIRED_DETAIL
_NO_ACTIVE_WALLET_RESPONSE: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET}
}
_BACKTEST_NOT_FOUND_RESPONSE: dict[int | str, dict[str, Any]] = {404: {"description": _NOT_FOUND}}
_BACKTEST_CONFLICT_RESPONSE: dict[int | str, dict[str, Any]] = {
    409: {"description": _BACKTEST_REQUEST_CONFLICT}
}
_BACKTEST_VALIDATION_RESPONSE: dict[int | str, dict[str, Any]] = {
    422: {"description": "Backtest request validation failed"}
}
_BACKTEST_SERVER_ERROR_RESPONSE: dict[int | str, dict[str, Any]] = {
    500: {"description": _BACKTEST_REQUEST_FAILED}
}
_COMPARISON_NOT_FOUND_RESPONSE: dict[int | str, dict[str, Any]] = {
    404: {"description": _COMPARISON_OR_RUN_NOT_FOUND}
}
_BACKTEST_CREATE_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET},
    500: {"description": _BACKTEST_REQUEST_FAILED},
}
_BACKTEST_LIST_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET}
}
_BACKTEST_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET},
    404: {"description": _NOT_FOUND},
}
_BACKTEST_CANCEL_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET},
    404: {"description": _NOT_FOUND},
    409: {"description": _BACKTEST_REQUEST_CONFLICT},
    500: {"description": _BACKTEST_REQUEST_FAILED},
}
_BACKTEST_COMPARISON_READ_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET},
    404: {"description": _COMPARISON_OR_RUN_NOT_FOUND},
}
_BACKTEST_COMPARISON_WRITE_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": _NO_ACTIVE_WALLET},
    404: {"description": _COMPARISON_OR_RUN_NOT_FOUND},
    409: {"description": _BACKTEST_REQUEST_CONFLICT},
    422: {"description": "Backtest request validation failed"},
    500: {"description": _BACKTEST_REQUEST_FAILED},
}


def _enforce_run_wallet(wallet_public_id: str, run: BacktestRunRow | dict[str, Any]) -> None:
    """Reject a run owned by a wallet other than the resolved one.

    Split out of :func:`_enforce_wallet_scope` so the WRITE routes can
    reuse the ownership half against a wallet that
    :func:`snapper.server.scoping.require_tradable_active_wallet` has
    already proven tradable, instead of re-reading the raw
    ``active_wallet_public_id`` claim (which certifies visibility only).

    Args:
        wallet_public_id: Wallet the caller is scoped to for this request.
        run: Backtest run row whose owning wallet is compared.

    Raises:
        HTTPException: 404 when the run belongs to another wallet, so a
            cross-tenant run's existence is never confirmed.
    """
    if run["wallet_public_id"] != wallet_public_id:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)


async def _enforce_wallet_scope(
    principal: AuthPrincipal,
    repo: Repository,
    run: BacktestRunRow | dict[str, Any],
) -> None:
    """Fail-closed wallet scope check for backtest READ endpoints.

    The old truthy guard
    ``if principal.active_wallet_public_id and run[...]!=...`` becomes
    a no-op when the wallet claim is cleared to ``None``. Every
    backtest read must instead fail-closed on no-active-wallet (400)
    and 404 on cross-tenant mismatch to match the
    ``create_backtest`` guard's shape.

    Reads resolve the claim through the READ plane on every request
    (:func:`snapper.server.scoping.resolve_readable_active_wallet`) —
    the right plane, because a personal read grant conferring read
    visibility is the intended behaviour, and the right time, because
    the claim is minted once and carried forward across refreshes, so
    consuming it as minted would keep a revoked read grant alive
    indefinitely. WRITE routes must NOT come through here — they take
    the wallet from
    :func:`snapper.server.scoping.require_tradable_active_wallet` and
    call :func:`_enforce_run_wallet` with it.

    Args:
        principal: Authenticated caller carrying the wallet claim.
        repo: Repository used for the read-plane re-resolution.
        run: Backtest run row whose owning wallet is compared.

    Raises:
        HTTPException: 400 when no wallet is selected, 403 when the
            claimed wallet is no longer readable, 404 when the run
            belongs to another wallet.
    """
    wallet_public_id = await resolve_readable_active_wallet(principal, repo)
    _enforce_run_wallet(wallet_public_id, run)


def _project_inline_result(result_row: BacktestResultRow) -> BacktestResultInline:
    """Build the detail-view inline result with typed-column precedence.

    For the 5 promoted metric names
    (``sortino_ratio``, ``cagr``, ``calmar_ratio``, ``expectancy``
    ``avg_trade_pnl``), the typed column takes precedence over any value
    the pre-0005 writer left inside ``extra_metrics``. The test is
    explicit ``is not None`` to preserve a legitimate ``0.0`` typed
    value against a stale non-zero JSON fallback. The 3 new metrics
    (``max_drawdown_duration_seconds``, ``exposure_ratio``
    ``turnover_ratio``) have no JSON fallback — pre-0005 rows simply
    emit ``None``.
    Response ``extra_metrics`` strips the 5 promoted names so a pre-0005
    row never emits them twice (once in the typed slot, once via the
    unfiltered JSON blob). Parallel to the comparison-diff set
    subtraction that uses the same ``PROMOTED_METRIC_NAMES`` source of
    truth. Defensive against a post-0005 writer mistakenly including
    promoted keys in ``extra_metrics``.
    """
    raw_extra = result_row.get("extra_metrics") or {}
    filtered_extra = {k: v for k, v in raw_extra.items() if k not in PROMOTED_METRIC_NAMES}

    def _promoted(name: str) -> float | None:
        typed = cast(Any, result_row).get(name)
        if isinstance(typed, int | float):
            return float(typed)
        fallback = raw_extra.get(name)
        return float(fallback) if isinstance(fallback, int | float) else None

    return BacktestResultInline(
        total_trades=result_row["total_trades"],
        winning_trades=result_row["winning_trades"],
        losing_trades=result_row["losing_trades"],
        total_pnl=result_row["total_pnl"],
        max_drawdown=result_row["max_drawdown"],
        sharpe_ratio=result_row.get("sharpe_ratio"),
        win_rate=result_row.get("win_rate"),
        profit_factor=result_row.get("profit_factor"),
        final_equity=result_row["final_equity"],
        max_equity=result_row["max_equity"],
        sortino_ratio=_promoted("sortino_ratio"),
        cagr=_promoted("cagr"),
        calmar_ratio=_promoted("calmar_ratio"),
        expectancy=_promoted("expectancy"),
        avg_trade_pnl=_promoted("avg_trade_pnl"),
        max_drawdown_duration_seconds=result_row.get("max_drawdown_duration_seconds"),
        exposure_ratio=result_row.get("exposure_ratio"),
        turnover_ratio=result_row.get("turnover_ratio"),
        extra_metrics=filtered_extra,
    )


def _get_process_factory(request: Request) -> ProcessLauncherService:
    """Retrieve ProcessLauncherService from app state."""
    return cast(ProcessLauncherService, request.app.state.process_factory)


async def _resolve_backtest_instrument(
    repo: Repository,
    instrument_ref: str,
    exchange: str,
    as_of: datetime,
) -> str:
    """Resolve a backtest request's instrument reference to an instrument public_id.

    The create form sends a native symbol (for example ``EUR-USD``) in the
    ``instrument_public_id`` field, mirroring order entry. Resolve it to the
    instrument's public_id so the value lands in the ``UUID``-typed
    ``backtest_runs.instrument_public_id`` column (Postgres rejects a raw symbol
    there with a ``DataError`` that surfaces as 500) AND so the join in
    ``get_run`` resolves the native ticker the runner needs for candle lookup.

    A reference that is already a well-formed UUID (the rerun path may replay a
    stored public_id) is accepted only after confirming it resolves to an active
    instrument. ``get_symbol_for_instrument`` is called only for UUID-shaped input
    because comparing the UUID column to a non-UUID string would itself raise a
    ``DataError`` on Postgres.

    Args:
        repo: Database repository.
        instrument_ref: Native symbol or instrument public_id from the request.
        exchange: Feed-source exchange the instrument belongs to.
        as_of: Point-in-time for the temporal instrument lookup.

    Returns:
        The resolved instrument public_id.

    Raises:
        HTTPException: 422 when the reference resolves to no active instrument.
    """
    resolved = await repo.get_instrument_public_id_by_symbol(
        native_symbol=instrument_ref,
        exchange=exchange,
        as_of=as_of,
    )
    if resolved is not None:
        return resolved
    try:
        normalized = str(UUID(str(instrument_ref)))
    except (ValueError, AttributeError, TypeError):
        raise _unknown_instrument(instrument_ref, exchange) from None
    symbol = await repo.get_symbol_for_instrument(
        instrument_public_id=normalized,
        as_of=as_of,
    )
    if symbol is None:
        raise _unknown_instrument(instrument_ref, exchange)
    on_exchange = await repo.get_instrument_public_id_by_symbol(
        native_symbol=symbol,
        exchange=exchange,
        as_of=as_of,
    )
    if on_exchange != normalized:
        raise _unknown_instrument(instrument_ref, exchange)
    return normalized


def _unknown_instrument(instrument_ref: str, exchange: str) -> HTTPException:
    """Build the 422 raised when a backtest instrument reference does not resolve."""
    return HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail={
            "error_code": "unknown_instrument",
            "symbol": instrument_ref,
            "exchange": exchange,
            "reason": (
                "no active instrument resolves for this (symbol, exchange) pair; "
                "enter a valid instrument symbol for the selected exchange"
            ),
        },
    )


@router.post(
    "",
    openapi_extra=openapi_schema(BacktestCreateCommand),
    dependencies=[Depends(validate_csrf_token)],
    responses=_BACKTEST_CREATE_RESPONSES,
)
async def create_backtest(
    request: Request,
    command: Annotated[BacktestCreateCommand, Depends(json_body(BacktestCreateCommand))],
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    wallet_id: Annotated[str, Depends(require_tradable_active_wallet)],
) -> BacktestRunResponse:
    """Create and launch a new backtest run.

    Args:
        request: FastAPI request.
        command: Validated create command envelope.
        principal: Authenticated caller with MANAGE_BACKTESTS.
        repo: Database repository.
        wallet_id: Active wallet re-resolved through the trade plane, so
            a wallet the caller may only READ cannot own a new run.

    Returns:
        Created backtest run response.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    settings = get_settings()

    run_public_id = str(uuid7())
    process_name = f"backtest_runner_{run_public_id}"

    body = command.payload
    instrument_public_id = await _resolve_backtest_instrument(
        repo, body.instrument_public_id, body.exchange, now
    )
    try:
        pairing_config = BacktestConfig(
            strategy_class=body.strategy_class,
            instruments={body.exchange: [instrument_public_id]},
            start_date=body.start_date,
            end_date=body.end_date,
            wallet_public_id=wallet_id,
            operator_public_id=principal.primary_operator_public_id or None,
            execution_mode=BacktestExecutionMode(body.execution_mode),
            initial_balance=body.initial_cash,
            strategy_params=dict(body.strategy_params),
            timeframe=body.timeframe,
            fill_model=BacktestFillModel(body.fill_model),
            slippage_bps=body.slippage_bps,
            commission_bps=body.commission_bps,
            target_execution_exchange=body.target_execution_exchange,
        )
        config_hash = compute_fingerprint(pairing_config, for_pairing=True)
    except Exception as exc:
        logger.warning("config_hash computation failed: {} — storing NULL", exc)
        config_hash = None
    _, public_id = await bt_repo.create_run(
        row={
            "wallet_public_id": wallet_id,
            "operator_public_id": principal.primary_operator_public_id or None,
            "strategy_name": body.strategy_class,
            "strategy_params": dict(body.strategy_params),
            "instrument_public_id": instrument_public_id,
            "exchange": body.exchange,
            "timeframe": body.timeframe,
            "start_date": body.start_date,
            "end_date": body.end_date,
            "initial_cash": body.initial_cash,
            "status": "pending",
            "execution_mode": body.execution_mode,
            "fill_model": body.fill_model,
            "slippage_bps": body.slippage_bps,
            "commission_bps": body.commission_bps,
            "config_hash": config_hash,
            "target_execution_exchange": body.target_execution_exchange,
            "created_by_user_id": principal.username,
            "process_name": process_name,
            "session_id": sid,
            "sequence_id": seq,
            "timestamp": now,
        },
        bus_time=now,
        session_id=sid,
        sequence_id=seq,
    )

    config = ProcessConfigModel(
        name=process_name,
        enabled=True,
        mode=ProcessModeEnum.THREAD,
        class_path="snapper.application.backtest.runner.BacktestRunnerProcess",
        method="start",
        parameters={
            "run_public_id": public_id,
            "db_url": settings.db_url,
        },
        lifecycle=ProcessLifecycleEnum.ONE_SHOT,
        role=ProcessRoleEnum.BACKTEST,
        tags=("backtest",),
    )
    factory = _get_process_factory(request)
    try:
        await factory.start_process(config)
    except Exception as exc:
        logger.error("Failed to launch backtest runner {}: {}", public_id[:8], exc)
        await bt_repo.update_run_status(
            public_id=public_id,
            new_status="failed",
            bus_time=datetime.now(UTC),
            session_id=sid,
            sequence_id=tracker.next_sequence(_REST_STREAM),
            error=f"Launch failed: {str(exc)[:512]}",
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to launch backtest: {str(exc)[:256]}",
        ) from exc

    run = await bt_repo.get_run(public_id, as_of=datetime.now(UTC))
    if run is None:
        raise HTTPException(status_code=500, detail="Run created but not found")

    return BacktestRunResponse(
        type="backtest_run_response",
        public_id=str(uuid7()),
        timestamp=now,
        session_id=sid,
        sequence_id=seq,
        payload=_run_to_data(run, sid, seq),
    )


@router.get("", responses=_BACKTEST_LIST_RESPONSES)
async def list_backtests(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    strategy: Annotated[str | None, Query(description="Filter by strategy name")] = None,
    run_status: Annotated[str | None, Query(alias="status", description="Filter by status")] = None,
    config_hash: Annotated[
        str | None,
        Query(
            description="Pairing-stable config-hash filter (64-hex SHA-256)",
            pattern=r"^[0-9a-f]{64}$",
        ),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> BacktestRunListResponse:
    """List backtest runs with optional filters.

    Args:
        request: FastAPI request.
        principal: Authenticated caller with READ_BACKTESTS.
        repo: Database repository.
        as_of: Temporal query parameter.
        strategy: Optional strategy filter.
        run_status: Optional status filter.
        config_hash: pairing-stable SHA-256 filter used by
            the auto-pair UI to fetch sibling runs.
        limit: Page size.
        offset: Page offset.

    Returns:
        List of backtest runs.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)

    wallet_id = await resolve_readable_active_wallet(principal, repo)
    runs = await bt_repo.list_runs(
        as_of=ts,
        wallet_public_id=wallet_id,
        strategy=strategy,
        status=run_status,
        config_hash=config_hash,
        limit=limit,
        offset=offset,
    )
    items = [_run_to_data(r, sid, seq) for r in runs]
    return BacktestRunListResponse(
        type="backtest_run_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=items,
        count=len(items),
    )


@router.get("/strategy-classes")
async def list_strategy_classes(
    request: Request,
    _principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
) -> BacktestStrategyClassListResponse:
    """List registered strategy-class identifiers valid for backtest creation.

    Returns the sorted keys of the in-memory ``StrategyFactory`` registry —
    the only values accepted by ``BacktestCreateBody.strategy_class``. The
    create-backtest UI calls this to populate its strategy dropdown so the
    options stay in lockstep with the create-time validator.

    Declared ahead of the dynamic ``/{run_id}`` route so the static path is
    not captured as a run identifier.

    Args:
        request: FastAPI request carrying the REST sequence tracker.
        _principal: Authenticated caller with READ_BACKTESTS.

    Returns:
        Sorted registered strategy-class names.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    names = sorted(StrategyFactory.STRATEGY_CLASSES.keys())
    return BacktestStrategyClassListResponse(
        type="backtest_strategy_class_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=names,
        count=len(names),
    )


_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})


def _normalise_pair(a: str, b: str) -> tuple[str, str]:
    """Lexical (min, max) normalisation so (A,B) == (B,A)."""
    return (a, b) if a < b else (b, a)


async def _resolve_auto_pair(
    bt_repo: BacktestRepository,
    wallet_id: str,
    config_hash: str,
    anchor: str | None,
    ts: datetime,
) -> tuple[str, str]:
    """Resolve the (run_a, run_b) pair for auto-mode.

    With an anchor: validate anchor belongs to caller's wallet, is
    terminal, has the requested hash. Pair it with the most-recent
    OTHER run matching the hash, preferring the opposite
    ``execution_mode`` (Direct-DB anchor → pick ZMQ-replay counterpart
    if available; same for the reverse). Falls back to any
    most-recent-OTHER when no opposite-mode candidate exists. Without
    an anchor: pair the two most-recent terminal runs, preferring one
    Direct-DB plus one ZMQ-replay (the
    "cross-execution-mode when available" contract). Falls back to the
    two most-recent terminal runs when only one mode is present.
    """
    candidates = await bt_repo.list_runs(
        as_of=ts,
        wallet_public_id=wallet_id,
        config_hash=config_hash,
        limit=50,
    )
    terminal = [r for r in candidates if r["status"] in _TERMINAL_STATUSES]
    if anchor is not None:
        anchor_row = next((r for r in candidates if r["public_id"] == anchor), None)
        if anchor_row is None:
            raise HTTPException(status_code=404, detail="anchor run not found")
        if anchor_row["status"] not in _TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail="anchor run is not terminal")
        if anchor_row.get("config_hash") is None:
            raise HTTPException(status_code=409, detail="anchor run has no config_hash")
        if anchor_row.get("config_hash") != config_hash:
            raise HTTPException(status_code=422, detail="anchor run config_hash mismatches request")
        others = [r for r in terminal if r["public_id"] != anchor]
        if not others:
            raise HTTPException(status_code=409, detail="anchor has no counterpart")
        anchor_mode = anchor_row.get("execution_mode")
        opposite = [r for r in others if r.get("execution_mode") != anchor_mode]
        counterpart = opposite[0] if opposite else others[0]
        return anchor_row["public_id"], counterpart["public_id"]
    if len(terminal) < 2:
        raise HTTPException(
            status_code=409, detail=f"not enough runs with config_hash={config_hash}"
        )
    head_mode = terminal[0].get("execution_mode")
    cross_mode = next(
        (r for r in terminal[1:] if r.get("execution_mode") != head_mode),
        None,
    )
    partner = cross_mode if cross_mode is not None else terminal[1]
    return terminal[0]["public_id"], partner["public_id"]


async def _resolve_manual_pair(
    bt_repo: BacktestRepository,
    wallet_id: str,
    run_a_id: str,
    run_b_id: str,
    ts: datetime,
) -> tuple[str, str, str | None]:
    """Validate both legs of a manual pair and return normalised ids + hash.

    Returns (a_id, b_id, config_hash) where config_hash is populated
    only when both legs share the same non-null hash (so manual
    cross-config pairs persist with ``config_hash=NULL`` and stay
    reachable only by run-id).
    """
    if run_a_id == run_b_id:
        raise HTTPException(status_code=422, detail="cannot compare a run with itself")
    a = await bt_repo.get_run(run_a_id, as_of=ts)
    b = await bt_repo.get_run(run_b_id, as_of=ts)
    if (
        a is None
        or b is None
        or a["wallet_public_id"] != wallet_id
        or b["wallet_public_id"] != wallet_id
    ):
        raise HTTPException(status_code=404, detail="run not found")
    if a["status"] not in _TERMINAL_STATUSES or b["status"] not in _TERMINAL_STATUSES:
        raise HTTPException(status_code=409, detail="both runs must be terminal")
    hash_a = a.get("config_hash")
    hash_b = b.get("config_hash")
    shared_hash = hash_a if hash_a is not None and hash_a == hash_b else None
    return a["public_id"], b["public_id"], shared_hash


def _to_comparison_data(row: BacktestComparisonRow) -> BacktestComparisonData:
    """Project a BacktestComparisonRow into the API schema."""
    return BacktestComparisonData(
        type="backtest_comparison",
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        wallet_public_id=row["wallet_public_id"],
        run_a_public_id=row["run_a_public_id"],
        run_b_public_id=row["run_b_public_id"],
        config_hash=row.get("config_hash"),
        pairing_mode=row["pairing_mode"],
        anchor_run_public_id=row.get("anchor_run_public_id"),
    )


@router.post(
    "/compare",
    openapi_extra=openapi_schema(BacktestCompareRequest),
    dependencies=[Depends(validate_csrf_token)],
    responses=_BACKTEST_COMPARISON_WRITE_RESPONSES,
)
async def create_comparison(
    request: Request,
    command: Annotated[BacktestCompareRequest, Depends(json_body(BacktestCompareRequest))],
    principal: Annotated[
        AuthPrincipal,
        Depends(require_permission(Permission.CREATE_BACKTEST_COMPARISONS)),
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    wallet_id: Annotated[str, Depends(require_tradable_active_wallet)],
) -> BacktestComparisonResponse:
    """Create (or return idempotent existing) backtest comparison.

    SELECT on normalised pair
    first; 200 with existing if found. If the INSERT races past that
    SELECT, the DB-enforced partial unique index
    ``uq_bc_active_pair_per_wallet`` (migration 0010) raises
    ``IntegrityError`` during the ``create_comparison`` commit
    ``SQLAlchemyRepository.session()`` rolls the failed session back
    at the contextmanager boundary, then this handler opens a fresh
    session via ``get_comparison_by_pair`` and returns the committed
    winner with 200. Route registered BEFORE ``/{run_id}`` so
    ``/compare`` is not captured as ``id="compare"``.

    Args:
        request: FastAPI request (provides REST tracker).
        command: Validated compare-request envelope.
        principal: Authenticated caller with CREATE_BACKTEST_COMPARISONS.
        repo: Database repository dependency.
        wallet_id: Active wallet re-resolved through the trade plane. The
            permission alone is not enough — ``AI_REVIEWER`` holds it and
            may also hold a personal read grant, so without this the
            read-plane wallet claim would authorize a shared write.

    Returns:
        Envelope wrapping the created (or existing) comparison row.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    body = command.payload
    ts = datetime.now(UTC)
    if body.mode == "auto":
        if body.config_hash is None:
            raise HTTPException(status_code=422, detail="config_hash required for mode=auto")
        raw_a, raw_b = await _resolve_auto_pair(
            bt_repo, wallet_id, body.config_hash, body.anchor_run_public_id, ts
        )
        pair_hash: str | None = body.config_hash
    else:
        if body.run_a_public_id is None or body.run_b_public_id is None:
            raise HTTPException(
                status_code=422,
                detail="run_a_public_id and run_b_public_id required for mode=manual",
            )
        raw_a, raw_b, pair_hash = await _resolve_manual_pair(
            bt_repo, wallet_id, body.run_a_public_id, body.run_b_public_id, ts
        )
    norm_a, norm_b = _normalise_pair(raw_a, raw_b)
    existing = await bt_repo.get_comparison_by_pair(norm_a, norm_b, wallet_id, ts)
    if existing is not None:
        return BacktestComparisonResponse(
            type="backtest_comparison_response",
            public_id=str(uuid7()),
            timestamp=now,
            session_id=sid,
            sequence_id=seq,
            payload=_to_comparison_data(existing),
        )
    try:
        _id, public_id = await bt_repo.create_comparison(
            row={
                "wallet_public_id": wallet_id,
                "operator_public_id": principal.primary_operator_public_id or None,
                "created_by_user_id": principal.username,
                "run_a_public_id": norm_a,
                "run_b_public_id": norm_b,
                "config_hash": pair_hash,
                "pairing_mode": body.mode,
                "anchor_run_public_id": body.anchor_run_public_id,
                "session_id": sid,
                "sequence_id": seq,
                "timestamp": now,
            },
            bus_time=now,
            session_id=sid,
            sequence_id=seq,
        )
    except SqlIntegrityError:
        retry = await bt_repo.get_comparison_by_pair(norm_a, norm_b, wallet_id, ts)
        if retry is None:
            raise HTTPException(status_code=500, detail="comparison race recovery failed") from None
        return BacktestComparisonResponse(
            type="backtest_comparison_response",
            public_id=str(uuid7()),
            timestamp=now,
            session_id=sid,
            sequence_id=seq,
            payload=_to_comparison_data(retry),
        )
    created = await bt_repo.get_comparison(public_id, as_of=ts)
    assert created is not None
    return BacktestComparisonResponse(
        type="backtest_comparison_response",
        public_id=str(uuid7()),
        timestamp=now,
        session_id=sid,
        sequence_id=seq,
        payload=_to_comparison_data(created),
    )


@router.get("/compare", responses=_BACKTEST_LIST_RESPONSES)
async def list_comparisons(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> BacktestComparisonListResponse:
    """List recent comparisons for the caller's wallet.

    Args:
        request: FastAPI request.
        principal: Authenticated caller with READ_BACKTESTS.
        repo: Database repository dependency.
        as_of: Temporal query parameter.
        limit: Page size.
        offset: Page offset.

    Returns:
        Wallet-scoped comparison list newest-first.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)
    wallet_id = await resolve_readable_active_wallet(principal, repo)
    rows = await bt_repo.list_comparisons(
        as_of=ts, wallet_public_id=wallet_id, limit=limit, offset=offset
    )
    items = [_to_comparison_data(r) for r in rows]
    return BacktestComparisonListResponse(
        type="backtest_comparison_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=items,
        count=len(items),
    )


@router.get(
    "/compare/{comparison_public_id}",
    responses=_BACKTEST_COMPARISON_READ_RESPONSES,
)
async def get_comparison(
    comparison_public_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
) -> BacktestComparisonDetailResponse:
    """Fetch a comparison + recomputed diff from current artifact rows.

    Args:
        comparison_public_id: UUID7 of the comparison row.
        request: FastAPI request.
        principal: Authenticated caller with READ_BACKTESTS.
        repo: Database repository dependency.
        as_of: Temporal query parameter.

    Returns:
        Envelope with comparison metadata, both run projections, and
        metrics/equity/trades/signals diffs recomputed on GET.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)
    wallet_id = await resolve_readable_active_wallet(principal, repo)
    comparison = await bt_repo.get_comparison(comparison_public_id, as_of=ts)
    if comparison is None or comparison["wallet_public_id"] != wallet_id:
        raise HTTPException(status_code=404, detail="Comparison not found")
    run_a = await bt_repo.get_run(comparison["run_a_public_id"], as_of=ts)
    run_b = await bt_repo.get_run(comparison["run_b_public_id"], as_of=ts)
    if run_a is None or run_b is None:
        raise HTTPException(status_code=404, detail="Underlying run not found")
    result_a = await bt_repo.get_result(comparison["run_a_public_id"], as_of=ts)
    result_b = await bt_repo.get_result(comparison["run_b_public_id"], as_of=ts)
    equity_a = await bt_repo.get_equity_points(comparison["run_a_public_id"], as_of=ts)
    equity_b = await bt_repo.get_equity_points(comparison["run_b_public_id"], as_of=ts)
    trades_a = await bt_repo.get_trades(comparison["run_a_public_id"], as_of=ts)
    trades_b = await bt_repo.get_trades(comparison["run_b_public_id"], as_of=ts)
    signals_a = await bt_repo.get_signals(comparison["run_a_public_id"], as_of=ts)
    signals_b = await bt_repo.get_signals(comparison["run_b_public_id"], as_of=ts)
    detail = BacktestComparisonDetailResponseData(
        type="backtest_comparison_detail",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        comparison=_to_comparison_data(comparison),
        run_a=_run_to_data(run_a, sid, seq),
        run_b=_run_to_data(run_b, sid, seq),
        metrics_diff=[MetricDiffRow(**row) for row in compute_metrics_diff(result_a, result_b)],
        equity_overlay=[
            EquityOverlayPoint(**row) for row in compute_equity_overlay(equity_a, equity_b)
        ],
        trades_diff=[TradeDiffEntry(**row) for row in compute_trades_diff(trades_a, trades_b)],
        signals_diff=[SignalDiffEntry(**row) for row in compute_signals_diff(signals_a, signals_b)],
    )
    return BacktestComparisonDetailResponse(
        type="backtest_comparison_detail_response",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=detail,
    )


@router.get(
    "/{run_id}",
    responses=_BACKTEST_READ_RESPONSES,
)
async def get_backtest(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
) -> BacktestRunDetailResponse:
    """Get backtest run detail with optional inline result for completed runs.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        principal: Authenticated caller.
        repo: Database repository.
        as_of: Temporal query parameter.

    Returns:
        Backtest run detail with result if completed.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)

    run = await bt_repo.get_run(run_id, as_of=ts)
    if run is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _enforce_wallet_scope(principal, repo, run)

    inline_result: BacktestResultInline | None = None
    if run["status"] == "completed":
        result_row = await bt_repo.get_result(run["public_id"], as_of=ts)
        if result_row is not None:
            inline_result = _project_inline_result(result_row)

    return BacktestRunDetailResponse(
        type="backtest_run_detail_response",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=_run_to_detail_data(run, sid, seq, inline_result),
    )


@router.post(
    "/{run_id}/cancel",
    openapi_extra=openapi_schema(BacktestCancelCommand),
    dependencies=[
        Depends(validate_csrf_token),
        Depends(json_body(BacktestCancelCommand)),
    ],
    responses=_BACKTEST_CANCEL_RESPONSES,
)
async def cancel_backtest(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    wallet_id: Annotated[str, Depends(require_tradable_active_wallet)],
) -> BacktestRunResponse:
    """Cancel a running or pending backtest run.

    The ``BacktestCancelCommand`` envelope moved from a handler parameter
    to a route-level dependency: the body was never read here, only
    validated, and keeping it as a parameter would push this signature
    past the argument ceiling once the trade-plane wallet joined it.
    Validation and the ``openapi_extra`` schema are unchanged.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        principal: Authenticated caller with MANAGE_BACKTESTS.
        repo: Database repository.
        wallet_id: Active wallet re-resolved through the trade plane.

    Returns:
        Updated backtest run.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    now = datetime.now(UTC)

    run = await bt_repo.get_run(run_id, as_of=now)
    if run is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    _enforce_run_wallet(wallet_id, run)
    if run["status"] not in _CANCELLABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot cancel run in status '{run['status']}'",
        )

    await bt_repo.update_run_status(
        public_id=run_id,
        new_status="cancel_requested",
        bus_time=now,
        session_id=sid,
        sequence_id=tracker.next_sequence(_REST_STREAM),
    )

    updated = await bt_repo.get_run(run_id, as_of=datetime.now(UTC))
    if updated is None:
        raise HTTPException(status_code=500, detail="Run updated but not found")

    return BacktestRunResponse(
        type="backtest_run_response",
        public_id=str(uuid7()),
        timestamp=now,
        session_id=sid,
        sequence_id=seq,
        payload=_run_to_data(updated, sid, seq),
    )


@router.post(
    "/{run_id}/rerun",
    dependencies=[Depends(validate_csrf_token)],
    responses=_BACKTEST_READ_RESPONSES,
)
async def rerun_backtest(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    wallet_id: Annotated[str, Depends(require_tradable_active_wallet)],
) -> BacktestRunResponse:
    """Re-run a backtest with the same configuration.

    Args:
        run_id: Original run public ID.
        request: FastAPI request.
        principal: Authenticated caller with MANAGE_BACKTESTS.
        repo: Database repository.
        wallet_id: Active wallet re-resolved through the trade plane and
            handed straight to :func:`create_backtest`, so the replay
            cannot land in a wallet the caller may only read.

    Returns:
        Newly created backtest run.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)

    original = await bt_repo.get_run(run_id, as_of=now)
    if original is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    _enforce_run_wallet(wallet_id, original)

    rerun_body = BacktestCreateBody(
        strategy_class=original["strategy_name"],
        instrument_public_id=original.get("instrument") or original["instrument_public_id"],
        exchange=original["exchange"],
        timeframe=original["timeframe"],
        start_date=original["start_date"],
        end_date=original["end_date"],
        initial_cash=original["initial_cash"],
        strategy_params=original["strategy_params"],
        execution_mode=original["execution_mode"],
        fill_model=original["fill_model"],
        slippage_bps=original["slippage_bps"],
        commission_bps=original["commission_bps"],
        target_execution_exchange=original.get("target_execution_exchange"),
    )
    rerun_command = BacktestCreateCommand(
        type="backtest_create_command",
        public_id=str(uuid7()),
        timestamp=now,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        payload=rerun_body,
    )

    return await create_backtest(request, rerun_command, principal, repo, wallet_id)


@router.get(
    "/{run_id}/trades",
    responses=_BACKTEST_READ_RESPONSES,
)
async def get_backtest_trades(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> BacktestTradeListResponse:
    """Get trades for a backtest run.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        principal: Authenticated caller.
        repo: Database repository.
        as_of: Temporal query.
        limit: Page size.
        offset: Page offset.

    Returns:
        List of backtest trades.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)

    run = await bt_repo.get_run(run_id, as_of=ts)
    if run is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _enforce_wallet_scope(principal, repo, run)

    trades = await bt_repo.get_trades(run_id, as_of=ts, limit=limit, offset=offset)
    items = [
        BacktestTradeData(
            type="backtest_trade",
            public_id=t["public_id"],
            timestamp=t["timestamp"],
            session_id=t["session_id"],
            sequence_id=t["sequence_id"],
            run_public_id=t["run_public_id"],
            executed_at=t["executed_at"],
            instrument=t["instrument"],
            side=t["side"],
            quantity=t["quantity"],
            price=t["price"],
            fee=t["fee"],
            pnl=t["pnl"],
            position_after=t["position_after"],
            signal_public_id=t.get("signal_public_id"),
        )
        for t in trades
    ]
    return BacktestTradeListResponse(
        type="backtest_trade_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=items,
        count=len(items),
    )


@router.get(
    "/{run_id}/signals",
    responses=_BACKTEST_READ_RESPONSES,
)
async def get_backtest_signals(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> BacktestSignalListResponse:
    """Get signals for a backtest run.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        principal: Authenticated caller.
        repo: Database repository.
        as_of: Temporal query.
        limit: Page size.
        offset: Page offset.

    Returns:
        List of backtest signals.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)

    run = await bt_repo.get_run(run_id, as_of=ts)
    if run is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _enforce_wallet_scope(principal, repo, run)

    signals = await bt_repo.get_signals(run_id, as_of=ts, limit=limit, offset=offset)
    items = [
        BacktestSignalData(
            type="backtest_signal",
            public_id=s["public_id"],
            timestamp=s["timestamp"],
            session_id=s["session_id"],
            sequence_id=s["sequence_id"],
            run_public_id=s["run_public_id"],
            signal_time=s["signal_time"],
            signal_type=s["signal_type"],
            instrument=s["instrument"],
            price=s["price"],
            indicators=s.get("indicators", {}),
        )
        for s in signals
    ]
    return BacktestSignalListResponse(
        type="backtest_signal_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=items,
        count=len(items),
    )


@router.get(
    "/{run_id}/events",
    responses=_BACKTEST_READ_RESPONSES,
)
async def get_backtest_events(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
) -> BacktestEventListResponse:
    """Get events for a backtest run.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        principal: Authenticated caller.
        repo: Database repository.
        as_of: Temporal query.

    Returns:
        List of backtest events.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)

    run = await bt_repo.get_run(run_id, as_of=ts)
    if run is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _enforce_wallet_scope(principal, repo, run)

    events = await bt_repo.get_events(run_id, as_of=ts)
    items = [
        BacktestEventData(
            type="backtest_event",
            public_id=e["public_id"],
            timestamp=e["timestamp"],
            session_id=e["session_id"],
            sequence_id=e["sequence_id"],
            run_public_id=e["run_public_id"],
            event_type=e["event_type"],
            detail=e.get("detail", {}),
        )
        for e in events
    ]
    return BacktestEventListResponse(
        type="backtest_event_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=items,
        count=len(items),
    )


@router.get(
    "/{run_id}/equity",
    responses=_BACKTEST_READ_RESPONSES,
)
async def get_backtest_equity(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    limit: Annotated[int, Query(ge=1, le=20000)] = 5000,
    after: Annotated[
        datetime | None,
        Query(description="Forward cursor: return points strictly after this point_time"),
    ] = None,
) -> BacktestEquityPointListResponse:
    """Get equity-curve points for a backtest run, ordered ascending by point_time.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        principal: Authenticated caller.
        repo: Database repository.
        as_of: Temporal query parameter.
        limit: Page size (max 20000).
        after: Forward cursor — set to the last seen point_time of the
            previous page to fetch the next slice (exclusive).

    Returns:
        List of equity points.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = _resolve_as_of(as_of)

    run = await bt_repo.get_run(run_id, as_of=ts)
    if run is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    await _enforce_wallet_scope(principal, repo, run)

    points = await bt_repo.get_equity_points(run_id, as_of=ts, limit=limit, after=after)
    items = [
        BacktestEquityPointInline(
            point_time=p["point_time"],
            equity=p["equity"],
            cash=p["cash"],
            position_value=p.get("position_value", 0.0),
            drawdown=p.get("drawdown", 0.0),
        )
        for p in points
    ]
    return BacktestEquityPointListResponse(
        type="backtest_equity_point_list",
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        session_id=sid,
        sequence_id=seq,
        payload=items,
        count=len(items),
    )
