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
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.backtest import BacktestCancelCommand
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
from snapper.api.schemas.backtest import BacktestTradeData
from snapper.api.schemas.backtest import BacktestTradeListResponse
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
from snapper.data.repository_types import BacktestRunRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/backtests", tags=["backtests"])

_REST_STREAM = "backtest_rest"
_CANCELLABLE_STATUSES = frozenset({"pending", "running"})
_NOT_FOUND = "Backtest run not found"


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
        started_at=run.get("started_at"),
        completed_at=run.get("completed_at"),
        error=run.get("error"),
        result=result,
    )


def _get_process_factory(request: Request) -> ProcessLauncherService:
    """Retrieve ProcessLauncherService from app state."""
    return cast(ProcessLauncherService, request.app.state.process_factory)


@router.post(
    "",
    openapi_extra=openapi_schema(BacktestCreateCommand),
    dependencies=[Depends(validate_csrf_token)],
)
async def create_backtest(
    request: Request,
    command: Annotated[BacktestCreateCommand, Depends(json_body(BacktestCreateCommand))],
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> BacktestRunResponse:
    """Create and launch a new backtest run.

    Args:
        request: FastAPI request.
        command: Validated create command envelope.
        principal: Authenticated caller with MANAGE_BACKTESTS.
        repo: Database repository.

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

    wallet_id = principal.active_wallet_public_id
    if not wallet_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No active wallet selected — select a wallet before creating backtests",
        )

    body = command.payload
    _, public_id = await bt_repo.create_run(
        row={
            "wallet_public_id": wallet_id,
            "operator_public_id": principal.primary_operator_public_id or None,
            "strategy_name": body.strategy_class,
            "strategy_params": dict(body.strategy_params),
            "instrument_public_id": body.instrument_public_id,
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


@router.get("")
async def list_backtests(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    as_of: Annotated[datetime | None, Query(description="Point-in-time query (UTC)")] = None,
    strategy: Annotated[str | None, Query(description="Filter by strategy name")] = None,
    run_status: Annotated[str | None, Query(alias="status", description="Filter by status")] = None,
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

    wallet_id = principal.active_wallet_public_id
    runs = await bt_repo.list_runs(
        as_of=ts,
        wallet_public_id=wallet_id,
        strategy=strategy,
        status=run_status,
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


@router.get("/{run_id}")
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
    if (
        principal.active_wallet_public_id
        and run["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    inline_result: BacktestResultInline | None = None
    if run["status"] == "completed":
        result_row = await bt_repo.get_result(run["public_id"], as_of=ts)
        if result_row is not None:
            inline_result = BacktestResultInline(
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
                extra_metrics=result_row.get("extra_metrics", {}),
            )

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
    dependencies=[Depends(validate_csrf_token)],
)
async def cancel_backtest(
    run_id: str,
    request: Request,
    command: Annotated[BacktestCancelCommand, Depends(json_body(BacktestCancelCommand))],
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> BacktestRunResponse:
    """Cancel a running or pending backtest run.

    Args:
        run_id: Run public ID.
        request: FastAPI request.
        command: Cancel command envelope.
        principal: Authenticated caller with MANAGE_BACKTESTS.
        repo: Database repository.

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
    if (
        principal.active_wallet_public_id
        and run["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
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
)
async def rerun_backtest(
    run_id: str,
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_BACKTESTS))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> BacktestRunResponse:
    """Re-run a backtest with the same configuration.

    Args:
        run_id: Original run public ID.
        request: FastAPI request.
        principal: Authenticated caller with MANAGE_BACKTESTS.
        repo: Database repository.

    Returns:
        Newly created backtest run.
    """
    bt_repo = _bt_repo(repo)
    tracker: SequenceTracker = request.app.state.rest_tracker
    now = datetime.now(UTC)

    original = await bt_repo.get_run(run_id, as_of=now)
    if original is None:
        raise HTTPException(status_code=404, detail=_NOT_FOUND)
    if (
        principal.active_wallet_public_id
        and original["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

    rerun_body = BacktestCreateBody(
        strategy_class=original["strategy_name"],
        instrument_public_id=original["instrument_public_id"],
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
    )
    rerun_command = BacktestCreateCommand(
        type="backtest_create_command",
        public_id=str(uuid7()),
        timestamp=now,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        payload=rerun_body,
    )

    return await create_backtest(request, rerun_command, principal, repo)


@router.get("/{run_id}/trades")
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
    if (
        principal.active_wallet_public_id
        and run["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

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


@router.get("/{run_id}/signals")
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
    if (
        principal.active_wallet_public_id
        and run["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

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


@router.get("/{run_id}/events")
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
    if (
        principal.active_wallet_public_id
        and run["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

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


@router.get("/{run_id}/equity")
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
    if (
        principal.active_wallet_public_id
        and run["wallet_public_id"] != principal.active_wallet_public_id
    ):
        raise HTTPException(status_code=404, detail=_NOT_FOUND)

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
