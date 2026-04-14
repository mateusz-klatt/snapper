"""Repository for backtest data operations.

Handles CRUD for backtest runs, events, signals, trades, equity points,
and results. All reads accept as_of for temporal queries. Write operations
use SCD2 close-and-insert for mutable state (BacktestRun status),
append-only for immutable data (events, signals, trades, equity).
"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker

from snapper.data.models import BacktestEquityPoint
from snapper.data.models import BacktestEvent
from snapper.data.models import BacktestResult
from snapper.data.models import BacktestRun
from snapper.data.models import BacktestSignal
from snapper.data.models import BacktestTrade
from snapper.data.repository import where_active
from snapper.data.repository_types import BacktestEquityPointInsertRow
from snapper.data.repository_types import BacktestEquityPointRow
from snapper.data.repository_types import BacktestEventInsertRow
from snapper.data.repository_types import BacktestEventRow
from snapper.data.repository_types import BacktestResultInsertRow
from snapper.data.repository_types import BacktestResultRow
from snapper.data.repository_types import BacktestRunInsertRow
from snapper.data.repository_types import BacktestRunRow
from snapper.data.repository_types import BacktestSignalInsertRow
from snapper.data.repository_types import BacktestSignalRow
from snapper.data.repository_types import BacktestTradeInsertRow
from snapper.data.repository_types import BacktestTradeRow


def _run_to_dict(row: BacktestRun) -> BacktestRunRow:
    """Project a BacktestRun ORM row into the TypedDict shape."""
    return BacktestRunRow(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        wallet_public_id=row.wallet_public_id,
        operator_public_id=row.operator_public_id,
        strategy_name=row.strategy_name,
        strategy_params=row.strategy_params,
        instrument_public_id=row.instrument_public_id,
        exchange=row.exchange,
        mode=row.mode,
        timeframe=row.timeframe,
        start_date=row.start_date,
        end_date=row.end_date,
        initial_cash=row.initial_cash,
        status=row.status,
        execution_mode=row.execution_mode,
        fill_model=row.fill_model,
        slippage_bps=row.slippage_bps,
        commission_bps=row.commission_bps,
        created_by_user_id=row.created_by_user_id,
        started_at=row.started_at,
        completed_at=row.completed_at,
        error=row.error,
        process_name=row.process_name,
    )


def _event_to_dict(row: BacktestEvent) -> BacktestEventRow:
    """Project a BacktestEvent ORM row into the TypedDict shape."""
    return BacktestEventRow(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        run_public_id=row.run_public_id,
        event_type=row.event_type,
        detail=row.detail,
    )


def _result_to_dict(row: BacktestResult) -> BacktestResultRow:
    """Project a BacktestResult ORM row into the TypedDict shape."""
    return BacktestResultRow(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        run_public_id=row.run_public_id,
        total_trades=row.total_trades,
        winning_trades=row.winning_trades,
        losing_trades=row.losing_trades,
        total_pnl=row.total_pnl,
        max_drawdown=row.max_drawdown,
        sharpe_ratio=row.sharpe_ratio,
        win_rate=row.win_rate,
        profit_factor=row.profit_factor,
        final_equity=row.final_equity,
        max_equity=row.max_equity,
        extra_metrics=row.extra_metrics,
    )


def _signal_to_dict(row: BacktestSignal) -> BacktestSignalRow:
    """Project a BacktestSignal ORM row into the TypedDict shape."""
    return BacktestSignalRow(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        run_public_id=row.run_public_id,
        signal_time=row.signal_time,
        signal_type=row.signal_type,
        instrument=row.instrument,
        price=row.price,
        indicators=row.indicators,
    )


def _trade_to_dict(row: BacktestTrade) -> BacktestTradeRow:
    """Project a BacktestTrade ORM row into the TypedDict shape."""
    return BacktestTradeRow(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        run_public_id=row.run_public_id,
        executed_at=row.executed_at,
        instrument=row.instrument,
        side=row.side,
        quantity=row.quantity,
        price=row.price,
        fee=row.fee,
        pnl=row.pnl,
        position_after=row.position_after,
        signal_public_id=row.signal_public_id,
    )


def _equity_to_dict(row: BacktestEquityPoint) -> BacktestEquityPointRow:
    """Project a BacktestEquityPoint ORM row into the TypedDict shape."""
    return BacktestEquityPointRow(
        public_id=row.public_id,
        timestamp=row.timestamp,
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        run_public_id=row.run_public_id,
        point_time=row.point_time,
        equity=row.equity,
        cash=row.cash,
        position_value=row.position_value,
        drawdown=row.drawdown,
    )


class BacktestRepository:
    """Async repository for backtest data operations.

    Shares the session factory from the main SQLAlchemyRepository
    so all backtest operations use the same connection pool and
    transaction isolation.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        """Initialize with a session factory.

        Args:
            session_factory: Async session maker from SQLAlchemyRepository.
        """
        self._session_factory = session_factory

    @asynccontextmanager
    async def session(self) -> Any:
        """Yield a transactional async session."""
        async with self._session_factory() as s:
            yield s

    async def create_run(
        self,
        row: BacktestRunInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> tuple[int, str]:
        """Insert a new backtest run in pending state.

        Args:
            row: Run insert payload.
            bus_time: Bus time for temporal tracking.
            session_id: Producer session ID.
            sequence_id: Monotonic sequence counter.

        Returns:
            Tuple of (id, public_id).
        """
        async with self.session() as s:
            run = BacktestRun(
                wallet_public_id=row["wallet_public_id"],
                operator_public_id=row.get("operator_public_id"),
                strategy_name=row["strategy_name"],
                strategy_params=row.get("strategy_params", {}),
                instrument_public_id=row["instrument_public_id"],
                exchange=row["exchange"],
                mode=row.get("mode", "paper"),
                timeframe=row["timeframe"],
                start_date=row["start_date"],
                end_date=row["end_date"],
                initial_cash=row.get("initial_cash", 10000.0),
                status=row.get("status", "pending"),
                execution_mode=row.get("execution_mode", "direct_db"),
                fill_model=row.get("fill_model", "market"),
                slippage_bps=row.get("slippage_bps", 0.0),
                commission_bps=row.get("commission_bps", 0.0),
                created_by_user_id=row.get("created_by_user_id"),
                process_name=row.get("process_name"),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(run)
            await s.commit()
            await s.refresh(run)
            return run.id, run.public_id

    async def get_run(self, public_id: str, as_of: datetime) -> BacktestRunRow | None:
        """Retrieve a backtest run by public_id.

        Args:
            public_id: Run public identifier.
            as_of: Bus time for temporal query.

        Returns:
            Run row or None.
        """
        async with self.session() as s:
            row = (
                (
                    await s.execute(
                        select(BacktestRun).where(
                            BacktestRun.public_id == public_id,
                            *where_active(BacktestRun, as_of),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if row is None:
                return None
            return _run_to_dict(row)

    async def list_runs(
        self,
        as_of: datetime,
        wallet_public_id: str | None = None,
        strategy: str | None = None,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[BacktestRunRow]:
        """List backtest runs with optional filters.

        Args:
            as_of: Bus time for temporal query.
            wallet_public_id: Optional wallet filter.
            strategy: Optional strategy_name filter.
            status: Optional status filter.
            limit: Max rows to return.
            offset: Rows to skip.

        Returns:
            List of run rows.
        """
        async with self.session() as s:
            conditions = list(where_active(BacktestRun, as_of))
            if wallet_public_id is not None:
                conditions.append(BacktestRun.wallet_public_id == wallet_public_id)
            if strategy is not None:
                conditions.append(BacktestRun.strategy_name == strategy)
            if status is not None:
                conditions.append(BacktestRun.status == status)
            rows = (
                (
                    await s.execute(
                        select(BacktestRun)
                        .where(*conditions)
                        .order_by(BacktestRun.timestamp.desc())
                        .limit(limit)
                        .offset(offset)
                    )
                )
                .scalars()
                .all()
            )
            return [_run_to_dict(r) for r in rows]

    async def update_run_status(
        self,
        public_id: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        error: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> int | None:
        """SCD2 close-and-insert for run status transition.

        Args:
            public_id: Run to update.
            new_status: Target status.
            bus_time: Bus time for SCD2 operation.
            session_id: Producer session ID.
            sequence_id: Sequence counter.
            error: Optional error message (for failed status).
            started_at: Optional start timestamp.
            completed_at: Optional completion timestamp.

        Returns:
            New row id, or None if run not found.
        """
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(BacktestRun)
                        .where(
                            BacktestRun.public_id == public_id,
                            *where_active(BacktestRun, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return None
            await s.execute(
                update(BacktestRun).where(BacktestRun.id == existing.id).values(known_to=bus_time)
            )
            new_row = BacktestRun(
                public_id=existing.public_id,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                strategy_name=existing.strategy_name,
                strategy_params=existing.strategy_params,
                instrument_public_id=existing.instrument_public_id,
                exchange=existing.exchange,
                mode=existing.mode,
                timeframe=existing.timeframe,
                start_date=existing.start_date,
                end_date=existing.end_date,
                initial_cash=existing.initial_cash,
                status=new_status,
                execution_mode=existing.execution_mode,
                fill_model=existing.fill_model,
                slippage_bps=existing.slippage_bps,
                commission_bps=existing.commission_bps,
                created_by_user_id=existing.created_by_user_id,
                started_at=started_at or existing.started_at,
                completed_at=completed_at or existing.completed_at,
                error=error if error is not None else existing.error,
                process_name=existing.process_name,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_row)
            await s.commit()
            await s.refresh(new_row)
            return new_row.id

    async def insert_event(
        self,
        row: BacktestEventInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str:
        """Insert an append-only backtest event.

        Args:
            row: Event insert payload.
            bus_time: Bus time.
            session_id: Producer session ID.
            sequence_id: Sequence counter.

        Returns:
            Public ID of the new event.
        """
        async with self.session() as s:
            event = BacktestEvent(
                run_public_id=row["run_public_id"],
                event_type=row["event_type"],
                detail=row.get("detail", {}),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(event)
            await s.commit()
            await s.refresh(event)
            return event.public_id

    async def insert_signals_batch(
        self,
        rows: list[BacktestSignalInsertRow],
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """Batch-insert backtest signals.

        Args:
            rows: Signal insert payloads.
            bus_time: Bus time.
            session_id: Producer session ID.
            sequence_id: Base sequence counter (incremented per row).
        """
        async with self.session() as s:
            for i, row in enumerate(rows):
                s.add(
                    BacktestSignal(
                        run_public_id=row["run_public_id"],
                        signal_time=row["signal_time"],
                        signal_type=row["signal_type"],
                        instrument=row["instrument"],
                        price=row["price"],
                        indicators=row.get("indicators", {}),
                        session_id=session_id,
                        sequence_id=sequence_id + i,
                        timestamp=bus_time,
                    )
                )
            await s.commit()

    async def insert_trades_batch(
        self,
        rows: list[BacktestTradeInsertRow],
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """Batch-insert backtest trades.

        Args:
            rows: Trade insert payloads.
            bus_time: Bus time.
            session_id: Producer session ID.
            sequence_id: Base sequence counter.
        """
        async with self.session() as s:
            for i, row in enumerate(rows):
                s.add(
                    BacktestTrade(
                        run_public_id=row["run_public_id"],
                        executed_at=row["executed_at"],
                        instrument=row["instrument"],
                        side=row["side"],
                        quantity=row["quantity"],
                        price=row["price"],
                        fee=row.get("fee", 0.0),
                        pnl=row.get("pnl"),
                        position_after=row.get("position_after", 0.0),
                        signal_public_id=row.get("signal_public_id"),
                        session_id=session_id,
                        sequence_id=sequence_id + i,
                        timestamp=bus_time,
                    )
                )
            await s.commit()

    async def insert_equity_points_batch(
        self,
        rows: list[BacktestEquityPointInsertRow],
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """Batch-insert equity curve data points.

        Args:
            rows: Equity point insert payloads.
            bus_time: Bus time.
            session_id: Producer session ID.
            sequence_id: Base sequence counter.
        """
        async with self.session() as s:
            for i, row in enumerate(rows):
                s.add(
                    BacktestEquityPoint(
                        run_public_id=row["run_public_id"],
                        point_time=row["point_time"],
                        equity=row["equity"],
                        cash=row["cash"],
                        position_value=row.get("position_value", 0.0),
                        drawdown=row.get("drawdown", 0.0),
                        session_id=session_id,
                        sequence_id=sequence_id + i,
                        timestamp=bus_time,
                    )
                )
            await s.commit()

    async def insert_result(
        self,
        row: BacktestResultInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str:
        """Insert aggregate backtest result metrics.

        Args:
            row: Result insert payload.
            bus_time: Bus time.
            session_id: Producer session ID.
            sequence_id: Sequence counter.

        Returns:
            Public ID of the result row.
        """
        async with self.session() as s:
            result = BacktestResult(
                run_public_id=row["run_public_id"],
                total_trades=row.get("total_trades", 0),
                winning_trades=row.get("winning_trades", 0),
                losing_trades=row.get("losing_trades", 0),
                total_pnl=row.get("total_pnl", 0.0),
                max_drawdown=row.get("max_drawdown", 0.0),
                sharpe_ratio=row.get("sharpe_ratio"),
                win_rate=row.get("win_rate"),
                profit_factor=row.get("profit_factor"),
                final_equity=row.get("final_equity", 0.0),
                max_equity=row.get("max_equity", 0.0),
                extra_metrics=row.get("extra_metrics", {}),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(result)
            await s.commit()
            await s.refresh(result)
            return result.public_id

    async def get_signals(
        self,
        run_public_id: str,
        as_of: datetime,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[BacktestSignalRow]:
        """Retrieve signals for a backtest run.

        Args:
            run_public_id: Run to query.
            as_of: Bus time for temporal query.
            limit: Max rows.
            offset: Skip rows.

        Returns:
            List of signal rows ordered by signal_time.
        """
        async with self.session() as s:
            rows = (
                (
                    await s.execute(
                        select(BacktestSignal)
                        .where(
                            BacktestSignal.run_public_id == run_public_id,
                            *where_active(BacktestSignal, as_of),
                        )
                        .order_by(BacktestSignal.signal_time)
                        .limit(limit)
                        .offset(offset)
                    )
                )
                .scalars()
                .all()
            )
            return [_signal_to_dict(r) for r in rows]

    async def get_trades(
        self,
        run_public_id: str,
        as_of: datetime,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[BacktestTradeRow]:
        """Retrieve trades for a backtest run.

        Args:
            run_public_id: Run to query.
            as_of: Bus time for temporal query.
            limit: Max rows.
            offset: Skip rows.

        Returns:
            List of trade rows ordered by executed_at.
        """
        async with self.session() as s:
            rows = (
                (
                    await s.execute(
                        select(BacktestTrade)
                        .where(
                            BacktestTrade.run_public_id == run_public_id,
                            *where_active(BacktestTrade, as_of),
                        )
                        .order_by(BacktestTrade.executed_at)
                        .limit(limit)
                        .offset(offset)
                    )
                )
                .scalars()
                .all()
            )
            return [_trade_to_dict(r) for r in rows]

    async def get_equity_points(
        self,
        run_public_id: str,
        as_of: datetime,
        limit: int | None = None,
        after: datetime | None = None,
    ) -> list[BacktestEquityPointRow]:
        """Retrieve equity curve for a backtest run, ordered ascending by point_time.

        Pagination model: forward cursor on point_time. Pass ``after`` set to the
        last seen ``point_time`` of the previous page to fetch the next slice.

        Args:
            run_public_id: Run to query.
            as_of: Bus time for temporal query.
            limit: Maximum points to return (None = unbounded).
            after: Cursor — return only points strictly greater than this
                timestamp (exclusive). Combined with ASC ordering and LIMIT
                this gives correct forward pagination.

        Returns:
            List of equity points ordered by point_time ascending.
        """
        async with self.session() as s:
            stmt = (
                select(BacktestEquityPoint)
                .where(
                    BacktestEquityPoint.run_public_id == run_public_id,
                    *where_active(BacktestEquityPoint, as_of),
                )
                .order_by(BacktestEquityPoint.point_time)
            )
            if after is not None:
                stmt = stmt.where(BacktestEquityPoint.point_time > after)
            if limit is not None:
                stmt = stmt.limit(limit)
            rows = (await s.execute(stmt)).scalars().all()
            return [_equity_to_dict(r) for r in rows]

    async def get_result(
        self,
        run_public_id: str,
        as_of: datetime,
    ) -> BacktestResultRow | None:
        """Retrieve aggregate result for a backtest run.

        Args:
            run_public_id: Run to query.
            as_of: Bus time for temporal query.

        Returns:
            Result row or None.
        """
        async with self.session() as s:
            row = (
                (
                    await s.execute(
                        select(BacktestResult).where(
                            BacktestResult.run_public_id == run_public_id,
                            *where_active(BacktestResult, as_of),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if row is None:
                return None
            return _result_to_dict(row)

    async def get_events(
        self,
        run_public_id: str,
        as_of: datetime,
    ) -> list[BacktestEventRow]:
        """Retrieve events for a backtest run.

        Args:
            run_public_id: Run to query.
            as_of: Bus time for temporal query.

        Returns:
            List of event rows ordered by timestamp.
        """
        async with self.session() as s:
            rows = (
                (
                    await s.execute(
                        select(BacktestEvent)
                        .where(
                            BacktestEvent.run_public_id == run_public_id,
                            *where_active(BacktestEvent, as_of),
                        )
                        .order_by(BacktestEvent.timestamp)
                    )
                )
                .scalars()
                .all()
            )
            return [_event_to_dict(r) for r in rows]

    async def reconcile_stale_runs(
        self,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int:
        """Mark stale running/pending/cancel_requested runs as failed.

        Called at boot time to clean up orphaned runs from a previous
        crash. Any run in a non-terminal state is assumed orphaned.

        Args:
            bus_time: Current bus time for SCD2 transitions.
            session_id: Producer session ID.
            sequence_id: Base sequence counter.

        Returns:
            Number of runs transitioned to failed.
        """
        async with self.session() as s:
            stale_statuses = ("pending", "running", "cancel_requested")
            conditions = list(where_active(BacktestRun, bus_time))
            conditions.append(BacktestRun.status.in_(stale_statuses))
            stale_rows = (await s.execute(select(BacktestRun).where(*conditions))).scalars().all()
            if not stale_rows:
                return 0

        count = 0
        for row in stale_rows:
            result = await self.update_run_status(
                public_id=row.public_id,
                new_status="failed",
                bus_time=bus_time,
                session_id=session_id,
                sequence_id=sequence_id + count,
                error="Orphaned run — process terminated before completion",
            )
            if result is not None:
                count += 1
        return count
