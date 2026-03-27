"""Database repository implementations for async and sync access.

This module provides the Repository abstract base class and concrete
implementations for different database backends:

- **SQLAlchemyRepository**: Async repository for SQLite and PostgreSQL.
- **DatabaseRepository**: Simple sync repository for scripts/notebooks.

Key Features:
    - Connection pooling with appropriate settings per database type.
    - Upsert operations with dialect-specific conflict handling.
    - Automatic timezone handling via TZDateTime type.
    - Cached repository instances via get_repository().

Example:
    Using the async repository::

        from snapper.data.repository import get_repository

        repo = get_repository("sqlite+aiosqlite:///./data/snapper.db")
        async with repo.session() as session:
            result = await session.execute(select(Instrument))
            instruments = result.scalars().all()

    Upserting candles::

        rows = [{"instrument_public_id": "...", "timeframe": "1m", ...}]
        inserted = await repo.upsert_candles(rows)
"""

from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from inspect import isawaitable
from typing import Any
from typing import cast
from uuid import uuid7

from loguru import logger
from sqlalchemy import and_
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import delete
from sqlalchemy import desc
from sqlalchemy import distinct
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.engine import Engine as SyncEngine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session as SyncSession
from sqlalchemy.orm import sessionmaker as sync_sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.pool import StaticPool

from snapper.core.types import AllExchange
from snapper.data.archive_symbols import resolve_archive_symbols
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import MarketSnapshot
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import Tick
from snapper.data.models import Trade
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import MarketSnapshotRow
from snapper.data.repository_types import MarketSnapshotUpsertRow
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import PositionRow
from snapper.data.repository_types import SettingRow
from snapper.data.repository_types import SignalRow
from snapper.data.repository_types import TickRow
from snapper.data.repository_types import TickUpsertRow
from snapper.data.repository_types import TradeRow
from snapper.data.repository_types import TradeUpsertRow

__all__ = [
    "Repository",
    "SQLAlchemyRepository",
    "DatabaseRepository",
    "InstrumentSpecInput",
    "close_and_insert",
    "close_and_insert_sync",
    "get_repository",
    "dispose_repositories",
    "where_active",
    "where_active_now",
]


def where_active(model: type[Any], at: datetime) -> tuple[Any, Any]:
    """Return temporal filter clauses for active records at a specific time.

    Args:
        model: SQLAlchemy model class with timestamp and known_to columns.
        at: Point-in-time to query. Required — use where_active_now() for
            operations that genuinely mean 'current state'.

    Returns:
        Tuple of two filter clauses: (timestamp <= t, known_to > t).
    """
    return model.timestamp <= at, model.known_to > at


def where_active_now(model: type[Any]) -> tuple[Any, Any]:
    """Return temporal filter clauses for currently active records.

    Use sparingly — only for operations that genuinely mean 'current state'
    and have no caller-provided time (e.g. auth login check, heartbeat).
    Prefer where_active(model, at) with explicit time in domain code.

    Args:
        model: SQLAlchemy model class with timestamp and known_to columns.

    Returns:
        Tuple of two filter clauses: (timestamp <= now, known_to > now).
    """
    return where_active(model, datetime.now(UTC))


async def close_and_insert(
    session: AsyncSession,
    model: type[Any],
    match_filters: list[Any],
    new_values: dict[str, Any],
    bus_time: datetime,
) -> Any:
    """Close the active version and insert a new one (SCD Type 2).

    Finds the active row matching the given filters at bus_time,
    closes it by setting known_to=bus_time, then inserts a new row
    carrying the same public_id. If no active row exists, inserts fresh.

    The model must have ``id``, ``public_id``, ``timestamp``, and
    ``known_to`` columns (all temporal ORM models in this project do).

    Args:
        session: Active async session (caller manages transaction).
        model: SQLAlchemy model class with temporal columns.
        match_filters: List of SQLAlchemy filter expressions for the natural key.
        new_values: Column values for the new row (excluding id, public_id, known_to).
        bus_time: Processing timestamp used for close and open.

    Returns:
        The newly inserted model instance.
    """
    existing = (
        (
            await session.execute(
                select(model)
                .where(
                    model.timestamp <= bus_time,
                    model.known_to > bus_time,
                    *match_filters,
                )
                .with_for_update()
            )
        )
        .scalars()
        .first()
    )
    if existing:
        await session.execute(
            update(model).where(model.id == existing.id).values(known_to=bus_time)
        )
        new_values["public_id"] = existing.public_id
    new_values["timestamp"] = bus_time
    new_values["known_to"] = KNOWN_TO_MAX
    new_row = model(**new_values)
    session.add(new_row)
    return new_row


def close_and_insert_sync(
    session: SyncSession,
    model: type[Any],
    match_filters: list[Any],
    new_values: dict[str, Any],
    bus_time: datetime,
) -> Any:
    """Synchronous variant of close_and_insert for SCD Type 2.

    Finds the active row matching the given filters at bus_time,
    closes it by setting known_to=bus_time, then inserts a new row
    carrying the same public_id. If no active row exists, inserts fresh.

    The model must have ``id``, ``public_id``, ``timestamp``, and
    ``known_to`` columns (all temporal ORM models in this project do).

    Args:
        session: Active sync session (caller manages transaction).
        model: SQLAlchemy model class with temporal columns.
        match_filters: List of SQLAlchemy filter expressions for the natural key.
        new_values: Column values for the new row (excluding id, public_id, known_to).
        bus_time: Processing timestamp used for close and open.

    Returns:
        The newly inserted model instance.
    """
    existing = (
        session.execute(
            select(model).where(
                model.timestamp <= bus_time,
                model.known_to > bus_time,
                *match_filters,
            )
        )
        .scalars()
        .first()
    )
    if existing:
        session.execute(update(model).where(model.id == existing.id).values(known_to=bus_time))
        new_values["public_id"] = existing.public_id
    new_values["timestamp"] = bus_time
    new_values["known_to"] = KNOWN_TO_MAX
    new_row = model(**new_values)
    session.add(new_row)
    return new_row


@dataclass(frozen=True)
class InstrumentSpecInput:
    """Typed payload for instrument trading specification revisions."""

    tick_size: float | None = None
    lot_size: float | None = None
    min_order_size: float | None = None
    max_order_size: float | None = None
    cost_decimals: int | None = None
    qty_decimals: int | None = None
    margin_initial: float | None = None
    position_limit_long: int | None = None
    position_limit_short: int | None = None
    status: str | None = None


class Repository(ABC):
    """Abstract base class defining the repository interface.

    All repository implementations must provide async methods for:
        - Session management (context manager)
        - Schema creation
        - CRUD operations for instruments, candles, trades, orders, executions
        - Query methods for market data retrieval

    Attributes:
        engine: SQLAlchemy engine instance (async or sync).
    """

    engine: Any = None

    @abstractmethod
    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Return async context manager for database session."""
        ...

    @abstractmethod
    async def create_all(self) -> None:
        """Create all database tables from model metadata."""
        ...

    @property
    @abstractmethod
    def dialect_name(self) -> str:
        """Return the database dialect name (sqlite, postgresql)."""
        ...

    @abstractmethod
    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[int, str]:
        """Idempotent resolve-or-create for instrument identity.

        Looks up the active Instrument by business key
        (symbol_public_id, exchange).  Returns (id, public_id) of the
        existing row, or inserts a new one if none exists.
        Never closes an existing version.
        """
        ...

    @abstractmethod
    async def revise_instrument(
        self,
        instrument_public_id: str,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> int:
        """SCD2 close+insert for instrument business attributes.

        Looks up the active Instrument by public_id as of *timestamp*.
        Compares payload (symbol_public_id, exchange) against the active
        version.  Returns existing id when identical (no-op).  When
        different, closes the old version and inserts a new one with
        the same public_id.

        Raises ValueError when no active version exists for the given
        instrument_public_id, or when the target business key is
        already occupied by a different instrument.
        """
        ...

    @abstractmethod
    async def revise_instrument_spec(
        self,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        spec: InstrumentSpecInput,
    ) -> int:
        """SCD2 close+insert for instrument trading specifications.

        Insert if absent, no-op if payload identical, close+insert if
        changed.  Returns id of the active or newly inserted row.
        """
        ...

    @abstractmethod
    async def get_latest_candle_ids(
        self, as_of: datetime
    ) -> dict[tuple[str, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_public_id, timeframe).

        Used by the publisher to populate the in-memory candle ID cache on
        startup so that live upserts reuse existing public_ids for the
        current open_at window.

        Args:
            as_of: Point-in-time for temporal query. Defaults to now.

        Returns:
            Mapping of (instrument_public_id, timeframe) to (open_at, public_id).
        """
        ...

    @abstractmethod
    async def upsert_candles(self, rows: list[CandleUpsertRow]) -> int:
        """Insert or update candles. Return affected row count."""
        ...

    @abstractmethod
    async def upsert_trades(self, rows: list[TradeUpsertRow]) -> int:
        """Insert trades, skipping duplicates. Return inserted count."""
        ...

    @abstractmethod
    async def upsert_ticks(self, rows: list[TickUpsertRow]) -> int:
        """Insert ticks. Return inserted count."""
        ...

    @abstractmethod
    async def insert_order(
        self,
        instrument_public_id: str,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        time_in_force: str | None = None,
    ) -> tuple[int, str]:
        """Insert new order record, returning (id, public_id) tuple."""
        ...

    @abstractmethod
    async def update_order(
        self,
        order_id: int,
        status: str,
        updated_at: datetime,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int:
        """Close old order version and insert new one (SCD Type 2).

        Returns the new version's integer id.
        """
        ...

    @abstractmethod
    async def insert_execution(
        self,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        session_id: str,
        sequence_id: int,
        exec_id: str | None = None,
        trade_id: str | None = None,
    ) -> int:
        """Insert execution record, returning execution ID."""
        ...

    @abstractmethod
    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        exchange: AllExchange,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[CandleRow]:
        """Retrieve candles for instrument.

        Two modes:
        - Range: start and end both provided.
        - Latest-as-of: start and end are None, limit provided.
        """
        ...

    @abstractmethod
    async def get_ticks(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> list[TickRow]:
        """Retrieve ticks for instrument in time range."""
        ...

    @abstractmethod
    async def get_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> list[TradeRow]:
        """Retrieve trades for instrument in time range."""
        ...

    @abstractmethod
    async def get_market_snapshots(
        self,
        instrument_public_ids: list[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[MarketSnapshotRow]:
        """Retrieve active market snapshots for instruments in time range."""
        ...

    @abstractmethod
    async def upsert_market_snapshots(self, rows: list[MarketSnapshotUpsertRow]) -> int:
        """SCD2 close+insert for market snapshots.

        Each row must contain instrument_public_id plus market data fields.
        Closes the active snapshot for the same instrument and inserts a
        new version, preserving public_id across updates.
        """
        ...

    @abstractmethod
    async def get_exchanges(self, as_of: datetime) -> list[str]:
        """Return distinct exchange names from active symbol aliases.

        Args:
            as_of: Point-in-time for temporal query.

        Returns:
            Sorted list of exchange name strings.
        """
        ...

    @abstractmethod
    async def get_exchange_instruments(self, exchange: str, as_of: datetime) -> list[str]:
        """Return distinct native symbols available on a given exchange.

        Args:
            exchange: Exchange name to query instruments for.
            as_of: Point-in-time for temporal query.

        Returns:
            Sorted list of native symbol strings.
        """
        ...

    @abstractmethod
    async def get_signals(
        self,
        since: datetime,
        limit: int,
        as_of: datetime,
        instrument: str | None = None,
        strategy: str | None = None,
        exchange: str | None = None,
    ) -> list[SignalRow]:
        """Retrieve signals with optional filters.

        Args:
            since: Start of time window for fired_at.
            limit: Maximum number of signals to return.
            as_of: Point-in-time for temporal query.
            instrument: Optional native symbol filter.
            strategy: Optional strategy name filter.
            exchange: Optional exchange filter.

        Returns:
            Signal dicts ordered by fired_at DESC, denormalized with
            instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_orders(
        self,
        limit: int,
        offset: int,
        as_of: datetime,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> list[OrderRow]:
        """Retrieve orders with optional filters and pagination.

        Args:
            limit: Maximum number of orders to return.
            offset: Number of orders to skip.
            as_of: Point-in-time for temporal query.
            symbol: Optional native symbol filter.
            exchange: Optional exchange filter.

        Returns:
            Order dicts ordered by created_at DESC, denormalized with
            instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_executions(self, limit: int, as_of: datetime) -> list[ExecutionRow]:
        """Retrieve executions with order/instrument/symbol info.

        Args:
            limit: Maximum number of executions to return.
            as_of: Point-in-time for temporal query.

        Returns:
            Execution dicts ordered by timestamp DESC, denormalized with
            order, instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_active_orders_for_recovery(
        self, exchange: str, as_of: datetime
    ) -> list[OrderRow]:
        """Retrieve non-terminal orders for startup recovery.

        Returns orders with active status (open, pending, pending_new,
        new, partially_filled) for a given exchange. Used exclusively
        by executor and trader recovery, not by API endpoints.

        Args:
            exchange: Exchange name to filter by.
            as_of: Point-in-time for temporal query.

        Returns:
            Active order dicts ordered by created_at ASC (chronological
            for replay), denormalized with instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_executions_for_recovery(
        self, as_of: datetime, exchange: str | None = None, instrument: str | None = None
    ) -> list[ExecutionRow]:
        """Retrieve all executions for startup state reconstruction.

        Unlike get_executions(), this method has no limit and returns
        results in chronological order (ASC) for correct replay.
        Optional exchange/instrument filters narrow the scope.

        Args:
            as_of: Point-in-time for temporal query.
            exchange: Optional exchange filter.
            instrument: Optional native symbol filter.

        Returns:
            Execution dicts ordered by timestamp ASC for replay,
            denormalized with order, instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_positions(self, as_of: datetime) -> list[PositionRow]:
        """Retrieve active positions with instrument/symbol info.

        Args:
            as_of: Point-in-time for temporal query.

        Returns:
            Position dicts denormalized with instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_settings(self, as_of: datetime, category: str | None = None) -> list[SettingRow]:
        """Retrieve active settings, optionally filtered by category.

        Args:
            as_of: Point-in-time for temporal query.
            category: Optional category filter.

        Returns:
            Setting dicts for active settings at as_of.
        """
        ...

    @abstractmethod
    async def get_setting_by_key(self, key: str, as_of: datetime) -> SettingRow | None:
        """Retrieve a single setting by key.

        Args:
            key: Setting key to look up.
            as_of: Point-in-time for temporal query.

        Returns:
            Setting dict or None if not found.
        """
        ...

    @abstractmethod
    async def get_setting_categories(self, as_of: datetime) -> list[str]:
        """Return distinct setting category names.

        Args:
            as_of: Point-in-time for temporal query.

        Returns:
            Sorted list of category name strings.
        """
        ...


def _register_sqlite_fk_pragma(engine: Any) -> None:
    """Register PRAGMA foreign_keys=ON for every new SQLite connection.

    SQLite disables foreign-key enforcement by default; this event
    listener ensures it is enabled on each connection.

    Args:
        engine: Sync or async-sync SQLAlchemy engine to register on.
    """
    if not isinstance(engine, SyncEngine):
        return

    @event.listens_for(engine, "connect")
    def _set_sqlite_fk(dbapi_connection: Any, _connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


class SQLAlchemyRepository(Repository):
    """Async SQLAlchemy repository for SQLite and PostgreSQL.

    Uses SQLAlchemy 2.0 async engine with appropriate connection pooling:
        - SQLite: NullPool or StaticPool for in-memory databases.
        - PostgreSQL: Default pool settings.

    Supports dialect-specific upsert operations with ON CONFLICT handling.

    Attributes:
        db_url: Database connection URL.
        engine: Async SQLAlchemy engine.
        session_factory: Async session factory.
    """

    def __init__(self, db_url: str) -> None:
        """Initialize repository with database URL.

        Args:
            db_url: SQLAlchemy async database URL
                (e.g., 'sqlite+aiosqlite:///./data/db.sqlite').
        """
        self.db_url = db_url
        connect_args: dict[str, Any] = {}
        poolclass: type | None = None
        if "sqlite" in db_url:
            connect_args = {
                "timeout": 30,
                "check_same_thread": False,
            }
            poolclass = StaticPool if ":memory:" in db_url else NullPool
        self.engine: AsyncEngine = create_async_engine(
            db_url, future=True, connect_args=connect_args, poolclass=poolclass
        )
        if "sqlite" in db_url:
            _register_sqlite_fk_pragma(self.engine.sync_engine)
        self.session_factory = async_sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )

    async def create_all(self) -> None:
        """Create all database tables from model metadata."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @property
    def dialect_name(self) -> str:
        """Return the database dialect name."""
        return self.engine.url.get_dialect().name

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Provide async session with automatic rollback on error."""
        async with self.session_factory() as s:
            try:
                yield s
            except GeneratorExit:
                pass
            except Exception:
                await s.rollback()
                raise

    async def get_latest_candle_ids(
        self, as_of: datetime
    ) -> dict[tuple[str, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_public_id, timeframe)."""
        now = as_of
        async with self.session() as s:
            latest = (
                select(
                    Candle.instrument_public_id,
                    Candle.timeframe,
                    func.max(Candle.open_at).label("max_open_at"),
                )
                .where(Candle.timestamp <= now, Candle.known_to > now)
                .group_by(Candle.instrument_public_id, Candle.timeframe)
                .subquery()
            )
            q = await s.execute(
                select(
                    Candle.instrument_public_id,
                    Candle.timeframe,
                    Candle.open_at,
                    Candle.public_id,
                )
                .where(Candle.timestamp <= now, Candle.known_to > now)
                .join(
                    latest,
                    and_(
                        Candle.instrument_public_id == latest.c.instrument_public_id,
                        Candle.timeframe == latest.c.timeframe,
                        Candle.open_at == latest.c.max_open_at,
                    ),
                )
            )
            return {
                (row.instrument_public_id, row.timeframe): (row.open_at, row.public_id)
                for row in q.all()
            }

    async def ensure_instrument(
        self,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[int, str]:
        """Idempotent resolve-or-create for instrument identity.

        Looks up the active Instrument by business key
        (symbol_public_id, exchange) as of *timestamp*.  Returns
        (id, public_id) of the existing row, or inserts a new one
        if none exists.  Never closes an existing version.
        """
        bus_time = timestamp
        async with self.session() as s:
            ts_filter, kt_filter = where_active(Instrument, bus_time)
            q = await s.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == symbol_public_id,
                    Instrument.exchange == exchange,
                    ts_filter,
                    kt_filter,
                )
            )
            inst = q.scalar_one_or_none()
            if inst is not None:
                return (int(inst.id), str(inst.public_id))
            new_inst = Instrument(
                symbol_public_id=symbol_public_id,
                exchange=exchange,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_inst)
            try:
                await s.commit()
            except IntegrityError as exc:
                await s.rollback()
                retry_ts, retry_kt = where_active(Instrument, bus_time)
                q2 = await s.execute(
                    select(Instrument).where(
                        Instrument.symbol_public_id == symbol_public_id,
                        Instrument.exchange == exchange,
                        retry_ts,
                        retry_kt,
                    )
                )
                inst = q2.scalar_one_or_none()
                if inst is None:
                    raise exc
                return (int(inst.id), str(inst.public_id))
            await s.refresh(new_inst)
            return (int(new_inst.id), str(new_inst.public_id))

    async def revise_instrument(
        self,
        instrument_public_id: str,
        symbol_public_id: str,
        exchange: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> int:
        """SCD2 close+insert for instrument business attributes.

        Raises ValueError when no active version exists or when the
        target business key is already occupied by another instrument.
        """
        async with self.session() as s:
            ts_filter, kt_filter = where_active(Instrument, timestamp)
            q = await s.execute(
                select(Instrument).where(
                    Instrument.public_id == instrument_public_id,
                    ts_filter,
                    kt_filter,
                )
            )
            inst = q.scalar_one_or_none()
            if inst is None:
                raise ValueError(f"No active Instrument with public_id={instrument_public_id}")
            if inst.symbol_public_id == symbol_public_id and inst.exchange == exchange:
                return int(inst.id)
            conflict_q = await s.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == symbol_public_id,
                    Instrument.exchange == exchange,
                    ts_filter,
                    kt_filter,
                )
            )
            conflict = conflict_q.scalar_one_or_none()
            if conflict is not None and conflict.public_id != instrument_public_id:
                raise ValueError(
                    f"Business key ({symbol_public_id}, {exchange}) "
                    f"already occupied by instrument {conflict.public_id}"
                )
            new_row = await close_and_insert(
                s,
                Instrument,
                [Instrument.public_id == instrument_public_id],
                {
                    "symbol_public_id": symbol_public_id,
                    "exchange": exchange,
                    "session_id": session_id,
                    "sequence_id": sequence_id,
                },
                timestamp,
            )
            await s.commit()
            await s.refresh(new_row)
            return int(new_row.id)

    async def revise_instrument_spec(
        self,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        spec: InstrumentSpecInput,
    ) -> int:
        """SCD2 close+insert for instrument trading specifications."""
        payload = asdict(spec)
        async with self.session() as s:
            ts_filter, kt_filter = where_active(InstrumentSpec, timestamp)
            q = await s.execute(
                select(InstrumentSpec).where(
                    InstrumentSpec.instrument_public_id == instrument_public_id,
                    ts_filter,
                    kt_filter,
                )
            )
            existing_spec = q.scalar_one_or_none()
            if existing_spec is not None:
                same = all(getattr(existing_spec, k) == v for k, v in payload.items())
                if same:
                    return int(existing_spec.id)
            new_values = {
                "instrument_public_id": instrument_public_id,
                "session_id": session_id,
                "sequence_id": sequence_id,
                **payload,
            }
            new_row = await close_and_insert(
                s,
                InstrumentSpec,
                [InstrumentSpec.instrument_public_id == instrument_public_id],
                new_values,
                timestamp,
            )
            await s.commit()
            await s.refresh(new_row)
            return int(new_row.id)

    async def _upsert_batch(
        self, model: type[Base], rows: list[Any], index_elements: list[str]
    ) -> int:
        """Dialect-aware batch upsert with conflict-do-nothing.

        Uses SQLite/PostgreSQL native INSERT ... ON CONFLICT DO NOTHING
        when available, falling back to row-by-row IntegrityError handling.

        Args:
            model: SQLAlchemy model class to insert into.
            rows: List of column-value dicts.
            index_elements: Columns that form the unique constraint.

        Returns:
            Number of rows successfully inserted.
        """
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
        name = self.dialect_name
        if name == "sqlite":
            async with self.session() as s:
                stmt_sq = sqlite_insert(model).values(rows)
                stmt_sq = stmt_sq.on_conflict_do_nothing(index_elements=index_elements)
                res = await s.execute(stmt_sq)
                await s.commit()
                return int(cast(Any, res).rowcount or 0)
        elif name.startswith("postgres"):
            async with self.session() as s:
                stmt_pg = pg_insert(model).values(rows)
                stmt_pg = stmt_pg.on_conflict_do_nothing(index_elements=index_elements)
                res = await s.execute(stmt_pg)
                await s.commit()
                return int(cast(Any, res).rowcount or 0)
        else:
            async with self.session() as s:
                inserted = 0
                for r in rows:
                    try:
                        async with s.begin_nested():
                            await s.execute(insert(model).values(**r))
                        inserted += 1
                    except IntegrityError:
                        continue
                await s.commit()
                return inserted

    async def upsert_candles(self, rows: list[CandleUpsertRow]) -> int:
        """Close-old + insert-new (SCD Type 2) for candle rows.

        When a candle with the same (instrument_public_id, timeframe, open_at)
        already exists as an active row (known_to == KNOWN_TO_MAX), the old row
        is closed by setting its known_to to now, and a new row is inserted
        carrying the same public_id.  This preserves full history of
        intra-interval updates.

        Rows without ``public_id`` get a generated UUID7 automatically.
        """
        if not rows:
            return 0
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
            if "known_to" not in r:
                r["known_to"] = KNOWN_TO_MAX
        async with self.session() as s:
            count = 0
            for r in rows:
                bus_time = r["timestamp"]
                existing = (
                    (
                        await s.execute(
                            select(Candle)
                            .where(
                                Candle.instrument_public_id == r["instrument_public_id"],
                                Candle.timeframe == r["timeframe"],
                                Candle.open_at == r["open_at"],
                                Candle.timestamp <= bus_time,
                                Candle.known_to > bus_time,
                            )
                            .with_for_update()
                        )
                    )
                    .scalars()
                    .first()
                )
                if existing:
                    await s.execute(
                        update(Candle).where(Candle.id == existing.id).values(known_to=bus_time)
                    )
                    r["public_id"] = existing.public_id
                s.add(Candle(**r))
                count += 1
            await s.commit()
        return count

    async def upsert_trades(self, rows: list[TradeUpsertRow]) -> int:
        """Insert trades with dialect-specific conflict handling."""
        if not rows:
            return 0
        return await self._upsert_batch(Trade, rows, ["trade_id"])

    async def upsert_ticks(self, rows: list[TickUpsertRow]) -> int:
        """Insert ticks as append-only (no dedup key)."""
        if not rows:
            return 0
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
        async with self.session() as s:
            s.add_all([Tick(**r) for r in rows])
            await s.commit()
            return len(rows)

    async def insert_order(
        self,
        instrument_public_id: str,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        time_in_force: str | None = None,
    ) -> tuple[int, str]:
        """Insert new order record and return (id, public_id) tuple."""
        async with self.session() as s:
            order = Order(
                instrument_public_id=instrument_public_id,
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                created_at=created_at,
                updated_at=None,
                timestamp=timestamp,
                side=side,
                order_type=order_type,
                price=price,
                size=size,
                filled_size=0.0,
                average_price=None,
                status=status,
                time_in_force=time_in_force,
                error=None,
                session_id=session_id,
                sequence_id=sequence_id,
            )
            s.add(order)
            await s.commit()
            await s.refresh(order)
            return (order.id, order.public_id)

    async def update_order(
        self,
        order_id: int,
        status: str,
        updated_at: datetime,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int:
        """Close old order version and insert new one (SCD Type 2)."""
        async with self.session() as s:
            old_order = (
                (await s.execute(select(Order).where(Order.id == order_id).with_for_update()))
                .scalars()
                .one()
            )
            await s.execute(update(Order).where(Order.id == order_id).values(known_to=timestamp))
            new_order = Order(
                public_id=old_order.public_id,
                instrument_public_id=old_order.instrument_public_id,
                client_order_id=old_order.client_order_id,
                exchange_order_id=exchange_order_id or old_order.exchange_order_id,
                created_at=old_order.created_at,
                updated_at=updated_at,
                timestamp=timestamp,
                side=old_order.side,
                order_type=old_order.order_type,
                price=old_order.price,
                size=old_order.size,
                filled_size=filled_size if filled_size is not None else old_order.filled_size,
                average_price=(
                    average_price if average_price is not None else old_order.average_price
                ),
                status=status,
                time_in_force=old_order.time_in_force,
                error=error,
                session_id=session_id,
                sequence_id=sequence_id,
            )
            s.add(new_order)
            await s.commit()
            await s.refresh(new_order)
            return new_order.id

    async def insert_execution(
        self,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        session_id: str,
        sequence_id: int,
        exec_id: str | None = None,
        trade_id: str | None = None,
    ) -> int:
        """Insert execution record and return generated ID."""
        async with self.session() as s:
            execution = Execution(
                order_public_id=order_public_id,
                exec_id=exec_id,
                trade_id=trade_id,
                timestamp=timestamp,
                side=side,
                status=status,
                price=price,
                size=size,
                fee=fee,
                fee_asset=fee_asset,
                session_id=session_id,
                sequence_id=sequence_id,
            )
            s.add(execution)
            await s.commit()
            await s.refresh(execution)
            return execution.id

    async def _resolve_active_instrument(
        self,
        session: AsyncSession,
        native_symbol: str,
        exchange: str,
        as_of: datetime,
    ) -> Instrument | None:
        """Resolve active Instrument from native symbol and exchange.

        Args:
            session: Active database session.
            native_symbol: Canonical symbol string (e.g. 'BTC-USD').
            exchange: Exchange name.
            as_of: Point-in-time for temporal query.

        Returns:
            Active Instrument or None if symbol/instrument not found.
        """
        s_ts, s_kt = where_active(Symbol, as_of)
        sym_q = await session.execute(
            select(Symbol.public_id).where(Symbol.native_symbol == native_symbol, s_ts, s_kt)
        )
        symbol_pid = sym_q.scalar_one_or_none()
        if symbol_pid is None:
            return None
        i_ts, i_kt = where_active(Instrument, as_of)
        q_inst = await session.execute(
            select(Instrument).where(
                Instrument.symbol_public_id == symbol_pid,
                Instrument.exchange == exchange,
                i_ts,
                i_kt,
            )
        )
        return q_inst.scalars().first()

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime | None,
        end: datetime | None,
        exchange: AllExchange,
        as_of: datetime,
        limit: int | None = None,
        order: str = "asc",
    ) -> list[CandleRow]:
        """Retrieve active candles for instrument.

        Supports two modes:
        - Range mode: start and end both provided.
        - Latest-as-of mode: start/end are None, limit provided.
        """
        async with self.session() as s:
            inst = await self._resolve_active_instrument(s, instrument, exchange, as_of)
            if inst is None:
                return []
            q = select(
                Candle.open_at,
                Candle.timeframe,
                Candle.open,
                Candle.high,
                Candle.low,
                Candle.close,
                Candle.volume,
                Candle.vwap,
                Candle.trades,
                Candle.public_id,
                Candle.timestamp,
                Candle.session_id,
                Candle.sequence_id,
            ).where(
                Candle.instrument_public_id == inst.public_id,
                Candle.timeframe == timeframe,
                Candle.timestamp <= as_of,
                Candle.known_to > as_of,
            )
            if start is not None and end is not None:
                q = q.where(Candle.open_at >= start, Candle.open_at <= end)
            order_col = Candle.open_at.desc() if order == "desc" else Candle.open_at.asc()
            q = q.order_by(order_col)
            if limit is not None:
                q = q.limit(limit)
            rows = (await s.execute(q)).all()
            return [
                {
                    "open_at": r.open_at,
                    "timeframe": r.timeframe,
                    "open": r.open,
                    "high": r.high,
                    "low": r.low,
                    "close": r.close,
                    "volume": r.volume,
                    "vwap": r.vwap,
                    "trades": r.trades,
                    "public_id": r.public_id,
                    "timestamp": r.timestamp,
                    "session_id": r.session_id,
                    "sequence_id": r.sequence_id,
                }
                for r in rows
            ]

    async def get_ticks(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> list[TickRow]:
        """Retrieve ticks for instrument within time range."""
        async with self.session() as s:
            inst = await self._resolve_active_instrument(s, instrument, exchange, as_of)
            if inst is None:
                return []
            q = await s.execute(
                select(
                    Tick.timestamp,
                    Tick.bid,
                    Tick.ask,
                    Tick.last,
                    Tick.volume,
                    Tick.public_id,
                    Tick.session_id,
                    Tick.sequence_id,
                )
                .where(
                    Tick.instrument_public_id == inst.public_id,
                    Tick.timestamp >= start,
                    Tick.timestamp <= end,
                    Tick.timestamp <= as_of,
                    Tick.known_to > as_of,
                )
                .order_by(Tick.timestamp.asc())
            )
            rows = q.all()
            return [
                {
                    "timestamp": r.timestamp,
                    "bid": r.bid,
                    "ask": r.ask,
                    "last": r.last,
                    "volume": r.volume,
                    "public_id": r.public_id,
                    "session_id": r.session_id,
                    "sequence_id": r.sequence_id,
                }
                for r in rows
            ]

    async def get_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> list[TradeRow]:
        """Retrieve trades for instrument within time range.

        Uses coalesce(executed_at, timestamp) for range filtering and ordering
        so that trades are selected by exchange event time when available,
        falling back to bus-time for legacy rows without executed_at.
        """
        async with self.session() as s:
            inst = await self._resolve_active_instrument(s, instrument, exchange, as_of)
            if inst is None:
                return []
            event_time = func.coalesce(Trade.executed_at, Trade.timestamp)
            q = await s.execute(
                select(
                    Trade.timestamp,
                    Trade.executed_at,
                    Trade.price,
                    Trade.size,
                    Trade.side,
                    Trade.trade_id,
                )
                .where(
                    Trade.instrument_public_id == inst.public_id,
                    event_time >= start,
                    event_time <= end,
                    Trade.timestamp <= as_of,
                    Trade.known_to > as_of,
                )
                .order_by(event_time.asc())
            )
            rows = q.all()
            return [
                {
                    "timestamp": r.timestamp,
                    "executed_at": r.executed_at,
                    "price": r.price,
                    "size": r.size,
                    "side": r.side,
                    "trade_id": r.trade_id,
                }
                for r in rows
            ]

    async def get_market_snapshots(
        self,
        instrument_public_ids: list[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[MarketSnapshotRow]:
        """Retrieve active market snapshots for instruments in time range."""
        now = as_of
        async with self.session() as s:
            q = await s.execute(
                select(
                    MarketSnapshot.timestamp,
                    MarketSnapshot.instrument_public_id,
                    MarketSnapshot.bid,
                    MarketSnapshot.bid_volume,
                    MarketSnapshot.ask,
                    MarketSnapshot.ask_volume,
                    MarketSnapshot.last_price,
                    MarketSnapshot.volume_24h,
                    MarketSnapshot.vwap_24h,
                    MarketSnapshot.low_24h,
                    MarketSnapshot.high_24h,
                )
                .where(
                    MarketSnapshot.instrument_public_id.in_(instrument_public_ids),
                    MarketSnapshot.timestamp >= start,
                    MarketSnapshot.timestamp <= end,
                    MarketSnapshot.timestamp <= now,
                    MarketSnapshot.known_to > now,
                )
                .order_by(MarketSnapshot.timestamp.asc())
            )
            rows = q.all()
            return [
                {
                    "ts": r.timestamp,
                    "instrument_public_id": r.instrument_public_id,
                    "bid": r.bid,
                    "bid_volume": r.bid_volume,
                    "ask": r.ask,
                    "ask_volume": r.ask_volume,
                    "last": r.last_price,
                    "volume": r.volume_24h,
                    "vwap": r.vwap_24h,
                    "low": r.low_24h,
                    "high": r.high_24h,
                }
                for r in rows
            ]

    async def upsert_market_snapshots(self, rows: list[MarketSnapshotUpsertRow]) -> int:
        """SCD2 close+insert for market snapshots.

        One active row per instrument_public_id.  Closes the existing
        active snapshot and inserts a new version with the same public_id.
        """
        if not rows:
            return 0
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
            if "known_to" not in r:
                r["known_to"] = KNOWN_TO_MAX
        async with self.session() as s:
            count = 0
            for r in rows:
                bus_time = r["timestamp"]
                existing = (
                    (
                        await s.execute(
                            select(MarketSnapshot)
                            .where(
                                MarketSnapshot.instrument_public_id == r["instrument_public_id"],
                                MarketSnapshot.timestamp <= bus_time,
                                MarketSnapshot.known_to > bus_time,
                            )
                            .with_for_update()
                        )
                    )
                    .scalars()
                    .first()
                )
                if existing:
                    await s.execute(
                        update(MarketSnapshot)
                        .where(MarketSnapshot.id == existing.id)
                        .values(known_to=bus_time)
                    )
                    r["public_id"] = existing.public_id
                s.add(MarketSnapshot(**r))
                count += 1
            await s.commit()
        return count

    async def get_exchanges(self, as_of: datetime) -> list[str]:
        """Return distinct exchange names from active symbol aliases."""
        async with self.session() as s:
            result = await s.execute(
                select(distinct(SymbolAlias.exchange))
                .where(*where_active(SymbolAlias, as_of))
                .order_by(SymbolAlias.exchange)
            )
            return list(result.scalars().all())

    async def get_exchange_instruments(self, exchange: str, as_of: datetime) -> list[str]:
        """Return distinct native symbols available on a given exchange."""
        async with self.session() as s:
            result = await s.execute(
                select(distinct(Symbol.native_symbol))
                .select_from(SymbolAlias)
                .join(Symbol, Symbol.public_id == SymbolAlias.symbol_public_id)
                .where(
                    SymbolAlias.exchange == exchange,
                    *where_active(SymbolAlias, as_of),
                    *where_active(Symbol, as_of),
                )
                .order_by(Symbol.native_symbol)
            )
            return list(result.scalars().all())

    async def get_signals(
        self,
        since: datetime,
        limit: int,
        as_of: datetime,
        instrument: str | None = None,
        strategy: str | None = None,
        exchange: str | None = None,
    ) -> list[SignalRow]:
        """Retrieve signals with optional filters."""
        async with self.session() as s:
            query = (
                select(Signal, Instrument, Symbol)
                .join(
                    Instrument,
                    and_(
                        Signal.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Instrument.symbol_public_id == Symbol.public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(
                    Signal.fired_at >= since,
                    *where_active(Signal, as_of),
                )
            )
            if instrument:
                s_ts, s_kt = where_active(Symbol, as_of)
                sym_subq = (
                    select(Symbol.public_id)
                    .where(Symbol.native_symbol == instrument, s_ts, s_kt)
                    .scalar_subquery()
                )
                query = query.where(Instrument.symbol_public_id == sym_subq)
            if strategy:
                query = query.where(Signal.strategy_name == strategy)
            if exchange:
                query = query.where(Instrument.exchange == exchange)
            query = query.order_by(desc(Signal.fired_at)).limit(limit)
            result = await s.execute(query)
            return [
                {
                    "public_id": sig.public_id,
                    "timestamp": sig.timestamp,
                    "session_id": sig.session_id,
                    "sequence_id": sig.sequence_id,
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "side": sig.side,
                    "strength": sig.strength,
                    "reason": sig.reason,
                    "strategy_name": sig.strategy_name,
                    "price": sig.price,
                    "fired_at": sig.fired_at,
                }
                for sig, inst, sym in result.all()
            ]

    async def get_orders(
        self,
        limit: int,
        offset: int,
        as_of: datetime,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> list[OrderRow]:
        """Retrieve orders with optional filters and pagination."""
        async with self.session() as s:
            query = (
                select(Order, Instrument, Symbol)
                .join(
                    Instrument,
                    and_(
                        Order.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Instrument.symbol_public_id == Symbol.public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(*where_active(Order, as_of))
            )
            if symbol:
                s_ts, s_kt = where_active(Symbol, as_of)
                sym_subq = (
                    select(Symbol.public_id)
                    .where(Symbol.native_symbol == symbol, s_ts, s_kt)
                    .scalar_subquery()
                )
                query = query.where(Instrument.symbol_public_id == sym_subq)
            if exchange:
                query = query.where(Instrument.exchange == exchange)
            query = query.order_by(desc(Order.created_at)).offset(offset).limit(limit)
            result = await s.execute(query)
            return [
                {
                    "public_id": order.public_id,
                    "timestamp": order.timestamp,
                    "session_id": order.session_id,
                    "sequence_id": order.sequence_id,
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "client_order_id": order.client_order_id or "",
                    "exchange_order_id": order.exchange_order_id,
                    "created_at": order.created_at,
                    "updated_at": order.updated_at,
                    "side": order.side,
                    "order_type": order.order_type,
                    "price": order.price,
                    "size": order.size,
                    "filled_size": order.filled_size,
                    "average_price": order.average_price,
                    "status": order.status,
                    "time_in_force": order.time_in_force,
                    "error": order.error,
                }
                for order, inst, sym in result.all()
            ]

    async def get_executions(self, limit: int, as_of: datetime) -> list[ExecutionRow]:
        """Retrieve executions with order/instrument/symbol info."""
        async with self.session() as s:
            query = (
                select(Execution, Order, Instrument, Symbol)
                .join(
                    Order,
                    and_(
                        Execution.order_public_id == Order.public_id,
                        *where_active(Order, as_of),
                    ),
                )
                .join(
                    Instrument,
                    and_(
                        Order.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Instrument.symbol_public_id == Symbol.public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(*where_active(Execution, as_of))
                .order_by(desc(Execution.timestamp))
                .limit(limit)
            )
            result = await s.execute(query)
            return [
                {
                    "public_id": exe.public_id,
                    "timestamp": exe.timestamp,
                    "session_id": exe.session_id,
                    "sequence_id": exe.sequence_id,
                    "trade_id": exe.trade_id,
                    "exchange_order_id": order.exchange_order_id,
                    "client_order_id": order.client_order_id or "",
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "side": exe.side,
                    "size": exe.size,
                    "price": exe.price,
                    "fee": exe.fee,
                    "fee_asset": exe.fee_asset,
                    "status": exe.status,
                    "executed_at": exe.executed_at or exe.timestamp,
                }
                for exe, order, inst, sym in result.all()
            ]

    _ACTIVE_ORDER_STATUSES = ("open", "pending", "pending_new", "new", "partially_filled")

    async def get_active_orders_for_recovery(
        self, exchange: str, as_of: datetime
    ) -> list[OrderRow]:
        """Retrieve non-terminal orders for startup recovery."""
        async with self.session() as s:
            query = (
                select(Order, Instrument, Symbol)
                .join(
                    Instrument,
                    and_(
                        Order.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Instrument.symbol_public_id == Symbol.public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(
                    *where_active(Order, as_of),
                    Instrument.exchange == exchange,
                    Order.status.in_(self._ACTIVE_ORDER_STATUSES),
                )
                .order_by(Order.created_at)
            )
            result = await s.execute(query)
            return [
                {
                    "public_id": order.public_id,
                    "timestamp": order.timestamp,
                    "session_id": order.session_id,
                    "sequence_id": order.sequence_id,
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "client_order_id": order.client_order_id or "",
                    "exchange_order_id": order.exchange_order_id,
                    "created_at": order.created_at,
                    "updated_at": order.updated_at,
                    "side": order.side,
                    "order_type": order.order_type,
                    "price": order.price,
                    "size": order.size,
                    "filled_size": order.filled_size,
                    "average_price": order.average_price,
                    "status": order.status,
                    "time_in_force": order.time_in_force,
                    "error": order.error,
                }
                for order, inst, sym in result.all()
            ]

    async def get_executions_for_recovery(
        self, as_of: datetime, exchange: str | None = None, instrument: str | None = None
    ) -> list[ExecutionRow]:
        """Retrieve all executions for startup state reconstruction."""
        async with self.session() as s:
            query = (
                select(Execution, Order, Instrument, Symbol)
                .join(
                    Order,
                    and_(
                        Execution.order_public_id == Order.public_id,
                        *where_active(Order, as_of),
                    ),
                )
                .join(
                    Instrument,
                    and_(
                        Order.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Instrument.symbol_public_id == Symbol.public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(*where_active(Execution, as_of))
                .order_by(Execution.timestamp)
            )
            if exchange:
                query = query.where(Instrument.exchange == exchange)
            if instrument:
                s_ts, s_kt = where_active(Symbol, as_of)
                sym_subq = (
                    select(Symbol.public_id)
                    .where(Symbol.native_symbol == instrument, s_ts, s_kt)
                    .scalar_subquery()
                )
                query = query.where(Instrument.symbol_public_id == sym_subq)
            result = await s.execute(query)
            return [
                {
                    "public_id": exe.public_id,
                    "timestamp": exe.timestamp,
                    "session_id": exe.session_id,
                    "sequence_id": exe.sequence_id,
                    "trade_id": exe.trade_id,
                    "exchange_order_id": order.exchange_order_id,
                    "client_order_id": order.client_order_id or "",
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "side": exe.side,
                    "size": exe.size,
                    "price": exe.price,
                    "fee": exe.fee,
                    "fee_asset": exe.fee_asset,
                    "status": exe.status,
                    "executed_at": exe.executed_at or exe.timestamp,
                }
                for exe, order, inst, sym in result.all()
            ]

    async def get_positions(self, as_of: datetime) -> list[PositionRow]:
        """Retrieve active positions with instrument/symbol info."""
        async with self.session() as s:
            query = (
                select(Position, Instrument, Symbol)
                .join(
                    Instrument,
                    and_(
                        Position.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Instrument.symbol_public_id == Symbol.public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(*where_active(Position, as_of))
            )
            result = await s.execute(query)
            return [
                {
                    "public_id": pos.public_id,
                    "timestamp": pos.timestamp,
                    "session_id": pos.session_id,
                    "sequence_id": pos.sequence_id,
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "quantity": pos.quantity,
                    "average_price": pos.average_price,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "realized_pnl": pos.realized_pnl,
                }
                for pos, inst, sym in result.all()
            ]

    async def get_settings(self, as_of: datetime, category: str | None = None) -> list[SettingRow]:
        """Retrieve active settings, optionally filtered by category."""
        async with self.session() as s:
            query = select(Setting).where(*where_active(Setting, as_of))
            if category:
                query = query.where(Setting.category == category)
            result = await s.execute(query)
            return [
                {
                    "public_id": setting.public_id,
                    "timestamp": setting.timestamp,
                    "session_id": setting.session_id,
                    "sequence_id": setting.sequence_id,
                    "key": setting.key,
                    "value": setting.value,
                    "category": setting.category,
                    "description": setting.description,
                    "updated_by": setting.updated_by,
                }
                for setting in result.scalars().all()
            ]

    async def get_setting_by_key(self, key: str, as_of: datetime) -> SettingRow | None:
        """Retrieve a single setting by key."""
        async with self.session() as s:
            result = await s.execute(
                select(Setting).where(Setting.key == key, *where_active(Setting, as_of))
            )
            setting = result.scalars().first()
            if setting is None:
                return None
            row: SettingRow = {
                "public_id": setting.public_id,
                "timestamp": setting.timestamp,
                "session_id": setting.session_id,
                "sequence_id": setting.sequence_id,
                "key": setting.key,
                "value": setting.value,
                "category": setting.category,
                "description": setting.description,
                "updated_by": setting.updated_by,
            }
            return row

    async def get_setting_categories(self, as_of: datetime) -> list[str]:
        """Return distinct setting category names."""
        async with self.session() as s:
            result = await s.execute(
                select(Setting.category).where(*where_active(Setting, as_of)).distinct()
            )
            return sorted(row[0] for row in result.fetchall())


_repository_cache: dict[str, Repository] = {}


def get_repository(db_url: str) -> Repository:
    """Get or create a cached repository instance.

    Returns an existing repository for the given URL or creates a new one.

    Args:
        db_url: Database connection URL.

    Returns:
        Cached or newly created Repository instance.
    """
    if db_url not in _repository_cache:
        _repository_cache[db_url] = SQLAlchemyRepository(db_url)
    return _repository_cache[db_url]


async def dispose_repositories() -> None:
    """Dispose all cached repository engines.

    Should be called during application shutdown to properly close
    database connections and release resources.
    """
    for repo in _repository_cache.values():
        engine = getattr(repo, "engine", None)
        if engine is None:
            continue
        try:
            dispose_result = engine.dispose()
            if isawaitable(dispose_result):
                await dispose_result
        except Exception as e:
            logger.warning(f"Failed to dispose repository engine: {e}")
    _repository_cache.clear()


class DatabaseRepository:
    """Simple synchronous repository for scripts and notebooks.

    Provides direct sync access to the database without async wrappers.
    Useful for data analysis, migrations, and one-off scripts.

    Attributes:
        db_url: Sync-converted database URL.
        engine: Sync SQLAlchemy engine.
        session_factory: Sync session factory.
    """

    def __init__(self, db_url: str) -> None:
        """Initialize sync database repository.

        Args:
            db_url: Database URL (will be converted to sync driver if async).
        """
        self.db_url = self._convert_to_sync_url(db_url)
        self.engine: SyncEngine = create_sync_engine(self.db_url, future=True)
        if "sqlite" in self.db_url:
            _register_sqlite_fk_pragma(self.engine)
        self.session_factory = sync_sessionmaker(
            self.engine, expire_on_commit=False, class_=SyncSession
        )

    @staticmethod
    def _convert_to_sync_url(db_url: str) -> str:
        """Convert an async database URL to its synchronous equivalent.

        Args:
            db_url: Possibly async database URL.

        Returns:
            Sync-compatible database URL.
        """
        if db_url.startswith("sqlite+aiosqlite://"):
            return db_url.replace("sqlite+aiosqlite://", "sqlite://")
        if db_url.startswith("postgresql+asyncpg://"):
            return db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
        return db_url

    def get_session(self) -> SyncSession:
        """Create and return a new database session."""
        return self.session_factory()

    def create_all(self) -> None:
        """Create all database tables from model metadata."""
        Base.metadata.create_all(self.engine)

    def get_archive_symbols(self) -> dict[str, str]:
        """Resolve stable archive symbols for all Symbol entities.

        Queries the anchor row (first version) of each Symbol, ordered
        by ``(timestamp, id)`` for deterministic seniority.  Delegates
        to ``resolve_archive_symbols`` for normalization and collision
        handling.

        Returns:
            ``{symbol_public_id: archive_symbol}`` mapping.
        """
        with self.get_session() as session:
            stmt = select(
                Symbol.public_id,
                Symbol.native_symbol,
                Symbol.timestamp,
                Symbol.id,
            ).order_by(Symbol.timestamp, Symbol.id)
            all_rows = session.execute(stmt).all()
        seen: dict[str, tuple[str, datetime, int]] = {}
        for pub_id, native, ts, row_id in all_rows:
            if pub_id not in seen or (ts, row_id) < (seen[pub_id][1], seen[pub_id][2]):
                seen[pub_id] = (native, ts, row_id)
        anchor_rows: list[tuple[str, str]] = []
        ordered = sorted(seen.items(), key=lambda kv: (kv[1][1], kv[1][2]))
        for pub_id, (native, _ts, _rid) in ordered:
            anchor_rows.append((pub_id, native))
        return resolve_archive_symbols(anchor_rows)

    def get_symbol_anchor_ids(self) -> set[int]:
        """Return row IDs of Symbol anchor rows (first version per public_id).

        Uses the same selection logic as ``get_archive_symbols`` to ensure
        identical anchor row identification.  These rows must be excluded
        from Symbol purge to preserve archive_symbol stability.

        Returns:
            Set of ``Symbol.id`` values for anchor rows.
        """
        with self.get_session() as session:
            stmt = select(
                Symbol.public_id,
                Symbol.timestamp,
                Symbol.id,
            ).order_by(Symbol.timestamp, Symbol.id)
            all_rows = session.execute(stmt).all()
        seen: dict[str, tuple[datetime, int]] = {}
        for pub_id, ts, row_id in all_rows:
            if pub_id not in seen or (ts, row_id) < seen[pub_id]:
                seen[pub_id] = (ts, row_id)
        return {row_id for _ts, row_id in seen.values()}

    def resolve_native_to_archive_symbol(self, native_symbol: str) -> str | None:
        """Resolve a current native_symbol to its stable archive_symbol.

        Looks up the active Symbol row by native_symbol, then maps its
        public_id through the archive_symbols mapping.  Works correctly
        even after symbol renames.

        Args:
            native_symbol: Current native symbol name (e.g. ``BTC-USD``).

        Returns:
            Archive symbol string, or None if no active Symbol found.
        """
        archive_symbols = self.get_archive_symbols()
        with self.get_session() as session:
            now = datetime.now(UTC)
            ts_filter, kt_filter = where_active(Symbol, now)
            row = session.execute(
                select(Symbol.public_id).where(
                    Symbol.native_symbol == native_symbol,
                    ts_filter,
                    kt_filter,
                )
            ).scalar_one_or_none()
        if row is None:
            return None
        return archive_symbols.get(row)

    def get_instrument_archive_map(self) -> dict[str, tuple[str, str]]:
        """Build mapping from instrument_public_id to (archive_symbol, exchange).

        Joins active Instrument rows with Symbol anchor rows to resolve
        stable archive paths for each instrument.

        Returns:
            ``{instrument_public_id: (archive_symbol, exchange)}`` mapping.
        """
        archive_symbols = self.get_archive_symbols()
        with self.get_session() as session:
            now = datetime.now(UTC)
            ts_filter, kt_filter = where_active(Instrument, now)
            rows = session.execute(
                select(
                    Instrument.public_id,
                    Instrument.symbol_public_id,
                    Instrument.exchange,
                ).where(ts_filter, kt_filter)
            ).all()
        result: dict[str, tuple[str, str]] = {}
        for inst_pub_id, sym_pub_id, exchange in rows:
            arch_sym = archive_symbols.get(sym_pub_id)
            if arch_sym is not None:
                result[inst_pub_id] = (arch_sym, exchange)
        return result

    def get_candles_for_cache_export(
        self,
        instrument_public_id: str,
        timeframe: str,
        day_start: date,
        day_end: date,
    ) -> list[tuple[datetime, float, float, float, float, float, float | None, int | None]]:
        """Query latest candle versions for cache projection export.

        Returns only the active version (``known_to = KNOWN_TO_MAX``) per
        ``(instrument_public_id, timeframe, open_at)``, sorted by ``open_at``.
        Output tuples match the Polygon CSV column order.

        Args:
            instrument_public_id: Instrument to export candles for.
            timeframe: Candle timeframe (e.g. ``1m``, ``1h``, ``1d``).
            day_start: First day (inclusive) of ``open_at`` range.
            day_end: Last day (inclusive) of ``open_at`` range.

        Returns:
            List of ``(open_at, open, high, low, close, volume, vwap, trades)``
            tuples sorted by ``open_at ASC``.
        """
        from_dt = datetime.combine(day_start, datetime.min.time(), tzinfo=UTC)
        to_dt = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        with self.get_session() as session:
            rows = session.execute(
                select(
                    Candle.open_at,
                    Candle.open,
                    Candle.high,
                    Candle.low,
                    Candle.close,
                    Candle.volume,
                    Candle.vwap,
                    Candle.trades,
                )
                .where(
                    Candle.instrument_public_id == instrument_public_id,
                    Candle.timeframe == timeframe,
                    Candle.open_at >= from_dt,
                    Candle.open_at < to_dt,
                    Candle.known_to == KNOWN_TO_MAX,
                )
                .order_by(Candle.open_at)
            ).all()
        return [tuple(r) for r in rows]

    def get_event_rows_for_archive(
        self,
        model: type[Any],
        columns: tuple[str, ...],
        day_start: date,
        day_end: date,
    ) -> list[tuple[Any, ...]]:
        """Query append-only event rows in a timestamp range.

        Returns ``(id, *column_values)`` tuples where column order matches
        the supplied *columns* tuple.  The ``id`` is included as the first
        element for purge tracking but should be excluded from CSV output.

        Args:
            model: SQLAlchemy model class (e.g. Tick, Trade, Signal).
            columns: Column names to select, in desired CSV order.
            day_start: First day (inclusive) of ``timestamp`` range.
            day_end: Last day (inclusive) of ``timestamp`` range.

        Returns:
            List of ``(id, col1, col2, ...)`` tuples sorted by
            ``(timestamp, id)``.
        """
        from_dt = datetime.combine(day_start, datetime.min.time(), tzinfo=UTC)
        to_dt = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        col_attrs = [getattr(model, name) for name in columns]
        with self.get_session() as session:
            rows = session.execute(
                select(model.id, *col_attrs)
                .where(
                    model.timestamp >= from_dt,
                    model.timestamp < to_dt,
                )
                .order_by(model.timestamp, model.id)
            ).all()
        return [tuple(r) for r in rows]

    def get_scd2_rows_for_archive(
        self,
        model: type[Any],
        columns: tuple[str, ...],
        day_start: date,
        day_end: date,
        closed_only: bool = False,
    ) -> list[tuple[Any, ...]]:
        """Query state-SCD2 rows in a timestamp range.

        Similar to ``get_event_rows_for_archive`` but supports
        ``closed_only`` filtering for SCD2 tables where ``known_to``
        is meaningful (not always ``KNOWN_TO_MAX``).

        Args:
            model: SQLAlchemy model class.
            columns: Column names to select, in desired CSV order.
            day_start: First day (inclusive) of ``timestamp`` range.
            day_end: Last day (inclusive) of ``timestamp`` range.
            closed_only: If True, only rows with
                ``known_to < now`` (closed versions).

        Returns:
            List of ``(id, col1, col2, ...)`` tuples sorted by
            ``(timestamp, id)``.
        """
        from_dt = datetime.combine(day_start, datetime.min.time(), tzinfo=UTC)
        to_dt = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        conditions = [
            model.timestamp >= from_dt,
            model.timestamp < to_dt,
        ]
        if closed_only:
            conditions.append(model.known_to < datetime.now(UTC))
        col_attrs = [getattr(model, name) for name in columns]
        with self.get_session() as session:
            rows = session.execute(
                select(model.id, *col_attrs).where(*conditions).order_by(model.timestamp, model.id)
            ).all()
        return [tuple(r) for r in rows]

    def delete_rows_by_id(self, model: type[Any], row_ids: list[int]) -> int:
        """Delete rows by primary key id in batches.

        Uses batch size of 500 to stay within SQLite parameter limits.

        Args:
            model: SQLAlchemy model class.
            row_ids: List of ``id`` values to delete.

        Returns:
            Number of rows actually deleted.
        """
        if not row_ids:
            return 0
        total = 0
        with self.get_session() as session:
            for i in range(0, len(row_ids), 500):
                batch = row_ids[i : i + 500]
                result = session.execute(delete(model).where(model.id.in_(batch)))
                total += cast(CursorResult[Any], result).rowcount
            session.commit()
        return total

    def get_order_archive_map(self) -> dict[str, tuple[str, str]]:
        """Map order_public_id to (archive_symbol, exchange) via instrument.

        Joins all Order rows (active and closed) through their
        ``instrument_public_id`` to the instrument archive map.
        Needed for Execution archiving, which references orders
        rather than instruments directly.

        Returns:
            ``{order_public_id: (archive_symbol, exchange)}`` mapping.
        """
        inst_map = self.get_instrument_archive_map()
        with self.get_session() as session:
            rows = session.execute(
                select(Order.public_id, Order.instrument_public_id).distinct()
            ).all()
        result: dict[str, tuple[str, str]] = {}
        for order_pub_id, inst_pub_id in rows:
            inst_info = inst_map.get(inst_pub_id)
            if inst_info is not None:
                result[order_pub_id] = inst_info
        return result

    def get_candle_versions_for_archive(
        self,
        instrument_public_id: str,
        timeframe: str,
        day_start: date,
        day_end: date,
        closed_only: bool = False,
    ) -> list[tuple[Any, ...]]:
        """Query candle versions for audit archive.

        Returns all SCD2 versions (closed and/or active) within the
        ``open_at`` date range.  Filters by ``open_at`` (candle time),
        not ``timestamp`` (bus_time), so a correction at T+1 for a
        candle at open_at=T archives with T's day.

        Args:
            instrument_public_id: Instrument to query.
            timeframe: Candle timeframe (e.g. ``1m``, ``1h``, ``1d``).
            day_start: First day (inclusive) of ``open_at`` range.
            day_end: Last day (inclusive) of ``open_at`` range.
            closed_only: If True, only closed versions
                (``known_to < now``).

        Returns:
            ``(id, public_id, timestamp, known_to, session_id,
            sequence_id, instrument_public_id, open_at, timeframe,
            open, high, low, close, volume, vwap, trades)`` tuples
            sorted by ``(open_at, timestamp)``.
        """
        from_dt = datetime.combine(day_start, datetime.min.time(), tzinfo=UTC)
        to_dt = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        conditions = [
            Candle.instrument_public_id == instrument_public_id,
            Candle.timeframe == timeframe,
            Candle.open_at >= from_dt,
            Candle.open_at < to_dt,
        ]
        if closed_only:
            conditions.append(Candle.known_to < datetime.now(UTC))
        cols = [
            Candle.id,
            Candle.public_id,
            Candle.timestamp,
            Candle.known_to,
            Candle.session_id,
            Candle.sequence_id,
            Candle.instrument_public_id,
            Candle.open_at,
            Candle.timeframe,
            Candle.open,
            Candle.high,
            Candle.low,
            Candle.close,
            Candle.volume,
            Candle.vwap,
            Candle.trades,
        ]
        with self.get_session() as session:
            rows = session.execute(
                select(*cols).where(*conditions).order_by(Candle.open_at, Candle.timestamp)
            ).all()
        return [tuple(r) for r in rows]

    def get_existing_archive_keys(
        self,
        model: type[Any],
        day_start: date,
        day_end: date,
    ) -> set[tuple[str, str, str]]:
        """Get existing (public_id, timestamp_iso, known_to_iso) for dedup.

        Returns a set of string triples for comparison against CSV row
        values, avoiding datetime precision mismatches.

        Args:
            model: SQLAlchemy model class.
            day_start: First day (inclusive) of ``timestamp`` range.
            day_end: Last day (inclusive) of ``timestamp`` range.

        Returns:
            Set of ``(public_id, timestamp_iso, known_to_iso)`` tuples.
        """
        from_dt = datetime.combine(day_start, datetime.min.time(), tzinfo=UTC)
        to_dt = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
        with self.get_session() as session:
            rows = session.execute(
                select(model.public_id, model.timestamp, model.known_to).where(
                    model.timestamp >= from_dt,
                    model.timestamp < to_dt,
                )
            ).all()
        return {(r[0], r[1].isoformat(), r[2].isoformat()) for r in rows}

    def bulk_insert_from_archive(
        self,
        model: type[Any],
        rows: list[dict[str, Any]],
    ) -> int:
        """Bulk insert archive rows into a table.

        Rows must contain all non-id columns with properly typed values.
        No dedup is performed — caller must filter duplicates before calling.

        Args:
            model: SQLAlchemy model class.
            rows: List of column dicts to insert.

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0
        with self.get_session() as session:
            session.execute(insert(model), rows)
            session.commit()
        return len(rows)

    def dispose(self) -> None:
        """Dispose the engine and release open database resources."""
        self.engine.dispose()

    def __del__(self) -> None:
        """Attempt to dispose the engine when the repository is garbage-collected."""
        engine = getattr(self, "engine", None)
        if engine is None:
            return
        try:
            engine.dispose()
        except Exception:
            return
