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
from datetime import UTC
from datetime import datetime
from inspect import isawaitable
from typing import Any
from typing import cast
from uuid import uuid7

from loguru import logger
from sqlalchemy import and_
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import event
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
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
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Order
from snapper.data.models import Symbol
from snapper.data.models import Tick
from snapper.data.models import Trade

__all__ = [
    "Repository",
    "SQLAlchemyRepository",
    "DatabaseRepository",
    "close_and_insert",
    "close_and_insert_sync",
    "get_repository",
    "dispose_repositories",
    "where_active",
]


def where_active(model: type[Any], at: datetime | None = None) -> tuple[Any, Any]:
    """Return temporal filter clauses for active records.

    Args:
        model: SQLAlchemy model class with timestamp and known_to columns.
        at: Point-in-time to query. Defaults to now.

    Returns:
        Tuple of two filter clauses: (timestamp <= t, known_to > t).
    """
    t = at or datetime.now(UTC)
    return model.timestamp <= t, model.known_to > t


_INSTRUMENT_COLUMNS = frozenset(c.key for c in Instrument.__table__.columns if c.key != "id")


def _filter_instrument_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Strip kwargs not in the Instrument model columns.

    Callers may pass tick_size / lot_size which were moved to
    InstrumentSpec; silently drop them so Instrument(**kwargs) works.
    Supplies a default ``timestamp`` of now(UTC) when not provided,
    since the TemporalMixin makes timestamp NOT NULL.

    Args:
        kwargs: Raw keyword arguments from callers.

    Returns:
        Filtered dict containing only valid Instrument column keys.
    """
    filtered = {k: v for k, v in kwargs.items() if k in _INSTRUMENT_COLUMNS}
    if "timestamp" not in filtered:
        filtered["timestamp"] = datetime.now(UTC)
    return filtered


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
    async def upsert_instrument(self, **kwargs: Any) -> tuple[int, str]:
        """Find or create instrument by (symbol_public_id, exchange).

        Instrument natural key is immutable, so no SCD2 versioning is
        needed.  Returns (id, public_id) of active row, creating one
        if none exists.
        """
        ...

    @abstractmethod
    async def get_latest_candle_ids(self) -> dict[tuple[str, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_public_id, timeframe).

        Used by the publisher to populate the in-memory candle ID cache on
        startup so that live upserts reuse existing public_ids for the
        current open_at window.

        Returns:
            Mapping of (instrument_public_id, timeframe) to (open_at, public_id).
        """
        ...

    @abstractmethod
    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        """Insert or update candles. Return affected row count."""
        ...

    @abstractmethod
    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        """Insert trades, skipping duplicates. Return inserted count."""
        ...

    @abstractmethod
    async def upsert_ticks(self, rows: list[dict[str, Any]]) -> int:
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
        start: datetime,
        end: datetime,
        exchange: AllExchange,
    ) -> list[dict[str, Any]]:
        """Retrieve candles for instrument in time range."""
        ...

    @abstractmethod
    async def get_trades(
        self, instrument: str, start: datetime, end: datetime, exchange: AllExchange
    ) -> list[dict[str, Any]]:
        """Retrieve trades for instrument in time range."""
        ...

    @abstractmethod
    async def get_market_snapshots(
        self, instrument_public_ids: list[str], start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Retrieve active market snapshots for instruments in time range."""
        ...

    @abstractmethod
    async def upsert_market_snapshots(self, rows: list[dict[str, Any]]) -> int:
        """SCD2 close+insert for market snapshots.

        Each row must contain instrument_public_id plus market data fields.
        Closes the active snapshot for the same instrument and inserts a
        new version, preserving public_id across updates.
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

    async def get_latest_candle_ids(self) -> dict[tuple[str, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_public_id, timeframe)."""
        async with self.session() as s:
            now = datetime.now(UTC)
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

    async def upsert_instrument(self, **kwargs: Any) -> tuple[int, str]:
        """Find or create instrument by (symbol_public_id, exchange).

        Instrument natural key is immutable (symbol_public_id + exchange),
        so no SCD2 versioning is needed.  Returns (id, public_id) of the
        active row, creating a new one if none exists.
        """
        filtered = _filter_instrument_kwargs(kwargs)
        symbol_public_id = filtered["symbol_public_id"]
        exchange = filtered["exchange"]
        bus_time = filtered.get("timestamp") or datetime.now(UTC)
        filtered["timestamp"] = bus_time
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
            new_inst = Instrument(**filtered)
            s.add(new_inst)
            try:
                await s.commit()
            except IntegrityError as exc:
                await s.rollback()
                retry_ts, retry_kt = where_active(Instrument)
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

    async def _upsert_batch(
        self, model: type[Base], rows: list[dict[str, Any]], index_elements: list[str]
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

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
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
            if "timestamp" not in r:
                r["timestamp"] = datetime.now(UTC)
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

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        """Insert trades with dialect-specific conflict handling."""
        if not rows:
            return 0
        return await self._upsert_batch(Trade, rows, ["trade_id"])

    async def upsert_ticks(self, rows: list[dict[str, Any]]) -> int:
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
                timestamp=datetime.now(UTC),
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
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int:
        """Close old order version and insert new one (SCD Type 2)."""
        async with self.session() as s:
            now = datetime.now(UTC)
            old_order = (
                (await s.execute(select(Order).where(Order.id == order_id).with_for_update()))
                .scalars()
                .one()
            )
            await s.execute(update(Order).where(Order.id == order_id).values(known_to=now))
            new_order = Order(
                public_id=old_order.public_id,
                instrument_public_id=old_order.instrument_public_id,
                client_order_id=old_order.client_order_id,
                exchange_order_id=exchange_order_id or old_order.exchange_order_id,
                created_at=old_order.created_at,
                updated_at=updated_at,
                timestamp=now,
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

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
    ) -> list[dict[str, Any]]:
        """Retrieve active candles for instrument within time range."""
        now = datetime.now(UTC)
        async with self.session() as s:
            s_ts, s_kt = where_active(Symbol, now)
            sym_q = await s.execute(
                select(Symbol.public_id).where(
                    Symbol.native_symbol == instrument,
                    s_ts,
                    s_kt,
                )
            )
            symbol_pid = sym_q.scalar_one_or_none()
            if symbol_pid is None:
                return []
            i_ts, i_kt = where_active(Instrument, now)
            q_inst = await s.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == symbol_pid,
                    Instrument.exchange == exchange,
                    i_ts,
                    i_kt,
                )
            )
            inst = q_inst.scalars().first()
            if inst is None:
                return []
            q = await s.execute(
                select(
                    Candle.open_at,
                    Candle.timeframe,
                    Candle.open,
                    Candle.high,
                    Candle.low,
                    Candle.close,
                    Candle.volume,
                    Candle.vwap,
                    Candle.trades,
                )
                .where(
                    Candle.instrument_public_id == inst.public_id,
                    Candle.timeframe == timeframe,
                    Candle.open_at >= start,
                    Candle.open_at <= end,
                    Candle.timestamp <= now,
                    Candle.known_to > now,
                )
                .order_by(Candle.open_at.asc())
            )
            rows = q.all()
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
                }
                for r in rows
            ]

    async def get_trades(
        self, instrument: str, start: datetime, end: datetime, exchange: AllExchange
    ) -> list[dict[str, Any]]:
        """Retrieve trades for instrument within time range."""
        now = datetime.now(UTC)
        async with self.session() as s:
            s_ts, s_kt = where_active(Symbol, now)
            sym_q = await s.execute(
                select(Symbol.public_id).where(
                    Symbol.native_symbol == instrument,
                    s_ts,
                    s_kt,
                )
            )
            symbol_pid = sym_q.scalar_one_or_none()
            if symbol_pid is None:
                return []
            i_ts, i_kt = where_active(Instrument, now)
            q_inst = await s.execute(
                select(Instrument).where(
                    Instrument.symbol_public_id == symbol_pid,
                    Instrument.exchange == exchange,
                    i_ts,
                    i_kt,
                )
            )
            inst = q_inst.scalars().first()
            if inst is None:
                return []
            q = await s.execute(
                select(
                    Trade.timestamp,
                    Trade.price,
                    Trade.size,
                    Trade.side,
                    Trade.trade_id,
                )
                .where(
                    Trade.instrument_public_id == inst.public_id,
                    Trade.timestamp >= start,
                    Trade.timestamp <= end,
                    Trade.known_to > now,
                )
                .order_by(Trade.timestamp.asc())
            )
            rows = q.all()
            return [
                {
                    "timestamp": r.timestamp,
                    "price": r.price,
                    "size": r.size,
                    "side": r.side,
                    "trade_id": r.trade_id,
                }
                for r in rows
            ]

    async def get_market_snapshots(
        self, instrument_public_ids: list[str], start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Retrieve active market snapshots for instruments in time range."""
        now = datetime.now(UTC)
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

    async def upsert_market_snapshots(self, rows: list[dict[str, Any]]) -> int:
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
            if "timestamp" not in r:
                r["timestamp"] = datetime.now(UTC)
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
