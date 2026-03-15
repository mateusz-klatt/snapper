"""Database repository implementations for async and sync access.

This module provides the Repository abstract base class and concrete
implementations for different database backends:

- **SQLAlchemyRepository**: Async repository for SQLite and PostgreSQL.
- **SQLiteRepository**: Convenience subclass for SQLite databases.
- **CloudRepository**: Convenience subclass for cloud databases.
- **MSSQLRepository**: Sync repository wrapped with asyncio.to_thread.
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

        rows = [{"instrument_id": 1, "timeframe": "1m", ...}]
        inserted = await repo.upsert_candles(rows)
"""

import asyncio
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from inspect import isawaitable
from typing import Any
from typing import cast
from urllib.parse import parse_qsl
from urllib.parse import urlencode
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
from snapper.data.models import Trade

_MSSQL_PREFIX = "mssql+pyodbc://"

__all__ = [
    "Repository",
    "SQLAlchemyRepository",
    "SQLiteRepository",
    "CloudRepository",
    "MSSQLRepository",
    "DatabaseRepository",
    "get_repository",
    "dispose_repositories",
    "clear_repository_cache",
]


_INSTRUMENT_COLUMNS = frozenset(c.key for c in Instrument.__table__.columns if c.key != "id")


def _filter_instrument_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Strip kwargs not in the Instrument model columns.

    Callers may pass tick_size / lot_size which were moved to
    InstrumentSpec; silently drop them so Instrument(**kwargs) works.

    Args:
        kwargs: Raw keyword arguments from callers.

    Returns:
        Filtered dict containing only valid Instrument column keys.
    """
    return {k: v for k, v in kwargs.items() if k in _INSTRUMENT_COLUMNS}


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
        """Return the database dialect name (sqlite, postgresql, mssql)."""
        ...

    @abstractmethod
    async def upsert_instrument(self, **kwargs: Any) -> int:
        """Insert or retrieve instrument by (symbol, exchange), returning its ID."""
        ...

    @abstractmethod
    async def get_latest_candle_ids(self) -> dict[tuple[int, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_id, timeframe).

        Used by the publisher to populate the in-memory candle ID cache on
        startup so that live upserts reuse existing public_ids for the
        current open_at window.

        Returns:
            Mapping of (instrument_id, timeframe) to (open_at, public_id).
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
    async def insert_order(
        self,
        instrument_id: int,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
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
        order_id: int,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
        executed_at: datetime | None = None,
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
        self, exchange: AllExchange, symbols: list[str], start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Retrieve market snapshots for symbols in time range."""
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

    def __del__(self) -> None:
        """Attempt to dispose the sync engine when the repository is garbage-collected."""
        engine = getattr(self, "engine", None)
        if engine is None:
            return
        try:
            engine.sync_engine.dispose()
        except Exception:
            return

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

    async def get_latest_candle_ids(self) -> dict[tuple[int, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_id, timeframe)."""
        async with self.session() as s:
            now = datetime.now(UTC)
            latest = (
                select(
                    Candle.instrument_id,
                    Candle.timeframe,
                    func.max(Candle.open_at).label("max_open_at"),
                )
                .where(Candle.timestamp <= now, Candle.known_to > now)
                .group_by(Candle.instrument_id, Candle.timeframe)
                .subquery()
            )
            q = await s.execute(
                select(Candle.instrument_id, Candle.timeframe, Candle.open_at, Candle.public_id)
                .where(Candle.timestamp <= now, Candle.known_to > now)
                .join(
                    latest,
                    and_(
                        Candle.instrument_id == latest.c.instrument_id,
                        Candle.timeframe == latest.c.timeframe,
                        Candle.open_at == latest.c.max_open_at,
                    ),
                )
            )
            return {
                (row.instrument_id, row.timeframe): (row.open_at, row.public_id) for row in q.all()
            }

    async def upsert_instrument(self, **kwargs: Any) -> int:
        """Insert or retrieve instrument by (symbol, exchange), returning its ID."""
        filtered = _filter_instrument_kwargs(kwargs)
        exchange = filtered["exchange"]
        async with self.session() as s:
            q = await s.execute(
                select(Instrument).where(
                    and_(Instrument.symbol == kwargs["symbol"], Instrument.exchange == exchange)
                )
            )
            inst = q.scalar_one_or_none()
            if inst is None:
                inst = Instrument(**filtered)
                s.add(inst)
                try:
                    await s.commit()
                except IntegrityError as exc:
                    await s.rollback()
                    q2 = await s.execute(
                        select(Instrument).where(
                            and_(
                                Instrument.symbol == kwargs["symbol"],
                                Instrument.exchange == exchange,
                            )
                        )
                    )
                    inst = q2.scalar_one_or_none()
                    if inst is None:
                        raise exc
                await s.refresh(inst)
                return int(inst.id)
            else:
                return int(inst.id)

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

        When a candle with the same (instrument_id, timeframe, open_at) already
        exists as an active row (known_to == KNOWN_TO_MAX), the old row is closed
        by setting its known_to to now, and a new row is inserted carrying the
        same public_id.  This preserves full history of intra-interval updates.

        Rows without ``public_id`` get a generated UUID7 automatically.
        """
        if not rows:
            return 0
        now = datetime.now(UTC)
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
            if "known_to" not in r:
                r["known_to"] = KNOWN_TO_MAX
        async with self.session() as s:
            count = 0
            for r in rows:
                existing = (
                    (
                        await s.execute(
                            select(Candle)
                            .where(
                                Candle.instrument_id == r["instrument_id"],
                                Candle.timeframe == r["timeframe"],
                                Candle.open_at == r["open_at"],
                                Candle.timestamp <= now,
                                Candle.known_to > now,
                            )
                            .with_for_update()
                        )
                    )
                    .scalars()
                    .first()
                )
                if existing:
                    await s.execute(
                        update(Candle).where(Candle.id == existing.id).values(known_to=now)
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

    async def insert_order(
        self,
        instrument_id: int,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
        time_in_force: str | None = None,
    ) -> tuple[int, str]:
        """Insert new order record and return (id, public_id) tuple."""
        async with self.session() as s:
            order = Order(
                instrument_id=instrument_id,
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
                instrument_id=old_order.instrument_id,
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
            )
            s.add(new_order)
            await s.commit()
            await s.refresh(new_order)
            return new_order.id

    async def insert_execution(
        self,
        order_id: int,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
        executed_at: datetime | None = None,
    ) -> int:
        """Insert execution record and return generated ID."""
        async with self.session() as s:
            execution = Execution(
                order_id=order_id,
                order_public_id=order_public_id,
                exec_id=exec_id,
                trade_id=trade_id,
                timestamp=timestamp,
                side=side,
                status=status,
                executed_at=executed_at,
                price=price,
                size=size,
                fee=fee,
                fee_asset=fee_asset,
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
            q_inst = await s.execute(
                select(Instrument).where(
                    and_(Instrument.symbol == instrument, Instrument.exchange == exchange)
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
                    and_(
                        Candle.instrument_id == inst.id,
                        Candle.timeframe == timeframe,
                        Candle.open_at >= start,
                        Candle.open_at <= end,
                        Candle.timestamp <= now,
                        Candle.known_to > now,
                    )
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
        async with self.session() as s:
            q_inst = await s.execute(
                select(Instrument).where(
                    and_(Instrument.symbol == instrument, Instrument.exchange == exchange)
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
                    and_(
                        Trade.instrument_id == inst.id,
                        Trade.timestamp >= start,
                        Trade.timestamp <= end,
                    )
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
        self, exchange: AllExchange, symbols: list[str], start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Retrieve market snapshots for exchange and symbols in time range."""
        async with self.session() as s:
            q = await s.execute(
                select(
                    MarketSnapshot.timestamp,
                    MarketSnapshot.symbol,
                    MarketSnapshot.exchange,
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
                    and_(
                        MarketSnapshot.exchange == exchange,
                        MarketSnapshot.symbol.in_(symbols),
                        MarketSnapshot.timestamp >= start,
                        MarketSnapshot.timestamp <= end,
                    )
                )
                .order_by(MarketSnapshot.timestamp.asc())
            )
            rows = q.all()
            return [
                {
                    "ts": r.timestamp,
                    "symbol": r.symbol,
                    "exchange": r.exchange,
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


_repository_cache: dict[str, Repository] = {}


def get_repository(db_url: str) -> Repository:
    """Get or create a cached repository instance.

    Returns an existing repository for the given URL or creates a new one.
    Uses MSSQLRepository for MSSQL URLs, SQLAlchemyRepository otherwise.

    Args:
        db_url: Database connection URL.

    Returns:
        Cached or newly created Repository instance.
    """
    if db_url not in _repository_cache:
        if db_url.startswith(_MSSQL_PREFIX):
            _repository_cache[db_url] = MSSQLRepository(db_url)
        else:
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


def clear_repository_cache() -> None:
    """Clear repository cache without disposing engines.

    Useful for testing when engines should not be disposed.
    """
    _repository_cache.clear()


class SQLiteRepository(SQLAlchemyRepository):
    """SQLite-specific repository.

    Convenience subclass that configures appropriate pooling for SQLite.
    """

    def __init__(self, path_url: str) -> None:
        """Initialize SQLite repository.

        Args:
            path_url: SQLite database URL (e.g., 'sqlite+aiosqlite:///./data/db.sqlite').
        """
        super().__init__(path_url)


class CloudRepository(SQLAlchemyRepository):
    """Cloud database repository (PostgreSQL, etc.).

    Convenience subclass for cloud-hosted databases.
    """

    def __init__(self, db_url: str) -> None:
        """Initialize cloud repository.

        Args:
            db_url: Async database URL for cloud database.
        """
        super().__init__(db_url)


class MSSQLRepository(Repository):
    """Microsoft SQL Server repository using sync driver with async wrapper.

    Uses pyodbc driver with synchronous SQLAlchemy engine, wrapping all
    operations in asyncio.to_thread() for async compatibility.

    Automatically configures ODBC driver settings for Azure SQL.

    Attributes:
        db_url: Processed connection URL with driver settings.
        engine: Sync SQLAlchemy engine.
        session_factory: Sync session factory.
    """

    def __init__(self, db_url: str) -> None:
        """Initialize MSSQL repository.

        Args:
            db_url: MSSQL connection URL (mssql+pyodbc://...).
        """
        self.db_url = self._ensure_driver(db_url)
        self.engine: SyncEngine = create_sync_engine(self.db_url, future=True)
        self.session_factory = sync_sessionmaker(
            self.engine, expire_on_commit=False, class_=SyncSession
        )

    @staticmethod
    def _ensure_driver(db_url: str) -> str:
        if not db_url.startswith(_MSSQL_PREFIX):
            return db_url
        if "?" not in db_url:
            return (
                db_url
                + "?"
                + urlencode(
                    {
                        "driver": "ODBC Driver 18 for SQL Server",
                        "Encrypt": "yes",
                        "TrustServerCertificate": "yes",
                    }
                )
            )
        base, qs = db_url.split("?", 1)
        params = dict(parse_qsl(qs))
        if "driver" not in params:
            params["driver"] = "ODBC Driver 18 for SQL Server"
        params.setdefault("Encrypt", "yes")
        params.setdefault("TrustServerCertificate", "yes")
        return base + "?" + urlencode(params)

    async def create_all(self) -> None:
        """Create all database tables synchronously via thread."""

        def _create() -> None:
            Base.metadata.create_all(self.engine)

        await asyncio.to_thread(_create)

    @property
    def dialect_name(self) -> str:
        """Return the database dialect name."""
        return self.engine.url.get_dialect().name

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Not implemented for MSSQL repository."""
        raise NotImplementedError("MSSQLRepository session() not implemented for server use")

    async def _run_sync[T](self, fn: Callable[[SyncSession], T]) -> T:
        """Execute a sync session callback in a background thread.

        Opens a sync session, passes it to ``fn``, and runs the whole
        closure via ``asyncio.to_thread`` so the event-loop stays free.

        Args:
            fn: Callable receiving a SyncSession and returning T.

        Returns:
            The value returned by *fn*.
        """

        def _work() -> T:
            with self.session_factory() as s:
                return fn(s)

        return await asyncio.to_thread(_work)

    @staticmethod
    def _sync_upsert_batch(s: SyncSession, model: type[Base], rows: list[dict[str, Any]]) -> int:
        """Insert rows one-by-one, skipping duplicates via SAVEPOINT.

        Each row is wrapped in a nested transaction (SAVEPOINT) so that
        an IntegrityError only rolls back the failing row, preserving
        previously inserted rows and keeping the counter accurate.

        Args:
            s: Active sync session.
            model: SQLAlchemy model class to insert into.
            rows: List of column-value dicts.

        Returns:
            Number of rows successfully inserted.
        """
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
        inserted = 0
        for r in rows:
            try:
                with s.begin_nested():
                    s.execute(insert(model).values(**r))
                inserted += 1
            except IntegrityError:
                continue
        s.commit()
        return inserted

    async def get_latest_candle_ids(self) -> dict[tuple[int, str], tuple[datetime, str]]:
        """Load the latest candle public_id per (instrument_id, timeframe) via sync thread."""

        def _do(s: SyncSession) -> dict[tuple[int, str], tuple[datetime, str]]:
            now = datetime.now(UTC)
            latest = (
                select(
                    Candle.instrument_id,
                    Candle.timeframe,
                    func.max(Candle.open_at).label("max_open_at"),
                )
                .where(Candle.timestamp <= now, Candle.known_to > now)
                .group_by(Candle.instrument_id, Candle.timeframe)
                .subquery()
            )
            q = s.execute(
                select(Candle.instrument_id, Candle.timeframe, Candle.open_at, Candle.public_id)
                .where(Candle.timestamp <= now, Candle.known_to > now)
                .join(
                    latest,
                    and_(
                        Candle.instrument_id == latest.c.instrument_id,
                        Candle.timeframe == latest.c.timeframe,
                        Candle.open_at == latest.c.max_open_at,
                    ),
                )
            )
            return {
                (row.instrument_id, row.timeframe): (row.open_at, row.public_id) for row in q.all()
            }

        return await self._run_sync(_do)

    async def upsert_instrument(self, **kwargs: Any) -> int:
        """Insert or retrieve instrument by (symbol, exchange) via sync thread."""
        filtered = _filter_instrument_kwargs(kwargs)
        exchange = filtered["exchange"]

        def _do(s: SyncSession) -> int:
            q = s.execute(
                select(Instrument).where(
                    and_(Instrument.symbol == kwargs["symbol"], Instrument.exchange == exchange)
                )
            )
            inst = q.scalar_one_or_none()
            if inst is None:
                inst = Instrument(**filtered)
                s.add(inst)
                try:
                    s.commit()
                except IntegrityError as exc:
                    s.rollback()
                    q2 = s.execute(
                        select(Instrument).where(
                            and_(
                                Instrument.symbol == kwargs["symbol"],
                                Instrument.exchange == exchange,
                            )
                        )
                    )
                    inst = q2.scalar_one_or_none()
                    if inst is None:
                        raise exc
                s.refresh(inst)
                return int(inst.id)
            else:
                return int(inst.id)

        return await self._run_sync(_do)

    async def upsert_candles(self, rows: list[dict[str, Any]]) -> int:
        """Close-old + insert-new (SCD Type 2) for candle rows via sync thread."""
        if not rows:
            return 0
        now = datetime.now(UTC)
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
            if "known_to" not in r:
                r["known_to"] = KNOWN_TO_MAX

        def _do(s: SyncSession) -> int:
            count = 0
            for r in rows:
                existing = (
                    s.execute(
                        select(Candle).where(
                            Candle.instrument_id == r["instrument_id"],
                            Candle.timeframe == r["timeframe"],
                            Candle.open_at == r["open_at"],
                            Candle.timestamp <= now,
                            Candle.known_to > now,
                        )
                    )
                    .scalars()
                    .first()
                )
                if existing:
                    s.execute(update(Candle).where(Candle.id == existing.id).values(known_to=now))
                    r["public_id"] = existing.public_id
                s.add(Candle(**r))
                count += 1
            s.commit()
            return count

        return await self._run_sync(_do)

    async def upsert_trades(self, rows: list[dict[str, Any]]) -> int:
        """Insert trades via sync thread, skipping duplicates."""
        if not rows:
            return 0
        return await self._run_sync(lambda s: self._sync_upsert_batch(s, Trade, rows))

    async def insert_order(
        self,
        instrument_id: int,
        client_order_id: str | None,
        exchange_order_id: str | None,
        created_at: datetime,
        side: str,
        order_type: str,
        price: float | None,
        size: float,
        status: str,
        time_in_force: str | None = None,
    ) -> tuple[int, str]:
        """Insert order record via sync thread."""

        def _do(s: SyncSession) -> tuple[int, str]:
            order = Order(
                instrument_id=instrument_id,
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
            )
            s.add(order)
            s.commit()
            s.refresh(order)
            return (order.id, order.public_id)

        return await self._run_sync(_do)

    async def update_order(
        self,
        order_id: int,
        status: str,
        updated_at: datetime,
        exchange_order_id: str | None = None,
        error: str | None = None,
        filled_size: float | None = None,
        average_price: float | None = None,
    ) -> int:
        """Close old order version and insert new one (SCD Type 2) via sync thread."""

        def _do(s: SyncSession) -> int:
            now = datetime.now(UTC)
            old_order = s.execute(select(Order).where(Order.id == order_id)).scalars().one()
            s.execute(update(Order).where(Order.id == order_id).values(known_to=now))
            new_order = Order(
                public_id=old_order.public_id,
                instrument_id=old_order.instrument_id,
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
            )
            s.add(new_order)
            s.commit()
            s.refresh(new_order)
            return new_order.id

        return await self._run_sync(_do)

    async def insert_execution(
        self,
        order_id: int,
        order_public_id: str,
        timestamp: datetime,
        side: str,
        status: str,
        price: float,
        size: float,
        fee: float,
        fee_asset: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
        executed_at: datetime | None = None,
    ) -> int:
        """Insert execution record via sync thread."""

        def _do(s: SyncSession) -> int:
            execution = Execution(
                order_id=order_id,
                order_public_id=order_public_id,
                exec_id=exec_id,
                trade_id=trade_id,
                timestamp=timestamp,
                side=side,
                status=status,
                executed_at=executed_at,
                price=price,
                size=size,
                fee=fee,
                fee_asset=fee_asset,
            )
            s.add(execution)
            s.commit()
            s.refresh(execution)
            return execution.id

        return await self._run_sync(_do)

    async def get_candles(
        self,
        instrument: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
    ) -> list[dict[str, Any]]:
        """Retrieve active candles via sync thread."""

        def _do(s: SyncSession) -> list[dict[str, Any]]:
            now = datetime.now(UTC)
            q_inst = s.execute(
                select(Instrument).where(
                    and_(Instrument.symbol == instrument, Instrument.exchange == exchange)
                )
            )
            inst = q_inst.scalars().first()
            if inst is None:
                return []
            q = s.execute(
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
                    and_(
                        Candle.instrument_id == inst.id,
                        Candle.timeframe == timeframe,
                        Candle.open_at >= start,
                        Candle.open_at <= end,
                        Candle.timestamp <= now,
                        Candle.known_to > now,
                    )
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

        return await self._run_sync(_do)

    async def get_trades(
        self, instrument: str, start: datetime, end: datetime, exchange: AllExchange
    ) -> list[dict[str, Any]]:
        """Retrieve trades via sync thread."""

        def _do(s: SyncSession) -> list[dict[str, Any]]:
            q_inst = s.execute(
                select(Instrument).where(
                    and_(Instrument.symbol == instrument, Instrument.exchange == exchange)
                )
            )
            inst = q_inst.scalars().first()
            if inst is None:
                return []
            q = s.execute(
                select(
                    Trade.timestamp,
                    Trade.price,
                    Trade.size,
                    Trade.side,
                    Trade.trade_id,
                )
                .where(
                    and_(
                        Trade.instrument_id == inst.id,
                        Trade.timestamp >= start,
                        Trade.timestamp <= end,
                    )
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

        return await self._run_sync(_do)

    async def get_market_snapshots(
        self, exchange: AllExchange, symbols: list[str], start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Retrieve market snapshots via sync thread."""

        def _do(s: SyncSession) -> list[dict[str, Any]]:
            q = s.execute(
                select(
                    MarketSnapshot.timestamp,
                    MarketSnapshot.symbol,
                    MarketSnapshot.exchange,
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
                    and_(
                        MarketSnapshot.exchange == exchange,
                        MarketSnapshot.symbol.in_(symbols),
                        MarketSnapshot.timestamp >= start,
                        MarketSnapshot.timestamp <= end,
                    )
                )
                .order_by(MarketSnapshot.timestamp.asc())
            )
            rows = q.all()
            return [
                {
                    "ts": r.timestamp,
                    "symbol": r.symbol,
                    "exchange": r.exchange,
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

        return await self._run_sync(_do)


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
        if db_url.startswith("sqlite+aiosqlite://"):
            return db_url.replace("sqlite+aiosqlite://", "sqlite://")
        if db_url.startswith("postgresql+asyncpg://"):
            return db_url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
        if db_url.startswith(_MSSQL_PREFIX):
            return db_url
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
