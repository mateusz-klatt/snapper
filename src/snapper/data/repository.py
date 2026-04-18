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

import weakref
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
from sqlalchemy import text
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

from snapper.core.json_types import JsonObject
from snapper.core.partitioning import ShardOwnership
from snapper.core.partitioning import ShardOwnershipError
from snapper.core.types import AllExchange
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.archive_symbols import resolve_archive_symbols
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import ExecutionPlan
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.models import ExecutionPlanDecision
from snapper.data.models import FundingRate
from snapper.data.models import Instrument
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import MarketSnapshot
from snapper.data.models import Operator
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import PositionCycle
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import Tick
from snapper.data.models import Trade
from snapper.data.models import TradeCommand
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import UnderlyingAsset
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.models import VenueEvent
from snapper.data.models import VenueFeeSchedule
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository_types import AccrualLedgerInsertRow
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CheckpointUpsertRow
from snapper.data.repository_types import CreateScopeGrantRequest
from snapper.data.repository_types import ExecutionPlanCheckpointRow
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanDecisionRow
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import FundingRateInsertRow
from snapper.data.repository_types import FundingRateRow
from snapper.data.repository_types import InstrumentContractRow
from snapper.data.repository_types import InstrumentFrontMonthRow
from snapper.data.repository_types import InstrumentOrderCapabilityRow
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import InstrumentUnderlyingRow
from snapper.data.repository_types import MarketSnapshotRow
from snapper.data.repository_types import MarketSnapshotUpsertRow
from snapper.data.repository_types import OperatorRow
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import PositionCycleInsertRow
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import PositionRow
from snapper.data.repository_types import ScopeGrantRow
from snapper.data.repository_types import SettingRow
from snapper.data.repository_types import SignalRow
from snapper.data.repository_types import TickRow
from snapper.data.repository_types import TickUpsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import TradeRow
from snapper.data.repository_types import TradeUpsertRow
from snapper.data.repository_types import UnderlyingAssetRow
from snapper.data.repository_types import UserOperatorMembershipRow
from snapper.data.repository_types import UserRecentSubmitRow
from snapper.data.repository_types import UserTradingCapsRow
from snapper.data.repository_types import VenueEventInsertRow
from snapper.data.repository_types import VenueEventRow
from snapper.data.repository_types import VenueFeeScheduleRow
from snapper.data.repository_types import WalletCredentialRow
from snapper.data.repository_types import WalletRow

__all__ = [
    "Repository",
    "SQLAlchemyRepository",
    "DatabaseRepository",
    "InstrumentSpecInput",
    "ScopeGrantConflictError",
    "ScopeGrantNotFoundError",
    "ScopeGrantValidationError",
    "WalletConflictError",
    "CredentialConflictError",
    "CredentialNotFoundError",
    "close_and_insert",
    "close_and_insert_sync",
    "get_repository",
    "dispose_repositories",
    "where_active",
    "where_active_now",
]


class ScopeGrantConflictError(Exception):
    """Raised when a wallet_operator_scope_grants insert overlaps an active grant.

    Maps to HTTP 409 at the API layer. Instrument-exclusive: at most one
    operator may hold an active grant covering a given instrument
    on a given wallet at any time.
    """

    def __init__(
        self,
        wallet_public_id: str,
        conflicting_grant_public_id: str,
        conflicting_operator_public_id: str,
        reason: str,
    ) -> None:
        """Capture the conflicting grant identity for the API layer."""
        super().__init__(
            f"Scope grant on wallet={wallet_public_id} conflicts with grant "
            f"{conflicting_grant_public_id} held by operator "
            f"{conflicting_operator_public_id}: {reason}"
        )
        self.wallet_public_id = wallet_public_id
        self.conflicting_grant_public_id = conflicting_grant_public_id
        self.conflicting_operator_public_id = conflicting_operator_public_id
        self.reason = reason


class ScopeGrantNotFoundError(Exception):
    """Raised when a referenced scope grant or operator does not exist.

    Maps to HTTP 404 at the API layer. Used by ``handover_grant`` for the
    source-grant existence check (rule 1) and the destination-operator
    existence check (rule 4).
    """


class ScopeGrantValidationError(Exception):
    """Raised when a scope grant request violates a structural invariant.

    Maps to HTTP 400 at the API layer. Examples: scope_kind/public_id
    XOR violation, self-handover (source operator equals target
    operator), or unknown scope_kind.
    """


class CredentialConflictError(Exception):
    """Raised when a credential insert collides with an existing active row.

    Maps to HTTP 409. The active-unique index on
    ``(wallet_public_id, exchange)`` enforces one active credential per
    exchange per wallet.
    """

    def __init__(self, wallet_public_id: str, exchange: str, reason: str) -> None:
        """Capture the conflicting key for the caller."""
        super().__init__(
            f"Credential insert failed for wallet={wallet_public_id} "
            f"exchange={exchange}: {reason}"
        )
        self.wallet_public_id = wallet_public_id
        self.exchange = exchange
        self.reason = reason


class CredentialNotFoundError(Exception):
    """Raised when a referenced credential does not exist or is not active.

    Maps to HTTP 404. Used by ``rotate_wallet_credential`` when the
    source credential row is missing or already closed.
    """


class WalletConflictError(Exception):
    """Raised when a wallet insert collides with an existing active row.

    Maps to HTTP 409 at the API layer. The active-unique index on
    ``(label, is_paper)`` enforces that two wallets sharing both
    columns cannot be active at the same bus time.
    """

    def __init__(self, label: str, is_paper: bool, reason: str) -> None:
        """Capture the conflicting (label, is_paper) tuple for the caller."""
        super().__init__(f"Wallet insert failed for label={label!r} is_paper={is_paper}: {reason}")
        self.label = label
        self.is_paper = is_paper
        self.reason = reason


_TRADE_COMMAND_TERMINAL_STATUSES: tuple[str, ...] = (
    TradeCommandStatusEnum.FILLED,
    TradeCommandStatusEnum.CANCELLED,
    TradeCommandStatusEnum.EXPIRED,
    TradeCommandStatusEnum.REJECTED,
    TradeCommandStatusEnum.FAILED,
)


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
            select(model)
            .where(
                model.timestamp <= bus_time,
                model.known_to > bus_time,
                *match_filters,
            )
            .with_for_update()
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
    """Typed payload for instrument trading specification revisions.

    The funding fields (``funding_type``, ``funding_frequency_hours``,
    ``rollover_rate_long``, ``rollover_rate_short``, ``max_funding_rate``)
    are populated by the per-exchange symbol updaters and consumed by
    the funding accrual coroutine. Spot exchanges without margin
    (Zonda, Walutomat) leave them ``None``.
    """

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
    expiry_at: datetime | None = None
    instrument_kind: str | None = None
    funding_type: str | None = None
    funding_frequency_hours: int | None = None
    rollover_rate_long: float | None = None
    rollover_rate_short: float | None = None
    max_funding_rate: float | None = None


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
    async def get_instrument_spec(
        self,
        instrument_public_id: str,
        as_of: datetime,
    ) -> InstrumentSpecRow | None:
        """Return the active InstrumentSpec for an instrument, or None.

        Args:
            instrument_public_id: Public ID of the instrument.
            as_of: Point-in-time for temporal query.

        Returns:
            InstrumentSpecRow dict or None if no spec exists.
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
        wallet_public_id: str,
        operator_public_id: str | None = None,
        time_in_force: str | None = None,
        mode: str = "live",
        leverage: int | None = None,
        reduce_only: bool = False,
    ) -> tuple[int, str]:
        """Insert new order record, returning (id, public_id) tuple.

        ``wallet_public_id`` is mandatory; the schema
        enforces ``NOT NULL`` and the routing layer relies on it to
        dispatch fills back to the owning per-wallet engine.
        ``operator_public_id`` is nullable for strategy-emitted orders.
        """
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

        Returns the new version's integer id. Wallet and operator
        attribution is copied from the closed row so NOT NULL
        tightening holds across SCD2 versions.
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
        wallet_public_id: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
        operator_public_id: str | None = None,
        liquidity_role: str = "unknown",
    ) -> int:
        """Insert execution record, returning execution ID.

        ``wallet_public_id`` is mandatory so recovery can
        group fills into the correct per-wallet engine.
        ``operator_public_id`` is nullable so strategy-
        emitted fills without a human operator still persist.
        ``liquidity_role`` indicates maker/taker/unknown.
        """
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
        wallet_public_ids: list[str] | None = None,
    ) -> list[SignalRow]:
        """Retrieve signals with optional filters.

        Args:
            since: Start of time window for fired_at.
            limit: Maximum number of signals to return.
            as_of: Point-in-time for temporal query.
            instrument: Optional native symbol filter.
            strategy: Optional strategy name filter.
            exchange: Optional exchange filter.
            wallet_public_ids: Optional wallet scope filter.

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
        wallet_public_ids: list[str] | None = None,
    ) -> list[OrderRow]:
        """Retrieve orders with optional filters and pagination.

        Args:
            limit: Maximum number of orders to return.
            offset: Number of orders to skip.
            as_of: Point-in-time for temporal query.
            symbol: Optional native symbol filter.
            exchange: Optional exchange filter.
            wallet_public_ids: Optional wallet scope filter for Phase 0d
                multi-tenant scoping. When ``None``, no wallet filter is
                applied (ADMIN sees all). When a non-empty list, only
                orders on the listed wallets are returned.

        Returns:
            Order dicts ordered by created_at DESC, denormalized with
            instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_executions(
        self,
        limit: int,
        as_of: datetime,
        wallet_public_ids: list[str] | None = None,
    ) -> list[ExecutionRow]:
        """Retrieve executions with order/instrument/symbol info.

        Args:
            limit: Maximum number of executions to return.
            as_of: Point-in-time for temporal query.
            wallet_public_ids: Optional wallet scope filter.

        Returns:
            Execution dicts ordered by timestamp DESC, denormalized with
            order, instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_active_orders_for_recovery(
        self,
        exchange: str,
        as_of: datetime,
        wallet_public_id: str = "",
    ) -> list[OrderRow]:
        """Retrieve non-terminal orders for startup recovery.

        Returns orders with active status (open, pending, pending_new,
        new, partially_filled) for a given exchange. Used exclusively
        by executor and trader recovery, not by API endpoints.

        Args:
            exchange: Exchange name to filter by.
            as_of: Point-in-time for temporal query.
            wallet_public_id: Multi-tenant filter. When
                non-empty, only orders matching ``Order.wallet_public_id
                == wallet_public_id`` are returned so each per-wallet
                executor instance recovers exclusively its own orders
                and never spills cross-wallet state into
                ``pending_orders``. The legacy default ``""`` skips the
                filter for backwards compatibility with the single-
                wallet template path.

        Returns:
            Active order dicts ordered by created_at ASC (chronological
            for replay), denormalized with instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_executions_for_recovery(
        self,
        as_of: datetime,
        exchange: str | None = None,
        instrument: str | None = None,
        wallet_public_id: str = "",
    ) -> list[ExecutionRow]:
        """Retrieve all executions for startup state reconstruction.

        Unlike get_executions(), this method has no limit and returns
        results in chronological order (ASC) for correct replay.
        Optional exchange/instrument filters narrow the scope.

        Args:
            as_of: Point-in-time for temporal query.
            exchange: Optional exchange filter.
            instrument: Optional native symbol filter.
            wallet_public_id: Multi-tenant filter. When
                non-empty, only executions whose
                ``Execution.wallet_public_id`` matches are returned.
                The legacy default ``""`` skips the filter for
                backwards compatibility with the single-wallet
                template path.

        Returns:
            Execution dicts ordered by timestamp ASC for replay,
            denormalized with order, instrument and symbol info.
        """
        ...

    @abstractmethod
    async def get_positions(
        self,
        as_of: datetime,
        wallet_public_ids: list[str] | None = None,
    ) -> list[PositionRow]:
        """Retrieve active positions with instrument/symbol info.

        Args:
            as_of: Point-in-time for temporal query.
            wallet_public_ids: Optional wallet scope filter.

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

    @abstractmethod
    async def get_underlying_assets(
        self,
        as_of: datetime,
    ) -> list[UnderlyingAssetRow]:
        """All active underlying assets at as_of.

        Args:
            as_of: Point-in-time for temporal query.

        Returns:
            List of underlying asset dicts.
        """
        ...

    @abstractmethod
    async def get_underlying_by_ticker(
        self,
        ticker: str,
        as_of: datetime,
    ) -> UnderlyingAssetRow | None:
        """Lookup underlying by ticker (e.g. 'SPX', 'GOLD').

        Args:
            ticker: Short code for the underlying asset.
            as_of: Point-in-time for temporal query.

        Returns:
            Underlying asset dict or None if not found.
        """
        ...

    @abstractmethod
    async def get_instruments_by_underlying(
        self,
        underlying_public_id: str,
        as_of: datetime,
        relationship_types: list[str] | None = None,
    ) -> list[InstrumentUnderlyingRow]:
        """Instruments mapped to an underlying asset.

        Args:
            underlying_public_id: Public ID of the underlying asset.
            as_of: Point-in-time for temporal query.
            relationship_types: Optional filter (e.g. ['derivative']).

        Returns:
            List of instrument-underlying mapping dicts with symbol info.
        """
        ...

    @abstractmethod
    async def get_underlying_for_instrument(
        self,
        instrument_public_id: str,
        as_of: datetime,
    ) -> UnderlyingAssetRow | None:
        """Reverse lookup: which underlying does this instrument belong to?

        Args:
            instrument_public_id: Public ID of the instrument.
            as_of: Point-in-time for temporal query.

        Returns:
            Underlying asset dict or None if unmapped.
        """
        ...

    @abstractmethod
    async def upsert_underlying_asset(
        self,
        ticker: str,
        name: str,
        asset_class: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        sector: str | None = None,
        description: str | None = None,
    ) -> tuple[str, str]:
        """SCD2 upsert for an underlying asset.

        Args:
            ticker: Short code (e.g. 'SPX').
            name: Canonical name (e.g. 'S&P 500').
            asset_class: Asset type from AssetTypeEnum.
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time.
            sector: Optional sector (e.g. 'Precious Metals').
            description: Optional human-readable description.

        Returns:
            Tuple of (underlying_public_id, status) where status is
            'created', 'updated', or 'unchanged'.
        """
        ...

    @abstractmethod
    async def upsert_instrument_underlying_mapping(
        self,
        instrument_public_id: str,
        underlying_public_id: str,
        relationship_type: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        contract_family: str | None = None,
    ) -> str:
        """SCD2 upsert for an instrument-underlying mapping.

        Compares (underlying_public_id, relationship_type, contract_family).
        'updated' when any of the three fields changed (close+insert).

        Args:
            instrument_public_id: Public ID of the instrument.
            underlying_public_id: Public ID of the underlying asset.
            relationship_type: One of 'exact', 'derivative', 'proxy'.
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time.
            contract_family: Optional futures product root (e.g. 'ES').

        Returns:
            Status: 'created', 'updated', or 'unchanged'.
        """
        ...

    @abstractmethod
    async def close_instrument_underlying_mapping(
        self,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> bool:
        """Close active mapping for instrument.

        Args:
            instrument_public_id: Public ID of the instrument.
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time.

        Returns:
            True if a row was closed, False if no active mapping.
        """
        ...

    @abstractmethod
    async def close_underlying_asset(
        self,
        public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> bool:
        """Close active underlying asset by public_id.

        Args:
            public_id: Public ID of the underlying to close.
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time.

        Returns:
            True if a row was closed, False if no active row.
        """
        ...

    @abstractmethod
    async def get_front_month_instrument(
        self,
        underlying_public_id: str,
        as_of: datetime,
        exchange: str | None = None,
        contract_family: str | None = None,
    ) -> InstrumentFrontMonthRow | None:
        """Return the nearest non-expired futures contract for an underlying.

        Joins InstrumentUnderlyingMapping -> Instrument -> InstrumentSpec.
        Filters: relationship_type='derivative', instrument_kind='future',
        expiry_at > as_of. Ordered by expiry_at ASC, contract_family ASC.

        Args:
            underlying_public_id: Public ID of the underlying asset.
            as_of: Point-in-time for temporal + expiry filtering.
            exchange: Optional exchange filter.
            contract_family: Optional product root filter (e.g., 'ES' vs 'MES').

        Returns:
            Front-month row or None if no active futures found.
        """
        ...

    @abstractmethod
    async def get_contracts_for_underlying(
        self,
        underlying_public_id: str,
        as_of: datetime,
        exchange: str | None = None,
        contract_family: str | None = None,
        include_expired: bool = False,
    ) -> list[InstrumentContractRow]:
        """Return all futures contracts for an underlying.

        Each row includes is_front_month (True for nearest non-expired
        within same contract_family). Sorted by (contract_family, expiry_at).

        Args:
            underlying_public_id: Public ID of the underlying asset.
            as_of: Point-in-time for temporal filtering.
            exchange: Optional exchange filter.
            contract_family: Optional product root filter.
            include_expired: If True, include contracts with expiry_at <= as_of.

        Returns:
            List of contract rows.
        """
        ...

    @abstractmethod
    async def insert_funding_rate(
        self,
        row: FundingRateInsertRow,
        session: AsyncSession | None = None,
    ) -> int:
        """Insert a funding rate row, returning its integer id.

        Idempotent on the partial unique index over
        ``(instrument_public_id, exchange, rate_type, direction,
        effective_from)``: a duplicate raises ``IntegrityError`` which
        the caller is expected to swallow when re-running backfills.

        Args:
            row: Insert payload with all required provenance fields.
            session: Optional caller-managed session. When provided, the
                method does not commit so the caller can group the
                insert into a larger transaction.

        Returns:
            Integer ``id`` of the new row.
        """
        ...

    @abstractmethod
    async def get_funding_rates(
        self,
        instrument_public_id: str,
        exchange: str,
        rate_type: str,
        direction: str,
        as_of: datetime,
        range_start: datetime | None = None,
        range_end: datetime | None = None,
    ) -> list[FundingRateRow]:
        """Return funding rates for an instrument and direction.

        Bitemporal query: ``as_of`` filters the active SCD2 versions,
        and ``range_start`` / ``range_end`` constrain the
        ``effective_from`` exchange-side timestamp. Range bounds follow
        the project temporal-query convention from
        ``feedback_temporal_query`` (always pass an explicit window
        instead of relying on KNOWN_TO_MAX).

        Args:
            instrument_public_id: Public ID of the instrument.
            exchange: Exchange name (lowercase).
            rate_type: One of ``spot_margin_rollover`` or
                ``perpetual_funding``.
            direction: One of ``long``, ``short``, or ``both``.
            as_of: Point-in-time for SCD2 version filter.
            range_start: Inclusive lower bound on ``effective_from``.
                ``None`` means no lower bound.
            range_end: Inclusive upper bound on ``effective_from``.
                ``None`` means no upper bound.

        Returns:
            Rows ordered by ``effective_from`` ascending.
        """
        ...

    @abstractmethod
    async def insert_accrual(
        self,
        row: AccrualLedgerInsertRow,
        session: AsyncSession | None = None,
    ) -> int:
        """Insert an accrual ledger row, returning its integer id.

        Idempotent on the partial unique index over
        ``(wallet_public_id, instrument_public_id, mode, accrual_type,
        accrued_at)``: a duplicate raises ``IntegrityError``, which the
        caller swallows as a "boundary already applied" signal. The
        wallet prefix in the dedup key lets two wallets accrue the
        same instrument/mode/type/boundary without conflict.

        Args:
            row: Insert payload with all provenance fields.
            session: Optional caller-managed session. When provided, the
                method does not commit so the caller can sequence the
                insert with the in-memory mutation in a single
                transaction.

        Returns:
            Integer ``id`` of the new row.
        """
        ...

    @abstractmethod
    async def get_accruals(
        self,
        instrument_public_id: str,
        mode: str,
        range_start: datetime,
        range_end: datetime,
        wallet_public_id: str = "",
    ) -> list[AccrualLedgerRow]:
        """Return accrual ledger rows in a half-open recovery window.

        Used by the funding accrual recovery path. The lower bound is
        STRICT on ``timestamp`` (insertion bus-time, NOT ``accrued_at``)
        so that late-arriving accruals inserted after a fill-triggered
        checkpoint are still replayed. The upper bound is INCLUSIVE
        (``timestamp <= range_end``).

        Args:
            instrument_public_id: Public ID of the instrument.
            mode: Trading mode (``live``, ``paper``, ``backtest``).
            range_start: Strict lower bound on ``timestamp``.
            range_end: Inclusive upper bound on ``timestamp``.
            wallet_public_id: Wallet filter. Empty string matches all.

        Returns:
            Rows ordered by ``accrued_at`` ascending.
        """
        ...

    @abstractmethod
    async def get_last_accrual(
        self,
        instrument_public_id: str,
        mode: str,
        accrual_type: str,
        wallet_public_id: str = "",
    ) -> AccrualLedgerRow | None:
        """Return the most recent accrual for an instrument and type.

        Used by the funding accrual loop's catch-up logic to compute
        which boundaries are still pending since the last applied row.
        Reads the active SCD2 version with the maximum ``accrued_at``.

        Args:
            instrument_public_id: Public ID of the instrument.
            mode: Trading mode (``live``, ``paper``, ``backtest``).
            accrual_type: One of ``funding``, ``rollover``, ``borrow``.
            wallet_public_id: Wallet filter. Empty string matches all.

        Returns:
            Most recent accrual row, or ``None`` if no accrual has been
            applied yet for the given key.
        """
        ...

    @abstractmethod
    async def get_instrument_capabilities(
        self,
        as_of: datetime,
        exchange: str | None = None,
        instrument_public_id: str | None = None,
    ) -> list[InstrumentOrderCapabilityRow]:
        """Retrieve active instrument order capability rows.

        Args:
            as_of: Point-in-time for temporal query.
            exchange: Optional exchange filter.
            instrument_public_id: Optional instrument filter.

        Returns:
            Capability rows ordered by exchange, instrument.
        """
        ...

    @abstractmethod
    async def get_venue_fee_schedules(
        self,
        as_of: datetime,
        exchange: str | None = None,
    ) -> list[VenueFeeScheduleRow]:
        """Retrieve active venue fee schedule rows.

        Args:
            as_of: Point-in-time for temporal query.
            exchange: Optional exchange filter.

        Returns:
            Fee schedule rows ordered by exchange, fee_tier.
        """
        ...

    @abstractmethod
    async def insert_execution_plan(
        self,
        row: ExecutionPlanInsertRow,
    ) -> tuple[int, str]:
        """Insert a new execution plan row.

        Args:
            row: Plan insert payload.

        Returns:
            Tuple of (id, public_id) for the new plan.
        """
        ...

    @abstractmethod
    async def get_execution_plan(
        self,
        public_id: str,
        as_of: datetime,
    ) -> ExecutionPlanRow | None:
        """Retrieve a single execution plan by public_id.

        Args:
            public_id: Plan public identifier.
            as_of: Point-in-time for temporal query.

        Returns:
            Plan row or None if not found.
        """
        ...

    @abstractmethod
    async def get_execution_plans(
        self,
        as_of: datetime,
        status: str | None = None,
        exchange: str | None = None,
        mode: str | None = None,
        wallet_public_ids: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExecutionPlanRow]:
        """Retrieve execution plans with optional filters.

        Args:
            as_of: Point-in-time for temporal query.
            status: Optional status filter.
            exchange: Optional exchange filter.
            mode: Optional mode filter (live/paper).
            wallet_public_ids: Optional wallet scope filter.
            limit: Maximum number of plans to return.
            offset: Number of plans to skip.

        Returns:
            Plan rows ordered by created_at DESC.
        """
        ...

    @abstractmethod
    async def update_execution_plan_status(
        self,
        public_id: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        filled_quantity: float | None = None,
        last_error: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        cancel_requested_at: datetime | None = None,
        last_evaluated_at: datetime | None = None,
    ) -> int | None:
        """SCD2 close-and-insert for plan status transition.

        Args:
            public_id: Plan public identifier.
            new_status: New status value.
            bus_time: Timestamp for SCD2 close and new row.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.
            filled_quantity: Updated fill quantity (if changed).
            last_error: Error message (if failed).
            started_at: When plan started evaluating.
            completed_at: When plan completed.
            cancel_requested_at: When cancel was requested.
            last_evaluated_at: Last evaluation timestamp.

        Returns:
            New row id, or None if no active row found.
        """
        ...

    @abstractmethod
    async def get_active_execution_plans(
        self,
    ) -> list[ExecutionPlanRow]:
        """Retrieve all plans with actionable status for executor startup.

        Returns plans where status is one of: pending, armed, active,
        paused, cancel_requested. Used by PlanExecutorService recovery.

        Returns:
            Plan rows ordered by created_at ASC.
        """
        ...

    @abstractmethod
    async def insert_execution_plan_checkpoint(
        self,
        plan_public_id: str,
        state: JsonObject,
        last_venue_event_id: int,
        checkpoint_at: datetime,
        session_id: str,
        sequence_id: int,
        bus_time: datetime,
        last_tick_timestamp: datetime | None = None,
    ) -> tuple[int, str]:
        """Insert a new checkpoint for a plan (SCD2 close previous).

        Args:
            plan_public_id: Plan this checkpoint belongs to.
            state: Evaluator-specific state snapshot.
            last_venue_event_id: Watermark for replay.
            checkpoint_at: When the checkpoint was taken.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.
            bus_time: Timestamp for SCD2 operations.
            last_tick_timestamp: Most recent tick seen.

        Returns:
            Tuple of (id, public_id) for the new checkpoint.
        """
        ...

    @abstractmethod
    async def get_latest_plan_checkpoint(
        self,
        plan_public_id: str,
    ) -> ExecutionPlanCheckpointRow | None:
        """Return the most recent active checkpoint for a plan.

        Args:
            plan_public_id: Plan to query.

        Returns:
            Checkpoint row or None if no checkpoint exists.
        """
        ...

    @abstractmethod
    async def insert_execution_plan_decision(
        self,
        row: ExecutionPlanDecisionInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str:
        """Insert a decision row for audit trail.

        Args:
            row: Decision insert payload.
            bus_time: Timestamp for SCD2 operations.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.

        Returns:
            public_id of the new decision row.
        """
        ...

    @abstractmethod
    async def list_execution_plan_decisions(
        self,
        plan_public_id: str,
        as_of: datetime,
        importance: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExecutionPlanDecisionRow]:
        """Retrieve decision rows for a plan.

        Args:
            plan_public_id: Plan to query decisions for.
            as_of: Point-in-time for temporal query.
            importance: Optional importance filter (action/transition/routine).
            limit: Maximum rows to return.
            offset: Number of rows to skip.

        Returns:
            Decision rows ordered by decided_at DESC.
        """
        ...

    @abstractmethod
    async def revise_execution_plan_params(
        self,
        public_id: str,
        param_updates: JsonObject,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 close-and-insert for plan params revision only.

        Shallow-merges param_updates into the existing params dict.
        All other fields are preserved from the current active row.

        Args:
            public_id: Plan public identifier.
            param_updates: Dict of param keys to update (shallow merge).
            bus_time: Timestamp for SCD2 close and new row.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.
        """
        ...

    @abstractmethod
    async def insert_trade_command(
        self,
        row: TradeCommandInsertRow,
        *,
        ownership: ShardOwnership | None = None,
    ) -> tuple[int, str]:
        """Insert a new trade command row.

        Args:
            row: Trade command insert payload.
            ownership: Optional Phase 4 partitioning guard. When
                provided, the row's ``shard_key`` MUST be owned by
                this ownership view or :class:`ShardOwnershipError`
                is raised before the DB write. Opt-in — callers that
                legitimately write foreign-shard rows (HTTP handlers,
                plan services) pass ``None`` and rely on downstream
                filtering (outbox + coordinator) to route commands
                to the owning coordinator.

        Returns:
            Tuple of (id, public_id) for the new command.

        Raises:
            ShardOwnershipError: If ``ownership`` is not None and
                ``row["shard_key"]`` is not owned by it.
        """
        ...

    @abstractmethod
    async def get_user_trading_caps(self, user_public_id: str) -> UserTradingCapsRow | None:
        """Return the active ``user_trading_caps`` row for a user.

        Consumed by
        :class:`~snapper.application.trade.caps_enforcer.TradingCapsEnforcer`
        before every user-bound insert. Returns ``None`` when the
        user has no caps row (meaning "unbounded on all axes" —
        the enforcer's policy is to admit in that case).

        Args:
            user_public_id: UUID of the user to look up.

        Returns:
            Active caps row projection or ``None``.
        """
        ...

    @abstractmethod
    async def count_user_open_commands(self, user_public_id: str) -> int:
        """Count user's non-terminal trade-commands (``max_open_orders`` cap).

        All-time count (no time window — see §3.5.3 R3-M2 resolution).
        Non-terminal statuses: ``created``, ``dispatched``,
        ``acked``, ``accepted``, ``partially_filled``. Only
        command_type ``submit`` / ``replace`` rows count — cancels
        are not in-flight exposure.

        Args:
            user_public_id: UUID of the user to count for.

        Returns:
            Count of the user's active, non-terminal commands.
        """
        ...

    @abstractmethod
    async def get_user_recent_submits(
        self, user_public_id: str, since: datetime
    ) -> list[UserRecentSubmitRow]:
        """Return user's submit commands since a cut-off timestamp.

        Used by the ``max_daily_notional_usd`` cap: the enforcer
        sums ``quantity × price`` over these rows to get the
        rolling 24h USD commitment (§3.5.3). Excludes rows whose
        current status is ``rejected``.

        Args:
            user_public_id: UUID of the user.
            since: Minimum ``created_at`` (typically
                ``now - 24h``).

        Returns:
            List of projected rows (one per submit command).
        """
        ...

    @abstractmethod
    async def count_user_rolling_cancels(self, user_public_id: str, since: datetime) -> int:
        """Count user's cancel commands since a cut-off.

        Used by the ``max_cancels_per_minute`` cap (sliding 60s
        window per §3.5.3). Counts every row where
        ``command_type == 'cancel'`` AND
        ``created_at >= since``, regardless of terminal status
        (a cancel that was later rejected still counts against
        the rate budget — this is intentional: the cap limits
        SUBMIT frequency of cancel intents, not successful
        cancellations).

        Args:
            user_public_id: UUID of the user.
            since: Minimum ``created_at`` (typically
                ``now - 60s``).

        Returns:
            Count of cancel commands in the window.
        """
        ...

    @abstractmethod
    async def get_plan_public_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Look up the plan public id that a client_order_id belongs to.

        Joins ``trade_commands`` where ``command_type='create'`` and
        ``plan_public_id IS NOT NULL``, returning the most recent
        active-version ``plan_public_id`` for the matching
        ``client_order_id`` as of ``as_of``. Used by UI
        cancel-by-client-order-id flows to resolve the active plan from
        an Order row.

        Args:
            client_order_id: Child order client id stamped on a plan.
            as_of: Temporal point for active-version selection.

        Returns:
            The linked plan public id, or None if no plan-linked command
            exists for this client_order_id.
        """
        ...

    @abstractmethod
    async def get_exchange_order_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Fetch the active exchange_order_id for a given client_order_id.

        Used by the cancel pipeline to hydrate ``OrderCancelData`` with
        the venue-assigned order id so that venue adapters that cancel
        by exchange id (Kraken, Zonda, Walutomat) can actually cancel.
        Returns ``None`` if no active order row exists yet (e.g., the
        venue has not ACKed the submit), or if it exists but has not
        been assigned an ``exchange_order_id`` yet.

        Args:
            client_order_id: Client-side order id.
            as_of: Temporal point for SCD2 active-version selection.

        Returns:
            The exchange-assigned order id, or None if unavailable.
        """
        ...

    @abstractmethod
    async def has_pending_cancel_command(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> bool:
        """Return True when a non-terminal cancel command already exists.

        PlanExecutorService recovery uses this to avoid re-emitting a
        cancel ``TradeCommand`` for a plan in ``cancel_requested`` that
        already has a pending or dispatched cancel command. Without
        this guard, every restart while the plan is
        ``cancel_requested`` would enqueue a duplicate venue cancel.

        Args:
            client_order_id: Child client id of the cancel target.
            as_of: Temporal point for SCD2 active-version selection.

        Returns:
            True if an active ``command_type='cancel'`` row with a
            non-terminal status (``created``/``dispatched``/``acked``)
            exists, False otherwise.
        """
        ...

    @abstractmethod
    async def insert_position_cycle(
        self,
        row: PositionCycleInsertRow,
    ) -> tuple[int, str]:
        """Insert a new ``position_cycles`` row in the open state.

        A position cycle spans one flat->non-flat->flat trading lifetime on
        a single shard. Brackets (SL/TP) attach to a cycle by its public_id.

        Args:
            row: Cycle insert payload; callers must supply all non-default
                fields. ``status`` is typically ``"open"`` at insert time.

        Returns:
            Tuple of (id, public_id) for the new cycle row.
        """
        ...

    @abstractmethod
    async def close_position_cycle(
        self,
        cycle_public_id: str,
        closed_at: datetime,
        closing_command_public_id: str | None,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int | None:
        """SCD2 close-and-insert transitioning a cycle to ``closed``.

        Loads the active open row under lock, closes it (``known_to=bus_time``),
        and inserts a new row carrying the same ``public_id`` with
        ``status='closed'``, ``closed_at``, and ``closing_command_public_id``.

        Args:
            cycle_public_id: Public identifier of the cycle to close.
            closed_at: Timestamp at which the position returned to flat.
            closing_command_public_id: Optional command id that closed the
                cycle (may be None when the fill path has no command lineage).
            bus_time: Bus timestamp for the SCD2 operation.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.

        Returns:
            New row id, or None when no active open cycle exists for this
            ``cycle_public_id``.
        """
        ...

    @abstractmethod
    async def get_open_position_cycle(
        self,
        shard_key: str,
        as_of: datetime,
    ) -> PositionCycleRow | None:
        """Return the active open cycle for a shard at a point in time.

        Keyed by ``shard_key`` because paper-mode shards embed
        ``strategy_tag`` in the key; querying by
        ``(instrument, wallet, mode)`` alone would collide across
        strategies. At most one open cycle exists per shard at any
        instant (enforced by ``uq_pc_shard_open_active``).

        Args:
            shard_key: Engine shard key identifying the position.
            as_of: Bus time for the temporal query.

        Returns:
            Cycle row or None if the shard currently has no open cycle.
        """
        ...

    @abstractmethod
    async def get_position_cycle_by_public_id(
        self,
        cycle_public_id: str,
        as_of: datetime,
    ) -> PositionCycleRow | None:
        """Retrieve a position cycle by its public_id.

        Used by bracket creation to resolve the target cycle by explicit
        ID (rather than by shard_key which can collide after a flip).

        Args:
            cycle_public_id: Cycle public identifier.
            as_of: Bus time for the temporal query.

        Returns:
            Cycle row or None if not found at the given point in time.
        """
        ...

    @abstractmethod
    async def flip_position_cycle(
        self,
        close_cycle_public_id: str,
        new_open_row: PositionCycleInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> tuple[int, str]:
        """Atomically close one cycle and open another in a single transaction.

        Used when a fill reverses position sign (long -> short or vice
        versa) in one event. The existing cycle is loaded under lock and
        its ``shard_key`` is asserted to match ``new_open_row['shard_key']``
        to prevent stale ``close_cycle_public_id`` values from corrupting
        a foreign cycle. Close and insert run in the same session/commit
        so a crash between them cannot leave the DB with a stranded open.

        Args:
            close_cycle_public_id: Public id of the cycle to close.
            new_open_row: Insert payload for the replacement open cycle
                (``status`` is coerced to ``"open"``; ``closed_at`` and
                ``closing_command_public_id`` must be absent or None).
            bus_time: Bus timestamp for both SCD2 operations.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.

        Returns:
            Tuple of (id, public_id) for the newly opened cycle row.

        Raises:
            ValueError: Source cycle is missing, not active, or its
                ``shard_key`` does not match ``new_open_row['shard_key']``.
        """
        ...

    @abstractmethod
    async def update_position_cycle_max_qty(
        self,
        cycle_public_id: str,
        new_max_qty: float,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int | None:
        """Monotonic SCD2 revision of a cycle's ``max_qty`` peak.

        Loads the active row under lock and enforces monotonic progression:
        ``new_max_qty <= existing.max_qty`` is a silent no-op (returns
        ``None`` without writing). Only strictly larger values trigger
        a SCD2 close-and-insert carrying the new peak. Targets that are
        not currently active raise ``ValueError``.

        Args:
            cycle_public_id: Public id of the cycle to update.
            new_max_qty: Candidate peak (absolute, non-negative).
            bus_time: Bus timestamp for the SCD2 operation.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.

        Returns:
            New row id on a real update, None on a monotonic no-op.

        Raises:
            ValueError: Target cycle is missing or not active.
        """
        ...

    @abstractmethod
    async def get_all_open_position_cycles(
        self,
        as_of: datetime,
        opened_before: datetime | None = None,
    ) -> list[PositionCycleRow]:
        """Return all open position cycles, optionally filtered by age.

        Args:
            as_of: Bus time for the temporal query.
            opened_before: When provided, only cycles opened before this
                timestamp are returned (stale-cycle detection).

        Returns:
            List of open cycle rows matching the criteria.
        """
        ...

    @abstractmethod
    async def create_scope_grant(self, request: CreateScopeGrantRequest) -> ScopeGrantRow:
        """Create a new ``wallet_operator_scope_grants`` row.

        Instrument-exclusive: a wallet-level advisory lock is
        acquired on PostgreSQL before the overlap check, and overlap detection
        spans both same-scope and cross-scope conflicts (an underlying grant
        whose expanded instrument set intersects an existing instrument grant,
        or vice versa).

        Args:
            request: Insert payload — see ``CreateScopeGrantRequest``.

        Returns:
            The newly inserted scope grant row.

        Raises:
            ScopeGrantConflictError: An active grant on the same wallet
                already covers (any of) the requested instruments.
            ScopeGrantValidationError: Structural invariant violation
                (unknown ``scope_kind``, XOR mismatch, etc.).
        """
        ...

    @abstractmethod
    async def list_active_scope_grants_for_wallet(
        self,
        wallet_public_id: str,
        as_of: datetime,
    ) -> list[ScopeGrantRow]:
        """Active scope grants on the given wallet at a point in time.

        Args:
            wallet_public_id: Public ID of the wallet.
            as_of: Bus time for the temporal query.

        Returns:
            List of active scope grant rows ordered by ``timestamp`` ascending.
        """
        ...

    @abstractmethod
    async def list_grant_covered_instrument_public_ids(
        self,
        operator_public_id: str,
        wallet_public_id: str,
        as_of: datetime,
    ) -> set[str]:
        """Set of instrument_public_ids covered by an operator's active grants.

        Expands underlying-scoped grants to their current instrument set
        per the dynamic-scope rule. Used by the strategy permission check
        to verify that every output a strategy emits is in the operator's
        scope at strategy creation/start time.
        """
        ...

    @abstractmethod
    async def get_instrument_public_id_by_symbol(
        self,
        native_symbol: str,
        exchange: str,
        as_of: datetime,
    ) -> str | None:
        """Resolve a (native_symbol, exchange) pair to its instrument public_id.

        Returns None when no active Instrument row exists for the pair.
        """
        ...

    @abstractmethod
    async def handover_grant(
        self,
        from_grant_public_id: str,
        to_operator_public_id: str,
        granted_by_user_public_id: str,
        reason: str | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[ScopeGrantRow, ScopeGrantRow]:
        """Atomically transfer a scope grant to a different operator.

        Single transaction: SCD2-close the source grant and insert a
        new grant carrying the same ``scope_kind`` /
        ``underlying_public_id`` / ``instrument_public_id`` under the
        new operator. Repository-layer validation enforces source-row
        existence, non-self-handover, and overlap detection against
        the target operator's existing grants. The caller-permission
        check (the user must hold a grant on the source operator)
        lives in the API layer.

        Args:
            from_grant_public_id: Public ID of the active source grant.
            to_operator_public_id: Public ID of the destination operator.
            granted_by_user_public_id: Audit identity of the user performing
                the handover (recorded on the new grant row).
            reason: Free-form audit note (stored in the new grant's ``note``).
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time for the close + insert.

        Returns:
            ``(closed_from_grant, new_grant)`` — both as ``ScopeGrantRow``.

        Raises:
            ScopeGrantNotFoundError: Source grant or destination operator
                does not exist (or source is no longer active).
            ScopeGrantValidationError: Self-handover (no-op) or other
                structural violation.
        """
        ...

    @abstractmethod
    async def list_active_operators(self, as_of: datetime) -> list[OperatorRow]:
        """Return every active operator at the given bus time.

        Used by the login flow's ADMIN role mapping (admins automatically
        receive the operator set covering every active operator at token
        issue time) and by future API endpoints
        that surface the operator catalogue.

        Args:
            as_of: Bus time for the temporal query.

        Returns:
            Active operator rows ordered by ``label`` ascending.
        """
        ...

    @abstractmethod
    async def create_wallet(
        self,
        label: str,
        description: str | None,
        is_paper: bool,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> WalletRow:
        """Create a new active wallet row.

        Enforces the ``(label, is_paper)`` active-unique index on
        ``wallets``: two concurrent active rows sharing both columns
        are rejected by the DB layer, which bubbles up as
        ``WalletConflictError`` from this method.

        Used by the Phase 0d admin wallet creation endpoint
        (``POST /api/wallets``).

        Args:
            label: Human-readable wallet name.
            description: Optional free-form description.
            is_paper: Paper-mode flag.
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time for the insert.

        Returns:
            The newly-inserted ``WalletRow``.

        Raises:
            WalletConflictError: An active wallet with the same
                ``(label, is_paper)`` already exists.
        """
        ...

    @abstractmethod
    async def list_active_wallets(self, as_of: datetime) -> list[WalletRow]:
        """Return every active wallet at the given bus time.

        Used by the Phase 0d wallet catalogue endpoint (ADMIN only —
        non-admin callers must go through
        ``list_accessible_wallets_for_operators`` so they only see
        wallets covered by at least one of their active scope grants).

        Args:
            as_of: Bus time for the temporal query.

        Returns:
            Active wallet rows ordered by ``(is_paper, label)`` so the
            default seed — one paper + one live wallet sharing the
            ``default`` label — renders deterministically.
        """
        ...

    @abstractmethod
    async def list_accessible_wallets_for_operators(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> list[WalletRow]:
        """Wallets covered by at least one active grant from the given operators.

        Scoped variant of ``list_active_wallets`` for VIEWER / OPERATOR
        principals. The result is the set union over all operator IDs:
        a wallet is accessible if ANY of the principal's operators
        holds an active ``wallet_operator_scope_grants`` row on it.
        This matches the Phase 0d wallet picker contract: the picker
        is filtered server-side to the wallets the current operator
        can act on.

        Args:
            operator_public_ids: Principal's full operator ID set.
                When empty, the method returns an empty list without
                querying.
            as_of: Bus time for the temporal query.

        Returns:
            Active wallet rows, deduplicated, ordered by
            ``(is_paper, label)``. Empty list when no grants match.
        """
        ...

    @abstractmethod
    async def get_user_operator_memberships(
        self,
        user_public_id: str,
        as_of: datetime,
    ) -> list[UserOperatorMembershipRow]:
        """Return active operator memberships for a user.

        Used by the login flow to compute ``operator_public_ids`` and
        ``primary_operator_public_id`` on ``AuthPrincipal``.

        Args:
            user_public_id: Public ID of the user.
            as_of: Bus time for the temporal query.

        Returns:
            Membership rows ordered by ``timestamp`` ascending. The
            primary membership (``is_primary=True``) is included; the
            ``user_operator_memberships`` partial unique index guarantees
            at most one primary per user, so callers may safely pick the
            first ``is_primary`` row.
        """
        ...

    @abstractmethod
    async def get_active_credential(
        self,
        exchange: str,
        wallet_public_id: str,
        as_of: datetime,
    ) -> WalletCredentialRow | None:
        """Return the active wallet credential row for ``(exchange, wallet)``.

        Used by ``CredentialResolver`` at executor
        process startup to fetch the encrypted credential payload before
        constructing the per-wallet exchange client. Pull-on-startup
        only — there is no caching contract on top of this method.

        Args:
            exchange: Exchange identifier (lowercase, matches the
                ``ck_wallet_credentials_exchange_lower`` constraint).
            wallet_public_id: Public ID of the wallet.
            as_of: Bus time for the temporal query.

        Returns:
            The active credential row or ``None`` when no credential
            exists for the given (exchange, wallet) pair at ``as_of``.
            ``CredentialResolver`` translates ``None`` to its own
            ``CredentialNotFoundError`` so the executor startup
            failure surfaces with a clear cause.
        """
        ...

    @abstractmethod
    async def get_active_credential_by_id(
        self,
        credential_public_id: str,
        as_of: datetime,
    ) -> WalletCredentialRow | None:
        """Return the active credential row for the given ``public_id``.

        Used by the rotation pre-check to load the existing row's
        ``credential_type`` for payload validation before the SCD2
        close+insert. Returns ``None`` when the credential does not
        exist or is already closed at ``as_of``.
        """
        ...

    @abstractmethod
    async def create_wallet_credential(
        self,
        wallet_public_id: str,
        exchange: str,
        credential_type: str,
        encrypted_payload: str,
        label: str | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> WalletCredentialRow:
        """Insert a new active wallet credential row.

        The ``encrypted_payload`` is already Fernet-encrypted by the
        caller (the route handler encrypts before calling). The
        active-unique index on ``(wallet_public_id, exchange)`` is
        enforced at the DB layer.

        Raises:
            CredentialConflictError: An active credential for the
                same ``(wallet, exchange)`` already exists.
        """
        ...

    @abstractmethod
    async def rotate_wallet_credential(
        self,
        credential_public_id: str,
        encrypted_payload: str,
        label: str | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> WalletCredentialRow:
        """SCD2 close + insert rotation of a wallet credential.

        Closes the existing active credential row (sets ``known_to``
        to ``timestamp``) and inserts a new active row carrying the
        same ``(wallet_public_id, exchange, credential_type)`` with
        the new encrypted payload.

        Raises:
            CredentialNotFoundError: The credential_public_id does
                not match an active row at ``timestamp``.
        """
        ...

    @abstractmethod
    async def list_wallet_credentials_for_wallet(
        self,
        wallet_public_id: str,
        as_of: datetime,
    ) -> list[WalletCredentialRow]:
        """Active credentials on a single wallet at ``as_of``.

        Returns every active ``wallet_credentials`` row where
        ``wallet_public_id`` matches. Ordered by ``exchange``.
        """
        ...

    @abstractmethod
    async def list_active_wallet_credentials(
        self,
        as_of: datetime,
    ) -> list[WalletCredentialRow]:
        """Return every active wallet credential row at ``as_of``.

        Dynamic per-wallet executor spawning consumes this
        list at server boot to discover the ``(exchange, wallet)``
        pairs that need a dedicated executor instance. Each row drives
        one ``ProcessLauncherService.start_process`` call with a
        per-wallet ``ProcessConfigModel`` whose ``parameters`` carry
        the wallet's public ID into ``ExchangeExecutorService``.

        The result is ordered by ``(exchange, wallet_public_id)`` so
        the spawner produces deterministic process names regardless
        of insertion order — important for log diff stability across
        boots.

        Args:
            as_of: Bus time for the temporal query (process boot time
                in production; explicit timestamps in tests).

        Returns:
            List of active credential rows. Empty list when no
            credentials are seeded yet (e.g. fresh DB before
            ``seed_default_multi_tenant`` runs).
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
        _live_sqlalchemy_repositories.add(self)

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

    async def get_instrument_spec(
        self,
        instrument_public_id: str,
        as_of: datetime,
    ) -> InstrumentSpecRow | None:
        """Return the active InstrumentSpec for an instrument, or None."""
        async with self.session() as s:
            ts_filter, kt_filter = where_active(InstrumentSpec, as_of)
            q = await s.execute(
                select(InstrumentSpec).where(
                    InstrumentSpec.instrument_public_id == instrument_public_id,
                    ts_filter,
                    kt_filter,
                )
            )
            row = q.scalar_one_or_none()
            if row is None:
                return None
            return InstrumentSpecRow(
                instrument_public_id=row.instrument_public_id,
                tick_size=row.tick_size,
                lot_size=row.lot_size,
                min_order_size=row.min_order_size,
                max_order_size=row.max_order_size,
                cost_decimals=row.cost_decimals,
                qty_decimals=row.qty_decimals,
                margin_initial=row.margin_initial,
                position_limit_long=row.position_limit_long,
                position_limit_short=row.position_limit_short,
                status=row.status,
                expiry_at=row.expiry_at,
                instrument_kind=row.instrument_kind,
                funding_type=row.funding_type,
                funding_frequency_hours=row.funding_frequency_hours,
                rollover_rate_long=row.rollover_rate_long,
                rollover_rate_short=row.rollover_rate_short,
                max_funding_rate=row.max_funding_rate,
            )

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
        return await self._upsert_batch(Trade, rows, ["instrument_public_id", "trade_id"])

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
        wallet_public_id: str,
        operator_public_id: str | None = None,
        time_in_force: str | None = None,
        mode: str = "live",
        leverage: int | None = None,
        reduce_only: bool = False,
    ) -> tuple[int, str]:
        """Insert new order record and return (id, public_id) tuple."""
        async with self.session() as s:
            order = Order(
                instrument_public_id=instrument_public_id,
                mode=mode,
                wallet_public_id=wallet_public_id,
                operator_public_id=operator_public_id,
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
                leverage=leverage,
                reduce_only=reduce_only,
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
                mode=old_order.mode,
                wallet_public_id=old_order.wallet_public_id,
                operator_public_id=old_order.operator_public_id,
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
                leverage=old_order.leverage,
                reduce_only=old_order.reduce_only,
                plan_public_id=old_order.plan_public_id,
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
        wallet_public_id: str,
        exec_id: str | None = None,
        trade_id: str | None = None,
        operator_public_id: str | None = None,
        liquidity_role: str = "unknown",
    ) -> int:
        """Insert execution record and return generated ID."""
        async with self.session() as s:
            execution = Execution(
                order_public_id=order_public_id,
                wallet_public_id=wallet_public_id,
                operator_public_id=operator_public_id,
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
                liquidity_role=liquidity_role,
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
        wallet_public_ids: list[str] | None = None,
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
            if wallet_public_ids is not None:
                query = query.where(Signal.wallet_public_id.in_(wallet_public_ids))
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
                    "wallet_public_id": sig.wallet_public_id,
                    "operator_public_id": sig.operator_public_id,
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
        wallet_public_ids: list[str] | None = None,
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
            if wallet_public_ids is not None:
                query = query.where(Order.wallet_public_id.in_(wallet_public_ids))
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
                    "mode": order.mode,
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
                    "leverage": order.leverage,
                    "reduce_only": order.reduce_only,
                    "wallet_public_id": order.wallet_public_id,
                    "operator_public_id": order.operator_public_id,
                }
                for order, inst, sym in result.all()
            ]

    async def get_executions(
        self,
        limit: int,
        as_of: datetime,
        wallet_public_ids: list[str] | None = None,
    ) -> list[ExecutionRow]:
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
            )
            if wallet_public_ids is not None:
                query = query.where(Execution.wallet_public_id.in_(wallet_public_ids))
            query = query.order_by(desc(Execution.timestamp)).limit(limit)
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
                    "wallet_public_id": exe.wallet_public_id,
                    "operator_public_id": exe.operator_public_id,
                    "liquidity_role": getattr(exe, "liquidity_role", "unknown"),
                }
                for exe, order, inst, sym in result.all()
            ]

    _ACTIVE_ORDER_STATUSES = ("open", "pending", "pending_new", "new", "partially_filled")

    async def get_active_orders_for_recovery(
        self,
        exchange: str,
        as_of: datetime,
        wallet_public_id: str = "",
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
            if wallet_public_id:
                query = query.where(Order.wallet_public_id == wallet_public_id)
            result = await s.execute(query)
            return [
                {
                    "public_id": order.public_id,
                    "timestamp": order.timestamp,
                    "session_id": order.session_id,
                    "sequence_id": order.sequence_id,
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "mode": order.mode,
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
                    "leverage": order.leverage,
                    "reduce_only": order.reduce_only,
                    "wallet_public_id": order.wallet_public_id,
                    "operator_public_id": order.operator_public_id,
                }
                for order, inst, sym in result.all()
            ]

    async def get_executions_for_recovery(
        self,
        as_of: datetime,
        exchange: str | None = None,
        instrument: str | None = None,
        wallet_public_id: str = "",
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
            if wallet_public_id:
                query = query.where(Execution.wallet_public_id == wallet_public_id)
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
                    "wallet_public_id": exe.wallet_public_id,
                    "operator_public_id": exe.operator_public_id,
                    "liquidity_role": getattr(exe, "liquidity_role", "unknown"),
                }
                for exe, order, inst, sym in result.all()
            ]

    async def get_positions(
        self,
        as_of: datetime,
        wallet_public_ids: list[str] | None = None,
    ) -> list[PositionRow]:
        """Retrieve active positions with instrument/symbol info.

        The position_cycle_public_id is resolved via a grouped subquery
        that returns a cycle only when exactly one open cycle matches
        the position's (instrument, exchange, mode, wallet). Multiple
        matching cycles (e.g. paper mode with strategy tags) yield NULL
        to prevent attaching a bracket to the wrong cycle.
        """
        async with self.session() as s:
            open_cycle_unambiguous = (
                select(
                    PositionCycle.instrument_public_id.label("instrument_public_id"),
                    PositionCycle.exchange.label("exchange"),
                    PositionCycle.mode.label("mode"),
                    PositionCycle.wallet_public_id.label("wallet_public_id"),
                    func.min(PositionCycle.public_id).label("position_cycle_public_id"),
                )
                .where(
                    PositionCycle.status == "open",
                    *where_active(PositionCycle, as_of),
                )
                .group_by(
                    PositionCycle.instrument_public_id,
                    PositionCycle.exchange,
                    PositionCycle.mode,
                    PositionCycle.wallet_public_id,
                )
                .having(func.count(PositionCycle.public_id) == 1)
                .subquery()
            )

            query = (
                select(
                    Position,
                    Instrument,
                    Symbol,
                    open_cycle_unambiguous.c.position_cycle_public_id,
                )
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
                .outerjoin(
                    open_cycle_unambiguous,
                    and_(
                        open_cycle_unambiguous.c.instrument_public_id == Instrument.public_id,
                        open_cycle_unambiguous.c.exchange == Instrument.exchange,
                        open_cycle_unambiguous.c.mode == Position.mode,
                        open_cycle_unambiguous.c.wallet_public_id == Position.wallet_public_id,
                    ),
                )
                .where(*where_active(Position, as_of))
            )
            if wallet_public_ids is not None:
                query = query.where(Position.wallet_public_id.in_(wallet_public_ids))
            result = await s.execute(query)
            return [
                {
                    "public_id": pos.public_id,
                    "timestamp": pos.timestamp,
                    "session_id": pos.session_id,
                    "sequence_id": pos.sequence_id,
                    "instrument": sym.native_symbol,
                    "exchange": inst.exchange,
                    "mode": pos.mode,
                    "quantity": pos.quantity,
                    "average_price": pos.average_price,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "realized_pnl": pos.realized_pnl,
                    "position_cycle_public_id": cycle_pid,
                }
                for pos, inst, sym, cycle_pid in result.all()
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

    async def insert_trade_command(
        self,
        row: TradeCommandInsertRow,
        *,
        ownership: ShardOwnership | None = None,
    ) -> tuple[int, str]:
        """Insert a new trade command row and return (id, public_id).

        ``wallet_public_id`` is NOT NULL at the schema
        level. Callers that have not yet migrated to providing an
        explicit wallet default to the empty-string legacy sentinel
        (the same default that :class:`ExchangeExecutorService` uses
        for single-wallet template instantiations).

        Phase 4 partitioning guard (opt-in, Day 3): when ``ownership``
        is non-None, the row's ``shard_key`` MUST be owned by it or
        :class:`ShardOwnershipError` is raised before the DB write.
        """
        if ownership is not None and not ownership.owns(row["shard_key"]):
            raise ShardOwnershipError(
                shard_key=row["shard_key"],
                instance_id=ownership.instance_id,
                instance_count=ownership.instance_count,
            )
        async with self.session() as s:
            row_with_defaults: dict[str, Any] = {"wallet_public_id": "", **row}
            cmd = TradeCommand(**row_with_defaults)
            s.add(cmd)
            await s.commit()
            await s.refresh(cmd)
            return (cmd.id, cmd.public_id)

    async def get_user_trading_caps(self, user_public_id: str) -> UserTradingCapsRow | None:
        """Return the active ``user_trading_caps`` row or ``None``.

        Reads the SCD2-active row (``known_to == KNOWN_TO_MAX``)
        for the user. Consumed by
        :class:`~snapper.application.trade.caps_enforcer.TradingCapsEnforcer`
        before every user-bound insert.
        """
        async with self.session() as s:
            result = await s.execute(
                select(
                    UserTradingCaps.public_id,
                    UserTradingCaps.user_public_id,
                    UserTradingCaps.max_order_quantity_per_instrument,
                    UserTradingCaps.max_open_orders,
                    UserTradingCaps.max_daily_notional_usd,
                    UserTradingCaps.max_cancels_per_minute,
                ).where(
                    UserTradingCaps.user_public_id == user_public_id,
                    UserTradingCaps.known_to == KNOWN_TO_MAX,
                )
            )
            row = result.first()
            if row is None:
                return None
            return {
                "public_id": row[0],
                "user_public_id": row[1],
                "max_order_quantity_per_instrument": row[2],
                "max_open_orders": row[3],
                "max_daily_notional_usd": float(row[4]) if row[4] is not None else None,
                "max_cancels_per_minute": row[5],
            }

    async def count_user_open_commands(self, user_public_id: str) -> int:
        """Count non-terminal submit-type commands for a user.

        All-time count per plan §3.5.3 (no time window). Cancel
        commands are excluded: they are not in-flight exposure.

        The DB persists two submit-type vocabularies: REST routes and
        plan helpers (bracket, trailing_stop) insert ``"create"``;
        strategy/engine paths insert ``"submit"`` via
        :class:`OrderCommandEnum`; replace paths insert ``"replace"``.
        All three are counted against ``max_open_orders`` — the cap
        limits user exposure regardless of origin surface.
        """
        terminal = _TRADE_COMMAND_TERMINAL_STATUSES
        async with self.session() as s:
            result = await s.execute(
                select(func.count())
                .select_from(TradeCommand)
                .where(
                    TradeCommand.user_public_id == user_public_id,
                    TradeCommand.command_type.in_(("create", "submit", "replace")),
                    TradeCommand.status.notin_(terminal),
                    TradeCommand.known_to == KNOWN_TO_MAX,
                )
            )
            count = result.scalar_one()
            return int(count)

    async def get_user_recent_submits(
        self, user_public_id: str, since: datetime
    ) -> list[UserRecentSubmitRow]:
        """Return submit-type rows since ``since`` (for 24h notional sum).

        Excludes rows whose active status is ``rejected``. Returns
        the minimal projection the enforcer needs to compute
        rolling 24h USD notional.

        Includes all three submit-type vocabularies (``create`` from
        REST/plan inserts, ``submit`` from strategy/engine, ``replace``
        from amends) — the 24h notional cap limits user exposure
        regardless of origin surface.
        """
        async with self.session() as s:
            result = await s.execute(
                select(
                    TradeCommand.instrument,
                    TradeCommand.exchange,
                    TradeCommand.quantity,
                    TradeCommand.price,
                ).where(
                    TradeCommand.user_public_id == user_public_id,
                    TradeCommand.command_type.in_(("create", "submit", "replace")),
                    TradeCommand.status != TradeCommandStatusEnum.REJECTED,
                    TradeCommand.created_at >= since,
                    TradeCommand.known_to == KNOWN_TO_MAX,
                )
            )
            return [
                {
                    "instrument": r.instrument,
                    "exchange": r.exchange,
                    "quantity": r.quantity,
                    "price": r.price,
                }
                for r in result.all()
            ]

    async def count_user_rolling_cancels(self, user_public_id: str, since: datetime) -> int:
        """Count cancel commands submitted by user since ``since``.

        Includes rows regardless of terminal status — the cap
        limits submit frequency of cancel intents, not success.
        """
        async with self.session() as s:
            result = await s.execute(
                select(func.count())
                .select_from(TradeCommand)
                .where(
                    TradeCommand.user_public_id == user_public_id,
                    TradeCommand.command_type == "cancel",
                    TradeCommand.created_at >= since,
                    TradeCommand.known_to == KNOWN_TO_MAX,
                )
            )
            count = result.scalar_one()
            return int(count)

    async def get_plan_public_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Resolve the plan public id linked to a child client_order_id.

        Selects the most recently created ``trade_commands`` row whose
        ``client_order_id`` matches, whose ``plan_public_id`` is
        non-null, and whose ``command_type`` is ``create`` so cancel
        rows on the same ``client_order_id`` do not hide the original
        plan. Ordered by ``created_at`` desc with ``id`` as a
        deterministic tie-breaker so callers always see the same answer
        across invocations at the requested temporal point.

        Args:
            client_order_id: Child order client id stamped by the plan.
            as_of: Temporal point for active-version selection.

        Returns:
            The resolved plan public id, or None if no plan-linked
            trade command exists for this client_order_id.
        """
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand.plan_public_id)
                .where(
                    TradeCommand.client_order_id == client_order_id,
                    TradeCommand.plan_public_id.is_not(None),
                    TradeCommand.command_type == "create",
                    *where_active(TradeCommand, as_of),
                )
                .order_by(TradeCommand.created_at.desc(), TradeCommand.id.desc())
                .limit(1)
            )
            row = result.first()
            if row is None:
                return None
            return cast(str | None, row[0])

    async def get_exchange_order_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Return the venue-assigned order id for a client_order_id, if any.

        Picks the active SCD2 ``orders`` row with a non-null
        ``exchange_order_id``, ordered by ``created_at`` desc and ``id``
        desc as a deterministic tie-breaker.

        Args:
            client_order_id: Client-side order id.
            as_of: Temporal point for active-version selection.

        Returns:
            The venue-assigned order id, or None if no active order row
            has been ACKed yet.
        """
        async with self.session() as s:
            result = await s.execute(
                select(Order.exchange_order_id)
                .where(
                    Order.client_order_id == client_order_id,
                    Order.exchange_order_id.is_not(None),
                    *where_active(Order, as_of),
                )
                .order_by(Order.created_at.desc(), Order.id.desc())
                .limit(1)
            )
            row = result.first()
            if row is None:
                return None
            return cast(str | None, row[0])

    async def has_pending_cancel_command(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> bool:
        """Return True when a non-terminal cancel command exists for the cid.

        Looks for an active SCD2 ``trade_commands`` row with
        ``command_type='cancel'`` whose status is not in a terminal
        set (``filled``/``cancelled``/``expired``/``rejected``/``failed``).
        PlanExecutorService recovery uses this as a dedup guard when
        re-emitting stranded cancels.

        Args:
            client_order_id: Child client id to search.
            as_of: Temporal point for active-version selection.

        Returns:
            True iff a live cancel command already exists.
        """
        terminal_statuses = _TRADE_COMMAND_TERMINAL_STATUSES
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand.id)
                .where(
                    TradeCommand.client_order_id == client_order_id,
                    TradeCommand.command_type == "cancel",
                    TradeCommand.status.notin_(terminal_statuses),
                    *where_active(TradeCommand, as_of),
                )
                .limit(1)
            )
            return result.first() is not None

    async def update_trade_command_status(
        self,
        public_id: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None = None,
        dispatched_at: datetime | None = None,
        acked_at: datetime | None = None,
        terminal_at: datetime | None = None,
        last_error: str | None = None,
        attempt_count: int | None = None,
    ) -> int | None:
        """SCD2 close-and-insert for trade command status transition.

        Returns the new row id, or None if no active row found.
        """
        async with self.session() as s:
            match_filters = [TradeCommand.public_id == public_id]
            existing = (
                (
                    await s.execute(
                        select(TradeCommand)
                        .where(*match_filters, *where_active(TradeCommand, bus_time))
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return None
            await s.execute(
                update(TradeCommand).where(TradeCommand.id == existing.id).values(known_to=bus_time)
            )
            new_cmd = TradeCommand(
                public_id=existing.public_id,
                command_type=existing.command_type,
                shard_key=existing.shard_key,
                exchange=existing.exchange,
                instrument=existing.instrument,
                mode=existing.mode,
                strategy_id=existing.strategy_id,
                client_order_id=existing.client_order_id,
                venue_client_id=existing.venue_client_id,
                idempotency_key=existing.idempotency_key,
                side=existing.side,
                order_type=existing.order_type,
                quantity=existing.quantity,
                price=existing.price,
                leverage=existing.leverage,
                reduce_only=existing.reduce_only,
                status=new_status,
                attempt_count=(
                    attempt_count if attempt_count is not None else existing.attempt_count
                ),
                last_error=last_error,
                created_at=existing.created_at,
                dispatched_at=(
                    dispatched_at if dispatched_at is not None else existing.dispatched_at
                ),
                acked_at=acked_at if acked_at is not None else existing.acked_at,
                terminal_at=terminal_at if terminal_at is not None else existing.terminal_at,
                exchange_order_id=(
                    exchange_order_id
                    if exchange_order_id is not None
                    else existing.exchange_order_id
                ),
                supersedes_command_id=existing.supersedes_command_id,
                correlation_id=existing.correlation_id,
                plan_public_id=existing.plan_public_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                user_public_id=existing.user_public_id,
            )
            s.add(new_cmd)
            await s.commit()
            await s.refresh(new_cmd)
            return new_cmd.id

    async def get_undispatched_commands(
        self,
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
    ) -> list[TradeCommandRow]:
        """Return trade commands with status='created' for outbox dispatch.

        Phase 4 pagination: when ``offset > 0``, the query skips the
        first ``offset`` rows. Used by
        :class:`OutboxDispatcher._dispatch_batch` to page through the
        ``created`` backlog while filtering for owned shards in Python
        — see plan §3.3 / §D4 for the starvation-bound contract.

        Ordering is ``(created_at, id)`` for deterministic pagination:
        plan-service dispatch inserts multiple commands within a single
        ``now`` tick (see ``application/plans/service.py``) so ties on
        ``created_at`` are realistic. Without the ``id`` tie-breaker,
        ``OFFSET`` pagination could skip or duplicate rows across pages
        → double-dispatch. Ticket: R1 review of Phase 4 Day 3 commit
        c640505.
        """
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(TradeCommand.status == "created", *where_active(TradeCommand, as_of))
                .order_by(TradeCommand.created_at, TradeCommand.id)
                .offset(offset)
                .limit(limit)
            )
            rows: list[TradeCommandRow] = []
            for cmd in result.scalars().all():
                rows.append(
                    {
                        "public_id": cmd.public_id,
                        "timestamp": cmd.timestamp,
                        "session_id": cmd.session_id,
                        "sequence_id": cmd.sequence_id,
                        "command_type": cmd.command_type,
                        "shard_key": cmd.shard_key,
                        "exchange": cmd.exchange,
                        "instrument": cmd.instrument,
                        "mode": cmd.mode,
                        "strategy_id": cmd.strategy_id,
                        "client_order_id": cmd.client_order_id,
                        "venue_client_id": cmd.venue_client_id,
                        "idempotency_key": cmd.idempotency_key,
                        "side": cmd.side,
                        "order_type": cmd.order_type,
                        "quantity": cmd.quantity,
                        "price": cmd.price,
                        "leverage": cmd.leverage,
                        "reduce_only": cmd.reduce_only,
                        "status": cmd.status,
                        "attempt_count": cmd.attempt_count,
                        "last_error": cmd.last_error,
                        "created_at": cmd.created_at,
                        "dispatched_at": cmd.dispatched_at,
                        "acked_at": cmd.acked_at,
                        "terminal_at": cmd.terminal_at,
                        "exchange_order_id": cmd.exchange_order_id,
                        "supersedes_command_id": cmd.supersedes_command_id,
                        "correlation_id": cmd.correlation_id,
                        "wallet_public_id": cmd.wallet_public_id,
                        "operator_public_id": cmd.operator_public_id,
                        "user_public_id": cmd.user_public_id,
                        "source_surface": cmd.source_surface,
                    }
                )
            return rows

    async def get_active_commands_for_shard(
        self, shard_key: str, as_of: datetime
    ) -> list[TradeCommandRow]:
        """Return non-terminal trade commands for a shard."""
        terminal_statuses = _TRADE_COMMAND_TERMINAL_STATUSES
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(
                    TradeCommand.shard_key == shard_key,
                    TradeCommand.status.notin_(terminal_statuses),
                    *where_active(TradeCommand, as_of),
                )
                .order_by(TradeCommand.created_at)
            )
            rows: list[TradeCommandRow] = []
            for cmd in result.scalars().all():
                rows.append(
                    {
                        "public_id": cmd.public_id,
                        "timestamp": cmd.timestamp,
                        "session_id": cmd.session_id,
                        "sequence_id": cmd.sequence_id,
                        "command_type": cmd.command_type,
                        "shard_key": cmd.shard_key,
                        "exchange": cmd.exchange,
                        "instrument": cmd.instrument,
                        "mode": cmd.mode,
                        "strategy_id": cmd.strategy_id,
                        "client_order_id": cmd.client_order_id,
                        "venue_client_id": cmd.venue_client_id,
                        "idempotency_key": cmd.idempotency_key,
                        "side": cmd.side,
                        "order_type": cmd.order_type,
                        "quantity": cmd.quantity,
                        "price": cmd.price,
                        "leverage": cmd.leverage,
                        "reduce_only": cmd.reduce_only,
                        "status": cmd.status,
                        "attempt_count": cmd.attempt_count,
                        "last_error": cmd.last_error,
                        "created_at": cmd.created_at,
                        "dispatched_at": cmd.dispatched_at,
                        "acked_at": cmd.acked_at,
                        "terminal_at": cmd.terminal_at,
                        "exchange_order_id": cmd.exchange_order_id,
                        "supersedes_command_id": cmd.supersedes_command_id,
                        "correlation_id": cmd.correlation_id,
                        "wallet_public_id": cmd.wallet_public_id,
                        "operator_public_id": cmd.operator_public_id,
                        "user_public_id": cmd.user_public_id,
                        "source_surface": cmd.source_surface,
                    }
                )
            return rows

    async def get_active_commands_for_exchange(
        self, exchange: str, as_of: datetime
    ) -> list[TradeCommandRow]:
        """Return non-terminal trade commands for an exchange.

        Queries all shards for the given exchange name, returning
        commands that are not in a terminal state.

        Args:
            exchange: Exchange name to filter by.
            as_of: Point-in-time for temporal query.

        Returns:
            List of active TradeCommandRow dicts.
        """
        terminal_statuses = _TRADE_COMMAND_TERMINAL_STATUSES
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(
                    TradeCommand.exchange == exchange,
                    TradeCommand.status.notin_(terminal_statuses),
                    *where_active(TradeCommand, as_of),
                )
                .order_by(TradeCommand.created_at)
            )
            rows: list[TradeCommandRow] = []
            for cmd in result.scalars().all():
                rows.append(
                    {
                        "public_id": cmd.public_id,
                        "timestamp": cmd.timestamp,
                        "session_id": cmd.session_id,
                        "sequence_id": cmd.sequence_id,
                        "command_type": cmd.command_type,
                        "shard_key": cmd.shard_key,
                        "exchange": cmd.exchange,
                        "instrument": cmd.instrument,
                        "mode": cmd.mode,
                        "strategy_id": cmd.strategy_id,
                        "client_order_id": cmd.client_order_id,
                        "venue_client_id": cmd.venue_client_id,
                        "idempotency_key": cmd.idempotency_key,
                        "side": cmd.side,
                        "order_type": cmd.order_type,
                        "quantity": cmd.quantity,
                        "price": cmd.price,
                        "leverage": cmd.leverage,
                        "reduce_only": cmd.reduce_only,
                        "status": cmd.status,
                        "attempt_count": cmd.attempt_count,
                        "last_error": cmd.last_error,
                        "created_at": cmd.created_at,
                        "dispatched_at": cmd.dispatched_at,
                        "acked_at": cmd.acked_at,
                        "terminal_at": cmd.terminal_at,
                        "exchange_order_id": cmd.exchange_order_id,
                        "supersedes_command_id": cmd.supersedes_command_id,
                        "correlation_id": cmd.correlation_id,
                        "wallet_public_id": cmd.wallet_public_id,
                        "operator_public_id": cmd.operator_public_id,
                        "user_public_id": cmd.user_public_id,
                        "source_surface": cmd.source_surface,
                    }
                )
            return rows

    async def insert_venue_event(self, row: VenueEventInsertRow) -> int:
        """Insert a venue event and return its local_seq.

        Callers without a populated ``wallet_public_id`` get the
        empty-string legacy sentinel to satisfy the NOT NULL
        constraint. Production executor paths always populate the
        wallet explicitly; the default only covers test fixtures
        that have not migrated to providing one.
        """
        async with self.session() as s:
            row_with_defaults: dict[str, Any] = {"wallet_public_id": "", **row}
            ve = VenueEvent(**row_with_defaults)
            s.add(ve)
            await s.commit()
            await s.refresh(ve)
            return ve.id

    async def get_venue_events_after(self, shard_key: str, after_id: int) -> list[VenueEventRow]:
        """Return venue events for a shard after the given watermark (id)."""
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent)
                .where(VenueEvent.shard_key == shard_key, VenueEvent.id > after_id)
                .order_by(VenueEvent.id)
            )
            rows: list[VenueEventRow] = []
            for ve in result.scalars().all():
                rows.append(
                    {
                        "id": ve.id,
                        "public_id": ve.public_id,
                        "timestamp": ve.timestamp,
                        "session_id": ve.session_id,
                        "sequence_id": ve.sequence_id,
                        "event_type": ve.event_type,
                        "shard_key": ve.shard_key,
                        "command_public_id": ve.command_public_id,
                        "exchange": ve.exchange,
                        "instrument": ve.instrument,
                        "mode": ve.mode,
                        "exchange_order_id": ve.exchange_order_id,
                        "client_order_id": ve.client_order_id,
                        "venue_client_id": ve.venue_client_id,
                        "side": ve.side,
                        "status": ve.status,
                        "fill_price": ve.fill_price,
                        "fill_size": ve.fill_size,
                        "cum_fill_size": ve.cum_fill_size,
                        "fee": ve.fee,
                        "fee_asset": ve.fee_asset,
                        "exec_id": ve.exec_id,
                        "trade_id": ve.trade_id,
                        "error": ve.error,
                        "venue_timestamp": ve.venue_timestamp,
                        "received_at": ve.received_at,
                        "liquidity_role": getattr(ve, "liquidity_role", "unknown"),
                    }
                )
            return rows

    async def upsert_checkpoint(self, row: CheckpointUpsertRow) -> int:
        """SCD2 upsert for trade projection checkpoint.

        Returns the new row id. Legacy-default: missing
        ``wallet_public_id`` collapses to empty string for
        NOT-NULL-compliant inserts on legacy fixtures.
        """
        async with self.session() as s:
            new_values: dict[str, Any] = {k: v for k, v in row.items() if k != "bus_time"}
            new_values.setdefault("wallet_public_id", "")
            obj = await close_and_insert(
                s,
                TradeProjectionCheckpoint,
                [TradeProjectionCheckpoint.shard_key == row["shard_key"]],
                new_values,
                row["bus_time"],
            )
            await s.commit()
            await s.refresh(obj)
            return int(obj.id)

    async def get_checkpoint(
        self, shard_key: str, as_of: datetime
    ) -> TradeProjectionCheckpointRow | None:
        """Return the active checkpoint for a shard, or None."""
        async with self.session() as s:
            result = await s.execute(
                select(TradeProjectionCheckpoint).where(
                    TradeProjectionCheckpoint.shard_key == shard_key,
                    *where_active(TradeProjectionCheckpoint, as_of),
                )
            )
            cp = result.scalars().first()
            if cp is None:
                return None
            row: TradeProjectionCheckpointRow = {
                "public_id": cp.public_id,
                "shard_key": cp.shard_key,
                "position_qty": cp.position_qty,
                "entry_price": cp.entry_price,
                "position_opened_at": cp.position_opened_at,
                "cash": cp.cash,
                "peak_equity": cp.peak_equity,
                "realized_pnl": cp.realized_pnl,
                "turnover": cp.turnover,
                "last_venue_event_id": cp.last_venue_event_id,
                "last_venue_event_at": cp.last_venue_event_at,
                "open_command_ids": cp.open_command_ids,
                "seen_exec_ids": cp.seen_exec_ids,
                "checkpoint_at": cp.checkpoint_at,
                "session_id": cp.session_id,
                "operator_public_id": cp.operator_public_id,
            }
            return row

    async def get_all_checkpoints(self, as_of: datetime) -> list[TradeProjectionCheckpointRow]:
        """Return all active checkpoints for recovery."""
        async with self.session() as s:
            result = await s.execute(
                select(TradeProjectionCheckpoint)
                .where(*where_active(TradeProjectionCheckpoint, as_of))
                .order_by(TradeProjectionCheckpoint.shard_key)
            )
            rows: list[TradeProjectionCheckpointRow] = []
            for cp in result.scalars().all():
                rows.append(
                    {
                        "public_id": cp.public_id,
                        "shard_key": cp.shard_key,
                        "position_qty": cp.position_qty,
                        "entry_price": cp.entry_price,
                        "position_opened_at": cp.position_opened_at,
                        "cash": cp.cash,
                        "peak_equity": cp.peak_equity,
                        "realized_pnl": cp.realized_pnl,
                        "turnover": cp.turnover,
                        "last_venue_event_id": cp.last_venue_event_id,
                        "last_venue_event_at": cp.last_venue_event_at,
                        "open_command_ids": cp.open_command_ids,
                        "seen_exec_ids": cp.seen_exec_ids,
                        "checkpoint_at": cp.checkpoint_at,
                        "session_id": cp.session_id,
                        "operator_public_id": cp.operator_public_id,
                    }
                )
            return rows

    async def insert_funding_rate(
        self,
        row: FundingRateInsertRow,
        session: AsyncSession | None = None,
    ) -> int:
        """Insert a funding rate row, optionally on a caller-managed session.

        See ``Repository.insert_funding_rate`` for the contract. When
        the caller passes its own ``session``, the insert is wrapped in
        a ``begin_nested()`` SAVEPOINT so a duplicate ``IntegrityError``
        from the partial unique index only rolls back the savepoint —
        the outer transaction stays usable and the caller can swallow
        the duplicate as a "boundary already applied" no-op.
        """
        if session is None:
            async with self.session() as s:
                obj = FundingRate(**row)
                s.add(obj)
                await s.commit()
                await s.refresh(obj)
                return int(obj.id)
        obj = FundingRate(**row)
        async with session.begin_nested():
            session.add(obj)
            await session.flush()
        return int(obj.id)

    async def get_funding_rates(
        self,
        instrument_public_id: str,
        exchange: str,
        rate_type: str,
        direction: str,
        as_of: datetime,
        range_start: datetime | None = None,
        range_end: datetime | None = None,
    ) -> list[FundingRateRow]:
        """Bitemporal lookup of funding rates with explicit time window."""
        async with self.session() as s:
            stmt = (
                select(FundingRate)
                .where(
                    FundingRate.instrument_public_id == instrument_public_id,
                    FundingRate.exchange == exchange,
                    FundingRate.rate_type == rate_type,
                    FundingRate.direction == direction,
                    *where_active(FundingRate, as_of),
                )
                .order_by(FundingRate.effective_from)
            )
            if range_start is not None:
                stmt = stmt.where(FundingRate.effective_from >= range_start)
            if range_end is not None:
                stmt = stmt.where(FundingRate.effective_from <= range_end)
            result = await s.execute(stmt)
            return [
                FundingRateRow(
                    public_id=fr.public_id,
                    instrument_public_id=fr.instrument_public_id,
                    exchange=fr.exchange,
                    rate_type=fr.rate_type,
                    direction=fr.direction,
                    rate=fr.rate,
                    notional_asset=fr.notional_asset,
                    effective_from=fr.effective_from,
                    source=fr.source,
                    timestamp=fr.timestamp,
                    session_id=fr.session_id,
                    sequence_id=fr.sequence_id,
                )
                for fr in result.scalars().all()
            ]

    async def insert_accrual(
        self,
        row: AccrualLedgerInsertRow,
        session: AsyncSession | None = None,
    ) -> int:
        """Insert an accrual ledger row, optionally on a caller-managed session.

        See ``Repository.insert_accrual`` for the contract. When the
        caller passes its own ``session``, the insert is wrapped in a
        ``begin_nested()`` SAVEPOINT so a duplicate ``IntegrityError``
        from the partial unique index only rolls back the savepoint —
        the outer transaction stays usable and the caller can swallow
        the duplicate as a "boundary already applied" no-op without
        losing earlier writes from the same outer transaction.
        """
        row_with_defaults: dict[str, Any] = {"wallet_public_id": "", **row}
        if session is None:
            async with self.session() as s:
                obj = AccrualLedger(**row_with_defaults)
                s.add(obj)
                await s.commit()
                await s.refresh(obj)
                return int(obj.id)
        obj = AccrualLedger(**row_with_defaults)
        async with session.begin_nested():
            session.add(obj)
            await session.flush()
        return int(obj.id)

    async def get_accruals(
        self,
        instrument_public_id: str,
        mode: str,
        range_start: datetime,
        range_end: datetime,
        wallet_public_id: str = "",
    ) -> list[AccrualLedgerRow]:
        """Replay-window query keyed on insertion timestamp, not accrued_at."""
        async with self.session() as s:
            now = datetime.now(UTC)
            filters = [
                AccrualLedger.instrument_public_id == instrument_public_id,
                AccrualLedger.mode == mode,
                AccrualLedger.timestamp > range_start,
                AccrualLedger.timestamp <= range_end,
                *where_active(AccrualLedger, now),
            ]
            if wallet_public_id:
                filters.append(AccrualLedger.wallet_public_id == wallet_public_id)
            stmt = select(AccrualLedger).where(*filters).order_by(AccrualLedger.accrued_at)
            result = await s.execute(stmt)
            return [self._accrual_row_to_dict(al) for al in result.scalars().all()]

    async def get_last_accrual(
        self,
        instrument_public_id: str,
        mode: str,
        accrual_type: str,
        wallet_public_id: str = "",
    ) -> AccrualLedgerRow | None:
        """Return the most recent accrual for catch-up boundary computation."""
        async with self.session() as s:
            now = datetime.now(UTC)
            filters = [
                AccrualLedger.instrument_public_id == instrument_public_id,
                AccrualLedger.mode == mode,
                AccrualLedger.accrual_type == accrual_type,
                *where_active(AccrualLedger, now),
            ]
            if wallet_public_id:
                filters.append(AccrualLedger.wallet_public_id == wallet_public_id)
            stmt = (
                select(AccrualLedger)
                .where(*filters)
                .order_by(AccrualLedger.accrued_at.desc())
                .limit(1)
            )
            result = await s.execute(stmt)
            row = result.scalars().first()
            if row is None:
                return None
            return self._accrual_row_to_dict(row)

    @staticmethod
    def _accrual_row_to_dict(al: AccrualLedger) -> AccrualLedgerRow:
        """Project an AccrualLedger ORM row into the TypedDict shape."""
        return AccrualLedgerRow(
            public_id=al.public_id,
            instrument_public_id=al.instrument_public_id,
            mode=al.mode,
            accrual_type=al.accrual_type,
            accrued_at=al.accrued_at,
            amount=al.amount,
            amount_asset=al.amount_asset,
            rate=al.rate,
            notional=al.notional,
            position_quantity_at_accrual=al.position_quantity_at_accrual,
            exchange=al.exchange,
            timestamp=al.timestamp,
            session_id=al.session_id,
            sequence_id=al.sequence_id,
        )

    async def get_fill_exec_ids_for_shard(self, shard_key: str) -> set[str]:
        """Return all exec_id and trade_id values from fill events for a shard.

        Used to rebuild the fill dedup set during recovery. Queries all
        VenueEvent rows with event_type='fill_observed' for the shard.

        Args:
            shard_key: Shard to query.

        Returns:
            Set of exec_id and trade_id strings (non-null values only).
        """
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent.exec_id, VenueEvent.trade_id).where(
                    VenueEvent.shard_key == shard_key,
                    VenueEvent.event_type == "fill_observed",
                )
            )
            ids: set[str] = set()
            for row in result.all():
                if row[0]:
                    ids.add(row[0])
                if row[1]:
                    ids.add(row[1])
            return ids

    async def get_latest_venue_event_id(self, shard_key: str) -> int | None:
        """Return the highest VenueEvent.id for a shard, or None if empty.

        Args:
            shard_key: Shard to query.

        Returns:
            Highest id value, or None if no events exist.
        """
        async with self.session() as s:
            result = await s.execute(
                select(func.max(VenueEvent.id)).where(VenueEvent.shard_key == shard_key)
            )
            return result.scalar()

    async def get_underlying_assets(
        self,
        as_of: datetime,
    ) -> list[UnderlyingAssetRow]:
        """All active underlying assets at as_of with instrument counts."""
        async with self.session() as s:
            mapping_count = (
                select(
                    InstrumentUnderlyingMapping.underlying_public_id,
                    func.count().label("cnt"),
                )
                .join(
                    Instrument,
                    and_(
                        Instrument.public_id == InstrumentUnderlyingMapping.instrument_public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Symbol.public_id == Instrument.symbol_public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(*where_active(InstrumentUnderlyingMapping, as_of))
                .group_by(InstrumentUnderlyingMapping.underlying_public_id)
                .subquery()
            )
            stmt = (
                select(
                    UnderlyingAsset.public_id,
                    UnderlyingAsset.ticker,
                    UnderlyingAsset.name,
                    UnderlyingAsset.asset_class,
                    UnderlyingAsset.sector,
                    UnderlyingAsset.description,
                    UnderlyingAsset.timestamp,
                    UnderlyingAsset.session_id,
                    UnderlyingAsset.sequence_id,
                    func.coalesce(mapping_count.c.cnt, 0).label("instrument_count"),
                )
                .outerjoin(
                    mapping_count,
                    mapping_count.c.underlying_public_id == UnderlyingAsset.public_id,
                )
                .where(*where_active(UnderlyingAsset, as_of))
                .order_by(UnderlyingAsset.ticker)
            )
            result = await s.execute(stmt)
            return [
                UnderlyingAssetRow(
                    public_id=r.public_id,
                    ticker=r.ticker,
                    name=r.name,
                    asset_class=r.asset_class,
                    sector=r.sector,
                    description=r.description,
                    timestamp=r.timestamp,
                    session_id=r.session_id,
                    sequence_id=r.sequence_id,
                    instrument_count=r.instrument_count,
                )
                for r in result.all()
            ]

    async def get_underlying_by_ticker(
        self,
        ticker: str,
        as_of: datetime,
    ) -> UnderlyingAssetRow | None:
        """Lookup underlying by ticker."""
        async with self.session() as s:
            count_sq = (
                select(func.count())
                .select_from(InstrumentUnderlyingMapping)
                .join(
                    Instrument,
                    and_(
                        Instrument.public_id == InstrumentUnderlyingMapping.instrument_public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Symbol.public_id == Instrument.symbol_public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(
                    InstrumentUnderlyingMapping.underlying_public_id == UnderlyingAsset.public_id,
                    *where_active(InstrumentUnderlyingMapping, as_of),
                )
                .correlate(UnderlyingAsset)
                .scalar_subquery()
                .label("instrument_count")
            )
            result = await s.execute(
                select(
                    UnderlyingAsset.public_id,
                    UnderlyingAsset.ticker,
                    UnderlyingAsset.name,
                    UnderlyingAsset.asset_class,
                    UnderlyingAsset.sector,
                    UnderlyingAsset.description,
                    UnderlyingAsset.timestamp,
                    UnderlyingAsset.session_id,
                    UnderlyingAsset.sequence_id,
                    count_sq,
                ).where(
                    UnderlyingAsset.ticker == ticker,
                    *where_active(UnderlyingAsset, as_of),
                )
            )
            r = result.first()
            if r is None:
                return None
            return UnderlyingAssetRow(
                public_id=r.public_id,
                ticker=r.ticker,
                name=r.name,
                asset_class=r.asset_class,
                sector=r.sector,
                description=r.description,
                timestamp=r.timestamp,
                session_id=r.session_id,
                sequence_id=r.sequence_id,
                instrument_count=r.instrument_count,
            )

    async def get_instruments_by_underlying(
        self,
        underlying_public_id: str,
        as_of: datetime,
        relationship_types: list[str] | None = None,
    ) -> list[InstrumentUnderlyingRow]:
        """Instruments mapped to an underlying asset."""
        async with self.session() as s:
            stmt = (
                select(
                    InstrumentUnderlyingMapping.public_id,
                    InstrumentUnderlyingMapping.instrument_public_id,
                    InstrumentUnderlyingMapping.underlying_public_id,
                    InstrumentUnderlyingMapping.relationship_type,
                    InstrumentUnderlyingMapping.contract_family,
                    Symbol.native_symbol.label("native_symbol"),
                    Instrument.exchange,
                    Symbol.asset_type,
                    InstrumentUnderlyingMapping.timestamp,
                    InstrumentUnderlyingMapping.session_id,
                    InstrumentUnderlyingMapping.sequence_id,
                )
                .join(
                    Instrument,
                    and_(
                        Instrument.public_id == InstrumentUnderlyingMapping.instrument_public_id,
                        *where_active(Instrument, as_of),
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Symbol.public_id == Instrument.symbol_public_id,
                        *where_active(Symbol, as_of),
                    ),
                )
                .where(
                    InstrumentUnderlyingMapping.underlying_public_id == underlying_public_id,
                    *where_active(InstrumentUnderlyingMapping, as_of),
                )
                .order_by(Instrument.exchange, Symbol.native_symbol)
            )
            if relationship_types:
                stmt = stmt.where(
                    InstrumentUnderlyingMapping.relationship_type.in_(relationship_types)
                )
            result = await s.execute(stmt)
            return [
                InstrumentUnderlyingRow(
                    public_id=r.public_id,
                    instrument_public_id=r.instrument_public_id,
                    underlying_public_id=r.underlying_public_id,
                    relationship_type=r.relationship_type,
                    contract_family=r.contract_family,
                    native_symbol=r.native_symbol,
                    exchange=r.exchange,
                    asset_type=r.asset_type,
                    timestamp=r.timestamp,
                    session_id=r.session_id,
                    sequence_id=r.sequence_id,
                )
                for r in result.all()
            ]

    async def get_underlying_for_instrument(
        self,
        instrument_public_id: str,
        as_of: datetime,
    ) -> UnderlyingAssetRow | None:
        """Reverse lookup: which underlying does this instrument belong to?"""
        async with self.session() as s:
            result = await s.execute(
                select(
                    UnderlyingAsset.public_id,
                    UnderlyingAsset.ticker,
                    UnderlyingAsset.name,
                    UnderlyingAsset.asset_class,
                    UnderlyingAsset.sector,
                    UnderlyingAsset.description,
                    UnderlyingAsset.timestamp,
                    UnderlyingAsset.session_id,
                    UnderlyingAsset.sequence_id,
                )
                .join(
                    InstrumentUnderlyingMapping,
                    and_(
                        InstrumentUnderlyingMapping.underlying_public_id
                        == UnderlyingAsset.public_id,
                        *where_active(InstrumentUnderlyingMapping, as_of),
                    ),
                )
                .where(
                    InstrumentUnderlyingMapping.instrument_public_id == instrument_public_id,
                    *where_active(UnderlyingAsset, as_of),
                )
            )
            r = result.first()
            if r is None:
                return None
            return UnderlyingAssetRow(
                public_id=r.public_id,
                ticker=r.ticker,
                name=r.name,
                asset_class=r.asset_class,
                sector=r.sector,
                description=r.description,
                timestamp=r.timestamp,
                session_id=r.session_id,
                sequence_id=r.sequence_id,
                instrument_count=0,
            )

    async def upsert_underlying_asset(
        self,
        ticker: str,
        name: str,
        asset_class: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        sector: str | None = None,
        description: str | None = None,
    ) -> tuple[str, str]:
        """SCD2 upsert for an underlying asset."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(UnderlyingAsset)
                        .where(
                            UnderlyingAsset.ticker == ticker,
                            *where_active(UnderlyingAsset, timestamp),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )

            if existing is not None:
                changed = (
                    existing.name != name
                    or existing.asset_class != asset_class
                    or existing.sector != sector
                    or existing.description != description
                )
                if not changed:
                    return existing.public_id, "unchanged"

                await s.execute(
                    update(UnderlyingAsset)
                    .where(UnderlyingAsset.id == existing.id)
                    .values(known_to=timestamp)
                )
                new_row = UnderlyingAsset(
                    public_id=existing.public_id,
                    ticker=ticker,
                    name=name,
                    asset_class=asset_class,
                    sector=sector,
                    description=description,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=timestamp,
                    known_to=KNOWN_TO_MAX,
                )
                s.add(new_row)
                await s.commit()
                return existing.public_id, "updated"

            new_row = UnderlyingAsset(
                ticker=ticker,
                name=name,
                asset_class=asset_class,
                sector=sector,
                description=description,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(new_row)
            await s.commit()
            return new_row.public_id, "created"

    async def upsert_instrument_underlying_mapping(
        self,
        instrument_public_id: str,
        underlying_public_id: str,
        relationship_type: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        contract_family: str | None = None,
    ) -> str:
        """SCD2 upsert for an instrument-underlying mapping."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(InstrumentUnderlyingMapping)
                        .where(
                            InstrumentUnderlyingMapping.instrument_public_id
                            == instrument_public_id,
                            *where_active(InstrumentUnderlyingMapping, timestamp),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )

            if existing is not None:
                changed = (
                    existing.underlying_public_id != underlying_public_id
                    or existing.relationship_type != relationship_type
                    or existing.contract_family != contract_family
                )
                if not changed:
                    return "unchanged"

                await s.execute(
                    update(InstrumentUnderlyingMapping)
                    .where(InstrumentUnderlyingMapping.id == existing.id)
                    .values(known_to=timestamp)
                )
                new_row = InstrumentUnderlyingMapping(
                    public_id=existing.public_id,
                    instrument_public_id=instrument_public_id,
                    underlying_public_id=underlying_public_id,
                    relationship_type=relationship_type,
                    contract_family=contract_family,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=timestamp,
                    known_to=KNOWN_TO_MAX,
                )
                s.add(new_row)
                await s.commit()
                return "updated"

            new_row = InstrumentUnderlyingMapping(
                instrument_public_id=instrument_public_id,
                underlying_public_id=underlying_public_id,
                relationship_type=relationship_type,
                contract_family=contract_family,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(new_row)
            await s.commit()
            return "created"

    async def close_instrument_underlying_mapping(
        self,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> bool:
        """Close active mapping for instrument."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(InstrumentUnderlyingMapping)
                        .where(
                            InstrumentUnderlyingMapping.instrument_public_id
                            == instrument_public_id,
                            *where_active(InstrumentUnderlyingMapping, timestamp),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return False
            await s.execute(
                update(InstrumentUnderlyingMapping)
                .where(InstrumentUnderlyingMapping.id == existing.id)
                .values(known_to=timestamp)
            )
            await s.commit()
            return True

    async def close_underlying_asset(
        self,
        public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> bool:
        """Close active underlying asset by public_id."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(UnderlyingAsset)
                        .where(
                            UnderlyingAsset.public_id == public_id,
                            *where_active(UnderlyingAsset, timestamp),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return False
            await s.execute(
                update(UnderlyingAsset)
                .where(UnderlyingAsset.id == existing.id)
                .values(known_to=timestamp)
            )
            await s.commit()
            return True

    async def get_front_month_instrument(
        self,
        underlying_public_id: str,
        as_of: datetime,
        exchange: str | None = None,
        contract_family: str | None = None,
    ) -> InstrumentFrontMonthRow | None:
        """Return the nearest non-expired futures contract for an underlying."""
        async with self.session() as s:
            mapping_ts, mapping_kt = where_active(InstrumentUnderlyingMapping, as_of)
            inst_ts, inst_kt = where_active(Instrument, as_of)
            spec_ts, spec_kt = where_active(InstrumentSpec, as_of)
            sym_ts, sym_kt = where_active(Symbol, as_of)

            stmt = (
                select(
                    Instrument.public_id.label("instrument_public_id"),
                    Symbol.native_symbol,
                    Instrument.exchange,
                    InstrumentSpec.expiry_at,
                    InstrumentUnderlyingMapping.relationship_type,
                    InstrumentUnderlyingMapping.contract_family,
                )
                .join(
                    InstrumentUnderlyingMapping,
                    and_(
                        InstrumentUnderlyingMapping.instrument_public_id == Instrument.public_id,
                        mapping_ts,
                        mapping_kt,
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Symbol.public_id == Instrument.symbol_public_id,
                        sym_ts,
                        sym_kt,
                    ),
                )
                .join(
                    InstrumentSpec,
                    and_(
                        InstrumentSpec.instrument_public_id == Instrument.public_id,
                        spec_ts,
                        spec_kt,
                    ),
                )
                .where(
                    inst_ts,
                    inst_kt,
                    InstrumentUnderlyingMapping.underlying_public_id == underlying_public_id,
                    InstrumentUnderlyingMapping.relationship_type == "derivative",
                    InstrumentSpec.instrument_kind == "future",
                    InstrumentSpec.expiry_at > as_of,
                )
                .order_by(
                    InstrumentSpec.expiry_at.asc(),
                    InstrumentUnderlyingMapping.contract_family.asc(),
                    Instrument.exchange.asc(),
                    Symbol.native_symbol.asc(),
                )
                .limit(1)
            )

            if exchange is not None:
                stmt = stmt.where(Instrument.exchange == exchange)
            if contract_family is not None:
                stmt = stmt.where(InstrumentUnderlyingMapping.contract_family == contract_family)

            row = (await s.execute(stmt)).first()
            if row is None:
                return None
            return InstrumentFrontMonthRow(
                instrument_public_id=row.instrument_public_id,
                native_symbol=row.native_symbol,
                exchange=row.exchange,
                expiry_at=row.expiry_at,
                relationship_type=row.relationship_type,
                contract_family=row.contract_family,
            )

    async def get_contracts_for_underlying(
        self,
        underlying_public_id: str,
        as_of: datetime,
        exchange: str | None = None,
        contract_family: str | None = None,
        include_expired: bool = False,
    ) -> list[InstrumentContractRow]:
        """Return all futures contracts for an underlying."""
        async with self.session() as s:
            mapping_ts, mapping_kt = where_active(InstrumentUnderlyingMapping, as_of)
            inst_ts, inst_kt = where_active(Instrument, as_of)
            spec_ts, spec_kt = where_active(InstrumentSpec, as_of)
            sym_ts, sym_kt = where_active(Symbol, as_of)

            stmt = (
                select(
                    Instrument.public_id.label("instrument_public_id"),
                    Symbol.native_symbol,
                    Instrument.exchange,
                    InstrumentSpec.expiry_at,
                    InstrumentSpec.instrument_kind,
                    InstrumentUnderlyingMapping.relationship_type,
                    InstrumentUnderlyingMapping.contract_family,
                )
                .join(
                    InstrumentUnderlyingMapping,
                    and_(
                        InstrumentUnderlyingMapping.instrument_public_id == Instrument.public_id,
                        mapping_ts,
                        mapping_kt,
                    ),
                )
                .join(
                    Symbol,
                    and_(
                        Symbol.public_id == Instrument.symbol_public_id,
                        sym_ts,
                        sym_kt,
                    ),
                )
                .join(
                    InstrumentSpec,
                    and_(
                        InstrumentSpec.instrument_public_id == Instrument.public_id,
                        spec_ts,
                        spec_kt,
                    ),
                )
                .where(
                    inst_ts,
                    inst_kt,
                    InstrumentUnderlyingMapping.underlying_public_id == underlying_public_id,
                    InstrumentUnderlyingMapping.relationship_type == "derivative",
                    InstrumentSpec.instrument_kind == "future",
                )
                .order_by(
                    InstrumentUnderlyingMapping.contract_family.asc(),
                    InstrumentSpec.expiry_at.asc(),
                    Instrument.exchange.asc(),
                    Symbol.native_symbol.asc(),
                )
            )

            if not include_expired:
                stmt = stmt.where(InstrumentSpec.expiry_at > as_of)
            if exchange is not None:
                stmt = stmt.where(Instrument.exchange == exchange)
            if contract_family is not None:
                stmt = stmt.where(InstrumentUnderlyingMapping.contract_family == contract_family)

            rows = (await s.execute(stmt)).all()

            front_months: dict[str | None, str] = {}
            for r in rows:
                family = r.contract_family
                if (
                    family not in front_months
                    and r.instrument_kind == "future"
                    and r.expiry_at is not None
                    and r.expiry_at > as_of
                ):
                    front_months[family] = r.instrument_public_id

            return [
                InstrumentContractRow(
                    instrument_public_id=r.instrument_public_id,
                    native_symbol=r.native_symbol,
                    exchange=r.exchange,
                    expiry_at=r.expiry_at,
                    instrument_kind=r.instrument_kind,
                    relationship_type=r.relationship_type,
                    contract_family=r.contract_family,
                    is_front_month=front_months.get(r.contract_family) == r.instrument_public_id,
                )
                for r in rows
            ]

    async def _acquire_wallet_advisory_lock(
        self,
        s: AsyncSession,
        wallet_public_id: str,
    ) -> None:
        """Transaction-scoped advisory lock keyed by ``hashtext(wallet_public_id)``.

        Enforcement layer 1 for instrument-exclusive grants. Required
        because PostgreSQL
        ``SELECT ... FOR UPDATE`` on zero rows locks nothing, allowing two
        concurrent inserts to both pass overlap checks. SQLite is single-writer
        so no extra lock is needed (BEGIN already serializes writers).
        """
        dialect = self.dialect_name
        if dialect == "postgresql":
            await s.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:wid))"),
                {"wid": wallet_public_id},
            )
        elif dialect == "sqlite":
            return
        else:
            raise NotImplementedError(f"wallet advisory lock not implemented for dialect={dialect}")

    @staticmethod
    def _row_from_grant(grant: WalletOperatorScopeGrant) -> ScopeGrantRow:
        return ScopeGrantRow(
            public_id=grant.public_id,
            operator_public_id=grant.operator_public_id,
            wallet_public_id=grant.wallet_public_id,
            granted_by_user_public_id=grant.granted_by_user_public_id,
            scope_kind=grant.scope_kind,
            underlying_public_id=grant.underlying_public_id,
            instrument_public_id=grant.instrument_public_id,
            note=grant.note,
            timestamp=grant.timestamp,
            known_to=grant.known_to,
            session_id=grant.session_id,
            sequence_id=grant.sequence_id,
        )

    async def _load_active_grants_for_wallet(
        self,
        s: AsyncSession,
        wallet_public_id: str,
        as_of: datetime,
    ) -> list[WalletOperatorScopeGrant]:
        result = await s.execute(
            select(WalletOperatorScopeGrant)
            .where(
                WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                *where_active(WalletOperatorScopeGrant, as_of),
            )
            .order_by(WalletOperatorScopeGrant.timestamp.asc())
        )
        return list(result.scalars().all())

    async def _expand_to_instruments(
        self,
        s: AsyncSession,
        scope_kind: str,
        underlying_public_id: str | None,
        instrument_public_id: str | None,
        as_of: datetime,
    ) -> set[str]:
        """Resolve a scope reference to its concrete instrument set.

        For ``scope_kind == "instrument"`` this is the singleton set
        ``{instrument_public_id}``. For ``scope_kind == "underlying"`` this
        queries ``instrument_underlying_mappings`` (active at ``as_of``) and
        returns every instrument currently linked to the underlying.
        Underlying-scope grants are dynamic — newly added mappings
        expand the grant transparently. Callers MUST have already
        validated the XOR invariant (either via ``_validate_scope_xor``
        for requests or via the DB ``ck_scope_grants_scope_kind_xor``
        CHECK constraint for grants loaded from the database).
        """
        if scope_kind == "instrument":
            return {cast(str, instrument_public_id)}
        result = await s.execute(
            select(InstrumentUnderlyingMapping.instrument_public_id).where(
                InstrumentUnderlyingMapping.underlying_public_id == cast(str, underlying_public_id),
                *where_active(InstrumentUnderlyingMapping, as_of),
            )
        )
        return {row[0] for row in result.all()}

    async def _find_overlap(
        self,
        s: AsyncSession,
        existing: list[WalletOperatorScopeGrant],
        request: CreateScopeGrantRequest,
        as_of: datetime,
    ) -> WalletOperatorScopeGrant | None:
        """Cross-scope overlap detection.

        Returns the first existing grant that overlaps the request, or
        ``None`` when no conflict is found. Two matching rules apply:

        1. **Same-scope identity fast path.** If an existing grant points at
           the same ``scope_kind`` + same ``underlying_public_id`` /
           ``instrument_public_id`` as the request, it conflicts regardless
           of any instrument-mapping expansion. This covers the edge case
           where an underlying currently has zero active instrument mappings
           (both expansion sets empty → empty intersection → would otherwise
           fall through to a raw partial-unique-index IntegrityError at
           insert time).

        2. **Expanded-set intersection.** Otherwise expand both sides to
           instrument sets via ``_expand_to_instruments`` (which honors the
           dynamic-scope rule by querying ``instrument_underlying_mappings``
           ACTIVE at ``as_of``) and report the first grant whose expansion
           intersects the request's.
        """
        req_kind = request["scope_kind"]
        req_underlying = request.get("underlying_public_id")
        req_instrument = request.get("instrument_public_id")
        for grant in existing:
            if (
                grant.scope_kind == req_kind
                and grant.underlying_public_id == req_underlying
                and grant.instrument_public_id == req_instrument
            ):
                return grant

        target = await self._expand_to_instruments(
            s,
            req_kind,
            req_underlying,
            req_instrument,
            as_of,
        )
        for grant in existing:
            grant_set = await self._expand_to_instruments(
                s,
                grant.scope_kind,
                grant.underlying_public_id,
                grant.instrument_public_id,
                as_of,
            )
            if target & grant_set:
                return grant
        return None

    @staticmethod
    def _validate_scope_xor(request: CreateScopeGrantRequest) -> None:
        kind = request["scope_kind"]
        underlying = request.get("underlying_public_id")
        instrument = request.get("instrument_public_id")
        if kind == "underlying":
            if underlying is None or instrument is not None:
                raise ScopeGrantValidationError(
                    "scope_kind='underlying' requires underlying_public_id and "
                    "no instrument_public_id"
                )
        elif kind == "instrument":
            if instrument is None or underlying is not None:
                raise ScopeGrantValidationError(
                    "scope_kind='instrument' requires instrument_public_id and "
                    "no underlying_public_id"
                )
        else:
            raise ScopeGrantValidationError(f"unknown scope_kind={kind!r}")

    async def get_instrument_capabilities(
        self,
        as_of: datetime,
        exchange: str | None = None,
        instrument_public_id: str | None = None,
    ) -> list[InstrumentOrderCapabilityRow]:
        """Retrieve active instrument order capability rows."""
        async with self.session() as s:
            filters: list[Any] = [*where_active(InstrumentOrderCapability, as_of)]
            if exchange is not None:
                filters.append(InstrumentOrderCapability.exchange == exchange)
            if instrument_public_id is not None:
                filters.append(
                    InstrumentOrderCapability.instrument_public_id == instrument_public_id
                )
            stmt = (
                select(InstrumentOrderCapability)
                .where(*filters)
                .order_by(
                    InstrumentOrderCapability.exchange,
                    InstrumentOrderCapability.instrument_public_id,
                )
            )
            result = await s.execute(stmt)
            return [
                InstrumentOrderCapabilityRow(
                    public_id=row.public_id,
                    timestamp=row.timestamp,
                    session_id=row.session_id,
                    sequence_id=row.sequence_id,
                    instrument_public_id=row.instrument_public_id,
                    exchange=row.exchange,
                    supported_order_types=row.supported_order_types,
                    supports_post_only=row.supports_post_only,
                    supports_reduce_only=row.supports_reduce_only,
                    supports_amend_in_place=row.supports_amend_in_place,
                    supports_native_stop_loss=row.supports_native_stop_loss,
                    supports_native_take_profit=row.supports_native_take_profit,
                    supports_trailing_stop_client_side=row.supports_trailing_stop_client_side,
                    supports_market_making=row.supports_market_making,
                    supports_short_selling=row.supports_short_selling,
                    supports_leverage=row.supports_leverage,
                    max_leverage_long=row.max_leverage_long,
                    max_leverage_short=row.max_leverage_short,
                    min_notional=row.min_notional,
                    max_order_size=row.max_order_size,
                    top_of_book_quality=row.top_of_book_quality,
                )
                for row in result.scalars().all()
            ]

    async def get_venue_fee_schedules(
        self,
        as_of: datetime,
        exchange: str | None = None,
    ) -> list[VenueFeeScheduleRow]:
        """Retrieve active venue fee schedule rows."""
        async with self.session() as s:
            filters: list[Any] = [*where_active(VenueFeeSchedule, as_of)]
            if exchange is not None:
                filters.append(VenueFeeSchedule.exchange == exchange)
            stmt = (
                select(VenueFeeSchedule)
                .where(*filters)
                .order_by(VenueFeeSchedule.exchange, VenueFeeSchedule.fee_tier)
            )
            result = await s.execute(stmt)
            return [
                VenueFeeScheduleRow(
                    public_id=row.public_id,
                    timestamp=row.timestamp,
                    session_id=row.session_id,
                    sequence_id=row.sequence_id,
                    exchange=row.exchange,
                    instrument_public_id=row.instrument_public_id,
                    fee_tier=row.fee_tier,
                    maker_bps=row.maker_bps,
                    taker_bps=row.taker_bps,
                    min_volume_30d=row.min_volume_30d,
                    currency=row.currency,
                )
                for row in result.scalars().all()
            ]

    @staticmethod
    def _plan_row_to_dict(p: ExecutionPlan) -> ExecutionPlanRow:
        """Project an ExecutionPlan ORM row into the TypedDict shape."""
        return ExecutionPlanRow(
            public_id=p.public_id,
            timestamp=p.timestamp,
            session_id=p.session_id,
            sequence_id=p.sequence_id,
            plan_type=p.plan_type,
            created_by_user_id=p.created_by_user_id,
            created_by_strategy=p.created_by_strategy,
            created_via=p.created_via,
            instrument_public_id=p.instrument_public_id,
            exchange=p.exchange,
            mode=p.mode,
            shard_key=p.shard_key,
            wallet_public_id=p.wallet_public_id,
            operator_public_id=p.operator_public_id,
            total_quantity=p.total_quantity,
            filled_quantity=p.filled_quantity,
            side=p.side,
            parent_plan_public_id=p.parent_plan_public_id,
            position_cycle_public_id=p.position_cycle_public_id,
            params=p.params,
            status=p.status,
            created_at=p.created_at,
            started_at=p.started_at,
            completed_at=p.completed_at,
            expires_at=p.expires_at,
            cancel_requested_at=p.cancel_requested_at,
            last_evaluated_at=p.last_evaluated_at,
            last_error=p.last_error,
            idempotency_key=p.idempotency_key,
        )

    async def insert_execution_plan(
        self,
        row: ExecutionPlanInsertRow,
    ) -> tuple[int, str]:
        """Insert a new execution plan row."""
        async with self.session() as s:
            plan = ExecutionPlan(
                plan_type=row["plan_type"],
                created_by_user_id=row.get("created_by_user_id"),
                created_by_strategy=row.get("created_by_strategy"),
                created_via=row["created_via"],
                instrument_public_id=row["instrument_public_id"],
                exchange=row["exchange"],
                mode=row["mode"],
                shard_key=row["shard_key"],
                wallet_public_id=row["wallet_public_id"],
                operator_public_id=row.get("operator_public_id"),
                total_quantity=row["total_quantity"],
                side=row["side"],
                params=row["params"],
                status=row["status"],
                created_at=row["created_at"],
                parent_plan_public_id=row.get("parent_plan_public_id"),
                position_cycle_public_id=row.get("position_cycle_public_id"),
                expires_at=row.get("expires_at"),
                idempotency_key=row.get("idempotency_key"),
                session_id=row["session_id"],
                sequence_id=row["sequence_id"],
                timestamp=row["timestamp"],
            )
            s.add(plan)
            await s.commit()
            await s.refresh(plan)
            return plan.id, plan.public_id

    async def get_execution_plan(
        self,
        public_id: str,
        as_of: datetime,
    ) -> ExecutionPlanRow | None:
        """Retrieve a single execution plan by public_id."""
        async with self.session() as s:
            stmt = select(ExecutionPlan).where(
                ExecutionPlan.public_id == public_id,
                *where_active(ExecutionPlan, as_of),
            )
            result = await s.execute(stmt)
            row = result.scalars().first()
            if row is None:
                return None
            return self._plan_row_to_dict(row)

    async def get_execution_plans(
        self,
        as_of: datetime,
        status: str | None = None,
        exchange: str | None = None,
        mode: str | None = None,
        wallet_public_ids: list[str] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExecutionPlanRow]:
        """Retrieve execution plans with optional filters."""
        async with self.session() as s:
            filters: list[Any] = [*where_active(ExecutionPlan, as_of)]
            if status is not None:
                filters.append(ExecutionPlan.status == status)
            if exchange is not None:
                filters.append(ExecutionPlan.exchange == exchange)
            if mode is not None:
                filters.append(ExecutionPlan.mode == mode)
            if wallet_public_ids is not None:
                filters.append(ExecutionPlan.wallet_public_id.in_(wallet_public_ids))
            stmt = (
                select(ExecutionPlan)
                .where(*filters)
                .order_by(ExecutionPlan.created_at.desc())
                .limit(limit)
                .offset(offset)
            )
            result = await s.execute(stmt)
            return [self._plan_row_to_dict(p) for p in result.scalars().all()]

    async def update_execution_plan_status(
        self,
        public_id: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        filled_quantity: float | None = None,
        last_error: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
        cancel_requested_at: datetime | None = None,
        last_evaluated_at: datetime | None = None,
    ) -> int | None:
        """SCD2 close-and-insert for plan status transition."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(ExecutionPlan)
                        .where(
                            ExecutionPlan.public_id == public_id,
                            *where_active(ExecutionPlan, bus_time),
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
                update(ExecutionPlan)
                .where(ExecutionPlan.id == existing.id)
                .values(known_to=bus_time)
            )
            new_plan = ExecutionPlan(
                public_id=existing.public_id,
                plan_type=existing.plan_type,
                created_by_user_id=existing.created_by_user_id,
                created_by_strategy=existing.created_by_strategy,
                created_via=existing.created_via,
                instrument_public_id=existing.instrument_public_id,
                exchange=existing.exchange,
                mode=existing.mode,
                shard_key=existing.shard_key,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                total_quantity=existing.total_quantity,
                filled_quantity=(
                    filled_quantity if filled_quantity is not None else existing.filled_quantity
                ),
                side=existing.side,
                parent_plan_public_id=existing.parent_plan_public_id,
                position_cycle_public_id=existing.position_cycle_public_id,
                params=existing.params,
                status=new_status,
                created_at=existing.created_at,
                started_at=(started_at if started_at is not None else existing.started_at),
                completed_at=(completed_at if completed_at is not None else existing.completed_at),
                expires_at=existing.expires_at,
                cancel_requested_at=(
                    cancel_requested_at
                    if cancel_requested_at is not None
                    else existing.cancel_requested_at
                ),
                last_evaluated_at=(
                    last_evaluated_at
                    if last_evaluated_at is not None
                    else existing.last_evaluated_at
                ),
                last_error=last_error,
                idempotency_key=existing.idempotency_key,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_plan)
            await s.commit()
            await s.refresh(new_plan)
            return new_plan.id

    _ACTIONABLE_STATUSES = ("pending", "armed", "active", "paused", "cancel_requested")

    async def get_active_execution_plans(
        self,
    ) -> list[ExecutionPlanRow]:
        """Retrieve all plans with actionable status for executor startup."""
        async with self.session() as s:
            now = datetime.now(UTC)
            stmt = (
                select(ExecutionPlan)
                .where(
                    ExecutionPlan.status.in_(self._ACTIONABLE_STATUSES),
                    *where_active(ExecutionPlan, now),
                )
                .order_by(ExecutionPlan.created_at)
            )
            result = await s.execute(stmt)
            return [self._plan_row_to_dict(p) for p in result.scalars().all()]

    async def insert_execution_plan_checkpoint(
        self,
        plan_public_id: str,
        state: JsonObject,
        last_venue_event_id: int,
        checkpoint_at: datetime,
        session_id: str,
        sequence_id: int,
        bus_time: datetime,
        last_tick_timestamp: datetime | None = None,
    ) -> tuple[int, str]:
        """Insert a new checkpoint for a plan (SCD2 close previous)."""
        async with self.session() as s:
            prev = (
                (
                    await s.execute(
                        select(ExecutionPlanCheckpoint)
                        .where(
                            ExecutionPlanCheckpoint.plan_public_id == plan_public_id,
                            *where_active(ExecutionPlanCheckpoint, bus_time),
                        )
                        .order_by(ExecutionPlanCheckpoint.checkpoint_at.desc())
                        .limit(1)
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if prev is not None:
                await s.execute(
                    update(ExecutionPlanCheckpoint)
                    .where(ExecutionPlanCheckpoint.id == prev.id)
                    .values(known_to=bus_time)
                )
            cp = ExecutionPlanCheckpoint(
                plan_public_id=plan_public_id,
                state=state,
                last_venue_event_id=last_venue_event_id,
                last_tick_timestamp=last_tick_timestamp,
                checkpoint_at=checkpoint_at,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(cp)
            await s.commit()
            await s.refresh(cp)
            return cp.id, cp.public_id

    async def get_latest_plan_checkpoint(
        self,
        plan_public_id: str,
    ) -> ExecutionPlanCheckpointRow | None:
        """Return the most recent active checkpoint for a plan."""
        async with self.session() as s:
            now = datetime.now(UTC)
            stmt = (
                select(ExecutionPlanCheckpoint)
                .where(
                    ExecutionPlanCheckpoint.plan_public_id == plan_public_id,
                    *where_active(ExecutionPlanCheckpoint, now),
                )
                .order_by(ExecutionPlanCheckpoint.checkpoint_at.desc())
                .limit(1)
            )
            result = await s.execute(stmt)
            row = result.scalars().first()
            if row is None:
                return None
            return ExecutionPlanCheckpointRow(
                public_id=row.public_id,
                timestamp=row.timestamp,
                session_id=row.session_id,
                sequence_id=row.sequence_id,
                plan_public_id=row.plan_public_id,
                state=row.state,
                last_venue_event_id=row.last_venue_event_id,
                last_tick_timestamp=row.last_tick_timestamp,
                checkpoint_at=row.checkpoint_at,
            )

    async def insert_execution_plan_decision(
        self,
        row: ExecutionPlanDecisionInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str:
        """Insert a decision row for audit trail."""
        async with self.session() as s:
            decision = ExecutionPlanDecision(
                plan_public_id=row["plan_public_id"],
                decision_type=row["decision_type"],
                decided_at=row["decided_at"],
                trigger_type=row["trigger_type"],
                evidence=row["evidence"],
                emitted_command_public_id=row.get("emitted_command_public_id"),
                new_status=row.get("new_status"),
                reason=row["reason"],
                decision_importance=row["decision_importance"],
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(decision)
            await s.commit()
            await s.refresh(decision)
            return decision.public_id

    async def list_execution_plan_decisions(
        self,
        plan_public_id: str,
        as_of: datetime,
        importance: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExecutionPlanDecisionRow]:
        """Retrieve decision rows for a plan."""
        async with self.session() as s:
            filters: list[Any] = [
                ExecutionPlanDecision.plan_public_id == plan_public_id,
                *where_active(ExecutionPlanDecision, as_of),
            ]
            if importance is not None:
                filters.append(ExecutionPlanDecision.decision_importance == importance)
            stmt = (
                select(ExecutionPlanDecision)
                .where(*filters)
                .order_by(ExecutionPlanDecision.decided_at.desc())
                .limit(limit)
                .offset(offset)
            )
            result = await s.execute(stmt)
            return [
                ExecutionPlanDecisionRow(
                    public_id=d.public_id,
                    timestamp=d.timestamp,
                    session_id=d.session_id,
                    sequence_id=d.sequence_id,
                    plan_public_id=d.plan_public_id,
                    decision_type=d.decision_type,
                    decided_at=d.decided_at,
                    trigger_type=d.trigger_type,
                    evidence=d.evidence,
                    emitted_command_public_id=d.emitted_command_public_id,
                    new_status=d.new_status,
                    reason=d.reason,
                    decision_importance=d.decision_importance,
                )
                for d in result.scalars().all()
            ]

    async def revise_execution_plan_params(
        self,
        public_id: str,
        param_updates: JsonObject,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 close-and-insert for plan params revision only."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(ExecutionPlan)
                        .where(
                            ExecutionPlan.public_id == public_id,
                            *where_active(ExecutionPlan, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return
            await s.execute(
                update(ExecutionPlan)
                .where(ExecutionPlan.id == existing.id)
                .values(known_to=bus_time)
            )
            merged_params: JsonObject = {**existing.params, **param_updates}
            new_plan = ExecutionPlan(
                public_id=existing.public_id,
                plan_type=existing.plan_type,
                created_by_user_id=existing.created_by_user_id,
                created_by_strategy=existing.created_by_strategy,
                created_via=existing.created_via,
                instrument_public_id=existing.instrument_public_id,
                exchange=existing.exchange,
                mode=existing.mode,
                shard_key=existing.shard_key,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                total_quantity=existing.total_quantity,
                filled_quantity=existing.filled_quantity,
                side=existing.side,
                parent_plan_public_id=existing.parent_plan_public_id,
                position_cycle_public_id=existing.position_cycle_public_id,
                params=merged_params,
                status=existing.status,
                created_at=existing.created_at,
                started_at=existing.started_at,
                completed_at=existing.completed_at,
                expires_at=existing.expires_at,
                cancel_requested_at=existing.cancel_requested_at,
                last_evaluated_at=existing.last_evaluated_at,
                last_error=existing.last_error,
                idempotency_key=existing.idempotency_key,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_plan)
            await s.commit()

    @staticmethod
    def _position_cycle_row_to_dict(pc: PositionCycle) -> PositionCycleRow:
        """Project a PositionCycle ORM row into the TypedDict shape."""
        return PositionCycleRow(
            public_id=pc.public_id,
            timestamp=pc.timestamp,
            session_id=pc.session_id,
            sequence_id=pc.sequence_id,
            instrument_public_id=pc.instrument_public_id,
            exchange=pc.exchange,
            mode=pc.mode,
            shard_key=pc.shard_key,
            wallet_public_id=pc.wallet_public_id,
            operator_public_id=pc.operator_public_id,
            direction=pc.direction,
            max_qty=pc.max_qty,
            status=pc.status,
            opened_at=pc.opened_at,
            closed_at=pc.closed_at,
            opening_command_public_id=pc.opening_command_public_id,
            closing_command_public_id=pc.closing_command_public_id,
        )

    async def insert_position_cycle(
        self,
        row: PositionCycleInsertRow,
    ) -> tuple[int, str]:
        """Insert a new position_cycles row in the open state."""
        async with self.session() as s:
            cycle = PositionCycle(
                instrument_public_id=row["instrument_public_id"],
                exchange=row["exchange"],
                mode=row["mode"],
                shard_key=row["shard_key"],
                wallet_public_id=row["wallet_public_id"],
                operator_public_id=row.get("operator_public_id"),
                direction=row["direction"],
                max_qty=row["max_qty"],
                status=row["status"],
                opened_at=row["opened_at"],
                closed_at=row.get("closed_at"),
                opening_command_public_id=row.get("opening_command_public_id"),
                closing_command_public_id=row.get("closing_command_public_id"),
                session_id=row["session_id"],
                sequence_id=row["sequence_id"],
                timestamp=row["timestamp"],
            )
            s.add(cycle)
            await s.commit()
            await s.refresh(cycle)
            return cycle.id, cycle.public_id

    async def close_position_cycle(
        self,
        cycle_public_id: str,
        closed_at: datetime,
        closing_command_public_id: str | None,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int | None:
        """SCD2 close-and-insert transitioning an open cycle to closed."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(PositionCycle)
                        .where(
                            PositionCycle.public_id == cycle_public_id,
                            PositionCycle.status == "open",
                            *where_active(PositionCycle, bus_time),
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
                update(PositionCycle)
                .where(PositionCycle.id == existing.id)
                .values(known_to=bus_time)
            )
            new_row = PositionCycle(
                public_id=existing.public_id,
                instrument_public_id=existing.instrument_public_id,
                exchange=existing.exchange,
                mode=existing.mode,
                shard_key=existing.shard_key,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                direction=existing.direction,
                max_qty=existing.max_qty,
                status="closed",
                opened_at=existing.opened_at,
                closed_at=closed_at,
                opening_command_public_id=existing.opening_command_public_id,
                closing_command_public_id=closing_command_public_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_row)
            await s.commit()
            await s.refresh(new_row)
            return new_row.id

    async def get_open_position_cycle(
        self,
        shard_key: str,
        as_of: datetime,
    ) -> PositionCycleRow | None:
        """Return the active open cycle for a shard at a point in time."""
        async with self.session() as s:
            stmt = select(PositionCycle).where(
                PositionCycle.shard_key == shard_key,
                PositionCycle.status == "open",
                *where_active(PositionCycle, as_of),
            )
            result = await s.execute(stmt)
            row = result.scalars().first()
            if row is None:
                return None
            return self._position_cycle_row_to_dict(row)

    async def get_position_cycle_by_public_id(
        self,
        cycle_public_id: str,
        as_of: datetime,
    ) -> PositionCycleRow | None:
        """Retrieve a position cycle by its public_id."""
        async with self.session() as s:
            stmt = select(PositionCycle).where(
                PositionCycle.public_id == cycle_public_id,
                *where_active(PositionCycle, as_of),
            )
            result = await s.execute(stmt)
            row = result.scalars().first()
            if row is None:
                return None
            return self._position_cycle_row_to_dict(row)

    async def flip_position_cycle(
        self,
        close_cycle_public_id: str,
        new_open_row: PositionCycleInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> tuple[int, str]:
        """Atomically close one cycle and open another in a single transaction."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(PositionCycle)
                        .where(
                            PositionCycle.public_id == close_cycle_public_id,
                            PositionCycle.status == "open",
                            *where_active(PositionCycle, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                raise ValueError(
                    f"flip_position_cycle: no active open cycle found for "
                    f"public_id={close_cycle_public_id!r}"
                )
            new_shard_key = new_open_row["shard_key"]
            if existing.shard_key != new_shard_key:
                raise ValueError(
                    f"flip_position_cycle: shard_key mismatch "
                    f"existing={existing.shard_key!r} new={new_shard_key!r}"
                )
            await s.execute(
                update(PositionCycle)
                .where(PositionCycle.id == existing.id)
                .values(known_to=bus_time)
            )
            closed_row = PositionCycle(
                public_id=existing.public_id,
                instrument_public_id=existing.instrument_public_id,
                exchange=existing.exchange,
                mode=existing.mode,
                shard_key=existing.shard_key,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                direction=existing.direction,
                max_qty=existing.max_qty,
                status="closed",
                opened_at=existing.opened_at,
                closed_at=bus_time,
                opening_command_public_id=existing.opening_command_public_id,
                closing_command_public_id=new_open_row.get("opening_command_public_id"),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(closed_row)
            opened_row = PositionCycle(
                instrument_public_id=new_open_row["instrument_public_id"],
                exchange=new_open_row["exchange"],
                mode=new_open_row["mode"],
                shard_key=new_shard_key,
                wallet_public_id=new_open_row["wallet_public_id"],
                operator_public_id=new_open_row.get("operator_public_id"),
                direction=new_open_row["direction"],
                max_qty=new_open_row["max_qty"],
                status="open",
                opened_at=new_open_row["opened_at"],
                closed_at=None,
                opening_command_public_id=new_open_row.get("opening_command_public_id"),
                closing_command_public_id=None,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(opened_row)
            await s.commit()
            await s.refresh(opened_row)
            return opened_row.id, opened_row.public_id

    async def update_position_cycle_max_qty(
        self,
        cycle_public_id: str,
        new_max_qty: float,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int | None:
        """Monotonic SCD2 revision of a cycle's max_qty peak."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(PositionCycle)
                        .where(
                            PositionCycle.public_id == cycle_public_id,
                            PositionCycle.status == "open",
                            *where_active(PositionCycle, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                raise ValueError(
                    f"update_position_cycle_max_qty: no active open cycle found "
                    f"for public_id={cycle_public_id!r}"
                )
            if new_max_qty <= existing.max_qty:
                return None
            await s.execute(
                update(PositionCycle)
                .where(PositionCycle.id == existing.id)
                .values(known_to=bus_time)
            )
            new_row = PositionCycle(
                public_id=existing.public_id,
                instrument_public_id=existing.instrument_public_id,
                exchange=existing.exchange,
                mode=existing.mode,
                shard_key=existing.shard_key,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                direction=existing.direction,
                max_qty=new_max_qty,
                status=existing.status,
                opened_at=existing.opened_at,
                closed_at=existing.closed_at,
                opening_command_public_id=existing.opening_command_public_id,
                closing_command_public_id=existing.closing_command_public_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_row)
            await s.commit()
            await s.refresh(new_row)
            return new_row.id

    async def get_all_open_position_cycles(
        self,
        as_of: datetime,
        opened_before: datetime | None = None,
    ) -> list[PositionCycleRow]:
        """Return all open position cycles, optionally filtered by age."""
        async with self.session() as s:
            conditions = [
                PositionCycle.status == "open",
                *where_active(PositionCycle, as_of),
            ]
            if opened_before is not None:
                conditions.append(PositionCycle.opened_at < opened_before)
            rows = (await s.execute(select(PositionCycle).where(*conditions))).scalars().all()
            return [self._position_cycle_row_to_dict(r) for r in rows]

    async def create_scope_grant(self, request: CreateScopeGrantRequest) -> ScopeGrantRow:
        """Create a new scope grant with advisory-locked overlap detection."""
        self._validate_scope_xor(request)
        timestamp = request["timestamp"]
        async with self.session() as s:
            await self._acquire_wallet_advisory_lock(s, request["wallet_public_id"])
            existing = await self._load_active_grants_for_wallet(
                s, request["wallet_public_id"], timestamp
            )
            conflict = await self._find_overlap(s, existing, request, timestamp)
            if conflict is not None:
                raise ScopeGrantConflictError(
                    wallet_public_id=request["wallet_public_id"],
                    conflicting_grant_public_id=conflict.public_id,
                    conflicting_operator_public_id=conflict.operator_public_id,
                    reason=(
                        f"existing {conflict.scope_kind}-scoped grant overlaps the "
                        f"requested {request['scope_kind']} scope"
                    ),
                )
            new_grant = WalletOperatorScopeGrant(
                operator_public_id=request["operator_public_id"],
                wallet_public_id=request["wallet_public_id"],
                granted_by_user_public_id=request["granted_by_user_public_id"],
                scope_kind=request["scope_kind"],
                underlying_public_id=request.get("underlying_public_id"),
                instrument_public_id=request.get("instrument_public_id"),
                note=request.get("note"),
                session_id=request["session_id"],
                sequence_id=request["sequence_id"],
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(new_grant)
            try:
                await s.commit()
            except IntegrityError as exc:
                err_msg = str(exc.orig).lower() if exc.orig else ""
                if "unique" not in err_msg and "duplicate" not in err_msg:
                    raise
                raise ScopeGrantConflictError(
                    wallet_public_id=request["wallet_public_id"],
                    conflicting_grant_public_id="",
                    conflicting_operator_public_id=request["operator_public_id"],
                    reason=(
                        "partial unique index fired during insert — a concurrent "
                        "grant creation on the same scope already succeeded"
                    ),
                ) from exc
            await s.refresh(new_grant)
            return self._row_from_grant(new_grant)

    async def list_active_scope_grants_for_wallet(
        self,
        wallet_public_id: str,
        as_of: datetime,
    ) -> list[ScopeGrantRow]:
        """Return all currently active scope grants on the wallet."""
        async with self.session() as s:
            grants = await self._load_active_grants_for_wallet(s, wallet_public_id, as_of)
            return [self._row_from_grant(g) for g in grants]

    async def list_grant_covered_instrument_public_ids(
        self,
        operator_public_id: str,
        wallet_public_id: str,
        as_of: datetime,
    ) -> set[str]:
        """Return covered instrument set for an operator on a wallet.

        Expands underlying-scoped grants to their current instrument set
        per the dynamic-scope rule.

        Returns an empty set when the operator has no active grants on the
        wallet OR when the operator has only underlying-scoped grants whose
        underlyings currently have zero active instrument mappings.
        """
        async with self.session() as s:
            grants = await self._load_active_grants_for_wallet(s, wallet_public_id, as_of)
            covered: set[str] = set()
            for grant in grants:
                if grant.operator_public_id != operator_public_id:
                    continue
                expanded = await self._expand_to_instruments(
                    s,
                    scope_kind=grant.scope_kind,
                    underlying_public_id=grant.underlying_public_id,
                    instrument_public_id=grant.instrument_public_id,
                    as_of=as_of,
                )
                covered.update(expanded)
            return covered

    async def get_instrument_public_id_by_symbol(
        self,
        native_symbol: str,
        exchange: str,
        as_of: datetime,
    ) -> str | None:
        """Resolve a native symbol on a given exchange to its instrument public_id.

        Returns None when no active Instrument row exists for the
        ``(native_symbol, exchange)`` pair at ``as_of``. Used by the
        strategy permission check to map strategy ``outputs``
        (symbols) to the instrument set covered by an operator's grants.
        """
        async with self.session() as s:
            instrument = await self._resolve_active_instrument(
                s, native_symbol=native_symbol, exchange=exchange, as_of=as_of
            )
            return instrument.public_id if instrument is not None else None

    async def handover_grant(
        self,
        from_grant_public_id: str,
        to_operator_public_id: str,
        granted_by_user_public_id: str,
        reason: str | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> tuple[ScopeGrantRow, ScopeGrantRow]:
        """Atomic SCD2 close + insert handover."""
        async with self.session() as s:
            from_grant = (
                (
                    await s.execute(
                        select(WalletOperatorScopeGrant).where(
                            WalletOperatorScopeGrant.public_id == from_grant_public_id,
                            *where_active(WalletOperatorScopeGrant, timestamp),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if from_grant is None:
                raise ScopeGrantNotFoundError(
                    f"active scope grant {from_grant_public_id} not found at {timestamp.isoformat()}"
                )
            if from_grant.operator_public_id == to_operator_public_id:
                raise ScopeGrantValidationError(
                    f"handover target operator {to_operator_public_id} is already the holder"
                )

            await self._acquire_wallet_advisory_lock(s, from_grant.wallet_public_id)

            destination = (
                (
                    await s.execute(
                        select(Operator).where(
                            Operator.public_id == to_operator_public_id,
                            *where_active(Operator, timestamp),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if destination is None:
                raise ScopeGrantNotFoundError(
                    f"destination operator {to_operator_public_id} not found at {timestamp.isoformat()}"
                )

            await s.execute(
                update(WalletOperatorScopeGrant)
                .where(WalletOperatorScopeGrant.id == from_grant.id)
                .values(known_to=timestamp)
            )
            new_grant = WalletOperatorScopeGrant(
                operator_public_id=to_operator_public_id,
                wallet_public_id=from_grant.wallet_public_id,
                granted_by_user_public_id=granted_by_user_public_id,
                scope_kind=from_grant.scope_kind,
                underlying_public_id=from_grant.underlying_public_id,
                instrument_public_id=from_grant.instrument_public_id,
                note=reason,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(new_grant)
            try:
                await s.commit()
            except IntegrityError as exc:
                err_msg = str(exc.orig).lower() if exc.orig else ""
                if "unique" not in err_msg and "duplicate" not in err_msg:
                    raise
                raise ScopeGrantConflictError(
                    wallet_public_id=from_grant.wallet_public_id,
                    conflicting_grant_public_id=from_grant.public_id,
                    conflicting_operator_public_id=to_operator_public_id,
                    reason="concurrent handover detected by partial unique index",
                ) from exc
            await s.refresh(new_grant)

            closed_row = ScopeGrantRow(
                public_id=from_grant.public_id,
                operator_public_id=from_grant.operator_public_id,
                wallet_public_id=from_grant.wallet_public_id,
                granted_by_user_public_id=from_grant.granted_by_user_public_id,
                scope_kind=from_grant.scope_kind,
                underlying_public_id=from_grant.underlying_public_id,
                instrument_public_id=from_grant.instrument_public_id,
                note=from_grant.note,
                timestamp=from_grant.timestamp,
                known_to=timestamp,
                session_id=from_grant.session_id,
                sequence_id=from_grant.sequence_id,
            )
            return closed_row, self._row_from_grant(new_grant)

    async def list_active_operators(self, as_of: datetime) -> list[OperatorRow]:
        """Return every active operator at the given bus time."""
        async with self.session() as s:
            result = await s.execute(
                select(Operator)
                .where(*where_active(Operator, as_of))
                .order_by(Operator.label.asc())
            )
            return [
                OperatorRow(
                    public_id=row.public_id,
                    label=row.label,
                    description=row.description,
                    timestamp=row.timestamp,
                    session_id=row.session_id,
                    sequence_id=row.sequence_id,
                )
                for row in result.scalars().all()
            ]

    @staticmethod
    def _wallet_row_from(wallet: Wallet) -> WalletRow:
        """Project a ``Wallet`` ORM instance to its ``WalletRow`` TypedDict."""
        return WalletRow(
            public_id=wallet.public_id,
            label=wallet.label,
            description=wallet.description,
            is_paper=bool(wallet.is_paper),
            timestamp=wallet.timestamp,
            session_id=wallet.session_id,
            sequence_id=wallet.sequence_id,
        )

    async def create_wallet(
        self,
        label: str,
        description: str | None,
        is_paper: bool,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> WalletRow:
        """Insert a new active wallet row."""
        async with self.session() as s:
            wallet = Wallet(
                label=label,
                description=description,
                is_paper=is_paper,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(wallet)
            try:
                await s.commit()
            except IntegrityError as exc:
                err_msg = str(exc.orig).lower() if exc.orig else ""
                if "unique" not in err_msg and "duplicate" not in err_msg:
                    raise
                raise WalletConflictError(
                    label=label,
                    is_paper=is_paper,
                    reason="active wallet with the same (label, is_paper) already exists",
                ) from exc
            await s.refresh(wallet)
            return self._wallet_row_from(wallet)

    async def list_active_wallets(self, as_of: datetime) -> list[WalletRow]:
        """Return every active wallet at the given bus time."""
        async with self.session() as s:
            result = await s.execute(
                select(Wallet)
                .where(*where_active(Wallet, as_of))
                .order_by(Wallet.is_paper.asc(), Wallet.label.asc())
            )
            return [self._wallet_row_from(row) for row in result.scalars().all()]

    async def list_accessible_wallets_for_operators(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> list[WalletRow]:
        """Wallets covered by at least one active grant from the given operators."""
        if not operator_public_ids:
            return []
        async with self.session() as s:
            subquery = (
                select(WalletOperatorScopeGrant.wallet_public_id)
                .where(
                    WalletOperatorScopeGrant.operator_public_id.in_(operator_public_ids),
                    *where_active(WalletOperatorScopeGrant, as_of),
                )
                .distinct()
            )
            result = await s.execute(
                select(Wallet)
                .where(
                    Wallet.public_id.in_(subquery),
                    *where_active(Wallet, as_of),
                )
                .order_by(Wallet.is_paper.asc(), Wallet.label.asc())
            )
            return [self._wallet_row_from(row) for row in result.scalars().all()]

    async def get_user_operator_memberships(
        self,
        user_public_id: str,
        as_of: datetime,
    ) -> list[UserOperatorMembershipRow]:
        """Return active operator memberships for a user."""
        async with self.session() as s:
            result = await s.execute(
                select(UserOperatorMembership)
                .where(
                    UserOperatorMembership.user_public_id == user_public_id,
                    *where_active(UserOperatorMembership, as_of),
                )
                .order_by(UserOperatorMembership.timestamp.asc())
            )
            return [
                UserOperatorMembershipRow(
                    public_id=row.public_id,
                    user_public_id=row.user_public_id,
                    operator_public_id=row.operator_public_id,
                    is_primary=bool(row.is_primary),
                    timestamp=row.timestamp,
                    session_id=row.session_id,
                    sequence_id=row.sequence_id,
                )
                for row in result.scalars().all()
            ]

    async def get_active_credential(
        self,
        exchange: str,
        wallet_public_id: str,
        as_of: datetime,
    ) -> WalletCredentialRow | None:
        """Return the active wallet credential row or None."""
        async with self.session() as s:
            result = await s.execute(
                select(WalletCredential).where(
                    WalletCredential.exchange == exchange.lower(),
                    WalletCredential.wallet_public_id == wallet_public_id,
                    *where_active(WalletCredential, as_of),
                )
            )
            row = result.scalars().first()
            if row is None:
                return None
            return self._credential_row_from(row)

    async def list_active_wallet_credentials(
        self,
        as_of: datetime,
    ) -> list[WalletCredentialRow]:
        """Return all active wallet credentials at ``as_of``.

        Ordered by ``(exchange, wallet_public_id)`` for deterministic
        spawner behaviour across boots.
        """
        async with self.session() as s:
            result = await s.execute(
                select(WalletCredential)
                .where(*where_active(WalletCredential, as_of))
                .order_by(WalletCredential.exchange, WalletCredential.wallet_public_id)
            )
            return [self._credential_row_from(row) for row in result.scalars().all()]

    @staticmethod
    def _credential_row_from(cred: WalletCredential) -> WalletCredentialRow:
        """Project a ``WalletCredential`` ORM instance to its ``WalletCredentialRow``."""
        return WalletCredentialRow(
            public_id=cred.public_id,
            wallet_public_id=cred.wallet_public_id,
            exchange=cred.exchange,
            credential_type=cred.credential_type,
            encrypted_payload=cred.encrypted_payload,
            label=cred.label,
            timestamp=cred.timestamp,
            session_id=cred.session_id,
            sequence_id=cred.sequence_id,
        )

    async def get_active_credential_by_id(
        self,
        credential_public_id: str,
        as_of: datetime,
    ) -> WalletCredentialRow | None:
        """Return the active credential row by its ``public_id``."""
        async with self.session() as s:
            result = await s.execute(
                select(WalletCredential).where(
                    WalletCredential.public_id == credential_public_id,
                    *where_active(WalletCredential, as_of),
                )
            )
            row = result.scalars().first()
            if row is None:
                return None
            return self._credential_row_from(row)

    async def create_wallet_credential(
        self,
        wallet_public_id: str,
        exchange: str,
        credential_type: str,
        encrypted_payload: str,
        label: str | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> WalletCredentialRow:
        """Insert a new active wallet credential row."""
        async with self.session() as s:
            cred = WalletCredential(
                wallet_public_id=wallet_public_id,
                exchange=exchange.lower(),
                credential_type=credential_type,
                encrypted_payload=encrypted_payload,
                label=label,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(cred)
            try:
                await s.commit()
            except IntegrityError as exc:
                err_msg = str(exc.orig).lower() if exc.orig else ""
                if "unique" not in err_msg and "duplicate" not in err_msg:
                    raise
                raise CredentialConflictError(
                    wallet_public_id=wallet_public_id,
                    exchange=exchange,
                    reason="active credential for (wallet, exchange) already exists",
                ) from exc
            await s.refresh(cred)
            return self._credential_row_from(cred)

    async def rotate_wallet_credential(
        self,
        credential_public_id: str,
        encrypted_payload: str,
        label: str | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
    ) -> WalletCredentialRow:
        """SCD2 close + insert rotation of a wallet credential."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(WalletCredential).where(
                            WalletCredential.public_id == credential_public_id,
                            *where_active(WalletCredential, timestamp),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                raise CredentialNotFoundError(
                    f"active credential {credential_public_id} not found at "
                    f"{timestamp.isoformat()}"
                )
            await s.execute(
                update(WalletCredential)
                .where(WalletCredential.id == existing.id)
                .values(known_to=timestamp)
            )
            new_cred = WalletCredential(
                wallet_public_id=existing.wallet_public_id,
                exchange=existing.exchange,
                credential_type=existing.credential_type,
                encrypted_payload=encrypted_payload,
                label=label if label is not None else existing.label,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
            )
            s.add(new_cred)
            await s.commit()
            await s.refresh(new_cred)
            return self._credential_row_from(new_cred)

    async def list_wallet_credentials_for_wallet(
        self,
        wallet_public_id: str,
        as_of: datetime,
    ) -> list[WalletCredentialRow]:
        """Active credentials on a single wallet at ``as_of``."""
        async with self.session() as s:
            result = await s.execute(
                select(WalletCredential)
                .where(
                    WalletCredential.wallet_public_id == wallet_public_id,
                    *where_active(WalletCredential, as_of),
                )
                .order_by(WalletCredential.exchange)
            )
            return [self._credential_row_from(row) for row in result.scalars().all()]


_repository_cache: dict[str, Repository] = {}
_live_sqlalchemy_repositories: weakref.WeakSet[object] = weakref.WeakSet()


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
    repos_to_dispose: dict[int, object] = {
        id(cached_repo): cached_repo for cached_repo in _repository_cache.values()
    }
    for live_repo in list(_live_sqlalchemy_repositories):
        repos_to_dispose[id(live_repo)] = live_repo
    for repo in repos_to_dispose.values():
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

    @staticmethod
    def get_instrument_spec_sync(
        session: SyncSession,
        instrument_public_id: str,
        timestamp: datetime,
    ) -> InstrumentSpec | None:
        """Synchronous read of active InstrumentSpec.

        Used by symbol updaters operating inside sync transactions.

        Args:
            session: SQLAlchemy sync session.
            instrument_public_id: Public ID of the instrument.
            timestamp: Point-in-time for temporal query.

        Returns:
            Active InstrumentSpec ORM instance or None.
        """
        return session.execute(
            select(InstrumentSpec).where(
                InstrumentSpec.instrument_public_id == instrument_public_id,
                InstrumentSpec.timestamp <= timestamp,
                InstrumentSpec.known_to > timestamp,
            )
        ).scalar_one_or_none()

    @staticmethod
    def revise_instrument_spec_sync(
        session: SyncSession,
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        spec: InstrumentSpecInput,
    ) -> str:
        """Synchronous SCD2 close+insert for instrument specs.

        Used by symbol updaters operating inside sync transactions.

        Args:
            session: SQLAlchemy sync session.
            instrument_public_id: Public ID of the instrument.
            session_id: Producer session identifier.
            sequence_id: Per-topic monotonic counter.
            timestamp: Current UTC timestamp.
            spec: Spec payload with all fields.

        Returns:
            One of ``created``, ``updated``, or ``unchanged``.
        """
        payload = asdict(spec)
        existing = session.execute(
            select(InstrumentSpec).where(
                InstrumentSpec.instrument_public_id == instrument_public_id,
                InstrumentSpec.timestamp <= timestamp,
                InstrumentSpec.known_to > timestamp,
            )
        ).scalar_one_or_none()
        if existing is not None:
            same = all(getattr(existing, k) == v for k, v in payload.items())
            if same:
                return "unchanged"
        new_values = {
            "instrument_public_id": instrument_public_id,
            "session_id": session_id,
            "sequence_id": sequence_id,
            **payload,
        }
        close_and_insert_sync(
            session=session,
            model=InstrumentSpec,
            match_filters=[InstrumentSpec.instrument_public_id == instrument_public_id],
            new_values=new_values,
            bus_time=timestamp,
        )
        if existing is None:
            return "created"
        return "updated"

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
