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

Concurrency / row locking (SQLite is a known, accepted limitation):
    The atomic-claim paths in this module use ``SELECT ... FOR UPDATE``
    (``.with_for_update()``) to serialize concurrent writers on the same
    row. This is a true row lock on PostgreSQL — the production backend.
    On SQLite ``FOR UPDATE`` is silently ignored (a no-op); SQLite instead
    serializes at the database/connection level, so single-writer dev and
    test runs stay correct. Running multiple concurrent writer processes
    against the SAME SQLite file is therefore NOT supported for these
    claim paths. This is WON'T-FIX by design: SQLite is the dev/test
    backend, PostgreSQL is the deployment backend, and the SCD2 atomicity
    behaviour is covered by ``tests/.../test_scd2_batch_atomicity.py``. Do
    not add application-level locking to paper over the SQLite no-op.

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

import asyncio
import os
import weakref
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from inspect import isawaitable
from typing import Any
from typing import Final
from typing import Protocol
from typing import Unpack
from typing import cast
from uuid import uuid7

from loguru import logger
from sqlalchemy import Select
from sqlalchemy import and_
from sqlalchemy import case
from sqlalchemy import create_engine as create_sync_engine
from sqlalchemy import delete
from sqlalchemy import desc
from sqlalchemy import distinct
from sqlalchemy import event
from sqlalchemy import exists
from sqlalchemy import func
from sqlalchemy import insert
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.engine import Engine as SyncEngine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session as SyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.orm import sessionmaker as sync_sessionmaker
from sqlalchemy.pool import StaticPool

from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.roles import UserRole
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.core.paired_execution import compute_paired_group_key
from snapper.core.partitioning import ShardOwnership
from snapper.core.partitioning import ShardOwnershipError
from snapper.core.types import AllExchange
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedFillProjection
from snapper.core.types import PairedGroupTerminalizeOutcome
from snapper.core.types import TradeCommandStatusEnum
from snapper.core.types import TradeSideEnum
from snapper.data.archive_symbols import resolve_archive_symbols
from snapper.data.db_stats_types import TableCounters
from snapper.data.db_stats_types import TableEntry
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccrualLedger
from snapper.data.models import AiDelegate
from snapper.data.models import AiReview
from snapper.data.models import AiReviewEvent
from snapper.data.models import AlertDelivery
from snapper.data.models import AlertEvent
from snapper.data.models import Base
from snapper.data.models import Candle
from snapper.data.models import DeviceAlertPref
from snapper.data.models import Execution
from snapper.data.models import ExecutionPlan
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.models import ExecutionPlanDecision
from snapper.data.models import FundingRate
from snapper.data.models import Instrument
from snapper.data.models import InstrumentFeedHealth
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import MarketSnapshot
from snapper.data.models import NotificationDevice
from snapper.data.models import Operator
from snapper.data.models import Order
from snapper.data.models import PairedExecutionGroup
from snapper.data.models import PairedExecutionHalt
from snapper.data.models import PairedExecutionLeg
from snapper.data.models import Position
from snapper.data.models import PositionCycle
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import Tick
from snapper.data.models import Trade
from snapper.data.models import TradeCommand
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import UnderlyingAsset
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.models import UserAlertDefault
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.models import VenueEvent
from snapper.data.models import VenueFeeSchedule
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository_types import AccrualLedgerInsertRow
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import AiDelegateRow
from snapper.data.repository_types import AiReviewEventInsertRow
from snapper.data.repository_types import AiReviewInsertRow
from snapper.data.repository_types import AiReviewRow
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertDeliveryRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import AlertListCursor
from snapper.data.repository_types import AtomicResolveResult
from snapper.data.repository_types import CancelClaimResult
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CheckpointUpsertRow
from snapper.data.repository_types import CreateScopeGrantRequest
from snapper.data.repository_types import DeviceAlertPrefRow
from snapper.data.repository_types import DeviceAlertPrefUpsertRow
from snapper.data.repository_types import ExecutionInsertRow
from snapper.data.repository_types import ExecutionPlanCheckpointRow
from snapper.data.repository_types import ExecutionPlanDecisionInsertRow
from snapper.data.repository_types import ExecutionPlanDecisionRow
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import FundingRateInsertRow
from snapper.data.repository_types import FundingRateRow
from snapper.data.repository_types import InstrumentContractRow
from snapper.data.repository_types import InstrumentDetailRow
from snapper.data.repository_types import InstrumentFeedHealthRow
from snapper.data.repository_types import InstrumentFeedHealthUpsertRow
from snapper.data.repository_types import InstrumentFrontMonthRow
from snapper.data.repository_types import InstrumentOrderCapabilityRow
from snapper.data.repository_types import InstrumentRelatedRow
from snapper.data.repository_types import InstrumentSpecRow
from snapper.data.repository_types import InstrumentUnderlyingRow
from snapper.data.repository_types import MarketDataCoverageRow
from snapper.data.repository_types import MarketSnapshotRow
from snapper.data.repository_types import MarketSnapshotUpsertRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.data.repository_types import OperatorRow
from snapper.data.repository_types import OrderInsertRow
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import PairedExecutionGroupFieldUpdate
from snapper.data.repository_types import PairedExecutionGroupInsertRow
from snapper.data.repository_types import PairedExecutionGroupRow
from snapper.data.repository_types import PairedExecutionHaltInsertRow
from snapper.data.repository_types import PairedExecutionHaltRow
from snapper.data.repository_types import PairedExecutionLegFieldUpdate
from snapper.data.repository_types import PairedExecutionLegInsertRow
from snapper.data.repository_types import PairedExecutionLegRow
from snapper.data.repository_types import PendingReviewSummary
from snapper.data.repository_types import PositionCycleInsertRow
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import PositionRow
from snapper.data.repository_types import ScopeGrantRow
from snapper.data.repository_types import SettingRow
from snapper.data.repository_types import SignalRow
from snapper.data.repository_types import TickRow
from snapper.data.repository_types import TickUpsertRow
from snapper.data.repository_types import TradeCommandDispatchUpdate
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import TradeRow
from snapper.data.repository_types import TradeUpsertRow
from snapper.data.repository_types import UnderlyingAssetRow
from snapper.data.repository_types import UserActiveTokenInsertRow
from snapper.data.repository_types import UserActiveTokenVerificationRow
from snapper.data.repository_types import UserAlertDefaultRow
from snapper.data.repository_types import UserAlertDefaultUpsertRow
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
    "ENV_VARS",
]

_DB_POOL_SIZE_ENV: Final[str] = "DB_POOL_SIZE"
_DB_MAX_OVERFLOW_ENV: Final[str] = "DB_MAX_OVERFLOW"
ENV_VARS: Final[frozenset[str]] = frozenset({_DB_POOL_SIZE_ENV, _DB_MAX_OVERFLOW_ENV})
"""Per-process SQLAlchemy engine pool-clamp keys (PostgreSQL only).

Read directly via ``os.getenv`` in :meth:`SQLAlchemyRepository.__init__`
so a feed container running N publisher subprocesses can cap each
process's pool — :func:`get_repository` caches one engine per URL per
process, so splitting publishers across processes otherwise multiplies
the default ``pool_size`` (5) + ``max_overflow`` (10) per process and
can exhaust Postgres ``max_connections``. Unset (the default) preserves
SQLAlchemy's default sizing, so the single-container backend is
unaffected. Registered on the shared ``.env`` allowlist via
:mod:`snapper.config.env_contract`.
"""


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


@dataclass(frozen=True, slots=True)
class _DeviceAlertPrefValues:
    """Merged values for a new active ``DeviceAlertPref`` version."""

    public_id: str
    enabled: bool
    min_priority: str
    quiet_hours_start_min: int | None
    quiet_hours_end_min: int | None
    mute_until: datetime | None
    timezone: str


@dataclass(frozen=True, slots=True)
class _UserAlertDefaultValues:
    """Merged values for a new active ``UserAlertDefault`` version."""

    public_id: str
    enabled: bool
    min_priority: str


@dataclass(frozen=True, slots=True)
class _DeviceAlertPrefAttemptResult:
    """Result of a single retryable device preference write attempt."""

    public_id: str
    error: Exception | None


@dataclass(frozen=True, slots=True)
class _UserAlertDefaultAttemptResult:
    """Result of a single retryable user-level default write attempt.

    Mirrors ``_DeviceAlertPrefAttemptResult`` so the retry-loop in
    ``upsert_user_alert_default`` can short-circuit on ``error is
    None`` and return ``public_id`` directly without an extra
    not-None guard.
    """

    public_id: str
    error: Exception | None


_TRADE_COMMAND_TERMINAL_STATUSES: tuple[str, ...] = (
    TradeCommandStatusEnum.FILLED,
    TradeCommandStatusEnum.CANCELLED,
    TradeCommandStatusEnum.EXPIRED,
    TradeCommandStatusEnum.REJECTED,
    TradeCommandStatusEnum.FAILED,
)
_ORDER_SUBMIT_EVIDENCE_EVENT_TYPES: tuple[str, ...] = (
    "order_accepted",
    "fill_observed",
    "order_terminal",
    "order_submit_unknown",
    "order_breaker_open",
)
"""Venue event types proving an order submit must not be re-submitted.

The duplicate-submit guard drops a replayed command whose
client_order_id carries any of these — the order is or was live (or its
state is UNKNOWN pending verification), so re-submitting could double a
MARKET position. ``order_rejected`` is deliberately EXCLUDED: a cid
whose only durable history is a rejection definitively never placed,
and re-publishing such a command is the outbox's legitimate retry path
— including it would strand every retried command.
``order_breaker_open`` IS included although that order never placed
either: the engine's intent releases on its REJECTED publish, so a
late redispatched frame submitting after the breaker closes would
place an order nobody tracks — breaker-open commands must wait for the
engine to decide anew (#145 P2-5 §2d).
"""
_ORDER_LIFECYCLE_EVENT_TYPES: tuple[str, ...] = (
    "order_accepted",
    "order_rejected",
    "order_terminal",
    "fill_observed",
    "order_submit_unknown",
    "order_breaker_open",
)
"""Venue event types the trade-command lifecycle fold consumes.

The coordinator ``ReconciliationLoop`` folds these append-only rows
into durable ``TradeCommand`` status advances (ack/partial/terminal),
so the command table can drive venue-side reconciliation instead of
warning forever about rows stuck at ``dispatched``. Superset of
``_ORDER_SUBMIT_EVIDENCE_EVENT_TYPES``: the fold also needs
``order_rejected`` (REJECTED advance) and ``order_breaker_open``
(FAILED advance) which the duplicate-submit guard deliberately treats
differently.
"""
_ORDER_LIFECYCLE_LOOKUP_CHUNK_SIZE = 300
_ORDER_RESOLVING_EVENT_TYPES: tuple[str, ...] = (
    "order_accepted",
    "order_rejected",
    "order_terminal",
    "fill_observed",
    "order_breaker_open",
)
"""Venue event types that RESOLVE a dispatched command's fate.

The executor's dispatched-verification sweep targets commands with NONE
of these: a lone ``order_submit_unknown`` does NOT resolve (those are
exactly the restart-lost parked entries the sweep must re-verify), and
``order_rejected`` DOES (rejecting again on stale absence evidence after
a legal retry would race the retry's own lifecycle).
"""
_LIFECYCLE_FOLD_COMMAND_TYPES: tuple[str, ...] = ("create", "submit")
_CandleNaturalKey = tuple[str, str, datetime]
_CANDLE_LOOKUP_CHUNK_SIZE = 300
_KRAKEN_EQUITIES_EXCHANGE: Final[str] = "kraken_equities"
_CANDLE_ID_CACHE_LOOKBACK: Final[timedelta] = timedelta(days=2)
"""How far back ``get_latest_candle_ids`` looks for the newest candle per
``(instrument, timeframe)`` when warming the publisher's startup cache.

The cache only needs the *current* candle per series so live upserts reuse its
``public_id``; a current candle's ``open_at`` is always recent (at most one
period old, and the longest emitted timeframe is daily). Bounding ``open_at``
to this window lets the lookup ride ``ix_candle_instrument_open`` instead of a
full scan of the (multi-hundred-million-row) candles table, which otherwise
blocks every feed publisher's startup for minutes before its WebSocket
connects. Two days comfortably covers the daily timeframe plus restart slack;
candles older than the window are closed and never receive further updates, so
omitting them from the cache cannot create duplicate versions."""
_SnapshotNaturalKey = str
_SNAPSHOT_LOOKUP_CHUNK_SIZE = 500
_OUTBOX_BULK_LOOKUP_CHUNK_SIZE = 200
_PLAN_CHECKPOINT_LOOKUP_CHUNK_SIZE = 200
DEFAULT_HIGH_CARDINALITY_LIMIT = 100_000
_HIGH_CARDINALITY_STREAM_CHUNK_SIZE = 5_000
_SQLITE_COUNT_GUARD_ROWS = 10_000_000
ScopeExpansionKey = tuple[str, str | None, str | None]


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
    (Walutomat) leave them ``None``.
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


class _ClosableConnection(Protocol):
    """Protocol for tracked raw driver connections."""

    def close(self) -> object:
        """Close the underlying connection."""


class _DisposableEngine(Protocol):
    """Protocol for engine-like objects that expose dispose()."""

    def dispose(self) -> object:
        """Dispose the underlying engine resources."""


def _resolve_disposable_engine(engine: object) -> _DisposableEngine | None:
    """Return an engine-like object when it exposes a callable dispose()."""
    dispose = getattr(engine, "dispose", None)
    if not callable(dispose):
        return None
    return cast(_DisposableEngine, engine)


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

    async def wait_until_ready(self, *, timeout_s: float = 120.0, interval_s: float = 1.0) -> None:
        """Block until the database accepts a trivial query, or raise on timeout.

        Startup gate for host reboots: the snapper containers can start
        before Postgres is accepting connections (Postgres is external to
        the compose stack, so ``depends_on`` cannot order it), and the first
        DB query then fails with asyncpg ``ConnectionRefusedError``. This
        retries ``SELECT 1`` every ``interval_s`` until it succeeds or
        ``timeout_s`` elapses, so the settings load and the process-manager
        summary loop never race a not-yet-ready database.

        Only a transient connection/startup failure is retried (see
        :meth:`_is_transient_db_connection_error`): any ``OSError`` (asyncpg
        ``ConnectionRefusedError`` and other network-layer failures) and a
        ``DBAPIError`` whose PostgreSQL SQLSTATE is absent, in the
        connection-exception class ``08*``, or ``57P03`` (server starting up).
        A permanent ``DBAPIError`` — bad credentials (``28P01``), wrong
        database (``3D000``), etc. — is re-raised IMMEDIATELY so a
        misconfiguration is not hidden behind a 120s "not ready" wait. On a
        retried failure the first one logs a concise WARNING, later attempts
        retry silently to avoid traceback spam, and a single INFO is logged
        once the wait clears. On timeout a ``RuntimeError`` is raised; with
        ``restart: unless-stopped`` a genuine prolonged outage then surfaces
        as a visible restart rather than a process running against a dead
        database.

        Args:
            timeout_s: Maximum seconds to wait before raising. Default 120s
                covers Postgres coming up during a slow host reboot without
                churning container restarts.
            interval_s: Seconds slept between connection attempts.

        Returns:
            None once the database answers ``SELECT 1``.

        Raises:
            RuntimeError: If the database is still unreachable after
                ``timeout_s``.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        warned = False
        waited = False
        while True:
            try:
                async with self.session() as session:
                    await session.execute(text("SELECT 1"))
            except (OSError, DBAPIError) as exc:
                if not self._is_transient_db_connection_error(exc):
                    raise
                if loop.time() >= deadline:
                    raise RuntimeError(f"database not ready after {timeout_s}s") from exc
                if not warned:
                    logger.warning("database not ready, waiting up to {}s: {}", timeout_s, exc)
                    warned = True
                waited = True
                await asyncio.sleep(interval_s)
                continue
            if waited:
                logger.info("database ready")
            return

    @staticmethod
    def _is_transient_db_connection_error(exc: OSError | DBAPIError) -> bool:
        """Return True if a readiness-probe failure is retryable.

        Distinguishes "database not up yet" from a permanent misconfiguration
        so :meth:`wait_until_ready` retries only the former and surfaces the
        latter immediately — a bad password or database name must not be
        hidden behind a 120s "not ready" wait + crash loop.

        Retryable: any ``OSError`` (asyncpg ``ConnectionRefusedError`` and
        other network-layer failures while the server comes up), and any
        ``DBAPIError`` whose underlying PostgreSQL SQLSTATE is absent (a bare
        connection-establishment failure with no server code), in the
        connection-exception class ``08*``, or ``57P03`` ("cannot_connect_now"
        — the server is starting up). Every other ``DBAPIError`` (for example
        ``28P01`` invalid_password, ``3D000`` invalid_catalog_name) is a
        permanent fault and is not retried.

        Args:
            exc: The probe exception, already narrowed to ``OSError`` or
                ``DBAPIError`` by the caller's ``except`` clause.

        Returns:
            True to retry the probe, False to re-raise the exception.
        """
        if isinstance(exc, OSError):
            return True
        sqlstate: str | None = getattr(getattr(exc, "orig", None), "sqlstate", None)
        return sqlstate is None or sqlstate.startswith("08") or sqlstate == "57P03"

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
        """Load the latest recent candle public_id per (instrument, timeframe).

        Warms the publisher's in-memory candle ID cache on startup so live
        upserts reuse existing public_ids for the current open_at window. This
        is a deliberately bounded lookup, NOT a general point-in-time query:
        only series with an active candle whose ``open_at`` falls within
        ``_CANDLE_ID_CACHE_LOOKBACK`` of ``as_of`` are returned. Series whose
        newest candle is older than that window are omitted (their candles are
        closed and never receive further updates, and ``upsert_candles`` still
        reuses any persisted public_id by natural key, so a cache miss cannot
        create a duplicate version). The bound is what keeps this off a full
        scan of the candles table.

        Args:
            as_of: Upper bound of the temporal/open_at window (typically now).

        Returns:
            Mapping of (instrument_public_id, timeframe) to (open_at, public_id),
            limited to series with a candle in the recent lookback window.
        """
        ...

    @abstractmethod
    async def upsert_candles(
        self, rows: list[CandleUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Insert or update candles. Return the inserted/updated count.

        Rows whose active version already carries identical OHLCV/vwap/trade
        values are a no-op and excluded from the count, so re-upserting
        unchanged data returns ``0``; only rows that insert a new version
        (new candle or a genuine correction) are counted.

        Optional ``session`` lets writer tasks share a pinned
        connection across many flushes.
        """
        ...

    @abstractmethod
    async def upsert_trades(
        self, rows: list[TradeUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Insert trades, skipping duplicates. Return inserted count.

        Optional ``session`` lets writer tasks share a pinned
        connection across many flushes.
        """
        ...

    @abstractmethod
    async def upsert_ticks(
        self, rows: list[TickUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Insert ticks via append-only bulk INSERT.

        Append-only path with no conflict semantics (ticks are unique by
        ``public_id`` UUID7). Uses SQLAlchemy Core ``insert(Tick).values``
        directly to bypass ORM identity-map and per-row object
        construction overhead — material at publisher throughputs above
        ~500 ticks/sec.

        When ``session`` is provided the caller is expected to manage the
        transaction (commit / rollback). When ``session`` is ``None`` a
        fresh session is opened, the insert is committed, and the
        session is closed before returning. Publisher writer tasks pass
        a long-lived session to amortise the per-flush connection-acquire
        cost; standalone callers (tests, bulk imports) pass ``None``.

        Args:
            rows: Tick rows to insert. ``public_id`` is auto-filled when
                missing.
            session: Optional caller-managed session. When ``None``, the
                repository opens its own session and commits.

        Returns:
            Number of rows inserted.
        """
        ...

    @abstractmethod
    async def insert_order(
        self,
        row: OrderInsertRow | None = None,
        **kwargs: Unpack[OrderInsertRow],
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
        row: ExecutionInsertRow | None = None,
        **kwargs: Unpack[ExecutionInsertRow],
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
        limit: int = DEFAULT_HIGH_CARDINALITY_LIMIT,
    ) -> list[TickRow]:
        """Retrieve ticks for instrument in time range.

        ``limit`` caps the materialized result to bound RAM on the
        highest-cardinality table in the schema. The default
        :data:`DEFAULT_HIGH_CARDINALITY_LIMIT` is large enough for
        typical UI windows but small enough to prevent multi-day
        replays from OOMing the engine. Callers that genuinely need
        full ranges should iterate via :meth:`iter_ticks`.
        """
        ...

    @abstractmethod
    def iter_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> AsyncIterator[TradeRow]:
        """Stream trades in time order without materialising the full result.

        Built for high-cardinality replays (paper backtest,
        multi-day windows). Implementations yield rows in
        ``event_time ASC`` order using the
        ``coalesce(executed_at, timestamp)`` event time.
        """
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
    def iter_market_snapshots(
        self,
        instrument_public_ids: list[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> AsyncIterator[MarketSnapshotRow]:
        """Stream market snapshots in time order without materialising.

        Companion to :meth:`get_market_snapshots` for paper-mode
        backtest replays over long windows. Implementations yield
        rows in ``timestamp ASC`` order.
        """
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
    async def get_exchange_instruments_detail(
        self,
        exchange: str,
        as_of: datetime,
    ) -> list[InstrumentDetailRow]:
        """Return capability-aware instrument rows for a given exchange.

        Joins ``Symbol`` + ``SymbolExchangeCapability`` + ``Instrument`` +
        ``InstrumentSpec`` at the requested temporal snapshot.
        Powers ``GET /api/exchanges/{exchange}/instruments/detail`` so the
        frontend can render market-data-only badges + gate order-entry
        without a separate capability round-trip.

        Args:
            exchange: Exchange identifier (lowercase; same canonicalization
                as ``get_exchange_instruments``).
            as_of: Point-in-time for the multi-table temporal join.

        Returns:
            Sorted list of ``InstrumentDetailRow`` entries (by native symbol).
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
        status: str | None = None,
        wallet_public_ids: list[str] | None = None,
    ) -> list[OrderRow]:
        """Retrieve orders with optional filters and pagination.

        Args:
            limit: Maximum number of orders to return.
            offset: Number of orders to skip.
            as_of: Point-in-time for temporal query.
            symbol: Optional native symbol filter.
            exchange: Optional exchange filter.
            status: Optional ``OrderStatusEnum`` filter pushed INTO SQL
                (must run pre-pagination — post-fetch filtering breaks
                pagination because limit/offset clip before the filter
                can discard non-matching rows).
            wallet_public_ids: Optional wallet scope filter for
                multi-tenant scoping. When ``None``, no wallet filter is
                applied (ADMIN sees all). When a non-empty list, only
                orders on the listed wallets are returned.

        Returns:
            Order dicts ordered by created_at DESC, denormalized with
            instrument and symbol info plus ``plan_public_id`` (needed
            by MCP ``get_order_status`` to resolve the parent execution
            plan without a second round-trip).
        """
        ...

    @abstractmethod
    async def get_orders_total_count(
        self,
        as_of: datetime,
        symbol: str | None = None,
        exchange: str | None = None,
        status: str | None = None,
        wallet_public_ids: list[str] | None = None,
    ) -> int:
        """Count orders matching the same filter shape as :meth:`get_orders`.

        Used by MCP ``list_orders`` to surface total-count alongside the
        offset/limit page so callers can size pagination without extra
        round-trips. Filter shape MUST mirror :meth:`get_orders` so the
        count is the exact pre-pagination cardinality.

        Args:
            as_of: Point-in-time for temporal query.
            symbol: Optional native symbol filter.
            exchange: Optional exchange filter.
            status: Optional ``OrderStatusEnum`` filter (SQL).
            wallet_public_ids: Optional wallet scope filter (same
                semantics as :meth:`get_orders`).

        Returns:
            Count of matching orders.
        """
        ...

    @abstractmethod
    async def get_order_by_command_public_id(
        self,
        command_public_id: str,
        as_of: datetime,
    ) -> OrderRow | None:
        """Resolve an order from its triggering trade-command public_id.

        ``Order`` rows do NOT carry ``command_public_id`` directly;
        the link is via the parent
        ``execution_plan``: ``trade_commands.public_id == :command``
        AND ``trade_commands.plan_public_id == orders.plan_public_id``.
        Implementation issues an internal JOIN on those columns.

        Returns ``None`` when:

        - The trade command does not exist (caller's responsibility to
          distinguish from "command exists but order not yet ACK'd" by
          calling :meth:`get_trade_command_by_public_id` first), OR
        - The trade command exists but no order has been written for
          its ``plan_public_id`` yet (the exchange has not ACK'd the
          submission). Callers surface this as a ``pending_dispatch``
          synthetic envelope using the command row's ``plan_public_id``.

        Args:
            command_public_id: UUID7 of the ``trade_commands`` row.
            as_of: Point-in-time for temporal query.
        """
        ...

    @abstractmethod
    async def get_trade_command_by_public_id(
        self,
        command_public_id: str,
        as_of: datetime,
    ) -> TradeCommandRow | None:
        """Fetch a single trade command row by its public_id.

        Used by MCP ``get_order_status`` to disambiguate "command not
        found" from "command exists but order not ACK'd" — a
        :meth:`get_order_by_command_public_id` ``None`` could mean
        either, and the synthetic ``pending_dispatch`` envelope needs
        the command's ``plan_public_id`` + wallet scope to populate.

        Args:
            command_public_id: UUID7 of the ``trade_commands`` row.
            as_of: Point-in-time for temporal query.
        """
        ...

    @abstractmethod
    async def get_executions_for_order(
        self,
        order_public_id: str,
        as_of: datetime,
    ) -> list[ExecutionRow]:
        """Return every execution row belonging to a single order.

        Filtering the generic :meth:`get_executions` result by
        ``order_public_id`` would silently omit fills if the wallet had
        ``limit`` newer fills on unrelated orders. SQL-side filter is
        the only correct shape.

        Args:
            order_public_id: UUID7 of the parent ``orders`` row.
            as_of: Point-in-time for temporal query.

        Returns:
            Executions ordered by ``timestamp ASC`` (oldest first) so
            the chronological fill history is the natural read order.
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
        exchange: str | None,
        as_of: datetime,
        wallet_public_id: str = "",
    ) -> list[OrderRow]:
        """Retrieve non-terminal orders for startup recovery.

        Returns orders with active status (open, pending, pending_new,
        new, partially_filled) for a given exchange (or all exchanges
        when ``exchange`` is ``None``). Used exclusively by executor and
        trader recovery, not by API endpoints.

        Args:
            exchange: Exchange name to filter by, or ``None`` to skip
                the exchange filter and return orders across every
                exchange in a single query. ZMQ trader recovery uses
                ``None`` to collapse the legacy per-exchange waterfall
                into one round-trip.
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
    async def get_related_instruments_for_symbol(
        self,
        exchange: str,
        native_symbol: str,
        as_of: datetime,
    ) -> tuple[UnderlyingAssetRow | None, list[InstrumentRelatedRow]]:
        """Resolve the underlying + every sibling instrument for a UI-selected symbol.

        Powers ``GET /api/instruments/{exchange}/{native_symbol}/related``.
        Returns ``(None, [])`` when the symbol is unknown, the instrument
        is not provisioned, or no underlying mapping exists (orphan).

        Args:
            exchange: Exchange identifier the symbol is requested for.
            native_symbol: Native symbol string (e.g. ``BTC-USD``).
            as_of: Point-in-time for temporal query.

        Returns:
            Tuple of (UnderlyingAssetRow|None, list[InstrumentRelatedRow]).
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

    def resolve_underlying_description(
        self,
        row: UnderlyingAssetRow,
        locale: str,
    ) -> str | None:
        """Resolve an underlying description map for a caller locale.

        Args:
            row: Underlying asset row containing the stored locale map.
            locale: Preferred caller language.

        Returns:
            Locale-specific description, English fallback, or ``None``.
        """
        description = row["description"]
        if description is None:
            return None
        if locale in description:
            return description[locale]
        return description.get("en")

    def resolve_underlying_name(
        self,
        row: UnderlyingAssetRow,
        locale: str,
    ) -> str:
        """Resolve an underlying name map for a caller locale.

        Args:
            row: Underlying asset row containing the stored locale map.
            locale: Preferred caller language.

        Returns:
            Locale-specific name, falling back to the English entry. Names
            are always non-empty (the English entry is required at YAML
            load time), so this method always returns a string.
        """
        name = row["name"]
        if locale in name:
            return name[locale]
        return name["en"]

    @abstractmethod
    async def upsert_underlying_asset(
        self,
        ticker: str,
        name: dict[str, str] | str,
        asset_class: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        sector: str | None = None,
        description: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        """SCD2 upsert for an underlying asset.

        Args:
            ticker: Short code (e.g. 'SPX').
            name: Locale-keyed canonical names (e.g. ``{"en": "S&P 500"}``).
                Bare strings are accepted for back-compat and stored under
                ``en``.
            asset_class: Asset type from AssetTypeEnum.
            session_id: Provenance session ID.
            sequence_id: Provenance sequence number.
            timestamp: Bus time.
            sector: Optional sector (e.g. 'Precious Metals').
            description: Optional locale-keyed human-readable descriptions.

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
        the project temporal-query convention (always pass an explicit
        window instead of relying on KNOWN_TO_MAX).

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
        cancel_idempotency_key: str | None = None,
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
            cancel_idempotency_key: Caller-supplied dedup key for the
                cancel transition. ``None`` preserves
                the existing column value on the new SCD2 row;
                non-``None`` writes the supplied key (used by
                :class:`PlansCancelService` to claim cancel idempotency).

        Returns:
            New row id, or None if no active row found.
        """
        ...

    @abstractmethod
    async def claim_execution_plan_cancel(
        self,
        public_id: str,
        idempotency_key: str | None,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        cancel_requested_at: datetime,
    ) -> CancelClaimResult:
        """Atomically claim the cancel transition with CAS-style preconditions.

        Locks the active execution-plan row, verifies
        the precondition (no key claimed yet, or matching caller key),
        and only then performs the SCD2 close-and-insert with the
        caller's ``idempotency_key`` written on the new row. Avoids the
        race window in :meth:`update_execution_plan_status` where two
        callers reading at different ``bus_time`` instants would both
        see an actionable row, both transition, and the second blindly
        overwrite the first's claim.

        Args:
            public_id: Plan public identifier.
            idempotency_key: Caller-supplied dedup key. ``None`` is
                accepted (REST-shaped legacy callers); only non-``None``
                keys are persisted on the new SCD2 row and only
                non-``None`` keys block cross-caller key reuse via the
                partial-unique index.
            bus_time: Timestamp for the SCD2 close-and-insert.
            session_id: Producer session identifier.
            sequence_id: Monotonic sequence counter.
            cancel_requested_at: Wall-clock instant the caller's
                cancel intent fired (for the new SCD2 row's
                ``cancel_requested_at`` column).

        Returns:
            :class:`CancelClaimResult` discriminating the outcome —
            ``claimed`` for a successful CAS transition,
            ``replay`` when the active row already carries the same
            key (idempotent retry; caller short-circuits without
            emitting another cancel command),
            ``key_mismatch`` when the active row has a different
            non-null key,
            ``in_progress`` when status is ``cancel_requested`` with
            no caller-key match,
            ``terminal`` when the plan is already terminal,
            ``not_found`` when no active row exists.
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
    async def get_latest_checkpoints_for_plans(
        self,
        plan_public_ids: list[str],
    ) -> dict[str, ExecutionPlanCheckpointRow]:
        """Return the most recent active checkpoint per plan, keyed by plan public_id.

        Bulk variant of :meth:`get_latest_plan_checkpoint` for the
        :py:class:`~snapper.application.plans.service.PlanExecutorService`
        startup recovery path (avoids a per-plan N+1 round-trip in
        ``_recover_plans``). Plans without an active checkpoint are
        absent from the returned dict.

        Args:
            plan_public_ids: Plans to query (deduplicated internally).

        Returns:
            Mapping from plan public_id to its latest active checkpoint
            row. Missing plans are omitted (not mapped to None).
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
            ownership: Optional partitioning guard. When
                provided, the row's ``shard_key`` MUST be owned by
                this ownership view or :class:`ShardOwnershipError`
                is raised before the DB write. Opt-in — callers that
                legitimately write foreign-shard rows (HTTP handlers
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

        All-time count.
        Non-terminal statuses: ``created``, ``dispatched``,
        ``direct_dispatched``, ``accepted``, ``partially_filled``.
        Only command_type ``create`` / ``submit`` / ``replace`` rows
        count — cancels are not in-flight exposure.

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
        rolling 24h USD commitment. Excludes rows whose
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
        window per). Counts every row where
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
    async def insert_user_active_tokens(
        self,
        rows: list[UserActiveTokenInsertRow],
    ) -> None:
        """Persist every freshly-minted access + refresh token row.

        Driven by :meth:`TokenManager.persist_tokens` after each
        successful ``create_tokens()`` — the inventory is what the
         DB-backed ``verify_token`` SELECTs against and what
        meth:`TokenManager.revoke_user_sessions` flips on the kill
        switch. Insertion is batched because both tokens in a pair
        share the same ``issued_at`` and a single round-trip
        preserves the invariant that either both rows land or
        neither does.

        Args:
            rows: Insert-row batch. Empty list is a no-op. Each row
                must supply every NOT NULL column on
                class:`~snapper.data.models.UserActiveToken`; the
                ``revoked_at`` column defaults to ``NULL`` (active).
        """
        ...

    @abstractmethod
    async def rotate_user_active_token(
        self,
        old_jti: str,
        new_rows: list[UserActiveTokenInsertRow],
        revoked_at: datetime,
    ) -> bool:
        """Atomic refresh-rotation: revoke ``old_jti`` AND insert new rows.

        The refresh endpoint exchanges a redeemed refresh JWT for a
        fresh access + refresh pair. This flow requires the
        exchange be atomic at the DB layer: the old row's
        ``revoked_at`` must flip ONLY if the new pair lands, and
        vice-versa. Splitting the two operations across two
        independent transactions opens two failure modes:
            1. Replay: if the old row is already revoked
               (``False`` return) and the caller still rotates, a
               compromised refresh JWT could issue multiple successor
               pairs inside the in-memory blacklist grace window. The
               atomic path surfaces ``False`` so the route can return
               401 BEFORE minting a new pair.
            2. Stranded user: if revoke commits but the subsequent
               insert fails (transient DB error, connection reset)
               the user loses their refresh token without receiving
               a replacement. One transaction → either both stick
               or both roll back; the caller retries with the
               original refresh JWT.
        The method does NOT seed the in-memory blacklist — that's
        the caller's responsibility AFTER this call returns ``True``,
        so the blacklist grace period starts at the post-commit
        moment.

        Args:
            old_jti: JTI of the refresh token being redeemed. Must
                identify the CURRENT active row; if the row is
                already revoked or missing, the method returns
                ``False`` and NOTHING is inserted (rollback contract).
            new_rows: Insert batch for the successor access +
                refresh rows. Typically 2 rows produced by
                meth:`TokenManager.persist_tokens`-style decoding.
            revoked_at: Timestamp stamped on the old row's
                ``revoked_at`` when the rotation commits.

        Returns:
            ``True`` when the rotation committed; ``False`` when the
            old row was already revoked or absent — in which case the
            new rows were NOT inserted.
        """
        ...

    @abstractmethod
    async def revoke_user_active_token_by_jti(
        self,
        jti: str,
        revoked_at: datetime,
    ) -> int:
        """Flip ``revoked_at`` on a single row identified by ``jti``.

        Single-row counterpart to
        meth:`revoke_user_active_tokens` used on the refresh-token
        rotation path: when a caller redeems a refresh
        JWT, the old row is marked revoked immediately so a replay
        of the same refresh JWT post-rotation fails even before the
        blacklist grace period elapses. The call is idempotent — a
        second invocation on an already-revoked row leaves
        ``revoked_at`` unchanged and returns 0 (rowcount excludes
        rows that already matched the WHERE clause).

        Args:
            jti: JWT ID of the row to revoke. Unknown JTIs are a
                no-op (not an error).
            revoked_at: Timestamp to stamp on ``revoked_at``.
                Typically ``datetime.now(UTC)`` at the rotation
                entry.

        Returns:
            1 when the row was flipped, 0 when no matching unrevoked
            row existed.
        """
        ...

    @abstractmethod
    async def get_active_token_by_hash(
        self,
        token_hash: str,
    ) -> UserActiveTokenVerificationRow | None:
        """Return verify-path row by ``token_hash``, joined with ``users.is_active``.

        Used by the async ``verify_token`` to check in a
        single round-trip that (a) the presented JWT has a matching
        ``user_active_tokens`` row (rejects pre-migration tokens per
         Deployment note), (b) it has not been revoked
        (``revoked_at IS NULL``), (c) the owner is still active
        (``users.is_active = True``), and (d) the cached
        ``expires_at`` matches what the JWT payload carries. The
        join on ``users`` uses the SCD2 active row (``known_to
        = KNOWN_TO_MAX``) so a deactivation (which writes a fresh
        SCD2 row with ``is_active=False``) is visible immediately
        without a second SELECT.

        Args:
            token_hash: SHA-256 HEX of the presented JWT. Callers
                must compute the hash; this method does NOT accept
                raw tokens so the plaintext never reaches the
                repository layer.

        Returns:
            A :class:`UserActiveTokenVerificationRow` when a row
            exists, or ``None`` when no matching row was found
            (pre-migration token, token was issued on a different
            instance before table deploy, or a malicious / tampered
            token whose hash doesn't collide with any known row).
        """
        ...

    @abstractmethod
    async def list_active_user_token_jtis(self, user_public_id: str) -> list[str]:
        """Return every unrevoked JTI for a user's active tokens.

        Reads the non-temporal ``user_active_tokens`` table and
        returns the JWT IDs whose ``revoked_at`` is still NULL. Used
        by :meth:`TokenManager.revoke_user_sessions` to load the
        in-memory fast-path blacklist before flipping DB state
        every JTI the kill switch revokes must appear in both the
        ``user_active_tokens.revoked_at`` column AND the
        attr:`TokenManager._blacklisted_tokens` cache so ``verify_token``
        rejects it on the very next request regardless of which code
        path checks first.

        Args:
            user_public_id: UUID of the user whose tokens are being
                revoked.

        Returns:
            List of JTI strings. Empty list if the user has no
            active tokens (possible if every token already expired
            naturally or was previously revoked).
        """
        ...

    @abstractmethod
    async def revoke_user_active_tokens(self, user_public_id: str, revoked_at: datetime) -> int:
        """Mark every unrevoked token row for a user as revoked.

        Sets ``revoked_at=revoked_at`` on every row where
        ``user_public_id`` matches AND ``revoked_at IS NULL``. The
        operation is a single UPDATE — atomic per SQLAlchemy
        session. Rows are NOT deleted (audit preservation); the
        daily ``token_cleanup_loop`` handles expired / long-revoked
        rows.

        Args:
            user_public_id: UUID of the user whose tokens are being
                revoked.
            revoked_at: Timestamp to stamp on ``revoked_at``.
                Typically ``datetime.now(UTC)`` at the kill-switch
                entry.

        Returns:
            Number of rows updated (0 if no unrevoked tokens
            existed — not an error).
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
    async def get_plan_public_ids_for_client_order_ids(
        self,
        client_order_ids: list[str],
        as_of: datetime,
    ) -> dict[str, str]:
        """Resolve many client_order_id -> plan_public_id in one query.

        Batched variant of :meth:`get_plan_public_id_for_client_order_id`
        used by plan recovery when ``_reemit_stranded_cancel``
        sweeps every child of a recovered plan. Per-id RTTs in the
        legacy loop scale linearly with child count (10-100 RTTs for
        wide grid plans on startup); the batched ``IN`` query keeps
        recovery cost flat.

        Returns:
            Mapping ``client_order_id -> plan_public_id`` for ids that
            resolved to a plan-linked ``create`` command. Unresolved
            ids are simply absent from the returned dict.
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
        by exchange id (Kraken, Walutomat) can actually cancel.
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
    async def get_open_position_cycles_for_shards(
        self,
        shard_keys: list[str],
        as_of: datetime,
    ) -> dict[str, PositionCycleRow]:
        """Return open cycles for many shards in a single query.

        Used by ZMQ trader recovery to collapse a
        per-engine ``get_open_position_cycle`` waterfall into one
        round-trip. Shards with no active open cycle are simply
        absent from the returned dict.

        Args:
            shard_keys: Shard keys to look up. May be empty (the
                caller should short-circuit but the repo handles it
                by returning ``{}``).
            as_of: Bus time for the temporal query.

        Returns:
            Mapping ``shard_key -> PositionCycleRow`` for shards that
            currently hold an open cycle.
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
    async def get_instrument_public_ids_by_symbols(
        self,
        native_symbols: set[str],
        exchange: str,
        as_of: datetime,
    ) -> dict[str, str]:
        """Resolve native symbols on an exchange to instrument public IDs.

        Returns only symbols that have an active Symbol and Instrument row
        at ``as_of``.

        Args:
            native_symbols: Native symbol strings to resolve.
            exchange: Exchange name.
            as_of: Point-in-time for temporal query.

        Returns:
            Mapping from native symbol to instrument public ID.
        """
        ...

    @abstractmethod
    async def get_symbol_for_instrument(
        self,
        instrument_public_id: str,
        as_of: datetime,
    ) -> str | None:
        """Resolve an instrument public_id back to its native symbol string.

        Joins active ``Instrument`` + active ``Symbol`` rows at ``as_of`` and
        returns ``Symbol.native_symbol``. Returns ``None`` when the
        instrument row does not exist or its joined Symbol is no longer
        active at the requested snapshot.

        Used by the order-entry capability guard to translate a UUID-shaped
        ``instrument_public_id`` (as produced by backend-side Instrument
        resolution) into the native symbol required by
        ``snapper.infrastructure.symbols.functions.is_tradeable`` — which
        reads ``SymbolExchangeCapability.can_trade`` keyed on the
        native-symbol cache.
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
    async def list_scope_grant_instrument_pairs(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> set[tuple[str, str]]:
        """Return ``(exchange, native_symbol)`` pairs covered by operators' grants.

        Unions every instrument reachable from the operators' active scope
        grants across ALL wallets, then projects each to the
        ``(exchange, native_symbol)`` pair via active ``Instrument`` +
        ``Symbol`` rows. Underlying-scoped grants expand dynamically via
        ``instrument_underlying_mappings`` active at ``as_of``;
        instrument-scoped grants resolve directly.

        Used by the subscribe-time AI_DELEGATE wallet-scope filter
        and by the admin-bus mid-session revalidation path.
        No caching — grants / mappings / symbol-exchange joins can all
        change between calls, so callers get an authoritative read.

        Implementation issues up to three sequential queries (grants,
        optional mapping expansion, instrument+symbol JOIN). The
        multi-query shape is preferred over a single round-trip
        because the per-step JOIN is cheaper than a CTE/UNION over
        four SCD2 tables when the first query returns zero grants
        (the common case for operators that hold no active grants at
        ``as_of``).

        Args:
            operator_public_ids: Operator identity set (typically the
                delegate principal's ``operator_public_ids``).
            as_of: Bus time for the temporal reads on grants +
                mappings + instruments + symbols.

        Returns:
            Set of ``(exchange, native_symbol)`` tuples. Empty set when
            ``operator_public_ids`` is empty, when no active grants are
            held, or when all reachable instruments lack active
            ``Instrument`` / ``Symbol`` rows.
        """
        ...

    @abstractmethod
    async def revoke_scope_grant(
        self,
        grant_public_id: str,
        revoked_by_user_public_id: str,
        revoked_at: datetime,
        reason: str | None,
    ) -> ScopeGrantRow:
        """SCD2-close an active scope grant in place (no replacement row).

        Differs from ``handover_grant`` in that no new grant row is
        inserted — this is a terminal close. The SCD2 close sets the
        row's ``known_to`` to ``revoked_at`` while preserving
        ``timestamp`` and all scope columns, so audit queries can still
        reconstruct the grant's lifetime.

        The per-wallet advisory lock (``pg_advisory_xact_lock`` on
        PostgreSQL, no-op on SQLite) serializes against concurrent
        ``handover_grant`` / ``create_scope_grant`` on the same wallet.
        A concurrent second revoke on the same grant is rejected as
        ``ScopeGrantNotFoundError`` once the first commit lands (the
        active-row predicate in the SELECT excludes it).

        Args:
            grant_public_id: Public ID of the active grant to revoke.
            revoked_by_user_public_id: Audit identity of the ADMIN user
                performing the revocation (NOT written to the row; the
                caller passes it for authorization + logging; the event
                payload carries it to subscribers).
            revoked_at: Bus-time for the SCD2 close.
            reason: Optional free-form audit note (NOT persisted to the
                closed row; the handover pattern writes ``note`` on the
                NEW row, but a revoke has no new row — the reason flows
                to the ``admin.scope_revoked`` event payload only).

        Returns:
            ``ScopeGrantRow`` projection of the row as it exists
            immediately after the close (``known_to == revoked_at``).

        Raises:
            ScopeGrantNotFoundError: Grant public_id does not exist OR
                is no longer active at ``revoked_at`` (double-revoke).
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
        Used by the admin wallet creation endpoint
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

        Used by the wallet catalogue endpoint (ADMIN only
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
        principals. The result is the set union over all operator IDs
        a wallet is accessible if ANY of the principal's operators
        holds an active ``wallet_operator_scope_grants`` row on it.
        This matches the wallet picker contract: the picker
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

    @abstractmethod
    async def upsert_notification_device(self, row: NotificationDeviceUpsertRow) -> str:
        """Register or refresh an iOS device via SCD2 close-and-insert.

        Idempotent on ``device_token``: when an active row already
        exists it is SCD2-closed (``known_to := row.timestamp``) and
        a new active version is inserted, reusing the stable
        ``public_id`` across versions. Caller provides ``session_id``,
        ``sequence_id``, and ``timestamp`` (bus time).

        Returns:
            The row's stable ``public_id``.
        """
        ...

    @abstractmethod
    async def list_active_notification_devices_for_user(
        self, user_public_id: str
    ) -> list[NotificationDeviceRow]:
        """Active (``known_to == KNOWN_TO_MAX``) devices owned by user."""
        ...

    @abstractmethod
    async def list_active_notification_devices_for_users(
        self, user_public_ids: list[str]
    ) -> dict[str, list[NotificationDeviceRow]]:
        """Bulk active-device lookup keyed by user_public_id.

        Bulk variant of :meth:`list_active_notification_devices_for_user`
        for the notify sidecar's drain/retry hot path — replaces the
        per-row round-trip with one ``WHERE user_public_id IN (...)``
        SELECT. Missing user_public_ids map to empty lists.
        """
        ...

    @abstractmethod
    async def deactivate_notification_device_scd2(
        self,
        public_id: str,
        *,
        reason: str,
        timestamp: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """SCD2 close + insert a tombstone successor row.

        Closes the active row (``known_to := timestamp``) and inserts
        a new row with the same ``public_id`` and device metadata but
        ``token_status = reason`` (``"unregistered"`` for APNs 410,
        ``"user_unregistered"`` for explicit user deregistration from
        the app). The successor keeps ``known_to = KNOWN_TO_MAX`` so
        ``as_of`` queries after the close see a visible inactive
        device row instead of a gap (invariant INV-9).

        Idempotent: returns ``False`` when no active row exists for
        ``public_id`` (deactivation already ran); returns ``True`` on
        a successful close + insert.

        Args:
            public_id: The device's stable public identifier.
            reason: New ``token_status`` value for the successor —
                one of the ``ck_notification_devices_token_status``
                permitted values (except ``"active"``).
            timestamp: Transition time — used for both the closed
                row's ``known_to`` and the successor's ``timestamp``.
            session_id: Provenance for the successor row.
            sequence_id: Provenance for the successor row.
        """
        ...

    @abstractmethod
    async def list_device_alert_prefs_for_user(
        self, user_public_id: str
    ) -> list[DeviceAlertPrefRow]:
        """Active prefs for the user's currently active devices."""
        ...

    @abstractmethod
    async def upsert_device_alert_pref(self, row: DeviceAlertPrefUpsertRow) -> str:
        """SCD2 close + insert on (device, alert_type, scope_tuple).

        Scope is inferred from which of ``operator_public_id`` /
        ``wallet_public_id`` are populated in the row. Merges
        optional fields with the prior active row if present.
        Returns the stable ``public_id`` (preserved across SCD2
        versions) so the caller can synthesize a response without an
        extra read that races against other writers on the same scope
        tuple (avoids the post-upsert race).
        """
        ...

    @abstractmethod
    async def deactivate_device_alert_pref_scd2(
        self,
        pref_public_id: str,
        *,
        device_public_id: str,
        timestamp: datetime,
    ) -> DeviceAlertPrefRow | None:
        """Close one active ``device_alert_prefs`` row in place (SCD2).

        Unlike ``deactivate_notification_device_scd2`` the prefs table
        carries no ``token_status`` discriminator, so the close is a
        bare ``known_to := timestamp`` on the active row — no successor
        is inserted. The row remains queryable as-of historical
        instants but drops out of the ``known_to == KNOWN_TO_MAX``
        active set, releasing the partial-unique-index slot for a
        future re-create at the same scope tuple.

        Args:
            pref_public_id: Target preference row to close.
            device_public_id: Owning device — guards against a foreign
                pref id being smuggled past the route's ownership
                check; mismatch returns ``None`` (404 at the route).
            timestamp: Transition time stamped onto ``known_to``.

        Returns:
            The closed row's pre-close projection
            (``DeviceAlertPrefRow``) so callers can synthesize a
            response without a follow-up read; ``None`` when no
            active row matches both ``pref_public_id`` and
            ``device_public_id`` (already closed, or wrong device).
        """
        ...

    @abstractmethod
    async def list_user_alert_defaults(self, user_public_id: str) -> list[UserAlertDefaultRow]:
        """Active user-level fallback prefs per alert type."""
        ...

    @abstractmethod
    async def upsert_user_alert_default(self, row: UserAlertDefaultUpsertRow) -> str:
        """SCD2 close + insert on (user, alert_type) default pref.

        Returns the stable ``public_id`` (preserved across SCD2
        versions) so the caller can synthesize a response without an
        extra read that races against other writers on the same
        (user, alert_type) key.
        """
        ...

    @abstractmethod
    async def insert_alert_event(self, row: AlertEventInsertRow) -> str:
        """Insert a temporal (SCD2) alert_events row.

        Auto-fills ``public_id`` (uuid7) and ``known_to``
        (``KNOWN_TO_MAX``) if absent. Required: ``session_id``,
        ``sequence_id``, ``timestamp``, ``user_public_id``,
        ``alert_type``, ``priority``, ``title``, ``body``.

        Returns:
            The row's ``public_id``.
        """
        ...

    @abstractmethod
    async def list_recent_alerts_for_user(
        self,
        user_public_id: str,
        limit: int,
        before: AlertListCursor | None,
    ) -> list[AlertEventRow]:
        """Active alert_events for iOS Alerts tab with composite pagination.

        ``before`` is an opaque ``AlertListCursor`` carrying the anchor
        row's ``(timestamp, public_id)`` — the repo applies the keyset
        predicate directly from those values, never re-derives them
        from the current active row, so pagination is stable even if
        AlertEvent rows are SCD2-revised between fetches. Rows are
        returned strictly earlier than the cursor, ordered
        ``(timestamp DESC, public_id DESC)``.
        """
        ...

    @abstractmethod
    async def get_alert_event_by_public_id(self, public_id: str) -> AlertEventRow | None:
        """Active alert_event by public_id; None when missing or SCD2-closed."""
        ...

    @abstractmethod
    async def get_alert_events_by_public_ids(
        self, public_ids: list[str]
    ) -> dict[str, AlertEventRow]:
        """Bulk active-alert-event lookup keyed by public_id.

        Bulk variant of :meth:`get_alert_event_by_public_id` for the
        notify sidecar's drain/retry hot path. Missing public_ids are
        absent from the returned dict (not mapped to None).
        """
        ...

    @abstractmethod
    async def list_alert_events_with_dedup_key(
        self,
        user_public_id: str,
        dedup_key: str,
        since: datetime,
    ) -> list[AlertEventRow]:
        """Active ``alert_events`` matching user+dedup_key emitted at/after ``since``.

        Used by the notify sidecar's rule-side dedup helper to
        suppress duplicate alerts within a per-rule
        ``suppression_window_seconds`` — the table's
        ``ix_alert_events_dedup`` composite index
        (``user_public_id, dedup_key, timestamp``) drives this query.

        Empty list when no matching events exist. ``since = now`` of
        the rule evaluation minus the configured window; a window of
        0 always yields an empty list (rules that set
        ``suppression_window_seconds = 0`` rely on index-side
        defence-in-depth rather than rule-side pre-check).

        Args:
            user_public_id: Recipient user UUID7 — scope cut per the
                index's leading column.
            dedup_key: Rule-minted suppression key
                (``f"{alert_type}.{logical_entity_id}"``).
            since: Lower bound on ``timestamp`` (inclusive). Rows
                older than ``since`` are excluded.
        """
        ...

    @abstractmethod
    async def get_default_languages_for_users(
        self, user_public_ids: list[str]
    ) -> dict[str, str | None]:
        """Bulk ``default_language`` lookup keyed by ``user_public_id``.

        Used by the notify sidecar's drain / retry path to localize
        APNs ``aps.alert.title`` / ``body`` per recipient before emit.
        One ``WHERE public_id IN (...)`` SELECT against the active
        ``users`` row replaces the per-row round-trip; missing
        ``user_public_id``s map to ``None`` so callers can treat
        absent / never-set preferences identically.

        Args:
            user_public_ids: List of user ``public_id`` UUIDs to look
                up. Duplicates are deduplicated; empty input returns
                an empty dict without touching the DB.

        Returns:
            Mapping ``{user_public_id: default_language | None}``. The
            value is ``None`` both when the user row exists with
            ``default_language = NULL`` AND when the user has no
            active row at all — the sidecar treats both as "emit
            English" so the distinction is irrelevant downstream.
        """
        ...

    @abstractmethod
    async def list_users_with_permission(self, permission: str) -> list[str]:
        """Active user ``public_id``s whose role grants ``permission``.

        Used by the notify sidecar's ``critical_system_error`` rule
        to fan the alert out to every admin — the
        rule emits one ``AlertEventInsertRow`` per returned user_id.
        Membership is derived from ``auth.domain.permissions.ROLE_PERMISSIONS``
        so the calling rule doesn't hard-code which roles count as
        "admin" (adding / removing roles remains a pure permissions
        change, no sidecar re-plumbing).

        Args:
            permission: ``snapper.auth.domain.permissions.Permission``
                value (string form — ``Permission.READ_SYSTEM_STATUS``
                serializes to ``"read:system_status"``).

        Returns:
            Newest-first list of user public_ids. Empty list when no
            active user holds a role granting the permission.
        """
        ...

    @abstractmethod
    async def insert_alert_delivery(self, row: AlertDeliveryInsertRow) -> str:
        """SCD2 insert of a new alert_delivery row.

        Scope columns (user/operator/wallet) MUST be denormalised from
        the source alert_event at queue time and provided in the row —
        they are what the scope-cancel path filters on.
        """
        ...

    @abstractmethod
    async def list_queued_deliveries_all(self) -> list[AlertDeliveryRow]:
        """Every active ``status='queued'`` delivery row (no time filter)."""
        ...

    @abstractmethod
    async def get_delivery_by_public_id(self, public_id: str) -> AlertDeliveryRow | None:
        """Return the active SCD2 version of one delivery by public_id, or None.

        Indexed lookup used by the notify sidecar's hot retry path —
        avoids the per-row linear scan over
        :meth:`list_queued_deliveries_all`.
        """
        ...

    @abstractmethod
    async def list_deliveries_ready_for_retry(self, now: datetime) -> list[AlertDeliveryRow]:
        """Active queued deliveries with ``next_attempt_at`` NULL or <= ``now``."""
        ...

    @abstractmethod
    async def mark_delivery_sent(
        self,
        public_id: str,
        apns_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> sent (guarded on current status=queued).

        Closes the active row and inserts a new version with
        ``status='sent'`` + ``apns_id``. No-op when the current
        active row is not queued (optimistic concurrency guard).
        """
        ...

    @abstractmethod
    async def mark_delivery_failed(
        self,
        public_id: str,
        error_reason: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> failed (terminal, give-up after N retries)."""
        ...

    @abstractmethod
    async def mark_delivery_unregistered(
        self,
        public_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> unregistered (APNs 410)."""
        ...

    @abstractmethod
    async def mark_delivery_cancelled(
        self,
        public_id: str,
        reason: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> cancelled_scope."""
        ...

    @abstractmethod
    async def update_delivery_retry_schedule(
        self,
        public_id: str,
        attempt_count: int,
        next_attempt_at: datetime | None,
        error_reason: str | None,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """SCD2 close + insert, status remains queued, bumps attempt_count.

        Called BEFORE the APNs HTTP call (crash-safety) so a
        mid-flight sidecar restart leaves a row with incremented
        attempt_count that is still retriable — bounded ≤1 duplicate
        send per crash.

        Returns:
            True on successful transition; False when the active row
            is no longer ``queued`` (race guard — a scope-
            revoke cancel raced the retry-loop bump).
        """
        ...

    @abstractmethod
    async def list_users_with_operator_membership(
        self, operator_public_id: str, as_of: datetime
    ) -> list[str]:
        """User public_ids with active membership in ``operator_public_id`` at ``as_of``."""
        ...

    @abstractmethod
    async def is_scope_grant_active(
        self,
        user_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
        as_of: datetime,
    ) -> bool:
        """True iff a matching grant AND active user membership exist at ``as_of``."""
        ...

    @abstractmethod
    async def has_grant_for_delegate(
        self,
        *,
        delegate_public_id: str,
        wallet_public_id: str,
        instrument_public_id: str,
        as_of: datetime,
    ) -> bool:
        """AI delegate scope check with underlying expansion.

        Resolves ``ai_delegates.public_id -> user_public_id``, then
        looks up the delegate's operator memberships
        (``UserOperatorMembership``), and finally checks whether ANY
        active ``wallet_operator_scope_grants`` row covers the
        requested ``(wallet_public_id, instrument_public_id)`` pair —
        including ``scope_kind='underlying'`` grants expanded via
        ``instrument_underlying_mappings`` to match the requested
        instrument's underlying.

        Used by:
        - ``submit_ai_review_decision`` MCP tool (auth load + scope
          check).
        - Per-frame ``enforce_ai_review_scope`` filter for
          ``ai_reviews.*`` WS topic family.
        - ``AiReviewService.create_review`` for delegate eligibility
          query (admission control candidate list).

        Args:
            delegate_public_id: ``ai_delegates.public_id`` (NOT the
                delegate's ``users.public_id``; the operational table
                holds its own UUID7).
            wallet_public_id: Wallet to check.
            instrument_public_id: Instrument to check (matched
                directly for ``scope_kind='instrument'`` grants OR
                via underlying mapping for ``scope_kind='underlying'``).
            as_of: Point-in-time temporal query.

        Returns:
            True iff some operator member of the delegate's user
            holds an active grant covering the (wallet, instrument).
        """
        ...

    @abstractmethod
    async def cancel_pending_deliveries_for_scope(
        self,
        user_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int:
        """Bulk-cancel queued deliveries by scope columns on the delivery row.

        Filters on the denormalised ``user_public_id`` /
        ``operator_public_id`` / ``wallet_public_id`` columns on
        ``alert_deliveries`` — does NOT join ``alert_events`` — so
        SCD2 corrections on the source event cannot cause misses.

        Returns:
            Count of rows transitioned to ``cancelled_scope``.
        """
        ...

    @abstractmethod
    async def cancel_pending_deliveries_for_user(
        self,
        user_public_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
        error_reason: str = "user_deactivated",
    ) -> int:
        """Bulk-cancel every queued delivery for a user (admin kill-switch).

        Used by the notify sidecar's ``admin.user_deactivated``
        subscriber. The ``error_reason`` column is stamped with the
        transition cause so audit replay can distinguish this from
        a scope-revoke cancellation.

        Returns:
            Count of rows transitioned to ``cancelled_scope``.
        """
        ...

    @abstractmethod
    async def count_deliveries_by_status(self) -> dict[str, int]:
        """Aggregate counts of ``alert_deliveries`` rows per ``status``.

        Covers the ``GET /api/metrics/notifications`` endpoint —
        returns a dict keyed by ``status`` (``queued`` / ``sent`` /
        ``failed`` / ``unregistered`` / ``cancelled_scope``). Status
        values absent from the DB are also absent from the returned
        dict; callers use ``.get(status, 0)`` for a zero-default.
        """
        ...

    @abstractmethod
    async def get_ai_review(self, review_public_id: str) -> AiReviewRow | None:
        """Fetch a single :class:`AiReview` row by public_id.

        Returns ``None`` if no row exists. Used by:
        - ``submit_decision`` step 2 (load + scope check).
        - ``await_ai_review`` poll fallback (DB-backed terminal
          status check on bus-loss / restart).
        - ``GET /api/ai-reviews/pending`` REST endpoint.
        """
        ...

    @abstractmethod
    async def insert_ai_review(self, row: AiReviewInsertRow) -> str:
        """INSERT new :class:`AiReview` row; returns ``public_id``.

        Used by ``AiReviewService.create_review`` step 2e (ATOMIC
        transaction with admission counter increment +
        ``ai_review_events`` append).
        """
        ...

    @abstractmethod
    async def get_ai_delegate_by_user_public_id(self, user_public_id: str) -> AiDelegateRow | None:
        """Lookup operational :class:`AiDelegate` row by FK to users.

        Used by:
        - WS authenticate handler (populate
          ``AuthPrincipal.delegate_public_id`` for AI_DELEGATE users).
        - ``submit_decision`` caller delegate resolution.

        Returns ``None`` if no operational row exists. Strategy
        layer creates the row when the AI_DELEGATE user is minted
        (per :class:`UserService.create_ai_delegate`).
        """
        ...

    @abstractmethod
    async def insert_ai_delegate(
        self,
        *,
        public_id: str,
        user_public_id: str,
        as_of: datetime,
    ) -> str:
        """INSERT new :class:`AiDelegate` operational row; returns ``public_id``.

        Called by ``UserService.create_ai_delegate`` in same DB
        transaction as the new ``users`` row insert. Pre-existing
        AI_DELEGATE users get backfilled by data migration step in
        0001_init.py at next prod cutover.
        """
        ...

    @abstractmethod
    async def update_delegate_last_seen(
        self, delegate_public_id: str, last_seen_at: datetime
    ) -> None:
        """Update ``ai_delegates.last_seen_at`` for reconnect hysteresis.

        Called by :class:`WebSocketAuthManager` on:
        - WS connection upgrade (initial).
        - ``authenticate`` frame (post-reauth).
        - ``system.heartbeat.client`` frame
          (``heartbeat_interval ≤ window/2``, default 7s).
        """
        ...

    @abstractmethod
    async def list_eligible_delegates_for_ai_review(
        self,
        *,
        operator_public_id: str,
        wallet_public_id: str,
        instrument_public_id: str,
        heartbeat_window_seconds: int,
        as_of: datetime,
    ) -> list[AiDelegateRow]:
        """Eligible AI-delegate candidates for admission control.

        Returns the ``ai_delegates`` rows whose users are active members of
        ``operator_public_id`` (``users.is_active = TRUE``,
        ``users.role = AI_DELEGATE``) and whose ``last_seen_at`` falls inside
        the heartbeat window (``as_of - heartbeat_window_seconds``,
        ``as_of``], ordered by ``last_seen_at DESC``. Pre-checks that the
        operator actually holds a matching active scope grant
        (instrument-direct OR underlying-expanded via
        ``InstrumentUnderlyingMapping``) for the ``(wallet, instrument)``
        tuple — when no grant exists the list is empty regardless of how
        many delegates are live.

        Used by :meth:`AiReviewService.create_review` admission control to
        compose the candidate list passed to
        :meth:`claim_and_insert_ai_review`. The order returned is the order
        the service tries to claim — most-recently-seen first, matching the
        freshness preference.

        Args:
            operator_public_id: Operator the strategy is consulting under.
                Delegates must have an active ``UserOperatorMembership`` to
                this operator AND that operator must hold the scope grant.
            wallet_public_id: Wallet the eventual trade would settle on.
            instrument_public_id: Instrument the strategy is signalling on.
            heartbeat_window_seconds: Liveness threshold (default 15s;
                the service passes the configured value through).
            as_of: Wall-clock used for SCD2-active filtering on users,
                memberships, grants, and underlying mappings.

        Returns:
            Eligible :class:`AiDelegateRow` rows ordered by
            ``last_seen_at DESC``. Empty when the operator has no matching
            grant, no AI_DELEGATE members, or no live members.
        """
        ...

    @abstractmethod
    async def claim_and_insert_ai_review(
        self,
        *,
        candidate_delegate_public_ids: list[str],
        review_data: AiReviewInsertRow,
        event_data: AiReviewEventInsertRow,
        now: datetime,
    ) -> str | None:
        """Atomic claim + INSERT for AI-review admission control.

        Iterates ``candidate_delegate_public_ids`` in order. For each, runs
        a CAS UPDATE on ``ai_delegates`` setting
        ``active_reviews_count = active_reviews_count + 1`` with predicate
        ``active_reviews_count = 0``. The first candidate whose UPDATE
        affects a row is the claim winner: the same transaction then
        INSERTs the ``ai_reviews`` row (with ``selected_delegate_public_id``
        rewritten to the claimed candidate) plus the ``ai_review_events``
        ``created`` audit row, and COMMITs.

        Single-transaction semantics: an INSERT failure rolls the whole
        transaction back including the claim, so the counter never leaks.
        Earlier candidates that lost the CAS (rowcount=0) made no data
        change so there is nothing to roll back for them either.

        Args:
            candidate_delegate_public_ids: Ordered candidates from
                :meth:`list_eligible_delegates_for_ai_review`. The caller
                MUST handle the empty-list case (no live delegate) by
                raising before invoking this method — passing an empty list
                here returns ``None`` and is indistinguishable from the
                all-busy outcome.
            review_data: :class:`AiReviewInsertRow`. The
                ``selected_delegate_public_id`` field is overwritten with
                the actually-claimed candidate before INSERT.
            event_data: :class:`AiReviewEventInsertRow` for the matching
                ``created`` audit row. ``review_public_id`` MUST already
                point to ``review_data["public_id"]``.
            now: Wall-clock used for ``ai_delegates.updated_at`` on the
                winning claim.

        Returns:
            The claimed ``ai_delegates.public_id`` on success; ``None``
            when every candidate's CAS lost (all busy).
        """
        ...

    @abstractmethod
    async def list_expired_pending_reviews(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> list[PendingReviewSummary]:
        """Reaper input: pending/fanout_dispatched rows past deadline.

        Snapshot read used by :meth:`AiReviewService._reaper_tick` to
        drive per-row atomic timeouts. The reaper trusts that any row
        observed here may already have raced against a peer decision /
        supersede / earlier reaper tick by the time the per-row
        :meth:`atomic_timeout_review_with_audit_and_counter` fires; the
        CAS shape of that method is the actual correctness guarantee.

        Args:
            now: Wall-clock the reaper is reaping against.
            limit: Cap on the number of rows returned per tick. Default
                100 keeps a single tick bounded under load surges.

        Returns:
            Up to ``limit`` :class:`PendingReviewSummary` rows ordered by
            ``deadline ASC`` (oldest first) so the reaper drains the
            backlog in a fair order.
        """
        ...

    @abstractmethod
    async def list_offline_pending_reviews(
        self,
        *,
        now: datetime,
        heartbeat_window_seconds: int,
        limit: int = 100,
    ) -> list[PendingReviewSummary]:
        """Offline scanner input: pending past fanout_after with stale delegate.

        Returns ``ai_reviews`` rows that are still ``status='pending'``
        AND whose ``fanout_after`` has elapsed AND whose
        ``selected_delegate_public_id`` has either never connected
        (``last_seen_at IS NULL``) or has gone silent
        (``last_seen_at < now - heartbeat_window``). Rows already in
        ``fanout_dispatched`` are excluded — only fresh pending rows
        get re-fanned out.

        Args:
            now: Wall-clock used for ``fanout_after`` / heartbeat window
                comparisons.
            heartbeat_window_seconds: Seconds of silence after which
                the delegate is considered offline (default 15s; the
                service passes the configured value through).
            limit: Cap on the number of rows returned per tick.

        Returns:
            Up to ``limit`` :class:`PendingReviewSummary` rows.
        """
        ...

    @abstractmethod
    async def list_pending_reviews_for_delegate(
        self,
        *,
        selected_delegate_public_id: str,
        now: datetime,
        wallet_public_id: str | None = None,
        limit: int = 100,
    ) -> list[PendingReviewSummary]:
        """Fast-path input: pending rows for one delegate past fanout.

        Used by :meth:`AiReviewService.handle_delegate_offline_bus_message`
        to snapshot the affected reviews when a ``bus.delegate_offline``
        event lands AND by the
        ``GET /api/ai-reviews/pending`` REST endpoint for bridge
        catch-up after WS reconnect. Mirrors
        :meth:`list_offline_pending_reviews` shape: the
        ``fanout_after < now`` predicate is REQUIRED so the fast
        path does not dispatch fanout before the natural fanout timer
        elapses (the :class:`DelegateOfflineData` schema docstring
        explicitly says subscribers compare ``last_seen_at`` to
        ``ai_reviews.fanout_after`` and either dispatch immediately
        OR wait for the natural fanout timer — which the offline
        scanner provides). Status filter excludes ``fanout_dispatched``
        and terminal states so the same row never gets re-fanned twice
        (idempotent if the Layer 2 scanner already fired).

        Args:
            selected_delegate_public_id: ``ai_delegates.public_id`` to
                scan for.
            now: Wall-clock used for the ``fanout_after`` cutoff —
                bus subscribers pass ``msg.last_seen_at`` from the
                ``bus.delegate_offline`` event so the gate matches
                the instant the delegate actually went offline; the
                REST surface passes ``datetime.now(UTC)`` since the
                live wall-clock is the right reference for
                catch-up polls.
            wallet_public_id: Optional filter — when provided,
                narrows the snapshot to one wallet (the bridge
                passes this for wallet-scoped catch-up). When
                ``None``, returns every wallet the delegate is
                assigned to.
            limit: Cap on returned rows per call.

        Returns:
            Up to ``limit`` :class:`PendingReviewSummary` rows.
        """
        ...

    @abstractmethod
    async def atomic_resolve_review_with_audit_and_counter(
        self,
        *,
        review_public_id: str,
        decision: str,
        responding_delegate_public_id: str,
        rationale: str | None,
        new_status: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> AtomicResolveResult | None:
        """Single-transaction resolve + audit + counter decrement.

        A 3-call sequence ``atomic_resolve_ai_review`` ->
        ``insert_ai_review_event`` ->
        ``decrement_delegate_active_count_for_review`` would run across
        THREE separate DB transactions, so a process crash between any
        two would leave the row in an inconsistent state (terminal
        status without audit row OR terminal status + audit but counter
        still elevated, blocking future admission control for the
        responding delegate).

        Folds every step into ONE transaction:

        1. ``SELECT ... FOR UPDATE`` on the ``ai_reviews`` row to capture
           ``previous_status`` + ``deadline`` + ``dispatch_version`` AND
           hold the row lock against concurrent peers. Avoids the
           previous_status SELECT-then-CAS race on engines that
           support row locking; SQLite serialises the entire transaction
           via the connection-level write lock so the same invariant
           holds.
        2. CAS UPDATE ``ai_reviews`` from non-terminal to ``new_status``
           with the deadline gate. Returns ``None`` when peer beat us
           OR deadline elapsed.
        3. INSERT the audit-event row using the captured
           ``previous_status`` (the caller's ``audit_event["previous_status"]``
           is overwritten by the actual SELECT-FOR-UPDATE value so a
           concurrent transition cannot record a stale predecessor).
        4. Counter decrement claim: CAS UPDATE
           ``ai_reviews.counter_decremented_at`` from NULL to ``now``.
        5. Counter decrement: UPDATE
           ``ai_delegates.active_reviews_count`` via portable
           ``CASE WHEN active_reviews_count > 0 THEN active_reviews_count - 1
           ELSE 0 END`` on the claim winner (works on both SQLite and
           PostgreSQL).
        6. ``COMMIT``.

        Caller MUST still publish the post-commit bus event +
        external WS frame; this primitive owns only the DB-side
        atomicity.

        Args:
            review_public_id: UUID7 of the ``ai_reviews`` row.
            decision: ``"approve"`` or ``"reject"``.
            responding_delegate_public_id: ``ai_delegates.public_id`` of
                the responding delegate (also wins the audit event's
                actor field if the caller threads it onto
                ``audit_event``).
            rationale: Optional free-text decision rationale.
            new_status: ``"resolved_approved"`` or ``"resolved_rejected"``
                — must match the ``decision`` per the
                ``ck_ai_reviews_status_consistency`` constraint.
            audit_event: :class:`AiReviewEventInsertRow` with at minimum
                ``public_id``, ``review_public_id``, ``event_type``,
                ``actor_delegate_public_id``, ``new_status``, ``payload``,
                ``occurred_at``. The ``previous_status`` field is
                OVERWRITTEN by the SELECT-FOR-UPDATE result so callers
                can pass any sentinel value.
            now: Wall-clock used for ``resolved_at`` / ``updated_at`` /
                ``counter_decremented_at`` and for the deadline gate
                comparison.

        Returns:
            :class:`AtomicResolveResult` (with the captured
            ``previous_status``) on a winning transition; ``None`` when
            the row was already terminal OR ``deadline <= now``.
        """
        ...

    @abstractmethod
    async def atomic_timeout_review_with_audit_and_counter(
        self,
        *,
        review_public_id: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> AtomicResolveResult | None:
        """Single-transaction timeout + audit + counter decrement.

        Mirrors :meth:`atomic_resolve_review_with_audit_and_counter` minus
        the deadline gate and the decision/responding_delegate fields:
        used by the strategy await-loop's late-decision shortcut, the
        reaper tick, and the offline scanner's terminal-state branches.

        Args:
            review_public_id: UUID7 of the ``ai_reviews`` row.
            audit_event: :class:`AiReviewEventInsertRow`. The
                ``previous_status`` field is OVERWRITTEN by the
                SELECT-FOR-UPDATE result.
            now: Wall-clock used for ``resolved_at`` / ``updated_at`` /
                ``counter_decremented_at``.

        Returns:
            :class:`AtomicResolveResult` on a winning transition;
            ``None`` when the row was already terminal.
        """
        ...

    @abstractmethod
    async def atomic_supersede_review_with_audit_and_counter(
        self,
        *,
        review_public_id: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> AtomicResolveResult | None:
        """Single-transaction supersede + audit + counter decrement.

        Mirrors :meth:`atomic_timeout_review_with_audit_and_counter`
        with ``new_status='superseded'`` + ``resolution_mode='superseded_by_strategy'``.
        Used by the strategy-abandon supersede path.

        Args:
            review_public_id: UUID7 of the ``ai_reviews`` row.
            audit_event: :class:`AiReviewEventInsertRow`. The
                ``previous_status`` field is OVERWRITTEN by the
                SELECT-FOR-UPDATE result.
            now: Wall-clock used for ``resolved_at`` / ``updated_at`` /
                ``counter_decremented_at``.

        Returns:
            :class:`AtomicResolveResult` on a winning transition;
            ``None`` when the row was already terminal.
        """
        ...

    @abstractmethod
    async def atomic_dispatch_fanout_with_audit(
        self,
        *,
        review_public_id: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> int | None:
        """Single-transaction fanout dispatch + audit.

        On the fanout dispatch path: a 2-call sequence
        ``atomic_dispatch_fanout`` -> ``insert_ai_review_event`` would
        run across TWO separate transactions, so a crash between them
        would leave the row in ``fanout_dispatched`` without the
        matching ``fanout_dispatched`` audit-event row.

        Folds both steps into ONE transaction. The ``audit_event``'s
        ``payload`` field has its ``dispatch_version`` slot OVERWRITTEN
        by the actual incremented version returned from the UPDATE so
        callers cannot record a stale version on the audit row even if
        the row gets re-fanned out concurrently between scanners.

        Args:
            review_public_id: UUID7 of the ``ai_reviews`` row.
            audit_event: :class:`AiReviewEventInsertRow`. The
                ``payload["dispatch_version"]`` slot is OVERWRITTEN by
                the UPDATE-incremented version (any caller-supplied
                value is replaced).
            now: Wall-clock used for ``updated_at`` /
                ``occurred_at``.

        Returns:
            The new ``dispatch_version`` on a winning CAS transition;
            ``None`` when the row was no longer ``pending`` (peer
            fanout or terminal transition won).
        """
        ...

    @abstractmethod
    async def count_table_stats(
        self,
        entry: TableEntry,
        *,
        archivable_window: tuple[date, date] | None = None,
    ) -> TableCounters:
        """Per-table four-counter primitive for the DB-stats counter.

        Returns ``TableCounters(total, current, closed, archivable)`` for
        a single table. Per-kind semantics:

        * ``entry.kind == "event"`` (append-only): ``total`` = ``COUNT(*)``;
          ``current`` and ``closed`` are ``None`` (no SCD2 lifecycle —
          ``0`` would imply the dimension exists). ``archivable`` is
          ``COUNT(timestamp >= window_start AND timestamp < window_end + 1d)``
          when ``archivable_window`` is provided, else ``None``.
        * ``entry.kind == "state"`` (SCD2-versioned):
          ``current = COUNT(known_to == KNOWN_TO_MAX)``,
          ``closed = COUNT(known_to != KNOWN_TO_MAX)``,
          ``total = current + closed`` (Python addition trusted on the
          SCD2 invariant). ``archivable`` is the closed-only count over
          the same half-open ``timestamp`` window when
          ``archivable_window`` is provided, else ``None``.

        Args:
            entry: Table descriptor (name, kind, ORM model).
            archivable_window: Inclusive ``(day_start, day_end)`` pair
                from
                :func:`snapper.application.retention.window.compute_retention_window`,
                or ``None`` when no retention policy applies. The
                concrete query emits a HALF-OPEN timestamp predicate
                (``>= day_start midnight UTC AND < day_end + 1d midnight UTC``).

        Returns:
            :class:`TableCounters` with all four fields populated per
            the per-kind semantics above.
        """
        ...

    @abstractmethod
    async def get_market_data_coverage(
        self,
        *,
        tick_window_seconds: int,
        candle_window_seconds: int,
        now: datetime | None = None,
    ) -> list[MarketDataCoverageRow]:
        """Per-exchange market-data coverage over active instruments.

        See :meth:`SQLAlchemyRepository.get_market_data_coverage` for
        the concrete cross-dialect implementation and semantics.

        Args:
            tick_window_seconds: Freshness window for ``ticks`` rows; a
                tick newer than ``now - tick_window_seconds`` is fresh.
            candle_window_seconds: Freshness window for ``candles`` rows
                (compared against ``open_at``).
            now: Reference instant for all freshness + active-row
                predicates; defaults to ``datetime.now(UTC)``. Injectable
                so tests can assert exact cutoff boundaries deterministically.

        Returns:
            One :class:`MarketDataCoverageRow` per exchange, ordered by
            exchange.
        """
        ...

    @abstractmethod
    async def upsert_instrument_feed_health(
        self, rows: list[InstrumentFeedHealthUpsertRow]
    ) -> None:
        """Upsert current-state per-symbol feed-health snapshot rows.

        Last-write-wins on the ``(coordinator, exchange, channel,
        symbol)`` natural key: an existing row is overwritten with the
        latest snapshot, a new key inserts a row. See
        :meth:`SQLAlchemyRepository.upsert_instrument_feed_health` for
        the cross-dialect implementation.

        Args:
            rows: Feed-health snapshot rows to persist. Empty list is a
                no-op.

        Returns:
            None.
        """
        ...

    @abstractmethod
    async def list_instrument_feed_health(
        self, *, exchange: str | None = None, fresh_within_seconds: int | None = None
    ) -> list[InstrumentFeedHealthRow]:
        """List current-state feed-health rows, newest snapshot first.

        See :meth:`SQLAlchemyRepository.list_instrument_feed_health` for the
        staleness semantics behind ``fresh_within_seconds``.

        Args:
            exchange: Optional exchange filter (lowercase). When ``None``
                every exchange's rows are returned.
            fresh_within_seconds: When set, return only rows whose
                ``snapshot_at`` is within this many seconds of now. ``None``
                returns all rows.

        Returns:
            One :class:`InstrumentFeedHealthRow` per active natural key,
            ordered by ``(exchange, channel, symbol)``.
        """
        ...


def _derive_resolve_resolution_mode(
    *,
    previous_status: str,
    selected_delegate_public_id: str,
    responding_delegate_public_id: str,
) -> str:
    """Derive ``resolution_mode`` for the resolve transition.

    Enum resolution rules:

    - ``pending`` + selected==responding -> ``pick_one_primary`` (most
      common path: the originally-selected delegate responds within
      the natural fanout window).
    - ``fanout_dispatched`` + selected==responding -> ``secondary_after_fanout``
      (the originally-selected delegate came back online after
      fanout had already fired to peers).
    - ``fanout_dispatched`` + selected!=responding -> ``fanout_first_responder``
      (a different eligible delegate won the fanout race).

    Lives at the repository module level (not on a service or enum
    class) so the resolve primitive can derive it INSIDE its own
    SELECT-FOR-UPDATE transaction without crossing a layer boundary
    or duplicating the rules. The service must not compute
    ``resolution_mode`` from a pre-snapshot ``status`` that could
    disagree with the actually-locked predecessor under concurrent
    fanout.
    """
    if previous_status == "pending":
        return "pick_one_primary"
    if responding_delegate_public_id == selected_delegate_public_id:
        return "secondary_after_fanout"
    return "fanout_first_responder"


_SQLITE_CONNECT_PRAGMAS: tuple[str, ...] = (
    "PRAGMA foreign_keys=ON",
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA busy_timeout=30000",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA cache_size=-65536",
    "PRAGMA wal_autocheckpoint=4000",
    "PRAGMA mmap_size=268435456",
)
"""Per-connection SQLite PRAGMAs for safe writer throughput.

Tuned 2026-05-15 against the live publisher workload. The
"mainstream defaults" alone (``foreign_keys`` / WAL /
``synchronous=NORMAL`` / ``busy_timeout``) give ~50% drop-rate
reduction but the cache + WAL checkpoint + mmap knobs are needed
for another ~50% to land at the ~1250 writes/sec ceiling. Pulling
the tuning out cut DB throughput from ~1258/sec back to ~446/sec
under identical load — measurable regression on the same hardware
the operator actually runs on, so we keep the full pack.

* ``foreign_keys=ON`` — application-level FK enforcement
  (mainstream; SQLite default is OFF).
* ``journal_mode=WAL`` — concurrent readers + faster commits.
* ``synchronous=NORMAL`` — standard WAL durability tradeoff;
  may lose the most recent committed transactions on host power
  loss, acceptable for live market data (ZMQ feed is the durable
  record + the publisher already accepts drop-oldest at the queue
  level).
* ``busy_timeout=30000`` — wait up to 30s on writer-lock
  contention before raising; prevents transient ``SQLITE_BUSY``
  retries from cascading into writer drops.
* ``temp_store=MEMORY`` — keep CTE / sort intermediates off disk.
* ``cache_size=-65536`` — 64 MB page cache; matches working-set
  size of the publisher / read-paths.
* ``wal_autocheckpoint=4000`` — let WAL grow to ~4 MB before the
  next automatic checkpoint, batching fsyncs at the WAL boundary
  instead of every commit.
* ``mmap_size=268435456`` — 256 MB mmap region for the main DB
  file; SQLite reads served from mmap avoid one userspace copy
  per page on cold reads."""


def _register_sqlite_fk_pragma(engine: Any) -> None:
    """Register per-connection SQLite PRAGMAs for foreign keys + write tuning.

    SQLite disables foreign-key enforcement by default; this event
    listener also applies the wider :data:`_SQLITE_CONNECT_PRAGMAS`
    pack (WAL + ``synchronous=NORMAL`` + cache/mmap sizing) on every
    new connection. WAL is sticky on the DB file so re-applying is a
    no-op after the first connection; ``synchronous`` and the cache
    pragmas are per-connection so the listener must fire on every
    new aiosqlite handle.

    Function name kept for backward compatibility with existing call
    sites + tests; the wider pragma surface is intentional.

    Args:
        engine: Sync or async-sync SQLAlchemy engine to register on.
    """
    if not isinstance(engine, SyncEngine):
        return

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection: Any, _connection_record: Any) -> None:
        driver_connection = getattr(dbapi_connection, "driver_connection", None)
        if driver_connection is not None and hasattr(driver_connection, "close"):
            _live_aiosqlite_connections[id(driver_connection)] = cast(
                _ClosableConnection, driver_connection
            )
        cursor = dbapi_connection.cursor()
        try:
            for pragma in _SQLITE_CONNECT_PRAGMAS:
                cursor.execute(pragma)
        finally:
            cursor.close()

    @event.listens_for(engine, "close")
    def _forget_sqlite_connection(dbapi_connection: Any, _connection_record: Any) -> None:
        driver_connection = getattr(dbapi_connection, "driver_connection", None)
        if driver_connection is not None:
            _live_aiosqlite_connections.pop(id(driver_connection), None)


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

        Pool selection:

        * ``:memory:`` SQLite — :class:`StaticPool`. The single in-memory
          DB instance MUST be shared across every connection or each
          new connection sees an empty schema; ``StaticPool`` reuses
          one connection for the engine's lifetime.
        * File-backed SQLite — SQLAlchemy's default
          :class:`AsyncAdaptedQueuePool`. Earlier revisions forced
          :class:`NullPool` here on the theory that aiosqlite
          serialises writes anyway, but profiling showed the per-commit
          connection release-and-reacquire churn against
          ``NullPool`` dominated publisher CPU. The default queue
          pool keeps connections alive across ``session.commit()``
          which is enough on its own to amortise per-flush cost.
        * Postgres + any other dialect — SQLAlchemy default pool.

        ``pool_pre_ping=True`` is set unconditionally so checkouts issue a
        cheap liveness check before handing a connection to the caller.
        Without it, every connection acquired before a Postgres restart
        (e.g. unattended-upgrades on the host) stays in the pool as a
        dead handle and surfaces as ``Errno 111 / Connection refused``
        cascades on every flush until the process is recreated. The pre-
        ping cost (~one ``SELECT 1`` per checkout) is well under the
        per-flush WAL fsync cost we already pay, so it is invisible on
        the publisher hot path. ``pool_recycle=3600`` is the safety net
        for stale TCP sessions and intermediate firewall idle-out — one
        hour is well below typical conntrack timeouts but long enough
        that the recycle is not load-bearing on a hot system.

        ``StaticPool`` is **not** appropriate for file-backed SQLite
        in this codebase because every test worker spawns multiple
        repositories against the same file and they must each open
        their own connection.

        Args:
            db_url: SQLAlchemy async database URL
                (e.g., 'sqlite+aiosqlite:///./data/db.sqlite').
        """
        self.db_url = db_url
        connect_args: dict[str, Any] = {}
        engine_kwargs: dict[str, Any] = {
            "future": True,
            "pool_pre_ping": True,
            "pool_recycle": 3600,
        }
        if "sqlite" in db_url:
            connect_args = {
                "timeout": 30,
                "check_same_thread": False,
            }
            if ":memory:" in db_url:
                engine_kwargs["poolclass"] = StaticPool
        elif "postgresql" in db_url:
            connect_args["server_settings"] = {"timezone": "UTC"}
            pool_size = os.getenv(_DB_POOL_SIZE_ENV)
            if pool_size is not None:
                engine_kwargs["pool_size"] = int(pool_size)
            max_overflow = os.getenv(_DB_MAX_OVERFLOW_ENV)
            if max_overflow is not None:
                engine_kwargs["max_overflow"] = int(max_overflow)
        self.engine: AsyncEngine = create_async_engine(
            db_url, connect_args=connect_args, **engine_kwargs
        )
        if "sqlite" in db_url:
            _register_sqlite_fk_pragma(self.engine.sync_engine)
        self.session_factory = async_sessionmaker(
            self.engine, expire_on_commit=False, class_=AsyncSession
        )
        _live_sqlalchemy_repositories.add(self)
        with suppress(TypeError):
            _live_sqlalchemy_engines.add(self.engine)

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
        session_or_context = self.session_factory()
        if isinstance(session_or_context, AsyncSession):
            session = session_or_context
            try:
                yield session
            except GeneratorExit:
                pass
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
            return
        session_context = cast(AbstractAsyncContextManager[AsyncSession], session_or_context)
        async with session_context as session:
            try:
                yield session
            except GeneratorExit:
                pass
            except Exception:
                await session.rollback()
                raise

    async def get_latest_candle_ids(
        self, as_of: datetime
    ) -> dict[tuple[str, str], tuple[datetime, str]]:
        """Load the latest recent candle public_id per (instrument, timeframe).

        Bounded to candles with ``open_at`` newer than ``_CANDLE_ID_CACHE_LOOKBACK``
        before ``as_of`` so the lookup rides ``ix_candle_instrument_open`` instead
        of scanning the full candles table. See the abstract method for the full
        contract and why omitting older series is safe.
        """
        now = as_of
        open_at_floor = now - _CANDLE_ID_CACHE_LOOKBACK
        async with self.session() as s:
            latest = (
                select(
                    Candle.instrument_public_id,
                    Candle.timeframe,
                    func.max(Candle.open_at).label("max_open_at"),
                )
                .where(
                    Candle.timestamp <= now,
                    Candle.known_to > now,
                    Candle.open_at > open_at_floor,
                )
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
                .where(
                    Candle.timestamp <= now,
                    Candle.known_to > now,
                    Candle.open_at > open_at_floor,
                )
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
        self,
        model: type[Base],
        rows: list[Any],
        index_elements: list[str],
        session: AsyncSession | None = None,
    ) -> int:
        """Dialect-aware batch upsert with conflict-do-nothing.

        Uses SQLite/PostgreSQL native INSERT ... ON CONFLICT DO NOTHING
        when available, falling back to row-by-row IntegrityError handling.

        When ``session`` is provided the upsert runs on the
        caller-managed session WITHOUT issuing ``commit()`` — the
        caller is responsible for committing. This mirrors the
        :meth:`upsert_ticks` writer-task pattern so trade and
        candle flush paths can amortise the per-batch connection
        acquire across many flushes on one pinned connection.

        Args:
            model: SQLAlchemy model class to insert into.
            rows: List of column-value dicts.
            index_elements: Columns that form the unique constraint.
            session: Optional caller-managed session. When ``None``
                a fresh session opens, the insert commits inline,
                and the session closes before returning.

        Returns:
            Number of rows successfully inserted.
        """
        self._ensure_public_ids(rows)
        stmt = self._build_conflict_do_nothing_statement(model, rows, index_elements)
        if stmt is not None:
            return await self._execute_upsert_statement(stmt, session)
        return await self._upsert_rows_with_integrity_fallback(model, rows, session)

    @staticmethod
    def _ensure_public_ids(rows: list[Any]) -> None:
        """Fill missing public identifiers before an insert attempt.

        Args:
            rows: Mutable row dictionaries accepted by SQLAlchemy insert
                statements.
        """
        for row in rows:
            if "public_id" not in row:
                row["public_id"] = str(uuid7())

    def _build_conflict_do_nothing_statement(
        self,
        model: type[Base],
        rows: list[Any],
        index_elements: list[str],
    ) -> Any | None:
        """Build a native conflict-do-nothing statement when supported.

        Args:
            model: SQLAlchemy model class to insert into.
            rows: Column-value dictionaries to insert.
            index_elements: Columns that form the unique constraint.

        Returns:
            SQLAlchemy insert statement for native dialects, otherwise
            ``None`` so callers can use the portable fallback.
        """
        name = self.dialect_name
        if name == "sqlite":
            stmt_sq = sqlite_insert(model).values(rows)
            return stmt_sq.on_conflict_do_nothing(index_elements=index_elements)
        if name.startswith("postgres"):
            stmt_pg = pg_insert(model).values(rows)
            return stmt_pg.on_conflict_do_nothing(index_elements=index_elements)
        return None

    async def _execute_upsert_statement(self, stmt: Any, session: AsyncSession | None) -> int:
        """Execute a native upsert statement in caller-owned or local scope.

        Args:
            stmt: SQLAlchemy native insert statement.
            session: Optional caller-managed session.

        Returns:
            Number of rows reported as inserted.
        """
        if session is not None:
            return await self._execute_statement_count(session, stmt)
        async with self.session() as s:
            inserted = await self._execute_statement_count(s, stmt)
            await s.commit()
            return inserted

    @staticmethod
    async def _execute_statement_count(session: AsyncSession, stmt: Any) -> int:
        """Execute a statement and normalize rowcount.

        Args:
            session: Active async SQLAlchemy session.
            stmt: SQLAlchemy statement to execute.

        Returns:
            Integer rowcount with ``None`` normalized to zero.
        """
        result = await session.execute(stmt)
        return int(cast(Any, result).rowcount or 0)

    async def _upsert_rows_with_integrity_fallback(
        self,
        model: type[Base],
        rows: list[Any],
        session: AsyncSession | None,
    ) -> int:
        """Insert rows one by one while ignoring integrity duplicates.

        Args:
            model: SQLAlchemy model class to insert into.
            rows: Column-value dictionaries to insert.
            session: Optional caller-managed session.

        Returns:
            Number of rows successfully inserted.
        """
        if session is not None:
            return await self._insert_rows_ignoring_integrity_errors(model, rows, session)
        async with self.session() as s:
            inserted = await self._insert_rows_ignoring_integrity_errors(model, rows, s)
            await s.commit()
            return inserted

    @staticmethod
    async def _insert_rows_ignoring_integrity_errors(
        model: type[Base], rows: list[Any], session: AsyncSession
    ) -> int:
        """Insert rows inside savepoints and skip duplicate failures.

        Args:
            model: SQLAlchemy model class to insert into.
            rows: Column-value dictionaries to insert.
            session: Active async SQLAlchemy session.

        Returns:
            Number of rows successfully inserted.
        """
        inserted = 0
        for row in rows:
            try:
                async with session.begin_nested():
                    await session.execute(insert(model).values(**row))
                inserted += 1
            except IntegrityError:
                continue
        return inserted

    async def upsert_candles(
        self, rows: list[CandleUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Close-old + insert-new (SCD Type 2) for candle rows.

        When a candle with the same (instrument_public_id, timeframe, open_at)
        already exists as an active row (known_to == KNOWN_TO_MAX), the old row
        is closed by setting its known_to to now, and a new row is inserted
        carrying the same public_id.  This preserves full history of
        intra-interval updates.

        Rows without ``public_id`` get a generated UUID7 automatically.

        ``session`` semantics mirror :meth:`upsert_ticks` and
        :meth:`upsert_trades`: when provided, the insert runs on the
        caller-managed session without committing so a writer task
        can keep a pinned :class:`AsyncConnection` across many
        flushes; when ``None`` the method opens its own session and
        commits inline.
        """
        if not rows:
            return 0
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
            if "known_to" not in r:
                r["known_to"] = KNOWN_TO_MAX
        if session is not None:
            unique_rows, sequential_rows = self._split_candle_rows_by_duplicate_key(rows)
            existing_by_key = await self._load_existing_candles_for_rows(session, unique_rows)
            count = await self._upsert_unique_candle_rows(session, unique_rows, existing_by_key)
            for r in sequential_rows:
                if await self._upsert_candle_row(session, r):
                    count += 1
            return count
        async with self.session() as s:
            unique_rows, sequential_rows = self._split_candle_rows_by_duplicate_key(rows)
            existing_by_key = await self._load_existing_candles_for_rows(s, unique_rows)
            count = await self._upsert_unique_candle_rows(s, unique_rows, existing_by_key)
            for r in sequential_rows:
                if await self._upsert_candle_row(s, r):
                    count += 1
            await s.commit()
        return count

    @staticmethod
    def _candle_natural_key(row: CandleUpsertRow) -> _CandleNaturalKey:
        """Return the SCD2 natural key for a candle upsert row."""
        return row["instrument_public_id"], row["timeframe"], row["open_at"]

    @staticmethod
    def _candle_row_matches(existing: Candle, row: CandleUpsertRow) -> bool:
        """Return True when every business column of ``row`` equals ``existing``.

        Used to make a re-upsert of identical data a true no-op: when the
        active version already carries the same OHLCV/vwap/trade values, the
        SCD2 close-old + insert-new churn is skipped.

        The stored layer types these columns as ``Float`` (open, high, low,
        close, volume, vwap) and ``Integer`` (trades), so the incoming
        ``CandleUpsertRow`` already carries Python ``float``/``int`` values
        matching the column types.  Comparison is exact equality on those
        stored values — no rounding or float coercion is introduced, so a
        genuine correction in any column still falls through to a new
        version.
        """
        return (
            existing.open == row["open"]
            and existing.high == row["high"]
            and existing.low == row["low"]
            and existing.close == row["close"]
            and existing.volume == row["volume"]
            and existing.vwap == row["vwap"]
            and existing.trades == row["trades"]
        )

    @classmethod
    def _split_candle_rows_by_duplicate_key(
        cls,
        rows: list[CandleUpsertRow],
    ) -> tuple[list[CandleUpsertRow], list[CandleUpsertRow]]:
        """Separate rows safe for batch lookup from duplicate-key rows.

        Rows sharing a natural key in the same incoming batch must keep
        the old sequential close+insert flow because earlier rows can
        create the version that later rows need to close.
        """
        seen: set[_CandleNaturalKey] = set()
        duplicate_keys: set[_CandleNaturalKey] = set()
        for row in rows:
            key = cls._candle_natural_key(row)
            if key in seen:
                duplicate_keys.add(key)
            seen.add(key)
        if not duplicate_keys:
            return rows, []
        unique_rows = [row for row in rows if cls._candle_natural_key(row) not in duplicate_keys]
        sequential_rows = [row for row in rows if cls._candle_natural_key(row) in duplicate_keys]
        return unique_rows, sequential_rows

    @classmethod
    async def _load_existing_candles_for_rows(
        cls,
        session: AsyncSession,
        rows: list[CandleUpsertRow],
    ) -> dict[_CandleNaturalKey, Candle]:
        """Load existing SCD2 candle versions for unique-key rows in one query."""
        if not rows:
            return {}
        row_by_key = {cls._candle_natural_key(row): row for row in rows}
        keys = list(row_by_key)
        existing_by_key: dict[_CandleNaturalKey, Candle] = {}
        for offset in range(0, len(keys), _CANDLE_LOOKUP_CHUNK_SIZE):
            key_chunk = keys[offset : offset + _CANDLE_LOOKUP_CHUNK_SIZE]
            row_chunk = [row_by_key[key] for key in key_chunk]
            min_timestamp = min(row["timestamp"] for row in row_chunk)
            max_timestamp = max(row["timestamp"] for row in row_chunk)
            result = await session.execute(
                select(Candle)
                .where(
                    tuple_(Candle.instrument_public_id, Candle.timeframe, Candle.open_at).in_(
                        key_chunk
                    ),
                    Candle.timestamp <= max_timestamp,
                    Candle.known_to > min_timestamp,
                )
                .with_for_update()
            )
            for candle in result.scalars().all():
                key = (candle.instrument_public_id, candle.timeframe, candle.open_at)
                row = row_by_key[key]
                bus_time = row["timestamp"]
                if candle.timestamp <= bus_time and candle.known_to > bus_time:
                    existing_by_key[key] = candle
        return existing_by_key

    @classmethod
    async def _upsert_unique_candle_rows(
        cls,
        session: AsyncSession,
        rows: list[CandleUpsertRow],
        existing_by_key: dict[_CandleNaturalKey, Candle],
    ) -> int:
        """Close matched candle rows and stage inserts for unique-key rows.

        When the active version already holds identical business values the
        row is a no-op: neither the close-update nor the insert runs and it is
        not counted, so re-loading unchanged data does not create a new
        SCD2 version.
        """
        count = 0
        for row in rows:
            existing = existing_by_key.get(cls._candle_natural_key(row))
            if existing is not None:
                if cls._candle_row_matches(existing, row):
                    continue
                await session.execute(
                    update(Candle).where(Candle.id == existing.id).values(known_to=row["timestamp"])
                )
                row["public_id"] = existing.public_id
            session.add(Candle(**row))
            count += 1
        return count

    @classmethod
    async def _upsert_candle_row(
        cls,
        session: AsyncSession,
        row: CandleUpsertRow,
    ) -> bool:
        """Run the sequential SCD2 close+insert path for one candle row.

        Returns ``True`` when a row was inserted and ``False`` when the row
        was a no-op because the active version already held identical
        business values (so the caller skips counting it).
        """
        bus_time = row["timestamp"]
        existing = (
            (
                await session.execute(
                    select(Candle)
                    .where(
                        Candle.instrument_public_id == row["instrument_public_id"],
                        Candle.timeframe == row["timeframe"],
                        Candle.open_at == row["open_at"],
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
            if cls._candle_row_matches(existing, row):
                return False
            await session.execute(
                update(Candle).where(Candle.id == existing.id).values(known_to=bus_time)
            )
            row["public_id"] = existing.public_id
        session.add(Candle(**row))
        return True

    async def upsert_trades(
        self, rows: list[TradeUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Insert trades with dialect-specific conflict handling.

        Mirrors :meth:`upsert_ticks` ``session`` semantics: when a
        caller-managed session is supplied the insert runs without
        committing so the writer task can batch many flushes onto a
        single pinned :class:`AsyncConnection`. Omit ``session`` for
        ad-hoc / test paths that want an inline commit.
        """
        if not rows:
            return 0
        return await self._upsert_batch(
            Trade, rows, ["instrument_public_id", "trade_id"], session=session
        )

    async def upsert_ticks(
        self, rows: list[TickUpsertRow], session: AsyncSession | None = None
    ) -> int:
        """Insert ticks via append-only Core bulk INSERT.

        Replaces the prior ORM ``s.add_all([Tick(**r) for r in rows])``
        path. Append-only ticks do not need the identity-map, dirty
        tracking, or per-row constructor work that the ORM provides;
        ``insert(Tick).values(rows)`` emits a single SQL statement with
        all rows inline and is materially cheaper at publisher rates.

        When ``session`` is provided the caller owns the transaction —
        no commit fires inside this method. Publisher writer tasks
        rely on this to amortise per-flush connection-acquire cost
        across many batches. When ``session`` is ``None`` (tests,
        bulk imports) a fresh session is opened, the insert commits,
        and the session closes before returning.

        Args:
            rows: Tick rows to insert. ``public_id`` is auto-filled
                when missing.
            session: Optional caller-managed session. When ``None``,
                a fresh session opens and commits inline.

        Returns:
            Number of rows inserted.
        """
        if not rows:
            return 0
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
        if session is not None:
            await session.execute(insert(Tick), list(rows))
            return len(rows)
        async with self.session() as s:
            await s.execute(insert(Tick), list(rows))
            await s.commit()
            return len(rows)

    async def insert_order(
        self,
        row: OrderInsertRow | None = None,
        **kwargs: Unpack[OrderInsertRow],
    ) -> tuple[int, str]:
        """Insert new order record and return (id, public_id) tuple."""
        if row is not None and kwargs:
            raise ValueError("insert_order accepts either row or keyword fields, not both")
        order_row = row if row is not None else kwargs
        mode = order_row.get("mode", "live")
        operator_public_id = order_row.get("operator_public_id")
        time_in_force = order_row.get("time_in_force")
        leverage = order_row.get("leverage")
        reduce_only = order_row.get("reduce_only", False)
        plan_public_id = order_row.get("plan_public_id")
        async with self.session() as s:
            order = Order(
                instrument_public_id=order_row["instrument_public_id"],
                mode=mode,
                wallet_public_id=order_row["wallet_public_id"],
                operator_public_id=operator_public_id,
                client_order_id=order_row["client_order_id"],
                exchange_order_id=order_row["exchange_order_id"],
                created_at=order_row["created_at"],
                updated_at=None,
                timestamp=order_row["timestamp"],
                side=order_row["side"],
                order_type=order_row["order_type"],
                price=order_row["price"],
                size=order_row["size"],
                filled_size=0.0,
                average_price=None,
                status=order_row["status"],
                time_in_force=time_in_force,
                error=None,
                leverage=leverage,
                reduce_only=reduce_only,
                plan_public_id=plan_public_id,
                session_id=order_row["session_id"],
                sequence_id=order_row["sequence_id"],
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
        row: ExecutionInsertRow | None = None,
        **kwargs: Unpack[ExecutionInsertRow],
    ) -> int:
        """Insert execution record and return generated ID."""
        if row is not None and kwargs:
            raise ValueError("insert_execution accepts either row or keyword fields, not both")
        execution_row = row if row is not None else kwargs
        operator_public_id = execution_row.get("operator_public_id")
        exec_id = execution_row.get("exec_id")
        trade_id = execution_row.get("trade_id")
        liquidity_role = execution_row.get("liquidity_role", "unknown")
        async with self.session() as s:
            execution = Execution(
                order_public_id=execution_row["order_public_id"],
                wallet_public_id=execution_row["wallet_public_id"],
                operator_public_id=operator_public_id,
                exec_id=exec_id,
                trade_id=trade_id,
                timestamp=execution_row["timestamp"],
                side=execution_row["side"],
                status=execution_row["status"],
                price=execution_row["price"],
                size=execution_row["size"],
                fee=execution_row["fee"],
                fee_asset=execution_row["fee_asset"],
                session_id=execution_row["session_id"],
                sequence_id=execution_row["sequence_id"],
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

        Collapses the legacy Symbol→Instrument lookup waterfall into
        a single joined query. Every market-data read API
        (``get_candles`` / ``get_ticks`` /
        :meth:`iter_ticks` / :meth:`iter_trades`) hit this method
        twice: one SELECT to look up the Symbol's ``public_id``,
        then another to fetch the matching active Instrument row.
        The JOIN below resolves both in one round-trip.

        Args:
            session: Active database session.
            native_symbol: Canonical symbol string (e.g. 'BTC-USD').
            exchange: Exchange name.
            as_of: Point-in-time for temporal query.

        Returns:
            Active Instrument or None if symbol/instrument not found.
        """
        s_ts, s_kt = where_active(Symbol, as_of)
        i_ts, i_kt = where_active(Instrument, as_of)
        q_inst = await session.execute(
            select(Instrument)
            .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
            .where(
                Symbol.native_symbol == native_symbol,
                s_ts,
                s_kt,
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
        limit: int = DEFAULT_HIGH_CARDINALITY_LIMIT,
    ) -> list[TickRow]:
        """Retrieve ticks for instrument within time range, capped at ``limit``."""
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
                .limit(limit)
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

    async def iter_ticks(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> AsyncIterator[TickRow]:
        """Stream ticks for instrument in time range without materialising the full result.

        Use this for multi-day replays / backtests where
        :meth:`get_ticks` would OOM. Yields rows in
        ``Tick.timestamp ASC`` order; the SQLAlchemy ``stream`` cursor
        yields ``_HIGH_CARDINALITY_STREAM_CHUNK_SIZE`` rows per
        round-trip so the per-row Python overhead amortises while RSS
        stays flat.
        """
        async with self.session() as s:
            inst = await self._resolve_active_instrument(s, instrument, exchange, as_of)
            if inst is None:
                return
            stmt = (
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
                .execution_options(yield_per=_HIGH_CARDINALITY_STREAM_CHUNK_SIZE)
            )
            stream = await s.stream(stmt)
            async for r in stream:
                yield {
                    "timestamp": r.timestamp,
                    "bid": r.bid,
                    "ask": r.ask,
                    "last": r.last,
                    "volume": r.volume,
                    "public_id": r.public_id,
                    "session_id": r.session_id,
                    "sequence_id": r.sequence_id,
                }

    async def iter_trades(
        self,
        instrument: str,
        start: datetime,
        end: datetime,
        exchange: AllExchange,
        as_of: datetime,
    ) -> AsyncIterator[TradeRow]:
        """Stream trades for instrument in time range without materialising the full result.

        Companion to :meth:`iter_ticks` for the equally-high-cardinality
        Trade table. Yields rows in event-time ASC order using
        coalesce(executed_at, timestamp) as the event time, falling back
        to bus-time for rows without executed_at.
        """
        async with self.session() as s:
            inst = await self._resolve_active_instrument(s, instrument, exchange, as_of)
            if inst is None:
                return
            event_time = func.coalesce(Trade.executed_at, Trade.timestamp)
            stmt = (
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
                .execution_options(yield_per=_HIGH_CARDINALITY_STREAM_CHUNK_SIZE)
            )
            stream = await s.stream(stmt)
            async for r in stream:
                yield {
                    "timestamp": r.timestamp,
                    "executed_at": r.executed_at,
                    "price": r.price,
                    "size": r.size,
                    "side": r.side,
                    "trade_id": r.trade_id,
                }

    async def iter_market_snapshots(
        self,
        instrument_public_ids: list[str],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> AsyncIterator[MarketSnapshotRow]:
        """Stream market snapshots in time order without materialising the full result.

        Companion to :meth:`iter_ticks` and :meth:`iter_trades` for
        the snapshot table. Paper-mode ticker replays over long
        windows used to materialise the entire range via
        :meth:`get_market_snapshots`; that bounded-list contract
        remains for UI / one-shot lookups while large-window replays
        prefer this streaming variant. ``yield_per`` chunks rows from
        the database so RSS stays flat across multi-day windows.

        Args:
            instrument_public_ids: Instruments to include.
            start: Lower bound (inclusive, event time).
            end: Upper bound (inclusive, event time).
            as_of: SCD2 ``known_to`` cutoff (bitemporal read).

        Yields:
            Snapshot rows in ``timestamp ASC`` order. Empty input
            list short-circuits without opening a session.
        """
        if not instrument_public_ids:
            return
        async with self.session() as s:
            stmt = (
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
                    MarketSnapshot.timestamp <= as_of,
                    MarketSnapshot.known_to > as_of,
                )
                .order_by(MarketSnapshot.timestamp.asc())
                .execution_options(yield_per=_HIGH_CARDINALITY_STREAM_CHUNK_SIZE)
            )
            stream = await s.stream(stmt)
            async for r in stream:
                yield {
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

        Bulk-loads existing SCD2 versions for the incoming batch in chunks
        of ``_SNAPSHOT_LOOKUP_CHUNK_SIZE`` instruments — replaces the legacy
        per-row SELECT/UPDATE/INSERT N+1 path on the publisher hot loop.
        Rows that share an instrument_public_id within the same incoming
        batch fall back to the sequential close+insert flow because earlier
        rows in the same call may create the version a later row needs to
        close.
        """
        if not rows:
            return 0
        for r in rows:
            if "public_id" not in r:
                r["public_id"] = str(uuid7())
            if "known_to" not in r:
                r["known_to"] = KNOWN_TO_MAX
        async with self.session() as s:
            unique_rows, sequential_rows = self._split_snapshot_rows_by_duplicate_key(rows)
            existing_by_key = await self._load_existing_snapshots_for_rows(s, unique_rows)
            count = await self._upsert_unique_snapshot_rows(s, unique_rows, existing_by_key)
            for r in sequential_rows:
                await self._upsert_snapshot_row(s, r)
                count += 1
            await s.commit()
        return count

    @staticmethod
    def _snapshot_natural_key(row: MarketSnapshotUpsertRow) -> _SnapshotNaturalKey:
        """Return the SCD2 natural key for a market-snapshot upsert row."""
        return row["instrument_public_id"]

    @classmethod
    def _split_snapshot_rows_by_duplicate_key(
        cls,
        rows: list[MarketSnapshotUpsertRow],
    ) -> tuple[list[MarketSnapshotUpsertRow], list[MarketSnapshotUpsertRow]]:
        """Separate rows safe for batch lookup from duplicate-key rows.

        Rows sharing a natural key in the same incoming batch must keep
        the sequential close+insert flow because earlier rows can create
        the version that later rows need to close.
        """
        seen: set[_SnapshotNaturalKey] = set()
        duplicate_keys: set[_SnapshotNaturalKey] = set()
        for row in rows:
            key = cls._snapshot_natural_key(row)
            if key in seen:
                duplicate_keys.add(key)
            seen.add(key)
        if not duplicate_keys:
            return rows, []
        unique_rows = [row for row in rows if cls._snapshot_natural_key(row) not in duplicate_keys]
        sequential_rows = [row for row in rows if cls._snapshot_natural_key(row) in duplicate_keys]
        return unique_rows, sequential_rows

    @classmethod
    async def _load_existing_snapshots_for_rows(
        cls,
        session: AsyncSession,
        rows: list[MarketSnapshotUpsertRow],
    ) -> dict[_SnapshotNaturalKey, MarketSnapshot]:
        """Load existing SCD2 snapshot versions for unique-key rows in one query."""
        if not rows:
            return {}
        row_by_key = {cls._snapshot_natural_key(row): row for row in rows}
        keys = list(row_by_key)
        existing_by_key: dict[_SnapshotNaturalKey, MarketSnapshot] = {}
        for offset in range(0, len(keys), _SNAPSHOT_LOOKUP_CHUNK_SIZE):
            key_chunk = keys[offset : offset + _SNAPSHOT_LOOKUP_CHUNK_SIZE]
            row_chunk = [row_by_key[key] for key in key_chunk]
            min_timestamp = min(row["timestamp"] for row in row_chunk)
            max_timestamp = max(row["timestamp"] for row in row_chunk)
            result = await session.execute(
                select(MarketSnapshot)
                .where(
                    MarketSnapshot.instrument_public_id.in_(key_chunk),
                    MarketSnapshot.timestamp <= max_timestamp,
                    MarketSnapshot.known_to > min_timestamp,
                )
                .with_for_update()
            )
            for snap in result.scalars().all():
                key = snap.instrument_public_id
                row = row_by_key[key]
                bus_time = row["timestamp"]
                if snap.timestamp <= bus_time and snap.known_to > bus_time:
                    existing_by_key[key] = snap
        return existing_by_key

    @classmethod
    async def _upsert_unique_snapshot_rows(
        cls,
        session: AsyncSession,
        rows: list[MarketSnapshotUpsertRow],
        existing_by_key: dict[_SnapshotNaturalKey, MarketSnapshot],
    ) -> int:
        """Close matched snapshot rows and stage inserts for unique-key rows."""
        for row in rows:
            existing = existing_by_key.get(cls._snapshot_natural_key(row))
            if existing is not None:
                await session.execute(
                    update(MarketSnapshot)
                    .where(MarketSnapshot.id == existing.id)
                    .values(known_to=row["timestamp"])
                )
                row["public_id"] = existing.public_id
            session.add(MarketSnapshot(**row))
        return len(rows)

    @classmethod
    async def _upsert_snapshot_row(
        cls,
        session: AsyncSession,
        row: MarketSnapshotUpsertRow,
    ) -> None:
        """Run the sequential SCD2 close+insert path for one snapshot row."""
        bus_time = row["timestamp"]
        existing = (
            (
                await session.execute(
                    select(MarketSnapshot)
                    .where(
                        MarketSnapshot.instrument_public_id == row["instrument_public_id"],
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
            await session.execute(
                update(MarketSnapshot)
                .where(MarketSnapshot.id == existing.id)
                .values(known_to=bus_time)
            )
            row["public_id"] = existing.public_id
        session.add(MarketSnapshot(**row))

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

    async def get_exchange_instruments_detail(
        self,
        exchange: str,
        as_of: datetime,
    ) -> list[InstrumentDetailRow]:
        """Return capability-aware instrument rows for a given exchange.

        Single joined query over four temporal tables at ``as_of``:

        1. ``Symbol`` (native symbol + asset type)
        2. ``SymbolExchangeCapability`` (can_trade + can_market_data)
        3. ``Instrument`` (instrument_public_id) — outer joined so that
           capability rows without a synced Instrument row still surface
           (``instrument_public_id`` defaults to ``symbol_public_id`` so
           the frontend still has a stable identifier).
        4. ``InstrumentSpec`` (instrument_kind + expiry_at) — outer joined
           so specs that don't exist yet (FCM catalog load lag) do not
           hide the instrument.

        ``where_active`` predicates apply to each table so all rows come
        from the same point-in-time snapshot.
        """
        async with self.session() as s:
            sym_ts, sym_kt = where_active(Symbol, as_of)
            cap_ts, cap_kt = where_active(SymbolExchangeCapability, as_of)
            inst_ts, inst_kt = where_active(Instrument, as_of)
            spec_ts, spec_kt = where_active(InstrumentSpec, as_of)
            query = (
                select(
                    Symbol.public_id.label("symbol_public_id"),
                    Symbol.native_symbol.label("symbol"),
                    SymbolExchangeCapability.can_trade.label("can_trade"),
                    SymbolExchangeCapability.can_market_data.label("can_market_data"),
                    Instrument.public_id.label("instrument_public_id"),
                    InstrumentSpec.instrument_kind.label("instrument_kind"),
                    InstrumentSpec.expiry_at.label("expiry_at"),
                )
                .select_from(SymbolExchangeCapability)
                .join(
                    Symbol,
                    Symbol.public_id == SymbolExchangeCapability.symbol_public_id,
                )
                .outerjoin(
                    Instrument,
                    (Instrument.symbol_public_id == Symbol.public_id)
                    & (Instrument.exchange == SymbolExchangeCapability.exchange)
                    & inst_ts
                    & inst_kt,
                )
                .outerjoin(
                    InstrumentSpec,
                    (InstrumentSpec.instrument_public_id == Instrument.public_id)
                    & spec_ts
                    & spec_kt,
                )
                .where(
                    SymbolExchangeCapability.exchange == exchange,
                    cap_ts,
                    cap_kt,
                    sym_ts,
                    sym_kt,
                )
                .order_by(Symbol.native_symbol)
            )
            result = await s.execute(query)
            rows: list[InstrumentDetailRow] = []
            for row in result.mappings().all():
                symbol_pid = str(row["symbol_public_id"])
                instrument_pid = row["instrument_public_id"]
                instrument_resolved = instrument_pid is not None
                rows.append(
                    {
                        "instrument_public_id": (
                            str(instrument_pid) if instrument_resolved else symbol_pid
                        ),
                        "symbol_public_id": symbol_pid,
                        "symbol": str(row["symbol"]),
                        "exchange": exchange,
                        "can_trade": bool(row["can_trade"]),
                        "can_market_data": bool(row["can_market_data"]),
                        "instrument_resolved": instrument_resolved,
                        "instrument_kind": row["instrument_kind"],
                        "expiry_at": row["expiry_at"],
                    }
                )
            return rows

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
        status: str | None = None,
        wallet_public_ids: list[str] | None = None,
    ) -> list[OrderRow]:
        """Retrieve orders with optional filters and pagination.

        ``status`` is pushed INTO SQL — post-fetch filtering broke
        pagination because limit/offset clipped before the filter could
        discard non-matching rows. ``OrderRow`` carries ``plan_public_id``
        so the MCP ``get_order_status`` tool can resolve the parent
        execution plan without a second round-trip.
        """
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
            if status is not None:
                query = query.where(Order.status == status)
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
                    "plan_public_id": order.plan_public_id,
                }
                for order, inst, sym in result.all()
            ]

    async def get_orders_total_count(
        self,
        as_of: datetime,
        symbol: str | None = None,
        exchange: str | None = None,
        status: str | None = None,
        wallet_public_ids: list[str] | None = None,
    ) -> int:
        """Count orders matching :meth:`get_orders` filter shape."""
        async with self.session() as s:
            query = (
                select(func.count(Order.id))
                .select_from(Order)
                .join(
                    Instrument,
                    and_(
                        Order.instrument_public_id == Instrument.public_id,
                        *where_active(Instrument, as_of),
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
            if status is not None:
                query = query.where(Order.status == status)
            result = await s.execute(query)
            count = result.scalar_one_or_none()
            return int(count or 0)

    async def get_order_by_command_public_id(
        self,
        command_public_id: str,
        as_of: datetime,
    ) -> OrderRow | None:
        """Resolve an order by traversing trade_commands.plan_public_id."""
        async with self.session() as s:
            query = (
                select(Order, Instrument, Symbol)
                .select_from(TradeCommand)
                .join(
                    Order,
                    and_(
                        Order.plan_public_id == TradeCommand.plan_public_id,
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
                .where(
                    TradeCommand.public_id == command_public_id,
                    *where_active(TradeCommand, as_of),
                )
                .order_by(desc(Order.created_at))
                .limit(1)
            )
            result = await s.execute(query)
            row = result.first()
            if row is None:
                return None
            order, inst, sym = row
            return {
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
                "plan_public_id": order.plan_public_id,
            }

    async def get_trade_command_by_public_id(
        self,
        command_public_id: str,
        as_of: datetime,
    ) -> TradeCommandRow | None:
        """Fetch a single trade-command row by public_id."""
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(
                    TradeCommand.public_id == command_public_id,
                    *where_active(TradeCommand, as_of),
                )
                .limit(1)
            )
            cmd = result.scalars().first()
            if cmd is None:
                return None
            return {
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
                "stop_price": cmd.stop_price,
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
                "plan_public_id": cmd.plan_public_id,
            }

    async def get_executions_for_order(
        self,
        order_public_id: str,
        as_of: datetime,
    ) -> list[ExecutionRow]:
        """SQL-filtered executions for a single order; oldest-first."""
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
                .where(
                    Execution.order_public_id == order_public_id,
                    *where_active(Execution, as_of),
                )
                .order_by(Execution.timestamp.asc())
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
                    "wallet_public_id": exe.wallet_public_id,
                    "operator_public_id": exe.operator_public_id,
                    "liquidity_role": getattr(exe, "liquidity_role", "unknown"),
                }
                for exe, order, inst, sym in result.all()
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
        exchange: str | None,
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
                    Order.status.in_(self._ACTIVE_ORDER_STATUSES),
                )
                .order_by(Order.created_at)
            )
            if exchange is not None:
                query = query.where(Instrument.exchange == exchange)
            if wallet_public_id:
                query = query.where(Order.wallet_public_id == wallet_public_id)
            result = await s.execute(query)
            return [
                {
                    "id": order.id,
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
                    "plan_public_id": order.plan_public_id,
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

        The position_cycle_public_id is resolved via a NOT-EXISTS
        anti-join that returns a cycle only when no other active open
        cycle matches the position's (instrument, exchange, mode,
        wallet). Multiple matching cycles (e.g. paper mode with
        strategy tags) yield NULL to prevent attaching a bracket to
        the wrong cycle.

        Anti-join (not an aggregate) is the right shape here: it
        states the actual invariant ("there is no second matching
        cycle") without depending on any property of UUID generation.
        An earlier MIN-based form was both PostgreSQL-incompatible
        (``min(uuid)`` does not exist) and quietly assumed uuid7
        time-ordering — a semantic landmine if the generator ever
        changes. Self-exclusion uses the internal PK ``id`` so no
        UUID comparison is involved.
        """
        async with self.session() as s:
            other_cycle = aliased(PositionCycle)
            open_cycle_unambiguous = (
                select(
                    PositionCycle.instrument_public_id.label("instrument_public_id"),
                    PositionCycle.exchange.label("exchange"),
                    PositionCycle.mode.label("mode"),
                    PositionCycle.wallet_public_id.label("wallet_public_id"),
                    PositionCycle.public_id.label("position_cycle_public_id"),
                )
                .where(
                    PositionCycle.status == "open",
                    *where_active(PositionCycle, as_of),
                    ~(
                        select(1)
                        .select_from(other_cycle)
                        .where(
                            other_cycle.status == "open",
                            *where_active(other_cycle, as_of),
                            other_cycle.instrument_public_id == PositionCycle.instrument_public_id,
                            other_cycle.exchange == PositionCycle.exchange,
                            other_cycle.mode == PositionCycle.mode,
                            other_cycle.wallet_public_id == PositionCycle.wallet_public_id,
                            other_cycle.id != PositionCycle.id,
                        )
                        .exists()
                    ),
                )
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
                    "instrument_public_id": inst.public_id,
                    "exchange": inst.exchange,
                    "mode": pos.mode,
                    "quantity": pos.quantity,
                    "average_price": pos.average_price,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "realized_pnl": pos.realized_pnl,
                    "position_cycle_public_id": cycle_pid,
                    "wallet_public_id": pos.wallet_public_id,
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
        When ``ownership``
        is non-None, the row's ``shard_key`` MUST be owned by it or
        class:`ShardOwnershipError` is raised before the DB write.
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

    async def insert_paired_compensation_command(self, row: TradeCommandInsertRow) -> str | None:
        """Insert a paired-execution compensation command, idempotent on idempotency_key.

        Used by the guard scanner to emit a venue cancel of a still-live original
        order or a reduce-only flatten for a broken / compensating group. The
        row MUST carry an ``idempotency_key``; the active-unique
        ``(idempotency_key)`` index dedups re-emission across scan
        cycles and coordinator instances. On an ``IntegrityError`` the method
        RE-CHECKS that an active row already holds this ``idempotency_key``; if so
        the collision was the expected idempotent dedup and ``None`` is returned,
        but ANY other integrity failure (a malformed row violating a different
        constraint) is RE-RAISED rather than silently swallowed — swallowing it
        would drop a compensation command and leave exposure unflattened. Returns
        the new command ``public_id`` iff this call inserted it, else ``None``.

        Raises ``ValueError`` when the row carries no ``idempotency_key``: the
        idempotent dedup hinges on a concrete key, and a null key would make the
        ``IntegrityError`` re-check match any active null-key command and swallow
        an unrelated constraint failure.
        """
        idempotency_key = row.get("idempotency_key")
        if not idempotency_key:
            raise ValueError(
                "insert_paired_compensation_command requires a non-empty idempotency_key"
            )
        async with self.session() as s:
            cmd = TradeCommand(**{"wallet_public_id": "", **row})
            s.add(cmd)
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()
                existing = (
                    await s.execute(
                        select(TradeCommand.id).where(
                            TradeCommand.idempotency_key == idempotency_key,
                            TradeCommand.known_to == KNOWN_TO_MAX,
                        )
                    )
                ).first()
                if existing is None:
                    raise
                return None
            await s.refresh(cmd)
            return cmd.public_id

    async def claim_leg_and_insert_flatten_command(
        self,
        *,
        leg_public_id: str,
        expected_status: str,
        new_compensation_seq: int,
        command_row: TradeCommandInsertRow,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str | None:
        """Atomically claim a leg for compensation and insert its flatten command.

        In ONE transaction: re-reads the CURRENT active leg
        (``known_to == KNOWN_TO_MAX``) ``FOR UPDATE``; if the leg is gone or its
        status no longer equals ``expected_status`` (another instance / cycle
        already claimed it — optimistic CAS) it returns ``None`` WITHOUT inserting;
        otherwise it SCD2 close-and-inserts the leg successor as
        ``compensating`` with ``compensation_seq = new_compensation_seq`` (keeping
        the ORIGINAL ``client_order_id`` — the leg is NOT rebound to the flatten),
        clamping the close / successor bus time to ``max(bus_time,
        existing.timestamp)`` for the same clock-skew SCD2 non-inversion reason as
        the fill projection, AND inserts the flatten ``TradeCommand``. Doing both
        in one commit means a crash rolls back both — never a claimed leg with no
        flatten command (or vice versa). On the flatten command's
        ``idempotency_key`` IntegrityError it RE-CHECKS the active key (a racing
        duplicate) and returns ``None``, else RE-RAISES. ``command_row`` MUST carry
        an ``idempotency_key``. Returns the flatten command ``public_id`` iff this
        call claimed-and-inserted, else ``None``.
        """
        idempotency_key = command_row.get("idempotency_key")
        if not idempotency_key:
            raise ValueError(
                "claim_leg_and_insert_flatten_command requires a non-empty idempotency_key"
            )
        async with self.session() as s:
            leg = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.public_id == leg_public_id,
                            PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if leg is None or leg.status != expected_status:
                return None
            effective_bus_time = max(bus_time, leg.timestamp)
            await s.execute(
                update(PairedExecutionLeg)
                .where(PairedExecutionLeg.id == leg.id)
                .values(known_to=effective_bus_time)
            )
            s.add(
                self._leg_successor(
                    leg,
                    status=PairedExecutionLegStatusEnum.COMPENSATING.value,
                    filled_signed_qty=leg.filled_signed_qty,
                    exchange_order_id=leg.exchange_order_id,
                    last_venue_event_id=leg.last_venue_event_id,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=effective_bus_time,
                    compensation_seq=new_compensation_seq,
                )
            )
            cmd = TradeCommand(**{"wallet_public_id": "", **command_row})
            s.add(cmd)
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()
                existing = (
                    await s.execute(
                        select(TradeCommand.id).where(
                            TradeCommand.idempotency_key == idempotency_key,
                            TradeCommand.known_to == KNOWN_TO_MAX,
                        )
                    )
                ).first()
                if existing is None:
                    raise
                return None
            await s.refresh(cmd)
            return cmd.public_id

    async def get_current_trade_command_status(self, command_public_id: str) -> str | None:
        """Return the CURRENT active trade command's status, or None if absent.

        Guards on the current active row (``known_to == KNOWN_TO_MAX``), NOT a
        temporal ``as_of`` view, so the compensation sweep's venue-liveness
        decision is not fooled by a command row stamped with a slightly future
        ``timestamp`` under cross-coordinator clock skew (a ``where_active(now)``
        read would exclude it and wrongly treat a live original as gone). Mirrors
        the current-active guard the trade-command CAS uses.
        """
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand.status).where(
                    TradeCommand.public_id == command_public_id,
                    TradeCommand.known_to == KNOWN_TO_MAX,
                )
            )
            row = result.first()
            return row[0] if row is not None else None

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

        All-time count (no time window). Cancel
        commands are excluded: they are not in-flight exposure.
        The DB persists two submit-type vocabularies: REST routes and
        plan helpers (bracket, trailing_stop) insert ``"create"``
        strategy/engine paths insert ``"submit"`` via
        class:`OrderCommandEnum`; replace paths insert ``"replace"``.
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

    async def list_active_user_token_jtis(self, user_public_id: str) -> list[str]:
        """Return every unrevoked JTI for a user's active tokens.

        See :meth:`Repository.list_active_user_token_jtis` for contract.
        """
        async with self.session() as s:
            result = await s.execute(
                select(UserActiveToken.jti).where(
                    UserActiveToken.user_public_id == user_public_id,
                    UserActiveToken.revoked_at.is_(None),
                )
            )
            return [row for (row,) in result.all()]

    async def revoke_user_active_tokens(self, user_public_id: str, revoked_at: datetime) -> int:
        """Mark every unrevoked token row for a user as revoked.

        See :meth:`Repository.revoke_user_active_tokens` for contract.
        """
        async with self.session() as s:
            result: Any = await s.execute(
                update(UserActiveToken)
                .where(
                    UserActiveToken.user_public_id == user_public_id,
                    UserActiveToken.revoked_at.is_(None),
                )
                .values(revoked_at=revoked_at)
            )
            await s.commit()
            return int(result.rowcount or 0)

    async def insert_user_active_tokens(
        self,
        rows: list[UserActiveTokenInsertRow],
    ) -> None:
        """Persist a batch of fresh access + refresh token rows.

        See :meth:`Repository.insert_user_active_tokens` for contract.
        """
        if not rows:
            return
        async with self.session() as s:
            await s.execute(insert(UserActiveToken), list(rows))
            await s.commit()

    async def revoke_user_active_token_by_jti(
        self,
        jti: str,
        revoked_at: datetime,
    ) -> int:
        """Flip ``revoked_at`` on the single row identified by ``jti``.

        See :meth:`Repository.revoke_user_active_token_by_jti` for contract.
        """
        async with self.session() as s:
            result: Any = await s.execute(
                update(UserActiveToken)
                .where(
                    UserActiveToken.jti == jti,
                    UserActiveToken.revoked_at.is_(None),
                )
                .values(revoked_at=revoked_at)
            )
            await s.commit()
            return int(result.rowcount or 0)

    async def rotate_user_active_token(
        self,
        old_jti: str,
        new_rows: list[UserActiveTokenInsertRow],
        revoked_at: datetime,
    ) -> bool:
        """Atomic revoke-old + insert-new across a single transaction.

        See :meth:`Repository.rotate_user_active_token` for contract.
        """
        async with self.session() as s:
            result: Any = await s.execute(
                update(UserActiveToken)
                .where(
                    UserActiveToken.jti == old_jti,
                    UserActiveToken.revoked_at.is_(None),
                )
                .values(revoked_at=revoked_at)
            )
            rowcount = int(result.rowcount or 0)
            if rowcount != 1:
                await s.rollback()
                return False
            await s.execute(insert(UserActiveToken), list(new_rows))
            await s.commit()
            return True

    async def get_active_token_by_hash(
        self,
        token_hash: str,
    ) -> UserActiveTokenVerificationRow | None:
        """Return the verify-path row for ``token_hash`` joined with ``users.is_active``.

        See :meth:`Repository.get_active_token_by_hash` for contract.

        Temporal filter note: the JOIN uses ``where_active_now(User)``
        (``timestamp <= now`` AND ``known_to > now``) instead of
        ``known_to == KNOWN_TO_MAX``. The equality form relies on
        exact datetime-string round-tripping between SQLAlchemy's
        ``DateTime`` binding and SQLite's stored value; pre-seeded
        rows persist the tz suffix (``+00:00``) while binds may
        elide it, producing silent empty JOIN results. The
        ``>``/``<=`` form matches the rest of the codebase idiom
        (see :func:`where_active_now`) and works across both seed
        shapes + dialects.
        """
        async with self.session() as s:
            result = await s.execute(
                select(
                    UserActiveToken.user_public_id,
                    UserActiveToken.revoked_at,
                    UserActiveToken.expires_at,
                    User.is_active,
                )
                .join(
                    User,
                    and_(
                        User.public_id == UserActiveToken.user_public_id,
                        *where_active_now(User),
                    ),
                )
                .where(UserActiveToken.token_hash == token_hash)
            )
            row = result.first()
            if row is None:
                return None
            user_public_id, revoked_at, expires_at, user_is_active = row
            return UserActiveTokenVerificationRow(
                user_public_id=user_public_id,
                revoked_at=revoked_at,
                expires_at=expires_at,
                user_is_active=bool(user_is_active),
            )

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

    async def get_plan_public_ids_for_client_order_ids(
        self,
        client_order_ids: list[str],
        as_of: datetime,
    ) -> dict[str, str]:
        """Batched child->parent plan resolver.

        Returns at most one ``plan_public_id`` per ``client_order_id``
        (the most recent active ``create`` row by ``created_at`` desc /
        ``id`` desc), matching the single-row helper's tie-break
        semantics so the batched caller can rely on equivalent results.
        """
        if not client_order_ids:
            return {}
        async with self.session() as s:
            result = await s.execute(
                select(
                    TradeCommand.client_order_id,
                    TradeCommand.plan_public_id,
                    TradeCommand.created_at,
                    TradeCommand.id,
                )
                .where(
                    TradeCommand.client_order_id.in_(client_order_ids),
                    TradeCommand.plan_public_id.is_not(None),
                    TradeCommand.command_type == "create",
                    *where_active(TradeCommand, as_of),
                )
                .order_by(TradeCommand.created_at.desc(), TradeCommand.id.desc())
            )
            resolved: dict[str, str] = {}
            for cid, plan_pid, _created_at, _row_id in result.all():
                if cid in resolved:
                    continue
                resolved[cid] = cast(str, plan_pid)
            return resolved

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
                stop_price=existing.stop_price,
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

    async def cas_trade_command_status(
        self,
        public_id: str,
        expected_status: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        terminal_at: datetime | None = None,
        last_error: str | None = None,
    ) -> bool:
        """SCD2 compare-and-swap for a trade command status transition.

        Unlike :meth:`update_trade_command_status` (which transitions the
        active row unconditionally), this guards on ``expected_status`` under
        the ``FOR UPDATE`` row lock of the CURRENT active row
        (``known_to == KNOWN_TO_MAX``, not a temporal ``as_of`` view): the
        command moves only if its current active row is still at
        ``expected_status``. So even with a stale ``bus_time``, a command that
        already moved ``created -> dispatched`` (reached the venue) is seen at
        its current ``dispatched`` status and the CAS returns ``False`` rather
        than locking the historical ``created`` row and inserting a duplicate
        active successor. The paired-execution guard scanner uses it to cancel
        a HELD ``created`` command of a broken assembling group. Returns
        ``True`` iff the transition was applied.

        Thin wrapper over :meth:`advance_trade_command_lifecycle` keeping the
        original call shape (no ack/exchange-id overrides) for the outbox TTL
        and guard-scanner callers.
        """
        return await self.advance_trade_command_lifecycle(
            public_id=public_id,
            expected_status=expected_status,
            new_status=new_status,
            bus_time=bus_time,
            session_id=session_id,
            sequence_id=sequence_id,
            terminal_at=terminal_at,
            last_error=last_error,
        )

    async def advance_trade_command_lifecycle(
        self,
        public_id: str,
        expected_status: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        *,
        acked_at: datetime | None = None,
        exchange_order_id: str | None = None,
        terminal_at: datetime | None = None,
        last_error: str | None = None,
        clear_terminal_at: bool = False,
    ) -> bool:
        """SCD2 CAS advancing a command with venue-evidenced lifecycle fields.

        Same race discipline as :meth:`cas_trade_command_status` (FOR UPDATE
        on the active row, ``expected_status`` guard, close-insert successor)
        — disjoint expected-status sets keep the lifecycle fold race-free
        against the outbox's ``created -> dispatched/expired`` and the guard
        scanner's ``created -> cancelled`` transitions. Additionally carries
        the ack-time fields the original CAS cannot express: ``acked_at`` and
        ``exchange_order_id`` override the successor row when provided and
        are carried forward from the existing row when ``None``.
        ``terminal_at`` keeps the same carry-forward rule unless
        ``clear_terminal_at`` is set (the false-absence-rejection restore
        must NULL the stale terminal stamp when resurrecting
        ``rejected -> accepted``); ``last_error`` is written verbatim
        (``None`` CLEARS a previous error — an advance to a healthy
        lifecycle state supersedes stale dispatch errors, mirroring
        ``bulk_dispatch_trade_commands``). Returns ``True`` iff the
        transition was applied.
        """
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(TradeCommand)
                        .where(
                            TradeCommand.public_id == public_id,
                            TradeCommand.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return False
            if existing.status != expected_status:
                return False
            await s.execute(
                update(TradeCommand).where(TradeCommand.id == existing.id).values(known_to=bus_time)
            )
            next_terminal_at = terminal_at if terminal_at is not None else existing.terminal_at
            if clear_terminal_at:
                next_terminal_at = None
            s.add(
                TradeCommand(
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
                    stop_price=existing.stop_price,
                    leverage=existing.leverage,
                    reduce_only=existing.reduce_only,
                    status=new_status,
                    attempt_count=existing.attempt_count,
                    last_error=last_error,
                    created_at=existing.created_at,
                    dispatched_at=existing.dispatched_at,
                    acked_at=acked_at if acked_at is not None else existing.acked_at,
                    terminal_at=next_terminal_at,
                    exchange_order_id=(
                        exchange_order_id
                        if exchange_order_id is not None
                        else existing.exchange_order_id
                    ),
                    supersedes_command_id=existing.supersedes_command_id,
                    correlation_id=existing.correlation_id,
                    plan_public_id=existing.plan_public_id,
                    source_surface=existing.source_surface,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=bus_time,
                    wallet_public_id=existing.wallet_public_id,
                    operator_public_id=existing.operator_public_id,
                    user_public_id=existing.user_public_id,
                )
            )
            await s.commit()
            return True

    async def bulk_dispatch_trade_commands(self, updates: list[TradeCommandDispatchUpdate]) -> int:
        """Apply N CREATED -> DISPATCHED SCD2 transitions in one session + commit.

        Replaces the per-row ``update_trade_command_status`` round-trip on
        the OutboxDispatcher hot loop. Each input describes one command's
        success-side transition. Within a single ``async with self.session``
        block we bulk-load the active source rows by ``public_id`` (chunked
        by ``_OUTBOX_BULK_LOOKUP_CHUNK_SIZE``), close each match, and stage
        its successor INSERT — then commit once.

        Inputs whose ``public_id`` no longer points to an active row are
        skipped silently; the returned count reflects only rows actually
        transitioned. Per-row ``last_error`` is cleared on the new
        version (success path never carries a previous error forward).

        CAS semantics on the CURRENT row: only an
        ACTIVE row still at ``status='created'`` transitions. A command
        that a concurrent writer already moved (outbox TTL
        CREATED→EXPIRED, guard-scanner CREATED→CANCELLED) between this
        dispatcher's publish and the bulk write is SKIPPED — writing
        DISPATCHED over it would resurrect a terminal command and fork
        overlapping SCD2 successors.

        Failure semantics: any DB error rolls back the entire transaction.
        The OutboxDispatcher therefore falls back to per-row
        ``update_trade_command_status`` for non-success transitions (e.g.
        revert-to-CREATED on publish failure) so a bulk-write blow-up only
        risks one tick's batch, never the per-row error-recovery path.
        """
        if not updates:
            return 0
        spec_by_pid = {u["public_id"]: u for u in updates}
        public_ids = list(spec_by_pid)
        new_status = TradeCommandStatusEnum.DISPATCHED.value
        applied = 0
        async with self.session() as s:
            for offset in range(0, len(public_ids), _OUTBOX_BULK_LOOKUP_CHUNK_SIZE):
                pid_chunk = public_ids[offset : offset + _OUTBOX_BULK_LOOKUP_CHUNK_SIZE]
                spec_chunk = [spec_by_pid[pid] for pid in pid_chunk]
                max_bus_time = max(spec["bus_time"] for spec in spec_chunk)
                result = await s.execute(
                    select(TradeCommand)
                    .where(
                        TradeCommand.public_id.in_(pid_chunk),
                        TradeCommand.timestamp <= max_bus_time,
                        TradeCommand.known_to == KNOWN_TO_MAX,
                    )
                    .with_for_update()
                )
                for existing in result.scalars().all():
                    spec = spec_by_pid[existing.public_id]
                    bus_time = spec["bus_time"]
                    if existing.status != TradeCommandStatusEnum.CREATED.value:
                        continue
                    if not (existing.timestamp <= bus_time and existing.known_to > bus_time):
                        continue
                    await s.execute(
                        update(TradeCommand)
                        .where(TradeCommand.id == existing.id)
                        .values(known_to=bus_time)
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
                        stop_price=existing.stop_price,
                        leverage=existing.leverage,
                        reduce_only=existing.reduce_only,
                        status=new_status,
                        attempt_count=spec["attempt_count"],
                        last_error=None,
                        created_at=existing.created_at,
                        dispatched_at=spec["dispatched_at"],
                        acked_at=existing.acked_at,
                        terminal_at=existing.terminal_at,
                        exchange_order_id=existing.exchange_order_id,
                        supersedes_command_id=existing.supersedes_command_id,
                        correlation_id=existing.correlation_id,
                        plan_public_id=existing.plan_public_id,
                        session_id=spec["session_id"],
                        sequence_id=spec["sequence_id"],
                        timestamp=bus_time,
                        wallet_public_id=existing.wallet_public_id,
                        operator_public_id=existing.operator_public_id,
                        user_public_id=existing.user_public_id,
                    )
                    s.add(new_cmd)
                    applied += 1
            await s.commit()
        return applied

    async def get_undispatched_commands(
        self,
        as_of: datetime,
        limit: int = 10,
        offset: int = 0,
    ) -> list[TradeCommandRow]:
        """Return trade commands with status='created' for outbox dispatch.

        When ``offset > 0``, the query skips the
        first ``offset`` rows. Used by
        class:`OutboxDispatcher._dispatch_batch` to page through the
        ``created`` backlog while filtering for owned shards in Python
        see for the starvation-bound contract.
        Ordering is ``(created_at, id)`` for deterministic pagination
        plan-service dispatch inserts multiple commands within a single
        ``now`` tick (see ``application/plans/service.py``) so ties on
        ``created_at`` are realistic. Without the ``id`` tie-breaker
        ``OFFSET`` pagination could skip or duplicate rows across pages
        → double-dispatch.
        """
        dispatchable_paired_gate = or_(
            TradeCommand.supersedes_command_id.isnot(None),
            ~exists(
                select(PairedExecutionGroup.id).where(
                    PairedExecutionGroup.public_id == TradeCommand.correlation_id,
                    *where_active(PairedExecutionGroup, as_of),
                )
            ),
            exists(
                select(PairedExecutionLeg.id)
                .join(
                    PairedExecutionGroup,
                    PairedExecutionLeg.group_public_id == PairedExecutionGroup.public_id,
                )
                .where(
                    PairedExecutionGroup.public_id == TradeCommand.correlation_id,
                    PairedExecutionGroup.status == PairedExecutionGroupStatusEnum.ARMED,
                    PairedExecutionLeg.command_public_id == TradeCommand.public_id,
                    *where_active(PairedExecutionGroup, as_of),
                    *where_active(PairedExecutionLeg, as_of),
                )
            ),
        )
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(
                    TradeCommand.status == "created",
                    *where_active(TradeCommand, as_of),
                    dispatchable_paired_gate,
                )
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
                        "stop_price": cmd.stop_price,
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
                        "plan_public_id": cmd.plan_public_id,
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
                        "stop_price": cmd.stop_price,
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
                        "plan_public_id": cmd.plan_public_id,
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
            return [self._trade_command_to_row(cmd) for cmd in result.scalars().all()]

    @staticmethod
    def _trade_command_to_row(cmd: TradeCommand) -> TradeCommandRow:
        """Project a TradeCommand ORM row into the TradeCommandRow TypedDict shape."""
        return {
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
            "stop_price": cmd.stop_price,
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
            "plan_public_id": cmd.plan_public_id,
        }

    async def get_active_create_command_by_client_order_id(
        self, client_order_id: str, exchange: str
    ) -> TradeCommandRow | None:
        """Strict create/submit command lookup for executor adoption sweeps.

        ``client_order_id`` is NOT unique on trade_commands — cancel
        commands deliberately share the original order's cid — so this
        targets the active SCD2 row (``known_to == KNOWN_TO_MAX``) of
        create/submit types only, scoped to the exchange. Fail-closed
        contract: more than one active match means the idempotency
        invariants are broken and adopting ANY of them could attribute
        venue state to the wrong command, so it raises instead of
        guessing.

        Args:
            client_order_id: The venue client id observed on the order.
            exchange: The executor's exchange name.

        Returns:
            The active command row, or ``None`` when no create/submit
            command exists for the cid.

        Raises:
            RuntimeError: When multiple active create/submit rows match.
        """
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand).where(
                    TradeCommand.client_order_id == client_order_id,
                    TradeCommand.exchange == exchange,
                    TradeCommand.command_type.in_(_LIFECYCLE_FOLD_COMMAND_TYPES),
                    TradeCommand.known_to == KNOWN_TO_MAX,
                )
            )
            rows = result.scalars().all()
            if not rows:
                return None
            if len(rows) > 1:
                raise RuntimeError(
                    f"multiple active create/submit commands for client_order_id "
                    f"{client_order_id} on {exchange} ({len(rows)} rows) — refusing "
                    f"to adopt ambiguously"
                )
            return self._trade_command_to_row(rows[0])

    async def get_unresolved_dispatched_commands(
        self, exchange: str, wallet_public_id: str, older_than: datetime
    ) -> list[TradeCommandRow]:
        """Dispatched create/submit commands with no resolving venue evidence.

        The executor's verification sweep's work queue: active rows in
        ``dispatched``/``direct_dispatched`` older than the cutoff with
        NO ``_ORDER_RESOLVING_EVENT_TYPES`` row for their cid (NOT
        EXISTS anti-join). A lone ``order_submit_unknown`` does not
        exempt — those are exactly the restart-lost parked entries the
        sweep must re-verify against the venue. Scoped to the
        executor's wallet: another wallet's commands belong to its own
        executor. Ordered ``(created_at, id)`` for stable rotation.

        Args:
            exchange: The executor's exchange name.
            wallet_public_id: The executor's wallet.
            older_than: Only commands created strictly before this.

        Returns:
            Ordered unresolved command rows.
        """
        evidence_exists = (
            select(VenueEvent.id)
            .where(
                VenueEvent.client_order_id == TradeCommand.client_order_id,
                VenueEvent.event_type.in_(_ORDER_RESOLVING_EVENT_TYPES),
            )
            .exists()
        )
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(
                    TradeCommand.exchange == exchange,
                    TradeCommand.wallet_public_id == wallet_public_id,
                    TradeCommand.command_type.in_(_LIFECYCLE_FOLD_COMMAND_TYPES),
                    TradeCommand.status.in_(
                        (
                            TradeCommandStatusEnum.DISPATCHED,
                            TradeCommandStatusEnum.DIRECT_DISPATCHED,
                        )
                    ),
                    TradeCommand.known_to == KNOWN_TO_MAX,
                    TradeCommand.created_at < older_than,
                    ~evidence_exists,
                )
                .order_by(TradeCommand.created_at, TradeCommand.id)
            )
            return [self._trade_command_to_row(cmd) for cmd in result.scalars().all()]

    async def get_rejected_commands_with_later_live_evidence(
        self, exchange: str, limit: int = 20
    ) -> list[TradeCommandRow]:
        """REJECTED create/submit commands whose cid shows LATER live evidence.

        The durable resurrection backstop for false absence rejections
        (#145 Phase E): a command can be durably REJECTED (stale gate,
        dispatched-verification sweep, fold) and only later prove alive —
        an ``order_accepted`` or ``fill_observed`` row with an id GREATER
        than the cid's latest ``order_rejected`` event. The executor's
        in-memory restore queue heals the common case; this query makes
        the heal restart-proof — the ReconciliationLoop resurrects such
        rows to ACCEPTED so the lifecycle fold can re-derive their true
        state. Bounded by ``limit`` per cycle.

        Args:
            exchange: Exchange name to scan.
            limit: Maximum rows per call.

        Returns:
            Matching command rows ordered by created_at.
        """
        latest_rejection = (
            select(func.coalesce(func.max(VenueEvent.id), 0))
            .where(
                VenueEvent.client_order_id == TradeCommand.client_order_id,
                VenueEvent.event_type == "order_rejected",
            )
            .correlate(TradeCommand)
            .scalar_subquery()
        )
        live_after_rejection = (
            select(VenueEvent.id)
            .where(
                VenueEvent.client_order_id == TradeCommand.client_order_id,
                VenueEvent.event_type.in_(("order_accepted", "fill_observed")),
                VenueEvent.id > latest_rejection,
            )
            .exists()
        )
        async with self.session() as s:
            result = await s.execute(
                select(TradeCommand)
                .where(
                    TradeCommand.exchange == exchange,
                    TradeCommand.command_type.in_(_LIFECYCLE_FOLD_COMMAND_TYPES),
                    TradeCommand.status == TradeCommandStatusEnum.REJECTED,
                    TradeCommand.known_to == KNOWN_TO_MAX,
                    live_after_rejection,
                )
                .order_by(TradeCommand.created_at)
                .limit(limit)
            )
            return [self._trade_command_to_row(cmd) for cmd in result.scalars().all()]

    async def get_order_identity_for_client_order_id(
        self, client_order_id: str, as_of: datetime
    ) -> tuple[int, str, str | None] | None:
        """Return the active orders row's identity triple for a client id.

        ``(id, public_id, exchange_order_id)`` of the newest active row —
        the executor adoption sweeps need the logical order identity to
        run dual-plane (executions vs venue_events) watermark seeding and
        to persist status updates, and the venue-id-only lookup cannot
        provide it.

        Args:
            client_order_id: Client-side order id.
            as_of: Temporal point for active-version selection.

        Returns:
            The identity triple, or None when no active row exists.
        """
        async with self.session() as s:
            result = await s.execute(
                select(Order.id, Order.public_id, Order.exchange_order_id)
                .where(
                    Order.client_order_id == client_order_id,
                    *where_active(Order, as_of),
                )
                .order_by(Order.created_at.desc(), Order.id.desc())
                .limit(1)
            )
            row = result.first()
            if row is None:
                return None
            return cast(tuple[int, str, str | None], tuple(row))

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

    _PEG_UPDATABLE_FIELDS: frozenset[str] = frozenset({"failure_reason", "halted_at"})
    _PEL_UPDATABLE_FIELDS: frozenset[str] = frozenset(
        {
            "command_public_id",
            "client_order_id",
            "exchange_order_id",
            "filled_signed_qty",
            "compensated_signed_qty",
            "compensation_seq",
            "last_venue_event_id",
        }
    )
    _PEL_FILL_TERMINAL_STATUSES: frozenset[str] = frozenset(
        {
            PairedExecutionLegStatusEnum.REJECTED.value,
            PairedExecutionLegStatusEnum.CANCELLED.value,
            PairedExecutionLegStatusEnum.EXPIRED.value,
            PairedExecutionLegStatusEnum.BROKEN.value,
            PairedExecutionLegStatusEnum.COMPENSATING.value,
            PairedExecutionLegStatusEnum.FLATTENED.value,
            PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value,
        }
    )
    _PEL_TERMINAL_PROJECT_SKIP_STATUSES: frozenset[str] = _PEL_FILL_TERMINAL_STATUSES | frozenset(
        {PairedExecutionLegStatusEnum.FILLED.value}
    )
    _PEL_LATE_FILL_REOPEN_STATUSES: frozenset[str] = frozenset(
        {
            PairedExecutionLegStatusEnum.FLATTENED.value,
            PairedExecutionLegStatusEnum.BROKEN.value,
        }
    )
    _PEL_SETTLED_STATUSES: frozenset[str] = frozenset(
        {
            PairedExecutionLegStatusEnum.FLATTENED.value,
            PairedExecutionLegStatusEnum.CANCELLED.value,
            PairedExecutionLegStatusEnum.EXPIRED.value,
            PairedExecutionLegStatusEnum.REJECTED.value,
            PairedExecutionLegStatusEnum.FILLED.value,
        }
    )
    _PEG_COMPLETABLE_STATUSES: frozenset[str] = frozenset(
        {
            PairedExecutionGroupStatusEnum.ARMED.value,
            PairedExecutionGroupStatusEnum.BROKEN.value,
            PairedExecutionGroupStatusEnum.COMPENSATING.value,
        }
    )
    _PEG_EXPOSED_STATUSES: frozenset[str] = frozenset(
        {
            PairedExecutionGroupStatusEnum.BROKEN.value,
            PairedExecutionGroupStatusEnum.COMPENSATING.value,
            PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
        }
    )
    _PEC_QTY_EPSILON: float = 1e-12

    @staticmethod
    def _paired_execution_group_row_to_dict(
        group: PairedExecutionGroup,
    ) -> PairedExecutionGroupRow:
        """Project a PairedExecutionGroup ORM row into the TypedDict shape."""
        return PairedExecutionGroupRow(
            id=group.id,
            public_id=group.public_id,
            session_id=group.session_id,
            sequence_id=group.sequence_id,
            timestamp=group.timestamp,
            known_to=group.known_to,
            wallet_public_id=group.wallet_public_id,
            operator_public_id=group.operator_public_id,
            strategy_id=group.strategy_id,
            policy=group.policy,
            expected_leg_count=group.expected_leg_count,
            group_key=group.group_key,
            status=group.status,
            assembly_deadline=group.assembly_deadline,
            fill_deadline=group.fill_deadline,
            failure_reason=group.failure_reason,
            halted_at=group.halted_at,
            created_at=group.created_at,
        )

    @staticmethod
    def _paired_execution_leg_row_to_dict(
        leg: PairedExecutionLeg,
    ) -> PairedExecutionLegRow:
        """Project a PairedExecutionLeg ORM row into the TypedDict shape."""
        return PairedExecutionLegRow(
            id=leg.id,
            public_id=leg.public_id,
            session_id=leg.session_id,
            sequence_id=leg.sequence_id,
            timestamp=leg.timestamp,
            known_to=leg.known_to,
            group_public_id=leg.group_public_id,
            leg_index=leg.leg_index,
            exchange=leg.exchange,
            mode=leg.mode,
            instrument=leg.instrument,
            shard_key=leg.shard_key,
            side=leg.side,
            target_qty=leg.target_qty,
            signal_public_id=leg.signal_public_id,
            command_public_id=leg.command_public_id,
            client_order_id=leg.client_order_id,
            exchange_order_id=leg.exchange_order_id,
            status=leg.status,
            filled_signed_qty=leg.filled_signed_qty,
            compensated_signed_qty=leg.compensated_signed_qty,
            compensation_seq=leg.compensation_seq,
            last_venue_event_id=leg.last_venue_event_id,
            wallet_public_id=leg.wallet_public_id,
            operator_public_id=leg.operator_public_id,
            created_at=leg.created_at,
        )

    @staticmethod
    def _paired_execution_halt_row_to_dict(
        halt: PairedExecutionHalt,
    ) -> PairedExecutionHaltRow:
        """Project a PairedExecutionHalt ORM row into the TypedDict shape."""
        return PairedExecutionHaltRow(
            id=halt.id,
            public_id=halt.public_id,
            session_id=halt.session_id,
            sequence_id=halt.sequence_id,
            timestamp=halt.timestamp,
            known_to=halt.known_to,
            wallet_public_id=halt.wallet_public_id,
            operator_public_id=halt.operator_public_id,
            strategy_id=halt.strategy_id,
            mode=halt.mode,
            group_key=halt.group_key,
            group_public_id=halt.group_public_id,
            reason=halt.reason,
            created_at=halt.created_at,
        )

    async def insert_paired_execution_group(self, row: PairedExecutionGroupInsertRow) -> str:
        """Insert a paired-execution group and return its public_id."""
        async with self.session() as s:
            group = PairedExecutionGroup(**row)
            s.add(group)
            await s.commit()
            await s.refresh(group)
            return group.public_id

    async def insert_paired_execution_leg(self, row: PairedExecutionLegInsertRow) -> str:
        """Insert a paired-execution leg and return its public_id."""
        async with self.session() as s:
            leg = PairedExecutionLeg(**row)
            s.add(leg)
            await s.commit()
            await s.refresh(leg)
            return leg.public_id

    async def insert_paired_execution_halt(self, row: PairedExecutionHaltInsertRow) -> str:
        """Insert a paired-execution halt and return its public_id."""
        async with self.session() as s:
            halt = PairedExecutionHalt(**row)
            s.add(halt)
            await s.commit()
            await s.refresh(halt)
            return halt.public_id

    async def ensure_paired_execution_halt(self, row: PairedExecutionHaltInsertRow) -> bool:
        """Insert a paired-execution halt unless an active one already exists.

        Idempotent across racing coordinators that each observe the same
        broken / compensating group on a later scan: the active-unique
        ``uq_peh_scope`` index admits exactly one active halt per
        ``(wallet_public_id, strategy_id, group_key)`` scope. On an
        ``IntegrityError`` the method re-checks for an active halt with that
        scope; if one exists the error was the expected active-unique
        collision and ``False`` is returned, but ANY other integrity failure
        (a malformed row violating a different constraint) is RE-RAISED
        rather than silently swallowed. Mirrors
        :meth:`ensure_paired_execution_group`. Returns ``True`` iff this call
        created the halt.
        """
        async with self.session() as s:
            s.add(PairedExecutionHalt(**row))
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()
                active = (
                    await s.execute(
                        select(PairedExecutionHalt.id).where(
                            PairedExecutionHalt.wallet_public_id == row.get("wallet_public_id"),
                            PairedExecutionHalt.strategy_id == row.get("strategy_id"),
                            PairedExecutionHalt.group_key == row.get("group_key"),
                            PairedExecutionHalt.known_to == KNOWN_TO_MAX,
                        )
                    )
                ).first()
                if active is None:
                    raise
                return False
            return True

    async def cas_paired_execution_group_status(
        self,
        public_id: str,
        expected_status: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        updates: PairedExecutionGroupFieldUpdate | None = None,
    ) -> bool:
        """CAS a paired-execution group status via SCD2 close-and-insert.

        ``updates`` may only carry mutable group fields
        (``failure_reason`` / ``halted_at``); any other key — a typo or a
        forbidden SCD2 column such as ``status`` / ``public_id`` /
        ``known_to`` — raises ``ValueError`` before any row is touched, so
        a bad caller can never silently drop a write or corrupt the SCD2
        chain.
        """
        if updates is not None:
            unknown = set(updates) - self._PEG_UPDATABLE_FIELDS
            if unknown:
                raise ValueError(f"unknown paired-execution group update fields: {sorted(unknown)}")
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(PairedExecutionGroup)
                        .where(
                            PairedExecutionGroup.public_id == public_id,
                            *where_active(PairedExecutionGroup, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return False
            if existing.status != expected_status:
                return False
            await s.execute(
                update(PairedExecutionGroup)
                .where(PairedExecutionGroup.id == existing.id)
                .values(known_to=bus_time)
            )
            new_row = PairedExecutionGroup(
                public_id=existing.public_id,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                strategy_id=existing.strategy_id,
                policy=existing.policy,
                expected_leg_count=existing.expected_leg_count,
                group_key=existing.group_key,
                status=new_status,
                assembly_deadline=existing.assembly_deadline,
                fill_deadline=existing.fill_deadline,
                failure_reason=existing.failure_reason,
                halted_at=existing.halted_at,
                created_at=existing.created_at,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            if updates is not None:
                for key, value in updates.items():
                    setattr(new_row, key, value)
            s.add(new_row)
            await s.commit()
            return True

    async def cas_paired_execution_leg_status(
        self,
        public_id: str,
        expected_status: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        updates: PairedExecutionLegFieldUpdate | None = None,
    ) -> bool:
        """CAS a paired-execution leg status via SCD2 close-and-insert.

        ``updates`` may only carry mutable leg fields (venue ids and the
        signed fill / compensation accounting); any other key — a typo or
        a forbidden SCD2 / identity column — raises ``ValueError`` before
        any row is touched, so a bad caller can never silently drop a
        write or corrupt the SCD2 chain.
        """
        if updates is not None:
            unknown = set(updates) - self._PEL_UPDATABLE_FIELDS
            if unknown:
                raise ValueError(f"unknown paired-execution leg update fields: {sorted(unknown)}")
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.public_id == public_id,
                            *where_active(PairedExecutionLeg, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing is None:
                return False
            if existing.status != expected_status:
                return False
            await s.execute(
                update(PairedExecutionLeg)
                .where(PairedExecutionLeg.id == existing.id)
                .values(known_to=bus_time)
            )
            new_row = PairedExecutionLeg(
                public_id=existing.public_id,
                group_public_id=existing.group_public_id,
                leg_index=existing.leg_index,
                exchange=existing.exchange,
                mode=existing.mode,
                instrument=existing.instrument,
                shard_key=existing.shard_key,
                side=existing.side,
                target_qty=existing.target_qty,
                signal_public_id=existing.signal_public_id,
                command_public_id=existing.command_public_id,
                client_order_id=existing.client_order_id,
                exchange_order_id=existing.exchange_order_id,
                status=new_status,
                filled_signed_qty=existing.filled_signed_qty,
                compensated_signed_qty=existing.compensated_signed_qty,
                compensation_seq=existing.compensation_seq,
                last_venue_event_id=existing.last_venue_event_id,
                wallet_public_id=existing.wallet_public_id,
                operator_public_id=existing.operator_public_id,
                created_at=existing.created_at,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            if updates is not None:
                for key, value in updates.items():
                    setattr(new_row, key, value)
            s.add(new_row)
            await s.commit()
            return True

    async def project_paired_execution_leg_fill(
        self,
        client_order_id: str,
        filled_signed_qty: float,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None = None,
        last_venue_event_id: int | None = None,
    ) -> PairedFillProjection:
        """Project a venue fill onto the CURRENT active paired-execution leg.

        Resolves the leg by its ``client_order_id`` (the leg's ORIGINAL order
        id; a non-grouped fill matches no leg and returns
        :attr:`PairedFillProjection.NO_MATCH`, the signal the live hook uses to
        then try the compensation projection). Guards the CURRENT active row
        (``known_to == KNOWN_TO_MAX``) under ``FOR UPDATE`` and applies MONOTONIC
        cumulative accounting: ``filled_signed_qty`` is replaced only when the
        incoming signed cumulative MAGNITUDE strictly exceeds the stored one, so
        a duplicate / out-of-order replay never regresses the leg. On apply it
        SCD2 close-and-inserts the successor carrying the new
        ``filled_signed_qty`` / ``status`` / ``exchange_order_id`` /
        ``last_venue_event_id`` (and all other columns forward). Returns
        :attr:`PairedFillProjection.ORIGINAL_APPLIED` iff a successor was
        written, :attr:`~PairedFillProjection.ORIGINAL_NOOP` when a leg matched
        but the monotonic guard left it unchanged, and
        :attr:`~PairedFillProjection.NO_MATCH` when no leg owns the order.

        A late ORIGINAL fill that arrives AFTER the leg went post-break is
        NOT dropped — the monotonic update still applies
        (real venue exposure only grows), and the successor status follows
        :meth:`_late_original_fill_status`: a ``flattened`` / ``broken`` leg whose
        ``open_qty`` reappears goes back to ``filled`` so the compensation sweep
        re-flattens the residual on the next ``compensation_seq``; a
        ``compensating`` leg (a flatten is in flight) keeps ``compensating`` to
        avoid a double-flatten; ``cancelled`` / ``expired`` / ``rejected`` keep
        their status (the sweep already flattens those); and
        ``manual_intervention`` keeps its status (the operator owns it). A normal
        in-flight leg uses the venue ``new_status`` as before.

        A late growing fill that lands while the leg's GROUP is already
        ``completed`` additionally reopens the group to
        ``compensating`` in the SAME transaction whenever the leg's resulting
        ``open_qty`` is non-zero — the guard scanner never lists completed
        groups (terminal-status listing is unbounded), so without the reopen
        the re-exposed leg would be invisible forever. LOCK ORDER: the
        completion DAL locks group THEN legs, so the reopen path here also
        locks the GROUP FIRST and only then the leg
        (:meth:`_project_original_fill_grouped`); the common path keeps
        today's leg-only lock (:meth:`_project_original_fill_fast`) and never
        waits on a group lock, so no lock cycle exists. The fast path detects
        a completed group with a NON-LOCKING read just before writing and
        retries once through the group-first path, closing the race where the
        group completes between the read and the leg lock.

        Unlike the generic :func:`close_and_insert` helper (which matches the
        predecessor with a temporal ``timestamp <= bus_time`` filter and so
        silently skips a future-stamped row), this method matches the CURRENT
        active row regardless of its ``timestamp`` — recovery deliberately
        re-projects a leg armed by a sibling coordinator whose clock ran
        slightly ahead. To keep the SCD2 validity intervals non-inverted in
        that clock-skew case, the close / successor bus time is clamped to
        ``max(bus_time, existing.timestamp)``: the predecessor's ``known_to``
        is never set before its own ``timestamp`` and the successor is never
        backdated before its predecessor. In the normal case
        (``bus_time >= existing.timestamp``) the clamp is a no-op.
        """
        result = await self._project_original_fill_fast(
            client_order_id,
            filled_signed_qty,
            new_status,
            bus_time,
            session_id,
            sequence_id,
            exchange_order_id,
            last_venue_event_id,
        )
        if result is not None:
            return result
        return await self._project_original_fill_grouped(
            client_order_id,
            filled_signed_qty,
            new_status,
            bus_time,
            session_id,
            sequence_id,
            exchange_order_id,
            last_venue_event_id,
        )

    async def _project_original_fill_fast(
        self,
        client_order_id: str,
        filled_signed_qty: float,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None,
        last_venue_event_id: int | None,
    ) -> PairedFillProjection | None:
        """Apply an original fill under the leg-only lock, or defer to the group path.

        This is the common path and keeps the pre-5d.3 locking exactly: the
        CURRENT active leg is locked ``FOR UPDATE`` by ``client_order_id`` and
        no group lock is ever taken, so it can never participate in a lock
        cycle with the completion DAL (which locks group then legs). Just
        before writing, when the resulting ``open_qty`` is non-zero, the
        group's status is read WITHOUT a lock; a ``completed`` group means the
        write must also reopen the group, which requires the group-first lock
        order — so this path returns ``None`` (nothing written, the session
        discards the close) and the caller retries through
        :meth:`_project_original_fill_grouped`. Every other outcome is final.
        """
        async with self.session() as s:
            existing = await self._lock_leg_by_client_order_id(s, client_order_id)
            if existing is None:
                return PairedFillProjection.NO_MATCH
            if abs(filled_signed_qty) <= abs(existing.filled_signed_qty):
                return PairedFillProjection.ORIGINAL_NOOP
            open_qty = filled_signed_qty - existing.compensated_signed_qty
            if abs(open_qty) >= self._PEC_QTY_EPSILON:
                group_status = (
                    await s.execute(
                        select(PairedExecutionGroup.status).where(
                            PairedExecutionGroup.public_id == existing.group_public_id,
                            PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                        )
                    )
                ).scalar_one_or_none()
                if group_status == PairedExecutionGroupStatusEnum.COMPLETED.value:
                    return None
            await self._write_original_fill_successor(
                s,
                existing,
                filled_signed_qty,
                new_status,
                bus_time,
                session_id,
                sequence_id,
                exchange_order_id,
                last_venue_event_id,
            )
            await s.commit()
            return PairedFillProjection.ORIGINAL_APPLIED

    async def _project_original_fill_grouped(
        self,
        client_order_id: str,
        filled_signed_qty: float,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None,
        last_venue_event_id: int | None,
    ) -> PairedFillProjection:
        """Apply an original fill under the group-first lock, reopening if completed.

        The completed-group reopen path: an UNLOCKED peek resolves the leg (and
        its immutable ``group_public_id``), the GROUP is locked ``FOR UPDATE``
        FIRST (matching the completion DAL's lock order, so the two can never
        deadlock), then the leg is locked and every guard re-validated against
        the locked rows. When the locked group is (still) ``completed`` and the
        leg's resulting ``open_qty`` is non-zero, the group is SCD2-reopened to
        ``compensating`` in the same transaction — stamping ``failure_reason``
        / ``halted_at`` if unset — so the next scanner cycle re-halts the scope
        and re-flattens the residual. A group that is no longer ``completed``
        under the lock needs no successor (the scanner already sees it); the
        leg write proceeds either way.
        """
        async with self.session() as s:
            peek = await self._lock_leg_by_client_order_id(s, client_order_id, lock=False)
            if peek is None:
                return PairedFillProjection.NO_MATCH
            group = (
                (
                    await s.execute(
                        select(PairedExecutionGroup)
                        .where(
                            PairedExecutionGroup.public_id == peek.group_public_id,
                            PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            existing = await self._lock_leg_by_client_order_id(s, client_order_id)
            if existing is None:
                return PairedFillProjection.NO_MATCH
            if abs(filled_signed_qty) <= abs(existing.filled_signed_qty):
                return PairedFillProjection.ORIGINAL_NOOP
            await self._write_original_fill_successor(
                s,
                existing,
                filled_signed_qty,
                new_status,
                bus_time,
                session_id,
                sequence_id,
                exchange_order_id,
                last_venue_event_id,
            )
            open_qty = filled_signed_qty - existing.compensated_signed_qty
            if (
                group is not None
                and group.status == PairedExecutionGroupStatusEnum.COMPLETED.value
                and abs(open_qty) >= self._PEC_QTY_EPSILON
            ):
                group_bus_time = max(bus_time, group.timestamp)
                await s.execute(
                    update(PairedExecutionGroup)
                    .where(PairedExecutionGroup.id == group.id)
                    .values(known_to=group_bus_time)
                )
                s.add(
                    PairedExecutionGroup(
                        public_id=group.public_id,
                        wallet_public_id=group.wallet_public_id,
                        operator_public_id=group.operator_public_id,
                        strategy_id=group.strategy_id,
                        policy=group.policy,
                        expected_leg_count=group.expected_leg_count,
                        group_key=group.group_key,
                        status=PairedExecutionGroupStatusEnum.COMPENSATING.value,
                        assembly_deadline=group.assembly_deadline,
                        fill_deadline=group.fill_deadline,
                        failure_reason=group.failure_reason or "late fill after completion",
                        halted_at=group.halted_at or group_bus_time,
                        created_at=group.created_at,
                        session_id=session_id,
                        sequence_id=sequence_id,
                        timestamp=group_bus_time,
                    )
                )
            await s.commit()
            return PairedFillProjection.ORIGINAL_APPLIED

    @staticmethod
    async def _lock_leg_by_client_order_id(
        s: AsyncSession, client_order_id: str, lock: bool = True
    ) -> PairedExecutionLeg | None:
        """Resolve the CURRENT active leg for an original order id, optionally locked.

        Shared by the fast and group-first fill paths. Raises the
        data-integrity ``ValueError`` when two active legs share the
        ``client_order_id`` (the active-unique invariant the fill projection
        has guarded since 5a); returns ``None`` when no leg owns the order.
        ``lock=False`` is the group-first path's unlocked peek (it only needs
        the immutable ``group_public_id`` before taking the group lock).
        """
        stmt = (
            select(PairedExecutionLeg)
            .where(
                PairedExecutionLeg.client_order_id == client_order_id,
                PairedExecutionLeg.known_to == KNOWN_TO_MAX,
            )
            .limit(2)
        )
        if lock:
            stmt = stmt.with_for_update()
        matches = (await s.execute(stmt)).scalars().all()
        if len(matches) > 1:
            raise ValueError(
                "paired-execution data integrity: "
                f"{len(matches)} active legs share client_order_id {client_order_id}"
            )
        return matches[0] if matches else None

    async def _write_original_fill_successor(
        self,
        s: AsyncSession,
        existing: PairedExecutionLeg,
        filled_signed_qty: float,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None,
        last_venue_event_id: int | None,
    ) -> None:
        """Close the locked leg and stage its original-fill SCD2 successor.

        Shared write half of the fast and group-first fill paths: picks the
        successor status via :meth:`_late_original_fill_status`, clamps the
        close / successor bus time to ``max(bus_time, existing.timestamp)``
        and carries the venue ids forward when the caller omitted them. The
        caller commits (its transaction may also carry a group reopen).
        """
        successor_status = self._late_original_fill_status(
            existing.status,
            filled_signed_qty,
            existing.compensated_signed_qty,
            new_status,
        )
        effective_bus_time = max(bus_time, existing.timestamp)
        await s.execute(
            update(PairedExecutionLeg)
            .where(PairedExecutionLeg.id == existing.id)
            .values(known_to=effective_bus_time)
        )
        s.add(
            self._leg_successor(
                existing,
                status=successor_status,
                filled_signed_qty=filled_signed_qty,
                exchange_order_id=(
                    exchange_order_id
                    if exchange_order_id is not None
                    else existing.exchange_order_id
                ),
                last_venue_event_id=(
                    last_venue_event_id
                    if last_venue_event_id is not None
                    else existing.last_venue_event_id
                ),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=effective_bus_time,
            )
        )

    @classmethod
    def _late_original_fill_status(
        cls,
        existing_status: str,
        filled_signed_qty: float,
        compensated_signed_qty: float,
        normal_status: str,
    ) -> str:
        """Pick a leg's successor status for a (possibly late) ORIGINAL fill.

        A fill on a leg still in a normal in-flight state (``pending`` / ``armed``
        / ``working`` / ``partially_filled``) uses the venue ``normal_status``
        (``filled`` / ``partially_filled``) as before. A LATE fill that grows
        exposure on a leg already in a fill-terminal state instead
        transitions per the safe re-entry FSM:

        - ``flattened`` / ``broken`` → ``filled`` when ``open_qty`` reappears (so
          the compensation sweep re-flattens the residual on the next
          ``compensation_seq``), else the status is kept.
        - ``compensating`` → kept: a flatten order is in flight, so re-flattening
          now would double-flatten; the residual is settled when that flatten
          terminalizes (:meth:`_apply_leg_compensation`).
        - ``cancelled`` / ``expired`` / ``rejected`` → kept: those are already
          flatten-eligible, so the sweep flattens the grown exposure.
        - ``manual_intervention`` → kept: an operator owns the leg; auto-flatten
          must never override an escalation.
        """
        if existing_status not in cls._PEL_FILL_TERMINAL_STATUSES:
            return normal_status
        if existing_status in cls._PEL_LATE_FILL_REOPEN_STATUSES:
            open_qty = filled_signed_qty - compensated_signed_qty
            if abs(open_qty) >= cls._PEC_QTY_EPSILON:
                return PairedExecutionLegStatusEnum.FILLED.value
        return existing_status

    async def project_paired_execution_compensation_fill(
        self,
        flatten_client_order_id: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        flatten_terminal: bool = False,
    ) -> PairedFillProjection:
        """Project a reduce-only FLATTEN order's venue event onto its leg's compensation.

        Routes BOTH a flatten order's fill (live fill hook) and its venue terminal
        — cancel / expire / reject (live terminal hook) — since each
        carries the flatten's FRESH ``client_order_id`` and a command whose
        ``supersedes_command_id`` is the leg's ORIGINAL command, so it matches NO
        leg by ``client_order_id`` (the original projection returns ``NO_MATCH``
        and the hook routes here). This resolves the active flatten ``TradeCommand``
        by ``client_order_id`` (a ``reduce_only`` ``submit`` with a non-null
        ``supersedes_command_id``; ``NO_MATCH`` when none, so an ordinary
        reduce-only order that supersedes nothing — or a non-flatten original
        terminal — is ignored), follows ``supersedes_command_id`` to the leg whose
        ``command_public_id`` matches (``NO_MATCH`` when the superseded command is
        not a leg's), then recomputes ``compensated_signed_qty`` and settles the
        leg status from the AUTHORITATIVE ``venue_events`` via
        :meth:`_apply_leg_compensation` (the event is only a trigger — the
        recompute is event-agnostic and replay-safe). ``flatten_terminal`` is set
        by the live terminal hook so a cancel / expire / reject MESSAGE settles its
        leg even before the executor's durable terminal row is visible (it
        publishes the message BEFORE recording the row, and a not-tradeable reject
        records no row at all); the hint only escalates terminality for the order
        the event names, so it cannot reopen a leg whose newer flatten is still
        live. Raises on a data-integrity duplicate (two active commands sharing the
        flatten id); the superseded original command is active-unique on the leg,
        so at most one leg matches. The result is
        :attr:`PairedFillProjection.COMPENSATION_APPLIED` /
        :attr:`~PairedFillProjection.COMPENSATION_NOOP` /
        :attr:`~PairedFillProjection.NO_MATCH`.
        """
        async with self.session() as s:
            commands = (
                (
                    await s.execute(
                        select(TradeCommand)
                        .where(
                            TradeCommand.client_order_id == flatten_client_order_id,
                            TradeCommand.known_to == KNOWN_TO_MAX,
                            TradeCommand.command_type == "submit",
                            TradeCommand.reduce_only.is_(True),
                            TradeCommand.supersedes_command_id.isnot(None),
                        )
                        .limit(2)
                    )
                )
                .scalars()
                .all()
            )
            if len(commands) > 1:
                raise ValueError(
                    "paired-execution data integrity: "
                    f"{len(commands)} active flatten commands share "
                    f"client_order_id {flatten_client_order_id}"
                )
            command = commands[0] if commands else None
            if command is None:
                return PairedFillProjection.NO_MATCH
            leg = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.command_public_id == command.supersedes_command_id,
                            PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if leg is None:
                return PairedFillProjection.NO_MATCH
            return await self._apply_leg_compensation(
                s,
                leg,
                bus_time,
                session_id,
                sequence_id,
                trigger_client_order_id=flatten_client_order_id,
                trigger_terminal=flatten_terminal,
            )

    async def reproject_paired_execution_leg_compensation(
        self,
        leg_public_id: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> PairedFillProjection:
        """Recompute one leg's compensated qty from venue_events on startup recovery.

        The live compensation-fill projection only runs on the live fill path, so
        a flatten order that filled while the coordinator was DOWN would recover
        with a stale ``compensated_signed_qty``. This pass re-derives it from the
        AUTHORITATIVE ``venue_events`` for the leg resolved by ``public_id``
        (current-active, ``FOR UPDATE``), mirroring the original-fill recovery
        parity. A leg already projected live is a ``COMPENSATION_NOOP``; a
        downtime flatten fill is restored. Returns ``NO_MATCH`` when the leg is
        gone. The leg ``public_id`` is active-unique, so at most one row matches.
        """
        async with self.session() as s:
            leg = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.public_id == leg_public_id,
                            PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if leg is None:
                return PairedFillProjection.NO_MATCH
            return await self._apply_leg_compensation(s, leg, bus_time, session_id, sequence_id)

    async def _apply_leg_compensation(
        self,
        s: AsyncSession,
        leg: PairedExecutionLeg,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        trigger_client_order_id: str | None = None,
        trigger_terminal: bool = False,
    ) -> PairedFillProjection:
        """Recompute and persist a leg's ``compensated_signed_qty`` from venue_events.

        Shared by the live compensation-fill projection and startup recovery, on a
        leg already locked ``FOR UPDATE``. ``compensated_signed_qty`` is the SIGNED
        sum (buy +, sell −) of EVERY reduce-only flatten command superseding the
        leg's ORIGINAL command, NEGATED so it moves toward ``filled_signed_qty``
        under the guard's ``open_qty = filled_signed_qty − compensated_signed_qty``
        convention (a net-long leg flattens by SELLING, so its flatten fills are
        negative and ``compensated`` grows positive toward the positive
        ``filled``). The sign uses the COMMAND side, not the venue-event side, so a
        mislabelled venue echo cannot invert it. Each flatten order contributes its
        MAX cumulative fill (replay-safe and additive across re-compensation
        rounds, unlike a single monotonic scalar whose round-2 order restarts at
        zero). The successor status (when the leg is ``compensating``) is chosen
        by :meth:`_compensation_successor_status`: ``flattened`` when the open
        residual collapses to zero (within :data:`_PEC_QTY_EPSILON`); else, when
        the CURRENT-``compensation_seq`` flatten order has TERMINALIZED (fully
        filled or venue cancel / expire / reject) while exposure remains —
        because a late ORIGINAL fill grew ``filled`` past what that round
        flattened — back to ``filled`` so the sweep re-flattens the residual on
        the NEXT ``compensation_seq``; else ``compensating``
        is kept (a flatten is still in flight — re-flattening now would
        double-flatten). ``manual_intervention`` and every non-``compensating``
        status are carried forward UNCHANGED (auto-flatten must never override an
        operator escalation). Writes an SCD2 successor (bus time clamped to
        ``max(bus_time, leg.timestamp)``) only when the value or status actually
        changes, else returns ``COMPENSATION_NOOP``.

        Settling on the CURRENT-seq flatten's terminality (not the triggering
        event) inherently ignores a STALE terminal for an older round: a late
        duplicate cancel for ``flatten:1`` arriving after ``flatten:2`` is in
        flight recomputes accounting but never reopens, so it cannot cause a
        double-flatten.

        FAILS CLOSED on corruption rather than mis-accounting real money: two
        active flatten commands sharing a ``client_order_id`` would each read the
        SAME venue max-cumulative and double-count it (falsely zeroing ``open_qty``
        and FLATTENING the leg), and an unexpected command ``side`` would be
        silently treated as a buy — both raise ``ValueError`` here instead.
        """
        original_command_id = leg.command_public_id
        if original_command_id is None:
            return PairedFillProjection.COMPENSATION_NOOP
        flatten_orders = (
            await s.execute(
                select(
                    TradeCommand.client_order_id,
                    TradeCommand.side,
                    TradeCommand.idempotency_key,
                    TradeCommand.quantity,
                ).where(
                    TradeCommand.supersedes_command_id == original_command_id,
                    TradeCommand.known_to == KNOWN_TO_MAX,
                    TradeCommand.command_type == "submit",
                    TradeCommand.reduce_only.is_(True),
                )
            )
        ).all()
        current_seq_key = (
            f"paired:{leg.group_public_id}:{leg.public_id}:flatten:{leg.compensation_seq}"
        )
        compensated = 0.0
        seen_orders: set[str] = set()
        current_flatten: tuple[str, float, float] | None = None
        for client_order_id, side, idempotency_key, quantity in flatten_orders:
            if client_order_id in seen_orders:
                raise ValueError(
                    "paired-execution data integrity: duplicate active flatten "
                    f"client_order_id {client_order_id} supersedes command "
                    f"{original_command_id}"
                )
            seen_orders.add(client_order_id)
            if side == TradeSideEnum.SELL.value:
                direction = 1.0
            elif side == TradeSideEnum.BUY.value:
                direction = -1.0
            else:
                raise ValueError(
                    "paired-execution data integrity: flatten command "
                    f"{client_order_id} has unexpected side {side!r}"
                )
            cum_fill = await self._max_cumulative_fill(s, client_order_id)
            if idempotency_key == current_seq_key:
                current_flatten = (client_order_id, quantity, cum_fill)
            if cum_fill <= 0.0:
                continue
            compensated += direction * cum_fill
        current_flatten_terminal = await self._current_flatten_is_terminal(
            s, current_flatten, trigger_client_order_id, trigger_terminal
        )
        new_status = self._compensation_successor_status(leg, compensated, current_flatten_terminal)
        if (
            abs(compensated - leg.compensated_signed_qty) < self._PEC_QTY_EPSILON
            and new_status == leg.status
        ):
            return PairedFillProjection.COMPENSATION_NOOP
        effective_bus_time = max(bus_time, leg.timestamp)
        await s.execute(
            update(PairedExecutionLeg)
            .where(PairedExecutionLeg.id == leg.id)
            .values(known_to=effective_bus_time)
        )
        s.add(
            self._leg_successor(
                leg,
                status=new_status,
                filled_signed_qty=leg.filled_signed_qty,
                exchange_order_id=leg.exchange_order_id,
                last_venue_event_id=leg.last_venue_event_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=effective_bus_time,
                compensated_signed_qty=compensated,
            )
        )
        await s.commit()
        return PairedFillProjection.COMPENSATION_APPLIED

    def _compensation_successor_status(
        self,
        leg: PairedExecutionLeg,
        compensated: float,
        current_flatten_terminal: bool,
    ) -> str:
        """Pick a compensating leg's successor status from its recomputed exposure.

        Only a ``compensating`` leg transitions (every other status is carried
        forward, so an operator ``manual_intervention`` is never auto-cleared).
        For a ``compensating`` leg: a zero open residual settles to ``flattened``;
        otherwise, if the CURRENT-``compensation_seq`` flatten order has
        terminalized while exposure remains, the leg reopens to ``filled`` so the
        sweep re-flattens the residual on the next round; otherwise the flatten is
        still in flight and ``compensating`` is kept (re-flattening now would
        double-flatten).
        """
        if leg.status != PairedExecutionLegStatusEnum.COMPENSATING.value:
            return leg.status
        open_qty = leg.filled_signed_qty - compensated
        if abs(open_qty) < self._PEC_QTY_EPSILON:
            return PairedExecutionLegStatusEnum.FLATTENED.value
        if current_flatten_terminal:
            return PairedExecutionLegStatusEnum.FILLED.value
        return leg.status

    async def _current_flatten_is_terminal(
        self,
        s: AsyncSession,
        current_flatten: tuple[str, float, float] | None,
        trigger_client_order_id: str | None,
        trigger_terminal: bool,
    ) -> bool:
        """Return whether the leg's CURRENT-seq flatten order has terminalized.

        Terminal when, for the current-seq flatten order
        (``current_flatten = (client_order_id, quantity, max_cum_fill)``): the
        TRIGGERING live event is itself this order's terminal (``trigger_terminal``
        for the matching ``client_order_id``) — which closes the executor's
        publish-BEFORE-record window where a reject message reaches the trader
        before its durable ``order_rejected`` row — OR the AUTHORITATIVE
        ``venue_events`` already record it as fully filled (max cumulative fill
        reached its ``quantity`` within :data:`_PEC_QTY_EPSILON`) or carry a
        durable ``order_terminal`` (cancel / expire) / ``order_rejected`` event.
        The DB clause makes the determination identical live and on restart /
        scanner-backstop recovery (where no live message exists), and replay-safe;
        the trigger clause only ever ESCALATES terminality for the current-seq
        order, so it cannot reopen a leg whose newer flatten is still live.

        RESIDUAL (accepted, fail-safe): a flatten rejected LOCALLY as not-tradeable
        records NO durable ``venue_events`` row (only the exchange / exception
        reject paths do) and fires only a live message. If that message is also
        missed (coordinator down), no durable terminality signal exists and the leg
        stays ``compensating`` + HALTED until an operator / reconciliation resolves
        it. This is CORRECT rather than a wedge: a flatten is only not-tradeable
        when its instrument was delisted / halted mid-compensation, in which case
        the exposure cannot be auto-flattened at the venue at all and MUST go to an
        operator — so halting is the right terminal state, not a lost reopen.
        """
        if current_flatten is None:
            return False
        client_order_id, quantity, cum_fill = current_flatten
        if trigger_terminal and trigger_client_order_id == client_order_id:
            return True
        if cum_fill >= quantity - self._PEC_QTY_EPSILON:
            return True
        terminal_event = await s.execute(
            select(VenueEvent.id)
            .where(
                VenueEvent.client_order_id == client_order_id,
                VenueEvent.event_type.in_(
                    ("order_terminal", "order_rejected", "order_breaker_open")
                ),
            )
            .limit(1)
        )
        return terminal_event.first() is not None

    @staticmethod
    async def _max_cumulative_fill(s: AsyncSession, client_order_id: str) -> float:
        """Return the MAX cumulative ``fill_observed`` size for an order, or 0.0.

        Session-bound twin of :meth:`get_max_cumulative_fill_venue_event` used
        inside the compensation recompute so the leg lock and the venue-event read
        share one transaction. Selects the greatest non-null ``cum_fill_size`` so
        an out-of-order executor write can never make the recompute read a smaller
        cumulative than was actually filled.
        """
        result = await s.execute(
            select(VenueEvent.cum_fill_size)
            .where(
                VenueEvent.client_order_id == client_order_id,
                VenueEvent.event_type == "fill_observed",
                VenueEvent.cum_fill_size.isnot(None),
            )
            .order_by(VenueEvent.cum_fill_size.desc(), VenueEvent.id.desc())
            .limit(1)
        )
        row = result.first()
        if row is None:
            return 0.0
        return float(row[0])

    async def project_paired_execution_leg_terminal(
        self,
        client_order_id: str,
        new_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        exchange_order_id: str | None = None,
    ) -> bool:
        """Project a venue terminal (reject / cancel / expire) onto the active leg.

        Resolves the leg by its ``client_order_id`` (a non-grouped terminal
        matches no leg and is a no-op). Guards the CURRENT active row
        (``known_to == KNOWN_TO_MAX``) under ``FOR UPDATE`` and raises on >1
        active legs sharing the id (the same data-integrity guard the fill
        projection uses). Skips a leg already FILLED or already in a
        fill-terminal state (``_PEL_TERMINAL_PROJECT_SKIP_STATUSES``): a fully
        filled order needs no breakage, and a re-delivered terminal event is a
        no-op. Otherwise it SCD2 close-and-inserts the successor carrying the new
        terminal ``status`` while PRESERVING ``filled_signed_qty`` /
        ``compensated_signed_qty`` (a partially-filled leg that then cancels keeps
        its real exposure for the compensator) and every other column forward.
        The close / successor bus time is clamped to
        ``max(bus_time, existing.timestamp)`` for the same clock-skew SCD2
        non-inversion reason as the fill projection. Returns ``True`` iff a
        successor was written.
        """
        async with self.session() as s:
            matches = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.client_order_id == client_order_id,
                            PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                        .limit(2)
                    )
                )
                .scalars()
                .all()
            )
            if len(matches) > 1:
                raise ValueError(
                    "paired-execution data integrity: "
                    f"{len(matches)} active legs share client_order_id {client_order_id}"
                )
            existing = matches[0] if matches else None
            if existing is None:
                return False
            if existing.status in self._PEL_TERMINAL_PROJECT_SKIP_STATUSES:
                return False
            effective_bus_time = max(bus_time, existing.timestamp)
            await s.execute(
                update(PairedExecutionLeg)
                .where(PairedExecutionLeg.id == existing.id)
                .values(known_to=effective_bus_time)
            )
            s.add(
                self._leg_successor(
                    existing,
                    status=new_status,
                    filled_signed_qty=existing.filled_signed_qty,
                    exchange_order_id=(
                        exchange_order_id
                        if exchange_order_id is not None
                        else existing.exchange_order_id
                    ),
                    last_venue_event_id=existing.last_venue_event_id,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=effective_bus_time,
                )
            )
            await s.commit()
            return True

    @staticmethod
    def _leg_successor(
        existing: PairedExecutionLeg,
        *,
        status: str,
        filled_signed_qty: float,
        exchange_order_id: str | None,
        last_venue_event_id: int | None,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        compensation_seq: int | None = None,
        compensated_signed_qty: float | None = None,
    ) -> PairedExecutionLeg:
        """Build an SCD2 successor leg row carrying every column forward.

        Shared by the fill, terminal, flatten-claim and compensation-fill
        projections: every immutable / carried column (identity, group, venue,
        side, target, signal/command ids) is copied from ``existing`` and only
        the explicitly-overridden ``status`` / ``filled_signed_qty`` / venue ids
        / provenance / ``timestamp`` differ. ``compensated_signed_qty`` is
        carried forward unless an override is given (the
        compensation-fill projection writes the recomputed value);
        ``compensation_seq`` is carried forward unless an override is given (the
        flatten claim bumps it). The caller closes ``existing``
        and adds this row.
        """
        return PairedExecutionLeg(
            public_id=existing.public_id,
            group_public_id=existing.group_public_id,
            leg_index=existing.leg_index,
            exchange=existing.exchange,
            mode=existing.mode,
            instrument=existing.instrument,
            shard_key=existing.shard_key,
            side=existing.side,
            target_qty=existing.target_qty,
            signal_public_id=existing.signal_public_id,
            command_public_id=existing.command_public_id,
            client_order_id=existing.client_order_id,
            exchange_order_id=exchange_order_id,
            status=status,
            filled_signed_qty=filled_signed_qty,
            compensated_signed_qty=(
                existing.compensated_signed_qty
                if compensated_signed_qty is None
                else compensated_signed_qty
            ),
            compensation_seq=(
                existing.compensation_seq if compensation_seq is None else compensation_seq
            ),
            last_venue_event_id=last_venue_event_id,
            wallet_public_id=existing.wallet_public_id,
            operator_public_id=existing.operator_public_id,
            created_at=existing.created_at,
            session_id=session_id,
            sequence_id=sequence_id,
            timestamp=timestamp,
        )

    @staticmethod
    def _paired_execution_leg_set_is_complete(
        legs: list[PairedExecutionLeg],
        group: PairedExecutionGroup,
    ) -> bool:
        """Return whether a group's active legs form its complete leg set.

        Complete means exactly ``expected_leg_count`` active legs with
        indices ``0 .. n-1``, each carrying a ``command_public_id``, whose
        canonical sorted ``{exchange}:{instrument}:{mode}`` key equals the
        group's ``group_key``. The index, command and key checks together
        reject a partial set, a duplicate-index set (e.g. two legs of the
        same instrument), and a wrong-instrument set, so a mismatched group
        can never arm and dispatch one leg naked.
        """
        if len(legs) != group.expected_leg_count:
            return False
        if sorted(leg.leg_index for leg in legs) != list(range(group.expected_leg_count)):
            return False
        if any(leg.command_public_id is None for leg in legs):
            return False
        reconstructed = compute_paired_group_key(
            [(leg.exchange, leg.instrument, leg.mode) for leg in legs]
        )
        return reconstructed == group.group_key

    async def ensure_paired_execution_group(self, row: PairedExecutionGroupInsertRow) -> bool:
        """Insert a paired-execution group unless an active one already exists.

        Idempotent across racing coordinators that each own a leg of the
        same group: the active-unique ``public_id`` index admits exactly one
        active group row. On an ``IntegrityError`` the method re-checks for an
        active group with the same ``public_id``; if one exists the error was
        the expected active-unique collision and ``False`` is returned, but
        ANY other integrity failure (a malformed row violating a different
        constraint) is RE-RAISED rather than silently swallowed. Swallowing a
        non-collision error would leave no group row, so the legs' commands
        would later pass the outbox gate as ungrouped and dispatch naked.
        Returns ``True`` iff this call created the group.
        """
        async with self.session() as s:
            s.add(PairedExecutionGroup(**row))
            try:
                await s.commit()
            except IntegrityError:
                await s.rollback()
                active = (
                    await s.execute(
                        select(PairedExecutionGroup.id).where(
                            PairedExecutionGroup.public_id == row.get("public_id"),
                            PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                        )
                    )
                ).first()
                if active is None:
                    raise
                return False
            return True

    async def try_arm_paired_execution_group_if_complete(
        self,
        group_public_id: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """Validate a group's full leg set and CAS ``assembling -> armed``.

        Locks the active group ``FOR UPDATE`` then arms it iff: status is
        ``assembling``; ``bus_time <= assembly_deadline``; and the active
        legs form the complete, command-bearing, key-matching leg set
        (:meth:`_paired_execution_leg_set_is_complete`). Any failed check
        returns ``False`` without arming, upholding the safety invariant
        that no grouped command dispatches until every sibling leg is
        durably registered with a command. The ``FOR UPDATE`` lock
        serialises concurrent arm attempts from sibling coordinators; a
        loser sees ``armed`` and returns ``False``. Returns ``True`` iff
        this call armed the group.
        """
        async with self.session() as s:
            group = (
                (
                    await s.execute(
                        select(PairedExecutionGroup)
                        .where(
                            PairedExecutionGroup.public_id == group_public_id,
                            *where_active(PairedExecutionGroup, bus_time),
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if group is None:
                return False
            if group.status != PairedExecutionGroupStatusEnum.ASSEMBLING:
                return False
            if bus_time > group.assembly_deadline:
                return False
            legs = (
                (
                    await s.execute(
                        select(PairedExecutionLeg).where(
                            PairedExecutionLeg.group_public_id == group_public_id,
                            *where_active(PairedExecutionLeg, bus_time),
                        )
                    )
                )
                .scalars()
                .all()
            )
            if not self._paired_execution_leg_set_is_complete(list(legs), group):
                return False
            await s.execute(
                update(PairedExecutionGroup)
                .where(PairedExecutionGroup.id == group.id)
                .values(known_to=bus_time)
            )
            s.add(
                PairedExecutionGroup(
                    public_id=group.public_id,
                    wallet_public_id=group.wallet_public_id,
                    operator_public_id=group.operator_public_id,
                    strategy_id=group.strategy_id,
                    policy=group.policy,
                    expected_leg_count=group.expected_leg_count,
                    group_key=group.group_key,
                    status=PairedExecutionGroupStatusEnum.ARMED,
                    assembly_deadline=group.assembly_deadline,
                    fill_deadline=group.fill_deadline,
                    failure_reason=group.failure_reason,
                    halted_at=group.halted_at,
                    created_at=group.created_at,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    timestamp=bus_time,
                )
            )
            await s.commit()
            return True

    async def get_paired_execution_group(
        self,
        public_id: str,
        as_of: datetime,
    ) -> PairedExecutionGroupRow | None:
        """Return the active paired-execution group by public_id."""
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionGroup).where(
                    PairedExecutionGroup.public_id == public_id,
                    *where_active(PairedExecutionGroup, as_of),
                )
            )
            group = result.scalars().first()
            if group is None:
                return None
            return self._paired_execution_group_row_to_dict(group)

    async def get_paired_execution_legs(
        self,
        group_public_id: str,
        as_of: datetime,
    ) -> list[PairedExecutionLegRow]:
        """Return active paired-execution legs for a group ordered by leg index."""
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionLeg)
                .where(
                    PairedExecutionLeg.group_public_id == group_public_id,
                    *where_active(PairedExecutionLeg, as_of),
                )
                .order_by(PairedExecutionLeg.leg_index)
            )
            return [self._paired_execution_leg_row_to_dict(leg) for leg in result.scalars().all()]

    async def get_current_paired_execution_legs(
        self,
        group_public_id: str,
    ) -> list[PairedExecutionLegRow]:
        """Return the CURRENT active legs for a group ordered by leg index.

        Guards on the current active row (``known_to == KNOWN_TO_MAX``), not a
        temporal ``as_of`` view, so startup recovery rebuilding the in-memory
        shard-halt mirror sees an owned leg even if a sibling coordinator stamped
        it with a slightly future ``timestamp`` under clock skew — a temporal
        ``get_paired_execution_legs(now)`` read would exclude such a leg and leave
        its shard un-halted until the scanner catches up. Mirrors the
        current-active guard :meth:`get_active_paired_execution_halt` uses.
        """
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionLeg)
                .where(
                    PairedExecutionLeg.group_public_id == group_public_id,
                    PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                )
                .order_by(PairedExecutionLeg.leg_index)
            )
            return [self._paired_execution_leg_row_to_dict(leg) for leg in result.scalars().all()]

    async def list_active_paired_execution_legs_for_shards(
        self,
        shard_keys: list[str],
        as_of: datetime,
    ) -> list[PairedExecutionLegRow]:
        """Return active paired-execution legs whose shard key is in the input set.

        Intended for the guard scanner: callers pass the bounded set of
        shard keys they own. The result is unbounded only in the number of
        active legs on those shards, which the arming barrier and
        compensation keep small.
        """
        if not shard_keys:
            return []
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionLeg)
                .where(
                    PairedExecutionLeg.shard_key.in_(shard_keys),
                    *where_active(PairedExecutionLeg, as_of),
                )
                .order_by(PairedExecutionLeg.group_public_id, PairedExecutionLeg.leg_index)
            )
            return [self._paired_execution_leg_row_to_dict(leg) for leg in result.scalars().all()]

    async def list_active_paired_execution_groups(
        self,
        statuses: list[str],
        as_of: datetime,
    ) -> list[PairedExecutionGroupRow]:
        """Return active paired-execution groups matching the status set.

        Intended for the guard scanner with NON-terminal statuses
        (``assembling`` / ``armed`` / ``broken`` / ``compensating``), which
        bounds the result to in-flight groups. Terminal groups stay
        SCD2-active until the completion path closes them, so listing by a
        terminal status is unbounded over a long deployment. Ordered by
        ``created_at`` then ``id`` for a deterministic page.
        """
        if not statuses:
            return []
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionGroup)
                .where(
                    PairedExecutionGroup.status.in_(statuses),
                    *where_active(PairedExecutionGroup, as_of),
                )
                .order_by(PairedExecutionGroup.created_at, PairedExecutionGroup.id)
            )
            return [
                self._paired_execution_group_row_to_dict(group) for group in result.scalars().all()
            ]

    async def list_current_paired_execution_groups(
        self, statuses: list[str]
    ) -> list[PairedExecutionGroupRow]:
        """Return CURRENT active groups matching the status set.

        Guards on the current active row (``known_to == KNOWN_TO_MAX``), not a
        temporal ``as_of`` view, so startup recovery does not skip a current
        ``armed`` / ``broken`` / ``compensating`` group stamped with a slightly
        future ``timestamp`` under clock skew — pairing with
        :meth:`get_current_paired_execution_legs` so the group and its legs use
        the same current-active read. Ordered by ``created_at`` then ``id`` for a
        deterministic page.
        """
        if not statuses:
            return []
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionGroup)
                .where(
                    PairedExecutionGroup.status.in_(statuses),
                    PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                )
                .order_by(PairedExecutionGroup.created_at, PairedExecutionGroup.id)
            )
            return [
                self._paired_execution_group_row_to_dict(group) for group in result.scalars().all()
            ]

    async def get_active_paired_execution_halt(
        self,
        wallet_public_id: str,
        strategy_id: str,
        group_key: str,
    ) -> PairedExecutionHaltRow | None:
        """Return the CURRENT active halt for a wallet-strategy-group scope.

        Guards on the current active row (``known_to == KNOWN_TO_MAX``), not a
        temporal ``as_of`` view: the live ``_on_signal`` fast-reject must see a
        halt the instant it is projected, even if a sibling coordinator stamped
        it with a slightly future ``timestamp`` under clock skew — a temporal
        ``where_active(now)`` read would exclude such a halt and fail OPEN.
        Mirrors the current-active guard the trade-command CAS uses. Returns
        ``None`` when no active halt covers the scope.
        """
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionHalt).where(
                    PairedExecutionHalt.wallet_public_id == wallet_public_id,
                    PairedExecutionHalt.strategy_id == strategy_id,
                    PairedExecutionHalt.group_key == group_key,
                    PairedExecutionHalt.known_to == KNOWN_TO_MAX,
                )
            )
            halt = result.scalars().first()
            if halt is None:
                return None
            return self._paired_execution_halt_row_to_dict(halt)

    async def list_active_paired_execution_halts(self) -> list[PairedExecutionHaltRow]:
        """Return every CURRENT active paired-execution halt.

        Lists current-active rows (``known_to == KNOWN_TO_MAX``), matching
        :meth:`get_active_paired_execution_halt`, so startup recovery can rebuild
        the in-memory shard-halt mirror from the authoritative durable halts: a
        halt closed by ``clear_paired_execution_halt`` is excluded, so a
        deliberately-cleared pair is never re-halted on restart. Bounded by the
        number of pairs currently in failure / compensation. Ordered by
        ``created_at`` then ``id`` for a deterministic page.
        """
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionHalt)
                .where(PairedExecutionHalt.known_to == KNOWN_TO_MAX)
                .order_by(PairedExecutionHalt.created_at, PairedExecutionHalt.id)
            )
            return [
                self._paired_execution_halt_row_to_dict(halt) for halt in result.scalars().all()
            ]

    async def clear_paired_execution_halt(self, public_id: str, bus_time: datetime) -> bool:
        """Close an active paired-execution halt without inserting a successor row."""
        async with self.session() as s:
            existing = (
                (
                    await s.execute(
                        select(PairedExecutionHalt)
                        .where(
                            PairedExecutionHalt.public_id == public_id,
                            *where_active(PairedExecutionHalt, bus_time),
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
                update(PairedExecutionHalt)
                .where(PairedExecutionHalt.id == existing.id)
                .values(known_to=bus_time)
            )
            await s.commit()
            return True

    async def clear_paired_execution_halt_if_scope_quiet(
        self,
        wallet_public_id: str,
        strategy_id: str,
        group_key: str,
        bus_time: datetime,
    ) -> bool:
        """Close a scope's active halt iff NO group in the scope is still exposed.

        The quiet-halt sweep's clear: in ONE transaction, checks
        for any CURRENT active group on the ``(wallet, strategy, group_key)``
        scope whose status is still exposed (``broken`` / ``compensating`` /
        ``manual_intervention``) and, only when none exists, closes the scope's
        CURRENT active halt row. A completed group no longer matches the
        exposed set, so the halt clears on the first sweep after the scope's
        LAST group completes; a sibling group still failing keeps the halt in
        force (it covers that group too — the scope is active-unique). Both
        reads guard on ``known_to == KNOWN_TO_MAX`` (not a temporal view) so a
        future-stamped row under clock skew cannot make the scope look quiet,
        and the close time is clamped to ``max(bus_time, halt.timestamp)`` so
        the SCD2 interval never inverts. Running it from a periodic sweep
        (rather than once at completion) makes the clear idempotent and
        crash-retryable: a coordinator dying between completing the group and
        clearing the halt just leaves the clear for the next cycle. Returns
        ``True`` iff this call closed the halt.

        The scope's group rows are read ``FOR UPDATE NOWAIT`` (ordered by id so
        two sweeping coordinators cannot deadlock each other): the quiet
        decision must be made against rows a concurrent late-fill REOPEN
        (which flips a completed group back to ``compensating`` under the same
        group lock) cannot change mid-transaction. Plain ``FOR UPDATE`` would
        be unsound under the SCD2 close-and-insert pattern — a clear WAITING on
        the reopen's lock would, after the wait, re-check and SKIP the closed
        predecessor row while the reopen's freshly INSERTED ``compensating``
        successor stays invisible to its snapshot, so the scope would look
        quiet exactly when it is not. ``NOWAIT`` makes the race fail-safe
        instead: ANY lock conflict (or other database error) aborts this clear
        with ``False`` — the halt stays and the next cycle retries. The lock
        order (groups, then the halt row; never legs) cannot cycle with the
        completion DAL (group → legs) or the reopen path (group → leg). A
        reopen that lands strictly AFTER this clear commits remains exposed
        without a halt for at most one scan cycle — the halts sweep re-halts a
        ``compensating`` group on the next pass; that residual ordering is
        inherent to clearing at all and self-heals.
        """
        async with self.session() as s:
            try:
                scope_groups = (
                    (
                        await s.execute(
                            select(PairedExecutionGroup)
                            .where(
                                PairedExecutionGroup.wallet_public_id == wallet_public_id,
                                PairedExecutionGroup.strategy_id == strategy_id,
                                PairedExecutionGroup.group_key == group_key,
                                PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                            )
                            .order_by(PairedExecutionGroup.id)
                            .with_for_update(nowait=True)
                        )
                    )
                    .scalars()
                    .all()
                )
            except DBAPIError:
                return False
            if any(group.status in self._PEG_EXPOSED_STATUSES for group in scope_groups):
                return False
            halt = (
                (
                    await s.execute(
                        select(PairedExecutionHalt)
                        .where(
                            PairedExecutionHalt.wallet_public_id == wallet_public_id,
                            PairedExecutionHalt.strategy_id == strategy_id,
                            PairedExecutionHalt.group_key == group_key,
                            PairedExecutionHalt.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if halt is None:
                return False
            await s.execute(
                update(PairedExecutionHalt)
                .where(PairedExecutionHalt.id == halt.id)
                .values(known_to=max(bus_time, halt.timestamp))
            )
            await s.commit()
            return True

    def _paired_execution_leg_is_settled_flat(self, leg: PairedExecutionLeg) -> bool:
        """Return whether a leg is settled with zero open exposure.

        Settled-flat means the leg carries a terminal status (a member of
        :data:`_PEL_SETTLED_STATUSES` — ``flattened`` / ``cancelled`` /
        ``expired`` / ``rejected`` / ``filled``) AND its open exposure
        ``|filled_signed_qty − compensated_signed_qty|`` is below
        :data:`_PEC_QTY_EPSILON`. ``filled``-with-zero-open is reachable (a
        reopened leg whose old flatten's late fill report zeroes the residual)
        and counts. A ``pending`` / ``working`` / ``compensating`` /
        ``manual_intervention`` leg is never settled-flat. Shared by the
        automatic completion predicate and the operator attestation predicate.
        """
        if leg.status not in self._PEL_SETTLED_STATUSES:
            return False
        open_qty = leg.filled_signed_qty - leg.compensated_signed_qty
        return abs(open_qty) < self._PEC_QTY_EPSILON

    def _paired_execution_group_is_settled_for_completion(
        self,
        group: PairedExecutionGroup,
        legs: Sequence[PairedExecutionLeg],
        expected_status: str,
    ) -> bool:
        """Return whether a group's locked legs satisfy the status-specific settled predicate.

        The per-status completion gate of
        :meth:`complete_paired_execution_group_if_settled`, evaluated on legs
        already locked ``FOR UPDATE``: for ``armed`` the legs must form the
        COMPLETE validated leg set (:meth:`_paired_execution_leg_set_is_complete`
        — never a vacuous or partial set) and every leg must be fully
        ``filled``; for ``broken`` / ``compensating`` every leg must be
        settled-flat (:meth:`_paired_execution_leg_is_settled_flat`). Callers
        guarantee ``legs`` is non-empty, so the all-legs checks are never
        vacuously true.
        """
        if expected_status == PairedExecutionGroupStatusEnum.ARMED.value:
            if not self._paired_execution_leg_set_is_complete(list(legs), group):
                return False
            return not any(leg.status != PairedExecutionLegStatusEnum.FILLED.value for leg in legs)
        return all(self._paired_execution_leg_is_settled_flat(leg) for leg in legs)

    @staticmethod
    async def _paired_execution_group_has_held_original(
        s: AsyncSession, group_public_id: str
    ) -> bool:
        """Return whether a current-active HELD ``created`` ORIGINAL command carries the group.

        Probes, inside the caller's transaction, for a CURRENT active
        ``created`` trade command (``supersedes_command_id IS NULL``) whose
        ``correlation_id`` is the group's public id. Completing or attesting a
        group over such a command would leave it held by the outbox gate
        forever (the gate releases grouped commands only for ARMED groups),
        polluting every outbox poll page — and a held original means the group
        was not truly settled. COMPENSATION commands (cancel / flatten, which
        supersede an original) are deliberately excluded: the outbox dispatches
        them regardless of group status.
        """
        held = (
            await s.execute(
                select(TradeCommand.id)
                .where(
                    TradeCommand.correlation_id == group_public_id,
                    TradeCommand.status == TradeCommandStatusEnum.CREATED.value,
                    TradeCommand.supersedes_command_id.is_(None),
                    TradeCommand.known_to == KNOWN_TO_MAX,
                )
                .limit(1)
            )
        ).first()
        return held is not None

    @staticmethod
    async def _close_paired_execution_group_as_completed(
        s: AsyncSession,
        group: PairedExecutionGroup,
        failure_reason: str | None,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2-close a locked group and insert its ``completed`` successor row.

        The shared row-transition tail of
        :meth:`complete_paired_execution_group_if_settled` and
        :meth:`terminalize_paired_execution_group`, on a group already locked
        ``FOR UPDATE`` inside the caller's transaction (the caller commits).
        The close / successor time is clamped to ``max(bus_time,
        group.timestamp)`` so the successor never precedes the row it closes.
        Every group attribute is carried forward unchanged except ``status``
        (forced to ``completed``) and ``failure_reason`` (the caller-supplied
        value: the prior reason for automatic completion, the attestation-
        stamped reason for operator terminalization).
        """
        effective_bus_time = max(bus_time, group.timestamp)
        await s.execute(
            update(PairedExecutionGroup)
            .where(PairedExecutionGroup.id == group.id)
            .values(known_to=effective_bus_time)
        )
        s.add(
            PairedExecutionGroup(
                public_id=group.public_id,
                wallet_public_id=group.wallet_public_id,
                operator_public_id=group.operator_public_id,
                strategy_id=group.strategy_id,
                policy=group.policy,
                expected_leg_count=group.expected_leg_count,
                group_key=group.group_key,
                status=PairedExecutionGroupStatusEnum.COMPLETED.value,
                assembly_deadline=group.assembly_deadline,
                fill_deadline=group.fill_deadline,
                failure_reason=failure_reason,
                halted_at=group.halted_at,
                created_at=group.created_at,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=effective_bus_time,
            )
        )

    async def complete_paired_execution_group_if_settled(
        self,
        group_public_id: str,
        expected_status: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """CAS a fully-settled group to ``completed`` under a group-then-legs lock.

        The completion check, in ONE transaction: locks the CURRENT
        active group ``FOR UPDATE`` (group THEN legs — the lock order every
        group-touching paired DAL shares), verifies ``status ==
        expected_status``, locks the CURRENT active legs, and verifies the
        status-specific settled predicate:

        - ``armed`` (the happy path): the legs form the COMPLETE validated leg
          set (:meth:`_paired_execution_leg_set_is_complete` — never a vacuous
          or partial set) and every leg is fully ``filled``. The exposure is
          INTENDED (the strategy holds the pair), so completion involves no
          halt or flatten logic.
        - ``broken`` / ``compensating``: at least one leg exists and EVERY leg
          is settled — a terminal status (``flattened`` / ``cancelled`` /
          ``expired`` / ``rejected`` / ``filled``) with zero open exposure
          (``|filled_signed_qty − compensated_signed_qty| <`` epsilon).
          ``filled``-with-zero-open is reachable (a reopened leg whose old
          flatten's late fill report zeroes the residual) and counts. A
          ``pending`` / ``working`` / ``compensating`` / ``manual_intervention``
          leg blocks completion; ``manual_intervention`` GROUPS are never
          completed automatically (`expected_status` rejects them).

        Additionally requires NO current-active HELD ``created`` ORIGINAL trade
        command (``supersedes_command_id IS NULL``) carrying this group as its
        ``correlation_id``: completing the group would leave such a command held
        by the outbox gate forever (the gate releases grouped commands only for
        ARMED groups), polluting every outbox poll page — and a held original
        means the group was not truly settled. COMPENSATION commands (cancel /
        flatten, which supersede an original) are deliberately excluded: the
        outbox dispatches them regardless of group status, so one that is
        momentarily ``created`` must only delay completion by its dispatch, not
        block it on a stale row. The group close / successor time is clamped to ``max(bus_time,
        group.timestamp)``. Completing settled BROKEN groups (e.g. a
        zero-exposure assembly-timeout break whose legs were all cancelled)
        deliberately terminalizes them so they stop bloating every scanner
        listing and the pair scope quiets. Returns ``True`` iff this call
        completed the group. Raises ``ValueError`` for an ``expected_status``
        outside the completable set (armed / broken / compensating).
        """
        if expected_status not in self._PEG_COMPLETABLE_STATUSES:
            raise ValueError(
                f"paired-execution group completion from {expected_status!r} is not allowed"
            )
        async with self.session() as s:
            group = (
                (
                    await s.execute(
                        select(PairedExecutionGroup)
                        .where(
                            PairedExecutionGroup.public_id == group_public_id,
                            PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if group is None or group.status != expected_status:
                return False
            legs = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.group_public_id == group_public_id,
                            PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if not legs:
                return False
            if not self._paired_execution_group_is_settled_for_completion(
                group, legs, expected_status
            ):
                return False
            if await self._paired_execution_group_has_held_original(s, group_public_id):
                return False
            await self._close_paired_execution_group_as_completed(
                s, group, group.failure_reason, bus_time, session_id, sequence_id
            )
            await s.commit()
            return True

    async def list_recent_completed_paired_execution_groups(
        self, completed_after: datetime
    ) -> list[PairedExecutionGroupRow]:
        """Return CURRENT active ``completed`` groups whose completion is recent.

        Startup recovery's bounded window over completed groups: a
        late original fill that landed while the coordinator was DOWN must
        reopen its completed group, but the live reopen trigger never fires for
        events already persisted and listing ALL completed groups is unbounded
        over a deployment's lifetime. The completion successor's ``timestamp``
        IS the completion time, so filtering ``timestamp > completed_after``
        (backed by the ``ix_peg_status_timestamp`` index) bounds the replay to
        groups completed within the recovery window; an older late fill is
        reconciliation / operator territory. Ordered by ``created_at`` then
        ``id`` for a deterministic page.
        """
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionGroup)
                .where(
                    PairedExecutionGroup.status == PairedExecutionGroupStatusEnum.COMPLETED.value,
                    PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                    PairedExecutionGroup.timestamp > completed_after,
                )
                .order_by(PairedExecutionGroup.created_at, PairedExecutionGroup.id)
            )
            return [
                self._paired_execution_group_row_to_dict(group) for group in result.scalars().all()
            ]

    async def get_current_paired_execution_group(
        self, public_id: str
    ) -> PairedExecutionGroupRow | None:
        """Return the CURRENT active group by public_id, or None.

        Guards on the current active row (``known_to == KNOWN_TO_MAX``), not a
        temporal ``as_of`` view, so the operator surface never 404s a group a
        sibling coordinator stamped with a slightly future ``timestamp`` under
        clock skew — pairing with :meth:`get_current_paired_execution_legs`.
        """
        async with self.session() as s:
            result = await s.execute(
                select(PairedExecutionGroup).where(
                    PairedExecutionGroup.public_id == public_id,
                    PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                )
            )
            group = result.scalars().first()
            if group is None:
                return None
            return self._paired_execution_group_row_to_dict(group)

    def _paired_execution_group_is_attestable(
        self,
        group: PairedExecutionGroup,
        legs: Sequence[PairedExecutionLeg],
    ) -> bool:
        """Return whether a locked group may be operator-attested as resolved.

        The attestable predicate of :meth:`terminalize_paired_execution_group`,
        evaluated on rows already locked ``FOR UPDATE``: the group must be
        ``manual_intervention`` (the operator owns it outright), OR
        ``compensating`` with at least one current ``manual_intervention`` leg
        — the re-attestation path after a late original fill reopened an
        already-terminalized group whose manual leg cannot re-enter
        automation. In BOTH cases every leg must be either settled-flat
        (:meth:`_paired_execution_leg_is_settled_flat`) or
        ``manual_intervention``: an operator must never attest over a sibling
        leg's in-flight automation. A group with NO current legs is never
        attestable: a vacuous attestation would clear the scope's halt while
        erasing the operator-visible incident.
        """
        if not legs:
            return False
        manual_legs = [
            leg
            for leg in legs
            if leg.status == PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value
        ]
        group_is_manual = group.status == PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value
        group_is_reopened_manual = (
            group.status == PairedExecutionGroupStatusEnum.COMPENSATING.value and bool(manual_legs)
        )
        if not (group_is_manual or group_is_reopened_manual):
            return False
        for leg in legs:
            if leg.status == PairedExecutionLegStatusEnum.MANUAL_INTERVENTION.value:
                continue
            if not self._paired_execution_leg_is_settled_flat(leg):
                return False
        return True

    async def terminalize_paired_execution_group(
        self,
        public_id: str,
        attested_by: str,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
    ) -> PairedGroupTerminalizeOutcome:
        """Record an operator attestation that a manual group is resolved at the venue.

        The OPERATOR counterpart of the automatic
        :meth:`complete_paired_execution_group_if_settled` (which deliberately
        refuses ``manual_intervention``): in ONE transaction it locks the
        CURRENT active group ``FOR UPDATE`` (group THEN legs — the shared lock
        order), validates the attestable predicate, and SCD2-closes the group
        into ``completed`` with the attestation stamped into ``failure_reason``
        as a bounded, parseable ``terminalized_by=<user_public_id>`` suffix
        (truncating the prior reason to fit the 512-char column). The next
        scanner cycle's quiet-halt sweep then clears the scope's durable halt
        and every coordinator's in-memory mirror — no cross-process call.

        Attestable means: the group is ``manual_intervention`` (the operator
        owns it outright), OR it is ``compensating`` with at least one current
        ``manual_intervention`` leg — the re-attestation path after a late
        original fill reopened an already-terminalized group whose manual leg
        cannot re-enter automation. In BOTH cases every leg must be either
        settled (a terminal status at zero open exposure) or
        ``manual_intervention``: an operator must never attest over a sibling
        leg's in-flight automation (a live original or an unfinished flatten),
        so such a group returns ``NOT_TERMINALIZABLE`` until the live hooks
        settle the sibling. Legs are NEVER mutated — their statuses and signed
        accounting remain the true books; the completed group is purely the
        attestation record. A group with NO current legs is never attestable:
        a vacuous attestation would clear the scope's halt while erasing the
        operator-visible incident, so corruption stays surfaced instead. The
        held-created-ORIGINAL guard mirrors
        :meth:`complete_paired_execution_group_if_settled` (an attested group
        must never strand a gate-held command in the outbox backlog), and the
        attesting subject is clamped to 64 chars so the stamp suffix can never
        consume the whole 512-char column.
        """
        async with self.session() as s:
            group = (
                (
                    await s.execute(
                        select(PairedExecutionGroup)
                        .where(
                            PairedExecutionGroup.public_id == public_id,
                            PairedExecutionGroup.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if group is None:
                return PairedGroupTerminalizeOutcome.NOT_FOUND
            legs = (
                (
                    await s.execute(
                        select(PairedExecutionLeg)
                        .where(
                            PairedExecutionLeg.group_public_id == public_id,
                            PairedExecutionLeg.known_to == KNOWN_TO_MAX,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .all()
            )
            if not self._paired_execution_group_is_attestable(group, legs):
                return PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE
            if await self._paired_execution_group_has_held_original(s, public_id):
                return PairedGroupTerminalizeOutcome.NOT_TERMINALIZABLE
            suffix = f"; terminalized_by={attested_by[:64]}"
            base = group.failure_reason or "manual intervention"
            stamped = base[: max(0, 512 - len(suffix))] + suffix
            await self._close_paired_execution_group_as_completed(
                s, group, stamped, bus_time, session_id, sequence_id
            )
            await s.commit()
            return PairedGroupTerminalizeOutcome.TERMINALIZED

    @staticmethod
    def _venue_event_to_row(ve: VenueEvent) -> VenueEventRow:
        """Project a VenueEvent ORM row into the VenueEventRow TypedDict shape."""
        return {
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
            "paired_group_id": ve.paired_group_id,
        }

    async def get_venue_events_after(self, shard_key: str, after_id: int) -> list[VenueEventRow]:
        """Return venue events for a shard after the given watermark (id)."""
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent)
                .where(VenueEvent.shard_key == shard_key, VenueEvent.id > after_id)
                .order_by(VenueEvent.id)
            )
            return [self._venue_event_to_row(ve) for ve in result.scalars().all()]

    async def get_max_cumulative_fill_venue_event(
        self, client_order_id: str
    ) -> VenueEventRow | None:
        """Return the ``fill_observed`` venue event with the MAX cumulative fill for an order.

        Selects the row with the greatest non-null ``cum_fill_size`` (tie-broken by
        highest ``id``) among the append-only ``fill_observed`` events for
        ``client_order_id``. Recovery fill parity reads the AUTHORITATIVE
        ``venue_events`` — the executor writes them fail-closed with the cumulative
        ``cum_fill_size`` BEFORE publishing — so a leg whose order filled while the
        coordinator was down is re-projected on restart. Selecting by MAX cumulative
        (NOT latest ``id``) ensures an out-of-order executor write can never make
        recovery read a smaller cumulative than was actually filled. Returns ``None``
        when no cumulative fill event exists for the order.
        """
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent)
                .where(
                    VenueEvent.client_order_id == client_order_id,
                    VenueEvent.event_type == "fill_observed",
                    VenueEvent.cum_fill_size.isnot(None),
                )
                .order_by(VenueEvent.cum_fill_size.desc(), VenueEvent.id.desc())
                .limit(1)
            )
            ve = result.scalars().first()
            if ve is None:
                return None
            return self._venue_event_to_row(ve)

    async def get_fill_venue_events_for_order(self, client_order_id: str) -> list[VenueEventRow]:
        """Return all cumulative ``fill_observed`` venue events for an order.

        Rows with a non-null ``cum_fill_size``, ordered by
        ``(cum_fill_size asc, id asc)`` so callers can walk the order's
        durable fill history monotonically regardless of out-of-order
        executor writes. Recovery reads this to seed the dual watermarks
        honestly and to republish recorded-but-unpublished fills under
        their ORIGINAL exec ids (every downstream consumer dedupes by
        exec id, so republishing is idempotent). Served by
        ``ix_venue_events_cid_event_type``; no ``known_to`` filter is
        needed because venue events are never closed.

        Args:
            client_order_id: Client order id whose fill history to read.

        Returns:
            Ordered fill rows; empty list when none exist.
        """
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent)
                .where(
                    VenueEvent.client_order_id == client_order_id,
                    VenueEvent.event_type == "fill_observed",
                    VenueEvent.cum_fill_size.isnot(None),
                )
                .order_by(VenueEvent.cum_fill_size.asc(), VenueEvent.id.asc())
            )
            return [self._venue_event_to_row(ve) for ve in result.scalars().all()]

    async def has_order_submit_evidence(self, client_order_id: str) -> bool:
        """Return True when durable evidence shows the submit may have reached the venue.

        Probes the append-only ``venue_events`` for any
        ``_ORDER_SUBMIT_EVIDENCE_EVENT_TYPES`` row of the client order
        id — the duplicate-submit guard's durable check,
        covering crash-replay where a fresh executor process has no
        in-memory pending state. Served by
        ``ix_venue_events_cid_event_type``; no ``known_to`` filter is
        needed because venue events are never closed.

        Residual gap, documented deliberately: a crash after the venue
        accepted the order but before the ``order_accepted`` row
        committed (and before recon healed it) leaves no durable trace
        — only a venue lookup by client id could close that window.

        Args:
            client_order_id: Client order id of the replayed command.

        Returns:
            True when at least one evidence event exists.
        """
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent.id)
                .where(
                    VenueEvent.client_order_id == client_order_id,
                    VenueEvent.event_type.in_(_ORDER_SUBMIT_EVIDENCE_EVENT_TYPES),
                )
                .limit(1)
            )
            return result.first() is not None

    async def has_venue_event(self, client_order_id: str, event_type: str) -> bool:
        """Return True when a venue event of the given type exists for the order.

        Targeted single-type probe (vs the multi-type
        :meth:`has_order_submit_evidence`) used by write-side dedupe: the
        executor's ``order_accepted`` heal retry checks whether the
        supposedly-failed insert actually committed (timeout-after-commit
        race) before inserting again, keeping the append-only plane free of
        avoidable duplicates. Served by ``ix_venue_events_cid_event_type``;
        no ``known_to`` filter is needed because venue events are never
        closed.

        Args:
            client_order_id: Client order id to probe.
            event_type: Exact venue event type to look for.

        Returns:
            True when at least one matching event exists.
        """
        async with self.session() as s:
            result = await s.execute(
                select(VenueEvent.id)
                .where(
                    VenueEvent.client_order_id == client_order_id,
                    VenueEvent.event_type == event_type,
                )
                .limit(1)
            )
            return result.first() is not None

    async def get_order_lifecycle_events(self, client_order_ids: list[str]) -> list[VenueEventRow]:
        """Return lifecycle venue events for a batch of client order ids.

        Feeds the coordinator's trade-command lifecycle fold: all
        ``_ORDER_LIFECYCLE_EVENT_TYPES`` rows for the given ids, globally
        ordered by ``id`` (the append-only plane's only total order) so the
        fold replays venue truth in observation order. The IN list is
        chunked to keep statement size bounded; active command sets are
        small, so a single chunk is the common case. Served by
        ``ix_venue_events_cid_event_type``.

        Args:
            client_order_ids: Client order ids of the active command set.

        Returns:
            Ordered lifecycle rows; empty list for an empty id set.
        """
        if not client_order_ids:
            return []
        rows: list[VenueEventRow] = []
        async with self.session() as s:
            for start in range(0, len(client_order_ids), _ORDER_LIFECYCLE_LOOKUP_CHUNK_SIZE):
                chunk = client_order_ids[start : start + _ORDER_LIFECYCLE_LOOKUP_CHUNK_SIZE]
                result = await s.execute(
                    select(VenueEvent).where(
                        VenueEvent.client_order_id.in_(chunk),
                        VenueEvent.event_type.in_(_ORDER_LIFECYCLE_EVENT_TYPES),
                    )
                )
                rows.extend(self._venue_event_to_row(ve) for ve in result.scalars().all())
        rows.sort(key=lambda r: r["id"])
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

    async def get_related_instruments_for_symbol(
        self,
        exchange: str,
        native_symbol: str,
        as_of: datetime,
    ) -> tuple[UnderlyingAssetRow | None, list[InstrumentRelatedRow]]:
        """Resolve the underlying + every sibling instrument for a UI-selected symbol.

        Returns a ``(underlying, related_rows)`` tuple powering the
        ``GET /api/instruments/{exchange}/{native_symbol}/related``
        endpoint and the MarketData "related instruments" UI row.

        Resolution steps (all SCD2-active at ``as_of``):

        1. ``Symbol`` lookup by ``(native_symbol, exchange)``.
        2. ``Instrument`` lookup by ``Symbol.public_id``.
        3. ``InstrumentUnderlyingMapping`` for the resolved instrument
           returns the parent underlying.
        4. ``get_instruments_by_underlying`` returns every sibling
           (including the input row itself, with ``is_selected=True``).

        Returns ``(None, [])`` when any step misses — symbol unknown,
        instrument not provisioned, or no underlying mapping (orphan).
        The route handler distinguishes "unknown symbol" (404) from
        "orphan" (200 + empty groups) using a second lookup.
        """
        async with self.session() as s:
            symbol_row = (
                (
                    await s.execute(
                        select(Symbol.public_id)
                        .join(
                            SymbolAlias,
                            and_(
                                SymbolAlias.symbol_public_id == Symbol.public_id,
                                SymbolAlias.exchange == exchange,
                                *where_active(SymbolAlias, as_of),
                            ),
                        )
                        .where(
                            Symbol.native_symbol == native_symbol,
                            *where_active(Symbol, as_of),
                        )
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            if symbol_row is None:
                return None, []
            inst_row = (
                (
                    await s.execute(
                        select(Instrument.public_id)
                        .where(
                            Instrument.symbol_public_id == symbol_row,
                            Instrument.exchange == exchange,
                            *where_active(Instrument, as_of),
                        )
                        .limit(1)
                    )
                )
                .scalars()
                .first()
            )
            if inst_row is None:
                return None, []
        underlying = await self.get_underlying_for_instrument(inst_row, as_of)
        if underlying is None:
            return None, []
        siblings = await self.get_instruments_by_underlying(underlying["public_id"], as_of)
        related: list[InstrumentRelatedRow] = []
        for sib in siblings:
            related.append(
                InstrumentRelatedRow(
                    instrument_public_id=sib["instrument_public_id"],
                    native_symbol=sib["native_symbol"],
                    exchange=sib["exchange"],
                    relationship_type=sib["relationship_type"],
                    contract_family=sib["contract_family"],
                    asset_type=sib["asset_type"],
                    is_selected=(
                        sib["instrument_public_id"] == inst_row and sib["exchange"] == exchange
                    ),
                )
            )
        return underlying, related

    async def upsert_underlying_asset(
        self,
        ticker: str,
        name: dict[str, str] | str,
        asset_class: str,
        session_id: str,
        sequence_id: int,
        timestamp: datetime,
        sector: str | None = None,
        description: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        """SCD2 upsert for an underlying asset."""
        description_json = cast(dict[str, JsonValue] | None, description)
        name_dict: dict[str, str] = {"en": name} if isinstance(name, str) else name
        name_json = cast(dict[str, JsonValue], name_dict)
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
                    existing.name != name_json
                    or existing.asset_class != asset_class
                    or existing.sector != sector
                    or existing.description != description_json
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
                    name=name_json,
                    asset_class=asset_class,
                    sector=sector,
                    description=description_json,
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
                name=name_json,
                asset_class=asset_class,
                sector=sector,
                description=description_json,
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

    @staticmethod
    def _scope_expansion_key(
        scope_kind: str,
        underlying_public_id: str | None,
        instrument_public_id: str | None,
    ) -> ScopeExpansionKey:
        """Build the immutable key used by batched scope expansion.

        Args:
            scope_kind: Scope discriminator from a grant or request.
            underlying_public_id: Underlying public ID for underlying scopes.
            instrument_public_id: Instrument public ID for instrument scopes.

        Returns:
            Tuple identity for a scope reference.
        """
        return (scope_kind, underlying_public_id, instrument_public_id)

    async def _expand_scope_refs_to_instruments(
        self,
        s: AsyncSession,
        refs: set[ScopeExpansionKey],
        as_of: datetime,
    ) -> dict[ScopeExpansionKey, set[str]]:
        """Resolve scope references to concrete instrument sets in one batch.

        Instrument scopes resolve locally to singleton sets. Underlying
        scopes are expanded with a single ``instrument_underlying_mappings``
        query for all requested underlying IDs active at ``as_of``.

        Args:
            s: Active SQLAlchemy async session.
            refs: Scope reference keys to expand.
            as_of: Wall-clock timestamp for SCD2-active mappings.

        Returns:
            Mapping from each input scope reference to its covered
            instrument public IDs.
        """
        expanded: dict[ScopeExpansionKey, set[str]] = {ref: set() for ref in refs}
        underlying_refs: dict[str, list[ScopeExpansionKey]] = {}
        for ref in refs:
            scope_kind, underlying_public_id, instrument_public_id = ref
            if scope_kind == "instrument":
                expanded[ref].add(cast(str, instrument_public_id))
            else:
                underlying_refs.setdefault(cast(str, underlying_public_id), []).append(ref)
        if not underlying_refs:
            return expanded
        result = await s.execute(
            select(
                InstrumentUnderlyingMapping.underlying_public_id,
                InstrumentUnderlyingMapping.instrument_public_id,
            ).where(
                InstrumentUnderlyingMapping.underlying_public_id.in_(tuple(underlying_refs)),
                *where_active(InstrumentUnderlyingMapping, as_of),
            )
        )
        for underlying_public_id, instrument_public_id in result.all():
            for ref in underlying_refs[underlying_public_id]:
                expanded[ref].add(instrument_public_id)
        return expanded

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
           instrument sets via ``_expand_scope_refs_to_instruments`` (which
           honors the dynamic-scope rule by querying
           ``instrument_underlying_mappings`` ACTIVE at ``as_of``) and report
           the first grant whose expansion intersects the request's.
        """
        req_kind = request["scope_kind"]
        req_underlying = request.get("underlying_public_id")
        req_instrument = request.get("instrument_public_id")
        request_key = self._scope_expansion_key(req_kind, req_underlying, req_instrument)
        for grant in existing:
            if (
                grant.scope_kind == req_kind
                and grant.underlying_public_id == req_underlying
                and grant.instrument_public_id == req_instrument
            ):
                return grant

        refs = {
            request_key,
            *(
                self._scope_expansion_key(
                    grant.scope_kind,
                    grant.underlying_public_id,
                    grant.instrument_public_id,
                )
                for grant in existing
            ),
        }
        expanded = await self._expand_scope_refs_to_instruments(s, refs, as_of)
        target = expanded[request_key]
        for grant in existing:
            grant_key = self._scope_expansion_key(
                grant.scope_kind,
                grant.underlying_public_id,
                grant.instrument_public_id,
            )
            if target & expanded[grant_key]:
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
            cancel_idempotency_key=p.cancel_idempotency_key,
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
                cancel_idempotency_key=row.get("cancel_idempotency_key"),
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
        cancel_idempotency_key: str | None = None,
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
                cancel_idempotency_key=(
                    cancel_idempotency_key
                    if cancel_idempotency_key is not None
                    else existing.cancel_idempotency_key
                ),
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_plan)
            await s.commit()
            await s.refresh(new_plan)
            return new_plan.id

    _ACTIONABLE_STATUSES = ("pending", "armed", "active", "paused", "cancel_requested")
    _CANCEL_TERMINAL_STATUSES: frozenset[str] = frozenset(
        {"completed", "cancelled", "failed", "expired"}
    )

    async def claim_execution_plan_cancel(
        self,
        public_id: str,
        idempotency_key: str | None,
        bus_time: datetime,
        session_id: str,
        sequence_id: int,
        cancel_requested_at: datetime,
    ) -> CancelClaimResult:
        """Atomically claim the cancel transition under FOR UPDATE lock."""
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
                return CancelClaimResult(outcome="not_found", plan=None)
            existing_key = existing.cancel_idempotency_key
            existing_dict = self._plan_row_to_dict(existing)
            if (
                idempotency_key is not None
                and existing_key is not None
                and existing_key == idempotency_key
            ):
                return CancelClaimResult(outcome="replay", plan=existing_dict)
            if existing_key is not None and existing_key != idempotency_key:
                return CancelClaimResult(outcome="key_mismatch", plan=existing_dict)
            if existing.status in self._CANCEL_TERMINAL_STATUSES:
                return CancelClaimResult(outcome="terminal", plan=existing_dict)
            if existing.status == "cancel_requested":
                return CancelClaimResult(outcome="in_progress", plan=existing_dict)
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
                filled_quantity=existing.filled_quantity,
                side=existing.side,
                parent_plan_public_id=existing.parent_plan_public_id,
                position_cycle_public_id=existing.position_cycle_public_id,
                params=existing.params,
                status="cancel_requested",
                created_at=existing.created_at,
                started_at=existing.started_at,
                completed_at=existing.completed_at,
                expires_at=existing.expires_at,
                cancel_requested_at=cancel_requested_at,
                last_evaluated_at=existing.last_evaluated_at,
                last_error=existing.last_error,
                idempotency_key=existing.idempotency_key,
                cancel_idempotency_key=idempotency_key,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=bus_time,
            )
            s.add(new_plan)
            await s.commit()
            await s.refresh(new_plan)
            return CancelClaimResult(outcome="claimed", plan=self._plan_row_to_dict(new_plan))

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
            return self._checkpoint_row_to_dict(row)

    async def get_latest_checkpoints_for_plans(
        self,
        plan_public_ids: list[str],
    ) -> dict[str, ExecutionPlanCheckpointRow]:
        """Bulk variant — one round-trip per ``_PLAN_CHECKPOINT_LOOKUP_CHUNK_SIZE``.

        The SCD2 close-on-insert invariant in
        :py:meth:`insert_execution_plan_checkpoint` guarantees at most
        one active row per ``plan_public_id`` at ``now``, so a single
        IN-list lookup is enough. We still sort by
        ``(plan_public_id, checkpoint_at DESC)`` and keep only the first
        match per plan as belt-and-braces against an unforeseen
        invariant break.
        """
        if not plan_public_ids:
            return {}
        unique_ids = list(dict.fromkeys(plan_public_ids))
        result_map: dict[str, ExecutionPlanCheckpointRow] = {}
        async with self.session() as s:
            now = datetime.now(UTC)
            for offset in range(0, len(unique_ids), _PLAN_CHECKPOINT_LOOKUP_CHUNK_SIZE):
                chunk = unique_ids[offset : offset + _PLAN_CHECKPOINT_LOOKUP_CHUNK_SIZE]
                stmt = (
                    select(ExecutionPlanCheckpoint)
                    .where(
                        ExecutionPlanCheckpoint.plan_public_id.in_(chunk),
                        *where_active(ExecutionPlanCheckpoint, now),
                    )
                    .order_by(
                        ExecutionPlanCheckpoint.plan_public_id,
                        ExecutionPlanCheckpoint.checkpoint_at.desc(),
                    )
                )
                result = await s.execute(stmt)
                for row in result.scalars().all():
                    if row.plan_public_id in result_map:
                        continue
                    result_map[row.plan_public_id] = self._checkpoint_row_to_dict(row)
        return result_map

    @staticmethod
    def _checkpoint_row_to_dict(
        row: ExecutionPlanCheckpoint,
    ) -> ExecutionPlanCheckpointRow:
        """Project an ORM checkpoint row to the TypedDict shape callers consume."""
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
                source_surface=row["source_surface"],
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
                    source_surface=d.source_surface,
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
                cancel_idempotency_key=existing.cancel_idempotency_key,
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

    async def get_open_position_cycles_for_shards(
        self,
        shard_keys: list[str],
        as_of: datetime,
    ) -> dict[str, PositionCycleRow]:
        """Batch-fetch open position cycles for many shards."""
        if not shard_keys:
            return {}
        async with self.session() as s:
            stmt = select(PositionCycle).where(
                PositionCycle.shard_key.in_(shard_keys),
                PositionCycle.status == "open",
                *where_active(PositionCycle, as_of),
            )
            result = await s.execute(stmt)
            return {
                row.shard_key: self._position_cycle_row_to_dict(row) for row in result.scalars()
            }

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
            result = await s.execute(
                select(
                    WalletOperatorScopeGrant.scope_kind,
                    WalletOperatorScopeGrant.underlying_public_id,
                    WalletOperatorScopeGrant.instrument_public_id,
                )
                .where(
                    WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                    WalletOperatorScopeGrant.operator_public_id == operator_public_id,
                    *where_active(WalletOperatorScopeGrant, as_of),
                )
                .order_by(WalletOperatorScopeGrant.timestamp.asc())
            )
            refs = {
                self._scope_expansion_key(scope_kind, underlying_public_id, instrument_public_id)
                for scope_kind, underlying_public_id, instrument_public_id in result.all()
            }
            if not refs:
                return set()
            expanded = await self._expand_scope_refs_to_instruments(s, refs, as_of)
            covered: set[str] = set()
            for instrument_ids in expanded.values():
                covered.update(instrument_ids)
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

    async def get_instrument_public_ids_by_symbols(
        self,
        native_symbols: set[str],
        exchange: str,
        as_of: datetime,
    ) -> dict[str, str]:
        """Resolve native symbols on an exchange to instrument public IDs.

        Args:
            native_symbols: Native symbol strings to resolve.
            exchange: Exchange name.
            as_of: Point-in-time for temporal query.

        Returns:
            Mapping from native symbol to active instrument public ID.
            Missing symbols and symbols without an active exchange
            instrument are omitted.
        """
        async with self.session() as s:
            s_ts, s_kt = where_active(Symbol, as_of)
            i_ts, i_kt = where_active(Instrument, as_of)
            result = await s.execute(
                select(Symbol.native_symbol, Instrument.public_id)
                .join(Instrument, Instrument.symbol_public_id == Symbol.public_id)
                .where(
                    Symbol.native_symbol.in_(tuple(native_symbols)),
                    Instrument.exchange == exchange,
                    s_ts,
                    s_kt,
                    i_ts,
                    i_kt,
                )
            )
            return dict(result.tuples().all())

    async def get_symbol_for_instrument(
        self,
        instrument_public_id: str,
        as_of: datetime,
    ) -> str | None:
        """Resolve an instrument public_id back to its native symbol.

        Single joined query over active ``Instrument`` + active ``Symbol``
        at ``as_of`` — both ``where_active`` predicates are combined into
        one SQL statement so the two rows are guaranteed to be from the
        same temporal snapshot (avoids the subtle read-skew that two
        sequential queries would expose under concurrent SCD2 writers).
        """
        async with self.session() as s:
            i_ts, i_kt = where_active(Instrument, as_of)
            s_ts, s_kt = where_active(Symbol, as_of)
            native = (
                await s.execute(
                    select(Symbol.native_symbol)
                    .join(Instrument, Instrument.symbol_public_id == Symbol.public_id)
                    .where(
                        Instrument.public_id == instrument_public_id,
                        i_ts,
                        i_kt,
                        s_ts,
                        s_kt,
                    )
                )
            ).scalar_one_or_none()
            return native

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

    async def list_scope_grant_instrument_pairs(
        self,
        operator_public_ids: list[str],
        as_of: datetime,
    ) -> set[tuple[str, str]]:
        """Project active grants held by operators to (exchange, native_symbol) pairs."""
        if not operator_public_ids:
            return set()
        async with self.session() as s:
            grants_result = await s.execute(
                select(WalletOperatorScopeGrant).where(
                    WalletOperatorScopeGrant.operator_public_id.in_(operator_public_ids),
                    *where_active(WalletOperatorScopeGrant, as_of),
                )
            )
            grants = grants_result.scalars().all()
            if not grants:
                return set()

            direct_instrument_ids: set[str] = set()
            underlying_ids: set[str] = set()
            for grant in grants:
                if grant.scope_kind == "instrument":
                    direct_instrument_ids.add(cast(str, grant.instrument_public_id))
                else:
                    underlying_ids.add(cast(str, grant.underlying_public_id))

            expanded_instrument_ids: set[str] = set(direct_instrument_ids)
            if underlying_ids:
                mapping_result = await s.execute(
                    select(InstrumentUnderlyingMapping.instrument_public_id).where(
                        InstrumentUnderlyingMapping.underlying_public_id.in_(underlying_ids),
                        *where_active(InstrumentUnderlyingMapping, as_of),
                    )
                )
                expanded_instrument_ids.update(row[0] for row in mapping_result.all())

            if not expanded_instrument_ids:
                return set()

            pairs_result = await s.execute(
                select(Instrument.exchange, Symbol.native_symbol)
                .select_from(Instrument)
                .join(Symbol, Instrument.symbol_public_id == Symbol.public_id)
                .where(
                    Instrument.public_id.in_(expanded_instrument_ids),
                    *where_active(Instrument, as_of),
                    *where_active(Symbol, as_of),
                )
            )
            return {(exchange, native_symbol) for exchange, native_symbol in pairs_result.all()}

    async def revoke_scope_grant(
        self,
        grant_public_id: str,
        revoked_by_user_public_id: str,
        revoked_at: datetime,
        reason: str | None,
    ) -> ScopeGrantRow:
        """Atomic SCD2 close (no replacement row) under per-wallet advisory lock."""
        del revoked_by_user_public_id, reason
        async with self.session() as s:
            grant = (
                (
                    await s.execute(
                        select(WalletOperatorScopeGrant).where(
                            WalletOperatorScopeGrant.public_id == grant_public_id,
                            *where_active(WalletOperatorScopeGrant, revoked_at),
                        )
                    )
                )
                .scalars()
                .first()
            )
            if grant is None:
                raise ScopeGrantNotFoundError(
                    f"active scope grant {grant_public_id} not found at {revoked_at.isoformat()}"
                )

            await self._acquire_wallet_advisory_lock(s, grant.wallet_public_id)

            result = await s.execute(
                update(WalletOperatorScopeGrant)
                .where(
                    WalletOperatorScopeGrant.id == grant.id,
                    *where_active(WalletOperatorScopeGrant, revoked_at),
                )
                .values(known_to=revoked_at)
            )
            if int(cast(Any, result).rowcount or 0) == 0:
                raise ScopeGrantNotFoundError(
                    f"active scope grant {grant_public_id} "
                    f"no longer active at {revoked_at.isoformat()} (concurrent mutation)"
                )
            await s.commit()

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
                known_to=revoked_at,
                session_id=grant.session_id,
                sequence_id=grant.sequence_id,
            )

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
                    f"active credential {credential_public_id} not found at {timestamp.isoformat()}"
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

    @staticmethod
    def _notification_device_row_from(model: NotificationDevice) -> NotificationDeviceRow:
        """Map ORM ``NotificationDevice`` (SCD2) to its TypedDict row shape."""
        return NotificationDeviceRow(
            public_id=model.public_id,
            session_id=model.session_id,
            sequence_id=model.sequence_id,
            timestamp=model.timestamp,
            known_to=model.known_to,
            user_public_id=model.user_public_id,
            device_token=model.device_token,
            device_id=model.device_id,
            platform=model.platform,
            env=model.env,
            app_version=model.app_version,
            previews_mode=model.previews_mode,
            registered_at=model.registered_at,
            last_seen_at=model.last_seen_at,
            token_status=model.token_status,
        )

    @staticmethod
    def _device_alert_pref_row_from(model: DeviceAlertPref) -> DeviceAlertPrefRow:
        """Map ORM ``DeviceAlertPref`` (SCD2) to its TypedDict row shape."""
        return DeviceAlertPrefRow(
            public_id=model.public_id,
            session_id=model.session_id,
            sequence_id=model.sequence_id,
            timestamp=model.timestamp,
            known_to=model.known_to,
            device_public_id=model.device_public_id,
            alert_type=model.alert_type,
            operator_public_id=model.operator_public_id,
            wallet_public_id=model.wallet_public_id,
            enabled=model.enabled,
            min_priority=model.min_priority,
            quiet_hours_start_min=model.quiet_hours_start_min,
            quiet_hours_end_min=model.quiet_hours_end_min,
            mute_until=model.mute_until,
            timezone=model.timezone,
        )

    @staticmethod
    def _user_alert_default_row_from(model: UserAlertDefault) -> UserAlertDefaultRow:
        """Map ORM ``UserAlertDefault`` (SCD2) to its TypedDict row shape."""
        return UserAlertDefaultRow(
            public_id=model.public_id,
            session_id=model.session_id,
            sequence_id=model.sequence_id,
            timestamp=model.timestamp,
            known_to=model.known_to,
            user_public_id=model.user_public_id,
            alert_type=model.alert_type,
            enabled=model.enabled,
            min_priority=model.min_priority,
        )

    @staticmethod
    def _alert_event_row_from(model: AlertEvent) -> AlertEventRow:
        """Map ORM ``AlertEvent`` (SCD2) to its TypedDict read-row shape."""
        return AlertEventRow(
            public_id=model.public_id,
            session_id=model.session_id,
            sequence_id=model.sequence_id,
            timestamp=model.timestamp,
            known_to=model.known_to,
            user_public_id=model.user_public_id,
            operator_public_id=model.operator_public_id,
            wallet_public_id=model.wallet_public_id,
            alert_type=model.alert_type,
            priority=model.priority,
            is_safety_critical=model.is_safety_critical,
            title=model.title,
            body=model.body,
            payload=model.payload,
            dedup_key=model.dedup_key,
            thread_key=model.thread_key,
            source_topic=model.source_topic,
        )

    @staticmethod
    def _alert_delivery_row_from(model: AlertDelivery) -> AlertDeliveryRow:
        """Map ORM ``AlertDelivery`` (SCD2) to its TypedDict read-row shape."""
        return AlertDeliveryRow(
            public_id=model.public_id,
            session_id=model.session_id,
            sequence_id=model.sequence_id,
            timestamp=model.timestamp,
            known_to=model.known_to,
            alert_event_public_id=model.alert_event_public_id,
            device_public_id=model.device_public_id,
            user_public_id=model.user_public_id,
            operator_public_id=model.operator_public_id,
            wallet_public_id=model.wallet_public_id,
            status=model.status,
            attempt_count=model.attempt_count,
            last_attempt_at=model.last_attempt_at,
            next_attempt_at=model.next_attempt_at,
            apns_id=model.apns_id,
            error_reason=model.error_reason,
            created_at=model.created_at,
        )

    async def upsert_notification_device(self, row: NotificationDeviceUpsertRow) -> str:
        """Register or refresh a device via SCD2 close-and-insert.

        Two concurrent callers on the same ``device_token`` converge
        idempotently. Two race paths are
        handled, both via the one-retry outer loop:
          - **IntegrityError on INSERT** (competitor committed a new
            active row between our SELECT and our COMMIT — partial
            unique on ``(device_token) WHERE known_to=MAX`` or on
            ``(public_id) WHERE known_to=MAX``).
          - **rowcount=0 on close UPDATE** (competitor closed the
            exact active row we SELECT'd before our UPDATE took the
            write lock). Detecting this early — rather than blundering
            into an INSERT that will collide on ``(public_id) WHERE
            known_to=MAX`` — keeps retries bounded under multi-way
            contention, because it short-circuits before the INSERT
            accidentally touches the competitor's brand-new active
            successor (which would waste an attempt).

        On retry, the new session SELECTs the winner's active row and
        close+inserts a new active version keyed on the winner's
        stable ``public_id``.
        """
        timestamp = row["timestamp"]
        max_attempts = 20
        last_error: Exception = RuntimeError("upsert_notification_device: retry budget exhausted.")
        for _attempt in range(max_attempts):
            async with self.session() as s:
                existing = (
                    await s.execute(
                        select(NotificationDevice).where(
                            NotificationDevice.device_token == row["device_token"],
                            NotificationDevice.known_to == KNOWN_TO_MAX,
                            NotificationDevice.token_status == "active",
                        )
                    )
                ).scalar_one_or_none()
                public_id = (
                    existing.public_id
                    if existing is not None
                    else (row.get("public_id") or str(uuid7()))
                )
                if existing is not None:
                    close_result = await s.execute(
                        update(NotificationDevice)
                        .where(
                            NotificationDevice.id == existing.id,
                            NotificationDevice.known_to == KNOWN_TO_MAX,
                        )
                        .values(known_to=timestamp)
                        .execution_options(synchronize_session=False)
                    )
                    if int(cast(Any, close_result).rowcount or 0) == 0:
                        await s.rollback()
                        last_error = RuntimeError(
                            "upsert_notification_device close-race: active row"
                            " closed by competitor between SELECT and UPDATE."
                        )
                        continue
                device = NotificationDevice(
                    public_id=public_id,
                    session_id=row["session_id"],
                    sequence_id=row["sequence_id"],
                    timestamp=timestamp,
                    known_to=row.get("known_to", KNOWN_TO_MAX),
                    user_public_id=row["user_public_id"],
                    device_token=row["device_token"],
                    device_id=row["device_id"],
                    platform=row.get("platform", "ios"),
                    env=row["env"],
                    app_version=row.get("app_version"),
                    previews_mode=row.get("previews_mode", "private"),
                    registered_at=row["registered_at"],
                    last_seen_at=row.get("last_seen_at"),
                    token_status=row.get("token_status", "active"),
                )
                s.add(device)
                try:
                    await s.commit()
                    return public_id
                except IntegrityError as exc:
                    await s.rollback()
                    last_error = exc
                    continue
        logger.warning(
            "upsert_notification_device: sustained contention exhausted"
            " {max_attempts}-attempt retry budget on device_token={tok};"
            " raising last_error={err}",
            max_attempts=max_attempts,
            tok=row["device_token"],
            err=type(last_error).__name__,
        )
        raise last_error

    async def list_active_notification_devices_for_user(
        self, user_public_id: str
    ) -> list[NotificationDeviceRow]:
        """Active iOS devices owned by ``user_public_id``, newest-first.

        Tombstone successor rows
        (``token_status IN ('unregistered', 'user_unregistered')``)
        are excluded: APNs must not receive sends for a token that has
        already been 410-rejected or voluntarily unregistered. The
        partial indexes on ``notification_devices`` already gate on
        this predicate; the explicit ``token_status = 'active'`` in
        the WHERE clause makes the intent readable from the call site.
        """
        async with self.session() as s:
            result = await s.execute(
                select(NotificationDevice)
                .where(
                    NotificationDevice.user_public_id == user_public_id,
                    NotificationDevice.known_to == KNOWN_TO_MAX,
                    NotificationDevice.token_status == "active",
                )
                .order_by(NotificationDevice.registered_at.desc())
            )
            return [self._notification_device_row_from(row) for row in result.scalars().all()]

    async def list_active_notification_devices_for_users(
        self, user_public_ids: list[str]
    ) -> dict[str, list[NotificationDeviceRow]]:
        """Bulk active-device lookup keyed by user_public_id.

        Replaces the per-row :meth:`list_active_notification_devices_for_user`
        round-trip on the notify sidecar drain/retry hot path. One
        ``WHERE user_public_id IN (...)`` SELECT returns every active
        device for the batch; rows are grouped in Python by
        ``user_public_id`` preserving the per-user newest-first ordering
        the original method guarantees.
        """
        if not user_public_ids:
            return {}
        unique_ids = list(dict.fromkeys(user_public_ids))
        async with self.session() as s:
            result = await s.execute(
                select(NotificationDevice)
                .where(
                    NotificationDevice.user_public_id.in_(unique_ids),
                    NotificationDevice.known_to == KNOWN_TO_MAX,
                    NotificationDevice.token_status == "active",
                )
                .order_by(NotificationDevice.registered_at.desc())
            )
            grouped: dict[str, list[NotificationDeviceRow]] = {pid: [] for pid in unique_ids}
            for row in result.scalars().all():
                grouped[row.user_public_id].append(self._notification_device_row_from(row))
        return grouped

    async def deactivate_notification_device_scd2(
        self,
        public_id: str,
        *,
        reason: str,
        timestamp: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """Close active row + insert tombstone successor (INV-9)."""
        async with self.session() as s:
            existing = (
                await s.execute(
                    select(NotificationDevice).where(
                        NotificationDevice.public_id == public_id,
                        NotificationDevice.known_to == KNOWN_TO_MAX,
                        NotificationDevice.token_status == "active",
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                return False
            existing.known_to = timestamp
            await s.flush()
            successor = NotificationDevice(
                public_id=existing.public_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=timestamp,
                known_to=KNOWN_TO_MAX,
                user_public_id=existing.user_public_id,
                device_token=existing.device_token,
                device_id=existing.device_id,
                platform=existing.platform,
                env=existing.env,
                app_version=existing.app_version,
                previews_mode=existing.previews_mode,
                registered_at=existing.registered_at,
                last_seen_at=existing.last_seen_at,
                token_status=reason,
            )
            s.add(successor)
            await s.commit()
            return True

    async def list_device_alert_prefs_for_user(
        self, user_public_id: str
    ) -> list[DeviceAlertPrefRow]:
        """Active per-device prefs for user's active devices.

        The device-side join filters by ``token_status = 'active'`` in
        addition to ``known_to = KNOWN_TO_MAX`` because tombstone
        successor rows
        (``token_status IN ('unregistered', 'user_unregistered')``)
        also remain at ``known_to = KNOWN_TO_MAX``. Without the
        status filter, prefs attached to a 410'd or user-unregistered
        device would leak back into this listing (regression closure).
        """
        async with self.session() as s:
            result = await s.execute(
                select(DeviceAlertPref)
                .join(
                    NotificationDevice,
                    NotificationDevice.public_id == DeviceAlertPref.device_public_id,
                )
                .where(
                    NotificationDevice.user_public_id == user_public_id,
                    NotificationDevice.known_to == KNOWN_TO_MAX,
                    NotificationDevice.token_status == "active",
                    DeviceAlertPref.known_to == KNOWN_TO_MAX,
                )
            )
            return [self._device_alert_pref_row_from(r) for r in result.scalars().all()]

    @staticmethod
    def _active_device_alert_pref_query(
        row: DeviceAlertPrefUpsertRow,
    ) -> Select[tuple[DeviceAlertPref]]:
        """Build the active-row lookup for a device alert preference scope."""
        operator_public_id = row.get("operator_public_id")
        wallet_public_id = row.get("wallet_public_id")
        operator_filter = (
            DeviceAlertPref.operator_public_id.is_(None)
            if operator_public_id is None
            else DeviceAlertPref.operator_public_id == operator_public_id
        )
        wallet_filter = (
            DeviceAlertPref.wallet_public_id.is_(None)
            if wallet_public_id is None
            else DeviceAlertPref.wallet_public_id == wallet_public_id
        )
        return select(DeviceAlertPref).where(
            DeviceAlertPref.device_public_id == row["device_public_id"],
            DeviceAlertPref.alert_type == row["alert_type"],
            DeviceAlertPref.known_to == KNOWN_TO_MAX,
            operator_filter,
            wallet_filter,
        )

    @staticmethod
    def _device_alert_pref_values(
        row: DeviceAlertPrefUpsertRow,
        existing: DeviceAlertPref | None,
    ) -> _DeviceAlertPrefValues:
        """Merge optional preference fields with the active row or defaults."""
        if existing is None:
            return _DeviceAlertPrefValues(
                public_id=row.get("public_id") or str(uuid7()),
                enabled=row.get("enabled", True),
                min_priority=row.get("min_priority", "medium"),
                quiet_hours_start_min=row.get("quiet_hours_start_min"),
                quiet_hours_end_min=row.get("quiet_hours_end_min"),
                mute_until=row.get("mute_until"),
                timezone=row.get("timezone", "UTC"),
            )
        return _DeviceAlertPrefValues(
            public_id=existing.public_id,
            enabled=row.get("enabled", existing.enabled),
            min_priority=row.get("min_priority", existing.min_priority),
            quiet_hours_start_min=row.get("quiet_hours_start_min", existing.quiet_hours_start_min),
            quiet_hours_end_min=row.get("quiet_hours_end_min", existing.quiet_hours_end_min),
            mute_until=row.get("mute_until", existing.mute_until),
            timezone=row.get("timezone", existing.timezone),
        )

    @staticmethod
    def _build_device_alert_pref(
        row: DeviceAlertPrefUpsertRow,
        values: _DeviceAlertPrefValues,
    ) -> DeviceAlertPref:
        """Create the replacement active ``DeviceAlertPref`` ORM object."""
        return DeviceAlertPref(
            public_id=values.public_id,
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            timestamp=row["timestamp"],
            known_to=row.get("known_to", KNOWN_TO_MAX),
            device_public_id=row["device_public_id"],
            alert_type=row["alert_type"],
            operator_public_id=row.get("operator_public_id"),
            wallet_public_id=row.get("wallet_public_id"),
            enabled=values.enabled,
            min_priority=values.min_priority,
            quiet_hours_start_min=values.quiet_hours_start_min,
            quiet_hours_end_min=values.quiet_hours_end_min,
            mute_until=values.mute_until,
            timezone=values.timezone,
        )

    @staticmethod
    def _affected_rows(result: object) -> int:
        """Return rowcount from SQLAlchemy update results."""
        return int(cast(Any, result).rowcount or 0)

    @staticmethod
    async def _close_active_device_alert_pref(
        s: AsyncSession,
        existing: DeviceAlertPref | None,
        timestamp: datetime,
    ) -> Exception | None:
        """Close the existing active preference row when one was found."""
        if existing is None:
            return None
        close_result = await s.execute(
            update(DeviceAlertPref)
            .where(
                DeviceAlertPref.id == existing.id,
                DeviceAlertPref.known_to == KNOWN_TO_MAX,
            )
            .values(known_to=timestamp)
            .execution_options(synchronize_session=False)
        )
        if SQLAlchemyRepository._affected_rows(close_result) > 0:
            return None
        await s.rollback()
        return RuntimeError(
            "upsert_device_alert_pref close-race: active row"
            " closed by competitor between SELECT and UPDATE."
        )

    @staticmethod
    async def _commit_write_attempt(s: AsyncSession) -> Exception | None:
        """Commit a retryable write attempt and return the retry cause."""
        try:
            await s.commit()
        except IntegrityError as exc:
            await s.rollback()
            return exc
        return None

    async def _try_upsert_device_alert_pref(
        self,
        row: DeviceAlertPrefUpsertRow,
    ) -> _DeviceAlertPrefAttemptResult:
        """Run one retryable device preference upsert attempt."""
        async with self.session() as s:
            existing = (
                await s.execute(self._active_device_alert_pref_query(row))
            ).scalar_one_or_none()
            values = self._device_alert_pref_values(row, existing)
            close_error = await self._close_active_device_alert_pref(
                s,
                existing,
                row["timestamp"],
            )
            if close_error is not None:
                return _DeviceAlertPrefAttemptResult(public_id=values.public_id, error=close_error)
            s.add(self._build_device_alert_pref(row, values))
            return _DeviceAlertPrefAttemptResult(
                public_id=values.public_id,
                error=await self._commit_write_attempt(s),
            )

    async def upsert_device_alert_pref(self, row: DeviceAlertPrefUpsertRow) -> str:
        """Atomic SCD2 close + insert on (device, alert_type, scope_tuple).

        Concurrent same-scope upserts converge idempotently via the
        same atomic-close + IntegrityError-retry pattern as
        ``upsert_notification_device``; the partial unique indexes
        ``uq_device_alert_{device,operator,wallet}_scope`` bound one
        active row per scope permutation.
        Returns the stable ``public_id`` preserved across versions —
        callers use it to synthesize the response without a second
        read that would race against other writers on the same scope
        (avoids the post-upsert race).
        """
        max_attempts = 20
        last_error: Exception = RuntimeError("upsert_device_alert_pref: retry budget exhausted.")
        for _attempt in range(max_attempts):
            result = await self._try_upsert_device_alert_pref(row)
            if result.error is None:
                return result.public_id
            last_error = result.error
        logger.warning(
            "upsert_device_alert_pref: sustained contention exhausted"
            " {max_attempts}-attempt retry budget on"
            " (device={dev}, alert_type={at}); raising last_error={err}",
            max_attempts=max_attempts,
            dev=row["device_public_id"],
            at=row["alert_type"],
            err=type(last_error).__name__,
        )
        raise last_error

    async def deactivate_device_alert_pref_scd2(
        self,
        pref_public_id: str,
        *,
        device_public_id: str,
        timestamp: datetime,
    ) -> DeviceAlertPrefRow | None:
        """SCD2 close in place; idempotent + ownership-guarded.

        Filters by both ``public_id`` and ``device_public_id`` so a
        caller who owns device A cannot close a pref attached to
        device B by submitting B's pref id; the route's
        ``list_active_notification_devices_for_user`` ownership check
        already gates on the device, this filter belt-and-braces
        that gate at the storage layer.
        """
        async with self.session() as s:
            existing = (
                await s.execute(
                    select(DeviceAlertPref).where(
                        DeviceAlertPref.public_id == pref_public_id,
                        DeviceAlertPref.device_public_id == device_public_id,
                        DeviceAlertPref.known_to == KNOWN_TO_MAX,
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                return None
            projection = self._device_alert_pref_row_from(existing)
            existing.known_to = timestamp
            await s.commit()
            return projection

    async def list_user_alert_defaults(self, user_public_id: str) -> list[UserAlertDefaultRow]:
        """Active user-level fallback prefs."""
        async with self.session() as s:
            result = await s.execute(
                select(UserAlertDefault).where(
                    UserAlertDefault.user_public_id == user_public_id,
                    UserAlertDefault.known_to == KNOWN_TO_MAX,
                )
            )
            return [self._user_alert_default_row_from(r) for r in result.scalars().all()]

    @staticmethod
    def _active_user_alert_default_query(
        row: UserAlertDefaultUpsertRow,
    ) -> Select[tuple[UserAlertDefault]]:
        """Build the active-row lookup for a user alert default."""
        return select(UserAlertDefault).where(
            UserAlertDefault.user_public_id == row["user_public_id"],
            UserAlertDefault.alert_type == row["alert_type"],
            UserAlertDefault.known_to == KNOWN_TO_MAX,
        )

    @staticmethod
    def _user_alert_default_values(
        row: UserAlertDefaultUpsertRow,
        existing: UserAlertDefault | None,
    ) -> _UserAlertDefaultValues:
        """Merge optional default fields with the active row or defaults."""
        if existing is None:
            return _UserAlertDefaultValues(
                public_id=row.get("public_id") or str(uuid7()),
                enabled=row.get("enabled", True),
                min_priority=row.get("min_priority", "medium"),
            )
        return _UserAlertDefaultValues(
            public_id=existing.public_id,
            enabled=row.get("enabled", existing.enabled),
            min_priority=row.get("min_priority", existing.min_priority),
        )

    @staticmethod
    def _build_user_alert_default(
        row: UserAlertDefaultUpsertRow,
        values: _UserAlertDefaultValues,
    ) -> UserAlertDefault:
        """Create the replacement active ``UserAlertDefault`` ORM object."""
        return UserAlertDefault(
            public_id=values.public_id,
            session_id=row["session_id"],
            sequence_id=row["sequence_id"],
            timestamp=row["timestamp"],
            known_to=row.get("known_to", KNOWN_TO_MAX),
            user_public_id=row["user_public_id"],
            alert_type=row["alert_type"],
            enabled=values.enabled,
            min_priority=values.min_priority,
        )

    @staticmethod
    async def _close_active_user_alert_default(
        s: AsyncSession,
        existing: UserAlertDefault | None,
        timestamp: datetime,
    ) -> Exception | None:
        """Close the existing active default row when one was found."""
        if existing is None:
            return None
        close_result = await s.execute(
            update(UserAlertDefault)
            .where(
                UserAlertDefault.id == existing.id,
                UserAlertDefault.known_to == KNOWN_TO_MAX,
            )
            .values(known_to=timestamp)
            .execution_options(synchronize_session=False)
        )
        if SQLAlchemyRepository._affected_rows(close_result) > 0:
            return None
        await s.rollback()
        return RuntimeError(
            "upsert_user_alert_default close-race: active row"
            " closed by competitor between SELECT and UPDATE."
        )

    async def _try_upsert_user_alert_default(
        self,
        row: UserAlertDefaultUpsertRow,
    ) -> _UserAlertDefaultAttemptResult:
        """Run one retryable user default upsert attempt."""
        async with self.session() as s:
            existing = (
                await s.execute(self._active_user_alert_default_query(row))
            ).scalar_one_or_none()
            values = self._user_alert_default_values(row, existing)
            close_error = await self._close_active_user_alert_default(
                s,
                existing,
                row["timestamp"],
            )
            if close_error is not None:
                return _UserAlertDefaultAttemptResult(public_id=values.public_id, error=close_error)
            s.add(self._build_user_alert_default(row, values))
            return _UserAlertDefaultAttemptResult(
                public_id=values.public_id,
                error=await self._commit_write_attempt(s),
            )

    async def upsert_user_alert_default(self, row: UserAlertDefaultUpsertRow) -> str:
        """Atomic SCD2 close + insert on (user, alert_type) default.

        Concurrent same-key upserts converge idempotently via the same
        atomic-close + IntegrityError-retry pattern as
        ``upsert_notification_device``.

        Returns:
            The stable ``public_id`` of the now-active row. Reused
            across SCD2 versions when an active row already existed
            for this ``(user, alert_type)`` tuple; freshly minted on
            the first write.
        """
        max_attempts = 20
        last_error: Exception = RuntimeError("upsert_user_alert_default: retry budget exhausted.")
        for _attempt in range(max_attempts):
            result = await self._try_upsert_user_alert_default(row)
            if result.error is None:
                return result.public_id
            last_error = result.error
        logger.warning(
            "upsert_user_alert_default: sustained contention exhausted"
            " {max_attempts}-attempt retry budget on"
            " (user={u}, alert_type={at}); raising last_error={err}",
            max_attempts=max_attempts,
            u=row["user_public_id"],
            at=row["alert_type"],
            err=type(last_error).__name__,
        )
        raise last_error

    async def insert_alert_event(self, row: AlertEventInsertRow) -> str:
        """Insert a temporal (SCD2) alert_events row."""
        public_id = row.get("public_id") or str(uuid7())
        async with self.session() as s:
            event_row = AlertEvent(
                public_id=public_id,
                session_id=row["session_id"],
                sequence_id=row["sequence_id"],
                timestamp=row["timestamp"],
                known_to=row.get("known_to", KNOWN_TO_MAX),
                user_public_id=row["user_public_id"],
                operator_public_id=row.get("operator_public_id"),
                wallet_public_id=row.get("wallet_public_id"),
                alert_type=row["alert_type"],
                priority=row["priority"],
                is_safety_critical=row.get("is_safety_critical", False),
                title=row["title"],
                body=row["body"],
                payload=row.get("payload"),
                dedup_key=row.get("dedup_key"),
                thread_key=row.get("thread_key"),
                source_topic=row.get("source_topic"),
            )
            s.add(event_row)
            await s.commit()
            return public_id

    async def list_recent_alerts_for_user(
        self,
        user_public_id: str,
        limit: int,
        before: AlertListCursor | None,
    ) -> list[AlertEventRow]:
        """Active alert_events rows, newest-first, composite keyset paginated.

        When ``before`` is provided, applies the keyset predicate
        directly from the cursor's ``(timestamp, public_id)`` — no
        re-derivation from the current active row, so paging stays
        stable even if the anchor alert_event is later SCD2-revised.
        Order: ``(timestamp DESC, public_id DESC)``.
        """
        async with self.session() as s:
            stmt = select(AlertEvent).where(
                AlertEvent.user_public_id == user_public_id,
                AlertEvent.known_to == KNOWN_TO_MAX,
            )
            if before is not None:
                cursor_ts = before["timestamp"]
                cursor_pid = before["public_id"]
                stmt = stmt.where(
                    or_(
                        AlertEvent.timestamp < cursor_ts,
                        and_(
                            AlertEvent.timestamp == cursor_ts,
                            AlertEvent.public_id < cursor_pid,
                        ),
                    )
                )
            stmt = stmt.order_by(AlertEvent.timestamp.desc(), AlertEvent.public_id.desc()).limit(
                limit
            )
            result = await s.execute(stmt)
            return [self._alert_event_row_from(row) for row in result.scalars().all()]

    async def get_alert_event_by_public_id(self, public_id: str) -> AlertEventRow | None:
        """Active alert_event by public_id, None if missing or SCD2-closed."""
        async with self.session() as s:
            result = await s.execute(
                select(AlertEvent).where(
                    AlertEvent.public_id == public_id,
                    AlertEvent.known_to == KNOWN_TO_MAX,
                )
            )
            event_row = result.scalar_one_or_none()
            if event_row is None:
                return None
            return self._alert_event_row_from(event_row)

    async def get_alert_events_by_public_ids(
        self, public_ids: list[str]
    ) -> dict[str, AlertEventRow]:
        """Bulk active-alert-event lookup keyed by public_id.

        Replaces the per-row :meth:`get_alert_event_by_public_id`
        round-trip on the notify sidecar drain/retry hot path. One
        ``WHERE public_id IN (...)`` SELECT returns every active event
        for the batch; missing public_ids are absent from the returned
        dict (not mapped to None).
        """
        if not public_ids:
            return {}
        unique_ids = list(dict.fromkeys(public_ids))
        async with self.session() as s:
            result = await s.execute(
                select(AlertEvent).where(
                    AlertEvent.public_id.in_(unique_ids),
                    AlertEvent.known_to == KNOWN_TO_MAX,
                )
            )
            return {
                row.public_id: self._alert_event_row_from(row) for row in result.scalars().all()
            }

    async def list_alert_events_with_dedup_key(
        self,
        user_public_id: str,
        dedup_key: str,
        since: datetime,
    ) -> list[AlertEventRow]:
        """Dedup-window read: active AlertEvents for (user, dedup_key, >= since)."""
        async with self.session() as s:
            result = await s.execute(
                select(AlertEvent).where(
                    AlertEvent.user_public_id == user_public_id,
                    AlertEvent.dedup_key == dedup_key,
                    AlertEvent.timestamp >= since,
                    AlertEvent.known_to == KNOWN_TO_MAX,
                )
            )
            return [self._alert_event_row_from(row) for row in result.scalars().all()]

    async def get_default_languages_for_users(
        self, user_public_ids: list[str]
    ) -> dict[str, str | None]:
        """Bulk ``default_language`` lookup keyed by ``user_public_id``.

        One ``WHERE public_id IN (...)`` SELECT against the active
        ``users`` rows. Missing ``user_public_id``s map to ``None`` so
        the sidecar treats unknown users + users-with-no-preference
        identically (both fall back to English emission).
        """
        if not user_public_ids:
            return {}
        unique_ids = list(dict.fromkeys(user_public_ids))
        result: dict[str, str | None] = dict.fromkeys(unique_ids)
        async with self.session() as s:
            rows = await s.execute(
                select(User.public_id, User.default_language).where(
                    User.public_id.in_(unique_ids),
                    User.known_to == KNOWN_TO_MAX,
                )
            )
            result.update(dict(rows.tuples().all()))
        return result

    async def list_users_with_permission(self, permission: str) -> list[str]:
        """Active user public_ids whose role grants ``permission``.

        Role → permissions mapping is read from
        ``snapper.auth.domain.permissions.ROLE_PERMISSIONS``; the
        matched role names are then used to filter the ``users`` SCD2
        table on its ``role`` column.
        """
        matching_roles: list[str] = [
            role.value
            for role, perms in ROLE_PERMISSIONS.items()
            if permission in {p.value for p in perms}
        ]
        if not matching_roles:
            return []
        async with self.session() as s:
            result = await s.execute(
                select(User.public_id)
                .where(
                    User.role.in_(matching_roles),
                    User.known_to == KNOWN_TO_MAX,
                )
                .order_by(User.timestamp.desc())
            )
            return list(result.scalars().all())

    async def insert_alert_delivery(self, row: AlertDeliveryInsertRow) -> str:
        """Insert a new SCD2 active version of an alert_delivery row.

        Scope columns (user/operator/wallet) MUST be set by the caller
        (denormalised from the source alert_event at queue time) — the
        repo does NOT rehydrate them from ``alert_events`` to avoid
        depending on the current SCD2 state of that table.
        """
        public_id = row.get("public_id") or str(uuid7())
        async with self.session() as s:
            delivery = AlertDelivery(
                public_id=public_id,
                session_id=row["session_id"],
                sequence_id=row["sequence_id"],
                timestamp=row["timestamp"],
                known_to=row.get("known_to", KNOWN_TO_MAX),
                alert_event_public_id=row["alert_event_public_id"],
                device_public_id=row["device_public_id"],
                user_public_id=row["user_public_id"],
                operator_public_id=row.get("operator_public_id"),
                wallet_public_id=row.get("wallet_public_id"),
                status=row["status"],
                attempt_count=row.get("attempt_count", 0),
                last_attempt_at=row.get("last_attempt_at"),
                next_attempt_at=row.get("next_attempt_at"),
                apns_id=row.get("apns_id"),
                error_reason=row.get("error_reason"),
                created_at=row["created_at"],
            )
            s.add(delivery)
            await s.commit()
            return public_id

    async def list_queued_deliveries_all(self) -> list[AlertDeliveryRow]:
        """Every active row with ``status='queued'`` (no time filter)."""
        async with self.session() as s:
            result = await s.execute(
                select(AlertDelivery)
                .where(
                    AlertDelivery.status == "queued",
                    AlertDelivery.known_to == KNOWN_TO_MAX,
                )
                .order_by(AlertDelivery.created_at.asc())
            )
            return [self._alert_delivery_row_from(r) for r in result.scalars().all()]

    async def get_delivery_by_public_id(self, public_id: str) -> AlertDeliveryRow | None:
        """Return the active SCD2 version of one delivery by public_id, or None.

        Indexed lookup replacing the per-row linear scan over
        :meth:`list_queued_deliveries_all` that the notify sidecar used
        on its hot retry path. Only the active SCD2 version is
        returned (``known_to == KNOWN_TO_MAX``).
        """
        async with self.session() as s:
            result = await s.execute(
                select(AlertDelivery).where(
                    AlertDelivery.public_id == public_id,
                    AlertDelivery.known_to == KNOWN_TO_MAX,
                )
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return self._alert_delivery_row_from(row)

    async def list_deliveries_ready_for_retry(self, now: datetime) -> list[AlertDeliveryRow]:
        """Active queued rows with ``next_attempt_at`` NULL or <= ``now``."""
        async with self.session() as s:
            result = await s.execute(
                select(AlertDelivery)
                .where(
                    AlertDelivery.status == "queued",
                    AlertDelivery.known_to == KNOWN_TO_MAX,
                    or_(
                        AlertDelivery.next_attempt_at.is_(None),
                        AlertDelivery.next_attempt_at <= now,
                    ),
                )
                .order_by(AlertDelivery.next_attempt_at.asc().nullsfirst())
            )
            return [self._alert_delivery_row_from(r) for r in result.scalars().all()]

    async def _scd2_transition_delivery(
        self,
        public_id: str,
        new_status: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
        apns_id: str | None = None,
        error_reason: str | None = None,
        attempt_count_override: int | None = None,
        next_attempt_at_override: datetime | None = None,
        last_attempt_at_override: datetime | None = None,
    ) -> bool:
        """Close the active ``alert_deliveries`` row and insert a new version.

        Only the ``queued`` active row may transition — every caller
        wants this guard (``mark_delivery_*``, ``cancel_*``,
        ``update_delivery_retry_schedule``), so the predicate is
        enforced unconditionally. Returns True when a transition
        happened, False when either:
          - no active row exists for ``public_id``;
          - the active row is no longer ``status='queued'`` (e.g. a
            previous ``mark_delivery_sent`` already ran — sequential
            idempotency);
          - another worker raced us to close this exact active row
            (conditional UPDATE rowcount==0 — concurrent idempotency).

        The close step is an atomic ``UPDATE ... WHERE id=:id AND
        known_to=MAX AND status='queued'`` — two concurrent workers
        both observing the queued row both attempt the UPDATE, but
        only one matches the post-lock predicate. The loser returns
        False without inserting a successor row, so the active-
        ``public_id`` partial unique index is never stressed.
        """
        async with self.session() as s:
            existing = (
                await s.execute(
                    select(AlertDelivery).where(
                        AlertDelivery.public_id == public_id,
                        AlertDelivery.known_to == KNOWN_TO_MAX,
                    )
                )
            ).scalar_one_or_none()
            if existing is None:
                return False
            if existing.status != "queued":
                return False
            close_result = await s.execute(
                update(AlertDelivery)
                .where(
                    AlertDelivery.id == existing.id,
                    AlertDelivery.known_to == KNOWN_TO_MAX,
                    AlertDelivery.status == "queued",
                )
                .values(known_to=transition_at)
                .execution_options(synchronize_session=False)
            )
            if int(cast(Any, close_result).rowcount or 0) == 0:
                await s.rollback()
                return False
            new_version = AlertDelivery(
                public_id=public_id,
                session_id=session_id,
                sequence_id=sequence_id,
                timestamp=transition_at,
                known_to=KNOWN_TO_MAX,
                alert_event_public_id=existing.alert_event_public_id,
                device_public_id=existing.device_public_id,
                user_public_id=existing.user_public_id,
                operator_public_id=existing.operator_public_id,
                wallet_public_id=existing.wallet_public_id,
                status=new_status,
                attempt_count=(
                    attempt_count_override
                    if attempt_count_override is not None
                    else existing.attempt_count
                ),
                last_attempt_at=(
                    last_attempt_at_override
                    if last_attempt_at_override is not None
                    else transition_at
                ),
                next_attempt_at=next_attempt_at_override,
                apns_id=apns_id if apns_id is not None else existing.apns_id,
                error_reason=error_reason if error_reason is not None else existing.error_reason,
                created_at=existing.created_at,
            )
            s.add(new_version)
            await s.commit()
            return True

    async def mark_delivery_sent(
        self,
        public_id: str,
        apns_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> sent (optimistic guard on queued)."""
        await self._scd2_transition_delivery(
            public_id,
            "sent",
            transition_at=transition_at,
            session_id=session_id,
            sequence_id=sequence_id,
            apns_id=apns_id,
        )

    async def mark_delivery_failed(
        self,
        public_id: str,
        error_reason: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> failed (terminal, give-up)."""
        await self._scd2_transition_delivery(
            public_id,
            "failed",
            transition_at=transition_at,
            session_id=session_id,
            sequence_id=sequence_id,
            error_reason=error_reason,
        )

    async def mark_delivery_unregistered(
        self,
        public_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> unregistered (APNs 410)."""
        await self._scd2_transition_delivery(
            public_id,
            "unregistered",
            transition_at=transition_at,
            session_id=session_id,
            sequence_id=sequence_id,
        )

    async def mark_delivery_cancelled(
        self,
        public_id: str,
        reason: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> None:
        """SCD2 transition queued -> cancelled_scope."""
        await self._scd2_transition_delivery(
            public_id,
            "cancelled_scope",
            transition_at=transition_at,
            session_id=session_id,
            sequence_id=sequence_id,
            error_reason=reason,
        )

    async def update_delivery_retry_schedule(
        self,
        public_id: str,
        attempt_count: int,
        next_attempt_at: datetime | None,
        error_reason: str | None,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """Bump attempt_count + reschedule next retry via SCD2 close+insert.

        Called BEFORE the APNs HTTP call (crash-safety). Status
        stays ``queued`` across versions.

        Returns:
            True when the active queued row was transitioned; False
            when the row is no longer queued (e.g. cancelled mid-send
            by an admin.scope_revoked handler racing the retry loop —
            race guard). Callers use the False branch to
            short-circuit the APNs send.
        """
        return await self._scd2_transition_delivery(
            public_id,
            "queued",
            transition_at=transition_at,
            session_id=session_id,
            sequence_id=sequence_id,
            error_reason=error_reason,
            attempt_count_override=attempt_count,
            next_attempt_at_override=next_attempt_at,
        )

    async def list_users_with_operator_membership(
        self, operator_public_id: str, as_of: datetime
    ) -> list[str]:
        """User public_ids with an active membership in ``operator_public_id``."""
        async with self.session() as s:
            result = await s.execute(
                select(UserOperatorMembership.user_public_id)
                .where(
                    UserOperatorMembership.operator_public_id == operator_public_id,
                    *where_active(UserOperatorMembership, as_of),
                )
                .distinct()
            )
            return list(result.scalars().all())

    async def is_scope_grant_active(
        self,
        user_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
        as_of: datetime,
    ) -> bool:
        """True iff the (user, operator, wallet) scope is active at ``as_of``."""
        async with self.session() as s:
            grant_exists = (
                await s.execute(
                    select(WalletOperatorScopeGrant.id)
                    .where(
                        WalletOperatorScopeGrant.operator_public_id == operator_public_id,
                        WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                        *where_active(WalletOperatorScopeGrant, as_of),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            if grant_exists is None:
                return False
            membership_exists = (
                await s.execute(
                    select(UserOperatorMembership.id)
                    .where(
                        UserOperatorMembership.user_public_id == user_public_id,
                        UserOperatorMembership.operator_public_id == operator_public_id,
                        *where_active(UserOperatorMembership, as_of),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            return membership_exists is not None

    async def has_grant_for_delegate(
        self,
        *,
        delegate_public_id: str,
        wallet_public_id: str,
        instrument_public_id: str,
        as_of: datetime,
    ) -> bool:
        """AI delegate scope check with underlying expansion.

        Resolves ``ai_delegates.public_id -> users.public_id``,
        joins on ``UserOperatorMembership`` for operator memberships,
        then checks ``WalletOperatorScopeGrant`` for matching
        ``scope_kind='instrument'`` (direct match) OR
        ``scope_kind='underlying'`` expanded via
        ``InstrumentUnderlyingMapping``.
        """
        async with self.session() as s:
            delegate_user_id = (
                await s.execute(
                    select(AiDelegate.user_public_id).where(
                        AiDelegate.public_id == delegate_public_id
                    )
                )
            ).scalar_one_or_none()
            if delegate_user_id is None:
                return False

            operator_ids = (
                (
                    await s.execute(
                        select(UserOperatorMembership.operator_public_id).where(
                            UserOperatorMembership.user_public_id == delegate_user_id,
                            *where_active(UserOperatorMembership, as_of),
                        )
                    )
                )
                .scalars()
                .all()
            )
            if not operator_ids:
                return False

            direct_grant = (
                await s.execute(
                    select(WalletOperatorScopeGrant.id)
                    .where(
                        WalletOperatorScopeGrant.operator_public_id.in_(operator_ids),
                        WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                        WalletOperatorScopeGrant.scope_kind == "instrument",
                        WalletOperatorScopeGrant.instrument_public_id == instrument_public_id,
                        *where_active(WalletOperatorScopeGrant, as_of),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            if direct_grant is not None:
                return True

            underlying_grant = (
                await s.execute(
                    select(WalletOperatorScopeGrant.id)
                    .join(
                        InstrumentUnderlyingMapping,
                        InstrumentUnderlyingMapping.underlying_public_id
                        == WalletOperatorScopeGrant.underlying_public_id,
                    )
                    .where(
                        WalletOperatorScopeGrant.operator_public_id.in_(operator_ids),
                        WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                        WalletOperatorScopeGrant.scope_kind == "underlying",
                        InstrumentUnderlyingMapping.instrument_public_id == instrument_public_id,
                        *where_active(WalletOperatorScopeGrant, as_of),
                        *where_active(InstrumentUnderlyingMapping, as_of),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            return underlying_grant is not None

    async def cancel_pending_deliveries_for_scope(
        self,
        user_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> int:
        """Bulk-cancel queued deliveries by scope columns on the delivery row.

        Filters on the denormalised scope columns on ``alert_deliveries``
        itself — does NOT join ``alert_events`` — so SCD2 corrections
        on the source event cannot cause misses. Each matching active
        row is SCD2-closed and a new version with
        ``status='cancelled_scope'`` is inserted via the per-row atomic
        close+insert helper so a concurrent retry-loop transition on
        the same row is a race the caller can win / lose safely.
        """
        async with self.session() as s:
            rows = await self._select_queued_deliveries_by_scope(
                s,
                user_public_id=user_public_id,
                operator_public_id=operator_public_id,
                wallet_public_id=wallet_public_id,
            )
            transitioned = 0
            for existing in rows:
                if await self._atomic_transition_queued_delivery(
                    s,
                    existing=existing,
                    new_status="cancelled_scope",
                    error_reason="scope_revoked",
                    transition_at=transition_at,
                    session_id=session_id,
                    sequence_id=sequence_id,
                ):
                    transitioned += 1
            await s.commit()
            return transitioned

    async def cancel_pending_deliveries_for_user(
        self,
        user_public_id: str,
        *,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
        error_reason: str = "user_deactivated",
    ) -> int:
        """Bulk-cancel every queued delivery for one user (admin kill-switch).

        Same per-row atomic close+insert pattern as
        ``cancel_pending_deliveries_for_scope`` — a concurrent retry
        loop transition on the same row is safely skipped when our
        close UPDATE affects zero rows (the competitor already closed
        it). Without this guard, inserting a ``cancelled_scope``
        successor for a row the competitor just transitioned to
        ``sent`` / ``failed`` would collide on the partial unique
        index ``(public_id, known_to=KNOWN_TO_MAX)`` and raise
        ``IntegrityError`` out of the admin-topic dispatcher — which
        in turn would kill the sidecar's receive loop.
        """
        async with self.session() as s:
            rows = await self._select_queued_deliveries_by_user(s, user_public_id)
            transitioned = 0
            for existing in rows:
                if await self._atomic_transition_queued_delivery(
                    s,
                    existing=existing,
                    new_status="cancelled_scope",
                    error_reason=error_reason,
                    transition_at=transition_at,
                    session_id=session_id,
                    sequence_id=sequence_id,
                ):
                    transitioned += 1
            await s.commit()
            return transitioned

    @staticmethod
    async def _select_queued_deliveries_by_scope(
        s: AsyncSession,
        *,
        user_public_id: str,
        operator_public_id: str,
        wallet_public_id: str,
    ) -> list[AlertDelivery]:
        """Snapshot active queued deliveries matching the scope triple."""
        result = await s.execute(
            select(AlertDelivery).where(
                AlertDelivery.status == "queued",
                AlertDelivery.known_to == KNOWN_TO_MAX,
                AlertDelivery.user_public_id == user_public_id,
                AlertDelivery.operator_public_id == operator_public_id,
                AlertDelivery.wallet_public_id == wallet_public_id,
            )
        )
        return list(result.scalars().all())

    @staticmethod
    async def _select_queued_deliveries_by_user(
        s: AsyncSession, user_public_id: str
    ) -> list[AlertDelivery]:
        """Snapshot active queued deliveries for the whole user."""
        result = await s.execute(
            select(AlertDelivery).where(
                AlertDelivery.status == "queued",
                AlertDelivery.known_to == KNOWN_TO_MAX,
                AlertDelivery.user_public_id == user_public_id,
            )
        )
        return list(result.scalars().all())

    @staticmethod
    async def _atomic_transition_queued_delivery(
        s: AsyncSession,
        *,
        existing: AlertDelivery,
        new_status: str,
        error_reason: str,
        transition_at: datetime,
        session_id: str,
        sequence_id: int,
    ) -> bool:
        """Atomically close one queued delivery + insert its terminal successor.

        The close is a guarded UPDATE on ``(id, known_to=KNOWN_TO_MAX,
        status='queued')`` — ``rowcount == 0`` means a competing
        transition (retry-loop or parallel admin handler) closed the
        row first, so we skip the successor insert and return False
        without contending for the partial-unique
        ``(public_id, known_to=KNOWN_TO_MAX)`` index.
        """
        close_result = await s.execute(
            update(AlertDelivery)
            .where(
                AlertDelivery.id == existing.id,
                AlertDelivery.known_to == KNOWN_TO_MAX,
                AlertDelivery.status == "queued",
            )
            .values(known_to=transition_at)
            .execution_options(synchronize_session=False)
        )
        if int(cast(Any, close_result).rowcount or 0) == 0:
            return False
        successor = AlertDelivery(
            public_id=existing.public_id,
            session_id=session_id,
            sequence_id=sequence_id,
            timestamp=transition_at,
            known_to=KNOWN_TO_MAX,
            alert_event_public_id=existing.alert_event_public_id,
            device_public_id=existing.device_public_id,
            user_public_id=existing.user_public_id,
            operator_public_id=existing.operator_public_id,
            wallet_public_id=existing.wallet_public_id,
            status=new_status,
            attempt_count=existing.attempt_count,
            last_attempt_at=existing.last_attempt_at,
            next_attempt_at=None,
            apns_id=existing.apns_id,
            error_reason=error_reason,
            created_at=existing.created_at,
        )
        s.add(successor)
        return True

    async def count_deliveries_by_status(self) -> dict[str, int]:
        """Aggregate counts of active ``alert_deliveries`` rows per ``status``."""
        async with self.session() as s:
            result = await s.execute(
                select(AlertDelivery.status, func.count())
                .where(AlertDelivery.known_to == KNOWN_TO_MAX)
                .group_by(AlertDelivery.status)
            )
            return {status: int(count) for status, count in result.all()}

    async def get_ai_review(self, review_public_id: str) -> AiReviewRow | None:
        """Fetch :class:`AiReview` row by public_id."""
        async with self.session() as s:
            row = (
                await s.execute(select(AiReview).where(AiReview.public_id == review_public_id))
            ).scalar_one_or_none()
            if row is None:
                return None
            return cast(
                AiReviewRow,
                {
                    "public_id": row.public_id,
                    "session_id": row.session_id,
                    "sequence_id": row.sequence_id,
                    "user_public_id": row.user_public_id,
                    "operator_public_id": row.operator_public_id,
                    "wallet_public_id": row.wallet_public_id,
                    "instrument_public_id": row.instrument_public_id,
                    "strategy_public_id": row.strategy_public_id,
                    "selected_delegate_public_id": row.selected_delegate_public_id,
                    "responding_delegate_public_id": row.responding_delegate_public_id,
                    "resolution_mode": row.resolution_mode,
                    "status": row.status,
                    "signal_envelope": row.signal_envelope,
                    "signal_snapshot_hash": row.signal_snapshot_hash,
                    "instrument_metadata": row.instrument_metadata,
                    "deadline": row.deadline,
                    "fanout_after": row.fanout_after,
                    "decision": row.decision,
                    "rationale": row.rationale,
                    "dispatch_version": row.dispatch_version,
                    "counter_decremented_at": row.counter_decremented_at,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                    "resolved_at": row.resolved_at,
                },
            )

    async def insert_ai_review(self, row: AiReviewInsertRow) -> str:
        """INSERT new :class:`AiReview` row; returns ``public_id``."""
        async with self.session() as s:
            review = AiReview(**row)
            s.add(review)
            await s.commit()
            return review.public_id

    async def get_ai_delegate_by_user_public_id(self, user_public_id: str) -> AiDelegateRow | None:
        """Lookup operational :class:`AiDelegate` row by user_public_id."""
        async with self.session() as s:
            row = (
                await s.execute(
                    select(AiDelegate).where(AiDelegate.user_public_id == user_public_id)
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return cast(
                AiDelegateRow,
                {
                    "public_id": row.public_id,
                    "user_public_id": row.user_public_id,
                    "last_seen_at": row.last_seen_at,
                    "active_reviews_count": row.active_reviews_count,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                },
            )

    async def insert_ai_delegate(
        self,
        *,
        public_id: str,
        user_public_id: str,
        as_of: datetime,
    ) -> str:
        """INSERT new :class:`AiDelegate` operational row; returns ``public_id``."""
        async with self.session() as s:
            delegate = AiDelegate(
                public_id=public_id,
                user_public_id=user_public_id,
                last_seen_at=None,
                active_reviews_count=0,
                created_at=as_of,
                updated_at=as_of,
            )
            s.add(delegate)
            await s.commit()
            return delegate.public_id

    async def update_delegate_last_seen(
        self, delegate_public_id: str, last_seen_at: datetime
    ) -> None:
        """Update ``ai_delegates.last_seen_at`` for reconnect hysteresis."""
        async with self.session() as s:
            await s.execute(
                update(AiDelegate)
                .where(AiDelegate.public_id == delegate_public_id)
                .values(last_seen_at=last_seen_at, updated_at=last_seen_at)
            )
            await s.commit()

    async def list_eligible_delegates_for_ai_review(
        self,
        *,
        operator_public_id: str,
        wallet_public_id: str,
        instrument_public_id: str,
        heartbeat_window_seconds: int,
        as_of: datetime,
    ) -> list[AiDelegateRow]:
        """Admission-control candidate list.

        Two-step query: first confirm the operator holds a matching
        scope grant (cheap LIMIT 1 lookup over instrument-direct then
        underlying-expanded shapes), then list live delegates whose
        users are members of that operator. Splitting the grant check
        from the delegate query keeps the JOIN trees shallow on
        SQLite (which optimises poorly for >3-way JOINs) while still
        producing a single answer per call.
        """
        async with self.session() as s:
            grant_id = (
                await s.execute(
                    select(WalletOperatorScopeGrant.id)
                    .where(
                        WalletOperatorScopeGrant.operator_public_id == operator_public_id,
                        WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                        WalletOperatorScopeGrant.scope_kind == "instrument",
                        WalletOperatorScopeGrant.instrument_public_id == instrument_public_id,
                        *where_active(WalletOperatorScopeGrant, as_of),
                    )
                    .limit(1)
                )
            ).scalar_one_or_none()
            if grant_id is None:
                grant_id = (
                    await s.execute(
                        select(WalletOperatorScopeGrant.id)
                        .join(
                            InstrumentUnderlyingMapping,
                            InstrumentUnderlyingMapping.underlying_public_id
                            == WalletOperatorScopeGrant.underlying_public_id,
                        )
                        .where(
                            WalletOperatorScopeGrant.operator_public_id == operator_public_id,
                            WalletOperatorScopeGrant.wallet_public_id == wallet_public_id,
                            WalletOperatorScopeGrant.scope_kind == "underlying",
                            InstrumentUnderlyingMapping.instrument_public_id
                            == instrument_public_id,
                            *where_active(WalletOperatorScopeGrant, as_of),
                            *where_active(InstrumentUnderlyingMapping, as_of),
                        )
                        .limit(1)
                    )
                ).scalar_one_or_none()
            if grant_id is None:
                return []
            threshold = as_of - timedelta(seconds=heartbeat_window_seconds)
            delegates = (
                (
                    await s.execute(
                        select(AiDelegate)
                        .join(User, User.public_id == AiDelegate.user_public_id)
                        .join(
                            UserOperatorMembership,
                            UserOperatorMembership.user_public_id == AiDelegate.user_public_id,
                        )
                        .where(
                            User.role == UserRole.AI_DELEGATE.value,
                            User.is_active.is_(True),
                            UserOperatorMembership.operator_public_id == operator_public_id,
                            AiDelegate.last_seen_at.is_not(None),
                            AiDelegate.last_seen_at > threshold,
                            *where_active(User, as_of),
                            *where_active(UserOperatorMembership, as_of),
                        )
                        .order_by(AiDelegate.last_seen_at.desc())
                    )
                )
                .scalars()
                .all()
            )
            return [
                cast(
                    AiDelegateRow,
                    {
                        "public_id": d.public_id,
                        "user_public_id": d.user_public_id,
                        "last_seen_at": d.last_seen_at,
                        "active_reviews_count": d.active_reviews_count,
                        "created_at": d.created_at,
                        "updated_at": d.updated_at,
                    },
                )
                for d in delegates
            ]

    async def claim_and_insert_ai_review(
        self,
        *,
        candidate_delegate_public_ids: list[str],
        review_data: AiReviewInsertRow,
        event_data: AiReviewEventInsertRow,
        now: datetime,
    ) -> str | None:
        """Atomic claim + INSERT for AI-review admission control.

        Iterates candidates in order; first one whose CAS UPDATE wins
        also gets INSERTed against. INSERT failure rolls the whole
        transaction back so no counter leaks; earlier CAS losers
        produced no data change and need no rollback.
        """
        async with self.session() as s:
            for candidate_id in candidate_delegate_public_ids:
                claim_stmt = (
                    update(AiDelegate)
                    .where(
                        AiDelegate.public_id == candidate_id,
                        AiDelegate.active_reviews_count == 0,
                    )
                    .values(
                        active_reviews_count=AiDelegate.active_reviews_count + 1,
                        updated_at=now,
                    )
                )
                claim_result = await s.execute(claim_stmt)
                if int(cast(Any, claim_result).rowcount or 0) == 0:
                    continue
                review_payload = dict(review_data)
                review_payload["selected_delegate_public_id"] = candidate_id
                s.add(AiReview(**review_payload))
                s.add(AiReviewEvent(**event_data))
                await s.commit()
                return candidate_id
            return None

    async def list_expired_pending_reviews(
        self,
        *,
        now: datetime,
        limit: int = 100,
    ) -> list[PendingReviewSummary]:
        """Reaper input: pending/fanout_dispatched past deadline."""
        async with self.session() as s:
            rows = (
                await s.execute(
                    select(
                        AiReview.public_id,
                        AiReview.selected_delegate_public_id,
                        AiReview.wallet_public_id,
                        AiReview.dispatch_version,
                        AiReview.status,
                        AiReview.deadline,
                        AiReview.fanout_after,
                    )
                    .where(
                        AiReview.status.in_(("pending", "fanout_dispatched")),
                        AiReview.deadline < now,
                    )
                    .order_by(AiReview.deadline.asc())
                    .limit(limit)
                )
            ).all()
            return [
                cast(
                    PendingReviewSummary,
                    {
                        "public_id": r[0],
                        "selected_delegate_public_id": r[1],
                        "wallet_public_id": str(r[2]),
                        "dispatch_version": int(r[3]),
                        "status": str(r[4]),
                        "deadline": r[5],
                        "fanout_after": r[6],
                    },
                )
                for r in rows
            ]

    async def list_offline_pending_reviews(
        self,
        *,
        now: datetime,
        heartbeat_window_seconds: int,
        limit: int = 100,
    ) -> list[PendingReviewSummary]:
        """Offline scanner input: pending past fanout_after with stale delegate."""
        threshold = now - timedelta(seconds=heartbeat_window_seconds)
        async with self.session() as s:
            rows = (
                await s.execute(
                    select(
                        AiReview.public_id,
                        AiReview.selected_delegate_public_id,
                        AiReview.wallet_public_id,
                        AiReview.dispatch_version,
                        AiReview.status,
                        AiReview.deadline,
                        AiReview.fanout_after,
                    )
                    .join(
                        AiDelegate,
                        AiDelegate.public_id == AiReview.selected_delegate_public_id,
                    )
                    .where(
                        AiReview.status == "pending",
                        AiReview.fanout_after < now,
                        or_(
                            AiDelegate.last_seen_at.is_(None),
                            AiDelegate.last_seen_at < threshold,
                        ),
                    )
                    .order_by(AiReview.fanout_after.asc())
                    .limit(limit)
                )
            ).all()
            return [
                cast(
                    PendingReviewSummary,
                    {
                        "public_id": r[0],
                        "selected_delegate_public_id": r[1],
                        "wallet_public_id": str(r[2]),
                        "dispatch_version": int(r[3]),
                        "status": str(r[4]),
                        "deadline": r[5],
                        "fanout_after": r[6],
                    },
                )
                for r in rows
            ]

    async def list_pending_reviews_for_delegate(
        self,
        *,
        selected_delegate_public_id: str,
        now: datetime,
        wallet_public_id: str | None = None,
        limit: int = 100,
    ) -> list[PendingReviewSummary]:
        """Fast-path input: pending rows for one delegate past fanout.

        Joins ``Instrument`` + ``Symbol`` so the returned rows carry the
        resolved ``instrument`` ticker and the raw ``signal_envelope``
        payload (``thesis`` + signal metadata), giving the inbox enough
        context to render a row without a follow-up read.
        """
        predicates = [
            AiReview.selected_delegate_public_id == selected_delegate_public_id,
            AiReview.status == "pending",
            AiReview.fanout_after < now,
        ]
        if wallet_public_id is not None:
            predicates.append(AiReview.wallet_public_id == wallet_public_id)
        async with self.session() as s:
            rows = (
                await s.execute(
                    select(
                        AiReview.public_id,
                        AiReview.selected_delegate_public_id,
                        AiReview.wallet_public_id,
                        AiReview.dispatch_version,
                        AiReview.status,
                        AiReview.deadline,
                        AiReview.fanout_after,
                        Symbol.native_symbol,
                        AiReview.signal_envelope,
                    )
                    .outerjoin(
                        Instrument,
                        and_(
                            AiReview.instrument_public_id == Instrument.public_id,
                            *where_active(Instrument, now),
                        ),
                    )
                    .outerjoin(
                        Symbol,
                        and_(
                            Instrument.symbol_public_id == Symbol.public_id,
                            *where_active(Symbol, now),
                        ),
                    )
                    .where(*predicates)
                    .order_by(AiReview.fanout_after.asc())
                    .limit(limit)
                )
            ).all()
            return [
                cast(
                    PendingReviewSummary,
                    {
                        "public_id": r[0],
                        "selected_delegate_public_id": r[1],
                        "wallet_public_id": str(r[2]),
                        "dispatch_version": int(r[3]),
                        "status": str(r[4]),
                        "deadline": r[5],
                        "fanout_after": r[6],
                        "instrument": r[7],
                        "signal_envelope": r[8],
                    },
                )
                for r in rows
            ]

    async def _select_for_update_pre_state(
        self,
        s: AsyncSession,
        review_public_id: str,
        *,
        with_deadline: bool,
    ) -> tuple[str, datetime | None, int, str] | None:
        """Helper for the combined terminal-transition primitives.

        SELECT-FOR-UPDATE on the ``ai_reviews`` row to capture
        ``previous_status`` (+ ``deadline`` when the resolve path needs
        the gate) + ``dispatch_version`` + ``selected_delegate_public_id``.
        PG holds the row lock for the rest of the open transaction so
        concurrent peers serialise; the SQLite fallback degrades to a
        plain SELECT, but the callers bind the subsequent UPDATE to
        ``status == previous_status`` so a peer that flips the row in
        the read-then-CAS gap on engines without row locking simply
        loses the rowcount=0 race + the primitive rolls back rather
        than recording a stale predecessor. Returns ``None`` when
        the row does not exist OR is already terminal so callers can
        short-circuit without the UPDATE.

        ``selected_delegate_public_id`` is captured here so the resolve
        primitive can derive ``resolution_mode`` inside the locked
        transaction (no second SELECT after the UPDATE is needed).
        """
        cols = (
            (
                AiReview.status,
                AiReview.deadline,
                AiReview.dispatch_version,
                AiReview.selected_delegate_public_id,
            )
            if with_deadline
            else (
                AiReview.status,
                AiReview.dispatch_version,
                AiReview.selected_delegate_public_id,
            )
        )
        select_stmt = select(*cols).where(AiReview.public_id == review_public_id)
        try:
            pre_row = (await s.execute(select_stmt.with_for_update())).first()
        except NotImplementedError:
            pre_row = (await s.execute(select_stmt)).first()
        if pre_row is None:
            return None
        if with_deadline:
            previous_status, deadline, dispatch_version, selected = pre_row
        else:
            previous_status, dispatch_version, selected = pre_row
            deadline = None
        if previous_status not in ("pending", "fanout_dispatched"):
            return None
        return str(previous_status), deadline, int(dispatch_version), str(selected)

    async def _decrement_delegate_counter_in_session(
        self,
        s: AsyncSession,
        *,
        review_public_id: str,
        selected_delegate_public_id: str,
        now: datetime,
    ) -> bool:
        """Helper for the combined terminal-transition primitives.

        Reuses the ``counter_decremented_at`` CAS pattern from
        :meth:`decrement_delegate_active_count_for_review` but operates
        on the open session instead of opening its own transaction so
        the entire terminal-transition + audit-event + counter chain
        commits or rolls back atomically. Returns ``True`` when this
        caller won the claim; ``False`` when a peer (concurrent
        decision / reaper / supersede) had already decremented for the
        same review.
        """
        claim_stmt = (
            update(AiReview)
            .where(
                AiReview.public_id == review_public_id,
                AiReview.counter_decremented_at.is_(None),
            )
            .values(counter_decremented_at=now, updated_at=now)
        )
        claim_result = await s.execute(claim_stmt)
        if int(cast(Any, claim_result).rowcount or 0) == 0:
            return False
        decrement_stmt = (
            update(AiDelegate)
            .where(AiDelegate.public_id == selected_delegate_public_id)
            .values(
                active_reviews_count=case(
                    (AiDelegate.active_reviews_count > 0, AiDelegate.active_reviews_count - 1),
                    else_=0,
                ),
                updated_at=now,
            )
        )
        await s.execute(decrement_stmt)
        return True

    async def atomic_resolve_review_with_audit_and_counter(
        self,
        *,
        review_public_id: str,
        decision: str,
        responding_delegate_public_id: str,
        rationale: str | None,
        new_status: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> AtomicResolveResult | None:
        """Single-transaction resolve + audit + counter decrement.

        ``resolution_mode`` is derived INSIDE the primitive from the
        SELECT-FOR-UPDATE-captured ``previous_status`` +
        ``selected_delegate_public_id`` so the row UPDATE, audit-event
        row, and the value returned to the service all match the
        actually-locked transition (the service must not compute
        ``resolution_mode`` from a pre-snapshot ``status`` that could
        disagree with the lock-time predecessor if a peer flipped
        pending -> fanout_dispatched in the gap).
        The UPDATE is bound to ``status == previous_status`` (the
        captured value) so even on engines without row locking
        (SQLite plain-SELECT fallback), a peer transition between the
        SELECT and the UPDATE drops rowcount to 0 + the primitive
        rolls back rather than recording a stale predecessor.
        """
        async with self.session() as s:
            pre = await self._select_for_update_pre_state(s, review_public_id, with_deadline=True)
            if pre is None:
                return None
            previous_status, deadline, dispatch_version, selected = pre
            if deadline is None or deadline <= now:
                return None
            resolution_mode = _derive_resolve_resolution_mode(
                previous_status=previous_status,
                selected_delegate_public_id=selected,
                responding_delegate_public_id=responding_delegate_public_id,
            )
            update_stmt = (
                update(AiReview)
                .where(
                    AiReview.public_id == review_public_id,
                    AiReview.status == previous_status,
                    AiReview.deadline > now,
                )
                .values(
                    status=new_status,
                    decision=decision,
                    responding_delegate_public_id=responding_delegate_public_id,
                    rationale=rationale,
                    resolution_mode=resolution_mode,
                    resolved_at=now,
                    updated_at=now,
                )
            )
            result = await s.execute(update_stmt)
            if int(cast(Any, result).rowcount or 0) == 0:
                await s.rollback()
                return None
            audit_payload = dict(audit_event)
            audit_payload["previous_status"] = previous_status
            s.add(AiReviewEvent(**audit_payload))
            await self._decrement_delegate_counter_in_session(
                s,
                review_public_id=review_public_id,
                selected_delegate_public_id=selected,
                now=now,
            )
            await s.commit()
            return AtomicResolveResult(
                selected_delegate_public_id=selected,
                dispatch_version=dispatch_version,
                previous_status=previous_status,
                resolution_mode=resolution_mode,
            )

    async def atomic_timeout_review_with_audit_and_counter(
        self,
        *,
        review_public_id: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> AtomicResolveResult | None:
        """Single-transaction timeout + audit + counter decrement.

        UPDATE is bound to ``status == previous_status`` (the captured
        value) so a peer transition in the SELECT-then-CAS gap on
        engines without row locking causes rowcount=0 + rollback
        rather than overwriting a row whose actual predecessor differs
        from the audit-event ``previous_status`` we are about to
        record.
        """
        async with self.session() as s:
            pre = await self._select_for_update_pre_state(s, review_public_id, with_deadline=False)
            if pre is None:
                return None
            previous_status, _deadline, dispatch_version, selected = pre
            update_stmt = (
                update(AiReview)
                .where(
                    AiReview.public_id == review_public_id,
                    AiReview.status == previous_status,
                )
                .values(
                    status="timeout",
                    resolution_mode="timeout_no_response",
                    resolved_at=now,
                    updated_at=now,
                )
            )
            result = await s.execute(update_stmt)
            if int(cast(Any, result).rowcount or 0) == 0:
                await s.rollback()
                return None
            audit_payload = dict(audit_event)
            audit_payload["previous_status"] = previous_status
            s.add(AiReviewEvent(**audit_payload))
            await self._decrement_delegate_counter_in_session(
                s,
                review_public_id=review_public_id,
                selected_delegate_public_id=selected,
                now=now,
            )
            await s.commit()
            return AtomicResolveResult(
                selected_delegate_public_id=selected,
                dispatch_version=dispatch_version,
                previous_status=previous_status,
            )

    async def atomic_supersede_review_with_audit_and_counter(
        self,
        *,
        review_public_id: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> AtomicResolveResult | None:
        """Single-transaction supersede + audit + counter decrement.

        UPDATE is bound to ``status == previous_status`` (the captured
        value) for the same predecessor-race reason as
        :meth:`atomic_timeout_review_with_audit_and_counter`.
        """
        async with self.session() as s:
            pre = await self._select_for_update_pre_state(s, review_public_id, with_deadline=False)
            if pre is None:
                return None
            previous_status, _deadline, dispatch_version, selected = pre
            update_stmt = (
                update(AiReview)
                .where(
                    AiReview.public_id == review_public_id,
                    AiReview.status == previous_status,
                )
                .values(
                    status="superseded",
                    resolution_mode="superseded_by_strategy",
                    resolved_at=now,
                    updated_at=now,
                )
            )
            result = await s.execute(update_stmt)
            if int(cast(Any, result).rowcount or 0) == 0:
                await s.rollback()
                return None
            audit_payload = dict(audit_event)
            audit_payload["previous_status"] = previous_status
            s.add(AiReviewEvent(**audit_payload))
            await self._decrement_delegate_counter_in_session(
                s,
                review_public_id=review_public_id,
                selected_delegate_public_id=selected,
                now=now,
            )
            await s.commit()
            return AtomicResolveResult(
                selected_delegate_public_id=selected,
                dispatch_version=dispatch_version,
                previous_status=previous_status,
            )

    async def atomic_dispatch_fanout_with_audit(
        self,
        *,
        review_public_id: str,
        audit_event: AiReviewEventInsertRow,
        now: datetime,
    ) -> int | None:
        """Single-transaction fanout dispatch + audit insert."""
        async with self.session() as s:
            update_stmt = (
                update(AiReview)
                .where(
                    AiReview.public_id == review_public_id,
                    AiReview.status == "pending",
                )
                .values(
                    status="fanout_dispatched",
                    dispatch_version=AiReview.dispatch_version + 1,
                    updated_at=now,
                )
            )
            result = await s.execute(update_stmt)
            if int(cast(Any, result).rowcount or 0) == 0:
                await s.rollback()
                return None
            new_version = int(
                (
                    await s.execute(
                        select(AiReview.dispatch_version).where(
                            AiReview.public_id == review_public_id
                        )
                    )
                ).scalar_one()
            )
            audit_payload = dict(audit_event)
            payload_field: dict[str, Any] = dict(
                cast(dict[str, Any], audit_payload.get("payload") or {})
            )
            payload_field["dispatch_version"] = new_version
            audit_payload["payload"] = payload_field
            s.add(AiReviewEvent(**audit_payload))
            await s.commit()
            return new_version

    async def _count_total_estimate(self, s: AsyncSession, model: type[Any]) -> int:
        """Dialect-aware fast row count for monitoring.

        PostgreSQL uses planner statistics from ``pg_class`` joined with
        ``pg_namespace`` for schema safety — this is microseconds vs
        minutes for ``count(*)`` on a multi-GB table. Accuracy is
        bounded by ``ANALYZE`` freshness (typically within a few percent;
        worse during high-velocity write bursts before autovacuum
        catches up). Adequate for monitoring growth trends.

        SQLite uses an exact ``count(*)`` — at dev scale (<10M rows)
        this is bounded and matches developer expectations. The guard
        threshold ``_SQLITE_COUNT_GUARD_ROWS`` logs a warning if a dev
        fixture exceeds the bound; the surrounding snapshotter's
        ``PER_TABLE_TIMEOUT_SECONDS`` still caps runtime.

        Raises:
            NotImplementedError: for dialects other than PostgreSQL
                and SQLite (none currently supported by Snapper).
        """
        dialect = self.dialect_name
        table_name = model.__tablename__
        if dialect == "postgresql":
            schema_name = model.__table__.schema or "public"
            stmt = text(
                "SELECT GREATEST(c.reltuples::bigint, 0) "
                "FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relname = :table "
                "AND n.nspname = :schema "
                "AND c.relkind = 'r'"
            )
            result = await s.execute(stmt, {"table": table_name, "schema": schema_name})
            scalar = result.scalar_one_or_none()
            return int(scalar) if scalar is not None else 0
        if dialect == "sqlite":
            count_stmt = select(func.count()).select_from(model)
            count = int((await s.execute(count_stmt)).scalar_one())
            if count > _SQLITE_COUNT_GUARD_ROWS:
                logger.warning(
                    f"SQLite count(*) on {table_name} returned {count} rows "
                    f"(above {_SQLITE_COUNT_GUARD_ROWS} guard threshold); "
                    "consider switching to PostgreSQL for this dataset size."
                )
            return count
        raise NotImplementedError(f"_count_total_estimate not implemented for dialect={dialect}")

    async def count_table_stats(
        self,
        entry: TableEntry,
        *,
        archivable_window: tuple[date, date] | None = None,
    ) -> TableCounters:
        """Per-table four-counter primitive (event + state).

        The ``total`` count uses :meth:`_count_total_estimate` which is
        dialect-aware: PostgreSQL returns a planner estimate (fast,
        accurate within autovacuum drift), SQLite returns an exact
        count. ``closed`` is derived as ``max(0, total - current)`` on
        SCD2/state tables to skip a slow full-table scan; the clamp
        handles stale PG estimates where ``current`` (exact, partial
        index) temporarily exceeds the estimated ``total``.
        """
        model = cast(Any, entry.model)
        async with self.session() as s:
            archivable_predicate = _archivable_window_predicate(entry, archivable_window)
            if entry.kind == "event":
                total = await self._count_total_estimate(s, model)
                archivable: int | None = None
                if archivable_predicate is not None:
                    archivable_stmt = (
                        select(func.count()).select_from(model).where(archivable_predicate)
                    )
                    archivable = int((await s.execute(archivable_stmt)).scalar_one())
                return TableCounters(total=total, current=None, closed=None, archivable=archivable)
            current_stmt = (
                select(func.count()).select_from(model).where(model.known_to == KNOWN_TO_MAX)
            )
            current = int((await s.execute(current_stmt)).scalar_one())
            total = await self._count_total_estimate(s, model)
            closed = max(0, total - current)
            archivable = None
            if archivable_predicate is not None:
                archivable_stmt = (
                    select(func.count())
                    .select_from(model)
                    .where(model.known_to != KNOWN_TO_MAX, archivable_predicate)
                )
                archivable = int((await s.execute(archivable_stmt)).scalar_one())
            return TableCounters(total=total, current=current, closed=closed, archivable=archivable)

    async def get_market_data_coverage(
        self,
        *,
        tick_window_seconds: int,
        candle_window_seconds: int,
        now: datetime | None = None,
    ) -> list[MarketDataCoverageRow]:
        """Per-exchange market-data coverage over active instruments.

        Replicates the validated prod reference query's *semantics* with
        portable SQLAlchemy Core constructs so the same statement runs on
        both the SQLite test fixture and PostgreSQL. The Postgres-only
        ``interval`` / ``FILTER`` syntax is deliberately avoided:

        * A single ``now`` (the ``now`` arg, else ``datetime.now(UTC)``)
          drives every freshness cutoff AND the bitemporal active-row
          predicates, so one call evaluates everything at one instant.
        * Cutoffs are computed in Python and compared with ``>``.
        * The query is **instrument-driven and fanout-free**: it counts
          only ``Instrument`` rows (active via :func:`where_active`), and
          expresses freshness AND capability-gating as correlated
          ``EXISTS`` subqueries. A capability ``LEFT JOIN`` would inflate
          every tally because ``where_active`` (``known_to > now``) is
          broader than the capability table's sentinel-only partial unique
          index, so overlapping active capability rows could each join the
          same instrument.
        * Per-exchange tallies use ``func.sum(case((cond, 1), else_=0))``
          (portable) instead of ``COUNT(*) FILTER``.

        ``gated_off`` counts instruments with an active capability row whose
        ``can_market_data`` is FALSE. ``dark`` counts instruments that are
        NOT gated off AND have no fresh ticks — the "should be live but
        isn't" gap; an instrument with no capability row is not gated off
        (so it can be dark), matching the reference LEFT JOIN semantics.

        Args:
            tick_window_seconds: Freshness window for ``ticks`` rows; a
                tick newer than ``now - tick_window_seconds`` is fresh.
            candle_window_seconds: Freshness window for ``candles`` rows
                (compared against ``open_at``).
            now: Reference instant for all freshness + active-row
                predicates; defaults to ``datetime.now(UTC)``. Injectable
                so tests can assert exact cutoff boundaries deterministically.

        Returns:
            One :class:`MarketDataCoverageRow` per exchange, ordered by
            exchange.
        """
        reference = now if now is not None else datetime.now(UTC)
        tick_cutoff = reference - timedelta(seconds=tick_window_seconds)
        candle_cutoff = reference - timedelta(seconds=candle_window_seconds)
        cap = aliased(SymbolExchangeCapability)
        fresh_ticks_exists = (
            select(Tick.id)
            .where(
                Tick.instrument_public_id == Instrument.public_id,
                Tick.timestamp > tick_cutoff,
            )
            .exists()
        )
        fresh_candles_exists = (
            select(Candle.id)
            .where(
                Candle.instrument_public_id == Instrument.public_id,
                Candle.open_at > candle_cutoff,
            )
            .exists()
        )
        gated_off_exists = (
            select(cap.id)
            .where(
                cap.symbol_public_id == Instrument.symbol_public_id,
                cap.exchange == Instrument.exchange,
                cap.can_market_data.is_(False),
                *where_active(cap, reference),
            )
            .exists()
        )
        statement = (
            select(
                Instrument.exchange.label("exchange"),
                func.count().label("instruments"),
                func.sum(case((fresh_ticks_exists, 1), else_=0)).label("fresh_ticks"),
                func.sum(case((fresh_candles_exists, 1), else_=0)).label("fresh_candles"),
                func.sum(case((gated_off_exists, 1), else_=0)).label("gated_off"),
                func.sum(case((and_(~gated_off_exists, ~fresh_ticks_exists), 1), else_=0)).label(
                    "dark"
                ),
            )
            .select_from(Instrument)
            .where(*where_active(Instrument, reference))
            .group_by(Instrument.exchange)
            .order_by(Instrument.exchange)
        )
        async with self.session() as s:
            result = await s.execute(statement)
            rows: list[MarketDataCoverageRow] = []
            for row in result.all():
                rows.append(
                    MarketDataCoverageRow(
                        exchange=row.exchange,
                        instruments=int(row.instruments),
                        fresh_ticks=int(row.fresh_ticks or 0),
                        fresh_candles=int(row.fresh_candles or 0),
                        gated_off=int(row.gated_off or 0),
                        dark=int(row.dark or 0),
                    )
                )
            return rows

    async def upsert_instrument_feed_health(
        self, rows: list[InstrumentFeedHealthUpsertRow]
    ) -> None:
        """Upsert current-state feed-health rows (last-write-wins).

        Cross-dialect by construction: both the SQLite (tests) and
        PostgreSQL (prod) ``insert`` dialect helpers expose
        ``on_conflict_do_update`` keyed on the
        ``(coordinator, exchange, channel, symbol)`` unique constraint,
        so the same statement form runs on both backends without any
        Postgres-only SQL. Every non-key column is overwritten with the
        incoming snapshot value on conflict, giving last-write-wins
        semantics for this current-state table. Rows are written in bounded
        chunks within a single transaction so a large snapshot (a wildcard
        publisher across many channels) never exceeds a dialect's bound
        parameter limit (SQLite's default 999) and fails the whole flush.

        Args:
            rows: Feed-health snapshot rows to persist. Empty list is a
                no-op.

        Returns:
            None.
        """
        if not rows:
            return
        values = [dict(row) for row in rows]
        conflict_cols = ["coordinator", "exchange", "channel", "symbol"]
        update_cols = [
            "status",
            "requested_at",
            "confirmed_at",
            "last_seen_data_at",
            "last_error",
            "retry_count",
            "snapshot_at",
        ]
        name = self.dialect_name
        chunk_size = 80
        async with self.session() as s:
            for start in range(0, len(values), chunk_size):
                batch = values[start : start + chunk_size]
                stmt = (
                    sqlite_insert(InstrumentFeedHealth)
                    if name == "sqlite"
                    else pg_insert(InstrumentFeedHealth)
                )
                stmt = stmt.values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_cols,
                    set_={col: getattr(stmt.excluded, col) for col in update_cols},
                )
                await s.execute(stmt)
            await s.commit()

    async def list_instrument_feed_health(
        self, *, exchange: str | None = None, fresh_within_seconds: int | None = None
    ) -> list[InstrumentFeedHealthRow]:
        """List current-state feed-health rows, ordered for display.

        Rows are upserted per natural key, so a symbol a publisher stops
        flushing (it was dropped from the universe, or the whole publisher
        died) keeps its last row with a frozen ``snapshot_at``. Pass
        ``fresh_within_seconds`` to exclude such stale rows and return only
        what is genuinely current; every row also carries ``snapshot_at``
        so a caller that wants everything can judge staleness itself.

        Args:
            exchange: Optional exchange filter (lowercase). When ``None``
                every exchange's rows are returned.
            fresh_within_seconds: When set, return only rows whose
                ``snapshot_at`` is within this many seconds of now (drops
                rows from stopped/shrunk publishers). ``None`` returns all.

        Returns:
            One :class:`InstrumentFeedHealthRow` per natural key, ordered
            by ``(exchange, channel, symbol)``.
        """
        statement = select(InstrumentFeedHealth)
        if exchange is not None:
            statement = statement.where(InstrumentFeedHealth.exchange == exchange)
        if fresh_within_seconds is not None:
            cutoff = datetime.now(UTC) - timedelta(seconds=fresh_within_seconds)
            statement = statement.where(InstrumentFeedHealth.snapshot_at >= cutoff)
        statement = statement.order_by(
            InstrumentFeedHealth.exchange,
            InstrumentFeedHealth.channel,
            InstrumentFeedHealth.symbol,
        )
        async with self.session() as s:
            result = await s.execute(statement)
            return [
                InstrumentFeedHealthRow(
                    coordinator=entity.coordinator,
                    exchange=entity.exchange,
                    channel=entity.channel,
                    symbol=entity.symbol,
                    status=entity.status,
                    requested_at=entity.requested_at,
                    confirmed_at=entity.confirmed_at,
                    last_seen_data_at=entity.last_seen_data_at,
                    last_error=entity.last_error,
                    retry_count=entity.retry_count,
                    snapshot_at=entity.snapshot_at,
                )
                for entity in result.scalars().all()
            ]


def _archivable_window_predicate(
    entry: TableEntry,
    archivable_window: tuple[date, date] | None,
) -> Any | None:
    """Build the half-open ``timestamp`` predicate for ``archivable_window``.

    Alignment contract: rows AT
    ``datetime(day_start, 0, 0, UTC)`` are INCLUDED; rows AT
    ``datetime(day_end + 1d, 0, 0, UTC)`` are EXCLUDED. Mirrors the
    archive-query bounds shape (``repository.py:get_event_rows_for_archive``
    timestamp-range filter).
    """
    if archivable_window is None:
        return None
    model = cast(Any, entry.model)
    day_start, day_end = archivable_window
    window_start = datetime.combine(day_start, datetime.min.time(), tzinfo=UTC)
    window_end = datetime.combine(day_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
    return and_(
        model.timestamp >= window_start,
        model.timestamp < window_end,
    )


_repository_cache: dict[str, Repository] = {}
_live_sqlalchemy_repositories: weakref.WeakSet[object] = weakref.WeakSet()
_live_sqlalchemy_engines: weakref.WeakSet[AsyncEngine] = weakref.WeakSet()
_live_aiosqlite_connections: dict[int, _ClosableConnection] = {}


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
    for engine in _collect_engines_to_dispose():
        await _dispose_engine_safely(engine)
    for connection in tuple(_live_aiosqlite_connections.values()):
        await _close_aiosqlite_connection_safely(connection)
    _live_aiosqlite_connections.clear()
    _repository_cache.clear()


def _collect_repositories_to_dispose() -> list[object]:
    """Collect cached and still-live repository instances without duplicates."""
    repos_to_dispose: dict[int, object] = {
        id(cached_repo): cached_repo for cached_repo in _repository_cache.values()
    }
    for live_repo in _live_sqlalchemy_repositories:
        repos_to_dispose[id(live_repo)] = live_repo
    return list(repos_to_dispose.values())


def _collect_engines_to_dispose() -> list[object]:
    """Collect repository engines and tracked live engines without duplicates."""
    engines_to_dispose: dict[int, object] = {}
    for repo in _collect_repositories_to_dispose():
        engine = getattr(repo, "engine", None)
        if engine is not None:
            engines_to_dispose[id(engine)] = engine
    for live_engine in _live_sqlalchemy_engines:
        engines_to_dispose[id(live_engine)] = live_engine
    return list(engines_to_dispose.values())


async def _dispose_engine_safely(engine: object) -> None:
    """Dispose one engine-like object and log failures without raising."""
    disposable_engine = _resolve_disposable_engine(engine)
    if disposable_engine is None:
        logger.warning("Failed to dispose repository engine: engine has no callable dispose()")
        return
    try:
        dispose_result = disposable_engine.dispose()
        if isawaitable(dispose_result):
            await dispose_result
    except Exception as exc:
        logger.warning(f"Failed to dispose repository engine: {exc}")


async def _close_aiosqlite_connection_safely(connection: _ClosableConnection) -> None:
    """Close one tracked aiosqlite connection and log failures without raising."""
    try:
        close_result = connection.close()
        if isawaitable(close_result):
            await close_result
    except Exception as exc:
        logger.warning(f"Failed to close tracked aiosqlite connection: {exc}")


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
        connect_args: dict[str, Any] = {}
        if "postgresql" in self.db_url:
            connect_args["options"] = "-c timezone=UTC"
        self.engine: SyncEngine = create_sync_engine(
            self.db_url, future=True, connect_args=connect_args
        )
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
