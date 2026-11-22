"""Session-scoped locking and evidence reads for execution-plan settlement.

Provides transaction bootstrap, deterministic `FOR UPDATE` row locking over
child `TradeCommand` and parent `ExecutionPlan` rows, and a session-bound
implementation of the settlement read contract so plan settlement re-verifies
durable child command status and venue lifecycle/fill evidence inside the same
database transaction that closes and inserts `ExecutionPlan` versions.

Note on residual concurrency window:
    Append-only `venue_events` writers (`insert_venue_event` and
    `insert_order_terminal_once`) do not lock `trade_commands` rows. Locking
    active `TradeCommand` and `ExecutionPlan` rows with `FOR UPDATE` under
    `READ COMMITTED` (and `BEGIN IMMEDIATE` on SQLite) fences all concurrent
    `trade_commands` lifecycle transitions and `execution_plans` mutations, and
    sees every `venue_events` row committed before the post-lock evidence
    `SELECT`s execute, while leaving the live executor's `venue_events` hot
    path untouched. An `order_accepted` or `fill_observed` appended to
    `venue_events` after the post-lock evidence `SELECT` and before `COMMIT`
    (before reconciliation advances `trade_commands`) remains outside the
    `TradeCommand` row lock; operator CLI `apply` eliminates live arrivals by
    running with the plan executor stopped or restarting it immediately on an
    idle shard.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from typing import Final

from sqlalchemy import ColumnElement
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.types import ExecutionPlanStatusEnum
from snapper.data.models import ExecutionPlan
from snapper.data.models import Order
from snapper.data.models import TemporalMixin
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.models import is_open_ended_version
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import where_active
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow

_CREATE_COMMAND_TYPES: Final[tuple[str, ...]] = ("create", "submit")
_ACTIONABLE_PLAN_STATUSES: Final[tuple[str, ...]] = (
    ExecutionPlanStatusEnum.PENDING,
    ExecutionPlanStatusEnum.ARMED,
    ExecutionPlanStatusEnum.ACTIVE,
    ExecutionPlanStatusEnum.CANCEL_REQUESTED,
)
_ORDER_LIFECYCLE_EVENT_TYPES: Final[tuple[str, ...]] = (
    "order_accepted",
    "order_rejected",
    "order_terminal",
    "fill_observed",
    "order_submit_unknown",
    "order_breaker_open",
    "order_interlock_blocked",
)
_LIFECYCLE_LOOKUP_CHUNK_SIZE: Final[int] = 300


def filter_open_versions[TemporalRowT: TemporalMixin](
    rows: Sequence[TemporalRowT],
) -> list[TemporalRowT]:
    """Filter loaded temporal ORM rows down to exact open-ended versions.

    Args:
        rows: Candidate ORM rows loaded from a query.

    Returns:
        Subset whose `known_to` equals `KNOWN_TO_MAX`.
    """
    return [row for row in rows if is_open_ended_version(row.known_to)]


async def begin_plan_settlement_transaction(
    session: AsyncSession,
    *,
    dialect_name: str,
) -> None:
    """Open write serialization or explicit `READ COMMITTED` on a fresh session.

    Must execute as the first statement on `session`. SQLite acquires the
    database write lock upfront via `BEGIN IMMEDIATE`; PostgreSQL pins
    `READ COMMITTED` explicitly so post-lock `SELECT` statements observe rows
    committed while waiting on `FOR UPDATE` locks.

    Args:
        session: Fresh SQLAlchemy async session before any query runs.
        dialect_name: Lowercase SQLAlchemy dialect name (`sqlite` or `postgresql`).

    Raises:
        NotImplementedError: When `dialect_name` is neither `sqlite` nor `postgresql`.
    """
    if dialect_name == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))
    elif dialect_name == "postgresql":
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
    else:
        raise NotImplementedError(
            f"plan settlement transaction not implemented for dialect={dialect_name}"
        )


async def lock_child_create_commands(
    session: AsyncSession,
    *,
    child_orders: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], TradeCommand]:
    """Lock open-ended child `create`/`submit` commands in deterministic `id` order.

    Args:
        session: Open settlement transaction session.
        child_orders: `(exchange, client_order_id)` pairs to lock.

    Returns:
        Mapping of `(exchange, client_order_id)` to locked `TradeCommand` ORM rows.

    Raises:
        RuntimeError: If multiple open-ended create/submit rows match one order.
    """
    unique_pairs = list(dict.fromkeys(child_orders))
    if not unique_pairs:
        return {}
    now = datetime.now(UTC)
    result = await session.execute(
        select(TradeCommand)
        .where(
            tuple_(TradeCommand.exchange, TradeCommand.client_order_id).in_(unique_pairs),
            TradeCommand.command_type.in_(_CREATE_COMMAND_TYPES),
            TradeCommand.known_to > now,
        )
        .order_by(TradeCommand.id)
        .with_for_update()
    )
    locked: dict[tuple[str, str], TradeCommand] = {}
    for row in filter_open_versions(result.scalars().all()):
        key = (str(row.exchange), str(row.client_order_id))
        if key in locked:
            raise RuntimeError(f"Multiple active create/submit commands for {key[0]}:{key[1]}")
        locked[key] = row
    return locked


async def lock_active_execution_plans(
    session: AsyncSession,
    *,
    plan_public_ids: Sequence[str],
) -> dict[str, ExecutionPlan]:
    """Lock open-ended `ExecutionPlan` rows in deterministic physical `id` order.

    Args:
        session: Open settlement transaction session.
        plan_public_ids: Execution plan `public_id` values to lock.

    Returns:
        Mapping of `public_id` to locked open-ended `ExecutionPlan` ORM rows.

    Raises:
        RuntimeError: If multiple open-ended versions exist for one `public_id`.
    """
    unique_ids = list(dict.fromkeys(plan_public_ids))
    if not unique_ids:
        return {}
    now = datetime.now(UTC)
    result = await session.execute(
        select(ExecutionPlan)
        .where(
            ExecutionPlan.public_id.in_(unique_ids),
            ExecutionPlan.known_to > now,
        )
        .order_by(ExecutionPlan.id)
        .with_for_update()
    )
    locked: dict[str, ExecutionPlan] = {}
    for row in filter_open_versions(result.scalars().all()):
        pid = str(row.public_id)
        if pid in locked:
            raise RuntimeError(f"Multiple active execution plan versions for {pid}")
        locked[pid] = row
    return locked


class SessionPlanSettlementReader:
    """Session-bound settlement evidence reader executing inside one transaction.

    All plan, child-command, and venue-event queries run on the caller's open
    `AsyncSession` rather than opening independent autocommit sessions, and
    every child-command lookup acquires `FOR UPDATE` on matching open-ended
    `TradeCommand` rows.
    """

    def __init__(
        self,
        repository: SQLAlchemyRepository,
        session: AsyncSession,
    ) -> None:
        """Bind a settlement reader to an open repository session.

        Args:
            repository: Owning `SQLAlchemyRepository` providing row mappers.
            session: Open transaction session used for all queries.
        """
        self._repository = repository
        self._session = session

    async def get_active_execution_plans(self) -> list[ExecutionPlanRow]:
        """Return all open-ended actionable execution plans on the bound session.

        Returns:
            Actionable execution plan rows ordered by creation time.
        """
        now = datetime.now(UTC)
        result = await self._session.execute(
            select(ExecutionPlan)
            .where(
                ExecutionPlan.status.in_(_ACTIONABLE_PLAN_STATUSES),
                ExecutionPlan.known_to > now,
            )
            .order_by(ExecutionPlan.created_at, ExecutionPlan.public_id)
        )
        return [
            self._repository._plan_row_to_dict(row)
            for row in filter_open_versions(result.scalars().all())
        ]

    async def get_active_create_command_by_client_order_id(
        self,
        client_order_id: str,
        exchange: str,
    ) -> TradeCommandRow | None:
        """Lock and return the open-ended create/submit command for a child order.

        Args:
            client_order_id: Child client order id recorded on the plan.
            exchange: Exchange name scoping the lookup.

        Returns:
            Locked active command row, or None when absent.

        Raises:
            RuntimeError: When multiple open-ended create/submit rows match.
        """
        now = datetime.now(UTC)
        result = await self._session.execute(
            select(TradeCommand)
            .where(
                TradeCommand.client_order_id == client_order_id,
                TradeCommand.exchange == exchange,
                TradeCommand.command_type.in_(_CREATE_COMMAND_TYPES),
                TradeCommand.known_to > now,
            )
            .order_by(TradeCommand.id)
            .with_for_update()
        )
        rows = filter_open_versions(result.scalars().all())
        if not rows:
            return None
        if len(rows) > 1:
            raise RuntimeError(
                f"Multiple active create/submit commands for {exchange}:{client_order_id}"
            )
        return self._repository._trade_command_to_row(rows[0])

    async def get_exchange_order_id_for_client_order_id(
        self,
        client_order_id: str,
        as_of: datetime,
    ) -> str | None:
        """Return the active Order venue id using the settlement transaction.

        Args:
            client_order_id: Child client order id recorded on the plan.
            as_of: Temporal point for active-version selection.

        Returns:
            Venue order id, or None when no acknowledged Order version exists.
        """
        result = await self._session.execute(
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
        return None if row is None else row[0]

    async def get_order_lifecycle_events(
        self,
        client_order_ids: list[str],
    ) -> list[VenueEventRow]:
        """Return ordered lifecycle venue events for `client_order_ids` on `session`.

        Args:
            client_order_ids: Client order ids to inspect.

        Returns:
            Lifecycle venue events ordered ascending by `id`.
        """
        if not client_order_ids:
            return []
        rows: list[VenueEventRow] = []
        for start in range(0, len(client_order_ids), _LIFECYCLE_LOOKUP_CHUNK_SIZE):
            chunk = client_order_ids[start : start + _LIFECYCLE_LOOKUP_CHUNK_SIZE]
            result = await self._session.execute(
                select(VenueEvent).where(
                    VenueEvent.client_order_id.in_(chunk),
                    VenueEvent.event_type.in_(_ORDER_LIFECYCLE_EVENT_TYPES),
                )
            )
            rows.extend(
                self._repository._venue_event_to_row(event) for event in result.scalars().all()
            )
        rows.sort(key=lambda item: item["id"])
        return rows

    async def get_max_cumulative_fill_venue_event(
        self,
        client_order_id: str,
    ) -> VenueEventRow | None:
        """Return the highest cumulative `fill_observed` event on `session`.

        Args:
            client_order_id: Client order id to inspect.

        Returns:
            Highest cumulative fill venue event row, or None when none exist.
        """
        result = await self._session.execute(
            select(VenueEvent)
            .where(
                VenueEvent.client_order_id == client_order_id,
                VenueEvent.event_type == "fill_observed",
                VenueEvent.cum_fill_size.isnot(None),
            )
            .order_by(VenueEvent.cum_fill_size.desc(), VenueEvent.id.desc())
            .limit(1)
        )
        event = result.scalars().first()
        if event is None:
            return None
        return self._repository._venue_event_to_row(event)

    async def get_latest_venue_event_received_at(
        self,
        client_order_id: str,
        event_types: Sequence[str],
        statuses: Sequence[str] | None = None,
    ) -> datetime | None:
        """Return the newest qualifying `received_at` on `session`.

        Args:
            client_order_id: Client order id whose venue events are queried.
            event_types: Qualifying venue event types.
            statuses: Optional case-insensitive statuses to match.

        Returns:
            Newest qualifying `received_at`, or None when no event matches.
        """
        conditions: list[ColumnElement[bool]] = [
            VenueEvent.client_order_id == client_order_id,
            VenueEvent.event_type.in_(list(event_types)),
        ]
        if statuses is not None:
            conditions.append(
                func.lower(VenueEvent.status).in_([status.lower() for status in statuses])
            )
        result = await self._session.execute(
            select(func.max(VenueEvent.received_at)).where(*conditions)
        )
        return result.scalar_one_or_none()
