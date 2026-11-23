"""Read-only queries of the cancel-target repair (G3-C).

Active-row reads are clockless. ``where_active`` at a point in time misses a
row stamped after it, so these queries bound only ``known_to > now`` in SQL,
which every open version passes, and then prove each loaded row open-ended
with :func:`~snapper.data.models.is_open_ended_version`, the sanctioned
exact comparison, which drops a version closed later than now. A read that finds more than one active row raises, as
``get_active_create_command_by_client_order_id`` does. Orders, executions,
paired legs and venue events are read without joins, so a row whose instrument or symbol
lineage is incomplete is still found. Nothing here writes; the repair writes
only through ``insert_order_terminal_once`` and
``advance_trade_command_lifecycle``.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import union

from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import PairedExecutionLeg
from snapper.data.models import TradeCommand
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import VenueEvent
from snapper.data.models import is_open_ended_version
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionEvidenceRow
from snapper.data.repository_types import OrderVersionRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow

OPEN_COMMAND_TYPES: tuple[str, ...] = ("create", "submit", "replace")
"""Command types that hold a shard's in-flight order."""

TERMINAL_COMMAND_STATUSES: tuple[str, ...] = (
    "filled",
    "cancelled",
    "rejected",
    "failed",
    "expired",
)
"""Command statuses after which a command no longer holds its shard."""


async def get_active_trade_command(
    repository: SQLAlchemyRepository, public_id: str
) -> TradeCommandRow | None:
    """Return the active version of one trade command.

    Args:
        repository: Repository whose session is used.
        public_id: Public id of the command.

    Returns:
        The active row, or ``None`` when the command has no active version.

    Raises:
        RuntimeError: When more than one version is active.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(TradeCommand).where(
                TradeCommand.public_id == public_id,
                TradeCommand.known_to > datetime.now(UTC),
            )
        )
        commands = [row for row in result.scalars().all() if is_open_ended_version(row.known_to)]
    if len(commands) > 1:
        raise RuntimeError("several active versions of one trade command")
    if not commands:
        return None
    return SQLAlchemyRepository._trade_command_to_row(commands[0])


async def get_trade_command_versions(
    repository: SQLAlchemyRepository, public_id: str
) -> list[TradeCommandRow]:
    """Return every version of one trade command, oldest first.

    Args:
        repository: Repository whose session is used.
        public_id: Public id of the command.

    Returns:
        Every version, closed ones included, ordered by ``id``.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(TradeCommand)
            .where(TradeCommand.public_id == public_id)
            .order_by(TradeCommand.id)
        )
        commands = result.scalars().all()
    return [SQLAlchemyRepository._trade_command_to_row(command) for command in commands]


async def get_order_versions_by_client_order_id(
    repository: SQLAlchemyRepository, client_order_id: str
) -> list[OrderVersionRow]:
    """Return every version of every order carrying one client order id.

    Args:
        repository: Repository whose session is used.
        client_order_id: Client order id shared by the order's versions.

    Returns:
        Every version, closed ones included, ordered by ``id``.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(Order).where(Order.client_order_id == client_order_id).order_by(Order.id)
        )
        orders = result.scalars().all()
    return [
        {
            "id": order.id,
            "public_id": order.public_id,
            "instrument_public_id": order.instrument_public_id,
            "mode": order.mode,
            "wallet_public_id": order.wallet_public_id,
            "client_order_id": order.client_order_id,
            "exchange_order_id": order.exchange_order_id,
            "status": order.status,
            "order_type": order.order_type,
            "side": order.side,
            "size": order.size,
            "filled_size": order.filled_size,
            "leverage": order.leverage,
            "timestamp": order.timestamp,
            "known_to": order.known_to,
        }
        for order in orders
    ]


async def get_executions_by_order_public_ids(
    repository: SQLAlchemyRepository, public_ids: Sequence[str]
) -> list[ExecutionEvidenceRow]:
    """Return every execution of the given orders, without joins.

    Args:
        repository: Repository whose session is used.
        public_ids: Order public ids whose executions are read.

    Returns:
        Every row, ordered by ``id``, with no ``known_to`` filter.
    """
    if not public_ids:
        return []
    async with repository.session() as session:
        result = await session.execute(
            select(Execution)
            .where(Execution.order_public_id.in_(list(public_ids)))
            .order_by(Execution.id)
        )
        executions = result.scalars().all()
    return [
        {
            "id": execution.id,
            "order_public_id": execution.order_public_id,
            "status": execution.status,
            "size": execution.size,
            "fee": execution.fee,
            "price": execution.price,
            "exec_id": execution.exec_id,
            "trade_id": execution.trade_id,
            "numeric_provenance": execution.numeric_provenance,
            "price_decimal": execution.price_decimal,
            "size_decimal": execution.size_decimal,
            "fee_decimal": execution.fee_decimal,
            "timestamp": execution.timestamp,
        }
        for execution in executions
    ]


async def get_venue_events_for_client_order_id(
    repository: SQLAlchemyRepository,
    client_order_id: str,
    exchange_order_ids: Sequence[str] = (),
) -> list[VenueEventRow]:
    """Return every venue event matching one client id or optional exchange ids.

    Args:
        repository: Repository whose session is used.
        client_order_id: Client order id of the events.
        exchange_order_ids: Exchange order ids that H-margin also treats as
            the same order's identity. Empty for the unchanged spot read.

    Returns:
        Every event of every type, ordered by ``id``, in the shape the
        reconciler's fold consumes.
    """
    identities = [VenueEvent.client_order_id == client_order_id]
    if exchange_order_ids:
        identities.append(VenueEvent.exchange_order_id.in_(list(exchange_order_ids)))
    async with repository.session() as session:
        result = await session.execute(
            select(VenueEvent).where(or_(*identities)).order_by(VenueEvent.id)
        )
        events = result.scalars().all()
    return [SQLAlchemyRepository._venue_event_to_row(event) for event in events]


async def has_paired_execution_leg(
    repository: SQLAlchemyRepository,
    client_order_id: str,
    exchange_order_ids: Sequence[str],
) -> bool:
    """Return whether any paired-execution leg names either order identity.

    Args:
        repository: Repository whose session is used.
        client_order_id: Client order id to match.
        exchange_order_ids: Exchange order ids to match.

    Returns:
        True when at least one historical or active paired-execution leg matches.
    """
    identities = [PairedExecutionLeg.client_order_id == client_order_id]
    if exchange_order_ids:
        identities.append(PairedExecutionLeg.exchange_order_id.in_(list(exchange_order_ids)))
    async with repository.session() as session:
        result = await session.execute(
            select(PairedExecutionLeg.id).where(or_(*identities)).limit(1)
        )
        leg_id = result.scalars().first()
    return leg_id is not None


async def get_open_commands_on_shard(
    repository: SQLAlchemyRepository, shard_key: str
) -> list[TradeCommandRow]:
    """Return the active commands that still hold a shard's in-flight order.

    Args:
        repository: Repository whose session is used.
        shard_key: Shard whose commands are read.

    Returns:
        Active create, submit and replace commands whose status is not
        terminal, ordered by ``created_at`` and then ``id``.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(TradeCommand)
            .where(
                TradeCommand.shard_key == shard_key,
                TradeCommand.known_to > datetime.now(UTC),
                TradeCommand.command_type.in_(OPEN_COMMAND_TYPES),
                TradeCommand.status.not_in(TERMINAL_COMMAND_STATUSES),
            )
            .order_by(TradeCommand.created_at, TradeCommand.id)
        )
        commands = result.scalars().all()
    return [
        SQLAlchemyRepository._trade_command_to_row(command)
        for command in commands
        if is_open_ended_version(command.known_to)
    ]


async def get_active_checkpoint(
    repository: SQLAlchemyRepository, shard_key: str
) -> TradeProjectionCheckpointRow | None:
    """Return a shard's active projection checkpoint, with its wallet.

    Args:
        repository: Repository whose session is used.
        shard_key: Shard whose checkpoint is read.

    Returns:
        The active checkpoint, or ``None`` when the shard has none.

    Raises:
        RuntimeError: When more than one checkpoint version is active.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(TradeProjectionCheckpoint).where(
                TradeProjectionCheckpoint.shard_key == shard_key,
                TradeProjectionCheckpoint.known_to > datetime.now(UTC),
            )
        )
        checkpoints = [row for row in result.scalars().all() if is_open_ended_version(row.known_to)]
    if len(checkpoints) > 1:
        raise RuntimeError("several active checkpoints of one shard")
    if not checkpoints:
        return None
    checkpoint = checkpoints[0]
    return {
        "public_id": checkpoint.public_id,
        "shard_key": checkpoint.shard_key,
        "projection_calc_version": checkpoint.projection_calc_version,
        "position_qty": checkpoint.position_qty,
        "entry_price": checkpoint.entry_price,
        "position_opened_at": checkpoint.position_opened_at,
        "cash": checkpoint.cash,
        "peak_equity": checkpoint.peak_equity,
        "realized_pnl": checkpoint.realized_pnl,
        "turnover": checkpoint.turnover,
        "last_venue_event_id": checkpoint.last_venue_event_id,
        "last_venue_event_at": checkpoint.last_venue_event_at,
        "open_command_ids": checkpoint.open_command_ids,
        "seen_exec_ids": checkpoint.seen_exec_ids,
        "checkpoint_at": checkpoint.checkpoint_at,
        "session_id": checkpoint.session_id,
        "wallet_public_id": checkpoint.wallet_public_id,
        "operator_public_id": checkpoint.operator_public_id,
    }


async def get_shard_wallets(repository: SQLAlchemyRepository, shard_key: str) -> list[str]:
    """Return every wallet that a shard's venue events and checkpoints name.

    Args:
        repository: Repository whose session is used.
        shard_key: Shard whose wallets are read.

    Returns:
        The distinct wallet public ids of every venue event and of every
        checkpoint version on the shard, sorted.
    """
    wallets = union(
        select(VenueEvent.wallet_public_id).where(VenueEvent.shard_key == shard_key),
        select(TradeProjectionCheckpoint.wallet_public_id).where(
            TradeProjectionCheckpoint.shard_key == shard_key
        ),
    )
    async with repository.session() as session:
        result = await session.execute(wallets)
        values = result.scalars().all()
    return sorted({str(value) for value in values})


async def get_terminal_written_by_session(
    repository: SQLAlchemyRepository, client_order_id: str, session_id: str
) -> VenueEventRow | None:
    """Read back the ``order_terminal`` a repair run wrote for one client order id.

    Args:
        repository: Repository whose session is used.
        client_order_id: Client order id the terminal belongs to.
        session_id: The repair run's session id.

    Returns:
        The written row, or ``None`` when that run wrote none.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(VenueEvent)
            .where(
                VenueEvent.client_order_id == client_order_id,
                VenueEvent.event_type == "order_terminal",
                VenueEvent.session_id == session_id,
            )
            .order_by(VenueEvent.id)
        )
        event = result.scalars().first()
    if event is None:
        return None
    return SQLAlchemyRepository._venue_event_to_row(event)
