"""Builders shared by the cancel-target repair (G3-C) tests: commands, orders and evidence."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from snapper.application.engine.service import compute_shard_key
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import Order
from snapper.data.models import TradeCommand
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository

WALLET = "00000000-0000-7000-8000-000000000001"
"""Wallet of every seeded Kraken row."""

OTHER_WALLET = "00000000-0000-7000-8000-000000000002"
"""A second wallet, for mismatch cases."""

INSTRUMENT_PUBLIC_ID = "00000000-0000-7000-8000-0000000000c1"
"""Instrument public id of the seeded orders."""

SESSION = "00000000-0000-7000-8000-0000000000a1"
"""Provenance session of every seeded row."""

CREATED_AT = datetime(2026, 11, 9, 9, 46, 0, tzinfo=UTC)
"""Creation time of the seeded create command."""

CANCEL_AT = CREATED_AT + timedelta(minutes=1)
"""Creation time of the seeded cancel command."""

CANCELED_AT = CANCEL_AT + timedelta(seconds=4)
"""Time the executor stored the order's ``canceled`` version."""

KRAKEN_SHARD = compute_shard_key(
    instrument="XRP-EUR",
    exchange=ExchangeEnum.KRAKEN,
    mode=ExecutionModeEnum.LIVE,
    wallet_public_id=WALLET,
    strategy_tag=None,
)
"""Shard key of the seeded Kraken spot commands."""

WALUTOMAT_SHARD = compute_shard_key(
    instrument="EUR-PLN",
    exchange=ExchangeEnum.WALUTOMAT,
    mode=ExecutionModeEnum.LIVE,
    wallet_public_id=WALLET,
    strategy_tag=None,
)
"""Shard key of the seeded Walutomat commands, a shard of its own as in production."""


@dataclass(frozen=True, slots=True)
class SeededPair:
    """Public ids of one seeded create and cancel pair.

    Attributes:
        create_id: Public id of the create command.
        cancel_id: Public id of the cancel command.
        order_id: Public id of the order.
        client_order_id: Client order id shared by both commands.
    """

    create_id: str
    cancel_id: str
    order_id: str
    client_order_id: str


@asynccontextmanager
async def sqlite_repository(path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an empty SQLite repository on one file.

    Args:
        path: Database file to create.

    Yields:
        The repository, disposed on exit.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{path}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def command(
    public_id: str,
    *,
    command_type: str,
    status: str,
    client_order_id: str,
    created_at: datetime = CREATED_AT,
) -> TradeCommand:
    """Build one active trade command row.

    Args:
        public_id: Public id of the command.
        command_type: ``create``, ``submit``, ``replace`` or ``cancel``.
        status: Current status.
        client_order_id: Client order id of the target order.
        created_at: Creation time.

    Returns:
        The ORM row, not yet added.
    """
    return TradeCommand(
        public_id=public_id,
        session_id=SESSION,
        sequence_id=1,
        timestamp=created_at,
        known_to=KNOWN_TO_MAX,
        command_type=command_type,
        shard_key=KRAKEN_SHARD,
        wallet_public_id=WALLET,
        exchange="kraken",
        instrument="XRP-EUR",
        mode="live",
        strategy_id="manual",
        client_order_id=client_order_id,
        venue_client_id=client_order_id,
        side="buy",
        order_type="limit",
        quantity=5.0,
        price=0.5,
        status=status,
        created_at=created_at,
    )


def order_version(
    public_id: str,
    *,
    status: str,
    client_order_id: str,
    timestamp: datetime,
    known_to: datetime = KNOWN_TO_MAX,
) -> Order:
    """Build one version of an order row.

    Args:
        public_id: Public id shared by the order's versions.
        status: Wire status of this version.
        client_order_id: Client order id of the order.
        timestamp: Time the version was stored.
        known_to: End of the version, ``KNOWN_TO_MAX`` while active.

    Returns:
        The ORM row, not yet added.
    """
    return Order(
        public_id=public_id,
        session_id=SESSION,
        sequence_id=1,
        timestamp=timestamp,
        known_to=known_to,
        instrument_public_id=INSTRUMENT_PUBLIC_ID,
        mode="live",
        wallet_public_id=WALLET,
        client_order_id=client_order_id,
        exchange_order_id="OX-1",
        created_at=CREATED_AT,
        side="buy",
        order_type="limit",
        price=0.5,
        size=5.0,
        status=status,
    )


def execution(
    order_public_id: str,
    *,
    size: float = 0.0,
    fee: float = 0.0,
    exec_id: str | None = None,
    trade_id: str | None = None,
) -> Execution:
    """Build one execution row.

    Args:
        order_public_id: Order the execution belongs to.
        size: Executed size.
        fee: Fee amount.
        exec_id: Venue execution id, or ``None``.
        trade_id: Venue trade id, or ``None``.

    Returns:
        The ORM row, not yet added.
    """
    return Execution(
        session_id=SESSION,
        sequence_id=1,
        timestamp=CANCELED_AT,
        known_to=KNOWN_TO_MAX,
        order_public_id=order_public_id,
        wallet_public_id=WALLET,
        exchange="kraken",
        mode="live",
        scope_sequence=1,
        exec_id=exec_id,
        trade_id=trade_id,
        side="buy",
        status="filled",
        price=0.0,
        size=size,
        fee=fee,
        fee_asset="EUR",
        numeric_provenance="legacy_float",
    )


def venue_event(
    client_order_id: str,
    *,
    event_type: str,
    status: str | None = None,
    wallet_public_id: str = WALLET,
    received_at: datetime = CREATED_AT,
) -> VenueEvent:
    """Build one venue event row.

    Args:
        client_order_id: Order the event belongs to.
        event_type: Venue event type.
        status: Event status, or ``None``.
        wallet_public_id: Wallet of the event.
        received_at: Observation time.

    Returns:
        The ORM row, not yet added.
    """
    return VenueEvent(
        session_id=SESSION,
        sequence_id=1,
        timestamp=received_at,
        known_to=KNOWN_TO_MAX,
        event_type=event_type,
        shard_key=KRAKEN_SHARD,
        wallet_public_id=wallet_public_id,
        exchange="kraken",
        instrument="XRP-EUR",
        mode="live",
        exchange_order_id="OX-1",
        client_order_id=client_order_id,
        status=status,
        received_at=received_at,
        liquidity_role="unknown",
    )


def checkpoint(
    *, open_command_ids: str | None = None, wallet_public_id: str = WALLET
) -> TradeProjectionCheckpoint:
    """Build one active projection checkpoint of the Kraken shard.

    Args:
        open_command_ids: JSON list of open command ids, or ``None``.
        wallet_public_id: Wallet of the checkpoint.

    Returns:
        The ORM row, not yet added.
    """
    return TradeProjectionCheckpoint(
        session_id=SESSION,
        sequence_id=1,
        timestamp=CREATED_AT,
        known_to=KNOWN_TO_MAX,
        shard_key=KRAKEN_SHARD,
        wallet_public_id=wallet_public_id,
        cash=0.0,
        peak_equity=0.0,
        last_venue_event_id=1,
        open_command_ids=open_command_ids,
        checkpoint_at=CREATED_AT,
    )


async def add_rows(
    repository: SQLAlchemyRepository,
    rows: list[TradeCommand | Order | Execution | VenueEvent | TradeProjectionCheckpoint],
) -> None:
    """Insert rows in one transaction.

    Args:
        repository: Repository to write to.
        rows: ORM rows to add.
    """
    async with repository.session() as session:
        session.add_all(rows)
        await session.commit()


async def seed_kraken_pair(repository: SQLAlchemyRepository, index: int = 1) -> SeededPair:
    """Seed a Kraken create stuck in ``accepted`` and its cancel stuck in ``dispatched``.

    The order went ``open`` then ``canceled`` four seconds after the cancel,
    has no execution, and has only its ``order_accepted`` event. Every pair
    sits on the one Kraken shard, as production's three pairs do, and pair
    ``index`` has the exchange order id ``OX-<index>``.

    Args:
        repository: Repository to seed.
        index: Distinguishes several pairs in one database.

    Returns:
        The seeded ids.
    """
    pair = SeededPair(
        create_id=f"00000000-0000-7000-8000-00000000{index:02d}c0",
        cancel_id=f"00000000-0000-7000-8000-00000000{index:02d}ca",
        order_id=f"00000000-0000-7000-8000-00000000{index:02d}0d",
        client_order_id=f"cid-{index}",
    )
    rows: list[TradeCommand | Order | Execution | VenueEvent | TradeProjectionCheckpoint] = [
        command(
            pair.create_id,
            command_type="create",
            status="accepted",
            client_order_id=pair.client_order_id,
        ),
        command(
            pair.cancel_id,
            command_type="cancel",
            status="dispatched",
            client_order_id=pair.client_order_id,
            created_at=CANCEL_AT,
        ),
        order_version(
            pair.order_id,
            status="open",
            client_order_id=pair.client_order_id,
            timestamp=CREATED_AT,
            known_to=CANCELED_AT,
        ),
        order_version(
            pair.order_id,
            status="canceled",
            client_order_id=pair.client_order_id,
            timestamp=CANCELED_AT,
        ),
        venue_event(pair.client_order_id, event_type="order_accepted", status="open"),
    ]
    for row in rows:
        if isinstance(row, Order | VenueEvent):
            row.exchange_order_id = f"OX-{index}"
    await add_rows(repository, rows)
    return pair


async def seed_walutomat_pair(repository: SQLAlchemyRepository) -> SeededPair:
    """Seed a Walutomat create refused by the interlock and its cancel in ``dispatched``.

    The pair and its event sit on their own EUR-PLN shard, as production's do.

    Args:
        repository: Repository to seed.

    Returns:
        The seeded ids.
    """
    pair = SeededPair(
        create_id="00000000-0000-7000-8000-0000000014c0",
        cancel_id="00000000-0000-7000-8000-0000000014ca",
        order_id="00000000-0000-7000-8000-00000000140d",
        client_order_id="cid-walutomat",
    )
    create = command(
        pair.create_id,
        command_type="create",
        status="failed",
        client_order_id=pair.client_order_id,
    )
    create.exchange = "walutomat"
    create.last_error = "order_interlock_blocked"
    create.terminal_at = None
    create.shard_key = WALUTOMAT_SHARD
    create.instrument = "EUR-PLN"
    cancel = command(
        pair.cancel_id,
        command_type="cancel",
        status="dispatched",
        client_order_id=pair.client_order_id,
        created_at=CANCEL_AT,
    )
    cancel.exchange = "walutomat"
    cancel.shard_key = WALUTOMAT_SHARD
    cancel.instrument = "EUR-PLN"
    blocked = venue_event(
        pair.client_order_id,
        event_type="order_interlock_blocked",
        status="rejected",
    )
    blocked.exchange = "walutomat"
    blocked.shard_key = WALUTOMAT_SHARD
    blocked.instrument = "EUR-PLN"
    await add_rows(repository, [create, cancel, blocked])
    return pair
