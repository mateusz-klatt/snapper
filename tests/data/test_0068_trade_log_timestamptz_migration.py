"""Migration 0068: the trade log tables' timestamps become ``timestamptz``."""

import importlib
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.orm import Session

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.models import TradeCommand
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import TZDateTime
from snapper.data.models import VenueEvent

migration = importlib.import_module("snapper.data.migrations.versions.0068_trade_log_timestamptz")

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLES = ("trade_commands", "venue_events", "trade_projection_checkpoints")
_ALREADY_AWARE = frozenset({"replay_window_start", "replay_window_end", "position_opened_at"})
_ACTIVE = "known_to = '9999-12-31T23:59:59+00:00'"
_AWARE = "TYPE timestamp with time zone"
_NAIVE = "TYPE timestamp without time zone"
_UPGRADE_SQL = (
    "SET LOCAL TIME ZONE 'UTC'",
    "SET LOCAL lock_timeout = '5s'",
    "SET LOCAL statement_timeout = '30s'",
    (
        'LOCK TABLE "trade_commands", "venue_events", "trade_projection_checkpoints" '
        "IN ACCESS EXCLUSIVE MODE"
    ),
    'DROP INDEX "ix_trade_commands_public_id"',
    'DROP INDEX "uq_trade_commands_idempotency"',
    'DROP INDEX "ix_venue_events_public_id"',
    'DROP INDEX "ix_trade_checkpoints_public_id"',
    'DROP INDEX "uq_trade_checkpoints_shard_key"',
    (
        'ALTER TABLE "trade_commands" '
        f'ALTER COLUMN "timestamp" {_AWARE}, '
        f'ALTER COLUMN "known_to" {_AWARE}, '
        f'ALTER COLUMN "created_at" {_AWARE}, '
        f'ALTER COLUMN "dispatched_at" {_AWARE}, '
        f'ALTER COLUMN "acked_at" {_AWARE}, '
        f'ALTER COLUMN "terminal_at" {_AWARE}'
    ),
    (
        'ALTER TABLE "venue_events" '
        f'ALTER COLUMN "timestamp" {_AWARE}, '
        f'ALTER COLUMN "known_to" {_AWARE}, '
        f'ALTER COLUMN "venue_timestamp" {_AWARE}, '
        f'ALTER COLUMN "received_at" {_AWARE}'
    ),
    (
        'ALTER TABLE "trade_projection_checkpoints" '
        f'ALTER COLUMN "timestamp" {_AWARE}, '
        f'ALTER COLUMN "known_to" {_AWARE}, '
        f'ALTER COLUMN "last_venue_event_at" {_AWARE}, '
        f'ALTER COLUMN "checkpoint_at" {_AWARE}'
    ),
    (
        'CREATE UNIQUE INDEX "ix_trade_commands_public_id" ON "trade_commands" ("public_id") '
        f"WHERE {_ACTIVE}"
    ),
    (
        'CREATE UNIQUE INDEX "uq_trade_commands_idempotency" ON "trade_commands" '
        f'("idempotency_key") WHERE idempotency_key IS NOT NULL AND {_ACTIVE}'
    ),
    (
        'CREATE UNIQUE INDEX "ix_venue_events_public_id" ON "venue_events" ("public_id") '
        f"WHERE {_ACTIVE}"
    ),
    (
        'CREATE UNIQUE INDEX "ix_trade_checkpoints_public_id" ON "trade_projection_checkpoints" '
        f'("public_id") WHERE {_ACTIVE}'
    ),
    (
        'CREATE UNIQUE INDEX "uq_trade_checkpoints_shard_key" ON "trade_projection_checkpoints" '
        f'("shard_key") WHERE {_ACTIVE}'
    ),
)
_DOWNGRADE_SQL = tuple(statement.replace(_AWARE, _NAIVE) for statement in _UPGRADE_SQL)
_WRITTEN_AT = datetime(2026, 11, 11, 8, 1, 1, tzinfo=UTC)
_CLOSED_AT = datetime(2026, 11, 11, 8, 1, 30, tzinfo=UTC)
_SHARD = "kraken|BTC-USD|live"


def _statements(dialect: str, direction: str) -> list[str]:
    """Run one migration direction against a recording bind.

    Args:
        dialect: Dialect name the bind reports.
        direction: ``upgrade`` or ``downgrade``.

    Returns:
        The SQL text of every statement executed, in order.
    """
    bind = MagicMock()
    bind.dialect.name = dialect
    operations = MagicMock()
    operations.get_bind.return_value = bind
    with patch.object(migration, "op", operations):
        getattr(migration, direction)()
    return [str(call.args[0]) for call in bind.execute.call_args_list]


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config for one isolated SQLite database.

    Args:
        db_url: SQLAlchemy URL of the database.

    Returns:
        The repository's Alembic config pointed at the database.
    """
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


@pytest.fixture
def pre_0068_db(migrated_db_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a private SQLite database downgraded to 0067.

    Args:
        migrated_db_path: Private copy of the head-migrated template.

    Yields:
        The database engine and its Alembic config.
    """
    db_url = f"sqlite:///{migrated_db_path}"
    engine = sa.create_engine(db_url, future=True)
    config = _make_alembic_config(db_url)
    command.downgrade(config, "0067")
    try:
        yield engine, config
    finally:
        engine.dispose()


def _seed(engine: sa.Engine) -> None:
    """Write active and closed versions and two append-only venue events.

    Every nullable altered column holds a value in one row and NULL in another.

    Args:
        engine: Engine of the database at 0067.
    """
    wallet = str(uuid4())
    session_id = str(uuid4())
    event_fields = {
        "shard_key": _SHARD,
        "wallet_public_id": wallet,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "client_order_id": "active",
        "received_at": _CLOSED_AT,
        "session_id": session_id,
        "timestamp": _CLOSED_AT,
    }
    command_fields = {
        "command_type": "submit",
        "shard_key": _SHARD,
        "wallet_public_id": wallet,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "manual",
        "venue_client_id": "venue-1",
        "side": "buy",
        "order_type": "limit",
        "quantity": 1.0,
        "price": 100.0,
        "created_at": _WRITTEN_AT,
        "session_id": session_id,
        "timestamp": _WRITTEN_AT,
    }
    checkpoint_fields = {
        "shard_key": _SHARD,
        "wallet_public_id": wallet,
        "cash": 100.0,
        "peak_equity": 100.0,
        "checkpoint_at": _WRITTEN_AT,
        "session_id": session_id,
        "timestamp": _WRITTEN_AT,
    }
    with Session(engine) as session, session.begin():
        session.add_all(
            [
                TradeCommand(
                    **command_fields,
                    client_order_id="closed",
                    idempotency_key="idem-closed",
                    status="filled",
                    dispatched_at=_WRITTEN_AT,
                    acked_at=_WRITTEN_AT,
                    terminal_at=_CLOSED_AT,
                    sequence_id=1,
                    known_to=_CLOSED_AT,
                ),
                TradeCommand(
                    **command_fields,
                    client_order_id="active",
                    idempotency_key="idem-active",
                    status="created",
                    sequence_id=2,
                ),
                VenueEvent(
                    **event_fields,
                    event_type="order_accepted",
                    venue_timestamp=_WRITTEN_AT,
                    sequence_id=1,
                ),
                VenueEvent(**event_fields, event_type="order_rejected", sequence_id=2),
                TradeProjectionCheckpoint(
                    **checkpoint_fields,
                    last_venue_event_at=_WRITTEN_AT,
                    sequence_id=1,
                    known_to=_CLOSED_AT,
                ),
                TradeProjectionCheckpoint(**checkpoint_fields, sequence_id=2),
            ]
        )


def _snapshot(engine: sa.Engine) -> tuple[tuple[tuple[str, str, str], ...], tuple[str, ...]]:
    """Read the exact DDL and every stored row of the three tables.

    Args:
        engine: Engine of the database.

    Returns:
        The ``sqlite_master`` entries of the tables and their indexes, and the
        rows of each table in id order.
    """
    with engine.connect() as connection:
        schema = tuple(
            (str(row.type), str(row.name), str(row.sql))
            for row in connection.execute(
                sa.text(
                    "SELECT type, name, sql FROM sqlite_master "
                    "WHERE tbl_name IN ('trade_commands', 'venue_events', "
                    "'trade_projection_checkpoints') ORDER BY type, name"
                )
            )
        )
        rows = tuple(
            repr(connection.execute(sa.text(f"SELECT * FROM {table} ORDER BY id")).all())
            for table in _TABLES
        )
    return schema, rows


def _active(engine: sa.Engine) -> tuple[list[str | None], list[str | None], list[str]]:
    """Read the active rows of the three tables through the ORM.

    Args:
        engine: Engine of the database.

    Returns:
        The client order ids of the active commands and venue events, and the
        shard keys of the active checkpoints.
    """
    with Session(engine) as session:
        commands = session.scalars(
            sa.select(TradeCommand.client_order_id).where(TradeCommand.known_to == KNOWN_TO_MAX)
        ).all()
        events = session.scalars(
            sa.select(VenueEvent.client_order_id).where(VenueEvent.known_to == KNOWN_TO_MAX)
        ).all()
        checkpoints = session.scalars(
            sa.select(TradeProjectionCheckpoint.shard_key).where(
                TradeProjectionCheckpoint.known_to == KNOWN_TO_MAX
            )
        ).all()
    return list(commands), list(events), list(checkpoints)


def test_upgrade_issues_the_postgresql_conversion_in_order() -> None:
    """The PostgreSQL upgrade pins the session, locks, then converts under UTC.

    Given: A bind that reports the PostgreSQL dialect.
    When: The upgrade runs.
    Then: It sets the UTC zone and both timeouts, locks the three tables in one
        statement, drops the five partial indexes, alters each table's columns
        to ``timestamp with time zone`` in one statement, and recreates the
        five indexes with the model's predicate text, in that order.
    """
    assert _statements("postgresql", "upgrade") == list(_UPGRADE_SQL)


def test_downgrade_issues_the_same_conversion_back_to_naive() -> None:
    """The PostgreSQL downgrade mirrors the upgrade with the naive type.

    Given: A bind that reports the PostgreSQL dialect.
    When: The downgrade runs.
    Then: It issues the upgrade's statements, with ``timestamp without time
        zone`` as the target type and the same index predicate text.
    """
    assert _statements("postgresql", "downgrade") == list(_DOWNGRADE_SQL)


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_sqlite_issues_nothing(direction: str) -> None:
    """SQLite already stores the columns as the ORM declares.

    Given: A bind that reports the SQLite dialect.
    When: Either direction runs.
    Then: No statement is executed.
    """
    assert _statements("sqlite", direction) == []


@pytest.mark.parametrize("direction", ["upgrade", "downgrade"])
def test_other_dialects_are_refused(direction: str) -> None:
    """A database other than PostgreSQL or SQLite fails closed.

    Given: A bind that reports the MySQL dialect.
    When: Either direction runs.
    Then: It raises before executing any statement.
    """
    with pytest.raises(RuntimeError, match="only PostgreSQL and SQLite"):
        _statements("mysql", direction)


def test_recreated_indexes_match_the_orm() -> None:
    """Each recreated index is the one the ORM declares.

    Given: The migration's five partial indexes and the ORM metadata.
    When: Each is compared with the ORM index of the same name.
    Then: The table, the single column, uniqueness and the PostgreSQL
        predicate text are identical.
    """
    for index in migration._INDEXES:
        declared = next(
            item for item in Base.metadata.tables[index.table].indexes if item.name == index.name
        )
        assert [column.name for column in declared.columns] == [index.column]
        assert declared.unique is True
        assert str(declared.dialect_options["postgresql"]["where"]) == index.predicate


def test_every_known_to_partial_index_is_recreated() -> None:
    """No partial index on these tables keeps a predicate the ALTER would reject.

    Given: Every ORM index of the three tables.
    When: The indexes whose PostgreSQL predicate names ``known_to`` are
        collected.
    Then: They are exactly the five the migration drops and recreates.
    """
    declared = {
        index.name
        for table in _TABLES
        for index in Base.metadata.tables[table].indexes
        if "known_to" in str(index.dialect_options["postgresql"]["where"])
    }
    assert declared == {index.name for index in migration._INDEXES}


def test_the_columns_are_every_legacy_orm_timestamp() -> None:
    """The migration converts every timestamp column 0001 created naive.

    Given: The ``TZDateTime`` columns the ORM declares on the three tables,
        minus the three that were created aware.
    When: They are compared with the migration's column lists.
    Then: They are equal for each table, and there are 14 in all.
    """
    for table in migration._TABLES:
        declared = {
            column.name
            for column in Base.metadata.tables[table.name].columns
            if isinstance(column.type, TZDateTime)
        }
        assert set(table.columns) == declared - _ALREADY_AWARE
    assert tuple(table.name for table in migration._TABLES) == _TABLES
    assert sum(len(table.columns) for table in migration._TABLES) == 14


def test_sqlite_round_trip_leaves_schema_and_rows_unchanged(
    pre_0068_db: tuple[sa.Engine, Config],
) -> None:
    """On SQLite the migration changes neither the schema nor a stored value.

    Given: A database at 0067 holding an active and a closed version of a
        command and a checkpoint, and two venue events, with every nullable
        altered column set in one row and NULL in another.
    When: It is upgraded to 0068, downgraded to 0067 and upgraded again.
    Then: The tables' and indexes' DDL and every row are unchanged at each
        step, and the ORM still reads the active rows.
    """
    engine, config = pre_0068_db
    _seed(engine)
    before = _snapshot(engine)
    expected_active = (["active"], ["active", "active"], [_SHARD])
    assert _active(engine) == expected_active

    command.upgrade(config, "0068")
    assert _snapshot(engine) == before
    assert _active(engine) == expected_active

    command.downgrade(config, "0067")
    assert _snapshot(engine) == before

    command.upgrade(config, "0068")
    assert _snapshot(engine) == before
    assert _active(engine) == expected_active
