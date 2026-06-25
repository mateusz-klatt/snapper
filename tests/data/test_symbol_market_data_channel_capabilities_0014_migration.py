"""Tests for the 0014 symbol_market_data_channel_capabilities migration."""

import importlib
from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import Base

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
MIGRATION = importlib.import_module(
    "snapper.data.migrations.versions.0014_symbol_market_data_channel_capabilities"
)
_TABLE = "symbol_market_data_channel_capabilities"
_TS = "2026-06-20 12:00:00.000000"
_CLOSED = "2026-06-21 12:00:00.000000"
_ACTIVE = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL.

    Args:
        db_url: Database URL used by Alembic.

    Returns:
        Configured Alembic ``Config``.
    """
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def db_at_0013(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded to 0013.

    Args:
        tmp_path: Per-test temporary directory.

    Yields:
        Tuple of SQLAlchemy engine and Alembic config.
    """
    db_path = tmp_path / "channel_capabilities.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0013")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


def _insert_channel_capability(
    engine: sa.Engine,
    *,
    public_id: str,
    symbol_public_id: str = "11111111-1111-7111-8111-111111111111",
    exchange: str = "kraken_futures",
    channel: str = "trade",
    can_market_data: bool = False,
    known_to: str = _ACTIVE,
) -> None:
    """Insert one channel capability row.

    Args:
        engine: Database engine.
        public_id: Public identity for the SCD2 row family.
        symbol_public_id: Symbol public identity.
        exchange: Lowercase exchange identifier.
        channel: Lowercase channel identifier.
        can_market_data: Market-data channel allowance.
        known_to: SCD2 close timestamp.

    Returns:
        None.
    """
    stmt = (
        f"INSERT INTO {_TABLE} "
        "(public_id, symbol_public_id, exchange, channel, can_market_data, "
        "source, reason, created_at, session_id, sequence_id, timestamp, known_to) "
        "VALUES (:pid, :spid, :ex, :ch, :can_md, 'test', 'reason', :ts, "
        "'22222222-2222-7222-8222-222222222222', 1, :ts, :known_to)"
    )
    with engine.begin() as conn:
        conn.execute(
            sa.text(stmt),
            {
                "pid": public_id,
                "spid": symbol_public_id,
                "ex": exchange,
                "ch": channel,
                "can_md": can_market_data,
                "ts": _TS,
                "known_to": known_to,
            },
        )


def _table_signature(
    engine: sa.Engine,
) -> tuple[
    list[tuple[str, str, bool, bool]],
    set[tuple[str, tuple[str, ...], bool]],
    set[str],
]:
    """Return comparable channel capability schema details.

    Args:
        engine: Database engine to inspect.

    Returns:
        Column, index, and check-constraint signatures.
    """
    inspector = sa.inspect(engine)
    columns = [
        (
            str(column["name"]),
            column["type"].__class__.__name__,
            bool(column["nullable"]),
            bool(column.get("primary_key", False)),
        )
        for column in inspector.get_columns(_TABLE)
    ]
    indexes = {
        (
            str(index["name"]),
            tuple(str(column) for column in index["column_names"]),
            bool(index["unique"]),
        )
        for index in inspector.get_indexes(_TABLE)
    }
    checks = {
        str(check["name"])
        for check in inspector.get_check_constraints(_TABLE)
        if check["name"] is not None
    }
    return columns, indexes, checks


def test_0014_creates_channel_capability_table_and_indexes(
    db_at_0013: tuple[sa.Engine, Config],
) -> None:
    """Migration creates the channel capability table and active indexes.

    Args:
        db_at_0013: SQLite database and Alembic config.
    """
    engine, cfg = db_at_0013
    command.upgrade(cfg, "0014")
    inspector = sa.inspect(engine)
    assert _TABLE in inspector.get_table_names()
    index_names = {str(index["name"]) for index in inspector.get_indexes(_TABLE)}
    assert {
        "uq_smdcc_symbol_exchange_channel",
        "ix_symbol_market_data_channel_capabilities_public_id",
        "ix_symbol_market_data_channel_capabilities_symbol_public_id",
        "ix_smdcc_exchange_channel",
    } <= index_names


def test_0014_revision_metadata() -> None:
    """Migration exposes the expected Alembic revision identifiers."""
    assert MIGRATION.revision == "0014"
    assert MIGRATION.down_revision == "0013"


def test_0014_enforces_lowercase_non_empty_channel_and_exchange(
    db_at_0013: tuple[sa.Engine, Config],
) -> None:
    """The CHECK constraints keep exchange lowercase and channel lowercase non-empty.

    Args:
        db_at_0013: SQLite database and Alembic config.
    """
    engine, cfg = db_at_0013
    command.upgrade(cfg, "0014")
    _insert_channel_capability(engine, public_id="33333333-3333-7333-8333-333333333333")
    with pytest.raises(sa.exc.IntegrityError):
        _insert_channel_capability(
            engine,
            public_id="44444444-4444-7444-8444-444444444444",
            exchange="Kraken_Futures",
            channel="trade",
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert_channel_capability(
            engine,
            public_id="55555555-5555-7555-8555-555555555555",
            channel="Trade",
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert_channel_capability(
            engine,
            public_id="66666666-6666-7666-8666-666666666666",
            channel="",
        )


def test_0014_active_unique_allows_closed_successor_history(
    db_at_0013: tuple[sa.Engine, Config],
) -> None:
    """Only active rows collide on the SCD2 natural key.

    Args:
        db_at_0013: SQLite database and Alembic config.
    """
    engine, cfg = db_at_0013
    command.upgrade(cfg, "0014")
    _insert_channel_capability(
        engine,
        public_id="33333333-3333-7333-8333-333333333333",
        known_to=_CLOSED,
    )
    _insert_channel_capability(
        engine,
        public_id="33333333-3333-7333-8333-333333333333",
        can_market_data=True,
    )
    with pytest.raises(sa.exc.IntegrityError):
        _insert_channel_capability(
            engine,
            public_id="77777777-7777-7777-8777-777777777777",
        )


def test_0014_metadata_matches_model_for_channel_capabilities(
    db_at_0013: tuple[sa.Engine, Config],
    tmp_path: Path,
) -> None:
    """Alembic and SQLAlchemy metadata create the same channel table shape.

    Args:
        db_at_0013: SQLite database and Alembic config.
        tmp_path: Per-test temporary directory.
    """
    migration_engine, cfg = db_at_0013
    command.upgrade(cfg, "0014")
    metadata_path = tmp_path / "metadata_channel_capabilities.db"
    metadata_engine = sa.create_engine(f"sqlite:///{metadata_path}", future=True)
    try:
        Base.metadata.create_all(metadata_engine)
        assert _table_signature(migration_engine) == _table_signature(metadata_engine)
    finally:
        metadata_engine.dispose()


def test_0014_downgrade_drops_channel_capabilities(
    db_at_0013: tuple[sa.Engine, Config],
) -> None:
    """Downgrade removes symbol_market_data_channel_capabilities.

    Args:
        db_at_0013: SQLite database and Alembic config.
    """
    engine, cfg = db_at_0013
    command.upgrade(cfg, "0014")
    command.downgrade(cfg, "0013")
    assert _TABLE not in sa.inspect(engine).get_table_names()
