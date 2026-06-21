"""Tests for the 0013 shadow_candles migration."""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import Base

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TS = "2026-06-20 12:00:00.000000"
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
def db_at_0012(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded to 0012.

    Args:
        tmp_path: Per-test temporary directory.

    Yields:
        Tuple of SQLAlchemy engine and Alembic config.
    """
    db_path = tmp_path / "shadow_candles.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0012")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


def _insert_shadow_candle(
    engine: sa.Engine,
    *,
    public_id: str,
    source: str,
    open_at: str = "2026-06-20 00:00:00.000000",
) -> None:
    """Insert one active shadow candle row.

    Args:
        engine: Database engine.
        public_id: Public identity for the inserted row.
        source: Source tag for CHECK validation.
        open_at: Candle open timestamp.

    Returns:
        None.
    """
    stmt = (
        "INSERT INTO shadow_candles "
        "(public_id, instrument_public_id, open_at, timeframe, open, high, low, "
        "close, volume, vwap, trades, source, complete, session_id, sequence_id, "
        "timestamp, known_to) VALUES (:pid, 'inst-1', :oa, '1m', 1.0, 2.0, "
        "0.5, 1.5, 10.0, 1.25, 4, :src, 1, 'sess-1', 1, :ts, :active)"
    )
    with engine.begin() as conn:
        conn.execute(
            sa.text(stmt),
            {"pid": public_id, "oa": open_at, "src": source, "ts": _TS, "active": _ACTIVE},
        )


def _table_signature(engine: sa.Engine) -> tuple[
    list[tuple[str, str, bool, bool]],
    set[tuple[str, tuple[str, ...], bool]],
    set[str],
]:
    """Return comparable shadow_candles schema details.

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
        for column in inspector.get_columns("shadow_candles")
    ]
    indexes = {
        (
            str(index["name"]),
            tuple(str(column) for column in index["column_names"]),
            bool(index["unique"]),
        )
        for index in inspector.get_indexes("shadow_candles")
    }
    checks = {
        str(check["name"])
        for check in inspector.get_check_constraints("shadow_candles")
        if check["name"] is not None
    }
    return columns, indexes, checks


def test_0013_creates_shadow_candles_table_and_indexes(
    db_at_0012: tuple[sa.Engine, Config],
) -> None:
    """Migration creates the shadow table and its indexes.

    Given: A database at 0012,
    When: 0013 is applied,
    Then: shadow_candles exists with the requested indexes.

    Args:
        db_at_0012: SQLite database and Alembic config.

    Returns:
        None.
    """
    engine, cfg = db_at_0012
    command.upgrade(cfg, "0013")
    inspector = sa.inspect(engine)
    assert "shadow_candles" in inspector.get_table_names()
    index_names = {str(index["name"]) for index in inspector.get_indexes("shadow_candles")}
    assert {
        "uq_shadow_candle_itf_open_source",
        "ix_shadow_candles_public_id",
        "ix_shadow_candle_instrument_open_source",
        "ix_shadow_candles_instrument_public_id",
    } <= index_names


def test_0013_source_check_accepts_calculated_and_rejects_unknown(
    db_at_0012: tuple[sa.Engine, Config],
) -> None:
    """The source CHECK accepts the shadow source vocabulary only.

    Given: A database upgraded to 0013,
    When: calculated and unknown source rows are inserted,
    Then: calculated succeeds and the unknown source raises IntegrityError.

    Args:
        db_at_0012: SQLite database and Alembic config.

    Returns:
        None.
    """
    engine, cfg = db_at_0012
    command.upgrade(cfg, "0013")
    _insert_shadow_candle(engine, public_id="sc-calc", source="calculated")
    with pytest.raises(sa.exc.IntegrityError):
        _insert_shadow_candle(
            engine,
            public_id="sc-bad",
            source="bogus",
            open_at="2026-06-20 00:01:00.000000",
        )


def test_0013_metadata_matches_model_for_shadow_candles(
    db_at_0012: tuple[sa.Engine, Config],
    tmp_path: Path,
) -> None:
    """Alembic and SQLAlchemy metadata create the same shadow table shape.

    Given: One database upgraded through 0013 and one created from metadata,
    When: their shadow_candles signatures are compared,
    Then: columns, indexes, and CHECK names match.

    Args:
        db_at_0012: SQLite database and Alembic config.
        tmp_path: Per-test temporary directory.

    Returns:
        None.
    """
    migration_engine, cfg = db_at_0012
    command.upgrade(cfg, "0013")
    metadata_path = tmp_path / "metadata_shadow.db"
    metadata_engine = sa.create_engine(f"sqlite:///{metadata_path}", future=True)
    try:
        Base.metadata.create_all(metadata_engine)
        assert _table_signature(migration_engine) == _table_signature(metadata_engine)
    finally:
        metadata_engine.dispose()


def test_0013_downgrade_drops_shadow_candles(
    db_at_0012: tuple[sa.Engine, Config],
) -> None:
    """Downgrade removes shadow_candles.

    Given: A database upgraded to 0013,
    When: it is downgraded to 0012,
    Then: the shadow_candles table is gone.

    Args:
        db_at_0012: SQLite database and Alembic config.

    Returns:
        None.
    """
    engine, cfg = db_at_0012
    command.upgrade(cfg, "0013")
    command.downgrade(cfg, "0012")
    assert "shadow_candles" not in sa.inspect(engine).get_table_names()
