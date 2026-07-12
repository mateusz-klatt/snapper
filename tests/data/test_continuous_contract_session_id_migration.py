"""Tests for the squashed ``continuous_contract_configs.session_id`` schema.

Verifies the column is ``VARCHAR(36)`` at head and remains stable across
downgrade/upgrade calls within the squashed migration chain.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
KNOWN_TO_MAX_LITERAL = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _column_type(engine: sa.Engine, table: str, column: str) -> str:
    """Return the SQLite-reported column type for the given table + column."""
    with engine.begin() as conn:
        rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).all()
    for row in rows:
        if row[1] == column:
            return str(row[2])
    raise AssertionError(f"column {column} not found in table {table}")


def _insert_config(
    engine: sa.Engine,
    *,
    session_id: str,
    known_to: str = KNOWN_TO_MAX_LITERAL,
) -> str:
    """Insert a continuous_contract_configs row with the given session_id."""
    public_id = str(uuid7())
    now = datetime.now(UTC).isoformat(sep=" ")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO continuous_contract_configs (public_id, session_id, "
                "sequence_id, timestamp, known_to, underlying_public_id, exchange, "
                "contract_family, method, rollover_days_before) "
                "VALUES (:public_id, :session_id, 1, :ts, :known_to, :under, "
                "'kraken_equities', 'ES', 'panama', 0)"
            ),
            {
                "public_id": public_id,
                "session_id": session_id,
                "ts": now,
                "known_to": known_to,
                "under": str(uuid7()),
            },
        )
    return public_id


@pytest.fixture
def migrated_db(migrated_db_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration.

    The schema comes from the worker's session-scoped Alembic template
    (one real ``upgrade head`` per worker); the copy is private to this
    test, so constraint probes and downgrades cannot leak across tests.
    """
    db_url = f"sqlite:///{migrated_db_path}"
    cfg = _make_alembic_config(db_url)
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestContinuousContractSessionIdMigration:
    """Schema behaviours across upgrade / downgrade / round-trip."""

    def test_upgrade_narrows_column_type(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """After upgrade the column type is VARCHAR(36)."""
        engine, _ = migrated_db
        col_type = _column_type(engine, "continuous_contract_configs", "session_id")
        assert col_type.upper() == "VARCHAR(36)"

    def test_downgrade_to_0001_keeps_varchar_36(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Downgrade to revision 0001 is a no-op in the squashed chain."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "0001")
        col_type = _column_type(engine, "continuous_contract_configs", "session_id")
        assert col_type.upper() == "VARCHAR(36)"

    def test_round_trip_flips_type_back_and_forth(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Upgrade -> downgrade -> upgrade leaves the column at VARCHAR(36)."""
        engine, cfg = migrated_db
        assert (
            _column_type(engine, "continuous_contract_configs", "session_id").upper()
            == "VARCHAR(36)"
        )
        command.downgrade(cfg, "0001")
        assert (
            _column_type(engine, "continuous_contract_configs", "session_id").upper()
            == "VARCHAR(36)"
        )
        command.upgrade(cfg, "head")
        assert (
            _column_type(engine, "continuous_contract_configs", "session_id").upper()
            == "VARCHAR(36)"
        )

    def test_thirty_six_char_session_id_roundtrips(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """A 36-char session_id inserts cleanly after upgrade."""
        engine, _ = migrated_db
        session_id = str(uuid7())
        assert len(session_id) == 36
        _insert_config(engine, session_id=session_id)
        with engine.begin() as conn:
            rows = conn.execute(sa.text("SELECT session_id FROM continuous_contract_configs")).all()
        assert len(rows) == 1
        assert rows[0][0] == session_id

    def test_oversize_session_id_roundtrips_in_sqlite(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """SQLite stores oversize text values even when declared as VARCHAR(36)."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "0001")
        oversize_id = "x" * 50
        _insert_config(engine, session_id=oversize_id)
        command.upgrade(cfg, "head")

        with engine.begin() as conn:
            rows = conn.execute(sa.text("SELECT session_id FROM continuous_contract_configs")).all()
        assert len(rows) == 1
        assert rows[0][0] == oversize_id
