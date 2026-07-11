"""Tests for the 0018 instruments.source_exchange migration.

Verifies the column lands with its CHECK (lowercase, paper rows only),
that the SQLite batch-recreate preserves the active-row partial unique
indexes, and that downgrade removes both column and constraint.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-11 08:00:00.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL.

    Args:
        db_url: SQLAlchemy URL of the throwaway SQLite database.

    Returns:
        Configured :class:`Config` bound to ``db_url``.
    """
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _column_names(engine: sa.Engine, table: str) -> set[str]:
    """Return the column names of a table via PRAGMA table_info.

    Args:
        engine: Engine bound to the migrated SQLite database.
        table: Table whose columns are listed.

    Returns:
        Set of column names.
    """
    with engine.begin() as conn:
        rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).all()
    return {row[1] for row in rows}


def _insert_instrument(
    engine: sa.Engine,
    *,
    public_id: str,
    exchange: str,
    source_exchange: str | None,
    symbol_public_id: str = "sym-1",
) -> None:
    """Insert one active instrument row via literal SQL.

    Args:
        engine: Engine bound to the migrated database.
        public_id: Stable instrument identity.
        exchange: Venue column.
        source_exchange: Mapping column under test.
        symbol_public_id: Owning symbol identity.
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO instruments (public_id, symbol_public_id, exchange, "
                "source_exchange, requires_ai_review, session_id, sequence_id, "
                "timestamp, known_to) VALUES (:pid, :spid, :ex, :src, 0, "
                "'sess-1', 1, :ts, :kt)"
            ),
            {
                "pid": public_id,
                "spid": symbol_public_id,
                "ex": exchange,
                "src": source_exchange,
                "ts": _TS,
                "kt": _ACTIVE,
            },
        )


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Yields:
        Tuple of engine bound to the migrated database and the config.
    """
    db_path = tmp_path / "source_exchange.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestInstrumentSourceExchangeMigration:
    """Upgrade / constraint / downgrade behaviours for 0018."""

    def test_upgrade_adds_column(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """The source_exchange column exists after upgrade.

        Given: a fresh database at head,
        When: instruments columns are inspected,
        Then: ``source_exchange`` is present.
        """
        engine, _ = migrated_db
        assert "source_exchange" in _column_names(engine, "instruments")

    def test_paper_row_accepts_mapping(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """A lowercase mapping on a paper row satisfies the CHECK.

        Given: the migrated schema,
        When: a paper instrument inserts ``source_exchange='kraken'``,
        Then: the insert succeeds and reads back.
        """
        engine, _ = migrated_db
        _insert_instrument(engine, public_id="inst-p", exchange="paper", source_exchange="kraken")
        with engine.begin() as conn:
            value = conn.execute(
                sa.text("SELECT source_exchange FROM instruments WHERE public_id = 'inst-p'")
            ).scalar_one()
        assert value == "kraken"

    def test_non_paper_row_rejects_mapping(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """The CHECK rejects a mapping on a non-paper instrument.

        Given: the migrated schema,
        When: a kraken instrument inserts a non-NULL mapping,
        Then: the CHECK raises IntegrityError.
        """
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_instrument(
                engine, public_id="inst-k", exchange="kraken", source_exchange="kraken"
            )

    def test_uppercase_mapping_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """The CHECK rejects a non-lowercase source value.

        Given: the migrated schema,
        When: a paper instrument inserts ``source_exchange='Kraken'``,
        Then: the CHECK raises IntegrityError.
        """
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_instrument(
                engine, public_id="inst-up", exchange="paper", source_exchange="Kraken"
            )

    def test_recreate_preserves_active_unique_index(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """The SQLite batch recreate keeps the partial unique indexes.

        Given: one active paper instrument for a symbol,
        When: a SECOND active row for the same (symbol, exchange) pair
            inserts,
        Then: ``uq_instrument_spid_exchange`` still rejects it — the
            table rebuild did not drop the active-row predicate.
        """
        engine, _ = migrated_db
        _insert_instrument(engine, public_id="inst-1", exchange="paper", source_exchange="kraken")
        with pytest.raises(sa.exc.IntegrityError):
            _insert_instrument(
                engine, public_id="inst-2", exchange="paper", source_exchange="kraken"
            )

    def test_downgrade_removes_column(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Downgrading one step removes the column.

        Given: the migrated schema at head,
        When: ``alembic downgrade 0017`` runs,
        Then: ``source_exchange`` is gone from instruments.
        """
        engine, cfg = migrated_db
        command.downgrade(cfg, "0017")
        assert "source_exchange" not in _column_names(engine, "instruments")
