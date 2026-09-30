"""Tests for the 0002 ``instrument_feed_health`` table migration.

Verifies that ``alembic upgrade head`` on a fresh SQLite database
creates the current-state ``instrument_feed_health`` table with its
natural-key unique constraint enforced and its exchange index present,
and that the downgrade drops the table cleanly.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_row(
    engine: sa.Engine,
    *,
    coordinator: str = "coord-0",
    exchange: str = "kraken",
    channel: str = "ticker",
    symbol: str = "BTC/USD",
) -> None:
    """Insert one feed-health row via literal SQL to exercise the schema."""
    now = datetime.now(UTC).isoformat(sep=" ")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO instrument_feed_health (coordinator, exchange, channel, "
                "symbol, status, requested_at, retry_count, snapshot_at) "
                "VALUES (:coordinator, :exchange, :channel, :symbol, 'confirmed', "
                ":ts, 0, :ts)"
            ),
            {
                "coordinator": coordinator,
                "exchange": exchange,
                "channel": channel,
                "symbol": symbol,
                "ts": now,
            },
        )


def _table_exists(engine: sa.Engine) -> bool:
    """Return True iff the ``instrument_feed_health`` table is present."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name='instrument_feed_health'"
            )
        ).all()
    return len(rows) == 1


def _index_exists(engine: sa.Engine) -> bool:
    """Return True iff the exchange index is present on SQLite."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='ix_instrument_feed_health_exchange'"
            )
        ).all()
    return len(rows) == 1


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


class TestInstrumentFeedHealthMigration:
    """Upgrade / constraint / downgrade behaviours for the table."""

    def test_upgrade_creates_table_and_index(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """A freshly-migrated DB carries the table and its exchange index."""
        engine, _ = migrated_db
        assert _table_exists(engine)
        assert _index_exists(engine)

    def test_natural_key_is_unique(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """A second row on the same natural key is rejected by the constraint."""
        engine, _ = migrated_db
        _insert_row(engine)
        with pytest.raises(sa.exc.IntegrityError):
            _insert_row(engine)

    def test_distinct_keys_are_allowed(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Differing only in coordinator yields two distinct rows."""
        engine, _ = migrated_db
        _insert_row(engine, coordinator="coord-0")
        _insert_row(engine, coordinator="coord-1")
        with engine.begin() as conn:
            count: int = conn.execute(
                sa.text("SELECT COUNT(*) FROM instrument_feed_health")
            ).scalar_one()
        assert count == 2

    def test_downgrade_drops_table(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Downgrading one step removes the table again."""
        engine, cfg = migrated_db
        assert _table_exists(engine)
        command.downgrade(cfg, "0001")
        assert not _table_exists(engine)
