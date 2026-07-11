"""Tests for the 0016 trade_commands lineage/notional migration.

Verifies that ``alembic upgrade head`` on a fresh SQLite database adds
the two nullable lineage columns (``signal_public_id``,
``ai_review_public_id``) with their lookup indexes plus the nullable
``submitted_notional_usd`` column to ``trade_commands``, and that
downgrading one step removes all three columns and both indexes.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


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


def _index_names(engine: sa.Engine, like: str) -> set[str]:
    """Return sqlite_master index names matching a LIKE pattern.

    Args:
        engine: Engine bound to the migrated SQLite database.
        like: SQL LIKE pattern for the index name.

    Returns:
        Set of matching index names.
    """
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text("SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE :like"),
            {"like": like},
        ).all()
    return {row[0] for row in rows}


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Yields:
        Tuple of engine bound to the migrated database and the config.
    """
    db_path = tmp_path / "lineage_notional.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestTradeCommandsLineageMigration:
    """Upgrade / downgrade behaviours for the 0016 schema delta."""

    def test_upgrade_adds_columns(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """The three Phase 1 columns exist after upgrade.

        Given: a fresh SQLite database,
        When: ``alembic upgrade head`` runs,
        Then: ``trade_commands`` carries ``signal_public_id``,
            ``ai_review_public_id``, and ``submitted_notional_usd``.
        """
        engine, _ = migrated_db
        cols = _column_names(engine, "trade_commands")
        assert {"signal_public_id", "ai_review_public_id", "submitted_notional_usd"} <= cols

    def test_upgrade_creates_lineage_indexes(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Both lineage columns gain their lookup indexes.

        Given: a freshly-migrated database,
        When: sqlite_master is inspected,
        Then: ``ix_trade_commands_signal_public_id`` and
            ``ix_trade_commands_ai_review_public_id`` exist.
        """
        engine, _ = migrated_db
        names = _index_names(engine, "ix_trade_commands_%public_id")
        assert {
            "ix_trade_commands_signal_public_id",
            "ix_trade_commands_ai_review_public_id",
        } <= names

    def test_columns_accept_null_and_values(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """New columns are nullable and round-trip inserted values.

        Given: the migrated schema,
        When: one row inserts NULLs and another inserts concrete
            lineage ids plus a cent-quantized notional,
        Then: both inserts succeed and read back verbatim.
        """
        engine, _ = migrated_db
        ts = "2026-07-11 08:00:00.000000"
        active = "9999-12-31 23:59:59.000000"
        base = (
            "INSERT INTO trade_commands (public_id, command_type, shard_key, "
            "wallet_public_id, exchange, instrument, mode, strategy_id, "
            "client_order_id, venue_client_id, side, order_type, quantity, "
            "status, attempt_count, created_at, correlation_id, session_id, "
            "sequence_id, timestamp, known_to, signal_public_id, "
            "ai_review_public_id, submitted_notional_usd) VALUES "
            "(:pid, 'submit', 'kraken.BTC-USD.live', 'wal-1', 'kraken', "
            "'BTC-USD', 'live', 'engine-buy', :cid, :cid, 'buy', 'market', "
            "1.0, 'created', 0, :ts, :pid, 'sess-1', 1, :ts, :active, "
            ":sig, :rev, :notional)"
        )
        with engine.begin() as conn:
            conn.execute(
                sa.text(base),
                {
                    "pid": "cmd-null",
                    "cid": "cid-1",
                    "ts": ts,
                    "active": active,
                    "sig": None,
                    "rev": None,
                    "notional": None,
                },
            )
            conn.execute(
                sa.text(base),
                {
                    "pid": "cmd-full",
                    "cid": "cid-2",
                    "ts": ts,
                    "active": active,
                    "sig": "sig-1",
                    "rev": "rev-1",
                    "notional": 64181.31,
                },
            )
            row = conn.execute(
                sa.text(
                    "SELECT signal_public_id, ai_review_public_id, "
                    "submitted_notional_usd FROM trade_commands "
                    "WHERE public_id = 'cmd-full'"
                )
            ).one()
        assert row[0] == "sig-1"
        assert row[1] == "rev-1"
        assert float(row[2]) == 64181.31

    def test_downgrade_removes_columns_and_indexes(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Downgrading one step removes the columns and both indexes.

        Given: the migrated schema at head,
        When: ``alembic downgrade 0015`` runs,
        Then: the three columns and the two lineage indexes are gone.
        """
        engine, cfg = migrated_db
        command.downgrade(cfg, "0015")
        cols = _column_names(engine, "trade_commands")
        assert (
            not {
                "signal_public_id",
                "ai_review_public_id",
                "submitted_notional_usd",
            }
            & cols
        )
        names = _index_names(engine, "ix_trade_commands_%public_id")
        assert "ix_trade_commands_signal_public_id" not in names
        assert "ix_trade_commands_ai_review_public_id" not in names
