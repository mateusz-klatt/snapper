"""Tests for the 0003 paired-execution guard migration.

Verifies that ``alembic upgrade head`` on a fresh SQLite database creates
the three SCD2 paired-execution tables with their active-row unique
indexes and immutable-enum CHECK constraints enforced, adds the
denormalised ``paired_group_id`` columns to ``signals`` and
``venue_events``, and that the downgrade removes all of it cleanly.
"""

from collections.abc import Iterator
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


def _names(engine: sa.Engine, kind: str, like: str) -> set[str]:
    """Return sqlite_master object names of a kind matching a LIKE pattern."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text("SELECT name FROM sqlite_master WHERE type = :kind AND name LIKE :like"),
            {"kind": kind, "like": like},
        ).all()
    return {row[0] for row in rows}


def _column_names(engine: sa.Engine, table: str) -> set[str]:
    """Return the column names of a table via PRAGMA table_info."""
    with engine.begin() as conn:
        rows = conn.execute(sa.text(f"PRAGMA table_info({table})")).all()
    return {row[1] for row in rows}


def _insert_leg(
    engine: sa.Engine,
    *,
    public_id: str,
    group_public_id: str = "grp-1",
    leg_index: int = 0,
    command_public_id: str | None = None,
    side: str = "buy",
    mode: str = "live",
) -> None:
    """Insert one active paired-execution leg row via literal SQL."""
    ts = "2026-06-08 12:00:00.000000"
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO paired_execution_legs (public_id, group_public_id, "
                "leg_index, exchange, mode, instrument, shard_key, side, "
                "target_qty, signal_public_id, command_public_id, status, "
                "filled_signed_qty, compensated_signed_qty, compensation_seq, "
                "wallet_public_id, created_at, session_id, sequence_id, "
                "timestamp, known_to) VALUES (:pid, :gid, :idx, 'kraken', :mode, "
                "'BTC-USD', 'kraken.BTC-USD.live', :side, 1.0, 'sig-1', :cid, "
                "'pending', 0, 0, 0, 'wal-1', :ts, 'sess-1', 1, :ts, :active)"
            ),
            {
                "pid": public_id,
                "gid": group_public_id,
                "idx": leg_index,
                "mode": mode,
                "side": side,
                "cid": command_public_id,
                "ts": ts,
                "active": "9999-12-31 23:59:59.000000",
            },
        )


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "paired_execution.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestPairedExecutionGuardMigration:
    """Upgrade / constraint / downgrade behaviours for the 0003 schema."""

    def test_upgrade_creates_tables_and_indexes(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """A freshly-migrated DB carries the three tables and their indexes."""
        engine, _ = migrated_db
        assert _names(engine, "table", "paired_%") == {
            "paired_execution_groups",
            "paired_execution_legs",
            "paired_execution_halts",
        }
        assert {"ix_peg_public_id", "ix_peg_status", "ix_peg_group_key"} <= _names(
            engine, "index", "ix_peg_%"
        )
        assert {"uq_pel_group_leg", "uq_pel_command"} <= _names(engine, "index", "uq_pel_%")

    def test_signals_and_venue_events_gain_group_column(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Both event tables gain the denormalised paired_group_id column."""
        engine, _ = migrated_db
        assert "paired_group_id" in _column_names(engine, "signals")
        assert "paired_group_id" in _column_names(engine, "venue_events")

    def test_active_group_leg_pair_is_unique(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """A second active leg on the same (group, leg_index) is rejected."""
        engine, _ = migrated_db
        _insert_leg(engine, public_id="leg-1", leg_index=0)
        with pytest.raises(sa.exc.IntegrityError):
            _insert_leg(engine, public_id="leg-2", leg_index=0)

    def test_closed_leg_does_not_block_reactivation(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """A closed historical leg version does not block a new active leg.

        The (group_public_id, leg_index) unique index is scoped to active
        rows (``known_to = KNOWN_TO_MAX``), so SCD2 close-and-insert and
        re-opening the same pair after a terminal group never trip the
        constraint. All the other new partial unique indexes share the
        identical active-row predicate.
        """
        engine, _ = migrated_db
        _insert_leg(engine, public_id="leg-1", group_public_id="grp-1", leg_index=0)
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "UPDATE paired_execution_legs SET known_to = :closed WHERE public_id = :pid"
                ),
                {"closed": "2026-06-08 12:00:01.000000", "pid": "leg-1"},
            )
        _insert_leg(engine, public_id="leg-2", group_public_id="grp-1", leg_index=0)
        with engine.begin() as conn:
            active = conn.execute(
                sa.text(
                    "SELECT COUNT(*) FROM paired_execution_legs "
                    "WHERE group_public_id = 'grp-1' AND leg_index = 0 "
                    "AND known_to = '9999-12-31 23:59:59.000000'"
                )
            ).scalar_one()
        assert active == 1

    def test_null_command_legs_escape_the_partial_unique(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Legs without a command id are exempt from the command unique index.

        The partial unique index only constrains non-null ``command_public_id``,
        so two legs with a null command id coexist while a duplicate non-null
        command id is rejected.
        """
        engine, _ = migrated_db
        _insert_leg(engine, public_id="leg-1", leg_index=0, command_public_id=None)
        _insert_leg(engine, public_id="leg-2", leg_index=1, command_public_id=None)
        _insert_leg(engine, public_id="leg-3", leg_index=2, command_public_id="cmd-1")
        with pytest.raises(sa.exc.IntegrityError):
            _insert_leg(engine, public_id="leg-4", leg_index=3, command_public_id="cmd-1")

    def test_leg_side_check_rejects_unknown_value(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """The immutable side CHECK rejects a non buy/sell value."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_leg(engine, public_id="leg-bad", side="hold")

    def test_leg_mode_check_rejects_unknown_value(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """The immutable mode CHECK rejects a non live/paper value."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_leg(engine, public_id="leg-bad", mode="margin")

    def test_downgrade_removes_tables_and_columns(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Downgrading one step removes the tables and the group columns."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "0002")
        assert _names(engine, "table", "paired_%") == set()
        assert "paired_group_id" not in _column_names(engine, "signals")
        assert "paired_group_id" not in _column_names(engine, "venue_events")
