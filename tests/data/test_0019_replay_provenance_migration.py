"""Tests for the 0019 trade_commands replay-provenance migration.

Verifies the origin/window columns land with the CHECK, that the
conservative backlog classification marks ONLY still-pending paper
strategy submits as replay, and that downgrade removes everything.
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


def _insert_command(
    engine: sa.Engine,
    *,
    public_id: str,
    mode: str = "paper",
    status: str = "created",
    command_type: str = "submit",
    plan_public_id: str | None = None,
    known_to: str = _ACTIVE,
) -> None:
    """Insert one pre-0019 trade command row via literal SQL.

    Args:
        engine: Engine bound to the migrated database.
        public_id: Stable command identity.
        mode: Execution mode column.
        status: Lifecycle status column.
        command_type: Command vocabulary entry.
        plan_public_id: Plan link (NULL marks strategy emits).
        known_to: SCD2 close marker (active by default).
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO trade_commands (public_id, command_type, shard_key, "
                "wallet_public_id, exchange, instrument, mode, strategy_id, "
                "client_order_id, venue_client_id, side, order_type, quantity, "
                "status, attempt_count, created_at, correlation_id, "
                "plan_public_id, session_id, sequence_id, timestamp, known_to) "
                "VALUES (:pid, :ctype, 'paper.BTC-USD.paper', 'wal-1', 'paper', "
                "'BTC-USD', :mode, 'engine-buy', :pid, :pid, 'buy', 'market', "
                "1.0, :status, 0, :ts, :pid, :plan, 'sess-1', 1, :ts, :kt)"
            ),
            {
                "pid": public_id,
                "ctype": command_type,
                "mode": mode,
                "status": status,
                "plan": plan_public_id,
                "ts": _TS,
                "kt": known_to,
            },
        )


def _origin_of(engine: sa.Engine, public_id: str) -> str:
    """Read back the migrated origin of one command row.

    Args:
        engine: Engine bound to the migrated database.
        public_id: Command identity to read.

    Returns:
        The origin column value.
    """
    with engine.begin() as conn:
        value: str = conn.execute(
            sa.text("SELECT origin FROM trade_commands WHERE public_id = :pid"),
            {"pid": public_id},
        ).scalar_one()
    return str(value)


@pytest.fixture
def pre_provenance_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database migrated to 0018 (pre-provenance).

    Args:
        tmp_path: Pytest-provided temporary directory.

    Yields:
        Tuple of engine bound to the database and the config.
    """
    db_path = tmp_path / "replay_provenance.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0018")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestReplayProvenanceMigration:
    """Upgrade / backlog classification / downgrade behaviours."""

    def test_pending_paper_strategy_backlog_marked_replay(
        self, pre_provenance_db: tuple[sa.Engine, Config]
    ) -> None:
        """Only still-pending paper strategy submits become replay.

        Given: a pre-0019 backlog with a pending paper strategy submit,
            a plan-linked paper submit, a live submit, a terminal paper
            submit, and a paper cancel,
        When: the migration runs,
        Then: ONLY the pending plan-less paper submit is classified
            replay — every other row keeps the live default.
        """
        engine, cfg = pre_provenance_db
        _insert_command(engine, public_id="cmd-backlog")
        _insert_command(engine, public_id="cmd-planned", plan_public_id="plan-1")
        _insert_command(engine, public_id="cmd-live", mode="live")
        _insert_command(engine, public_id="cmd-done", status="filled")
        _insert_command(engine, public_id="cmd-cancel", command_type="cancel")
        command.upgrade(cfg, "head")
        assert _origin_of(engine, "cmd-backlog") == "replay"
        assert _origin_of(engine, "cmd-planned") == "live"
        assert _origin_of(engine, "cmd-live") == "live"
        assert _origin_of(engine, "cmd-done") == "live"
        assert _origin_of(engine, "cmd-cancel") == "live"

    def test_closed_versions_stay_live(self, pre_provenance_db: tuple[sa.Engine, Config]) -> None:
        """Historical closed versions are never reclassified.

        Given: a CLOSED paper strategy submit version,
        When: the migration runs,
        Then: the closed row keeps the live default (only active rows
            can release into the guard).
        """
        engine, cfg = pre_provenance_db
        _insert_command(engine, public_id="cmd-closed", known_to="2026-07-11 09:00:00.000000")
        command.upgrade(cfg, "head")
        assert _origin_of(engine, "cmd-closed") == "live"

    def test_origin_check_rejects_unknown_value(
        self, pre_provenance_db: tuple[sa.Engine, Config]
    ) -> None:
        """The origin CHECK rejects vocabulary outside live/replay.

        Given: the migrated schema,
        When: a row inserts ``origin='backtest'``,
        Then: IntegrityError raises.
        """
        engine, cfg = pre_provenance_db
        command.upgrade(cfg, "head")
        rejected_origin_insert = sa.text(
            "INSERT INTO trade_commands (public_id, command_type, "
            "shard_key, wallet_public_id, exchange, instrument, mode, "
            "strategy_id, client_order_id, venue_client_id, side, "
            "order_type, quantity, status, attempt_count, created_at, "
            "correlation_id, session_id, sequence_id, timestamp, "
            "known_to, origin) VALUES ('cmd-bad', 'submit', "
            "'paper.BTC-USD.paper', 'wal-1', 'paper', 'BTC-USD', "
            "'paper', 'engine-buy', 'cid-bad', 'cid-bad', 'buy', "
            "'market', 1.0, 'created', 0, :ts, 'corr-bad', 'sess-1', "
            "1, :ts, :kt, 'backtest')"
        )
        with engine.begin() as conn, pytest.raises(sa.exc.IntegrityError):
            conn.execute(rejected_origin_insert, {"ts": _TS, "kt": _ACTIVE})

    def test_downgrade_removes_columns(self, pre_provenance_db: tuple[sa.Engine, Config]) -> None:
        """Downgrading one step removes the three provenance columns.

        Given: the migrated schema at head,
        When: ``alembic downgrade 0018`` runs,
        Then: origin and both window columns are gone.
        """
        engine, cfg = pre_provenance_db
        command.upgrade(cfg, "head")
        command.downgrade(cfg, "0018")
        with engine.begin() as conn:
            cols = {
                row[1] for row in conn.execute(sa.text("PRAGMA table_info(trade_commands)")).all()
            }
        assert not {"origin", "replay_window_start", "replay_window_end"} & cols


def test_paired_safety_commands_stay_live(
    pre_provenance_db: tuple[sa.Engine, Config],
) -> None:
    """Paired safety machinery is exempt from backlog classification.

    Given: a pending plan-less paper submit whose idempotency key marks
        it as a guard-scanner FLATTEN (an exposure-REDUCING safety
        command),
    When: the migration runs,
    Then: it keeps the live default — rejecting a flatten pre-venue
        would be the opposite of safety.
    """
    engine, cfg = pre_provenance_db
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO trade_commands (public_id, command_type, shard_key, "
                "wallet_public_id, exchange, instrument, mode, strategy_id, "
                "client_order_id, venue_client_id, side, order_type, quantity, "
                "status, attempt_count, created_at, correlation_id, "
                "idempotency_key, session_id, sequence_id, timestamp, known_to) "
                "VALUES ('cmd-flatten', 'submit', 'paper.BTC-USD.paper', 'wal-1', "
                "'paper', 'BTC-USD', 'paper', 'guard-flatten', 'cid-fl', 'cid-fl', "
                "'sell', 'market', 1.0, 'created', 0, :ts, 'corr-fl', "
                "'paired:grp-1:leg-1:flatten:0', 'sess-1', 1, :ts, :kt)"
            ),
            {"ts": _TS, "kt": _ACTIVE},
        )
    command.upgrade(cfg, "head")
    assert _origin_of(engine, "cmd-flatten") == "live"
