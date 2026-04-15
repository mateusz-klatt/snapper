"""Tests for migration 0004_backtest_single_running.

Verifies the partial unique index ``uq_bt_single_running`` ensures at
most one backtest_runs row may carry status='running' at known_to=MAX,
that closed (SCD2) rows do not block follow-up runs, and that the
downgrade path removes the index.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
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


def _insert_run(
    engine: sa.Engine,
    *,
    status: str,
    known_to: str = KNOWN_TO_MAX_LITERAL,
) -> str:
    """Insert one backtest_runs row with the supplied status / known_to.

    ``known_to`` is passed as a literal string so the test row matches
    the partial-index predicate exactly (the predicate was written
    against the canonical SQLite datetime string SA emits, which
    includes a microsecond suffix).
    """
    public_id = str(uuid7())
    now = datetime.now(UTC).isoformat(sep=" ")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO backtest_runs (public_id, session_id, sequence_id, "
                "timestamp, known_to, wallet_public_id, strategy_name, "
                "strategy_params, instrument_public_id, exchange, mode, timeframe, "
                "start_date, end_date, initial_cash, status, execution_mode, "
                "fill_model, slippage_bps, commission_bps) VALUES (:public_id, "
                ":session_id, 1, :ts, :known_to, :wallet, 'sma_cross', '{}', :instr, "
                "'kraken', 'paper', '1h', :sd, :ed, 10000.0, :status, 'direct_db', "
                "'market', 0.0, 0.0)"
            ),
            {
                "public_id": public_id,
                "session_id": str(uuid7()),
                "ts": now,
                "known_to": known_to,
                "wallet": str(uuid7()),
                "instr": str(uuid7()),
                "sd": now,
                "ed": now,
                "status": status,
            },
        )
    return public_id


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "single_running.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


def _index_exists(engine: sa.Engine) -> bool:
    """Return True iff the uq_bt_single_running partial index is present."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='uq_bt_single_running'"
            )
        ).all()
    return len(rows) == 1


class TestSingleRunningMigration:
    """Behaviours of the partial unique index across upgrade and downgrade."""

    def test_upgrade_creates_partial_index(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Index exists after upgrade."""
        engine, _ = migrated_db
        assert _index_exists(engine)

    def test_two_running_rows_at_max_is_rejected(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Second running row at known_to=MAX raises IntegrityError."""
        engine, _ = migrated_db
        _insert_run(engine, status="running")
        with pytest.raises(sa.exc.IntegrityError):
            _insert_run(engine, status="running")

    def test_closed_row_does_not_block_new_running_row(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """An SCD2-closed running row does not occupy the partial index slot."""
        engine, _ = migrated_db
        closed_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat(sep=" ")
        _insert_run(engine, status="running", known_to=closed_at)
        _insert_run(engine, status="running")

    def test_completed_and_failed_rows_unconstrained(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Non-running rows at known_to=MAX coexist freely."""
        engine, _ = migrated_db
        _insert_run(engine, status="completed")
        _insert_run(engine, status="completed")
        _insert_run(engine, status="failed")
        _insert_run(engine, status="cancelled")
        _insert_run(engine, status="pending")

    def test_downgrade_drops_index_and_allows_dual_running_rows(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """After downgrade two running rows are accepted again."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "0003")
        assert not _index_exists(engine)
        _insert_run(engine, status="running")
        _insert_run(engine, status="running")
