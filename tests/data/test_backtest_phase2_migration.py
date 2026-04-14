"""Tests for migration 0003_backtest_phase2_fields."""

from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
PHASE2_FIELDS = ("execution_mode", "fill_model", "slippage_bps", "commission_bps")


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _columns(engine: sa.Engine, table: str) -> set[str]:
    """Return the column names present on the given table."""
    inspector = sa.inspect(engine)
    return {col["name"] for col in inspector.get_columns(table)}


def _insert_minimal_run(
    engine: sa.Engine,
    *,
    execution_mode: str = "direct_db",
    fill_model: str = "market",
    slippage_bps: float = 0.0,
    commission_bps: float = 0.0,
) -> None:
    """Insert one backtest_runs row exercising the new fields."""
    now = datetime.now(UTC)
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO backtest_runs (public_id, session_id, sequence_id, "
                "timestamp, known_to, wallet_public_id, strategy_name, strategy_params, "
                "instrument_public_id, exchange, mode, timeframe, start_date, end_date, "
                "initial_cash, status, execution_mode, fill_model, slippage_bps, "
                "commission_bps) VALUES (:public_id, :session_id, 1, :ts, :known_to, "
                ":wallet, 'sma_cross', '{}', :instr, 'kraken', 'paper', '1h', :sd, :ed, "
                "10000.0, 'pending', :em, :fm, :sl, :cm)"
            ),
            {
                "public_id": str(uuid7()),
                "session_id": str(uuid7()),
                "ts": now,
                "known_to": datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
                "wallet": str(uuid7()),
                "instr": str(uuid7()),
                "sd": now,
                "ed": now,
                "em": execution_mode,
                "fm": fill_model,
                "sl": slippage_bps,
                "cm": commission_bps,
            },
        )


@pytest.fixture
def migrated_db(tmp_path: Path) -> tuple[sa.Engine, Config]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "phase2_migration.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    return engine, cfg


class TestPhase2MigrationUpgrade:
    """Behaviours exposed by the 0003 upgrade path."""

    def test_adds_four_phase2_columns(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """All four new columns appear on backtest_runs."""
        engine, _ = migrated_db
        cols = _columns(engine, "backtest_runs")
        for field in PHASE2_FIELDS:
            assert field in cols

    def test_default_row_persists_phase2_defaults(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Inserting a default row stores direct_db/market/0/0 on read-back."""
        engine, _ = migrated_db
        _insert_minimal_run(engine)
        with engine.connect() as conn:
            row = conn.execute(
                sa.text(
                    "SELECT execution_mode, fill_model, slippage_bps, commission_bps "
                    "FROM backtest_runs"
                )
            ).one()
        assert row.execution_mode == "direct_db"
        assert row.fill_model == "market"
        assert row.slippage_bps == 0.0
        assert row.commission_bps == 0.0

    def test_zmq_replay_value_accepted(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """zmq_replay is accepted by the execution_mode CHECK constraint."""
        engine, _ = migrated_db
        _insert_minimal_run(engine, execution_mode="zmq_replay")
        with engine.connect() as conn:
            stored = conn.execute(sa.text("SELECT execution_mode FROM backtest_runs")).scalar_one()
        assert stored == "zmq_replay"

    def test_invalid_execution_mode_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Unknown execution_mode triggers the CHECK constraint."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_minimal_run(engine, execution_mode="other")

    def test_invalid_fill_model_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Unknown fill_model triggers the CHECK constraint."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_minimal_run(engine, fill_model="midpoint")

    def test_negative_slippage_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Negative slippage_bps triggers the CHECK constraint."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_minimal_run(engine, slippage_bps=-1.0)

    def test_slippage_above_cap_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """slippage_bps > 500 triggers the CHECK constraint."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_minimal_run(engine, slippage_bps=501.0)

    def test_commission_above_cap_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """commission_bps > 500 triggers the CHECK constraint."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_minimal_run(engine, commission_bps=600.0)


class TestPhase2MigrationDowngrade:
    """Downgrade reverses both columns and constraints."""

    def test_downgrade_drops_phase2_columns(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """After downgrade -1, the 4 columns disappear from backtest_runs."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "-1")
        cols = _columns(engine, "backtest_runs")
        for field in PHASE2_FIELDS:
            assert field not in cols
