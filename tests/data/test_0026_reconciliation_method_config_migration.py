"""Tests for reconciliation-method configuration migration 0026."""

import importlib
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from snapper.data import models
from snapper.data.models import PortfolioReconciliationMethodConfig
from snapper.data.models import PortfolioReconciliationObservation
from snapper.data.models import PortfolioReconciliationState
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-14 08:00:00.000000"
_TS_LATER = "2026-07-15 08:00:00.000000"
_LARGE_WATERMARK = 2_147_483_649


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _uuid() -> str:
    """Return one database-safe public identifier."""
    return str(uuid4())


def _insert_observation(
    engine: sa.Engine,
    *,
    method: str | None,
    evaluation_status: str,
    sequence_id: int,
    wallet_public_id: str | None = None,
    anchor_public_id: str | None = None,
    retained_evidence: bool = False,
    source_watermark: int | None = None,
) -> str:
    """Insert one reconciliation observation through literal SQL."""
    public_id = _uuid()
    full = evaluation_status in {"matched", "mismatched"}
    carries_evidence = full or retained_evidence
    watermark = source_watermark
    if watermark is None and carries_evidence:
        watermark = sequence_id
    source_watermark_kind = None
    if watermark is not None:
        source_watermark_kind = "venue_sequence"
    mismatch_count = 1 if evaluation_status == "mismatched" else 0
    evidence_json = "{}" if carries_evidence else None
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO portfolio_reconciliation_observations "
                "(wallet_public_id, exchange, mode, method, evaluation_status, "
                "venue_account_state_public_id, venue_account_observation_id, "
                "account_authoritative_until, source_watermark_kind, source_watermark, "
                "anchor_public_id, expected_json, actual_json, difference_json, tolerance_json, "
                "resulting_full_mismatch_count, drift_episode_public_id, error, public_id, "
                "session_id, sequence_id, timestamp, known_to) VALUES "
                "(:wallet_public_id, 'kraken', 'live', :method, :evaluation_status, "
                ":venue_account_state_public_id, :venue_account_observation_id, "
                ":account_authoritative_until, :source_watermark_kind, :source_watermark, "
                ":anchor_public_id, :expected_json, :actual_json, :difference_json, "
                ":tolerance_json, :mismatch_count, NULL, :error, :public_id, :session_id, "
                ":sequence_id, :timestamp, :known_to)"
            ),
            {
                "wallet_public_id": wallet_public_id or _uuid(),
                "method": method,
                "evaluation_status": evaluation_status,
                "venue_account_state_public_id": _uuid() if carries_evidence else None,
                "venue_account_observation_id": sequence_id if carries_evidence else None,
                "account_authoritative_until": _TS_LATER if carries_evidence else None,
                "source_watermark_kind": source_watermark_kind,
                "source_watermark": watermark,
                "anchor_public_id": anchor_public_id,
                "expected_json": evidence_json,
                "actual_json": evidence_json,
                "difference_json": evidence_json,
                "tolerance_json": evidence_json,
                "mismatch_count": mismatch_count,
                "error": "bounded_reason" if evaluation_status == "error" else None,
                "public_id": public_id,
                "session_id": _uuid(),
                "sequence_id": sequence_id,
                "timestamp": _TS,
                "known_to": _ACTIVE,
            },
        )
    return public_id


def _insert_state(
    engine: sa.Engine,
    *,
    method: str | None,
    current_evaluation_status: str,
    sequence_id: int,
    wallet_public_id: str | None = None,
    anchor_public_id: str | None = None,
    retained_evidence: bool = False,
    source_watermark: int | None = None,
) -> str:
    """Insert one reconciliation state through literal SQL."""
    public_id = _uuid()
    full = current_evaluation_status in {"matched", "mismatched"}
    carries_evidence = full or retained_evidence
    last_full_outcome = None
    if full:
        last_full_outcome = current_evaluation_status
    elif retained_evidence:
        last_full_outcome = "matched"
    current_observation_id = sequence_id if full else sequence_id + 10_000
    full_observation_id = sequence_id if carries_evidence else None
    evidence_json = "{}" if carries_evidence else None
    mismatch_count = 1 if last_full_outcome == "mismatched" else 0
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO portfolio_reconciliation_states "
                "(wallet_public_id, exchange, mode, method, current_evaluation_status, "
                "current_observation_id, last_full_observation_id, last_full_outcome, "
                "detail_source_observation_id, consecutive_full_mismatches, "
                "open_drift_episode_public_id, anchor_public_id, "
                "venue_account_state_public_id, venue_account_observation_id, "
                "source_watermark_kind, source_watermark, expected_json, actual_json, "
                "difference_json, tolerance_json, reconciled_at, authoritative_until, error, "
                "public_id, session_id, sequence_id, timestamp, known_to) VALUES "
                "(:wallet_public_id, 'kraken', 'live', :method, :current_status, "
                ":current_observation_id, :last_full_observation_id, :last_full_outcome, "
                ":detail_source_observation_id, :mismatch_count, NULL, :anchor_public_id, "
                ":venue_account_state_public_id, :venue_account_observation_id, "
                ":source_watermark_kind, :source_watermark, :expected_json, :actual_json, "
                ":difference_json, :tolerance_json, :reconciled_at, :authoritative_until, "
                ":error, :public_id, :session_id, :sequence_id, :timestamp, :known_to)"
            ),
            {
                "wallet_public_id": wallet_public_id or _uuid(),
                "method": method,
                "current_status": current_evaluation_status,
                "current_observation_id": current_observation_id,
                "last_full_observation_id": full_observation_id,
                "last_full_outcome": last_full_outcome,
                "detail_source_observation_id": full_observation_id,
                "mismatch_count": mismatch_count,
                "anchor_public_id": anchor_public_id,
                "venue_account_state_public_id": _uuid() if carries_evidence else None,
                "venue_account_observation_id": sequence_id if carries_evidence else None,
                "source_watermark_kind": "venue_sequence" if carries_evidence else None,
                "source_watermark": (
                    (source_watermark if source_watermark is not None else sequence_id)
                    if carries_evidence
                    else None
                ),
                "expected_json": evidence_json,
                "actual_json": evidence_json,
                "difference_json": evidence_json,
                "tolerance_json": evidence_json,
                "reconciled_at": _TS if carries_evidence else None,
                "authoritative_until": _TS_LATER if carries_evidence else None,
                "error": "bounded_reason" if current_evaluation_status == "error" else None,
                "public_id": public_id,
                "session_id": _uuid(),
                "sequence_id": sequence_id,
                "timestamp": _TS,
                "known_to": _ACTIVE,
            },
        )
    return public_id


def _insert_config(
    engine: sa.Engine,
    *,
    wallet_public_id: str,
    exchange: str,
    method: str,
    public_id: str,
    sequence_id: int,
    timestamp: str = _TS,
) -> None:
    """Insert one active reconciliation-method config row."""
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO portfolio_reconciliation_method_configs "
                "(wallet_public_id, exchange, mode, method, public_id, session_id, sequence_id, "
                "timestamp, known_to) VALUES "
                "(:wallet_public_id, :exchange, 'live', :method, :public_id, :session_id, "
                ":sequence_id, :timestamp, :known_to)"
            ),
            {
                "wallet_public_id": wallet_public_id,
                "exchange": exchange,
                "method": method,
                "public_id": public_id,
                "session_id": _uuid(),
                "sequence_id": sequence_id,
                "timestamp": timestamp,
                "known_to": _ACTIVE,
            },
        )


def test_0026_sqlite_upgrade_downgrade_reupgrade_preserves_evidence(
    tmp_path: Path,
) -> None:
    """SQLite batch recreation preserves old rows, indexes, and a 64-bit watermark."""
    db_url = f"sqlite:///{tmp_path / 'reconciliation-method-cycle.db'}"
    config = _config(db_url)
    command.upgrade(config, "0025")
    engine = sa.create_engine(db_url)
    futures_observation_id = _insert_observation(
        engine,
        method="futures_position",
        evaluation_status="incomplete",
        sequence_id=1,
        source_watermark=_LARGE_WATERMARK,
    )
    anchor_public_id = _uuid()
    spot_observation_id = _insert_observation(
        engine,
        method="spot_execution_replay",
        evaluation_status="matched",
        sequence_id=2,
        anchor_public_id=anchor_public_id,
    )
    futures_state_id = _insert_state(
        engine,
        method="futures_position",
        current_evaluation_status="matched",
        sequence_id=3,
        source_watermark=_LARGE_WATERMARK,
    )
    spot_state_id = _insert_state(
        engine,
        method="spot_execution_replay",
        current_evaluation_status="matched",
        sequence_id=4,
        anchor_public_id=anchor_public_id,
    )

    command.upgrade(config, "0026")
    inspector = sa.inspect(engine)
    assert "portfolio_reconciliation_method_configs" in inspector.get_table_names()
    with engine.begin() as connection:
        assert (
            connection.execute(
                sa.text("SELECT COUNT(*) FROM portfolio_reconciliation_method_configs")
            ).scalar_one()
            == 0
        )
        observation_rows = connection.execute(
            sa.text(
                "SELECT public_id, method, evaluation_status, source_watermark, anchor_public_id, "
                "timestamp, known_to FROM portfolio_reconciliation_observations "
                "ORDER BY sequence_id"
            )
        ).all()
        state_rows = connection.execute(
            sa.text(
                "SELECT public_id, method, current_evaluation_status, anchor_public_id, "
                "source_watermark, timestamp, known_to FROM "
                "portfolio_reconciliation_states ORDER BY sequence_id"
            )
        ).all()
    assert observation_rows == [
        (
            futures_observation_id,
            "futures_position",
            "incomplete",
            _LARGE_WATERMARK,
            None,
            _TS,
            _ACTIVE,
        ),
        (
            spot_observation_id,
            "spot_execution_replay",
            "matched",
            2,
            anchor_public_id,
            _TS,
            _ACTIVE,
        ),
    ]
    assert state_rows == [
        (
            futures_state_id,
            "futures_position",
            "matched",
            None,
            _LARGE_WATERMARK,
            _TS,
            _ACTIVE,
        ),
        (
            spot_state_id,
            "spot_execution_replay",
            "matched",
            anchor_public_id,
            4,
            _TS,
            _ACTIVE,
        ),
    ]
    observation_indexes = {
        item["name"] for item in inspector.get_indexes("portfolio_reconciliation_observations")
    }
    assert observation_indexes == {
        "ix_portfolio_reconciliation_observations_identity",
        "ix_portfolio_reconciliation_observations_public_id",
        "uq_portfolio_reconciliation_observations_evaluation",
    }
    state_indexes = {
        item["name"] for item in inspector.get_indexes("portfolio_reconciliation_states")
    }
    assert state_indexes == {
        "ix_portfolio_reconciliation_states_public_id",
        "ix_portfolio_reconciliation_states_wallet",
        "uq_portfolio_reconciliation_states_identity",
    }
    observation_checks = {
        item["name"]
        for item in inspector.get_check_constraints("portfolio_reconciliation_observations")
    }
    assert "ck_portfolio_recon_obs_method_status" in observation_checks
    assert "ck_portfolio_recon_obs_nonfull_method_evidence" in observation_checks

    command.downgrade(config, "0025")
    assert "portfolio_reconciliation_method_configs" not in sa.inspect(engine).get_table_names()
    with engine.begin() as connection:
        assert (
            connection.execute(
                sa.text(
                    "SELECT source_watermark FROM portfolio_reconciliation_observations "
                    "WHERE public_id = :public_id"
                ),
                {"public_id": futures_observation_id},
            ).scalar_one()
            == _LARGE_WATERMARK
        )
        assert (
            connection.execute(
                sa.text(
                    "SELECT source_watermark FROM portfolio_reconciliation_states "
                    "WHERE public_id = :public_id"
                ),
                {"public_id": futures_state_id},
            ).scalar_one()
            == _LARGE_WATERMARK
        )
    command.upgrade(config, "0026")
    assert "portfolio_reconciliation_method_configs" in sa.inspect(engine).get_table_names()
    with engine.begin() as connection:
        assert (
            connection.execute(
                sa.text(
                    "SELECT source_watermark FROM portfolio_reconciliation_observations "
                    "WHERE public_id = :public_id"
                ),
                {"public_id": futures_observation_id},
            ).scalar_one()
            == _LARGE_WATERMARK
        )
        assert (
            connection.execute(
                sa.text(
                    "SELECT source_watermark FROM portfolio_reconciliation_states "
                    "WHERE public_id = :public_id"
                ),
                {"public_id": futures_state_id},
            ).scalar_one()
            == _LARGE_WATERMARK
        )
    engine.dispose()


def test_0026_config_checks_and_active_uniqueness(tmp_path: Path) -> None:
    """Config rows accept only real live methods and enforce both active identities."""
    db_url = f"sqlite:///{tmp_path / 'reconciliation-method-config.db'}"
    config = _config(db_url)
    command.upgrade(config, "0026")
    engine = sa.create_engine(db_url)
    wallet_public_id = _uuid()
    public_id = _uuid()
    _insert_config(
        engine,
        wallet_public_id=wallet_public_id,
        exchange="kraken",
        method="spot_execution_replay",
        public_id=public_id,
        sequence_id=1,
    )
    with pytest.raises(IntegrityError):
        _insert_config(
            engine,
            wallet_public_id=wallet_public_id,
            exchange="kraken",
            method="margin_ledger_replay",
            public_id=_uuid(),
            sequence_id=2,
        )
    with pytest.raises(IntegrityError):
        _insert_config(
            engine,
            wallet_public_id=_uuid(),
            exchange="walutomat",
            method="spot_execution_replay",
            public_id=public_id,
            sequence_id=3,
        )
    with pytest.raises(IntegrityError):
        _insert_config(
            engine,
            wallet_public_id=_uuid(),
            exchange="kraken",
            method="unclassified",
            public_id=_uuid(),
            sequence_id=4,
        )
    with pytest.raises(IntegrityError):
        _insert_config(
            engine,
            wallet_public_id=_uuid(),
            exchange="Kraken",
            method="spot_execution_replay",
            public_id=_uuid(),
            sequence_id=5,
        )
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "UPDATE portfolio_reconciliation_method_configs SET known_to = :known_to "
                "WHERE public_id = :public_id"
            ),
            {"known_to": _TS_LATER, "public_id": public_id},
        )
    _insert_config(
        engine,
        wallet_public_id=wallet_public_id,
        exchange="kraken",
        method="margin_ledger_replay",
        public_id=public_id,
        sequence_id=6,
        timestamp=_TS_LATER,
    )
    with engine.begin() as connection:
        assert (
            connection.execute(
                sa.text(
                    "SELECT COUNT(*) FROM portfolio_reconciliation_method_configs "
                    "WHERE public_id = :public_id"
                ),
                {"public_id": public_id},
            ).scalar_one()
            == 2
        )
    engine.dispose()


def test_0026_method_status_anchor_and_nonfull_evidence_guards(tmp_path: Path) -> None:
    """Method compatibility and non-full evidence checks reject forged combinations."""
    db_url = f"sqlite:///{tmp_path / 'reconciliation-method-checks.db'}"
    config = _config(db_url)
    command.upgrade(config, "0026")
    engine = sa.create_engine(db_url)
    _insert_observation(
        engine,
        method="margin_ledger_replay",
        evaluation_status="error",
        sequence_id=1,
    )
    _insert_observation(
        engine,
        method="unclassified",
        evaluation_status="incomplete",
        sequence_id=2,
    )
    _insert_observation(
        engine,
        method="unclassified",
        evaluation_status="error",
        sequence_id=3,
    )
    _insert_observation(
        engine,
        method="spot_execution_replay",
        evaluation_status="incomplete",
        sequence_id=4,
    )
    _insert_state(
        engine,
        method="margin_ledger_replay",
        current_evaluation_status="error",
        sequence_id=5,
    )
    _insert_state(
        engine,
        method="unclassified",
        current_evaluation_status="incomplete",
        sequence_id=6,
    )
    _insert_state(
        engine,
        method="unclassified",
        current_evaluation_status="error",
        sequence_id=7,
    )
    _insert_state(
        engine,
        method="spot_execution_replay",
        current_evaluation_status="incomplete",
        sequence_id=8,
    )
    invalid_observations = (
        (None, "incomplete", False),
        ("margin_ledger_replay", "incomplete", False),
        ("margin_ledger_replay", "unsupported", False),
        ("margin_ledger_replay", "matched", False),
        ("unclassified", "matched", False),
        ("unclassified", "mismatched", False),
        ("unclassified", "unsupported", False),
        ("margin_ledger_replay", "error", True),
    )
    for sequence_id, (method, status, retained_evidence) in enumerate(
        invalid_observations, start=100
    ):
        with pytest.raises(IntegrityError):
            _insert_observation(
                engine,
                method=method,
                evaluation_status=status,
                sequence_id=sequence_id,
                retained_evidence=retained_evidence,
            )
    with pytest.raises(IntegrityError):
        _insert_observation(
            engine,
            method="spot_execution_replay",
            evaluation_status="matched",
            sequence_id=200,
            anchor_public_id=None,
        )
    invalid_states = (
        (None, "incomplete", False),
        ("margin_ledger_replay", "incomplete", False),
        ("margin_ledger_replay", "unsupported", False),
        ("margin_ledger_replay", "matched", False),
        ("unclassified", "matched", False),
        ("unclassified", "mismatched", False),
        ("unclassified", "unsupported", False),
        ("margin_ledger_replay", "error", True),
    )
    for sequence_id, (method, status, retained_evidence) in enumerate(invalid_states, start=300):
        with pytest.raises(IntegrityError):
            _insert_state(
                engine,
                method=method,
                current_evaluation_status=status,
                sequence_id=sequence_id,
                retained_evidence=retained_evidence,
            )
    with pytest.raises(IntegrityError):
        _insert_state(
            engine,
            method="spot_execution_replay",
            current_evaluation_status="matched",
            sequence_id=400,
            anchor_public_id=None,
        )
    engine.dispose()


@pytest.mark.parametrize(
    ("history_kind", "message"),
    (
        ("observation", "observation history uses a new method"),
        ("state", "state history uses a new method"),
    ),
)
def test_0026_downgrade_fails_closed_before_schema_mutation(
    tmp_path: Path,
    history_kind: str,
    message: str,
) -> None:
    """Downgrade refuses new-method history without changing the 0026 schema."""
    db_url = f"sqlite:///{tmp_path / f'reconciliation-downgrade-{history_kind}.db'}"
    config = _config(db_url)
    command.upgrade(config, "0026")
    engine = sa.create_engine(db_url)
    if history_kind == "observation":
        _insert_observation(
            engine,
            method="unclassified",
            evaluation_status="incomplete",
            sequence_id=1,
        )
    else:
        _insert_state(
            engine,
            method="margin_ledger_replay",
            current_evaluation_status="error",
            sequence_id=1,
        )
    with pytest.raises(RuntimeError, match=message):
        command.downgrade(config, "0025")
    inspector = sa.inspect(engine)
    assert "portfolio_reconciliation_method_configs" in inspector.get_table_names()
    observation_checks = {
        item["name"]
        for item in inspector.get_check_constraints("portfolio_reconciliation_observations")
    }
    assert "ck_portfolio_recon_obs_method_status" in observation_checks
    with engine.begin() as connection:
        assert connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one() == ("0026")
    engine.dispose()


def test_0026_postgresql_compile_and_model_signature() -> None:
    """PostgreSQL DDL uses native types, named constraints, and direct ALTERs."""
    migration = importlib.import_module(
        "snapper.data.migrations.versions.0026_reconciliation_method_config"
    )
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    with patch.object(migration, "op", operations):
        migration.upgrade()
        with pytest.raises(RuntimeError, match="requires an online connection"):
            migration._ensure_downgrade_history_compatible()
        with patch.object(migration, "_ensure_downgrade_history_compatible"):
            migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0026"
    assert migration.down_revision == "0025"
    assert "CREATE TABLE portfolio_reconciliation_method_configs" in ddl
    assert "wallet_public_id UUID NOT NULL" in ddl
    assert "TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "ck_portfolio_recon_method_configs_method" in ddl
    assert "ck_portfolio_recon_obs_method_status" in ddl
    assert "ck_portfolio_recon_obs_nonfull_method_evidence" in ddl
    assert "ck_portfolio_recon_states_method_status" in ddl
    assert "ck_portfolio_recon_states_nonfull_method_evidence" in ddl
    assert "CHECK (method IS NOT NULL AND method IN" in ddl
    assert "evaluation_status IS NOT NULL" in ddl
    assert "current_evaluation_status IS NOT NULL" in ddl
    assert "method = 'margin_ledger_replay' AND evaluation_status = 'error'" in ddl
    assert "method = 'margin_ledger_replay' AND current_evaluation_status = 'error'" in ddl
    assert "method = 'unclassified' AND evaluation_status IN ('incomplete', 'error')" in ddl
    assert "WHERE known_to = '9999-12-31T23:59:59+00:00'" in ddl
    assert "ALTER TABLE portfolio_reconciliation_observations DROP CONSTRAINT" in ddl
    assert "ALTER TABLE portfolio_reconciliation_states DROP CONSTRAINT" in ddl
    assert "DROP TABLE portfolio_reconciliation_method_configs" in ddl
    assert "INSERT INTO" not in ddl
    assert "UPDATE " not in ddl

    config_table = PortfolioReconciliationMethodConfig.__table__
    assert isinstance(config_table.c.wallet_public_id.type, UUIDColumn)
    assert isinstance(config_table.c.timestamp.type, TZDateTime)
    assert config_table.c.known_to.server_default is None
    assert PortfolioReconciliationMethodConfig.__name__ in models.__all__
    assert isinstance(
        PortfolioReconciliationObservation.__table__.c.source_watermark.type,
        sa.BigInteger,
    )
    assert isinstance(PortfolioReconciliationState.__table__.c.source_watermark.type, sa.BigInteger)
