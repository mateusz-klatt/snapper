"""Tests for migration 0074 creating venue ledger observation tables."""

from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.exc import IntegrityError

from snapper.data.models import VenueLedgerCursor
from snapper.data.models import VenueLedgerEntry

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_NOW = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)
_WALLET = "00000000-0000-7000-8000-000000000101"
_SESSION = "00000000-0000-7000-8000-000000000201"
_OBSERVATION = "00000000-0000-7000-8000-000000000301"
_RECONCILIATION = "portfolio_reconciliation_states"


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one isolated SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _schema_fingerprint(engine: sa.Engine, table: str) -> dict[str, object]:
    """Reduce one live table schema to a comparable structural fingerprint."""
    inspector = sa.inspect(engine)
    return {
        "columns": [
            (column["name"], str(column["type"]), bool(column["nullable"]))
            for column in inspector.get_columns(table)
        ],
        "primary_key": inspector.get_pk_constraint(table)["constrained_columns"],
        "indexes": sorted(
            (str(index["name"]), tuple(index["column_names"]), bool(index["unique"]))
            for index in inspector.get_indexes(table)
        ),
        "checks": sorted(
            (str(check["name"]), " ".join(str(check["sqltext"]).split()))
            for check in inspector.get_check_constraints(table)
        ),
    }


def _entry() -> dict[str, object]:
    """Return one valid append-only ledger row."""
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "venue_ledger_id": "L1",
        "refid": "R1",
        "ledger_time_text": "1700000000.0",
        "ledger_type": "margin",
        "subtype": "",
        "aclass": "currency",
        "asset": "ZEUR",
        "amount_text": "1.2500",
        "fee_text": "0.0100",
        "balance_text": "8.0000",
        "observed_at": _NOW,
        "observation_id": _OBSERVATION,
    }


def _cursor(**overrides: object) -> dict[str, object]:
    """Return one valid ledger cursor row."""
    row: dict[str, object] = {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "cursor_second": 1700000000,
        "cursor_ids_json": "[]",
        "boundary_as_of": _NOW,
        "boundary_watermark": 4,
        "boundary_session_id": _SESSION,
        "boundary_sequence_id": 7,
        "evidence_status": None,
        "reported_count": 0,
        "ingested_count": 0,
        "pass_started_at": _NOW,
        "observed_at": _NOW,
        "observation_id": _OBSERVATION,
        "trade_balance_json": None,
        "trade_balance_observed_at": None,
        "api_points_used": 0,
    }
    row.update(overrides)
    return row


def test_0074_upgrade_downgrade_preserves_reconciliation_schema(tmp_path: Path) -> None:
    """Add the ledger tables and remove them without changing reconciliation.

    Given a database at revision 0073,
    When it is upgraded to 0074, populated, and downgraded to 0073,
    Then the new tables appear and disappear and portfolio reconciliation checks
        stay identical.
    """
    db_url = f"sqlite:///{tmp_path / 'ledger.db'}"
    config = _config(db_url)
    command.upgrade(config, "0073")
    engine = sa.create_engine(db_url)
    before = _schema_fingerprint(engine, _RECONCILIATION)
    assert "venue_ledger_entries" not in sa.inspect(engine).get_table_names()
    engine.dispose()

    command.upgrade(config, "0074")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        connection.execute(sa.insert(VenueLedgerEntry.__table__), _entry())
        connection.execute(sa.insert(VenueLedgerCursor.__table__), _cursor())
    names = set(sa.inspect(engine).get_table_names())
    assert {"venue_ledger_entries", "venue_ledger_cursors"} <= names
    assert _schema_fingerprint(engine, _RECONCILIATION) == before
    engine.dispose()

    command.downgrade(config, "0073")
    engine = sa.create_engine(db_url)
    names = set(sa.inspect(engine).get_table_names())
    assert "venue_ledger_entries" not in names
    assert "venue_ledger_cursors" not in names
    assert _schema_fingerprint(engine, _RECONCILIATION) == before
    engine.dispose()


def test_0074_schema_matches_orm_model_exactly(tmp_path: Path) -> None:
    """The migrated ledger tables match the ORM tables.

    Given one database upgraded to 0074 and one built from the ORM models,
    When both schemas are fingerprinted,
    Then the entries table and the cursor table match.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url), "0074")
    migrated = sa.create_engine(migrated_url)
    model = sa.create_engine(f"sqlite:///{tmp_path / 'model.db'}")
    VenueLedgerEntry.__table__.create(model)
    VenueLedgerCursor.__table__.create(model)
    for table in ("venue_ledger_entries", "venue_ledger_cursors"):
        assert _schema_fingerprint(migrated, table) == _schema_fingerprint(model, table)
    migrated.dispose()
    model.dispose()


def test_0074_constraints_reject_duplicate_ids_and_unknown_status(tmp_path: Path) -> None:
    """The new tables enforce identity and the evidence-status vocabulary.

    Given a database upgraded to 0074 with one posting and one cursor,
    When the same ledger id is inserted again and a cursor uses an unknown status,
    Then both inserts raise IntegrityError.
    """
    db_url = f"sqlite:///{tmp_path / 'check.db'}"
    command.upgrade(_config(db_url), "0074")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        connection.execute(sa.insert(VenueLedgerEntry.__table__), _entry())
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(sa.insert(VenueLedgerEntry.__table__), _entry())
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            sa.insert(VenueLedgerCursor.__table__),
            _cursor(evidence_status="not-a-status"),
        )
    engine.dispose()
