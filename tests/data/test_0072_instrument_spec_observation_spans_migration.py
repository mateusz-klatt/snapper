"""Tests for migration 0072 creating ``instrument_spec_observation_spans``.

Covers empty and populated upgrade/downgrade round-trips, active-only backfill
without touching historical ``instrument_specs`` rows, strict ORM-versus-migration
schema parity, row-level CHECK enforcement, and offline PostgreSQL DDL generation.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from importlib import import_module
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import InstrumentSpec
from snapper.data.models import InstrumentSpecObservationSpan

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLE = "instrument_spec_observation_spans"
_INDEX = "ix_inst_spec_obs_spans_lookup"
_T0 = datetime(2026, 11, 20, 10, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(hours=1)
_T2 = _T0 + timedelta(hours=2)
_INST_ACTIVE = "00000000-0000-7000-8000-000000000201"
_INST_NULL_OBS = "00000000-0000-7000-8000-000000000202"
_SPEC_CLOSED_PUB = "00000000-0000-7000-8000-000000000301"
_SPEC_ACTIVE_PUB = "00000000-0000-7000-8000-000000000301"
_SPEC_NULL_PUB = "00000000-0000-7000-8000-000000000302"
_SESSION_1 = "00000000-0000-7000-8000-000000000401"
_SESSION_2 = "00000000-0000-7000-8000-000000000402"


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


def _seed_specs(engine: sa.Engine) -> list[tuple[object, ...]]:
    """Seed one historical closed spec, one active observed spec, and one active null spec."""
    with engine.begin() as connection:
        connection.execute(
            sa.insert(InstrumentSpec.__table__),
            [
                {
                    "public_id": _SPEC_CLOSED_PUB,
                    "timestamp": _T0,
                    "known_to": _T1,
                    "session_id": _SESSION_1,
                    "sequence_id": 1,
                    "instrument_public_id": _INST_ACTIVE,
                    "spec_source": "kraken:ccxt.load_markets",
                    "spec_version": "s2a-v1:old",
                    "spec_observed_at": _T0,
                    "unit_certified": False,
                },
                {
                    "public_id": _SPEC_ACTIVE_PUB,
                    "timestamp": _T1,
                    "known_to": KNOWN_TO_MAX,
                    "session_id": _SESSION_2,
                    "sequence_id": 2,
                    "instrument_public_id": _INST_ACTIVE,
                    "spec_source": "kraken:ccxt.load_markets",
                    "spec_version": "s2a-v1:active",
                    "spec_observed_at": _T1,
                    "unit_certified": False,
                },
                {
                    "public_id": _SPEC_NULL_PUB,
                    "timestamp": _T1,
                    "known_to": KNOWN_TO_MAX,
                    "session_id": _SESSION_2,
                    "sequence_id": 3,
                    "instrument_public_id": _INST_NULL_OBS,
                    "spec_source": None,
                    "spec_version": None,
                    "spec_observed_at": None,
                    "unit_certified": False,
                },
            ],
        )
        rows = connection.execute(
            sa.select(InstrumentSpec.__table__).order_by(InstrumentSpec.id)
        ).all()
    return [tuple(row) for row in rows]


def test_0072_round_trip_empty_database(tmp_path: Path) -> None:
    """Upgrade and downgrade on an empty database create and remove only the span table.

    Given: A SQLite database migrated to revision 0071 with zero specs.
    When: Revision 0072 upgrades, downgrades to 0071, and upgrades again.
    Then: The observation-span table and lookup index appear on upgrade and vanish on downgrade.
    """
    db_url = f"sqlite:///{tmp_path / 'empty.db'}"
    config = _config(db_url)
    command.upgrade(config, "0071")
    engine = sa.create_engine(db_url)
    specs_before = _schema_fingerprint(engine, "instrument_specs")
    assert _TABLE not in sa.inspect(engine).get_table_names()

    command.upgrade(config, "0072")
    assert _TABLE in sa.inspect(engine).get_table_names()
    assert _INDEX in {idx["name"] for idx in sa.inspect(engine).get_indexes(_TABLE)}
    assert _schema_fingerprint(engine, "instrument_specs") == specs_before
    with engine.connect() as connection:
        count = connection.execute(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar_one()
    assert count == 0

    command.downgrade(config, "0071")
    assert _TABLE not in sa.inspect(engine).get_table_names()
    assert _schema_fingerprint(engine, "instrument_specs") == specs_before

    command.upgrade(config, "0072")
    assert _TABLE in sa.inspect(engine).get_table_names()
    engine.dispose()


def test_0072_backfills_only_active_observed_specs_without_rewriting_history(
    tmp_path: Path,
) -> None:
    """Active-only backfill preserves all historical ``instrument_specs`` rows verbatim.

    Given: A revision-0071 database with one closed spec, one active observed spec,
        and one active spec with null ``spec_observed_at``.
    When: Revision 0072 upgrades and then downgrades.
    Then: Only the active observed spec gets an initial span, and ``instrument_specs``
        rows remain byte-for-byte unchanged across both upgrade and downgrade.
    """
    db_url = f"sqlite:///{tmp_path / 'populated.db'}"
    config = _config(db_url)
    command.upgrade(config, "0071")
    engine = sa.create_engine(db_url)
    specs_before = _seed_specs(engine)

    command.upgrade(config, "0072")
    with engine.connect() as connection:
        specs_after_upgrade = [
            tuple(row)
            for row in connection.execute(
                sa.select(InstrumentSpec.__table__).order_by(InstrumentSpec.id)
            ).all()
        ]
        spans = connection.execute(
            sa.select(InstrumentSpecObservationSpan.__table__).order_by(
                InstrumentSpecObservationSpan.id
            )
        ).all()
    assert specs_after_upgrade == specs_before
    assert len(spans) == 1
    span = spans[0]
    assert span.instrument_public_id == _INST_ACTIVE
    assert span.spec_public_id == _SPEC_ACTIVE_PUB
    assert span.spec_timestamp == _T1
    assert span.spec_version == "s2a-v1:active"
    assert span.first_observed_at == _T1
    assert span.last_observed_at == _T1
    assert span.session_id == _SESSION_2
    assert span.sequence_id == 2

    command.downgrade(config, "0071")
    with engine.connect() as connection:
        specs_after_downgrade = [
            tuple(row)
            for row in connection.execute(
                sa.select(InstrumentSpec.__table__).order_by(InstrumentSpec.id)
            ).all()
        ]
    assert specs_after_downgrade == specs_before
    engine.dispose()


def test_0072_schema_matches_orm_model_exactly(tmp_path: Path) -> None:
    """The Alembic-built and ORM-built tables share an identical fingerprint.

    Given: One SQLite database upgraded to revision 0072 and a second built via
        ``InstrumentSpecObservationSpan.__table__.create``.
    When: Both schemas are reduced to column, primary key, index, and CHECK fingerprints.
    Then: Both fingerprints match identically.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url), "0072")
    migrated_engine = sa.create_engine(migrated_url)

    model_url = f"sqlite:///{tmp_path / 'model.db'}"
    model_engine = sa.create_engine(model_url)
    InstrumentSpecObservationSpan.__table__.create(model_engine)

    assert _schema_fingerprint(migrated_engine, _TABLE) == _schema_fingerprint(model_engine, _TABLE)
    migrated_engine.dispose()
    model_engine.dispose()


def test_0072_check_constraint_rejects_inverted_span_bounds(tmp_path: Path) -> None:
    """The database enforces ``first_observed_at <= last_observed_at``.

    Given: A database upgraded to revision 0072.
    When: An insert supplies ``first_observed_at > last_observed_at``.
    Then: SQLite rejects the row with ``IntegrityError``.
    """
    db_url = f"sqlite:///{tmp_path / 'check.db'}"
    command.upgrade(_config(db_url), "0072")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            sa.insert(InstrumentSpecObservationSpan.__table__),
            {
                "instrument_public_id": _INST_ACTIVE,
                "spec_public_id": _SPEC_ACTIVE_PUB,
                "spec_timestamp": _T1,
                "spec_version": "s2a-v1:active",
                "first_observed_at": _T2,
                "last_observed_at": _T1,
                "session_id": _SESSION_1,
                "sequence_id": 1,
            },
        )
    engine.dispose()


def test_0072_postgresql_ddl_round_trip_uses_native_types() -> None:
    """PostgreSQL offline DDL emits native UUID, TIMESTAMPTZ, DESC index, and backfill.

    Given: Revision 0072 executed against an offline PostgreSQL SQL recorder.
    When: Both ``upgrade()`` and ``downgrade()`` are rendered to SQL.
    Then: The revision pins ``0072`` over ``0071`` and renders native types, the
        DESC lookup index, the active-only INSERT-SELECT backfill, and clean drop DDL.
    """
    migration = import_module(
        "snapper.data.migrations.versions.0072_instrument_spec_observation_spans"
    )
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    with patch.object(migration, "op", Operations(context)):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0072"
    assert migration.down_revision == "0071"
    assert f"CREATE TABLE {_TABLE}" in ddl
    assert "instrument_public_id UUID NOT NULL" in ddl
    assert "spec_public_id UUID NOT NULL" in ddl
    assert "spec_timestamp TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "first_observed_at TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "last_observed_at TIMESTAMP WITH TIME ZONE NOT NULL" in ddl
    assert "ck_inst_spec_obs_spans_bounds" in ddl
    assert f"CREATE INDEX {_INDEX}" in ddl
    assert "last_observed_at DESC" in ddl
    assert f"INSERT INTO {_TABLE}" in ddl
    assert "FROM instrument_specs" in ddl
    assert f"DROP INDEX {_INDEX}" in ddl
    assert f"DROP TABLE {_TABLE}" in ddl
