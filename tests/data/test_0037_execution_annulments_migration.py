"""Tests for the append-only execution-annulment manifest migration 0037.

Migration 0037 creates the brand-new ``execution_annulments`` table. Migrations
are excluded from the coverage gate, so the SQLite branch is pinned explicitly:
the upgrade creates the table with its four indexes and both immutability
triggers, the downgrade drops all of it and stays re-runnable, every CHECK that
guards the closed vocabularies and the always-open ``known_to`` sentinel
actually rejects a violating row, and both TOTAL unique indexes refuse a second
annulment.

The module also pins MIGRATION/MODEL PARITY, which no other check covers: the
Alembic-built production schema and the ``create_all``-built schema every test
fixture uses are compared column by column, index by index, and CHECK by CHECK.
Without that, the two schemas could drift silently — the ORM model would keep
passing every unit test while production carried a different table. The
PostgreSQL branch is exercised by ``db-init`` against the live scratch database.
"""

from pathlib import Path
from typing import Final
from typing import get_args

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import EXECUTION_ANNULMENT_REASONS
from snapper.data.models import ExecutionAnnulment
from snapper.data.repository_types import ExecutionAnnulmentReason

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLE = "execution_annulments"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-25 08:00:00.000000"
_INDEXES: Final[list[str]] = [
    "ix_execution_annulments_manifest",
    "ix_execution_annulments_public_id",
    "uq_execution_annulments_scope",
    "uq_execution_annulments_target",
]
_TRIGGERS: Final[list[str]] = [
    "execution_annulments_reject_delete",
    "execution_annulments_reject_update",
]
_DIGEST = "a" * 64

_INSERT_SQL = (
    "INSERT INTO execution_annulments ("
    "target_execution_public_id, target_execution_digest, wallet_public_id, exchange, mode, "
    "scope_sequence, annulled_by_user_public_id, correction_time, reason, evidence_json, "
    "public_id, session_id, sequence_id, timestamp, known_to) VALUES ("
    ":target_execution_public_id, :target_execution_digest, :wallet_public_id, :exchange, "
    ":mode, :scope_sequence, :annulled_by_user_public_id, :correction_time, :reason, "
    ":evidence_json, :public_id, :session_id, :sequence_id, :timestamp, :known_to)"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _row(**overrides: object) -> dict[str, object]:
    """Build a valid manifest row, overriding named columns for CHECK probes."""
    base: dict[str, object] = {
        "target_execution_public_id": "0000face-0000-7000-8000-0000000000e1",
        "target_execution_digest": _DIGEST,
        "wallet_public_id": "0000face-0000-7000-8000-000000000001",
        "exchange": "kraken",
        "mode": "live",
        "scope_sequence": 1,
        "annulled_by_user_public_id": "0000face-0000-7000-8000-0000000000c1",
        "correction_time": _TS,
        "reason": "unwitnessed_phantom",
        "evidence_json": '{"diagnosis":"size 0 price 0, no fill_observed"}',
        "public_id": "0000face-0000-7000-8000-0000000000a0",
        "session_id": "0000face-0000-7000-8000-0000000000b0",
        "sequence_id": 1,
        "timestamp": _TS,
        "known_to": _ACTIVE,
    }
    base.update(overrides)
    return base


def _insert(engine: sa.Engine, **overrides: object) -> None:
    """Insert one manifest row into the throwaway database."""
    with engine.begin() as connection:
        connection.execute(sa.text(_INSERT_SQL), _row(**overrides))


def _object_names(engine: sa.Engine, object_type: str) -> list[str]:
    """Return the manifest's SQLite objects of one catalog type."""
    query = (
        "SELECT name FROM sqlite_master WHERE type = :object_type AND tbl_name = :table "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    with engine.connect() as connection:
        return [
            str(row[0])
            for row in connection.execute(
                sa.text(query), {"object_type": object_type, "table": _TABLE}
            )
        ]


def _has_table(engine: sa.Engine) -> bool:
    """Report whether the manifest table exists in SQLite."""
    return _TABLE in sa.inspect(engine).get_table_names()


def _schema_fingerprint(engine: sa.Engine) -> dict[str, object]:
    """Reduce one live manifest schema to a comparable structural fingerprint."""
    inspector = sa.inspect(engine)
    return {
        "columns": [
            (column["name"], str(column["type"]), bool(column["nullable"]))
            for column in inspector.get_columns(_TABLE)
        ],
        "primary_key": inspector.get_pk_constraint(_TABLE)["constrained_columns"],
        "indexes": sorted(
            (str(index["name"]), tuple(index["column_names"]), bool(index["unique"]))
            for index in inspector.get_indexes(_TABLE)
        ),
        "checks": sorted(
            (str(check["name"]), " ".join(str(check["sqltext"]).split()))
            for check in inspector.get_check_constraints(_TABLE)
        ),
    }


def test_0037_upgrade_creates_manifest_downgrade_drops_and_is_rerunnable(tmp_path: Path) -> None:
    """The manifest table, indexes, and triggers round-trip cleanly, repeatably.

    Given: A SQLite database migrated to 0036 without the manifest table.
    When: It is upgraded to 0037, downgraded to 0036, and cycled once more.
    Then: The table, its four indexes, and both immutability triggers exist
        exactly after each upgrade and are all absent after each downgrade —
        the downgrade drops the triggers before the table rather than leaving
        orphaned DDL behind.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0036")
    engine = sa.create_engine(db_url)
    assert not _has_table(engine)

    command.upgrade(config, "0037")
    assert _has_table(engine)
    assert _object_names(engine, "index") == _INDEXES
    assert _object_names(engine, "trigger") == _TRIGGERS

    command.downgrade(config, "0036")
    assert not _has_table(engine)
    assert _object_names(engine, "trigger") == []

    command.upgrade(config, "0037")
    assert _has_table(engine)
    assert _object_names(engine, "index") == _INDEXES
    assert _object_names(engine, "trigger") == _TRIGGERS

    command.downgrade(config, "0036")
    assert not _has_table(engine)
    engine.dispose()


def test_0037_schema_matches_the_orm_model_exactly(tmp_path: Path) -> None:
    """The Alembic-built and ``create_all``-built manifests are the same schema.

    Given: One SQLite database built by migrating to 0037 and one built by
        creating ``ExecutionAnnulment.__table__`` directly.
    When: Both schemas are reduced to a structural fingerprint of columns,
        primary key, indexes, and CHECK constraints.
    Then: The fingerprints are identical, so the production table Alembic
        builds and the table every test fixture builds cannot drift apart.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url), "0037")
    migrated_engine = sa.create_engine(migrated_url)

    model_url = f"sqlite:///{tmp_path / 'model.db'}"
    model_engine = sa.create_engine(model_url)
    ExecutionAnnulment.__table__.create(model_engine)

    assert _schema_fingerprint(migrated_engine) == _schema_fingerprint(model_engine)
    migrated_engine.dispose()
    model_engine.dispose()


def test_0037_reason_vocabulary_is_pinned_across_model_type_and_migration(
    tmp_path: Path,
) -> None:
    """One closed reason vocabulary is shared by the model, the type, and the DDL.

    Given: The exported ``EXECUTION_ANNULMENT_REASONS`` tuple, the typed
        ``ExecutionAnnulmentReason`` literal, and a database migrated to 0037.
    When: All three are compared, and each documented reason is inserted.
    Then: The tuple and the literal hold exactly the same values, every one of
        them is accepted by the migrated CHECK, and a value outside the set is
        rejected — so a reason cannot become typable without also becoming
        storable, or the reverse.
    """
    assert set(get_args(ExecutionAnnulmentReason.__value__)) == set(EXECUTION_ANNULMENT_REASONS)
    db_url = f"sqlite:///{tmp_path / 'reasons.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0037")
    for index, reason in enumerate(EXECUTION_ANNULMENT_REASONS, start=1):
        _insert(
            engine,
            reason=reason,
            scope_sequence=index,
            public_id=f"0000face-0000-7000-8000-0000000000{index:02d}",
            target_execution_public_id=f"0000face-0000-7000-8000-0000000000e{index}",
        )
    with engine.connect() as connection:
        stored = connection.execute(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()
    assert stored == len(EXECUTION_ANNULMENT_REASONS)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, reason="operator_felt_like_it", scope_sequence=99)
    engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"mode": "shadow"}, id="bad_mode"),
        pytest.param({"exchange": "Kraken"}, id="uppercase_exchange"),
        pytest.param({"exchange": "  "}, id="blank_exchange"),
        pytest.param({"scope_sequence": 0}, id="zero_scope_sequence"),
        pytest.param({"target_execution_digest": "a" * 63}, id="short_digest"),
        pytest.param({"target_execution_digest": "A" * 64}, id="uppercase_digest"),
        pytest.param({"evidence_json": "   "}, id="blank_evidence"),
        pytest.param({"known_to": _TS}, id="closed_known_to"),
    ],
)
def test_0037_check_constraints_reject_violations(
    tmp_path: Path, overrides: dict[str, object]
) -> None:
    """Each guarded manifest invariant physically rejects a violating row.

    Given: A SQLite database upgraded to 0037.
    When: A row violating the mode vocabulary, the lowercase non-blank
        exchange, the positive scope counter, the 64-lowercase-hex digest, the
        non-blank evidence envelope, or the always-open ``known_to`` sentinel is
        inserted.
    Then: The database raises an integrity error rather than storing it — in
        particular a pre-closed manifest row can never be created, which is
        what makes "rows always keep ``known_to`` open" a schema fact rather
        than a convention.
    """
    db_url = f"sqlite:///{tmp_path / 'checks.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0037")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, **overrides)
    engine.dispose()


def test_0037_target_and_scope_uniqueness_are_total(tmp_path: Path) -> None:
    """One execution and one scope slot each admit exactly one annulment, forever.

    Given: A SQLite database upgraded to 0037 holding one manifest row.
    When: A second row repeats the target execution id, and separately a third
        row repeats the scope coordinates under a different target.
    Then: Both are rejected — and because neither unique index carries a
        ``known_to`` predicate, no SCD2 successor could evade them either,
        mirroring the executions ledger's own TOTAL scope-sequence index.
    """
    db_url = f"sqlite:///{tmp_path / 'unique.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0037")
    _insert(engine)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            public_id="0000face-0000-7000-8000-0000000000a1",
            scope_sequence=2,
        )
    with pytest.raises(sa.exc.IntegrityError):
        _insert(
            engine,
            public_id="0000face-0000-7000-8000-0000000000a2",
            target_execution_public_id="0000face-0000-7000-8000-0000000000e2",
        )
    engine.dispose()
