"""Tests for the append-only annulment visibility-observation migration 0039.

Migration 0039 creates the brand-new ``execution_annulment_visibility`` table,
the plane that turns a correction's knowledge instant from an assumption into a
proof. Migrations are excluded from the coverage gate, so the SQLite branch is
pinned explicitly: the upgrade creates the table with its four indexes and both
immutability triggers, the downgrade drops all of it and stays re-runnable,
every CHECK actually rejects a violating row, and both TOTAL unique indexes
refuse a second observation for one correction — the property that keeps a
retry from moving a proven knowledge instant forward.

The module also pins MIGRATION/MODEL PARITY, which no other check covers: the
Alembic-built production schema and the ``create_all``-built schema every test
fixture uses are compared column by column, index by index, and CHECK by CHECK.
The PostgreSQL branch is exercised by ``db-init`` against the live scratch
database.
"""

from pathlib import Path
from typing import Final

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import ExecutionAnnulmentVisibility

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLE = "execution_annulment_visibility"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-25 08:00:00.000000"
_INDEXES: Final[list[str]] = [
    "ix_execution_annulment_visibility_public_id",
    "ix_execution_annulment_visibility_scope",
    "uq_execution_annulment_visibility_annulment",
    "uq_execution_annulment_visibility_annulment_id",
]
_TRIGGERS: Final[list[str]] = [
    "execution_annulment_visibility_reject_delete",
    "execution_annulment_visibility_reject_update",
]

_INSERT_SQL = (
    "INSERT INTO execution_annulment_visibility ("
    "annulment_public_id, annulment_id, observed_at, wallet_public_id, exchange, mode, "
    "public_id, session_id, sequence_id, timestamp, known_to) VALUES ("
    ":annulment_public_id, :annulment_id, :observed_at, :wallet_public_id, :exchange, "
    ":mode, :public_id, :session_id, :sequence_id, :timestamp, :known_to)"
)


def _config(db_url: str) -> Config:
    """Build an Alembic config bound to one throwaway SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _row(**overrides: object) -> dict[str, object]:
    """Build one canonical observation row, overriding named columns."""
    base: dict[str, object] = {
        "annulment_public_id": "0000face-0000-7000-8000-0000000000a1",
        "annulment_id": 1,
        "observed_at": _TS,
        "wallet_public_id": "0000face-0000-7000-8000-000000000001",
        "exchange": "kraken",
        "mode": "live",
        "public_id": "0000face-0000-7000-8000-0000000000b0",
        "session_id": "0000face-0000-7000-8000-0000000000c0",
        "sequence_id": 1,
        "timestamp": _TS,
        "known_to": _ACTIVE,
    }
    base.update(overrides)
    return base


def _insert(engine: sa.Engine, **overrides: object) -> None:
    """Insert one observation row into the throwaway database."""
    with engine.begin() as connection:
        connection.execute(sa.text(_INSERT_SQL), _row(**overrides))


def _object_names(engine: sa.Engine, object_type: str) -> list[str]:
    """Return the observation ledger's SQLite objects of one catalog type."""
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
    """Report whether the observation table exists in SQLite."""
    return _TABLE in sa.inspect(engine).get_table_names()


def _schema_fingerprint(engine: sa.Engine) -> dict[str, object]:
    """Reduce one live observation schema to a comparable structural fingerprint."""
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


def test_0039_upgrade_creates_ledger_downgrade_drops_and_is_rerunnable(tmp_path: Path) -> None:
    """The observation table, indexes, and triggers round-trip cleanly, repeatably.

    Given: A SQLite database migrated to 0038 without the observation table.
    When: It is upgraded to 0039, downgraded to 0038, and cycled once more.
    Then: The table, its four indexes, and both immutability triggers exist
        exactly after each upgrade and are all absent after each downgrade —
        the downgrade drops the triggers before the table rather than leaving
        orphaned DDL behind.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0038")
    engine = sa.create_engine(db_url)
    assert not _has_table(engine)

    command.upgrade(config, "0039")
    assert _has_table(engine)
    assert _object_names(engine, "index") == _INDEXES
    assert _object_names(engine, "trigger") == _TRIGGERS

    command.downgrade(config, "0038")
    assert not _has_table(engine)
    assert _object_names(engine, "trigger") == []

    command.upgrade(config, "0039")
    assert _has_table(engine)
    assert _object_names(engine, "index") == _INDEXES
    assert _object_names(engine, "trigger") == _TRIGGERS

    command.downgrade(config, "0038")
    assert not _has_table(engine)
    engine.dispose()


def test_0039_schema_matches_the_orm_model_exactly(tmp_path: Path) -> None:
    """The Alembic-built and ``create_all``-built ledgers are the same schema.

    Given: One SQLite database built by migrating to 0039 and one built by
        creating ``ExecutionAnnulmentVisibility.__table__`` directly.
    When: Both schemas are reduced to a structural fingerprint of columns,
        primary key, indexes, and CHECK constraints.
    Then: The fingerprints are identical, so the production table Alembic
        builds and the table every test fixture builds cannot drift apart.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url), "0039")
    migrated_engine = sa.create_engine(migrated_url)

    model_url = f"sqlite:///{tmp_path / 'model.db'}"
    model_engine = sa.create_engine(model_url)
    ExecutionAnnulmentVisibility.__table__.create(model_engine)

    assert _schema_fingerprint(migrated_engine) == _schema_fingerprint(model_engine)
    migrated_engine.dispose()
    model_engine.dispose()


@pytest.mark.parametrize(
    ("overrides", "constraint"),
    [
        pytest.param({"exchange": "Kraken"}, "exchange_lower", id="exchange-not-lower"),
        pytest.param({"exchange": "   "}, "exchange_lower", id="exchange-blank"),
        pytest.param({"mode": "backtest"}, "mode", id="mode-outside-vocabulary"),
        pytest.param({"annulment_id": 0}, "annulment_id", id="annulment-id-not-positive"),
        pytest.param({"known_to": _TS}, "known_to_open", id="known-to-closed"),
    ],
)
def test_0039_check_constraints_reject_violations(
    tmp_path: Path,
    overrides: dict[str, object],
    constraint: str,
) -> None:
    """Every CHECK the migration declares actually refuses its violation.

    Given: A database migrated to 0039.
    When: A row violating one CHECK is inserted directly.
    Then: The insert is refused by that CHECK. The always-open ``known_to`` is
        load-bearing here: an observation that could be closed would let a
        correction's durability proof be retired without deleting anything.
    """
    db_url = f"sqlite:///{tmp_path / 'checks.db'}"
    command.upgrade(_config(db_url), "0039")
    engine = sa.create_engine(db_url)

    with pytest.raises(sa.exc.IntegrityError, match=constraint):
        _insert(engine, **overrides)
    engine.dispose()


def test_0039_observation_uniqueness_is_total(tmp_path: Path) -> None:
    """One correction admits exactly one observation, under either spelling.

    Given: A database migrated to 0039 holding one observation.
    When: A second observation is inserted for the same correction — once by
        repeating its public id, once by repeating its surrogate id.
    Then: Both are refused. Neither index carries a ``known_to`` predicate, so
        the refusal is TOTAL: a maintenance retry cannot mint a later
        observation and thereby move a correction's proven knowledge instant
        forward, which is the one way this ledger could be made to lie.
    """
    db_url = f"sqlite:///{tmp_path / 'unique.db'}"
    command.upgrade(_config(db_url), "0039")
    engine = sa.create_engine(db_url)
    _insert(engine)

    with pytest.raises(
        sa.exc.IntegrityError,
        match="execution_annulment_visibility.annulment_public_id",
    ):
        _insert(
            engine,
            annulment_id=2,
            public_id="0000face-0000-7000-8000-0000000000b1",
        )
    with pytest.raises(
        sa.exc.IntegrityError,
        match="execution_annulment_visibility.annulment_id",
    ):
        _insert(
            engine,
            annulment_public_id="0000face-0000-7000-8000-0000000000a2",
            public_id="0000face-0000-7000-8000-0000000000b2",
        )
    engine.dispose()
