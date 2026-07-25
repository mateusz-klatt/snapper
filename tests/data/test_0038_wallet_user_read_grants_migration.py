"""Tests for the wallet user read-grant migration 0038.

Migration 0038 creates the brand-new bitemporal ``wallet_user_read_grants``
table. Migrations are excluded from the coverage gate, so the SQLite branch is
pinned explicitly: the upgrade creates the table with its four indexes, the
downgrade drops all of it and stays re-runnable, and the active partial unique
index physically admits many users on one wallet while refusing a second ACTIVE
row for one ``(user, wallet)`` pair — the property the read plane exists for.

The module also pins MIGRATION/MODEL PARITY, which no other check covers: the
Alembic-built production schema and the ``create_all``-built schema every test
fixture uses are compared column by column and index by index, INCLUDING each
index's partial predicate. The predicate has to be in the comparison: it is the
only thing that scopes the unique indexes to active rows, and a fingerprint of
``(name, columns, unique)`` alone would let a migration whose sentinel spelling
drifts from the model's pass unnoticed. Without that the two could drift
silently — the ORM model would keep passing every unit test while production
carried a different table. The PostgreSQL branch is exercised by ``db-init``
against the live scratch database.
"""

from pathlib import Path
from typing import Final

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine.interfaces import ReflectedIndex

from snapper.data.models import WalletUserReadGrant

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLE = "wallet_user_read_grants"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-25 08:00:00.000000"
_CLOSED = "2026-07-25 09:00:00.000000"
_INDEXES: Final[list[str]] = [
    "ix_wallet_user_read_grants_public_id",
    "ix_wallet_user_read_grants_unique_active",
    "ix_wallet_user_read_grants_user",
    "ix_wallet_user_read_grants_wallet",
]
_USER_A = "0000face-0000-7000-8000-0000000000a1"
_USER_B = "0000face-0000-7000-8000-0000000000a2"
_WALLET = "0000face-0000-7000-8000-0000000000e1"

_INSERT_SQL = (
    "INSERT INTO wallet_user_read_grants ("
    "user_public_id, wallet_public_id, granted_by_user_public_id, note, "
    "public_id, session_id, sequence_id, timestamp, known_to) VALUES ("
    ":user_public_id, :wallet_public_id, :granted_by_user_public_id, :note, "
    ":public_id, :session_id, :sequence_id, :timestamp, :known_to)"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _row(**overrides: object) -> dict[str, object]:
    """Build a valid read-grant row, overriding named columns per probe."""
    base: dict[str, object] = {
        "user_public_id": _USER_A,
        "wallet_public_id": _WALLET,
        "granted_by_user_public_id": "0000face-0000-7000-8000-0000000000c1",
        "note": "uat observer",
        "public_id": "0000face-0000-7000-8000-0000000000b1",
        "session_id": "0000face-0000-7000-8000-0000000000d1",
        "sequence_id": 1,
        "timestamp": _TS,
        "known_to": _ACTIVE,
    }
    base.update(overrides)
    return base


def _insert(engine: sa.Engine, **overrides: object) -> None:
    """Insert one read-grant row into the throwaway database."""
    with engine.begin() as connection:
        connection.execute(sa.text(_INSERT_SQL), _row(**overrides))


def _index_names(engine: sa.Engine) -> list[str]:
    """Return the table's SQLite index names in sorted order."""
    query = (
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = :table "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    with engine.connect() as connection:
        return [str(row[0]) for row in connection.execute(sa.text(query), {"table": _TABLE})]


def _has_table(engine: sa.Engine) -> bool:
    """Report whether the read-grant table exists in SQLite."""
    return _TABLE in sa.inspect(engine).get_table_names()


def _index_predicate(index: ReflectedIndex) -> str:
    """Return one reflected index's SQLite partial predicate.

    Args:
        index: One entry as reflected by ``Inspector.get_indexes``.

    Returns:
        The ``WHERE`` clause text, or the empty string for a full index.
    """
    options: dict[str, object] = dict(index.get("dialect_options") or {})
    where = options.get("sqlite_where")
    return "" if where is None else str(where)


def _index_predicates(engine: sa.Engine) -> dict[str, str]:
    """Map each read-grant index name to its SQLite partial predicate."""
    inspector = sa.inspect(engine)
    return {str(index["name"]): _index_predicate(index) for index in inspector.get_indexes(_TABLE)}


def _schema_fingerprint(engine: sa.Engine) -> dict[str, object]:
    """Reduce one live read-grant schema to a comparable structural fingerprint."""
    inspector = sa.inspect(engine)
    return {
        "columns": [
            (column["name"], str(column["type"]), bool(column["nullable"]))
            for column in inspector.get_columns(_TABLE)
        ],
        "primary_key": inspector.get_pk_constraint(_TABLE)["constrained_columns"],
        "indexes": sorted(
            (
                str(index["name"]),
                tuple(index["column_names"]),
                bool(index["unique"]),
                _index_predicate(index),
            )
            for index in inspector.get_indexes(_TABLE)
        ),
    }


def test_0038_upgrade_creates_table_downgrade_drops_and_is_rerunnable(tmp_path: Path) -> None:
    """The read-grant table and its indexes round-trip cleanly, repeatably.

    Given: A SQLite database migrated to 0037 without the read-grant table.
    When: It is upgraded to 0038, downgraded to 0037, and cycled once more.
    Then: The table and its four indexes exist exactly after each upgrade and
        are all absent after each downgrade, so the migration can be rolled
        back and re-applied without leaving orphaned DDL behind.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0037")
    engine = sa.create_engine(db_url)
    assert not _has_table(engine)

    command.upgrade(config, "0038")
    assert _has_table(engine)
    assert _index_names(engine) == _INDEXES

    command.downgrade(config, "0037")
    assert not _has_table(engine)

    command.upgrade(config, "0038")
    assert _has_table(engine)
    assert _index_names(engine) == _INDEXES

    command.downgrade(config, "0037")
    assert not _has_table(engine)
    engine.dispose()


def test_0038_schema_matches_the_orm_model_exactly(tmp_path: Path) -> None:
    """The Alembic-built and ``create_all``-built tables are the same schema.

    Given: One SQLite database built by migrating to 0038 and one built by
        creating ``WalletUserReadGrant.__table__`` directly.
    When: Both schemas are reduced to a structural fingerprint of columns,
        primary key, and indexes — each index carrying its partial predicate.
    Then: The fingerprints are identical, so the production table Alembic
        builds and the table every test fixture builds cannot drift apart.
        Both predicates are additionally pinned to the exact active sentinel,
        so the two halves drifting TOGETHER onto a different spelling — which
        a pure migration-versus-model comparison cannot see — fails too.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    command.upgrade(_config(migrated_url), "0038")
    migrated_engine = sa.create_engine(migrated_url)

    model_url = f"sqlite:///{tmp_path / 'model.db'}"
    model_engine = sa.create_engine(model_url)
    WalletUserReadGrant.__table__.create(model_engine)

    assert _schema_fingerprint(migrated_engine) == _schema_fingerprint(model_engine)
    assert _index_predicates(migrated_engine) == {
        "ix_wallet_user_read_grants_public_id": f"known_to = '{_ACTIVE}'",
        "ix_wallet_user_read_grants_unique_active": f"known_to = '{_ACTIVE}'",
        "ix_wallet_user_read_grants_user": "",
        "ix_wallet_user_read_grants_wallet": "",
    }
    migrated_engine.dispose()
    model_engine.dispose()


def test_0038_granter_is_nullable_for_seed_provisioned_grants(tmp_path: Path) -> None:
    """A grant with no human granter stores NULL rather than being refused.

    Given: A SQLite database upgraded to 0038.
    When: A row is inserted with ``granted_by_user_public_id`` NULL and a NULL
        note, as the seed profile provisions them.
    Then: The insert is accepted and the column reads back NULL, so
        seed-provisioned provenance never has to name a stand-in admin.
    """
    db_url = f"sqlite:///{tmp_path / 'nullable.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0038")

    _insert(engine, granted_by_user_public_id=None, note=None)

    with engine.connect() as connection:
        stored = connection.execute(
            sa.text(f"SELECT granted_by_user_public_id, note FROM {_TABLE}")
        ).one()
    assert stored[0] is None
    assert stored[1] is None
    engine.dispose()


def test_0038_active_index_is_per_pair_not_per_wallet(tmp_path: Path) -> None:
    """One wallet admits many readers, one pair admits one active row.

    Given: A SQLite database upgraded to 0038 holding an active grant for
        user A on the wallet.
    When: A second user is granted an active row on the SAME wallet, and
        separately user A's pair is repeated with another active row.
    Then: The second USER is accepted while the repeated PAIR is rejected —
        the exact shape ``wallet_operator_scope_grants`` cannot express, whose
        exclusivity indexes omit the grantee identity entirely.
    """
    db_url = f"sqlite:///{tmp_path / 'pairs.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0038")
    _insert(engine)

    _insert(
        engine,
        user_public_id=_USER_B,
        public_id="0000face-0000-7000-8000-0000000000b2",
    )

    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, public_id="0000face-0000-7000-8000-0000000000b3")
    engine.dispose()


def test_0038_active_index_admits_a_successor_after_a_close(tmp_path: Path) -> None:
    """Closing a grant frees its pair, so re-granting is possible.

    Given: A SQLite database upgraded to 0038 holding one grant for a pair
        that has been SCD2-closed (``known_to`` set to a past instant rather
        than deleted).
    When: A new active row is inserted for the same pair.
    Then: It is accepted, proving the unique index is predicated on the
        active sentinel and constrains only live rows — revoked history stays
        on the table without blocking future grants.
    """
    db_url = f"sqlite:///{tmp_path / 'successor.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0038")
    _insert(engine, known_to=_CLOSED)

    _insert(engine, public_id="0000face-0000-7000-8000-0000000000b4")

    with engine.connect() as connection:
        stored = connection.execute(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()
    assert stored == 2
    engine.dispose()
