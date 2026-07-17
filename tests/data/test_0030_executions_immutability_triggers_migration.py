"""Tests for the executions append-only trigger migration 0030.

Migration 0030 installs the physical append-only triggers on the
ALREADY-EXISTING production table (created by 0001, extended by 0025 and
0029). Migrations are excluded from the coverage gate, so the dialect
branches are pinned here explicitly: the SQLite upgrade installs the
triggers (a seeded row's raw DELETE is then physically refused), the
downgrade removes them (the raw DELETE then succeeds), a re-upgrade
restores them, and offline ``--sql`` rendering is refused before any DDL
because the install helper needs a live, dialect-aware bind. The
PostgreSQL upgrade branch is exercised by ``db-init`` against the live
scratch database and proven by the opt-in live-PostgreSQL adversarial
module.
"""

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-17 08:00:00.000000"
_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='executions' ORDER BY name"
)
_INSERT_EXECUTION_SQL = (
    "INSERT INTO executions (public_id, order_public_id, wallet_public_id, exchange, "
    "mode, scope_sequence, side, status, price, size, fee, fee_asset, session_id, "
    "sequence_id, timestamp, known_to) VALUES ('exec-1', 'order-1', "
    "'0000face-0000-7000-8000-000000000001', 'walutomat', 'live', 1, 'buy', 'filled', "
    "1.25, 2.0, 0.1, 'PLN', 'session-1', 1, :timestamp, :known_to)"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _trigger_names(engine: sa.Engine) -> list[str]:
    """Return the executions triggers present on a SQLite database."""
    with engine.connect() as connection:
        return [str(row[0]) for row in connection.execute(sa.text(_TRIGGER_QUERY))]


def _seed_execution(engine: sa.Engine) -> None:
    """Insert one sealed execution row (INSERT is always permitted)."""
    with engine.begin() as connection:
        connection.execute(sa.text(_INSERT_EXECUTION_SQL), {"timestamp": _TS, "known_to": _ACTIVE})


def _raw_delete_is_rejected(engine: sa.Engine) -> bool:
    """Return whether a raw DELETE of the seeded row is physically refused."""
    try:
        with engine.begin() as connection:
            connection.execute(sa.text("DELETE FROM executions WHERE public_id = 'exec-1'"))
        return False
    except sa.exc.DBAPIError as exc:
        return "append-only" in str(exc)


def test_0030_sqlite_upgrade_installs_downgrade_removes_reupgrade_restores(
    tmp_path: Path,
) -> None:
    """The upgrade installs the triggers, the downgrade removes them, re-upgrade restores.

    Given: A SQLite database migrated to 0029 with one sealed execution row.
    When: 0030 upgrades, then downgrades to 0029, then re-upgrades.
    Then: After the upgrade both triggers exist and a raw DELETE is
        physically refused; after the downgrade no executions trigger
        exists and the raw DELETE succeeds; after the re-upgrade the
        triggers are present again and the DELETE is refused once more.
    """
    db_url = f"sqlite:///{tmp_path / 'triggers.db'}"
    config = _config(db_url)
    command.upgrade(config, "0029")
    engine = sa.create_engine(db_url)
    _seed_execution(engine)

    command.upgrade(config, "0030")
    assert _trigger_names(engine) == ["executions_reject_delete", "executions_reject_update"]
    assert _raw_delete_is_rejected(engine) is True

    command.downgrade(config, "0029")
    assert _trigger_names(engine) == []
    assert _raw_delete_is_rejected(engine) is False

    _seed_execution(engine)
    command.upgrade(config, "0030")
    assert _trigger_names(engine) == ["executions_reject_delete", "executions_reject_update"]
    assert _raw_delete_is_rejected(engine) is True
    engine.dispose()


def test_0030_refuses_offline_sql_rendering(tmp_path: Path) -> None:
    """Offline ``--sql`` rendering is refused before any DDL is emitted.

    Given: A configuration for a SQLite database.
    When: The 0029 -> 0030 upgrade is rendered offline (``sql=True``), which
        has no live bind and no true dialect.
    Then: The migration raises a ``RuntimeError`` naming the online
        requirement — the install helper needs a live, dialect-aware bind.
    """
    config = _config(f"sqlite:///{tmp_path / 'offline.db'}")
    with pytest.raises(RuntimeError, match="requires an online connection"):
        command.upgrade(config, "0029:0030", sql=True)
