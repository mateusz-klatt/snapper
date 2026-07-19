"""Tests for the executions exact-counter-amount migration 0034.

Migration 0034 adds the nullable ``counter_amount_decimal`` column to the
ALREADY-EXISTING ``executions`` table (created by 0001, extended by 0025 and
0029, and guarded by the 0030 append-only triggers). Migrations are excluded
from the coverage gate, so the SQLite branch is pinned here explicitly: the
upgrade adds the column as a metadata-only ``ADD COLUMN`` that leaves the
append-only triggers intact, the column accepts an exact decimal string while
the triggers still physically refuse a raw UPDATE/DELETE, and the downgrade
drops the column cleanly and remains re-runnable. The PostgreSQL branch is
exercised by ``db-init`` against the live scratch database.
"""

from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-19 08:00:00.000000"
_COLUMN = "counter_amount_decimal"
_TRIGGERS = ["executions_reject_delete", "executions_reject_update"]
_INSERT_EXECUTION_SQL = (
    "INSERT INTO executions (public_id, order_public_id, wallet_public_id, exchange, "
    "mode, scope_sequence, side, status, price, size, fee, fee_asset, "
    "counter_amount_decimal, session_id, sequence_id, timestamp, known_to) VALUES "
    "(:public_id, 'order-1', '0000face-0000-7000-8000-000000000001', 'walutomat', "
    "'live', :scope, 'buy', 'filled', 1.25, 2.0, 0.1, 'PLN', :counter, 'session-1', "
    ":scope, :timestamp, :known_to)"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _has_column(engine: sa.Engine) -> bool:
    """Report whether the counter-amount column exists on executions in SQLite."""
    with engine.connect() as connection:
        rows = connection.execute(sa.text("PRAGMA table_info(executions)"))
        return _COLUMN in {str(row[1]) for row in rows}


def _trigger_names(engine: sa.Engine) -> list[str]:
    """Return the executions append-only triggers present on a SQLite database."""
    query = (
        "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='executions' "
        "ORDER BY name"
    )
    with engine.connect() as connection:
        return [str(row[0]) for row in connection.execute(sa.text(query))]


def _seed_execution(engine: sa.Engine, *, scope: int, counter: str | None) -> None:
    """Insert one sealed execution row (INSERT is always permitted)."""
    with engine.begin() as connection:
        connection.execute(
            sa.text(_INSERT_EXECUTION_SQL),
            {
                "public_id": f"exec-{scope}",
                "scope": scope,
                "counter": counter,
                "timestamp": _TS,
                "known_to": _ACTIVE,
            },
        )


def _raw_delete_is_rejected(engine: sa.Engine, *, scope: int) -> bool:
    """Return whether a raw DELETE of the seeded row is physically refused."""
    try:
        with engine.begin() as connection:
            connection.execute(
                sa.text("DELETE FROM executions WHERE scope_sequence = :scope"), {"scope": scope}
            )
        return False
    except sa.exc.DBAPIError as exc:
        return "append-only" in str(exc)


def test_0034_upgrade_adds_column_downgrade_drops_and_is_rerunnable(
    tmp_path: Path,
) -> None:
    """The counter-amount column round-trips cleanly on SQLite, repeatably.

    Given: A SQLite database migrated to 0033 without the counter-amount column.
    When: It is upgraded to 0034, downgraded to 0033, and cycled once more.
    Then: The column is present exactly after each upgrade and absent after each
        downgrade, and the append-only triggers stay installed throughout.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0033")
    engine = sa.create_engine(db_url)
    assert not _has_column(engine)
    assert _trigger_names(engine) == _TRIGGERS

    command.upgrade(config, "0034")
    assert _has_column(engine)
    assert _trigger_names(engine) == _TRIGGERS

    command.downgrade(config, "0033")
    assert not _has_column(engine)
    assert _trigger_names(engine) == _TRIGGERS

    command.upgrade(config, "0034")
    assert _has_column(engine)
    assert _trigger_names(engine) == _TRIGGERS

    command.downgrade(config, "0033")
    assert not _has_column(engine)
    engine.dispose()


def test_0034_column_accepts_a_value_while_triggers_still_refuse_mutation(
    tmp_path: Path,
) -> None:
    """The new column stores an exact decimal without disarming the ledger guard.

    Given: A SQLite database upgraded through the 0030 append-only triggers to
        0034.
    When: A legacy row and a row carrying an exact counter amount are inserted.
    Then: The counter amount persists verbatim and a raw DELETE of any row is
        still physically refused by the surviving append-only triggers.
    """
    db_url = f"sqlite:///{tmp_path / 'guard.db'}"
    config = _config(db_url)
    command.upgrade(config, "0034")
    engine = sa.create_engine(db_url)
    _seed_execution(engine, scope=1, counter=None)
    _seed_execution(engine, scope=2, counter="30.30")
    with engine.connect() as connection:
        stored = connection.execute(
            sa.text("SELECT counter_amount_decimal FROM executions WHERE scope_sequence = 2")
        ).scalar()
    assert stored == "30.30"
    assert _raw_delete_is_rejected(engine, scope=1) is True
    engine.dispose()
