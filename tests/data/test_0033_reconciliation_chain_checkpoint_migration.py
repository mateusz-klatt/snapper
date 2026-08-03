"""Tests for the reconciliation chain-tip checkpoint migration 0033.

Migration 0033 adds the nullable ``source_chain_tip`` column with its
64-lowercase-hex format CHECK to the append-only reconciliation observations
and the SCD2 reconciliation states. Migrations are excluded from the coverage
gate, so the SQLite branch is pinned here explicitly: the upgrade adds the
column to both planes, the CHECK accepts NULL and a canonical tip while
refusing short, uppercased, and padded ones on both tables, and the downgrade
drops the column cleanly and remains re-runnable. The PostgreSQL branch is
exercised by ``db-init`` against the live scratch database.
"""

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_OBSERVATIONS = "portfolio_reconciliation_observations"
_STATES = "portfolio_reconciliation_states"
_COLUMN = "source_chain_tip"
_TIP = "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
_OBSERVATION = {
    "public_id": "0000face-0000-7000-8000-000000000011",
    "wallet_public_id": "0000face-0000-7000-8000-000000000001",
    "exchange": "walutomat",
    "mode": "live",
    "method": "unclassified",
    "evaluation_status": "incomplete",
    "resulting_full_mismatch_count": 0,
    "session_id": "0000face-0000-7000-8000-000000000004",
    "sequence_id": 1,
    "timestamp": "2026-07-18 08:00:00.000000",
    "known_to": _ACTIVE,
}
_STATE = {
    "public_id": "0000face-0000-7000-8000-000000000012",
    "wallet_public_id": "0000face-0000-7000-8000-000000000001",
    "exchange": "walutomat",
    "mode": "live",
    "method": "unclassified",
    "current_evaluation_status": "incomplete",
    "current_observation_id": 1,
    "consecutive_full_mismatches": 0,
    "session_id": "0000face-0000-7000-8000-000000000004",
    "sequence_id": 1,
    "timestamp": "2026-07-18 08:00:00.000000",
    "known_to": _ACTIVE,
}
_BASE_ROWS = {_OBSERVATIONS: _OBSERVATION, _STATES: _STATE}


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _insert(engine: sa.Engine, table: str, values: dict[str, object]) -> None:
    """Insert one row with the exact given columns."""
    columns = ", ".join(values)
    parameters = ", ".join(f":{name}" for name in values)
    with engine.begin() as connection:
        connection.execute(
            sa.text(f"INSERT INTO {table} ({columns}) VALUES ({parameters})"), values
        )


def _row_values(table: str, tip: str | None) -> dict[str, object]:
    """Build one otherwise-valid row for ``table`` carrying ``tip``."""
    values: dict[str, object] = dict(_BASE_ROWS[table])
    values[_COLUMN] = tip
    return values


def _has_chain_tip_column(engine: sa.Engine, table: str) -> bool:
    """Report whether the chain-tip column exists on ``table`` in SQLite."""
    with engine.connect() as connection:
        rows = connection.execute(sa.text(f"PRAGMA table_info({table})"))
        return _COLUMN in {str(row[1]) for row in rows}


def test_0033_upgrade_adds_and_rerunnable_downgrade_drops_both_columns(
    tmp_path: Path,
) -> None:
    """The chain-tip column round-trips cleanly on both planes, repeatably.

    Given: A SQLite database migrated to 0032 without the chain-tip column.
    When: It is upgraded to 0033, downgraded to 0032, and cycled once more.
    Then: Both planes carry the column exactly after each upgrade and shed it
        exactly after each downgrade.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0032")
    engine = sa.create_engine(db_url)
    assert not _has_chain_tip_column(engine, _OBSERVATIONS)
    assert not _has_chain_tip_column(engine, _STATES)

    command.upgrade(config, "0033")
    assert _has_chain_tip_column(engine, _OBSERVATIONS)
    assert _has_chain_tip_column(engine, _STATES)

    command.downgrade(config, "0032")
    assert not _has_chain_tip_column(engine, _OBSERVATIONS)
    assert not _has_chain_tip_column(engine, _STATES)

    command.upgrade(config, "0033")
    assert _has_chain_tip_column(engine, _OBSERVATIONS)
    assert _has_chain_tip_column(engine, _STATES)

    command.downgrade(config, "0032")
    assert not _has_chain_tip_column(engine, _OBSERVATIONS)
    assert not _has_chain_tip_column(engine, _STATES)
    engine.dispose()


@pytest.mark.parametrize("table", [_OBSERVATIONS, _STATES])
@pytest.mark.parametrize("tip", [None, _TIP])
def test_0033_accepts_a_null_and_a_canonical_chain_tip(
    tmp_path: Path,
    table: str,
    tip: str | None,
) -> None:
    """The format CHECK admits the honest pre-0033 value and a canonical tip.

    Given: A SQLite database migrated to 0033.
    When: An otherwise-valid row with a NULL or 64-lowercase-hex tip inserts.
    Then: The insert succeeds and one row is present.

    Args:
        tmp_path: Pytest temporary directory.
        table: Reconciliation plane under test.
        tip: Chain-tip value under test.
    """
    db_url = f"sqlite:///{tmp_path / 'accept.db'}"
    config = _config(db_url)
    command.upgrade(config, "0033")
    engine = sa.create_engine(db_url)
    _insert(engine, table, _row_values(table, tip))
    with engine.connect() as connection:
        count = connection.execute(sa.text(f"SELECT COUNT(*) FROM {table}")).scalar()
    assert count == 1
    engine.dispose()


@pytest.mark.parametrize("table", [_OBSERVATIONS, _STATES])
@pytest.mark.parametrize(
    "tip",
    [
        pytest.param("abc", id="short"),
        pytest.param(_TIP.upper(), id="uppercased"),
        pytest.param(" " + _TIP[1:], id="padded"),
    ],
)
def test_0033_rejects_a_degraded_chain_tip(
    tmp_path: Path,
    table: str,
    tip: str,
) -> None:
    """The format CHECK refuses every non-canonical tip on both planes.

    Given: A SQLite database migrated to 0033.
    When: An otherwise-valid row with a short, uppercased, or padded tip
        inserts.
    Then: The insert is refused with an integrity error.

    Args:
        tmp_path: Pytest temporary directory.
        table: Reconciliation plane under test.
        tip: Degraded chain-tip value under test.
    """
    db_url = f"sqlite:///{tmp_path / 'refuse.db'}"
    config = _config(db_url)
    command.upgrade(config, "0033")
    engine = sa.create_engine(db_url)
    s5778_value_1 = _row_values(table, tip)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, table, s5778_value_1)
    engine.dispose()
