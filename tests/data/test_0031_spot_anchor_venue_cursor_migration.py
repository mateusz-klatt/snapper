"""Tests for the spot anchor venue-cursor + chain-tip migration 0031.

Migration 0031 completes ``portfolio_spot_reconciliation_anchors`` with the
venue history cursor, the read-order instants, and the execution chain tip, and
narrows the watermark / boundary / inventory / timestamp CHECKs to the single
certified shape. Migrations are excluded from the coverage gate, so the dialect
branches are pinned here explicitly: the SQLite upgrade adds the NOT NULL
columns and swaps the CHECKs (a valid anchor then inserts, a degraded one is
refused), the downgrade removes them and restores the wide CHECKs, a populated
table refuses both directions, and offline ``--sql`` rendering is refused
before any DDL because the emptiness assert needs a live bind. The PostgreSQL
branch is exercised by ``db-init`` against the live scratch database.
"""

from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TABLE = "portfolio_spot_reconciliation_anchors"
_NEW_COLUMNS = frozenset(
    {
        "source_chain_tip",
        "venue_cursor_kind",
        "venue_cursor_scheme",
        "venue_cursor_value",
        "venue_cursor_requested_at",
        "venue_cursor_observed_at",
        "venue_cursor_confirmed_at",
        "source_watermark_requested_at",
        "source_watermark_captured_at",
    }
)
_CHAIN_TIP = "e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6"
_LEGACY_ANCHOR = {
    "public_id": "0000face-0000-7000-8000-000000000009",
    "wallet_public_id": "0000face-0000-7000-8000-000000000001",
    "exchange": "walutomat",
    "mode": "live",
    "venue_account_state_public_id": "0000face-0000-7000-8000-000000000002",
    "balance_observation_id": 41,
    "source_watermark_kind": "scope_sequence",
    "source_watermark": 0,
    "balances_json": '{"USD":"1"}',
    "first_request_started_at": "2026-07-18 08:00:00.000000",
    "first_request_completed_at": "2026-07-18 08:00:01.000000",
    "second_request_started_at": "2026-07-18 08:00:01.000000",
    "second_request_completed_at": "2026-07-18 08:00:02.000000",
    "boundary_status": "double_read_equal",
    "inventory_status": "certified_full",
    "margin_status": "cash",
    "provenance": "test",
    "session_id": "0000face-0000-7000-8000-000000000004",
    "sequence_id": 1,
    "timestamp": "2026-07-18 08:00:03.000000",
    "known_to": _ACTIVE,
}
_NEW_FIELDS = {
    "source_chain_tip": _CHAIN_TIP,
    "venue_cursor_kind": "account_history_item_id",
    "venue_cursor_scheme": "walutomat:api:account/history:v1",
    "venue_cursor_value": "5",
    "venue_cursor_requested_at": "2026-07-18 07:59:56.000000",
    "venue_cursor_observed_at": "2026-07-18 07:59:57.000000",
    "venue_cursor_confirmed_at": "2026-07-18 08:00:02.000000",
    "source_watermark_requested_at": "2026-07-18 07:59:58.000000",
    "source_watermark_captured_at": "2026-07-18 07:59:59.000000",
}


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _anchor_values(**overrides: object) -> dict[str, object]:
    """Build a full valid 0031-schema anchor row, with per-test overrides."""
    values: dict[str, object] = dict(_LEGACY_ANCHOR)
    values["source_watermark"] = 5
    values["boundary_status"] = "cursor_certified"
    values["inventory_status"] = "venue_reported_full"
    values.update(_NEW_FIELDS)
    values.update(overrides)
    return values


def _insert(engine: sa.Engine, values: dict[str, object]) -> None:
    """Insert one anchor row with the exact given columns."""
    columns = ", ".join(values)
    parameters = ", ".join(f":{name}" for name in values)
    with engine.begin() as connection:
        connection.execute(
            sa.text(f"INSERT INTO {_TABLE} ({columns}) VALUES ({parameters})"), values
        )


def _column_names(engine: sa.Engine) -> set[str]:
    """Return the anchor table's column names on SQLite."""
    with engine.connect() as connection:
        rows = connection.execute(sa.text(f"PRAGMA table_info({_TABLE})"))
        return {str(row[1]) for row in rows}


def test_0031_refuses_offline_sql_rendering(tmp_path: Path) -> None:
    """Offline ``--sql`` rendering is refused in both directions before any DDL.

    Given: A configuration for a SQLite database.
    When: The 0030 -> 0031 upgrade and the 0031 -> 0030 downgrade are rendered
        offline (``sql=True``), which has no live bind for the emptiness assert.
    Then: Each raises a ``RuntimeError`` naming the online requirement.
    """
    config = _config(f"sqlite:///{tmp_path / 'offline.db'}")
    with pytest.raises(RuntimeError, match="requires an online connection"):
        command.upgrade(config, "0030:0031", sql=True)
    with pytest.raises(RuntimeError, match="requires an online connection"):
        command.downgrade(config, "0031:0030", sql=True)


def test_0031_upgrade_adds_columns_and_downgrade_removes_them(tmp_path: Path) -> None:
    """The upgrade adds the nine columns; the downgrade removes them again.

    Given: A SQLite database migrated to 0030 with an empty anchor table.
    When: It is upgraded to 0031 and then downgraded back to 0030.
    Then: The nine cursor/chain-tip columns are present after the upgrade and
        absent after the downgrade.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0030")
    engine = sa.create_engine(db_url)
    assert _NEW_COLUMNS.isdisjoint(_column_names(engine))

    command.upgrade(config, "0031")
    assert _column_names(engine) >= _NEW_COLUMNS

    command.downgrade(config, "0030")
    assert _NEW_COLUMNS.isdisjoint(_column_names(engine))
    engine.dispose()


def test_0031_upgrade_accepts_a_certified_anchor(tmp_path: Path) -> None:
    """A fully certified anchor inserts after the upgrade.

    Given: A SQLite database migrated to 0031.
    When: A valid certified anchor with all nine new columns is inserted.
    Then: The insert succeeds and one row is present.
    """
    db_url = f"sqlite:///{tmp_path / 'accept.db'}"
    config = _config(db_url)
    command.upgrade(config, "0031")
    engine = sa.create_engine(db_url)
    _insert(engine, _anchor_values())
    with engine.connect() as connection:
        count = connection.execute(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()
    assert count == 1
    engine.dispose()


_REFUSED_ANCHORS = [
    pytest.param({"source_watermark": 0}, id="zero_watermark"),
    pytest.param({"boundary_status": "double_read_equal"}, id="uncertified_boundary"),
    pytest.param({"inventory_status": "certified_full"}, id="non_venue_reported_inventory"),
    pytest.param({"venue_cursor_kind": "unknown_kind"}, id="unknown_cursor_kind"),
    pytest.param({"venue_cursor_scheme": "Walutomat:api"}, id="uppercased_scheme"),
    pytest.param({"venue_cursor_scheme": "kraken:api"}, id="cross_venue_scheme"),
    pytest.param({"source_chain_tip": "abc"}, id="short_chain_tip"),
    pytest.param({"source_chain_tip": _CHAIN_TIP.upper()}, id="uppercased_chain_tip"),
    pytest.param({"venue_cursor_confirmed_at": "2026-07-18 07:00:00.000000"}, id="inverted_order"),
]


@pytest.mark.parametrize("override", _REFUSED_ANCHORS)
def test_0031_refuses_a_degraded_anchor(tmp_path: Path, override: dict[str, object]) -> None:
    """Each narrowed CHECK refuses its degraded anchor after the upgrade.

    Given: A SQLite database migrated to 0031.
    When: An anchor violating one tightened CHECK is inserted.
    Then: The insert is refused with an integrity error.
    """
    db_url = f"sqlite:///{tmp_path / 'refuse.db'}"
    config = _config(db_url)
    command.upgrade(config, "0031")
    engine = sa.create_engine(db_url)
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, _anchor_values(**override))
    engine.dispose()


def test_0031_upgrade_aborts_on_a_populated_anchor_table(tmp_path: Path) -> None:
    """A pre-existing degraded anchor aborts the upgrade with the revision intact.

    Given: A SQLite database at 0030 carrying one legacy cursor-less anchor.
    When: The 0031 upgrade runs.
    Then: It raises a ``RuntimeError`` naming the abort, and the schema stays at
        0030 (the new columns are never added).
    """
    db_url = f"sqlite:///{tmp_path / 'populated.db'}"
    config = _config(db_url)
    command.upgrade(config, "0030")
    engine = sa.create_engine(db_url)
    _insert(engine, dict(_LEGACY_ANCHOR))
    with pytest.raises(RuntimeError, match="migration 0031 aborted"):
        command.upgrade(config, "0031")
    assert _NEW_COLUMNS.isdisjoint(_column_names(engine))
    engine.dispose()


def test_0031_downgrade_refuses_while_an_anchor_exists(tmp_path: Path) -> None:
    """The downgrade refuses while any anchor row exists, before destructive DDL.

    Given: A SQLite database at 0031 carrying one certified anchor.
    When: The 0031 -> 0030 downgrade runs.
    Then: It raises a ``RuntimeError`` naming the downgrade abort, and the new
        columns remain (nothing was dropped).
    """
    db_url = f"sqlite:///{tmp_path / 'downgrade.db'}"
    config = _config(db_url)
    command.upgrade(config, "0031")
    engine = sa.create_engine(db_url)
    _insert(engine, _anchor_values())
    with pytest.raises(RuntimeError, match="migration 0031 downgrade aborted"):
        command.downgrade(config, "0030")
    assert _column_names(engine) >= _NEW_COLUMNS
    engine.dispose()
