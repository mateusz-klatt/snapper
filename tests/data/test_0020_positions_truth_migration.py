"""Tests for the 0020 positions truth-provenance migration.

Verifies the three nullable provenance columns land, the valuation
columns relax to nullable, the SQLite batch recreate preserves the
active-row partial unique indexes and existing rows, and downgrade
coalesces honest NULLs to zero before re-tightening NOT NULL.
"""

from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-11 08:00:00.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL.

    Args:
        db_url: SQLAlchemy URL of the throwaway SQLite database.

    Returns:
        Configured :class:`Config` bound to ``db_url``.
    """
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _table_info(engine: sa.Engine) -> dict[str, tuple[str, int]]:
    """Return positions column metadata via PRAGMA table_info.

    Args:
        engine: Engine bound to the migrated SQLite database.

    Returns:
        Mapping of column name to (declared type, notnull flag).
    """
    with engine.begin() as conn:
        rows = conn.execute(sa.text("PRAGMA table_info(positions)")).all()
    return {row[1]: (row[2], row[3]) for row in rows}


def _index_names(engine: sa.Engine) -> set[str]:
    """Return the positions index names.

    Args:
        engine: Engine bound to the migrated SQLite database.

    Returns:
        Set of index names on the positions table.
    """
    with engine.begin() as conn:
        rows = conn.execute(sa.text("PRAGMA index_list(positions)")).all()
    return {row[1] for row in rows}


def _insert_position(
    engine: sa.Engine,
    *,
    public_id: str,
    instrument_public_id: str,
    average_price: float | None,
    unrealized_pnl: float | None,
    with_provenance: bool,
) -> None:
    """Insert one active position row via literal SQL.

    Args:
        engine: Engine bound to the migrated SQLite database.
        public_id: Row public id.
        instrument_public_id: Instrument identity.
        average_price: Entry price or honest NULL.
        unrealized_pnl: Unrealized PnL or honest NULL.
        with_provenance: Whether to also set the provenance columns
            (only valid after 0020).
    """
    columns = (
        "public_id, instrument_public_id, mode, quantity, average_price, "
        "unrealized_pnl, realized_pnl, wallet_public_id, session_id, "
        "sequence_id, timestamp, known_to"
    )
    values = (
        ":public_id, :instrument_public_id, 'paper', 1.5, :average_price, "
        ":unrealized_pnl, 10.0, 'wallet-1', 'session-1', 1, :ts, :active"
    )
    if with_provenance:
        columns += ", mark_price, marked_at, source_venue_event_id"
        values += ", 50050.0, :ts, 42"
    with engine.begin() as conn:
        conn.execute(
            sa.text(f"INSERT INTO positions ({columns}) VALUES ({values})"),
            {
                "public_id": public_id,
                "instrument_public_id": instrument_public_id,
                "average_price": average_price,
                "unrealized_pnl": unrealized_pnl,
                "ts": _TS,
                "active": _ACTIVE,
            },
        )


def test_0020_upgrade_adds_provenance_and_relaxes_nullability(tmp_path: Path) -> None:
    """Upgrade lands nullable provenance columns without losing rows.

    Given: a database migrated to 0019 carrying one active position row,
    When: 0020 is applied,
    Then: the provenance columns exist nullable, average_price and
        unrealized_pnl are nullable, the pre-existing row survives with
        NULL provenance, honest-NULL inserts succeed, and both
        active-row partial unique indexes survive the batch recreate.
    """
    db_url = f"sqlite:///{tmp_path / 'positions_0020.db'}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0019")
    engine = sa.create_engine(db_url)
    _insert_position(
        engine,
        public_id="pos-old",
        instrument_public_id="inst-1",
        average_price=50000.0,
        unrealized_pnl=25.0,
        with_provenance=False,
    )
    command.upgrade(cfg, "0020")
    info = _table_info(engine)
    assert info["mark_price"][1] == 0
    assert info["marked_at"][1] == 0
    assert info["source_venue_event_id"][1] == 0
    assert info["average_price"][1] == 0
    assert info["unrealized_pnl"][1] == 0
    with engine.begin() as conn:
        row = conn.execute(
            sa.text(
                "SELECT quantity, average_price, mark_price, marked_at, "
                "source_venue_event_id FROM positions WHERE public_id = 'pos-old'"
            )
        ).one()
    assert row[0] == 1.5
    assert row[1] == 50000.0
    assert row[2] is None
    assert row[3] is None
    assert row[4] is None
    _insert_position(
        engine,
        public_id="pos-null",
        instrument_public_id="inst-2",
        average_price=None,
        unrealized_pnl=None,
        with_provenance=True,
    )
    with engine.begin() as conn:
        nulls = conn.execute(
            sa.text(
                "SELECT average_price, unrealized_pnl, mark_price, "
                "source_venue_event_id FROM positions WHERE public_id = 'pos-null'"
            )
        ).one()
    assert nulls[0] is None
    assert nulls[1] is None
    assert nulls[2] == 50050.0
    assert nulls[3] == 42
    indexes = _index_names(engine)
    assert "uq_positions_instrument_public_id" in indexes
    assert "ix_positions_public_id" in indexes
    engine.dispose()


def test_0020_downgrade_coalesces_nulls_and_retightens(tmp_path: Path) -> None:
    """Downgrade drops provenance and restores NOT NULL after coalescing.

    Given: a database at 0020 with one row carrying honest NULL
        valuation fields,
    When: the migration is downgraded to 0019,
    Then: the provenance columns are gone, the NULLs became zero, and
        average_price plus unrealized_pnl are NOT NULL again.
    """
    db_url = f"sqlite:///{tmp_path / 'positions_0020_down.db'}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0020")
    engine = sa.create_engine(db_url)
    _insert_position(
        engine,
        public_id="pos-null",
        instrument_public_id="inst-1",
        average_price=None,
        unrealized_pnl=None,
        with_provenance=True,
    )
    command.downgrade(cfg, "0019")
    info = _table_info(engine)
    assert "mark_price" not in info
    assert "marked_at" not in info
    assert "source_venue_event_id" not in info
    assert info["average_price"][1] == 1
    assert info["unrealized_pnl"][1] == 1
    with engine.begin() as conn:
        row = conn.execute(
            sa.text(
                "SELECT average_price, unrealized_pnl FROM positions "
                "WHERE public_id = 'pos-null'"
            )
        ).one()
    assert row[0] == 0.0
    assert row[1] == 0.0
    engine.dispose()
