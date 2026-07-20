"""Tests for the canonical P&L series migration 0035.

Migration 0035 creates the brand-new ``portfolio_pnl_points`` SCD2 table. Since
migrations are excluded from the coverage gate, the SQLite branch is pinned here
explicitly: the upgrade creates the table with its three indexes, the downgrade
drops it cleanly and remains re-runnable, and each CHECK constraint that guards
the anchor/sample invariants and the honest-valuation contract actually rejects a
violating row. The PostgreSQL branch is exercised by ``db-init`` against the live
scratch database.
"""

from pathlib import Path
from typing import Final

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLE = "portfolio_pnl_points"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-20 08:00:00.000000"
_POINT_TIME = "2026-07-20 08:00:00.000000"
_INDEXES: Final[list[str]] = [
    "ix_portfolio_pnl_points_public_id",
    "ix_portfolio_pnl_points_series",
    "uq_portfolio_pnl_points_identity",
]
_ANCHOR_BASKET = '{"USD": 100.0}'

_INSERT_SQL = (
    "INSERT INTO portfolio_pnl_points ("
    "wallet_public_id, mode, valuation_ccy, point_time, point_kind, epoch_public_id, "
    "calc_version, valuation_status, realized_pnl, fee_pnl, accrual_pnl, unrealized_pnl, "
    "external_flow_adjustment, cash_usd, position_value_usd, drawdown, mark_source, mark_time, "
    "watermarks_json, opening_basket_json, contributions_json, "
    "public_id, session_id, sequence_id, timestamp, known_to) VALUES ("
    ":wallet_public_id, :mode, :valuation_ccy, :point_time, :point_kind, :epoch_public_id, "
    ":calc_version, :valuation_status, :realized_pnl, :fee_pnl, :accrual_pnl, :unrealized_pnl, "
    ":external_flow_adjustment, :cash_usd, :position_value_usd, :drawdown, :mark_source, "
    ":mark_time, :watermarks_json, :opening_basket_json, :contributions_json, "
    ":public_id, :session_id, :sequence_id, :sequence_id, :known_to)"
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _row(**overrides: object) -> dict[str, object]:
    """Build a valid ``anchor`` row, overriding named columns for CHECK probes."""
    base: dict[str, object] = {
        "wallet_public_id": "0000face-0000-7000-8000-000000000001",
        "mode": "live",
        "valuation_ccy": "USD",
        "point_time": _POINT_TIME,
        "point_kind": "anchor",
        "epoch_public_id": "0000face-0000-7000-8000-0000000000e0",
        "calc_version": "v1",
        "valuation_status": "complete",
        "realized_pnl": 0.0,
        "fee_pnl": 0.0,
        "accrual_pnl": 0.0,
        "unrealized_pnl": 0.0,
        "external_flow_adjustment": 0.0,
        "cash_usd": None,
        "position_value_usd": None,
        "drawdown": None,
        "mark_source": "candle_1m",
        "mark_time": _TS,
        "watermarks_json": '{"walutomat": 1}',
        "opening_basket_json": _ANCHOR_BASKET,
        "contributions_json": None,
        "public_id": "0000face-0000-7000-8000-0000000000a0",
        "session_id": "0000face-0000-7000-8000-0000000000b0",
        "sequence_id": 1,
        "known_to": _ACTIVE,
    }
    base.update(overrides)
    return base


def _insert(engine: sa.Engine, **overrides: object) -> None:
    """Insert one P&L point row into the throwaway database."""
    with engine.begin() as connection:
        connection.execute(sa.text(_INSERT_SQL), _row(**overrides))


def _index_names(engine: sa.Engine) -> list[str]:
    """Return the indexes present on the P&L points table in SQLite."""
    query = (
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=:t "
        "AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    with engine.connect() as connection:
        return [str(row[0]) for row in connection.execute(sa.text(query), {"t": _TABLE})]


def _has_table(engine: sa.Engine) -> bool:
    """Report whether the P&L points table exists in SQLite."""
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=:t"
    with engine.connect() as connection:
        return connection.execute(sa.text(query), {"t": _TABLE}).scalar() == _TABLE


def test_0035_upgrade_creates_table_downgrade_drops_and_is_rerunnable(tmp_path: Path) -> None:
    """The P&L points table round-trips cleanly on SQLite, repeatably.

    Given: A SQLite database migrated to 0034 without the P&L points table.
    When: It is upgraded to 0035, downgraded to 0034, and cycled once more.
    Then: The table and its three indexes exist exactly after each upgrade and
        are absent after each downgrade.
    """
    db_url = f"sqlite:///{tmp_path / 'roundtrip.db'}"
    config = _config(db_url)
    command.upgrade(config, "0034")
    engine = sa.create_engine(db_url)
    assert not _has_table(engine)

    command.upgrade(config, "0035")
    assert _has_table(engine)
    assert _index_names(engine) == _INDEXES

    command.downgrade(config, "0034")
    assert not _has_table(engine)

    command.upgrade(config, "0035")
    assert _has_table(engine)

    command.downgrade(config, "0034")
    assert not _has_table(engine)
    engine.dispose()


def test_0035_accepts_valid_anchor_and_incomplete_sample(tmp_path: Path) -> None:
    """A zero-baseline anchor and an incomplete sample both persist.

    Given: A SQLite database upgraded to 0035.
    When: A valid complete anchor and a valid incomplete sample are inserted.
    Then: Both rows are stored without violating any CHECK.
    """
    db_url = f"sqlite:///{tmp_path / 'valid.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0035")
    _insert(engine, public_id="p-anchor")
    _insert(
        engine,
        public_id="p-sample",
        point_time="2026-07-20 08:01:00.000000",
        point_kind="sample",
        valuation_status="incomplete",
        realized_pnl=5.0,
        unrealized_pnl=None,
        mark_source=None,
        mark_time=None,
        opening_basket_json=None,
    )
    with engine.connect() as connection:
        count = connection.execute(sa.text(f"SELECT COUNT(*) FROM {_TABLE}")).scalar()
    assert count == 2
    engine.dispose()


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"realized_pnl": 1.0}, id="anchor_nonzero_realized"),
        pytest.param({"opening_basket_json": None}, id="anchor_without_basket"),
        pytest.param(
            {"valuation_status": "complete", "unrealized_pnl": None}, id="complete_without_mark"
        ),
        pytest.param(
            {
                "valuation_status": "incomplete",
                "point_kind": "sample",
                "opening_basket_json": None,
                "realized_pnl": 1.0,
            },
            id="incomplete_with_unrealized",
        ),
        pytest.param({"point_kind": "bogus"}, id="bad_point_kind"),
        pytest.param({"valuation_status": "bogus", "unrealized_pnl": None}, id="bad_valuation"),
        pytest.param({"mode": "shadow"}, id="bad_mode"),
    ],
)
def test_0035_check_constraints_reject_violations(
    tmp_path: Path, overrides: dict[str, object]
) -> None:
    """Each guarded invariant physically rejects a violating row.

    Given: A SQLite database upgraded to 0035.
    When: A row violating an anchor-zero, honest-valuation, or vocabulary CHECK
        is inserted.
    Then: The database raises an integrity error rather than storing it.
    """
    db_url = f"sqlite:///{tmp_path / 'checks.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0035")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, **overrides)
    engine.dispose()


def test_0035_active_identity_is_unique(tmp_path: Path) -> None:
    """Two active rows for one (wallet, mode, ccy, point_time) collide.

    Given: A SQLite database upgraded to 0035 with one active anchor.
    When: A second active row for the same identity is inserted.
    Then: The partial-unique active index rejects the duplicate.
    """
    db_url = f"sqlite:///{tmp_path / 'identity.db'}"
    engine = sa.create_engine(db_url)
    command.upgrade(_config(db_url), "0035")
    _insert(engine, public_id="p-first")
    with pytest.raises(sa.exc.IntegrityError):
        _insert(engine, public_id="p-second")
    engine.dispose()
