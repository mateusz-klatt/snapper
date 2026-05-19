"""Tests for Alembic migration behaviours added after the base schema."""

import importlib
import json
from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from pytest import MonkeyPatch

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


class _Dialect:
    """Minimal dialect stand-in for migration dispatch tests."""

    def __init__(self, name: str) -> None:
        self.name = name


class _Bind:
    """Minimal bind stand-in exposing a SQLAlchemy-like dialect."""

    def __init__(self, dialect_name: str) -> None:
        self.dialect = _Dialect(dialect_name)


def _make_alembic_config(db_url: str) -> Config:
    """Build Alembic config pointing at a test database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_underlying(
    engine: sa.Engine,
    *,
    ticker: str,
    description: str | None,
) -> None:
    """Insert one 0001-compatible underlying row."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO underlying_assets "
                "(public_id, name, ticker, asset_class, sector, description, "
                "session_id, sequence_id, timestamp, known_to) "
                "VALUES "
                "(:public_id, :name, :ticker, :asset_class, :sector, :description, "
                ":session_id, :sequence_id, :timestamp, :known_to)"
            ),
            {
                "public_id": f"ua-{ticker.lower()}",
                "name": ticker,
                "ticker": ticker,
                "asset_class": "index",
                "sector": None,
                "description": description,
                "session_id": "session-1",
                "sequence_id": 1,
                "timestamp": "2026-01-01 00:00:00.000000",
                "known_to": "9999-12-31 23:59:59.000000",
            },
        )


def _fetch_description(engine: sa.Engine, ticker: str) -> object:
    """Fetch the raw stored description value for one underlying."""
    with engine.begin() as conn:
        return conn.execute(
            sa.text("SELECT description FROM underlying_assets WHERE ticker = :ticker"),
            {"ticker": ticker},
        ).scalar_one()


def test_upgrade_string_to_json_preserves_existing_value_under_en_key(tmp_path: Path) -> None:
    """SQLite upgrade wraps an existing scalar description under ``en``."""
    db_path = tmp_path / "upgrade_string.db"
    cfg = _make_alembic_config(f"sqlite:///{db_path}")
    command.upgrade(cfg, "0001")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    _insert_underlying(engine, ticker="SPX", description="S&P 500 exposure.")
    command.upgrade(cfg, "head")
    value = _fetch_description(engine, "SPX")
    assert json.loads(str(value)) == {"en": "S&P 500 exposure."}
    engine.dispose()


def test_downgrade_json_to_string_extracts_en_key(tmp_path: Path) -> None:
    """SQLite downgrade extracts the English value from the JSON map."""
    db_path = tmp_path / "downgrade_json.db"
    cfg = _make_alembic_config(f"sqlite:///{db_path}")
    command.upgrade(cfg, "0001")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    _insert_underlying(engine, ticker="SPX", description="S&P 500 exposure.")
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "0001")
    assert _fetch_description(engine, "SPX") == "S&P 500 exposure."
    engine.dispose()


def test_upgrade_handles_null_description(tmp_path: Path) -> None:
    """SQLite upgrade keeps null descriptions null."""
    db_path = tmp_path / "upgrade_null.db"
    cfg = _make_alembic_config(f"sqlite:///{db_path}")
    command.upgrade(cfg, "0001")
    engine = sa.create_engine(f"sqlite:///{db_path}")
    _insert_underlying(engine, ticker="NULLX", description=None)
    command.upgrade(cfg, "head")
    assert _fetch_description(engine, "NULLX") is None
    engine.dispose()


def test_upgrade_dispatches_postgres_path(monkeypatch: MonkeyPatch) -> None:
    """Upgrade dispatches the Postgres branch without a live Postgres DB."""
    migration = importlib.import_module(
        "snapper.data.migrations.versions.0002_underlying_description_json"
    )
    calls: list[str] = []

    def get_bind() -> _Bind:
        return _Bind("postgresql")

    def upgrade_postgresql() -> None:
        calls.append("postgresql")

    def upgrade_sqlite() -> None:
        calls.append("sqlite")

    monkeypatch.setattr(migration.op, "get_bind", get_bind)
    monkeypatch.setattr(migration, "_upgrade_postgresql", upgrade_postgresql)
    monkeypatch.setattr(migration, "_upgrade_sqlite", upgrade_sqlite)
    migration.upgrade()
    assert calls == ["postgresql"]


def _load_0003() -> object:
    """Import the 0003 partial-indexes-pg-counterparts migration module."""
    return importlib.import_module(
        "snapper.data.migrations.versions.0003_partial_indexes_pg_counterparts"
    )


def _stub_op_index_capture(
    monkeypatch: MonkeyPatch,
    migration: object,
    bind_dialect: str,
) -> tuple[list[tuple[str, str]], list[tuple[str, str, list[str], dict[str, object]]]]:
    """Replace ``op.get_bind``/``drop_index``/``create_index`` with capturing stubs."""
    drops: list[tuple[str, str]] = []
    creates: list[tuple[str, str, list[str], dict[str, object]]] = []

    def get_bind() -> _Bind:
        return _Bind(bind_dialect)

    def drop_index(name: str, table_name: str = "") -> None:
        drops.append((name, table_name))

    def create_index(
        name: str,
        table_name: str,
        columns: list[str],
        **kwargs: object,
    ) -> None:
        creates.append((name, table_name, columns, kwargs))

    monkeypatch.setattr(migration.op, "get_bind", get_bind)
    monkeypatch.setattr(migration.op, "drop_index", drop_index)
    monkeypatch.setattr(migration.op, "create_index", create_index)
    return drops, creates


def test_0003_upgrade_on_postgres_replaces_four_indexes_with_partial(
    monkeypatch: MonkeyPatch,
) -> None:
    """Upgrade on PG drops 4 full indexes and recreates them as partial with PG syntax.

    Given: an existing PG database where four ``sqlite_where``-only partial
    indexes were materialised as full indexes (because no
    ``postgresql_where`` was declared),
    When: ``0003.upgrade()`` runs against the PG dialect,
    Then: each index is dropped and recreated with the appropriate
    ``postgresql_where`` predicate using PG-correct boolean syntax
    (``can_trade = true``, not ``can_trade = 1``).
    """
    migration = _load_0003()
    drops, creates = _stub_op_index_capture(monkeypatch, migration, "postgresql")
    migration.upgrade()

    assert [(name, tbl) for name, tbl in drops] == [
        ("ix_sec_exchange_trade", "symbol_exchange_capabilities"),
        ("ix_sec_exchange_md", "symbol_exchange_capabilities"),
        ("uq_executions_order_exec", "executions"),
        ("uq_executions_order_trade", "executions"),
    ]
    assert [(name, tbl, cols) for name, tbl, cols, _ in creates] == [
        ("ix_sec_exchange_trade", "symbol_exchange_capabilities", ["exchange", "can_trade"]),
        ("ix_sec_exchange_md", "symbol_exchange_capabilities", ["exchange", "can_market_data"]),
        ("uq_executions_order_exec", "executions", ["order_public_id", "exec_id"]),
        ("uq_executions_order_trade", "executions", ["order_public_id", "trade_id"]),
    ]
    where_sql = [str(kwargs["postgresql_where"]) for _, _, _, kwargs in creates]
    assert where_sql == [
        "can_trade = true",
        "can_market_data = true",
        "exec_id IS NOT NULL",
        "trade_id IS NOT NULL",
    ]
    assert creates[2][3]["unique"] is True
    assert creates[3][3]["unique"] is True


def test_0003_upgrade_on_sqlite_is_noop(monkeypatch: MonkeyPatch) -> None:
    """Upgrade on SQLite touches no indexes — existing partial indexes remain.

    Given: a SQLite database whose four partial indexes are already
    correct (declared via ``sqlite_where`` in 0001),
    When: ``0003.upgrade()`` runs against the SQLite dialect,
    Then: no DROP or CREATE INDEX statement is emitted.
    """
    migration = _load_0003()
    drops, creates = _stub_op_index_capture(monkeypatch, migration, "sqlite")
    migration.upgrade()
    assert drops == []
    assert creates == []


def test_0003_downgrade_on_postgres_restores_four_full_indexes(
    monkeypatch: MonkeyPatch,
) -> None:
    """Downgrade on PG drops 4 partial indexes and recreates them as full.

    Given: a PG database upgraded to 0003 (four partial indexes),
    When: ``0003.downgrade()`` runs against the PG dialect,
    Then: each index is dropped and recreated WITHOUT ``postgresql_where``
    (restoring the pre-0003 full-index shape).
    """
    migration = _load_0003()
    drops, creates = _stub_op_index_capture(monkeypatch, migration, "postgresql")
    migration.downgrade()
    assert len(drops) == 4
    assert len(creates) == 4
    for _, _, _, kwargs in creates:
        assert "postgresql_where" not in kwargs


def test_0003_downgrade_on_sqlite_is_noop(monkeypatch: MonkeyPatch) -> None:
    """Downgrade on SQLite touches no indexes.

    Given: a SQLite database (whose partial indexes were never modified by
    0003 upgrade),
    When: ``0003.downgrade()`` runs against the SQLite dialect,
    Then: no DROP or CREATE INDEX statement is emitted.
    """
    migration = _load_0003()
    drops, creates = _stub_op_index_capture(monkeypatch, migration, "sqlite")
    migration.downgrade()
    assert drops == []
    assert creates == []
