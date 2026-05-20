"""Tests for Alembic migration behaviours added after the base schema."""

import importlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from pytest import MonkeyPatch
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite

from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import Telemetry
from snapper.data.models import Tick
from snapper.data.models import Trade
from snapper.data.models import User

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


def _load_0004() -> object:
    """Import the 0004 bigint-pk-high-write-tables migration module."""
    return importlib.import_module(
        "snapper.data.migrations.versions.0004_bigint_pk_high_write_tables"
    )


def _stub_op_bigint_capture(
    monkeypatch: MonkeyPatch,
    migration: object,
    bind_dialect: str,
    *,
    sequence_factory: Callable[[str], str] | None = None,
    max_id_factory: Callable[[str], int] | None = None,
) -> tuple[list[str], _Bind]:
    """Capture ``op.execute`` SQL strings during a 0004 dispatch.

    ``sequence_factory(table)`` returns the owned sequence name for
    PG ``pg_get_serial_sequence`` lookups (default: ``<table>_id_seq``).
    ``max_id_factory(table)`` returns the simulated ``max(id)`` for PG
    downgrade safety checks (default: ``0`` so downgrade always
    passes).
    """
    executed: list[str] = []

    def _default_seq(table: str) -> str:
        return f"{table}_id_seq"

    def _default_max(_table: str) -> int:
        return 0

    seq_fn: Callable[[str], str] = sequence_factory or _default_seq
    max_fn: Callable[[str], int] = max_id_factory or _default_max

    class _StubBind:
        def __init__(self) -> None:
            self.dialect = _Dialect(bind_dialect)

        def execute(self, stmt: object, params: dict[str, str] | None = None) -> object:
            text = str(stmt)
            if "pg_get_serial_sequence" in text:
                table = (params or {}).get("t", "")
                seq = seq_fn(table)
                return _ScalarResult(seq)
            if text.upper().startswith("SELECT MAX(ID) FROM "):
                table = text.rsplit(maxsplit=1)[-1]
                return _ScalarResult(max_fn(table))
            executed.append(text)
            return _ScalarResult(None)

    class _ScalarResult:
        def __init__(self, value: object) -> None:
            self._value = value

        def scalar(self) -> object:
            return self._value

    bind = _StubBind()

    def get_bind() -> _Bind:
        return bind

    def op_execute(stmt: object) -> None:
        executed.append(str(stmt))

    monkeypatch.setattr(migration.op, "get_bind", get_bind)
    monkeypatch.setattr(migration.op, "execute", op_execute)
    return executed, bind


def test_0004_upgrade_on_postgres_alters_id_and_sequence_for_each_table(
    monkeypatch: MonkeyPatch,
) -> None:
    """Upgrade on PG emits ANALYZE + ALTER TABLE + ALTER SEQUENCE per table.

    Given: a PostgreSQL database whose high-write tables have INT4 PKs
    sourced from ``Integer`` columns,
    When: ``0004.upgrade()`` runs against the PG dialect,
    Then: each table receives ``ANALYZE`` pre-rewrite,
        ``ALTER TABLE ... ALTER COLUMN id TYPE BIGINT``,
        ``ALTER SEQUENCE <owned_seq> AS BIGINT``,
        and post-rewrite ``ANALYZE`` — for all 7 tables.
    """
    migration = _load_0004()
    executed, _ = _stub_op_bigint_capture(monkeypatch, migration, "postgresql")
    migration.upgrade()

    expected_tables = (
        "ticks",
        "trades",
        "candles",
        "market_snapshots",
        "executions",
        "orders",
        "telemetry",
    )
    for table in expected_tables:
        assert any(
            f"ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT" in s for s in executed
        ), f"missing ALTER TABLE for {table}"
        assert any(
            f"ALTER SEQUENCE {table}_id_seq AS BIGINT" in s for s in executed
        ), f"missing ALTER SEQUENCE for {table}"
    analyze_count = sum(1 for s in executed if s.startswith("ANALYZE "))
    assert analyze_count == 2 * len(
        expected_tables
    ), f"expected {2 * len(expected_tables)} ANALYZE (pre + post), got {analyze_count}"


def test_0004_upgrade_on_sqlite_is_noop(monkeypatch: MonkeyPatch) -> None:
    """Upgrade on SQLite emits no ALTER statements.

    Given: a SQLite database where INTEGER PRIMARY KEY is already 64-bit
    rowid,
    When: ``0004.upgrade()`` runs against the SQLite dialect,
    Then: no ALTER TABLE / ALTER SEQUENCE is emitted.
    """
    migration = _load_0004()
    executed, _ = _stub_op_bigint_capture(monkeypatch, migration, "sqlite")
    migration.upgrade()
    assert all("ALTER " not in s for s in executed)


def test_0004_downgrade_on_postgres_refuses_when_max_id_exceeds_int4(
    monkeypatch: MonkeyPatch,
) -> None:
    """Downgrade aborts loudly if any row's id is beyond INT4 range.

    Given: a PG database where ``ticks.max(id) = 3_000_000_000``
        (above INT4 limit ``2_147_483_647``),
    When: ``0004.downgrade()`` runs against the PG dialect,
    Then: ``RuntimeError`` is raised before any ALTER is executed —
        prevents silent data corruption.
    """
    migration = _load_0004()

    def overflowing_max(table: str) -> int:
        return 3_000_000_000 if table == "ticks" else 0

    executed, _ = _stub_op_bigint_capture(
        monkeypatch, migration, "postgresql", max_id_factory=overflowing_max
    )

    with pytest.raises(RuntimeError, match="ticks.*INT4"):
        migration.downgrade()
    assert all("ALTER " not in s for s in executed)


def test_0004_downgrade_on_postgres_safe_path_reverts_each_table(
    monkeypatch: MonkeyPatch,
) -> None:
    """Downgrade emits ALTER SEQUENCE + ALTER TABLE per table when safe.

    Given: a PG database where every table's ``max(id) <= INT4_MAX``,
    When: ``0004.downgrade()`` runs against the PG dialect,
    Then: per table, an ``ALTER SEQUENCE ... AS INTEGER`` precedes an
        ``ALTER TABLE ... ALTER COLUMN id TYPE INTEGER``.
    """
    migration = _load_0004()
    executed, _ = _stub_op_bigint_capture(monkeypatch, migration, "postgresql")
    migration.downgrade()
    for table in (
        "ticks",
        "trades",
        "candles",
        "market_snapshots",
        "executions",
        "orders",
        "telemetry",
    ):
        assert any(
            f"ALTER SEQUENCE {table}_id_seq AS INTEGER" in s for s in executed
        ), f"missing ALTER SEQUENCE downgrade for {table}"
        assert any(
            f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER" in s for s in executed
        ), f"missing ALTER TABLE downgrade for {table}"


def test_0004_downgrade_on_sqlite_is_noop(monkeypatch: MonkeyPatch) -> None:
    """Downgrade on SQLite emits no ALTER statements."""
    migration = _load_0004()
    executed, _ = _stub_op_bigint_capture(monkeypatch, migration, "sqlite")
    migration.downgrade()
    assert all("ALTER " not in s for s in executed)


def test_0004_resolve_owned_sequence_returns_none_when_no_sequence(
    monkeypatch: MonkeyPatch,
) -> None:
    """``_resolve_owned_sequence`` returns ``None`` when pg_get_serial_sequence yields NULL.

    Given: a column without an owned sequence (defensive — should not
    happen for ``autoincrement=True`` tables),
    When: ``_resolve_owned_sequence`` queries pg_get_serial_sequence,
    Then: the result is ``None``, so upgrade/downgrade skip the
    ALTER SEQUENCE step rather than fail loudly.
    """
    migration = _load_0004()

    class _NoSequenceBind:
        def execute(self, _stmt: object, _params: dict[str, str] | None = None) -> object:
            class _NullResult:
                def scalar(self) -> object:
                    return None

            return _NullResult()

    result = migration._resolve_owned_sequence(_NoSequenceBind(), "telemetry")
    assert result is None


def test_high_write_tables_have_bigint_pk() -> None:
    """The 7 high-write tables declare ``BigInteger().with_variant(Integer, 'sqlite')``.

    Given: the model declarations in ``snapper.data.models``,
    When: the table is rendered against a PostgreSQL dialect,
    Then: the ``id`` column type is ``BIGINT``;
        rendered against SQLite, it is ``INTEGER`` (so
        ``INTEGER PRIMARY KEY ROWID`` semantics are preserved).
    """
    for model in (Candle, Tick, Trade, Order, Execution, MarketSnapshot, Telemetry):
        pg_type = model.__table__.c.id.type.dialect_impl(postgresql.dialect())
        sqlite_type = model.__table__.c.id.type.dialect_impl(sqlite.dialect())
        assert "BIGINT" in pg_type.compile(dialect=postgresql.dialect()).upper(), (
            f"{model.__name__}.id should be BIGINT on PG, got "
            f"{pg_type.compile(dialect=postgresql.dialect())}"
        )
        assert "INTEGER" in sqlite_type.compile(dialect=sqlite.dialect()).upper(), (
            f"{model.__name__}.id should be INTEGER on SQLite, got "
            f"{sqlite_type.compile(dialect=sqlite.dialect())}"
        )


def test_other_tables_keep_integer_pk() -> None:
    """Tables NOT in the high-write set keep ``Integer`` (INT4) PK.

    Validates the explicit scoping decision: only volume-exposed
    tables get the BigInteger override. Migrating every table would
    be wasted DDL on tables with negligible growth (e.g. ``users``).
    """
    for model in (Instrument, Position, User):
        pg_type = model.__table__.c.id.type.dialect_impl(postgresql.dialect())
        rendered = pg_type.compile(dialect=postgresql.dialect()).upper()
        assert "BIGINT" not in rendered, f"{model.__name__}.id should NOT be BIGINT, got {rendered}"
        assert "INTEGER" in rendered, f"{model.__name__}.id should be INTEGER, got {rendered}"
