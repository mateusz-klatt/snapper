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
