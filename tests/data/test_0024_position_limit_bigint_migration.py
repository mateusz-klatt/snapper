"""Tests for the dual-dialect position-limit widening migration."""

import importlib
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations

from snapper.data.models import InstrumentSpec

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-14 08:00:00.000000"
_LARGE_LIMIT = 1_000_000_000_000


def _config(db_url: str) -> Config:
    """Build an Alembic configuration for one throwaway database."""
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def test_0024_sqlite_stores_large_limit_downgrade_and_reupgrade(tmp_path: Path) -> None:
    """SQLite stores a 10^12 position limit and survives downgrade/re-upgrade."""
    db_url = f"sqlite:///{tmp_path / 'position_limit.db'}"
    config = _config(db_url)
    command.upgrade(config, "0024")
    engine = sa.create_engine(db_url)
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO instrument_specs "
                "(public_id, instrument_public_id, session_id, sequence_id, timestamp, known_to, "
                "position_limit_long, position_limit_short) VALUES "
                "('spec-big', 'inst-big', 'session-big', 1, :timestamp, :known_to, :limit, :limit)"
            ),
            {"timestamp": _TS, "known_to": _ACTIVE, "limit": _LARGE_LIMIT},
        )
        stored = connection.execute(
            sa.text(
                "SELECT position_limit_long, position_limit_short FROM instrument_specs "
                "WHERE public_id = 'spec-big'"
            )
        ).one()
    assert stored == (_LARGE_LIMIT, _LARGE_LIMIT)
    command.downgrade(config, "0023")
    command.upgrade(config, "0024")
    engine.dispose()


def test_0024_postgresql_compile_and_model_signature() -> None:
    """PostgreSQL DDL widens both limit columns to BIGINT and the ORM agrees."""
    migration = importlib.import_module(
        "snapper.data.migrations.versions.0024_position_limit_bigint"
    )
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    operations = Operations(context)
    with patch.object(migration, "op", operations):
        migration.upgrade()
        migration.downgrade()
    ddl = output.getvalue()
    assert migration.revision == "0024"
    assert migration.down_revision == "0023"
    assert "position_limit_long TYPE BIGINT" in ddl
    assert "position_limit_short TYPE BIGINT" in ddl
    assert "position_limit_long TYPE INTEGER" in ddl
    assert "position_limit_short TYPE INTEGER" in ddl
    for name in ("position_limit_long", "position_limit_short"):
        assert isinstance(InstrumentSpec.__table__.c[name].type, sa.BigInteger)
