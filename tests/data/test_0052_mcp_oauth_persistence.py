"""Tests for MCP OAuth persistence migration 0052."""

from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config

from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TABLES = (
    OAuthClient,
    OAuthGrant,
    OAuthAuthorizationRequest,
    OAuthAuthorizationCode,
    OAuthRefreshToken,
)


def _config(db_url: str) -> Config:
    """Build Alembic configuration for one throwaway database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _schema_fingerprint(engine: sa.Engine, table: str) -> dict[str, object]:
    """Return columns, keys, constraints, and indexes for one table."""
    inspector = sa.inspect(engine)
    indexes = inspector.get_indexes(table)
    return {
        "columns": [
            (column["name"], str(column["type"]), bool(column["nullable"]))
            for column in inspector.get_columns(table)
        ],
        "primary_key": inspector.get_pk_constraint(table)["constrained_columns"],
        "unique_constraints": sorted(
            (
                str(constraint["name"]),
                tuple(constraint["column_names"]),
            )
            for constraint in inspector.get_unique_constraints(table)
        ),
        "check_constraints": sorted(
            (
                str(constraint["name"]),
                str(constraint["sqltext"]),
            )
            for constraint in inspector.get_check_constraints(table)
        ),
        "indexes": sorted(
            (
                str(index["name"]),
                tuple(index["column_names"]),
                bool(index["unique"]),
                (
                    ""
                    if dict(index.get("dialect_options") or {}).get("sqlite_where") is None
                    else str(dict(index.get("dialect_options") or {})["sqlite_where"])
                ),
            )
            for index in indexes
        ),
    }


def test_0052_schema_matches_oauth_models_and_downgrades(tmp_path: Path) -> None:
    """Verify Alembic and ORM build identical OAuth tables.

    Given a blank SQLite database and the four OAuth ORM tables,
    When Alembic upgrades through 0052 and a second database uses ORM DDL,
    Then every table has identical structure and downgrade removes all four.
    """
    migrated_url = f"sqlite:///{tmp_path / 'migrated.db'}"
    config = _config(migrated_url)
    command.upgrade(config, "0052")
    migrated_engine = sa.create_engine(migrated_url)

    model_url = f"sqlite:///{tmp_path / 'model.db'}"
    model_engine = sa.create_engine(model_url)
    for model in _TABLES:
        model.__table__.create(model_engine)

    for model in _TABLES:
        table = model.__tablename__
        assert _schema_fingerprint(migrated_engine, table) == _schema_fingerprint(
            model_engine,
            table,
        )

    command.downgrade(config, "0051")
    migrated_tables = set(sa.inspect(migrated_engine).get_table_names())
    assert not {model.__tablename__ for model in _TABLES} & migrated_tables
    migrated_engine.dispose()
    model_engine.dispose()
