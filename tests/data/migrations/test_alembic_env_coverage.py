"""Coverage tests for the Alembic environment driver branches."""

import importlib.util
from pathlib import Path
from types import ModuleType
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from alembic import context
from sqlalchemy import Connection

_ENVIRONMENT_PATH = Path(__file__).resolve().parents[3] / "src/snapper/data/migrations/env.py"


def _load_environment(
    *,
    offline: bool = True,
    config_file_name: str | None = None,
) -> ModuleType:
    """Load the Alembic script under a complete offline context proxy."""
    spec = importlib.util.spec_from_file_location("snapper_test_migration_env", _ENVIRONMENT_PATH)
    if spec is None:
        raise RuntimeError("Alembic environment module could not be loaded")
    loader = spec.loader
    if loader is None:
        raise RuntimeError("Alembic environment loader is unavailable")
    module = importlib.util.module_from_spec(spec)
    config = MagicMock()
    config.config_file_name = config_file_name
    config.get_main_option.return_value = "sqlite:///offline.db"
    config.config_ini_section = "alembic"
    config.get_section.return_value = {"sqlalchemy.url": "sqlite:///online.db"}
    transaction = MagicMock()
    transaction.__enter__.return_value = None
    transaction.__exit__.return_value = False
    connection = MagicMock(spec=Connection)
    connection_context = MagicMock()
    connection_context.__enter__.return_value = connection
    connection_context.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = connection_context
    with (
        patch.object(context, "config", config, create=True),
        patch.object(context, "is_offline_mode", return_value=offline),
        patch.object(context, "begin_transaction", return_value=transaction),
        patch.object(context, "configure"),
        patch.object(context, "run_migrations"),
        patch("sqlalchemy.engine_from_config", return_value=engine),
        patch("logging.config.fileConfig") as file_config,
    ):
        loader.exec_module(module)
    if config_file_name is None:
        file_config.assert_not_called()
    else:
        file_config.assert_called_once_with(config_file_name)
    return module


def _async_engine() -> tuple[MagicMock, AsyncMock]:
    """Build an async-engine double with a working connection context."""
    connection = AsyncMock()
    context_manager = MagicMock()
    context_manager.__aenter__ = AsyncMock(return_value=connection)
    context_manager.__aexit__ = AsyncMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = context_manager
    engine.execution_options.return_value = engine
    engine.dispose = AsyncMock()
    return engine, connection


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("url", "expects_isolation"),
    [
        ("postgresql+asyncpg://db/snapper", True),
        ("sqlite+aiosqlite:///snapper.db", False),
    ],
)
async def test_async_environment_applies_migrations_and_disposes(
    url: str,
    expects_isolation: bool,
) -> None:
    """Both async-engine forms bridge once and always dispose the engine."""
    migration_env = _load_environment()
    engine, connection = _async_engine()
    section = {"sqlalchemy.url": url}

    with patch.object(migration_env, "async_engine_from_config", return_value=engine):
        await migration_env._run_async_migrations(section)

    assert engine.execution_options.called is expects_isolation
    if expects_isolation:
        engine.execution_options.assert_called_once_with(isolation_level="READ COMMITTED")
    connection.run_sync.assert_awaited_once_with(migration_env._apply_migrations)
    engine.dispose.assert_awaited_once_with()


def test_online_environment_routes_asyncpg_through_async_driver() -> None:
    """The public online path executes the asyncpg bridge to completion."""
    migration_env = _load_environment()
    engine, connection = _async_engine()
    section = {"sqlalchemy.url": "postgresql+asyncpg://db/snapper"}

    with (
        patch.object(migration_env.config, "get_section", return_value=section),
        patch.object(migration_env, "async_engine_from_config", return_value=engine),
    ):
        migration_env.run_migrations_online()

    connection.run_sync.assert_awaited_once_with(migration_env._apply_migrations)
    engine.dispose.assert_awaited_once_with()


def test_online_environment_rewrites_aiosqlite_to_sync_driver() -> None:
    """The online SQLite path strips the async driver before engine creation."""
    migration_env = _load_environment()
    section = {"sqlalchemy.url": "sqlite+aiosqlite:///snapper.db"}
    connection = MagicMock(spec=Connection)
    connection_context = MagicMock()
    connection_context.__enter__.return_value = connection
    connection_context.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = connection_context

    with (
        patch.object(migration_env.config, "get_section", return_value=section),
        patch.object(migration_env, "engine_from_config", return_value=engine) as factory,
        patch.object(migration_env, "_apply_migrations") as apply_migrations,
    ):
        migration_env.run_migrations_online()

    assert section["sqlalchemy.url"] == "sqlite:///snapper.db"
    factory.assert_called_once()
    apply_migrations.assert_called_once_with(connection)


def test_environment_load_without_logging_config_runs_offline() -> None:
    """A config without an ini filename skips logging and runs offline SQL."""
    migration_env = _load_environment()

    assert migration_env.config.config_file_name is None


def test_environment_load_with_logging_config_runs_online() -> None:
    """The import-time dispatcher configures logging and runs a sync engine."""
    migration_env = _load_environment(offline=False, config_file_name="alembic.ini")

    assert migration_env.config.config_file_name == "alembic.ini"
