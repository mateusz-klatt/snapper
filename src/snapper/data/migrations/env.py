"""Alembic environment configuration for database migrations.

Configures Alembic to run migrations in online and offline modes,
handling async SQLite driver conversion for synchronous migration execution.
"""

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config
from sqlalchemy import pool

from snapper.data.models import Base

_SA_URL_KEY = "sqlalchemy.url"

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in offline mode.

    Configures the context with just a URL and not an Engine.
    Generates SQL script without database connection.
    """
    url = config.get_main_option(_SA_URL_KEY)
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in online mode.

    Creates an Engine and associates a connection with the context.
    Converts async SQLite driver to sync for migration execution.
    """
    section = config.get_section(config.config_ini_section) or {}
    db_url = section.get(_SA_URL_KEY, "")
    if "aiosqlite" in db_url:
        section[_SA_URL_KEY] = db_url.replace("sqlite+aiosqlite://", "sqlite://")
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
