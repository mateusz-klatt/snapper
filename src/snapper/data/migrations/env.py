"""Alembic environment configuration for database migrations.

Routes the sync Alembic ``context.run_migrations`` call onto whichever
driver is configured: a plain sync engine for ``sqlite://`` style
URLs (after rewriting ``aiosqlite`` → sync), or an async engine
bridged through ``Connection.run_sync`` for ``asyncpg`` URLs. The
async path keeps us off ``psycopg2`` — the runtime only ships
``asyncpg`` for Postgres, so adding a sync driver would balloon the
container layer just for migrations.
"""

import asyncio
from logging.config import fileConfig
from typing import Any

from alembic import context
from sqlalchemy import Connection
from sqlalchemy import engine_from_config
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from snapper.data.models import Base

_SA_URL_KEY = "sqlalchemy.url"

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
target_metadata = Base.metadata


def _apply_migrations(connection: Connection) -> None:
    """Bind the Alembic context to a sync connection and run pending revisions.

    Called directly from the sync engine path and through
    :meth:`AsyncConnection.run_sync` from the async path. The
    same body runs in both modes; the only difference is who is
    holding the underlying DBAPI handle.

    Args:
        connection: SQLAlchemy sync :class:`Connection` handle.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_offline() -> None:
    """Run migrations in offline mode.

    Configures the context with just a URL and not an Engine.
    Generates SQL script without database connection.
    """
    url = config.get_main_option(_SA_URL_KEY)
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async_migrations(section: dict[str, Any]) -> None:
    """Drive ``_apply_migrations`` from an async engine.

    Used for ``aiosqlite`` + ``asyncpg`` URLs that we cannot rewrite
    to a sync driver without adding another DBAPI dependency.

    Args:
        section: SQLAlchemy section dict from the Alembic config,
            already mutated by :func:`run_migrations_online` if the
            URL needed normalisation.
    """
    connectable = async_engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    async with connectable.connect() as connection:
        await connection.run_sync(_apply_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in online mode.

    Branches on the configured DBAPI:

    * ``asyncpg`` (Postgres) — async engine + ``run_sync`` bridge so
      the existing sync Alembic ``context.run_migrations`` works
      without pulling in ``psycopg2``.
    * ``aiosqlite`` — rewrite the URL back to the sync ``sqlite``
      driver and use the plain sync engine path. The codebase has
      had this rewrite since the original SQLite-only days; keeping
      it avoids spinning up a fresh aiosqlite event loop for what
      is a tiny one-shot migration script.
    * Anything else — assume sync driver in the URL, no rewrite.
    """
    section = config.get_section(config.config_ini_section) or {}
    db_url = section.get(_SA_URL_KEY, "")
    if "asyncpg" in db_url:
        asyncio.run(_run_async_migrations(section))
        return
    if "aiosqlite" in db_url:
        section[_SA_URL_KEY] = db_url.replace("sqlite+aiosqlite://", "sqlite://")
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        _apply_migrations(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
