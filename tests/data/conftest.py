"""Shared fixtures for data-layer tests.

Provides a worker-local, session-scoped SQLite template already upgraded
to the Alembic head so migration tests copy a file instead of re-running
the full 0001->head chain for every test. Exactly one real blank-database
``upgrade head`` still executes per xdist worker (preserving the chain
guarantee), while each test receives a private copy under its own
``tmp_path`` — downgrade and constraint probes therefore cannot leak
across tests or workers.
"""

import shutil
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"


def make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL.

    Args:
        db_url: SQLAlchemy URL the migration commands should run against.

    Returns:
        A Config bound to the repository ``alembic.ini`` with the URL set.
    """
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture(scope="session")
def alembic_template_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Return a worker-local SQLite file migrated once to the Alembic head.

    Built from a blank database with the real ``upgrade head`` chain, so
    copies of this file prove the migrated schema exactly as a per-test
    upgrade would. ``tmp_path_factory`` roots are unique per xdist worker,
    which keeps concurrent workers from sharing the template file.
    """
    template = tmp_path_factory.mktemp("alembic-template") / "template.db"
    command.upgrade(make_alembic_config(f"sqlite:///{template}"), "head")
    return template


@pytest.fixture
def migrated_db_path(alembic_template_db: Path, tmp_path: Path) -> Path:
    """Copy the migrated template into the test's private tmp directory.

    Returns:
        Path to a fresh head-migrated SQLite file owned by this test only.
    """
    db_path = tmp_path / "migrated.db"
    shutil.copyfile(alembic_template_db, db_path)
    return db_path
