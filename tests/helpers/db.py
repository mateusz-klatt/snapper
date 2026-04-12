"""Test database helpers for fast schema initialization.

Instead of calling ``create_all()`` per test (~3-4s), copy a pre-built
template DB file (~1ms). Use the ``db_template_path`` session fixture
from conftest.py as the source.
"""

import shutil
from pathlib import Path

from snapper.data.repository import SQLAlchemyRepository


async def create_test_repo(
    tmp_path: Path, template: Path, name: str = "test.db"
) -> SQLAlchemyRepository:
    """Create a test repository from a pre-built template DB.

    Args:
        tmp_path: Pytest tmp_path for this test.
        template: Path to the template DB file (from db_template_path fixture).
        name: Filename for the test DB copy.

    Returns:
        SQLAlchemyRepository connected to the copied DB.
    """
    db_path = tmp_path / name
    shutil.copy2(template, db_path)
    return SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
