"""Regression tests for decoded SQLite paths in schema-template fixtures."""

from pathlib import Path

import pytest
from sqlalchemy import text

from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository


@pytest.mark.parametrize(
    ("database", "disk_file"),
    [
        (":memory:", None),
        ("fixture space %.db", "fixture space %.db"),
        ("file::memory:?cache=shared&uri=true", None),
        ("file:fixture-uri.db?mode=rwc&uri=true", "fixture-uri.db"),
    ],
)
@pytest.mark.asyncio
async def test_async_schema_fixture_uses_actual_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database: str, disk_file: str | None
) -> None:
    """Create the async schema without treating URL escapes as disk paths.

    Given: a memory URL, a SQLite URI, or a filename containing escaped characters,
    When: the template-backed create_all fixture runs in an empty directory,
    Then: the actual database has its schema and no encoded file is created.
    """
    monkeypatch.chdir(tmp_path)
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{database}")
    try:
        await repository.create_all()
        async with repository.session() as session:
            count: int = (await session.execute(text("SELECT COUNT(*) FROM symbols"))).scalar_one()
        assert count == 0
    finally:
        await repository.engine.dispose()
    assert sorted(path.name for path in tmp_path.iterdir()) == (
        [] if disk_file is None else [disk_file]
    )


@pytest.mark.parametrize(
    ("database", "disk_file"),
    [
        (":memory:", None),
        ("fixture space %.db", "fixture space %.db"),
        ("file::memory:?cache=shared&uri=true", None),
        ("file:fixture-uri.db?mode=rwc&uri=true", "fixture-uri.db"),
    ],
)
def test_sync_schema_fixture_uses_actual_database(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, database: str, disk_file: str | None
) -> None:
    """Create the sync schema without copying to a rendered URL path.

    Given: a memory URL, a SQLite URI, or a filename containing escaped characters,
    When: the synchronous template-backed create_all fixture runs,
    Then: symbols can be queried and only the actual disk database exists.
    """
    monkeypatch.chdir(tmp_path)
    repository = DatabaseRepository(f"sqlite:///{database}")
    try:
        repository.create_all()
        with repository.engine.connect() as connection:
            count: int = connection.execute(text("SELECT COUNT(*) FROM symbols")).scalar_one()
        assert count == 0
    finally:
        repository.engine.dispose()
    assert sorted(path.name for path in tmp_path.iterdir()) == (
        [] if disk_file is None else [disk_file]
    )
