"""Exercise both generic SCD2 helpers against SQLite setting versions."""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import Table
from sqlalchemy import Update
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Setting
from snapper.data.repository import TemporalVersionConflictError
from snapper.data.repository import close_and_insert
from snapper.data.repository import close_and_insert_sync

_START = datetime(2026, 1, 1, tzinfo=UTC)
_BUS_TIME = datetime(2026, 1, 2, tzinfo=UTC)
_CLOSED_AT = datetime(2026, 1, 3, tzinfo=UTC)
_KEY = "temporal_guard_test"
_PUBLIC_ID = "00000000-0000-7000-8000-000000000001"
_SESSION_ID = "00000000-0000-7000-8000-000000000002"
_HISTORY = select(
    Setting.id, Setting.public_id, Setting.timestamp, Setting.known_to, Setting.value
).order_by(Setting.timestamp)


@pytest.fixture
def settings_db_path(tmp_path: Path) -> Path:
    """Create the real Setting table and its indexes in a private SQLite file.

    Args:
        tmp_path: Directory reserved for this test.

    Returns:
        Path to the initialized database.
    """
    path = tmp_path / "settings.db"
    engine = create_engine(f"sqlite:///{path}")
    try:
        cast(Table, Setting.__table__).create(engine)
    finally:
        engine.dispose()
    return path


@pytest.fixture
async def async_session(settings_db_path: Path) -> AsyncIterator[AsyncSession]:
    """Open an async SQLite session on the private database.

    Args:
        settings_db_path: Path to the initialized database.

    Yields:
        Session that the async helper can use to write versions.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{settings_db_path}")
    try:
        async with AsyncSession(engine) as session:
            yield session
    finally:
        await engine.dispose()


@pytest.fixture
def sync_session(settings_db_path: Path) -> Iterator[Session]:
    """Open a sync SQLite session on the private database.

    Args:
        settings_db_path: Path to the initialized database.

    Yields:
        Session that the synchronous helper can use to write versions.
    """
    engine = create_engine(f"sqlite:///{settings_db_path}")
    try:
        with Session(engine) as session:
            yield session
    finally:
        engine.dispose()


def _setting_values(value: str, sequence_id: int) -> dict[str, object]:
    """Return the payload and provenance for one fixture version."""
    return {
        "key": _KEY,
        "value": value,
        "category": "tests",
        "session_id": _SESSION_ID,
        "sequence_id": sequence_id,
    }


def _original_setting(known_to: datetime = KNOWN_TO_MAX) -> Setting:
    """Build the first version in the history with the supplied interval end."""
    return Setting(
        **_setting_values("original", 1),
        public_id=_PUBLIC_ID,
        timestamp=_START,
        known_to=known_to,
    )


def _current_successor() -> Setting:
    """Build the current version that starts at the original version's close."""
    return Setting(**_setting_values("successor", 3), public_id=_PUBLIC_ID, timestamp=_CLOSED_AT)


def _close_without_synchronizing(row_id: int) -> Update:
    """Close the stored version while deliberately leaving its cached identity stale."""
    return (
        update(Setting)
        .where(Setting.id == row_id)
        .values(known_to=_CLOSED_AT)
        .execution_options(synchronize_session=False)
    )


def _assert_conflict(error: TemporalVersionConflictError, row_id: int) -> None:
    """Check the conflict identifies the closed version and both timestamps."""
    assert error.model_name == "settings"
    assert error.row_id == row_id
    assert error.existing_known_to == _CLOSED_AT
    assert error.bus_time == _BUS_TIME


async def test_close_and_insert_replaces_current_version(async_session: AsyncSession) -> None:
    """Preserve the normal async version replacement.

    Given: An open-ended setting version.
    When: The async helper writes its replacement at the bus time.
    Then: The predecessor closes and the open successor carries its public ID.

    Args:
        async_session: Async session backed by a private SQLite database.
    """
    original = _original_setting()
    async_session.add(original)
    await async_session.flush()
    original_id = original.id
    replacement: Setting = await close_and_insert(
        async_session, Setting, [Setting.key == _KEY], _setting_values("replacement", 2), _BUS_TIME
    )
    await async_session.flush()
    replacement_id = replacement.id
    await async_session.commit()

    assert (await async_session.execute(_HISTORY)).all() == [
        (original_id, _PUBLIC_ID, _START, _BUS_TIME, "original"),
        (replacement_id, _PUBLIC_ID, _BUS_TIME, KNOWN_TO_MAX, "replacement"),
    ]


async def test_close_and_insert_inserts_first_version(async_session: AsyncSession) -> None:
    """Preserve the async insert for a missing logical key.

    Given: An empty settings table.
    When: The async helper writes a setting for the first time.
    Then: The new version has a public ID and an open interval starting at the bus time.

    Args:
        async_session: Async session backed by a private SQLite database.
    """
    inserted: Setting = await close_and_insert(
        async_session, Setting, [Setting.key == _KEY], _setting_values("first", 1), _BUS_TIME
    )
    await async_session.flush()
    assert inserted.public_id
    expected = (inserted.id, inserted.public_id, _BUS_TIME, KNOWN_TO_MAX, "first")
    await async_session.commit()

    assert (await async_session.execute(_HISTORY)).all() == [expected]


async def test_close_and_insert_refuses_closed_version(async_session: AsyncSession) -> None:
    """Keep a committed history intact when an async write targets its predecessor.

    Given: A finite predecessor and a newer open successor for the same key.
    When: The async helper writes at a bus time inside the predecessor's interval.
    Then: The typed conflict reports the predecessor and both versions stay intact.

    Args:
        async_session: Async session backed by a private SQLite database.
    """
    original, successor = _original_setting(_CLOSED_AT), _current_successor()
    async_session.add_all([original, successor])
    await async_session.flush()
    original_id, successor_id = original.id, successor.id
    await async_session.commit()

    with pytest.raises(
        TemporalVersionConflictError, match="settings version .* is no longer current"
    ) as conflict:
        await close_and_insert(
            async_session,
            Setting,
            [Setting.key == _KEY],
            _setting_values("backdated", 2),
            _BUS_TIME,
        )
    _assert_conflict(conflict.value, original_id)
    await async_session.commit()

    assert (await async_session.execute(_HISTORY)).all() == [
        (original_id, _PUBLIC_ID, _START, _CLOSED_AT, "original"),
        (successor_id, _PUBLIC_ID, _CLOSED_AT, KNOWN_TO_MAX, "successor"),
    ]


async def test_close_and_insert_refreshes_stale_identity(async_session: AsyncSession) -> None:
    """Require the async helper to see a close hidden by the identity map.

    Given: A cached open version closed by an unsynchronized update in the same session.
    When: The async helper writes at a bus time inside the newly closed interval.
    Then: The typed conflict reports the stored close and no successor is inserted.

    Args:
        async_session: Async session backed by a private SQLite database.
    """
    async_session.add(_original_setting())
    await async_session.commit()
    cached = (await async_session.scalars(select(Setting))).one()
    original_id = cached.id
    await async_session.execute(_close_without_synchronizing(original_id))
    assert cached.known_to == KNOWN_TO_MAX

    with pytest.raises(
        TemporalVersionConflictError, match="settings version .* is no longer current"
    ) as conflict:
        await close_and_insert(
            async_session,
            Setting,
            [Setting.key == _KEY],
            _setting_values("backdated", 2),
            _BUS_TIME,
        )
    _assert_conflict(conflict.value, original_id)
    await async_session.commit()

    assert (await async_session.execute(_HISTORY)).all() == [
        (original_id, _PUBLIC_ID, _START, _CLOSED_AT, "original")
    ]


def test_close_and_insert_sync_replaces_current_version(sync_session: Session) -> None:
    """Preserve the normal synchronous version replacement.

    Given: An open-ended setting version.
    When: The synchronous helper writes its replacement at the bus time.
    Then: The predecessor closes and the open successor carries its public ID.

    Args:
        sync_session: Sync session backed by a private SQLite database.
    """
    original = _original_setting()
    sync_session.add(original)
    sync_session.flush()
    original_id = original.id
    replacement: Setting = close_and_insert_sync(
        sync_session, Setting, [Setting.key == _KEY], _setting_values("replacement", 2), _BUS_TIME
    )
    sync_session.flush()
    replacement_id = replacement.id
    sync_session.commit()

    assert sync_session.execute(_HISTORY).all() == [
        (original_id, _PUBLIC_ID, _START, _BUS_TIME, "original"),
        (replacement_id, _PUBLIC_ID, _BUS_TIME, KNOWN_TO_MAX, "replacement"),
    ]


def test_close_and_insert_sync_inserts_first_version(sync_session: Session) -> None:
    """Preserve the synchronous insert for a missing logical key.

    Given: An empty settings table.
    When: The synchronous helper writes a setting for the first time.
    Then: The new version has a public ID and an open interval starting at the bus time.

    Args:
        sync_session: Sync session backed by a private SQLite database.
    """
    inserted: Setting = close_and_insert_sync(
        sync_session, Setting, [Setting.key == _KEY], _setting_values("first", 1), _BUS_TIME
    )
    sync_session.flush()
    assert inserted.public_id
    expected = (inserted.id, inserted.public_id, _BUS_TIME, KNOWN_TO_MAX, "first")
    sync_session.commit()

    assert sync_session.execute(_HISTORY).all() == [expected]


def test_close_and_insert_sync_refuses_closed_version(sync_session: Session) -> None:
    """Keep a committed history intact when a synchronous write targets its predecessor.

    Given: A finite predecessor and a newer open successor for the same key.
    When: The synchronous helper writes at a bus time inside the predecessor's interval.
    Then: The typed conflict reports the predecessor and both versions stay intact.

    Args:
        sync_session: Sync session backed by a private SQLite database.
    """
    original, successor = _original_setting(_CLOSED_AT), _current_successor()
    sync_session.add_all([original, successor])
    sync_session.flush()
    original_id, successor_id = original.id, successor.id
    sync_session.commit()

    with pytest.raises(
        TemporalVersionConflictError, match="settings version .* is no longer current"
    ) as conflict:
        close_and_insert_sync(
            sync_session, Setting, [Setting.key == _KEY], _setting_values("backdated", 2), _BUS_TIME
        )
    _assert_conflict(conflict.value, original_id)
    sync_session.commit()

    assert sync_session.execute(_HISTORY).all() == [
        (original_id, _PUBLIC_ID, _START, _CLOSED_AT, "original"),
        (successor_id, _PUBLIC_ID, _CLOSED_AT, KNOWN_TO_MAX, "successor"),
    ]


def test_close_and_insert_sync_refreshes_stale_identity(sync_session: Session) -> None:
    """Require the synchronous helper to see a close hidden by the identity map.

    Given: A cached open version closed by an unsynchronized update in the same session.
    When: The synchronous helper writes at a bus time inside the newly closed interval.
    Then: The typed conflict reports the stored close and no successor is inserted.

    Args:
        sync_session: Sync session backed by a private SQLite database.
    """
    sync_session.add(_original_setting())
    sync_session.commit()
    cached = sync_session.scalars(select(Setting)).one()
    original_id = cached.id
    sync_session.execute(_close_without_synchronizing(original_id))
    assert cached.known_to == KNOWN_TO_MAX

    with pytest.raises(
        TemporalVersionConflictError, match="settings version .* is no longer current"
    ) as conflict:
        close_and_insert_sync(
            sync_session, Setting, [Setting.key == _KEY], _setting_values("backdated", 2), _BUS_TIME
        )
    _assert_conflict(conflict.value, original_id)
    sync_session.commit()

    assert sync_session.execute(_HISTORY).all() == [
        (original_id, _PUBLIC_ID, _START, _CLOSED_AT, "original")
    ]
