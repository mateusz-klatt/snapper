"""Real SQLite witnesses for registration ownership and stale intent rejection."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.api.schemas.android_devices import RegisterAndroidDeviceBody
from snapper.data.android_devices import AndroidRegistrationError
from snapper.data.android_devices import RegistrationWrite
from snapper.data.android_devices import android_device_info
from snapper.data.android_devices import registration_revision
from snapper.data.models import NotificationDevice
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import AndroidDeviceStore
from snapper.data.repository import SQLAlchemyRepository

_CONFIG = str(uuid4())
_INSTALLATION = str(uuid4())
_OWNER_A = str(uuid4())
_OWNER_B = str(uuid4())
_TIME = datetime(2026, 10, 16, tzinfo=UTC)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Create only a disposable test database in the test-owned directory."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'android.db'}")
    await repository.create_all()
    await AndroidConfigurationStore(repository).activate(
        AndroidPushConfiguration(
            configuration_id=_CONFIG,
            package_name="ch.snapper.android",
            project_id="synthetic",
            application_id="synthetic-app",
            api_key="synthetic-public-key",
            messaging_sender_id="123",
        ),
        None,
        _write(),
    )
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def _body(owner: str = _OWNER_A, revision: str | None = None) -> RegisterAndroidDeviceBody:
    """Build one distinct local intent with an opaque SDK identifier."""
    return RegisterAndroidDeviceBody(
        protocol="fcm_fid",
        configuration_id=_CONFIG,
        package_name="ch.snapper.android",
        installation_id=_INSTALLATION,
        binding_id=str(uuid4()),
        expected_revision=revision or registration_revision(None),
        firebase_installation_id="synthetic-FID",
        expected_user_public_id=owner,
        app_version="0.2.0",
    )


def _write(owner: str = _OWNER_A, seconds: int = 0) -> RegistrationWrite:
    """Provide explicit deterministic server provenance."""
    return RegistrationWrite(owner, str(uuid4()), seconds + 1, _TIME + timedelta(seconds=seconds))


@pytest.mark.asyncio
async def test_delayed_first_submission_cannot_replace_newer_account(
    repo: SQLAlchemyRepository,
) -> None:
    """A prepared before B cannot overwrite B after its first network delay."""
    store = AndroidDeviceStore(repo)
    delayed_a = _body()
    accepted_b = await store.register(_body(_OWNER_B), _write(_OWNER_B))
    with pytest.raises(AndroidRegistrationError, match="revision_changed"):
        await store.register(delayed_a, _write(seconds=1))
    assert await store.list_owned(_OWNER_A) == []
    assert (await store.list_owned(_OWNER_B))[0].public_id == accepted_b.public_id


@pytest.mark.asyncio
async def test_active_retry_is_idempotent_and_retired_retry_is_refused(
    repo: SQLAlchemyRepository,
) -> None:
    """Receipt recovery retains identity but never reactivates a replaced intent."""
    store = AndroidDeviceStore(repo)
    first = _body()
    receipt = await store.register(first, _write())
    assert (await store.register(first, _write(seconds=1))).public_id == receipt.public_id
    revision = registration_revision(await store.state(_CONFIG, _INSTALLATION))
    replacement = await store.register(_body(_OWNER_B, revision), _write(_OWNER_B, 2))
    assert replacement.public_id != receipt.public_id
    with pytest.raises(AndroidRegistrationError, match="binding_retired"):
        await store.register(first, _write(seconds=3))
    assert await repo.list_active_notification_devices_for_user(_OWNER_B) == []
    assert await repo.list_active_notification_devices_for_users([_OWNER_B]) == {_OWNER_B: []}


@pytest.mark.asyncio
async def test_concurrent_owners_converge_to_one_binding(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The database fences simultaneous absent-head claims without latest-writer takeover."""
    store = AndroidDeviceStore(repo)
    original = store._current
    barrier = asyncio.Barrier(2)

    async def synchronized_read(
        session: AsyncSession, configuration: str, installation: str
    ) -> NotificationDevice | None:
        """Hold both absent-head readers before either may insert its binding."""
        row = await original(session, configuration, installation)
        await barrier.wait()
        return row

    monkeypatch.setattr(store, "_current", synchronized_read)
    results = await asyncio.gather(
        store.register(_body(), _write()),
        store.register(_body(_OWNER_B), _write(_OWNER_B)),
        return_exceptions=True,
    )
    assert sum(isinstance(result, AndroidRegistrationError) for result in results) == 1
    assert len(await store.list_owned(_OWNER_A)) + len(await store.list_owned(_OWNER_B)) == 1


@pytest.mark.asyncio
async def test_revoke_preserves_binding_identity_and_blocks_replay(
    repo: SQLAlchemyRepository,
) -> None:
    """Shared SCD2 revocation keeps a tombstone usable by optimistic preparation."""
    store = AndroidDeviceStore(repo)
    body = _body()
    receipt = await store.register(body, _write())
    before = registration_revision(await store.state(_CONFIG, _INSTALLATION))
    assert await repo.deactivate_notification_device_scd2(
        receipt.public_id,
        reason="user_unregistered",
        timestamp=_TIME + timedelta(seconds=1),
        session_id=str(uuid4()),
        sequence_id=2,
    )
    after = await store.state(_CONFIG, _INSTALLATION)
    assert after is not None
    assert after.binding_id == body.binding_id
    assert registration_revision(after) != before
    assert await store.list_owned(_OWNER_A) == []
    with pytest.raises(AndroidRegistrationError, match="binding_retired"):
        await store.register(body, _write(seconds=2))


@pytest.mark.asyncio
async def test_foreign_installation_cannot_claim_existing_fid(repo: SQLAlchemyRepository) -> None:
    """A unique FID collision rolls back without touching the original owner's binding."""
    store = AndroidDeviceStore(repo)
    receipt = await store.register(_body(), _write())
    other = _body(_OWNER_B).model_copy(update={"installation_id": str(uuid4())})
    with pytest.raises(AndroidRegistrationError, match="binding_conflict"):
        await store.register(other, _write(_OWNER_B, 1))
    assert (await store.list_owned(_OWNER_A))[0].public_id == receipt.public_id
    assert await store.list_owned(_OWNER_B) == []


@pytest.mark.asyncio
async def test_binding_cannot_be_reused_for_another_owner(repo: SQLAlchemyRepository) -> None:
    """Even a matching binding identifier does not authorize account reassignment."""
    store = AndroidDeviceStore(repo)
    body = _body()
    await store.register(body, _write())
    with pytest.raises(AndroidRegistrationError, match="binding_conflict"):
        await store.register(body, _write(_OWNER_B, 1))


@pytest.mark.asyncio
async def test_lost_close_compare_and_swap_refuses_insert(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row closed after preparation cannot be silently replaced by stale state."""
    store = AndroidDeviceStore(repo)
    await store.register(_body(), _write())
    previous = await store.state(_CONFIG, _INSTALLATION)
    assert previous is not None
    revision = registration_revision(previous)
    async with repo.session() as session:
        await session.execute(
            update(NotificationDevice)
            .where(NotificationDevice.id == previous.id)
            .values(known_to=_TIME + timedelta(seconds=1))
        )
        await session.commit()
    monkeypatch.setattr(store, "_current", AsyncMock(return_value=previous))
    with pytest.raises(AndroidRegistrationError, match="revision_changed"):
        await store.register(_body(revision=revision), _write(seconds=2))
    assert await store.list_owned(_OWNER_A) == []


def test_projection_refuses_incomplete_identity() -> None:
    """No fabricated identifier can turn malformed storage into a valid receipt."""
    row = NotificationDevice(installation_id=None)
    with pytest.raises(ValueError, match="incomplete"):
        android_device_info(row)
    with pytest.raises(ValueError, match="APNs projection"):
        SQLAlchemyRepository._notification_device_row_from(row)
