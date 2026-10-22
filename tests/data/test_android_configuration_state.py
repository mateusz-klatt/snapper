"""Persisted configuration admission and irreversible rotation using isolated SQLite."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.api.schemas.android_devices import RegisterAndroidDeviceBody
from snapper.data.android_devices import AndroidConfigurationError
from snapper.data.android_devices import AndroidRegistrationError
from snapper.data.android_devices import RegistrationWrite
from snapper.data.android_devices import registration_revision
from snapper.data.models import AndroidPushConfigurationState
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import AndroidDeviceStore
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 10, 16, tzinfo=UTC)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Own every database connection and file used by these tests."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'configuration.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def _configuration() -> AndroidPushConfiguration:
    """Create a distinct immutable project configuration identity."""
    return AndroidPushConfiguration(
        configuration_id=str(uuid4()),
        package_name="ch.snapper.android",
        project_id="synthetic",
        application_id="app",
        api_key="public-key",
        messaging_sender_id="123",
    )


def _write(seconds: int = 0) -> RegistrationWrite:
    """Give temporal successors strictly increasing deterministic boundaries."""
    return RegistrationWrite(str(uuid4()), str(uuid4()), 1, _NOW + timedelta(seconds=seconds))


@pytest.mark.asyncio
async def test_rotation_retires_bindings_and_refuses_late_never_seen_configuration(
    repo: SQLAlchemyRepository,
) -> None:
    """A third process cannot treat its stale predecessor as current after B starts."""
    store = AndroidConfigurationStore(repo)
    a, b, delayed = _configuration(), _configuration(), _configuration()
    assert not await store.matches(a)
    await store.activate(a, None, _write())
    a_revision = (await store.revision()).revision
    write = _write(1)
    body = RegisterAndroidDeviceBody(
        protocol="fcm_fid",
        configuration_id=a.configuration_id,
        package_name=a.package_name,
        installation_id=str(uuid4()),
        binding_id=str(uuid4()),
        expected_revision=registration_revision(None),
        firebase_installation_id="synthetic-fid",
        expected_user_public_id=write.owner,
    )
    devices = AndroidDeviceStore(repo)
    receipt = await devices.register(body, write)
    await store.activate(b, a_revision, _write(2))
    assert not await store.matches(a)
    assert await store.matches(b)
    assert await devices.list_owned(write.owner) == []
    retired = await devices.state(a.configuration_id, body.installation_id)
    assert retired is not None and retired.public_id == receipt.public_id
    assert retired.token_status == "user_unregistered"
    with pytest.raises(AndroidConfigurationError, match="configuration_changed"):
        await store.activate(delayed, a_revision, _write(3))
    with pytest.raises(AndroidConfigurationError, match="configuration_retired"):
        await store.activate(a, b.configuration_id, _write(4))
    with pytest.raises(AndroidRegistrationError, match="configuration_changed"):
        await devices.register(body, _write(5))
    assert await store.matches(b)


@pytest.mark.asyncio
async def test_same_configuration_is_idempotent_but_public_changes_are_refused(
    repo: SQLAlchemyRepository,
) -> None:
    """Credential-only rotation does not alter public state or permit option mutation."""
    store = AndroidConfigurationStore(repo)
    configuration = _configuration()
    await store.activate(configuration, None, _write())
    await store.activate(configuration, None, _write(1))
    changed = configuration.model_copy(update={"api_key": "another-public-key"})
    assert not await store.matches(changed)
    with pytest.raises(AndroidConfigurationError, match="configuration_immutable"):
        await store.activate(changed, configuration.configuration_id, _write(2))
    async with repo.session() as session:
        versions = (await session.scalars(select(AndroidPushConfigurationState))).all()
    assert len(versions) == 1


@pytest.mark.asyncio
async def test_explicit_disable_is_predecessor_fenced_and_irreversible(
    repo: SQLAlchemyRepository,
) -> None:
    """Global disable leaves a tombstone while a stale disable cannot retire a replacement."""
    store = AndroidConfigurationStore(repo)
    configuration = _configuration()
    await store.disable(None, _write())
    initial_disable = (await store.revision()).revision
    assert initial_disable is not None
    with pytest.raises(AndroidConfigurationError, match="configuration_changed"):
        await store.activate(configuration, None, _write(1))
    await store.activate(configuration, initial_disable, _write(1))
    with pytest.raises(AndroidConfigurationError, match="configuration_changed"):
        await store.disable(None, _write(2))
    assert await store.matches(configuration)
    await store.disable((await store.revision()).revision, _write(3))
    assert not await store.matches(configuration)
    with pytest.raises(AndroidConfigurationError, match="configuration_changed"):
        await store.activate(_configuration(), None, _write(4))
    await store.disable((await store.revision()).revision, _write(4))
    with pytest.raises(AndroidConfigurationError, match="configuration_retired"):
        await store.activate(configuration, None, _write(5))


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["activate", "disable"])
async def test_concurrent_first_configuration_decisions_have_one_winner(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    """The singleton head also fences absent-head concurrency on actual SQLite."""
    store = AndroidConfigurationStore(repo)
    original = store._head
    barrier = asyncio.Barrier(2)
    reads = 0

    async def aligned_read(session: AsyncSession) -> AndroidPushConfigurationState | None:
        """Force both initial transactions to observe the same absent revision."""
        nonlocal reads
        current = await original(session)
        reads += 1
        if reads <= 2:
            await barrier.wait()
        return current

    monkeypatch.setattr(store, "_head", aligned_read)
    if operation == "activate":
        results = await asyncio.gather(
            store.activate(_configuration(), None, _write()),
            store.activate(_configuration(), None, _write()),
            return_exceptions=True,
        )
    else:
        results = await asyncio.gather(
            store.disable(None, _write()), store.disable(None, _write()), return_exceptions=True
        )
    assert sum(result is None for result in results) == 1
    errors = [result for result in results if result is not None]
    assert len(errors) == 1 and isinstance(errors[0], AndroidConfigurationError)
    assert (await store.revision()).revision is not None


@pytest.mark.asyncio
async def test_same_first_activation_race_is_idempotent(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two identical process startups converge on one public-options head and both succeed."""
    store = AndroidConfigurationStore(repo)
    configuration = _configuration()
    original = store._head
    barrier = asyncio.Barrier(2)
    reads = 0

    async def aligned(session: AsyncSession) -> AndroidPushConfigurationState | None:
        """Align only the initial reads, not the losing transaction's idempotency reread."""
        nonlocal reads
        current = await original(session)
        reads += 1
        if reads <= 2:
            await barrier.wait()
        return current

    monkeypatch.setattr(store, "_head", aligned)
    await asyncio.gather(
        store.activate(configuration, None, _write()), store.activate(configuration, None, _write())
    )
    assert await store.matches(configuration)
    async with repo.session() as session:
        assert len((await session.scalars(select(AndroidPushConfigurationState))).all()) == 1


@pytest.mark.asyncio
async def test_configuration_guarded_close_rejects_a_superseded_loaded_version(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A SELECT result retained across another rotation cannot close its historical version again."""
    store = AndroidConfigurationStore(repo)
    a, b, c = _configuration(), _configuration(), _configuration()
    await store.activate(a, None, _write())
    async with repo.session() as session:
        old = await store._head(session)
    assert old is not None
    await store.activate(b, old.public_id, _write(1))
    monkeypatch.setattr(store, "_head", AsyncMock(return_value=old))
    with pytest.raises(AndroidConfigurationError, match="configuration_changed"):
        await store.activate(c, old.public_id, _write(2))
    assert await AndroidConfigurationStore(repo).matches(b)


@pytest.mark.asyncio
async def test_retirement_refuses_an_already_closed_registration_version(
    repo: SQLAlchemyRepository,
) -> None:
    """A configuration rotation cannot resurrect or double-retire a stale registration snapshot."""
    configuration = _configuration()
    write = _write()
    await AndroidConfigurationStore(repo).activate(configuration, None, write)
    body = RegisterAndroidDeviceBody(
        protocol="fcm_fid",
        configuration_id=configuration.configuration_id,
        package_name=configuration.package_name,
        installation_id=str(uuid4()),
        binding_id=str(uuid4()),
        expected_revision=registration_revision(None),
        firebase_installation_id="synthetic",
        expected_user_public_id=write.owner,
    )
    devices = AndroidDeviceStore(repo)
    receipt = await devices.register(body, write)
    old = await devices.state(configuration.configuration_id, body.installation_id)
    assert old is not None
    await repo.deactivate_notification_device_scd2(
        receipt.public_id,
        reason="user_unregistered",
        timestamp=_NOW + timedelta(seconds=1),
        session_id=str(uuid4()),
        sequence_id=1,
    )
    async with repo.session() as session:
        with pytest.raises(AndroidConfigurationError, match="configuration_changed"):
            await devices.retire(session, old, _write(2))
    current = await devices.state(configuration.configuration_id, body.installation_id)
    assert current is not None and current.token_status == "user_unregistered"
