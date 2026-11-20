"""Exercise SQLite user version replacements across independent engines."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path
from typing import Literal

import pytest
from aiosqlite import Connection
from sqlalchemy import ColumnElement
from sqlalchemy import SQLColumnExpression
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.auth.schemas.user import UserProfile
from snapper.auth.user_service import UserAuthorityConflictError
from snapper.auth.user_service import UserService
from snapper.data.models import User
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import where_active_now

_WAIT = 5
_USERNAME = "sqlite-race-user"
_OLD_PASSWORD = "initial-test-password"
_NEW_PASSWORD = "replacement-test-password"
_Writer = Literal[
    "update_user",
    "update_self_preferences",
    "change_password",
    "admin_reset_password",
    "reset_password_by_username",
]


@pytest.fixture
async def repositories(
    tmp_path: Path,
) -> AsyncIterator[tuple[SQLAlchemyRepository, SQLAlchemyRepository]]:
    """Open two independent engines on a single disposable SQLite file.

    Args:
        tmp_path: Directory owned by this test case.

    Yields:
        The writer and deactivator repositories.
    """
    url = f"sqlite+aiosqlite:///{tmp_path / 'user-scd2.db'}"
    first = SQLAlchemyRepository(url)
    second = SQLAlchemyRepository(url)
    try:
        await asyncio.wait_for(first.create_all(), _WAIT)
        yield first, second
    finally:
        await asyncio.wait_for(
            asyncio.gather(first.engine.dispose(), second.engine.dispose()), _WAIT
        )


def _user_service(repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch) -> UserService:
    """Construct a service bound to one engine.

    Args:
        repository: Repository used by the service.
        monkeypatch: Restores the shared service factory after construction.

    Returns:
        A real user service bound to the requested repository.
    """
    with monkeypatch.context() as setup:
        setup.setattr(UserService, "_instance", None)
        setup.setattr(UserService, "_initialized", False)
        setup.setattr("snapper.auth.user_service.get_repository", lambda _: repository)
        return UserService()


async def _current_user(repository: SQLAlchemyRepository) -> User:
    """Read exactly one current version of the target username in a fresh session.

    Args:
        repository: Engine used to observe committed state.

    Returns:
        The only current version.
    """
    async with repository.session() as session:
        result = await session.execute(
            select(User).where(User.username == _USERNAME, *where_active_now(User))
        )
        return result.scalar_one()


async def _write_user(service: UserService, writer: _Writer) -> UserProfile | bool | None:
    """Invoke the real writer selected for this case.

    Args:
        service: Writer service on the first engine.
        writer: Public method under test.

    Returns:
        The writer's normal return value.
    """
    if writer == "update_user":
        return await service.update_user(_USERNAME, email="changed@example.com")
    if writer == "update_self_preferences":
        return await service.update_self_preferences(_USERNAME, "pl")
    if writer == "change_password":
        return await service.change_password(_USERNAME, _OLD_PASSWORD, _NEW_PASSWORD)
    if writer == "admin_reset_password":
        await service.admin_reset_password(_USERNAME, _NEW_PASSWORD)
        return None
    await service.reset_password_by_username(_USERNAME, _NEW_PASSWORD)
    return None


def _describe_outcome(outcome: UserProfile | bool | None | BaseException) -> str:
    """Describe the writer result without including user profile details.

    Args:
        outcome: Return value or exception from the writer.

    Returns:
        Return type and active state, or exception class and message.
    """
    if isinstance(outcome, BaseException):
        return f"raised {type(outcome).__name__}: {outcome}"
    if isinstance(outcome, UserProfile):
        return f"returned UserProfile(is_active={outcome.is_active})"
    return f"returned {outcome!r}"


@dataclass
class _RaceObservation:
    """Captured SQLite lock state and synchronization events for one race."""

    read: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    revoking: asyncio.Event = field(default_factory=asyncio.Event)
    active_reads: list[bool | None] = field(default_factory=list)
    native_transaction: bool = False
    busy_timeout: int | None = None


def _install_race_gates(
    first: UserService,
    second_repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> _RaceObservation:
    """Pause writer A after its first read and signal when B enters SQLite write.

    Args:
        first: Writer service on engine A.
        second_repo: Repository on engine B.
        monkeypatch: Installs temporary observation hooks around real methods.

    Returns:
        Shared events and captured transaction state for the race.
    """
    observation = _RaceObservation()
    read_user = first._lock_current_user
    revoke = second_repo.revoke_user_active_tokens

    async def hold_read(
        session: AsyncSession,
        target_predicate: ColumnElement[bool],
        extra_predicates: tuple[SQLColumnExpression[bool], ...] = (),
    ) -> User | None:
        """Pause after the first real read, before the caller samples its write time.

        Args:
            session: Writer's live SQLAlchemy session.
            target_predicate: User lookup predicate.
            extra_predicates: Additional conditions from the public writer.

        Returns:
            The row read by the real service method.
        """
        row = await read_user(session, target_predicate, extra_predicates)
        observation.active_reads.append(row.is_active if row is not None else None)
        if not observation.read.is_set():
            connection = await session.connection()
            driver = (await connection.get_raw_connection()).driver_connection
            assert isinstance(driver, Connection)
            observation.native_transaction = driver.in_transaction
            observation.busy_timeout = await session.scalar(text("PRAGMA busy_timeout"))
            observation.read.set()
            await asyncio.wait_for(observation.release.wait(), _WAIT)
        return row

    async def observe_deactivation_write(user_public_id: str, revoked_at: datetime) -> int:
        """Observe when deactivation reaches its first real SQLite write.

        Args:
            user_public_id: User whose legacy sessions are being revoked.
            revoked_at: Timestamp of the revocation.

        Returns:
            The count returned by the real revocation method.
        """
        observation.revoking.set()
        return await revoke(user_public_id, revoked_at)

    monkeypatch.setattr(first, "_lock_current_user", hold_read)
    monkeypatch.setattr(second_repo, "revoke_user_active_tokens", observe_deactivation_write)
    return observation


async def _cancel_and_drain(
    writing: asyncio.Task[UserProfile | bool | None],
    deactivating: asyncio.Task[bool] | None,
) -> None:
    """Cancel and await unfinished race tasks so sessions never leak on failure.

    Args:
        writing: Writer task on engine A.
        deactivating: Optional deactivation task on engine B.
    """
    tasks: list[asyncio.Task[UserProfile | bool | None] | asyncio.Task[bool]] = [writing]
    if deactivating is not None:
        tasks.append(deactivating)
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), _WAIT)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "writer",
    [
        "update_user",
        "update_self_preferences",
        "change_password",
        "admin_reset_password",
        "reset_password_by_username",
    ],
)
async def test_sqlite_user_writer_preserves_deactivation(
    repositories: tuple[SQLAlchemyRepository, SQLAlchemyRepository],
    monkeypatch: pytest.MonkeyPatch,
    writer: _Writer,
) -> None:
    """A SQLite writer must serialize with and preserve concurrent deactivation.

    Ordering: writer A reads the active user row and pauses before sampling its
    write timestamp. When A opened a native write transaction via
    ``BEGIN IMMEDIATE``, engine B starts ``deactivate_user``, reaches its first
    real SQLite write (``revoke_user_active_tokens``), and blocks on SQLite's
    ``busy_timeout`` (asserted unfinished via a bounded event wait) until A is
    released and commits, after which B deactivates A's committed successor.
    Without ``BEGIN IMMEDIATE``, B completes deactivation while A is still gated
    and A resumes to copy its stale active flag. Both branches assert that one
    current version remains and it is inactive.

    Given two SQLite engines and a writer paused immediately after its locked read.
    When the second engine deactivates the same user and both service calls finish.
    Then the single current version exists and is inactive.

    Args:
        repositories: Independent engines for the writer and deactivation.
        monkeypatch: Installs the observation gates around the real operations.
        writer: User mutation racing against deactivation.
    """
    first_repo, second_repo = repositories
    first = _user_service(first_repo, monkeypatch)
    second = _user_service(second_repo, monkeypatch)
    user = await asyncio.wait_for(first.create_user(_USERNAME, _OLD_PASSWORD), _WAIT)
    observation = _install_race_gates(first, second_repo, monkeypatch)
    writing = asyncio.create_task(_write_user(first, writer))
    deactivating = None
    try:
        await asyncio.wait_for(observation.read.wait(), _WAIT)
        assert observation.active_reads == [True]
        deactivating = asyncio.create_task(second.deactivate_user(user.public_id, reason=None))
        if observation.native_transaction:
            await asyncio.wait_for(observation.revoking.wait(), _WAIT)
            assert not deactivating.done()
            observation.release.set()
            deactivated_result, outcome = await asyncio.wait_for(
                asyncio.gather(deactivating, writing, return_exceptions=True), _WAIT
            )
        else:
            deactivated_result = await asyncio.wait_for(deactivating, _WAIT)
            deactivated = await asyncio.wait_for(_current_user(second_repo), _WAIT)
            assert deactivated.public_id == user.public_id and not deactivated.is_active
            observation.release.set()
            (outcome,) = await asyncio.wait_for(
                asyncio.gather(writing, return_exceptions=True), _WAIT
            )
        assert deactivated_result is True
        current = await asyncio.wait_for(_current_user(second_repo), _WAIT)
    finally:
        observation.release.set()
        await _cancel_and_drain(writing, deactivating)

    detail = (
        f"{writer}: A {_describe_outcome(outcome)}; A read states={observation.active_reads}; "
        f"A native transaction after SELECT={observation.native_transaction}; "
        f"busy_timeout={observation.busy_timeout} ms; "
        f"deactivated_result={deactivated_result!r}; "
        f"current is_active={current.is_active}"
    )
    assert current.public_id == user.public_id and not current.is_active, detail
    if isinstance(outcome, BaseException):
        assert isinstance(outcome, UserAuthorityConflictError), detail
