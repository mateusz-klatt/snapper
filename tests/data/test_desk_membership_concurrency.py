"""Transactional desk-membership mutation tests."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import Operator
from snapper.data.models import User
from snapper.data.repository import DeskMembershipNotFoundError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import DeskMembershipAttach
from snapper.data.repository_types import DeskMembershipDetach

_DESK_A = "00000000-0000-7000-8000-000000000101"
_DESK_B = "00000000-0000-7000-8000-000000000102"
_VIEWER = "00000000-0000-7000-8000-000000000103"


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Build one VIEWER and two desks in a dedicated SQLite database."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/desk-concurrency.db")
    await repo.create_all()
    fixture_at = datetime.now(UTC) - timedelta(minutes=1)
    async with repo.session() as session:
        session.add_all(
            [
                Operator(
                    public_id=_DESK_A,
                    label="desk-a",
                    description=None,
                    timestamp=fixture_at,
                    session_id="fixture",
                    sequence_id=1,
                ),
                Operator(
                    public_id=_DESK_B,
                    label="desk-b",
                    description=None,
                    timestamp=fixture_at,
                    session_id="fixture",
                    sequence_id=2,
                ),
                User(
                    public_id=_VIEWER,
                    username="viewer",
                    email=None,
                    password_hash="unused",
                    role="viewer",
                    is_active=True,
                    created_at=fixture_at,
                    timestamp=fixture_at,
                    session_id="fixture",
                    sequence_id=3,
                ),
            ]
        )
        await session.commit()
    yield repo
    await repo.engine.dispose()


def _attach(desk: str, sequence_id: int) -> DeskMembershipAttach:
    """Build one live attachment request."""
    return DeskMembershipAttach(
        username="viewer",
        operator_public_id=desk,
        timestamp=datetime.now(UTC),
        session_id="attach",
        sequence_id=sequence_id,
    )


def _detach(desk: str, sequence_id: int) -> DeskMembershipDetach:
    """Build one live detachment request."""
    return DeskMembershipDetach(
        username="viewer",
        operator_public_id=desk,
        timestamp=datetime.now(UTC),
        session_id="detach",
        sequence_id=sequence_id,
    )


async def _active_memberships(
    repository: SQLAlchemyRepository,
) -> list[tuple[str, bool]]:
    """Return current desk identities and primary flags."""
    rows = await repository.get_user_operator_memberships(_VIEWER, datetime.now(UTC))
    return [(row["operator_public_id"], row["is_primary"]) for row in rows]


@pytest.mark.asyncio
async def test_sqlite_detach_allows_revocation_writer_on_separate_session(
    repository: SQLAlchemyRepository,
) -> None:
    """A real callback write completes before the serialized membership close."""
    await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))
    issued_at = datetime.now(UTC)
    await repository.insert_user_active_tokens(
        [
            {
                "public_id": "00000000-0000-7000-8000-000000000104",
                "user_public_id": _VIEWER,
                "jti": "desk-detach-jti",
                "token_hash": "a" * 64,
                "token_type": "access",
                "issued_at": issued_at,
                "expires_at": issued_at + timedelta(hours=1),
            }
        ]
    )
    revoked_counts: list[int] = []

    async def revoke_authority(user_public_id: str) -> None:
        revoked_counts.append(
            await repository.revoke_user_active_tokens(user_public_id, datetime.now(UTC))
        )

    result = await asyncio.wait_for(
        repository.detach_viewer_from_desk(_detach(_DESK_A, 2), revoke_authority),
        timeout=2,
    )
    assert result is not None
    assert await _active_memberships(repository) == []
    assert revoked_counts == [1]
    assert await repository.list_active_user_token_jtis(_VIEWER) == []


@pytest.mark.asyncio
async def test_concurrent_first_attachments_preserve_exactly_one_primary(
    repository: SQLAlchemyRepository,
) -> None:
    """SQLite serializes first-membership decisions across connections."""
    release = asyncio.Event()

    async def attach_after_release(
        desk: str,
        sequence_id: int,
    ) -> None:
        await release.wait()
        await repository.attach_viewer_to_desk(_attach(desk, sequence_id))

    first = asyncio.create_task(attach_after_release(_DESK_A, 1))
    second = asyncio.create_task(attach_after_release(_DESK_B, 2))
    release.set()
    await asyncio.gather(first, second)
    memberships = await _active_memberships(repository)
    assert {desk for desk, _is_primary in memberships} == {_DESK_A, _DESK_B}
    assert sum(is_primary for _desk, is_primary in memberships) == 1


@pytest.mark.asyncio
async def test_postgresql_path_requests_user_and_membership_row_locks(
    repository: SQLAlchemyRepository,
) -> None:
    """Production mutations select both serialization planes with locking enabled."""
    revoked: list[str] = []

    async def revoke_authority(user_public_id: str) -> None:
        revoked.append(user_public_id)

    with (
        patch.object(
            SQLAlchemyRepository,
            "dialect_name",
            new_callable=PropertyMock,
            return_value="postgresql",
        ),
        patch.object(
            repository,
            "_load_desk_target_user",
            wraps=repository._load_desk_target_user,
        ) as load_user,
        patch.object(
            repository,
            "_load_desk_memberships",
            wraps=repository._load_desk_memberships,
        ) as load_memberships,
    ):
        await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))
        result = await repository.detach_viewer_from_desk(
            _detach(_DESK_A, 2),
            revoke_authority,
        )
        repeated = await repository.detach_viewer_from_desk(
            _detach(_DESK_A, 3),
            revoke_authority,
        )
    assert result is not None
    assert repeated is None
    assert revoked == [_VIEWER]
    assert [awaited.kwargs["lock"] for awaited in load_user.await_args_list] == [
        True,
        True,
        True,
    ]
    assert [awaited.kwargs["lock"] for awaited in load_memberships.await_args_list] == [
        True,
        True,
        True,
    ]


@pytest.mark.asyncio
async def test_attach_during_sqlite_revocation_becomes_promoted_survivor(
    repository: SQLAlchemyRepository,
) -> None:
    """A later attach committed during revocation is visible to promotion."""
    await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))
    revocation_started = asyncio.Event()
    finish_revocation = asyncio.Event()

    async def revoke_authority(_user_public_id: str) -> None:
        revocation_started.set()
        await finish_revocation.wait()

    detach_task = asyncio.create_task(
        repository.detach_viewer_from_desk(_detach(_DESK_A, 2), revoke_authority)
    )
    await revocation_started.wait()
    attached = await repository.attach_viewer_to_desk(_attach(_DESK_B, 3))
    assert attached["is_primary"] is False
    finish_revocation.set()
    result = await detach_task
    assert result is not None
    assert result.promoted_operator_public_id == _DESK_B
    assert await _active_memberships(repository) == [(_DESK_B, True)]


@pytest.mark.asyncio
async def test_concurrent_sqlite_detaches_keep_revocation_idempotent(
    repository: SQLAlchemyRepository,
) -> None:
    """Concurrent preflight winners converge through an idempotent token update."""
    await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))
    issued_at = datetime.now(UTC)
    await repository.insert_user_active_tokens(
        [
            {
                "public_id": "00000000-0000-7000-8000-000000000105",
                "user_public_id": _VIEWER,
                "jti": "concurrent-desk-detach-jti",
                "token_hash": "b" * 64,
                "token_type": "access",
                "issued_at": issued_at,
                "expires_at": issued_at + timedelta(hours=1),
            }
        ]
    )
    callbacks_ready = asyncio.Event()
    callback_count = 0
    revoked_counts: list[int] = []

    async def revoke_authority(user_public_id: str) -> None:
        nonlocal callback_count
        callback_count += 1
        if callback_count == 2:
            callbacks_ready.set()
        await callbacks_ready.wait()
        revoked_counts.append(
            await repository.revoke_user_active_tokens(user_public_id, datetime.now(UTC))
        )

    first = asyncio.create_task(
        repository.detach_viewer_from_desk(_detach(_DESK_A, 2), revoke_authority)
    )
    second = asyncio.create_task(
        repository.detach_viewer_from_desk(_detach(_DESK_A, 3), revoke_authority)
    )
    results = await asyncio.wait_for(asyncio.gather(first, second), timeout=2)
    assert sum(result is not None for result in results) == 1
    assert sorted(revoked_counts) == [0, 1]
    assert await repository.list_active_user_token_jtis(_VIEWER) == []
    assert await _active_memberships(repository) == []


@pytest.mark.asyncio
async def test_detaching_non_primary_keeps_existing_primary(
    repository: SQLAlchemyRepository,
) -> None:
    """Removing a secondary desk does not rewrite the primary version."""
    await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))
    await repository.attach_viewer_to_desk(_attach(_DESK_B, 2))
    revoked: list[str] = []

    async def revoke_authority(user_public_id: str) -> None:
        revoked.append(user_public_id)

    result = await repository.detach_viewer_from_desk(
        _detach(_DESK_B, 3),
        revoke_authority,
    )
    assert result is not None
    assert result.promoted_operator_public_id is None
    assert revoked == [_VIEWER]
    assert await _active_memberships(repository) == [(_DESK_A, True)]


@pytest.mark.asyncio
async def test_detaching_last_membership_leaves_no_primary(
    repository: SQLAlchemyRepository,
) -> None:
    """Removing the sole membership produces an empty valid membership set."""
    await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))

    async def revoke_authority(_user_public_id: str) -> None:
        return None

    result = await repository.detach_viewer_from_desk(
        _detach(_DESK_A, 2),
        revoke_authority,
    )
    assert result is not None
    assert result.promoted_operator_public_id is None
    assert await _active_memberships(repository) == []


@pytest.mark.asyncio
async def test_desk_directory_rejects_inactive_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Directory reads reject an unknown or inactive desk identity."""
    s5778_value_1 = datetime.now(UTC)
    with pytest.raises(DeskMembershipNotFoundError, match="desk"):
        await repository.list_human_desk_members(
            "00000000-0000-7000-8000-000000000199",
            s5778_value_1,
        )


@pytest.mark.asyncio
async def test_membership_commit_failure_keeps_revocation_and_rolls_back_state(
    repository: SQLAlchemyRepository,
) -> None:
    """A failed membership commit cannot resurrect already-revoked credentials."""
    await repository.attach_viewer_to_desk(_attach(_DESK_A, 1))
    revoked: list[str] = []

    async def revoke_authority(user_public_id: str) -> None:
        revoked.append(user_public_id)

    async def fail_commit(_session: AsyncSession) -> None:
        raise RuntimeError("forced membership commit failure")

    with (
        patch.object(AsyncSession, "commit", new=fail_commit),
        pytest.raises(RuntimeError, match="forced membership commit failure"),
    ):
        await repository.detach_viewer_from_desk(
            _detach(_DESK_A, 2),
            revoke_authority,
        )
    assert revoked == [_VIEWER]
    assert await _active_memberships(repository) == [(_DESK_A, True)]
