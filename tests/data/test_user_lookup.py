"""Tests for ``SQLAlchemyRepository.get_active_user_public_id_by_username``.

Backs the scoped-strategy ``label:<username>`` reference resolver: the
lookup must return the single active user's public id for an exact
username, None for an unknown username, and None for a closed (inactive)
SCD2 version so a stale username can never resolve a live scope.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.data.repository import SQLAlchemyRepository


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/user_lookup.db")
    await r.create_all()
    return r


async def _insert_user(
    repository: SQLAlchemyRepository,
    *,
    username: str,
    timestamp: datetime,
    known_to: datetime = KNOWN_TO_MAX,
    is_active: bool = True,
) -> str:
    """Insert one user version and return its public id."""
    async with repository.session() as session:
        user = User(
            username=username,
            email=None,
            password_hash="x",
            role="viewer",
            is_active=is_active,
            created_at=timestamp,
            timestamp=timestamp,
            known_to=known_to,
            session_id="test-session",
            sequence_id=1,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user.public_id


@pytest.mark.asyncio
async def test_returns_public_id_for_active_username(repo: SQLAlchemyRepository) -> None:
    """An exact active username resolves to its public id.

    Given: an active user 'alice',
    When: looked up by username at now,
    Then: the user's public id is returned.
    """
    now = datetime.now(UTC)
    public_id = await _insert_user(repo, username="alice", timestamp=now - timedelta(minutes=5))
    assert await repo.get_active_user_public_id_by_username("alice", now) == public_id


@pytest.mark.asyncio
async def test_returns_none_for_unknown_username(repo: SQLAlchemyRepository) -> None:
    """An unknown username resolves to None (fail-closed on unknown).

    Given: only user 'alice' exists,
    When: looking up 'ghost',
    Then: None is returned.
    """
    now = datetime.now(UTC)
    await _insert_user(repo, username="alice", timestamp=now - timedelta(minutes=5))
    assert await repo.get_active_user_public_id_by_username("ghost", now) is None


@pytest.mark.asyncio
async def test_returns_none_for_closed_user_version(repo: SQLAlchemyRepository) -> None:
    """A closed (inactive) SCD2 user version does not resolve.

    Given: user 'alice' whose only version was closed a minute ago,
    When: looking up 'alice' at now,
    Then: None is returned — a stale username never resolves a live scope.
    """
    now = datetime.now(UTC)
    await _insert_user(
        repo,
        username="alice",
        timestamp=now - timedelta(minutes=5),
        known_to=now - timedelta(minutes=1),
    )
    assert await repo.get_active_user_public_id_by_username("alice", now) is None


@pytest.mark.asyncio
async def test_returns_none_for_deactivated_user(repo: SQLAlchemyRepository) -> None:
    """A soft-deactivated user (is_active False) does not resolve.

    Given: an active SCD2 user row for 'alice' with is_active False,
    When: looking up 'alice' at now,
    Then: None is returned — a deactivated account never resolves a scope.
    """
    now = datetime.now(UTC)
    await _insert_user(
        repo,
        username="alice",
        timestamp=now - timedelta(minutes=5),
        is_active=False,
    )
    assert await repo.get_active_user_public_id_by_username("alice", now) is None
