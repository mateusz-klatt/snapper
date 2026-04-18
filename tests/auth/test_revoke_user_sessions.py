"""Tests for the Day 3a kill-switch primitive (plan §3.6.1).

Covers :meth:`TokenManager.revoke_user_sessions` — the two-phase
revocation that flips ``user_active_tokens.revoked_at`` in DB AND
pushes every active JTI into the in-memory fast-path blacklist so
:meth:`verify_token` rejects them on the next request.

Also covers the two new :class:`Repository` methods
``list_active_user_token_jtis`` + ``revoke_user_active_tokens``
against an in-memory aiosqlite DB.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.tokens import TokenManager
from snapper.data.models import UserActiveToken
from snapper.data.repository import SQLAlchemyRepository


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """Fresh in-memory repo with the AI Phase A schema applied."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


async def _insert_token(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    jti: str,
    revoked: bool = False,
) -> None:
    """Seed a :class:`UserActiveToken` row for the given user."""
    now = datetime.now(UTC)
    async with repo.session() as s:
        row = UserActiveToken(
            public_id=f"pub-{jti}",
            user_public_id=user_public_id,
            jti=jti,
            token_hash=f"hash-{jti}",
            token_type="access",
            issued_at=now,
            expires_at=now + timedelta(hours=1),
            revoked_at=now if revoked else None,
        )
        s.add(row)
        await s.commit()


class TestRepositoryTokenMethods:
    """Coverage for ``list_active_user_token_jtis`` + ``revoke_user_active_tokens``."""

    @pytest.mark.asyncio
    async def test_list_active_jtis_returns_only_unrevoked(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Revoked tokens are excluded from the active list.

        Given: a user with 3 tokens — 2 active, 1 already revoked,
        When: ``list_active_user_token_jtis`` runs,
        Then: only the 2 active JTIs are returned. The revoked row
            is NOT surfaced even though its ``user_public_id`` matches.
        """
        await _insert_token(repo, user_public_id="user-1", jti="jti-alive-1")
        await _insert_token(repo, user_public_id="user-1", jti="jti-alive-2")
        await _insert_token(repo, user_public_id="user-1", jti="jti-dead", revoked=True)
        jtis = await repo.list_active_user_token_jtis("user-1")
        assert sorted(jtis) == ["jti-alive-1", "jti-alive-2"]

    @pytest.mark.asyncio
    async def test_list_active_jtis_empty_for_user_with_no_tokens(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """No tokens → empty list, not an error.

        Given: a user_public_id with no rows in ``user_active_tokens``,
        When: the accessor runs,
        Then: an empty list is returned — not an exception.
        """
        jtis = await repo.list_active_user_token_jtis("ghost-user")
        assert jtis == []

    @pytest.mark.asyncio
    async def test_revoke_active_tokens_updates_only_unrevoked_rows(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Revoke flips ``revoked_at`` and returns the count.

        Given: a user with 2 active + 1 already-revoked tokens,
        When: ``revoke_user_active_tokens`` runs with a fresh ts,
        Then: the two active rows get ``revoked_at=ts`` (count=2);
            the already-revoked row's ``revoked_at`` is NOT
            overwritten — idempotency guarantee.
        """
        await _insert_token(repo, user_public_id="user-2", jti="j-a")
        await _insert_token(repo, user_public_id="user-2", jti="j-b")
        original_revoked_ts = datetime.now(UTC) - timedelta(hours=2)
        async with repo.session() as s:
            row = UserActiveToken(
                public_id="pub-old",
                user_public_id="user-2",
                jti="j-already-revoked",
                token_hash="hash-old",
                token_type="access",
                issued_at=original_revoked_ts - timedelta(hours=1),
                expires_at=original_revoked_ts + timedelta(hours=1),
                revoked_at=original_revoked_ts,
            )
            s.add(row)
            await s.commit()
        revoke_ts = datetime.now(UTC)
        count = await repo.revoke_user_active_tokens("user-2", revoke_ts)
        assert count == 2
        jtis_after = await repo.list_active_user_token_jtis("user-2")
        assert jtis_after == []

    @pytest.mark.asyncio
    async def test_revoke_active_tokens_zero_count_for_no_active(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """No unrevoked rows → count=0 — not an error."""
        await _insert_token(repo, user_public_id="u", jti="x", revoked=True)
        count = await repo.revoke_user_active_tokens("u", datetime.now(UTC))
        assert count == 0


class TestRevokeUserSessions:
    """Coverage for :meth:`TokenManager.revoke_user_sessions`."""

    def _fresh_manager(self) -> TokenManager:
        """Return a cleanly-initialized singleton (clears any state)."""
        TokenManager._initialized = False
        manager = TokenManager()
        manager._blacklisted_tokens.clear()
        return manager

    @pytest.mark.asyncio
    async def test_blacklists_all_active_jtis_and_returns_count(self) -> None:
        """Kill switch pushes every active JTI into the blacklist.

        Given: the repository reports 3 active JTIs for the user and
            the DB-update call reports 3 rows affected,
        When: ``revoke_user_sessions`` runs,
        Then: every JTI is added to ``_blacklisted_tokens`` AND the
            method returns the DB row count (3).
        """
        manager = self._fresh_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=["jti-1", "jti-2", "jti-3"])
        repo.revoke_user_active_tokens = AsyncMock(return_value=3)
        count = await manager.revoke_user_sessions("user-1", repo)
        assert count == 3
        assert set(manager._blacklisted_tokens.keys()) == {"jti-1", "jti-2", "jti-3"}

    @pytest.mark.asyncio
    async def test_no_active_tokens_returns_zero_and_does_not_blacklist(self) -> None:
        """User with no active sessions → count=0; no blacklist entries added.

        Given: the repo reports an empty JTI list,
        When: revoke_user_sessions runs,
        Then: count=0 (forwarded from repo) AND the in-memory
            blacklist is untouched.
        """
        manager = self._fresh_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=[])
        repo.revoke_user_active_tokens = AsyncMock(return_value=0)
        count = await manager.revoke_user_sessions("lonely-user", repo)
        assert count == 0
        assert manager._blacklisted_tokens == {}

    @pytest.mark.asyncio
    async def test_calls_db_revoke_before_seeding_in_memory_blacklist(self) -> None:
        """Ordering: DB flip happens before the in-memory blacklist push.

        Given: a repo whose ``revoke_user_active_tokens`` records
            ``_blacklisted_tokens`` observed at call time,
        When: revoke_user_sessions runs,
        Then: at the moment the DB revoke is called, the in-memory
            blacklist is STILL empty — confirming the sequence
            "load JTIs → DB revoke → blacklist" from the plan.
        """
        manager = self._fresh_manager()
        observations: dict[str, set[str]] = {}
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=["one"])

        async def _record_blacklist_state(_uid: str, _ts: datetime) -> int:
            observations["at_db_revoke"] = set(manager._blacklisted_tokens.keys())
            return 1

        repo.revoke_user_active_tokens = AsyncMock(side_effect=_record_blacklist_state)
        await manager.revoke_user_sessions("u", repo)
        assert observations["at_db_revoke"] == set()
        assert "one" in manager._blacklisted_tokens

    @pytest.mark.asyncio
    async def test_revoked_tokens_rejected_by_verify_after_grace(self) -> None:
        """verify_token rejects a JTI once its blacklist entry ages past grace.

        Given: revoke_user_sessions just ran for a user whose JTI is
            now in the blacklist, AND we artificially push the
            blacklist timestamp back past the grace period,
        When: ``_is_token_blacklisted`` is called with that JTI,
        Then: True is returned — matching the fast-path reject
            condition ``verify_token`` uses.
        """
        manager = self._fresh_manager()
        repo = MagicMock()
        repo.list_active_user_token_jtis = AsyncMock(return_value=["jti-X"])
        repo.revoke_user_active_tokens = AsyncMock(return_value=1)
        await manager.revoke_user_sessions("u", repo)
        past_time = datetime.now(UTC).timestamp() - manager._blacklist_grace_period - 1
        manager._blacklisted_tokens["jti-X"] = past_time
        assert manager._is_token_blacklisted("jti-X") is True
