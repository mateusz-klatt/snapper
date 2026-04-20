"""Tests for Day 3d-B DB-backed async verify_token + 30s LRU cache (plan §3.6.3).

Covers :meth:`TokenManager.verify_token_with_db`:

- DB lookup rejects tokens missing from ``user_active_tokens``.
- DB lookup rejects revoked rows.
- DB lookup rejects deactivated users via the SCD2-active join.
- 30s LRU cache memoises positive AND negative verdicts.
- :meth:`TokenManager.invalidate_user_cache` evicts matching entries.
- Stale entries (past TTL / JWT ``exp``) are pruned under bounded growth.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import VERIFY_CACHE_MAX_ENTRIES
from snapper.auth.tokens import VERIFY_CACHE_TTL_SECONDS
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import _VerifyCacheEntry
from snapper.auth.tokens import hash_token
from snapper.data.repository_types import UserActiveTokenVerificationRow


def _fresh_manager() -> TokenManager:
    """Clean-state TokenManager singleton."""
    TokenManager._initialized = False
    manager = TokenManager()
    manager._blacklisted_tokens.clear()
    manager._verify_cache.clear()
    return manager


def _make_verification_row(
    *,
    revoked: bool = False,
    user_is_active: bool = True,
    user_public_id: str = "user-verify",
) -> UserActiveTokenVerificationRow:
    now = datetime.now(UTC)
    return UserActiveTokenVerificationRow(
        user_public_id=user_public_id,
        revoked_at=now if revoked else None,
        expires_at=now + timedelta(minutes=15),
        user_is_active=user_is_active,
    )


def _mint_access_token(manager: TokenManager, *, user_public_id: str = "user-verify") -> str:
    """Produce a fresh access JWT suitable for verify_token_with_db."""
    principal = AuthPrincipal(
        username="verify-user",
        role=UserRole.VIEWER,
        user_public_id=user_public_id,
    )
    return manager.create_tokens(principal).access_token


class TestVerifyTokenWithDB:
    """DB + cache gates in sequence."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_claims_and_caches(self) -> None:
        """Valid JWT + inventory row + active user → claims + positive cache entry."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=_make_verification_row())
        claims = await manager.verify_token_with_db(token, repo)
        assert claims is not None
        assert claims.username == "verify-user"
        assert hash_token(token) in manager._verify_cache
        entry = manager._verify_cache[hash_token(token)]
        assert entry.is_valid is True
        assert entry.user_is_active is True
        assert entry.user_public_id == "user-verify"

    @pytest.mark.asyncio
    async def test_cache_hit_avoids_second_db_call(self) -> None:
        """Second call within 30s TTL short-circuits; DB is consulted ONCE."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=_make_verification_row())
        assert await manager.verify_token_with_db(token, repo) is not None
        assert await manager.verify_token_with_db(token, repo) is not None
        assert repo.get_active_token_by_hash.await_count == 1

    @pytest.mark.asyncio
    async def test_missing_row_returns_none_caches_negative(self) -> None:
        """Token absent from inventory → None + negative verdict cached."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        assert await manager.verify_token_with_db(token, repo) is None
        assert await manager.verify_token_with_db(token, repo) is None
        assert repo.get_active_token_by_hash.await_count == 1
        entry = manager._verify_cache[hash_token(token)]
        assert entry.is_valid is False
        assert entry.user_is_active is False

    @pytest.mark.asyncio
    async def test_revoked_row_returns_none(self) -> None:
        """Row present but ``revoked_at`` set → None."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=_make_verification_row(revoked=True))
        assert await manager.verify_token_with_db(token, repo) is None
        entry = manager._verify_cache[hash_token(token)]
        assert entry.is_valid is False

    @pytest.mark.asyncio
    async def test_deactivated_user_returns_none(self) -> None:
        """Row present + active but ``users.is_active=False`` → None."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(user_is_active=False)
        )
        assert await manager.verify_token_with_db(token, repo) is None

    @pytest.mark.asyncio
    async def test_invalid_jwt_never_touches_db(self) -> None:
        """Failed JWT signature/expiry short-circuits BEFORE DB lookup."""
        manager = _fresh_manager()
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        assert await manager.verify_token_with_db("not.a.jwt", repo) is None
        repo.get_active_token_by_hash.assert_not_awaited()


class TestInvalidateUserCache:
    """Cross-instance LRU eviction for the admin-bus subscriber (3d-C)."""

    def test_evicts_matching_user_entries_only(self) -> None:
        """Entries whose user_public_id matches are dropped; others stay.

        Simulates the Day 3c ``admin.user_deactivated`` dispatch: the
        TokenManager subscribes to the bus (wired in 3d-C) and calls
        :meth:`invalidate_user_cache` on receipt. Unaffected users
        keep their cached verdicts.
        """
        manager = _fresh_manager()
        now_ts = datetime.now(UTC).timestamp()
        for th, uid in [
            ("hash-a1", "target"),
            ("hash-a2", "target"),
            ("hash-b1", "bystander"),
        ]:
            manager._verify_cache[th] = _VerifyCacheEntry(
                is_valid=True,
                user_is_active=True,
                user_public_id=uid,
                expires_at_ts=now_ts + 900,
                cached_at_ts=now_ts,
            )
        evicted = manager.invalidate_user_cache("target")
        assert evicted == 2
        assert "hash-b1" in manager._verify_cache
        assert "hash-a1" not in manager._verify_cache
        assert "hash-a2" not in manager._verify_cache

    def test_unknown_user_returns_zero(self) -> None:
        """No matching entries → 0 evictions, no exception."""
        manager = _fresh_manager()
        assert manager.invalidate_user_cache("never-cached") == 0


class TestVerifyCachePrune:
    """Opportunistic pruning keeps the cache bounded (§3.6.3)."""

    @pytest.mark.asyncio
    async def test_hard_cap_applies_when_all_entries_fresh(self) -> None:
        """Fresh-only burst → oldest entries hard-evicted to stay at cap.

        Codex R1 MAJOR regression guard: a burst of unique tokens
        within TTL must not let the cache grow unbounded past
        ``VERIFY_CACHE_MAX_ENTRIES``. The stale pass would find
        nothing to evict; the hard-cap pass drops the oldest by
        ``cached_at_ts`` down to the limit.
        """
        manager = _fresh_manager()
        base_ts = datetime.now(UTC).timestamp()
        for i in range(VERIFY_CACHE_MAX_ENTRIES):
            manager._verify_cache[f"fresh-{i}"] = _VerifyCacheEntry(
                is_valid=True,
                user_is_active=True,
                user_public_id=f"user-{i}",
                expires_at_ts=base_ts + 900,
                cached_at_ts=base_ts + i * 0.001,
            )
        assert len(manager._verify_cache) == VERIFY_CACHE_MAX_ENTRIES
        token = _mint_access_token(manager, user_public_id="burst-user")
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(user_public_id="burst-user")
        )
        await manager.verify_token_with_db(token, repo)
        assert len(manager._verify_cache) <= VERIFY_CACHE_MAX_ENTRIES
        assert "fresh-0" not in manager._verify_cache
        assert hash_token(token) in manager._verify_cache

    @pytest.mark.asyncio
    async def test_prune_runs_when_cache_exceeds_threshold(self) -> None:
        """Adding past the bound evicts every expired / past-TTL entry.

        Seed the cache over the max-entries threshold with stale
        timestamps. The next verify call triggers
        :meth:`_prune_verify_cache` via :meth:`_cache_verdict` when
        size > ``VERIFY_CACHE_MAX_ENTRIES``.
        """
        manager = _fresh_manager()
        stale_ts = datetime.now(UTC).timestamp() - (VERIFY_CACHE_TTL_SECONDS * 2)
        for i in range(VERIFY_CACHE_MAX_ENTRIES + 1):
            manager._verify_cache[f"stale-{i}"] = _VerifyCacheEntry(
                is_valid=False,
                user_is_active=False,
                user_public_id="",
                expires_at_ts=stale_ts,
                cached_at_ts=stale_ts,
            )
        assert len(manager._verify_cache) == VERIFY_CACHE_MAX_ENTRIES + 1
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=_make_verification_row())
        await manager.verify_token_with_db(token, repo)
        stale_keys_remaining = [k for k in manager._verify_cache if k.startswith("stale-")]
        assert stale_keys_remaining == []
