"""Tests for the DB-backed async verify_token + 30s LRU cache.

Covers :meth:`TokenManager.verify_token_with_db`:

- DB lookup rejects tokens missing from ``user_active_tokens``.
- DB lookup rejects revoked rows.
- DB lookup rejects deactivated users via the SCD2-active join.
- 30s LRU cache memoises positive AND negative verdicts.
- :meth:`TokenManager.invalidate_user_cache` evicts matching entries.
- Stale entries (past TTL / JWT ``exp``) are pruned under bounded growth.
"""

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from loguru import logger

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import BLACKLIST_MAX_ENTRIES
from snapper.auth.tokens import PURPOSE_MISMATCH_JTI
from snapper.auth.tokens import PURPOSE_MISMATCH_JTI_PREFIX
from snapper.auth.tokens import REJECTION_REASON_INVALID
from snapper.auth.tokens import REJECTION_REASON_USER_DEACTIVATED
from snapper.auth.tokens import TOKEN_PURPOSE_MISMATCH_EVENT
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TOKEN_TYPE_REFRESH
from snapper.auth.tokens import VERIFY_CACHE_MAX_ENTRIES
from snapper.auth.tokens import VERIFY_CACHE_TTL_SECONDS
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import _inventory_facts
from snapper.auth.tokens import _VerifyCacheEntry
from snapper.auth.tokens import _VerifyCacheGuard
from snapper.auth.tokens import hash_token
from snapper.data.repository_types import UserActiveTokenVerificationRow


def _fresh_manager() -> TokenManager:
    """Clean-state TokenManager singleton."""
    TokenManager._initialized = False
    manager = TokenManager()
    manager._blacklisted_tokens.clear()
    manager._blacklist_cleanup_heap.clear()
    manager._next_blacklist_cleanup_ts = float("inf")
    manager._verify_cache.clear()
    manager._user_cache_generations.clear()
    return manager


_ACCESS: Final[str] = TOKEN_TYPE_ACCESS
"""Local alias so the mandatory purpose argument fits the line budget."""


def _signed_jti(manager: TokenManager, token: str) -> str:
    """Return the ``jti`` the presented JWT was actually signed with."""
    claims = manager.verify_token(token)
    assert claims is not None
    return claims.jti


def _make_verification_row(
    *,
    jti: str,
    revoked: bool = False,
    user_is_active: bool = True,
    user_public_id: str = "user-verify",
    token_type: str = TOKEN_TYPE_ACCESS,
) -> UserActiveTokenVerificationRow:
    """Inventory projection that names ``jti`` and records ``token_type``."""
    now = datetime.now(UTC)
    return UserActiveTokenVerificationRow(
        user_public_id=user_public_id,
        revoked_at=now if revoked else None,
        expires_at=now + timedelta(minutes=15),
        user_is_active=user_is_active,
        token_type=token_type,
        jti=jti,
    )


def _entry(
    row: UserActiveTokenVerificationRow | None,
    *,
    cached_at_ts: float,
    expires_at_ts: float,
) -> _VerifyCacheEntry:
    """Cache entry holding exactly the facts one inventory read would yield."""
    return _VerifyCacheEntry(
        facts=_inventory_facts(row),
        expires_at_ts=expires_at_ts,
        cached_at_ts=cached_at_ts,
        membership_claim_is_current=True,
    )


def _mint_access_token(
    manager: TokenManager,
    *,
    user_public_id: str = "user-verify",
    operator_public_ids: list[str] | None = None,
    operator_membership_public_ids: dict[str, str] | None = None,
) -> str:
    """Produce a fresh access JWT suitable for verify_token_with_db."""
    principal = AuthPrincipal(
        username="verify-user",
        role=UserRole.VIEWER,
        user_public_id=user_public_id,
        operator_public_ids=operator_public_ids or [],
        operator_membership_public_ids=operator_membership_public_ids or {},
    )
    return manager.create_tokens(principal).access_token


def _mint_refresh_token(manager: TokenManager, *, user_public_id: str = "user-verify") -> str:
    """Produce a fresh refresh JWT — the credential a bearer must never be."""
    principal = AuthPrincipal(
        username="verify-user",
        role=UserRole.VIEWER,
        user_public_id=user_public_id,
    )
    return manager.create_tokens(principal).refresh_token


@contextmanager
def _purpose_events() -> Iterator[list[tuple[str, str]]]:
    """Capture ``(expected, actual)`` from every structured purpose-mismatch record.

    Renders the BOUND fields rather than the interpolated sentence, so
    the assertions fail if the event stops carrying its structure even
    when the human-readable text still reads correctly.
    """
    captured: list[tuple[str, str]] = []

    def _sink(message: str) -> None:
        """Collect one rendered record's expected/actual purpose pair."""
        expected, actual = message.strip().split("|")
        captured.append((expected, actual))

    handler_id = logger.add(
        _sink,
        level="WARNING",
        format="{extra[expected_token_type]}|{extra[actual_token_type]}",
        filter=lambda record: record["extra"].get("event") == TOKEN_PURPOSE_MISMATCH_EVENT,
    )
    try:
        yield captured
    finally:
        logger.remove(handler_id)


class TestVerifyTokenWithDB:
    """DB + cache gates in sequence."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_claims_and_caches(self) -> None:
        """Valid JWT + inventory row + active user → claims + positive cache entry."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        claims = await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS)
        assert claims is not None
        assert claims.username == "verify-user"
        assert hash_token(token) in manager._verify_cache
        entry = manager._verify_cache[hash_token(token)]
        assert entry.facts.row_exists is True
        assert entry.facts.is_revoked is False
        assert entry.facts.user_is_active is True
        assert entry.facts.user_public_id == "user-verify"
        assert entry.facts.token_type == TOKEN_TYPE_ACCESS

    @pytest.mark.asyncio
    async def test_cache_hit_avoids_second_db_call(self) -> None:
        """Second call within 30s TTL short-circuits; DB is consulted ONCE."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        assert (
            await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS) is not None
        )
        assert (
            await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS) is not None
        )
        assert repo.get_active_token_by_hash.await_count == 1

    @pytest.mark.asyncio
    async def test_token_persisted_after_detach_cannot_revive_on_reattach(self) -> None:
        """Mint-before-detach and inventory-write-after-detach stays closed.

        The inventory row is present and active, modelling a login that minted
        from an old membership snapshot and persisted its token after the desk
        detach committed. Re-attaching the same operator creates a different
        membership public ID, so that otherwise-identical operator claim cannot
        revive before JWT expiry.
        """
        manager = _fresh_manager()
        operator_public_id = "detached-operator"
        old_membership_public_id = "membership-before-detach"
        token = _mint_access_token(
            manager,
            user_public_id="detached-user",
            operator_public_ids=[operator_public_id],
            operator_membership_public_ids={operator_public_id: old_membership_public_id},
        )
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token),
                user_public_id="detached-user",
            )
        )
        repo.get_user_operator_memberships = AsyncMock(return_value=[])
        assert (
            await manager.verify_token_with_db(
                token,
                repo,
                expected_token_type=_ACCESS,
            )
            is None
        )
        entry = manager._verify_cache[hash_token(token)]
        assert entry.role == UserRole.VIEWER
        assert entry.operator_public_ids == (operator_public_id,)
        assert entry.operator_membership_public_ids == (
            (operator_public_id, old_membership_public_id),
        )
        assert entry.membership_claim_is_current is False
        assert (
            await manager.verify_token_with_db(
                token,
                repo,
                expected_token_type=_ACCESS,
            )
            is None
        )
        repo.get_active_token_by_hash.assert_awaited_once()
        repo.get_user_operator_memberships.assert_awaited_once()

        manager.invalidate_user_cache("detached-user")
        repo.get_user_operator_memberships.return_value = [
            {
                "public_id": "membership-after-reattach",
                "operator_public_id": operator_public_id,
            }
        ]
        assert (
            await manager.verify_token_with_db(
                token,
                repo,
                expected_token_type=_ACCESS,
            )
            is None
        )
        assert repo.get_active_token_by_hash.await_count == 2
        assert repo.get_user_operator_memberships.await_count == 2

    @pytest.mark.asyncio
    async def test_nonempty_operator_claim_rejects_membership_lookup_failure(self) -> None:
        """A DB error cannot turn an unverified desk claim into authority."""
        manager = _fresh_manager()
        token = _mint_access_token(manager, operator_public_ids=["operator-1"])
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        repo.get_user_operator_memberships = AsyncMock(
            side_effect=RuntimeError("membership database unavailable")
        )
        assert (
            await manager.verify_token_with_db(
                token,
                repo,
                expected_token_type=_ACCESS,
            )
            is None
        )
        assert manager._verify_cache[hash_token(token)].membership_claim_is_current is False

    @pytest.mark.asyncio
    async def test_nonempty_operator_claim_without_inventory_owner_fails_closed(self) -> None:
        """A desk claim cannot be checked when inventory has no owning user."""
        manager = _fresh_manager()
        token = _mint_access_token(manager, operator_public_ids=["operator-1"])
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        repo.get_user_operator_memberships = AsyncMock(return_value=[])
        assert (
            await manager.verify_token_with_db(
                token,
                repo,
                expected_token_type=_ACCESS,
            )
            is None
        )
        repo.get_user_operator_memberships.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nonempty_operator_claim_accepts_active_membership_subset(self) -> None:
        """A legacy version-less claim keeps operator-subset compatibility."""
        manager = _fresh_manager()
        token = _mint_access_token(manager, operator_public_ids=["operator-1"])
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        repo.get_user_operator_memberships = AsyncMock(
            return_value=[{"operator_public_id": "operator-1"}]
        )
        claims = await manager.verify_token_with_db(
            token,
            repo,
            expected_token_type=_ACCESS,
        )
        assert claims is not None
        assert manager._verify_cache[hash_token(token)].membership_claim_is_current is True

    @pytest.mark.asyncio
    async def test_versioned_operator_claim_accepts_exact_active_membership(self) -> None:
        """A current membership public ID admits the newly versioned credential."""
        manager = _fresh_manager()
        token = _mint_access_token(
            manager,
            operator_public_ids=["operator-1"],
            operator_membership_public_ids={"operator-1": "membership-current"},
        )
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        repo.get_user_operator_memberships = AsyncMock(
            return_value=[
                {
                    "public_id": "membership-current",
                    "operator_public_id": "operator-1",
                }
            ]
        )
        claims = await manager.verify_token_with_db(
            token,
            repo,
            expected_token_type=_ACCESS,
        )
        assert claims is not None
        assert claims.operator_membership_public_ids == {"operator-1": "membership-current"}

    @pytest.mark.asyncio
    async def test_explicit_admin_scope_uses_effective_global_authority(self) -> None:
        """Structural impersonation authority, not ADMIN identity, drives bypass."""
        manager = _fresh_manager()
        principal = AuthPrincipal(
            username="admin-user",
            role=UserRole.ADMIN,
            user_public_id="admin-user",
            operator_public_ids=["global-operator"],
        )
        token = manager.create_tokens(
            principal,
            permissions=[Permission.MANAGE_DESK_MEMBERSHIPS],
        ).access_token
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token),
                user_public_id="admin-user",
            )
        )
        repo.get_user_operator_memberships = AsyncMock(return_value=[])
        claims = await manager.verify_token_with_db(
            token,
            repo,
            expected_token_type=_ACCESS,
        )
        assert claims is not None
        entry = manager._verify_cache[hash_token(token)]
        assert entry.has_global_operator_authority is True
        repo.get_user_operator_memberships.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_row_returns_none_caches_negative(self) -> None:
        """Token absent from inventory → None + negative verdict cached."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        assert await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS) is None
        assert await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS) is None
        assert repo.get_active_token_by_hash.await_count == 1
        entry = manager._verify_cache[hash_token(token)]
        assert entry.facts.row_exists is False
        assert entry.facts.user_is_active is False

    @pytest.mark.asyncio
    async def test_revoked_row_returns_none(self) -> None:
        """Row present but ``revoked_at`` set → None."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token), revoked=True)
        )
        assert await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS) is None
        entry = manager._verify_cache[hash_token(token)]
        assert entry.facts.is_revoked is True

    @pytest.mark.asyncio
    async def test_deactivated_user_returns_none(self) -> None:
        """Row present + active but ``users.is_active=False`` → None."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), user_is_active=False
            )
        )
        assert await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS) is None

    @pytest.mark.asyncio
    async def test_invalid_jwt_never_touches_db(self) -> None:
        """Failed JWT signature/expiry short-circuits BEFORE DB lookup."""
        manager = _fresh_manager()
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        assert (
            await manager.verify_token_with_db("not.a.jwt", repo, expected_token_type=_ACCESS)
            is None
        )
        repo.get_active_token_by_hash.assert_not_awaited()


class TestVerifyTokenWithReasonBranches:
    """Direct coverage of :meth:`TokenManager.verify_token_with_reason` reason paths.

    ``verify_token_with_db`` delegates to ``verify_token_with_reason``;
    the wrapper tests above only exercise the ``TokenClaims | None``
    projection. These tests assert the ``rejection_reason`` field
    directly so a future refactor that collapses the
    USER_DEACTIVATED / INVALID mapping fails loudly.
    """

    @pytest.mark.asyncio
    async def test_jwt_fail_returns_invalid(self) -> None:
        """Signature / expiry / blacklist failure → ``invalid``."""
        manager = _fresh_manager()
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        outcome = await manager.verify_token_with_reason(
            "not.a.jwt", repo, expected_token_type=_ACCESS
        )
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_INVALID
        repo.get_active_token_by_hash.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_blacklisted_token_returns_invalid_without_db_lookup(self) -> None:
        """Post-grace blacklist rejection short-circuits before DB lookup."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        token_data = manager.verify_token(token)
        assert token_data is not None
        manager.blacklist_token_immediately(token_data.jti)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=token_data.jti)
        )
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_INVALID
        repo.get_active_token_by_hash.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_success_has_none_reason(self) -> None:
        """Valid JWT + row + active user → claims + ``rejection_reason=None``."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is not None
        assert outcome.rejection_reason is None

    @pytest.mark.asyncio
    async def test_missing_inventory_row_returns_invalid(self) -> None:
        """No row in inventory → ``invalid`` (pre-migration / forged)."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_INVALID

    @pytest.mark.asyncio
    async def test_revoked_row_user_active_returns_invalid(self) -> None:
        """Revoked token but user still active → ``invalid`` (not deactivated)."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), revoked=True, user_is_active=True
            )
        )
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_INVALID

    @pytest.mark.asyncio
    async def test_revoked_row_user_deactivated_returns_user_deactivated(self) -> None:
        """Revoked AND deactivated → ``user_deactivated`` wins.

        When a token is both revoked (e.g. by the kill switch) AND
        the user is deactivated, the deactivation signal is more
        informative to the client than the revocation.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), revoked=True, user_is_active=False
            )
        )
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_USER_DEACTIVATED

    @pytest.mark.asyncio
    async def test_active_row_user_deactivated_returns_user_deactivated(self) -> None:
        """Token not revoked but user deactivated → ``user_deactivated``."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), user_is_active=False
            )
        )
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_USER_DEACTIVATED

    @pytest.mark.asyncio
    async def test_cache_hit_deactivated_returns_user_deactivated(self) -> None:
        """Cached verdict with populated ``user_public_id`` + ``user_is_active=False``."""
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        th = hash_token(token)
        now_ts = datetime.now(UTC).timestamp()
        manager._verify_cache[th] = _entry(
            _make_verification_row(
                jti=_signed_jti(manager, token),
                user_is_active=False,
                user_public_id="user-cached-deactivated",
            ),
            cached_at_ts=now_ts,
            expires_at_ts=now_ts + 900,
        )
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.rejection_reason == REJECTION_REASON_USER_DEACTIVATED
        repo.get_active_token_by_hash.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cache_hit_missing_inventory_returns_invalid(self) -> None:
        """Cached negative entry with empty ``user_public_id`` → ``invalid``.

        The ``user_public_id=""`` sentinel means the row was missing
        at the time of the prior lookup — neither kill-switch nor
        deactivation applies.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        th = hash_token(token)
        now_ts = datetime.now(UTC).timestamp()
        manager._verify_cache[th] = _entry(
            None,
            cached_at_ts=now_ts,
            expires_at_ts=now_ts + 900,
        )
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=None)
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.rejection_reason == REJECTION_REASON_INVALID
        repo.get_active_token_by_hash.assert_not_awaited()


class TestInvalidateUserCache:
    """Cross-instance LRU eviction for the admin-bus subscriber."""

    def test_evicts_matching_user_entries_only(self) -> None:
        """Entries whose user_public_id matches are dropped; others stay.

        Simulates the ``admin.user_deactivated`` dispatch: the
        TokenManager subscribes to the bus and calls
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
            manager._verify_cache[th] = _entry(
                _make_verification_row(jti=f"jti-{uid}", user_public_id=uid),
                cached_at_ts=now_ts,
                expires_at_ts=now_ts + 900,
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
    """Opportunistic pruning keeps the cache bounded."""

    @pytest.mark.asyncio
    async def test_hard_cap_applies_when_all_entries_fresh(self) -> None:
        """Fresh-only burst → oldest entries hard-evicted to stay at cap.

        Regression guard: a burst of unique tokens
        within TTL must not let the cache grow unbounded past
        ``VERIFY_CACHE_MAX_ENTRIES``. The stale pass would find
        nothing to evict; the hard-cap pass drops the oldest by
        ``cached_at_ts`` down to the limit.
        """
        manager = _fresh_manager()
        base_ts = datetime.now(UTC).timestamp()
        for i in range(VERIFY_CACHE_MAX_ENTRIES):
            manager._verify_cache[f"fresh-{i}"] = _entry(
                _make_verification_row(jti=f"jti-{i}", user_public_id=f"user-{i}"),
                cached_at_ts=base_ts + i * 0.001,
                expires_at_ts=base_ts + 900,
            )
        assert len(manager._verify_cache) == VERIFY_CACHE_MAX_ENTRIES
        token = _mint_access_token(manager, user_public_id="burst-user")
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), user_public_id="burst-user"
            )
        )
        await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS)
        assert len(manager._verify_cache) <= VERIFY_CACHE_MAX_ENTRIES
        assert "fresh-0" not in manager._verify_cache
        assert hash_token(token) in manager._verify_cache

    @pytest.mark.asyncio
    async def test_prune_runs_when_cache_exceeds_threshold(self) -> None:
        """Adding past the bound evicts every expired / past-TTL entry.

        Seed the cache over the max-entries threshold with stale
        timestamps. The next verify call triggers
        :meth:`_prune_verify_cache` via :meth:`_cache_inventory_facts` when
        size > ``VERIFY_CACHE_MAX_ENTRIES``.
        """
        manager = _fresh_manager()
        stale_ts = datetime.now(UTC).timestamp() - (VERIFY_CACHE_TTL_SECONDS * 2)
        for i in range(VERIFY_CACHE_MAX_ENTRIES + 1):
            manager._verify_cache[f"stale-{i}"] = _entry(
                None,
                cached_at_ts=stale_ts,
                expires_at_ts=stale_ts,
            )
        assert len(manager._verify_cache) == VERIFY_CACHE_MAX_ENTRIES + 1
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        await manager.verify_token_with_db(token, repo, expected_token_type=_ACCESS)
        stale_keys_remaining = [k for k in manager._verify_cache if k.startswith("stale-")]
        assert stale_keys_remaining == []


class TestVerifyCacheGenerationRace:
    """Cache cannot be repopulated with stale data after invalidation.

    Simulates the race: a concurrent ``invalidate_user_cache`` bump
    during a DB read must prevent the racing verify call from
    repopulating the cache with a verdict that the admin event
    already superseded.
    """

    @pytest.mark.asyncio
    async def test_generation_bump_blocks_cache_repopulation(self) -> None:
        """Concurrent admin-bus eviction during DB read → cache NOT written.

        Given: a verify call that has passed the sync ``verify_token``
            gate and is about to read the DB,
        When: :meth:`invalidate_user_cache` runs for the same user
            BEFORE the verify call reaches :meth:`_cache_inventory_facts`
            (simulated by bumping the generation manually after
            sampling),
        Then: :meth:`_cache_inventory_facts` sees the mismatched generation
            and skips the write — the cache stays empty rather than
            storing a positive verdict the admin event has already
            invalidated.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager, user_public_id="race-user")
        th = hash_token(token)
        token_data = manager.verify_token(token)
        assert token_data is not None
        gen_before = manager._user_cache_generations.get("race-user", 0)
        manager.invalidate_user_cache("race-user")
        manager._cache_inventory_facts(
            th,
            facts=_inventory_facts(
                _make_verification_row(jti=token_data.jti, user_public_id="race-user")
            ),
            token_data=token_data,
            now_ts=datetime.now(UTC).timestamp(),
            guard=_VerifyCacheGuard(generation_before=gen_before),
        )
        assert th not in manager._verify_cache

    @pytest.mark.asyncio
    async def test_generation_unchanged_still_caches(self) -> None:
        """Quiescent path — no racing bump → verdict lands in cache.

        Given: a verify call with no competing admin event,
        When: :meth:`_cache_inventory_facts` is called with the generation
            sampled at entry,
        Then: the cache entry IS written (the guard must not
            regress the normal path).
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager, user_public_id="quiet-user")
        th = hash_token(token)
        token_data = manager.verify_token(token)
        assert token_data is not None
        gen_before = manager._user_cache_generations.get("quiet-user", 0)
        manager._cache_inventory_facts(
            th,
            facts=_inventory_facts(
                _make_verification_row(jti=token_data.jti, user_public_id="quiet-user")
            ),
            token_data=token_data,
            now_ts=datetime.now(UTC).timestamp(),
            guard=_VerifyCacheGuard(generation_before=gen_before),
        )
        assert th in manager._verify_cache

    @pytest.mark.asyncio
    async def test_blank_claim_always_fails_closed(self) -> None:
        """Legacy blank-claim tokens are NEVER cached — fail-closed.

        Given: a token whose claim ``user_public_id=""`` (legacy
            issuance shape) and a successful DB-row lookup,
        When: :meth:`_cache_inventory_facts` runs,
        Then: the cache is NOT written — the race guard needs a
            pre-DB-read sample keyed off the same identifier the
            admin-bus listener bumps, but a blank claim has no such
            identifier before the await. This closes the gap where
            sampling the row id post-await cannot detect an
            invalidate-during-DB-read race. ``refresh_tokens``
            re-mints pairs from old claims, so blank-claim tokens
            remain representable; they pay a perf penalty (always
            DB-backed) for refresh/long-lived token lifetimes, not
            merely the 15-minute access TTL.
        """
        manager = _fresh_manager()
        now = int(datetime.now(UTC).timestamp())
        legacy_claims = TokenClaims(
            sub="legacy-subject",
            username="legacy-user",
            role=UserRole.VIEWER,
            permissions=[],
            exp=now + 3600,
            iat=now,
            jti="legacy-jti",
            sid="legacy-sid",
            user_public_id="",
        )
        manager._cache_inventory_facts(
            "legacy-hash",
            facts=_inventory_facts(
                _make_verification_row(jti="legacy-jti", user_public_id="legacy-row-id")
            ),
            token_data=legacy_claims,
            now_ts=datetime.now(UTC).timestamp(),
            guard=_VerifyCacheGuard(generation_before=0),
        )
        assert "legacy-hash" not in manager._verify_cache

    @pytest.mark.asyncio
    async def test_blank_claim_and_blank_row_skips_cache(self) -> None:
        """When neither claim nor row carries a user id, caching is skipped.

        Given: a token whose claim is blank AND the DB lookup
            returned a row without a ``user_public_id``,
        When: :meth:`_cache_inventory_facts` runs,
        Then: nothing is cached — the race guard has no
            authoritative key to compare against, so fail-closed.
        """
        manager = _fresh_manager()
        now = int(datetime.now(UTC).timestamp())
        blank_claims = TokenClaims(
            sub="s",
            username="u",
            role=UserRole.VIEWER,
            permissions=[],
            exp=now + 3600,
            iat=now,
            jti="j",
            sid="sid",
            user_public_id="",
        )
        manager._cache_inventory_facts(
            "orphan-hash",
            facts=_inventory_facts(None),
            token_data=blank_claims,
            now_ts=datetime.now(UTC).timestamp(),
            guard=_VerifyCacheGuard(generation_before=0),
        )
        assert "orphan-hash" not in manager._verify_cache

    @pytest.mark.asyncio
    async def test_end_to_end_concurrent_invalidate_blocks_positive(self) -> None:
        """End-to-end: invalidate during the awaited DB read → no cache fill.

        Given: a repository whose
            ``get_active_token_by_hash`` awaits long enough for
            :meth:`invalidate_user_cache` to run concurrently,
        When: ``verify_token_with_reason`` completes,
        Then: the verdict is NOT cached; the next verify re-reads
            the DB rather than serving a stale positive.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager, user_public_id="concurrent-user")

        async def _slow_lookup(_hash: str) -> UserActiveTokenVerificationRow:
            await asyncio.sleep(0)
            manager.invalidate_user_cache("concurrent-user")
            return _make_verification_row(
                jti=_signed_jti(manager, token), user_public_id="concurrent-user"
            )

        repo = MagicMock()
        repo.get_active_token_by_hash = _slow_lookup
        await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert hash_token(token) not in manager._verify_cache


class TestBlacklistHardCap:
    """JTI blacklist cannot leak unbounded.

    Covers :meth:`_enforce_blacklist_cap`: when a mass deactivation
    pushes the set past :data:`BLACKLIST_MAX_ENTRIES`, the oldest
    overflow is evicted so a low-traffic instance does not keep
    every JTI in memory until grace expires.
    """

    def test_cap_evicts_oldest_on_overflow(self) -> None:
        """Adding past the cap drops the oldest entries first.

        Given: the blacklist seeded to exactly the cap with
            monotonically-increasing timestamps,
        When: one more JTI is added,
        Then: the set size stays at the cap and the oldest entry is
            the one that was evicted.
        """
        manager = _fresh_manager()
        base_ts = datetime.now(UTC).timestamp() - 1000.0
        for i in range(BLACKLIST_MAX_ENTRIES):
            manager._blacklisted_tokens[f"jti-{i:06d}"] = base_ts + i
        assert len(manager._blacklisted_tokens) == BLACKLIST_MAX_ENTRIES
        manager.blacklist_token("jti-fresh")
        assert len(manager._blacklisted_tokens) == BLACKLIST_MAX_ENTRIES
        assert "jti-000000" not in manager._blacklisted_tokens
        assert "jti-fresh" in manager._blacklisted_tokens

    def test_cap_no_op_when_under_threshold(self) -> None:
        """No evictions when set stays within the cap.

        Given: a blacklist well below the cap,
        When: more entries are added,
        Then: nothing is evicted.
        """
        manager = _fresh_manager()
        for i in range(100):
            manager.blacklist_token(f"jti-{i}")
        assert len(manager._blacklisted_tokens) == 100

    def test_immediate_blacklist_also_enforces_cap(self) -> None:
        """:meth:`blacklist_token_immediately` shares the same cap path.

        Given: blacklist at the cap with the immediate-invalidate
            API used instead of the grace-period one,
        When: one more JTI is invalidated,
        Then: the cap is enforced identically.
        """
        manager = _fresh_manager()
        base_ts = datetime.now(UTC).timestamp() - 1000.0
        for i in range(BLACKLIST_MAX_ENTRIES):
            manager._blacklisted_tokens[f"imm-{i:06d}"] = base_ts + i
        manager.blacklist_token_immediately("imm-fresh")
        assert len(manager._blacklisted_tokens) == BLACKLIST_MAX_ENTRIES
        assert "imm-000000" not in manager._blacklisted_tokens


class TestTokenPurposeGate:
    """The inventory row decides what a live credential may be used AS.

    Every case here holds the lifecycle gates constant — unrevoked
    row, unexpired row, active user — so the only thing under test is
    whether purpose is enforced. Before this gate existed each of these
    tokens authenticated.
    """

    @pytest.mark.asyncio
    async def test_refresh_row_rejected_for_an_access_check(self) -> None:
        """A refresh credential presented as a bearer is refused.

        Given: a live ``refresh`` inventory row whose ``jti`` matches
            the presented refresh JWT and whose owner is active,
        When: a transport that accepts only access bearers verifies it,
        Then: verification fails and the structured security event
            names the expected and the actual purpose.
        """
        manager = _fresh_manager()
        token = _mint_refresh_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), token_type=TOKEN_TYPE_REFRESH
            )
        )
        with _purpose_events() as events:
            outcome = await manager.verify_token_with_reason(
                token, repo, expected_token_type=_ACCESS
            )
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_INVALID
        assert events == [(TOKEN_TYPE_ACCESS, TOKEN_TYPE_REFRESH)]

    @pytest.mark.asyncio
    async def test_refresh_row_accepted_for_a_refresh_check(self) -> None:
        """The same credential still works where it is supposed to.

        Given: the live ``refresh`` row and JWT of the previous case,
        When: refresh rotation verifies it as a refresh credential,
        Then: the claims come back and no security event is emitted.
        """
        manager = _fresh_manager()
        token = _mint_refresh_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), token_type=TOKEN_TYPE_REFRESH
            )
        )
        with _purpose_events() as events:
            outcome = await manager.verify_token_with_reason(
                token, repo, expected_token_type=TOKEN_TYPE_REFRESH
            )
        assert outcome.claims is not None
        assert events == []

    @pytest.mark.asyncio
    async def test_access_token_rejected_at_a_refresh_check(self) -> None:
        """An access bearer cannot be redeemed for a new token pair.

        Given: a live ``access`` row and its matching access JWT,
        When: refresh rotation verifies it,
        Then: verification fails with the purpose event.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        with _purpose_events() as events:
            outcome = await manager.verify_token_with_reason(
                token, repo, expected_token_type=TOKEN_TYPE_REFRESH
            )
        assert outcome.claims is None
        assert events == [(TOKEN_TYPE_REFRESH, TOKEN_TYPE_ACCESS)]

    @pytest.mark.asyncio
    async def test_row_naming_a_different_jti_is_rejected(self) -> None:
        """The row must name the credential it is being trusted to describe.

        Given: an ``access`` row whose ``jti`` is not the signed one,
        When: an access check runs,
        Then: verification fails and the event reports the jti
            disagreement rather than a type disagreement.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti="some-other-credential")
        )
        with _purpose_events() as events:
            outcome = await manager.verify_token_with_reason(
                token, repo, expected_token_type=_ACCESS
            )
        assert outcome.claims is None
        assert events == [(TOKEN_TYPE_ACCESS, PURPOSE_MISMATCH_JTI)]

    @pytest.mark.asyncio
    async def test_access_row_with_a_refresh_prefixed_jti_is_rejected(self) -> None:
        """The signed prefix must corroborate the row, not contradict it.

        Given: a refresh JWT whose row was recorded as ``access`` — the
            two records of one credential's purpose disagree,
        When: an access check runs,
        Then: verification fails on the prefix contradiction. Trusting
            the row alone here would admit the refresh credential.
        """
        manager = _fresh_manager()
        token = _mint_refresh_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        with _purpose_events() as events:
            outcome = await manager.verify_token_with_reason(
                token, repo, expected_token_type=_ACCESS
            )
        assert outcome.claims is None
        assert events == [(TOKEN_TYPE_ACCESS, PURPOSE_MISMATCH_JTI_PREFIX)]

    @pytest.mark.asyncio
    async def test_refresh_row_with_a_bare_jti_is_rejected(self) -> None:
        """The contradiction is refused in the other direction too.

        Given: an access JWT whose row was recorded as ``refresh``,
        When: refresh rotation verifies it,
        Then: verification fails on the prefix contradiction rather
            than accepting it because the row's type matched.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), token_type=TOKEN_TYPE_REFRESH
            )
        )
        with _purpose_events() as events:
            outcome = await manager.verify_token_with_reason(
                token, repo, expected_token_type=TOKEN_TYPE_REFRESH
            )
        assert outcome.claims is None
        assert events == [(TOKEN_TYPE_REFRESH, PURPOSE_MISMATCH_JTI_PREFIX)]

    @pytest.mark.asyncio
    async def test_a_cached_refresh_verdict_cannot_satisfy_an_access_check(self) -> None:
        """The 30-second cache must not launder purpose across transports.

        Given: a refresh credential that has just been accepted at
            refresh rotation, leaving a fresh entry in the shared
            hash-keyed cache,
        When: the same credential is presented as a bearer within the
            TTL, so the DB is never re-read,
        Then: the cached entry is re-judged under the access purpose
            and rejected. A cache that stored the rotation's verdict
            instead of the row's facts would return the claims here.
        """
        manager = _fresh_manager()
        token = _mint_refresh_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(
                jti=_signed_jti(manager, token), token_type=TOKEN_TYPE_REFRESH
            )
        )
        accepted = await manager.verify_token_with_reason(
            token, repo, expected_token_type=TOKEN_TYPE_REFRESH
        )
        assert accepted.claims is not None
        assert hash_token(token) in manager._verify_cache
        with _purpose_events() as events:
            replayed = await manager.verify_token_with_reason(
                token, repo, expected_token_type=_ACCESS
            )
        assert replayed.claims is None
        assert replayed.rejection_reason == REJECTION_REASON_INVALID
        assert events == [(TOKEN_TYPE_ACCESS, TOKEN_TYPE_REFRESH)]
        assert repo.get_active_token_by_hash.await_count == 1

    @pytest.mark.asyncio
    async def test_a_cached_access_verdict_still_serves_an_access_check(self) -> None:
        """Re-judging a hit must not cost the cache its purpose.

        Given: an access credential accepted once, leaving a cache
            entry,
        When: it is presented again within the TTL,
        Then: it is accepted from the cache with no second DB read —
            the re-evaluation is a gate, not a cache bypass.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(
            return_value=_make_verification_row(jti=_signed_jti(manager, token))
        )
        assert (
            await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        ).claims is not None
        assert (
            await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        ).claims is not None
        assert repo.get_active_token_by_hash.await_count == 1

    @pytest.mark.asyncio
    async def test_an_expired_inventory_row_is_refused(self) -> None:
        """A row past its own ``expires_at`` is dead regardless of the JWT.

        Given: an unrevoked ``access`` row whose ``expires_at`` is in
            the past, owned by an active user,
        When: an access check runs,
        Then: verification fails — acceptance requires the row to be
            unexpired, not merely unrevoked.
        """
        manager = _fresh_manager()
        token = _mint_access_token(manager)
        stale = datetime.now(UTC) - timedelta(minutes=5)
        row = _make_verification_row(jti=_signed_jti(manager, token))
        row["expires_at"] = stale
        repo = MagicMock()
        repo.get_active_token_by_hash = AsyncMock(return_value=row)
        outcome = await manager.verify_token_with_reason(token, repo, expected_token_type=_ACCESS)
        assert outcome.claims is None
        assert outcome.rejection_reason == REJECTION_REASON_INVALID
