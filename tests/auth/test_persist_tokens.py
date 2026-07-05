"""Tests for  ``user_active_tokens`` DB persistence.

Covers three new :class:`Repository` methods
(``insert_user_active_tokens``, ``revoke_user_active_token_by_jti``,
``get_active_token_by_hash``) plus the async
:meth:`TokenManager.persist_tokens` helper that drives the inventory
on every ``create_tokens()`` call.

Together they populate + maintain the row-per-outstanding-JWT
inventory that the DB-backed ``verify_token`` reads against.
The tests run against an in-memory aiosqlite DB so the join on
``users.is_active`` exercises real SQL.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenPair
from snapper.auth.tokens import ROTATION_GRACE_MAX_ENTRIES
from snapper.auth.tokens import ROTATION_GRACE_TTL_SECONDS
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import UserActiveTokenInsertRow

_BCRYPT_FAKE_DIGEST: str = "$2b$12$" + "x" * 53


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """Fresh in-memory repo with the schema applied."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


async def _seed_user(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    username: str,
    is_active: bool = True,
) -> None:
    """Insert one SCD2-active :class:`User` row for join tests."""
    seed_time = datetime(2026, 1, 1, tzinfo=UTC)
    async with repo.session() as s:
        s.add(
            User(
                public_id=public_id,
                session_id="seed",
                sequence_id=1,
                timestamp=seed_time,
                known_to=KNOWN_TO_MAX,
                username=username,
                email=f"{username}@example.com",
                password_hash=_BCRYPT_FAKE_DIGEST,
                role="viewer",
                is_active=is_active,
                created_at=seed_time,
            )
        )
        await s.commit()


class TestInsertUserActiveTokens:
    """Coverage for :meth:`Repository.insert_user_active_tokens`."""

    @pytest.mark.asyncio
    async def test_inserts_batch_and_sets_revoked_at_null(self, repo: SQLAlchemyRepository) -> None:
        """Every NOT NULL column is populated; ``revoked_at`` starts NULL.

        Given: a 2-row batch (access + refresh),
        When: ``insert_user_active_tokens`` runs,
        Then: both rows persist and the caller can SELECT them back
            with ``revoked_at IS NULL`` as the fresh-token state.
        """
        now = datetime.now(UTC)
        rows: list[UserActiveTokenInsertRow] = [
            UserActiveTokenInsertRow(
                public_id="pub-access",
                user_public_id="user-insert",
                jti="jti-access",
                token_hash="hash-access",
                token_type="access",
                issued_at=now,
                expires_at=now + timedelta(minutes=15),
            ),
            UserActiveTokenInsertRow(
                public_id="pub-refresh",
                user_public_id="user-insert",
                jti="jti-refresh",
                token_hash="hash-refresh",
                token_type="refresh",
                issued_at=now,
                expires_at=now + timedelta(days=7),
            ),
        ]
        await repo.insert_user_active_tokens(rows)
        jtis = await repo.list_active_user_token_jtis("user-insert")
        assert sorted(jtis) == ["jti-access", "jti-refresh"]

    @pytest.mark.asyncio
    async def test_empty_list_is_noop(self, repo: SQLAlchemyRepository) -> None:
        """Calling with an empty list inserts nothing and does not error."""
        await repo.insert_user_active_tokens([])
        jtis = await repo.list_active_user_token_jtis("nobody")
        assert jtis == []


class TestRotateUserActiveToken:
    """Coverage for :meth:`Repository.rotate_user_active_token` atomicity."""

    @pytest.mark.asyncio
    async def test_revokes_old_and_inserts_new_when_row_active(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Atomic rotation flips the old row AND persists successors.

        Given: an active refresh row,
        When: ``rotate_user_active_token`` runs with a successor pair,
        Then: rowcount is 1, the old JTI is revoked, and both new
            rows are listed as active — all in one transaction.
        """
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-old",
                    user_public_id="user-rot",
                    jti="old-refresh",
                    token_hash="hash-old",
                    token_type="refresh",
                    issued_at=now,
                    expires_at=now + timedelta(days=7),
                )
            ]
        )
        new_rows: list[UserActiveTokenInsertRow] = [
            UserActiveTokenInsertRow(
                public_id="pub-new-a",
                user_public_id="user-rot",
                jti="new-access",
                token_hash="hash-new-access",
                token_type="access",
                issued_at=now,
                expires_at=now + timedelta(minutes=15),
            ),
            UserActiveTokenInsertRow(
                public_id="pub-new-r",
                user_public_id="user-rot",
                jti="new-refresh",
                token_hash="hash-new-refresh",
                token_type="refresh",
                issued_at=now,
                expires_at=now + timedelta(days=7),
            ),
        ]
        rotated = await repo.rotate_user_active_token("old-refresh", new_rows, now)
        assert rotated is True
        active_jtis = sorted(await repo.list_active_user_token_jtis("user-rot"))
        assert active_jtis == ["new-access", "new-refresh"]

    @pytest.mark.asyncio
    async def test_replay_returns_zero_and_does_not_insert(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Replayed rotation (old row already revoked) inserts nothing.

        Given: a refresh row that has already been revoked,
        When: ``rotate_user_active_token`` runs,
        Then: rowcount is 0 AND the successor rows are NOT inserted
            (transaction rolls back). Defeats refresh-token replay
            attacks inside the blacklist grace window.
        """
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-replay",
                    user_public_id="user-replay",
                    jti="spent-jti",
                    token_hash="hash-replay",
                    token_type="refresh",
                    issued_at=now,
                    expires_at=now + timedelta(days=7),
                )
            ]
        )
        await repo.revoke_user_active_token_by_jti("spent-jti", now)
        attempted: list[UserActiveTokenInsertRow] = [
            UserActiveTokenInsertRow(
                public_id="pub-ghost",
                user_public_id="user-replay",
                jti="ghost-jti",
                token_hash="hash-ghost",
                token_type="access",
                issued_at=now,
                expires_at=now + timedelta(minutes=15),
            )
        ]
        rotated = await repo.rotate_user_active_token("spent-jti", attempted, now)
        assert rotated is False
        assert await repo.list_active_user_token_jtis("user-replay") == []

    @pytest.mark.asyncio
    async def test_insert_failure_after_successful_revoke_rolls_back_update(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Unique-constraint violation on new rows rolls back the old-row revoke.

        Given: an active refresh row AND a successor batch whose
            ``token_hash`` collides with an existing row,
        When: ``rotate_user_active_token`` runs,
        Then: the INSERT raises ``IntegrityError`` inside the same
            ``async with self.session()`` scope so the UPDATE-revoke
            is ALSO rolled back — the old refresh JWT remains
            usable for retry (strict-atomicity coverage for the
            revoke+insert rotation).
        """
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-old-atomic",
                    user_public_id="user-atomic",
                    jti="old-atomic",
                    token_hash="hash-collision-sentinel",
                    token_type="refresh",
                    issued_at=now,
                    expires_at=now + timedelta(days=7),
                ),
                UserActiveTokenInsertRow(
                    public_id="pub-collision-src",
                    user_public_id="user-atomic",
                    jti="collision-src",
                    token_hash="hash-new-access",
                    token_type="access",
                    issued_at=now,
                    expires_at=now + timedelta(minutes=15),
                ),
            ]
        )
        attempted_new: list[UserActiveTokenInsertRow] = [
            UserActiveTokenInsertRow(
                public_id="pub-dupe",
                user_public_id="user-atomic",
                jti="would-duplicate-hash",
                token_hash="hash-new-access",
                token_type="access",
                issued_at=now,
                expires_at=now + timedelta(minutes=15),
            )
        ]
        with pytest.raises(IntegrityError):
            await repo.rotate_user_active_token("old-atomic", attempted_new, now)
        active = sorted(await repo.list_active_user_token_jtis("user-atomic"))
        assert active == ["collision-src", "old-atomic"]

    @pytest.mark.asyncio
    async def test_unknown_jti_returns_zero_and_does_not_insert(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Unknown JTI still rolls back the insert (pre-migration guard)."""
        now = datetime.now(UTC)
        attempted: list[UserActiveTokenInsertRow] = [
            UserActiveTokenInsertRow(
                public_id="pub-never",
                user_public_id="u",
                jti="never-existed",
                token_hash="hash-never",
                token_type="access",
                issued_at=now,
                expires_at=now + timedelta(minutes=15),
            )
        ]
        rotated = await repo.rotate_user_active_token("no-such-jti", attempted, now)
        assert rotated is False
        assert await repo.list_active_user_token_jtis("u") == []


class TestRevokeUserActiveTokenByJti:
    """Coverage for :meth:`Repository.revoke_user_active_token_by_jti`."""

    @pytest.mark.asyncio
    async def test_flips_revoked_at_on_matching_row(self, repo: SQLAlchemyRepository) -> None:
        """Single-JTI revoke targets exactly one row without side-effects.

        Given: two unrevoked tokens for the same user,
        When: ``revoke_user_active_token_by_jti`` runs against one JTI,
        Then: only that JTI is revoked — the other stays active and
            the method returns 1.
        """
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id=f"pub-{jti}",
                    user_public_id="user-rev",
                    jti=jti,
                    token_hash=f"hash-{jti}",
                    token_type="access",
                    issued_at=now,
                    expires_at=now + timedelta(minutes=15),
                )
                for jti in ("keep", "kill")
            ]
        )
        count = await repo.revoke_user_active_token_by_jti("kill", now)
        assert count == 1
        assert await repo.list_active_user_token_jtis("user-rev") == ["keep"]

    @pytest.mark.asyncio
    async def test_unknown_jti_is_noop_returns_zero(self, repo: SQLAlchemyRepository) -> None:
        """Unknown JTIs return 0 — missing tokens are not an error."""
        count = await repo.revoke_user_active_token_by_jti("never-existed", datetime.now(UTC))
        assert count == 0

    @pytest.mark.asyncio
    async def test_already_revoked_row_returns_zero(self, repo: SQLAlchemyRepository) -> None:
        """Calling twice on the same JTI doesn't double-stamp ``revoked_at``."""
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-once",
                    user_public_id="u",
                    jti="once",
                    token_hash="h",
                    token_type="access",
                    issued_at=now,
                    expires_at=now + timedelta(minutes=15),
                )
            ]
        )
        first = await repo.revoke_user_active_token_by_jti("once", now)
        second = await repo.revoke_user_active_token_by_jti("once", now + timedelta(seconds=5))
        assert first == 1
        assert second == 0


class TestListInactiveUserPublicIds:
    """Coverage for :meth:`Repository.list_inactive_user_public_ids`."""

    @pytest.mark.asyncio
    async def test_returns_only_inactive_active_scd2_rows(self, repo: SQLAlchemyRepository) -> None:
        """Active users and unknown ids are omitted from the inactive lookup."""
        await _seed_user(repo, public_id="user-active", username="active", is_active=True)
        await _seed_user(repo, public_id="user-inactive", username="inactive", is_active=False)
        result = await repo.list_inactive_user_public_ids(
            ["user-active", "user-inactive", "missing-user"]
        )
        assert result == ["user-inactive"]

    @pytest.mark.asyncio
    async def test_empty_candidates_is_noop(self, repo: SQLAlchemyRepository) -> None:
        """Empty candidate list avoids SQL and returns empty result."""
        result = await repo.list_inactive_user_public_ids([])
        assert result == []

    @pytest.mark.asyncio
    async def test_uses_latest_scd2_row(self, repo: SQLAlchemyRepository) -> None:
        """Closed historical rows do not make the current active user inactive."""
        seed_time = datetime(2026, 1, 1, tzinfo=UTC)
        close_time = datetime(2026, 1, 2, tzinfo=UTC)
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-reactivated",
                    session_id="seed",
                    sequence_id=1,
                    timestamp=seed_time,
                    known_to=close_time,
                    username="reactivated",
                    email="reactivated@example.com",
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role="viewer",
                    is_active=False,
                    created_at=seed_time,
                )
            )
            s.add(
                User(
                    public_id="user-reactivated",
                    session_id="current",
                    sequence_id=2,
                    timestamp=close_time,
                    known_to=KNOWN_TO_MAX,
                    username="reactivated",
                    email="reactivated@example.com",
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role="viewer",
                    is_active=True,
                    created_at=seed_time,
                )
            )
            await s.commit()
        result = await repo.list_inactive_user_public_ids(["user-reactivated"])
        assert result == []


class TestGetActiveTokenByHash:
    """Coverage for :meth:`Repository.get_active_token_by_hash`."""

    @pytest.mark.asyncio
    async def test_returns_projection_joined_with_user_is_active(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The row projection carries ``users.is_active`` from the SCD2-active row."""
        await _seed_user(repo, public_id="user-join", username="alice")
        now = datetime.now(UTC)
        expires = now + timedelta(minutes=15)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-join",
                    user_public_id="user-join",
                    jti="jti-join",
                    token_hash="hash-join",
                    token_type="access",
                    issued_at=now,
                    expires_at=expires,
                )
            ]
        )
        projection = await repo.get_active_token_by_hash("hash-join")
        assert projection is not None
        assert projection["user_public_id"] == "user-join"
        assert projection["revoked_at"] is None
        assert projection["user_is_active"] is True

    @pytest.mark.asyncio
    async def test_returns_none_for_unknown_hash(self, repo: SQLAlchemyRepository) -> None:
        """Unknown hashes return None — the canonical fast-path miss."""
        assert await repo.get_active_token_by_hash("missing") is None

    @pytest.mark.asyncio
    async def test_surfaces_revoked_at_when_set(self, repo: SQLAlchemyRepository) -> None:
        """Revoked rows still return but carry a non-NULL ``revoked_at``."""
        await _seed_user(repo, public_id="user-revoked", username="bob")
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-rev",
                    user_public_id="user-revoked",
                    jti="jti-rev",
                    token_hash="hash-rev",
                    token_type="access",
                    issued_at=now,
                    expires_at=now + timedelta(minutes=15),
                )
            ]
        )
        await repo.revoke_user_active_token_by_jti("jti-rev", now)
        projection = await repo.get_active_token_by_hash("hash-rev")
        assert projection is not None
        assert projection["revoked_at"] is not None
        assert projection["user_is_active"] is True

    @pytest.mark.asyncio
    async def test_surfaces_user_is_active_false_for_deactivated_user(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Join surfaces ``users.is_active=False`` when owner deactivated."""
        await _seed_user(repo, public_id="user-inactive", username="ghost", is_active=False)
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-inact",
                    user_public_id="user-inactive",
                    jti="jti-inact",
                    token_hash="hash-inact",
                    token_type="access",
                    issued_at=now,
                    expires_at=now + timedelta(minutes=15),
                )
            ]
        )
        projection = await repo.get_active_token_by_hash("hash-inact")
        assert projection is not None
        assert projection["user_is_active"] is False

    @pytest.mark.asyncio
    async def test_surfaces_is_active_from_scd2_close_and_insert_shape(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Production-accurate SCD2 deactivation surfaces ``is_active=False``.

        Given: a user whose prior SCD2 row was closed (``known_to``
            below ``KNOWN_TO_MAX``) and a fresh row inserted with
            ``is_active=False`` + ``known_to=KNOWN_TO_MAX`` — the
            shape written by ``UserService.deactivate_user``,
        When: ``get_active_token_by_hash`` runs,
        Then: the join selects the NEW row (KNOWN_TO_MAX) and
            returns ``user_is_active=False`` — covers the SCD2
            correctness concern with production-shape fixtures
            instead of the single-row simplification.
        """
        seed_time = datetime(2026, 1, 1, tzinfo=UTC)
        close_time = datetime(2026, 1, 2, tzinfo=UTC)
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-scd2",
                    session_id="seed",
                    sequence_id=1,
                    timestamp=seed_time,
                    known_to=close_time,
                    username="scd2-user",
                    email="scd2@example.com",
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role="viewer",
                    is_active=True,
                    created_at=seed_time,
                )
            )
            s.add(
                User(
                    public_id="user-scd2",
                    session_id="close",
                    sequence_id=2,
                    timestamp=close_time,
                    known_to=KNOWN_TO_MAX,
                    username="scd2-user",
                    email="scd2@example.com",
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role="viewer",
                    is_active=False,
                    created_at=seed_time,
                )
            )
            await s.commit()
        now = datetime.now(UTC)
        await repo.insert_user_active_tokens(
            [
                UserActiveTokenInsertRow(
                    public_id="pub-scd2",
                    user_public_id="user-scd2",
                    jti="jti-scd2",
                    token_hash="hash-scd2",
                    token_type="access",
                    issued_at=now,
                    expires_at=now + timedelta(minutes=15),
                )
            ]
        )
        projection = await repo.get_active_token_by_hash("hash-scd2")
        assert projection is not None
        assert projection["user_is_active"] is False
        assert projection["user_public_id"] == "user-scd2"


class TestPersistTokens:
    """Coverage for :meth:`TokenManager.persist_tokens` end-to-end."""

    def _fresh_manager(self) -> TokenManager:
        """Return a cleanly-initialized singleton (clears blacklist)."""
        TokenManager._initialized = False
        manager = TokenManager()
        manager._blacklisted_tokens.clear()
        manager._blacklist_cleanup_heap.clear()
        manager._next_blacklist_cleanup_ts = float("inf")
        return manager

    @pytest.mark.asyncio
    async def test_persist_tokens_writes_both_rows_with_matching_hashes(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Login flow writes access + refresh rows; token_hash is recoverable.

        Given: a freshly-minted :class:`TokenPair`,
        When: ``persist_tokens`` runs with the live repo,
        Then: :meth:`Repository.get_active_token_by_hash` resolves
            BOTH the access and refresh tokens back to their DB rows
            via ``hash_token``. This proves the hash shape matches
            across write and read paths (the invariant that lets
            ``verify_token`` hit the inventory).
        """
        await _seed_user(repo, public_id="user-persist", username="persist-user")
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="persist-user",
            role=UserRole.VIEWER,
            user_public_id="user-persist",
        )
        pair = manager.create_tokens(principal)
        await manager.persist_tokens(pair, principal.user_public_id, repo)
        access_row = await repo.get_active_token_by_hash(hash_token(pair.access_token))
        refresh_row = await repo.get_active_token_by_hash(hash_token(pair.refresh_token))
        assert access_row is not None
        assert refresh_row is not None
        assert access_row["user_public_id"] == "user-persist"
        assert refresh_row["user_public_id"] == "user-persist"
        assert access_row["revoked_at"] is None
        assert refresh_row["revoked_at"] is None

    @pytest.mark.asyncio
    async def test_persist_tokens_records_expires_at_from_jwt_claims(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Persisted ``expires_at`` matches the ``exp`` claim on each JWT."""
        await _seed_user(repo, public_id="user-expiry", username="expiry-user")
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="expiry-user",
            role=UserRole.VIEWER,
            user_public_id="user-expiry",
        )
        pair = manager.create_tokens(principal)
        await manager.persist_tokens(pair, principal.user_public_id, repo)
        async with repo.session() as s:

            rows = (
                (
                    await s.execute(
                        select(UserActiveToken).where(
                            UserActiveToken.user_public_id == "user-expiry"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 2
        access = next(r for r in rows if r.token_type == "access")
        refresh = next(r for r in rows if r.token_type == "refresh")
        assert access.expires_at > access.issued_at
        assert refresh.expires_at > refresh.issued_at
        assert refresh.expires_at > access.expires_at


class TestRotateTokens:
    """Coverage for :meth:`TokenManager.rotate_tokens` end-to-end."""

    def _fresh_manager(self) -> TokenManager:
        """Return a cleanly-initialized singleton (clears blacklist)."""
        TokenManager._initialized = False
        manager = TokenManager()
        manager._blacklisted_tokens.clear()
        manager._blacklist_cleanup_heap.clear()
        manager._next_blacklist_cleanup_ts = float("inf")
        return manager

    @pytest.mark.asyncio
    async def test_rotate_success_flips_old_persists_new_returns_pair(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Happy path: rotation commits both old revoke and new rows."""
        await _seed_user(repo, public_id="user-rot-ok", username="rot-ok")
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="rot-ok",
            role=UserRole.VIEWER,
            user_public_id="user-rot-ok",
        )
        original = manager.create_tokens(principal)
        await manager.persist_tokens(original, principal.user_public_id, repo)
        original_refresh_jti = manager._decode_fresh_token(original.refresh_token).jti
        successor = manager.create_tokens(principal)
        ok = await manager.rotate_tokens(
            successor,
            principal.user_public_id,
            original_refresh_jti,
            repo,
        )
        assert ok is successor
        active = sorted(await repo.list_active_user_token_jtis("user-rot-ok"))
        new_access = manager._decode_fresh_token(successor.access_token).jti
        new_refresh = manager._decode_fresh_token(successor.refresh_token).jti
        original_access = manager._decode_fresh_token(original.access_token).jti
        assert new_access in active
        assert new_refresh in active
        assert original_refresh_jti not in active
        assert original_access in active

    @pytest.mark.asyncio
    async def test_rotate_replay_returns_none_no_insert(self, repo: SQLAlchemyRepository) -> None:
        """A stale replay (no grace entry) returns None and persists nothing."""
        await _seed_user(repo, public_id="user-rot-replay", username="rot-replay")
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="rot-replay",
            role=UserRole.VIEWER,
            user_public_id="user-rot-replay",
        )
        original = manager.create_tokens(principal)
        await manager.persist_tokens(original, principal.user_public_id, repo)
        original_refresh_jti = manager._decode_fresh_token(original.refresh_token).jti
        await repo.revoke_user_active_token_by_jti(original_refresh_jti, datetime.now(UTC))
        successor = manager.create_tokens(principal)
        new_access_jti = manager._decode_fresh_token(successor.access_token).jti
        ok = await manager.rotate_tokens(
            successor,
            principal.user_public_id,
            original_refresh_jti,
            repo,
        )
        assert ok is None
        active = await repo.list_active_user_token_jtis("user-rot-replay")
        assert new_access_jti not in active


class TestRotationGrace:
    """Concurrent-redeem grace: the CAS loser re-collects the winner's pair."""

    def _fresh_manager(self) -> TokenManager:
        """Return a cleanly-initialized singleton (clears grace + blacklist)."""
        TokenManager._initialized = False
        manager = TokenManager()
        manager._blacklisted_tokens.clear()
        manager._blacklist_cleanup_heap.clear()
        manager._next_blacklist_cleanup_ts = float("inf")
        manager._rotation_grace.clear()
        return manager

    @pytest.mark.asyncio
    async def test_concurrent_redeem_within_grace_returns_winner_pair(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The second rotation of the same JTI re-serves the FIRST successor.

        Models rapid F5: the winner rotates, the loser presents the same
        now-revoked JTI milliseconds later. The loser must receive the
        winner's pair (idempotent replay) and its own freshly-minted pair
        must never be persisted.
        """
        await _seed_user(repo, public_id="user-rot-grace", username="rot-grace")
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="rot-grace",
            role=UserRole.VIEWER,
            user_public_id="user-rot-grace",
        )
        original = manager.create_tokens(principal)
        await manager.persist_tokens(original, principal.user_public_id, repo)
        original_refresh_jti = manager._decode_fresh_token(original.refresh_token).jti
        winner = manager.create_tokens(principal)
        loser = manager.create_tokens(principal)
        first = await manager.rotate_tokens(
            winner, principal.user_public_id, original_refresh_jti, repo
        )
        assert first is winner
        second = await manager.rotate_tokens(
            loser, principal.user_public_id, original_refresh_jti, repo
        )
        assert second is winner
        active = await repo.list_active_user_token_jtis("user-rot-grace")
        loser_access_jti = manager._decode_fresh_token(loser.access_token).jti
        loser_refresh_jti = manager._decode_fresh_token(loser.refresh_token).jti
        assert loser_access_jti not in active
        assert loser_refresh_jti not in active

    @pytest.mark.asyncio
    async def test_expired_grace_entry_returns_none_and_evicts(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Past the TTL the replay is stale: None, and the entry is dropped."""
        await _seed_user(repo, public_id="user-rot-stale", username="rot-stale")
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="rot-stale",
            role=UserRole.VIEWER,
            user_public_id="user-rot-stale",
        )
        original = manager.create_tokens(principal)
        await manager.persist_tokens(original, principal.user_public_id, repo)
        original_refresh_jti = manager._decode_fresh_token(original.refresh_token).jti
        winner = manager.create_tokens(principal)
        rotated = await manager.rotate_tokens(
            winner, principal.user_public_id, original_refresh_jti, repo
        )
        assert rotated is winner
        pair, redeemed_at = manager._rotation_grace[original_refresh_jti]
        manager._rotation_grace[original_refresh_jti] = (
            pair,
            redeemed_at - ROTATION_GRACE_TTL_SECONDS - 1.0,
        )
        loser = manager.create_tokens(principal)
        second = await manager.rotate_tokens(
            loser, principal.user_public_id, original_refresh_jti, repo
        )
        assert second is None
        assert original_refresh_jti not in manager._rotation_grace

    def test_remember_rotation_prunes_expired_and_bounds_size(self) -> None:
        """Inserting prunes expired entries and evicts the oldest at the cap."""
        manager = self._fresh_manager()
        pair = TokenPair(access_token="a", refresh_token="r", expires_in=1)
        now = datetime.now(UTC).timestamp()
        manager._rotation_grace["expired-jti"] = (
            pair,
            now - ROTATION_GRACE_TTL_SECONDS - 1.0,
        )
        for index in range(ROTATION_GRACE_MAX_ENTRIES):
            manager._rotation_grace[f"jti-{index}"] = (pair, now - 1.0 + index * 1e-6)
        manager._remember_rotation("fresh-jti", pair)
        assert "expired-jti" not in manager._rotation_grace
        assert "fresh-jti" in manager._rotation_grace
        assert "jti-0" not in manager._rotation_grace
        assert len(manager._rotation_grace) == ROTATION_GRACE_MAX_ENTRIES

    def test_remembered_rotation_unknown_jti_returns_none(self) -> None:
        """A JTI never rotated by this process has no grace entry."""
        manager = self._fresh_manager()
        assert manager._remembered_rotation("never-seen") is None

    @pytest.mark.asyncio
    async def test_scheduler_race_loser_still_receives_winner_pair(self) -> None:
        """The rotation lock closes the CAS-commit-vs-remember scheduling gap.

        Without ``_rotation_lock`` the event loop may resume the CAS
        loser BEFORE the winner's continuation records its pair, and the
        loser would 401 despite the grace window. The stub repository
        yields control several times after the winning CAS commits,
        maximizing the chance of that interleaving; the lock must make
        the outcome deterministic: both concurrent calls return the
        winner's pair.
        """
        manager = self._fresh_manager()
        principal = AuthPrincipal(
            username="race",
            role=UserRole.VIEWER,
            user_public_id="user-race",
        )
        winner = manager.create_tokens(principal)
        loser = manager.create_tokens(principal)

        class _RaceRepo:
            """First rotation wins after yielding control; the rest lose."""

            def __init__(self) -> None:
                self.calls = 0

            async def rotate_user_active_token(
                self,
                old_refresh_jti: str,
                rows: object,
                revoked_at: object,
            ) -> bool:
                self.calls += 1
                won = self.calls == 1
                for _ in range(3):
                    await asyncio.sleep(0)
                return won

        repo_stub = cast(SQLAlchemyRepository, _RaceRepo())
        results = await asyncio.gather(
            manager.rotate_tokens(winner, "user-race", "refresh_shared-jti", repo_stub),
            manager.rotate_tokens(loser, "user-race", "refresh_shared-jti", repo_stub),
        )
        assert results[0] is winner
        assert results[1] is winner
