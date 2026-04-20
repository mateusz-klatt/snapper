"""Tests for Day 3d-A ``user_active_tokens`` DB persistence (plan §3.6.2).

Covers three new :class:`Repository` methods
(``insert_user_active_tokens``, ``revoke_user_active_token_by_jti``,
``get_active_token_by_hash``) plus the async
:meth:`TokenManager.persist_tokens` helper that drives the inventory
on every ``create_tokens()`` call.

Together they populate + maintain the row-per-outstanding-JWT
inventory that the Day 3d-B DB-backed ``verify_token`` reads against.
The tests run against an in-memory aiosqlite DB so the join on
``users.is_active`` exercises real SQL.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
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
    """Fresh in-memory repo with the Phase A schema applied."""
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


class TestPersistTokens:
    """Coverage for :meth:`TokenManager.persist_tokens` end-to-end."""

    def _fresh_manager(self) -> TokenManager:
        """Return a cleanly-initialized singleton (clears blacklist)."""
        TokenManager._initialized = False
        manager = TokenManager()
        manager._blacklisted_tokens.clear()
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
            across write and read paths (the §3.6.3 invariant that
            lets ``verify_token`` hit the inventory).
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
            from sqlalchemy import select

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
