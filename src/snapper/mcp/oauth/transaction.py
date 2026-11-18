"""Session-bound OAuth persistence with no transaction ownership or credentials."""

from datetime import UTC
from datetime import datetime
from typing import cast

from sqlalchemy import CursorResult
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.auth.tokens import hash_token
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.models import User
from snapper.data.repository import where_active_now


class OAuthTransactionConflictError(RuntimeError):
    """Require the caller to roll back a lost claim before fresh classification."""


class OAuthTransaction:
    """Use only the supplied session; the caller owns commit and rollback."""

    def __init__(self, session: AsyncSession) -> None:
        """Bind operations to one caller-owned transaction."""
        self.session = session

    async def client(
        self, client_id: str, *, lock: bool = True, active_only: bool = True
    ) -> OAuthClient | None:
        """Load an active client, refreshing the ORM identity after its lock.

        Args:
            client_id: Exact registered client identity.
            lock: Whether to acquire a row lock in the caller transaction.
            active_only: Whether inactive clients must be excluded.

        Returns:
            The registered client, or None when it does not match the active-state policy.
        """
        statement = select(OAuthClient).where(OAuthClient.client_id == client_id)
        if active_only:
            statement = statement.where(OAuthClient.is_active.is_(True))
        if lock:
            statement = statement.with_for_update()
        result = await self.session.execute(statement.execution_options(populate_existing=True))
        return result.scalar_one_or_none()

    async def request(
        self, raw_request_id: str, now: datetime, *, lock: bool = True
    ) -> OAuthAuthorizationRequest | None:
        """Load one pending, unexpired request without resolving it.

        Args:
            raw_request_id: Opaque request credential; only its hash is queried.
            now: UTC decision instant used for expiry and mutation checks.
            lock: Whether to acquire a row lock in the caller transaction.

        Returns:
            The unresolved, unexpired request, or None when it is unavailable.
        """
        statement = select(OAuthAuthorizationRequest).where(
            OAuthAuthorizationRequest.request_hash == hash_token(raw_request_id),
            OAuthAuthorizationRequest.decision.is_(None),
            OAuthAuthorizationRequest.expires_at > now,
        )
        if lock:
            statement = statement.with_for_update()
        result = await self.session.execute(statement.execution_options(populate_existing=True))
        return result.scalar_one_or_none()

    async def grant(
        self, grant_public_id: str, client_id: str, *, lock: bool = True
    ) -> OAuthGrant | None:
        """Load an active grant bound to the exact authenticated client.

        Args:
            grant_public_id: Exact OAuth grant identity.
            client_id: Exact registered client identity.
            lock: Whether to acquire a row lock in the caller transaction.

        Returns:
            The active grant for this exact client, or None.
        """
        if lock:
            identity = await self.grant(grant_public_id, client_id, lock=False)
            if identity is None:
                return None
            await self.lock_owner_identity(identity.owner_user_public_id)
        statement = select(OAuthGrant).where(
            OAuthGrant.public_id == grant_public_id,
            OAuthGrant.client_id == client_id,
            OAuthGrant.revoked_at.is_(None),
        )
        if lock:
            statement = statement.with_for_update()
        result = await self.session.execute(statement.execution_options(populate_existing=True))
        return result.scalar_one_or_none()

    async def lock_owner_identity(self, owner_public_id: str) -> None:
        """Serialize binding writers before any grant lock or absent-binding read.

        Authority decisions additionally require the live owner validator. These
        low-level persistence primitives also support historical fixture rows
        whose owner identity has not been materialized.

        Args:
            owner_public_id: Existing owner identity used to serialize binding writers.
        """
        result = await self.session.execute(
            select(User)
            .where(User.public_id == owner_public_id, *where_active_now(User))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        row = result.scalar_one_or_none()
        if row is not None and row.known_to <= datetime.now(UTC):
            raise OAuthTransactionConflictError("OAuth owner version closed during its lock wait")

    async def code(self, raw_code: str, *, lock: bool = True) -> OAuthAuthorizationCode | None:
        """Load an opaque code identity; callers validate grant and expiry.

        Args:
            raw_code: Opaque one-time authorization code.
            lock: Whether to acquire a row lock in the caller transaction.

        Returns:
            The persisted code including its expiry and consumed state, or None.
        """
        statement = select(OAuthAuthorizationCode).where(
            OAuthAuthorizationCode.code_hash == hash_token(raw_code)
        )
        if lock:
            statement = statement.with_for_update()
        result = await self.session.execute(statement.execution_options(populate_existing=True))
        return result.scalar_one_or_none()

    async def refresh(self, raw_token: str, *, lock: bool = True) -> OAuthRefreshToken | None:
        """Load a refresh identity, keeping used state visible for replay checks.

        Args:
            raw_token: Opaque refresh credential; only its hash is queried.
            lock: Whether to acquire a row lock in the caller transaction.

        Returns:
            The persisted refresh token including used and revoked state, or None.
        """
        statement = select(OAuthRefreshToken).where(
            OAuthRefreshToken.token_hash == hash_token(raw_token)
        )
        if lock:
            statement = statement.with_for_update()
        result = await self.session.execute(statement.execution_options(populate_existing=True))
        return result.scalar_one_or_none()

    async def resolve_request(
        self, request: OAuthAuthorizationRequest, decision: str, now: datetime
    ) -> None:
        """Claim an unresolved request or force whole-transaction rollback.

        Args:
            request: Pending authorization request already loaded in this transaction.
            decision: Terminal consent decision to claim exactly once.
            now: UTC decision instant used for expiry and mutation checks.

        Raises:
            OAuthTransactionConflictError: If the compare-and-swap claim lost; roll back the whole unit of work.
        """
        result = await self.session.execute(
            update(OAuthAuthorizationRequest)
            .where(
                OAuthAuthorizationRequest.id == request.id,
                OAuthAuthorizationRequest.decision.is_(None),
                OAuthAuthorizationRequest.expires_at > now,
            )
            .values(decision=decision, resolved_at=now)
        )
        if cast(CursorResult[tuple[object, ...]], result).rowcount != 1:
            raise OAuthTransactionConflictError("OAuth request claim lost")
        request.decision = decision
        request.resolved_at = now

    async def consume_code(self, code: OAuthAuthorizationCode, now: datetime) -> None:
        """Consume one code without committing or exposing credentials.

        Args:
            code: Authorization code bound to the approved request and grant.
            now: UTC decision instant used for expiry and mutation checks.

        Raises:
            OAuthTransactionConflictError: If the compare-and-swap claim lost; roll back the whole unit of work.
        """
        result = await self.session.execute(
            update(OAuthAuthorizationCode)
            .where(
                OAuthAuthorizationCode.id == code.id,
                OAuthAuthorizationCode.consumed_at.is_(None),
                OAuthAuthorizationCode.expires_at > now,
            )
            .values(consumed_at=now)
        )
        if cast(CursorResult[tuple[object, ...]], result).rowcount != 1:
            raise OAuthTransactionConflictError("OAuth code claim lost")
        code.consumed_at = now

    async def rotate_refresh(
        self, predecessor: OAuthRefreshToken, successor: OAuthRefreshToken | None, now: datetime
    ) -> None:
        """Claim one unused token and add its optional successor in the caller's unit of work.

        A ``None`` successor is the terminal access-only exchange: the predecessor is
        consumed exactly like a rotation, so a later reuse is still detected as replay.

        Args:
            predecessor: Previously issued refresh token whose unused state is claimed.
            successor: Hash-only successor retaining the grant and refresh family, or
                None when the exchange no longer carries ``offline_access``.
            now: UTC decision instant used for expiry and mutation checks.

        Raises:
            OAuthTransactionConflictError: If the compare-and-swap claim lost; roll back the whole unit of work.
            ValueError: If a successor leaves the grant or family or widens scopes.
        """
        if successor is not None:
            same_identity = (
                successor.grant_public_id == predecessor.grant_public_id
                and successor.family_public_id == predecessor.family_public_id
            )
            if not same_identity or not set(successor.scopes) <= set(predecessor.scopes):
                raise ValueError("OAuth refresh successor violates family or scope")
        replacement = successor.public_id if successor is not None else None
        result = await self.session.execute(
            update(OAuthRefreshToken)
            .where(
                OAuthRefreshToken.id == predecessor.id,
                OAuthRefreshToken.used_at.is_(None),
                OAuthRefreshToken.revoked_at.is_(None),
                OAuthRefreshToken.expires_at > now,
            )
            .values(used_at=now, replaced_by_public_id=replacement)
        )
        if cast(CursorResult[tuple[object, ...]], result).rowcount != 1:
            raise OAuthTransactionConflictError("OAuth refresh claim lost")
        if successor is not None:
            self.session.add(successor)

    async def revoke_grant(self, grant_public_id: str, now: datetime) -> bool:
        """Revoke the affected grant and all its families in this same transaction.

        Args:
            grant_public_id: Exact OAuth grant identity.
            now: UTC decision instant used for expiry and mutation checks.

        Returns:
            True only when this transaction newly revokes the grant.
        """
        identity = (
            await self.session.execute(
                select(OAuthGrant).where(OAuthGrant.public_id == grant_public_id)
            )
        ).scalar_one_or_none()
        if identity is None:
            return False
        await self.client(identity.client_id, active_only=False)
        await self.lock_owner_identity(identity.owner_user_public_id)
        await self.session.execute(
            select(OAuthGrant)
            .where(OAuthGrant.public_id == grant_public_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        result = await self.session.execute(
            update(OAuthGrant)
            .where(OAuthGrant.public_id == grant_public_id, OAuthGrant.revoked_at.is_(None))
            .values(revoked_at=now)
        )
        await self.session.execute(
            update(OAuthRefreshToken)
            .where(
                OAuthRefreshToken.grant_public_id == grant_public_id,
                OAuthRefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )
        return cast(CursorResult[tuple[object, ...]], result).rowcount == 1
