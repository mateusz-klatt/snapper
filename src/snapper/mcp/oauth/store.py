"""Persistence primitives for the MCP OAuth authorization server."""

from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.auth.tokens import hash_token
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.repository import Repository


class OAuthRefreshRotationOutcome(StrEnum):
    """Result of one atomic opaque refresh-token rotation attempt."""

    ROTATED = "rotated"
    REUSE_DETECTED = "reuse_detected"
    INVALID = "invalid"


class MCPOAuthStore:
    """Own transactional persistence for MCP OAuth protocol artifacts."""

    def __init__(self, repository: Repository) -> None:
        """Initialize with the application's shared repository.

        Args:
            repository: Repository providing transactional async sessions.
        """
        self._repository = repository

    async def add_client(self, client: OAuthClient) -> None:
        """Persist one pre-registered OAuth client.

        Args:
            client: Fully validated client row with a hashed secret.
        """
        async with self._repository.session() as session:
            session.add(client)
            await session.commit()

    async def get_client(self, client_id: str) -> OAuthClient | None:
        """Return an active OAuth client by exact identifier.

        Args:
            client_id: Exact pre-registered client identifier.

        Returns:
            Active client row, otherwise ``None``.
        """
        async with self._repository.session() as session:
            result = await session.execute(
                select(OAuthClient).where(
                    OAuthClient.client_id == client_id,
                    OAuthClient.is_active.is_(True),
                )
            )
            return result.scalar_one_or_none()

    async def add_grant(self, grant: OAuthGrant) -> None:
        """Persist one active resource-owner grant.

        Args:
            grant: Grant bound to owner, operator, delegate, and client.
        """
        async with self._repository.session() as session:
            session.add(grant)
            await session.commit()

    async def get_active_grant(self, grant_public_id: str) -> OAuthGrant | None:
        """Return one unrevoked OAuth grant.

        Args:
            grant_public_id: Stable public grant identity.

        Returns:
            Active grant row, otherwise ``None``.
        """
        async with self._repository.session() as session:
            result = await session.execute(
                select(OAuthGrant).where(
                    OAuthGrant.public_id == grant_public_id,
                    OAuthGrant.revoked_at.is_(None),
                )
            )
            return result.scalar_one_or_none()

    async def add_authorization_code(self, code: OAuthAuthorizationCode) -> None:
        """Persist one hashed, short-lived authorization code.

        Args:
            code: Authorization-code row whose raw code is not retained.
        """
        async with self._repository.session() as session:
            session.add(code)
            await session.commit()

    async def add_authorization_request(
        self,
        authorization_request: OAuthAuthorizationRequest,
    ) -> None:
        """Persist one hashed browser authorization request.

        Args:
            authorization_request: Pending request with no raw request ID.
        """
        async with self._repository.session() as session:
            session.add(authorization_request)
            await session.commit()

    async def get_pending_authorization_request(
        self,
        raw_request_id: str,
        *,
        now: datetime,
    ) -> OAuthAuthorizationRequest | None:
        """Return one unresolved, unexpired request for an active client.

        Args:
            raw_request_id: Opaque browser request identifier.
            now: UTC validity boundary.

        Returns:
            Pending request row, otherwise ``None``.
        """
        async with self._repository.session() as session:
            result = await session.execute(
                select(OAuthAuthorizationRequest)
                .join(
                    OAuthClient,
                    OAuthClient.client_id == OAuthAuthorizationRequest.client_id,
                )
                .where(
                    OAuthAuthorizationRequest.request_hash == hash_token(raw_request_id),
                    OAuthAuthorizationRequest.decision.is_(None),
                    OAuthAuthorizationRequest.expires_at > now,
                    OAuthClient.is_active.is_(True),
                )
            )
            return result.scalar_one_or_none()

    async def approve_authorization_request(
        self,
        raw_request_id: str,
        *,
        code: OAuthAuthorizationCode,
        resolved_at: datetime,
    ) -> OAuthAuthorizationRequest | None:
        """Atomically approve one pending request and persist its code.

        Args:
            raw_request_id: Opaque browser request identifier.
            code: Hashed authorization code bound to the approved grant.
            resolved_at: UTC consent boundary.

        Returns:
            Approved request row on first resolution, otherwise ``None``.
        """
        async with self._repository.session() as session:
            request = await self._resolve_authorization_request(
                session,
                raw_request_id,
                decision="approved",
                resolved_at=resolved_at,
            )
            if request is None:
                return None
            session.add(code)
            await session.commit()
            return request

    async def deny_authorization_request(
        self,
        raw_request_id: str,
        *,
        resolved_at: datetime,
    ) -> OAuthAuthorizationRequest | None:
        """Atomically deny one pending request.

        Args:
            raw_request_id: Opaque browser request identifier.
            resolved_at: UTC consent boundary.

        Returns:
            Denied request row on first resolution, otherwise ``None``.
        """
        async with self._repository.session() as session:
            request = await self._resolve_authorization_request(
                session,
                raw_request_id,
                decision="denied",
                resolved_at=resolved_at,
            )
            if request is None:
                return None
            await session.commit()
            return request

    async def consume_authorization_code(
        self,
        raw_code: str,
        *,
        client_id: str,
        consumed_at: datetime,
        refresh_token: OAuthRefreshToken | None = None,
    ) -> OAuthAuthorizationCode | None:
        """Atomically consume one valid authorization code.

        Args:
            raw_code: Raw credential presented once at the token endpoint.
            client_id: Client attempting the exchange.
            consumed_at: UTC transaction boundary time.
            refresh_token: Optional first refresh-family row persisted
                atomically with code consumption.

        Returns:
            Consumed code row on first valid exchange, otherwise ``None``.
        """
        async with self._repository.session() as session:
            result = await session.execute(
                select(OAuthAuthorizationCode)
                .join(
                    OAuthGrant,
                    OAuthGrant.public_id == OAuthAuthorizationCode.grant_public_id,
                )
                .where(
                    OAuthAuthorizationCode.code_hash == hash_token(raw_code),
                    OAuthAuthorizationCode.consumed_at.is_(None),
                    OAuthAuthorizationCode.expires_at > consumed_at,
                    OAuthGrant.client_id == client_id,
                    OAuthGrant.revoked_at.is_(None),
                )
                .with_for_update()
            )
            code = result.scalar_one_or_none()
            if code is None:
                return None
            claim: Any = await session.execute(
                update(OAuthAuthorizationCode)
                .where(
                    OAuthAuthorizationCode.id == code.id,
                    OAuthAuthorizationCode.consumed_at.is_(None),
                )
                .values(consumed_at=consumed_at)
            )
            if int(claim.rowcount or 0) != 1:
                await session.rollback()
                return None
            if refresh_token is not None:
                session.add(refresh_token)
            await session.commit()
            code.consumed_at = consumed_at
            return code

    async def load_authorization_code(
        self,
        raw_code: str,
        *,
        client_id: str,
    ) -> tuple[OAuthAuthorizationCode, OAuthGrant] | None:
        """Load one unconsumed code and its active client-bound grant.

        Args:
            raw_code: Raw authorization code presented to the token endpoint.
            client_id: Exact client attempting exchange.

        Returns:
            Code and active grant rows, otherwise ``None``. Expiry remains
            visible so the SDK token handler can classify it explicitly.
        """
        async with self._repository.session() as session:
            result = await session.execute(
                select(OAuthAuthorizationCode, OAuthGrant)
                .join(
                    OAuthGrant,
                    OAuthGrant.public_id == OAuthAuthorizationCode.grant_public_id,
                )
                .where(
                    OAuthAuthorizationCode.code_hash == hash_token(raw_code),
                    OAuthAuthorizationCode.consumed_at.is_(None),
                    OAuthGrant.client_id == client_id,
                    OAuthGrant.revoked_at.is_(None),
                )
            )
            row = result.first()
            if row is None:
                return None
            return row[0], row[1]

    async def add_refresh_token(self, refresh_token: OAuthRefreshToken) -> None:
        """Persist one opaque refresh-token hash.

        Args:
            refresh_token: Token row with no raw credential material.
        """
        async with self._repository.session() as session:
            session.add(refresh_token)
            await session.commit()

    async def load_refresh_token(
        self,
        raw_token: str,
        *,
        client_id: str,
        now: datetime,
    ) -> OAuthRefreshToken | None:
        """Load one unrevoked, unexpired refresh token.

        Args:
            raw_token: Opaque credential presented by the client.
            client_id: Exact client attempting the exchange.
            now: UTC validity boundary.

        Returns:
            Token row, including a used predecessor for replay detection,
            otherwise ``None``.
        """
        async with self._repository.session() as session:
            row = await self._select_refresh_token(
                session,
                hash_token(raw_token),
                client_id,
            )
            if row is None or row.revoked_at is not None or row.expires_at <= now:
                return None
            return row

    async def rotate_refresh_token(
        self,
        raw_token: str,
        *,
        client_id: str,
        successor: OAuthRefreshToken,
        rotated_at: datetime,
    ) -> OAuthRefreshRotationOutcome:
        """Rotate a refresh token or revoke its family on reuse.

        Args:
            raw_token: Opaque predecessor credential.
            client_id: Exact client attempting rotation.
            successor: Fresh hashed token row for successful rotation.
            rotated_at: UTC transaction boundary time.

        Returns:
            Rotation, replay-detection, or invalid-token outcome.

        Raises:
            ValueError: If successor identity or scopes violate the family.
        """
        async with self._repository.session() as session:
            predecessor = await self._select_refresh_token(
                session,
                hash_token(raw_token),
                client_id,
                lock=True,
            )
            if predecessor is None or predecessor.revoked_at is not None:
                return OAuthRefreshRotationOutcome.INVALID
            if predecessor.used_at is not None:
                await self._revoke_family(session, predecessor.family_public_id, rotated_at)
                await session.commit()
                return OAuthRefreshRotationOutcome.REUSE_DETECTED
            if predecessor.expires_at <= rotated_at:
                return OAuthRefreshRotationOutcome.INVALID
            self._validate_successor(predecessor, successor)
            claim: Any = await session.execute(
                update(OAuthRefreshToken)
                .where(
                    OAuthRefreshToken.id == predecessor.id,
                    OAuthRefreshToken.used_at.is_(None),
                    OAuthRefreshToken.revoked_at.is_(None),
                )
                .values(
                    used_at=rotated_at,
                    replaced_by_public_id=successor.public_id,
                )
            )
            if int(claim.rowcount or 0) != 1:
                await session.rollback()
                return await self._revoke_after_lost_rotation(
                    raw_token,
                    client_id=client_id,
                    revoked_at=rotated_at,
                )
            session.add(successor)
            await session.commit()
            return OAuthRefreshRotationOutcome.ROTATED

    async def revoke_refresh_family(self, family_public_id: str, revoked_at: datetime) -> int:
        """Revoke every live refresh token in a family.

        Args:
            family_public_id: Stable refresh-family identity.
            revoked_at: UTC revocation boundary.

        Returns:
            Number of newly revoked rows.
        """
        async with self._repository.session() as session:
            count = await self._revoke_family(session, family_public_id, revoked_at)
            await session.commit()
            return count

    async def revoke_grant(self, grant_public_id: str, revoked_at: datetime) -> bool:
        """Revoke one grant and every live refresh token it issued.

        Args:
            grant_public_id: Stable grant identity.
            revoked_at: UTC revocation boundary.

        Returns:
            ``True`` when an active grant transitioned, otherwise ``False``.
        """
        async with self._repository.session() as session:
            result: Any = await session.execute(
                update(OAuthGrant)
                .where(
                    OAuthGrant.public_id == grant_public_id,
                    OAuthGrant.revoked_at.is_(None),
                )
                .values(revoked_at=revoked_at)
            )
            changed = int(result.rowcount or 0) == 1
            await session.execute(
                update(OAuthRefreshToken)
                .where(
                    OAuthRefreshToken.grant_public_id == grant_public_id,
                    OAuthRefreshToken.revoked_at.is_(None),
                )
                .values(revoked_at=revoked_at)
            )
            await session.commit()
            return changed

    @staticmethod
    async def _resolve_authorization_request(
        session: AsyncSession,
        raw_request_id: str,
        *,
        decision: str,
        resolved_at: datetime,
    ) -> OAuthAuthorizationRequest | None:
        """Claim one pending request inside the caller's transaction."""
        result = await session.execute(
            select(OAuthAuthorizationRequest)
            .where(
                OAuthAuthorizationRequest.request_hash == hash_token(raw_request_id),
                OAuthAuthorizationRequest.decision.is_(None),
                OAuthAuthorizationRequest.expires_at > resolved_at,
            )
            .with_for_update()
        )
        request = result.scalar_one_or_none()
        if request is None:
            return None
        claim: Any = await session.execute(
            update(OAuthAuthorizationRequest)
            .where(
                OAuthAuthorizationRequest.id == request.id,
                OAuthAuthorizationRequest.decision.is_(None),
            )
            .values(decision=decision, resolved_at=resolved_at)
        )
        if int(claim.rowcount or 0) != 1:
            await session.rollback()
            return None
        request.decision = decision
        request.resolved_at = resolved_at
        return request

    @staticmethod
    async def _select_refresh_token(
        session: AsyncSession,
        token_hash: str,
        client_id: str,
        *,
        lock: bool = False,
    ) -> OAuthRefreshToken | None:
        """Load one token joined to its active client-bound grant."""
        statement = (
            select(OAuthRefreshToken)
            .join(OAuthGrant, OAuthGrant.public_id == OAuthRefreshToken.grant_public_id)
            .where(
                OAuthRefreshToken.token_hash == token_hash,
                OAuthGrant.client_id == client_id,
                OAuthGrant.revoked_at.is_(None),
            )
        )
        if lock:
            statement = statement.with_for_update()
        result = await session.execute(statement)
        return result.scalar_one_or_none()

    @staticmethod
    def _validate_successor(
        predecessor: OAuthRefreshToken,
        successor: OAuthRefreshToken,
    ) -> None:
        """Require one family, grant, and non-widening scope across rotation."""
        same_identity = (
            successor.family_public_id == predecessor.family_public_id
            and successor.grant_public_id == predecessor.grant_public_id
        )
        scope_narrows = set(successor.scopes) <= set(predecessor.scopes)
        if not same_identity or not scope_narrows:
            raise ValueError("OAuth refresh successor violates family or scope")

    @staticmethod
    async def _revoke_family(
        session: AsyncSession,
        family_public_id: str,
        revoked_at: datetime,
    ) -> int:
        """Revoke one family inside the caller's transaction."""
        result: Any = await session.execute(
            update(OAuthRefreshToken)
            .where(
                OAuthRefreshToken.family_public_id == family_public_id,
                OAuthRefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=revoked_at)
        )
        return int(result.rowcount or 0)

    async def _revoke_after_lost_rotation(
        self,
        raw_token: str,
        *,
        client_id: str,
        revoked_at: datetime,
    ) -> OAuthRefreshRotationOutcome:
        """Resolve a lost CAS as reuse when another writer consumed it."""
        async with self._repository.session() as session:
            row = await self._select_refresh_token(
                session,
                hash_token(raw_token),
                client_id,
                lock=True,
            )
            if row is None or row.used_at is None:
                return OAuthRefreshRotationOutcome.INVALID
            await self._revoke_family(session, row.family_public_id, revoked_at)
            await session.commit()
            return OAuthRefreshRotationOutcome.REUSE_DETECTED
