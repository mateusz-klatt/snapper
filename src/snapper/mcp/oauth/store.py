"""Persistence primitives for the MCP OAuth authorization server."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.auth.tokens import hash_token
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.repository import Repository
from snapper.mcp.oauth.transaction import OAuthTransaction
from snapper.mcp.oauth.transaction import OAuthTransactionConflictError


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
        self, raw_request_id: str, *, code: OAuthAuthorizationCode, resolved_at: datetime
    ) -> OAuthAuthorizationRequest | None:
        """Resolve a consent and code together, rolling back a lost claim.

        Args:
            raw_request_id: Opaque request credential; only its hash is queried.
            code: Authorization code bound to the approved request and grant.
            resolved_at: UTC instant of the terminal consent decision.

        Returns:
            The claimed request, or None when its live linkage is invalid.
        """
        try:
            async with self.begin_write_transaction() as session:
                request = await self.approve_authorization_request_in_tx(
                    session, raw_request_id, code=code, resolved_at=resolved_at
                )
                if request is not None:
                    await session.commit()
                return request
        except OAuthTransactionConflictError:
            return None

    async def deny_authorization_request(
        self, raw_request_id: str, *, resolved_at: datetime
    ) -> OAuthAuthorizationRequest | None:
        """Resolve an active-client request once, with one outer commit.

        Args:
            raw_request_id: Opaque request credential; only its hash is queried.
            resolved_at: UTC instant of the terminal consent decision.

        Returns:
            The denied request, or None when it cannot be claimed.
        """
        try:
            async with self.begin_write_transaction() as session:
                tx = OAuthTransaction(session)
                request = await self._locked_request(tx, raw_request_id, resolved_at)
                if request is None:
                    return None
                await tx.resolve_request(request, "denied", resolved_at)
                await session.commit()
                return request
        except OAuthTransactionConflictError:
            return None

    async def consume_authorization_code(
        self,
        raw_code: str,
        *,
        client_id: str,
        consumed_at: datetime,
        refresh_token: OAuthRefreshToken | None = None,
    ) -> OAuthAuthorizationCode | None:
        """Consume a client-bound code and optional refresh row in one unit of work.

        Args:
            raw_code: Opaque one-time authorization code.
            client_id: Exact registered client identity.
            consumed_at: UTC instant of the one-time code claim.
            refresh_token: Optional initial hash-only refresh row to insert atomically.

        Returns:
            The consumed code, or None when it is unavailable.
        """
        try:
            async with self.begin_write_transaction() as session:
                code = await self.consume_authorization_code_in_tx(
                    session,
                    raw_code,
                    client_id=client_id,
                    consumed_at=consumed_at,
                    refresh_token=refresh_token,
                )
                if code is not None:
                    await session.commit()
                return code
        except OAuthTransactionConflictError:
            return None

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
        """Rotate once or commit revocation of the affected grant and all families.

        Args:
            raw_token: Opaque refresh credential; only its hash is queried.
            client_id: Exact registered client identity.
            successor: Hash-only successor retaining the grant and refresh family.
            rotated_at: UTC instant of rotation or confirmed replay revocation.

        Returns:
            The rotation, strict replay, or invalid-state outcome.
        """
        try:
            async with self.begin_write_transaction() as session:
                outcome = await self.rotate_refresh_token_in_tx(
                    session,
                    raw_token,
                    client_id=client_id,
                    successor=successor,
                    rotated_at=rotated_at,
                )
                if outcome != OAuthRefreshRotationOutcome.INVALID:
                    await session.commit()
                return outcome
        except OAuthTransactionConflictError:
            return await self._revoke_after_lost_rotation(
                raw_token, client_id=client_id, revoked_at=rotated_at
            )

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
        """Retire one grant and all of its refresh families atomically.

        Args:
            grant_public_id: Exact OAuth grant identity.
            revoked_at: UTC instant recorded on the grant and all attached families.

        Returns:
            True only when this transaction newly revokes the grant.
        """
        async with self.begin_write_transaction() as session:
            changed = await OAuthTransaction(session).revoke_grant(grant_public_id, revoked_at)
            await session.commit()
            return changed

    @staticmethod
    async def _select_refresh_token(
        session: AsyncSession,
        token_hash: str,
        client_id: str,
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
        result = await session.execute(statement)
        return result.scalar_one_or_none()

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
        self, raw_token: str, *, client_id: str, revoked_at: datetime
    ) -> OAuthRefreshRotationOutcome:
        """Classify only after the losing transaction has completely rolled back."""
        async with self.begin_write_transaction() as session:
            row = await self._locked_refresh(OAuthTransaction(session), raw_token, client_id)
            if row is None or row.revoked_at is not None or row.used_at is None:
                return OAuthRefreshRotationOutcome.INVALID
            await OAuthTransaction(session).revoke_grant(row.grant_public_id, revoked_at)
            await session.commit()
            return OAuthRefreshRotationOutcome.REUSE_DETECTED

    @asynccontextmanager
    async def begin_write_transaction(self) -> AsyncIterator[AsyncSession]:
        """Select isolation before decision reads; leave commit and rollback to the caller.

        Yields:
            The single write session; the caller owns its commit and rollback.
        """
        async with self._repository.session() as session:
            dialect = session.get_bind().dialect.name
            statement = (
                "BEGIN IMMEDIATE"
                if dialect == "sqlite"
                else "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            )
            await session.execute(text(statement))
            yield session

    @staticmethod
    async def _locked_request(
        tx: OAuthTransaction, raw_request_id: str, now: datetime
    ) -> OAuthAuthorizationRequest | None:
        """Read identity first, then lock the active client before the request."""
        identity = await tx.request(raw_request_id, now, lock=False)
        if identity is None or await tx.client(identity.client_id) is None:
            return None
        return await tx.request(raw_request_id, now)

    async def approve_authorization_request_in_tx(
        self,
        session: AsyncSession,
        raw_request_id: str,
        *,
        code: OAuthAuthorizationCode,
        resolved_at: datetime,
    ) -> OAuthAuthorizationRequest | None:
        """Validate and claim consent/code linkage without committing or rolling back.

        Args:
            session: Caller-owned transaction with isolation selected before decision reads.
            raw_request_id: Opaque request credential; only its hash is queried.
            code: Authorization code bound to the approved request and grant.
            resolved_at: UTC instant of the terminal consent decision.

        Returns:
            The claimed request; None leaves invalid linkage unconsumed.
        """
        tx = OAuthTransaction(session)
        request = await self._locked_request(tx, raw_request_id, resolved_at)
        if request is None:
            return None
        grant = await tx.grant(code.grant_public_id, request.client_id)
        if grant is None or grant.resource != request.resource:
            return None
        binding = (
            code.redirect_uri == request.redirect_uri
            and code.redirect_uri_provided_explicitly == request.redirect_uri_provided_explicitly
            and code.code_challenge == request.code_challenge
            and code.code_challenge_method == "S256"
            and set(code.scopes) == set(request.scopes)
            and set(code.scopes) <= set(grant.scopes)
        )
        if not binding:
            return None
        await tx.resolve_request(request, "approved", resolved_at)
        session.add(code)
        return request

    async def consume_authorization_code_in_tx(
        self,
        session: AsyncSession,
        raw_code: str,
        *,
        client_id: str,
        consumed_at: datetime,
        refresh_token: OAuthRefreshToken | None = None,
    ) -> OAuthAuthorizationCode | None:
        """Consume code and initial refresh row in the supplied unit of work.

        Args:
            session: Caller-owned transaction with isolation selected before decision reads.
            raw_code: Opaque one-time authorization code.
            client_id: Exact registered client identity.
            consumed_at: UTC instant of the one-time code claim.
            refresh_token: Optional initial hash-only refresh row to insert atomically.

        Returns:
            The consumed code, or None without consuming an invalid code.
        """
        tx = OAuthTransaction(session)
        if await tx.client(client_id) is None:
            return None
        identity = await tx.code(raw_code, lock=False)
        if identity is None:
            return None
        grant = await tx.grant(identity.grant_public_id, client_id)
        code = await tx.code(raw_code)
        if grant is None or code is None or code.consumed_at is not None:
            return None
        if code.expires_at <= consumed_at:
            return None
        if refresh_token is not None and (
            refresh_token.grant_public_id != grant.public_id
            or not set(refresh_token.scopes) <= set(code.scopes)
        ):
            raise ValueError("OAuth refresh token violates code binding")
        await tx.consume_code(code, consumed_at)
        if refresh_token is not None:
            session.add(refresh_token)
        return code

    @staticmethod
    async def _locked_refresh(
        tx: OAuthTransaction, raw_token: str, client_id: str
    ) -> OAuthRefreshToken | None:
        """Lock client then grant then refresh after reading only credential identity."""
        if await tx.client(client_id) is None:
            return None
        identity = await tx.refresh(raw_token, lock=False)
        if identity is None or await tx.grant(identity.grant_public_id, client_id) is None:
            return None
        return await tx.refresh(raw_token)

    async def rotate_refresh_token_in_tx(
        self,
        session: AsyncSession,
        raw_token: str,
        *,
        client_id: str,
        successor: OAuthRefreshToken,
        rotated_at: datetime,
    ) -> OAuthRefreshRotationOutcome:
        """Rotate or revoke the entire affected grant inside the caller's transaction.

        Args:
            session: Caller-owned transaction with isolation selected before decision reads.
            raw_token: Opaque refresh credential; only its hash is queried.
            client_id: Exact registered client identity.
            successor: Hash-only successor retaining the grant and refresh family.
            rotated_at: UTC instant of rotation or confirmed replay revocation.

        Returns:
            The rotation or revocation outcome, pending the caller commit.
        """
        tx = OAuthTransaction(session)
        predecessor = await self._locked_refresh(tx, raw_token, client_id)
        if predecessor is None or predecessor.revoked_at is not None:
            return OAuthRefreshRotationOutcome.INVALID
        if predecessor.used_at is not None:
            await tx.revoke_grant(predecessor.grant_public_id, rotated_at)
            return OAuthRefreshRotationOutcome.REUSE_DETECTED
        if predecessor.expires_at <= rotated_at:
            return OAuthRefreshRotationOutcome.INVALID
        await tx.rotate_refresh(predecessor, successor, rotated_at)
        return OAuthRefreshRotationOutcome.ROTATED
