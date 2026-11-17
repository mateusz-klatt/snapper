"""Dormant typed MCP OAuth transactions; no HTTP routes or runtime activation."""

import asyncio
import base64
import hashlib
import hmac
import re
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

import bcrypt
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.ai_delegates.oauth_authority import lock_oauth_operator
from snapper.application.ai_delegates.oauth_authority import lock_oauth_owner
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import OAuthDelegateIdentity
from snapper.application.ai_delegates.service import OAuthDelegateRequest
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.schemas.tokens import MCPOAuthAccessTokenClaims
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.tokens import MCPOAuthAccessTokenRequest
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.repository import Repository
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.contracts import MCPOAuthServiceConfig
from snapper.mcp.oauth.contracts import OAuthApprovedAuthorization
from snapper.mcp.oauth.contracts import OAuthAuthorizationRequestCreate
from snapper.mcp.oauth.contracts import OAuthCodeExchangeCommand
from snapper.mcp.oauth.contracts import OAuthConsentApprovalCommand
from snapper.mcp.oauth.contracts import OAuthConsentDenialCommand
from snapper.mcp.oauth.contracts import OAuthPendingAuthorization
from snapper.mcp.oauth.contracts import OAuthRefreshExchangeCommand
from snapper.mcp.oauth.contracts import OAuthRefreshReuseDetectedError
from snapper.mcp.oauth.contracts import OAuthTokenExchangeResult
from snapper.mcp.oauth.contracts import OAuthVerifiedAccess
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import permissions_for_oauth_scopes
from snapper.mcp.oauth.scopes import validate_initial_oauth_scopes
from snapper.mcp.oauth.store import MCPOAuthStore
from snapper.mcp.oauth.transaction import OAuthTransaction
from snapper.mcp.oauth.transaction import OAuthTransactionConflictError

_PKCE_CHALLENGE = re.compile(r"[A-Za-z0-9_-]{43}\Z")
_PKCE_VERIFIER = re.compile(r"[A-Za-z0-9._~-]{43,128}\Z")
_BINDING_CONFLICT = (
    "UNIQUE constraint failed: oauth_grants.owner_user_public_id, "
    "oauth_grants.operator_public_id, oauth_grants.client_id"
)


def _binding_conflict(error: IntegrityError) -> bool:
    """Retry only the known active-binding uniqueness conflict after whole-TX rollback."""
    original = error.driver_exception
    diagnostic = getattr(original, "diag", None)
    constraint = getattr(original, "constraint_name", getattr(diagnostic, "constraint_name", None))
    postgres = (
        getattr(original, "sqlstate", None) == "23505"
        and constraint == "uq_oauth_grants_active_binding"
    )
    sqlite = (
        getattr(original, "sqlite_errorcode", None) == 2067 and str(original) == _BINDING_CONFLICT
    )
    return postgres or sqlite


class MCPOAuthService:
    """Own each OAuth unit of work and publish credentials only after its commit."""

    def __init__(
        self,
        *,
        repository: Repository,
        token_manager: TokenManager,
        config: MCPOAuthServiceConfig,
        delegate_service: DelegateService,
    ) -> None:
        """Bind existing persistence, token cryptography, and delegate authority services."""
        self.repository = repository
        self.token_manager = token_manager
        self.config = config
        self.delegate_service = delegate_service
        self.store = MCPOAuthStore(repository)

    def _resource(self, resource: str, grant: OAuthGrant | None = None) -> None:
        """Require an exact protected-resource identity before any consuming mutation."""
        if resource != self.config.resource or (grant is not None and grant.resource != resource):
            raise MCPOAuthError("invalid_resource")

    @staticmethod
    def _scopes(scopes: tuple[str, ...], client: OAuthClient) -> tuple[str, ...]:
        """Apply the closed first-release and currently registered client ceilings."""
        try:
            return validate_initial_oauth_scopes(
                scopes, allowed_client_scopes=client.allowed_scopes
            )
        except ValueError as error:
            raise MCPOAuthError("invalid_scope") from error

    @staticmethod
    async def _client(
        tx: OAuthTransaction, client_id: str, secret: str, method: str
    ) -> OAuthClient:
        """Lock and authenticate the exact active confidential client without storing its secret."""
        client = await tx.client(client_id)
        if client is None or method != client.token_endpoint_auth_method:
            raise MCPOAuthError("invalid_client")
        try:
            accepted = await asyncio.to_thread(
                bcrypt.checkpw, secret.encode("utf-8"), client.client_secret_hash.encode("ascii")
            )
        except (ValueError, UnicodeError) as error:
            raise MCPOAuthError("invalid_client") from error
        if not accepted:
            raise MCPOAuthError("invalid_client")
        return client

    @staticmethod
    def _redirect(client: OAuthClient, command: OAuthAuthorizationRequestCreate) -> str:
        """Resolve only an exact registered redirect, preserving whether it was supplied."""
        if command.redirect_uri_provided_explicitly != (command.redirect_uri is not None):
            raise MCPOAuthError("invalid_redirect_uri")
        redirect = command.redirect_uri
        if redirect is None:
            if len(client.redirect_uris) != 1:
                raise MCPOAuthError("invalid_redirect_uri")
            redirect = client.redirect_uris[0]
        if redirect not in client.redirect_uris:
            raise MCPOAuthError("invalid_redirect_uri")
        return redirect

    async def create_authorization_request(
        self, command: OAuthAuthorizationRequestCreate
    ) -> OAuthPendingAuthorization:
        """Persist one exact-client, resource-bound, S256 authorization request.

        Args:
            command: Exact client, resource, redirect, scopes and S256 challenge.

        Returns:
            The opaque pending request credential after commit.

        Raises:
            MCPOAuthError: If protocol identity, state or current authority is invalid.
        """
        self._resource(command.resource)
        if command.code_challenge_method != "S256" or not _PKCE_CHALLENGE.fullmatch(
            command.code_challenge
        ):
            raise MCPOAuthError("invalid_pkce")
        async with self.store.begin_write_transaction() as session:
            client = await OAuthTransaction(session).client(command.client_id)
            if client is None:
                raise MCPOAuthError("invalid_client")
            scopes = self._scopes(command.scopes, client)
            redirect = self._redirect(client, command)
            credential = self.token_manager.mint_oauth_refresh_token()
            now = datetime.now(UTC)
            session.add(
                OAuthAuthorizationRequest(
                    public_id=str(uuid7()),
                    request_hash=credential.token_hash,
                    client_id=client.client_id,
                    redirect_uri=redirect,
                    redirect_uri_provided_explicitly=command.redirect_uri_provided_explicitly,
                    scopes=list(scopes),
                    code_challenge=command.code_challenge,
                    resource=command.resource,
                    state=command.state,
                    created_at=now,
                    expires_at=now + timedelta(seconds=self.config.request_ttl_seconds),
                )
            )
            await session.commit()
            return OAuthPendingAuthorization(credential.token, self.config.request_ttl_seconds)

    async def _pending(
        self, tx: OAuthTransaction, raw_request_id: str
    ) -> tuple[OAuthAuthorizationRequest, OAuthClient]:
        """Use identity-only lookup, then client/request locks and fresh validity checks."""
        identity = await tx.request(raw_request_id, datetime.now(UTC), lock=False)
        if identity is None:
            raise MCPOAuthError("invalid_request")
        client = await tx.client(identity.client_id)
        request = await tx.request(raw_request_id, datetime.now(UTC))
        if client is None or request is None:
            raise MCPOAuthError("invalid_request")
        self._resource(request.resource)
        self._scopes(tuple(request.scopes), client)
        if request.redirect_uri not in client.redirect_uris or not _PKCE_CHALLENGE.fullmatch(
            request.code_challenge
        ):
            raise MCPOAuthError("invalid_request")
        return request, client

    async def approve_authorization_request(
        self, command: OAuthConsentApprovalCommand
    ) -> OAuthApprovedAuthorization:
        """Expose authority refusals as protocol errors after the unit of work rolls back.

        Args:
            command: Pending request credential, authenticated owner and explicit replacement decision.

        Returns:
            The one-time code, grant, exact redirect and state after commit.

        Raises:
            MCPOAuthError: If protocol identity, state or current authority is invalid.
        """
        try:
            return await self._approve_with_binding_retry(command)
        except PermissionError as error:
            raise MCPOAuthError("access_denied") from error

    async def _approve_with_binding_retry(
        self, command: OAuthConsentApprovalCommand
    ) -> OAuthApprovedAuthorization:
        """Retry only one independently committed binding winner after complete rollback."""
        try:
            return await self._approve_once(command)
        except IntegrityError as error:
            if not _binding_conflict(error):
                raise
        return await self._approve_once(command)

    async def _approve_once(
        self, command: OAuthConsentApprovalCommand
    ) -> OAuthApprovedAuthorization:
        """Commit delegate, grant, request resolution and code as one atomic consent."""
        async with self.store.begin_write_transaction() as session:
            request, client = await self._pending(OAuthTransaction(session), command.raw_request_id)
            identity = await self.delegate_service.resolve_oauth_delegate_in_tx(
                session,
                command.owner,
                OAuthDelegateRequest(
                    client_id=client.client_id,
                    resource=request.resource,
                    scopes=tuple(request.scopes),
                    operator_public_id=command.operator_public_id,
                    label=command.label,
                    replace_existing_grant=command.replace_existing_grant,
                ),
            )
            now = datetime.now(UTC)
            if request.expires_at <= now:
                raise MCPOAuthError("invalid_request")
            credential = self.token_manager.mint_oauth_refresh_token()
            code = self._build_authorization_code(
                request, identity.grant_public_id, credential.token, now
            )
            resolved = await self.store.approve_authorization_request_in_tx(
                session, command.raw_request_id, code=code, resolved_at=now
            )
            if resolved is None:
                raise MCPOAuthError("invalid_request")
            await session.commit()
            return OAuthApprovedAuthorization(
                credential.token, identity.grant_public_id, request.redirect_uri, request.state
            )

    def _build_authorization_code(
        self, request: OAuthAuthorizationRequest, grant_id: str, raw_code: str, now: datetime
    ) -> OAuthAuthorizationCode:
        """Build only the hash of a short-lived code linked to the exact approved request."""
        return OAuthAuthorizationCode(
            public_id=str(uuid7()),
            code_hash=hash_token(raw_code),
            grant_public_id=grant_id,
            redirect_uri=request.redirect_uri,
            redirect_uri_provided_explicitly=request.redirect_uri_provided_explicitly,
            code_challenge=request.code_challenge,
            code_challenge_method="S256",
            scopes=list(request.scopes),
            created_at=now,
            expires_at=now + timedelta(seconds=self.config.code_ttl_seconds),
        )

    async def deny_authorization_request(self, command: OAuthConsentDenialCommand) -> None:
        """Deny once under current owner/operator authority without creating a grant.

        Args:
            command: Pending request credential and authenticated owner/operator authority.

        Raises:
            MCPOAuthError: If protocol identity, state or current authority is invalid.
        """
        try:
            async with self.store.begin_write_transaction() as session:
                tx = OAuthTransaction(session)
                request, _client = await self._pending(tx, command.raw_request_id)
                owner = await lock_oauth_owner(
                    session, command.owner, permissions_for_oauth_scopes(request.scopes)
                )
                await lock_oauth_operator(session, owner, command.operator_public_id)
                await tx.resolve_request(request, "denied", datetime.now(UTC))
                await session.commit()
        except PermissionError as error:
            raise MCPOAuthError("access_denied") from error

    def _mint_tokens(
        self, identity: OAuthDelegateIdentity, scopes: tuple[str, ...], client_id: str
    ) -> OAuthTokenExchangeResult:
        """Construct a read-only audience-bound JWT privately, before the outer commit."""
        permissions = permissions_for_oauth_scopes(scopes) & get_effective_permissions(
            identity.principal.role
        )
        signed = self.token_manager.create_mcp_oauth_access_token(
            MCPOAuthAccessTokenRequest(
                user=identity.principal,
                client_id=client_id,
                grant_id=identity.grant_public_id,
                issuer=self.config.issuer,
                audience=self.config.resource,
                scopes=scopes,
                issued_at=datetime.now(UTC),
                ttl_seconds=self.config.access_token_ttl_seconds,
                permissions=tuple(permissions),
            )
        )
        return OAuthTokenExchangeResult(signed.access_token, None, signed.expires_in, scopes)

    def _refresh_row(
        self, grant_id: str, scopes: tuple[str, ...], family_id: str | None = None
    ) -> tuple[str, OAuthRefreshToken]:
        """Build an opaque hash-only refresh successor without storing the raw credential."""
        credential = self.token_manager.mint_oauth_refresh_token()
        now = datetime.now(UTC)
        return credential.token, OAuthRefreshToken(
            public_id=str(uuid7()),
            token_hash=credential.token_hash,
            family_public_id=family_id or str(uuid7()),
            grant_public_id=grant_id,
            scopes=list(scopes),
            created_at=now,
            expires_at=now + timedelta(days=self.config.refresh_token_ttl_days),
        )

    async def _project(
        self, session: AsyncSession, grant: OAuthGrant
    ) -> OAuthDelegateIdentity | None:
        """Translate authority refusals without obscuring actual database failures."""
        try:
            return await self.delegate_service.project_oauth_grant_in_tx(session, grant)
        except (PermissionError, ValueError) as error:
            raise MCPOAuthError("access_denied") from error

    def _code_parameters(
        self, code: OAuthAuthorizationCode, grant: OAuthGrant, command: OAuthCodeExchangeCommand
    ) -> None:
        """Validate every non-consuming code parameter before claiming it."""
        self._resource(command.resource, grant)
        if code.consumed_at is not None or code.expires_at <= datetime.now(UTC):
            raise MCPOAuthError("invalid_grant")
        redirect_valid = command.redirect_uri == code.redirect_uri or (
            command.redirect_uri is None and not code.redirect_uri_provided_explicitly
        )
        if not redirect_valid or not _PKCE_VERIFIER.fullmatch(command.code_verifier):
            raise MCPOAuthError("invalid_grant")
        challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(command.code_verifier.encode("ascii")).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        if code.code_challenge_method != "S256" or not hmac.compare_digest(
            challenge, code.code_challenge
        ):
            raise MCPOAuthError("invalid_grant")

    async def exchange_authorization_code(
        self, command: OAuthCodeExchangeCommand
    ) -> OAuthTokenExchangeResult:
        """Authenticate and validate once, then atomically consume the code and issue its family.

        Args:
            command: Client authentication, code, verifier, resource and exact redirect.

        Returns:
            An access token and optional initial refresh token after commit.

        Raises:
            MCPOAuthError: If protocol identity, state or current authority is invalid.
        """
        result = await self._exchange_code_once(command)
        if isinstance(result, MCPOAuthError):
            raise result
        return result

    async def _exchange_code_once(
        self, command: OAuthCodeExchangeCommand
    ) -> OAuthTokenExchangeResult | MCPOAuthError:
        """Keep every consuming mutation and access-token construction in one unit of work."""
        async with self.store.begin_write_transaction() as session:
            tx = OAuthTransaction(session)
            client = await self._client(
                tx, command.client_id, command.client_secret, command.auth_method
            )
            code = await tx.code(command.raw_code, lock=False)
            if code is None:
                raise MCPOAuthError("invalid_grant")
            grant = await tx.grant(code.grant_public_id, client.client_id, lock=False)
            if grant is None:
                raise MCPOAuthError("invalid_grant")
            self._code_parameters(code, grant, command)
            identity = await self._project(session, grant)
            if identity is None:
                await session.commit()
                return MCPOAuthError("invalid_grant")
            code = await tx.code(command.raw_code)
            if code is None:
                raise MCPOAuthError("invalid_grant")
            self._code_parameters(code, grant, command)
            scopes = self._scopes(tuple(code.scopes), client)
            if not set(scopes) <= set(grant.scopes):
                raise MCPOAuthError("invalid_scope")
            raw_refresh = None
            refresh_row = None
            if OFFLINE_ACCESS in scopes:
                raw_refresh, refresh_row = self._refresh_row(grant.public_id, scopes)
            consumed = await self.store.consume_authorization_code_in_tx(
                session,
                command.raw_code,
                client_id=client.client_id,
                consumed_at=datetime.now(UTC),
                refresh_token=refresh_row,
            )
            if consumed is None:
                raise MCPOAuthError("invalid_grant")
            minted = self._mint_tokens(identity, scopes, client.client_id)
            result = OAuthTokenExchangeResult(
                minted.access_token, raw_refresh, minted.expires_in, scopes
            )
            await session.commit()
            return result

    async def _refresh_context(
        self, tx: OAuthTransaction, command: OAuthRefreshExchangeCommand
    ) -> tuple[OAuthClient, OAuthGrant, OAuthRefreshToken, tuple[str, ...]]:
        """Validate client/resource/scopes before any replay-driven revocation."""
        client = await self._client(
            tx, command.client_id, command.client_secret, command.auth_method
        )
        predecessor = await tx.refresh(command.raw_refresh_token, lock=False)
        if predecessor is None:
            raise MCPOAuthError("invalid_grant")
        grant = await tx.grant(predecessor.grant_public_id, client.client_id, lock=False)
        if grant is None:
            raise MCPOAuthError("invalid_grant")
        self._resource(command.resource, grant)
        scopes = self._scopes(
            command.scopes if command.scopes is not None else tuple(predecessor.scopes), client
        )
        if not set(scopes) <= set(predecessor.scopes) or not set(scopes) <= set(grant.scopes):
            raise MCPOAuthError("invalid_scope")
        return client, grant, predecessor, scopes

    async def exchange_refresh_token(
        self, command: OAuthRefreshExchangeCommand
    ) -> OAuthTokenExchangeResult:
        """Return a rotation or report committed strict grant-wide replay revocation.

        Args:
            command: Client authentication, predecessor credential, resource and optional narrower scopes.

        Returns:
            An access token and rotated refresh credential after commit.

        Raises:
            MCPOAuthError: If protocol identity, state or current authority is invalid.
        """
        try:
            result = await self._exchange_refresh_once(command)
        except OAuthTransactionConflictError:
            result = await self._classify_lost_refresh(command)
        if isinstance(result, MCPOAuthError):
            raise result
        return result

    async def _exchange_refresh_once(
        self, command: OAuthRefreshExchangeCommand
    ) -> OAuthTokenExchangeResult | MCPOAuthError:
        """Consume one predecessor with one commit and no nested repository session."""
        async with self.store.begin_write_transaction() as session:
            tx = OAuthTransaction(session)
            client, grant, _predecessor, scopes = await self._refresh_context(tx, command)
            identity = await self._project(session, grant)
            if identity is None:
                await session.commit()
                return MCPOAuthError("invalid_grant")
            predecessor = await tx.refresh(command.raw_refresh_token)
            if predecessor is None or predecessor.revoked_at is not None:
                raise MCPOAuthError("invalid_grant")
            if predecessor.used_at is not None:
                await tx.revoke_grant(grant.public_id, datetime.now(UTC))
                await session.commit()
                return OAuthRefreshReuseDetectedError("invalid_grant")
            if predecessor.expires_at <= datetime.now(UTC):
                raise MCPOAuthError("invalid_grant")
            raw_refresh, successor = self._refresh_row(
                grant.public_id, scopes, predecessor.family_public_id
            )
            successor.expires_at = min(successor.expires_at, predecessor.expires_at)
            await tx.rotate_refresh(predecessor, successor, datetime.now(UTC))
            minted = self._mint_tokens(identity, scopes, client.client_id)
            result = OAuthTokenExchangeResult(
                minted.access_token, raw_refresh, minted.expires_in, scopes
            )
            await session.commit()
            return result

    async def _classify_lost_refresh(self, command: OAuthRefreshExchangeCommand) -> MCPOAuthError:
        """Reauthenticate and classify only in a new transaction after complete loser rollback."""
        async with self.store.begin_write_transaction() as session:
            tx = OAuthTransaction(session)
            _client, grant, _predecessor, _scopes = await self._refresh_context(tx, command)
            identity = await self._project(session, grant)
            if identity is None:
                await session.commit()
                return MCPOAuthError("invalid_grant")
            row = await tx.refresh(command.raw_refresh_token)
            if row is None or row.revoked_at is not None or row.used_at is None:
                return MCPOAuthError("invalid_grant")
            await tx.revoke_grant(grant.public_id, datetime.now(UTC))
            await session.commit()
            return OAuthRefreshReuseDetectedError("invalid_grant")

    async def verify_access_token(self, raw_token: str) -> OAuthVerifiedAccess:
        """Perform an uncached live JWT/client/grant/current-permission intersection.

        Args:
            raw_token: Signed short-lived MCP OAuth access JWT.

        Returns:
            The live, scope-clamped principal and permissions for this exact grant.

        Raises:
            MCPOAuthError: If protocol identity, state or current authority is invalid.
        """
        claims = self.token_manager.verify_mcp_oauth_access_token(
            raw_token, issuer=self.config.issuer, audience=self.config.resource
        )
        if claims is None:
            raise MCPOAuthError("invalid_token")
        result = await self._verify_live(claims)
        if isinstance(result, MCPOAuthError):
            raise result
        return result

    async def _verify_live(
        self, claims: MCPOAuthAccessTokenClaims
    ) -> OAuthVerifiedAccess | MCPOAuthError:
        """Bind signed identities to the currently active client, owner, and exact delegate."""
        async with self.store.begin_write_transaction() as session:
            tx = OAuthTransaction(session)
            client = await tx.client(claims.client_id)
            grant = await tx.grant(claims.grant_id, claims.client_id, lock=False)
            if client is None or grant is None:
                raise MCPOAuthError("invalid_token")
            self._resource(grant.resource)
            identity = await self._project(session, grant)
            if identity is None:
                await session.commit()
                return MCPOAuthError("invalid_token")
            self._claims_identity(claims, grant, identity)
            try:
                signed_scopes = validate_initial_oauth_scopes(claims.scope.split())
            except ValueError as error:
                raise MCPOAuthError("invalid_token") from error
            scopes = tuple(
                s for s in signed_scopes if s in grant.scopes and s in client.allowed_scopes
            )
            scopes = self._scopes(scopes, client)
            permissions = permissions_for_oauth_scopes(scopes) & get_effective_permissions(
                identity.principal.role, claims.permissions, claims.permission_scope_version
            )
            if not permissions:
                raise MCPOAuthError("access_denied")
            principal = identity.principal.model_copy(
                update={
                    "permissions": sorted(permission.value for permission in permissions),
                    "permission_scope_version": PERMISSION_SCOPE_VERSION,
                    "active_wallet_public_id": claims.active_wallet_public_id,
                }
            )
            await session.commit()
            return OAuthVerifiedAccess(
                principal, frozenset(permissions), grant.public_id, client.client_id, scopes
            )

    @staticmethod
    def _claims_identity(
        claims: MCPOAuthAccessTokenClaims, grant: OAuthGrant, identity: OAuthDelegateIdentity
    ) -> None:
        """Refuse validly signed credentials whose subject, role, operator or grant linkage differs."""
        principal = identity.principal
        matches = (
            claims.sub == principal.user_public_id
            and claims.user_public_id == principal.user_public_id
            and claims.role == principal.role
            and claims.sid == grant.public_id
            and claims.operator_public_ids == principal.operator_public_ids
            and claims.primary_operator_public_id == principal.primary_operator_public_id
            and claims.operator_membership_public_ids == principal.operator_membership_public_ids
        )
        if not matches:
            raise MCPOAuthError("invalid_token")
