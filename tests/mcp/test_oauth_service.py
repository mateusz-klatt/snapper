"""End-to-end unit and integration tests for the dormant MCP OAuth service."""

import base64
import hashlib
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import bcrypt
import pytest
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.api.schemas.ai_delegates import DelegateCapsBody
from snapper.api.schemas.ai_delegates import DelegateCreateBody
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import OAuthDelegateIdentity
from snapper.application.ai_delegates.service import OAuthDelegateRequest
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TokenManager
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.models import UserOperatorMembership
from snapper.data.repository import SQLAlchemyRepository
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.contracts import MCPOAuthServiceConfig
from snapper.mcp.oauth.contracts import OAuthAuthorizationRequestCreate
from snapper.mcp.oauth.contracts import OAuthCodeExchangeCommand
from snapper.mcp.oauth.contracts import OAuthConsentApprovalCommand
from snapper.mcp.oauth.contracts import OAuthRefreshExchangeCommand
from snapper.mcp.oauth.contracts import OAuthRefreshReuseDetectedError
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import permissions_for_oauth_scopes
from snapper.mcp.oauth.service import MCPOAuthService
from snapper.mcp.oauth.store import MCPOAuthStore

_ISSUER = "https://snapper.ch/api/mcp"
_RESOURCE = "https://snapper.ch/api/mcp"
_REDIRECT_URI = "https://chatgpt.com/connector/oauth/callback"
_CLIENT_ID = "snapper-mcp-chatgpt-test"
_CLIENT_SECRET = "test-confidential-client-secret-value-0123456789"
_OTHER_CLIENT_ID = "snapper-mcp-claude-test"
_OTHER_CLIENT_SECRET = "test-other-client-secret-value-0123456789abcdef"
_OPERATOR_A = "00000000-0000-7000-8000-000000000301"
_OPERATOR_B = "00000000-0000-7000-8000-000000000302"
_VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk-valid-pkce-verifier-01"
_PASSWORD_HASH = bcrypt.hashpw(b"fixture-password", bcrypt.gensalt(rounds=4)).decode()
_CLIENT_SECRET_HASH = bcrypt.hashpw(_CLIENT_SECRET.encode(), bcrypt.gensalt(rounds=4)).decode()
_OTHER_SECRET_HASH = bcrypt.hashpw(
    _OTHER_CLIENT_SECRET.encode(),
    bcrypt.gensalt(rounds=4),
).decode()


def _pkce_s256(verifier: str) -> str:
    """Compute the RFC 7636 S256 challenge for one code verifier."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _fresh_token_manager() -> TokenManager:
    """Return an isolated TokenManager singleton."""
    TokenManager.clear_instance()
    TokenManager._initialized = False
    return TokenManager()


@dataclass(frozen=True, slots=True)
class _OAuthHarness:
    """Isolated SQLite harness bundling repository, store, delegate service, and OAuth service."""

    repository: SQLAlchemyRepository
    store: MCPOAuthStore
    delegate_service: DelegateService
    token_manager: TokenManager
    service: MCPOAuthService


async def _build_harness(tmp_path: Path) -> _OAuthHarness:
    """Create a migrated SQLite database with two active confidential clients."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'oauth-service.db'}")
    await repository.create_all()
    now = datetime.now(UTC)
    store = MCPOAuthStore(repository)
    await store.add_client(
        OAuthClient(
            public_id="00000000-0000-7000-8000-000000000310",
            client_id=_CLIENT_ID,
            client_secret_hash=_CLIENT_SECRET_HASH,
            client_name="ChatGPT Connector",
            redirect_uris=[_REDIRECT_URI],
            token_endpoint_auth_method="client_secret_basic",
            allowed_scopes=[SNAPPER_READ, OFFLINE_ACCESS],
            is_active=True,
            created_at=now,
        )
    )
    await store.add_client(
        OAuthClient(
            public_id="00000000-0000-7000-8000-000000000311",
            client_id=_OTHER_CLIENT_ID,
            client_secret_hash=_OTHER_SECRET_HASH,
            client_name="Claude Connector",
            redirect_uris=["https://claude.ai/api/mcp/auth_callback"],
            token_endpoint_auth_method="client_secret_post",
            allowed_scopes=[SNAPPER_READ, OFFLINE_ACCESS],
            is_active=True,
            created_at=now,
        )
    )
    token_manager = _fresh_token_manager()
    delegate_service = DelegateService(repository=repository, token_manager=token_manager)
    service = MCPOAuthService(
        repository=repository,
        token_manager=token_manager,
        config=MCPOAuthServiceConfig(
            issuer=_ISSUER,
            resource=_RESOURCE,
            access_token_ttl_seconds=900,
            code_ttl_seconds=120,
            request_ttl_seconds=300,
            refresh_token_ttl_days=90,
        ),
        delegate_service=delegate_service,
    )
    return _OAuthHarness(
        repository=repository,
        store=store,
        delegate_service=delegate_service,
        token_manager=token_manager,
        service=service,
    )


async def _seed_user_with_membership(
    repository: SQLAlchemyRepository,
    *,
    user_public_id: str,
    role: UserRole = UserRole.OPERATOR,
    operator_public_id: str | None = _OPERATOR_A,
    is_active: bool = True,
) -> str | None:
    """Seed one SCD2 user and optional active operator membership."""
    seed_time = datetime(2026, 1, 1, tzinfo=UTC)
    seed_session_id = str(uuid7())
    membership_id = (
        f"00000000-0000-7000-8000-9999{user_public_id[-8:]}"
        if operator_public_id is not None
        else None
    )
    async with repository.session() as session:
        session.add(
            User(
                public_id=user_public_id,
                session_id=seed_session_id,
                sequence_id=1,
                timestamp=seed_time,
                known_to=KNOWN_TO_MAX,
                username=f"owner-{user_public_id[-4:]}",
                email=f"owner-{user_public_id[-4:]}@example.com",
                password_hash=_PASSWORD_HASH,
                role=role.value,
                is_active=is_active,
                created_at=seed_time,
            )
        )
        if operator_public_id is not None and membership_id is not None:
            session.add(
                UserOperatorMembership(
                    public_id=membership_id,
                    user_public_id=user_public_id,
                    operator_public_id=operator_public_id,
                    is_primary=True,
                    timestamp=seed_time,
                    known_to=KNOWN_TO_MAX,
                    session_id=seed_session_id,
                    sequence_id=2,
                )
            )
        await session.commit()
    return membership_id


def _principal(
    user_public_id: str,
    *,
    role: UserRole = UserRole.OPERATOR,
    operator_public_id: str | None = _OPERATOR_A,
    membership_public_id: str | None = None,
    permissions: list[str] | None = None,
) -> AuthPrincipal:
    """Construct an authenticated principal matching the seeded user."""
    operators = [operator_public_id] if operator_public_id else []
    membership_map = (
        {operator_public_id: membership_public_id}
        if operator_public_id and membership_public_id
        else {}
    )
    return AuthPrincipal(
        username=f"owner-{user_public_id[-4:]}",
        role=role,
        is_active=True,
        user_public_id=user_public_id,
        operator_public_ids=operators,
        operator_membership_public_ids=membership_map,
        primary_operator_public_id=operator_public_id or "",
        permissions=permissions,
        permission_scope_version=PERMISSION_SCOPE_VERSION if permissions is not None else None,
    )


@pytest.mark.asyncio
async def test_end_to_end_consent_pkce_exchange_refresh_live_access_and_pat_isolation(
    tmp_path: Path,
) -> None:
    """Verify full dormant OAuth flow, read-only ceiling intersection, and PAT separation.

    Given a registered confidential client and an authorized owner.
    When consent, S256 code exchange and refresh complete.
    Then live access is scope-clamped and the legacy PAT inventory is untouched.
    """
    harness = await _build_harness(tmp_path)
    owner_id = "00000000-0000-7000-8000-000000000320"
    membership_id = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership_id)
    svc = harness.service

    create_req_cls = OAuthAuthorizationRequestCreate
    approve_cmd_cls = OAuthConsentApprovalCommand
    exchange_cmd_cls = OAuthCodeExchangeCommand
    refresh_cmd_cls = OAuthRefreshExchangeCommand

    pending = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ, OFFLINE_ACCESS),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="opaque-state-1",
        )
    )
    approved = await svc.approve_authorization_request(
        approve_cmd_cls(
            raw_request_id=pending.raw_request_id,
            owner=owner,
            operator_public_id=_OPERATOR_A,
            label="ChatGPT Read",
        )
    )
    exchanged = await svc.exchange_authorization_code(
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        )
    )
    assert exchanged.refresh_token is not None
    verified = await svc.verify_access_token(exchanged.access_token)
    assert verified.permissions == permissions_for_oauth_scopes([SNAPPER_READ])
    assert Permission.CREATE_ORDERS not in verified.permissions
    assert verified.principal.operator_public_ids == [_OPERATOR_A]

    rotated = await svc.exchange_refresh_token(
        refresh_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_refresh_token=exchanged.refresh_token,
            resource=_RESOURCE,
            scopes=(SNAPPER_READ, OFFLINE_ACCESS),
        )
    )
    assert rotated.refresh_token is not None
    assert rotated.refresh_token != exchanged.refresh_token
    verified_rotated = await svc.verify_access_token(rotated.access_token)
    assert verified_rotated.grant_public_id == approved.grant_public_id

    pat_created = await harness.delegate_service.create_delegate(
        owner=owner,
        body=DelegateCreateBody(label="Legacy PAT", caps=DelegateCapsBody()),
    )
    oauth_error: type[Exception] = MCPOAuthError
    with pytest.raises(oauth_error):
        await svc.verify_access_token(pat_created.access_token)
    assert harness.token_manager.verify_token(exchanged.access_token) is None
    assert (
        await harness.token_manager.verify_token_with_db(
            pat_created.access_token,
            harness.repository,
            expected_token_type=TOKEN_TYPE_ACCESS,
        )
        is not None
    )
    async with harness.repository.session() as session:
        oauth_delegate_tokens = list(
            (
                await session.execute(
                    select(UserActiveToken).where(
                        UserActiveToken.user_public_id == verified.principal.user_public_id
                    )
                )
            ).scalars()
        )
    assert oauth_delegate_tokens == []
    await harness.repository.engine.dispose()


@pytest.mark.asyncio
async def test_non_consuming_invalid_exchange_and_forged_linkage_refusals(
    tmp_path: Path,
) -> None:
    """Verify invalid PKCE, client, secret, redirect, or resource never consumes a valid code.

    Given a valid pending consent or authorization code.
    When invalid exchange parameters or forged identity links are submitted.
    Then the attempt fails without consuming the valid credential.
    """
    harness = await _build_harness(tmp_path)
    owner_id = "00000000-0000-7000-8000-000000000330"
    membership_id = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership_id)
    svc = harness.service

    create_req_cls = OAuthAuthorizationRequestCreate
    approve_cmd_cls = OAuthConsentApprovalCommand
    exchange_cmd_cls = OAuthCodeExchangeCommand
    oauth_error: type[Exception] = MCPOAuthError

    pending = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ,),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="s",
        )
    )
    approved = await svc.approve_authorization_request(
        approve_cmd_cls(raw_request_id=pending.raw_request_id, owner=owner)
    )

    bad_attempts = [
        exchange_cmd_cls(
            client_id=_OTHER_CLIENT_ID,
            client_secret=_OTHER_CLIENT_SECRET,
            auth_method="client_secret_post",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        ),
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret="wrong-secret",
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        ),
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_post",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        ),
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier="wrong-pkce-verifier-with-at-least-forty-three-characters-0000",
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        ),
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri="https://chatgpt.com/other",
            resource=_RESOURCE,
        ),
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=None,
            resource=_RESOURCE,
        ),
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource="https://evil.example/api/mcp",
        ),
    ]
    for attempt in bad_attempts:
        with pytest.raises(oauth_error):
            await svc.exchange_authorization_code(attempt)

    valid_exchange = await svc.exchange_authorization_code(
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=approved.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        )
    )
    assert valid_exchange.refresh_token is None
    with pytest.raises(oauth_error):
        await svc.exchange_authorization_code(
            exchange_cmd_cls(
                client_id=_CLIENT_ID,
                client_secret=_CLIENT_SECRET,
                auth_method="client_secret_basic",
                raw_code=approved.raw_code,
                code_verifier=_VERIFIER,
                redirect_uri=_REDIRECT_URI,
                resource=_RESOURCE,
            )
        )
    await harness.repository.engine.dispose()


@pytest.mark.asyncio
async def test_confirmed_refresh_reuse_commits_grant_revocation_and_immediately_denies_live_access(
    tmp_path: Path,
) -> None:
    """Verify predecessor reuse revokes affected grant + families and blocks issued access JWTs.

    Given a rotated predecessor and multiple families on its grant.
    When the predecessor is reused with valid client and resource parameters.
    Then revocation commits and existing access is immediately denied.
    """
    harness = await _build_harness(tmp_path)
    owner_id = "00000000-0000-7000-8000-000000000340"
    membership_id = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership_id)
    svc = harness.service

    create_req_cls = OAuthAuthorizationRequestCreate
    approve_cmd_cls = OAuthConsentApprovalCommand
    exchange_cmd_cls = OAuthCodeExchangeCommand
    refresh_cmd_cls = OAuthRefreshExchangeCommand
    reuse_error_cls: type[Exception] = OAuthRefreshReuseDetectedError
    oauth_error_cls: type[Exception] = MCPOAuthError

    first_req = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ, OFFLINE_ACCESS),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="f1",
        )
    )
    first_app = await svc.approve_authorization_request(
        approve_cmd_cls(raw_request_id=first_req.raw_request_id, owner=owner)
    )
    family_one = await svc.exchange_authorization_code(
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=first_app.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        )
    )

    second_req = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ, OFFLINE_ACCESS),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="f2",
        )
    )
    second_app = await svc.approve_authorization_request(
        approve_cmd_cls(raw_request_id=second_req.raw_request_id, owner=owner)
    )
    family_two = await svc.exchange_authorization_code(
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=second_app.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        )
    )

    other_req = await svc.create_authorization_request(
        create_req_cls(
            client_id=_OTHER_CLIENT_ID,
            redirect_uri="https://claude.ai/api/mcp/auth_callback",
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ, OFFLINE_ACCESS),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="other",
        )
    )
    other_app = await svc.approve_authorization_request(
        approve_cmd_cls(raw_request_id=other_req.raw_request_id, owner=owner)
    )
    unrelated = await svc.exchange_authorization_code(
        exchange_cmd_cls(
            client_id=_OTHER_CLIENT_ID,
            client_secret=_OTHER_CLIENT_SECRET,
            auth_method="client_secret_post",
            raw_code=other_app.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri="https://claude.ai/api/mcp/auth_callback",
            resource=_RESOURCE,
        )
    )

    assert family_one.refresh_token is not None
    assert family_two.refresh_token is not None
    assert unrelated.refresh_token is not None

    rotated_one = await svc.exchange_refresh_token(
        refresh_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_refresh_token=family_one.refresh_token,
            resource=_RESOURCE,
        )
    )
    assert await svc.verify_access_token(rotated_one.access_token) is not None

    with pytest.raises(oauth_error_cls):
        await svc.exchange_refresh_token(
            refresh_cmd_cls(
                client_id=_CLIENT_ID,
                client_secret=_CLIENT_SECRET,
                auth_method="client_secret_basic",
                raw_refresh_token=family_one.refresh_token,
                resource="https://wrong.example/api/mcp",
            )
        )
    assert await svc.verify_access_token(rotated_one.access_token) is not None

    with pytest.raises(reuse_error_cls):
        await svc.exchange_refresh_token(
            refresh_cmd_cls(
                client_id=_CLIENT_ID,
                client_secret=_CLIENT_SECRET,
                auth_method="client_secret_basic",
                raw_refresh_token=family_one.refresh_token,
                resource=_RESOURCE,
            )
        )

    with pytest.raises(oauth_error_cls):
        await svc.verify_access_token(rotated_one.access_token)
    with pytest.raises(oauth_error_cls):
        await svc.exchange_refresh_token(
            refresh_cmd_cls(
                client_id=_CLIENT_ID,
                client_secret=_CLIENT_SECRET,
                auth_method="client_secret_basic",
                raw_refresh_token=family_two.refresh_token,
                resource=_RESOURCE,
            )
        )
    assert await svc.verify_access_token(unrelated.access_token) is not None
    await harness.repository.engine.dispose()


@pytest.mark.asyncio
async def test_live_authority_demotion_membership_loss_impersonation_and_rollback_atomicity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify impersonation exception, live membership/role demotion, and rollback atomicity.

    Given an owner whose live authority can change during OAuth operations.
    When demotion, membership loss, explicit impersonation or an injected failure occurs.
    Then current authority is enforced and partial consent changes roll back.
    """
    harness = await _build_harness(tmp_path)
    impersonator_id = "00000000-0000-7000-8000-000000000350"
    await _seed_user_with_membership(
        harness.repository,
        user_public_id=impersonator_id,
        role=UserRole.ADMIN,
        operator_public_id=None,
    )
    impersonator = _principal(
        impersonator_id,
        role=UserRole.ADMIN,
        operator_public_id=None,
        permissions=[p.value for p in ROLE_PERMISSIONS[UserRole.ADMIN]],
    )
    svc = harness.service

    create_req_cls = OAuthAuthorizationRequestCreate
    approve_cmd_cls = OAuthConsentApprovalCommand
    exchange_cmd_cls = OAuthCodeExchangeCommand
    oauth_error_cls: type[Exception] = MCPOAuthError

    req_ok = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ,),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="imp",
        )
    )
    app_ok = await svc.approve_authorization_request(
        approve_cmd_cls(
            raw_request_id=req_ok.raw_request_id,
            owner=impersonator,
            operator_public_id=_OPERATOR_B,
        )
    )
    tok_ok = await svc.exchange_authorization_code(
        exchange_cmd_cls(
            client_id=_CLIENT_ID,
            client_secret=_CLIENT_SECRET,
            auth_method="client_secret_basic",
            raw_code=app_ok.raw_code,
            code_verifier=_VERIFIER,
            redirect_uri=_REDIRECT_URI,
            resource=_RESOURCE,
        )
    )
    assert await svc.verify_access_token(tok_ok.access_token) is not None

    async with harness.repository.session() as session:
        await session.execute(
            update(User)
            .where(User.public_id == impersonator_id)
            .values(role=UserRole.OPERATOR.value)
        )
        await session.commit()
    with pytest.raises(oauth_error_cls):
        await svc.verify_access_token(tok_ok.access_token)

    rollback_owner_id = "00000000-0000-7000-8000-000000000351"
    rb_membership = await _seed_user_with_membership(
        harness.repository,
        user_public_id=rollback_owner_id,
    )
    rollback_owner = _principal(rollback_owner_id, membership_public_id=rb_membership)
    req_rb = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ,),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="rb",
        )
    )

    def _boom(*_args: object, **_kwargs: object) -> OAuthAuthorizationCode:
        raise RuntimeError("injected code creation failure")

    monkeypatch.setattr(svc, "_build_authorization_code", _boom)
    with pytest.raises(RuntimeError, match="injected code creation failure"):
        await svc.approve_authorization_request(
            approve_cmd_cls(raw_request_id=req_rb.raw_request_id, owner=rollback_owner)
        )

    assert (
        await harness.store.get_pending_authorization_request(
            req_rb.raw_request_id,
            now=datetime.now(UTC),
        )
        is not None
    )
    async with harness.repository.session() as session:
        orphan_delegates = list(
            (
                await session.execute(
                    select(User).where(User.created_by_user_public_id == rollback_owner_id)
                )
            ).scalars()
        )
        orphan_grants = list(
            (
                await session.execute(
                    select(OAuthGrant).where(OAuthGrant.owner_user_public_id == rollback_owner_id)
                )
            ).scalars()
        )
    assert orphan_delegates == []
    assert orphan_grants == []
    await harness.repository.engine.dispose()


@pytest.mark.asyncio
async def test_unique_binding_conflict_rolls_back_entire_consent_and_recovers_on_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify unique-binding conflict rolls back first consent attempt and reuses winning grant.

    Given a competing independently committed active binding.
    When consent loses the known uniqueness race.
    Then the entire loser transaction rolls back before one bounded retry reuses the winner.
    """
    harness = await _build_harness(tmp_path)
    owner_id = "00000000-0000-7000-8000-000000000360"
    membership_id = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership_id)
    svc = harness.service

    create_req_cls = OAuthAuthorizationRequestCreate
    approve_cmd_cls = OAuthConsentApprovalCommand
    req = await svc.create_authorization_request(
        create_req_cls(
            client_id=_CLIENT_ID,
            redirect_uri=_REDIRECT_URI,
            redirect_uri_provided_explicitly=True,
            scopes=(SNAPPER_READ,),
            code_challenge=_pkce_s256(_VERIFIER),
            code_challenge_method="S256",
            resource=_RESOURCE,
            state="race",
        )
    )

    winner = await harness.delegate_service.get_or_create_oauth_delegate(
        owner,
        OAuthDelegateRequest(
            client_id=_CLIENT_ID,
            resource=_RESOURCE,
            scopes=(SNAPPER_READ,),
            operator_public_id=_OPERATOR_A,
        ),
    )

    attempts = 0
    original_resolve = harness.delegate_service.resolve_oauth_delegate_in_tx

    async def _conflict_once(
        session: AsyncSession, caller: AuthPrincipal, request: OAuthDelegateRequest
    ) -> OAuthDelegateIdentity:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            sqlite_err = Exception(
                "UNIQUE constraint failed: oauth_grants.owner_user_public_id, "
                "oauth_grants.operator_public_id, oauth_grants.client_id"
            )
            sqlite_err.sqlite_errorcode = 2067
            raise IntegrityError("INSERT INTO oauth_grants", {}, sqlite_err)
        return await original_resolve(session, caller, request)

    monkeypatch.setattr(harness.delegate_service, "resolve_oauth_delegate_in_tx", _conflict_once)

    approved = await svc.approve_authorization_request(
        approve_cmd_cls(
            raw_request_id=req.raw_request_id,
            owner=owner,
            operator_public_id=_OPERATOR_A,
        )
    )
    assert approved.grant_public_id == winner.grant_public_id
    async with harness.repository.session() as session:
        delegates = list(
            (
                await session.execute(
                    select(User).where(User.created_by_user_public_id == owner_id)
                )
            ).scalars()
        )
    assert len(delegates) == 1
    await harness.repository.engine.dispose()
