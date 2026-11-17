"""Regression tests for session-bound OAuth delegate bindings and live projections."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid7

import pytest
from sqlalchemy import select
from sqlalchemy import update

import snapper.application.ai_delegates.service as delegate_module
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import OAuthDelegateRequest
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.repository import SQLAlchemyRepository

_BCRYPT_PLACEHOLDER = "$2b$12$" + "x" * 53
_RESOURCE = "https://snapper.ch/api/mcp"
_CLIENT_ID = "chatgpt-client"
_OPERATOR_ID = "00000000-0000-7000-8000-000000000201"


def _fresh_token_manager() -> TokenManager:
    """Return an isolated TokenManager instance for deterministic tests."""
    TokenManager.clear_instance()
    TokenManager._initialized = False
    return TokenManager()


async def _repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Create one migrated file-backed SQLite repository."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'oauth-delegate.db'}")
    await repo.create_all()
    return repo


async def _seed_owner(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    role: UserRole = UserRole.OPERATOR,
    is_active: bool = True,
    with_membership: bool = True,
) -> str:
    """Insert an owner User row and optional primary operator membership."""
    seed_time = datetime(2026, 1, 1, tzinfo=UTC)
    membership_id = str(uuid7())
    async with repo.session() as session:
        session.add(
            User(
                public_id=public_id,
                session_id="seed",
                sequence_id=1,
                timestamp=seed_time,
                known_to=KNOWN_TO_MAX,
                username=f"user-{public_id[-4:]}",
                email=f"{public_id[-4:]}@example.com",
                password_hash=_BCRYPT_PLACEHOLDER,
                role=role.value,
                is_active=is_active,
                created_at=seed_time,
            )
        )
        if with_membership:
            session.add(
                UserOperatorMembership(
                    public_id=membership_id,
                    user_public_id=public_id,
                    operator_public_id=_OPERATOR_ID,
                    is_primary=True,
                    timestamp=seed_time,
                    known_to=KNOWN_TO_MAX,
                    session_id="seed",
                    sequence_id=2,
                )
            )
        await session.commit()
    return membership_id


def _owner_principal(
    public_id: str,
    *,
    role: UserRole = UserRole.OPERATOR,
    permissions: list[str] | None = None,
) -> AuthPrincipal:
    """Build an authenticated owner principal for OAuth delegate binding tests."""
    return AuthPrincipal(
        username=f"user-{public_id[-4:]}",
        role=role,
        is_active=True,
        user_public_id=public_id,
        operator_public_ids=[_OPERATOR_ID],
        primary_operator_public_id=_OPERATOR_ID,
        permissions=permissions,
        permission_scope_version=PERMISSION_SCOPE_VERSION if permissions is not None else None,
    )


async def _seed_refresh_family(
    repo: SQLAlchemyRepository,
    *,
    grant_public_id: str,
    family_public_id: str,
    token_public_id: str,
) -> None:
    """Persist one live refresh token under the given grant and family."""
    now = datetime.now(UTC)
    async with repo.session() as session:
        session.add(
            OAuthRefreshToken(
                public_id=token_public_id,
                token_hash=hash_token(f"raw-{token_public_id}"),
                family_public_id=family_public_id,
                grant_public_id=grant_public_id,
                scopes=["snapper.read", "offline_access"],
                created_at=now,
                expires_at=now + timedelta(days=90),
            )
        )
        await session.commit()


@pytest.mark.asyncio
async def test_oauth_delegate_rejects_scope_or_resource_mismatch_until_replacement_approved(
    tmp_path: Path,
) -> None:
    """Verify mismatched resource or scopes refuse reuse unless replacement is explicit.

    Given an active OAuth grant with two live refresh families on one delegate,
    When a call requests widened scopes without replacement and then with replacement,
    Then the first call refuses without mutating state and explicit replacement retires
    the old grant plus both families while reusing the valid delegate under a new grant ID.
    """
    repo = await _repository(tmp_path)
    owner_id = "00000000-0000-7000-8000-000000000210"
    await _seed_owner(repo, public_id=owner_id)
    service = DelegateService(repository=repo, token_manager=_fresh_token_manager())
    owner = _owner_principal(owner_id)
    initial = await service.get_or_create_oauth_delegate(
        owner,
        OAuthDelegateRequest(
            client_id=_CLIENT_ID,
            resource=_RESOURCE,
            scopes=("snapper.read",),
        ),
    )
    await _seed_refresh_family(
        repo,
        grant_public_id=initial.grant_public_id,
        family_public_id="00000000-0000-7000-8000-000000000211",
        token_public_id="00000000-0000-7000-8000-000000000212",
    )
    await _seed_refresh_family(
        repo,
        grant_public_id=initial.grant_public_id,
        family_public_id="00000000-0000-7000-8000-000000000213",
        token_public_id="00000000-0000-7000-8000-000000000214",
    )

    mismatch_error: type[Exception] = getattr(
        delegate_module,
        "OAuthGrantReplacementRequiredError",
        RuntimeError,
    )
    with pytest.raises(mismatch_error):
        await service.get_or_create_oauth_delegate(
            owner,
            OAuthDelegateRequest(
                client_id=_CLIENT_ID,
                resource=_RESOURCE,
                scopes=("snapper.read", "offline_access"),
            ),
        )

    replacement_request = OAuthDelegateRequest(
        client_id=_CLIENT_ID,
        resource=_RESOURCE,
        scopes=("snapper.read", "offline_access"),
        replace_existing_grant=True,
    )
    replaced = await service.get_or_create_oauth_delegate(owner, replacement_request)
    assert replaced.delegate.public_id == initial.delegate.public_id
    assert replaced.grant_public_id != initial.grant_public_id

    async with repo.session() as session:
        old_grant = (
            await session.execute(
                select(OAuthGrant).where(OAuthGrant.public_id == initial.grant_public_id)
            )
        ).scalar_one()
        refresh_rows = list(
            (
                await session.execute(
                    select(OAuthRefreshToken).where(
                        OAuthRefreshToken.grant_public_id == initial.grant_public_id
                    )
                )
            ).scalars()
        )
    assert old_grant.revoked_at is not None
    assert len(refresh_rows) == 2
    assert all(row.revoked_at is not None for row in refresh_rows)
    await repo.engine.dispose()


@pytest.mark.asyncio
async def test_oauth_delegate_rejects_missing_inactive_demoted_auditor_or_narrowed_owner(
    tmp_path: Path,
) -> None:
    """Verify OAuth binding locks and verifies live owner existence, status, and authority.

    Given an owner with missing or insufficient current authority.
    When OAuth delegate creation is requested.
    Then delegation is refused without persisting an identity.
    """
    repo = await _repository(tmp_path)
    service = DelegateService(repository=repo, token_manager=_fresh_token_manager())
    request = OAuthDelegateRequest(
        client_id=_CLIENT_ID,
        resource=_RESOURCE,
        scopes=("snapper.read",),
    )

    with pytest.raises(PermissionError):
        await service.get_or_create_oauth_delegate(
            _owner_principal("00000000-0000-7000-8000-000000000220"),
            request,
        )

    inactive_id = "00000000-0000-7000-8000-000000000221"
    await _seed_owner(repo, public_id=inactive_id, is_active=False)
    with pytest.raises(PermissionError):
        await service.get_or_create_oauth_delegate(_owner_principal(inactive_id), request)

    demoted_id = "00000000-0000-7000-8000-000000000222"
    await _seed_owner(repo, public_id=demoted_id, role=UserRole.VIEWER)
    with pytest.raises(PermissionError):
        await service.get_or_create_oauth_delegate(_owner_principal(demoted_id), request)

    auditor_id = "00000000-0000-7000-8000-000000000223"
    await _seed_owner(repo, public_id=auditor_id, role=UserRole.AUDITOR)
    with pytest.raises(PermissionError):
        await service.get_or_create_oauth_delegate(
            _owner_principal(auditor_id, role=UserRole.AUDITOR),
            request,
        )

    narrowed_id = "00000000-0000-7000-8000-000000000224"
    await _seed_owner(repo, public_id=narrowed_id, role=UserRole.OPERATOR)
    with pytest.raises(PermissionError):
        await service.get_or_create_oauth_delegate(
            _owner_principal(
                narrowed_id,
                permissions=[Permission.MANAGE_AI_INTEGRATION.value],
            ),
            request,
        )
    await repo.engine.dispose()


@pytest.mark.asyncio
async def test_oauth_delegate_by_grant_enforces_exact_role_creator_and_retires_families_on_dead_delegate(
    tmp_path: Path,
) -> None:
    """Verify live grant projection checks AI_DELEGATE role + creator and retires old families.

    Given a grant whose delegate linkage changes.
    When the live binding is projected.
    Then wrong roles or creators cannot authenticate and dead bindings retire all families.
    """
    repo = await _repository(tmp_path)
    owner_id = "00000000-0000-7000-8000-000000000230"
    await _seed_owner(repo, public_id=owner_id)
    service = DelegateService(repository=repo, token_manager=_fresh_token_manager())
    owner = _owner_principal(owner_id)
    request = OAuthDelegateRequest(
        client_id=_CLIENT_ID,
        resource=_RESOURCE,
        scopes=("snapper.read", "offline_access"),
    )
    created = await service.get_or_create_oauth_delegate(owner, request)
    await _seed_refresh_family(
        repo,
        grant_public_id=created.grant_public_id,
        family_public_id="00000000-0000-7000-8000-000000000231",
        token_public_id="00000000-0000-7000-8000-000000000232",
    )

    async with repo.session() as session:
        await session.execute(
            update(User)
            .where(User.public_id == created.delegate.public_id)
            .values(role=UserRole.AI_REVIEWER.value)
        )
        await session.commit()
    assert await service.get_oauth_delegate_by_grant(created.grant_public_id, _CLIENT_ID) is None

    async with repo.session() as session:
        await session.execute(
            update(User)
            .where(User.public_id == created.delegate.public_id)
            .values(
                role=UserRole.AI_DELEGATE.value,
                created_by_user_public_id="00000000-0000-7000-8000-000000000299",
            )
        )
        await session.commit()
    assert await service.get_oauth_delegate_by_grant(created.grant_public_id, _CLIENT_ID) is None

    replacement = await service.get_or_create_oauth_delegate(owner, request)
    assert replacement.grant_public_id != created.grant_public_id
    async with repo.session() as session:
        retired_token = (
            await session.execute(
                select(OAuthRefreshToken).where(
                    OAuthRefreshToken.public_id == "00000000-0000-7000-8000-000000000232"
                )
            )
        ).scalar_one()
    assert retired_token.revoked_at is not None
    await repo.engine.dispose()
