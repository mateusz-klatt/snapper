"""Tests for AI researcher principal provisioning."""

from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import MagicMock
from uuid import uuid7

import bcrypt
import pytest
from fastapi import HTTPException
from fastapi import Request
from sqlalchemy import select

from snapper.api.schemas.ai_delegates import DelegateCapsBody
from snapper.api.schemas.ai_delegates import DelegateCreateBody
from snapper.api.schemas.ai_researchers import ResearcherCreateBody
from snapper.api.schemas.ai_researchers import ResearcherCreateRequest
from snapper.application.ai_delegates.service import MAX_AI_DELEGATES_PER_OWNER
from snapper.application.ai_delegates.service import DelegateProliferationError
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_researchers.service import MAX_AI_RESEARCHERS_PER_OWNER
from snapper.application.ai_researchers.service import ResearcherProliferationError
from snapper.application.ai_researchers.service import ResearcherService
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AiDelegate
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import ai_researcher_routes
from snapper.server.app import create_app

_BCRYPT_FAKE_DIGEST = "$2b$12$" + "x" * 53


def _fresh_manager() -> TokenManager:
    """Return a newly initialized token-manager singleton."""
    TokenManager.clear_instance()
    TokenManager._initialized = False
    return TokenManager()


def _fake_gensalt() -> bytes:
    """Return a deterministic bcrypt salt for fast service tests."""
    return b"unused-test-salt"


def _fake_hashpw(_password: bytes, _salt: bytes) -> bytes:
    """Return a structurally valid deterministic password digest."""
    return _BCRYPT_FAKE_DIGEST.encode("utf-8")


@pytest.fixture(autouse=True)
def _fast_password_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace bcrypt work while preserving each service hashing call."""
    monkeypatch.setattr(bcrypt, "gensalt", _fake_gensalt)
    monkeypatch.setattr(bcrypt, "hashpw", _fake_hashpw)
    ResearcherService._owner_locks.clear()


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """Return an isolated in-memory repository with the complete schema."""
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repository.create_all()
    return repository


async def _seed_owner(
    repo: SQLAlchemyRepository,
    public_id: str,
    username: str,
) -> None:
    """Insert the operator that owns provisioned automation principals."""
    seed_time = datetime(2026, 1, 1, tzinfo=UTC)
    async with repo.session() as session:
        session.add(
            User(
                public_id=public_id,
                session_id="seed",
                sequence_id=1,
                timestamp=seed_time,
                known_to=KNOWN_TO_MAX,
                username=username,
                email=f"{username}@example.test",
                password_hash=_BCRYPT_FAKE_DIGEST,
                role=UserRole.OPERATOR.value,
                is_active=True,
                created_at=seed_time,
            )
        )
        await session.commit()


async def _seed_owned_users(
    repo: SQLAlchemyRepository,
    owner_public_id: str,
    role: UserRole,
    count: int,
) -> None:
    """Insert active owned users for a role-specific cap boundary."""
    seed_time = datetime(2026, 1, 2, tzinfo=UTC)
    async with repo.session() as session:
        for index in range(count):
            session.add(
                User(
                    public_id=str(uuid7()),
                    session_id="seed-owned",
                    sequence_id=index + 1,
                    timestamp=seed_time,
                    known_to=KNOWN_TO_MAX,
                    username=f"{role.value}-{owner_public_id}-{index}",
                    email=None,
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role=role.value,
                    is_active=True,
                    created_at=seed_time,
                    created_by_user_public_id=owner_public_id,
                )
            )
        await session.commit()


def _owner(
    user_public_id: str,
    operator_public_id: str = "operator-1",
) -> AuthPrincipal:
    """Build an operator principal eligible to provision automation users."""
    return AuthPrincipal(
        username="owner",
        role=UserRole.OPERATOR,
        user_public_id=user_public_id,
        operator_public_ids=[operator_public_id],
        primary_operator_public_id=operator_public_id,
    )


def _request() -> Request:
    """Build a request carrying the REST provenance tracker."""
    request = MagicMock(spec=Request)
    request.app.state.rest_tracker = SequenceTracker()
    return cast(Request, request)


def _create_request(
    label: str,
    permissions: list[Permission] | None = None,
) -> ResearcherCreateRequest:
    """Build a valid researcher creation request envelope."""
    return ResearcherCreateRequest(
        session_id="request-session",
        sequence_id=1,
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
        payload=ResearcherCreateBody(label=label, permissions=permissions),
    )


class TestResearcherProvisioning:
    """Researcher creation persists only identity and token inventory."""

    @pytest.mark.asyncio
    async def test_default_token_and_rows_are_research_only(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given an owner, provisioning creates only a researcher user and PAT.

        Given: An active operator and the default researcher role grant.
        When: The dedicated route provisions a researcher.
        Then: The token has the exact role grant and no trading, membership,
            or delegate runtime rows exist for the principal.
        """
        await _seed_owner(repo, "owner-default", "owner-default")
        manager = _fresh_manager()
        response = await ai_researcher_routes.create_researcher(
            request=_request(),
            body=_create_request("Macro Research"),
            owner=_owner("owner-default"),
            repo=repo,
            _csrf=None,
        )
        researcher = response.payload.researcher
        outcome = await manager.verify_token_with_reason(
            response.payload.access_token, repo, expected_token_type=TOKEN_TYPE_ACCESS
        )
        assert outcome.claims is not None
        assert outcome.rejection_reason is None
        assert outcome.claims.role == UserRole.AI_RESEARCHER
        assert set(outcome.claims.permissions or []) == {
            Permission.READ_MARKET_DATA.value,
            Permission.READ_MARKET_VIEWS.value,
            Permission.SUBMIT_MARKET_VIEW.value,
        }
        assert outcome.claims.operator_public_ids == []
        assert outcome.claims.primary_operator_public_id == ""
        assert researcher.username.startswith("ai-research-macroresearch-")
        assert researcher.created_by_user_public_id == "owner-default"
        async with repo.session() as session:
            users = (
                (await session.execute(select(User).where(User.public_id == researcher.public_id)))
                .scalars()
                .all()
            )
            tokens = (
                (
                    await session.execute(
                        select(UserActiveToken).where(
                            UserActiveToken.user_public_id == researcher.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            caps = (
                (
                    await session.execute(
                        select(UserTradingCaps).where(
                            UserTradingCaps.user_public_id == researcher.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            memberships = (
                (
                    await session.execute(
                        select(UserOperatorMembership).where(
                            UserOperatorMembership.user_public_id == researcher.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            delegate_rows = (
                (
                    await session.execute(
                        select(AiDelegate).where(AiDelegate.user_public_id == researcher.public_id)
                    )
                )
                .scalars()
                .all()
            )
        assert len(users) == 1
        assert users[0].role == UserRole.AI_RESEARCHER.value
        assert users[0].password_hash == _BCRYPT_FAKE_DIGEST
        assert len(tokens) == 1
        assert tokens[0].token_type == "access"
        assert tokens[0].token_hash == hash_token(response.payload.access_token)
        assert caps == []
        assert memberships == []
        assert delegate_rows == []
        assert await repo.get_ai_delegate_by_user_public_id(researcher.public_id) is None

    @pytest.mark.asyncio
    async def test_narrow_token_retains_only_requested_research_permission(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given a narrower grant, token minting cannot restore role permissions.

        Given: A researcher request scoped only to reading market views.
        When: The service mints and verifies its access token.
        Then: The token contains only that requested permission.
        """
        await _seed_owner(repo, "owner-narrow", "owner-narrow")
        manager = _fresh_manager()
        tracker = SequenceTracker()
        service = ResearcherService(repo, manager, tracker=tracker)
        payload = await service.create_researcher(
            owner=_owner("owner-narrow"),
            body=ResearcherCreateBody(
                label="Narrow",
                permissions=[Permission.READ_MARKET_VIEWS],
            ),
        )
        outcome = await manager.verify_token_with_reason(
            payload.access_token, repo, expected_token_type=TOKEN_TYPE_ACCESS
        )
        assert outcome.claims is not None
        assert outcome.claims.permissions == [Permission.READ_MARKET_VIEWS.value]
        assert service._tracker is tracker

    @pytest.mark.asyncio
    async def test_order_scope_is_rejected_and_transaction_rolls_back(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given an order permission, provisioning fails without a partial user.

        Given: A researcher request that asks for CREATE_ORDERS.
        When: The route validates the requested PAT permission ceiling.
        Then: It returns 422 and the flushed researcher user is rolled back.
        """
        await _seed_owner(repo, "owner-forbidden", "owner-forbidden")
        _fresh_manager()
        with pytest.raises(HTTPException) as exc:
            await ai_researcher_routes.create_researcher(
                request=_request(),
                body=_create_request("Forbidden", [Permission.CREATE_ORDERS]),
                owner=_owner("owner-forbidden"),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 422
        async with repo.session() as session:
            users = (
                (
                    await session.execute(
                        select(User).where(User.created_by_user_public_id == "owner-forbidden")
                    )
                )
                .scalars()
                .all()
            )
        assert users == []

    @pytest.mark.asyncio
    async def test_blank_owner_is_mapped_to_unauthorized(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given a legacy blank owner ID, the route rejects provisioning.

        Given: An authenticated principal without a stable user public ID.
        When: The researcher route invokes its service boundary.
        Then: It returns 401 before opening a provisioning transaction.
        """
        _fresh_manager()
        with pytest.raises(HTTPException) as exc:
            await ai_researcher_routes.create_researcher(
                request=_request(),
                body=_create_request("Blank"),
                owner=_owner(""),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 401

    def test_punctuation_only_label_uses_researcher_fallback(self) -> None:
        """Given punctuation-only text, normalization yields a stable label.

        Given: A label containing no username-safe characters.
        When: The service normalizes the label.
        Then: It uses the researcher fallback rather than an empty segment.
        """
        assert ResearcherService._slugify_label("!!!") == "researcher"


class TestIndependentProvisioningCaps:
    """Delegate and researcher proliferation limits remain independent."""

    @pytest.mark.asyncio
    async def test_full_delegate_cap_does_not_consume_researcher_slots(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given five delegates, two researchers remain independently available.

        Given: An owner already at the five-delegate cap.
        When: It provisions two researchers and attempts a third.
        Then: The first two succeed and only the researcher-specific cap rejects
            the third request with 409.
        """
        owner_public_id = "owner-delegate-cap"
        await _seed_owner(repo, owner_public_id, owner_public_id)
        await _seed_owned_users(
            repo,
            owner_public_id,
            UserRole.AI_DELEGATE,
            MAX_AI_DELEGATES_PER_OWNER,
        )
        manager = _fresh_manager()
        service = ResearcherService(repo, manager)
        for index in range(MAX_AI_RESEARCHERS_PER_OWNER):
            await service.create_researcher(
                owner=_owner(owner_public_id),
                body=ResearcherCreateBody(label=f"research-{index}"),
            )
        with pytest.raises(HTTPException) as exc:
            await ai_researcher_routes.create_researcher(
                request=_request(),
                body=_create_request("one-too-many"),
                owner=_owner(owner_public_id),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 409
        assert f"limit {MAX_AI_RESEARCHERS_PER_OWNER}" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_full_researcher_cap_does_not_consume_delegate_slots(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Given two researchers, the fifth delegate slot remains available.

        Given: An owner at the researcher cap with four active delegates.
        When: It provisions two additional delegates.
        Then: The fifth delegate succeeds and only the sixth hits the existing
            delegate-specific cap.
        """
        owner_public_id = "owner-researcher-cap"
        await _seed_owner(repo, owner_public_id, owner_public_id)
        await _seed_owned_users(
            repo,
            owner_public_id,
            UserRole.AI_RESEARCHER,
            MAX_AI_RESEARCHERS_PER_OWNER,
        )
        await _seed_owned_users(
            repo,
            owner_public_id,
            UserRole.AI_DELEGATE,
            MAX_AI_DELEGATES_PER_OWNER - 1,
        )
        service = DelegateService(repo, _fresh_manager())
        fifth = await service.create_delegate(
            owner=_owner(owner_public_id, "operator-cap"),
            body=DelegateCreateBody(label="fifth", caps=DelegateCapsBody()),
        )
        assert fifth.delegate.is_active is True
        with pytest.raises(DelegateProliferationError):
            await service.create_delegate(
                owner=_owner(owner_public_id, "operator-cap"),
                body=DelegateCreateBody(label="sixth", caps=DelegateCapsBody()),
            )


def test_researcher_router_is_mounted_on_api_app() -> None:
    """Given the server app, the dedicated researcher path is registered.

    Given: A freshly built FastAPI application.
    When: Its route paths are inspected.
    Then: POST provisioning is mounted at ``/api/ai-researchers``.
    """
    app = create_app()
    researcher_path = app.openapi()["paths"]["/api/ai-researchers"]
    assert set(researcher_path) == {"post"}


def test_researcher_proliferation_error_is_role_specific() -> None:
    """Given the new cap exception, it remains distinct from delegate errors.

    Given: The two provisioning proliferation exception classes.
    When: Their inheritance relationship is inspected.
    Then: Researcher failures cannot be mistaken for delegate-cap failures.
    """
    assert not issubclass(ResearcherProliferationError, DelegateProliferationError)
