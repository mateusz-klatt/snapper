"""Tests for AI delegate CRUD routes.

Exercises :class:`DelegateService` + the five ``/api/ai-delegates``
routes against a real in-memory SQLite DB so the SCD2 + caps
+ token-inventory atomicity guarantees are covered end-to-end.
"""

import asyncio
import json as _json_mod
from datetime import UTC
from datetime import datetime
from datetime import timedelta as _td
from typing import Any
from unittest.mock import MagicMock as _Magic
from unittest.mock import patch as _patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import select as _sel
from sqlalchemy import update as _up
from sqlalchemy.exc import IntegrityError

from snapper.api.schemas.ai_delegates import DelegateCapsBody
from snapper.api.schemas.ai_delegates import DelegateCapsUpdateBody
from snapper.api.schemas.ai_delegates import DelegateCapsUpdateRequest
from snapper.api.schemas.ai_delegates import DelegateCreateBody
from snapper.api.schemas.ai_delegates import DelegateCreatedPayload
from snapper.api.schemas.ai_delegates import DelegateCreateRequest
from snapper.api.schemas.ai_delegates import DelegateDeactivateBody
from snapper.api.schemas.ai_delegates import DelegateDeactivateRequest
from snapper.api.schemas.ai_delegates import DelegateRead
from snapper.application.ai_delegates.service import MAX_AI_DELEGATES_PER_OWNER
from snapper.application.ai_delegates.service import DelegateLabelConflictError
from snapper.application.ai_delegates.service import DelegateNotFoundError
from snapper.application.ai_delegates.service import DelegateOperatorBindingError
from snapper.application.ai_delegates.service import DelegateProliferationError
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import InvalidOwnerPrincipalError
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.data.models import UserActiveToken
from snapper.data.models import UserOperatorMembership
from snapper.data.models import UserTradingCaps
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import where_active_now
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import ai_delegate_routes
from snapper.server.ai_delegate_routes import _build_service

_BCRYPT_FAKE_DIGEST: str = "$2b$12$" + "x" * 53


def _fresh_manager() -> TokenManager:
    """Return a cleanly-initialised TokenManager singleton."""
    TokenManager.clear_instance()
    TokenManager._initialized = False
    return TokenManager()


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """In-memory aiosqlite repo with the schema applied."""
    r = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await r.create_all()
    return r


async def _seed_owner(repo: SQLAlchemyRepository, public_id: str, username: str) -> None:
    """Insert the operator that will own the delegates under test."""
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
                role="operator",
                is_active=True,
                created_at=seed_time,
            )
        )
        await s.commit()


def _make_owner_principal(
    user_public_id: str = "owner-1",
    operator_public_id: str = "operator-1",
) -> AuthPrincipal:
    return AuthPrincipal(
        username="owner",
        role=UserRole.OPERATOR,
        user_public_id=user_public_id,
        operator_public_ids=[operator_public_id],
        primary_operator_public_id=operator_public_id,
    )


class TestCreateDelegate:
    """Atomic create emits User + caps + token rows in one transaction."""

    @pytest.mark.asyncio
    async def test_happy_path_persists_user_caps_and_tokens(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """All four rows land in one commit + tokens surface once in the response."""
        await _seed_owner(repo, public_id="owner-1", username="owner")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(
            label="Research Bot",
            caps=DelegateCapsBody(max_open_orders=3, max_daily_notional_usd=1000.0),
        )
        payload = await service.create_delegate(owner=_make_owner_principal(), body=body)
        assert payload.access_token
        assert payload.expires_in > 0
        assert payload.delegate.is_active is True
        assert payload.delegate.username.startswith("ai-researchbot-")
        assert payload.delegate.created_by_user_public_id == "owner-1"
        assert payload.delegate.caps.max_open_orders == 3
        assert payload.delegate.caps.max_daily_notional_usd is not None
        assert abs(payload.delegate.caps.max_daily_notional_usd - 1000.0) < 1e-9
        async with repo.session() as s:

            users = (
                (await s.execute(_sel(User).where(User.public_id == payload.delegate.public_id)))
                .scalars()
                .all()
            )
            caps = (
                (
                    await s.execute(
                        _sel(UserTradingCaps).where(
                            UserTradingCaps.user_public_id == payload.delegate.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            tokens = (
                (
                    await s.execute(
                        _sel(UserActiveToken).where(
                            UserActiveToken.user_public_id == payload.delegate.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(users) == 1
        assert users[0].role == UserRole.AI_DELEGATE.value
        assert users[0].created_by_user_public_id == "owner-1"
        assert users[0].password_hash
        assert len(caps) == 1
        assert caps[0].max_open_orders == 3
        assert {t.token_type for t in tokens} == {"access"}
        token_hashes = {t.token_hash for t in tokens}
        assert hash_token(payload.access_token) in token_hashes

    def test_create_body_rejects_legacy_long_lived_field(self) -> None:
        """Schema guard: FastAPI maps this extra body field to HTTP 422."""
        with pytest.raises(ValidationError):
            DelegateCreateBody.model_validate({"label": "legacy", "caps": {}, "long_lived": True})

    @pytest.mark.asyncio
    async def test_all_none_caps_still_persist_caps_row(self, repo: SQLAlchemyRepository) -> None:
        """Every cap field ``None`` still writes a caps row (enforcer reads it)."""
        await _seed_owner(repo, public_id="owner-1", username="owner")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(label="Null Bot", caps=DelegateCapsBody())
        payload = await service.create_delegate(owner=_make_owner_principal(), body=body)
        async with repo.session() as s:

            caps = (
                (
                    await s.execute(
                        _sel(UserTradingCaps).where(
                            UserTradingCaps.user_public_id == payload.delegate.public_id
                        )
                    )
                )
                .scalars()
                .one()
            )
        assert caps.max_open_orders is None
        assert caps.max_daily_notional_usd is None

    @pytest.mark.asyncio
    async def test_delegate_tokens_pass_day3d_b_verify(self, repo: SQLAlchemyRepository) -> None:
        """The minted access token verifies against the inventory on next request.

        Proves the DB-backed verify path accepts
        freshly-minted delegate tokens — which is the core
        functional guarantee the atomic create flow must uphold
        (otherwise the delegate can't use the tokens at all).
        """
        await _seed_owner(repo, public_id="owner-2", username="owner2")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(label="verified", caps=DelegateCapsBody())
        payload = await service.create_delegate(owner=_make_owner_principal("owner-2"), body=body)
        outcome = await manager.verify_token_with_reason(payload.access_token, repo)
        assert outcome.claims is not None
        assert outcome.rejection_reason is None


class TestDelegateOperatorBinding:
    """Delegates inherit a validated operator scope.

    Pins the contract that ``create_delegate`` must mint JWTs whose
    ``operator_public_ids`` are populated — an empty list makes the
    MCP wallet-scope gate reject every freshly-created delegate on
    its first ``submit_manual_order``.
    """

    @pytest.mark.asyncio
    async def test_minted_tokens_carry_primary_operator_by_default(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Default path → delegate inherits caller's primary operator.

        Given: a caller with a single operator membership and no
            explicit ``operator_public_id`` in the create body,
        When: the delegate is minted,
        Then: the decoded access-token claims carry the primary
            operator both in ``operator_public_ids`` and
            ``primary_operator_public_id``.
        """
        await _seed_owner(repo, public_id="owner-bind-a", username="owner-bind-a")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(label="bind-default", caps=DelegateCapsBody())
        principal = _make_owner_principal("owner-bind-a", operator_public_id="op-primary")
        payload = await service.create_delegate(owner=principal, body=body)
        claims = manager.decode_fresh_token(payload.access_token)
        assert claims.operator_public_ids == ["op-primary"]
        assert claims.primary_operator_public_id == "op-primary"

    @pytest.mark.asyncio
    async def test_explicit_operator_in_claims_is_honoured(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Explicit in-set choice → delegate bound to that operator.

        Given: a caller who is a member of two operators and picks
            the non-primary one explicitly,
        When: the delegate is minted,
        Then: the minted claims reflect the explicit selection.
        """
        await _seed_owner(repo, public_id="owner-bind-b", username="owner-bind-b")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(
            label="bind-explicit",
            caps=DelegateCapsBody(),
            operator_public_id="op-secondary",
        )
        principal = AuthPrincipal(
            username="owner",
            role=UserRole.OPERATOR,
            user_public_id="owner-bind-b",
            operator_public_ids=["op-primary", "op-secondary"],
            primary_operator_public_id="op-primary",
        )
        payload = await service.create_delegate(owner=principal, body=body)
        claims = manager.decode_fresh_token(payload.access_token)
        assert claims.operator_public_ids == ["op-secondary"]
        assert claims.primary_operator_public_id == "op-secondary"

    @pytest.mark.asyncio
    async def test_operator_outside_claim_set_raises(self, repo: SQLAlchemyRepository) -> None:
        """Cross-operator pick → :class:`DelegateOperatorBindingError`.

        Given: a non-ADMIN caller who explicitly names an operator
            that is NOT in their authenticated set,
        When: the delegate create runs,
        Then: ``DelegateOperatorBindingError`` is raised and no
            User / caps / membership / token row is persisted.
        """
        await _seed_owner(repo, public_id="owner-bind-c", username="owner-bind-c")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(
            label="spoof",
            caps=DelegateCapsBody(),
            operator_public_id="op-NOT-MINE",
        )
        principal = _make_owner_principal("owner-bind-c", operator_public_id="op-primary")
        with pytest.raises(DelegateOperatorBindingError) as exc:
            await service.create_delegate(owner=principal, body=body)
        assert "op-NOT-MINE" in str(exc.value)

    @pytest.mark.asyncio
    async def test_admin_bypasses_claim_set_check(self, repo: SQLAlchemyRepository) -> None:
        """ADMIN caller may bind delegate to any operator.

        Given: an ADMIN caller with an empty operator claim set,
        When: they bind a delegate to a specific operator,
        Then: the binding is accepted because ADMIN implicitly
            spans every operator — matches the MCP wallet gate's
            ADMIN bypass.
        """
        await _seed_owner(repo, public_id="owner-bind-d", username="owner-bind-d")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(
            label="admin-pick",
            caps=DelegateCapsBody(),
            operator_public_id="op-any",
        )
        principal = AuthPrincipal(
            username="admin",
            role=UserRole.ADMIN,
            user_public_id="owner-bind-d",
        )
        payload = await service.create_delegate(owner=principal, body=body)
        claims = manager.decode_fresh_token(payload.access_token)
        assert claims.operator_public_ids == ["op-any"]

    @pytest.mark.asyncio
    async def test_no_explicit_and_no_primary_raises(self, repo: SQLAlchemyRepository) -> None:
        """Ambiguous binding → raise rather than pick silently.

        Given: a non-ADMIN caller with no primary operator and no
            explicit selection,
        When: the delegate create runs,
        Then: ``DelegateOperatorBindingError`` surfaces so the
            route can return 422 — delegates are not minted with
            ambiguous scope.
        """
        await _seed_owner(repo, public_id="owner-bind-e", username="owner-bind-e")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(label="no-op", caps=DelegateCapsBody())
        principal = AuthPrincipal(
            username="owner",
            role=UserRole.OPERATOR,
            user_public_id="owner-bind-e",
        )
        with pytest.raises(DelegateOperatorBindingError):
            await service.create_delegate(owner=principal, body=body)

    @pytest.mark.asyncio
    async def test_membership_row_is_persisted_atomically(self, repo: SQLAlchemyRepository) -> None:
        """Happy path persists the ``UserOperatorMembership`` row.

        Given: a successful create_delegate call,
        When: the DB is queried for active memberships of the new
            delegate,
        Then: exactly one active ``is_primary=True`` row exists
            pointing at the bound operator — proving the row
            landed in the same transaction as the User / caps /
            token rows.
        """
        await _seed_owner(repo, public_id="owner-bind-f", username="owner-bind-f")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(label="atomic-bind", caps=DelegateCapsBody())
        principal = _make_owner_principal("owner-bind-f", operator_public_id="op-bind")
        payload = await service.create_delegate(owner=principal, body=body)
        async with repo.session() as s:
            rows = (
                (
                    await s.execute(
                        _sel(UserOperatorMembership).where(
                            UserOperatorMembership.user_public_id == payload.delegate.public_id,
                            *where_active_now(UserOperatorMembership),
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].operator_public_id == "op-bind"
        assert rows[0].is_primary is True


class TestListDelegates:
    """List returns only active delegates owned by the caller."""

    @pytest.mark.asyncio
    async def test_empty_when_no_delegates(self, repo: SQLAlchemyRepository) -> None:
        """Fresh owner with no delegates → empty list."""
        await _seed_owner(repo, public_id="owner-a", username="a")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        delegates = await service.list_delegates(owner_public_id="owner-a")
        assert delegates == []

    @pytest.mark.asyncio
    async def test_only_owner_delegates_are_returned(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Delegates created by other operators don't leak into the caller's list."""
        await _seed_owner(repo, public_id="owner-mine", username="mine")
        await _seed_owner(repo, public_id="owner-other", username="other")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        monkeypatch.setattr(
            "snapper.application.ai_delegates.service._now_for_join",
            lambda: datetime.now(UTC) + _td(minutes=1),
        )
        await service.create_delegate(
            owner=_make_owner_principal("owner-mine"),
            body=DelegateCreateBody(label="Mine One", caps=DelegateCapsBody()),
        )
        await service.create_delegate(
            owner=_make_owner_principal("owner-other"),
            body=DelegateCreateBody(label="Theirs", caps=DelegateCapsBody()),
        )
        mine = await service.list_delegates(owner_public_id="owner-mine")
        theirs = await service.list_delegates(owner_public_id="owner-other")
        assert len(mine) == 1
        assert mine[0].label == "mineone"
        assert len(theirs) == 1


class TestGetDelegate:
    """Detail path 404s cleanly for both missing and cross-tenant cases."""

    @pytest.mark.asyncio
    async def test_happy_path(self, repo: SQLAlchemyRepository) -> None:
        """Owner reads their own delegate → populated projection."""
        await _seed_owner(repo, public_id="owner-g", username="g")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-g"),
            body=DelegateCreateBody(
                label="Readable",
                caps=DelegateCapsBody(max_open_orders=7),
            ),
        )
        read = await service.get_delegate(
            public_id=payload.delegate.public_id, owner_public_id="owner-g"
        )
        assert read.public_id == payload.delegate.public_id
        assert read.caps.max_open_orders == 7

    @pytest.mark.asyncio
    async def test_cross_tenant_returns_not_found(self, repo: SQLAlchemyRepository) -> None:
        """Different operator reading the same public_id → DelegateNotFoundError.

        Guards against tenant-existence leaks via probing: the
        error is identical to "no such delegate".
        """
        await _seed_owner(repo, public_id="owner-x", username="x")
        await _seed_owner(repo, public_id="owner-y", username="y")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-x"),
            body=DelegateCreateBody(label="xsdelegate", caps=DelegateCapsBody()),
        )
        with pytest.raises(DelegateNotFoundError):
            await service.get_delegate(
                public_id=payload.delegate.public_id, owner_public_id="owner-y"
            )


class TestUpdateCaps:
    """PATCH close+inserts new caps; owner guard applies."""

    @pytest.mark.asyncio
    async def test_updates_caps_via_scd2_close_and_insert(self, repo: SQLAlchemyRepository) -> None:
        """New caps land as a fresh active row; the prior row is closed."""
        await _seed_owner(repo, public_id="owner-u", username="u")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        created = await service.create_delegate(
            owner=_make_owner_principal("owner-u"),
            body=DelegateCreateBody(
                label="CapUp",
                caps=DelegateCapsBody(max_open_orders=1),
            ),
        )
        updated = await service.update_caps(
            public_id=created.delegate.public_id,
            owner_public_id="owner-u",
            body=DelegateCapsUpdateBody(caps=DelegateCapsBody(max_open_orders=42)),
        )
        assert updated.caps.max_open_orders == 42
        async with repo.session() as s:

            all_caps = (
                (
                    await s.execute(
                        _sel(UserTradingCaps).where(
                            UserTradingCaps.user_public_id == created.delegate.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(all_caps) == 2
        active_count = sum(1 for c in all_caps if c.known_to == KNOWN_TO_MAX)
        assert active_count == 1

    @pytest.mark.asyncio
    async def test_cross_tenant_update_returns_not_found(self, repo: SQLAlchemyRepository) -> None:
        """Cross-tenant PATCH fails with DelegateNotFoundError — caps unchanged."""
        await _seed_owner(repo, public_id="owner-u1", username="u1")
        await _seed_owner(repo, public_id="owner-u2", username="u2")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        created = await service.create_delegate(
            owner=_make_owner_principal("owner-u1"),
            body=DelegateCreateBody(
                label="OrigCaps",
                caps=DelegateCapsBody(max_open_orders=9),
            ),
        )
        with pytest.raises(DelegateNotFoundError):
            await service.update_caps(
                public_id=created.delegate.public_id,
                owner_public_id="owner-u2",
                body=DelegateCapsUpdateBody(caps=DelegateCapsBody(max_open_orders=99)),
            )


class TestBlankOwnerGuard:
    """Empty ``owner.user_public_id`` is rejected at every entry point."""

    @pytest.mark.asyncio
    async def test_create_with_blank_owner_raises_invalid_principal(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Empty ``owner.user_public_id`` blocks create at the service boundary."""
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        with pytest.raises(InvalidOwnerPrincipalError):
            await service.create_delegate(
                owner=_make_owner_principal(""),
                body=DelegateCreateBody(label="blank", caps=DelegateCapsBody()),
            )

    @pytest.mark.asyncio
    async def test_list_with_blank_owner_raises_invalid_principal(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """List refuses blank owner IDs — no cross-tenant leak."""
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        with pytest.raises(InvalidOwnerPrincipalError):
            await service.list_delegates(owner_public_id="")

    @pytest.mark.asyncio
    async def test_get_with_blank_owner_raises_invalid_principal(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Detail refuses blank owner IDs even when the delegate public_id is set."""
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        with pytest.raises(InvalidOwnerPrincipalError):
            await service.get_delegate(public_id="whatever", owner_public_id="")

    @pytest.mark.asyncio
    async def test_update_with_blank_owner_raises_invalid_principal(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """PATCH refuses blank owner IDs."""
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        with pytest.raises(InvalidOwnerPrincipalError):
            await service.update_caps(
                public_id="whatever",
                owner_public_id="",
                body=DelegateCapsUpdateBody(caps=DelegateCapsBody()),
            )

    @pytest.mark.asyncio
    async def test_create_route_converts_invalid_principal_to_401(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Route layer maps ``InvalidOwnerPrincipalError`` → 401 with detail."""

        class _BlankService:
            async def create_delegate(
                self, owner: AuthPrincipal, body: DelegateCreateBody
            ) -> DelegateCreatedPayload:
                raise InvalidOwnerPrincipalError("blank principal")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _BlankService())
        request = _make_rest_request()
        body = DelegateCreateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCreateBody(label="anything", caps=DelegateCapsBody()),
        )
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.create_delegate(
                request=request,
                body=body,
                owner=_make_owner_principal(""),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_list_route_converts_invalid_principal_to_401(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """List route maps ``InvalidOwnerPrincipalError`` → 401."""

        class _BlankService:
            async def list_delegates(self, owner_public_id: str) -> list[DelegateRead]:
                raise InvalidOwnerPrincipalError("blank principal")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _BlankService())
        request = _make_rest_request()
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.list_delegates(
                request=request,
                owner=_make_owner_principal(""),
                repo=repo,
            )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_get_route_converts_invalid_principal_to_401(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GET route maps ``InvalidOwnerPrincipalError`` → 401."""

        class _BlankService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                raise InvalidOwnerPrincipalError("blank principal")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _BlankService())
        request = _make_rest_request()
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.get_delegate(
                request=request,
                delegate_public_id="whatever",
                owner=_make_owner_principal(""),
                repo=repo,
            )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_patch_route_converts_invalid_principal_to_401(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PATCH route maps ``InvalidOwnerPrincipalError`` → 401."""

        class _BlankService:
            async def update_caps(
                self, public_id: str, owner_public_id: str, body: DelegateCapsUpdateBody
            ) -> DelegateRead:
                raise InvalidOwnerPrincipalError("blank principal")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _BlankService())
        request = _make_rest_request()
        body = DelegateCapsUpdateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCapsUpdateBody(caps=DelegateCapsBody()),
        )
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.update_delegate_caps(
                request=request,
                delegate_public_id="whatever",
                body=body,
                owner=_make_owner_principal(""),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 401

    @pytest.mark.asyncio
    async def test_deactivate_route_converts_invalid_principal_to_401(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deactivate route maps ``InvalidOwnerPrincipalError`` → 401.

        The pre-load via ``get_delegate`` raises on a blank owner;
        the route must map that to 401 before ever touching
        ``UserService.deactivate_user``.
        """

        class _BlankService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                raise InvalidOwnerPrincipalError("blank principal")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _BlankService())
        request = _make_rest_request()
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.deactivate_delegate(
                request=request,
                delegate_public_id="whatever",
                body=None,
                owner=_make_owner_principal(""),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 401


class TestDelegateProliferationCap:
    """Bound delegates per operator."""

    @pytest.mark.asyncio
    async def test_sixth_create_raises_proliferation_error(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Creating the 6th active delegate fails with DelegateProliferationError."""
        await _seed_owner(repo, public_id="owner-cap", username="cap")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        for i in range(MAX_AI_DELEGATES_PER_OWNER):
            await service.create_delegate(
                owner=_make_owner_principal("owner-cap"),
                body=DelegateCreateBody(label=f"d-{i}", caps=DelegateCapsBody()),
            )
        with pytest.raises(DelegateProliferationError):
            await service.create_delegate(
                owner=_make_owner_principal("owner-cap"),
                body=DelegateCreateBody(label="one-too-many", caps=DelegateCapsBody()),
            )

    @pytest.mark.asyncio
    async def test_deactivated_delegates_do_not_count_against_cap(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Rotations stay unbounded: deactivating frees a slot for a fresh mint."""
        await _seed_owner(repo, public_id="owner-rot", username="rot")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        first = await service.create_delegate(
            owner=_make_owner_principal("owner-rot"),
            body=DelegateCreateBody(label="first", caps=DelegateCapsBody()),
        )
        for i in range(MAX_AI_DELEGATES_PER_OWNER - 1):
            await service.create_delegate(
                owner=_make_owner_principal("owner-rot"),
                body=DelegateCreateBody(label=f"f-{i}", caps=DelegateCapsBody()),
            )
        async with repo.session() as s:

            await s.execute(
                _up(User).where(User.public_id == first.delegate.public_id).values(is_active=False)
            )
            await s.commit()
        replacement = await service.create_delegate(
            owner=_make_owner_principal("owner-rot"),
            body=DelegateCreateBody(label="replacement", caps=DelegateCapsBody()),
        )
        assert replacement.delegate.is_active is True

    @pytest.mark.asyncio
    async def test_cap_applies_per_owner_not_global(self, repo: SQLAlchemyRepository) -> None:
        """The cap is scoped per ``created_by_user_public_id``."""
        await _seed_owner(repo, public_id="owner-a", username="a")
        await _seed_owner(repo, public_id="owner-b", username="b")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        for i in range(MAX_AI_DELEGATES_PER_OWNER):
            await service.create_delegate(
                owner=_make_owner_principal("owner-a"),
                body=DelegateCreateBody(label=f"a{i}", caps=DelegateCapsBody()),
            )
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-b"),
            body=DelegateCreateBody(label="b-slot-1", caps=DelegateCapsBody()),
        )
        assert payload.delegate.is_active is True

    @pytest.mark.asyncio
    async def test_create_route_maps_proliferation_error_to_409(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """HTTP surface: 409 with the cap message when proliferation fires."""

        class _ProliferationService:
            async def create_delegate(
                self, owner: AuthPrincipal, body: DelegateCreateBody
            ) -> DelegateCreatedPayload:
                raise DelegateProliferationError("limit reached")

        monkeypatch.setattr(
            ai_delegate_routes, "_build_service", lambda _repo: _ProliferationService()
        )
        request = _make_rest_request()
        body = DelegateCreateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCreateBody(label="hit-cap", caps=DelegateCapsBody()),
        )
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.create_delegate(
                request=request,
                body=body,
                owner=_make_owner_principal(),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 409
        assert "limit reached" in exc.value.detail


class TestDelegateProliferationConcurrency:
    """R2 closure — proliferation cap survives concurrent creates.

    Before the row-lock fix, two parallel ``POST /api/ai-delegates``
    calls from the same owner could both read the same pre-insert
    count and both commit, minting ``MAX + N`` delegates. The fix
    serialises on a ``SELECT ... FOR UPDATE`` of the owner's User
    row, which PostgreSQL honours as a real lock and SQLite
    promotes to its write lock. Either way, the count → guard →
    insert sequence is atomic per-owner.
    """

    @pytest.mark.asyncio
    async def test_parallel_creates_respect_cap(self, repo: SQLAlchemyRepository) -> None:
        """MAX_AI_DELEGATES_PER_OWNER + 2 concurrent creates = 5 rows, not 7.

        Given: an operator at the cap boundary with
            ``MAX_AI_DELEGATES_PER_OWNER`` slots available,
        When: ``MAX + 2`` create calls fire concurrently via
            :func:`asyncio.gather`,
        Then: exactly ``MAX`` delegates are persisted and the
            extra attempts raise :class:`DelegateProliferationError`.
            Confirms the row-lock serialises the count-then-insert
            under concurrent load.
        """
        await _seed_owner(repo, public_id="owner-race", username="owner-race")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        owner = _make_owner_principal("owner-race", operator_public_id="op-race")

        async def _attempt(slot: int) -> bool:
            body = DelegateCreateBody(
                label=f"concurrent-{slot:02d}",
                caps=DelegateCapsBody(),
            )
            try:
                await service.create_delegate(owner=owner, body=body)
                return True
            except DelegateProliferationError:
                return False

        attempts = MAX_AI_DELEGATES_PER_OWNER + 2
        outcomes = await asyncio.gather(*[_attempt(i) for i in range(attempts)])
        admitted = sum(1 for ok in outcomes if ok)
        rejected = sum(1 for ok in outcomes if not ok)
        assert admitted == MAX_AI_DELEGATES_PER_OWNER
        assert rejected == 2
        persisted = await service.list_delegates(owner_public_id="owner-race")
        assert len(persisted) == MAX_AI_DELEGATES_PER_OWNER


class TestCreateDelegateInsertFailureRollsBackAtomically:
    """Codex R1 NICE-TO-HAVE — pin the transactional rollback invariant."""

    @pytest.mark.asyncio
    async def test_token_insert_conflict_rolls_back_user_and_caps(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Token-hash collision mid-transaction → IntegrityError → full rollback.

        Patches :func:`snapper.application.ai_delegates.service.hash_token`
        to a constant value, pre-seeds a ``user_active_tokens`` row
        with that hash, then attempts to create a delegate. The
        service's token INSERT collides on the ``uq_user_active_tokens_token_hash``
        constraint after the User + UserTradingCaps rows have
        flushed. The outer ``async with session()`` scope must roll
        back so NO user / caps / token rows persist.
        """
        await _seed_owner(repo, public_id="owner-rb", username="rb")
        now = datetime.now(UTC)
        collision_hash = "deadbeef" * 8
        async with repo.session() as s:
            s.add(
                UserActiveToken(
                    public_id="pub-collision",
                    user_public_id="preview-user",
                    jti="jti-collision",
                    token_hash=collision_hash,
                    token_type="access",
                    issued_at=now,
                    expires_at=now + _td(minutes=15),
                )
            )
            await s.commit()
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        principal = _make_owner_principal("owner-rb")
        with (
            _patch(
                "snapper.application.ai_delegates.service.hash_token",
                return_value=collision_hash,
            ),
            pytest.raises(IntegrityError),
        ):
            await service.create_delegate(
                owner=principal,
                body=DelegateCreateBody(label="rollback", caps=DelegateCapsBody()),
            )
        async with repo.session() as s:
            users = (
                (await s.execute(_sel(User).where(User.created_by_user_public_id == "owner-rb")))
                .scalars()
                .all()
            )
            created_user_ids = [u.public_id for u in users]
            leaked_caps = (
                (
                    await s.execute(
                        _sel(UserTradingCaps).where(
                            UserTradingCaps.user_public_id.in_(created_user_ids + ["__never__"])
                        )
                    )
                )
                .scalars()
                .all()
            )
            leaked_tokens = (
                (
                    await s.execute(
                        _sel(UserActiveToken).where(
                            UserActiveToken.user_public_id.in_(created_user_ids + ["__never__"])
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert users == []
        assert leaked_caps == []
        assert leaked_tokens == []


class TestBuildServiceFactory:
    """``_build_service`` returns a wired DelegateService for the route layer."""

    def test_build_service_returns_delegate_service_instance(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Factory wires repository + TokenManager into a live service."""
        _fresh_manager()
        service = _build_service(repo)
        assert isinstance(service, DelegateService)
        assert service.repository is repo


class TestServiceEdgeCases:
    """Static helper + label-conflict coverage."""

    @pytest.mark.asyncio
    async def test_label_conflict_raises_after_exhausted_retries(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Every generated username collides → DelegateLabelConflictError.

        Planted 9+ SCD2-active users that match ``ai-collider-*``
        would require astronomical luck; instead we stub
        ``_random_suffix`` to a constant so every retry collides
        on the first SQL lookup against a single seeded row.
        """
        await _seed_owner(repo, public_id="owner-c", username="owner-c")
        async with repo.session() as s:
            s.add(
                User(
                    public_id="collider-delegate",
                    session_id="seed",
                    sequence_id=2,
                    timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                    known_to=KNOWN_TO_MAX,
                    username="ai-collider-fixed1",
                    email="col@example.test",
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role="ai_delegate",
                    is_active=True,
                    created_at=datetime(2026, 1, 1, tzinfo=UTC),
                )
            )
            await s.commit()
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        with (
            _patch.object(DelegateService, "_random_suffix", staticmethod(lambda: "fixed1")),
            pytest.raises(DelegateLabelConflictError),
        ):
            await service.create_delegate(
                owner=_make_owner_principal("owner-c"),
                body=DelegateCreateBody(label="Collider", caps=DelegateCapsBody()),
            )

    def test_slugify_empty_label_returns_fallback(self) -> None:
        """Degenerate labels fall back to ``delegate``."""
        assert DelegateService._slugify_label("!!!") == "delegate"

    def test_label_from_username_plain_string_untouched(self) -> None:
        """Usernames that don't match ``ai-<slug>-<suffix>`` pass through."""
        assert DelegateService._label_from_username("plain") == "plain"

    def test_label_from_username_missing_suffix_returns_stripped(self) -> None:
        """Username lacking a trailing ``-<suffix>`` returns just the stripped form."""
        assert DelegateService._label_from_username("ai-label") == "label"

    def test_coerce_caps_none_row_returns_none(self) -> None:
        """Missing caps row → None (no caps configured)."""
        assert DelegateService._coerce_caps_json(None) is None

    def test_coerce_caps_none_column_returns_none(self) -> None:
        """Caps row exists but the JSON column is NULL → None."""
        caps_row = _Magic()
        caps_row.max_order_quantity_per_instrument = None
        assert DelegateService._coerce_caps_json(caps_row) is None

    def test_coerce_caps_populated_column_passes_through(self) -> None:
        """Populated JSON dict is returned verbatim."""
        caps_row = _Magic()
        caps_row.max_order_quantity_per_instrument = {"BTC-USD": 0.5}
        assert DelegateService._coerce_caps_json(caps_row) == {"BTC-USD": 0.5}


class TestAiDelegatesFeatureFlagGate:
    """`/api/ai-delegates/*` shares the MCP feature flag.

    The delegate management surface must refuse every request when
    ``ai_integration_enabled`` is off, parity with the ``/api/mcp``
    sub-app's :class:`FeatureFlagMiddleware`. Without the gate,
    operators could mint delegates + tokens on an instance where the
    rest of the AI surface is disabled.
    """

    def test_flag_off_raises_custom_error(self) -> None:
        """Flag off → :class:`AiIntegrationDisabledError` raised.

        Given: a settings service whose ``get_setting`` returns
            ``False`` for ``ai_integration_enabled``,
        When: the dependency runs,
        Then: it raises :class:`AiIntegrationDisabledError` so the
            app-level handler can translate it to the structured
            503 envelope that mirrors the MCP sub-app.
        """
        settings_service = _Magic()
        settings_service.get_setting.return_value = False
        request = _Magic()
        request.app.state.settings_service = settings_service
        with pytest.raises(ai_delegate_routes.AiIntegrationDisabledError) as exc:
            ai_delegate_routes.require_ai_integration_enabled(request)
        assert "AI integration is disabled" in str(exc.value)

    def test_flag_on_returns_none(self) -> None:
        """Flag on → the dependency passes through silently.

        Given: the setting resolves to ``True``,
        When: the dependency runs,
        Then: it returns ``None`` so the downstream route body runs.
        """
        settings_service = _Magic()
        settings_service.get_setting.return_value = True
        request = _Magic()
        request.app.state.settings_service = settings_service
        ai_delegate_routes.require_ai_integration_enabled(request)

    def test_settings_service_missing_defaults_to_on(self) -> None:
        """Pre-lifespan startup (no settings service) → default-on.

        Given: ``app.state.settings_service`` is absent (lifespan
            has not populated the singleton yet),
        When: the dependency runs,
        Then: it returns ``None`` because the flag defaults to
            enabled — matches the MCP middleware's default-on
            startup behaviour.
        """
        request = _Magic()
        request.app.state = _Magic(spec=["rest_tracker"])
        ai_delegate_routes.require_ai_integration_enabled(request)

    def test_flag_absent_defaults_to_on(self) -> None:
        """Settings service returns the ``default`` kwarg → default-on.

        Given: ``get_setting`` is called with
            ``default=True`` and the key is absent from the DB so
            the default is returned,
        When: the dependency runs,
        Then: it returns ``None`` and the downstream route runs.
        """
        settings_service = _Magic()
        settings_service.get_setting.side_effect = lambda key, default=None: default
        request = _Magic()
        request.app.state.settings_service = settings_service
        ai_delegate_routes.require_ai_integration_enabled(request)
        settings_service.get_setting.assert_called_with("ai_integration_enabled", default=True)

    def test_handler_returns_mcp_parity_envelope(self) -> None:
        """Rfollowup (gpt-5.4): envelope must match MCP 503 parity.

        Given: an :class:`AiIntegrationDisabledError`,
        When: :func:`ai_integration_disabled_handler` translates it,
        Then: the 503 :class:`JSONResponse` body is
            ``{"error_code": "feature_disabled", "detail": "..."}``
            — EXACTLY the envelope MCP's
            :class:`FeatureFlagMiddleware` emits. No nested
            ``{"detail": ...}`` wrapper FastAPI would have produced
            for a raw ``HTTPException``.
        """
        exc = ai_delegate_routes.AiIntegrationDisabledError("AI integration is disabled.")
        response = ai_delegate_routes.ai_integration_disabled_handler(_Magic(), exc)
        assert response.status_code == 503
        body = _json_mod.loads(bytes(response.body).decode("utf-8"))
        assert body == {
            "error_code": "feature_disabled",
            "detail": "AI integration is disabled.",
        }


class TestRouteHandlers:
    """Direct invocation of route functions for 409 / 404 / happy path coverage."""

    @pytest.mark.asyncio
    async def test_create_route_converts_operator_binding_error_to_422(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``DelegateOperatorBindingError`` → 422 HTTPException.

        Given: the service raises ``DelegateOperatorBindingError``
            because the caller picked an operator outside their
            claim set,
        When: the route runs,
        Then: HTTP 422 surfaces with the exception detail so clients
            can self-correct with a valid operator selection.
        """

        class _BindService:
            async def create_delegate(
                self, owner: AuthPrincipal, body: DelegateCreateBody
            ) -> DelegateCreatedPayload:
                await asyncio.sleep(0)
                raise DelegateOperatorBindingError("Operator 'op-NOT-MINE' is not in claim set.")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _BindService())
        request = _make_rest_request()
        body = DelegateCreateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCreateBody(
                label="spoofed",
                caps=DelegateCapsBody(),
                operator_public_id="op-NOT-MINE",
            ),
        )
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.create_delegate(
                request=request,
                body=body,
                owner=_make_owner_principal(),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 422
        assert "op-NOT-MINE" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_create_route_converts_label_conflict_to_409(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``DelegateLabelConflictError`` from the service → 409 HTTPException."""

        class _ConflictService:
            async def create_delegate(
                self, owner: AuthPrincipal, body: DelegateCreateBody
            ) -> DelegateCreatedPayload:
                await asyncio.sleep(0)
                raise DelegateLabelConflictError("no slugs available")

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _ConflictService())
        request = _make_rest_request()
        body = DelegateCreateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCreateBody(label="anything", caps=DelegateCapsBody()),
        )
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.create_delegate(
                request=request,
                body=body,
                owner=_make_owner_principal(),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_get_route_404_on_delegate_not_found(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``DelegateNotFoundError`` → 404 HTTPException."""

        class _MissingService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                await asyncio.sleep(0)
                raise DelegateNotFoundError(public_id)

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _MissingService())
        request = _make_rest_request()
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.get_delegate(
                request=request,
                delegate_public_id="does-not-exist",
                owner=_make_owner_principal(),
                repo=repo,
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_patch_route_404_on_delegate_not_found(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PATCH 404 path for cross-tenant / missing delegate."""

        class _MissingService:
            async def update_caps(
                self, public_id: str, owner_public_id: str, body: DelegateCapsUpdateBody
            ) -> DelegateRead:
                await asyncio.sleep(0)
                raise DelegateNotFoundError(public_id)

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _MissingService())
        request = _make_rest_request()
        body = DelegateCapsUpdateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCapsUpdateBody(caps=DelegateCapsBody()),
        )
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.update_delegate_caps(
                request=request,
                delegate_public_id="nope",
                body=body,
                owner=_make_owner_principal(),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_list_route_returns_empty_envelope(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path list returns a typed envelope with count=0."""

        class _EmptyService:
            async def list_delegates(self, owner_public_id: str) -> list[DelegateRead]:
                await asyncio.sleep(0)
                return []

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _EmptyService())
        request = _make_rest_request()
        response = await ai_delegate_routes.list_delegates(
            request=request,
            owner=_make_owner_principal(),
            repo=repo,
        )
        assert response.count == 0
        assert response.payload == []

    @pytest.mark.asyncio
    async def test_get_route_returns_delegate_response(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path GET wraps the service's delegate in a typed envelope."""
        delegate = DelegateRead(
            public_id="d1",
            username="ai-happy-abc123",
            label="happy",
            created_by_user_public_id="owner-1",
            created_at=datetime.now(UTC),
            is_active=True,
            caps=DelegateCapsBody(),
        )

        class _OkService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                await asyncio.sleep(0)
                return delegate

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _OkService())
        request = _make_rest_request()
        response = await ai_delegate_routes.get_delegate(
            request=request,
            delegate_public_id="d1",
            owner=_make_owner_principal(),
            repo=repo,
        )
        assert response.payload.public_id == "d1"

    @pytest.mark.asyncio
    async def test_patch_route_returns_updated_delegate(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path PATCH returns the updated delegate with new caps."""
        updated = DelegateRead(
            public_id="d2",
            username="ai-u-abc123",
            label="u",
            created_by_user_public_id="owner-1",
            created_at=datetime.now(UTC),
            is_active=True,
            caps=DelegateCapsBody(max_open_orders=42),
        )

        class _OkService:
            async def update_caps(
                self, public_id: str, owner_public_id: str, body: DelegateCapsUpdateBody
            ) -> DelegateRead:
                await asyncio.sleep(0)
                return updated

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _OkService())
        request = _make_rest_request()
        body = DelegateCapsUpdateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCapsUpdateBody(caps=DelegateCapsBody(max_open_orders=42)),
        )
        response = await ai_delegate_routes.update_delegate_caps(
            request=request,
            delegate_public_id="d2",
            body=body,
            owner=_make_owner_principal(),
            repo=repo,
            _csrf=None,
        )
        assert response.payload.caps.max_open_orders == 42

    @pytest.mark.asyncio
    async def test_create_route_happy_path_wraps_payload(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path CREATE returns the one-shot token payload."""
        delegate = DelegateRead(
            public_id="d3",
            username="ai-fresh-abc123",
            label="fresh",
            created_by_user_public_id="owner-1",
            created_at=datetime.now(UTC),
            is_active=True,
            caps=DelegateCapsBody(),
        )

        class _OkService:
            async def create_delegate(
                self, owner: AuthPrincipal, body: DelegateCreateBody
            ) -> DelegateCreatedPayload:
                await asyncio.sleep(0)
                return DelegateCreatedPayload(
                    delegate=delegate,
                    access_token="tok.access",
                    expires_in=900,
                )

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _OkService())
        request = _make_rest_request()
        body = DelegateCreateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateCreateBody(label="fresh", caps=DelegateCapsBody()),
        )
        response = await ai_delegate_routes.create_delegate(
            request=request,
            body=body,
            owner=_make_owner_principal(),
            repo=repo,
            _csrf=None,
        )
        assert response.payload.access_token == "tok.access"
        assert response.payload.delegate.public_id == "d3"

    @pytest.mark.asyncio
    async def test_deactivate_route_404_when_service_reports_not_found(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deactivate 404 path when the pre-load returns DelegateNotFoundError."""

        class _MissingService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                await asyncio.sleep(0)
                raise DelegateNotFoundError(public_id)

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _MissingService())
        request = _make_rest_request()
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.deactivate_delegate(
                request=request,
                delegate_public_id="nope",
                body=None,
                owner=_make_owner_principal(),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_deactivate_route_calls_user_service_and_returns_inactive(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy-path deactivate delegates to ``UserService.deactivate_user``."""
        delegate = DelegateRead(
            public_id="d4",
            username="ai-bye-abc123",
            label="bye",
            created_by_user_public_id="owner-1",
            created_at=datetime.now(UTC),
            is_active=True,
            caps=DelegateCapsBody(),
        )

        class _FoundService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                await asyncio.sleep(0)
                return delegate

        class _OkUserService:
            async def deactivate_user(self, user_public_id: str, reason: str | None) -> bool:
                await asyncio.sleep(0)
                return True

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _FoundService())
        monkeypatch.setattr(ai_delegate_routes, "get_user_service", lambda: _OkUserService())
        request = _make_rest_request()
        body = DelegateDeactivateRequest(
            session_id="s",
            sequence_id=1,
            public_id="p",
            timestamp=datetime.now(UTC),
            payload=DelegateDeactivateBody(reason="ops rotation"),
        )
        response = await ai_delegate_routes.deactivate_delegate(
            request=request,
            delegate_public_id="d4",
            body=body,
            owner=_make_owner_principal(),
            repo=repo,
            _csrf=None,
        )
        assert response.payload.is_active is False

    @pytest.mark.asyncio
    async def test_deactivate_route_404_when_user_service_returns_false(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Race: delegate existed at load but was deactivated by a concurrent caller."""
        delegate = DelegateRead(
            public_id="d5",
            username="ai-race-abc123",
            label="race",
            created_by_user_public_id="owner-1",
            created_at=datetime.now(UTC),
            is_active=True,
            caps=DelegateCapsBody(),
        )

        class _FoundService:
            async def get_delegate(self, public_id: str, owner_public_id: str) -> DelegateRead:
                await asyncio.sleep(0)
                return delegate

        class _RaceUserService:
            async def deactivate_user(self, user_public_id: str, reason: str | None) -> bool:
                await asyncio.sleep(0)
                return False

        monkeypatch.setattr(ai_delegate_routes, "_build_service", lambda _repo: _FoundService())
        monkeypatch.setattr(ai_delegate_routes, "get_user_service", lambda: _RaceUserService())
        request = _make_rest_request()
        with pytest.raises(HTTPException) as exc:
            await ai_delegate_routes.deactivate_delegate(
                request=request,
                delegate_public_id="d5",
                body=None,
                owner=_make_owner_principal(),
                repo=repo,
                _csrf=None,
            )
        assert exc.value.status_code == 404


def _make_rest_request() -> Any:
    """Mock FastAPI Request with a real SequenceTracker."""
    request = _Magic()
    request.app.state.rest_tracker = SequenceTracker()
    return request


class TestDelegateAccessTokens:
    """Single-token delegate credentials."""

    @pytest.mark.asyncio
    async def test_create_delegate_persists_single_access_token_row(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Create writes exactly one UserActiveToken row with ``token_type='access'``."""
        await _seed_owner(repo, public_id="owner-pat-1", username="owner-pat-1")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(label="Token Bot", caps=DelegateCapsBody())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-pat-1"), body=body
        )
        async with repo.session() as s:
            tokens = (
                (
                    await s.execute(
                        _sel(UserActiveToken).where(
                            UserActiveToken.user_public_id == payload.delegate.public_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert len(tokens) == 1
        assert tokens[0].token_type == "access"
        assert tokens[0].token_hash == hash_token(payload.access_token)
        ninety_days = _td(days=90)
        observed_lifetime = tokens[0].expires_at - tokens[0].issued_at
        assert abs((observed_lifetime - ninety_days).total_seconds()) < 60

    @pytest.mark.asyncio
    async def test_create_delegate_payload_shape(self, repo: SQLAlchemyRepository) -> None:
        """Create response contains the delegate, access token, and long expiry only."""
        await _seed_owner(repo, public_id="owner-pat-2", username="owner-pat-2")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        body = DelegateCreateBody(label="Token Two", caps=DelegateCapsBody())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-pat-2"), body=body
        )
        assert payload.access_token
        assert payload.expires_in > 5_000_000
        assert payload.delegate.public_id

    @pytest.mark.asyncio
    async def test_delegate_access_token_decode_roundtrip(self, repo: SQLAlchemyRepository) -> None:
        """Decoded JWT's ``exp - iat`` equals roughly three months in seconds."""
        await _seed_owner(repo, public_id="owner-pat-4", username="owner-pat-4")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(label="Decode", caps=DelegateCapsBody())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-pat-4"), body=body
        )
        claims = manager.decode_fresh_token(payload.access_token)
        lifetime_seconds = claims.exp - claims.iat
        ninety_days_seconds = 90 * 86400
        assert abs(lifetime_seconds - ninety_days_seconds) < 120

    @pytest.mark.asyncio
    async def test_delegate_jti_not_refresh_prefixed(self, repo: SQLAlchemyRepository) -> None:
        """Delegate access-token JTIs never carry the ``refresh_`` prefix."""
        await _seed_owner(repo, public_id="owner-pat-5", username="owner-pat-5")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(label="Jti", caps=DelegateCapsBody())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-pat-5"), body=body
        )
        claims = manager.decode_fresh_token(payload.access_token)
        assert not claims.jti.startswith("refresh_")

    @pytest.mark.asyncio
    async def test_delegate_read_projection_has_current_shape(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Read projections expose identity and caps without credential metadata."""
        await _seed_owner(repo, public_id="owner-pat-7", username="owner-pat-7")
        service = DelegateService(repository=repo, token_manager=_fresh_manager())
        monkeypatch.setattr(
            "snapper.application.ai_delegates.service._now_for_join",
            lambda: datetime.now(UTC) + _td(minutes=1),
        )
        body = DelegateCreateBody(label="Read", caps=DelegateCapsBody())
        created = await service.create_delegate(
            owner=_make_owner_principal("owner-pat-7"), body=body
        )
        view = await service.get_delegate(
            public_id=created.delegate.public_id, owner_public_id="owner-pat-7"
        )
        assert view.public_id == created.delegate.public_id
        assert not hasattr(view, "token_kind")

    @pytest.mark.asyncio
    async def test_deactivate_delegate_blacklists_jti(self, repo: SQLAlchemyRepository) -> None:
        """Kill-switch parity: delegate JTI lands in the blacklist after deactivate."""
        await _seed_owner(repo, public_id="owner-pat-8", username="owner-pat-8")
        manager = _fresh_manager()
        service = DelegateService(repository=repo, token_manager=manager)
        body = DelegateCreateBody(label="Kill", caps=DelegateCapsBody())
        payload = await service.create_delegate(
            owner=_make_owner_principal("owner-pat-8"), body=body
        )
        claims = manager.decode_fresh_token(payload.access_token)
        await manager.revoke_user_sessions(
            user_public_id=payload.delegate.public_id, repository=repo
        )
        assert claims.jti in manager._blacklisted_tokens
