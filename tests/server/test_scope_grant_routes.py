"""Tests for the Phase 0d scope grant read + write routes.

Exercises:

- The wallet-visibility authorization gate on
  ``list_scope_grants`` — ADMIN may query any wallet, non-ADMIN
  principals must have the target wallet in their accessible set
  or they receive 403 so the wallet's existence is not leaked via
  an empty payload.
- The create-grant path including XOR pre-validation (400),
  overlap conflict bubbling (409), and granted_by_user_public_id
  audit stamping from the principal.
- The handover path including successful close-and-insert,
  cross-scope overlap (409), self-handover rejection (400), and
  missing source grant (404).

All tests invoke the handler functions directly with a mocked
``Repository`` so the authorization branches are exercised without
booting a FastAPI TestClient.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateScopeGrantBody
from snapper.api.schemas.multi_tenant import CreateScopeGrantCommand
from snapper.api.schemas.multi_tenant import HandoverScopeGrantBody
from snapper.api.schemas.multi_tenant import HandoverScopeGrantCommand
from snapper.api.schemas.multi_tenant import RevokeScopeGrantBody
from snapper.api.schemas.multi_tenant import RevokeScopeGrantCommand
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository import ScopeGrantConflictError
from snapper.data.repository import ScopeGrantNotFoundError
from snapper.data.repository import ScopeGrantValidationError
from snapper.data.repository_types import ScopeGrantRow
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.scope_grant_routes import _scope_grant_service_dependency
from snapper.server.scope_grant_routes import create_scope_grant
from snapper.server.scope_grant_routes import handover_scope_grant
from snapper.server.scope_grant_routes import list_scope_grants
from snapper.server.scope_grant_routes import revoke_scope_grant


def _wallet_row(public_id: str) -> WalletRow:
    """Minimal ``WalletRow`` TypedDict fixture for visibility checks."""
    return WalletRow(
        public_id=public_id,
        label="default",
        description=None,
        is_paper=True,
        timestamp=datetime.now(UTC),
        session_id="test-sid",
        sequence_id=1,
    )


def _grant_row(
    public_id: str,
    operator_public_id: str,
    wallet_public_id: str,
    scope_kind: str,
) -> ScopeGrantRow:
    """Minimal ``ScopeGrantRow`` TypedDict fixture."""
    now = datetime.now(UTC)
    return ScopeGrantRow(
        public_id=public_id,
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        granted_by_user_public_id="00000000-0000-7000-8000-000000000099",
        scope_kind=scope_kind,
        underlying_public_id=(
            "00000000-0000-7000-8000-0000000000aa" if scope_kind == "underlying" else None
        ),
        instrument_public_id=(
            "00000000-0000-7000-8000-0000000000bb" if scope_kind == "instrument" else None
        ),
        note=None,
        timestamp=now,
        known_to=KNOWN_TO_MAX,
        session_id="test-sid",
        sequence_id=1,
    )


def _make_request() -> Request:
    """Return a ``Request`` mock with a real ``SequenceTracker`` attached."""
    mock_request = MagicMock(spec=Request)
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


class TestListScopeGrants:
    """Authorization and projection behaviour of ``list_scope_grants``."""

    @pytest.mark.asyncio
    async def test_admin_bypasses_visibility_check(self) -> None:
        """ADMIN skips the wallet-accessibility gate entirely.

        Given: An ADMIN principal and a wallet ID,
        When: ``list_scope_grants`` is called,
        Then: ``list_accessible_wallets_for_operators`` is NOT
            called and ``list_active_scope_grants_for_wallet``
            returns the full set.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock()
        mock_repo.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[
                _grant_row("grant-1", "op-1", "wallet-42", "underlying"),
                _grant_row("grant-2", "op-2", "wallet-42", "instrument"),
            ]
        )
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await list_scope_grants(
            request=_make_request(),
            principal=principal,
            repo=mock_repo,
            wallet_public_id="wallet-42",
        )

        assert result.count == 2
        kinds = [g.scope_kind for g in result.payload]
        assert kinds == ["underlying", "instrument"]
        mock_repo.list_accessible_wallets_for_operators.assert_not_awaited()
        mock_repo.list_active_scope_grants_for_wallet.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_operator_with_visible_wallet_succeeds(self) -> None:
        """OPERATOR with the wallet in their accessible set receives the grants.

        Given: An OPERATOR principal whose operators cover the target wallet,
        When: ``list_scope_grants`` is called,
        Then: The accessibility check passes and the grant list returns.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-42")]
        )
        mock_repo.list_active_scope_grants_for_wallet = AsyncMock(
            return_value=[_grant_row("grant-1", "op-1", "wallet-42", "underlying")]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1"],
        )

        result = await list_scope_grants(
            request=_make_request(),
            principal=principal,
            repo=mock_repo,
            wallet_public_id="wallet-42",
        )

        assert result.count == 1
        assert result.payload[0].operator_public_id == "op-1"

    @pytest.mark.asyncio
    async def test_operator_with_inaccessible_wallet_returns_403(self) -> None:
        """A non-ADMIN asking about a hidden wallet is refused with 403.

        Given: An OPERATOR whose accessible set does not contain the
            target wallet,
        When: ``list_scope_grants`` is called,
        Then: An HTTPException 403 is raised and the grant query is
            never executed so the wallet's existence is not leaked.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-other")]
        )
        mock_repo.list_active_scope_grants_for_wallet = AsyncMock()
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1"],
        )

        with pytest.raises(HTTPException) as excinfo:
            await list_scope_grants(
                request=_make_request(),
                principal=principal,
                repo=mock_repo,
                wallet_public_id="wallet-42",
            )

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN
        mock_repo.list_active_scope_grants_for_wallet.assert_not_awaited()


def _make_create_command(
    *,
    scope_kind: str = "underlying",
    underlying_public_id: str | None = "00000000-0000-7000-8000-0000000000aa",
    instrument_public_id: str | None = None,
) -> CreateScopeGrantCommand:
    """Return a minimal valid create command envelope."""
    return CreateScopeGrantCommand(
        session_id="test-sid",
        sequence_id=1,
        public_id="00000000-0000-7000-8000-000000000500",
        timestamp=datetime.now(UTC),
        payload=CreateScopeGrantBody(
            operator_public_id="00000000-0000-7000-8000-000000000100",
            wallet_public_id="00000000-0000-7000-8000-000000000200",
            scope_kind=scope_kind,
            underlying_public_id=underlying_public_id,
            instrument_public_id=instrument_public_id,
            note="audit note",
        ),
    )


def _admin_principal() -> AuthPrincipal:
    """Return an ADMIN principal with a non-empty user_public_id."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000099",
    )


class TestCreateScopeGrant:
    """Behaviour of ``create_scope_grant`` POST handler."""

    @pytest.mark.asyncio
    async def test_successful_create_stamps_audit_identity_from_principal(
        self,
    ) -> None:
        """Successful create returns the new row with audit fields from the principal.

        Given: An ADMIN principal and a valid underlying-scoped command,
        When: ``create_scope_grant`` is called,
        Then: The repository receives the principal's ``user_public_id``
            as ``granted_by_user_public_id`` (not anything from the
            client payload) and the response wraps the new row.
        """
        mock_repo = AsyncMock()
        returned_row = _grant_row("grant-new", "op-1", "wallet-42", "underlying")
        mock_repo.create_scope_grant = AsyncMock(return_value=returned_row)

        result = await create_scope_grant(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_create_command(),
            repo=mock_repo,
        )

        assert result.payload.operator_public_id == "op-1"
        assert result.payload.scope_kind == "underlying"
        mock_repo.create_scope_grant.assert_awaited_once()
        call_kwargs = mock_repo.create_scope_grant.await_args.args[0]
        assert call_kwargs["granted_by_user_public_id"] == "00000000-0000-7000-8000-000000000099"

    @pytest.mark.asyncio
    async def test_xor_mismatch_rejected_with_400_before_repo_call(self) -> None:
        """XOR pre-validation prevents malformed inputs from reaching the repo.

        Given: A command with ``scope_kind='underlying'`` but
            ``underlying_public_id=None``,
        When: ``create_scope_grant`` is called,
        Then: HTTPException 400 is raised and the repository is
            never consulted.
        """
        mock_repo = AsyncMock()
        command = _make_create_command(
            scope_kind="underlying",
            underlying_public_id=None,
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        mock_repo.create_scope_grant.assert_not_called()

    @pytest.mark.asyncio
    async def test_xor_mismatch_both_ids_rejected(self) -> None:
        """Providing BOTH underlying and instrument IDs also fails pre-validation."""
        mock_repo = AsyncMock()
        command = _make_create_command(
            scope_kind="instrument",
            underlying_public_id="00000000-0000-7000-8000-0000000000aa",
            instrument_public_id="00000000-0000-7000-8000-0000000000bb",
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST
        mock_repo.create_scope_grant.assert_not_called()

    @pytest.mark.asyncio
    async def test_overlap_conflict_bubbles_up_as_409(self) -> None:
        """Repository ``ScopeGrantConflictError`` maps to HTTP 409.

        Given: The repository raises ``ScopeGrantConflictError``,
        When: ``create_scope_grant`` is called,
        Then: HTTPException 409 is raised with the conflict detail.
        """
        mock_repo = AsyncMock()
        mock_repo.create_scope_grant = AsyncMock(
            side_effect=ScopeGrantConflictError(
                wallet_public_id="wallet-42",
                conflicting_grant_public_id="grant-existing",
                conflicting_operator_public_id="op-2",
                reason="overlap",
            )
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_create_command(),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT

    @pytest.mark.asyncio
    async def test_validation_error_maps_to_400(self) -> None:
        """Repository ``ScopeGrantValidationError`` maps to HTTP 400."""
        mock_repo = AsyncMock()
        mock_repo.create_scope_grant = AsyncMock(
            side_effect=ScopeGrantValidationError("bad scope_kind")
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_create_command(),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_not_found_error_maps_to_404(self) -> None:
        """Repository ``ScopeGrantNotFoundError`` maps to HTTP 404."""
        mock_repo = AsyncMock()
        mock_repo.create_scope_grant = AsyncMock(
            side_effect=ScopeGrantNotFoundError("operator not found")
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_create_command(),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND


def _make_handover_command() -> HandoverScopeGrantCommand:
    """Return a minimal valid handover command envelope."""
    return HandoverScopeGrantCommand(
        session_id="test-sid",
        sequence_id=1,
        public_id="00000000-0000-7000-8000-000000000600",
        timestamp=datetime.now(UTC),
        payload=HandoverScopeGrantBody(
            from_grant_public_id="00000000-0000-7000-8000-0000000000c1",
            to_operator_public_id="00000000-0000-7000-8000-000000000102",
            reason="vacation cover",
        ),
    )


class TestHandoverScopeGrant:
    """Behaviour of ``handover_scope_grant`` POST handler."""

    @pytest.mark.asyncio
    async def test_successful_handover_returns_both_rows(self) -> None:
        """Successful handover wraps closed + new grant in the response.

        Given: An ADMIN principal and a repository that returns a
            ``(closed, new)`` tuple,
        When: ``handover_scope_grant`` is called,
        Then: Both rows appear in the response payload and the
            repository received the principal's ``user_public_id`` as
            ``granted_by_user_public_id``.
        """
        mock_repo = AsyncMock()
        closed_row = _grant_row("grant-old", "op-1", "wallet-42", "underlying")
        new_row = _grant_row("grant-new", "op-2", "wallet-42", "underlying")
        mock_repo.handover_grant = AsyncMock(return_value=(closed_row, new_row))

        result = await handover_scope_grant(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_handover_command(),
            repo=mock_repo,
        )

        assert result.payload.closed_grant.operator_public_id == "op-1"
        assert result.payload.new_grant.operator_public_id == "op-2"
        mock_repo.handover_grant.assert_awaited_once()
        call_kwargs = mock_repo.handover_grant.await_args.kwargs
        assert call_kwargs["granted_by_user_public_id"] == "00000000-0000-7000-8000-000000000099"
        assert call_kwargs["reason"] == "vacation cover"

    @pytest.mark.asyncio
    async def test_missing_source_grant_maps_to_404(self) -> None:
        """A missing source grant surfaces as HTTP 404."""
        mock_repo = AsyncMock()
        mock_repo.handover_grant = AsyncMock(
            side_effect=ScopeGrantNotFoundError("source grant not found")
        )

        with pytest.raises(HTTPException) as excinfo:
            await handover_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_handover_command(),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_self_handover_validation_maps_to_400(self) -> None:
        """Self-handover (source operator == target operator) fails with 400."""
        mock_repo = AsyncMock()
        mock_repo.handover_grant = AsyncMock(
            side_effect=ScopeGrantValidationError("self-handover rejected")
        )

        with pytest.raises(HTTPException) as excinfo:
            await handover_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_handover_command(),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_400_BAD_REQUEST

    @pytest.mark.asyncio
    async def test_cross_scope_conflict_maps_to_409(self) -> None:
        """Overlap against the destination operator's grants fails with 409."""
        mock_repo = AsyncMock()
        mock_repo.handover_grant = AsyncMock(
            side_effect=ScopeGrantConflictError(
                wallet_public_id="wallet-42",
                conflicting_grant_public_id="grant-other",
                conflicting_operator_public_id="op-2",
                reason="target operator already has overlapping grant",
            )
        )

        with pytest.raises(HTTPException) as excinfo:
            await handover_scope_grant(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_handover_command(),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT


def _make_revoke_command(reason: str | None = "audit") -> RevokeScopeGrantCommand:
    """Return a minimal valid revoke command envelope."""
    return RevokeScopeGrantCommand(
        session_id="test-sid",
        sequence_id=1,
        public_id="00000000-0000-7000-8000-000000000700",
        timestamp=datetime.now(UTC),
        payload=RevokeScopeGrantBody(reason=reason),
    )


def _closed_grant_row(public_id: str) -> ScopeGrantRow:
    """Return a closed-grant projection as ``ScopeGrantService.revoke_grant`` would.

    known_to is stamped at the revoke timestamp (mimicking the SCD2 close).
    """
    now = datetime.now(UTC)
    return ScopeGrantRow(
        public_id=public_id,
        operator_public_id="00000000-0000-7000-8000-000000000101",
        wallet_public_id="00000000-0000-7000-8000-000000000200",
        granted_by_user_public_id="00000000-0000-7000-8000-000000000099",
        scope_kind="underlying",
        underlying_public_id="00000000-0000-7000-8000-0000000000aa",
        instrument_public_id=None,
        note=None,
        timestamp=now,
        known_to=now,
        session_id="test-sid",
        sequence_id=1,
    )


class TestRevokeScopeGrant:
    """Behaviour of ``revoke_scope_grant`` POST handler."""

    @pytest.mark.asyncio
    async def test_successful_revoke_returns_closed_row(self) -> None:
        """Happy path: service returns closed projection, route wraps it.

        Given: An ADMIN principal and a service that returns a closed row,
        When: ``revoke_scope_grant`` is called,
        Then: The response envelope carries the projected info; the service
            was called exactly once with the caller's principal bound to
            ``revoked_by_user_public_id``.
        """
        target_grant = "00000000-0000-7000-8000-0000000000c1"
        service = AsyncMock()
        service.revoke_grant = AsyncMock(return_value=_closed_grant_row(target_grant))

        response = await revoke_scope_grant(
            request=_make_request(),
            grant_public_id=target_grant,
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_revoke_command(reason="alice left"),
            scope_grant_service=service,
        )

        assert response.payload.public_id == target_grant
        assert response.payload.scope_kind == "underlying"
        service.revoke_grant.assert_awaited_once()
        call_kwargs = service.revoke_grant.await_args.kwargs
        assert call_kwargs["grant_public_id"] == target_grant
        assert call_kwargs["revoked_by_user_public_id"] == _admin_principal().user_public_id
        assert call_kwargs["reason"] == "alice left"

    @pytest.mark.asyncio
    async def test_not_found_maps_to_404(self) -> None:
        """Missing / already-closed grant surfaces as HTTP 404."""
        service = AsyncMock()
        service.revoke_grant = AsyncMock(side_effect=ScopeGrantNotFoundError("no such grant"))

        with pytest.raises(HTTPException) as excinfo:
            await revoke_scope_grant(
                request=_make_request(),
                grant_public_id="00000000-0000-7000-8000-0000000000ff",
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_revoke_command(),
                scope_grant_service=service,
            )
        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND

    @pytest.mark.asyncio
    async def test_reason_is_forwarded_verbatim(self) -> None:
        """None + non-None reason both round-trip to the service unchanged."""
        target_grant = "00000000-0000-7000-8000-0000000000c1"
        service = AsyncMock()
        service.revoke_grant = AsyncMock(return_value=_closed_grant_row(target_grant))

        await revoke_scope_grant(
            request=_make_request(),
            grant_public_id=target_grant,
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_revoke_command(reason=None),
            scope_grant_service=service,
        )
        assert service.revoke_grant.await_args.kwargs["reason"] is None

    def test_scope_grant_service_dependency_returns_singleton(self) -> None:
        """The FastAPI dependency returns the shared ScopeGrantService instance."""
        ScopeGrantService.clear_instance()
        try:
            first = _scope_grant_service_dependency()
            second = _scope_grant_service_dependency()
            assert first is second
        finally:
            ScopeGrantService.clear_instance()

    @pytest.mark.parametrize(
        "role",
        [UserRole.VIEWER, UserRole.OPERATOR, UserRole.AI_DELEGATE],
    )
    def test_revoke_route_rejects_non_admin_roles(self, role: UserRole) -> None:
        """``require_permission(MANAGE_SCOPE_GRANTS)`` blocks every non-ADMIN role.

        The POST /api/scope-grants/{id}/revoke route is guarded by
        ``require_permission(MANAGE_SCOPE_GRANTS)``
        (ADMIN-only at MVP). The permission is granted to ADMIN only in
        ``ROLE_PERMISSIONS``; VIEWER / OPERATOR / AI_DELEGATE all hit
        the 403 branch of the dependency. This pins that contract so a
        future role-permission rewrite cannot silently relax the gate.
        """
        checker = require_permission(Permission.MANAGE_SCOPE_GRANTS)
        principal = AuthPrincipal(
            username=f"non-admin-{role.value}",
            role=role,
            user_public_id="00000000-0000-7000-8000-0000000000c0",
        )
        with pytest.raises(HTTPException) as excinfo:
            checker(current_user=principal)
        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_revoke_route_double_revoke_maps_to_404(self) -> None:
        """Two successive revokes on the same grant both return 404 on the second.

        Service raises ``ScopeGrantNotFoundError`` on the already-closed
        grant; the route translates it to HTTP 404 at the second attempt.
        Verifies the idempotency surface from the client's perspective.
        """
        target_grant = "00000000-0000-7000-8000-0000000000c1"
        service = AsyncMock()
        service.revoke_grant = AsyncMock(
            side_effect=[
                _closed_grant_row(target_grant),
                ScopeGrantNotFoundError("grant already closed"),
            ]
        )

        response = await revoke_scope_grant(
            request=_make_request(),
            grant_public_id=target_grant,
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_revoke_command(),
            scope_grant_service=service,
        )
        assert response.payload.public_id == target_grant

        with pytest.raises(HTTPException) as excinfo:
            await revoke_scope_grant(
                request=_make_request(),
                grant_public_id=target_grant,
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_revoke_command(),
                scope_grant_service=service,
            )
        assert excinfo.value.status_code == status.HTTP_404_NOT_FOUND
