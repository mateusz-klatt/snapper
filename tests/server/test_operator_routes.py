"""Tests for the operator catalogue read route.

Verifies the ADMIN-wide vs. membership-bound visibility branches of
``list_operators``. ADMIN returns every active operator; VIEWER and
OPERATOR return only operators listed in ``principal.operator_public_ids``.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateOperatorBody
from snapper.api.schemas.multi_tenant import CreateOperatorCommand
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import OperatorConflictError
from snapper.data.repository_types import OperatorRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.operator_routes import create_operator
from snapper.server.operator_routes import list_operators


def _admin_principal() -> AuthPrincipal:
    """Return an ADMIN principal."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000099",
    )


def _make_create_operator_command(
    *, label: str = "firm-desk", description: str | None = "created by test"
) -> CreateOperatorCommand:
    """Return a minimal valid create-operator command envelope."""
    return CreateOperatorCommand(
        session_id="test-sid",
        sequence_id=1,
        public_id="00000000-0000-7000-8000-000000000701",
        timestamp=datetime.now(UTC),
        payload=CreateOperatorBody(label=label, description=description),
    )


def _operator_row(public_id: str, label: str) -> OperatorRow:
    """Return a minimal ``OperatorRow`` TypedDict fixture."""
    return OperatorRow(
        public_id=public_id,
        label=label,
        description=None,
        timestamp=datetime.now(UTC),
        session_id="test-sid",
        sequence_id=1,
    )


def _make_request() -> Request:
    """Return a ``Request`` mock with a real ``SequenceTracker`` attached."""
    mock_request = MagicMock(spec=Request)
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


class TestListOperators:
    """Role-scoped behaviour of ``list_operators``."""

    @pytest.mark.asyncio
    async def test_admin_sees_every_active_operator(self) -> None:
        """ADMIN receives the full catalogue without membership filtering.

        Given: An ADMIN principal,
        When: ``list_operators`` is called,
        Then: Every active operator row appears in the response.
        """
        mock_repo = AsyncMock()
        mock_repo.list_active_operators = AsyncMock(
            return_value=[
                _operator_row("op-1", "alpha"),
                _operator_row("op-2", "beta"),
                _operator_row("op-3", "gamma"),
            ]
        )
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await list_operators(request=_make_request(), principal=principal, repo=mock_repo)

        assert result.count == 3
        assert [o.label for o in result.payload] == ["alpha", "beta", "gamma"]
        mock_repo.list_active_operators.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_explicit_as_of_drives_the_catalogue_snapshot(self) -> None:
        """The picker and desk-directory query can share one history horizon.

        Given: An ADMIN and an explicit historical timestamp.
        When: The operator catalogue is listed at that timestamp.
        Then: The repository receives the timestamp unchanged.
        """
        horizon = datetime(2025, 1, 2, 3, 4, 5, tzinfo=UTC)
        mock_repo = AsyncMock()
        mock_repo.list_active_operators = AsyncMock(return_value=[])

        result = await list_operators(
            request=_make_request(),
            principal=AuthPrincipal(username="admin", role=UserRole.ADMIN),
            repo=mock_repo,
            as_of=horizon,
        )

        assert result.payload == []
        mock_repo.list_active_operators.assert_awaited_once_with(horizon)

    @pytest.mark.asyncio
    async def test_operator_sees_only_membership_operators(self) -> None:
        """OPERATOR filters the full catalogue down to membership IDs.

        Given: An OPERATOR principal with one membership,
        When: ``list_operators`` is called,
        Then: Only the membership operator appears in the response.
        """
        mock_repo = AsyncMock()
        mock_repo.list_active_operators = AsyncMock(
            return_value=[
                _operator_row("op-1", "alpha"),
                _operator_row("op-2", "beta"),
                _operator_row("op-3", "gamma"),
            ]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-2"],
        )

        result = await list_operators(request=_make_request(), principal=principal, repo=mock_repo)

        assert result.count == 1
        assert result.payload[0].label == "beta"

    @pytest.mark.asyncio
    async def test_viewer_with_no_memberships_receives_empty(self) -> None:
        """VIEWER without memberships receives an empty payload.

        Given: A VIEWER principal carrying no operator IDs,
        When: ``list_operators`` is called,
        Then: The payload is empty even though the catalogue is not.
        """
        mock_repo = AsyncMock()
        mock_repo.list_active_operators = AsyncMock(return_value=[_operator_row("op-1", "alpha")])
        principal = AuthPrincipal(
            username="bob",
            role=UserRole.VIEWER,
            operator_public_ids=[],
        )

        result = await list_operators(request=_make_request(), principal=principal, repo=mock_repo)

        assert result.count == 0
        assert result.payload == []


class TestCreateOperator:
    """Behaviour of the ``create_operator`` POST handler."""

    @pytest.mark.asyncio
    async def test_successful_create_returns_projected_row(self) -> None:
        """Happy path: repository row projected into an ``OperatorInfo`` response.

        Given: An ADMIN principal and a valid command,
        When: ``create_operator`` is called,
        Then: The repository method is awaited with the command fields and the
            response wraps the returned row.
        """
        mock_repo = AsyncMock()
        mock_repo.create_operator = AsyncMock(return_value=_operator_row("op-new", "firm-desk"))

        result = await create_operator(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_create_operator_command(label="firm-desk"),
            repo=mock_repo,
        )

        assert result.payload.label == "firm-desk"
        assert result.payload.public_id == "op-new"
        mock_repo.create_operator.assert_awaited_once()
        call_kwargs = mock_repo.create_operator.await_args.kwargs
        assert call_kwargs["label"] == "firm-desk"
        assert call_kwargs["description"] == "created by test"

    @pytest.mark.asyncio
    async def test_duplicate_label_maps_to_409(self) -> None:
        """Repository ``OperatorConflictError`` maps to HTTP 409.

        Given: The repository raises ``OperatorConflictError`` for a duplicate
            active label,
        When: ``create_operator`` is called,
        Then: HTTPException 409 is raised.
        """
        mock_repo = AsyncMock()
        mock_repo.create_operator = AsyncMock(
            side_effect=OperatorConflictError(
                label="firm-desk",
                reason="active operator with the same label already exists",
            )
        )

        error_request = _make_request()
        error_principal = _admin_principal()
        error_command = _make_create_operator_command(label="firm-desk")
        with pytest.raises(HTTPException) as excinfo:
            await create_operator(
                request=error_request,
                _principal=error_principal,
                _csrf=None,
                command=error_command,
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT
