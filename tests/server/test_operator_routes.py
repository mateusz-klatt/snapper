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
from fastapi import Request

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import OperatorRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.operator_routes import list_operators


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
