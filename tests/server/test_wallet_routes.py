"""Tests for the Phase 0d wallet catalogue read route.

Exercises the role-scoped visibility contract: ADMIN sees every
active wallet through ``list_active_wallets``; VIEWER and OPERATOR
see only the wallets their operator set covers via
``list_accessible_wallets_for_operators``. The handler is called
directly with a mocked repository to keep the test fast and
focused on the branch being verified.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.wallet_routes import list_wallets


def _wallet_row(public_id: str, label: str, is_paper: bool) -> WalletRow:
    """Return a minimal ``WalletRow`` TypedDict fixture."""
    return WalletRow(
        public_id=public_id,
        label=label,
        description=None,
        is_paper=is_paper,
        timestamp=datetime.now(UTC),
        session_id="test-sid",
        sequence_id=1,
    )


def _make_request() -> Request:
    """Return a ``Request`` mock with a real ``SequenceTracker`` attached."""
    mock_request = MagicMock(spec=Request)
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


class TestListWallets:
    """Role-scoped behaviour of ``list_wallets``."""

    @pytest.mark.asyncio
    async def test_admin_sees_every_active_wallet(self) -> None:
        """ADMIN branch calls ``list_active_wallets`` and passes through the result.

        Given: An ADMIN principal,
        When: ``list_wallets`` is called,
        Then: The repository's full-catalogue method is awaited
            exactly once and the scoped-for-operators method is
            never called.
        """
        mock_repo = AsyncMock()
        mock_repo.list_active_wallets = AsyncMock(
            return_value=[
                _wallet_row("wallet-paper", "default", True),
                _wallet_row("wallet-live", "default", False),
            ]
        )
        mock_repo.list_accessible_wallets_for_operators = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await list_wallets(request=_make_request(), principal=principal, repo=mock_repo)

        assert result.count == 2
        assert [w.label for w in result.payload] == ["default", "default"]
        assert [w.is_paper for w in result.payload] == [True, False]
        mock_repo.list_active_wallets.assert_awaited_once()
        mock_repo.list_accessible_wallets_for_operators.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_operator_sees_only_accessible_wallets(self) -> None:
        """OPERATOR branch routes through the operator-scoped lookup.

        Given: An OPERATOR principal with two operator memberships,
        When: ``list_wallets`` is called,
        Then: The repository's operator-scoped method is awaited with
            the principal's operator IDs and the full-catalogue method
            is never called.
        """
        mock_repo = AsyncMock()
        mock_repo.list_active_wallets = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-firm", "firm", False)]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1", "op-2"],
        )

        result = await list_wallets(request=_make_request(), principal=principal, repo=mock_repo)

        assert result.count == 1
        assert result.payload[0].label == "firm"
        mock_repo.list_active_wallets.assert_not_awaited()
        mock_repo.list_accessible_wallets_for_operators.assert_awaited_once()
        call = mock_repo.list_accessible_wallets_for_operators.await_args
        assert call.args[0] == ["op-1", "op-2"]

    @pytest.mark.asyncio
    async def test_viewer_with_empty_operator_set_returns_empty(self) -> None:
        """VIEWER without memberships receives an empty payload.

        Given: A VIEWER principal carrying no operator IDs,
        When: ``list_wallets`` is called,
        Then: The repository's operator-scoped method is invoked with
            an empty list and the response payload is empty.
        """
        mock_repo = AsyncMock()
        mock_repo.list_active_wallets = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        principal = AuthPrincipal(
            username="bob",
            role=UserRole.VIEWER,
            operator_public_ids=[],
        )

        result = await list_wallets(request=_make_request(), principal=principal, repo=mock_repo)

        assert result.count == 0
        assert result.payload == []
        mock_repo.list_accessible_wallets_for_operators.assert_awaited_once()
        call = mock_repo.list_accessible_wallets_for_operators.await_args
        assert call.args[0] == []
