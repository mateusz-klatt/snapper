"""Tests for the multi-tenant wallet scoping helper.

``resolve_target_wallets`` is the single function that every scoped
list endpoint calls to derive the ``wallet_public_ids`` filter from
the authenticated principal and optional query parameters.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi import status

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import WalletRow
from snapper.server.scoping import resolve_target_wallets


def _wallet_row(public_id: str) -> WalletRow:
    return WalletRow(
        public_id=public_id,
        label="w",
        description=None,
        is_paper=False,
        timestamp=datetime.now(UTC),
        session_id="test-sid",
        sequence_id=1,
    )


class TestResolveTargetWallets:
    """Behaviour of ``resolve_target_wallets``."""

    @pytest.mark.asyncio
    async def test_admin_no_params_returns_none(self) -> None:
        """ADMIN without explicit params sees all (returns ``None``).

        Given: An ADMIN principal with no query params,
        When: ``resolve_target_wallets`` is called,
        Then: ``None`` is returned so the repo applies no filter.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_target_wallets(principal, mock_repo)

        assert result is None
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_global_scope_survives_token_downscoping(self) -> None:
        """Structural global scope follows the named role permission set.

        Given: An ADMIN principal whose token permission claim is empty,
        When: Wallet visibility is resolved without explicit narrowing,
        Then: The historical global-scope result remains unfiltered.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="downscoped-admin",
            role=UserRole.ADMIN,
            permissions=[],
        )

        result = await resolve_target_wallets(principal, mock_repo)

        assert result is None
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_with_wallet_param_returns_singleton_list(self) -> None:
        """ADMIN with explicit wallet_public_id narrows to that wallet.

        Given: An ADMIN principal and a wallet_public_id query param,
        When: ``resolve_target_wallets`` is called,
        Then: The specified wallet ID is returned as a singleton list
            without consulting the operator accessibility lookup.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_target_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert result == ["wallet-42"]
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_operator_scopes_to_accessible_wallets(self) -> None:
        """OPERATOR without explicit params returns accessible wallet set.

        Given: An OPERATOR principal with two operator memberships,
        When: ``resolve_target_wallets`` is called with no explicit params,
        Then: The accessible wallet set is derived from the operator set.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-1"), _wallet_row("wallet-2")]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1", "op-2"],
        )

        result = await resolve_target_wallets(principal, mock_repo)

        assert result == ["wallet-1", "wallet-2"]

    @pytest.mark.asyncio
    async def test_operator_with_wallet_param_validates_accessibility(self) -> None:
        """OPERATOR with explicit wallet_public_id: 403 if outside accessible set.

        Given: An OPERATOR whose accessible set does not contain the
            requested wallet,
        When: ``resolve_target_wallets`` is called with that wallet_public_id,
        Then: HTTPException 403 is raised.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-other")]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1"],
        )

        with pytest.raises(HTTPException) as excinfo:
            await resolve_target_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_operator_with_accessible_wallet_returns_singleton(self) -> None:
        """OPERATOR with explicit wallet_public_id in accessible set narrows.

        Given: An OPERATOR whose accessible set contains the requested wallet,
        When: ``resolve_target_wallets`` is called,
        Then: The singleton list is returned.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-42")]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1"],
        )

        result = await resolve_target_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert result == ["wallet-42"]

    @pytest.mark.asyncio
    async def test_operator_with_foreign_operator_param_returns_403(self) -> None:
        """OPERATOR asking about a different operator gets 403.

        Given: An OPERATOR principal whose operator set is ``["op-1"]``,
        When: ``resolve_target_wallets`` is called with ``operator_public_id="op-99"``,
        Then: HTTPException 403 is raised.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1"],
        )

        with pytest.raises(HTTPException) as excinfo:
            await resolve_target_wallets(principal, mock_repo, operator_public_id="op-99")

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_admin_with_operator_param_narrows_to_that_operator(self) -> None:
        """ADMIN with explicit operator_public_id narrows via that operator's grants.

        Given: An ADMIN principal and an operator_public_id query param,
        When: ``resolve_target_wallets`` is called,
        Then: The accessible wallets are derived from that single operator.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-firm")]
        )
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_target_wallets(principal, mock_repo, operator_public_id="op-99")

        assert result == ["wallet-firm"]
        call = mock_repo.list_accessible_wallets_for_operators.await_args
        assert call.args[0] == ["op-99"]

    @pytest.mark.asyncio
    async def test_viewer_empty_operators_returns_empty_list(self) -> None:
        """VIEWER with no operators returns empty wallet list.

        Given: A VIEWER with no operator memberships,
        When: ``resolve_target_wallets`` is called,
        Then: An empty list is returned (the repo call returns ``[]``).
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        principal = AuthPrincipal(
            username="bob",
            role=UserRole.VIEWER,
            operator_public_ids=[],
        )

        result = await resolve_target_wallets(principal, mock_repo)

        assert result == []
