"""Tests for the wallet catalogue read + create routes.

Exercises:

- The role-scoped visibility contract on ``list_wallets``: ADMIN
  sees every active wallet through ``list_active_wallets``;
  VIEWER and OPERATOR see only the wallets their operator set
  covers via ``list_accessible_wallets_for_operators``.
- The ``create_wallet`` POST handler, including happy-path
  projection and the ``WalletConflictError`` -> HTTP 409 mapping.

The handlers are called directly with a mocked repository to keep
the tests fast and focused on the branches being verified.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.multi_tenant import CreateWalletBody
from snapper.api.schemas.multi_tenant import CreateWalletCommand
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import WalletConflictError
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.wallet_routes import create_wallet
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


def _make_create_wallet_command(
    *, label: str = "firm", is_paper: bool = False
) -> CreateWalletCommand:
    """Return a minimal valid create wallet command envelope."""
    return CreateWalletCommand(
        session_id="test-sid",
        sequence_id=1,
        public_id="00000000-0000-7000-8000-000000000700",
        timestamp=datetime.now(UTC),
        payload=CreateWalletBody(
            label=label,
            description="created by test",
            is_paper=is_paper,
        ),
    )


def _admin_principal() -> AuthPrincipal:
    """Return an ADMIN principal with a non-empty user_public_id."""
    return AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000099",
    )


class TestCreateWallet:
    """Behaviour of ``create_wallet`` POST handler."""

    @pytest.mark.asyncio
    async def test_successful_create_returns_projected_row(self) -> None:
        """Happy path: repository row projected into ``WalletInfo`` response.

        Given: An ADMIN principal and a valid command,
        When: ``create_wallet`` is called,
        Then: The repository method is awaited with the command
            fields and the response wraps the returned row.
        """
        mock_repo = AsyncMock()
        mock_repo.create_wallet = AsyncMock(return_value=_wallet_row("wallet-new", "firm", False))

        result = await create_wallet(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_create_wallet_command(label="firm", is_paper=False),
            repo=mock_repo,
        )

        assert result.payload.label == "firm"
        assert result.payload.is_paper is False
        mock_repo.create_wallet.assert_awaited_once()
        call_kwargs = mock_repo.create_wallet.await_args.kwargs
        assert call_kwargs["label"] == "firm"
        assert call_kwargs["is_paper"] is False
        assert call_kwargs["description"] == "created by test"

    @pytest.mark.asyncio
    async def test_duplicate_label_maps_to_409(self) -> None:
        """Repository ``WalletConflictError`` maps to HTTP 409.

        Given: The repository raises ``WalletConflictError`` because
            an active wallet with the same ``(label, is_paper)``
            already exists,
        When: ``create_wallet`` is called,
        Then: HTTPException 409 is raised.
        """
        mock_repo = AsyncMock()
        mock_repo.create_wallet = AsyncMock(
            side_effect=WalletConflictError(
                label="firm",
                is_paper=False,
                reason="active wallet with the same (label, is_paper) already exists",
            )
        )

        with pytest.raises(HTTPException) as excinfo:
            await create_wallet(
                request=_make_request(),
                _principal=_admin_principal(),
                _csrf=None,
                command=_make_create_wallet_command(label="firm", is_paper=False),
                repo=mock_repo,
            )

        assert excinfo.value.status_code == status.HTTP_409_CONFLICT

    @pytest.mark.asyncio
    async def test_paper_and_live_same_label_both_succeed(self) -> None:
        """Paper and live wallets sharing a label are independent inserts.

        Given: Two sequential creates with identical ``label='default'``
            but different ``is_paper`` values,
        When: Each ``create_wallet`` call is invoked,
        Then: Both complete without raising because the active-unique
            index is on ``(label, is_paper)``, not ``label`` alone.
        """
        mock_repo = AsyncMock()
        mock_repo.create_wallet = AsyncMock(
            side_effect=[
                _wallet_row("wallet-live", "default", False),
                _wallet_row("wallet-paper", "default", True),
            ]
        )

        live = await create_wallet(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_create_wallet_command(label="default", is_paper=False),
            repo=mock_repo,
        )
        paper = await create_wallet(
            request=_make_request(),
            _principal=_admin_principal(),
            _csrf=None,
            command=_make_create_wallet_command(label="default", is_paper=True),
            repo=mock_repo,
        )

        assert live.payload.is_paper is False
        assert paper.payload.is_paper is True
        assert mock_repo.create_wallet.await_count == 2
