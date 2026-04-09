"""Tests for the Phase 0d scope grant read route.

Exercises the wallet-visibility authorization gate on
``list_scope_grants``: ADMIN may query any wallet, non-ADMIN
principals must have the target wallet in their accessible set or
they receive 403. The 403 path is important so the existence of a
wallet is not leaked via an empty payload.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository_types import ScopeGrantRow
from snapper.data.repository_types import WalletRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.scope_grant_routes import list_scope_grants


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
