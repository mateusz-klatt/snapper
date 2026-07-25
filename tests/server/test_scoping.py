"""Tests for the split multi-tenant wallet scoping primitives.

``resolve_readable_wallets`` and ``resolve_tradable_wallets`` are two
separately named functions — never one function with an ``intent`` flag —
so that widening what a caller may SEE can never widen what they may DO.
The read primitive resolves through ``list_readable_wallets_for_user``
(operator scope grants UNION the user's personal read grants); the trade
primitive resolves through ``list_accessible_wallets_for_operators``
(operator scope grants alone).
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
from snapper.server.scoping import ACTIVE_WALLET_REQUIRED_DETAIL
from snapper.server.scoping import require_tradable_active_wallet
from snapper.server.scoping import resolve_readable_active_wallet
from snapper.server.scoping import resolve_readable_wallets
from snapper.server.scoping import resolve_tradable_wallets


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


class TestResolveTradableWallets:
    """Behaviour of ``resolve_tradable_wallets`` (the trade plane)."""

    @pytest.mark.asyncio
    async def test_admin_no_params_returns_none(self) -> None:
        """ADMIN without explicit params sees all (returns ``None``).

        Given: An ADMIN principal with no query params,
        When: ``resolve_tradable_wallets`` is called,
        Then: ``None`` is returned so the repo applies no filter.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_tradable_wallets(principal, mock_repo)

        assert result is None
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_global_scope_survives_token_downscoping(self) -> None:
        """Structural global scope follows the named role permission set.

        Given: An ADMIN principal whose token permission claim is empty,
        When: Wallet tradability is resolved without explicit narrowing,
        Then: The historical global-scope result remains unfiltered.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="downscoped-admin",
            role=UserRole.ADMIN,
            permissions=[],
        )

        result = await resolve_tradable_wallets(principal, mock_repo)

        assert result is None
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_with_wallet_param_returns_singleton_list(self) -> None:
        """ADMIN with explicit wallet_public_id narrows to that wallet.

        Given: An ADMIN principal and a wallet_public_id query param,
        When: ``resolve_tradable_wallets`` is called,
        Then: The specified wallet ID is returned as a singleton list
            without consulting the operator accessibility lookup.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_tradable_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert result == ["wallet-42"]
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_operator_scopes_to_accessible_wallets(self) -> None:
        """OPERATOR without explicit params returns accessible wallet set.

        Given: An OPERATOR principal with two operator memberships,
        When: ``resolve_tradable_wallets`` is called with no explicit params,
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

        result = await resolve_tradable_wallets(principal, mock_repo)

        assert result == ["wallet-1", "wallet-2"]

    @pytest.mark.asyncio
    async def test_operator_with_wallet_param_validates_accessibility(self) -> None:
        """OPERATOR with explicit wallet_public_id: 403 if outside accessible set.

        Given: An OPERATOR whose accessible set does not contain the
            requested wallet,
        When: ``resolve_tradable_wallets`` is called with that wallet_public_id,
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
            await resolve_tradable_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_operator_with_accessible_wallet_returns_singleton(self) -> None:
        """OPERATOR with explicit wallet_public_id in accessible set narrows.

        Given: An OPERATOR whose accessible set contains the requested wallet,
        When: ``resolve_tradable_wallets`` is called,
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

        result = await resolve_tradable_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert result == ["wallet-42"]

    @pytest.mark.asyncio
    async def test_operator_with_foreign_operator_param_returns_403(self) -> None:
        """OPERATOR asking about a different operator gets 403.

        Given: An OPERATOR principal whose operator set is ``["op-1"]``,
        When: ``resolve_tradable_wallets`` is called with ``operator_public_id="op-99"``,
        Then: HTTPException 403 is raised.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            operator_public_ids=["op-1"],
        )

        with pytest.raises(HTTPException) as excinfo:
            await resolve_tradable_wallets(principal, mock_repo, operator_public_id="op-99")

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_admin_with_operator_param_narrows_to_that_operator(self) -> None:
        """ADMIN with explicit operator_public_id narrows via that operator's grants.

        Given: An ADMIN principal and an operator_public_id query param,
        When: ``resolve_tradable_wallets`` is called,
        Then: The accessible wallets are derived from that single operator.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-firm")]
        )
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_tradable_wallets(principal, mock_repo, operator_public_id="op-99")

        assert result == ["wallet-firm"]
        call = mock_repo.list_accessible_wallets_for_operators.await_args
        assert call.args[0] == ["op-99"]

    @pytest.mark.asyncio
    async def test_viewer_empty_operators_returns_empty_list(self) -> None:
        """VIEWER with no operators returns empty wallet list.

        Given: A VIEWER with no operator memberships,
        When: ``resolve_tradable_wallets`` is called,
        Then: An empty list is returned (the repo call returns ``[]``).
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        principal = AuthPrincipal(
            username="bob",
            role=UserRole.VIEWER,
            operator_public_ids=[],
        )

        result = await resolve_tradable_wallets(principal, mock_repo)

        assert result == []

    @pytest.mark.asyncio
    async def test_read_grants_never_reach_the_trade_plane(self) -> None:
        """A read-granted wallet is refused on the trade plane.

        Given: A VIEWER with zero operator memberships whose only wallet
            visibility comes from a personal read grant,
        When: ``resolve_tradable_wallets`` is called for that wallet,
        Then: HTTPException 403 is raised and the read-grant union lookup
            is never consulted.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-read-only")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=[],
        )

        with pytest.raises(HTTPException) as excinfo:
            await resolve_tradable_wallets(
                principal,
                mock_repo,
                wallet_public_id="wallet-read-only",
            )

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN
        mock_repo.list_readable_wallets_for_user.assert_not_called()


class TestResolveReadableWallets:
    """Behaviour of ``resolve_readable_wallets`` (the read plane)."""

    @pytest.mark.asyncio
    async def test_admin_no_params_returns_none(self) -> None:
        """ADMIN without explicit params sees all (returns ``None``).

        Given: An ADMIN principal with no query params,
        When: ``resolve_readable_wallets`` is called,
        Then: ``None`` is returned and neither wallet lookup runs.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_readable_wallets(principal, mock_repo)

        assert result is None
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()
        mock_repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_with_wallet_param_returns_singleton_list(self) -> None:
        """ADMIN with explicit wallet_public_id narrows to that wallet.

        Given: An ADMIN principal and a wallet_public_id query param,
        When: ``resolve_readable_wallets`` is called,
        Then: The specified wallet ID is returned without any lookup.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_readable_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert result == ["wallet-42"]
        mock_repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_with_operator_param_uses_the_operator_plane(self) -> None:
        """ADMIN narrowing to an operator asks about THAT operator's grants.

        Given: An ADMIN principal and an operator_public_id query param,
        When: ``resolve_readable_wallets`` is called,
        Then: The operator scope-grant lookup answers, so the admin's own
            personal read grants never leak into an operator-narrowed view.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-firm")]
        )
        principal = AuthPrincipal(username="admin", role=UserRole.ADMIN)

        result = await resolve_readable_wallets(principal, mock_repo, operator_public_id="op-99")

        assert result == ["wallet-firm"]
        call = mock_repo.list_accessible_wallets_for_operators.await_args
        assert call.args[0] == ["op-99"]
        mock_repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_operator_scopes_to_readable_union(self) -> None:
        """OPERATOR without explicit params returns the readable union.

        Given: An OPERATOR principal with two operator memberships,
        When: ``resolve_readable_wallets`` is called with no explicit params,
        Then: The union lookup is called with the user id and the full
            operator set, and its rows are returned.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-1"), _wallet_row("wallet-2")]
        )
        principal = AuthPrincipal(
            username="alice",
            role=UserRole.OPERATOR,
            user_public_id="user-alice",
            operator_public_ids=["op-1", "op-2"],
        )

        result = await resolve_readable_wallets(principal, mock_repo)

        assert result == ["wallet-1", "wallet-2"]
        call = mock_repo.list_readable_wallets_for_user.await_args
        assert call.args[0] == "user-alice"
        assert call.args[1] == ["op-1", "op-2"]
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_zero_membership_read_grant_is_visible(self) -> None:
        """A membership-less user still sees their read-granted wallet.

        Given: A VIEWER with an EMPTY operator set whose only visibility is
            one personal ``wallet_user_read_grants`` row,
        When: ``resolve_readable_wallets`` is called with no explicit params,
        Then: The read plane is consulted with the empty operator list —
            never short-circuited to ``[]`` the way the trade plane is —
            and the granted wallet is returned.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-read-only")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=[],
        )

        result = await resolve_readable_wallets(principal, mock_repo)

        assert result == ["wallet-read-only"]
        call = mock_repo.list_readable_wallets_for_user.await_args
        assert call.args[0] == "user-carol"
        assert call.args[1] == []
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_zero_membership_read_grant_narrows_to_that_wallet(self) -> None:
        """A membership-less user may name their read-granted wallet.

        Given: A VIEWER with an EMPTY operator set and one read grant,
        When: ``resolve_readable_wallets`` is called for that wallet — the
            shape every record-derived read surface uses,
        Then: The singleton list is returned instead of a 403.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-read-only")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=[],
        )

        result = await resolve_readable_wallets(
            principal,
            mock_repo,
            wallet_public_id="wallet-read-only",
        )

        assert result == ["wallet-read-only"]

    @pytest.mark.asyncio
    async def test_wallet_outside_readable_union_returns_403(self) -> None:
        """A wallet neither operator-covered nor read-granted is refused.

        Given: A VIEWER whose readable union does not contain the wallet,
        When: ``resolve_readable_wallets`` is called with that wallet,
        Then: HTTPException 403 is raised.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-other")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=[],
        )

        with pytest.raises(HTTPException) as excinfo:
            await resolve_readable_wallets(principal, mock_repo, wallet_public_id="wallet-42")

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN

    @pytest.mark.asyncio
    async def test_foreign_operator_param_returns_403(self) -> None:
        """Narrowing to an operator the caller does not belong to is refused.

        Given: A VIEWER principal whose operator set is ``["op-1"]``,
        When: ``resolve_readable_wallets`` is called with
            ``operator_public_id="op-99"``,
        Then: HTTPException 403 is raised before any lookup runs.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=["op-1"],
        )

        with pytest.raises(HTTPException) as excinfo:
            await resolve_readable_wallets(principal, mock_repo, operator_public_id="op-99")

        assert excinfo.value.status_code == status.HTTP_403_FORBIDDEN
        mock_repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_own_operator_param_narrows_the_read_union(self) -> None:
        """Narrowing to one of the caller's own operators is honoured.

        Given: A VIEWER principal who belongs to ``op-1`` and ``op-2``,
        When: ``resolve_readable_wallets`` is called with
            ``operator_public_id="op-2"``,
        Then: The read union runs against that single operator id.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-op2")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=["op-1", "op-2"],
        )

        result = await resolve_readable_wallets(principal, mock_repo, operator_public_id="op-2")

        assert result == ["wallet-op2"]
        call = mock_repo.list_readable_wallets_for_user.await_args
        assert call.args[1] == ["op-2"]


class TestReadPlaneOperatorNarrowing:
    """What ``operator_public_id`` does — and does not — narrow on reads.

    On the trade plane the parameter narrows the whole answer. On the read
    plane it narrows only the operator-covered half of the union: personal
    ``wallet_user_read_grants`` rows hang off the USER, not off an
    operator, so no operator filter can match them and they always
    participate. These tests pin that contract so it cannot be quietly
    reverted to the pre-split operator-only resolver, which would 403 a
    wallet the caller demonstrably may read.
    """

    @pytest.mark.asyncio
    async def test_operator_narrowing_keeps_personal_read_grants(self) -> None:
        """An operator-narrowed read still resolves through the read union.

        Given: A VIEWER who belongs to ``op-1`` and additionally holds a
            personal read grant on a wallet ``op-1`` does not cover,
        When: ``resolve_readable_wallets`` is called with
            ``operator_public_id="op-1"``,
        Then: The read-union lookup answers (never the operator-only trade
            lookup), so the read-granted wallet survives the narrowing.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-op1"), _wallet_row("wallet-read-only")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=["op-1"],
        )

        result = await resolve_readable_wallets(principal, mock_repo, operator_public_id="op-1")

        assert result == ["wallet-op1", "wallet-read-only"]
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_operator_narrowing_does_not_refuse_a_read_granted_wallet(self) -> None:
        """Both params together still answer for a read-granted wallet.

        Given: The same VIEWER, naming BOTH their operator and the wallet
            they hold only a personal read grant on,
        When: ``resolve_readable_wallets`` is called with both params,
        Then: The wallet is returned rather than the 403 the pre-split
            operator-only resolver raised for exactly this combination.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-op1"), _wallet_row("wallet-read-only")]
        )
        principal = AuthPrincipal(
            username="carol",
            role=UserRole.VIEWER,
            user_public_id="user-carol",
            operator_public_ids=["op-1"],
        )

        result = await resolve_readable_wallets(
            principal,
            mock_repo,
            operator_public_id="op-1",
            wallet_public_id="wallet-read-only",
        )

        assert result == ["wallet-read-only"]


class TestRequireTradableActiveWallet:
    """Behaviour of the consumption-time gate on the wallet claim.

    ``active_wallet_public_id`` is minted after READ-plane validation, so
    a personal read grant is enough to put a wallet in the claim. This
    dependency is what stops that claim from becoming write authority on
    a later request; nothing in the call graph joins the two requests, so
    no static guard can substitute for it.
    """

    @pytest.mark.asyncio
    async def test_missing_claim_is_refused_with_400(self) -> None:
        """A cleared wallet claim fails closed rather than defaulting.

        Given: A principal whose ``active_wallet_public_id`` is ``None``,
        When: The dependency runs,
        Then: 400 is raised with the uniform detail and the trade-plane
            lookup is never issued.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="dana", role=UserRole.OPERATOR)

        with pytest.raises(HTTPException) as exc_info:
            await require_tradable_active_wallet(principal, mock_repo)

        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert exc_info.value.detail == ACTIVE_WALLET_REQUIRED_DETAIL
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_granted_wallet_is_refused_with_403(self) -> None:
        """A wallet the caller may only SEE is not a wallet they may WRITE.

        Given: An AI_REVIEWER with zero operator memberships whose claim
            names a wallet reachable only through a personal read grant,
            so the trade plane returns an empty accessible set,
        When: The dependency runs,
        Then: 403 is raised — this is the escalation the gate closes.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
        principal = AuthPrincipal(
            username="erin",
            role=UserRole.AI_REVIEWER,
            user_public_id="user-erin",
            active_wallet_public_id="wallet-read-only",
        )

        with pytest.raises(HTTPException) as exc_info:
            await require_tradable_active_wallet(principal, mock_repo)

        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
        mock_repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_operator_granted_wallet_is_returned(self) -> None:
        """A wallet covered by an operator scope grant passes through.

        Given: An OPERATOR whose claim names a wallet its operator holds
            an active scope grant on,
        When: The dependency runs,
        Then: The wallet id is returned unchanged for the handler to use.
        """
        mock_repo = AsyncMock()
        mock_repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-op1")]
        )
        principal = AuthPrincipal(
            username="frank",
            role=UserRole.OPERATOR,
            user_public_id="user-frank",
            operator_public_ids=["op-1"],
            active_wallet_public_id="wallet-op1",
        )

        result = await require_tradable_active_wallet(principal, mock_repo)

        assert result == "wallet-op1"

    @pytest.mark.asyncio
    async def test_global_scope_passes_without_a_lookup(self) -> None:
        """Global scope keeps its unscoped answer on the write path too.

        Given: An ADMIN principal with a wallet claim,
        When: The dependency runs,
        Then: The claim is returned and no wallet lookup is issued, so the
            gate adds no query to the admin path.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="gina",
            role=UserRole.ADMIN,
            active_wallet_public_id="wallet-any",
        )

        result = await require_tradable_active_wallet(principal, mock_repo)

        assert result == "wallet-any"
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()


class TestResolveReadableActiveWallet:
    """Behaviour of the consumption-time gate on the READ plane.

    The claim is minted once, against the read plane, and then carried
    forward on every refresh. Consuming it as minted is the right plane
    at the wrong time: a revoked personal read grant would keep
    answering rows for as long as the holder kept refreshing. This
    resolver is what makes the wallet-scoped backtest readers
    revocation-immediate, the property every other read surface already
    had by calling ``resolve_readable_wallets`` per request.
    """

    @pytest.mark.asyncio
    async def test_missing_claim_is_refused_with_400(self) -> None:
        """A cleared wallet claim fails closed on the read path too.

        Given: A principal whose ``active_wallet_public_id`` is ``None``,
        When: The resolver runs,
        Then: 400 is raised with the same uniform detail the trade gate
            uses, and the read-plane lookup is never issued.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(username="hana", role=UserRole.VIEWER)

        with pytest.raises(HTTPException) as exc_info:
            await resolve_readable_active_wallet(principal, mock_repo)

        assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
        assert exc_info.value.detail == ACTIVE_WALLET_REQUIRED_DETAIL
        mock_repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_revoked_read_grant_is_refused_with_403(self) -> None:
        """A claim whose grant has been revoked stops working immediately.

        Given: An AI_REVIEWER with zero operator memberships whose claim
            names a wallet the read plane no longer returns, because the
            personal ``wallet_user_read_grants`` row was revoked after
            the claim was minted,
        When: The resolver runs,
        Then: 403 is raised — the revocation takes effect on the next
            request rather than on the client's next refresh.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(return_value=[])
        principal = AuthPrincipal(
            username="iris",
            role=UserRole.AI_REVIEWER,
            user_public_id="user-iris",
            active_wallet_public_id="wallet-read-only",
        )

        with pytest.raises(HTTPException) as exc_info:
            await resolve_readable_active_wallet(principal, mock_repo)

        assert exc_info.value.status_code == status.HTTP_403_FORBIDDEN
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_granted_wallet_is_returned(self) -> None:
        """A live personal read grant still answers, with no membership.

        Given: An AI_REVIEWER with zero operator memberships whose claim
            names a wallet the read plane still returns,
        When: The resolver runs,
        Then: The wallet id is returned. Read authority conferring read
            visibility is the feature the plane split exists to deliver,
            so the gate must not narrow it to the operator plane.
        """
        mock_repo = AsyncMock()
        mock_repo.list_readable_wallets_for_user = AsyncMock(
            return_value=[_wallet_row("wallet-read-only")]
        )
        principal = AuthPrincipal(
            username="jonas",
            role=UserRole.AI_REVIEWER,
            user_public_id="user-jonas",
            active_wallet_public_id="wallet-read-only",
        )

        result = await resolve_readable_active_wallet(principal, mock_repo)

        assert result == "wallet-read-only"
        mock_repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_global_scope_passes_without_a_lookup(self) -> None:
        """Global scope keeps its unscoped answer on the read path too.

        Given: An ADMIN principal with a wallet claim,
        When: The resolver runs,
        Then: The claim is returned and no wallet lookup is issued, so
            the per-request re-resolution adds no query to the admin
            path.
        """
        mock_repo = AsyncMock()
        principal = AuthPrincipal(
            username="kira",
            role=UserRole.ADMIN,
            active_wallet_public_id="wallet-any",
        )

        result = await resolve_readable_active_wallet(principal, mock_repo)

        assert result == "wallet-any"
        mock_repo.list_readable_wallets_for_user.assert_not_called()
