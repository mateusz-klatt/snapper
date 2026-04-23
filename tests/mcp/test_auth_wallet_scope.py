"""Tests for :func:`snapper.mcp.auth.validate_user_wallet_scope`.

Per-request wallet-scope re-validation gate. The
helper is called on every MCP tool invocation that touches a
specific wallet so a grant revoked after login immediately blocks
the next tool call even with a still-valid JWT.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.mcp.auth import OPERATOR_SCOPE_ERROR_CODE
from snapper.mcp.auth import WALLET_SCOPE_ERROR_CODE
from snapper.mcp.auth import ensure_operator_in_claims
from snapper.mcp.auth import validate_user_wallet_scope


def _claims(
    role: UserRole = UserRole.AI_DELEGATE,
    operator_public_ids: list[str] | None = None,
) -> TokenClaims:
    """Build a :class:`TokenClaims` pinned to the given role + operators."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="u1",
        username="u1",
        role=role,
        permissions=[],
        exp=now + 3600,
        iat=now,
        jti="jti",
        sid="sid",
        user_public_id="u1",
        operator_public_ids=operator_public_ids if operator_public_ids is not None else ["op-1"],
        primary_operator_public_id="op-1",
    )


class TestValidateUserWalletScope:
    """Coverage for the wallet-scope re-validation gate."""

    @pytest.mark.asyncio
    async def test_admin_bypasses_lookup(self) -> None:
        """ADMIN role short-circuits without hitting the repository.

        Given: a caller whose role is ADMIN,
        When: the gate runs for any wallet,
        Then: it returns without calling
            ``list_accessible_wallets_for_operators`` — matches the
            REST ``resolve_target_wallets`` ADMIN bypass behaviour.
        """
        repo = AsyncMock()
        await validate_user_wallet_scope(
            _claims(role=UserRole.ADMIN, operator_public_ids=[]),
            wallet_public_id="any-wallet",
            repository=repo,
        )
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_operator_memberships_rejected(self) -> None:
        """No operator memberships → reject without repo lookup.

        Given: a non-ADMIN caller with an empty operator set,
        When: the gate runs,
        Then: PermissionError is raised citing the stable error code
            and the repository is never consulted.
        """
        repo = AsyncMock()
        with pytest.raises(PermissionError) as exc:
            await validate_user_wallet_scope(
                _claims(operator_public_ids=[]),
                wallet_public_id="w-1",
                repository=repo,
            )
        assert WALLET_SCOPE_ERROR_CODE in str(exc.value)
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_wallet_inside_scope_admits(self) -> None:
        """Wallet in the operator-accessible set → admit.

        Given: a caller whose operator covers the target wallet,
        When: the gate runs,
        Then: it returns without raising.
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "w-1"}, {"public_id": "w-2"}]
        )
        await validate_user_wallet_scope(
            _claims(),
            wallet_public_id="w-1",
            repository=repo,
        )
        repo.list_accessible_wallets_for_operators.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wallet_outside_scope_rejected(self) -> None:
        """Wallet not in the accessible set → PermissionError.

        Given: the repository returns a set that does not include the
            target wallet,
        When: the gate runs,
        Then: PermissionError surfaces the stable error code so FastMCP
            emits a structured tool error clients can branch on.
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[{"public_id": "w-2"}])
        with pytest.raises(PermissionError) as exc:
            await validate_user_wallet_scope(
                _claims(),
                wallet_public_id="w-1",
                repository=repo,
            )
        assert WALLET_SCOPE_ERROR_CODE in str(exc.value)
        assert "w-1" in str(exc.value)

    @pytest.mark.asyncio
    async def test_passes_operator_list_to_repository(self) -> None:
        """Repository receives the caller's full operator set.

        Given: a caller with two operator memberships,
        When: the gate runs,
        Then: both operator IDs are forwarded verbatim to
            ``list_accessible_wallets_for_operators``.
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[{"public_id": "w-x"}])
        await validate_user_wallet_scope(
            _claims(operator_public_ids=["op-a", "op-b"]),
            wallet_public_id="w-x",
            repository=repo,
        )
        args, _kwargs = repo.list_accessible_wallets_for_operators.call_args
        assert args[0] == ["op-a", "op-b"]

    @pytest.mark.asyncio
    async def test_as_of_is_forwarded_when_provided(self) -> None:
        """Explicit ``as_of`` overrides ``datetime.now(UTC)`` default.

        Given: a pinned bus time,
        When: the gate runs,
        Then: the repository lookup receives that exact instant — lets
            callers perform deterministic time-travel checks.
        """
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[{"public_id": "w-1"}])
        pinned = datetime(2026, 1, 1, tzinfo=UTC)
        await validate_user_wallet_scope(
            _claims(),
            wallet_public_id="w-1",
            repository=repo,
            as_of=pinned,
        )
        args, _kwargs = repo.list_accessible_wallets_for_operators.call_args
        assert args[1] == pinned


class TestEnsureOperatorInClaims:
    """Coverage for the operator-binding re-validation helper."""

    def test_none_selection_is_admitted(self) -> None:
        """``operator_public_id=None`` → caller defers to primary; admit.

        Given: a non-ADMIN caller who passes no operator selection,
        When: the gate runs,
        Then: it returns without raising so the tool can fall back to
            ``primary_operator_public_id`` from the claims.
        """
        ensure_operator_in_claims(_claims(), None)

    def test_admin_bypass(self) -> None:
        """ADMIN role → admitted even for operators not on their claims.

        Given: an ADMIN caller whose claim set happens to be empty,
        When: the gate runs against an arbitrary operator,
        Then: it returns without raising — ADMIN implicitly covers
            every operator, matching the wallet-gate ADMIN bypass.
        """
        ensure_operator_in_claims(
            _claims(role=UserRole.ADMIN, operator_public_ids=[]),
            "op-arbitrary",
        )

    def test_operator_in_claims_admitted(self) -> None:
        """Operator present in claims → admit.

        Given: a delegate whose claims list ``op-1``,
        When: they select ``op-1``,
        Then: the gate returns.
        """
        ensure_operator_in_claims(_claims(operator_public_ids=["op-1"]), "op-1")

    def test_operator_not_in_claims_rejected(self) -> None:
        """Operator outside the claim set → PermissionError with stable code.

        Given: a delegate whose claims list only ``op-1``,
        When: they select ``op-2``,
        Then: the gate raises PermissionError citing
            :data:`OPERATOR_SCOPE_ERROR_CODE` — the stable classifier
            FastMCP tool errors surface to clients.
        """
        with pytest.raises(PermissionError) as exc:
            ensure_operator_in_claims(
                _claims(operator_public_ids=["op-1"]),
                "op-2",
            )
        assert OPERATOR_SCOPE_ERROR_CODE in str(exc.value)
        assert "op-2" in str(exc.value)
