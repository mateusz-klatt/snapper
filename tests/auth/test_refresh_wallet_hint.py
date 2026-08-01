"""Tests for refresh-token desk-scope and wallet-hint validation.

Covers ``_apply_wallet_hint`` (role-branched membership validation,
404 on foreign wallet, model_copy projection, and re-validation of the
claim carried across a hint-less refresh) and the
``RefreshTokenPayload`` field/model validators. It also proves refresh
rotation can shrink, but never widen, the authenticated desk scope.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import _apply_wallet_hint
from snapper.auth.routes import _constrain_refresh_memberships
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import RefreshTokenPayload
from snapper.auth.schemas.tokens import TokenClaims

_OWN_WALLET = "01948f94-0001-7a00-8000-000000000001"
_FOREIGN_WALLET = "01948f94-0001-7a00-8000-0000000000ff"


def _principal(
    role: UserRole = UserRole.OPERATOR,
    active_wallet: str | None = None,
    operator_public_ids: list[str] | None = None,
    operator_membership_public_ids: dict[str, str] | None = None,
) -> AuthPrincipal:
    """Build an AuthPrincipal fixture."""
    return AuthPrincipal(
        username="u",
        role=role,
        active_wallet_public_id=active_wallet,
        operator_public_ids=["op-1"] if operator_public_ids is None else operator_public_ids,
        operator_membership_public_ids=operator_membership_public_ids or {},
    )


def _repo(
    rows: list[dict[str, str]] | None = None, admin_rows: list[dict[str, str]] | None = None
) -> MagicMock:
    """Build a Repository mock returning configured wallet rows."""
    repo = MagicMock()
    repo.list_active_wallets = AsyncMock(return_value=admin_rows or [])
    repo.list_readable_wallets_for_user = AsyncMock(return_value=rows or [])
    return repo


def _claims(
    operator_public_ids: list[str],
    primary_operator_public_id: str,
    operator_membership_public_ids: dict[str, str] | None = None,
) -> TokenClaims:
    """Build verified refresh claims with one carried desk scope."""
    return TokenClaims(
        sub="user-1",
        username="u",
        role=UserRole.OPERATOR,
        exp=2_000_000_000,
        iat=1_900_000_000,
        jti="refresh_scope-test",
        sid="session-1",
        user_public_id="user-1",
        operator_public_ids=operator_public_ids,
        operator_membership_public_ids=operator_membership_public_ids or {},
        primary_operator_public_id=primary_operator_public_id,
    )


class TestConstrainRefreshMemberships:
    """Refresh rotation preserves the existing session's desk ceiling."""

    def test_live_role_promotion_requires_explicit_login(self) -> None:
        """Refresh cannot turn an OPERATOR session into global ADMIN authority.

        Given: An OPERATOR refresh token and a principal rebuilt after the
            account was promoted to ADMIN in the database.
        When: Refresh authority is constrained.
        Then: Rotation fails with 401 so only explicit login can acquire the
            new role and its structural impersonation permission.
        """
        principal = _principal(role=UserRole.ADMIN)
        claims = _claims(["op-1"], "op-1")

        with pytest.raises(HTTPException) as exc_info:
            _constrain_refresh_memberships(principal, claims)

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == "Session authority changed; sign in again"

    def test_new_database_membership_does_not_widen_the_session(self) -> None:
        """A newly attached desk remains unavailable until explicit login.

        Given: A live principal rebuilt with both the old and newly attached desks,
            while the verified refresh token carries only the old desk.
        When: The refresh membership scope is constrained.
        Then: Only the old desk and its primary marker survive rotation.
        """
        principal = _principal(
            operator_public_ids=["desk-old", "desk-new"],
            operator_membership_public_ids={
                "desk-old": "membership-old-current",
                "desk-new": "membership-new-current",
            },
        )
        result = _constrain_refresh_memberships(
            principal,
            _claims(["desk-old"], "desk-old"),
        )
        assert result.operator_public_ids == ["desk-old"]
        assert result.operator_membership_public_ids == {"desk-old": "membership-old-current"}
        assert result.primary_operator_public_id == "desk-old"

    def test_replaced_membership_generation_does_not_resurrect_the_session(self) -> None:
        """A versioned refresh token cannot adopt a post-detach membership row.

        Given: A refresh token bound to an old membership generation and a
            live principal rebuilt after detach and re-attachment to the same desk.
        When: Refresh authority is constrained.
        Then: The desk, generation, and primary marker are all removed rather
            than upgrading the existing session to the new grant.
        """
        principal = _principal(
            operator_public_ids=["desk"],
            operator_membership_public_ids={"desk": "membership-new"},
        )
        result = _constrain_refresh_memberships(
            principal,
            _claims(
                ["desk"],
                "desk",
                {"desk": "membership-old"},
            ),
        )

        assert result.operator_public_ids == []
        assert result.operator_membership_public_ids == {}
        assert result.primary_operator_public_id == ""

    def test_removed_membership_and_primary_are_not_carried_forward(self) -> None:
        """Refresh may narrow stale claims and clears an unauthorized primary.

        Given: Claims carrying one removed primary desk and one surviving desk.
        When: The database-backed principal contains only the survivor.
        Then: The removed desk disappears and the stale primary becomes empty.
        """
        principal = _principal(
            operator_public_ids=["desk-surviving"],
            operator_membership_public_ids={"desk-surviving": "membership-surviving-current"},
        )
        result = _constrain_refresh_memberships(
            principal,
            _claims(["desk-removed", "desk-surviving"], "desk-removed"),
        )
        assert result.operator_public_ids == ["desk-surviving"]
        assert result.operator_membership_public_ids == {
            "desk-surviving": "membership-surviving-current"
        }
        assert result.primary_operator_public_id == ""


class TestRefreshTokenPayloadValidators:
    """Coverage for the field + model validators on RefreshTokenPayload."""

    def test_empty_payload_default_constructible(self) -> None:
        """Default-construct succeeds — used by empty-body sentinel path.

        Given: No fields supplied,
        When: RefreshTokenPayload() is invoked,
        Then: Both fields hold their defaults (None / False).
        """
        payload = RefreshTokenPayload()
        assert payload.active_wallet_public_id is None
        assert payload.clear_active_wallet is False

    def test_clear_only_payload_valid(self) -> None:
        """clear_active_wallet alone is valid (mints null claim).

        Given: clear_active_wallet=True without an explicit wallet,
        When: RefreshTokenPayload validates,
        Then: The instance reports clear_active_wallet True.
        """
        payload = RefreshTokenPayload(clear_active_wallet=True)
        assert payload.clear_active_wallet is True

    def test_valid_uuid7_payload(self) -> None:
        """Canonical UUID7 wallet hint passes the field validator.

        Given: A UUID7-shaped wallet hint,
        When: RefreshTokenPayload validates,
        Then: The hint is preserved verbatim.
        """
        payload = RefreshTokenPayload(active_wallet_public_id=_OWN_WALLET)
        assert payload.active_wallet_public_id == _OWN_WALLET

    def test_malformed_uuid7_rejected(self) -> None:
        """Non-UUID7 hint raises before any DB lookup runs.

        Given: A non-UUID7 wallet hint,
        When: RefreshTokenPayload validates,
        Then: ValidationError is raised mentioning UUID7.
        """
        with pytest.raises(ValidationError, match="UUID7"):
            RefreshTokenPayload(active_wallet_public_id="not-a-uuid")

    def test_mutually_exclusive_fields_rejected(self) -> None:
        """active_wallet + clear_active_wallet together → ValidationError.

        Given: Both fields set,
        When: RefreshTokenPayload validates,
        Then: ValidationError is raised about exclusivity.
        """
        with pytest.raises(ValidationError, match="mutually exclusive"):
            RefreshTokenPayload(
                active_wallet_public_id=_OWN_WALLET,
                clear_active_wallet=True,
            )


class TestApplyWalletHint:
    """Coverage for the role-branched membership validator.

    Two paths reach the same plane: the hint supplied on this request,
    and the claim carried from the previous one. The second exists
    because the claim is minted once and then travels on every refresh,
    so mint-time validation alone left a revoked read grant live for as
    long as the client kept refreshing.
    """

    @pytest.mark.asyncio
    async def test_no_hint_preserves_a_still_readable_claim(self) -> None:
        """Empty payload preserves the existing claim byte-identically.

        Given: A payload with no fields set and a carried claim the
            caller can still see,
        When: _apply_wallet_hint runs,
        Then: The original principal is returned unchanged, so the
            common refresh keeps the user's wallet selection.
        """
        principal = _principal(active_wallet=_OWN_WALLET)
        payload = RefreshTokenPayload()
        repo = _repo(rows=[{"public_id": _OWN_WALLET}])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result is principal
        repo.list_readable_wallets_for_user.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_hint_drops_a_claim_that_is_no_longer_readable(self) -> None:
        """A revoked read grant stops being carried across refreshes.

        Given: A payload with no fields set and a carried claim naming a
            wallet the READ plane no longer returns (the personal
            ``wallet_user_read_grants`` row was revoked),
        When: _apply_wallet_hint runs,
        Then: The claim is dropped to None rather than re-minted. Mint
            time used to be the ONLY validation the claim ever got, and
            the documented zero-body callers refresh forever, so without
            this the revocation would never take effect on any surface
            reading the claim.
        """
        principal = _principal(active_wallet=_OWN_WALLET)
        payload = RefreshTokenPayload()
        repo = _repo(rows=[])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id is None
        repo.list_readable_wallets_for_user.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_no_hint_and_no_claim_skips_the_lookup(self) -> None:
        """Nothing to re-validate costs no query.

        Given: A payload with no fields set and no carried claim,
        When: _apply_wallet_hint runs,
        Then: The principal is returned unchanged and neither wallet
            lookup is issued, so the re-validation adds no query to the
            "All wallets" refresh.
        """
        principal = _principal(active_wallet=None)
        payload = RefreshTokenPayload()
        repo = _repo()
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result is principal
        repo.list_active_wallets.assert_not_called()
        repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_carried_claim_re_validates_against_active_wallets(self) -> None:
        """The carried-claim check uses the same role branch as the hint.

        Given: An ADMIN principal carrying a claim, and a repository
            whose ``list_active_wallets`` no longer returns it,
        When: _apply_wallet_hint runs with an empty payload,
        Then: The claim is dropped via the global-scope branch, and the
            operator-scoped lookup is never consulted — pinning that
            both paths share ``_readable_wallet_ids``.
        """
        principal = _principal(role=UserRole.ADMIN, active_wallet=_OWN_WALLET)
        payload = RefreshTokenPayload()
        repo = _repo(admin_rows=[])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id is None
        repo.list_active_wallets.assert_awaited_once()
        repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_clear_active_wallet_sets_claim_to_none(self) -> None:
        """clear_active_wallet projects the claim to None.

        Given: clear_active_wallet=True on the payload,
        When: _apply_wallet_hint runs,
        Then: The returned principal has active_wallet_public_id=None.
        """
        principal = _principal(active_wallet=_OWN_WALLET)
        payload = RefreshTokenPayload(clear_active_wallet=True)
        repo = _repo()
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id is None

    @pytest.mark.asyncio
    async def test_admin_uses_list_active_wallets(self) -> None:
        """ADMIN role calls list_active_wallets, not the operator-scoped variant.

        Given: An ADMIN principal hinting at a wallet,
        When: _apply_wallet_hint runs,
        Then: list_active_wallets is queried, not list_accessible_*.
        """
        principal = _principal(role=UserRole.ADMIN, active_wallet=None)
        payload = RefreshTokenPayload(active_wallet_public_id=_OWN_WALLET)
        repo = _repo(admin_rows=[{"public_id": _OWN_WALLET}])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id == _OWN_WALLET
        repo.list_active_wallets.assert_awaited_once()
        repo.list_readable_wallets_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_admin_uses_readable_wallets(self) -> None:
        """Non-admin role calls list_readable_wallets_for_user.

        Given: An OPERATOR principal hinting at a readable wallet,
        When: _apply_wallet_hint runs,
        Then: The read-plane query runs, carrying the caller's user id
            and operator set, and the hint applies.
        """
        principal = _principal(role=UserRole.OPERATOR)
        payload = RefreshTokenPayload(active_wallet_public_id=_OWN_WALLET)
        repo = _repo(rows=[{"public_id": _OWN_WALLET}])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id == _OWN_WALLET
        repo.list_readable_wallets_for_user.assert_awaited_once()
        call = repo.list_readable_wallets_for_user.await_args
        assert call.args[0] == principal.user_public_id
        assert call.args[1] == ["op-1"]
        repo.list_active_wallets.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_granted_wallet_hint_survives_zero_memberships(self) -> None:
        """A membership-less user may restore a read-granted wallet hint.

        Given: A VIEWER with an EMPTY operator set whose only visibility
            of the hinted wallet is a personal read grant,
        When: _apply_wallet_hint runs,
        Then: The hint applies instead of 404, and the read-plane query
            was asked with the empty operator list.
        """
        principal = _principal(role=UserRole.VIEWER, operator_public_ids=[])
        payload = RefreshTokenPayload(active_wallet_public_id=_OWN_WALLET)
        repo = _repo(rows=[{"public_id": _OWN_WALLET}])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id == _OWN_WALLET
        call = repo.list_readable_wallets_for_user.await_args
        assert call.args[1] == []

    @pytest.mark.asyncio
    async def test_foreign_wallet_returns_404(self) -> None:
        """Hinted wallet outside caller scope → 404 (no info leak).

        Given: A non-admin principal hinting at a wallet they cannot read,
        When: _apply_wallet_hint runs,
        Then: HTTPException 404 with uniform "wallet not found" detail.
        """
        principal = _principal(role=UserRole.OPERATOR)
        payload = RefreshTokenPayload(active_wallet_public_id=_FOREIGN_WALLET)
        repo = _repo(rows=[{"public_id": _OWN_WALLET}])
        with pytest.raises(HTTPException) as exc_info:
            await _apply_wallet_hint(payload, principal, repo)
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "wallet not found"

    @pytest.mark.asyncio
    async def test_admin_foreign_wallet_returns_404(self) -> None:
        """ADMIN hinting at a wallet not in list_active_wallets is also denied.

        Given: ADMIN principal but the hinted wallet is absent (e.g. archived),
        When: _apply_wallet_hint runs,
        Then: HTTPException 404 fires regardless of role.
        """
        principal = _principal(role=UserRole.ADMIN)
        payload = RefreshTokenPayload(active_wallet_public_id=_OWN_WALLET)
        repo = _repo(admin_rows=[])
        _ = datetime.now(UTC)
        with pytest.raises(HTTPException) as exc_info:
            await _apply_wallet_hint(payload, principal, repo)
        assert exc_info.value.status_code == 404
