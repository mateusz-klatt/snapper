"""Tests for refresh-token wallet-hint validation.

Covers ``_apply_wallet_hint`` (role-branched membership validation,
404 on foreign wallet, model_copy projection) and the
``RefreshTokenPayload`` field/model validators.
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
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import RefreshTokenPayload

_OWN_WALLET = "01948f94-0001-7a00-8000-000000000001"
_FOREIGN_WALLET = "01948f94-0001-7a00-8000-0000000000ff"


def _principal(
    role: UserRole = UserRole.OPERATOR,
    active_wallet: str | None = None,
    operator_public_ids: list[str] | None = None,
) -> AuthPrincipal:
    """Build an AuthPrincipal fixture."""
    return AuthPrincipal(
        username="u",
        role=role,
        active_wallet_public_id=active_wallet,
        operator_public_ids=operator_public_ids or ["op-1"],
    )


def _repo(
    rows: list[dict[str, str]] | None = None, admin_rows: list[dict[str, str]] | None = None
) -> MagicMock:
    """Build a Repository mock returning configured wallet rows."""
    repo = MagicMock()
    repo.list_active_wallets = AsyncMock(return_value=admin_rows or [])
    repo.list_accessible_wallets_for_operators = AsyncMock(return_value=rows or [])
    return repo


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
    """Coverage for the role-branched membership validator."""

    @pytest.mark.asyncio
    async def test_no_hint_returns_principal_unchanged(self) -> None:
        """Empty payload preserves the existing claim byte-identically.

        Given: A payload with no fields set,
        When: _apply_wallet_hint runs,
        Then: The original principal is returned unchanged.
        """
        principal = _principal(active_wallet=_OWN_WALLET)
        payload = RefreshTokenPayload()
        repo = _repo()
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result is principal
        repo.list_active_wallets.assert_not_called()
        repo.list_accessible_wallets_for_operators.assert_not_called()

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
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_admin_uses_accessible_wallets(self) -> None:
        """Non-admin role calls list_accessible_wallets_for_operators.

        Given: An OPERATOR principal hinting at an accessible wallet,
        When: _apply_wallet_hint runs,
        Then: The accessible-wallets query runs and the hint applies.
        """
        principal = _principal(role=UserRole.OPERATOR)
        payload = RefreshTokenPayload(active_wallet_public_id=_OWN_WALLET)
        repo = _repo(rows=[{"public_id": _OWN_WALLET}])
        result = await _apply_wallet_hint(payload, principal, repo)
        assert result.active_wallet_public_id == _OWN_WALLET
        repo.list_accessible_wallets_for_operators.assert_awaited_once()
        repo.list_active_wallets.assert_not_called()

    @pytest.mark.asyncio
    async def test_foreign_wallet_returns_404(self) -> None:
        """Hinted wallet outside caller scope → 404 (no info leak).

        Given: A non-admin principal hinting at a wallet they cannot see,
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
