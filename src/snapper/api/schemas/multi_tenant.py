"""Pydantic schemas for Phase 0d multi-tenant read endpoints.

Carries the minimal projection of the Wallet / Operator / Scope Grant
tables needed by the frontend operator/wallet pickers and the admin
Scope Grants + Wallet Credentials tabs. Credential encrypted payloads
are explicitly NOT included in any schema here — credential handling
lives on separate write endpoints and never surfaces plaintext or
ciphertext through the read surface.

Each domain object is wrapped in a ``PayloadListResponse`` carrying
session / sequence / public_id provenance so gap detection on the
REST stream stays uniform with the rest of the API.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import StrictDataSchema


class WalletInfo(StrictDataSchema[Literal["wallet_info"]]):
    """Read projection of a single ``wallets`` SCD2 row.

    Attributes:
        type: Payload item type discriminator.
        label: Human-readable wallet name (``default``, ``firm``...).
        description: Optional free-form description.
        is_paper: Paper-mode flag. Paper and live wallets sharing the
            same label are disambiguated by this boolean because the
            wallets table's active-unique index is
            ``(label, is_paper)``.
    """

    type: Literal["wallet_info"] = "wallet_info"
    label: str
    description: str | None = None
    is_paper: bool


class WalletListResponse(PayloadListResponse[Literal["wallet_list_response"], WalletInfo]):
    """List wrapper for ``GET /api/wallets``."""

    type: Literal["wallet_list_response"] = "wallet_list_response"


class OperatorInfo(StrictDataSchema[Literal["operator_info"]]):
    """Read projection of a single ``operators`` SCD2 row.

    Attributes:
        type: Payload item type discriminator.
        label: Human-readable operator name.
        description: Optional free-form description.
    """

    type: Literal["operator_info"] = "operator_info"
    label: str
    description: str | None = None


class OperatorListResponse(PayloadListResponse[Literal["operator_list_response"], OperatorInfo]):
    """List wrapper for ``GET /api/operators``."""

    type: Literal["operator_list_response"] = "operator_list_response"


class ScopeGrantInfo(StrictDataSchema[Literal["scope_grant_info"]]):
    """Read projection of a single ``wallet_operator_scope_grants`` SCD2 row.

    Exactly one of ``underlying_public_id`` / ``instrument_public_id``
    is non-null, matching ``scope_kind``, enforced by the CHECK
    constraint on the source table.

    Attributes:
        type: Payload item type discriminator.
        operator_public_id: Operator holding the grant.
        wallet_public_id: Wallet covered by the grant.
        granted_by_user_public_id: Audit identity that created the grant.
        scope_kind: Either ``"underlying"`` or ``"instrument"``.
        underlying_public_id: Set iff ``scope_kind == "underlying"``.
        instrument_public_id: Set iff ``scope_kind == "instrument"``.
        note: Free-form audit note (null when unset).
        known_to: SCD2 end-of-validity timestamp — the sentinel
            ``9999-12-31 23:59:59`` indicates an active grant.
    """

    type: Literal["scope_grant_info"] = "scope_grant_info"
    operator_public_id: str
    wallet_public_id: str
    granted_by_user_public_id: str
    scope_kind: str
    underlying_public_id: str | None = None
    instrument_public_id: str | None = None
    note: str | None = None
    known_to: datetime


class ScopeGrantListResponse(
    PayloadListResponse[Literal["scope_grant_list_response"], ScopeGrantInfo]
):
    """List wrapper for ``GET /api/scope-grants``."""

    type: Literal["scope_grant_list_response"] = "scope_grant_list_response"
