"""Pydantic schemas for multi-tenant read endpoints.

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

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
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


class WalletResponse(PayloadResponse[Literal["wallet_response"], WalletInfo]):
    """Singleton wrapper returned by ``POST /api/wallets``."""

    type: Literal["wallet_response"] = "wallet_response"


class CreateWalletBody(StrictBody):
    """Request body for ``POST /api/wallets``.

    Attributes:
        label: Human-readable wallet name (1-128 chars). The
            ``(label, is_paper)`` active-unique index enforces that
            two wallets sharing both fields cannot be active at the
            same time.
        description: Optional free-form description.
        is_paper: Paper-mode flag. Paper and live wallets may share
            the same label (e.g. ``default`` + ``default-paper``)
            provided they differ on ``is_paper``.
    """

    label: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=512)
    is_paper: bool = False


class CreateWalletCommand(PayloadRequest[Literal["create_wallet_command"], CreateWalletBody]):
    """Request envelope for ``POST /api/wallets``."""

    type: Literal["create_wallet_command"] = "create_wallet_command"


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


class ScopeGrantResponse(PayloadResponse[Literal["scope_grant_response"], ScopeGrantInfo]):
    """Singleton wrapper returned by ``POST /api/scope-grants``."""

    type: Literal["scope_grant_response"] = "scope_grant_response"


class HandoverScopeGrantResult(StrictBody):
    """Payload returned by ``POST /api/scope-grants/handover``.

    The handover is an atomic SCD2 close + insert: the source grant
    is closed (``known_to`` set to the handover timestamp) and a new
    active grant is inserted under the destination operator. Both
    rows are returned so the client can update caches without a
    second fetch.

    Attributes:
        closed_grant: The source grant with ``known_to`` stamped at
            the handover timestamp.
        new_grant: The newly-inserted active grant under the
            destination operator.
    """

    closed_grant: ScopeGrantInfo
    new_grant: ScopeGrantInfo


class HandoverScopeGrantResponse(
    PayloadResponse[Literal["handover_scope_grant_response"], HandoverScopeGrantResult]
):
    """Envelope wrapper for the handover result."""

    type: Literal["handover_scope_grant_response"] = "handover_scope_grant_response"


class CreateScopeGrantBody(StrictBody):
    """Request body for ``POST /api/scope-grants``.

    Exactly one of ``underlying_public_id`` / ``instrument_public_id``
    must be supplied, matching the chosen ``scope_kind``. The server
    fills in ``granted_by_user_public_id`` from
    ``principal.user_public_id`` so the client never supplies an
    audit identity it could spoof.

    Attributes:
        operator_public_id: The operator being granted scope.
        wallet_public_id: The wallet covered by the grant.
        scope_kind: Either ``"underlying"`` or ``"instrument"``.
        underlying_public_id: Set iff ``scope_kind == "underlying"``.
        instrument_public_id: Set iff ``scope_kind == "instrument"``.
        note: Optional free-form audit note.
    """

    operator_public_id: str = Field(min_length=1, max_length=64)
    wallet_public_id: str = Field(min_length=1, max_length=64)
    scope_kind: Literal["underlying", "instrument"]
    underlying_public_id: str | None = Field(default=None, max_length=64)
    instrument_public_id: str | None = Field(default=None, max_length=64)
    note: str | None = Field(default=None, max_length=512)


class CreateScopeGrantCommand(
    PayloadRequest[Literal["create_scope_grant_command"], CreateScopeGrantBody]
):
    """Request envelope for ``POST /api/scope-grants``."""

    type: Literal["create_scope_grant_command"] = "create_scope_grant_command"


class HandoverScopeGrantBody(StrictBody):
    """Request body for ``POST /api/scope-grants/handover``.

    Attributes:
        from_grant_public_id: Public ID of the active source grant.
        to_operator_public_id: Public ID of the destination operator.
        reason: Optional free-form audit note recorded on the new
            grant's ``note`` column.
    """

    from_grant_public_id: str = Field(min_length=1, max_length=64)
    to_operator_public_id: str = Field(min_length=1, max_length=64)
    reason: str | None = Field(default=None, max_length=512)


class HandoverScopeGrantCommand(
    PayloadRequest[Literal["handover_scope_grant_command"], HandoverScopeGrantBody]
):
    """Request envelope for ``POST /api/scope-grants/handover``."""

    type: Literal["handover_scope_grant_command"] = "handover_scope_grant_command"


class RevokeScopeGrantBody(StrictBody):
    """Request body for ``POST /api/scope-grants/{grant_public_id}/revoke``.

    Attributes:
        reason: Optional free-form audit note forwarded verbatim to
            the ``admin.scope_revoked`` event payload. Not persisted
            on the closed grant row (SCD2 close in place, no new row).
    """

    reason: str | None = Field(default=None, max_length=512)


class RevokeScopeGrantCommand(
    PayloadRequest[Literal["revoke_scope_grant_command"], RevokeScopeGrantBody]
):
    """Request envelope for ``POST /api/scope-grants/{grant_public_id}/revoke``."""

    type: Literal["revoke_scope_grant_command"] = "revoke_scope_grant_command"


class RevokeScopeGrantResponse(
    PayloadResponse[Literal["revoke_scope_grant_response"], ScopeGrantInfo]
):
    """Envelope wrapper for the closed grant projection.

    The payload is the grant as it exists immediately after the SCD2
    close (``known_to`` stamped at the revoke timestamp). Clients can
    update caches without a second round-trip.
    """

    type: Literal["revoke_scope_grant_response"] = "revoke_scope_grant_response"


class CredentialSummary(StrictDataSchema[Literal["credential_summary"]]):
    """Read projection of a wallet credential WITHOUT the encrypted payload.

    The ``encrypted_payload`` column is intentionally excluded so the
    API never surfaces ciphertext. The frontend credential tab shows
    label / exchange / credential_type and offers a "Rotate" action;
    the actual secret never leaves the server.

    Attributes:
        type: Payload item type discriminator.
        wallet_public_id: Owning wallet.
        exchange: Exchange identifier (lowercase).
        credential_type: One of ``api_key_secret`` / ``rsa_pem`` /
            ``oauth`` / ``paper``.
        label: Human-readable description (null when unset).
    """

    type: Literal["credential_summary"] = "credential_summary"
    wallet_public_id: str
    exchange: str
    credential_type: str
    label: str | None = None


class CredentialListResponse(
    PayloadListResponse[Literal["credential_list_response"], CredentialSummary]
):
    """List wrapper for ``GET /api/wallets/{id}/credentials``."""

    type: Literal["credential_list_response"] = "credential_list_response"


class CredentialResponse(PayloadResponse[Literal["credential_response"], CredentialSummary]):
    """Singleton wrapper for create / rotate responses."""

    type: Literal["credential_response"] = "credential_response"


class CreateCredentialBody(StrictBody):
    """Request body for ``POST /api/wallets/{id}/credentials``.

    The ``payload`` field carries the plaintext credential JSON that
    will be Fernet-encrypted server-side before DB insert. The shape
    is polymorphic on ``credential_type``:

    - ``api_key_secret`` → ``{"api_key": ..., "api_secret": ...}``
    - ``rsa_pem`` → ``{"api_key": ..., "private_key_pem": ...}``
    - ``paper`` → ``{"initial_balance": ...}``
    - ``oauth`` → ``{"client_id": ..., "client_secret": ..., "refresh_token": ...}``

    The server validates required keys per credential_type before
    accepting the payload.

    Attributes:
        exchange: Exchange identifier (lowercase enforced server-side).
        credential_type: One of the four supported types.
        credential_payload: Plaintext JSON dict that will be encrypted.
        label: Optional human-readable description.
    """

    exchange: str = Field(min_length=1, max_length=20)
    credential_type: Literal["api_key_secret", "rsa_pem", "oauth", "paper"]
    credential_payload: dict[str, str] = Field(
        description="Plaintext credential fields, encrypted server-side"
    )
    label: str | None = Field(default=None, max_length=128)


class CreateCredentialCommand(
    PayloadRequest[Literal["create_credential_command"], CreateCredentialBody]
):
    """Request envelope for ``POST /api/wallets/{id}/credentials``."""

    type: Literal["create_credential_command"] = "create_credential_command"


class RotateCredentialBody(StrictBody):
    """Request body for ``POST /api/wallets/{id}/credentials/{cid}/rotate``.

    Same polymorphic payload as create — the new plaintext credential
    fields replace the old ones. The old credential row is SCD2-closed
    and a fresh row is inserted.

    Attributes:
        credential_payload: New plaintext credential fields.
        label: Optional updated human-readable description (None
            preserves the existing label).
    """

    credential_payload: dict[str, str] = Field(
        description="New plaintext credential fields, encrypted server-side"
    )
    label: str | None = Field(default=None, max_length=128)


class RotateCredentialCommand(
    PayloadRequest[Literal["rotate_credential_command"], RotateCredentialBody]
):
    """Request envelope for ``POST /api/wallets/{id}/credentials/{cid}/rotate``."""

    type: Literal["rotate_credential_command"] = "rotate_credential_command"
