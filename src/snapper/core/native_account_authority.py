"""Immutable native account identity with exact, observed REST wallet-name binding.

This source family preserves the legacy route-authority payload and hash.
Its selected-key proof must be loaded from the closed producer's independently
retained safe public record. Model validation alone establishes no source fact,
current freshness, collateral, order permission or permission to resend.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionRouteSelection
from snapper.core.execution_route_authority import PublicApiOrigin
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID

_NATIVE_AUTHORITY_DOMAIN = b"snapper:native-account-authority:v1\x00"
_NATIVE_ORIGINS = {
    "production": "https://futures.kraken.com",
    "demo": "https://demo-futures.kraken.com",
}


class _NativeAuthorityEvidence(BaseModel):
    """Refuse coercion, unchecked mutable updates and unknown native proof fields."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class NativeAuthorityOwnerReference(_NativeAuthorityEvidence):
    """The real same-attempt safe key-check proof, without any retained signing key."""

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    read_attempt_public_id: EvidenceUUID
    account_uid: EvidenceUUID
    observed_at: EvidenceInstant


class NativeAuthorityProfile(_NativeAuthorityEvidence):
    """A literal REST wallet and complete exact profile digest, without derived permissions."""

    native_account_key: EvidenceText
    account_type: Literal["cashAccount", "marginAccount", "multiCollateralMarginAccount"]
    settlement_currency: EvidenceText | None
    content_digest: EvidenceDigest

    @model_validator(mode="after")
    def validate_native_profile(self) -> Self:
        """Retain documented settlement metadata without inventing it for other profile types.

        Returns:
            The same internally consistent literal native profile reference.
        """
        if (self.account_type == "marginAccount") != (self.settlement_currency is not None):
            raise ValueError("Only a single-collateral profile carries its settlement currency")
        return self


class NativeAuthorityReadReference(_NativeAuthorityEvidence):
    """The actual committed accounts read and retained original successful response."""

    attempt_public_id: EvidenceUUID
    attempt_content_digest: EvidenceDigest
    family_sequence: int = Field(ge=1)
    outcome_public_id: EvidenceUUID
    outcome_content_digest: EvidenceDigest
    raw_receipt_public_id: EvidenceUUID
    raw_content_digest: EvidenceDigest
    request_started_at: EvidenceInstant
    source_observed_at: EvidenceInstant
    source_recorded_at: EvidenceInstant
    server_time_text: EvidenceText
    server_time: EvidenceInstant
    parser_policy: Literal[
        "kraken_futures_accounts_v1", "kraken_futures_accounts_prepared_key_v2"
    ] = "kraken_futures_accounts_v1"

    @model_validator(mode="after")
    def validate_original_clocks(self) -> Self:
        """Keep source observation and durable recording distinct from later authority issuance.

        Returns:
            The same monotonically observed source reference without a freshness verdict.
        """
        if not self.request_started_at <= self.source_observed_at <= self.source_recorded_at:
            raise ValueError("Native authority request, observation and recording clocks disagree")
        return self


class NativeAccountAuthorityPayload(ExecutionRouteSelection):
    """Actual native account identity, with independently rebound source and safe owner proof.

    The exact wallet-name bridge is narrower than a WS alias or product-margin
    route. A fresh identity record does not establish complete accounted cash,
    positions, account regime, collateral, order permission or instrument eligibility.
    """

    schema_version: Literal[1] = 1
    source_kind: Literal["kraken_futures_native_accounts_v1"] = "kraken_futures_native_accounts_v1"
    credential_public_id: EvidenceUUID
    credential_timestamp: EvidenceInstant
    public_api_origin: PublicApiOrigin
    venue_account_id: EvidenceUUID
    owner_proof: NativeAuthorityOwnerReference
    native_read: NativeAuthorityReadReference
    native_profile: NativeAuthorityProfile
    wallet_name_policy: Literal["kraken_futures_exact_wallet_name_v1"] = (
        "kraken_futures_exact_wallet_name_v1"
    )
    observed_at: EvidenceInstant
    valid_until: EvidenceInstant
    route_revision: EvidenceDigest

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_integer_version(cls, value: object) -> object:
        """Refuse boolean and floating aliases for the explicit immutable schema version.

        Args:
            value: Original discriminator supplied at the authority boundary.

        Returns:
            The same strict integer before supported-version validation.
        """
        if type(value) is not int:
            raise ValueError("Native authority schema version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_native_route(self) -> Self:
        """Keep literal names and selected authenticated environment internally consistent.

        Returns:
            The same source-family-specific authority without a current-source verdict.
        """
        scope = self.scope
        expected_origin = _NATIVE_ORIGINS.get(scope.environment)
        if (
            scope.exchange != "kraken_futures"
            or scope.mode != "live"
            or expected_origin is None
            or self.public_api_origin != expected_origin
        ):
            raise ValueError("Native authority requires its actual supported Futures route")
        if scope.margin_account != self.native_profile.native_account_key:
            raise ValueError("Native scope must retain its exact documented REST wallet name")
        expected_revision = route_selection_digest(
            scope,
            self.credential_public_id,
            self.credential_timestamp,
            self.public_api_origin,
        )
        if self.route_revision != expected_revision:
            raise ValueError("Native authority route revision disagrees with its selected client")
        return self

    @model_validator(mode="after")
    def validate_owner_and_source(self) -> Self:
        """Require one actual read attempt and preserve original source freshness clocks.

        Returns:
            The same immutable references, pending actual store-side graph verification.
        """
        owner = self.owner_proof
        source = self.native_read
        if (
            owner.read_attempt_public_id != source.attempt_public_id
            or owner.account_uid != self.venue_account_id
        ):
            raise ValueError("Native authority owner proof must belong to the same read attempt")
        if not self.credential_timestamp <= owner.observed_at <= source.request_started_at:
            raise ValueError("Native authority key proof must precede its actual accounts request")
        if self.observed_at != source.source_observed_at or self.valid_until <= self.observed_at:
            raise ValueError("Native authority freshness must retain the original source clock")
        return self


def canonical_native_account_authority(payload: NativeAccountAuthorityPayload) -> str:
    """Serialize one exact native-family payload without changing the legacy v1 representation.

    Args:
        payload: Actual producer material whose source graph the store independently validates.

    Returns:
        Stable canonical native authority JSON under its distinct explicit schema.
    """
    proven = NativeAccountAuthorityPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_account_authority_digest(payload: NativeAccountAuthorityPayload) -> str:
    """Hash native authority under a distinct domain rather than reuse the legacy v1 digest.

    Args:
        payload: Source-family-specific immutable public authority evidence.

    Returns:
        Native authority SHA-256 over its versioned canonical payload.
    """
    material = canonical_native_account_authority(payload)
    return hashlib.sha256(_NATIVE_AUTHORITY_DOMAIN + material.encode()).hexdigest()


def client_selection_from_native_authority(
    payload: NativeAccountAuthorityPayload,
) -> ExecutionClientSelection:
    """Project validated native-family identity onto the actual public client selection.

    Args:
        payload: Native identity material whose immutable source graph is separately rebound.

    Returns:
        The explicit selected public client, without a margin alias or dispatch permission.
    """
    proven = NativeAccountAuthorityPayload.model_validate(payload)
    return ExecutionClientSelection(
        wallet_public_id=proven.scope.wallet_public_id,
        exchange=proven.scope.exchange,
        mode=proven.scope.mode,
        environment=proven.scope.environment,
        credential_public_id=proven.credential_public_id,
        credential_timestamp=proven.credential_timestamp,
        public_api_origin=proven.public_api_origin,
    )
