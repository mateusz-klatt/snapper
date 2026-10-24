"""Public selected-key identity evidence without API keys or secret response bodies.

Identity is observed during one committed native account read attempt. A first
retained proof anchors its local wallet family to the observed external account;
later contradictory proofs remain auditable but cannot silently replace it.
Read identity, trading permission and available collateral are separate facts.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import native_account_read_attempt_digest

_IDENTITY_DOMAIN = b"snapper:native-account-public-identity:v1\x00"
type NativeKeyAccess = Literal["NO_ACCESS", "READ_ONLY", "FULL_ACCESS"]


class _PublicIdentityEvidence(BaseModel):
    """Retain only explicit public fields and reject coercion, extra secrets or unchecked mutation."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class NativeAccountPublicIdentity(_PublicIdentityEvidence):
    """Whitelisted result of an actual key-match check inside the authenticated HTTP producer.

    The returned API key is compared with the selected signed request in memory
    and never enters this value, logs, exception evidence or an ordinary raw sink.
    Permissions are observed source values, not an instruction to enable trading.
    """

    source_policy: Literal["kraken_futures_key_check_redacted_v1"] = (
        "kraken_futures_key_check_redacted_v1"
    )
    client_selection: ExecutionClientSelection
    account_uid: EvidenceUUID
    general_access: NativeKeyAccess
    transfer_access: NativeKeyAccess
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_actual_source(self) -> Self:
        """Require the same supported authenticated source family and honest original clocks.

        Returns:
            The same immutable public observation without its sensitive response material.
        """
        selected = self.client_selection
        origins = {
            "production": "https://futures.kraken.com",
            "demo": "https://demo-futures.kraken.com",
        }
        if (
            selected.exchange != "kraken_futures"
            or selected.mode != "live"
            or selected.environment not in origins
            or selected.public_api_origin != origins[selected.environment]
        ):
            raise ValueError("Native identity source route is unproven")
        if (
            selected.credential_timestamp is None
            or not selected.credential_timestamp <= self.request_started_at <= self.observed_at
        ):
            raise ValueError("Native identity source chronology is inconsistent")
        return self


class NativeAccountIdentityRootReference(_PublicIdentityEvidence):
    """Independently rebound original owner proof, rather than a caller-selected replacement account."""

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    account_uid: EvidenceUUID


class NativeAccountIdentityProofPayload(_PublicIdentityEvidence):
    """One retained public identity and its actual account-read attempt and immutable owner root."""

    schema_version: Literal[1] = 1
    attempt_public_id: EvidenceUUID
    attempt_content_digest: EvidenceDigest
    attempt: NativeAccountReadAttemptPayload
    observation: NativeAccountPublicIdentity
    recorded_at: EvidenceInstant
    owner_root: NativeAccountIdentityRootReference | None

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Refuse boolean or floating version aliases before the literal discriminator."""
        if type(value) is not int:
            raise ValueError("Native identity version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_attempt_binding(self) -> Self:
        """Preserve actual selection and pre-network request ordering without hiding owner conflicts.

        Returns:
            The original public evidence, including an honestly contradictory observed account UID.
        """
        if self.attempt_content_digest != native_account_read_attempt_digest(self.attempt):
            raise ValueError("Native identity attempt digest is inconsistent")
        if self.observation.client_selection != self.attempt.client_selection:
            raise ValueError("Native identity belongs to a different selected client")
        if (
            not self.attempt.started_at
            <= self.observation.request_started_at
            <= self.observation.observed_at
            <= self.recorded_at
        ):
            raise ValueError("Native identity is outside its actual attempt knowledge interval")
        return self

    @property
    def matches_owner(self) -> bool:
        """Keep an observed account conflict explicit without rewriting its original evidence.

        Returns:
            Whether this first proof or rebound owner root names the same observed external account.
        """
        return (
            self.owner_root is None or self.owner_root.account_uid == self.observation.account_uid
        )


def canonical_native_account_identity(payload: NativeAccountIdentityProofPayload) -> str:
    """Serialize only the typed public proof; sensitive original key responses are never accepted.

    Args:
        payload: Strict public observation, original request and optional independently verified root.

    Returns:
        Stable canonical JSON containing no API key, signature, bank identifier or source body.
    """
    proven = NativeAccountIdentityProofPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_account_identity_digest(payload: NativeAccountIdentityProofPayload) -> str:
    """Hash public identity evidence under a domain distinct from requests, raw receipts and money.

    Args:
        payload: The exact immutable redacted identity proof.

    Returns:
        Versioned SHA-256 of its canonical public fields, without hashing a secret API key.
    """
    return hashlib.sha256(
        _IDENTITY_DOMAIN + canonical_native_account_identity(payload).encode()
    ).hexdigest()
