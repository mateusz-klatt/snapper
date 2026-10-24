"""Separate immutable product-read requests and outcomes without financial raw-receipt reuse.

These public shapes require the actual closed transport and durable binders.
They establish neither authenticated provenance nor order permission by shape.
Product ordering is independent of accounts reads and spans credential rotation
and margin profiles inside the same actual wallet, venue and environment.
"""

import base64
import hashlib
import json
from typing import Annotated
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import accounting_hold_scope_digest
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID

_ATTEMPT_DOMAIN = b"snapper:futures-product-read-attempt:v1\x00"
_OUTCOME_DOMAIN = b"snapper:futures-product-read-outcome:v1\x00"
_MAX_ENTITY_BYTES = 8 * 1024 * 1024
_ORIGINS = {
    "production": "https://futures.kraken.com",
    "demo": "https://demo-futures.kraken.com",
}


class _ProductReadEvidence(BaseModel):
    """Reject coercion, extra authentication material and unchecked nested model copies."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class ProductReadKeyReference(_ProductReadEvidence):
    """Public reference to the actual previously committed same-key proof and immutable owner root.

    Actual final Requests PreparedRequest binding belongs to the closed issued
    token and transport boundary. A later selected credential or caller-created
    copy cannot establish that relationship from these public values alone.
    """

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    read_attempt_public_id: EvidenceUUID
    account_uid: EvidenceUUID
    client_selection: ExecutionClientSelection
    key_owner_root_public_id: EvidenceUUID
    key_owner_root_content_digest: EvidenceDigest
    observed_at: EvidenceInstant
    recorded_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_original_key_clock(self) -> Self:
        """Retain observed key knowledge before its actual original durable recording clock.

        Returns:
            The same immutable reference, still requiring the actual proof and root loader.
        """
        if self.observed_at > self.recorded_at:
            raise ValueError("Product key proof recording precedes its original observation")
        return self


class _VersionedProductRead(_ProductReadEvidence):
    """Keep the version distinct from Boolean and floating aliases."""

    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Require an actual integer before checking the supported literal discriminator."""
        if type(value) is not int:
            raise ValueError("Product read version must be an integer")
        return value


class FuturesProductReadAttempt(_VersionedProductRead):
    """A committed fixed-purpose request with its own coarse-family ordering coordinate."""

    purpose: Literal["kraken_futures.trading_instruments"] = "kraken_futures.trading_instruments"
    client_selection: ExecutionClientSelection
    key_proof: ProductReadKeyReference
    family_sequence: int = Field(ge=1)
    started_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_actual_selection(self) -> Self:
        """Preserve the exact supported client and earlier committed source-proof chronology.

        Returns:
            The same product request without asserting that network I/O or an outcome exists.
        """
        selected = self.client_selection
        origin = _ORIGINS.get(selected.environment)
        if (
            selected.exchange != "kraken_futures"
            or selected.mode != "live"
            or origin is None
            or selected.public_api_origin != origin
            or self.key_proof.client_selection != selected
        ):
            raise ValueError("Product read requires the same supported authenticated selection")
        revision = selected.credential_timestamp
        if revision is None or not (
            revision <= self.key_proof.observed_at <= self.key_proof.recorded_at <= self.started_at
        ):
            raise ValueError("Product read key proof was not known before the actual attempt")
        return self


class FuturesProductReadSource(_ProductReadEvidence):
    """Original privacy-checked product entity retained atomically inside its terminal outcome.

    This source never enters the financial account-cash or execution receipt
    domain. The actual binder must reparse it under the retained parser policy
    and refuse malformed, incomplete or secret-bearing material. Source/entity
    clocks remain distinct from the later outcome recording clock.
    """

    kind: Literal["captured"] = "captured"
    parser_policy: Literal["kraken_futures_trading_instruments_v1"] = (
        "kraken_futures_trading_instruments_v1"
    )
    representation: Literal["wire_bytes_base64"] = "wire_bytes_base64"
    status_code: int = Field(default=200, ge=200, le=200)
    body: str = Field(min_length=1, max_length=4 * ((_MAX_ENTITY_BYTES + 2) // 3), repr=False)
    request_started_at: EvidenceInstant
    observed_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_original_entity(self) -> Self:
        """Require bounded canonical base64 and honest original HTTP knowledge clocks.

        Returns:
            The same original entity, without claiming its content was safely classified.
        """
        try:
            decoded = base64.b64decode(self.body, validate=True)
        except ValueError:
            raise ValueError("Product source requires original canonical entity bytes") from None
        if (
            not decoded
            or len(decoded) > _MAX_ENTITY_BYTES
            or base64.b64encode(decoded).decode("ascii") != self.body
        ):
            raise ValueError("Product source entity is empty, oversized or noncanonical")
        if self.request_started_at > self.observed_at:
            raise ValueError("Product source observation precedes its actual request")
        return self


class FuturesProductReadFailure(_ProductReadEvidence):
    """Fixed refusal metadata without retaining unknown or authentication-bearing error bodies."""

    kind: Literal["failed"] = "failed"
    reason: Literal["transport_error", "cancelled", "http_error", "source_invalid", "source_unsafe"]


type FuturesProductReadResult = Annotated[
    FuturesProductReadSource | FuturesProductReadFailure, Field(discriminator="kind")
]


class FuturesProductReadOutcome(_VersionedProductRead):
    """One immutable result for an independently committed original product request."""

    attempt_public_id: EvidenceUUID
    attempt_content_digest: EvidenceDigest
    attempt: FuturesProductReadAttempt
    completed_at: EvidenceInstant
    result: FuturesProductReadResult

    @model_validator(mode="after")
    def validate_attempt_and_knowledge(self) -> Self:
        """Bind one exact request and preserve delayed-recording chronology without renewing age.

        Returns:
            The same historical terminal result, still subject to latest-attempt and freshness checks.
        """
        if self.attempt_content_digest != futures_product_attempt_digest(self.attempt):
            raise ValueError("Product outcome disagrees with its exact attempt digest")
        if self.completed_at < self.attempt.started_at:
            raise ValueError("Product outcome predates its original committed attempt")
        if isinstance(self.result, FuturesProductReadSource) and not (
            self.attempt.started_at
            <= self.result.request_started_at
            <= self.result.observed_at
            <= self.completed_at
        ):
            raise ValueError("Product source lies outside its original request knowledge interval")
        return self


def futures_product_read_family(selection: ExecutionClientSelection) -> str:
    """Use the existing coarse account family only inside the separate fixed product-purpose domain.

    Args:
        selection: Exact current public wallet, venue, mode and environment selection.

    Returns:
        Coarse family digest independent of credentials and margin labels; not an account-read ordinal.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    scope = AccountingObservationScope(
        wallet_public_id=selected.wallet_public_id,
        exchange=selected.exchange,
        mode=selected.mode,
        environment=selected.environment,
    )
    return accounting_hold_scope_digest(scope)


def canonical_futures_product_attempt(payload: FuturesProductReadAttempt) -> str:
    """Serialize one strictly validated product attempt without changing account-read v1 bytes.

    Args:
        payload: Original fixed-purpose product attempt and independently bound public key reference.

    Returns:
        Canonical JSON for exact immutable persistence and replay.
    """
    proven = FuturesProductReadAttempt.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def futures_product_attempt_digest(payload: FuturesProductReadAttempt) -> str:
    """Bind product ordering to its separate canonical domain.

    Args:
        payload: The exact original product request.

    Returns:
        Versioned SHA-256 distinct from accounts-read and financial evidence domains.
    """
    return hashlib.sha256(
        _ATTEMPT_DOMAIN + canonical_futures_product_attempt(payload).encode()
    ).hexdigest()


def canonical_futures_product_outcome(payload: FuturesProductReadOutcome) -> str:
    """Serialize an actual terminal product result without changing any earlier outcome.

    Args:
        payload: The complete original outcome, including safe source bytes or fixed failure metadata.

    Returns:
        Canonical JSON under the product-specific immutable result contract.
    """
    proven = FuturesProductReadOutcome.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def futures_product_outcome_digest(payload: FuturesProductReadOutcome) -> str:
    """Keep product results outside request, account state and financial receipt hash domains.

    Args:
        payload: The exact original terminal product result.

    Returns:
        Versioned SHA-256 over its complete canonical public evidence and original safe entity.
    """
    return hashlib.sha256(
        _OUTCOME_DOMAIN + canonical_futures_product_outcome(payload).encode()
    ).hexdigest()
