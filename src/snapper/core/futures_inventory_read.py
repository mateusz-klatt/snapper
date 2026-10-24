"""Separate immutable endpoint-specific inventory reads without financial raw-receipt reuse.

These public shapes require the actual closed transport and durable binders.
They establish neither authenticated provenance nor order permission by shape.
Each endpoint has independent request ordering across credential rotation and
margin profiles inside the same actual wallet, venue and environment. Separate
positions and orders responses never establish an atomic account snapshot.
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

_FAMILY_DOMAIN = b"snapper:futures-inventory-read-family:v1\x00"
_ATTEMPT_DOMAIN = b"snapper:futures-inventory-read-attempt:v1\x00"
_OUTCOME_DOMAIN = b"snapper:futures-inventory-read-outcome:v1\x00"
_MAX_ENTITY_BYTES = 8 * 1024 * 1024
_ORIGINS = {
    "production": "https://futures.kraken.com",
    "demo": "https://demo-futures.kraken.com",
}


type InventoryReadPurpose = Literal["kraken_futures.open_orders", "kraken_futures.open_positions"]
type InventoryParserPolicy = Literal[
    "kraken_futures_open_orders_v1", "kraken_futures_open_positions_v1"
]
_PURPOSE_PARSER_POLICIES: dict[InventoryReadPurpose, InventoryParserPolicy] = {
    "kraken_futures.open_orders": "kraken_futures_open_orders_v1",
    "kraken_futures.open_positions": "kraken_futures_open_positions_v1",
}


class _InventoryReadEvidence(BaseModel):
    """Reject coercion, extra authentication material and unchecked nested model copies."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class InventoryReadKeyReference(_InventoryReadEvidence):
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
            raise ValueError("Inventory key proof recording precedes its original observation")
        return self


class _VersionedInventoryRead(_InventoryReadEvidence):
    """Keep the version distinct from Boolean and floating aliases."""

    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Require an actual integer before checking the supported literal discriminator."""
        if type(value) is not int:
            raise ValueError("Inventory read version must be an integer")
        return value


class FuturesInventoryReadAttempt(_VersionedInventoryRead):
    """A committed fixed-purpose request with its own coarse-family ordering coordinate."""

    purpose: InventoryReadPurpose
    client_selection: ExecutionClientSelection
    key_proof: InventoryReadKeyReference
    family_sequence: int = Field(ge=1)
    started_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_actual_selection(self) -> Self:
        """Preserve the exact supported client and earlier committed source-proof chronology.

        Returns:
            The same inventory request without asserting that network I/O or an outcome exists.
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
            raise ValueError("Inventory read requires the same supported authenticated selection")
        revision = selected.credential_timestamp
        if revision is None or not (
            revision <= self.key_proof.observed_at <= self.key_proof.recorded_at <= self.started_at
        ):
            raise ValueError("Inventory read key proof was not known before the actual attempt")
        return self


class FuturesInventoryReadSource(_InventoryReadEvidence):
    """Original privacy-checked inventory entity retained atomically inside its terminal outcome.

    This source never enters the financial account-cash or execution receipt
    domain. The actual binder must reparse it under the retained parser policy
    and refuse malformed, incomplete or secret-bearing material. Source/entity
    clocks remain distinct from the later outcome recording clock.
    """

    kind: Literal["captured"] = "captured"
    parser_policy: InventoryParserPolicy
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
            raise ValueError("Inventory source requires original canonical entity bytes") from None
        if (
            not decoded
            or len(decoded) > _MAX_ENTITY_BYTES
            or base64.b64encode(decoded).decode("ascii") != self.body
        ):
            raise ValueError("Inventory source entity is empty, oversized or noncanonical")
        if self.request_started_at > self.observed_at:
            raise ValueError("Inventory source observation precedes its actual request")
        return self


class FuturesInventoryReadFailure(_InventoryReadEvidence):
    """Fixed refusal metadata without retaining unknown or authentication-bearing error bodies."""

    kind: Literal["failed"] = "failed"
    reason: Literal["transport_error", "cancelled", "http_error", "source_invalid", "source_unsafe"]


type FuturesInventoryReadResult = Annotated[
    FuturesInventoryReadSource | FuturesInventoryReadFailure, Field(discriminator="kind")
]


class FuturesInventoryReadOutcome(_VersionedInventoryRead):
    """One immutable result for an independently committed original inventory request."""

    attempt_public_id: EvidenceUUID
    attempt_content_digest: EvidenceDigest
    attempt: FuturesInventoryReadAttempt
    completed_at: EvidenceInstant
    result: FuturesInventoryReadResult

    @model_validator(mode="after")
    def validate_attempt_and_knowledge(self) -> Self:
        """Bind one exact request and preserve delayed-recording chronology without renewing age.

        Returns:
            The same historical terminal result, still subject to latest-attempt and freshness checks.
        """
        if self.attempt_content_digest != futures_inventory_attempt_digest(self.attempt):
            raise ValueError("Inventory outcome disagrees with its exact attempt digest")
        if self.completed_at < self.attempt.started_at:
            raise ValueError("Inventory outcome predates its original committed attempt")
        if isinstance(self.result, FuturesInventoryReadSource):
            if self.result.parser_policy != _PURPOSE_PARSER_POLICIES[self.attempt.purpose]:
                raise ValueError(
                    "Inventory source parser does not match its original endpoint purpose"
                )
            if not (
                self.attempt.started_at
                <= self.result.request_started_at
                <= self.result.observed_at
                <= self.completed_at
            ):
                raise ValueError(
                    "Inventory source lies outside its original request knowledge interval"
                )
        return self


def futures_inventory_read_family(
    selection: ExecutionClientSelection, purpose: InventoryReadPurpose
) -> str:
    """Bind one explicit endpoint to the existing coarse account family in an independent domain.

    Args:
        selection: Exact current public wallet, venue, mode and environment selection.
        purpose: Actual fixed endpoint whose latest pending or failed request forms its own barrier.

    Returns:
        Endpoint family digest independent of credentials and margin labels, never a shared ordinal.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    scope = AccountingObservationScope(
        wallet_public_id=selected.wallet_public_id,
        exchange=selected.exchange,
        mode=selected.mode,
        environment=selected.environment,
    )
    if purpose not in ("kraken_futures.open_orders", "kraken_futures.open_positions"):
        raise ValueError("Inventory read purpose is unsupported")
    material = json.dumps(
        {"scope_digest": accounting_hold_scope_digest(scope), "purpose": purpose},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(_FAMILY_DOMAIN + material.encode()).hexdigest()


def canonical_futures_inventory_attempt(payload: FuturesInventoryReadAttempt) -> str:
    """Serialize one strictly validated inventory attempt without changing account-read v1 bytes.

    Args:
        payload: Original fixed-purpose inventory attempt and independently bound public key reference.

    Returns:
        Canonical JSON for exact immutable persistence and replay.
    """
    proven = FuturesInventoryReadAttempt.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def futures_inventory_attempt_digest(payload: FuturesInventoryReadAttempt) -> str:
    """Bind inventory ordering to its separate canonical domain.

    Args:
        payload: The exact original inventory request.

    Returns:
        Versioned SHA-256 distinct from accounts-read and financial evidence domains.
    """
    return hashlib.sha256(
        _ATTEMPT_DOMAIN + canonical_futures_inventory_attempt(payload).encode()
    ).hexdigest()


def canonical_futures_inventory_outcome(payload: FuturesInventoryReadOutcome) -> str:
    """Serialize an actual terminal inventory result without changing any earlier outcome.

    Args:
        payload: The complete original outcome, including safe source bytes or fixed failure metadata.

    Returns:
        Canonical JSON under the inventory-specific immutable result contract.
    """
    proven = FuturesInventoryReadOutcome.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def futures_inventory_outcome_digest(payload: FuturesInventoryReadOutcome) -> str:
    """Keep inventory results outside request, account state and financial receipt hash domains.

    Args:
        payload: The exact original terminal inventory result.

    Returns:
        Versioned SHA-256 over its complete canonical public evidence and original safe entity.
    """
    return hashlib.sha256(
        _OUTCOME_DOMAIN + canonical_futures_inventory_outcome(payload).encode()
    ).hexdigest()
