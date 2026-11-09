"""Bind accrual corrections to exact stored versions without changing native cash.

Stored accrual amounts are binary floats. Their audit representation preserves
the exact stored bits through hexadecimal strings; it does not claim recovery
of a provider's original decimal lexeme. A correction is not an execution,
native fee, refund, or authorization to apply a production data repair.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceExchange
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance

_VERSION_HASH_DOMAIN = b"snapper:accrual-version:v1\x00"
_CORRECTION_HASH_DOMAIN = b"snapper:accrual-correction:v1\x00"
_SCOPE_HASH_DOMAIN = b"snapper:accrual-input-scope:v1\x00"


class _AccrualEvidence(BaseModel):
    """Keep corrections immutable, strictly typed and closed to unknown inputs."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class AccrualCorrectionScope(_AccrualEvidence):
    """One instrument's input revision, never an account-wide create quarantine."""

    wallet_public_id: EvidenceUUID
    instrument_public_id: EvidenceUUID
    exchange: EvidenceExchange
    mode: Literal["live", "paper", "backtest"]


class AccrualVersionEvidence(_AccrualEvidence):
    """Exact physical version, business boundary, stored floats and provenance."""

    physical_id: int = Field(gt=0)
    public_id: EvidenceUUID
    scope: AccrualCorrectionScope
    timestamp: EvidenceInstant
    known_to: EvidenceInstant
    session_id: EvidenceUUID
    sequence_id: int = Field(gt=0)
    operator_public_id: EvidenceUUID | None
    accrual_type: Literal["funding", "rollover", "borrow"]
    accrued_at: EvidenceInstant
    amount: float = Field(allow_inf_nan=False)
    amount_asset: EvidenceText
    rate: float = Field(allow_inf_nan=False)
    notional: float = Field(allow_inf_nan=False)
    position_quantity_at_accrual: float = Field(allow_inf_nan=False)
    record_status: Literal["posted", "voided"] = "posted"
    correction_public_id: EvidenceUUID | None = None

    @field_serializer(
        "amount", "rate", "notional", "position_quantity_at_accrual", when_used="json"
    )
    def serialize_stored_float(self, value: float) -> str:
        """Preserve binary float identity, including the sign of zero.

        Args:
            value: Finite stored floating-point operand.

        Returns:
            Exact hexadecimal representation of that operand.
        """
        return value.hex()

    @model_validator(mode="after")
    def validate_disposition(self) -> Self:
        """Refuse impossible temporal intervals and internally inconsistent tombstones."""
        if self.timestamp >= self.known_to:
            raise ValueError("Accrual version requires a nonempty knowledge interval")
        if self.record_status == "posted":
            if self.correction_public_id is not None:
                raise ValueError("Posted accrual cannot claim a void correction")
        elif self.correction_public_id is None or self.amount.hex() != (0.0).hex():
            raise ValueError("Voided accrual requires an audited positive-zero contribution")
        return self


class AccrualCorrectionDraft(_AccrualEvidence):
    """Bounded exact-version correction intent, independently requiring operator consent."""

    scope: AccrualCorrectionScope
    expected_revision: int = Field(ge=0)
    actor_public_id: EvidenceUUID
    request_key: EvidenceText = Field(max_length=128)
    reason: EvidenceText = Field(max_length=512)
    evidence_digest: EvidenceDigest
    provenance: SnapshotWriteProvenance
    targets: tuple[AccrualVersionEvidence, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def validate_targets(self) -> Self:
        """Require one scope, unique posted versions and a strictly later correction clock."""
        identities = {row.physical_id for row in self.targets}
        boundaries = {(row.accrual_type, row.accrued_at) for row in self.targets}
        if len(identities) != len(self.targets) or len(boundaries) != len(self.targets):
            raise ValueError(
                "Accrual correction cannot choose duplicate version or boundary winners"
            )
        if len({row.public_id for row in self.targets}) != len(self.targets):
            raise ValueError(
                "Accrual correction cannot target multiple versions of one logical row"
            )
        if any(row.scope != self.scope or row.record_status != "posted" for row in self.targets):
            raise ValueError("Accrual correction requires posted versions from its exact scope")
        if any(self.provenance.timestamp <= row.timestamp for row in self.targets):
            raise ValueError("Accrual correction knowledge must follow every original version")
        return self


def accrual_version_digest(version: AccrualVersionEvidence) -> str:
    """Hash the complete stored version without a decimal reinterpretation.

    Args:
        version: Validated immutable exact-version evidence.

    Returns:
        Domain-separated SHA256 digest of canonical binary-preserving evidence.
    """
    version = AccrualVersionEvidence.model_validate(version)
    material = json.dumps(version.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(_VERSION_HASH_DOMAIN + material.encode()).hexdigest()


def accrual_scope_digest(scope: AccrualCorrectionScope) -> str:
    """Hash one exact correction/revision scope without collapsing sibling instruments.

    Args:
        scope: Validated wallet, instrument, exchange and mode identity.

    Returns:
        Stable SHA256 scope digest.
    """
    scope = AccrualCorrectionScope.model_validate(scope)
    material = json.dumps(scope.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(_SCOPE_HASH_DOMAIN + material.encode()).hexdigest()


def canonical_accrual_correction(draft: AccrualCorrectionDraft) -> str:
    """Freeze one complete intent in deterministic source-version order.

    Args:
        draft: Validated bounded intent with no application authority of its own.

    Returns:
        Canonical JSON preserving every source float's exact stored bits.
    """
    draft = AccrualCorrectionDraft.model_validate(draft)
    ordered = draft.model_copy(
        update={"targets": tuple(sorted(draft.targets, key=lambda row: row.public_id))}
    )
    return json.dumps(ordered.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def accrual_correction_digest(draft: AccrualCorrectionDraft) -> str:
    """Bind actor, evidence, expected revision and every exact original operand.

    Args:
        draft: Validated immutable correction intent.

    Returns:
        Domain-separated SHA256 digest used by idempotency and audit validation.
    """
    return hashlib.sha256(
        _CORRECTION_HASH_DOMAIN + canonical_accrual_correction(draft).encode()
    ).hexdigest()
