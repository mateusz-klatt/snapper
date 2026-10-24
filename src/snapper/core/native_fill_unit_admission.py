"""Explicit retained admission and unit references for a later native financial envelope.

The unchanged v1 unit snapshot cannot identify an account-admission family.
This separate strict shape must be retained by a new financial wrapper and
rebound against actual rows. It is not a dispatch or spending capability.
"""

from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID


class NativeUnitRecordReference(BaseModel):
    """Exact immutable record identity and original retained knowledge, without source issuance."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    public_id: EvidenceUUID
    content_digest: EvidenceDigest
    recorded_at: EvidenceInstant


class NativeFillUnitAdmissionEvidence(BaseModel):
    """A versioned original v3 account-admission edge alongside an unchanged v1 fill-unit edge."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[1] = 1
    submission_admission: NativeUnitRecordReference
    unit_snapshot: NativeUnitRecordReference

    @field_validator("schema_version", mode="before")
    @classmethod
    def require_integer_version(cls, value: object) -> object:
        """Refuse coercible version aliases before selecting the retained envelope contract.

        Args:
            value: Original uncoerced version value.

        Returns:
            The same integer awaiting the supported literal check.
        """
        if type(value) is not int:
            raise ValueError("Native fill-unit admission version must be an integer")
        return value

    @model_validator(mode="after")
    def require_original_knowledge_order(self) -> Self:
        """Keep local recording chronology distinct from the venue's execution clock.

        Returns:
            The unchanged reference shape pending actual record, source and command binding.
        """
        if self.submission_admission.recorded_at > self.unit_snapshot.recorded_at:
            raise ValueError("Native fill units precede original admission knowledge")
        if self.submission_admission.public_id == self.unit_snapshot.public_id:
            raise ValueError("Native admission and fill units require distinct record identities")
        return self
