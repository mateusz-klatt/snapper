"""Immutable accounting notifications that never grant trading or spending authority."""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID


class ContractPositionPublication(BaseModel):
    """Notify readers of one durable position fact while account reconciliation remains separate.

    This message contains references, not an executable order or a spot cash delta.
    Consumers deduplicate its immutable publication identity before updating any
    projection. Receipt of this notification never resolves an accounting hold.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    schema_version: Literal[1] = 1
    event_type: Literal["contract_position_recorded"] = "contract_position_recorded"
    scope: ExecutionAccountScope
    source_execution_id: EvidenceText
    execution_public_id: EvidenceUUID
    venue_event_public_id: EvidenceUUID
    unit_snapshot_public_id: EvidenceUUID
    position_event_public_id: EvidenceUUID
    instrument_public_id: EvidenceUUID
    raw_receipt_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    ledger_sequence: int = Field(gt=0)
    executed_at: EvidenceInstant
    execution_known_at: EvidenceInstant
    recorded_at: EvidenceInstant

    @field_validator("schema_version", "ledger_sequence", mode="before")
    @classmethod
    def _strict_integer(cls, value: object) -> object:
        """Prevent boolean or floating aliases from changing immutable message identity."""
        if type(value) is not int:
            raise ValueError("Financial publication integer fields require integers")
        return value

    @model_validator(mode="after")
    def validate_recorded_clock(self) -> Self:
        """Keep local record knowledge monotonic without ordering untrusted venue clocks.

        Returns:
            The same immutable notification with its two distinct clocks retained.
        """
        if self.recorded_at < self.execution_known_at:
            raise ValueError("Financial publication predates its execution knowledge")
        return self


def canonical_financial_publication(payload: ContractPositionPublication) -> str:
    """Serialize a revalidated accounting notification with deterministic field ordering.

    Args:
        payload: Actual ledger-bound reference payload, never a caller's money projection.

    Returns:
        Stable canonical text for immutable storage and replay comparison.
    """
    validated = ContractPositionPublication.model_validate(payload)
    return json.dumps(
        validated.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def financial_publication_digest(payload: ContractPositionPublication) -> str:
    """Bind the full typed notification under a dedicated versioned hash domain.

    Args:
        payload: Revalidated notification whose actual references the store separately checks.

    Returns:
        A lowercase SHA-256 digest of the complete canonical notification.
    """
    return hashlib.sha256(
        b"snapper:financial-publication:v1\x00" + canonical_financial_publication(payload).encode()
    ).hexdigest()
