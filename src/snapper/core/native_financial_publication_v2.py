"""Reference-only native position notifications, independent of legacy spot fill arithmetic.

These new bytes never rewrite the v1 publication codec or its hash domain.
A well-formed message is not financial admission, authenticated ownership or
proof of delivery. The actual store independently rebinds every retained source.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import StrictInt
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicyReference
from snapper.core.private_source_ownership import PrivateSourceOwnershipReference


class NativeContractPositionPublicationV2(BaseModel):
    """Identify one actually recorded position without treating source components as cash."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[2] = 2
    event_type: Literal["native_contract_position_recorded"] = "native_contract_position_recorded"
    scope: ExecutionAccountScope
    source_execution_id: EvidenceText
    execution_public_id: EvidenceUUID
    venue_event_public_id: EvidenceUUID
    unit_snapshot_public_id: EvidenceUUID
    position_event_public_id: EvidenceUUID
    position_event_digest: EvidenceDigest
    instrument_public_id: EvidenceUUID
    fill_authority_reference: AccountAuthorityReference
    fill_ownership_reference: PrivateSourceOwnershipReference
    position_authority_reference: AccountAuthorityReference
    position_ownership_reference: PrivateSourceOwnershipReference
    owner_policy_reference: FinancialAccountOwnerPolicyReference
    ledger_sequence: StrictInt = Field(gt=0)
    executed_at: EvidenceInstant
    execution_known_at: EvidenceInstant
    position_known_at: EvidenceInstant
    owner_decided_at: EvidenceInstant
    recorded_at: EvidenceInstant

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_exact_version(cls, value: object) -> object:
        """Refuse coercible version aliases before choosing the new canonical domain.

        Args:
            value: Original uncoerced publication discriminator.

        Returns:
            The same exact integer, awaiting the supported literal check.
        """
        if type(value) is not int:
            raise ValueError("Native publication version requires an integer")
        return value

    @model_validator(mode="after")
    def validate_source_knowledge(self) -> Self:
        """Keep original fill and later log knowledge separate from venue economic time.

        Returns:
            The same strict reference message, without proof that any referenced row exists.
        """
        if (
            max(
                self.fill_authority_reference.recorded_at,
                self.fill_ownership_reference.recorded_at,
            )
            > self.execution_known_at
        ):
            raise ValueError("Native publication original fill knowledge is unavailable")
        if self.position_authority_reference.family != "native_accounts_v1":
            raise ValueError("Native publication requires a tagged native position authority")
        if (
            max(
                self.execution_known_at,
                self.position_authority_reference.recorded_at,
                self.position_ownership_reference.recorded_at,
                self.owner_policy_reference.timestamp,
            )
            > self.position_known_at
        ):
            raise ValueError("Native publication precedes its actual source knowledge")
        if (
            self.owner_decided_at != self.position_known_at
            or self.recorded_at < self.position_known_at
        ):
            raise ValueError("Native publication changes its original posting decision clock")
        return self


def canonical_native_position_publication_v2(
    payload: NativeContractPositionPublicationV2,
) -> str:
    """Serialize all actual reference copies with deterministic strict JSON.

    Args:
        payload: Strict native position reference message awaiting actual store binding.

    Returns:
        Stable text independent of input field order, without changing legacy bytes.
    """
    value = NativeContractPositionPublicationV2.model_validate(payload)
    return json.dumps(
        value.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def native_position_publication_digest_v2(
    payload: NativeContractPositionPublicationV2,
) -> str:
    """Hash the complete reference message under its own versioned notification domain.

    Args:
        payload: Native reference message whose actual financial graph is independently rebound.

    Returns:
        Lowercase SHA-256 identity distinct from financial facts and legacy publications.
    """
    return hashlib.sha256(
        b"snapper:native-position-publication:v2\x00"
        + canonical_native_position_publication_v2(payload).encode()
    ).hexdigest()
