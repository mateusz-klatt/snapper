"""Unapplied complete native claim shape without altering v1 economic source identity or uniqueness.

This new witness requires the actual bound authority, exact historical policy,
private sidecar and financial event. Its strict public model grants no right to
write. Missing actual source readers remain an explicit promotion refusal.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_account_log import native_futures_cash_leg
from snapper.application.portfolio.native_financial_payloads_v2 import NativeFinancialSourceEvidence
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceUUID

_NATIVE_CLAIM_V2_DOMAIN = b"snapper:accounting-native-source-claim:v2\x00"


class AccountingNativeSourceClaimPayloadV2(BaseModel):
    """One actual consumed log entry with required native authority, policy and private ownership.

    Native identity and its existing semantic digest retain their v1 helper
    domains. Schema, kind, margin, instrument, credential and authority family
    never create a second economic namespace or another claim for this entry.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[2] = 2
    source_domain: Literal["kraken_futures.account_log"] = "kraken_futures.account_log"
    scope: ExecutionAccountScope
    accounting_event_public_id: EvidenceUUID
    accounting_event_content_digest: EvidenceDigest
    raw_receipt_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    kind: Literal["native_cash", "contract_position_fill"]
    source: NativeFinancialSourceEvidence
    source_entry: FuturesAccountLogEntry

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, value: object) -> object:
        """Reject bool, string and floating aliases before assigning the new witness domain.

        Args:
            value: Original immutable claim discriminator.

        Returns:
            The same strict integer before supported-version validation.
        """
        if type(value) is not int:
            raise ValueError("Native source claim schema version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_actual_source(self) -> Self:
        """Require all account/log copies and preserve the source's financial classification.

        Returns:
            The same complete shape, requiring actual immutable row and source graph binding.
        """
        reference = self.source.ownership_reference
        if (
            self.scope != self.source.authority.scope
            or self.route_authority_public_id != self.source.authority_reference.public_id
            or self.raw_receipt_public_id != reference.raw_receipt_public_id
            or self.source_entry.target_json_pointer != reference.target_json_pointer
            or self.source_entry.margin_account != self.scope.margin_account
        ):
            raise ValueError("Native claim contradicts its source references or account scope")
        if self.kind == "native_cash":
            native_futures_cash_leg(self.source_entry)
        elif self.source_entry.asset == self.source_entry.collateral:
            raise ValueError("Native currency balance is not a position claim")
        return self


def canonical_native_source_claim_v2(payload: AccountingNativeSourceClaimPayloadV2) -> str:
    """Serialize required v2 source/policy evidence independently of legacy canonical records.

    Args:
        payload: Complete strict native-family consumption witness before actual row binding.

    Returns:
        Canonical JSON preserving all required source references and financial operands.
    """
    evidence = AccountingNativeSourceClaimPayloadV2.model_validate(payload)
    return json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_source_claim_digest_v2(payload: AccountingNativeSourceClaimPayloadV2) -> str:
    """Hash only the new full witness, retaining the existing shared economic identity namespace.

    Args:
        payload: Strict v2 claim whose actual event/source/policy graph is independently verified.

    Returns:
        The complete v2 content digest, without partitioning physical native-ID uniqueness.
    """
    return hashlib.sha256(
        _NATIVE_CLAIM_V2_DOMAIN + canonical_native_source_claim_v2(payload).encode()
    ).hexdigest()
