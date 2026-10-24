"""Describe exclusive native log consumption across account margins and accounting kinds.

Kraken documents the log identifier without a separate margin namespace. The
claim therefore conservatively shares one identifier family across the actual
wallet, exchange, mode and environment. Booking groups, credentials, assets and
margin labels cannot split that family. Cross-wallet account allocation remains
an independent requirement.
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

from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import accounting_hold_scope_digest
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_unit_snapshot import EvidenceUUID

_IDENTITY_DOMAIN = b"snapper:kraken-futures-account-log-identity:v1\x00"
_SEMANTIC_DOMAIN = b"snapper:kraken-futures-account-log-semantics:v1\x00"
_CLAIM_DOMAIN = b"snapper:accounting-native-source-claim:v1\x00"


def native_source_family_digest(scope: ExecutionAccountScope) -> str:
    """Share native source ownership across margins, instruments and credential rotations.

    Args:
        scope: The actual financial account scope whose margin is deliberately excluded.

    Returns:
        The existing coarse wallet, exchange, mode and environment family digest.
    """
    validated = ExecutionAccountScope.model_validate(scope)
    family = AccountingObservationScope.model_validate(validated.model_dump())
    return accounting_hold_scope_digest(family)


def native_source_log_id(entry: FuturesAccountLogEntry) -> str:
    """Retain an exact positive source identifier without a database integer-width assumption.

    Args:
        entry: A strictly decoded actual account-log entry.

    Returns:
        Canonical positive ASCII decimal text with no sign, exponent or leading zero.
    """
    return str(FuturesAccountLogEntry.model_validate(entry).source_log_id)


def native_source_identity_digest(entry: FuturesAccountLogEntry) -> str:
    """Hash the exact native identifier under the fixed provider source domain.

    Args:
        entry: Actual selected source entry, independent of its transport pointer.

    Returns:
        A bounded versioned identity retaining the full original ID in the claim.
    """
    return hashlib.sha256(_IDENTITY_DOMAIN + native_source_log_id(entry).encode()).hexdigest()


def native_source_semantic_digest(entry: FuturesAccountLogEntry) -> str:
    """Identify the native entry body independently of transport and optional frame headers.

    Args:
        entry: All parsed native economic fields, including the literal margin label.

    Returns:
        A digest excluding only the transport pointer and envelope account header.
        Header continuity is checked separately against actual bound route evidence.
    """
    evidence = FuturesAccountLogEntry.model_validate(entry)
    material = json.dumps(
        evidence.model_dump(mode="json", exclude={"target_json_pointer", "venue_account_id"}),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(_SEMANTIC_DOMAIN + material.encode()).hexdigest()


class AccountingNativeSourceClaimPayload(BaseModel):
    """One actual native entry exclusively consumed by one immutable financial event."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[1] = 1
    source_domain: Literal["kraken_futures.account_log"] = "kraken_futures.account_log"
    scope: ExecutionAccountScope
    accounting_event_public_id: EvidenceUUID
    accounting_event_content_digest: str = Field(pattern="^[0-9a-f]{64}$")
    raw_receipt_public_id: EvidenceUUID
    route_authority_public_id: EvidenceUUID
    kind: Literal["native_cash", "contract_position_fill"]
    authority: ExecutionRouteAuthorityPayload
    source_entry: FuturesAccountLogEntry

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version(cls, value: object) -> object:
        """Refuse boolean and floating aliases for the immutable schema discriminator.

        Args:
            value: Uncoerced immutable schema version supplied at the model boundary.

        Returns:
            The original integer discriminator for the strict literal validator.
        """
        if type(value) is not int:
            raise ValueError("Native source claim schema version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_actual_account(self) -> Self:
        """Bind the retained actual account and native margin without inventing an account ID.

        Returns:
            The same immutable payload after checking all source account dimensions.
        """
        if (
            self.scope.exchange != "kraken_futures"
            or self.authority.scope != self.scope
            or self.source_entry.margin_account != self.scope.margin_account
            or (
                self.source_entry.venue_account_id is not None
                and self.authority.venue_account_id is not None
                and self.source_entry.venue_account_id != self.authority.venue_account_id
            )
        ):
            raise ValueError("Native source claim contradicts actual account evidence")
        return self


def canonical_native_source_claim(payload: AccountingNativeSourceClaimPayload) -> str:
    """Serialize complete actual source, account and financial event references.

    Args:
        payload: The strictly validated immutable claim.

    Returns:
        Canonical JSON preserving exact source operands and full native identifiers.
    """
    validated = AccountingNativeSourceClaimPayload.model_validate(payload)
    return json.dumps(validated.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_source_claim_digest(payload: AccountingNativeSourceClaimPayload) -> str:
    """Hash one complete exclusive source-consumption witness.

    Args:
        payload: A claim whose account and native body are independently validated.

    Returns:
        The versioned SHA-256 identity of the complete immutable payload.
    """
    return hashlib.sha256(
        _CLAIM_DOMAIN + canonical_native_source_claim(payload).encode()
    ).hexdigest()
