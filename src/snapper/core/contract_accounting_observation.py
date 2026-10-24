"""Retain uninterpreted financial observations before accounting can fail.

The receipt identity represents one local observation, not a deduplicated venue
execution fact. Invalid JSON, non-finite provider text and unknown account or
instrument details remain capturable without interpreting financial amounts.
"""

import base64
import hashlib
import json
from typing import Annotated
from typing import Literal
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceExchange
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance

ACCOUNTING_OBSERVATION_HASH_DOMAIN = b"snapper:accounting-observation:v2\x00"
ACCOUNTING_HOLD_SCOPE_HASH_DOMAIN = b"snapper:accounting-hold-scope:v1\x00"


def _json_pointer(value: str) -> str:
    """Validate RFC6901 selection syntax without interpreting a financially malformed frame."""
    if value and not value.startswith("/"):
        raise ValueError("Accounting target requires an RFC6901 JSON pointer")
    index = 0
    while index < len(value):
        if value[index] == "~":
            if index + 1 >= len(value) or value[index + 1] not in {"0", "1"}:
                raise ValueError("Accounting target contains an invalid JSON pointer escape")
            index += 1
        index += 1
    return value


type AccountingJsonPointer = Annotated[str, AfterValidator(_json_pointer)]


class AccountingObservationScope(BaseModel):
    """Actual runtime route identity with an honestly unknown margin account allowed."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    wallet_public_id: EvidenceUUID
    exchange: EvidenceExchange
    mode: Literal["live", "paper"]
    environment: Literal["production", "demo", "paper"]
    margin_account: EvidenceText | None = None

    @model_validator(mode="after")
    def validate_environment(self) -> Self:
        """Prevent a paper route from being labeled production or demo.

        Returns:
            The same immutable source scope.
        """
        if (self.mode == "paper") != (self.environment == "paper"):
            raise ValueError("Accounting mode and actual environment disagree")
        if (self.exchange == "paper") != (self.mode == "paper"):
            raise ValueError("Accounting mode and actual exchange disagree")
        return self


class RawAccountingObservation(BaseModel):
    """A frozen raw observation that claims no successful financial interpretation."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[2] = 2
    scope: AccountingObservationScope
    route_revision: EvidenceText
    route_authority_public_id: EvidenceUUID | None = None
    credential_public_id: EvidenceUUID | None = None
    credential_timestamp: EvidenceInstant | None = None
    kind: Literal["execution", "account_cash", "funding", "settlement", "unknown"]
    channel: EvidenceText
    representation: Literal[
        "wire_bytes_base64", "decoded_json_text", "parsed_fields_json", "opaque_text"
    ]
    material: str = Field(repr=False)
    target_json_pointer: AccountingJsonPointer | None = None
    observed_at: EvidenceInstant
    source_identity: EvidenceText | None = None
    source_identity_kind: Literal["venue_execution_id", "booking_uid", "unproven"] | None = None
    order_public_id: EvidenceUUID | None = None
    source_instrument_public_id: EvidenceUUID | None = None
    native_symbol: str | None = None

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Reject bool/float schema aliases before the literal discriminator is applied."""
        if type(value) is not int:
            raise ValueError("Accounting observation schema version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_provenance(self) -> Self:
        """Validate tags while retaining financially malformed provider material.

        Returns:
            The same receipt without treating source identities as proven facts.
        """
        if (self.credential_public_id is None) != (self.credential_timestamp is None):
            raise ValueError("Credential identity and exact revision must be supplied together")
        if (self.source_identity is None) != (self.source_identity_kind is None):
            raise ValueError("Source identity requires its explicit provenance tag")
        if self.representation == "wire_bytes_base64":
            try:
                raw = base64.b64decode(self.material, validate=True)
            except ValueError as error:
                raise ValueError("Wire bytes require canonical base64") from error
            if base64.b64encode(raw).decode("ascii") != self.material:
                raise ValueError("Wire bytes require canonical base64")
        return self


class AccountingWriteProvenance(SnapshotWriteProvenance):
    """Stable local observation UUID and insertion provenance, separate from venue facts."""


def canonical_accounting_observation(observation: RawAccountingObservation) -> str:
    """Serialize source evidence without parsing or rounding its provider material.

    Args:
        observation: Frozen uninterpreted financial observation.

    Returns:
        Canonical envelope JSON retaining the exact tagged material string.
    """
    evidence = RawAccountingObservation.model_validate(observation)
    return json.dumps(evidence.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def accounting_observation_digest(observation: RawAccountingObservation) -> str:
    """Hash one observation's complete content independently of its insertion UUID.

    Args:
        observation: The exact frozen material and its interpretation-free provenance.

    Returns:
        Versioned SHA-256 content identity. It is not a venue execution dedup key.
    """
    material = canonical_accounting_observation(observation)
    return hashlib.sha256(ACCOUNTING_OBSERVATION_HASH_DOMAIN + material.encode()).hexdigest()


def accounting_hold_scope_digest(scope: AccountingObservationScope) -> str:
    """Identify the account family independently of instrument and credential rotation.

    Args:
        scope: Actual wallet, venue, source mode and selected environment.

    Returns:
        A coarse key. Hold queries additionally compare a known margin account;
        an unknown margin account can affect every profile in this family.
    """
    evidence = AccountingObservationScope.model_validate(scope)
    material = json.dumps(
        evidence.model_dump(mode="json", exclude={"margin_account"}),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(ACCOUNTING_HOLD_SCOPE_HASH_DOMAIN + material.encode()).hexdigest()
