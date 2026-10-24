"""Immutable native account read attempts and separately retained outcomes.

An attempt must commit before network I/O. Its family ordinal, rather than
response arrival time, determines which read can supply current authority.
These payloads contain public provenance only and do not certify collateral,
release financial holds or authorize an order.
"""

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

_ATTEMPT_DOMAIN = b"snapper:native-account-read-attempt:v1\x00"
_OUTCOME_DOMAIN = b"snapper:native-account-read-outcome:v1\x00"
_ORIGINS = {
    "production": "https://futures.kraken.com",
    "demo": "https://demo-futures.kraken.com",
}


class _ReadEvidence(BaseModel):
    """Reject coercion and unchecked mutations at the immutable read boundary."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class _VersionedReadEvidence(_ReadEvidence):
    """Keep strict version identity independent of boolean or floating aliases."""

    schema_version: Literal[1] = 1

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Refuse schema discriminator coercion before validating its supported literal."""
        if type(value) is not int:
            raise ValueError("Native account read version must be an integer")
        return value


class NativeAccountReadAttemptPayload(_VersionedReadEvidence):
    """One committed read request through the actual selected public client revision."""

    purpose: Literal["kraken_futures.accounts"] = "kraken_futures.accounts"
    client_selection: ExecutionClientSelection
    family_sequence: int = Field(ge=1)
    started_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_selected_source(self) -> Self:
        """Bind supported routing and reject a read that predates its selected credential.

        Returns:
            The same immutable attempt, without claiming that any response exists.
        """
        selected = self.client_selection
        origin = _ORIGINS.get(selected.environment)
        if (
            origin is None
            or selected.exchange != "kraken_futures"
            or selected.public_api_origin != origin
        ):
            raise ValueError("Native account read client origin is unproven")
        if selected.credential_timestamp is None or selected.credential_timestamp > self.started_at:
            raise ValueError("Native account read precedes the selected credential revision")
        return self


class NativeAccountReadCapturedSource(_ReadEvidence):
    """An actual retained original HTTP response, still requiring source interpretation."""

    kind: Literal["captured"] = "captured"
    raw_receipt_public_id: EvidenceUUID
    raw_content_digest: EvidenceDigest
    request_started_at: EvidenceInstant
    source_observed_at: EvidenceInstant
    source_recorded_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_source_clocks(self) -> Self:
        """Keep actual network start, original capture and durable source knowledge ordered.

        Returns:
            The same immutable source reference and its separate clocks.
        """
        if not self.request_started_at <= self.source_observed_at <= self.source_recorded_at:
            raise ValueError("Native account read request, capture and recording clocks disagree")
        return self


class NativeAccountReadFailure(_ReadEvidence):
    """A completed refusal without exception text, secret material or a fabricated response."""

    kind: Literal["failed"] = "failed"
    reason: Literal[
        "transport_error", "cancelled", "capture_failed", "source_invalid", "http_error"
    ]
    source: NativeAccountReadCapturedSource | None = None


type NativeAccountReadResult = Annotated[
    NativeAccountReadCapturedSource | NativeAccountReadFailure, Field(discriminator="kind")
]


class NativeAccountReadOutcomePayload(_VersionedReadEvidence):
    """One immutable outcome bound to an actual attempt, independent of whether it is still latest."""

    attempt_public_id: EvidenceUUID
    attempt_content_digest: EvidenceDigest
    attempt: NativeAccountReadAttemptPayload
    completed_at: EvidenceInstant
    result: NativeAccountReadResult

    @property
    def source_reference(self) -> NativeAccountReadCapturedSource | None:
        """Retain a captured failure body without promoting it into a successful account read.

        Returns:
            The original source when one was actually retained, otherwise explicit absence.
        """
        if isinstance(self.result, NativeAccountReadCapturedSource):
            return self.result
        return self.result.source

    @model_validator(mode="after")
    def validate_attempt_and_clocks(self) -> Self:
        """Keep an outcome bound to its original request and honest source-knowledge chronology.

        Returns:
            The same immutable historical outcome, without granting current-read authority.
        """
        if self.attempt_content_digest != native_account_read_attempt_digest(self.attempt):
            raise ValueError("Native account read outcome contradicts its attempt digest")
        if self.completed_at < self.attempt.started_at:
            raise ValueError("Native account read completion precedes its attempt")
        source = self.source_reference
        if source is not None and not (
            self.attempt.started_at
            <= source.request_started_at
            <= source.source_observed_at
            <= source.source_recorded_at
            <= self.completed_at
        ):
            raise ValueError("Native account read source lies outside its knowledge interval")
        return self


def native_account_read_family_digest(selection: ExecutionClientSelection) -> str:
    """Share read ordering across credential rotation and all margin profiles in one actual account.

    Args:
        selection: Actual public wallet, venue, mode, environment and selected client metadata.

    Returns:
        The existing coarse account digest, excluding credential and margin namespaces.
    """
    selected = ExecutionClientSelection.model_validate(selection)
    return accounting_hold_scope_digest(
        AccountingObservationScope(
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            mode=selected.mode,
            environment=selected.environment,
        )
    )


def canonical_native_account_read_attempt(payload: NativeAccountReadAttemptPayload) -> str:
    """Serialize one strict attempt without reading current configuration or constructing a response.

    Args:
        payload: Frozen public request identity and assigned family ordinal.

    Returns:
        Stable canonical JSON for storage and exact replay verification.
    """
    proven = NativeAccountReadAttemptPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_account_read_attempt_digest(payload: NativeAccountReadAttemptPayload) -> str:
    """Hash the original ordered request under its dedicated immutable domain.

    Args:
        payload: Exact public request evidence, with no credentials or balances.

    Returns:
        Versioned SHA-256 over the canonical attempt.
    """
    return hashlib.sha256(
        _ATTEMPT_DOMAIN + canonical_native_account_read_attempt(payload).encode()
    ).hexdigest()


def canonical_native_account_read_outcome(payload: NativeAccountReadOutcomePayload) -> str:
    """Serialize a strict historical outcome without confusing arrival order with request order.

    Args:
        payload: Exact outcome and its independently verified original attempt.

    Returns:
        Stable canonical JSON retaining either source reference or explicit refusal.
    """
    proven = NativeAccountReadOutcomePayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def native_account_read_outcome_digest(payload: NativeAccountReadOutcomePayload) -> str:
    """Hash the exact outcome under a domain separate from requests and raw source material.

    Args:
        payload: Original attempt-bound result, including its local completion clock.

    Returns:
        Versioned SHA-256 of the retained outcome.
    """
    return hashlib.sha256(
        _OUTCOME_DOMAIN + canonical_native_account_read_outcome(payload).encode()
    ).hexdigest()
