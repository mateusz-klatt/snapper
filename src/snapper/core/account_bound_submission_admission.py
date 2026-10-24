"""Additive admission envelopes naming an exact immutable account source family.

The nested unit graph retains the version-two shape and canonicalizer. It is
not independently persisted as a legacy admission when the outer envelope is
version three. Actual source binding, current eligibility, financial ownership,
order permission and dispatch remain separate store and execution checks.
"""

import hashlib
import json
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload

_ACCOUNT_BOUND_ADMISSION_DOMAIN = b"snapper:execution-unit-admission:v3\x00"


class AccountBoundSubmissionAdmissionPayload(BaseModel):
    """One attempt's unchanged unit graph with an explicit historical authority family."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )

    schema_version: Literal[3] = 3
    units: SubmissionUnitAdmissionPayload
    authority: AccountAuthorityReference

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_integer_version(cls, value: object) -> object:
        """Refuse numeric aliases for the separate immutable envelope version.

        Args:
            value: Original discriminator supplied at the envelope boundary.

        Returns:
            The same integer before supported-version validation.
        """
        if type(value) is not int:
            raise ValueError("Account-bound admission version must be an integer")
        return value

    @model_validator(mode="after")
    def validate_authority_reference(self) -> Self:
        """Bind duplicate references and preserve the original durable knowledge horizon.

        Returns:
            The coherent envelope without asserting that any referenced row exists.
        """
        route = self.units.route
        authority = self.authority
        if (route.authority_public_id, route.authority_content_digest) != (
            authority.public_id,
            authority.content_digest,
        ):
            raise ValueError("Admission route and account authority reference disagree")
        if authority.recorded_at > self.units.interpreted_at:
            raise ValueError("Admission authority was not known at interpretation")
        return self


def canonical_account_bound_admission(payload: AccountBoundSubmissionAdmissionPayload) -> str:
    """Serialize the explicit envelope without changing retained version-two evidence.

    Args:
        payload: The strict unit graph and independently rebound authority reference.

    Returns:
        Canonical JSON preserving exact values and original knowledge clocks.
    """
    proven = AccountBoundSubmissionAdmissionPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def account_bound_admission_digest(payload: AccountBoundSubmissionAdmissionPayload) -> str:
    """Hash the complete account-qualified attempt under its separate version-three domain.

    Args:
        payload: Immutable envelope whose source graph requires actual store-side binding.

    Returns:
        SHA-256 over the versioned canonical envelope.
    """
    material = canonical_account_bound_admission(payload)
    return hashlib.sha256(_ACCOUNT_BOUND_ADMISSION_DOMAIN + material.encode()).hexdigest()
