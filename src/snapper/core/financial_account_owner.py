"""Represent explicit versioned financial ownership without granting financial permission.

Read-only account identity can be shared by several local wallets. This policy
names one financial owner per actual external account and environment. Its pure
evidence must be rebound to the exact durable Settings revision and native
authority inside the financial transaction; constructing a model proves neither
that assignment exists nor that its source transport or current admission is safe.
"""

import hashlib
import json
from typing import Final
from typing import Literal
from typing import NoReturn
from typing import Self
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.native_account_authority import NativeAccountAuthorityPayload

FINANCIAL_OWNER_SETTING_KEY: Final = "financial_account_owner_policy_v1"
_KEY_DOMAIN = b"snapper:financial-account-owner-key:v1\x00"
_POLICY_DOMAIN = b"snapper:financial-account-owner-policy:v1\x00"
_EVIDENCE_DOMAIN = b"snapper:financial-account-owner-evidence:v1\x00"


class FinancialAccountOwnerUnavailableError(ValueError):
    """The supplied policy does not assign this external account to the actual local wallet."""


class FinancialAccountOwnerMismatchError(FinancialAccountOwnerUnavailableError):
    """The explicit policy assigns this external account to another local wallet."""


class _FrozenOwnerEvidence(BaseModel):
    """Refuse implicit coercion, extra fields and unchecked nested evidence mutations."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class FinancialAccountOwnerKey(_FrozenOwnerEvidence):
    """One external financial book, independent of local aliases and instrument or margin labels."""

    exchange: Literal["kraken_futures"] = "kraken_futures"
    mode: Literal["live"] = "live"
    environment: Literal["production", "demo"]
    account_uid: EvidenceUUID
    source_domain: Literal["kraken_futures.account_log"] = "kraken_futures.account_log"


def financial_account_owner_key_digest(key: FinancialAccountOwnerKey) -> str:
    """Hash the external book namespace without allowing local aliases to split ownership.

    Args:
        key: Exact authenticated external account and actual environment.

    Returns:
        Versioned owner-key digest, independent of wallet, margin, instrument and credential.
    """
    proven = FinancialAccountOwnerKey.model_validate(key)
    material = json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(_KEY_DOMAIN + material.encode()).hexdigest()


def financial_account_owner_key_from_authority(
    authority: NativeAccountAuthorityPayload,
) -> FinancialAccountOwnerKey:
    """Retain the actual native owner UID without treating the WS account label as an identity.

    Args:
        authority: Typed native source evidence whose actual graph is independently rebound.

    Returns:
        The authenticated external financial namespace without a current assignment verdict.
    """
    proven = NativeAccountAuthorityPayload.model_validate(authority)
    return FinancialAccountOwnerKey(
        environment=cast(Literal["production", "demo"], proven.scope.environment),
        account_uid=proven.venue_account_id,
    )


class FinancialAccountOwnerAssignment(_FrozenOwnerEvidence):
    """An explicitly configured local owner; read roots do not imply this assignment."""

    owner_key: FinancialAccountOwnerKey
    owner_wallet_public_id: EvidenceUUID


class FinancialAccountOwnerPolicy(_FrozenOwnerEvidence):
    """One versioned assignment document; an empty policy revokes all current assignments."""

    schema_version: Literal[1] = 1
    assignments: tuple[FinancialAccountOwnerAssignment, ...]

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_integer_version(cls, value: object) -> object:
        """Reject boolean, string and floating aliases for the immutable policy version.

        Args:
            value: Original policy discriminator before strict literal validation.

        Returns:
            The same integer version without coercion.
        """
        if type(value) is not int:
            raise ValueError("Financial owner policy version must be an integer")
        return value

    @field_validator("assignments")
    @classmethod
    def validate_unique_assignments(
        cls, assignments: tuple[FinancialAccountOwnerAssignment, ...]
    ) -> tuple[FinancialAccountOwnerAssignment, ...]:
        """Refuse every duplicate external key and normalize only nonsemantic document order.

        Args:
            assignments: Strict immutable explicit assignments, including an empty revocation.

        Returns:
            The same unique assignments in deterministic external-key order.
        """
        keyed = {financial_account_owner_key_digest(item.owner_key): item for item in assignments}
        if len(keyed) != len(assignments):
            raise ValueError("Financial owner policy repeats an external account key")
        return tuple(keyed[key] for key in sorted(keyed))


def canonical_financial_account_owner_policy(policy: FinancialAccountOwnerPolicy) -> str:
    """Serialize exact assignment semantics independently of input list order.

    Args:
        policy: Explicit assignment document, without an implication that it is persisted.

    Returns:
        Stable strict JSON retaining the schema and every unique owner assignment.
    """
    proven = FinancialAccountOwnerPolicy.model_validate(policy)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def financial_account_owner_policy_digest(policy: FinancialAccountOwnerPolicy) -> str:
    """Hash complete policy semantics under its own domain, separate from source or authority IDs.

    Args:
        policy: Strict explicit assignments to be rebound to a particular Setting version.

    Returns:
        The canonical versioned policy digest without granting financial permission.
    """
    material = canonical_financial_account_owner_policy(policy)
    return hashlib.sha256(_POLICY_DOMAIN + material.encode()).hexdigest()


def _unique_policy_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Refuse repeated JSON fields rather than silently select another configured owner."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Financial owner policy contains duplicate JSON fields")
        result[key] = value
    return result


def _reject_policy_constant(value: str) -> NoReturn:
    """Refuse nonstandard JSON numbers without retaining the raw configured input in errors."""
    raise ValueError("Financial owner policy contains a nonstandard JSON constant")


def parse_financial_account_owner_policy(value: str) -> FinancialAccountOwnerPolicy:
    """Decode an explicit configured document without last-key wins or implicit empty fallback.

    Args:
        value: Exact original Setting value, parsed without proving its durable source version.

    Returns:
        The strictly typed policy, including an explicitly configured empty revocation document.
    """
    json.loads(
        value, object_pairs_hook=_unique_policy_fields, parse_constant=_reject_policy_constant
    )
    return FinancialAccountOwnerPolicy.model_validate_json(value)


def require_financial_account_owner_assignment(
    policy: FinancialAccountOwnerPolicy,
    key: FinancialAccountOwnerKey,
    wallet_public_id: str,
) -> FinancialAccountOwnerAssignment:
    """Refuse missing or foreign assignments rather than infer a primary wallet from read roots.

    Args:
        policy: Explicit configured semantics whose durable version still needs independent binding.
        key: Exact authenticated external namespace for the proposed financial fact.
        wallet_public_id: Actual local wallet requesting ownership, without alias fallback.

    Returns:
        The uniquely matching configured assignment; this is not a current durable permission.
    """
    proven = FinancialAccountOwnerPolicy.model_validate(policy)
    expected = FinancialAccountOwnerAssignment(
        owner_key=key, owner_wallet_public_id=wallet_public_id
    )
    for assignment in proven.assignments:
        if assignment.owner_key == expected.owner_key:
            if assignment.owner_wallet_public_id != expected.owner_wallet_public_id:
                raise FinancialAccountOwnerMismatchError(
                    "Financial account belongs to another configured wallet"
                )
            return assignment
    raise FinancialAccountOwnerUnavailableError(
        "Explicit financial account owner assignment is unavailable"
    )


class FinancialAccountOwnerPolicyReference(_FrozenOwnerEvidence):
    """An exact SCD2 Setting revision, never a cache or a separately read value-only setting."""

    setting_key: Literal["financial_account_owner_policy_v1"] = FINANCIAL_OWNER_SETTING_KEY
    public_id: EvidenceUUID
    timestamp: EvidenceInstant
    session_id: EvidenceUUID
    sequence_id: int = Field(ge=1)
    policy_content_digest: EvidenceDigest


class FinancialAccountOwnerEvidence(_FrozenOwnerEvidence):
    """Persisted decision evidence to rebind; a valid shape cannot grant current permission.

    Historical replay may retain a closed policy revision. New financial writes
    must independently lock and verify the active durable revision and refuse
    unsupported ownership transfers or absent actual private transport proof.
    """

    policy_reference: FinancialAccountOwnerPolicyReference
    policy: FinancialAccountOwnerPolicy
    assignment: FinancialAccountOwnerAssignment
    authority_reference: AccountAuthorityReference
    decided_at: EvidenceInstant

    @model_validator(mode="after")
    def validate_retained_decision(self) -> Self:
        """Keep exact policy semantics and original knowledge references mutually consistent.

        Returns:
            The same historical evidence, pending actual policy and native graph binding.
        """
        reference = self.policy_reference
        authority = self.authority_reference
        if reference.policy_content_digest != financial_account_owner_policy_digest(self.policy):
            raise ValueError("Financial owner evidence contradicts its policy digest")
        require_financial_account_owner_assignment(
            self.policy, self.assignment.owner_key, self.assignment.owner_wallet_public_id
        )
        if authority.family != "native_accounts_v1":
            raise ValueError(
                "Financial owner evidence requires an explicit native authority family"
            )
        if max(reference.timestamp, authority.recorded_at) > self.decided_at:
            raise ValueError(
                "Financial owner decision predates retained policy or authority evidence"
            )
        return self


def financial_account_owner_evidence_digest(evidence: FinancialAccountOwnerEvidence) -> str:
    """Hash an exact retained policy decision without treating that hash as current authorization.

    Args:
        evidence: Complete immutable policy version, configured assignment and authority reference.

    Returns:
        The versioned evidence digest for later exact graph rebinding.
    """
    proven = FinancialAccountOwnerEvidence.model_validate(evidence)
    material = json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(_EVIDENCE_DOMAIN + material.encode()).hexdigest()
