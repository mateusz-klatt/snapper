"""Keep explicit durable-policy evidence distinct from aliases and current financial permission."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.core.financial_account_owner import FinancialAccountOwnerKey
from snapper.core.financial_account_owner import FinancialAccountOwnerMismatchError
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicyReference
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import canonical_financial_account_owner_policy
from snapper.core.financial_account_owner import financial_account_owner_evidence_digest
from snapper.core.financial_account_owner import financial_account_owner_key_digest
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.financial_account_owner import financial_account_owner_policy_digest
from snapper.core.financial_account_owner import parse_financial_account_owner_policy
from snapper.core.financial_account_owner import require_financial_account_owner_assignment
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from tests.core.test_native_account_authority import authority as _authority_fixture

_NOW = datetime(2026, 10, 23, 17, tzinfo=UTC)
authority = _authority_fixture


def _id(number: int) -> str:
    """Provide deterministic public fixture IDs without relying on a selected runtime account."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _assignment(uid: int = 1, wallet: int = 2) -> FinancialAccountOwnerAssignment:
    """Construct explicit illustrative configured ownership without a persisted grant."""
    return FinancialAccountOwnerAssignment(
        owner_key=FinancialAccountOwnerKey(environment="production", account_uid=_id(uid)),
        owner_wallet_public_id=_id(wallet),
    )


def _evidence() -> FinancialAccountOwnerEvidence:
    """Keep independent exact policy and authority clocks in a pure historical fixture."""
    assignment = _assignment()
    policy = FinancialAccountOwnerPolicy(assignments=(assignment,))
    return FinancialAccountOwnerEvidence(
        policy_reference=FinancialAccountOwnerPolicyReference(
            public_id=_id(3),
            timestamp=_NOW - timedelta(seconds=2),
            session_id=_id(4),
            sequence_id=1,
            policy_content_digest=financial_account_owner_policy_digest(policy),
        ),
        policy=policy,
        assignment=assignment,
        authority_reference=AccountAuthorityReference(
            family="native_accounts_v1",
            public_id=_id(5),
            content_digest="a" * 64,
            recorded_at=_NOW - timedelta(seconds=1),
        ),
        decided_at=_NOW,
    )


def test_document_order_has_no_effect_on_unique_owner_policy_semantics() -> None:
    """Keep configuration order independent without deduplicating contradictory assignments.

    Given: The same two distinct external owners are explicitly assigned in opposite orders.
    When: Both policies cross the strict canonical boundary.
    Then: Assignment order, canonical JSON and versioned policy digests are identical.
    """
    first, second = _assignment(), _assignment(3, 4)
    forward = FinancialAccountOwnerPolicy(assignments=(first, second))
    backward = FinancialAccountOwnerPolicy(assignments=(second, first))

    assert forward == backward
    assert canonical_financial_account_owner_policy(
        forward
    ) == canonical_financial_account_owner_policy(backward)
    assert financial_account_owner_policy_digest(forward) == financial_account_owner_policy_digest(
        backward
    )
    assert (
        parse_financial_account_owner_policy(canonical_financial_account_owner_policy(forward))
        == forward
    )


@pytest.mark.parametrize("other_wallet", [2, 3])
def test_repeated_external_key_refuses_same_and_conflicting_owner_duplicates(
    other_wallet: int,
) -> None:
    """Reject repeated namespace configuration even when the repeated owner happens to agree.

    Given: One external namespace has an explicit local owner.
    When: Another assignment repeats its key with the same or a different wallet.
    Then: The document refuses rather than silently deduplicating or selecting a primary.
    """
    with pytest.raises(ValidationError, match="repeats an external account key"):
        FinancialAccountOwnerPolicy(assignments=(_assignment(), _assignment(wallet=other_wallet)))


def test_explicit_empty_policy_is_revocation_while_missing_document_fields_refuse() -> None:
    """Make revoke-all a real versioned document rather than an interpretation failure fallback.

    Given: A policy explicitly contains an empty assignment list.
    When: It is compared with missing assignments and an attempted owner lookup.
    Then: Empty semantics have a real digest, missing fields refuse and no assignment is inferred.
    """
    empty = parse_financial_account_owner_policy('{"schema_version":1,"assignments":[]}')

    assert empty.assignments == ()
    assert len(financial_account_owner_policy_digest(empty)) == 64
    assert financial_account_owner_policy_digest(empty) != financial_account_owner_policy_digest(
        _evidence().policy
    )
    with pytest.raises(ValidationError, match="assignments"):
        parse_financial_account_owner_policy('{"schema_version":1}')
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="unavailable"):
        require_financial_account_owner_assignment(empty, _assignment().owner_key, _id(2))


def test_actual_wallet_must_match_explicit_owner_without_alias_fallback() -> None:
    """Refuse another local alias even when its read-only identity would name the same UID.

    Given: One external account is explicitly assigned to wallet two.
    When: Wallet two and wallet nine each request that same configured assignment.
    Then: Only the exact owner matches and the other alias receives a typed mismatch.
    """
    assignment = _assignment()
    policy = FinancialAccountOwnerPolicy(assignments=(assignment,))

    assert (
        require_financial_account_owner_assignment(policy, assignment.owner_key, _id(2))
        == assignment
    )
    with pytest.raises(FinancialAccountOwnerMismatchError, match="another configured wallet"):
        require_financial_account_owner_assignment(policy, assignment.owner_key, _id(9))


def test_same_uid_demo_and_production_are_distinct_external_financial_books() -> None:
    """Separate authenticated environments without multiplying owners by margin or instrument.

    Given: The same source UID appears in production and demo namespaces.
    When: Its key digest and explicit production assignment are evaluated for demo.
    Then: The keys differ and production assignment cannot authorize the demo namespace.
    """
    assignment = _assignment()
    demo = FinancialAccountOwnerKey(
        environment="demo", account_uid=assignment.owner_key.account_uid
    )

    assert financial_account_owner_key_digest(demo) != financial_account_owner_key_digest(
        assignment.owner_key
    )
    with pytest.raises(FinancialAccountOwnerUnavailableError):
        require_financial_account_owner_assignment(
            FinancialAccountOwnerPolicy(assignments=(assignment,)), demo, _id(2)
        )


@pytest.mark.parametrize(
    "field", ["wallet_public_id", "margin_account", "credential_public_id", "instrument_public_id"]
)
def test_external_owner_key_cannot_be_partitioned_by_alias_or_instrument_metadata(
    field: str,
) -> None:
    """Reject extra partition fields instead of dropping their different financial intent.

    Given: A valid external financial key without local or per-instrument namespaces.
    When: Another partition dimension is supplied.
    Then: Strict validation refuses the field and cannot create a second independent owner key.
    """
    with pytest.raises(ValidationError, match="Extra inputs"):
        FinancialAccountOwnerKey.model_validate(
            _assignment().owner_key.model_dump() | {field: _id(7)}
        )


def test_native_key_projection_uses_safe_uid_not_profile_label_or_credential(
    authority: NativeAccountAuthorityPayload,
) -> None:
    """Project only the authenticated external owner namespace from a validated native authority.

    Given: A native authority has a literal margin profile and actual safe owner UID.
    When: The pure owner key is derived.
    Then: The key retains the UID and environment with no wallet, margin or credential partition.
    """
    key = financial_account_owner_key_from_authority(authority)

    assert key.account_uid == authority.venue_account_id
    assert key.environment == authority.scope.environment
    assert key.model_dump() == {
        "exchange": "kraken_futures",
        "mode": "live",
        "environment": "production",
        "account_uid": authority.venue_account_id,
        "source_domain": "kraken_futures.account_log",
    }


@pytest.mark.parametrize("version", [True, False, "1", 1.0])
def test_policy_version_aliases_cannot_enter_a_canonical_owner_document(version: object) -> None:
    """Refuse forged copied policy versions before hashing or evaluating the assignment.

    Given: An otherwise valid policy has an unchecked non-integer version alias.
    When: It reaches the canonical boundary.
    Then: The source refuses rather than hashing it under the version-one domain.
    """
    forged = _evidence().policy.model_copy(update={"schema_version": version})
    with pytest.raises(ValidationError, match="must be an integer"):
        canonical_financial_account_owner_policy(forged)


@pytest.mark.parametrize(
    "value",
    [
        '{"assignments":[],"assignments":[]}',
        '{"schema_version":1,"assignments":[],"extra":NaN}',
        '{"schema_version":1,"assignments":[],"extra":Infinity}',
    ],
)
def test_ambiguous_or_nonstandard_json_policy_never_becomes_implicit_empty(value: str) -> None:
    """Reject ambiguous source documents before any last-key or invalid-number interpretation.

    Given: Original configured JSON repeats a field or contains a nonstandard number.
    When: The exact document is parsed.
    Then: It refuses without selecting an owner or constructing a revoke-all fallback.
    """
    with pytest.raises(ValueError, match="Financial owner policy contains"):
        parse_financial_account_owner_policy(value)


def test_exact_setting_revision_remains_distinct_even_when_policy_semantics_repeat() -> None:
    """Pin the actual historical policy version instead of substituting an equal current value.

    Given: Identical policy semantics appear at two different durable Setting versions.
    When: Their complete retained decision evidence is serialized and hashed.
    Then: Each exact revision survives and yields a distinct evidence digest.
    """
    evidence = _evidence()
    reference = FinancialAccountOwnerPolicyReference.model_validate(
        evidence.policy_reference.model_dump()
        | {"public_id": _id(8), "timestamp": _NOW - timedelta(seconds=1)}
    )
    later = FinancialAccountOwnerEvidence.model_validate(
        evidence.model_dump() | {"policy_reference": reference}
    )
    restored = FinancialAccountOwnerEvidence.model_validate_json(evidence.model_dump_json())

    assert restored == evidence
    assert later.policy == evidence.policy
    assert financial_account_owner_evidence_digest(
        restored
    ) == financial_account_owner_evidence_digest(evidence)
    assert financial_account_owner_evidence_digest(
        later
    ) != financial_account_owner_evidence_digest(evidence)


@pytest.mark.parametrize("target", ["policy", "authority"])
def test_decision_cannot_predate_original_policy_or_authority_knowledge(target: str) -> None:
    """Keep policy and authority retained clocks independent of source economic time.

    Given: A configured owner decision names actual retained policy and native authority references.
    When: Either retained clock is moved after the decision.
    Then: The evidence refuses rather than claiming the later source was already known.
    """
    evidence = _evidence()
    change: dict[str, object]
    if target == "policy":
        change = {
            "policy_reference": evidence.policy_reference.model_copy(
                update={"timestamp": _NOW + timedelta(microseconds=1)}
            )
        }
    else:
        change = {
            "authority_reference": evidence.authority_reference.model_copy(
                update={"recorded_at": _NOW + timedelta(microseconds=1)}
            )
        }
    with pytest.raises(ValidationError, match="predates retained"):
        FinancialAccountOwnerEvidence.model_validate(evidence.model_dump() | change)


def test_policy_digest_and_explicit_native_family_must_match_retained_decision() -> None:
    """Refuse contradictory policy bytes or a family-qualified historical authority substitution.

    Given: Valid complete illustrative owner evidence.
    When: Its policy digest or native authority family is changed through an unchecked copy.
    Then: Revalidation refuses before any digest or financial interpretation can use that copy.
    """
    evidence = _evidence()
    wrong_digest = evidence.policy_reference.model_copy(update={"policy_content_digest": "f" * 64})
    with pytest.raises(ValidationError, match="policy digest"):
        financial_account_owner_evidence_digest(
            evidence.model_copy(update={"policy_reference": wrong_digest})
        )
    wrong_family = evidence.authority_reference.model_copy(update={"family": "legacy_route_v1"})
    with pytest.raises(ValidationError, match="native authority family"):
        financial_account_owner_evidence_digest(
            evidence.model_copy(update={"authority_reference": wrong_family})
        )


def test_nested_assignment_copy_cannot_replace_configured_owner_in_evidence() -> None:
    """Revalidate nested immutable fields rather than trusting a forged model-copy decision.

    Given: A policy explicitly assigns the external account to wallet two.
    When: Retained decision evidence claims wallet nine instead.
    Then: The unchanged explicit policy rejects that foreign wallet.
    """
    evidence = _evidence()
    forged = evidence.model_copy(
        update={
            "assignment": evidence.assignment.model_copy(update={"owner_wallet_public_id": _id(9)})
        }
    )
    with pytest.raises(ValidationError, match="another configured wallet"):
        financial_account_owner_evidence_digest(forged)


def test_pure_policy_evidence_has_no_current_permission_or_transport_attestation() -> None:
    """Keep evidence-only configuration from masquerading as a runtime financial grant.

    Given: A fully shaped illustrative owner decision with exact policy and authority references.
    When: Its retained fields are inspected.
    Then: No current, funded, transport-verified or trading-permission flag appears.
    """
    evidence = _evidence()
    material = set(evidence.model_dump(mode="json"))

    assert (
        not {"is_current", "is_allowed", "can_trade", "transport_verified", "available_cash"}
        & material
    )
    assert evidence.policy_reference.setting_key == "financial_account_owner_policy_v1"
