"""Version-three envelopes preserve older unit evidence and pin an explicit account family."""

import json
from datetime import timedelta

import pytest
from pydantic import ValidationError

from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.account_bound_submission_admission import AccountBoundSubmissionAdmissionPayload
from snapper.core.account_bound_submission_admission import account_bound_admission_digest
from snapper.core.account_bound_submission_admission import canonical_account_bound_admission
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload
from snapper.core.execution_unit_admission import canonical_unit_admission_payload
from snapper.core.execution_unit_admission import unit_admission_content_digest
from tests.helpers.unit_admission_payload import unit_admission_payload


@pytest.fixture
def evidence() -> AccountBoundSubmissionAdmissionPayload:
    """Supply an explicitly shaped native-family envelope without claiming a persisted source."""
    units = unit_admission_payload()
    return AccountBoundSubmissionAdmissionPayload(
        units=units,
        authority=AccountAuthorityReference(
            family="native_accounts_v1",
            public_id=units.route.authority_public_id,
            content_digest=units.route.authority_content_digest,
            recorded_at=units.interpreted_at - timedelta(seconds=1),
        ),
    )


def test_envelope_roundtrip_preserves_exact_older_unit_graph(
    evidence: AccountBoundSubmissionAdmissionPayload,
) -> None:
    """Retain the original unit schema and canonical bytes inside the additive envelope.

    Given: A complete unit graph and an explicit authority reference.
    When: The version-three envelope is canonically serialized and rebound.
    Then: Its nested version-two bytes, original snapshot references and digest remain intact.
    """
    old_canonical = canonical_unit_admission_payload(evidence.units)
    old_digest = unit_admission_content_digest(evidence.units)
    canonical = canonical_account_bound_admission(evidence)
    restored = AccountBoundSubmissionAdmissionPayload.model_validate_json(canonical)
    assert restored == evidence
    assert canonical_unit_admission_payload(restored.units) == old_canonical
    assert unit_admission_content_digest(restored.units) == old_digest
    assert json.dumps(json.loads(canonical)["units"], sort_keys=True, separators=(",", ":")) == (
        old_canonical
    )
    assert account_bound_admission_digest(restored) != old_digest
    assert canonical_account_bound_admission(restored) == canonical


@pytest.mark.parametrize("version", [True, False, 3.0, "3", 2, 4])
def test_envelope_refuses_coerced_or_other_schema_versions(
    evidence: AccountBoundSubmissionAdmissionPayload, version: object
) -> None:
    """Make the new envelope discriminator a strict compatibility boundary.

    Given: An otherwise complete envelope with a numeric alias or different version.
    When: The candidate is validated.
    Then: It cannot be silently interpreted as a version-three admission.
    """
    data = evidence.model_dump(mode="python")
    data["schema_version"] = version
    with pytest.raises(ValidationError):
        AccountBoundSubmissionAdmissionPayload.model_validate(data)


@pytest.mark.parametrize("field", ["public_id", "content_digest"])
def test_copied_reference_cannot_disagree_with_the_existing_route(
    evidence: AccountBoundSubmissionAdmissionPayload, field: str
) -> None:
    """Revalidate duplicate identities before canonicalization instead of trusting copied models.

    Given: An envelope whose nested account reference is replaced through unchecked model_copy.
    When: The complete envelope is hashed.
    Then: A changed UUID or digest cannot diverge from the unit graph's actual route reference.
    """
    replacement = "00000000-0000-7000-8000-000000000999" if field == "public_id" else "f" * 64
    authority = evidence.authority.model_copy(update={field: replacement})
    forged = evidence.model_copy(update={"authority": authority})
    with pytest.raises(ValidationError, match="reference disagree"):
        account_bound_admission_digest(forged)


def test_later_durable_authority_cannot_supply_earlier_admission_knowledge(
    evidence: AccountBoundSubmissionAdmissionPayload,
) -> None:
    """Preserve the actual local recording horizon independently of exchange timestamps.

    Given: Authority recording occurs one microsecond after admission interpretation.
    When: A copied envelope attempts to bind that later source.
    Then: Canonicalization refuses rather than backdating durable knowledge.
    """
    authority = evidence.authority.model_copy(
        update={"recorded_at": evidence.units.interpreted_at + timedelta(microseconds=1)}
    )
    with pytest.raises(ValidationError, match="not known at interpretation"):
        canonical_account_bound_admission(evidence.model_copy(update={"authority": authority}))


def test_family_and_actual_recording_clock_change_the_complete_envelope_digest(
    evidence: AccountBoundSubmissionAdmissionPayload,
) -> None:
    """Keep differently interpreted source identities distinct even when the unit graph agrees.

    Given: Two shaped authority families or two recording clocks share a UUID and unit graph.
    When: Separate envelopes are canonically hashed before independent database binding.
    Then: Their complete digests differ while the original unit digest remains identical.
    """
    other_family = evidence.authority.model_copy(update={"family": "legacy_route_v1"})
    earlier = evidence.authority.model_copy(
        update={"recorded_at": evidence.authority.recorded_at - timedelta(seconds=1)}
    )
    variants = (
        evidence,
        evidence.model_copy(update={"authority": other_family}),
        evidence.model_copy(update={"authority": earlier}),
    )
    assert len({account_bound_admission_digest(item) for item in variants}) == 3
    assert len({unit_admission_content_digest(item.units) for item in variants}) == 1


def test_old_reader_refuses_new_envelope_and_new_reader_refuses_old_graph(
    evidence: AccountBoundSubmissionAdmissionPayload,
) -> None:
    """Prevent implicit schema fallback at the historical compatibility boundary.

    Given: The unchanged version-two unit graph and its complete version-three envelope.
    When: Either is offered to the other schema's historical parser.
    Then: Both mismatches fail instead of guessing an account family.
    """
    with pytest.raises(ValidationError):
        SubmissionUnitAdmissionPayload.model_validate_json(
            canonical_account_bound_admission(evidence)
        )
    with pytest.raises(ValidationError):
        AccountBoundSubmissionAdmissionPayload.model_validate_json(
            canonical_unit_admission_payload(evidence.units)
        )


def test_nested_unit_copy_is_revalidated_before_the_new_envelope_is_hashed(
    evidence: AccountBoundSubmissionAdmissionPayload,
) -> None:
    """Retain all existing unit invariants through the new outer schema.

    Given: An unchecked model copy changes the unit graph's wallet without changing its route.
    When: The complete envelope is hashed.
    Then: Original unit validation rejects the contradictory financial scope.
    """
    scope = evidence.units.scope.model_copy(
        update={"wallet_public_id": "00000000-0000-7000-8000-000000000999"}
    )
    units = evidence.units.model_copy(update={"scope": scope})
    with pytest.raises(ValidationError, match="scope must agree"):
        account_bound_admission_digest(evidence.model_copy(update={"units": units}))
