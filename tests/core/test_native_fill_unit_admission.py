"""Validate only the strict retained reference envelope, without claiming source or row authority."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

import pytest
from pydantic import ValidationError

from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference

_RECORDED = datetime(2026, 10, 24, 4, 0, tzinfo=UTC)
_ADMISSION_ID = "d497a83b-381a-430b-a296-adcb0b63607c"
_UNIT_ID = "f6f7a699-19cb-4cda-98ee-9bfbbd03c90f"


def _envelope() -> NativeFillUnitAdmissionEvidence:
    """Build schema references with ordered local clocks and distinct stable record identities."""
    return NativeFillUnitAdmissionEvidence(
        schema_version=1,
        submission_admission=NativeUnitRecordReference(
            public_id=_ADMISSION_ID,
            content_digest="a" * 64,
            recorded_at=_RECORDED,
        ),
        unit_snapshot=NativeUnitRecordReference(
            public_id=_UNIT_ID,
            content_digest="b" * 64,
            recorded_at=_RECORDED + timedelta(microseconds=1),
        ),
    )


def test_explicit_integer_version_preserves_exact_reference_json_roundtrip() -> None:
    """Round-trip the explicit version and both original record references.

    Given integer version one and exact references,
    When serialized,
    Then both edges survive.
    """
    evidence = _envelope()
    encoded = evidence.model_dump_json()
    restored = NativeFillUnitAdmissionEvidence.model_validate_json(encoded)
    assert restored == evidence
    assert type(restored.schema_version) is int
    assert restored.schema_version == 1
    assert restored.submission_admission.public_id == _ADMISSION_ID
    assert restored.unit_snapshot.public_id == _UNIT_ID
    assert (
        restored.unit_snapshot.recorded_at - restored.submission_admission.recorded_at
        == timedelta(microseconds=1)
    )


@pytest.mark.parametrize("version", [True, False, 1.0, "1", None, 2])
def test_version_aliases_and_unsupported_integer_are_rejected(version: object) -> None:
    """Refuse coercible aliases and unsupported envelope versions.

    Given a coercible or unsupported version,
    When parsed,
    Then the original schema is refused.
    """
    payload: dict[str, object] = _envelope().model_dump(mode="python")
    payload["schema_version"] = version
    with pytest.raises(ValidationError):
        NativeFillUnitAdmissionEvidence.model_validate(payload)


@pytest.mark.parametrize("damage", ["later_admission", "same_identity"])
def test_reference_edges_require_original_knowledge_order_and_distinct_ids(
    damage: Literal["later_admission", "same_identity"],
) -> None:
    """Enforce chronology and distinct row identities at the envelope boundary.

    Given independently valid but inconsistent edges,
    When composed,
    Then the envelope is refused.
    """
    evidence = _envelope()
    admission = evidence.submission_admission
    if damage == "later_admission":
        admission = admission.model_copy(
            update={"recorded_at": evidence.unit_snapshot.recorded_at + timedelta(microseconds=1)}
        )
    else:
        admission = admission.model_copy(update={"public_id": evidence.unit_snapshot.public_id})
    with pytest.raises(ValidationError):
        NativeFillUnitAdmissionEvidence(
            submission_admission=admission,
            unit_snapshot=evidence.unit_snapshot,
        )


@pytest.mark.parametrize("damage", ["version", "later_admission", "same_identity"])
def test_unsafe_copied_envelope_is_revalidated_as_an_existing_model(
    damage: Literal["version", "later_admission", "same_identity"],
) -> None:
    """Revalidate unsafe copies of the complete envelope.

    Given an unsafe model copy,
    When the public schema revalidates it,
    Then no invalid edge survives.
    """
    evidence = _envelope()
    changes: dict[str, object]
    if damage == "version":
        changes = {"schema_version": True}
    elif damage == "later_admission":
        changes = {
            "submission_admission": evidence.submission_admission.model_copy(
                update={
                    "recorded_at": evidence.unit_snapshot.recorded_at + timedelta(microseconds=1)
                }
            )
        }
    else:
        changes = {
            "unit_snapshot": evidence.unit_snapshot.model_copy(
                update={"public_id": evidence.submission_admission.public_id}
            )
        }
    unsafe = evidence.model_copy(update=changes)
    with pytest.raises(ValidationError):
        NativeFillUnitAdmissionEvidence.model_validate(unsafe)


@pytest.mark.parametrize("field", ["public_id", "content_digest", "recorded_at"])
def test_unsafe_nested_reference_is_revalidated_directly_and_inside_envelope(
    field: Literal["public_id", "content_digest", "recorded_at"],
) -> None:
    """Revalidate each original nested reference even inside a copied envelope.

    Given an unsafe nested copy,
    When validated alone or retained in an envelope,
    Then it is refused.
    """
    evidence = _envelope()
    invalid_values: dict[str, object] = {
        "public_id": "not-a-record-uuid",
        "content_digest": "not-a-sha256",
        "recorded_at": _RECORDED.replace(tzinfo=None),
    }
    unsafe = evidence.submission_admission.model_copy(update={field: invalid_values[field]})
    with pytest.raises(ValidationError):
        NativeUnitRecordReference.model_validate(unsafe)
    with pytest.raises(ValidationError):
        NativeFillUnitAdmissionEvidence.model_validate(
            evidence.model_copy(update={"submission_admission": unsafe})
        )


@pytest.mark.parametrize("nested", [False, True])
def test_envelope_and_nested_reference_refuse_ordinary_mutation(nested: bool) -> None:
    """Keep both the outer envelope and its retained references immutable.

    Given a retained frozen shape,
    When ordinary mutation is attempted,
    Then both layers refuse it.
    """
    evidence = _envelope()
    target = evidence.unit_snapshot if nested else evidence
    name = "public_id" if nested else "schema_version"
    value: object = _ADMISSION_ID if nested else 1
    with pytest.raises(ValidationError, match="frozen"):
        setattr(target, name, value)
    assert evidence == _envelope()
