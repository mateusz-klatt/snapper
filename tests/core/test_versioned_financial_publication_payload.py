"""Require explicit exact notification versions without weakening either original model validator."""

import hashlib
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from snapper.core.native_financial_publication_v2 import canonical_native_position_publication_v2
from snapper.core.native_financial_publication_v2 import native_position_publication_digest_v2
from snapper.core.versioned_financial_publication_payload import (
    validate_financial_publication_payload,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.messaging.infrastructure.test_financial_publication_transport import _structural_envelope
from tests.messaging.infrastructure.test_financial_publication_transport import _structural_payload


@pytest.mark.parametrize("version", [None, True, False, 1.0, 2.0, "1", "2", 0, 3])
def test_wire_version_aliases_cannot_select_another_notification_codec(version: object) -> None:
    """The external envelope requires the exact declared codec before validating its payload.

    Given: Structural legacy message material with no assertion that its referenced records exist.
    When: Its wire version is absent, an alias or unsupported rather than an exact supported integer.
    Then: The actual envelope validator rejects it without trying another notification family.
    """
    envelope = _structural_envelope(SequenceTracker())
    values = envelope.model_dump(mode="python")
    values["payload"] = {**envelope.payload.model_dump(mode="python"), "schema_version": version}
    with pytest.raises(ValidationError):
        FinancialPublicationData.model_validate(values)


def test_missing_wire_version_cannot_inherit_a_model_default() -> None:
    """A model's construction default cannot silently choose an external notification family.

    Given: An otherwise valid structural legacy wire envelope with an explicit version removed.
    When: The actual message model validates the external payload.
    Then: It refuses the missing declaration before default values can select a family.
    """
    envelope = _structural_envelope(SequenceTracker())
    values = envelope.model_dump(mode="python")
    values["payload"] = envelope.payload.model_dump(mode="python", exclude={"schema_version"})
    with pytest.raises(ValidationError):
        FinancialPublicationData.model_validate(values)


@pytest.mark.parametrize("payload", [None, "1", 1, []])
def test_nonobject_wire_payload_cannot_become_a_notification(payload: object) -> None:
    """A malformed external payload cannot acquire financial notification structure.

    Given: A complete structural message envelope whose payload is replaced by a scalar or list.
    When: The actual external message model validates the original value.
    Then: It rejects the value without selecting or constructing a notification variant.
    """
    values = _structural_envelope(SequenceTracker()).model_dump(mode="python")
    values["payload"] = payload
    with pytest.raises(ValidationError):
        FinancialPublicationData.model_validate(values)


@pytest.mark.parametrize("version", [None, True, False, 1.0, 2.0, "1", "2", 0, 2, 3])
def test_unsafe_model_copy_cannot_replace_its_original_notification_family(version: object) -> None:
    """A typed instance remains subject to explicit family selection and complete validation.

    Given: A structural legacy model whose version is changed through an unsafe model copy.
    When: The actual typed publication boundary revalidates the instance.
    Then: It refuses aliases and mismatched variants rather than treating the old class as authority.
    """
    copied = _structural_payload().model_copy(update={"schema_version": version})
    with pytest.raises(ValidationError):
        validate_financial_publication_payload(copied)


def test_exact_legacy_wire_and_model_keep_their_original_values() -> None:
    """Strict family routing preserves both external and typed structural payload values.

    Given: An explicitly versioned structural legacy message with canonical original fields.
    When: Both the wire envelope and typed publication validators run.
    Then: They retain the original payload without claiming that any referenced fact exists.
    """
    envelope = _structural_envelope(SequenceTracker())
    actual = FinancialPublicationData.model_validate(envelope.model_dump(mode="python"))
    assert actual.payload == envelope.payload
    assert validate_financial_publication_payload(envelope.payload) == envelope.payload


@pytest.mark.asyncio
async def test_native_publication_canonical_boundary_revalidates_original_source_clocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse contradictory copied references at the public canonical and digest boundaries.

    Given: A genuine SDK-derived position has committed its complete native notification.
    When: Unsafe copies change a version, source family or original financial knowledge edge.
    Then: Both public serializers refuse while the original canonical identity stays exact.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        original = recorded.publication.payload
        canonical = canonical_native_position_publication_v2(original)
        digest = native_position_publication_digest_v2(original)
        assert (
            digest
            == hashlib.sha256(
                b"snapper:native-position-publication:v2\x00" + canonical.encode()
            ).hexdigest()
        )
        after_execution = original.execution_known_at + timedelta(microseconds=1)
        after_position = original.position_known_at + timedelta(microseconds=1)
        candidates = (
            ("version requires an integer", original.model_copy(update={"schema_version": 2.0})),
            (
                "original fill knowledge is unavailable",
                original.model_copy(
                    update={
                        "fill_authority_reference": original.fill_authority_reference.model_copy(
                            update={"recorded_at": after_execution}
                        )
                    }
                ),
            ),
            (
                "original fill knowledge is unavailable",
                original.model_copy(
                    update={
                        "fill_ownership_reference": original.fill_ownership_reference.model_copy(
                            update={"recorded_at": after_execution}
                        )
                    }
                ),
            ),
            (
                "requires a tagged native position authority",
                original.model_copy(
                    update={
                        "position_authority_reference": original.position_authority_reference.model_copy(
                            update={"family": "legacy_route_v1"}
                        )
                    }
                ),
            ),
            (
                "precedes its actual source knowledge",
                original.model_copy(
                    update={
                        "position_authority_reference": original.position_authority_reference.model_copy(
                            update={"recorded_at": after_position}
                        )
                    }
                ),
            ),
            (
                "precedes its actual source knowledge",
                original.model_copy(
                    update={
                        "position_ownership_reference": original.position_ownership_reference.model_copy(
                            update={"recorded_at": after_position}
                        )
                    }
                ),
            ),
            (
                "precedes its actual source knowledge",
                original.model_copy(
                    update={
                        "owner_policy_reference": original.owner_policy_reference.model_copy(
                            update={"timestamp": after_position}
                        )
                    }
                ),
            ),
            (
                "changes its original posting decision clock",
                original.model_copy(update={"owner_decided_at": after_position}),
            ),
            (
                "changes its original posting decision clock",
                original.model_copy(
                    update={"recorded_at": original.position_known_at - timedelta(microseconds=1)}
                ),
            ),
        )
        for reason, candidate in candidates:
            with pytest.raises(ValidationError, match=reason):
                canonical_native_position_publication_v2(candidate)
            with pytest.raises(ValidationError, match=reason):
                native_position_publication_digest_v2(candidate)
        assert canonical_native_position_publication_v2(original) == canonical
        assert native_position_publication_digest_v2(original) == digest
