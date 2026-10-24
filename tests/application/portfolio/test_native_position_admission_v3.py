"""Verify V3 envelope shape against independently retained genuine SDK admission and source rows."""

import hashlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from snapper.application.portfolio.accounting_native_source import native_source_family_digest
from snapper.application.portfolio.accounting_native_source import native_source_identity_digest
from snapper.application.portfolio.accounting_native_source import native_source_log_id
from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.portfolio.native_financial_payloads_v2 import (
    canonical_native_financial_payload_v2,
)
from snapper.application.portfolio.native_position_admission_v3 import AdmittedNativePositionEventV3
from snapper.application.portfolio.native_position_admission_v3 import (
    admitted_native_position_digest_v3,
)
from snapper.application.portfolio.native_position_admission_v3 import (
    canonical_admitted_native_position_v3,
)
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.helpers.unit_admission import admission_id


def _assert_exact_versions(envelope: AdmittedNativePositionEventV3) -> None:
    """Reject scalar aliases independently at both immutable financial boundaries."""
    assert type(envelope.schema_version) is int and envelope.schema_version == 3
    assert type(envelope.position.schema_version) is int and envelope.position.schema_version == 2
    for rejected in (True, False, 3.0, "3", 0, 2, 4):
        material = envelope.model_dump(mode="python")
        material["schema_version"] = rejected
        with pytest.raises(ValidationError):
            AdmittedNativePositionEventV3.model_validate(material)
    for rejected in (True, False, 2.0, "2", 0, 1, 3):
        position = envelope.position.model_dump(mode="python")
        position["schema_version"] = rejected
        material = envelope.model_dump(mode="python")
        material["position"] = position
        with pytest.raises(ValidationError):
            AdmittedNativePositionEventV3.model_validate(material)


def _assert_original_bytes(
    envelope: AdmittedNativePositionEventV3, original: ContractPositionEventV2
) -> None:
    """Preserve the exact original inner codec while hashing the complete separate envelope."""
    original_bytes = canonical_native_financial_payload_v2(original)
    canonical = canonical_admitted_native_position_v3(envelope)
    restored = AdmittedNativePositionEventV3.model_validate_json(canonical)
    assert restored.position == original
    assert canonical_native_financial_payload_v2(restored.position) == original_bytes
    assert canonical_native_financial_payload_v2(envelope.position) == original_bytes
    assert canonical_admitted_native_position_v3(restored) == canonical
    assert (
        admitted_native_position_digest_v3(envelope)
        == hashlib.sha256(
            b"snapper:admitted-native-position:v3\x00" + canonical.encode()
        ).hexdigest()
    )


def _assert_unsafe_copies_revalidate(envelope: AdmittedNativePositionEventV3) -> None:
    """Force unchecked copies through both public canonicalization and digest validation."""
    evidence = envelope.fill_unit_admission
    malformed_unit = evidence.unit_snapshot.model_copy(update={"content_digest": "invalid"})
    malformed_evidence = evidence.model_copy(update={"unit_snapshot": malformed_unit})
    malformed_position = envelope.position.model_copy(update={"schema_version": 2.0})
    malformed_source = envelope.position.source.model_copy(update={"recorded_at": "invalid"})
    candidates = (
        envelope.model_copy(update={"schema_version": 3.0}),
        envelope.model_copy(update={"position": malformed_position}),
        envelope.model_copy(update={"fill_unit_admission": malformed_evidence}),
        envelope.model_copy(
            update={"position": envelope.position.model_copy(update={"source": malformed_source})}
        ),
    )
    for candidate in candidates:
        with pytest.raises(ValidationError):
            canonical_admitted_native_position_v3(candidate)
        with pytest.raises(ValidationError):
            admitted_native_position_digest_v3(candidate)


def _assert_unit_and_clock_edges(envelope: AdmittedNativePositionEventV3) -> None:
    """Refuse each changed reference or knowledge edge using explicitly synthetic shape mutations."""
    evidence = envelope.fill_unit_admission
    execution_at = envelope.position.operands.execution_known_at
    posting_at = envelope.position.source.recorded_at
    before_execution = execution_at - timedelta(microseconds=1)
    assert evidence.submission_admission.recorded_at <= execution_at
    assert execution_at <= evidence.unit_snapshot.recorded_at <= posting_at
    assert evidence.unit_snapshot.public_id == envelope.position.operands.unit_snapshot_public_id
    changed_units = (
        evidence.unit_snapshot.model_copy(update={"public_id": admission_id(99001)}),
        evidence.unit_snapshot.model_copy(update={"recorded_at": before_execution}),
        evidence.unit_snapshot.model_copy(
            update={"recorded_at": posting_at + timedelta(microseconds=1)}
        ),
    )
    for changed in changed_units:
        candidate = envelope.model_copy(
            update={"fill_unit_admission": evidence.model_copy(update={"unit_snapshot": changed})}
        )
        with pytest.raises(ValidationError):
            canonical_admitted_native_position_v3(candidate)
    after_execution = execution_at + timedelta(microseconds=1)
    later_admission = evidence.submission_admission.model_copy(
        update={"recorded_at": after_execution}
    )
    later_unit = evidence.unit_snapshot.model_copy(update={"recorded_at": after_execution})
    candidate = envelope.model_copy(
        update={
            "fill_unit_admission": evidence.model_copy(
                update={"submission_admission": later_admission, "unit_snapshot": later_unit}
            )
        }
    )
    assert execution_at <= posting_at
    with pytest.raises(ValidationError):
        canonical_admitted_native_position_v3(candidate)


def _assert_complete_source_keys(
    envelope: AdmittedNativePositionEventV3, claim: AccountingNativeSourceClaim
) -> None:
    """Keep both actual native uniqueness keys independent of financial wrapper version."""
    position = envelope.position
    scope = position.operands.scope
    entry = position.operands.source_position.source_entry
    assert (
        claim.wallet_public_id,
        claim.exchange,
        claim.mode,
        claim.environment,
        claim.source_domain,
        claim.source_log_id,
    ) == (
        scope.wallet_public_id,
        scope.exchange,
        scope.mode,
        scope.environment,
        "kraken_futures.account_log",
        native_source_log_id(entry),
    )
    assert (claim.scope_digest, claim.source_domain, claim.source_identity_digest) == (
        native_source_family_digest(scope),
        "kraken_futures.account_log",
        native_source_identity_digest(entry),
    )


@pytest.mark.asyncio
async def test_v3_shape_preserves_actual_v2_bytes_references_clocks_and_source_namespace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconstruct the actual admitted envelope independently before testing its strict codec.

    Given: Genuine signed HTTP and SDK sources commit a V3 position with original admission and unit rows.
    When: Independently read references reconstruct that envelope and public codecs receive unsafe shapes.
    Then: Versions, nested copies, unit and clocks are strict while inner bytes and complete source keys persist.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        result = await record_native_position_scenario_v3(scenario)
        async with scenario.source.repository.session() as session:
            original = await get_native_financial_event_family(
                session, result.position.public_id, datetime.now(UTC)
            )
            assert isinstance(original, AdmittedNativeEventMaterialV3)
            assert isinstance(original.payload, ContractPositionEventV2)
            admission = await get_bound_account_submission_admission(
                session, scenario.request.submission_admission.public_id
            )
            unit = (
                await session.scalars(
                    select(ExecutionUnitSnapshot).where(
                        ExecutionUnitSnapshot.public_id == result.unit_snapshot_public_id
                    )
                )
            ).one()
            assert (
                await get_bound_execution_unit_snapshot(session, unit.public_id)
                == original.payload.operands.unit
            )
            claim = (
                await session.scalars(
                    select(AccountingNativeSourceClaim).where(
                        AccountingNativeSourceClaim.accounting_event_public_id
                        == result.position.public_id
                    )
                )
            ).one()
            evidence = NativeFillUnitAdmissionEvidence(
                submission_admission=NativeUnitRecordReference(
                    public_id=admission.public_id,
                    content_digest=admission.content_digest,
                    recorded_at=admission.recorded_at,
                ),
                unit_snapshot=NativeUnitRecordReference(
                    public_id=unit.public_id,
                    content_digest=unit.content_digest,
                    recorded_at=unit.timestamp,
                ),
            )
            envelope = AdmittedNativePositionEventV3(
                position=original.payload, fill_unit_admission=evidence
            )
            assert evidence.submission_admission == scenario.request.submission_admission
            assert envelope == original.envelope
            assert canonical_admitted_native_position_v3(envelope) == original.row.canonical_payload
            assert admitted_native_position_digest_v3(envelope) == original.row.content_digest
            assert envelope.position.source.recorded_at == original.row.timestamp
            _assert_exact_versions(envelope)
            _assert_original_bytes(envelope, original.payload)
            _assert_unsafe_copies_revalidate(envelope)
            _assert_unit_and_clock_edges(envelope)
            _assert_complete_source_keys(envelope, claim)
