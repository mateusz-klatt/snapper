"""Exercise retained V3 evidence and owner clocks without granting financial readiness."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import tzinfo
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.financial_account_owner_policy_writer as policy_writer
from snapper.application.trade.native_position_recording_v3 import record_native_position_v3
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.native_fill_unit_admission import NativeFillUnitAdmissionEvidence
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Setting
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import admitted_position_request_v3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_publication_store_v2 import (
    append_bound_native_position_publication_v2,
)
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_position_family_history import get_bound_native_position_family
from snapper.data.native_position_history_v2 import get_bound_native_position_event_v2
from snapper.data.native_position_store_v2 import append_native_position_from_receipts_v2
from snapper.data.native_position_store_v3 import append_native_position_from_receipts_v3
from tests.application.trade.test_native_position_recording_v3 import _facts
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_position_accounting_v2 import native_position_scenario_v2
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.helpers.unit_admission import admission_stamp

pytestmark = pytest.mark.asyncio


class _PolicyClock:
    """Observe only an explicitly controlled sample after the actual reserved lock returns."""

    current = datetime(2026, 10, 24, tzinfo=UTC)
    lock_returned = False
    samples: list[datetime] = []

    @staticmethod
    def now(timezone: tzinfo | None = None) -> datetime:
        """Require the actual lock path to complete before returning the one controlled sample."""
        assert _PolicyClock.lock_returned
        _PolicyClock.samples.append(_PolicyClock.current)
        return (
            _PolicyClock.current if timezone is None else _PolicyClock.current.astimezone(timezone)
        )


def _observe_post_lock_clock(monkeypatch: pytest.MonkeyPatch, current: datetime) -> None:
    """Delegate the real database lock and observe its return without simulating a lock wait."""
    actual_lock = policy_writer._lock_reserved_policy
    _PolicyClock.current = current
    _PolicyClock.lock_returned = False
    _PolicyClock.samples = []

    async def observe_lock(session: AsyncSession) -> Setting | None:
        """Run the original reservation before allowing the controlled Python clock sample."""
        row = await actual_lock(session)
        _PolicyClock.lock_returned = True
        return row

    monkeypatch.setattr(policy_writer, "_lock_reserved_policy", observe_lock)
    monkeypatch.setattr(policy_writer, "datetime", _PolicyClock)


def _changed_reference(
    reference: NativeUnitRecordReference, damage: Literal["digest", "clock"]
) -> NativeUnitRecordReference:
    """Retain a valid reference shape while contradicting one actual persisted proof field."""
    changed = NativeUnitRecordReference(
        public_id=reference.public_id,
        content_digest="0" * 64 if damage == "digest" else reference.content_digest,
        recorded_at=(
            reference.recorded_at + timedelta(microseconds=1)
            if damage == "clock"
            else reference.recorded_at
        ),
    )
    assert changed != reference
    return changed


@pytest.mark.parametrize("boundary", ("before", "equal"))
async def test_v3_policy_close_refuses_before_or_at_original_financial_decision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: Literal["before", "equal"]
) -> None:
    """Keep the original V3 owner decision inside its actual immutable policy interval.

    Given: A genuine SDK-derived V3 financial event retains one real owner-policy decision.
    When: The reserved writer samples before or exactly at that decision after its actual lock.
    Then: It refuses without closing the policy, changing financial facts or sampling again.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            original = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
            assert isinstance(original, AdmittedNativeEventMaterialV3)
        decision = original.payload.source.owner_evidence.decided_at
        reference = original.payload.source.owner_evidence.policy_reference
        sampled_at = (
            reference.timestamp + (decision - reference.timestamp) / 2
            if boundary == "before"
            else decision
        )
        assert reference.timestamp < sampled_at <= decision
        if boundary == "before":
            assert sampled_at < decision
        _observe_post_lock_clock(monkeypatch, sampled_at)
        with pytest.raises(
            FinancialAccountOwnerUnavailableError,
            match="financial_owner_policy_clock_not_advanced",
        ):
            await policy_writer.write_financial_owner_policy(
                repository,
                FinancialOwnerPolicyChange(
                    policy=FinancialAccountOwnerPolicy(assignments=()),
                    updated_by="v3-retained-clock-refusal",
                ),
                scenario.source.tracker,
            )
        assert _PolicyClock.samples == [sampled_at]
        assert await _facts(repository) == before
        async with repository.session() as session:
            marker = (
                await session.scalars(
                    select(Setting.known_to).where(
                        Setting.public_id == reference.public_id,
                        Setting.timestamp == reference.timestamp,
                        Setting.session_id == reference.session_id,
                        Setting.sequence_id == reference.sequence_id,
                    )
                )
            ).one()
            rebound = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
        assert marker == KNOWN_TO_MAX and marker > decision
        assert rebound.payload == original.payload
        assert rebound.row.content_digest == original.row.content_digest


async def test_v3_later_policy_close_uses_one_actual_post_lock_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Permit an explicit later revocation without moving the original financial decision.

    Given: The real V3 source graph has one committed owner decision under the current policy.
    When: The real reserved lock returns and the writer samples one later revision instant.
    Then: The policy closes exactly there while the original V3 graph and decision remain readable.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            original = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
            assert isinstance(original, AdmittedNativeEventMaterialV3)
        decision = original.payload.source.owner_evidence.decided_at
        reference = original.payload.source.owner_evidence.policy_reference
        sampled_at = datetime.now(UTC)
        assert sampled_at > decision
        _observe_post_lock_clock(monkeypatch, sampled_at)
        changed = await policy_writer.write_financial_owner_policy(
            repository,
            FinancialOwnerPolicyChange(
                policy=FinancialAccountOwnerPolicy(assignments=()),
                updated_by="v3-retained-later-revocation",
            ),
            scenario.source.tracker,
        )
        assert changed.changed and changed.reference.timestamp == sampled_at
        assert _PolicyClock.samples == [sampled_at]
        assert await _facts(repository) == before
        async with repository.session() as session:
            marker = (
                await session.scalars(
                    select(Setting.known_to).where(
                        Setting.public_id == reference.public_id,
                        Setting.timestamp == reference.timestamp,
                    )
                )
            ).one()
            rebound = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
        assert marker == sampled_at and marker > decision
        assert rebound.payload == original.payload
        assert rebound.row.canonical_payload == original.row.canonical_payload


@pytest.mark.parametrize("damage", ("digest", "clock"))
async def test_wrong_actual_submission_reference_refuses_before_new_v3_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: Literal["digest", "clock"]
) -> None:
    """Refuse a validly shaped reference that contradicts the actual durable admission.

    Given: Genuine signed HTTP and SDK sources exist before any financial target is recorded.
    When: The incoming admission UUID keeps an incorrect digest or recording clock.
    Then: The actual recorder refuses with no new targets and the original request still succeeds.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        before = await _facts(repository)
        assert (
            before.execution,
            before.venue_event,
            before.fill_unit,
            before.anchor,
            before.event,
            before.claim,
            before.publication,
        ) == (0, 0, 0, 0, 0, 0, 0)
        request = scenario.request.model_copy(
            update={
                "submission_admission": _changed_reference(
                    scenario.request.submission_admission, damage
                )
            }
        )
        with pytest.raises(
            NativeFinancialBindingError,
            match="native_recording_original_admission_reference_changed",
        ):
            await record_native_position_v3(
                repository, request, scenario.identity, scenario.source.policy
            )
        assert await _facts(repository) == before
        recorded = await record_native_position_scenario_v3(scenario)
        assert recorded.created and recorded.publication.created


@pytest.mark.parametrize("damage", ("digest", "clock"))
async def test_wrong_retained_unit_reference_cannot_add_or_relabel_v3_targets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: Literal["digest", "clock"]
) -> None:
    """Rebind actual original unit bytes before allowing an otherwise idempotent position retry.

    Given: The real atomic SDK caller has committed a complete original V3 graph and unit proof.
    When: Its actual unit UUID is retried with a changed digest or recording clock.
    Then: The lower writer refuses without new targets and an exact original retry stays idempotent.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            material = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
            assert isinstance(material, AdmittedNativeEventMaterialV3)
        original = admitted_position_request_v3(material.envelope)
        evidence = NativeFillUnitAdmissionEvidence(
            submission_admission=original.fill_unit_admission.submission_admission,
            unit_snapshot=_changed_reference(original.fill_unit_admission.unit_snapshot, damage),
        )
        changed = original.model_copy(update={"fill_unit_admission": evidence})
        with pytest.raises(
            SnapshotBindingError, match="native_fill_unit_original_record_reference_mismatch"
        ):
            await append_native_position_from_receipts_v3(
                repository, changed, native_cash_identity(98001), scenario.source.policy
            )
        assert await _facts(repository) == before
        replay = await append_native_position_from_receipts_v3(
            repository, original, native_cash_identity(98002), scenario.source.policy
        )
        assert not replay.created and replay.public_id == recorded.position.public_id
        assert await _facts(repository) == before


async def test_actual_v2_history_remains_readable_without_v3_promotion_or_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain explicit V2 history without inventing the missing durable V3 admission envelope.

    Given: The SDK-backed V2 producer commits a real historical position and shared source claim.
    When: The strict V3 history and native notification writers receive that actual event UUID.
    Then: Both refuse promotion while the original V2 bytes and explicit reader remain unchanged.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        result = await append_native_position_from_receipts_v2(
            repository, scenario.request, native_cash_identity(98011), scenario.source.policy
        )
        before = await _facts(repository)
        async with repository.session() as session:
            original = await get_bound_native_position_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
            assert original.payload.schema_version == 2
            with pytest.raises(
                NativeFinancialBindingError,
                match="native_position_durable_admission_history_unproven",
            ):
                await get_bound_native_position_family(session, result.public_id, datetime.now(UTC))
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(
                session, scenario.request.fill.wallet_public_id
            )
            with pytest.raises(
                NativeFinancialBindingError,
                match="native_position_durable_admission_history_unproven",
            ):
                await append_bound_native_position_publication_v2(
                    session, result.public_id, admission_stamp(98012, datetime.now(UTC))
                )
            await session.rollback()
        assert await _facts(repository) == before
        async with repository.session() as session:
            rebound = await get_bound_native_position_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        assert rebound.payload == original.payload
        assert rebound.row.canonical_payload == original.row.canonical_payload
        assert rebound.row.content_digest == original.row.content_digest
