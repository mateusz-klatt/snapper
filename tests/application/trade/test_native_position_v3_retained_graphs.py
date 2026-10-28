"""Refuse damaged retained graphs and preserve genuine native position successors."""

import json
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Literal

import pytest
from pydantic import TypeAdapter
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.native_financial_publication_store_v2 as publication_store
import snapper.data.native_position_admission_binding_v3 as admission_binding
import snapper.data.native_position_family_history as family_history
import snapper.data.native_position_store_v3 as position_store
import tests.helpers.native_position_recording_v3 as recording_helper
from snapper.application.trade.native_position_recording_v3 import NativePositionRecordingRequestV3
from snapper.application.trade.native_position_recording_v3 import RecordedNativePositionV3
from snapper.application.trade.native_position_recording_v3 import record_native_position_v3
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.json_types import JsonObject
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.contract_accounting_triggers import install_contract_accounting_table_triggers
from snapper.data.execution_account_authority_store import get_bound_account_authority_reference
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.financial_publication_history import get_bound_versioned_financial_publication
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.financial_publication_triggers import install_financial_publication_triggers
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import SubmissionUnitAdmission
from snapper.data.native_financial_event_binding_v2 import NativeFinancialEventMaterialV2
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import admitted_position_request_v3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_publication_store_v2 import (
    append_bound_native_position_publication_v2,
)
from snapper.data.native_financial_publication_store_v2 import (
    get_bound_native_position_publication_v2,
)
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_position_admission_binding_v3 import NativePositionSourceRequestV3
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_fill_binding_v2 import bind_original_position_fill_v2
from snapper.data.native_position_history_v2 import BoundNativePositionEventV2
from snapper.data.native_position_history_v2 import get_bound_native_position_event_v2
from snapper.data.native_position_history_v2 import load_native_position_history_v2
from snapper.data.native_position_source_v2 import BoundNativePositionSourceV2
from snapper.data.native_position_source_v2 import NativePositionSourceRequestV2
from snapper.data.native_position_store_v2 import append_native_position_from_receipts_v2
from snapper.data.native_position_store_v3 import append_native_position_from_receipts_v3
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_native_position_recording_v3 import _facts
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2
from tests.helpers.native_fill_unit_admission import NativeFillSide
from tests.helpers.native_position_accounting_v2 import native_position_scenario_v2
from tests.helpers.native_position_recording_v3 import NativePositionRecordingScenarioV3
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp

pytestmark = pytest.mark.asyncio
_JSON: TypeAdapter[JsonObject] = TypeAdapter(JsonObject)


def _install_payload_guard(
    connection: Connection,
    table: Literal["contract_accounting_events", "financial_publication_intents"],
) -> None:
    """Restore the genuine immutable guard for one explicitly selected damaged-restore witness."""
    if table == "contract_accounting_events":
        install_contract_accounting_table_triggers(connection, table)
    else:
        install_financial_publication_triggers(connection, table)


async def _restore_payload(
    repository: SQLAlchemyRepository,
    table: Literal["contract_accounting_events", "financial_publication_intents"],
    public_id: str,
    payload: str,
) -> None:
    """Change only one retained JSON copy under a scoped restore and reinstall original guards."""
    async with repository.engine.begin() as connection:
        await connection.execute(text(f"DROP TRIGGER {table}_reject_update"))
        await connection.execute(
            text(f"UPDATE {table} SET canonical_payload = :payload WHERE public_id = :public_id"),
            {"payload": payload, "public_id": public_id},
        )
        await connection.run_sync(_install_payload_guard, table)


async def test_missing_actual_rows_and_unowned_publication_transaction_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing retained identities never become financial facts or an implicit transaction.

    Given: Real native SDK source infrastructure exists without a financial event or notification.
    When: Exact absent event/notification UUIDs and a caller without a transaction are supplied.
    Then: Each real reader refuses its missing identity and the writer refuses before opening a transaction.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            assert not session.in_transaction()
            with pytest.raises(
                RuntimeError, match="native_publication_requires_caller_transaction"
            ):
                await append_bound_native_position_publication_v2(
                    session, admission_id(99001), admission_stamp(99002, datetime.now(UTC))
                )
            assert not session.in_transaction()
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_actual_event_missing"
            ):
                await get_native_financial_event_family(
                    session, admission_id(99001), datetime.now(UTC)
                )
            with pytest.raises(
                NativeFinancialBindingError, match="native_publication_actual_intent_missing"
            ):
                await get_bound_native_position_publication_v2(session, admission_id(99001))
            with pytest.raises(FinancialPublicationError, match="financial_publication_missing"):
                await get_bound_versioned_financial_publication(session, admission_id(99001))
        assert await _facts(repository) == before


async def test_retained_version_aliases_refuse_before_any_reader_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual retained row cannot select another financial codec through a version alias.

    Given: The real SDK recorder commits a source-bound V3 event and its native notification.
    When: Isolated restores change only their JSON discriminators to a boolean or string alias.
    Then: Real declared-family readers refuse and exact restored original bytes remain readable.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            event = (
                await session.scalars(
                    select(ContractAccountingEvent).where(
                        ContractAccountingEvent.public_id == recorded.position.public_id
                    )
                )
            ).one()
            intent = (
                await session.scalars(
                    select(FinancialPublicationIntent).where(
                        FinancialPublicationIntent.public_id == recorded.publication.public_id
                    )
                )
            ).one()
            original_event, original_intent = event.canonical_payload, intent.canonical_payload
        changed_event = _JSON.validate_json(original_event)
        changed_event["schema_version"] = True
        await _restore_payload(
            repository,
            "contract_accounting_events",
            recorded.position.public_id,
            json.dumps(changed_event),
        )
        async with repository.session() as session:
            with pytest.raises(
                ValidationError, match="Native financial version requires an integer"
            ):
                await get_native_financial_event_family(
                    session, recorded.position.public_id, datetime.now(UTC)
                )
        await _restore_payload(
            repository, "contract_accounting_events", recorded.position.public_id, original_event
        )
        changed_intent = _JSON.validate_json(original_intent)
        changed_intent["schema_version"] = "2"
        await _restore_payload(
            repository,
            "financial_publication_intents",
            recorded.publication.public_id,
            json.dumps(changed_intent),
        )
        async with repository.session() as session:
            with pytest.raises(
                ValidationError, match="Financial publication version requires an integer"
            ):
                await get_bound_versioned_financial_publication(
                    session, recorded.publication.public_id
                )
        await _restore_payload(
            repository,
            "financial_publication_intents",
            recorded.publication.public_id,
            original_intent,
        )
        async with repository.session() as session:
            rebound = await get_bound_versioned_financial_publication(
                session, recorded.publication.public_id
            )
        assert rebound.content_digest == recorded.publication.content_digest
        assert await _facts(repository) == before


async def test_missing_correlated_order_refuses_before_native_target_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual admission and fill do not manufacture the missing explicitly requested order.

    Given: Genuine retained SDK fill/log sources and an actual durable account-bound admission.
    When: The recorder receives another canonical order UUID that has no actual matching row.
    Then: The real correlation selector refuses and every original source stays unchanged.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        before = await _facts(repository)
        request = scenario.request.model_copy(update={"order_public_id": admission_id(99003)})
        with pytest.raises(
            NativeFinancialBindingError,
            match="native_recording_actual_order_or_admitted_command_missing",
        ):
            await record_native_position_v3(
                repository, request, scenario.identity, scenario.source.policy
            )
        assert await _facts(repository) == before


def _second_fill_wire(original: bytes) -> bytes:
    """Complete the same actual two-unit order with another distinct one-unit source fill."""
    payload = _JSON.validate_json(original)
    entries = payload["fills"]
    assert isinstance(entries, list) and len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, dict)
    entry["fill_id"] = "native-fill-unit-execution-2"
    entry["seq"] = 2
    entry["remaining_order_qty"] = 0
    return json.dumps(payload, separators=(",", ":")).encode()


def _second_position_wire(original: bytes) -> bytes:
    """Retain a genuine later log whose before/after quantity conserves the first recorded fill."""
    payload = _JSON.validate_json(original)
    entry = payload["new_entry"]
    assert isinstance(entry, dict)
    entry.update(
        id=32,
        booking_uid="native-recording-v3-position-booking-2",
        old_balance=1,
        new_balance=2,
        old_average_entry_price=100,
    )
    return json.dumps(payload, separators=(",", ":")).encode()


async def _next_v3_recording(
    scenario: NativePositionRecordingScenarioV3, monkeypatch: pytest.MonkeyPatch
) -> tuple[NativePositionRecordingRequestV3, RecordedNativePositionV3]:
    """Use the same real SDK/issuance path with distinct source wire bytes and trace identities."""
    source = scenario.source
    fill_reference, _authority = await fresh_native_cash_authority_v2(source, 17301)
    actual_wire, actual_identity = (
        recording_helper.native_fill_wire,
        recording_helper.native_cash_identity,
    )

    def next_wire(side: NativeFillSide) -> bytes:
        """Derive the second actual provider frame without replacing authentication or source issuance."""
        return _second_fill_wire(actual_wire(side))

    def next_identity(number: int) -> ExecutionEvidenceIdentity:
        """Allocate distinct ordinary fixture provenance for the second genuine raw and sidecar capture."""
        return actual_identity(number + 10000)

    with monkeypatch.context() as scoped:
        scoped.setattr(recording_helper, "native_fill_wire", next_wire)
        scoped.setattr(recording_helper, "native_cash_identity", next_identity)
        captured = await recording_helper._receive_recording_fill(
            source, monkeypatch, "buy", "partial"
        )
    fill_request = OriginalPositionFillRequestV2(
        wallet_public_id=scenario.request.wallet_public_id,
        raw_receipt_public_id=captured.raw.public_id,
        authority_reference=fill_reference,
        ownership_reference=captured.ownership.reference,
    )
    async with source.repository.session() as session:
        bound = await bind_original_position_fill_v2(session, fill_request, datetime.now(UTC))
    position = await receive_native_cash_v2(
        source,
        monkeypatch,
        _second_position_wire(recording_helper.native_recording_position_wire_v3(bound)),
        17601,
    )
    reference, _authority = await fresh_native_cash_authority_v2(source, 17701)
    request = NativePositionRecordingRequestV3(
        **fill_request.model_dump(mode="python"),
        order_public_id=scenario.request.order_public_id,
        submission_admission=scenario.request.submission_admission,
        position_receipt_public_id=position.raw.public_id,
        position_authority_reference=reference,
    )
    result = await record_native_position_v3(
        source.repository, request, native_cash_identity(17801), source.policy
    )
    assert result.created
    return request, result


@pytest.mark.parametrize("handoff", ["predecessor_admission", "selected_result"])
async def test_real_second_sdk_fill_reuses_anchor_and_refuses_incomplete_history_handoffs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    handoff: Literal["predecessor_admission", "selected_result"],
) -> None:
    """Prove actual successor conservation before testing lost admission or incomplete read results.

    Given: Two real SDK fills complete the admitted two-unit order and share one genuine position anchor.
    When: Delegating reader wrappers either drop a predecessor's typed admission witness or omit the selected row.
    Then: Strict history refuses each incomplete handoff and the untouched actual graph still replays.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        first = await record_native_position_scenario_v3(scenario)
        _request, second = await _next_v3_recording(scenario, monkeypatch)
        repository = scenario.source.repository
        assert second.position.anchor_public_id == first.position.anchor_public_id
        assert second.position.ledger_sequence == first.position.ledger_sequence + 1
        assert second.position.observed.quantity == 2
        before = await _facts(repository)
        actual_material = family_history.get_native_financial_event_family
        actual_history = family_history.load_native_position_history_v2

        async def lose_predecessor_witness(
            session: AsyncSession, public_id: str, known_at: datetime
        ) -> NativeFinancialEventMaterialV2:
            """Delegate actual SQL/source binding, then remove only the typed predecessor admission witness."""
            material = await actual_material(session, public_id, known_at)
            if public_id == first.position.public_id:
                return NativeFinancialEventMaterialV2(
                    material.row, material.payload, material.source
                )
            return material

        async def omit_selected_result(
            session: AsyncSession, scope: ExecutionAccountScope, instrument: str, known_at: datetime
        ) -> tuple[BoundNativePositionEventV2, ...]:
            """Delegate the complete actual history reader before simulating a missing selected result."""
            events = await actual_history(session, scope, instrument, known_at)
            assert len(events) == 2
            return tuple(
                event for event in events if event.row.public_id != second.position.public_id
            )

        with monkeypatch.context() as scoped:
            if handoff == "predecessor_admission":
                scoped.setattr(
                    family_history, "get_native_financial_event_family", lose_predecessor_witness
                )
                expected_error = "native_position_prior_admission_migration_required"
            else:
                scoped.setattr(
                    family_history, "load_native_position_history_v2", omit_selected_result
                )
                expected_error = "native_position_event_not_known_at_cutoff"
            async with repository.session() as session:
                with pytest.raises(NativeFinancialBindingError, match=expected_error):
                    await family_history.get_bound_native_position_family(
                        session, second.position.public_id, datetime.now(UTC)
                    )
        async with repository.session() as session:
            rebound = await family_history.get_bound_native_position_family(
                session, second.position.public_id, datetime.now(UTC)
            )
        assert rebound.row.public_id == second.position.public_id
        assert await _facts(repository) == before


async def test_legacy_v2_recording_cannot_supply_a_missing_original_v3_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep genuine older financial facts readable while refusing automatic V3 promotion at both writers.

    Given: The actual original V2 SDK fixture commits a position and its real shared source claim.
    When: The new recorder or admitted-history validator sees that same retained source without an outer V3 witness.
    Then: Both require migration without changing financial rows or inventing admission history.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        result = await append_native_position_from_receipts_v2(
            repository, scenario.request, native_cash_identity(99010), scenario.source.policy
        )
        before = await _facts(repository)
        async with repository.session() as session:
            event = await get_bound_native_position_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
            execution = (
                await session.scalars(
                    select(Execution).where(
                        Execution.public_id == event.payload.operands.execution_public_id
                    )
                )
            ).one()
            admission_row = (
                await session.scalars(
                    select(SubmissionUnitAdmission).where(
                        SubmissionUnitAdmission.command_public_id == admission_id(5000)
                    )
                )
            ).one()
            admission = await get_bound_account_submission_admission(
                session, admission_row.public_id
            )
            assert isinstance(execution.order_public_id, str)
        request = NativePositionRecordingRequestV3(
            **scenario.request.fill.model_dump(mode="python"),
            order_public_id=execution.order_public_id,
            submission_admission=NativeUnitRecordReference(
                public_id=admission.public_id,
                content_digest=admission.content_digest,
                recorded_at=admission.recorded_at,
            ),
            position_receipt_public_id=scenario.request.position_receipt_public_id,
            position_authority_reference=scenario.request.position_authority_reference,
        )
        with pytest.raises(
            NativeFinancialBindingError, match="native_recording_prior_admission_migration_required"
        ):
            await record_native_position_v3(
                repository, request, native_cash_identity(99011), scenario.source.policy
            )
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(session, request.wallet_public_id)
            history = await load_native_position_history_v2(
                session,
                event.payload.operands.scope,
                event.payload.operands.instrument_public_id,
                datetime.now(UTC),
            )
            with pytest.raises(
                NativeFinancialBindingError,
                match="native_position_prior_admission_migration_required",
            ):
                await position_store._admitted_position_history(session, history, datetime.now(UTC))
        assert await _facts(repository) == before


async def test_actual_reference_shapes_and_legacy_authority_handoff_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid legacy source family cannot replace the actual native structural profile or unit copy.

    Given: One genuine V3 SDK graph and the fixture's independently bound legacy route authority exist.
    When: A wrong unit copy or a delegating binder returns that actual legacy authority in the native log slot.
    Then: The public request and financial writer refuse before any target or original reference changes.
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
            legacy_row = (
                await session.scalars(
                    select(ExecutionRouteAuthority).where(
                        ExecutionRouteAuthority.public_id == admission_id(93)
                    )
                )
            ).one()
            legacy_reference = AccountAuthorityReference(
                family="legacy_route_v1",
                public_id=legacy_row.public_id,
                content_digest=legacy_row.content_digest,
                recorded_at=legacy_row.timestamp,
            )
            legacy = await get_bound_account_authority_reference(session, legacy_reference)
        with pytest.raises(ValidationError, match="requires a native later-log authority"):
            NativePositionRecordingRequestV3.model_validate(
                {
                    **scenario.request.model_dump(mode="python"),
                    "position_authority_reference": legacy_reference,
                }
            )
        request = admitted_position_request_v3(material.envelope)
        with pytest.raises(ValidationError, match="names another original unit"):
            NativePositionSourceRequestV3.model_validate(
                {
                    **request.model_dump(mode="python"),
                    "unit_snapshot_public_id": admission_id(99012),
                }
            )
        actual_bind = admission_binding.bind_native_position_receipt_v2

        async def wrong_authority_handoff(
            session: AsyncSession, source_request: NativePositionSourceRequestV2, known_at: datetime
        ) -> BoundNativePositionSourceV2:
            """Delegate the actual source binder before substituting a real but wrong-family authority."""
            bound = await actual_bind(session, source_request, known_at)
            return replace(
                bound, financial_source=replace(bound.financial_source, authority=legacy)
            )

        with monkeypatch.context() as scoped:
            scoped.setattr(
                admission_binding, "bind_native_position_receipt_v2", wrong_authority_handoff
            )
            with pytest.raises(
                NativeFinancialBindingError,
                match="native_position_original_structural_profile_unproven",
            ):
                await append_native_position_from_receipts_v3(
                    repository, request, native_cash_identity(99013), scenario.source.policy
                )
        assert await _facts(repository) == before


async def test_native_publication_refuses_an_actual_command_unit_handoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A delegating internal reader cannot replace the actual fill target with a real command target.

    Given: One actual V3 position and its independently readable original command snapshot exist.
    When: A delegating position reader hands that genuine command unit to the notification boundary.
    Then: The native publisher refuses the wrong target family without inserting or changing any intent.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            admission = await get_bound_account_submission_admission(
                session, scenario.request.submission_admission.public_id
            )
            command_unit = await get_bound_execution_unit_snapshot(
                session, admission.payload.units.command_snapshot_public_id
            )
            assert isinstance(command_unit.target, SubmissionSnapshotTarget)
        actual_position = publication_store.get_bound_native_position_family

        async def command_unit_handoff(
            session: AsyncSession, public_id: str, known_at: datetime
        ) -> BoundNativePositionEventV2:
            """Delegate complete SQL/source history before simulating the wrong internal target handoff."""
            bound = await actual_position(session, public_id, known_at)
            operands = bound.payload.operands.model_copy(update={"unit": command_unit})
            return replace(bound, payload=bound.payload.model_copy(update={"operands": operands}))

        with monkeypatch.context() as scoped:
            scoped.setattr(
                publication_store, "get_bound_native_position_family", command_unit_handoff
            )
            async with repository.session() as session:
                await repository._begin_execution_insert_transaction(session)
                await repository._acquire_execution_fence_lock(
                    session, scenario.request.wallet_public_id
                )
                with pytest.raises(
                    NativeFinancialBindingError,
                    match="native_publication_actual_fill_target_required",
                ):
                    await append_bound_native_position_publication_v2(
                        session,
                        recorded.position.public_id,
                        admission_stamp(99014, datetime.now(UTC)),
                    )
        assert await _facts(repository) == before
