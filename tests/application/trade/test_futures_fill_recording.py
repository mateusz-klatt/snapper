"""Real atomic financial recording and publication recovery without a network or financial order."""

import base64
from collections.abc import AsyncIterator
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.financial_publication as publisher
import snapper.application.trade.futures_fill_recording as recording
from snapper.application.trade.futures_fill_recording import FuturesPositionRecordingRequest
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_publication import ContractPositionPublication
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.contract_position_ledger_store import ContractPositionAccountingError
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.financial_publication_delivery import load_pending_financial_publications
from snapper.data.financial_publication_delivery import record_financial_publication_delivery
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.financial_publication_store import append_bound_financial_publication
from snapper.data.financial_publication_store import get_bound_financial_publication
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import Base
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.application.trade.test_unit_snapshot_fill import _VENUE
from tests.application.trade.test_unit_snapshot_fill import _observation
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import create_position_units
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp

_RECORDED = _KNOWN + timedelta(seconds=2)


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ContractPositionScenario]:
    """Seed real immutable source, order, admission and account evidence in a private SQLite file."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'financial.db'}")
    await repository.create_all()
    try:
        yield await seed_contract_position_scenario(repository, monkeypatch)
    finally:
        await repository.engine.dispose()


def _request(scenario: ContractPositionScenario) -> FuturesPositionRecordingRequest:
    """Select actual separately committed raw source identities, never fabricated financial targets."""
    return FuturesPositionRecordingRequest(
        **scenario.fill.source_request.model_dump(mode="python"),
        position_receipt_public_id=scenario.position_receipt_public_id,
    )


async def _count(repository: SQLAlchemyRepository, model: type[Base]) -> int:
    """Inspect committed row counts from a session independent of the recording transaction."""
    async with repository.session() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


async def _record(scenario: ContractPositionScenario) -> recording.RecordedFuturesPosition:
    """Invoke the actual atomic orchestration seam with source inputs and a local transaction clock."""
    return await recording.record_futures_position(
        scenario.fill.admission.repository, _request(scenario), admission_stamp(600, _RECORDED)
    )


async def _retain_fill_wire(
    scenario: ContractPositionScenario, wire: bytes
) -> FuturesPositionRecordingRequest:
    """Retain a distinct genuine wire fixture before asking the actual financial writer to interpret it."""
    raw = _observation(scenario.fill.admission)
    variant = RawAccountingObservation.model_validate(
        {**raw.model_dump(mode="python"), "material": base64.b64encode(wire).decode("ascii")}
    )
    retained = await capture_accounting_observation(
        scenario.fill.admission.repository,
        variant,
        AccountingWriteProvenance.model_validate(admission_stamp(630, _RECORDED).model_dump()),
    )
    return FuturesPositionRecordingRequest.model_validate(
        {**_request(scenario).model_dump(), "raw_receipt_public_id": retained.public_id}
    )


@pytest.mark.parametrize("price", [b'"1e9999"', b'"1e-9999"'])
async def test_unrepresentable_legacy_mirror_does_not_change_the_exact_source_fact(
    scenario: ContractPositionScenario, price: bytes
) -> None:
    """Raw evidence survives refusal.

    Given: an exact source operand outside legacy range,
    When: recording,
    Then: raw evidence survives refusal.
    """
    repository = scenario.fill.admission.repository
    wire = base64.b64decode(_observation(scenario.fill.admission).material)
    request = await _retain_fill_wire(scenario, wire.replace(b'"price": 100', b'"price": ' + price))
    with pytest.raises(SnapshotBindingError, match="financial_float_projection_unrepresentable"):
        await recording.record_futures_position(
            repository, request, admission_stamp(631, _RECORDED)
        )
    assert await _count(repository, Execution) == 0
    assert await _count(repository, AccountingRawReceipt) == 3


async def test_order_identity_not_yet_known_cannot_be_invented_from_the_source_fill(
    scenario: ContractPositionScenario,
) -> None:
    """Financial writes wait.

    Given: a fill before ACK,
    When: its actual order identity is still unknown,
    Then: financial writes wait.
    """
    with pytest.raises(SnapshotBindingError, match="financial_actual_order_correlation_unproven"):
        await recording.record_futures_position(
            scenario.fill.admission.repository,
            _request(scenario),
            admission_stamp(632, _KNOWN - timedelta(milliseconds=1250)),
        )
    assert await _count(scenario.fill.admission.repository, Execution) == 0


async def test_contradictory_provider_client_id_cannot_borrow_an_existing_order(
    scenario: ContractPositionScenario,
) -> None:
    """Correlation refuses.

    Given: matching venue order ID but a different reported client ID,
    When: recording,
    Then: correlation refuses.
    """
    wire = base64.b64decode(_observation(scenario.fill.admission).material)
    request = await _retain_fill_wire(scenario, wire.replace(b'"client-1"', b'"another-client"'))
    with pytest.raises(SnapshotBindingError, match="financial_actual_client_order_contradiction"):
        await recording.record_futures_position(
            scenario.fill.admission.repository, request, admission_stamp(633, _RECORDED)
        )


async def test_closed_command_without_a_successor_cannot_supply_current_correlation(
    scenario: ContractPositionScenario,
) -> None:
    """No request is fabricated.

    Given: durable order history but no command at the knowledge horizon,
    When: recording,
    Then: no request is fabricated.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await session.execute(
            update(TradeCommand)
            .where(TradeCommand.public_id == admission_id(5))
            .values(known_to=_KNOWN)
        )
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="financial_actual_command_correlation_unproven"):
        await _record(scenario)
    assert await _count(repository, Execution) == 0


async def test_actual_financial_rows_and_publication_intent_commit_together(
    scenario: ContractPositionScenario,
) -> None:
    """All facts share one durable outcome.

    Given: paired retained sources,
    When: recording commits,
    Then: all facts share one durable outcome.
    """
    repository = scenario.fill.admission.repository
    result = await _record(scenario)
    assert result.created
    assert result.publication.created
    assert result.publication.payload.execution_public_id == result.execution_public_id
    assert result.publication.payload.unit_snapshot_public_id == result.unit_snapshot_public_id
    assert result.publication.payload.executed_at == _VENUE
    assert result.publication.payload.execution_known_at == _RECORDED
    for model in (Execution, VenueEvent, ContractAccountingEvent, FinancialPublicationIntent):
        assert await _count(repository, model) == 1
    assert await _count(repository, AccountingScopeHold) == 2
    assert await _count(repository, AccountingHoldResolution) == 0
    assert await _count(repository, FinancialPublicationDelivery) == 0
    pending = await load_pending_financial_publications(
        repository, scenario.fill.admission.authority.scope
    )
    assert [item.public_id for item in pending] == [result.publication.public_id]


async def test_uncommitted_financial_intent_is_invisible_to_the_recovery_reader(
    scenario: ContractPositionScenario,
) -> None:
    """Nothing publishes.

    Given: flushed financial facts,
    When: a separate reader polls before commit,
    Then: nothing publishes.
    """
    repository = scenario.fill.admission.repository
    scope = scenario.fill.admission.authority.scope
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        _, position = await stage_contract_position(session, scenario)
        intent = await append_bound_financial_publication(
            session, position.public_id, admission_stamp(620, _RECORDED)
        )
        assert await load_pending_financial_publications(repository, scope) == ()
        await session.commit()
    committed = await load_pending_financial_publications(repository, scope)
    assert [item.public_id for item in committed] == [intent.public_id]


async def test_same_source_retry_reuses_original_targets_and_notification(
    scenario: ContractPositionScenario,
) -> None:
    """No duplicate execution exists.

    Given: an already committed source fact,
    When: a caller retries,
    Then: no duplicate execution exists.
    """
    first = await _record(scenario)
    second = await recording.record_futures_position(
        scenario.fill.admission.repository,
        _request(scenario),
        admission_stamp(601, _RECORDED + timedelta(seconds=1)),
    )
    assert not second.created
    assert not second.publication.created
    assert second.execution_public_id == first.execution_public_id
    assert second.publication.public_id == first.publication.public_id
    assert second.publication.payload == first.publication.payload
    assert await _count(scenario.fill.admission.repository, Execution) == 1


async def test_existing_execution_without_accounting_cannot_be_rebooked_as_a_new_fill(
    scenario: ContractPositionScenario,
) -> None:
    """Missing linkage is held.

    Given: a legacy or torn financial record,
    When: source retries,
    Then: missing linkage is held.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await create_position_units(session, scenario)
        await session.commit()
    with pytest.raises(SnapshotBindingError, match="existing_execution_without_bound_accounting"):
        await _record(scenario)
    assert await _count(repository, Execution) == 1
    assert await _count(repository, ContractAccountingEvent) == 0
    assert await _count(repository, FinancialPublicationIntent) == 0
    assert await _count(repository, AccountingHoldResolution) == 0


async def test_duplicate_fill_does_not_hide_a_missing_requested_position_receipt(
    scenario: ContractPositionScenario,
) -> None:
    """No false pair is accepted.

    Given: a valid prior fill and an invented new receipt UUID,
    When: retried,
    Then: no false pair is accepted.
    """
    repository = scenario.fill.admission.repository
    await _record(scenario)
    request = FuturesPositionRecordingRequest.model_validate(
        {**_request(scenario).model_dump(), "position_receipt_public_id": admission_id(999)}
    )
    with pytest.raises(ValueError, match="missing"):
        await recording.record_futures_position(
            repository, request, admission_stamp(621, _RECORDED + timedelta(seconds=1))
        )
    assert await _count(repository, Execution) == 1
    assert await _count(repository, ContractAccountingEvent) == 1
    assert await _count(repository, FinancialPublicationIntent) == 1


async def test_changed_source_with_same_venue_fill_id_stays_held_without_rebooking(
    scenario: ContractPositionScenario,
) -> None:
    """Original money facts survive.

    Given: contradictory redelivery,
    When: the source identity repeats,
    Then: original money facts survive.
    """
    repository = scenario.fill.admission.repository
    original = await _record(scenario)
    raw = _observation(scenario.fill.admission)
    wire = base64.b64decode(raw.material).replace(b'"price": 100', b'"price": 101')
    variant = RawAccountingObservation.model_validate(
        {**raw.model_dump(mode="python"), "material": base64.b64encode(wire).decode("ascii")}
    )
    retained = await capture_accounting_observation(
        repository,
        variant,
        AccountingWriteProvenance.model_validate(admission_stamp(602, _RECORDED).model_dump()),
    )
    request = FuturesPositionRecordingRequest.model_validate(
        {**_request(scenario).model_dump(), "raw_receipt_public_id": retained.public_id}
    )
    with pytest.raises(SnapshotBindingError, match="financial_source_redelivery_conflict"):
        await recording.record_futures_position(
            repository, request, admission_stamp(603, _RECORDED + timedelta(seconds=1))
        )
    assert await _count(repository, Execution) == 1
    assert await _count(repository, AccountingScopeHold) == 3
    pending = await load_pending_financial_publications(
        repository, scenario.fill.admission.authority.scope
    )
    assert [item.public_id for item in pending] == [original.publication.public_id]


async def test_native_position_contradiction_rolls_back_every_financial_target(
    scenario: ContractPositionScenario,
) -> None:
    """Only raw evidence remains.

    Given: a source quantity contradiction,
    When: accounting refuses,
    Then: only raw evidence remains.
    """
    repository = scenario.fill.admission.repository
    bad_id = await capture_position_log(scenario.fill, 610, position_log_entry(new_balance=3))
    request = FuturesPositionRecordingRequest.model_validate(
        {**_request(scenario).model_dump(), "position_receipt_public_id": bad_id}
    )
    with pytest.raises((ContractPositionAccountingError, ValueError), match="quantity|Quantity"):
        await recording.record_futures_position(
            repository, request, admission_stamp(611, _RECORDED)
        )
    for model in (Execution, VenueEvent, ContractAccountingEvent, FinancialPublicationIntent):
        assert await _count(repository, model) == 0
    assert await _count(repository, AccountingRawReceipt) == 3
    assert await _count(repository, AccountingScopeHold) == 3


async def test_successful_publication_is_marked_only_after_callback_returns(
    scenario: ContractPositionScenario,
) -> None:
    """One local receipt is durable.

    Given: a committed notification,
    When: its publisher succeeds,
    Then: one local receipt is durable.
    """
    repository = scenario.fill.admission.repository
    result = await _record(scenario)
    messages: list[str] = []

    async def publish(topic: str, public_id: str, payload: ContractPositionPublication) -> None:
        """Observe committed facts before recording the actual successful local send."""
        assert topic.startswith("accounting.contract_position.")
        assert payload.execution_public_id == result.execution_public_id
        assert await _count(repository, FinancialPublicationDelivery) == 0
        messages.append(public_id)

    scope = scenario.fill.admission.authority.scope
    assert (
        await publisher.publish_pending_financial_notifications(
            repository, scope, publish, SequenceTracker()
        )
        == 1
    )
    assert (
        await publisher.publish_pending_financial_notifications(
            repository, scope, publish, SequenceTracker()
        )
        == 0
    )
    assert messages == [result.publication.public_id]
    assert await _count(repository, FinancialPublicationDelivery) == 1
    assert await _count(repository, AccountingHoldResolution) == 0


async def test_failed_publication_remains_pending_with_its_original_message_identity(
    scenario: ContractPositionScenario,
) -> None:
    """The same committed message is recovered.

    Given: unavailable transport,
    When: delivery retries,
    Then: the same committed message is recovered.
    """
    repository = scenario.fill.admission.repository
    result = await _record(scenario)
    messages: list[str] = []

    async def publish(topic: str, public_id: str, payload: ContractPositionPublication) -> None:
        """Fail exactly the first actual transport boundary without modifying database state."""
        messages.append(public_id)
        if len(messages) == 1:
            raise ConnectionError("isolated transport unavailable")

    scope = scenario.fill.admission.authority.scope
    with pytest.raises(ConnectionError, match="transport unavailable"):
        await publisher.publish_pending_financial_notifications(
            repository, scope, publish, SequenceTracker()
        )
    assert await _count(repository, FinancialPublicationDelivery) == 0
    assert (
        await publisher.publish_pending_financial_notifications(
            repository, scope, publish, SequenceTracker()
        )
        == 1
    )
    assert messages == [result.publication.public_id, result.publication.public_id]


@pytest.mark.parametrize("after_commit", [False, True])
async def test_uncertain_delivery_receipt_preserves_at_least_once_recovery(
    scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch, after_commit: bool
) -> None:
    """Only absent receipts trigger resend.

    Given: a send/receipt crash boundary,
    When: recovery runs,
    Then: only absent receipts trigger resend.
    """
    repository = scenario.fill.admission.repository
    result = await _record(scenario)
    messages: list[str] = []
    attempts = 0

    async def publish(topic: str, public_id: str, payload: ContractPositionPublication) -> None:
        """Represent an accepted local transport send without any downstream money projection."""
        messages.append(public_id)

    async def receipt(
        repo: AccountingRepository, identity: str, at: datetime, stamp: SnapshotWriteProvenance
    ) -> str:
        """Inject failure before a real commit or lose its response after the real commit."""
        nonlocal attempts
        attempts += 1
        if attempts == 1 and not after_commit:
            raise OperationalError("receipt", {}, RuntimeError("before commit"))
        retained = await record_financial_publication_delivery(repo, identity, at, stamp)
        if attempts == 1:
            raise OperationalError("receipt", {}, RuntimeError("commit response lost"))
        return retained

    monkeypatch.setattr(publisher, "record_financial_publication_delivery", receipt)
    scope = scenario.fill.admission.authority.scope
    with pytest.raises(OperationalError):
        await publisher.publish_pending_financial_notifications(
            repository, scope, publish, SequenceTracker()
        )
    assert await publisher.publish_pending_financial_notifications(
        repository, scope, publish, SequenceTracker()
    ) == (0 if after_commit else 1)
    assert messages == [result.publication.public_id] * (1 if after_commit else 2)
    assert await _count(repository, FinancialPublicationDelivery) == 1


async def test_intent_failure_rolls_back_execution_unit_and_position_together(
    scenario: ContractPositionScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No earlier financial flush commits.

    Given: an outbox write failure,
    When: recording aborts,
    Then: no earlier financial flush commits.
    """
    repository = scenario.fill.admission.repository

    async def fail_intent(
        session: AsyncSession, position_event_public_id: str, provenance: SnapshotWriteProvenance
    ) -> BoundFinancialPublication:
        """Interrupt the final write after actual execution, unit and position flushes."""
        raise OperationalError("publication intent", {}, RuntimeError("storage unavailable"))

    monkeypatch.setattr(recording, "append_bound_financial_publication", fail_intent)
    with pytest.raises(OperationalError):
        await _record(scenario)
    for model in (Execution, VenueEvent, ContractAccountingEvent, FinancialPublicationIntent):
        assert await _count(repository, model) == 0
    assert await _count(repository, AccountingRawReceipt) == 2
    assert await _count(repository, AccountingScopeHold) == 2


async def test_local_receipt_retry_returns_original_acknowledgement_without_duplicate_history(
    scenario: ContractPositionScenario,
) -> None:
    """Identity is stable.

    Given: a committed local send receipt,
    When: its acknowledgement is retried,
    Then: identity is stable.
    """
    repository = scenario.fill.admission.repository
    recorded = await _record(scenario)
    at = _RECORDED + timedelta(seconds=1)
    first = await record_financial_publication_delivery(
        repository, recorded.publication.public_id, at, admission_stamp(640, at)
    )
    second = await record_financial_publication_delivery(
        repository,
        recorded.publication.public_id,
        at + timedelta(seconds=1),
        admission_stamp(641, at + timedelta(seconds=1)),
    )
    assert first == second
    assert await _count(repository, FinancialPublicationDelivery) == 1


async def test_corrupt_delivery_digest_cannot_suppress_the_actual_pending_intent(
    scenario: ContractPositionScenario,
) -> None:
    """No false delivery is accepted.

    Given: an imported receipt with the wrong digest,
    When: read or retried,
    Then: no false delivery is accepted.
    """
    repository = scenario.fill.admission.repository
    recorded = await _record(scenario)
    scope = recorded.publication.payload.scope
    at = _RECORDED + timedelta(seconds=1)
    async with repository.session() as session:
        session.add(
            FinancialPublicationDelivery(
                **admission_stamp(642, at).model_dump(mode="python"),
                **scope.model_dump(mode="python"),
                publication_public_id=recorded.publication.public_id,
                publication_digest="f" * 64,
                delivery_semantics="publisher_returned",
                published_at=at,
            )
        )
        await session.commit()
    pending = await load_pending_financial_publications(repository, scope)
    assert [item.public_id for item in pending] == [recorded.publication.public_id]
    with pytest.raises(
        FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
    ):
        await record_financial_publication_delivery(
            repository, recorded.publication.public_id, at, admission_stamp(643, at)
        )


@pytest.mark.parametrize("offset", [-1, 2])
async def test_impossible_local_delivery_clock_cannot_create_a_receipt(
    scenario: ContractPositionScenario, offset: int
) -> None:
    """Chronology refuses.

    Given: a send before intent creation or after receipt recording,
    When: accepted,
    Then: chronology refuses.
    """
    repository = scenario.fill.admission.repository
    recorded = await _record(scenario)
    with pytest.raises(FinancialPublicationError, match="financial_delivery_clock_mismatch"):
        await record_financial_publication_delivery(
            repository,
            recorded.publication.public_id,
            _RECORDED + timedelta(seconds=offset),
            admission_stamp(644, _RECORDED + timedelta(seconds=1)),
        )
    assert await _count(repository, FinancialPublicationDelivery) == 0


@pytest.mark.parametrize("limit", [0, -1, 1001])
async def test_pending_publication_scan_has_an_explicit_bounded_page_size(
    scenario: ContractPositionScenario, limit: int
) -> None:
    """The caller must choose a valid page.

    Given: an unbounded or empty scan request,
    When: recovery starts,
    Then: the caller must choose a valid page.
    """
    with pytest.raises(ValueError, match="page limit"):
        await load_pending_financial_publications(
            scenario.fill.admission.repository, scenario.fill.admission.authority.scope, limit
        )


async def test_missing_notification_uuid_is_not_a_delivery_authority(
    scenario: ContractPositionScenario,
) -> None:
    """No notification is manufactured.

    Given: no actual publication,
    When: looked up by a caller UUID,
    Then: no notification is manufactured.
    """
    async with scenario.fill.admission.repository.session() as session:
        with pytest.raises(FinancialPublicationError, match="financial_publication_missing"):
            await get_bound_financial_publication(session, admission_id(999))


async def test_publication_writer_requires_an_existing_financial_transaction(
    scenario: ContractPositionScenario,
) -> None:
    """No partial write starts.

    Given: a genuine position but no outer transaction,
    When: an intent is appended,
    Then: no partial write starts.
    """
    repository = scenario.fill.admission.repository
    recorded = await _record(scenario)
    async with repository.session() as session:
        assert not session.in_transaction()
        with pytest.raises(RuntimeError, match="requires_caller_transaction"):
            await append_bound_financial_publication(
                session, recorded.position_event_public_id, admission_stamp(645, _RECORDED)
            )


async def test_publication_clock_cannot_precede_the_actual_position_record(
    scenario: ContractPositionScenario,
) -> None:
    """The whole transaction remains uncommitted.

    Given: a later actual position fact,
    When: its notification is backdated,
    Then: the whole transaction remains uncommitted.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        _, position = await stage_contract_position(session, scenario)
        with pytest.raises(FinancialPublicationError, match="precedes_position_knowledge"):
            await append_bound_financial_publication(
                session, position.public_id, admission_stamp(646, _KNOWN)
            )
        await session.rollback()
    assert await _count(repository, FinancialPublicationIntent) == 0
