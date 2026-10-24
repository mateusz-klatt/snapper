"""Exercise actual immutable notification recovery against genuine financial source graphs."""

import asyncio
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid7

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.versioned_financial_publication as publication_module
import snapper.data.financial_publication_delivery as legacy_delivery_module
import snapper.data.versioned_financial_publication_delivery as delivery_module
from snapper.application.portfolio.contract_account_projection import ContractAccountProjection
from snapper.application.portfolio.versioned_contract_account_projection import (
    VersionedContractAccountProjection,
)
from snapper.application.trade.financial_publication import publish_pending_financial_notifications
from snapper.application.trade.versioned_financial_publication import (
    publish_pending_versioned_financial_notifications,
)
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_publication import ContractPositionPublication
from snapper.core.financial_publication import canonical_financial_publication
from snapper.core.financial_publication import financial_publication_digest
from snapper.core.versioned_financial_publication_payload import FinancialPublicationPayload
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.financial_publication_delivery import load_pending_financial_publications
from snapper.data.financial_publication_delivery import record_financial_publication_delivery
from snapper.data.financial_publication_history import BoundVersionedFinancialPublication
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.financial_publication_triggers import install_financial_publication_triggers
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.versioned_financial_publication_delivery import (
    load_pending_versioned_financial_publications,
)
from snapper.data.versioned_financial_publication_delivery import (
    record_versioned_financial_publication_delivery,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3

pytestmark = pytest.mark.asyncio
projection_scenario = shared_projection_scenario


async def _receipt_count(repository: AccountingRepository) -> int:
    """Read committed local-return receipts independently of the publication transaction."""
    async with repository.session() as session:
        return int(
            await session.scalar(select(func.count()).select_from(FinancialPublicationDelivery))
            or 0
        )


async def test_versioned_recovery_preserves_actual_legacy_payload_bytes_and_identity(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Recover a genuine legacy notification through the new explicitly versioned seam.

    Given: A real original legacy position and its notification were committed together.
    When: The versioned publisher delivers to the actual versioned account projection.
    Then: Canonical legacy bytes and digest stay unchanged and exactly one local receipt commits.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = intent.payload.scope
    canonical = canonical_financial_publication(intent.payload)
    digest = financial_publication_digest(intent.payload)
    pending = await load_pending_versioned_financial_publications(repository, scope)
    assert len(pending) == 1 and pending[0].public_id == intent.public_id
    observer = VersionedContractAccountProjection(repository, scope)
    assert (
        await publish_pending_versioned_financial_notifications(
            repository, scope, observer, SequenceTracker()
        )
        == 1
    )
    snapshot = observer.snapshot
    assert snapshot is not None and snapshot.native_cash == () and snapshot.readiness == "held"
    assert snapshot.positions[0].latest_event_public_id == intent.payload.position_event_public_id
    assert canonical_financial_publication(intent.payload) == canonical
    assert financial_publication_digest(intent.payload) == digest == pending[0].content_digest
    assert await _receipt_count(repository) == 1
    assert await load_pending_versioned_financial_publications(repository, scope) == ()


async def test_real_native_v3_notification_recovers_and_restart_hydrates_after_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deliver an actual admitted native position without reapplying any financial delta.

    Given: Original signed native reads, admission and SDK source frames feed the real atomic V3 caller.
    When: Its retained notification is recovered and another observer restarts after local receipt.
    Then: Both read the same held position while the restarted publisher has no pending message.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        intent = recorded.publication
        repository = scenario.source.repository
        scope = intent.payload.scope
        pending = await load_pending_versioned_financial_publications(repository, scope)
        assert len(pending) == 1 and pending[0].payload == intent.payload
        assert pending[0].content_digest == intent.content_digest
        observer = VersionedContractAccountProjection(repository, scope)
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, scope, observer, SequenceTracker()
            )
            == 1
        )
        snapshot = observer.snapshot
        assert snapshot is not None and snapshot.native_cash == () and snapshot.readiness == "held"
        restarted = VersionedContractAccountProjection(repository, scope)
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, scope, restarted, SequenceTracker()
            )
            == 0
        )
        assert restarted.snapshot is None
        recovered = await restarted.refresh()
        assert recovered.journal == snapshot.journal
        assert recovered.positions == snapshot.positions
        assert await _receipt_count(repository) == 1


@pytest.mark.parametrize("cancelled", [False, True])
async def test_interrupted_callback_preserves_actual_intent_for_same_identity_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    """Preserve committed facts when an actual publication callback does not return successfully.

    Given: A genuine V3 position has one committed notification and no local delivery receipt.
    When: The publication callback raises or is cancelled before its local return.
    Then: No receipt commits and the subsequent recovery uses the exact original notification UUID.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        scope = recorded.publication.payload.scope
        observed: list[str] = []

        async def interrupt(
            topic: str, identity: str, payload: FinancialPublicationPayload
        ) -> None:
            """Observe the genuine original callback arguments and refuse a successful return."""
            assert topic == recorded.publication.topic and payload == recorded.publication.payload
            observed.append(identity)
            if cancelled:
                raise asyncio.CancelledError("owned_test_publication_cancelled")
            raise RuntimeError("owned_test_publication_failed")

        with pytest.raises((asyncio.CancelledError, RuntimeError), match="owned_test_publication"):
            await publish_pending_versioned_financial_notifications(
                repository, scope, interrupt, SequenceTracker()
            )
        assert observed == [recorded.publication.public_id]
        assert await _receipt_count(repository) == 0
        pending = await load_pending_versioned_financial_publications(repository, scope)
        assert len(pending) == 1 and pending[0].public_id == observed[0]
        observer = VersionedContractAccountProjection(repository, scope)
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, scope, observer, SequenceTracker()
            )
            == 1
        )
        assert await _receipt_count(repository) == 1


async def test_actual_delivery_insert_refusal_redelivers_original_uuid_without_rebooking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recover between actual notification consumption and a rejected local receipt insert.

    Given: A genuine native position is committed and a real SQLite trigger refuses delivery receipts.
    When: The observer refreshes successfully but the subsequent receipt transaction fails.
    Then: Recovery delivers the same UUID and the observer's durable financial journal stays identical.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        scope = recorded.publication.payload.scope
        observer = VersionedContractAccountProjection(repository, scope)
        observed: list[str] = []

        async def deliver(topic: str, identity: str, payload: FinancialPublicationPayload) -> None:
            """Hydrate the actual durable account before returning to the receipt writer."""
            observed.append(identity)
            await observer(topic, identity, payload)

        async with repository.session() as session:
            await session.execute(
                text(
                    "CREATE TRIGGER owned_delivery_refusal BEFORE INSERT ON financial_publication_deliveries BEGIN SELECT RAISE(ABORT, 'owned_delivery_refusal'); END"
                )
            )
            await session.commit()
        with pytest.raises(IntegrityError, match="owned_delivery_refusal"):
            await publish_pending_versioned_financial_notifications(
                repository, scope, deliver, SequenceTracker()
            )
        before = observer.snapshot
        assert before is not None and await _receipt_count(repository) == 0
        async with repository.session() as session:
            await session.execute(text("DROP TRIGGER owned_delivery_refusal"))
            await session.commit()
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, scope, deliver, SequenceTracker()
            )
            == 1
        )
        after = observer.snapshot
        assert after is not None and after.journal == before.journal
        assert observed == [recorded.publication.public_id, recorded.publication.public_id]
        assert await _receipt_count(repository) == 1


async def test_unknown_acknowledgement_after_actual_receipt_commit_does_not_publish_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read back a real committed receipt after its successful commit acknowledgement is lost.

    Given: A genuine native notification is consumed before the actual receipt commit begins.
    When: The original AsyncSession.commit succeeds and an observer raises immediately afterward.
    Then: Restart finds the original receipt and publishes nothing, retaining one financial journal.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        scope = recorded.publication.payload.scope
        observer = VersionedContractAccountProjection(repository, scope)
        original_commit = AsyncSession.commit

        async def committed_without_ack(session: AsyncSession) -> None:
            """Call the actual database commit before losing only its acknowledgement."""
            await original_commit(session)
            raise RuntimeError("owned_delivery_commit_ack_lost")

        with monkeypatch.context() as acknowledgement_loss:

            async def deliver(
                topic: str, identity: str, payload: FinancialPublicationPayload
            ) -> None:
                """Install the loss only after the actual consumer's independent hydration commits."""
                await observer(topic, identity, payload)
                acknowledgement_loss.setattr(AsyncSession, "commit", committed_without_ack)

            with pytest.raises(RuntimeError, match="owned_delivery_commit_ack_lost"):
                await publish_pending_versioned_financial_notifications(
                    repository, scope, deliver, SequenceTracker()
                )
        assert await _receipt_count(repository) == 1
        assert await load_pending_versioned_financial_publications(repository, scope) == ()
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, scope, observer, SequenceTracker()
            )
            == 0
        )
        snapshot = observer.snapshot
        assert snapshot is not None and len(snapshot.journal) == 1


@pytest.mark.parametrize("limit", [0, 1001, True, 1.0])
async def test_versioned_pending_page_refuses_nonbounded_or_coerced_limits(
    projection_scenario: ContractPositionScenario, limit: int | float
) -> None:
    """Keep the recovery page bound explicit before opening a read transaction.

    Given: An actual isolated financial repository and its declared account scope exist.
    When: A caller supplies an out-of-range or coercible limit instead of an exact bounded integer.
    Then: The versioned reader refuses before producing any notification page.
    """
    repository = projection_scenario.fill.admission.repository
    scope = projection_scenario.fill.admission.authority.scope
    with pytest.raises(ValueError, match="page limit"):
        await load_pending_versioned_financial_publications(repository, scope, cast(int, limit))


async def test_receipt_committed_after_pending_query_is_verified_and_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Respect a real receipt committed by another reader between query and final binding.

    Given: A genuine native notification is pending when the real bounded identity query runs.
    When: Another actual consumer hydrates and commits its receipt before the first page is rebound.
    Then: The versioned reader verifies that actual receipt and returns no stale pending entry.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        intent = recorded.publication
        observer = VersionedContractAccountProjection(repository, intent.payload.scope)
        original_query = delivery_module._pending_publication_ids
        tracker = SequenceTracker()

        async def observe_after_query(
            session: AsyncSession, scope: ExecutionAccountScope, limit: int
        ) -> tuple[str, ...]:
            """Run the actual query, then allow a real independent consumer and receipt commit."""
            identities = await original_query(session, scope, limit)
            assert identities == (intent.public_id,)
            await observer(intent.topic, intent.public_id, intent.payload)
            returned_at = datetime.now(UTC)
            await record_versioned_financial_publication_delivery(
                repository,
                intent.public_id,
                returned_at,
                SnapshotWriteProvenance(
                    public_id=str(uuid7()),
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("actual_delivery_race"),
                    timestamp=datetime.now(UTC),
                ),
            )
            return identities

        monkeypatch.setattr(delivery_module, "_pending_publication_ids", observe_after_query)
        assert (
            await load_pending_versioned_financial_publications(repository, intent.payload.scope)
            == ()
        )
        assert observer.snapshot is not None and await _receipt_count(repository) == 1


async def test_restored_delivery_with_changed_digest_refuses_before_redelivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detect restored receipt damage instead of treating a conflicting receipt as absent.

    Given: A genuine native publication was consumed and its actual receipt committed.
    When: A simulated damaged restore changes the receipt digest and reinstalls original guards.
    Then: The pending reader rejects the conflicting actual receipt before any publisher callback.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        scope = recorded.publication.payload.scope
        observer = VersionedContractAccountProjection(repository, scope)
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, scope, observer, SequenceTracker()
            )
            == 1
        )
        async with repository.engine.begin() as connection:
            await connection.execute(
                text("DROP TRIGGER financial_publication_deliveries_reject_update")
            )
            await connection.execute(
                text("UPDATE financial_publication_deliveries SET publication_digest = :digest"),
                {"digest": "0" * 64},
            )
            await connection.run_sync(
                lambda actual: install_financial_publication_triggers(
                    actual, "financial_publication_deliveries"
                )
            )
        with pytest.raises(
            FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
        ):
            await load_pending_versioned_financial_publications(repository, scope)
        assert await _receipt_count(repository) == 1


async def _receipt_material(repository: AccountingRepository) -> tuple[object, ...]:
    """Retain actual original receipt provenance and clock fields without reconstructing a row."""
    async with repository.session() as session:
        row = (await session.scalars(select(FinancialPublicationDelivery))).one()
        return (
            row.id,
            row.public_id,
            row.session_id,
            row.sequence_id,
            row.timestamp,
            row.known_to,
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.environment,
            row.margin_account,
            row.publication_public_id,
            row.publication_digest,
            row.delivery_semantics,
            row.published_at,
        )


async def test_original_v1_delivery_receipt_and_canonical_intent_survive_both_retry_readers(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Original legacy source bytes and first actual receipt stay immutable through the common seam.

    Given: The real v1 producer and original v1 projection commit a genuine source-bound notification receipt.
    When: Both receipt entry points retry with independent later provenance and the versioned reader reloads it.
    Then: The first UUID, all original receipt fields and exact canonical intent bytes remain unchanged.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = intent.payload.scope
    observer = ContractAccountProjection(repository, scope)
    assert (
        await publish_pending_financial_notifications(
            repository, scope, observer, SequenceTracker()
        )
        == 1
    )
    original = await _receipt_material(repository)
    async with repository.session() as session:
        row = (await session.scalars(select(FinancialPublicationIntent))).one()
        before = row.canonical_payload, row.content_digest
    assert before == (
        canonical_financial_publication(intent.payload),
        financial_publication_digest(intent.payload),
    )
    tracker = SequenceTracker()
    returned_at = datetime.now(UTC)
    stamp = SnapshotWriteProvenance(
        public_id=str(uuid7()),
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("original_v1_receipt_retry"),
        timestamp=datetime.now(UTC),
    )
    original_id = await record_financial_publication_delivery(
        repository, intent.public_id, returned_at, stamp
    )
    versioned_id = await record_versioned_financial_publication_delivery(
        repository, intent.public_id, returned_at, stamp
    )
    assert original_id == versioned_id == original[1]
    assert await _receipt_material(repository) == original
    assert await load_pending_financial_publications(repository, scope) == ()
    assert await load_pending_versioned_financial_publications(repository, scope) == ()
    async with repository.session() as session:
        row = (await session.scalars(select(FinancialPublicationIntent))).one()
        assert (row.canonical_payload, row.content_digest) == before


async def _restore_damaged_receipt(repository: AccountingRepository, field: str) -> None:
    """Damage a real retained SQLite row through a controlled restore and reinstall original guards."""
    statements = {
        "digest": "UPDATE financial_publication_deliveries SET publication_digest = '"
        + "0" * 64
        + "'",
        "known_to": "UPDATE financial_publication_deliveries SET known_to = '9998-01-01 00:00:00.000000'",
        "published_after_record": "UPDATE financial_publication_deliveries SET published_at = datetime(timestamp, '+1 second')",
    }
    async with repository.session() as session:
        await session.execute(text("PRAGMA ignore_check_constraints = ON"))
        try:
            await session.execute(
                text("DROP TRIGGER financial_publication_deliveries_reject_update")
            )
            await session.execute(text(statements[field]))
            connection = await session.connection()
            await connection.run_sync(
                lambda actual: install_financial_publication_triggers(
                    actual, "financial_publication_deliveries"
                )
            )
            await session.commit()
        finally:
            await session.execute(text("PRAGMA ignore_check_constraints = OFF"))


@pytest.mark.parametrize("damaged_field", ("digest", "known_to", "published_after_record"))
async def test_legacy_damaged_receipt_is_refused_before_original_or_versioned_callback(
    projection_scenario: ContractPositionScenario,
    damaged_field: str,
) -> None:
    """A malformed real receipt cannot be treated as merely absent by either recovery entry point.

    Given: Original v1 publication and its actual local-return receipt were committed by the real producer.
    When: A damaged restore changes a digest, permanent lifetime or publication-to-recording clock relation.
    Then: Both real pending readers refuse before any callback while original financial facts remain retained.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = intent.payload.scope
    observer = ContractAccountProjection(repository, scope)
    assert (
        await publish_pending_financial_notifications(
            repository, scope, observer, SequenceTracker()
        )
        == 1
    )
    await _restore_damaged_receipt(repository, damaged_field)
    callbacks: list[str] = []

    async def legacy_callback(
        topic: str, public_id: str, payload: ContractPositionPublication
    ) -> None:
        """Observe an improper callback before invoking the original real legacy projection."""
        callbacks.append(public_id)
        await observer(topic, public_id, payload)

    versioned_observer = VersionedContractAccountProjection(repository, scope)

    async def versioned_callback(
        topic: str, public_id: str, payload: FinancialPublicationPayload
    ) -> None:
        """Observe an improper callback before invoking the real versioned projection."""
        callbacks.append(public_id)
        await versioned_observer(topic, public_id, payload)

    with pytest.raises(
        FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
    ):
        await publish_pending_financial_notifications(
            repository, scope, legacy_callback, SequenceTracker()
        )
    with pytest.raises(
        FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
    ):
        await publish_pending_versioned_financial_notifications(
            repository, scope, versioned_callback, SequenceTracker()
        )
    assert callbacks == [] and await _receipt_count(repository) == 1
    with pytest.raises(
        FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
    ):
        now = datetime.now(UTC)
        await record_financial_publication_delivery(
            repository,
            intent.public_id,
            now,
            SnapshotWriteProvenance(
                public_id=str(uuid7()), session_id=str(uuid7()), sequence_id=1, timestamp=now
            ),
        )


async def test_legacy_receipt_committed_after_real_pending_query_is_verified_and_skipped(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The original reader also respects an actual receipt committed after selecting its pending IDs.

    Given: The real legacy pending query observes one genuine committed notification before any receipt exists.
    When: An independent original projection consumes it and commits a real receipt before page rebinding.
    Then: The first reader verifies the new receipt and returns an empty page without a duplicate callback.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    observer = ContractAccountProjection(repository, intent.payload.scope)
    original_query = legacy_delivery_module._pending_publication_ids

    async def after_query(
        session: AsyncSession, scope: ExecutionAccountScope, limit: int
    ) -> tuple[str, ...]:
        """Invoke the original query then the actual independent legacy consumer and receipt writer."""
        identities = await original_query(session, scope, limit)
        assert identities == (intent.public_id,)
        await observer(intent.topic, intent.public_id, intent.payload)
        now = datetime.now(UTC)
        await record_financial_publication_delivery(
            repository,
            intent.public_id,
            now,
            SnapshotWriteProvenance(
                public_id=str(uuid7()), session_id=str(uuid7()), sequence_id=1, timestamp=now
            ),
        )
        return identities

    monkeypatch.setattr(legacy_delivery_module, "_pending_publication_ids", after_query)
    assert await load_pending_financial_publications(repository, intent.payload.scope) == ()
    assert observer.snapshot is not None and await _receipt_count(repository) == 1


async def test_two_actual_versioned_publishers_share_one_receipt_after_both_callbacks_return(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Independent callbacks can overlap while the actual wallet-fenced receipt writer remains idempotent.

    Given: Two real recovery calls load the same genuine committed legacy intent before either callback returns.
    When: Both actual projections hydrate but the later-returning callback records its real receipt first.
    Then: The earlier caller accepts that original receipt without changing its UUID or recording clocks.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = intent.payload.scope
    observer = VersionedContractAccountProjection(repository, scope)
    barrier = asyncio.Barrier(2)
    seen: list[str] = []
    stamps: list[SnapshotWriteProvenance] = []
    later_committed = asyncio.Event()
    original_record = publication_module.record_versioned_financial_publication_delivery

    async def reverse_receipt_order(
        actual_repository: AccountingRepository,
        public_id: str,
        published_at: datetime,
        provenance: SnapshotWriteProvenance,
    ) -> str:
        """Preserve the actual receipt writers while letting the later callback acquire its wallet lock first."""
        index = len(stamps)
        stamps.append(provenance)
        if index == 0:
            await later_committed.wait()
        result = await original_record(actual_repository, public_id, published_at, provenance)
        if index == 1:
            later_committed.set()
        return result

    monkeypatch.setattr(
        publication_module, "record_versioned_financial_publication_delivery", reverse_receipt_order
    )

    async def deliver(topic: str, public_id: str, payload: FinancialPublicationPayload) -> None:
        """Hold both genuine callbacks until both independent recovery pages have already been read."""
        seen.append(public_id)
        await barrier.wait()
        await observer(topic, public_id, payload)

    tasks = [
        asyncio.create_task(
            publish_pending_versioned_financial_notifications(
                repository, scope, deliver, SequenceTracker()
            )
        )
        for _ in range(2)
    ]
    try:
        assert await asyncio.gather(*tasks) == [1, 1]
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    assert seen == [intent.public_id, intent.public_id]
    assert await _receipt_count(repository) == 1
    assert len(stamps) == 2 and stamps[0].timestamp <= stamps[1].timestamp
    material = await _receipt_material(repository)
    assert material[1] == stamps[1].public_id and material[4] == stamps[1].timestamp
    assert await load_pending_versioned_financial_publications(repository, scope) == ()
    snapshot = observer.snapshot
    assert snapshot is not None and len(snapshot.journal) == 1


async def test_missing_actual_delivery_intent_refuses_before_acquiring_any_wallet_fence(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent untrusted selector cannot become a wallet lock or invented publication receipt.

    Given: A genuine isolated financial repository has no notification with the requested UUID.
    When: The versioned receipt writer queries that exact immutable identity.
    Then: It refuses the missing row before acquiring a wallet fence or writing a receipt.
    """
    repository = projection_scenario.fill.admission.repository
    original_fence = repository._acquire_execution_fence_lock
    wallets: list[str] = []

    async def observe_fence(session: AsyncSession, wallet_public_id: str) -> None:
        """Record and preserve any actual wallet fence attempted by the real writer."""
        wallets.append(wallet_public_id)
        await original_fence(session, wallet_public_id)

    monkeypatch.setattr(repository, "_acquire_execution_fence_lock", observe_fence)
    now = datetime.now(UTC)
    with pytest.raises(FinancialPublicationError, match="financial_publication_missing"):
        await record_versioned_financial_publication_delivery(
            repository,
            str(uuid7()),
            now,
            SnapshotWriteProvenance(
                public_id=str(uuid7()), session_id=str(uuid7()), sequence_id=1, timestamp=now
            ),
        )
    assert wallets == [] and await _receipt_count(repository) == 0


async def test_repaired_actual_selector_cannot_write_under_a_different_wallet_fence(
    projection_scenario: ContractPositionScenario,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The untrusted initial selector must still match the independently rebound original source scope.

    Given: A damaged restore changes the actual intent's indexed wallet before the receipt writer reads it.
    When: An observer acquires that real selected fence then repairs the actual row before full rebinding.
    Then: The writer detects the wallet mismatch and inserts no receipt under the wrong fence.
    """
    intent = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    wrong_wallet = str(uuid7())
    async with repository.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER financial_publication_intents_reject_update"))
        await connection.execute(
            text("UPDATE financial_publication_intents SET wallet_public_id = :wallet"),
            {"wallet": wrong_wallet},
        )
        await connection.run_sync(
            lambda actual: install_financial_publication_triggers(
                actual, "financial_publication_intents"
            )
        )
    original_fence = repository._acquire_execution_fence_lock
    wallets: list[str] = []

    async def repair_after_actual_fence(session: AsyncSession, wallet_public_id: str) -> None:
        """Call the original actual lock primitive before repairing the stored selector with real SQL."""
        await original_fence(session, wallet_public_id)
        wallets.append(wallet_public_id)
        await session.execute(text("DROP TRIGGER financial_publication_intents_reject_update"))
        await session.execute(
            text("UPDATE financial_publication_intents SET wallet_public_id = :wallet"),
            {"wallet": intent.payload.scope.wallet_public_id},
        )
        connection = await session.connection()
        await connection.run_sync(
            lambda actual: install_financial_publication_triggers(
                actual, "financial_publication_intents"
            )
        )

    monkeypatch.setattr(repository, "_acquire_execution_fence_lock", repair_after_actual_fence)
    now = datetime.now(UTC)
    with pytest.raises(
        FinancialPublicationError, match="financial_delivery_actual_wallet_mismatch"
    ):
        await record_versioned_financial_publication_delivery(
            repository,
            intent.public_id,
            now,
            SnapshotWriteProvenance(
                public_id=str(uuid7()), session_id=str(uuid7()), sequence_id=1, timestamp=now
            ),
        )
    assert wallets == [wrong_wallet] and await _receipt_count(repository) == 0


async def test_actual_native_full_reader_runs_only_after_original_wallet_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Full native source rebinding may acquire the global guard only after the selected wallet fence.

    Given: The genuine admitted V3 producer committed one actual native financial notification.
    When: Observers preserve the real fence and full reader while recording their invocation order.
    Then: Every complete native read follows the actual wallet lock and the real receipt commits once.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        intent = recorded.publication
        original_fence = repository._acquire_execution_fence_lock
        original_reader = delivery_module.get_bound_versioned_financial_publication
        sequence: list[str] = []
        observer = VersionedContractAccountProjection(repository, intent.payload.scope)
        await observer(intent.topic, intent.public_id, intent.payload)

        async def observe_fence(session: AsyncSession, wallet_public_id: str) -> None:
            """Preserve the actual repository wallet lock before marking that it is owned."""
            await original_fence(session, wallet_public_id)
            assert wallet_public_id == intent.payload.scope.wallet_public_id
            sequence.append("wallet")

        async def observe_reader(
            session: AsyncSession, public_id: str
        ) -> BoundVersionedFinancialPublication:
            """Require the real wallet lock before calling the original full native graph and guard reader."""
            assert sequence and sequence[0] == "wallet"
            sequence.append("full_reader")
            return await original_reader(session, public_id)

        monkeypatch.setattr(repository, "_acquire_execution_fence_lock", observe_fence)
        monkeypatch.setattr(
            delivery_module, "get_bound_versioned_financial_publication", observe_reader
        )
        now = datetime.now(UTC)
        await record_versioned_financial_publication_delivery(
            repository,
            intent.public_id,
            now,
            SnapshotWriteProvenance(
                public_id=str(uuid7()), session_id=str(uuid7()), sequence_id=1, timestamp=now
            ),
        )
        assert sequence == ["wallet", "full_reader", "full_reader"]
        assert await _receipt_count(repository) == 1
