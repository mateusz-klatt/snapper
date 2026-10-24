"""Audit actual restored journals without inventing financial or publication evidence."""

import base64
import json
from datetime import timedelta

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from snapper.application.portfolio.contract_account_projection import ContractAccountProjection
from snapper.application.trade.financial_publication import publish_pending_financial_notifications
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.data.accounting_native_source_triggers import install_accounting_native_source_triggers
from snapper.data.contract_accounting_guards import ContractAccountingIntegrityError
from snapper.data.contract_accounting_guards import acquire_contract_accounting_restore_barrier
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.contract_accounting_guards import require_contract_accounting_guards
from snapper.data.contract_accounting_integrity import audit_contract_accounting_restore
from snapper.data.contract_accounting_integrity import run_contract_accounting_restore_audit
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.contract_accounting_triggers import install_contract_accounting_table_triggers
from snapper.data.contract_position_ledger_store import ContractPositionAccountingRequest
from snapper.data.contract_position_ledger_store import append_bound_contract_position
from snapper.data.financial_publication_triggers import install_financial_publication_triggers
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.application.trade.test_unit_snapshot_fill import _VENUE
from tests.application.trade.test_unit_snapshot_fill import _observation
from tests.application.trade.test_unit_snapshot_fill import _Scenario
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_cash
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import create_position_units
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp

projection_scenario = shared_projection_scenario


async def test_full_audit_binds_actual_position_cash_claims_and_delivery(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Bind actual retained graphs without changing account balances or releasing readiness.

    Given: Real position and cash facts, claims, anchors and a delivered v1 intent exist.
    When: The finite startup audit runs through the actual repository boundary.
    Then: It verifies every retained financial fact and local publication graph.
    """
    publication = await record_projection_position(projection_scenario)
    await record_projection_cash(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    observer = ContractAccountProjection(repository, publication.payload.scope)
    assert (
        await publish_pending_financial_notifications(
            repository, publication.payload.scope, observer, SequenceTracker()
        )
        == 1
    )
    report = await run_contract_accounting_restore_audit(repository)
    assert (report.event_count, report.claim_count, report.anchor_count) == (2, 2, 2)
    assert (report.publication_count, report.delivery_count) == (1, 1)
    assert report.scopes == (publication.payload.scope,)
    assert len(report.physical_guards) == 16
    assert len(report.retained_graph_digest) == 64
    assert observer.snapshot is not None
    assert observer.snapshot.readiness == "held"


async def test_cash_only_fact_needs_no_fake_execution_or_v1_position_notification(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Audit actual native cash independently of the position-only notification contract.

    Given: One real cash event and claim exist without a local position or Execution.
    When: The finite retained graph audit runs.
    Then: It accepts the native cash lineage without manufacturing v1 references.
    """
    await record_projection_cash(projection_scenario)
    report = await run_contract_accounting_restore_audit(
        projection_scenario.fill.admission.repository
    )
    assert (report.event_count, report.claim_count, report.anchor_count) == (1, 1, 1)
    assert (report.publication_count, report.delivery_count) == (0, 0)


async def test_raw_pending_evidence_is_not_a_fabricated_funded_flat_account(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Preserve raw-stage unknowns while auditing an empty interpreted financial graph.

    Given: Genuine raw receipts and their durable pending holds exist.
    When: The finite startup audit finds no financial interpretation.
    Then: It reports zero financial facts without constructing a cash balance or position.
    """
    report = await run_contract_accounting_restore_audit(
        projection_scenario.fill.admission.repository
    )
    assert (report.event_count, report.claim_count, report.anchor_count) == (0, 0, 0)
    assert report.scopes == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("wallet_public_id", admission_id(800)),
        ("environment", "demo"),
        ("kind", "unknown_native_event"),
        ("account_scope_digest", "0" * 64),
    ],
)
async def test_reinstalled_guards_do_not_hide_restored_index_damage(
    projection_scenario: ContractPositionScenario, field: str, value: str
) -> None:
    """Enumerate every actual event before trusting restored kind or account indexes.

    Given: Privileged restore damage lost a claim and changed a real event index.
    When: All physical guards are reinstalled and the startup audit runs.
    Then: Canonical lineage refuses instead of allowing filtered queries to hide the damaged row.
    """
    await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_native_source_claims_reject_delete"))
        await session.execute(text("DELETE FROM accounting_native_source_claims"))
        await session.execute(text("DROP TRIGGER contract_accounting_events_reject_update"))
        await session.execute(
            text(f"UPDATE contract_accounting_events SET {field} = :value"), {"value": value}
        )
        connection = await session.connection()
        await connection.run_sync(
            install_accounting_native_source_triggers, "accounting_native_source_claims"
        )
        await connection.run_sync(
            install_contract_accounting_table_triggers, "contract_accounting_events"
        )
        await session.commit()
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        assert (
            len(await require_contract_accounting_guards(session, include_publications=True)) == 16
        )
    with pytest.raises(ValueError):
        await run_contract_accounting_restore_audit(repository)
    async with repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 1


@pytest.mark.parametrize(
    "trigger",
    [
        "accounting_native_source_claims_reject_delete",
        "contract_accounting_events_reject_update",
        "accounting_raw_receipts_reject_update",
        "financial_publication_intents_reject_delete",
    ],
)
async def test_missing_physical_guard_refuses_even_before_financial_interpretation(
    projection_scenario: ContractPositionScenario, trigger: str
) -> None:
    """Check actual physical preconditions independently of financial row indexes.

    Given: A private restore fixture drops one required immutable journal or outbox trigger.
    When: The real finite startup audit reads its physical metadata.
    Then: It refuses the missing guard even if no financial fact exists yet.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await session.execute(text(f"DROP TRIGGER {trigger}"))
        await session.commit()
    with pytest.raises(ContractAccountingIntegrityError, match="physical_guard_invalid"):
        await run_contract_accounting_restore_audit(repository)


async def test_familiar_trigger_name_with_permissive_body_is_not_immutability_proof(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Validate actual installed trigger semantics instead of a familiar catalog name.

    Given: A restore fixture replaces the native delete guard with a same-named no-op.
    When: The physical precondition compares the installed definition.
    Then: It refuses the permissive body before reading financial history.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_native_source_claims_reject_delete"))
        await session.execute(
            text(
                "CREATE TRIGGER accounting_native_source_claims_reject_delete "
                "BEFORE DELETE ON accounting_native_source_claims BEGIN SELECT 1; END"
            )
        )
        await session.commit()
    with pytest.raises(ContractAccountingIntegrityError, match="physical_guard_invalid"):
        await run_contract_accounting_restore_audit(repository)


async def test_sqlite_connection_local_table_cannot_shadow_the_guarded_relation(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Keep guard evidence attached to the actual unqualified financial relation.

    Given: A connection creates a temporary table with the native claim table's name.
    When: The actual guarded transaction checks physical metadata.
    Then: It refuses the shadow instead of checking main guards for writes to another table.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await session.execute(
            text("CREATE TEMP TABLE accounting_native_source_claims (id INTEGER)")
        )
        try:
            with pytest.raises(ContractAccountingIntegrityError, match="temporary_relation_shadow"):
                await require_contract_accounting_guards(session)
        finally:
            await session.execute(text("DROP TABLE temp.accounting_native_source_claims"))


async def test_deferred_sqlite_caller_holds_actual_schema_reservation_until_transaction_end(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Prove the write barrier acquires a real reservation rather than trusting logical session state.

    Given: A SQLite session has only read and has not explicitly reserved database writes.
    When: It joins the actual accounting barrier while a second connection attempts trigger DDL.
    Then: DDL is blocked until the protected transaction rolls back and then succeeds unchanged.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as protected:
        await protected.execute(text("SELECT 1"))
        assert protected.in_transaction()
        await acquire_contract_accounting_write_barrier(protected)
        async with repository.session() as ddl:
            await ddl.execute(text("PRAGMA busy_timeout = 100"))
            with pytest.raises(OperationalError, match="locked"):
                await ddl.execute(
                    text("DROP TRIGGER accounting_native_source_claims_reject_delete")
                )
            await ddl.rollback()
        await protected.rollback()
    async with repository.session() as ddl:
        await ddl.execute(text("DROP TRIGGER accounting_native_source_claims_reject_delete"))
        await ddl.commit()


async def test_orphan_native_seed_is_not_a_certified_financial_restore(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Verify closure between every retained native anchor and the supported actual event graph.

    Given: A restore fixture adds an unreferenced copy of a genuine source anchor.
    When: The complete finite audit checks all anchors without account filters.
    Then: It refuses the orphan without inventing a financial event for it.
    """
    await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        original = (await session.scalars(select(ContractAccountingAnchor))).one()
        values = {
            column.name: getattr(original, column.name)
            for column in ContractAccountingAnchor.__table__.columns
            if column.name not in {"id", "public_id", "source_receipt_public_id"}
        }
        session.add(
            ContractAccountingAnchor(
                **values,
                public_id=admission_id(701),
                source_receipt_public_id=projection_scenario.fill.source_request.raw_receipt_public_id,
            )
        )
        await session.commit()
    with pytest.raises(ContractAccountingIntegrityError, match="anchor_inventory"):
        await run_contract_accounting_restore_audit(repository)


@pytest.mark.parametrize("operation", ["audit", "guard", "barrier", "restore_barrier"])
async def test_integrity_requires_the_actual_caller_transaction(
    projection_scenario: ContractPositionScenario, operation: str
) -> None:
    """Require a real transaction before returning physical or startup audit evidence.

    Given: A genuine repository session has not begun a transaction.
    When: A caller requests a startup audit, guard check or writer barrier.
    Then: The operation refuses instead of returning unprotected readiness-looking evidence.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        with pytest.raises(RuntimeError, match="caller_transaction"):
            if operation == "audit":
                await audit_contract_accounting_restore(session)
            elif operation == "guard":
                await require_contract_accounting_guards(session)
            elif operation == "barrier":
                await acquire_contract_accounting_write_barrier(session)
            else:
                await acquire_contract_accounting_restore_barrier(session)


async def _second_position_source(scenario: ContractPositionScenario) -> ContractPositionScenario:
    """Retain a second genuine fill and native position transition for the same contract."""
    original = _observation(scenario.fill.admission)
    document = json.loads(base64.b64decode(original.material))
    document["fills"][0].update(
        fill_id="native-fill-2",
        time=int((_VENUE + timedelta(seconds=1)).timestamp()) * 1000,
        seq=13,
        remaining_order_qty=0,
    )
    observation = RawAccountingObservation.model_validate(
        {
            **original.model_dump(),
            "material": base64.b64encode(json.dumps(document).encode()).decode(),
            "observed_at": original.observed_at + timedelta(seconds=1),
        }
    )
    raw = await capture_accounting_observation(
        scenario.fill.admission.repository,
        observation,
        AccountingWriteProvenance.model_validate(admission_stamp(302, _KNOWN).model_dump()),
    )
    fill = _Scenario(
        scenario.fill.admission,
        FuturesFillSourceRequest.model_validate(
            {**scenario.fill.source_request.model_dump(), "raw_receipt_public_id": raw.public_id}
        ),
    )
    receipt = await capture_position_log(
        fill,
        303,
        position_log_entry(
            id=18,
            execution="native-fill-2",
            date=(_VENUE + timedelta(seconds=1, milliseconds=10)).isoformat(),
            old_balance=1,
            new_balance=2,
            old_average_entry_price=100,
            new_average_entry_price=100,
        ),
    )
    return ContractPositionScenario(fill, receipt)


async def test_restored_surrogate_order_cannot_replace_the_latest_semantic_position(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Select actual ledger chronology independently of restored database surrogate order.

    Given: Two genuine source fills form one conserved native position and claims.
    When: A restore reorders only their surrogate IDs and reinstalls every physical guard.
    Then: The finite audit chooses the latest ledger fact and validates both original fills.
    """
    repository = projection_scenario.fill.admission.repository
    await record_projection_position(projection_scenario)
    second = await _second_position_source(projection_scenario)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, second.fill.source_request.wallet_public_id
        )
        fill, unit_id = await create_position_units(session, second, unit_sequence=401)
        await append_bound_contract_position(
            session,
            ContractPositionAccountingRequest(
                fill=fill,
                unit_snapshot_public_id=unit_id,
                position_receipt_public_id=second.position_receipt_public_id,
            ),
            admission_stamp(501, _KNOWN + timedelta(seconds=1)),
        )
        await session.commit()
    async with repository.session() as session:
        ids = tuple((await session.scalars(select(ContractAccountingEvent.id))).all())
        assert len(ids) == 2
        await session.execute(text("DROP TRIGGER contract_accounting_events_reject_update"))
        await session.execute(text("UPDATE contract_accounting_events SET id = -id"))
        await session.execute(
            text("UPDATE contract_accounting_events SET id = :total + id"),
            {"total": sum(ids)},
        )
        connection = await session.connection()
        await connection.run_sync(
            install_contract_accounting_table_triggers, "contract_accounting_events"
        )
        await session.commit()
    report = await run_contract_accounting_restore_audit(repository)
    assert report.event_count == 2
    assert report.claim_count == 2
    assert report.anchor_count == 1


async def test_orphaned_delivery_is_detected_after_physical_guards_are_reinstalled(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Rebind every existing local delivery instead of trusting a restored publication UUID.

    Given: A real committed position notification has one genuine local delivery receipt.
    When: Restore damage changes that receipt to an absent intent and restores its physical guard.
    Then: The complete audit refuses the orphan instead of declaring the local graph consistent.
    """
    publication = await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    observer = ContractAccountProjection(repository, publication.payload.scope)
    assert (
        await publish_pending_financial_notifications(
            repository, publication.payload.scope, observer, SequenceTracker()
        )
        == 1
    )
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER financial_publication_deliveries_reject_update"))
        await session.execute(
            text("UPDATE financial_publication_deliveries SET publication_public_id = :missing"),
            {"missing": admission_id(999)},
        )
        connection = await session.connection()
        await connection.run_sync(
            install_financial_publication_triggers, "financial_publication_deliveries"
        )
        await session.commit()
    with pytest.raises(ContractAccountingIntegrityError, match="delivery_intent_missing"):
        await run_contract_accounting_restore_audit(repository)
