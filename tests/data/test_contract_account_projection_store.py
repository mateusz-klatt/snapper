"""Prove current account hydration binds actual raw, unit, claim and journal history."""

from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy import text

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.data.contract_account_projection_store import ContractAccountProjectionError
from snapper.data.contract_account_projection_store import _project_rows
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.models import ContractAccountingEvent
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import read_projection
from tests.helpers.contract_account_projection import record_projection_cash
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import position_log_entry

projection_scenario = shared_projection_scenario


async def test_empty_observed_journal_remains_unknown_with_actual_pending_holds(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Keep missing native state unknown while raw receipts already hold the account.

    Given: Actual fill and position raw receipts exist without interpreted financial facts.
    When: The exact declared account is hydrated under its real wallet fence.
    Then: It has no invented cash or flat position and both unresolved holds remain visible.
    """
    snapshot = await read_projection(projection_scenario)
    assert snapshot.journal == ()
    assert snapshot.positions == ()
    assert snapshot.native_cash == ()
    assert len(snapshot.unresolved_holds) == 2
    assert all(hold.margin_account is None for hold in snapshot.unresolved_holds)
    assert snapshot.readiness == "held"


async def test_actual_position_and_separate_cash_row_hydrate_without_double_posting_components(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Hydrate genuine immutable position and cash facts without an invented pairing contract.

    Given: Distinct actual native IDs17 and18 established a position and a USD balance transition.
    When: The full retained account is read through its bound financial readers.
    Then: The absolute USD balance is99.75 and every position's absent component remains unknown.
    """
    publication = await record_projection_position(projection_scenario)
    cash = await record_projection_cash(projection_scenario)
    snapshot = await read_projection(projection_scenario)
    assert snapshot.scope == projection_scenario.fill.admission.authority.scope
    assert [item.kind for item in snapshot.journal] == ["contract_position_fill", "native_cash"]
    assert (
        snapshot.positions[0].latest_event_public_id == publication.payload.position_event_public_id
    )
    assert snapshot.positions[0].quantity == Decimal("1")
    assert snapshot.positions[0].entry_price == Decimal("100")
    assert snapshot.positions[0].modeled_realized_pnl_since_anchor == Decimal("0")
    assert snapshot.native_cash[0].balance == Decimal("99.75")
    assert snapshot.native_cash[0].latest_event_public_id == cash.public_id
    assert snapshot.reported_cash_components[0].fee is None
    assert snapshot.reported_cash_components[0].source_log_id == "17"
    assert snapshot.reported_cash_components[0].cash_reconciliation_status == "unproven"
    assert len(snapshot.unresolved_holds) == 3


async def test_snapshot_refuses_retained_financial_knowledge_after_its_read_clock(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Refuse a historical-looking current read that predates an actual journal row.

    Given: The position event was retained after the exact source fill knowledge.
    When: A current snapshot is requested with an earlier local read clock.
    Then: The reader refuses instead of exposing future facts or silently truncating history.
    """
    await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    scope = projection_scenario.fill.admission.authority.scope
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, scope.wallet_public_id)
        with pytest.raises(ContractAccountProjectionError, match="retained_knowledge"):
            await load_bound_contract_account_snapshot(session, scope, _KNOWN)


async def test_snapshot_refuses_hold_source_knowledge_after_its_read_clock(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Preserve raw-stage knowledge clocks independently of financial interpretation.

    Given: A position account log is observed after the fill, before any financial event.
    When: The current snapshot's read clock predates that durable pending hold.
    Then: The reader refuses instead of pretending the future source was already known.
    """
    repository = projection_scenario.fill.admission.repository
    scope = projection_scenario.fill.admission.authority.scope
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, scope.wallet_public_id)
        with pytest.raises(ContractAccountProjectionError, match="hold_knowledge"):
            await load_bound_contract_account_snapshot(session, scope, _KNOWN)


async def test_snapshot_requires_actual_outer_transaction(
    tmp_path: Path,
) -> None:
    """Require the caller's transaction boundary before hydration queries begin.

    Given: A real isolated repository session has no outer transaction or hydration query.
    When: The loader is called on a session without an outer transaction.
    Then: It refuses instead of reading across independently changing financial commits.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'unstarted-projection.db'}")
    scope = ExecutionAccountScope(
        wallet_public_id="00000000-0000-7000-8000-000000000003",
        exchange="kraken_futures",
        mode="live",
        environment="production",
        margin_account="futures.flex",
    )
    try:
        async with repository.session() as session:
            with pytest.raises(RuntimeError, match="caller_transaction"):
                await load_bound_contract_account_snapshot(session, scope, _KNOWN)
    finally:
        await repository.engine.dispose()


async def test_naive_read_clock_cannot_alias_an_aware_financial_frontier(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Keep local knowledge frontiers aware even when no financial event exists.

    Given: A real wallet-fenced read has genuine retained source evidence.
    When: Its supplied local knowledge clock has no timezone.
    Then: Validation rejects it before building a partial account projection.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        with pytest.raises(ValidationError):
            await load_bound_contract_account_snapshot(
                session,
                projection_scenario.fill.admission.authority.scope,
                _KNOWN.replace(tzinfo=None),
            )


@pytest.mark.parametrize("damage", ["kind", "instrument", "payload"])
async def test_damaged_selected_financial_fact_cannot_be_projected(
    projection_scenario: ContractPositionScenario, damage: str
) -> None:
    """Reject selected immutable-history damage rather than return a plausible account.

    Given: A genuine committed position and claim were created through the actual writer.
    When: A restore fixture explicitly removes immutability and damages a selected event.
    Then: Hydration refuses the unsupported kind, missing instrument or invalid source projection.
    """
    await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    statements = {
        "kind": "UPDATE contract_accounting_events SET kind = 'unsupported_native_kind'",
        "instrument": "UPDATE contract_accounting_events SET instrument_public_id = NULL",
        "payload": "UPDATE contract_accounting_events SET content_digest = :digest",
    }
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER contract_accounting_events_reject_update"))
        await session.execute(text(statements[damage]), {"digest": "0" * 64})
        await session.commit()
    with pytest.raises(ValueError):
        await read_projection(projection_scenario)


async def test_damaged_hold_margin_cannot_claim_its_original_broad_raw_scope(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Validate the actual hold-to-source margin relationship during partial hydration.

    Given: Genuine raw receipts carry broad unknown-margin holds.
    When: A restore fixture removes the guard and changes their margin index to the selected profile.
    Then: The current reader refuses the altered hold instead of hiding the proof discrepancy.
    """
    repository = projection_scenario.fill.admission.repository
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_scope_holds_reject_update"))
        await session.execute(
            text("UPDATE accounting_scope_holds SET margin_account = :margin"),
            {"margin": projection_scenario.fill.admission.authority.scope.margin_account},
        )
        await session.commit()
    with pytest.raises(ContractAccountProjectionError, match="hold_scope_mismatch"):
        await read_projection(projection_scenario)


async def test_actual_reported_fee_and_funding_remain_unreconciled_without_cash(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Retain actual nonzero position components without manufacturing a balance transition.

    Given: The selected native position row reports the fill's fee and a funding amount.
    When: Its genuine execution, unit, position and claim commit without a native wallet row.
    Then: The account exposes both reported values while cash stays unknown and reconciliation held.
    """
    raw_id = await capture_position_log(
        projection_scenario.fill,
        311,
        position_log_entry(fee="0.10", realized_funding="-0.000001", funding_rate="0.0000001"),
    )
    scenario = ContractPositionScenario(projection_scenario.fill, raw_id)
    await record_projection_position(scenario)
    snapshot = await read_projection(scenario)
    component = snapshot.reported_cash_components[0]
    assert component.fee == Decimal("0.10")
    assert component.realized_funding == Decimal("-0.000001")
    assert component.funding_rate == Decimal("0.0000001")
    assert component.reported_collateral_currency == "USD"
    assert component.cash_reconciliation_status == "unproven"
    assert snapshot.native_cash == ()


async def test_validated_native_facts_cannot_be_repacked_into_a_foreign_scope(
    projection_scenario: ContractPositionScenario,
) -> None:
    """Keep account identity attached even during conversion of already verified retained rows.

    Given: A real position event and its native claim belong to the selected margin account.
    When: The projection conversion receives those real rows with another declared margin scope.
    Then: It refuses to relabel the actual financial evidence as a foreign account.
    """
    await record_projection_position(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    foreign = projection_scenario.fill.admission.authority.scope.model_copy(
        update={"margin_account": "other-margin-profile"}
    )
    async with repository.session() as session:
        rows = tuple((await session.scalars(select(ContractAccountingEvent))).all())
        with pytest.raises(ContractAccountProjectionError, match="requested_scope"):
            _project_rows(rows, foreign)
