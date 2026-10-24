"""Bind real source executions, observed derivative positions and native cash in one transaction."""

import base64
import json
from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text

from snapper.application.portfolio.contract_position_ledger import CURRENT_POSITION_DECIMAL_POLICY
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.data.contract_accounting_store import AccountingScopeUnavailable
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.contract_accounting_store import require_accounting_scope_available
from snapper.data.contract_position_ledger_store import ContractPositionAccountingError
from snapper.data.contract_position_ledger_store import ContractPositionAccountingRequest
from snapper.data.contract_position_ledger_store import append_bound_contract_position
from snapper.data.contract_position_ledger_store import find_bound_contract_position_event
from snapper.data.contract_position_ledger_store import get_bound_contract_position_event
from snapper.data.contract_position_ledger_store import load_observed_contract_position
from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import append_bound_futures_cash
from snapper.data.futures_cash_ledger_store import load_observed_futures_cash
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import Base
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.application.trade.test_unit_snapshot_fill import _VENUE
from tests.application.trade.test_unit_snapshot_fill import _observation
from tests.application.trade.test_unit_snapshot_fill import _Scenario
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import create_position_units
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ContractPositionScenario]:
    """Use genuine temporal wallet, order, admission, source, alias and account observations."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'position.db'}")
    await repository.create_all()
    try:
        yield await seed_contract_position_scenario(repository, monkeypatch)
    finally:
        await repository.engine.dispose()


async def _count(repository: SQLAlchemyRepository, model: type[Base]) -> int:
    """Read independent committed row counts rather than trusting the caller's flushed state."""
    async with repository.session() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


async def test_actual_position_is_recorded_and_replayed_without_spot_cash_principal(
    scenario: ContractPositionScenario,
) -> None:
    """Persist source quantity and basis against the real execution and native unit witness.

    Given: One admitted real source fill and its separately retained native position log.
    When: All financial rows are committed in one actual fenced transaction.
    Then: Position, clocks, profile and source links replay exactly while no cash event appears.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        request = ContractPositionAccountingRequest(
            fill=fill,
            unit_snapshot_public_id=unit_id,
            position_receipt_public_id=scenario.position_receipt_public_id,
        )
        result = await append_bound_contract_position(
            session, request, admission_stamp(500, _KNOWN + timedelta(seconds=1))
        )
        verified = await get_bound_contract_position_event(session, result.public_id)
        assert verified.decimal_policy == CURRENT_POSITION_DECIMAL_POLICY
        assert verified.execution_public_id == fill.execution_public_id
        assert verified.executed_at == _VENUE
        assert verified.execution_known_at == _KNOWN
        assert verified.source_position.source_entry.occurred_at == _VENUE + timedelta(
            milliseconds=10
        )
        await session.commit()
    async with repository.session() as session:
        replayed = await load_observed_contract_position(
            session, scenario.fill.admission.authority.scope, admission_id(2)
        )
        found = await find_bound_contract_position_event(
            session, scenario.fill.admission.authority.scope, "native-fill-1"
        )
        assert (
            await load_observed_futures_cash(session, scenario.fill.admission.authority.scope) == ()
        )
    assert result.created is True
    assert result.observed.quantity == Decimal(1)
    assert result.observed.entry_price == Decimal(100)
    assert result.observed.realized_pnl_since_anchor == 0
    assert result.observed.quote_turnover_since_anchor == Decimal(100)
    assert result.basis_status == "equal"
    assert replayed is not None
    assert replayed.public_id == result.public_id
    assert found is not None
    assert found.unit_snapshot_public_id == unit_id
    assert await _count(repository, ContractAccountingAnchor) == 1
    assert await _count(repository, ContractAccountingEvent) == 1
    assert await _count(repository, AccountingHoldResolution) == 0


async def test_first_position_seed_is_observed_nonflat_state_and_retains_basis_precision_difference(
    scenario: ContractPositionScenario,
) -> None:
    """Never seed a derivative position flat merely because local accounting has no earlier event.

    Given: The source position already contains two native units with a known basis.
    When: Its next real fill adds one unit and reports a rounded native average entry price.
    Then: The old position is the explicit seed and observed/derived bases remain separate.
    """
    position_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            old_balance=2,
            new_balance=3,
            old_average_entry_price=90,
            new_average_entry_price="93.33333333333333",
        ),
    )
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        result = await append_bound_contract_position(
            session,
            ContractPositionAccountingRequest(
                fill=fill, unit_snapshot_public_id=unit_id, position_receipt_public_id=position_id
            ),
            admission_stamp(500, _KNOWN + timedelta(seconds=1)),
        )
        payload = await get_bound_contract_position_event(session, result.public_id)
        await session.commit()
    assert payload.before.quantity == Decimal(2)
    assert payload.before.entry_price == Decimal(90)
    assert result.observed.quantity == Decimal(3)
    assert result.observed.entry_price == Decimal("93.33333333333333")
    assert result.derived.entry_price != result.observed.entry_price
    assert result.basis_status == "source_precision_unproven"
    assert result.observed.realized_pnl_since_anchor == 0


async def test_buy_reduces_an_observed_short_and_realizes_settlement_pnl_without_principal_cash(
    scenario: ContractPositionScenario,
) -> None:
    """Use the signed source position rather than treating every buy as a spot principal purchase.

    Given: A real native source position is short two units at an entry of 110.
    When: The actual buy of one unit at 100 is committed with its unit witness.
    Then: One short unit remains and economic P&L is positive ten, without a cash posting.
    """
    position_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            old_balance=-2,
            new_balance=-1,
            old_average_entry_price=110,
            new_average_entry_price=110,
        ),
    )
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        result = await append_bound_contract_position(
            session,
            ContractPositionAccountingRequest(
                fill=fill, unit_snapshot_public_id=unit_id, position_receipt_public_id=position_id
            ),
            admission_stamp(500, _KNOWN + timedelta(seconds=1)),
        )
        await session.commit()
    assert result.observed.quantity == Decimal(-1)
    assert result.observed.entry_price == Decimal(110)
    assert result.observed.realized_pnl_since_anchor == Decimal(10)
    assert result.observed.quote_turnover_since_anchor == Decimal(100)
    async with repository.session() as session:
        assert (
            await load_observed_futures_cash(session, scenario.fill.admission.authority.scope) == ()
        )


async def test_multiple_actual_fills_replay_one_conserved_source_position_chain(
    scenario: ContractPositionScenario,
) -> None:
    """Keep real source executions separate while preserving one seeded native position pool.

    Given: Two distinct original fill IDs and paired native log IDs form a continuous position.
    When: Each real execution/unit/position transaction commits and the entire chain is reloaded.
    Then: Both fills persist once against one source seed and turnover accumulates exactly.
    """
    repository = scenario.fill.admission.repository
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
        repository,
        observation,
        AccountingWriteProvenance.model_validate(admission_stamp(302, _KNOWN).model_dump()),
    )
    second_fill = _Scenario(
        scenario.fill.admission,
        FuturesFillSourceRequest(
            **{
                **scenario.fill.source_request.model_dump(),
                "raw_receipt_public_id": raw.public_id,
            }
        ),
    )
    position_id = await capture_position_log(
        second_fill,
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
    results = []
    for index, current in enumerate((scenario, ContractPositionScenario(second_fill, position_id))):
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            fill, unit_id = await create_position_units(session, current, unit_sequence=400 + index)
            results.append(
                await append_bound_contract_position(
                    session,
                    ContractPositionAccountingRequest(
                        fill=fill,
                        unit_snapshot_public_id=unit_id,
                        position_receipt_public_id=current.position_receipt_public_id,
                    ),
                    admission_stamp(500 + index, _KNOWN + timedelta(seconds=1)),
                )
            )
            await session.commit()
    assert results[0].anchor_public_id == results[1].anchor_public_id
    async with repository.session() as session:
        replayed = await load_observed_contract_position(
            session, scenario.fill.admission.authority.scope, admission_id(2)
        )
    assert replayed is not None
    assert replayed.observed.quantity == Decimal(2)
    assert replayed.observed.quote_turnover_since_anchor == Decimal(200)
    assert replayed.ledger_sequence == 2
    assert await _count(repository, Execution) == 2
    assert await _count(repository, ContractAccountingAnchor) == 1


async def test_source_position_and_native_cash_can_commit_together_without_double_posting_fee(
    scenario: ContractPositionScenario,
) -> None:
    """Separate a real position increment from the native currency balance that paid its fee.

    Given: A position log and USD cash log belong to the same source booking and actual fill.
    When: Execution, unit proof, position and native cash are flushed in one transaction.
    Then: Only the observed cash delta debits money; no trade principal or extra fee is posted.
    """
    cash_id = await capture_position_log(
        scenario.fill,
        301,
        {
            **position_log_entry(),
            "id": 18,
            "asset": "USD",
            "old_balance": "1000",
            "new_balance": "999.90",
            "old_average_entry_price": 0,
            "new_average_entry_price": 0,
            "fee": "0.10",
        },
    )
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        position = await append_bound_contract_position(
            session,
            ContractPositionAccountingRequest(
                fill=fill,
                unit_snapshot_public_id=unit_id,
                position_receipt_public_id=scenario.position_receipt_public_id,
            ),
            admission_stamp(500, _KNOWN + timedelta(seconds=1)),
        )
        cash = await append_bound_futures_cash(
            session,
            NativeCashAccountingRequest(
                wallet_public_id=fill.wallet_public_id,
                raw_receipt_public_id=cash_id,
                route_authority_public_id=fill.route_authority_public_id,
            ),
            admission_stamp(501, _KNOWN + timedelta(seconds=1)),
        )
        await session.commit()
    assert position.observed.quantity == 1
    assert cash.native_currency == "USD"
    assert cash.observed_balance == Decimal("999.90")
    assert position.ledger_sequence == 1
    assert cash.ledger_sequence == 2
    assert await _count(repository, ContractAccountingEvent) == 2
    async with repository.session() as session:
        with pytest.raises(AccountingScopeUnavailable):
            await require_accounting_scope_available(
                session, _observation(scenario.fill.admission).scope
            )


async def test_outer_rollback_discards_every_financial_row_but_preserves_committed_source_evidence(
    scenario: ContractPositionScenario,
) -> None:
    """Make actual execution, unit and accounting writes share the caller's atomic boundary.

    Given: Raw fill and native position receipts already committed with durable holds.
    When: The composed execution/unit/position transaction is rolled back after successful flush.
    Then: No financial target survives and both original source receipts remain intact.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        await append_bound_contract_position(
            session,
            ContractPositionAccountingRequest(
                fill=fill,
                unit_snapshot_public_id=unit_id,
                position_receipt_public_id=scenario.position_receipt_public_id,
            ),
            admission_stamp(500, _KNOWN + timedelta(seconds=1)),
        )
        await session.rollback()
    for model in (Execution, VenueEvent, ContractAccountingAnchor, ContractAccountingEvent):
        assert await _count(repository, model) == 0
    assert await _count(repository, AccountingRawReceipt) == 2
    async with repository.session() as session:
        fills = await session.scalar(
            select(func.count())
            .select_from(ExecutionUnitSnapshot)
            .where(ExecutionUnitSnapshot.phase == "fill")
        )
    assert fills == 0


async def test_exact_position_retry_reuses_one_actual_execution_event_and_source_seed(
    scenario: ContractPositionScenario,
) -> None:
    """Keep retry identity separate from a new financial fact.

    Given: An actual execution and source-bound position event already committed.
    When: The original target references are processed again with new insertion metadata.
    Then: The immutable existing outcome is reused with created false.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        request = ContractPositionAccountingRequest(
            fill=fill,
            unit_snapshot_public_id=unit_id,
            position_receipt_public_id=scenario.position_receipt_public_id,
        )
        first = await append_bound_contract_position(
            session, request, admission_stamp(500, _KNOWN + timedelta(seconds=1))
        )
        await session.commit()
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, fill.wallet_public_id)
        second = await append_bound_contract_position(
            session, request, admission_stamp(501, _KNOWN + timedelta(seconds=2))
        )
        await session.commit()
    assert first.public_id == second.public_id
    assert second.created is False
    assert await _count(repository, ContractAccountingEvent) == 1
    assert await _count(repository, ContractAccountingAnchor) == 1


async def test_resnapshot_position_redelivery_reuses_original_fact_after_validating_new_receipt(
    scenario: ContractPositionScenario,
) -> None:
    """Separate transport selector changes from the native position event's actual identity.

    Given: A native position was originally booked from a live /new_entry source.
    When: The same complete native event is later retained at /logs/1 in a snapshot frame.
    Then: Its new receipt is verified and the original immutable financial fact is reused once.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        original_request = ContractPositionAccountingRequest(
            fill=fill,
            unit_snapshot_public_id=unit_id,
            position_receipt_public_id=scenario.position_receipt_public_id,
        )
        first = await append_bound_contract_position(
            session, original_request, admission_stamp(500, _KNOWN + timedelta(seconds=1))
        )
        await session.commit()
    frame = {
        "feed": "account_log_snapshot",
        "logs": [position_log_entry(id=999, new_balance=999), position_log_entry()],
    }
    observed_at = _KNOWN + timedelta(milliseconds=1500)
    raw = await capture_accounting_observation(
        repository,
        RawAccountingObservation.model_validate(
            {
                **_observation(scenario.fill.admission).model_dump(),
                "kind": "account_cash",
                "channel": "account_log",
                "route_authority_public_id": None,
                "material": base64.b64encode(json.dumps(frame).encode()).decode(),
                "target_json_pointer": "/logs/1",
                "observed_at": observed_at,
            }
        ),
        AccountingWriteProvenance.model_validate(admission_stamp(302, observed_at).model_dump()),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, fill.wallet_public_id)
        second = await append_bound_contract_position(
            session,
            original_request.model_copy(update={"position_receipt_public_id": raw.public_id}),
            admission_stamp(501, _KNOWN + timedelta(seconds=2)),
        )
        await session.commit()
    assert second.public_id == first.public_id
    assert second.created is False
    async with repository.session() as session:
        event = await get_bound_contract_position_event(session, first.public_id)
    assert event.source_position.source_entry.target_json_pointer == "/new_entry"
    assert event.position_receipt_public_id == scenario.position_receipt_public_id
    assert await _count(repository, ContractAccountingEvent) == 1
    assert await _count(repository, AccountingRawReceipt) == 3
    assert await _count(repository, AccountingHoldResolution) == 0


@pytest.mark.parametrize(
    "changes",
    (
        {"execution": "foreign-source-execution"},
        {"asset": "USD", "contract": "PF_XBTUSD"},
        {"asset": "pf_xbtusd", "contract": "PF_XBTUSD"},
        {"new_balance": 2},
        {"margin_account": "foreign-margin-account"},
    ),
)
async def test_unproven_source_position_cannot_create_or_replace_a_financial_book(
    scenario: ContractPositionScenario, changes: dict[str, object]
) -> None:
    """Refuse wrong source identity, cash units, alias case inference and cumulative fill aggregation.

    Given: Retained native position material contradicts the actual source execution or units.
    When: The real composed transaction tries to interpret it.
    Then: No financial target commits and all original raw evidence survives for reconciliation.
    """
    raw_id = await capture_position_log(scenario.fill, 301, {**position_log_entry(), **changes})
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        with pytest.raises(ValueError):
            await append_bound_contract_position(
                session,
                ContractPositionAccountingRequest(
                    fill=fill, unit_snapshot_public_id=unit_id, position_receipt_public_id=raw_id
                ),
                admission_stamp(500, _KNOWN + timedelta(seconds=1)),
            )
        await session.rollback()
    assert await _count(repository, ContractAccountingEvent) == 0
    assert await _count(repository, Execution) == 0
    assert await _count(repository, AccountingRawReceipt) == 3


async def test_missing_position_book_remains_unknown_instead_of_becoming_flat(
    scenario: ContractPositionScenario,
) -> None:
    """An authenticated route and source receipt alone do not establish a zero position.

    Given: No committed native position seed or position event exists.
    When: Current observed state or an unknown source fill is requested.
    Then: Both reads return unknown and an absent actual event UUID refuses verification.
    """
    repository = scenario.fill.admission.repository
    scope = scenario.fill.admission.authority.scope
    async with repository.session() as session:
        assert await load_observed_contract_position(session, scope, admission_id(2)) is None
        assert await find_bound_contract_position_event(session, scope, "unknown-source") is None
        with pytest.raises(ContractPositionAccountingError, match="position_event_missing"):
            await get_bound_contract_position_event(session, admission_id(999))


async def test_missing_actual_execution_cannot_be_replaced_with_a_position_only_record(
    scenario: ContractPositionScenario,
) -> None:
    """Require a real source execution rather than accepting caller-provided target identities.

    Given: Actual source/unit rows exist for a different immutable execution UUID.
    When: A position request names an absent execution target.
    Then: It refuses and the outer rollback retains only the original source evidence.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        missing = fill.model_copy(update={"execution_public_id": admission_id(999)})
        with pytest.raises(ContractPositionAccountingError, match="actual_execution_missing"):
            await append_bound_contract_position(
                session,
                ContractPositionAccountingRequest(
                    fill=missing,
                    unit_snapshot_public_id=unit_id,
                    position_receipt_public_id=scenario.position_receipt_public_id,
                ),
                admission_stamp(500, _KNOWN + timedelta(seconds=1)),
            )
        await session.rollback()
    assert await _count(repository, ContractAccountingEvent) == 0


async def test_lost_source_seed_after_incomplete_restore_refuses_financial_publication(
    scenario: ContractPositionScenario,
) -> None:
    """Verify the original source anchor whenever an outbox consumer binds a position event.

    Given: A genuine committed financial event in a disposable isolated database.
    When: Deliberate fixture corruption removes its retained native opening seed.
    Then: Event verification refuses instead of publishing a guessed zero predecessor.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        result = await append_bound_contract_position(
            session,
            ContractPositionAccountingRequest(
                fill=fill,
                unit_snapshot_public_id=unit_id,
                position_receipt_public_id=scenario.position_receipt_public_id,
            ),
            admission_stamp(500, _KNOWN + timedelta(seconds=1)),
        )
        await session.commit()
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER contract_accounting_anchors_reject_delete"))
        await session.execute(text("DELETE FROM contract_accounting_anchors"))
        await session.commit()
    async with repository.session() as session:
        with pytest.raises(ContractPositionAccountingError, match="source_anchor_missing"):
            await get_bound_contract_position_event(session, result.public_id)


@pytest.mark.parametrize("proof", ("position_raw", "unit"))
async def test_financial_position_cannot_predate_actual_retention_of_log_or_unit_evidence(
    scenario: ContractPositionScenario, proof: str
) -> None:
    """Check actual retained row clocks independently of earlier claimed observation times.

    Given: Source payload clocks are valid but a retained proof row lies after posting knowledge.
    When: The composed financial transaction tries to record the position.
    Then: It refuses without backdating the later native log or unit witness.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        fill, unit_id = await create_position_units(session, scenario)
        if proof == "position_raw":
            table = "accounting_raw_receipts"
            row_id = await session.scalar(
                select(AccountingRawReceipt.id).where(
                    AccountingRawReceipt.public_id == scenario.position_receipt_public_id
                )
            )
        else:
            table = "execution_unit_snapshots"
            row_id = await session.scalar(
                select(ExecutionUnitSnapshot.id).where(ExecutionUnitSnapshot.public_id == unit_id)
            )
        assert row_id is not None
        await session.execute(text(f"DROP TRIGGER {table}_reject_update"))
        await session.execute(
            text(f"UPDATE {table} SET timestamp = :retained WHERE id = :row_id"),
            {
                "retained": (_KNOWN + timedelta(seconds=2)).strftime("%Y-%m-%d %H:%M:%S.%f"),
                "row_id": row_id,
            },
        )
        with pytest.raises(
            ContractPositionAccountingError, match="precedes_retained_source_knowledge"
        ):
            await append_bound_contract_position(
                session,
                ContractPositionAccountingRequest(
                    fill=fill,
                    unit_snapshot_public_id=unit_id,
                    position_receipt_public_id=scenario.position_receipt_public_id,
                ),
                admission_stamp(500, _KNOWN + timedelta(seconds=1)),
            )
        await session.rollback()
    assert await _count(repository, ContractAccountingEvent) == 0
