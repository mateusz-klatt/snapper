"""Prove native source identities cannot create contradictory position and cash facts."""

from collections.abc import AsyncIterator
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text

from snapper.data.accounting_native_source_store import AccountingNativeSourceError
from snapper.data.accounting_native_source_store import claim_new_accounting_source
from snapper.data.accounting_native_source_store import get_bound_native_source_claim
from snapper.data.accounting_native_source_triggers import install_accounting_native_source_triggers
from snapper.data.contract_position_ledger_store import append_bound_contract_position
from snapper.data.contract_position_ledger_store import get_bound_contract_position_event
from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import append_bound_futures_cash
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import refresh_admission_account


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ContractPositionScenario]:
    """Build genuine isolated admission, account, alias, original fill and native position evidence."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'source-consumption.db'}")
    await repository.create_all()
    try:
        yield await seed_contract_position_scenario(repository, monkeypatch)
    finally:
        await repository.engine.dispose()


async def test_one_native_log_id_cannot_be_consumed_as_both_position_and_cash(
    scenario: ContractPositionScenario,
) -> None:
    """Refuse contradictory changed-content redelivery across accounting event kinds.

    Given: Native account-log ID17 already established a genuine derivative position event.
    When: Another retained receipt reuses ID17 for a conflicting USD fee balance leg.
    Then: The second financial interpretation refuses and the original event remains immutable.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, position = await stage_contract_position(session, scenario)
        await session.commit()
    cash_id = await capture_position_log(
        scenario.fill,
        301,
        {
            **position_log_entry(),
            "asset": "USD",
            "old_balance": "0",
            "new_balance": "-0.1",
            "old_average_entry_price": 0,
            "new_average_entry_price": 0,
            "fee": "0.1",
        },
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, request.fill.wallet_public_id)
        with pytest.raises(AccountingNativeSourceError, match="already_consumed"):
            await append_bound_futures_cash(
                session,
                NativeCashAccountingRequest(
                    wallet_public_id=request.fill.wallet_public_id,
                    raw_receipt_public_id=cash_id,
                    route_authority_public_id=request.fill.route_authority_public_id,
                ),
                admission_stamp(501, _KNOWN + timedelta(seconds=2)),
            )
        await session.rollback()
    async with repository.session() as session:
        count = await session.scalar(select(func.count()).select_from(ContractAccountingEvent))
        receipts = await session.scalar(select(func.count()).select_from(AccountingRawReceipt))
    assert count == 1
    assert receipts == 3
    assert position.created is True


async def test_genuine_position_and_its_claim_commit_and_retry_as_one_fact(
    scenario: ContractPositionScenario,
) -> None:
    """Keep an exact retry bound to the original committed event and exclusive source claim.

    Given: A real position and its source claim commit in one financial transaction.
    When: The original source request is retried and its retained claim is reloaded.
    Then: One event, one claim and the original source/log/account references remain.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, first = await stage_contract_position(session, scenario)
        await session.commit()
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        second = await append_bound_contract_position(
            session, request, admission_stamp(501, _KNOWN + timedelta(seconds=2))
        )
        claim = await get_bound_native_source_claim(session, second.public_id)
        count = await session.scalar(select(func.count()).select_from(AccountingNativeSourceClaim))
        await session.commit()
    assert second.created is False
    assert second.public_id == first.public_id
    assert claim.accounting_event_public_id == first.public_id
    assert claim.raw_receipt_public_id == scenario.position_receipt_public_id
    assert claim.source_entry.source_log_id == 17
    assert claim.kind == "contract_position_fill"
    assert count == 1


async def test_financial_rollback_also_discards_the_flushed_native_source_claim(
    scenario: ContractPositionScenario,
) -> None:
    """Reserve source identity only as part of the actual financial transaction.

    Given: A real fill, native position and exclusive source claim flush together.
    When: The surrounding financial transaction rolls back.
    Then: No execution, financial seed, event or claim survives; original receipts remain.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        _, result = await stage_contract_position(session, scenario)
        claim = await get_bound_native_source_claim(session, result.public_id)
        assert claim.accounting_event_public_id == result.public_id
        await session.rollback()
    async with repository.session() as session:
        for model in (
            Execution,
            ContractAccountingAnchor,
            ContractAccountingEvent,
            AccountingNativeSourceClaim,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert await session.scalar(select(func.count()).select_from(AccountingRawReceipt)) == 2


async def test_changed_margin_cannot_fork_the_native_log_identity_namespace(
    scenario: ContractPositionScenario,
) -> None:
    """Require one native ID owner even when contradictory redelivery changes the margin label.

    Given: Log17 has already established a real position in the original margin account.
    When: A genuine later authority and raw cash leg reuse log17 in another margin account.
    Then: The shared account-family claim refuses the second fact despite its different scope.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, _ = await stage_contract_position(session, scenario)
        await session.commit()
    scope = scenario.fill.admission.authority.scope.model_copy(
        update={"margin_account": "futures.other"}
    )
    route = await refresh_admission_account(
        scenario.fill.admission,
        {"scope": scope},
        _KNOWN + timedelta(seconds=1),
        600,
    )
    raw_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            asset="USD",
            margin_account="futures.other",
            old_balance=0,
            new_balance="-0.1",
            old_average_entry_price=0,
            new_average_entry_price=0,
        ),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, request.fill.wallet_public_id)
        with pytest.raises(AccountingNativeSourceError, match="already_consumed"):
            await append_bound_futures_cash(
                session,
                NativeCashAccountingRequest(
                    wallet_public_id=request.fill.wallet_public_id,
                    raw_receipt_public_id=raw_id,
                    route_authority_public_id=route.authority_public_id,
                ),
                admission_stamp(501, _KNOWN + timedelta(seconds=2)),
            )
        await session.rollback()
    async with repository.session() as session:
        assert (
            await session.scalar(select(func.count()).select_from(AccountingNativeSourceClaim)) == 1
        )
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 1


async def test_orphaned_claim_after_incomplete_restore_cannot_certify_source_consumption(
    scenario: ContractPositionScenario,
) -> None:
    """Require a real retained financial event behind every exclusive source claim.

    Given: An actual committed claim survives but its financial event is lost in a damaged restore.
    When: Source ownership is independently loaded through the verified public claim reader.
    Then: The missing actual event refuses instead of turning the orphan claim into financial proof.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        _, result = await stage_contract_position(session, scenario)
        await session.commit()
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER contract_accounting_events_reject_delete"))
        await session.execute(text("DELETE FROM contract_accounting_events"))
        await session.commit()
    async with repository.session() as session:
        with pytest.raises(AccountingNativeSourceError, match="actual_event_missing"):
            await get_bound_native_source_claim(session, result.public_id)
        assert (
            await session.scalar(select(func.count()).select_from(AccountingNativeSourceClaim)) == 1
        )


async def test_distinct_native_log_ids_can_share_booking_and_execution_group(
    scenario: ContractPositionScenario,
) -> None:
    """Do not turn a provider booking group into per-leg source uniqueness.

    Given: A real position consumes native log17.
    When: Separate native cash log18 carries the same booking and execution group.
    Then: Two exclusive source claims commit while cash contains only its actual balance delta.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, _ = await stage_contract_position(session, scenario)
        await session.commit()
    cash_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            id=18,
            asset="USD",
            old_balance=0,
            new_balance="-0.1",
            old_average_entry_price=0,
            new_average_entry_price=0,
        ),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        result = await append_bound_futures_cash(
            session,
            NativeCashAccountingRequest(
                wallet_public_id=request.fill.wallet_public_id,
                raw_receipt_public_id=cash_id,
                route_authority_public_id=request.fill.route_authority_public_id,
            ),
            admission_stamp(501, _KNOWN + timedelta(seconds=2)),
        )
        await session.commit()
    assert result.observed_balance == Decimal("-0.1")
    async with repository.session() as session:
        claims = (await session.scalars(select(AccountingNativeSourceClaim))).all()
    assert {claim.source_log_id for claim in claims} == {"17", "18"}
    assert {claim.booking_uid for claim in claims} == {"Booking-Position-1"}


@pytest.mark.parametrize("replay", (False, True))
async def test_unclaimed_historical_native_events_are_not_silently_certified(
    scenario: ContractPositionScenario, replay: bool
) -> None:
    """Treat a claim-less legacy restore as unresolved ownership across the whole account family.

    Given: An actual retained position remains but its claim was lost in a damaged restore.
    When: Its public event is read or another native cash leg is submitted.
    Then: Both paths refuse and no replacement claim is invented.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, result = await stage_contract_position(session, scenario)
        await session.commit()
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_native_source_claims_reject_delete"))
        await session.execute(text("DELETE FROM accounting_native_source_claims"))
        connection = await session.connection()
        await connection.run_sync(
            install_accounting_native_source_triggers, "accounting_native_source_claims"
        )
        await session.commit()
    if replay:
        async with repository.session() as session:
            with pytest.raises(AccountingNativeSourceError, match="legacy_event_unclaimed"):
                await get_bound_contract_position_event(session, result.public_id)
        return
    raw_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            id=18,
            asset="USD",
            old_balance=0,
            new_balance="-0.1",
            old_average_entry_price=0,
            new_average_entry_price=0,
        ),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        with pytest.raises(AccountingNativeSourceError, match="legacy_family_history_unclaimed"):
            await append_bound_futures_cash(
                session,
                NativeCashAccountingRequest(
                    wallet_public_id=request.fill.wallet_public_id,
                    raw_receipt_public_id=raw_id,
                    route_authority_public_id=request.fill.route_authority_public_id,
                ),
                admission_stamp(501, _KNOWN + timedelta(seconds=2)),
            )
        await session.rollback()
    async with repository.session() as session:
        assert (
            await session.scalar(select(func.count()).select_from(AccountingNativeSourceClaim)) == 0
        )


async def test_claim_writer_cannot_certify_an_already_retained_financial_row(
    scenario: ContractPositionScenario,
) -> None:
    """Require new pending event ownership rather than accepting arbitrary historical UUIDs.

    Given: A genuine immutable position has already committed.
    When: The low-level claim writer is handed its persistent financial row.
    Then: It refuses instead of issuing another source-consumption witness.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        _, result = await stage_contract_position(session, scenario)
        await session.commit()
    async with repository.session() as session:
        event = await session.scalar(
            select(ContractAccountingEvent).where(
                ContractAccountingEvent.public_id == result.public_id
            )
        )
        assert event is not None
        with pytest.raises(AccountingNativeSourceError, match="requires_new_pending_event"):
            await claim_new_accounting_source(session, event)


@pytest.mark.parametrize(
    "field,value",
    (
        ("scope_digest", "f" * 64),
        ("account_scope_digest", "f" * 64),
        ("source_identity_digest", "f" * 64),
        ("semantic_digest", "f" * 64),
        ("content_digest", "f" * 64),
        ("source_log_id", "999"),
    ),
)
async def test_foreign_or_corrupt_claim_cannot_lift_legacy_family_barrier(
    scenario: ContractPositionScenario, field: str, value: str
) -> None:
    """Verify existing claim contents instead of trusting a matching accounting-event UUID.

    Given: A damaged restore leaves a claim pointing at an actual event with corrupted scope or body.
    When: Another genuinely retained cash leg seeks to consume a different native log ID.
    Then: The invalid prior ownership proof refuses the new fact across accounting kinds.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, _ = await stage_contract_position(session, scenario)
        await session.commit()
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_native_source_claims_reject_update"))
        await session.execute(
            text(f"UPDATE accounting_native_source_claims SET {field} = :value"),
            {"value": value},
        )
        connection = await session.connection()
        await connection.run_sync(
            install_accounting_native_source_triggers, "accounting_native_source_claims"
        )
        await session.commit()
    raw_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            id=18,
            asset="USD",
            old_balance=0,
            new_balance="-0.1",
            old_average_entry_price=0,
            new_average_entry_price=0,
        ),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        with pytest.raises(AccountingNativeSourceError, match="claim_projection_mismatch"):
            await append_bound_futures_cash(
                session,
                NativeCashAccountingRequest(
                    wallet_public_id=request.fill.wallet_public_id,
                    raw_receipt_public_id=raw_id,
                    route_authority_public_id=request.fill.route_authority_public_id,
                ),
                admission_stamp(501, _KNOWN + timedelta(seconds=2)),
            )
        await session.rollback()
    async with repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 1
