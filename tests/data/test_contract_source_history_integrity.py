"""Expose damaged native history that indexed family queries cannot safely omit."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text

from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import append_bound_futures_cash
from snapper.data.models import ContractAccountingEvent
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ContractPositionScenario]:
    """Persist genuine admitted source and native position proofs before explicit restore damage."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'history-integrity.db'}")
    await repository.create_all()
    try:
        yield await seed_contract_position_scenario(repository, monkeypatch)
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize(
    "field,value",
    (
        ("wallet_public_id", admission_id(800)),
        ("environment", "demo"),
        ("kind", "unknown_native_event"),
    ),
)
async def test_corrupt_legacy_event_indexes_cannot_hide_an_already_consumed_native_log(
    scenario: ContractPositionScenario, field: str, value: str
) -> None:
    """Derive historical source ownership from evidence instead of trusting exclusionary indexes.

    Given: A real position consumed log17 but restore damage lost its claim and changed one index.
    When: A contradictory cash entry tries to consume log17 in the original actual account family.
    Then: Missing or corrupt historical proof must refuse instead of creating a second native fact.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        request, _ = await stage_contract_position(session, scenario)
        await session.commit()
    async with repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_native_source_claims_reject_delete"))
        await session.execute(text("DELETE FROM accounting_native_source_claims"))
        await session.execute(text("DROP TRIGGER contract_accounting_events_reject_update"))
        await session.execute(
            text(f"UPDATE contract_accounting_events SET {field} = :value"),
            {"value": value},
        )
        await session.commit()
    raw_id = await capture_position_log(
        scenario.fill,
        301,
        position_log_entry(
            asset="USD",
            old_balance=0,
            new_balance="-0.1",
            old_average_entry_price=0,
            new_average_entry_price=0,
        ),
    )
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, request.fill.wallet_public_id)
        with pytest.raises(ValueError):
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
