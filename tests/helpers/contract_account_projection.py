"""Real source-bound account projection fixtures without financial network operations."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import append_bound_financial_publication
from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import NativeCashLedgerWriteResult
from snapper.data.futures_cash_ledger_store import append_bound_futures_cash
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.unit_admission import admission_stamp


@pytest.fixture
async def projection_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[ContractPositionScenario]:
    """Seed real admission, raw receipts and pending holds in a private file-backed repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'projection.db'}")
    await repository.create_all()
    try:
        yield await seed_contract_position_scenario(repository, monkeypatch)
    finally:
        await repository.engine.dispose()


async def record_projection_position(
    scenario: ContractPositionScenario,
) -> BoundFinancialPublication:
    """Commit actual execution, unit, position, source claim and v1 notification together.

    Args:
        scenario: Actual admitted source fill and separately retained position account log.

    Returns:
        A real committed position publication with genuine financial target references.
    """
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, scenario.fill.admission.authority.scope.wallet_public_id
        )
        _, position = await stage_contract_position(session, scenario)
        publication = await append_bound_financial_publication(
            session, position.public_id, admission_stamp(601, _KNOWN + timedelta(seconds=2))
        )
        await session.commit()
    return publication


async def record_projection_cash(scenario: ContractPositionScenario) -> NativeCashLedgerWriteResult:
    """Commit one separately proven source currency row without inferring API-row pairing.

    Args:
        scenario: Actual isolated wallet and source-client namespace already used for the position.

    Returns:
        A genuine cash event from distinct native log ID18, with its own immutable claim.
    """
    raw_id = await capture_position_log(
        scenario.fill,
        310,
        position_log_entry(
            id=18,
            asset="USD",
            old_balance="100",
            new_balance="99.75",
            old_average_entry_price=0,
            new_average_entry_price=0,
            fee="0.25",
        ),
    )
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, scenario.fill.source_request.wallet_public_id
        )
        result = await append_bound_futures_cash(
            session,
            NativeCashAccountingRequest(
                wallet_public_id=scenario.fill.source_request.wallet_public_id,
                raw_receipt_public_id=raw_id,
                route_authority_public_id=scenario.fill.source_request.route_authority_public_id,
            ),
            admission_stamp(602, _KNOWN + timedelta(seconds=3)),
        )
        await session.commit()
    return result


async def read_projection(scenario: ContractPositionScenario) -> ContractAccountSnapshot:
    """Read the actual partial account under the real wallet fence and an explicit knowledge clock.

    Args:
        scenario: Actual isolated repository and exact declared account scope.

    Returns:
        A source-bound snapshot whose complete inventory and cash reconciliation remain unproven.
    """
    repository = scenario.fill.admission.repository
    scope = scenario.fill.admission.authority.scope
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, scope.wallet_public_id)
        return await load_bound_contract_account_snapshot(
            session, scope, _KNOWN + timedelta(seconds=10)
        )
