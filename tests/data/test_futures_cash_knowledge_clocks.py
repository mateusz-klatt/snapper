"""Require real cash posting knowledge to follow actual retained source and account proof."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import bindparam
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text

from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import append_futures_cash_from_receipt
from snapper.data.futures_cash_ledger_store import load_observed_futures_cash
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import TZDateTime
from snapper.data.repository import SQLAlchemyRepository
from tests.data.test_futures_cash_ledger_store import _NOW
from tests.data.test_futures_cash_ledger_store import _capture
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import refresh_admission_account
from tests.helpers.unit_admission import seed_admission_scenario


@pytest.fixture
async def scenario(tmp_path: Path) -> AsyncIterator[AdmissionScenario]:
    """Use actual raw receipts and account authority rows in an isolated cash ledger."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'cash-clock.db'}")
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    finally:
        await repository.engine.dispose()


async def _future_knowledge_request(
    scenario: AdmissionScenario,
    proof: Literal["raw_observed", "raw_retained", "authority_observed", "authority_retained"],
) -> NativeCashAccountingRequest:
    """Retain independently chosen source observation and database insertion clocks."""
    future = _NOW + timedelta(seconds=2)
    if proof == "authority_observed":
        route = await refresh_admission_account(scenario, {}, future, 600)
        return await _capture(scenario, 300, raw_updates={"observed_at": _NOW}, route=route)
    request = await _capture(scenario, 300)
    if proof == "authority_retained":
        payload = scenario.authority.model_copy(
            update={"valid_until": scenario.authority.valid_until - timedelta(seconds=1)}
        )
        authority = await insert_execution_route_authority(
            scenario.repository, payload, admission_stamp(600, future)
        )
        return request.model_copy(update={"route_authority_public_id": authority.public_id})
    async with scenario.repository.session() as session:
        raw = await get_bound_accounting_observation(session, request.raw_receipt_public_id)
    observed_at = future if proof == "raw_observed" else _NOW
    receipt = await capture_accounting_observation(
        scenario.repository,
        raw.model_copy(update={"observed_at": observed_at}),
        AccountingWriteProvenance.model_validate(admission_stamp(301, future).model_dump()),
    )
    return request.model_copy(update={"raw_receipt_public_id": receipt.public_id})


@pytest.mark.parametrize(
    "proof",
    ("raw_observed", "raw_retained", "authority_observed", "authority_retained"),
)
async def test_cash_cannot_be_posted_before_actual_source_or_authority_knowledge(
    scenario: AdmissionScenario,
    proof: Literal["raw_observed", "raw_retained", "authority_observed", "authority_retained"],
) -> None:
    """Do not make later raw or account evidence appear known at an earlier posting clock.

    Given: One actual source or authority observation/retention timestamp follows the posting clock.
    When: A financial writer tries to append the otherwise valid native cash leg.
    Then: It refuses and leaves no financial event or synthetic earlier ownership evidence.
    """
    request = await _future_knowledge_request(scenario, proof)
    with pytest.raises(ValueError, match="knowledge"):
        await append_futures_cash_from_receipt(
            scenario.repository,
            request,
            admission_stamp(400, _NOW + timedelta(seconds=1)),
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 0


async def test_cash_knowledge_cannot_move_backwards_along_a_conserved_native_chain(
    scenario: AdmissionScenario,
) -> None:
    """Keep local knowledge monotonic independently of native log economic ordering.

    Given: The first native cash entry was actually posted at local knowledge time10.
    When: A later native log with a conserved old balance is assigned local knowledge time5.
    Then: The new posting refuses rather than exposing the later fact before its predecessor.
    """
    first = await _capture(scenario, 300)
    await append_futures_cash_from_receipt(
        scenario.repository, first, admission_stamp(400, _NOW + timedelta(seconds=10))
    )
    second = await _capture(scenario, 301, {"id": 18, "old_balance": "10.75", "new_balance": "11"})
    with pytest.raises(ValueError, match="knowledge"):
        await append_futures_cash_from_receipt(
            scenario.repository,
            second,
            admission_stamp(401, _NOW + timedelta(seconds=5)),
        )
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 1


@pytest.mark.parametrize(
    "table",
    ("accounting_raw_receipts", "execution_route_authorities", "contract_accounting_anchors"),
)
async def test_cash_replay_cannot_hide_later_source_or_anchor_retention(
    scenario: AdmissionScenario, table: str
) -> None:
    """Verify actual retained clocks again when rebuilding recorded financial facts.

    Given: A committed cash chain whose source or opening retention clock was damaged by restore.
    When: Replay sees that the raw receipt, authority or anchor was retained after the event.
    Then: It refuses the false historical knowledge instead of publishing the original cash value.
    """
    request = await _capture(scenario, 300)
    await append_futures_cash_from_receipt(scenario.repository, request, admission_stamp(400, _NOW))
    async with scenario.repository.session() as session:
        await session.execute(text(f"DROP TRIGGER {table}_reject_update"))
        await session.execute(
            text(f"UPDATE {table} SET timestamp = :timestamp").bindparams(
                bindparam("timestamp", type_=TZDateTime())
            ),
            {"timestamp": _NOW + timedelta(seconds=1)},
        )
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(ValueError, match="knowledge"):
            await load_observed_futures_cash(session, scenario.authority.scope)
