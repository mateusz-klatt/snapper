"""Real paired private-fill/position-log fixtures for accounting and financial outbox tests."""

import base64
import json
from dataclasses import dataclass
from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.application.trade.unit_snapshot_submission as admission_producer
from snapper.application.trade.unit_snapshot_fill import FillUnitCaptureRequest
from snapper.application.trade.unit_snapshot_fill import FuturesFillSourceRequest
from snapper.application.trade.unit_snapshot_fill import capture_bound_futures_fill_units
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.contract_position_ledger_store import ContractPositionAccountingRequest
from snapper.data.contract_position_ledger_store import ContractPositionLedgerWriteResult
from snapper.data.contract_position_ledger_store import append_bound_contract_position
from snapper.data.repository import SQLAlchemyRepository
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.application.trade.test_unit_snapshot_fill import _NOW
from tests.application.trade.test_unit_snapshot_fill import _OBSERVED
from tests.application.trade.test_unit_snapshot_fill import _VENUE
from tests.application.trade.test_unit_snapshot_fill import _Clock
from tests.application.trade.test_unit_snapshot_fill import _observation
from tests.application.trade.test_unit_snapshot_fill import _Scenario
from tests.application.trade.test_unit_snapshot_fill import _seed_order_history
from tests.application.trade.test_unit_snapshot_fill import _stage_targets
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import seed_admission_scenario


@dataclass(frozen=True)
class ContractPositionScenario:
    """An actual admitted command and two independently committed native source receipts."""

    fill: _Scenario
    position_receipt_public_id: str


def position_log_entry(**updates: object) -> dict[str, object]:
    """Provide an explicit native old/new position associated with one actual source fill.

    Args:
        **updates: Explicit provider fields changed for the isolated source scenario.

    Returns:
        A native wire-entry object with independently supplied old and new positions.
    """
    return {
        "id": 17,
        "date": (_VENUE + timedelta(milliseconds=10)).isoformat(),
        "asset": "PF_XBTUSD",
        "contract": "PF_XBTUSD",
        "collateral": "USD",
        "margin_account": "futures.flex",
        "booking_uid": "Booking-Position-1",
        "execution": "native-fill-1",
        "info": "futures trade",
        "old_balance": 0,
        "new_balance": 1,
        "old_average_entry_price": 0,
        "new_average_entry_price": 100,
        **updates,
    }


async def capture_position_log(scenario: _Scenario, number: int, entry: dict[str, object]) -> str:
    """Commit the original private account-log frame and exact selector before interpretation.

    Args:
        scenario: Actual isolated source client/account and admitted order.
        number: Independent local receipt identity and insertion sequence.
        entry: Native wire fields retained without financial interpretation.

    Returns:
        The actual committed immutable raw receipt UUID with its durable pending hold.
    """
    base = _observation(scenario.admission)
    wire = json.dumps({"feed": "account_log", "new_entry": entry}).encode()
    observation = RawAccountingObservation.model_validate(
        {
            **base.model_dump(),
            "route_authority_public_id": None,
            "kind": "account_cash",
            "channel": "account_log",
            "material": base64.b64encode(wire).decode(),
            "target_json_pointer": "/new_entry",
            "observed_at": _KNOWN + timedelta(milliseconds=500),
        }
    )
    result = await capture_accounting_observation(
        scenario.admission.repository,
        observation,
        AccountingWriteProvenance.model_validate(
            admission_stamp(number, _KNOWN + timedelta(milliseconds=500)).model_dump()
        ),
    )
    return result.public_id


async def seed_contract_position_scenario(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> ContractPositionScenario:
    """Persist genuine isolated admission, order, original fill bytes and paired native position log.

    Args:
        repository: Actual private test repository whose schema has already been created.
        monkeypatch: Test-owned clock override restored by pytest after the complete test.

    Returns:
        Real source references before any execution, fill unit, position or outbox row exists.
    """
    monkeypatch.setattr(admission_producer, "datetime", _Clock)
    admission = await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    await admission_producer.capture_submission_unit_admission(
        repository, admission.order, admission.route, admission_identity()
    )
    await _seed_order_history(repository)
    raw = await capture_accounting_observation(
        repository,
        _observation(admission),
        AccountingWriteProvenance.model_validate(admission_stamp(202, _OBSERVED).model_dump()),
    )
    fill = _Scenario(
        admission,
        FuturesFillSourceRequest(
            wallet_public_id=admission_id(3),
            raw_receipt_public_id=raw.public_id,
            route_authority_public_id=admission.route.authority_public_id,
        ),
    )
    position_id = await capture_position_log(fill, 300, position_log_entry())
    return ContractPositionScenario(fill, position_id)


async def create_position_units(
    session: AsyncSession, scenario: ContractPositionScenario, unit_sequence: int = 400
) -> tuple[FillUnitCaptureRequest, str]:
    """Create actual execution/event rows and a real native unit witness without an inner commit.

    Args:
        session: Existing real caller-owned financial transaction and SQLite write reservation.
        scenario: Genuine admitted order and original retained source receipts.
        unit_sequence: Independent immutable unit witness identity for this actual fill.

    Returns:
        The real target references and flushed unit UUID, with no inner commit or publication.
    """
    fill = await _stage_targets(session, scenario.fill)
    captured = await capture_bound_futures_fill_units(
        session, fill, admission_stamp(unit_sequence, _KNOWN)
    )
    return fill, captured.result.public_id


async def stage_contract_position(
    session: AsyncSession, scenario: ContractPositionScenario, sequence: int = 500
) -> tuple[ContractPositionAccountingRequest, ContractPositionLedgerWriteResult]:
    """Flush real execution, event, unit proof and position inside the caller's actual transaction.

    Args:
        session: Existing real transaction whose SQLite write reservation has already started.
        scenario: Actual paired source receipts and durable admitted order.
        sequence: Independent immutable position event identity for the isolated test.

    Returns:
        The real accounting request and flushed outcome, without an inner commit or publication.
    """
    fill, unit_id = await create_position_units(session, scenario)
    request = ContractPositionAccountingRequest(
        fill=fill,
        unit_snapshot_public_id=unit_id,
        position_receipt_public_id=scenario.position_receipt_public_id,
    )
    result = await append_bound_contract_position(
        session, request, admission_stamp(sequence, _KNOWN + timedelta(seconds=1))
    )
    return request, result
