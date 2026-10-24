"""Keep actual receipt lifetime proofs ahead of publication exclusion and page limits."""

import base64
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal
from uuid import uuid7

import pytest
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import Connection

from snapper.application.portfolio.contract_account_projection import ContractAccountProjection
from snapper.application.portfolio.versioned_contract_account_projection import (
    VersionedContractAccountProjection,
)
from snapper.application.trade.futures_fill_recording import FuturesPositionRecordingRequest
from snapper.application.trade.futures_fill_recording import record_futures_position
from snapper.application.trade.versioned_financial_publication import (
    publish_pending_versioned_financial_notifications,
)
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.financial_publication_delivery import load_pending_financial_publications
from snapper.data.financial_publication_delivery import record_financial_publication_delivery
from snapper.data.financial_publication_store import BoundFinancialPublication
from snapper.data.financial_publication_store import FinancialPublicationError
from snapper.data.financial_publication_triggers import install_financial_publication_triggers
from snapper.data.models import FinancialPublicationDelivery
from snapper.data.models import FinancialPublicationIntent
from snapper.data.versioned_financial_publication_delivery import (
    load_pending_versioned_financial_publications,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.application.trade.test_unit_snapshot_fill import _KNOWN
from tests.application.trade.test_unit_snapshot_fill import _observation
from tests.helpers.contract_account_projection import (
    projection_scenario as shared_projection_scenario,
)
from tests.helpers.contract_account_projection import record_projection_position
from tests.helpers.contract_position_accounting import ContractPositionScenario
from tests.helpers.contract_position_accounting import capture_position_log
from tests.helpers.contract_position_accounting import position_log_entry
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.helpers.unit_admission import admission_stamp

pytestmark = pytest.mark.asyncio
projection_scenario = shared_projection_scenario
_FINITE_FUTURE = datetime(9998, 1, 1, tzinfo=UTC)


async def _next_legacy_publication(
    scenario: ContractPositionScenario, number: Literal[2, 3]
) -> BoundFinancialPublication:
    """Finish the actual two-unit order with two separate half-unit source fills."""
    original = _observation(scenario.fill.admission)
    wire = base64.b64decode(original.material)
    remaining = b'"0.5"' if number == 2 else b'"0"'
    replacements = (
        (b'"fill_id": "native-fill-1"', f'"fill_id": "native-fill-{number}"'.encode()),
        (b'"seq": 12', f'"seq": {11 + number}'.encode()),
        (b'"qty": 1', b'"qty": "0.5"'),
        (b'"remaining_order_qty": 1', b'"remaining_order_qty": ' + remaining),
    )
    for before, after in replacements:
        assert wire.count(before) == 1
        wire = wire.replace(before, after)
    observed = RawAccountingObservation.model_validate(
        {**original.model_dump(mode="python"), "material": base64.b64encode(wire).decode()}
    )
    repository = scenario.fill.admission.repository
    raw = await capture_accounting_observation(
        repository,
        observed,
        AccountingWriteProvenance.model_validate(
            admission_stamp(93000 + number, _KNOWN + timedelta(seconds=number)).model_dump()
        ),
    )
    position_id = await capture_position_log(
        scenario.fill,
        94000 + number,
        position_log_entry(
            id=16 + number,
            booking_uid=f"Booking-Publication-{number}",
            execution=f"native-fill-{number}",
            old_balance="1" if number == 2 else "1.5",
            new_balance="1.5" if number == 2 else "2",
            old_average_entry_price=100,
            new_average_entry_price=100,
        ),
    )
    request = FuturesPositionRecordingRequest(
        wallet_public_id=scenario.fill.source_request.wallet_public_id,
        raw_receipt_public_id=raw.public_id,
        route_authority_public_id=scenario.fill.source_request.route_authority_public_id,
        position_receipt_public_id=position_id,
    )
    recorded = await record_futures_position(
        repository,
        request,
        admission_stamp(95000 + number, _KNOWN + timedelta(seconds=number + 2)),
    )
    assert recorded.created and recorded.publication.created
    return recorded.publication


async def _three_legacy_publications(
    scenario: ContractPositionScenario,
) -> tuple[BoundFinancialPublication, BoundFinancialPublication, BoundFinancialPublication]:
    """Retain three genuine transitions totaling the original admitted two units."""
    first = await record_projection_position(scenario)
    second = await _next_legacy_publication(scenario, 2)
    third = await _next_legacy_publication(scenario, 3)
    repository = scenario.fill.admission.repository
    async with repository.session() as session:
        identities = tuple(
            (
                await session.scalars(
                    select(FinancialPublicationIntent.public_id).order_by(
                        FinancialPublicationIntent.id
                    )
                )
            ).all()
        )
    assert identities == (first.public_id, second.public_id, third.public_id)
    return first, second, third


async def _acknowledge_legacy(
    repository: AccountingRepository, intent: BoundFinancialPublication
) -> None:
    """Record a real receipt only after the actual source-bound account consumer returns."""
    observer = ContractAccountProjection(repository, intent.payload.scope)
    await observer(intent.topic, intent.public_id, intent.payload)
    returned_at = datetime.now(UTC)
    tracker = SequenceTracker()
    await record_financial_publication_delivery(
        repository,
        intent.public_id,
        returned_at,
        SnapshotWriteProvenance(
            public_id=str(uuid7()),
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("publication_boundary_receipt"),
            timestamp=datetime.now(UTC),
        ),
    )


def _install_delivery_guards(connection: Connection) -> None:
    """Restore the original append-only installer after an isolated damaged-restore witness."""
    install_financial_publication_triggers(connection, "financial_publication_deliveries")


async def _restore_finite_receipt(
    repository: AccountingRepository, publication_public_id: str
) -> None:
    """Close one actual SQLite receipt in a controlled restore and reinstall genuine guards."""
    async with repository.session() as session:
        await session.execute(text("PRAGMA ignore_check_constraints = ON"))
        try:
            await session.execute(
                text("DROP TRIGGER financial_publication_deliveries_reject_update")
            )
            await session.execute(
                text(
                    "UPDATE financial_publication_deliveries SET known_to = :known_to "
                    "WHERE publication_public_id = :publication_public_id"
                ),
                {
                    "known_to": "9998-01-01 00:00:00.000000",
                    "publication_public_id": publication_public_id,
                },
            )
            connection = await session.connection()
            await connection.run_sync(_install_delivery_guards)
            await session.commit()
        finally:
            await session.execute(text("PRAGMA ignore_check_constraints = OFF"))
    async with repository.session() as session:
        marker = (
            await session.scalars(
                select(FinancialPublicationDelivery.known_to).where(
                    FinancialPublicationDelivery.publication_public_id == publication_public_id
                )
            )
        ).one()
    assert marker == _FINITE_FUTURE and marker > datetime.now(UTC)


@pytest.mark.parametrize("reader", ("legacy", "versioned"))
async def test_delivered_prefix_longer_than_page_reveals_actual_pending_tail(
    projection_scenario: ContractPositionScenario, reader: Literal["legacy", "versioned"]
) -> None:
    """Apply the pending page bound after acknowledging a longer genuine prefix.

    Given: Three real transitions complete one admitted order and the first two consumers return.
    When: Either real pending reader requests one item from that account.
    Then: It returns the third original identity and becomes empty after that consumer returns.
    """
    first, second, third = await _three_legacy_publications(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    await _acknowledge_legacy(repository, first)
    await _acknowledge_legacy(repository, second)
    load_pending = (
        load_pending_financial_publications
        if reader == "legacy"
        else load_pending_versioned_financial_publications
    )
    pending = await load_pending(repository, first.payload.scope, limit=1)
    assert tuple(intent.public_id for intent in pending) == (third.public_id,)
    await _acknowledge_legacy(repository, third)
    assert await load_pending(repository, first.payload.scope, limit=1) == ()


@pytest.mark.parametrize("reader", ("legacy", "versioned"))
async def test_finite_future_receipt_beyond_pending_page_refuses_before_exclusion(
    projection_scenario: ContractPositionScenario, reader: Literal["legacy", "versioned"]
) -> None:
    """A damaged later receipt cannot hide behind an earlier healthy pending page.

    Given: The third real notification has a genuine receipt while the first two remain pending.
    When: A damaged restore gives that receipt a finite future end and a reader requests one item.
    Then: Both readers refuse before returning the page or acknowledging the damaged receipt.
    """
    first, second, third = await _three_legacy_publications(projection_scenario)
    repository = projection_scenario.fill.admission.repository
    await _acknowledge_legacy(repository, third)
    await _restore_finite_receipt(repository, third.public_id)
    load_pending = (
        load_pending_financial_publications
        if reader == "legacy"
        else load_pending_versioned_financial_publications
    )
    with pytest.raises(
        FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
    ):
        await load_pending(repository, first.payload.scope, limit=1)
    async with repository.session() as session:
        receipts = tuple((await session.scalars(select(FinancialPublicationDelivery))).all())
        intents = tuple((await session.scalars(select(FinancialPublicationIntent))).all())
    assert len(receipts) == 1 and receipts[0].publication_public_id == third.public_id
    assert {intent.public_id for intent in intents} == {
        first.public_id,
        second.public_id,
        third.public_id,
    }


async def test_actual_native_v3_finite_future_receipt_is_not_an_acknowledgment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep a real SDK-derived native publication subject to the same exact receipt lifetime proof.

    Given: A native SDK source graph commits its V3 position, notification and consumer receipt.
    When: An isolated damaged restore changes only that receipt's end to a finite future instant.
    Then: Versioned recovery refuses while retaining the original intent and receipt.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        recorded = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        intent = recorded.publication
        observer = VersionedContractAccountProjection(repository, intent.payload.scope)
        assert (
            await publish_pending_versioned_financial_notifications(
                repository, intent.payload.scope, observer, SequenceTracker()
            )
            == 1
        )
        await _restore_finite_receipt(repository, intent.public_id)
        with pytest.raises(
            FinancialPublicationError, match="financial_delivery_actual_intent_mismatch"
        ):
            await load_pending_versioned_financial_publications(
                repository, intent.payload.scope, limit=1
            )
        async with repository.session() as session:
            receipts = tuple((await session.scalars(select(FinancialPublicationDelivery))).all())
            intents = tuple((await session.scalars(select(FinancialPublicationIntent))).all())
        assert len(receipts) == len(intents) == 1
        assert receipts[0].publication_public_id == intents[0].public_id == intent.public_id
