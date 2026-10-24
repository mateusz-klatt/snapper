"""Preserve unowned raw evidence, immutable retry identities and absent-client refusals."""

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

import snapper.application.trade.private_source_observation_ingestion as ingestion
import tests.application.trade.test_execution_observation_ingestion as observations
import tests.data.test_private_source_ownership_store as actual_frames
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.native_account_authority import native_authority_scenario


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Install actual schema and immutable guards in a private physical SQLite database."""
    value = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'ingestion-guards.db'}")
    try:
        await value.create_all()
        yield value
    finally:
        await value.engine.dispose()


@pytest.mark.parametrize("invalid_base64", (False, True))
def test_unissued_or_invalid_wire_ticket_keeps_observation_unowned(
    invalid_base64: bool,
) -> None:
    """Untrusted ticket inputs cannot turn coarse source retention into authenticated ownership.

    Given: An unissued object and original financial bytes or malformed base64.
    When: The original-wire matching boundary evaluates the candidate.
    Then: It returns explicit absence of ownership without discarding raw material.
    """
    source = observations._source()
    if invalid_base64:
        source = PrivateExecutionObservation(source.context, source.representation, "not-base64!")
    assert ingestion._matching_ticket(object(), source) is None


@pytest.mark.parametrize("with_unissued_ticket", (False, True))
async def test_missing_client_scope_refuses_before_any_raw_capture(
    repository: SQLAlchemyRepository, with_unissued_ticket: bool
) -> None:
    """Neither an absent scope nor an unissued ticket authorizes an invented wallet source.

    Given: No selected client and optionally an unissued ticket lookalike.
    When: The synchronous original-frame boundary tries to freeze routing.
    Then: It refuses and the actual database retains no raw financial evidence.
    """
    sink = ingestion.AuthenticatedPrivateObservationSink(
        repository, lambda: None, lambda: None, SequenceTracker()
    )
    ticket = object() if with_unissued_ticket else None
    with pytest.raises(RuntimeError, match="client_identity_unproven"):
        sink.bind_frame(ticket, b"unproven financial bytes")
    async with repository.session() as session:
        assert (await session.execute(select(AccountingRawReceipt))).first() is None


async def test_lost_commit_acknowledgement_retries_identical_frozen_evidence(
    repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An uncertain storage acknowledgement cannot allocate a second raw identity or financial hold.

    Given: A real raw-and-hold commit whose first successful response is lost.
    When: The frozen callback retries its unchanged evidence through the real transaction.
    Then: Both attempts share one object and the database retains one raw and one hold.
    """
    original_capture = ingestion._capture_atomically
    attempts: list[ingestion._CaptureEvidence] = []

    async def lose_first_response(
        selected_repository: AccountingRepository, data: ingestion._CaptureEvidence
    ) -> str:
        """Lose the response only after the actual original transaction has committed."""
        attempts.append(data)
        result = await original_capture(selected_repository, data)
        if len(attempts) == 1:
            raise OperationalError("commit", {}, RuntimeError("synthetic response lost"))
        return result

    monkeypatch.setattr(ingestion, "_capture_atomically", lose_first_response)
    sink = ingestion.AuthenticatedPrivateObservationSink(
        repository, observations._selection, lambda: None, SequenceTracker()
    ).bind_frame(None, None)
    raw_id = await sink(observations._source())
    assert len(attempts) == 2 and attempts[0] is attempts[1]
    assert raw_id == attempts[0].provenance.public_id
    async with repository.session() as session:
        raw = (await session.execute(select(AccountingRawReceipt))).scalars().all()
        holds = (await session.execute(select(AccountingScopeHold))).scalars().all()
        assert [row.public_id for row in raw] == [raw_id]
        assert [row.raw_receipt_public_id for row in holds] == [raw_id]


async def test_actual_ticket_with_inconsistent_observation_clock_keeps_raw_and_hold_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual socket ticket does not excuse a later callback's inconsistent source clock.

    Given: An actual issued receive and a financial observation predating its original frame.
    When: The real atomic callback attempts ownership alongside raw retention.
    Then: Ownership is refused while the original raw and financial hold commit together.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        actual_frames._received_frame(source, monkeypatch) as received,
    ):
        context = received.ticket.context
        earlier = RawAccountingObservation.model_validate(
            received.observation.model_copy(
                update={"observed_at": context.frame_observed_at - timedelta(seconds=1)}
            )
        )
        callback = ingestion.AuthenticatedPrivateObservationSink(
            source.repository,
            lambda: context.client_selection,
            lambda: None,
            SequenceTracker(),
        ).bind_frame(received.ticket, actual_frames._WIRE)
        raw_id = await callback(actual_frames._private_source(earlier))
        async with source.repository.session() as session:
            raw = (
                await session.execute(
                    select(AccountingRawReceipt).where(AccountingRawReceipt.public_id == raw_id)
                )
            ).scalar_one()
            hold = (
                await session.execute(
                    select(AccountingScopeHold).where(
                        AccountingScopeHold.raw_receipt_public_id == raw_id
                    )
                )
            ).scalar_one()
            ownership = (
                await session.execute(
                    select(PrivateSourceOwnershipReceipt).where(
                        PrivateSourceOwnershipReceipt.raw_receipt_public_id == raw_id
                    )
                )
            ).first()
        assert raw.public_id == hold.raw_receipt_public_id == raw_id
        assert ownership is None
