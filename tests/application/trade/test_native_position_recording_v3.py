"""Actual original-source V3 recording, transaction rollback and uncertain-commit recovery."""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.native_financial_payloads_v2 import ContractPositionEventV2
from snapper.application.trade.account_bound_submission_admission import (
    capture_account_submission_admission,
)
from snapper.application.trade.native_position_recording_v3 import NativePositionRecordingRequestV3
from snapper.application.trade.native_position_recording_v3 import record_native_position_v3
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.contract_accounting_integrity import run_contract_accounting_restore_audit
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.models import VenueEvent
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_cash_store_v2 import NativeCashAccountingRequestV2
from snapper.data.native_cash_store_v2 import append_native_cash_from_receipt_v2
from snapper.data.native_financial_event_family import AdmittedNativeEventMaterialV3
from snapper.data.native_financial_event_family import get_native_financial_event_family
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_position_family_history import get_bound_native_position_family
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import native_cash_wire_v2
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2
from tests.helpers.native_position_recording_v3 import NativePositionRecordingScenarioV3
from tests.helpers.native_position_recording_v3 import native_position_recording_scenario_v3
from tests.helpers.native_position_recording_v3 import record_native_position_scenario_v3
from tests.helpers.unit_admission import admission_identity

pytestmark = pytest.mark.asyncio


@dataclass(frozen=True, slots=True)
class _Facts:
    """Independent global counts, separating real source retention from financial target creation."""

    execution: int
    venue_event: int
    fill_unit: int
    anchor: int
    event: int
    claim: int
    publication: int
    raw: int
    ownership: int


async def _facts(repository: SQLAlchemyRepository) -> _Facts:
    """Read actual rows in a fresh session rather than treating allocated result IDs as persisted facts."""
    values: list[int] = []
    async with repository.session() as session:
        for model in (
            Execution,
            VenueEvent,
            ContractAccountingAnchor,
            ContractAccountingEvent,
            AccountingNativeSourceClaim,
            FinancialPublicationIntent,
            AccountingRawReceipt,
            PrivateSourceOwnershipReceipt,
        ):
            values.append(int(await session.scalar(select(func.count()).select_from(model)) or 0))
        fill = int(
            await session.scalar(
                select(func.count())
                .select_from(ExecutionUnitSnapshot)
                .where(ExecutionUnitSnapshot.phase == "fill")
            )
            or 0
        )
    return _Facts(values[0], values[1], fill, *values[2:])


def _require_one_recording(before: _Facts, after: _Facts) -> None:
    """Require each actual financial target once and every already committed original source unchanged."""
    assert after == _Facts(
        before.execution + 1,
        before.venue_event + 1,
        before.fill_unit + 1,
        before.anchor + 1,
        before.event + 1,
        before.claim + 1,
        before.publication + 1,
        before.raw,
        before.ownership,
    )


@pytest.mark.parametrize(
    ("side", "completion"),
    [("buy", "partial"), ("buy", "full"), ("sell", "partial"), ("sell", "full")],
)
async def test_original_native_partial_or_full_fill_commits_one_admitted_position_without_cash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    side: Literal["buy", "sell"],
    completion: Literal["partial", "full"],
) -> None:
    """Commit actual provider partial/full operands with original admission and reference-only notification.

    Given: Real signed admission and original SDK fill/log sources before any financial target exists.
    When: The actual one-transaction V3 caller records the original provider transition.
    Then: One complete admitted graph and outbox survive while cash stays unknown and source holds remain.
    """
    async with native_position_recording_scenario_v3(
        tmp_path, monkeypatch, side=side, completion=completion
    ) as scenario:
        before = await _facts(scenario.source.repository)
        recorded = await record_native_position_scenario_v3(scenario)
        _require_one_recording(before, await _facts(scenario.source.repository))
        absolute = Decimal(1 if completion == "partial" else 2)
        quantity = absolute if side == "buy" else absolute.copy_negate()
        assert recorded.created and recorded.position.observed.quantity == quantity
        async with scenario.source.repository.session() as session:
            material = await get_native_financial_event_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
            assert isinstance(material, AdmittedNativeEventMaterialV3)
            assert isinstance(material.payload, ContractPositionEventV2)
            assert (
                material.envelope.fill_unit_admission.submission_admission
                == scenario.request.submission_admission
            )
            assert material.admitted.original.execution.status == (
                "partial" if completion == "partial" else "filled"
            )
            assert recorded.publication.payload.position_event_digest == material.row.content_digest
            assert (
                material.payload.operands.observed_after.quote_turnover_since_anchor
                == absolute * 100
            )
            bound = await get_bound_native_position_family(
                session, recorded.position.public_id, datetime.now(UTC)
            )
            assert bound.row.public_id == recorded.position.public_id
        async with scenario.source.repository.session() as session:
            await scenario.source.repository._begin_execution_insert_transaction(session)
            await scenario.source.repository._acquire_execution_fence_lock(
                session, scenario.request.wallet_public_id
            )
            snapshot = await load_bound_contract_account_snapshot(
                session, scenario.authority.scope, datetime.now(UTC)
            )
            await session.commit()
        assert snapshot.positions[0].quantity == quantity and snapshot.native_cash == ()
        assert snapshot.readiness == "held" and snapshot.execution_prefix_status == "unavailable"
        assert snapshot.reported_cash_components[0].fee == Decimal("0.10000000000000001")
        assert len(snapshot.unresolved_holds) >= 2
        report = await run_contract_accounting_restore_audit(scenario.source.repository)
        assert (report.event_count, report.claim_count, report.publication_count) == (1, 1, 1)
        assert len(report.physical_guards) == 30


@pytest.mark.parametrize(
    "table",
    [
        "contract_accounting_events",
        "accounting_native_source_claims",
        "financial_publication_intents",
    ],
)
async def test_late_actual_insert_refusal_leaves_no_partial_native_recording(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    table: Literal[
        "contract_accounting_events",
        "accounting_native_source_claims",
        "financial_publication_intents",
    ],
) -> None:
    """Rollback actual Execution, venue event, unit, seed and source claim after late insert refusal.

    Given: Genuine retained private sources and a real isolated SQL INSERT failure at a late boundary.
    When: The complete native recording reaches that actual table in its sole transaction.
    Then: Every new financial target rolls back, original raw/ownership stay exact, and retry succeeds.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        before = await _facts(repository)
        async with repository.session() as session:
            await session.execute(
                text(
                    f"CREATE TRIGGER isolated_native_recording_failure BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT, 'isolated_native_recording_sql_refusal'); END"
                )
            )
            await session.commit()
        with pytest.raises(IntegrityError, match="isolated_native_recording_sql_refusal"):
            await record_native_position_scenario_v3(scenario)
        assert await _facts(repository) == before
        async with repository.session() as session:
            await session.execute(text("DROP TRIGGER isolated_native_recording_failure"))
            await session.commit()
        recorded = await record_native_position_scenario_v3(scenario)
        assert recorded.created
        _require_one_recording(before, await _facts(repository))


async def test_identical_admitted_retry_survives_revocation_but_other_actual_admission_cannot_replace_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve the actual original admission and row clocks on historical idempotent replay.

    Given: One native recording commits and another real same-command admission is retained later.
    When: A changed original admission is requested, then the original request repeats after revocation.
    Then: The changed reference refuses and the original immutable graph returns without renewed permission.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        first = await record_native_position_scenario_v3(scenario)
        repository = scenario.source.repository
        committed = await _facts(repository)
        reference, authority = await fresh_native_cash_authority_v2(scenario.source, 7901)
        result = await capture_account_submission_admission(
            repository,
            scenario.order,
            ExecutionRouteReference(
                authority_public_id=reference.public_id,
                authority_content_digest=reference.content_digest,
                scope=authority.scope,
                route_revision=authority.route_revision,
            ),
            admission_identity(7902),
            scenario.source.policy,
        )
        async with repository.session() as session:
            later = await get_bound_account_submission_admission(session, result.public_id)
        changed_reference = NativeUnitRecordReference(
            public_id=later.public_id,
            content_digest=later.content_digest,
            recorded_at=later.recorded_at,
        )
        assert changed_reference != scenario.request.submission_admission
        prepared = await _facts(repository)
        assert prepared == _Facts(
            committed.execution,
            committed.venue_event,
            committed.fill_unit,
            committed.anchor,
            committed.event,
            committed.claim,
            committed.publication,
            committed.raw + 1,
            committed.ownership,
        )
        async with repository.session() as session:
            assert (
                await session.scalar(
                    select(AccountingRawReceipt.public_id).where(
                        AccountingRawReceipt.public_id
                        == authority.native_read.raw_receipt_public_id
                    )
                )
                is not None
            )
        with pytest.raises(
            NativeFinancialBindingError, match="native_recording_source_redelivery_conflict"
        ):
            await record_native_position_v3(
                repository,
                scenario.request.model_copy(update={"submission_admission": changed_reference}),
                scenario.identity,
                scenario.source.policy,
            )
        assert await _facts(repository) == prepared
        await write_financial_owner_policy(
            repository,
            FinancialOwnerPolicyChange(
                policy=FinancialAccountOwnerPolicy(assignments=()),
                updated_by="isolated-native-v3-revocation",
            ),
            scenario.source.tracker,
        )
        strict = NativeAuthorityFreshnessPolicy(
            timedelta(microseconds=1), timedelta(microseconds=1)
        )
        second = await record_native_position_v3(
            repository, scenario.request, scenario.identity, strict
        )
        assert not second.created
        assert (
            second.execution_public_id,
            second.unit_snapshot_public_id,
            second.position.public_id,
            second.publication.public_id,
        ) == (
            first.execution_public_id,
            first.unit_snapshot_public_id,
            first.position.public_id,
            first.publication.public_id,
        )
        assert second.publication.content_digest == first.publication.content_digest
        assert await _facts(repository) == prepared


class _CommitAcknowledgementLostError(RuntimeError):
    """The isolated observer lost the return after the actual SQL commit completed."""


async def test_actual_financial_commit_then_lost_ack_recovers_all_original_coordinates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recover a real successful financial commit after its acknowledgement is deliberately lost.

    Given: Actual admitted source facts and an observer that delegates to the real commit before raising once.
    When: The caller retries from a new repository instance against the same actual database.
    Then: One original complete financial graph returns without another execution, source claim or intent.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        before = await _facts(scenario.source.repository)
        original_commit = AsyncSession.commit
        lost = False

        async def commit_then_lose_ack(session: AsyncSession) -> None:
            nonlocal lost
            await original_commit(session)
            if not lost:
                lost = True
                raise _CommitAcknowledgementLostError("isolated_native_recording_commit_ack_lost")

        with monkeypatch.context() as scoped:
            scoped.setattr(AsyncSession, "commit", commit_then_lose_ack)
            with pytest.raises(_CommitAcknowledgementLostError):
                await record_native_position_scenario_v3(scenario)
        committed = await _facts(scenario.source.repository)
        _require_one_recording(before, committed)
        reopened = SQLAlchemyRepository(str(scenario.source.repository.engine.url))
        try:
            result = await record_native_position_v3(
                reopened, scenario.request, scenario.identity, scenario.source.policy
            )
            assert not result.created
            assert await _facts(reopened) == committed
            async with reopened.session() as session:
                event = await get_bound_native_position_family(
                    session, result.position.public_id, datetime.now(UTC)
                )
                assert event.row.public_id == result.position.public_id
                assert result.execution_public_id == event.payload.operands.execution_public_id
        finally:
            await reopened.engine.dispose()


async def _contradictory_native_cash_request(
    scenario: NativePositionRecordingScenarioV3, monkeypatch: pytest.MonkeyPatch
) -> NativeCashAccountingRequestV2:
    """Capture a real independently authenticated cash entry reusing the position log's native ID.

    Args:
        scenario: Genuine original position source graph in the shared isolated repository.
        monkeypatch: Lifetime owner of the actual closed SDK transport boundaries.

    Returns:
        Strict source request naming the real new cash receipt and its current authority.
    """
    captured = await receive_native_cash_v2(
        scenario.source,
        monkeypatch,
        native_cash_wire_v2(31, Decimal(0), Decimal(9)),
        8001,
    )
    reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 8101)
    return NativeCashAccountingRequestV2(
        wallet_public_id=scenario.request.wallet_public_id,
        raw_receipt_public_id=captured.raw.public_id,
        authority_reference=reference,
    )


@pytest.mark.parametrize("position_first", [False, True])
async def test_actual_native_cash_and_admitted_position_share_one_source_consumption_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, position_first: bool
) -> None:
    """Keep one actual native log identity unique across original cash2 and admitted position3.

    Given: Two independently authenticated contradictory provider entries reuse native source ID31.
    When: One real cash or admitted-position fact commits before the other writer consumes that ID.
    Then: The second financial fact refuses and its surrounding targets roll back without another namespace.
    """
    async with native_position_recording_scenario_v3(tmp_path, monkeypatch) as scenario:
        if position_first:
            first_position = await record_native_position_scenario_v3(scenario)
            assert first_position.created
        cash = await _contradictory_native_cash_request(scenario, monkeypatch)
        request = scenario.request
        if not position_first:
            first_cash = await append_native_cash_from_receipt_v2(
                scenario.source.repository, cash, native_cash_identity(8201), scenario.source.policy
            )
            assert first_cash.created
            reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 8301)
            request = NativePositionRecordingRequestV3.model_validate(
                {
                    **scenario.request.model_dump(mode="python"),
                    "position_authority_reference": reference,
                }
            )
            assert request.position_receipt_public_id == scenario.request.position_receipt_public_id
            assert (
                request.position_authority_reference
                != scenario.request.position_authority_reference
            )
        before = await _facts(scenario.source.repository)
        assert before.claim == before.event == 1
        with pytest.raises(
            NativeFinancialBindingError, match="native_financial_source_already_consumed"
        ):
            if position_first:
                await append_native_cash_from_receipt_v2(
                    scenario.source.repository,
                    cash,
                    native_cash_identity(8201),
                    scenario.source.policy,
                )
            else:
                await record_native_position_v3(
                    scenario.source.repository, request, scenario.identity, scenario.source.policy
                )
        assert await _facts(scenario.source.repository) == before
