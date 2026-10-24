"""Exercise native-v2 absence, historical clocks and family boundaries with real isolated sources."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.accounting_native_source_store_v2 as claim_store
import snapper.data.native_cash_history_v2 as cash_history
import snapper.data.native_cash_store_v2 as cash_store
import snapper.data.native_position_fill_binding_v2 as fill_binding
import snapper.data.native_position_history_v2 as position_history
import snapper.data.native_position_source_v2 as position_source
from snapper.application.portfolio.accounting_native_source import (
    AccountingNativeSourceClaimPayload,
)
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.application.portfolio.futures_cash_ledger import account_log_cash_source_identity
from snapper.application.portfolio.native_financial_payloads_v2 import FuturesNativeCashEventV2
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.futures_cash_ledger_store import append_futures_cash_from_receipt
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import VenueAccountObservation
from snapper.data.models import VenueAccountState
from snapper.data.native_cash_store_v2 import NativeCashAccountingRequestV2
from snapper.data.native_cash_store_v2 import append_native_cash_from_receipt_v2
from snapper.data.native_financial_event_binding_v2 import NativeFinancialEventMaterialV2
from snapper.data.native_financial_event_binding_v2 import get_native_financial_event_material
from snapper.data.native_financial_event_binding_v2 import native_event_columns_v2
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_financial_source_binding_v2 import bind_native_financial_receipt
from snapper.data.native_position_fill_binding_v2 import OriginalPositionFillRequestV2
from snapper.data.native_position_store_v2 import append_native_position_from_receipts_v2
from snapper.data.repository_types import VenueAccountAttemptRow
from tests.data.test_futures_cash_ledger_store import _NOW as _LEGACY_NOW
from tests.data.test_futures_cash_ledger_store import _capture as capture_legacy_cash
from tests.data.test_futures_cash_ledger_store import scenario as _legacy_scenario_fixture
from tests.helpers.account_bound_admission import account_admission_scenario
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import native_cash_scenario_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_wire_v2
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2
from tests.helpers.native_position_accounting_v2 import _observe_current_position_catalog
from tests.helpers.native_position_accounting_v2 import _receive_fill
from tests.helpers.native_position_accounting_v2 import _seed_native_order
from tests.helpers.native_position_accounting_v2 import native_position_scenario_v2
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp

legacy_scenario = _legacy_scenario_fixture


async def test_missing_native_financial_identities_and_seeds_remain_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse absent actual SQL identities independently of a well-shaped immutable payload.

    Given: A genuine native cash event and its source graph are retained in isolated SQL.
    When: Readers request nonexistent claim, event or anchor UUIDs.
    Then: Each exact boundary refuses, and the original cash event remains readable.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(8000),
            scenario.source.policy,
        )
        clock, missing = datetime.now(UTC), native_authority_uuid(8999)
        async with scenario.source.repository.session() as session:
            bound = await cash_history.get_bound_native_cash_event_v2(
                session, result.public_id, clock
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_actual_claim_missing"
            ):
                await claim_store.get_bound_native_source_claim_v2(session, missing, clock)
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_actual_event_missing"
            ):
                await get_native_financial_event_material(session, missing, clock)
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_actual_cash_event_missing"
            ):
                await cash_history.get_bound_native_cash_event_v2(session, missing, clock)
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_actual_event_missing"
            ):
                await position_history.get_bound_native_position_event_v2(session, missing, clock)
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_actual_seed_missing"
            ):
                await position_history._bound_anchor(session, missing, clock)
            crossed = bound.payload.model_copy(
                update={
                    "operands": bound.payload.operands.model_copy(
                        update={"anchor_public_id": missing}
                    )
                }
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_cash_anchor_missing"
            ):
                await cash_history._bound_anchor(session, crossed, clock)
            assert (
                await cash_history.get_bound_native_cash_event_v2(session, result.public_id, clock)
                == bound
            )


async def test_original_position_execution_and_alias_guards_query_actual_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep actual fill targets distinct from submission targets and nonexistent provider aliases.

    Given: A genuine original fill and retained execution unit exist under intact schema guards.
    When: An internal caller supplies a submission target, unknown execution or unknown provider symbol.
    Then: Each exact source lookup refuses without selecting a different execution or catalog.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        clock = datetime.now(UTC)
        async with scenario.source.repository.session() as session:
            unit = await get_bound_execution_unit_snapshot(
                session, scenario.request.unit_snapshot_public_id
            )
            target = unit.target
            assert isinstance(target, FillSnapshotTarget)
            submission = unit.model_copy(
                update={
                    "target": SubmissionSnapshotTarget(
                        command_public_id=admission_id(5000),
                        client_order_id="native-admission-client-1",
                        request_digest="a" * 64,
                    )
                }
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_actual_fill_unit_required"
            ):
                await position_source._actual_execution(session, submission, clock)
            missing = unit.model_copy(
                update={
                    "target": target.model_copy(
                        update={"execution_public_id": native_authority_uuid(8999)}
                    )
                }
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_actual_execution_missing"
            ):
                await position_source._actual_execution(session, missing, clock)
            original = await fill_binding.bind_original_position_fill_v2(
                session, scenario.request.fill, clock
            )
            unknown = original.fill.model_copy(update={"provider_symbol": "PF_UNLISTEDUSD"})
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_original_provider_alias_missing"
            ):
                await fill_binding._native_fill_catalog(session, unknown, clock)
            actual = await position_source._actual_execution(session, unit, clock)
            assert actual.public_id == target.execution_public_id


async def test_native_position_history_does_not_move_posting_knowledge_into_the_past(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve the actual position posting clock independently of earlier provider execution time.

    Given: A real position event has committed after its original fill and later account-log source.
    When: Replay supplies a cutoff one microsecond before the financial row was retained.
    Then: Its history is empty and its exact event UUID remains unknown at that cutoff.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(8000),
            scenario.source.policy,
        )
        async with scenario.source.repository.session() as session:
            bound = await position_history.get_bound_native_position_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
            cutoff = bound.row.timestamp - timedelta(microseconds=1)
            assert (
                await position_history.load_native_position_history_v2(
                    session, scenario.authority.scope, result.instrument_public_id, cutoff
                )
                == ()
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_event_not_known_at_cutoff"
            ):
                await position_history.get_bound_native_position_event_v2(
                    session, result.public_id, cutoff
                )


async def test_native_history_rejects_an_opposite_kind_from_its_material_dependency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep defensive history kind checks independent of the lower binder's current guarantees.

    Given: Genuine position and cash events have independently bound actual sources in one account.
    When: A narrowly substituted material dependency returns the other real event's complete material.
    Then: Actual SQL history selection still runs, and each history refuses the opposite public kind.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        position = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(8000),
            scenario.source.policy,
        )
        captured = await receive_native_cash_v2(
            scenario.source,
            monkeypatch,
            native_cash_wire_v2(32, Decimal(100), Decimal("99.9")),
            8100,
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 8300)
        cash = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            NativeCashAccountingRequestV2(
                wallet_public_id=scenario.request.fill.wallet_public_id,
                raw_receipt_public_id=captured.raw.public_id,
                authority_reference=reference,
            ),
            native_cash_identity(8400),
            scenario.source.policy,
        )
        clock = datetime.now(UTC)
        async with scenario.source.repository.session() as session:
            position_material = await get_native_financial_event_material(
                session, position.public_id, clock
            )
            cash_material = await get_native_financial_event_material(
                session, cash.public_id, clock
            )

            async def return_actual_cash(
                active: AsyncSession, row: ContractAccountingEvent, known_at: datetime
            ) -> NativeFinancialEventMaterialV2:
                """Inject an actual cash result only at the position history's typed material boundary."""
                assert (
                    active is session and row.public_id == position.public_id and known_at == clock
                )
                return cash_material

            async def return_actual_position(
                active: AsyncSession, row: ContractAccountingEvent, known_at: datetime
            ) -> NativeFinancialEventMaterialV2:
                """Inject an actual position result only at the cash history's typed material boundary."""
                assert active is session and row.public_id == cash.public_id and known_at == clock
                return position_material

            monkeypatch.setattr(
                position_history, "bind_native_financial_event_material", return_actual_cash
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_position_actual_kind_mismatch"
            ):
                await position_history.load_native_position_history_v2(
                    session, scenario.authority.scope, position.instrument_public_id, clock
                )
            monkeypatch.setattr(
                cash_history, "bind_native_financial_event_material", return_actual_position
            )
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_cash_actual_kind_mismatch"
            ):
                await cash_history.load_native_cash_history_v2(
                    session, scenario.authority.scope, clock
                )


async def test_native_family_refuses_an_actual_restored_event_without_its_shared_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detect an actual orphaned event without deleting rows or weakening any database guard.

    Given: Real current sources and owner policy can establish a new native cash event.
    When: A restore-shaped transaction inserts its valid anchor and event but omits the claim.
    Then: The actual SQL outer join refuses that orphan, and rollback leaves no financial row.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(
                session, scenario.request.wallet_public_id
            )
            provenance = SnapshotWriteProvenance(
                **native_cash_identity(8000).model_dump(mode="python"), timestamp=datetime.now(UTC)
            )
            received = await bind_native_financial_receipt(
                session,
                scenario.authority_reference,
                scenario.captured.raw.public_id,
                provenance.timestamp,
            )
            admission = await cash_store._current_cash_admission(
                session, scenario.request, provenance, scenario.source.policy, received
            )
            anchor = await cash_store._stage_anchor(session, admission, provenance)
            leg = admission.leg
            operands = FuturesNativeCashEvent(
                scope=scenario.authority.scope,
                route_authority_public_id=scenario.authority_reference.public_id,
                anchor_public_id=anchor,
                raw_receipt_public_id=scenario.captured.raw.public_id,
                ledger_sequence=1,
                source_identity=account_log_cash_source_identity(leg.entry),
                native_currency=leg.native_currency,
                cash_before=leg.entry.old_balance,
                cash_after=leg.entry.new_balance,
                cash_delta=leg.cash_delta,
                source_entry=leg.entry,
            )
            payload = FuturesNativeCashEventV2(source=admission.evidence, operands=operands)
            session.add(
                ContractAccountingEvent(
                    **provenance.model_dump(mode="python"), **native_event_columns_v2(payload)
                )
            )
            await session.flush()
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_family_history_unclaimed"
            ):
                await claim_store._require_claimed_family(
                    session, scenario.authority.scope, provenance.timestamp
                )
            await session.rollback()
        async with repository.session() as session:
            assert (
                await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 0
            )
            assert (
                await session.scalar(select(func.count()).select_from(AccountingNativeSourceClaim))
                == 0
            )


async def test_legacy_shared_claim_is_rebound_but_cannot_be_promoted_to_native_history(
    legacy_scenario: AdmissionScenario,
) -> None:
    """Retain v1 claim dispatch while requiring explicit migration before native-v2 family use.

    Given: The actual v1 appender commits a genuine source, anchor, event and shared claim.
    When: The v2 family reader encounters that intact historical claim.
    Then: Original v1 source binding succeeds before the family explicitly refuses silent promotion.
    """
    request = await capture_legacy_cash(legacy_scenario, 8000)
    result = await append_futures_cash_from_receipt(
        legacy_scenario.repository, request, admission_stamp(8100, _LEGACY_NOW)
    )
    async with legacy_scenario.repository.session() as session:
        row = (await session.scalars(select(AccountingNativeSourceClaim))).one()
        original = await claim_store._bound_existing_claim(session, row, _LEGACY_NOW)
        assert isinstance(original, AccountingNativeSourceClaimPayload)
        assert original.accounting_event_public_id == result.public_id
        with pytest.raises(
            NativeFinancialBindingError,
            match="native_financial_family_history_requires_explicit_migration",
        ):
            await claim_store._require_claimed_family(
                session, legacy_scenario.authority.scope, _LEGACY_NOW
            )


async def _same_client_legacy_authority(
    source: NativeAuthorityScenario,
) -> AccountAuthorityReference:
    """Retain a genuine separate generic account observation for the exact issued private client.

    Empty generic balances prove no financial amount or UID. The legacy route
    uses only the actual selected credential and the new durable generic observation;
    authenticated private ownership remains independently supplied by the real fill ticket.
    """
    clock = datetime.now(UTC)
    selected = source.selection
    attempt: VenueAccountAttemptRow = {
        "wallet_public_id": selected.wallet_public_id,
        "exchange": selected.exchange,
        "mode": selected.mode,
        "balance_status": "observed",
        "position_status": "not_applicable",
        "valuation_status": "native_only",
        "balances_json": "[]",
        "open_positions_json": None,
        "balance_observed_at": clock,
        "position_observed_at": None,
        "authoritative_until": clock + timedelta(minutes=1),
        "error": None,
        "session_id": native_authority_uuid(9000),
        "sequence_id": 18000,
        "bus_time": clock,
    }
    state_id = await source.repository.record_venue_account_snapshot(attempt)
    async with source.repository.session() as session:
        state = await session.get(VenueAccountState, state_id)
        assert state is not None
        observation = await session.get(
            VenueAccountObservation, state.current_attempt_observation_id
        )
        assert observation is not None
        scope = ExecutionAccountScope(
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            mode=selected.mode,
            environment=selected.environment,
            margin_account="flex",
        )
        payload = ExecutionRouteAuthorityPayload(
            scope=scope,
            credential_public_id=selected.credential_public_id,
            credential_timestamp=selected.credential_timestamp,
            public_api_origin=selected.public_api_origin,
            account_observation_public_id=observation.public_id,
            account_observation_timestamp=observation.timestamp,
            balance_observed_at=clock,
            balance_receipt_json="[]",
            observed_at=clock,
            valid_until=clock + timedelta(minutes=1),
            route_revision=route_selection_digest(
                scope,
                selected.credential_public_id,
                selected.credential_timestamp,
                selected.public_api_origin,
            ),
        )
    result = await insert_execution_route_authority(
        source.repository,
        payload,
        SnapshotWriteProvenance(
            **native_cash_identity(18001).model_dump(mode="python"), timestamp=clock
        ),
    )
    return AccountAuthorityReference(
        family="legacy_route_v1",
        public_id=result.public_id,
        content_digest=result.content_digest,
        recorded_at=clock,
    )


async def test_original_fill_adapter_preserves_actual_legacy_family_and_private_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise legacy fill dispatch without replacing its original client with later native authority.

    Given: An actual generic legacy account route is retained before a genuinely issued private fill.
    When: The v2 original-fill adapter binds that exact legacy reference at the actual later clock.
    Then: It uses the unchanged v1 fill reader, retains the legacy family and independently binds UID ownership.
    """
    async with account_admission_scenario(tmp_path, monkeypatch) as scenario:
        await _observe_current_position_catalog(scenario, datetime.now(UTC))
        await _seed_native_order(scenario)
        reference = await _same_client_legacy_authority(scenario.native)
        captured = await _receive_fill(scenario.native, monkeypatch)
        request = OriginalPositionFillRequestV2(
            wallet_public_id=scenario.native.selection.wallet_public_id,
            raw_receipt_public_id=captured.raw.public_id,
            authority_reference=reference,
            ownership_reference=captured.ownership.reference,
        )
        async with scenario.native.repository.session() as session:
            actual = await fill_binding.bind_original_position_fill_v2(
                session, request, datetime.now(UTC)
            )
            assert actual.authority.family == "legacy_route_v1"
            assert isinstance(actual.authority.payload, ExecutionRouteAuthorityPayload)
            assert actual.authority.payload.venue_account_id is None
            assert actual.ownership.reference == captured.ownership.reference
            assert actual.fill.execution_id == "native-position-fill-1"
            assert actual.alias.exchange_symbol == actual.fill.provider_symbol
