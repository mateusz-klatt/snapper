"""Actual native position targets, independently issued private sources and immutable owner/replay composition."""

import base64
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.contract_accounting_integrity import run_contract_accounting_restore_audit
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.financial_account_owner_store import load_current_financial_owner_evidence
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import FinancialPublicationIntent
from snapper.data.native_cash_store_v2 import NativeCashAccountingRequestV2
from snapper.data.native_cash_store_v2 import append_native_cash_from_receipt_v2
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.native_position_history_v2 import get_bound_native_position_event_v2
from snapper.data.native_position_history_v2 import load_native_position_history_v2
from snapper.data.native_position_source_v2 import NativePositionSourceRequestV2
from snapper.data.native_position_store_v2 import append_bound_native_position_v2
from snapper.data.native_position_store_v2 import append_native_position_from_receipts_v2
from snapper.data.private_source_ownership_store import get_bound_private_source_ownership
from tests.data.test_financial_account_owner_store import _alias
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import native_cash_wire_v2
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2
from tests.helpers.native_position_accounting_v2 import NativePositionScenarioV2
from tests.helpers.native_position_accounting_v2 import native_position_scenario_v2
from tests.helpers.native_position_accounting_v2 import next_native_position_request_v2

pytestmark = pytest.mark.asyncio


async def _financial_counts(
    scenario: NativePositionScenarioV2,
) -> tuple[int, int, int, int]:
    """Read actual retained financial facts independently of public write-result coordinates."""
    async with scenario.source.repository.session() as session:
        values: list[int] = []
        for model in (
            ContractAccountingAnchor,
            ContractAccountingEvent,
            AccountingNativeSourceClaim,
            FinancialPublicationIntent,
        ):
            value = await session.scalar(select(func.count()).select_from(model))
            values.append(int(value or 0))
    return values[0], values[1], values[2], values[3]


async def test_actual_fill_and_later_native_log_book_only_partial_position(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Book genuine original targets and later native ownership without manufacturing fee cash.

    Given: Actual signed native admission, SDK fills and account_log subscriptions, and committed unit targets.
    When: The wallet-fenced native writer records the independently owned position source.
    Then: One position/anchor/shared claim survives with exact original clocks and no cash or fake publication.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        assert result.created and result.observed.quantity == Decimal(1)
        assert result.ledger_sequence == 1 and result.observed.entry_price == Decimal(100)
        assert await _financial_counts(scenario) == (1, 1, 1, 0)
        async with scenario.source.repository.session() as session:
            event = await get_bound_native_position_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        assert event.payload.fill_ownership_reference == scenario.fill.ownership.reference
        assert event.payload.source.ownership_reference == scenario.position.ownership.reference
        assert event.payload.operands.execution_known_at < event.row.timestamp
        assert event.payload.operands.executed_at == event.fill.fill.occurred_at
        assert event.fill.fill.venue_account_id == "DemoUser"
        assert event.fill.ownership.account_uid == scenario.authority.venue_account_id
        assert event.fill.fill.venue_account_id != event.fill.ownership.account_uid
        assert event.anchor.operands.source_execution_scope_sequence is None
        assert event.anchor.operands.source_execution_prefix_status == "unavailable"
        report = await run_contract_accounting_restore_audit(scenario.source.repository)
        assert (report.event_count, report.claim_count, report.anchor_count) == (
            1,
            1,
            1,
        )
        assert len(report.physical_guards) == 26
        async with scenario.source.repository.session() as session:
            await scenario.source.repository._begin_execution_insert_transaction(session)
            await scenario.source.repository._acquire_execution_fence_lock(
                session, scenario.request.fill.wallet_public_id
            )
            snapshot = await load_bound_contract_account_snapshot(
                session, scenario.authority.scope, datetime.now(UTC)
            )
        assert snapshot.positions[0].quantity == Decimal(1)
        assert snapshot.native_cash == ()
        assert snapshot.reported_cash_components[0].fee == Decimal("0.10")
        assert snapshot.readiness == "held" and snapshot.execution_prefix_status == "unavailable"
        assert len(snapshot.unresolved_holds) >= 2


async def test_actual_position_retry_after_revocation_keeps_original_policy_and_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Replay an already recorded position without renewing expired source or revoked admission.

    Given: A genuine position fact commits before explicit policy revocation and native source expiry.
    When: Its actual original references are retried under a later wallet transaction.
    Then: The same immutable event returns and complete history retains the original legal policy interval.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        await write_financial_owner_policy(
            scenario.source.repository,
            FinancialOwnerPolicyChange(
                policy=FinancialAccountOwnerPolicy(assignments=()),
                updated_by="isolated-position-revoke",
            ),
            scenario.source.tracker,
        )
        clock = max(datetime.now(UTC), scenario.authority.valid_until + timedelta(seconds=1))
        async with scenario.source.repository.session() as session:
            await scenario.source.repository._begin_execution_insert_transaction(session)
            await scenario.source.repository._acquire_execution_fence_lock(
                session, scenario.request.fill.wallet_public_id
            )
            original = await get_bound_native_position_event_v2(session, first.public_id, clock)
            repeated = await append_bound_native_position_v2(
                session,
                scenario.request,
                SnapshotWriteProvenance(
                    **native_cash_identity(7001).model_dump(mode="python"), timestamp=clock
                ),
                scenario.source.policy,
            )
            assert repeated.created is False and repeated.public_id == first.public_id
            assert (
                await get_bound_native_position_event_v2(session, first.public_id, clock)
                == original
            )
            await session.commit()
        assert await _financial_counts(scenario) == (1, 1, 1, 0)


async def test_independent_actual_cash_after_position_uses_one_account_sequence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Consume only a separately observed native wallet row after the position is recorded.

    Given: A genuine position entry reports a fee but has not supplied an authoritative wallet movement.
    When: Another actual account_log entry with its own ID supplies a currency old/new balance.
    Then: One separate cash claim follows the position, and no inferred fee or booking join is asserted.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        position = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        cash = await receive_native_cash_v2(
            scenario.source,
            monkeypatch,
            native_cash_wire_v2(32, Decimal(100), Decimal("99.9")),
            7100,
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 7300)
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            NativeCashAccountingRequestV2(
                wallet_public_id=scenario.request.fill.wallet_public_id,
                raw_receipt_public_id=cash.raw.public_id,
                authority_reference=reference,
            ),
            native_cash_identity(7400),
            scenario.source.policy,
        )
        assert position.ledger_sequence == 1 and result.ledger_sequence == 2
        assert result.observed_balance == Decimal("99.9")
        assert await _financial_counts(scenario) == (2, 2, 2, 0)
        async with scenario.source.repository.session() as session:
            history = await load_native_position_history_v2(
                session,
                scenario.authority.scope,
                position.instrument_public_id,
                datetime.now(UTC),
            )
        assert len(history) == 1 and history[0].row.public_id == position.public_id


async def test_actual_account_log_resnapshot_reuses_original_position_fact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recognize the same native entry at a new actual transport selector without replacing its proof.

    Given: A genuine position is retained from an original account_log/new_entry frame.
    When: The provider retransmits the same exact entry inside an authenticated account_log snapshot.
    Then: The original event and sidecar references remain immutable with no second source consumption.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        original: dict[str, object] = json.loads(
            base64.b64decode(scenario.position.observation.material)
        )
        wire = json.dumps(
            {"feed": "account_log_snapshot", "logs": [original["new_entry"]]}
        ).encode()
        captured = await receive_native_cash_v2(scenario.source, monkeypatch, wire, 7100, "/logs/0")
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 7300)
        request = scenario.request.model_copy(
            update={
                "position_receipt_public_id": captured.raw.public_id,
                "position_authority_reference": reference,
            }
        )
        repeated = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            request,
            native_cash_identity(7400),
            scenario.source.policy,
        )
        assert repeated.created is False and repeated.public_id == first.public_id
        assert await _financial_counts(scenario) == (1, 1, 1, 0)
        async with scenario.source.repository.session() as session:
            retained = await get_bound_native_position_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        assert retained.payload.source.ownership_reference == scenario.position.ownership.reference


async def test_one_actual_native_log_id_cannot_be_reused_as_cash_after_position(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve the same physical native identity namespace across new cash and position schemas.

    Given: Actual log ID31 is consumed by a proven position event.
    When: Another authenticated frame changes that same ID into a native wallet debit.
    Then: Shared source consumption refuses and the original position book remains alone.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        cash = await receive_native_cash_v2(
            scenario.source,
            monkeypatch,
            native_cash_wire_v2(31, Decimal(100), Decimal("99.9")),
            7100,
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 7300)
        with pytest.raises(NativeFinancialBindingError, match="source_already_consumed"):
            await append_native_cash_from_receipt_v2(
                scenario.source.repository,
                NativeCashAccountingRequestV2(
                    wallet_public_id=scenario.request.fill.wallet_public_id,
                    raw_receipt_public_id=cash.raw.public_id,
                    authority_reference=reference,
                ),
                native_cash_identity(7400),
                scenario.source.policy,
            )
        assert await _financial_counts(scenario) == (1, 1, 1, 0)


async def test_actual_claim_flush_failure_rolls_back_position_and_seed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the financial write atomic when the real SQLite source-claim INSERT fails.

    Given: Actual original fill/unit and later owned log exist without any position book.
    When: A real database trigger rejects the shared source-claim insertion during flush.
    Then: No position seed, event or claim commits, while the independent original sources survive.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        async with scenario.source.repository.engine.begin() as connection:
            await connection.execute(
                text(
                    "CREATE TRIGGER isolated_position_claim_failure BEFORE INSERT ON "
                    "accounting_native_source_claims BEGIN SELECT RAISE(ABORT, "
                    "'isolated_position_claim_fault'); END"
                )
            )
        with pytest.raises(IntegrityError, match="isolated_position_claim_fault"):
            await append_native_position_from_receipts_v2(
                scenario.source.repository,
                scenario.request,
                native_cash_identity(7000),
                scenario.source.policy,
            )
        assert await _financial_counts(scenario) == (0, 0, 0, 0)
        async with scenario.source.repository.session() as session:
            fill = await get_bound_private_source_ownership(
                session, scenario.fill.raw.public_id, datetime.now(UTC)
            )
            position = await get_bound_private_source_ownership(
                session, scenario.position.raw.public_id, datetime.now(UTC)
            )
        assert fill.reference == scenario.fill.ownership.reference
        assert position.reference == scenario.position.ownership.reference


async def test_later_native_authority_cannot_replace_original_fill_knowledge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the actual original Execution clock independent of the later position-log authority.

    Given: The original fill and units commit before a later same-UID native accounts authority.
    When: A request substitutes that genuinely later authority for the original fill reference.
    Then: Chronology refuses instead of retroactively proving the earlier Execution.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        changed = scenario.request.model_copy(
            update={
                "fill": scenario.request.fill.model_copy(
                    update={"authority_reference": scenario.request.position_authority_reference}
                )
            }
        )
        with pytest.raises(NativeFinancialBindingError, match="knowledge_after_execution"):
            await append_native_position_from_receipts_v2(
                scenario.source.repository,
                changed,
                native_cash_identity(7000),
                scenario.source.policy,
            )
        assert await _financial_counts(scenario) == (0, 0, 0, 0)


async def test_two_genuine_fills_conserve_one_partial_native_position_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fold independently owned original fills into the same conserved instrument seed.

    Given: One admitted two-unit order has an actual first fill and native old/new position source.
    When: A separately authenticated second fill and log complete the same admitted order.
    Then: Quantity and exact turnover conserve through one anchor and two distinct immutable source claims.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        request = await next_native_position_request_v2(scenario, monkeypatch)
        second = await append_native_position_from_receipts_v2(
            scenario.source.repository, request, native_cash_identity(8700), scenario.source.policy
        )
        assert first.observed.quantity == Decimal(1)
        assert second.observed.quantity == Decimal(2)
        assert second.observed.entry_price == Decimal(100)
        assert second.observed.quote_turnover_since_anchor == Decimal(200)
        assert second.observed.realized_pnl_since_anchor == Decimal(0)
        assert second.anchor_public_id == first.anchor_public_id
        assert second.ledger_sequence == first.ledger_sequence + 1
        assert await _financial_counts(scenario) == (1, 2, 2, 0)
        async with scenario.source.repository.session() as session:
            history = await load_native_position_history_v2(
                session, scenario.authority.scope, first.instrument_public_id, datetime.now(UTC)
            )
        assert len(history) == 2
        assert history[1].payload.operands.before == history[0].payload.operands.observed_after
        assert history[1].payload.operands.venue_execution_id == "native-position-fill-2"
        assert history[1].payload.operands.execution_scope_sequence == 2
        assert history[0].anchor == history[1].anchor


async def test_actual_position_history_pins_owner_against_uid_alias_reassignment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep prior actual position economics in the original configured wallet despite a safe read alias.

    Given: A genuine position belongs to one wallet and another actual SDK key proves the same external UID.
    When: Explicit policy reassignment attempts to move that UID to the other local wallet.
    Then: Reassignment refuses while full source history and the original wallet's current owner check still succeed.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        alias = await _alias(scenario.source)
        _alias_reference, alias_authority = await fresh_native_cash_authority_v2(alias, 8800)
        assert alias_authority.venue_account_id == scenario.authority.venue_account_id
        assert alias.selection.wallet_public_id != scenario.request.fill.wallet_public_id
        proposed = FinancialAccountOwnerPolicy(
            assignments=(
                FinancialAccountOwnerAssignment(
                    owner_key=financial_account_owner_key_from_authority(alias_authority),
                    owner_wallet_public_id=alias.selection.wallet_public_id,
                ),
            )
        )
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="history_or_migration_unproven"
        ):
            await write_financial_owner_policy(
                scenario.source.repository,
                FinancialOwnerPolicyChange(
                    policy=proposed, updated_by="isolated-position-alias-transfer"
                ),
                scenario.source.tracker,
            )
        async with scenario.source.repository.session() as session:
            await scenario.source.repository._begin_execution_insert_transaction(session)
            await scenario.source.repository._acquire_execution_fence_lock(
                session, scenario.request.fill.wallet_public_id
            )
            owner = await load_current_financial_owner_evidence(
                session, scenario.request.position_authority_reference, datetime.now(UTC)
            )
            retained = await get_bound_native_position_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
            await session.commit()
        assert owner.assignment.owner_wallet_public_id == scenario.request.fill.wallet_public_id
        assert retained.row.public_id == first.public_id
        assert retained.payload.operands.observed_after.quantity == Decimal(1)
        assert await _financial_counts(scenario) == (1, 1, 1, 0)


async def test_actual_cash_consumption_prevents_reverse_position_reuse_of_the_same_native_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep the shared source identity exclusive when native cash is committed before a position.

    Given: Genuine original fill/unit and log ID31 exist without a position fact.
    When: A separate actual wallet row consumes native ID31 before the position writer attempts it.
    Then: The reverse cross-kind collision rolls back its position seed/event and preserves only real cash.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        captured = await receive_native_cash_v2(
            scenario.source, monkeypatch, native_cash_wire_v2(31, Decimal(0), Decimal(1)), 9100
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 9300)
        cash = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            NativeCashAccountingRequestV2(
                wallet_public_id=scenario.request.fill.wallet_public_id,
                raw_receipt_public_id=captured.raw.public_id,
                authority_reference=reference,
            ),
            native_cash_identity(9400),
            scenario.source.policy,
        )
        current = NativePositionSourceRequestV2.model_validate(
            {
                **scenario.request.model_dump(mode="python"),
                "position_authority_reference": reference,
            }
        )
        with pytest.raises(NativeFinancialBindingError, match="source_already_consumed"):
            await append_native_position_from_receipts_v2(
                scenario.source.repository,
                current,
                native_cash_identity(9500),
                scenario.source.policy,
            )
        assert cash.created and cash.ledger_sequence == 1
        assert await _financial_counts(scenario) == (1, 1, 1, 0)
        async with scenario.source.repository.session() as session:
            unit = await get_bound_execution_unit_snapshot(
                session, scenario.request.unit_snapshot_public_id
            )
            positions = await load_native_position_history_v2(
                session,
                scenario.authority.scope,
                unit.scope.instrument_public_id,
                datetime.now(UTC),
            )
        assert positions == ()


async def test_two_actual_position_fills_conserve_around_a_distinct_native_cash_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Conserve one position while the shared account journal records a real independent currency fact.

    Given: Actual position log31 and one original fill establish a partial quantity-one position.
    When: Actual currency log32 commits before actual fill two and position log33.
    Then: Account sequences are one/two/three, position history remains one/three, and no source ID is synthesized.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        captured = await receive_native_cash_v2(
            scenario.source, monkeypatch, native_cash_wire_v2(32, Decimal(0), Decimal(1)), 9100
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 9300)
        cash = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            NativeCashAccountingRequestV2(
                wallet_public_id=scenario.request.fill.wallet_public_id,
                raw_receipt_public_id=captured.raw.public_id,
                authority_reference=reference,
            ),
            native_cash_identity(9400),
            scenario.source.policy,
        )
        request = await next_native_position_request_v2(scenario, monkeypatch, source_log_id=33)
        second = await append_native_position_from_receipts_v2(
            scenario.source.repository, request, native_cash_identity(9600), scenario.source.policy
        )
        assert (first.ledger_sequence, cash.ledger_sequence, second.ledger_sequence) == (1, 2, 3)
        assert second.anchor_public_id == first.anchor_public_id
        assert second.observed.quantity == Decimal(2)
        assert second.derived.quote_turnover_since_anchor == Decimal(200)
        assert second.derived.realized_pnl_since_anchor == Decimal(0)
        async with scenario.source.repository.session() as session:
            history = await load_native_position_history_v2(
                session, scenario.authority.scope, second.instrument_public_id, datetime.now(UTC)
            )
        assert [item.row.ledger_sequence for item in history] == [1, 3]
        assert [
            item.payload.operands.source_position.source_entry.source_log_id for item in history
        ] == [31, 33]
        assert await _financial_counts(scenario) == (2, 3, 3, 0)


async def test_real_changed_predecessor_refuses_then_original_conserving_source_can_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject an actual conflicting old balance without consuming the valid second source identity.

    Given: A genuine position and actual second fill/unit plus its original conserving account-log source exist.
    When: Another actual private frame changes only that second entry's old/new positions to five/six.
    Then: The discontinuous attempt rolls back, while the original one/two source commits once under fresh authority.
    """
    async with native_position_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await append_native_position_from_receipts_v2(
            scenario.source.repository,
            scenario.request,
            native_cash_identity(7000),
            scenario.source.policy,
        )
        original = await next_native_position_request_v2(scenario, monkeypatch)
        async with scenario.source.repository.session() as session:
            source = await get_bound_accounting_observation(
                session, original.position_receipt_public_id
            )
        document = json.loads(base64.b64decode(source.material))
        document["new_entry"].update(old_balance=5, new_balance=6)
        changed = await receive_native_cash_v2(
            scenario.source, monkeypatch, json.dumps(document, separators=(",", ":")).encode(), 9700
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 9900)
        conflicting = NativePositionSourceRequestV2.model_validate(
            {
                **original.model_dump(mode="python"),
                "position_receipt_public_id": changed.raw.public_id,
                "position_authority_reference": reference,
            }
        )
        with pytest.raises(ValueError, match="Position predecessor contradicts native source"):
            await append_native_position_from_receipts_v2(
                scenario.source.repository,
                conflicting,
                native_cash_identity(10000),
                scenario.source.policy,
            )
        assert await _financial_counts(scenario) == (1, 1, 1, 0)
        current = NativePositionSourceRequestV2.model_validate(
            {**original.model_dump(mode="python"), "position_authority_reference": reference}
        )
        second = await append_native_position_from_receipts_v2(
            scenario.source.repository, current, native_cash_identity(10001), scenario.source.policy
        )
        assert second.created and second.anchor_public_id == first.anchor_public_id
        assert second.observed.quantity == Decimal(2)
        assert await _financial_counts(scenario) == (1, 2, 2, 0)
