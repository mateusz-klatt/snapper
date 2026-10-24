"""Compose real prepared-key HTTP, authenticated account-log ownership and native cash transactions."""

from dataclasses import dataclass
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
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.native_cash_store_v2 as cash_module
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_account_projection_store import load_bound_contract_account_snapshot
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.contract_accounting_integrity import run_contract_accounting_restore_audit
from snapper.data.financial_account_owner_policy_writer import FinancialOwnerPolicyChange
from snapper.data.financial_account_owner_policy_writer import write_financial_owner_policy
from snapper.data.futures_cash_ledger_store import NativeCashLedgerWriteResult
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import FinancialPublicationIntent
from snapper.data.models import NativeAccountReadOutcome
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_identity_store import get_bound_native_account_identity
from snapper.data.native_account_read_store import get_bound_native_account_read_outcome
from snapper.data.native_cash_history_v2 import get_bound_native_cash_event_v2
from snapper.data.native_cash_history_v2 import load_native_cash_history_v2
from snapper.data.native_cash_store_v2 import NativeCashAccountingRequestV2
from snapper.data.native_cash_store_v2 import append_bound_native_cash_v2
from snapper.data.native_cash_store_v2 import append_native_cash_from_receipt_v2
from snapper.data.native_financial_source_binding_v2 import NativeFinancialBindingError
from snapper.data.private_source_ownership_store import PrivateSourceOwnershipBindingError
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.kraken_futures_account_receipts import (
    FUTURES_PREPARED_ACCOUNTS_CHANNEL,
)
from tests.data.test_financial_account_owner_store import _alias
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_cash_accounting_v2 import NativeCashScenarioV2
from tests.helpers.native_cash_accounting_v2 import fresh_native_cash_authority_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_identity
from tests.helpers.native_cash_accounting_v2 import native_cash_scenario_v2
from tests.helpers.native_cash_accounting_v2 import native_cash_wire_v2
from tests.helpers.native_cash_accounting_v2 import receive_native_cash_v2

_BALANCE = Decimal("100.000000000000000001")


@dataclass(frozen=True, slots=True)
class _Inventory:
    """Actual durable source and financial row counts, with no completeness or spending claim."""

    private_raw: int
    holds: int
    ownership: int
    anchors: int
    events: int
    claims: int
    resolutions: int
    executions: int
    publications: int


async def _raw_inventory(session: AsyncSession) -> dict[str, tuple[str, str]]:
    """Inventory every retained source before separating financial frames from proved HTTP state."""
    rows = (await session.scalars(select(AccountingRawReceipt))).all()
    result: dict[str, tuple[str, str]] = {}
    for row in rows:
        observation = await get_bound_accounting_observation(session, row.public_id)
        result[row.public_id] = (observation.kind, observation.channel)
    return result


async def _rest_raw_ids(session: AsyncSession) -> set[str]:
    """Rebind every excluded HTTP receipt to its actual immutable request and outcome."""
    attempts = (
        await session.scalars(
            select(NativeAccountReadOutcome.attempt_public_id).where(
                NativeAccountReadOutcome.raw_receipt_public_id.is_not(None)
            )
        )
    ).all()
    result: set[str] = set()
    for attempt in attempts:
        outcome = await get_bound_native_account_read_outcome(session, attempt)
        assert outcome is not None and outcome.payload.source_reference is not None
        raw_id = outcome.payload.source_reference.raw_receipt_public_id
        assert raw_id not in result
        result.add(raw_id)
    return result


async def _private_raw_count(session: AsyncSession) -> int:
    """Account for every raw row, refusing unknown namespaces or HTTP rows without actual outcomes."""
    rows = await _raw_inventory(session)
    rest = await _rest_raw_ids(session)
    private = {
        identity
        for identity, (_, channel) in rows.items()
        if channel in {"kraken_futures.ws.account_log", "kraken_futures.ws.account_log_snapshot"}
    }
    assert private.isdisjoint(rest)
    assert set(rows) == private | rest
    assert all(
        rows[identity] == ("account_cash", FUTURES_PREPARED_ACCOUNTS_CHANNEL) for identity in rest
    )
    assert all(rows[identity][0] == "unknown" for identity in private)
    return len(private)


async def _inventory(repository: SQLAlchemyRepository) -> _Inventory:
    """Count private and financial rows only after proving the complete separate HTTP source set."""
    async with repository.session() as session:
        counts = [await _private_raw_count(session)]
        for model in (
            AccountingScopeHold,
            PrivateSourceOwnershipReceipt,
            ContractAccountingAnchor,
            ContractAccountingEvent,
            AccountingNativeSourceClaim,
            AccountingHoldResolution,
            Execution,
            FinancialPublicationIntent,
        ):
            counts.append(int((await session.scalar(select(func.count()).select_from(model))) or 0))
    return _Inventory(*counts)


async def _assert_initial_source_partition(scenario: NativeCashScenarioV2) -> None:
    """Identify both real HTTP operations independently of the original private financial frame."""
    async with scenario.source.repository.session() as session:
        identity = await get_bound_native_account_identity(
            session, scenario.captured.ownership.payload.frame_context.key_proof_public_id
        )
        socket_attempt = identity.payload.attempt_public_id
        current_attempt = scenario.authority.native_read.attempt_public_id
        assert socket_attempt != current_attempt
        socket_read = await get_bound_native_account_read_outcome(session, socket_attempt)
        current_read = await get_bound_native_account_read_outcome(session, current_attempt)
        assert socket_read is not None and socket_read.payload.source_reference is not None
        assert current_read is not None and current_read.payload.source_reference is not None
        socket_raw = socket_read.payload.source_reference.raw_receipt_public_id
        current_raw = current_read.payload.source_reference.raw_receipt_public_id
        assert socket_raw != current_raw
        assert current_raw == scenario.authority.native_read.raw_receipt_public_id
        assert await _raw_inventory(session) == {
            scenario.captured.raw.public_id: ("unknown", "kraken_futures.ws.account_log"),
            socket_raw: ("account_cash", FUTURES_PREPARED_ACCOUNTS_CHANNEL),
            current_raw: ("account_cash", FUTURES_PREPARED_ACCOUNTS_CHANNEL),
        }
        holds = (await session.scalars(select(AccountingScopeHold.raw_receipt_public_id))).all()
        assert list(holds) == [scenario.captured.raw.public_id]


async def _append(
    scenario: NativeCashScenarioV2, number: int = 2000
) -> NativeCashLedgerWriteResult:
    """Use the actual owned financial transaction and its post-wallet-fence production clock."""
    return await append_native_cash_from_receipt_v2(
        scenario.source.repository,
        scenario.request,
        native_cash_identity(number),
        scenario.source.policy,
    )


async def _revoke(scenario: NativeCashScenarioV2) -> None:
    """Commit actual explicit revocation through the reserved policy writer after posting."""
    result = await write_financial_owner_policy(
        scenario.source.repository,
        FinancialOwnerPolicyChange(
            policy=FinancialAccountOwnerPolicy(assignments=()),
            updated_by="isolated-owner-revocation",
        ),
        scenario.source.tracker,
    )
    assert result.changed is True


async def test_actual_owned_cash_commits_one_exact_native_fact_and_keeps_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Book one actual native balance leg without manufacturing account readiness or executions.

    Given: Real local signed HTTP and SDK account-log subscription prove raw, UID and explicit owner.
    When: The actual wallet-fenced cash producer commits one original numeric source entry.
    Then: One exact anchor/event/shared claim exists, while holds stay pending and no fill is invented.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        await _assert_initial_source_partition(scenario)
        before = await _inventory(repository)
        assert before == _Inventory(1, 1, 1, 0, 0, 0, 0, 0, 0)
        result = await _append(scenario)
        assert result.created and result.observed_balance == _BALANCE
        assert result.ledger_sequence == 1 and result.native_currency == "USD"
        assert await _inventory(repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)
        async with repository.session() as session:
            event = await get_bound_native_cash_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        assert event.payload.operands.cash_before == Decimal(0)
        assert event.payload.operands.cash_after == _BALANCE
        assert event.payload.operands.cash_delta == _BALANCE
        assert event.anchor.operands.source_execution_scope_sequence is None
        assert event.anchor.operands.source_execution_prefix_status == "unavailable"
        assert event.payload.source.ownership_reference == scenario.captured.ownership.reference
        assert event.payload.source.authority_reference == scenario.authority_reference
        assert event.source.ownership.account_uid == scenario.authority.venue_account_id
        assert event.payload.source.owner_evidence.decided_at == event.row.timestamp
        assert event.row.timestamp >= scenario.captured.ownership.recorded_at
        assert event.row.timestamp >= scenario.authority_reference.recorded_at


async def test_retry_after_revocation_and_authority_expiry_preserves_original_fact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Return an existing immutable economic fact without reactivating revoked current admission.

    Given: A real cash fact committed before explicit policy revocation and source expiry.
    When: Its exact proved receipt is redelivered under a later actual caller transaction.
    Then: The original identity, digest and original policy interval survive without a second posting.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await _append(scenario)
        await _revoke(scenario)
        clock = max(datetime.now(UTC), scenario.authority.valid_until + timedelta(seconds=1))
        repository = scenario.source.repository
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(
                session, scenario.request.wallet_public_id
            )
            before = await get_bound_native_cash_event_v2(session, first.public_id, clock)
            payload = before.row.canonical_payload
            retry = await append_bound_native_cash_v2(
                session,
                scenario.request,
                SnapshotWriteProvenance(
                    **native_cash_identity(2001).model_dump(mode="python"),
                    timestamp=clock,
                ),
                scenario.source.policy,
            )
            await session.commit()
        assert retry.created is False
        assert (retry.public_id, retry.content_digest, retry.ledger_sequence) == (
            first.public_id,
            first.content_digest,
            first.ledger_sequence,
        )
        async with repository.session() as session:
            rebound = await get_bound_native_cash_event_v2(session, first.public_id, clock)
        assert rebound.row.canonical_payload == payload
        assert (
            rebound.payload.source.owner_evidence.policy.assignments == scenario.policy.assignments
        )
        assert await _inventory(repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)


async def test_second_actual_cash_source_conserves_chain_under_original_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Accept a second real source only after full original owner and balance history rebinds.

    Given: A genuine native-v2 cash prefix already pins one actual external UID to its owner.
    When: A separately authenticated source reports the exact predecessor balance and fresh authority.
    Then: The existing partial anchor is retained and the next sequence carries only the actual delta.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await _append(scenario)
        after = Decimal("99.900000000000000001")
        captured = await receive_native_cash_v2(
            scenario.source, monkeypatch, native_cash_wire_v2(18, _BALANCE, after), 2100
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 2500)
        request = NativeCashAccountingRequestV2(
            wallet_public_id=scenario.request.wallet_public_id,
            raw_receipt_public_id=captured.raw.public_id,
            authority_reference=reference,
        )
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            request,
            native_cash_identity(2600),
            scenario.source.policy,
        )
        assert result.created and result.anchor_public_id == first.anchor_public_id
        assert result.ledger_sequence == first.ledger_sequence + 1
        assert result.observed_balance == after
        async with scenario.source.repository.session() as session:
            events = await load_native_cash_history_v2(
                session, scenario.authority.scope, datetime.now(UTC)
            )
        assert [item.payload.operands.cash_delta for item in events] == [
            _BALANCE,
            Decimal("-0.1"),
        ]
        assert await _inventory(scenario.source.repository) == _Inventory(2, 2, 2, 1, 2, 2, 0, 0, 0)


async def test_real_claim_insert_failure_rolls_back_every_financial_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Roll back the actual anchor/event/claim transaction when the database rejects final source ownership.

    Given: Committed original raw/hold/sidecar exists and a real extra INSERT trigger rejects claims.
    When: The financial appender reaches its actual shared-claim flush.
    Then: No financial row survives, source evidence stays durable, and a later clean retry can commit.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        async with repository.session() as session:
            await session.execute(
                text(
                    "CREATE TRIGGER isolated_native_claim_insert_refusal BEFORE INSERT ON accounting_native_source_claims "
                    "BEGIN SELECT RAISE(ABORT, 'isolated_native_claim_insert_refused'); END"
                )
            )
            await session.commit()
        with pytest.raises(IntegrityError, match="isolated_native_claim_insert_refused"):
            await _append(scenario)
        assert await _inventory(repository) == _Inventory(1, 1, 1, 0, 0, 0, 0, 0, 0)
        async with repository.session() as session:
            await session.execute(text("DROP TRIGGER isolated_native_claim_insert_refusal"))
            await session.commit()
        result = await _append(scenario)
        assert result.created is True and result.ledger_sequence == 1
        assert await _inventory(repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)


async def test_changed_content_for_same_native_id_refuses_without_rewriting_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject contradictory authentic source redelivery independently of its new local receipt identity.

    Given: A genuine native log ID already owns one exact committed cash event.
    When: Another real account-log subscription reports that ID with a different ending balance.
    Then: The original event stays unchanged and the contradictory new raw, hold and sidecar remain retained.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await _append(scenario)
        captured = await receive_native_cash_v2(
            scenario.source,
            monkeypatch,
            native_cash_wire_v2(17, Decimal(0), Decimal("999")),
            2100,
        )
        request = scenario.request.model_copy(
            update={"raw_receipt_public_id": captured.raw.public_id}
        )
        with pytest.raises(
            NativeFinancialBindingError,
            match="native_financial_cash_source_identity_conflict",
        ):
            await append_native_cash_from_receipt_v2(
                scenario.source.repository,
                request,
                native_cash_identity(2600),
                scenario.source.policy,
            )
        async with scenario.source.repository.session() as session:
            original = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        assert original.row.content_digest == first.content_digest
        assert original.payload.operands.cash_after == _BALANCE
        assert await _inventory(scenario.source.repository) == _Inventory(2, 2, 2, 1, 1, 1, 0, 0, 0)


async def test_resnapshot_redelivery_changes_only_transport_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deduplicate the same real source when a later account-log snapshot moves its JSON selector.

    Given: One original account-log/new_entry already committed a source-bound cash fact.
    When: A separately authenticated account_log_snapshot contains that same native entry in logs/0.
    Then: No second claim appears and the first source sidecar, selector, identity and clock stay immutable.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await _append(scenario)
        original_wire = native_cash_wire_v2(17, Decimal(0), _BALANCE)
        entry = original_wire.removeprefix(b'{"feed":"account_log","new_entry":').removesuffix(b"}")
        snapshot = b'{"feed":"account_log_snapshot","logs":[' + entry + b"]}"
        captured = await receive_native_cash_v2(
            scenario.source, monkeypatch, snapshot, 2100, pointer="/logs/0"
        )
        request = scenario.request.model_copy(
            update={"raw_receipt_public_id": captured.raw.public_id}
        )
        result = await append_native_cash_from_receipt_v2(
            scenario.source.repository,
            request,
            native_cash_identity(2600),
            scenario.source.policy,
        )
        assert not result.created and result.public_id == first.public_id
        async with scenario.source.repository.session() as session:
            original = await get_bound_native_cash_event_v2(
                session, first.public_id, datetime.now(UTC)
            )
        assert original.payload.source.ownership_reference == scenario.captured.ownership.reference
        assert original.payload.operands.source_entry.target_json_pointer == "/new_entry"
        assert await _inventory(scenario.source.repository) == _Inventory(2, 2, 2, 1, 1, 1, 0, 0, 0)


async def test_current_revocation_refuses_a_new_native_source_without_losing_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep historical posting valid while rejecting new financial interpretation after explicit revocation.

    Given: A real original cash event and a later committed empty owner policy.
    When: A fresh authenticated next source and prepared-key authority request another financial event.
    Then: Admission refuses, preserving the old book and both raw source holds without creating cash.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        await _append(scenario)
        await _revoke(scenario)
        captured = await receive_native_cash_v2(
            scenario.source,
            monkeypatch,
            native_cash_wire_v2(18, _BALANCE, Decimal("101")),
            2100,
        )
        reference, _authority = await fresh_native_cash_authority_v2(scenario.source, 2500)
        request = NativeCashAccountingRequestV2(
            wallet_public_id=scenario.request.wallet_public_id,
            raw_receipt_public_id=captured.raw.public_id,
            authority_reference=reference,
        )
        with pytest.raises(
            FinancialAccountOwnerUnavailableError,
            match="Explicit financial account owner assignment is unavailable",
        ):
            await append_native_cash_from_receipt_v2(
                scenario.source.repository,
                request,
                native_cash_identity(2600),
                scenario.source.policy,
            )
        assert await _inventory(scenario.source.repository) == _Inventory(2, 2, 2, 1, 1, 1, 0, 0, 0)


async def test_prior_cash_history_pins_owner_against_an_explicit_alias_reassignment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require migration of prior actual economics instead of allowing current configuration to move a book.

    Given: A committed cash book and a second safe read-only identity root for the same external UID.
    When: An administrator explicitly assigns that UID to the other local wallet.
    Then: The dedicated writer refuses the transfer and the original owner and cash event stay valid.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await _append(scenario)
        alias = await _alias(scenario.source)
        await fresh_native_cash_authority_v2(alias, 2800)
        reassigned = FinancialAccountOwnerPolicy(
            assignments=(
                FinancialAccountOwnerAssignment(
                    owner_key=financial_account_owner_key_from_authority(scenario.authority),
                    owner_wallet_public_id=alias.selection.wallet_public_id,
                ),
            )
        )
        with pytest.raises(
            FinancialAccountOwnerUnavailableError,
            match="financial_owner_native_history_or_migration_unproven",
        ):
            await write_financial_owner_policy(
                scenario.source.repository,
                FinancialOwnerPolicyChange(
                    policy=reassigned, updated_by="isolated-explicit-alias-transfer"
                ),
                scenario.source.tracker,
            )
        result = await _append(scenario, 2900)
        assert result.created is False and result.public_id == first.public_id
        assert await _inventory(scenario.source.repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)


async def test_new_repository_replays_actual_source_policy_and_cash_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reconstruct the original financial book without relying on in-memory authenticated tickets or cache.

    Given: A real cash event committed and its original policy was later explicitly revoked.
    When: Another actual repository opens the same isolated durable database after those transactions.
    Then: Full historical cash replay returns the original source, amount, policy interval and claim.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        first = await _append(scenario)
        await _revoke(scenario)
        repository = SQLAlchemyRepository(str(scenario.source.repository.engine.url))
        try:
            async with repository.session() as session:
                history = await load_native_cash_history_v2(
                    session, scenario.authority.scope, datetime.now(UTC)
                )
            assert len(history) == 1
            event = history[0]
            assert event.row.public_id == first.public_id
            assert event.payload.operands.cash_after == _BALANCE
            assert event.source.ownership.reference == scenario.captured.ownership.reference
            assert event.payload.source.owner_evidence.policy == scenario.policy
            assert event.anchor.operands.source_execution_scope_sequence is None
        finally:
            await repository.engine.dispose()


async def test_bound_native_cash_requires_an_actual_outer_transaction(
    tmp_path: Path,
) -> None:
    """Refuse an absent caller transaction before touching any source or financial graph.

    Given: A real AsyncSession has no begun outer transaction and no financial fixture was staged.
    When: The bound cash writer receives a strictly shaped reference request.
    Then: Its immediate transaction precondition refuses without opening or changing a book.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'unbegun.db'}")
    request = NativeCashAccountingRequestV2(
        wallet_public_id=native_authority_uuid(1),
        raw_receipt_public_id=native_authority_uuid(1100),
        authority_reference=AccountAuthorityReference(
            family="native_accounts_v1",
            public_id=native_authority_uuid(1500),
            content_digest="a" * 64,
            recorded_at=datetime.now(UTC),
        ),
    )
    try:
        async with AsyncSession(repository.engine) as session:
            assert not session.in_transaction()
            with pytest.raises(RuntimeError, match="transaction"):
                await append_bound_native_cash_v2(
                    session,
                    request,
                    SnapshotWriteProvenance(
                        **native_cash_identity(2000).model_dump(mode="python"),
                        timestamp=datetime.now(UTC),
                    ),
                    NativeAuthorityFreshnessPolicy(timedelta(seconds=60), timedelta(seconds=60)),
                )
            assert not (tmp_path / "unbegun.db").exists()
    finally:
        await repository.engine.dispose()


async def test_missing_actual_sidecar_keeps_unowned_raw_held_without_cash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse raw capture alone even when nominal account metadata and source content match.

    Given: Genuine owned source exists and another actual raw-and-hold transaction has no sidecar.
    When: Financial interpretation names the unowned receipt under the actual native authority.
    Then: The mandatory bound ownership reader refuses before creating any financial row.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        observation = scenario.captured.observation.model_copy(
            update={"observed_at": datetime.now(UTC)}
        )
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(
                session, scenario.request.wallet_public_id
            )
            raw = await capture_bound_accounting_observation(
                session,
                observation,
                AccountingWriteProvenance(
                    **native_cash_identity(2100).model_dump(mode="python"),
                    timestamp=datetime.now(UTC),
                ),
            )
            assert raw.created and not raw.conflicted
            await session.commit()
        request = scenario.request.model_copy(update={"raw_receipt_public_id": raw.public_id})
        with pytest.raises(
            PrivateSourceOwnershipBindingError, match="private_ownership_sidecar_missing"
        ):
            await append_native_cash_from_receipt_v2(
                repository, request, native_cash_identity(2600), scenario.source.policy
            )
        assert await _inventory(repository) == _Inventory(2, 2, 1, 0, 0, 0, 0, 0, 0)


async def test_owned_producer_samples_recording_clock_after_actual_wallet_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retain honest local knowledge after the actual wallet transaction lock has been acquired.

    Given: A genuine native cash source and the real repository wallet-fence implementation.
    When: The owned producer passes that actual fence and then creates its provenance.
    Then: Its retained event clock cannot precede the independently observed post-fence instant.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        repository = scenario.source.repository
        original = repository._acquire_execution_fence_lock
        acquired: list[datetime] = []

        async def _fence(session: AsyncSession, wallet_public_id: str) -> None:
            """Observe completion of the actual wallet fence without replacing its locking behavior."""
            await original(session, wallet_public_id)
            acquired.append(datetime.now(UTC))

        monkeypatch.setattr(repository, "_acquire_execution_fence_lock", _fence)
        result = await cash_module.append_native_cash_from_receipt_v2(
            repository, scenario.request, native_cash_identity(2000), scenario.source.policy
        )
        assert len(acquired) == 1
        async with repository.session() as session:
            event = await get_bound_native_cash_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
        assert event.row.timestamp >= acquired[0]


async def test_cash_event_is_unknown_before_its_actual_retained_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep provider occurrence time separate from actual local financial knowledge.

    Given: A real source dated earlier than its actual durable cash posting.
    When: Replay requests a cutoff immediately before that event's retained row clock.
    Then: The event is unavailable at that cutoff and its later observed balance is not projected backward.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await _append(scenario)
        repository = scenario.source.repository
        async with repository.session() as session:
            bound = await get_bound_native_cash_event_v2(
                session, result.public_id, datetime.now(UTC)
            )
            cutoff = bound.row.timestamp - timedelta(microseconds=1)
            history = await load_native_cash_history_v2(session, scenario.authority.scope, cutoff)
            assert history == ()
            with pytest.raises(
                NativeFinancialBindingError, match="native_financial_cash_event_not_known_at_cutoff"
            ):
                await get_bound_native_cash_event_v2(session, result.public_id, cutoff)
        assert await _inventory(repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)


async def test_finite_restore_audit_rebinds_native_cash_and_shared_source_claim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pass the actual finite boot audit for a native cash book after its original owner policy closes.

    Given: A genuine cash event, original transport sidecar and closed original policy remain durable.
    When: The real global restore boundary inventories every financial row and binds its strict schema.
    Then: Exactly one cash event/claim/anchor is verified without inventing a publication or readiness.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        await _append(scenario)
        await _revoke(scenario)
        report = await run_contract_accounting_restore_audit(scenario.source.repository)
        assert report.event_count == report.claim_count == report.anchor_count == 1
        assert report.publication_count == report.delivery_count == 0
        assert report.scopes == (scenario.authority.scope,)
        assert len(report.physical_guards) == 26
        assert len(report.retained_graph_digest) == 64
        assert await _inventory(scenario.source.repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)


async def test_public_account_projection_hydrates_native_cash_as_partial_and_held(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Read native cash through the existing actual account consumer without implying a complete account.

    Given: A genuine native cash book exists while its source hold remains unresolved.
    When: The public projection reader refreshes the exact scope under the actual wallet fence.
    Then: Its journal and exact observed cash bind to that event, while positions and readiness stay unknown.
    """
    async with native_cash_scenario_v2(tmp_path, monkeypatch) as scenario:
        result = await _append(scenario)
        repository = scenario.source.repository
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(
                session, scenario.request.wallet_public_id
            )
            snapshot = await load_bound_contract_account_snapshot(
                session, scenario.authority.scope, datetime.now(UTC)
            )
            await session.commit()
        assert snapshot.scope == scenario.authority.scope
        assert len(snapshot.journal) == len(snapshot.native_cash) == 1
        assert snapshot.journal[0].public_id == result.public_id
        assert snapshot.native_cash[0].balance == _BALANCE
        assert snapshot.native_cash[0].native_currency == "USD"
        assert snapshot.positions == snapshot.reported_cash_components == ()
        assert snapshot.inventory_status == snapshot.owner_identity_status == "unproven"
        assert snapshot.execution_prefix_status == "unavailable" and snapshot.readiness == "held"
        assert len(snapshot.unresolved_holds) == 1
        assert snapshot.unresolved_holds[0].raw_receipt_public_id == scenario.captured.raw.public_id
        assert await _inventory(repository) == _Inventory(1, 1, 1, 1, 1, 1, 0, 0, 0)
