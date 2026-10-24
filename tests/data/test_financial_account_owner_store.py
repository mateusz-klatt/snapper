"""Bind real native HTTP identity to versioned explicit owner policy without enabling trades."""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from requests import Session
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.financial_account_owner_store as owner_store
from snapper.config.credentials import CredentialVersion
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.financial_account_owner import FINANCIAL_OWNER_SETTING_KEY
from snapper.core.financial_account_owner import FinancialAccountOwnerAssignment
from snapper.core.financial_account_owner import FinancialAccountOwnerEvidence
from snapper.core.financial_account_owner import FinancialAccountOwnerKey
from snapper.core.financial_account_owner import FinancialAccountOwnerMismatchError
from snapper.core.financial_account_owner import FinancialAccountOwnerPolicy
from snapper.core.financial_account_owner import FinancialAccountOwnerUnavailableError
from snapper.core.financial_account_owner import canonical_financial_account_owner_policy
from snapper.core.financial_account_owner import financial_account_owner_key_digest
from snapper.core.financial_account_owner import financial_account_owner_key_from_authority
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.data.contract_accounting_triggers import install_contract_accounting_table_triggers
from snapper.data.financial_account_owner_policy_store import get_bound_financial_owner_evidence
from snapper.data.financial_account_owner_policy_store import load_current_financial_owner_policy
from snapper.data.financial_account_owner_store import FINANCIAL_OWNER_LOCK_NAMESPACE
from snapper.data.financial_account_owner_store import _owner_barrier
from snapper.data.financial_account_owner_store import _require_supported_history
from snapper.data.financial_account_owner_store import load_current_financial_owner_evidence
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AccountingNativeSourceClaim
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import NativeAccountAuthority
from snapper.data.models import Setting
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from tests.helpers.contract_position_accounting import seed_contract_position_scenario
from tests.helpers.contract_position_accounting import stage_contract_position
from tests.helpers.native_account_authority import NATIVE_AUTHORITY_TEST_TIME
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import build_native_authority_payload
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.native_account_authority import insert_native_authority
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid as _id


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[NativeAuthorityScenario]:
    """Own one actual isolated SQLite database and local signed HTTP fixture lifetime."""
    async with native_authority_scenario(tmp_path, monkeypatch) as value:
        yield value


async def _authority(
    scenario: NativeAuthorityScenario, number: int = 500
) -> tuple[AccountAuthorityReference, NativeAccountAuthorityPayload]:
    """Run real local signed key/accounts HTTP and commit its independently bound authority."""
    outcome = await capture_native_account_read(scenario)
    payload = await build_native_authority_payload(scenario, outcome)
    result = await insert_native_authority(scenario, payload, number)
    return (
        AccountAuthorityReference(
            family="native_accounts_v1",
            public_id=result.public_id,
            content_digest=result.content_digest,
            recorded_at=scenario.clock.current,
        ),
        payload,
    )


async def _policy(
    scenario: NativeAuthorityScenario,
    payload: NativeAccountAuthorityPayload,
    number: int = 600,
    owner: str | None = None,
    changes: dict[str, object] | None = None,
) -> FinancialAccountOwnerPolicy:
    """Commit an explicit actual SCD2 configuration revision without any financial write."""
    assignment = FinancialAccountOwnerAssignment(
        owner_key=financial_account_owner_key_from_authority(payload),
        owner_wallet_public_id=owner if owner is not None else payload.scope.wallet_public_id,
    )
    policy = FinancialAccountOwnerPolicy(assignments=(assignment,))
    async with scenario.repository.session() as session:
        previous = await session.scalar(
            select(Setting).where(
                Setting.key == FINANCIAL_OWNER_SETTING_KEY, Setting.known_to == KNOWN_TO_MAX
            )
        )
        if previous is not None:
            previous.known_to = scenario.clock.current
        row = Setting(
            public_id=_id(number),
            session_id=_id(9),
            sequence_id=number,
            timestamp=scenario.clock.current,
            key=FINANCIAL_OWNER_SETTING_KEY,
            value=canonical_financial_account_owner_policy(policy),
            category="financial_owner",
            is_encrypted=False,
            updated_by="explicit-isolated-fixture-owner",
        )
        for name, value in (changes or {}).items():
            setattr(row, name, value)
        session.add(row)
        await session.commit()
    return policy


async def _current(
    scenario: NativeAuthorityScenario,
    reference: AccountAuthorityReference,
    decided_at: datetime | None = None,
) -> FinancialAccountOwnerEvidence:
    """Read configuration under the actual repository transaction and wallet fence."""
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, scenario.selection.wallet_public_id
        )
        evidence = await load_current_financial_owner_evidence(
            session, reference, decided_at if decided_at is not None else scenario.clock.current
        )
        await session.commit()
        return evidence


async def _alias(
    scenario: NativeAuthorityScenario, wallet_number: int = 10
) -> NativeAuthorityScenario:
    """Bind a distinct actual SDK client and committed credential to a local alias of the same UID."""
    credential_at = NATIVE_AUTHORITY_TEST_TIME - timedelta(days=1)
    async with scenario.repository.session() as session:
        wallet = await session.scalar(select(Wallet).where(Wallet.public_id == _id(wallet_number)))
        if wallet is None:
            session.add(
                Wallet(
                    public_id=_id(wallet_number),
                    session_id=_id(9),
                    sequence_id=wallet_number,
                    timestamp=credential_at,
                    label="explicit-read-only-alias",
                    is_paper=False,
                )
            )
        credential = await session.scalar(
            select(WalletCredential).where(
                WalletCredential.wallet_public_id == _id(wallet_number),
                WalletCredential.exchange == "kraken_futures",
                WalletCredential.known_to == KNOWN_TO_MAX,
            )
        )
        if credential is None:
            credential = WalletCredential(
                public_id=_id(wallet_number + 1),
                session_id=_id(9),
                sequence_id=wallet_number + 1,
                timestamp=credential_at,
                wallet_public_id=_id(wallet_number),
                exchange="kraken_futures",
                credential_type="api_key_secret",
                encrypted_payload="fixture-only-not-decrypted",
            )
            session.add(credential)
        actual_credential = CredentialVersion(
            public_id=credential.public_id,
            wallet_public_id=credential.wallet_public_id,
            exchange=credential.exchange,
            credential_type=credential.credential_type,
            timestamp=credential.timestamp,
        )
        await session.commit()
    client = KrakenFuturesExchangeClient(
        api_key="synthetic-alias-key", api_secret="c3ludGhldGljLXNlY3JldA=="
    )
    client.bind_execution_identity(
        _id(wallet_number),
        actual_credential,
    )
    selection = client.execution_client_selection()
    assert selection is not None
    user = client._user_client
    assert user is not None
    sdk_session = getattr(user, "_FuturesClient__session", None)
    assert isinstance(sdk_session, Session)
    sdk_session.mount("https://", scenario.adapter)
    scenario.clients.append(client)
    scenario.sessions.append(sdk_session)
    return replace(scenario, client=client, selection=selection)


async def test_current_evidence_uses_actual_setting_revision_and_creates_no_financial_permission(
    scenario: NativeAuthorityScenario,
) -> None:
    """Bind explicit configuration to the real closed HTTP graph without writing a funded book.

    Given: Real local signed key/accounts responses and one explicit persisted owner policy.
    When: Current evidence is read inside the wallet-fenced transaction.
    Then: Exact version/native references survive while financial tables remain empty.
    """
    reference, native = await _authority(scenario)
    policy = await _policy(scenario, native)
    evidence = await _current(scenario, reference)

    assert evidence.policy == policy
    assert evidence.policy_reference.public_id == _id(600)
    assert evidence.authority_reference == reference
    assert evidence.assignment.owner_wallet_public_id == scenario.selection.wallet_public_id
    async with scenario.repository.session() as session:
        assert await get_bound_financial_owner_evidence(session, evidence) == evidence
        for model in (ContractAccountingEvent, AccountingNativeSourceClaim):
            assert await session.scalar(select(func.count()).select_from(model)) == 0


async def test_missing_policy_and_actual_foreign_alias_owner_are_distinct_refusals(
    scenario: NativeAuthorityScenario,
) -> None:
    """Do not infer ownership from a native read root or prefer a local alias.

    Given: One actual native authority exists before any policy or financial history.
    When: Configuration is absent and is later explicitly assigned to another wallet.
    Then: Both reads refuse with their specific missing-policy or foreign-owner reason.
    """
    reference, native = await _authority(scenario)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="current_setting_missing"):
        await _current(scenario, reference)
    await _policy(scenario, native, owner=_id(10))
    with pytest.raises(FinancialAccountOwnerMismatchError, match="another configured wallet"):
        await _current(scenario, reference)


async def test_policy_can_change_owner_before_financial_history_and_prior_revision_remains_bound(
    scenario: NativeAuthorityScenario,
) -> None:
    """Allow explicit pre-book reassignment without rewriting a retained historical decision.

    Given: Wallet one has current configuration evidence and no financial events.
    When: A real alias reads the same UID and a later SCD2 policy explicitly names it.
    Then: New alias configuration binds while the closed original policy decision still replays.
    """
    original_reference, original_native = await _authority(scenario)
    await _policy(scenario, original_native)
    original = await _current(scenario, original_reference)
    scenario.clock.current += timedelta(seconds=1)
    alias = await _alias(scenario)
    alias_reference, alias_native = await _authority(alias, 501)
    await _policy(alias, alias_native, 601)
    reassigned = await _current(alias, alias_reference)

    assert reassigned.assignment.owner_wallet_public_id == _id(10)
    async with scenario.repository.session() as session:
        assert await get_bound_financial_owner_evidence(session, original) == original
    with pytest.raises(FinancialAccountOwnerMismatchError):
        await _current(scenario, original_reference)
    scenario.clock.current += timedelta(seconds=1)
    await _policy(
        alias, alias_native, 602, changes={"value": '{"schema_version":1,"assignments":[]}'}
    )
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="assignment is unavailable"):
        await _current(alias, alias_reference)


@pytest.mark.parametrize(
    "changes",
    [
        {"category": "other"},
        {"is_encrypted": True},
        {"timestamp": NATIVE_AUTHORITY_TEST_TIME + timedelta(seconds=1)},
        {"value": '{"assignments":[],"assignments":[]}'},
    ],
)
async def test_invalid_setting_metadata_future_clock_and_ambiguous_json_are_not_owner_proof(
    scenario: NativeAuthorityScenario, changes: dict[str, object]
) -> None:
    """Refuse unsafe actual configuration revisions before any financial interpretation.

    Given: An actual native source and a persisted policy with one damaged metadata or value field.
    When: Current configuration evidence is requested.
    Then: The bound reader refuses without creating financial events or replacing raw source evidence.
    """
    reference, native = await _authority(scenario)
    await _policy(scenario, native, changes=changes)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="financial_owner_setting"):
        await _current(scenario, reference)


async def test_historical_reader_rejects_missing_revision_and_changed_original_policy(
    scenario: NativeAuthorityScenario,
) -> None:
    """Require the exact retained version and its semantics rather than a similar active value.

    Given: An actual original owner decision is retained.
    When: Its reference names an absent version or its original persisted value is changed.
    Then: Each exact replay refuses rather than selecting another version or accepting a new value.
    """
    reference, native = await _authority(scenario)
    await _policy(scenario, native)
    original = await _current(scenario, reference)
    missing = original.model_copy(
        update={
            "policy_reference": original.policy_reference.model_copy(update={"public_id": _id(999)})
        }
    )
    async with scenario.repository.session() as session:
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="historical_setting_missing"
        ):
            await get_bound_financial_owner_evidence(session, missing)
        row = await session.scalar(
            select(Setting).where(Setting.public_id == original.policy_reference.public_id)
        )
        assert row is not None
        row.value = '{"schema_version":1,"assignments":[]}'
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="historical_setting_changed"
        ):
            await get_bound_financial_owner_evidence(session, original)


async def test_pure_consistent_policy_cannot_borrow_an_actual_native_authority_for_another_uid(
    scenario: NativeAuthorityScenario,
) -> None:
    """Rebind policy identity to the actual owner UID instead of trusting consistent caller models.

    Given: A real authority belongs to one UID but an explicit configured document names another.
    When: Pure consistent evidence combines that policy with the real authority reference.
    Then: Historical graph binding refuses the owner mismatch despite internally matching digests.
    """
    reference, native = await _authority(scenario)
    assignment = FinancialAccountOwnerAssignment(
        owner_key=FinancialAccountOwnerKey(environment="production", account_uid=_id(99)),
        owner_wallet_public_id=native.scope.wallet_public_id,
    )
    policy = FinancialAccountOwnerPolicy(assignments=(assignment,))
    await _policy(
        scenario, native, changes={"value": canonical_financial_account_owner_policy(policy)}
    )
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, scenario.selection.wallet_public_id
        )
        bound = await load_current_financial_owner_policy(session, scenario.clock.current)
        evidence = FinancialAccountOwnerEvidence(
            policy_reference=bound.reference,
            policy=policy,
            assignment=assignment,
            authority_reference=reference,
            decided_at=scenario.clock.current,
        )
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="assignment_authority_mismatch"
        ):
            await get_bound_financial_owner_evidence(session, evidence)


async def test_current_entrypoint_rejects_non_native_family_and_preexisting_staged_facts(
    scenario: NativeAuthorityScenario,
) -> None:
    """Require the source family and early-call ordering before unclaimed pending rows can be hidden.

    Given: A real native authority and a separate transaction with a pending financial model.
    When: The current reader receives a wrong family or staged accounting event.
    Then: It refuses before querying policy or flushing unrelated financial state.
    """
    reference, _native = await _authority(scenario)
    async with scenario.repository.session() as session:
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="native_authority_required"
        ):
            await load_current_financial_owner_evidence(
                session,
                reference.model_copy(update={"family": "legacy_route_v1"}),
                scenario.clock.current,
            )
        session.add(ContractAccountingEvent())
        with pytest.raises(FinancialAccountOwnerUnavailableError, match="staged_financial_rows"):
            await load_current_financial_owner_evidence(session, reference, scenario.clock.current)
        await session.rollback()


async def test_future_native_alias_evidence_and_unknown_uid_history_are_refused(
    scenario: NativeAuthorityScenario,
) -> None:
    """Keep actual retained authority knowledge separate from requested decision time.

    Given: One owner reference predates another real alias authority for the same UID.
    When: Current evidence is backdated before that alias or a nonexistent UID history is checked.
    Then: Both cases refuse rather than ignore relevant evidence or infer an empty book.
    """
    reference, native = await _authority(scenario)
    await _policy(scenario, native)
    original_clock = scenario.clock.current
    scenario.clock.current += timedelta(seconds=1)
    alias = await _alias(scenario)
    await _authority(alias, 501)
    with pytest.raises(
        FinancialAccountOwnerUnavailableError, match="history_native_authority_mismatch"
    ):
        await _current(scenario, reference, original_clock)
    with pytest.raises(FinancialAccountOwnerUnavailableError, match="authority_not_known"):
        await _current(scenario, reference, original_clock - timedelta(microseconds=1))
    async with scenario.repository.session() as session:
        with pytest.raises(
            FinancialAccountOwnerUnavailableError, match="history_native_authority_missing"
        ):
            await _require_supported_history(
                session,
                FinancialAccountOwnerKey(environment="production", account_uid=_id(999)),
                scenario.clock.current,
            )


@pytest.mark.parametrize(
    "retained_event,profile_authority", [(True, True), (False, True), (True, False)]
)
async def test_real_legacy_position_under_uid_linked_alias_blocks_policy_reassignment(
    scenario: NativeAuthorityScenario,
    monkeypatch: pytest.MonkeyPatch,
    retained_event: bool,
    profile_authority: bool,
) -> None:
    """Retain unknown legacy ownership rather than certify a funded transfer from read-only roots.

    Given: A genuine paired fill/log has a committed position and source claim, possibly after incomplete restore.
    When: Its wallet obtains a real native UID authority and policy selects a different alias.
    Then: Current evidence refuses unsupported financial history even though both read roots agree.
    """
    original_reference, original_native = await _authority(scenario)
    legacy = await seed_contract_position_scenario(scenario.repository, monkeypatch)
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        _request, result = await stage_contract_position(session, legacy)
        await session.commit()
    if not retained_event:
        async with scenario.repository.session() as session:
            await session.execute(text("DROP TRIGGER contract_accounting_events_reject_delete"))
            await session.execute(
                delete(ContractAccountingEvent).where(
                    ContractAccountingEvent.public_id == result.public_id
                )
            )
            connection = await session.connection()
            await connection.run_sync(
                install_contract_accounting_table_triggers, "contract_accounting_events"
            )
            await session.commit()
    alias = await _alias(scenario, 3)
    if profile_authority:
        await _authority(alias, 501)
    else:
        await capture_native_account_read(alias)
    await _policy(scenario, original_native)
    with pytest.raises(
        FinancialAccountOwnerUnavailableError, match="native_history_or_migration_unproven"
    ):
        await _current(scenario, original_reference)


async def test_repeated_actual_profile_reads_do_not_rebind_all_authorities_for_owner_discovery(
    scenario: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bound owner discovery by actual initial identity roots rather than read-history length.

    Given: Three actual signed HTTP reads issue three independent profile authorities for one UID.
    When: Current explicit owner evidence is read with a tracing wrapper around the real root reader.
    Then: Discovery rebinds one actual initial root while every profile row remains retained.
    """
    reference, native = await _authority(scenario)
    await _policy(scenario, native)
    for number in (501, 502):
        scenario.clock.current += timedelta(seconds=1)
        reference, _native = await _authority(scenario, number)
    original = owner_store.get_bound_native_account_identity
    rebound: list[str] = []

    async def _observe(session: AsyncSession, public_id: str) -> BoundNativeAccountIdentityProof:
        """Record actual discovery calls while retaining the genuine source graph reader."""
        rebound.append(public_id)
        return await original(session, public_id)

    monkeypatch.setattr(owner_store, "get_bound_native_account_identity", _observe)
    evidence = await _current(scenario, reference)

    assert len(rebound) == 1
    assert evidence.authority_reference == reference
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 3


async def test_external_owner_advisory_uses_uid_key_and_disjoint_namespace_before_policy_read() -> (
    None
):
    """Verify the exact PostgreSQL boundary operands without claiming actual lock concurrency proof.

    Given: A typed SQLAlchemy boundary reports PostgreSQL and one external owner key.
    When: The external-owner advisory helper executes.
    Then: Its namespace is distinct from wallet/startup locks and no local alias partitions the key.
    """
    boundary = MagicMock(spec=AsyncSession)
    boundary.get_bind.return_value.dialect.name = "postgresql"
    boundary.execute = AsyncMock()
    key = FinancialAccountOwnerKey(environment="production", account_uid=_id(80))
    await _owner_barrier(cast(AsyncSession, boundary), key)

    statement, operands = boundary.execute.await_args.args
    assert "pg_advisory_xact_lock" in str(statement)
    assert operands == {
        "namespace": FINANCIAL_OWNER_LOCK_NAMESPACE,
        "owner_key": financial_account_owner_key_digest(key),
    }
    assert FINANCIAL_OWNER_LOCK_NAMESPACE not in {"execution_fence", "snapper_startup_integrity"}
