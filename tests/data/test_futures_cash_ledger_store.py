"""Exercise durable native cash accounting against real raw receipts and historical routes."""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashAnchor
from snapper.application.portfolio.futures_cash_ledger import FuturesNativeCashEvent
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.data.contract_accounting_store import AccountingScopeUnavailable
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.contract_accounting_store import load_unresolved_accounting_holds
from snapper.data.contract_accounting_store import require_accounting_scope_available
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.futures_cash_ledger_store import NativeCashAccountingError
from snapper.data.futures_cash_ledger_store import NativeCashAccountingRequest
from snapper.data.futures_cash_ledger_store import append_bound_futures_cash
from snapper.data.futures_cash_ledger_store import append_futures_cash_from_receipt
from snapper.data.futures_cash_ledger_store import load_observed_futures_cash
from snapper.data.models import Base
from snapper.data.models import ContractAccountingAnchor
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import WalletCredential
from snapper.data.repository import SQLAlchemyRepository
from tests.helpers.unit_admission import AdmissionScenario
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import refresh_admission_account
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 8, tzinfo=UTC)


@pytest.fixture
async def scenario(tmp_path: Path) -> AsyncIterator[AdmissionScenario]:
    """Provide real isolated source, wallet, credential and immutable account-route evidence."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'cash.db'}")
    try:
        await repository.create_all()
        yield await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    finally:
        await repository.engine.dispose()


def _source_entry(margin_account: str) -> dict[str, object]:
    """Keep a native currency transition independent of position and valuation quantities."""
    return {
        "id": 17,
        "date": "2026-10-23T07:01:02Z",
        "asset": "bch",
        "collateral": "bch",
        "margin_account": margin_account,
        "booking_uid": "Booking-SameGroup",
        "info": "futures trade",
        "old_balance": "10",
        "new_balance": "10.75",
        "realized_pnl": "1",
        "fee": "0.2",
        "realized_funding": "-0.05",
        "execution": "Execution-CaseSensitive",
    }


async def _capture(
    scenario: AdmissionScenario,
    number: int,
    entry_updates: dict[str, object] | None = None,
    raw_updates: dict[str, object] | None = None,
    route: ExecutionRouteReference | None = None,
) -> NativeCashAccountingRequest:
    """Commit original bytes and their mandatory hold before a financial writer is called."""
    reference = route or scenario.route
    async with scenario.repository.session() as session:
        authority = await get_bound_route_authority(session, reference.authority_public_id)
    entry = {**_source_entry(authority.scope.margin_account), **(entry_updates or {})}
    wire = json.dumps({"feed": "account_log", "new_entry": entry}).encode()
    material: dict[str, object] = {
        "scope": {**authority.scope.model_dump(), "margin_account": None},
        "route_revision": client_selection_digest(client_selection_from_authority(authority)),
        "credential_public_id": authority.credential_public_id,
        "credential_timestamp": authority.credential_timestamp,
        "kind": "account_cash",
        "channel": "account_log",
        "representation": "wire_bytes_base64",
        "material": base64.b64encode(wire).decode(),
        "target_json_pointer": "/new_entry",
        "observed_at": max(_NOW, authority.observed_at),
        "source_identity": entry["booking_uid"],
        "source_identity_kind": "booking_uid",
    }
    observation = RawAccountingObservation.model_validate({**material, **(raw_updates or {})})
    provenance = AccountingWriteProvenance.model_validate(
        admission_stamp(number, max(_NOW, observation.observed_at)).model_dump()
    )
    raw = await capture_accounting_observation(scenario.repository, observation, provenance)
    return NativeCashAccountingRequest(
        wallet_public_id=authority.scope.wallet_public_id,
        raw_receipt_public_id=raw.public_id,
        route_authority_public_id=reference.authority_public_id,
    )


async def _count(scenario: AdmissionScenario, model: type[Base]) -> int:
    """Read a committed count from an independent real database transaction."""
    async with scenario.repository.session() as session:
        return int((await session.scalar(select(func.count()).select_from(model))) or 0)


def _account_header(
    scenario: AdmissionScenario, account: str, changes: dict[str, object] | None = None
) -> dict[str, object]:
    """Retain a real account header in complete original bytes rather than callback context."""
    wire = json.dumps(
        {
            "feed": "account_log",
            "account": account,
            "new_entry": {
                **_source_entry(scenario.authority.scope.margin_account),
                **(changes or {}),
            },
        }
    ).encode()
    return {"material": base64.b64encode(wire).decode()}


async def test_native_cash_source_commits_anchor_and_one_delta_without_clearing_hold(
    scenario: AdmissionScenario,
) -> None:
    """Persist real native currency facts without inventing full account certification.

    Given: A held original cash log and a genuinely bound historical account route.
    When: Its native currency leg is appended under the actual wallet fence.
    Then: One source anchor and one exact cash delta commit while the source hold persists.
    """
    request = await _capture(scenario, 300)
    result = await append_futures_cash_from_receipt(
        scenario.repository, request, admission_stamp(400, _NOW)
    )
    async with scenario.repository.session() as session:
        anchor_row = (await session.execute(select(ContractAccountingAnchor))).scalar_one()
        event_row = (await session.execute(select(ContractAccountingEvent))).scalar_one()
        holdings = await load_observed_futures_cash(session, scenario.authority.scope)
        holds = await load_unresolved_accounting_holds(
            session,
            AccountingObservationScope.model_validate(scenario.authority.scope.model_dump()),
        )
    anchor = FuturesNativeCashAnchor.model_validate_json(anchor_row.canonical_payload)
    event = FuturesNativeCashEvent.model_validate_json(event_row.canonical_payload)
    assert result.created is True
    assert result.observed_balance == Decimal("10.75")
    assert anchor.balance == Decimal(10)
    assert anchor.source_execution_prefix_status == "unavailable"
    assert anchor.source_execution_scope_sequence is None
    assert event.cash_delta == Decimal("0.75")
    assert event.source_entry.fee == Decimal("0.2")
    assert event_row.instrument_public_id is None
    assert event_row.execution_public_id is None
    assert holdings[0].native_currency == "bch"
    assert holdings[0].balance == Decimal("10.75")
    assert result.anchor_public_id == anchor_row.public_id
    assert len(holds) == 1
    async with scenario.repository.session() as session:
        scope = AccountingObservationScope.model_validate(scenario.authority.scope.model_dump())
        with pytest.raises(AccountingScopeUnavailable):
            await require_accounting_scope_available(session, scope)


async def test_source_redelivery_reuses_one_posting_but_retains_each_new_receipt_hold(
    scenario: AdmissionScenario,
) -> None:
    """Deduplicate venue facts independently of local receipt observations.

    Given: Two actual observations carry the identical native source log.
    When: Both are interpreted through their own committed receipt references.
    Then: One cash posting survives and both unresolved source holds remain.
    """
    first = await _capture(scenario, 300)
    second = await _capture(scenario, 301)
    one = await append_futures_cash_from_receipt(
        scenario.repository, first, admission_stamp(400, _NOW)
    )
    two = await append_futures_cash_from_receipt(
        scenario.repository, second, admission_stamp(401, _NOW)
    )
    assert one.public_id == two.public_id
    assert two.created is False
    assert await _count(scenario, ContractAccountingEvent) == 1
    assert await _count(scenario, ContractAccountingAnchor) == 1
    async with scenario.repository.session() as session:
        scope = AccountingObservationScope.model_validate(scenario.authority.scope.model_dump())
        holds = await load_unresolved_accounting_holds(session, scope)
    assert len(holds) == 2


async def test_same_booking_group_does_not_collapse_distinct_cash_source_legs(
    scenario: AdmissionScenario,
) -> None:
    """Use exact log identity rather than treating a booking group as one financial event.

    Given: Two source log IDs share a booking UID and form a continuous currency chain.
    When: Both native balance legs are appended.
    Then: Both post exactly once against the same initial currency anchor.
    """
    first = await _capture(scenario, 300)
    second = await _capture(scenario, 301, {"id": 18, "old_balance": "10.75", "new_balance": "11"})
    one = await append_futures_cash_from_receipt(
        scenario.repository, first, admission_stamp(400, _NOW)
    )
    two = await append_futures_cash_from_receipt(
        scenario.repository, second, admission_stamp(401, _NOW)
    )
    assert two.anchor_public_id == one.anchor_public_id
    assert two.ledger_sequence == 2
    assert two.observed_balance == Decimal(11)
    assert await _count(scenario, ContractAccountingEvent) == 2
    assert await _count(scenario, ContractAccountingAnchor) == 1
    async with scenario.repository.session() as session:
        replayed = await load_observed_futures_cash(session, scenario.authority.scope)
    assert replayed[0].balance == Decimal(11)
    assert replayed[0].latest_event_public_id == two.public_id
    assert replayed[0].latest_ledger_sequence == 2


async def test_different_native_currencies_remain_separate_within_the_same_cash_account(
    scenario: AdmissionScenario,
) -> None:
    """Share account identity across instruments without adding incompatible currency units.

    Given: Independent bch and USD balance legs for one actual margin account.
    When: Their observed balances are replayed from the durable ledger.
    Then: Both native balances and distinct source anchors survive without an FX guess.
    """
    bch = await _capture(scenario, 300)
    usd = await _capture(
        scenario,
        301,
        {"id": 18, "asset": "USD", "collateral": "USD", "old_balance": "100", "new_balance": "90"},
    )
    await append_futures_cash_from_receipt(scenario.repository, bch, admission_stamp(400, _NOW))
    await append_futures_cash_from_receipt(scenario.repository, usd, admission_stamp(401, _NOW))
    async with scenario.repository.session() as session:
        observed = await load_observed_futures_cash(session, scenario.authority.scope)
    assert {item.native_currency: item.balance for item in observed} == {
        "bch": Decimal("10.75"),
        "USD": Decimal(90),
    }
    assert len({item.anchor_public_id for item in observed}) == 2


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        ({"id": 17, "new_balance": "12"}, "source_identity_conflict"),
        ({"id": 18, "old_balance": "11", "new_balance": "12"}, "chain_discontinuous"),
        ({"id": 16, "old_balance": "10.75", "new_balance": "12"}, "chain_discontinuous"),
    ),
)
async def test_conflicting_or_discontinuous_cash_evidence_never_replaces_the_retained_chain(
    scenario: AdmissionScenario, changes: dict[str, object], reason: str
) -> None:
    """Keep the first cash fact immutable and preserve later contradictory raw evidence.

    Given: One committed native balance and a contradictory duplicate or unaligned next event.
    When: Financial posting tries to adopt the later source.
    Then: It refuses without changing cash or clearing the new observation's hold.
    """
    first = await _capture(scenario, 300)
    await append_futures_cash_from_receipt(scenario.repository, first, admission_stamp(400, _NOW))
    changed = await _capture(scenario, 301, changes)
    with pytest.raises(NativeCashAccountingError, match=reason):
        await append_futures_cash_from_receipt(
            scenario.repository, changed, admission_stamp(401, _NOW)
        )
    assert await _count(scenario, ContractAccountingEvent) == 1
    async with scenario.repository.session() as session:
        balances = await load_observed_futures_cash(session, scenario.authority.scope)
    assert balances[0].balance == Decimal("10.75")


@pytest.mark.parametrize(
    ("raw_updates", "entry_updates", "reason"),
    (
        ({"route_revision": "not-the-real-client"}, {}, "actual_client_route_mismatch"),
        ({"credential_public_id": admission_id(777)}, {}, "actual_client_route_mismatch"),
        ({"route_authority_public_id": admission_id(778)}, {}, "raw_authority_mismatch"),
        ({"observed_at": _NOW - timedelta(hours=3)}, {}, "credential_revision_from_future"),
        (
            {
                "scope": {
                    "wallet_public_id": admission_id(3),
                    "exchange": "kraken_futures",
                    "mode": "live",
                    "environment": "production",
                    "margin_account": "another-account",
                }
            },
            {},
            "raw_margin_account_mismatch",
        ),
        ({"source_identity": "not-the-booking"}, {}, "source_identity_mismatch"),
        ({}, {"margin_account": "another-account"}, "source_margin_account_mismatch"),
    ),
)
async def test_raw_source_and_actual_selected_account_must_match_before_any_ledger_write(
    scenario: AdmissionScenario,
    raw_updates: dict[str, object],
    entry_updates: dict[str, object],
    reason: str,
) -> None:
    """Do not let a valid receipt or credential-shaped field substitute for actual route proof.

    Given: Retained raw evidence contradicts the selected client, source identity or margin account.
    When: Stage B tries to post its native cash leg.
    Then: No account anchor or financial event is committed.
    """
    request = await _capture(scenario, 300, entry_updates, raw_updates)
    with pytest.raises(NativeCashAccountingError, match=reason):
        await append_futures_cash_from_receipt(
            scenario.repository, request, admission_stamp(400, _NOW)
        )
    assert await _count(scenario, ContractAccountingAnchor) == 0
    assert await _count(scenario, ContractAccountingEvent) == 0


async def test_expired_historical_authority_can_record_an_actual_old_cash_fact(
    scenario: AdmissionScenario,
) -> None:
    """Keep source accounting possible without granting expired authority permission to spend.

    Given: A genuine route witness whose current freshness has expired.
    When: Its already captured historical cash event is appended later.
    Then: The actual fact commits without requiring a new current balance query.
    """
    request = await _capture(scenario, 300)
    result = await append_futures_cash_from_receipt(
        scenario.repository, request, admission_stamp(400, _NOW + timedelta(days=1))
    )
    assert result.observed_balance == Decimal("10.75")


async def test_missing_receipt_and_crossed_wallet_cannot_allocate_financial_rows(
    scenario: AdmissionScenario,
) -> None:
    """Bind the actual wallet fence and durable raw source before interpreting money.

    Given: A nonexistent raw UUID or a different claimed wallet fence.
    When: The standalone writer receives those references.
    Then: It refuses before allocating any account cash evidence.
    """
    request = await _capture(scenario, 300)
    for changes, reason in (
        ({"raw_receipt_public_id": admission_id(799)}, "raw_receipt_missing"),
        ({"wallet_public_id": admission_id(798)}, "actual_client_route_mismatch"),
    ):
        with pytest.raises(NativeCashAccountingError, match=reason):
            await append_futures_cash_from_receipt(
                scenario.repository, request.model_copy(update=changes), admission_stamp(400, _NOW)
            )
    assert await _count(scenario, ContractAccountingEvent) == 0


async def test_caller_rollback_discards_anchor_and_posting_but_retains_stage_a_evidence(
    scenario: AdmissionScenario,
) -> None:
    """Make financial facts join the caller's execution transaction without an inner commit.

    Given: Stage A committed an actual raw receipt and hold.
    When: The session-level cash writer succeeds but its caller rolls the transaction back.
    Then: Neither anchor nor posting survives, and the original receipt remains retryable.
    """
    request = await _capture(scenario, 300)
    repository = scenario.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, request.wallet_public_id)
        await append_bound_futures_cash(session, request, admission_stamp(400, _NOW))
        await session.rollback()
    assert await _count(scenario, ContractAccountingAnchor) == 0
    assert await _count(scenario, ContractAccountingEvent) == 0
    result = await append_futures_cash_from_receipt(repository, request, admission_stamp(400, _NOW))
    assert result.created is True


async def test_commit_failure_keeps_financial_facts_atomic_and_does_not_erase_source_hold(
    scenario: AdmissionScenario,
) -> None:
    """Do not publish a successful financial result before transaction completion.

    Given: A source-proven cash event can flush but its commit fails.
    When: The standalone writer attempts its atomic transaction.
    Then: No cash facts survive and the original source still requires reconciliation.
    """
    request = await _capture(scenario, 300)
    with (
        patch.object(
            AsyncSession, "commit", new=AsyncMock(side_effect=RuntimeError("commit failed"))
        ),
        pytest.raises(RuntimeError, match="commit failed"),
    ):
        await append_futures_cash_from_receipt(
            scenario.repository, request, admission_stamp(400, _NOW)
        )
    assert await _count(scenario, ContractAccountingAnchor) == 0
    assert await _count(scenario, ContractAccountingEvent) == 0


async def test_concurrent_same_source_receipts_allocate_only_one_native_cash_event(
    scenario: AdmissionScenario,
) -> None:
    """Serialize source deduplication through the actual database wallet execution fence.

    Given: Two committed observations of the same native source event.
    When: Separate sessions simultaneously try to post them.
    Then: One creates the posting and the other reuses its immutable coordinates.
    """
    one = await _capture(scenario, 300)
    two = await _capture(scenario, 301)
    results = await asyncio.gather(
        append_futures_cash_from_receipt(scenario.repository, one, admission_stamp(400, _NOW)),
        append_futures_cash_from_receipt(scenario.repository, two, admission_stamp(401, _NOW)),
    )
    assert sorted(item.created for item in results) == [False, True]
    assert len({item.public_id for item in results}) == 1
    assert await _count(scenario, ContractAccountingEvent) == 1


async def test_an_empty_observed_currency_set_never_becomes_a_zero_cash_account(
    scenario: AdmissionScenario,
) -> None:
    """Keep missing native inventory explicitly absent until genuine source facts exist.

    Given: An authenticated route without an interpreted native balance leg.
    When: Observed cash chains are loaded.
    Then: No guessed currency, zero seed or synthetic initial capital appears.
    """
    async with scenario.repository.session() as session:
        assert await load_observed_futures_cash(session, scenario.authority.scope) == ()


async def test_restarting_repository_replays_exact_native_cash_from_durable_source_evidence(
    scenario: AdmissionScenario,
) -> None:
    """Reconstruct money from immutable anchors and postings after all in-memory state is gone.

    Given: One committed cash source and a closed original processing session.
    When: A second repository reads the same real isolated database.
    Then: The exact native balance and event coordinates remain reproducible.
    """
    request = await _capture(scenario, 300)
    result = await append_futures_cash_from_receipt(
        scenario.repository, request, admission_stamp(400, _NOW)
    )
    other = SQLAlchemyRepository(str(scenario.repository.engine.url))
    try:
        async with other.session() as session:
            balances = await load_observed_futures_cash(session, scenario.authority.scope)
        assert balances[0].latest_event_public_id == result.public_id
        assert balances[0].balance == Decimal("10.75")
    finally:
        await other.engine.dispose()


@pytest.mark.parametrize("identity", ("unknown", "same", "different"))
@pytest.mark.parametrize("new_currency", (False, True))
async def test_credential_rotation_requires_proven_account_continuity_even_for_a_new_currency(
    scenario: AdmissionScenario, identity: str, new_currency: bool
) -> None:
    """Never silently fork spendable cash when a credential is rotated.

    Given: One account already has a durable observed currency balance.
    When: A different actual credential presents another native cash fact.
    Then: Only the same explicitly known external account can extend the ledger.
    """
    account_id = None if identity == "unknown" else "Account-Original"
    first_route = await refresh_admission_account(
        scenario, {"venue_account_id": account_id}, _NOW + timedelta(seconds=1), 600
    )
    first = await _capture(scenario, 300, route=first_route)
    await append_futures_cash_from_receipt(
        scenario.repository, first, admission_stamp(400, _NOW + timedelta(seconds=1))
    )
    credential_at = _NOW + timedelta(seconds=2)
    async with scenario.repository.session() as session:
        previous = (await session.execute(select(WalletCredential))).scalar_one()
        previous.known_to = credential_at
        session.add(
            WalletCredential(
                **admission_stamp(94, credential_at).model_dump(mode="python"),
                wallet_public_id=scenario.authority.scope.wallet_public_id,
                exchange="kraken_futures",
                credential_type="api_key_secret",
                encrypted_payload="isolated-new-fixture-value-never-decrypted",
            )
        )
        await session.commit()
    route = await refresh_admission_account(
        scenario,
        {
            "credential_public_id": admission_id(94),
            "credential_timestamp": credential_at,
            "venue_account_id": "Account-Other" if identity == "different" else account_id,
        },
        _NOW + timedelta(seconds=3),
        601,
    )
    updates: dict[str, object] = {"id": 18, "old_balance": "10.75", "new_balance": "11"}
    if new_currency:
        updates.update(asset="USD", collateral="USD", old_balance="100", new_balance="99")
    second = await _capture(scenario, 301, updates, route=route)
    if identity == "same":
        result = await append_futures_cash_from_receipt(
            scenario.repository, second, admission_stamp(401, _NOW + timedelta(seconds=3))
        )
        assert result.created is True
        assert await _count(scenario, ContractAccountingEvent) == 2
    else:
        reason = "external_account_changed" if identity == "different" else "continuity_unproven"
        with pytest.raises(NativeCashAccountingError, match=reason):
            await append_futures_cash_from_receipt(
                scenario.repository, second, admission_stamp(401, _NOW + timedelta(seconds=3))
            )
        assert await _count(scenario, ContractAccountingEvent) == 1


@pytest.mark.parametrize(
    ("table", "reason"),
    (
        ("accounting_raw_receipts", "raw_receipt_projection_mismatch"),
        ("contract_accounting_anchors", "anchor_projection_mismatch"),
        ("contract_accounting_events", "event_projection_mismatch"),
    ),
)
async def test_replay_refuses_a_corrupted_immutable_content_projection(
    scenario: AdmissionScenario, table: str, reason: str
) -> None:
    """Recheck retained source proofs even when a restored database bypassed its physical guard.

    Given: A valid committed cash chain in a disposable isolated fixture.
    When: Deliberate fixture corruption changes a stored content digest after removing its guard.
    Then: Financial replay refuses the inconsistent retained evidence.
    """
    request = await _capture(scenario, 300)
    await append_futures_cash_from_receipt(scenario.repository, request, admission_stamp(400, _NOW))
    async with scenario.repository.session() as session:
        await session.execute(text(f"DROP TRIGGER {table}_reject_update"))
        await session.execute(
            text(f"UPDATE {table} SET content_digest = :digest"), {"digest": "f" * 64}
        )
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(NativeCashAccountingError, match=reason):
            await load_observed_futures_cash(session, scenario.authority.scope)


async def test_replay_refuses_a_missing_currency_anchor_after_incomplete_restore(
    scenario: AdmissionScenario,
) -> None:
    """Do not recreate a missing opening balance from a later cash event.

    Given: A genuine cash posting whose source anchor is lost in a deliberately corrupted fixture.
    When: Durable accounting replay is requested.
    Then: It refuses rather than fabricating a zero or adopting the event as a replacement anchor.
    """
    request = await _capture(scenario, 300)
    await append_futures_cash_from_receipt(scenario.repository, request, admission_stamp(400, _NOW))
    async with scenario.repository.session() as session:
        await session.execute(text("DROP TRIGGER contract_accounting_anchors_reject_delete"))
        await session.execute(text("DELETE FROM contract_accounting_anchors"))
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(NativeCashAccountingError, match="anchor_missing"):
            await load_observed_futures_cash(session, scenario.authority.scope)


@pytest.mark.parametrize("header", ("Account-Original", "Account-Other"))
async def test_actual_wire_account_header_must_match_known_historical_authority(
    scenario: AdmissionScenario, header: str
) -> None:
    """Check source bytes against proven account identity before allocating native cash.

    Given: Historical authority explicitly names the actual external account.
    When: The original source frame presents the same or a different account header.
    Then: Only the exact same account can contribute a native cash fact.
    """
    route = await refresh_admission_account(
        scenario, {"venue_account_id": "Account-Original"}, _NOW + timedelta(seconds=1), 600
    )
    request = await _capture(
        scenario, 300, raw_updates=_account_header(scenario, header), route=route
    )
    if header == "Account-Original":
        result = await append_futures_cash_from_receipt(
            scenario.repository, request, admission_stamp(400, _NOW + timedelta(seconds=1))
        )
        assert result.created is True
    else:
        with pytest.raises(NativeCashAccountingError, match="source_account_header_mismatch"):
            await append_futures_cash_from_receipt(
                scenario.repository, request, admission_stamp(400, _NOW + timedelta(seconds=1))
            )
        assert await _count(scenario, ContractAccountingEvent) == 0


@pytest.mark.parametrize("new_currency", (False, True))
async def test_known_source_header_survives_an_intervening_unknown_header(
    scenario: AdmissionScenario, new_currency: bool
) -> None:
    """Keep strong account proof pinned without inventing missing provider identifiers.

    Given: A known original-frame account followed by a frame whose account header is absent.
    When: A later currency or event claims another account using the same credential.
    Then: The earlier known source identity still refuses the contradictory cash fact.
    """
    first = await _capture(scenario, 300, raw_updates=_account_header(scenario, "Account-Original"))
    await append_futures_cash_from_receipt(scenario.repository, first, admission_stamp(400, _NOW))
    middle = await _capture(scenario, 301, {"id": 18, "old_balance": "10.75", "new_balance": "11"})
    await append_futures_cash_from_receipt(scenario.repository, middle, admission_stamp(401, _NOW))
    changes: dict[str, object] = {"id": 19, "old_balance": "11", "new_balance": "12"}
    if new_currency:
        changes.update(asset="USD", collateral="USD", old_balance="100", new_balance="101")
    last = await _capture(
        scenario, 302, changes, _account_header(scenario, "Account-Other", changes)
    )
    with pytest.raises(NativeCashAccountingError, match="known_source_account_changed"):
        await append_futures_cash_from_receipt(
            scenario.repository, last, admission_stamp(402, _NOW)
        )
    assert await _count(scenario, ContractAccountingEvent) == 2
    async with scenario.repository.session() as session:
        observed = await load_observed_futures_cash(session, scenario.authority.scope)
    assert observed[0].balance == Decimal(11)


async def test_redelivery_without_a_header_does_not_double_post_or_drop_known_source_identity(
    scenario: AdmissionScenario,
) -> None:
    """Separate financial source identity from optional envelope header presence.

    Given: A source log was already posted from bytes containing a known account header.
    When: A genuine redelivery omits that optional header under the same actual client.
    Then: The original posting and its stronger source identity remain immutable.
    """
    first = await _capture(scenario, 300, raw_updates=_account_header(scenario, "Account-Original"))
    one = await append_futures_cash_from_receipt(
        scenario.repository, first, admission_stamp(400, _NOW)
    )
    second = await _capture(scenario, 301)
    two = await append_futures_cash_from_receipt(
        scenario.repository, second, admission_stamp(401, _NOW)
    )
    assert two.created is False
    assert one.public_id == two.public_id
    async with scenario.repository.session() as session:
        row = (await session.execute(select(ContractAccountingEvent))).scalar_one()
    payload = FuturesNativeCashEvent.model_validate_json(row.canonical_payload)
    assert payload.source_entry.venue_account_id == "Account-Original"


async def test_changed_observation_identity_remains_held_and_cannot_be_promoted_as_cash(
    scenario: AdmissionScenario,
) -> None:
    """Keep captured identity conflicts distinct from accepted provider financial facts.

    Given: One local observation UUID was already bound to different source material.
    When: Stage A retains its changed content as a separate conflict receipt.
    Then: The shared financial reader refuses promotion without deleting either raw source.
    """
    original = await _capture(scenario, 300)
    conflict = await _capture(scenario, 300, {"new_balance": "12"})
    assert conflict.raw_receipt_public_id != original.raw_receipt_public_id
    with pytest.raises(NativeCashAccountingError, match="raw_receipt_projection_mismatch"):
        await append_futures_cash_from_receipt(
            scenario.repository, conflict, admission_stamp(400, _NOW)
        )
    assert await _count(scenario, ContractAccountingEvent) == 0


async def test_invalid_retained_raw_envelope_refuses_with_the_shared_binding_error(
    scenario: AdmissionScenario,
) -> None:
    """Do not reinterpret an incompletely restored envelope as a different provider receipt.

    Given: A real raw source whose isolated fixture payload is deliberately corrupted.
    When: Native cash accounting uses the shared immutable observation reader.
    Then: The malformed retained envelope refuses before any financial row is allocated.
    """
    request = await _capture(scenario, 300)
    async with scenario.repository.session() as session:
        await session.execute(text("DROP TRIGGER accounting_raw_receipts_reject_update"))
        await session.execute(
            text("UPDATE accounting_raw_receipts SET canonical_payload = 'not-json'")
        )
        await session.commit()
    with pytest.raises(NativeCashAccountingError, match="raw_receipt_payload_invalid"):
        await append_futures_cash_from_receipt(
            scenario.repository, request, admission_stamp(400, _NOW)
        )
    assert await _count(scenario, ContractAccountingEvent) == 0
