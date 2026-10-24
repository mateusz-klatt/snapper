"""Real SQLite and signed local HTTP native authority source graph tests.

Every valid source uses the actual closed key/accounts producer; no
generic legacy balance observation is authored as native account evidence.
"""

import asyncio
import traceback
from collections.abc import AsyncIterator
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import route_selection_digest
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_account_authority import native_account_authority_digest
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import canonical_native_account_identity
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import native_account_read_outcome_digest
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import ContractAccountingEvent
from snapper.data.models import Execution
from snapper.data.models import ExecutionRouteAuthority
from snapper.data.models import NativeAccountAuthority
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import Wallet
from snapper.data.native_account_authority_store import NativeAccountAuthorityBindingError
from snapper.data.native_account_authority_store import NativeAuthorityFreshnessPolicy
from snapper.data.native_account_authority_store import get_bound_native_account_authority
from snapper.data.native_account_authority_store import require_current_native_account_authority
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_read_store import begin_native_account_read
from tests.helpers.native_account_authority import (
    NATIVE_AUTHORITY_PRIVATE_MARKER as _PRIVATE_MARKER,
)
from tests.helpers.native_account_authority import NATIVE_AUTHORITY_TEST_TIME as _NOW
from tests.helpers.native_account_authority import NativeAuthorityScenario as _Scenario
from tests.helpers.native_account_authority import bound_native_authority_sources as _source
from tests.helpers.native_account_authority import build_native_authority_payload as _payload
from tests.helpers.native_account_authority import capture_native_account_read as _read
from tests.helpers.native_account_authority import current_native_authority as _current
from tests.helpers.native_account_authority import insert_native_authority as _insert
from tests.helpers.native_account_authority import native_authority_identity as _identity
from tests.helpers.native_account_authority import native_authority_reference as _reference
from tests.helpers.native_account_authority import native_authority_scenario as _native_scenario
from tests.helpers.native_account_authority import native_authority_uuid as _id
from tests.helpers.native_account_authority import rotate_native_credential as _rotate


@pytest.fixture
async def scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Scenario]:
    """Use the shared actual closed-HTTP source fixture with its own isolated SQLite lifetime."""
    async with _native_scenario(tmp_path, monkeypatch) as value:
        yield value


_TABLE = "native_account_authorities"


@pytest.mark.parametrize(
    ("maximum_age", "maximum_skew"),
    (
        (timedelta(0), timedelta(seconds=1)),
        (timedelta(seconds=-1), timedelta(seconds=1)),
        (timedelta(seconds=60), timedelta(seconds=-1)),
    ),
)
def test_invalid_trusted_freshness_configuration_is_refused(
    maximum_age: timedelta, maximum_skew: timedelta
) -> None:
    """Trusted policy configuration cannot silently disable source age or accept negative skew.

    Given: A nonpositive source-age limit or negative server-clock tolerance,
    When: The native authority policy is constructed,
    Then: Configuration fails before any source or authority can be interpreted.
    """
    with pytest.raises(ValueError, match="bounds must be positive and nonnegative"):
        NativeAuthorityFreshnessPolicy(maximum_age, maximum_skew)


async def _restore_authority(
    scenario: _Scenario, public_id: str, values: dict[str, object]
) -> bool:
    """Try a damaged local restore using typed SQL columns and no SQLite datetime adapter."""
    async with scenario.repository.session() as session:
        await session.execute(text(f"DROP TRIGGER IF EXISTS {_TABLE}_reject_update"))
        try:
            await session.execute(
                update(NativeAccountAuthority)
                .where(NativeAccountAuthority.public_id == public_id)
                .values(**values)
            )
            await session.commit()
        except IntegrityError:
            await session.rollback()
            return False
    return True


async def _legacy_slot(
    scenario: _Scenario, payload: NativeAccountAuthorityPayload, number: int
) -> None:
    """Occupy only a restored legacy UUID slot; its opaque payload is intentionally not authority."""
    async with scenario.repository.session() as session:
        session.add(
            ExecutionRouteAuthority(
                **_identity(number).model_dump(),
                timestamp=scenario.clock.current,
                wallet_public_id=payload.scope.wallet_public_id,
                exchange=payload.scope.exchange,
                mode=payload.scope.mode,
                environment=payload.scope.environment,
                margin_account=payload.scope.margin_account,
                account_scope_digest=account_scope_digest(payload.scope),
                credential_public_id=payload.credential_public_id,
                credential_timestamp=payload.credential_timestamp,
                venue_account_id=payload.venue_account_id,
                account_observation_public_id=_id(99000),
                account_observation_timestamp=scenario.clock.current,
                observed_at=payload.observed_at,
                valid_until=payload.valid_until,
                route_revision=payload.route_revision,
                content_digest="f" * 64,
                canonical_payload="non-authorizing-restored-identity-slot",
            )
        )
        await session.commit()


@pytest.mark.parametrize("key", ("cash", "flex", "fi_xbtusd"))
async def test_closed_http_sources_create_exact_native_profiles_with_empty_financial_history(
    scenario: _Scenario, key: str
) -> None:
    """Source account identity does not depend on a fabricated legacy balance or pre-existing trade.

    Given: Actual signed local key/accounts responses containing exact native quantities,
    When: Their bound literal profile is inserted and checked under the wallet fence,
    Then: Its immutable source and owner are preserved without any financial event or hold release.
    """
    outcome = await _read(scenario)
    payload = await _payload(scenario, outcome, key)
    result = await _insert(scenario, payload)
    current = await _current(scenario, result, payload)
    owner, accounts = await _source(scenario, outcome)

    assert current.payload == payload and current.recorded_at == _NOW
    assert result.content_digest == native_account_authority_digest(payload)
    assert current.payload.owner_proof.public_id == owner.public_id
    assert scenario.adapter.proofs_at_accounts_send == [1]
    assert len(scenario.adapter.calls) == 2
    assert all(response.raw.closed for response in scenario.adapter.responses)
    flex = next(
        profile for profile in accounts.snapshot.profiles if profile.native_account_key == "flex"
    )
    assert next(value.quantity for value in flex.quantities if value.currency == "USD") == Decimal(
        "12.3400000000000000001"
    )
    async with scenario.repository.session() as session:
        for model in (
            ContractAccountingEvent,
            AccountingScopeHold,
            Execution,
            ExecutionRouteAuthority,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 1


async def test_exact_source_profile_retry_returns_original_identity_and_recording_clock(
    scenario: _Scenario,
) -> None:
    """Re-reading the same source cannot manufacture a new authority timestamp or namespace.

    Given: One current authority for one actual read and literal profile,
    When: Identical canonical evidence is retried with a new proposed insertion UUID,
    Then: The original row and recording clock return after current source checks.
    """
    payload = await _payload(scenario, await _read(scenario))
    first = await _insert(scenario, payload)
    scenario.clock.current += timedelta(seconds=5)
    repeated = await _insert(scenario, payload, 501)

    assert repeated.public_id == first.public_id and not repeated.created
    current = await _current(scenario, first, payload)
    assert current.recorded_at == _NOW and current.payload.valid_until == payload.valid_until
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 1


async def test_different_valid_latest_source_cannot_reuse_an_occupied_native_uuid(
    scenario: _Scenario,
) -> None:
    """A new successful read never changes the material owned by an existing authority identity.

    Given: One committed authority and a distinct actual latest successful HTTP read,
    When: The new valid material requests the original native authority UUID,
    Then: The identity conflict is refused and the original immutable history remains intact.
    """
    original = await _payload(scenario, await _read(scenario))
    result = await _insert(scenario, original)
    scenario.clock.current += timedelta(seconds=1)
    newer = await _payload(scenario, await _read(scenario))

    with pytest.raises(NativeAccountAuthorityBindingError, match="public_identity_changed"):
        await _insert(scenario, newer)
    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_authority(session, result.public_id)
        ).payload == original
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 1


async def test_retry_cannot_hide_a_damaged_stored_source_profile_index(
    scenario: _Scenario,
) -> None:
    """Canonical duplicate lookup cannot treat a corrupted projection as a reusable original row.

    Given: A committed authority whose literal wallet-name projection was damaged by local restore,
    When: Its unchanged actual source retries under a fresh free UUID,
    Then: Historical rebinding refuses the corrupted original and no replacement authority appears.
    """
    payload = await _payload(scenario, await _read(scenario))
    result = await _insert(scenario, payload)
    assert await _restore_authority(
        scenario, result.public_id, {"margin_account": "different-observed-profile"}
    )

    with pytest.raises(NativeAccountAuthorityBindingError, match="projection_mismatch"):
        await _insert(scenario, payload, 501)
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 1


async def test_one_actual_read_can_name_distinct_profiles_without_merging_native_inventory(
    scenario: _Scenario,
) -> None:
    """Source/profile uniqueness is not a one-authority-per-request restriction or a cash aggregation.

    Given: One original response containing cash, multi-collateral and single-collateral profiles,
    When: Each literal profile is independently bound from that same actual source,
    Then: Three distinct scope records share source ownership without combining currency balances.
    """
    outcome = await _read(scenario)
    results = []
    for number, key in enumerate(("cash", "flex", "fi_xbtusd"), start=500):
        payload = await _payload(scenario, outcome, key)
        results.append(await _insert(scenario, payload, number))

    assert len({result.public_id for result in results}) == 3
    assert len(scenario.adapter.calls) == 2
    async with scenario.repository.session() as session:
        names = (
            (await session.execute(select(NativeAccountAuthority.margin_account))).scalars().all()
        )
        assert set(names) == {"cash", "flex", "fi_xbtusd"}
        assert await session.scalar(select(func.count()).select_from(ContractAccountingEvent)) == 0


@pytest.mark.parametrize("alias", ("FLEX", "flex_futures", "f-xbt:usd"))
async def test_missing_literal_profile_is_not_replaced_by_an_alias_or_default(
    scenario: _Scenario, alias: str
) -> None:
    """The bound source controls account names even when an internally coherent alias claim is supplied.

    Given: A real native response and a differently spelled requested wallet key,
    When: Scope and route are consistently changed to that unobserved alias,
    Then: Raw replay refuses the absent literal profile instead of choosing a default wallet.
    """
    payload = await _payload(scenario, await _read(scenario))
    scope = ExecutionAccountScope.model_validate(
        payload.scope.model_dump() | {"margin_account": alias}
    )
    forged = NativeAccountAuthorityPayload.model_validate(
        payload.model_dump()
        | {
            "scope": scope,
            "native_profile": payload.native_profile.model_dump() | {"native_account_key": alias},
            "route_revision": route_selection_digest(
                scope,
                payload.credential_public_id,
                payload.credential_timestamp,
                payload.public_api_origin,
            ),
        }
    )

    with pytest.raises(
        NativeAccountAuthorityBindingError, match="literal_profile_missing_or_ambiguous"
    ):
        await _insert(scenario, forged)


@pytest.mark.parametrize("changed", ("digest", "type"))
async def test_complete_profile_digest_and_type_are_recomputed_from_original_response(
    scenario: _Scenario, changed: str
) -> None:
    """Caller-supplied native metadata cannot stand in for the complete actual parsed profile.

    Given: Exact original quantities and profile type from the signed accounts response,
    When: A supplied digest or type contradicts that actual profile,
    Then: Independent raw replay refuses the mismatch before storing authority.
    """
    payload = await _payload(scenario, await _read(scenario))
    values = (
        {"content_digest": "f" * 64} if changed == "digest" else {"account_type": "cashAccount"}
    )
    forged = NativeAccountAuthorityPayload.model_validate(
        payload.model_dump() | {"native_profile": payload.native_profile.model_dump() | values}
    )

    with pytest.raises(NativeAccountAuthorityBindingError, match="literal_profile_changed"):
        await _insert(scenario, forged)


@pytest.mark.parametrize(
    "changed", ("owner_attempt", "outcome", "raw_source", "server", "missing_owner")
)
async def test_actual_read_graph_cannot_borrow_another_attempts_immutable_evidence(
    scenario: _Scenario, changed: str
) -> None:
    """Exact same-client metadata and matching source quantities do not authorize crossed read references.

    Given: Two genuine closed key/accounts reads in the same account with identical native balances,
    When: A later payload borrows an earlier owner, outcome, raw response or server clock,
    Then: The actual independently bound source graph refuses that substituted evidence.
    """
    first = await _payload(scenario, await _read(scenario))
    scenario.clock.current += timedelta(seconds=1)
    second = await _payload(scenario, await _read(scenario))
    values = second.model_dump()
    if changed == "owner_attempt":
        values["owner_proof"] = first.owner_proof.model_dump() | {
            "read_attempt_public_id": second.native_read.attempt_public_id
        }
    elif changed == "missing_owner":
        values["owner_proof"] = second.owner_proof.model_dump() | {"public_id": _id(99001)}
    else:
        source = second.native_read.model_dump()
        if changed == "outcome":
            source.update(
                outcome_public_id=first.native_read.outcome_public_id,
                outcome_content_digest=first.native_read.outcome_content_digest,
            )
        elif changed == "raw_source":
            source.update(
                raw_receipt_public_id=first.native_read.raw_receipt_public_id,
                raw_content_digest=first.native_read.raw_content_digest,
            )
        else:
            source.update(
                server_time=first.native_read.server_time,
                server_time_text=first.native_read.server_time_text,
            )
        values["native_read"] = source
    forged = NativeAccountAuthorityPayload.model_validate(values)

    with pytest.raises(NativeAccountAuthorityBindingError):
        await _insert(scenario, forged)


async def test_same_source_cannot_extend_first_expiry_even_under_a_larger_current_age_policy(
    scenario: _Scenario,
) -> None:
    """Renewal requires a new actual network attempt rather than a wider claim about old bytes.

    Given: A source/profile authority first issued with a ten-second trusted policy,
    When: A later sixty-second policy tries to recreate its expiry from the same source,
    Then: The changed material is refused and the expired original remains historical only.
    """
    outcome = await _read(scenario)
    short_policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=10), timedelta(seconds=1))
    original = await _payload(scenario, outcome, policy=short_policy)
    result = await _insert(scenario, original, policy=short_policy)
    scenario.clock.current += timedelta(seconds=20)
    renewed = await _payload(scenario, outcome)

    with pytest.raises(NativeAccountAuthorityBindingError, match="source_profile_material_changed"):
        await _insert(scenario, renewed, 501)
    with pytest.raises(NativeAccountAuthorityBindingError, match="not_current"):
        await _insert(scenario, original, 502)
    async with scenario.repository.session() as session:
        history = await get_bound_native_account_authority(session, result.public_id)
    assert history.payload == original and history.payload.valid_until == _NOW + timedelta(
        seconds=10
    )


@pytest.mark.parametrize("elapsed", (timedelta(seconds=1), timedelta(seconds=61)))
async def test_actual_sqlite_fence_wait_uses_the_post_wait_recording_and_freshness_clock(
    scenario: _Scenario, monkeypatch: pytest.MonkeyPatch, elapsed: timedelta
) -> None:
    """A real reserved writer transaction cannot preserve a stale pre-fence eligibility clock.

    Given: Fresh actual HTTP evidence and another SQLite transaction holding the writer fence,
    When: The authority writer waits while the controlled local clock advances,
    Then: Its recorded clock is sampled after release and an expired source creates no row.
    """
    payload = await _payload(scenario, await _read(scenario))
    entered = asyncio.Event()
    begin = scenario.repository._begin_execution_insert_transaction

    async def observed_begin(session: AsyncSession) -> None:
        """Expose entry into the real blocking transaction without replacing its lock semantics."""
        entered.set()
        await begin(session)

    async with scenario.repository.session() as blocker:
        await begin(blocker)
        await scenario.repository._acquire_execution_fence_lock(
            blocker, scenario.selection.wallet_public_id
        )
        monkeypatch.setattr(
            scenario.repository, "_begin_execution_insert_transaction", observed_begin
        )
        task = asyncio.create_task(_insert(scenario, payload))
        try:
            await entered.wait()
            assert not task.done()
            scenario.clock.current += elapsed
            await blocker.rollback()
            if elapsed >= scenario.policy.maximum_source_age:
                with pytest.raises(NativeAccountAuthorityBindingError, match="not_current"):
                    await task
            else:
                result = await task
                assert (await _current(scenario, result, payload)).recorded_at == _NOW + elapsed
        finally:
            await blocker.rollback()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    async with scenario.repository.session() as session:
        count = await session.scalar(select(func.count()).select_from(NativeAccountAuthority))
    assert count == (0 if elapsed >= scenario.policy.maximum_source_age else 1)


@pytest.mark.parametrize("failed", (False, True))
async def test_newer_pending_or_failed_read_blocks_current_source_without_erasing_history(
    scenario: _Scenario, failed: bool
) -> None:
    """The newest actual request owns eligibility even when no usable replacement source arrives.

    Given: A valid authority followed by another request in the same actual family,
    When: That request remains pending or its real accounts HTTP call fails,
    Then: Current lookup and exact retry refuse the older source while typed replay survives.
    """
    payload = await _payload(scenario, await _read(scenario))
    original = await _insert(scenario, payload)
    scenario.clock.current += timedelta(seconds=1)
    if failed:
        scenario.adapter.fail_accounts = True
        newer = await _read(scenario)
        assert newer.payload.result.kind == "failed"
    else:
        await begin_native_account_read(
            scenario.repository,
            scenario.selection,
            ExecutionEvidenceIdentity(public_id=_id(600), session_id=_id(999), sequence_id=600),
        )

    with pytest.raises(
        NativeAccountAuthorityBindingError, match="newest_attempt_changed_or_pending"
    ):
        await _current(scenario, original, payload)
    with pytest.raises(
        NativeAccountAuthorityBindingError, match="newest_attempt_changed_or_pending"
    ):
        await _insert(scenario, payload, 501)
    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_authority(session, original.public_id)
        ).payload == payload


async def test_genuine_pending_accounts_http_cannot_borrow_the_previous_completed_source(
    scenario: _Scenario,
) -> None:
    """A real committed key preflight does not invent an outcome while its state HTTP is pending.

    Given: One completed source and a second actual producer paused inside its accounts HTTP,
    When: A candidate names the second attempt but tries to reuse the earlier source references,
    Then: Pending outcome binding refuses it before old raw state can be promoted.
    """
    original = await _payload(scenario, await _read(scenario))
    scenario.adapter.block_accounts = True
    task = asyncio.create_task(_read(scenario))
    try:
        assert await asyncio.to_thread(scenario.adapter.accounts_entered.wait, 4)
        async with scenario.repository.session() as session:
            proof_rows = (
                (
                    await session.execute(
                        select(NativeAccountIdentityProof).order_by(
                            NativeAccountIdentityProof.family_sequence.desc()
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(proof_rows) == 2
            pending = await find_native_account_identity(session, proof_rows[0].attempt_public_id)
        assert pending is not None
        owner = original.owner_proof.model_dump() | {
            "public_id": pending.public_id,
            "content_digest": native_account_identity_digest(pending.payload),
            "read_attempt_public_id": pending.payload.attempt_public_id,
        }
        source = original.native_read.model_dump() | {
            "attempt_public_id": pending.payload.attempt_public_id,
            "attempt_content_digest": pending.payload.attempt_content_digest,
            "family_sequence": pending.payload.attempt.family_sequence,
        }
        candidate = NativeAccountAuthorityPayload.model_validate(
            original.model_dump() | {"owner_proof": owner, "native_read": source}
        )
        with pytest.raises(NativeAccountAuthorityBindingError, match="accounts_read_pending"):
            await _insert(scenario, candidate)
    finally:
        scenario.adapter.accounts_release.set()
        await task


async def test_actual_failed_accounts_read_cannot_borrow_an_older_successful_raw_source(
    scenario: _Scenario,
) -> None:
    """A committed safe key proof does not turn a failed accounts request into successful state.

    Given: One actual successful source followed by a real accounts transport failure,
    When: A candidate names the failed attempt and proof but borrows the earlier successful raw,
    Then: The actual failed outcome is refused while original raw evidence remains retained.
    """
    original = await _payload(scenario, await _read(scenario))
    scenario.adapter.fail_accounts = True
    failed = await _read(scenario)
    assert failed.payload.result.kind == "failed"
    async with scenario.repository.session() as session:
        proof = await find_native_account_identity(session, failed.payload.attempt_public_id)
    assert proof is not None and proof.payload.matches_owner
    owner = original.owner_proof.model_dump() | {
        "public_id": proof.public_id,
        "content_digest": native_account_identity_digest(proof.payload),
        "read_attempt_public_id": proof.payload.attempt_public_id,
    }
    source = original.native_read.model_dump() | {
        "attempt_public_id": failed.payload.attempt_public_id,
        "attempt_content_digest": failed.payload.attempt_content_digest,
        "family_sequence": failed.payload.attempt.family_sequence,
        "outcome_public_id": failed.public_id,
        "outcome_content_digest": native_account_read_outcome_digest(failed.payload),
    }
    candidate = NativeAccountAuthorityPayload.model_validate(
        original.model_dump() | {"owner_proof": owner, "native_read": source}
    )

    with pytest.raises(NativeAccountAuthorityBindingError, match="accounts_read_unsuccessful"):
        await _insert(scenario, candidate)
    assert scenario.adapter.proofs_at_accounts_send == [1, 2]
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0
        assert await session.scalar(select(func.count()).select_from(AccountingRawReceipt)) == 1


async def test_missing_authority_record_is_not_reconstructed_from_valid_but_unstored_material(
    scenario: _Scenario,
) -> None:
    """A valid source graph is distinct from an independently committed authority row.

    Given: Actual original HTTP evidence whose native authority has never been inserted,
    When: A caller requests an unrelated authority UUID,
    Then: Historical lookup explicitly refuses the missing row rather than constructing one.
    """
    await _read(scenario)
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountAuthorityBindingError, match="authority_missing"):
            await get_bound_native_account_authority(session, _id(99002))


async def test_current_reference_must_name_the_actual_native_digest_scope_and_route(
    scenario: _Scenario,
) -> None:
    """A well-shaped public reference cannot replace the independently verified authority digest.

    Given: One actual committed current native record,
    When: Its runtime reference supplies a different valid-length digest,
    Then: Current lookup refuses the selected-reference contradiction under the real wallet fence.
    """
    payload = await _payload(scenario, await _read(scenario))
    result = await _insert(scenario, payload)
    reference = ExecutionRouteReference.model_validate(
        _reference(result, payload).model_dump() | {"authority_content_digest": "f" * 64}
    )
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, payload.scope.wallet_public_id
        )
        with pytest.raises(
            NativeAccountAuthorityBindingError, match="actual_route_reference_mismatch"
        ):
            await require_current_native_account_authority(
                session, reference, scenario.policy, scenario.clock.current
            )


@pytest.mark.parametrize("as_of", (_NOW + timedelta(seconds=60), _NOW.replace(tzinfo=None)))
async def test_expired_or_naive_interpretation_clock_is_not_current_authority(
    scenario: _Scenario, as_of: datetime
) -> None:
    """Current identity requires an aware clock strictly inside the original validity window.

    Given: One retained authority based on an original exact source clock,
    When: Interpretation reaches expiry or omits a timezone,
    Then: Current lookup refuses without modifying historical evidence.
    """
    payload = await _payload(scenario, await _read(scenario))
    result = await _insert(scenario, payload)

    with pytest.raises(NativeAccountAuthorityBindingError, match="not_current|clock_not_aware"):
        await _current(scenario, result, payload, as_of)


async def test_tighter_current_source_age_does_not_rewrite_an_older_validity_claim(
    scenario: _Scenario,
) -> None:
    """Policy changes apply dynamically without renewing or mutating original source age.

    Given: An authority issued under a sixty-second source-age limit,
    When: Current policy tightens to ten seconds after twenty seconds elapsed,
    Then: It is no longer current even though its preserved original expiry lies later.
    """
    payload = await _payload(scenario, await _read(scenario))
    result = await _insert(scenario, payload)
    scenario.clock.current += timedelta(seconds=20)
    scenario.policy = NativeAuthorityFreshnessPolicy(timedelta(seconds=10), timedelta(seconds=1))

    with pytest.raises(NativeAccountAuthorityBindingError, match="original_source_window"):
        await _current(scenario, result, payload)
    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_authority(session, result.public_id)
        ).payload == payload


@pytest.mark.parametrize("offset", (timedelta(seconds=-120), timedelta(seconds=120)))
async def test_original_server_clock_is_checked_by_current_policy_without_losing_raw_source(
    scenario: _Scenario, offset: timedelta
) -> None:
    """A captured HTTP state is not automatically current authority when provider/local clocks disagree.

    Given: A valid actual state response whose provider clock differs from capture time,
    When: A stricter authority policy checks that original source,
    Then: Authority is refused while the exact source and successful read remain retained.
    """
    scenario.adapter.server_offset = offset
    outcome = await _read(scenario)
    payload = await _payload(scenario, outcome)

    with pytest.raises(NativeAccountAuthorityBindingError, match="server_clock_outside_policy"):
        await _insert(scenario, payload)
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(AccountingRawReceipt)) == 1
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 0


async def test_later_issued_row_cannot_be_used_by_an_earlier_interpretation_or_backdated_retry(
    scenario: _Scenario,
) -> None:
    """Original source time is not a substitute for the later authority record's knowledge clock.

    Given: Source observed at time zero and an authority actually issued at time two,
    When: Interpretation or exact retry claims time one,
    Then: Both paths refuse the not-yet-known row while its original history remains unchanged.
    """
    payload = await _payload(scenario, await _read(scenario))
    scenario.clock.current += timedelta(seconds=2)
    result = await _insert(scenario, payload)
    scenario.clock.current -= timedelta(seconds=1)

    with pytest.raises(NativeAccountAuthorityBindingError, match="not_yet_known"):
        await _current(scenario, result, payload)
    with pytest.raises(NativeAccountAuthorityBindingError, match="knowledge_clock_regressed"):
        await _insert(scenario, payload, 501)
    async with scenario.repository.session() as session:
        history = await get_bound_native_account_authority(session, result.public_id)
    assert history.recorded_at == _NOW + timedelta(seconds=2)


@pytest.mark.parametrize("conflict", (False, True))
async def test_rotated_actual_credential_preserves_historical_owner_and_requires_matching_uid(
    scenario: _Scenario, conflict: bool
) -> None:
    """A new credential cannot create a second account namespace or reinterpret old account history.

    Given: A real retained authority and an actual new SDK/public credential revision,
    When: Its key preflight reports the same or a different external account UID,
    Then: Same-account source can create a new authority while conflict stops before state HTTP.
    """
    original_payload = await _payload(scenario, await _read(scenario))
    original = await _insert(scenario, original_payload)
    await _rotate(scenario)
    scenario.adapter.account_uid = _id(81) if conflict else _id(80)
    newer = await _read(scenario)

    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_authority(session, original.public_id)
        ).payload == original_payload
        observed = await find_native_account_identity(session, newer.payload.attempt_public_id)
    assert observed is not None and observed.payload.matches_owner is (not conflict)
    if conflict:
        assert newer.payload.result.kind == "failed"
        assert len(scenario.adapter.calls) == 3
    else:
        payload = await _payload(scenario, newer)
        new_authority = await _insert(scenario, payload, 501)
        assert (await _current(scenario, new_authority, payload)).payload == payload
        assert account_scope_digest(payload.scope) == account_scope_digest(original_payload.scope)
        assert payload.venue_account_id == original_payload.venue_account_id
    with pytest.raises(ValueError):
        await _current(scenario, original, original_payload)


async def test_owner_proof_recorded_after_accounts_request_cannot_explain_that_source(
    scenario: _Scenario,
) -> None:
    """An observed key clock alone cannot replace the required committed-before-state ordering.

    Given: Actual original key/accounts evidence and a damaged restore moving key recording later,
    When: All native references are rebuilt from that internally coherent proof,
    Then: Cross-source chronology refuses it because key recording exceeds the accounts request start.
    """
    outcome = await _read(scenario)
    owner, _ = await _source(scenario, outcome)
    revised = NativeAccountIdentityProofPayload.model_validate(
        owner.payload.model_dump() | {"recorded_at": _NOW + timedelta(seconds=1)}
    )
    async with scenario.repository.session() as session:
        await session.execute(text("DROP TRIGGER native_account_identity_proofs_reject_update"))
        await session.execute(
            update(NativeAccountIdentityProof)
            .where(NativeAccountIdentityProof.public_id == owner.public_id)
            .values(
                timestamp=revised.recorded_at,
                canonical_payload=canonical_native_account_identity(revised),
                content_digest=native_account_identity_digest(revised),
            )
        )
        await session.commit()
    scenario.clock.current += timedelta(seconds=2)
    payload = await _payload(scenario, outcome)

    with pytest.raises(NativeAccountAuthorityBindingError, match="source_knowledge_mismatch"):
        await _insert(scenario, payload)


@pytest.mark.parametrize("late", (False, True))
async def test_legacy_uuid_occupancy_refuses_current_but_preserves_explicit_native_history(
    scenario: _Scenario, late: bool
) -> None:
    """A family-typed replay and an ambiguous current public UUID have different knowledge contracts.

    Given: Actual native source and a deliberately non-authorizing occupied legacy UUID slot,
    When: That slot exists before insertion or appears after the native record,
    Then: New/current use is refused while an already selected native historical graph survives.
    """
    payload = await _payload(scenario, await _read(scenario))
    if not late:
        await _legacy_slot(scenario, payload, 500)
        with pytest.raises(NativeAccountAuthorityBindingError, match="occupied_by_legacy"):
            await _insert(scenario, payload)
        return
    result = await _insert(scenario, payload)
    scenario.clock.current += timedelta(seconds=1)
    await _legacy_slot(scenario, payload, 500)

    with pytest.raises(NativeAccountAuthorityBindingError, match="public_identity_ambiguous"):
        await _current(scenario, result, payload)
    with pytest.raises(NativeAccountAuthorityBindingError, match="public_identity_ambiguous"):
        await _insert(scenario, payload, 501)
    async with scenario.repository.session() as session:
        assert (
            await get_bound_native_account_authority(session, result.public_id)
        ).payload == payload


@pytest.mark.parametrize("current", (False, True))
async def test_bound_readers_do_not_commit_or_rollback_the_callers_transaction(
    scenario: _Scenario, current: bool
) -> None:
    """Read-only graph validation participates in the caller's existing atomic work.

    Given: A stored authority and an unrelated marker staged in the same caller transaction,
    When: Historical or current lookup succeeds and the caller explicitly rolls back,
    Then: The marker disappears while the independently committed authority remains.
    """
    payload = await _payload(scenario, await _read(scenario))
    result = await _insert(scenario, payload)
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, payload.scope.wallet_public_id
        )
        session.add(
            Wallet(
                public_id=_id(9000),
                session_id=_id(9),
                sequence_id=9000,
                timestamp=_NOW,
                label="native-authority-rollback-marker",
                is_paper=False,
            )
        )
        if current:
            bound = await require_current_native_account_authority(
                session,
                _reference(result, payload),
                scenario.policy,
                scenario.clock.current,
            )
        else:
            bound = await get_bound_native_account_authority(session, result.public_id)
        assert bound.payload == payload
        assert await session.scalar(select(Wallet).where(Wallet.public_id == _id(9000))) is not None
        await session.rollback()
    async with scenario.repository.session() as session:
        assert await session.scalar(select(Wallet).where(Wallet.public_id == _id(9000))) is None
        assert (
            await get_bound_native_account_authority(session, result.public_id)
        ).payload == payload


async def test_concurrent_same_source_profile_writers_reuse_one_original_record(
    scenario: _Scenario,
) -> None:
    """The wallet fence and source/profile uniqueness prevent parallel duplicate native authorities.

    Given: One actual current accounts source and identical profile evidence,
    When: Four independently identified insertions race through real SQLite transactions,
    Then: Exactly one row is created and every result names its original public identity.
    """
    payload = await _payload(scenario, await _read(scenario))
    results = await asyncio.gather(
        *(_insert(scenario, payload, number) for number in range(500, 504))
    )

    assert len({result.public_id for result in results}) == 1
    assert sum(result.created for result in results) == 1
    async with scenario.repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(NativeAccountAuthority)) == 1


async def test_external_bound_source_error_is_sanitized_without_revealing_stored_private_material(
    scenario: _Scenario,
) -> None:
    """A damaged source graph cannot publish sensitive canonical text through chained loader errors.

    Given: Genuine closed HTTP evidence followed by a deliberately damaged safe-proof restore,
    When: Native authority attempts to rebind that source,
    Then: Only a fixed authority error escapes, with private text and original exception chain suppressed.
    """
    outcome = await _read(scenario)
    payload = await _payload(scenario, outcome)
    async with scenario.repository.session() as session:
        await session.execute(text("DROP TRIGGER native_account_identity_proofs_reject_update"))
        await session.execute(
            update(NativeAccountIdentityProof)
            .where(NativeAccountIdentityProof.public_id == payload.owner_proof.public_id)
            .values(canonical_payload="{" + _PRIVATE_MARKER)
        )
        await session.commit()

    with pytest.raises(NativeAccountAuthorityBindingError, match="source_graph_unproven") as caught:
        await _insert(scenario, payload)
    assert caught.value.__cause__ is None
    assert _PRIVATE_MARKER not in "".join(traceback.format_exception(caught.value))


@pytest.mark.parametrize(
    "column",
    (
        "wallet_public_id",
        "exchange",
        "mode",
        "environment",
        "margin_account",
        "account_scope_digest",
        "credential_public_id",
        "credential_timestamp",
        "public_api_origin",
        "venue_account_id",
        "owner_proof_public_id",
        "read_attempt_public_id",
        "read_outcome_public_id",
        "raw_receipt_public_id",
        "observed_at",
        "valid_until",
        "route_revision",
        "content_digest",
        "canonical_payload",
        "known_to",
        "timestamp",
    ),
)
async def test_every_projected_copy_or_unsafe_clock_is_guarded_physically_or_by_bound_replay(
    scenario: _Scenario, column: str
) -> None:
    """Stored indexed copies cannot silently replace the complete independently bound native graph.

    Given: A valid native authority and another actual closed HTTP source for foreign-key operands,
    When: A local damaged restore changes one indexed/canonical copy or unsafe knowledge clock,
    Then: SQL constraints refuse it or the historical loader detects the contradiction.
    """
    first = await _read(scenario)
    payload = await _payload(scenario, first)
    scenario.clock.current += timedelta(seconds=2)
    result = await _insert(scenario, payload)
    scenario.clock.current += timedelta(seconds=1)
    second = await _read(scenario)
    second_owner, _ = await _source(scenario, second)
    second_source = second.payload.source_reference
    assert second_source is not None
    values: dict[str, object] = {
        "wallet_public_id": _id(11),
        "exchange": "kraken",
        "mode": "paper",
        "environment": "demo",
        "margin_account": "FLEX",
        "account_scope_digest": "f" * 64,
        "credential_public_id": _id(12),
        "credential_timestamp": _NOW - timedelta(hours=1),
        "public_api_origin": "https://different.invalid",
        "venue_account_id": _id(81),
        "owner_proof_public_id": second_owner.public_id,
        "read_attempt_public_id": second.payload.attempt_public_id,
        "read_outcome_public_id": second.public_id,
        "raw_receipt_public_id": second_source.raw_receipt_public_id,
        "observed_at": _NOW + timedelta(seconds=1),
        "valid_until": payload.valid_until + timedelta(seconds=1),
        "route_revision": "f" * 64,
        "content_digest": "f" * 64,
        "canonical_payload": "{" + _PRIVATE_MARKER,
        "known_to": _NOW + timedelta(seconds=1),
        "timestamp": _NOW - timedelta(seconds=1),
    }
    changed = await _restore_authority(scenario, result.public_id, {column: values[column]})
    async with scenario.repository.session() as session:
        if changed:
            with pytest.raises(NativeAccountAuthorityBindingError) as caught:
                await get_bound_native_account_authority(session, result.public_id)
            assert _PRIVATE_MARKER not in "".join(traceback.format_exception(caught.value))
        else:
            assert (
                await get_bound_native_account_authority(session, result.public_id)
            ).payload == payload
