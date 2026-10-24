"""Actual durable key identity roots, exact proof retries and historical source rebinding."""

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text

import snapper.data.native_account_identity_store as identity_store
import snapper.data.native_account_read_store as read_store
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.native_account_identity import NativeAccountIdentityProofPayload
from snapper.core.native_account_identity import NativeAccountIdentityRootReference
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.core.native_account_identity import canonical_native_account_identity
from snapper.core.native_account_identity import native_account_identity_digest
from snapper.core.native_account_read import NativeAccountReadAttemptPayload
from snapper.core.native_account_read import NativeAccountReadFailure
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import NativeAccountIdentityProof
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.native_account_identity_store import BoundNativeAccountIdentityProof
from snapper.data.native_account_identity_store import NativeAccountIdentityBindingError
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.data.native_account_identity_store import get_bound_native_account_identity
from snapper.data.native_account_identity_store import retain_native_account_identity
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.data.native_account_read_store import append_native_account_read_outcome
from snapper.data.native_account_read_store import begin_native_account_read
from snapper.data.native_account_read_store import get_bound_native_account_read_outcome
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 10, 23, 14, tzinfo=UTC)
_CREDENTIAL_AT = _NOW - timedelta(days=1)
_TABLE = "native_account_identity_proofs"


def _id(number: int) -> str:
    """Keep constructed wallet, credential, source and proof identities independently recognizable."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Supply stable producer identity without allowing a caller-authored knowledge timestamp."""
    return ExecutionEvidenceIdentity(public_id=_id(number), session_id=_id(999), sequence_id=number)


@dataclass
class _Clock:
    """Advance actual producer knowledge explicitly without sleeps or rewritten source clocks."""

    current: datetime = _NOW

    def now(self, zone: object) -> datetime:
        """Return the same controlled local clock to both real durable producers."""
        assert zone is UTC
        return self.current


@dataclass
class _Scenario:
    """One actual isolated database, selected public client and independently controlled clock."""

    repository: SQLAlchemyRepository
    selection: ExecutionClientSelection
    clock: _Clock


def _selection(wallet: int = 1, credential: int = 2) -> ExecutionClientSelection:
    """Keep synthetic public credential metadata separate from real venue account identifiers."""
    return ExecutionClientSelection(
        wallet_public_id=_id(wallet),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        credential_public_id=_id(credential),
        credential_timestamp=_CREDENTIAL_AT,
        public_api_origin="https://futures.kraken.com",
    )


async def _add_wallet(scenario: _Scenario, wallet: int, credential: int) -> None:
    """Persist real nonsecret fixture metadata through an independently committed session."""
    async with scenario.repository.session() as session:
        session.add_all(
            [
                Wallet(
                    **_identity(wallet).model_dump(),
                    timestamp=_CREDENTIAL_AT,
                    label=f"native-identity-fixture-{wallet}",
                    is_paper=False,
                ),
                WalletCredential(
                    **_identity(credential).model_dump(),
                    timestamp=_CREDENTIAL_AT,
                    wallet_public_id=_id(wallet),
                    exchange="kraken_futures",
                    credential_type="api_key_secret",
                    encrypted_payload="fixture-only-never-decrypted",
                ),
            ]
        )
        await session.commit()


@pytest.fixture
async def scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Scenario]:
    """Create actual wallet/credential rows and the real immutable proof schema in an isolated DB."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-identity.db'}")
    clock = _Clock()
    monkeypatch.setattr(identity_store, "datetime", clock)
    monkeypatch.setattr(read_store, "datetime", clock)
    value = _Scenario(repository, _selection(), clock)
    try:
        await repository.create_all()
        await _add_wallet(value, 1, 2)
        yield value
    finally:
        await repository.engine.dispose()


def _observation(scenario: _Scenario, **updates: object) -> NativeAccountPublicIdentity:
    """Construct explicit safe public source facts, never an original key response body."""
    return NativeAccountPublicIdentity.model_validate(
        {
            "client_selection": scenario.selection,
            "account_uid": _id(90),
            "general_access": "READ_ONLY",
            "transfer_access": "NO_ACCESS",
            "request_started_at": scenario.clock.current,
            "observed_at": scenario.clock.current,
            **updates,
        }
    )


async def _retain(
    scenario: _Scenario,
    attempt: int,
    proof: int = 300,
    observation: NativeAccountPublicIdentity | None = None,
) -> BoundNativeAccountIdentityProof:
    """Use the actual public transaction/fence-owning writer rather than insert fixture proof rows."""
    return await retain_native_account_identity(
        scenario.repository,
        _id(attempt),
        observation if observation is not None else _observation(scenario),
        _identity(proof),
    )


async def _finish(scenario: _Scenario, attempt: int) -> None:
    """Finalize an actual read under its real transaction and wallet fence."""
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, scenario.selection.wallet_public_id
        )
        await append_native_account_read_outcome(
            session,
            _id(attempt),
            NativeAccountReadFailure(reason="transport_error"),
            _identity(attempt + 500),
        )
        await session.commit()


async def _rotate_credential(scenario: _Scenario) -> None:
    """Add a real SCD2 public revision while preserving the original historical credential row."""
    revised_at = scenario.clock.current
    async with scenario.repository.session() as session:
        previous = (
            await session.execute(
                select(WalletCredential).where(WalletCredential.known_to == KNOWN_TO_MAX)
            )
        ).scalar_one()
        previous.known_to = revised_at
        session.add(
            WalletCredential(
                public_id=previous.public_id,
                session_id=_id(999),
                sequence_id=100,
                timestamp=revised_at,
                wallet_public_id=scenario.selection.wallet_public_id,
                exchange="kraken_futures",
                credential_type="api_key_secret",
                encrypted_payload="rotated-fixture-only-never-decrypted",
            )
        )
        await session.commit()
    scenario.selection = ExecutionClientSelection.model_validate(
        scenario.selection.model_dump() | {"credential_timestamp": revised_at}
    )


async def _restore_projection(scenario: _Scenario, proof: int, changes: dict[str, object]) -> None:
    """Simulate a damaged local restore after explicitly removing its ordinary update guard."""
    assignments = ", ".join(f"{column} = :value{index}" for index, column in enumerate(changes))
    parameters = {
        f"value{index}": (
            value.astimezone(UTC).replace(tzinfo=None).isoformat(sep=" ")
            if isinstance(value, datetime)
            else value
        )
        for index, value in enumerate(changes.values())
    }
    parameters["proof"] = _id(proof)
    async with scenario.repository.session() as session:
        await session.execute(text(f"DROP TRIGGER IF EXISTS {_TABLE}_reject_update"))
        await session.execute(
            text(f"UPDATE {_TABLE} SET {assignments} WHERE public_id = :proof"), parameters
        )
        await session.commit()


async def test_first_proof_commits_before_account_output_and_survives_reopening(
    scenario: _Scenario,
) -> None:
    """Actual committed owner knowledge remains available independently of an accounts HTTP result.

    Given: A real committed pending accounts attempt and an observed public key identity,
    When: Its first proof commits and the SQLite database is opened independently,
    Then: One original owner is visible with no fabricated account outcome or sensitive response.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    async with scenario.repository.session() as session:
        assert await find_native_account_identity(session, _id(200)) is None
    observation = _observation(scenario)
    scenario.clock.current += timedelta(seconds=2)
    first = await _retain(scenario, 200, observation=observation)
    reopened = SQLAlchemyRepository(str(scenario.repository.engine.url))
    try:
        async with reopened.session() as session:
            assert await get_bound_native_account_identity(session, first.public_id) == first
            assert await find_native_account_identity(session, _id(200)) == first
            assert await get_bound_native_account_read_outcome(session, _id(200)) is None
            assert (
                await session.scalar(select(func.count()).select_from(NativeAccountIdentityProof))
                == 1
            )
    finally:
        await reopened.engine.dispose()

    assert first.payload.owner_root is None and first.payload.matches_owner
    assert first.payload.recorded_at == _NOW + timedelta(seconds=2)
    assert first.payload.observation == observation
    assert "encrypted_payload" not in canonical_native_account_identity(first.payload)
    assert "apiKey" not in canonical_native_account_identity(first.payload)


async def test_exact_retry_after_failed_accounts_read_preserves_original_proof_and_clock(
    scenario: _Scenario,
) -> None:
    """Accounts failure cannot erase genuine identity knowledge or force a second owner root.

    Given: A retained key proof followed by an actual failed account-read outcome,
    When: The exact original proof identity and source are retried at a later clock,
    Then: The original proof and recording time survive and no duplicate row is written.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    observation = _observation(scenario)
    original = await _retain(scenario, 200, observation=observation)
    scenario.clock.current += timedelta(seconds=1)
    await _finish(scenario, 200)
    scenario.clock.current += timedelta(seconds=10)

    assert await _retain(scenario, 200, observation=observation) == original
    assert original.payload.recorded_at == _NOW
    async with scenario.repository.session() as session:
        assert (
            await session.scalar(select(func.count()).select_from(NativeAccountIdentityProof)) == 1
        )


@pytest.mark.parametrize("change", ("public_id", "account_uid", "permission", "source_clock"))
async def test_same_attempt_cannot_acquire_changed_proof_identity_or_public_material(
    scenario: _Scenario, change: str
) -> None:
    """One completed preflight cannot be rewritten by a materially different delivery retry.

    Given: One actual retained proof owned by a pending read attempt,
    When: Its retry changes public UUID, observed owner, permission or original clock,
    Then: The writer refuses the contradiction while preserving the first proof.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    observed = _observation(scenario)
    original = await _retain(scenario, 200, observation=observed)
    scenario.clock.current += timedelta(seconds=1)
    changed = observed.model_dump()
    if change == "account_uid":
        changed["account_uid"] = _id(91)
    elif change == "permission":
        changed["general_access"] = "FULL_ACCESS"
    elif change == "source_clock":
        changed["observed_at"] = scenario.clock.current
    proof_id = 301 if change == "public_id" else 300

    with pytest.raises(NativeAccountIdentityBindingError, match="attempt_proof_changed"):
        await _retain(scenario, 200, proof_id, NativeAccountPublicIdentity.model_validate(changed))
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_identity(session, original.public_id) == original


async def test_new_proof_is_refused_after_attempt_outcome_without_inventing_identity(
    scenario: _Scenario,
) -> None:
    """A terminal accounts read cannot acquire an independently authored late preflight.

    Given: A committed read attempt already finalized without a key proof,
    When: A new owner proof attempts to join that terminal request,
    Then: The store refuses it and retains no identity row.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await _finish(scenario, 200)

    with pytest.raises(NativeAccountIdentityBindingError, match="attempt_already_finished"):
        await _retain(scenario, 200)
    async with scenario.repository.session() as session:
        assert await find_native_account_identity(session, _id(200)) is None


async def test_missing_attempt_or_proof_is_not_replaced_by_an_older_owner(
    scenario: _Scenario,
) -> None:
    """Missing parent/source identities remain explicit despite other valid family history.

    Given: One real first owner and a separately missing attempt or proof UUID,
    When: Those missing references are used for retention or historical lookup,
    Then: The store refuses rather than substituting the existing owner proof.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await _retain(scenario, 200)

    with pytest.raises(NativeAccountReadBindingError, match="attempt_missing"):
        await _retain(scenario, 900, 901)
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match="proof_missing"):
            await get_bound_native_account_identity(session, _id(9999))


@pytest.mark.parametrize("change", ("credential", "environment"))
async def test_observation_from_another_selected_client_cannot_bind_the_attempt(
    scenario: _Scenario, change: str
) -> None:
    """The exact request selection remains authoritative over caller-supplied public metadata.

    Given: A committed production read through one selected credential revision,
    When: Its key proof instead names another credential or an actual demo namespace,
    Then: Material validation refuses the crossed selection before any row is retained.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    selected = scenario.selection.model_dump()
    if change == "credential":
        selected["credential_public_id"] = _id(22)
    else:
        selected.update(environment="demo", public_api_origin="https://demo-futures.kraken.com")
    observation = _observation(
        scenario, client_selection=ExecutionClientSelection.model_validate(selected)
    )

    with pytest.raises(ValueError, match="different selected client"):
        await _retain(scenario, 200, observation=observation)
    async with scenario.repository.session() as session:
        assert await find_native_account_identity(session, _id(200)) is None


@pytest.mark.parametrize("change", ("before_attempt", "future_observation"))
async def test_proof_source_clock_must_lie_within_actual_attempt_knowledge(
    scenario: _Scenario, change: str
) -> None:
    """An owner observation cannot predate its request or come from beyond its recording clock.

    Given: An actual committed request with a controlled local recording clock,
    When: Its observed request start is too early or its response lies in the future,
    Then: The producer refuses that source chronology without retaining proof.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    fields = (
        {"request_started_at": _NOW - timedelta(seconds=1)}
        if change == "before_attempt"
        else {"observed_at": _NOW + timedelta(seconds=1)}
    )
    observation = _observation(scenario, **fields)

    with pytest.raises(ValueError, match="actual attempt knowledge interval"):
        await _retain(scenario, 200, observation=observation)


@pytest.mark.parametrize("owner", (90, 91))
async def test_credential_rotation_preserves_first_external_owner_and_retains_conflict(
    scenario: _Scenario, owner: int
) -> None:
    """A new selected credential does not silently replace the wallet's actual original account.

    Given: One retained owner root and an actual new public credential revision,
    When: The next request observes the same or a different external account UID,
    Then: Both proofs remain historical and the changed owner stays explicitly conflicting.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    original = await _retain(scenario, 200)
    scenario.clock.current += timedelta(seconds=1)
    await _rotate_credential(scenario)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    later = await _retain(scenario, 201, 301, _observation(scenario, account_uid=_id(owner)))

    assert later.payload.owner_root == NativeAccountIdentityRootReference(
        public_id=original.public_id,
        content_digest=native_account_identity_digest(original.payload),
        account_uid=_id(90),
    )
    assert later.payload.matches_owner is (owner == 90)
    assert later.payload.observation.account_uid == _id(owner)
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_identity(session, original.public_id) == original
        assert await find_native_account_identity(session, _id(201)) == later
        assert (
            await session.scalar(select(func.count()).select_from(NativeAccountIdentityProof)) == 2
        )


@pytest.mark.parametrize("family", ("environment", "wallet"))
async def test_distinct_families_can_observe_same_uid_without_global_identity_uniqueness(
    scenario: _Scenario, family: str
) -> None:
    """Identity evidence can represent overlapping ownership without declaring independent cash budgets.

    Given: One retained root and another actual wallet or demo source family,
    When: Both families independently observe the same provider account UID,
    Then: Each retains its own first proof without a global UID uniqueness assumption.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    original = await _retain(scenario, 200)
    if family == "environment":
        scenario.selection = ExecutionClientSelection.model_validate(
            scenario.selection.model_dump()
            | {"environment": "demo", "public_api_origin": "https://demo-futures.kraken.com"}
        )
    else:
        await _add_wallet(scenario, 11, 12)
        scenario.selection = _selection(11, 12)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    separate = await _retain(scenario, 201, 301)

    assert separate.payload.owner_root is None
    assert separate.payload.observation.account_uid == original.payload.observation.account_uid
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_identity(session, original.public_id) == original
        assert await get_bound_native_account_identity(session, separate.public_id) == separate


async def test_first_committed_proof_wins_even_when_an_earlier_attempt_finishes_later(
    scenario: _Scenario,
) -> None:
    """Root knowledge order is independent of request ordinal and original observation arrival.

    Given: An earlier pending attempt and a later request whose proof commits first,
    When: The earlier observation is finally retained after that root,
    Then: Its proof references the already committed root despite its lower request ordinal.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    early = _observation(scenario)
    scenario.clock.current += timedelta(seconds=1)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    later = _observation(scenario)
    scenario.clock.current += timedelta(seconds=1)
    first_root = await _retain(scenario, 201, 301, later)
    scenario.clock.current += timedelta(seconds=1)
    child = await _retain(scenario, 200, 300, early)

    assert first_root.payload.attempt.family_sequence == 2
    assert child.payload.attempt.family_sequence == 1
    assert child.payload.owner_root is not None
    assert child.payload.owner_root.public_id == first_root.public_id
    assert (
        child.payload.attempt.started_at
        < first_root.payload.recorded_at
        <= child.payload.recorded_at
    )
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_identity(session, child.public_id) == child


async def test_concurrent_actual_proof_writers_keep_one_immutable_family_root(
    scenario: _Scenario,
) -> None:
    """The shared execution fence serializes first-owner creation across independent transactions.

    Given: Four independently committed pending requests in one wallet family,
    When: Their actual proof writes run concurrently through separate sessions,
    Then: Exactly one root commits and every other proof references that same original owner.
    """
    for number in range(200, 204):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(number))
    observed = _observation(scenario)
    proofs = await asyncio.gather(
        *(_retain(scenario, number, number + 100, observed) for number in range(200, 204))
    )
    roots = [proof for proof in proofs if proof.payload.owner_root is None]

    assert len(roots) == 1
    assert all(proof.payload.matches_owner for proof in proofs)
    assert {
        proof.payload.owner_root.public_id
        for proof in proofs
        if proof.payload.owner_root is not None
    } == {roots[0].public_id}
    async with scenario.repository.session() as session:
        assert (
            await session.scalar(select(func.count()).select_from(NativeAccountIdentityProof)) == 4
        )


async def test_public_proof_uuid_cannot_cross_between_actual_attempts(scenario: _Scenario) -> None:
    """One immutable proof UUID cannot be claimed by a second otherwise valid request.

    Given: Two committed requests and an existing proof owned by the first,
    When: The second request tries to reuse that public proof UUID,
    Then: The writer refuses the collision and leaves the second request without proof.
    """
    for number in (200, 201):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(number))
    await _retain(scenario, 200)

    with pytest.raises(NativeAccountIdentityBindingError, match="public_id_already_used"):
        await _retain(scenario, 201)
    async with scenario.repository.session() as session:
        assert await find_native_account_identity(session, _id(201)) is None


@pytest.mark.parametrize("retry", (False, True))
async def test_regressed_recording_clock_cannot_retain_new_or_retried_owner_proof(
    scenario: _Scenario, retry: bool
) -> None:
    """A later proof cannot be recorded before the already known immutable family owner.

    Given: Two pending requests and an owner proof recorded at the later controlled clock,
    When: A new proof or exact original retry is attempted after local clock regression,
    Then: The store refuses the impossible knowledge ordering without replacing the root.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    observed = _observation(scenario)
    scenario.clock.current += timedelta(seconds=1)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    second_observed = _observation(scenario)
    scenario.clock.current += timedelta(seconds=2)
    await _retain(scenario, 200, observation=observed)
    scenario.clock.current -= timedelta(seconds=1)

    with pytest.raises(NativeAccountIdentityBindingError, match="knowledge_clock_regressed"):
        await _retain(
            scenario,
            200 if retry else 201,
            300 if retry else 301,
            observed if retry else second_observed,
        )


@pytest.mark.parametrize(
    "column,value,reason",
    (
        ("canonical_payload", "{}", "payload_invalid"),
        ("content_digest", "f" * 64, "public_projection_mismatch"),
        ("scope_digest", "f" * 64, "scope_projection_mismatch"),
        ("family_sequence", 99, "scope_projection_mismatch"),
        ("client_selection_digest", "f" * 64, "scope_projection_mismatch"),
        ("general_access", "FULL_ACCESS", "public_projection_mismatch"),
        ("account_uid", _id(91), "public_projection_mismatch"),
    ),
)
async def test_restored_public_or_scope_copies_are_independently_checked(
    scenario: _Scenario, column: str, value: object, reason: str
) -> None:
    """Immutable application binding detects damaged restores independently of ordinary SQL guards.

    Given: A genuine retained first proof whose update trigger is removed only in this fixture,
    When: One canonical, digest, source or scope projection is corrupted,
    Then: Historical reads refuse the contradiction instead of trusting indexed copies.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await _retain(scenario, 200)
    await _restore_projection(scenario, 300, {column: value})

    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match=reason):
            await get_bound_native_account_identity(session, _id(300))


async def test_self_consistent_embedded_attempt_cannot_replace_actual_committed_parent(
    scenario: _Scenario,
) -> None:
    """Matching canonical and indexed digests cannot substitute a fabricated request history.

    Given: A proof whose actual stored read attempt has one family ordinal,
    When: A restore replaces all proof copies with another internally coherent ordinal,
    Then: The bound loader rejects it against the independently loaded original attempt.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    proof = await _retain(scenario, 200)
    attempt = NativeAccountReadAttemptPayload.model_validate(
        proof.payload.attempt.model_dump() | {"family_sequence": 99}
    )
    forged = NativeAccountIdentityProofPayload.model_validate(
        proof.payload.model_dump()
        | {
            "attempt": attempt,
            "attempt_content_digest": native_account_read_attempt_digest(attempt),
        }
    )
    await _restore_projection(
        scenario,
        300,
        {
            "family_sequence": 99,
            "attempt_content_digest": forged.attempt_content_digest,
            "canonical_payload": canonical_native_account_identity(forged),
            "content_digest": native_account_identity_digest(forged),
        },
    )

    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match="actual_attempt_mismatch"):
            await get_bound_native_account_identity(session, proof.public_id)


async def test_self_consistent_false_owner_reference_cannot_hide_a_retained_uid_conflict(
    scenario: _Scenario,
) -> None:
    """A matching owner property has no authority before its actual root is independently rebound.

    Given: A genuine root and a later proof honestly observing a different account UID,
    When: A restore rewrites the root-UID copy so the pure model falsely reports a match,
    Then: The historical loader refuses it against the actual unchanged first owner row.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await _retain(scenario, 200)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    conflict = await _retain(scenario, 201, 301, _observation(scenario, account_uid=_id(91)))
    assert conflict.payload.owner_root is not None and not conflict.payload.matches_owner
    forged = NativeAccountIdentityProofPayload.model_validate(
        conflict.payload.model_dump()
        | {"owner_root": conflict.payload.owner_root.model_dump() | {"account_uid": _id(91)}}
    )
    assert forged.matches_owner
    await _restore_projection(
        scenario,
        301,
        {
            "owner_root_account_uid": _id(91),
            "canonical_payload": canonical_native_account_identity(forged),
            "content_digest": native_account_identity_digest(forged),
        },
    )

    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match="actual_owner_root_mismatch"):
            await get_bound_native_account_identity(session, conflict.public_id)


async def test_cross_family_root_reference_cannot_replace_the_actual_same_wallet_root(
    scenario: _Scenario,
) -> None:
    """A valid referenced proof from another wallet is not this wallet's immutable original owner.

    Given: Two genuine independent wallet roots and one later child of the first wallet,
    When: A restore redirects that child to the other wallet's valid root and matching digest,
    Then: The loader rejects the cross-family substitution through its direct-family query.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await _retain(scenario, 200)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    child = await _retain(scenario, 201, 301)
    await _add_wallet(scenario, 11, 12)
    scenario.selection = _selection(11, 12)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(202))
    other = await _retain(scenario, 202, 302)
    reference = NativeAccountIdentityRootReference(
        public_id=other.public_id,
        content_digest=native_account_identity_digest(other.payload),
        account_uid=other.payload.observation.account_uid,
    )
    forged = NativeAccountIdentityProofPayload.model_validate(
        child.payload.model_dump() | {"owner_root": reference}
    )
    await _restore_projection(
        scenario,
        301,
        {
            "owner_root_public_id": reference.public_id,
            "owner_root_content_digest": reference.content_digest,
            "owner_root_account_uid": reference.account_uid,
            "canonical_payload": canonical_native_account_identity(forged),
            "content_digest": native_account_identity_digest(forged),
        },
    )

    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match="actual_owner_root_mismatch"):
            await get_bound_native_account_identity(session, child.public_id)


async def test_missing_initial_root_after_restore_is_not_replaced_by_a_child_or_other_family(
    scenario: _Scenario,
) -> None:
    """A damaged root graph cannot be repaired by silently selecting another retained owner.

    Given: A valid root and child plus another wallet's independently valid root,
    When: A restore turns the original root into a child of that other family,
    Then: Its original family's missing initial owner is explicitly refused.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    original = await _retain(scenario, 200)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    child = await _retain(scenario, 201, 301)
    await _add_wallet(scenario, 11, 12)
    scenario.selection = _selection(11, 12)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(202))
    other = await _retain(scenario, 202, 302)
    reference = NativeAccountIdentityRootReference(
        public_id=other.public_id,
        content_digest=native_account_identity_digest(other.payload),
        account_uid=other.payload.observation.account_uid,
    )
    forged = NativeAccountIdentityProofPayload.model_validate(
        original.payload.model_dump() | {"owner_root": reference}
    )
    await _restore_projection(
        scenario,
        300,
        {
            "owner_root_public_id": reference.public_id,
            "owner_root_content_digest": reference.content_digest,
            "owner_root_account_uid": reference.account_uid,
            "canonical_payload": canonical_native_account_identity(forged),
            "content_digest": native_account_identity_digest(forged),
        },
    )

    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match="owner_root_missing"):
            await get_bound_native_account_identity(session, child.public_id)


async def test_self_consistent_root_from_later_knowledge_cannot_explain_an_earlier_child(
    scenario: _Scenario,
) -> None:
    """All matching copied digests still require the actual root to precede its child proof.

    Given: An original owner and a later valid child proof,
    When: A restore moves root knowledge after that child and updates every copied digest,
    Then: Independent historical binding refuses the impossible root chronology.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    root = await _retain(scenario, 200)
    scenario.clock.current += timedelta(seconds=1)
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    child = await _retain(scenario, 201, 301)
    revised = NativeAccountIdentityProofPayload.model_validate(
        root.payload.model_dump() | {"recorded_at": _NOW + timedelta(seconds=2)}
    )
    digest = native_account_identity_digest(revised)
    assert child.payload.owner_root is not None
    forged = NativeAccountIdentityProofPayload.model_validate(
        child.payload.model_dump()
        | {"owner_root": child.payload.owner_root.model_dump() | {"content_digest": digest}}
    )
    await _restore_projection(
        scenario,
        300,
        {
            "timestamp": revised.recorded_at,
            "canonical_payload": canonical_native_account_identity(revised),
            "content_digest": digest,
        },
    )
    await _restore_projection(
        scenario,
        301,
        {
            "owner_root_content_digest": digest,
            "canonical_payload": canonical_native_account_identity(forged),
            "content_digest": native_account_identity_digest(forged),
        },
    )

    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountIdentityBindingError, match="actual_owner_root_mismatch"):
            await get_bound_native_account_identity(session, child.public_id)
