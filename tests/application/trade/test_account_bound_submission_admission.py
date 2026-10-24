"""Real account-family admission binding, compatibility, original knowledge and exact retries."""

import json
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update

import snapper.application.trade.account_bound_submission_admission as producer
import snapper.application.trade.unit_snapshot_submission as legacy_producer
from snapper.core.account_authority_reference import AccountAuthorityReference
from snapper.core.account_bound_submission_admission import AccountBoundSubmissionAdmissionPayload
from snapper.core.account_bound_submission_admission import account_bound_admission_digest
from snapper.core.account_bound_submission_admission import canonical_account_bound_admission
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.data.account_bound_submission_admission_store import BoundAccountSubmissionAdmission
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.account_bound_submission_admission_store import (
    insert_bound_account_submission_admission,
)
from snapper.data.execution_account_authority_store import require_current_account_authority
from snapper.data.execution_admission_triggers import install_execution_admission_table_triggers
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_unit_admission_store import UnitAdmissionConflictError
from snapper.data.execution_unit_admission_store import get_bound_submission_admission
from snapper.data.execution_unit_admission_store import observe_submission_catalogs
from snapper.data.execution_unit_admission_store import require_submission_command_history
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.execution_unit_snapshot_triggers import install_execution_unit_snapshot_triggers
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import InstrumentSpec
from snapper.data.models import SubmissionUnitAdmission
from snapper.data.models import TradeCommand
from snapper.data.models import WalletCredential
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from tests.helpers.account_bound_admission import AccountAdmissionScenario
from tests.helpers.account_bound_admission import account_admission_scenario
from tests.helpers.account_bound_admission import capture_account_admission
from tests.helpers.native_account_authority import capture_native_account_read
from tests.helpers.unit_admission import admission_id
from tests.helpers.unit_admission import admission_identity
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import refresh_admission_account

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[AccountAdmissionScenario]:
    """Own an actual source-bound native account and a separate real legacy account."""
    async with account_admission_scenario(tmp_path, monkeypatch) as value:
        yield value


async def _bound(
    scenario: AccountAdmissionScenario, result: SnapshotWriteResult
) -> BoundAccountSubmissionAdmission:
    """Read independently committed admission and account sources through the historical binder."""
    async with scenario.native.repository.session() as session:
        return await get_bound_account_submission_admission(session, result.public_id)


async def _counts(scenario: AccountAdmissionScenario) -> tuple[int, int]:
    """Observe actual retained baseline/admission counts after commit or rollback."""
    async with scenario.native.repository.session() as session:
        snapshots = await session.scalar(select(func.count()).select_from(ExecutionUnitSnapshot))
        admissions = await session.scalar(select(func.count()).select_from(SubmissionUnitAdmission))
    assert snapshots is not None and admissions is not None
    return snapshots, admissions


async def _write(
    scenario: AccountAdmissionScenario,
    payload: AccountBoundSubmissionAdmissionPayload,
    stamp: SnapshotWriteProvenance,
) -> SnapshotWriteResult:
    """Keep the actual wallet fence through source binding, immutable insertion and commit."""
    repository = scenario.native.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, payload.units.scope.wallet_public_id
        )
        result = await insert_bound_account_submission_admission(
            session, payload, stamp, scenario.native.policy
        )
        await session.commit()
        return result


async def _restore_admission(
    scenario: AccountAdmissionScenario, public_id: str, changes: dict[str, object]
) -> None:
    """Simulate a corrupt local restore and reinstall real guards before any semantic read."""
    async with scenario.native.repository.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER submission_unit_admissions_reject_update"))
        await connection.execute(
            update(SubmissionUnitAdmission)
            .where(SubmissionUnitAdmission.public_id == public_id)
            .values(**changes)
        )
        await connection.run_sync(
            install_execution_admission_table_triggers, "submission_unit_admissions"
        )


@pytest.mark.parametrize("native", [False, True])
async def test_real_source_family_and_original_units_commit_without_an_order_call(
    scenario: AccountAdmissionScenario, native: bool
) -> None:
    """Retain actual account knowledge and unit evidence without converting identity into trading.

    Given: Real retained legacy evidence or a closed signed native HTTP account read.
    When: The fence-owning producer captures that account's durable command.
    Then: Both immutable records commit with the exact family and no additional HTTP request.
    """
    requests = tuple(request.url for request in scenario.native.adapter.calls)
    result = await capture_account_admission(scenario, native)
    bound = await _bound(scenario, result)
    assert result.created is True
    assert await _counts(scenario) == (1, 1)
    assert bound.authority.family == ("native_accounts_v1" if native else "legacy_route_v1")
    assert bound.payload.authority.recorded_at == scenario.native.clock.current
    assert bound.recorded_at == scenario.native.clock.current
    assert bound.payload.units.schema_version == 2
    assert (
        bound.payload.units.request_material.wallet_public_id
        == bound.authority.payload.scope.wallet_public_id
    )
    assert tuple(request.url for request in scenario.native.adapter.calls) == requests
    async with scenario.native.repository.session() as session:
        original = await session.scalar(
            select(ExecutionUnitSnapshot).where(
                ExecutionUnitSnapshot.public_id == bound.payload.units.command_snapshot_public_id
            )
        )
        assert original is not None
        assert original.content_digest == bound.payload.units.command_snapshot_content_digest


@pytest.mark.parametrize("native", [False, True])
async def test_exact_retry_preserves_the_first_recording_clock_and_identity(
    scenario: AccountAdmissionScenario, native: bool
) -> None:
    """Repeat only source validation while retaining the original immutable attempt.

    Given: A committed admission whose account and catalog remain current.
    When: The same stable attempt is captured one second later.
    Then: It reuses the original identity and clocks without creating another record.
    """
    first = await capture_account_admission(scenario, native)
    original = await _bound(scenario, first)
    scenario.native.clock.current += timedelta(seconds=1)
    repeated = await capture_account_admission(scenario, native)
    assert repeated.public_id == first.public_id
    assert repeated.content_digest == first.content_digest
    assert repeated.created is False
    assert await _bound(scenario, repeated) == original
    assert await _counts(scenario) == (1, 1)


async def test_legacy_v2_history_is_unchanged_when_a_new_legacy_attempt_uses_v3(
    scenario: AccountAdmissionScenario,
) -> None:
    """Keep the original canonical version while sharing one cross-version attempt domain.

    Given: A genuine version-two legacy admission already references its original unit snapshot.
    When: A distinct attempt for the same account and command is captured through version three.
    Then: Both records preserve their own schemas and the original snapshot remains unchanged.
    """
    legacy = scenario.legacy
    first = await legacy_producer.capture_submission_unit_admission(
        legacy.repository, legacy.order, legacy.route, admission_identity(96)
    )
    async with legacy.repository.session() as session:
        original = await get_bound_submission_admission(session, first.public_id)
        before = await session.scalar(
            select(SubmissionUnitAdmission.canonical_payload).where(
                SubmissionUnitAdmission.public_id == first.public_id
            )
        )
    second = await capture_account_admission(scenario, native=False, number=97)
    bound = await _bound(scenario, second)
    assert bound.payload.units.command_snapshot_public_id == original.command_snapshot_public_id
    assert await _counts(scenario) == (1, 2)
    async with legacy.repository.session() as session:
        assert await get_bound_submission_admission(session, first.public_id) == original
        assert (
            await session.scalar(
                select(SubmissionUnitAdmission.canonical_payload).where(
                    SubmissionUnitAdmission.public_id == first.public_id
                )
            )
            == before
        )


async def test_same_attempt_cannot_be_reinterpreted_from_v2_to_v3(
    scenario: AccountAdmissionScenario,
) -> None:
    """Refuse an envelope-version switch for an already retained submit attempt.

    Given: A real legacy version-two admission is committed under a stable attempt UUID.
    When: Version three tries to capture that exact attempt again.
    Then: The producer refuses and keeps the original row and unit snapshot alone.
    """
    legacy = scenario.legacy
    await legacy_producer.capture_submission_unit_admission(
        legacy.repository, legacy.order, legacy.route, admission_identity(96)
    )
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError):
        await capture_account_admission(scenario, native=False, number=96)
    assert await _counts(scenario) == (1, 1)


async def test_later_cross_family_uuid_collision_preserves_history_and_refuses_current_capture(
    scenario: AccountAdmissionScenario,
) -> None:
    """Distinguish exact-family historical reads from ambiguous current discovery.

    Given: A committed native admission followed by a genuine legacy authority sharing its UUID.
    When: History is replayed and a new current attempt is requested.
    Then: Native history is unchanged while current capture refuses before adding any evidence.
    """
    result = await capture_account_admission(scenario)
    original = await _bound(scenario, result)
    scenario.native.clock.current += timedelta(seconds=1)
    collision = await refresh_admission_account(
        scenario.legacy, {}, scenario.native.clock.current, 500
    )
    assert collision.authority_public_id == scenario.native_route.authority_public_id
    assert collision.authority_content_digest != scenario.legacy.route.authority_content_digest
    async with scenario.native.repository.session() as session:
        legacy = await get_bound_route_authority(session, collision.authority_public_id)
    assert route_authority_content_digest(legacy) == collision.authority_content_digest
    assert await _bound(scenario, result) == original
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError):
        await capture_account_admission(scenario, number=97)
    assert await _counts(scenario) == (1, 1)


async def test_later_failed_native_read_prevents_an_older_success_from_authorizing_a_new_attempt(
    scenario: AccountAdmissionScenario,
) -> None:
    """Use latest attempted source state instead of selecting the last successful payload.

    Given: An original native admission and a later genuinely failed closed accounts request.
    When: The old route is offered for a new attempt.
    Then: Current capture refuses while the original historical evidence remains readable.
    """
    result = await capture_account_admission(scenario)
    original = await _bound(scenario, result)
    scenario.native.clock.current += timedelta(seconds=1)
    scenario.native.adapter.fail_accounts = True
    await capture_native_account_read(scenario.native)
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError):
        await capture_account_admission(scenario, number=97)
    assert await _bound(scenario, result) == original
    assert await _counts(scenario) == (1, 1)


async def test_native_current_capture_requires_its_trusted_freshness_policy(
    scenario: AccountAdmissionScenario,
) -> None:
    """Keep a shaped native reference from choosing or disabling freshness configuration.

    Given: A genuine current native source but no trusted native policy at the producer boundary.
    When: The actual command is offered for capture.
    Then: It refuses before either original units or admission evidence is inserted.
    """
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError):
        await producer.capture_account_submission_admission(
            scenario.native.repository,
            scenario.native_order,
            scenario.native_route,
            admission_identity(),
            None,
        )
    assert await _counts(scenario) == (0, 0)


async def test_expired_account_still_replays_but_cannot_refresh_an_exact_attempt(
    scenario: AccountAdmissionScenario,
) -> None:
    """Require current source validity even when the attempt would otherwise be an exact retry.

    Given: A retained native admission whose source expires after sixty seconds.
    When: The actual clock advances beyond that source lifetime.
    Then: Historical reading succeeds but current recapture refuses and preserves its old clocks.
    """
    result = await capture_account_admission(scenario)
    original = await _bound(scenario, result)
    scenario.native.clock.current += timedelta(seconds=61)
    assert await _bound(scenario, result) == original
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError):
        await capture_account_admission(scenario)
    assert await _counts(scenario) == (1, 1)


async def test_missing_historical_identity_has_no_account_family_fallback(
    scenario: AccountAdmissionScenario,
) -> None:
    """Keep an unknown admission UUID from selecting whichever current source exists.

    Given: Both source families exist but no admission has the requested identity.
    When: The historical binder resolves that UUID.
    Then: It reports the missing admission instead of returning any account route.
    """
    async with scenario.native.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="account_admission_missing"):
            await get_bound_account_submission_admission(session, admission_id(99999))


async def test_actual_authority_recording_clock_cannot_be_backdated_by_a_new_envelope(
    scenario: AccountAdmissionScenario,
) -> None:
    """Rebind local row knowledge even when all copied UUID and digest fields agree.

    Given: Actual native evidence and a new envelope claiming that authority existed earlier.
    When: The fenced writer compares its reference with the real current bound source.
    Then: It refuses the false recording clock before creating a second admission.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    authority = bound.payload.authority.model_copy(
        update={"recorded_at": bound.payload.authority.recorded_at - timedelta(seconds=1)}
    )
    proposed = bound.payload.model_copy(
        update={
            "authority": authority,
            "units": bound.payload.units.model_copy(update={"attempt_public_id": admission_id(97)}),
        }
    )
    with pytest.raises(SnapshotBindingError, match="authority_reference_mismatch"):
        await _write(scenario, proposed, admission_stamp(1097, scenario.native.clock.current))
    assert await _counts(scenario) == (1, 1)


async def test_rehashed_historical_reference_still_rebinds_actual_authority_knowledge(
    scenario: AccountAdmissionScenario,
) -> None:
    """Detect a consistent outer rehash that falsifies the independently retained source clock.

    Given: A corrupt restore changes authority recording time and recomputes the envelope digest.
    When: Actual immutable guards are restored and the historical reader binds the source.
    Then: It refuses the invented local knowledge instead of trusting canonical consistency alone.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    changed = bound.payload.model_copy(
        update={
            "authority": bound.payload.authority.model_copy(
                update={"recorded_at": bound.payload.authority.recorded_at - timedelta(seconds=1)}
            )
        }
    )
    await _restore_admission(
        scenario,
        result.public_id,
        {
            "canonical_payload": canonical_account_bound_admission(changed),
            "content_digest": account_bound_admission_digest(changed),
        },
    )
    async with scenario.native.repository.session() as session:
        with pytest.raises(ValueError, match="reference_mismatch"):
            await get_bound_account_submission_admission(session, result.public_id)


@pytest.mark.parametrize("number", [96, 97])
async def test_existing_attempt_with_future_recording_time_is_not_currently_known(
    scenario: AccountAdmissionScenario,
    number: int,
) -> None:
    """Check the actual retained row clock independently of its original interpretation payload.

    Given: A restored admission row claims a recording time after the actual current producer clock.
    When: Its stable attempt or a new attempt for the same command is captured.
    Then: Current validation refuses instead of returning evidence not yet durably known.
    """
    result = await capture_account_admission(scenario)
    await _restore_admission(
        scenario,
        result.public_id,
        {"timestamp": scenario.native.clock.current + timedelta(seconds=1)},
    )
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await capture_account_admission(scenario, number=number)
    assert isinstance(raised.value.__cause__, SnapshotBindingError)
    assert "prior_attempt_not_yet_known" in str(raised.value.__cause__)


async def test_new_attempt_cannot_reuse_another_admissions_public_identity(
    scenario: AccountAdmissionScenario,
) -> None:
    """Keep immutable row identity separate from attempt identity across exact source reuse.

    Given: One admission is retained and a new attempt reuses its public insertion UUID.
    When: The actual fenced writer validates the otherwise unchanged account and unit graph.
    Then: It reports a public identity conflict and leaves the original record alone.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    proposed = bound.payload.model_copy(
        update={
            "units": bound.payload.units.model_copy(update={"attempt_public_id": admission_id(97)})
        }
    )
    with pytest.raises(UnitAdmissionConflictError, match="public_identity_conflict"):
        await _write(scenario, proposed, admission_stamp(1096, scenario.native.clock.current))
    assert await _counts(scenario) == (1, 1)


async def test_same_attempt_cannot_acquire_a_new_interpretation_clock_at_the_store_boundary(
    scenario: AccountAdmissionScenario,
) -> None:
    """Refuse a changed immutable attempt even when all current source checks succeed.

    Given: A committed account/unit attempt and still-current underlying sources.
    When: A direct fenced insertion proposes a later interpretation for the same attempt.
    Then: The writer refuses changed evidence rather than renewing the existing witness.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    scenario.native.clock.current += timedelta(seconds=1)
    proposed = bound.payload.model_copy(
        update={
            "units": bound.payload.units.model_copy(
                update={"interpreted_at": scenario.native.clock.current}
            )
        }
    )
    with pytest.raises(UnitAdmissionConflictError, match="attempt_evidence_changed"):
        await _write(scenario, proposed, admission_stamp(1096, scenario.native.clock.current))
    assert await _bound(scenario, result) == bound


async def test_insert_cannot_claim_recording_before_its_own_interpretation(
    scenario: AccountAdmissionScenario,
) -> None:
    """Keep local producer provenance monotonic before any current source lookup.

    Given: An actual source graph whose proposed insertion timestamp predates interpretation.
    When: The fenced writer receives that contradictory provenance.
    Then: It refuses immediately without backdating the retained attempt.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    stamp = admission_stamp(1096, scenario.native.clock.current - timedelta(microseconds=1))
    with pytest.raises(SnapshotBindingError, match="insert_precedes_interpretation"):
        await _write(scenario, bound.payload, stamp)
    assert await _bound(scenario, result) == bound


async def test_retained_admission_cannot_predate_its_serialized_interpretation(
    scenario: AccountAdmissionScenario,
) -> None:
    """Check indexed original knowledge even when all canonical account/unit bytes remain intact.

    Given: A corrupt restore backdates the admission row below its serialized interpretation.
    When: The historical reader rebinds the restored row with physical guards installed.
    Then: It refuses the impossible immutable chronology.
    """
    result = await capture_account_admission(scenario)
    await _restore_admission(
        scenario,
        result.public_id,
        {"timestamp": scenario.native.clock.current - timedelta(microseconds=1)},
    )
    async with scenario.native.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="immutable_knowledge_mismatch"):
            await get_bound_account_submission_admission(session, result.public_id)


@pytest.mark.parametrize(
    "column", ["authority_content_digest", "account_scope_digest", "request_digest"]
)
async def test_indexed_account_and_request_copies_cannot_disagree_with_canonical_evidence(
    scenario: AccountAdmissionScenario, column: str
) -> None:
    """Rebind actual indexed projections instead of trusting a canonical payload alone.

    Given: A corrupt restore changes one well-shaped indexed digest while retaining the original envelope.
    When: The historical reader verifies the guarded row.
    Then: It refuses the projection mismatch before returning account evidence.
    """
    result = await capture_account_admission(scenario)
    await _restore_admission(scenario, result.public_id, {column: "f" * 64})
    async with scenario.native.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="projection_mismatch"):
            await get_bound_account_submission_admission(session, result.public_id)


async def test_original_unit_snapshot_must_already_be_known_at_current_interpretation(
    scenario: AccountAdmissionScenario,
) -> None:
    """Bind the original physical unit record's knowledge separately from its serialized source clocks.

    Given: A restored original unit row claims a recording time after current interpretation.
    When: A new account-bound attempt reuses that original unit record.
    Then: Current capture refuses the later durable knowledge without creating another admission.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    async with scenario.native.repository.engine.begin() as connection:
        await connection.execute(text("DROP TRIGGER execution_unit_snapshots_reject_update"))
        await connection.execute(
            update(ExecutionUnitSnapshot)
            .where(
                ExecutionUnitSnapshot.public_id == bound.payload.units.command_snapshot_public_id
            )
            .values(timestamp=scenario.native.clock.current + timedelta(seconds=1))
        )
        await connection.run_sync(install_execution_unit_snapshot_triggers)
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await capture_account_admission(scenario, number=97)
    assert isinstance(raised.value.__cause__, SnapshotBindingError)
    assert "original_snapshot_not_yet_known" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 1)


async def _clone_opposite_family_command(
    scenario: AccountAdmissionScenario, previous_native: bool
) -> None:
    """Create an actual independent intent whose CID collides with the other account family."""
    source_id = admission_id(5 if previous_native else 5000)
    client_id = (
        scenario.native_order.client_order_id
        if previous_native
        else scenario.legacy.order.client_order_id
    )
    async with scenario.native.repository.session() as session:
        original = (
            await session.execute(select(TradeCommand).where(TradeCommand.public_id == source_id))
        ).scalar_one()
        values = {
            column.key: getattr(original, column.key)
            for column in TradeCommand.__table__.columns
            if column.key != "id"
        }
        values.update(
            public_id=admission_id(6000),
            sequence_id=6000,
            client_order_id=client_id,
            venue_client_id=client_id,
            correlation_id=admission_id(6001),
        )
        session.add(TradeCommand(**values))
        await session.commit()


async def _capture_opposite_family_directly(
    scenario: AccountAdmissionScenario, previous_native: bool
) -> None:
    """Exercise the lower store's independent continuity invariant with wholly bound source rows."""
    route = scenario.legacy.route if previous_native else scenario.native_route
    repository = scenario.native.repository
    now = scenario.native.clock.current
    identity = admission_identity(98)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, route.scope.wallet_public_id)
        authority = await require_current_account_authority(
            session, route, now, scenario.native.policy
        )
        command = (
            await session.execute(
                select(TradeCommand).where(TradeCommand.public_id == admission_id(6000))
            )
        ).scalar_one()
        first_known = await require_submission_command_history(session, command)
        observations = await observe_submission_catalogs(session, command, first_known, now)
        baseline = await legacy_producer._capture_original(
            session,
            command,
            observations,
            legacy_producer._provenance(identity.command_snapshot, now),
        )
        payload = AccountBoundSubmissionAdmissionPayload(
            units=legacy_producer._admission_payload(
                route, identity, baseline, command, observations
            ),
            authority=AccountAuthorityReference(
                family=authority.family,
                public_id=authority.public_id,
                content_digest=authority.content_digest,
                recorded_at=authority.recorded_at,
            ),
        )
        await insert_bound_account_submission_admission(
            session,
            payload,
            legacy_producer._provenance(identity.admission, now),
            scenario.native.policy,
        )
        await session.commit()


@pytest.mark.parametrize("previous_native", [False, True])
async def test_existing_client_order_id_cannot_change_authority_family_at_the_store_boundary(
    scenario: AccountAdmissionScenario, previous_native: bool
) -> None:
    """Keep cross-account CID continuity independent of the outer producer's ambiguity checks.

    Given: An actual admission and a new opposite-family command with the same exchange and CID.
    When: The lower fenced store receives independently rebound account, catalog and unit sources.
    Then: It refuses the family change and rolls back the newly proposed original unit record.
    """
    await capture_account_admission(scenario, native=previous_native)
    await _clone_opposite_family_command(scenario, previous_native)
    with pytest.raises(SnapshotBindingError, match="command_authority_family_changed"):
        await _capture_opposite_family_directly(scenario, previous_native)
    assert await _counts(scenario) == (1, 1)


async def test_rehashed_route_revision_must_match_the_actual_historical_account_source(
    scenario: AccountAdmissionScenario,
) -> None:
    """Rebind selected client revision instead of trusting copied account UUID and digest alone.

    Given: A corrupt restore changes only the embedded route revision and rehashes the envelope.
    When: The historical reader loads the exact unchanged authority source.
    Then: Its actual route revision disagrees and the reader refuses the restored witness.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    changed = bound.payload.model_copy(
        update={
            "units": bound.payload.units.model_copy(
                update={
                    "route": bound.payload.units.route.model_copy(
                        update={"route_revision": "f" * 64}
                    )
                }
            )
        }
    )
    await _restore_admission(
        scenario,
        result.public_id,
        {
            "canonical_payload": canonical_account_bound_admission(changed),
            "content_digest": account_bound_admission_digest(changed),
        },
    )
    async with scenario.native.repository.session() as session:
        with pytest.raises(SnapshotBindingError, match="authority_reference_mismatch"):
            await get_bound_account_submission_admission(session, result.public_id)


async def _refresh_catalog_observation(scenario: AccountAdmissionScenario) -> None:
    """Persist one actual later catalog revision with identical economic dimensions."""
    now = scenario.native.clock.current
    async with scenario.native.repository.session() as session:
        previous = (
            await session.execute(
                select(InstrumentSpec).where(InstrumentSpec.known_to == KNOWN_TO_MAX)
            )
        ).scalar_one()
        values = {
            column.key: getattr(previous, column.key)
            for column in InstrumentSpec.__table__.columns
            if column.key != "id"
        }
        previous.known_to = now
        values.update(
            timestamp=now,
            known_to=KNOWN_TO_MAX,
            sequence_id=9000,
            spec_observed_at=now,
            spec_version=f"{FUTURES_SPEC_ETL_VERSION}:later-observation",
        )
        session.add(InstrumentSpec(**values))
        await session.commit()


async def test_catalog_refresh_requires_a_distinct_attempt_while_preserving_original_units(
    scenario: AccountAdmissionScenario,
) -> None:
    """Keep current observations exact without treating a benign revision as changed unit economics.

    Given: A retained attempt and a real later catalog observation with equal economic dimensions.
    When: The same attempt is retried and then a distinct attempt is captured.
    Then: Reuse refuses changed evidence while the new witness retains the original unit snapshot.
    """
    first = await capture_account_admission(scenario)
    original = await _bound(scenario, first)
    scenario.native.clock.current += timedelta(seconds=1)
    await _refresh_catalog_observation(scenario)
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await capture_account_admission(scenario)
    assert isinstance(raised.value.__cause__, SnapshotBindingError)
    assert "retained_attempt_context_changed" in str(raised.value.__cause__)
    second = await capture_account_admission(scenario, number=97)
    current = await _bound(scenario, second)
    assert (
        current.payload.units.command_snapshot_public_id
        == original.payload.units.command_snapshot_public_id
    )
    assert current.payload.units.economic_context == original.payload.units.economic_context
    assert current.payload.units.current_catalog != original.payload.units.current_catalog
    assert await _counts(scenario) == (1, 2)


@pytest.mark.parametrize("environment", [False, True])
async def test_one_command_cannot_move_to_another_margin_account_or_environment(
    scenario: AccountAdmissionScenario, environment: bool
) -> None:
    """Pin actual account scope across independent source reads and new attempts.

    Given: An original legacy-family v3 admission for a durable production command.
    When: A later real account observation presents another margin account or demo origin.
    Then: Current insertion refuses the changed scope and preserves the original evidence.
    """
    await capture_account_admission(scenario, native=False)
    scope = scenario.legacy.authority.scope
    changes: dict[str, object] = {
        "scope": scope.model_copy(update={"margin_account": "futures.cash"})
    }
    if environment:
        changes = {
            "scope": scope.model_copy(update={"environment": "demo"}),
            "public_api_origin": "https://demo-futures.kraken.com",
        }
    scenario.native.clock.current += timedelta(seconds=1)
    route = await refresh_admission_account(
        scenario.legacy, changes, scenario.native.clock.current, 400
    )
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await producer.capture_account_submission_admission(
            scenario.native.repository,
            scenario.legacy.order,
            route,
            admission_identity(97),
            None,
        )
    assert isinstance(raised.value.__cause__, SnapshotBindingError)
    assert "command_account_changed" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 1)


async def _rotate_legacy_credential(scenario: AccountAdmissionScenario) -> None:
    """Commit an actual later selected credential while preserving its historical predecessor."""
    scenario.native.clock.current += timedelta(seconds=1)
    async with scenario.native.repository.session() as session:
        old = (
            await session.execute(
                select(WalletCredential).where(WalletCredential.public_id == admission_id(90))
            )
        ).scalar_one()
        old.known_to = scenario.native.clock.current
        session.add(
            WalletCredential(
                **admission_stamp(94, scenario.native.clock.current).model_dump(mode="python"),
                wallet_public_id=scenario.legacy.authority.scope.wallet_public_id,
                exchange=scenario.legacy.authority.scope.exchange,
                credential_type="api_key_secret",
                encrypted_payload="isolated-new-fixture-value-never-decrypted",
            )
        )
        await session.commit()


@pytest.mark.parametrize("identity", ["unknown", "same", "different"])
async def test_actual_credential_rotation_requires_the_same_proven_venue_account(
    scenario: AccountAdmissionScenario, identity: str
) -> None:
    """Keep current credentials separate from proof of continuity of the external account.

    Given: An original v3 admission followed by an actual selected credential revision.
    When: Its fresh account observation reports an unknown, equal or conflicting venue account.
    Then: Only the same explicitly known account permits a distinct admission witness.
    """
    account_id = None if identity == "unknown" else "venue-account-original"
    scenario.native.clock.current += timedelta(seconds=1)
    first_route = await refresh_admission_account(
        scenario.legacy, {"venue_account_id": account_id}, scenario.native.clock.current, 400
    )
    first = await producer.capture_account_submission_admission(
        scenario.native.repository,
        scenario.legacy.order,
        first_route,
        admission_identity(96),
        None,
    )
    original = await _bound(scenario, first)
    await _rotate_legacy_credential(scenario)
    credential_at = scenario.native.clock.current
    scenario.native.clock.current += timedelta(seconds=1)
    current_route = await refresh_admission_account(
        scenario.legacy,
        {
            "credential_public_id": admission_id(94),
            "credential_timestamp": credential_at,
            "venue_account_id": "venue-account-other" if identity == "different" else account_id,
        },
        scenario.native.clock.current,
        401,
    )
    if identity == "same":
        result = await producer.capture_account_submission_admission(
            scenario.native.repository,
            scenario.legacy.order,
            current_route,
            admission_identity(97),
            None,
        )
        assert result.created is True
        assert await _counts(scenario) == (1, 2)
    else:
        with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
            await producer.capture_account_submission_admission(
                scenario.native.repository,
                scenario.legacy.order,
                current_route,
                admission_identity(97),
                None,
            )
        reason = "venue_account_changed" if identity == "different" else "rotation_account_unproven"
        assert isinstance(raised.value.__cause__, SnapshotBindingError)
        assert reason in str(raised.value.__cause__)
        assert await _counts(scenario) == (1, 1)
    assert await _bound(scenario, first) == original


@pytest.mark.parametrize("version", [True, 3.0, "3", 4, 2])
async def test_restored_outer_version_cannot_select_a_more_permissive_historical_reader(
    scenario: AccountAdmissionScenario, version: object
) -> None:
    """Keep stored-version dispatch strict across the two independently frozen payload formats.

    Given: A restored v3 envelope has a coerced, unsupported or falsely legacy outer version.
    When: A new attempt must rebind that command's actual earlier admission.
    Then: Both literal-version models refuse the corruption before another admission commits.
    """
    result = await capture_account_admission(scenario)
    bound = await _bound(scenario, result)
    material = bound.payload.model_dump(mode="json")
    material["schema_version"] = version
    await _restore_admission(
        scenario,
        result.public_id,
        {"canonical_payload": json.dumps(material, sort_keys=True, separators=(",", ":"))},
    )
    with pytest.raises(producer.SubmissionUnitSnapshotUnavailableError) as raised:
        await capture_account_admission(scenario, number=97)
    assert isinstance(raised.value.__cause__, ValidationError)
    assert "schema_version" in str(raised.value.__cause__)
    assert await _counts(scenario) == (1, 1)
