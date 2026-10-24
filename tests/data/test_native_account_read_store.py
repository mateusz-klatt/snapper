"""Real durable request ordering, outcome binding and refusal of stale account responses."""

import asyncio
import base64
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

from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.native_account_read import NativeAccountReadCapturedSource
from snapper.core.native_account_read import NativeAccountReadFailure
from snapper.core.native_account_read import NativeAccountReadOutcomePayload
from snapper.core.native_account_read import NativeAccountReadResult
from snapper.core.native_account_read import canonical_native_account_read_outcome
from snapper.core.native_account_read import native_account_read_attempt_digest
from snapper.data import native_account_read_store
from snapper.data.accounting_observation_binding import AccountingObservationBindingError
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.models import AccountingHoldResolution
from snapper.data.models import NativeAccountReadAttempt
from snapper.data.models import Wallet
from snapper.data.models import WalletCredential
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.data.native_account_read_store import append_native_account_read_outcome
from snapper.data.native_account_read_store import begin_native_account_read
from snapper.data.native_account_read_store import get_bound_native_account_read_attempt
from snapper.data.native_account_read_store import get_bound_native_account_read_outcome
from snapper.data.native_account_read_store import get_latest_native_account_read
from snapper.data.native_account_read_store import native_account_response_public_id
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 10, 23, 12, tzinfo=UTC)
_CREDENTIAL_AT = _NOW - timedelta(days=1)


def _id(number: int) -> str:
    """Keep each fixture request, source and outcome identity independently recognizable."""
    return f"00000000-0000-7000-8000-{number:012d}"


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Supply stable local identity without caller-authored knowledge clocks."""
    return ExecutionEvidenceIdentity(public_id=_id(number), session_id=_id(999), sequence_id=number)


@dataclass
class _Clock:
    """Move producer time explicitly without sleeping or altering stored source timestamps."""

    current: datetime = _NOW

    def now(self, zone: object) -> datetime:
        """Provide the producer's actual local clock at its read boundary."""
        assert zone is UTC
        return self.current


@dataclass
class _Scenario:
    """Actual isolated database, public selected client and independently controlled clock."""

    repository: SQLAlchemyRepository
    selection: ExecutionClientSelection
    clock: _Clock


@pytest.fixture
async def scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Scenario]:
    """Create real live wallet metadata and a selected nonsecret fixture credential revision."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-read.db'}")
    clock = _Clock()
    monkeypatch.setattr(native_account_read_store, "datetime", clock)
    selected = ExecutionClientSelection(
        wallet_public_id=_id(1),
        exchange="kraken_futures",
        mode="live",
        environment="production",
        credential_public_id=_id(2),
        credential_timestamp=_CREDENTIAL_AT,
        public_api_origin="https://futures.kraken.com",
    )
    try:
        await repository.create_all()
        async with repository.session() as session:
            session.add_all(
                [
                    Wallet(
                        **_identity(1).model_dump(),
                        timestamp=_CREDENTIAL_AT,
                        label="native-read-fixture",
                        is_paper=False,
                    ),
                    WalletCredential(
                        **_identity(2).model_dump(),
                        timestamp=_CREDENTIAL_AT,
                        wallet_public_id=_id(1),
                        exchange="kraken_futures",
                        credential_type="api_key_secret",
                        encrypted_payload="fixture-only-never-decrypted",
                    ),
                ]
            )
            await session.commit()
        yield _Scenario(repository, selected, clock)
    finally:
        await repository.engine.dispose()


async def _finish(
    scenario: _Scenario, attempt: int, result: NativeAccountReadResult, identity: int = 300
) -> BoundNativeAccountReadOutcome:
    """Use the actual transaction and wallet fence required by the outcome writer."""
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, scenario.selection.wallet_public_id
        )
        value = await append_native_account_read_outcome(
            session, _id(attempt), result, _identity(identity)
        )
        await session.commit()
        return value


async def _source(scenario: _Scenario, **updates: object) -> NativeAccountReadCapturedSource:
    """Retain actual original bytes; this existing movement writer intentionally retains its hold."""
    selected = scenario.selection
    raw = RawAccountingObservation(
        scope=AccountingObservationScope(
            wallet_public_id=selected.wallet_public_id,
            exchange=selected.exchange,
            mode=selected.mode,
            environment=selected.environment,
        ),
        route_revision=client_selection_digest(selected),
        credential_public_id=selected.credential_public_id,
        credential_timestamp=selected.credential_timestamp,
        kind="account_cash",
        channel="kraken_futures.rest.accounts.v1",
        representation="wire_bytes_base64",
        material=base64.b64encode(b'{"result":"success","accounts":{}}').decode(),
        observed_at=_NOW + timedelta(seconds=1),
        source_identity=_id(200),
        source_identity_kind="unproven",
    )
    raw = RawAccountingObservation.model_validate({**raw.model_dump(), **updates})
    stamp = AccountingWriteProvenance(
        **{**_identity(400).model_dump(), "public_id": native_account_response_public_id(_id(200))},
        timestamp=_NOW + timedelta(seconds=2),
    )
    retained = await capture_accounting_observation(scenario.repository, raw, stamp)
    scenario.clock.current = _NOW + timedelta(seconds=3)
    return NativeAccountReadCapturedSource(
        raw_receipt_public_id=retained.public_id,
        raw_content_digest=retained.content_digest,
        request_started_at=_NOW,
        source_observed_at=raw.observed_at,
        source_recorded_at=stamp.timestamp,
    )


async def test_attempt_commits_before_io_and_exact_retry_preserves_its_original_ordinal(
    scenario: _Scenario,
) -> None:
    """Request knowledge survives a process boundary before any response exists.

    Given: An actual selected wallet and credential,
    When: A read starts and its commit acknowledgement is retried later,
    Then: Independent sessions see one pending request with its original start and ordinal.
    """
    first = await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    scenario.clock.current += timedelta(seconds=10)
    retry = await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    assert retry == first
    assert first.payload.started_at == _NOW
    assert first.payload.family_sequence == 1
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_read_attempt(session, first.public_id) == first
        latest = await get_latest_native_account_read(
            session, scenario.selection, scenario.clock.current
        )
        assert latest is not None and latest.attempt == first and latest.outcome is None
        assert (
            await session.execute(select(func.count()).select_from(NativeAccountReadAttempt))
        ).scalar_one() == 1


async def test_concurrent_actual_writers_allocate_one_shared_contiguous_family_order(
    scenario: _Scenario,
) -> None:
    """Concurrent request starts use the same database execution fence.

    Given: Four independently identified reads of one actual account,
    When: Their real SQLite transactions begin concurrently,
    Then: Each committed request receives exactly one distinct contiguous ordinal.
    """
    attempts = await asyncio.gather(
        *(
            begin_native_account_read(scenario.repository, scenario.selection, _identity(number))
            for number in range(200, 204)
        )
    )
    assert sorted(value.payload.family_sequence for value in attempts) == [1, 2, 3, 4]


async def test_pending_then_failed_new_request_never_restores_an_older_captured_response(
    scenario: _Scenario,
) -> None:
    """The newest request owns eligibility even without usable output.

    Given: One retained successful source and a newer request,
    When: The newer request remains pending and later fails,
    Then: Both reads expose that newer state without falling back or releasing a hold.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    original = await _finish(scenario, 200, await _source(scenario))
    newer = await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    async with scenario.repository.session() as session:
        pending = await get_latest_native_account_read(
            session, scenario.selection, scenario.clock.current
        )
    assert pending is not None and pending.attempt == newer and pending.outcome is None
    failed = await _finish(scenario, 201, NativeAccountReadFailure(reason="transport_error"), 301)
    async with scenario.repository.session() as session:
        latest = await get_latest_native_account_read(
            session, scenario.selection, scenario.clock.current
        )
        historical = await get_bound_native_account_read_outcome(session, _id(200))
        assert (
            await session.execute(select(func.count()).select_from(AccountingHoldResolution))
        ).scalar_one() == 0
    assert latest is not None and latest.outcome == failed
    assert historical == original


async def test_late_older_response_and_old_attempt_retry_cannot_reclaim_current_status(
    scenario: _Scenario,
) -> None:
    """Arrival order does not replace request order.

    Given: Two committed requests and a completed cancellation of the newer one,
    When: The original request later completes and is retried,
    Then: Historical source survives while the newer cancellation remains current.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))
    latest_result = await _finish(scenario, 201, NativeAccountReadFailure(reason="cancelled"), 301)
    await _finish(scenario, 200, await _source(scenario))
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    async with scenario.repository.session() as session:
        latest = await get_latest_native_account_read(
            session, scenario.selection, scenario.clock.current
        )
    assert latest is not None and latest.outcome == latest_result


async def test_unfinished_attempt_survives_reopening_the_actual_database(
    scenario: _Scenario,
) -> None:
    """Process-local state is unnecessary to preserve an interrupted read.

    Given: A committed request without any response,
    When: A separate repository instance opens the database,
    Then: It observes the same latest pending request.
    """
    original = await begin_native_account_read(
        scenario.repository, scenario.selection, _identity(200)
    )
    reopened = SQLAlchemyRepository(str(scenario.repository.engine.url))
    try:
        async with reopened.session() as session:
            latest = await get_latest_native_account_read(session, scenario.selection, _NOW)
        assert latest is not None and latest.attempt == original and latest.outcome is None
    finally:
        await reopened.engine.dispose()


async def test_credential_rotation_invalidates_the_prior_client_without_resetting_family_order(
    scenario: _Scenario,
) -> None:
    """Credential revisions share ordering but cannot reuse current source authority.

    Given: One request through the previous current credential revision,
    When: The actual selected credential rotates and a new request starts,
    Then: Old selection is refused and the new client receives the next family ordinal.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    async with scenario.repository.session() as session:
        credential = (await session.execute(select(WalletCredential))).scalar_one()
        credential.timestamp = _NOW - timedelta(hours=1)
        await session.commit()
    rotated = ExecutionClientSelection.model_validate(
        {**scenario.selection.model_dump(), "credential_timestamp": _NOW - timedelta(hours=1)}
    )
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountReadBindingError, match="selected_credential_changed"):
            await get_latest_native_account_read(session, scenario.selection, _NOW)
        with pytest.raises(NativeAccountReadBindingError, match="latest_client_changed"):
            await get_latest_native_account_read(session, rotated, _NOW)
    next_read = await begin_native_account_read(scenario.repository, rotated, _identity(201))
    assert next_read.payload.family_sequence == 2


async def test_demo_environment_has_a_separate_family_but_cannot_reuse_a_production_request_id(
    scenario: _Scenario,
) -> None:
    """Actual endpoint selection distinguishes authenticated production and demo accounts.

    Given: An existing production request,
    When: The same wallet selects the actual demo origin,
    Then: Its separate family starts at one and a crossed request UUID is refused.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    demo = ExecutionClientSelection.model_validate(
        {
            **scenario.selection.model_dump(),
            "environment": "demo",
            "public_api_origin": "https://demo-futures.kraken.com",
        }
    )
    with pytest.raises(NativeAccountReadBindingError, match="attempt_identity_changed"):
        await begin_native_account_read(scenario.repository, demo, _identity(200))
    separate = await begin_native_account_read(scenario.repository, demo, _identity(201))
    assert separate.payload.family_sequence == 1


async def test_exact_outcome_retry_retains_original_completion_and_refuses_changed_identity_or_result(
    scenario: _Scenario,
) -> None:
    """One request cannot acquire multiple independently authored final results.

    Given: A committed explicit transport failure,
    When: Its exact identity is retried and contradictory retries are attempted,
    Then: Original completion survives and changed material or identity is rejected.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    result = NativeAccountReadFailure(reason="transport_error")
    original = await _finish(scenario, 200, result)
    scenario.clock.current += timedelta(seconds=1)
    assert await _finish(scenario, 200, result) == original
    with pytest.raises(NativeAccountReadBindingError, match="outcome_identity_changed"):
        await _finish(scenario, 200, NativeAccountReadFailure(reason="cancelled"))
    with pytest.raises(NativeAccountReadBindingError, match="outcome_identity_changed"):
        await _finish(scenario, 200, result, 301)


async def test_outcome_identity_cannot_cross_between_distinct_actual_requests(
    scenario: _Scenario,
) -> None:
    """Unique outcome identity binds its original attempt.

    Given: Two committed requests and one finalized result,
    When: The other request attempts to reuse that result UUID,
    Then: The writer refuses the crossed identity before insertion.
    """
    for number in (200, 201):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(number))
    result = NativeAccountReadFailure(reason="transport_error")
    await _finish(scenario, 200, result)
    with pytest.raises(NativeAccountReadBindingError, match="outcome_identity_reused"):
        await _finish(scenario, 201, result)


async def test_outcome_writer_neither_invents_a_missing_attempt_nor_commits_the_callers_transaction(
    scenario: _Scenario,
) -> None:
    """The outcome write participates in the caller's atomic capture transaction.

    Given: One real attempt and an independently missing request identity,
    When: An outcome is staged then rolled back and the missing request is queried,
    Then: No result remains and missing parent evidence is refused.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    async with scenario.repository.session() as session:
        await scenario.repository._begin_execution_insert_transaction(session)
        await scenario.repository._acquire_execution_fence_lock(
            session, scenario.selection.wallet_public_id
        )
        await append_native_account_read_outcome(
            session, _id(200), NativeAccountReadFailure(reason="cancelled"), _identity(300)
        )
        await session.rollback()
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_read_outcome(session, _id(200)) is None
        with pytest.raises(NativeAccountReadBindingError, match="attempt_missing"):
            await get_bound_native_account_read_outcome(session, _id(900))


@pytest.mark.parametrize(
    "change",
    (
        "wallet_missing",
        "wallet_paper",
        "credential_missing",
        "credential_type",
        "credential_revision",
    ),
)
async def test_current_public_database_selection_is_required_independently_of_client_metadata(
    scenario: _Scenario,
    change: str,
) -> None:
    """A syntactically correct client identity is insufficient without actual selected metadata.

    Given: A missing or contradictory current wallet or credential revision,
    When: A new account read attempts to commit,
    Then: Selection is refused and no request is retained.
    """
    async with scenario.repository.session() as session:
        wallet = (await session.execute(select(Wallet))).scalar_one()
        credential = (await session.execute(select(WalletCredential))).scalar_one()
        if change == "wallet_missing":
            wallet.known_to = _NOW
        elif change == "wallet_paper":
            wallet.is_paper = True
        elif change == "credential_missing":
            credential.known_to = _NOW
        elif change == "credential_type":
            credential.credential_type = "paper"
        else:
            credential.timestamp = _NOW - timedelta(hours=1)
        await session.commit()
    with pytest.raises(
        NativeAccountReadBindingError, match="native_read_(live_wallet|selected_credential)"
    ):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    async with scenario.repository.session() as session:
        assert (
            await session.execute(select(func.count()).select_from(NativeAccountReadAttempt))
        ).scalar_one() == 0


async def test_regressed_local_clock_cannot_append_a_later_ordinal_with_earlier_knowledge(
    scenario: _Scenario,
) -> None:
    """Request ordering never disguises a backwards local knowledge clock.

    Given: One already committed attempt,
    When: The next producer clock regresses,
    Then: The write refuses rather than silently changing the retained start time.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    scenario.clock.current -= timedelta(seconds=1)
    with pytest.raises(NativeAccountReadBindingError, match="knowledge_clock_regressed"):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(201))


async def test_never_queried_family_is_explicit_absence_and_naive_knowledge_is_refused(
    scenario: _Scenario,
) -> None:
    """Missing requests do not manufacture empty account state.

    Given: An actual wallet that has never been queried,
    When: A current read is requested with aware and ambiguous local clocks,
    Then: Only honest absence is returned and the ambiguous clock is rejected.
    """
    async with scenario.repository.session() as session:
        assert await get_latest_native_account_read(session, scenario.selection, _NOW) is None
        with pytest.raises(NativeAccountReadBindingError, match="clock_not_aware"):
            await get_latest_native_account_read(
                session, scenario.selection, _NOW.replace(tzinfo=None)
            )


@pytest.mark.parametrize("phase", ("attempt", "outcome"))
async def test_latest_read_does_not_fallback_around_a_result_beyond_its_knowledge_boundary(
    scenario: _Scenario,
    phase: str,
) -> None:
    """The latest request remains visible even when the consumer clock is older.

    Given: A stored request or outcome beyond the consumer's local knowledge clock,
    When: The latest account read is requested,
    Then: The future record is refused without selecting an earlier result.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    at = _NOW - timedelta(microseconds=1)
    if phase == "outcome":
        scenario.clock.current += timedelta(seconds=1)
        await _finish(scenario, 200, NativeAccountReadFailure(reason="cancelled"))
        at = _NOW
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountReadBindingError, match=f"{phase}_not_yet_known"):
            await get_latest_native_account_read(session, scenario.selection, at)


@pytest.mark.parametrize(
    "field,value",
    (
        ("route_revision", "changed-selection"),
        ("credential_public_id", _id(90)),
        ("credential_timestamp", _CREDENTIAL_AT + timedelta(seconds=1)),
        ("kind", "unknown"),
        ("representation", "opaque_text"),
        ("target_json_pointer", "/accounts"),
        (
            "scope",
            {
                "wallet_public_id": _id(1),
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "demo",
            },
        ),
        (
            "scope",
            {
                "wallet_public_id": _id(1),
                "exchange": "kraken_futures",
                "mode": "live",
                "environment": "production",
                "margin_account": "flex",
            },
        ),
    ),
)
async def test_actual_raw_response_must_match_the_original_request_selection(
    scenario: _Scenario,
    field: str,
    value: object,
) -> None:
    """Raw evidence is rebound instead of trusting a caller's captured label.

    Given: Retained original material with one contradictory source coordinate,
    When: An account read outcome references it,
    Then: The mismatch prevents outcome insertion while raw evidence remains retained.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = await _source(scenario, **{field: value})
    with pytest.raises(NativeAccountReadBindingError, match="source_binding_mismatch"):
        await _finish(scenario, 200, source)


@pytest.mark.parametrize(
    "channel", ("kraken_futures.ws.balances", "kraken_futures.rest.accounts.http_error.v1")
)
async def test_captured_success_cannot_relabel_an_unrelated_feed_or_http_error(
    scenario: _Scenario,
    channel: str,
) -> None:
    """Successful source kind remains bound to the actual account HTTP response channel.

    Given: A retained response on an incompatible source channel,
    When: It is claimed as a successful captured account response,
    Then: The writer refuses source promotion.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = await _source(scenario, channel=channel)
    with pytest.raises(NativeAccountReadBindingError, match="source_channel_mismatch"):
        await _finish(scenario, 200, source)


async def test_http_failure_retains_its_actual_body_reference_without_promoting_it(
    scenario: _Scenario,
) -> None:
    """Retained error material remains inspectable as a failed request.

    Given: Original bytes retained from a non-success HTTP account response,
    When: Its failure outcome is committed,
    Then: The exact source remains attached and the newest result is failed.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = await _source(scenario, channel="kraken_futures.rest.accounts.http_error.v1")
    retained = await _finish(
        scenario, 200, NativeAccountReadFailure(reason="http_error", source=source)
    )
    async with scenario.repository.session() as session:
        latest = await get_latest_native_account_read(
            session, scenario.selection, scenario.clock.current
        )
    assert latest is not None and latest.outcome == retained
    assert retained.payload.result.kind == "failed"
    assert retained.payload.source_reference == source


async def test_one_original_http_response_cannot_supply_two_distinct_read_attempts(
    scenario: _Scenario,
) -> None:
    """Response identity binds one actual network read independently of equal account amounts.

    Given: Two requests started before one original response was received,
    When: The same retained response is claimed by both requests,
    Then: Only its original outcome survives and the second request remains pending.
    """
    for number in (200, 201):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(number))
    source = await _source(scenario)
    await _finish(scenario, 200, source)
    with pytest.raises(NativeAccountReadBindingError, match="source_attempt_mismatch"):
        await _finish(scenario, 201, source, 301)
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_read_outcome(session, _id(201)) is None


async def test_unclaimed_response_from_another_request_cannot_be_borrowed_as_current_evidence(
    scenario: _Scenario,
) -> None:
    """Source affinity is required even before the response has an outcome owner.

    Given: Two actual requests and an unclaimed response retained for the first request,
    When: The second request attempts to use that same-client temporally valid response,
    Then: The original request tag and deterministic receipt identity prevent borrowing.
    """
    for number in (200, 201):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(number))
    source = await _source(scenario)
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_read_outcome(session, _id(200)) is None
    with pytest.raises(NativeAccountReadBindingError, match="source_attempt_mismatch"):
        await _finish(scenario, 201, source, 301)
    original = await _finish(scenario, 200, source)
    assert original.payload.source_reference == source


@pytest.mark.parametrize(
    "updates",
    (
        {"source_identity": _id(201)},
        {"source_identity_kind": "venue_execution_id"},
        {"source_identity": None, "source_identity_kind": None},
    ),
)
async def test_raw_identity_alone_cannot_substitute_the_original_local_attempt_tag(
    scenario: _Scenario,
    updates: dict[str, object],
) -> None:
    """Both deterministic local UUID and its canonical request tag are independently bound.

    Given: The expected raw UUID with changed or missing local attempt provenance,
    When: The actual request tries to record a source outcome,
    Then: The canonical source tag mismatch is refused.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = await _source(scenario, **updates)
    with pytest.raises(NativeAccountReadBindingError, match="source_attempt_mismatch"):
        await _finish(scenario, 200, source)


async def test_later_raw_identity_conflict_invalidates_current_read_but_preserves_history(
    scenario: _Scenario,
) -> None:
    """Conflict knowledge cannot silently preserve current eligibility of its original response.

    Given: An original captured result followed by changed bytes claiming the same receipt UUID,
    When: Historical and current read evidence are requested,
    Then: Original history remains inspectable while current source eligibility is refused.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = await _source(scenario)
    original = await _finish(scenario, 200, source)
    async with scenario.repository.session() as session:
        raw = await get_bound_accounting_observation(session, source.raw_receipt_public_id)
    changed = RawAccountingObservation.model_validate(
        {
            **raw.model_dump(),
            "material": base64.b64encode(b"different original body").decode(),
        }
    )
    conflict = await capture_accounting_observation(
        scenario.repository,
        changed,
        AccountingWriteProvenance(
            **{**_identity(400).model_dump(), "public_id": source.raw_receipt_public_id},
            timestamp=scenario.clock.current,
        ),
    )
    assert conflict.conflicted is True
    async with scenario.repository.session() as session:
        assert await get_bound_native_account_read_outcome(session, _id(200)) == original
        with pytest.raises(NativeAccountReadBindingError, match="source_identity_contested"):
            await get_latest_native_account_read(
                session, scenario.selection, scenario.clock.current
            )


@pytest.mark.parametrize(
    "change",
    (
        {"public_api_origin": "https://unrelated.example"},
        {"exchange": "kraken"},
        {"environment": "demo"},
    ),
)
async def test_unsupported_source_is_refused_even_when_no_family_attempt_exists(
    scenario: _Scenario,
    change: dict[str, str],
) -> None:
    """A missing account read cannot mask an unproven actual client route.

    Given: Syntactically valid public metadata for a mismatched endpoint or exchange,
    When: The current family is queried without any previous attempts,
    Then: Source selection is refused before returning an empty state.
    """
    selection = ExecutionClientSelection.model_validate(
        {**scenario.selection.model_dump(), **change}
    )
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountReadBindingError, match="client_source_unproven"):
            await get_latest_native_account_read(session, selection, _NOW)


async def test_same_attempt_retry_cannot_start_network_io_before_its_original_knowledge(
    scenario: _Scenario,
) -> None:
    """Commit retries preserve chronology even after a local clock regression.

    Given: A committed attempt and a subsequently regressed producer clock,
    When: Its exact request identity is retried,
    Then: The writer refuses to return a network-ready future attempt.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    scenario.clock.current -= timedelta(microseconds=1)
    with pytest.raises(NativeAccountReadBindingError, match="knowledge_clock_regressed"):
        await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))


async def test_self_consistent_outcome_cannot_replace_its_actual_stored_attempt(
    scenario: _Scenario,
) -> None:
    """Recomputing embedded hashes does not replace the independently retained request.

    Given: A damaged restore with an internally valid but substituted embedded attempt,
    When: The latest outcome is independently rebound to its real parent row,
    Then: The parent mismatch is refused before indexed result fields are trusted.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    outcome = await _finish(scenario, 200, NativeAccountReadFailure(reason="cancelled"))
    attempt = type(outcome.payload.attempt).model_validate(
        {**outcome.payload.attempt.model_dump(), "family_sequence": 2}
    )
    altered = NativeAccountReadOutcomePayload.model_validate(
        {
            **outcome.payload.model_dump(),
            "attempt": attempt,
            "attempt_content_digest": native_account_read_attempt_digest(attempt),
        }
    )
    async with scenario.repository.session() as session:
        await session.execute(text("DROP TRIGGER native_account_read_outcomes_reject_update"))
        await session.execute(
            text("UPDATE native_account_read_outcomes SET canonical_payload = :value"),
            {"value": canonical_native_account_read_outcome(altered)},
        )
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountReadBindingError, match="outcome_attempt_mismatch"):
            await get_latest_native_account_read(session, scenario.selection, _NOW)


@pytest.mark.parametrize(
    "field,value",
    (
        ("raw_content_digest", "e" * 64),
        ("source_observed_at", _NOW + timedelta(microseconds=1)),
        ("source_recorded_at", _NOW + timedelta(seconds=2, microseconds=1)),
    ),
)
async def test_source_digest_and_original_recording_clocks_are_loaded_from_actual_storage(
    scenario: _Scenario,
    field: str,
    value: object,
) -> None:
    """A correct raw UUID cannot substitute invented source knowledge.

    Given: A real retained response and a changed digest or source clock,
    When: An outcome attempts to bind the substituted reference,
    Then: Actual stored evidence rejects the contradiction.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = await _source(scenario)
    changed = NativeAccountReadCapturedSource.model_validate({**source.model_dump(), field: value})
    with pytest.raises(NativeAccountReadBindingError, match="source_binding_mismatch"):
        await _finish(scenario, 200, changed)


async def test_missing_actual_raw_source_is_not_replaced_by_a_well_formed_reference(
    scenario: _Scenario,
) -> None:
    """A source UUID must resolve to real retained evidence.

    Given: A syntactically valid source reference to an absent receipt,
    When: An actual attempt tries to retain it as its outcome,
    Then: Source binding refuses before any result is recorded.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    source = NativeAccountReadCapturedSource(
        raw_receipt_public_id=_id(800),
        raw_content_digest="e" * 64,
        request_started_at=_NOW,
        source_observed_at=_NOW,
        source_recorded_at=_NOW,
    )
    with pytest.raises(AccountingObservationBindingError, match="receipt_missing"):
        await _finish(scenario, 200, source)


@pytest.mark.parametrize(
    "table,column,value,reason",
    (
        ("native_account_read_attempts", "canonical_payload", "{}", "attempt_payload_invalid"),
        ("native_account_read_attempts", "content_digest", "f" * 64, "attempt_projection_mismatch"),
        ("native_account_read_attempts", "scope_digest", "f" * 64, "scope_projection_mismatch"),
        ("native_account_read_outcomes", "canonical_payload", "{}", "outcome_payload_invalid"),
        ("native_account_read_outcomes", "content_digest", "f" * 64, "outcome_projection_mismatch"),
        ("native_account_read_outcomes", "scope_digest", "f" * 64, "scope_projection_mismatch"),
    ),
)
async def test_restored_rows_with_broken_canonical_or_indexed_material_are_refused_on_read(
    scenario: _Scenario,
    table: str,
    column: str,
    value: str,
    reason: str,
) -> None:
    """Restored corruption is distinct from an ordinary guarded SQL writer.

    Given: A valid history whose update guard is deliberately removed to simulate damaged restore,
    When: An indexed or canonical field is changed and the latest result is read,
    Then: Independent application binding detects the contradiction.
    """
    await begin_native_account_read(scenario.repository, scenario.selection, _identity(200))
    await _finish(scenario, 200, NativeAccountReadFailure(reason="cancelled"))
    async with scenario.repository.session() as session:
        await session.execute(text(f"DROP TRIGGER {table}_reject_update"))
        await session.execute(text(f"UPDATE {table} SET {column} = :value"), {"value": value})
        await session.commit()
    async with scenario.repository.session() as session:
        with pytest.raises(NativeAccountReadBindingError, match=reason):
            await get_latest_native_account_read(session, scenario.selection, _NOW)
