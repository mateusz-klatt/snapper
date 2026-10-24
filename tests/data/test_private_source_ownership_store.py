"""Exercise real raw/hold/sidecar transactions using actual closed signed HTTP and SDK receive.

The shared bridge fixture drives the pinned SDK challenge, encoded subscription,
ACK and physical receive. No test issues a ticket through a private factory or
uses a public context as authentication. All candidates remain off main.
"""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import snapper.data.private_source_ownership_store as ownership_store
import tests.infrastructure.exchanges.test_authenticated_private_socket as socket_fixture
from snapper.application.trade.execution_observation_recovery import _child_identity
from snapper.application.trade.execution_observation_recovery import recoverable_frame_children
from snapper.application.trade.private_source_observation_ingestion import (
    AuthenticatedPrivateObservationSink,
)
from snapper.application.trade.unit_snapshot_route import CapturedExecutionRoute
from snapper.core.contract_accounting_observation import AccountingObservationScope
from snapper.core.contract_accounting_observation import AccountingWriteProvenance
from snapper.core.contract_accounting_observation import RawAccountingObservation
from snapper.core.execution_route_authority import ExecutionClientSelection
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_route_authority import client_selection_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.private_frame_ticket import AuthenticatedPrivateFrameTicket
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.accounting_observation_binding import get_bound_accounting_observation
from snapper.data.contract_accounting_capture_session import StagedAccountingObservationError
from snapper.data.contract_accounting_capture_session import capture_bound_accounting_observation
from snapper.data.contract_accounting_capture_session import require_staged_accounting_observation
from snapper.data.contract_accounting_store import RawAccountingWriteResult
from snapper.data.contract_accounting_store import capture_accounting_observation
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_route_authority_store import insert_execution_route_authority
from snapper.data.models import AccountingRawReceipt
from snapper.data.models import AccountingScopeHold
from snapper.data.models import PrivateSourceOwnershipReceipt
from snapper.data.native_account_identity_store import get_bound_native_account_identity
from snapper.data.private_source_ownership_store import BoundPrivateSourceOwnership
from snapper.data.private_source_ownership_store import PrivateSourceOwnershipBindingError
from snapper.data.private_source_ownership_store import capture_private_source_ownership
from snapper.data.private_source_ownership_store import capture_recovered_private_source_ownership
from snapper.data.private_source_ownership_store import get_bound_private_source_ownership
from snapper.infrastructure.exchanges.contracts import ExecutionObservationContext
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from tests.helpers.native_account_authority import NativeAuthorityClock
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.unit_admission import admission_stamp
from tests.helpers.unit_admission import seed_admission_scenario

_WIRE = b'{"feed":"fills","account":"DemoUser","fills":[{"qty":0.100000000000000001},{"qty":2}]}'


@dataclass(frozen=True)
class _Received:
    """One original actually issued frame plus its later callback observation and actual safe owner."""

    ticket: AuthenticatedPrivateFrameTicket
    observation: RawAccountingObservation
    account_uid: str


@dataclass(frozen=True)
class _Capture:
    """Frozen exact source/identities reused after an uncertain commit or transaction rollback."""

    raw: RawAccountingObservation
    stamp: AccountingWriteProvenance
    identity: ExecutionEvidenceIdentity
    ticket: object


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Allocate stable isolated local identities without selecting a recording clock."""
    return ExecutionEvidenceIdentity(
        public_id=native_authority_uuid(number),
        session_id=native_authority_uuid(9000),
        sequence_id=number,
    )


@asynccontextmanager
async def _received_frame(
    source: NativeAuthorityScenario, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[_Received]:
    """Reuse the real closed SDK bridge; change only the local provider's original financial reply."""
    monkeypatch.setattr(socket_fixture, "_FILL", _WIRE.decode())
    async with socket_fixture._scenario(source, monkeypatch, "valid") as value:
        task = await value.start()
        subscription = asyncio.create_task(value.ws.subscribe(feed="fills"))
        await value.frame_seen.wait()
        value.release_send.set()
        await subscription
        await task
        ticket = value.tickets[0]
        assert ticket is not None and value.proof_seen_at_send == 2
        context = require_authenticated_private_frame(ticket, _WIRE)
        selected = context.client_selection
        async with source.repository.session() as session:
            owner = await get_bound_native_account_identity(session, context.key_proof_public_id)
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
            kind="unknown",
            channel="kraken_futures.ws.fills",
            representation="wire_bytes_base64",
            material=base64.b64encode(_WIRE).decode(),
            observed_at=datetime.now(UTC),
        )
        yield _Received(ticket, raw, owner.payload.observation.account_uid)


def _capture_input(frame: _Received, raw_number: int, pointer: str) -> _Capture:
    """Freeze a real parser-selected child while retaining the complete original wire envelope."""
    children = recoverable_frame_children(frame.observation)
    raw = next(child for child in children if child.target_json_pointer == pointer)
    return _Capture(
        raw,
        AccountingWriteProvenance(
            **_identity(raw_number).model_dump(), timestamp=datetime.now(UTC)
        ),
        _identity(raw_number + 100),
        frame.ticket,
    )


async def _capture(
    source: NativeAuthorityScenario, original: _Capture
) -> tuple[RawAccountingWriteResult, BoundPrivateSourceOwnership]:
    """Own the real caller wallet fence and one commit across raw, hold and issued-ticket sidecar."""
    async with source.repository.session() as session:
        await source.repository._begin_execution_insert_transaction(session)
        await source.repository._acquire_execution_fence_lock(
            session, source.selection.wallet_public_id
        )
        raw = await capture_bound_accounting_observation(session, original.raw, original.stamp)
        assert not raw.conflicted
        sidecar = await capture_private_source_ownership(
            session, raw.public_id, original.ticket, original.identity
        )
        await session.commit()
        return raw, sidecar


async def _counts(source: NativeAuthorityScenario) -> tuple[int, int, int]:
    """Observe real committed source/hold/sidecar counts using an independently opened session."""
    async with source.repository.session() as session:
        raw = await session.scalar(select(func.count()).select_from(AccountingRawReceipt))
        holds = await session.scalar(select(func.count()).select_from(AccountingScopeHold))
        sidecars = await session.scalar(
            select(func.count()).select_from(PrivateSourceOwnershipReceipt)
        )
    assert raw is not None and holds is not None and sidecars is not None
    return raw, holds, sidecars


async def test_actual_receive_children_commit_once_and_replay_the_same_physical_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual authenticated physical frame supports its distinct original children and exact retries.

    Given: A real committed key proof and actual SDK challenge/subscribe/ACK/two-child receive,
    When: Both selected children and an exact repeated first callback commit through the wallet fence,
    Then: Two raw/hold/sidecar facts retain one receive UUID and the original first recording identity.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        baseline = await _counts(source)
        first = _capture_input(frame, 1001, "/fills/0")
        second = _capture_input(frame, 1002, "/fills/1")
        raw, original = await _capture(source, first)
        _, sibling = await _capture(source, second)
        repeated_raw, repeated = await _capture(source, first)
        assert raw.created and not repeated_raw.created
        assert repeated == original and original.account_uid == frame.account_uid
        assert sibling.payload.frame_context == original.payload.frame_context
        assert sibling.payload.raw_receipt_public_id != original.payload.raw_receipt_public_id
        assert original.reference.target_json_pointer == "/fills/0"
        assert sibling.reference.target_json_pointer == "/fills/1"
        assert await _counts(source) == tuple(count + 2 for count in baseline)


async def test_actual_database_failure_rolls_back_all_three_facts_and_exact_retry_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real SQL constraint failure cannot leave raw or hold committed separately from its sidecar.

    Given: One actual issued frame and stable raw/sidecar identities inside a real wallet transaction,
    When: Duplicate physical sidecar insertion fails after all three facts were flushed,
    Then: Closing the failed transaction removes every staged fact and the original exact retry succeeds.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        baseline = await _counts(source)
        original = _capture_input(frame, 1001, "/fills/0")
        with pytest.raises(IntegrityError):
            async with source.repository.session() as session:
                await source.repository._begin_execution_insert_transaction(session)
                await source.repository._acquire_execution_fence_lock(
                    session, source.selection.wallet_public_id
                )
                raw = await capture_bound_accounting_observation(
                    session, original.raw, original.stamp
                )
                await capture_private_source_ownership(
                    session, raw.public_id, original.ticket, original.identity
                )
                await session.execute(
                    text(
                        "INSERT INTO private_source_ownership_receipts SELECT * FROM private_source_ownership_receipts"
                    )
                )
        assert await _counts(source) == baseline
        _, committed = await _capture(source, original)
        assert committed.public_id == original.identity.public_id
        assert await _counts(source) == tuple(count + 1 for count in baseline)


@pytest.mark.parametrize("coordinate", ("session_id", "sequence_id"))
async def test_exact_sidecar_uuid_retry_refuses_changed_insertion_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, coordinate: str
) -> None:
    """Exact sidecar identity cannot silently accept a different original trace coordinate.

    Given: An actual committed raw/hold/sidecar relation from the real issued receive ticket,
    When: The same sidecar UUID is retried with another session or sequence while material is unchanged,
    Then: The retry refuses provenance drift and preserves the complete first retained relation.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        request = _capture_input(frame, 1001, "/fills/0")
        raw, original = await _capture(source, request)
        values = request.identity.model_dump()
        values[coordinate] = native_authority_uuid(9001) if coordinate == "session_id" else 9001
        changed = ExecutionEvidenceIdentity.model_validate(values)
        drifted = _Capture(request.raw, request.stamp, changed, request.ticket)
        with pytest.raises(PrivateSourceOwnershipBindingError, match="insertion_identity_changed"):
            await _capture(source, drifted)
        async with source.repository.session() as session:
            retained = await get_bound_private_source_ownership(
                session, raw.public_id, datetime.now(UTC)
            )
        assert retained == original


async def test_public_ticket_constructor_is_refused_while_actual_raw_and_hold_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A copied public frame context and identical bytes cannot borrow original private issuance.

    Given: A genuine actual ticket plus an unissued public constructor using its same context and bytes,
    When: The real raw/hold transaction attempts sidecar capture with the public lookalike,
    Then: The typed expected refusal leaves the transaction usable for raw/hold commit and no sidecar exists.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        baseline = await _counts(source)
        original = _capture_input(frame, 1001, "/fills/0")
        forged = AuthenticatedPrivateFrameTicket(frame.ticket.context, _WIRE)
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            raw = await capture_bound_accounting_observation(session, original.raw, original.stamp)
            with pytest.raises(PrivateSourceOwnershipBindingError, match="actual_ticket_unproven"):
                await capture_private_source_ownership(
                    session, raw.public_id, forged, original.identity
                )
            await session.commit()
        assert await _counts(source) == (baseline[0] + 1, baseline[1] + 1, baseline[2])
        async with source.repository.session() as session:
            with pytest.raises(PrivateSourceOwnershipBindingError, match="sidecar_missing"):
                await get_bound_private_source_ownership(session, raw.public_id, datetime.now(UTC))


async def test_recovered_sibling_preserves_original_source_and_honest_later_raw_knowledge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recovery keeps one direct actual origin and cannot delegate through a later projection.

    Given: One committed original child proving the complete actual authenticated frame,
    When: A sibling's deterministic raw UUID is captured later and then a projection is offered as origin,
    Then: Original source clocks survive, the new raw clock stays later, and multi-hop ownership is refused.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        _, original = await _capture(source, _capture_input(frame, 1001, "/fills/0"))
        _, sibling_origin = await _capture(source, _capture_input(frame, 1002, "/fills/1"))
        clock = NativeAuthorityClock(original.recorded_at + timedelta(seconds=2))
        monkeypatch.setattr(ownership_store, "datetime", clock)
        child = recoverable_frame_children(frame.observation)[1]
        stamp = AccountingWriteProvenance.model_validate(
            {
                **_identity(1002).model_dump(),
                "public_id": _child_identity(child),
                "timestamp": clock.current,
            }
        )
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            raw = await capture_bound_accounting_observation(session, child, stamp)
            recovered = await capture_recovered_private_source_ownership(
                session, raw.public_id, original.public_id, _identity(1202)
            )
            await session.commit()
        assert recovered.payload.frame_context == original.payload.frame_context
        assert recovered.payload.raw_observed_at == original.payload.raw_observed_at
        assert (
            recovered.payload.raw_recorded_at == stamp.timestamp > original.payload.raw_recorded_at
        )
        assert recovered.payload.origin_sidecar_public_id == original.public_id
        async with source.repository.session() as session:
            bound = await get_bound_private_source_ownership(session, raw.public_id, clock.current)
        assert bound == recovered and bound.account_uid == frame.account_uid
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            repeated = await capture_bound_accounting_observation(session, child, stamp)
            assert not repeated.created
            same_source = await capture_recovered_private_source_ownership(
                session, raw.public_id, sibling_origin.public_id, _identity(1203)
            )
            await session.commit()
        assert same_source == recovered
        assert same_source.payload.origin_sidecar_public_id == original.public_id
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            with pytest.raises(
                PrivateSourceOwnershipBindingError, match="projection_origin_refused"
            ):
                await capture_recovered_private_source_ownership(
                    session, raw.public_id, recovered.public_id, _identity(1204)
                )


async def test_sidecar_capture_requires_the_existing_caller_transaction_before_database_access() -> (
    None
):
    """The sidecar helper cannot silently own a separate commit outside raw-and-hold composition.

    Given: An actual AsyncSession with no transaction, bind or physical database connection,
    When: An original sidecar write is requested,
    Then: The transaction precondition fails before source lookup or any implicit database work.
    """
    async with AsyncSession() as session:
        with pytest.raises(RuntimeError, match="caller wallet-fenced transaction"):
            await capture_private_source_ownership(
                session, native_authority_uuid(1001), object(), _identity(1101)
            )


@pytest.mark.parametrize("restage", (False, True))
async def test_an_old_unowned_raw_cannot_gain_ownership_in_a_later_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restage: bool
) -> None:
    """Even a genuine original ticket cannot append ownership after a generic Stage-A commit.

    Given: A genuine actual ticket whose raw and hold already committed without any sidecar,
    When: A later wallet transaction tries the original ticket, with or without recapturing the old raw,
    Then: Missing transaction registration or created=False refuses promotion and the raw stays held.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        original = _capture_input(frame, 1001, "/fills/0")
        raw = await capture_accounting_observation(source.repository, original.raw, original.stamp)
        baseline = await _counts(source)
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            if restage:
                repeated = await capture_bound_accounting_observation(
                    session, original.raw, original.stamp
                )
                assert repeated.public_id == raw.public_id and not repeated.created
            reason = (
                "preexisting_unowned_raw_refused"
                if restage
                else "same_capture_transaction_required"
            )
            with pytest.raises(PrivateSourceOwnershipBindingError, match=reason):
                await capture_private_source_ownership(
                    session, raw.public_id, original.ticket, original.identity
                )
            await session.commit()
        assert await _counts(source) == baseline
        async with source.repository.session() as session:
            with pytest.raises(PrivateSourceOwnershipBindingError, match="sidecar_missing"):
                await get_bound_private_source_ownership(session, raw.public_id, datetime.now(UTC))


async def test_unowned_recovered_child_cannot_be_promoted_after_its_separate_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An existing held recovery child cannot acquire ownership through a later original-row lookup.

    Given: A valid original actual_receive and a sibling raw committed separately without ownership,
    When: A later transaction reuses that child and explicitly names the real original sidecar,
    Then: Correct frame and origin proofs do not override the new same-transaction creation requirement.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        _, origin = await _capture(source, _capture_input(frame, 1001, "/fills/0"))
        child = recoverable_frame_children(frame.observation)[1]
        stamp = AccountingWriteProvenance.model_validate(
            {
                **_identity(1002).model_dump(),
                "public_id": _child_identity(child),
                "timestamp": datetime.now(UTC),
            }
        )
        raw = await capture_accounting_observation(source.repository, child, stamp)
        baseline = await _counts(source)
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            repeated = await capture_bound_accounting_observation(session, child, stamp)
            assert repeated.public_id == raw.public_id and not repeated.created
            with pytest.raises(
                PrivateSourceOwnershipBindingError, match="preexisting_unowned_raw_refused"
            ):
                await capture_recovered_private_source_ownership(
                    session, raw.public_id, origin.public_id, _identity(1102)
                )
            await session.commit()
        assert await _counts(source) == baseline


async def test_nested_capture_refuses_before_registration_and_root_capture_remains_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A SAVEPOINT cannot leave a stale created=True registration after nested rollback.

    Given: A genuine received frame and the actual caller wallet-fenced root transaction,
    When: Capture is requested inside a SAVEPOINT and then correctly from the root scope,
    Then: Nested capture refuses before writing or registering and the root commits all three facts.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        baseline = await _counts(source)
        request = _capture_input(frame, 1001, "/fills/0")
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            async with session.begin_nested():
                with pytest.raises(StagedAccountingObservationError, match="without a SAVEPOINT"):
                    await capture_bound_accounting_observation(session, request.raw, request.stamp)
            absent = await session.scalar(
                select(func.count())
                .select_from(AccountingRawReceipt)
                .where(AccountingRawReceipt.public_id == request.stamp.public_id)
            )
            assert absent == 0
            raw = await capture_bound_accounting_observation(session, request.raw, request.stamp)
            bound = await capture_private_source_ownership(
                session, raw.public_id, request.ticket, request.identity
            )
            await session.commit()
        assert bound.public_id == request.identity.public_id
        assert await _counts(source) == tuple(count + 1 for count in baseline)


async def _actual_nominal_provider_route(source: NativeAuthorityScenario) -> CapturedExecutionRoute:
    """Load a real different-client historical route without presenting its generic balance as native ownership."""
    legacy = await seed_admission_scenario(
        source.repository, source.clock.current, source="kraken_futures"
    )
    result = await insert_execution_route_authority(
        source.repository, legacy.authority, admission_stamp(93, source.clock.current)
    )
    async with source.repository.session() as session:
        actual = await get_bound_route_authority(session, result.public_id)
    assert actual == legacy.authority
    return CapturedExecutionRoute(result, actual, legacy.route)


def _private_source(raw: RawAccountingObservation) -> PrivateExecutionObservation:
    """Keep exact genuine original wire and its actual parser-selected child for the closed adapter."""
    return PrivateExecutionObservation(
        context=ExecutionObservationContext(
            kind=raw.kind,
            channel=raw.channel,
            observed_at=raw.observed_at,
            target_json_pointer=raw.target_json_pointer,
            source_identity=raw.source_identity,
        ),
        representation=raw.representation,
        material=raw.material,
    )


def _mutate_provider_views(
    selected: ExecutionClientSelection,
    authority: CapturedExecutionRoute,
    original: ExecutionClientSelection,
) -> None:
    """Change only exposed provider objects after frame binding, leaving actual DB and issued source facts untouched."""
    object.__setattr__(selected, "wallet_public_id", native_authority_uuid(9901))
    scope = authority.payload.scope.model_copy(
        update={
            "wallet_public_id": original.wallet_public_id,
            "exchange": original.exchange,
            "mode": original.mode,
            "environment": original.environment,
        }
    )
    object.__setattr__(authority.payload, "scope", scope)
    object.__setattr__(authority.payload, "credential_public_id", original.credential_public_id)
    object.__setattr__(authority.payload, "credential_timestamp", original.credential_timestamp)
    object.__setattr__(authority.payload, "public_api_origin", original.public_api_origin)


@pytest.mark.parametrize("issued", [False, True])
async def test_bound_frame_cannot_be_relabelled_by_later_public_provider_model_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, issued: bool
) -> None:
    """Frame siblings retain their synchronous original scope and nominal route decision across provider mutation.

    Given: Actual closed signed HTTP and SDK receive plus a real different-client historical provider route.
    When: The frame is bound, then only public provider model views are mutated before child storage.
    Then: Its original scope and absent nominal route remain; only the actually issued ticket supplies ownership.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        original = require_authenticated_private_frame(frame.ticket, _WIRE).client_selection
        public_selection = ExecutionClientSelection.model_validate(
            original.model_dump(mode="python")
        )
        public_route = await _actual_nominal_provider_route(source)
        assert client_selection_from_authority(public_route.payload) != original
        adapter = AuthenticatedPrivateObservationSink(
            source.repository, lambda: public_selection, lambda: public_route, source.tracker
        )
        bound = adapter.bind_frame(frame.ticket if issued else None, _WIRE)
        _mutate_provider_views(public_selection, public_route, original)
        assert public_selection.wallet_public_id != original.wallet_public_id
        assert client_selection_from_authority(public_route.payload) == original
        child = _capture_input(frame, 1001, "/fills/0").raw
        raw_id = await bound(_private_source(child))
        async with source.repository.session() as session:
            actual = await get_bound_accounting_observation(session, raw_id)
            assert actual.scope.wallet_public_id == original.wallet_public_id
            assert actual.route_revision == client_selection_digest(original)
            assert actual.credential_public_id == original.credential_public_id
            assert actual.route_authority_public_id is None
            sidecars = await session.scalar(
                select(func.count())
                .select_from(PrivateSourceOwnershipReceipt)
                .where(PrivateSourceOwnershipReceipt.raw_receipt_public_id == raw_id)
            )
            assert sidecars == int(issued)
            if issued:
                ownership = await get_bound_private_source_ownership(
                    session, raw_id, datetime.now(UTC)
                )
                assert ownership.account_uid == frame.account_uid


@pytest.mark.parametrize("public_view", ["capture_return", "require_return"])
async def test_public_restage_result_mutation_cannot_promote_an_old_unowned_raw(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, public_view: str
) -> None:
    """Public dataclass views cannot alter the private same-transaction new-raw evidence bit.

    Given: An actual original issued ticket whose genuine raw and hold already committed unowned.
    When: A later real restage returns createdFalse and only its public result view is mutated to True.
    Then: Actual ownership insertion still refuses old raw and no sidecar or financial fact is invented.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _received_frame(source, monkeypatch) as frame,
    ):
        original = _capture_input(frame, 1001, "/fills/0")
        raw = await capture_accounting_observation(source.repository, original.raw, original.stamp)
        baseline = await _counts(source)
        async with source.repository.session() as session:
            await source.repository._begin_execution_insert_transaction(session)
            await source.repository._acquire_execution_fence_lock(
                session, source.selection.wallet_public_id
            )
            repeated = await capture_bound_accounting_observation(
                session, original.raw, original.stamp
            )
            assert repeated.public_id == raw.public_id and not repeated.created
            exposed = (
                repeated
                if public_view == "capture_return"
                else require_staged_accounting_observation(session, raw.public_id)
            )
            assert not exposed.created
            object.__setattr__(exposed, "created", True)
            assert exposed.created
            with pytest.raises(
                PrivateSourceOwnershipBindingError, match="preexisting_unowned_raw_refused"
            ):
                await capture_private_source_ownership(
                    session, raw.public_id, original.ticket, original.identity
                )
            await session.commit()
        assert await _counts(source) == baseline
        async with source.repository.session() as session:
            with pytest.raises(PrivateSourceOwnershipBindingError, match="sidecar_missing"):
                await get_bound_private_source_ownership(session, raw.public_id, datetime.now(UTC))
