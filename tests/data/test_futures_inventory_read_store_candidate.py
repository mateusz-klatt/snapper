"""Exercise isolated inventory persistence with actual SDK requests and real SQLite.

The local adapter supplies controlled original HTTP entities and independently
observes committed requests without contacting a venue. Separate migrated
SQLite/PostgreSQL tests cover the physical migration chain. These observations
grant no financial authority, recovery capability or order access.
"""

import base64
import json
import sqlite3
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import cast

import pytest
from requests import PreparedRequest
from requests import Response
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import set_committed_value
from urllib3.response import HTTPResponse

import snapper.application.trade.current_futures_inventory as current_reader
import snapper.data.futures_inventory_read_store as store
import snapper.infrastructure.exchanges.futures_inventory_wire as wire
from snapper.application.trade.current_futures_inventory import CurrentFuturesInventoryRead
from snapper.application.trade.current_futures_inventory import FuturesInventoryFreshnessPolicy
from snapper.application.trade.current_futures_inventory import read_current_futures_inventory
from snapper.application.trade.native_private_owner_preparation import PreparedPrivateOwnerRead
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.futures_inventory_read import FuturesInventoryReadAttempt
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import FuturesInventoryReadOutcome
from snapper.core.futures_inventory_read import FuturesInventoryReadSource
from snapper.core.futures_inventory_read import InventoryReadKeyReference
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.core.futures_inventory_read import futures_inventory_attempt_digest
from snapper.core.futures_inventory_read import futures_inventory_read_family
from snapper.data.futures_inventory_capture import FuturesInventoryWriteResult
from snapper.data.futures_inventory_key_binding import FuturesInventoryBindingError
from snapper.data.futures_inventory_key_binding import inventory_read_key_reference
from snapper.data.futures_inventory_read_projection import inventory_read_attempt_projection
from snapper.data.futures_inventory_read_projection import inventory_read_outcome_projection
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryAttempt
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryOutcome
from snapper.data.futures_inventory_read_store import _append_futures_inventory_outcome
from snapper.data.futures_inventory_read_store import begin_futures_inventory_read
from snapper.data.futures_inventory_read_store import get_bound_futures_inventory_attempt
from snapper.data.futures_inventory_read_store import get_bound_futures_inventory_outcome
from snapper.data.futures_inventory_read_store import get_latest_futures_inventory_read
from snapper.data.models import FuturesInventoryReadAttempt as AttemptRow
from snapper.data.models import FuturesInventoryReadOutcome as OutcomeRow
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.infrastructure.exchanges.futures_inventory_wire import GuardedFuturesInventoryReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import read_futures_inventory_wire
from snapper.infrastructure.network.egress_context import egress_identity
from tests.helpers.native_account_authority import NativeAuthorityClock
from tests.helpers.native_account_authority import NativeAuthorityHttpAdapter
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid
from tests.helpers.native_account_authority import rotate_native_credential

_ORDERS: InventoryReadPurpose = "kraken_futures.open_orders"
_POSITIONS: InventoryReadPurpose = "kraken_futures.open_positions"
_PATH_PURPOSES: dict[str, InventoryReadPurpose] = {
    "/derivatives/api/v3/openorders": _ORDERS,
    "/derivatives/api/v3/openpositions": _POSITIONS,
}


def _identity(number: int) -> ExecutionEvidenceIdentity:
    """Allocate stable producer provenance without generating replacement retry identities."""
    return ExecutionEvidenceIdentity(
        public_id=native_authority_uuid(number),
        session_id=native_authority_uuid(900),
        sequence_id=number,
    )


class _InventoryAdapter(NativeAuthorityHttpAdapter):
    """Observe committed inventory evidence at the actual final SDK Requests adapter boundary."""

    def __init__(self, path: Path, clock: NativeAuthorityClock) -> None:
        """Extend the existing isolated key/accounts adapter with two fixed inventory responses."""
        super().__init__(path, clock)
        self.expected_attempt_id: str | None = None
        self.inventory_calls: list[PreparedRequest] = []
        self.visible_attempt_ids: list[str] = []

    def _observe_attempt(self, purpose: InventoryReadPurpose) -> None:
        """Read the actual committed attempt and matching original proof from another connection."""
        with closing(sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)) as connection:
            row = connection.execute(
                "SELECT attempt.public_id, attempt.purpose, attempt.key_proof_content_digest "
                "FROM futures_inventory_read_attempts AS attempt "
                "JOIN native_account_identity_proofs AS proof "
                "ON proof.public_id = attempt.key_proof_public_id "
                "AND proof.content_digest = attempt.key_proof_content_digest "
                "WHERE attempt.public_id = ?",
                (self.expected_attempt_id,),
            ).fetchone()
        assert row is not None
        assert row[0] == self.expected_attempt_id and row[1] == purpose
        assert isinstance(row[2], str) and len(row[2]) == 64
        self.visible_attempt_ids.append(str(row[0]))

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Return controlled original HTTP bytes after independently observing the committed attempt.

        Args:
            request: Actual final Requests object signed by the fixture's original SDK User.
            args: Original adapter positional arguments.
            kwargs: Original adapter stream, timeout and proxy arguments.

        Returns:
            Genuine streaming Response backed by local bytes, with no external network operation.
        """
        path = (request.url or "").removeprefix("https://futures.kraken.com")
        purpose = _PATH_PURPOSES.get(path)
        if purpose is None:
            return super().send(request, *args, **kwargs)
        self._observe_attempt(purpose)
        self.calls.append(request)
        self.inventory_calls.append(request)
        name = "openOrders" if purpose == _ORDERS else "openPositions"
        body = json.dumps(
            {
                "result": "success",
                "serverTime": self.clock.current.isoformat().replace("+00:00", "Z"),
                name: [],
            },
            separators=(",", ":"),
        ).encode()
        response = Response()
        response.status_code = 200
        response.url = request.url or ""
        response.request = request
        response.raw = HTTPResponse(body=BytesIO(body), status=200, preload_content=False)
        self.responses.append(response)
        return response


@dataclass
class _Scenario:
    """Actual isolated repository, original prepared token and committed original owner reference."""

    native: NativeAuthorityScenario
    adapter: _InventoryAdapter
    prepared: PreparedPrivateOwnerRead
    reference: InventoryReadKeyReference


@asynccontextmanager
async def _scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Scenario]:
    """Prepare genuine key/accounts evidence before exercising the future inventory store."""
    async with native_authority_scenario(tmp_path, monkeypatch) as native:
        adapter = _InventoryAdapter(native.adapter.path, native.clock)
        native.adapter = adapter
        for session in native.sessions:
            session.mount("https://", adapter)
        monkeypatch.setattr(store, "datetime", native.clock)
        monkeypatch.setattr(wire, "datetime", native.clock)
        prepared = await prepare_native_private_owner(
            native.repository, native.client, native.tracker
        )
        try:
            async with native.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
                assert proof is not None
                assert proof.payload.observation.general_access == "READ_ONLY"
                reference = inventory_read_key_reference(proof)
            yield _Scenario(native, adapter, prepared, reference)
        finally:
            prepared.close()
            adapter.close()


async def _begin(
    scenario: _Scenario, purpose: InventoryReadPurpose, number: int
) -> BoundFuturesInventoryAttempt:
    """Commit the genuine independent request before any inventory HTTP is permitted."""
    return await begin_futures_inventory_read(
        scenario.native.repository, scenario.reference, purpose, _identity(number)
    )


def _issue(
    scenario: _Scenario, attempt: BoundFuturesInventoryAttempt
) -> GuardedFuturesInventoryReceipt:
    """Cross the actual SDK signing and guarded Requests source without fabricating a receipt."""
    user = scenario.native.client._user_client
    assert user is not None
    scenario.adapter.expected_attempt_id = attempt.public_id
    with egress_identity(
        exchange="kraken_futures", traffic_class="private", owner="test", operation="inventory"
    ):
        return read_futures_inventory_wire(
            user,
            scenario.native.selection,
            scenario.prepared.require_token(),
            attempt.public_id,
            attempt.payload.purpose,
        )


async def _append(
    scenario: _Scenario,
    attempt: BoundFuturesInventoryAttempt,
    result: FuturesInventoryWriteResult,
    identity: ExecutionEvidenceIdentity,
) -> BoundFuturesInventoryOutcome:
    """Commit one original terminal result in the actual repository wallet-fenced transaction."""
    repository = scenario.native.repository
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(
            session, scenario.native.selection.wallet_public_id
        )
        outcome = await _append_futures_inventory_outcome(
            session, attempt.public_id, result, identity
        )
        await session.commit()
        return outcome


async def _capture(
    scenario: _Scenario, purpose: InventoryReadPurpose, number: int
) -> tuple[BoundFuturesInventoryAttempt, BoundFuturesInventoryOutcome]:
    """Use a genuine committed request and actual issued source for the first successful outcome."""
    attempt = await _begin(scenario, purpose, number)
    outcome = await _append(scenario, attempt, _issue(scenario, attempt), _identity(number + 1))
    assert isinstance(outcome.payload.result, FuturesInventoryReadSource)
    return attempt, outcome


async def test_endpoint_ordinals_are_independent_and_http_sees_committed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep orders and positions independent while retaining actual READ_ONLY owner provenance.

    Given: Actual READ_ONLY key/accounts preparation committed the original public proof.
    When: Real inventory HTTP observes orders, positions and a second orders request.
    Then: Each endpoint starts at ordinal one and every physical send sees its committed request.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        async with scenario.native.repository.session() as session:
            assert (
                await get_latest_futures_inventory_read(
                    session, scenario.native.selection, _ORDERS, scenario.native.clock.current
                )
                is None
            )
        orders, _ = await _capture(scenario, _ORDERS, 100)
        positions, _ = await _capture(scenario, _POSITIONS, 110)
        later = await _begin(scenario, _ORDERS, 120)
        assert (orders.payload.family_sequence, positions.payload.family_sequence) == (1, 1)
        assert later.payload.family_sequence == 2
        assert scenario.adapter.visible_attempt_ids == [orders.public_id, positions.public_id]
        assert len(scenario.adapter.inventory_calls) == 2
        async with scenario.native.repository.session() as session:
            rows = (await session.scalars(select(AttemptRow))).all()
            for row in rows:
                assert row.margin_account is None
                assert row.scope_digest == futures_inventory_read_family(
                    scenario.native.selection, cast(InventoryReadPurpose, row.purpose)
                )
            scopes = {row.purpose: row.scope_digest for row in rows}
            assert scopes[_ORDERS] != scopes[_POSITIONS]


async def test_unknown_endpoint_cannot_allocate_or_query_an_inventory_family(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse unsupported public endpoint coordinates before retaining a request or selecting a book.

    Given: Genuine READ_ONLY owner preparation exists without any inventory attempt.
    When: A caller requests another endpoint through the begin or latest-read boundary.
    Then: Both operations refuse the purpose and no request or inventory HTTP is created.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        invalid = cast(InventoryReadPurpose, "kraken_futures.accounts")
        with pytest.raises(FuturesInventoryBindingError, match="purpose_invalid"):
            await _begin(scenario, invalid, 100)
        async with scenario.native.repository.session() as session:
            with pytest.raises(FuturesInventoryBindingError, match="purpose_invalid"):
                await get_latest_futures_inventory_read(
                    session, scenario.native.selection, invalid, scenario.native.clock.current
                )
            assert await session.scalar(select(func.count()).select_from(AttemptRow)) == 0
        assert not scenario.adapter.inventory_calls


@pytest.mark.parametrize("kind", ("attempt", "outcome"))
@pytest.mark.parametrize("field", ("session_id", "sequence_id"))
async def test_loaded_inventory_insertion_provenance_cannot_bypass_strict_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, field: str
) -> None:
    """Refuse corrupted loaded provenance without changing immutable SQL or weakening its guards.

    Given: Actual SDK HTTP and real isolated SQL retained one original request and outcome.
    When: A caller replaces a loaded session UUID or positive sequence with an invalid value.
    Then: Historical loading refuses that copy and both independently loaded original rows stay valid.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        attempt, outcome = await _capture(scenario, _ORDERS, 100)
        async with scenario.native.repository.session() as session:
            row: AttemptRow | OutcomeRow | None
            if kind == "attempt":
                row = await session.scalar(select(AttemptRow))
            else:
                row = await session.scalar(select(OutcomeRow))
            assert row is not None
            original = getattr(row, field)
            set_committed_value(row, field, "invalid-session" if field == "session_id" else 0)
            try:
                with pytest.raises(
                    FuturesInventoryBindingError, match="insertion_identity_invalid"
                ):
                    if kind == "attempt":
                        await get_bound_futures_inventory_attempt(session, attempt.public_id)
                    else:
                        await get_bound_futures_inventory_outcome(session, attempt.public_id)
            finally:
                set_committed_value(row, field, original)
            assert not session.dirty
        async with scenario.native.repository.session() as session:
            assert await get_bound_futures_inventory_attempt(session, attempt.public_id) == attempt
            assert await get_bound_futures_inventory_outcome(session, attempt.public_id) == outcome


async def test_exact_retries_keep_original_clocks_provenance_and_http_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retry genuine committed evidence without inventing fresh source age or an extra request.

    Given: One actual guarded inventory read produced a committed successful outcome.
    When: The same request and outcome identities are retried after the local clock advances.
    Then: Original payloads, insertion provenance and row counts remain unchanged without HTTP.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        attempt, outcome = await _capture(scenario, _ORDERS, 100)
        scenario.native.clock.current += timedelta(seconds=5)
        assert await _begin(scenario, _ORDERS, 100) == attempt
        assert await _append(scenario, attempt, outcome.payload.result, _identity(101)) == outcome
        async with scenario.native.repository.session() as session:
            assert await session.scalar(select(func.count()).select_from(AttemptRow)) == 1
            assert await session.scalar(select(func.count()).select_from(OutcomeRow)) == 1
            row = await session.scalar(select(OutcomeRow))
            assert row is not None
            assert (row.public_id, row.session_id, row.sequence_id) == (
                _identity(101).public_id,
                _identity(101).session_id,
                101,
            )
        assert len(scenario.adapter.inventory_calls) == 1


@pytest.mark.parametrize("kind", ("attempt", "outcome"))
@pytest.mark.parametrize("changed", ("session_id", "sequence_id"))
async def test_retry_rejects_changed_insertion_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str, changed: str
) -> None:
    """Preserve the original producer identity even when a caller retains the public UUID.

    Given: Genuine inventory request and outcome rows have committed insertion provenance.
    When: A retry changes only its session or sequence coordinate.
    Then: The actual store refuses instead of accepting or rewriting the immutable original row.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        attempt, outcome = await _capture(scenario, _ORDERS, 100)
        original = _identity(100 if kind == "attempt" else 101)
        value = original.model_dump(mode="python")
        value[changed] = native_authority_uuid(901) if changed == "session_id" else 999
        identity = ExecutionEvidenceIdentity.model_validate(value)
        with pytest.raises(FuturesInventoryBindingError, match="insertion_identity_changed"):
            if kind == "attempt":
                await begin_futures_inventory_read(
                    scenario.native.repository, scenario.reference, _ORDERS, identity
                )
            else:
                await _append(scenario, attempt, outcome.payload.result, identity)
        assert len(scenario.adapter.inventory_calls) == 1


async def test_attempt_uuid_cannot_be_repurposed_or_outcome_identity_reused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep endpoint identity separate even when both reads share the actual same owner proof.

    Given: A real orders request completed and a distinct positions request is pending.
    When: The original attempt is repurposed or its outcome UUID is reused for the other endpoint.
    Then: Both writes refuse and positions remains honestly pending.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        _, outcome = await _capture(scenario, _ORDERS, 100)
        with pytest.raises(FuturesInventoryBindingError, match="attempt_identity_changed"):
            await _begin(scenario, _POSITIONS, 100)
        pending = await _begin(scenario, _POSITIONS, 110)
        with pytest.raises(FuturesInventoryBindingError, match="outcome_identity_reused"):
            await _append(scenario, pending, outcome.payload.result, _identity(101))
        async with scenario.native.repository.session() as session:
            assert await get_bound_futures_inventory_outcome(session, pending.public_id) is None


async def test_public_success_and_another_attempts_actual_receipt_cannot_create_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A valid-looking public source is never a replacement for this request's actual issued transport.

    Given: Actual source HTTP belongs to one committed request and a later request is pending.
    When: The earlier issued receipt or a public reconstructed success is offered to the newer row.
    Then: No newer success is inserted and the original request still has no fabricated outcome.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        original = await _begin(scenario, _ORDERS, 100)
        issued = _issue(scenario, original)
        pending = await _begin(scenario, _ORDERS, 110)
        public = wire.require_guarded_inventory_receipt(issued)
        invented = FuturesInventoryReadSource(
            parser_policy="kraken_futures_open_orders_v1",
            body=base64.b64encode(public.entity_bytes).decode("ascii"),
            request_started_at=public.request_started_at,
            observed_at=public.observed_at,
        )
        for result in (issued, invented):
            with pytest.raises(FuturesInventoryBindingError):
                await _append(scenario, pending, result, _identity(111))
        async with scenario.native.repository.session() as session:
            assert await get_bound_futures_inventory_outcome(session, pending.public_id) is None
            assert await get_bound_futures_inventory_outcome(session, original.public_id) is None
        assert len(scenario.adapter.inventory_calls) == 1


@pytest.mark.parametrize("terminal", (False, True))
async def test_newest_pending_or_failed_request_masks_prior_success_and_late_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, terminal: bool
) -> None:
    """Latest means newest original request, never whichever successful outcome arrived last.

    Given: A successful first request, another unresolved request and a third newest request exist.
    When: The newest request remains pending or fails and the middle request completes late.
    Then: Latest returns the third request and its actual pending or failure disposition.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        await _capture(scenario, _ORDERS, 100)
        middle = await _begin(scenario, _ORDERS, 110)
        late = _issue(scenario, middle)
        newest = await _begin(scenario, _ORDERS, 120)
        expected: BoundFuturesInventoryOutcome | None = None
        if terminal:
            expected = await _append(
                scenario,
                newest,
                FuturesInventoryReadFailure(reason="transport_error"),
                _identity(121),
            )
        scenario.native.clock.current += timedelta(seconds=1)
        await _append(scenario, middle, late, _identity(111))
        async with scenario.native.repository.session() as session:
            latest = await get_latest_futures_inventory_read(
                session, scenario.native.selection, _ORDERS, scenario.native.clock.current
            )
        assert latest is not None and latest.attempt == newest and latest.outcome == expected


async def test_credential_rotation_does_not_restart_or_hide_endpoint_ordering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A current credential cannot silently fall back to a successful prior credential observation.

    Given: Actual inventory success was captured under the original credential revision.
    When: Real credential rotation occurs and another genuine owner preparation commits.
    Then: Old source refuses and the next endpoint request continues at ordinal two, still pending.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        await _capture(scenario, _ORDERS, 100)
        await rotate_native_credential(scenario.native)
        async with scenario.native.repository.session() as session:
            with pytest.raises(FuturesInventoryBindingError, match="latest_client_changed"):
                await get_latest_futures_inventory_read(
                    session, scenario.native.selection, _ORDERS, scenario.native.clock.current
                )
        native = scenario.native
        prepared = await prepare_native_private_owner(
            native.repository, native.client, native.tracker
        )
        try:
            async with native.repository.session() as session:
                proof = await find_native_account_identity(
                    session, prepared.outcome.payload.attempt_public_id
                )
                assert proof is not None
                reference = inventory_read_key_reference(proof)
            pending = await begin_futures_inventory_read(
                native.repository, reference, _ORDERS, _identity(110)
            )
            assert pending.payload.family_sequence == 2
            async with native.repository.session() as session:
                latest = await get_latest_futures_inventory_read(
                    session, native.selection, _ORDERS, native.clock.current
                )
            assert latest is not None and latest.attempt == pending and latest.outcome is None
        finally:
            prepared.close()


@pytest.mark.parametrize("boundary", ("attempt", "outcome"))
async def test_newest_future_clock_cannot_be_filtered_out_to_reveal_older_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    """An unknown future request remains a barrier instead of making an older result look current.

    Given: A genuine newer request commits after an earlier successful inventory read.
    When: The reader's knowledge clock precedes the newer request or its later completion.
    Then: It refuses that unknown evidence without returning the older successful source.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        await _capture(scenario, _ORDERS, 100)
        earlier = scenario.native.clock.current
        reason = "attempt_not_yet_known"
        if boundary == "attempt":
            scenario.native.clock.current += timedelta(seconds=1)
            await _begin(scenario, _ORDERS, 110)
        else:
            pending = await _begin(scenario, _ORDERS, 110)
            issued = _issue(scenario, pending)
            scenario.native.clock.current += timedelta(seconds=1)
            await _append(scenario, pending, issued, _identity(111))
            reason = "outcome_not_yet_known"
        async with scenario.native.repository.session() as session:
            with pytest.raises(FuturesInventoryBindingError, match=reason):
                await get_latest_futures_inventory_read(
                    session, scenario.native.selection, _ORDERS, earlier
                )


async def test_corrupt_newest_scope_copy_remains_visible_to_direct_family_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Inspect a corrupt latest indexed digest instead of filtering it out before verification.

    Given: An isolated INSERT keeps real proof/canonical evidence but a wrong indexed scope copy.
    When: Latest selects the actual wallet/environment/purpose family.
    Then: Projection verification refuses that newest row rather than returning older success.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        await _capture(scenario, _ORDERS, 100)
        payload = FuturesInventoryReadAttempt(
            purpose=_ORDERS,
            client_selection=scenario.native.selection,
            key_proof=scenario.reference,
            family_sequence=2,
            started_at=scenario.native.clock.current,
        )
        projection = inventory_read_attempt_projection(payload) | {"scope_digest": "0" * 64}
        async with scenario.native.repository.session() as session:
            session.add(AttemptRow(**_identity(110).model_dump(mode="python"), **projection))
            await session.commit()
        async with scenario.native.repository.session() as session:
            with pytest.raises(FuturesInventoryBindingError, match="projection_mismatch"):
                await get_latest_futures_inventory_read(
                    session, scenario.native.selection, _ORDERS, scenario.native.clock.current
                )


async def test_historical_success_is_semantically_parsed_before_its_hash_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Original bytes must be semantically valid before a historical body's hash is trusted.

    Given: An isolated INSERT contains a valid envelope with an incomplete original orders row.
    When: Historical loading encounters that source and the projection callback is instrumented.
    Then: Strict original-source parsing refuses before that outcome hash/projection callback runs.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        attempt = await _begin(scenario, _ORDERS, 100)
        body = b'{"result":"success","serverTime":"2026-10-23T15:00:00Z","openOrders":[{}]}'
        payload = FuturesInventoryReadOutcome(
            attempt_public_id=attempt.public_id,
            attempt_content_digest=futures_inventory_attempt_digest(attempt.payload),
            attempt=attempt.payload,
            completed_at=scenario.native.clock.current,
            result=FuturesInventoryReadSource(
                parser_policy="kraken_futures_open_orders_v1",
                body=base64.b64encode(body).decode("ascii"),
                request_started_at=scenario.native.clock.current,
                observed_at=scenario.native.clock.current,
            ),
        )
        async with scenario.native.repository.session() as session:
            session.add(
                OutcomeRow(
                    **_identity(101).model_dump(mode="python"),
                    **inventory_read_outcome_projection(payload),
                )
            )
            await session.commit()
        called: list[bool] = []

        def unexpected_projection(value: FuturesInventoryReadOutcome) -> dict[str, object]:
            """Observe ordering without replacing the actual semantic source parser or immutable rows."""
            called.append(True)
            raise AssertionError(
                "Historical outcome hashing preceded strict original-source parsing"
            )

        monkeypatch.setattr(store, "inventory_read_outcome_projection", unexpected_projection)
        async with scenario.native.repository.session() as session:
            with pytest.raises(ValueError):
                await get_bound_futures_inventory_outcome(session, attempt.public_id)
        assert not called


async def test_uncommitted_outcome_rolls_back_and_missing_attempt_remains_explicit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The private append helper neither commits its caller's transaction nor invents missing evidence.

    Given: An actual pending request and its genuine issued response exist.
    When: A caller appends then rolls back, or asks historical readers for a missing request.
    Then: The original request stays pending and missing identities are refused explicitly.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        attempt = await _begin(scenario, _ORDERS, 100)
        received = _issue(scenario, attempt)
        repository = scenario.native.repository
        async with repository.session() as session:
            await repository._begin_execution_insert_transaction(session)
            await repository._acquire_execution_fence_lock(
                session, scenario.native.selection.wallet_public_id
            )
            await _append_futures_inventory_outcome(
                session, attempt.public_id, received, _identity(101)
            )
            await session.rollback()
        async with repository.session() as session:
            assert await get_bound_futures_inventory_outcome(session, attempt.public_id) is None
            with pytest.raises(FuturesInventoryBindingError, match="attempt_missing"):
                await get_bound_futures_inventory_attempt(session, native_authority_uuid(999))
            with pytest.raises(FuturesInventoryBindingError, match="attempt_missing"):
                await get_bound_futures_inventory_outcome(session, native_authority_uuid(999))


async def test_every_indexed_copy_and_open_temporal_marker_is_checked_on_historical_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify corruption in loaded indexed copies without issuing any SQL UPDATE or bypassing DDL guards.

    Given: Genuine source produced one committed attempt and outcome in an isolated database.
    When: Each loaded projection copy or open temporal marker is independently changed in memory.
    Then: Historical loading refuses every mismatch instead of trusting only the canonical payload.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        attempt, outcome = await _capture(scenario, _ORDERS, 100)
        async with scenario.native.repository.session() as session:
            row = await session.scalar(select(AttemptRow))
            assert row is not None
            expected = inventory_read_attempt_projection(attempt.payload) | {
                "known_to": row.known_to
            }
            for name, original in expected.items():
                set_committed_value(row, name, _changed_projection(original))
                try:
                    with pytest.raises(ValueError):
                        await get_bound_futures_inventory_attempt(session, attempt.public_id)
                finally:
                    set_committed_value(row, name, original)
            result_row = await session.scalar(select(OutcomeRow))
            assert result_row is not None
            result_expected = inventory_read_outcome_projection(outcome.payload) | {
                "known_to": result_row.known_to
            }
            for name, original in result_expected.items():
                set_committed_value(result_row, name, _changed_projection(original))
                try:
                    with pytest.raises(ValueError):
                        await get_bound_futures_inventory_outcome(session, attempt.public_id)
                finally:
                    set_committed_value(result_row, name, original)


def _changed_projection(value: object) -> object:
    """Change a loaded copy while preserving the basic type needed for the real loader to inspect it."""
    if isinstance(value, datetime):
        return value - timedelta(microseconds=1)
    if isinstance(value, str):
        return value + "changed"
    if isinstance(value, int):
        return value + 1
    return "unexpected-margin"


async def test_lost_commit_acknowledgement_preserves_one_original_attempt_on_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual committed request survives a lost acknowledgement without reallocating its ordinal.

    Given: The original isolated commit succeeds before its acknowledgement is lost once.
    When: The caller retries the exact original request identity after that transport error.
    Then: Only one actual row exists and its original start clock and family ordinal are preserved.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        actual_commit = AsyncSession.commit
        armed = True

        async def lose_acknowledgement(session: AsyncSession) -> None:
            """Complete real SQL commitment before failing one original acknowledgement boundary."""
            nonlocal armed
            await actual_commit(session)
            if armed:
                armed = False
                raise ConnectionError("Synthetic lost acknowledgement after actual local commit")

        monkeypatch.setattr(AsyncSession, "commit", lose_acknowledgement)
        with pytest.raises(ConnectionError, match="lost acknowledgement"):
            await _begin(scenario, _ORDERS, 100)
        async with scenario.native.repository.session() as session:
            original = await get_bound_futures_inventory_attempt(session, _identity(100).public_id)
        scenario.native.clock.current += timedelta(seconds=2)
        retried = await _begin(scenario, _ORDERS, 100)
        assert retried == original and retried.payload.family_sequence == 1
        async with scenario.native.repository.session() as session:
            assert await session.scalar(select(func.count()).select_from(AttemptRow)) == 1
        assert not scenario.adapter.inventory_calls


async def _current_inventory(
    scenario: _Scenario, purpose: InventoryReadPurpose
) -> CurrentFuturesInventoryRead:
    """Read genuine current evidence under its actual repository transaction and wallet fence."""
    return await read_current_futures_inventory(
        scenario.native.repository,
        scenario.native.selection,
        purpose,
        FuturesInventoryFreshnessPolicy(
            maximum_source_age=timedelta(seconds=30),
            maximum_server_clock_skew=timedelta(seconds=5),
        ),
    )


@pytest.mark.parametrize("purpose", (_ORDERS, _POSITIONS))
async def test_real_current_reader_preserves_endpoint_barriers_and_original_source_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, purpose: InventoryReadPurpose
) -> None:
    """Compose genuine SDK capture, committed SQL and the current observational reader.

    Given: Both endpoints have actual signed empty responses and independently committed outcomes.
    When: One endpoint gains a pending then failed request and the other source reaches expiry.
    Then: Only genuine fresh success supplies a snapshot; endpoint barriers and original age remain.
    """
    async with _scenario(tmp_path, monkeypatch) as scenario:
        monkeypatch.setattr(current_reader, "datetime", scenario.native.clock)
        absent = await _current_inventory(scenario, purpose)
        assert absent.reason == "not_read" and absent.snapshot is None
        await _capture(scenario, _ORDERS, 100)
        await _capture(scenario, _POSITIONS, 110)
        available = await _current_inventory(scenario, purpose)
        assert available.status == "available" and available.snapshot is not None
        assert available.attempt is not None and available.outcome is not None
        pending = await _begin(scenario, purpose, 120)
        unavailable = await _current_inventory(scenario, purpose)
        assert unavailable.reason == "pending" and unavailable.snapshot is None
        assert unavailable.attempt == pending and unavailable.outcome is None
        failure = await _append(
            scenario, pending, FuturesInventoryReadFailure(reason="source_invalid"), _identity(121)
        )
        failed = await _current_inventory(scenario, purpose)
        assert failed.reason == "failed" and failed.snapshot is None and failed.outcome == failure
        other = _POSITIONS if purpose == _ORDERS else _ORDERS
        independent = await _current_inventory(scenario, other)
        assert independent.status == "available" and independent.snapshot is not None
        scenario.native.clock.current += timedelta(seconds=30)
        stale = await _current_inventory(scenario, other)
        assert stale.reason == "stale" and stale.snapshot is None
        assert len(scenario.adapter.inventory_calls) == 2
