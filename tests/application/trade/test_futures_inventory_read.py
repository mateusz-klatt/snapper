"""Test inventory producer orchestration at explicit transaction and transport boundaries.

These boundary doubles establish ownership, ordering and safe failure policy;
they do not establish SDK issuance, physical SQL constraints or provenance.
Separate guarded HTTP and migrated SQLite/PostgreSQL witnesses cover those.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call

import pytest
from kraken.futures import User
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade import futures_inventory_read as runtime
from snapper.application.trade.current_futures_inventory import (
    CurrentFuturesInventoryRead as StoredCurrentInventory,
)
from snapper.application.trade.futures_open_inventory import parse_futures_open_orders
from snapper.application.trade.native_private_owner_preparation import PreparedPrivateOwnerRead
from snapper.core.futures_inventory_read import FuturesInventoryReadFailure
from snapper.core.futures_inventory_read import InventoryReadPurpose
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryAttempt
from snapper.data.futures_inventory_read_store import BoundFuturesInventoryOutcome
from snapper.data.native_account_read_store import NativeAccountReadBindingError
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.futures_inventory_wire import FuturesInventoryWireError
from snapper.infrastructure.exchanges.futures_inventory_wire import FuturesInventoryWireReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import GuardedFuturesInventoryReceipt
from snapper.infrastructure.exchanges.futures_inventory_wire import InventoryWireFailureReason
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.core.test_futures_inventory_read import _ENTITY
from tests.core.test_futures_inventory_read import _attempt
from tests.core.test_futures_inventory_read import _key
from tests.core.test_futures_inventory_read import _outcome
from tests.core.test_native_account_read import _id
from tests.core.test_native_account_read import _selection


@dataclass(frozen=True)
class _Scenario:
    """Keep public shape fixtures and explicit boundary doubles together without actual issuance."""

    repository: MagicMock
    session: MagicMock
    client: MagicMock
    user: MagicMock
    token: MagicMock
    prepared: MagicMock
    tracker: SequenceTracker
    operation: runtime._InventoryOperation
    outcome: BoundFuturesInventoryOutcome
    receipt: GuardedFuturesInventoryReceipt


def _scenario(purpose: InventoryReadPurpose = "kraken_futures.open_orders") -> _Scenario:
    """Build coherent public shapes while explicitly mocking persistence and wire issuance."""
    repository = MagicMock(spec=SQLAlchemyRepository)
    session = MagicMock(spec=AsyncSession)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=None)
    repository.session.return_value = context
    repository._begin_execution_insert_transaction = AsyncMock()
    repository._acquire_execution_fence_lock = AsyncMock()
    session.commit = AsyncMock()
    client = MagicMock(spec=KrakenFuturesExchangeClient)
    client.execution_client_selection.return_value = _selection()
    user = MagicMock(spec=User)
    client._user_client = user
    token = MagicMock(spec=PreparedKeyToken)
    reference = PreparedKeyProofReference.model_validate(_key().model_dump(mode="python"))
    token.require_proof_reference.return_value = reference
    prepared = MagicMock(spec=PreparedPrivateOwnerRead)
    prepared.require_token.return_value = token
    tracker = SequenceTracker()
    operation = runtime._InventoryOperation(
        BoundFuturesInventoryAttempt(_id(13), _attempt(purpose=purpose)),
        runtime._identity(tracker, _selection()),
    )
    receipt = GuardedFuturesInventoryReceipt(
        FuturesInventoryWireReceipt(
            inventory_attempt_public_id=_id(13),
            purpose=purpose,
            client_selection=_selection(),
            key_proof=reference,
            request_started_at=operation.attempt.payload.started_at,
            observed_at=operation.attempt.payload.started_at,
            entity_bytes=_ENTITY,
        )
    )
    return _Scenario(
        repository,
        session,
        client,
        user,
        token,
        prepared,
        tracker,
        operation,
        BoundFuturesInventoryOutcome(_id(17), _outcome()),
        receipt,
    )


def _available(scenario: _Scenario) -> runtime.CurrentFuturesInventoryRead:
    """Expose the current-reader boundary result without claiming the unit fixture was persisted."""
    return runtime.CurrentFuturesInventoryRead(
        parse_futures_open_orders(_ENTITY), scenario.outcome.payload, "available"
    )


@pytest.mark.asyncio
async def test_inventory_get_waits_for_independent_attempt_commit_acknowledgement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given a blocked commit acknowledgement, when read starts, then inventory GET cannot start yet.

    Given: A real producer coroutine and explicit begin, wire and result-persistence boundaries.
    When: The independently committed attempt has not yet returned acknowledgement.
    Then: No inventory source call occurs until acknowledgement, with separate result identity.
    """
    scenario = _scenario()
    begin_started, commit_acknowledged = asyncio.Event(), asyncio.Event()
    trace: list[str] = []

    async def begin(*args: object) -> BoundFuturesInventoryAttempt:
        """Hold the actual producer await until the boundary's commit acknowledgement arrives."""
        trace.append("begin")
        begin_started.set()
        await commit_acknowledged.wait()
        trace.append("commit_acknowledged")
        return scenario.operation.attempt

    async def owned(
        client: KrakenFuturesExchangeClient,
        user: User,
        operation: str,
        read: Callable[[], GuardedFuturesInventoryReceipt],
    ) -> GuardedFuturesInventoryReceipt:
        """Execute the actual producer callback while making the ownership boundary explicit."""
        assert client is scenario.client and user is scenario.user
        assert operation == "get_native_open_orders_source"
        trace.append("inventory_get")
        return read()

    wire = MagicMock(return_value=scenario.receipt)
    record = AsyncMock(return_value=scenario.outcome)
    monkeypatch.setattr(runtime, "begin_futures_inventory_read", begin)
    monkeypatch.setattr(runtime, "read_owned_native_source", owned)
    monkeypatch.setattr(runtime, "read_futures_inventory_wire", wire)
    monkeypatch.setattr(runtime, "_record_result", record)
    task = asyncio.create_task(
        runtime._read_with_prepared_owner(
            scenario.repository,
            scenario.client,
            scenario.prepared,
            "kraken_futures.open_orders",
            scenario.tracker,
        )
    )
    await begin_started.wait()
    assert trace == ["begin"] and not wire.called and not record.called
    commit_acknowledged.set()
    assert await task is scenario.outcome
    assert trace == ["begin", "commit_acknowledged", "inventory_get"]
    arguments = record.await_args
    assert arguments is not None
    operation = arguments.args[1]
    assert operation.outcome_identity.public_id != operation.attempt.public_id
    assert arguments.args[2] is scenario.receipt
    scenario.prepared.close.assert_not_called()


@pytest.mark.asyncio
async def test_failed_attempt_commit_acknowledgement_never_reaches_inventory_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given an uncertain attempt commit, when its boundary fails, then no inventory GET is retried.

    Given: Begin may have committed but does not return successful acknowledgement.
    When: The producer awaits that actual boundary and receives a failure.
    Then: The original error propagates without source I/O or a fabricated terminal outcome.
    """
    scenario = _scenario()
    failure = RuntimeError("Synthetic attempt commit acknowledgement lost")
    monkeypatch.setattr(runtime, "begin_futures_inventory_read", AsyncMock(side_effect=failure))
    read, record = AsyncMock(), AsyncMock()
    monkeypatch.setattr(runtime, "_read_result", read)
    monkeypatch.setattr(runtime, "_record_result", record)
    with pytest.raises(RuntimeError) as raised:
        await runtime._read_with_prepared_owner(
            scenario.repository,
            scenario.client,
            scenario.prepared,
            "kraken_futures.open_orders",
            scenario.tracker,
        )
    assert raised.value is failure
    read.assert_not_called()
    record.assert_not_called()


@pytest.mark.asyncio
async def test_outcome_transaction_holds_original_wallet_fence_before_append_and_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given a result, when persisted, then its original wallet fence precedes append and commit.

    Given: The actual result transaction orchestrator with explicit SQL session boundaries.
    When: A safe failed source result is recorded.
    Then: Lock and commit ordering binds the original request without claiming physical SQL proof.
    """
    scenario = _scenario()
    result = FuturesInventoryReadFailure(reason="source_invalid")
    append = AsyncMock(return_value=scenario.outcome)
    monkeypatch.setattr(runtime, "_append_futures_inventory_outcome", append)
    trace = MagicMock()
    trace.attach_mock(scenario.repository._begin_execution_insert_transaction, "begin")
    trace.attach_mock(scenario.repository._acquire_execution_fence_lock, "fence")
    trace.attach_mock(append, "append")
    trace.attach_mock(scenario.session.commit, "commit")
    assert (
        await runtime._record_result(scenario.repository, scenario.operation, result)
        is scenario.outcome
    )
    assert trace.mock_calls == [
        call.begin(scenario.session),
        call.fence(scenario.session, _selection().wallet_public_id),
        call.append(
            scenario.session,
            scenario.operation.attempt.public_id,
            result,
            scenario.operation.outcome_identity,
        ),
        call.commit(),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ("kraken_futures.open_orders", "kraken_futures.open_positions"))
async def test_each_endpoint_uses_its_original_user_selection_and_owned_operation(
    monkeypatch: pytest.MonkeyPatch, purpose: InventoryReadPurpose
) -> None:
    """Given either inventory endpoint, when dispatched, then the original user and token persist.

    Given: The actual source-read orchestrator, an original user and one independently begun request.
    When: Its owned callback invokes the fixed endpoint wire boundary.
    Then: It preserves original user, selected revision, token and attempt identity.
    """
    scenario = _scenario(purpose)
    wire = MagicMock(return_value=scenario.receipt)

    async def owned(
        client: KrakenFuturesExchangeClient,
        user: User,
        operation: str,
        read: Callable[[], GuardedFuturesInventoryReceipt],
    ) -> GuardedFuturesInventoryReceipt:
        """Exercise the unchanged producer callback instead of substituting a read result."""
        assert client is scenario.client and user is scenario.user
        assert operation == (
            "get_native_open_orders_source"
            if purpose == "kraken_futures.open_orders"
            else "get_native_open_positions_source"
        )
        return read()

    monkeypatch.setattr(runtime, "read_owned_native_source", owned)
    monkeypatch.setattr(runtime, "read_futures_inventory_wire", wire)
    assert (
        await runtime._read_result(
            scenario.client, scenario.user, scenario.token, scenario.operation
        )
        is scenario.receipt
    )
    wire.assert_called_once_with(
        scenario.user, _selection(), scenario.token, scenario.operation.attempt.public_id, purpose
    )
    scenario.client._record_rest_call.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("moment", ("before", "during"))
@pytest.mark.parametrize("change", ("user", "selection"))
async def test_source_read_refuses_changed_original_user_or_selection_across_await(
    monkeypatch: pytest.MonkeyPatch, moment: str, change: str
) -> None:
    """Given changed original ownership, when an await completes, then captured success is refused.

    Given: An independently begun source request and the exact user selected before dispatch.
    When: The user slot or credential revision changes before or during the owned await.
    Then: Continuity fails without accepting the result under a later public selection.
    """
    scenario = _scenario()

    def mutate() -> None:
        """Change one genuine identity coordinate without rewriting the original request."""
        if change == "user":
            scenario.client._user_client = MagicMock(spec=User)
        else:
            scenario.client.execution_client_selection.return_value = _selection(
                credential_public_id=_id(99)
            )

    async def owned(*args: object) -> GuardedFuturesInventoryReceipt:
        """Change the active identity while the actual source await is outstanding."""
        mutate()
        return scenario.receipt

    read = AsyncMock(side_effect=owned)
    monkeypatch.setattr(runtime, "read_owned_native_source", read)
    if moment == "before":
        mutate()
    with pytest.raises(NativeAccountReadBindingError, match="native_read_actual_client_changed"):
        await runtime._read_result(
            scenario.client, scenario.user, scenario.token, scenario.operation
        )
    assert read.await_count == (0 if moment == "before" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason", ("transport_error", "http_error", "source_invalid", "source_unsafe")
)
async def test_safe_source_failure_disposition_is_preserved(
    monkeypatch: pytest.MonkeyPatch, reason: InventoryWireFailureReason
) -> None:
    """Given a fixed safe wire failure, when completed, then its original classification persists.

    Given: The transport boundary rejects a request with an explicit privacy-safe reason.
    When: The real completion orchestrator records the result.
    Then: Only that fixed disposition reaches persistence, without raw body or exception content.
    """
    scenario = _scenario()
    refused = FuturesInventoryWireError(reason)
    monkeypatch.setattr(runtime, "_read_result", AsyncMock(side_effect=refused))
    record = AsyncMock(return_value=scenario.outcome)
    monkeypatch.setattr(runtime, "_record_result", record)
    assert (
        await runtime._complete_operation(
            scenario.repository, scenario.client, scenario.user, scenario.token, scenario.operation
        )
        is scenario.outcome
    )
    record.assert_awaited_once_with(
        scenario.repository, scenario.operation, FuturesInventoryReadFailure(reason=reason)
    )


@pytest.mark.asyncio
async def test_unknown_source_exception_is_fixed_transport_failure_without_sensitive_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given an unknown exception, when completed, then its text is excluded from retained failure.

    Given: An external source collaborator fails with synthetic sensitive exception text.
    When: The actual completion producer handles it.
    Then: Persistence sees only the fixed transport failure code and original operation identity.
    """
    scenario = _scenario()
    monkeypatch.setattr(
        runtime, "_read_result", AsyncMock(side_effect=RuntimeError("SYNTHETIC-PRIVATE-MESSAGE"))
    )
    record = AsyncMock(return_value=scenario.outcome)
    monkeypatch.setattr(runtime, "_record_result", record)
    await runtime._complete_operation(
        scenario.repository, scenario.client, scenario.user, scenario.token, scenario.operation
    )
    arguments = record.await_args
    assert arguments is not None
    result = arguments.args[2]
    assert result == FuturesInventoryReadFailure(reason="transport_error")
    assert "SYNTHETIC-PRIVATE-MESSAGE" not in result.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "record_failure", (None, RuntimeError("lost ack"), asyncio.CancelledError())
)
async def test_read_cancellation_keeps_original_signal_and_attempts_only_safe_cancelled_outcome(
    monkeypatch: pytest.MonkeyPatch, record_failure: BaseException | None
) -> None:
    """Given joined read cancellation, when failure retention is attempted, then original signal wins.

    Given: The owned source boundary has joined its worker and propagates the original cancellation.
    When: Safe cancelled-outcome retention succeeds, fails or receives another cancellation.
    Then: The exact first cancellation propagates and no captured-success replacement is attempted.
    """
    scenario = _scenario()
    interrupted = asyncio.CancelledError("Original synthetic caller cancellation")
    monkeypatch.setattr(runtime, "_read_result", AsyncMock(side_effect=interrupted))
    record = AsyncMock(return_value=scenario.outcome, side_effect=record_failure)
    monkeypatch.setattr(runtime, "_record_result", record)
    with pytest.raises(asyncio.CancelledError) as raised:
        await runtime._complete_operation(
            scenario.repository, scenario.client, scenario.user, scenario.token, scenario.operation
        )
    assert raised.value is interrupted
    record.assert_awaited_once_with(
        scenario.repository, scenario.operation, FuturesInventoryReadFailure(reason="cancelled")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (RuntimeError("commit ack lost"), asyncio.CancelledError()))
async def test_ambiguous_successful_outcome_commit_never_writes_a_replacement_failure(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    """Given an ambiguous captured-result commit, when acknowledgement fails, then result is not replaced.

    Given: Actual source completion succeeded and persistence may have committed the captured outcome.
    When: The result-recording boundary raises an error or cancellation after its own start.
    Then: The producer propagates that original signal and makes exactly one write attempt.
    """
    scenario = _scenario()
    monkeypatch.setattr(runtime, "_read_result", AsyncMock(return_value=scenario.receipt))
    record = AsyncMock(side_effect=failure)
    monkeypatch.setattr(runtime, "_record_result", record)
    with pytest.raises(type(failure)) as raised:
        await runtime._complete_operation(
            scenario.repository, scenario.client, scenario.user, scenario.token, scenario.operation
        )
    assert raised.value is failure
    record.assert_awaited_once_with(scenario.repository, scenario.operation, scenario.receipt)


@pytest.mark.asyncio
async def test_bootstrap_reuses_one_owned_preparation_for_two_independent_endpoint_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given original bootstrap ownership, when captured, then two endpoint attempts reuse that owner.

    Given: The real bootstrap orchestration has the actual still-owned prepared key/accounts result.
    When: It captures initial independent inventory endpoints before the socket handoff.
    Then: Both purposes use the same owner, no duplicate preparation runs and ownership stays caller's.
    """
    scenario = _scenario()
    read = AsyncMock(return_value=scenario.outcome)
    prepare = AsyncMock()
    monkeypatch.setattr(runtime, "_read_with_prepared_owner", read)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    await runtime.capture_prepared_futures_inventory(
        scenario.repository, scenario.client, scenario.prepared, scenario.tracker
    )
    assert read.await_args_list == [
        call(
            scenario.repository,
            scenario.client,
            scenario.prepared,
            purpose,
            scenario.tracker,
        )
        for purpose in ("kraken_futures.open_orders", "kraken_futures.open_positions")
    ]
    prepare.assert_not_called()
    scenario.prepared.close.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("available", (False, True))
async def test_current_wrapper_preserves_fenced_reader_outcome_and_explicit_availability(
    monkeypatch: pytest.MonkeyPatch, available: bool
) -> None:
    """Given a fenced reader result, when wrapped, then its exact outcome and availability survive.

    Given: A stored pending result or a complete available empty inventory observation.
    When: The runtime exposes its stable consumer-facing current view.
    Then: Absence remains unavailable, complete empty remains available and original outcome is kept.
    """
    scenario = _scenario()
    stored = StoredCurrentInventory(
        purpose="kraken_futures.open_orders",
        status="available" if available else "unavailable",
        reason=None if available else "pending",
        attempt=scenario.operation.attempt,
        outcome=scenario.outcome if available else None,
        snapshot=parse_futures_open_orders(_ENTITY) if available else None,
    )
    read = AsyncMock(return_value=stored)
    monkeypatch.setattr(runtime, "read_current_futures_inventory", read)
    current = await runtime._current(
        scenario.repository, _selection(), "kraken_futures.open_orders"
    )
    assert current.snapshot is stored.snapshot
    assert current.available is available
    assert current.outcome is (scenario.outcome.payload if available else None)
    assert current.reason == ("available" if available else "pending")
    read.assert_awaited_once_with(
        scenario.repository, _selection(), "kraken_futures.open_orders", runtime._FRESHNESS
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ("selection", "user"))
async def test_refresh_without_current_client_identity_never_prepares_or_reads(
    monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    """Given missing client identity, when refreshed, then no current read or source request occurs.

    Given: The actual client lacks its selected public revision or original SDK user.
    When: The runtime inventory entry point is invoked.
    Then: It returns explicit client unavailability before any persistence or source I/O.
    """
    scenario = _scenario()
    if missing == "selection":
        scenario.client.execution_client_selection.return_value = None
    else:
        scenario.client._user_client = None
    current, prepare = AsyncMock(), AsyncMock()
    monkeypatch.setattr(runtime, "_current", current)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    result = await runtime.refresh_current_futures_inventory(
        scenario.repository, scenario.client, "kraken_futures.open_orders", scenario.tracker
    )
    assert not result.available and result.reason == "client_unavailable"
    current.assert_not_called()
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_inventory_purpose_refuses_before_client_selection_or_source_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given an unsupported purpose, when refreshed, then no selected user or endpoint is consulted.

    Given: A caller bypasses static typing and supplies an unknown inventory endpoint purpose.
    When: The public runtime entry point validates its fixed allowed endpoints.
    Then: It returns unavailable before selecting a client, reading storage or preparing source I/O.
    """
    scenario = _scenario()
    current, prepare = AsyncMock(), AsyncMock()
    monkeypatch.setattr(runtime, "_current", current)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    result = await runtime.refresh_current_futures_inventory(
        scenario.repository,
        scenario.client,
        cast(InventoryReadPurpose, "unsupported"),
        scenario.tracker,
    )
    assert not result.available and result.reason == "unverifiable"
    scenario.client.execution_client_selection.assert_not_called()
    current.assert_not_called()
    prepare.assert_not_called()


@pytest.mark.asyncio
async def test_fresh_qualified_inventory_skips_duplicate_key_accounts_and_inventory_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given fresh qualified inventory, when refreshed, then bootstrap source ownership is reused.

    Given: The fenced current reader has a fresh result for the original current user and revision.
    When: An actual consumer requests the same endpoint.
    Then: No additional key/accounts preparation or inventory GET occurs.
    """
    scenario = _scenario()
    current = _available(scenario)
    read = AsyncMock(return_value=current)
    prepare, capture = AsyncMock(), AsyncMock()
    monkeypatch.setattr(runtime, "_current", read)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    monkeypatch.setattr(runtime, "_read_with_prepared_owner", capture)
    assert (
        await runtime.refresh_current_futures_inventory(
            scenario.repository, scenario.client, "kraken_futures.open_orders", scenario.tracker
        )
        is current
    )
    read.assert_awaited_once_with(scenario.repository, _selection(), "kraken_futures.open_orders")
    prepare.assert_not_called()
    capture.assert_not_called()


@pytest.mark.asyncio
async def test_unavailable_current_inventory_gets_explicit_new_owner_then_closes_after_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given unavailable current inventory, when refreshed, then fresh ownership has one cleanup owner.

    Given: A stale or missing current observation and the original selected user still active.
    When: The runtime prepares a new key/accounts owner and captures its requested endpoint.
    Then: That owner closes before the final qualified read and no socket-private token is consulted.
    """
    scenario = _scenario()
    trace: list[str] = []
    unavailable, available = (
        runtime.CurrentFuturesInventoryRead(None, None, "stale"),
        _available(scenario),
    )

    async def current(*args: object) -> runtime.CurrentFuturesInventoryRead:
        """Observe closure at the real producer's second current-read boundary."""
        if trace:
            scenario.prepared.close.assert_called_once()
            return available
        trace.append("current_unavailable")
        return unavailable

    prepare = AsyncMock(return_value=scenario.prepared)
    capture = AsyncMock(return_value=scenario.outcome)
    monkeypatch.setattr(runtime, "_current", current)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    monkeypatch.setattr(runtime, "_read_with_prepared_owner", capture)
    assert (
        await runtime.refresh_current_futures_inventory(
            scenario.repository, scenario.client, "kraken_futures.open_orders", scenario.tracker
        )
        is available
    )
    prepare.assert_awaited_once_with(scenario.repository, scenario.client, scenario.tracker)
    capture.assert_awaited_once_with(
        scenario.repository,
        scenario.client,
        scenario.prepared,
        "kraken_futures.open_orders",
        scenario.tracker,
    )
    scenario.prepared.close.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ("current_before", "prepare", "capture", "current_after"))
async def test_refresh_failure_reports_unavailability_and_closes_only_an_owned_preparation(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    """Given a refresh failure, when the stage fails, then availability stays false and ownership closes.

    Given: Source preparation or current-reader collaborators can fail at each asynchronous boundary.
    When: The actual runtime refresh executes until that boundary.
    Then: The fixed unavailable result contains no exception text and any acquired owner closes once.
    """
    scenario = _scenario()
    failure = RuntimeError("SYNTHETIC-PRIVATE-ERROR")
    stale = runtime.CurrentFuturesInventoryRead(None, None, "stale")
    current = AsyncMock(
        side_effect=(
            [failure]
            if stage == "current_before"
            else [stale, failure if stage == "current_after" else stale]
        )
    )
    prepare = AsyncMock(
        return_value=scenario.prepared, side_effect=failure if stage == "prepare" else None
    )
    capture = AsyncMock(side_effect=failure if stage == "capture" else None)
    monkeypatch.setattr(runtime, "_current", current)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    monkeypatch.setattr(runtime, "_read_with_prepared_owner", capture)
    result = await runtime.refresh_current_futures_inventory(
        scenario.repository, scenario.client, "kraken_futures.open_orders", scenario.tracker
    )
    assert result == runtime.CurrentFuturesInventoryRead(None, None, "unverifiable")
    assert scenario.prepared.close.call_count == (1 if stage in ("capture", "current_after") else 0)


@pytest.mark.asyncio
async def test_refresh_read_cancellation_closes_preparation_and_propagates_original_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Given cancelled joined source work, when refresh unwinds, then the actual owner closes once.

    Given: A new preparation was acquired after explicit current unavailability.
    When: Its owned inventory-read boundary propagates the original post-join cancellation.
    Then: The owner closes and cancellation is preserved rather than becoming availability data.
    """
    scenario = _scenario()
    interrupted = asyncio.CancelledError("Original synthetic refresh cancellation")
    current = AsyncMock(return_value=runtime.CurrentFuturesInventoryRead(None, None, "pending"))
    monkeypatch.setattr(runtime, "_current", current)
    monkeypatch.setattr(
        runtime, "prepare_native_private_owner", AsyncMock(return_value=scenario.prepared)
    )
    monkeypatch.setattr(runtime, "_read_with_prepared_owner", AsyncMock(side_effect=interrupted))
    with pytest.raises(asyncio.CancelledError) as raised:
        await runtime.refresh_current_futures_inventory(
            scenario.repository, scenario.client, "kraken_futures.open_orders", scenario.tracker
        )
    assert raised.value is interrupted
    scenario.prepared.close.assert_called_once()
    current.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ("current_before", "prepare", "current_after"))
async def test_refresh_refuses_original_user_changes_after_every_asynchronous_gap(
    monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    """Given user replacement during refresh, when the await returns, then later ownership is refused.

    Given: A captured original SDK user and unavailable current inventory for its selected revision.
    When: That actual user slot changes during a current-read or preparation await.
    Then: The result is unavailable, no later user inherits authority and acquired ownership closes.
    """
    scenario = _scenario()
    calls = 0

    async def current(*args: object) -> runtime.CurrentFuturesInventoryRead:
        """Replace the user only at the designated real current-reader boundary."""
        nonlocal calls
        calls += 1
        if stage == "current_before" or (stage == "current_after" and calls == 2):
            scenario.client._user_client = MagicMock(spec=User)
        return runtime.CurrentFuturesInventoryRead(None, None, "stale")

    async def prepare(*args: object) -> PreparedPrivateOwnerRead:
        """Replace the active user only after the new owner has actually been acquired."""
        if stage == "prepare":
            scenario.client._user_client = MagicMock(spec=User)
        return scenario.prepared

    capture = AsyncMock(return_value=scenario.outcome)
    monkeypatch.setattr(runtime, "_current", current)
    monkeypatch.setattr(runtime, "prepare_native_private_owner", prepare)
    monkeypatch.setattr(runtime, "_read_with_prepared_owner", capture)
    result = await runtime.refresh_current_futures_inventory(
        scenario.repository, scenario.client, "kraken_futures.open_orders", scenario.tracker
    )
    assert not result.available and result.reason == "unverifiable"
    assert scenario.prepared.close.call_count == (0 if stage == "current_before" else 1)
    assert capture.await_count == (1 if stage == "current_after" else 0)
