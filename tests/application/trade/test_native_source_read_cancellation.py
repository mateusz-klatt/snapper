"""Actual SDK and local Requests cancellation witnesses without replacing transport issuance."""

import asyncio
import threading
from collections.abc import Callable
from collections.abc import Iterator
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Literal

import pytest
from kraken.futures import User
from requests import ConnectionError as RequestsConnectionError
from requests import PreparedRequest
from requests import Response

from snapper.application.trade import native_account_read as producer
from snapper.core.native_account_identity import NativeAccountPublicIdentity
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.data.native_account_read_store import BoundNativeAccountReadOutcome
from snapper.data.native_account_read_store import get_latest_native_account_read
from snapper.infrastructure.exchanges import kraken_futures_key_identity as key_transport
from snapper.infrastructure.exchanges import kraken_rest_egress as routing
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario


@dataclass
class _ReadGate:
    """Own one transient local transport wait and observe only actual issued tokens."""

    phase: Literal["key", "body"]
    fail_after_release: bool
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    failure_started: threading.Event = field(default_factory=threading.Event)
    issued: list[PreparedKeyToken] = field(default_factory=list)

    def wait(self) -> None:
        """Hold real SDK source processing until the test explicitly releases its owned worker."""
        self.entered.set()
        if not self.release.wait(5):
            raise TimeoutError("Synthetic local source release was not delivered")
        if self.fail_after_release:
            raise RequestsConnectionError("Synthetic late local source failure")


def _observe_issuer(gate: _ReadGate, monkeypatch: pytest.MonkeyPatch) -> None:
    """Observe the real closed transport mint without manufacturing a replacement token."""
    actual = key_transport._mint_prepared_private_key

    def observe(user: User, key: str, observation: NativeAccountPublicIdentity) -> PreparedKeyToken:
        """Return exactly the token minted by the actual transport boundary."""
        token = actual(user, key, observation)
        gate.issued.append(token)
        return token

    monkeypatch.setattr(key_transport, "_mint_prepared_private_key", observe)


def _observe_failure(gate: _ReadGate, monkeypatch: pytest.MonkeyPatch) -> None:
    """Notice premature outcome recording while still calling the actual repository writer."""
    actual = producer._record_failure

    async def observe(
        repository: AccountingRepository,
        operation: producer._ReadOperation,
        reason: Literal["transport_error", "cancelled", "source_invalid", "http_error"],
    ) -> BoundNativeAccountReadOutcome:
        """Keep actual durable failure behavior and expose its ordering to the test."""
        gate.failure_started.set()
        return await actual(repository, operation, reason)

    monkeypatch.setattr(producer, "_record_failure", observe)


def _block_body(response: Response, gate: _ReadGate, monkeypatch: pytest.MonkeyPatch) -> None:
    """Delay delivery from the actual original HTTP entity without changing its bytes."""
    original = response.iter_content

    def chunks(chunk_size: int = 1, decode_unicode: bool = False) -> Iterator[bytes]:
        """Join the original byte iterator only after the controlled body read is released."""
        assert not decode_unicode
        gate.wait()
        yield from original(chunk_size=chunk_size, decode_unicode=False)

    monkeypatch.setattr(response, "iter_content", chunks)


def _install_gate(
    source: NativeAuthorityScenario, gate: _ReadGate, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep real SDK signing, actual egress, immutable proof checks and original local HTTP bytes."""
    original = source.adapter.send

    def send(request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Observe the actual adapter response and block only the selected source boundary."""
        response = original(request, *args, **kwargs)
        is_key = (request.url or "").endswith("/api/auth/v1/api-keys/v3/check")
        if gate.phase == "key" and is_key:
            try:
                gate.wait()
            except BaseException:
                response.close()
                raise
        elif gate.phase == "body" and not is_key:
            _block_body(response, gate, monkeypatch)
        return response

    monkeypatch.setattr(source.adapter, "send", send)
    _observe_issuer(gate, monkeypatch)
    _observe_failure(gate, monkeypatch)


async def _cleanup(
    source: NativeAuthorityScenario,
    gate: _ReadGate,
    task: asyncio.Task[BoundNativeAccountReadOutcome],
) -> None:
    """Join every real REST worker and close observed late tokens even when the RED assertion fires."""
    gate.release.set()
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    pool = source.client._rest_pool
    if pool is not None:
        await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=False)
    for token in gate.issued:
        token.close()


@pytest.mark.parametrize("phase", ["key", "body"])
@pytest.mark.parametrize("late_failure", [False, True])
@pytest.mark.parametrize("repeated", [False, True])
async def test_cancelled_actual_source_joins_before_failure_or_owner_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: Literal["key", "body"],
    late_failure: bool,
    repeated: bool,
) -> None:
    """Cancellation retains actual transport ownership until every original source worker exits.

    Given: A real signed key request or proof-bound accounts body is blocked in its REST thread,
    When: Its caller receives one or repeated cancellations and the source succeeds or fails late,
    Then: No cancellation outcome or token release precedes the join, and late tokens are closed.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
        phase: Original key response or accounts body boundary to hold.
        late_failure: Whether the blocked original source raises after its explicit release.
        repeated: Whether further caller cancellations arrive while source work is still owned.
    """
    gate = _ReadGate(phase, late_failure)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(gate.entered.wait, 2)
            task.cancel("synthetic-first-cancellation")
            await asyncio.sleep(0)
            for _ in range(3 if repeated else 0):
                task.cancel("synthetic-later-cancellation")
                await asyncio.sleep(0)
            assert not task.done(), "Caller completed while the original SDK worker was blocked"
            assert not gate.failure_started.is_set(), "Failure recording preceded the source join"
            if phase == "body":
                assert len(gate.issued) == 1
                gate.issued[0].require_proof_reference()
            gate.release.set()
            with pytest.raises(asyncio.CancelledError, match="synthetic-first-cancellation"):
                await task
            assert gate.failure_started.is_set()
            for token in gate.issued:
                with pytest.raises(PreparedKeyTokenError, match="closed"):
                    token.require_proof_reference()
            assert len(source.adapter.calls) == (1 if phase == "key" else 2)
            async with source.repository.session() as session:
                latest = await get_latest_native_account_read(
                    session, source.selection, source.clock.current
                )
            assert latest is not None and latest.outcome is not None
            assert latest.outcome.payload.result.kind == "failed"
            assert latest.outcome.payload.result.reason == "cancelled"
        finally:
            await _cleanup(source, gate, task)


@pytest.mark.parametrize("phase", ["key", "body"])
async def test_uncancelled_actual_source_keeps_one_shot_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: Literal["key", "body"]
) -> None:
    """Ordinary source success retains its original one-shot HTTP and public-only cleanup.

    Given: The actual SDK and original local key/accounts responses,
    When: The controlled source is released without caller cancellation,
    Then: Exactly one key GET and one accounts GET complete and the public reader closes its token.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
        phase: Original key response or accounts body boundary to hold.
    """
    gate = _ReadGate(phase, False)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(gate.entered.wait, 2)
            gate.release.set()
            outcome = await task
            assert outcome.payload.result.kind == "captured"
            assert len(source.adapter.calls) == 2
            assert len(gate.issued) == 1
            with pytest.raises(PreparedKeyTokenError, match="closed"):
                gate.issued[0].require_proof_reference()
        finally:
            await _cleanup(source, gate, task)


class _ObservedExecutor(ThreadPoolExecutor):
    """Observe actual scheduling into a real single-worker queue without replacing dispatch."""

    def __init__(self) -> None:
        """Keep the first real worker occupied and signal the second actual submitted job."""
        super().__init__(max_workers=1, thread_name_prefix="native-owned-source-test")
        self.queued = threading.Event()
        self.submissions = 0

    def submit[**P, ResultT](
        self, fn: Callable[P, ResultT], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[ResultT]:
        """Return the real executor future and observe only its scheduling position."""
        result = super().submit(fn, *args, **kwargs)
        self.submissions += 1
        if self.submissions == 2:
            self.queued.set()
        return result


async def test_cancelled_queued_source_joins_without_http_or_token_issuance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancelled queued operation remains owned until its real queue entry is consumed.

    Given: The actual selected client has one occupied REST worker and its key read is queued,
    When: The caller is cancelled before that queued source callable starts,
    Then: The owner joins the queue entry and suppresses HTTP without minting a token or retrying.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
    """
    gate = _ReadGate("key", False)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        pool = _ObservedExecutor()
        source.client._rest_pool = pool
        occupied = pool.submit(gate.release.wait, 5)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(pool.queued.wait, 2)
            task.cancel("synthetic-queued-cancellation")
            await asyncio.sleep(0)
            assert not task.done()
            assert not gate.failure_started.is_set()
            gate.release.set()
            with pytest.raises(asyncio.CancelledError, match="synthetic-queued-cancellation"):
                await task
            assert occupied.result(timeout=1)
            assert source.adapter.calls == []
            assert gate.issued == []
        finally:
            await _cleanup(source, gate, task)


class _EnqueueThenRaiseExecutor(ThreadPoolExecutor):
    """Use genuine executor enqueueing before reproducing its documented submit failure boundary."""

    def __init__(self, gate: _ReadGate, fail_submission: int, wait_for_start: bool) -> None:
        """Retain the actual worker and select one scheduling acknowledgement to lose."""
        super().__init__(max_workers=1, thread_name_prefix="native-enqueue-failure-test")
        self.gate = gate
        self.fail_submission = fail_submission
        self.wait_for_start = wait_for_start
        self.submissions = 0

    def submit[**P, ResultT](
        self, fn: Callable[P, ResultT], /, *args: P.args, **kwargs: P.kwargs
    ) -> Future[ResultT]:
        """Call the real submit first and leave its actual queued or running work untouched."""
        result = super().submit(fn, *args, **kwargs)
        self.submissions += 1
        if self.submissions == self.fail_submission:
            if self.wait_for_start and not self.gate.entered.wait(2):
                raise RuntimeError("Synthetic source did not reach its controlled actual HTTP wait")
            raise RuntimeError("Synthetic thread creation acknowledgement failure after enqueue")
        return result


@pytest.mark.parametrize("cancel_during_join", [False, True])
@pytest.mark.parametrize("late_failure", [False, True])
async def test_enqueue_then_raise_joins_running_source_and_disposes_its_actual_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancel_during_join: bool,
    late_failure: bool,
) -> None:
    """An exceptional dispatch Task is not evidence that its underlying source callback stopped.

    Given: The genuine executor starts actual key HTTP before submit raises its acknowledgement error,
    When: The dispatcher fails and the joining caller may subsequently be cancelled,
    Then: Source completion precedes release, late tokens close, and delivered cancellation wins.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
        cancel_during_join: Whether caller cancellation arrives after the earlier enqueue error.
        late_failure: Whether original SDK work fails after its enqueue acknowledgement was lost.
    """
    gate = _ReadGate("key", late_failure)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        source.client._rest_pool = _EnqueueThenRaiseExecutor(gate, 1, True)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(gate.entered.wait, 2)
            await asyncio.sleep(0)
            assert not task.done()
            assert not gate.failure_started.is_set()
            if cancel_during_join:
                task.cancel("synthetic-cancellation-during-error-join")
                await asyncio.sleep(0)
                task.cancel("synthetic-repeated-error-join-cancellation")
                await asyncio.sleep(0)
                assert not task.done()
            gate.release.set()
            if cancel_during_join:
                with pytest.raises(
                    asyncio.CancelledError, match="synthetic-cancellation-during-error-join"
                ) as interrupted:
                    await task
                assert interrupted.value.__context__ is None
                assert interrupted.value.__cause__ is None
            else:
                outcome = await task
                assert outcome.payload.result.kind == "failed"
                assert outcome.payload.result.reason == "transport_error"
            assert len(source.adapter.calls) == 1
            assert len(gate.issued) == (0 if late_failure else 1)
            for token in gate.issued:
                with pytest.raises(PreparedKeyTokenError, match="closed"):
                    token.require_proof_reference()
        finally:
            await _cleanup(source, gate, task)


async def test_enqueue_then_raise_prevents_late_queued_http_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lost enqueue acknowledgement cannot permit a later unowned source request.

    Given: Genuine submit enqueues the key read behind an occupied worker and then raises,
    When: The caller records its transport failure before the queued callback can start,
    Then: Releasing and joining the real executor produces no HTTP request or issued token.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
    """
    gate = _ReadGate("key", False)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        pool = _EnqueueThenRaiseExecutor(gate, 2, False)
        source.client._rest_pool = pool
        occupied = pool.submit(gate.release.wait, 5)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            outcome = await task
            assert outcome.payload.result.kind == "failed"
            assert outcome.payload.result.reason == "transport_error"
            gate.release.set()
            await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=False)
            assert occupied.result(timeout=1)
            assert source.adapter.calls == []
            assert gate.issued == []
        finally:
            await _cleanup(source, gate, task)


async def test_actual_executor_shutdown_cancels_queued_source_without_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A genuinely cancelled executor Future remains distinct from a running source callback.

    Given: An actual key read is queued behind an occupied real REST worker,
    When: Executor shutdown cancels the pending job before its SDK callable starts,
    Then: Cancellation is retrieved without HTTP, token issuance or an unobserved task exception.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
    """
    gate = _ReadGate("key", False)
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        pool = _ObservedExecutor()
        source.client._rest_pool = pool
        occupied = pool.submit(gate.release.wait, 5)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(pool.queued.wait, 2)
            pool.shutdown(wait=False, cancel_futures=True)
            with pytest.raises(asyncio.CancelledError):
                await task
            assert source.adapter.calls == []
            assert gate.issued == []
            gate.release.set()
            assert occupied.result(timeout=1)
        finally:
            await _cleanup(source, gate, task)


async def test_enqueue_failure_joins_original_proxy_restoration_before_token_disposal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Completing inner HTTP is insufficient while the actual egress wrapper still owns SDK state.

    Given: A real enqueue acknowledgement fails and its running key read returns an actual token,
    When: The original router is delayed immediately before restoring its real SDK/session proxies,
    Then: The caller and token remain owned until original proxy restoration and routed cleanup finish.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
    """
    gate = _ReadGate("key", False)
    restoration = _ReadGate("key", False)
    original = routing._restore_proxy

    def restore(
        target: routing.RestProxyTarget,
        client_state: routing._ClientProxyState,
        session_state: routing._SessionProxyState | None,
    ) -> None:
        """Delay and then call the actual original proxy restoration without fabricating its result."""
        restoration.wait()
        original(target, client_state, session_state)

    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        monkeypatch.setattr(routing, "_restore_proxy", restore)
        source.client._rest_pool = _EnqueueThenRaiseExecutor(gate, 1, True)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(gate.entered.wait, 2)
            gate.release.set()
            assert await asyncio.to_thread(restoration.entered.wait, 2)
            await asyncio.sleep(0)
            assert not task.done()
            assert not gate.failure_started.is_set()
            assert len(gate.issued) == 1
            with pytest.raises(PreparedKeyTokenError, match="proof_not_committed"):
                gate.issued[0].require_proof_reference()
            restoration.release.set()
            outcome = await task
            assert outcome.payload.result.kind == "failed"
            assert outcome.payload.result.reason == "transport_error"
            with pytest.raises(PreparedKeyTokenError, match="closed"):
                gate.issued[0].require_proof_reference()
        finally:
            restoration.release.set()
            await _cleanup(source, gate, task)


async def test_cancelled_source_waiting_for_actual_egress_lock_performs_no_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A started routed worker still refuses source I/O after cancellation during its real lock wait.

    Given: The actual route is waiting for its selected client's held proxy lock,
    When: The caller is cancelled before the original route can invoke the SDK source,
    Then: Releasing the lock allows owned cleanup without any HTTP request or token issuance.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
    """
    gate = _ReadGate("key", False)
    routed_entered = threading.Event()
    original = routing._execute_with_proxy

    def execute(
        *,
        target: routing.RestProxyTarget,
        proxy_lock: AbstractContextManager[object],
        proxy_url: str | None,
        sync_call: Callable[[], object],
    ) -> object:
        """Observe the real route entry and retain its actual lock, proxy mutation and callback."""
        routed_entered.set()
        return original(
            target=target, proxy_lock=proxy_lock, proxy_url=proxy_url, sync_call=sync_call
        )

    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        monkeypatch.setattr(routing, "_execute_with_proxy", execute)
        lock = source.client._rest_egress_proxy_lock
        lock.acquire()
        held = True
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(routed_entered.wait, 2)
            task.cancel("synthetic-egress-wait-cancellation")
            await asyncio.sleep(0)
            assert not task.done()
            assert not gate.failure_started.is_set()
            lock.release()
            held = False
            with pytest.raises(asyncio.CancelledError, match="synthetic-egress-wait-cancellation"):
                await task
            assert source.adapter.calls == []
            assert gate.issued == []
        finally:
            if held:
                lock.release()
            await _cleanup(source, gate, task)


async def test_real_caller_cancellation_supersedes_cancelled_dispatch_with_historical_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Handled historical cancellation and cancelled dispatch work cannot replace a new caller signal.

    Given: A caller retains a handled cancellation count while actual SDK work remains blocked,
    When: Its private dispatch Task is cancelled and a later actual caller cancellation arrives,
    Then: The first newly delivered caller cancellation propagates after the real worker and token join.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped source clocks and observers that retain actual dispatch and issuance.
    """
    gate = _ReadGate("key", False)
    dispatched: list[asyncio.Task[object]] = []
    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, gate, monkeypatch)
        actual = source.client._dispatch_blocking

        async def observe[**P, ResultT](
            func: Callable[P, ResultT], /, *args: P.args, **kwargs: P.kwargs
        ) -> ResultT:
            """Observe the owning Task while executing the actual client-owned blocking dispatcher."""
            owner = asyncio.current_task()
            assert owner is not None
            dispatched.append(owner)
            return await actual(func, *args, **kwargs)

        async def caller() -> BoundNativeAccountReadOutcome:
            """Carry a deliberately handled historical count into the genuine account-read operation."""
            owner = asyncio.current_task()
            assert owner is not None
            owner.cancel("synthetic-historical-handled-cancellation")
            with pytest.raises(asyncio.CancelledError):
                await asyncio.sleep(0)
            return await producer.read_native_futures_accounts(
                source.repository, source.client, source.tracker
            )

        monkeypatch.setattr(source.client, "_dispatch_blocking", observe)
        task = asyncio.create_task(caller())
        try:
            assert await asyncio.to_thread(gate.entered.wait, 2)
            assert len(dispatched) == 1
            dispatched[0].cancel("synthetic-dispatch-task-cancellation")
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert not task.done()
            task.cancel("synthetic-new-real-caller-cancellation")
            await asyncio.sleep(0)
            task.cancel("synthetic-later-real-caller-cancellation")
            await asyncio.sleep(0)
            assert not task.done()
            gate.release.set()
            with pytest.raises(
                asyncio.CancelledError, match="synthetic-new-real-caller-cancellation"
            ) as interrupted:
                await task
            assert interrupted.value.__context__ is None
            assert len(gate.issued) == 1
            with pytest.raises(PreparedKeyTokenError, match="closed"):
                gate.issued[0].require_proof_reference()
        finally:
            await _cleanup(source, gate, task)


@pytest.mark.parametrize("cancelled", [False, True])
async def test_actual_key_result_is_closed_after_original_proxy_restore_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    """A failing egress finalizer cannot make an already issued real key token inaccessible.

    Given: Actual SDK key HTTP has created its real token and original proxy restoration is held,
    When: That original restoration runs and raises, with or without intervening caller cancellation,
    Then: The actual token remains owned through cleanup and is closed before outward failure.

    Args:
        tmp_path: Caller-owned directory for the isolated SQLite source fixture.
        monkeypatch: Scoped actual transport observers and the original restoration wrapper.
        cancelled: Whether the caller is cancelled while original restoration remains held.
    """
    source_gate = _ReadGate("key", False)
    source_gate.release.set()
    restoration = _ReadGate("key", False)
    original = routing._restore_proxy

    def restore(
        target: routing.RestProxyTarget,
        client_state: routing._ClientProxyState,
        session_state: routing._SessionProxyState | None,
    ) -> None:
        """Call the actual original restoration before raising one fixed synthetic finalizer failure."""
        restoration.wait()
        original(target, client_state, session_state)
        raise RuntimeError("Synthetic original proxy restoration completion failure")

    async with native_authority_scenario(tmp_path, monkeypatch) as source:
        _install_gate(source, source_gate, monkeypatch)
        monkeypatch.setattr(routing, "_restore_proxy", restore)
        task = asyncio.create_task(
            producer.read_native_futures_accounts(source.repository, source.client, source.tracker)
        )
        try:
            assert await asyncio.to_thread(restoration.entered.wait, 2)
            assert len(source_gate.issued) == 1
            with pytest.raises(PreparedKeyTokenError, match="proof_not_committed"):
                source_gate.issued[0].require_proof_reference()
            if cancelled:
                task.cancel("synthetic-cancel-before-proxy-restore-error")
                await asyncio.sleep(0)
                task.cancel("synthetic-repeat-before-proxy-restore-error")
                await asyncio.sleep(0)
                assert not task.done()
                assert not source_gate.failure_started.is_set()
            restoration.release.set()
            if cancelled:
                with pytest.raises(
                    asyncio.CancelledError, match="synthetic-cancel-before-proxy-restore-error"
                ) as interrupted:
                    await task
                assert interrupted.value.__context__ is None
            else:
                outcome = await task
                assert outcome.payload.result.kind == "failed"
                assert outcome.payload.result.reason == "transport_error"
            assert len(source.adapter.calls) == 1
            with pytest.raises(PreparedKeyTokenError, match="closed"):
                source_gate.issued[0].require_proof_reference()
        finally:
            restoration.release.set()
            await _cleanup(source, source_gate, task)
