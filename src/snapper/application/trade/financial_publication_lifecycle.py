"""Own one explicit account's read-only publication recovery and actual typed receive path.

An explicit scope is required from the application's real financial context.
It supplies no account permission. Full journal hydration is independent of
publication receipts and includes retained cash-only facts. This owner never
closes its borrowed publisher or joins itself from an owned callback task.
"""

import asyncio
from collections.abc import Coroutine
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import cast

import zmq
import zmq.asyncio

from snapper.application.portfolio.versioned_contract_account_projection import (
    VersionedContractAccountProjection,
)
from snapper.application.trade.financial_publication_transport import FinancialPublicationTransport
from snapper.application.trade.versioned_financial_publication import (
    publish_pending_versioned_financial_notifications,
)
from snapper.core.contract_account_projection import ContractAccountSnapshot
from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.data.contract_accounting_store import AccountingRepository
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.financial_publication import FinancialPublicationData
from snapper.messaging.schemas.messages import parse_message


class FinancialPublicationLifecycleError(RuntimeError):
    """A lifecycle operation cannot complete under its actual ownership contract."""


class FinancialPublicationLifecycleStatus(StrEnum):
    """Describe resource ownership separately from the last retained snapshot clock."""

    NEW = "new"
    STARTING = "starting"
    ACTIVE = "active"
    STALE = "stale"
    CLOSING = "closing"
    CLOSED = "closed"
    UNRESOLVED = "unresolved"


@dataclass(frozen=True, slots=True)
class FinancialPublicationLifecycleOptions:
    """Keep local internal transport and polling policy outside public financial schemas."""

    broker_xpub: str
    refresh_interval: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        """Refuse unusable local transport policy without coercing caller values."""
        if type(self.broker_xpub) is not str or not self.broker_xpub.strip():
            raise ValueError("financial_publication_broker_required")
        if type(self.refresh_interval) is not timedelta or self.refresh_interval <= timedelta(0):
            raise ValueError("financial_publication_refresh_interval_invalid")


class FinancialPublicationLifecycle:
    """Supervise dedicated receive and recovery for one exact explicitly supplied account.

    The caller retains the actual publisher lifetime. A successful snapshot is
    still a held partial journal view. Failed refreshes expose only the last
    historical snapshot, never a newly labelled current value. Automatic book
    discovery and installation into an executor remain separate application work.
    """

    def __init__(
        self,
        repository: AccountingRepository,
        scope: ExecutionAccountScope,
        publisher: MessagePublisher,
        options: FinancialPublicationLifecycleOptions,
    ) -> None:
        """Bind real dependencies without taking ownership of the publisher.

        Args:
            repository: Actual transactional financial source repository.
            scope: Explicit complete book from the application's real typed financial context.
            publisher: Actual borrowed publisher, including its original sequence tracker.
            options: Internal broker endpoint and local refresh cadence.
        """
        self._scope = ExecutionAccountScope.model_validate(scope.model_dump(mode="python"))
        self._options = FinancialPublicationLifecycleOptions(
            options.broker_xpub, options.refresh_interval
        )
        self._repository = repository
        self._publisher = publisher
        self._projection = VersionedContractAccountProjection(repository, self._scope)
        self._transport = FinancialPublicationTransport(publisher)
        self._context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._startup: asyncio.Task[None] | None = None
        self._cleanup: asyncio.Task[None] | None = None
        self._workers: set[asyncio.Task[None]] = set()
        self._wake = asyncio.Event()
        self._finished = asyncio.Event()
        self._closing = False
        self._status = FinancialPublicationLifecycleStatus.NEW
        self._failure_code: str | None = None
        self._last_snapshot: ContractAccountSnapshot | None = None

    @property
    def status(self) -> FinancialPublicationLifecycleStatus:
        """Return actual lifecycle state without inferring financial eligibility.

        Returns:
            The current resource and refresh state.
        """
        return self._status

    @property
    def failure_code(self) -> str | None:
        """Return only fixed local failure metadata, never exception text.

        Returns:
            A fixed code or None after the last complete successful refresh.
        """
        return self._failure_code

    @property
    def last_snapshot(self) -> ContractAccountSnapshot | None:
        """Copy the last successful view while preserving its original as_of.

        Returns:
            Historical retained journal material, or None before any successful refresh.
        """
        snapshot = self._last_snapshot
        return (
            None
            if snapshot is None
            else ContractAccountSnapshot.model_validate(snapshot.model_dump(mode="python"))
        )

    @property
    def snapshot(self) -> ContractAccountSnapshot | None:
        """Expose a current disposable view only while the lifecycle is active.

        Returns:
            The copied last successful view, or None while starting, stale or closing.
        """
        return (
            self.last_snapshot
            if self._status is FinancialPublicationLifecycleStatus.ACTIVE
            else None
        )

    async def start(self) -> None:
        """Subscribe, hydrate actual journals and drain pending intents before returning.

        Startup owns a distinct task so external cancellation can join it
        without making cleanup wait on the caller that awaits cleanup.
        """
        if self._status is not FinancialPublicationLifecycleStatus.NEW:
            raise FinancialPublicationLifecycleError("financial_publication_start_once")
        self._status = FinancialPublicationLifecycleStatus.STARTING
        startup_operation = self._initialize()
        try:
            self._startup = asyncio.create_task(
                startup_operation, name="financial-publication-startup"
            )
        except BaseException:
            startup_operation.close()
            self._status = FinancialPublicationLifecycleStatus.UNRESOLVED
            self._failure_code = "financial_publication_start_task_failed"
            self._finished.set()
            raise
        self._startup.add_done_callback(self._startup_done)
        try:
            await asyncio.shield(self._startup)
        except BaseException:
            await self.stop()
            raise

    async def _initialize(self) -> None:
        """Normalize startup failure after the original exception handler has unwound."""
        succeeded = False
        try:
            self._connect_subscriber()
            await self._hydrate()
            await self._drain()
            succeeded = True
        except Exception:
            self._mark_stale("financial_publication_startup_failed")
        if not succeeded:
            raise FinancialPublicationLifecycleError("financial_publication_startup_failed")
        if self._closing:
            raise FinancialPublicationLifecycleError("financial_publication_start_closed")
        self._status = FinancialPublicationLifecycleStatus.ACTIVE
        self._spawn_worker(self._receive_loop(), "financial-publication-receive")
        self._spawn_worker(self._refresh_loop(), "financial-publication-refresh")

    def _connect_subscriber(self) -> None:
        """Own a separate socket and subscribe only to this explicit wallet namespace."""
        self._context = zmq.asyncio.Context()
        raw = self._context.socket(zmq.SUB)
        self._subscriber = ValidatedSubscriber(raw)
        apply_hwm(raw, rcvhwm=HWM_AUDIT)
        raw.connect(self._options.broker_xpub)
        scope = self._scope
        for family in ("accounting.contract_position", "accounting.native_position.v2"):
            self._subscriber.subscribe(
                f"{family}.{scope.exchange}.{scope.environment}.{scope.wallet_public_id}."
            )

    def _spawn_worker(self, operation: Coroutine[object, object, None], name: str) -> None:
        """Register an owned coroutine before its completion callback can run.

        Args:
            operation: Concrete owned loop coroutine created only by this lifecycle.
            name: Fixed local task name without source data.
        """
        registered = asyncio.Event()

        async def run_registered() -> None:
            """Do not enter the operation until its actual task is tracked, including eager factories."""
            try:
                await registered.wait()
                await operation
            finally:
                operation.close()

        runner = run_registered()
        try:
            task = asyncio.create_task(runner, name=name)
        except BaseException:
            runner.close()
            operation.close()
            raise
        self._workers.add(task)
        task.add_done_callback(lambda completed: self._worker_done(completed, operation))
        registered.set()

    def _startup_done(self, task: asyncio.Task[None]) -> None:
        """Retrieve startup exceptions without retaining or logging their tracebacks.

        Args:
            task: Actual startup task owned by this lifecycle.
        """
        if not task.cancelled():
            task.exception()

    def _worker_done(
        self, task: asyncio.Task[None], operation: Coroutine[object, object, None]
    ) -> None:
        """Retrieve completion failures and refuse an active state with a missing worker.

        Args:
            task: Actual receive or refresh task whose exception must be retrieved.
            operation: Original operation, also closed if cancellation preceded its first task step.
        """
        operation.close()
        self._workers.discard(task)
        if not task.cancelled():
            task.exception()
        if not self._closing:
            self._status = FinancialPublicationLifecycleStatus.UNRESOLVED
            self._failure_code = "financial_publication_worker_stopped"
            self._finished.set()

    def _accept_snapshot(self, snapshot: ContractAccountSnapshot) -> None:
        """Replace the view only after actual complete hydration succeeded.

        Args:
            snapshot: Actual full retained-journal reader result with its original clock.
        """
        self._last_snapshot = ContractAccountSnapshot.model_validate(
            snapshot.model_dump(mode="python")
        )
        if self._status in {
            FinancialPublicationLifecycleStatus.ACTIVE,
            FinancialPublicationLifecycleStatus.STALE,
        }:
            self._status = FinancialPublicationLifecycleStatus.ACTIVE
            self._failure_code = None

    def _mark_stale(self, reason: str) -> None:
        """Preserve historical material while invalidating only the current view.

        Args:
            reason: Fixed internal reason literal, never source exception text.
        """
        if not self._closing and self._status is not FinancialPublicationLifecycleStatus.UNRESOLVED:
            self._status = FinancialPublicationLifecycleStatus.STALE
            self._failure_code = reason

    async def _hydrate(self) -> None:
        """Read actual complete retained journals, including cash without publication intents."""
        self._accept_snapshot(await self._projection.refresh())

    async def _drain(self) -> None:
        """Publish committed exact identities without a database lock across transport."""
        await publish_pending_versioned_financial_notifications(
            self._repository, self._scope, self._transport, self._publisher.tracker
        )

    async def _consume(self, topic: str, wire: bytes) -> None:
        """Bind original transport frames and exact literal profile before real hydration.

        Args:
            topic: Original separately received actual topic.
            wire: Original JSON envelope bytes from the dedicated internal socket.
        """
        message = parse_message(wire.decode("utf-8"), topic=topic)
        if (
            not isinstance(message, FinancialPublicationData)
            or message.payload.scope != self._scope
        ):
            return
        await self._projection.receive_message(topic, message)
        self._accept_snapshot(cast(ContractAccountSnapshot, self._projection.snapshot))

    async def _receive_loop(self) -> None:
        """Keep receive failures stale and retry without busy polling or exception logging."""
        subscriber = cast(ValidatedSubscriber, self._subscriber)
        while not self._closing:
            failed = False
            try:
                topic, wire = await subscriber.recv_multipart()
                await self._consume(topic, wire)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._mark_stale("financial_publication_receive_failed")
                failed = True
            if failed:
                await asyncio.sleep(self._options.refresh_interval.total_seconds())

    async def _refresh_loop(self) -> None:
        """Recover missed notifications and cash-only changes using actual journal reads."""
        while not self._closing:
            await self._wait_refresh()
            try:
                await self._hydrate()
                await self._drain()
            except asyncio.CancelledError:
                raise
            except Exception:
                self._mark_stale("financial_publication_refresh_failed")

    async def _wait_refresh(self) -> None:
        """Wait for the local cadence or a scheduling hint without claiming source freshness."""
        with suppress(TimeoutError):
            await asyncio.wait_for(
                self._wake.wait(), timeout=self._options.refresh_interval.total_seconds()
            )
        self._wake.clear()

    def request_refresh(self) -> None:
        """Wake the actual owner without treating a caller hint as source proof."""
        if self._status not in {
            FinancialPublicationLifecycleStatus.ACTIVE,
            FinancialPublicationLifecycleStatus.STALE,
        }:
            raise FinancialPublicationLifecycleError("financial_publication_not_running")
        self._wake.set()

    async def wait(self) -> None:
        """Expose unexpected worker termination to the external lifetime supervisor."""
        await self._finished.wait()
        if self._status is FinancialPublicationLifecycleStatus.UNRESOLVED:
            raise FinancialPublicationLifecycleError("financial_publication_owner_unresolved")

    async def stop(self) -> None:
        """Join owned work before disposing only owned SUB/context resources.

        Caller cancellation leaves the strongly referenced shielded cleanup
        running. No cancelled wait is described as completed cleanup.
        """
        current = asyncio.current_task()
        if current in self._workers or current is self._startup:
            self._mark_stale("financial_publication_self_join_refused")
            raise FinancialPublicationLifecycleError("financial_publication_self_join_refused")
        if self._cleanup is None:
            self._closing = True
            self._status = FinancialPublicationLifecycleStatus.CLOSING
            cleanup_operation = self._finish_close()
            try:
                self._cleanup = asyncio.create_task(
                    cleanup_operation, name="financial-publication-close"
                )
            except BaseException:
                cleanup_operation.close()
                self._status = FinancialPublicationLifecycleStatus.UNRESOLVED
                self._failure_code = "financial_publication_close_task_failed"
                self._finished.set()
                raise
            self._cleanup.add_done_callback(self._cleanup_done)
        failed = False
        try:
            await asyncio.shield(self._cleanup)
        except asyncio.CancelledError:
            if self._status is not FinancialPublicationLifecycleStatus.CLOSED:
                self._status = FinancialPublicationLifecycleStatus.UNRESOLVED
            raise
        except Exception:
            self._status = FinancialPublicationLifecycleStatus.UNRESOLVED
            self._failure_code = "financial_publication_cleanup_failed"
            failed = True
        if failed:
            raise FinancialPublicationLifecycleError("financial_publication_cleanup_failed")

    async def _finish_close(self) -> None:
        """Cancel and join every original task before releasing the dedicated receive resources."""
        if self._startup is not None:
            if not self._startup.done():
                self._startup.cancel()
            await asyncio.gather(self._startup, return_exceptions=True)
            self._startup = None
        while self._workers:
            tasks = tuple(self._workers)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._workers.difference_update(tasks)
        if self._subscriber is not None:
            self._subscriber.close()
            self._subscriber = None
        if self._context is not None:
            self._context.destroy(linger=0)
            self._context = None
        self._status = FinancialPublicationLifecycleStatus.CLOSED
        self._finished.set()

    def _cleanup_done(self, task: asyncio.Task[None]) -> None:
        """Retrieve cleanup failure and preserve unresolved ownership after an incomplete close.

        Args:
            task: Strongly retained actual cleanup task, independent of cancelled caller waits.
        """
        failed = task.cancelled() or task.exception() is not None
        if failed:
            self._status = FinancialPublicationLifecycleStatus.UNRESOLVED
            self._failure_code = "financial_publication_cleanup_failed"
            self._finished.set()
