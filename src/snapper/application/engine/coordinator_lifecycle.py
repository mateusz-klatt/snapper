"""Own coordinator tasks and join teardown before exposing a lifecycle outcome."""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Never
from typing import cast

from loguru import logger


def retain_failure(
    primary: BaseException | None, failure: BaseException, stage: str
) -> BaseException:
    """Keep the selected failure while reporting only a secondary error's type.

    Args:
        primary: Previously selected operational or cleanup failure.
        failure: Newly observed failure whose outcome has been retrieved.
        stage: Constant description of the failing lifecycle boundary.

    Returns:
        The first failure, or the newly observed failure if there was none.
    """
    if primary is None:
        return failure
    if failure is not primary:
        logger.warning("Coordinator {} secondary failure: {}", stage, type(failure).__name__)
    return primary


async def join_cleanup(task: asyncio.Task[BaseException | None]) -> BaseException | None:
    """Join shielded cleanup and then rethrow the first caller cancellation.

    Each cancellation while cleanup remains pending adds one recursive frame.
    Ordinary repeated cancellation is supported within Python's recursion limit;
    native synchronous resource destruction has no hard wall-clock deadline.

    Args:
        task: Independently scheduled cleanup or owner-join operation.

    Returns:
        The cleanup failure, if any.
    """
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancellation:
        try:
            if not task.done():
                await join_cleanup(task)
        finally:
            raise cancellation


async def cancel_and_join(
    workers: list[asyncio.Task[None]], primary: BaseException | None
) -> BaseException | None:
    """Cancel every pending worker before joining and retrieving every outcome.

    Args:
        workers: Every created child in stable creation order.
        primary: Previously selected lifecycle outcome.

    Returns:
        The selected failure, including a cleanup failure when no primary existed.
    """
    for task in workers:
        if not task.done():
            task.cancel()
    outcomes = await asyncio.gather(*workers, return_exceptions=True)
    for outcome in outcomes:
        if isinstance(outcome, BaseException) and not isinstance(outcome, asyncio.CancelledError):
            primary = retain_failure(primary, outcome, "worker cleanup")
    return primary


def validate_completed_workers(done: list[asyncio.Task[None]]) -> None:
    """Raise the first real failure before considering child cancellation.

    Args:
        done: Completed owned tasks in stable creation order, listener first.
    """
    for task in done:
        if not task.cancelled():
            failure = task.exception()
            if failure is not None:
                raise failure
    for task in done:
        if task.cancelled():
            task.result()


async def supervise_workers(workers: list[asyncio.Task[None]]) -> Never:
    """Observe required intake and preserve optional normal worker completion.

    The first worker is the listener. At each completion observation, prefer its
    real failure, then other real failures in creation order, then cancellation,
    and finally synthesized unexpected listener return. This orders the observed
    snapshot, not wall-clock failures. Finished optional workers leave the wait set.
    The required listener keeps the wait set nonempty until an outcome raises;
    explicit stop cancels the owning task rather than returning from supervision.

    Args:
        workers: Owned tasks, with the required listener first.

    Returns:
        Never returns; raises the observed lifecycle failure or cancellation.
    """
    pending = set(workers)
    while True:
        await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        done = [task for task in workers if task.done()]
        validate_completed_workers(done)
        if workers[0].done():
            raise RuntimeError("TraderCoordinator signal listener exited before stop")
        pending.difference_update(done)


class CoordinatorLifetime:
    """Serialize one coordinator start and retain one resource-cleanup outcome.

    A successful idle cleanup may precede the first start. Reusing a started
    coordinator or starting during unfinished/failed idle cleanup is rejected.
    Cleanup runs on the owning event loop, never awaits the owner, and is shared
    by external stop callers without replaying the owner's operational failure.
    """

    def __init__(self) -> None:
        """Initialize ownership without acquiring resources or scheduling tasks."""
        self.owner: asyncio.Task[object] | None = None
        self.workers: list[asyncio.Task[None]] = []
        self.stop_requested = False
        self.joining_workers = False
        self.finishing = False
        self.started = False
        self._cancellation_baseline = 0
        self.cleanup_task: asyncio.Task[BaseException | None] | None = None

    def begin(self) -> None:
        """Claim the first start after any successful idle cleanup has completed."""
        if self.started:
            raise RuntimeError("TraderCoordinator instances support only one start")
        if self.cleanup_task is not None:
            if not self.cleanup_task.done():
                raise RuntimeError("TraderCoordinator cleanup is still running")
            failure = self.cleanup_task.result()
            if failure is not None:
                raise failure
        self.owner = asyncio.current_task()
        self.started = True
        self.stop_requested = False
        self.cleanup_task = None

    async def run(
        self,
        operation: Callable[[], Awaitable[None]],
        cleanup: Callable[[], BaseException | None],
    ) -> None:
        """Keep startup, workers and resource finalization within one owner.

        Deliver any pending caller cancellation before acquiring resources, then
        distinguish previously handled cancellation counts from new requests.
        A stop during that checkpoint prevents initialization from resuming.

        Args:
            operation: Existing settings, setup, recovery and trading sequence.
            cleanup: Synchronous destruction of coordinator-owned resources only.
        """
        self.begin()
        primary: BaseException | None = None
        try:
            await asyncio.sleep(0)
            self._cancellation_baseline = cast(asyncio.Task[object], self.owner).cancelling()
            if self.stop_requested:
                raise asyncio.CancelledError("TraderCoordinator stop requested")
            await operation()
        except BaseException as exc:
            primary = exc
            raise
        finally:
            self.finishing = True
            try:
                failure = await join_cleanup(self.ensure_cleanup(cleanup))
                if failure is not None:
                    failure = retain_failure(primary, failure, "resource cleanup")
                    if primary is None:
                        raise failure
            finally:
                self.owner = None
                if primary is not None:
                    raise primary

    def ensure_cleanup(
        self, cleanup: Callable[[], BaseException | None]
    ) -> asyncio.Task[BaseException | None]:
        """Create cleanup once, caching both successful and failed outcomes.

        Args:
            cleanup: Same-loop resource finalizer that reports ordinary failures.

        Returns:
            The task owning this generation's resource finalization.
        """
        if self.cleanup_task is None:
            self.cleanup_task = asyncio.create_task(self._cleanup(cleanup))
        return self.cleanup_task

    async def _cleanup(self, cleanup: Callable[[], BaseException | None]) -> BaseException | None:
        """Run native destruction before yielding to publish its cached outcome.

        The finalizer runs synchronously on the resource-owning loop. A completion
        checkpoint keeps the independently scheduled cleanup task joinable while
        stop callers observe cancellation or await its cached result.

        Args:
            cleanup: Finalizer that attempts each resource independently.

        Returns:
            The cached first cleanup failure, if any.
        """
        return await asyncio.sleep(0, result=cleanup())

    def request_stop(self) -> None:
        """Cancel an active owner once without interrupting its joined finalizers."""
        if self.stop_requested:
            return
        self.stop_requested = True
        owner = self.owner
        if (
            owner is not None
            and not owner.done()
            and not self.joining_workers
            and not self.finishing
            and owner.cancelling() <= self._cancellation_baseline
        ):
            owner.cancel()

    async def stop(self, cleanup: Callable[[], BaseException | None]) -> None:
        """Request owner termination, join it, and expose only cleanup failure.

        A stop invoked by the owner unwinds that owner through its finally block.
        An owned child requests termination and returns without awaiting an owner
        that must join it. External callers wait through teardown; cancelling such
        a waiter does not cancel the owner or resource cleanup a second time.

        Args:
            cleanup: Same resource finalizer used by the start owner.
        """
        current = asyncio.current_task()
        owner = self.owner
        if owner is not None and current is owner:
            self.stop_requested = True
            raise asyncio.CancelledError("TraderCoordinator stop requested")
        self.request_stop()
        if current in self.workers:
            return
        if owner is not None and not owner.done():
            waiter = asyncio.create_task(self._join_owner(owner, cleanup))
            failure = await join_cleanup(waiter)
        else:
            failure = await join_cleanup(self.ensure_cleanup(cleanup))
        if failure is not None:
            raise failure

    async def _join_owner(
        self, owner: asyncio.Task[object], cleanup: Callable[[], BaseException | None]
    ) -> BaseException | None:
        """Retrieve the owner's outcome without replaying it to later stop callers.

        Args:
            owner: Start task whose children must finish before resources close.
            cleanup: Finalizer used only if the owner has not already scheduled it.

        Returns:
            Resource cleanup failure independently of the operational outcome.
        """
        await asyncio.gather(owner, return_exceptions=True)
        return await join_cleanup(self.ensure_cleanup(cleanup))
