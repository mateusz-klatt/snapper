"""ZmqReplayEngine — backtest engine driven by an in-process ZMQ replay broker.

Wires together the building blocks
:func:`snapper.application.backtest.endpoints.allocate_replay_endpoints` for an
  ephemeral per-run XPUB/XSUB broker (xpub_verbose=True).
:class:`snapper.application.backtest.drain.DrainCoordinator` for end-of-stream
  parity between publisher and strategy.
:func:`snapper.strategies.backtest_strategy.make_backtest_replay_strategy` for
  the replay-shimmed strategy (skips heartbeat, market-only subs, echo-ack
  detection in _listen_loop).
:class:`snapper.messaging.publishers.replay_publisher.ReplayPublisher` for the
  echo-ack handshake + candle streaming.
Field-for-field parity with ``DirectDbEngine.run`` on the same config, but
candles travel publisher → broker → strategy via ZMQ so the strategy's
production message-bus path is exercised end-to-end.
Cancellation, drain, and broker cleanup are bounded
``asyncio.wait({publisher_task, strategy._listen_task}, FIRST_COMPLETED)``
  whichever finishes first decides the outcome; exceptions bubble up.
``finally``: cancel both tasks, ``asyncio.wait(timeout=2.0)`` with leak
  logging on tasks that refuse to terminate, then strategy.stop() and
  broker.stop() in that order so the strategy's socket is closed before
  the broker tears down its sockets.
Empty-instrument config fast-fails with ValueError before any broker is
allocated so misconfigured runs do not consume ephemeral ports.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from typing import Any
from typing import cast

from loguru import logger

from snapper.application.backtest.batch_processor import process_time_batch
from snapper.application.backtest.cancel import CancelProbe
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.drain import DrainCoordinator
from snapper.application.backtest.endpoints import allocate_replay_endpoints
from snapper.application.backtest.progress import BacktestProgressEmitter
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.replay_publisher import ReplayPublisher
from snapper.strategies.backtest_strategy import BacktestReplayState
from snapper.strategies.backtest_strategy import make_backtest_replay_strategy
from snapper.strategies.factory import StrategyFactory
from snapper.strategies.models import StrategyConfig

_BROKER_SUB_TIMEOUT_S: float = 5.0


def _strategy_task(strategy: object | None, attr_name: str) -> asyncio.Task[None] | None:
    """Fetch one optional cleanup task from the strategy-like object."""
    if strategy is None:
        return None
    return cast(asyncio.Task[None] | None, getattr(strategy, attr_name, None))


def _cleanup_tasks(
    publisher_task: asyncio.Task[None] | None,
    strategy: object | None,
) -> set[asyncio.Task[None]]:
    """Collect every task that should be cancelled before teardown."""
    return {
        task
        for task in (
            publisher_task,
            _strategy_task(strategy, "_listen_task"),
            _strategy_task(strategy, "_heartbeat_task"),
        )
        if task is not None
    }


def _cancel_cleanup_tasks(tasks: set[asyncio.Task[None]]) -> None:
    """Cancel each still-running cleanup task."""
    for task in tasks:
        if not task.done():
            task.cancel()


def _log_cleanup_wait_results(
    done: set[asyncio.Task[None]],
    pending: set[asyncio.Task[None]],
) -> None:
    """Emit diagnostics for leaked tasks and already-surfaced exceptions."""
    for task in pending:
        logger.error(
            "cleanup: task {!r} did not terminate within 2s after cancel — leak",
            task.get_name(),
        )
    for task in done:
        if not task.cancelled() and task.exception() is not None:
            logger.debug(
                "cleanup: task {!r} finished with exception (already surfaced)",
                task.get_name(),
            )


async def _wait_for_cleanup_tasks(tasks: set[asyncio.Task[None]]) -> None:
    """Wait up to two seconds for cancelled tasks, then log the outcome."""
    if not tasks:
        return
    done, pending = await asyncio.wait(tasks, timeout=2.0)
    _log_cleanup_wait_results(done=done, pending=pending)


async def _stop_cleanup_target(
    stop_call: Callable[[], Awaitable[object]],
    timeout_message: str,
    exception_message: str,
) -> None:
    """Run one stop coroutine under the shared teardown timeout policy."""
    try:
        await asyncio.wait_for(stop_call(), timeout=5.0)
    except TimeoutError:
        logger.error(timeout_message)
    except Exception:
        logger.exception(exception_message)


class ZmqReplayEngine:
    """Replay engine that streams DB candles through a per-run ZMQ broker.

    Same external contract as ``DirectDbEngine``: ``run(public_id, config,
    collector) → (portfolio, latest_closes)``.
    """

    def __init__(
        self,
        repository: Repository,
        snapshot_as_of: datetime,
        cancel_probe: CancelProbe | None = None,
        emitter: BacktestProgressEmitter | None = None,
    ) -> None:
        """Wire the engine to a repository + bitemporal anchor.

        Args:
            repository: Source of historical candles.
            snapshot_as_of: Bitemporal snapshot for all DB reads.
            cancel_probe: Optional shared CancelProbe (
                Step 3). When supplied, the strategy mixin's ``_listen_loop``
                calls ``await probe.check()`` per processed candle so a
                cancel_requested status is detected within ``cancel_poll_ms``
                + ``probe_timeout_s``.
            emitter: Optional progress emitter shared with
                ``DirectDbEngine`` so both replay modes emit identical
                WS progress events.
        """
        self._repository = repository
        self._snapshot_as_of = snapshot_as_of
        self._cancel_probe = cancel_probe
        self._emitter = emitter

    async def run(
        self,
        run_public_id: str,
        config: BacktestConfig,
        collector: ResultCollector,
    ) -> tuple[PortfolioTracker, dict[str, float]]:
        """Execute a backtest run via the ZMQ replay path.

        Args:
            run_public_id: Public ID of the backtest run (also used as the
                strategy instance name suffix).
            config: Backtest configuration; ``config.instruments`` must be
                non-empty.
            collector: Result collector buffering signals/trades/equity.

        Returns:
            Tuple of (final portfolio state, latest close prices), matching
            the DirectDbEngine signature so the runner can swap engines
            without changing its post-run path.

        Raises:
            ValueError: If ``config.instruments`` is empty (fast-fail).
            BacktestReadinessTimeoutError: If the strategy never ACKs.
            BacktestDrainTimeoutError: If the strategy stalls mid-stream.
            asyncio.CancelledError: If the engine is cancelled mid-run.
        """
        if not any(config.instruments.values()):
            raise ValueError("config.instruments is empty — nothing to replay")

        broker = None
        strategy = None
        publisher_task: asyncio.Task[None] | None = None
        try:
            broker, endpoints = await allocate_replay_endpoints()
            drain = DrainCoordinator()
            expected_topics = frozenset(
                f"market.{exchange}.{instr}.candles.{config.timeframe}"
                for exchange, instruments in config.instruments.items()
                for instr in instruments
            )

            all_instruments: list[str] = []
            for instruments in config.instruments.values():
                all_instruments.extend(instruments)

            strategy_config = StrategyConfig(
                name=f"bt_{run_public_id[:8]}",
                strategy_class=config.strategy_class,
                inputs=sorted(expected_topics),
                outputs=all_instruments,
                exchange=cast(Any, "paper"),
                params=dict(config.strategy_params),
            )

            state = BacktestReplayState(
                run_public_id=run_public_id,
                config=config,
                snapshot_as_of=self._snapshot_as_of,
                pending_batch=[],
                portfolio=PortfolioTracker(cash=config.initial_balance),
                latest_closes={},
                collector=collector,
                tracker=SequenceTracker(),
                expected_topics=expected_topics,
                cancel_probe=self._cancel_probe,
                emitter=self._emitter,
            )

            inner_class = StrategyFactory.STRATEGY_CLASSES[config.strategy_class]
            strategy = make_backtest_replay_strategy(
                inner_class=inner_class,
                inner_config=strategy_config,
                state=state,
                drain=drain,
                local_xsub=endpoints.xsub,
                local_xpub=endpoints.xpub,
            )
            await strategy.start()

            async with asyncio.timeout(_BROKER_SUB_TIMEOUT_S):
                await broker.wait_for_subscription(b"market.")

            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=self._repository,
                config=config,
                snapshot_as_of=self._snapshot_as_of,
                drain=drain,
                subscriber_ready=state.subscriber_ready,
            )
            publisher_task = asyncio.create_task(publisher.start())

            listen_task = strategy._listen_task
            assert listen_task is not None
            tasks: set[asyncio.Task[Any]] = {publisher_task, listen_task}
            done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

            self._log_completion(publisher_task, listen_task, drain)

            cancelled = any(t.cancelled() for t in done)
            exceptions = [t.exception() for t in done if not t.cancelled() and t.exception()]
            if exceptions:
                primary = exceptions[0]
                for other in exceptions[1:]:
                    logger.error("secondary task exception", exc_info=other)
                assert primary is not None
                raise primary
            if cancelled:
                raise asyncio.CancelledError

            if state.pending_batch:
                await process_time_batch(
                    batch=state.pending_batch,
                    run_public_id=state.run_public_id,
                    config=state.config,
                    strategy=strategy,
                    portfolio=state.portfolio,
                    latest_closes=state.latest_closes,
                    collector=state.collector,
                    tracker=state.tracker,
                    snapshot_as_of=self._snapshot_as_of,
                    emitter=self._emitter,
                )
                state.pending_batch = []

            return state.portfolio, dict(state.latest_closes)
        finally:
            await self._cleanup(publisher_task, strategy, broker)

    @staticmethod
    def _log_completion(
        publisher_task: asyncio.Task[None] | None,
        listen_task: asyncio.Task[None],
        drain: DrainCoordinator,
    ) -> None:
        """Diagnostic log so an operator can tell publisher-stall from strategy-death."""
        publisher_done = publisher_task is not None and publisher_task.done()
        publisher_exc: BaseException | None = None
        if publisher_done and publisher_task is not None and not publisher_task.cancelled():
            publisher_exc = publisher_task.exception()
        listen_done = listen_task.done()
        listen_exc: BaseException | None = None
        if listen_done and not listen_task.cancelled():
            listen_exc = listen_task.exception()
        logger.info(
            "engine: task completion — publisher done={} exc={} | listen done={} exc={} | "
            "drain published={} processed={}",
            publisher_done,
            publisher_exc,
            listen_done,
            listen_exc,
            drain.published_count,
            drain.processed_count,
        )

    @staticmethod
    async def _cleanup(
        publisher_task: asyncio.Task[None] | None,
        strategy: Any,
        broker: Any,
    ) -> None:
        """Bounded cleanup: cancel tasks, wait ≤2s, log leaks, stop strategy + broker.

        Uses ``asyncio.wait(tasks, timeout=2.0)`` (NOT
        ``asyncio.wait_for(asyncio.shield(t), 2.0)`` which keeps cancelled
        tasks alive past the timeout and silently leaks). Tasks still
        running after 2s are logged with their name so an operator can
        investigate; ``cancel()`` already fired so they are detached but
        will eventually finish.
        """
        tasks = _cleanup_tasks(publisher_task, strategy)
        _cancel_cleanup_tasks(tasks)
        await _wait_for_cleanup_tasks(tasks)
        if strategy is not None:
            await _stop_cleanup_target(
                strategy.stop,
                "cleanup: strategy.stop() exceeded 5s — proceeding to broker.stop "
                "to avoid leaking the per-run broker",
                "cleanup: strategy.stop() raised — continuing teardown",
            )
        if broker is not None:
            await _stop_cleanup_target(
                broker.stop,
                "cleanup: broker.stop() exceeded 5s — ports may stay bound until "
                "the worker process exits",
                "cleanup: broker.stop() raised — continuing teardown",
            )
