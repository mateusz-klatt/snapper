"""Drain coordination + bounded readiness primitives for the ZMQ replay engine.

The replay engine streams candles from a publisher into a strategy that
runs in the same event loop, connected via an XPUB/XSUB broker. The engine
needs three guarantees that this module provides:

1. **Drain bookkeeping**: every published candle is processed by the strategy
   before the run is allowed to finish. ``DrainCoordinator`` tracks
   ``published_count`` and ``processed_count`` and exposes a ``drained``
   ``asyncio.Event`` that fires when the publisher has finished AND every
   published candle has been processed.

2. **Drain timeout** (``BacktestDrainTimeoutError``): raised by the publisher
   when the drained event does not fire within the engine's drain budget.
   Carries the published / processed counters in its message so an operator
   can tell publisher-stall from strategy-death.

3. **Readiness timeout** (``BacktestReadinessTimeoutError``): raised by the
   publisher when the strategy fails to ack the warmup handshake within
   the bounded retry budget.

The coordinator deliberately uses simple counters and an Event rather
than a Queue: there is exactly one publisher coroutine and one strategy
coroutine in the same event loop; there is no contention to mediate.
"""

import asyncio
from dataclasses import dataclass
from dataclasses import field


class BacktestReadinessTimeoutError(RuntimeError):
    """Strategy failed to ack the warmup handshake within the retry budget.

    Raised by ``ReplayPublisher.start()`` after exhausting
    ``WARMUP_MAX_RETRIES`` attempts. The message includes the topics that
    were retried so an operator can spot a misconfigured subscription set.
    """


class BacktestDrainTimeoutError(RuntimeError):
    """Drain did not complete within the engine's drain budget.

    Raised by ``ReplayPublisher.start()`` after the publisher has finished
    streaming and ``mark_done_publishing()`` has been called but the
    strategy has not processed the remaining buffered candles in time.
    The message carries ``published_count`` and ``processed_count`` so an
    operator can tell publisher-stall from strategy-death from listener
    cancellation.
    """


@dataclass
class DrainCoordinator:
    """Coordinate end-of-stream drain between publisher and strategy.

    Single-publisher / single-strategy semantics in the same event loop.
    Counters are plain ints because asyncio is cooperative and increments
    happen between awaits — there is no concurrent mutation to reconcile.
    """

    published_count: int = 0
    processed_count: int = 0
    publishing_done: bool = False
    drained: asyncio.Event = field(default_factory=asyncio.Event)

    def on_publish(self) -> None:
        """Record one outbound candle from the publisher."""
        self.published_count += 1

    def on_processed(self) -> None:
        """Record one candle consumed by the strategy.

        When the publisher has already signalled ``mark_done_publishing()``
        and the processed counter catches up to the published counter, the
        ``drained`` event fires so the publisher can return cleanly.
        """
        self.processed_count += 1
        if self.publishing_done and self.processed_count >= self.published_count:
            self.drained.set()

    def mark_done_publishing(self) -> None:
        """Signal that no further candles will be published.

        If the strategy has already caught up at this point, the drained
        event fires immediately. Otherwise it fires when ``on_processed``
        observes parity.
        """
        self.publishing_done = True
        if self.processed_count >= self.published_count:
            self.drained.set()
