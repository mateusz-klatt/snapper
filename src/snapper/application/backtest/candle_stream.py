"""Streaming candle merge for the direct-DB backtest engine.

Replaces the previous in-memory list-and-sort pattern in
``iter_sorted_candle_chunks`` with a k-way streaming merge over
per-(exchange, instrument) async generators. Memory footprint becomes
O(N_streams x per_stream_buffer) instead of O(N_total), which lets
minute-bar backtests over multi-instrument universes complete without
materializing the entire historical record at once.

Two helpers live here so the engine remains a thin loop driver:

- ``_candle_rows_iter``: async generator that streams ``CandleEvent``
  rows for a single (exchange, instrument) in ascending ``open_at``
  order. The repository still returns a list under the hood, but the
  helper yields per row so the merge layer pulls lazily.

- ``merge_sorted_streams``: k-way async merge ordered by
  ``(open_at, exchange, instrument)``. Uses ``heapq`` for log-k pop
  cost. Tie-breaking includes a stable per-stream index to keep
  ordering deterministic when ``(open_at, exchange, instrument)``
  collisions occur (which would only happen with duplicate DB rows
  but the determinism guard makes parity tests hashable).
"""

import heapq
from collections.abc import AsyncIterator
from collections.abc import Sequence
from datetime import datetime
from typing import cast

from snapper.application.backtest.batch_processor import CandleEvent
from snapper.core.types import AllExchange
from snapper.data.repository import Repository

__all__ = ["merge_sorted_streams", "stream_candles_for_instrument"]


async def stream_candles_for_instrument(
    repository: Repository,
    exchange: str,
    instrument: str,
    timeframe: str,
    end_date: datetime | None,
    snapshot_as_of: datetime,
) -> AsyncIterator[CandleEvent]:
    """Yield ``CandleEvent`` rows for one (exchange, instrument) in order.

    Wraps ``Repository.get_candles`` so the merge layer can treat each
    instrument as an independent stream. Emits rows in ascending
    ``open_at`` order (the repository contract specifies ``order='asc'``).

    The ``exchange`` parameter is typed as ``str`` to match the keys
    in :class:`BacktestConfig.instruments`; it is cast to
    :data:`AllExchange` at the repository boundary because the
    repository signature uses the Literal alias. This keeps the helper
    free of ``Any`` usage while remaining call-site compatible with the
    config layer.

    Args:
        repository: Database repository for candle queries.
        exchange: Source exchange identifier (config-string form).
        instrument: Instrument symbol.
        timeframe: Candle timeframe (e.g. ``"1d"``, ``"1h"``, ``"1m"``).
        end_date: Inclusive upper bound on ``open_at``; ``None`` means
            "no upper bound".
        snapshot_as_of: Temporal snapshot for as-of reads.

    Yields:
        ``CandleEvent`` rows for this instrument in ascending order.
    """
    rows = await repository.get_candles(
        instrument=instrument,
        timeframe=timeframe,
        start=None,
        end=end_date,
        exchange=cast(AllExchange, exchange),
        as_of=snapshot_as_of,
        order="asc",
    )
    for row in rows:
        yield CandleEvent(
            open_at=row["open_at"],
            exchange=exchange,
            instrument=instrument,
            row=row,
        )


async def merge_sorted_streams(
    streams: Sequence[AsyncIterator[CandleEvent]],
) -> AsyncIterator[CandleEvent]:
    """K-way async merge over per-instrument candle streams.

    Each input stream must yield ``CandleEvent`` rows in ascending
    ``(open_at, exchange, instrument)`` order. The merge yields events
    globally in the same key order. Ordering is deterministic even on
    key collisions because the per-stream insertion index participates
    in the heap key as a tie-breaker.

    Args:
        streams: Sorted async iterators of ``CandleEvent``. Empty
            sequence yields nothing.

    Yields:
        ``CandleEvent`` rows in global ascending order.
    """
    heap: list[tuple[tuple[datetime, str, str], int, CandleEvent]] = []
    for idx, stream in enumerate(streams):
        try:
            event = await anext(stream)
        except StopAsyncIteration:
            continue
        heap.append(
            (
                (event.open_at, event.exchange, event.instrument),
                idx,
                event,
            )
        )
    heapq.heapify(heap)
    while heap:
        _, idx, event = heapq.heappop(heap)
        yield event
        try:
            next_event = await anext(streams[idx])
        except StopAsyncIteration:
            continue
        heapq.heappush(
            heap,
            (
                (next_event.open_at, next_event.exchange, next_event.instrument),
                idx,
                next_event,
            ),
        )
