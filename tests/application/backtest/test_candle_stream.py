"""Unit tests for the streaming candle merge helpers.

Covers :mod:`snapper.application.backtest.candle_stream` —
``stream_candles_for_instrument`` and ``merge_sorted_streams``. Tests
are scoped narrowly to the merge / iteration semantics so the engine
layer's existing tests cover behavioural integration.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.backtest.batch_processor import CandleEvent
from snapper.application.backtest.candle_stream import merge_sorted_streams
from snapper.application.backtest.candle_stream import stream_candles_for_instrument
from snapper.data.repository_types import CandleRow

NOW = datetime(2026, 5, 8, 12, 0, 0, tzinfo=UTC)


def _row(ts: datetime, close: float) -> CandleRow:
    return cast(
        CandleRow,
        {
            "open_at": ts,
            "open": close - 1.0,
            "high": close + 1.0,
            "low": close - 2.0,
            "close": close,
            "volume": 1.0,
            "vwap": close,
            "instrument": "X",
            "timeframe": "1h",
        },
    )


def _event(ts: datetime, exchange: str, instrument: str) -> CandleEvent:
    return CandleEvent(
        open_at=ts,
        exchange=exchange,
        instrument=instrument,
        row=_row(ts, 100.0),
    )


async def _stream_from(events: list[CandleEvent]) -> AsyncIterator[CandleEvent]:
    for ev in events:
        yield ev


class TestStreamCandlesForInstrument:
    """Tests for ``stream_candles_for_instrument`` async generator."""

    @pytest.mark.asyncio
    async def test_yields_each_repository_row(self) -> None:
        """Each row from the repository becomes one yielded CandleEvent."""
        repo = AsyncMock()
        t1 = NOW
        t2 = NOW + timedelta(hours=1)
        repo.get_candles = AsyncMock(return_value=[_row(t1, 100.0), _row(t2, 101.0)])

        out: list[CandleEvent] = []
        async for ev in stream_candles_for_instrument(
            repository=repo,
            exchange="kraken",
            instrument="BTC-USD",
            timeframe="1h",
            end_date=t2,
            snapshot_as_of=NOW,
        ):
            out.append(ev)
        assert len(out) == 2
        assert out[0].open_at == t1
        assert out[1].open_at == t2
        assert out[0].instrument == "BTC-USD"
        assert out[0].exchange == "kraken"

    @pytest.mark.asyncio
    async def test_empty_repository_yields_nothing(self) -> None:
        """Empty repository result yields no events."""
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=[])

        out: list[CandleEvent] = []
        async for ev in stream_candles_for_instrument(
            repository=repo,
            exchange="kraken",
            instrument="BTC-USD",
            timeframe="1h",
            end_date=None,
            snapshot_as_of=NOW,
        ):
            out.append(ev)
        assert out == []

    @pytest.mark.asyncio
    async def test_calls_repository_with_asc_order(self) -> None:
        """Helper passes order='asc' to honour the repository contract."""
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=[])

        async for _ in stream_candles_for_instrument(
            repository=repo,
            exchange="kraken",
            instrument="BTC-USD",
            timeframe="1h",
            end_date=None,
            snapshot_as_of=NOW,
        ):
            pass
        repo.get_candles.assert_awaited_once()
        kwargs = repo.get_candles.await_args.kwargs
        assert kwargs["order"] == "asc"
        assert kwargs["start"] is None


class TestMergeSortedStreams:
    """Tests for ``merge_sorted_streams`` k-way merge."""

    @pytest.mark.asyncio
    async def test_empty_input_yields_nothing(self) -> None:
        """No input streams produces no merged events."""
        out: list[CandleEvent] = []
        async for ev in merge_sorted_streams([]):
            out.append(ev)
        assert out == []

    @pytest.mark.asyncio
    async def test_single_stream_passes_through(self) -> None:
        """Single-stream merge yields the source events in order."""
        events = [
            _event(NOW, "kraken", "BTC-USD"),
            _event(NOW + timedelta(hours=1), "kraken", "BTC-USD"),
        ]
        out: list[CandleEvent] = []
        async for ev in merge_sorted_streams([_stream_from(events)]):
            out.append(ev)
        assert [e.open_at for e in out] == [e.open_at for e in events]

    @pytest.mark.asyncio
    async def test_two_streams_interleaved(self) -> None:
        """Two streams interleave by ascending open_at."""
        t1 = NOW
        t2 = NOW + timedelta(hours=1)
        t3 = NOW + timedelta(hours=2)
        s1 = _stream_from([_event(t1, "kraken", "BTC-USD"), _event(t3, "kraken", "BTC-USD")])
        s2 = _stream_from([_event(t2, "kraken", "ETH-USD")])
        out: list[CandleEvent] = []
        async for ev in merge_sorted_streams([s1, s2]):
            out.append(ev)
        assert [(e.open_at, e.instrument) for e in out] == [
            (t1, "BTC-USD"),
            (t2, "ETH-USD"),
            (t3, "BTC-USD"),
        ]

    @pytest.mark.asyncio
    async def test_collision_breaks_ties_by_exchange_then_instrument(self) -> None:
        """Same-timestamp events sort by (exchange, instrument)."""
        s1 = _stream_from([_event(NOW, "kraken", "ETH-USD")])
        s2 = _stream_from([_event(NOW, "binance", "BTC-USD")])
        s3 = _stream_from([_event(NOW, "kraken", "BTC-USD")])
        out: list[CandleEvent] = []
        async for ev in merge_sorted_streams([s1, s2, s3]):
            out.append(ev)
        assert [(e.exchange, e.instrument) for e in out] == [
            ("binance", "BTC-USD"),
            ("kraken", "BTC-USD"),
            ("kraken", "ETH-USD"),
        ]

    @pytest.mark.asyncio
    async def test_unbalanced_streams_drain_correctly(self) -> None:
        """Unbalanced stream sizes still drain in global order."""
        big = _stream_from(
            [_event(NOW + timedelta(minutes=i), "kraken", "BTC-USD") for i in range(10)]
        )
        small = _stream_from([_event(NOW + timedelta(minutes=5, seconds=30), "kraken", "ETH-USD")])
        out: list[CandleEvent] = []
        async for ev in merge_sorted_streams([big, small]):
            out.append(ev)
        assert len(out) == 11
        assert sum(1 for e in out if e.instrument == "BTC-USD") == 10
        assert sum(1 for e in out if e.instrument == "ETH-USD") == 1
        for i in range(len(out) - 1):
            assert out[i].open_at <= out[i + 1].open_at

    @pytest.mark.asyncio
    async def test_one_empty_stream_still_drains_others(self) -> None:
        """An empty stream does not block the merge of others."""
        empty = _stream_from([])
        full = _stream_from([_event(NOW, "kraken", "BTC-USD")])
        out: list[CandleEvent] = []
        async for ev in merge_sorted_streams([empty, full]):
            out.append(ev)
        assert len(out) == 1
        assert out[0].instrument == "BTC-USD"
