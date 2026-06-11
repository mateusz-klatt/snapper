"""Tests for the Kraken Equities candle fragmentation repair service."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from uuid import UUID

import pytest

from snapper.application.maintenance.equity_candle_repair import EquityCandleRepairService
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import EquityCandleRepairBatch
from snapper.data.repository_types import EquityCandleRepairRow

_START = datetime(2026, 1, 2, tzinfo=UTC)
_OPEN = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
_REPAIR_TIME = datetime(2026, 1, 2, 15, tzinfo=UTC)
_INSTRUMENT_ID = "00000000-0000-0000-0000-000000000001"
_ONE_HOUR = timedelta(hours=1)


class _FakeRepairRepository:
    """Narrow repository double for chunking and batching tests."""

    def __init__(
        self,
        repairs_by_start: dict[datetime, list[EquityCandleRepairRow]],
        upsert_return_counts: list[int] | None = None,
        unreconstructable_by_start: dict[datetime, int] | None = None,
    ) -> None:
        self._repairs_by_start = repairs_by_start
        self._upsert_return_counts = (
            upsert_return_counts if upsert_return_counts is not None else []
        )
        self._unreconstructable_by_start = (
            unreconstructable_by_start if unreconstructable_by_start is not None else {}
        )
        self.repair_calls: list[tuple[datetime, datetime]] = []
        self.upsert_batches: list[list[CandleUpsertRow]] = []

    async def get_equity_candle_repairs_from_trades(
        self, *, start: datetime, end: datetime
    ) -> EquityCandleRepairBatch:
        """Return the fixture repair batch for the requested chunk.

        Args:
            start: Inclusive chunk start.
            end: Exclusive chunk end.

        Returns:
            Configured repair batch for ``start``.
        """
        self.repair_calls.append((start, end))
        return EquityCandleRepairBatch(
            repairs=list(self._repairs_by_start.get(start, [])),
            unreconstructable_minutes=self._unreconstructable_by_start.get(start, 0),
        )

    async def upsert_candles(self, rows: list[CandleUpsertRow]) -> int:
        """Capture upsert batches and return configured write counts.

        Args:
            rows: Upsert rows sent by the service.

        Returns:
            Next configured write count, or the batch length by default.
        """
        self.upsert_batches.append(list(rows))
        if self._upsert_return_counts:
            return self._upsert_return_counts.pop(0)
        return len(rows)


def _repair_row(
    open_at: datetime,
    *,
    close: float = 2.0,
) -> EquityCandleRepairRow:
    """Build one corrected repair row for service tests."""
    return EquityCandleRepairRow(
        instrument_public_id=_INSTRUMENT_ID,
        open_at=open_at,
        timeframe="1m",
        open=1.0,
        high=3.0,
        low=0.5,
        close=close,
        volume=5.0,
        vwap=2.0,
        trades=5,
    )


def _service(fake: _FakeRepairRepository) -> EquityCandleRepairService:
    """Build a service using the fake repository."""
    return EquityCandleRepairService(cast(Repository, fake))


class TestEquityCandleRepairService:
    """Service tests for chunking, dry runs, writes, and idempotency."""

    def test_dry_run_chunks_range_and_writes_nothing(self) -> None:
        """Test dry-run chunking counts candidates without upserts.

        Given: Two one-hour chunks with three configured repair rows,
        When: The service runs in dry-run mode,
        Then: It reports all candidates and never calls ``upsert_candles``.
        """
        second_chunk = _START + _ONE_HOUR
        fake = _FakeRepairRepository(
            {
                _START: [_repair_row(_OPEN)],
                second_chunk: [
                    _repair_row(_OPEN + _ONE_HOUR),
                    _repair_row(_OPEN + _ONE_HOUR + timedelta(minutes=1)),
                ],
            }
        )
        stats = asyncio.run(
            _service(fake).repair(
                start=_START,
                end=_START + timedelta(hours=1, minutes=30),
                repair_bus_time=_REPAIR_TIME,
                dry_run=True,
                batch_size=500,
                chunk_size=_ONE_HOUR,
            )
        )
        assert stats.dry_run is True
        assert stats.chunks_processed == 2
        assert stats.fragmented_minutes_found == 3
        assert stats.rows_rewritten == 3
        assert fake.upsert_batches == []
        assert fake.repair_calls == [
            (_START, second_chunk),
            (second_chunk, _START + timedelta(hours=1, minutes=30)),
        ]

    def test_no_dry_run_batches_upserts_and_counts_actual_writes(self) -> None:
        """Test non-dry repair writes batches and uses repository counts.

        Given: Three corrected rows and batch size two,
        When: The service runs with writes enabled,
        Then: It sends two upsert batches and reports the returned write count.
        """
        fake = _FakeRepairRepository(
            {
                _START: [
                    _repair_row(_OPEN),
                    _repair_row(_OPEN + timedelta(minutes=1)),
                    _repair_row(_OPEN + timedelta(minutes=2)),
                ]
            },
            upsert_return_counts=[2, 1],
        )
        stats = asyncio.run(
            _service(fake).repair(
                start=_START,
                end=_START + timedelta(hours=1),
                repair_bus_time=_REPAIR_TIME,
                dry_run=False,
                batch_size=2,
                chunk_size=_ONE_HOUR,
            )
        )
        assert stats.rows_rewritten == 3
        assert [len(batch) for batch in fake.upsert_batches] == [2, 1]
        assert [row["sequence_id"] for row in fake.upsert_batches[0]] == [1, 2]
        assert [row["sequence_id"] for row in fake.upsert_batches[1]] == [3]
        assert all(
            row["timestamp"] == _REPAIR_TIME for batch in fake.upsert_batches for row in batch
        )
        assert all(UUID(row["session_id"]) for batch in fake.upsert_batches for row in batch)

    def test_no_dry_run_empty_chunk_writes_no_batches(self) -> None:
        """Test a write-enabled chunk with no repairs is a no-op.

        Given: A one-day chunk with no repair candidates,
        When: The service runs with writes enabled,
        Then: It reports zero rows and sends no upsert batches.
        """
        fake = _FakeRepairRepository({})
        stats = asyncio.run(
            _service(fake).repair(
                start=_START,
                end=_START + timedelta(hours=1),
                repair_bus_time=_REPAIR_TIME,
                dry_run=False,
                batch_size=500,
                chunk_size=_ONE_HOUR,
            )
        )
        assert stats.chunks_processed == 1
        assert stats.fragmented_minutes_found == 0
        assert stats.rows_rewritten == 0
        assert fake.upsert_batches == []

    def test_empty_range_processes_no_chunks(self) -> None:
        """Test an empty half-open range returns zero stats.

        Given: Start equals end,
        When: The repair service runs,
        Then: No chunks are processed and no repository calls occur.
        """
        fake = _FakeRepairRepository({})
        stats = asyncio.run(
            _service(fake).repair(
                start=_START,
                end=_START,
                repair_bus_time=_REPAIR_TIME,
                dry_run=True,
                batch_size=500,
                chunk_size=_ONE_HOUR,
            )
        )
        assert stats.chunks_processed == 0
        assert stats.fragmented_minutes_found == 0
        assert stats.rows_rewritten == 0
        assert stats.chunk_stats == ()
        assert fake.repair_calls == []

    def test_invalid_batch_size_raises(self) -> None:
        """Test invalid batch sizes are rejected before repository access.

        Given: Batch size zero,
        When: The repair service starts,
        Then: It raises ``ValueError`` and does not query the repository.
        """
        fake = _FakeRepairRepository({})
        with pytest.raises(ValueError, match="batch_size"):
            asyncio.run(
                _service(fake).repair(
                    start=_START,
                    end=_START + timedelta(hours=1),
                    repair_bus_time=_REPAIR_TIME,
                    dry_run=True,
                    batch_size=0,
                    chunk_size=_ONE_HOUR,
                )
            )
        assert fake.repair_calls == []

    def test_invalid_chunk_size_raises(self) -> None:
        """Test invalid chunk sizes are rejected before repository access.

        Given: Chunk size zero,
        When: The repair service starts,
        Then: It raises ``ValueError`` and does not query the repository.
        """
        fake = _FakeRepairRepository({})
        with pytest.raises(ValueError, match="chunk_size"):
            asyncio.run(
                _service(fake).repair(
                    start=_START,
                    end=_START + timedelta(hours=1),
                    repair_bus_time=_REPAIR_TIME,
                    dry_run=True,
                    batch_size=500,
                    chunk_size=timedelta(0),
                )
            )
        assert fake.repair_calls == []

    def test_unreconstructable_minutes_propagate_to_stats(self) -> None:
        """Test detected-but-unrebuildable minutes surface in all stats.

        Given: One chunk with a single rebuilt repair row and two detected
            fragmented minutes that have no raw trades,
        When: The service runs in dry-run mode,
        Then: Chunk and aggregate stats report the unreconstructable count and
            include it in ``fragmented_minutes_found`` so the dry run never
            understates how many minutes are actually fragmented.
        """
        fake = _FakeRepairRepository(
            {_START: [_repair_row(_OPEN)]},
            unreconstructable_by_start={_START: 2},
        )
        stats = asyncio.run(
            _service(fake).repair(
                start=_START,
                end=_START + _ONE_HOUR,
                repair_bus_time=_REPAIR_TIME,
                dry_run=True,
                batch_size=500,
                chunk_size=_ONE_HOUR,
            )
        )
        assert stats.fragmented_minutes_found == 3
        assert stats.rows_rewritten == 1
        assert stats.unreconstructable_minutes == 2
        assert stats.chunk_stats[0].fragmented_minutes_found == 3
        assert stats.chunk_stats[0].unreconstructable_minutes == 2

    def test_fractional_minute_chunk_size_raises(self) -> None:
        """Test sub-minute chunk granularity is rejected before any reads.

        Given: A chunk size of ninety seconds,
        When: The repair service starts,
        Then: It raises ``ValueError`` about whole minutes and never queries
        the repository, because fractional-minute chunk boundaries would split
        a one-minute candle bucket across two trade windows.
        """
        fake = _FakeRepairRepository({})
        with pytest.raises(ValueError, match="whole number of minutes"):
            asyncio.run(
                _service(fake).repair(
                    start=_START,
                    end=_START + timedelta(hours=1),
                    repair_bus_time=_REPAIR_TIME,
                    dry_run=True,
                    batch_size=500,
                    chunk_size=timedelta(seconds=90),
                )
            )
        assert fake.repair_calls == []

    def test_non_minute_bounds_are_floored_to_whole_minutes(self) -> None:
        """Test mid-minute start and end are floored before chunking.

        Given: A start with trailing seconds and a wall-clock-style end with
        seconds and microseconds inside the second hour,
        When: The service runs in dry-run mode with one-hour chunks,
        Then: Every repository window and reported chunk boundary uses the
        floored whole-minute instants, so no trade window can truncate a
        candle bucket mid-minute.
        """
        floored_start = _START
        floored_end = _START + timedelta(hours=1, minutes=30)
        fake = _FakeRepairRepository({floored_start: [_repair_row(_OPEN)]})
        stats = asyncio.run(
            _service(fake).repair(
                start=_START + timedelta(seconds=30),
                end=floored_end + timedelta(seconds=17, microseconds=123456),
                repair_bus_time=_REPAIR_TIME,
                dry_run=True,
                batch_size=500,
                chunk_size=_ONE_HOUR,
            )
        )
        assert fake.repair_calls == [
            (floored_start, floored_start + _ONE_HOUR),
            (floored_start + _ONE_HOUR, floored_end),
        ]
        assert stats.chunk_stats[0].start == floored_start
        assert stats.chunk_stats[-1].end == floored_end

    def test_second_real_run_is_noop_via_value_guard_count(self) -> None:
        """Test repeated repair reports the repository value-guard no-op.

        Given: The repository returns the same candidate on two service runs,
        When: The first upsert count is one and the second is zero,
        Then: The service preserves the no-op count in its returned stats.
        """
        fake = _FakeRepairRepository({_START: [_repair_row(_OPEN)]}, upsert_return_counts=[1, 0])
        service = _service(fake)
        first = asyncio.run(
            service.repair(
                start=_START,
                end=_START + timedelta(hours=1),
                repair_bus_time=_REPAIR_TIME,
                dry_run=False,
                batch_size=1,
                chunk_size=_ONE_HOUR,
            )
        )
        second = asyncio.run(
            service.repair(
                start=_START,
                end=_START + timedelta(hours=1),
                repair_bus_time=_REPAIR_TIME,
                dry_run=False,
                batch_size=1,
                chunk_size=_ONE_HOUR,
            )
        )
        assert first.rows_rewritten == 1
        assert second.fragmented_minutes_found == 1
        assert second.rows_rewritten == 0
        assert len(fake.upsert_batches) == 2
