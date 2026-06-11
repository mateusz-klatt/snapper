"""Maintenance service for Kraken Equities candle-fragment reconstruction."""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta

from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import EquityCandleRepairRow

__all__ = [
    "EquityCandleRepairChunkStats",
    "EquityCandleRepairService",
    "EquityCandleRepairStats",
]

_EQUITY_REPAIR_SESSION_ID = "00000000-0000-0000-0000-000000000901"
_DEFAULT_CHUNK_SIZE = timedelta(hours=1)
_ONE_MINUTE = timedelta(minutes=1)


@dataclass(frozen=True)
class EquityCandleRepairChunkStats:
    """Repair statistics for one half-open chunk.

    ``unreconstructable_minutes`` counts fragmented minutes detected in the
    chunk that have no persisted raw trades to rebuild from; they remain
    fragmented and need a different remediation (for example a venue
    backfill), so they are surfaced instead of silently dropped.
    """

    start: datetime
    end: datetime
    fragmented_minutes_found: int
    rows_rewritten: int
    unreconstructable_minutes: int


@dataclass(frozen=True)
class EquityCandleRepairStats:
    """Aggregate statistics for one repair service run."""

    dry_run: bool
    chunks_processed: int
    fragmented_minutes_found: int
    rows_rewritten: int
    unreconstructable_minutes: int
    chunk_stats: tuple[EquityCandleRepairChunkStats, ...]


class EquityCandleRepairService:
    """Reconstruct and optionally rewrite fragmented Kraken Equities candles."""

    def __init__(self, repository: Repository) -> None:
        """Store the repository used for repair reads and SCD2 writes."""
        self._repository = repository

    async def repair(
        self,
        *,
        start: datetime,
        end: datetime,
        repair_bus_time: datetime,
        dry_run: bool,
        batch_size: int,
        chunk_size: timedelta = _DEFAULT_CHUNK_SIZE,
    ) -> EquityCandleRepairStats:
        """Repair fragmented one-minute candles across bounded trade chunks.

        Reruns are idempotent because detection reads the historical candle
        version count, reconstruction reads persisted raw trades, and writes go
        through the candle SCD2 value guard. Prior repair output is never part
        of the reconstruction input, so a later run rebuilds the same values
        and reports zero writes once the corrected active candle already exists.

        ``start`` and ``end`` are floored down to whole UTC minutes before any
        chunking, and ``chunk_size`` must be a whole number of minutes. This
        keeps every chunk boundary minute-aligned, so a candle bucket detected
        via ``open_at < boundary`` always has its full trade minute
        ``[open_at, open_at + 1m)`` inside the same chunk's trade window — a
        mid-minute ``end`` (such as a wall-clock default) can therefore never
        rebuild a candle from a truncated set of trades. Flooring ``end`` also
        deliberately excludes the still-in-progress minute from repair.

        Callers must additionally keep ``end`` behind a settled horizon (the
        CLI caps it at one hour before now): Kraken Equities is a delayed,
        event-watermark-driven feed, so trades for a recent minute may not
        all be persisted yet, and rebuilding such a minute would write a
        wrong bar from a partial trade set. The same horizon removes the
        live-write race — live synthesis only upserts minutes near the
        delayed live edge, so repairs to settled minutes never contend with
        the feed. A concurrent backfill write to the same minute would abort
        the upsert batch loudly on the active-version unique index rather
        than corrupt SCD2 history; avoid running an equities candle backfill
        in parallel with a repair.

        Args:
            start: Inclusive UTC ``open_at`` lower bound; floored to the
                containing minute.
            end: Exclusive UTC ``open_at`` upper bound; floored to the
                containing minute.
            repair_bus_time: Bus-time stamped on corrected SCD2 versions.
            dry_run: When true, read and count repairs without writing.
            batch_size: Maximum rows per ``upsert_candles`` call.
            chunk_size: Time span per read chunk, defaulting to one hour. Must
                be a positive whole number of minutes.

        Returns:
            Aggregate and per-chunk repair statistics.

        Raises:
            ValueError: If ``batch_size`` is less than one, ``chunk_size`` is
                not positive, or ``chunk_size`` is not a whole number of
                minutes.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if chunk_size <= timedelta(0):
            raise ValueError("chunk_size must be positive")
        if chunk_size % _ONE_MINUTE != timedelta(0):
            raise ValueError("chunk_size must be a whole number of minutes")
        start = _floor_to_utc_minute(start)
        end = _floor_to_utc_minute(end)
        chunks: list[EquityCandleRepairChunkStats] = []
        chunk_start = start
        sequence_start = 1
        while chunk_start < end:
            chunk_end = min(chunk_start + chunk_size, end)
            batch = await self._repository.get_equity_candle_repairs_from_trades(
                start=chunk_start,
                end=chunk_end,
            )
            repairs = batch.repairs
            rows_rewritten = len(repairs)
            if not dry_run:
                rows_rewritten = await self._rewrite_repairs(
                    repairs=repairs,
                    repair_bus_time=repair_bus_time,
                    batch_size=batch_size,
                    sequence_start=sequence_start,
                )
            sequence_start += len(repairs)
            chunks.append(
                EquityCandleRepairChunkStats(
                    start=chunk_start,
                    end=chunk_end,
                    fragmented_minutes_found=len(repairs) + batch.unreconstructable_minutes,
                    rows_rewritten=rows_rewritten,
                    unreconstructable_minutes=batch.unreconstructable_minutes,
                )
            )
            chunk_start = chunk_end
        return EquityCandleRepairStats(
            dry_run=dry_run,
            chunks_processed=len(chunks),
            fragmented_minutes_found=sum(chunk.fragmented_minutes_found for chunk in chunks),
            rows_rewritten=sum(chunk.rows_rewritten for chunk in chunks),
            unreconstructable_minutes=sum(chunk.unreconstructable_minutes for chunk in chunks),
            chunk_stats=tuple(chunks),
        )

    async def _rewrite_repairs(
        self,
        *,
        repairs: list[EquityCandleRepairRow],
        repair_bus_time: datetime,
        batch_size: int,
        sequence_start: int,
    ) -> int:
        rows = _build_upsert_rows(repairs, repair_bus_time, sequence_start)
        rows_rewritten = 0
        for offset in range(0, len(rows), batch_size):
            rows_rewritten += await self._repository.upsert_candles(
                rows[offset : offset + batch_size]
            )
        return rows_rewritten


def _floor_to_utc_minute(value: datetime) -> datetime:
    """Floor an aware UTC datetime down to its containing whole minute."""
    return value.replace(second=0, microsecond=0)


def _build_upsert_rows(
    repairs: list[EquityCandleRepairRow],
    repair_bus_time: datetime,
    sequence_start: int,
) -> list[CandleUpsertRow]:
    return [
        _build_upsert_row(
            repair=repair,
            repair_bus_time=repair_bus_time,
            sequence_id=sequence_start + index,
        )
        for index, repair in enumerate(repairs)
    ]


def _build_upsert_row(
    *,
    repair: EquityCandleRepairRow,
    repair_bus_time: datetime,
    sequence_id: int,
) -> CandleUpsertRow:
    return {
        "instrument_public_id": repair.instrument_public_id,
        "open_at": repair.open_at,
        "timestamp": repair_bus_time,
        "timeframe": repair.timeframe,
        "open": repair.open,
        "high": repair.high,
        "low": repair.low,
        "close": repair.close,
        "volume": repair.volume,
        "vwap": repair.vwap,
        "trades": repair.trades,
        "session_id": _EQUITY_REPAIR_SESSION_ID,
        "sequence_id": sequence_id,
    }
