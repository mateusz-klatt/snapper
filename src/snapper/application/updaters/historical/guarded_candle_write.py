"""Apply guarded candle chunks and record their outcomes.

Retries require proof that the previous attempt rolled back. Every other
failure gets one lock-free reconciliation attempt and stops the run.
"""

import json
import os
import re
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from io import BufferedWriter
from pathlib import Path
from typing import Final
from typing import cast

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.data.repository_types import ClientSample

ATTEMPTS: Final[int] = 4
"""Most attempts a write chunk gets."""

BACKOFF_SECONDS: Final[tuple[float, ...]] = (0.25, 0.5, 1.0)
"""Pause after a failed attempt, outside the transaction, before the next one."""

NATURAL_KEY_INDEXES: Final[frozenset[str]] = frozenset(
    {"uq_candle_itf_open", "candles_p_uq_itf_open"}
)
"""The ordinary and the partitioned parent index of the candle natural key."""

_LEAF_NATURAL_KEY_INDEX: Final[re.Pattern[str]] = re.compile(
    r"candles_(?:d\d{8}|default)_instrument_public_id_timeframe_open_at_idx"
)
"""Leaf index names that ``data/daily_partitions.py`` derives from the parent.

A daily leaf is ``candles_dYYYYMMDD`` and the anomaly buffer ``candles_default``;
each inherits the parent index as ``{leaf}_{columns joined by _}_idx``.
"""

_SQLITE_NATURAL_KEY_VIOLATION: Final[str] = (
    "UNIQUE constraint failed: candles.instrument_public_id, candles.timeframe, candles.open_at"
)
"""SQLite names the columns, not the index, of a violated unique index."""

_SQLITE_UNIQUE_VIOLATION: Final[int] = 2067
"""SQLite extended result code of a unique-constraint violation."""

_RETRYABLE_SQLSTATES: Final[Mapping[str, str]] = {
    "55P03": "lock_timeout",
    "40001": "serialization_failure",
    "40P01": "deadlock",
}
"""PostgreSQL errors that prove the transaction committed nothing."""

_RETRYABLE_OUTCOMES: Final[frozenset[str]] = frozenset(
    {*_RETRYABLE_SQLSTATES.values(), "natural_key_conflict"}
)

_STATEMENT_TIMEOUT: Final[str] = "SET LOCAL statement_timeout = '20s'"
_READ_COMMITTED: Final[str] = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"

type CandleKey = tuple[str, str, datetime]


def candle_key(row: CandleUpsertRow) -> CandleKey:
    """Identify a candle by instrument, timeframe and opening time.

    Args:
        row: Candle write payload.

    Returns:
        The candle's natural key.
    """
    return row["instrument_public_id"], row["timeframe"], row["open_at"]


@dataclass(frozen=True, slots=True)
class Chunk:
    """Numbered batch of rows passed to a guarded writer."""

    number: int
    action: str
    rows: tuple[CandleUpsertRow, ...]


@dataclass(frozen=True, slots=True)
class ChunkAcknowledgement:
    """Per-key results and the observation from an acknowledged attempt."""

    applied: Sequence[CandleUpsertRow]
    skipped: Mapping[CandleKey, JsonObject]
    sample: ClientSample | None


@dataclass(frozen=True, slots=True)
class ChunkAdapter[ChunkT: Chunk]:
    """Persistence and receipt details supplied by the calling candle command."""

    write: Callable[[ChunkT], Awaitable[Sequence[CandleUpsertRow] | ChunkAcknowledgement]]
    reconcile: Callable[[Sequence[CandleUpsertRow]], Awaitable[dict[CandleKey, str]]]
    row_reference: Callable[[CandleUpsertRow], JsonObject]
    receipt_fields: Callable[[ChunkAcknowledgement | None, str | None], JsonObject] | None = None


@dataclass(frozen=True, slots=True)
class WriteOptions:
    """Options that only a write run takes.

    Attributes:
        voted_preview: Directory holding the voted ``preview.csv`` and ``approval.json``.
        expect_digest: sha256 of the voted ``approval.json``, in lower-case hex.
        max_rows: Most selected rows this run applies, or None for all.
        chunk_pause_seconds: Pause between two chunks.
        max_consecutive_lock_timeouts: Consecutive lock timeouts that stop the run.
    """

    voted_preview: Path
    expect_digest: str
    max_rows: int | None
    chunk_pause_seconds: float
    max_consecutive_lock_timeouts: int


def canonical_json(document: JsonValue) -> bytes:
    """Serialize JSON with sorted keys and no insignificant whitespace.

    Args:
        document: JSON value with finite numbers.

    Returns:
        UTF-8 bytes without a trailing newline.
    """
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode()


def open_exclusive(path: Path) -> BufferedWriter:
    """Create a new file readable and writable by its owner only.

    Args:
        path: File to create.

    Returns:
        The open binary writer.

    Raises:
        FileExistsError: The path already exists.
        OSError: The file cannot be created or its mode cannot be set.
    """
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    return os.fdopen(descriptor, "wb")


def write_exclusive(path: Path, data: bytes) -> None:
    """Write a new file with mode 0600, refusing to replace any existing path.

    Args:
        path: File to create.
        data: Complete content.
    """
    with open_exclusive(path) as handle:
        handle.write(data)


def _sqlstate(error: DBAPIError) -> str | None:
    """Return the PostgreSQL SQLSTATE of a driver error, if it has one."""
    value = getattr(error.orig, "sqlstate", None)
    return value if isinstance(value, str) else None


def _constraint_names(error: DBAPIError) -> set[str]:
    """Collect the constraint names a driver error and its cause report."""
    names: set[str] = set()
    for layer in (error.orig, getattr(error.orig, "__cause__", None)):
        for holder in (layer, getattr(layer, "diag", None)):
            name = getattr(holder, "constraint_name", None)
            if isinstance(name, str):
                names.add(name)
    return names


def is_natural_key_index(name: str) -> bool:
    """Recognize the candle natural-key index by name.

    Args:
        name: Index or constraint name a unique violation reports.

    Returns:
        True for ``uq_candle_itf_open``, ``candles_p_uq_itf_open`` and every leaf
        index derived from the parent.
    """
    return name in NATURAL_KEY_INDEXES or _LEAF_NATURAL_KEY_INDEX.fullmatch(name) is not None


def classify_failure(error: BaseException) -> str:
    """Name a failed attempt's outcome for the retry policy.

    Args:
        error: What the attempt raised.

    Returns:
        ``lock_timeout``, ``serialization_failure``, ``deadlock`` or
        ``natural_key_conflict``, which prove nothing was committed and allow a
        retry; ``unique_violation`` for any other unique index, which stops the
        run; otherwise ``uncertain``.
    """
    if not isinstance(error, DBAPIError):
        return "uncertain"
    sqlstate = _sqlstate(error)
    retryable = _RETRYABLE_SQLSTATES.get(sqlstate or "")
    if retryable is not None:
        return retryable
    sqlite_unique = getattr(error.orig, "sqlite_errorcode", None) == _SQLITE_UNIQUE_VIOLATION
    if sqlstate != "23505" and not sqlite_unique:
        return "uncertain"
    natural = any(is_natural_key_index(name) for name in _constraint_names(error)) or (
        sqlite_unique and str(error.orig) == _SQLITE_NATURAL_KEY_VIOLATION
    )
    return "natural_key_conflict" if natural else "unique_violation"


def error_fields(error: BaseException) -> JsonObject:
    """Describe an error without its text, which can carry connection details.

    Args:
        error: Any exception.

    Returns:
        Its class, its driver error's class and its SQLSTATE when known.
    """
    original = getattr(error, "orig", None)
    return {
        "error": type(error).__name__,
        "driver_error": None if original is None else type(original).__name__,
        "sqlstate": _sqlstate(error) if isinstance(error, DBAPIError) else None,
    }


_WRITTEN_FIELDS: Final[tuple[str, ...]] = (
    "session_id",
    "sequence_id",
    "instrument_public_id",
    "timeframe",
    "open_at",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trades",
    "source",
    "complete",
)
"""Fields that identify this run's version of a row: its writer ids and payload."""


def _written_by(version: CandleVersionRow, row: CandleUpsertRow, fields: Sequence[str]) -> bool:
    """Recognize the version a write of ``row`` would have stored."""
    stored = cast(Mapping[str, object], version)
    written = cast(Mapping[str, object], row)
    return all(stored[name] == written[name] for name in fields)


async def reconcile_versions(
    repository: Repository,
    rows: Mapping[CandleKey, CandleUpsertRow],
    session_id: str,
    *,
    extra_fields: Sequence[str] = (),
) -> dict[CandleKey, str]:
    """Resolve this run's versions from one fresh read without a writer lock.

    Read currentness in the same query as the payload. Callers may require
    additional fields when their writer preserves those values exactly.

    Args:
        repository: Repository used for the reconciliation read.
        rows: Submitted payloads by candle key.
        session_id: Write run identifier used to find committed versions.
        extra_fields: Additional payload fields that must match exactly.

    Returns:
        Each key's applied, superseded or uncertain outcome.
    """
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(text(_READ_COMMITTED))
            await session.execute(text(_STATEMENT_TIMEOUT))
        versions = await repository.get_session_candle_versions(session, list(rows), session_id)
    compared_fields = (*_WRITTEN_FIELDS, *extra_fields)
    outcomes: dict[CandleKey, str] = {}
    for key, row in rows.items():
        current = [
            is_current
            for version, is_current in versions
            if _written_by(version, row, compared_fields)
        ]
        outcomes[key] = (
            "uncertain" if not current else "applied" if any(current) else "applied_superseded"
        )
    return outcomes


_ROW_OUTCOMES: Final[tuple[str, ...]] = (
    "applied",
    "skipped_by_guard",
    "applied_superseded",
    "uncertain",
    "not_applied",
)
"""Outcomes a chunk receipt lists rows under."""


@dataclass
class _ChunkRecord:
    """The receipt of one chunk, filled while it runs."""

    chunk: Chunk
    started_at: datetime
    attempts: list[JsonObject] = field(default_factory=list)
    outcomes: dict[CandleKey, str] = field(default_factory=dict)
    reconciliation_error: JsonObject | None = None
    acknowledgement: ChunkAcknowledgement | None = None

    def settle(self, outcome: str) -> None:
        """Give every row without an outcome the same one."""
        for row in self.chunk.rows:
            self.outcomes.setdefault(candle_key(row), outcome)

    def to_json(
        self, ended_at: datetime, row_reference: Callable[[CandleUpsertRow], JsonObject]
    ) -> JsonObject:
        """Render the chunk's receipt line."""
        listed: dict[str, list[JsonValue]] = {outcome: [] for outcome in _ROW_OUTCOMES}
        skipped = {} if self.acknowledgement is None else self.acknowledgement.skipped
        for row in self.chunk.rows:
            key = candle_key(row)
            reference = row_reference(row)
            if key in skipped:
                reference["guard_outcome"] = skipped[key]
            listed[self.outcomes.get(key, "uncertain")].append(reference)
        return {
            "chunk": self.chunk.number,
            "action": self.chunk.action,
            "rows": len(self.chunk.rows),
            "attempts": list(self.attempts),
            "attempt_count": len(self.attempts),
            "reconciliation_error": self.reconciliation_error,
            "started_at": self.started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            **{outcome: cast(JsonValue, rows) for outcome, rows in listed.items()},
        }


class ReceiptLog:
    """Append one durable JSON line per finished chunk to ``receipt.jsonl``."""

    def __init__(self, path: Path) -> None:
        """Create the log exclusively with mode 0600."""
        self._handle = open_exclusive(path)

    def append(self, record: JsonObject) -> None:
        """Write, flush and sync one line.

        Args:
            record: Chunk receipt to serialize on the line.

        Raises:
            OSError: The line cannot be written or synced.
        """
        self._handle.write(canonical_json(record) + b"\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        """Close the log."""
        self._handle.close()


class ChunkWriter[ChunkT: Chunk]:
    """Apply chunks with the retry policy and keep their receipts."""

    def __init__(
        self,
        options: WriteOptions,
        log: ReceiptLog,
        adapter: ChunkAdapter[ChunkT],
        clock: Callable[[], datetime],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        """Bind the writer to its run."""
        self._options = options
        self._log = log
        self._adapter = adapter
        self._clock = clock
        self._sleep = sleep
        self._lock_streak = 0
        self.records: list[JsonObject] = []

    async def apply(self, chunks: Sequence[ChunkT]) -> str:
        """Apply chunks in order, pausing between them, until one stops the run.

        Args:
            chunks: Numbered chunks in application order.

        Returns:
            The terminal stop reason, or ``completed`` after the last chunk.
        """
        for index, chunk in enumerate(chunks):
            if index:
                await self._sleep(self._options.chunk_pause_seconds)
            stop = await self._apply_chunk(chunk)
            if stop is not None:
                return stop
        return "completed"

    async def _apply_chunk(self, chunk: ChunkT) -> str | None:
        """Run one chunk's attempts and log its receipt as soon as it ends."""
        record = _ChunkRecord(chunk, self._clock())
        stop: str | None = "cancelled"
        try:
            stop = await self._attempts(chunk, record)
            return stop
        finally:
            record.settle("uncertain")
            line = record.to_json(self._clock(), self._adapter.row_reference)
            if self._adapter.receipt_fields is not None:
                line.update(self._adapter.receipt_fields(record.acknowledgement, stop))
            self.records.append(line)
            self._log.append(line)

    async def _attempts(self, chunk: ChunkT, record: _ChunkRecord) -> str | None:
        """Attempt a chunk until it is acknowledged or the policy stops the run."""
        attempt = 0
        while True:
            attempt += 1
            try:
                returned = await self._adapter.write(chunk)
            except Exception as error:
                stop = await self._failed(chunk, record, attempt, error)
                if stop is not None:
                    return stop
                continue
            except BaseException as error:
                record.attempts.append(
                    {"attempt": attempt, "outcome": "cancelled", **error_fields(error)}
                )
                raise
            record.attempts.append({"attempt": attempt, "outcome": "acknowledged"})
            if isinstance(returned, ChunkAcknowledgement):
                record.acknowledgement = returned
                returned = returned.applied
            for row in returned:
                record.outcomes[candle_key(row)] = "applied"
            record.settle("skipped_by_guard")
            self._lock_streak = 0
            return None

    async def _failed(
        self, chunk: ChunkT, record: _ChunkRecord, attempt: int, error: Exception
    ) -> str | None:
        """Apply the retry policy to a failed attempt; return a stop reason or None to retry."""
        outcome = classify_failure(error)
        record.attempts.append({"attempt": attempt, "outcome": outcome, **error_fields(error)})
        if outcome not in _RETRYABLE_OUTCOMES:
            if outcome == "uncertain":
                await self._reconcile(chunk, record)
                return "uncertain_acknowledgement"
            record.settle("not_applied")
            return outcome
        self._lock_streak = self._lock_streak + 1 if outcome == "lock_timeout" else 0
        if self._lock_streak >= self._options.max_consecutive_lock_timeouts:
            record.settle("not_applied")
            return "lock_timeouts"
        if attempt == ATTEMPTS:
            record.settle("not_applied")
            return "attempts_exhausted"
        await self._sleep(BACKOFF_SECONDS[attempt - 1])
        return None

    async def _reconcile(self, chunk: ChunkT, record: _ChunkRecord) -> None:
        """Record what a plain read finds of an uncertain chunk; a failed read leaves it uncertain."""
        try:
            record.outcomes.update(await self._adapter.reconcile(chunk.rows))
        except Exception as error:
            record.reconciliation_error = error_fields(error)


def reached(records: Sequence[JsonObject]) -> int:
    """Count the rows of every chunk the run submitted.

    Args:
        records: Receipts for submitted chunks.

    Returns:
        The total number of rows submitted across the chunks.
    """
    return sum(cast(int, record["rows"]) for record in records)


def totals(records: Sequence[JsonObject]) -> JsonObject:
    """Count rows per outcome over all chunk receipts.

    Args:
        records: Receipts for submitted chunks.

    Returns:
        A count for each row outcome, including zero counts.
    """
    return {
        outcome: sum(len(cast(list[JsonValue], record[outcome])) for record in records)
        for outcome in _ROW_OUTCOMES
    }
