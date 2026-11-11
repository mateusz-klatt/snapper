"""Plan and apply guarded repairs of absent and false-zero one-minute candles.

A run takes correct bars from one of two sources: a saved Kraken REST OHLC
snapshot, or the stored trades of an exchange. It classifies each minute
against every stored version of that candle and asks the guarded repository
writers, in dry-run mode, which rows they would apply. It then writes
``preview.csv`` (one line per minute), a canonical ``approval.json`` whose
sha256 is the digest the operators vote on, and ``summary.json``.

A write run repeats the plan, keeps only the planned rows that the voted
preview holds with the same values, and applies them through
``insert_missing_native_candles``, ``insert_missing_calculated_candles`` and
``correct_false_zero_candles`` in chunks of at most 100 rows. Those writers
never close, update or replace a native or calculated row. A chunk is
retried only after an error that proves nothing was committed. Any other
error makes the chunk uncertain: it is reconciled by one plain read, without
a table lock, and the run stops. ``receipt.jsonl`` gets one line per chunk as
soon as the chunk ends, and ``receipt.json`` is written even when the run
fails or is cancelled.

Error texts can carry connection details, so receipts and refusals name only
an error's class and SQLSTATE.
"""

import asyncio
import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import sys
import zlib
from collections import Counter
from collections import defaultdict
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import astuple
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from enum import StrEnum
from io import BufferedWriter
from itertools import batched
from pathlib import Path
from types import ModuleType
from typing import Final
from typing import Literal
from typing import cast
from uuid import uuid7

from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from snapper.application.updaters.historical import trade_candle_rows
from snapper.application.updaters.historical.trade_candle_rows import as_utc
from snapper.application.updaters.historical.trade_candle_rows import calculated_row
from snapper.application.updaters.historical.trade_candle_rows import trade_update
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data import repository as repository_module
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import Repository
from snapper.data.repository import where_active
from snapper.data.repository_types import CandlePredecessor
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.infrastructure.exchanges import _trade_candle_builder
from snapper.infrastructure.exchanges import kraken_closed_candles
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.kraken_closed_candles import ClosedCandleRequest
from snapper.infrastructure.exchanges.kraken_closed_candles import parse_closed_candles

TIMEFRAME: Final[str] = "1m"
"""The only timeframe this driver repairs."""

SNAPSHOT_EXCHANGE: Final[str] = "kraken"
"""Exchange of every saved REST OHLC snapshot."""

SNAPSHOT_FOLDERS: Final[tuple[str, ...]] = ("ohlc", "ohlc_tokenized")
"""Snapshot subdirectories whose ``<pair key>.json.gz`` files are read."""

CHUNK_ROWS: Final[int] = 100
"""Largest batch handed to a guarded writer, in a dry run or a write."""

REQUEST_MINUTES: Final[int] = 719
"""Longest window the Kraken REST OHLC parser accepts per call."""

LIVE_EDGE: Final[timedelta] = timedelta(minutes=180)
"""No repaired minute may end later than this before the time of the run."""

ATTEMPTS: Final[int] = 4
"""Most attempts a write chunk gets."""

BACKOFF_SECONDS: Final[tuple[float, ...]] = (0.25, 0.5, 1.0)
"""Pause after a failed attempt, outside the transaction, before the next one."""

PREVIEW_FILE: Final[str] = "preview.csv"
APPROVAL_FILE: Final[str] = "approval.json"
SUMMARY_FILE: Final[str] = "summary.json"
RECEIPT_LINES_FILE: Final[str] = "receipt.jsonl"
RECEIPT_FILE: Final[str] = "receipt.json"

PREVIEW_COLUMNS: Final[tuple[str, ...]] = (
    "symbol",
    "instrument_public_id",
    "minute_utc",
    "action",
    "reason",
    "current_id",
    "current_public_id",
    "current_source",
    "current_volume",
    "current_known_at",
    "new_source",
    "new_known_at",
    "new_open",
    "new_high",
    "new_low",
    "new_close",
    "new_vwap",
    "new_volume",
    "new_trades",
)
"""Header of ``preview.csv``, in column order."""

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

POSTGRESQL_URL_OPTIONS: Final[frozenset[str]] = frozenset({"ssl"})
"""Query options a PostgreSQL target URL may carry.

SQLAlchemy's asyncpg dialect hands every query option to ``asyncpg.connect``
in place of the URL's own fields. ``ssl`` only selects the TLS mode of the
same connection; every other option is refused, routing or not.
"""

_RETRYABLE_SQLSTATES: Final[Mapping[str, str]] = {
    "55P03": "lock_timeout",
    "40001": "serialization_failure",
    "40P01": "deadlock",
}
"""PostgreSQL errors that prove the transaction committed nothing."""

_RETRYABLE_OUTCOMES: Final[frozenset[str]] = frozenset(
    {*_RETRYABLE_SQLSTATES.values(), "natural_key_conflict"}
)

_COMPLETE_STOPS: Final[frozenset[str]] = frozenset({"completed", "max_rows"})
"""Stop reasons of a write run that ended as planned."""

_NATIVE: Final[str] = "native"
_SYNTHESIZED: Final[str] = "synthesized"
_MINUTE: Final[timedelta] = timedelta(minutes=1)
_DAY: Final[timedelta] = timedelta(days=1)
_VERSION_READ_INSTRUMENTS: Final[int] = 20
_ALIAS_READ_PAIRS: Final[int] = 500
_ERROR_TEXT_LIMIT: Final[int] = 200
_STATEMENT_TIMEOUT: Final[str] = "SET LOCAL statement_timeout = '20s'"
_READ_COMMITTED: Final[str] = "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"

type MinuteKey = tuple[str, datetime]
type _GuardKey = tuple[str, str, datetime]


class RepairRefusedError(Exception):
    """A condition under which the run refuses; its text names no credential."""


class RepairSource(StrEnum):
    """Where the correct bars come from."""

    SNAPSHOT = "snapshot"
    TRADES = "trades"

    @property
    def row_source(self) -> Literal["native", "calculated"]:
        """Return the source of every candle row this input produces.

        Returns:
            ``native`` for the snapshot, ``calculated`` for stored trades.
        """
        return "native" if self is RepairSource.SNAPSHOT else "calculated"


class Action(StrEnum):
    """What the plan does with one source minute."""

    INSERT = "insert"
    CORRECT = "correct"
    EQUAL = "equal"
    DIFFERS = "differs"
    SKIP = "skip"
    REFUSED_BY_GUARD = "refused_by_guard"


_WRITE_ACTIONS: Final[frozenset[Action]] = frozenset({Action.INSERT, Action.CORRECT})


@dataclass(frozen=True, slots=True)
class RepairWindow:
    """Half-open UTC minute window ``[start, end)`` of the repair.

    Attributes:
        start: Inclusive first minute.
        end: Exclusive end minute.
    """

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        """Refuse naive, non-UTC, unaligned or empty windows.

        Raises:
            RepairRefusedError: When a bound is not a UTC minute or the window is empty.
        """
        for value in (self.start, self.end):
            if value.utcoffset() != timedelta(0) or value.second or value.microsecond:
                raise RepairRefusedError("--start and --end must be UTC minute boundaries")
        if self.start >= self.end:
            raise RepairRefusedError("--start must precede --end")

    def days(self) -> list[tuple[datetime, datetime]]:
        """Split the window at UTC midnights.

        Returns:
            ``[max(d, start), min(d + 1 day, end))`` for each UTC day ``d`` it overlaps.
        """
        windows: list[tuple[datetime, datetime]] = []
        day = self.start.replace(hour=0, minute=0)
        while day < self.end:
            windows.append((max(day, self.start), min(day + _DAY, self.end)))
            day += _DAY
        return windows

    def to_json(self) -> JsonObject:
        """Render the window for the approval.

        Returns:
            ISO ``start`` and ``end``.
        """
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}


@dataclass(frozen=True, slots=True)
class RepairRequest:
    """One validated operator request.

    Attributes:
        source: Where the correct bars come from.
        exchange: Exchange of the instruments; ``kraken`` for the snapshot.
        window: Minutes to repair.
        symbols: Requested native symbols, sorted and unique; empty with ``all_mapped``.
        all_mapped: Whether every snapshot file with a mapped pair key is used.
        snapshot_dir: Snapshot root for the snapshot source, else None.
        output_dir: Empty directory receiving this run's files.
    """

    source: RepairSource
    exchange: str
    window: RepairWindow
    symbols: tuple[str, ...]
    all_mapped: bool
    snapshot_dir: Path | None
    output_dir: Path


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


def _utc_now() -> datetime:
    """Return the current UTC time.

    Returns:
        Timezone-aware current time.
    """
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RunContext:
    """The repository, the run's bound identity and its clock.

    Attributes:
        repository: Repository of the target database.
        database: Credential-free endpoint from :func:`database_endpoint`.
        code_revision: Hashes from :func:`code_revision`.
        clock: Current UTC time.
        sleep: Asynchronous pause, used for backoff and between chunks.
    """

    repository: Repository
    database: JsonObject
    code_revision: JsonObject
    clock: Callable[[], datetime] = _utc_now
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep


@dataclass(frozen=True, slots=True, order=True)
class ResolvedInstrument:
    """One instrument selected for the run.

    Attributes:
        instrument_public_id: Instrument public ID.
        symbol: Its native symbol.
    """

    instrument_public_id: str
    symbol: str


@dataclass(frozen=True, slots=True)
class SnapshotFile:
    """One saved OHLC response, read in full.

    Attributes:
        path: Path relative to the snapshot root, for example ``ohlc/XXBTZEUR.json.gz``.
        pair: Pair key, the file name without ``.json.gz``.
        sha256: Hash of the file's bytes.
        fetched_at: Acquisition time in epoch seconds; None for an invalid file.
        candles: Closed bars with trades inside the clipped window.
        error: Why the file is invalid, or None.
    """

    path: str
    pair: str
    sha256: str
    fetched_at: float | None
    candles: tuple[CandleUpdate, ...]
    error: str | None


@dataclass(frozen=True, slots=True)
class SourceMinute:
    """The source's bar for one instrument and minute.

    Attributes:
        instrument: Instrument of the bar.
        minute: Opening time of the minute, UTC.
        candle: The bar, or None when snapshot files disagree about it.
        known_at: Snapshot acquisition time; None for stored trades.
    """

    instrument: ResolvedInstrument
    minute: datetime
    candle: CandleUpdate | None
    known_at: datetime | None

    @property
    def key(self) -> MinuteKey:
        """Return the minute's natural key without its fixed timeframe.

        Returns:
            Instrument public ID and minute.
        """
        return self.instrument.instrument_public_id, self.minute


@dataclass(frozen=True, slots=True)
class SourceInputs:
    """Everything a source contributes to a plan.

    Attributes:
        instruments: Selected instruments, sorted.
        minutes: Source bars of those instruments inside the window.
        contributing: Valid snapshot files of selected instruments.
        invalid: Snapshot files that failed validation.
        unmapped: Paths of valid snapshot files whose pair key maps to no instrument.
        ambiguous: Paths of valid snapshot files whose pair key maps to several.
    """

    instruments: tuple[ResolvedInstrument, ...]
    minutes: tuple[SourceMinute, ...]
    contributing: tuple[SnapshotFile, ...] = ()
    invalid: tuple[SnapshotFile, ...] = ()
    unmapped: tuple[str, ...] = ()
    ambiguous: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PlanEntry:
    """The plan's decision for one source minute.

    Attributes:
        source: The source minute.
        action: What the plan does with it.
        reason: Why, for ``skip``; the refused action for ``refused_by_guard``.
        current: The version known at the time of the run, if exactly one.
    """

    source: SourceMinute
    action: Action
    reason: str
    current: CandleVersionRow | None


@dataclass(frozen=True, slots=True)
class PreviewLine:
    """One line of ``preview.csv``; every field is text as written.

    Attributes:
        symbol: Native symbol.
        instrument_public_id: Instrument public ID.
        minute_utc: ISO minute.
        action: Planned action.
        reason: Reason of a skip or of a guard refusal.
        current_id: Physical id of the current version.
        current_public_id: Its public ID.
        current_source: Its source.
        current_volume: Its volume, ``repr`` of the float.
        current_known_at: Its ISO knowledge time.
        new_source: Source of the new row.
        new_known_at: ISO knowledge time of a snapshot row; empty for trades.
        new_open: New open.
        new_high: New high.
        new_low: New low.
        new_close: New close.
        new_vwap: New VWAP.
        new_volume: New volume.
        new_trades: New trade count.
    """

    symbol: str
    instrument_public_id: str
    minute_utc: str
    action: str
    reason: str
    current_id: str
    current_public_id: str
    current_source: str
    current_volume: str
    current_known_at: str
    new_source: str
    new_known_at: str
    new_open: str
    new_high: str
    new_low: str
    new_close: str
    new_vwap: str
    new_volume: str
    new_trades: str

    @property
    def key(self) -> tuple[str, str]:
        """Return the line's instrument and minute text.

        Returns:
            Instrument public ID and ISO minute.
        """
        return self.instrument_public_id, self.minute_utc

    @property
    def vote(self) -> tuple[str, ...]:
        """Return the fields a write must find unchanged in the voted line.

        Returns:
            Action, predecessor identity and time, and every new value.
        """
        return (
            self.action,
            self.current_id,
            self.current_public_id,
            self.current_known_at,
            *self.new_values,
        )

    @property
    def new_values(self) -> tuple[str, ...]:
        """Return the new row's source, knowledge time and bar.

        Returns:
            Source, knowledge time, OHLC, VWAP, volume and trade count.
        """
        return (
            self.new_source,
            self.new_known_at,
            self.new_open,
            self.new_high,
            self.new_low,
            self.new_close,
            self.new_vwap,
            self.new_volume,
            self.new_trades,
        )


@dataclass(frozen=True, slots=True)
class PlanArtifacts:
    """The files of one plan.

    Attributes:
        digest: sha256 of ``approval.json``.
        summary: Content of ``summary.json``.
        lines: Lines of ``preview.csv``, in plan order.
    """

    digest: str
    summary: JsonObject
    lines: tuple[PreviewLine, ...]


@dataclass(frozen=True, slots=True)
class VotedPreview:
    """A voted preview whose hashes have been verified.

    Attributes:
        digest: sha256 of its ``approval.json``.
        approval: The parsed approval.
        lines: Its preview lines.
    """

    digest: str
    approval: JsonObject
    lines: tuple[PreviewLine, ...]


@dataclass(frozen=True, slots=True)
class Selection:
    """The voted rows a write applies and the rest it reports.

    Attributes:
        selected: Planned rows the vote holds unchanged, in plan order.
        not_voted: Planned writes the vote does not hold unchanged.
        already_applied: Voted writes whose current row now equals the voted bar.
        changed: Voted writes that no longer match the plan.
    """

    selected: tuple[PlanEntry, ...]
    not_voted: tuple[PlanEntry, ...]
    already_applied: tuple[PreviewLine, ...]
    changed: tuple[PreviewLine, ...]


@dataclass(frozen=True, slots=True)
class WriteChunk:
    """One batch for one guarded writer.

    Attributes:
        number: Position of the chunk in the run, from 1.
        action: ``insert`` or ``correct``.
        rows: Complete rows carrying the run's session and sequence ids.
        predecessors: The voted zero of each correction, by natural key.
    """

    number: int
    action: Action
    rows: tuple[CandleUpsertRow, ...]
    predecessors: Mapping[_GuardKey, CandlePredecessor]


@dataclass(frozen=True, slots=True)
class WriteReport:
    """Outcome of a write run.

    Attributes:
        voted_digest: Digest of the voted approval.
        plan_digest: Digest of the plan rebuilt by this run.
        stop_reason: Why the run stopped.
        totals: Rows per outcome over all chunks.
    """

    voted_digest: str
    plan_digest: str
    stop_reason: str
    totals: JsonObject

    @property
    def complete(self) -> bool:
        """Return whether the run ended as planned.

        Returns:
            True after all selected rows or the requested maximum were processed.
        """
        return self.stop_reason in _COMPLETE_STOPS


def sha256_hex(data: bytes) -> str:
    """Hash bytes.

    Args:
        data: Bytes to hash.

    Returns:
        Lower-case hex sha256.
    """
    return hashlib.sha256(data).hexdigest()


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


def database_endpoint(db_url: str) -> JsonObject:
    """Name the database the run will reach, exactly as its URL writes it, without credentials.

    A PostgreSQL URL must itself name one host, its port and its database,
    and may carry no query option other than those in
    :data:`POSTGRESQL_URL_OPTIONS`, which is ``ssl`` alone. A missing field
    would fall back to an environment or service-file default (``PGHOST``,
    ``PGPORT``, ``PGDATABASE``, ``PGSERVICE`` and the like), and query options
    such as ``database``, ``host``, ``port`` or ``service`` replace the URL's
    own fields, so the approval could name a database other than the one
    written. With all three fields explicit, the driver uses them over every
    such default. A SQLite target is named by the canonical absolute path of
    its file and may carry no query option, because one such as ``uri`` can
    select another or an in-memory database.

    Args:
        db_url: Configured SQLAlchemy URL.

    Returns:
        Dialect, host, port and database name as the URL writes them, or the
        canonical absolute file path for SQLite.

    Raises:
        RepairRefusedError: For an incomplete PostgreSQL URL, a query option
            outside the allowlist, an in-memory or optioned SQLite URL, or
            another dialect.
    """
    url = make_url(db_url)
    dialect = url.get_backend_name()
    if dialect == "sqlite":
        return _sqlite_endpoint(url)
    if dialect != "postgresql":
        raise RepairRefusedError(f"database dialect {dialect} is not supported")
    return _postgresql_endpoint(url)


def _sqlite_endpoint(url: URL) -> JsonObject:
    """Name a SQLite database file by its canonical absolute path."""
    if not url.database or url.database == ":memory:" or url.query:
        raise RepairRefusedError("a SQLite target must be a database file without query options")
    return {"dialect": "sqlite", "path": str(Path(url.database).resolve())}


def _postgresql_endpoint(url: URL) -> JsonObject:
    """Name a PostgreSQL database by the explicit host, port and database of its URL."""
    if not url.host or "," in url.host or url.port is None or not url.database:
        raise RepairRefusedError(
            "a PostgreSQL target URL must name one host, its port and its database"
        )
    options = sorted(set(url.query) - POSTGRESQL_URL_OPTIONS)
    if options:
        raise RepairRefusedError(
            f"PostgreSQL target URL options are not allowed: {', '.join(options)}"
        )
    return {"dialect": "postgresql", "host": url.host, "port": url.port, "database": url.database}


def code_revision(extra_modules: Sequence[ModuleType]) -> JsonObject:
    """Hash the installed source of every module that decides what is written.

    The guards, the snapshot parser, the live trade builder, the shared row
    builder and this module are always included; the caller adds its own
    module.

    Args:
        extra_modules: Further imported modules to hash.

    Returns:
        sha256 of each module's file, keyed by its path below the package root.
    """
    modules = (
        repository_module,
        kraken_closed_candles,
        _trade_candle_builder,
        trade_candle_rows,
        sys.modules[__name__],
        *extra_modules,
    )
    return {
        f"{module.__name__.replace('.', '/')}.py": sha256_hex(
            Path(cast(str, module.__file__)).read_bytes()
        )
        for module in modules
    }


def prepare_output_dir(directory: Path) -> None:
    """Create the output directory or require an existing one to be empty.

    Args:
        directory: Requested output directory.

    Raises:
        RepairRefusedError: When the path is not a directory or is not empty.
    """
    if not directory.exists():
        directory.mkdir(mode=0o700)
        return
    if not directory.is_dir():
        raise RepairRefusedError("--output-dir is not a directory")
    if any(directory.iterdir()):
        raise RepairRefusedError("--output-dir must be empty")


def _open_exclusive(path: Path) -> BufferedWriter:
    """Create a new file readable and writable by its owner only."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.fchmod(descriptor, 0o600)
    return os.fdopen(descriptor, "wb")


def write_exclusive(path: Path, data: bytes) -> None:
    """Write a new file with mode 0600, refusing to replace any existing path.

    Args:
        path: File to create.
        data: Complete content.
    """
    with _open_exclusive(path) as handle:
        handle.write(data)


def refuse_live_edge(window: RepairWindow, run_time: datetime) -> None:
    """Refuse minutes that end within the live writers' reach.

    Args:
        window: Requested window.
        run_time: Time of the run.

    Raises:
        RepairRefusedError: When ``--end`` is later than the run time minus 180 minutes.
    """
    if window.end > run_time - LIVE_EDGE:
        raise RepairRefusedError(
            f"--end must not be later than {(run_time - LIVE_EDGE).isoformat()}, "
            "180 minutes before the time of the run"
        )


def request_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Split ``[start, end)`` into consecutive parser windows of at most 719 minutes.

    Args:
        start: Inclusive start minute.
        end: Exclusive end minute.

    Returns:
        Consecutive windows covering the span; none when it is empty.
    """
    windows: list[tuple[datetime, datetime]] = []
    lower = start
    while lower < end:
        upper = min(lower + timedelta(minutes=REQUEST_MINUTES), end)
        windows.append((lower, upper))
        lower = upper
    return windows


def _error_text(error: Exception) -> str:
    """Describe why a snapshot file is invalid, bounded in length."""
    return f"{type(error).__name__}: {error}"[:_ERROR_TEXT_LIMIT]


def _fetched_at(payload: object, run_time: datetime) -> float:
    """Return a response's acquisition time after checking it is a finite past instant."""
    if not isinstance(payload, dict):
        raise ValueError("snapshot file is not a JSON object")
    value = payload.get("fetched_at")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("snapshot fetched_at is not a finite number")
    if value > run_time.timestamp():
        raise ValueError("snapshot fetched_at is later than the time of the run")
    return float(value)


def _response_rows(payload: dict[str, object], pair: str) -> list[object]:
    """Return the native rows of a response that names exactly its pair and ``last``."""
    if payload.get("error") != []:
        raise ValueError("snapshot response reports an error")
    result = payload.get("result")
    if not isinstance(result, dict) or set(result) != {pair, "last"}:
        raise ValueError("snapshot result does not hold exactly the pair key and last")
    rows = result[pair]
    if not isinstance(rows, list):
        raise ValueError("snapshot rows are not a list")
    return cast(list[object], rows)


def _row_times(rows: list[object]) -> list[int]:
    """Return each row's epoch minute after checking its shape."""
    times: list[int] = []
    for row in rows:
        if not isinstance(row, list) or len(row) != 8 or type(row[0]) is not int:
            raise ValueError("snapshot row is not an eight-field row with an integer time")
        times.append(row[0])
    return times


def _snapshot_candles(
    payload: object, pair: str, window: RepairWindow, run_time: datetime
) -> tuple[float, tuple[CandleUpdate, ...]]:
    """Validate one response and return its traded closed bars inside the clipped window.

    The window is ``[max(oldest row, start), min(floor(fetched_at), end))``. The
    parser runs once per window of at most 719 minutes with ``observed_at``
    set to ``fetched_at``; it drops the response's last, still open entry and
    returns nothing for an empty list or the lone all-zero row of an untraded
    window. Bars with a trade count of 0 are dropped. A minute absent from
    the file is unknown, so nothing is derived for it.
    """
    fetched_at = _fetched_at(payload, run_time)
    rows = _response_rows(cast(dict[str, object], payload), pair)
    times = _row_times(rows)
    if times and max(times) > fetched_at:
        raise ValueError("snapshot holds a row newer than its fetched_at")
    observed = datetime.fromtimestamp(fetched_at, UTC)
    lower = max(window.start, datetime.fromtimestamp(min(times), UTC)) if times else window.start
    upper = min(window.end, observed.replace(second=0, microsecond=0))
    candles: list[CandleUpdate] = []
    for start, end in request_windows(lower, upper):
        request = ClosedCandleRequest(
            symbol=pair, pair=pair, start=start, end=end, observed_at=observed
        )
        candles.extend(
            candle for candle in parse_closed_candles(payload, request) if candle.trades > 0
        )
    return fetched_at, tuple(candles)


def parse_snapshot_file(
    path: str, data: bytes, window: RepairWindow, run_time: datetime
) -> SnapshotFile:
    """Read one saved response; an invalid file is returned with its error.

    Args:
        path: Path relative to the snapshot root.
        data: The file's bytes.
        window: Requested window.
        run_time: Time of the run.

    Returns:
        The file's hash, acquisition time and traded closed bars, or its error.
    """
    pair = path.rsplit("/", 1)[-1].removesuffix(".json.gz")
    digest = sha256_hex(data)
    try:
        payload: object = json.loads(gzip.decompress(data))
        fetched_at, candles = _snapshot_candles(payload, pair, window, run_time)
    except (OSError, EOFError, zlib.error, ValueError, OverflowError) as error:
        return SnapshotFile(path, pair, digest, None, (), _error_text(error))
    return SnapshotFile(path, pair, digest, fetched_at, candles, None)


def read_snapshot(directory: Path, window: RepairWindow, run_time: datetime) -> list[SnapshotFile]:
    """Read every ``*.json.gz`` file under ``ohlc/`` and ``ohlc_tokenized/``.

    Args:
        directory: Snapshot root.
        window: Requested window.
        run_time: Time of the run.

    Returns:
        Every file, valid or not, in path order.

    Raises:
        RepairRefusedError: When neither subdirectory exists.
    """
    folders = [directory / name for name in SNAPSHOT_FOLDERS]
    if not any(folder.is_dir() for folder in folders):
        raise RepairRefusedError("--snapshot-dir holds neither ohlc/ nor ohlc_tokenized/")
    files: list[SnapshotFile] = []
    for folder in folders:
        if not folder.is_dir():
            continue
        for path in sorted(folder.iterdir()):
            if path.name.endswith(".json.gz") and path.is_file():
                relative = f"{folder.name}/{path.name}"
                files.append(parse_snapshot_file(relative, path.read_bytes(), window, run_time))
    return files


async def kraken_rest_aliases(
    repository: Repository, pairs: Collection[str], as_of: datetime
) -> dict[str, set[ResolvedInstrument]]:
    """Map pair keys to Kraken instruments through active REST aliases of exactly that name.

    Args:
        repository: Repository of the target database.
        pairs: Pair keys to resolve.
        as_of: Time at which aliases, symbols and instruments must be active.

    Returns:
        Every instrument found for each pair key that has one.
    """
    found: dict[str, set[ResolvedInstrument]] = defaultdict(set)
    async with repository.session() as session:
        for chunk in batched(sorted(pairs), _ALIAS_READ_PAIRS, strict=False):
            result = await session.execute(
                select(SymbolAlias.exchange_symbol, Symbol.native_symbol, Instrument.public_id)
                .select_from(SymbolAlias)
                .join(Symbol, Symbol.public_id == SymbolAlias.symbol_public_id)
                .join(Instrument, Instrument.symbol_public_id == Symbol.public_id)
                .where(
                    SymbolAlias.exchange == SNAPSHOT_EXCHANGE,
                    SymbolAlias.channel == "rest",
                    SymbolAlias.exchange_symbol.in_(chunk),
                    Instrument.exchange == SNAPSHOT_EXCHANGE,
                    *where_active(SymbolAlias, as_of),
                    *where_active(Symbol, as_of),
                    *where_active(Instrument, as_of),
                )
            )
            for pair, symbol, instrument in result.all():
                found[pair].add(ResolvedInstrument(instrument, symbol))
    return dict(found)


def _requested_instruments(
    request: RepairRequest, available: Collection[ResolvedInstrument]
) -> tuple[ResolvedInstrument, ...]:
    """Select the instruments of the requested symbols, refusing one that is missing."""
    if request.all_mapped:
        return tuple(sorted(set(available)))
    chosen = sorted({item for item in available if item.symbol in request.symbols})
    missing = sorted(set(request.symbols) - {item.symbol for item in chosen})
    if missing:
        raise RepairRefusedError(f"requested symbols without a mapped snapshot file: {missing}")
    return tuple(chosen)


def merge_snapshot_minutes(
    files: Sequence[SnapshotFile], mapping: Mapping[str, ResolvedInstrument]
) -> tuple[SourceMinute, ...]:
    """Combine the bars of every file per instrument and minute.

    Equal bars make one source minute, known at the latest ``fetched_at`` of
    their files; different bars make one without a bar, which the plan skips
    as ``conflicting_snapshot_files``.

    Args:
        files: Valid files whose pair keys are in ``mapping``.
        mapping: Instrument of each pair key.

    Returns:
        One source minute per instrument and minute.
    """
    observed: dict[MinuteKey, list[tuple[float, CandleUpdate]]] = defaultdict(list)
    instruments: dict[str, ResolvedInstrument] = {}
    for file in files:
        instrument = mapping[file.pair]
        instruments[instrument.instrument_public_id] = instrument
        for candle in file.candles:
            key = instrument.instrument_public_id, as_utc(candle.interval_begin)
            observed[key].append((cast(float, file.fetched_at), candle))
    minutes: list[SourceMinute] = []
    for (instrument_id, minute), bars in observed.items():
        distinct = {_candle_values(candle) for _, candle in bars}
        fetched_at, newest = max(bars, key=lambda bar: bar[0])
        agreed = len(distinct) == 1
        minutes.append(
            SourceMinute(
                instruments[instrument_id],
                minute,
                newest if agreed else None,
                datetime.fromtimestamp(fetched_at, UTC) if agreed else None,
            )
        )
    return tuple(minutes)


async def snapshot_sources(
    repository: Repository, request: RepairRequest, run_time: datetime
) -> SourceInputs:
    """Read the snapshot and select the files of the requested instruments.

    Args:
        repository: Repository of the target database.
        request: Snapshot request.
        run_time: Time of the run.

    Returns:
        Instruments, source minutes and the classification of every file.
    """
    files = read_snapshot(cast(Path, request.snapshot_dir), request.window, run_time)
    valid = [file for file in files if file.error is None]
    aliases = await kraken_rest_aliases(repository, {file.pair for file in valid}, run_time)
    mapping = {pair: next(iter(found)) for pair, found in aliases.items() if len(found) == 1}
    instruments = _requested_instruments(request, set(mapping.values()))
    contributing = tuple(file for file in valid if mapping.get(file.pair) in instruments)
    return SourceInputs(
        instruments=instruments,
        minutes=merge_snapshot_minutes(contributing, mapping),
        contributing=contributing,
        invalid=tuple(file for file in files if file.error is not None),
        unmapped=tuple(file.path for file in valid if file.pair not in aliases),
        ambiguous=tuple(file.path for file in valid if len(aliases.get(file.pair, ())) > 1),
    )


async def trade_candles(
    repository: Repository,
    exchange: str,
    instrument_public_id: str,
    window: tuple[datetime, datetime],
    as_of: datetime,
) -> list[CandleUpdate]:
    """Fold one instrument's stored trades of one window into closed minutes.

    A new builder starts with ``history_start`` at the window start. The
    repository read includes its end, so a trade at the exclusive end belongs
    to the next window and is dropped here. The final flush at the window end
    emits every bar that starts before it.

    Args:
        repository: Repository of the target database.
        exchange: Exchange of the trades.
        instrument_public_id: Instrument whose trades are folded.
        window: Half-open window inside one UTC day.
        as_of: Knowledge time of the trade read.

    Returns:
        Closed one-minute bars in time order.
    """
    lower, upper = window
    builder = TradeCandleBuilder(interval_seconds=60, history_start=lower)
    candles: list[CandleUpdate] = []
    async for _, trade in repository.iter_exchange_trades(
        exchange=exchange,
        start=lower,
        end=upper,
        as_of=as_of,
        instrument_public_ids=[instrument_public_id],
    ):
        event_time = as_utc(trade["executed_at"])
        if event_time >= upper:
            continue
        builder.update(trade_update(trade, instrument_public_id, event_time))
        candles.extend(builder.pop_completed(event_time))
    candles.extend(builder.pop_completed(upper))
    return candles


async def trade_sources(
    repository: Repository, request: RepairRequest, run_time: datetime
) -> SourceInputs:
    """Resolve the requested symbols and rebuild their bars from stored trades, day by day.

    Args:
        repository: Repository of the target database.
        request: Trades request.
        run_time: Time of the run, also the knowledge time of the trade reads.

    Returns:
        Instruments and their trade-built source minutes.

    Raises:
        RepairRefusedError: When a requested symbol has no active instrument.
    """
    resolved = await repository.get_instrument_public_ids_by_symbols(
        set(request.symbols), request.exchange, run_time
    )
    missing = sorted(set(request.symbols) - set(resolved))
    if missing:
        raise RepairRefusedError(
            f"symbols without an active {request.exchange} instrument: {missing}"
        )
    instruments = tuple(
        sorted(ResolvedInstrument(resolved[symbol], symbol) for symbol in request.symbols)
    )
    minutes: list[SourceMinute] = []
    for instrument in instruments:
        for day in request.window.days():
            for candle in await trade_candles(
                repository, request.exchange, instrument.instrument_public_id, day, run_time
            ):
                minutes.append(
                    SourceMinute(instrument, as_utc(candle.interval_begin), candle, None)
                )
    return SourceInputs(instruments=instruments, minutes=tuple(minutes))


async def gather_sources(
    repository: Repository, request: RepairRequest, run_time: datetime
) -> SourceInputs:
    """Collect the source minutes of the request.

    Args:
        repository: Repository of the target database.
        request: Validated request.
        run_time: Time of the run.

    Returns:
        The request's source inputs.
    """
    if request.source is RepairSource.SNAPSHOT:
        return await snapshot_sources(repository, request, run_time)
    return await trade_sources(repository, request, run_time)


async def _version_rows(
    repository: Repository,
    instrument_ids: Sequence[str],
    lower: datetime,
    upper: datetime,
    wanted: Collection[MinuteKey],
) -> list[CandleVersionRow]:
    """Read every one-minute version of some instruments in one span of one UTC day."""
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(text(_STATEMENT_TIMEOUT))
        result = await session.execute(
            select(Candle.__table__).where(
                Candle.instrument_public_id.in_(instrument_ids),
                Candle.timeframe == TIMEFRAME,
                Candle.open_at >= lower,
                Candle.open_at < upper,
            )
        )
        return [
            cast(CandleVersionRow, dict(row))
            for row in result.mappings()
            if (row["instrument_public_id"], row["open_at"]) in wanted
        ]


async def read_versions(
    repository: Repository, keys: Collection[MinuteKey]
) -> dict[MinuteKey, list[CandleVersionRow]]:
    """Read every stored version of the given minutes, by instrument list and UTC day.

    Candles are never joined with instruments. Each read covers at most 20
    instruments and the span of their wanted minutes inside one UTC day, and
    runs with a 20-second statement timeout on PostgreSQL.

    Args:
        repository: Repository of the target database.
        keys: Instrument and minute of every source minute.

    Returns:
        All versions of each key that has any.
    """
    by_day: dict[date, dict[str, list[datetime]]] = defaultdict(lambda: defaultdict(list))
    for instrument_id, minute in keys:
        by_day[minute.date()][instrument_id].append(minute)
    wanted = set(keys)
    found: dict[MinuteKey, list[CandleVersionRow]] = defaultdict(list)
    for day in sorted(by_day):
        minutes_by_instrument = by_day[day]
        for chunk in batched(
            sorted(minutes_by_instrument), _VERSION_READ_INSTRUMENTS, strict=False
        ):
            minutes = [
                minute for instrument in chunk for minute in minutes_by_instrument[instrument]
            ]
            for version in await _version_rows(
                repository, chunk, min(minutes), max(minutes) + _MINUTE, wanted
            ):
                found[(version["instrument_public_id"], version["open_at"])].append(version)
    return dict(found)


def _current_version(
    versions: Sequence[CandleVersionRow], as_of: datetime
) -> CandleVersionRow | None:
    """Return the one version known at ``as_of``, or None when there is not exactly one."""
    known = [version for version in versions if version["timestamp"] <= as_of < version["known_to"]]
    return known[0] if len(known) == 1 else None


def _candle_values(candle: CandleUpdate) -> tuple[float, float, float, float, float, float, int]:
    """Return a bar's business values in a fixed order."""
    return (
        candle.open,
        candle.high,
        candle.low,
        candle.close,
        candle.vwap,
        candle.volume,
        candle.trades,
    )


def _stored_values(
    version: CandleVersionRow,
) -> tuple[float, float, float, float, float | None, float, int | None]:
    """Return a stored version's business values in the order of :func:`_candle_values`."""
    return (
        version["open"],
        version["high"],
        version["low"],
        version["close"],
        version["vwap"],
        version["volume"],
        version["trades"],
    )


def _is_flat_zero(version: CandleVersionRow) -> bool:
    """Recognize a flat bar with volume 0 and trade count 0."""
    return (
        version["volume"] == 0
        and version["trades"] == 0
        and version["open"] == version["high"] == version["low"] == version["close"]
    )


def _protects_zero(version: CandleVersionRow) -> bool:
    """Recognize a version that proves the minute was not a synthesized zero."""
    return (
        version["source"] != _SYNTHESIZED
        or version["volume"] != 0
        or version["trades"] is None
        or version["trades"] != 0
    )


def classify(
    minute: SourceMinute, versions: Sequence[CandleVersionRow], as_of: datetime
) -> PlanEntry:
    """Decide what the plan does with one source minute.

    Args:
        minute: Source minute.
        versions: Every stored version of its natural key.
        as_of: Time of the run; the version known then is the current one.

    Returns:
        ``insert`` without any version; ``equal`` when the current row equals
        the bar; ``differs`` for another native or calculated row; ``correct``
        for a flat synthesized zero without protected history; otherwise
        ``skip`` with a reason.
    """
    current = _current_version(versions, as_of)
    if minute.candle is None:
        return PlanEntry(minute, Action.SKIP, "conflicting_snapshot_files", current)
    if not versions:
        return PlanEntry(minute, Action.INSERT, "", None)
    if current is None:
        return PlanEntry(minute, Action.SKIP, "no_single_current_version", None)
    if _stored_values(current) == _candle_values(minute.candle):
        return PlanEntry(minute, Action.EQUAL, "", current)
    if current["source"] != _SYNTHESIZED:
        return PlanEntry(minute, Action.DIFFERS, "", current)
    if not _is_flat_zero(current):
        return PlanEntry(minute, Action.SKIP, "synthesized_not_flat_zero", current)
    if any(_protects_zero(version) for version in versions):
        return PlanEntry(minute, Action.SKIP, "zero_with_protected_history", current)
    return PlanEntry(minute, Action.CORRECT, "", current)


def _entry_candle(entry: PlanEntry) -> CandleUpdate:
    """Return the bar of a writable entry."""
    if entry.source.candle is None:
        raise ValueError("a minute without one agreed bar cannot be written")
    return entry.source.candle


def repair_row(
    entry: PlanEntry, source: RepairSource, timestamp: datetime, session_id: str, sequence_id: int
) -> CandleUpsertRow:
    """Build the complete guarded-writer row of one entry.

    Calculated rows come from the row builder shared with the trade candle
    backfill. Native rows carry the snapshot bar unchanged.

    Args:
        entry: Entry with a bar.
        source: Source of the run.
        timestamp: Knowledge time of the row.
        session_id: Writer session of the row.
        sequence_id: Writer sequence number of the row.

    Returns:
        Row for an insert or a correction.
    """
    candle = _entry_candle(entry)
    instrument_id = entry.source.instrument.instrument_public_id
    if source is RepairSource.TRADES:
        return calculated_row(
            candle,
            instrument_id,
            timestamp=timestamp,
            session_id=session_id,
            sequence_id=sequence_id,
        )
    return {
        "instrument_public_id": instrument_id,
        "open_at": entry.source.minute,
        "timestamp": timestamp,
        "timeframe": TIMEFRAME,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
        "vwap": candle.vwap,
        "trades": candle.trades,
        "source": _NATIVE,
        "complete": True,
        "session_id": session_id,
        "sequence_id": sequence_id,
    }


def _guard_key(row: CandleUpsertRow) -> MinuteKey:
    """Return a writer row's instrument and minute."""
    return row["instrument_public_id"], row["open_at"]


def _predecessor(entry: PlanEntry) -> CandlePredecessor:
    """Return the current zero of a correction as the writer's expected predecessor."""
    current = cast(CandleVersionRow, entry.current)
    return {
        "id": current["id"],
        "public_id": current["public_id"],
        "timestamp": current["timestamp"],
    }


def _predecessors(entries: Sequence[PlanEntry]) -> dict[_GuardKey, CandlePredecessor]:
    """Return the expected predecessor of each correction, by full natural key."""
    return {
        (
            entry.source.instrument.instrument_public_id,
            TIMEFRAME,
            entry.source.minute,
        ): _predecessor(entry)
        for entry in entries
    }


async def _guard_call(
    repository: Repository,
    source: RepairSource,
    action: Action,
    entries: Sequence[PlanEntry],
    rows: list[CandleUpsertRow],
) -> list[CandleUpsertRow]:
    """Ask the guarded writer of an action which rows it would apply, without writing."""
    if action is Action.CORRECT:
        return await repository.correct_false_zero_candles(
            rows,
            dry_run=True,
            source=source.row_source,
            expected_predecessors=_predecessors(entries),
        )
    if source is RepairSource.SNAPSHOT:
        return await repository.insert_missing_native_candles(rows, dry_run=True)
    return await repository.insert_missing_calculated_candles(rows, dry_run=True)


async def guard_refusals(
    repository: Repository, source: RepairSource, entries: Sequence[PlanEntry], run_time: datetime
) -> set[MinuteKey]:
    """Return the planned writes that the guarded writers would not apply.

    The rows carry the source bar, this planning run's own session id and
    sequence numbers in plan order, and as knowledge time the snapshot's
    ``fetched_at`` or, for trades, the time of the run. They are never written.

    Args:
        repository: Repository of the target database.
        source: Source of the run.
        entries: The plan in plan order.
        run_time: Time of the run.

    Returns:
        Keys of ``insert`` and ``correct`` entries the dry run dropped.
    """
    session_id = str(uuid7())
    writes = [entry for entry in entries if entry.action in _WRITE_ACTIONS]
    rows = {
        entry.source.key: repair_row(
            entry, source, entry.source.known_at or run_time, session_id, sequence
        )
        for sequence, entry in enumerate(writes, start=1)
    }
    refused: set[MinuteKey] = set()
    for action in (Action.INSERT, Action.CORRECT):
        planned = [entry for entry in writes if entry.action is action]
        for chunk in batched(planned, CHUNK_ROWS, strict=False):
            chunk_rows = [rows[entry.source.key] for entry in chunk]
            accepted = {
                _guard_key(row)
                for row in await _guard_call(repository, source, action, chunk, chunk_rows)
            }
            refused.update(entry.source.key for entry in chunk if entry.source.key not in accepted)
    return refused


def _refused(entry: PlanEntry) -> PlanEntry:
    """Turn a planned write into a guard refusal that names the refused action."""
    return PlanEntry(entry.source, Action.REFUSED_BY_GUARD, entry.action.value, entry.current)


async def build_plan(
    repository: Repository, source: RepairSource, inputs: SourceInputs, run_time: datetime
) -> list[PlanEntry]:
    """Classify every source minute and confirm planned writes with the guards' dry run.

    Args:
        repository: Repository of the target database.
        source: Source of the run.
        inputs: Source minutes.
        run_time: Time of the run.

    Returns:
        One entry per source minute, sorted by instrument public ID and minute.
    """
    versions = await read_versions(repository, [minute.key for minute in inputs.minutes])
    entries = sorted(
        (classify(minute, versions.get(minute.key, []), run_time) for minute in inputs.minutes),
        key=lambda entry: entry.source.key,
    )
    refused = await guard_refusals(repository, source, entries, run_time)
    return [_refused(entry) if entry.source.key in refused else entry for entry in entries]


def _number_text(value: float | int | None) -> str:
    """Write a number with ``repr``, or nothing when it is unknown."""
    return "" if value is None else repr(value)


def _time_text(value: datetime | None) -> str:
    """Write an ISO time, or nothing when it is unknown."""
    return "" if value is None else value.isoformat()


def _current_columns(current: CandleVersionRow | None) -> tuple[str, str, str, str, str]:
    """Return the preview columns of the current version."""
    if current is None:
        return "", "", "", "", ""
    return (
        str(current["id"]),
        current["public_id"],
        current["source"],
        _number_text(current["volume"]),
        _time_text(current["timestamp"]),
    )


def _bar_columns(values: Sequence[float | int | None] | None) -> tuple[str, ...]:
    """Return the preview columns of a bar: OHLC, VWAP, volume and trade count."""
    if values is None:
        return ("",) * 7
    return tuple(_number_text(value) for value in values)


def preview_line(entry: PlanEntry, source: RepairSource) -> PreviewLine:
    """Render one entry as a ``preview.csv`` line.

    Args:
        entry: Plan entry.
        source: Source of the run.

    Returns:
        The line; floats are written with ``repr``.
    """
    candle = entry.source.candle
    return PreviewLine(
        entry.source.instrument.symbol,
        entry.source.instrument.instrument_public_id,
        entry.source.minute.isoformat(),
        entry.action.value,
        entry.reason,
        *_current_columns(entry.current),
        source.row_source,
        _time_text(entry.source.known_at),
        *_bar_columns(None if candle is None else _candle_values(candle)),
    )


def preview_bytes(lines: Sequence[PreviewLine]) -> bytes:
    """Serialize preview lines with their header.

    Args:
        lines: Lines in plan order.

    Returns:
        UTF-8 CSV whose lines end with a single newline.
    """
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(PREVIEW_COLUMNS)
    writer.writerows(astuple(line) for line in lines)
    return buffer.getvalue().encode()


def parse_preview(data: bytes) -> tuple[PreviewLine, ...]:
    """Parse a voted ``preview.csv``.

    Args:
        data: The file's bytes.

    Returns:
        Its lines.

    Raises:
        RepairRefusedError: When the header, a line's width, an action or a repeated
            minute shows that the file is not a preview.
    """
    try:
        rows = list(csv.reader(io.StringIO(data.decode(), newline="")))
    except (UnicodeDecodeError, csv.Error) as error:
        raise RepairRefusedError("voted preview.csv does not parse") from error
    if not rows or tuple(rows[0]) != PREVIEW_COLUMNS:
        raise RepairRefusedError("voted preview.csv has an unexpected header")
    actions = {action.value for action in Action}
    if any(len(row) != len(PREVIEW_COLUMNS) or row[3] not in actions for row in rows[1:]):
        raise RepairRefusedError("voted preview.csv holds a malformed line")
    lines = tuple(PreviewLine(*row) for row in rows[1:])
    if len({line.key for line in lines}) != len(lines):
        raise RepairRefusedError("voted preview.csv repeats a minute")
    return lines


def approval_header(
    request: RepairRequest, context: RunContext, inputs: SourceInputs | None
) -> JsonObject:
    """Return the approval's binding fields, without the preview hash.

    Without inputs only the fields known before any database read are given.

    Args:
        request: Validated request.
        context: Run context.
        inputs: Resolved inputs, or None.

    Returns:
        Database, exchange, timeframe, source, window, requested symbols and
        code revision, and with inputs also the instruments and snapshot files.
    """
    header: JsonObject = {
        "database": context.database,
        "exchange": request.exchange,
        "timeframe": TIMEFRAME,
        "source": request.source.value,
        "window": request.window.to_json(),
        "symbols": {"all_mapped": request.all_mapped, "requested": list(request.symbols)},
        "code_revision": context.code_revision,
    }
    if inputs is not None:
        header["instruments"] = [
            {"instrument_public_id": item.instrument_public_id, "symbol": item.symbol}
            for item in inputs.instruments
        ]
        header["snapshot_files"] = [
            {"path": file.path, "sha256": file.sha256, "fetched_at": file.fetched_at}
            for file in inputs.contributing
        ]
    return cast(JsonObject, json.loads(canonical_json(header)))


def _counts(entries: Sequence[PlanEntry]) -> JsonObject:
    """Count entries per action, listing every action."""
    counted = Counter(entry.action.value for entry in entries)
    return {action.value: counted[action.value] for action in Action}


def _grouped_counts(groups: Mapping[str, list[PlanEntry]]) -> JsonObject:
    """Count entries per action within each group, in group order."""
    return {name: _counts(groups[name]) for name in sorted(groups)}


def summary_document(
    digest: str, preview_sha256: str, entries: Sequence[PlanEntry], inputs: SourceInputs
) -> JsonObject:
    """Summarize a plan for the vote.

    Args:
        digest: sha256 of ``approval.json``.
        preview_sha256: sha256 of ``preview.csv``.
        entries: The plan.
        inputs: Its source inputs.

    Returns:
        Counts per action, per reason, per symbol and per UTC day, and the
        snapshot files that were skipped.
    """
    by_symbol: dict[str, list[PlanEntry]] = defaultdict(list)
    by_day: dict[str, list[PlanEntry]] = defaultdict(list)
    for entry in entries:
        by_symbol[entry.source.instrument.symbol].append(entry)
        by_day[entry.source.minute.date().isoformat()].append(entry)
    reasons = Counter(f"{entry.action.value}:{entry.reason}" for entry in entries if entry.reason)
    return {
        "digest": digest,
        "preview_sha256": preview_sha256,
        "minutes": len(entries),
        "instruments": len(inputs.instruments),
        "actions": _counts(entries),
        "reasons": {reason: reasons[reason] for reason in sorted(reasons)},
        "by_symbol": _grouped_counts(by_symbol),
        "by_day": _grouped_counts(by_day),
        "snapshot_files": {
            "contributing": len(inputs.contributing),
            "invalid": [{"path": file.path, "error": file.error or ""} for file in inputs.invalid],
            "unmapped": list(inputs.unmapped),
            "ambiguous": list(inputs.ambiguous),
        },
    }


def write_plan_artifacts(
    request: RepairRequest,
    context: RunContext,
    inputs: SourceInputs,
    entries: Sequence[PlanEntry],
) -> PlanArtifacts:
    """Write ``preview.csv``, ``approval.json`` and ``summary.json`` exclusively, mode 0600.

    Args:
        request: Validated request.
        context: Run context.
        inputs: Source inputs of the plan.
        entries: The plan in plan order.

    Returns:
        The approval digest, the summary and the preview lines.
    """
    lines = tuple(preview_line(entry, request.source) for entry in entries)
    preview = preview_bytes(lines)
    preview_sha256 = sha256_hex(preview)
    approval = canonical_json(
        approval_header(request, context, inputs) | {"preview_sha256": preview_sha256}
    )
    digest = sha256_hex(approval)
    summary = summary_document(digest, preview_sha256, entries, inputs)
    write_exclusive(request.output_dir / PREVIEW_FILE, preview)
    write_exclusive(request.output_dir / APPROVAL_FILE, approval)
    write_exclusive(
        request.output_dir / SUMMARY_FILE,
        json.dumps(summary, indent=2, sort_keys=True).encode() + b"\n",
    )
    return PlanArtifacts(digest, summary, lines)


def _read_voted_file(directory: Path, name: str) -> bytes:
    """Read one file of the voted preview, refusing when it cannot be read."""
    try:
        return (directory / name).read_bytes()
    except OSError as error:
        raise RepairRefusedError(f"voted {name} cannot be read") from error


def load_voted_preview(directory: Path, expect_digest: str) -> VotedPreview:
    """Verify and parse the voted preview before any database read.

    Args:
        directory: Directory of the voted run.
        expect_digest: The voted digest.

    Returns:
        The verified approval and preview lines.

    Raises:
        RepairRefusedError: When a file is missing, a hash differs or a file does not parse.
    """
    approval_data = _read_voted_file(directory, APPROVAL_FILE)
    preview_data = _read_voted_file(directory, PREVIEW_FILE)
    digest = sha256_hex(approval_data)
    if digest != expect_digest:
        raise RepairRefusedError("sha256 of the voted approval.json differs from --expect-digest")
    try:
        approval: object = json.loads(approval_data)
    except ValueError as error:
        raise RepairRefusedError("voted approval.json does not parse") from error
    if not isinstance(approval, dict) or not isinstance(approval.get("preview_sha256"), str):
        raise RepairRefusedError("voted approval.json is not an approval")
    if sha256_hex(preview_data) != approval["preview_sha256"]:
        raise RepairRefusedError("voted preview.csv does not match the hash in approval.json")
    return VotedPreview(digest, cast(JsonObject, approval), parse_preview(preview_data))


def refuse_approval_mismatch(voted: JsonObject, header: JsonObject, *, complete: bool) -> None:
    """Refuse when the voted approval binds anything other than this run.

    Args:
        voted: The voted approval.
        header: This run's binding fields.
        complete: Whether ``header`` holds every binding field, so that a
            voted field this run does not produce also differs.

    Raises:
        RepairRefusedError: Naming each differing field.
    """
    names = set(header) | (set(voted) - {"preview_sha256"} if complete else set())
    differing = sorted(name for name in names if voted.get(name) != header.get(name))
    if differing:
        raise RepairRefusedError(
            f"voted approval.json differs from this run in: {', '.join(differing)}"
        )


def _vote_lines(lines: Sequence[PreviewLine]) -> dict[tuple[str, str], PreviewLine]:
    """Index the voted ``insert`` and ``correct`` lines by instrument and minute."""
    writes = {Action.INSERT.value, Action.CORRECT.value}
    return {line.key: line for line in lines if line.action in writes}


def _current_matches_vote(entry: PlanEntry | None, vote: PreviewLine) -> bool:
    """Recognize a voted bar that the current row now carries."""
    if entry is None or entry.current is None:
        return False
    return _bar_columns(_stored_values(entry.current)) == vote.new_values[2:]


def select_voted(
    entries: Sequence[PlanEntry], lines: Sequence[PreviewLine], voted: Sequence[PreviewLine]
) -> Selection:
    """Keep the planned writes that the vote holds unchanged.

    A planned row is selected only if the voted line has the same action,
    the same new values and, for a correction, the same predecessor id,
    ``public_id`` and knowledge time.

    Args:
        entries: The rebuilt plan.
        lines: Its preview lines, aligned with ``entries``.
        voted: The voted preview lines.

    Returns:
        Selected rows and the ``not_voted``, ``already_applied`` and ``changed`` reports.
    """
    votes = _vote_lines(voted)
    rebuilt = {line.key: entry for entry, line in zip(entries, lines, strict=True)}
    selected: list[PlanEntry] = []
    not_voted: list[PlanEntry] = []
    for entry, line in zip(entries, lines, strict=True):
        if entry.action not in _WRITE_ACTIONS:
            continue
        vote = votes.get(line.key)
        if vote is not None and vote.vote == line.vote:
            selected.append(entry)
            del votes[line.key]
        else:
            not_voted.append(entry)
    already: list[PreviewLine] = []
    changed: list[PreviewLine] = []
    for vote in votes.values():
        matched = _current_matches_vote(rebuilt.get(vote.key), vote)
        (already if matched else changed).append(vote)
    return Selection(tuple(selected), tuple(not_voted), tuple(already), tuple(changed))


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


def _written_by(version: CandleVersionRow, row: CandleUpsertRow) -> bool:
    """Recognize the version a write of ``row`` would have stored."""
    stored = cast(Mapping[str, object], version)
    written = cast(Mapping[str, object], row)
    return all(stored[name] == written[name] for name in _WRITTEN_FIELDS)


async def reconcile(
    repository: Repository, rows: Sequence[CandleUpsertRow], session_id: str
) -> dict[MinuteKey, str]:
    """Look for this run's versions of an uncertain chunk without taking any lock.

    One fresh transaction, explicitly READ COMMITTED on PostgreSQL and a plain
    read on SQLite, reads every version this run's writer session stored for
    the chunk's natural keys. Whether a version is current is decided by that
    same SELECT from the current-version sentinel, never from a clock read
    before it, so a successor committed before the read is always seen.

    Args:
        repository: Repository of the target database.
        rows: The chunk's rows, with this run's session and sequence ids.
        session_id: This run's writer session id.

    Returns:
        ``applied`` for a version the read found current, ``applied_superseded``
        for one it found superseded, else ``uncertain``, by key.
    """
    keys = [(row["instrument_public_id"], TIMEFRAME, row["open_at"]) for row in rows]
    async with repository.session() as session:
        if repository.dialect_name == "postgresql":
            await session.execute(text(_READ_COMMITTED))
            await session.execute(text(_STATEMENT_TIMEOUT))
        versions = await repository.get_session_candle_versions(session, keys, session_id)
    outcomes: dict[MinuteKey, str] = {}
    for row in rows:
        current = [is_current for version, is_current in versions if _written_by(version, row)]
        outcomes[_guard_key(row)] = (
            "uncertain" if not current else "applied" if any(current) else "applied_superseded"
        )
    return outcomes


def write_chunks(
    entries: Sequence[PlanEntry], source: RepairSource, run_id: str, write_time: datetime
) -> list[WriteChunk]:
    """Build the run's rows and split them into insert and correction chunks.

    Rows carry the run id as session id and sequence ids from 1 in plan
    order. A snapshot row keeps ``fetched_at`` as knowledge time; a trades row
    takes the write time. Inserts precede corrections.

    Args:
        entries: Selected entries in plan order.
        source: Source of the run.
        run_id: The write run's id.
        write_time: Fixed time of the write run.

    Returns:
        Chunks of at most 100 rows, numbered from 1.
    """
    rows = {
        entry.source.key: repair_row(
            entry, source, entry.source.known_at or write_time, run_id, sequence
        )
        for sequence, entry in enumerate(entries, start=1)
    }
    chunks: list[WriteChunk] = []
    for action in (Action.INSERT, Action.CORRECT):
        planned = [entry for entry in entries if entry.action is action]
        for chunk in batched(planned, CHUNK_ROWS, strict=False):
            chunks.append(
                WriteChunk(
                    number=len(chunks) + 1,
                    action=action,
                    rows=tuple(rows[entry.source.key] for entry in chunk),
                    predecessors=_predecessors(chunk) if action is Action.CORRECT else {},
                )
            )
    return chunks


def _row_reference(row: CandleUpsertRow) -> JsonObject:
    """Identify a written row in a receipt."""
    return {
        "instrument_public_id": row["instrument_public_id"],
        "minute_utc": row["open_at"].isoformat(),
        "sequence_id": row["sequence_id"],
    }


def _line_reference(line: PreviewLine) -> JsonObject:
    """Identify a voted line in a receipt."""
    return {
        "instrument_public_id": line.instrument_public_id,
        "minute_utc": line.minute_utc,
        "action": line.action,
    }


def _entry_reference(entry: PlanEntry) -> JsonObject:
    """Identify a planned row in a receipt."""
    return {
        "instrument_public_id": entry.source.instrument.instrument_public_id,
        "minute_utc": entry.source.minute.isoformat(),
        "action": entry.action.value,
    }


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

    chunk: WriteChunk
    started_at: datetime
    attempts: list[JsonObject] = field(default_factory=list)
    outcomes: dict[MinuteKey, str] = field(default_factory=dict)
    reconciliation_error: JsonObject | None = None

    def settle(self, outcome: str) -> None:
        """Give every row without an outcome the same one."""
        for row in self.chunk.rows:
            self.outcomes.setdefault(_guard_key(row), outcome)

    def to_json(self, ended_at: datetime) -> JsonObject:
        """Render the chunk's receipt line."""
        listed: dict[str, list[JsonValue]] = {outcome: [] for outcome in _ROW_OUTCOMES}
        for row in self.chunk.rows:
            listed[self.outcomes.get(_guard_key(row), "uncertain")].append(_row_reference(row))
        return {
            "chunk": self.chunk.number,
            "action": self.chunk.action.value,
            "rows": len(self.chunk.rows),
            "attempts": list(self.attempts),
            "attempt_count": len(self.attempts),
            "reconciliation_error": self.reconciliation_error,
            "started_at": self.started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            **{outcome: cast(JsonValue, rows) for outcome, rows in listed.items()},
        }


class _ReceiptLog:
    """Append one durable JSON line per finished chunk to ``receipt.jsonl``."""

    def __init__(self, path: Path) -> None:
        """Create the log exclusively with mode 0600."""
        self._handle = _open_exclusive(path)

    def append(self, record: JsonObject) -> None:
        """Write, flush and sync one line."""
        self._handle.write(canonical_json(record) + b"\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        """Close the log."""
        self._handle.close()


class _ChunkWriter:
    """Apply chunks with the retry policy and keep their receipts."""

    def __init__(
        self,
        context: RunContext,
        source: RepairSource,
        options: WriteOptions,
        log: _ReceiptLog,
        run_id: str,
    ) -> None:
        """Bind the writer to its run."""
        self._context = context
        self._source = source
        self._options = options
        self._log = log
        self._run_id = run_id
        self._lock_streak = 0
        self.records: list[JsonObject] = []

    async def apply(self, chunks: Sequence[WriteChunk]) -> str:
        """Apply chunks in order, pausing between them, until one stops the run."""
        for index, chunk in enumerate(chunks):
            if index:
                await self._context.sleep(self._options.chunk_pause_seconds)
            stop = await self._apply_chunk(chunk)
            if stop is not None:
                return stop
        return "completed"

    async def _apply_chunk(self, chunk: WriteChunk) -> str | None:
        """Run one chunk's attempts and log its receipt as soon as it ends."""
        record = _ChunkRecord(chunk, self._context.clock())
        try:
            return await self._attempts(chunk, record)
        finally:
            record.settle("uncertain")
            line = record.to_json(self._context.clock())
            self.records.append(line)
            self._log.append(line)

    async def _attempts(self, chunk: WriteChunk, record: _ChunkRecord) -> str | None:
        """Attempt a chunk until it is acknowledged or the policy stops the run."""
        attempt = 0
        while True:
            attempt += 1
            try:
                returned = await self._call(chunk)
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
            for row in returned:
                record.outcomes[_guard_key(row)] = "applied"
            record.settle("skipped_by_guard")
            self._lock_streak = 0
            return None

    async def _call(self, chunk: WriteChunk) -> list[CandleUpsertRow]:
        """Hand one chunk to its guarded writer in an owned transaction."""
        repository = self._context.repository
        rows = list(chunk.rows)
        if chunk.action is Action.CORRECT:
            return await repository.correct_false_zero_candles(
                rows, source=self._source.row_source, expected_predecessors=chunk.predecessors
            )
        if self._source is RepairSource.SNAPSHOT:
            return await repository.insert_missing_native_candles(rows)
        return await repository.insert_missing_calculated_candles(rows)

    async def _failed(
        self, chunk: WriteChunk, record: _ChunkRecord, attempt: int, error: Exception
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
        await self._context.sleep(BACKOFF_SECONDS[attempt - 1])
        return None

    async def _reconcile(self, chunk: WriteChunk, record: _ChunkRecord) -> None:
        """Record what a plain read finds of an uncertain chunk; a failed read leaves it uncertain."""
        try:
            record.outcomes.update(
                await reconcile(self._context.repository, chunk.rows, self._run_id)
            )
        except Exception as error:
            record.reconciliation_error = error_fields(error)


def _totals(records: Sequence[JsonObject]) -> JsonObject:
    """Count rows per outcome over all chunk receipts."""
    return {
        outcome: sum(len(cast(list[JsonValue], record[outcome])) for record in records)
        for outcome in _ROW_OUTCOMES
    }


@dataclass(frozen=True, slots=True)
class _WriteRun:
    """Fixed facts of one write run, for its receipt."""

    voted_digest: str
    run_id: str
    write_time: datetime
    started_at: datetime
    selection: Selection
    batch: int

    def receipt(
        self, stop_reason: str, records: Sequence[JsonObject], ended_at: datetime
    ) -> JsonObject:
        """Render ``receipt.json``."""
        return {
            "voted_digest": self.voted_digest,
            "run_id": self.run_id,
            "write_time": self.write_time.isoformat(),
            "started_at": self.started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "stop_reason": stop_reason,
            "selected": len(self.selection.selected),
            "attempted": self.batch,
            "left": len(self.selection.selected) - self.batch,
            "not_voted": [_entry_reference(entry) for entry in self.selection.not_voted],
            "already_applied": [_line_reference(line) for line in self.selection.already_applied],
            "changed": [_line_reference(line) for line in self.selection.changed],
            "chunks": list(records),
            "totals": _totals(records),
        }


async def apply_selection(
    context: RunContext,
    request: RepairRequest,
    options: WriteOptions,
    voted_digest: str,
    selection: Selection,
) -> tuple[str, JsonObject]:
    """Apply the selected rows and always leave ``receipt.json``.

    Args:
        context: Run context.
        request: Validated request.
        options: Write options.
        voted_digest: Digest of the voted approval.
        selection: Selected rows and reports.

    Returns:
        The stop reason and the row totals.
    """
    write_time = context.clock()
    batch = selection.selected[: options.max_rows]
    run = _WriteRun(voted_digest, str(uuid7()), write_time, write_time, selection, len(batch))
    log = _ReceiptLog(request.output_dir / RECEIPT_LINES_FILE)
    writer = _ChunkWriter(context, request.source, options, log, run.run_id)
    stop_reason = "error"
    try:
        stop_reason = await writer.apply(
            write_chunks(batch, request.source, run.run_id, write_time)
        )
        if stop_reason == "completed" and run.batch < len(selection.selected):
            stop_reason = "max_rows"
    except (asyncio.CancelledError, KeyboardInterrupt):
        stop_reason = "cancelled"
        raise
    finally:
        log.close()
        write_exclusive(
            request.output_dir / RECEIPT_FILE,
            json.dumps(
                run.receipt(stop_reason, writer.records, context.clock()), indent=2, sort_keys=True
            ).encode()
            + b"\n",
        )
    return stop_reason, _totals(writer.records)


async def plan_repair(context: RunContext, request: RepairRequest) -> PlanArtifacts:
    """Run the dry run: plan, preview, approval and summary.

    Args:
        context: Run context.
        request: Validated request.

    Returns:
        The plan's files.
    """
    run_time = context.clock()
    refuse_live_edge(request.window, run_time)
    inputs = await gather_sources(context.repository, request, run_time)
    entries = await build_plan(context.repository, request.source, inputs, run_time)
    return write_plan_artifacts(request, context, inputs, entries)


async def write_repair(
    context: RunContext, request: RepairRequest, options: WriteOptions
) -> WriteReport:
    """Run a write bound to a voted preview.

    The voted files are verified before any database read, and the voted
    approval must bind this run's database, request and code. The plan is then
    rebuilt and only its rows that the vote holds unchanged are applied.

    Args:
        context: Run context.
        request: Validated request.
        options: Write options.

    Returns:
        The write's outcome.
    """
    voted = load_voted_preview(options.voted_preview, options.expect_digest)
    run_time = context.clock()
    refuse_live_edge(request.window, run_time)
    refuse_approval_mismatch(
        voted.approval, approval_header(request, context, None), complete=False
    )
    inputs = await gather_sources(context.repository, request, run_time)
    refuse_approval_mismatch(
        voted.approval, approval_header(request, context, inputs), complete=True
    )
    entries = await build_plan(context.repository, request.source, inputs, run_time)
    artifacts = write_plan_artifacts(request, context, inputs, entries)
    selection = select_voted(entries, artifacts.lines, voted.lines)
    stop_reason, totals = await apply_selection(context, request, options, voted.digest, selection)
    return WriteReport(voted.digest, artifacts.digest, stop_reason, totals)
